// master/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/types"
)

const (
	MaxRetries = 3
)

type Master struct {
	workers      map[string]*types.WorkerInfo
	workersMutex sync.RWMutex
	jobs         map[string]*types.Job
	jobsMutex    sync.RWMutex
	scheduler    *Scheduler
	port         string
}

func NewMaster(port string) *Master {
	m := &Master{
		workers: make(map[string]*types.WorkerInfo),
		jobs:    make(map[string]*types.Job),
		port:    port,
	}
	m.scheduler = NewScheduler(m)
	return m
}

func main() {
	port := "8080"
	master := NewMaster(port)

	// Iniciar monitor de heartbeats
	go master.monitorWorkers()

	// Iniciar scheduler
	master.scheduler.Start()

	// Configurar rutas HTTP con mejor ruteo
	http.HandleFunc("/api/v1/workers/register", master.handleWorkerRegister)
	http.HandleFunc("/api/v1/workers/heartbeat", master.handleHeartbeat)
	http.HandleFunc("/api/v1/jobs", master.handleJobsDispatch) // POST /api/v1/jobs
	http.HandleFunc("/api/v1/jobs/", master.routeJobs)         // GET /api/v1/jobs/{id}
	http.HandleFunc("/api/v1/workers", master.handleWorkersList)
	http.HandleFunc("/internal/tasks/", master.handleTaskResult)
	http.HandleFunc("/api/v1/metrics", master.handleMetrics)
	http.HandleFunc("/api/v1/metrics/detailed", master.handleDetailedMetrics)
	http.HandleFunc("/", master.handleDashboard) // Dashboard HTML

	log.Printf("🚀 Master iniciando en puerto %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Error iniciando servidor: %v", err)
	}
}

// ============================================================================
// WORKER MANAGEMENT
// ============================================================================

func (m *Master) handleWorkerRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		WorkerID string `json:"worker_id"`
		Address  string `json:"address"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	m.workersMutex.Lock()
	m.workers[req.WorkerID] = &types.WorkerInfo{
		ID:            req.WorkerID,
		Address:       req.Address,
		Status:        "IDLE",
		LastHeartbeat: time.Now(),
		ActiveTasks:   0,
		TotalTasks:    0,
	}
	m.workersMutex.Unlock()

	log.Printf("✅ Worker registrado: %s (%s)", req.WorkerID, req.Address)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Worker registrado exitosamente",
	})
}

func (m *Master) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var hb types.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	m.workersMutex.Lock()
	if worker, exists := m.workers[hb.WorkerID]; exists {
		worker.LastHeartbeat = time.Now()
		worker.ActiveTasks = hb.ActiveTasks
		if hb.ActiveTasks > 0 {
			worker.Status = "BUSY"
		} else {
			worker.Status = "IDLE"
		}
	}
	m.workersMutex.Unlock()

	json.NewEncoder(w).Encode(types.HeartbeatResponse{
		Success: true,
		Message: "Heartbeat recibido",
	})
}

func (m *Master) monitorWorkers() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m.workersMutex.Lock()
		now := time.Now()
		for id, worker := range m.workers {
			if now.Sub(worker.LastHeartbeat) > 10*time.Second && worker.Status != "DOWN" {
				log.Printf("⚠️  Worker %s marcado como DOWN (sin heartbeat)", id)
				worker.Status = "DOWN"
			}
		}
		m.workersMutex.Unlock()
	}
}

// ============================================================================
// JOB ROUTING
// ============================================================================

func (m *Master) handleJobsDispatch(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		m.handleJobSubmit(w, r)
		return
	}
	http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
}

func (m *Master) routeJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		m.handleJobRequest(w, r)
		return
	}
	http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
}

func (m *Master) handleJobSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, fmt.Sprintf("JSON inválido: %v", err), http.StatusBadRequest)
		return
	}

	job.ID = fmt.Sprintf("job-%d", time.Now().Unix())
	job.Status = "PENDING"
	job.SubmittedAt = time.Now()

	if len(job.DAG.Nodes) == 0 {
		http.Error(w, "DAG debe tener al menos un nodo", http.StatusBadRequest)
		return
	}

	m.jobsMutex.Lock()
	m.jobs[job.ID] = &job
	m.jobsMutex.Unlock()

	log.Printf("📥 Job recibido: %s (%s) con %d nodos", job.ID, job.Name, len(job.DAG.Nodes))

	go func() {
		if err := m.scheduler.ScheduleJob(&job); err != nil {
			log.Printf("❌ Error planificando job %s: %v", job.ID, err)
			job.Status = "FAILED"
		}
	}()

	response := types.JobSubmitResponse{
		JobID:   job.ID,
		Status:  "PENDING",
		Message: "Job aceptado para ejecución",
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)
}

func (m *Master) handleJobRequest(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	log.Printf("🔍 DEBUG: handleJobRequest - path: %s (len: %d)", path, len(path))

	// /api/v1/jobs/{jobID} o /api/v1/jobs/{jobID}/results
	// "/api/v1/jobs/" = 13 caracteres

	var jobID string
	var isResults bool

	// Remover "/api/v1/jobs/" (13 caracteres)
	if len(path) <= 13 {
		http.Error(w, "ID de job inválido", http.StatusBadRequest)
		return
	}
	jobID = path[13:]
	log.Printf("🔍 DEBUG: jobID extracción inicial: '%s'", jobID)

	// Verificar si termina con /results
	if len(jobID) > 8 && jobID[len(jobID)-8:] == "/results" {
		isResults = true
		jobID = jobID[:len(jobID)-8]
		log.Printf("🔍 DEBUG: Es /results, jobID final: '%s'", jobID)
	}

	log.Printf("🔍 DEBUG: Buscando job: '%s'", jobID)
	m.jobsMutex.RLock()
	job, exists := m.jobs[jobID]
	jobsKeys := make([]string, 0, len(m.jobs))
	for k := range m.jobs {
		jobsKeys = append(jobsKeys, k)
	}
	m.jobsMutex.RUnlock()

	log.Printf("🔍 DEBUG: Jobs en memoria: %v", jobsKeys)
	log.Printf("🔍 DEBUG: Job existe: %v", exists)

	if !exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("Job %s no encontrado. Jobs disponibles: %v", jobID, jobsKeys),
		})
		return
	}

	if isResults {
		m.handleJobResults(w, r, job)
	} else {
		m.handleJobStatus(w, r, job)
	}
}

func (m *Master) handleJobStatus(w http.ResponseWriter, r *http.Request, job *types.Job) {
	w.Header().Set("Content-Type", "application/json")

	execution := m.scheduler.GetJobExecution(job.ID)

	var tasksTotal, tasksDone, tasksPending, tasksFailed int

	if execution != nil {
		execution.TasksMutex.RLock()
		tasksTotal = len(execution.Tasks)
		for _, task := range execution.Tasks {
			switch task.Status {
			case "COMPLETED":
				tasksDone++
			case "FAILED":
				if task.AttemptNum >= MaxRetries {
					tasksFailed++
				}
			case "PENDING", "RUNNING":
				tasksPending++
			}
		}
		execution.TasksMutex.RUnlock()
	}

	progress := 0.0
	if tasksTotal > 0 {
		progress = float64(tasksDone) / float64(tasksTotal) * 100.0
	}

	response := types.JobStatusResponse{
		Job:          *job,
		Progress:     progress,
		TasksTotal:   tasksTotal,
		TasksDone:    tasksDone,
		TasksFailed:  tasksFailed,
		TasksPending: tasksPending,
	}

	json.NewEncoder(w).Encode(response)
}

func (m *Master) handleJobResults(w http.ResponseWriter, r *http.Request, job *types.Job) {
	w.Header().Set("Content-Type", "application/json")

	execution := m.scheduler.GetJobExecution(job.ID)

	var outputPaths []string
	if execution != nil {
		for _, path := range execution.OutputPaths {
			outputPaths = append(outputPaths, path)
		}
	}

	response := types.JobResultsResponse{
		JobID:       job.ID,
		Status:      job.Status,
		OutputPaths: outputPaths,
	}

	json.NewEncoder(w).Encode(response)
}

func (m *Master) handleTaskResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var result types.TaskResult
	if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	m.scheduler.HandleTaskResult(result)

	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

// ============================================================================
// WORKERS LIST
// ============================================================================

func (m *Master) handleWorkersList(w http.ResponseWriter, r *http.Request) {
	m.workersMutex.RLock()
	workers := make([]types.WorkerInfo, 0, len(m.workers))
	for _, worker := range m.workers {
		workers = append(workers, *worker)
	}
	m.workersMutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workers)
}

// ============================================================================
// METRICS
// ============================================================================

func (m *Master) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m.workersMutex.RLock()
	workersCount := len(m.workers)
	m.workersMutex.RUnlock()

	m.jobsMutex.RLock()
	jobsCount := len(m.jobs)
	m.jobsMutex.RUnlock()

	metrics := map[string]interface{}{
		"timestamp":     time.Now(),
		"workers_total": workersCount,
		"jobs_total":    jobsCount,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (m *Master) handleDetailedMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	m.workersMutex.RLock()
	workers := make([]map[string]interface{}, 0)
	workersIdle := 0
	workersBusy := 0
	workersDown := 0
	totalActiveTasks := 0

	for _, worker := range m.workers {
		workers = append(workers, map[string]interface{}{
			"id":             worker.ID,
			"status":         worker.Status,
			"active_tasks":   worker.ActiveTasks,
			"total_tasks":    worker.TotalTasks,
			"last_heartbeat": worker.LastHeartbeat,
		})

		switch worker.Status {
		case "IDLE":
			workersIdle++
		case "BUSY":
			workersBusy++
			totalActiveTasks += worker.ActiveTasks
		case "DOWN":
			workersDown++
		}
	}
	m.workersMutex.RUnlock()

	m.jobsMutex.RLock()
	jobs := make([]map[string]interface{}, 0)
	jobsRunning := 0
	jobsCompleted := 0
	jobsFailed := 0

	for _, job := range m.jobs {
		execution := m.scheduler.GetJobExecution(job.ID)
		var tasksTotal, tasksDone, tasksFailed int

		if execution != nil {
			execution.TasksMutex.RLock()
			tasksTotal = len(execution.Tasks)
			for _, task := range execution.Tasks {
				if task.Status == "COMPLETED" {
					tasksDone++
				} else if task.Status == "FAILED" {
					tasksFailed++
				}
			}
			execution.TasksMutex.RUnlock()
		}

		jobs = append(jobs, map[string]interface{}{
			"id":           job.ID,
			"name":         job.Name,
			"status":       job.Status,
			"tasks_total":  tasksTotal,
			"tasks_done":   tasksDone,
			"tasks_failed": tasksFailed,
			"submitted_at": job.SubmittedAt,
			"completed_at": job.CompletedAt,
		})

		switch job.Status {
		case "RUNNING":
			jobsRunning++
		case "COMPLETED":
			jobsCompleted++
		case "FAILED":
			jobsFailed++
		}
	}
	m.jobsMutex.RUnlock()

	metrics := map[string]interface{}{
		"timestamp": time.Now(),
		"workers": map[string]interface{}{
			"total": len(workers),
			"idle":  workersIdle,
			"busy":  workersBusy,
			"down":  workersDown,
			"list":  workers,
		},
		"jobs": map[string]interface{}{
			"total":     len(jobs),
			"running":   jobsRunning,
			"completed": jobsCompleted,
			"failed":    jobsFailed,
			"list":      jobs,
		},
		"system": map[string]interface{}{
			"total_active_tasks": totalActiveTasks,
		},
	}

	json.NewEncoder(w).Encode(metrics)
}

func (m *Master) handleDashboard(w http.ResponseWriter, r *http.Request) {
	// No servir dashboard para rutas de API
	if len(r.URL.Path) > 1 && r.URL.Path != "/" {
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	html := getDashboardHTML()
	fmt.Fprint(w, html)
}

func getDashboardHTML() string {
	return `<!DOCTYPE html>
<html>
<head>
    <title>PSO Batch - Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 {
            color: white;
            margin-bottom: 30px;
            text-align: center;
            font-size: 2.5em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .card {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            transition: transform 0.3s, box-shadow 0.3s;
        }
        .card:hover { transform: translateY(-5px); box-shadow: 0 15px 40px rgba(0,0,0,0.3); }
        .card-title { font-size: 0.9em; color: #999; text-transform: uppercase; letter-spacing: 1px; }
        .card-value { font-size: 2.5em; font-weight: bold; color: #667eea; margin: 10px 0; }
        .card-detail { font-size: 0.85em; color: #666; }
        .status-idle { color: #4caf50; }
        .status-busy { color: #ff9800; }
        .status-down { color: #f44336; }
        .list { list-style: none; }
        .list-item {
            padding: 10px;
            border-bottom: 1px solid #f0f0f0;
            font-size: 0.9em;
            color: #333;
        }
        .list-item:last-child { border-bottom: none; }
        .job-name { font-weight: bold; color: #667eea; }
        .job-status { 
            display: inline-block;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 0.8em;
            font-weight: bold;
            margin-left: 10px;
        }
        .status-running { background: #ff9800; color: white; }
        .status-completed { background: #4caf50; color: white; }
        .status-failed { background: #f44336; color: white; }
        .refresh-info {
            text-align: center;
            color: white;
            margin-top: 30px;
            font-size: 0.9em;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 PSO Batch - Dashboard</h1>
        
        <div class="grid" id="metrics"></div>
        
        <div class="card">
            <h2 style="margin-bottom: 15px; color: #667eea;">👷 Workers Activos</h2>
            <ul class="list" id="workers-list"></ul>
        </div>
        
        <div class="card">
            <h2 style="margin-bottom: 15px; color: #667eea;">📋 Jobs Recientes</h2>
            <ul class="list" id="jobs-list"></ul>
        </div>
        
        <div class="refresh-info">
            Actualizar: <span id="update-time"></span> | Próxima actualización en <span id="countdown">5</span>s
        </div>
    </div>

    <script>
        async function updateDashboard() {
            try {
                const resp = await fetch('/api/v1/metrics/detailed');
                const data = await resp.json();
                
                document.getElementById('update-time').textContent = new Date().toLocaleTimeString();
                
                // Métricas
                const metrics = document.getElementById('metrics');
                metrics.innerHTML = '';
                
                const workerTotal = data.workers.total;
                const workerIdle = data.workers.idle;
                const workerBusy = data.workers.busy;
                const workerDown = data.workers.down;
                
                metrics.innerHTML += createCard('👷 Workers Total', workerTotal, '');
                metrics.innerHTML += createCard('💤 Idle', workerIdle, 'status-idle');
                metrics.innerHTML += createCard('⚙️ Busy', workerBusy, 'status-busy');
                metrics.innerHTML += createCard('💀 Down', workerDown, 'status-down');
                
                metrics.innerHTML += createCard('📋 Jobs Running', data.jobs.running, '');
                metrics.innerHTML += createCard('✅ Jobs Done', data.jobs.completed, '');
                metrics.innerHTML += createCard('❌ Jobs Failed', data.jobs.failed, '');
                metrics.innerHTML += createCard('⚙️ Active Tasks', data.system.total_active_tasks, '');
                
                // Workers
                const workersList = document.getElementById('workers-list');
                workersList.innerHTML = data.workers.list
                    .map(w => '<li class="list-item">' + w.id + ' <span class="' + 
                        (w.status === 'IDLE' ? 'status-idle' : w.status === 'BUSY' ? 'status-busy' : 'status-down') + 
                        '">' + w.status + '</span> (' + w.active_tasks + '/' + w.total_tasks + ' tasks)</li>')
                    .join('');
                
                // Jobs
                const jobsList = document.getElementById('jobs-list');
                jobsList.innerHTML = data.jobs.list.slice(0, 10)
                    .map(j => '<li class="list-item"><span class="job-name">' + j.name + 
                        '</span><span class="job-status status-' + j.status.toLowerCase() + '">' + 
                        j.status + '</span><br><small style="color: #999;">Tasks: ' + j.tasks_done + 
                        '/' + j.tasks_total + '</small></li>')
                    .join('');
                
                updateCountdown();
            } catch (e) {
                console.error('Error:', e);
            }
        }
        
        function createCard(title, value, className) {
            return '<div class="card"><div class="card-title">' + title + 
                '</div><div class="card-value ' + className + '">' + value + 
                '</div></div>';
        }
        
        let countdown = 5;
        function updateCountdown() {
            countdown = 5;
            updateCountdownDisplay();
        }
        
        function updateCountdownDisplay() {
            document.getElementById('countdown').textContent = countdown;
            if (countdown > 0) {
                countdown--;
                setTimeout(updateCountdownDisplay, 1000);
            } else {
                updateDashboard();
            }
        }
        
        updateDashboard();
    </script>
</body>
</html>`
}
