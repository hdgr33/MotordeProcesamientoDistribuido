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
