// master/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/protocol"
	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/types"
)

type Master struct {
	workers      map[string]*types.WorkerInfo
	workersMutex sync.RWMutex
	jobs         map[string]*types.Job
	jobsMutex    sync.RWMutex
	port         string
}

func NewMaster(port string) *Master {
	return &Master{
		workers: make(map[string]*types.WorkerInfo),
		jobs:    make(map[string]*types.Job),
		port:    port,
	}
}

func main() {
	port := "8080"
	master := NewMaster(port)

	// Iniciar monitor de heartbeats
	go master.monitorWorkers()

	// Configurar rutas HTTP
	http.HandleFunc(protocol.EndpointWorkerRegister, master.handleWorkerRegister)
	http.HandleFunc(protocol.EndpointWorkerHeartbeat, master.handleHeartbeat)
	http.HandleFunc(protocol.EndpointJobSubmit, master.handleJobSubmit)
	http.HandleFunc("/api/v1/jobs/", master.handleJobRequest) // Catch-all para /jobs/{id}
	http.HandleFunc("/api/v1/workers", master.handleWorkersList)
	http.HandleFunc(protocol.EndpointMetrics, master.handleMetrics)

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
				// TODO: Replanificar tareas asignadas a este worker
			}
		}
		m.workersMutex.Unlock()
	}
}

// ============================================================================
// JOB MANAGEMENT
// ============================================================================

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

	// Generar ID y configurar timestamps
	job.ID = fmt.Sprintf("job-%d", time.Now().Unix())
	job.Status = "PENDING"
	job.SubmittedAt = time.Now()

	// Validación básica
	if len(job.DAG.Nodes) == 0 {
		http.Error(w, "DAG debe tener al menos un nodo", http.StatusBadRequest)
		return
	}

	m.jobsMutex.Lock()
	m.jobs[job.ID] = &job
	m.jobsMutex.Unlock()

	log.Printf("📥 Job recibido: %s (%s) con %d nodos", job.ID, job.Name, len(job.DAG.Nodes))

	// TODO: Iniciar planificación y ejecución del job

	response := types.JobSubmitResponse{
		JobID:   job.ID,
		Status:  "PENDING",
		Message: "Job aceptado para ejecución",
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)
}

func (m *Master) handleJobRequest(w http.ResponseWriter, r *http.Request) {
	// Extraer job_id de la URL
	// Simplificado: asumir formato /api/v1/jobs/{job_id} o /api/v1/jobs/{job_id}/results

	// Por ahora, respuesta dummy
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Endpoint de status/results - TODO implementar",
	})
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

	json.NewEncoder(w).Encode(metrics)
}
