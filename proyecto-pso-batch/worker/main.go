// worker/main.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/protocol"
	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/types"
)

type Worker struct {
	id          string
	port        string
	masterURL   string
	activeTasks int
}

func NewWorker(id, port, masterURL string) *Worker {
	return &Worker{
		id:          id,
		port:        port,
		masterURL:   masterURL,
		activeTasks: 0,
	}
}

func main() {
	// Configuración desde variables de entorno o defaults
	workerID := getEnv("WORKER_ID", fmt.Sprintf("worker-%d", time.Now().Unix()))
	port := getEnv("WORKER_PORT", "9001")
	masterURL := getEnv("MASTER_URL", "http://localhost:8080")

	worker := NewWorker(workerID, port, masterURL)

	// Registrar con el master
	if err := worker.register(); err != nil {
		log.Fatalf("❌ Error registrando worker: %v", err)
	}

	// Iniciar envío de heartbeats
	go worker.sendHeartbeats()

	// Configurar servidor HTTP para recibir tareas
	http.HandleFunc(protocol.EndpointWorkerExecuteTask, worker.handleExecuteTask)

	log.Printf("🚀 Worker %s iniciando en puerto %s...", workerID, port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Error iniciando servidor: %v", err)
	}
}

// ============================================================================
// REGISTRATION & HEARTBEATS
// ============================================================================

func (w *Worker) register() error {
	payload := map[string]string{
		"worker_id": w.id,
		"address":   fmt.Sprintf("http://localhost:%s", w.port),
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(
		w.masterURL+protocol.EndpointWorkerRegister,
		"application/json",
		bytes.NewBuffer(body),
	)

	if err != nil {
		return fmt.Errorf("error conectando con master: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("master rechazó registro: status %d", resp.StatusCode)
	}

	log.Printf("✅ Registrado con master en %s", w.masterURL)
	return nil
}

func (w *Worker) sendHeartbeats() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		hb := types.HeartbeatRequest{
			WorkerID:    w.id,
			ActiveTasks: w.activeTasks,
			MemoryMB:    0, // TODO: obtener memoria real
		}

		body, _ := json.Marshal(hb)
		resp, err := http.Post(
			w.masterURL+protocol.EndpointWorkerHeartbeat,
			"application/json",
			bytes.NewBuffer(body),
		)

		if err != nil {
			log.Printf("⚠️  Error enviando heartbeat: %v", err)
			continue
		}
		resp.Body.Close()
	}
}

// ============================================================================
// TASK EXECUTION
// ============================================================================

func (w *Worker) handleExecuteTask(wr http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(wr, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var assignment types.TaskAssignment
	if err := json.NewDecoder(r.Body).Decode(&assignment); err != nil {
		http.Error(wr, "JSON inválido", http.StatusBadRequest)
		return
	}

	log.Printf("📦 Tarea recibida: %s (operación: %s)", assignment.Task.ID, assignment.Task.Operation)

	// Responder inmediatamente que la tarea fue aceptada
	wr.WriteHeader(protocol.StatusTaskAccepted)
	json.NewEncoder(wr).Encode(map[string]string{
		"status": "ACCEPTED",
	})

	// Ejecutar tarea en goroutine
	go w.executeTask(assignment)
}

func (w *Worker) executeTask(assignment types.TaskAssignment) {
	w.activeTasks++
	defer func() { w.activeTasks-- }()

	task := assignment.Task
	startTime := time.Now()

	log.Printf("▶️  Ejecutando tarea %s...", task.ID)

	// TODO: Implementar ejecución real según task.Operation
	// Por ahora, simulamos trabajo
	time.Sleep(2 * time.Second)

	// Simular resultado exitoso
	result := types.TaskResult{
		TaskID:           task.ID,
		Status:           "COMPLETED",
		OutputPath:       fmt.Sprintf("/tmp/output-%s.json", task.ID),
		RecordsProcessed: 100,
		Duration:         time.Since(startTime).Seconds(),
		CompletedAt:      time.Now(),
	}

	// Reportar resultado al master
	if err := w.reportResult(assignment.MasterURL, result); err != nil {
		log.Printf("❌ Error reportando resultado: %v", err)
	} else {
		log.Printf("✅ Tarea %s completada", task.ID)
	}
}

func (w *Worker) reportResult(masterURL string, result types.TaskResult) error {
	endpoint := fmt.Sprintf(masterURL+protocol.EndpointTaskResult, result.TaskID)
	body, _ := json.Marshal(result)

	resp, err := http.Post(endpoint, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("master respondió con status %d", resp.StatusCode)
	}

	return nil
}

// ============================================================================
// UTILITIES
// ============================================================================

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
