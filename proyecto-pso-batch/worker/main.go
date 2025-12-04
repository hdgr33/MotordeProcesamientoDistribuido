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
		log.Fatalf("ERROR: Failed to register worker: %v", err)
	}

	// Iniciar envío de heartbeats
	go worker.sendHeartbeats()

	// Configurar servidor HTTP para recibir tareas
	http.HandleFunc(protocol.EndpointWorkerExecuteTask, worker.handleExecuteTask)

	log.Printf("INFO: Worker %s starting on port %s", workerID, port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("ERROR: Failed to start server: %v", err)
	}
}

// ============================================================================
// REGISTRATION & HEARTBEATS
// ============================================================================

func (w *Worker) register() error {
	// En Docker, los contenedores se pueden alcanzar por su nombre en la red
	// Ejemplo: worker-1 -> http://worker-1:9001
	workerAddress := fmt.Sprintf("http://%s:%s", w.id, w.port)

	payload := map[string]string{
		"worker_id": w.id,
		"address":   workerAddress,
	}

	body, _ := json.Marshal(payload)

	log.Printf("INFO: Registering with address: %s", workerAddress)

	resp, err := http.Post(
		w.masterURL+protocol.EndpointWorkerRegister,
		"application/json",
		bytes.NewBuffer(body),
	)

	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("master rejected registration: status %d", resp.StatusCode)
	}

	log.Printf("INFO: Successfully registered with master at %s", w.masterURL)
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
			log.Printf("WARN: Failed to send heartbeat: %v", err)
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
		http.Error(wr, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var assignment types.TaskAssignment
	if err := json.NewDecoder(r.Body).Decode(&assignment); err != nil {
		http.Error(wr, "Invalid JSON", http.StatusBadRequest)
		return
	}

	log.Printf("INFO: Task received: %s (operation: %s)", assignment.Task.ID, assignment.Task.Operation)

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

	log.Printf("INFO: Executing task %s (op: %s)", task.ID, task.Operation)

	// Ejecutar operador según el tipo
	records, err := w.runOperator(&task)

	var result types.TaskResult

	if err != nil {
		log.Printf("ERROR: Task %s failed: %v", task.ID, err)
		result = types.TaskResult{
			TaskID:      task.ID,
			Status:      "FAILED",
			Error:       err.Error(),
			Duration:    time.Since(startTime).Seconds(),
			CompletedAt: time.Now(),
		}
	} else {
		// Escribir resultados
		if err := writeRecordsToFile(task.OutputPath, records); err != nil {
			log.Printf("ERROR: Failed to write results: %v", err)
			result = types.TaskResult{
				TaskID:      task.ID,
				Status:      "FAILED",
				Error:       fmt.Sprintf("failed to write output: %v", err),
				Duration:    time.Since(startTime).Seconds(),
				CompletedAt: time.Now(),
			}
		} else {
			result = types.TaskResult{
				TaskID:           task.ID,
				Status:           "COMPLETED",
				OutputPath:       task.OutputPath,
				RecordsProcessed: len(records),
				Duration:         time.Since(startTime).Seconds(),
				CompletedAt:      time.Now(),
			}
			log.Printf("INFO: Task %s completed: %d records processed in %.2fs",
				task.ID, len(records), time.Since(startTime).Seconds())
		}
	}

	// Reportar resultado al master
	if err := w.reportResult(assignment.MasterURL, result); err != nil {
		log.Printf("ERROR: Failed to report result: %v", err)
	}
}

func (w *Worker) runOperator(task *types.Task) ([]types.Record, error) {
	switch task.Operation {
	case "read_csv":
		return operatorReadCSV(task)

	case "map":
		input, err := w.readInputs(task)
		if err != nil {
			return nil, err
		}
		return operatorMap(task, input)

	case "filter":
		input, err := w.readInputs(task)
		if err != nil {
			return nil, err
		}
		return operatorFilter(task, input)

	case "flat_map":
		input, err := w.readInputs(task)
		if err != nil {
			return nil, err
		}
		return operatorFlatMap(task, input)

	case "reduce_by_key":
		input, err := w.readInputs(task)
		if err != nil {
			return nil, err
		}
		return operatorReduceByKey(task, input)

	case "join":
		var inputs [][]types.Record
		for _, path := range task.InputPaths {
			records, err := readRecordsFromFile(path)
			if err != nil {
				return nil, fmt.Errorf("failed to read input %s: %w", path, err)
			}
			inputs = append(inputs, records)
		}
		return operatorJoin(task, inputs)

	default:
		return nil, fmt.Errorf("unsupported operator: %s", task.Operation)
	}
}

func (w *Worker) readInputs(task *types.Task) ([]types.Record, error) {
	if len(task.InputPaths) == 0 {
		return nil, fmt.Errorf("no input paths provided")
	}

	var allRecords []types.Record
	for _, path := range task.InputPaths {
		records, err := readRecordsFromFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", path, err)
		}
		allRecords = append(allRecords, records...)
	}

	return allRecords, nil
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
		return fmt.Errorf("master responded with status %d", resp.StatusCode)
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
