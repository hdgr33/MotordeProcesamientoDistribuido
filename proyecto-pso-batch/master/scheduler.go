// master/scheduler.go
// ============================================================================
// MONITOREO DE FALLOS
// ============================================================================
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/protocol"
	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/types"
)

func (s *Scheduler) monitorTaskTimeouts() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.jobsMutex.RLock()
		for _, execution := range s.jobs {
			execution.TasksMutex.RLock()
			for _, task := range execution.Tasks {
				// Si la tarea está RUNNING hace más de 5 minutos, es timeout
				if task.Status == "RUNNING" && task.StartedAt != nil {
					elapsed := time.Since(*task.StartedAt)
					if elapsed > 5*time.Minute {
						log.Printf("⏱️  Timeout detectado en tarea %s (elapsed: %v)", task.ID, elapsed)
						execution.TasksMutex.RUnlock()

						// Cambiar a failed para que se reintente
						task.Status = "FAILED"
						task.Error = "Timeout después de 5 minutos"
						s.retryTask(task)

						execution.TasksMutex.Lock()
						continue
					}
				}
			}
			execution.TasksMutex.RUnlock()
		}
		s.jobsMutex.RUnlock()
	}
}

func (s *Scheduler) monitorWorkerFailures() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Obtener workers caídos
		s.master.workersMutex.RLock()
		downWorkers := make(map[string]bool)
		for id, worker := range s.master.workers {
			if worker.Status == "DOWN" {
				downWorkers[id] = true
			}
		}
		s.master.workersMutex.RUnlock()

		if len(downWorkers) == 0 {
			continue
		}

		log.Printf("⚠️  Workers caídos detectados: %v", downWorkers)

		// Replanificar tareas de workers caídos
		s.jobsMutex.RLock()
		for _, execution := range s.jobs {
			execution.TasksMutex.Lock()
			for _, task := range execution.Tasks {
				if downWorkers[task.AssignedTo] && task.Status == "RUNNING" {
					log.Printf("🔄 Replanificando tarea %s (worker %s está DOWN)", task.ID, task.AssignedTo)
					task.Status = "PENDING"
					task.AssignedTo = ""
					s.taskQueue <- task
				}
			}
			execution.TasksMutex.Unlock()
		}
		s.jobsMutex.RUnlock()
	}
}

// ============================================================================
// UTILITIES
// ============================================================================

func (s *Scheduler) findNode(nodes []types.Node, nodeID string) *types.Node {
	for i := range nodes {
		if nodes[i].ID == nodeID {
			return &nodes[i]
		}
	}
	return nil
}

// ============================================================================
// SCHEDULER STRUCT Y INIT
// ============================================================================

type Scheduler struct {
	master    *Master
	jobs      map[string]*JobExecution
	jobsMutex sync.RWMutex
	taskQueue chan *types.Task
	stopChan  chan struct{}
}

type JobExecution struct {
	Job          *types.Job
	Tasks        map[string]*types.Task
	TasksMutex   sync.RWMutex
	Stages       [][]string
	CurrentStage int
	OutputPaths  map[string]string
}

func NewScheduler(master *Master) *Scheduler {
	return &Scheduler{
		master:    master,
		jobs:      make(map[string]*JobExecution),
		taskQueue: make(chan *types.Task, 1000),
		stopChan:  make(chan struct{}),
	}
}

func (s *Scheduler) Start() {
	log.Println("📋 Scheduler iniciado")

	// Worker que procesa la cola de tareas
	go s.processTaskQueue()

	// Monitorear timeouts de tareas
	go s.monitorTaskTimeouts()

	// Monitorear fallos de workers
	go s.monitorWorkerFailures()
}

func (s *Scheduler) Stop() {
	close(s.stopChan)
	log.Println("📋 Scheduler detenido")
}

// ============================================================================
// JOB SCHEDULING
// ============================================================================

func (s *Scheduler) ScheduleJob(job *types.Job) error {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	log.Printf("📋 Planificando job %s (%s)...", job.ID, job.Name)

	// Crear ejecución del job
	execution := &JobExecution{
		Job:         job,
		Tasks:       make(map[string]*types.Task),
		OutputPaths: make(map[string]string),
	}

	// Analizar DAG y determinar stages
	stages, err := s.analyzeDAG(&job.DAG)
	if err != nil {
		return fmt.Errorf("error analizando DAG: %w", err)
	}
	execution.Stages = stages

	log.Printf("📊 DAG analizado: %d stages", len(stages))
	for i, stage := range stages {
		log.Printf("  Stage %d: %v", i, stage)
	}

	s.jobs[job.ID] = execution

	// Cambiar estado del job
	job.Status = "RUNNING"

	// Iniciar ejecución del primer stage
	go s.executeStage(job.ID, 0)

	return nil
}

// ============================================================================
// DAG ANALYSIS
// ============================================================================

func (s *Scheduler) analyzeDAG(dag *types.DAG) ([][]string, error) {
	// Construir mapa de dependencias
	dependencies := make(map[string][]string)
	inDegree := make(map[string]int)

	// Inicializar todos los nodos
	for _, node := range dag.Nodes {
		dependencies[node.ID] = []string{}
		inDegree[node.ID] = 0
	}

	// Construir grafo de dependencias
	for _, edge := range dag.Edges {
		if len(edge) != 2 {
			return nil, fmt.Errorf("edge inválido: %v", edge)
		}
		from, to := edge[0], edge[1]
		dependencies[to] = append(dependencies[to], from)
		inDegree[to]++
	}

	// Topological sort por niveles (stages)
	var stages [][]string
	processed := make(map[string]bool)

	for len(processed) < len(dag.Nodes) {
		// Encontrar nodos sin dependencias no procesadas
		var currentStage []string
		for nodeID := range inDegree {
			if processed[nodeID] {
				continue
			}

			// Verificar si todas las dependencias están procesadas
			allDepsProcessed := true
			for _, dep := range dependencies[nodeID] {
				if !processed[dep] {
					allDepsProcessed = false
					break
				}
			}

			if allDepsProcessed {
				currentStage = append(currentStage, nodeID)
			}
		}

		if len(currentStage) == 0 {
			return nil, fmt.Errorf("ciclo detectado en el DAG o nodos huérfanos")
		}

		stages = append(stages, currentStage)

		// Marcar como procesados
		for _, nodeID := range currentStage {
			processed[nodeID] = true
		}
	}

	return stages, nil
}

// ============================================================================
// STAGE EXECUTION (CORREGIDO CON WAITGROUP)
// ============================================================================

func (s *Scheduler) executeStage(jobID string, stageIdx int) {
	s.jobsMutex.RLock()
	execution, exists := s.jobs[jobID]
	s.jobsMutex.RUnlock()

	if !exists {
		log.Printf("❌ Job %s no encontrado", jobID)
		return
	}

	if stageIdx >= len(execution.Stages) {
		log.Printf("✨ Todos los stages completados para job %s", jobID)
		s.completeJob(jobID)
		return
	}

	stage := execution.Stages[stageIdx]
	log.Printf("▶️  Ejecutando Stage %d de job %s: %v (total stages: %d)", stageIdx, jobID, stage, len(execution.Stages))

	execution.CurrentStage = stageIdx

	// Crear tareas para cada nodo en el stage
	var wg sync.WaitGroup
	var stageErrors []error
	var errorMutex sync.Mutex

	if len(stage) == 0 {
		log.Printf("⚠️  Stage %d está vacío", stageIdx)
		// Si el stage está vacío, avanzar al siguiente
		s.executeStage(jobID, stageIdx+1)
		return
	}

	// Crear todas las tareas del stage
	for _, nodeID := range stage {
		node := s.findNode(execution.Job.DAG.Nodes, nodeID)
		if node == nil {
			log.Printf("❌ Nodo %s no encontrado en DAG", nodeID)
			continue
		}

		// Determinar particiones basado en el nodo
		partitions := 1
		if node.Partitions > 0 {
			partitions = node.Partitions
		} else if execution.Job.Parallelism > 0 {
			partitions = execution.Job.Parallelism
		}

		log.Printf("   Creando %d tareas para nodo %s", partitions, nodeID)

		// Crear una tarea por partición
		for p := 0; p < partitions; p++ {
			wg.Add(1)

			go func(nodeID string, partition int) {
				defer wg.Done()

				task := s.createTask(execution, nodeID, partition)

				execution.TasksMutex.Lock()
				execution.Tasks[task.ID] = task
				execution.TasksMutex.Unlock()

				log.Printf("   📤 Cola: %s", task.ID)
				s.taskQueue <- task

				// Esperar a que complete con timeout robusto
				if err := s.waitForTaskCompletion(jobID, task.ID, execution, 10*time.Minute); err != nil {
					log.Printf("   ❌ Tarea %s error: %v", task.ID, err)
					errorMutex.Lock()
					stageErrors = append(stageErrors, err)
					errorMutex.Unlock()
				} else {
					log.Printf("   ✅ Tarea %s completó", task.ID)
				}
			}(nodeID, p)
		}
	}

	// Esperar a que TODAS las goroutines del stage terminen
	log.Printf("⏳ Esperando que %d goroutines del stage completen...", len(stage))
	wg.Wait()

	// Verificar si hubo errores
	if len(stageErrors) > 0 {
		log.Printf("❌ Stage %d falló con %d errores", stageIdx, len(stageErrors))
		s.failJob(jobID, fmt.Sprintf("Errores en stage %d: %d tareas fallidas", stageIdx, len(stageErrors)))
		return
	}

	log.Printf("✅ Stage %d de job %s completado - Avanzando a stage %d", stageIdx, jobID, stageIdx+1)

	// Ejecutar siguiente stage (RECURSIVO)
	s.executeStage(jobID, stageIdx+1)
}

// ============================================================================
// TASK COMPLETION MONITORING (MEJORADO)
// ============================================================================

// waitForTaskCompletion espera a que una tarea complete con un mecanismo robusto
// Polling periódico en lugar de espera indefinida
func (s *Scheduler) waitForTaskCompletion(jobID string, taskID string,
	execution *JobExecution, timeout time.Duration) error {

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond) // Poll más frecuente
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			execution.TasksMutex.RLock()
			task, exists := execution.Tasks[taskID]
			execution.TasksMutex.RUnlock()

			if !exists {
				return fmt.Errorf("tarea no encontrada: %s", taskID)
			}

			// Verificar estado actual
			switch task.Status {
			case "COMPLETED":
				return nil // ✅ Éxito

			case "FAILED":
				if task.AttemptNum >= MaxRetries {
					return fmt.Errorf("tarea falló después de %d intentos", MaxRetries)
				}
				// Si aún hay reintentos, esperar a que se reencolé
				// (handleTaskResult() la reencolará automáticamente)

			case "PENDING", "RUNNING":
				// Seguir esperando

			case "TIMEOUT":
				return fmt.Errorf("tarea expiró por timeout")
			}

			// Verificar timeout global
			if time.Now().After(deadline) {
				// Marcar como timeout y permitir reintento
				execution.TasksMutex.Lock()
				task.Status = "TIMEOUT"
				task.Error = "Timeout global alcanzado"
				execution.TasksMutex.Unlock()

				log.Printf("⏱️  Timeout para tarea %s (intento %d/%d)",
					taskID, task.AttemptNum, MaxRetries)

				if task.AttemptNum >= MaxRetries {
					return fmt.Errorf("tarea excedió timeout después de %d reintentos", MaxRetries)
				}

				// Reintentar
				s.retryTask(task)
				return nil // No es error fatal, el reintento continuará
			}

		case <-s.stopChan:
			return fmt.Errorf("scheduler detenido")
		}
	}
}

// ============================================================================
// TASK MANAGEMENT
// ============================================================================

func (s *Scheduler) createTask(execution *JobExecution, nodeID string, partition int) *types.Task {
	node := s.findNode(execution.Job.DAG.Nodes, nodeID)

	taskID := fmt.Sprintf("%s-%s-p%d", execution.Job.ID, nodeID, partition)

	// Determinar input paths basado en dependencias
	var inputPaths []string

	// Para JOIN: buscar TODOS los nodos que alimentan este nodo
	if node.Operation == "join" {
		for _, edge := range execution.Job.DAG.Edges {
			if len(edge) == 2 && edge[1] == nodeID {
				// Este nodo depende de edge[0]
				if outputPath, exists := execution.OutputPaths[edge[0]]; exists {
					inputPaths = append(inputPaths, outputPath)
				}
			}
		}
	} else {
		// Para otros operadores, buscar dependencias
		for _, edge := range execution.Job.DAG.Edges {
			if len(edge) == 2 && edge[1] == nodeID {
				if outputPath, exists := execution.OutputPaths[edge[0]]; exists {
					inputPaths = append(inputPaths, outputPath)
				}
			}
		}
	}

	// Para nodos read_csv, usar el path especificado
	if node.Operation == "read_csv" && node.Path != "" {
		inputPaths = []string{node.Path}
	}

	outputPath := fmt.Sprintf("data/output/%s.json", taskID)

	task := &types.Task{
		ID:         taskID,
		JobID:      execution.Job.ID,
		NodeID:     nodeID,
		Operation:  node.Operation,
		Function:   node.Function,
		Key:        node.Key,
		InputPaths: inputPaths,
		OutputPath: outputPath,
		Partition:  partition,
		Params:     node.Params,
		Status:     "PENDING",
		AttemptNum: 0,
	}

	return task
}

func (s *Scheduler) processTaskQueue() {
	for {
		select {
		case task := <-s.taskQueue:
			s.assignTask(task)
		case <-s.stopChan:
			return
		}
	}
}

func (s *Scheduler) assignTask(task *types.Task) {
	// Encontrar worker disponible
	worker := s.selectWorker()
	if worker == nil {
		log.Printf("⚠️  No hay workers disponibles, reencolando tarea %s", task.ID)
		time.Sleep(2 * time.Second)
		s.taskQueue <- task
		return
	}

	log.Printf("📤 Asignando tarea %s a worker %s", task.ID, worker.ID)

	task.Status = "RUNNING"
	task.AssignedTo = worker.ID
	now := time.Now()
	task.StartedAt = &now
	task.AttemptNum++

	// Enviar tarea al worker
	assignment := types.TaskAssignment{
		Task:      *task,
		MasterURL: fmt.Sprintf("http://localhost:%s", s.master.port),
	}

	body, _ := json.Marshal(assignment)
	resp, err := http.Post(
		worker.Address+protocol.EndpointWorkerExecuteTask,
		"application/json",
		bytes.NewBuffer(body),
	)

	if err != nil {
		log.Printf("❌ Error enviando tarea a worker %s: %v", worker.ID, err)
		s.retryTask(task)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != protocol.StatusTaskAccepted {
		log.Printf("❌ Worker %s rechazó tarea: status %d", worker.ID, resp.StatusCode)
		s.retryTask(task)
		return
	}

	log.Printf("✅ Tarea %s asignada a %s", task.ID, worker.ID)

	// Actualizar estado del worker
	s.master.workersMutex.Lock()
	if w, exists := s.master.workers[worker.ID]; exists {
		w.ActiveTasks++
		w.TotalTasks++
	}
	s.master.workersMutex.Unlock()
}

func (s *Scheduler) selectWorker() *types.WorkerInfo {
	s.master.workersMutex.RLock()
	defer s.master.workersMutex.RUnlock()

	var bestWorker *types.WorkerInfo
	minLoad := int(^uint(0) >> 1) // Max int

	for _, worker := range s.master.workers {
		if worker.Status == "DOWN" {
			continue
		}

		if worker.ActiveTasks < minLoad {
			minLoad = worker.ActiveTasks
			bestWorker = worker
		}
	}

	return bestWorker
}

func (s *Scheduler) retryTask(task *types.Task) {
	if task.AttemptNum >= MaxRetries {
		log.Printf("❌ Tarea %s excedió número máximo de reintentos (%d)",
			task.ID, MaxRetries)
		task.Status = "FAILED"
		task.Error = fmt.Sprintf("Máximo número de reintentos excedido (%d)", MaxRetries)
		return
	}

	log.Printf("🔄 Reintentando tarea %s (intento %d/%d)",
		task.ID, task.AttemptNum+1, MaxRetries)

	task.Status = "PENDING"
	task.AssignedTo = ""
	task.StartedAt = nil

	// Esperar un poco antes de reenencolar
	time.Sleep(1 * time.Second)

	s.taskQueue <- task
}

// ============================================================================
// TASK COMPLETION
// ============================================================================

func (s *Scheduler) HandleTaskResult(result types.TaskResult) {
	log.Printf("📥 Resultado recibido de tarea %s: %s", result.TaskID, result.Status)

	// Encontrar la tarea
	var execution *JobExecution
	var task *types.Task

	s.jobsMutex.RLock()
	for _, exec := range s.jobs {
		exec.TasksMutex.RLock()
		if t, exists := exec.Tasks[result.TaskID]; exists {
			task = t
			execution = exec
		}
		exec.TasksMutex.RUnlock()
		if task != nil {
			break
		}
	}
	s.jobsMutex.RUnlock()

	if task == nil {
		log.Printf("⚠️  Tarea %s no encontrada", result.TaskID)
		return
	}

	// Actualizar estado
	execution.TasksMutex.Lock()
	task.Status = result.Status
	task.Error = result.Error
	now := time.Now()
	task.CompletedAt = &now
	execution.TasksMutex.Unlock()

	// Actualizar worker
	if task.AssignedTo != "" {
		s.master.workersMutex.Lock()
		if worker, exists := s.master.workers[task.AssignedTo]; exists {
			worker.ActiveTasks--
		}
		s.master.workersMutex.Unlock()
	}

	// Si falló, reintentar automáticamente
	if result.Status == "FAILED" {
		log.Printf("❌ Tarea %s falló: %s", result.TaskID, result.Error)
		s.retryTask(task)
		return
	}

	// Si completó exitosamente, guardar output path
	if result.Status == "COMPLETED" {
		if execution != nil {
			execution.TasksMutex.Lock()
			execution.OutputPaths[task.NodeID] = result.OutputPath
			execution.TasksMutex.Unlock()
		}
		log.Printf("✅ Tarea %s completada exitosamente", result.TaskID)
	}
}

// ============================================================================
// JOB COMPLETION
// ============================================================================

func (s *Scheduler) completeJob(jobID string) {
	s.jobsMutex.Lock()
	execution, exists := s.jobs[jobID]
	if !exists {
		s.jobsMutex.Unlock()
		return
	}
	s.jobsMutex.Unlock()

	execution.Job.Status = "COMPLETED"
	now := time.Now()
	execution.Job.CompletedAt = &now

	// Actualizar también en el master
	s.master.jobsMutex.Lock()
	if job, exists := s.master.jobs[jobID]; exists {
		job.Status = "COMPLETED"
		job.CompletedAt = &now
	}
	s.master.jobsMutex.Unlock()

	duration := now.Sub(execution.Job.SubmittedAt)
	log.Printf("✨ Job %s completado en %s", jobID, duration)
}

func (s *Scheduler) failJob(jobID string, reason string) {
	s.jobsMutex.Lock()
	execution, exists := s.jobs[jobID]
	if !exists {
		s.jobsMutex.Unlock()
		return
	}
	s.jobsMutex.Unlock()

	execution.Job.Status = "FAILED"
	now := time.Now()
	execution.Job.CompletedAt = &now

	// Actualizar también en el master
	s.master.jobsMutex.Lock()
	if job, exists := s.master.jobs[jobID]; exists {
		job.Status = "FAILED"
		job.CompletedAt = &now
	}
	s.master.jobsMutex.Unlock()

	log.Printf("❌ Job %s falló: %s", jobID, reason)
}

func (s *Scheduler) GetJobExecution(jobID string) *JobExecution {
	s.jobsMutex.RLock()
	defer s.jobsMutex.RUnlock()
	return s.jobs[jobID]
}
