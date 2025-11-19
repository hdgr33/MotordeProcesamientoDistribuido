// master/scheduler.go
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

const (
	MaxRetries = 3
)

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
	Stages       [][]string // Lista de stages (grupos de nodos que pueden ejecutarse en paralelo)
	CurrentStage int
	OutputPaths  map[string]string // nodeID -> output path
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
	log.Println("📅 Scheduler iniciado")

	// Worker que procesa la cola de tareas
	go s.processTaskQueue()
}

func (s *Scheduler) Stop() {
	close(s.stopChan)
	log.Println("📅 Scheduler detenido")
}

// ============================================================================
// JOB SCHEDULING
// ============================================================================

func (s *Scheduler) ScheduleJob(job *types.Job) error {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	log.Printf("📅 Planificando job %s (%s)...", job.ID, job.Name)

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
// STAGE EXECUTION
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
		// Job completado
		s.completeJob(jobID)
		return
	}

	stage := execution.Stages[stageIdx]
	log.Printf("▶️  Ejecutando Stage %d de job %s: %v", stageIdx, jobID, stage)

	execution.CurrentStage = stageIdx

	// Crear tareas para cada nodo en el stage
	var wg sync.WaitGroup
	taskResults := make(chan error, len(stage))

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

		// Crear una tarea por partición
		for p := 0; p < partitions; p++ {
			wg.Add(1)
			go func(nodeID string, partition int) {
				defer wg.Done()

				task := s.createTask(execution, nodeID, partition)
				execution.TasksMutex.Lock()
				execution.Tasks[task.ID] = task
				execution.TasksMutex.Unlock()

				// Encolar tarea
				s.taskQueue <- task

				// Esperar a que complete
				if err := s.waitForTask(task.ID, 5*time.Minute); err != nil {
					taskResults <- err
				} else {
					taskResults <- nil
				}
			}(nodeID, p)
		}
	}

	// Esperar a que todas las tareas del stage completen
	wg.Wait()
	close(taskResults)

	// Verificar si hubo errores
	hasErrors := false
	for err := range taskResults {
		if err != nil {
			log.Printf("❌ Error en stage %d: %v", stageIdx, err)
			hasErrors = true
		}
	}

	if hasErrors {
		s.failJob(jobID, "Errores en stage de ejecución")
		return
	}

	log.Printf("✅ Stage %d de job %s completado", stageIdx, jobID)

	// Ejecutar siguiente stage
	s.executeStage(jobID, stageIdx+1)
}

// ============================================================================
// TASK MANAGEMENT
// ============================================================================

func (s *Scheduler) createTask(execution *JobExecution, nodeID string, partition int) *types.Task {
	node := s.findNode(execution.Job.DAG.Nodes, nodeID)

	taskID := fmt.Sprintf("%s-%s-p%d", execution.Job.ID, nodeID, partition)

	// Determinar input paths basado en dependencias
	var inputPaths []string
	for _, edge := range execution.Job.DAG.Edges {
		if len(edge) == 2 && edge[1] == nodeID {
			// Este nodo depende de edge[0]
			if outputPath, exists := execution.OutputPaths[edge[0]]; exists {
				inputPaths = append(inputPaths, outputPath)
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
		Key:        node.Key, // Agregar Key
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
		log.Printf("❌ Tarea %s excedió número máximo de reintentos", task.ID)
		task.Status = "FAILED"
		task.Error = "Máximo número de reintentos excedido"
		return
	}

	log.Printf("🔄 Reintentando tarea %s (intento %d/%d)", task.ID, task.AttemptNum, MaxRetries)

	task.Status = "PENDING"
	time.Sleep(2 * time.Second)
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
	task.Status = result.Status
	task.Error = result.Error
	now := time.Now()
	task.CompletedAt = &now

	// Actualizar worker
	if task.AssignedTo != "" {
		s.master.workersMutex.Lock()
		if worker, exists := s.master.workers[task.AssignedTo]; exists {
			worker.ActiveTasks--
		}
		s.master.workersMutex.Unlock()
	}

	// Si falló, reintentar
	if result.Status == "FAILED" {
		log.Printf("❌ Tarea %s falló: %s", result.TaskID, result.Error)
		s.retryTask(task)
		return
	}

	// Guardar output path
	execution.OutputPaths[task.NodeID] = result.OutputPath

	log.Printf("✅ Tarea %s completada exitosamente", result.TaskID)
}

func (s *Scheduler) waitForTask(taskID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Buscar tarea
		s.jobsMutex.RLock()
		var task *types.Task
		for _, exec := range s.jobs {
			exec.TasksMutex.RLock()
			if t, exists := exec.Tasks[taskID]; exists {
				task = t
			}
			exec.TasksMutex.RUnlock()
			if task != nil {
				break
			}
		}
		s.jobsMutex.RUnlock()

		if task == nil {
			return fmt.Errorf("tarea no encontrada")
		}

		if task.Status == "COMPLETED" {
			return nil
		}

		if task.Status == "FAILED" && task.AttemptNum >= MaxRetries {
			return fmt.Errorf("tarea falló después de %d intentos", MaxRetries)
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("timeout esperando tarea")
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

	log.Printf("❌ Job %s falló: %s", jobID, reason)
}

func (s *Scheduler) GetJobExecution(jobID string) *JobExecution {
	s.jobsMutex.RLock()
	defer s.jobsMutex.RUnlock()
	return s.jobs[jobID]
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
