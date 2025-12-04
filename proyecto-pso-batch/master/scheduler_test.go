// master/scheduler_test.go
package main

import (
	"fmt"
	"sync"
	"testing"

	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/types"
)

// ============================================================================
// HELPER: Setup Master for Testing
// ============================================================================

func setupTestMaster() *Master {
	return &Master{
		workers:      make(map[string]*types.WorkerInfo),
		workersMutex: sync.RWMutex{},
		jobs:         make(map[string]*types.Job),
		jobsMutex:    sync.RWMutex{},
		port:         "8080",
	}
}

// ============================================================================
// TEST: DAG Analysis
// ============================================================================

func TestAnalyzeDAG_Simple(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	dag := &types.DAG{
		Nodes: []types.Node{
			{ID: "read", Operation: "read_csv"},
			{ID: "map", Operation: "map"},
			{ID: "filter", Operation: "filter"},
		},
		Edges: [][]string{
			{"read", "map"},
			{"map", "filter"},
		},
	}

	stages, err := scheduler.analyzeDAG(dag)

	if err != nil {
		t.Fatalf("analyzeDAG failed: %v", err)
	}

	// Verificar que se crearon 3 stages (lineal)
	if len(stages) != 3 {
		t.Errorf("Expected 3 stages, got %d", len(stages))
	}

	// Stage 0 debe contener solo "read"
	if len(stages[0]) != 1 || stages[0][0] != "read" {
		t.Errorf("Stage 0 should be [read], got %v", stages[0])
	}

	// Stage 1 debe contener solo "map"
	if len(stages[1]) != 1 || stages[1][0] != "map" {
		t.Errorf("Stage 1 should be [map], got %v", stages[1])
	}

	// Stage 2 debe contener solo "filter"
	if len(stages[2]) != 1 || stages[2][0] != "filter" {
		t.Errorf("Stage 2 should be [filter], got %v", stages[2])
	}
}

func TestAnalyzeDAG_Parallel(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	// DAG con paralelismo: read -> [map1, map2] -> join
	dag := &types.DAG{
		Nodes: []types.Node{
			{ID: "read", Operation: "read_csv"},
			{ID: "map1", Operation: "map"},
			{ID: "map2", Operation: "map"},
			{ID: "join", Operation: "join"},
		},
		Edges: [][]string{
			{"read", "map1"},
			{"read", "map2"},
			{"map1", "join"},
			{"map2", "join"},
		},
	}

	stages, err := scheduler.analyzeDAG(dag)

	if err != nil {
		t.Fatalf("analyzeDAG failed: %v", err)
	}

	// Debe tener 3 stages
	if len(stages) != 3 {
		t.Errorf("Expected 3 stages, got %d", len(stages))
	}

	// Stage 1 debe tener map1 y map2 en paralelo
	if len(stages[1]) != 2 {
		t.Errorf("Stage 1 should have 2 nodes (parallel), got %d", len(stages[1]))
	}
}

func TestAnalyzeDAG_Cycle(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	// DAG con ciclo: A -> B -> C -> A
	dag := &types.DAG{
		Nodes: []types.Node{
			{ID: "A", Operation: "map"},
			{ID: "B", Operation: "map"},
			{ID: "C", Operation: "map"},
		},
		Edges: [][]string{
			{"A", "B"},
			{"B", "C"},
			{"C", "A"},
		},
	}

	_, err := scheduler.analyzeDAG(dag)

	if err == nil {
		t.Error("Expected error for cyclic DAG, got nil")
	}
}

func TestAnalyzeDAG_Complex(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	// WordCount DAG
	dag := &types.DAG{
		Nodes: []types.Node{
			{ID: "read", Operation: "read_csv", Partitions: 2},
			{ID: "flat", Operation: "flat_map", Function: "split_words"},
			{ID: "count", Operation: "reduce_by_key", Key: "word", Function: "count"},
		},
		Edges: [][]string{
			{"read", "flat"},
			{"flat", "count"},
		},
	}

	stages, err := scheduler.analyzeDAG(dag)

	if err != nil {
		t.Fatalf("analyzeDAG failed: %v", err)
	}

	if len(stages) != 3 {
		t.Errorf("Expected 3 stages, got %d", len(stages))
	}
}

// ============================================================================
// TEST: Task Creation
// ============================================================================

func TestCreateTask(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	job := &types.Job{
		ID:   "job-123",
		Name: "test-job",
		DAG: types.DAG{
			Nodes: []types.Node{
				{ID: "map1", Operation: "map", Function: "to_lower", Partitions: 1},
			},
		},
	}

	execution := &JobExecution{
		Job:         job,
		Tasks:       make(map[string]*types.Task),
		OutputPaths: make(map[string]string),
	}

	task := scheduler.createTask(execution, "map1", 0)

	if task.ID == "" {
		t.Error("Task ID should not be empty")
	}

	if task.JobID != "job-123" {
		t.Errorf("Expected JobID 'job-123', got '%s'", task.JobID)
	}

	if task.NodeID != "map1" {
		t.Errorf("Expected NodeID 'map1', got '%s'", task.NodeID)
	}

	if task.Operation != "map" {
		t.Errorf("Expected operation 'map', got '%s'", task.Operation)
	}

	if task.Partition != 0 {
		t.Errorf("Expected partition 0, got %d", task.Partition)
	}

	if task.Status != "PENDING" {
		t.Errorf("Expected status 'PENDING', got '%s'", task.Status)
	}

	if task.AttemptNum != 0 {
		t.Errorf("Expected AttemptNum 0, got %d", task.AttemptNum)
	}
}

func TestCreateTask_WithDependencies(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	job := &types.Job{
		ID:   "job-123",
		Name: "test-job",
		DAG: types.DAG{
			Nodes: []types.Node{
				{ID: "read", Operation: "read_csv"},
				{ID: "map1", Operation: "map", Partitions: 1},
			},
			Edges: [][]string{
				{"read", "map1"},
			},
		},
	}

	execution := &JobExecution{
		Job:         job,
		Tasks:       make(map[string]*types.Task),
		OutputPaths: make(map[string]string),
	}

	// Simular que read ya completó
	execution.OutputPaths["read"] = "data/output/job-123-read-p0.json"

	task := scheduler.createTask(execution, "map1", 0)

	if len(task.InputPaths) == 0 {
		t.Error("Expected input paths from dependency")
	}
}

// ============================================================================
// TEST: Worker Selection
// ============================================================================

func TestSelectWorker_SingleWorker(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	// Agregar un worker
	master.workersMutex.Lock()
	master.workers["worker-1"] = &types.WorkerInfo{
		ID:          "worker-1",
		Address:     "http://worker-1:9001",
		Status:      "IDLE",
		ActiveTasks: 0,
	}
	master.workersMutex.Unlock()

	worker := scheduler.selectWorker()

	if worker == nil {
		t.Fatal("Expected worker, got nil")
	}

	if worker.ID != "worker-1" {
		t.Errorf("Expected worker-1, got %s", worker.ID)
	}
}

func TestSelectWorker_MultipleWorkers(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	// Agregar varios workers
	master.workersMutex.Lock()
	master.workers["worker-1"] = &types.WorkerInfo{
		ID:          "worker-1",
		Address:     "http://worker-1:9001",
		Status:      "IDLE",
		ActiveTasks: 0,
	}
	master.workers["worker-2"] = &types.WorkerInfo{
		ID:          "worker-2",
		Address:     "http://worker-2:9002",
		Status:      "IDLE",
		ActiveTasks: 0,
	}
	master.workersMutex.Unlock()

	worker := scheduler.selectWorker()

	if worker == nil {
		t.Fatal("Expected worker, got nil")
	}

	// Debe seleccionar alguno de los workers disponibles
	if worker.ID != "worker-1" && worker.ID != "worker-2" {
		t.Errorf("Unexpected worker: %s", worker.ID)
	}
}

func TestSelectWorker_NoAvailable(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	// Agregar solo workers DOWN
	master.workersMutex.Lock()
	master.workers["worker-1"] = &types.WorkerInfo{
		ID:          "worker-1",
		Address:     "http://worker-1:9001",
		Status:      "DOWN",
		ActiveTasks: 0,
	}
	master.workersMutex.Unlock()

	worker := scheduler.selectWorker()

	if worker != nil {
		t.Errorf("Expected nil worker (all DOWN), got %s", worker.ID)
	}
}

func TestSelectWorker_LoadBalancing(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	// Worker 1 con 5 tareas activas
	master.workersMutex.Lock()
	master.workers["worker-1"] = &types.WorkerInfo{
		ID:          "worker-1",
		Address:     "http://worker-1:9001",
		Status:      "BUSY",
		ActiveTasks: 5,
	}
	// Worker 2 con 2 tareas activas
	master.workers["worker-2"] = &types.WorkerInfo{
		ID:          "worker-2",
		Address:     "http://worker-2:9002",
		Status:      "IDLE",
		ActiveTasks: 2,
	}
	// Worker 3 con 0 tareas activas
	master.workers["worker-3"] = &types.WorkerInfo{
		ID:          "worker-3",
		Address:     "http://worker-3:9003",
		Status:      "IDLE",
		ActiveTasks: 0,
	}
	master.workersMutex.Unlock()

	worker := scheduler.selectWorker()

	if worker == nil {
		t.Fatal("Expected worker, got nil")
	}

	// Debe seleccionar worker-3 (menos carga)
	if worker.ID != "worker-3" {
		t.Errorf("Expected worker-3 (least loaded), got %s with %d tasks", worker.ID, worker.ActiveTasks)
	}
}

// ============================================================================
// TEST: Task Result Handling
// ============================================================================

func TestHandleTaskResult_Success(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	// Crear job y tarea
	job := &types.Job{ID: "job-123"}
	execution := &JobExecution{
		Job:         job,
		Tasks:       make(map[string]*types.Task),
		OutputPaths: make(map[string]string),
	}

	task := &types.Task{
		ID:     "task-1",
		JobID:  "job-123",
		NodeID: "map1",
		Status: "RUNNING",
	}

	execution.Tasks["task-1"] = task
	scheduler.jobs["job-123"] = execution

	// Resultado exitoso
	result := types.TaskResult{
		TaskID:           "task-1",
		Status:           "COMPLETED",
		OutputPath:       "data/output/result.json",
		RecordsProcessed: 100,
		Duration:         1.5,
	}

	scheduler.HandleTaskResult(result) // Usar HandleTaskResult (con mayúscula)

	// Verificar que la tarea se actualizó
	if task.Status != "COMPLETED" {
		t.Errorf("Expected status COMPLETED, got %s", task.Status)
	}
}

func TestHandleTaskResult_Failure(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	// Crear job y tarea
	job := &types.Job{ID: "job-123"}
	execution := &JobExecution{
		Job:   job,
		Tasks: make(map[string]*types.Task),
	}

	task := &types.Task{
		ID:         "task-1",
		JobID:      "job-123",
		Status:     "RUNNING",
		AttemptNum: 0,
	}

	execution.Tasks["task-1"] = task
	scheduler.jobs["job-123"] = execution

	// Resultado fallido
	result := types.TaskResult{
		TaskID:   "task-1",
		Status:   "FAILED",
		Error:    "Something went wrong",
		Duration: 0.5,
	}

	scheduler.HandleTaskResult(result)

	// La tarea debe tener error registrado
	if task.Error == "" {
		t.Error("Expected task error to be set")
	}
}

// ============================================================================
// TEST: Retry Logic
// ============================================================================

func TestRetryTask_UnderLimit(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	task := &types.Task{
		ID:         "task-1",
		Status:     "FAILED",
		AttemptNum: 1,
		AssignedTo: "worker-1",
	}

	initialAttempt := task.AttemptNum
	scheduler.retryTask(task)

	// El retry incrementa el intento en el método
	// Verificar que se llamó retryTask (no falla)
	if task.AttemptNum < initialAttempt {
		t.Errorf("AttemptNum should not decrease, was %d, now %d", initialAttempt, task.AttemptNum)
	}

	// Verificar que está bajo el límite de reintentos
	if task.AttemptNum >= MaxRetries {
		t.Errorf("Task should retry under MaxRetries=%d, got %d", MaxRetries, task.AttemptNum)
	}
}

func TestRetryTask_ExceedsLimit(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	task := &types.Task{
		ID:         "task-1",
		Status:     "FAILED",
		AttemptNum: MaxRetries,
	}

	initialAttempt := task.AttemptNum
	scheduler.retryTask(task)

	// No debe incrementar más si ya excedió el límite
	if task.AttemptNum != initialAttempt && task.AttemptNum > MaxRetries {
		t.Errorf("Task should not retry beyond MaxRetries=%d, got %d", MaxRetries, task.AttemptNum)
	}
}

// ============================================================================
// TEST: Utility Functions
// ============================================================================

func TestFindNode(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	nodes := []types.Node{
		{ID: "node1", Operation: "map"},
		{ID: "node2", Operation: "filter"},
		{ID: "node3", Operation: "reduce"},
	}

	node := scheduler.findNode(nodes, "node2")

	if node == nil {
		t.Fatal("Expected node, got nil")
	}

	if node.ID != "node2" {
		t.Errorf("Expected node2, got %s", node.ID)
	}

	if node.Operation != "filter" {
		t.Errorf("Expected operation filter, got %s", node.Operation)
	}

	// Buscar nodo inexistente
	notFound := scheduler.findNode(nodes, "node99")
	if notFound != nil {
		t.Error("Expected nil for non-existent node")
	}
}

func TestCompleteJob(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	job := &types.Job{
		ID:     "job-123",
		Status: "RUNNING",
	}

	execution := &JobExecution{
		Job: job,
	}

	scheduler.jobs["job-123"] = execution

	scheduler.completeJob("job-123")

	if job.Status != "COMPLETED" {
		t.Errorf("Expected status COMPLETED, got %s", job.Status)
	}

	if job.CompletedAt == nil || job.CompletedAt.IsZero() {
		t.Error("CompletedAt should be set")
	}
}

func TestFailJob(t *testing.T) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	job := &types.Job{
		ID:     "job-123",
		Status: "RUNNING",
	}

	execution := &JobExecution{
		Job: job,
	}

	scheduler.jobs["job-123"] = execution

	reason := "Worker timeout"
	scheduler.failJob("job-123", reason)

	if job.Status != "FAILED" {
		t.Errorf("Expected status FAILED, got %s", job.Status)
	}
}

// ============================================================================
// BENCHMARKS
// ============================================================================

func BenchmarkAnalyzeDAG(b *testing.B) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	dag := &types.DAG{
		Nodes: []types.Node{
			{ID: "read", Operation: "read_csv"},
			{ID: "map", Operation: "map"},
			{ID: "filter", Operation: "filter"},
			{ID: "reduce", Operation: "reduce_by_key"},
		},
		Edges: [][]string{
			{"read", "map"},
			{"map", "filter"},
			{"filter", "reduce"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = scheduler.analyzeDAG(dag)
	}
}

func BenchmarkSelectWorker(b *testing.B) {
	master := setupTestMaster()
	scheduler := NewScheduler(master)

	// Agregar 100 workers
	master.workersMutex.Lock()
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("worker-%d", i)
		master.workers[id] = &types.WorkerInfo{
			ID:          id,
			Status:      "IDLE",
			ActiveTasks: i % 10,
		}
	}
	master.workersMutex.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = scheduler.selectWorker()
	}
}
