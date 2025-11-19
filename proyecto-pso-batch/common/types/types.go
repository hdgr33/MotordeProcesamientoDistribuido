// common/types/types.go
package types

import "time"

// ============================================================================
// WORKER MANAGEMENT
// ============================================================================

type WorkerInfo struct {
	ID            string    `json:"id"`
	Address       string    `json:"address"` // host:port
	Status        string    `json:"status"`  // "IDLE", "BUSY", "DOWN"
	LastHeartbeat time.Time `json:"last_heartbeat"`
	ActiveTasks   int       `json:"active_tasks"`
	TotalTasks    int       `json:"total_tasks"`
}

type HeartbeatRequest struct {
	WorkerID    string  `json:"worker_id"`
	ActiveTasks int     `json:"active_tasks"`
	MemoryMB    float64 `json:"memory_mb"`
}

type HeartbeatResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// ============================================================================
// JOB DEFINITION (Ruta A - Batch DAG)
// ============================================================================

type Job struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	DAG         DAG        `json:"dag"`
	Parallelism int        `json:"parallelism"` // número de workers a usar
	Status      string     `json:"status"`      // "PENDING", "RUNNING", "COMPLETED", "FAILED"
	SubmittedAt time.Time  `json:"submitted_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type DAG struct {
	Nodes []Node     `json:"nodes"`
	Edges [][]string `json:"edges"` // [["nodeA", "nodeB"], ...]
}

type Node struct {
	ID         string            `json:"id"`
	Operation  string            `json:"op"`                   // "read_csv", "map", "filter", "flat_map", "reduce_by_key", "join"
	Path       string            `json:"path,omitempty"`       // para read_csv
	Partitions int               `json:"partitions,omitempty"` // para read_csv
	Function   string            `json:"fn,omitempty"`         // nombre de la función a aplicar
	Key        string            `json:"key,omitempty"`        // para reduce_by_key, join
	Params     map[string]string `json:"params,omitempty"`     // parámetros adicionales
}

// ============================================================================
// TASK EXECUTION
// ============================================================================

type Task struct {
	ID          string            `json:"id"`
	JobID       string            `json:"job_id"`
	NodeID      string            `json:"node_id"`
	Operation   string            `json:"operation"`
	Function    string            `json:"function,omitempty"`
	InputPaths  []string          `json:"input_paths,omitempty"` // archivos o URLs
	OutputPath  string            `json:"output_path"`
	Partition   int               `json:"partition"`
	Params      map[string]string `json:"params,omitempty"`
	Status      string            `json:"status"`                // "PENDING", "RUNNING", "COMPLETED", "FAILED"
	AssignedTo  string            `json:"assigned_to,omitempty"` // worker ID
	AttemptNum  int               `json:"attempt_num"`
	StartedAt   *time.Time        `json:"started_at,omitempty"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	Error       string            `json:"error,omitempty"`
}

type TaskAssignment struct {
	Task      Task   `json:"task"`
	MasterURL string `json:"master_url"` // para reportar resultados
}

type TaskResult struct {
	TaskID           string    `json:"task_id"`
	Status           string    `json:"status"` // "COMPLETED", "FAILED"
	OutputPath       string    `json:"output_path,omitempty"`
	RecordsProcessed int       `json:"records_processed"`
	Error            string    `json:"error,omitempty"`
	Duration         float64   `json:"duration_seconds"`
	CompletedAt      time.Time `json:"completed_at"`
}

// ============================================================================
// DATA RECORDS
// ============================================================================

type Record struct {
	Data map[string]interface{} `json:"data"` // flexible key-value
}

// ============================================================================
// API RESPONSES
// ============================================================================

type JobSubmitResponse struct {
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type JobStatusResponse struct {
	Job          Job     `json:"job"`
	Progress     float64 `json:"progress"` // 0.0 - 100.0
	TasksTotal   int     `json:"tasks_total"`
	TasksDone    int     `json:"tasks_done"`
	TasksFailed  int     `json:"tasks_failed"`
	TasksPending int     `json:"tasks_pending"`
}

type JobResultsResponse struct {
	JobID       string   `json:"job_id"`
	Status      string   `json:"status"`
	OutputPaths []string `json:"output_paths"`
}

// ============================================================================
// METRICS
// ============================================================================

type Metrics struct {
	Timestamp      time.Time `json:"timestamp"`
	TasksCompleted int       `json:"tasks_completed"`
	TasksFailed    int       `json:"tasks_failed"`
	TasksRunning   int       `json:"tasks_running"`
	AvgLatencyMs   float64   `json:"avg_latency_ms"`
	TotalRecords   int64     `json:"total_records"`
	MemoryUsageMB  float64   `json:"memory_usage_mb"`
}
