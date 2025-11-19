// common/protocol/api.go
package protocol

// Endpoints del Master
const (
	// Worker registration & health
	EndpointWorkerRegister  = "/api/v1/workers/register"
	EndpointWorkerHeartbeat = "/api/v1/workers/heartbeat"

	// Job management
	EndpointJobSubmit  = "/api/v1/jobs"
	EndpointJobStatus  = "/api/v1/jobs/%s"         // /api/v1/jobs/{job_id}
	EndpointJobResults = "/api/v1/jobs/%s/results" // /api/v1/jobs/{job_id}/results

	// Internal - Task reporting (worker -> master)
	EndpointTaskResult = "/internal/tasks/%s/result" // /internal/tasks/{task_id}/result

	// Metrics
	EndpointMetrics = "/api/v1/metrics"
)

// Endpoints del Worker
const (
	// Task execution (master -> worker)
	EndpointWorkerExecuteTask = "/tasks/execute"
)

// HTTP Status Codes personalizados
const (
	StatusTaskAccepted = 202 // Task aceptado para ejecución
)
