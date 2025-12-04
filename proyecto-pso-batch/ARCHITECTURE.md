# PSO Batch - Documentación de Arquitectura

## Índice

1. [Visión General](#visión-general)
2. [Componentes del Sistema](#componentes-del-sistema)
3. [Flujo de Ejecución](#flujo-de-ejecución)
4. [Protocolo de Comunicación](#protocolo-de-comunicación)
5. [Gestión de Estado](#gestión-de-estado)
6. [Tolerancia a Fallos](#tolerancia-a-fallos)
7. [Gestión de Memoria](#gestión-de-memoria)
8. [Decisiones de Diseño](#decisiones-de-diseño)

---

## Visión General

PSO Batch es un motor de procesamiento distribuido diseñado para ejecutar pipelines de transformación de datos representados como DAGs (Directed Acyclic Graphs). La arquitectura sigue el patrón Master-Worker con las siguientes características clave:

- **Planificación Dinámica**: El master analiza el DAG y genera stages de ejecución
- **Ejecución por Etapas**: Los stages se ejecutan secuencialmente, las tareas dentro de un stage en paralelo
- **Comunicación HTTP/JSON**: Protocolo simple y debuggable
- **State Management**: Estado distribuido con sincronización mediante mutexes
- **Fault Recovery**: Reintentos automáticos y reasignación de tareas

### Diagrama de Alto Nivel

```
┌───────────────────────────────────────────────────────────┐
│                        Client                              │
│  - Envía jobs (JSON)                                       │
│  - Consulta estado                                         │
│  - Recibe resultados                                       │
└────────────────────┬──────────────────────────────────────┘
                     │
                     │ HTTP POST /api/v1/jobs
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                      Master Node                             │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              HTTP API Layer                         │    │
│  │  - /api/v1/jobs (submit)                            │    │
│  │  - /api/v1/jobs/{id} (status)                       │    │
│  │  - /api/v1/workers (list)                           │    │
│  │  - /internal/tasks/{id}/result (worker→master)      │    │
│  └─────────────────┬───────────────────────────────────┘    │
│                    │                                         │
│  ┌─────────────────▼───────────────────────────────────┐    │
│  │              Job Manager                            │    │
│  │  - Valida y almacena jobs                          │    │
│  │  - Gestiona estado (PENDING→RUNNING→COMPLETED)      │    │
│  └─────────────────┬───────────────────────────────────┘    │
│                    │                                         │
│  ┌─────────────────▼───────────────────────────────────┐    │
│  │              Scheduler                              │    │
│  │                                                     │    │
│  │  ┌──────────────────────────────────────────────┐  │    │
│  │  │ 1. DAG Analyzer                              │  │    │
│  │  │    - Topological sort                         │  │    │
│  │  │    - Genera stages de ejecución               │  │    │
│  │  └──────────────────────────────────────────────┘  │    │
│  │                                                     │    │
│  │  ┌──────────────────────────────────────────────┐  │    │
│  │  │ 2. Task Generator                            │  │    │
│  │  │    - Crea tareas por nodo/partición           │  │    │
│  │  │    - Determina dependencias                   │  │    │
│  │  └──────────────────────────────────────────────┘  │    │
│  │                                                     │    │
│  │  ┌──────────────────────────────────────────────┐  │    │
│  │  │ 3. Task Queue Manager                        │  │    │
│  │  │    - Cola de tareas pendientes                │  │    │
│  │  │    - Asignación a workers                     │  │    │
│  │  └──────────────────────────────────────────────┘  │    │
│  │                                                     │    │
│  │  ┌──────────────────────────────────────────────┐  │    │
│  │  │ 4. Failure Monitor                           │  │    │
│  │  │    - Detecta timeouts (5 min)                 │  │    │
│  │  │    - Detecta workers caídos                   │  │    │
│  │  │    - Reintenta tareas (max 3 veces)           │  │    │
│  │  └──────────────────────────────────────────────┘  │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           Worker Registry                            │   │
│  │  - Mantiene lista de workers activos                 │   │
│  │  - Heartbeat monitoring (cada 2s)                    │   │
│  │  - Load balancing (mínimo de tareas activas)         │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     │ Asigna tareas
                     │ POST /tasks/execute
                     ▼
┌───────────────────────────────────────────────────────────────┐
│                        Worker Nodes                           │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │               Executor Engine                          │  │
│  │                                                        │  │
│  │  1. Recibe TaskAssignment                             │  │
│  │  2. Selecciona operador (read_csv, map, filter, etc.) │  │
│  │  3. Lee inputs (si existen)                           │  │
│  │  4. Ejecuta operación                                 │  │
│  │  5. Escribe output                                    │  │
│  │  6. Reporta resultado al master                       │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │               Operators Library                        │  │
│  │                                                        │  │
│  │  - read_csv: Lee y parsea CSV                         │  │
│  │  - map: Transforma records 1:1                        │  │
│  │  - filter: Filtra records                             │  │
│  │  - flat_map: Expande records 1:N                      │  │
│  │  - reduce_by_key: Agrupa y agrega                     │  │
│  │  - join: Une datasets por clave                       │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │               Memory Manager (Cache)                   │  │
│  │                                                        │  │
│  │  - Buffer en memoria (default 100MB)                  │  │
│  │  - Spill a disco cuando se excede límite              │  │
│  │  - Lectura combinada (memoria + disco)                │  │
│  │  - Cleanup automático post-tarea                      │  │
│  └────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
```

---

## Componentes del Sistema

### 1. Master Node

El nodo master es el cerebro del sistema. Implementado en `master/main.go` y `master/scheduler.go`.

#### 1.1 HTTP API Layer

**Endpoints Públicos** (para clientes):

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/api/v1/jobs` | POST | Enviar nuevo job |
| `/api/v1/jobs/{id}` | GET | Consultar estado de job |
| `/api/v1/jobs/{id}/results` | GET | Obtener resultados |
| `/api/v1/workers` | GET | Listar workers activos |
| `/api/v1/metrics` | GET | Métricas del sistema |

**Endpoints Internos** (para workers):

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/api/v1/workers/register` | POST | Registrar worker |
| `/api/v1/workers/heartbeat` | POST | Enviar heartbeat |
| `/internal/tasks/{id}/result` | POST | Reportar resultado de tarea |

#### 1.2 Job Manager

```go
type Master struct {
    jobs       map[string]*types.Job
    jobsMutex  sync.RWMutex
    workers    map[string]*types.WorkerInfo
    workersMutex sync.RWMutex
    scheduler  *Scheduler
    port       string
}
```

**Responsabilidades:**
- Validar JSON de jobs entrantes
- Asignar IDs únicos
- Mantener estado de jobs
- Coordinar con scheduler

#### 1.3 Scheduler

```go
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
```

**Algoritmo de Planificación:**

1. **Análisis de DAG** (`analyzeDAG`):
   ```
   Input: DAG (nodos + aristas)
   Output: Lista de stages [[node1, node2], [node3], ...]
   
   Algoritmo:
   - Construir mapa de dependencias
   - Calcular in-degree de cada nodo
   - Topological sort por niveles:
     * Stage i = todos los nodos cuyas dependencias están en stages < i
   - Detectar ciclos (si ningún nodo tiene in-degree 0)
   ```

2. **Generación de Tareas** (`createTask`):
   ```
   Por cada nodo en el stage actual:
     Por cada partición (1..N):
       - Crear Task con ID único
       - Determinar input paths de outputs de stages previos
       - Asignar output path
       - Encolar en taskQueue
   ```

3. **Asignación de Tareas** (`assignTask`):
   ```
   1. Seleccionar worker con menos carga (minLoad)
   2. Marcar tarea como RUNNING
   3. Enviar TaskAssignment al worker vía HTTP POST
   4. Actualizar estado del worker (active_tasks++)
   ```

4. **Ejecución de Stage** (`executeStage`):
   ```
   1. Crear todas las tareas del stage
   2. Usar WaitGroup para esperar completación
   3. Por cada resultado de tarea:
      - Si COMPLETED: guardar output path
      - Si FAILED: reintentar (hasta 3 veces)
   4. Si todas las tareas completan: avanzar a stage + 1
   5. Si alguna falla definitivamente: marcar job como FAILED
   ```

#### 1.4 Failure Monitor

**Goroutines de monitoreo:**

1. **Task Timeout Monitor** (cada 5s):
   - Busca tareas RUNNING con más de 5 minutos
   - Marca como FAILED y reintenta

2. **Worker Failure Monitor** (cada 3s):
   - Identifica workers con status=DOWN
   - Reencola tareas asignadas a esos workers

**Estrategia de Reintentos:**
```go
func (s *Scheduler) retryTask(task *types.Task) {
    if task.AttemptNum >= MaxRetries {  // MaxRetries = 3
        task.Status = "FAILED"
        return
    }
    
    task.Status = "PENDING"
    task.AssignedTo = ""
    time.Sleep(1 * time.Second)
    s.taskQueue <- task
}
```

---

### 2. Worker Nodes

Implementados en `worker/main.go`, `worker/operators.go`, `worker/cache.go`.

#### 2.1 Registro y Heartbeats

```go
// Registro inicial
func (w *Worker) register() error {
    payload := map[string]string{
        "worker_id": w.id,
        "address":   "http://" + w.id + ":" + w.port,
    }
    // POST a /api/v1/workers/register
}

// Heartbeats periódicos (cada 2s)
func (w *Worker) sendHeartbeats() {
    ticker := time.NewTicker(2 * time.Second)
    for range ticker.C {
        hb := types.HeartbeatRequest{
            WorkerID:    w.id,
            ActiveTasks: w.activeTasks,
            MemoryMB:    getCurrentMemoryMB(),
        }
        // POST a /api/v1/workers/heartbeat
    }
}
```

#### 2.2 Ejecución de Tareas

```go
func (w *Worker) handleExecuteTask(wr http.ResponseWriter, r *http.Request) {
    var assignment types.TaskAssignment
    json.NewDecoder(r.Body).Decode(&assignment)
    
    // Responder inmediatamente 202 ACCEPTED
    wr.WriteHeader(protocol.StatusTaskAccepted)
    
    // Ejecutar en goroutine
    go w.executeTask(assignment)
}

func (w *Worker) executeTask(assignment types.TaskAssignment) {
    startTime := time.Now()
    
    // 1. Ejecutar operador
    records, err := w.runOperator(&task)
    
    // 2. Escribir resultados
    if err == nil {
        err = writeRecordsToFile(task.OutputPath, records)
    }
    
    // 3. Construir resultado
    result := types.TaskResult{
        TaskID:           task.ID,
        Status:           "COMPLETED" or "FAILED",
        OutputPath:       task.OutputPath,
        RecordsProcessed: len(records),
        Error:            err.Error() if err != nil,
        Duration:         time.Since(startTime).Seconds(),
    }
    
    // 4. Reportar al master
    w.reportResult(assignment.MasterURL, result)
}
```

#### 2.3 Operadores

**Categorías:**

1. **Source**: `read_csv`
2. **Transformation**: `map`, `filter`, `flat_map`
3. **Aggregation**: `reduce_by_key`
4. **Join**: `join`

**Ejemplo: reduce_by_key**

```go
func operatorReduceByKey(task *types.Task, input []types.Record) ([]types.Record, error) {
    keyField := task.Key // e.g., "word"
    
    // 1. Agrupar por clave
    groups := make(map[string][]types.Record)
    for _, record := range input {
        key := record.Data[keyField].(string)
        groups[key] = append(groups[key], record)
    }
    
    // 2. Aplicar función de reducción
    var output []types.Record
    for key, records := range groups {
        reduced := applyReduceFunction(task.Function, key, keyField, records)
        output = append(output, reduced)
    }
    
    return output, nil
}

func applyReduceFunction(fn string, key string, keyField string, records []types.Record) types.Record {
    switch fn {
    case "count":
        return types.Record{
            Data: map[string]interface{}{
                keyField: key,
                "count":  len(records),
            },
        }
    // ... más funciones
    }
}
```

#### 2.4 Memory Manager

```go
type RecordCache struct {
    records      []types.Record
    memoryUsed   int64
    memoryLimit  int64           // Default: 100MB
    spilledFiles []string
    mutex        sync.RWMutex
    spillPath    string
}

func (rc *RecordCache) Add(record types.Record) error {
    recordSize := estimateRecordSize(record)
    
    // Si excede límite, spillear a disco
    if rc.memoryUsed + recordSize > rc.memoryLimit {
        rc.spillToDisk()
    }
    
    rc.records = append(rc.records, record)
    rc.memoryUsed += recordSize
    return nil
}

func (rc *RecordCache) spillToDisk() error {
    spillFile := filepath.Join(rc.spillPath, fmt.Sprintf("spill-%d.json", len(rc.spilledFiles)))
    writeRecordsToFile(spillFile, rc.records)
    
    rc.spilledFiles = append(rc.spilledFiles, spillFile)
    rc.records = make([]types.Record, 0)
    rc.memoryUsed = 0
    
    return nil
}
```

**Ventajas:**
- Permite procesar datasets más grandes que la RAM disponible
- Transparente para los operadores
- Limpieza automática post-tarea

---

## Flujo de Ejecución

### Ejemplo Completo: WordCount

**Job Definition:**
```json
{
  "name": "wordcount",
  "parallelism": 2,
  "dag": {
    "nodes": [
      {"id": "read", "op": "read_csv", "path": "data/input/text.csv", "partitions": 2},
      {"id": "tokenize", "op": "flat_map", "fn": "split_words"},
      {"id": "count", "op": "reduce_by_key", "fn": "count", "key": "word"}
    ],
    "edges": [["read", "tokenize"], ["tokenize", "count"]]
  }
}
```

**Ejecución Paso a Paso:**

```
T=0s: Cliente envía job
  ├─ POST /api/v1/jobs
  ├─ Master valida JSON
  ├─ Master asigna ID: job-1234567890
  └─ Response: {"job_id": "job-1234567890", "status": "PENDING"}

T=0.1s: Scheduler analiza DAG
  ├─ analyzeDAG() ejecuta topological sort
  ├─ Stages generados:
  │   Stage 0: [read]
  │   Stage 1: [tokenize]
  │   Stage 2: [count]
  └─ Job status: PENDING → RUNNING

T=0.2s: Ejecución de Stage 0 (read)
  ├─ createTask(): 2 tareas (partitions=2)
  │   ├─ job-1234567890-read-p0
  │   └─ job-1234567890-read-p1
  ├─ assignTask(): selecciona workers
  │   ├─ Tarea p0 → worker-1
  │   └─ Tarea p1 → worker-2
  └─ Envía TaskAssignment via HTTP

T=0.3s: Workers ejecutan read_csv
  worker-1:
    ├─ Abre data/input/text.csv
    ├─ Lee partición 0 (líneas 0-500)
    ├─ Parsea CSV → 500 Records
    ├─ Escribe data/output/job-...-read-p0.json
    └─ POST /internal/tasks/.../result: STATUS=COMPLETED
  
  worker-2:
    ├─ Abre data/input/text.csv
    ├─ Lee partición 1 (líneas 501-1000)
    ├─ Parsea CSV → 500 Records
    ├─ Escribe data/output/job-...-read-p1.json
    └─ POST /internal/tasks/.../result: STATUS=COMPLETED

T=3.5s: Scheduler detecta Stage 0 completado
  ├─ WaitGroup unblocked
  ├─ OutputPaths actualizados:
  │   read → [data/output/job-...-read-p0.json, data/output/job-...-read-p1.json]
  └─ Llama executeStage(jobID, 1)

T=3.6s: Ejecución de Stage 1 (tokenize)
  ├─ createTask(): 2 tareas
  │   ├─ job-1234567890-tokenize-p0
  │   │   InputPaths: [data/output/job-...-read-p0.json]
  │   └─ job-1234567890-tokenize-p1
  │       InputPaths: [data/output/job-...-read-p1.json]
  ├─ assignTask()
  │   ├─ Tarea p0 → worker-3
  │   └─ Tarea p1 → worker-1
  └─ Envía TaskAssignment

T=3.7s: Workers ejecutan flat_map (split_words)
  worker-3:
    ├─ Lee data/output/job-...-read-p0.json (500 records)
    ├─ Por cada record con campo "text":
    │   └─ Divide en palabras → N records con {"word": "..."}
    ├─ Total: 500 records → 5000 word records
    ├─ Escribe data/output/job-...-tokenize-p0.json
    └─ Reporta COMPLETED
  
  worker-1:
    ├─ Lee data/output/job-...-read-p1.json (500 records)
    ├─ Split words → 5000 word records
    ├─ Escribe data/output/job-...-tokenize-p1.json
    └─ Reporta COMPLETED

T=8.2s: Stage 1 completado → executeStage(jobID, 2)

T=8.3s: Ejecución de Stage 2 (count - reduce_by_key)
  ├─ createTask(): 2 tareas
  │   ├─ job-1234567890-count-p0
  │   │   InputPaths: [data/output/job-...-tokenize-p0.json]
  │   └─ job-1234567890-count-p1
  │       InputPaths: [data/output/job-...-tokenize-p1.json]
  └─ assignTask()

T=8.4s: Workers ejecutan reduce_by_key
  worker-2:
    ├─ Lee 5000 word records
    ├─ Agrupa por word → HashMap[word][]Record
    ├─ Aplica función "count"
    │   "hello" → {"word": "hello", "count": 42}
    │   "world" → {"word": "world", "count": 38}
    ├─ Resultado: 150 unique words
    ├─ Escribe data/output/job-...-count-p0.json
    └─ Reporta COMPLETED
  
  worker-3:
    ├─ Similar proceso
    ├─ Resultado: 145 unique words
    ├─ Escribe data/output/job-...-count-p1.json
    └─ Reporta COMPLETED

T=15.1s: Stage 2 completado
  ├─ Todos los stages ejecutados
  ├─ completeJob(jobID) llamado
  ├─ Job status: RUNNING → COMPLETED
  ├─ OutputPaths finales:
  │   count → [data/output/job-...-count-p0.json, data/output/job-...-count-p1.json]
  └─ Log: "✨ Job job-1234567890 completado en 14.9s"

T=16s: Cliente consulta resultados
  ├─ GET /api/v1/jobs/job-1234567890/results
  └─ Response: {
      "job_id": "job-1234567890",
      "status": "COMPLETED",
      "output_paths": [
        "data/output/job-...-count-p0.json",
        "data/output/job-...-count-p1.json"
      ]
    }
```

---

## Protocolo de Comunicación

### Formato de Mensajes

Todos los mensajes usan **JSON sobre HTTP**.

#### Job Submission

```json
// Request: POST /api/v1/jobs
{
  "name": "my-job",
  "parallelism": 3,
  "dag": {
    "nodes": [...],
    "edges": [...]
  }
}

// Response: 200 OK
{
  "job_id": "job-1234567890",
  "status": "PENDING",
  "message": "Job enqueued successfully"
}
```

#### Task Assignment (Master → Worker)

```json
// Request: POST http://worker-1:9001/tasks/execute
{
  "task": {
    "id": "job-123-read-p0",
    "job_id": "job-123",
    "node_id": "read",
    "operation": "read_csv",
    "input_paths": ["data/input/file.csv"],
    "output_path": "data/output/job-123-read-p0.json",
    "partition": 0,
    "params": {}
  },
  "master_url": "http://master:8080"
}

// Response: 202 Accepted
{
  "status": "ACCEPTED"
}
```

#### Task Result (Worker → Master)

```json
// Request: POST http://master:8080/internal/tasks/job-123-read-p0/result
{
  "task_id": "job-123-read-p0",
  "status": "COMPLETED",
  "output_path": "data/output/job-123-read-p0.json",
  "records_processed": 1000,
  "duration_seconds": 2.5,
  "completed_at": "2024-01-15T10:30:00Z"
}

// Response: 200 OK
{
  "success": true
}
```

#### Worker Heartbeat

```json
// Request: POST /api/v1/workers/heartbeat
{
  "worker_id": "worker-1",
  "active_tasks": 2,
  "memory_mb": 512.5
}

// Response: 200 OK
{
  "success": true
}
```

---

## Gestión de Estado

### Estado en Master

```go
// Jobs activos
master.jobs: map[string]*types.Job
  "job-123" → Job{
    ID: "job-123",
    Name: "wordcount",
    Status: "RUNNING",
    SubmittedAt: time.Time,
    CompletedAt: nil
  }

// Workers registrados
master.workers: map[string]*types.WorkerInfo
  "worker-1" → WorkerInfo{
    ID: "worker-1",
    Address: "http://worker-1:9001",
    Status: "BUSY",
    LastHeartbeat: time.Now(),
    ActiveTasks: 2,
    TotalTasks: 15
  }

// Ejecuciones de jobs (en scheduler)
scheduler.jobs: map[string]*JobExecution
  "job-123" → JobExecution{
    Job: *Job,
    Tasks: map[string]*Task{
      "job-123-read-p0" → Task{Status: "COMPLETED", ...},
      "job-123-read-p1" → Task{Status: "RUNNING", ...}
    },
    Stages: [["read"], ["tokenize"], ["count"]],
    CurrentStage: 1,
    OutputPaths: {"read": "data/output/..."}
  }
```

### Sincronización

**Mutexes utilizados:**

1. `master.jobsMutex`: Protege `master.jobs`
2. `master.workersMutex`: Protege `master.workers`
3. `scheduler.jobsMutex`: Protege `scheduler.jobs`
4. `execution.TasksMutex`: Protege `execution.Tasks` de cada job

**Patrón de uso:**
```go
// Lectura
s.jobsMutex.RLock()
execution := s.jobs[jobID]
s.jobsMutex.RUnlock()

// Escritura
s.jobsMutex.Lock()
s.jobs[jobID] = newExecution
s.jobsMutex.Unlock()
```

---

## Tolerancia a Fallos

### Escenarios de Fallo

#### 1. Worker Crash Durante Ejecución

**Detección:**
- Worker deja de enviar heartbeats
- Tras 6 segundos (3 heartbeats perdidos), master marca worker como DOWN

**Recuperación:**
```
1. monitorWorkerFailures() detecta worker=DOWN
2. Busca todas las tareas con AssignedTo=worker-X y Status=RUNNING
3. Por cada tarea:
   - Cambiar Status: RUNNING → PENDING
   - Limpiar AssignedTo
   - Reencolar en taskQueue
4. processTaskQueue() reasigna tareas a workers activos
```

**Ejemplo:**
```
T=0: worker-2 tiene tarea "job-123-map-p1" RUNNING
T=5: worker-2 se cae
T=11: Master detecta 3 heartbeats perdidos → worker-2.Status = DOWN
T=14: monitorWorkerFailures() encuentra tarea "job-123-map-p1"
T=14.1: Tarea reencolada con AttemptNum++
T=15: worker-3 recibe la tarea
T=20: Tarea completa exitosamente
```

#### 2. Task Timeout

**Detección:**
- monitorTaskTimeouts() corre cada 5 segundos
- Busca tareas RUNNING con StartedAt > 5 minutos

**Recuperación:**
```go
if task.Status == "RUNNING" && time.Since(*task.StartedAt) > 5*time.Minute {
    log.Printf("⏱️  Timeout en tarea %s", task.ID)
    task.Status = "FAILED"
    task.Error = "Timeout después de 5 minutos"
    s.retryTask(task)
}
```

#### 3. Task Failure (Error en Worker)

**Detección:**
- Worker reporta TaskResult con Status="FAILED"

**Recuperación:**
```go
func (s *Scheduler) HandleTaskResult(result types.TaskResult) {
    if result.Status == "FAILED" {
        log.Printf("❌ Tarea %s falló: %s", result.TaskID, result.Error)
        s.retryTask(task)  // Reintenta hasta MaxRetries (3)
    }
}
```

**Límite de Reintentos:**
```go
func (s *Scheduler) retryTask(task *types.Task) {
    if task.AttemptNum >= 3 {
        log.Printf("❌ Tarea %s excedió reintentos máximos", task.ID)
        task.Status = "FAILED"
        // Job marca como FAILED si es tarea crítica
        return
    }
    
    task.AttemptNum++
    task.Status = "PENDING"
    s.taskQueue <- task
}
```

#### 4. Master Crash

**Limitación Actual:**
- Estado solo en memoria (no persistente)
- Si master se cae, se pierden todos los jobs en ejecución

**Mejora Futura:**
- Checkpoint periódico a disco/base de datos
- Recuperación de estado al reiniciar
- Réplicas de master con elección de líder (Raft)

---

## Gestión de Memoria

### Problema

Procesar datasets de 10GB con workers de 2GB RAM:
- `read_csv` carga todo el archivo en memoria → OOM
- `reduce_by_key` acumula todos los grupos → OOM

### Solución: RecordCache con Spill

```
┌─────────────────────────────────────────┐
│         RecordCache                      │
│                                          │
│  Memory Buffer (100MB)                   │
│  ┌────────────────────────────────────┐  │
│  │ [Record1, Record2, ..., RecordN]   │  │
│  └────────────────────────────────────┘  │
│           │                              │
│           │ memoryUsed >= memoryLimit    │
│           ▼                              │
│  ┌────────────────────────────────────┐  │
│  │     spillToDisk()                  │  │
│  │  - Escribe records a JSON          │  │
│  │  - Limpia memoria                  │  │
│  │  - Registra archivo spill          │  │
│  └────────────────────────────────────┘  │
│           │                              │
│           ▼                              │
│  Disk Spill Files                        │
│  ┌────────────────────────────────────┐  │
│  │ /data/spill/task-123/              │  │
│  │   spill-0.json (100MB)             │  │
│  │   spill-1.json (100MB)             │  │
│  │   spill-2.json (50MB)              │  │
│  └────────────────────────────────────┘  │
│           │                              │
│           │ GetAll() → merge memory+disk │
│           ▼                              │
│  Combined Result                         │
└─────────────────────────────────────────┘
```

**Uso en Operadores:**

```go
func operatorReadCSV(task *types.Task) ([]types.Record, error) {
    cache := NewRecordCache(100, task.ID)  // 100MB limit
    defer cache.Cleanup()
    
    reader := csv.NewReader(file)
    for {
        row, err := reader.Read()
        if err == io.EOF {
            break
        }
        
        record := parseRow(row)
        cache.Add(record)  // Spill automático si excede límite
    }
    
    cache.Flush()                 // Escribir restante a disco
    return cache.GetAll()         // Retorna memoria + todos los spills
}
```

**Ventajas:**
- Permite procesar datasets arbitrariamente grandes
- Transparente: operadores no necesitan saber sobre spill
- Configurnable: límite de memoria ajustable

**Trade-off:**
- Performance: I/O de disco es 100x más lento que RAM
- Espacio: Requiere disco suficiente para spillover

---

## Decisiones de Diseño

### 1. ¿Por qué HTTP/JSON en lugar de gRPC?

**Ventajas de HTTP/JSON:**
- Simplicidad: fácil de debuggear con curl
- Sin dependencias: no requiere protobuf
- Firewall-friendly: puerto 80/8080 típicamente abierto

**Desventaja:**
- Performance: JSON parsing es más lento que binario
- Overhead: headers HTTP añaden latencia

**Decisión:** Para un proyecto académico y datasets medianos, la simplicidad vale más que el 10-20% de performance extra.

### 2. ¿Por qué Stages Secuenciales?

**Alternativa:** Ejecución especulativa (como Spark)
- Iniciar siguiente stage antes de que termine el anterior
- Útil para reducir latencia end-to-end

**Problema:**
- Complejidad: gestión de dependencias parciales
- Desperdicio: si stage previo falla, trabajo especulativo se pierde

**Decisión:** Stages estrictamente secuenciales son más simples y predecibles.

### 3. ¿Por qué Tareas por Partición en lugar de Micro-batches?

**Diseño Actual:**
```
1 nodo read_csv con partitions=4 → 4 tareas
Cada tarea procesa 1/4 del archivo completo
```

**Alternativa: Micro-batches**
```
1 nodo read_csv → 100 tareas de 1000 registros cada una
Menor granularidad, mejor load balancing
```

**Trade-off:**
- Micro-batches: mejor distribución de carga, más overhead de coordinación
- Particiones: menos overhead, puede desbalancear si particiones son desiguales

**Decisión:** Particiones simples para MVP, micro-batches para versión futura.

### 4. ¿Por qué Estado en Memoria?

**Problema:** Si master crashea, todo el estado se pierde

**Alternativa:** Persistencia en base de datos (Redis, PostgreSQL)
- Sobrevive crashes del master
- Permite recuperar jobs en ejecución

**Razón de no usar:**
- Complejidad: requiere esquema de BD, migraciones, transacciones
- Latencia: cada actualización de estado requiere write a BD

**Decisión:** Estado en memoria para MVP, checkpoint a BD para producción.

### 5. ¿Por qué JSON para Output en lugar de Formato Binario?

**JSON:**
- Pro: Human-readable, fácil de debuggear
- Con: Tamaño 2-3x mayor que binario, parsing más lento

**Alternativas:** Parquet, Avro, Protocol Buffers
- Pro: Compacto, eficiente
- Con: No human-readable, requiere librerías especiales

**Decisión:** JSON para simplicidad en proyecto académico. Para producción, considerar Parquet.

---

## Métricas y Observabilidad

### Logging

**Niveles de Log:**
- `INFO`: Eventos normales (job submitted, task completed)
- `WARN`: Situaciones recuperables (worker timeout, retry)
- `ERROR`: Fallos críticos (max retries exceeded, job failed)

**Ejemplo de Logs:**

```
2024-01-15 10:25:31 INFO  [Master] 🚀 Master iniciando en puerto 8080
2024-01-15 10:25:32 INFO  [Scheduler] 📋 Scheduler iniciado
2024-01-15 10:25:45 INFO  [Worker] worker-1 ✅ Registrado con master
2024-01-15 10:26:00 INFO  [Master] 📥 Job recibido: wordcount (job-123)
2024-01-15 10:26:01 INFO  [Scheduler] 📋 Planificando job job-123...
2024-01-15 10:26:01 INFO  [Scheduler] 📊 DAG analizado: 3 stages
2024-01-15 10:26:01 INFO  [Scheduler] ▶️  Ejecutando Stage 0: [read]
2024-01-15 10:26:02 INFO  [Scheduler] 📤 Asignando tarea job-123-read-p0 a worker-1
2024-01-15 10:26:05 INFO  [Worker] worker-1 ▶️  Ejecutando tarea job-123-read-p0...
2024-01-15 10:26:08 INFO  [Worker] worker-1 ✅ Tarea job-123-read-p0 completada
2024-01-15 10:26:15 INFO  [Scheduler] ✨ Job job-123 completado en 14.5s
```

### Métricas Expuestas

Endpoint: `GET /api/v1/metrics`

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "jobs": {
    "total": 42,
    "pending": 2,
    "running": 5,
    "completed": 34,
    "failed": 1
  },
  "tasks": {
    "total": 1250,
    "pending": 15,
    "running": 30,
    "completed": 1200,
    "failed": 5
  },
  "workers": {
    "total": 4,
    "idle": 1,
    "busy": 3,
    "down": 0
  },
  "performance": {
    "avg_task_duration_sec": 2.5,
    "total_records_processed": 15000000,
    "throughput_records_per_sec": 8500
  }
}
```


## Referencias

- [MapReduce: Simplified Data Processing on Large Clusters](https://research.google/pubs/pub62/)
- [Apache Spark Architecture](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Resilient Distributed Datasets (RDD Paper)](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
- [Go Concurrency Patterns](https://go.dev/blog/pipelines)

---

## Conclusión

PSO Batch implementa los conceptos fundamentales de sistemas de procesamiento distribuido:
- **Particionado de datos** para paralelización
- **Coordinación master-worker** con fault tolerance
- **Pipeline de transformaciones** mediante DAG
- **Gestión de memoria** con spillover


