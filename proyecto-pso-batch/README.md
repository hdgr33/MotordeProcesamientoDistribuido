# Motor de Procesamiento Distribuido - PSO Batch

Sistema distribuido de procesamiento de datos en batch implementado en Go, basado en arquitectura Master-Worker con DAG (Directed Acyclic Graph) para definir pipelines de transformación de datos.

## Declaración de Uso de IA

Este proyecto fue desarrollado con asistencia de Claude (Anthropic) para:
- Generación de estructura de código base
- Implementación de algoritmos de planificación
- Diseño de tests unitarios
- Documentación técnica

El código fue revisado, modificado y adaptado para cumplir los requisitos del proyecto académico.

## Descripción

Motor de procesamiento distribuido que implementa:
- Análisis de DAG con topological sort
- Planificación de tareas con round-robin y load balancing
- Sistema de caché con spill a disco (límite 100MB en memoria)
- Tolerancia a fallos con heartbeats y reintentos (máximo 3)
- 6 operadores: read_csv, map, filter, flat_map, reduce_by_key, join

## Arquitectura

Componentes principales:
- Master: Coordina workers, planifica tareas, monitorea estado
- Workers: Ejecutan operadores, reportan resultados, mantienen caché
- Cliente: CLI para enviar jobs y consultar estado

Comunicación:
- Master: HTTP REST API en puerto 8080
- Workers: HTTP endpoints en puertos 9001-9003
- Protocolo: JSON sobre HTTP

Conceptos de Sistemas Operativos implementados:
- Procesos: Master y Workers como procesos independientes
- Hilos: Goroutines para procesamiento concurrente
- IPC: HTTP para comunicación entre procesos
- Planificación: Round-robin con prioridad por carga
- Memoria: Gestión de caché con límites y spill
- Tolerancia a fallos: Detección de caídas y replanificación

## Requisitos

- Go 1.21 o superior
- Docker y Docker Compose
- 4GB RAM mínimo
- 10GB espacio en disco

## Instalación

Clonar repositorio:
```bash
git clone https://github.com/hdgr33/MotordeProcesamientoDistribuido.git
cd MotordeProcesamientoDistribuido/proyecto-pso-batch
```

Instalar dependencias:
```bash
go mod download
```

Compilar:
```bash
make build
```

Ejecutar tests:
```bash
make test
```

## Uso

Iniciar cluster con Docker:
```bash
docker-compose up -d
```

Verificar workers activos:
```bash
./bin/client workers
```

Enviar job:
```bash
./bin/client submit examples/wordcount.json
```

Ver estado:
```bash
./bin/client status JOB_ID
```

Ver resultados:
```bash
./bin/client results JOB_ID
```

Detener cluster:
```bash
docker-compose down
```

## Estructura del Proyecto

```
proyecto-pso-batch/
├── master/          # Coordinador principal
│   ├── main.go      # Servidor HTTP
│   ├── scheduler.go # Planificador de tareas
│   └── api.go       # Endpoints REST
├── worker/          # Procesadores
│   ├── main.go      # Servidor worker
│   ├── operators.go # Implementación de operadores
│   └── cache.go     # Sistema de caché
├── client/          # Cliente CLI
│   └── main.go
├── common/          # Tipos compartidos
│   ├── types/
│   └── protocol/
├── data/            # Datos de entrada/salida
│   ├── input/
│   ├── output/
│   └── spill/
├── examples/        # Jobs de ejemplo
│   └── wordcount.json
└── docs/            # Documentación adicional
```

## Ejemplo de Job: WordCount

Archivo: examples/wordcount.json

```json
{
  "name": "wordcount-example",
  "dag": {
    "nodes": [
      {
        "id": "read",
        "op": "read_csv",
        "path": "data/input/text.csv",
        "partitions": 2
      },
      {
        "id": "tokenize",
        "op": "flat_map",
        "fn": "split_words"
      },
      {
        "id": "lowercase",
        "op": "map",
        "fn": "to_lower"
      },
      {
        "id": "count",
        "op": "reduce_by_key",
        "key": "word",
        "fn": "count"
      }
    ],
    "edges": [
      ["read", "tokenize"],
      ["tokenize", "lowercase"],
      ["lowercase", "count"]
    ]
  },
  "parallelism": 2
}
```

Ejecución:
```bash
./bin/client submit examples/wordcount.json
```

## Operadores Disponibles

read_csv: Lee archivo CSV y lo convierte a registros
- Parámetros: path (archivo), partitions (número de particiones)

map: Aplica función a cada registro
- Funciones: to_lower, to_upper, trim

filter: Filtra registros según condición
- Funciones: non_empty, has_text

flat_map: Genera múltiples registros desde uno
- Funciones: split_words, tokenize, split_lines

reduce_by_key: Agrupa y reduce por clave
- Funciones: count, sum, collect, first, last

join: Une dos datasets por clave común
- Tipo: inner join

## Tolerancia a Fallos

El sistema implementa:
- Heartbeats cada 2 segundos
- Detección de workers caídos (sin heartbeat > 6s)
- Replanificación automática de tareas en workers activos
- Máximo 3 reintentos por tarea
- Logs estructurados de fallos

Demostración:
```bash
# Enviar job
./bin/client submit examples/wordcount.json

# Matar un worker
docker kill pso-batch-worker-1

# Ver logs del master detectando fallo
docker-compose logs master | tail -20

# El job debe completar exitosamente
./bin/client status JOB_ID
```

## Tests

El proyecto incluye tests unitarios completos:

```bash
# Ejecutar todos los tests
make test

# Tests por módulo
cd master && go test -v
cd worker && go test -v

# Coverage
make test-coverage
```

Cobertura actual:
- Master: 22.8% (lógica de planificación y DAG)
- Worker: 56.6% (operadores y caché)
- Total: ~40% (lógica core cubierta)

Tests incluidos:
- Análisis de DAG (topological sort, detección de ciclos)
- Selección de workers (load balancing)
- Manejo de resultados de tareas
- Sistema de caché (spill to disk, límites de memoria)
- Todos los operadores (6 operadores con múltiples casos)
- Tolerancia a fallos (reintentos, replanificación)

## Benchmarks

Sistema probado con JMeter:
- 50 jobs procesados exitosamente
- 0% error rate
- Throughput: 458 req/s
- Latencia promedio: 440ms
- 10 usuarios concurrentes sin degradación

Para ejecutar benchmarks:
```bash
# Con JMeter
jmeter -n -t tests/pso-batch-load-test.jmx -l results.jtl

# Simple
./scripts/benchmark-simple.sh
```

## API REST

Endpoints del Master (puerto 8080):

POST /api/v1/jobs
- Enviar nuevo job
- Body: definición de job en JSON
- Retorna: job_id y status

GET /api/v1/jobs/:id
- Consultar estado de job
- Retorna: job completo con progreso

GET /api/v1/jobs/:id/results
- Obtener resultados de job completado
- Retorna: rutas de archivos de salida

GET /api/v1/workers
- Listar workers activos
- Retorna: lista de workers con estado

GET /api/v1/metrics
- Obtener métricas del sistema
- Retorna: estadísticas de jobs y tasks

POST /api/v1/workers/register
- Registrar nuevo worker (uso interno)

POST /api/v1/workers/heartbeat
- Heartbeat de worker (uso interno)

POST /api/v1/tasks/:id/result
- Reportar resultado de tarea (uso interno)

## Logs

El sistema genera logs estructurados sin emojis:

Formatos:
- INFO: Información general
- WARN: Advertencias (workers caídos, timeouts)
- ERROR: Errores críticos
- SUCCESS: Operaciones exitosas
- RETRY: Reintentos de tareas

Ejemplo:
```
INFO: Master starting on port 8080
INFO: Worker worker-1 registered successfully
INFO: Job job-123 submitted with 6 tasks
WARN: Worker worker-2 missed heartbeat
RETRY: Reintentando tarea task-456 (intento 2/3)
SUCCESS: Job job-123 completado
```

## Comandos Makefile

```bash
make build          # Compilar binarios
make test           # Ejecutar tests
make test-coverage  # Tests con coverage
make clean          # Limpiar binarios
make docker-build   # Construir imágenes Docker
make docker-up      # Iniciar cluster
make docker-down    # Detener cluster
make docker-logs    # Ver logs
make help           # Ver ayuda completa
```

## Desarrollo

Ejecutar localmente sin Docker:

Terminal 1 - Master:
```bash
make run-master
```

Terminal 2 - Worker 1:
```bash
make run-worker1
```

Terminal 3 - Worker 2:
```bash
make run-worker2
```

Terminal 4 - Cliente:
```bash
./bin/client submit examples/wordcount.json
```

## Limitaciones Conocidas

- Máximo 100MB de datos en memoria por worker (configurable)
- Reintentos limitados a 3 por tarea
- Sin persistencia de estado (memoria volátil)
- Workers deben registrarse al iniciar
- No soporta joins de más de 2 datasets

## Documentación Adicional

- ARCHITECTURE.md: Diseño técnico detallado
- BENCHMARK.md: Resultados de performance
- INSTALLATION_GUIDE.md: Guía de instalación paso a paso
- VIDEO_GUIDE.md: Guía para grabar video demostrativo

## Autores

- Nombre del estudiante
- Carné
- Proyecto Sistemas Operativos - TEC

## Licencia

Proyecto académico - Tecnológico de Costa Rica
