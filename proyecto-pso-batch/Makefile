.PHONY: build clean run-master run-worker test

# Variables
MASTER_PORT=8080
WORKER1_PORT=9001
WORKER2_PORT=9002

# Build
build:
	@echo "🔨 Compilando componentes..."
	go build -o bin/master ./master
	go build -o bin/worker ./worker
	go build -o bin/client ./client
	@echo "✅ Build completado"

# Clean
clean:
	@echo "🧹 Limpiando binarios..."
	rm -rf bin/
	rm -rf data/output/*
	@echo "✅ Limpieza completada"

# Run Master
run-master: build
	@echo "🚀 Iniciando Master en puerto $(MASTER_PORT)..."
	./bin/master

# Run Workers (en terminales separadas)
run-worker1: build
	@echo "🚀 Iniciando Worker 1 en puerto $(WORKER1_PORT)..."
	WORKER_ID=worker-1 WORKER_PORT=$(WORKER1_PORT) MASTER_URL=http://localhost:$(MASTER_PORT) ./bin/worker

run-worker2: build
	@echo "🚀 Iniciando Worker 2 en puerto $(WORKER2_PORT)..."
	WORKER_ID=worker-2 WORKER_PORT=$(WORKER2_PORT) MASTER_URL=http://localhost:$(MASTER_PORT) ./bin/worker

# Run all (requiere tmux o usar script separado)
run-all: build
	@echo "⚠️  Ejecuta en terminales separadas:"
	@echo "Terminal 1: make run-master"
	@echo "Terminal 2: make run-worker1"
	@echo "Terminal 3: make run-worker2"

# Test
test:
	@echo "🧪 Ejecutando tests..."
	go test -v ./...

# Format
fmt:
	@echo "🎨 Formateando código..."
	go fmt ./...

# Install dependencies
deps:
	@echo "📦 Descargando dependencias..."
	go mod download
	go mod tidy

# Client commands (shortcuts)
submit:
	@if [ -z "$(JOB)" ]; then \
		echo "❌ Error: especifica el archivo con JOB="; \
		echo "Ejemplo: make submit JOB=examples/wordcount.json"; \
		exit 1; \
	fi
	./bin/client submit $(JOB)

status:
	@if [ -z "$(ID)" ]; then \
		echo "❌ Error: especifica el job ID con ID="; \
		echo "Ejemplo: make status ID=job-1234567890"; \
		exit 1; \
	fi
	./bin/client status $(ID)

watch:
	@if [ -z "$(ID)" ]; then \
		echo "❌ Error: especifica el job ID con ID="; \
		echo "Ejemplo: make watch ID=job-1234567890"; \
		exit 1; \
	fi
	./bin/client watch $(ID)

workers:
	./bin/client workers

# Setup inicial
setup:
	@chmod +x scripts/setup.sh
	@./scripts/setup.sh

# Help
help:
	@echo "Comandos disponibles:"
	@echo ""
	@echo "Build & Run:"
	@echo "  make build        - Compilar todos los componentes"
	@echo "  make setup        - Setup inicial del proyecto"
	@echo "  make run-master   - Ejecutar Master"
	@echo "  make run-worker1  - Ejecutar Worker 1"
	@echo "  make run-worker2  - Ejecutar Worker 2"
	@echo ""
	@echo "Cliente:"
	@echo "  make submit JOB=<file>  - Enviar job"
	@echo "  make status ID=<id>     - Ver estado de job"
	@echo "  make watch ID=<id>      - Monitorear job"
	@echo "  make workers            - Listar workers"
	@echo ""
	@echo "Otros:"
	@echo "  make test         - Ejecutar tests"
	@echo "  make clean        - Limpiar binarios"
	@echo "  make fmt          - Formatear código"