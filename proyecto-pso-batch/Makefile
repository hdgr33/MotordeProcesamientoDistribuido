# PSO Batch - Makefile

.PHONY: all build test clean up down logs workers wordcount benchmark help

# Variables
MASTER_PORT=8080
WORKERS=3

# Colores para output
GREEN=\033[0;32m
YELLOW=\033[1;33m
NC=\033[0m # No Color

all: build

help:
	@echo "PSO Batch - Motor de Procesamiento Distribuido"
	@echo ""
	@echo "Comandos disponibles:"
	@echo "  make build       - Compilar todos los componentes"
	@echo "  make test        - Ejecutar suite de tests"
	@echo "  make up          - Iniciar cluster (1 master + 3 workers)"
	@echo "  make down        - Detener cluster"
	@echo "  make logs        - Ver logs del cluster"
	@echo "  make wordcount   - Ejecutar ejemplo WordCount"
	@echo "  make benchmark   - Ejecutar benchmarks de performance"
	@echo "  make clean       - Limpiar archivos generados"

# Build
build:
	@echo "$(GREEN)Building project...$(NC)"
	@cd master && go build -o ../bin/master
	@cd worker && go build -o ../bin/worker
	@cd client && go build -o ../bin/client
	@echo "$(GREEN)Build complete!$(NC)"

# Tests
test:
	@echo "$(GREEN)Running tests...$(NC)"
	@echo "\n=== Master Tests ==="
	@cd master && go test -v -cover
	@echo "\n=== Worker Tests ==="
	@cd worker && go test -v -cover
	@echo "\n=== Common Tests ==="
	@cd common && go test -v -cover ./...
	@echo "$(GREEN)All tests passed!$(NC)"

test-coverage:
	@echo "$(GREEN)Generating coverage report...$(NC)"
	@cd master && go test -coverprofile=coverage.out && go tool cover -html=coverage.out -o coverage.html
	@cd worker && go test -coverprofile=coverage.out && go tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)Coverage reports generated: master/coverage.html, worker/coverage.html$(NC)"

# Docker operations
build-docker:
	@echo "$(GREEN)Building Docker images...$(NC)"
	@docker-compose build
	@echo "$(GREEN)Docker images ready!$(NC)"

up:
	@echo "$(GREEN)Starting cluster (1 master + $(WORKERS) workers)...$(NC)"
	@docker-compose up -d --scale worker=$(WORKERS)
	@sleep 3
	@echo "$(GREEN)Cluster started!$(NC)"
	@make status

down:
	@echo "$(YELLOW)Stopping cluster...$(NC)"
	@docker-compose down
	@echo "$(GREEN)Cluster stopped$(NC)"

restart: down up

status:
	@echo "=== Cluster Status ==="
	@docker-compose ps

logs:
	@docker-compose logs -f

logs-master:
	@docker-compose logs -f master

logs-worker:
	@docker-compose logs -f worker

# Client operations
workers:
	@echo "=== Active Workers ==="
	@./bin/client workers

metrics:
	@echo "=== System Metrics ==="
	@./bin/client metrics

# Examples
wordcount:
	@echo "$(GREEN)Running WordCount example...$(NC)"
	@./bin/client submit examples/wordcount.json
	@echo "\n$(YELLOW)Job submitted! Use 'make status JOB_ID=<id>' to check progress$(NC)"

wordcount-watch:
	@if [ -z "$(JOB_ID)" ]; then \
		echo "$(YELLOW)Usage: make wordcount-watch JOB_ID=<job-id>$(NC)"; \
		exit 1; \
	fi
	@./bin/client watch $(JOB_ID)

job-status:
	@if [ -z "$(JOB_ID)" ]; then \
		echo "$(YELLOW)Usage: make job-status JOB_ID=<job-id>$(NC)"; \
		exit 1; \
	fi
	@./bin/client status $(JOB_ID)

job-results:
	@if [ -z "$(JOB_ID)" ]; then \
		echo "$(YELLOW)Usage: make job-results JOB_ID=<job-id>$(NC)"; \
		exit 1; \
	fi
	@./bin/client results $(JOB_ID)

# Benchmarks
benchmark:
	@echo "$(GREEN)Running benchmarks...$(NC)"
	@./scripts/run-benchmarks.sh

benchmark-go:
	@echo "$(GREEN)Running Go benchmarks...$(NC)"
	@cd master && go test -bench=. -benchmem
	@cd worker && go test -bench=. -benchmem

# Test de fallos
test-failure:
	@echo "$(GREEN)Testing failure recovery...$(NC)"
	@./scripts/test-failure.sh

# Cleanup
clean:
	@echo "$(YELLOW)Cleaning up...$(NC)"
	@rm -rf bin/
	@rm -rf data/output/*
	@rm -rf data/spill/*
	@rm -f master/coverage.out master/coverage.html
	@rm -f worker/coverage.out worker/coverage.html
	@docker-compose down -v
	@echo "$(GREEN)Cleanup complete$(NC)"

# Desarrollo
fmt:
	@echo "$(GREEN)Formatting code...$(NC)"
	@go fmt ./...

lint:
	@echo "$(GREEN)Running linter...$(NC)"
	@golangci-lint run ./...

# Instalación de dependencias
deps:
	@echo "$(GREEN)Installing dependencies...$(NC)"
	@go mod download
	@go mod tidy

# Setup inicial
setup: deps
	@echo "$(GREEN)Initial setup...$(NC)"
	@mkdir -p bin
	@mkdir -p data/input
	@mkdir -p data/output
	@mkdir -p data/spill
	@echo "$(GREEN)Setup complete! Run 'make build' to compile$(NC)"

# Demo completo
demo: clean setup build build-docker up
	@echo "\n$(GREEN)=== PSO Batch Demo ===$(NC)"
	@sleep 5
	@echo "\n$(YELLOW)1. Checking workers...$(NC)"
	@make workers
	@sleep 2
	@echo "\n$(YELLOW)2. Submitting WordCount job...$(NC)"
	@make wordcount
	@echo "\n$(GREEN)Demo complete! Check logs with: make logs$(NC)"
