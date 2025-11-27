#!/bin/bash
# scripts/benchmark-spill.sh - Benchmark del sistema de caché y spill

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo ""
echo -e "${CYAN}=========================================${NC}"
echo -e "${GREEN}BENCHMARK: CACHE Y SPILL TO DISK${NC}"
echo -e "${CYAN}=========================================${NC}"
echo ""

MASTER_URL="http://localhost:8080"

# Verificar sistema
echo -e "${YELLOW}1. Verificando sistema...${NC}"
if ! curl -s "$MASTER_URL/api/v1/metrics" > /dev/null; then
    echo -e "${RED}ERROR: Master no responde${NC}"
    echo "Inicia: make run-master"
    exit 1
fi
echo -e "${GREEN}OK: Sistema activo${NC}"
echo ""

# Generar dataset si no existe
echo -e "${YELLOW}2. Verificando dataset grande...${NC}"
if [ ! -f "data/input/large-dataset.csv" ]; then
    echo -e "${YELLOW}   Generando dataset de 1M registros...${NC}"
    chmod +x scripts/generate-large-dataset.sh
    ./scripts/generate-large-dataset.sh
fi

DATASET_SIZE=$(du -h "data/input/large-dataset.csv" | cut -f1)
echo -e "${GREEN}OK: Dataset listo ($DATASET_SIZE)${NC}"
echo ""

# Enviar job
echo -e "${YELLOW}3. Enviando job de benchmark...${NC}"
JOB_RESPONSE=$(curl -s -X POST "$MASTER_URL/api/v1/jobs" \
    -H "Content-Type: application/json" \
    -d @examples/benchmark-large.json)

JOB_ID=$(echo "$JOB_RESPONSE" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}ERROR: No se pudo crear el job${NC}"
    exit 1
fi

echo -e "${GREEN}OK: Job creado: $JOB_ID${NC}"
echo ""

# Monitorear ejecución
echo -e "${YELLOW}4. Ejecutando benchmark (esto puede tomar varios minutos)...${NC}"
echo ""

START_TIME=$(date +%s)

for i in {1..120}; do
    STATUS=$(curl -s "$MASTER_URL/api/v1/jobs/$JOB_ID" 2>/dev/null || echo "{}")
    
    PROGRESS=$(echo "$STATUS" | grep -o '"Progress":[0-9.]*' | cut -d':' -f2)
    JOB_STATUS=$(echo "$STATUS" | grep -o '"Status":"[^"]*"' | cut -d'"' -f4)
    TASKS_DONE=$(echo "$STATUS" | grep -o '"TasksDone":[0-9]*' | cut -d':' -f2)
    TASKS_TOTAL=$(echo "$STATUS" | grep -o '"TasksTotal":[0-9]*' | cut -d':' -f2)
    
    if [ -z "$PROGRESS" ]; then
        PROGRESS="0"
    fi
    
    ELAPSED=$(($(date +%s) - START_TIME))
    
    echo "[$i/120] Status: $JOB_STATUS | Progress: ${PROGRESS}% | Tasks: $TASKS_DONE/$TASKS_TOTAL | Elapsed: ${ELAPSED}s"
    
    if [ "$JOB_STATUS" = "COMPLETED" ]; then
        TOTAL_TIME=$ELAPSED
        echo ""
        echo -e "${GREEN}=========================================${NC}"
        echo -e "${GREEN}BENCHMARK COMPLETADO${NC}"
        echo -e "${GREEN}=========================================${NC}"
        echo ""
        
        # Estadísticas
        echo -e "${CYAN}Estadísticas:${NC}"
        echo "  Tiempo total: ${TOTAL_TIME}s"
        echo "  Tamaño dataset: $DATASET_SIZE"
        echo "  Tareas completadas: $TASKS_TOTAL"
        echo "  Workers utilizados: 2"
        echo ""
        
        # Calcular throughput
        if [ "$TOTAL_TIME" -gt 0 ]; then
            THROUGHPUT=$((1000000 / TOTAL_TIME))
            echo -e "${CYAN}Throughput:${NC}"
            echo "  Records/segundo: $THROUGHPUT"
            echo "  MB/segundo: $(echo "scale=2; $(echo $DATASET_SIZE | grep -o '^[0-9]*') / $TOTAL_TIME" | bc)"
        fi
        
        echo ""
        echo -e "${GREEN}Ver spill files:${NC}"
        echo "  ls -lh data/spill/"
        echo ""
        
        exit 0
    fi
    
    sleep 1
done

echo ""
echo -e "${YELLOW}Benchmark aún en progreso después de 2 minutos${NC}"
echo "Ejecutando en background. Verifica manualmente:"
echo "  ./bin/client watch $JOB_ID"