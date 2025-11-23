#!/bin/bash
# scripts/test-failure.sh - Test de tolerancia a fallos

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo ""
echo -e "${CYAN}=========================================${NC}"
echo -e "${GREEN}TEST DE TOLERANCIA A FALLOS${NC}"
echo -e "${CYAN}=========================================${NC}"
echo ""

MASTER_URL="http://localhost:8080"

# 1. Verificar que el sistema está corriendo
echo -e "${YELLOW}1. Verificando que el sistema está activo...${NC}"
if ! curl -s "$MASTER_URL/api/v1/metrics" > /dev/null; then
    echo -e "${RED}ERROR: Master no responde${NC}"
    echo "Asegúrate de que master, worker1 y worker2 estén corriendo"
    exit 1
fi

WORKERS=$(curl -s "$MASTER_URL/api/v1/workers" | grep -c "worker" || echo "0")
echo -e "${GREEN}OK: $WORKERS workers activos${NC}"
echo ""

# 2. Enviar job
echo -e "${YELLOW}2. Enviando job WordCount...${NC}"
JOB_RESPONSE=$(curl -s -X POST "$MASTER_URL/api/v1/jobs" \
    -H "Content-Type: application/json" \
    -d @examples/wordcount.json)

JOB_ID=$(echo "$JOB_RESPONSE" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}ERROR: No se pudo crear el job${NC}"
    exit 1
fi

echo -e "${GREEN}OK: Job creado: $JOB_ID${NC}"
echo ""

# 3. Esperar a que inicie
echo -e "${YELLOW}3. Esperando que el job inicie...${NC}"
sleep 3

# 4. Verificar estado
echo -e "${YELLOW}4. Verificando estado del job...${NC}"
STATUS=$(curl -s "$MASTER_URL/api/v1/jobs/$JOB_ID")
JOB_STATUS=$(echo "$STATUS" | grep -o '"Status":"[^"]*"' | cut -d'"' -f4)
echo -e "${GREEN}Estado: $JOB_STATUS${NC}"
echo ""

# 5. MATAR UN WORKER
echo -e "${YELLOW}5. MATANDO WORKER-1 en 5 segundos...${NC}"
echo "   Presiona Ctrl+C ahora si no quieres matar el worker"
sleep 5

echo -e "${RED}MATANDO WORKER-1...${NC}"
pkill -f "WORKER_ID=worker-1" || true

echo -e "${RED}WORKER-1 MUERTO${NC}"
echo ""

# 6. Monitorear recuperación
echo -e "${YELLOW}6. Monitoreando recuperación durante 30 segundos...${NC}"
echo ""

for i in {1..15}; do
    STATUS=$(curl -s "$MASTER_URL/api/v1/jobs/$JOB_ID" 2>/dev/null || echo "{}")
    
    PROGRESS=$(echo "$STATUS" | grep -o '"Progress":[0-9.]*' | cut -d':' -f2)
    JOB_STATUS=$(echo "$STATUS" | grep -o '"Status":"[^"]*"' | cut -d'"' -f4)
    TASKS_DONE=$(echo "$STATUS" | grep -o '"TasksDone":[0-9]*' | cut -d':' -f2)
    TASKS_TOTAL=$(echo "$STATUS" | grep -o '"TasksTotal":[0-9]*' | cut -d':' -f2)
    
    if [ -z "$PROGRESS" ]; then
        PROGRESS="0"
    fi
    
    WORKERS_ALIVE=$(curl -s "$MASTER_URL/api/v1/workers" 2>/dev/null | grep -c "IDLE\|BUSY" || echo "0")
    
    echo "[$i/15] Status: $JOB_STATUS | Progress: ${PROGRESS}% | Tasks: $TASKS_DONE/$TASKS_TOTAL | Workers Activos: $WORKERS_ALIVE"
    
    if [ "$JOB_STATUS" = "COMPLETED" ]; then
        echo ""
        echo -e "${GREEN}=========================================${NC}"
        echo -e "${GREEN}EXITO: JOB COMPLETO AUNQUE WORKER-1 FALLÓ${NC}"
        echo -e "${GREEN}=========================================${NC}"
        echo ""
        echo "Resultados:"
        curl -s "$MASTER_URL/api/v1/jobs/$JOB_ID/results" | python3 -m json.tool 2>/dev/null || \
            curl -s "$MASTER_URL/api/v1/jobs/$JOB_ID/results"
        exit 0
    fi
    
    sleep 2
done

echo ""
echo -e "${YELLOW}Job aún en progreso${NC}"
echo "Estado final:"
curl -s "$MASTER_URL/api/v1/jobs/$JOB_ID" | python3 -m json.tool 2>/dev/null || \
    curl -s "$MASTER_URL/api/v1/jobs/$JOB_ID"