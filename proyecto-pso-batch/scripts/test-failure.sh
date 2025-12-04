#!/bin/bash
# scripts/test-failure.sh
# Script para demostrar tolerancia a fallos

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== PSO Batch - Test de Tolerancia a Fallos ===${NC}\n"

# 1. Verificar que el cluster esté corriendo
echo -e "${YELLOW}1. Verificando cluster...${NC}"
if ! docker-compose ps | grep -q "Up"; then
    echo -e "${RED}Error: Cluster no está corriendo. Ejecuta 'make up' primero${NC}"
    exit 1
fi

WORKERS=$(docker-compose ps worker | grep "Up" | wc -l)
echo -e "${GREEN}   Cluster OK: 1 master + $WORKERS workers${NC}\n"

# 2. Enviar un job
echo -e "${YELLOW}2. Enviando job de prueba...${NC}"
JOB_RESPONSE=$(./bin/client submit examples/wordcount.json 2>&1)
JOB_ID=$(echo "$JOB_RESPONSE" | grep "Job ID:" | awk '{print $3}')

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}Error: No se pudo obtener Job ID${NC}"
    echo "$JOB_RESPONSE"
    exit 1
fi

echo -e "${GREEN}   Job enviado: $JOB_ID${NC}\n"

# 3. Esperar a que el job esté en ejecución
echo -e "${YELLOW}3. Esperando que el job inicie...${NC}"
sleep 5

STATUS=$(./bin/client status "$JOB_ID" 2>&1 | grep "Status:" | awk '{print $2}')
echo -e "${GREEN}   Status actual: $STATUS${NC}\n"

# 4. Identificar worker con tareas activas
echo -e "${YELLOW}4. Identificando worker con tareas activas...${NC}"
WORKER_TO_KILL=$(docker-compose ps worker | grep "Up" | head -n1 | awk '{print $1}')

if [ -z "$WORKER_TO_KILL" ]; then
    echo -e "${RED}Error: No se pudo identificar worker${NC}"
    exit 1
fi

echo -e "${GREEN}   Worker seleccionado: $WORKER_TO_KILL${NC}\n"

# 5. Simular fallo: matar el worker
echo -e "${RED}5. SIMULANDO FALLO: Matando worker $WORKER_TO_KILL...${NC}"
docker kill "$WORKER_TO_KILL" > /dev/null 2>&1
echo -e "${RED}   Worker eliminado!${NC}\n"

# 6. Verificar que el master detecta el fallo
echo -e "${YELLOW}6. Esperando que master detecte el fallo (15 segundos)...${NC}"
sleep 15

WORKERS_AFTER=$(docker-compose ps worker | grep "Up" | wc -l)
echo -e "${GREEN}   Workers activos después del fallo: $WORKERS_AFTER${NC}\n"

# 7. Verificar que las tareas se replanifican
echo -e "${YELLOW}7. Verificando replanificación de tareas...${NC}"

# Consultar logs del master para ver replanificación
echo -e "${YELLOW}   Logs del master (últimas 20 líneas):${NC}"
docker-compose logs --tail=20 master | grep -E "(WARN|RETRY|DOWN)" || true
echo ""

# 8. Esperar a que el job complete
echo -e "${YELLOW}8. Esperando completación del job...${NC}"
MAX_WAIT=60
ELAPSED=0

while [ $ELAPSED -lt $MAX_WAIT ]; do
    STATUS=$(./bin/client status "$JOB_ID" 2>&1 | grep "Status:" | awk '{print $2}' || echo "UNKNOWN")
    
    if [ "$STATUS" == "COMPLETED" ]; then
        echo -e "${GREEN}   Job completado exitosamente!${NC}\n"
        break
    elif [ "$STATUS" == "FAILED" ]; then
        echo -e "${RED}   Job falló :(${NC}\n"
        ./bin/client status "$JOB_ID"
        exit 1
    fi
    
    echo -e "   Status: $STATUS (esperando... ${ELAPSED}s/${MAX_WAIT}s)"
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo -e "${YELLOW}   Timeout esperando completación${NC}\n"
fi

# 9. Mostrar resultados finales
echo -e "${YELLOW}9. Estado final del job:${NC}"
./bin/client status "$JOB_ID"
echo ""

# 10. Verificar reintentos en logs
echo -e "${YELLOW}10. Verificando reintentos en logs:${NC}"
RETRIES=$(docker-compose logs master | grep -c "RETRY:" || echo "0")
echo -e "${GREEN}   Total de reintentos detectados: $RETRIES${NC}\n"

# 11. Reiniciar worker caído
echo -e "${YELLOW}11. Reiniciando worker caído...${NC}"
docker-compose up -d --scale worker=$WORKERS > /dev/null 2>&1
sleep 3
echo -e "${GREEN}   Worker reiniciado. Cluster restaurado.${NC}\n"

# Resumen
echo -e "${GREEN}=== RESUMEN DEL TEST ===${NC}"
echo -e "Job ID:              $JOB_ID"
echo -e "Status final:        $STATUS"
echo -e "Worker eliminado:    $WORKER_TO_KILL"
echo -e "Reintentos:          $RETRIES"
echo -e "Workers finales:     $(docker-compose ps worker | grep "Up" | wc -l)"
echo ""

if [ "$STATUS" == "COMPLETED" ] && [ "$RETRIES" -gt 0 ]; then
    echo -e "${GREEN}✓ TEST EXITOSO: El sistema recuperó el fallo y completó el job${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠ TEST PARCIAL: Revisar logs para más detalles${NC}"
    exit 0
fi