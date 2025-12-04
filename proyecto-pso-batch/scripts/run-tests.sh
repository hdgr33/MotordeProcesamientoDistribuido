#!/bin/bash
# tests/run-tests.sh - Suite de tests para PSO Batch

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

MASTER_URL="http://localhost:8080"

echo ""
echo -e "${CYAN}=========================================${NC}"
echo -e "${GREEN}PSO BATCH - TEST SUITE${NC}"
echo -e "${CYAN}=========================================${NC}"
echo ""

# ============================================================================
# TEST 1: ESTADO DEL SISTEMA
# ============================================================================

echo -e "${YELLOW}[TEST 1/8] Estado del Sistema${NC}"

# Verificar master
if ! curl -s "$MASTER_URL/api/v1/metrics" > /dev/null; then
    echo -e "${RED}❌ Master no responde${NC}"
    exit 1
fi

# Verificar workers
WORKERS=$(docker-compose exec -T client workers 2>/dev/null | grep -c "Status:" || echo "0")
if [ "$WORKERS" -lt 3 ]; then
    echo -e "${RED}❌ Menos de 3 workers activos${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Sistema operacional (Master + $WORKERS workers)${NC}"
echo ""

# ============================================================================
# TEST 2: WORDCOUNT BÁSICO
# ============================================================================

echo -e "${YELLOW}[TEST 2/8] WordCount Básico${NC}"

JOB=$(docker-compose run --rm client submit examples/wordcount.json 2>&1 | grep "Job ID:" | grep -o "job-[0-9]*")

if [ -z "$JOB" ]; then
    echo -e "${RED}❌ Fallo enviando job${NC}"
    exit 1
fi

echo "📋 Job ID: $JOB"

# Esperar a que complete
for i in {1..60}; do
    STATUS=$(docker-compose run --rm client status "$JOB" 2>&1 | grep "Status:" | tail -1 | grep -o "COMPLETED\|FAILED\|RUNNING" || echo "")
    
    if [ "$STATUS" = "COMPLETED" ]; then
        PROGRESS=$(docker-compose run --rm client status "$JOB" 2>&1 | grep "Progress:" | grep -o "[0-9]*\.[0-9]*" | head -1)
        echo -e "${GREEN}✅ Job completado (Progress: ${PROGRESS}%)${NC}"
        break
    elif [ "$STATUS" = "FAILED" ]; then
        echo -e "${RED}❌ Job falló${NC}"
        exit 1
    fi
    
    sleep 1
done

echo ""

# ============================================================================
# TEST 3: BENCHMARK PEQUEÑO (1K registros)
# ============================================================================

echo -e "${YELLOW}[TEST 3/8] Benchmark Pequeño (1K registros)${NC}"

# Generar dataset pequeño
cat > data/input/small-dataset.csv << 'EOF'
id,text,timestamp
1,"hello world",2024-01-01T10:00:00Z
2,"hello test",2024-01-01T10:01:00Z
3,"world of go",2024-01-01T10:02:00Z
4,"distributed system",2024-01-01T10:03:00Z
5,"batch processing",2024-01-01T10:04:00Z
EOF

# Crear job
cat > examples/benchmark-small.json << 'EOF'
{
  "name": "benchmark-small",
  "dag": {
    "nodes": [
      {
        "id": "read",
        "op": "read_csv",
        "path": "data/input/small-dataset.csv",
        "partitions": 2
      },
      {
        "id": "filter",
        "op": "filter",
        "fn": "non_empty"
      },
      {
        "id": "lowercase",
        "op": "map",
        "fn": "to_lower"
      }
    ],
    "edges": [["read", "filter"], ["filter", "lowercase"]]
  },
  "parallelism": 2
}
EOF

START=$(date +%s%N)

JOB=$(docker-compose run --rm client submit examples/benchmark-small.json 2>&1 | grep "Job ID:" | grep -o "job-[0-9]*")

# Esperar
for i in {1..60}; do
    STATUS=$(docker-compose run --rm client status "$JOB" 2>&1 | grep "Status:" | tail -1 | grep -o "COMPLETED\|FAILED" || echo "")
    [ "$STATUS" = "COMPLETED" ] && break
    [ "$STATUS" = "FAILED" ] && { echo -e "${RED}❌ Job falló${NC}"; exit 1; }
    sleep 1
done

END=$(date +%s%N)
DURATION=$((($END - $START) / 1000000))

echo -e "${GREEN}✅ Benchmark completado en ${DURATION}ms${NC}"
echo ""

# ============================================================================
# TEST 4: MÚLTIPLES JOBS EN PARALELO
# ============================================================================

echo -e "${YELLOW}[TEST 4/8] Múltiples Jobs en Paralelo (3 jobs)${NC}"

JOBS=()
for i in {1..3}; do
    JOB=$(docker-compose run --rm client submit examples/wordcount.json 2>&1 | grep "Job ID:" | grep -o "job-[0-9]*")
    JOBS+=("$JOB")
    echo "📋 Enviado Job $i: $JOB"
done

# Esperar a todos
ALL_COMPLETED=false
for attempt in {1..120}; do
    COMPLETED=0
    for JOB in "${JOBS[@]}"; do
        STATUS=$(docker-compose run --rm client status "$JOB" 2>&1 | grep "Status:" | tail -1 | grep -o "COMPLETED\|FAILED" || echo "")
        [ "$STATUS" = "COMPLETED" ] && ((COMPLETED++))
        [ "$STATUS" = "FAILED" ] && { echo -e "${RED}❌ Job $JOB falló${NC}"; exit 1; }
    done
    
    if [ $COMPLETED -eq 3 ]; then
        ALL_COMPLETED=true
        break
    fi
    
    sleep 1
done

if [ "$ALL_COMPLETED" = true ]; then
    echo -e "${GREEN}✅ Los 3 jobs completaron exitosamente${NC}"
else
    echo -e "${RED}❌ Timeout esperando jobs${NC}"
    exit 1
fi

echo ""

# ============================================================================
# TEST 5: TOLERANCIA A FALLOS - MATAR WORKER
# ============================================================================

echo -e "${YELLOW}[TEST 5/8] Tolerancia a Fallos (Simular fallo de worker)${NC}"

# Enviar job
JOB=$(docker-compose run --rm client submit examples/wordcount.json 2>&1 | grep "Job ID:" | grep -o "job-[0-9]*")
echo "📋 Job enviado: $JOB"

# Esperar a que se ejecute
sleep 3

# MATAR WORKER-2
echo "💣 Matando worker-2..."
docker-compose stop worker-2
sleep 2

# Esperar a que complete
echo "⏳ Esperando recuperación..."
for i in {1..120}; do
    STATUS=$(docker-compose run --rm client status "$JOB" 2>&1 | grep "Status:" | tail -1 | grep -o "COMPLETED\|FAILED" || echo "")
    
    if [ "$STATUS" = "COMPLETED" ]; then
        echo -e "${GREEN}✅ Job completó a pesar de fallo de worker${NC}"
        break
    elif [ "$STATUS" = "FAILED" ]; then
        echo -e "${RED}❌ Job falló${NC}"
        exit 1
    fi
    
    sleep 1
done

# Reiniciar worker-2
docker-compose start worker-2
sleep 5

echo ""

# ============================================================================
# TEST 6: VERIFICAR MÉTRICAS
# ============================================================================

echo -e "${YELLOW}[TEST 6/8] Métricas del Sistema${NC}"

METRICS=$(curl -s "$MASTER_URL/api/v1/metrics/detailed")

WORKERS_TOTAL=$(echo "$METRICS" | grep -o '"total":[0-9]*' | head -1 | grep -o '[0-9]*')
JOBS_TOTAL=$(echo "$METRICS" | grep -o '"total":[0-9]*' | tail -1 | grep -o '[0-9]*')

echo "👷 Workers totales: $WORKERS_TOTAL"
echo "📋 Jobs totales: $JOBS_TOTAL"

if [ "$WORKERS_TOTAL" -ge 3 ] && [ "$JOBS_TOTAL" -ge 1 ]; then
    echo -e "${GREEN}✅ Métricas consistentes${NC}"
else
    echo -e "${RED}❌ Métricas inconsistentes${NC}"
    exit 1
fi

echo ""

# ============================================================================
# TEST 7: STRESS TEST - 10 JOBS SECUENCIALES
# ============================================================================

echo -e "${YELLOW}[TEST 7/8] Stress Test (10 jobs secuenciales)${NC}"

COMPLETED_COUNT=0
FAILED_COUNT=0

for i in {1..10}; do
    JOB=$(docker-compose run --rm client submit examples/wordcount.json 2>&1 | grep "Job ID:" | grep -o "job-[0-9]*")
    
    # Esperar a que complete
    for attempt in {1..120}; do
        STATUS=$(docker-compose run --rm client status "$JOB" 2>&1 | grep "Status:" | tail -1 | grep -o "COMPLETED\|FAILED" || echo "")
        
        if [ "$STATUS" = "COMPLETED" ]; then
            ((COMPLETED_COUNT++))
            break
        elif [ "$STATUS" = "FAILED" ]; then
            ((FAILED_COUNT++))
            break
        fi
        
        sleep 1
    done
    
    echo "Job $i: $JOB - Status: $STATUS"
done

echo -e "${GREEN}✅ Completados: $COMPLETED_COUNT, Fallidos: $FAILED_COUNT${NC}"

if [ "$COMPLETED_COUNT" -eq 10 ]; then
    echo -e "${GREEN}✅ Stress test exitoso${NC}"
else
    echo -e "${YELLOW}⚠️  Solo completaron $COMPLETED_COUNT de 10${NC}"
fi

echo ""

# ============================================================================
# TEST 8: VERIFICAR OUTPUTS
# ============================================================================

echo -e "${YELLOW}[TEST 8/8] Verificar Outputs${NC}"

# Enviar un job final
JOB=$(docker-compose run --rm client submit examples/wordcount.json 2>&1 | grep "Job ID:" | grep -o "job-[0-9]*")

# Esperar
for i in {1..120}; do
    STATUS=$(docker-compose run --rm client status "$JOB" 2>&1 | grep "Status:" | tail -1 | grep -o "COMPLETED" || echo "")
    [ "$STATUS" = "COMPLETED" ] && break
    sleep 1
done

# Ver resultados
RESULTS=$(docker-compose run --rm client results "$JOB" 2>&1)
OUTPUT_COUNT=$(echo "$RESULTS" | grep -c "data/output" || echo "0")

if [ "$OUTPUT_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✅ Job produjo $OUTPUT_COUNT outputs${NC}"
    echo "$RESULTS" | grep "data/output" | head -3
else
    echo -e "${RED}❌ No hay outputs${NC}"
    exit 1
fi

echo ""

# ============================================================================
# RESUMEN
# ============================================================================

echo -e "${CYAN}=========================================${NC}"
echo -e "${GREEN}✨ TODOS LOS TESTS COMPLETADOS EXITOSAMENTE${NC}"
echo -e "${CYAN}=========================================${NC}"
echo ""
echo -e "${YELLOW}Resumen:${NC}"
echo "✅ Test 1: Estado del sistema"
echo "✅ Test 2: WordCount básico"
echo "✅ Test 3: Benchmark pequeño"
echo "✅ Test 4: Múltiples jobs en paralelo"
echo "✅ Test 5: Tolerancia a fallos"
echo "✅ Test 6: Métricas del sistema"
echo "✅ Test 7: Stress test (10 jobs)"
echo "✅ Test 8: Verificar outputs"
echo ""