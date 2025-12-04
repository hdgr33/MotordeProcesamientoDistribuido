#!/bin/bash
# scripts/docker-setup.sh - Configurar y levantar PSO Batch con Docker

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo ""
echo -e "${CYAN}=========================================${NC}"
echo -e "${GREEN}PSO Batch - Docker Setup${NC}"
echo -e "${CYAN}=========================================${NC}"
echo ""

# Verificar Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker no está instalado${NC}"
    echo "Instala Docker desde: https://www.docker.com/products/docker-desktop"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose no está instalado${NC}"
    echo "Instala Docker Compose desde: https://docs.docker.com/compose/install/"
    exit 1
fi

echo -e "${GREEN}✅ Docker y Docker Compose encontrados${NC}"
echo ""

# Verificar Docker daemon
if ! docker ps &> /dev/null; then
    echo -e "${RED}❌ Docker daemon no está corriendo${NC}"
    echo "Inicia Docker Desktop o el demonio de Docker"
    exit 1
fi

echo -e "${GREEN}✅ Docker daemon activo${NC}"
echo ""

# Crear directorios necesarios
echo -e "${YELLOW}📁 Creando directorios...${NC}"
mkdir -p data/input data/output data/spill logs examples
echo -e "${GREEN}✅ Directorios creados${NC}"
echo ""

# Generar dataset de prueba si no existe
if [ ! -f "data/input/text.csv" ]; then
    echo -e "${YELLOW}📄 Generando dataset de prueba...${NC}"
    cat > data/input/text.csv << 'EOF'
id,text,timestamp
1,"hello world from the distributed system",2024-01-01T10:00:00Z
2,"hello master and worker nodes",2024-01-01T10:01:00Z
3,"processing data in the cluster",2024-01-01T10:02:00Z
4,"hello world again from worker",2024-01-01T10:03:00Z
5,"distributed computing with go language",2024-01-01T10:04:00Z
6,"the system processes data efficiently",2024-01-01T10:05:00Z
7,"hello from the batch processing engine",2024-01-01T10:06:00Z
8,"world class distributed system design",2024-01-01T10:07:00Z
EOF
    echo -e "${GREEN}✅ Dataset generado${NC}"
fi
echo ""

# Build images
echo -e "${YELLOW}🐳 Construyendo imágenes Docker...${NC}"
echo "(Esto puede tomar 2-3 minutos la primera vez)"
docker-compose build

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Imágenes construidas exitosamente${NC}"
else
    echo -e "${RED}❌ Error construyendo imágenes${NC}"
    exit 1
fi
echo ""

# Levantar cluster
echo -e "${YELLOW}🚀 Levantando cluster...${NC}"
docker-compose up -d

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Cluster levantado${NC}"
else
    echo -e "${RED}❌ Error levantando cluster${NC}"
    docker-compose logs
    exit 1
fi
echo ""

# Esperar a que master esté listo
echo -e "${YELLOW}⏳ Esperando a que Master esté listo...${NC}"
for i in {1..30}; do
    if curl -s http://localhost:8080/api/v1/metrics > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Master está listo${NC}"
        break
    fi
    echo -n "."
    sleep 1
done
echo ""

# Esperar a que workers se registren
echo -e "${YELLOW}⏳ Esperando a que Workers se registren...${NC}"
sleep 5

# Verificar workers
echo -e "${YELLOW}📋 Verificando workers...${NC}"
WORKERS=$(docker-compose exec -T client workers 2>/dev/null | grep -c "worker-" || echo "0")

if [ "$WORKERS" -gt 0 ]; then
    echo -e "${GREEN}✅ $WORKERS workers registrados${NC}"
else
    echo -e "${YELLOW}⚠️  No se detectaron workers aún, esperando...${NC}"
    sleep 5
fi
echo ""

# Mostrar información
echo -e "${CYAN}=========================================${NC}"
echo -e "${GREEN}✨ Setup completado exitosamente!${NC}"
echo -e "${CYAN}=========================================${NC}"
echo ""
echo -e "${CYAN}📊 Acceso al Sistema:${NC}"
echo "  Dashboard: ${YELLOW}http://localhost:8080${NC}"
echo "  Master API: ${YELLOW}http://localhost:8080/api/v1${NC}"
echo "  Logs: ${YELLOW}docker-compose logs -f${NC}"
echo ""
echo -e "${CYAN}🎮 Próximos Pasos:${NC}"
echo "  1. Ver workers: ${YELLOW}docker-compose exec client workers${NC}"
echo "  2. Ver métricas: ${YELLOW}docker-compose exec client metrics${NC}"
echo "  3. Enviar job: ${YELLOW}docker-compose exec client submit examples/wordcount.json${NC}"
echo "  4. Ver progreso: ${YELLOW}docker-compose exec client watch <job-id>${NC}"
echo ""
echo -e "${CYAN}🧪 Tests:${NC}"
echo "  Test fallo: ${YELLOW}docker-compose stop worker-2${NC}"
echo "  Ver recuperación: ${YELLOW}docker-compose exec client workers${NC}"
echo ""
echo -e "${CYAN}📖 Documentación:${NC}"
echo "  Ver logs: ${YELLOW}docker-compose logs -f master${NC}"
echo "  Parar cluster: ${YELLOW}docker-compose down${NC}"
echo "  Reset data: ${YELLOW}docker-compose down -v${NC}"
echo ""
echo -e "${CYAN}=========================================${NC}"
echo ""