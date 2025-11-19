#!/bin/bash
# scripts/setup.sh - Setup inicial del proyecto

set -e

echo "🔧 Configurando proyecto PSO Batch..."

# Crear directorios necesarios
mkdir -p bin
mkdir -p data/input
mkdir -p data/output
mkdir -p examples
mkdir -p logs

# Crear archivo de texto de prueba si no existe
if [ ! -f "data/input/text.csv" ]; then
    echo "📝 Creando dataset de prueba..."
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
fi

# Crear ejemplo de job si no existe
if [ ! -f "examples/wordcount.json" ]; then
    echo "📄 Creando job de ejemplo..."
    cat > examples/wordcount.json << 'EOF'
{
  "name": "wordcount-basic",
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
        "fn": "sum"
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
EOF
fi

# Descargar dependencias de Go
echo "📦 Descargando dependencias..."
go mod download
go mod tidy

# Compilar
echo "🔨 Compilando componentes..."
make build

echo ""
echo "✅ Setup completado!"
echo ""
echo "🚀 Para iniciar el sistema:"
echo "   Terminal 1: make run-master"
echo "   Terminal 2: make run-worker1"
echo "   Terminal 3: make run-worker2"
echo ""
echo "📋 Para enviar un job:"
echo "   ./bin/client submit examples/wordcount.json"
echo ""