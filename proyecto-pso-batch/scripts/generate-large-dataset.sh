#!/bin/bash
# scripts/generate-large-dataset.sh - Generar dataset de 1M registros

set -e

echo "Generando dataset de 1M registros..."
echo "Esto puede tomar 30-60 segundos..."
echo ""

OUTPUT_FILE="data/input/large-dataset.csv"

# Crear directorio si no existe
mkdir -p data/input

# Generar CSV con 1M registros
{
    echo "id,text,category,timestamp"
    
    for i in {1..1000000}; do
        # Generar texto aleatorio
        text="Record $i Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua"
        category=$((RANDOM % 10))
        timestamp="2025-01-01T$(printf '%02d' $((RANDOM % 24))):$(printf '%02d' $((RANDOM % 60))):$(printf '%02d' $((RANDOM % 60)))Z"
        
        echo "$i,\"$text\",$category,$timestamp"
        
        # Mostrar progreso cada 100k
        if [ $((i % 100000)) -eq 0 ]; then
            echo "  $i registros generados..." >&2
        fi
    done
} > "$OUTPUT_FILE"

FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
LINE_COUNT=$(wc -l < "$OUTPUT_FILE")

echo ""
echo "✅ Dataset generado exitosamente"
echo "   Archivo: $OUTPUT_FILE"
echo "   Tamaño: $FILE_SIZE"
echo "   Registros: $((LINE_COUNT - 1))" # -1 para el header
echo ""
echo "Para usar en un job, copia esta ruta en el JSON:"
echo "   \"path\": \"data/input/large-dataset.csv\""