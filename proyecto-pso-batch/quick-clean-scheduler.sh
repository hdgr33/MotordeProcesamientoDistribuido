#!/bin/bash
# quick-clean-scheduler.sh
# Limpia emojis solo del scheduler.go

set -e

echo "Limpiando emojis de scheduler.go..."

if [ ! -f "master/scheduler.go" ]; then
    echo "ERROR: No se encuentra master/scheduler.go"
    exit 1
fi

# Backup
cp master/scheduler.go master/scheduler.go.bak

# Limpiar emojis
cat master/scheduler.go | \
    sed 's/📋/INFO:/g' | \
    sed 's/🚀/INFO:/g' | \
    sed 's/✅/SUCCESS:/g' | \
    sed 's/❌/ERROR:/g' | \
    sed 's/⚠️/WARN:/g' | \
    sed 's/🔄/RETRY:/g' | \
    sed 's/▶️/EXEC:/g' | \
    sed 's/✨/COMPLETE:/g' | \
    sed 's/⏱️/TIMEOUT:/g' | \
    sed 's/💷/WORKER:/g' | \
    sed 's/💤/IDLE:/g' | \
    sed 's/⚙️/BUSY:/g' | \
    sed 's/💀/DOWN:/g' | \
    sed 's/📥/RECV:/g' | \
    sed 's/📤/SEND:/g' | \
    sed 's/⏳/WAIT:/g' > master/scheduler.go.tmp

mv master/scheduler.go.tmp master/scheduler.go

echo "✓ Limpieza completada"
echo "Backup guardado en: master/scheduler.go.bak"
