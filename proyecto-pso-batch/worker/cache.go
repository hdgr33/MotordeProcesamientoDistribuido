// worker/cache.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/types"
)

const (
	// Límite de memoria en bytes (100 MB por defecto)
	DefaultMemoryLimit = 100 * 1024 * 1024 // 100MB

	// Directorio de spill
	SpillDir = "data/spill"
)

type RecordCache struct {
	records      []types.Record
	memoryUsed   int64
	memoryLimit  int64
	spilledFiles []string
	mutex        sync.RWMutex
	spillPath    string
}

func NewRecordCache(memoryLimitMB int64, taskID string) *RecordCache {
	if memoryLimitMB <= 0 {
		memoryLimitMB = 100 // Default 100MB
	}

	spillPath := filepath.Join(SpillDir, taskID)

	return &RecordCache{
		records:     make([]types.Record, 0),
		memoryUsed:  0,
		memoryLimit: memoryLimitMB * 1024 * 1024,
		spillPath:   spillPath,
	}
}

// Add agrega un record al caché, spilleando si es necesario
func (rc *RecordCache) Add(record types.Record) error {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	// Estimar tamaño del record
	recordSize := estimateRecordSize(record)

	// Verificar si necesita spill
	if rc.memoryUsed+recordSize > rc.memoryLimit {
		log.Printf("INFO: Spilling to disk (memory: %d/%d bytes)", rc.memoryUsed, rc.memoryLimit)
		if err := rc.spillToDisk(); err != nil {
			return fmt.Errorf("spill failed: %w", err)
		}
	}

	rc.records = append(rc.records, record)
	rc.memoryUsed += recordSize

	return nil
}

// AddBatch agrega múltiples records
func (rc *RecordCache) AddBatch(records []types.Record) error {
	for _, record := range records {
		if err := rc.Add(record); err != nil {
			return err
		}
	}
	return nil
}

// GetAll retorna todos los records (en memoria + spill)
func (rc *RecordCache) GetAll() ([]types.Record, error) {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()

	result := make([]types.Record, 0, len(rc.records))

	// Agregar records en memoria
	result = append(result, rc.records...)

	// Leer records del spill
	for _, spillFile := range rc.spilledFiles {
		records, err := readRecordsFromFile(spillFile)
		if err != nil {
			log.Printf("WARN: Failed to read spill file %s: %v", spillFile, err)
			continue
		}
		result = append(result, records...)
	}

	return result, nil
}

// spillToDisk escribe records actuales a disco y limpia la memoria
func (rc *RecordCache) spillToDisk() error {
	if len(rc.records) == 0 {
		return nil
	}

	// Crear directorio si no existe
	if err := os.MkdirAll(rc.spillPath, 0755); err != nil {
		return err
	}

	// Generar nombre de archivo
	spillFile := filepath.Join(rc.spillPath, fmt.Sprintf("spill-%d.json", len(rc.spilledFiles)))

	// Escribir a disco
	if err := writeRecordsToFile(spillFile, rc.records); err != nil {
		return err
	}

	log.Printf("INFO: Spilled %d records to %s", len(rc.records), spillFile)

	// Registrar archivo y limpiar memoria
	rc.spilledFiles = append(rc.spilledFiles, spillFile)
	rc.records = make([]types.Record, 0)
	rc.memoryUsed = 0

	return nil
}

// Flush escribe todos los records restantes a disco
func (rc *RecordCache) Flush() error {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	return rc.spillToDisk()
}

// GetStats retorna estadísticas del caché
func (rc *RecordCache) GetStats() map[string]interface{} {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()

	totalRecords := len(rc.records)
	for _, spillFile := range rc.spilledFiles {
		records, _ := readRecordsFromFile(spillFile)
		totalRecords += len(records)
	}

	return map[string]interface{}{
		"memory_used_mb":    float64(rc.memoryUsed) / (1024 * 1024),
		"memory_limit_mb":   float64(rc.memoryLimit) / (1024 * 1024),
		"records_in_memory": len(rc.records),
		"spill_files":       len(rc.spilledFiles),
		"total_records":     totalRecords,
		"spill_path":        rc.spillPath,
	}
}

// Cleanup limpia archivos spilleados
func (rc *RecordCache) Cleanup() error {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	if err := os.RemoveAll(rc.spillPath); err != nil {
		log.Printf("WARN: Failed to cleanup spill directory: %v", err)
		return err
	}

	rc.spilledFiles = make([]string, 0)
	rc.records = make([]types.Record, 0)
	rc.memoryUsed = 0

	return nil
}

// ============================================================================
// HELPERS
// ============================================================================

func estimateRecordSize(record types.Record) int64 {
	// Estimación aproximada: convertir a JSON y medir
	b, _ := json.Marshal(record)
	return int64(len(b))
}

func getMemoryStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func getCurrentMemoryMB() float64 {
	m := getMemoryStats()
	return float64(m.Alloc) / (1024 * 1024)
}

func getSystemMemoryMB() float64 {
	m := getMemoryStats()
	return float64(m.TotalAlloc) / (1024 * 1024)
}
