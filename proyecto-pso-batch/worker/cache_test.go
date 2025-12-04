// worker/cache_test.go
package main

import (
	"os"
	"strings"
	"testing"

	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/types"
)

// ============================================================================
// TEST: Basic Cache Operations
// ============================================================================

func TestRecordCache_Add(t *testing.T) {
	cache := NewRecordCache(100, "test-task")
	defer cache.Cleanup()

	record := types.Record{
		Data: map[string]interface{}{
			"id":   "1",
			"text": "hello world",
		},
	}

	err := cache.Add(record)
	if err != nil {
		t.Fatalf("Error agregando record: %v", err)
	}

	records, err := cache.GetAll()
	if err != nil {
		t.Fatalf("Error obteniendo records: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("Esperaba 1 record, obtuvo %d", len(records))
	}

	if records[0].Data["id"] != "1" {
		t.Error("Record no fue almacenado correctamente")
	}
}

func TestRecordCache_AddBatch(t *testing.T) {
	cache := NewRecordCache(100, "test-batch")
	defer cache.Cleanup()

	records := []types.Record{
		{Data: map[string]interface{}{"id": "1"}},
		{Data: map[string]interface{}{"id": "2"}},
		{Data: map[string]interface{}{"id": "3"}},
	}

	err := cache.AddBatch(records)
	if err != nil {
		t.Fatalf("Error agregando batch: %v", err)
	}

	retrieved, err := cache.GetAll()
	if err != nil {
		t.Fatalf("Error obteniendo records: %v", err)
	}

	if len(retrieved) != 3 {
		t.Fatalf("Esperaba 3 records, obtuvo %d", len(retrieved))
	}
}

// ============================================================================
// TEST: Spill to Disk
// ============================================================================

func TestRecordCache_SpillToDisk(t *testing.T) {
	// Crear cache con límite muy pequeño (1MB) para forzar spill
	cache := NewRecordCache(1, "test-spill")
	defer cache.Cleanup()

	// Crear muchos records para exceder el límite
	largeText := make([]byte, 50000) // 50KB por record
	for i := range largeText {
		largeText[i] = 'a'
	}

	// Agregar records hasta que haga spill
	for i := 0; i < 30; i++ { // 30 * 50KB = 1.5MB > 1MB
		record := types.Record{
			Data: map[string]interface{}{
				"id":   i,
				"text": string(largeText),
			},
		}
		err := cache.Add(record)
		if err != nil {
			t.Fatalf("Error agregando record %d: %v", i, err)
		}
	}

	// Verificar que se hizo spill
	stats := cache.GetStats()
	spillFiles := stats["spill_files"].(int)

	if spillFiles == 0 {
		t.Error("Debería haber creado archivos de spill")
	}

	t.Logf("Spill files creados: %d", spillFiles)

	// Verificar que GetAll() retorna todos los records
	allRecords, err := cache.GetAll()
	if err != nil {
		t.Fatalf("Error obteniendo todos los records: %v", err)
	}

	if len(allRecords) != 30 {
		t.Fatalf("Esperaba 30 records, obtuvo %d", len(allRecords))
	}
}

func TestRecordCache_Flush(t *testing.T) {
	cache := NewRecordCache(100, "test-flush")
	defer cache.Cleanup()

	// Agregar algunos records
	for i := 0; i < 5; i++ {
		record := types.Record{
			Data: map[string]interface{}{"id": i},
		}
		cache.Add(record)
	}

	// Flush debería escribir records restantes a disco
	err := cache.Flush()
	if err != nil {
		t.Fatalf("Error en flush: %v", err)
	}

	// Después del flush, memoria debería estar vacía
	stats := cache.GetStats()
	recordsInMemory := stats["records_in_memory"].(int)

	if recordsInMemory != 0 {
		t.Errorf("Después de flush, memoria debería estar vacía. Tiene %d records", recordsInMemory)
	}

	// Pero GetAll() debería retornar todos los records
	allRecords, err := cache.GetAll()
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(allRecords) != 5 {
		t.Fatalf("Esperaba 5 records después de flush, obtuvo %d", len(allRecords))
	}
}

func TestRecordCache_Cleanup(t *testing.T) {
	cache := NewRecordCache(1, "test-cleanup")

	// Crear algunos spill files
	largeText := make([]byte, 50000)
	for i := 0; i < 30; i++ {
		record := types.Record{
			Data: map[string]interface{}{
				"id":   i,
				"text": string(largeText),
			},
		}
		cache.Add(record)
	}

	stats := cache.GetStats()
	spillPath := stats["spill_path"].(string)

	// Verificar que el directorio existe
	if _, err := os.Stat(spillPath); os.IsNotExist(err) {
		t.Fatal("Directorio de spill no existe")
	}

	// Cleanup debería eliminar todo
	err := cache.Cleanup()
	if err != nil {
		t.Fatalf("Error en cleanup: %v", err)
	}

	// Verificar que el directorio fue eliminado
	if _, err := os.Stat(spillPath); !os.IsNotExist(err) {
		t.Error("Directorio de spill no fue eliminado")
	}

	// Verificar que memoria está limpia
	stats = cache.GetStats()
	if stats["records_in_memory"].(int) != 0 {
		t.Error("Memoria no fue limpiada")
	}
	if stats["spill_files"].(int) != 0 {
		t.Error("Spill files no fueron limpiados")
	}
}

// ============================================================================
// TEST: Memory Management
// ============================================================================

func TestRecordCache_MemoryTracking(t *testing.T) {
	cache := NewRecordCache(100, "test-memory")
	defer cache.Cleanup()

	initialStats := cache.GetStats()
	initialMemory := initialStats["memory_used_mb"].(float64)

	if initialMemory != 0 {
		t.Error("Memoria inicial debería ser 0")
	}

	// Agregar un record
	record := types.Record{
		Data: map[string]interface{}{
			"text": "hello world",
		},
	}
	cache.Add(record)

	stats := cache.GetStats()
	memoryUsed := stats["memory_used_mb"].(float64)

	if memoryUsed == 0 {
		t.Error("Memoria debería incrementarse después de agregar record")
	}

	t.Logf("Memoria usada: %.4f MB", memoryUsed)
}

func TestRecordCache_MemoryLimit(t *testing.T) {
	// Límite muy bajo para test rápido
	limitMB := int64(1)
	cache := NewRecordCache(limitMB, "test-limit")
	defer cache.Cleanup()

	stats := cache.GetStats()
	limit := stats["memory_limit_mb"].(float64)

	if limit != float64(limitMB) {
		t.Errorf("Límite incorrecto: esperaba %d MB, obtuvo %.1f MB", limitMB, limit)
	}
}

func TestRecordCache_EstimateRecordSize(t *testing.T) {
	// Test de función auxiliar
	record := types.Record{
		Data: map[string]interface{}{
			"id":   "123",
			"text": "hello",
		},
	}

	size := estimateRecordSize(record)

	if size <= 0 {
		t.Error("Tamaño estimado debería ser positivo")
	}

	t.Logf("Tamaño estimado: %d bytes", size)

	// Record más grande debería tener mayor tamaño estimado
	largeRecord := types.Record{
		Data: map[string]interface{}{
			"id":   "123",
			"text": string(make([]byte, 1000)),
		},
	}

	largeSize := estimateRecordSize(largeRecord)

	if largeSize <= size {
		t.Error("Record más grande debería tener mayor tamaño estimado")
	}
}

// ============================================================================
// TEST: Concurrent Access
// ============================================================================

func TestRecordCache_ConcurrentAdd(t *testing.T) {
	cache := NewRecordCache(100, "test-concurrent")
	defer cache.Cleanup()

	// Agregar records concurrentemente
	done := make(chan bool)
	numGoroutines := 10
	recordsPerGoroutine := 100

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < recordsPerGoroutine; i++ {
				record := types.Record{
					Data: map[string]interface{}{
						"goroutine": goroutineID,
						"id":        i,
					},
				}
				err := cache.Add(record)
				if err != nil {
					t.Errorf("Error agregando record: %v", err)
				}
			}
			done <- true
		}(g)
	}

	// Esperar a que todas las goroutines terminen
	for g := 0; g < numGoroutines; g++ {
		<-done
	}

	// Verificar que todos los records fueron agregados
	allRecords, err := cache.GetAll()
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	expectedTotal := numGoroutines * recordsPerGoroutine
	if len(allRecords) != expectedTotal {
		t.Errorf("Esperaba %d records, obtuvo %d", expectedTotal, len(allRecords))
	}
}

// ============================================================================
// TEST: Edge Cases
// ============================================================================

func TestRecordCache_EmptyCache(t *testing.T) {
	cache := NewRecordCache(100, "test-empty")
	defer cache.Cleanup()

	records, err := cache.GetAll()
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("Cache vacío debería retornar 0 records, obtuvo %d", len(records))
	}

	stats := cache.GetStats()
	if stats["total_records"].(int) != 0 {
		t.Error("total_records debería ser 0")
	}
}

func TestRecordCache_SingleLargeRecord(t *testing.T) {
	// Límite de 1MB
	cache := NewRecordCache(1, "test-large-single")
	defer cache.Cleanup()

	// Record de 2MB (mayor que el límite)
	largeData := make([]byte, 2*1024*1024)
	for i := range largeData {
		largeData[i] = 'x'
	}

	record := types.Record{
		Data: map[string]interface{}{
			"data": string(largeData),
		},
	}

	// Debería hacer spill inmediatamente
	err := cache.Add(record)
	if err != nil {
		t.Fatalf("Error agregando record grande: %v", err)
	}

	cache.Flush()

	stats := cache.GetStats()
	if stats["spill_files"].(int) == 0 {
		t.Error("Debería haber creado spill file")
	}

	allRecords, err := cache.GetAll()
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(allRecords) != 1 {
		t.Fatalf("Esperaba 1 record, obtuvo %d", len(allRecords))
	}
}

func TestRecordCache_RepeatedFlush(t *testing.T) {
	cache := NewRecordCache(100, "test-repeated-flush")
	defer cache.Cleanup()

	// Agregar y flush varias veces
	for round := 0; round < 3; round++ {
		for i := 0; i < 10; i++ {
			record := types.Record{
				Data: map[string]interface{}{
					"round": round,
					"id":    i,
				},
			}
			cache.Add(record)
		}

		err := cache.Flush()
		if err != nil {
			t.Fatalf("Error en flush %d: %v", round, err)
		}
	}

	// Debería tener 3 spill files
	stats := cache.GetStats()
	spillFiles := stats["spill_files"].(int)

	if spillFiles != 3 {
		t.Errorf("Esperaba 3 spill files, obtuvo %d", spillFiles)
	}

	// Total de records: 3 rounds * 10 records = 30
	allRecords, err := cache.GetAll()
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(allRecords) != 30 {
		t.Fatalf("Esperaba 30 records, obtuvo %d", len(allRecords))
	}
}

func TestRecordCache_SpillPathCreation(t *testing.T) {
	taskID := "test-path-creation"
	cache := NewRecordCache(1, taskID)
	defer cache.Cleanup()

	// Forzar spill
	largeData := make([]byte, 50000)
	for i := 0; i < 30; i++ {
		record := types.Record{
			Data: map[string]interface{}{"data": string(largeData)},
		}
		cache.Add(record)
	}

	stats := cache.GetStats()
	spillPath := stats["spill_path"].(string)

	// Verificar que el path contiene el taskID
	if !strings.Contains(spillPath, taskID) {
		t.Errorf("spillPath debería contener taskID '%s', got '%s'", taskID, spillPath)
	}

	// Verificar que el directorio existe (puede ser relativo o absoluto)
	if _, err := os.Stat(spillPath); os.IsNotExist(err) {
		t.Error("Directorio de spill no fue creado")
	}

	t.Logf("Spill path created: %s", spillPath)
}

// ============================================================================
// TEST: Stats
// ============================================================================

func TestRecordCache_GetStats(t *testing.T) {
	cache := NewRecordCache(50, "test-stats")
	defer cache.Cleanup()

	// Estado inicial
	stats := cache.GetStats()

	requiredKeys := []string{
		"memory_used_mb",
		"memory_limit_mb",
		"records_in_memory",
		"spill_files",
		"total_records",
		"spill_path",
	}

	for _, key := range requiredKeys {
		if _, exists := stats[key]; !exists {
			t.Errorf("Stats debería contener key '%s'", key)
		}
	}

	// Agregar algunos records
	for i := 0; i < 5; i++ {
		cache.Add(types.Record{Data: map[string]interface{}{"id": i}})
	}

	stats = cache.GetStats()

	if stats["records_in_memory"].(int) != 5 {
		t.Errorf("records_in_memory incorrecto: %v", stats["records_in_memory"])
	}

	if stats["total_records"].(int) != 5 {
		t.Errorf("total_records incorrecto: %v", stats["total_records"])
	}
}

// ============================================================================
// BENCHMARK TESTS
// ============================================================================

func BenchmarkRecordCache_Add(b *testing.B) {
	cache := NewRecordCache(1000, "bench-add")
	defer cache.Cleanup()

	record := types.Record{
		Data: map[string]interface{}{
			"id":   "123",
			"text": "hello world",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Add(record)
	}
}

func BenchmarkRecordCache_AddWithSpill(b *testing.B) {
	// Límite bajo para forzar spill
	cache := NewRecordCache(1, "bench-spill")
	defer cache.Cleanup()

	largeData := make([]byte, 10000)
	record := types.Record{
		Data: map[string]interface{}{"data": string(largeData)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Add(record)
	}
}

func BenchmarkRecordCache_GetAll(b *testing.B) {
	cache := NewRecordCache(100, "bench-getall")
	defer cache.Cleanup()

	// Agregar 1000 records
	for i := 0; i < 1000; i++ {
		cache.Add(types.Record{
			Data: map[string]interface{}{"id": i},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.GetAll()
	}
}

func BenchmarkEstimateRecordSize(b *testing.B) {
	record := types.Record{
		Data: map[string]interface{}{
			"id":    "12345",
			"text":  "hello world this is a test",
			"count": 42,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		estimateRecordSize(record)
	}
}
