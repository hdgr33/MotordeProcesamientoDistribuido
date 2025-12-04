// worker/operators_test.go
package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/types"
)

// ============================================================================
// TEST SETUP
// ============================================================================

func setupTestDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "worker-test-*")
	if err != nil {
		t.Fatalf("Error creando directorio temporal: %v", err)
	}
	return dir
}

func cleanupTestDir(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Errorf("Error limpiando directorio: %v", err)
	}
}

func createTestCSV(t *testing.T, path string, rows [][]string) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("Error creando directorio: %v", err)
	}

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Error creando archivo CSV: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			t.Fatalf("Error escribiendo CSV: %v", err)
		}
	}
}

// ============================================================================
// TEST: READ_CSV
// ============================================================================

func TestOperatorReadCSV_Basic(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(t, testDir)

	csvPath := filepath.Join(testDir, "test.csv")
	rows := [][]string{
		{"id", "name", "age"},
		{"1", "Alice", "30"},
		{"2", "Bob", "25"},
		{"3", "Charlie", "35"},
	}
	createTestCSV(t, csvPath, rows)

	task := &types.Task{
		ID:         "test-read",
		InputPaths: []string{csvPath},
	}

	records, err := operatorReadCSV(task)

	if err != nil {
		t.Fatalf("Error ejecutando read_csv: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("Esperaba 3 records, obtuvo %d", len(records))
	}

	// Verificar primer record
	if records[0].Data["id"] != "1" {
		t.Errorf("ID incorrecto: %v", records[0].Data["id"])
	}
	if records[0].Data["name"] != "Alice" {
		t.Errorf("Nombre incorrecto: %v", records[0].Data["name"])
	}
	if records[0].Data["age"] != "30" {
		t.Errorf("Edad incorrecta: %v", records[0].Data["age"])
	}
}

func TestOperatorReadCSV_Empty(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(t, testDir)

	csvPath := filepath.Join(testDir, "empty.csv")
	rows := [][]string{
		{"id", "name"},
	}
	createTestCSV(t, csvPath, rows)

	task := &types.Task{
		ID:         "test-empty",
		InputPaths: []string{csvPath},
	}

	records, err := operatorReadCSV(task)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("Esperaba 0 records, obtuvo %d", len(records))
	}
}

func TestOperatorReadCSV_FileNotFound(t *testing.T) {
	task := &types.Task{
		ID:         "test-notfound",
		InputPaths: []string{"/nonexistent/file.csv"},
	}

	_, err := operatorReadCSV(task)

	if err == nil {
		t.Fatal("Debería retornar error para archivo inexistente")
	}
}

// ============================================================================
// TEST: MAP
// ============================================================================

func TestOperatorMap_ToLower(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"text": "HELLO WORLD"}},
		{Data: map[string]interface{}{"text": "TEST"}},
	}

	task := &types.Task{
		Function: "to_lower",
	}

	output, err := operatorMap(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(output) != 2 {
		t.Fatalf("Esperaba 2 records, obtuvo %d", len(output))
	}

	if output[0].Data["text"] != "hello world" {
		t.Errorf("Transformación incorrecta: %v", output[0].Data["text"])
	}

	if output[1].Data["text"] != "test" {
		t.Errorf("Transformación incorrecta: %v", output[1].Data["text"])
	}
}

func TestOperatorMap_ToUpper(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"text": "hello"}},
	}

	task := &types.Task{
		Function: "to_upper",
	}

	output, err := operatorMap(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if output[0].Data["text"] != "HELLO" {
		t.Errorf("Esperaba HELLO, obtuvo %v", output[0].Data["text"])
	}
}

func TestOperatorMap_Trim(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"text": "  hello  "}},
		{Data: map[string]interface{}{"text": "\tworld\n"}},
	}

	task := &types.Task{
		Function: "trim",
	}

	output, err := operatorMap(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if output[0].Data["text"] != "hello" {
		t.Errorf("Trim falló: '%v'", output[0].Data["text"])
	}

	if output[1].Data["text"] != "world" {
		t.Errorf("Trim falló: '%v'", output[1].Data["text"])
	}
}

func TestOperatorMap_PreservesNonStringFields(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{
			"text":   "HELLO",
			"number": 42,
			"flag":   true,
		}},
	}

	task := &types.Task{
		Function: "to_lower",
	}

	output, err := operatorMap(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	// Verificar que text fue transformado
	if output[0].Data["text"] != "hello" {
		t.Error("Campo text no fue transformado")
	}

	// Verificar que otros campos se preservaron
	if output[0].Data["number"] != 42 {
		t.Error("Campo number no se preservó")
	}

	if output[0].Data["flag"] != true {
		t.Error("Campo flag no se preservó")
	}
}

// ============================================================================
// TEST: FILTER
// ============================================================================

func TestOperatorFilter_NonEmpty(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"text": "hello"}},
		{Data: map[string]interface{}{"text": ""}},
		{Data: map[string]interface{}{"text": "world"}},
		{Data: map[string]interface{}{"text": ""}},
	}

	task := &types.Task{
		Function: "non_empty",
	}

	output, err := operatorFilter(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(output) != 2 {
		t.Fatalf("Esperaba 2 records, obtuvo %d", len(output))
	}

	if output[0].Data["text"] != "hello" {
		t.Error("Primer record incorrecto")
	}

	if output[1].Data["text"] != "world" {
		t.Error("Segundo record incorrecto")
	}
}

func TestOperatorFilter_HasText(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"text": "hello", "id": "1"}},
		{Data: map[string]interface{}{"id": "2"}},
		{Data: map[string]interface{}{"text": "world", "id": "3"}},
	}

	task := &types.Task{
		Function: "has_text",
	}

	output, err := operatorFilter(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(output) != 2 {
		t.Fatalf("Esperaba 2 records con campo 'text', obtuvo %d", len(output))
	}
}

// ============================================================================
// TEST: FLAT_MAP
// ============================================================================

func TestOperatorFlatMap_SplitWords(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"text": "hello world"}},
		{Data: map[string]interface{}{"text": "foo bar baz"}},
	}

	task := &types.Task{
		Function: "split_words",
	}

	output, err := operatorFlatMap(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(output) != 5 {
		t.Fatalf("Esperaba 5 palabras, obtuvo %d", len(output))
	}

	expectedWords := []string{"hello", "world", "foo", "bar", "baz"}
	for i, expected := range expectedWords {
		if output[i].Data["word"] != expected {
			t.Errorf("Palabra %d incorrecta: esperaba %s, obtuvo %v",
				i, expected, output[i].Data["word"])
		}
	}
}

func TestOperatorFlatMap_Tokenize(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"text": "Hello, World!"}},
	}

	task := &types.Task{
		Function: "tokenize",
	}

	output, err := operatorFlatMap(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(output) != 2 {
		t.Fatalf("Esperaba 2 tokens, obtuvo %d", len(output))
	}

	// Debe remover puntuación
	if output[0].Data["word"] != "Hello" {
		t.Errorf("Token incorrecto: %v", output[0].Data["word"])
	}

	if output[1].Data["word"] != "World" {
		t.Errorf("Token incorrecto: %v", output[1].Data["word"])
	}
}

func TestOperatorFlatMap_SplitLines(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{
			"text": "Line 1\nLine 2\nLine 3",
			"id":   "doc1",
		}},
	}

	task := &types.Task{
		Function: "split_lines",
	}

	output, err := operatorFlatMap(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(output) != 3 {
		t.Fatalf("Esperaba 3 líneas, obtuvo %d", len(output))
	}

	// Verificar que cada línea tiene line_num
	for i, record := range output {
		if record.Data["line_num"] != i {
			t.Errorf("line_num incorrecto en línea %d", i)
		}

		if record.Data["id"] != "doc1" {
			t.Error("Campo original 'id' no se preservó")
		}
	}
}

func TestOperatorFlatMap_EmptyLines(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"text": "Line 1\n\nLine 3"}},
	}

	task := &types.Task{
		Function: "split_lines",
	}

	output, err := operatorFlatMap(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	// Debe saltar líneas vacías
	if len(output) != 2 {
		t.Fatalf("Esperaba 2 líneas (saltando vacías), obtuvo %d", len(output))
	}
}

// ============================================================================
// TEST: REDUCE_BY_KEY
// ============================================================================

func TestOperatorReduceByKey_Count(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"word": "hello"}},
		{Data: map[string]interface{}{"word": "world"}},
		{Data: map[string]interface{}{"word": "hello"}},
		{Data: map[string]interface{}{"word": "hello"}},
		{Data: map[string]interface{}{"word": "world"}},
	}

	task := &types.Task{
		Key:      "word",
		Function: "count",
	}

	output, err := operatorReduceByKey(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(output) != 2 {
		t.Fatalf("Esperaba 2 palabras únicas, obtuvo %d", len(output))
	}

	// Construir map para fácil verificación
	counts := make(map[string]int)
	for _, record := range output {
		word := record.Data["word"].(string)
		count := record.Data["count"].(int)
		counts[word] = count
	}

	if counts["hello"] != 3 {
		t.Errorf("Conteo incorrecto para 'hello': esperaba 3, obtuvo %d", counts["hello"])
	}

	if counts["world"] != 2 {
		t.Errorf("Conteo incorrecto para 'world': esperaba 2, obtuvo %d", counts["world"])
	}
}

func TestOperatorReduceByKey_Sum(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"key": "A"}},
		{Data: map[string]interface{}{"key": "B"}},
		{Data: map[string]interface{}{"key": "A"}},
	}

	task := &types.Task{
		Key:      "key",
		Function: "sum",
	}

	output, err := operatorReduceByKey(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(output) != 2 {
		t.Fatalf("Esperaba 2 claves únicas, obtuvo %d", len(output))
	}
}

func TestOperatorReduceByKey_DefaultKey(t *testing.T) {
	// Si no se especifica Key, debe usar "word" por defecto
	input := []types.Record{
		{Data: map[string]interface{}{"word": "test"}},
		{Data: map[string]interface{}{"word": "test"}},
	}

	task := &types.Task{
		Key:      "", // No especificado
		Function: "count",
	}

	output, err := operatorReduceByKey(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(output) != 1 {
		t.Fatal("Debería agrupar por 'word' por defecto")
	}

	if output[0].Data["count"] != 2 {
		t.Errorf("Conteo incorrecto: %v", output[0].Data["count"])
	}
}

func TestOperatorReduceByKey_Collect(t *testing.T) {
	input := []types.Record{
		{Data: map[string]interface{}{"key": "A", "value": "v1"}},
		{Data: map[string]interface{}{"key": "A", "value": "v2"}},
		{Data: map[string]interface{}{"key": "B", "value": "v3"}},
	}

	task := &types.Task{
		Key:      "key",
		Function: "collect",
	}

	output, err := operatorReduceByKey(task, input)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	// Buscar el grupo con key=A
	var groupA *types.Record
	for _, record := range output {
		if record.Data["key"] == "A" {
			groupA = &record
			break
		}
	}

	if groupA == nil {
		t.Fatal("No encontró grupo con key=A")
	}

	values, ok := groupA.Data["values"].([]interface{})
	if !ok {
		t.Fatal("Campo 'values' no es un array")
	}

	if len(values) != 2 {
		t.Errorf("Esperaba 2 valores en grupo A, obtuvo %d", len(values))
	}
}

// ============================================================================
// TEST: JOIN
// ============================================================================

func TestOperatorJoin_Basic(t *testing.T) {
	left := []types.Record{
		{Data: map[string]interface{}{"id": "1", "name": "Alice"}},
		{Data: map[string]interface{}{"id": "2", "name": "Bob"}},
	}

	right := []types.Record{
		{Data: map[string]interface{}{"id": "1", "age": "30"}},
		{Data: map[string]interface{}{"id": "2", "age": "25"}},
	}

	task := &types.Task{
		Key: "id",
	}

	inputs := [][]types.Record{left, right}
	output, err := operatorJoin(task, inputs)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if len(output) != 2 {
		t.Fatalf("Esperaba 2 records joinados, obtuvo %d", len(output))
	}

	// Verificar que el join combinó los campos
	record1 := output[0]
	if record1.Data["id"] == "1" {
		if record1.Data["name"] != "Alice" {
			t.Error("Nombre incorrecto en join")
		}
		if record1.Data["age"] != "30" {
			t.Error("Edad incorrecta en join")
		}
	}
}

func TestOperatorJoin_NoMatch(t *testing.T) {
	left := []types.Record{
		{Data: map[string]interface{}{"id": "1", "name": "Alice"}},
	}

	right := []types.Record{
		{Data: map[string]interface{}{"id": "2", "age": "25"}},
	}

	task := &types.Task{
		Key: "id",
	}

	inputs := [][]types.Record{left, right}
	output, err := operatorJoin(task, inputs)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	// Inner join: si no hay match, no hay output
	if len(output) != 0 {
		t.Errorf("Join sin match debería retornar 0 records, obtuvo %d", len(output))
	}
}

func TestOperatorJoin_MultipleMatches(t *testing.T) {
	left := []types.Record{
		{Data: map[string]interface{}{"id": "1", "item": "A"}},
		{Data: map[string]interface{}{"id": "1", "item": "B"}},
	}

	right := []types.Record{
		{Data: map[string]interface{}{"id": "1", "price": "10"}},
	}

	task := &types.Task{
		Key: "id",
	}

	inputs := [][]types.Record{left, right}
	output, err := operatorJoin(task, inputs)

	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	// Debe generar producto cartesiano de matches
	if len(output) != 2 {
		t.Fatalf("Esperaba 2 records (1x1 join multiple), obtuvo %d", len(output))
	}
}

func TestOperatorJoin_InvalidInputCount(t *testing.T) {
	task := &types.Task{
		Key: "id",
	}

	// Join requiere exactamente 2 inputs
	inputs := [][]types.Record{
		{{Data: map[string]interface{}{"id": "1"}}},
	}

	_, err := operatorJoin(task, inputs)

	if err == nil {
		t.Fatal("Debería retornar error si no hay 2 inputs")
	}
}

// ============================================================================
// TEST: I/O Helpers
// ============================================================================

func TestWriteAndReadRecords(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(t, testDir)

	outputPath := filepath.Join(testDir, "output.json")

	records := []types.Record{
		{Data: map[string]interface{}{"id": "1", "value": "test"}},
		{Data: map[string]interface{}{"id": "2", "value": "data"}},
	}

	// Escribir
	err := writeRecordsToFile(outputPath, records)
	if err != nil {
		t.Fatalf("Error escribiendo records: %v", err)
	}

	// Leer
	readRecords, err := readRecordsFromFile(outputPath)
	if err != nil {
		t.Fatalf("Error leyendo records: %v", err)
	}

	if len(readRecords) != len(records) {
		t.Fatalf("Esperaba %d records, obtuvo %d", len(records), len(readRecords))
	}

	// Verificar contenido
	if readRecords[0].Data["id"] != "1" {
		t.Error("Primer record incorrecto")
	}

	if readRecords[1].Data["value"] != "data" {
		t.Error("Segundo record incorrecto")
	}
}

// ============================================================================
// BENCHMARK TESTS
// ============================================================================

func BenchmarkOperatorMap(b *testing.B) {
	input := make([]types.Record, 1000)
	for i := 0; i < 1000; i++ {
		input[i] = types.Record{
			Data: map[string]interface{}{"text": "HELLO WORLD"},
		}
	}

	task := &types.Task{
		Function: "to_lower",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		operatorMap(task, input)
	}
}

func BenchmarkOperatorFlatMap(b *testing.B) {
	input := make([]types.Record, 100)
	for i := 0; i < 100; i++ {
		input[i] = types.Record{
			Data: map[string]interface{}{"text": "the quick brown fox jumps over the lazy dog"},
		}
	}

	task := &types.Task{
		Function: "split_words",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		operatorFlatMap(task, input)
	}
}

func BenchmarkOperatorReduceByKey(b *testing.B) {
	input := make([]types.Record, 10000)
	words := []string{"hello", "world", "test", "data", "code"}

	for i := 0; i < 10000; i++ {
		input[i] = types.Record{
			Data: map[string]interface{}{"word": words[i%len(words)]},
		}
	}

	task := &types.Task{
		Key:      "word",
		Function: "count",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		operatorReduceByKey(task, input)
	}
}
