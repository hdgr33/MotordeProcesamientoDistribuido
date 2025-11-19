// worker/operators.go
package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/types"
)

// Este archivo contiene todos los operadores de procesamiento de datos

// ============================================================================
// OPERADOR: READ_CSV
// ============================================================================

func operatorReadCSV(task *types.Task) ([]types.Record, error) {
	if len(task.InputPaths) == 0 {
		return nil, fmt.Errorf("no input path especificado")
	}

	path := task.InputPaths[0]
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error abriendo archivo %s: %w", path, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Leer header
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error leyendo headers: %w", err)
	}

	var records []types.Record

	// Leer todas las filas
	lineNum := 1
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error en línea %d: %w", lineNum, err)
		}

		// Convertir fila a Record
		data := make(map[string]interface{})
		for i, header := range headers {
			if i < len(row) {
				data[strings.TrimSpace(header)] = row[i]
			}
		}

		records = append(records, types.Record{Data: data})
		lineNum++
	}

	return records, nil
}

// ============================================================================
// OPERADOR: MAP
// ============================================================================

func operatorMap(task *types.Task, input []types.Record) ([]types.Record, error) {
	var output []types.Record

	for _, record := range input {
		transformed, err := applyMapFunction(task.Function, record)
		if err != nil {
			return nil, fmt.Errorf("error en map: %w", err)
		}
		output = append(output, transformed)
	}

	return output, nil
}

func applyMapFunction(fn string, record types.Record) (types.Record, error) {
	switch fn {
	case "to_lower":
		// Convertir todos los strings a minúsculas
		newData := make(map[string]interface{})
		for k, v := range record.Data {
			if str, ok := v.(string); ok {
				newData[k] = strings.ToLower(str)
			} else {
				newData[k] = v
			}
		}
		return types.Record{Data: newData}, nil

	case "to_upper":
		newData := make(map[string]interface{})
		for k, v := range record.Data {
			if str, ok := v.(string); ok {
				newData[k] = strings.ToUpper(str)
			} else {
				newData[k] = v
			}
		}
		return types.Record{Data: newData}, nil

	case "trim":
		newData := make(map[string]interface{})
		for k, v := range record.Data {
			if str, ok := v.(string); ok {
				newData[k] = strings.TrimSpace(str)
			} else {
				newData[k] = v
			}
		}
		return types.Record{Data: newData}, nil

	default:
		return record, nil // No transformation
	}
}

// ============================================================================
// OPERADOR: FILTER
// ============================================================================

func operatorFilter(task *types.Task, input []types.Record) ([]types.Record, error) {
	var output []types.Record

	for _, record := range input {
		keep, err := applyFilterFunction(task.Function, record)
		if err != nil {
			return nil, fmt.Errorf("error en filter: %w", err)
		}
		if keep {
			output = append(output, record)
		}
	}

	return output, nil
}

func applyFilterFunction(fn string, record types.Record) (bool, error) {
	switch fn {
	case "non_empty":
		// Mantener solo records con al menos un campo no vacío
		for _, v := range record.Data {
			if str, ok := v.(string); ok && str != "" {
				return true, nil
			}
		}
		return false, nil

	case "has_text":
		// Mantener records que tengan campo "text"
		if _, exists := record.Data["text"]; exists {
			return true, nil
		}
		return false, nil

	default:
		return true, nil // Keep all by default
	}
}

// ============================================================================
// OPERADOR: FLAT_MAP
// ============================================================================

func operatorFlatMap(task *types.Task, input []types.Record) ([]types.Record, error) {
	var output []types.Record

	for _, record := range input {
		expanded, err := applyFlatMapFunction(task.Function, record)
		if err != nil {
			return nil, fmt.Errorf("error en flat_map: %w", err)
		}
		output = append(output, expanded...)
	}

	return output, nil
}

func applyFlatMapFunction(fn string, record types.Record) ([]types.Record, error) {
	switch fn {
	case "split_words", "tokenize":
		// Dividir el campo "text" en palabras
		var results []types.Record

		text, ok := record.Data["text"].(string)
		if !ok {
			return nil, fmt.Errorf("campo 'text' no encontrado o no es string")
		}

		// Dividir por espacios y limpiar
		words := strings.Fields(text)
		for _, word := range words {
			// Limpiar puntuación básica
			word = strings.Trim(word, ".,!?;:\"'")
			if word != "" {
				results = append(results, types.Record{
					Data: map[string]interface{}{
						"word":        word,
						"original_id": record.Data["id"],
					},
				})
			}
		}

		return results, nil

	case "split_lines":
		// Dividir por líneas
		var results []types.Record
		text, ok := record.Data["text"].(string)
		if !ok {
			return []types.Record{record}, nil
		}

		lines := strings.Split(text, "\n")
		for i, line := range lines {
			if strings.TrimSpace(line) != "" {
				newData := make(map[string]interface{})
				for k, v := range record.Data {
					newData[k] = v
				}
				newData["text"] = line
				newData["line_num"] = i
				results = append(results, types.Record{Data: newData})
			}
		}

		return results, nil

	default:
		return []types.Record{record}, nil
	}
}

// ============================================================================
// OPERADOR: REDUCE_BY_KEY
// ============================================================================

func operatorReduceByKey(task *types.Task, input []types.Record) ([]types.Record, error) {
	keyField := task.Key
	if keyField == "" {
		keyField = "word" // Default para wordcount
	}

	// Agrupar por clave
	groups := make(map[string][]types.Record)
	for _, record := range input {
		key, ok := record.Data[keyField].(string)
		if !ok {
			// Si no es string, convertir a string
			key = fmt.Sprintf("%v", record.Data[keyField])
		}

		groups[key] = append(groups[key], record)
	}

	// Aplicar función de reducción
	var output []types.Record
	for key, records := range groups {
		reduced, err := applyReduceFunction(task.Function, key, keyField, records)
		if err != nil {
			return nil, fmt.Errorf("error en reduce: %w", err)
		}
		output = append(output, reduced)
	}

	return output, nil
}

func applyReduceFunction(fn string, key string, keyField string, records []types.Record) (types.Record, error) {
	switch fn {
	case "sum", "count":
		// Contar ocurrencias
		return types.Record{
			Data: map[string]interface{}{
				keyField: key,
				"count":  len(records),
			},
		}, nil

	case "collect":
		// Colectar todos los valores
		var values []interface{}
		for _, r := range records {
			for k, v := range r.Data {
				if k != keyField {
					values = append(values, v)
				}
			}
		}
		return types.Record{
			Data: map[string]interface{}{
				keyField: key,
				"values": values,
			},
		}, nil

	case "first":
		// Tomar el primer record
		return records[0], nil

	case "last":
		// Tomar el último record
		return records[len(records)-1], nil

	default:
		return types.Record{
			Data: map[string]interface{}{
				keyField: key,
				"count":  len(records),
			},
		}, nil
	}
}

// ============================================================================
// OPERADOR: JOIN (Básico)
// ============================================================================

func operatorJoin(task *types.Task, inputs [][]types.Record) ([]types.Record, error) {
	if len(inputs) != 2 {
		return nil, fmt.Errorf("join requiere exactamente 2 inputs, recibidos: %d", len(inputs))
	}

	left := inputs[0]
	right := inputs[1]

	keyField := task.Key
	if keyField == "" {
		keyField = "id" // Default
	}

	// Crear índice del dataset derecho
	rightIndex := make(map[string][]types.Record)
	for _, record := range right {
		key := fmt.Sprintf("%v", record.Data[keyField])
		rightIndex[key] = append(rightIndex[key], record)
	}

	// Hacer join
	var output []types.Record
	for _, leftRecord := range left {
		key := fmt.Sprintf("%v", leftRecord.Data[keyField])

		if rightRecords, exists := rightIndex[key]; exists {
			// Inner join: combinar con cada match
			for _, rightRecord := range rightRecords {
				joined := make(map[string]interface{})

				// Copiar campos del left
				for k, v := range leftRecord.Data {
					joined[k] = v
				}

				// Copiar campos del right (con prefijo si hay conflicto)
				for k, v := range rightRecord.Data {
					if _, exists := joined[k]; exists && k != keyField {
						joined["right_"+k] = v
					} else {
						joined[k] = v
					}
				}

				output = append(output, types.Record{Data: joined})
			}
		}
	}

	return output, nil
}

// ============================================================================
// I/O HELPERS
// ============================================================================

func readRecordsFromFile(path string) ([]types.Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []types.Record
	decoder := json.NewDecoder(file)

	// Intentar leer como array
	if err := decoder.Decode(&records); err != nil {
		// Si falla, intentar leer línea por línea (JSONL)
		file.Seek(0, 0)
		decoder = json.NewDecoder(file)

		for {
			var record types.Record
			if err := decoder.Decode(&record); err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			records = append(records, record)
		}
	}

	return records, nil
}

func writeRecordsToFile(path string, records []types.Record) error {
	// Crear directorio si no existe
	dir := path[:strings.LastIndex(path, "/")]
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	return encoder.Encode(records)
}
