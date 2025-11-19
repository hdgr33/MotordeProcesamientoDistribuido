// client/main.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/protocol"
	"github.com/hdgr33/MotordeProcesamientoDistribuido/PROYECTO-PSO-BATCH/common/types"
)

const (
	defaultMasterURL = "http://localhost:8080"
)

type Client struct {
	masterURL string
	client    *http.Client
}

func NewClient(masterURL string) *Client {
	return &Client{
		masterURL: masterURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	masterURL := os.Getenv("MASTER_URL")
	if masterURL == "" {
		masterURL = defaultMasterURL
	}

	client := NewClient(masterURL)

	command := os.Args[1]

	switch command {
	case "submit":
		if len(os.Args) < 3 {
			fmt.Println("❌ Error: falta el archivo de job")
			fmt.Println("Uso: client submit <job.json>")
			os.Exit(1)
		}
		handleSubmit(client, os.Args[2])

	case "status":
		if len(os.Args) < 3 {
			fmt.Println("❌ Error: falta el ID del job")
			fmt.Println("Uso: client status <job-id>")
			os.Exit(1)
		}
		handleStatus(client, os.Args[2])

	case "results":
		if len(os.Args) < 3 {
			fmt.Println("❌ Error: falta el ID del job")
			fmt.Println("Uso: client results <job-id>")
			os.Exit(1)
		}
		handleResults(client, os.Args[2])

	case "list":
		handleList(client)

	case "workers":
		handleWorkers(client)

	case "metrics":
		handleMetrics(client)

	case "watch":
		if len(os.Args) < 3 {
			fmt.Println("❌ Error: falta el ID del job")
			fmt.Println("Uso: client watch <job-id>")
			os.Exit(1)
		}
		handleWatch(client, os.Args[2])

	default:
		fmt.Printf("❌ Comando desconocido: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

// ============================================================================
// COMMANDS
// ============================================================================

func handleSubmit(c *Client, jobFile string) {
	// Leer archivo de job
	data, err := os.ReadFile(jobFile)
	if err != nil {
		fmt.Printf("❌ Error leyendo archivo: %v\n", err)
		os.Exit(1)
	}

	// Validar JSON
	var job types.Job
	if err := json.Unmarshal(data, &job); err != nil {
		fmt.Printf("❌ JSON inválido: %v\n", err)
		os.Exit(1)
	}

	// Enviar al master
	resp, err := c.client.Post(
		c.masterURL+protocol.EndpointJobSubmit,
		"application/json",
		bytes.NewBuffer(data),
	)
	if err != nil {
		fmt.Printf("❌ Error conectando con master: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		fmt.Printf("❌ Error del servidor (%d):\n%s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var result types.JobSubmitResponse
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Printf("❌ Error parseando respuesta: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ Job enviado exitosamente")
	fmt.Printf("📋 Job ID: %s\n", result.JobID)
	fmt.Printf("📊 Status: %s\n", result.Status)
	if result.Message != "" {
		fmt.Printf("💬 Mensaje: %s\n", result.Message)
	}
	fmt.Printf("\n💡 Para ver el progreso: client watch %s\n", result.JobID)
}

func handleStatus(c *Client, jobID string) {
	endpoint := fmt.Sprintf(c.masterURL+"/api/v1/jobs/%s", jobID)
	resp, err := c.client.Get(endpoint)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("❌ Job %s no encontrado\n", jobID)
		os.Exit(1)
	}

	var status types.JobStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		fmt.Printf("❌ Error parseando respuesta: %v\n", err)
		os.Exit(1)
	}

	printJobStatus(status)
}

func handleResults(c *Client, jobID string) {
	endpoint := fmt.Sprintf(c.masterURL+"/api/v1/jobs/%s/results", jobID)
	resp, err := c.client.Get(endpoint)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("❌ Job %s no encontrado\n", jobID)
		os.Exit(1)
	}

	var results types.JobResultsResponse
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		fmt.Printf("❌ Error parseando respuesta: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("📋 Resultados del Job: %s\n", results.JobID)
	fmt.Printf("📊 Status: %s\n", results.Status)

	if len(results.OutputPaths) > 0 {
		fmt.Println("\n📁 Archivos de salida:")
		for i, path := range results.OutputPaths {
			fmt.Printf("  %d. %s\n", i+1, path)
		}
	} else {
		fmt.Println("\n⚠️  No hay resultados disponibles aún")
	}
}

func handleList(c *Client) {
	// TODO: Implementar endpoint de listado en master
	fmt.Println("⚠️  Comando 'list' en desarrollo")
	fmt.Println("Por ahora usa: client status <job-id>")
}

func handleWorkers(c *Client) {
	endpoint := c.masterURL + "/api/v1/workers"
	resp, err := c.client.Get(endpoint)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var workers []types.WorkerInfo
	if err := json.NewDecoder(resp.Body).Decode(&workers); err != nil {
		fmt.Printf("❌ Error parseando respuesta: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("👷 Workers activos: %d\n\n", len(workers))

	if len(workers) == 0 {
		fmt.Println("⚠️  No hay workers registrados")
		return
	}

	for _, w := range workers {
		status := getStatusEmoji(w.Status)
		fmt.Printf("%s %s\n", status, w.ID)
		fmt.Printf("   📍 Address: %s\n", w.Address)
		fmt.Printf("   📊 Status: %s\n", w.Status)
		fmt.Printf("   ⚙️  Active Tasks: %d\n", w.ActiveTasks)
		fmt.Printf("   📈 Total Tasks: %d\n", w.TotalTasks)

		timeSince := time.Since(w.LastHeartbeat)
		fmt.Printf("   💓 Last Heartbeat: %s ago\n", formatDuration(timeSince))
		fmt.Println()
	}
}

func handleMetrics(c *Client) {
	resp, err := c.client.Get(c.masterURL + protocol.EndpointMetrics)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var metrics map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		fmt.Printf("❌ Error parseando respuesta: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("📊 Métricas del Sistema")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━")

	prettyPrint(metrics)
}

func handleWatch(c *Client, jobID string) {
	fmt.Printf("👀 Monitoreando job %s (Ctrl+C para salir)\n\n", jobID)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	lastStatus := ""

	for {
		endpoint := fmt.Sprintf(c.masterURL+"/api/v1/jobs/%s", jobID)
		resp, err := c.client.Get(endpoint)

		if err != nil {
			fmt.Printf("\r❌ Error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusNotFound {
			fmt.Printf("\r❌ Job %s no encontrado\n", jobID)
			resp.Body.Close()
			break
		}

		var status types.JobStatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		// Limpiar pantalla y mostrar status
		if status.Job.Status != lastStatus {
			fmt.Print("\033[2J\033[H") // Clear screen
			fmt.Printf("👀 Monitoreando job %s\n\n", jobID)
		}

		printJobStatus(status)

		lastStatus = status.Job.Status

		// Salir si el job terminó
		if status.Job.Status == "COMPLETED" || status.Job.Status == "FAILED" {
			fmt.Println("\n✨ Monitoreo finalizado")
			break
		}

		<-ticker.C
	}
}

// ============================================================================
// HELPERS
// ============================================================================

func printJobStatus(status types.JobStatusResponse) {
	job := status.Job

	statusEmoji := map[string]string{
		"PENDING":   "⏳",
		"RUNNING":   "▶️",
		"COMPLETED": "✅",
		"FAILED":    "❌",
	}

	emoji := statusEmoji[job.Status]
	if emoji == "" {
		emoji = "❓"
	}

	fmt.Printf("%s Job: %s (%s)\n", emoji, job.Name, job.ID)
	fmt.Printf("📊 Status: %s\n", job.Status)
	fmt.Printf("📈 Progress: %.1f%%\n", status.Progress)
	fmt.Printf("📦 Tasks: %d total | %d done | %d pending | %d failed\n",
		status.TasksTotal, status.TasksDone, status.TasksPending, status.TasksFailed)

	elapsed := time.Since(job.SubmittedAt)
	fmt.Printf("⏱️  Elapsed: %s\n", formatDuration(elapsed))

	if job.CompletedAt != nil {
		duration := job.CompletedAt.Sub(job.SubmittedAt)
		fmt.Printf("✨ Completed in: %s\n", formatDuration(duration))
	}
}

func printUsage() {
	fmt.Println("🚀 Cliente PSO Batch - Motor de Procesamiento Distribuido")
	fmt.Println()
	fmt.Println("Uso: client <comando> [argumentos]")
	fmt.Println()
	fmt.Println("Comandos disponibles:")
	fmt.Println("  submit <job.json>    Enviar un nuevo job al cluster")
	fmt.Println("  status <job-id>      Ver el estado de un job")
	fmt.Println("  results <job-id>     Obtener resultados de un job")
	fmt.Println("  watch <job-id>       Monitorear progreso en tiempo real")
	fmt.Println("  workers              Listar workers activos")
	fmt.Println("  metrics              Ver métricas del sistema")
	fmt.Println()
	fmt.Println("Variables de entorno:")
	fmt.Println("  MASTER_URL           URL del master (default: http://localhost:8080)")
	fmt.Println()
	fmt.Println("Ejemplos:")
	fmt.Println("  client submit examples/wordcount.json")
	fmt.Println("  client watch job-1234567890")
	fmt.Println("  MASTER_URL=http://master:8080 client workers")
}

func getStatusEmoji(status string) string {
	switch status {
	case "IDLE":
		return "💤"
	case "BUSY":
		return "⚙️"
	case "DOWN":
		return "💀"
	default:
		return "❓"
	}
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.0fm %.0fs", d.Minutes(), d.Seconds()-d.Minutes()*60)
	}
	return fmt.Sprintf("%.0fh %.0fm", d.Hours(), d.Minutes()-d.Hours()*60)
}

func prettyPrint(data interface{}) {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fmt.Printf("%v\n", data)
		return
	}
	fmt.Println(string(b))
}
