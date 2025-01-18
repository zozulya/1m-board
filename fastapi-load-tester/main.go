package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// TileUpdate represents the JSON payload for POST /update_tile
type TileUpdate struct {
	Row    int    `json:"row"`
	Col    int    `json:"col"`
	Color  string `json:"color"`
	UserID string `json:"user_id"`
}

// RequestMetrics represents the metrics for a request type
type RequestMetrics struct {
	latencies []time.Duration
}

func (m *RequestMetrics) addLatency(d time.Duration) {
	m.latencies = append(m.latencies, d)
}

func printLatencyHistogram(metrics *RequestMetrics, requestType string) {
	if len(metrics.latencies) == 0 {
		fmt.Printf("No latency data for %s requests\n", requestType)
		return
	}

	// Calculate percentiles
	sorted := append([]time.Duration(nil), metrics.latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p50 := sorted[len(sorted)*50/100]
	p90 := sorted[len(sorted)*90/100]
	p99 := sorted[len(sorted)*99/100]

	fmt.Printf("\nLatency histogram for %s requests:\n", requestType)
	fmt.Printf("50th percentile: %v\n", p50)
	fmt.Printf("90th percentile: %v\n", p90)
	fmt.Printf("99th percentile: %v\n", p99)

	// Create histogram buckets (in milliseconds)
	buckets := []int{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}
	counts := make([]int, len(buckets)+1)

	for _, lat := range metrics.latencies {
		ms := lat.Milliseconds()
		placed := false
		for i, bucket := range buckets {
			if ms <= int64(bucket) {
				counts[i]++
				placed = true
				break
			}
		}
		if !placed {
			counts[len(counts)-1]++
		}
	}

	// Print histogram
	fmt.Println("\nLatency distribution (ms):")
	for i, bucket := range buckets {
		prev := 0
		if i > 0 {
			prev = buckets[i-1]
		}
		if counts[i] > 0 {
			fmt.Printf("%4d-%4d ms: %d\n", prev, bucket, counts[i])
		}
	}
	if counts[len(counts)-1] > 0 {
		fmt.Printf(">=%5d ms: %d\n", buckets[len(buckets)-1], counts[len(counts)-1])
	}
}

func main() {
	// Define command-line flags
	postRate := flag.Float64("post-rate", 10, "Number of POST /update_tile requests per second")
	getRate := flag.Float64("get-rate", 10, "Number of GET /board requests per second")
	duration := flag.Int("duration", 0, "Duration of the test in seconds (0 for infinite)")
	url := flag.String("url", "http://localhost:8000", "Base URL of the FastAPI application")

	flag.Parse()

	fmt.Printf("Starting load test with the following parameters:\n")
	fmt.Printf("POST /update_tile rate: %.2f req/sec\n", *postRate)
	fmt.Printf("GET /board rate: %.2f req/sec\n", *getRate)
	if *duration > 0 {
		fmt.Printf("Test duration: %d seconds\n", *duration)
	} else {
		fmt.Printf("Test duration: Infinite (Ctrl+C to stop)\n")
	}
	fmt.Printf("Base URL: %s\n", *url)
	fmt.Println("----------------------------------------")

	// Create a shared HTTP client
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Create a context that is canceled on interrupt signal or after duration
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals to gracefully shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt)
		<-sigChan
		fmt.Println("\nInterrupt signal received. Shutting down...")
		cancel()
	}()

	// If duration is set, cancel the context after the duration
	if *duration > 0 {
		go func() {
			time.Sleep(time.Duration(*duration) * time.Second)
			fmt.Println("\nTest duration elapsed. Shutting down...")
			cancel()
		}()
	}

	// Initialize rate limiters
	postLimiter := rate.NewLimiter(rate.Limit(*postRate), 100)
	getLimiter := rate.NewLimiter(rate.Limit(*getRate), 100)

	// Counters for statistics
	var postSuccess int64
	var postFailure int64
	var getSuccess int64
	var getFailure int64

	// Add these variables after the counters
	postMetrics := &RequestMetrics{}
	getMetrics := &RequestMetrics{}

	// Start POST /update_tile goroutine
	go func() {
		for {
			// Wait for rate limiter
			if err := postLimiter.Wait(ctx); err != nil {
				// Context canceled
				return
			}

			// Generate a random TileUpdate payload
			payload := generateRandomTileUpdate()

			// Marshal payload to JSON
			jsonData, err := json.Marshal(payload)
			if err != nil {
				log.Printf("Error marshaling JSON: %v\n", err)
				atomic.AddInt64(&postFailure, 1)
				continue
			}

			// Create POST request
			req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/update_tile", *url), bytes.NewBuffer(jsonData))
			if err != nil {
				log.Printf("Error creating POST request: %v\n", err)
				atomic.AddInt64(&postFailure, 1)
				continue
			}
			req.Header.Set("Content-Type", "application/json")

			// Send the request
			start := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Error sending POST request: %v\n", err)
				atomic.AddInt64(&postFailure, 1)
				continue
			}
			postMetrics.addLatency(time.Since(start))

			// Read and discard the response body
			resp.Body.Close()

			// Check response status
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				atomic.AddInt64(&postSuccess, 1)
			} else {
				log.Printf("POST /update_tile returned status: %s\n", resp.Status)
				atomic.AddInt64(&postFailure, 1)
			}
		}
	}()

	// Start GET /board goroutine
	go func() {
		for {
			// Wait for rate limiter
			if err := getLimiter.Wait(ctx); err != nil {
				// Context canceled
				return
			}

			// Create GET request
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/board", *url), nil)
			if err != nil {
				log.Printf("Error creating GET request: %v\n", err)
				atomic.AddInt64(&getFailure, 1)
				continue
			}

			// Send the request
			start := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Error sending GET request: %v\n", err)
				atomic.AddInt64(&getFailure, 1)
				continue
			}
			getMetrics.addLatency(time.Since(start))

			// Read and discard the response body
			resp.Body.Close()

			// Check response status
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				atomic.AddInt64(&getSuccess, 1)
			} else {
				log.Printf("GET /board returned status: %s\n", resp.Status)
				atomic.AddInt64(&getFailure, 1)
			}
		}
	}()

	// Periodically print statistics every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Test ended, print final statistics
			fmt.Println("\nLoad test completed.")
			fmt.Printf("POST /update_tile: Success: %d, Failure: %d\n", atomic.LoadInt64(&postSuccess), atomic.LoadInt64(&postFailure))
			fmt.Printf("GET /board: Success: %d, Failure: %d\n", atomic.LoadInt64(&getSuccess), atomic.LoadInt64(&getFailure))

			printLatencyHistogram(postMetrics, "POST /update_tile")
			printLatencyHistogram(getMetrics, "GET /board")
			return
		case <-ticker.C:
			// Print current statistics
			fmt.Printf("POST /update_tile: Success: %d, Failure: %d | GET /board: Success: %d, Failure: %d\n",
				atomic.LoadInt64(&postSuccess),
				atomic.LoadInt64(&postFailure),
				atomic.LoadInt64(&getSuccess),
				atomic.LoadInt64(&getFailure),
			)
		}
	}
}

// generateRandomTileUpdate creates a TileUpdate with random data
func generateRandomTileUpdate() TileUpdate {
	colors := []string{"red", "blue", "green", "yellow", "purple", "orange", "black", "white", "cyan", "magenta"}
	row := rand.Intn(1000) // 0-999
	col := rand.Intn(1000) // 0-999
	color := colors[rand.Intn(len(colors))]

	// Generate a unique user_id using timestamp and a random number
	userID := fmt.Sprintf("user_%d_%d", time.Now().UnixNano(), rand.Intn(1000000))

	return TileUpdate{
		Row:    row,
		Col:    col,
		Color:  color,
		UserID: userID,
	}
}
