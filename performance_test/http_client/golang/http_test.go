// 运行方法 cd performance_test/http_client/golang && go test -v -run .

package httpclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	URL                                = "http://nginx:21000"
	DURATION                           = 30 * time.Second
	CLIENT_TIMEOUT                     = 10 * time.Second
	SINGLE_CORE_CONCURRENCY            = 50
	MULTI_CORE_WORKERS                 = 4
	MULTI_CORE_CONCURRENCY_PER_WORKER  = 50
)

func worker(ctx context.Context, client *http.Client, wg *sync.WaitGroup, success, failed *atomic.Uint64) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			req, err := http.NewRequestWithContext(ctx, "GET", URL, nil)
			if err != nil {
				continue
			}

			resp, err := client.Do(req)
			if err != nil {
				failed.Add(1)
				continue
			}

			// We don't need the body, but reading it ensures the connection
			// is released back to the pool properly.
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				success.Add(1)
			} else {
				failed.Add(1)
			}
		}
	}
}

func runTest(duration time.Duration, concurrency int) (uint64, uint64, time.Duration) {
	var success, failed atomic.Uint64

	transport := &http.Transport{
		MaxIdleConns:        concurrency,
		MaxIdleConnsPerHost: concurrency,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   CLIENT_TIMEOUT,
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(concurrency)

	start := time.Now()
	for i := 0; i < concurrency; i++ {
		go worker(ctx, client, wg, &success, &failed)
	}
	wg.Wait()
	actualDuration := time.Since(start)

	return success.Load(), failed.Load(), actualDuration
}

func TestSingleCore(t *testing.T) {
	runtime.GOMAXPROCS(1)
	fmt.Printf("Starting single-core test for %s with a concurrency of %d...\n", DURATION, SINGLE_CORE_CONCURRENCY)

	success, failed, duration := runTest(DURATION, SINGLE_CORE_CONCURRENCY)

	fmt.Println("\n--- Single-Core Test Results ---")
	fmt.Printf("Test ran for: %.2f seconds\n", duration.Seconds())
	fmt.Printf("Successful requests: %d\n", success)
	fmt.Printf("Failed requests: %d\n", failed)

	rps := 0.0
	if duration.Seconds() > 0 {
		rps = float64(success) / duration.Seconds()
	}
	fmt.Printf("Successful requests per second (RPS): %.0f\n", rps)
}

func TestMultiCore(t *testing.T) {
	runtime.GOMAXPROCS(MULTI_CORE_WORKERS)
	fmt.Printf(
		"Starting multi-core test for %s with a concurrency of %d per worker across %d workers...\n",
		DURATION,
		MULTI_CORE_CONCURRENCY_PER_WORKER,
		MULTI_CORE_WORKERS,
	)

	results := make(chan struct {
		success  uint64
		failed   uint64
		duration time.Duration
	}, MULTI_CORE_WORKERS)

	var wg sync.WaitGroup
	wg.Add(MULTI_CORE_WORKERS)

	start := time.Now()
	for i := 0; i < MULTI_CORE_WORKERS; i++ {
		go func() {
			defer wg.Done()
			success, failed, duration := runTest(DURATION, MULTI_CORE_CONCURRENCY_PER_WORKER)
			results <- struct {
				success  uint64
				failed   uint64
				duration time.Duration
			}{success, failed, duration}
		}()
	}

	wg.Wait()
	close(results)
	totalDuration := time.Since(start)

	var totalSuccess, totalFailed uint64
	var totalRps float64

	for res := range results {
		totalSuccess += res.success
		totalFailed += res.failed
		if res.duration.Seconds() > 0 {
			totalRps += float64(res.success) / res.duration.Seconds()
		}
	}

	fmt.Println("\n--- Multi-Core Test Results ---")
	fmt.Printf("Test ran for: %.2f seconds\n", totalDuration.Seconds())
	fmt.Printf("Total successful requests: %d\n", totalSuccess)
	fmt.Printf("Total failed requests: %d\n", totalFailed)
	fmt.Printf("Aggregated RPS (sum of RPS from each worker): %.0f\n", totalRps)
}
