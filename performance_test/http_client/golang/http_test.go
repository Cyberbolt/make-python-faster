// 运行方法 cd performance_test/http_client/golang && go test -v -run .

package httpclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	URL            = "http://nginx:21000"
	DURATION       = 30 * time.Second
	CONCURRENCY    = 500
	CLIENT_TIMEOUT = 10 * time.Second
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

	client := &http.Client{
		Timeout: CLIENT_TIMEOUT,
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

func TestHttpPerformance(t *testing.T) {
	fmt.Printf("Starting test for %s with a concurrency of %d...\n", DURATION, CONCURRENCY)

	success, failed, duration := runTest(DURATION, CONCURRENCY)

	fmt.Printf("\nTest ran for: %.2f seconds\n", duration.Seconds())
	fmt.Printf("Successful requests: %d\n", success)
	fmt.Printf("Failed requests: %d\n", failed)

	if duration.Seconds() > 0 {
		rps := float64(success) / duration.Seconds()
		fmt.Printf("Successful requests per second (RPS): %.0f\n", rps)
	}
}
