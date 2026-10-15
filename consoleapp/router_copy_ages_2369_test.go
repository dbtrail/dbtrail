package consoleapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A slow index never makes the status document late: the lag is placed
// within its budget, and the document is served after it either way.
func TestResolveCopyAgesFirstIsBounded(t *testing.T) {
	served := false
	h := resolveCopyAgesFirst(func(ctx context.Context) { <-ctx.Done() }, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served = true }))
	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", routerStatusPath, nil))
	if took := time.Since(start); !served || took > 2*routerStatusResolveBudget {
		t.Fatalf("served %v after %s", served, took)
	}
}

// The loop bounds each call, so one hung read does not stop the next.
func TestResolveCopyAgesEveryBoundsEachCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan struct{}, 4)
	go resolveCopyAgesEvery(ctx, func(ctx context.Context) { <-ctx.Done(); calls <- struct{}{} }, 20*time.Millisecond)
	for range 2 {
		select {
		case <-calls:
		case <-time.After(5 * time.Second):
			t.Fatal("a hung call was not ended by its bound")
		}
	}
}
