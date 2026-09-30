package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/doctor"
)

// POST /api/servers/identify is step 1 of connecting a server (#1953): host
// and port, no login, and back comes what answered there or why nothing did.
// The probe dials an address the caller chose, so the route is write-tier,
// logged, and spaced out per address: each probe ends before a login, which a
// MySQL server may count against DBTrail's address until it blocks it (1129).

type identifyStub struct {
	calls atomic.Int32
	res   doctor.Identification
	err   error
	gate  chan struct{} // when set, each probe waits on it
}

func (s *identifyStub) probe(ctx context.Context, host, port string) (doctor.Identification, error) {
	s.calls.Add(1)
	if s.gate != nil {
		// Like the real probe, it stops when its context does.
		select {
		case <-s.gate:
		case <-ctx.Done():
			return doctor.Identification{}, ctx.Err()
		}
	}
	if s.err != nil {
		return doctor.Identification{}, s.err
	}
	res := s.res
	res.Addr, _ = doctor.TargetAddr(host, port)
	return res, nil
}

func newIdentifyServer(t *testing.T, stub *identifyStub) (*Server, *time.Time) {
	t.Helper()
	srv, _ := newSupervisorServer(t)
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	srv.identifyFn = stub.probe
	srv.identify.now = func() time.Time { return now }
	return srv, &now
}

func decodeIdentify(t *testing.T, body []byte) identifyResponse {
	t.Helper()
	var out identifyResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return out
}

func TestIdentifyAnswersWhatIsThere(t *testing.T) {
	stub := &identifyStub{res: doctor.Identification{Version: "10.11.6-MariaDB-log", Flavor: doctor.FlavorMariaDB, Managed: doctor.ManagedRDS}}
	srv, _ := newIdentifyServer(t, stub)
	rec, body := doServersReq(t, srv, "POST", "/api/servers/identify", `{"source_host":"db.example.com","source_port":"3306"}`)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	got := decodeIdentify(t, body)
	if got.Flavor != "mariadb" || got.Managed != "rds" || got.Version != "10.11.6-MariaDB-log" || got.Addr != "db.example.com:3306" || got.Cached {
		t.Errorf("got %+v", got)
	}
}

// A failure to get there is an answer, not an HTTP error: the screen draws it.
func TestIdentifyAFailureIsAnAnswer(t *testing.T) {
	stub := &identifyStub{res: doctor.Identification{Kind: doctor.KindTimeout, From: "10.0.3.7"}}
	srv, _ := newIdentifyServer(t, stub)
	rec, body := doServersReq(t, srv, "POST", "/api/servers/identify", `{"source_host":"db.example.com"}`)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	if got := decodeIdentify(t, body); got.Kind != "timeout" || got.From != "10.0.3.7" {
		t.Errorf("got %+v", got)
	}
}

// A second probe of the same address within the interval is answered from
// the first, however the address is spelled; after the interval it probes
// again.
func TestIdentifySpacesOutProbesPerAddress(t *testing.T) {
	stub := &identifyStub{res: doctor.Identification{Version: "8.4.3", Flavor: doctor.FlavorMySQL}}
	srv, now := newIdentifyServer(t, stub)
	for i, body := range []string{
		`{"source_host":"db.example.com","source_port":"3306"}`,
		`{"source_host":"DB.Example.com."}`,
		`{"source_host":"db.example.com:3306"}`,
	} {
		rec, out := doServersReq(t, srv, "POST", "/api/servers/identify", body)
		if rec.Code != 200 {
			t.Fatalf("call %d: code=%d body=%s", i, rec.Code, out)
		}
		if got := decodeIdentify(t, out); got.Cached != (i > 0) || got.Version != "8.4.3" {
			t.Errorf("call %d: cached=%v version=%q", i, got.Cached, got.Version)
		}
	}
	if n := stub.calls.Load(); n != 1 {
		t.Errorf("probed %d times within the interval, want 1", n)
	}
	// Another address is its own.
	doServersReq(t, srv, "POST", "/api/servers/identify", `{"source_host":"other.example.com"}`)
	if n := stub.calls.Load(); n != 2 {
		t.Errorf("another address probed %d times in total, want 2", n)
	}
	*now = now.Add(identifyMinInterval)
	_, out := doServersReq(t, srv, "POST", "/api/servers/identify", `{"source_host":"db.example.com"}`)
	if got := decodeIdentify(t, out); got.Cached || stub.calls.Load() != 3 {
		t.Errorf("after the interval: cached=%v calls=%d, want a fresh probe", got.Cached, stub.calls.Load())
	}
}

// Two presses at once still make one probe: the second waits for the first.
func TestIdentifyConcurrentRequestsShareOneProbe(t *testing.T) {
	stub := &identifyStub{res: doctor.Identification{Version: "8.4.3"}, gate: make(chan struct{})}
	srv, _ := newIdentifyServer(t, stub)
	var wg sync.WaitGroup
	codes := make([]int, 3)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/servers/identify", strings.NewReader(`{"source_host":"db.example.com"}`))
			req.Host = "127.0.0.1:8090"
			req.Header.Set("Authorization", "Bearer t")
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			codes[i] = rec.Code
		}()
	}
	// Release the probe only once the other two are waiting on it: released
	// earlier, they would be answered from the kept answer instead, and the
	// test would pass with the sharing broken.
	waitCalls(t, stub, 1)
	for deadline := time.Now().Add(5 * time.Second); ; {
		srv.identify.mu.Lock()
		c := srv.identify.inflight["db.example.com:3306"]
		n := 0
		if c != nil {
			n = c.waiters
		}
		srv.identify.mu.Unlock()
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d requests joined the running probe, waited for 2", n)
		}
		time.Sleep(time.Millisecond)
	}
	close(stub.gate)
	wg.Wait()
	if n := stub.calls.Load(); n != 1 {
		t.Errorf("three concurrent requests probed %d times, want 1", n)
	}
	for i, c := range codes {
		if c != 200 {
			t.Errorf("request %d answered %d", i, c)
		}
	}
}

// An error that names no cause is not kept: the next press probes again.
func TestIdentifyAnErrorIsNotKept(t *testing.T) {
	stub := &identifyStub{err: fmt.Errorf("connect to db.example.com:3306: %w", errors.New("something new"))}
	srv, _ := newIdentifyServer(t, stub)
	for range 2 {
		rec, body := doServersReq(t, srv, "POST", "/api/servers/identify", `{"source_host":"db.example.com"}`)
		if rec.Code != 502 {
			t.Errorf("code=%d body=%s, want 502", rec.Code, body)
		}
	}
	if n := stub.calls.Load(); n != 2 {
		t.Errorf("probed %d times, want 2 (an error must not be kept)", n)
	}
	// Nothing is left behind for it either.
	srv.identify.mu.Lock()
	n := len(srv.identify.kept) + len(srv.identify.inflight)
	srv.identify.mu.Unlock()
	if n != 0 {
		t.Errorf("%d entries kept after failed probes, want 0", n)
	}
}

// A request that gives up does not take its probe with it: the probe has
// already counted against the server, so its answer is kept and the next
// press gets it instead of probing again.
func TestIdentifyAnAbandonedRequestStillKeepsItsAnswer(t *testing.T) {
	stub := &identifyStub{res: doctor.Identification{Version: "8.4.3"}, gate: make(chan struct{})}
	srv, _ := newIdentifyServer(t, stub)
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/servers/identify", strings.NewReader(`{"source_host":"db.example.com"}`)).WithContext(ctx)
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer t")
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}()
	waitCalls(t, stub, 1)
	cancel()
	<-done // the request returned while its probe was still running
	close(stub.gate)
	for deadline := time.Now().Add(5 * time.Second); ; {
		srv.identify.mu.Lock()
		_, kept := srv.identify.kept["db.example.com:3306"]
		srv.identify.mu.Unlock()
		if kept {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the abandoned request's probe never kept its answer")
		}
		time.Sleep(time.Millisecond)
	}
	_, body := doServersReq(t, srv, "POST", "/api/servers/identify", `{"source_host":"db.example.com"}`)
	if got := decodeIdentify(t, body); !got.Cached || got.Version != "8.4.3" {
		t.Errorf("after an abandoned request: %+v, want its kept answer", got)
	}
	if n := stub.calls.Load(); n != 1 {
		t.Errorf("probed %d times, want 1", n)
	}
}

func TestIdentifyRefusesAnInvalidAddressWithoutProbing(t *testing.T) {
	stub := &identifyStub{}
	srv, _ := newIdentifyServer(t, stub)
	for _, body := range []string{`{"source_host":""}`, `{"source_host":"db.example.com","source_port":"99999"}`, `{"source_host":"a b"}`} {
		if rec, out := doServersReq(t, srv, "POST", "/api/servers/identify", body); rec.Code != 400 {
			t.Errorf("%s: code=%d body=%s, want 400", body, rec.Code, out)
		}
	}
	if rec, out := doServersReq(t, srv, "POST", "/api/servers/identify", `not json`); rec.Code != 400 {
		t.Errorf("bad json: code=%d body=%s, want 400", rec.Code, out)
	}
	if n := stub.calls.Load(); n != 0 {
		t.Errorf("an invalid address was probed %d times", n)
	}
}

func TestIdentifyRefusesOnAConsoleThatCannotCapture(t *testing.T) {
	srv := newRegistryServer(t) // no MonitorCtrl
	stub := &identifyStub{}
	srv.identifyFn = stub.probe
	if rec, body := doServersReq(t, srv, "POST", "/api/servers/identify", `{"source_host":"db.example.com"}`); rec.Code != 403 {
		t.Errorf("code=%d body=%s, want 403", rec.Code, body)
	}
	if stub.calls.Load() != 0 {
		t.Error("a console that cannot capture probed anyway")
	}
}

// Probing an address somebody typed is part of adding a server, and a
// read-only session must not be able to map the network with it.
func TestIdentifyIsWriteTier(t *testing.T) {
	perm, ok := permForRoute("POST", "/api/servers/identify")
	if !ok {
		t.Fatal("POST /api/servers/identify is not classified")
	}
	if perm != ext.PermServersWrite {
		t.Errorf("POST /api/servers/identify is %q, want %q", perm, ext.PermServersWrite)
	}
}

// The kept answers do not grow without bound: an old one is dropped when a
// new address is probed.
func TestIdentifyDropsOldAnswers(t *testing.T) {
	stub := &identifyStub{res: doctor.Identification{Version: "8.4.3"}}
	srv, now := newIdentifyServer(t, stub)
	for i := range 5 {
		doServersReq(t, srv, "POST", "/api/servers/identify", fmt.Sprintf(`{"source_host":"db%d.example.com"}`, i))
	}
	*now = now.Add(identifyMinInterval)
	doServersReq(t, srv, "POST", "/api/servers/identify", `{"source_host":"fresh.example.com"}`)
	srv.identify.mu.Lock()
	n := len(srv.identify.kept)
	srv.identify.mu.Unlock()
	if n != 1 {
		t.Errorf("%d answers kept after the interval, want 1 (the fresh one)", n)
	}
}

func waitCalls(t *testing.T, s *identifyStub, n int32) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); s.calls.Load() < n; {
		if time.Now().After(deadline) {
			t.Fatalf("the probe was called %d times, waited for %d", s.calls.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
}
