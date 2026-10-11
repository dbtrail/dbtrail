package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// #2318: the address of a router as an operator types it.
func TestParseRouterURL(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"", ""},
		{"   ", ""},
		{"http://router:9091", "http://router:9091/status"},
		{"http://router:9091/", "http://router:9091/status"},
		{" http://127.0.0.1:9091/status ", "http://127.0.0.1:9091/status"},
		{"http://127.0.0.1:9091/status/", "http://127.0.0.1:9091/status"},
		{"https://router.internal", "https://router.internal/status"},
		{"http://[::1]:9091", "http://[::1]:9091/status"},
		{"http://proxy/router", "http://proxy/router/status"},
	} {
		got, err := ParseRouterURL(c.in)
		if err != nil || got != c.want {
			t.Errorf("ParseRouterURL(%q) = %q, %v: want %q", c.in, got, err, c.want)
		}
	}
	for _, c := range []struct{ in, says string }{
		{"router:9091", "http://"},
		{"127.0.0.1:9091", "http://"},
		{"ftp://admin:hunter2@router:9091", "user or a password"},
		{"http://admin:hunter2@:9091", "user or a password"},
		{"http://admin:hunter2@rou ter:9091", "not a URL"},
		{"ftp://router:9091", "http://"},
		{"http://", "names no host"},
		{"http://:9091", "names no host"},
		{"http://admin:hunter2@router:9091", "user or a password"},
		{"http://router:9091/status?x=1", "query"},
		{"http://router:9091/#frag", "query"},
		{"http://rou ter:9091", "not a URL"},
	} {
		got, err := ParseRouterURL(c.in)
		if err == nil || got != "" || !strings.Contains(err.Error(), c.says) {
			t.Errorf("ParseRouterURL(%q) = %q, %v: want a refusal that says %q", c.in, got, err, c.says)
		}
		if err != nil && strings.Contains(err.Error(), "hunter2") {
			t.Errorf("the refusal repeats a password: %v", err)
		}
	}
}

const routerDocJSON = `{"version":1,"listen":"0.0.0.0:3310","read_only":true,"cost_threshold":10000,"scan_rows":100000,
"copy_snapshots_read_at":"2026-10-11T14:31:00Z","counting_since":"2026-10-11T14:00:00Z",
"pool":{"workers":4,"running":1,"waiting":0,"threads_each":2,"memory_each":"2048MiB","worker_processes_started":5,"statements_per_worker":500},
"servers":{"s1":{"copy":4,"mysql":9,"refused":1,"reasons":{"expensive_plan":4,"cheap_plan":9,"copy_queue_full":1},"copy_snapshot":"2026-10-11T14:30:00Z"}}}`

func routerViewServer(t *testing.T, url string) *Server {
	t.Helper()
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", RouterURL: url})
	if err != nil {
		t.Fatal(err)
	}
	if r := srv.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	return srv
}

// The three answers of GET /api/router, and every way a router can fail to
// give its numbers: each is an error in words, never a document of zeros.
func TestRouterView_threeAnswers(t *testing.T) {
	if got := routerViewServer(t, "").routerView(context.Background()); got.Configured || got.URL != "" || got.Error != "" || got.Status != nil {
		t.Errorf("no address: %+v, want nothing but configured=false", got)
	}

	// The handler is swapped between cases while a request of an earlier one
	// (the slow one) may still be in the server: read and written under a lock.
	var mu sync.Mutex
	var serving http.HandlerFunc
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		h := serving
		mu.Unlock()
		h(w, r)
	}))
	setServe := func(h http.HandlerFunc) { mu.Lock(); serving = h; mu.Unlock() }
	t.Cleanup(ts.Close)
	srv := routerViewServer(t, ts.URL+"/status")
	srv.routerClient = newRouterClient(300 * time.Millisecond)

	setServe(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/status" {
			t.Errorf("asked %s %s", r.Method, r.URL.Path)
		}
		w.Write([]byte(routerDocJSON))
	})
	got := srv.routerView(context.Background())
	if !got.Configured || got.Error != "" || got.Newer || got.Status == nil || got.ReadAt == "" || got.URL != ts.URL+"/status" {
		t.Fatalf("answering router: %+v", got)
	}
	if s := got.Status.Servers["s1"]; s.Copy != 4 || s.MySQL != 9 || s.Refused != 1 || s.CopySnapshot != "2026-10-11T14:30:00Z" || got.Status.Pool == nil || got.Status.Pool.Workers != 4 || !got.Status.ReadOnly {
		t.Errorf("document = %+v", got.Status)
	}

	setServe(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(strings.Replace(routerDocJSON, `"version":1`, `"version":7,"something_new":true`, 1)))
	})
	if got := srv.routerView(context.Background()); got.Error != "" || !got.Newer || got.Status == nil || got.Status.Servers["s1"].Copy != 4 {
		t.Errorf("a later shape: %+v, want its numbers and newer=true", got)
	}

	for _, c := range []struct {
		what  string
		serve http.HandlerFunc
		says  string
	}{
		{"404", func(w http.ResponseWriter, _ *http.Request) { http.NotFound(w, nil) }, "HTTP 404"},
		{"500", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }, "HTTP 500"},
		{"a redirect is not followed", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/elsewhere" {
				w.Write([]byte(routerDocJSON))
				return
			}
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
		}, "HTTP 302"},
		{"not JSON", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("# HELP go_goroutines")) }, "not a router's status document"},
		{"an empty object", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("{}")) }, "not a router's status document"},
		{"another program's JSON", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`{"status":"ok","servers":{}}`)) }, "not a router's status document"},
		{"no servers object", func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte(`{"version":1,"counting_since":"2026-10-11T14:00:00Z"}`))
		}, "not a router's status document"},
		{"too slow", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
			}
		}, "no answer within 300ms"},
		{"too large", func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte(`{"version":1,"pad":"` + strings.Repeat("x", routerStatusMaxBytes) + `"}`))
		}, "larger than"},
	} {
		setServe(c.serve)
		got := srv.routerView(context.Background())
		if !got.Configured || got.Status != nil || got.ReadAt != "" || !strings.Contains(got.Error, c.says) {
			t.Errorf("%s: %+v, want no numbers and an error that says %q", c.what, got, c.says)
		}
	}

	ts.Close()
	if got := srv.routerView(context.Background()); got.Status != nil || !strings.Contains(got.Error, "could not be reached") || strings.Contains(got.Error, `Get "`) {
		t.Errorf("a router that is down: %+v, want \"could not be reached\" and the cause without the request line", got)
	}
}

// The handler, called directly: an unconfigured console says so in one
// field. Its permission is in the authz table; the route over the real mux
// is in consoleapp.
func TestRouterView_route(t *testing.T) {
	srv := routerViewServer(t, "")
	rec := httptest.NewRecorder()
	srv.handleRouterGet(rec, httptest.NewRequest("GET", "/api/router", nil))
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != `{"configured":false}` {
		t.Errorf("GET /api/router = %d %s", rec.Code, rec.Body)
	}
	// An address that could not be used at startup: said, with no request made.
	bad, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", RouterURLError: "BINTRAIL_CONSOLE_ROUTER_URL: the router's address names no host"})
	if err != nil {
		t.Fatal(err)
	}
	if r := bad.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	if got := bad.routerView(context.Background()); !got.Configured || got.Status != nil || got.URL != "" || !strings.Contains(got.Error, "cannot be used: BINTRAIL_CONSOLE_ROUTER_URL: the router's address names no host") {
		t.Errorf("an unusable address: %+v", got)
	}
	var back routerViewDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &back); err != nil || back.Configured {
		t.Errorf("decode: %v %+v", err, back)
	}
}
