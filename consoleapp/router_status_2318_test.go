package consoleapp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

func routerStatusSettings() routerSettings {
	return routerSettings{Listen: "127.0.0.1:3310", ReadOnly: true, Policy: readrouter.Policy{CostThreshold: 5000, ScanRows: 100000}}
}

// #2318: the copies' snapshot times are joined to the counts by server id.
func TestRouterStatus_joinsTheCopiesToTheCounts(t *testing.T) {
	snap := time.Date(2026, 10, 11, 9, 30, 0, 0, time.FixedZone("x", -5*3600))
	readAt := time.Date(2026, 10, 11, 14, 31, 0, 0, time.UTC)
	last := &routerCopySnapshots{}
	last.set(map[string]time.Time{
		"counted":   snap,
		"copy-only": snap,
		"zero-time": {},
	}, readAt)
	numbers := console.RouterNumbers{CountingSince: "2026-10-11T14:00:00Z", Servers: map[string]console.RouterServerNumbers{
		"counted": {Copy: 4, MySQL: 9, Refused: 1, Reasons: map[string]uint64{"expensive_plan": 4}},
		"no-copy": {MySQL: 2, Reasons: map[string]uint64{"cheap_plan": 2}},
	}}
	doc := routerStatus(routerStatusSettings(), numbers, last)

	if doc.Version != console.RouterStatusVersion || doc.Listen != "127.0.0.1:3310" || !doc.ReadOnly || doc.CostThreshold != 5000 || doc.ScanRows != 100000 {
		t.Errorf("settings = %+v", doc)
	}
	if doc.CopySnapshotsReadAt != "2026-10-11T14:31:00Z" {
		t.Errorf("copy_snapshots_read_at = %q", doc.CopySnapshotsReadAt)
	}
	if c := doc.Servers["counted"]; c.Copy != 4 || c.MySQL != 9 || c.Refused != 1 || c.CopySnapshot != "2026-10-11T14:30:00Z" {
		t.Errorf("counted = %+v: want its counts kept and its snapshot in UTC", c)
	}
	if c, ok := doc.Servers["copy-only"]; !ok || c.Copy+c.MySQL+c.Refused != 0 || c.Reasons == nil || c.CopySnapshot != "2026-10-11T14:30:00Z" {
		t.Errorf("copy-only = %+v (present %v): want zeros, reasons not null, and its snapshot", c, ok)
	}
	if c := doc.Servers["no-copy"]; c.MySQL != 2 || c.CopySnapshot != "" {
		t.Errorf("no-copy = %+v: want no snapshot", c)
	}
	if _, ok := doc.Servers["zero-time"]; ok {
		t.Error("a server whose snapshot time is zero got an entry")
	}
}

// Before the first reading there is no reading time: "not read yet" must not
// look like "read, and no server has a copy".
func TestRouterStatus_beforeTheFirstReading(t *testing.T) {
	doc := routerStatus(routerStatusSettings(), console.RouterNumbers{Servers: map[string]console.RouterServerNumbers{}}, &routerCopySnapshots{})
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "copy_snapshots_read_at") {
		t.Errorf("a reading time before any reading: %s", raw)
	}
	for _, want := range []string{`"servers":{}`, `"version":1`, `"read_only":true`, `"listen":"127.0.0.1:3310"`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("want %s in %s", want, raw)
		}
	}
}

// The document is served beside the metrics, on the listener the router
// opens for them: GET answers, another method does not, /metrics is still
// there.
func TestRouterStatus_servedBesideTheMetrics(t *testing.T) {
	addr := freeAddr(t)
	last := &routerCopySnapshots{}
	var asked atomic.Int64
	numbers := func() console.RouterNumbers {
		n := asked.Add(1)
		return console.RouterNumbers{CountingSince: "2026-10-11T14:00:00Z", Servers: map[string]console.RouterServerNumbers{
			"s": {Copy: uint64(n), Reasons: map[string]uint64{}},
		}}
	}
	stop, err := streamrun.StartMetricsServerWith(addr, map[string]http.Handler{
		"GET " + routerStatusPath: routerStatusHandler(routerStatusSettings(), numbers, last),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)

	get := func() console.RouterStatusDoc {
		t.Helper()
		resp, err := http.Get("http://" + addr + routerStatusPath)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
			t.Fatalf("GET %s = %d %q", routerStatusPath, resp.StatusCode, resp.Header.Get("Content-Type"))
		}
		var doc console.RouterStatusDoc
		if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	// Read on every request, not once.
	if a, b := get().Servers["s"].Copy, get().Servers["s"].Copy; a != 1 || b != 2 {
		t.Errorf("two requests read %d then %d: want 1 then 2", a, b)
	}
	resp, err := http.Post("http://"+addr+routerStatusPath, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST %s = %d: want 405", routerStatusPath, resp.StatusCode)
	}
	resp, err = http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "# HELP") {
		t.Errorf("GET /metrics = %d, %d bytes", resp.StatusCode, len(body))
	}
}

// The loop that reads the copies leaves each reading for the document, a
// server that lost its copy included, and stops with its context.
func TestExportCopySnapshots_leavesEachReadingAndStops(t *testing.T) {
	at := time.Date(2026, 10, 11, 14, 30, 0, 0, time.UTC)
	var reads atomic.Int64
	read := func(context.Context) map[string]time.Time {
		if reads.Add(1) == 1 {
			return map[string]time.Time{"a": at, "b": at}
		}
		return map[string]time.Time{"a": at}
	}
	last := &routerCopySnapshots{}
	if got, readAt := last.get(); got != nil || !readAt.IsZero() {
		t.Fatalf("before the loop: %v read at %v", got, readAt)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); exportCopySnapshots(ctx, read, 2*time.Millisecond, last) }()
	waitUntil(t, "the second reading, without b", func() bool {
		got, readAt := last.get()
		_, hasB := got["b"]
		return !readAt.IsZero() && reads.Load() >= 2 && len(got) == 1 && !hasB && got["a"].Equal(at)
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop with its context")
	}
}

// #2327: the router's line for a worker is long, so that what bounds it is
// the wait and a refusal is never free to repeat; a console that shares its
// process with capture keeps the short one.
func TestNewRouterConsole_letsStatementsWaitTheirTurn(t *testing.T) {
	files := newRouterFiles(t)
	_, runner, err := newRouterConsole(files.settings("tok"), console.SQLPool{Workers: 2, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	if got := runner.MaxWaiters(); got != routerMaxWaiters || got < 1000 {
		t.Errorf("the router lets %d statements wait, want %d", got, routerMaxWaiters)
	}
	web, err := console.New(console.Config{Listen: "127.0.0.1:8090", Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(web.SQLSandbox().Close)
	if got := web.SQLSandbox().MaxWaiters(); got != 16 {
		t.Errorf("a console in the capture process lets %d statements wait, want 16", got)
	}
}
