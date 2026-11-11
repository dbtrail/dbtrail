package consoleapp

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/observe"
)

// lineOf renders attrs the way the service's log does, so a test reads the
// line a person would read.
func lineOf(attrs []any) string {
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})).Info(statusLogMsg, attrs...)
	return strings.TrimSpace(buf.String())
}

func TestStatusLogInterval(t *testing.T) {
	for value, want := range map[string]time.Duration{
		"":      time.Minute,
		"30s":   30 * time.Second,
		"5m":    5 * time.Minute,
		" 2m ":  2 * time.Minute,
		"off":   0,
		"OFF":   0,
		"0":     0,
		"false": 0,
		"10ms":  time.Second,
		// A typo keeps the log on: it must not be what turns it off.
		"1 minute": time.Minute,
		"abc":      time.Minute,
		"-5s":      time.Minute,
		"60":       time.Minute,
	} {
		t.Setenv(statusLogIntervalEnv, value)
		if got := statusLogInterval(); got != want {
			t.Errorf("%s=%q: %v, want %v", statusLogIntervalEnv, value, got, want)
		}
	}
}

func secs(n int64) *int64 { return &n }

func TestPortStatusLine(t *testing.T) {
	before := portCounts{copy: 100, mysql: 40, refused: 3, copyErr: 1, mysqlErr: 0, copySecs: 50, mysqlSecs: 2}
	now := portCounts{copy: 210, mysql: 52, refused: 5, copyErr: 2, mysqlErr: 1, copySecs: 96.2, mysqlSecs: 2.36, behind: secs(45)}
	got := lineOf(portStatusAttrs("shop", time.Minute, now, before))
	// 110 to the copy + 12 to MySQL + 2 refused = 124 seen in this minute,
	// not the 267 since the process started.
	want := "level=INFO msg=status service=port server=shop interval=1m0s queries=124 dbtrail=110 mysql=12 refused=2 errors=2 dbtrail_avg_ms=420 mysql_avg_ms=30 copy_behind_s=45"
	if got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
}

func TestPortStatusLineEdges(t *testing.T) {
	t.Run("a quiet minute has no averages and is not an instant answer", func(t *testing.T) {
		c := portCounts{copy: 100, mysql: 40, copySecs: 50, mysqlSecs: 2}
		got := lineOf(portStatusAttrs("shop", time.Minute, c, c))
		if !strings.Contains(got, "queries=0 dbtrail=0 mysql=0 refused=0 errors=0") || strings.Contains(got, "avg_ms") {
			t.Errorf("got %s", got)
		}
	})
	t.Run("a copy whose distance is not known says nothing, never 0", func(t *testing.T) {
		got := lineOf(portStatusAttrs("shop", time.Minute, portCounts{copy: 1, copySecs: 0.1}, portCounts{}))
		if strings.Contains(got, "copy_behind_s") {
			t.Errorf("got %s", got)
		}
	})
	t.Run("a copy that is caught up says 0", func(t *testing.T) {
		got := lineOf(portStatusAttrs("shop", time.Minute, portCounts{behind: secs(0)}, portCounts{}))
		if !strings.Contains(got, "copy_behind_s=0") {
			t.Errorf("got %s", got)
		}
	})
	t.Run("the first line counts from the start", func(t *testing.T) {
		got := lineOf(portStatusAttrs("shop", time.Minute, portCounts{copy: 7, copySecs: 0.7}, portCounts{}))
		if !strings.Contains(got, "queries=7 dbtrail=7") || !strings.Contains(got, "dbtrail_avg_ms=100") {
			t.Errorf("got %s", got)
		}
	})
	// Counters that went down started over. The line is then the new
	// count, not a number near 2^64.
	t.Run("counters that started over", func(t *testing.T) {
		got := lineOf(portStatusAttrs("shop", time.Minute, portCounts{copy: 3, copySecs: 0.3}, portCounts{copy: 900, copySecs: 400}))
		if !strings.Contains(got, "queries=3 dbtrail=3") || !strings.Contains(got, "dbtrail_avg_ms=100") {
			t.Errorf("got %s", got)
		}
	})
	t.Run("a name with a space stays one value", func(t *testing.T) {
		got := lineOf(portStatusAttrs("my shop", time.Minute, portCounts{}, portCounts{}))
		if !strings.Contains(got, `server="my shop"`) {
			t.Errorf("got %s", got)
		}
	})
}

func TestPortCountsOf(t *testing.T) {
	n := console.RouterServerNumbers{
		Copy: 999, MySQL: 999, Refused: 4, BehindSourceSeconds: secs(12),
		Answers: map[string]console.RouteAnswers{
			"copy":  {Count: 10, Errors: 1, Seconds: 2.5},
			"mysql": {Count: 3, Errors: 2, Seconds: 0.5},
		},
	}
	// The counts are of statements FINISHED (Answers), which is what has a
	// time and an error count; the routing decisions (Copy, MySQL) are not
	// the same number while a statement runs.
	got := portCountsOf(n)
	if got.copy != 10 || got.mysql != 3 || got.refused != 4 || got.copyErr != 1 || got.mysqlErr != 2 ||
		got.copySecs != 2.5 || got.mysqlSecs != 0.5 || got.behind == nil || *got.behind != 12 {
		t.Errorf("got %+v", got)
	}
	// With Engine Routing off the copy answers every statement and nothing
	// is routed: those are DBTrail's answers too, added to the routed ones.
	n.Unrouted = &console.RouteAnswers{Count: 7, Errors: 1, Seconds: 1.5}
	got = portCountsOf(n)
	if got.copy != 17 || got.copyErr != 2 || got.copySecs != 4 || got.mysql != 3 {
		t.Errorf("with unrouted answers: %+v", got)
	}
	only := portCountsOf(console.RouterServerNumbers{Unrouted: &console.RouteAnswers{Count: 4, Seconds: 0.4}})
	if line := lineOf(portStatusAttrs("shop", time.Minute, only, portCounts{})); !strings.Contains(line, "queries=4 dbtrail=4 mysql=0") || !strings.Contains(line, "dbtrail_avg_ms=100") {
		t.Errorf("a port that never routed: %s", line)
	}
	if empty := portCountsOf(console.RouterServerNumbers{}); empty.copy != 0 || empty.behind != nil {
		t.Errorf("a server with no answers: %+v", empty)
	}
}

func TestPortStatusLoggerWritesOneLinePerServerEachTime(t *testing.T) {
	readings := []console.RouterNumbers{
		{Servers: map[string]console.RouterServerNumbers{}},
		{Servers: map[string]console.RouterServerNumbers{
			"id-b": {Answers: map[string]console.RouteAnswers{"copy": {Count: 5, Seconds: 1}}},
			"id-a": {Answers: map[string]console.RouteAnswers{"mysql": {Count: 2, Seconds: 0.2}}},
		}},
		{Servers: map[string]console.RouterServerNumbers{
			"id-b": {Answers: map[string]console.RouteAnswers{"copy": {Count: 9, Seconds: 3}}},
			"id-a": {Answers: map[string]console.RouteAnswers{"mysql": {Count: 2, Seconds: 0.2}}},
		}},
	}
	var lines []string
	i := 0
	p := &portStatusLogger{
		numbers: func() console.RouterNumbers { r := readings[i]; i++; return r },
		nameOf:  func(id string) string { return map[string]string{"id-a": "shop", "id-b": "billing"}[id] },
		every:   time.Minute, last: map[string]portCounts{},
		log: func(msg string, attrs ...any) { lines = append(lines, lineOf(attrs)) },
	}
	p.write()
	if len(lines) != 1 || !strings.Contains(lines[0], "queries=0") || !strings.Contains(lines[0], "no statement has reached the port yet") {
		t.Fatalf("with no server: %q", lines)
	}
	lines = nil
	p.write()
	// By id, so the order does not change from one minute to the next.
	if len(lines) != 2 || !strings.Contains(lines[0], "server=shop") || !strings.Contains(lines[0], "queries=2 dbtrail=0 mysql=2") ||
		!strings.Contains(lines[1], "server=billing") || !strings.Contains(lines[1], "queries=5 dbtrail=5 mysql=0") {
		t.Fatalf("first reading: %q", lines)
	}
	lines = nil
	p.write()
	if len(lines) != 2 || !strings.Contains(lines[0], "server=shop") || !strings.Contains(lines[0], "queries=0") ||
		!strings.Contains(lines[1], "queries=4 dbtrail=4") || !strings.Contains(lines[1], "dbtrail_avg_ms=500") {
		t.Fatalf("second reading must be the difference: %q", lines)
	}
}

func captureRegistry(t *testing.T, indexed map[string]float64, errs map[string]float64, lag map[string]float64, lastSave map[string]float64) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	ix := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "bintrail", Subsystem: "stream", Name: "events_indexed_total"}, []string{"source"})
	er := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "bintrail", Subsystem: "stream", Name: "errors_total"}, []string{"source", "type"})
	lg := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "bintrail", Subsystem: "stream", Name: "availability_lag_seconds"}, []string{"source"})
	ls := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "bintrail", Subsystem: "stream", Name: "last_flush_timestamp_seconds"}, []string{"source"})
	other := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "bintrail", Subsystem: "stream", Name: "events_received_total"}, []string{"source"})
	reg.MustRegister(ix, er, lg, ls, other)
	for s, v := range indexed {
		ix.WithLabelValues(s).Add(v)
		other.WithLabelValues(s).Add(v * 10)
	}
	for s, v := range errs {
		er.WithLabelValues(s, "flush").Add(v)
		er.WithLabelValues(s, "parse").Add(1)
	}
	for s, v := range lag {
		lg.WithLabelValues(s).Set(v)
	}
	for s, v := range lastSave {
		ls.WithLabelValues(s).Set(v)
	}
	return reg
}

// The names read here are the ones the stream registers. Were one renamed
// there, the line would go quiet with no error, so this drives the stream's
// own handles and reads them back through the registry the daemon uses.
func TestCaptureCountsAreReadFromTheStreamsOwnMetrics(t *testing.T) {
	const source = "statuslog-test-source"
	m := observe.ForSource(source)
	m.EventsIndexed.Add(41)
	m.Errors.WithLabelValues("batch_flush").Add(2)
	m.Errors.WithLabelValues("checkpoint").Add(1)
	m.AvailabilityLag.Set(7)
	m.LastFlushTimestamp.Set(1234)
	// The receive-side figures must not be what is read.
	m.EventsReceived.Add(1000)
	m.ReplicationLag.Set(99)
	got, err := readCaptureCounts(prometheus.DefaultGatherer)
	if err != nil {
		t.Fatal(err)
	}
	c, ok := got[source]
	if !ok {
		t.Fatalf("the source is not among %d read: a metric name here no longer matches the stream's", len(got))
	}
	if c.recorded != 41 || c.errors != 3 || !c.hasBehind || c.behind != 7 || c.lastSave != 1234 {
		t.Errorf("read %+v, want recorded 41, errors 3, behind 7, last save 1234", c)
	}
}

func TestReadCaptureCounts(t *testing.T) {
	reg := captureRegistry(t,
		map[string]float64{"id-a": 5321, "id-b": 7},
		map[string]float64{"id-a": 2},
		map[string]float64{"id-a": 2.4, "id-b": 0},
		map[string]float64{"id-a": 1000})
	got, err := readCaptureCounts(reg)
	if err != nil {
		t.Fatal(err)
	}
	a, b := got["id-a"], got["id-b"]
	// Errors are added over their types (2 flush + 1 parse); the received
	// counter, ten times the indexed one here, is not what is recorded.
	if a.recorded != 5321 || a.errors != 3 || !a.hasBehind || a.behind != 2.4 || a.lastSave != 1000 {
		t.Errorf("id-a = %+v", a)
	}
	if b.recorded != 7 || b.errors != 0 || !b.hasBehind || b.behind != 0 || b.lastSave != 0 {
		t.Errorf("id-b = %+v", b)
	}
}

func TestCaptureStatusLine(t *testing.T) {
	at := time.Unix(1060, 0)
	before := captureCounts{recorded: 5000, errors: 1}
	now := captureCounts{recorded: 5321, errors: 3, behind: 2.4, hasBehind: true, lastSave: 1000}
	got := lineOf(captureStatusAttrs("shop", time.Minute, now, before, at))
	want := "level=INFO msg=status service=capture server=shop interval=1m0s changes=321 errors=2 behind_s=2 since_last_save_s=60"
	if got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
	// Nothing saved yet: no time since a save that never happened. A clock
	// that reads earlier than the save is 0, not a negative number.
	if got := lineOf(captureStatusAttrs("shop", time.Minute, captureCounts{}, captureCounts{}, at)); strings.Contains(got, "since_last_save_s") || strings.Contains(got, "behind_s") {
		t.Errorf("nothing known yet: %s", got)
	}
	if got := lineOf(captureStatusAttrs("shop", time.Minute, captureCounts{lastSave: 2000}, captureCounts{}, at)); !strings.Contains(got, "since_last_save_s=0") {
		t.Errorf("a save in the future: %s", got)
	}
}

func TestCaptureStatusLogger(t *testing.T) {
	var lines []string
	c := &captureStatusLogger{
		gather: prometheus.NewRegistry(),
		nameOf: func(id string) string { return "name-of-" + id },
		every:  time.Minute, last: map[string]captureCounts{}, now: func() time.Time { return time.Unix(1060, 0) },
		log: func(msg string, attrs ...any) { lines = append(lines, lineOf(attrs)) },
	}
	c.write()
	if len(lines) != 1 || !strings.Contains(lines[0], "no server is being recorded yet") {
		t.Fatalf("with no source: %q", lines)
	}
	lines = nil
	c.gather = captureRegistry(t, map[string]float64{"x": 10}, nil, nil, nil)
	c.write()
	c.gather = captureRegistry(t, map[string]float64{"x": 25}, nil, nil, nil)
	c.write()
	if len(lines) != 2 || !strings.Contains(lines[0], "server=name-of-x") || !strings.Contains(lines[0], "changes=10") || !strings.Contains(lines[1], "changes=15") {
		t.Fatalf("lines = %q", lines)
	}
}

// A line that panics must not take the service with it, and the next one is
// still written.
func TestEveryStatusIntervalSurvivesAPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan int, 10)
	n := 0
	go everyStatusInterval(ctx, 5*time.Millisecond, func() {
		n++
		calls <- n
		if n == 1 {
			panic(fmt.Sprint("first line"))
		}
	})
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-calls:
			if got >= 2 {
				return
			}
		case <-deadline:
			t.Fatal("no second line after the first one panicked")
		}
	}
}

func TestEveryStatusIntervalOffWritesNothing(t *testing.T) {
	done := make(chan struct{})
	go func() {
		everyStatusInterval(context.Background(), 0, func() { t.Error("wrote a line with the log off") })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("with the log off it must return at once, not wait")
	}
}
