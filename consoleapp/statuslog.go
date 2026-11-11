package consoleapp

import (
	"context"
	"log/slog"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The status log: one line a minute per server, in the service's own log, of
// what DBTrail did in that minute. It is for a person at a terminal,
//
//	journalctl -u bintrail-router -f | grep msg=status
//	docker compose logs -f router | grep msg=status
//
// and answers "is it doing anything?" without a browser or a metrics stack.
// The process that answers the MySQL port (router) says how many statements
// it saw and who answered them; the one that records changes (watch) says
// how many it recorded and how far behind it is. Each line holds only what
// its own process knows.
//
// The numbers are the ones the web interface and /metrics already show,
// taken as the difference between two readings, so a line is about its
// interval and not about everything since the process started.

// statusLogMsg is the message of every status line: what to grep for.
const statusLogMsg = "status"

// statusLogIntervalEnv sets how often a line is written. "off" or 0 writes
// none.
const statusLogIntervalEnv = "BINTRAIL_STATUS_LOG_INTERVAL"

const statusLogDefault = time.Minute

// statusLogInterval reads the interval; 0 means no status log. A value that
// is not a duration keeps the default and says so, once, at startup: a typo
// must not silently turn the log off.
func statusLogInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(statusLogIntervalEnv))
	switch strings.ToLower(raw) {
	case "":
		return statusLogDefault
	case "off", "0", "no", "false":
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		slog.Warn("the status log's interval is not a duration such as 30s, 1m or 5m; one minute is used",
			"setting", statusLogIntervalEnv, "value", raw)
		return statusLogDefault
	}
	// Below a second it would be the log, not a line in it.
	if d > 0 && d < time.Second {
		return time.Second
	}
	return d
}

// everyStatusInterval calls write every interval until ctx ends. Nothing
// runs when the interval is 0.
func everyStatusInterval(ctx context.Context, every time.Duration, write func()) {
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// A status line must never be what stops the service.
			func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("the status log line could not be written", "panic", r)
					}
				}()
				write()
			}()
		}
	}
}

// ── the MySQL port ────────────────────────────────────────────────────────

// portCounts is one reading of what one server's port has done since the
// process started.
type portCounts struct {
	copy, mysql, refused   uint64
	copyErr, mysqlErr      uint64
	copySecs, mysqlSecs    float64
	behind                 *int64 // the copy's distance to the source at its last answer
	behindUnknown, pending bool
}

func portCountsOf(n console.RouterServerNumbers) portCounts {
	c := portCounts{refused: n.Refused, behind: n.BehindSourceSeconds,
		pending: n.BehindSourcePending, behindUnknown: n.BehindSourceUnknownReason != ""}
	if a, ok := n.Answers["copy"]; ok {
		c.copy, c.copyErr, c.copySecs = a.Count, a.Errors, a.Seconds
	}
	if a, ok := n.Answers["mysql"]; ok {
		c.mysql, c.mysqlErr, c.mysqlSecs = a.Count, a.Errors, a.Seconds
	}
	// With Engine Routing off, which is how the port starts, the copy
	// answers every statement and nothing is "routed": those are counted on
	// their own and are DBTrail's answers all the same.
	if a := n.Unrouted; a != nil {
		c.copy += a.Count
		c.copyErr += a.Errors
		c.copySecs += a.Seconds
	}
	return c
}

// since is how much a counter grew. A counter that went DOWN means the
// numbers started over (the process did not, so something reset them): the
// new value is then the whole interval's, never a huge wrapped number.
func since(now, before uint64) uint64 {
	if now < before {
		return now
	}
	return now - before
}

func sinceSecs(now, before float64) float64 {
	if now < before {
		return now
	}
	return now - before
}

// avgMillis is the mean time of n statements that took secs in all, in
// milliseconds; -1 when there were none, so "no statements" never reads as
// "instant".
func avgMillis(secs float64, n uint64) int64 {
	if n == 0 {
		return -1
	}
	return int64(math.Round(secs / float64(n) * 1000))
}

// portStatusAttrs is one server's line for the interval between two
// readings. queries is every statement the port finished or refused;
// dbtrail and mysql are who answered.
func portStatusAttrs(name string, every time.Duration, now, before portCounts) []any {
	copyN, mysqlN, refused := since(now.copy, before.copy), since(now.mysql, before.mysql), since(now.refused, before.refused)
	attrs := []any{
		"service", "port", "server", name, "interval", every.String(),
		"queries", copyN + mysqlN + refused,
		"dbtrail", copyN, "mysql", mysqlN, "refused", refused,
		"errors", since(now.copyErr, before.copyErr) + since(now.mysqlErr, before.mysqlErr),
	}
	if ms := avgMillis(sinceSecs(now.copySecs, before.copySecs), copyN); ms >= 0 {
		attrs = append(attrs, "dbtrail_avg_ms", ms)
	}
	if ms := avgMillis(sinceSecs(now.mysqlSecs, before.mysqlSecs), mysqlN); ms >= 0 {
		attrs = append(attrs, "mysql_avg_ms", ms)
	}
	// How far the copy was behind the source at its last answer. Left out
	// when it is not known, never written as 0.
	if now.behind != nil {
		attrs = append(attrs, "copy_behind_s", *now.behind)
	}
	return attrs
}

// portStatusLogger writes the port's status lines from successive readings.
type portStatusLogger struct {
	numbers func() console.RouterNumbers
	nameOf  func(id string) string
	every   time.Duration
	last    map[string]portCounts
	log     func(msg string, attrs ...any)
}

func (p *portStatusLogger) write() {
	servers := p.numbers().Servers
	ids := make([]string, 0, len(servers))
	for id := range servers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	next := make(map[string]portCounts, len(ids))
	for _, id := range ids {
		now := portCountsOf(servers[id])
		next[id] = now
		p.log(statusLogMsg, portStatusAttrs(p.nameOf(id), p.every, now, p.last[id])...)
	}
	if len(ids) == 0 {
		// Still a line: silence would not tell "nothing to do" from "stopped".
		p.log(statusLogMsg, "service", "port", "interval", p.every.String(), "queries", 0, "note", "no statement has reached the port yet")
	}
	p.last = next
}

// startPortStatusLog writes the MySQL port's status line every interval.
func startPortStatusLog(ctx context.Context, numbers func() console.RouterNumbers, nameOf func(string) string) {
	every := statusLogInterval()
	p := &portStatusLogger{numbers: numbers, nameOf: nameOf, every: every, last: map[string]portCounts{}, log: slog.Info}
	go everyStatusInterval(ctx, every, p.write)
}

// ── capture ───────────────────────────────────────────────────────────────

// captureCounts is one reading of one source's capture, from the metrics the
// stream already keeps (bintrail_stream_*).
type captureCounts struct {
	recorded, errors uint64
	behind           float64 // seconds from the source's commit to queryable, at the last save
	lastSave         float64 // unix time of the last batch saved; 0 = none yet
	hasBehind        bool
}

// readCaptureCounts reads every source's counts from a metrics registry.
func readCaptureCounts(g prometheus.Gatherer) (map[string]captureCounts, error) {
	families, err := g.Gather()
	if err != nil && len(families) == 0 {
		return nil, err
	}
	out := map[string]captureCounts{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			source := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "source" {
					source = l.GetValue()
				}
			}
			if source == "" {
				continue
			}
			c := out[source]
			switch f.GetName() {
			case "bintrail_stream_events_indexed_total":
				c.recorded += uint64(m.GetCounter().GetValue())
			case "bintrail_stream_errors_total":
				c.errors += uint64(m.GetCounter().GetValue())
			// Commit-side: from the source's commit to the change being
			// queryable in the index. The receive-side gauge beside it can
			// read "caught up" while changes still wait in a batch.
			case "bintrail_stream_availability_lag_seconds":
				c.behind, c.hasBehind = m.GetGauge().GetValue(), true
			case "bintrail_stream_last_flush_timestamp_seconds":
				c.lastSave = m.GetGauge().GetValue()
			default:
				continue
			}
			out[source] = c
		}
	}
	return out, nil
}

// captureStatusAttrs is one source's line. since_last_save_s is the one to
// watch: behind_s is the stream's own gauge, which stops moving when the
// stream does, while the time since the last save keeps growing.
func captureStatusAttrs(name string, every time.Duration, now, before captureCounts, at time.Time) []any {
	attrs := []any{
		"service", "capture", "server", name, "interval", every.String(),
		"changes", since(now.recorded, before.recorded),
		"errors", since(now.errors, before.errors),
	}
	if now.hasBehind {
		attrs = append(attrs, "behind_s", int64(math.Round(now.behind)))
	}
	if now.lastSave > 0 {
		attrs = append(attrs, "since_last_save_s", max(int64(0), at.Unix()-int64(now.lastSave)))
	}
	return attrs
}

type captureStatusLogger struct {
	gather prometheus.Gatherer
	nameOf func(id string) string
	every  time.Duration
	last   map[string]captureCounts
	now    func() time.Time
	log    func(msg string, attrs ...any)
}

func (c *captureStatusLogger) write() {
	counts, err := readCaptureCounts(c.gather)
	if err != nil {
		slog.Warn("the status log could not read capture's numbers this time", "error", err.Error())
		return
	}
	ids := make([]string, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		c.log(statusLogMsg, captureStatusAttrs(c.nameOf(id), c.every, counts[id], c.last[id], c.now())...)
	}
	if len(ids) == 0 {
		c.log(statusLogMsg, "service", "capture", "interval", c.every.String(), "changes", 0, "note", "no server is being recorded yet")
	}
	c.last = counts
}

// startCaptureStatusLog writes capture's status line every interval.
func startCaptureStatusLog(ctx context.Context, nameOf func(string) string) {
	every := statusLogInterval()
	c := &captureStatusLogger{gather: prometheus.DefaultGatherer, nameOf: nameOf, every: every,
		last: map[string]captureCounts{}, now: time.Now, log: slog.Info}
	go everyStatusInterval(ctx, every, c.write)
}
