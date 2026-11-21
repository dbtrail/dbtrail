package consoleapp

import (
	"context"
	"log/slog"
	"sort"
	"time"
)

// The PostgreSQL router's status log (#3021): one line a minute per
// PostgreSQL server, in the service's own log, of what the PostgreSQL port
// did in that minute.
//
//	journalctl -u bintrail-pg-router -f | grep msg=status
//	docker compose logs -f pg-router | grep msg=status
//
// It is the MySQL router's line (statuslog.go) for this process, written
// from this process's own tally (pgrouter_status.go): the two routers share
// nothing at run time. The interval, the message to grep for and the rule
// that a quiet minute still writes a line are that file's
// (BINTRAIL_STATUS_LOG_INTERVAL, "status"), called as they are.
//
// The names are the MySQL line's where they mean the same (queries, dbtrail,
// refused, errors, dbtrail_avg_ms, copy_behind_s); the source's answers are
// postgresql and postgresql_avg_ms, and the service is pg-port.

// pgPortCounts is one reading of what one server's port has done since the
// process started.
type pgPortCounts struct {
	copy, source, refused uint64
	copyErr, sourceErr    uint64
	copySecs, sourceSecs  float64
	// behind is how old the data of the copy's last answer was, nil when
	// that is not known.
	behind *int64
}

func pgPortCountsOf(n pgRouterServerNumbers) pgPortCounts {
	c := pgPortCounts{behind: n.DataAgeSeconds}
	if a, ok := n.Answers["copy"]; ok {
		c.copy, c.copyErr, c.copySecs = a.Count, a.Errors, a.Seconds
	}
	if a, ok := n.Answers["source"]; ok {
		c.source, c.sourceErr, c.sourceSecs = a.Count, a.Errors, a.Seconds
	}
	if a, ok := n.Answers["refused"]; ok {
		c.refused = a.Count
	}
	return c
}

// pgPortStatusAttrs is one server's line for the interval between two
// readings. queries is every statement a side finished or the port refused;
// dbtrail and postgresql are who answered.
func pgPortStatusAttrs(name string, every time.Duration, now, before pgPortCounts) []any {
	copyN, sourceN, refused := since(now.copy, before.copy), since(now.source, before.source), since(now.refused, before.refused)
	attrs := []any{
		"service", "pg-port", "server", name, "interval", every.String(),
		"queries", copyN + sourceN + refused,
		"dbtrail", copyN, "postgresql", sourceN, "refused", refused,
		"errors", since(now.copyErr, before.copyErr) + since(now.sourceErr, before.sourceErr),
	}
	if ms := avgMillis(sinceSecs(now.copySecs, before.copySecs), copyN); ms >= 0 {
		attrs = append(attrs, "dbtrail_avg_ms", ms)
	}
	if ms := avgMillis(sinceSecs(now.sourceSecs, before.sourceSecs), sourceN); ms >= 0 {
		attrs = append(attrs, "postgresql_avg_ms", ms)
	}
	// How old the data of the copy's last answer was. Left out when it is
	// not known (fast mode, or no answer of the copy yet), never written as 0.
	if now.behind != nil {
		attrs = append(attrs, "copy_behind_s", *now.behind)
	}
	return attrs
}

// pgPortStatusLogger writes the port's status lines from successive readings.
type pgPortStatusLogger struct {
	servers func() map[string]pgRouterServerNumbers
	nameOf  func(id string) string
	every   time.Duration
	last    map[string]pgPortCounts
	log     func(msg string, attrs ...any)
}

func (p *pgPortStatusLogger) write() {
	servers := p.servers()
	ids := make([]string, 0, len(servers))
	for id := range servers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	next := make(map[string]pgPortCounts, len(ids))
	for _, id := range ids {
		now := pgPortCountsOf(servers[id])
		next[id] = now
		p.log(statusLogMsg, pgPortStatusAttrs(p.nameOf(id), p.every, now, p.last[id])...)
	}
	if len(ids) == 0 {
		// Still a line: silence would not tell "nothing to do" from "stopped".
		p.log(statusLogMsg, "service", "pg-port", "interval", p.every.String(), "queries", 0, "note", "there is no PostgreSQL server yet")
	}
	p.last = next
}

// startPGPortStatusLog writes the PostgreSQL port's status line every
// interval. servers is the status document's servers: every PostgreSQL
// server, whether or not a statement reached it yet.
func startPGPortStatusLog(ctx context.Context, servers func() map[string]pgRouterServerNumbers, nameOf func(string) string) {
	every := statusLogInterval()
	p := &pgPortStatusLogger{servers: servers, nameOf: nameOf, every: every, last: map[string]pgPortCounts{}, log: slog.Info}
	go everyStatusInterval(ctx, every, p.write)
}
