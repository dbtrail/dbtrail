package consoleapp

import (
	"cmp"
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// The PostgreSQL port's status (#2770): which side answered each statement,
// and why, per server since the process started, served as JSON on
// --metrics-addr at /status like the MySQL router's. Its own tally, not the
// console's: that one counts the MySQL port's sides, and this process shares
// nothing with it at run time. With --statement-table each server also
// carries a row per statement shape and side (pgshim.Fingerprint: values
// blanked, table and column names kept).

// pgRouterStatusVersion is the shape of pgRouterStatusDoc.
const pgRouterStatusVersion = 1

// pgRouterStatusDoc is the document /status serves.
type pgRouterStatusDoc struct {
	Version int `json:"version"`
	// Listen is the PostgreSQL port's address as configured.
	Listen string `json:"listen"`
	// ReadOnly is always true: nothing that writes is sent to a source.
	ReadOnly bool `json:"read_only"`
	// CountingSince is when the counts started (this process's start), RFC
	// 3339 UTC.
	CountingSince string `json:"counting_since"`
	// StatementTable: each server carries Statements.
	StatementTable bool `json:"statement_table,omitempty"`
	// Servers is keyed by registry id; a server no statement reached yet
	// has no entry. Never null.
	Servers map[string]pgRouterServerNumbers `json:"servers"`
}

// pgRouterServerNumbers is one server's decisions.
type pgRouterServerNumbers struct {
	Copy    uint64 `json:"copy"`
	Source  uint64 `json:"source"`
	Refused uint64 `json:"refused"`
	// Reasons uses pgshim's Reason* names, which are the MySQL port's
	// where they mean the same. Never null.
	Reasons map[string]uint64 `json:"reasons"`
	// Answers is what each side ("copy", "source", "refused") finished:
	// counts, errors and times.
	Answers map[string]console.RouteAnswers `json:"answers"`
	// Statements is the per-statement table, present only with
	// StatementTable.
	Statements *console.RouterStatementTable `json:"statements,omitempty"`
}

// pgRouteStats is the tally.
type pgRouteStats struct {
	mu         sync.Mutex
	since      time.Time
	statements bool
	perServer  map[string]*pgRouteTally
}

type pgRouteTally struct {
	copy, source, refused uint64
	reasons               map[string]uint64
	answers               map[string]*pgRouteAnswers
	rows                  map[pgShapeKey]*pgShapeRow
	notCounted            uint64
}

type pgRouteAnswers struct {
	count, failed  uint64
	total, slowest time.Duration
}

func (a *pgRouteAnswers) add(took time.Duration, failed bool) {
	a.count++
	if failed {
		a.failed++
	}
	a.total += took
	a.slowest = max(a.slowest, took)
}

func (a *pgRouteAnswers) dto() console.RouteAnswers {
	return console.RouteAnswers{Count: a.count, Errors: a.failed, Seconds: a.total.Seconds(), SlowestSeconds: a.slowest.Seconds()}
}

type pgShapeKey struct{ shape, side string }

type pgShapeRow struct {
	pgRouteAnswers
	reasons map[string]uint64
}

func newPGRouteStats(now time.Time, statements bool) *pgRouteStats {
	return &pgRouteStats{since: now, statements: statements, perServer: map[string]*pgRouteTally{}}
}

// pgShape is the row a statement lands on, cut to console.StatementMaxBytes;
// "" for none.
func pgShape(stmt string) string {
	shape := pgshim.Fingerprint(stmt)
	if len(shape) <= console.StatementMaxBytes {
		return shape
	}
	cut := console.StatementMaxBytes
	for cut > 0 && !utf8.RuneStart(shape[cut]) {
		cut--
	}
	return shape[:cut] + "…"
}

// note tallies one statement of server id.
func (r *pgRouteStats) note(id string, n pgshim.RouteNote) {
	shape := ""
	if r.statements {
		// Worked out before the lock is taken.
		shape = pgShape(n.Statement)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.perServer[id]
	if t == nil {
		t = &pgRouteTally{reasons: map[string]uint64{}, answers: map[string]*pgRouteAnswers{}}
		r.perServer[id] = t
	}
	switch n.Side {
	case pgshim.RouteCopy:
		t.copy++
	case pgshim.RouteSource:
		t.source++
	case pgshim.RouteRefused:
		t.refused++
	}
	t.reasons[n.Reason]++
	a := t.answers[n.Side]
	if a == nil {
		a = &pgRouteAnswers{}
		t.answers[n.Side] = a
	}
	a.add(n.Took, n.Failed)
	if !r.statements {
		return
	}
	k := pgShapeKey{shape, n.Side}
	row := t.rows[k]
	if row == nil {
		if shape == "" || len(t.rows) >= console.StatementTableLimit {
			t.notCounted++
			return
		}
		if t.rows == nil {
			t.rows = map[pgShapeKey]*pgShapeRow{}
		}
		row = &pgShapeRow{reasons: map[string]uint64{}}
		t.rows[k] = row
	}
	row.add(n.Took, n.Failed)
	row.reasons[n.Reason]++
}

// forServer is the callback a connection of server id is bound with.
func (r *pgRouteStats) forServer(id string) func(pgshim.RouteNote) {
	if r == nil {
		return nil
	}
	return func(n pgshim.RouteNote) { r.note(id, n) }
}

// doc is the status document now.
func (r *pgRouteStats) doc(listen string) pgRouterStatusDoc {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := pgRouterStatusDoc{Version: pgRouterStatusVersion, Listen: listen, ReadOnly: true,
		CountingSince: r.since.UTC().Format(time.RFC3339), StatementTable: r.statements,
		Servers: map[string]pgRouterServerNumbers{}}
	for id, t := range r.perServer {
		n := pgRouterServerNumbers{Copy: t.copy, Source: t.source, Refused: t.refused,
			Reasons: maps.Clone(t.reasons), Answers: map[string]console.RouteAnswers{}}
		for side, a := range t.answers {
			n.Answers[side] = a.dto()
		}
		if r.statements {
			tab := &console.RouterStatementTable{Rows: []console.RouterStatementRow{}, NotCounted: t.notCounted, Limit: console.StatementTableLimit}
			for k, row := range t.rows {
				tab.Rows = append(tab.Rows, console.RouterStatementRow{Statement: k.shape, Side: k.side, RouteAnswers: row.dto(), Reasons: maps.Clone(row.reasons)})
			}
			slices.SortFunc(tab.Rows, func(x, y console.RouterStatementRow) int {
				return cmp.Or(cmp.Compare(y.Seconds, x.Seconds), cmp.Compare(y.Count, x.Count),
					cmp.Compare(x.Statement, y.Statement), cmp.Compare(x.Side, y.Side))
			})
			n.Statements = tab
		}
		d.Servers[id] = n
	}
	return d
}

// pgRouterStatusHandler serves the document.
func pgRouterStatusHandler(stats *pgRouteStats, listen string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(stats.doc(listen)); err != nil {
			slog.Warn("PostgreSQL router: the status document could not be written", "error", err)
		}
	})
}
