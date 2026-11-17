package consoleapp

import (
	"cmp"
	"encoding/json"
	"log/slog"
	"maps"
	"math"
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
	// has no entry, unless its mode is said (PerServerMode). Never null.
	Servers map[string]pgRouterServerNumbers `json:"servers"`
	// The fresh-mode fields of the MySQL router's document, with the same
	// meaning, so the web interface reads either the same way. Fresh is
	// true when BINTRAIL_SQL_TAILS puts every server in fresh mode, and
	// SQLModeForced is "fresh" or "fast" when it decides for every server.
	// PerServerMode and FreshByDefault are true from this version on: every
	// PostgreSQL server is listed with its sql_mode.
	Fresh          *bool  `json:"fresh,omitempty"`
	SQLModeForced  string `json:"sql_mode_forced,omitempty"`
	FreshByDefault bool   `json:"fresh_by_default,omitempty"`
	PerServerMode  bool   `json:"per_server_mode,omitempty"`
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
	// ForwardingOff says why nothing is sent to this server's source though
	// it has a forwarding account (it hides tables or columns), as its last
	// statement found; absent when statements may go there.
	ForwardingOff string `json:"forwarding_off,omitempty"`
	// SQLMode is "fresh" or "fast" as this process answers the server now,
	// and SQLModeFrom where that comes from ("environment", "saved",
	// "default"), as in the MySQL router's document. Its warm-up is not
	// said: this process does not record which answers read every tail.
	SQLMode     string `json:"sql_mode,omitempty"`
	SQLModeFrom string `json:"sql_mode_from,omitempty"`
	// How old the data of the copy's last answer was (#2756), in the MySQL
	// router's fields. DataAsOf is the commit time on the source of the
	// newest captured change it read, RFC 3339 UTC, and DataAgeSeconds how
	// long before the answer that was, by this machine's clock, rounded up;
	// DataAgeMaxSeconds the worst since CountingSince. A source with no
	// write since makes it grow though nothing is missing, and a clock of
	// this machine ahead of the source's makes it larger. Absent until a
	// fresh answer: DataAgeUnknownReason says why the last answer has none.
	DataAsOf             string `json:"data_as_of,omitempty"`
	DataAgeSeconds       *int64 `json:"data_age_seconds,omitempty"`
	DataAgeMaxSeconds    *int64 `json:"data_age_max_seconds,omitempty"`
	DataAgeUnknownReason string `json:"data_age_unknown_reason,omitempty"`
}

// pgAgeSnapshotAlone is why an answer from the snapshot alone has no age.
const pgAgeSnapshotAlone = "the last answer read the snapshot alone (fast mode, or a table with no recent changes laid over it), and how far a PostgreSQL snapshot reaches is not recorded: it is as old as the snapshot's last update"

// pgModes is what the status asks the console about each server's mode.
type pgModes interface {
	SQLModeOf(id string) (mode, from string)
	PostgresServerIDs() []string
}

// pgRouteStats is the tally.
type pgRouteStats struct {
	// modes, when set, gives the document each server's mode.
	modes      pgModes
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
	barred                string
	// The copy's last answer's age (#2756): its through and when it ran,
	// or snapshotAlone; worst is the oldest since the start.
	ageRan, ageThrough time.Time
	snapshotAlone      bool
	worst              time.Duration
	anyAge             bool
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

// age records how old the data of the copy's last answer for server id was.
func (r *pgRouteStats) age(id string, a console.CopyRunAge) {
	if r == nil || (!a.Tailed && !a.Snapshot) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.perServer[id]
	if t == nil {
		t = &pgRouteTally{reasons: map[string]uint64{}, answers: map[string]*pgRouteAnswers{}}
		r.perServer[id] = t
	}
	if a.Snapshot {
		t.snapshotAlone = true
		return
	}
	t.snapshotAlone, t.ageRan, t.ageThrough = false, a.Ran, a.Through
	d := max(a.Ran.Sub(a.Through), 0)
	if !t.anyAge || d > t.worst {
		t.worst, t.anyAge = d, true
	}
}

// pgAgeSeconds is d in whole seconds, rounded up: a lag is never said
// smaller than it was.
func pgAgeSeconds(d time.Duration) *int64 {
	v := int64(math.Ceil(d.Seconds()))
	return &v
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

// setBarred keeps why nothing is sent to server id's source, "" for nothing.
func (r *pgRouteStats) setBarred(id, why string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.perServer[id]
	if t == nil {
		if why == "" {
			return
		}
		t = &pgRouteTally{reasons: map[string]uint64{}, answers: map[string]*pgRouteAnswers{}}
		r.perServer[id] = t
	}
	t.barred = why
}

// forServer is the callback a connection of server id is bound with.
func (r *pgRouteStats) forServer(id string) func(pgshim.RouteNote) {
	if r == nil {
		return nil
	}
	return func(n pgshim.RouteNote) { r.note(id, n) }
}

// doc is the status document now: the counts, then each server's mode,
// asked of the console outside the tally's lock, which every statement takes.
func (r *pgRouteStats) doc(listen string) pgRouterStatusDoc {
	d := r.counts(listen)
	if r.modes != nil {
		forced, _ := console.SQLModeSetting()
		fresh := forced == console.SQLModeForcedFresh
		d.Fresh, d.SQLModeForced, d.FreshByDefault, d.PerServerMode = &fresh, forced, true, true
		ids := r.modes.PostgresServerIDs()
		for id := range d.Servers {
			if !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		}
		for _, id := range ids {
			n, ok := d.Servers[id]
			if !ok {
				n = pgRouterServerNumbers{Reasons: map[string]uint64{}, Answers: map[string]console.RouteAnswers{}}
			}
			n.SQLMode, n.SQLModeFrom = r.modes.SQLModeOf(id)
			d.Servers[id] = n
		}
	}
	return d
}

// counts is the document's counts, under the tally's lock.
func (r *pgRouteStats) counts(listen string) pgRouterStatusDoc {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := pgRouterStatusDoc{Version: pgRouterStatusVersion, Listen: listen, ReadOnly: true,
		CountingSince: r.since.UTC().Format(time.RFC3339), StatementTable: r.statements,
		Servers: map[string]pgRouterServerNumbers{}}
	for id, t := range r.perServer {
		n := pgRouterServerNumbers{Copy: t.copy, Source: t.source, Refused: t.refused,
			Reasons: maps.Clone(t.reasons), Answers: map[string]console.RouteAnswers{}, ForwardingOff: t.barred}
		switch {
		case t.snapshotAlone:
			n.DataAgeUnknownReason = pgAgeSnapshotAlone
		case !t.ageRan.IsZero():
			n.DataAsOf = t.ageThrough.UTC().Format(time.RFC3339)
			n.DataAgeSeconds = pgAgeSeconds(max(t.ageRan.Sub(t.ageThrough), 0))
		}
		if t.anyAge {
			n.DataAgeMaxSeconds = pgAgeSeconds(t.worst)
		}
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
