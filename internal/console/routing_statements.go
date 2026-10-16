package console

import (
	"cmp"
	"context"
	"slices"
	"time"
	"unicode/utf8"
)

// The per-statement table (#2360): what each statement shape cost on each
// side, per server. Kept only when the router was started with it on
// (Config.StatementShape), since its rows name tables and columns and the
// router's status address asks for no password.

const (
	// StatementTableLimit is how many rows (a shape on a side) one server
	// keeps. A shape first seen past it is not a row: it is counted in
	// NotCounted, so the table never reads as complete when it is not.
	StatementTableLimit = 200
	// StatementMaxBytes is how much of a shape a row keeps. Two statements
	// alike for longer than this share a row.
	StatementMaxBytes = 1024
)

// RouterStatementTable is one server's table on the wire.
type RouterStatementTable struct {
	// Rows, most time first. Never null while the table is on.
	Rows []RouterStatementRow `json:"rows"`
	// NotCounted is the statements with no row: their shape found the
	// table full, or they were blank.
	NotCounted uint64 `json:"not_counted"`
	// Limit is StatementTableLimit, so a reader can say the table is full.
	Limit int `json:"limit"`
}

// RouterStatementRow is one statement shape on one side.
type RouterStatementRow struct {
	// Statement is readrouter.Fingerprint of what the client sent: every
	// value blanked, names kept. Cut to StatementMaxBytes, ending in "…".
	Statement string `json:"statement"`
	// Side is "copy" or "mysql": the router sends each statement to one.
	Side string `json:"side"`
	RouteAnswers
}

type statementKey struct{ shape, side string }

// statementTally is one server's table.
type statementTally struct {
	rows       map[statementKey]*routingAnswers
	notCounted uint64
}

// statementShape is the row a statement lands on, "" for none (nothing
// but white space). Worked out before the lock is taken.
func statementShape(fingerprint func(string) string, stmt string) string {
	shape := fingerprint(stmt)
	if len(shape) <= StatementMaxBytes {
		return shape
	}
	cut := StatementMaxBytes
	for cut > 0 && !utf8.RuneStart(shape[cut]) {
		cut--
	}
	return shape[:cut] + "…"
}

// answeredStatement adds one finished statement to its row. Called with
// r.mu held, side already checked.
func (t *routingTally) answeredStatement(shape, side string, took time.Duration, failed, timedOut bool) {
	if t.statements == nil {
		t.statements = &statementTally{rows: map[statementKey]*routingAnswers{}}
	}
	if shape == "" {
		// Nothing but white space: no shape to show, and still counted, so
		// the rows and NotCounted add up to the side's answers.
		t.statements.notCounted++
		return
	}
	k := statementKey{shape, side}
	a := t.statements.rows[k]
	if a == nil {
		if len(t.statements.rows) >= StatementTableLimit {
			t.statements.notCounted++
			return
		}
		a = &routingAnswers{}
		t.statements.rows[k] = a
	}
	a.add(took, failed, timedOut)
}

// statementTablesLocked copies every server's table for the wire, r.mu
// held.
func (r *routingStats) statementTablesLocked() map[string]*RouterStatementTable {
	out := map[string]*RouterStatementTable{}
	for id, t := range r.perServer {
		if t.statements == nil {
			continue
		}
		tab := &RouterStatementTable{Rows: make([]RouterStatementRow, 0, len(t.statements.rows)), NotCounted: t.statements.notCounted, Limit: StatementTableLimit}
		for k, a := range t.statements.rows {
			tab.Rows = append(tab.Rows, RouterStatementRow{Statement: k.shape, Side: k.side, RouteAnswers: a.dto()})
		}
		slices.SortFunc(tab.Rows, func(x, y RouterStatementRow) int {
			return cmp.Or(cmp.Compare(y.Seconds, x.Seconds), cmp.Compare(y.Count, x.Count),
				cmp.Compare(x.Statement, y.Statement), cmp.Compare(x.Side, y.Side))
		})
		out[id] = tab
	}
	return out
}

// fullDataAccess: this request may be shown what the router's statements
// name. Not under a data profile, the console's own or the session's: a
// row names tables and columns, and filtering rows by the tables they name
// would mean trusting a reading of the statement's text.
func (s *Server) fullDataAccess(ctx context.Context) bool {
	return !s.profileActive && !policyFrom(ctx).DataRestricted()
}

// withholdStatements takes the statement tables out of a router view the
// request may not see, and says so. Whatever the document says of itself:
// a router of another version, or another program on that address, may
// carry rows without statement_table, and they name tables all the same.
func (s *Server) withholdStatements(ctx context.Context, v *routerViewDTO) {
	if v.Status == nil || s.fullDataAccess(ctx) {
		return
	}
	withheld := v.Status.StatementTable
	for id, n := range v.Status.Servers {
		if n.Statements != nil {
			withheld = true
			n.Statements = nil
			v.Status.Servers[id] = n
		}
	}
	v.StatementsWithheld = withheld
}
