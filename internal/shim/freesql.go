package shim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// Free SQL on the copy, over the MySQL protocol.
//
// Before this, a connection to the shim answered ONLY the four time-travel
// shapes (AS OF on a table or a row, _diff between two instants) and every
// other statement got ER_NOT_SUPPORTED_YET (1235). With a FreeSQL bound,
// an ordinary statement that is not time travel is handed to it instead:
// the embedded port binds the console's own SQL-on-the-copy executor (the
// one behind POST /api/sql), so an analyst with nothing but a mysql client
// runs joins and aggregations over the Parquet copy — same locked DuckDB
// child process, same views, same caps.
//
// What the client gets is DuckDB SQL over a MySQL wire: the SQL dialect is
// DuckDB's (the one the console's SQL card takes), not MySQL's. The wire
// does three MySQL-shaped things on top: USE selects the schema unqualified
// names resolve in (sqlsandbox.Job.Schema), SHOW DATABASES / SHOW COLUMNS
// are rewritten to their DuckDB equivalents (rewriteForDuckDB), and a
// result with more rows than the row cap is an error (1104, rowCapError)
// unless the connection asked for the cut with SET sql_select_limit. The
// standalone `bintrail shim` binds nothing and keeps refusing as before.
//
// The order in HandleQuery is load-bearing: time travel first (a parse
// error on a virtual schema stays 1064), then handshake noise (so a
// client's `select @@version_comment limit 1` never reaches DuckDB, which
// would fail it), then free SQL.

// FreeSQL runs one ordinary read-only statement on the server's Parquet
// copy. schema is where unqualified names resolve, "" for the default. The
// error must be one the client can be shown: the executor replaces a
// worker failure, whose text can carry host paths, before it gets here
// (freeSQLError does so again, as a belt).
type FreeSQL interface {
	// sess is what the connection set for itself (#2035): the statement
	// runs under that time zone and select limit, or fails; it never runs
	// under other ones.
	Run(ctx context.Context, statement, schema string, sess sqlsandbox.Session) (sqlsandbox.Result, error)
	// CopyUpdatedAt is the time of the snapshot the copy's tables answer
	// from; zero when there is none or it cannot be read. The router
	// compares it with the server's maximum copy age before it sends a
	// statement to the copy.
	CopyUpdatedAt(ctx context.Context) time.Time
}

// Router is the read-routing seam (#2038): with one bound, an ordinary
// statement is no longer run on the copy by default. Everything that is not
// a SELECT, everything inside a transaction, every vetoed construct and every
// statement whose plan is cheap is FORWARDED to
// MySQL and answered with MySQL's own result; only a SELECT whose plan is
// expensive runs on the copy, and when the copy refuses it MySQL runs it
// instead. The implementation (internal/readrouter) holds one upstream
// MySQL connection per client connection, so transactions and USE behave as
// on MySQL itself, and once that connection is lost it says so on every
// statement (readrouter.CodeUpstreamLost) rather than reconnect behind the
// client's back.
type Router interface {
	// Decide says whether the copy should take the statement (EXPLAIN on
	// the source, except for the one shape decided on its text). An error
	// means "could not decide": the statement is forwarded.
	Decide(ctx context.Context, statement string) (readrouter.Decision, error)
	// Forward runs the statement on the source. A resultset is streamed
	// through sink and the returned Result is its tail (go-mysql writes the
	// trailing EOF from it); an OK packet comes back whole.
	Forward(ctx context.Context, statement string, sink readrouter.RowSink) (*mysql.Result, error)
	// Prepare prepares the statement on the source, which then binds its
	// arguments itself on every execution.
	Prepare(ctx context.Context, statement string) (readrouter.Stmt, error)
	// UseDB selects the database on the upstream connection.
	UseDB(ctx context.Context, db string) error
	// InTransaction reports whether the upstream connection is inside an
	// explicit transaction (from MySQL's status flags).
	InTransaction() bool
	// Status is the state of the session on the source in MySQL's status
	// flags, as the source last reported it; known is false while there is
	// no session (the connection not opened yet, or lost). It is what the
	// port tells the client about its session, whoever answered the
	// statement (see SessionStatus).
	Status() (status uint16, known bool)
	// Ping asks the source whether the session this connection holds is
	// still there, which also refreshes what Status reports. It never opens
	// a connection: with none opened yet it returns nil and sends nothing.
	Ping(ctx context.Context) error
	// Lost is the error every command gets once a session this connection
	// had on the source is gone (readrouter.CodeUpstreamLost), nil while it
	// is not, and nil for a source that never let the connection in.
	Lost() error
	// Close drops the upstream connection.
	Close()
}

// RouterConfig is the per-server routing policy the handler applies before
// asking the Router.
type RouterConfig struct {
	// MaxCopyAge is how old the copy's snapshot may be for a statement to
	// run there. Zero means never: every statement is forwarded. Past it the
	// copy is asked for an answer only over tables unchanged since their
	// snapshot, and MaxCopyAge is then how long ago capture must be known to
	// have been complete (#2085).
	MaxCopyAge time.Duration
	// Observe, when set, is told every routing decision this connection
	// makes: route is "copy", "mysql" or "refused", reason one of the RouteReason*
	// constants (a closed vocabulary, so a caller can hang a metric label on
	// it). Called once per statement, whatever the statement then does on
	// the side it was sent to.
	Observe func(route RouteSide, reason RouteReason)
	// ReadOnly refuses every statement that is not a read or the session and
	// transaction control around reads (readrouter.ReadOnlyRefusal), before
	// anything is asked of the Router: a refused statement never reaches the
	// source, not even as an EXPLAIN or a PREPARE (#2079).
	ReadOnly bool
}

// RouteSide is who answered a routed statement; RouteReason why. Both are
// typed so a free-text detail (an error message, a host name) cannot be
// passed where a bounded metric label is expected.
type (
	RouteSide   string
	RouteReason string
)

// The routing decision vocabulary RouterConfig.Observe speaks. One constant
// per rung of routeStatement's ladder, plus SHOW WARNINGS and USE: a counter
// keyed on these answers "who answered, and why MySQL" without a label per
// statement.
const (
	RouteCopy  RouteSide = "copy"
	RouteMySQL RouteSide = "mysql"
	// RouteRefused: nobody ran the statement, the port refused it.
	RouteRefused RouteSide = "refused"

	RouteReasonNotASelect     RouteReason = "not_a_select"     // SHOW, BEGIN, COMMIT, a USE sent as a statement, …
	RouteReasonWrite          RouteReason = "write"            // INSERT/UPDATE/DELETE/DDL
	RouteReasonSessionSetting RouteReason = "session_setting"  // a SET statement
	RouteReasonInTransaction  RouteReason = "in_transaction"   // read-your-writes
	RouteReasonSessionDiffers RouteReason = "session_differs"  // the source's session holds a setting the copy does not reproduce
	RouteReasonRoutingOff     RouteReason = "routing_off"      // MaxCopyAge is zero
	RouteReasonVeto           RouteReason = "veto"             // a construct the copy answers differently
	RouteReasonExplainFailed  RouteReason = "explain_failed"   // EXPLAIN did not run (MySQL refused it)
	RouteReasonCheapPlan      RouteReason = "cheap_plan"       // below both thresholds
	RouteReasonBoundedLimit   RouteReason = "bounded_limit"    // a small LIMIT MySQL answers without reading past it
	RouteReasonCopyAgeUnknown RouteReason = "copy_age_unknown" // no snapshot time
	RouteReasonCopyTooOld     RouteReason = "copy_too_old"     // snapshot older than MaxCopyAge, and its tables not vouched for as unchanged (#2085)
	RouteReasonCopyRefused    RouteReason = "copy_refused"     // the copy errored or cut the result
	RouteReasonShowWarnings   RouteReason = "show_warnings"    // SHOW WARNINGS after a MySQL statement
	RouteReasonUpstreamLost   RouteReason = "upstream_lost"    // nobody answered: the port's connection to the source is lost
	RouteReasonExpensivePlan  RouteReason = "expensive_plan"   // the copy answered, its snapshot within MaxCopyAge (past it: tables_unchanged)
	RouteReasonReadOnly       RouteReason = "read_only"        // refused: not a read, and the port is read-only
)

// RouteReasonPinned: a CREATE TEMPORARY TABLE, LOCK TABLES or PREPARE ran
// earlier on this connection, and nothing on it goes to the copy any more.
const RouteReasonPinned RouteReason = "connection_pinned"

// RouteReasonCopyColumnsDiffer: the copy declined a star or a NATURAL JOIN
// over a table whose columns there are not MySQL's (#2111), or a statement
// that names a column the copy does not hold (#2123), or one that uses a
// date, time or year column where the copy's type for it answers another
// way (#2133). A decision, not a fault, which is why it is not counted
// under RouteReasonCopyRefused.
const RouteReasonCopyColumnsDiffer RouteReason = "copy_columns_differ"

// RouteReasonTablesUnchanged is the copy answering an expensive statement
// although its snapshot is older than MaxCopyAge, because every table the
// statement reads has had no change since its snapshot and capture is known
// to have been complete within MaxCopyAge (#2085). Apart from
// expensive_plan, so the two can be told apart in the tally.
const RouteReasonTablesUnchanged RouteReason = "tables_unchanged"

// observeRoute reports one decision to the bound observer, if any.
func (h *Handler) observeRoute(route RouteSide, reason RouteReason) {
	if h.routerCfg.Observe != nil {
		h.routerCfg.Observe(route, reason)
	}
}

// BindRouter turns this connection into a routing one. Call once after the
// handshake, with a FreeSQL already bound; r must be non-nil.
func (h *Handler) BindRouter(r Router, cfg RouterConfig) {
	h.router = r
	h.routerCfg = cfg
}

// Close releases what a connection holds: today, the router's upstream
// connection. Safe to call on a handler that bound nothing.
func (h *Handler) Close() {
	if h.router != nil {
		h.router.Close()
	}
}

// errCopyTruncated: the copy answered, but cut the result at the port's row
// or cell cap. Under routing that is a refusal, not an answer: MySQL would
// have returned every row, and a client that does not read the warning
// count would take the cut result for the whole one.
var errCopyTruncated = errors.New("the copy's result exceeded the port's row or cell cap")

// routeOps is how one statement is run on each side of the routing ladder.
// A text statement and a prepared one differ only in these: the ladder
// itself (route) is one.
type routeOps struct {
	// forward runs the statement on the source and observes the decision.
	forward func(reason RouteReason, detail string) (*mysql.Result, error)
	// decide asks the source for the plan and applies the policy.
	decide func() (readrouter.Decision, error)
	// runCopy runs the statement on the copy; reason is the decision's.
	// unchangedWithin, when not zero, asks the copy to answer only if the
	// tables the statement reads are unchanged since their snapshot
	// (sqlsandbox.Session.UnchangedWithin, #2085).
	runCopy func(reason string, unchangedWithin time.Duration) (*mysql.Result, error)
	// extraVeto, when set, names one more reason to keep the statement on
	// MySQL (checked after the statement's own vetoes), or "".
	extraVeto func() string
}

// routeStatement is HandleQuery's path with a Router bound: the time-travel
// shapes were already served, so what arrives here is MySQL traffic.
func (h *Handler) routeStatement(currentDB, qstr string) (*mysql.Result, error) {
	ctx, cancel := h.queryContext()
	defer cancel()
	return h.route(ctx, qstr, routeOps{
		forward: func(reason RouteReason, detail string) (*mysql.Result, error) {
			return h.forward(ctx, qstr, reason, detail)
		},
		decide: func() (readrouter.Decision, error) { return h.router.Decide(ctx, qstr) },
		runCopy: func(reason string, unchangedWithin time.Duration) (*mysql.Result, error) {
			text, err := copyText(qstr)
			if err != nil {
				return nil, err
			}
			return h.runFreeSQLRouted(currentDB, text, readrouter.ShapeOf(qstr), reason, unchangedWithin)
		},
	})
}

// route is the routing ladder, one rung per reason a statement is MySQL's;
// only an expensive SELECT on a fresh copy reaches the last one.
func (h *Handler) route(ctx context.Context, qstr string, ops routeOps) (*mysql.Result, error) {
	if err := h.readOnlyRefusal(qstr); err != nil {
		return nil, err
	}
	// Whatever is forwarded and is not positively a plain read may change
	// the source's session: the port stops assuming it knows it
	// (routedsession.go). Marked after the statement ran, whatever it
	// answered: a failed CALL may have set half of what it meant to.
	if !readrouter.PlainRead(qstr) {
		forward := ops.forward
		ops.forward = func(reason RouteReason, detail string) (*mysql.Result, error) {
			res, err := forward(reason, detail)
			h.markSessionUnknown()
			return res, err
		}
	}
	kind := readrouter.Classify(qstr)
	switch kind {
	case readrouter.KindSet:
		// A SET is MySQL's and nothing more: what it did to the session is
		// read back when a statement next heads for the copy. The other
		// statements of this kind (CREATE TEMPORARY TABLE, LOCK TABLES,
		// PREPARE, and anything opening with an executable comment) change
		// what later statements mean in ways no read-back shows: they keep
		// the connection on MySQL for good.
		if readrouter.LeadingKeyword(qstr) != "SET" {
			h.mu.Lock()
			h.routePinned = true
			h.mu.Unlock()
		}
		return ops.forward(RouteReasonSessionSetting, "session setting")
	case readrouter.KindWrite:
		// A write reaches the source with the registry's account, on the
		// strength of the console token. Named in the log at info: the
		// audit trail covers historical reads, and this is neither.
		h.logger.Info("read routing: write forwarded to mysql", "statement", readrouter.LeadingKeyword(qstr))
		return ops.forward(RouteReasonWrite, "write")
	case readrouter.KindTxnBegin, readrouter.KindTxnEnd, readrouter.KindOther:
		return ops.forward(RouteReasonNotASelect, "not a select")
	}
	h.mu.Lock()
	pinned := h.routePinned
	h.mu.Unlock()
	switch {
	case h.router.InTransaction():
		return ops.forward(RouteReasonInTransaction, "in transaction")
	case pinned:
		return ops.forward(RouteReasonPinned, "a temporary table, a table lock or a PREPARE earlier on this connection")
	case h.routerCfg.MaxCopyAge <= 0:
		return ops.forward(RouteReasonRoutingOff, "routing to the copy is off (no max copy age)")
	}
	if v := readrouter.Veto(qstr); v != "" {
		return ops.forward(RouteReasonVeto, "veto: "+v)
	}
	if ops.extraVeto != nil {
		if v := ops.extraVeto(); v != "" {
			return ops.forward(RouteReasonVeto, "veto: "+v)
		}
	}
	// The plan first, the copy's freshness second: freshness costs a
	// snapshot listing (an S3 call on an S3 copy), and the cheap reads
	// this exists to keep fast must not pay it.
	d, err := ops.decide()
	if err != nil {
		h.routeWarn("explain", "read routing: could not explain, statement forwarded", err)
		return ops.forward(RouteReasonExplainFailed, "could not explain: "+err.Error())
	}
	if !d.ToCopy {
		reason := RouteReasonCheapPlan
		if d.Rule == readrouter.RuleBoundedLimit {
			reason = RouteReasonBoundedLimit
		}
		return ops.forward(reason, d.Reason)
	}
	at := h.freeSQL.CopyUpdatedAt(ctx)
	if at.IsZero() {
		h.routeWarn("age", "read routing: copy age unknown, expensive statement forwarded", nil)
		return ops.forward(RouteReasonCopyAgeUnknown, "copy age unknown")
	}
	// A snapshot older than the limit is not the end (#2085): a table with
	// no change since its snapshot reads the same on the copy as on MySQL,
	// whatever the snapshot's age. The copy is asked for an answer only over
	// such tables, and only if capture is known to have read everything the
	// source had written at some moment within the limit, so the answer is
	// never older than the limit allows. It decides from the tables the
	// statement names, which only it knows; when it cannot say so, the
	// statement is MySQL's as before.
	age := time.Since(at)
	var unchangedWithin time.Duration
	tooOld := fmt.Sprintf("copy is %s old, max %s", age.Round(time.Second), h.routerCfg.MaxCopyAge)
	if age > h.routerCfg.MaxCopyAge {
		unchangedWithin = h.routerCfg.MaxCopyAge
	}
	// Last, the source's session: the copy answers only under one it
	// reproduces. Asked here and not earlier, so only a statement that
	// would otherwise go to the copy pays the read-back.
	if why := h.sessionKeepsCopyFromAnswering(ctx); why != "" {
		return ops.forward(RouteReasonSessionDiffers, "session differs: "+why)
	}
	res, err := ops.runCopy(d.Reason, unchangedWithin)
	var changed *sqlsandbox.MayHaveChangedError
	if errors.As(err, &changed) {
		// Not a fault and not a warning: the rule the port had before it
		// asked, under the reason it always had.
		return ops.forward(RouteReasonCopyTooOld, tooOld+"; "+changed.Reason)
	}
	var differ *sqlsandbox.ColumnsDifferError
	if errors.As(err, &differ) {
		// The copy works; it declined this statement because its answer would
		// not have MySQL's columns (#2111), one of its names could mean
		// something else there (#2123), or a date, time or year column
		// would be read another way (#2133). Its own reason and its own
		// warning, so it does not read as a fault nor use up a fault's.
		h.routeWarn("columns", "read routing: the copy's columns are not MySQL's for a statement (a star, a NATURAL JOIN, the name of a column the copy does not hold, or a date, time or year column it reads another way), forwarded to mysql", err)
		return ops.forward(RouteReasonCopyColumnsDiffer, "copy's columns differ: "+differ.Reason)
	}
	if err != nil {
		// The slow path is always right: whatever the copy could not do
		// (a construct DuckDB lacks, a missing table, busy, a timeout, a
		// result over the cap), MySQL does.
		h.routeWarn("copy", "read routing: copy refused an expensive statement, forwarded to mysql", err)
		return ops.forward(RouteReasonCopyRefused, "copy refused: "+shortErr(err))
	}
	// The copy's side of the trace: forwarded statements log their reason,
	// so a copy-served one must too, or the log reads as if nothing ever
	// reached the copy.
	if unchangedWithin > 0 {
		// Its own reason: the copy answering past the limit has to show in
		// the tally, or a snapshot that is never refreshed reads as fresh.
		h.logger.Debug("read routing: served by the copy", "reason", d.Reason+"; "+tooOld+", and the statement's tables are unchanged since their snapshot")
		h.observeRoute(RouteCopy, RouteReasonTablesUnchanged)
	} else {
		h.logger.Debug("read routing: served by the copy", "reason", d.Reason)
		h.observeRoute(RouteCopy, RouteReasonExpensivePlan)
	}
	h.mu.Lock()
	h.routeLastForwarded = false
	h.mu.Unlock()
	return res, nil
}

// readOnlyRefusal is the read-only gate (RouterConfig.ReadOnly): nil when the
// mode is off or the statement is a read, else the error the client gets. It
// is called at the two places a statement's text can reach the Router (route,
// for a text statement and for each execution of a prepared one, and
// prepareRouted), BEFORE any of the Router's methods. The error is MySQL's
// own code for "an option of this server prevents the statement" (1290) and
// names the flag, so the reader knows what refused it and how to change it.
func (h *Handler) readOnlyRefusal(qstr string) error {
	if !h.routerCfg.ReadOnly {
		return nil
	}
	why := readrouter.ReadOnlyRefusal(qstr)
	if why == "" {
		return nil
	}
	// The reason alone: it is fixed text and known keywords, where the
	// statement's first word can be anything the client typed.
	h.logger.Debug("read routing: refused by read-only mode", "reason", why)
	h.observeRoute(RouteRefused, RouteReasonReadOnly)
	msg := "this port is read-only (--route-read-only) and refused the statement: " + why + ". Nothing was sent to the source."
	h.mu.Lock()
	h.routeRefusal = msg
	h.mu.Unlock()
	return mysql.NewError(mysql.ER_OPTION_PREVENTS_STATEMENT, msg)
}

// clearRefusal forgets the last read-only refusal: the statement now
// starting replaces it as "the last statement".
func (h *Handler) clearRefusal() {
	h.mu.Lock()
	h.routeRefusal = ""
	h.mu.Unlock()
}

// refusalDiagnostics is SHOW WARNINGS after a read-only refusal: one row,
// the way MySQL shows the error of the statement before.
func refusalDiagnostics(msg string) (*mysql.Result, error) {
	rs, err := mysql.BuildSimpleTextResultset([]string{"Level", "Code", "Message"},
		[][]any{{"Error", int64(mysql.ER_OPTION_PREVENTS_STATEMENT), msg}})
	if err != nil {
		return nil, fmt.Errorf("show warnings: %w", err)
	}
	return &mysql.Result{Status: mysql.SERVER_STATUS_AUTOCOMMIT, Resultset: rs}, nil
}

// routeWarn logs a fallback that hides an operational problem (the feature
// is on, yet the copy never answers) once per key per connection, so a
// broken copy path is visible without debug logging and without a line per
// statement.
func (h *Handler) routeWarn(key, msg string, err error) {
	h.mu.Lock()
	if h.routeWarned == nil {
		h.routeWarned = map[string]bool{}
	}
	seen := h.routeWarned[key]
	h.routeWarned[key] = true
	h.mu.Unlock()
	if seen {
		return
	}
	if err != nil {
		h.logger.Warn(msg, "err", err, "note", "logged once per connection")
		return
	}
	h.logger.Warn(msg, "note", "logged once per connection")
}

// forward sends the statement to MySQL and hands its result back as is. A
// resultset is streamed to the client as it arrives (the handler's conn is
// the sink), so a forwarded SELECT * never sits whole in the capture
// process. The decision is observed under its closed-vocabulary reason
// (RouteReason*) and the detail is logged at debug: it is the trace a
// routing decision leaves.
func (h *Handler) forward(ctx context.Context, qstr string, reason RouteReason, detail string) (*mysql.Result, error) {
	return h.forwardWith(reason, detail, false, func(sink readrouter.RowSink) (*mysql.Result, error) {
		return h.router.Forward(ctx, qstr, sink)
	})
}

// forwardWith is forward for any way of running a statement on the source:
// run executes it, streaming rows through the sink it is given. binary says
// the client is reading a prepared statement's answer, so the rows go out in
// the binary encoding.
func (h *Handler) forwardWith(reason RouteReason, detail string, binary bool, run func(readrouter.RowSink) (*mysql.Result, error)) (*mysql.Result, error) {
	h.logger.Debug("read routing: forwarded to mysql", "reason", detail)
	h.mu.Lock()
	h.routeLastForwarded = true
	h.mu.Unlock()
	var sink readrouter.RowSink
	var buf *readrouter.BufferSink
	if h.conn != nil {
		w := newStreamWriterFields(h.conn, nil)
		w.status = h.SessionStatus()
		w.binary = binary
		sink = w
	} else {
		buf = &readrouter.BufferSink{}
		sink = buf
	}
	res, err := run(sink)
	// Observed AFTER the forward, so a source nobody reached is counted as
	// such: with the upstream lost (or never opened: wrong credentials, the
	// source down) every statement fails with 2006, and tallying those under
	// the rung's own reason would read as "MySQL answered N" while nothing
	// did. Still exactly one observation per statement.
	if err != nil && readrouter.IsLost(err) {
		reason = RouteReasonUpstreamLost
	}
	h.observeRoute(RouteMySQL, reason)
	if err != nil {
		if reason == RouteReasonUpstreamLost {
			h.routeWarn("lost", "read routing: the connection to the source was lost; this client connection answers 2006 until it reconnects", err)
		}
		return nil, err
	}
	if res != nil && res.Resultset != nil {
		// A forwarded result set carries MySQL's warning count; put it on
		// the connection like a copy result does (see setWarnings), before
		// go-mysql writes the trailing EOF from this result.
		if ws, ok := h.conn.(warningsSetter); ok {
			ws.SetWarnings(res.Warnings)
		}
		if buf != nil && res.Resultset.StreamingDone {
			// No connection to stream to and the router streamed through
			// the buffer: hand back a buffered resultset.
			names := make([]string, len(buf.Fields))
			for i, f := range buf.Fields {
				names[i] = string(f.Name)
			}
			rs, err := mysql.BuildSimpleTextResultset(names, buf.Rows)
			if err != nil {
				return nil, err
			}
			if binary {
				// The text builder guessed the types; the source's own
				// definitions are the ones to encode under.
				for i, f := range buf.Fields {
					rs.Fields[i].Type, rs.Fields[i].Flag, rs.Fields[i].Charset = f.Type, f.Flag, f.Charset
				}
				if rs, err = binaryResultset(rs); err != nil {
					return nil, err
				}
			}
			return &mysql.Result{Status: res.Status, Warnings: res.Warnings, Resultset: rs}, nil
		}
	}
	return res, nil
}

func shortErr(err error) string {
	var me *mysql.MyError
	if errors.As(err, &me) {
		return fmt.Sprintf("%d %s", me.Code, me.Message)
	}
	return err.Error()
}

// BindFreeSQL enables free SQL on this connection. Call once after the
// handshake, like BindActor; f must be non-nil.
func (h *Handler) BindFreeSQL(f FreeSQL) {
	h.freeSQL = f
	h.freeSQLWhyNot = ""
}

// BindFreeSQLUnavailable records why this connection has no free SQL, so
// the 1235 refusal says it instead of only "time travel only".
func (h *Handler) BindFreeSQLUnavailable(why string) {
	h.freeSQL = nil
	h.freeSQLWhyNot = why
}

// notTimeTravelError is the refusal for an ordinary statement with no
// FreeSQL bound: the pre-free-SQL 1235, plus the reason when one is known.
func (h *Handler) notTimeTravelError(qstr string) error {
	msg := fmt.Sprintf("this server only handles _flashback / _snapshot / _diff virtual-schema queries; got: %s",
		strings.TrimSpace(qstr))
	if h.freeSQLWhyNot != "" {
		msg += "; SQL on the copy is unavailable here: " + h.freeSQLWhyNot
	}
	return mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, msg)
}

// runFreeSQL serves one ordinary statement through the bound FreeSQL.
func (h *Handler) runFreeSQL(schema, qstr string) (*mysql.Result, error) {
	return h.runFreeSQLRouted(schema, qstr, nil, "", 0)
}

// copyText is the statement the routing ladder sends the copy: the client's
// text with its backtick-quoted names in double quotes (readrouter.ForCopy),
// the one thing translated from MySQL's dialect. The ladder vetoes every
// statement the rewrite refuses before it gets here; the error is for a
// caller that did not, and reads as one more refusal by the copy.
func copyText(qstr string) (string, error) {
	text, why := readrouter.ForCopy(qstr)
	if why != "" {
		return "", fmt.Errorf("not sent to the copy: %s", why)
	}
	return text, nil
}

// runFreeSQLRouted is runFreeSQL with the routing reason when there is one.
// The copy runs the statement it is given: the routing ladder hands it the
// client's text with backtick-quoted names rewritten (copyText), a client
// on the port without routing its own text. Nothing else is translated from
// MySQL's dialect, and a statement the copy refuses is the caller's to
// forward. unchangedWithin is sqlsandbox.Session.UnchangedWithin (#2085).
//
// types is sqlsandbox.Session.Types: the routing layer's reading of the
// client's own statement, nil for a port without routing.
func (h *Handler) runFreeSQLRouted(schema, qstr string, types sqlsandbox.ColumnTypes, routeReason string, unchangedWithin time.Duration) (*mysql.Result, error) {
	ctx, cancel := h.queryContext()
	defer cancel()
	stmt, schema := rewriteForDuckDB(qstr, schema)
	// What the connection set for itself (#2035). Under routing: the
	// source's session as read back just before this statement was sent
	// here (#2082, routedsession.go).
	h.mu.Lock()
	vars := h.sessVars
	h.mu.Unlock()
	sess := vars.session()
	if stmt != qstr {
		// A SHOW this port rewrote into a SELECT: sql_select_limit limits
		// SELECTs, and the client did not send one.
		sess.SelectLimit = 0
		if showDatabasesRE.MatchString(qstr) {
			// The schema list holds no time and reads no table, and it is
			// the way out of every dead end (rewriteForDuckDB): it must not
			// be refused because some table cannot be read under the
			// connection's zone.
			sess = sqlsandbox.Session{}
		}
	}
	// Under routing the client expects MySQL's answer: a star over a table
	// whose columns the copy cannot return as MySQL does is refused there,
	// and the refusal is the caller's to forward (#2111).
	// Asked of the connection, not of the reason text: a routing connection
	// only ever gets here from the ladder.
	sess.StrictStar = h.router != nil
	sess.Types = types
	sess.UnchangedWithin = unchangedWithin
	res, err := h.freeSQL.Run(ctx, stmt, schema, sess)
	var differ *sqlsandbox.ColumnsDifferError
	var changed *sqlsandbox.MayHaveChangedError
	if errors.As(err, &differ) || errors.As(err, &changed) {
		// Handed back as it is: the routing ladder tells it from a fault.
		return nil, err
	}
	if err != nil {
		return nil, h.freeSQLError(err)
	}
	if routeReason != "" && (res.Truncated || res.TruncatedCells > 0) {
		return nil, errCopyTruncated
	}
	if res.Truncated {
		// The result did not fit the row cap (#2037). It is refused, not
		// returned short: a client that does not read warnings would take the
		// first rows for the whole answer. The one cut that is NOT an error
		// never gets here: a connection that SET sql_select_limit at or under
		// the cap asked for it, and the executor does not report that cut as
		// Truncated (sqlsandbox.Session.SelectLimit).
		return nil, rowCapError(qstr, len(res.Rows), vars.selectLimit)
	}
	rs, err := freeSQLResultset(res, vars.loc)
	if err != nil {
		// Only a worker bug (a row with the wrong column count) gets here.
		h.logger.Error("free sql: build resultset", "err", err)
		return nil, fmt.Errorf("free sql: build resultset: %w", err)
	}
	var warnings []string
	if res.TruncatedCells > 0 {
		warnings = append(warnings, fmt.Sprintf("%d cell(s) longer than this server's cap were cut; each ends with a marker", res.TruncatedCells))
	}
	h.setWarnings(warnings)
	h.recordFreeSQL(qstr, schema, res, routeReason)
	return &mysql.Result{Status: mysql.SERVER_STATUS_AUTOCOMMIT, Resultset: rs}, nil
}

// rowCapError is the refusal for a result with more rows than the port's row
// cap: MySQL's own code for a SELECT it will not run to the end (1104), the
// cap, and the ways out. rowCap is the cap itself: a result cut there holds
// exactly that many rows. The ways out depend on the statement: a SELECT
// takes a LIMIT, or the connection's sql_select_limit; a listing (SHOW,
// DESCRIBE, SUMMARIZE) takes neither, so it is pointed at the catalog, which
// does. selectLimit is the connection's sql_select_limit (0 when not set),
// named when it is above the cap, because the client that set it expects it
// to have covered this.
func rowCapError(qstr string, rowCap int, selectLimit uint64) error {
	msg := fmt.Sprintf("the result has more than %d rows, the row cap for SQL on the copy on this server, and is not returned cut; ", rowCap)
	switch readrouter.LeadingKeyword(qstr) {
	case "SHOW", "DESCRIBE", "DESC", "SUMMARIZE":
		msg += fmt.Sprintf("this statement takes no LIMIT: read the same from the catalog with one "+
			"(SELECT ... FROM information_schema.tables, or information_schema.columns, WHERE ... LIMIT %d)", rowCap)
	default:
		msg += fmt.Sprintf("add a LIMIT of %d or less to the statement, or narrow it. To have a SELECT with no LIMIT of its own cut "+
			"without an error, SET sql_select_limit to %d or less on the connection", rowCap, rowCap)
		if selectLimit > uint64(rowCap) {
			msg += fmt.Sprintf(" (it is %d now, above the cap, so the cap applied)", selectLimit)
		}
	}
	return mysql.NewError(mysql.ER_TOO_BIG_SELECT, msg)
}

// warningsSetter is the part of go-mysql's server.Conn that carries the
// warning count: the EOF packet closing a resultset reads it from the
// CONNECTION (Conn.SetWarnings), never from mysql.Result.Warnings, which is
// written only into an OK packet. A connection bound by BindConn gets the
// count set here; an unbound handler (unit tests) keeps the SHOW WARNINGS
// half alone.
type warningsSetter interface{ SetWarnings(uint16) }

// setWarnings stores what SHOW WARNINGS answers until the next statement and
// puts the count on the connection, so the client's "N warning(s)" shows.
func (h *Handler) setWarnings(msgs []string) {
	h.mu.Lock()
	h.lastWarnings = msgs
	// Called when a statement starts and when the copy answered one: either
	// way the refusal of an earlier statement is no longer the last word.
	h.routeRefusal = ""
	h.mu.Unlock()
	if ws, ok := h.conn.(warningsSetter); ok {
		ws.SetWarnings(uint16(len(msgs)))
	}
}

// showWarnings answers SHOW WARNINGS the way MySQL does: Level, Code,
// Message rows, none when the last statement raised nothing. The one warning
// a copy result raises is a cell cut at the cell cap. Only a
// handler with free SQL bound answers this way; without it SHOW WARNINGS
// stays handshake noise (an empty OK), as it always was.
func (h *Handler) showWarnings() (*mysql.Result, error) {
	h.mu.Lock()
	msgs := h.lastWarnings
	h.mu.Unlock()
	cols := []string{"Level", "Code", "Message"}
	var rows [][]any
	for _, msg := range msgs {
		rows = append(rows, []any{"Warning", int64(mysql.ER_WARN_TOO_MANY_RECORDS), msg})
	}
	rs, err := mysql.BuildSimpleTextResultset(cols, rows)
	if err != nil {
		return nil, fmt.Errorf("show warnings: %w", err)
	}
	return &mysql.Result{Status: mysql.SERVER_STATUS_AUTOCOMMIT, Resultset: rs}, nil
}

var (
	useStatementRE  = regexp.MustCompile("(?is)^\\s*use\\s+(`[^`]+`|[^\\s;`]+)\\s*;?\\s*$")
	showWarningsRE  = regexp.MustCompile(`(?is)^\s*show\s+warnings\s*;?\s*$`)
	showDatabasesRE = regexp.MustCompile(`(?is)^\s*show\s+(?:databases|schemas)\s*;?\s*$`)
	// SHOW COLUMNS FROM t [FROM s] / SHOW FIELDS FROM t: the forms a MySQL
	// client sends to describe a table. DuckDB spells it DESCRIBE [s.]t.
	showColumnsRE = regexp.MustCompile(`(?is)^\s*show\s+(?:columns|fields)\s+(?:from|in)\s+(\S+)(?:\s+(?:from|in)\s+(\S+))?\s*;?\s*$`)
)

// showDatabasesSQL lists the schemas the views created, under the column
// name a MySQL client expects from SHOW DATABASES. main stays: the events
// view lives there. DuckDB's own catalog schemas do not.
const showDatabasesSQL = `SELECT DISTINCT schema_name AS "Database" FROM information_schema.schemata ` +
	`WHERE schema_name NOT IN ('information_schema', 'pg_catalog') ORDER BY 1`

// rewriteForDuckDB maps the two MySQL metadata statements DuckDB does not
// read to their DuckDB form; everything else passes through unchanged,
// SHOW TABLES and DESCRIBE included, which DuckDB reads as they are. SHOW
// DATABASES runs with NO schema: it needs none, and it must keep working
// when the connection's current database is not in the copy (a default
// seeded from the source DSN, or a typo in -D), since it is the way out.
func rewriteForDuckDB(qstr, schema string) (string, string) {
	if showDatabasesRE.MatchString(qstr) {
		return showDatabasesSQL, ""
	}
	if m := showColumnsRE.FindStringSubmatch(qstr); m != nil {
		table := strings.Trim(m[1], "`")
		schemaPart := strings.Trim(m[2], "`")
		// The dotted spelling, SHOW COLUMNS FROM shop.orders.
		if schemaPart == "" {
			if dot := strings.Index(table, "."); dot > 0 && !strings.HasPrefix(m[1], "`") {
				schemaPart, table = table[:dot], strings.Trim(table[dot+1:], "`")
			}
		}
		if schemaPart != "" {
			return "DESCRIBE " + quoteDuckIdent(schemaPart) + "." + quoteDuckIdent(table), schema
		}
		return "DESCRIBE " + quoteDuckIdent(table), schema
	}
	return qstr, schema
}

// quoteDuckIdent quotes one name the way DuckDB reads identifiers.
func quoteDuckIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// freeSQLError maps the executor's errors to MySQL wire errors. The two
// shapes a client caused get client-input codes (1064 for a statement the
// sandbox would not run, 1317 for one it stopped); the rest is 1105 with
// the message, except a worker failure, which is replaced (its text can
// carry host paths) and logged.
func (h *Handler) freeSQLError(err error) error {
	var refused *sqlsandbox.RefusedError
	var qerr *sqlsandbox.QueryError
	var timeout *sqlsandbox.TimeoutError
	var werr *sqlsandbox.WorkerError
	switch {
	case errors.As(err, &refused):
		return mysql.NewError(mysql.ER_PARSE_ERROR, refused.Reason)
	case errors.As(err, &qerr):
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, qerr.Message)
	case errors.As(err, &timeout):
		return mysql.NewError(mysql.ER_QUERY_INTERRUPTED,
			fmt.Sprintf("the query ran longer than this server's cap of %.0f s and was stopped; narrow it", timeout.Limit.Seconds()))
	case errors.Is(err, sqlsandbox.ErrBusy):
		return mysql.NewError(mysql.ER_TOO_MANY_USER_CONNECTIONS, err.Error())
	case errors.Is(err, context.DeadlineExceeded) && h.cfg.QueryTimeout > 0:
		// This connection's own cap (shorter than the sandbox's), not a cancel.
		return mysql.NewError(mysql.ER_QUERY_INTERRUPTED,
			fmt.Sprintf("the query ran longer than this connection's cap of %.0f s and was stopped; narrow it", h.cfg.QueryTimeout.Seconds()))
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return mysql.NewError(mysql.ER_QUERY_INTERRUPTED, "the query was cancelled")
	case errors.As(err, &werr):
		h.logger.Error("free sql: the SQL worker failed", "err", err)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, "the SQL worker failed before it could answer; DBTrail's log has the details")
	default:
		// The executor's own refusals (copy not local, no view, too large):
		// the message is written for the reader. Logged too, so a new error
		// shape from a future runner leaves a trace.
		h.logger.Warn("free sql: statement refused", "err", err)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, err.Error())
	}
}

// freeSQLResultset renders a sandbox result as a MySQL text resultset. The
// cells arrive JSON-shaped (nil, bool, string, json.Number, []any, map); each
// is rendered to text by its column's DuckDB type, and the column
// definitions carry the MySQL type that DuckDB type maps to, so a client
// shows numbers as numbers and dates as dates. Every cell is sent as text
// bytes: BuildSimpleTextResultset fixes a column's wire type from its first
// non-null row, and a column mixing Go types across rows is an error there.
// loc is the session's time zone, nil for UTC: an instant is printed in it.
func freeSQLResultset(res sqlsandbox.Result, loc *time.Location) (*mysql.Resultset, error) {
	names := make([]string, len(res.Columns))
	for i, c := range res.Columns {
		names[i] = c.Name
	}
	values := make([][]any, len(res.Rows))
	for i, row := range res.Rows {
		out := make([]any, len(row))
		for j, cell := range row {
			typ := ""
			if j < len(res.Columns) {
				typ = res.Columns[j].Type
			}
			out[j] = freeSQLCell(cell, typ, loc)
		}
		values[i] = out
	}
	rs, err := mysql.BuildSimpleTextResultset(names, values)
	if err != nil {
		return nil, err
	}
	for i, c := range res.Columns {
		if i >= len(rs.Fields) {
			break
		}
		applyDuckType(rs.Fields[i], c.Type)
	}
	return rs, nil
}

// freeSQLCell renders one JSON-shaped cell to the text the wire carries,
// nil staying NULL. A timestamp arrives RFC 3339 in UTC and leaves in
// MySQL's DATETIME spelling, because that is what the column is declared
// as. An INSTANT (TIMESTAMP WITH TIME ZONE: now(), a MySQL TIMESTAMP column)
// is printed in the session's zone, loc, as MySQL prints one; a zone-less
// TIMESTAMP is a wall clock and is printed as it is, whatever the zone. A
// nested value (LIST, STRUCT, MAP, JSON) is its JSON text; a boolean
// is 1 or 0, as MySQL's own BOOLEAN reads back.
func freeSQLCell(v any, duckType string, loc *time.Location) any {
	switch x := v.(type) {
	case nil:
		return nil
	case json.Number:
		// The worker's own rendering, every digit kept. (A DECIMAL is not
		// here: it arrives as text, already carrying its scale's zeros.)
		return []byte(x.String())
	case bool:
		if x {
			return []byte("1")
		}
		return []byte("0")
	case string:
		if isTimestampType(duckType) {
			if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
				t = t.UTC()
				if loc != nil && isInstantType(duckType) {
					t = t.In(loc)
				}
				return []byte(t.Format("2006-01-02 15:04:05.999999"))
			}
		}
		return []byte(x)
	case []any, map[string]any:
		b, err := json.Marshal(x)
		if err != nil {
			return []byte(fmt.Sprint(x))
		}
		return b
	default:
		return []byte(fmt.Sprint(x))
	}
}

func isTimestampType(duckType string) bool {
	return strings.HasPrefix(strings.ToUpper(duckType), "TIMESTAMP")
}

// isInstantType: DuckDB's TIMESTAMP WITH TIME ZONE, under either of the names
// the driver reports it by. Every other TIMESTAMP flavour has no zone.
func isInstantType(duckType string) bool {
	switch strings.ToUpper(strings.TrimSpace(duckType)) {
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return true
	}
	return false
}

// applyDuckType sets the MySQL column type a DuckDB type maps to. Text
// columns are declared utf8mb4, never binary: a MySQL 8 client shows a
// binary-charset string column as hex, which would make every VARCHAR
// unreadable. Numbers, dates and times keep MySQL's own convention
// (charset 63, binary flag). What has no MySQL shape (HUGEINT, INTERVAL,
// UUID, ENUM) is text; nested types are JSON.
func applyDuckType(f *mysql.Field, duckType string) {
	upper := strings.ToUpper(strings.TrimSpace(duckType))
	// A LIST or ARRAY (INTEGER[], VARCHAR[3]) is nested whatever its element.
	nested := strings.HasSuffix(upper, "]") || strings.HasPrefix(upper, "STRUCT") || strings.HasPrefix(upper, "MAP") || upper == "JSON"
	base := upper
	if i := strings.IndexAny(base, "(["); i >= 0 {
		base = strings.TrimSpace(base[:i])
	}
	numeric := func(t uint8, unsigned bool) {
		f.Type = t
		f.Charset = 63
		f.Flag |= mysql.BINARY_FLAG
		if unsigned {
			f.Flag |= mysql.UNSIGNED_FLAG
		}
	}
	text := func(t uint8) {
		f.Type = t
		f.Charset = 255 // utf8mb4
		f.Flag &^= mysql.BINARY_FLAG
	}
	switch {
	case nested:
		text(mysql.MYSQL_TYPE_JSON)
	case base == "TINYINT", base == "SMALLINT", base == "INTEGER", base == "BIGINT":
		numeric(mysql.MYSQL_TYPE_LONGLONG, false)
	case base == "UTINYINT", base == "USMALLINT", base == "UINTEGER", base == "UBIGINT":
		numeric(mysql.MYSQL_TYPE_LONGLONG, true)
	case strings.HasPrefix(base, "DECIMAL"):
		numeric(mysql.MYSQL_TYPE_NEWDECIMAL, false)
		f.Decimal = decimalScale(duckType)
	case base == "FLOAT", base == "REAL":
		numeric(mysql.MYSQL_TYPE_FLOAT, false)
	case base == "DOUBLE":
		numeric(mysql.MYSQL_TYPE_DOUBLE, false)
	case base == "BOOLEAN":
		numeric(mysql.MYSQL_TYPE_TINY, false)
	case base == "DATE":
		numeric(mysql.MYSQL_TYPE_DATE, false)
	case strings.HasPrefix(base, "TIMESTAMP"):
		numeric(mysql.MYSQL_TYPE_DATETIME, false)
		f.Decimal = 6
	case base == "TIME":
		numeric(mysql.MYSQL_TYPE_TIME, false)
	default:
		text(mysql.MYSQL_TYPE_VAR_STRING)
	}
}

// decimalScale reads the scale of DECIMAL(p,s); 0 when absent.
func decimalScale(duckType string) uint8 {
	open, comma, closeP := strings.Index(duckType, "("), strings.Index(duckType, ","), strings.Index(duckType, ")")
	if open < 0 || comma < open || closeP < comma {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(duckType[comma+1 : closeP]))
	if err != nil || n < 0 || n > 255 {
		return 0
	}
	return uint8(n)
}

// recordFreeSQL reports one served free-SQL statement on the audit seam:
// raw row data left the copy, and the statement is the reader's own text,
// which is what an auditor needs to know what was asked. Success path only,
// after the resultset is built, same contract as auditTimeTravel.
func (h *Handler) recordFreeSQL(statement, schema string, res sqlsandbox.Result, routeReason string) {
	if !ext.Auditing() {
		return
	}
	actor := h.actor
	if actor == "" {
		actor = unboundActor
	}
	detail := map[string]string{
		"sql":       statement,
		"rows":      strconv.Itoa(len(res.Rows)),
		"truncated": strconv.FormatBool(res.Truncated),
	}
	if res.TruncatedCells > 0 {
		detail["truncated_cells"] = strconv.Itoa(res.TruncatedCells)
	}
	if routeReason != "" {
		// Under read routing (#2038): the copy took this statement, and why.
		detail["route"] = "copy"
		detail["route_reason"] = routeReason
	}
	if schema != "" {
		detail["schema"] = schema
	}
	ctx := context.Background()
	if h.baseCtx != nil {
		ctx = context.WithoutCancel(h.baseCtx)
	}
	ext.Record(ctx, ext.AuditEvent{
		Surface: "shim",
		Action:  "sql.run",
		Actor:   actor,
		Detail:  detail,
	})
}
