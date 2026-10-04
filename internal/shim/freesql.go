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
// result cut at the row cap raises a warning SHOW WARNINGS returns. The
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
	Run(ctx context.Context, statement, schema string) (sqlsandbox.Result, error)
	// CopyUpdatedAt is the time of the snapshot the copy's tables answer
	// from; zero when there is none or it cannot be read. The router
	// compares it with the server's maximum copy age before it sends a
	// statement to the copy.
	CopyUpdatedAt(ctx context.Context) time.Time
}

// Router is the read-routing seam (#2038): with one bound, an ordinary
// statement is no longer run on the copy by default. Everything that is not
// a SELECT, everything inside a transaction or after a session SET, every
// vetoed construct and every statement whose plan is cheap is FORWARDED to
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
	// Close drops the upstream connection.
	Close()
}

// RouterConfig is the per-server routing policy the handler applies before
// asking the Router.
type RouterConfig struct {
	// MaxCopyAge is how old the copy's snapshot may be for a statement to
	// run there. Zero means never: every statement is forwarded.
	MaxCopyAge time.Duration
	// Observe, when set, is told every routing decision this connection
	// makes: route is "copy" or "mysql", reason one of the RouteReason*
	// constants (a closed vocabulary, so a caller can hang a metric label on
	// it). Called once per statement, whatever the statement then does on
	// the side it was sent to.
	Observe func(route RouteSide, reason RouteReason)
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

	RouteReasonNotASelect     RouteReason = "not_a_select"     // SHOW, BEGIN, COMMIT, a USE sent as a statement, …
	RouteReasonWrite          RouteReason = "write"            // INSERT/UPDATE/DELETE/DDL
	RouteReasonSessionSetting RouteReason = "session_setting"  // a SET statement
	RouteReasonInTransaction  RouteReason = "in_transaction"   // read-your-writes
	RouteReasonSettingsSet    RouteReason = "settings_set"     // a SET earlier on this connection
	RouteReasonRoutingOff     RouteReason = "routing_off"      // MaxCopyAge is zero
	RouteReasonVeto           RouteReason = "veto"             // a construct the copy answers differently
	RouteReasonExplainFailed  RouteReason = "explain_failed"   // EXPLAIN did not run (MySQL refused it)
	RouteReasonCheapPlan      RouteReason = "cheap_plan"       // below both thresholds
	RouteReasonBoundedLimit   RouteReason = "bounded_limit"    // a small LIMIT MySQL answers without reading past it
	RouteReasonCopyAgeUnknown RouteReason = "copy_age_unknown" // no snapshot time
	RouteReasonCopyTooOld     RouteReason = "copy_too_old"     // snapshot older than MaxCopyAge
	RouteReasonCopyRefused    RouteReason = "copy_refused"     // the copy errored or cut the result
	RouteReasonShowWarnings   RouteReason = "show_warnings"    // SHOW WARNINGS after a MySQL statement
	RouteReasonUpstreamLost   RouteReason = "upstream_lost"    // nobody answered: the port's connection to the source is lost
	RouteReasonExpensivePlan  RouteReason = "expensive_plan"   // the one reason a statement goes to the copy
)

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
	runCopy func(reason string) (*mysql.Result, error)
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
		decide:  func() (readrouter.Decision, error) { return h.router.Decide(ctx, qstr) },
		runCopy: func(reason string) (*mysql.Result, error) { return h.runFreeSQLRouted(currentDB, qstr, reason) },
	})
}

// route is the routing ladder, one rung per reason a statement is MySQL's;
// only an expensive SELECT on a fresh copy reaches the last one.
func (h *Handler) route(ctx context.Context, qstr string, ops routeOps) (*mysql.Result, error) {
	kind := readrouter.Classify(qstr)
	switch kind {
	case readrouter.KindSet:
		if !readrouter.HarmlessSet(qstr) {
			h.mu.Lock()
			h.routeSettingsSet = true
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
	settingsSet := h.routeSettingsSet
	h.mu.Unlock()
	switch {
	case h.router.InTransaction():
		return ops.forward(RouteReasonInTransaction, "in transaction")
	case settingsSet:
		return ops.forward(RouteReasonSettingsSet, "session settings were set on this connection")
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
	if age := time.Since(at); age > h.routerCfg.MaxCopyAge {
		return ops.forward(RouteReasonCopyTooOld, fmt.Sprintf("copy is %s old, max %s", age.Round(time.Second), h.routerCfg.MaxCopyAge))
	}
	res, err := ops.runCopy(d.Reason)
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
	h.logger.Debug("read routing: served by the copy", "reason", d.Reason)
	h.observeRoute(RouteCopy, RouteReasonExpensivePlan)
	h.mu.Lock()
	h.routeLastForwarded = false
	h.mu.Unlock()
	return res, nil
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
	return h.runFreeSQLRouted(schema, qstr, "")
}

// runFreeSQLRouted is runFreeSQL with the routing reason when there is one.
// The copy runs the statement as the client wrote it, under routing too:
// nothing is translated from MySQL's dialect, and a statement the copy
// refuses (backtick names among them) is the caller's to forward.
func (h *Handler) runFreeSQLRouted(schema, qstr, routeReason string) (*mysql.Result, error) {
	ctx, cancel := h.queryContext()
	defer cancel()
	stmt, schema := rewriteForDuckDB(qstr, schema)
	res, err := h.freeSQL.Run(ctx, stmt, schema)
	if err != nil {
		return nil, h.freeSQLError(err)
	}
	if routeReason != "" && (res.Truncated || res.TruncatedCells > 0) {
		return nil, errCopyTruncated
	}
	rs, err := freeSQLResultset(res)
	if err != nil {
		// Only a worker bug (a row with the wrong column count) gets here.
		h.logger.Error("free sql: build resultset", "err", err)
		return nil, fmt.Errorf("free sql: build resultset: %w", err)
	}
	var warnings []string
	if res.Truncated {
		warnings = append(warnings, fmt.Sprintf("the result was cut at %d rows, this server's cap; add a LIMIT or narrow the query", len(res.Rows)))
	}
	if res.TruncatedCells > 0 {
		warnings = append(warnings, fmt.Sprintf("%d cell(s) longer than this server's cap were cut; each ends with a marker", res.TruncatedCells))
	}
	h.setWarnings(warnings)
	h.recordFreeSQL(qstr, schema, res, routeReason)
	return &mysql.Result{Status: mysql.SERVER_STATUS_AUTOCOMMIT, Resultset: rs}, nil
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
	h.mu.Unlock()
	if ws, ok := h.conn.(warningsSetter); ok {
		ws.SetWarnings(uint16(len(msgs)))
	}
}

// showWarnings answers SHOW WARNINGS the way MySQL does: Level, Code,
// Message rows, none when the last statement raised nothing. Only a
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
func freeSQLResultset(res sqlsandbox.Result) (*mysql.Resultset, error) {
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
			out[j] = freeSQLCell(cell, typ)
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
// as; a nested value (LIST, STRUCT, MAP, JSON) is its JSON text; a boolean
// is 1 or 0, as MySQL's own BOOLEAN reads back.
func freeSQLCell(v any, duckType string) any {
	switch x := v.(type) {
	case nil:
		return nil
	case json.Number:
		// The worker's own rendering, every digit kept: re-rendering through
		// a float would turn a DECIMAL's 12.50 into 12.5.
		return []byte(x.String())
	case bool:
		if x {
			return []byte("1")
		}
		return []byte("0")
	case string:
		if isTimestampType(duckType) {
			if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
				return []byte(t.UTC().Format("2006-01-02 15:04:05.999999"))
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
