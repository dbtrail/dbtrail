package pgshim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/shim"
)

// session is the per-connection query state: the backend it writes to, the
// engine handler, and the currently-selected schema.
type session struct {
	be        *pgproto3.Backend
	h         *shim.Handler
	currentDB string
	logger    *slog.Logger
	// bound is set on a port with a login of its own (login.go): ordinary
	// statements run on the copy through it, and h is nil. ctx is that
	// connection's context, which ends when the client leaves.
	bound *Binding
	ctx   context.Context
	// tx is the transaction status ReadyForQuery reports: 'I' idle, 'T' in
	// a transaction block, 'E' in a failed one. The port keeps no
	// transaction of its own (every statement reads the copy as it is); the
	// status is what a client is told so that it moves through BEGIN,
	// COMMIT and ROLLBACK as on PostgreSQL.
	tx byte
	// ext is the extended query protocol's state (extended.go).
	ext extState
	// budget is the port's bound on the results it holds while it sends
	// them (resultbudget.go), nil for none; heldResult is what this
	// connection's last result counts in it now, before a portal takes it
	// over (portal.held).
	budget     *ResultBudget
	heldResult int64

	// set is the connection's settings now (settings.go); sessionLevel is
	// what a COMMIT keeps of them (set without its SET LOCALs), txSaved what
	// they were at BEGIN, which a ROLLBACK or a failed block goes back to,
	// and defaults what RESET goes back to: the start-up message's values.
	set, sessionLevel, txSaved, defaults settings
	// reported is each reported parameter's value as the client was last
	// told it (ParameterStatus); nil before the welcome.
	reported map[string]string
	// defaultSchema is where unqualified names resolve before a SET
	// search_path, user the login's username (session_authorization).
	defaultSchema, user string
	// cancelTarget is what a cancel request for this connection stops
	// (cancel.go); nil cancels nothing.
	cancelTarget *cancelTarget
	// database is the database the client connected to (the catalog's one
	// database), and catalogTables the copy's tables as the catalog last
	// read them, at catalogAt (catalog.go).
	database      string
	catalogTables []CatalogTable
	catalogAt     time.Time
	// up is the connection to the server's source (forward.go), nil until a
	// statement needs it; upLost says why it was lost, after which nothing
	// more is sent there.
	up     *upstream
	upLost string
	// savepoints are the savepoints of the client's block on the source,
	// oldest first (forward.go).
	savepoints []savepoint
	// fallback is why the statement the copy just failed goes to the
	// source instead (route.go), "" when it does not; set by runFree.
	fallback string
	// busyRefused is the busy reason the statement the copy just failed
	// was refused for instead (copyFailed, #3109), "" when it was not.
	busyRefused string
	// screenBusy is why the statement route just decided was not screened
	// (routeFree): the copy was too busy to list its columns.
	screenBusy string
	// catalogBusy is the busy reason the copy's list of columns could not
	// be read for at its last read (catalog), "" when it was read or failed
	// otherwise.
	catalogBusy string
	// costVerdicts are the source's plans' verdicts this connection
	// remembers, by statement shape (route_cost.go).
	costVerdicts map[string]costVerdict
	// sourceAnsweredAt is when the source last answered a statement of this
	// connection: the copy may not answer it with data older than that
	// (copyOlderThanSource). Zero when the source never answered one.
	sourceAnsweredAt time.Time
	// copyWant is the source's WAL position read on the connection that
	// asked for a statement's plan, at copyWantAt, kept for the wait of the
	// statement the copy then runs (waitForCopy).
	copyWant   uint64
	copyWantAt time.Time
	// slotWaitCap, when set, bounds the wait for a connection to the source
	// (source), below the statement's own.
	slotWaitCap time.Duration
	// stmtCtx is the context of the message being handled (beginMessage),
	// nil between messages.
	stmtCtx context.Context
	// src is which tables the source sent this connection rows or a
	// statement's end about (markSourceRead, source_tables.go): the copy
	// must not answer it with less of those. proving is the statement the
	// source is answering now when its tables may be confirmed, nil when
	// they cannot: what it sends then marks every table.
	// knownLSN is the source's WAL position read after the last such answer
	// on this connection, valid while knownValid.
	src        sourceReads
	proving    *sourceProof
	knownLSN   uint64
	knownValid bool
	// last is which side answered the connection's last statement, and why
	// (portstatus.go): what SHOW dbtrail.router_status says.
	last LastAnswer
}

// initSettings sets a connection's settings before its start-up message is
// read: names resolve in schema, and user is who logged in.
func (s *session) initSettings(schema, user string) {
	s.defaultSchema, s.user = schema, user
	s.defaults = defaultSettings(schema)
	s.set, s.sessionLevel, s.txSaved = s.defaults, s.defaults, s.defaults
}

// schema is where an unqualified name resolves now: the search_path's
// schema, or the connect database on a session whose settings were never
// set up (initSettings).
func (s *session) schema() string {
	if s.defaults.searchPath == nil {
		return s.currentDB
	}
	return s.set.schema
}

// connContext is the connection's context.
func (s *session) connContext() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

// welcome finishes the login: AuthenticationOk, each reported parameter
// (ParameterStatus), the connection's process number and secret key
// (BackendKeyData), then ReadyForQuery, in PostgreSQL's order.
func (s *session) welcome() {
	// From here on a message is a statement: a larger ceiling, still one.
	s.be.SetMaxBodyLen(maxStatementMessage)
	s.reported = map[string]string{}
	s.reportChanges()
	if t := s.cancelTarget; t != nil {
		s.be.Send(&pgproto3.BackendKeyData{ProcessID: t.pid, SecretKey: append([]byte(nil), t.key[:]...)})
	}
	s.be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

// txStatus is the status ReadyForQuery reports.
func (s *session) txStatus() byte {
	if s.tx == 0 {
		return 'I'
	}
	return s.tx
}

// pgErr is a query-level failure carrying a 5-char SQLSTATE and a message.
type pgErr struct {
	code string
	msg  string
}

// handleSimpleQuery answers one simple-protocol Query. It returns a non-nil
// error ONLY on a wire write failure (the caller then closes); a query-level
// failure is sent as an ErrorResponse and returns nil. Every path ends with
// ReadyForQuery so the client is never left hanging (advisor invariant).
func (s *session) handleSimpleQuery(qstr string) error {
	// A simple query ends the unnamed statement and portal, as on
	// PostgreSQL.
	s.dropUnnamed()
	if isEmptyQuery(qstr) {
		s.be.Send(&pgproto3.EmptyQueryResponse{})
		return s.readyFlush()
	}

	// Whatever result this statement counts in the port's budget is given
	// back when this returns: its answer is written by then, or never will
	// be (the write failed, the client left, the statement was refused).
	defer s.releaseResult()
	// A statement the source answers (forward.go, route.go) is relayed from
	// it.
	started := time.Now()
	var d routeDecision
	screenBusy := ""
	if s.abortedRefusal(qstr) == nil {
		d = s.route(s.classify(qstr), qstr)
		screenBusy = s.takeScreenBusy()
	}
	if byCostApplies(d) {
		// The screen lets the copy answer it: the source's plan says
		// whether the copy is worth it (route_cost.go).
		var stop *pgErr
		if d, stop = s.routeByCostRemembered(qstr); stop != nil {
			s.fail(stop)
			s.routeNote(qstr, d, time.Since(started), true)
			return s.readyFlush()
		}
	}
	if busyOnCopy(withScreenBusy(d, screenBusy)) {
		// For the copy, which was too busy to list its columns (#3109,
		// route.go): refused, not sent to the source.
		s.fail(busyRefusal(screenBusy))
		s.routeNote(qstr, routeDecision{RouteRefused, screenBusy}, time.Since(started), true)
		return s.readyFlush()
	}
	if d.side == RouteSource || d.side == RouteRefused {
		ok := s.forwardSimple(qstr)
		s.routeNote(qstr, d, time.Since(started), !ok)
		return s.readyFlush()
	}
	s.fallback, s.busyRefused = "", ""
	cols, types, rows, tag, perr := s.run(qstr)
	if perr != nil {
		if why := s.takeBusyRefused(); why != "" && d.side == RouteCopy {
			// The copy was busy (#3109, route.go): refused, not sent to the
			// source, and counted under its reason.
			d = routeDecision{RouteRefused, why}
		}
		if why := s.fallback; why != "" && d.side == RouteCopy {
			// The copy did not answer it, and nothing was sent yet: the
			// source answers instead (route.go).
			s.fallback = ""
			s.logger.Debug("pgshim: the copy did not answer a statement; the source does", "reason", why, "code", perr.code, "err", perr.msg)
			s.releaseResult()
			d = routeDecision{RouteSource, why}
			ok := s.forwardSimple(qstr)
			s.routeNote(qstr, d, time.Since(started), !ok)
			return s.readyFlush()
		}
		s.fail(perr)
		s.routeNote(qstr, d, time.Since(started), true)
		return s.readyFlush()
	}
	s.routeNote(qstr, d, time.Since(started), false)
	// cols == nil marks a non-row reply (a handshake/setup probe like SET): no
	// RowDescription, just the CommandComplete tag. A real query — even one that
	// resolves to zero rows (row absent at AsOf) — has a non-nil cols and emits
	// the RowDescription so the client sees the real column header.
	if cols != nil {
		s.rowDescription(cols, types, nil)
		for _, r := range rows {
			s.dataRow(r)
		}
	}
	s.commandComplete(tag)
	return s.readyFlush()
}

// isEmptyQuery: nothing but blanks and semicolons is an empty query, as on
// PostgreSQL.
func isEmptyQuery(qstr string) bool { return strings.Trim(qstr, " \t\r\n;") == "" }

// run answers one statement, either protocol's, and moves the transaction
// status: BEGIN opens a block, COMMIT and ROLLBACK end it, and in a failed
// block nothing but those runs (25P02), as on PostgreSQL.
//
// types are the columns' types (coltypes.go); nil, or a column past its
// end, is text.
func (s *session) run(qstr string) (cols []string, types []colType, rows [][][]byte, tag string, perr *pgErr) {
	if perr := s.abortedRefusal(qstr); perr != nil {
		return nil, nil, nil, "", perr
	}
	c := s.classify(qstr)
	cols, types, rows, tag, perr = s.resolveClassified(qstr, c)
	if perr != nil {
		return nil, nil, nil, "", perr
	}
	if c.kind != kindProbe || c.cmd == nil {
		return cols, types, rows, tag, nil
	}
	// A transaction block the client opens lives on the source, when the
	// connection can reach it: its start and end are sent there first.
	srcTag, perr := s.forwardTxControl(c.cmd)
	if perr != nil {
		return nil, nil, nil, "", perr
	}
	if c.cmd.kind == cmdCommit && srcTag == "ROLLBACK" && s.txStatus() == 'T' {
		// The source rolled the block back (it had failed there): so is it
		// here, and the client is told ROLLBACK, as PostgreSQL tells it.
		s.markFailed()
	}
	switch c.cmd.kind {
	case cmdBegin:
		if s.txStatus() != 'I' {
			s.warning("25001", "there is already a transaction in progress")
			break
		}
		s.tx = 'T'
		// What a ROLLBACK goes back to, and the block's isolation level.
		s.txSaved = s.set
		s.set.txIso = s.set.defaultIso
		if c.cmd.modes.iso != "" {
			s.set.txIso = c.cmd.modes.iso
		}
	case cmdCommit, cmdRollback:
		switch s.txStatus() {
		case 'I':
			s.warning("25P01", "there is no transaction in progress")
		case 'E':
			// A failed block is rolled back, whatever ended it; what it set
			// was undone when it failed.
			tag = "ROLLBACK"
		case 'T':
			if c.cmd.kind == cmdCommit {
				// A SET LOCAL ends with its block; a SET stays.
				s.set = s.sessionLevel
			} else {
				s.set, s.sessionLevel = s.txSaved, s.txSaved
			}
		}
		s.tx = 'I'
		s.savepoints = nil
		s.dropPortals()
	}
	return cols, types, rows, tag, nil
}

// abortedRefusal refuses a statement in a failed transaction block other
// than the COMMIT or ROLLBACK that ends it.
func (s *session) abortedRefusal(qstr string) *pgErr {
	if s.txStatus() != 'E' {
		return nil
	}
	if c, ok, _ := parseSessionCmd(qstr); ok && (c.kind == cmdCommit || c.kind == cmdRollback) {
		return nil
	}
	// A block on the source is brought back from its error by ROLLBACK TO
	// SAVEPOINT, which the source answers (forward.go).
	if s.pinned() && isRollbackTo(qstr) {
		return nil
	}
	// The port's status reads nothing of the block, and is how a client
	// learns which side failed it (portstatus.go).
	if s.isStatusStatement(qstr) {
		return nil
	}
	return &pgErr{"25P02", "current transaction is aborted, commands ignored until end of transaction block"}
}

// fail sends a statement's error. In a transaction block the block fails
// with it, and what the block set is undone, as on PostgreSQL.
func (s *session) fail(perr *pgErr) {
	s.errorResponse(perr.code, perr.msg)
	s.markFailed()
}

// markFailed fails the transaction block a statement failed in, and undoes
// what the block set.
func (s *session) markFailed() {
	if s.txStatus() == 'T' {
		s.tx = 'E'
		s.set, s.sessionLevel = s.txSaved, s.txSaved
	}
}

// warning sends a WARNING notice, unless client_min_messages is above it.
func (s *session) warning(code, msg string) {
	if messageRank["warning"] < messageRank[s.set.minMessages] {
		return
	}
	s.be.Send(&pgproto3.NoticeResponse{Severity: "WARNING", SeverityUnlocalized: "WARNING", Code: code, Message: msg})
}

// stmtKind is which way resolve answers a statement.
type stmtKind int

const (
	// kindFree is ordinary SQL, answered on the copy.
	kindFree stmtKind = iota
	// kindTimeTravel is a well-formed time-travel statement.
	kindTimeTravel
	// kindProbe is a session statement answered here with a tag (SET,
	// BEGIN...), or refused with one.
	kindProbe
	// kindLocal is a session statement answered here with rows (SHOW,
	// version()).
	kindLocal
	// kindCatalog is a catalog statement a client sends to list what the
	// server holds, answered here from the copy's tables (catalog.go).
	kindCatalog
	// kindRefused is refused before anything runs.
	kindRefused
)

// classified is what classify says about a statement.
type classified struct {
	kind stmtKind
	// q is the time-travel statement, for kindTimeTravel.
	q shim.TimeTravelQuery
	// cmd is the session statement of a kindProbe or a kindLocal; perr
	// refuses a kindRefused.
	cmd  *sessionCmd
	perr *pgErr
	// cat is the recognized catalog statement of a kindCatalog, and catArgs
	// what was read out of it.
	cat     *catalogStmt
	catArgs []string
	// timeTravel says the time-travel parser reads the text (well formed or
	// not), which decides how an argument is written into it.
	timeTravel bool
}

// monitorRefusal is what a connection opened with the monitor login answers
// to a statement that is not one of a proxy's checks.
var monitorRefusal = &pgErr{"0A000", "this is the port's monitor login, for a proxy's health checks: it opens no server, and answers an empty query and SELECT pg_is_in_recovery() only; connect with the port's password or an application login to read a server"}

// classify decides which way resolve answers qstr, without running it. It is
// the one place that decides: resolve acts on it, and the extended protocol
// asks it before a statement runs (to say whether it returns rows, and how
// its arguments are written).
func (s *session) classify(qstr string) classified {
	// The port's own status statement, before anything else reads the text
	// and can send it on (portstatus.go).
	if c, own := s.classifyStatus(qstr); own {
		return c
	}
	q, err := shim.Parse(qstr, s.schema())
	tt := !errors.Is(err, shim.ErrNotTimeTravel)
	if s.h == nil && tt {
		// A port that runs SQL on the copy and has no time-travel engine
		// bound, and a statement written for time travel, well formed or
		// not. Refused by name: run as ordinary SQL it would answer from the
		// copy as it is now, not as of the moment asked for, and a parse
		// error here would send the reader to fix a statement this port
		// does not serve.
		return classified{kind: kindRefused, timeTravel: true, perr: &pgErr{"0A000",
			"time travel (_flashback, _snapshot, AS OF) is not served on this port; it runs SQL on the copy as it is now"}}
	}
	if err != nil {
		if tt {
			// Recognised as a time-travel query but malformed (bad AS OF literal,
			// missing schema, bad grammar) → syntax_error.
			return classified{kind: kindRefused, timeTravel: true, perr: &pgErr{"42601", err.Error()}}
		}
		// Not a time-travel query. The session statements a client sends
		// (SET, SHOW, BEGIN, version()...) are answered here, on either
		// port; on the time-travel port nothing else is.
		cmd, ok, syntaxErr := parseSessionCmd(qstr)
		switch {
		case syntaxErr != nil:
			return classified{kind: kindRefused, perr: syntaxErr}
		case ok && cmd.kind == cmdShow && !cmd.all && gucs[cmd.param] == nil && s.bound != nil:
			// Not a parameter: the copy's own SHOW (SHOW TABLES), as before.
			return classified{kind: kindFree}
		case ok && cmd.kind == cmdFunc && cmd.unknownSetting() && s.bound != nil:
			// current_setting of a parameter this port does not take goes on,
			// as its SHOW does: to the source for a forwarding role, and so
			// does a prepared current_setting($1), which a Describe reads with
			// a placeholder value in place of the argument.
			return classified{kind: kindFree}
		case ok && cmd.returnsRows():
			return classified{kind: kindLocal, cmd: cmd}
		case ok:
			return classified{kind: kindProbe, cmd: cmd}
		}
		// The system catalog: the statements tools send to list schemas,
		// tables and columns are answered here, any other is refused by
		// name, and none reaches the copy's engine (catalog.go).
		canon := catalogCanon(qstr)
		if catalogish := readsCatalog(qstr); catalogish || strings.HasPrefix(canon, "select current_setting(") {
			if st, m := matchCatalog(canon); st != nil && s.bound != nil && s.bound.Catalog != nil {
				return classified{kind: kindCatalog, cat: st, catArgs: m}
			}
			if catalogish {
				if s.bound == nil || s.bound.Catalog == nil {
					return classified{kind: kindRefused, perr: &pgErr{"0A000", "the system catalog (pg_catalog, information_schema) is not served on this port"}}
				}
				return classified{kind: kindRefused, perr: catalogRefusal}
			}
		}
		if s.bound != nil && s.bound.Monitor {
			return classified{kind: kindRefused, perr: monitorRefusal}
		}
		if s.bound != nil {
			return classified{kind: kindFree}
		}
		return classified{kind: kindRefused, perr: &pgErr{"0A000", fmt.Sprintf(
			"this endpoint serves only _flashback / _snapshot AS OF time-travel queries; got: %s",
			strings.TrimSpace(qstr))}}
	}
	return classified{kind: kindTimeTravel, q: q, timeTravel: true}
}

// resolve parses and runs a query through the shared engine, returning the
// column list and types, the rendered rows, the CommandComplete tag, and a
// query-level error. Time travel is single-row AS OF only (full-table and
// _diff are refused), and its columns are text: a row as it was can carry
// other columns, and other types, than the table has now.
func (s *session) resolve(qstr string) (cols []string, types []colType, rows [][][]byte, tag string, perr *pgErr) {
	return s.resolveClassified(qstr, s.classify(qstr))
}

// resolveClassified is resolve of a statement classify already read.
func (s *session) resolveClassified(qstr string, c classified) (cols []string, types []colType, rows [][][]byte, tag string, perr *pgErr) {
	switch c.kind {
	case kindRefused:
		return nil, nil, nil, "", c.perr
	case kindProbe:
		tag, perr := s.sessionReply(c.cmd)
		return nil, nil, nil, tag, perr
	case kindLocal:
		return s.answerLocal(c.cmd)
	case kindCatalog:
		return s.answerCatalog(c.cat, c.catArgs)
	case kindFree:
		return s.runFree(qstr)
	}
	cols, rows, tag, perr = s.resolveTimeTravel(c.q)
	return cols, nil, rows, tag, perr
}

// timeTravelGates are the refusals a time-travel statement meets before
// anything is read or said about its table, in this order. A Describe asks
// them too, before it names the table's columns.
//
// run is set when the statement is about to run, not only be described: the
// port's result budget is then asked too, at its place in the order.
func (s *session) timeTravelGates(q shim.TimeTravelQuery, run bool) *pgErr {
	// Time travel reads its AS OF moment in UTC and writes its cells as the
	// change history holds them: under another session zone the moment
	// would be read hours off, with nothing to say so (#2769).
	if s.set.zone.duck != "" {
		return &pgErr{"0A000", fmt.Sprintf(
			"time travel (_flashback, _snapshot) reads AS OF in UTC, and this session's TimeZone is %s; SET TimeZone = 'UTC' for it, and write the moment in UTC", s.set.zone.name)}
	}
	// A port's own gate on time travel (Binding.TimeTravelRefusal): asked for
	// every statement, so a rule saved while the connection is open governs
	// its next one, and before anything is read or said about the table.
	if s.bound != nil && s.bound.TimeTravelRefusal != nil {
		if why := s.bound.TimeTravelRefusal(); why != "" {
			return &pgErr{"42501", why}
		}
	}

	// Per-tenant schema authorization (#824/#1261), on the RESOLVED target
	// schema — the same rule and the same call the MySQL front-end applies in
	// HandleQuery. Parse fills q.Schema from the connect database for the
	// virtual-schema shapes AND from an explicit `<schema>.<table>` on the bare
	// AS OF form, so this one site covers every way a client can name a schema,
	// with or without a prior selection.
	//
	// It runs BEFORE the PK check, matching the MySQL order: an unauthorized
	// schema must not first be told whether its WHERE column is a PK.
	if msg, deny := s.h.SchemaAuthzCheck(q.Schema); deny {
		return &pgErr{"42501", msg}
	}

	// A time-travel answer is held like any other while it is sent, so the
	// port's result budget is asked for it too (#2804): after the statement
	// was found to be one this connection may send, before the index is read
	// for it. Only for a statement that would be answered with a row: a
	// whole table as of a moment and _diff are refused below for what they
	// are, which sending them again would not change. A Describe reads no
	// row and holds none, so it does not ask.
	if run && (q.Type == shim.TypeFlashback || q.Type == shim.TypeSnapshot) && q.PKColumn != "" {
		if refusal := s.admitResult(); refusal != nil {
			return refusal
		}
	}

	// PK-column validation is shared with the MySQL front-end (#296/#821): a
	// WHERE whose column is not the table's single-column PK is refused so a
	// non-PK filter cannot silently return the wrong row.
	if msg, reject := s.h.PKColumnCheck(q); reject {
		return &pgErr{"42601", msg}
	}

	switch q.Type {
	case shim.TypeDiff:
		return &pgErr{"0A000",
			"_diff is not supported over the PostgreSQL wire front-end; use `bintrail-pg reconstruct --history` for a row's per-event history"}
	case shim.TypeFlashback, shim.TypeSnapshot:
		if q.PKColumn == "" {
			return &pgErr{"0A000", fullTableRefusalMsg}
		}
		return nil
	}
	return &pgErr{"XX000", fmt.Sprintf("unsupported query type: %s", q.Type)}
}

// resolveTimeTravel answers a well-formed time-travel statement: its gates,
// then the row as of the moment asked for.
func (s *session) resolveTimeTravel(q shim.TimeTravelQuery) (cols []string, rows [][][]byte, tag string, perr *pgErr) {
	if perr := s.timeTravelGates(q, true); perr != nil {
		return nil, nil, "", perr
	}
	// The statement's own context, which a cancel request and
	// statement_timeout end; the engine derives its query context from it.
	stmtCtx, done := s.statementContext()
	s.h.BindConnContext(stmtCtx)
	defer func() {
		s.h.BindConnContext(s.connContext())
		done()
	}()
	ctx, cancel := s.h.QueryContext()
	defer cancel()
	var image map[string]any
	var rerr error
	if q.Type == shim.TypeSnapshot {
		image, rerr = s.h.ResolveSnapshotRow(ctx, q)
	} else {
		image, rerr = s.h.ResolveFlashbackRow(ctx, q)
	}
	if rerr != nil {
		if perr := stoppedError(stmtCtx); perr != nil {
			return nil, nil, "", perr
		}
		return nil, nil, "", pgResolveError(rerr)
	}
	cols = s.h.ColumnsFor(image, q)
	if image == nil {
		// Row did not exist at AsOf (never created, or a DELETE tail): a real
		// resultset with the table's columns and zero rows. Still a served
		// time-travel read, so it is audited like the MySQL front-end's
		// empty resultsets (side channel on the success path, see
		// ext/audit.go; a refusal above emits nothing).
		s.h.AuditResolve(q, 0)
		return cols, nil, "SELECT 0", nil
	}
	cells, cerr := imageCells(image, cols)
	if cerr != nil {
		// A marker that stands for a value the index does not hold (the
		// unchanged-TOAST one, #592, or the not-captured one, #3027): refuse
		// rather than serve the marker's JSON as a value.
		s.logger.Error("pgshim: a row as of a moment holds a marker in place of a value; the read was refused",
			"schema", q.Schema, "table", q.Table, "err", cerr)
		return nil, nil, "", &pgErr{"XX000", cerr.Error()}
	}
	rows = [][][]byte{cells}
	// Counted from here until it is written; refused when the budget was
	// taken while the row was read. Before the audit record: a refused
	// answer served nobody.
	if refusal := s.holdResult(rows); refusal != nil {
		return nil, nil, "", refusal
	}
	s.h.AuditResolve(q, 1)
	return cols, rows, "SELECT 1", nil
}

// pgResolveError maps the wire-neutral *shim.ResolveError (or a raw data-fault)
// to a PostgreSQL SQLSTATE. The class split mirrors the MySQL front-end's
// wire-code mapping; only the codes differ.
func pgResolveError(err error) *pgErr {
	var re *shim.ResolveError
	if errors.As(err, &re) {
		switch re.Class {
		case shim.ResolveGap:
			return &pgErr{"22023", fmt.Sprintf(
				"resolve %s: %s — the AS OF instant is outside the history this index retains (rotated and not archived)",
				re.QType, re.Err)}
		case shim.ResolveNoSnapshot:
			// invalid_parameter_value, like a coverage gap: the instant asked
			// for is one this server cannot answer for.
			return &pgErr{"22023", re.Error()}
		case shim.ResolveTimeout:
			return &pgErr{"57014", fmt.Sprintf(
				"resolve %s: query exceeded --query-timeout and was aborted; narrow the AS OF range, filter by PK, or raise --query-timeout",
				re.QType)}
		case shim.ResolveCanceled:
			return &pgErr{"57014", fmt.Sprintf(
				"resolve %s: query canceled (client disconnected or server shutting down)", re.QType)}
		default:
			return &pgErr{"XX000", re.Error()}
		}
	}
	// The source's binary log started again after the baseline (#2174): the
	// changes since it cannot be read, the same class as a coverage gap.
	if errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		return &pgErr{"22023", err.Error()}
	}
	// Raw data-fault (ApplyAt / baseline read, e.g. a TOAST marker).
	return &pgErr{"XX000", err.Error()}
}

// imageCells renders a row image to PostgreSQL text-format cells in cols order.
// A key missing from the image is SQL NULL (nil cell) — the same "column dropped
// since AsOf" semantic the MySQL verbatim path uses.
func imageCells(image map[string]any, cols []string) ([][]byte, error) {
	cells := make([][]byte, len(cols))
	for i, c := range cols {
		v, ok := image[c]
		if !ok {
			cells[i] = nil
			continue
		}
		cell, err := textCell(c, v)
		if err != nil {
			return nil, err
		}
		cells[i] = cell
	}
	return cells, nil
}

// textCell renders one image value as a PostgreSQL text-format cell (nil = SQL
// NULL). The image is post-ApplyAt: numbers arrive as json.Number, strings/bytes
// pass through, and a residual unchanged-TOAST marker is refused (#592) rather
// than serialised, as is the not-captured marker (#3027), which stands for a
// column of an earlier row PostgreSQL never sent and which a row as of a
// moment is never built from: one here is a defect, said as one. Byte values
// go on the wire verbatim (the conservative all-text encoding), so a
// bytea/blob round-trips its exact bytes.
func textCell(col string, v any) ([]byte, error) {
	if event.IsNotCapturedMarker(v) {
		return nil, fmt.Errorf("column %s holds the not-captured marker in place of a value: the row's value at that moment "+
			"was never captured (the table was not at REPLICA IDENTITY FULL when the row changed), and a row read as of a "+
			"moment is never meant to be built from it; this is a defect in DBTrail, please report it", col)
	}
	if event.IsUnchangedToastMarker(v) {
		return nil, event.UnresolvedToastError("", "", "", []string{col})
	}
	switch x := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	case json.Number:
		return []byte(x.String()), nil
	// Native numeric types appear for a baseline-origin cell (ReadBaselineRow
	// scans a typed Parquet column into a native Go value) — reachable when a
	// MySQL/MariaDB-source index is served here and _snapshot returns a value
	// untouched in the binlog window. Render them explicitly rather than via the
	// default fmt path, whose %v on a float uses scientific notation for extreme
	// magnitudes; strconv 'g'/-1 is the shortest round-trippable form and matches
	// PostgreSQL's own float text. (A PG-source index stores every value as text,
	// so it hits the string case above and never reaches here.)
	case float64:
		return strconv.AppendFloat(nil, x, 'g', -1, 64), nil
	case float32:
		return strconv.AppendFloat(nil, float64(x), 'g', -1, 32), nil
	case int64:
		return strconv.AppendInt(nil, x, 10), nil
	case int32:
		return strconv.AppendInt(nil, int64(x), 10), nil
	case int:
		return strconv.AppendInt(nil, int64(x), 10), nil
	case uint64:
		return strconv.AppendUint(nil, x, 10), nil
	case uint32:
		return strconv.AppendUint(nil, uint64(x), 10), nil
	case bool:
		if x {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case time.Time:
		return timestampText(x), nil
	default:
		return []byte(fmt.Sprint(x)), nil
	}
}

// timestampText writes a moment as PostgreSQL 16 writes a timestamp, in
// UTC: the fraction of the second to the microsecond with its trailing
// zeros dropped (03:04:05.12, 03:04:05 when it has none), a year before 1
// as its number and BC (0044-03-15 12:00:00 BC), and the two ends the
// copy's engine holds as its largest and smallest microsecond counts as
// infinity and -infinity. Finer than a microsecond is rounded to one, as
// PostgreSQL rounds what it is given. textCell is not told a column's type:
// a date or a timestamp with time zone that reaches it as a time.Time is
// written as a timestamp too (2026-01-02 00:00:00), as before.
func timestampText(t time.Time) []byte {
	switch {
	case t.Equal(engineInfinity):
		return []byte("infinity")
	case t.Equal(engineMinusInfinity):
		return []byte("-infinity")
	}
	t = t.UTC().Round(time.Microsecond)
	year, era := t.Year(), ""
	if year <= 0 {
		// Year 0 of the calendar this counts in is 1 BC.
		year, era = 1-year, " BC"
	}
	b := fmt.Appendf(nil, "%04d-%02d-%02d %02d:%02d:%02d", year, int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second())
	if us := t.Nanosecond() / 1000; us != 0 {
		b = append(b, '.')
		b = append(b, strings.TrimRight(fmt.Sprintf("%06d", us), "0")...)
	}
	return append(b, era...)
}

// engineInfinity and engineMinusInfinity are what the copy's engine hands
// over for the timestamps infinity and -infinity.
var (
	engineInfinity      = time.UnixMicro(math.MaxInt64)
	engineMinusInfinity = time.UnixMicro(-math.MaxInt64)
)

// --- wire writers ---------------------------------------------------------

// rowDescription describes the columns, each with its type (types; text
// where it has none). formats are a portal's result formats from its Bind:
// none (text), one for every column, or one each.
func (s *session) rowDescription(cols []string, types []colType, formats []int16) {
	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, c := range cols {
		t := typeAt(types, i)
		fields[i] = pgproto3.FieldDescription{
			Name:         []byte(c),
			DataTypeOID:  t.oid,
			DataTypeSize: t.size,
			TypeModifier: t.typmod,
			Format:       formatAt(formats, len(cols), i),
		}
	}
	s.be.Send(&pgproto3.RowDescription{Fields: fields})
}

func (s *session) dataRow(cells [][]byte) {
	s.be.Send(&pgproto3.DataRow{Values: cells})
}

func (s *session) commandComplete(tag string) {
	s.be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

func (s *session) errorResponse(code, msg string) {
	s.be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: code, Message: msg})
}

// readyFlush sends ReadyForQuery with the transaction status and flushes —
// the required end of every simple-query cycle and of every Sync, success or
// failure — after a ParameterStatus for each reported parameter the
// statements before it changed.
func (s *session) readyFlush() error {
	// A connection to the source nothing of the client's lives on goes back
	// to the port (forward.go).
	s.releaseIdleSource()
	s.reportChanges()
	s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
	return s.be.Flush()
}
