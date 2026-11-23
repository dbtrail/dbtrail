package pgshim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/pgtext"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// This file is the front-end's second mode: a port that runs ordinary
// read-only SQL on a server's Parquet copy, for a host that brings its own
// login (Config.Login). The router uses it for its PostgreSQL-protocol port,
// next to the MySQL one; `bintrail-pg flashback` keeps the first mode, a
// shim.yaml tenant and time travel alone.
//
// What a client gets is the copy's SQL over a PostgreSQL wire: the simple
// and the extended query protocol (extended.go), every column announced with
// its PostgreSQL type and written the way PostgreSQL writes a value of that
// type (pgtext.Cell; in binary when the client asks, coltypes.go), and nothing
// that writes.
// The login is SCRAM-SHA-256 (scram.go), so the password never crosses the
// wire, and the port serves TLS when the host gives it a certificate.
//
// Time travel (_flashback, _snapshot, AS OF) is served on the same
// connection when the host binds an engine for it (Binding.TimeTravel), by
// the code the first mode runs; without one it is refused by name.

// Error is a failure the client is shown: a five-character SQLSTATE and a
// message written for the person reading it.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }

// LoginFunc decides whether a client may connect and what its connection is
// bound to. user and database are the client's start-up parameters (database
// is user when the client named none, as PostgreSQL has it) and password is
// what the client proved in its SCRAM exchange: the login checks it against
// the passwords that open the port (Password.Matches), and a login that lets
// a client in without one matching is refused all the same. drop closes the
// connection, for a host that must put a client out later (a credential
// withdrawn). A refusal is sent to the client as a FATAL error and the
// connection is closed.
type LoginFunc func(ctx context.Context, user, database string, password *Password, drop func()) (*Binding, *Error)

// Binding is what one logged-in connection runs its statements with.
type Binding struct {
	// Run answers one statement on the copy, in what the connection set for
	// itself (opts). An *Error is sent to the client as it is; the SQL
	// sandbox's typed errors are sent by kind (freeError); anything else must
	// already be text the client can be shown.
	Run func(ctx context.Context, statement string, opts RunOptions) (sqlsandbox.Result, error)
	// Actor is who the audit trail says read the rows.
	Actor string
	// Schema is where unqualified names resolve, for the audit record.
	Schema string
	// Catalog, when set, reads the copy's tables for the system catalog
	// statements tools send to list them (catalog.go): the tables a
	// statement on the copy can read, each column with the copy engine's
	// type, and what the source recorded of each. Nil: such statements are
	// refused.
	Catalog func(ctx context.Context) ([]CatalogTable, error)
	// CopyTableNames, when set, is the schema and name of each table a
	// statement on the copy can read, read without one of the copy's workers:
	// what the capture gate asks of every statement sent to the source, which
	// must not wait behind the copy's reports. Nil: Catalog is read.
	CopyTableNames func(ctx context.Context) ([][2]string, error)
	// TimeTravel, when set, answers the time-travel statements of this
	// connection (single-row _flashback and _snapshot AS OF, as the first
	// mode serves them). It reads the index and the snapshot directly. Nil:
	// a time-travel statement is refused by name.
	TimeTravel *shim.Handler
	// TimeTravelRefusal, when set, is asked before every time-travel
	// statement; a reason it returns refuses the statement with that reason.
	// It is how a host keeps time travel away from what its views hide:
	// time travel reads past the views, so rules applied in them (rows or
	// columns a server hides) would not hold here.
	TimeTravelRefusal func() string
	// Answered, when set, is told how long each statement took and whether
	// the client got an error.
	Answered func(took time.Duration, refused bool)
	// Close, when set, runs once when the connection ends.
	Close func()
	// Monitor is a connection opened with a proxy's monitor login: it opens
	// no server, and answers only the proxy's checks, an empty query, the
	// session's own statements and SELECT pg_is_in_recovery(); any other
	// statement is refused (monitorRefusal).
	Monitor bool
	// Forward, when set, is how the connection reaches the server's source,
	// which answers a transaction block the client opens, a catalog
	// statement the copy does not answer, and every statement the copy is
	// not known to answer as PostgreSQL does (forward.go, route.go). Nil:
	// those are answered as the port answers them without one.
	Forward *Forward
	// Routed, when set, is told which side answered each statement that
	// reached the copy or the source, and why (route.go), for the port's
	// status.
	Routed func(RouteNote)
	// RunAge, when set, says how far the data of Run's last successful run
	// on this connection reaches, asked right after it returned. With it a
	// connection the source answered is not given an older answer of the
	// copy (copyOlderThanSource); nil: the copy's answers are given as they
	// are.
	RunAge func() CopyAge
	// CopyWait, when set, waits within ctx until the copy holds every commit
	// of the source before lsn, a WAL position, and says whether it does, or
	// why not (waitForCopy). nil: nothing waits, and RunAge alone decides.
	CopyWait func(ctx context.Context, lsn uint64) (CopyWaitOutcome, string)
	// NotCaptured, when set, lists the copy's tables whose capture lost or
	// dropped changes since their snapshot's rows were read from the source,
	// asked before a statement runs on the copy (not_captured.go). On a
	// connection that can fall back, a statement that reads one is answered
	// by the source (ReasonTableNotCaptured); an error sends it there too.
	// Where nothing can go to the source the copy answers, with a NOTICE that
	// names the table. nil: nothing is asked.
	NotCaptured func(ctx context.Context) ([]TableNotCaptured, error)
	// Status, when set, answers SHOW dbtrail.router_status on this
	// connection (portstatus.go): name and value pairs, in the order to
	// show them. last is the connection's last answer, whose rows
	// (LastAnswer.Rows) the host places among its own. Nil: the statement
	// answers the last answer's rows alone.
	Status func(ctx context.Context, last LastAnswer) [][2]string
}

// TableNotCaptured is a table of the copy whose capture lost or dropped
// changes since its snapshot's rows were read (Binding.NotCaptured): Schema
// and Name as the copy's catalog writes them, Why in words.
type TableNotCaptured struct {
	Schema, Name, Why string
	// SnapshotRead is when the rows of its snapshot were read from the
	// source, for the notice; zero when not known.
	SnapshotRead time.Time
}

// CopyWaitOutcome is what Binding.CopyWait found.
type CopyWaitOutcome int

const (
	// CopyNoWait: there is nothing to wait on (no recent changes are kept
	// for the server yet: the copy's run starts them); the copy runs, and
	// RunAge decides after.
	CopyNoWait CopyWaitOutcome = iota
	// CopyReached: the copy holds every commit before the position.
	CopyReached
	// CopyNotReached: it does not within the time given, or cannot; the
	// source answers.
	CopyNotReached
)

// CopyAge is how far the data of a statement the copy ran reaches.
type CopyAge struct {
	// Read: it read a table of the copy (a statement that reads none, SELECT
	// 1 or SHOW TABLES, has no age).
	Read bool
	// Through: every change capture recorded up to this instant (a commit
	// time on the source) was in what it read; zero when that is not known,
	// as for a table read from its snapshot alone.
	Through time.Time
}

// RunOptions are what a connection set for itself that a statement on the
// copy runs in (#2769).
type RunOptions struct {
	// Schema is where unqualified names resolve: the session's search_path
	// (Binding.Schema until the client sets one).
	Schema string
	// TimeZone is the session time zone as the copy's engine names it
	// (sqlsandbox.Session.TimeZone), "" for UTC.
	TimeZone string
	// Probe says the port runs it for itself (a schema or time zone check
	// of a SET), not as an answer to the client: the host must not count it
	// as one.
	Probe bool
	// NeedTails says the answer is given only if every table it reads has
	// its recent changes over its snapshot (copyOlderThanSource): when they
	// are not ready in the copy's wait for them, the host refuses the
	// statement before running it, with a *sqlsandbox.MayHaveChangedError,
	// and the source answers it (#3086). Without it a snapshot alone
	// answers, as on a connection with nowhere else to send the statement.
	NeedTails bool
}

// runOptions are the options the connection's next statement runs in.
func (s *session) runOptions() RunOptions {
	return RunOptions{Schema: s.schema(), TimeZone: s.set.zone.duck}
}

// handleLoginConn runs one connection of a port with a login of its own.
func handleLoginConn(ctx context.Context, c net.Conn, cfg Config, pt *portTLS, cancels *cancelRegistry, logger *slog.Logger, place *connPlace) {
	// The first defer, so the last to run: one connection's panic is logged
	// with its stack and ends that connection, not the process the port
	// shares with every other one.
	defer func() {
		if r := recover(); r != nil {
			logger.Error("pgshim: a connection ended in a panic", "panic", fmt.Sprint(r), "remote", c.RemoteAddr(), "stack", string(debug.Stack()))
		}
	}()
	defer c.Close()

	// The socket is read on a goroutine of its own (shim.WatchConn), so a
	// client that hangs up in the middle of a statement ends connCtx at once
	// and its statement stops, instead of running to the end for nobody.
	wc, connCtx, stop := shim.WatchConn(ctx, c)
	defer stop()
	stopClose := context.AfterFunc(connCtx, func() { _ = c.Close() })
	defer stopClose()

	// A client has this long to log in. A socket that connects and says
	// nothing, or stops at the password, would otherwise hold one of the
	// port's connections for as long as it liked, and a handful of them
	// would keep every real client out.
	loginTimer := time.AfterFunc(loginTimeout, func() { _ = c.Close() })
	defer loginTimer.Stop()

	// TLS, when the client asks for it, runs over the watched connection:
	// the socket has one reader, the watcher, and TLS reads what it relays.
	conn, encrypted, err := negotiate(connCtx, wc, pt)
	if err != nil {
		logNegotiateFailure(logger, err, c.RemoteAddr())
		return
	}
	defer conn.Close()
	// Written in pieces, each with its own deadline: a client that stops
	// reading its answer is cut, and what the port held for it is given
	// back (resultbudget.go). On TLS through it, so the answers are
	// encrypted.
	be := pgproto3.NewBackend(conn, &deadlineWriter{c: conn, logger: logger})
	startup, err := receiveStartup(be, cancels, logger)
	if err != nil {
		logger.Debug("pgshim startup failed", "err", err, "remote", c.RemoteAddr())
		return
	}
	if startup == nil {
		return // CancelRequest / clean close
	}
	user := startup.Parameters["user"]
	database := startup.Parameters["database"]
	if database == "" {
		database = user
	}
	place.spoke.Store(true)
	password, err := scramExchange(connCtx, be, user, encrypted, pt.bindingData(), place)
	if err != nil {
		// Debug: psql itself closes here once, to ask for the password and
		// connect again. A malformed exchange was told why.
		logger.Debug("pgshim: no password was proved", "err", err, "user", user, "remote", c.RemoteAddr())
		return
	}
	bound, refused := cfg.Login(connCtx, user, database, password, func() { _ = c.Close() })
	if refused == nil && bound != nil && !password.matched() {
		// The host let the client in without checking its password against
		// any: a bug of the host's, and the client is not let in on it.
		logger.Error("pgshim: the login let a client in without checking its password; refused", "user", user, "remote", c.RemoteAddr())
		if bound.Close != nil {
			bound.Close()
		}
		bound, refused = nil, &Error{Code: "28P01", Message: fmt.Sprintf("password authentication failed for user %q", user)}
	}
	if password.lostPlace && bound == nil {
		// Not a wrong password: the port closed this connection to make
		// room, and its proof was never checked.
		logger.Info("pgshim: "+errLostPlace.Error(), "user", user, "database", database, "remote", c.RemoteAddr())
		return
	}
	if bound == nil && !password.matched() {
		// No refusal of a proof comes sooner than the floor, whatever the
		// port had in memory for the username.
		password.holdFailure()
	}
	if password.busy && bound == nil {
		// The proof was not checked, or failed while the port had no room
		// to check a new one: not said as a wrong password.
		refused = &Error{Code: busyLoginCode, Message: busyLoginMessage}
	}
	if refused != nil || bound == nil {
		if refused == nil {
			refused = &Error{Code: "XX000", Message: "the login returned nothing to connect to"}
		}
		// With the reason: it is the text the client is shown, and "cannot
		// open server" is the operator's to fix.
		logger.Info("pgshim login refused", "user", user, "database", database, "code", refused.Code, "reason", refused.Message, "remote", c.RemoteAddr())
		sendFatal(be, refused.Code, refused.Message)
		return
	}
	if bound.Close != nil {
		defer bound.Close()
	}
	if !loginTimer.Stop() {
		// It fired: the socket is closed, or about to be.
		logger.Info("pgshim login took too long", "user", user, "database", database, "remote", c.RemoteAddr())
		return
	}
	if !place.loggedIn() {
		// Its place went to a newer connection while it was logging in, and
		// its socket is closed.
		logger.Info("pgshim: the port was full and this connection had not logged in; its place went to a newer one", "user", user, "database", database, "remote", c.RemoteAddr())
		return
	}
	sess := &session{be: be, bound: bound, ctx: connCtx, logger: logger, budget: cfg.ResultBudget}
	if bound.TimeTravel != nil {
		// The engine's statements end with the connection, as free SQL's do.
		bound.TimeTravel.BindConnContext(connCtx)
		sess.h = bound.TimeTravel
	}
	// Where unqualified names resolve, a time-travel statement's table
	// included: the schema free SQL resolves names in, until a SET
	// search_path.
	sess.currentDB = bound.Schema
	sess.database = database
	sess.initSettings(bound.Schema, user)
	target, release := cancels.register()
	defer release()
	sess.cancelTarget = target
	sendSASLFinal(be, password)
	be.Send(&pgproto3.AuthenticationOk{})
	// The start-up message's settings, as PostgreSQL applies them: after the
	// login, and a value refused ends the connection with the reason.
	if perr := sess.applyStartup(startup.Parameters); perr != nil {
		logger.Info("pgshim: a start-up parameter was refused", "user", user, "code", perr.code, "reason", perr.msg, "remote", c.RemoteAddr())
		sendFatal(be, perr.code, perr.msg)
		return
	}
	sess.welcome()
	if err := be.Flush(); err != nil {
		return
	}
	commandLoop(be, sess)
}

// loginTimeout is how long a client has, from connecting, to send its
// start-up message and its password. A variable so a test can shorten it.
var loginTimeout = 30 * time.Second

// runFree answers a statement that is not time travel on the copy.
func (s *session) runFree(qstr string) (cols []string, types []colType, rows [][][]byte, tag string, perr *pgErr) {
	started := time.Now()
	// A session time zone the copy could not be asked about when it was set
	// is asked about now (settings.go), before the statement's own context:
	// the question has its own, and the statement's must stay the one a
	// cancel request stops.
	if perr := s.zoneOnCopy(s.set.zone, true); perr != nil {
		if s.busyRefused == "" {
			s.fallTo(ReasonCopyRefused)
		}
		return nil, nil, nil, "", perr
	}
	ctx, done := s.statementContext()
	defer done()
	defer func() {
		// A client that left, a port that is closing, a cancel request and
		// the client's own statement_timeout are not the copy refusing a
		// statement: they are not counted at all.
		if s.bound.Answered != nil && ctx.Err() == nil {
			s.bound.Answered(time.Since(started), perr != nil)
		}
	}()
	// Before anything is run (#2804): a statement whose result the port has
	// no room to hold is refused here, not after the copy answered it.
	if refusal := s.admitResult(); refusal != nil {
		return nil, nil, nil, "", refusal
	}
	var res sqlsandbox.Result
	waited, apart, named := false, false, 0
	text, err := s.copyText(qstr)
	if err == nil {
		// A char(n) column used where the copy counts its padding (#3068):
		// the source's, or refused where nothing is sent there.
		if refusal := s.paddedRefusal(text); refusal != nil {
			return nil, nil, nil, "", refusal
		}
		// A table whose capture dropped its changes (not_captured.go): the
		// copy holds old rows of it, and the source answers, before the copy
		// does any work or waits for anything.
		if refusal := s.notCapturedRefusal(ctx, text); refusal != nil {
			return nil, nil, nil, "", refusal
		}
		// On a connection the source answered, the copy first waits to
		// reach what the source showed it (#2985), or the source answers
		// instead, before the copy does any work. Unless it reads no table
		// the source answered this connection about: then it neither waits
		// nor is held back.
		apart, named = s.apartFromSource(text)
		var stop *pgErr
		if waited, stop = s.waitForCopy(ctx, qstr, apart); stop != nil {
			return nil, nil, nil, "", stop
		}
		// An answer copyOlderThanSource would hold back unless every table it
		// read had its recent changes: the copy is told before it runs, so it
		// does not run one it would answer from the snapshot alone (#3086).
		opts := s.runOptions()
		opts.NeedTails = s.holdsBackCopy(apart && named > 0)
		res, err = s.bound.Run(ctx, text, opts)
		var changed *sqlsandbox.MayHaveChangedError
		if opts.NeedTails && errors.As(err, &changed) {
			s.logger.Debug("pgshim: the copy's recent changes for a statement were not ready; it did not run there, and the source answers it", "why", changed.Reason)
		}
	}
	if res.Spool != nil {
		// Only a bug gets here: this port asks for no result in a file.
		_ = res.Spool.Close()
		if err == nil {
			err = &Error{Code: "XX000", Message: "the copy answered from a file this port did not ask for"}
		}
	}
	if err != nil {
		perr := s.statementError(ctx, err)
		if ctx.Err() == nil {
			// To the source, or refused when the copy was busy (route.go).
			perr = s.copyFailed(err, perr)
		}
		return nil, nil, nil, "", perr
	}
	// A statement read as apart that names no table, when the copy says it
	// read one, was read wrong: it is held back as any other.
	if why := s.copyOlderThanSource(waited, apart && named > 0); why != "" {
		// Nothing of it was sent: the source answers instead (#2985).
		s.logger.Debug("pgshim: the copy's answer is older than the source's last one on this connection; the source answers", "why", why)
		s.fallTo(ReasonCopyTooOld)
		return nil, nil, nil, "", &pgErr{"55000", why}
	}
	if res.Truncated {
		// Refused, not returned short: a client would take the first rows
		// for the whole answer.
		return nil, nil, nil, "", &pgErr{"54000", fmt.Sprintf(
			"the result has more than %d rows, the most one statement returns on this port, and is not returned cut; add a LIMIT or narrow the statement",
			len(res.Rows))}
	}
	if col := writtenOtherwise(res.Columns); col != "" && s.canFallBack() {
		// A numeric declared without precision, which the copy writes with
		// all its decimals: the source writes it as PostgreSQL does. Only
		// where the source can answer; without, the copy's answer is the
		// one this port has always given.
		s.fallTo(ReasonCopyColumnsDiffer)
		return nil, nil, nil, "", &pgErr{"0A000", fmt.Sprintf("column %q is a numeric the copy writes otherwise", col)}
	}
	if res.TruncatedCells > 0 {
		// Refused too. The copy cuts a cell past its cap and ends it with a
		// marker, which a reader of text can see; here a cell is written
		// again the way PostgreSQL writes its kind, and a cut bytea would
		// come out as other bytes with no marker in sight. A notice is not
		// enough: most drivers drop it.
		return nil, nil, nil, "", &pgErr{"54000", fmt.Sprintf(
			"%d cell(s) of the result are longer than the most one cell may be on this port, and are not returned cut; select fewer columns, or a part of the long one (substring, left)",
			res.TruncatedCells)}
	}
	cols = make([]string, len(res.Columns))
	types = make([]colType, len(res.Columns))
	for i, c := range res.Columns {
		cols[i] = c.Name
		types[i] = columnType(c.Type)
	}
	rows = make([][][]byte, len(res.Rows))
	for i, r := range res.Rows {
		if len(r) != len(cols) {
			s.logger.Error("pgshim: a row of the copy's answer has the wrong number of cells", "row", i, "cells", len(r), "columns", len(cols))
			return nil, nil, nil, "", &pgErr{"XX000", "the copy's answer could not be read; DBTrail's log has the details"}
		}
		cells := make([][]byte, len(r))
		for j, v := range r {
			// A timestamp with time zone in the session's zone (SET
			// TimeZone), as PostgreSQL writes one.
			if text := pgtext.CellIn(res.Columns[j].Type, v, s.set.zone.loc); text != nil {
				// Never nil for a value: an empty string is not NULL.
				cells[j] = append([]byte{}, *text...)
			}
		}
		rows[i] = cells
	}
	// Counted from here until it is written, or refused when the budget was
	// taken while the statement ran. Before the notice and the audit
	// record: a refused answer served nobody.
	if refusal := s.holdResult(rows); refusal != nil {
		return nil, nil, nil, "", refusal
	}
	if res.Note != "" {
		s.notice(res.Note)
	}
	s.recordFree(qstr, res)
	return cols, types, rows, "SELECT " + strconv.Itoa(len(rows)), nil
}

// notice sends the client a sentence beside the statement's answer, unless
// client_min_messages is above a notice.
func (s *session) notice(msg string) {
	if messageRank["notice"] < messageRank[s.set.minMessages] {
		return
	}
	s.be.Send(&pgproto3.NoticeResponse{Severity: "NOTICE", SeverityUnlocalized: "NOTICE", Code: "00000", Message: msg})
}

// recordFree writes the audit record of a statement this port answered: the
// same record, under the same name, as the MySQL-protocol port's
// (shim.Handler.recordFreeSQL). On the success path only, outside every lock.
func (s *session) recordFree(statement string, res sqlsandbox.Result) {
	if !ext.Auditing() {
		return
	}
	actor := s.bound.Actor
	if actor == "" {
		actor = unboundActor
	}
	detail := map[string]string{
		"sql":       statement,
		"rows":      strconv.Itoa(len(res.Rows)),
		"truncated": strconv.FormatBool(res.Truncated),
		"protocol":  "postgresql",
	}
	if res.TruncatedCells > 0 {
		detail["truncated_cells"] = strconv.Itoa(res.TruncatedCells)
	}
	if schema := s.schema(); schema != "" {
		detail["schema"] = schema
	}
	if res.Note != "" {
		detail["note"] = res.Note
	}
	ext.Record(context.WithoutCancel(s.ctx), ext.AuditEvent{
		Surface: "shim",
		Action:  "sql.run",
		Actor:   actor,
		Detail:  detail,
	})
}

// unboundActor is the audit actor of a connection whose host named none: a
// visible sentinel, never a default that reads as a person.
const unboundActor = "postgresql:unbound"

// freeError turns what the copy's executor returned into the client's error.
// The codes are PostgreSQL's nearest: a client tells a statement it wrote
// wrong (class 42) from one that ran out of room or time (53, 54, 57) and
// from a fault that is not its own (XX).
func freeError(err error, logger *slog.Logger) *pgErr {
	var own *Error
	var refused *sqlsandbox.RefusedError
	var qerr *sqlsandbox.QueryError
	var timeout *sqlsandbox.TimeoutError
	var werr *sqlsandbox.WorkerError
	var behind *sqlsandbox.BehindSourceError
	var unavailable *sqlsandbox.UnavailableError
	var differ *sqlsandbox.ColumnsDifferError
	var changed *sqlsandbox.MayHaveChangedError
	var stopped *stoppedErr
	switch {
	case errors.As(err, &stopped):
		return stopped.perr
	case errors.As(err, &own):
		return &pgErr{own.Code, own.Message}
	case errors.As(err, &refused):
		if strings.HasPrefix(refused.Reason, "syntax error") {
			// The sandbox parses the statement before it runs anything, and
			// says so in these words (sqlsandbox worker).
			return &pgErr{"42601", refused.Reason}
		}
		// The statement's shape: not a read, more than one statement, or
		// something the sandbox does not run.
		return &pgErr{"0A000", refused.Reason}
	case errors.As(err, &behind):
		return &pgErr{"55000", behind.Reason}
	case errors.As(err, &unavailable):
		return &pgErr{"55000", unavailable.Reason}
	case errors.As(err, &differ):
		return &pgErr{"22000", differ.Reason}
	case errors.As(err, &changed):
		return &pgErr{"55000", changed.Reason}
	case errors.As(err, &qerr):
		return &pgErr{engineErrorCode(qerr.Message), qerr.Message}
	case errors.As(err, &timeout):
		return &pgErr{"57014", fmt.Sprintf("the query ran longer than this server's cap of %.0f s and was stopped; narrow it", timeout.Limit.Seconds())}
	case errors.Is(err, sqlsandbox.ErrBusy):
		return &pgErr{"53000", err.Error()}
	case errors.Is(err, sqlsandbox.ErrResultTooLarge):
		return &pgErr{"54000", fmt.Sprintf("the result is larger than %d MB, the most one statement returns on this port, "+
			"and is not returned cut; add a LIMIT, select fewer columns, or narrow the statement", sqlsandbox.DefaultLimits().MaxResultBytes>>20)}
	case errors.Is(err, context.DeadlineExceeded):
		return &pgErr{"57014", "the query ran longer than this connection's cap and was stopped; narrow it"}
	case errors.Is(err, context.Canceled):
		return &pgErr{"57014", "the query was cancelled"}
	case errors.As(err, &werr):
		// Its text can carry host paths: logged here, never sent.
		logger.Error("pgshim: the SQL worker failed", "err", err)
		return &pgErr{"XX000", "the SQL worker failed before it could answer; DBTrail's log has the details"}
	default:
		logger.Warn("pgshim: statement refused", "err", err)
		return &pgErr{"XX000", err.Error()}
	}
}

// engineErrorCode reads the kind the copy's engine puts in front of its
// error. A kind this does not list is an internal error: the statement may
// be right.
func engineErrorCode(msg string) string {
	switch {
	case strings.HasPrefix(msg, "Parser Error"):
		return "42601" // syntax_error
	case strings.HasPrefix(msg, "Catalog Error") && strings.Contains(msg, "Table with name"):
		return "42P01" // undefined_table
	case strings.HasPrefix(msg, "Catalog Error"):
		return "42704" // undefined_object
	case strings.HasPrefix(msg, "Binder Error"):
		return "42000" // syntax_error_or_access_rule_violation
	case strings.HasPrefix(msg, "Conversion Error"), strings.HasPrefix(msg, "Invalid Input Error"),
		strings.HasPrefix(msg, "Out of Range Error"), strings.HasPrefix(msg, "Mismatch Type Error"):
		return "22000" // data_exception
	case strings.HasPrefix(msg, "Not implemented Error"):
		return "0A000" // feature_not_supported
	default:
		return "XX000"
	}
}
