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
// query protocol, every column sent as text (a value written the way
// PostgreSQL writes one of its kind, pgtext.Cell), and nothing that writes.
// The password crosses the wire in clear text, so the host binds loopback.

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
// what it answered. drop closes the connection, for a host that must put a
// client out later (a credential withdrawn). A refusal is sent to the client
// as a FATAL error and the connection is closed.
type LoginFunc func(ctx context.Context, user, database, password string, drop func()) (*Binding, *Error)

// Binding is what one logged-in connection runs its statements with.
type Binding struct {
	// Run answers one statement on the copy. An *Error is sent to the client
	// as it is; the SQL sandbox's typed errors are sent by kind (freeError);
	// anything else must already be text the client can be shown.
	Run func(ctx context.Context, statement string) (sqlsandbox.Result, error)
	// Actor is who the audit trail says read the rows.
	Actor string
	// Schema is where unqualified names resolve, for the audit record.
	Schema string
	// Answered, when set, is told how long each statement took and whether
	// the client got an error.
	Answered func(took time.Duration, refused bool)
	// Close, when set, runs once when the connection ends.
	Close func()
}

// handleLoginConn runs one connection of a port with a login of its own.
func handleLoginConn(ctx context.Context, c net.Conn, cfg Config, logger *slog.Logger) {
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

	be := pgproto3.NewBackend(wc, wc)
	startup, err := negotiateStartup(c, be)
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
	password, err := readPassword(be)
	if err != nil {
		return
	}
	bound, refused := cfg.Login(connCtx, user, database, password, func() { _ = c.Close() })
	if refused != nil || bound == nil {
		if refused == nil {
			refused = &Error{Code: "XX000", Message: "the login returned nothing to connect to"}
		}
		logger.Info("pgshim login refused", "user", user, "database", database, "code", refused.Code, "remote", c.RemoteAddr())
		sendFatal(be, refused.Code, refused.Message)
		return
	}
	if bound.Close != nil {
		defer bound.Close()
	}
	sendAuthOK(be)
	if err := be.Flush(); err != nil {
		return
	}
	commandLoop(be, &session{be: be, bound: bound, ctx: connCtx, logger: logger})
}

// runFree answers a statement that is not time travel on the copy.
func (s *session) runFree(qstr string) (cols []string, rows [][][]byte, tag string, perr *pgErr) {
	started := time.Now()
	defer func() {
		if s.bound.Answered != nil {
			s.bound.Answered(time.Since(started), perr != nil)
		}
	}()
	res, err := s.bound.Run(s.ctx, qstr)
	if res.Spool != nil {
		// Only a bug gets here: this port asks for no result in a file.
		_ = res.Spool.Close()
		if err == nil {
			err = &Error{Code: "XX000", Message: "the copy answered from a file this port did not ask for"}
		}
	}
	if err != nil {
		return nil, nil, "", freeError(err, s.logger)
	}
	if res.Truncated {
		// Refused, not returned short: a client would take the first rows
		// for the whole answer.
		return nil, nil, "", &pgErr{"54000", fmt.Sprintf(
			"the result has more than %d rows, the most one statement returns on this port, and is not returned cut; add a LIMIT or narrow the statement",
			len(res.Rows))}
	}
	cols = make([]string, len(res.Columns))
	for i, c := range res.Columns {
		cols[i] = c.Name
	}
	rows = make([][][]byte, len(res.Rows))
	for i, r := range res.Rows {
		if len(r) != len(cols) {
			s.logger.Error("pgshim: a row of the copy's answer has the wrong number of cells", "row", i, "cells", len(r), "columns", len(cols))
			return nil, nil, "", &pgErr{"XX000", "the copy's answer could not be read; DBTrail's log has the details"}
		}
		cells := make([][]byte, len(r))
		for j, v := range r {
			if text := pgtext.Cell(res.Columns[j].Type, v); text != nil {
				// Never nil for a value: an empty string is not NULL.
				cells[j] = append([]byte{}, *text...)
			}
		}
		rows[i] = cells
	}
	if res.TruncatedCells > 0 {
		s.notice(fmt.Sprintf("%d cell(s) longer than this server's cap were cut; each ends with a marker", res.TruncatedCells))
	}
	if res.Note != "" {
		s.notice(res.Note)
	}
	s.recordFree(qstr, res)
	return cols, rows, "SELECT " + strconv.Itoa(len(rows)), nil
}

// notice sends the client a sentence beside the statement's answer.
func (s *session) notice(msg string) {
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
	if s.bound.Schema != "" {
		detail["schema"] = s.bound.Schema
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
	switch {
	case errors.As(err, &own):
		return &pgErr{own.Code, own.Message}
	case errors.As(err, &refused):
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
