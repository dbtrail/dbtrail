package pgshim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	cols, rows, tag, perr := s.run(qstr)
	if perr != nil {
		s.fail(perr)
		return s.readyFlush()
	}
	// cols == nil marks a non-row reply (a handshake/setup probe like SET): no
	// RowDescription, just the CommandComplete tag. A real query — even one that
	// resolves to zero rows (row absent at AsOf) — has a non-nil cols and emits
	// the RowDescription so the client sees the real column header.
	if cols != nil {
		s.rowDescription(cols, nil)
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
func (s *session) run(qstr string) (cols []string, rows [][][]byte, tag string, perr *pgErr) {
	if perr := s.abortedRefusal(qstr); perr != nil {
		return nil, nil, "", perr
	}
	cols, rows, tag, perr = s.resolve(qstr)
	if perr != nil {
		return nil, nil, "", perr
	}
	switch tag {
	case "BEGIN":
		if s.txStatus() != 'I' {
			s.warning("25001", "there is already a transaction in progress")
		} else {
			s.tx = 'T'
		}
	case "COMMIT", "ROLLBACK":
		switch s.txStatus() {
		case 'I':
			s.warning("25P01", "there is no transaction in progress")
		case 'E':
			// A failed block is rolled back, whatever ended it.
			tag = "ROLLBACK"
		}
		s.tx = 'I'
		s.dropPortals()
	}
	return cols, rows, tag, nil
}

// abortedRefusal refuses a statement in a failed transaction block other
// than the COMMIT or ROLLBACK that ends it.
func (s *session) abortedRefusal(qstr string) *pgErr {
	if s.txStatus() != 'E' {
		return nil
	}
	if tag, ok := probeReply(qstr); ok && (tag == "COMMIT" || tag == "ROLLBACK") {
		return nil
	}
	return &pgErr{"25P02", "current transaction is aborted, commands ignored until end of transaction block"}
}

// fail sends a statement's error. In a transaction block the block fails
// with it.
func (s *session) fail(perr *pgErr) {
	s.errorResponse(perr.code, perr.msg)
	if s.txStatus() == 'T' {
		s.tx = 'E'
	}
}

// warning sends a WARNING notice.
func (s *session) warning(code, msg string) {
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
	// kindRefused is refused before anything runs.
	kindRefused
)

// classified is what classify says about a statement.
type classified struct {
	kind stmtKind
	// q is the time-travel statement, for kindTimeTravel.
	q shim.TimeTravelQuery
	// tag answers a kindProbe; perr refuses a kindProbe or a kindRefused.
	tag  string
	perr *pgErr
	// timeTravel says the time-travel parser reads the text (well formed or
	// not), which decides how an argument is written into it.
	timeTravel bool
}

// classify decides which way resolve answers qstr, without running it. It is
// the one place that decides: resolve acts on it, and the extended protocol
// asks it before a statement runs (to say whether it returns rows, and how
// its arguments are written).
func (s *session) classify(qstr string) classified {
	q, err := shim.Parse(qstr, s.currentDB)
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
		// Not a time-travel query. Tolerate the handful of setup statements a
		// client fires on connect; reject anything else (we are not a general
		// PostgreSQL engine).
		if s.bound != nil {
			if t, refusal, ok := boundProbe(qstr); ok {
				return classified{kind: kindProbe, tag: t, perr: refusal}
			}
			return classified{kind: kindFree}
		}
		if t, ok := probeReply(qstr); ok {
			return classified{kind: kindProbe, tag: t}
		}
		return classified{kind: kindRefused, perr: &pgErr{"0A000", fmt.Sprintf(
			"this endpoint serves only _flashback / _snapshot AS OF time-travel queries; got: %s",
			strings.TrimSpace(qstr))}}
	}
	return classified{kind: kindTimeTravel, q: q, timeTravel: true}
}

// resolve parses and runs a query through the shared engine, returning the
// column list, the (zero or one) rendered rows, the CommandComplete tag, and a
// query-level error. Single-row AS OF only; full-table and _diff are refused.
func (s *session) resolve(qstr string) (cols []string, rows [][][]byte, tag string, perr *pgErr) {
	c := s.classify(qstr)
	switch c.kind {
	case kindRefused:
		return nil, nil, "", c.perr
	case kindProbe:
		if c.perr != nil {
			return nil, nil, "", c.perr
		}
		return nil, nil, c.tag, nil
	case kindFree:
		return s.runFree(qstr)
	}
	return s.resolveTimeTravel(c.q)
}

// timeTravelGates are the refusals a time-travel statement meets before
// anything is read or said about its table, in this order. A Describe asks
// them too, before it names the table's columns.
func (s *session) timeTravelGates(q shim.TimeTravelQuery) *pgErr {
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
	// are, which sending them again would not change.
	if (q.Type == shim.TypeFlashback || q.Type == shim.TypeSnapshot) && q.PKColumn != "" {
		if refusal := s.admitResult(); refusal != nil {
			return nil, nil, "", refusal
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
	if perr := s.timeTravelGates(q); perr != nil {
		return nil, nil, "", perr
	}
	// A time-travel answer is held like any other while it is sent, so the
	// port's result budget is asked for it too (#2804): once the statement
	// passed every refusal of what it is, before the index is read for it.
	if refusal := s.admitResult(); refusal != nil {
		return nil, nil, "", refusal
	}
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
		// Residual unchanged-TOAST marker (#592) — refuse rather than serve
		// the marker's JSON as a value.
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
// than serialised. Byte values go on the wire verbatim (the conservative
// all-text encoding), so a bytea/blob round-trips its exact bytes.
func textCell(col string, v any) ([]byte, error) {
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
		return []byte(x.UTC().Format("2006-01-02 15:04:05")), nil
	default:
		return []byte(fmt.Sprint(x)), nil
	}
}

// probeReply tolerates the setup statements a client may fire on connect that
// are not time-travel queries, so the connection is not torn down by an error
// the operator did not type. Deliberately narrow: an unrecognised statement
// falls through to the "only time-travel queries" rejection.
func probeReply(qstr string) (tag string, ok bool) {
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(qstr), ";")))
	switch {
	case strings.HasPrefix(q, "set "):
		return "SET", true
	case q == "begin", strings.HasPrefix(q, "begin "):
		return "BEGIN", true
	case q == "commit":
		return "COMMIT", true
	case q == "rollback":
		return "ROLLBACK", true
	}
	return "", false
}

// --- wire writers ---------------------------------------------------------

// rowDescription describes the columns. formats are a portal's result
// formats from its Bind: none (text), one for every column, or one each.
func (s *session) rowDescription(cols []string, formats []int16) {
	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, c := range cols {
		format := int16(pgproto3.TextFormat)
		switch len(formats) {
		case 1:
			format = formats[0]
		case len(cols):
			format = formats[i]
		}
		fields[i] = pgproto3.FieldDescription{
			Name:         []byte(c),
			DataTypeOID:  pgtypeText,
			DataTypeSize: -1, // variable-length
			TypeModifier: -1,
			Format:       format,
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
// failure.
func (s *session) readyFlush() error {
	s.be.Send(&pgproto3.ReadyForQuery{TxStatus: s.txStatus()})
	return s.be.Flush()
}
