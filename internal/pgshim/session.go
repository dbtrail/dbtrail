package pgshim

import (
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
	if strings.TrimSpace(qstr) == "" {
		s.be.Send(&pgproto3.EmptyQueryResponse{})
		return s.readyFlush()
	}

	cols, rows, tag, perr := s.resolve(qstr)
	if perr != nil {
		s.errorResponse(perr.code, perr.msg)
		return s.readyFlush()
	}
	// cols == nil marks a non-row reply (a handshake/setup probe like SET): no
	// RowDescription, just the CommandComplete tag. A real query — even one that
	// resolves to zero rows (row absent at AsOf) — has a non-nil cols and emits
	// the RowDescription so the client sees the real column header.
	if cols != nil {
		s.rowDescription(cols)
		for _, r := range rows {
			s.dataRow(r)
		}
	}
	s.commandComplete(tag)
	return s.readyFlush()
}

// resolve parses and runs a query through the shared engine, returning the
// column list, the (zero or one) rendered rows, the CommandComplete tag, and a
// query-level error. Single-row AS OF only; full-table and _diff are refused.
func (s *session) resolve(qstr string) (cols []string, rows [][][]byte, tag string, perr *pgErr) {
	q, err := shim.Parse(qstr, s.currentDB)
	if err != nil {
		if !errors.Is(err, shim.ErrNotTimeTravel) {
			// Recognised as a time-travel query but malformed (bad AS OF literal,
			// missing schema, bad grammar) → syntax_error.
			return nil, nil, "", &pgErr{"42601", err.Error()}
		}
		// Not a time-travel query. Tolerate the handful of setup statements a
		// client fires on connect; reject anything else (we are not a general
		// PostgreSQL engine).
		if t, ok := probeReply(qstr); ok {
			return nil, nil, t, nil
		}
		return nil, nil, "", &pgErr{"0A000", fmt.Sprintf(
			"this endpoint serves only _flashback / _snapshot AS OF time-travel queries; got: %s",
			strings.TrimSpace(qstr))}
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
		return nil, nil, "", &pgErr{"42501", msg}
	}

	// PK-column validation is shared with the MySQL front-end (#296/#821): a
	// WHERE whose column is not the table's single-column PK is refused so a
	// non-PK filter cannot silently return the wrong row.
	if msg, reject := s.h.PKColumnCheck(q); reject {
		return nil, nil, "", &pgErr{"42601", msg}
	}

	switch q.Type {
	case shim.TypeDiff:
		return nil, nil, "", &pgErr{"0A000",
			"_diff is not supported over the PostgreSQL wire front-end; use `bintrail-pg reconstruct --history` for a row's per-event history"}
	case shim.TypeFlashback, shim.TypeSnapshot:
		if q.PKColumn == "" {
			return nil, nil, "", &pgErr{"0A000", fullTableRefusalMsg}
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
		s.h.AuditResolve(q, 1)
		return cols, [][][]byte{cells}, "SELECT 1", nil
	default:
		return nil, nil, "", &pgErr{"XX000", fmt.Sprintf("unsupported query type: %s", q.Type)}
	}
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

func (s *session) rowDescription(cols []string) {
	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, c := range cols {
		fields[i] = pgproto3.FieldDescription{
			Name:         []byte(c),
			DataTypeOID:  pgtypeText,
			DataTypeSize: -1, // variable-length
			TypeModifier: -1,
			Format:       pgproto3.TextFormat,
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

// readyFlush sends ReadyForQuery(idle) and flushes — the required end of every
// simple-query cycle, success or failure.
func (s *session) readyFlush() error {
	s.be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return s.be.Flush()
}
