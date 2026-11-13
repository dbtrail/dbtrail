package pgcompare

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// SnapshotCopy is the copy of one snapshot directory, queried through the SQL
// sandbox with that snapshot's views installed.
type SnapshotCopy struct {
	runner   *sqlsandbox.Runner
	dir      string
	viewsSQL string
	schema   string
}

// OpenSnapshotCopy prepares the copy held in snapshotDir (one complete
// snapshot, <root>/<timestamp>). schema, when set, is where unqualified
// names resolve, as on the source.
func OpenSnapshotCopy(ctx context.Context, runner *sqlsandbox.Runner, snapshotDir, schema string) (*SnapshotCopy, error) {
	abs, err := filepath.Abs(snapshotDir)
	if err != nil {
		return nil, err
	}
	script, ok, err := views.GenerateSnapshotViews(ctx, abs, filepath.ToSlash(abs))
	if err != nil {
		return nil, fmt.Errorf("views for %s: %w", abs, err)
	}
	if !ok {
		return nil, fmt.Errorf("%s is not a snapshot directory with tables", abs)
	}
	return &SnapshotCopy{runner: runner, dir: abs, viewsSQL: script, schema: schema}, nil
}

// Query runs one statement on the copy.
func (c *SnapshotCopy) Query(ctx context.Context, stmt string, max int) (sqlcompare.Rows, bool, error) {
	res, err := c.runner.Run(ctx, sqlsandbox.Job{
		CopyDirs: []string{c.dir},
		ViewsSQL: c.viewsSQL,
		SQL:      stmt,
		Schema:   c.schema,
		Session:  sqlsandbox.Session{PostgresRules: true},
		Limits:   sqlsandbox.Limits{MaxRows: max},
	})
	if err != nil {
		return sqlcompare.Rows{}, false, classifyCopyError(err)
	}
	if res.Spool != nil {
		res.Spool.Close()
		return sqlcompare.Rows{}, true, nil
	}
	out := sqlcompare.Rows{}
	for _, col := range res.Columns {
		out.Columns = append(out.Columns, col.Name)
	}
	for _, r := range res.Rows {
		row := make([]*string, len(r))
		for i, v := range r {
			typ := ""
			if i < len(res.Columns) {
				typ = res.Columns[i].Type
			}
			row[i] = CellText(typ, v)
		}
		out.Rows = append(out.Rows, row)
	}
	return out, res.Truncated || res.TruncatedCells > 0, nil
}

// statementErrors are the engine's error kinds that are about the statement:
// it does not parse, names something the copy does not have, or asks for
// something the copy's types cannot do. Any other engine error (out of
// memory, a file that does not read, an internal error) is about the copy
// or the harness.
var statementErrors = []string{
	"Parser Error", "Binder Error", "Catalog Error", "Conversion Error",
	"Invalid Input Error", "Not implemented Error", "Out of Range Error",
	"Mismatch Type Error", "Invalid type Error",
}

// classifyCopyError says what a sandbox error means for the statement: its
// own error when it is about the statement (NOT_ON_COPY), a CutError when
// the answer could not be had whole, a FatalError otherwise.
func classifyCopyError(err error) error {
	var refused *sqlsandbox.RefusedError
	if errors.As(err, &refused) {
		return err
	}
	var query *sqlsandbox.QueryError
	if errors.As(err, &query) {
		for _, kind := range statementErrors {
			if strings.HasPrefix(query.Message, kind) {
				return err
			}
		}
		return &FatalError{Err: fmt.Errorf("the copy failed in a way that is not about the statement: %w", err)}
	}
	var timeout *sqlsandbox.TimeoutError
	if errors.As(err, &timeout) {
		return &CutError{Kind: "copy_timeout", Err: err}
	}
	if errors.Is(err, sqlsandbox.ErrResultTooLarge) {
		return &CutError{Kind: "copy_result_too_large", Err: err}
	}
	return &FatalError{Err: err}
}

// CellText writes one cell of the copy's answer the way PostgreSQL writes a
// value of that kind as text, or nil for NULL. typ is the copy's type name
// for the column (BOOLEAN, TIMESTAMP, TIMESTAMP WITH TIME ZONE, ...). This is
// the whole list of renderings the comparison allows for; a kind not listed
// here is compared as the copy wrote it.
func CellText(typ string, v any) *string {
	var s string
	switch x := v.(type) {
	case nil:
		return nil
	case bool:
		s = "f"
		if x {
			s = "t"
		}
	case json.Number:
		s = x.String()
		if typ == "DOUBLE" || typ == "FLOAT" {
			s = pgFloat(typ, s)
		}
	case string:
		s = x
		if typ == "BLOB" {
			s = pgBytea(x)
		}
		if strings.HasPrefix(typ, "TIMESTAMP") {
			if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
				s = pgTimestamp(t)
				if strings.Contains(typ, "TIME ZONE") || typ == "TIMESTAMPTZ" {
					s += "+00"
				}
			}
		}
	case []any:
		if elem, ok := strings.CutSuffix(typ, "[]"); ok {
			s = pgArray(elem, x)
			break
		}
		s = jsonText(x)
	default:
		s = jsonText(x)
	}
	return &s
}

func jsonText(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// pgArray writes a list cell as PostgreSQL writes an array (array_out): the
// elements between braces with commas, each as CellText writes a value of
// the element's type, NULL bare for a missing one, and between double
// quotes, with a backslash before a quote or a backslash, when the element
// is empty, spells NULL in any case, or holds a brace, a comma, a quote, a
// backslash or a blank.
func pgArray(elem string, list []any) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, v := range list {
		if i > 0 {
			b.WriteByte(',')
		}
		cell := CellText(elem, v)
		if cell == nil {
			b.WriteString("NULL")
			continue
		}
		if !pgArrayNeedsQuotes(*cell) {
			b.WriteString(*cell)
			continue
		}
		b.WriteByte('"')
		for _, r := range *cell {
			if r == '"' || r == '\\' {
				b.WriteByte('\\')
			}
			b.WriteRune(r)
		}
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func pgArrayNeedsQuotes(s string) bool {
	if s == "" || strings.EqualFold(s, "NULL") {
		return true
	}
	// array_out's own list: braces, the delimiter, a quote, a backslash,
	// and what its scanner calls a blank.
	return strings.ContainsAny(s, "{},\"\\ \t\n\r\v\f")
}

// pgFloat writes a floating point number the way PostgreSQL does: the
// shortest digits that read back as the same value, with an exponent below
// 1e-4 and from 1e15 up for a double precision, from 1e6 up for a real.
func pgFloat(typ, s string) string {
	bits := 64
	if typ == "FLOAT" {
		bits = 32
	}
	f, err := strconv.ParseFloat(s, bits)
	if err != nil {
		return s
	}
	top := 1e15
	if bits == 32 {
		top = 1e6
	}
	if abs := math.Abs(f); abs != 0 && (abs >= top || abs < 1e-4) {
		return strconv.FormatFloat(f, 'e', -1, bits)
	}
	return strconv.FormatFloat(f, 'f', -1, bits)
}

// pgBytea writes a BLOB cell as PostgreSQL writes a bytea: \x and hex
// digits. The sandbox hands a BLOB over as its text when the bytes are valid
// UTF-8 and as 0x and hex digits otherwise, so bytes that happen to spell
// "0x" and hex digits cannot be told from the bytes those digits name; they
// are read as the second.
func pgBytea(cell string) string {
	if rest, ok := strings.CutPrefix(cell, "0x"); ok {
		if _, err := hex.DecodeString(rest); err == nil {
			return `\x` + strings.ToLower(rest)
		}
	}
	return `\x` + hex.EncodeToString([]byte(cell))
}

// pgTimestamp is PostgreSQL's ISO text for an instant in UTC: seconds, then
// the fraction with its trailing zeros dropped, to microseconds.
func pgTimestamp(t time.Time) string {
	t = t.UTC()
	s := t.Format("2006-01-02 15:04:05")
	if us := t.Nanosecond() / 1000; us != 0 {
		s += strings.TrimRight(fmt.Sprintf(".%06d", us), "0")
	}
	return s
}
