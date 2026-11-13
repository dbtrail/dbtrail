package pgcompare

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		Limits:   sqlsandbox.Limits{MaxRows: max},
	})
	if err != nil {
		// A refusal or an engine error is about the statement. Anything
		// else (the worker died, no slot, no local copy) is about the
		// harness and must not be filed as the statement's answer.
		var refused *sqlsandbox.RefusedError
		var query *sqlsandbox.QueryError
		if errors.As(err, &refused) || errors.As(err, &query) {
			return sqlcompare.Rows{}, false, err
		}
		return sqlcompare.Rows{}, false, &FatalError{Err: err}
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
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	case string:
		s = x
		if strings.HasPrefix(typ, "TIMESTAMP") {
			if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
				s = pgTimestamp(t)
				if strings.Contains(typ, "TIME ZONE") || typ == "TIMESTAMPTZ" {
					s += "+00"
				}
			}
		}
	default:
		b, err := json.Marshal(x)
		if err != nil {
			s = fmt.Sprint(x)
		} else {
			s = string(b)
		}
	}
	return &s
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
