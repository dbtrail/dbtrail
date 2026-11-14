package pgcompare

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/dbtrail/dbtrail/internal/pgtext"
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
// value of that kind as text, or nil for NULL (pgtext.Cell, which the
// PostgreSQL-protocol port sends to its clients too). typ is the copy's type
// name for the column. A kind pgtext does not list is compared as the copy
// wrote it.
func CellText(typ string, v any) *string { return pgtext.Cell(typ, v) }
