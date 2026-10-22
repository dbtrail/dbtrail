package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// Cut is a quiet point of a run: the generator is paused, capture has
// indexed everything before Position, and Rows is every table of the source
// read there. At is the server clock at the cut, to the second, strictly
// after the previous cut's.
type Cut struct {
	Step     int
	Position Position
	At       time.Time
	Schema   string
	Tables   []Table
	Rows     map[string]TableRows
	// Ops are the ops of the step that ended at this cut.
	Ops []Op
}

// HadPKChange reports whether an op of the step moved a row to another key.
func (c *Cut) HadPKChange() bool { return c.HadPKChangeOn("") }

// HadPKChangeOn reports whether an op of the step moved a row of table to
// another key; "" means any table.
func (c *Cut) HadPKChangeOn(table string) bool {
	for _, op := range c.Ops {
		if table != "" && op.Table != table {
			continue
		}
		for _, ch := range op.Changes {
			if ch.Kind == KindPKUpdate {
				return true
			}
		}
	}
	return false
}

// Step is what a run does to the copy at a cut, before reading it: a
// snapshot cut, a refresh with table deltas, later a tail laid by the port's
// keeper. A step knows nothing of the oracle; the read after it must equal
// the source. Weight is its share of the draw between steps; the first cut
// of a run always takes the first step of the list.
type Step interface {
	Name() string
	Weight() int
	Apply(ctx context.Context, cut *Cut) error
}

// Refuser is a Step that can refuse a cut for a reason the generator
// caused, which the run then answers with its Fallback step instead of
// failing. The only such reason today: the fold refuses a window with a
// PK-changing UPDATE and tells the operator to take a new snapshot.
type Refuser interface {
	Refused(err error, cut *Cut) bool
}

// SnapshotStep writes a full snapshot of the source rows at the cut, the way
// a dump lands under the baselines root: one Parquet file per table anchored
// at the cut's binlog position, an empty delta pair beside each, and the
// success marker. It is the no-pipeline reference the chain starts from.
type SnapshotStep struct {
	Root   string
	Source *sql.DB
	W      int
	ddl    map[string]string
}

func (s *SnapshotStep) Name() string { return "snapshot" }
func (s *SnapshotStep) Weight() int  { return s.W }

func (s *SnapshotStep) Apply(ctx context.Context, cut *Cut) error {
	dir := filepath.Join(s.Root, reconstruct.SnapshotDirName(cut.At))
	if _, err := os.Stat(dir); err == nil {
		return fmt.Errorf("snapshot at %s: directory %s exists", cut.At.Format(time.RFC3339), dir)
	}
	ts := cut.At.UTC().Format(time.RFC3339)
	for _, t := range cut.Tables {
		ddl, err := s.createTable(ctx, cut.Schema, t.Name)
		if err != nil {
			return err
		}
		cols, err := baseline.ParseSchemaText(ddl)
		if err != nil {
			return fmt.Errorf("parse the CREATE TABLE of %s: %w", t.Name, err)
		}
		path := filepath.Join(dir, cut.Schema, t.Name+".parquet")
		w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
			Compression: "zstd", RowGroupSize: 1000,
			Metadata: map[string]string{
				baseline.MetaKeyCreateTableSQL:    ddl,
				baseline.MetaKeyBinlogFile:        cut.Position.File,
				baseline.MetaKeyBinlogPos:         strconv.FormatUint(cut.Position.Pos, 10),
				baseline.MetaKeySnapshotTimestamp: ts,
				baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
				baseline.MetaKeyLastDumpAt:        ts,
				baseline.MetaKeyFoldGeneration:    "0",
				baseline.MetaKeyMydumperFormat:    "sql",
			},
		})
		if err != nil {
			return fmt.Errorf("snapshot writer for %s: %w", t.Name, err)
		}
		rows := cut.Rows[t.Name]
		if err := writeRows(w, cols, rows); err != nil {
			w.Close()
			return fmt.Errorf("snapshot of %s: %w", t.Name, err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("snapshot of %s: %w", t.Name, err)
		}
	}
	if err := baseline.WriteEmptyTableDeltas(dir); err != nil {
		return err
	}
	return baseline.WriteSuccessMarker(dir)
}

// writeRows writes the source rows in the writer's column order (the
// CREATE TABLE's, which is the source's SELECT * order; matched by name so a
// reordering on either side cannot shift a value into another column).
func writeRows(w *baseline.Writer, cols []baseline.Column, rows TableRows) error {
	idx := make([]int, len(cols))
	for i, c := range cols {
		idx[i] = -1
		for j, name := range rows.Columns {
			if strings.EqualFold(name, c.Name) {
				idx[i] = j
			}
		}
		if idx[i] < 0 {
			return fmt.Errorf("column %s of the CREATE TABLE is not in the source read (%v)", c.Name, rows.Columns)
		}
	}
	for r := range rows.Rows {
		values := make([]string, len(cols))
		nulls := make([]bool, len(cols))
		for i, j := range idx {
			if rows.Nulls[r][j] {
				nulls[i] = true
				continue
			}
			values[i] = rows.Rows[r][j]
		}
		if err := w.WriteRow(values, nulls); err != nil {
			return err
		}
	}
	return nil
}

func (s *SnapshotStep) createTable(ctx context.Context, schema, table string) (string, error) {
	if ddl, ok := s.ddl[table]; ok {
		return ddl, nil
	}
	var name, ddl string
	if err := s.Source.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)).Scan(&name, &ddl); err != nil {
		return "", fmt.Errorf("SHOW CREATE TABLE %s: %w", table, err)
	}
	if s.ddl == nil {
		s.ddl = map[string]string{}
	}
	s.ddl[table] = ddl + ";\n"
	return s.ddl[table], nil
}

// RefreshStep folds the newest snapshot forward to the cut with table
// deltas, the daemon's scheduled refresh: reconstruct.ReconstructTables with
// TableDeltas on, into a new snapshot directory named by the cut.
type RefreshStep struct {
	IndexDSN string
	Root     string
	W        int
}

func (s *RefreshStep) Name() string { return "refresh" }
func (s *RefreshStep) Weight() int  { return s.W }

func (s *RefreshStep) Apply(ctx context.Context, cut *Cut) error {
	tables := make([]string, len(cut.Tables))
	for i, t := range cut.Tables {
		tables[i] = cut.Schema + "." + t.Name
	}
	_, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
		IndexDSN: s.IndexDSN, BaselineSrc: s.Root, Tables: tables, At: cut.At,
		OutputDir: s.Root, OutputFormat: reconstruct.OutputFormatParquet, TableDeltas: true, Parallelism: 2,
	})
	if err != nil {
		return &RefreshError{Failures: failures, Err: err}
	}
	return nil
}

// RefreshError is a refused or failed refresh with its per-table failures.
type RefreshError struct {
	Failures []reconstruct.TableFailure
	Err      error
}

func (e *RefreshError) Error() string { return e.Err.Error() }
func (e *RefreshError) Unwrap() error { return e.Err }

// Refused: the fold refuses a window holding a PK-changing UPDATE and says
// to take a new snapshot (reconstruct's #782 guard). A refresh is refused
// only when EVERY failed table failed that way and the step's ops moved a
// row on that table; any other failure, or that error on a table whose ops
// changed no key, is a failure of the run.
func (s *RefreshStep) Refused(err error, cut *Cut) bool {
	var re *RefreshError
	if !errors.As(err, &re) || len(re.Failures) == 0 {
		return false
	}
	for _, f := range re.Failures {
		if !strings.Contains(f.Err.Error(), "PK-changing UPDATE") || !cut.HadPKChangeOn(f.Table) {
			return false
		}
	}
	return true
}

// PortViews is the port's read of the copy under root, as `bintrail views`
// and the console's SQL on the copy build it: the newest complete snapshot,
// each table marked with the delta chain beside it and the column types of
// its footer, rendered to the views SQL pinned to that snapshot. wallClock
// is what the console sets for a statement under a session time zone other
// than UTC (the DATETIME columns cast to zone-less timestamps); under UTC
// it leaves them as the files hold them. It returns the SQL and the
// snapshot directory it reads.
func PortViews(ctx context.Context, root string, now time.Time, wallClock bool) (sqlText, snapshotDir string, err error) {
	files, _, err := reconstruct.ListBaselinesReport(ctx, root)
	if err != nil {
		return "", "", fmt.Errorf("list the snapshots under %s: %w", root, err)
	}
	if len(files) == 0 {
		return "", "", fmt.Errorf("no complete snapshot under %s", root)
	}
	newest := files[0].SnapshotTime
	in := views.Input{GeneratedAt: now, Version: "oracle", BaselineSource: root, BaselineSnapshot: newest, Follow: views.FollowNone}
	for _, f := range files {
		if !f.SnapshotTime.Equal(newest) {
			continue
		}
		in.Baselines = append(in.Baselines, views.BaselineTable{
			Schema: f.Schema, Table: f.Table, Path: f.Path, Rel: f.Schema + "/" + f.Table + ".parquet",
		})
		snapshotDir = filepath.Dir(filepath.Dir(f.Path))
	}
	if err := views.MarkTableDeltas(ctx, in.Baselines); err != nil {
		return "", "", err
	}
	footers, err := baseline.TableFootersFor(ctx, in.BaselinePaths())
	if err != nil {
		return "", "", fmt.Errorf("read the column types of the snapshot: %w", err)
	}
	in.ApplyFooters(footers)
	for i := range in.Baselines {
		if !in.Baselines[i].SchemaKnown {
			return "", "", fmt.Errorf("%s.%s: the snapshot does not record the column types", in.Baselines[i].Schema, in.Baselines[i].Table)
		}
		in.Baselines[i].WallClockDatetimes = wallClock
	}
	return views.Generate(in), snapshotDir, nil
}
