//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

type epochCol struct{ name, ctype string }

// epochSnap is one schema snapshot, with offsets relative to the baseline.
type epochSnap struct {
	taken    time.Duration  // schema_snapshots.snapshot_time
	detected *time.Duration // schema_changes.detected_at of the DDL it was taken for; nil: no row
	cols     []epochCol
}

func ddlAt(d time.Duration) *time.Duration { return &d }

// foldWithSnapshots publishes a Parquet fold of shop.t at base+target from a
// baseline with baselineCols taken at base, the given snapshot history, and one
// INSERT at base+eventAt.
func foldWithSnapshots(t *testing.T, baselineCols []epochCol, snaps []epochSnap, eventAt, target time.Duration) ([]reconstruct.TableFailure, error) {
	t.Helper()
	ctx := context.Background()
	const schema, table = "shop", "t"
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Hour)
	stamp := func(d time.Duration) string { return base.Add(d).Format("2006-01-02 15:04:05") }

	for i, s := range snaps {
		for j, c := range s.cols {
			key, nullable := "", "YES"
			if c.name == "id" {
				key, nullable = "PRI", "NO"
			}
			testutil.MustExec(t, db, `INSERT INTO schema_snapshots
				(snapshot_id, snapshot_time, schema_name, table_name, column_name,
				 ordinal_position, column_key, data_type, column_type, is_nullable)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				i+1, stamp(s.taken), schema, table, c.name, j+1, key, strings.SplitN(c.ctype, "(", 2)[0], c.ctype, nullable)
		}
		if s.detected != nil {
			testutil.MustExec(t, db, `INSERT INTO schema_changes
				(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
				VALUES (?, 'binlog.000001', 150, ?, ?, 'ALTER TABLE', 'ALTER TABLE t', ?)`,
				stamp(*s.detected), schema, table, i+1)
		}
	}
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, stamp(eventAt), nil,
		schema, table, 1, "3", nil, nil, []byte(`{"id":3,"c":5}`))

	defs := make([]string, 0, len(baselineCols))
	for _, c := range baselineCols {
		null := "DEFAULT NULL"
		if c.name == "id" {
			null = "NOT NULL"
		}
		defs = append(defs, fmt.Sprintf("  `%s` %s %s", c.name, c.ctype, null))
	}
	createSQL := "CREATE TABLE `t` (\n" + strings.Join(defs, ",\n") + ",\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	root := t.TempDir()
	snapDir := filepath.Join(root, strings.ReplaceAll(base.Format(time.RFC3339), ":", "-"))
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	w, err := baseline.NewWriter(filepath.Join(snapDir, schema, table+".parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL:    createSQL,
			baseline.MetaKeyBinlogFile:        "binlog.000001",
			baseline.MetaKeyBinlogPos:         "4",
			baseline.MetaKeySnapshotTimestamp: base.Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	row, nulls := make([]string, len(cols)), make([]bool, len(cols))
	for i := range row {
		row[i] = "1"
	}
	if err := w.WriteRow(row, nulls); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
	_, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:  root,
		Tables:       []string{schema + "." + table},
		At:           base.Add(target),
		OutputDir:    root,
		OutputFormat: reconstruct.OutputFormatParquet,
	})
	return failures, err
}

// TestFold_schemaInEffectAtTheTarget is #1667: a fold compares the baseline's
// CREATE TABLE against the schema in effect at its target, placed by when the
// DDL ran on the source, not by when capture recorded its snapshot. Names are
// compared with it only when it was read after the baseline; otherwise with the
// latest, the only record of a DDL between the two that got no snapshot.
func TestFold_schemaInEffectAtTheTarget(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	intCols := []epochCol{{"id", "int"}, {"c", "int"}}
	bigCols := []epochCol{{"id", "int"}, {"c", "bigint"}}
	extraCols := []epochCol{{"id", "int"}, {"c", "int"}, {"extra", "int"}}
	idOnly := []epochCol{{"id", "int"}}

	refuses := func(t *testing.T, failures []reconstruct.TableFailure, err error, want string) {
		t.Helper()
		if err == nil || len(failures) != 1 || !errors.Is(failures[0].Err, reconstruct.ErrSchemaChanged) ||
			!strings.Contains(failures[0].Err.Error(), want) {
			t.Fatalf("fold: err=%v failures=%+v; want the schema-change refusal naming %q", err, failures, want)
		}
	}
	publishes := func(t *testing.T, failures []reconstruct.TableFailure, err error) {
		t.Helper()
		if err != nil || len(failures) != 0 {
			t.Fatalf("fold: err=%v failures=%+v; want it published", err, failures)
		}
	}

	t.Run("capture behind: a type change that ran before the target is seen though its snapshot was recorded after", func(t *testing.T) {
		failures, err := foldWithSnapshots(t, intCols, []epochSnap{
			{-time.Minute, nil, intCols},
			{100 * time.Second, ddlAt(20 * time.Second), bigCols},
		}, 40*time.Second, 60*time.Second)
		refuses(t, failures, err, "c (int -> bigint)")
	})
	t.Run("control: a snapshot with no recorded DDL counts from when it was taken", func(t *testing.T) {
		failures, err := foldWithSnapshots(t, intCols, []epochSnap{
			{-time.Minute, nil, intCols},
			{100 * time.Second, nil, bigCols},
		}, 40*time.Second, 60*time.Second)
		publishes(t, failures, err)
	})
	t.Run("a snapshot read after the baseline is compared though its DDL is dated before it", func(t *testing.T) {
		// Clock skew: the source dates the DDL before the host read the
		// baseline. The snapshot's content is from when it was taken.
		failures, err := foldWithSnapshots(t, intCols, []epochSnap{
			{-time.Hour, nil, intCols},
			{100 * time.Second, ddlAt(-30 * time.Second), bigCols},
		}, 40*time.Second, 60*time.Second)
		refuses(t, failures, err, "c (int -> bigint)")
	})
	t.Run("a snapshot taken in the same second as the baseline is compared", func(t *testing.T) {
		failures, err := foldWithSnapshots(t, intCols, []epochSnap{
			{-time.Hour, nil, intCols},
			{0, nil, bigCols},
		}, 10*time.Second, 30*time.Second)
		refuses(t, failures, err, "c (int -> bigint)")
	})
	t.Run("a column added after the target still refuses when the snapshot in effect predates the baseline", func(t *testing.T) {
		failures, err := foldWithSnapshots(t, intCols, []epochSnap{
			{-time.Minute, nil, intCols},
			{60 * time.Second, ddlAt(60 * time.Second), extraCols},
		}, 10*time.Second, 30*time.Second)
		refuses(t, failures, err, "added since: extra")
	})
	t.Run("a DDL between the baseline and the target with no snapshot of its own is caught by a later one", func(t *testing.T) {
		// File mode without --source-dsn, a failed snapshot or a DDL the
		// parser missed adds d at +15m; a manual snapshot at +2h records it.
		failures, err := foldWithSnapshots(t, intCols, []epochSnap{
			{-time.Hour, nil, intCols},
			{2 * time.Hour, nil, []epochCol{{"id", "int"}, {"c", "int"}, {"d", "int"}}},
		}, 10*time.Minute, 30*time.Minute)
		refuses(t, failures, err, "added since: d")
	})
	t.Run("names follow a snapshot taken after the baseline and in effect at the target, not a later one", func(t *testing.T) {
		failures, err := foldWithSnapshots(t, intCols, []epochSnap{
			{-time.Minute, nil, intCols},
			{20 * time.Second, ddlAt(20 * time.Second), bigCols},
			{60 * time.Second, ddlAt(60 * time.Second), []epochCol{{"id", "int"}, {"c", "bigint"}, {"extra", "int"}}},
		}, 10*time.Second, 30*time.Second)
		refuses(t, failures, err, "added since: none")
	})
	t.Run("a column added before the target still refuses", func(t *testing.T) {
		failures, err := foldWithSnapshots(t, intCols, []epochSnap{
			{-time.Minute, nil, intCols},
			{20 * time.Second, ddlAt(20 * time.Second), extraCols},
		}, 10*time.Second, 30*time.Second)
		refuses(t, failures, err, "added since: extra")
	})
	t.Run("a stale snapshot with nothing newer still refuses", func(t *testing.T) {
		failures, err := foldWithSnapshots(t, intCols, []epochSnap{
			{-time.Minute, nil, idOnly},
		}, 10*time.Second, 30*time.Second)
		refuses(t, failures, err, "gone since: c")
	})
	t.Run("a snapshot taken after the target to fix a stale one lets a restore to before it publish", func(t *testing.T) {
		failures, err := foldWithSnapshots(t, intCols, []epochSnap{
			{-time.Minute, nil, idOnly},
			{60 * time.Second, nil, intCols},
		}, 10*time.Second, 30*time.Second)
		publishes(t, failures, err)
	})
}
