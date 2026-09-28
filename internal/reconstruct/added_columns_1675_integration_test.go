//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// addedColumnFold is one fold of shop.t (id, c) from a baseline at base,
// anchored at binlog.000001:4, with a first snapshot an hour before it and a
// later one that has the column d. Offsets are relative to base.
type addedColumnFold struct {
	target time.Duration
	// The later snapshot, and the DDL row that records where d came from.
	snapshotAt time.Duration
	ddl        *addedColumnDDL
	// Row changes captured besides the INSERT on shop.t at +20s
	// (binlog.000001:100-200).
	events            []addedColumnEvent
	dropSchemaChanges bool
}

type addedColumnDDL struct {
	at           time.Duration
	pos          uint64
	table, query string
	noSnapshot   bool
}

type addedColumnEvent struct {
	at         time.Duration
	start, end uint64
}

func (f addedColumnFold) run(t *testing.T) ([]reconstruct.TableFailure, error) {
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

	snapshot := func(id int, at time.Duration, cols ...string) {
		for i, c := range cols {
			key, nullable := "", "YES"
			if c == "id" {
				key, nullable = "PRI", "NO"
			}
			testutil.MustExec(t, db, `INSERT INTO schema_snapshots
				(snapshot_id, snapshot_time, schema_name, table_name, column_name,
				 ordinal_position, column_key, data_type, column_type, is_nullable)
				VALUES (?, ?, ?, ?, ?, ?, ?, 'int', 'int', ?)`,
				id, stamp(at), schema, table, c, i+1, key, nullable)
		}
	}
	snapshot(1, -time.Hour, "id", "c")
	snapshot(2, f.snapshotAt, "id", "c", "d")
	if f.ddl != nil {
		var snap any = 2
		if f.ddl.noSnapshot {
			snap = nil
		}
		testutil.MustExec(t, db, `INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
			VALUES (?, 'binlog.000001', ?, ?, ?, 'ALTER TABLE', ?, ?)`,
			stamp(f.ddl.at), f.ddl.pos, schema, f.ddl.table, f.ddl.query, snap)
	}
	if f.dropSchemaChanges {
		testutil.MustExec(t, db, "DROP TABLE schema_changes")
	}
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, stamp(20*time.Second), nil,
		schema, table, 1, "3", nil, nil, []byte(`{"id":3,"c":5}`))
	// The other changes are on another table: they place the cut and are not
	// folded into shop.t.
	for _, e := range f.events {
		testutil.InsertEvent(t, db, "binlog.000001", e.start, e.end, stamp(e.at), nil,
			schema, "other", 1, "1", nil, nil, []byte(`{"id":1}`))
	}

	createSQL := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `c` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
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
	if err := w.WriteRow([]string{"1", "1"}, make([]bool, 2)); err != nil {
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
		At:           base.Add(f.target),
		OutputDir:    root,
		OutputFormat: reconstruct.OutputFormatParquet,
	})
	return failures, err
}

// TestFold_beforeAnAddedColumn is #1675 through the real fold: a restore to
// before a column was added publishes when a recorded ALTER TABLE after the
// target, by time and by position, adds it, and refuses when nothing places
// the column.
func TestFold_beforeAnAddedColumn(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	const addD = "ALTER TABLE t ADD COLUMN d INT"
	// A change on another table between the target and the DDL: the first
	// event past the target, so the cut lands at its start, before the DDL.
	between := addedColumnEvent{at: 40 * time.Second, start: 300, end: 400}
	after := addedColumnEvent{at: 90 * time.Second, start: 600, end: 700}

	refuses := func(t *testing.T, failures []reconstruct.TableFailure, err error, want ...string) {
		t.Helper()
		if err == nil || len(failures) != 1 || !errors.Is(failures[0].Err, reconstruct.ErrSchemaChanged) {
			t.Fatalf("fold: err=%v failures=%+v; want the schema-change refusal", err, failures)
		}
		for _, w := range append(want, "added since: d") {
			if !strings.Contains(failures[0].Err.Error(), w) {
				t.Errorf("the refusal %q does not say %q", failures[0].Err, w)
			}
		}
	}
	publishes := func(t *testing.T, failures []reconstruct.TableFailure, err error) {
		t.Helper()
		if err != nil || len(failures) != 0 {
			t.Fatalf("fold: err=%v failures=%+v; want it published", err, failures)
		}
	}

	t.Run("a: the column was added after the target, with a snapshot", func(t *testing.T) {
		failures, err := addedColumnFold{target: 30 * time.Second, snapshotAt: 60 * time.Second,
			ddl:    &addedColumnDDL{at: 60 * time.Second, pos: 500, table: "t", query: addD},
			events: []addedColumnEvent{between, after}}.run(t)
		publishes(t, failures, err)
	})
	t.Run("a: nothing was captured after the DDL", func(t *testing.T) {
		// No event is past the target, so the cut is the end of the newest one.
		failures, err := addedColumnFold{target: 30 * time.Second, snapshotAt: 60 * time.Second,
			ddl: &addedColumnDDL{at: 60 * time.Second, pos: 500, table: "t", query: addD}}.run(t)
		publishes(t, failures, err)
	})
	t.Run("b: the DDL left no snapshot and a manual one came later", func(t *testing.T) {
		failures, err := addedColumnFold{target: 30 * time.Second, snapshotAt: time.Hour,
			ddl:    &addedColumnDDL{at: 60 * time.Second, pos: 500, table: "t", query: addD, noSnapshot: true},
			events: []addedColumnEvent{between, after}}.run(t)
		publishes(t, failures, err)
	})
	t.Run("c: the DDL ran before the target and left neither a snapshot nor a row", func(t *testing.T) {
		failures, err := addedColumnFold{target: 30 * time.Minute, snapshotAt: 2 * time.Hour,
			events: []addedColumnEvent{{at: 40 * time.Minute, start: 300, end: 400}}}.run(t)
		refuses(t, failures, err, "no ALTER TABLE recorded after the target adds d")
	})
	t.Run("the target is after the DDL", func(t *testing.T) {
		failures, err := addedColumnFold{target: 2 * time.Minute, snapshotAt: 60 * time.Second,
			ddl:    &addedColumnDDL{at: 60 * time.Second, pos: 500, table: "t", query: addD},
			events: []addedColumnEvent{between, after}}.run(t)
		refuses(t, failures, err, "is not after the target by both its time and its binlog position")
	})
	t.Run("g: the target is in the DDL's second", func(t *testing.T) {
		failures, err := addedColumnFold{target: 60 * time.Second, snapshotAt: 60 * time.Second,
			ddl:    &addedColumnDDL{at: 60 * time.Second, pos: 500, table: "t", query: addD},
			events: []addedColumnEvent{between, after}}.run(t)
		refuses(t, failures, err, "is not after the target by both its time and its binlog position")
	})
	t.Run("g: no change was captured between the target and the DDL, so the cut is past it", func(t *testing.T) {
		failures, err := addedColumnFold{target: 30 * time.Second, snapshotAt: 60 * time.Second,
			ddl:    &addedColumnDDL{at: 60 * time.Second, pos: 500, table: "t", query: addD},
			events: []addedColumnEvent{after}}.run(t)
		refuses(t, failures, err, "(the cut is binlog.000001:600)")
	})
	t.Run("e: the statement also drops a column", func(t *testing.T) {
		failures, err := addedColumnFold{target: 30 * time.Second, snapshotAt: 60 * time.Second,
			ddl:    &addedColumnDDL{at: 60 * time.Second, pos: 500, table: "t", query: addD + ", DROP COLUMN x"},
			events: []addedColumnEvent{between, after}}.run(t)
		refuses(t, failures, err, "cannot be read as only adding columns")
	})
	t.Run("f: the statement is on another table", func(t *testing.T) {
		failures, err := addedColumnFold{target: 30 * time.Second, snapshotAt: 60 * time.Second,
			ddl:    &addedColumnDDL{at: 60 * time.Second, pos: 500, table: "u", query: "ALTER TABLE u ADD COLUMN d INT"},
			events: []addedColumnEvent{between, after}}.run(t)
		refuses(t, failures, err, "no ALTER TABLE recorded after the target adds d")
	})
	t.Run("f: the row names the table in another case", func(t *testing.T) {
		failures, err := addedColumnFold{target: 30 * time.Second, snapshotAt: 60 * time.Second,
			ddl:    &addedColumnDDL{at: 60 * time.Second, pos: 500, table: "T", query: "ALTER TABLE T ADD COLUMN d INT"},
			events: []addedColumnEvent{between, after}}.run(t)
		// Whether the row is loaded at all depends on the index's collation;
		// either way it is not evidence.
		refuses(t, failures, err)
	})
	t.Run("h: the index has no schema_changes table", func(t *testing.T) {
		failures, err := addedColumnFold{target: 30 * time.Second, snapshotAt: 60 * time.Second,
			events: []addedColumnEvent{between, after}, dropSchemaChanges: true}.run(t)
		refuses(t, failures, err, "keeps no record of DDL statements")
	})
}
