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

// TestRefresh_columnTypeChange drives #1651 and #1652 through the real fold:
// a baseline taken while `c` was INT, a schema snapshot taken after
// `ALTER TABLE t MODIFY c BIGINT`, and a row whose value only fits the new type.
//
// Before the fix the column-name guard passed, the new snapshot reused the old
// CREATE TABLE, and the write of 3000000000 into the INT column failed with a
// conversion error that did not say the schema changed (#1652), or, for a value
// that fit, published INT in the footer (#1651). The refusal must be the
// schema-change refusal, raised before anything is written.
//
// Types are compared against the schema snapshot in effect at the target, and
// only when it was taken after the baseline. Three controls pin that: a
// display width a server upgrade dropped is not a change; a type change after
// the target does not refuse a fold that stops before it (a restore to an
// earlier moment); and a snapshot older than the baseline says nothing about
// the baseline's types, so a stale one cannot refuse every run.
func TestRefresh_columnTypeChange(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	const schema, table = "shop", "t"

	// snap is one schema snapshot: when it was taken, relative to the
	// baseline, and the type it records for c.
	type snap struct {
		offset time.Duration
		ctype  string
	}
	run := func(t *testing.T, baselineCType string, snaps []snap, value string) (string, string, []reconstruct.TableFailure, error) {
		t.Helper()
		db, dbName := testutil.CreateTestDB(t)
		if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
			t.Fatalf("CreateIndexTables: %v", err)
		}
		if err := indexer.EnsureSchema(db); err != nil {
			t.Fatalf("EnsureSchema: %v", err)
		}
		base := time.Now().UTC().Truncate(time.Hour)
		cut := base.Add(30 * time.Second)

		for i, s := range snaps {
			ts := base.Add(s.offset).Format("2006-01-02 15:04:05")
			for _, c := range []struct{ name, key, dataType, columnType string }{
				{"id", "PRI", "int", "int"},
				{"c", "", strings.SplitN(s.ctype, "(", 2)[0], s.ctype},
			} {
				testutil.MustExec(t, db, `INSERT INTO schema_snapshots
					(snapshot_id, snapshot_time, schema_name, table_name, column_name,
					 ordinal_position, column_key, data_type, column_type, is_nullable)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'YES')`,
					i+1, ts, schema, table, c.name, map[string]int{"id": 1, "c": 2}[c.name], c.key, c.dataType, c.columnType)
			}
		}
		testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
			base.Add(10*time.Second).Format("2006-01-02 15:04:05"), nil,
			schema, table, 1, "3", nil, nil, []byte(`{"id":3,"c":`+value+`}`))

		createSQL := "CREATE TABLE `t` (\n" +
			"  `id` int NOT NULL,\n" +
			"  `c` " + baselineCType + " DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB;\n"
		root := t.TempDir()
		snapDir := filepath.Join(root, strings.ReplaceAll(base.Format(time.RFC3339), ":", "-"))
		cols, err := baseline.ParseSchemaText(createSQL)
		if err != nil {
			t.Fatalf("ParseSchemaText: %v", err)
		}
		w, err := baseline.NewWriter(filepath.Join(snapDir, schema, table+".parquet"), cols, baseline.WriterConfig{
			Compression:  "none",
			RowGroupSize: 100,
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
		for _, r := range [][]string{{"1", "1"}, {"2", "2"}} {
			if err := w.WriteRow(r, []bool{false, false}); err != nil {
				t.Fatalf("WriteRow: %v", err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if err := baseline.WriteSuccessMarker(snapDir); err != nil {
			t.Fatalf("WriteSuccessMarker: %v", err)
		}
		source, _, _, err := reconstruct.FindBaseline(ctx, root, schema, table, cut)
		if err != nil {
			t.Fatalf("FindBaseline before the refresh: %v", err)
		}

		_, failures, runErr := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
			IndexDSN:     testutil.BaseDSN() + "/" + dbName,
			BaselineSrc:  root,
			Tables:       []string{schema + "." + table},
			At:           cut,
			OutputDir:    root,
			OutputFormat: reconstruct.OutputFormatParquet,
		})
		newest, _, _, err := reconstruct.FindBaseline(ctx, root, schema, table, cut.Add(time.Second))
		if err != nil {
			t.Fatalf("FindBaseline after the refresh: %v", err)
		}
		return source, newest, failures, runErr
	}
	mustPublish := func(t *testing.T, source, newest string, failures []reconstruct.TableFailure, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("refresh refused: %v (%+v)", err, failures)
		}
		if newest == source {
			t.Fatal("the refresh reported success but published no new snapshot")
		}
	}

	t.Run("int widened to bigint with a value past int refuses as a schema change", func(t *testing.T) {
		source, newest, failures, err := run(t, "int", []snap{{5 * time.Second, "bigint"}}, "3000000000")
		if err == nil {
			t.Fatal("a refresh over a column whose type changed since the baseline was published")
		}
		if len(failures) != 1 || !errors.Is(failures[0].Err, reconstruct.ErrSchemaChanged) {
			t.Fatalf("failure is not the schema-change refusal (baseline refresh would not say refused-ddl): %+v", failures)
		}
		if msg := failures[0].Err.Error(); !strings.Contains(msg, "shop.t") || !strings.Contains(msg, "c (int -> bigint)") {
			t.Errorf("refusal does not name the table and the retyped column: %q", msg)
		}
		if newest != source {
			t.Errorf("a snapshot was published despite the refusal: newest %s, source %s", newest, source)
		}
	})

	t.Run("display width dropped by an upgrade is not a type change", func(t *testing.T) {
		source, newest, failures, err := run(t, "int(11)", []snap{{5 * time.Second, "int"}}, "5")
		mustPublish(t, source, newest, failures, err)
	})

	t.Run("a type change after the target does not refuse a fold that stops before it", func(t *testing.T) {
		source, newest, failures, err := run(t, "int", []snap{{5 * time.Second, "int"}, {time.Minute, "bigint"}}, "5")
		mustPublish(t, source, newest, failures, err)
	})

	t.Run("a target older than every schema snapshot compares no types", func(t *testing.T) {
		// EpochAt answers the first snapshot for such a target, and that one
		// describes a schema from after it.
		source, newest, failures, err := run(t, "int", []snap{{time.Minute, "bigint"}}, "5")
		mustPublish(t, source, newest, failures, err)
	})

	t.Run("a schema snapshot older than the baseline does not speak for its types", func(t *testing.T) {
		source, newest, failures, err := run(t, "bigint", []snap{{-10 * time.Minute, "int"}}, "3000000000")
		mustPublish(t, source, newest, failures, err)
	})

	// A fold copies the CREATE TABLE unchanged into a directory named by its
	// own target, so its directory time says nothing about the definition's
	// age. A snapshot committed after the first fold but stamped before its
	// directory (the snapshot is stamped when it starts and committed after
	// its inserts), or in the same second as the original dump, still
	// postdates the definition, and the next fold must see it.
	for _, stamp := range []struct {
		name   string
		offset time.Duration
	}{
		{"stamped before the first fold's directory", 20 * time.Second},
		{"stamped in the same second as the dump", 0},
		{"stamped in the same second as the first fold", 30 * time.Second},
	} {
		t.Run("a type change after an earlier fold is seen by the next fold: "+stamp.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
				t.Fatalf("CreateIndexTables: %v", err)
			}
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatalf("EnsureSchema: %v", err)
			}
			base := time.Now().UTC().Truncate(time.Hour)
			first, second := base.Add(30*time.Second), base.Add(50*time.Second)
			snapshot := func(id int, at time.Time, ctype string) {
				for _, c := range []struct{ name, key, dataType, columnType string }{
					{"id", "PRI", "int", "int"}, {"c", "", ctype, ctype},
				} {
					testutil.MustExec(t, db, `INSERT INTO schema_snapshots
					(snapshot_id, snapshot_time, schema_name, table_name, column_name,
					 ordinal_position, column_key, data_type, column_type, is_nullable)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'YES')`,
						id, at.Format("2006-01-02 15:04:05"), schema, table, c.name, map[string]int{"id": 1, "c": 2}[c.name], c.key, c.dataType, c.columnType)
				}
			}
			snapshot(1, base.Add(-time.Minute), "int")
			testutil.InsertEvent(t, db, "binlog.000001", 100, 200, base.Add(10*time.Second).Format("2006-01-02 15:04:05"), nil,
				schema, table, 1, "3", nil, nil, []byte(`{"id":3,"c":5}`))
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
				t.Fatalf("NewWriter: %v", err)
			}
			if err := w.WriteRow([]string{"1", "1"}, []bool{false, false}); err != nil {
				t.Fatalf("WriteRow: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := baseline.WriteSuccessMarker(snapDir); err != nil {
				t.Fatalf("WriteSuccessMarker: %v", err)
			}
			fold := func(at time.Time) ([]reconstruct.TableFailure, error) {
				_, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
					IndexDSN: testutil.BaseDSN() + "/" + dbName, BaselineSrc: root,
					Tables: []string{schema + "." + table}, At: at, OutputDir: root,
					OutputFormat: reconstruct.OutputFormatParquet,
				})
				return failures, err
			}
			if failures, err := fold(first); err != nil {
				t.Fatalf("first fold: %v (%+v)", err, failures)
			}
			// Committed only now, after the first fold published.
			snapshot(2, base.Add(stamp.offset), "bigint")
			failures, err := fold(second)
			if err == nil || len(failures) != 1 || !errors.Is(failures[0].Err, reconstruct.ErrSchemaChanged) ||
				!strings.Contains(failures[0].Err.Error(), "c (int -> bigint)") {
				t.Fatalf("second fold: err=%v failures=%+v; want the type-change refusal", err, failures)
			}
		})
	}
}
