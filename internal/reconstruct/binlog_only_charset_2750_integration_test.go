//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
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

// #2750 through the changes-only full-table path: shop.t has no backup, so it
// is rebuilt from its recorded changes alone. An ALTER TABLE between its last
// change and the target narrowed v from utf8mb4 to latin1 with no row events,
// so a value latin1 cannot hold is '?' in the source: the rebuild refuses,
// before writing anything. Once that row is rewritten after the ALTER, it
// rebuilds.
func TestBinlogOnly_charsetNarrowing(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Hour)
	at := base.Add(50 * time.Minute)
	ts := func(tm time.Time) string { return tm.Format("2006-01-02 15:04:05") }

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, s := range []struct {
		id      int
		at      time.Time
		charset string
	}{{1, base, "utf8mb4"}, {2, base.Add(20 * time.Minute), "latin1"}} {
		for _, table := range []string{"t", "other"} {
			testutil.MustExec(t, db, `INSERT INTO schema_snapshots
				(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, character_set_name, is_nullable)
				VALUES (?, ?, 'shop', ?, 'id', 1, 'PRI', 'int', 'int', NULL, 'NO'),
				       (?, ?, 'shop', ?, 'v', 2, '', 'varchar', 'varchar(16)', ?, 'YES')`,
				s.id, ts(s.at), table, s.id, ts(s.at), table, s.charset)
		}
	}

	// A real snapshot that holds only shop.other, so shop.t has no backup
	// and takes the changes-only fallback.
	root := t.TempDir()
	snapDir := filepath.Join(root, strings.ReplaceAll(base.Format(time.RFC3339), ":", "-"))
	const createSQL = "CREATE TABLE `other` (\n  `id` int NOT NULL,\n  `v` varchar(16) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	w, err := baseline.NewWriter(filepath.Join(snapDir, "shop", "other.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: createSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			baseline.MetaKeyBinlogPos:      "4",
		},
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts(base.Add(10*time.Minute)), nil,
		"shop", "t", 1, "1", nil, nil, []byte(`{"id":1,"v":"Ñandú ✓"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 240, ts(base.Add(10*time.Minute)), nil,
		"shop", "t", 1, "2", nil, nil, []byte(`{"id":2,"v":"Ñandú €"}`))
	testutil.MustExec(t, db, `INSERT INTO schema_changes
		(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
		VALUES (?, 'binlog.000001', 250, 'shop', 't', 'ALTER TABLE', 'ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET latin1', 2)`,
		base.Add(20*time.Minute))

	run := func(db *sql.DB) (*reconstruct.TableReport, string, error) {
		out := t.TempDir()
		reports, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
			IndexDSN:         testutil.BaseDSN() + "/" + dbName,
			BaselineSrc:      root,
			Tables:           []string{"shop.t"},
			At:               at,
			OutputDir:        out,
			AllowChangesOnly: true,
		})
		for _, f := range failures {
			if f.Table == "t" {
				return nil, out, f.Err
			}
		}
		if err != nil {
			return nil, out, err
		}
		for _, r := range reports {
			if r.Table == "t" {
				return r, out, nil
			}
		}
		t.Fatalf("no report and no failure for shop.t")
		return nil, out, nil
	}

	_, out, err := run(db)
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL: the rebuilt row would keep text the table no longer has", err)
	}
	for _, want := range []string{"recorded changes alone", "from utf8mb4 to latin1", "binlog.000001:250"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not say %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "✓") {
		t.Errorf("the refusal carries the row's value: %v", err)
	}
	if files, _ := filepath.Glob(filepath.Join(out, "shop.t*")); len(files) != 0 {
		t.Errorf("a refused table left output behind: %v", files)
	}

	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts(base.Add(30*time.Minute)), nil,
		"shop", "t", 2, "1", []byte(`["v"]`), []byte(`{"id":1,"v":"Ñandú ?"}`), []byte(`{"id":1,"v":"Ñandú"}`))
	rep, _, err := run(db)
	if err != nil {
		t.Fatalf("the row rewritten after the ALTER: %v", err)
	}
	if rep.InsertsEmitted != 2 {
		t.Errorf("rows written = %d, want 2", rep.InsertsEmitted)
	}
}
