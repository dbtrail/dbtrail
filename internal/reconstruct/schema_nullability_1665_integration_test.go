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

// TestRefresh_nullabilityChange is #1665 through the real fold: a baseline
// taken while `c` was NOT NULL, a schema snapshot taken after the column was
// made nullable, and a row that sets it to NULL. Before the fix the refresh
// published, and its CREATE TABLE still said NOT NULL, so loading it failed
// with "Column 'c' cannot be null". The control keeps the snapshot NOT NULL.
func TestRefresh_nullabilityChange(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	const schema, table = "shop", "t"

	run := func(t *testing.T, nowNullable string) (source, newest string, failures []reconstruct.TableFailure, err error) {
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
		ts := base.Add(5 * time.Second).Format("2006-01-02 15:04:05")
		testutil.InsertSnapshot(t, db, 1, ts, schema, table, "id", 1, "PRI", "int", "NO")
		testutil.InsertSnapshot(t, db, 1, ts, schema, table, "c", 2, "", "int", nowNullable)
		value := "7"
		if nowNullable == "YES" {
			value = "null"
		}
		testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
			base.Add(10*time.Second).Format("2006-01-02 15:04:05"), nil,
			schema, table, 2, "1", nil, []byte(`{"id":1,"c":1}`), []byte(`{"id":1,"c":`+value+`}`))

		createSQL := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `c` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
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
		if source, _, _, err = reconstruct.FindBaseline(ctx, root, schema, table, cut); err != nil {
			t.Fatalf("FindBaseline before the refresh: %v", err)
		}
		_, failures, err = reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
			IndexDSN: testutil.BaseDSN() + "/" + dbName, BaselineSrc: root,
			Tables: []string{schema + "." + table}, At: cut, OutputDir: root,
			OutputFormat: reconstruct.OutputFormatParquet,
		})
		newest, _, _, ferr := reconstruct.FindBaseline(ctx, root, schema, table, cut.Add(time.Second))
		if ferr != nil {
			t.Fatalf("FindBaseline after the refresh: %v", ferr)
		}
		return source, newest, failures, err
	}

	t.Run("a NOT NULL column made nullable refuses and publishes nothing", func(t *testing.T) {
		source, newest, failures, err := run(t, "YES")
		if err == nil || len(failures) != 1 || !errors.Is(failures[0].Err, reconstruct.ErrSchemaChanged) {
			t.Fatalf("err=%v failures=%+v; want the schema-change refusal", err, failures)
		}
		if msg := failures[0].Err.Error(); !strings.Contains(msg, "c (int NOT NULL -> int NULL)") {
			t.Errorf("refusal does not name the column and its NULL-ness: %q", msg)
		}
		if newest != source {
			t.Errorf("a snapshot was published despite the refusal: newest %s, source %s", newest, source)
		}
	})
	t.Run("still NOT NULL publishes", func(t *testing.T) {
		source, newest, failures, err := run(t, "NO")
		if err != nil || newest == source {
			t.Fatalf("err=%v failures=%+v newest=%s source=%s; want a published snapshot", err, failures, newest, source)
		}
	})
}
