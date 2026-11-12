//go:build integration

package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestReconstructTables_snapshotRemovedBetweenTwoTables is the mid-run case.
// The snapshot is looked up per table, when that table starts, and retention
// removes a snapshot folder with no lock against readers. Here the folder
// holds both tables when the run starts and is removed as soon as the first
// table is done. The second table then has no snapshot: it must be refused,
// not written from its one recorded change with its other row missing.
func TestReconstructTables_snapshotRemovedBetweenTwoTables(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Hour)
	at := base.Add(50 * time.Minute)
	ts := func(tm time.Time) string { return tm.Format("2006-01-02 15:04:05") }

	root := t.TempDir()
	snapDir := filepath.Join(root, strings.ReplaceAll(base.Format(time.RFC3339), ":", "-"))
	for i, table := range []string{"a", "b"} {
		testutil.InsertSnapshot(t, db, 1, ts(base), "shop", table, "id", 1, "PRI", "int", "NO")
		testutil.InsertSnapshot(t, db, 1, ts(base), "shop", table, "v", 2, "", "varchar", "NO")

		createSQL := "CREATE TABLE `" + table + "` (\n  `id` int NOT NULL,\n  `v` varchar(16) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
		cols, err := baseline.ParseSchemaText(createSQL)
		if err != nil {
			t.Fatalf("ParseSchemaText: %v", err)
		}
		w, err := baseline.NewWriter(filepath.Join(snapDir, "shop", table+".parquet"), cols, baseline.WriterConfig{
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
		// Row 1 is never changed; row 2 is.
		for _, row := range [][]string{{"1", "untouched"}, {"2", "old"}} {
			if err := w.WriteRow(row, []bool{false, false}); err != nil {
				t.Fatalf("WriteRow: %v", err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		pos := uint64(100 + 200*i)
		testutil.InsertEvent(t, db, "binlog.000001", pos, pos+100, ts(base.Add(10*time.Minute)), nil,
			"shop", table, 2 /* UPDATE */, "2", nil,
			[]byte(`{"id":2,"v":"old"}`), []byte(`{"id":2,"v":"new"}`))
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}

	prev := foldOneTable
	defer func() { foldOneTable = prev }()
	var once sync.Once
	foldOneTable = func(ctx context.Context, cfg FullTableConfig, sch, table string,
		db *sql.DB, engine *query.Engine, archSources []string,
		resolver *metadata.Resolver, dbName string) (*TableReport, error) {
		rep, err := prev(ctx, cfg, sch, table, db, engine, archSources, resolver, dbName)
		once.Do(func() {
			if rmErr := os.RemoveAll(snapDir); rmErr != nil {
				t.Errorf("remove the snapshot: %v", rmErr)
			}
		})
		return rep, err
	}

	out := t.TempDir()
	reports, failures, err := ReconstructTablesDetailed(ctx, FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:  root,
		Tables:       []string{"shop.a", "shop.b"},
		At:           at,
		OutputDir:    out,
		OutputFormat: OutputFormatMydumper,
		Parallelism:  1, // one table at a time: the second starts after the removal
	})
	if err == nil {
		t.Fatal("the run succeeded although one table lost its snapshot before it started")
	}
	t.Logf("rendered: %s", err)
	if len(reports) != 1 || reports[0].BinlogOnly || reports[0].RowsWritten != 2 {
		t.Fatalf("reports = %+v, want the first table alone, written in full from its snapshot", reports)
	}
	var refusal *ChangesOnlyError
	if len(failures) != 1 || !errors.As(failures[0].Err, &refusal) {
		t.Fatalf("failures = %+v, want the second table refused with a ChangesOnlyError", failures)
	}
	if failures[0].Table == reports[0].Table {
		t.Fatalf("table %s both succeeded and failed", failures[0].Table)
	}
	if _, statErr := os.Stat(filepath.Join(out, baseline.IncompleteMarker)); statErr != nil {
		t.Errorf("the output directory is not marked %s: %v", baseline.IncompleteMarker, statErr)
	}
	ents, rdErr := os.ReadDir(out)
	if rdErr != nil {
		t.Fatalf("ReadDir: %v", rdErr)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "shop."+failures[0].Table+".") || strings.HasPrefix(e.Name(), "shop."+failures[0].Table+"-") {
			t.Errorf("the refused table left %s", e.Name())
		}
		if e.Name() == baseline.SuccessMarker {
			t.Errorf("the output directory is marked %s", baseline.SuccessMarker)
		}
	}
}
