//go:build integration

package reconstruct_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2181: on an S3 destination a snapshot update of a small source cost more
// than reading the whole source, and most of the difference was work done
// once PER TABLE whatever the table held: each archive read is a listing and
// a download over the network. This pins how many archive reads one update
// makes per table, from the slope between a 4-table and a 16-table update
// with the same few changes, so a new per-table read fails here instead of
// showing up as a slower update on someone's bucket.
func TestRefresh_archiveReadsPerTable_2181(t *testing.T) {
	small, large := archiveReadsForTables(t, 4), archiveReadsForTables(t, 16)
	if small == 0 {
		t.Fatal("the update never reached the archive fetcher: the slope below would hold on nothing")
	}
	perTable := float64(large-small) / 12
	t.Logf("COUNT archive reads: %d for 4 tables, %d for 16 (%.2f per table)", small, large, perTable)
	if perTable > 1 {
		t.Errorf("archive reads: %d for 4 tables, %d for 16, %.2f per table; want at most 1 per table per update "+
			"(each one is a listing and a download on S3)", small, large, perTable)
	}
}

// archiveReadsForTables runs one update over n tables, three of which
// changed since the snapshot, against an index whose older hours are
// archived, and returns how many times the fold called the archive fetcher.
func archiveReadsForTables(t *testing.T, n int) int {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	N := time.Now().UTC().Truncate(time.Hour)
	var hours []time.Time
	for h := N.Add(-48 * time.Hour); !h.After(N.Add(2 * time.Hour)); h = h.Add(time.Hour) {
		hours = append(hours, h)
	}
	testutil.SetupPartitionedTable(t, db, dbName, hours)
	markStreamCaptured(t, db)

	tables := make([]string, n)
	full := make([]string, n)
	schemaAt := N.Add(-49 * time.Hour).Format("2006-01-02 15:04:05")
	for i := range tables {
		tables[i] = fmt.Sprintf("t%02d", i)
		full[i] = "shop." + tables[i]
		testutil.InsertSnapshot(t, db, 1, schemaAt, "shop", tables[i], "id", 1, "PRI", "int", "NO")
		testutil.InsertSnapshot(t, db, 1, schemaAt, "shop", tables[i], "status", 2, "", "varchar", "YES")
	}
	id := uint64(0)
	// One old change per table, in an hour rotation archives and drops.
	for _, tb := range tables {
		id++
		insertTableEvent(t, db, "shop", tb, id, 2+id, N.Add(-40*time.Hour), 2, "1", `{"id":1,"status":"old"}`)
	}
	if _, err := rotation.Perform(ctx, db, dbName, rotation.Options{
		RetainDur: 24 * time.Hour, RetainRaw: "24h", ArchiveDir: t.TempDir(), ArchiveCompression: "zstd",
		BintrailID: "2181f00d-dead-beef-dead-beefdeadbeef", Format: "json",
	}); err != nil {
		t.Fatalf("rotation.Perform: %v", err)
	}

	root := t.TempDir()
	dumpAt := N.Add(-30 * time.Minute)
	writeTablesBaseline2181(t, root, dumpAt, tables)
	for i := range 3 {
		id++
		insertTableEvent(t, db, "shop", tables[i], id, 1000+id, N.Add(-10*time.Minute), 2, "1", `{"id":1,"status":"new"}`)
	}

	var mu sync.Mutex
	calls := 0
	fetch := func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return parquetquery.Fetch(ctx, opts, src)
	}
	if _, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN: testutil.BaseDSN() + "/" + dbName, BaselineSrc: root, Tables: full,
		At: N.Add(-time.Minute), OutputDir: root, OutputFormat: reconstruct.OutputFormatParquet,
		CarryForwardUnchanged: true, TableDeltas: true, Parallelism: 2, ArchiveFetcher: fetch,
	}); err != nil {
		t.Fatalf("update over %d tables: %v", n, err)
	}
	return calls
}

// writeTablesBaseline2181 writes one complete dump snapshot holding every
// table, anchored at binlog.000001:4, through the real baseline writer.
func writeTablesBaseline2181(t *testing.T, root string, at time.Time, tables []string) {
	t.Helper()
	snapDir := filepath.Join(root, strings.ReplaceAll(at.UTC().Format(time.RFC3339), ":", "-"))
	for _, tb := range tables {
		create := strings.ReplaceAll(ordersCreateSQL, "`orders`", "`"+tb+"`")
		cols, err := baseline.ParseSchemaText(create)
		if err != nil {
			t.Fatal(err)
		}
		w, err := baseline.NewWriter(filepath.Join(snapDir, "shop", tb+".parquet"), cols, baseline.WriterConfig{
			Compression: "none", RowGroupSize: 100,
			Metadata: map[string]string{
				baseline.MetaKeyCreateTableSQL: create,
				baseline.MetaKeyBinlogFile:     "binlog.000001",
				baseline.MetaKeyBinlogPos:      "500",
				"bintrail.snapshot_timestamp":  at.UTC().Format(time.RFC3339),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow([]string{"1", "dumped"}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
}
