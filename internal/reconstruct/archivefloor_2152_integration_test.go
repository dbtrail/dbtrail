//go:build integration

package reconstruct_test

import (
	"context"
	"path/filepath"
	"slices"
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

// #2152 through the real refresh, the real rotation and real archives.
//
// Two tables. items changes all the time; orders does not change at all, so
// every refresh carries its file forward with the dump's old position. Two
// refreshes run, then rotation archives and drops every hour older than a day,
// then a third refresh runs.
//
//   - Steady state: the archives hold nothing after the second refresh's cut.
//     The third refresh must not read them for orders, although every one of
//     them holds changes after orders' own (old) position: reading them on
//     every refresh of every quiet table is the cost this pins.
//   - The issue's case: after the second refresh, a change to orders is
//     indexed LATE into an old hour, and that hour is archived and dropped
//     before the third refresh. The third refresh must apply it.
func TestRefresh_archivedHours_quietTableReadsNoArchive_lateChangeApplied_2152(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "steady state: the quiet table reads no archive"
		if late {
			name = "a late change archived before the update is applied"
		}
		t.Run(name, func(t *testing.T) {
			testutil.SkipIfNoMySQL(t)
			ctx := context.Background()
			db, dbName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatalf("EnsureSchema: %v", err)
			}
			N := time.Now().UTC().Truncate(time.Hour)
			var hours []time.Time
			for h := N.Add(-72 * time.Hour); !h.After(N.Add(2 * time.Hour)); h = h.Add(time.Hour) {
				hours = append(hours, h)
			}
			testutil.SetupPartitionedTable(t, db, dbName, hours)
			schemaAt := N.Add(-73 * time.Hour)
			seedOrdersSnapshot(t, db, "shop", schemaAt)
			ts := schemaAt.Format("2006-01-02 15:04:05")
			testutil.InsertSnapshot(t, db, 1, ts, "shop", "items", "id", 1, "PRI", "int", "NO")
			testutil.InsertSnapshot(t, db, 1, ts, "shop", "items", "status", 2, "", "varchar", "YES")
			markStreamCaptured(t, db)

			root := t.TempDir()
			dumpAt := N.Add(-71 * time.Hour)
			seedSourceBaseline(t, root, dumpAt, "shop")
			seedItemsBaseline2152(t, root, dumpAt)

			var mu sync.Mutex
			var ordersArchiveSince []time.Time
			fetch := func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
				if opts.Table == "orders" && opts.Since != nil {
					mu.Lock()
					ordersArchiveSince = append(ordersArchiveSince, *opts.Since)
					mu.Unlock()
				}
				return parquetquery.Fetch(ctx, opts, src)
			}
			refresh := func(at time.Time) {
				t.Helper()
				if _, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
					IndexDSN: testutil.BaseDSN() + "/" + dbName, BaselineSrc: root, Tables: []string{"shop.orders", "shop.items"},
					At: at, OutputDir: root, OutputFormat: reconstruct.OutputFormatParquet,
					CarryForwardUnchanged: true, ArchiveFetcher: fetch,
				}); err != nil {
					t.Fatalf("refresh at %s: %v", at.Format(time.RFC3339), err)
				}
			}

			id := uint64(100)
			item := func(pos uint64, at time.Time, status string) {
				id++
				insertTableEvent(t, db, "shop", "items", id, pos, at, 2, "1", `{"id":1,"status":"`+status+`"}`)
			}
			item(100, N.Add(-70*time.Hour), "a")
			refresh(N.Add(-69 * time.Hour))
			item(300, N.Add(-60*time.Hour), "b")
			item(500, N.Add(-50*time.Hour), "c")
			T2 := N.Add(-30 * time.Hour)
			refresh(T2)
			if late {
				// Positioned after the second refresh's cut, dated 40 hours ago.
				id++
				insertTableEvent(t, db, "shop", "orders", id, 1500, N.Add(-40*time.Hour), 2, "2", `{"id":2,"status":"late"}`)
			}
			if _, err := rotation.Perform(ctx, db, dbName, rotation.Options{
				RetainDur: 24 * time.Hour, RetainRaw: "24h", ArchiveDir: t.TempDir(), ArchiveCompression: "zstd",
				BintrailID: "2152f00d-dead-beef-dead-beefdeadbeef", Format: "json",
			}); err != nil {
				t.Fatalf("rotation.Perform: %v", err)
			}

			mu.Lock()
			ordersArchiveSince = nil
			mu.Unlock()
			refresh(N)

			p, _, _, err := reconstruct.FindBaseline(ctx, root, "shop", "orders", N)
			if err != nil {
				t.Fatalf("FindBaseline: %v", err)
			}
			got := readOrders(t, p)
			want := []string{"1=new", "2=paid", "3=shipped"}
			if late {
				want = []string{"1=new", "2=late", "3=shipped"}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("orders after the third refresh = %v, want %v", got, want)
			}
			mu.Lock()
			defer mu.Unlock()
			// The archive reads of orders' fetch start no earlier than the
			// floor of the second refresh's time when nothing late is
			// archived, and at the late change's hour when it is.
			floor := query.CoarseSinceFloor(T2)
			if len(ordersArchiveSince) == 0 {
				t.Fatal("orders' fetch never reached the archive fetcher: the checks below would pass on nothing")
			}
			for _, s := range ordersArchiveSince {
				if !late && s.Before(floor) {
					t.Fatalf("steady state: orders' archive read starts at %s, before %s: a quiet table read the archives rotation wrote since the last refresh", s.Format(time.RFC3339), floor.Format(time.RFC3339))
				}
			}
			if late && !slices.ContainsFunc(ordersArchiveSince, func(s time.Time) bool { return !s.After(N.Add(-40 * time.Hour)) }) {
				t.Fatalf("late: orders' archive reads started at %v, none at or before the late change's hour", ordersArchiveSince)
			}
		})
	}
}

// seedItemsBaseline2152 writes items into the dump snapshot seedSourceBaseline
// wrote for orders: the same layout, the same dump position.
func seedItemsBaseline2152(t *testing.T, root string, at time.Time) {
	t.Helper()
	snapDir := filepath.Join(root, strings.ReplaceAll(at.UTC().Format(time.RFC3339), ":", "-"))
	cols, err := baseline.ParseSchemaText(strings.ReplaceAll(ordersCreateSQL, "`orders`", "`items`"))
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	w, err := baseline.NewWriter(filepath.Join(snapDir, "shop", "items.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: strings.ReplaceAll(ordersCreateSQL, "`orders`", "`items`"),
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			baseline.MetaKeyBinlogPos:      "4",
			"bintrail.snapshot_timestamp":  at.UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1", "new"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
