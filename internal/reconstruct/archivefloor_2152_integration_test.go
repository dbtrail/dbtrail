//go:build integration

package reconstruct_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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

// readRecord2152 reads a snapshot directory's archive record: table → "file:pos".
func readRecord2152(t *testing.T, dir string) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "_ARCHIVE_CHECKED"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Tables map[string]struct {
			File string `json:"binlog_file"`
			Pos  uint64 `json:"start_pos"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatalf("parse record: %v", err)
	}
	out := map[string]string{}
	for k, v := range f.Tables {
		out[k] = fmt.Sprintf("%s:%d", v.File, v.Pos)
	}
	return out
}

// When a refresh records its cut for a table (#2152): only when its own
// fetches checked the archives. Otherwise the table keeps what its source
// folder recorded, and a dump records nothing.
func TestRefresh_recordsItsCutOnlyWhenItCheckedTheArchives_2152(t *testing.T) {
	for _, tc := range []struct {
		name       string
		allowGaps  bool
		backfilled bool
		want       bool
	}{
		{"a stream-only index", false, false, true},
		{"--allow-gaps", true, false, false},
		{"`bintrail index` also wrote into the index", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var T time.Time
			r, _ := newLagRig(t, func(first time.Time) time.Time {
				T = first.Add(10 * time.Hour)
				return T.Add(-8 * time.Hour)
			})
			markStreamCaptured(t, r.db)
			if tc.backfilled {
				testutil.MustExec(t, r.db, `INSERT INTO index_state (binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at)
					VALUES ('binlog.000000', 1, 1, 0, 'completed', '2020-01-01 00:00:00', '2020-01-01 00:00:01')`)
			}
			insertTableEvent(t, r.db, r.schema, "orders", 10, 1000, T.Add(-time.Hour), 2, "1", `{"id":1,"status":"A"}`)
			if _, err := reconstruct.ReconstructTables(r.ctx, reconstruct.FullTableConfig{
				IndexDSN: r.dsn, BaselineSrc: r.root, Tables: []string{r.schema + ".orders"},
				At: T, OutputDir: r.root, OutputFormat: reconstruct.OutputFormatParquet, AllowGaps: tc.allowGaps,
			}); err != nil {
				t.Fatalf("refresh: %v", err)
			}
			p, _, _, err := reconstruct.FindBaseline(r.ctx, r.root, r.schema, "orders", T)
			if err != nil {
				t.Fatalf("FindBaseline: %v", err)
			}
			m, err := baseline.ReadParquetMetadata(p)
			if err != nil {
				t.Fatalf("read footer: %v", err)
			}
			rec := readRecord2152(t, filepath.Dir(filepath.Dir(p)))
			got, ok := rec["shop.orders"]
			if ok != tc.want {
				t.Fatalf("record = %v, want an entry for shop.orders: %v", rec, tc.want)
			}
			if tc.want && got != fmt.Sprintf("%s:%d", m.BinlogFile, m.BinlogPos) {
				t.Fatalf("record for shop.orders = %s, want the run's cut %s:%d", got, m.BinlogFile, m.BinlogPos)
			}
		})
	}
}

// The second review's case: a run that did NOT check the archives assembles
// a folder from tables checked to different positions, and the next refresh
// must still read, for each table, the archives after ITS position.
//
// R1 checks orders and items up to C1. A refresh of items alone checks it up
// to C1b. Between the two a change to orders is indexed (C1 < p < C1b) into
// an old hour, which rotation archives and drops. R2 runs with --allow-gaps
// and cannot read that archive: orders is carried forward unchanged from R1's
// folder, items from the later one. R3 runs normally and must apply the
// change: checked through C1 for orders, not C1b.
func TestRefresh_aRunThatDidNotCheckKeepsEachTablesPosition_2152(t *testing.T) {
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

	failArchives := false
	fetch := func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
		if failArchives {
			return nil, errors.New("simulated: the archive cannot be read")
		}
		return parquetquery.Fetch(ctx, opts, src)
	}
	refresh := func(at time.Time, tables []string, allowGaps bool) {
		t.Helper()
		if _, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
			IndexDSN: testutil.BaseDSN() + "/" + dbName, BaselineSrc: root, Tables: tables,
			At: at, OutputDir: root, OutputFormat: reconstruct.OutputFormatParquet,
			CarryForwardUnchanged: true, ArchiveFetcher: fetch, AllowGaps: allowGaps,
		}); err != nil {
			t.Fatalf("refresh at %s: %v", at.Format(time.RFC3339), err)
		}
	}
	both := []string{"shop.orders", "shop.items"}

	insertTableEvent(t, db, "shop", "items", 101, 100, N.Add(-70*time.Hour), 2, "1", `{"id":1,"status":"a"}`)
	refresh(N.Add(-69*time.Hour), both, false) // R1: C1 = binlog.000001:200 (end of the newest event)
	// Indexed after R1, positioned after C1, dated in an old hour.
	insertTableEvent(t, db, "shop", "orders", 102, 250, N.Add(-65*time.Hour), 2, "2", `{"id":2,"status":"late"}`)
	insertTableEvent(t, db, "shop", "items", 103, 300, N.Add(-60*time.Hour), 2, "1", `{"id":1,"status":"b"}`)
	refresh(N.Add(-55*time.Hour), []string{"shop.items"}, false) // items alone: C1b = binlog.000001:400

	if _, err := rotation.Perform(ctx, db, dbName, rotation.Options{
		RetainDur: 24 * time.Hour, RetainRaw: "24h", ArchiveDir: t.TempDir(), ArchiveCompression: "zstd",
		BintrailID: "2152f11d-dead-beef-dead-beefdeadbeef", Format: "json",
	}); err != nil {
		t.Fatalf("rotation.Perform: %v", err)
	}

	// A change still in the live index, so R2 has a cut of its own (ahead of
	// both tables' positions): the one a run that did not check must not
	// record.
	insertTableEvent(t, db, "shop", "items", 104, 500, N.Add(-31*time.Hour), 2, "1", `{"id":1,"status":"c"}`)

	failArchives = true
	R2 := N.Add(-30 * time.Hour)
	refresh(R2, both, true) // cannot read the archive; carries both forward
	failArchives = false

	r2dir := filepath.Join(root, reconstruct.SnapshotDirName(R2))
	rec := readRecord2152(t, r2dir)
	if rec["shop.orders"] != "binlog.000001:200" {
		t.Fatalf("R2 recorded orders as %q (record %v), want R1's binlog.000001:200: a run that did not check must not lift a table to another table's position", rec["shop.orders"], rec)
	}

	refresh(N, both, false) // R3
	p, _, _, err := reconstruct.FindBaseline(ctx, root, "shop", "orders", N)
	if err != nil {
		t.Fatalf("FindBaseline: %v", err)
	}
	if got, want := readOrders(t, p), []string{"1=new", "2=late", "3=shipped"}; !slices.Equal(got, want) {
		t.Fatalf("orders after R3 = %v, want %v: the archived change after orders' own checked position was skipped", got, want)
	}
}
