//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A table read again alone (#2640), through the real fold and a real index.
//
// Every test starts from lateDDLIndex: shop.orders in a snapshot taken at
// base and anchored at binlog.000003:500, holding ids 1, 2 and 3. The read
// that replaces it is staged OUTSIDE the snapshots root, at a later
// coordinate, and holds ids 7 and 8: a fold that started from the wrong file
// cannot produce the same rows.

// stageTable writes a finished read of an orders-shaped table under staging:
// taken at readAt, anchored at binlog.000003:pos, holding ids 7 and 8.
func stageTable(t *testing.T, staging, table string, readAt time.Time, pos uint64) string {
	t.Helper()
	dir := filepath.Join(staging, reconstruct.SnapshotDirName(readAt))
	p := filepath.Join(dir, "shop", table+".parquet")
	writeOrdersShaped(t, p, readAt, pos, [][]string{{"7", "fresh"}, {"8", "fresh"}})
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
	return p
}

func writeOrdersShaped(t *testing.T, path string, at time.Time, pos uint64, rows [][]string) {
	t.Helper()
	cols, err := baseline.ParseSchemaText(ordersCreateSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: ordersCreateSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000003",
			baseline.MetaKeyBinlogPos:      strconv.FormatUint(pos, 10),
			"bintrail.snapshot_timestamp":  at.UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// addItems puts a second table, shop.items, in the index's schema and in the
// snapshot lateDDLIndex wrote, with the same rows and anchor as orders.
func addItems(t *testing.T, db *sql.DB, root string, base time.Time) {
	t.Helper()
	ts := base.UTC().Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, ts, "shop", "items", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "shop", "items", "status", 2, "", "varchar", "YES")
	writeOrdersShaped(t, filepath.Join(root, reconstruct.SnapshotDirName(base), "shop", "items.parquet"), base, 500,
		[][]string{{"1", "new"}, {"2", "paid"}, {"3", "shipped"}})
}

func change(t *testing.T, db *sql.DB, table string, start, end uint64, at time.Time, pk, after string) {
	t.Helper()
	testutil.InsertEvent(t, db, "binlog.000003", start, end, at.Format("2006-01-02 15:04:05"), nil,
		"shop", table, 2, pk, nil, nil, []byte(after))
}

func truncateOf(t *testing.T, db *sql.DB, table string, endPos uint64, ranAt time.Time) {
	t.Helper()
	if err := indexer.InsertSchemaChange(db, event.Event{
		BinlogFile: "binlog.000003", EndPos: endPos, Timestamp: ranAt,
		Schema: "shop", Table: table, EventType: event.EventDDL,
		DDLType: event.DDLTruncateTable, DDLQuery: "TRUNCATE TABLE " + table,
	}, nil); err != nil {
		t.Fatalf("InsertSchemaChange: %v", err)
	}
}

type rereadRun struct {
	dsn, root string
	tables    []string
	override  map[string]string
	carry     bool
	deltas    bool
}

func (r rereadRun) at(at time.Time) ([]reconstruct.TableFailure, error) {
	_, failures, err := reconstruct.ReconstructTablesDetailed(context.Background(), reconstruct.FullTableConfig{
		IndexDSN: r.dsn, BaselineSrc: r.root, OutputDir: r.root, Tables: r.tables, At: at,
		OutputFormat: reconstruct.OutputFormatParquet, BaselineOverride: r.override,
		CarryForwardUnchanged: r.carry, TableDeltas: r.deltas,
	})
	return failures, err
}

// published is the table's file in the newest complete snapshot at or before
// at, with the time FindBaseline bounds its window from.
func published(t *testing.T, root, table string, at time.Time) (string, time.Time) {
	t.Helper()
	p, ts, _, err := reconstruct.FindBaseline(context.Background(), root, "shop", table, at)
	if err != nil {
		t.Fatalf("FindBaseline(%s): %v", table, err)
	}
	return p, ts
}

func wantAnchor(t *testing.T, path string, pos int64) {
	t.Helper()
	m, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		t.Fatalf("ReadParquetMetadata(%s): %v", path, err)
	}
	if m.BinlogFile != "binlog.000003" || m.BinlogPos != pos {
		t.Fatalf("%s is anchored at %s:%d, want binlog.000003:%d", path, m.BinlogFile, m.BinlogPos, pos)
	}
}

func wantRows(t *testing.T, what, path string, want ...string) {
	t.Helper()
	if got := readOrders(t, path); !equalStrings(got, want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

// The case the issue is about: a TRUNCATE refuses the table, a read of the
// table alone taken after it is folded in its place, and the run publishes.
// The refresh after it starts from what was published, with no override.
func TestBaselineOverride_aRereadTableIsFoldedPastATruncate(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)
	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders"}}

	truncateOf(t, db, "orders", 600, base.Add(10*time.Second))
	// Before the read's coordinate: the read already holds what this left
	// (id 8 is "fresh" in it). A window that started before the read would
	// put "stale" back.
	change(t, db, "orders", 620, 650, base.Add(12*time.Second), "8", `{"id":8,"status":"stale"}`)
	if _, err := run.at(base.Add(30 * time.Second)); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("without the read: %v, want the TRUNCATE to refuse", err)
	}

	staging := t.TempDir()
	staged := stageTable(t, staging, "orders", base.Add(15*time.Second), 700)
	change(t, db, "orders", 800, 900, base.Add(20*time.Second), "7", `{"id":7,"status":"B"}`)
	run.override = map[string]string{"shop.orders": staged}
	if _, err := run.at(base.Add(30 * time.Second)); err != nil {
		t.Fatalf("with the read: %v", err)
	}
	out, _ := published(t, root, "orders", base.Add(31*time.Second))
	if filepath.Dir(filepath.Dir(out)) != filepath.Join(root, reconstruct.SnapshotDirName(base.Add(30*time.Second))) {
		t.Fatalf("the newest snapshot is %s, not the one this run wrote", out)
	}
	wantRows(t, "the published table", out, "7=B", "8=fresh")
	wantAnchor(t, out, 900)
	// The read is the caller's: left as it was.
	wantRows(t, "the staged read", staged, "7=fresh", "8=fresh")
	if _, err := os.Stat(filepath.Join(filepath.Dir(filepath.Dir(staged)), baseline.SuccessMarker)); err != nil {
		t.Fatalf("the staged read lost its marker: %v", err)
	}

	// The next refresh has no override and no staged read to lean on.
	if err := os.RemoveAll(staging); err != nil {
		t.Fatal(err)
	}
	change(t, db, "orders", 950, 1000, base.Add(40*time.Second), "8", `{"id":8,"status":"C"}`)
	run.override = nil
	if _, err := run.at(base.Add(60 * time.Second)); err != nil {
		t.Fatalf("the refresh after it: %v", err)
	}
	out, _ = published(t, root, "orders", base.Add(61*time.Second))
	wantRows(t, "the table after the next refresh", out, "7=B", "8=C")
	wantAnchor(t, out, 1000)
}

// Two tables in one run, one read again and one folded from its snapshot:
// both end at the run's one cut, in one snapshot directory, and what the
// snapshot says about skipped views still comes from the snapshot before it,
// not from the read of one table.
func TestBaselineOverride_theOtherTablesFoldToTheSameCut(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)
	addItems(t, db, root, base)
	prev := filepath.Join(root, reconstruct.SnapshotDirName(base))
	if err := baseline.WriteViewsSkipped(prev, baseline.NewViewsSkipped([]string{"shop.v_orders"}, base)); err != nil {
		t.Fatalf("WriteViewsSkipped: %v", err)
	}

	truncateOf(t, db, "orders", 600, base.Add(10*time.Second))
	staged := stageTable(t, t.TempDir(), "orders", base.Add(15*time.Second), 700)
	change(t, db, "items", 750, 780, base.Add(16*time.Second), "2", `{"id":2,"status":"X"}`)
	change(t, db, "orders", 800, 900, base.Add(20*time.Second), "7", `{"id":7,"status":"B"}`)

	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders", "shop.items"},
		override: map[string]string{"shop.orders": staged}}
	if failures, err := run.at(base.Add(30 * time.Second)); err != nil {
		t.Fatalf("the run: %v (failures %v)", err, failures)
	}
	orders, _ := published(t, root, "orders", base.Add(31*time.Second))
	items, _ := published(t, root, "items", base.Add(31*time.Second))
	newDir := filepath.Join(root, reconstruct.SnapshotDirName(base.Add(30*time.Second)))
	if filepath.Dir(filepath.Dir(orders)) != newDir || filepath.Dir(filepath.Dir(items)) != newDir {
		t.Fatalf("the tables are not in one snapshot directory: %s and %s", orders, items)
	}
	wantRows(t, "orders", orders, "7=B", "8=fresh")
	wantRows(t, "items", items, "1=new", "2=X", "3=shipped")
	wantAnchor(t, orders, 900)
	wantAnchor(t, items, 900)
	if _, err := os.Stat(filepath.Join(newDir, baselineintegrity.ManifestName)); err != nil {
		t.Fatalf("the snapshot has no integrity manifest: %v", err)
	}
	rec, ok, err := baseline.ReadViewsSkipped(newDir)
	if err != nil || !ok || !rec.Carried || len(rec.Views) != 1 || rec.Views[0] != "shop.v_orders" {
		t.Fatalf("the skipped views were not carried from the previous snapshot: %+v ok=%v err=%v", rec, ok, err)
	}
}

// Two refused tables, and a read for only one of them: the run publishes
// neither. The newest snapshot is still the old one.
func TestBaselineOverride_oneTableRereadAndAnotherStillRefused(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)
	addItems(t, db, root, base)
	truncateOf(t, db, "orders", 600, base.Add(10*time.Second))
	truncateOf(t, db, "items", 610, base.Add(11*time.Second))
	staged := stageTable(t, t.TempDir(), "orders", base.Add(15*time.Second), 700)
	change(t, db, "orders", 800, 900, base.Add(20*time.Second), "7", `{"id":7,"status":"B"}`)

	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders", "shop.items"},
		override: map[string]string{"shop.orders": staged}}
	failures, err := run.at(base.Add(30 * time.Second))
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("got %v, want the TRUNCATE of items to refuse the run", err)
	}
	if len(failures) != 1 || failures[0].Table != "items" {
		t.Fatalf("failures = %+v, want items alone", failures)
	}
	if baseline.SnapshotComplete(filepath.Join(root, reconstruct.SnapshotDirName(base.Add(30*time.Second)))) {
		t.Fatalf("the run's directory reads as a complete snapshot")
	}
	for _, table := range []string{"orders", "items"} {
		p, ts := published(t, root, table, base.Add(31*time.Second))
		if !ts.Equal(base) {
			t.Fatalf("%s is now read from the %s snapshot, want the one before the run (%s)", table, ts, base)
		}
		wantRows(t, table, p, "1=new", "2=paid", "3=shipped")
	}
}

// The read was taken at a coordinate capture has not reached: the run is
// refused, by a class a caller can wait on, and publishes nothing. Once
// capture has passed the read, the same file is taken.
func TestBaselineOverride_aReadCaptureHasNotReachedIsRefusedThenTaken(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)
	truncateOf(t, db, "orders", 600, base.Add(10*time.Second))
	change(t, db, "orders", 800, 900, base.Add(12*time.Second), "8", `{"id":8,"status":"stale"}`)
	staged := stageTable(t, t.TempDir(), "orders", base.Add(15*time.Second), 5000)
	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders"},
		override: map[string]string{"shop.orders": staged}, carry: true}

	failures, err := run.at(base.Add(30 * time.Second))
	if !errors.Is(err, reconstruct.ErrBaselineOverrideAhead) {
		t.Fatalf("capture behind the read: %v, want ErrBaselineOverrideAhead", err)
	}
	if len(failures) != 1 || !errors.Is(failures[0].Err, reconstruct.ErrBaselineOverrideAhead) {
		t.Fatalf("failures = %+v, want the table's own refusal", failures)
	}
	if _, ts := published(t, root, "orders", base.Add(31*time.Second)); !ts.Equal(base) {
		t.Fatalf("a snapshot was published at %s over a read capture had not reached", ts)
	}

	change(t, db, "orders", 5000, 5100, base.Add(35*time.Second), "7", `{"id":7,"status":"late"}`)
	if _, err := run.at(base.Add(45 * time.Second)); err != nil {
		t.Fatalf("once capture has passed the read: %v", err)
	}
	out, _ := published(t, root, "orders", base.Add(46*time.Second))
	wantRows(t, "the published table", out, "7=late", "8=fresh")
	wantAnchor(t, out, 5100)
}

// No change to the table after the read, in a run that carries unchanged
// tables forward: the read itself is what is published, it outlives the
// staging folder, and the next refresh folds from it.
func TestBaselineOverride_aRereadTableWithNoChangeIsCarriedForward(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)
	truncateOf(t, db, "orders", 600, base.Add(10*time.Second))
	staging := t.TempDir()
	staged := stageTable(t, staging, "orders", base.Add(15*time.Second), 700)
	// Another table's change carries the cut past the read.
	change(t, db, "elsewhere", 800, 900, base.Add(20*time.Second), "1", `{"id":1,"status":"x"}`)

	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders"},
		override: map[string]string{"shop.orders": staged}, carry: true}
	if _, err := run.at(base.Add(30 * time.Second)); err != nil {
		t.Fatalf("the run: %v", err)
	}
	if err := os.RemoveAll(staging); err != nil {
		t.Fatal(err)
	}
	out, _ := published(t, root, "orders", base.Add(31*time.Second))
	wantRows(t, "the published table", out, "7=fresh", "8=fresh")
	wantAnchor(t, out, 700)

	change(t, db, "orders", 950, 1000, base.Add(40*time.Second), "7", `{"id":7,"status":"Z"}`)
	run.override = nil
	if _, err := run.at(base.Add(60 * time.Second)); err != nil {
		t.Fatalf("the refresh after it: %v", err)
	}
	out, _ = published(t, root, "orders", base.Add(61*time.Second))
	wantRows(t, "the table after the next refresh", out, "7=Z", "8=fresh")
}

// With table deltas on, the read becomes the base of a new chain that starts
// at the read, and the next refresh extends it.
func TestBaselineOverride_aRereadTableStartsANewDeltaChain(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)
	truncateOf(t, db, "orders", 600, base.Add(10*time.Second))
	readAt := base.Add(15 * time.Second)
	staging := t.TempDir()
	staged := stageTable(t, staging, "orders", readAt, 700)
	change(t, db, "orders", 800, 900, base.Add(20*time.Second), "7", `{"id":7,"status":"B"}`)

	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders"},
		override: map[string]string{"shop.orders": staged}, deltas: true}
	if _, err := run.at(base.Add(30 * time.Second)); err != nil {
		t.Fatalf("the run: %v", err)
	}
	if err := os.RemoveAll(staging); err != nil {
		t.Fatal(err)
	}
	out, since := published(t, root, "orders", base.Add(31*time.Second))
	if !since.Equal(readAt) {
		t.Fatalf("the table's window starts at %s, want the time of the read (%s)", since, readAt)
	}
	if got, want := readOrdersState(t, out), []string{"7=B", "8=fresh"}; !equalStrings(got, want) {
		t.Fatalf("the published table = %v, want %v", got, want)
	}

	change(t, db, "orders", 950, 1000, base.Add(40*time.Second), "8", `{"id":8,"status":"C"}`)
	run.override = nil
	if _, err := run.at(base.Add(60 * time.Second)); err != nil {
		t.Fatalf("the refresh after it: %v", err)
	}
	out, _ = published(t, root, "orders", base.Add(61*time.Second))
	if got, want := readOrdersState(t, out), []string{"7=B", "8=C"}; !equalStrings(got, want) {
		t.Fatalf("the table after the next refresh = %v, want %v", got, want)
	}
}
