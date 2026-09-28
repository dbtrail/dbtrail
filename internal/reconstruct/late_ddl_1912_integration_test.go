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
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A TRUNCATE that reaches the index late (#1912).
//
// detected_at is when the statement RAN on the source. With capture behind,
// the row is written after a refresh already published, and its time is before
// that refresh's. A check that places the statement by time alone never sees
// it: the first refresh ran before the row existed, and every later one looks
// from its predecessor's time onward.
//
// The timeline, all on the source's binlog:
//
//	  4      the first snapshot's anchor (ids 1, 2, 3)
//	100-200  UPDATE id=1, ran at +10s       indexed before the first refresh
//	200-300  TRUNCATE orders, ran at +20s   indexed AFTER the first refresh
//	300-400  INSERT id=9, ran at +25s       indexed after the first refresh
//
// The TRUNCATE's row is written by the writer capture uses, from an event
// shaped like the one the parser emits: the statement's execution time, the
// file, and its END position.
func TestReconstructParquet_aTruncateIndexedLateIsStillReported(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	dsn := testutil.BaseDSN() + "/" + dbName
	const schema = "shop"

	// One hour for the whole timeline, as in the #1169 tests: the planner
	// reads an hour with no live rows as a coverage gap.
	base := time.Now().UTC().Truncate(time.Hour)
	first := base.Add(30 * time.Second)
	second := base.Add(60 * time.Second)
	ts := func(d time.Duration) string { return base.Add(d).Format("2006-01-02 15:04:05") }

	seedOrdersSnapshot(t, db, schema, base)
	root := t.TempDir()
	seedSourceBaseline(t, root, base, schema)

	refresh := func(at time.Time) error {
		_, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
			IndexDSN:     dsn,
			BaselineSrc:  root,
			Tables:       []string{schema + ".orders"},
			At:           at,
			OutputDir:    root,
			OutputFormat: reconstruct.OutputFormatParquet,
		})
		return err
	}

	// Capture is behind: only the UPDATE has reached the index.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts(10*time.Second), nil,
		schema, "orders", 2, "1", nil, nil, []byte(`{"id":1,"status":"A"}`))

	if err := refresh(first); err != nil {
		t.Fatalf("the first refresh: %v", err)
	}
	mid, midTime, _, err := reconstruct.FindBaseline(ctx, root, schema, "orders", second)
	if err != nil {
		t.Fatalf("FindBaseline after the first refresh: %v", err)
	}
	if !midTime.Equal(first) {
		t.Fatalf("the second refresh would start from the %s snapshot, not the one just published (%s)", midTime, first)
	}
	if got, want := readOrders(t, mid), []string{"1=A", "2=paid", "3=shipped"}; !equalStrings(got, want) {
		t.Fatalf("first snapshot = %v, want %v", got, want)
	}

	// Capture catches up. The TRUNCATE ran 10 seconds BEFORE the first
	// refresh's time and is recorded only now.
	if err := indexer.InsertSchemaChange(db, event.Event{
		BinlogFile: "binlog.000001",
		EndPos:     300,
		Timestamp:  base.Add(20 * time.Second),
		Schema:     schema,
		Table:      "orders",
		EventType:  event.EventDDL,
		DDLType:    event.DDLTruncateTable,
		DDLQuery:   "TRUNCATE TABLE orders",
	}, nil); err != nil {
		t.Fatalf("InsertSchemaChange: %v", err)
	}
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts(25*time.Second), nil,
		schema, "orders", 1, "9", nil, nil, []byte(`{"id":9,"status":"after"}`))

	err = refresh(second)
	if err == nil {
		out, _, _, ferr := reconstruct.FindBaseline(ctx, root, schema, "orders", second.Add(time.Second))
		if ferr != nil {
			t.Fatalf("FindBaseline after the second refresh: %v", ferr)
		}
		t.Fatalf("the refresh published over a TRUNCATE and said nothing. The snapshot holds %v: "+
			"ids 1, 2 and 3 were removed by the TRUNCATE and only id 9 exists on the source",
			readOrders(t, out))
	}
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("refused for another reason: %v", err)
	}
	// What ran, on which table, when, where in the binlog.
	for _, want := range []string{"TRUNCATE TABLE", "shop.orders", base.Add(20 * time.Second).Format(time.RFC3339), "binlog.000001:300"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// lateDDLIndex is a fresh index with the orders table described, and a
// snapshot of it under root taken at base and anchored at binlog.000003:500.
func lateDDLIndex(t *testing.T) (db *sql.DB, dsn, root string, base time.Time) {
	t.Helper()
	return lateDDLIndexAt(t, "binlog.000003", "500")
}

// lateDDLIndexAt is lateDDLIndex with the snapshot's recorded position given;
// an empty file records none, as older snapshots did.
func lateDDLIndexAt(t *testing.T, file, pos string) (db *sql.DB, dsn, root string, base time.Time) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	base = time.Now().UTC().Truncate(time.Hour)
	seedOrdersSnapshot(t, db, "shop", base)

	root = t.TempDir()
	snapDir := filepath.Join(root, reconstruct.SnapshotDirName(base))
	cols, err := baseline.ParseSchemaText(ordersCreateSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	meta := map[string]string{
		baseline.MetaKeyCreateTableSQL: ordersCreateSQL,
		"bintrail.snapshot_timestamp":  base.Format(time.RFC3339),
	}
	if file != "" {
		meta[baseline.MetaKeyBinlogFile], meta[baseline.MetaKeyBinlogPos] = file, pos
	}
	w, err := baseline.NewWriter(filepath.Join(snapDir, "shop", "orders.parquet"), cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata:     meta,
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, r := range [][]string{{"1", "new"}, {"2", "paid"}, {"3", "shipped"}} {
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
	return db, testutil.BaseDSN() + "/" + dbName, root, base
}

func recordTruncate(t *testing.T, db *sql.DB, file string, endPos uint64, ranAt time.Time) {
	t.Helper()
	if err := indexer.InsertSchemaChange(db, event.Event{
		BinlogFile: file,
		EndPos:     endPos,
		Timestamp:  ranAt,
		Schema:     "shop",
		Table:      "orders",
		EventType:  event.EventDDL,
		DDLType:    event.DDLTruncateTable,
		DDLQuery:   "TRUNCATE TABLE orders",
	}, nil); err != nil {
		t.Fatalf("InsertSchemaChange: %v", err)
	}
}

func rowChange(t *testing.T, db *sql.DB, file string, start, end uint64, at time.Time, pk, after string) {
	t.Helper()
	testutil.InsertEvent(t, db, file, start, end, at.Format("2006-01-02 15:04:05"), nil,
		"shop", "orders", 2, pk, nil, nil, []byte(after))
}

func foldOrders(dsn, root, out, format string, at time.Time) error {
	_, err := reconstruct.ReconstructTables(context.Background(), reconstruct.FullTableConfig{
		IndexDSN:     dsn,
		BaselineSrc:  root,
		Tables:       []string{"shop.orders"},
		At:           at,
		OutputDir:    out,
		OutputFormat: format,
	})
	return err
}

// Indexed after TWO refreshes published (the source went quiet, capture was
// behind): the third one reports it.
func TestReconstructParquet_aTruncateIndexedAfterTwoRefreshes(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)

	rowChange(t, db, "binlog.000003", 600, 700, base.Add(10*time.Second), "1", `{"id":1,"status":"A"}`)
	for _, at := range []time.Duration{30 * time.Second, 40 * time.Second} {
		if err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(at)); err != nil {
			t.Fatalf("the refresh at +%s: %v", at, err)
		}
	}
	recordTruncate(t, db, "binlog.000003", 800, base.Add(20*time.Second))

	err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(60*time.Second))
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("the third refresh = %v, want ErrDestructiveDDL: the TRUNCATE ran before both published "+
			"snapshots' times and is in neither", err)
	}
}

// A TRUNCATE from before the snapshot, by time and by position, refuses no
// refresh: not the first after it, not the next.
func TestReconstructParquet_aTruncateBeforeTheSnapshotRefusesNothing(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)

	recordTruncate(t, db, "binlog.000002", 900, base.Add(-10*time.Minute))
	// Same file as the anchor, ending exactly on it, in the snapshot's second.
	recordTruncate(t, db, "binlog.000003", 500, base)
	rowChange(t, db, "binlog.000003", 600, 700, base.Add(10*time.Second), "1", `{"id":1,"status":"A"}`)
	if err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(30*time.Second)); err != nil {
		t.Fatalf("the first refresh refused on a TRUNCATE the snapshot already holds: %v", err)
	}
	rowChange(t, db, "binlog.000003", 700, 800, base.Add(40*time.Second), "2", `{"id":2,"status":"B"}`)
	if err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(60*time.Second)); err != nil {
		t.Fatalf("the second refresh refused on a TRUNCATE the snapshot already holds: %v", err)
	}
	out, _, _, err := reconstruct.FindBaseline(context.Background(), root, "shop", "orders", base.Add(61*time.Second))
	if err != nil {
		t.Fatalf("FindBaseline: %v", err)
	}
	if got, want := readOrders(t, out), []string{"1=A", "2=B", "3=shipped"}; !equalStrings(got, want) {
		t.Fatalf("snapshot = %v, want %v", got, want)
	}
}

// The statement and the snapshot in the same second: the position decides.
func TestReconstructParquet_aTruncateInTheSnapshotsSecond(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)

	rowChange(t, db, "binlog.000003", 600, 700, base.Add(10*time.Second), "1", `{"id":1,"status":"A"}`)
	recordTruncate(t, db, "binlog.000003", 560, base)
	err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(30*time.Second))
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL: the TRUNCATE ran in the snapshot's second, after its position", err)
	}
}

// A restore to a moment, as a SQL dump: no cut, the same check.
func TestReconstructDump_aTruncateIndexedLateRefusesARestore(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)

	rowChange(t, db, "binlog.000003", 600, 700, base.Add(10*time.Second), "1", `{"id":1,"status":"A"}`)
	// Ran 5 seconds before the snapshot's time, recorded after its position.
	recordTruncate(t, db, "binlog.000003", 560, base.Add(-5*time.Second))

	err := foldOrders(dsn, root, t.TempDir(), reconstruct.OutputFormatMydumper, base.Add(30*time.Second))
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL", err)
	}

	// A statement that ran after the target stays outside: restoring to
	// before a TRUNCATE is what a restore is for.
	db2, dsn2, root2, base2 := lateDDLIndex(t)
	rowChange(t, db2, "binlog.000003", 600, 700, base2.Add(10*time.Second), "1", `{"id":1,"status":"A"}`)
	recordTruncate(t, db2, "binlog.000003", 800, base2.Add(40*time.Second))
	if err := foldOrders(dsn2, root2, t.TempDir(), reconstruct.OutputFormatMydumper, base2.Add(30*time.Second)); err != nil {
		t.Fatalf("a restore to before the TRUNCATE refused: %v", err)
	}
}

// A TRUNCATE stamped past the refresh's time (the source's clock is ahead)
// that sits before row changes the refresh folds: inside by position, and
// reported by this refresh, not by the next one over a snapshot that already
// published without it.
func TestReconstructParquet_aTruncateStampedPastTheTargetInsideTheCut(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)

	recordTruncate(t, db, "binlog.000003", 560, base.Add(45*time.Second))
	rowChange(t, db, "binlog.000003", 600, 700, base.Add(10*time.Second), "1", `{"id":1,"status":"A"}`)
	err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(30*time.Second))
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL", err)
	}
}

// A snapshot that recorded no binlog position places nothing by position: an
// old TRUNCATE must not refuse every restore from it. The check is then the
// one by time, which still refuses a statement inside the window.
func TestReconstructDump_aSnapshotWithNoPositionLooksByTime(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndexAt(t, "", "")

	recordTruncate(t, db, "binlog.000002", 900, base.Add(-10*time.Minute))
	rowChange(t, db, "binlog.000003", 600, 700, base.Add(10*time.Second), "1", `{"id":1,"status":"A"}`)
	if err := foldOrders(dsn, root, t.TempDir(), reconstruct.OutputFormatMydumper, base.Add(30*time.Second)); err != nil {
		t.Fatalf("a TRUNCATE from before the snapshot's time refused the restore: %v", err)
	}
	recordTruncate(t, db, "binlog.000003", 750, base.Add(20*time.Second))
	err := foldOrders(dsn, root, t.TempDir(), reconstruct.OutputFormatMydumper, base.Add(30*time.Second))
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL: the TRUNCATE ran inside the window", err)
	}
}

// A TRUNCATE recorded under another binlog file name comes from a source the
// index followed before. Its name sorts after the snapshot's, and it refuses
// nothing: not the first refresh, not the next.
func TestReconstructParquet_aTruncateFromAnotherBinlogSequenceRefusesNothing(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)

	recordTruncate(t, db, "mysql-bin.000812", 1000, base.Add(-10*time.Minute))
	rowChange(t, db, "binlog.000003", 600, 700, base.Add(10*time.Second), "1", `{"id":1,"status":"A"}`)
	if err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(30*time.Second)); err != nil {
		t.Fatalf("the first refresh: %v", err)
	}
	rowChange(t, db, "binlog.000003", 700, 800, base.Add(40*time.Second), "2", `{"id":2,"status":"B"}`)
	if err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(60*time.Second)); err != nil {
		t.Fatalf("the second refresh: %v", err)
	}
	if err := foldOrders(dsn, root, t.TempDir(), reconstruct.OutputFormatMydumper, base.Add(70*time.Second)); err != nil {
		t.Fatalf("a restore: %v", err)
	}
}
