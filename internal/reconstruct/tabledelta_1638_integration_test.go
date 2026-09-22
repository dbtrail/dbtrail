//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// readOrdersState reads a table's state through baseline.TableDeltaStateSQL,
// the way `bintrail views` does for a table with a delta beside it.
func readOrdersState(t *testing.T, base string) []string {
	t.Helper()
	posdel, upserts := baseline.TableDeltaGlobs(base)
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer ddb.Close()
	q := "SELECT id, status FROM (" + baseline.TableDeltaStateSQL("'"+base+"'", "'"+posdel+"'", "'"+upserts+"'", base, "") + ")"
	rows, err := ddb.Query(q)
	if err != nil {
		t.Fatalf("state of %s: %v", base, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id int32
		var status sql.NullString
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, fmt.Sprintf("%d=%s", id, status.String))
	}
	sort.Strings(out)
	return out
}

// TestReconstructParquet_tableDeltaChainAcrossHours is the #1638 acceptance
// test, through the real fold and a real index.
//
// The chain spans FOUR clock hours on purpose. A reader that ignores the delta
// folds the index over the base, and its fetch has a coarse time floor one hour
// before the time FindBaseline returns. With the snapshot DIRECTORY's time, by
// the last run that floor sits after the first two hours' events, which the
// base does not hold: they would vanish without an error. FindBaseline returns
// the chain's START for exactly that reason, and the last leg here — deltas
// turned OFF over a snapshot written with them on — is the reader that proves
// it, because it rebuilds the table from the base alone.
func TestReconstructParquet_tableDeltaChainAcrossHours(t *testing.T) {
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

	// Hours AHEAD of now: CreateIndexTables lays its hourly partitions forward
	// from the current hour, and the planner reads an hour with no partition
	// as a coverage gap.
	h0 := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
	hour := func(n int, d time.Duration) time.Time { return h0.Add(time.Duration(n)*time.Hour + d) }
	seedOrdersSnapshot(t, db, schema, h0)

	ev := func(start uint64, at time.Time, evType uint8, pk, after string) {
		var afterJSON []byte
		if after != "" {
			afterJSON = []byte(after)
		}
		var before []byte
		if evType == 3 {
			before = []byte(`{"id":` + pk + `,"status":"gone"}`)
		}
		testutil.InsertEvent(t, db, "binlog.000001", start, start+100, at.Format("2006-01-02 15:04:05"),
			nil, schema, "orders", evType, pk, nil, before, afterJSON)
	}
	// One event per hour, so no hour in the window reads as a gap.
	ev(100, hour(0, 10*time.Minute), 2, "1", `{"id":1,"status":"A"}`) // update
	ev(200, hour(1, 10*time.Minute), 3, "2", "")                      // delete
	ev(300, hour(2, 10*time.Minute), 1, "4", `{"id":4,"status":"D"}`) // insert
	ev(400, hour(3, 10*time.Minute), 2, "1", `{"id":1,"status":"A2"}`)

	root := t.TempDir()
	seedSourceBaseline(t, root, h0, schema)
	// #1717: what each run's manifest reused from the snapshot it read.
	var manifest baselineintegrity.ManifestStats
	defer reconstruct.CountManifestReuseForTest(&manifest)()
	run := func(at time.Time, deltas bool) {
		t.Helper()
		if _, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
			IndexDSN: dsn, BaselineSrc: root, Tables: []string{schema + ".orders"},
			At: at, OutputDir: root, OutputFormat: reconstruct.OutputFormatParquet, TableDeltas: deltas,
		}); err != nil {
			t.Fatalf("ReconstructTables(at=%s, deltas=%v): %v", at.Format(time.RFC3339), deltas, err)
		}
	}
	newest := func(at time.Time) (string, time.Time) {
		t.Helper()
		p, ts, _, err := reconstruct.FindBaseline(ctx, root, schema, "orders", at)
		if err != nil {
			t.Fatalf("FindBaseline(%s): %v", at.Format(time.RFC3339), err)
		}
		return p, ts
	}

	wantAfter := [][]string{
		{"1=A", "2=paid", "3=shipped"},
		{"1=A", "3=shipped"},
		{"1=A", "3=shipped", "4=D"},
		{"1=A2", "3=shipped", "4=D"},
	}
	original, _ := newest(h0)
	for n, want := range wantAfter {
		at := hour(n, 30*time.Minute)
		run(at, true)
		base, since := newest(at)
		if got := readOrdersState(t, base); !equalStrings(got, want) {
			t.Fatalf("hour %d: state = %v, want %v", n, got, want)
		}
		// The base is still the ORIGINAL file, rows and all.
		if got := readOrders(t, base); !equalStrings(got, []string{"1=new", "2=paid", "3=shipped"}) {
			t.Fatalf("hour %d: the base was rewritten: %v", n, got)
		}
		a, _ := os.Stat(original)
		b, _ := os.Stat(base)
		if !os.SameFile(a, b) {
			t.Fatalf("hour %d: the base is not the original file carried forward", n)
		}
		if !since.Equal(h0) {
			t.Fatalf("hour %d: FindBaseline time = %s, want the chain's start %s", n, since, h0)
		}
		// The manifest hashed this window's pair (2 files) and took the
		// linked files' digests from the previous snapshot: the base and
		// every earlier pair. The seed snapshot has no manifest, so hour 0
		// also hashes the base it links from it.
		wantReused, wantHashed := 1+2*n, 2 // base + pairs 0..n-1; the new pair
		if n == 0 {
			wantReused, wantHashed = 0, 3
		}
		if manifest.Reused != wantReused || manifest.Hashed != wantHashed {
			t.Fatalf("hour %d: manifest reused %d, hashed %d; want %d reused, %d hashed", n, manifest.Reused, manifest.Hashed, wantReused, wantHashed)
		}
	}

	// Two snapshots of the chain hold the SAME table file. The default check
	// never pairs them (its newer side is always a read of the database, below),
	// but a pair built that way must still say nothing was checked rather than
	// report a match for a table that changed.
	prevBase, _ := newest(hour(2, 30*time.Minute))
	lastBase, _ := newest(hour(3, 30*time.Minute))
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	vcfg := verify.BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	anchor := query.BinlogPos{File: "binlog.000001", Pos: 4}
	res, err := verify.VerifyBaselinePair(ctx, vcfg, verify.BaselinePair{
		Schema: schema, Table: "orders", PrevPath: prevBase, NewPath: lastBase,
		PrevSnapshot: h0, NewSnapshot: hour(3, 30*time.Minute), NewHasDelta: true,
		PrevAnchor: anchor, NewAnchor: anchor,
	})
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if res.Status != verify.StatusInconclusive || !strings.Contains(res.Detail, "nothing was checked") {
		t.Fatalf("verify over an unchanged table file = %s (%q), want inconclusive and the reason", res.Status, res.Detail)
	}

	// Deltas OFF over the chain. This run reads the base alone.
	off := hour(3, 40*time.Minute)
	run(off, false)
	base, since := newest(off)
	if !since.Equal(off) {
		t.Fatalf("the rewritten snapshot reports time %s, want its own directory %s", since, off)
	}
	if got, want := readOrders(t, base), wantAfter[3]; !equalStrings(got, want) {
		t.Fatalf("turning deltas off lost changes — the base-only fold read the wrong window:\n got: %v\nwant: %v", got, want)
	}
	if has, err := baseline.HasTableDelta(ctx, base); err != nil || has {
		t.Fatalf("a run with deltas off left a delta beside the table (has=%v err=%v)", has, err)
	}
	// Rewritten in full at the same relative path as the linked prior: the
	// inode differs, so it is hashed. Inode, not path, decides (#1717).
	if manifest.Reused != 0 || manifest.Hashed != 1 {
		t.Fatalf("rewritten table: manifest reused %d, hashed %d; want 0 / 1", manifest.Reused, manifest.Hashed)
	}

	// The default check over this real tree: a read of the database after the
	// folds, stamped the way baseline.Run stamps one, holding the table's true
	// state at its anchor, then two real folds after it. The first keeps the
	// read's own file (deltas on: a chain beside it); the second rewrites the
	// table (deltas off), so the newest file is one the fold writer stamped.
	// The check must find the read from both, and compare it with the snapshot
	// before it, the rewritten fold above.
	readAt := hour(3, 50*time.Minute)
	readPath := writeReadOfOrders(t, root, readAt, schema, 500, [][]string{{"1", "A2"}, {"3", "shipped"}, {"4", "D"}})
	wantReadPair := func(when string) verify.BaselinePair {
		t.Helper()
		pairs, _, err := verify.FindBaselinePair(ctx, root)
		if err != nil || len(pairs) != 1 {
			t.Fatalf("%s: FindBaselinePair: pairs=%d err=%v", when, len(pairs), err)
		}
		p := pairs[0]
		if p.Settled != nil || !p.NewReadFromDatabase || !p.NewSnapshot.Equal(readAt) || !p.PrevSnapshot.Equal(off) || p.NewPath != readPath {
			t.Fatalf("%s: pair = %+v (settled %v), want the read at %s compared with the snapshot at %s", when, p, p.Settled, readAt, off)
		}
		return p
	}
	run(hour(3, 55*time.Minute), true)
	wantReadPair("after a fold that keeps the read's file")
	rewritten := hour(3, 58*time.Minute)
	run(rewritten, false)
	newestPath, _ := newest(rewritten)
	md, err := baseline.ReadParquetMetadata(newestPath)
	if err != nil {
		t.Fatal(err)
	}
	if md.Producer != baseline.ProducerReconstruct || !md.SnapshotTimestamp.Equal(rewritten) || !md.LastDumpAt.Equal(readAt) {
		t.Fatalf("fixture: the newest file is not a fold written at %s carrying the read at %s (producer %q, written %s, read %s)",
			rewritten, readAt, md.Producer, md.SnapshotTimestamp, md.LastDumpAt)
	}
	p := wantReadPair("after a fold that rewrote the table")
	if res, err = verify.VerifyBaselinePair(ctx, vcfg, p); err != nil || res.Status != verify.StatusMatch || !res.ComparedTo.Equal(readAt) {
		t.Fatalf("the read of the true state: %s (%q) compared to %s, err=%v; want a match against the read at %s", res.Status, res.Detail, res.ComparedTo, err, readAt)
	}
	// And the comparison is real: the same read holding a wrong row differs.
	posdel, upserts := baseline.TableDeltaPaths(readPath, 0)
	for _, f := range []string{readPath, posdel, upserts} {
		if err := os.Remove(f); err != nil {
			t.Fatal(err)
		}
	}
	writeReadOfOrders(t, root, readAt, schema, 500, [][]string{{"1", "A2"}, {"3", "shipped"}, {"4", "WRONG"}})
	if res, err = verify.VerifyBaselinePair(ctx, vcfg, p); err != nil || res.Status != verify.StatusMismatch {
		t.Fatalf("a read that differs from the recorded changes: %s (%q) err=%v, want a mismatch", res.Status, res.Detail, err)
	}
}

// writeReadOfOrders writes a snapshot of the orders table the way a read of
// the database lands (baseline.Run's footer: producer, its own instant as the
// last read, zero folds, the dump's binlog position), with the empty pair
// baseline.Run writes beside every table when table deltas are on.
func writeReadOfOrders(t *testing.T, root string, at time.Time, schema string, pos int, rows [][]string) string {
	t.Helper()
	ts := at.UTC().Format(time.RFC3339)
	snapDir := filepath.Join(root, reconstruct.SnapshotDirName(at))
	path := filepath.Join(snapDir, schema, "orders.parquet")
	cols, err := baseline.ParseSchemaText(ordersCreateSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
		baseline.MetaKeyCreateTableSQL:    ordersCreateSQL,
		baseline.MetaKeySnapshotTimestamp: ts,
		baseline.MetaKeyMydumperFormat:    "sql",
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
		baseline.MetaKeyLastDumpAt:        ts,
		baseline.MetaKeyFoldGeneration:    "0",
		baseline.MetaKeyBinlogFile:        "binlog.000001",
		baseline.MetaKeyBinlogPos:         fmt.Sprint(pos),
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteEmptyTableDeltas(snapDir); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
	return path
}
