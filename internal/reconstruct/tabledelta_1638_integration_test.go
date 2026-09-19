//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
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

	// verify over the two newest snapshots of the chain: both hold the SAME
	// table file, so there is nothing for it to compare. It must say so rather
	// than report a match for a table that changed, and the pair's window must
	// start where the chain did.
	pairs, _, _, err := verify.FindBaselinePair(ctx, root)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: pairs=%d err=%v", len(pairs), err)
	}
	if !pairs[0].NewHasDelta || !pairs[0].PrevSnapshot.Equal(h0) {
		t.Fatalf("pair = {NewHasDelta:%v PrevSnapshot:%s}, want a delta pair bounded from the chain's start %s",
			pairs[0].NewHasDelta, pairs[0].PrevSnapshot, h0)
	}
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	res, err := verify.VerifyBaselinePair(ctx, verify.BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName}, pairs[0])
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
}
