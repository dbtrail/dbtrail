package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// zooChain publishes n windows over the zoo base as a chain of n+... pairs
// under one root, each snapshot in its own directory as a refresh leaves
// them, and returns the table file of every snapshot, oldest first. Every
// window changes id 1 and id 5, and the last deletes id 5, so the pairs
// share keys and the newest version of one of them is a tombstone.
func zooChain(t *testing.T, windows int) (bases []string, root string, lastAt time.Time) {
	t.Helper()
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root = t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	base, at := src, t0
	for i := 1; i <= windows; i++ {
		next := t0.Add(time.Duration(i) * 5 * time.Minute)
		w := changeMap(upd(1, fmt.Sprint("v", i)), ins(5, fmt.Sprint("five", i)), ins(10+i, "own"))
		if i == windows {
			w = changeMap(upd(1, "last"), del(5), ins(10+i, "own"))
		}
		var err error
		base, _, err = deltaWindow(t, root, base, at, w, next, &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * i)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		bases, at = append(bases, base), next
	}
	return bases, root, at
}

func listedChain(t *testing.T, base string) *baseline.TableDeltaChain {
	t.Helper()
	c, err := baseline.ListTableDelta(context.Background(), base)
	if err != nil || c == nil {
		t.Fatalf("no chain beside %s (err=%v)", base, err)
	}
	return c
}

// jsonRows runs a state SQL and returns every row as JSON, sorted.
func jsonRows(t *testing.T, stateSQL string) []string {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	rows, err := ddb.Query("SELECT to_json(s)::VARCHAR FROM (" + stateSQL + ") AS s ORDER BY 1")
	if err != nil {
		t.Fatalf("%v\n%s", err, stateSQL)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// validateWith replaces the check ResolveTableDelta runs on each input.
func validateWith(t *testing.T, check func(path string) error) {
	t.Helper()
	prev := resolveValidate
	t.Cleanup(func() { resolveValidate = prev })
	resolveValidate = check
}

// countMerges counts the merges ResolveTableDelta runs from here on.
func countMerges(t *testing.T) *int {
	t.Helper()
	n, prev := new(int), resolveMerge
	t.Cleanup(func() { resolveMerge = prev })
	resolveMerge = func(ctx context.Context, basePath string, chain *baseline.TableDeltaChain, lo, hi int, outDir string) (*MinorCompaction, error) {
		*n++
		return prev(ctx, basePath, chain, lo, hi, outDir)
	}
	return n
}

// TestResolveTableDelta_theWholeChainAsOnePair: the resolved pair holds the
// chain's state, last pair included, each key once, outside the snapshot;
// the snapshot's own files are untouched; and a second call finds it and
// merges nothing.
func TestResolveTableDelta_theWholeChainAsOnePair(t *testing.T) {
	ctx := context.Background()
	bases, root, _ := zooChain(t, 3)
	base := bases[2]
	chain := listedChain(t, base)
	if len(chain.Files) != 3 {
		t.Fatalf("chain has %d pairs, want 3", len(chain.Files))
	}
	before := snapshotFilesOf(t, base)
	merges := countMerges(t)
	var checked []string
	validateWith(t, func(p string) error { checked = append(checked, p); return nil })
	done, linked, err := ResolveTableDelta(ctx, base, chain, "")
	if err != nil || !done || linked || *merges != 1 {
		t.Fatalf("done=%v linked=%v merges=%d err=%v", done, linked, *merges, err)
	}
	if !reflect.DeepEqual(checked, chain.Paths()) {
		t.Errorf("files checked before the merge = %v, want every file of the chain %v", checked, chain.Paths())
	}
	r, ok := baseline.FindResolvedTableDelta(base, chain.Files)
	if !ok {
		t.Fatal("no resolved pair found after writing it")
	}
	if want := filepath.Join(root, baseline.ResolvedDirName, filepath.Base(filepath.Dir(filepath.Dir(base))), "mydb"); filepath.Dir(r.Upserts) != want {
		t.Errorf("the pair is under %s, want %s", filepath.Dir(r.Upserts), want)
	}
	if after := snapshotFilesOf(t, base); !reflect.DeepEqual(after, before) {
		t.Errorf("the snapshot's files changed: %v, were %v", after, before)
	}
	if n := repeatedKeys(t, r.Upserts); n != 0 {
		t.Errorf("%d keys are written more than once in the resolved pair", n)
	}
	through := jsonRows(t, baseline.TableDeltaOnePairStateSQL(sqlLit(base), sqlLit(r.Posdel), sqlLit(r.Upserts), ""))
	chainState := jsonRows(t, chainStateSQL(base, chain))
	if len(chainState) == 0 || !reflect.DeepEqual(through, chainState) {
		t.Fatalf("state through the resolved pair:\n %v\nthrough the chain:\n %v", through, chainState)
	}

	done, linked, err = ResolveTableDelta(ctx, base, chain, "")
	if err != nil || !done || linked || *merges != 1 {
		t.Fatalf("second call: done=%v linked=%v merges=%d err=%v, want it to find the pair", done, linked, *merges, err)
	}
	// Nothing but the pair is left beside it: no work directory.
	left, err := os.ReadDir(filepath.Dir(r.Upserts))
	if err != nil || len(left) != 2 {
		t.Fatalf("beside the pair: %v (err=%v), want its two files only", left, err)
	}
}

// TestResolveTableDelta_nothingToResolve: one pair is read as it is, and a
// legacy pair or no chain has nothing to merge. No directory is made.
func TestResolveTableDelta_nothingToResolve(t *testing.T) {
	bases, root, _ := zooChain(t, 1)
	merges := countMerges(t)
	one := listedChain(t, bases[0])
	if len(one.Files) != 1 {
		t.Fatalf("chain has %d pairs, want 1", len(one.Files))
	}
	for name, c := range map[string]*baseline.TableDeltaChain{"one pair": one, "no chain": nil, "a legacy pair": {Legacy: true}} {
		done, linked, err := ResolveTableDelta(context.Background(), bases[0], c, "")
		if done || linked || err != nil || *merges != 0 {
			t.Errorf("%s: done=%v linked=%v merges=%d err=%v", name, done, linked, *merges, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, baseline.ResolvedDirName)); !os.IsNotExist(err) {
		t.Errorf("a directory was made with nothing to resolve (stat err=%v)", err)
	}
}

// TestResolveTableDelta_aChainCarriedForwardIsLinked: a refresh with no
// change to the table links its chain into the new snapshot, and the pair
// that resolved the previous snapshot is linked too, not merged again. A
// refresh that adds a pair is merged.
func TestResolveTableDelta_aChainCarriedForwardIsLinked(t *testing.T) {
	ctx := context.Background()
	bases, root, at := zooChain(t, 2)
	prevBase := bases[1]
	if _, _, err := ResolveTableDelta(ctx, prevBase, listedChain(t, prevBase), ""); err != nil {
		t.Fatal(err)
	}
	prevPair, ok := baseline.FindResolvedTableDelta(prevBase, listedChain(t, prevBase).Files)
	if !ok {
		t.Fatal("no resolved pair for the previous snapshot")
	}
	// An empty window: the chain is carried forward whole.
	quiet, rep, err := deltaWindow(t, root, prevBase, at, map[string]*query.ResultRow{}, at.Add(5*time.Minute), &query.BinlogPos{File: "binlog.000009", Pos: 9000}, nil)
	if err != nil || rep.DeltaPairWritten {
		t.Fatalf("the quiet window wrote a pair (%v) or failed (%v)", rep.DeltaPairWritten, err)
	}
	merges := countMerges(t)
	validateWith(t, func(string) error { return errors.New("a linked pair is not checked again") })
	done, linked, err := ResolveTableDelta(ctx, quiet, listedChain(t, quiet), prevBase)
	if err != nil || !done || !linked || *merges != 0 {
		t.Fatalf("carried chain: done=%v linked=%v merges=%d err=%v", done, linked, *merges, err)
	}
	pair, _ := baseline.FindResolvedTableDelta(quiet, listedChain(t, quiet).Files)
	for _, f := range [][2]string{{prevPair.Posdel, pair.Posdel}, {prevPair.Upserts, pair.Upserts}} {
		if !sameFile(f[0], f[1]) {
			t.Errorf("%s is not a link to %s", f[1], f[0])
		}
	}

	// A window with a change: one pair more, so the previous pair is not it.
	grown, _, err := deltaWindow(t, root, quiet, at.Add(5*time.Minute), changeMap(upd(2, "grown")), at.Add(10*time.Minute), &query.BinlogPos{File: "binlog.000009", Pos: 9500}, nil)
	if err != nil {
		t.Fatal(err)
	}
	validateWith(t, func(string) error { return nil })
	done, linked, err = ResolveTableDelta(ctx, grown, listedChain(t, grown), quiet)
	if err != nil || !done || linked || *merges != 1 {
		t.Fatalf("grown chain: done=%v linked=%v merges=%d err=%v", done, linked, *merges, err)
	}
	g := listedChain(t, grown)
	r, _ := baseline.FindResolvedTableDelta(grown, g.Files)
	if got, want := jsonRows(t, baseline.TableDeltaOnePairStateSQL(sqlLit(grown), sqlLit(r.Posdel), sqlLit(r.Upserts), "")), jsonRows(t, chainStateSQL(grown, g)); !reflect.DeepEqual(got, want) {
		t.Fatalf("grown chain through its pair:\n %v\nthrough the chain:\n %v", got, want)
	}
}

// otherZooChain is a chain of two pairs over the zoo base that is not
// zooChain's: other windows, at other times and binlog positions.
func otherZooChain(t *testing.T) (base string) {
	t.Helper()
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	b1, _, err := deltaWindow(t, root, src, t0, changeMap(upd(2, "mine")), t0.Add(5*time.Minute), &query.BinlogPos{File: "binlog.000011", Pos: 100}, nil)
	if err != nil {
		t.Fatal(err)
	}
	base, _, err = deltaWindow(t, root, b1, t0.Add(5*time.Minute), changeMap(upd(3, "mine too")), t0.Add(10*time.Minute), &query.BinlogPos{File: "binlog.000011", Pos: 200}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return base
}

// TestResolveTableDelta_anotherChainOfTheSameRangeIsNotLinked: the previous
// snapshot's pair is this chain's only when its footers name this chain. A
// chain of the same range that is another chain (a table written in full and
// changed again as often) must be merged: linking would show the other
// chain's rows as this table's.
func TestResolveTableDelta_anotherChainOfTheSameRangeIsNotLinked(t *testing.T) {
	ctx := context.Background()
	other, _, _ := zooChain(t, 2)
	prevBase, base := other[1], otherZooChain(t)
	if _, _, err := ResolveTableDelta(ctx, prevBase, listedChain(t, prevBase), ""); err != nil {
		t.Fatal(err)
	}
	if _, ok := baseline.FindResolvedTableDelta(prevBase, listedChain(t, prevBase).Files); !ok {
		t.Fatal("no resolved pair for the other chain: the case needs one to be tempted by")
	}
	chain := listedChain(t, base)
	merges := countMerges(t)
	done, linked, err := ResolveTableDelta(ctx, base, chain, prevBase)
	if err != nil || !done || linked || *merges != 1 {
		t.Fatalf("done=%v linked=%v merges=%d err=%v, want a merge", done, linked, *merges, err)
	}
	r, _ := baseline.FindResolvedTableDelta(base, chain.Files)
	if got, want := jsonRows(t, baseline.TableDeltaOnePairStateSQL(sqlLit(base), sqlLit(r.Posdel), sqlLit(r.Upserts), "")), jsonRows(t, chainStateSQL(base, chain)); !reflect.DeepEqual(got, want) {
		t.Fatalf("through the pair:\n %v\nthrough the chain:\n %v", got, want)
	}
}

// TestResolveTableDelta_aFailureLeavesNoPair: a file of the chain that fails
// its check stops the merge before it reads anything, a merge that fails
// leaves nothing a reader would take for a pair, and the posdel a killed run
// left alone is replaced by the next one.
func TestResolveTableDelta_aFailureLeavesNoPair(t *testing.T) {
	ctx := context.Background()
	bases, _, _ := zooChain(t, 2)
	base := bases[1]
	chain := listedChain(t, base)
	want, _ := baseline.ResolvedTableDeltaPaths(base, chain.Files)
	noPair := func(when string) {
		t.Helper()
		if _, ok := baseline.FindResolvedTableDelta(base, chain.Files); ok {
			t.Fatalf("%s: a resolved pair is found", when)
		}
	}
	merges := countMerges(t)
	bad := errors.New("checksum mismatch")
	validateWith(t, func(p string) error {
		if p == chain.Files[1].Upserts {
			return bad
		}
		return nil
	})
	if done, _, err := ResolveTableDelta(ctx, base, chain, ""); done || !errors.Is(err, bad) || *merges != 0 {
		t.Fatalf("a damaged pair: done=%v merges=%d err=%v", done, *merges, err)
	}
	noPair("after a failed check")
	validateWith(t, func(string) error { return nil })

	// A merge that wrote both files and then failed: what it wrote is in
	// the call's own directory and goes with it.
	prev := resolveMerge
	resolveMerge = func(ctx context.Context, basePath string, c *baseline.TableDeltaChain, lo, hi int, outDir string) (*MinorCompaction, error) {
		if _, err := prev(ctx, basePath, c, lo, hi, outDir); err != nil {
			t.Fatal(err)
		}
		return nil, errors.New("no space left on device")
	}
	if done, _, err := ResolveTableDelta(ctx, base, chain, ""); done || err == nil {
		t.Fatalf("a failed merge: done=%v err=%v", done, err)
	}
	resolveMerge = prev
	noPair("after a failed merge")
	if left, err := os.ReadDir(filepath.Dir(want.Upserts)); err != nil || len(left) != 0 {
		t.Fatalf("a failed merge left %v beside the pair's place (err=%v)", left, err)
	}

	// A run killed after the posdel's rename and before the upserts'.
	if err := os.MkdirAll(filepath.Dir(want.Posdel), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(want.Posdel, []byte("half"), 0o644); err != nil {
		t.Fatal(err)
	}
	noPair("with the posdel alone")
	if done, _, err := ResolveTableDelta(ctx, base, chain, ""); !done || err != nil {
		t.Fatalf("over a half pair: done=%v err=%v", done, err)
	}
	r, ok := baseline.FindResolvedTableDelta(base, chain.Files)
	if !ok {
		t.Fatal("no pair after resolving over a half one")
	}
	if got, want := jsonRows(t, baseline.TableDeltaOnePairStateSQL(sqlLit(base), sqlLit(r.Posdel), sqlLit(r.Upserts), "")), jsonRows(t, chainStateSQL(base, chain)); !reflect.DeepEqual(got, want) {
		t.Fatalf("through the pair written over a half one:\n %v\nthrough the chain:\n %v", got, want)
	}
}

// TestResolveTableDelta_twoCallsForOneChain: two daemons on one snapshot
// folder resolve the same chain at the same time. Each works in a directory
// of its own, so neither removes the other's files: both succeed and the
// pair is there, whole. The two calls are held inside their merges until
// both are there, so the overlap does not depend on the scheduler.
func TestResolveTableDelta_twoCallsForOneChain(t *testing.T) {
	ctx := context.Background()
	bases, _, _ := zooChain(t, 2)
	base := bases[1]
	chain := listedChain(t, base)
	want := jsonRows(t, chainStateSQL(base, chain))
	prev := resolveMerge
	t.Cleanup(func() { resolveMerge = prev })
	inside, both := make(chan string, 2), make(chan struct{})
	resolveMerge = func(ctx context.Context, basePath string, c *baseline.TableDeltaChain, lo, hi int, outDir string) (*MinorCompaction, error) {
		inside <- outDir
		<-both
		return prev(ctx, basePath, c, lo, hi, outDir)
	}
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			done, _, err := ResolveTableDelta(ctx, base, chain, "")
			if err == nil && !done {
				err = errors.New("nothing was resolved")
			}
			errs <- err
		}()
	}
	a, b := <-inside, <-inside
	if a == b {
		t.Fatalf("both merges write into %s", a)
	}
	close(both)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	r, ok := baseline.FindResolvedTableDelta(base, chain.Files)
	if !ok || !ResolvedTableDeltaCurrent(base, chain) {
		t.Fatal("no whole pair of this chain after two calls")
	}
	if got := jsonRows(t, baseline.TableDeltaOnePairStateSQL(sqlLit(base), sqlLit(r.Posdel), sqlLit(r.Upserts), "")); !reflect.DeepEqual(got, want) {
		t.Fatalf("through the pair:\n %v\nthrough the chain:\n %v", got, want)
	}
	if left, err := os.ReadDir(filepath.Dir(r.Upserts)); err != nil || len(left) != 2 {
		t.Fatalf("beside the pair: %v (err=%v), want its two files only", left, err)
	}
}

// TestResolveTableDelta_aChainCarriedByCopyIsStillLinked: where a snapshot
// cannot link its previous files (another filesystem) it copies them, so the
// chain's files are not the previous snapshot's inodes. The previous pair is
// still this chain's, by its footers, and is linked: merging it again at
// every refresh would be a table's whole chain checked and merged for
// nothing, for as long as the table does not change.
func TestResolveTableDelta_aChainCarriedByCopyIsStillLinked(t *testing.T) {
	ctx := context.Background()
	bases, root, at := zooChain(t, 2)
	prevBase := bases[1]
	if _, _, err := ResolveTableDelta(ctx, prevBase, listedChain(t, prevBase), ""); err != nil {
		t.Fatal(err)
	}
	quiet, _, err := deltaWindow(t, root, prevBase, at, map[string]*query.ResultRow{}, at.Add(5*time.Minute), &query.BinlogPos{File: "binlog.000009", Pos: 9000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Each file of the carried chain becomes a copy.
	for _, p := range listedChain(t, quiet).Paths() {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if sameFile(listedChain(t, quiet).Last().Upserts, listedChain(t, prevBase).Last().Upserts) {
		t.Fatal("the chain is still linked: the case needs copies")
	}
	merges := countMerges(t)
	if done, linked, err := ResolveTableDelta(ctx, quiet, listedChain(t, quiet), prevBase); err != nil || !done || !linked || *merges != 0 {
		t.Fatalf("done=%v linked=%v merges=%d err=%v, want the previous pair linked", done, linked, *merges, err)
	}
}

// TestResolveTableDelta_removesWhatAKilledRunLeft: a daemon killed in the
// middle of a merge leaves its work directory. The next call for a table of
// that schema removes those old enough to be no run's, and leaves a recent
// one, which may be a merge still at work.
func TestResolveTableDelta_removesWhatAKilledRunLeft(t *testing.T) {
	bases, _, _ := zooChain(t, 2)
	base := bases[1]
	chain := listedChain(t, base)
	want, _ := baseline.ResolvedTableDeltaPaths(base, chain.Files)
	dir := filepath.Dir(want.Upserts)
	stale, recent := filepath.Join(dir, resolveWorkPrefix+"killed"), filepath.Join(dir, resolveWorkPrefix+"running")
	for _, d := range []string{stale, recent} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "orders.000000-000001.upserts.tmp"), []byte("half"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-resolveWorkMaxAge - time.Minute)
	if err := os.Chtimes(stale, past, past); err != nil {
		t.Fatal(err)
	}
	if done, _, err := ResolveTableDelta(context.Background(), base, chain, ""); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("a killed run's work directory is still there (stat err=%v)", err)
	}
	if _, err := os.Stat(recent); err != nil {
		t.Errorf("a recent work directory, maybe a merge at work, was removed: %v", err)
	}
}

// TestResolveTableDelta_aPairOfAnotherChainIsReplaced: a reader trusts a pair
// by the snapshot's name and the range. A pair under that name that was
// merged from another chain (a snapshot directory replaced by hand, with a
// chain of the same length) is not this chain's: the job that looks after
// each refresh finds it is not current and writes this chain's over it.
func TestResolveTableDelta_aPairOfAnotherChainIsReplaced(t *testing.T) {
	ctx := context.Background()
	other, _, _ := zooChain(t, 2)
	base := otherZooChain(t)
	chain := listedChain(t, base)
	if _, _, err := ResolveTableDelta(ctx, other[1], listedChain(t, other[1]), ""); err != nil {
		t.Fatal(err)
	}
	theirs, ok := baseline.FindResolvedTableDelta(other[1], listedChain(t, other[1]).Files)
	mine, ok2 := baseline.ResolvedTableDeltaPaths(base, chain.Files)
	if !ok || !ok2 || filepath.Base(theirs.Upserts) != filepath.Base(mine.Upserts) {
		t.Fatalf("the two chains do not share a range name: %v %v", theirs, mine)
	}
	if err := os.MkdirAll(filepath.Dir(mine.Upserts), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, cp := range [][2]string{{theirs.Posdel, mine.Posdel}, {theirs.Upserts, mine.Upserts}} {
		raw, err := os.ReadFile(cp[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cp[1], raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, found := baseline.FindResolvedTableDelta(base, chain.Files); !found {
		t.Fatal("the planted pair is not found by name: the case needs it to be")
	}
	if ResolvedTableDeltaCurrent(base, chain) {
		t.Fatal("a pair merged from another chain passes as this chain's")
	}
	merges := countMerges(t)
	if done, linked, err := ResolveTableDelta(ctx, base, chain, ""); err != nil || !done || linked || *merges != 1 {
		t.Fatalf("done=%v linked=%v merges=%d err=%v, want the pair merged again", done, linked, *merges, err)
	}
	if !ResolvedTableDeltaCurrent(base, chain) {
		t.Fatal("the pair is still not this chain's after resolving")
	}
	r, _ := baseline.FindResolvedTableDelta(base, chain.Files)
	if got, want := jsonRows(t, baseline.TableDeltaOnePairStateSQL(sqlLit(base), sqlLit(r.Posdel), sqlLit(r.Upserts), "")), jsonRows(t, chainStateSQL(base, chain)); !reflect.DeepEqual(got, want) {
		t.Fatalf("through the replaced pair:\n %v\nthrough the chain:\n %v", got, want)
	}
}

// TestResolveTableDelta_aChainThatStartsWithARange: after a compaction is
// adopted the chain is a range pair and the pairs after it. Its resolved pair
// covers all of them and reads as the chain does.
func TestResolveTableDelta_aChainThatStartsWithARange(t *testing.T) {
	ctx := context.Background()
	bases, _, _ := zooChain(t, 4)
	base := bases[3]
	plain := listedChain(t, base)
	want := jsonRows(t, chainStateSQL(base, plain))
	mc, err := CompactTableDeltaMinor(ctx, base, plain, 0, 2, filepath.Join(t.TempDir(), "staged"))
	if err != nil {
		t.Fatal(err)
	}
	installRange(t, base, plain, mc)
	chain := listedChain(t, base)
	if len(chain.Files) != 2 || !chain.Files[0].Range() {
		t.Fatalf("chain = %+v, want a range and one pair", chain.Files)
	}
	if done, _, err := ResolveTableDelta(ctx, base, chain, ""); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	r, ok := baseline.FindResolvedTableDelta(base, chain.Files)
	if !ok || r.SeqLo != 0 || r.Seq != 3 {
		t.Fatalf("pair = %+v ok=%v, want the range 0-3", r, ok)
	}
	if got := jsonRows(t, baseline.TableDeltaOnePairStateSQL(sqlLit(base), sqlLit(r.Posdel), sqlLit(r.Upserts), "")); !reflect.DeepEqual(got, want) {
		t.Fatalf("through the pair:\n %v\nthrough the chain before the range:\n %v", got, want)
	}
}
