package reconstruct

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/keyidx"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2496 slice 4: the key index is used by default, and a lookup over a base
// that has none (or an unusable one) builds it there and then, once per
// version of the base; a build that cannot produce one is not retried while
// the base keeps its identity.

// countIndexBuilds wraps the build seam and returns a counter of the calls.
func countIndexBuilds(t *testing.T) *int32 {
	t.Helper()
	var n int32
	prev := writeIndexBeside
	writeIndexBeside = func(ctx context.Context, schema, table, basePath string, keyCols []string, tuning duckdbutil.Tuning) keyidx.Result {
		atomic.AddInt32(&n, 1)
		return prev(ctx, schema, table, basePath, keyCols, tuning)
	}
	t.Cleanup(func() { writeIndexBeside = prev })
	return &n
}

// forceBuildOutcome replaces the build seam with one that builds nothing and
// returns res (a skip when res.Skipped is set, a failure otherwise: WriteBeside
// swallows the error and returns a Result with no index written).
func forceBuildOutcome(t *testing.T, res keyidx.Result) *int32 {
	t.Helper()
	var n int32
	prev := writeIndexBeside
	writeIndexBeside = func(ctx context.Context, schema, table, basePath string, keyCols []string, tuning duckdbutil.Tuning) keyidx.Result {
		atomic.AddInt32(&n, 1)
		res.Path = keyidx.PathFor(basePath)
		return res
	}
	t.Cleanup(func() { writeIndexBeside = prev })
	return &n
}

// freshMemo empties the process-wide no-index memo for the test and after it.
func freshMemo(t *testing.T) {
	t.Helper()
	resetNoIndexMemo()
	t.Cleanup(resetNoIndexMemo)
}

// indexLookup is the production lookup with the switch on.
func indexLookup(t *testing.T, ctx context.Context, base string, pkCols []metadata.ColumnMeta, keys map[string]*query.ResultRow) ([]int64, lookupStats, error) {
	t.Helper()
	useKeyIndex(t, true)
	return lookupBasePositions(ctx, base, "s", "t", pkCols, keys, duckdbutil.DefaultTuning())
}

// rewriteBase writes rows to base again under the same path with a later
// modification time: a new version of the file (size, mtime), as a rewrite
// leaves one.
func rewriteBase(t *testing.T, base, ddl string, rows [][]string) {
	t.Helper()
	cols, err := baseline.ParseSchemaText(ddl)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(base, later, later); err != nil {
		t.Fatal(err)
	}
}

// TestKeyIndexOn_defaultOn_2496s4: unset is on; the off spellings are off in
// any case with spaces around; anything else is on (the default) with a
// warning that names the accepted spellings and says the default applies.
func TestKeyIndexOn_defaultOn_2496s4(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		on   bool
		warn bool
	}{
		{"", true, false}, {"1", true, false}, {"TRUE", true, false}, {" on ", true, false}, {"yes", true, false},
		{"0", false, false}, {"Off", false, false}, {"no", false, false}, {" false\n", false, false},
		{"maybe", true, true}, {"2", true, true},
	} {
		on, warning := keyIndexOn(tc.raw)
		if on != tc.on || (warning != "") != tc.warn {
			t.Errorf("%q: on=%v warning=%q; want on=%v warning=%v", tc.raw, on, warning, tc.on, tc.warn)
		}
		if tc.warn {
			for _, want := range []string{KeyIndexEnv, "1/true/yes/on", "0/false/no/off", "default"} {
				if !strings.Contains(warning, want) {
					t.Errorf("%q: the warning lacks %q: %s", tc.raw, want, warning)
				}
			}
		}
	}
}

// TestKeyIndexSwitch_saysItsStateAtBoot_2496s4: on (the default) and off are
// each one Info line naming the variable; an unreadable value is the WARN
// plus the on line, since the default applies.
func TestKeyIndexSwitch_saysItsStateAtBoot_2496s4(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		on    bool
		lines []string
	}{
		{"", true, []string{"level=INFO", "by default", "BINTRAIL_KEY_INDEX=0"}},
		{"1", true, []string{"level=INFO", "enabled by " + KeyIndexEnv}},
		{"0", false, []string{"level=INFO", "disabled by " + KeyIndexEnv, "scans the table's file"}},
		{"maybe", true, []string{"level=WARN", "level=INFO", "default"}},
	} {
		log := captureDebugLog(t)
		sw := newKeyIndexSwitch(func(string) string { return tc.raw })
		for range 3 {
			if sw() != tc.on {
				t.Fatalf("%q: on=%v, want %v", tc.raw, sw(), tc.on)
			}
		}
		for _, want := range tc.lines {
			if !strings.Contains(log.String(), want) {
				t.Fatalf("%q: no %q in the log:\n%s", tc.raw, want, log.String())
			}
		}
		// "by default" is for the empty value only (F8): an explicit 1 is
		// said as the operator's choice.
		if tc.raw == "1" && strings.Contains(log.String(), "by default") {
			t.Fatalf("%q: an explicit value is said as the default:\n%s", tc.raw, log.String())
		}
		wantLines := 1
		if tc.raw == "maybe" {
			wantLines = 2
		}
		lines := 0
		for _, l := range strings.Split(log.String(), "\n") {
			if strings.Contains(l, KeyIndexEnv) {
				lines++
			}
		}
		if lines != wantLines {
			t.Fatalf("%q: %d lines naming the switch, want %d:\n%s", tc.raw, lines, wantLines, log.String())
		}
		if strings.Contains(log.String(), "level=INFO") && !strings.Contains(log.String(), "env="+KeyIndexEnv) {
			t.Fatalf("%q: the Info line does not carry the variable:\n%s", tc.raw, log.String())
		}
	}
}

// TestLookupBasePositions_buildsTheIndexOnFirstMiss_2496s4: a base with no
// index gets one built by the lookup, which then resolves through it in the
// same call, reports the build's time, and the next lookup over the same base
// builds nothing.
func TestLookupBasePositions_buildsTheIndexOnFirstMiss_2496s4(t *testing.T) {
	freshMemo(t)
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	builds := countIndexBuilds(t)
	log := captureDebugLog(t)
	got, st, err := indexLookup(t, context.Background(), base, pkCols, touched("1", "3"))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "index" || st.Built <= 0 || *builds != 1 {
		t.Fatalf("first lookup: got=%v st=%+v builds=%d err=%v\n%s", got, st, *builds, err, log.String())
	}
	if ok, err := keyidx.Bound(base); !ok || err != nil {
		t.Fatalf("no bound index beside the base after the lookup (ok=%v err=%v)", ok, err)
	}
	if !strings.Contains(log.String(), `msg="key index written"`) {
		t.Fatalf("the build's own Info line is missing:\n%s", log.String())
	}
	got, st, err = indexLookup(t, context.Background(), base, pkCols, touched("2"))
	if err != nil || !reflect.DeepEqual(got, []int64{1}) || st.Path != "index" || st.Built != 0 || *builds != 1 {
		t.Fatalf("second lookup: got=%v st=%+v builds=%d err=%v", got, st, *builds, err)
	}
}

// TestLookupBasePositions_skippedBuildIsNotRetried_2496s4: a build the
// writer skips (disk, a non-integer key per DuckDB) sends the lookup to the
// join; the next lookup over the same base does not try again, and a
// rewritten base (new identity) gets one new attempt.
func TestLookupBasePositions_skippedBuildIsNotRetried_2496s4(t *testing.T) {
	freshMemo(t)
	rows := [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}}
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, rows)
	builds := forceBuildOutcome(t, keyidx.Result{Skipped: "free space short beside the base", Warn: true})
	log := captureDebugLog(t)
	got, st, err := indexLookup(t, context.Background(), base, pkCols, touched("1", "3"))
	// The attempt's time is reported even when it produced nothing: a
	// build that fails late (a NULL key at the end of the sort) is a cost
	// the refresh paid.
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "join" || st.Built <= 0 || *builds != 1 {
		t.Fatalf("first lookup: got=%v st=%+v builds=%d err=%v", got, st, *builds, err)
	}
	if _, err := os.Stat(keyidx.PathFor(base)); !os.IsNotExist(err) {
		t.Fatalf("an index beside the base after a skipped build: %v", err)
	}
	log.Reset()
	got, st, err = indexLookup(t, context.Background(), base, pkCols, touched("2"))
	if err != nil || !reflect.DeepEqual(got, []int64{1}) || st.Path != "join" || *builds != 1 {
		t.Fatalf("second lookup: got=%v st=%+v builds=%d err=%v", got, st, *builds, err)
	}
	if !strings.Contains(log.String(), "level=DEBUG") || !strings.Contains(log.String(), "not tried again") || strings.Contains(log.String(), "level=WARN") {
		t.Fatalf("the memoised miss is not one Debug line:\n%s", log.String())
	}
	// A new version of the base: one new attempt.
	rewriteBase(t, base, intKeyDDL, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}, {"4", "d"}})
	got, st, err = indexLookup(t, context.Background(), base, pkCols, touched("4"))
	if err != nil || !reflect.DeepEqual(got, []int64{3}) || st.Path != "join" || *builds != 2 {
		t.Fatalf("rewritten base: got=%v st=%+v builds=%d err=%v", got, st, *builds, err)
	}
}

// TestLookupBasePositions_failedBuildIsNotRetried_2496s4: a build that fails
// (the writer leaves nothing) is the same: join now, no retry on the same
// base, one retry on a rewritten one.
func TestLookupBasePositions_failedBuildIsNotRetried_2496s4(t *testing.T) {
	freshMemo(t)
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	builds := forceBuildOutcome(t, keyidx.Result{})
	for i := range 2 {
		got, st, err := indexLookup(t, context.Background(), base, pkCols, touched("1", "3"))
		if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "join" || *builds != 1 {
			t.Fatalf("lookup %d: got=%v st=%+v builds=%d err=%v", i+1, got, st, *builds, err)
		}
	}
	rewriteBase(t, base, intKeyDDL, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}, {"4", "d"}})
	if _, _, err := indexLookup(t, context.Background(), base, pkCols, touched("4")); err != nil || *builds != 2 {
		t.Fatalf("rewritten base: builds=%d err=%v", *builds, err)
	}
}

// TestLookupBasePositions_unusableIndexIsRebuilt_2496s4: an index that is
// there and cannot be used (damaged, bound to another version of the base,
// built on other key columns) is said at WARN with its cause and "rebuilding
// it now", replaced, and the same lookup resolves through the new one.
func TestLookupBasePositions_unusableIndexIsRebuilt_2496s4(t *testing.T) {
	const intVDDL = "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	keys := touched("1", "3", "-4", "9")
	want := []int64{0, 2, 3}
	for _, c := range []struct {
		name    string
		prepare func(t *testing.T, base string)
		text    string
	}{
		{"corrupt", func(t *testing.T, base string) {
			indexBeside(t, base, "id")
			flip(t, keyidx.PathFor(base), hdr("id")+5)
		}, "corrupt"},
		{"unbound", func(t *testing.T, base string) {
			indexBeside(t, base, "id")
			rewriteBase(t, base, intVDDL, [][]string{{"1", "7"}, {"2", "7"}, {"3", "7"}, {"-4", "7"}})
		}, "not bound"},
		{"mismatch", func(t *testing.T, base string) {
			indexBeside(t, base, "v")
		}, "other key columns"},
	} {
		t.Run(c.name, func(t *testing.T) {
			freshMemo(t)
			base, pkCols := writeKeyedBaseline(t, intVDDL, []string{"id"}, [][]string{{"1", "7"}, {"2", "7"}, {"3", "7"}, {"-4", "7"}})
			c.prepare(t, base)
			stale, err := os.ReadFile(keyidx.PathFor(base))
			if err != nil {
				t.Fatal(err)
			}
			builds := countIndexBuilds(t)
			log := captureDebugLog(t)
			got, st, err := indexLookup(t, context.Background(), base, pkCols, keys)
			if err != nil || !reflect.DeepEqual(got, want) || st.Path != "index" || st.Built <= 0 || *builds != 1 {
				t.Fatalf("got=%v st=%+v builds=%d err=%v\n%s", got, st, *builds, err, log.String())
			}
			var line string
			for _, l := range strings.Split(log.String(), "\n") {
				if strings.Contains(l, "level=WARN") && strings.Contains(l, "key index") {
					line = l
				}
			}
			if line == "" || !strings.Contains(line, c.text) || !strings.Contains(line, "rebuilding it now") || !strings.Contains(line, " path="+keyidx.PathFor(base)) {
				t.Fatalf("no WARN naming the cause %q, the path and the rebuild:\n%s", c.text, log.String())
			}
			if strings.Contains(log.String(), "next full rewrite") || strings.Contains(log.String(), "remove it to silence") {
				t.Fatalf("the old remediation is still said:\n%s", log.String())
			}
			fresh, err := os.ReadFile(keyidx.PathFor(base))
			if err != nil || reflect.DeepEqual(fresh, stale) {
				t.Fatalf("the stale index was not replaced (err=%v)", err)
			}
			if ok, err := keyidx.Bound(base); !ok || err != nil {
				t.Fatalf("the rebuilt index is not bound (ok=%v err=%v)", ok, err)
			}
		})
	}
}

// TestLookupBasePositions_staleIndexThatCannotBeRemovedFallsBack_2496s4: when
// the stale file cannot be removed, nothing is built over it; one WARN, and
// the join answers this lookup.
func TestLookupBasePositions_staleIndexThatCannotBeRemovedFallsBack_2496s4(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root removes anything")
	}
	freshMemo(t)
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	indexBeside(t, base, "id")
	flip(t, keyidx.PathFor(base), hdr("id")+5)
	dir := filepath.Dir(base)
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })
	builds := countIndexBuilds(t)
	log := captureDebugLog(t)
	got, st, err := indexLookup(t, context.Background(), base, pkCols, touched("1", "3"))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "join" || *builds != 0 {
		t.Fatalf("got=%v st=%+v builds=%d err=%v\n%s", got, st, *builds, err, log.String())
	}
	if !strings.Contains(log.String(), "level=WARN") || !strings.Contains(log.String(), "could not be removed") {
		t.Fatalf("no WARN about the removal:\n%s", log.String())
	}
}

// TestLookupBasePositions_cancelledBuildIsNotMemoised_2496s4: a build cut
// short by the context is a Debug line and leaves no memo entry, so the next
// lookup with a live context builds.
func TestLookupBasePositions_cancelledBuildIsNotMemoised_2496s4(t *testing.T) {
	freshMemo(t)
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	builds := countIndexBuilds(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	log := captureDebugLog(t)
	_, st, _ := indexLookup(t, ctx, base, pkCols, touched("1", "3"))
	if st.Path == "index" || *builds != 1 {
		t.Fatalf("cancelled: st=%+v builds=%d", st, *builds)
	}
	for _, l := range strings.Split(log.String(), "\n") {
		// The join's DuckDB session warns about its SETs under a cancelled
		// context; the lines about the index must all be Debug.
		if strings.Contains(l, "key index") && strings.Contains(l, "level=WARN") {
			t.Fatalf("a cancelled build is a Debug line, not a WARN:\n%s", log.String())
		}
	}
	if !strings.Contains(log.String(), "cancelled") {
		t.Fatalf("no line about the cancellation:\n%s", log.String())
	}
	if _, memo := noIndexMemoEntry(base); memo {
		t.Fatal("a cancelled build left a memo entry")
	}
	got, st, err := indexLookup(t, context.Background(), base, pkCols, touched("1", "3"))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "index" || *builds != 2 {
		t.Fatalf("after the cancellation: got=%v st=%+v builds=%d err=%v", got, st, *builds, err)
	}
}

// TestLookupBasePositions_switchOffBuildsNothing_2496s4: with the switch
// off there is no build and no use, as before.
func TestLookupBasePositions_switchOffBuildsNothing_2496s4(t *testing.T) {
	freshMemo(t)
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	builds := countIndexBuilds(t)
	opens := countIndexOpens(t)
	useKeyIndex(t, false)
	got, st, err := lookupBasePositions(context.Background(), base, "s", "t", pkCols, touched("1", "3"), duckdbutil.DefaultTuning())
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "join" || *builds != 0 || *opens != 0 {
		t.Fatalf("got=%v st=%+v builds=%d opens=%d err=%v", got, st, *builds, *opens, err)
	}
	if _, err := os.Stat(keyidx.PathFor(base)); !os.IsNotExist(err) {
		t.Fatalf("an index was written with the switch off: %v", err)
	}
}

// TestLookupBasePositions_s3BaseBuildsNothing_2496s4: a base on S3 never
// gets a build attempt.
func TestLookupBasePositions_s3BaseBuildsNothing_2496s4(t *testing.T) {
	freshMemo(t)
	builds := countIndexBuilds(t)
	_, st, _ := lookupBasePositionsVia(context.Background(), "s3://bucket/x/t.parquet", "s", "t", pkColsIntID(), touched("1"), duckdbutil.DefaultTuning(), false, true)
	if st.Path == "index" || *builds != 0 {
		t.Fatalf("st=%+v builds=%d", st, *builds)
	}
}

// TestLookupBasePositions_concurrentBuildsWait_2496s4: two tables looked up
// at once each build their own index, one build at a time (the second waits
// for the slot), both end on the index path, and the seam is called once per
// base.
func TestLookupBasePositions_concurrentBuildsWait_2496s4(t *testing.T) {
	freshMemo(t)
	var cur, peak atomic.Int32
	keyidx.BuildHook = func() {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		cur.Add(-1)
	}
	t.Cleanup(func() { keyidx.BuildHook = nil })
	builds := countIndexBuilds(t)
	useKeyIndex(t, true)
	bases := make([]string, 2)
	var pkCols []metadata.ColumnMeta
	for i := range bases {
		bases[i], pkCols = writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	stats := make([]lookupStats, 2)
	for i, base := range bases {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, stats[i], errs[i] = lookupBasePositions(context.Background(), base, "s", fmt.Sprintf("t%d", i), pkCols, touched("1", "3"), duckdbutil.DefaultTuning())
		}()
	}
	wg.Wait()
	for i := range bases {
		if errs[i] != nil || stats[i].Path != "index" || stats[i].Built <= 0 {
			t.Fatalf("table %d: st=%+v err=%v", i, stats[i], errs[i])
		}
	}
	if *builds != 2 || peak.Load() != 1 {
		t.Fatalf("builds=%d peak=%d; want one build per base, one at a time", *builds, peak.Load())
	}
}

// TestTableDelta_reportsTheIndexBuild_2496s4: a refresh over a base with no
// index builds it, the report carries the build's time and the per-table
// line says lookup_index_built_ms=; the next refresh over the carried base
// (its index linked beside it) reports no build and the key is absent.
func TestTableDelta_reportsTheIndexBuild_2496s4(t *testing.T) {
	noCompaction(t)
	freshMemo(t)
	useKeyIndex(t, true)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := indexedBase(t, changeMap(upd(1, "a")))
	if err := os.Remove(keyidx.PathFor(f.ref1)); err != nil {
		t.Fatal(err)
	}
	log := captureDebugLog(t)
	base2, rep, err := deltaWindow(t, t.TempDir(), f.ref1, f.at1, changeMap(upd(1, "s2"), ins(10, "ten")), f.at2, f.cut2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.LookupPath != "index" || rep.LookupBuilt <= 0 {
		t.Fatalf("first refresh: lookup_path=%q built=%v:\n%s", rep.LookupPath, rep.LookupBuilt, log.String())
	}
	line := deltaLine(t, log.String())
	if !strings.Contains(line+" ", fmt.Sprintf(" lookup_index_built_ms=%d ", rep.LookupBuilt.Milliseconds())) {
		t.Fatalf("the delta line lacks lookup_index_built_ms:\n%s", line)
	}
	if ok, err := keyidx.Bound(base2); !ok || err != nil {
		t.Fatalf("the carried base has no bound index (ok=%v err=%v)", ok, err)
	}
	log.Reset()
	_, rep, err = deltaWindow(t, t.TempDir(), base2, f.at2, changeMap(del(2), upd(10, "ten-b")), t0.Add(15*time.Minute), &query.BinlogPos{File: "binlog.000009", Pos: 3000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.LookupPath != "index" || rep.LookupBuilt != 0 {
		t.Fatalf("second refresh: lookup_path=%q built=%v", rep.LookupPath, rep.LookupBuilt)
	}
	if line := deltaLine(t, log.String()); strings.Contains(line, "lookup_index_built_ms") {
		t.Fatalf("the delta line says a build ran when none did:\n%s", line)
	}
}

// deltaLine is the per-table delta line in log.
func deltaLine(t *testing.T, log string) string {
	t.Helper()
	var line string
	for _, l := range strings.Split(log, "\n") {
		if strings.Contains(l, `msg="table published as a delta over its previous file"`) {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no delta line in the log:\n%s", log)
	}
	return line
}

// TestTableReport_addLookupSumsTheBuild_2496s4: Built flows from the stats
// to the report, summed over passes.
func TestTableReport_addLookupSumsTheBuild_2496s4(t *testing.T) {
	var rep TableReport
	rep.addLookup(lookupStats{Path: "index", Built: 3 * time.Millisecond})
	rep.addLookup(lookupStats{Path: "index"})
	if rep.LookupBuilt != 3*time.Millisecond || rep.LookupPath != "index" {
		t.Fatalf("report %+v", rep)
	}
}

// TestCarryForward_copyArmSaysTheNextRefreshBuilds_2496s4: the copy arm's
// Debug line says what now happens: no index is built beside the copy, the
// base is read through DuckDB until the folders share a filesystem (round
// 2, F2; the lookup half is in tabledelta_2496s4r2_test.go).
func TestCarryForward_copyArmSaysTheNextRefreshBuilds_2496s4(t *testing.T) {
	base, _ := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}})
	indexBeside(t, base, "id")
	t.Cleanup(StubLinkFileForTest(func(string, string) error { return syscall.EXDEV }))
	log := captureDebugLog(t)
	if _, err := carryForward(context.Background(), base, t.TempDir(), "s", "t"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(log.String(), "none is built beside the copy") || !strings.Contains(log.String(), "until the snapshot folders share a filesystem") {
		t.Fatalf("the copy arm does not say no index is built beside the copy:\n%s", log.String())
	}
}
