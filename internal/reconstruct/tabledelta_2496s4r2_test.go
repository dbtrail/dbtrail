package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/keyidx"
)

// #2496 slice 4, round 2: what the lookup does when the index cannot be
// replaced, cannot be checked, or is unusable again after a rebuild, and
// what a copied base gets. In every case the second lookup over the same
// base must be quiet: one Debug line, no build, no WARN.

// keyIndexLines returns the log lines about the key index at the level.
func keyIndexLines(log, level string) []string {
	var out []string
	for _, l := range strings.Split(log, "\n") {
		if strings.Contains(l, "key index") && strings.Contains(l, "level="+level) {
			out = append(out, l)
		}
	}
	return out
}

// TestLookupBasePositions_unremovableStaleIndexIsNotRetried_2496s4r2 (F1): a
// damaged index in a folder that will not let it go is one WARN at the first
// lookup; the second lookup over the same base is one Debug line, no build,
// no WARN, the join answers.
func TestLookupBasePositions_unremovableStaleIndexIsNotRetried_2496s4r2(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root removes anything")
	}
	// Damage the open sees (a header byte: ErrCorrupt) and damage only the
	// Find sees (a body block's CRC): two sites that remove the stale file.
	for name, at := range map[string]int{"header": hdr() + 1 + 2, "body": hdr("id") + 5} {
		t.Run(name, func(t *testing.T) { unremovableStaleIndex(t, at) })
	}
}

func unremovableStaleIndex(t *testing.T, at int) {
	freshMemo(t)
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	indexBeside(t, base, "id")
	flip(t, keyidx.PathFor(base), at)
	dir := filepath.Dir(base)
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })
	builds := countIndexBuilds(t)
	log := captureDebugLog(t)
	got, st, err := indexLookup(t, context.Background(), base, pkCols, touched("1", "3"))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "join" || *builds != 0 {
		t.Fatalf("first lookup: got=%v st=%+v builds=%d err=%v\n%s", got, st, *builds, err, log.String())
	}
	if n := len(keyIndexLines(log.String(), "WARN")); n == 0 {
		t.Fatalf("the first lookup did not warn:\n%s", log.String())
	}
	log.Reset()
	got, st, err = indexLookup(t, context.Background(), base, pkCols, touched("2"))
	if err != nil || !reflect.DeepEqual(got, []int64{1}) || st.Path != "join" || *builds != 0 {
		t.Fatalf("second lookup: got=%v st=%+v builds=%d err=%v", got, st, *builds, err)
	}
	if warns := keyIndexLines(log.String(), "WARN"); len(warns) != 0 {
		t.Fatalf("the second lookup warned again:\n%s", strings.Join(warns, "\n"))
	}
	if debugs := keyIndexLines(log.String(), "DEBUG"); len(debugs) != 1 || !strings.Contains(debugs[0], "not tried again") {
		t.Fatalf("the second lookup is not one Debug line saying it is not tried again:\n%s", log.String())
	}
}

// TestCarryForward_copiedBaseGetsNoBuild_2496s4r2 (F2): a base carried by
// copy has a new inode and modification time at every refresh, so a build
// beside it would be paid at every refresh; the carry remembers the copy as
// a base not to build beside, says so at Debug, and the next lookup over it
// builds nothing, warns nothing, and takes the join.
func TestCarryForward_copiedBaseGetsNoBuild_2496s4r2(t *testing.T) {
	freshMemo(t)
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	indexBeside(t, base, "id")
	t.Cleanup(StubLinkFileForTest(func(string, string) error { return syscall.EXDEV }))
	log := captureDebugLog(t)
	dir := t.TempDir()
	linked, err := carryForward(context.Background(), base, dir, "s", "t")
	if err != nil || linked {
		t.Fatalf("linked=%v err=%v", linked, err)
	}
	if !strings.Contains(log.String(), "until the snapshot folders share a filesystem") {
		t.Fatalf("the copy arm does not say the base is read through DuckDB until the folders share a filesystem:\n%s", log.String())
	}
	dst := filepath.Join(dir, "s", "t.parquet")
	builds := countIndexBuilds(t)
	log.Reset()
	got, st, err := indexLookup(t, context.Background(), dst, pkCols, touched("1", "3"))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "join" || *builds != 0 {
		t.Fatalf("lookup over the copy: got=%v st=%+v builds=%d err=%v\n%s", got, st, *builds, err, log.String())
	}
	if warns := keyIndexLines(log.String(), "WARN"); len(warns) != 0 {
		t.Fatalf("the lookup over the copy warned:\n%s", strings.Join(warns, "\n"))
	}
	if _, err := os.Stat(keyidx.PathFor(dst)); !os.IsNotExist(err) {
		t.Fatalf("an index beside the copy: %v", err)
	}
}

// TestLookupBasePositions_baseReadErrorStandsAside_2496s4r2 (F3): an error
// from the open that is not the index's (the base could not be read: a
// permission, an I/O error) is not damage; the lookup warns, leaves the
// index alone, builds nothing, remembers nothing, and the join answers. The
// next lookup, with the base readable again, takes the untouched index.
func TestLookupBasePositions_baseReadErrorStandsAside_2496s4r2(t *testing.T) {
	freshMemo(t)
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	indexBeside(t, base, "id")
	before, err := os.ReadFile(keyidx.PathFor(base))
	if err != nil {
		t.Fatal(err)
	}
	builds := countIndexBuilds(t)
	prev := keyIndexOpen
	keyIndexOpen = func(string) (*keyidx.Reader, error) {
		return nil, errors.New("read the footer of " + base + ": input/output error")
	}
	log := captureDebugLog(t)
	got, st, err := indexLookup(t, context.Background(), base, pkCols, touched("1", "3"))
	keyIndexOpen = prev
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "join" || *builds != 0 {
		t.Fatalf("got=%v st=%+v builds=%d err=%v\n%s", got, st, *builds, err, log.String())
	}
	warns := keyIndexLines(log.String(), "WARN")
	if len(warns) != 1 || !strings.Contains(warns[0], "could not be checked") || strings.Contains(warns[0], "rebuilding") {
		t.Fatalf("want one WARN standing aside, not a rebuild:\n%s", log.String())
	}
	after, err := os.ReadFile(keyidx.PathFor(base))
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("the index was touched (err=%v)", err)
	}
	if _, memo := noIndexMemoEntry(base); memo {
		t.Fatal("a transient error left a memo entry")
	}
	got, st, err = indexLookup(t, context.Background(), base, pkCols, touched("1", "3"))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "index" || *builds != 0 {
		t.Fatalf("after the error: got=%v st=%+v builds=%d err=%v", got, st, *builds, err)
	}
}

// flipBodyAfterBuild wraps the build seam: the real build, then one byte of
// the first body block flipped, so the index opens (header and skip table
// agree) and fails at Find (the block's CRC).
func flipBodyAfterBuild(t *testing.T) *int32 {
	t.Helper()
	n := countIndexBuilds(t)
	prev := writeIndexBeside
	writeIndexBeside = func(ctx context.Context, schema, table, basePath string, keyCols []string, tuning duckdbutil.Tuning) keyidx.Result {
		res := prev(ctx, schema, table, basePath, keyCols, tuning)
		flip(t, keyidx.PathFor(basePath), hdr("id")+5)
		return res
	}
	t.Cleanup(func() { writeIndexBeside = prev })
	return n
}

// TestLookupBasePositions_rebuiltAndStillUnusableIsNotRetried_2496s4r2 (F4):
// an index that fails again right after being rebuilt is a deterministic
// defect, not damage: the lookup says so once, removes it, remembers the
// base, and the next lookup builds nothing and warns nothing.
func TestLookupBasePositions_rebuiltAndStillUnusableIsNotRetried_2496s4r2(t *testing.T) {
	freshMemo(t)
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	builds := flipBodyAfterBuild(t)
	log := captureDebugLog(t)
	got, st, err := indexLookup(t, context.Background(), base, pkCols, touched("1", "3"))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "join" || *builds != 2 {
		t.Fatalf("first lookup: got=%v st=%+v builds=%d err=%v\n%s", got, st, *builds, err, log.String())
	}
	warns := keyIndexLines(log.String(), "WARN")
	var last string
	if len(warns) > 0 {
		last = warns[len(warns)-1]
	}
	if !strings.Contains(last, "not built again until the file is rewritten or DBTrail restarts") || strings.Contains(log.String(), "remove it to silence this") {
		t.Fatalf("the second failure does not name the retry condition:\n%s", log.String())
	}
	if _, err := os.Stat(keyidx.PathFor(base)); !os.IsNotExist(err) {
		t.Fatalf("the unusable index was left beside the base: %v", err)
	}
	log.Reset()
	got, st, err = indexLookup(t, context.Background(), base, pkCols, touched("2"))
	if err != nil || !reflect.DeepEqual(got, []int64{1}) || st.Path != "join" || *builds != 2 {
		t.Fatalf("second lookup: got=%v st=%+v builds=%d err=%v", got, st, *builds, err)
	}
	if warns := keyIndexLines(log.String(), "WARN"); len(warns) != 0 {
		t.Fatalf("the second lookup warned:\n%s", strings.Join(warns, "\n"))
	}
}

// TestNoIndexMemo_capClearsWithOneDebugLine_2496s4r2 (F7): the memo is
// bounded; adding past the cap empties it with one Debug line, and the
// entry being added survives (one re-probe per base is the whole cost).
func TestNoIndexMemo_capClearsWithOneDebugLine_2496s4r2(t *testing.T) {
	freshMemo(t)
	prev := noIndexCap
	noIndexCap = 2
	t.Cleanup(func() { noIndexCap = prev })
	var bases []string
	for range 3 {
		b, _ := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}})
		bases = append(bases, b)
	}
	log := captureDebugLog(t)
	rememberNoIndex(bases[0])
	rememberNoIndex(bases[1])
	if _, ok := noIndexMemoEntry(bases[0]); !ok || log.Len() != 0 {
		t.Fatalf("two entries under a cap of two: entry=%v log=%q", ok, log.String())
	}
	rememberNoIndex(bases[2])
	if _, ok := noIndexMemoEntry(bases[0]); ok {
		t.Fatal("the memo was not cleared past the cap")
	}
	if _, ok := noIndexMemoEntry(bases[2]); !ok {
		t.Fatal("the entry that crossed the cap was dropped")
	}
	if debugs := keyIndexLines(log.String(), "DEBUG"); len(debugs) != 1 || !strings.Contains(debugs[0], "cap") {
		t.Fatalf("want one Debug line about the cap:\n%s", log.String())
	}
}

// TestLookupBasePositions_memoFollowsTheHardLink_2496s4r2 (F10): a base whose
// build was skipped is carried forward by hard link into the next snapshot;
// the identity follows the link, so the lookup over the carried path builds
// nothing either.
func TestLookupBasePositions_memoFollowsTheHardLink_2496s4r2(t *testing.T) {
	freshMemo(t)
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	builds := forceBuildOutcome(t, keyidx.Result{Skipped: "free space short beside the base", Warn: true})
	if _, st, err := indexLookup(t, context.Background(), base, pkCols, touched("1")); err != nil || st.Path != "join" || *builds != 1 {
		t.Fatalf("first lookup: st=%+v builds=%d err=%v", st, *builds, err)
	}
	dir := t.TempDir()
	linked, err := carryForward(context.Background(), base, dir, "s", "t")
	if err != nil || !linked {
		t.Fatalf("linked=%v err=%v", linked, err)
	}
	dst := filepath.Join(dir, "s", "t.parquet")
	log := captureDebugLog(t)
	got, st, err := indexLookup(t, context.Background(), dst, pkCols, touched("1", "3"))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "join" || *builds != 1 {
		t.Fatalf("over the carried path: got=%v st=%+v builds=%d err=%v\n%s", got, st, *builds, err, log.String())
	}
	if debugs := keyIndexLines(log.String(), "DEBUG"); len(debugs) != 1 || !strings.Contains(debugs[0], "not tried again") {
		t.Fatalf("want one Debug line about the memo:\n%s", log.String())
	}
}
