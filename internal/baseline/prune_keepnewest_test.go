package baseline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Keep-newest-N retention for a snapshot root with no external destination
// (#1681). Every test here runs against real files in t.TempDir(): pruning
// deletes an operator's recovery copies, so the decision table alone is not
// the proof — the directories left on disk are.

// kn is the "now" every keep-newest test prunes at. Snapshots are days old
// unless a test says otherwise, so the one-hour floor never decides a case by
// accident.
var kn = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

// snapName is the on-disk directory name of a snapshot taken d before kn.
func snapName(d time.Duration) string {
	return strings.ReplaceAll(kn.Add(-d).Format(time.RFC3339), ":", "-")
}

func days(n int) time.Duration { return time.Duration(n) * 24 * time.Hour }

func keepNewest(t *testing.T, root string, n int) PruneResult {
	t.Helper()
	res, err := PruneLocal(context.Background(), PruneOptions{LocalDir: root, KeepNewest: n, Now: kn})
	if err != nil {
		t.Fatalf("PruneLocal(keep newest %d): %v", n, err)
	}
	return res
}

// onDisk lists the snapshot directories still present under root, sorted.
func onDisk(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if _, ok := parseBaselineDirTimestamp(e.Name()); ok && e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

func sorted(names ...string) []string {
	out := slices.Clone(names)
	sort.Strings(out)
	return out
}

func wantOnDisk(t *testing.T, root string, want ...string) {
	t.Helper()
	if got := onDisk(t, root); !slices.Equal(got, sorted(want...)) {
		t.Fatalf("snapshots on disk = %v, want %v", got, sorted(want...))
	}
}

func TestKeepNewest_exactlyNKeepsAll(t *testing.T) {
	root := t.TempDir()
	a, b, c := snapName(days(30)), snapName(days(20)), snapName(days(10))
	for _, s := range []string{a, b, c} {
		makeSnapshot(t, root, s, true, "shop/orders")
	}
	res := keepNewest(t, root, 3)
	if len(res.Pruned) != 0 {
		t.Fatalf("pruned %v with exactly N snapshots, want none", res.Pruned)
	}
	wantOnDisk(t, root, a, b, c)
}

func TestKeepNewest_nPlusOnePrunesTheOldest(t *testing.T) {
	root := t.TempDir()
	a, b, c, d := snapName(days(40)), snapName(days(30)), snapName(days(20)), snapName(days(10))
	for _, s := range []string{a, b, c, d} {
		makeSnapshot(t, root, s, true, "shop/orders")
	}
	res := keepNewest(t, root, 3)
	if !slices.Equal(res.Pruned, []string{a}) {
		t.Fatalf("pruned %v, want [%s]", res.Pruned, a)
	}
	if res.KeptNewest != 2 || res.KeptKeeper != 1 {
		t.Errorf("kept newest=%d keeper=%d, want 2 and 1 (the newest is the per-table keeper)", res.KeptNewest, res.KeptKeeper)
	}
	wantOnDisk(t, root, b, c, d)
}

func TestKeepNewest_fewerThanNKeepsAll(t *testing.T) {
	root := t.TempDir()
	a, b := snapName(days(300)), snapName(days(200))
	makeSnapshot(t, root, a, true, "shop/orders")
	makeSnapshot(t, root, b, true, "shop/orders")
	if res := keepNewest(t, root, 3); len(res.Pruned) != 0 {
		t.Fatalf("pruned %v with fewer than N snapshots, want none", res.Pruned)
	}
	wantOnDisk(t, root, a, b)
}

// An _INCOMPLETE snapshot neither counts toward N nor is ever touched, however
// old it is: it may be a run in progress or one resumable with --retry.
func TestKeepNewest_incompleteNeitherCountsNorIsTouched(t *testing.T) {
	root := t.TempDir()
	oldest, old, inc, newest := snapName(days(40)), snapName(days(30)), snapName(days(20)), snapName(days(10))
	makeSnapshot(t, root, oldest, true, "shop/orders")
	makeSnapshot(t, root, old, true, "shop/orders")
	makeSnapshot(t, root, inc, false, "shop/orders")
	makeSnapshot(t, root, newest, true, "shop/orders")
	// And an ancient incomplete one: never ours to delete.
	ancient := snapName(days(900))
	makeSnapshot(t, root, ancient, false, "shop/orders")

	res := keepNewest(t, root, 2)
	if !slices.Equal(res.Pruned, []string{oldest}) {
		t.Fatalf("pruned %v, want [%s]: the incomplete one must not take a slot", res.Pruned, oldest)
	}
	if res.KeptIncomplete != 2 {
		t.Errorf("KeptIncomplete = %d, want 2", res.KeptIncomplete)
	}
	wantOnDisk(t, root, ancient, old, inc, newest)
	if SnapshotComplete(filepath.Join(root, inc)) {
		t.Errorf("the incomplete snapshot lost its marker")
	}
}

// A table the newer snapshots no longer hold keeps its newest older snapshot,
// even though that snapshot is outside the newest N.
func TestKeepNewest_newestPerTableSurvivesOutsideN(t *testing.T) {
	root := t.TempDir()
	s1, s2, s3, s4 := snapName(days(40)), snapName(days(30)), snapName(days(20)), snapName(days(10))
	makeSnapshot(t, root, s1, true, "shop/orders", "shop/legacy") // only copy of legacy
	makeSnapshot(t, root, s2, true, "shop/orders")
	makeSnapshot(t, root, s3, true, "shop/orders")
	makeSnapshot(t, root, s4, true, "shop/orders")

	before := tablesResolvable(t, root, kn)
	res := keepNewest(t, root, 2)
	if !slices.Equal(res.Pruned, []string{s2}) {
		t.Fatalf("pruned %v, want [%s] (s1 is the only copy of shop/legacy)", res.Pruned, s2)
	}
	wantOnDisk(t, root, s1, s3, s4)
	after := tablesResolvable(t, root, kn)
	for tbl := range before {
		if !after[tbl] {
			t.Errorf("table %s lost its last snapshot", tbl)
		}
	}
}

// linkForward makes <root>/<to>/<schema>/<table>.parquet a HARD LINK to the
// same file in <from>, which is what carry-forward publishes for a table that
// did not change.
func linkForward(t *testing.T, root, from, to, tbl string) {
	t.Helper()
	schema, table, _ := strings.Cut(tbl, "/")
	dst := filepath.Join(root, to, schema, table+".parquet")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(dst)
	if err := os.Link(filepath.Join(root, from, schema, table+".parquet"), dst); err != nil {
		t.Fatal(err)
	}
}

// Carried-forward files are hard links shared between snapshots. Deleting an
// old snapshot must leave the newer one's file readable, byte for byte, and
// the bytes still linked from a kept snapshot are not reclaimed disk.
func TestKeepNewest_hardLinkedFilesSurviveInTheKeptSnapshot(t *testing.T) {
	root := t.TempDir()
	s1, s2, s3 := snapName(days(30)), snapName(days(20)), snapName(days(10))
	makeSnapshot(t, root, s1, true, "shop/orders", "shop/users")
	payload := []byte("the only bytes of users, written once")
	if err := os.WriteFile(filepath.Join(root, s1, "shop", "users.parquet"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	makeSnapshot(t, root, s2, true, "shop/orders")
	makeSnapshot(t, root, s3, true, "shop/orders")
	linkForward(t, root, s1, s2, "shop/users")
	linkForward(t, root, s1, s3, "shop/users")

	res := keepNewest(t, root, 1)
	if !slices.Equal(sorted(res.Pruned...), sorted(s1, s2)) {
		t.Fatalf("pruned %v, want %s and %s", res.Pruned, s1, s2)
	}
	got, err := os.ReadFile(filepath.Join(root, s3, "shop", "users.parquet"))
	if err != nil {
		t.Fatalf("the kept snapshot's carried file is gone: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("the kept snapshot's carried file changed: %q", got)
	}
	// Two copies of orders.parquet ("parquet-bytes", 13 bytes each) were
	// really freed, plus the two _SUCCESS markers. users.parquet was not:
	// it is still linked from s3.
	ordersFreed := int64(2 * len("parquet-bytes"))
	if res.ReclaimedBytes >= ordersFreed+int64(len(payload)) {
		t.Errorf("ReclaimedBytes = %d counts the users file that s3 still links (%d bytes)", res.ReclaimedBytes, len(payload))
	}
	if res.ReclaimedBytes < ordersFreed {
		t.Errorf("ReclaimedBytes = %d, want at least the %d bytes of orders that were really freed", res.ReclaimedBytes, ordersFreed)
	}
}

// A rename-aside that fails keeps the snapshot where it was and does not count
// it: nothing disappeared from the list.
func TestKeepNewest_renameFailureKeepsAndDoesNotCount(t *testing.T) {
	root := t.TempDir()
	a, b := snapName(days(20)), snapName(days(10))
	makeSnapshot(t, root, a, true, "shop/orders")
	makeSnapshot(t, root, b, true, "shop/orders")
	orig := renameAside
	t.Cleanup(func() { renameAside = orig })
	renameAside = func(string, string) error { return os.ErrPermission }

	res := keepNewest(t, root, 1)
	if len(res.Pruned) != 0 {
		t.Fatalf("pruned %v although the rename failed", res.Pruned)
	}
	wantOnDisk(t, root, a, b)
	if _, ok, _ := ReadLastPrune(root); ok {
		t.Errorf("a prune that removed nothing recorded a last prune")
	}
}

// A rename that succeeds followed by a delete that fails half way: the
// snapshot has already left the list (its directory no longer parses as a
// snapshot), so it MUST be counted as removed. Leaving it out would make a
// copy vanish from the page with a count that does not include it.
func TestKeepNewest_deleteFailingAfterRenameStillCounts(t *testing.T) {
	root := t.TempDir()
	a, b := snapName(days(20)), snapName(days(10))
	makeSnapshot(t, root, a, true, "shop/orders")
	makeSnapshot(t, root, b, true, "shop/orders")
	orig := removeAll
	t.Cleanup(func() { removeAll = orig })
	removeAll = func(p string) error {
		// Delete one file, then fail: a half-removed tree.
		_ = os.Remove(filepath.Join(p, "shop", "orders.parquet"))
		return os.ErrPermission
	}

	res := keepNewest(t, root, 1)
	if !slices.Equal(res.Pruned, []string{a}) {
		t.Fatalf("pruned %v, want [%s]: it left the list, so it counts", res.Pruned, a)
	}
	wantOnDisk(t, root, b)
	lp, ok, err := ReadLastPrune(root)
	if err != nil || !ok || lp.Removed != 1 {
		t.Fatalf("last prune = %+v ok=%v err=%v, want removed 1", lp, ok, err)
	}
	// The staged leftover is swept by the next prune once deletion works.
	staged := filepath.Join(root, "."+a+pruningSuffix)
	if _, err := os.Stat(staged); err != nil {
		t.Fatalf("test premise: no staged leftover: %v", err)
	}
	removeAll = orig
	keepNewest(t, root, 1)
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Errorf("the staged leftover survived the next prune: %v", err)
	}
}

// Two prunes on one directory: one holds the directory's prune lock, the other
// steps aside and deletes nothing.
func TestKeepNewest_secondPruneStepsAsideWhileTheFirstHoldsTheLock(t *testing.T) {
	root := t.TempDir()
	for i := 5; i >= 1; i-- {
		makeSnapshot(t, root, snapName(days(10*i)), true, "shop/orders")
	}
	unlock, busy, err := lockPrune(root)
	if err != nil || busy {
		t.Fatalf("lockPrune: busy=%v err=%v", busy, err)
	}
	res := keepNewest(t, root, 2)
	unlock()
	if !res.Busy || len(res.Pruned) != 0 {
		t.Fatalf("a prune ran while another held the lock: busy=%v pruned=%v", res.Busy, res.Pruned)
	}
	if got := len(onDisk(t, root)); got != 5 {
		t.Fatalf("%d snapshots on disk, want all 5", got)
	}
}

// Two prunes racing for real: whatever the interleaving, the directory ends
// with exactly the newest N, and the removals add up to the rest.
func TestKeepNewest_racingPrunesNeverLeaveFewerThanN(t *testing.T) {
	for round := 0; round < 20; round++ {
		root := t.TempDir()
		var all []string
		for i := 8; i >= 1; i-- {
			n := snapName(days(10 * i))
			all = append(all, n)
			makeSnapshot(t, root, n, true, "shop/orders")
		}
		var wg sync.WaitGroup
		results := make([]PruneResult, 2)
		errs := make([]error, 2)
		for g := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[g], errs[g] = PruneLocal(context.Background(), PruneOptions{LocalDir: root, KeepNewest: 3, Now: kn})
			}()
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		wantOnDisk(t, root, all[5:]...)
		if got := len(results[0].Pruned) + len(results[1].Pruned); got != 5 {
			t.Fatalf("round %d: removals add up to %d, want 5", round, got)
		}
	}
}

// Names decide, never mtimes: reversing the modification times must not
// change which snapshots are the newest.
func TestKeepNewest_namesDecideNotModificationTimes(t *testing.T) {
	root := t.TempDir()
	a, b, c := snapName(days(30)), snapName(days(20)), snapName(days(10))
	for i, s := range []string{a, b, c} {
		makeSnapshot(t, root, s, true, "shop/orders")
		// a gets the NEWEST mtime, c the oldest.
		mt := kn.Add(-time.Duration(i) * days(100))
		if err := os.Chtimes(filepath.Join(root, s), mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	res := keepNewest(t, root, 2)
	if !slices.Equal(res.Pruned, []string{a}) {
		t.Fatalf("pruned %v, want [%s] by name", res.Pruned, a)
	}
}

// A snapshot dated in the future (clock skew on the host that wrote it) is
// kept, and does NOT take one of the N slots from the real newest ones.
func TestKeepNewest_futureSnapshotDoesNotTakeASlot(t *testing.T) {
	root := t.TempDir()
	a, b, c := snapName(days(30)), snapName(days(20)), snapName(days(10))
	future := snapName(-days(5))
	for _, s := range []string{a, b, c, future} {
		makeSnapshot(t, root, s, true, "shop/orders")
	}
	res := keepNewest(t, root, 2)
	if !slices.Equal(res.Pruned, []string{a}) {
		t.Fatalf("pruned %v, want [%s]", res.Pruned, a)
	}
	wantOnDisk(t, root, b, c, future)
}

// A complete snapshot with no table in it is not a copy of anything, so it
// does not take a slot either (the page would list N-1 snapshots otherwise).
func TestKeepNewest_emptySnapshotDoesNotTakeASlot(t *testing.T) {
	root := t.TempDir()
	a, b, empty := snapName(days(30)), snapName(days(20)), snapName(days(10))
	makeSnapshot(t, root, a, true, "shop/orders")
	makeSnapshot(t, root, b, true, "shop/orders")
	makeSnapshot(t, root, empty, true)
	res := keepNewest(t, root, 2)
	if slices.Contains(res.Pruned, a) || slices.Contains(res.Pruned, b) {
		t.Fatalf("pruned %v: an empty snapshot took a slot from a real one", res.Pruned)
	}
}

// A snapshot whose directory cannot be listed is kept and does not take a slot.
func TestKeepNewest_unreadableIsKeptAndDoesNotTakeASlot(t *testing.T) {
	root := t.TempDir()
	a, b, c := snapName(days(30)), snapName(days(20)), snapName(days(10))
	for _, s := range []string{a, b, c} {
		makeSnapshot(t, root, s, true, "shop/orders")
	}
	orig := readDir
	t.Cleanup(func() { readDir = orig })
	readDir = func(p string) ([]os.DirEntry, error) {
		if p == filepath.Join(root, c) {
			return nil, os.ErrPermission
		}
		return orig(p)
	}
	res := keepNewest(t, root, 2)
	if len(res.Pruned) != 0 {
		t.Fatalf("pruned %v: with the newest unreadable, a and b are the two readable newest", res.Pruned)
	}
	if res.KeptUnreadable != 1 {
		t.Errorf("KeptUnreadable = %d, want 1", res.KeptUnreadable)
	}
}

// The one-hour floor still holds: two snapshots minutes old are both kept at N=1.
func TestKeepNewest_minAgeFloorHolds(t *testing.T) {
	root := t.TempDir()
	a, b := snapName(20*time.Minute), snapName(10*time.Minute)
	makeSnapshot(t, root, a, true, "shop/orders")
	makeSnapshot(t, root, b, true, "shop/orders")
	if res := keepNewest(t, root, 1); len(res.Pruned) != 0 {
		t.Fatalf("pruned %v younger than an hour", res.Pruned)
	}
}

// A configured age retention still protects younger snapshots in this mode:
// the two rules only ever add up to keeping more.
func TestKeepNewest_retainProtectsYoungerSnapshots(t *testing.T) {
	root := t.TempDir()
	a, b, c := snapName(days(30)), snapName(days(5)), snapName(days(1))
	for _, s := range []string{a, b, c} {
		makeSnapshot(t, root, s, true, "shop/orders")
	}
	res, err := PruneLocal(context.Background(), PruneOptions{LocalDir: root, KeepNewest: 1, Retain: days(7), Now: kn})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Pruned, []string{a}) {
		t.Fatalf("pruned %v, want [%s] (b is inside the 7-day window)", res.Pruned, a)
	}
}

// With an external destination set, keep-newest does not apply: that mode
// deletes only what the destination confirmed, by age.
func TestKeepNewest_ignoredWhenThereIsAnExternalDestination(t *testing.T) {
	root := t.TempDir()
	for i := 5; i >= 1; i-- {
		makeSnapshot(t, root, snapName(days(10*i)), true, "shop/orders")
	}
	probe := func(context.Context, string) (bool, error) { return false, nil } // nothing durable
	res, err := pruneWithProbe(context.Background(), PruneOptions{
		LocalDir: root, S3URL: "s3://b/p", Retain: days(1), KeepNewest: 1, Now: kn,
	}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pruned) != 0 {
		t.Fatalf("pruned %v copies the destination never confirmed", res.Pruned)
	}
}

func TestKeepNewest_rejectsANegativeCount(t *testing.T) {
	if _, err := PruneLocal(context.Background(), PruneOptions{LocalDir: t.TempDir(), KeepNewest: -1}); err == nil {
		t.Fatal("a negative keep-newest must be refused")
	}
}

// Keep-newest needs no age: Retain may be zero in this mode.
func TestKeepNewest_noRetainNeeded(t *testing.T) {
	root := t.TempDir()
	makeSnapshot(t, root, snapName(days(20)), true, "shop/orders")
	makeSnapshot(t, root, snapName(days(10)), true, "shop/orders")
	if res := keepNewest(t, root, 1); len(res.Pruned) != 1 {
		t.Fatalf("pruned %v, want one", res.Pruned)
	}
}

// The floor, as a property: across every mix of counts, the newest
// min(N, readable) snapshots survive, and every table stays resolvable.
func TestKeepNewest_neverFewerThanN(t *testing.T) {
	for total := 0; total <= 6; total++ {
		for n := 1; n <= 4; n++ {
			t.Run(fmt.Sprintf("total%d_keep%d", total, n), func(t *testing.T) {
				root := t.TempDir()
				var names []string
				for i := total; i >= 1; i-- {
					name := snapName(days(10 * i))
					names = append(names, name)
					tables := []string{"shop/orders"}
					if i%2 == 0 {
						tables = append(tables, "shop/users")
					}
					makeSnapshot(t, root, name, true, tables...)
				}
				before := tablesResolvable(t, root, kn)
				keepNewest(t, root, n)
				left := onDisk(t, root)
				want := min(n, total)
				if len(left) < want {
					t.Fatalf("%d snapshots left, want at least %d", len(left), want)
				}
				for _, name := range names[len(names)-want:] {
					if !slices.Contains(left, name) {
						t.Errorf("one of the newest %d (%s) was removed", n, name)
					}
				}
				after := tablesResolvable(t, root, kn)
				for tbl := range before {
					if !after[tbl] {
						t.Errorf("table %s lost its last snapshot", tbl)
					}
				}
			})
		}
	}
}

// ─── the last-prune record ───────────────────────────────────────────────────

func TestLastPrune_recordedOnlyWhenSomethingWasRemoved(t *testing.T) {
	root := t.TempDir()
	makeSnapshot(t, root, snapName(days(20)), true, "shop/orders")
	makeSnapshot(t, root, snapName(days(10)), true, "shop/orders")

	keepNewest(t, root, 3)
	if _, ok, err := ReadLastPrune(root); ok || err != nil {
		t.Fatalf("a prune that removed nothing left a record (ok=%v err=%v)", ok, err)
	}
	if _, err := PruneLocal(context.Background(), PruneOptions{LocalDir: root, KeepNewest: 1, Now: kn, DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := ReadLastPrune(root); ok {
		t.Fatal("a dry run left a record")
	}
	keepNewest(t, root, 1)
	lp, ok, err := ReadLastPrune(root)
	if err != nil || !ok {
		t.Fatalf("no record after a real prune: ok=%v err=%v", ok, err)
	}
	if lp.Removed != 1 || !lp.At.Equal(kn) {
		t.Fatalf("record = %+v, want removed 1 at %s", lp, kn)
	}
	info, err := os.Stat(filepath.Join(root, LastPruneFile))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("record mode = %o, want 0600", info.Mode().Perm())
	}
	// A later prune that removes nothing does not erase the record: the page
	// still has to say why the copies are gone.
	keepNewest(t, root, 1)
	if _, ok, _ := ReadLastPrune(root); !ok {
		t.Fatal("a later empty prune erased the record")
	}
}

// The record lives beside the snapshots and must be invisible to everything
// that walks them: not a snapshot, not uploaded.
func TestLastPrune_isNotASnapshotAndIsNotUploaded(t *testing.T) {
	root := t.TempDir()
	makeSnapshot(t, root, snapName(days(20)), true, "shop/orders")
	keep := snapName(days(10))
	makeSnapshot(t, root, keep, true, "shop/orders")
	keepNewest(t, root, 1)
	if _, err := os.Stat(filepath.Join(root, LastPruneFile)); err != nil {
		t.Fatalf("test premise: no record: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, pruneLockName)); err != nil {
		t.Fatalf("test premise: no lock file: %v", err)
	}
	snaps, err := enumerateLocalSnapshots(root)
	if err != nil || len(snaps) != 1 {
		t.Fatalf("enumerate = %v, %v; want the one kept snapshot", snaps, err)
	}
	var keys []string
	ops := s3UploadOps{
		putEmpty:     func(context.Context, string) error { return nil },
		uploadFile:   func(_ context.Context, _, k string) error { keys = append(keys, k); return nil },
		objectExists: func(context.Context, string) (bool, error) { return false, nil },
		deleteObject: func(context.Context, string) error { return nil },
	}
	if _, err := uploadWithOps(context.Background(), root, "p", false, ops); err != nil {
		t.Fatalf("upload: %v", err)
	}
	for _, k := range keys {
		if strings.Contains(k, LastPruneFile) || strings.Contains(k, pruneLockName) {
			t.Fatalf("uploaded %q, a local bookkeeping file", k)
		}
	}
	if len(keys) == 0 {
		t.Fatal("test premise: nothing was uploaded at all")
	}
}

func TestLastPrune_unreadableRecordIsAnError(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, LastPruneFile), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadLastPrune(root); err == nil {
		t.Fatal("a corrupt record must be reported, not read as never pruned")
	}
	if _, ok, err := ReadLastPrune(filepath.Join(root, "missing")); ok || err != nil {
		t.Fatalf("a missing directory is never pruned: ok=%v err=%v", ok, err)
	}
}

// Per-server directories under a shared root: <root>/<server id>/<ts>/... The
// server id is hex, which never parses as a snapshot name, so a prune of the
// shared root never sees, counts or deletes the server's snapshots.
func TestKeepNewest_serverDirectoryInsideASharedRootIsInvisible(t *testing.T) {
	root := t.TempDir()
	const serverID = "a1b2c3d4e5f60718"
	srvRoot := filepath.Join(root, serverID)
	var inServer []string
	for i := 4; i >= 1; i-- {
		n := snapName(days(10 * i))
		inServer = append(inServer, n)
		makeSnapshot(t, srvRoot, n, true, "shop/orders")
	}
	makeSnapshot(t, root, snapName(days(50)), true, "shop/orders")
	makeSnapshot(t, root, snapName(days(5)), true, "shop/orders")
	keepNewest(t, root, 1)
	wantOnDisk(t, srvRoot, inServer...)
	probe := func(context.Context, string) (bool, error) { return true, nil }
	if _, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", Retain: days(1), Now: kn}, probe); err != nil {
		t.Fatal(err)
	}
	wantOnDisk(t, srvRoot, inServer...)
	// And the shared root's listing does not include the server's copies.
	got, err := DiscoverBaselines(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range got {
		if strings.Contains(b.Path, serverID) {
			t.Fatalf("the shared root listed a server's snapshot: %s", b.Path)
		}
	}
}

// Partial snapshots (a run over some tables) must not use up the places of
// the tables they leave out: N is counted per table, so an older full
// snapshot survives while any of its tables has fewer than N newer copies.
func TestKeepNewest_partialSnapshotsDoNotCrowdOutOtherTables(t *testing.T) {
	root := t.TempDir()
	full1, full2 := snapName(days(50)), snapName(days(40))
	p1, p2, p3 := snapName(days(30)), snapName(days(20)), snapName(days(10))
	makeSnapshot(t, root, full1, true, "shop/orders", "shop/users")
	makeSnapshot(t, root, full2, true, "shop/orders", "shop/users")
	for _, p := range []string{p1, p2, p3} {
		makeSnapshot(t, root, p, true, "shop/orders")
	}
	res := keepNewest(t, root, 2)
	// users has copies only in full1 and full2: both stay. orders' newest two
	// are p2 and p3, so p1 goes.
	if !slices.Equal(res.Pruned, []string{p1}) {
		t.Fatalf("pruned %v, want [%s]", res.Pruned, p1)
	}
	wantOnDisk(t, root, full1, full2, p2, p3)
}

// A prune whose record cannot be written removes the previous record: left in
// place it would show an older date and count as the latest prune.
func TestLastPrune_aFailedWriteDoesNotLeaveAStaleRecord(t *testing.T) {
	root := t.TempDir()
	for i := 4; i >= 1; i-- {
		makeSnapshot(t, root, snapName(days(10*i)), true, "shop/orders")
	}
	keepNewest(t, root, 3) // removes one, records it
	if _, ok, _ := ReadLastPrune(root); !ok {
		t.Fatal("test premise: no first record")
	}
	makeSnapshot(t, root, snapName(days(5)), true, "shop/orders")
	orig := writeRecord
	t.Cleanup(func() { writeRecord = orig })
	writeRecord = func(string, LastPrune) error { return os.ErrPermission }
	res := keepNewest(t, root, 1)
	if len(res.Pruned) == 0 {
		t.Fatal("test premise: the second prune removed nothing")
	}
	if _, ok, err := ReadLastPrune(root); ok || err != nil {
		t.Fatalf("a stale record survived a failed write: ok=%v err=%v", ok, err)
	}
}

// ─── the failed-attempt record ───────────────────────────────────────────────

// A prune that could not remove what it planned records when and why, beside
// the snapshots, so a folder that stops shrinking is not visible only in the
// log. The next attempt that succeeds clears it.
func TestPruneFailure_recordedAndClearedBySuccess(t *testing.T) {
	root := t.TempDir()
	for i := 4; i >= 1; i-- {
		makeSnapshot(t, root, snapName(days(10*i)), true, "shop/orders")
	}
	orig := renameAside
	t.Cleanup(func() { renameAside = orig })
	renameAside = func(string, string) error { return os.ErrPermission }
	keepNewest(t, root, 1)
	f, ok, err := ReadLastPruneFailure(root)
	if err != nil || !ok {
		t.Fatalf("no failure recorded: ok=%v err=%v", ok, err)
	}
	if !f.At.Equal(kn) || !strings.Contains(f.Reason, "3 snapshots could not be removed") {
		t.Errorf("failure = %+v", f)
	}
	info, err := os.Stat(filepath.Join(root, LastPruneFailureFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("failure record mode: %v %v", info, err)
	}
	// A dry run neither records nor clears.
	renameAside = orig
	if _, err := PruneLocal(context.Background(), PruneOptions{LocalDir: root, KeepNewest: 1, Now: kn, DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := ReadLastPruneFailure(root); !ok {
		t.Fatal("a dry run cleared the failure")
	}
	keepNewest(t, root, 1)
	if _, ok, err := ReadLastPruneFailure(root); ok || err != nil {
		t.Fatalf("a successful prune left the failure: ok=%v err=%v", ok, err)
	}
	// And the record is never uploaded or taken for a snapshot.
	if !isPruneArtifact(root, filepath.Join(root, LastPruneFailureFile)) {
		t.Error("the failure record is not excluded from the upload")
	}
}

// A prune that fails outright (the folder cannot be listed) is recorded too.
func TestPruneFailure_anErrorIsRecorded(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root lists through permissions")
	}
	root := t.TempDir()
	makeSnapshot(t, root, snapName(days(10)), true, "shop/orders")
	if err := os.Chmod(root, 0o300); err != nil { // writable, not listable
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o700) })
	if _, err := PruneLocal(context.Background(), PruneOptions{LocalDir: root, KeepNewest: 1, Now: kn}); err == nil {
		t.Fatal("test premise: the prune did not fail")
	}
	_ = os.Chmod(root, 0o700)
	if f, ok, err := ReadLastPruneFailure(root); err != nil || !ok || f.Reason == "" {
		t.Fatalf("an outright failure is not recorded: %+v ok=%v err=%v", f, ok, err)
	}
}

// Another prune holding the lock is not a failure: nothing is recorded.
func TestPruneFailure_busyIsNotAFailure(t *testing.T) {
	root := t.TempDir()
	makeSnapshot(t, root, snapName(days(10)), true, "shop/orders")
	unlock, _, err := lockPrune(root)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	keepNewest(t, root, 1)
	if _, ok, _ := ReadLastPruneFailure(root); ok {
		t.Fatal("stepping aside for another prune was recorded as a failure")
	}
}

// A snapshot renamed aside whose files then could not be deleted left the
// list, so it counts as removed, AND the attempt is a failure: the disk it
// was supposed to free is still used. The record must survive the next cycle
// while the leftover still cannot be deleted (the sweep fails too), and only
// a cycle that actually deletes it clears the record.
func TestPruneFailure_deleteAfterRenameIsRecordedUntilTheFilesAreGone(t *testing.T) {
	root := t.TempDir()
	a, b := snapName(days(20)), snapName(days(10))
	makeSnapshot(t, root, a, true, "shop/orders")
	makeSnapshot(t, root, b, true, "shop/orders")
	orig := removeAll
	t.Cleanup(func() { removeAll = orig })
	removeAll = func(string) error { return os.ErrPermission }

	res := keepNewest(t, root, 1)
	if !slices.Equal(res.Pruned, []string{a}) {
		t.Fatalf("pruned %v, want [%s]", res.Pruned, a)
	}
	f, ok, err := ReadLastPruneFailure(root)
	if err != nil || !ok {
		t.Fatalf("cycle 1: a delete that failed after the rename is not recorded: ok=%v err=%v", ok, err)
	}
	for _, want := range []string{"1 snapshot", "could not all be deleted", "permission denied", root} {
		if !strings.Contains(f.Reason, want) {
			t.Errorf("cycle 1 reason %q does not say %q", f.Reason, want)
		}
	}

	// Cycle 2: nothing new to remove, the leftover still cannot be deleted.
	keepNewest(t, root, 1)
	f, ok, err = ReadLastPruneFailure(root)
	if err != nil || !ok {
		t.Fatalf("cycle 2: the leftover still fills the disk and the failure record was cleared: ok=%v err=%v", ok, err)
	}
	if !strings.Contains(f.Reason, "could not be deleted") || !strings.Contains(f.Reason, "permission denied") {
		t.Errorf("cycle 2 reason %q does not say the leftover files could not be deleted", f.Reason)
	}

	// Cycle 3: deletion works, the leftover goes, and so does the record.
	removeAll = orig
	keepNewest(t, root, 1)
	if _, ok, err := ReadLastPruneFailure(root); ok || err != nil {
		t.Fatalf("cycle 3: the files are gone but the failure record stayed: ok=%v err=%v", ok, err)
	}
}

// A snapshot whose directory cannot be listed is kept (never removed), and the
// attempt says so with the path, so an operator can fix the permission instead
// of reading only a retention line.
func TestPruneFailure_unreadableSnapshotIsRecordedWithItsPath(t *testing.T) {
	root := t.TempDir()
	a, b, c := snapName(days(30)), snapName(days(20)), snapName(days(10))
	for _, s := range []string{a, b, c} {
		makeSnapshot(t, root, s, true, "shop/orders")
	}
	orig := readDir
	t.Cleanup(func() { readDir = orig })
	bad := filepath.Join(root, b)
	readDir = func(p string) ([]os.DirEntry, error) {
		if p == bad {
			return nil, os.ErrPermission
		}
		return orig(p)
	}
	res := keepNewest(t, root, 1)
	if res.KeptUnreadable != 1 {
		t.Fatalf("KeptUnreadable = %d, want 1", res.KeptUnreadable)
	}
	wantOnDisk(t, root, b, c)
	f, ok, err := ReadLastPruneFailure(root)
	if err != nil || !ok {
		t.Fatalf("an unreadable snapshot is not recorded: ok=%v err=%v", ok, err)
	}
	for _, want := range []string{"1 snapshot", "could not be read", bad, "permission denied"} {
		if !strings.Contains(f.Reason, want) {
			t.Errorf("reason %q does not say %q", f.Reason, want)
		}
	}
	// Readable again: the next attempt clears it.
	readDir = orig
	keepNewest(t, root, 1)
	if _, ok, err := ReadLastPruneFailure(root); ok || err != nil {
		t.Fatalf("readable again, but the failure record stayed: ok=%v err=%v", ok, err)
	}
}
