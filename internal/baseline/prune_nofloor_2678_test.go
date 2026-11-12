package baseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// The folder holds the newest N, for real (#2678): no hour in the count
// modes, the hour kept in the age mode, the prune standing still under a
// reader, and a restore's result kept until the next restore. Real files
// throughout: what is left on disk is the proof.

// minutes makes one full snapshot per age given, in minutes before kn,
// oldest first, and returns the names in that order.
func minutes(t *testing.T, root string, ages ...int) []string {
	t.Helper()
	var names []string
	for _, m := range ages {
		n := snapName(time.Duration(m) * time.Minute)
		makeSnapshot(t, root, n, true, "shop/orders")
		names = append(names, n)
	}
	return names
}

// A 5-minute schedule with a count of 3: three snapshots, not thirteen.
func TestNoFloor_fiveMinuteScheduleHoldsExactlyTheCount(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 61, 56, 51, 46, 41, 36, 31, 26, 21, 16, 11, 6, 1)
	res := keepNewest(t, root, 3)
	if len(res.Pruned) != 10 || res.KeptRecent != 0 {
		t.Fatalf("pruned %d, kept as recent %d: want 10 and 0", len(res.Pruned), res.KeptRecent)
	}
	wantOnDisk(t, root, s[10:]...)
}

// One minute old and past the count: goes. Inside the count: stays.
func TestNoFloor_oneMinuteOldPastTheCountGoes(t *testing.T) {
	root := t.TempDir()
	// The older one is dated one minute ago; the newest is seconds old.
	old := snapName(time.Minute)
	newest := snapName(5 * time.Second)
	makeSnapshot(t, root, old, true, "shop/orders")
	makeSnapshot(t, root, newest, true, "shop/orders")
	if res := keepNewest(t, root, 2); len(res.Pruned) != 0 {
		t.Fatalf("count 2 pruned %v: both are inside the count", res.Pruned)
	}
	res := keepNewest(t, root, 1)
	if !slices.Equal(res.Pruned, []string{old}) {
		t.Fatalf("count 1 pruned %v, want [%s]", res.Pruned, old)
	}
	wantOnDisk(t, root, newest)
}

// What the count never removes, with every snapshot minutes old: an
// _INCOMPLETE one, the `current` target, an unreadable one, the only copy of
// a table, and a folder a writer has just created.
func TestNoFloor_theOtherGuaranteesHold(t *testing.T) {
	t.Run("incomplete", func(t *testing.T) {
		root := t.TempDir()
		s := minutes(t, root, 20, 10, 5)
		part := snapName(30 * time.Minute)
		makeSnapshot(t, root, part, false, "shop/orders")
		res := keepNewest(t, root, 1)
		if res.KeptIncomplete != 1 {
			t.Fatalf("KeptIncomplete = %d, want 1", res.KeptIncomplete)
		}
		wantOnDisk(t, root, part, s[2])
	})
	t.Run("the current target", func(t *testing.T) {
		root := t.TempDir()
		s := minutes(t, root, 20, 10, 5)
		link := filepath.Join(root, CurrentLinkName)
		if err := os.Remove(link); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err := os.Symlink(s[0], link); err != nil {
			t.Fatal(err)
		}
		keepNewest(t, root, 1)
		wantOnDisk(t, root, s[0], s[2])
	})
	t.Run("unreadable", func(t *testing.T) {
		root := t.TempDir()
		s := minutes(t, root, 20, 10, 5)
		orig := readDir
		t.Cleanup(func() { readDir = orig })
		readDir = func(p string) ([]os.DirEntry, error) {
			if p == filepath.Join(root, s[0]) {
				return nil, os.ErrPermission
			}
			return orig(p)
		}
		res := keepNewest(t, root, 1)
		if res.KeptUnreadable != 1 {
			t.Fatalf("KeptUnreadable = %d, want 1", res.KeptUnreadable)
		}
		wantOnDisk(t, root, s[0], s[2])
	})
	t.Run("the only copy of a table", func(t *testing.T) {
		root := t.TempDir()
		only := snapName(30 * time.Minute)
		makeSnapshot(t, root, only, true, "shop/orders", "shop/audit")
		s := minutes(t, root, 20, 10, 5)
		keepNewest(t, root, 1)
		wantOnDisk(t, root, only, s[2])
	})
	t.Run("a folder a writer has just created", func(t *testing.T) {
		root := t.TempDir()
		s := minutes(t, root, 20, 10, 5)
		// What every producer leaves for an instant: the folder, and not
		// yet its _INCOMPLETE. Named for a past moment, as a restore's is.
		fresh := snapName(45 * time.Minute)
		if err := os.MkdirAll(filepath.Join(root, fresh), 0o755); err != nil {
			t.Fatal(err)
		}
		res := keepNewest(t, root, 1)
		if slices.Contains(res.Pruned, fresh) || res.KeptIncomplete != 1 {
			t.Fatalf("a folder being created: pruned %v, KeptIncomplete %d", res.Pruned, res.KeptIncomplete)
		}
		wantOnDisk(t, root, fresh, s[2])
	})
}

// The age mode (a destination, no count) keeps its hour: a snapshot the
// bucket has, past a sub-hour age and 30 minutes old, stays; the same two
// hours old goes.
func TestNoFloor_theAgeModeKeepsItsHour(t *testing.T) {
	root := t.TempDir()
	twoHours := snapName(2 * time.Hour)
	young := snapName(30 * time.Minute)
	newest := snapName(time.Minute)
	for _, n := range []string{twoHours, young, newest} {
		makeSnapshot(t, root, n, true, "shop/orders")
	}
	everything := func(context.Context, string) (bool, error) { return true, nil }
	res, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", Retain: time.Minute, Now: kn}, everything)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Pruned, []string{twoHours}) || res.KeptRecent != 1 {
		t.Fatalf("pruned %v, kept as recent %d: want [%s] and 1 (the 30-minute one is under the hour)", res.Pruned, res.KeptRecent, twoHours)
	}
	wantOnDisk(t, root, young, newest)
}

// A count AND an age: the age still protects what is younger than it, in
// both count modes, and what is older than it goes by the count.
func TestNoFloor_anAgeBesideTheCountStillProtects(t *testing.T) {
	for _, mode := range []string{"no destination", "with a destination"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			s := minutes(t, root, 50, 40, 20, 10, 5)
			opts := PruneOptions{LocalDir: root, KeepNewest: 1, Retain: 30 * time.Minute, Now: kn}
			var res PruneResult
			var err error
			if mode == "no destination" {
				res, err = PruneLocal(context.Background(), opts)
			} else {
				opts.S3URL = "s3://b/p"
				res, err = pruneWithProbe(context.Background(), opts, func(context.Context, string) (bool, error) { return true, nil })
			}
			if err != nil {
				t.Fatal(err)
			}
			slices.Sort(res.Pruned)
			if !slices.Equal(res.Pruned, s[:2]) || res.KeptRecent != 2 {
				t.Fatalf("pruned %v, kept as recent %d: want %v and 2 (20 and 10 minutes are inside the 30-minute age)", res.Pruned, res.KeptRecent, s[:2])
			}
			wantOnDisk(t, root, s[2:]...)
		})
	}
}

// With a destination: each candidate is asked about once, the snapshots
// inside the count never, and a bucket that fails to answer removes nothing
// that needed its answer, minutes old or not.
func TestNoFloor_theBucketIsAskedOncePerCandidateAndSilenceRemovesNothing(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 25, 20, 15, 10, 5)
	asked := map[string]int{}
	probe := func(_ context.Context, name string) (bool, error) {
		asked[name]++
		return true, nil
	}
	res, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: 3, Now: kn}, probe)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, s[:2]) {
		t.Fatalf("pruned %v, want %v", res.Pruned, s[:2])
	}
	if len(asked) != 2 || asked[s[0]] != 1 || asked[s[1]] != 1 {
		t.Fatalf("asked %v: want the two past the count, once each", asked)
	}
	// The next prune, after one more snapshot: one candidate, one question.
	clear(asked)
	next := snapName(0)
	makeSnapshot(t, root, next, true, "shop/orders")
	if _, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: 3, Now: kn}, probe); err != nil {
		t.Fatal(err)
	}
	if len(asked) != 1 || asked[s[2]] != 1 {
		t.Fatalf("second prune asked %v: want only %s, once", asked, s[2])
	}
	wantOnDisk(t, root, s[3], s[4], next)

	// A silent bucket.
	root2 := t.TempDir()
	s2 := minutes(t, root2, 25, 20, 15, 10, 5)
	res, err = pruneWithProbe(context.Background(), PruneOptions{LocalDir: root2, S3URL: "s3://b/p", KeepNewest: 3, Now: kn},
		func(context.Context, string) (bool, error) { return false, errors.New("dial tcp: i/o timeout") })
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pruned) != 0 || !res.DestinationSilent {
		t.Fatalf("a silent bucket: pruned %v, silent %v", res.Pruned, res.DestinationSilent)
	}
	wantOnDisk(t, root2, s2...)
	if rec, ok, _ := ReadLastPruneFailure(root2); !ok {
		t.Fatal("a silent bucket left no failure record")
	} else {
		t.Logf("failure record as the page shows it: %s", rec.Reason)
	}
}

// While a job of this process reads older snapshots of the folder, the prune
// removes nothing from it and records nothing; when the job ends, however it
// ends, the next prune does the work. A reader of another folder stops
// nothing here.
func TestNoFloor_thePruneStandsStillUnderAReader(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 25, 20, 15, 10, 5)
	other := t.TempDir()
	endOther := BeginRead(other, "a restore to a moment")
	defer endOther()

	end := BeginRead(root, "a .sql build")
	res := keepNewest(t, root, 1)
	if len(res.Pruned) != 0 || res.Reading != "a .sql build" {
		t.Fatalf("under a reader: pruned %v, Reading %q", res.Pruned, res.Reading)
	}
	wantOnDisk(t, root, s...)
	if _, ok, _ := ReadLastPruneFailure(root); ok {
		t.Fatal("standing still for a reader was recorded as a failed cleanup")
	}
	// The folder spelled through a symlink is the same folder.
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if res := keepNewest(t, link, 1); res.Reading == "" || len(res.Pruned) != 0 {
		t.Fatalf("the same folder through a link: %+v", res)
	}
	// Two readers: the prune waits for both.
	end2 := BeginRead(root, "a check")
	end()
	end() // twice is safe
	if res := keepNewest(t, root, 1); res.Reading != "a check" || len(res.Pruned) != 0 {
		t.Fatalf("one of two readers ended: %+v", res)
	}
	// The second ends by a panic that its defer still releases.
	func() {
		defer func() { _ = recover() }()
		defer end2()
		panic("the job crashed")
	}()
	res = keepNewest(t, root, 1)
	if len(res.Pruned) != 4 || res.Reading != "" {
		t.Fatalf("after the readers ended: pruned %v, Reading %q", res.Pruned, res.Reading)
	}
	wantOnDisk(t, root, s[4])
}

// A reader registered longer ago than any such job runs is taken for stuck
// and stops nothing: a job that never ends does not fill the disk.
func TestNoFloor_aStuckReaderDoesNotStopThePruneForever(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 15, 10, 5)
	end := BeginRead(root, "a download")
	defer end()
	if what, stuck := readingNow(root, time.Now().Add(readHoldLimit-time.Minute)); what == "" || stuck != 0 {
		t.Fatalf("just inside the limit: what %q stuck %d", what, stuck)
	}
	if what, stuck := readingNow(root, time.Now().Add(readHoldLimit+time.Minute)); what != "" || stuck != 1 {
		t.Fatalf("past the limit: what %q stuck %d", what, stuck)
	}
	// Through the prune itself: age the registration, then prune.
	readsMu.Lock()
	for h := range reads[readKey(root)] {
		h.since = time.Now().Add(-readHoldLimit - time.Minute)
	}
	readsMu.Unlock()
	res := keepNewest(t, root, 1)
	if len(res.Pruned) != 2 || res.Reading != "" {
		t.Fatalf("a stuck reader: pruned %v, Reading %q", res.Pruned, res.Reading)
	}
	wantOnDisk(t, root, s[2])
}

// A restore to a moment publishes a snapshot named for that past moment.
// Without the mark the count removes it at once; with it, it stays until the
// next restore, whose result takes its place.
func TestNoFloor_theLastRestoreResultStays(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 15, 10, 5)
	restored := snapName(3 * time.Hour)
	makeSnapshot(t, root, restored, true, "shop/orders")

	// The defect: named in the past, so neither the newest of a table nor
	// among the newest N.
	bare := t.TempDir()
	for _, n := range append(slices.Clone(s), restored) {
		makeSnapshot(t, bare, n, true, "shop/orders")
	}
	if res := keepNewest(t, bare, 3); !slices.Equal(res.Pruned, []string{restored}) {
		t.Fatalf("with no mark the prune removed %v, expected the restore's result %s", res.Pruned, restored)
	}

	if err := MarkRestored(root, restored); err != nil {
		t.Fatal(err)
	}
	if res := keepNewest(t, root, 3); len(res.Pruned) != 0 {
		t.Fatalf("pruned %v: the restore's result is among them or took a place", res.Pruned)
	}
	wantOnDisk(t, root, append(slices.Clone(s), restored)...)
	// It takes no place in the count: with a count of 1 the newest stays too.
	keepNewest(t, root, 1)
	wantOnDisk(t, root, s[2], restored)

	// The next restore replaces it.
	second := snapName(5 * time.Hour)
	makeSnapshot(t, root, second, true, "shop/orders")
	if err := os.Chtimes(filepath.Join(root, restoredPrefix+restored), kn.Add(-time.Hour), kn.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := MarkRestored(root, second); err != nil {
		t.Fatal(err)
	}
	res := keepNewest(t, root, 1)
	if !slices.Equal(res.Pruned, []string{restored}) {
		t.Fatalf("after a second restore pruned %v, want the first one's result [%s]", res.Pruned, restored)
	}
	wantOnDisk(t, root, s[2], second)
	if _, err := os.Stat(filepath.Join(root, restoredPrefix+restored)); !os.IsNotExist(err) {
		t.Fatalf("the first restore's mark outlived its snapshot: %v", err)
	}

	// With a destination that has it: still kept, and not asked about.
	var asked []string
	res2, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: 1, Now: kn}, answers(&asked, second, s[2]))
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Pruned) != 0 || len(asked) != 0 {
		t.Fatalf("with a destination: pruned %v, asked %v", res2.Pruned, asked)
	}

	// A result removed by hand: its mark is swept, nothing else changes.
	if err := os.RemoveAll(filepath.Join(root, second)); err != nil {
		t.Fatal(err)
	}
	keepNewest(t, root, 1)
	if _, err := os.Stat(filepath.Join(root, restoredPrefix+second)); !os.IsNotExist(err) {
		t.Fatalf("the mark of a result removed by hand is still there: %v", err)
	}
	wantOnDisk(t, root, s[2])
}

// The mark is not a snapshot, refuses a name that is not one, and is not
// sent by an upload of the whole root.
func TestNoFloor_theRestoreMarkIsNotSnapshotData(t *testing.T) {
	root := t.TempDir()
	if err := MarkRestored(root, "../outside"); err == nil {
		t.Fatal("marked a name that is not a snapshot directory")
	}
	n := snapName(time.Hour)
	makeSnapshot(t, root, n, true, "shop/orders")
	if err := MarkRestored(root, n); err != nil {
		t.Fatal(err)
	}
	if !isRestoredMark(root, filepath.Join(root, restoredPrefix+n)) {
		t.Fatal("the mark at the root is not recognised")
	}
	if isRestoredMark(root, filepath.Join(root, n, restoredPrefix+n)) {
		t.Fatal("a file inside a snapshot was taken for the mark")
	}
	if got, err := CountLocalSnapshots(root); err != nil || got != 1 {
		t.Fatalf("the listing counts %d snapshots (err %v), want 1", got, err)
	}
}
