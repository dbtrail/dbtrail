package baseline

import (
	"context"
	"encoding/json"
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
	if what, _, stuck := readingNow(root, time.Now().Add(readHoldLimit-time.Minute)); what == "" || stuck != 0 {
		t.Fatalf("just inside the limit: what %q stuck %d", what, stuck)
	}
	if what, _, stuck := readingNow(root, time.Now().Add(readHoldLimit+time.Minute)); what != "" || stuck != 1 {
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

	// With a destination that has it: still kept, asked about once, and
	// not reported as missing.
	var asked []string
	res2, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: 1, Now: kn}, answers(&asked, second, s[2]))
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Pruned) != 0 || !slices.Equal(asked, []string{second}) || len(res2.NotAtDestination) != 0 {
		t.Fatalf("with a destination: pruned %v, asked %v, missing %v", res2.Pruned, asked, res2.NotAtDestination)
	}
	// With a destination that does NOT have it (its upload failed): kept,
	// and listed as missing there, which is what sends it again. On a
	// bucket that cannot be asked it is kept, and the attempt is a failure.
	res3, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: 1, Now: kn}, answers(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Pruned) != 0 || !slices.Equal(res3.NotAtDestination, []string{second}) || res3.KeptNotDurable != 1 {
		t.Fatalf("the bucket lacks the restore's result: %+v", res3)
	}
	if rec, ok, _ := ReadNotInDestination(root); !ok || rec.Count != 1 || !slices.Equal(rec.Names, []string{second}) {
		t.Fatalf("the record the re-send and the page read: %+v ok=%v", rec, ok)
	}
	res4, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: 1, Now: kn}, unreachable)
	if err != nil {
		t.Fatal(err)
	}
	if len(res4.Pruned) != 0 || res4.ProbeErrors != 1 {
		t.Fatalf("a bucket that cannot be asked about the restore's result: %+v", res4)
	}
	wantOnDisk(t, root, s[2], second)

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

// A reader on ONE snapshot pins it: the prune keeps that snapshot, counts it,
// and removes the others past the count. Many readers of one snapshot pin
// one snapshot. When the pin is released, or its deadline passes, the
// snapshot goes like any other.
func TestNoFloor_aPinKeepsOneSnapshotAndNothingElse(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 25, 20, 15, 10, 5)
	var releases []func()
	for range 50 {
		releases = append(releases, PinSnapshot(root, s[1], "a snapshot download", time.Hour))
	}
	if got := Pinned(root); !slices.Equal(got, []string{s[1]}) {
		t.Fatalf("50 downloads of one snapshot pin %v, want only %s", got, s[1])
	}
	res := keepNewest(t, root, 1)
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, []string{s[0], s[2], s[3]}) || res.KeptPinned != 1 || res.Reading != "" {
		t.Fatalf("pruned %v, KeptPinned %d, Reading %q: want the three others past the count, 1, and no stand-still", res.Pruned, res.KeptPinned, res.Reading)
	}
	wantOnDisk(t, root, s[1], s[4])
	if _, ok, _ := ReadLastPruneFailure(root); ok {
		t.Fatal("a pinned snapshot was recorded as a failed cleanup")
	}
	// All but one released: still pinned.
	for _, r := range releases[1:] {
		r()
	}
	if res := keepNewest(t, root, 1); len(res.Pruned) != 0 || res.KeptPinned != 1 {
		t.Fatalf("one reader left: %+v", res)
	}
	releases[0]()
	releases[0]()
	if res := keepNewest(t, root, 1); !slices.Equal(res.Pruned, []string{s[1]}) || res.KeptPinned != 0 {
		t.Fatalf("after the last release: %+v", res)
	}
	wantOnDisk(t, root, s[4])
}

// With a destination a pinned snapshot is not asked about and is not
// reported as missing from the bucket.
func TestNoFloor_aPinnedSnapshotIsNotAskedAbout(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 15, 10, 5)
	defer PinSnapshot(root, s[0], "a SQL statement", time.Hour)()
	var asked []string
	res := s3Count(t, root, 1, answers(&asked, s[1]))
	if !slices.Equal(res.Pruned, []string{s[1]}) || !slices.Equal(asked, []string{s[1]}) || res.KeptPinned != 1 || res.KeptNotDurable != 0 {
		t.Fatalf("pruned %v, asked %v, %+v", res.Pruned, asked, res)
	}
}

// A pin past its deadline keeps nothing: a reader whose length someone else
// decides cannot keep a snapshot on the disk.
func TestNoFloor_aPinPastItsDeadlineKeepsNothing(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 15, 10, 5)
	release := PinSnapshot(root, s[0], "a snapshot download", 50*time.Millisecond)
	defer release()
	if res := keepNewest(t, root, 2); len(res.Pruned) != 0 || res.KeptPinned != 1 {
		t.Fatalf("inside the deadline: %+v", res)
	}
	time.Sleep(80 * time.Millisecond)
	if got := Pinned(root); len(got) != 0 {
		t.Fatalf("past the deadline the pin still counts: %v", got)
	}
	res := keepNewest(t, root, 2)
	if !slices.Equal(res.Pruned, []string{s[0]}) || res.KeptPinned != 0 {
		t.Fatalf("past the deadline: %+v", res)
	}
	// The default is the folder-wide limit, not forever.
	defer PinSnapshot(root, s[1], "the Explain of a check", 0)()
	if got := pinnedNow(root, time.Now().Add(readHoldLimit+time.Minute)); len(got) != 0 {
		t.Fatalf("a pin with no limit of its own outlives the limit: %v", got)
	}
}

// A cleanup that stands still for a job leaves a record saying which job and
// when THAT JOB started; a later cleanup that waits for another job names
// the other one and its start; the first one that runs removes it. A record
// nothing has rewritten for two hours says nothing. Not snapshot data.
func TestNoFloor_theWaitIsRecordedForThePage(t *testing.T) {
	root := t.TempDir()
	minutes(t, root, 15, 10, 5)
	if _, ok := ReadCleanupWaiting(root); ok {
		t.Fatal("a record before any cleanup")
	}
	started := time.Now()
	end := BeginRead(root, "a restore to a moment")
	time.Sleep(20 * time.Millisecond)
	keepNewest(t, root, 1)
	first, ok := ReadCleanupWaiting(root)
	if !ok || first.What != "a restore to a moment" {
		t.Fatalf("after a cleanup that waited: %+v ok=%v", first, ok)
	}
	// The job's own start, not the cleanup's time.
	if first.Since.Before(started.Add(-time.Second)) || !first.Since.Before(first.At) {
		t.Fatalf("since %s is not when the job started (%s), or not before the cleanup (%s)", first.Since, started, first.At)
	}
	// Another job takes over: the record names it, with its own start.
	time.Sleep(20 * time.Millisecond)
	end2 := BeginRead(root, "a check of the snapshots")
	end()
	keepNewest(t, root, 1)
	second, ok := ReadCleanupWaiting(root)
	if !ok || second.What != "a check of the snapshots" || !second.Since.After(first.Since) {
		t.Fatalf("after the job changed: %+v, first %+v", second, first)
	}
	if !isPruneArtifact(root, filepath.Join(root, CleanupWaitingFile)) {
		t.Fatal("the record would be uploaded as snapshot data")
	}
	// Left behind by a process that stopped: not believed after two hours.
	old := CleanupWaiting{Since: time.Now().Add(-5 * time.Hour).UTC(), At: time.Now().Add(-cleanupWaitingFresh - time.Minute).UTC(), What: "a check of the snapshots"}
	b, _ := json.Marshal(old)
	if err := os.WriteFile(filepath.Join(root, CleanupWaitingFile), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if rec, ok := ReadCleanupWaiting(root); ok {
		t.Fatalf("a record two hours stale is still reported: %+v", rec)
	}
	end2()
	keepNewest(t, root, 1)
	if _, err := os.Stat(filepath.Join(root, CleanupWaitingFile)); !os.IsNotExist(err) {
		t.Fatalf("the cleanup ran and the record is still there: %v", err)
	}
}

// The restore's result takes no place in the count even when the moment
// restored falls among the newest N: snapshots 15, 10 and 5 minutes old, a
// restore of 7 minutes ago, count 3: all four stay. Counted, the 15-minute
// one was removed and the folder held 3.
func TestNoFloor_aRestoreAmongTheNewestTakesNoPlace(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 15, 10, 5)
	restored := snapName(7 * time.Minute)
	makeSnapshot(t, root, restored, true, "shop/orders")
	if err := MarkRestored(root, restored); err != nil {
		t.Fatal(err)
	}
	res := keepNewest(t, root, 3)
	if len(res.Pruned) != 0 {
		t.Fatalf("pruned %v: the restore's result took a place in the count", res.Pruned)
	}
	wantOnDisk(t, root, s[0], s[1], s[2], restored)
	// One more snapshot: the 15-minute one goes, the restore's stays.
	next := snapName(time.Minute)
	makeSnapshot(t, root, next, true, "shop/orders")
	if res := keepNewest(t, root, 3); !slices.Equal(res.Pruned, []string{s[0]}) {
		t.Fatalf("after one more snapshot pruned %v, want [%s]", res.Pruned, s[0])
	}
	wantOnDisk(t, root, s[1], s[2], next, restored)
}

// A reader that registers between the plan and the removal is honoured: the
// bucket is asked in between, which takes seconds, and that is where a
// download pins its snapshot or a restore starts. Injected at the question.
func TestNoFloor_aReaderThatArrivesDuringTheCleanupIsHonoured(t *testing.T) {
	t.Run("a pin", func(t *testing.T) {
		root := t.TempDir()
		s := minutes(t, root, 20, 15, 10, 5)
		var release func()
		probe := func(_ context.Context, name string) (bool, error) {
			if release == nil {
				release = PinSnapshot(root, s[0], "a snapshot download", time.Hour)
			}
			return true, nil
		}
		res := s3Count(t, root, 1, probe)
		defer release()
		slices.Sort(res.Pruned)
		if !slices.Equal(res.Pruned, []string{s[1], s[2]}) || res.KeptPinned != 1 {
			t.Fatalf("pruned %v, KeptPinned %d: want the two not pinned, and 1", res.Pruned, res.KeptPinned)
		}
		wantOnDisk(t, root, s[0], s[3])
	})
	t.Run("a job on the folder", func(t *testing.T) {
		root := t.TempDir()
		s := minutes(t, root, 20, 15, 10, 5)
		var end func()
		probe := func(context.Context, string) (bool, error) {
			if end == nil {
				end = BeginRead(root, "a restore to a moment")
			}
			return true, nil
		}
		res := s3Count(t, root, 1, probe)
		defer end()
		if len(res.Pruned) != 0 || res.Reading != "a restore to a moment" {
			t.Fatalf("pruned %v, Reading %q: a job that started during the cleanup lost snapshots", res.Pruned, res.Reading)
		}
		wantOnDisk(t, root, s...)
		if rec, ok := ReadCleanupWaiting(root); !ok || rec.What != "a restore to a moment" {
			t.Fatalf("the wait is not on record: %+v ok=%v", rec, ok)
		}
	})
}
