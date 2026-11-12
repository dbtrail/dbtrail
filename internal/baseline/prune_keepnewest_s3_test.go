package baseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// Keep-newest-N for a snapshot root that ALSO has an S3 destination (#2658):
// one count for every server. The count decides which snapshots are past it;
// the destination decides whether each of those may go. Every test runs
// against real files, and each one is a way a local snapshot can be the only
// copy of something.

// s3Count prunes root to the newest n against a destination answered by probe.
func s3Count(t *testing.T, root string, n int, probe durableProbe) PruneResult {
	t.Helper()
	res, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: n, Now: kn}, probe)
	if err != nil {
		t.Fatalf("prune by count against S3: %v", err)
	}
	return res
}

// answers builds a probe that says "present" for exactly the names given,
// and records what it was asked.
func answers(asked *[]string, present ...string) durableProbe {
	return func(_ context.Context, name string) (bool, error) {
		if asked != nil {
			*asked = append(*asked, name)
		}
		return slices.Contains(present, name), nil
	}
}

func unreachable(context.Context, string) (bool, error) {
	return false, errors.New("dial tcp: i/o timeout")
}

// five makes five full snapshots, 50 to 10 days old, oldest first.
func five(t *testing.T, root string) []string {
	t.Helper()
	var names []string
	for i := 5; i >= 1; i-- {
		n := snapName(days(10 * i))
		makeSnapshot(t, root, n, true, "shop/orders")
		names = append(names, n)
	}
	return names
}

// Past the count and confirmed in the bucket: removed. The newest N stay
// whatever the bucket says about them.
func TestS3Count_confirmedPastTheCountIsRemoved(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	var asked []string
	res := s3Count(t, root, 3, answers(&asked, s...))
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, s[:2]) {
		t.Fatalf("pruned %v, want the two past the count %v", res.Pruned, s[:2])
	}
	wantOnDisk(t, root, s[2:]...)
	// Only what could go is asked about: the newest three never are.
	slices.Sort(asked)
	if !slices.Equal(asked, s[:2]) {
		t.Fatalf("asked the bucket about %v, want only %v", asked, s[:2])
	}
	if res.KeptNewest != 2 || res.KeptKeeper != 1 {
		t.Fatalf("kept as newest %d and as keeper %d, want 2 and 1: %+v", res.KeptNewest, res.KeptKeeper, res)
	}
}

// Not yet uploaded, or an upload that failed: the bucket answers "not found",
// and the snapshot stays however far past the count it is. It is named, so
// the daemon sends it again.
func TestS3Count_notInTheBucketIsNeverRemoved(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	res := s3Count(t, root, 1, answers(nil, s[1])) // only one of the four past the count is there
	if !slices.Equal(res.Pruned, []string{s[1]}) {
		t.Fatalf("pruned %v, want only the confirmed %s", res.Pruned, s[1])
	}
	want := []string{s[0], s[2], s[3]}
	if !slices.Equal(res.NotAtDestination, want) || res.KeptNotDurable != 3 {
		t.Fatalf("not at the destination %v (kept %d), want %v", res.NotAtDestination, res.KeptNotDurable, want)
	}
	wantOnDisk(t, root, s[0], s[2], s[3], s[4])
	rec, ok, err := ReadNotInDestination(root)
	if err != nil || !ok || rec.Count != 3 {
		t.Fatalf("the record the page reads: %+v ok=%v err=%v, want 3", rec, ok, err)
	}
}

// The bucket cannot be asked: NOTHING is removed, not even the snapshots it
// did confirm before the error, and the attempt is recorded as failed in
// words.
func TestS3Count_unreachableBucketRemovesNothing(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	// The first question is answered "present", the next one fails.
	n := 0
	probe := func(_ context.Context, name string) (bool, error) {
		n++
		if n == 1 {
			return true, nil
		}
		return false, errors.New("dial tcp: i/o timeout")
	}
	res := s3Count(t, root, 1, probe)
	if len(res.Pruned) != 0 {
		t.Fatalf("pruned %v while the bucket could not be asked", res.Pruned)
	}
	wantOnDisk(t, root, s...)
	if !res.DestinationSilent || res.KeptUnasked != 1 || res.ProbeErrors != 3 {
		t.Fatalf("want silent, 1 held back and 3 unchecked: %+v", res)
	}
	rec, ok, err := ReadLastPruneFailure(root)
	if err != nil || !ok {
		t.Fatalf("no failure record: ok=%v err=%v", ok, err)
	}
	t.Logf("failure record, as the page shows it:\n%s", rec.Reason)
	if !strings.Contains(rec.Reason, "The S3 destination could not be asked, so no snapshot that was sent to S3 was removed from this folder in this cleanup.") {
		t.Fatalf("the record does not say nothing sent to S3 was removed: %q", rec.Reason)
	}
	// Nothing is reported as missing from the bucket: it did not answer.
	if _, ok, _ := ReadNotInDestination(root); ok {
		t.Fatal("snapshots reported as not in S3 by a bucket that did not answer")
	}
	// And what an earlier attempt learned stays: a bucket that goes silent
	// must not read as "S3 has them all".
	root3 := t.TempDir()
	five(t, root3)
	s3Count(t, root3, 1, answers(nil))
	before, ok, _ := ReadNotInDestination(root3)
	if !ok || before.Count != 4 {
		t.Fatalf("setup: record %+v ok=%v, want 4 not in S3", before, ok)
	}
	s3Count(t, root3, 1, unreachable)
	if after, ok, _ := ReadNotInDestination(root3); !ok || after.Count != 4 {
		t.Fatalf("a silent bucket changed the record of what S3 lacks: %+v ok=%v", after, ok)
	}

	// The same, with every question failing.
	root2 := t.TempDir()
	s2 := five(t, root2)
	if res := s3Count(t, root2, 1, unreachable); len(res.Pruned) != 0 || !res.DestinationSilent {
		t.Fatalf("an unreachable bucket: %+v", res)
	}
	wantOnDisk(t, root2, s2...)
}

// A snapshot the upload interval kept here on purpose (#2596) is by its own
// definition not meant to be in the bucket, so removing it loses nothing that
// was meant to be durable. With a count it goes by the count alone, like on
// a server with no destination: the bucket is not asked about it and nothing
// newer has to be confirmed there first. The bucket keeps the last snapshot
// that was sent; the disk keeps the newest N.
func TestS3Count_heldSnapshotsGoByTheCount(t *testing.T) {
	setup := func(t *testing.T, held ...int) (string, []string) {
		root := t.TempDir()
		s := five(t, root)
		for _, i := range held {
			if err := MarkUploadHeld(root, s[i]); err != nil {
				t.Fatal(err)
			}
		}
		return root, s
	}

	t.Run("past the count: removed with its mark, the bucket not asked", func(t *testing.T) {
		root, s := setup(t, 0, 1, 2, 3, 4)
		var asked []string
		res := s3Count(t, root, 3, answers(&asked)) // the bucket holds nothing
		slices.Sort(res.Pruned)
		if !slices.Equal(res.Pruned, s[:2]) {
			t.Fatalf("pruned %v, want the two held past the count %v", res.Pruned, s[:2])
		}
		if len(asked) != 0 {
			t.Fatalf("asked the bucket about held snapshots: %v", asked)
		}
		wantOnDisk(t, root, s[2:]...)
		for _, n := range s[:2] {
			if held, _ := UploadHeld(root, n); held {
				t.Errorf("%s: its mark outlived it", n)
			}
		}
		// Inside the count: kept, and still marked (the newest may be owed
		// to the bucket; its mark is what the owed upload reads).
		for _, n := range s[2:] {
			if held, _ := UploadHeld(root, n); !held {
				t.Errorf("%s: kept, but its mark is gone", n)
			}
		}
		if res.KeptNotDurable != 0 || len(res.NotAtDestination) != 0 || res.KeptHeld != 0 {
			t.Fatalf("held snapshots reported as missing or as waiting: %+v", res)
		}
		if _, ok, _ := ReadNotInDestination(root); ok {
			t.Fatal("the page's 'not in S3' record counts snapshots held on purpose")
		}
	})

	t.Run("younger than an hour: kept", func(t *testing.T) {
		root := t.TempDir()
		var s []string
		for _, d := range []time.Duration{50 * time.Minute, 40 * time.Minute, 30 * time.Minute, 20 * time.Minute} {
			n := snapName(d)
			makeSnapshot(t, root, n, true, "shop/orders")
			if err := MarkUploadHeld(root, n); err != nil {
				t.Fatal(err)
			}
			s = append(s, n)
		}
		if res := s3Count(t, root, 1, answers(nil)); len(res.Pruned) != 0 {
			t.Fatalf("pruned %v: held snapshots younger than an hour", res.Pruned)
		}
		wantOnDisk(t, root, s...)
	})

	t.Run("the only copy of a table, the current target and an incomplete one: kept", func(t *testing.T) {
		root := t.TempDir()
		full := snapName(days(60))
		makeSnapshot(t, root, full, true, "shop/orders", "shop/customers")
		part := snapName(days(55))
		makeSnapshot(t, root, part, false, "shop/orders")
		s := five(t, root)
		for _, n := range append([]string{full, part}, s...) {
			if err := MarkUploadHeld(root, n); err != nil {
				t.Fatal(err)
			}
		}
		link := filepath.Join(root, CurrentLinkName)
		if err := os.Remove(link); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err := os.Symlink(s[0], link); err != nil {
			t.Fatal(err)
		}
		s3Count(t, root, 1, answers(nil))
		// full holds the only shop/customers; part is being written; s[0] is
		// what `current` points at; s[4] is the newest.
		wantOnDisk(t, root, full, part, s[0], s[4])
	})

	t.Run("a mark is not a copy in S3: an unheld snapshot the bucket lacks stays", func(t *testing.T) {
		// s[1] was meant for the bucket and is not there (its upload
		// failed). The held ones around it go; it does not, whatever marks
		// its neighbours carry, and it is the one named for a resend.
		root, s := setup(t, 0, 2, 3, 4)
		var asked []string
		res := s3Count(t, root, 1, answers(&asked))
		slices.Sort(res.Pruned)
		if !slices.Equal(res.Pruned, []string{s[0], s[2], s[3]}) {
			t.Fatalf("pruned %v, want only the held ones", res.Pruned)
		}
		if !slices.Equal(asked, []string{s[1]}) || !slices.Equal(res.NotAtDestination, []string{s[1]}) || res.KeptNotDurable != 1 {
			t.Fatalf("asked %v, not at destination %v: want only %s", asked, res.NotAtDestination, s[1])
		}
		wantOnDisk(t, root, s[1], s[4])
	})

	t.Run("the bucket cannot be asked: held ones still go, the rest stay", func(t *testing.T) {
		// A held snapshot needs no answer, so a bucket that gives none does
		// not keep it. What does need an answer stays, every one of it.
		root, s := setup(t, 0, 2)
		res := s3Count(t, root, 1, unreachable)
		slices.Sort(res.Pruned)
		if !slices.Equal(res.Pruned, []string{s[0], s[2]}) {
			t.Fatalf("pruned %v, want only the two held ones", res.Pruned)
		}
		wantOnDisk(t, root, s[1], s[3], s[4])
		if !res.DestinationSilent || res.ProbeErrors != 2 {
			t.Fatalf("want a silent destination with 2 unchecked: %+v", res)
		}
		rec, ok, err := ReadLastPruneFailure(root)
		if err != nil || !ok {
			t.Fatalf("no failure record: ok=%v err=%v", ok, err)
		}
		t.Logf("failure record, as the page shows it:\n%s", rec.Reason)
		if rec.Reason != "The S3 destination could not be asked, so no snapshot that was sent to S3 was removed from this folder in this cleanup. DBTrail asks again at the next one." {
			t.Fatalf("the record: %q", rec.Reason)
		}
	})

	t.Run("with an age too: a held snapshot younger than the age stays", func(t *testing.T) {
		root, s := setup(t, 0, 1, 2, 3) // 50, 40, 30, 20 days, held
		res, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: 1, Retain: days(35), Now: kn}, answers(nil))
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(res.Pruned)
		if !slices.Equal(res.Pruned, s[:2]) {
			t.Fatalf("count 1, age 35d: pruned %v, want only the two held ones older than the age", res.Pruned)
		}
	})

	t.Run("dry run removes no folder and no mark", func(t *testing.T) {
		root, s := setup(t, 0, 1)
		res, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: 3, Now: kn, DryRun: true}, answers(nil))
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Pruned) != 2 {
			t.Fatalf("a dry run would remove %v, want the two held", res.Pruned)
		}
		wantOnDisk(t, root, s...)
		if held, _ := UploadHeld(root, s[0]); !held {
			t.Fatal("a dry run removed a mark")
		}
	})
}

// Cycle after cycle of a 5-minute schedule that sends to S3 once a day: the
// disk ends each cleanup with the count's snapshots and the ones still
// younger than an hour, never with the held ones of the day, and the bucket
// keeps the last snapshot that was sent. A run whose upload failed leaves a
// snapshot that is never removed until the bucket has it.
func TestS3Count_cyclesOfASpacedSchedule(t *testing.T) {
	root := t.TempDir()
	bucket := map[string]bool{}
	probe := func(_ context.Context, name string) (bool, error) { return bucket[name], nil }
	start := kn.Add(-48 * time.Hour)
	name := func(at time.Time) string { return strings.ReplaceAll(at.Format(time.RFC3339), ":", "-") }
	publish := func(at time.Time, sent, held bool) string {
		n := name(at)
		makeSnapshot(t, root, n, true, "shop/orders")
		if sent {
			bucket[n] = true
		}
		if held {
			if err := MarkUploadHeld(root, n); err != nil {
				t.Fatal(err)
			}
		}
		return n
	}
	prune := func(now time.Time) PruneResult {
		res, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: 3, Now: now}, probe)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	// The full read, sent. Then an update every hour, held, with a cleanup
	// after each; one run's upload was due and failed (not held, not sent);
	// the next day's first run is sent.
	first := publish(start, true, false)
	var failed, lastSent string
	lastSent = first
	for h := 1; h <= 30; h++ {
		at := start.Add(time.Duration(h) * time.Hour)
		switch h {
		case 10:
			failed = publish(at, false, false)
		case 24:
			lastSent = publish(at, true, false)
		default:
			publish(at, false, true)
		}
		res := prune(at.Add(30 * time.Minute))
		for _, p := range res.Pruned {
			if p == failed {
				t.Fatalf("hour %d: removed %s, whose upload failed and which S3 does not have", h, p)
			}
		}
	}
	end := start.Add(30 * time.Hour)
	prune(end.Add(2 * time.Hour))
	left := onDisk(t, root)
	want := sorted(failed, name(end.Add(-2*time.Hour)), name(end.Add(-time.Hour)), name(end))
	if !slices.Equal(left, want) {
		t.Fatalf("on disk after the cycles: %v\nwant the newest 3 and the one S3 lacks: %v", left, want)
	}
	if !bucket[lastSent] || !bucket[first] || len(bucket) != 2 {
		t.Fatalf("the bucket holds %v, want the two snapshots that were sent", bucket)
	}
	// No mark points at a folder that is gone.
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		if n, ok := strings.CutPrefix(e.Name(), uploadHeldPrefix); ok && !slices.Contains(left, n) {
			t.Errorf("a held mark outlived its snapshot: %s", e.Name())
		}
	}
	// The bucket gets it at last: the next cleanup removes it.
	bucket[failed] = true
	prune(end.Add(3 * time.Hour))
	wantOnDisk(t, root, want[1:]...)
}

// The guarantees of the local-only count hold with a destination: an
// _INCOMPLETE snapshot, the `current` pointer's target, anything younger
// than an hour, an unreadable snapshot and the newest copy of each table all
// stay even when the bucket says it has every one of them.
func TestS3Count_localGuaranteesHold(t *testing.T) {
	everything := func(context.Context, string) (bool, error) { return true, nil }

	t.Run("incomplete", func(t *testing.T) {
		root := t.TempDir()
		s := five(t, root)
		part := snapName(days(60))
		makeSnapshot(t, root, part, false, "shop/orders")
		res := s3Count(t, root, 1, everything)
		if slices.Contains(res.Pruned, part) || res.KeptIncomplete != 1 {
			t.Fatalf("an incomplete snapshot: %+v", res)
		}
		wantOnDisk(t, root, part, s[4])
	})

	t.Run("the current pointer's target", func(t *testing.T) {
		root := t.TempDir()
		s := five(t, root)
		// The pointer lags on the oldest snapshot (a publication that failed).
		link := filepath.Join(root, CurrentLinkName)
		if err := os.Remove(link); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err := os.Symlink(s[0], link); err != nil {
			t.Fatal(err)
		}
		res := s3Count(t, root, 1, everything)
		if slices.Contains(res.Pruned, s[0]) {
			t.Fatal("removed the snapshot `current` points at")
		}
		wantOnDisk(t, root, s[0], s[4])
		if name, ok := ResolveCurrentPointer(root); !ok || name != s[0] {
			t.Fatalf("current resolves to %q ok=%v after the prune", name, ok)
		}
	})

	t.Run("younger than an hour", func(t *testing.T) {
		root := t.TempDir()
		var s []string
		for _, d := range []time.Duration{50 * time.Minute, 40 * time.Minute, 30 * time.Minute, 20 * time.Minute} {
			n := snapName(d)
			makeSnapshot(t, root, n, true, "shop/orders")
			s = append(s, n)
		}
		var asked []string
		res := s3Count(t, root, 1, answers(&asked, s...))
		if len(res.Pruned) != 0 || len(asked) != 0 {
			t.Fatalf("pruned %v, asked %v: nothing younger than an hour goes or is asked about", res.Pruned, asked)
		}
	})

	t.Run("the newest copy of each table", func(t *testing.T) {
		root := t.TempDir()
		full := snapName(days(40))
		makeSnapshot(t, root, full, true, "shop/orders", "shop/customers")
		var runs []string
		for i := 3; i >= 1; i-- {
			n := snapName(days(10 * i))
			makeSnapshot(t, root, n, true, "shop/orders")
			runs = append(runs, n)
		}
		res := s3Count(t, root, 1, everything)
		if slices.Contains(res.Pruned, full) {
			t.Fatal("removed the only snapshot holding shop/customers")
		}
		wantOnDisk(t, root, full, runs[2])
		for tbl, ok := range tablesResolvable(t, root, kn) {
			if !ok {
				t.Errorf("%s resolves to nothing after the prune", tbl)
			}
		}
	})

	t.Run("unreadable", func(t *testing.T) {
		root := t.TempDir()
		s := five(t, root)
		bad := filepath.Join(root, s[0])
		orig := readDir
		readDir = func(name string) ([]os.DirEntry, error) {
			if name == bad {
				return nil, errors.New("too many open files")
			}
			return orig(name)
		}
		t.Cleanup(func() { readDir = orig })
		res := s3Count(t, root, 1, everything)
		if slices.Contains(res.Pruned, s[0]) || res.KeptUnreadable != 1 {
			t.Fatalf("an unreadable snapshot: %+v", res)
		}
	})
}

// The count and --baseline-retain on one server: a snapshot goes only when
// both say so (past the count AND older than the age), and the bucket has it.
// The age only ever keeps more, as it does in a folder with no destination.
func TestS3Count_withAnAgeBothMustAgree(t *testing.T) {
	everything := func(context.Context, string) (bool, error) { return true, nil }
	prune := func(t *testing.T, root string, keep int, retain time.Duration) PruneResult {
		t.Helper()
		res, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: keep, Retain: retain, Now: kn}, everything)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	// Past the count, younger than the age: kept.
	root := t.TempDir()
	s := five(t, root) // 50, 40, 30, 20, 10 days
	res := prune(t, root, 1, days(35))
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, s[:2]) {
		t.Fatalf("count 1, age 35d: pruned %v, want only the two older than the age %v", res.Pruned, s[:2])
	}

	// Older than the age, inside the count: kept. The age alone never takes
	// a folder under its count.
	root = t.TempDir()
	s = five(t, root)
	res = prune(t, root, 4, days(15))
	if !slices.Equal(res.Pruned, s[:1]) {
		t.Fatalf("count 4, age 15d: pruned %v, want only %s (three more are older than the age, inside the count)", res.Pruned, s[0])
	}
	wantOnDisk(t, root, s[1:]...)

	// The age alone (no count) is the mode it always was.
	root = t.TempDir()
	s = five(t, root)
	res = prune(t, root, 0, days(15))
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, s[:4]) {
		t.Fatalf("age 15d and no count: pruned %v, want the four older ones", res.Pruned)
	}
}

// Across every mix of counts and bucket answers, the newest min(N, total)
// snapshots survive and nothing the bucket did not confirm is removed.
func TestS3Count_neverFewerThanNNorUnconfirmed(t *testing.T) {
	for total := 0; total <= 6; total++ {
		for n := 1; n <= 4; n++ {
			for mask := 0; mask < 1<<total; mask++ {
				root := t.TempDir()
				var names, present []string
				for i := total; i >= 1; i-- {
					name := snapName(days(i))
					makeSnapshot(t, root, name, true, "shop/orders")
					names = append(names, name)
					if mask&(1<<(i-1)) != 0 {
						present = append(present, name)
					}
				}
				res := s3Count(t, root, n, answers(nil, present...))
				for _, p := range res.Pruned {
					if !slices.Contains(present, p) {
						t.Fatalf("total %d n %d mask %b: removed %s, which the bucket does not have", total, n, mask, p)
					}
				}
				left := onDisk(t, root)
				keep := min(n, total)
				if len(left) < keep {
					t.Fatalf("total %d n %d mask %b: %d left on disk, fewer than the count %d: %v", total, n, mask, len(left), keep, left)
				}
				if tail := names[total-keep:]; !slices.Equal(left[len(left)-keep:], tail) {
					t.Fatalf("total %d n %d mask %b: the newest %d are %v, left on disk %v", total, n, mask, keep, tail, left)
				}
			}
		}
	}
}

// PruneLocal accepts a destination with a count and no age, and still refuses
// a destination with neither.
func TestS3Count_argumentsOfPruneLocal(t *testing.T) {
	if _, err := PruneLocal(context.Background(), PruneOptions{LocalDir: t.TempDir(), S3URL: "s3://b/p"}); err == nil || !strings.Contains(err.Error(), "Retain must be positive") {
		t.Fatalf("a destination with no age and no count: %v", err)
	}
	if _, err := PruneLocal(context.Background(), PruneOptions{LocalDir: t.TempDir(), S3URL: "s3://b/p", KeepNewest: 3, Retain: -time.Hour}); err == nil || !strings.Contains(err.Error(), "Retain must not be negative") {
		t.Fatalf("a destination with a count and a negative age: %v", err)
	}
	// A count and no age gets as far as reading the destination: the URL is
	// the next thing refused, which proves the age was not asked for.
	if _, err := PruneLocal(context.Background(), PruneOptions{LocalDir: t.TempDir(), S3URL: "not-a-url", KeepNewest: 3}); err == nil || !strings.Contains(err.Error(), "invalid S3 URL") {
		t.Fatalf("a destination with a count and no age: %v", err)
	}
}
