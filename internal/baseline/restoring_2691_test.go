package baseline

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// A restore marks its result BEFORE the result exists (#2691): the daemon
// dying between the publish and the mark left the result unmarked, and the
// next cleanup removed it by the count. The mark written first is pending
// until the restore's snapshot is complete; a pending mark is never swept
// while young, never names a result, and never takes the place of the
// previous restore's mark.

func restoringMark(root, name string) string { return filepath.Join(root, restoredPrefix+name) }

// The crash window: the mark is written, the snapshot is published, and the
// process dies before the restore ends. The result stays, by the count and
// outside it, as the last restore's result.
func TestRestoring_aResultPublishedBeforeTheEndIsKept(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 15, 10, 5)
	restored := snapName(3 * time.Hour)
	if err := MarkRestoring(root, restored); err != nil {
		t.Fatal(err)
	}
	makeSnapshot(t, root, restored, true, "shop/orders")
	// No MarkRestored: the process died here.
	res := keepNewest(t, root, 1)
	if slices.Contains(res.Pruned, restored) {
		t.Fatalf("the cleanup removed a restore's result whose restore died after publishing it: pruned %v", res.Pruned)
	}
	wantOnDisk(t, root, s[2], restored)
	if name, _ := Restored(root); name != restored {
		t.Fatalf("Restored = %q, want the published result %q", name, restored)
	}
	kept, err := KeptNow(root, 1, 0)
	if err != nil || !kept[restored] {
		t.Fatalf("KeptNow = %v, %v: the result must be kept whatever readers there are", kept, err)
	}
}

// A pending mark whose folder does not exist yet (the restore is still
// reading) is not swept as the mark of a result removed by hand, is not
// named as a result, and is not uploaded.
func TestRestoring_aPendingMarkWithNoFolderStays(t *testing.T) {
	root := t.TempDir()
	minutes(t, root, 15, 10, 5)
	restored := snapName(3 * time.Hour)
	if err := MarkRestoring(root, restored); err != nil {
		t.Fatal(err)
	}
	keepNewest(t, root, 1)
	if _, err := os.Stat(restoringMark(root, restored)); err != nil {
		t.Fatalf("a real cleanup swept the mark of a restore that is still running: %v", err)
	}
	if name, _ := Restored(root); name != "" {
		t.Fatalf("Restored = %q while the restore has published nothing", name)
	}
	if !isRestoredMark(root, restoringMark(root, restored)) {
		t.Fatal("a pending mark is not recognised as a mark, so an upload of the root would send it")
	}
}

// A pending mark next to the previous restore's result: the previous
// result keeps its mark and its place until the new one is complete, so a
// restore that fails midway (or dies) leaves it protected.
func TestRestoring_thePreviousResultKeepsItsMarkWhileANewOneRuns(t *testing.T) {
	for _, folder := range []string{"none", "incomplete"} {
		t.Run(folder, func(t *testing.T) {
			root := t.TempDir()
			s := minutes(t, root, 15, 10, 5)
			first := snapName(3 * time.Hour)
			makeSnapshot(t, root, first, true, "shop/orders")
			if err := markRestoredAt(root, first, restoredNow().Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
			second := snapName(5 * time.Hour)
			if err := MarkRestoring(root, second); err != nil {
				t.Fatal(err)
			}
			if folder == "incomplete" {
				makeSnapshot(t, root, second, false, "shop/orders")
			}
			res := keepNewest(t, root, 1)
			if slices.Contains(res.Pruned, first) {
				t.Fatalf("a running restore cost the previous result its place: pruned %v", res.Pruned)
			}
			if _, err := os.Stat(restoringMark(root, first)); err != nil {
				t.Fatalf("the previous result's mark was removed while the next restore had published nothing: %v", err)
			}
			if name, _ := Restored(root); name != first {
				t.Fatalf("Restored = %q, want the previous result %q", name, first)
			}
			want := []string{s[2], first}
			if folder == "incomplete" {
				want = append(want, second)
			}
			wantOnDisk(t, root, want...)

			// The restore fails: its own mark goes, the previous one stays.
			ForgetRestoring(root, second)
			if _, err := os.Stat(restoringMark(root, second)); !os.IsNotExist(err) {
				t.Fatalf("a failed restore left its mark: %v", err)
			}
			if _, err := os.Stat(restoringMark(root, first)); err != nil {
				t.Fatalf("forgetting a failed restore removed the previous result's mark: %v", err)
			}
		})
	}
}

// The restore ends: the mark is rewritten as finished, with the time the
// restore finished, and the new result replaces the previous one.
func TestRestoring_theEndRewritesTheMarkAsFinished(t *testing.T) {
	root := t.TempDir()
	minutes(t, root, 15, 10, 5)
	first := snapName(3 * time.Hour)
	makeSnapshot(t, root, first, true, "shop/orders")
	if err := markRestoredAt(root, first, restoredNow().Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	second := snapName(5 * time.Hour)
	start := restoredNow().Add(-time.Hour)
	if err := markRestoringAt(root, second, start); err != nil {
		t.Fatal(err)
	}
	makeSnapshot(t, root, second, true, "shop/orders")
	if err := MarkRestored(root, second); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(restoringMark(root, second))
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(string(b), restoringWord) {
		t.Fatalf("the mark still reads as pending after the restore ended: %q", b)
	}
	at, pending, ok := restoredAt(root, second, restoredNow(), true)
	if !ok || pending || at.Before(start.Add(30*time.Minute)) {
		t.Fatalf("restoredAt = %s pending=%v ok=%v: want the end time, not the start", at, pending, ok)
	}
	// Forgetting a finished mark does nothing: only a pending one is the
	// failed restore's own.
	ForgetRestoring(root, second)
	if _, err := os.Stat(restoringMark(root, second)); err != nil {
		t.Fatalf("ForgetRestoring removed a finished mark: %v", err)
	}
	res := keepNewest(t, root, 1)
	if !slices.Equal(res.Pruned, []string{first}) {
		t.Fatalf("pruned %v, want the previous result [%s]", res.Pruned, first)
	}
}

// A pending mark lives a day like any mark: one left by a restore that died
// before it published anything goes after RestoredKeep.
func TestRestoring_aPendingMarkExpires(t *testing.T) {
	now := time.Now()
	prev := restoredNow
	t.Cleanup(func() { restoredNow = prev })
	restoredNow = func() time.Time { return now }
	root := t.TempDir()
	minutes(t, root, 5)
	restored := snapName(3 * time.Hour)
	if err := markRestoringAt(root, restored, now.Add(-RestoredKeep+time.Minute)); err != nil {
		t.Fatal(err)
	}
	keepNewest(t, root, 1)
	if _, err := os.Stat(restoringMark(root, restored)); err != nil {
		t.Fatalf("a pending mark younger than a day was swept: %v", err)
	}
	if err := markRestoringAt(root, restored, now.Add(-RestoredKeep-time.Minute)); err != nil {
		t.Fatal(err)
	}
	keepNewest(t, root, 1)
	if _, err := os.Stat(restoringMark(root, restored)); !os.IsNotExist(err) {
		t.Fatalf("a pending mark older than a day stayed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, uploadHeldPrefix+restored)); !os.IsNotExist(err) {
		t.Fatalf("an expired pending mark with no snapshot left a held mark behind: %v", err)
	}
}

// A pending mark dated in the future (the clock moved back) is read as now
// and rewritten once, and stays pending: rewritten as finished, a restore
// still running would read as one that ended.
func TestRestoring_aFuturePendingMarkStaysPending(t *testing.T) {
	root := t.TempDir()
	minutes(t, root, 5)
	restored := snapName(3 * time.Hour)
	if err := markRestoringAt(root, restored, restoredNow().Add(72*time.Hour)); err != nil {
		t.Fatal(err)
	}
	keepNewest(t, root, 1)
	at, pending, ok := restoredAt(root, restored, restoredNow(), true)
	if !ok || !pending || at.After(restoredNow()) {
		t.Fatalf("restoredAt = %s pending=%v ok=%v: want a pending mark dated now", at, pending, ok)
	}
}

// The mark is written into a root that does not exist yet (the first
// restore of a new folder), and refuses a name that is not a snapshot.
func TestRestoring_theMarkMakesItsRootAndRefusesBadNames(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not", "yet")
	n := snapName(time.Hour)
	if err := MarkRestoring(root, n); err != nil {
		t.Fatalf("MarkRestoring into a root that does not exist yet: %v", err)
	}
	if _, err := os.Stat(restoringMark(root, n)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "../outside", "current", n + "/x"} {
		if err := MarkRestoring(root, bad); err == nil {
			t.Fatalf("MarkRestoring(%q) wrote a mark", bad)
		}
	}
	ForgetRestoring(root, "../outside") // must not panic or touch anything
}

// A mark an earlier build wrote (the time alone) is a finished mark, and
// one this build writes pending is still read by the time it holds.
func TestRestoring_markFormats(t *testing.T) {
	root := t.TempDir()
	n := snapName(time.Hour)
	at := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		body    string
		pending bool
	}{
		{at.Format(time.RFC3339Nano) + "\n", false},
		{restoringWord + " " + at.Format(time.RFC3339Nano) + "\n", true},
	} {
		if err := os.WriteFile(restoringMark(root, n), []byte(tc.body), 0o644); err != nil {
			t.Fatal(err)
		}
		got, pending, ok := restoredAt(root, n, at.Add(time.Hour), true)
		if !ok || !got.Equal(at) || pending != tc.pending {
			t.Fatalf("mark %q: at=%s pending=%v ok=%v", tc.body, got, pending, ok)
		}
	}
}
