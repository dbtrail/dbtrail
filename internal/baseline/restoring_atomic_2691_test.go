package baseline

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A prune in ANOTHER process reads the marks while a restore writes its own
// (#2691 review). Written in place, a mark is empty for an instant (the file
// is truncated, then written), and an empty mark read as a finished one made
// a restore that had published nothing "the last restore": the previous
// result lost its mark and went by the count. The mark is now written to a
// temporary file and renamed over, and a mark that cannot be parsed protects
// like a pending one.

// A reader never sees a mark that is empty or cannot be parsed while it is
// rewritten again and again.
func TestRestoringAtomic_aReaderNeverSeesATornMark(t *testing.T) {
	root := t.TempDir()
	name := snapName(5 * time.Hour)
	if err := MarkRestoring(root, name); err != nil {
		t.Fatal(err)
	}
	mark := filepath.Join(root, restoredPrefix+name)
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load(); i++ {
			if i%2 == 0 {
				_ = MarkRestoring(root, name)
			} else {
				_ = MarkRestored(root, name)
			}
		}
	}()
	torn := 0
	deadline := time.Now().Add(2 * time.Second)
	for reads := 0; time.Now().Before(deadline); reads++ {
		if _, _, ok := readRestoredMark(mark); !ok {
			torn++
		}
	}
	stop.Store(true)
	wg.Wait()
	if torn != 0 {
		t.Fatalf("a reader saw an empty or partial mark %d times while it was rewritten", torn)
	}
	leftovers(t, root, 0)
}

// The consequences, under a real cleanup from another "process" (a loop that
// does not see the restore's in-process read hold): the previous result keeps
// its mark and its snapshot, and the running restore keeps its own mark,
// with no folder yet and on a retry over a folder holding only _INCOMPLETE.
func TestRestoringAtomic_aConcurrentCleanupKeepsBothMarks(t *testing.T) {
	for _, withFolder := range []bool{false, true} {
		t.Run(map[bool]string{false: "no folder yet", true: "retry onto an incomplete folder"}[withFolder], func(t *testing.T) {
			root := t.TempDir()
			minutes(t, root, 15, 10, 5)
			first := snapName(3 * time.Hour)
			makeSnapshot(t, root, first, true, "shop/orders")
			if err := markRestoredAt(root, first, restoredNow().Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
			second := snapName(5 * time.Hour)
			if withFolder {
				makeSnapshot(t, root, second, false)
			}
			if err := MarkRestoring(root, second); err != nil {
				t.Fatal(err)
			}
			var stop atomic.Bool
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !stop.Load() {
					_ = MarkRestoring(root, second)
				}
			}()
			deadline := time.Now().Add(2 * time.Second)
			var pruned []string
			for time.Now().Before(deadline) {
				res := keepNewest(t, root, 1)
				pruned = append(pruned, res.Pruned...)
				if _, err := os.Stat(filepath.Join(root, restoredPrefix+first)); err != nil {
					break
				}
				if _, err := os.Stat(filepath.Join(root, restoredPrefix+second)); err != nil {
					break
				}
			}
			stop.Store(true)
			wg.Wait()
			if slices.Contains(pruned, first) {
				t.Fatalf("the cleanup removed the previous restore's result while the next restore wrote its mark: pruned %v", pruned)
			}
			for _, n := range []string{first, second} {
				if _, err := os.Stat(filepath.Join(root, restoredPrefix+n)); err != nil {
					t.Fatalf("the mark of %s was lost while the restore wrote its own: %v", n, err)
				}
			}
		})
	}
}

// A mark that holds no time (empty, or not a time) protects like a pending
// one: it is not swept for lack of a folder, it names no result while its
// folder is incomplete, and it does not take the previous result's place.
// Bounded by its file time: it expires a day after it was last written.
func TestRestoringAtomic_anUnparsableMarkProtectsLikeAPendingOne(t *testing.T) {
	for _, body := range []string{"", "not a time\n"} {
		t.Run(strings.TrimSpace(body)+"|", func(t *testing.T) {
			root := t.TempDir()
			minutes(t, root, 15, 10, 5)
			first := snapName(3 * time.Hour)
			makeSnapshot(t, root, first, true, "shop/orders")
			if err := markRestoredAt(root, first, restoredNow().Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
			second := snapName(5 * time.Hour)
			third := snapName(6 * time.Hour)
			makeSnapshot(t, root, third, false)
			for _, n := range []string{second, third} {
				if err := os.WriteFile(filepath.Join(root, restoredPrefix+n), []byte(body), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			res := keepNewest(t, root, 1)
			if slices.Contains(res.Pruned, first) {
				t.Fatalf("an unparsable mark took the previous result's place: pruned %v", res.Pruned)
			}
			for _, n := range []string{first, second, third} {
				if _, err := os.Stat(filepath.Join(root, restoredPrefix+n)); err != nil {
					t.Fatalf("mark of %s lost: %v", n, err)
				}
			}
			if got, _ := Restored(root); got != first {
				t.Fatalf("Restored = %q, want the previous result %q", got, first)
			}
			// Past a day by its file time it goes.
			old := time.Now().Add(-RestoredKeep - time.Minute)
			if err := os.Chtimes(filepath.Join(root, restoredPrefix+second), old, old); err != nil {
				t.Fatal(err)
			}
			keepNewest(t, root, 1)
			if _, err := os.Stat(filepath.Join(root, restoredPrefix+second)); !os.IsNotExist(err) {
				t.Fatalf("an unparsable mark older than a day stayed: %v", err)
			}
		})
	}
}

// leftovers checks how many of the mark's temporary files are in root.
func leftovers(t *testing.T, root string, want int) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), restoredTmpPrefix) {
			got = append(got, e.Name())
		}
	}
	if len(got) != want {
		t.Fatalf("temporary mark files in the folder: %v, want %d", got, want)
	}
	return got
}

// The temporary file is never taken for a mark, and one a process left
// behind when it died goes at a cleanup once it is an hour old.
func TestRestoringAtomic_temporaryFiles(t *testing.T) {
	if strings.HasPrefix(restoredTmpPrefix, restoredPrefix) {
		t.Fatalf("the temporary name %q starts like a mark %q", restoredTmpPrefix, restoredPrefix)
	}
	root := t.TempDir()
	minutes(t, root, 5)
	young := filepath.Join(root, restoredTmpPrefix+"young")
	stale := filepath.Join(root, restoredTmpPrefix+"stale")
	for _, p := range []string{young, stale} {
		if err := os.WriteFile(p, []byte("restoring "), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-restoredTmpMaxAge - time.Minute)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	if name, _ := Restored(root); name != "" {
		t.Fatalf("a temporary file was named as a result: %q", name)
	}
	keepNewest(t, root, 1)
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("an abandoned temporary mark file older than %s stayed: %v", restoredTmpMaxAge, err)
	}
	if _, err := os.Stat(young); err != nil {
		t.Fatalf("a young temporary mark file (a write in progress) was removed: %v", err)
	}
	// A dry run removes nothing.
	if err := os.Chtimes(young, old, old); err != nil {
		t.Fatal(err)
	}
	Restored(root)
	if _, err := os.Stat(young); err != nil {
		t.Fatalf("a dry run removed a temporary file: %v", err)
	}
}
