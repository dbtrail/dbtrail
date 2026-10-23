package baselineintegrity

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #2517: ValidateLocalFileOnce reads a file once per process per identity
// (device + inode + size + mtime). Every test here pins one bullet of that
// contract; the reads are counted through the crcFile seam.

const skippedMsg = "integrity check skipped: these bytes were verified earlier by this process"
const readMsg = "integrity check read the whole file and verified it"

// countReads replaces the crcFile seam with a counting wrapper for the test.
func countReads(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	prev := crcFile
	crcFile = func(path string) (string, error) {
		n.Add(1)
		return prev(path)
	}
	t.Cleanup(func() { crcFile = prev })
	return &n
}

// forgetOnExit starts the test with an empty memo and leaves it empty.
func forgetOnExit(t *testing.T) {
	t.Helper()
	ForgetVerified()
	t.Cleanup(ForgetVerified)
}

// captureLog routes slog to a buffer at Debug level for the test. Process-global:
// no t.Parallel() in a test that uses it.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// snapshotWithFile builds <root>/<name>/db/t.parquet holding data and writes
// the snapshot's manifest over it.
func snapshotWithFile(t *testing.T, root, name string, data []byte) (snap, path string) {
	t.Helper()
	snap = filepath.Join(root, name)
	path = filepath.Join(snap, "db", "t.parquet")
	writeFileT(t, path, data)
	if err := WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	// The writer seeds the memo with what it hashed (F4); these fixtures
	// want the file NEW to the process, so the seed is dropped here.
	ForgetVerified()
	// A file modified less than racyMargin ago is never remembered (coarse
	// timestamps); these fixtures expect a skip after one read, so the file
	// is aged a minute.
	ageFileT(t, path)
	return snap, path
}

// ageFileT sets path's mtime a minute in the past, outside racyMargin.
func ageFileT(t *testing.T, path string) {
	t.Helper()
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
}

func memoLen() int {
	verifiedMu.Lock()
	defer verifiedMu.Unlock()
	return len(verifiedCurrent) + len(verifiedPrevious)
}

// needIdentity skips a test whose contract needs a device/inode: on any
// other platform fileIdentity answers ok=false and every call validates.
func needIdentity(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skipf("no file identity on %s: the memo is never consulted here", runtime.GOOS)
	}
}

// Bullet 1: the same "cannot verify" outcomes as ValidateLocalFile, and none of
// them leaves anything in the memo (there was nothing to verify).
func TestOnce_nothingToVerifyMemoizesNothing(t *testing.T) {
	forgetOnExit(t)
	reads := countReads(t)
	root := t.TempDir()

	// No manifest at all.
	noManifest := filepath.Join(root, "legacy", "db", "t.parquet")
	writeFileT(t, noManifest, []byte("legacy bytes"))
	if err := ValidateLocalFileOnce(noManifest); err != nil {
		t.Errorf("no manifest: want nil, got %v", err)
	}
	// An unreadable manifest: Warn + nil.
	rotted := filepath.Join(root, "rotted", "db", "t.parquet")
	writeFileT(t, rotted, []byte("bytes"))
	writeFileT(t, filepath.Join(root, "rotted", ManifestName), []byte("{not json"))
	if err := ValidateLocalFileOnce(rotted); err != nil {
		t.Errorf("unreadable manifest: want nil, got %v", err)
	}
	// A file the manifest does not list.
	snap, _ := snapshotWithFile(t, root, "listed", []byte("listed bytes"))
	unlisted := filepath.Join(snap, "db", "later.parquet")
	writeFileT(t, unlisted, []byte("added after the manifest"))
	if err := ValidateLocalFileOnce(unlisted); err != nil {
		t.Errorf("unlisted file: want nil, got %v", err)
	}
	// An algorithm this build does not know.
	foreign := filepath.Join(root, "foreign", "db", "t.parquet")
	writeFileT(t, foreign, []byte("bytes"))
	writeFileT(t, filepath.Join(root, "foreign", ManifestName), []byte(`{"version":1,"algo":"sha256","files":{"db/t.parquet":"00"}}`))
	if err := ValidateLocalFileOnce(foreign); err != nil {
		t.Errorf("unknown algo: want nil, got %v", err)
	}

	if n := reads.Load(); n != 0 {
		t.Errorf("read the file %d times with nothing to verify", n)
	}
	if n := memoLen(); n != 0 {
		t.Errorf("memo holds %d entries after calls that verified nothing", n)
	}
}

// Bullet 3 and 4: the first call reads the whole file and says so at Info
// (path, size in MB, ms); the second call over the same identity reads nothing
// and says so at Debug, with the path.
func TestOnce_readsOnceThenSkipsAndLogsBoth(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	logs := captureLog(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", bytes.Repeat([]byte("x"), 3000))

	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if n := reads.Load(); n != 1 {
		t.Fatalf("first call read the file %d times, want 1", n)
	}
	first := logs.String()
	if !strings.Contains(first, `msg="`+readMsg+`"`) {
		t.Errorf("the first call did not log that it read the file:\n%s", first)
	}
	for _, want := range []string{"path=" + path, "size_mb=", "ms="} {
		if !strings.Contains(first, want) {
			t.Errorf("the read line lacks %q:\n%s", want, first)
		}
	}
	logs.Reset()

	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if n := reads.Load(); n != 1 {
		t.Errorf("second call over the same identity read the file again (%d reads in all)", n)
	}
	second := logs.String()
	if !strings.Contains(second, `msg="`+skippedMsg+`"`) || !strings.Contains(second, "path="+path) {
		t.Errorf("the skipped call did not say so with the path:\n%s", second)
	}
	if strings.Contains(second, readMsg) {
		t.Errorf("the skipped call logged a read:\n%s", second)
	}
}

// Bullet 3: a mismatch is the same ErrIntegrity as ValidateLocalFile, and it is
// NOT remembered: the next call reads again (the file may have been repaired).
func TestOnce_mismatchFailsEveryTimeAndMemoizesNothing(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	snap, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("original contents"))
	writeFileT(t, path, []byte("corrupted contents"))

	for i := 1; i <= 2; i++ {
		err := ValidateLocalFileOnce(path)
		if !errors.Is(err, ErrIntegrity) {
			t.Fatalf("call %d: want ErrIntegrity, got %v", i, err)
		}
		if !strings.Contains(err.Error(), path) {
			t.Errorf("call %d: the error does not name the file: %v", i, err)
		}
		if n := reads.Load(); n != int64(i) {
			t.Errorf("after %d failing calls the file was read %d times (a mismatch was memoized)", i, n)
		}
	}
	if n := memoLen(); n != 0 {
		t.Errorf("memo holds %d entries after a mismatch", n)
	}
	// The two functions agree on the error.
	if err := ValidateLocalFile(path); !errors.Is(err, ErrIntegrity) {
		t.Errorf("ValidateLocalFile on the same file: %v", err)
	}
	_ = snap
}

// A listed file that cannot be stat'ed is the non-integrity error
// ValidateLocalFile gives for a file it cannot read.
func TestOnce_missingListedFileIsNotAnIntegrityError(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("bytes"))
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	err := ValidateLocalFileOnce(path)
	if err == nil || errors.Is(err, ErrIntegrity) {
		t.Fatalf("want a read error, got %v", err)
	}
	if !strings.Contains(err.Error(), "read baseline for integrity check") {
		t.Errorf("unexpected error shape: %v", err)
	}
}

// Bullet 4: the same identity under a manifest that disagrees (another
// snapshot lists a different digest for the linked file) reads again and fails.
func TestOnce_sameIdentityDifferentManifestDigestReadsAgain(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	root := t.TempDir()
	_, pathA := snapshotWithFile(t, root, "A", []byte("the bytes"))
	if err := ValidateLocalFileOnce(pathA); err != nil {
		t.Fatal(err)
	}
	pathB := filepath.Join(root, "B", "db", "t.parquet")
	if err := os.MkdirAll(filepath.Dir(pathB), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(pathA, pathB); err != nil {
		t.Skipf("hard links unavailable here: %v", err)
	}
	writeFileT(t, filepath.Join(root, "B", ManifestName), []byte(`{"version":1,"algo":"crc32c","files":{"db/t.parquet":"deadbeef"}}`))

	err := ValidateLocalFileOnce(pathB)
	if !errors.Is(err, ErrIntegrity) {
		t.Fatalf("B's manifest disagrees with the linked bytes: want ErrIntegrity, got %v", err)
	}
	if n := reads.Load(); n != 2 {
		t.Errorf("the file was read %d times, want 2 (the memo matched on identity alone, ignoring the digest)", n)
	}
}

// Bullet 6, the whole point: a file carried forward by hard link is the inode
// this process verified under the previous snapshot, and its new manifest
// carries the same digest (#1717), so the new path reads nothing.
func TestOnce_hardLinkedFileUnderTheNextSnapshotReadsNothing(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	logs := captureLog(t)
	root := t.TempDir()
	snapA, pathA := snapshotWithFile(t, root, "A", []byte("parquet bytes carried forward"))
	if err := ValidateLocalFileOnce(pathA); err != nil {
		t.Fatal(err)
	}
	snapB := filepath.Join(root, "B")
	pathB := filepath.Join(snapB, "db", "t.parquet")
	if err := os.MkdirAll(filepath.Dir(pathB), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(pathA, pathB); err != nil {
		t.Skipf("hard links unavailable here: %v", err)
	}
	st, err := WriteManifestFrom(snapB, []string{snapA})
	if err != nil {
		t.Fatal(err)
	}
	if st.Reused != 1 || st.Hashed != 0 {
		t.Fatalf("fixture: the manifest writer did not reuse the digest: %+v", st)
	}
	logs.Reset()

	if err := ValidateLocalFileOnce(pathB); err != nil {
		t.Fatalf("the linked file under B: %v", err)
	}
	if n := reads.Load(); n != 1 {
		t.Errorf("the linked file was read again under the next snapshot (%d reads in all)", n)
	}
	if !strings.Contains(logs.String(), "path="+pathB) || !strings.Contains(logs.String(), skippedMsg) {
		t.Errorf("the skip under B was not logged with B's path:\n%s", logs.String())
	}
}

// Bullet 5: a new inode at the same path (rewrite by rename) reads again.
func TestOnce_newInodeAtTheSamePathReadsAgain(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	snap, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("the bytes"))
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	// The same bytes under a fresh inode, as a rewrite publishes them.
	tmp := filepath.Join(snap, "db", "t.parquet.tmp")
	writeFileT(t, tmp, []byte("the bytes"))
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatalf("the rewritten file: %v", err)
	}
	if n := reads.Load(); n != 2 {
		t.Errorf("a new inode was not read again (%d reads in all)", n)
	}
	// And a corrupt rewrite is caught, not remembered as the old inode.
	tmp2 := filepath.Join(snap, "db", "t.parquet.tmp")
	writeFileT(t, tmp2, []byte("corrupt!!"))
	if err := os.Rename(tmp2, path); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(path); !errors.Is(err, ErrIntegrity) {
		t.Errorf("a corrupt file under a new inode: want ErrIntegrity, got %v", err)
	}
}

// Bullet 5: a changed size (truncated in place, same inode) reads again.
func TestOnce_changedSizeReadsAgain(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("the bytes of the file"))
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, 5); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(path); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("a truncated file: want ErrIntegrity, got %v", err)
	}
	if n := reads.Load(); n != 2 {
		t.Errorf("the truncated file was not read again (%d reads in all)", n)
	}
}

// Bullet 5: a changed mtime (same inode, same size) reads again, and catches
// the changed bytes under it.
func TestOnce_changedMtimeReadsAgain(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("the bytes"))
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Same size, other bytes, same inode (WriteFile truncates in place), and
	// an mtime set explicitly so the test does not hang on the clock's tick.
	writeFileT(t, path, []byte("THE BYTES"))
	later := info.ModTime().Add(time.Second)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(path); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("same inode and size, newer mtime, other bytes: want ErrIntegrity, got %v", err)
	}
	if n := reads.Load(); n != 2 {
		t.Errorf("a newer mtime was not read again (%d reads in all)", n)
	}
}

// Bullet 5, the deliberate limitation: the same inode, size AND mtime with
// other bytes is skipped. That is the trade-off the identity makes; the
// writers never edit a published file in place (temp name, then rename), so
// this shape does not occur on their own output. Pinned so a change to the
// identity is a decision, not an accident.
func TestOnce_sameIdentityWithChangedBytesIsSkipped_limitation(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("the bytes"))
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	writeFileT(t, path, []byte("THE BYTES"))
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatalf("the identity is unchanged, so the call is expected to skip; got %v", err)
	}
	if n := reads.Load(); n != 1 {
		t.Errorf("read %d times; the identity did not change, so the second call is expected not to read", n)
	}
	// The one-shot check still sees it, which is why the one-shot readers keep it.
	if err := ValidateLocalFile(path); !errors.Is(err, ErrIntegrity) {
		t.Errorf("ValidateLocalFile: want ErrIntegrity, got %v", err)
	}
}

// Bullet 3: a file whose identity changed across the read is not remembered.
// The probe: the seam rewrites the file (other bytes, newer mtime) after
// hashing the good bytes, then the test presents the ORIGINAL identity again
// (mtime set back) — a memo taken from the stat BEFORE the read would match
// it and skip the corrupt bytes; the memo taken after the second stat is empty
// and the next call reads and fails.
func TestOnce_identityChangedAcrossTheReadIsNotMemoized(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("the bytes"))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	prev := crcFile
	calls := 0
	crcFile = func(p string) (string, error) {
		calls++
		got, err := prev(p)
		if calls == 1 {
			writeFileT(t, p, []byte("THE BYTES"))
			later := info.ModTime().Add(time.Second)
			if err := os.Chtimes(p, later, later); err != nil {
				t.Fatal(err)
			}
		}
		return got, err
	}
	t.Cleanup(func() { crcFile = prev })

	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatalf("first call hashed the good bytes: %v", err)
	}
	if n := memoLen(); n != 0 {
		t.Fatalf("memo holds %d entries for a file that changed across the read", n)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(path); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("second call under the original identity: want ErrIntegrity, got %v", err)
	}
	if calls != 2 {
		t.Errorf("the file was hashed %d times, want 2", calls)
	}
}

// Bullet 8: ForgetVerified empties the memo; the next call reads.
func TestOnce_forgetVerifiedReadsAgain(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("the bytes"))
	for range 2 {
		if err := ValidateLocalFileOnce(path); err != nil {
			t.Fatal(err)
		}
	}
	ForgetVerified()
	if n := memoLen(); n != 0 {
		t.Fatalf("memo holds %d entries after ForgetVerified", n)
	}
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	if n := reads.Load(); n != 2 {
		t.Errorf("read %d times, want 2 (once before the forget, once after)", n)
	}
}

// Bullet 7: safe from several goroutines at once (run under -race). Each
// goroutine may read before any memo lands, never more than once each, and the
// memo holds one entry at the end.
func TestOnce_concurrentCallsOnOneFile(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", bytes.Repeat([]byte("y"), 10000))
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers*4)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 4 {
				if err := ValidateLocalFileOnce(path); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent call: %v", err)
	}
	if n := reads.Load(); n < 1 || n > workers {
		t.Errorf("read %d times; want between 1 and %d", n, workers)
	}
	if n := memoLen(); n != 1 {
		t.Errorf("memo holds %d entries, want 1", n)
	}
}
