package baselineintegrity

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #2517 round 2: expiry, two generations, the writer seeding the memo, the
// open-before-skip, the counters.

// setVerifiedTTL sets verifiedTTL for the test; no test depends on the default.
func setVerifiedTTL(t *testing.T, ttl time.Duration) {
	t.Helper()
	prev := verifiedTTL
	verifiedTTL = ttl
	t.Cleanup(func() { verifiedTTL = prev })
}

// F1: an entry older than verifiedTTL is read again, then remembered again;
// a TTL of 0 never expires while the process lives.
func TestOnce_entryOlderThanTheTTLIsReadAgain(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	setVerifiedTTL(t, time.Millisecond)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("the bytes"))
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	if n := reads.Load(); n != 2 {
		t.Fatalf("an entry past its TTL was not read again (%d reads, want 2)", n)
	}
	// Re-remembered: a call right after the re-read skips (the jitter is at
	// most TTL/10, far below the 5 ms the sleep above spent).
	setVerifiedTTL(t, time.Hour)
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	if n := reads.Load(); n != 3 {
		t.Errorf("a re-read entry was not remembered again (%d reads, want 3)", n)
	}
}

func TestOnce_zeroTTLNeverExpires(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	setVerifiedTTL(t, 0)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("the bytes"))
	for range 3 {
		if err := ValidateLocalFileOnce(path); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if n := reads.Load(); n != 1 {
		t.Errorf("TTL 0 re-read the file (%d reads, want 1)", n)
	}
}

// F1: the deadline is the TTL plus a random extra of up to a tenth of it,
// so the daemon does not re-read every base in one refresh; 0 is "never".
func TestExpiryFor_spreadsWithinATenthOfTheTTL(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const ttl = 100 * time.Hour
	var min, max time.Duration = ttl * 2, 0
	for range 500 {
		d := expiryFor(now, ttl).Sub(now)
		if d < ttl || d > ttl+ttl/10 {
			t.Fatalf("deadline %v outside [%v, %v]", d, ttl, ttl+ttl/10)
		}
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}
	if min == max {
		t.Errorf("500 deadlines all equal (%v): no spread", min)
	}
	if !expiryFor(now, 0).IsZero() {
		t.Errorf("TTL 0 gave a deadline; want the zero time (never)")
	}
}

// F3: two generations. A live set larger than the cap keeps skipping its hot
// entries: with cap 2 and three files touched round-robin, each file is read
// once and never again, and the rotation says what it kept and dropped.
func TestOnce_twoGenerationsKeepTheHotEntries(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	logs := captureLog(t)
	prevCap := verifiedCap
	verifiedCap = 2
	t.Cleanup(func() { verifiedCap = prevCap })
	root := t.TempDir()
	var paths []string
	for i := range 3 {
		_, p := snapshotWithFile(t, root, fmt.Sprintf("s%d", i), []byte(fmt.Sprintf("bytes %d", i)))
		paths = append(paths, p)
	}
	for round := 1; round <= 3; round++ {
		for _, p := range paths {
			if err := ValidateLocalFileOnce(p); err != nil {
				t.Fatal(err)
			}
		}
		if n := reads.Load(); n != 3 {
			t.Fatalf("after round %d the files were read %d times, want 3: a hot entry was lost", round, n)
		}
	}
	out := logs.String()
	if !strings.Contains(out, `msg="integrity memo rotated`) || !strings.Contains(out, "kept=2") || !strings.Contains(out, "dropped=") {
		t.Errorf("the rotation was not logged with its counts:\n%s", out)
	}
	if n := memoLen(); n > 4 {
		t.Errorf("memo holds %d entries across both generations, more than twice the cap", n)
	}
}

// F3: an entry not seen since the last rotation is gone after the next one.
func TestOnce_coldEntryIsDroppedAfterTwoRotations(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	prevCap := verifiedCap
	verifiedCap = 1
	t.Cleanup(func() { verifiedCap = prevCap })
	root := t.TempDir()
	var paths []string
	for i := range 3 {
		_, p := snapshotWithFile(t, root, fmt.Sprintf("s%d", i), []byte(fmt.Sprintf("bytes %d", i)))
		paths = append(paths, p)
	}
	// cold, then two others: cold moves to previous at the first rotation and
	// is dropped at the second.
	for _, p := range paths {
		if err := ValidateLocalFileOnce(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidateLocalFileOnce(paths[0]); err != nil {
		t.Fatal(err)
	}
	if n := reads.Load(); n != 4 {
		t.Errorf("the cold entry survived two rotations (%d reads, want 4)", n)
	}
}

// F4: the manifest writer just read the bytes it hashed, so it remembers
// them: a fresh snapshot's files are not read again by their first check.
func TestWriteManifest_seedsTheMemo(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	snap := t.TempDir()
	writeFileT(t, filepath.Join(snap, "db", "a.parquet"), []byte("aaaa"))
	writeFileT(t, filepath.Join(snap, "db", "b.posdel"), []byte("bbbb"))
	// Aged past racyMargin: a file written just now is not seeded (round 3).
	ageFileT(t, filepath.Join(snap, "db", "a.parquet"))
	ageFileT(t, filepath.Join(snap, "db", "b.posdel"))
	if err := WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	if n := memoLen(); n != 2 {
		t.Fatalf("the writer remembered %d files, want 2", n)
	}
	for _, f := range []string{"a.parquet", "b.posdel"} {
		if err := ValidateLocalFileOnce(filepath.Join(snap, "db", f)); err != nil {
			t.Fatal(err)
		}
	}
	if n := reads.Load(); n != 0 {
		t.Errorf("the first check read %d files the writer had just hashed", n)
	}
	// And it remembered the bytes it hashed, not a stale identity: a corrupt
	// rewrite under a new inode is still read and refused.
	tmp := filepath.Join(snap, "db", "a.tmp")
	writeFileT(t, tmp, []byte("AAAA"))
	if err := os.Rename(tmp, filepath.Join(snap, "db", "a.parquet")); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(filepath.Join(snap, "db", "a.parquet")); !errors.Is(err, ErrIntegrity) {
		t.Errorf("want ErrIntegrity, got %v", err)
	}
}

// F5: a remembered file that became unreadable fails with the error
// ValidateLocalFile gives, instead of being linked and failing later in
// another reader with another message.
func TestOnce_remberedButUnreadableFileFails(t *testing.T) {
	needIdentity(t)
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-000 file")
	}
	forgetOnExit(t)
	reads := countReads(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("the bytes"))
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(path, 0o644) })
	err := ValidateLocalFileOnce(path)
	if err == nil || errors.Is(err, ErrIntegrity) {
		t.Fatalf("an unreadable remembered file: want a read error, got %v", err)
	}
	if !strings.Contains(err.Error(), "read baseline for integrity check") || !errors.Is(err, os.ErrPermission) {
		t.Errorf("want the permission error ValidateLocalFile gives, got %v", err)
	}
	if n := reads.Load(); n != 1 {
		t.Errorf("the whole file was read %d times, want 1 (the open is the probe, not a read)", n)
	}
}

// F5: a file that cannot be stat'ed after the read is verified but not
// remembered, and says so instead of returning silently.
func TestOnce_unstatableAfterTheReadSaysNotRemembered(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	logs := captureLog(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", []byte("the bytes"))
	prev := crcFile
	crcFile = func(p string) (string, error) {
		got, err := prev(p)
		if rmErr := os.Remove(p); rmErr != nil {
			t.Fatal(rmErr)
		}
		return got, err
	}
	t.Cleanup(func() { crcFile = prev })
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatalf("the bytes read were good: %v", err)
	}
	if n := memoLen(); n != 0 {
		t.Errorf("memo holds %d entries for a file that vanished after the read", n)
	}
	if !strings.Contains(logs.String(), `msg="integrity check verified the file but did not remember it`) {
		t.Errorf("the missing memo was silent:\n%s", logs.String())
	}
}

// F5: Stats counts whole-file reads, skips and the bytes read, so a refresh
// can report the delta of its cycle.
func TestStats_countsReadsSkipsAndBytes(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	data := make([]byte, 1234)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", data)
	r0, s0, b0 := Stats()
	for range 3 {
		if err := ValidateLocalFileOnce(path); err != nil {
			t.Fatal(err)
		}
	}
	r1, s1, b1 := Stats()
	if r1-r0 != 1 || s1-s0 != 2 || b1-b0 != 1234 {
		t.Errorf("delta reads=%d skips=%d bytes=%d, want 1/2/1234", r1-r0, s1-s0, b1-b0)
	}
}

// A read that ends in a mismatch is still a whole-file read: the cycle's
// summary counts it, so "read_files=0" never sits beside a corruption error.
func TestStats_countsAReadThatFailedTheCheck(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	data := make([]byte, 2048)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", data)
	corrupt := make([]byte, 2048)
	corrupt[7] = 1
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}
	r0, _, b0 := Stats()
	if err := ValidateLocalFileOnce(path); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("want ErrIntegrity, got %v", err)
	}
	r1, _, b1 := Stats()
	if r1-r0 != 1 || b1-b0 != 2048 {
		t.Errorf("delta reads=%d bytes=%d, want 1/2048", r1-r0, b1-b0)
	}
}

// F7: the size on the read line has one decimal, so a small file does not
// read as 0.
func TestOnce_readLineSizeHasOneDecimal(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	logs := captureLog(t)
	_, path := snapshotWithFile(t, t.TempDir(), "snap", make([]byte, 300*1024))
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "size_mb=0.3") {
		t.Errorf("want size_mb=0.3:\n%s", logs.String())
	}
}
