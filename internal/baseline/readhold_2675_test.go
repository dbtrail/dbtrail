//go:build unix

package baseline

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

// #2675 point 2: a reader of ANY process holds the one snapshot it reads
// (HoldSnapshot), and the prune keeps it. Real files and, for the claims
// about another process, a real child process: what is left on disk is the
// proof.

// threeSnapshots writes three complete snapshots of one table, 30, 20 and
// 10 minutes old, and returns their names oldest first. A count of 1 keeps
// only the newest.
func threeSnapshots(t *testing.T, root string) []string {
	t.Helper()
	return minutes(t, root, 30, 20, 10)
}

// A held snapshot past the count stays and is counted as pinned; the other
// one past the count goes. Released, the next prune removes it.
func TestHoldSnapshot_keepsTheSnapshotItReads(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	release, gone := HoldSnapshot(filepath.Join(root, s[0]))
	if gone {
		t.Fatal("a snapshot on disk reads as gone")
	}
	res := keepNewest(t, root, 1)
	if !slices.Equal(res.Pruned, []string{s[1]}) || res.KeptPinned != 1 {
		t.Fatalf("pruned %v, kept pinned %d: want [%s] and 1", res.Pruned, res.KeptPinned, s[1])
	}
	wantOnDisk(t, root, s[0], s[2])
	release()
	release() // twice is safe
	if res := keepNewest(t, root, 1); !slices.Equal(res.Pruned, []string{s[0]}) {
		t.Fatalf("after the release the prune removed %v, want [%s]", res.Pruned, s[0])
	}
	wantOnDisk(t, root, s[2])
}

// Two readers on one snapshot: it stays until the last one ends.
func TestHoldSnapshot_twoReadersOneSnapshot(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	one, _ := HoldSnapshot(filepath.Join(root, s[0]))
	two, _ := HoldSnapshot(filepath.Join(root, s[0]))
	one()
	if res := keepNewest(t, root, 1); res.KeptPinned != 1 {
		t.Fatalf("with one reader left, kept pinned %d, want 1 (pruned %v)", res.KeptPinned, res.Pruned)
	}
	two()
	if res := keepNewest(t, root, 1); !slices.Equal(res.Pruned, []string{s[0]}) {
		t.Fatalf("with no reader left the prune removed %v, want [%s]", res.Pruned, s[0])
	}
}

// What cannot be held is read as before: nothing to hold, nothing gone.
func TestHoldSnapshot_whatCannotBeHeld(t *testing.T) {
	root := t.TempDir()
	legacy := snapName(15 * time.Minute)
	if err := os.MkdirAll(filepath.Join(root, legacy, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ name, dir string }{
		{"empty", ""},
		{"s3", "s3://bucket/prefix/" + legacy},
		{"no marker (a snapshot from before markers)", filepath.Join(root, legacy)},
	} {
		release, gone := HoldSnapshot(c.dir)
		if gone || release == nil {
			t.Errorf("%s: gone %v, release nil %v", c.name, gone, release == nil)
		}
		release()
	}
	if _, gone := HoldSnapshot(filepath.Join(root, "2020-01-01T00-00-00Z")); !gone {
		t.Error("a snapshot folder that is not there does not read as gone")
	}
	if release, gone := HoldSnapshotOfTable("s3://b/p/" + legacy + "/shop/orders.parquet"); gone || release == nil {
		t.Error("an s3:// table path is held or gone")
	}
}

// A reader with no write permission on the folder (an operator's command run
// as another user) still holds the snapshot: the hold opens the marker to
// read it, nothing else.
func TestHoldSnapshot_aReadOnlyFolderIsHeld(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	dir := filepath.Join(root, s[0])
	if err := os.Chmod(filepath.Join(dir, SuccessMarker), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	release, gone := HoldSnapshot(dir)
	if gone {
		t.Fatal("a read-only snapshot reads as gone")
	}
	defer release()
	if err := os.Chmod(dir, 0o755); err != nil { // the prune may move it
		t.Fatal(err)
	}
	if res := keepNewest(t, root, 1); res.KeptPinned != 1 {
		t.Fatalf("a read-only hold is not honoured: kept pinned %d, pruned %v", res.KeptPinned, res.Pruned)
	}
}

// The prune moved the folder aside between the reader opening the marker and
// the reader getting the lock: the reader learns its snapshot is gone, and
// holds nothing.
func TestHoldSnapshot_movedAsideWhileTakingTheLock(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	dir := filepath.Join(root, s[0])
	afterReadHoldLock = func() {
		if err := os.Rename(dir, filepath.Join(root, "."+s[0]+pruningSuffix)); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { afterReadHoldLock = nil })
	release, gone := HoldSnapshot(dir)
	if !gone {
		t.Error("a snapshot moved aside under the reader does not read as gone")
	}
	release()
	// The lock was given back: the moved marker can be taken exclusively.
	f, err := os.Open(filepath.Join(root, "."+s[0]+pruningSuffix, SuccessMarker))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Errorf("the reader kept its lock on a snapshot it reported gone: %v", err)
	}
}

// The prune does not hold the marker through the delete: a reader waiting
// for the lock gets it as soon as the folder has moved.
func TestLockForRemoval_releasedAfterTheRename(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	var during error
	prev := removeAll
	removeAll = func(p string) error {
		f, err := os.Open(filepath.Join(p, SuccessMarker))
		if err == nil {
			during = syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
			f.Close()
		}
		return prev(p)
	}
	t.Cleanup(func() { removeAll = prev })
	keepNewest(t, root, 1)
	if during != nil {
		t.Errorf("the marker was still locked while the folder was deleted: %v", during)
	}
	wantOnDisk(t, root, s[2])
}

// holdEnv names the snapshot a child process holds (TestHoldSnapshot_child).
const holdEnv = "BINTRAIL_TEST_HOLD_SNAPSHOT"

// TestHoldSnapshot_child is not a test on its own: run by the tests below as
// a separate process, it holds the snapshot named in holdEnv, says so on
// stdout, and waits to be killed.
func TestHoldSnapshot_child(t *testing.T) {
	dir := os.Getenv(holdEnv)
	if dir == "" {
		t.Skip("run as a child process only")
	}
	if _, gone := HoldSnapshot(dir); gone {
		os.Stdout.WriteString("gone\n")
		os.Exit(3)
	}
	os.Stdout.WriteString("held\n")
	time.Sleep(time.Minute)
	os.Exit(0)
}

// A reader in ANOTHER process holds its snapshot, and a reader that dies
// (killed, so nothing of its own runs) holds nothing: the next prune removes
// the snapshot, with nothing left to sweep.
func TestHoldSnapshot_anotherProcessAndItsDeath(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	cmd := exec.Command(os.Args[0], "-test.run=^TestHoldSnapshot_child$", "-test.count=1")
	cmd.Env = append(os.Environ(), holdEnv+"="+filepath.Join(root, s[0]))
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil || line != "held\n" {
		t.Fatalf("the child said %q (%v), want held", line, err)
	}
	res := keepNewest(t, root, 1)
	if !slices.Equal(res.Pruned, []string{s[1]}) || res.KeptPinned != 1 {
		t.Fatalf("with another process on %s: pruned %v, kept pinned %d", s[0], res.Pruned, res.KeptPinned)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if res := keepNewest(t, root, 1); !slices.Equal(res.Pruned, []string{s[0]}) {
		t.Fatalf("after the reader was killed the prune removed %v, want [%s]", res.Pruned, s[0])
	}
	wantOnDisk(t, root, s[2])
	if left, _ := filepath.Glob(filepath.Join(root, holdRecordPrefix+"*")); len(left) != 0 {
		t.Errorf("records of the dead reader left beside the snapshots: %v", left)
	}
}
