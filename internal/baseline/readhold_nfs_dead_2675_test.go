//go:build unix

package baseline

import (
	"bufio"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// #2675 point 2, the review of the cross-process hold: a dead reader's age
// is never charged to the reader that comes after it, and a lock that fails
// for any reason but "held" (NFS: an exclusive lock on a file open
// read-only) is said, not taken for "nobody reads it".

// holderChild starts TestHoldSnapshot_child on dir and waits for it to hold.
func holderChild(t *testing.T, dir string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHoldSnapshot_child$", "-test.count=1")
	cmd.Env = append(os.Environ(), holdEnv+"="+dir)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	if line, err := bufio.NewReader(out).ReadString('\n'); err != nil || line != "held\n" {
		t.Fatalf("the child said %q (%v), want held", line, err)
	}
	return cmd
}

func kill(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
}

// The reviewer's case. Reader A has held the snapshot for 20 hours as far
// as the cleanup's clock goes, and is killed; reader B takes the snapshot
// right after, before any cleanup ran. Four hours later B has read for four
// hours, not 24: the snapshot stays.
func TestHoldSnapshot_aNewReaderIsNotChargedWithADeadOnesClock(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	dir := filepath.Join(root, s[0])
	t0 := time.Now()
	t.Cleanup(func() { holdNow = time.Now })
	a := holderChild(t, dir)
	holdNow = func() time.Time { return t0.Add(-20 * time.Hour) }
	if res := keepNewest(t, root, 1); res.KeptPinned != 1 {
		t.Fatalf("A is not honoured: kept pinned %d, pruned %v", res.KeptPinned, res.Pruned)
	}
	kill(t, a)
	holderChild(t, dir)
	holdNow = func() time.Time { return t0.Add(4*time.Hour + time.Second) }
	res := keepNewest(t, root, 1)
	if _, err := os.Stat(dir); err != nil || res.KeptPinned != 1 {
		t.Fatalf("B lost its snapshot to A's clock: pruned %v, kept pinned %d", res.Pruned, res.KeptPinned)
	}
}

// The same with A's own record: it says A started more than a day ago, and
// it is still beside the snapshot (A was killed, nothing removed it) when B
// takes the snapshot. A's record is a dead reader's and goes; B keeps it.
func TestHoldSnapshot_aDeadReadersRecordIsNotInherited(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	dir := filepath.Join(root, s[0])
	a := holderChild(t, dir)
	kill(t, a)
	recs := holdRecordsOf(t, root, s[0])
	if len(recs) != 1 {
		t.Fatalf("records after A was killed: %v, want A's", recs)
	}
	old, _ := json.Marshal(holdRecord{PID: 1, Since: time.Now().Add(-readHoldLimit - time.Hour)})
	if err := os.WriteFile(recs[0], old, 0o600); err != nil {
		t.Fatal(err)
	}
	holderChild(t, dir)
	res := keepNewest(t, root, 1)
	if _, err := os.Stat(dir); err != nil || res.KeptPinned != 1 {
		t.Fatalf("B lost its snapshot to A's record: pruned %v, kept pinned %d", res.Pruned, res.KeptPinned)
	}
	if left := holdRecordsOf(t, root, s[0]); slices.Contains(left, recs[0]) || len(left) != 1 {
		t.Errorf("records left: %v, want B's only", left)
	}
}

// A lock that fails for another reason than "held" (EBADF is what NFS
// answers for an exclusive lock on a file open read-only): the snapshot is
// removed as before, and it is said, once per folder and error, at Warn.
func TestLockForRemoval_aLockErrorIsWarnedOnce(t *testing.T) {
	logs := warnLog(t)
	root := t.TempDir()
	s := minutes(t, root, 40, 30, 20, 10)
	prev := flockFn
	t.Cleanup(func() { flockFn = prev })
	flockFn = func(fd, how int) error {
		if how&syscall.LOCK_EX != 0 {
			return syscall.EBADF
		}
		return prev(fd, how)
	}
	if res := keepNewest(t, root, 3); !slices.Equal(res.Pruned, []string{s[0]}) {
		t.Fatalf("pruned %v, want [%s]", res.Pruned, s[0])
	}
	if res := keepNewest(t, root, 2); !slices.Equal(res.Pruned, []string{s[1]}) {
		t.Fatalf("pruned %v, want [%s]", res.Pruned, s[1])
	}
	out := logs.String()
	t.Logf("log: %s", out)
	const line = "could not lock a snapshot's _SUCCESS before removing it, so a reader in another process may be reading it unprotected"
	if n := strings.Count(out, line); n != 1 || !strings.Contains(out, "level=WARN") || !strings.Contains(out, "opened_for_writing=true") {
		t.Errorf("the lock error is said %d times (want once, at Warn, opened for writing): %s", n, out)
	}
}

// The prune opens _SUCCESS for writing (an exclusive lock on NFS needs it),
// and read-only when that is refused; the lock is taken either way here.
// A _SUCCESS that is a FIFO is not opened for long: the prune does not hang
// on it.
func TestOpenMarker_modeAndKind(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	f, writable, err := openMarker(filepath.Join(root, s[0]), true)
	if err != nil || !writable {
		t.Fatalf("the prune's open: writable %v, err %v", writable, err)
	}
	f.Close()
	if f, writable, _ := openMarker(filepath.Join(root, s[0]), false); writable {
		t.Error("a reader opened the marker for writing")
	} else {
		f.Close()
	}
	if os.Geteuid() != 0 {
		if err := os.Chmod(filepath.Join(root, s[1], SuccessMarker), 0o444); err != nil {
			t.Fatal(err)
		}
		f, writable, err := openMarker(filepath.Join(root, s[1]), true)
		if err != nil || writable {
			t.Fatalf("a read-only marker: writable %v, err %v; want the read-only fallback", writable, err)
		}
		f.Close()
	}
	m := filepath.Join(root, s[0], SuccessMarker)
	if err := os.Remove(m); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(m, 0o644); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		keepNewest(t, root, 1)
		release, _ := HoldSnapshot(filepath.Join(root, s[0]))
		release()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("the prune or a reader hangs on a _SUCCESS that is a FIFO")
		if w, _ := os.OpenFile(m, os.O_WRONLY, 0); w != nil {
			w.Close()
		}
		<-done
	}
}
