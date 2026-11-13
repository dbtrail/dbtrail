//go:build unix

package baseline

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// #2675 point 2, the bounds: a hold of another process stops the cleanup
// for at most readHoldLimit, the reader never waits for long, and the record
// it writes is its own file, 0600, never through a link.

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func warnLog(t *testing.T) *syncBuf {
	t.Helper()
	var buf syncBuf
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func holdRecordsOf(t *testing.T, root, name string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(root, holdRecordPrefix+name+".*"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

const overrideLine = "a reader has held this snapshot for longer than any read is allowed to, so the cleanup no longer waits for it and removes it; that reader will fail"

// A hold whose record says it started more than readHoldLimit ago no longer
// stops the cleanup, and the override is said once.
func TestHoldSnapshot_anExpiredHoldNoLongerStopsTheCleanup(t *testing.T) {
	logs := warnLog(t)
	root := t.TempDir()
	s := threeSnapshots(t, root)
	release, _ := HoldSnapshot(filepath.Join(root, s[0]))
	defer release()
	recs := holdRecordsOf(t, root, s[0])
	if len(recs) != 1 {
		t.Fatalf("records beside the held snapshot: %v, want one", recs)
	}
	b, _ := json.Marshal(holdRecord{PID: 4242, Since: time.Now().Add(-readHoldLimit - time.Hour)})
	if err := os.WriteFile(recs[0], b, 0o600); err != nil {
		t.Fatal(err)
	}
	// Said once: asked twice about the same expired hold.
	for i := 0; i < 2; i++ {
		if _, held := lockForRemoval(filepath.Join(root, s[0])); held {
			t.Fatal("an expired hold still stops the cleanup")
		}
	}
	t.Logf("log: %s", logs.String())
	if n := strings.Count(logs.String(), overrideLine); n != 1 || !strings.Contains(logs.String(), "4242") || !strings.Contains(logs.String(), "snapshot="+s[0]) {
		t.Errorf("the override is said %d times (want 1, naming the snapshot and the holder's pid): %s", n, logs.String())
	}
	b, _ = json.Marshal(holdRecord{PID: 4242, Since: time.Now().Add(-readHoldLimit - time.Hour)})
	_ = os.WriteFile(recs[0], b, 0o600)
	res := keepNewest(t, root, 1)
	if !slices.Contains(res.Pruned, s[0]) || res.KeptPinned != 0 {
		t.Fatalf("pruned %v, kept pinned %d: the expired hold kept %s", res.Pruned, res.KeptPinned, s[0])
	}
	if left := holdRecordsOf(t, root, s[0]); len(left) != 0 {
		t.Errorf("the record of a removed snapshot is left: %v", left)
	}
}

// A hold with no record (a reader that may only read the folder) is bounded
// by the cleanup's own clock: kept now, removed once readHoldLimit has passed
// since the cleanup first found it held.
func TestHoldSnapshot_aHoldWithNoRecordIsBoundedByTheCleanupsClock(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	release, _ := HoldSnapshot(filepath.Join(root, s[0]))
	defer release()
	for _, r := range holdRecordsOf(t, root, s[0]) {
		_ = os.Remove(r)
	}
	start := time.Now()
	holdNow = func() time.Time { return start }
	t.Cleanup(func() { holdNow = time.Now })
	if res := keepNewest(t, root, 1); res.KeptPinned != 1 {
		t.Fatalf("a fresh hold with no record is not honoured: kept pinned %d, pruned %v", res.KeptPinned, res.Pruned)
	}
	holdNow = func() time.Time { return start.Add(readHoldLimit + time.Minute) }
	if res := keepNewest(t, root, 1); !slices.Equal(res.Pruned, []string{s[0]}) {
		t.Fatalf("past the limit the cleanup removed %v, want [%s]", res.Pruned, s[0])
	}
}

// A record rewritten to a later start does not extend the hold: the earlier
// clock decides.
func TestHoldSnapshot_aRewrittenRecordDoesNotExtendTheHold(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	release, _ := HoldSnapshot(filepath.Join(root, s[0]))
	defer release()
	start := time.Now()
	holdNow = func() time.Time { return start }
	t.Cleanup(func() { holdNow = time.Now })
	keepNewest(t, root, 1)
	later := start.Add(readHoldLimit + time.Minute)
	for _, r := range holdRecordsOf(t, root, s[0]) {
		b, _ := json.Marshal(holdRecord{PID: 1, Since: later})
		_ = os.WriteFile(r, b, 0o600)
	}
	holdNow = func() time.Time { return later.Add(time.Second) }
	if res := keepNewest(t, root, 1); !slices.Contains(res.Pruned, s[0]) {
		t.Fatalf("a record moved forward kept the snapshot past the limit: pruned %v", res.Pruned)
	}
}

// A marker someone else holds exclusively does not hang the reader: it
// gives up after readHoldWait, reads unheld, says so, and writes no record.
func TestHoldSnapshot_aLockedMarkerDoesNotHangTheReader(t *testing.T) {
	logs := warnLog(t)
	root := t.TempDir()
	s := threeSnapshots(t, root)
	f, err := os.Open(filepath.Join(root, s[0], SuccessMarker))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	prevWait := readHoldWait
	readHoldWait = 200 * time.Millisecond
	t.Cleanup(func() { readHoldWait = prevWait })
	done := make(chan bool, 1)
	go func() {
		release, gone := HoldSnapshot(filepath.Join(root, s[0]))
		release()
		done <- gone
	}()
	select {
	case gone := <-done:
		if gone {
			t.Error("a locked marker reads as gone")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the reader is still waiting for a marker someone else holds")
	}
	if !strings.Contains(logs.String(), "could not hold this snapshot against the cleanup in time, so it is read without a hold") {
		t.Errorf("the reader did not say it reads unheld: %s", logs.String())
	}
	t.Logf("log: %s", logs.String())
	if r := holdRecordsOf(t, root, s[0]); len(r) != 0 {
		t.Errorf("a reader that holds nothing wrote a record: %v", r)
	}
}

// The record is a new file of the reader's own, 0600, and a marker that is a
// symlink is not followed: neither by the reader nor by the cleanup.
func TestHoldSnapshot_recordModeAndNoLinkFollowed(t *testing.T) {
	root := t.TempDir()
	s := threeSnapshots(t, root)
	release, _ := HoldSnapshot(filepath.Join(root, s[0]))
	recs := holdRecordsOf(t, root, s[0])
	if len(recs) != 1 {
		t.Fatalf("records: %v", recs)
	}
	fi, err := os.Lstat(recs[0])
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 || !fi.Mode().IsRegular() {
		t.Errorf("record mode %v, want a regular file 0600", fi.Mode())
	}
	release()
	if r := holdRecordsOf(t, root, s[0]); len(r) != 0 {
		t.Errorf("the record outlives its release: %v", r)
	}
	// s[1]'s marker becomes a link to s[0]'s: holding s[1] holds nothing,
	// and a hold on s[0] does not protect s[1] through the link.
	linked := filepath.Join(root, s[1], SuccessMarker)
	if err := os.Remove(linked); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, s[0], SuccessMarker), linked); err != nil {
		t.Fatal(err)
	}
	rel, gone := HoldSnapshot(filepath.Join(root, s[1]))
	rel()
	if gone || len(holdRecordsOf(t, root, s[1])) != 0 {
		t.Errorf("a linked marker was held (gone %v)", gone)
	}
	hold0, _ := HoldSnapshot(filepath.Join(root, s[0]))
	defer hold0()
	if _, held := lockForRemoval(filepath.Join(root, s[1])); held {
		t.Error("the cleanup followed a linked marker to another snapshot's hold")
	}
}
