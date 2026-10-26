package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// snapDir makes a local snapshot folder under root, with _SUCCESS when done.
func snapDir(t *testing.T, root, name string, done bool) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, name), 0o755); err != nil {
		t.Fatal(err)
	}
	if done {
		if err := os.WriteFile(filepath.Join(root, name, baseline.SuccessMarker), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// After each cleanup, the snapshots the destination answered it does not
// have are sent again, one at a time, at most resendPerCycle per folder; a
// failure does not stop the rest; one without its local _SUCCESS (an older
// build's) is never tried; a target with no destination sends nothing; and
// the folder's record is rewritten to what is still missing.
func TestPruneCycleResendsWhatTheDestinationLacks(t *testing.T) {
	d1, d2 := t.TempDir(), t.TempDir()
	for _, n := range []string{"a", "b", "c", "d"} {
		snapDir(t, d1, n, true)
	}
	snapDir(t, d1, "legacy", false)
	type call struct{ dir, s3, name string }
	var calls []call
	prev := resendSnapshotFn
	t.Cleanup(func() { resendSnapshotFn = prev })
	resendSnapshotFn = func(_ context.Context, dir, s3, name string) error {
		calls = append(calls, call{dir, s3, name})
		if name == "a" {
			return errors.New("network")
		}
		return nil
	}
	// Even a result that names snapshots for a target with no destination
	// sends nothing there: there is nowhere to send them.
	prune := func(_ context.Context, o baseline.PruneOptions) (baseline.PruneResult, error) {
		return baseline.PruneResult{KeptNotDurable: 5, NotAtDestination: []string{"legacy", "a", "b", "c", "d"}}, nil
	}
	runBaselinePruneCycle(context.Background(), []baselinePruneTarget{
		{dir: d1, s3: "s3://bkt/p1/"},
		{dir: d2, keepNewest: 3},
	}, 4*60*60*1e9, prune)
	want := []call{{d1, "s3://bkt/p1/", "a"}, {d1, "s3://bkt/p1/", "b"}, {d1, "s3://bkt/p1/", "c"}}
	if len(calls) != len(want) {
		t.Fatalf("resend calls %+v, want %+v (legacy skipped, at most %d)", calls, want, resendPerCycle)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("resend calls %+v, want %+v", calls, want)
		}
	}
	rec, ok, err := baseline.ReadNotInDestination(d1)
	if err != nil || !ok || rec.Count != 3 || !rec.Resending || strings.Join(rec.Names, ",") != "legacy,a,d" {
		t.Fatalf("record after the resends: %+v ok=%v err=%v; want 3 left (legacy, a, d), resending", rec, ok, err)
	}
}

// A folder two targets share with different destinations sends nothing:
// which one its snapshots belong to cannot be told.
func TestPruneCycleDoesNotResendFromASharedFolder(t *testing.T) {
	d := t.TempDir()
	snapDir(t, d, "a", true)
	var calls int
	prev := resendSnapshotFn
	t.Cleanup(func() { resendSnapshotFn = prev })
	resendSnapshotFn = func(context.Context, string, string, string) error { calls++; return nil }
	prune := func(context.Context, baseline.PruneOptions) (baseline.PruneResult, error) {
		return baseline.PruneResult{KeptNotDurable: 1, NotAtDestination: []string{"a"}}, nil
	}
	runBaselinePruneCycle(context.Background(), []baselinePruneTarget{
		{dir: d, s3: "s3://bkt/global/"}, {dir: d, s3: "s3://bkt/srvA"},
	}, 4*60*60*1e9, prune)
	if calls != 0 {
		t.Fatalf("%d resends from a folder with two destinations", calls)
	}
}

// The resend sends exactly that snapshot to its own key, skipping objects
// already there, and refuses one without its _SUCCESS marker.
func TestResendLackingSendsTheSnapshotToItsOwnKey(t *testing.T) {
	root := t.TempDir()
	good := filepath.Join(root, "2026-10-19T03-15-06Z")
	if err := os.MkdirAll(good, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(good, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "2026-10-02T17-00-41Z"), 0o755); err != nil {
		t.Fatal(err)
	}
	type up struct {
		dir, dest string
		skip      bool
	}
	var ups []up
	prev := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = prev })
	uploadSnapshot = func(_ context.Context, dir, dest, _ string, skip bool) (int, error) {
		ups = append(ups, up{dir, dest, skip})
		return 3, nil
	}
	s := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	if err := s.resendLacking(context.Background(), root, "s3://bkt/baselinesabirds/", "2026-10-19T03-15-06Z"); err != nil {
		t.Fatal(err)
	}
	if len(ups) != 1 || ups[0] != (up{good, "s3://bkt/baselinesabirds/2026-10-19T03-15-06Z", true}) {
		t.Fatalf("uploads %+v", ups)
	}
	if err := s.resendLacking(context.Background(), root, "s3://bkt/baselinesabirds/", "2026-10-02T17-00-41Z"); err == nil || len(ups) != 1 {
		t.Fatalf("a snapshot without _SUCCESS was sent (err %v, uploads %d)", err, len(ups))
	}
}
