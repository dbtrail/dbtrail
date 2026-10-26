package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// After each cleanup, the snapshots the destination answered it does not
// have are sent again, one at a time; a failure does not stop the rest.
func TestPruneCycleResendsWhatTheDestinationLacks(t *testing.T) {
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
		return baseline.PruneResult{KeptNotDurable: 2, NotAtDestination: []string{"a", "b"}}, nil
	}
	runBaselinePruneCycle(context.Background(), []baselinePruneTarget{
		{dir: "/d1", s3: "s3://bkt/p1/"},
		{dir: "/d2", keepNewest: 3},
	}, 4*60*60*1e9, prune)
	if len(calls) != 2 || calls[0] != (call{"/d1", "s3://bkt/p1/", "a"}) || calls[1] != (call{"/d1", "s3://bkt/p1/", "b"}) {
		t.Fatalf("resend calls %+v, want a then b for /d1 only", calls)
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
