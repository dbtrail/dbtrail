package baseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The record of old snapshots the destination does not have (2026-10-01):
// written by a completed attempt against S3, counted without the ones that
// could not be checked, removed when none is missing or the folder has no
// destination any more, and never touched by a dry run or a failed attempt.
func TestNotInDestination_record(t *testing.T) {
	absent := func(context.Context, string) (bool, error) { return false, nil }
	present := func(context.Context, string) (bool, error) { return true, nil }
	old1, old2, young := snapName(days(30)), snapName(days(20)), snapName(days(1))
	setup := func(t *testing.T) string {
		root := t.TempDir()
		for _, n := range []string{old1, old2, young} {
			makeSnapshot(t, root, n, true, "shop/orders")
		}
		return root
	}
	opts := func(root string) PruneOptions {
		return PruneOptions{LocalDir: root, S3URL: "s3://b/p", Retain: days(7), Now: kn}
	}

	t.Run("absent at the destination: counted", func(t *testing.T) {
		root := setup(t)
		res, err := pruneWithProbe(context.Background(), opts(root), absent)
		if err != nil {
			t.Fatal(err)
		}
		rec, ok, err := ReadNotInDestination(root)
		if err != nil || !ok || rec.Count != res.KeptNotDurable || rec.Count == 0 || !rec.At.Equal(kn.UTC()) {
			t.Fatalf("record %+v ok=%v err=%v, kept %d", rec, ok, err, res.KeptNotDurable)
		}
	})

	t.Run("present at the destination: removed", func(t *testing.T) {
		root := setup(t)
		if _, err := pruneWithProbe(context.Background(), opts(root), absent); err != nil {
			t.Fatal(err)
		}
		if _, err := pruneWithProbe(context.Background(), opts(root), present); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := ReadNotInDestination(root); ok || err != nil {
			t.Fatalf("every old snapshot is at the destination: ok=%v err=%v", ok, err)
		}
	})

	t.Run("could not be checked: not counted here", func(t *testing.T) {
		root := setup(t)
		probeErr := func(context.Context, string) (bool, error) { return false, errors.New("throttled") }
		res, _ := pruneWithProbe(context.Background(), opts(root), probeErr)
		if res.ProbeErrors == 0 {
			t.Fatalf("setup: no probe errors in %+v", res)
		}
		if _, ok, err := ReadNotInDestination(root); ok || err != nil {
			t.Fatalf("unchecked snapshots belong to the failure record: ok=%v err=%v", ok, err)
		}
	})

	t.Run("mixed: only the absent ones are counted", func(t *testing.T) {
		root := setup(t)
		mixed := func(_ context.Context, name string) (bool, error) {
			if strings.Contains(name, old1) {
				return false, nil
			}
			return false, errors.New("throttled")
		}
		res, _ := pruneWithProbe(context.Background(), opts(root), mixed)
		if res.ProbeErrors != 1 || res.KeptNotDurable != 2 {
			t.Fatalf("setup: %+v", res)
		}
		if rec, ok, err := ReadNotInDestination(root); !ok || err != nil || rec.Count != 1 {
			t.Fatalf("record %+v ok=%v err=%v, want the one S3 answered absent", rec, ok, err)
		}
	})

	t.Run("an attempt that failed leaves it", func(t *testing.T) {
		root := setup(t)
		if _, err := pruneWithProbe(context.Background(), opts(root), absent); err != nil {
			t.Fatal(err)
		}
		// No destination and no count: pruneAttempt refuses with an error,
		// and an errored attempt's counts are partial, so the record stays.
		if _, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, Now: kn}, nil); err == nil {
			t.Fatal("setup: the attempt did not fail")
		}
		if _, ok, _ := ReadNotInDestination(root); !ok {
			t.Fatal("a failed attempt removed the record")
		}
	})

	t.Run("a record that cannot be written does not fail the prune", func(t *testing.T) {
		root := setup(t)
		was := writeNotInDestRecord
		t.Cleanup(func() { writeNotInDestRecord = was })
		writeNotInDestRecord = func(string, NotInDestination) error { return errors.New("disk full") }
		res, err := pruneWithProbe(context.Background(), opts(root), absent)
		if err != nil || res.KeptNotDurable == 0 {
			t.Fatalf("res %+v err %v", res, err)
		}
		if _, ok, _ := ReadNotInDestination(root); ok {
			t.Fatal("a record appeared although the write failed")
		}
	})

	t.Run("dry run leaves it", func(t *testing.T) {
		root := setup(t)
		if _, err := pruneWithProbe(context.Background(), opts(root), absent); err != nil {
			t.Fatal(err)
		}
		dry := opts(root)
		dry.DryRun = true
		if _, err := pruneWithProbe(context.Background(), dry, present); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := ReadNotInDestination(root); !ok {
			t.Fatal("a dry run removed the record")
		}
	})

	t.Run("no destination any more: removed", func(t *testing.T) {
		root := setup(t)
		if _, err := pruneWithProbe(context.Background(), opts(root), absent); err != nil {
			t.Fatal(err)
		}
		if _, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, KeepNewest: 5, Now: kn}, nil); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := ReadNotInDestination(root); ok || err != nil {
			t.Fatalf("a folder with no destination keeps no such record: ok=%v err=%v", ok, err)
		}
	})

	t.Run("corrupt or empty record is an error", func(t *testing.T) {
		for _, body := range []string{"{", `{"at":"2026-01-01T00:00:00Z","count":0}`, `{"count":3}`} {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, NotInDestinationFile), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := ReadNotInDestination(root); ok || err == nil {
				t.Errorf("record %q: ok=%v err=%v, want an error", body, ok, err)
			}
		}
		if _, ok, err := ReadNotInDestination(filepath.Join(t.TempDir(), "missing")); ok || err != nil {
			t.Fatalf("a missing folder has no record: ok=%v err=%v", ok, err)
		}
	})

	t.Run("bookkeeping, not snapshot data", func(t *testing.T) {
		root := t.TempDir()
		for _, name := range []string{NotInDestinationFile, NotInDestinationFile + ".tmp-123"} {
			if !isPruneArtifact(root, filepath.Join(root, name)) {
				t.Errorf("%s would be uploaded as snapshot data", name)
			}
		}
	})
}
