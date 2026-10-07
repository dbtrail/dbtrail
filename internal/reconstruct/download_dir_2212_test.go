package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// #2212: the daemon hands the fold a download folder so an s3:// previous
// snapshot lands on the disk the fold's space check measures. The download
// folder must be made UNDER that folder, and a folder that cannot be used is
// an error naming it, never a quiet fall back to the system temporary
// directory (which is the disk nobody measured).
func TestBaselineDownloadDir_2212(t *testing.T) {
	t.Run("under the given folder", func(t *testing.T) {
		dir := t.TempDir()
		got, err := baselineDownloadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Dir(got) != dir || !strings.HasPrefix(filepath.Base(got), "bintrail-baseline-") {
			t.Fatalf("download folder = %s, want bintrail-baseline-* directly under %s", got, dir)
		}
	})
	t.Run("empty keeps the system temporary directory (the command line)", func(t *testing.T) {
		got, err := baselineDownloadDir("")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(got) })
		if filepath.Dir(got) != filepath.Clean(os.TempDir()) {
			t.Fatalf("download folder = %s, want it under %s", got, os.TempDir())
		}
	})
	t.Run("an unusable folder is an error naming it", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "not-a-dir")
		if err := os.WriteFile(file, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := baselineDownloadDir(file)
		if err == nil || !strings.Contains(err.Error(), file) {
			t.Fatalf("err = %v, want a refusal naming %s", err, file)
		}
		t.Log(err)
	})
}

// #2212 review: the download of an s3:// previous snapshot into DownloadDir is
// checked against free disk BEFORE it starts, sized on the object, so a full
// staging disk refuses the table instead of failing inside DuckDB's COPY.
func TestMaterializeBaselineLocalIn_checksTheDiskBeforeTheDownload(t *testing.T) {
	prevSize, prevValidate := s3ObjectSize, validateS3Baseline
	t.Cleanup(func() { s3ObjectSize, validateS3Baseline = prevSize, prevValidate })
	validateS3Baseline = func(context.Context, string) error { return nil }
	s3ObjectSize = func(_ context.Context, url string) (int64, error) {
		if url != "s3://b/p/2026-08-28T09-00-00Z/shop/orders.parquet" {
			t.Errorf("sized %q", url)
		}
		return 7 << 20, nil
	}
	dir := t.TempDir()
	full := errors.New("disk full")
	var gotDir string
	var gotNeed int64
	check := func(d string, need int64) error { gotDir, gotNeed = d, need; return full }
	_, _, err := materializeBaselineLocalIn(context.Background(), "s3://b/p/2026-08-28T09-00-00Z/shop/orders.parquet", duckdbutil.Tuning{}, dir, check)
	if !errors.Is(err, full) {
		t.Fatalf("err = %v, want the disk refusal", err)
	}
	if gotDir != dir || gotNeed != 7<<20 {
		t.Fatalf("checked (%q, %d), want (%q, %d)", gotDir, gotNeed, dir, 7<<20)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("a refused download left %v", entries)
	}
	// No download folder chosen (the command line): no check, as before.
	prevDL := downloadS3Baseline
	t.Cleanup(func() { downloadS3Baseline = prevDL })
	downloadS3Baseline = func(context.Context, duckdbutil.Tuning, string, string) error { return errors.New("stop") }
	called := false
	s3ObjectSize = func(context.Context, string) (int64, error) { called = true; return 0, nil }
	_, _, _ = materializeBaselineLocalIn(context.Background(), "s3://b/p/2026-08-28T09-00-00Z/shop/orders.parquet", duckdbutil.Tuning{}, "", func(string, int64) error {
		t.Error("the disk was checked with no download folder chosen")
		return nil
	})
	if called {
		t.Error("the object was sized with no download folder chosen")
	}
}

// #2212 review: a full disk under the download is classified HERE, where the
// error is known to come from a local write, never by matching text later: the
// index MySQL reports its own full tmp disk with the same words ("OS errno 28
// - No space left on device"), and that one a full read would cure.
func TestMaterializeBaselineLocalIn_aFullDiskDuringTheDownloadIsLocalDiskFull(t *testing.T) {
	prevSize, prevValidate, prevDL := s3ObjectSize, validateS3Baseline, downloadS3Baseline
	t.Cleanup(func() { s3ObjectSize, validateS3Baseline, downloadS3Baseline = prevSize, prevValidate, prevDL })
	validateS3Baseline = func(context.Context, string) error { return nil }
	s3ObjectSize = func(context.Context, string) (int64, error) { return 1, nil }
	for _, tc := range []struct {
		name string
		err  error
		full bool
	}{
		{"disk full", errors.New(`IO Error: Could not write file "/stage/x/baseline.parquet": No space left on device`), true},
		{"forbidden", errors.New("HTTP Error: 403 Forbidden"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			downloadS3Baseline = func(context.Context, duckdbutil.Tuning, string, string) error { return tc.err }
			dir := t.TempDir()
			_, _, err := materializeBaselineLocalIn(context.Background(), "s3://b/p/2026-08-28T09-00-00Z/shop/orders.parquet",
				duckdbutil.Tuning{}, dir, func(string, int64) error { return nil })
			if err == nil || errors.Is(err, ErrLocalDiskFull) != tc.full || !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want local-disk-full = %v, wrapping the cause", err, tc.full)
			}
			if entries, _ := os.ReadDir(dir); len(entries) != 0 {
				t.Fatalf("a failed download left %v", entries)
			}
		})
	}
}

// The base+delta merge of a compaction writes a temporary file of about the
// base plus its chain into the same folder: it is checked there first.
func TestMaterializeBaseWithDelta_checksTheDiskFirst(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(t.TempDir(), "orders.parquet")
	if err := os.WriteFile(base, make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	full := errors.New("disk full")
	var need int64
	_, _, err := materializeBaseWithDelta(context.Background(), base, &tableDelta{PairSize: 500}, duckdbutil.Tuning{}, dir,
		func(d string, n int64) error {
			if d != dir {
				t.Errorf("checked %q, want %q", d, dir)
			}
			need = n
			return full
		})
	if !errors.Is(err, full) || need != 1500 {
		t.Fatalf("err = %v need = %d, want the refusal sized 1500", err, need)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("a refused merge left %v", entries)
	}
}

// localWriteErr: only the disk-full words of a LOCAL write become
// ErrLocalDiskFull; everything else passes through untouched.
func TestLocalWriteErr(t *testing.T) {
	if err := localWriteErr(nil); err != nil {
		t.Fatal(err)
	}
	full := errors.New("IO Error: No space left on device")
	if got := localWriteErr(full); !errors.Is(got, ErrLocalDiskFull) || !errors.Is(got, full) {
		t.Fatalf("got %v", got)
	}
	other := errors.New("Binder Error")
	if got := localWriteErr(other); got != other {
		t.Fatalf("got %v, want the error unchanged", got)
	}
}
