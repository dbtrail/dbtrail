package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestEffectiveDuckDBTuning pins the #842 normalization contract: a
// caller-supplied Tuning that is the exact zero value ("not specified" — the
// same convention FullTableConfig already uses for ArchiveFetcher==nil and
// WarnEventThreshold==0) falls back to the container-safe default; any
// non-zero Tuning — including Ultrafast(), which deliberately leaves
// Threads/MemoryLimit unset but sets S3Direct — passes through unchanged.
func TestEffectiveDuckDBTuning(t *testing.T) {
	if got := effectiveDuckDBTuning(duckdbutil.Tuning{}); got != duckdbutil.DefaultTuning() {
		t.Errorf("zero-value Tuning must fall back to DefaultTuning(): got %+v, want %+v", got, duckdbutil.DefaultTuning())
	}
	explicit := duckdbutil.Tuning{Threads: 8, MemoryLimit: "16GB"}
	if got := effectiveDuckDBTuning(explicit); got != explicit {
		t.Errorf("explicit non-zero Tuning must pass through unchanged: got %+v, want %+v", got, explicit)
	}
	if got := effectiveDuckDBTuning(duckdbutil.Ultrafast()); got != duckdbutil.Ultrafast() {
		t.Errorf("Ultrafast() must pass through unchanged (never the zero value): got %+v, want %+v", got, duckdbutil.Ultrafast())
	}
}

// TestMergeBaselineIntoWriter_customDuckDBTuningStillWorks proves the #842
// tuning plumbing (mergeInput.DuckDBTuning → mergeCore.DuckDBTuning →
// mergeBaselineImages' DuckDB session, and readBaselineColumns' own session)
// doesn't break the merge: applying a non-default budget to both DuckDB
// sessions this function opens must produce the exact same output as the
// zero-tuning (container-safe default) path.
func TestMergeBaselineIntoWriter_customDuckDBTuningStillWorks(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
	})
	outDir := t.TempDir()

	rep := &TableReport{Schema: "mydb", Table: "orders"}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           map[string]*query.ResultRow{},
		OutputDir:         outDir,
		ChunkSize:         0,
		DuckDBTuning:      duckdbutil.Tuning{Threads: 1, MemoryLimit: "128MB"},
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter with custom DuckDBTuning: %v", err)
	}
	if rep.BaselineRows != 2 {
		t.Errorf("BaselineRows = %d, want 2", rep.BaselineRows)
	}
	chunk := mustReadOnlyChunk(t, outDir)
	for _, want := range []string{"(1, 'new')", "(2, 'paid')"} {
		if !strings.Contains(chunk, want) {
			t.Errorf("chunk missing %q:\n%s", want, chunk)
		}
	}
}

// TestReconstructTables_incompleteMarkerOnDBConnectFailure covers the #842
// completeness marker against an early refusal. These two tests used a
// DB-connect failure as the stand-in for a mid-run failure, because the
// marker was written before the connection. Since #2684 a dump folder's
// marker is written after every check that can refuse the run before a
// table starts (the index connection among them), so that a run refused up
// front leaves an earlier complete dump complete; the mid-run half (marker
// written before the first table, stale _SUCCESS removed on a reused folder)
// is pinned by TestReconstruct2684_reusedFolderAndIfNotExists, which fails a
// table in a real run, and by TestMarkRunIncomplete_removesStaleSuccess.
// Port 1 on loopback refuses instantly, so this needs no MySQL.
func TestReconstructTables_incompleteMarkerOnDBConnectFailure(t *testing.T) {
	outDir := t.TempDir()
	cfg := FullTableConfig{
		IndexDSN:    "root@tcp(127.0.0.1:1)/idx",
		BaselineSrc: t.TempDir(),
		Tables:      []string{"db.t"},
		OutputDir:   outDir,
	}
	if _, err := ReconstructTables(context.Background(), cfg); err == nil {
		t.Fatal("expected a DB-connect error, got nil")
	}
	if _, statErr := os.Stat(filepath.Join(outDir, baseline.SuccessMarker)); statErr == nil {
		t.Errorf("%s marker must not exist after a failed run", baseline.SuccessMarker)
	}
	if _, statErr := os.Stat(filepath.Join(outDir, baseline.IncompleteMarker)); statErr == nil {
		t.Errorf("%s written by a run refused before any table started", baseline.IncompleteMarker)
	}
}

// TestReconstructTables_staleSuccessMarkerClearedOnReuse: an earlier,
// complete dump in a reused OutputDir stays complete when the next run is
// refused before any table starts, because nothing in it was changed. Before
// #2684 the refusal removed its _SUCCESS and wrote _INCOMPLETE, and
// `bintrail baseline` then refused a dump that was whole.
func TestReconstructTables_staleSuccessMarkerClearedOnReuse(t *testing.T) {
	outDir := t.TempDir()
	if err := baseline.WriteSuccessMarker(outDir); err != nil {
		t.Fatalf("seed _SUCCESS from a prior run: %v", err)
	}

	cfg := FullTableConfig{
		IndexDSN:    "root@tcp(127.0.0.1:1)/idx",
		BaselineSrc: t.TempDir(),
		Tables:      []string{"db.t"},
		OutputDir:   outDir,
	}
	if _, err := ReconstructTables(context.Background(), cfg); err == nil {
		t.Fatal("expected a DB-connect error, got nil")
	}

	if _, statErr := os.Stat(filepath.Join(outDir, baseline.IncompleteMarker)); statErr == nil {
		t.Errorf("%s written by a run refused before any table started", baseline.IncompleteMarker)
	}
	if !baseline.SnapshotComplete(outDir) {
		t.Error("the earlier complete dump no longer reads as complete after a run that changed nothing")
	}
}

// TestMarkRunIncomplete_removesStaleSuccess is the narrow unit test for the
// same regression, directly against the extracted helper.
func TestMarkRunIncomplete_removesStaleSuccess(t *testing.T) {
	dir := t.TempDir()
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatalf("seed stale _SUCCESS: %v", err)
	}
	if err := markRunIncomplete(dir); err != nil {
		t.Fatalf("markRunIncomplete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); !os.IsNotExist(err) {
		t.Errorf("stale %s must be removed, stat err: %v", baseline.SuccessMarker, err)
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.IncompleteMarker)); err != nil {
		t.Errorf("%s must be written: %v", baseline.IncompleteMarker, err)
	}
}

// TestFinalizeCompletenessMarker covers the #842 completeness marker's
// "replaced with _SUCCESS only on a genuinely clean finish" half, directly
// against the extracted decision function (no live index DB needed).
func TestFinalizeCompletenessMarker(t *testing.T) {
	t.Run("clean finish replaces _INCOMPLETE with _SUCCESS", func(t *testing.T) {
		dir := t.TempDir()
		if err := baseline.WriteIncompleteMarker(dir); err != nil {
			t.Fatalf("seed _INCOMPLETE: %v", err)
		}
		if err := finalizeCompletenessMarker(dir, nil, nil); err != nil {
			t.Fatalf("finalizeCompletenessMarker: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
			t.Errorf("%s marker missing after a clean finish: %v", baseline.SuccessMarker, err)
		}
		if _, err := os.Stat(filepath.Join(dir, baseline.IncompleteMarker)); !os.IsNotExist(err) {
			t.Errorf("%s marker should have been removed on clean finish, stat err: %v", baseline.IncompleteMarker, err)
		}
	})

	t.Run("cancelled context leaves _INCOMPLETE in place", func(t *testing.T) {
		dir := t.TempDir()
		if err := baseline.WriteIncompleteMarker(dir); err != nil {
			t.Fatalf("seed _INCOMPLETE: %v", err)
		}
		cancelErr := context.Canceled
		err := finalizeCompletenessMarker(dir, cancelErr, nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected the cancellation error back, got %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); !os.IsNotExist(err) {
			t.Errorf("%s marker must not be written when the run was cancelled", baseline.SuccessMarker)
		}
		if _, err := os.Stat(filepath.Join(dir, baseline.IncompleteMarker)); err != nil {
			t.Errorf("%s marker must remain after cancellation: %v", baseline.IncompleteMarker, err)
		}
	})

	t.Run("per-table errors leave _INCOMPLETE in place and join every error", func(t *testing.T) {
		dir := t.TempDir()
		if err := baseline.WriteIncompleteMarker(dir); err != nil {
			t.Fatalf("seed _INCOMPLETE: %v", err)
		}
		e1 := errors.New("schema.t1: boom")
		e2 := errors.New("schema.t2: kaboom")
		err := finalizeCompletenessMarker(dir, nil, []error{e1, e2})
		if !errors.Is(err, e1) || !errors.Is(err, e2) {
			t.Fatalf("expected both per-table errors joined, got %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); !os.IsNotExist(err) {
			t.Errorf("%s marker must not be written when a table failed", baseline.SuccessMarker)
		}
		if _, err := os.Stat(filepath.Join(dir, baseline.IncompleteMarker)); err != nil {
			t.Errorf("%s marker must remain after a table failure: %v", baseline.IncompleteMarker, err)
		}
	})

	t.Run("cancellation AND a per-table error are both surfaced, not just the cancellation", func(t *testing.T) {
		// Regression guard: a table that genuinely failed before a Ctrl-C
		// landed must not go invisible behind the cancellation error — both
		// must be recoverable off the returned error.
		dir := t.TempDir()
		if err := baseline.WriteIncompleteMarker(dir); err != nil {
			t.Fatalf("seed _INCOMPLETE: %v", err)
		}
		tableErr := errors.New("schema.t1: boom")
		err := finalizeCompletenessMarker(dir, context.Canceled, []error{tableErr})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected the cancellation error recoverable, got %v", err)
		}
		if !errors.Is(err, tableErr) {
			t.Fatalf("expected the per-table error recoverable alongside the cancellation, got %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); !os.IsNotExist(err) {
			t.Errorf("%s marker must not be written", baseline.SuccessMarker)
		}
	})
}

// TestEffectiveDuckDBTuningFor: the zero value takes the job's budget from
// ctx (#2385) before the default; an explicit tuning still wins.
func TestEffectiveDuckDBTuningFor(t *testing.T) {
	b := duckdbutil.Tuning{Threads: 2, MemoryLimit: "512MiB"}
	ctx := duckdbutil.WithSessionTuning(context.Background(), b)
	if got := effectiveDuckDBTuningFor(ctx, duckdbutil.Tuning{}); got != b {
		t.Errorf("zero tuning under a job budget = %+v, want %+v", got, b)
	}
	explicit := duckdbutil.Tuning{Threads: 8, MemoryLimit: "16GB"}
	if got := effectiveDuckDBTuningFor(ctx, explicit); got != explicit {
		t.Errorf("explicit tuning = %+v, want it kept", got)
	}
}
