package reconstruct

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func writeFindFixture(t *testing.T, dir string, parts ...string) {
	t.Helper()
	p := filepath.Join(append([]string{dir}, parts...)...)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

// captureWarns routes the default slog logger into a buffer for the test.
// Process-global (slog.SetDefault) — do not t.Parallel() tests using it.
func captureWarns(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// TestFindBaselineLocal_staleSnapshotWarns pins the #461 staleness signal:
// when the requested table is absent from the newest eligible snapshot and an
// older one is used, findBaselineLocal must warn — and must NOT warn when the
// chosen snapshot IS the newest. It also pins the selection invariants the
// warn depends on: newest-eligible wins, and snapshots after `at` are
// excluded from both selection and the newest-snapshot comparison.
func TestFindBaselineLocal_staleSnapshotWarns(t *testing.T) {
	dir := t.TempDir()
	writeFindFixture(t, dir, "2026-01-01T00-00-00Z", "shop", "orders.parquet")
	writeFindFixture(t, dir, "2026-02-01T00-00-00Z", "shop", "users.parquet")  // newest eligible, NO orders
	writeFindFixture(t, dir, "2026-03-01T00-00-00Z", "shop", "orders.parquet") // after `at` — must be invisible

	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)

	buf := captureWarns(t)
	path, ts, stale, err := findBaselineLocal(dir, "shop", "orders", at)
	if err != nil {
		t.Fatal(err)
	}
	wantTS := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !ts.Equal(wantTS) || filepath.Base(filepath.Dir(filepath.Dir(path))) != "2026-01-01T00-00-00Z" {
		t.Fatalf("selected %s (%s), want the 2026-01-01 snapshot (newer eligible snapshot lacks the table; 2026-03-01 is after `at`)", path, ts)
	}
	if !bytes.Contains(buf.Bytes(), []byte("absent from the newest snapshot")) {
		t.Fatalf("want the staleness warn, got log output: %q", buf.String())
	}
	// The returned StaleWarning is the in-band signal callers surface (#466).
	if !stale.Stale() {
		t.Fatalf("want a non-empty StaleWarning on fallback, got %+v", stale)
	}
	if !stale.NewestSnapshot.Equal(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)) || !stale.UsingSnapshot.Equal(wantTS) {
		t.Fatalf("StaleWarning snapshots = using %s / newest %s, want 2026-01-01 / 2026-02-01", stale.UsingSnapshot, stale.NewestSnapshot)
	}

	// Chosen snapshot IS the newest eligible → no warn, no StaleWarning.
	buf2 := captureWarns(t)
	at2 := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	_, ts2, stale2, err := findBaselineLocal(dir, "shop", "orders", at2)
	if err != nil || !ts2.Equal(wantTS) {
		t.Fatalf("selection at %s: ts=%s err=%v", at2, ts2, err)
	}
	if stale2.Stale() {
		t.Fatalf("spurious StaleWarning when the chosen snapshot is the newest eligible: %+v", stale2)
	}
	if bytes.Contains(buf2.Bytes(), []byte("absent from the newest snapshot")) {
		t.Fatalf("spurious staleness warn when the chosen snapshot is the newest eligible: %q", buf2.String())
	}
}

// TestFindBaselineLocal_skipsIncompleteSnapshot pins #467 on the local lookup:
// a snapshot flagged _INCOMPLETE is invisible to selection, so a newer partial
// snapshot can never shadow an older complete one (and a complete table inside
// the partial dir is NOT returned).
func TestFindBaselineLocal_skipsIncompleteSnapshot(t *testing.T) {
	dir := t.TempDir()
	// Older COMPLETE snapshot with the table.
	writeFindFixture(t, dir, "2026-01-01T00-00-00Z", "shop", "orders.parquet")
	if err := baseline.WriteSuccessMarker(filepath.Join(dir, "2026-01-01T00-00-00Z")); err != nil {
		t.Fatal(err)
	}
	// Newer INCOMPLETE snapshot that also has orders.parquet — must be ignored.
	writeFindFixture(t, dir, "2026-02-01T00-00-00Z", "shop", "orders.parquet")
	if err := baseline.WriteIncompleteMarker(filepath.Join(dir, "2026-02-01T00-00-00Z")); err != nil {
		t.Fatal(err)
	}

	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	path, ts, stale, err := findBaselineLocal(dir, "shop", "orders", at)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC); !ts.Equal(want) {
		t.Fatalf("selected snapshot %s (%s), want the 2026-01-01 complete one (newer is _INCOMPLETE)", path, ts)
	}
	// The incomplete snapshot is excluded from the newest-eligible comparison,
	// so the complete one is the newest → not stale.
	if stale.Stale() {
		t.Fatalf("an _INCOMPLETE newer snapshot must not count as a stale-fallback target: %+v", stale)
	}
}

// The three tests below run the glob lookup's helpers, which since #1740 are
// the REFERENCE the s3:// lookup is compared with (baseline_s3globs_test.go),
// not what it runs: they keep the reference honest. What the lookup itself
// does on a failed read is pinned by
// TestFindBaselineS3_aFailedListingIsNotAnAnswer.
//
// TestS3IncompleteSnapshots exercises the marker-detection logic of the glob
// lookup against real DuckDB glob() on a local path (glob() works on local
// paths; httpfs is only needed for the s3:// scheme, loaded by the caller).
// _INCOMPLETE-without-_SUCCESS → incomplete; _SUCCESS present (even alongside a
// stale _INCOMPLETE) → complete; marker-absent (legacy) → complete.
func TestS3IncompleteSnapshots(t *testing.T) {
	dir := t.TempDir()
	mk := func(ts string, markers ...string) {
		sub := filepath.Join(dir, ts)
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, m := range markers {
			if err := os.WriteFile(filepath.Join(sub, m), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	mk("2026-01-01T00-00-00Z", baseline.SuccessMarker)                            // complete
	mk("2026-02-01T00-00-00Z", baseline.IncompleteMarker)                         // incomplete
	mk("2026-03-01T00-00-00Z", baseline.SuccessMarker, baseline.IncompleteMarker) // success wins
	mk("2026-04-01T00-00-00Z")                                                    // legacy, no marker

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got, err := s3IncompleteSnapshots(context.Background(), db, filepath.ToSlash(dir))
	if err != nil {
		t.Fatalf("s3IncompleteSnapshots: %v", err)
	}
	incompleteKey := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	if !got[incompleteKey] {
		t.Errorf("the _INCOMPLETE-only snapshot must be flagged incomplete: %v", got)
	}
	for _, ts := range []time.Time{
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
	} {
		if got[ts.Format(time.RFC3339)] {
			t.Errorf("snapshot %s must NOT be flagged incomplete: %v", ts, got)
		}
	}
	if len(got) != 1 {
		t.Errorf("exactly one incomplete snapshot expected, got %d: %v", len(got), got)
	}
}

// TestS3IncompleteSnapshots_errorSurfaces pins the #524 LOW fix's contract: the
// marker filter is a CORRECTNESS filter, so an error reading the markers must
// surface (never silently treat all snapshots as complete-by-default). The
// per-row Scan-error branch (continue→return) is defensive — glob() returns one
// VARCHAR, so a Scan can't be made to fail with real DuckDB — but the loud-fail
// contract is observable here via a closed *sql.DB, which makes the marker query
// fail. Mirrors the hardened listBaselinesS3 Scan branch.
func TestS3IncompleteSnapshots_errorSurfaces(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	db.Close() // closed DB → the marker QueryContext fails

	if _, err := s3IncompleteSnapshots(context.Background(), db, "s3://bucket/prefix"); err == nil {
		t.Fatal("s3IncompleteSnapshots must return an error when the marker glob/scan fails, not silently treat all snapshots as complete")
	}
}

// TestS3NewestSnapshot exercises the broad newest-snapshot derivation used by
// the S3 lookup (#466) against real DuckDB glob() on a local path: it returns
// the newest COMPLETE snapshot at-or-before `at`, ignoring snapshots after `at`
// and any in the incomplete set.
func TestS3NewestSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeFindFixture(t, dir, "2026-01-01T00-00-00Z", "shop", "orders.parquet")
	writeFindFixture(t, dir, "2026-02-01T00-00-00Z", "shop", "users.parquet")  // newest eligible, no orders
	writeFindFixture(t, dir, "2026-03-01T00-00-00Z", "shop", "orders.parquet") // after `at`

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	src := filepath.ToSlash(dir)

	// No incomplete set: newest at-or-before `at` is the 2026-02-01 snapshot.
	got, err := s3NewestSnapshot(context.Background(), db, src, at, map[string]bool{})
	if err != nil {
		t.Fatalf("s3NewestSnapshot: %v", err)
	}
	if want := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("newest = %s, want %s (2026-03-01 is after `at`)", got, want)
	}

	// Flag 2026-02-01 incomplete → the next-newest complete one (2026-01-01) wins.
	got, err = s3NewestSnapshot(context.Background(), db, src, at, map[string]bool{
		time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339): true,
	})
	if err != nil {
		t.Fatalf("s3NewestSnapshot (incomplete): %v", err)
	}
	if want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("newest with 2026-02-01 incomplete = %s, want %s", got, want)
	}
}
