package archive

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

var (
	tNow      = time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	tOld      = tNow.Add(-24 * time.Hour)
	tModified = time.Date(2026, 6, 5, 9, 30, 0, 0, time.UTC)
)

func nInt(v int64) sql.NullInt64     { return sql.NullInt64{Int64: v, Valid: true} }
func nStr(v string) sql.NullString   { return sql.NullString{String: v, Valid: true} }
func nTime(v time.Time) sql.NullTime { return sql.NullTime{Time: v, Valid: true} }

func localFile(part, id, path string, size, rows int64) ScannedFile {
	return ScannedFile{PartitionName: part, BintrailID: id, Backend: BackendLocal,
		LocalPath: path, SizeBytes: size, RowCount: nInt(rows), LastModified: tModified}
}

func s3File(part, id, bucket, key string, size int64) ScannedFile {
	return ScannedFile{PartitionName: part, BintrailID: id, Backend: BackendS3,
		S3Bucket: bucket, S3Key: key, SizeBytes: size, LastModified: tModified}
}

func changes(a Action) map[string]any {
	m := make(map[string]any, len(a.Changes))
	for _, c := range a.Changes {
		m[c.Column] = c.Value
	}
	return m
}

func bothScanned() DiffOptions {
	return DiffOptions{ScannedLocal: true, ScannedS3: true, PruneMinAge: time.Hour, Now: tNow}
}

// TestDiffInsert covers files-without-rows: the post-index-loss rebuild.
func TestDiffInsert(t *testing.T) {
	files := []ScannedFile{
		localFile("p_2026060510", "abc", "/a/bintrail_id=abc/event_date=2026-06-05/event_hour=10/events.parquet", 100, 42),
		s3File("p_2026060510", "abc", "bkt", "pre/bintrail_id=abc/event_date=2026-06-05/event_hour=10/events.parquet", 100),
	}
	rep := Diff(files, nil, bothScanned())
	if rep.Inserts != 1 || len(rep.Actions) != 1 {
		t.Fatalf("want 1 insert, got %+v", rep)
	}
	ch := changes(rep.Actions[0])
	if ch["local_path"] != files[0].LocalPath || ch["s3_bucket"] != "bkt" || ch["file_size_bytes"] != int64(100) || ch["row_count"] != int64(42) {
		t.Errorf("insert changes incomplete: %v", ch)
	}
	// The drop-block trap: an S3-confirmed insert MUST stamp s3_uploaded_at —
	// rotate's pendingS3Uploads reads bucket-set + stamp-NULL as a pending
	// upload and refuses to drop the partition forever.
	if _, ok := ch["s3_uploaded_at"]; !ok {
		t.Error("insert with confirmed S3 object must stamp s3_uploaded_at (pendingS3Uploads drop-block trap)")
	}
	if err := rep.Err(); err == nil {
		t.Error("drift must make Err() non-nil (cron exit-code contract)")
	}
}

// TestDiffInSync: matching row and file → no action, Err() nil.
func TestDiffInSync(t *testing.T) {
	f := localFile("p_2026060510", "abc", "/a/x.parquet", 100, 42)
	rows := []StateRow{{
		PartitionName: "p_2026060510", BintrailID: "abc",
		LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100), RowCount: nInt(42), ArchivedAt: tOld,
	}}
	rep := Diff([]ScannedFile{f}, rows, bothScanned())
	if len(rep.Actions) != 0 || rep.InSync != 1 {
		t.Fatalf("want clean in-sync report, got %+v", rep)
	}
	if err := rep.Err(); err != nil {
		t.Errorf("in-sync must exit clean, got %v", err)
	}
}

// TestDiffBackendScopedUpdates pins the per-backend repair rules.
func TestDiffBackendScopedUpdates(t *testing.T) {
	t.Run("S3 object present but row lacks it → add columns + stamp", func(t *testing.T) {
		f := s3File("p_2026060510", "abc", "bkt", "k/events.parquet", 100)
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/gone.parquet"), FileSizeBytes: nInt(100), ArchivedAt: tOld}}
		rep := Diff([]ScannedFile{f}, rows, bothScanned())
		if rep.Updates != 1 {
			t.Fatalf("want 1 update, got %+v", rep)
		}
		ch := changes(rep.Actions[0])
		if ch["s3_bucket"] != "bkt" {
			t.Errorf("missing s3 columns: %v", ch)
		}
		// Local was scanned and the registered local file is gone, while
		// the S3 copy holds the data → the stale local_path is cleared.
		// Deliberately scans ZERO local files: this subtest pins the
		// trusted-clear-under-blind-scan tradeoff the #1280 comments cite —
		// do not add a local witness file here.
		if v, ok := ch["local_path"]; !ok || v != nil {
			t.Errorf("stale local_path should be cleared (SET NULL), got %v present=%v", v, ok)
		}
	})

	t.Run("local NOT scanned → stale local_path untouched", func(t *testing.T) {
		f := s3File("p_2026060510", "abc", "bkt", "k/events.parquet", 100)
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/maybe-fine.parquet"), FileSizeBytes: nInt(100),
			S3Bucket: nStr("bkt"), S3Key: nStr("k/events.parquet"), S3UploadedAt: nTime(tModified), ArchivedAt: tOld}}
		rep := Diff([]ScannedFile{f}, rows, DiffOptions{ScannedS3: true, PruneMinAge: time.Hour, Now: tNow})
		for _, a := range rep.Actions {
			if _, touched := changes(a)["local_path"]; touched {
				t.Errorf("local_path must not be touched when local was not scanned: %+v", a)
			}
		}
	})

	t.Run("blind S3 scan → stale S3 columns still cleared (mirror of the local direction)", func(t *testing.T) {
		// Zero S3 files scanned; the local copy for this key is present.
		// Pins the S3 direction of the same deliberate-trust tradeoff.
		f := localFile("p_2026060510", "abc", "/a/x.parquet", 100, 42)
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100),
			S3Bucket: nStr("bkt"), S3Key: nStr("gone/events.parquet"), S3UploadedAt: nTime(tModified), ArchivedAt: tOld}}
		rep := Diff([]ScannedFile{f}, rows, bothScanned())
		if rep.Updates != 1 {
			t.Fatalf("want 1 update, got %+v", rep)
		}
		if v, ok := changes(rep.Actions[0])["s3_bucket"]; !ok || v != nil {
			t.Errorf("stale S3 columns should be cleared under a blind S3 scan, got %v present=%v", v, ok)
		}
	})

	t.Run("missing s3_uploaded_at stamp on confirmed object", func(t *testing.T) {
		f := s3File("p_2026060510", "abc", "bkt", "k/events.parquet", 100)
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			FileSizeBytes: nInt(100),
			S3Bucket:      nStr("bkt"), S3Key: nStr("k/events.parquet"), ArchivedAt: tOld}}
		rep := Diff([]ScannedFile{f}, rows, DiffOptions{ScannedS3: true, PruneMinAge: time.Hour, Now: tNow})
		if rep.Updates != 1 {
			t.Fatalf("want 1 update, got %+v", rep)
		}
		if _, ok := changes(rep.Actions[0])["s3_uploaded_at"]; !ok {
			t.Error("confirmed S3 object with NULL stamp must be stamped")
		}
	})

	t.Run("size drift always; row_count drift only under Deep", func(t *testing.T) {
		f := localFile("p_2026060510", "abc", "/a/x.parquet", 999, 42)
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100), RowCount: nInt(7), ArchivedAt: tOld}}

		shallow := Diff([]ScannedFile{f}, rows, bothScanned())
		if shallow.Updates != 1 {
			t.Fatalf("want size-drift update, got %+v", shallow)
		}
		ch := changes(shallow.Actions[0])
		if ch["file_size_bytes"] != int64(999) {
			t.Errorf("size drift not repaired: %v", ch)
		}
		if _, ok := ch["row_count"]; ok {
			t.Error("row_count drift must be gated behind Deep")
		}

		deepOpts := bothScanned()
		deepOpts.Deep = true
		deep := Diff([]ScannedFile{f}, rows, deepOpts)
		if _, ok := changes(deep.Actions[0])["row_count"]; !ok {
			t.Error("Deep must repair row_count drift")
		}
	})
}

// TestDiffPruneSafety pins the three prune gates: backend coverage
// (partial-scan misclassification — the registry-data-loss trap), the age
// margin, and the genuinely-orphaned happy path.
func TestDiffPruneSafety(t *testing.T) {
	s3OnlyRow := StateRow{PartitionName: "p_2026060510", BintrailID: "abc",
		S3Bucket: nStr("bkt"), S3Key: nStr("k/events.parquet"), S3UploadedAt: nTime(tModified), ArchivedAt: tOld}

	t.Run("partial scan: S3-referenced row, local-only scan → unverified, never pruned", func(t *testing.T) {
		rep := Diff(nil, []StateRow{s3OnlyRow}, DiffOptions{ScannedLocal: true, PruneMinAge: time.Hour, Now: tNow})
		if rep.Prunes != 0 || rep.SkippedUnverified != 1 {
			t.Fatalf("partial scan must yield skip-unverified, got %+v", rep)
		}
	})

	t.Run("full scan with testimony, target's files gone → prune candidate", func(t *testing.T) {
		// An unrelated S3 file proves the scan can see the layout
		// (testimony); the target row's own files are gone → prunable.
		witness := s3File("p_2026060409", "other-id", "bkt", "w/events.parquet", 1)
		rep := Diff([]ScannedFile{witness}, []StateRow{s3OnlyRow}, bothScanned())
		if rep.Prunes != 1 {
			t.Fatalf("want prune candidate, got %+v", rep)
		}
		// Negative marker guard: an evidence-backed prune must never read as
		// vouched, or the report's audit distinction (#1280) is destroyed.
		if strings.Contains(rep.Actions[0].Reason, "trust-empty-scan") {
			t.Errorf("evidence-backed prune must not carry the vouch marker, got: %s", rep.Actions[0].Reason)
		}
	})

	t.Run("evidence-backed prune stays unmarked even with both vouches set", func(t *testing.T) {
		// The marker keys off blindness, not off flag presence — a vouched
		// invocation whose scans DID see files must produce ordinary,
		// unmarked prunes.
		witness := s3File("p_2026060409", "other-id", "bkt", "w/events.parquet", 1)
		opts := bothScanned()
		opts.TrustEmptyLocal, opts.TrustEmptyS3 = true, true
		rep := Diff([]ScannedFile{witness}, []StateRow{s3OnlyRow}, opts)
		if rep.Prunes != 1 {
			t.Fatalf("want prune candidate, got %+v", rep)
		}
		if strings.Contains(rep.Actions[0].Reason, "trust-empty-scan") {
			t.Errorf("marker must key off blindness, not flags, got: %s", rep.Actions[0].Reason)
		}
	})

	t.Run("blind local scanner + local vouch → prune candidate (mirror of the S3 case)", func(t *testing.T) {
		localRow := StateRow{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/data/events.parquet"), ArchivedAt: tOld}
		opts := bothScanned()
		opts.TrustEmptyLocal = true
		rep := Diff(nil, []StateRow{localRow}, opts)
		if rep.Prunes != 1 || rep.SkippedUnverified != 0 {
			t.Fatalf("local vouch must allow the local-row prune, got %+v", rep)
		}
	})

	t.Run("blind scanner: zero files seen in the referenced backend → never pruned", func(t *testing.T) {
		// The scan ran (flag present) but saw NOTHING — wrong directory,
		// or a scanner blind spot. It cannot distinguish "all orphaned"
		// from "I am blind", so pruning on its testimony is forbidden
		// (the registry-wipe trap from the #392 adversarial review).
		rep := Diff(nil, []StateRow{s3OnlyRow}, bothScanned())
		if rep.Prunes != 0 || rep.SkippedUnverified != 1 {
			t.Fatalf("empty-scan prune must be refused, got %+v", rep)
		}
	})

	t.Run("blind scanner + vouched backend: operator vouched for the wipe → prune candidate, marked", func(t *testing.T) {
		// #1280: the legitimate total wipe (e.g. S3 lifecycle expiry of the
		// whole prefix) is indistinguishable from a mistyped path, so the
		// gate needs an explicit override — otherwise the stale rows are
		// unprunable forever and strict queries keep failing.
		opts := bothScanned()
		opts.TrustEmptyS3 = true
		rep := Diff(nil, []StateRow{s3OnlyRow}, opts)
		if rep.Prunes != 1 || rep.SkippedUnverified != 0 {
			t.Fatalf("vouched empty scan must allow the prune, got %+v", rep)
		}
		// The audit trail: a vouched prune must be distinguishable from an
		// evidence-backed one in the report.
		if !strings.Contains(rep.Actions[0].Reason, "trust-empty-scan") {
			t.Errorf("vouched prune must be marked in its reason, got: %s", rep.Actions[0].Reason)
		}
	})

	t.Run("vouch is per-backend: an S3 vouch never disarms a blind LOCAL scan", func(t *testing.T) {
		// The #1280-review trap: real S3 wipe vouched, but --archive-dir
		// points somewhere wrong (its scan is also blind). Local-referencing
		// rows must stay unverified — a global vouch would prune them.
		localRow := StateRow{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/data/events.parquet"), ArchivedAt: tOld}
		opts := bothScanned()
		opts.TrustEmptyS3 = true
		rep := Diff(nil, []StateRow{localRow}, opts)
		if rep.Prunes != 0 || rep.SkippedUnverified != 1 {
			t.Fatalf("S3 vouch must not cover a blind local scan, got %+v", rep)
		}
	})

	t.Run("vouch never bypasses the backend-scoped gate", func(t *testing.T) {
		// The override vouches for a SCANNED backend's emptiness; a row
		// referencing a backend this invocation did not scan at all stays
		// unverified regardless.
		opts := DiffOptions{ScannedLocal: true, PruneMinAge: time.Hour, Now: tNow, TrustEmptyLocal: true, TrustEmptyS3: true}
		rep := Diff(nil, []StateRow{s3OnlyRow}, opts)
		if rep.Prunes != 0 || rep.SkippedUnverified != 1 {
			t.Fatalf("unscanned backend must stay unverified even when vouched, got %+v", rep)
		}
	})

	t.Run("recent row inside the margin → skip-recent", func(t *testing.T) {
		recent := s3OnlyRow
		recent.ArchivedAt = tNow.Add(-10 * time.Minute)
		witness := s3File("p_2026060409", "other-id", "bkt", "w/events.parquet", 1)
		rep := Diff([]ScannedFile{witness}, []StateRow{recent}, bothScanned())
		if rep.Prunes != 0 || rep.SkippedRecent != 1 {
			t.Fatalf("recent row must be skip-recent, got %+v", rep)
		}
	})

	t.Run("row referencing NOTHING (phantom coverage) → prune candidate", func(t *testing.T) {
		phantom := StateRow{PartitionName: "p_2026060511", BintrailID: "abc", ArchivedAt: tOld}
		rep := Diff(nil, []StateRow{phantom}, DiffOptions{ScannedLocal: true, PruneMinAge: time.Hour, Now: tNow})
		// References nothing → the (empty) referenced set is covered by any
		// scan; the row only feeds phantom planner coverage.
		if rep.Prunes != 1 {
			t.Fatalf("phantom row must be a prune candidate, got %+v", rep)
		}
	})
}

// TestDiffDeepUnverified pins the decision-layer deep-verify accounting
// (closes the dual-backend / local-only --deep silent-downgrade): when --deep
// is asked for but the PICKED row_count is Invalid, the pair is counted in
// Report.DeepUnverified even though it produces no diff action (so Err() stays
// nil) — exactly the silent state the cron monitor must still fail on. A pair
// deep-verified via the SURVIVING backend (picked count valid) must NOT be
// counted: that is the over-count false-positive the raw per-backend probe
// counter had.
func TestDiffDeepUnverified(t *testing.T) {
	// localFile/s3File always set a valid RowCount; build the broken-footer
	// variants by hand (zero-value sql.NullInt64 = Invalid).
	const part, id = "p_2026060510", "abc"
	localNoCount := func(path string, size int64) ScannedFile {
		return ScannedFile{PartitionName: part, BintrailID: id, Backend: BackendLocal,
			LocalPath: path, SizeBytes: size, LastModified: tModified}
	}
	s3NoCount := func(bucket, key string, size int64) ScannedFile {
		return ScannedFile{PartitionName: part, BintrailID: id, Backend: BackendS3,
			S3Bucket: bucket, S3Key: key, SizeBytes: size, LastModified: tModified}
	}
	// An in-sync StateRow that matches size + the (S3) row_count, so no diff
	// action is produced — the silent path.
	inSyncRow := func(rowCount int64) []StateRow {
		return []StateRow{{
			PartitionName: part, BintrailID: id,
			LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100), RowCount: nInt(rowCount),
			S3Bucket: nStr("bkt"), S3Key: nStr("k/events.parquet"), S3UploadedAt: nTime(tModified),
			ArchivedAt: tOld,
		}}
	}

	deep := bothScanned()
	deep.Deep = true

	t.Run("dual-backend prefer-local: local footer failed, S3 valid → counted, no action", func(t *testing.T) {
		// pickMeta PREFERS local; its row_count is Invalid → the deep
		// row_count check is skipped, even though the S3 footer read fine.
		s3Verified := s3File(part, id, "bkt", "k/events.parquet", 100)
		s3Verified.RowCount = nInt(42) // S3 footer read fine — but pickMeta won't use it
		files := []ScannedFile{
			localNoCount("/a/x.parquet", 100),
			s3Verified,
		}
		rep := Diff(files, inSyncRow(42), deep)
		if rep.DeepUnverified != 1 {
			t.Fatalf("dual-backend prefer-local footer failure must be deep-unverified, got %+v", rep)
		}
		// The downgrade is SILENT: no action, Err() nil — only DeepUnverified
		// (and the command's dry-run-exit/JSON/WARNING wiring) catches it.
		if len(rep.Actions) != 0 || rep.Err() != nil {
			t.Fatalf("the silent-downgrade state must produce no action and nil Err(), got %+v err=%v", rep, rep.Err())
		}
	})

	t.Run("local-only: footer failed → counted", func(t *testing.T) {
		rep := Diff([]ScannedFile{localNoCount("/a/x.parquet", 100)},
			[]StateRow{{PartitionName: part, BintrailID: id,
				LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100), RowCount: nInt(42), ArchivedAt: tOld}},
			deep)
		if rep.DeepUnverified != 1 {
			t.Fatalf("local-only footer failure must be deep-unverified, got %+v", rep)
		}
	})

	t.Run("S3-only: footer failed → counted (parity with the old per-probe counter)", func(t *testing.T) {
		rep := Diff([]ScannedFile{s3NoCount("bkt", "k/events.parquet", 100)},
			inSyncRow(42),
			DiffOptions{ScannedS3: true, Deep: true, PruneMinAge: time.Hour, Now: tNow})
		if rep.DeepUnverified != 1 {
			t.Fatalf("S3-only footer failure must be deep-unverified, got %+v", rep)
		}
	})

	t.Run("no false-positive: picked count valid → NOT counted", func(t *testing.T) {
		// Local present with a VALID footer → picked count valid → genuinely
		// deep-verified, must not inflate DeepUnverified.
		rep := Diff([]ScannedFile{localFile(part, id, "/a/x.parquet", 100, 42)}, inSyncRow(42), deep)
		if rep.DeepUnverified != 0 {
			t.Fatalf("a deep-verified pair must not be counted, got %+v", rep)
		}
	})

	t.Run("no false-positive via surviving backend: S3-only with a valid footer", func(t *testing.T) {
		// S3-only file whose --deep footer read SUCCEEDED (valid RowCount, no
		// local) → pickMeta returns the valid S3 count → not counted. Confirms
		// the count keys on the PICKED value, not on any-backend-failed.
		s3Verified := s3File(part, id, "bkt", "k/events.parquet", 100)
		s3Verified.RowCount = nInt(42)
		rep := Diff([]ScannedFile{s3Verified},
			inSyncRow(42),
			DiffOptions{ScannedS3: true, Deep: true, PruneMinAge: time.Hour, Now: tNow})
		if rep.DeepUnverified != 0 {
			t.Fatalf("a valid-S3-footer pair must not be counted, got %+v", rep)
		}
	})

	t.Run("not counted without --deep", func(t *testing.T) {
		rep := Diff([]ScannedFile{localNoCount("/a/x.parquet", 100)},
			[]StateRow{{PartitionName: part, BintrailID: id,
				LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100), RowCount: nInt(42), ArchivedAt: tOld}},
			bothScanned())
		if rep.DeepUnverified != 0 {
			t.Fatalf("DeepUnverified must stay zero without --deep, got %+v", rep)
		}
	})

	// Over-count guard (pins the historical regression of the old per-probe
	// counter, which counted once per BACKEND probe rather than once per key).
	t.Run("dual-backend prefer-local: local VALID + S3 no-count → NOT counted", func(t *testing.T) {
		// pickMeta PREFERS local; its footer read fine → picked count valid →
		// genuinely deep-verified, even though the S3 footer was never read.
		// A per-backend counter would (wrongly) count the unread S3 probe.
		rep := Diff([]ScannedFile{
			localFile(part, id, "/a/x.parquet", 100, 42),
			s3NoCount("bkt", "k/events.parquet", 100),
		}, inSyncRow(42), deep)
		if rep.DeepUnverified != 0 {
			t.Fatalf("local-valid pair must not be counted (pickMeta prefers local), got %+v", rep)
		}
	})

	t.Run("dual-backend both no-count → counted ONCE per key, not per backend", func(t *testing.T) {
		// Both footers failed for the SAME (partition, bintrail_id) → one pair →
		// counted exactly once. A per-backend counter would inflate this to 2.
		rep := Diff([]ScannedFile{
			localNoCount("/a/x.parquet", 100),
			s3NoCount("bkt", "k/events.parquet", 100),
		}, inSyncRow(42), deep)
		if rep.DeepUnverified != 1 {
			t.Fatalf("both-failed pair must be counted once per key (not per backend), got %+v", rep)
		}
	})
}

// TestDiffDeterministicOrder: actions sort by (partition, bintrail_id).
func TestDiffDeterministicOrder(t *testing.T) {
	files := []ScannedFile{
		localFile("p_2026060512", "zzz", "/a/2.parquet", 1, 1),
		localFile("p_2026060510", "aaa", "/a/1.parquet", 1, 1),
		localFile("p_2026060510", "bbb", "/a/3.parquet", 1, 1),
	}
	rep := Diff(files, nil, bothScanned())
	got := make([]string, len(rep.Actions))
	for i, a := range rep.Actions {
		got[i] = a.PartitionName + "/" + a.BintrailID
	}
	want := []string{"p_2026060510/aaa", "p_2026060510/bbb", "p_2026060512/zzz"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}
