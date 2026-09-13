package archive

import (
	"database/sql"
	"fmt"
	"sort"
	"time"
)

// This file implements the pure diff at the heart of `bintrail archive
// reconcile` (#392): archive_state is a REBUILDABLE CACHE over the
// self-describing Hive layout, and Diff computes the actions that bring
// the cache back in line with scanned reality. It is deliberately free of
// DB and storage IO — the command layer scans backends and executes
// actions; this layer only decides.
//
// The load-bearing safety rule is BACKEND-SCOPED CLASSIFICATION: a state
// row may only be judged against backends this invocation actually
// scanned. A row referencing S3 when only --archive-dir was scanned is
// "unverified", never "orphaned" — pruning it would delete the registry
// entry of a perfectly healthy S3 archive and silently drop planner
// coverage for its partition.

// Backend identifies where a scanned file lives.
type Backend string

const (
	BackendLocal Backend = "local"
	BackendS3    Backend = "s3"
)

// ScannedFile is one Hive-layout Parquet file found by a backend scan.
type ScannedFile struct {
	PartitionName string // p_YYYYMMDDHH, derived from event_date/event_hour
	BintrailID    string
	Backend       Backend

	LocalPath string // local backend: absolute file path
	S3Bucket  string // s3 backend
	S3Key     string // s3 backend

	SizeBytes    int64
	LastModified time.Time // file mtime / S3 LastModified

	// RowCount is set when the footer was read (always for local scans —
	// a local footer read is cheap — and for S3 only under --deep).
	RowCount sql.NullInt64
	// ColumnSet is the file's own column set in archive.ColumnSet form, read
	// from the same footer as RowCount and therefore under the same rule:
	// always on a local scan, on S3 only under --deep. Empty = not read.
	//
	// This is the backfill path for #1535: rotate records the set for archives
	// it writes from now on, and everything already on disk gets it here, one
	// footer each, offline — instead of DuckDB paying for every footer on
	// every statement over the events view.
	ColumnSet string
}

// StateRow is one archive_state row as stored.
type StateRow struct {
	PartitionName string
	BintrailID    string
	LocalPath     sql.NullString
	FileSizeBytes sql.NullInt64
	RowCount      sql.NullInt64
	S3Bucket      sql.NullString
	S3Key         sql.NullString
	S3UploadedAt  sql.NullTime
	ColumnSet     sql.NullString
	ArchivedAt    time.Time
}

// ActionKind classifies one reconcile action.
type ActionKind string

const (
	// ActionInsert registers a scanned file that has no archive_state row
	// (e.g. the index was rebuilt and the registry was lost).
	ActionInsert ActionKind = "insert"
	// ActionUpdate repairs drift on an existing row (missing backend
	// columns, stale local_path, size/row_count mismatch, missing
	// s3_uploaded_at stamp).
	ActionUpdate ActionKind = "update"
	// ActionPrune deletes a row whose EVERY referenced backend was scanned
	// and came up empty — a stale registration pointing at nothing.
	ActionPrune ActionKind = "prune"
	// ActionSkipUnverified marks a row that LOOKS orphaned but references
	// a backend this invocation did not scan — never pruned.
	ActionSkipUnverified ActionKind = "skip-unverified"
	// ActionSkipRecent marks a prune candidate younger than the safety
	// margin — a concurrent rotate may still be mid-write.
	ActionSkipRecent ActionKind = "skip-recent"
)

// FieldChange is one column write within an insert/update action. A nil
// Value means SET NULL (used to clear a stale backend reference). Column
// names come from the fixed archive_state schema — never user input.
type FieldChange struct {
	Column string
	Value  any
}

// Action is one reconcile decision, keyed by (partition, bintrail_id).
type Action struct {
	Kind          ActionKind
	PartitionName string
	BintrailID    string
	Reason        string
	Changes       []FieldChange // insert/update only
}

// DiffOptions tells Diff what this invocation scanned and how deep.
type DiffOptions struct {
	// ScannedLocal / ScannedS3 record which backends were actually
	// scanned — the backend-scoped classification input.
	ScannedLocal bool
	ScannedS3    bool
	// Deep enables row_count drift checks on existing rows. (Inserts
	// always carry whatever RowCount the scan filled in.)
	Deep bool
	// PruneMinAge is the concurrency margin: rows younger than this are
	// never prune candidates (an in-flight rotate may still be writing
	// its files when the scan ran).
	PruneMinAge time.Duration
	// TrustEmptyLocal / TrustEmptyS3 treat THAT backend's zero-file scan as
	// genuine emptiness rather than possible misconfiguration, bypassing the
	// blind-scanner prune gate for rows referencing it (#1280). Off by
	// default: an empty scan usually means a mistyped
	// --archive-dir/--archive-s3, and pruning on it would wipe the registry
	// of healthy archives. Per-backend ON PURPOSE — a single global vouch
	// would let a real S3 wipe also disarm the gate for a silently blind
	// local scan (wrong dir, unmounted path) in the same invocation, pruning
	// healthy local registrations. Vouch only for the backend that was
	// legitimately emptied (e.g. S3 lifecycle expiry of the whole prefix) —
	// the one case the gate otherwise dead-ends forever. Neither bypasses
	// the backend-scoped gate: a row referencing an UNSCANNED backend is
	// never a prune candidate.
	TrustEmptyLocal bool
	TrustEmptyS3    bool
	// Now anchors the PruneMinAge comparison (injected for testability).
	Now time.Time
}

// Report is the full reconcile decision set.
type Report struct {
	Actions []Action

	Inserts, Updates, Prunes, SkippedUnverified, SkippedRecent, InSync int

	// DeepUnverified counts scanned (partition, bintrail_id) pairs that
	// --deep was asked to verify but whose PICKED row_count came back Invalid
	// — the footer read failed on the backend pickMeta prefers, so the deep
	// row_count drift check (see updateAction) silently skipped them. Counting
	// at the decision layer (where pickMeta runs) catches BOTH a failed local
	// footer read AND the dual-backend prefer-local case: a row paired with a
	// readable S3 footer but a broken local one would otherwise look
	// deep-verified while the deep check was skipped on the unreadable local
	// count. It does NOT false-positive when the picked count is valid — a
	// partition genuinely deep-verified via the surviving backend (#469).
	DeepUnverified int
}

// Err returns non-nil when drift exists (any insert/update/prune action) —
// the doctor report.Err() pattern, so a dry-run cron invocation exits
// non-zero on drift and zero when the registry matches reality. Skips
// count as drift too: they need operator attention (rescan with the
// missing backend, or wait out the margin).
func (r *Report) Err() error {
	pending := r.Inserts + r.Updates + r.Prunes + r.SkippedUnverified + r.SkippedRecent
	if pending == 0 {
		return nil
	}
	return fmt.Errorf("archive_state drift: %d insert(s), %d update(s), %d prune candidate(s), %d unverified, %d too recent",
		r.Inserts, r.Updates, r.Prunes, r.SkippedUnverified, r.SkippedRecent)
}

type key struct{ partition, bintrailID string }

// Diff computes the actions that reconcile archive_state with the scanned
// files. Output order is deterministic (sorted by partition, bintrail_id).
func Diff(files []ScannedFile, rows []StateRow, opts DiffOptions) Report {
	type pair struct {
		local *ScannedFile
		s3    *ScannedFile
	}
	scanned := make(map[key]*pair)
	for i := range files {
		f := &files[i]
		k := key{f.PartitionName, f.BintrailID}
		p := scanned[k]
		if p == nil {
			p = &pair{}
			scanned[k] = p
		}
		switch f.Backend {
		case BackendLocal:
			p.local = f
		case BackendS3:
			p.s3 = f
		}
	}

	stateByKey := make(map[key]*StateRow, len(rows))
	for i := range rows {
		r := &rows[i]
		stateByKey[key{r.PartitionName, r.BintrailID}] = r
	}

	// Per-backend scan testimony: did the scan SEE any layout file at all?
	// A scan that found zero files cannot distinguish "everything is
	// orphaned" from "wrong --archive-dir/--archive-s3, or a scanner blind
	// spot" — and the latter plus --prune would wipe the registry of
	// healthy archives. Pruning on a backend's testimony requires that
	// backend's scan to have proven it can see the layout (≥1 file) —
	// unless the operator vouched for that specific backend's emptiness
	// via TrustEmptyLocal/TrustEmptyS3 (#1280).
	localSaw, s3Saw := false, false
	for i := range files {
		switch files[i].Backend {
		case BackendLocal:
			localSaw = true
		case BackendS3:
			s3Saw = true
		}
	}

	var report Report

	// Pass 1: scanned files → inserts for keys with no row, updates for
	// rows that drifted from the scanned reality.
	keys := make([]key, 0, len(scanned))
	for k := range scanned {
		keys = append(keys, k)
	}
	sortKeys(keys)
	for _, k := range keys {
		p := scanned[k]
		row, exists := stateByKey[k]
		if !exists {
			// A footer-failed INSERT under --deep is surfaced via report.Inserts
			// (drift → non-zero exit), NOT DeepUnverified — the row is missing
			// entirely, so it's already unaddressed drift; the deep-unverified
			// accounting below is only for EXISTING rows whose row_count check
			// would otherwise be silently skipped. This placement is intentional.
			report.add(insertAction(k, p.local, p.s3))
			continue
		}
		// Deep-verify accounting at the decision layer: every key here has ≥1
		// scanned file. If --deep was asked for but the PICKED row_count is
		// Invalid (the preferred backend's footer read failed), updateAction's
		// row_count drift check is skipped for this pair — a silent --deep
		// downgrade. Count it so the dry-run fails and the report surfaces it,
		// instead of relying on a per-backend probe counter that misses the
		// dual-backend prefer-local case and over-counts when the OTHER
		// backend verified it just fine.
		if opts.Deep {
			if _, rowCount := pickMeta(p.local, p.s3); !rowCount.Valid {
				report.DeepUnverified++
			}
		}
		if a, ok := updateAction(k, row, p.local, p.s3, opts); ok {
			report.add(a)
		} else {
			report.InSync++
		}
	}

	// Pass 2: rows with no scanned file in any of THEIR backends →
	// prune candidates, gated by backend coverage and the age margin.
	rowKeys := make([]key, 0, len(stateByKey))
	for k := range stateByKey {
		if _, hasFiles := scanned[k]; !hasFiles {
			rowKeys = append(rowKeys, k)
		}
	}
	sortKeys(rowKeys)
	for _, k := range rowKeys {
		row := stateByKey[k]
		refsLocal := row.LocalPath.Valid && row.LocalPath.String != ""
		refsS3 := row.S3Bucket.Valid && row.S3Bucket.String != ""

		// Backend-scoped gate: every backend the row references must have
		// been scanned for "no file found" to mean "orphaned".
		if (refsLocal && !opts.ScannedLocal) || (refsS3 && !opts.ScannedS3) {
			report.add(Action{
				Kind: ActionSkipUnverified, PartitionName: k.partition, BintrailID: k.bintrailID,
				Reason: "row references a backend this invocation did not scan; rescan with that backend before pruning",
			})
			continue
		}
		// Blind-scanner gate: a scanned backend whose scan saw ZERO layout
		// files anywhere provides no testimony — refuse to prune on it,
		// unless the operator explicitly vouched for THAT backend's
		// emptiness (#1280).
		if (refsLocal && !localSaw && !opts.TrustEmptyLocal) || (refsS3 && !s3Saw && !opts.TrustEmptyS3) {
			report.add(Action{
				Kind: ActionSkipUnverified, PartitionName: k.partition, BintrailID: k.bintrailID,
				Reason: "the scan found no archive-layout files at all in a referenced backend — wrong --archive-dir/--archive-s3, or the backend was legitimately emptied; refusing to prune on an empty scan (pass --trust-empty-scan=local|s3 naming the wiped backend if the wipe is real)",
			})
			continue
		}
		if opts.PruneMinAge > 0 && opts.Now.Sub(row.ArchivedAt) < opts.PruneMinAge {
			report.add(Action{
				Kind: ActionSkipRecent, PartitionName: k.partition, BintrailID: k.bintrailID,
				Reason: fmt.Sprintf("archived_at within the %s safety margin; a concurrent rotate may still be writing", opts.PruneMinAge),
			})
			continue
		}
		// A prune that only passed the gate because the operator vouched is
		// marked in its Reason — post-incident review of the report must be
		// able to tell evidence-backed deletes from vouched ones (#1280).
		reason := "no backing file in any referenced backend (registry rows only — data files are never touched)"
		if (refsLocal && !localSaw) || (refsS3 && !s3Saw) {
			reason += "; NOTE: --trust-empty-scan overrode the empty-scan gate for this row"
		}
		report.add(Action{
			Kind: ActionPrune, PartitionName: k.partition, BintrailID: k.bintrailID,
			Reason: reason,
		})
	}

	return report
}

func (r *Report) add(a Action) {
	r.Actions = append(r.Actions, a)
	switch a.Kind {
	case ActionInsert:
		r.Inserts++
	case ActionUpdate:
		r.Updates++
	case ActionPrune:
		r.Prunes++
	case ActionSkipUnverified:
		r.SkippedUnverified++
	case ActionSkipRecent:
		r.SkippedRecent++
	}
}

func sortKeys(ks []key) {
	sort.Slice(ks, func(i, j int) bool {
		if ks[i].partition != ks[j].partition {
			return ks[i].partition < ks[j].partition
		}
		return ks[i].bintrailID < ks[j].bintrailID
	})
}

// insertAction builds the full-row insert for a file (or backend pair)
// with no registry row. The column set mirrors rotate's upsert, PLUS
// s3_uploaded_at when the S3 object was confirmed — rotate stamps that in
// a separate post-upload UPDATE, and hasPendingS3Upload treats
// s3_bucket-set + s3_uploaded_at-NULL as an upload still pending, which
// makes rotate REFUSE to drop the partition. A reconcile row must never
// manufacture that phantom pending state.
func insertAction(k key, local, s3f *ScannedFile) Action {
	a := Action{Kind: ActionInsert, PartitionName: k.partition, BintrailID: k.bintrailID, Reason: "file exists with no archive_state row"}
	size, rowCount := pickMeta(local, s3f)
	if local != nil {
		a.Changes = append(a.Changes, FieldChange{"local_path", local.LocalPath})
	}
	a.Changes = append(a.Changes, FieldChange{"file_size_bytes", size})
	if rowCount.Valid {
		a.Changes = append(a.Changes, FieldChange{"row_count", rowCount.Int64})
	}
	if cs := pickColumnSet(local, s3f); cs != "" {
		a.Changes = append(a.Changes, FieldChange{"column_set", cs})
	}
	if s3f != nil {
		a.Changes = append(a.Changes,
			FieldChange{"s3_bucket", s3f.S3Bucket},
			FieldChange{"s3_key", s3f.S3Key},
			FieldChange{"s3_uploaded_at", s3f.LastModified.UTC()},
		)
	}
	return a
}

// updateAction computes backend-scoped field repairs for an existing row.
// Rules:
//   - a scanned backend with a file the row lacks → add its columns
//   - a row-referenced backend that was scanned and has NO file → clear
//     its columns (the other backend has the data — this key reached
//     pass 1, so at least one scanned file exists)
//   - size drift → update (cheap, always when a scanned file exists)
//   - row_count drift → update only under Deep (footer reads are the
//     expensive part on S3)
//   - S3 object confirmed but s3_uploaded_at NULL → stamp it (the
//     hasPendingS3Upload drop-block trap, see insertAction)
//
// Backends NOT scanned this run are never touched.
func updateAction(k key, row *StateRow, local, s3f *ScannedFile, opts DiffOptions) (Action, bool) {
	a := Action{Kind: ActionUpdate, PartitionName: k.partition, BintrailID: k.bintrailID}
	reasons := ""
	addReason := func(s string) {
		if reasons != "" {
			reasons += "; "
		}
		reasons += s
	}

	if opts.ScannedLocal {
		rowHasLocal := row.LocalPath.Valid && row.LocalPath.String != ""
		switch {
		case local != nil && (!rowHasLocal || row.LocalPath.String != local.LocalPath):
			a.Changes = append(a.Changes, FieldChange{"local_path", local.LocalPath})
			addReason("local file present but not (correctly) registered")
		case local == nil && rowHasLocal:
			// Deliberately trusted even when the local scan saw zero files
			// (pinned by TestDiffBackendScopedUpdates): the clear is
			// registry-only, the S3 copy was verified present for this key,
			// and a re-run with the right --archive-dir re-registers it.
			a.Changes = append(a.Changes, FieldChange{"local_path", nil})
			addReason("registered local file is gone (data lives in the other backend; if --archive-dir was mistyped, re-run with the right one to re-register)")
		}
	}

	if opts.ScannedS3 {
		rowHasS3 := row.S3Bucket.Valid && row.S3Bucket.String != ""
		switch {
		case s3f != nil && (!rowHasS3 || row.S3Bucket.String != s3f.S3Bucket || !row.S3Key.Valid || row.S3Key.String != s3f.S3Key):
			a.Changes = append(a.Changes,
				FieldChange{"s3_bucket", s3f.S3Bucket},
				FieldChange{"s3_key", s3f.S3Key},
				FieldChange{"s3_uploaded_at", s3f.LastModified.UTC()},
			)
			addReason("S3 object present but not (correctly) registered")
		case s3f != nil && !row.S3UploadedAt.Valid:
			// Object confirmed; stamp the upload so hasPendingS3Upload
			// doesn't read this row as an upload forever in flight.
			a.Changes = append(a.Changes, FieldChange{"s3_uploaded_at", s3f.LastModified.UTC()})
			addReason("S3 object confirmed but s3_uploaded_at was never stamped (would block partition drops)")
		case s3f == nil && rowHasS3:
			// Same deliberate trust as the local-clear above: reversible,
			// and the local copy was verified PRESENT for this key (the key
			// reached pass 1). Note the clear NULLs s3_bucket as well, so
			// rotate's pending-upload drop-block (bucket set + stamp NULL,
			// see insertAction) no longer applies to this row — the
			// drop-safety argument is the verified local file, not the
			// stamp.
			a.Changes = append(a.Changes,
				FieldChange{"s3_bucket", nil},
				FieldChange{"s3_key", nil},
				FieldChange{"s3_uploaded_at", nil},
			)
			addReason("registered S3 object is gone (data lives in the other backend; if --archive-s3 was mistyped, re-run with the right one to re-register)")
		}
	}

	size, rowCount := pickMeta(local, s3f)
	if (local != nil || s3f != nil) && (!row.FileSizeBytes.Valid || row.FileSizeBytes.Int64 != size) {
		a.Changes = append(a.Changes, FieldChange{"file_size_bytes", size})
		addReason("file size drift")
	}
	if opts.Deep && rowCount.Valid && (!row.RowCount.Valid || row.RowCount.Int64 != rowCount.Int64) {
		a.Changes = append(a.Changes, FieldChange{"row_count", rowCount.Int64})
		addReason("row count drift")
	}
	// column_set: recorded whenever a footer was READ and the row disagrees,
	// NOT gated on --deep. The gate exists to keep S3 footer reads off the
	// default path, and a set that was not read is empty here — so on S3
	// without --deep this condition simply never fires, while a local scan
	// (whose footer read is cheap and already happened) backfills every row
	// #1535 needs. A row already carrying the right set produces no action,
	// which is what keeps a repaired registry from reporting drift forever.
	if cs := pickColumnSet(local, s3f); cs != "" && (!row.ColumnSet.Valid || row.ColumnSet.String != cs) {
		a.Changes = append(a.Changes, FieldChange{"column_set", cs})
		if row.ColumnSet.Valid {
			addReason("archived column set drift")
		} else {
			addReason("archived column set not recorded (the events view binds one footer per file without it)")
		}
	}

	if len(a.Changes) == 0 {
		return Action{}, false
	}
	a.Reason = reasons
	return a, true
}

// pickColumnSet picks the column set from the scanned files, preferring local
// for the same reason pickMeta does: the two describe the same Parquet object,
// and the local footer read is the one that already happened. Empty when
// neither footer was read (an S3-only scan without --deep).
func pickColumnSet(local, s3f *ScannedFile) string {
	if local != nil && local.ColumnSet != "" {
		return local.ColumnSet
	}
	if s3f != nil {
		return s3f.ColumnSet
	}
	return ""
}

// pickMeta picks size/row_count from the scanned files, preferring local
// (cheaper to have read; the two must describe the same Parquet object).
func pickMeta(local, s3f *ScannedFile) (int64, sql.NullInt64) {
	switch {
	case local != nil:
		return local.SizeBytes, local.RowCount
	case s3f != nil:
		return s3f.SizeBytes, s3f.RowCount
	default:
		return 0, sql.NullInt64{}
	}
}
