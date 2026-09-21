// Package assurance exposes read-only access to the backup-assurance signals
// — restore coverage, the capture-continuity verdict, baseline staleness, and
// the scheduled-verify run history — for tooling and embedding distributions
// that import the core as a module.
//
// It is a thin facade, the same shape as indexquery: type aliases to the
// internal types (usable across module boundaries through the alias) plus
// one-line wrappers over the internal entry points. It adds no computation of
// its own, and deliberately so — every verdict here has exactly one
// implementation in the core, and an embedder that rendered its own would
// contradict `bintrail status` about the same index.
//
// What the aliases buy is narrow and worth stating exactly: they make a
// FORKED STRUCT impossible, because an alias is the internal type rather than
// a copy of it. They do not make a forked COMPUTATION impossible — DeltaFloor
// exposes Hour, so "grade through Grade, never through Hour alone" stays a
// rule a caller must follow. Where such a rule exists, the doc says so.
//
// Read-only, with one exception worth naming rather than glossing: no
// function here writes anything, but VerifyHistory is an alias, so its whole
// method set travels — including Append, which rewrites the console's history
// file. That is the watch daemon's write path; an embedder must not call it
// (see OpenVerifyHistory).
//
// Note for callers inside this repo: this package imports internal/console
// (the verify history is console-local state on disk), so importing assurance
// from anything cmd/bintrail links would trip the decouple guard in
// cliapp/uifree_test.go. This facade is for embedders, not for the core CLI.
package assurance

import (
	"context"
	"database/sql"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/verify"
)

type (
	// CoverageSummary is the live restore-coverage window (#1194).
	CoverageSummary = status.CoverageSummary
	// StreamStateInfo is the capture checkpoint ContinuityStatus grades.
	StreamStateInfo = status.StreamStateInfo
	// DeltaFloor is the delta-coverage floor and whether values below it can
	// be attributed to a source at all (#1193/#1219). Grade THROUGH it; using
	// Hour alone on a multi-source index manufactures false "broken".
	DeltaFloor = status.DeltaFloor
	// BaselineInfo is one baseline snapshot as graded by staleness (#1193).
	BaselineInfo = status.BaselineInfo
	// BaselineStalenessVerdict is ok / aging / broken / unknown — plus the
	// empty string, which OverallBaselineStaleness can return; see there.
	BaselineStalenessVerdict = status.BaselineStalenessVerdict
	// BaselineFile is one table's Parquet file in a baseline listing. Its
	// Schema field is BaselineInfo's Database under another name.
	BaselineFile = reconstruct.BaselineFile

	// VerifyHistory is the persisted scheduled-verify run history (#1191).
	VerifyHistory = console.VerifyHistory
	// VerifyRunRecord is one completed or skipped run.
	VerifyRunRecord = console.VerifyRunRecord
	// VerifyStatus is the per-run verdict embedded in VerifyRunRecord.
	VerifyStatus = console.VerifyStatus
	// VerifyTableResult is one table's outcome within a run.
	VerifyTableResult = console.VerifyTableResult
	// VerifySummary is a run's match/mismatch/inconclusive tally.
	VerifySummary = console.VerifySummary
	// VerifyMode is which engine path a run used: baseline-anchored (compare
	// the two newest baselines), live-source (compare against production) or
	// recover-inputs (index-only chain walk). They assert different things —
	// a summary that renders them alike lets the weakest inherit the
	// strongest's meaning.
	VerifyMode = console.VerifyMode
)

// Baseline staleness verdicts. Unknown means the check could not be
// evaluated — never treat it as either healthy or broken.
const (
	BaselineOK      = status.BaselineOK
	BaselineAging   = status.BaselineAging
	BaselineBroken  = status.BaselineBroken
	BaselineUnknown = status.BaselineUnknown
)

// Continuity verdicts, as returned by ContinuityStatus and carried in
// CoverageSummary.Continuity:
//
//	ContinuityOK          — no gap in the captured range. NOT a liveness
//	                        claim: it says nothing about the stream running
//	                        now (CoverageSummary.LagSeconds is that signal).
//	ContinuityGapLost     — an unfillable gap was stamped: events are
//	                        permanently lost.
//	ContinuityUnknown     — a legacy index without the gap columns: whether a
//	                        gap happened is unevaluable, never a false "ok".
//	ContinuityUnavailable — stream_state could not be read, likewise.
//	ContinuityNone        — no stream row (file-mode index): no capture ran,
//	                        so no continuity could break. A genuine no-claim.
//
// Branching on "not gap_lost" folds the two unevaluable verdicts into a pass,
// which is the error this vocabulary exists to prevent.
const (
	ContinuityOK          = status.ContinuityOK
	ContinuityGapLost     = status.ContinuityGapLost
	ContinuityUnknown     = status.ContinuityUnknown
	ContinuityUnavailable = status.ContinuityUnavailable
	ContinuityNone        = status.ContinuityNone
)

// Verify run triggers and the VerifyStatus.State vocabulary. "Not skipped" is
// not "verified" — it also admits failed and the two transient states.
const (
	VerifyTriggerManual    = console.VerifyTriggerManual
	VerifyTriggerScheduled = console.VerifyTriggerScheduled
	VerifyStateSkipped     = console.VerifyStateSkipped
	VerifyStateIdle        = console.VerifyStateIdle
	VerifyStateRunning     = console.VerifyStateRunning
	VerifyStateSucceeded   = console.VerifyStateSucceeded
	VerifyStateFailed      = console.VerifyStateFailed
)

// The run-level verdicts VerifyStatus.Verdict carries for a succeeded run,
// by the rule `bintrail verify` exits on. "succeeded" only says a run reached
// its end; a run that proved no table succeeds with VerifyVerdictUnproven.
// VerifyHistory.List fills the field; VerifyStatus.WithVerdict computes it
// for a status held any other way.
const (
	VerifyVerdictVerified      = verify.VerdictVerified
	VerifyVerdictMismatch      = verify.VerdictMismatch
	VerifyVerdictError         = verify.VerdictError
	VerifyVerdictUnproven      = verify.VerdictUnproven
	VerifyVerdictNoPredecessor = verify.VerdictNoPredecessor
)

// Verify modes (VerifyStatus.Mode).
const (
	VerifyModeBaselineAnchored = console.VerifyModeBaselineAnchored
	VerifyModeLiveSource       = console.VerifyModeLiveSource
	VerifyModeRecoverInputs    = console.VerifyModeRecoverInputs
)

// Per-table verify outcomes (VerifyTableResult.Status), sourced from the
// engine's own vocabulary so they cannot drift from it.
//
// VerifyTableInconclusive is NOT a failure — the comparison could not be made
// meaningfully — and is equally not a match. VerifyTableError is what an
// unrecognized engine status normalizes to, so it is never a benign value
// (#1127). A consumer that only looks for mismatch misses both.
const (
	VerifyTableMatch        = string(verify.StatusMatch)
	VerifyTableMismatch     = string(verify.StatusMismatch)
	VerifyTableInconclusive = string(verify.StatusInconclusive)
	VerifyTableError        = string(verify.StatusError)
)

// VerifyHistoryCap is how many runs the history keeps per server. Eviction is
// SILENT, so a List of exactly this length must be reported as a possibly
// truncated window — otherwise a summary states "no failed runs" about a
// period whose failures fell off the front.
const VerifyHistoryCap = console.VerifyHistoryCap

// CollectCoverageSummary computes the restore-coverage window for one index:
// the delta floor, the newest indexed event, capture lag, and the continuity
// verdict. Cheap by construction (no COUNT(*), no whole-table MAX).
//
// It warns-and-degrades rather than failing: only the newest-event read is
// fatal. A failed floor query leaves Floor zero and a failed stream read
// yields ContinuityUnavailable, both with a nil error — so read a zero
// Floor.Hour as "the floor is unknown", never as "coverage starts at the
// epoch". Call OldestDeltaFromDB directly when an unknown floor and a failed
// query have to be told apart.
func CollectCoverageSummary(ctx context.Context, db *sql.DB, dbName string, now time.Time) (*CoverageSummary, error) {
	return status.CollectCoverageSummary(ctx, db, dbName, now)
}

// ContinuityStatus is the single rule that turns a stream checkpoint into a
// continuity verdict (see the Continuity* constants). Pass the error from
// LoadStreamState alongside the value — ContinuityUnavailable is reachable
// only through that error, so dropping it renders an unreadable checkpoint as
// ContinuityNone, a genuine no-claim.
func ContinuityStatus(stream *StreamStateInfo, streamErr error) string {
	return status.ContinuityStatus(stream, streamErr)
}

// LoadStreamState reads the single-row capture checkpoint. A nil value with a
// nil error means no stream row at all (a file-mode index).
//
// On an index predating the gap columns it falls back to the older column set
// and returns GapColumnsPresent false with a nil error — which is why such an
// index grades ContinuityUnknown. The console never migrates registry-
// configured indexes, so an embedder reading one hits this routinely; it is
// not evidence of a gap, nor evidence against one.
func LoadStreamState(ctx context.Context, db *sql.DB) (*StreamStateInfo, error) {
	return status.LoadStreamState(ctx, db)
}

// OldestDeltaFromDB determines how far back the index can restore: the oldest
// live partition, extended backwards across archived hours that reach it, and
// only when those archives can be attributed to a single source.
//
// It is a lower bound on where coverage STARTS, not a promise the range is
// hole-free: interior holes inside the archived range are invisible to it.
func OldestDeltaFromDB(ctx context.Context, db *sql.DB, dbName string) (DeltaFloor, error) {
	return status.OldestDeltaFromDB(ctx, db, dbName)
}

// AnnotateBaselineStaleness grades each baseline in place against the floor.
// OverallBaselineStaleness reads the verdicts this writes, so it has to run
// first — an ungraded slice reduces to the empty verdict.
func AnnotateBaselineStaleness(baselines []BaselineInfo, floor DeltaFloor, now time.Time) {
	status.AnnotateBaselineStaleness(baselines, floor, now)
}

// OverallBaselineStaleness reduces to one verdict, worst-first (broken >
// unknown > aging > ok), over each TABLE'S NEWEST snapshot — an old snapshot
// does not outvote a fresh one for the same table.
//
// It returns the empty verdict, which is none of the four constants, when the
// input is empty or ungraded. Empty input is the worst baseline posture there
// is (no full-table reconstruct is possible at all), so it must not fall
// through a consumer's switch as "no finding".
func OverallBaselineStaleness(baselines []BaselineInfo) BaselineStalenessVerdict {
	return status.OverallBaselineStaleness(baselines)
}

// ListBaselines enumerates the baseline snapshot files under source (a local
// directory or an s3:// prefix), newest snapshot first. Path-derived only —
// no Parquet contents are read, so BaselineFile carries no binlog coordinates
// and no file size, and this listing is narrower than `bintrail status`'s.
//
// It can come back short without an error: snapshots marked incomplete are
// excluded by design (#467), and an unreadable snapshot or schema directory
// is skipped with a log warning. An empty result means "nothing readable was
// found", not "no baselines exist" — and feeding it to
// OverallBaselineStaleness yields the empty verdict, so the two silent paths
// converge unless the caller checks.
func ListBaselines(ctx context.Context, source string) ([]BaselineFile, error) {
	return reconstruct.ListBaselines(ctx, source)
}

// ListBaselinesReport is ListBaselines plus the number of snapshot or schema
// folders the local walk could not read and skipped (#1601, #1639). A caller
// that grades or picks from the listing must treat skipped > 0 as "listed in
// part": the newest snapshot may be among the skipped ones.
func ListBaselinesReport(ctx context.Context, source string) (files []BaselineFile, skipped int, err error) {
	return reconstruct.ListBaselinesReport(ctx, source)
}

// DefaultRegistryPath returns the console server-registry path used when
// neither --servers-file (serve) nor --console-servers-file (watch) is set.
// Re-exported because the history is located relative to it: without this a
// caller would hardcode the path and silently read no history once an
// operator configured one.
//
// It cannot fail, which is itself the caveat: with no usable HOME — a daemon
// under a hardened unit, a scratch container — it falls back to a
// CWD-RELATIVE path, so which file it names depends on the working directory.
// A caller that must not read the wrong history should take the path from its
// own configuration rather than defaulting.
func DefaultRegistryPath() string {
	return console.DefaultRegistryPath()
}

// DefaultVerifyHistoryPath returns the verify-history file path for a given
// console server-registry path (the history lives beside the registry). For a
// console on defaults that is
// DefaultVerifyHistoryPath(DefaultRegistryPath()); pass the operator's
// --servers-file / --console-servers-file value when one is configured.
func DefaultVerifyHistoryPath(serversPath string) string {
	return console.DefaultVerifyHistoryPath(serversPath)
}

// OpenVerifyHistory loads the verify-run history at path. A corrupt or
// newer-versioned file is an error rather than a silent empty history.
//
// A missing file yields an empty history whose Found reports false — check
// it. The file is written only by `bintrail-console watch`: a CLI-only or
// `bintrail-console serve` deployment has none, and watch itself records
// nothing when the file was unreadable at startup. Found false therefore
// means "no runs were ever RECORDED here", which must not render like
// "nothing failed".
//
// The returned value is the daemon's own handle: Append is reachable on it
// and rewrites the whole file from that instance's in-memory state, so an
// embedder calling it while watch runs would discard the daemon's records.
// Read through Found, ServerIDs and List; never write.
func OpenVerifyHistory(path string) (*VerifyHistory, error) {
	return console.OpenVerifyHistory(path)
}
