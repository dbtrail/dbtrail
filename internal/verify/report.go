package verify

import (
	"fmt"
	"sort"

	"github.com/dbtrail/dbtrail/internal/verify/verdict"
)

// Mode names the comparison a run performed. Emitted so a scheduled consumer
// can tell a drift-free baseline-anchored run from a live-source one without
// re-deriving it from the flags it passed.
const (
	ModeBaselinePair = "baseline-anchored"
	ModeLive         = "live-source"
	// ModeRecoverInputs is the recover-input check (#1001): a per-PK walk of
	// the event chain asserting the before/after images `recover` consumes are
	// internally consistent. It compares no table content, so its per-table
	// rows carry the chain counts below instead of row counts and digests.
	ModeRecoverInputs = "recover-inputs"
)

// Verdict is the run-level outcome — the exit code's reason, as a value. The
// values and the rule that picks one live in internal/verify/verdict, which
// the web interface shares (it cannot link this package).
const (
	// VerdictVerified: at least one table was proven and nothing diverged.
	VerdictVerified = verdict.Verified
	// VerdictMismatch: at least one table diverged from the comparison.
	VerdictMismatch = verdict.Mismatch
	// VerdictError: no mismatch, but at least one table hit a hard error.
	VerdictError = verdict.Error
	// VerdictUnproven: tables were reported but none could be proven (all
	// inconclusive). Fails the run — an all-inconclusive run must never read as
	// "recovery verified".
	VerdictUnproven = verdict.Unproven
	// VerdictNoPredecessor: the source has exactly one baseline, so there is no
	// predecessor to compare against yet. Reported, not failed (exit 0).
	VerdictNoPredecessor = verdict.NoPredecessor
)

// Report is the machine-readable outcome of one verify run: the same per-table
// verdicts, run-level verdict and summary counts the text report prints, plus
// the per-table Anchor the text table has no column for.
//
// It is the payload of `bintrail verify --format json`, and ONLY that. The
// console's verify surface (#677) shipped with a per-table wire shape of its
// own — console.VerifyTableResult/VerifyStatus, fed by consoleapp's
// verifySupervisor — but both surfaces share this package's classification:
// NormalizeStatus decides every table's bucket and Summary.Count tallies it,
// so the CLI and the console can never disagree on whether a status counts as
// a pass, a failure, or "couldn't tell" (#1127). The console's summary mirrors
// this package's Summary field-for-field (console cannot import this package —
// it must not link the capture library — so consoleapp publishes the tally via
// a compile-checked struct conversion).
//
// It is still built in this package rather than in the command layer so the
// construction stays pure — NewReport does no IO, reads no flags, writes no
// stdout — and the CLI owns only the encoding.
//
// It carries no stream-continuity signal: verify never reads stream_state, and
// asserting continuity it did not check would be exactly the false assurance
// this command exists to prevent. That verdict lives in
// `bintrail status --format json` (`continuity.status`).
type Report struct {
	Mode           string `json:"mode"`
	BaselineSource string `json:"baseline_source,omitempty"`
	// Verdict is derived from Summary, never set by callers, so the text and
	// JSON renderings of one run can never disagree.
	Verdict string `json:"verdict"`
	// Message carries a run-level note that has no per-table row (today: the
	// "only one baseline" case).
	Message string        `json:"message,omitempty"`
	Tables  []TableReport `json:"tables"`
	Summary Summary       `json:"summary"`
	// Explain holds the --explain row-level drill-downs, one per mismatched
	// table. Empty unless --explain was passed.
	Explain []ExplainReport `json:"explain,omitempty"`
}

// TableReport is one table's verdict in a Report.
type TableReport struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	// Status is the normalized verdict — the same bucket the summary counted
	// this table in, so a consumer filtering on status and a consumer reading
	// the counts can never disagree.
	Status            Status `json:"status"`
	SourceRows        int64  `json:"source_rows"`
	ReconstructRows   int64  `json:"reconstruct_rows"`
	SourceDigest      string `json:"source_digest,omitempty"`
	ReconstructDigest string `json:"reconstruct_digest,omitempty"`
	// Anchor is the point the comparison was anchored to: a GTID set
	// (live-source) or a binlog coordinate file:pos (baseline pair).
	Anchor string `json:"anchor,omitempty"`
	// Reason is the detail behind an inconclusive/mismatch/error verdict, or a
	// note carried on a match.
	Reason string `json:"reason,omitempty"`

	// The three fields below are populated only by ModeRecoverInputs. They are
	// separate from SourceRows/ReconstructRows on purpose: those two mean
	// "rows in a table", and overloading them with event/chain counts would
	// silently break every consumer already reading them.
	//
	// EventsChecked is how many binlog events the chain walk visited —
	// including events a newer event on the same PK superseded, which the
	// content-comparison modes never look at.
	EventsChecked int `json:"events_checked,omitempty"`
	// ChainsChecked is the number of distinct primary keys walked.
	ChainsChecked int `json:"chains_checked,omitempty"`
	// ChainsInconclusive counts chains that held at least one event with no
	// predecessor state to assert against: the window opened mid-history
	// (first event on the key was not an INSERT), a PK-changing UPDATE moved
	// the row out from under the key, or the chain was restarted after a
	// nil-image/unresolved-TOAST/unknown-type finding. All are legitimately
	// unverifiable rather than divergent.
	ChainsInconclusive int `json:"chains_inconclusive,omitempty"`
	// InconclusiveKind subdivides an inconclusive verdict (#1416): no-activity
	// | nothing-to-assert | unproven. Empty on other statuses and on modes
	// that do not classify.
	InconclusiveKind string `json:"inconclusive_kind,omitempty"`
}

// Summary is the run's per-status counts. The console's VerifySummary mirrors
// it field-for-field (enforced by a struct conversion in consoleapp), so both
// machine-readable surfaces tally with the same buckets and field names.
type Summary struct {
	Match        int `json:"match"`
	Mismatch     int `json:"mismatch"`
	Inconclusive int `json:"inconclusive"`
	// InconclusiveNothingToCheck is the benign slice of Inconclusive — tables
	// with no activity or an append-only shape, where zero assertions is the
	// expected and permanent outcome (#1416). Always <= Inconclusive; the
	// difference is the slice that deserves attention. A subdivision, not a
	// fifth bucket: Total still sums the four statuses.
	InconclusiveNothingToCheck int `json:"inconclusive_nothing_to_check"`
	Error                      int `json:"error"`
	Total                      int `json:"total"`
}

// Count files one table's status under its summary bucket and bumps Total.
// The status is normalized first, so an unrecognized value lands in Error —
// every summary, CLI or console, applies the default-to-failure rule from the
// one place that owns it (NormalizeStatus) instead of re-deciding it locally.
func (s *Summary) Count(status Status) { s.CountWithKind(status, "") }

// CountWithKind is Count carrying the inconclusive subdivision (#1416). The
// kind only matters for StatusInconclusive and only when it is one of the two
// benign values; anything else — including empty, the unclassified case —
// counts as attention-worthy, because defaulting the unknown to benign is the
// direction a verify tool must never round.
func (s *Summary) CountWithKind(status Status, kind string) {
	normalized, _ := NormalizeStatus(status, "")
	if normalized == StatusInconclusive && InconclusiveKindBenign(kind) {
		s.InconclusiveNothingToCheck++
	}
	switch normalized {
	case StatusMatch:
		s.Match++
	case StatusMismatch:
		s.Mismatch++
	case StatusInconclusive:
		s.Inconclusive++
	case StatusError:
		s.Error++
	}
	s.Total++
}

// ExplainReport is the JSON-facing form of a MismatchExplanation. The internal
// type keeps its per-kind totals and deferred-type caveat unexported (they
// exist to drive Write), so marshaling it directly would silently drop both;
// this shape carries everything the text drill-down prints.
type ExplainReport struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Anchor string `json:"anchor,omitempty"`
	// TotalDifferingRows is the full count; Rows is capped at maxExplainRows.
	TotalDifferingRows int          `json:"total_differing_rows"`
	Rows               []ExplainRow `json:"rows"`
	// OverflowByKind counts the differing rows beyond the Rows cap, per kind
	// (missing/changed/extra) — so the data-loss class is never invisible
	// behind changed rows that filled the cap first.
	OverflowByKind map[string]int `json:"overflow_by_kind,omitempty"`
	// DeferredTypeNote mirrors the text caveat: a deferred-type column
	// (ENUM/SET/JSON/binary) is among the diffs, so a shown value pair may be
	// an event image rather than the source text — not necessarily corruption.
	DeferredTypeNote bool `json:"deferred_type_note,omitempty"`
	// Unavailable is set when the drill-down itself failed. Non-fatal: it never
	// changes the run's verdict, exactly as the text path's "unavailable" line.
	Unavailable string `json:"unavailable,omitempty"`
}

// ExplainRow is one primary key that diverged.
type ExplainRow struct {
	PK    string        `json:"pk"`
	Kind  string        `json:"kind"` // changed | missing | extra
	Cells []ExplainCell `json:"cells,omitempty"`
}

// ExplainCell is one column whose reconstructed value diverged.
type ExplainCell struct {
	Column   string `json:"column"`
	Recovery string `json:"recovery"`
	Baseline string `json:"baseline"`
}

// NewReport classifies results into per-table rows, summary counts and a
// run-level verdict. Pure: no IO, no globals. Results are sorted by
// schema.table so the output is stable across runs.
func NewReport(mode string, results []TableResult) *Report {
	sorted := make([]TableResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Schema != sorted[j].Schema {
			return sorted[i].Schema < sorted[j].Schema
		}
		return sorted[i].Table < sorted[j].Table
	})

	rep := &Report{Mode: mode, Tables: make([]TableReport, 0, len(sorted))}
	for _, r := range sorted {
		status, reason := NormalizeStatus(r.Status, r.Detail)
		rep.Summary.CountWithKind(status, r.InconclusiveKind)
		rep.Tables = append(rep.Tables, TableReport{
			Schema:            r.Schema,
			Table:             r.Table,
			Status:            status,
			SourceRows:        r.SourceRows,
			ReconstructRows:   r.ReconstructRows,
			SourceDigest:      r.SourceDigest,
			ReconstructDigest: r.ReconstructDigest,
			Anchor:            r.Anchor,
			Reason:            reason,

			EventsChecked:      r.EventsChecked,
			ChainsChecked:      r.ChainsChecked,
			ChainsInconclusive: r.ChainsInconclusive,
			InconclusiveKind:   r.InconclusiveKind,
		})
	}
	rep.Verdict = verdictOf(rep.Summary)
	return rep
}

// NewNoPredecessorReport is the report for a source with exactly one baseline:
// a legitimate first run with nothing to compare against yet. It exits zero —
// see ExitError.
func NewNoPredecessorReport(mode, baselineSource, message string) *Report {
	return &Report{
		Mode:           mode,
		BaselineSource: baselineSource,
		Verdict:        VerdictNoPredecessor,
		Message:        message,
		Tables:         []TableReport{},
	}
}

// NormalizeStatus maps a TableResult status onto the canonical status a
// consumer sees — the ONE status→bucket decision every verify surface uses
// (the CLI report here, the console wire path in consoleapp; #1127). An
// unrecognized status (including the zero value) is reported as an error,
// never filed under the benign inconclusive bucket — a verify tool's job is
// to not hand out false assurance — and the raw value is kept in the reason
// so the cause is not lost.
func NormalizeStatus(s Status, detail string) (Status, string) {
	switch s {
	case StatusMatch, StatusMismatch, StatusInconclusive, StatusError:
		return s, detail
	}
	reason := fmt.Sprintf("unrecognized verify status %q", string(s))
	if detail != "" {
		reason += ": " + detail
	}
	return StatusError, reason
}

// verdictOf collapses the counts into the run verdict, in the same precedence
// the exit code uses (verdict.Of, shared with the web interface).
func verdictOf(s Summary) string {
	return verdict.Of(s.Match, s.Mismatch, s.Error)
}

// ExitError returns the non-nil error that makes the run exit non-zero, or nil
// for a clean run. It is the single source of the exit contract for both the
// text and the JSON rendering: fail on any divergence or hard error, and fail
// when nothing was proven (an all-inconclusive run must not read as success to
// an operator or a CI gate). A source with only one baseline exits zero.
func (r *Report) ExitError() error {
	switch r.Verdict {
	case VerdictMismatch:
		return fmt.Errorf("%d table(s) diverged from the source", r.Summary.Mismatch)
	case VerdictError:
		return fmt.Errorf("%d table(s) could not be verified due to errors", r.Summary.Error)
	case VerdictUnproven:
		// The exit stays non-zero even when every inconclusive is benign: the
		// operator asked this run to prove recover inputs and it proved none,
		// which a CI gate must not read as success. The message carries the
		// split so a human reading the failure knows whether anything was
		// actually wrong.
		if n := r.Summary.InconclusiveNothingToCheck; n > 0 {
			return fmt.Errorf("no tables were verified (%d inconclusive, of which %d had nothing to check: no changes, or only new rows); nothing proven", r.Summary.Inconclusive, n)
		}
		return fmt.Errorf("no tables were verified (%d inconclusive); nothing proven", r.Summary.Inconclusive)
	}
	return nil
}

// ReportEntry converts a drill-down into its JSON-facing form, carrying the
// per-kind overflow breakdown and the deferred-type caveat that Write prints
// but the struct's exported fields alone do not expose.
func (ex *MismatchExplanation) ReportEntry() ExplainReport {
	out := ExplainReport{
		Schema:             ex.Schema,
		Table:              ex.Table,
		Anchor:             ex.Anchor,
		TotalDifferingRows: ex.Total,
		Rows:               make([]ExplainRow, 0, len(ex.Diffs)),
		DeferredTypeNote:   ex.deferredSeen,
	}
	for _, d := range ex.Diffs {
		row := ExplainRow{PK: d.PK, Kind: d.Kind}
		for _, c := range d.Cells {
			row.Cells = append(row.Cells, ExplainCell{Column: c.Column, Recovery: c.Recovery, Baseline: c.Baseline})
		}
		out.Rows = append(out.Rows, row)
	}
	if ex.Total > len(ex.Diffs) && len(ex.byKind) > 0 {
		shown := map[string]int{}
		for _, d := range ex.Diffs {
			shown[d.Kind]++
		}
		over := map[string]int{}
		for kind, total := range ex.byKind {
			if n := total - shown[kind]; n > 0 {
				over[kind] = n
			}
		}
		if len(over) > 0 {
			out.OverflowByKind = over
		}
	}
	return out
}
