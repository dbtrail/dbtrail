package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// gapInWindow reports whether a recorded capture-gap timestamp falls inside
// (since, until] — the reconstruction window a baseline+delta replay covers.
// Extracted so the boundary logic is unit-testable without a *sql.DB.
func gapInWindow(gapAt, since, until time.Time) bool {
	return gapAt.After(since) && !gapAt.After(until)
}

// CaptureGap describes what stream_state says about PERMANENT capture loss
// relative to one reconstruction window. A non-nil value means the fold over
// that window cannot be asserted complete — either a stamped loss falls inside
// it, or the index cannot answer the question at all.
type CaptureGap struct {
	// Unevaluable is true when gap state was never readable: a legacy index
	// whose stream_state predates the gap_lost_* columns (#765), read before
	// any migrating command ran. status reports that as "not evaluated" rather
	// than a clean verdict (internal/status/status.go, GapColumnsPresent); the
	// same honesty applies here — absent data is not evidence of no loss.
	// At/Detail are zero in this case.
	Unevaluable bool
	// At is the stamped moment capture permanently lost events, and Detail the
	// supplementary human-readable context (may be empty). Set only when
	// Unevaluable is false.
	At     time.Time
	Detail string
	// Since/Until are the reconstruction window the gap was evaluated against.
	Since, Until time.Time
}

// Reason renders the finding as a flag-free sentence, so each surface can
// append its own remediation (`--allow-gaps` on the CLI, `allow_gaps: true` on
// MCP) without the other surface's vocabulary leaking into it.
func (g *CaptureGap) Reason() string {
	window := fmt.Sprintf("(%s, %s]", g.Since.UTC().Format(time.RFC3339), g.Until.UTC().Format(time.RFC3339))
	if g.Unevaluable {
		return "capture gap state is NOT EVALUABLE for the reconstruction window " + window +
			": this index predates the gap_lost_* columns in stream_state, so a permanent capture loss inside the window cannot be ruled out" +
			" (migrate the index schema — any indexing/streaming command runs the migration — to enable the check)"
	}
	detail := g.Detail
	if detail == "" {
		detail = "no detail recorded"
	}
	return fmt.Sprintf("stamped capture gap at %s falls inside the reconstruction window %s — the index permanently lost events after that point (%s), and no archive can refill them",
		g.At.UTC().Format(time.RFC3339), window, detail)
}

// CaptureGapStatus evaluates stream_state's permanent-loss record (#765)
// against the (since, until] reconstruction window and reports the finding
// WITHOUT deciding what to do about it — CheckCaptureGap is the strict/allow
// policy on top, and surfaces that need to both refuse and, when overridden,
// carry the finding into their payload (the MCP reconstruct tool) call this
// directly. A nil result means "no permanent loss in scope, and that verdict
// was actually evaluated".
//
// An operator's ACKNOWLEDGEMENT of the loss (stream_state.gap_acked_at, stamped
// when the source is stopped in the console) is deliberately not consulted: it
// silences the alarm surfaces, and the events are as gone as before. A window
// spanning the loss stays a gap until the window no longer spans it, which is
// what a full snapshot taken after the loss achieves (its anchor is the new
// `since`).
func CaptureGapStatus(ctx context.Context, db *sql.DB, since, until time.Time) (*CaptureGap, error) {
	ss, err := status.LoadStreamState(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("check stream_state capture gap: %w", err)
	}
	// A nil state is an EMPTY stream_state — no streaming daemon ever ran
	// against this index (file-mode indexing only). That is a genuine "no loss
	// to record", not an unknown: there is no capture whose continuity could
	// have broken. Do not fold it into the Unevaluable arm below.
	if ss == nil {
		return nil, nil
	}
	// The gap_lost_* columns were absent, so LoadStreamState fell back to the
	// base column set and GapLostAt is invalid for a reason that has nothing to
	// do with whether a gap happened. Checked BEFORE GapLostAt.Valid: reading
	// that field first is exactly the silent no-op this arm exists to close.
	if !ss.GapColumnsPresent {
		return &CaptureGap{Unevaluable: true, Since: since, Until: until}, nil
	}
	if !ss.GapLostAt.Valid {
		return nil, nil
	}
	gapAt := ss.GapLostAt.Time
	if !gapInWindow(gapAt, since, until) {
		return nil, nil
	}
	return &CaptureGap{At: gapAt, Detail: ss.GapLostDetail.String, Since: since, Until: until}, nil
}

// CheckCaptureGap consults stream_state.gap_lost_at/gap_lost_detail (#765) —
// the durable record indexer/streamrun stamps when an unfillable binlog gap
// forces an auto-advance, permanently losing events (docs/rotation-and-status.md:
// "the index is valid only up to the gap"). This is a DIFFERENT check from
// GapDetected in gap.go (baseline-anchor-vs-first-event position gap) and from
// the query planner's coverage-gap check (query.GapError: an hour rotated out
// of MySQL with no archive, which is potentially fillable by re-archiving).
// gap_lost_at marks events that no longer exist ANYWHERE — not live MySQL, not
// an archive — so a reconstruction spanning it is silently incomplete
// regardless of archive coverage.
//
// If gap_lost_at falls inside (since, until], the loss is in scope for this
// reconstruction. The same verdict is returned when gap state is NOT EVALUABLE
// (a legacy index missing the gap_lost_* columns): the check would otherwise be
// silently inert on exactly the un-migrated indexes it protects. Under strict
// mode (allowGaps=false, the reconstruct default) this returns an error; under
// --allow-gaps it logs a slog.Warn and returns nil so the caller proceeds with
// a known-incomplete result.
func CheckCaptureGap(ctx context.Context, db *sql.DB, schema, table string, since, until time.Time, allowGaps bool) error {
	_, err := CheckCaptureGapStatus(ctx, db, schema, table, since, until, allowGaps)
	return err
}

// CheckCaptureGapStatus is CheckCaptureGap plus the finding it acted on: the
// same strict/allow policy, but the caller also learns WHAT was overridden when
// allowGaps let the run proceed.
//
// It exists because "proceeded over a known gap" has to outlive the log line
// that reported it. A snapshot published under --allow-gaps is knowingly
// incomplete forever, and the only place that can say so forever is the
// artifact itself (baseline.MetaKeyCaptureGap, #1170) — a warning in a
// terminal the operator has since closed is not a record.
//
// A nil finding with a nil error means the window was evaluated and is clean.
func CheckCaptureGapStatus(ctx context.Context, db *sql.DB, schema, table string, since, until time.Time, allowGaps bool) (*CaptureGap, error) {
	gap, err := CaptureGapStatus(ctx, db, since, until)
	if err != nil || gap == nil {
		return nil, err
	}
	if allowGaps {
		slog.Warn("reconstruct: "+gap.Reason()+"; output may be incomplete, proceeding due to --allow-gaps",
			"schema", schema, "table", table,
			"gap_lost_at", gap.At.UTC().Format(time.RFC3339), "detail", gap.Detail,
			"gap_evaluable", !gap.Unevaluable,
			"window_since", since.UTC().Format(time.RFC3339), "window_until", until.UTC().Format(time.RFC3339))
		return gap, nil
	}
	return nil, fmt.Errorf("reconstruct: %s for %s.%s; pass --allow-gaps to proceed with a known-incomplete reconstruction: %w",
		gap.Reason(), schema, table, ErrCaptureGap)
}
