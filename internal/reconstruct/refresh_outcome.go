package reconstruct

import (
	"errors"
	"fmt"
)

// The verdicts a refresh gives one table. The literals are what
// `bintrail baseline refresh` prints and what the web interface stores in its
// run history, so they are a format: add one, never reword one.
const (
	RefreshVerdictRefreshed  = "refreshed"
	RefreshVerdictUnchanged  = "unchanged"
	RefreshVerdictRefusedGap = "refused-gap"
	RefreshVerdictRefusedDDL = "refused-ddl"
	RefreshVerdictRefused    = "refused"
	// RefreshVerdictRefusedRenumbered: the source's binary log started again
	// from another numbering since the snapshot (#2160); a full snapshot fixes it.
	RefreshVerdictRefusedRenumbered = "refused-renumbered"
	RefreshVerdictSkipped           = "skipped"
)

// RefreshOutcome is one table's verdict in a refresh run.
type RefreshOutcome struct {
	Table   string // schema.table, as the run was asked for it
	Verdict string // one of the RefreshVerdict constants
	Detail  string
}

// Refused reports whether this table stopped the run: publication is
// all-or-nothing, so one refused table means no table was published.
// A skipped table did not stop anything; the run ended before reaching it.
func (o RefreshOutcome) Refused() bool {
	switch o.Verdict {
	case RefreshVerdictRefusedGap, RefreshVerdictRefusedDDL, RefreshVerdictRefusedRenumbered, RefreshVerdictRefused:
		return true
	}
	return false
}

// RefreshOutcomes pairs every requested table with its verdict. It is the one
// place the verdict is decided: the command line prints it and the web
// interface stores it, and two copies of the rule would drift.
//
// The classification reads the sentinels this package exports rather than the
// message text: "the events are gone" and "the table changed shape" have
// completely different remedies, and a summary that blurs them sends the
// operator down the wrong one.
func RefreshOutcomes(tables []string, reports []*TableReport, failures []TableFailure) []RefreshOutcome {
	failed := make(map[string]error, len(failures))
	for _, f := range failures {
		failed[f.Schema+"."+f.Table] = f.Err
	}
	done := make(map[string]bool, len(reports))
	// Separate from done rather than a second bool on it: a carried-forward
	// table WAS published, so it must not read as skipped, and it was not
	// rewritten, so calling it "refreshed" would hide the thing an operator
	// most wants to see here: which tables are actually costing them a full
	// rewrite each cycle.
	unchanged := make(map[string]bool, len(reports))
	// With --table-deltas (#1638) "refreshed" alone would say the opposite of
	// what happened for most tables: the file was NOT rewritten. The verdict
	// stays "refreshed" (the table is current), and the detail says how.
	deltaDetail := make(map[string]string, len(reports))
	for _, r := range reports {
		if r == nil {
			continue
		}
		k := r.Schema + "." + r.Table
		done[k] = true
		unchanged[k] = r.CarriedForward
		switch {
		case r.TableDelta && !r.DeltaPairWritten:
			deltaDetail[k] = fmt.Sprintf("no events in the window; the previous file and its %d delta pairs were kept as they are (last pair %d)",
				r.DeltaChainFiles, r.DeltaSeq)
		case r.TableDelta:
			deltaDetail[k] = fmt.Sprintf("the previous file was kept and this window's change written beside it as pair %d; the chain now has %d pairs (%d rows replaced or removed, %d changed or new rows)",
				r.DeltaSeq, r.DeltaChainFiles, r.DeltaDeadRows, r.DeltaUpsertRows)
		case r.DeltaCompacted != "":
			deltaDetail[k] = "written again in full: " + r.DeltaCompacted
		}
	}

	out := make([]RefreshOutcome, 0, len(tables))
	for _, t := range tables {
		switch err, bad := failed[t]; {
		case bad && err == nil:
			// A failure with no error attached still stopped the run. Said
			// as a refusal with no reason rather than dereferenced.
			out = append(out, RefreshOutcome{t, RefreshVerdictRefused, ""})
		case bad && errors.Is(err, ErrBinlogRenumbered):
			out = append(out, RefreshOutcome{t, RefreshVerdictRefusedRenumbered, err.Error()})
		case bad && errors.Is(err, ErrCaptureGap):
			out = append(out, RefreshOutcome{t, RefreshVerdictRefusedGap, err.Error()})
		case bad && (errors.Is(err, ErrSchemaChanged) || errors.Is(err, ErrDestructiveDDL)):
			out = append(out, RefreshOutcome{t, RefreshVerdictRefusedDDL, err.Error()})
		case bad:
			out = append(out, RefreshOutcome{t, RefreshVerdictRefused, err.Error()})
		case done[t] && unchanged[t]:
			out = append(out, RefreshOutcome{t, RefreshVerdictUnchanged, "no events in the window; the previous file was published as-is"})
		case done[t]:
			out = append(out, RefreshOutcome{t, RefreshVerdictRefreshed, deltaDetail[t]})
		default:
			// Requested, neither reported nor failed: the run was cancelled
			// before this table started. Not "fine", so say so.
			out = append(out, RefreshOutcome{t, RefreshVerdictSkipped, "the run ended before this table was reached"})
		}
	}
	return out
}
