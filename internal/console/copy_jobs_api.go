package console

import (
	"net/http"
	"slices"
	"strings"
)

// Outcome values for copyJobDTO. The literals are the wire format.
const (
	CopyJobOK      = "ok"
	CopyJobFailed  = "failed"
	CopyJobSkipped = "skipped"
)

// copyJobDTO is one entry of a server's copy-job history as the page draws
// it (#2330): what ran, when, how it ended and how much it moved. It is a
// narrow view of BaselineRunRecord on purpose. The record's lists name
// tables one by one (refused, left out, new), and one of them is uncapped;
// none of them is copied here. What is left that can name a table is the
// free text (error, summary, why, skip reason), and a reader under a data
// profile, the session's own or one the service was started with, is not
// sent it: it keeps the outcome and the counts.
type copyJobDTO struct {
	// Kind is empty for a skipped slot: the schedule files every skip under
	// the full-read kind whatever would have run, so the record's kind says
	// nothing about it.
	Kind    string `json:"kind,omitempty"`
	Trigger string `json:"trigger,omitempty"`
	// Outcome: skipped for a scheduled slot that did not start, failed for a
	// run that recorded an error, ok otherwise.
	Outcome    string `json:"outcome"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	// Repeats: this entry can stand for several tries. A skip, and a
	// compaction that failed, are not recorded again while the same thing
	// keeps happening; the entry's end moves instead. From start to end is
	// then how long it has gone on, not how long a job took.
	Repeats bool `json:"repeats,omitempty"`
	// Published: the run left a snapshot. A failed run can have one (only
	// its upload failed), which is why this is not derived from Outcome.
	Published bool  `json:"published,omitempty"`
	Tables    int   `json:"tables,omitempty"`
	Carried   int   `json:"carried,omitempty"`
	S3Copied  int   `json:"s3_copied,omitempty"`
	Rows      int64 `json:"rows,omitempty"`
	// Events: see BaselineRunRecord.Events. Zero is "not measured", never
	// "no changes".
	Events     int64  `json:"events,omitempty"`
	Why        string `json:"why,omitempty"`
	SkipReason string `json:"skip_reason,omitempty"`
	Error      string `json:"error,omitempty"`
	// Summary is the failure's one-line hint (SnapshotFailure.Summary), for
	// a row that has no room for the whole error text.
	Summary string `json:"summary,omitempty"`
}

func copyJobFromRecord(r BaselineRunRecord, restricted bool) copyJobDTO {
	d := copyJobDTO{
		Kind: r.Kind, Trigger: r.Trigger, Outcome: CopyJobOK,
		StartedAt: r.StartedAt, FinishedAt: r.FinishedAt,
		Published: r.SnapshotTime != "",
		Tables:    r.Tables, Carried: r.Carried, S3Copied: r.S3Copied,
		Rows: r.Rows, Events: r.Events,
		Why: r.Why, SkipReason: r.SkipReason, Error: r.Error,
	}
	switch {
	case r.SkipReason != "":
		d.Outcome, d.Kind, d.Repeats = CopyJobSkipped, "", true
	case r.Error != "":
		d.Outcome = CopyJobFailed
		d.Repeats = r.Kind == BaselineRunCompact
	}
	if r.Failure != nil {
		d.Summary = r.Failure.Summary
	}
	if restricted {
		d.Why, d.SkipReason, d.Error, d.Summary = "", "", "", ""
	}
	return d
}

// handleCopyJobs serves GET /api/servers/{id}/copy-jobs: the copy jobs this
// daemon ran for one server, newest first, with the cap the history is kept
// under so the page can say how far back the list goes. A history file that
// did not open is refused rather than served as an empty list, which would
// read as "no job ever ran here".
func (s *Server) handleCopyJobs(w http.ResponseWriter, r *http.Request) {
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	if s.baselineHistory == nil {
		// Two causes, told apart like the verify history does: with no copy
		// job turned on nothing opens the file, and sending that operator to
		// the log would send them after a failure that did not happen.
		if s.baselineCtrl == nil && s.baselineRefresh == nil && s.baselineRestore == nil {
			writeJSONError(w, http.StatusForbidden,
				"no copy job runs from this DBTrail service, so there is no history of them; turn on snapshots for it to keep one")
			return
		}
		writeJSONError(w, http.StatusForbidden,
			"the copy-job history is not available: its file could not be opened when DBTrail started; check DBTrail's log")
		return
	}
	recs := s.baselineHistory.List(e.ID)
	// Newest first by when each entry last ended. The file's own order is
	// not that: an entry that repeats stays where it was first written
	// while its end moves forward.
	slices.Reverse(recs)
	slices.SortStableFunc(recs, func(a, b BaselineRunRecord) int {
		return strings.Compare(copyJobEnd(b), copyJobEnd(a))
	})
	jobs := make([]copyJobDTO, 0, len(recs))
	// Either kind of restriction: a profile on the session, or one the
	// service was started with (deny or redact rules for every session).
	restricted := s.rbacActiveFor(r)
	for _, rec := range recs {
		jobs = append(jobs, copyJobFromRecord(rec, restricted))
	}
	out := map[string]any{"jobs": jobs, "cap": BaselineRunHistoryCap}
	if restricted {
		// So the page can say why a failed job shows no reason.
		out["detail_withheld"] = true
	}
	writeJSON(w, http.StatusOK, out)
}

// copyJobEnd is the stamp an entry is ordered by: its end, or its start for
// a record that has none. Both are RFC3339 UTC, which sorts as text.
func copyJobEnd(r BaselineRunRecord) string {
	if r.FinishedAt != "" {
		return r.FinishedAt
	}
	return r.StartedAt
}
