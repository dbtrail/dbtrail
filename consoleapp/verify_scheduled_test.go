package consoleapp

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

func TestScheduledVerifyRequest_modeSelection(t *testing.T) {
	entry := func(dir, s3 string) console.ServerEntry {
		return console.ServerEntry{ID: "id1", Name: "wp", DSN: "dsn", BaselineDir: dir, BaselineS3: s3}
	}
	cases := []struct {
		name            string
		e               console.ServerEntry
		wantMode        console.VerifyMode
		wantDir, wantS3 string
	}{
		{"own baseline dir", entry("/b", ""), console.VerifyModeBaselineAnchored, "/b", ""},
		{"own S3", entry("", "s3://own"), console.VerifyModeBaselineAnchored, "", "s3://own"},
		{"both", entry("/b", "s3://own"), console.VerifyModeBaselineAnchored, "/b", "s3://own"},
		// #1684: the daemon's --baseline-dir no longer backs a server with
		// none of its own, so the schedule checks what it can without one.
		{"no location of its own → recover-inputs", entry("", ""), console.VerifyModeRecoverInputs, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := scheduledVerifyRequest(tc.e, []string{"s.t"})
			if req.Mode != tc.wantMode || req.BaselineDir != tc.wantDir || req.BaselineS3 != tc.wantS3 {
				t.Fatalf("got mode=%s dir=%q s3=%q, want mode=%s dir=%q s3=%q",
					req.Mode, req.BaselineDir, req.BaselineS3, tc.wantMode, tc.wantDir, tc.wantS3)
			}
			if req.ServerID != "id1" || req.IndexDSN != "dsn" || len(req.Tables) != 1 {
				t.Fatalf("request lost entry fields: %+v", req)
			}
		})
	}
}

func TestSplitVerifyTables(t *testing.T) {
	if got := splitVerifyTables(""); got != nil {
		t.Fatalf("empty flag must mean no filter (nil), got %v", got)
	}
	got := splitVerifyTables(" a.b, ,c.d ")
	if len(got) != 2 || got[0] != "a.b" || got[1] != "c.d" {
		t.Fatalf("got %v", got)
	}
}

// TestVerifySupervisor_finishRecordsHistory drives begin→appendResult→finish
// (the exact path both manual and scheduled runs take) and asserts the run
// lands in the persisted history with its trigger and summary.
func TestVerifySupervisor_finishRecordsHistory(t *testing.T) {
	hist, err := console.OpenVerifyHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := newVerifySupervisor(context.Background(), hist, nil)
	req := console.VerifyRequest{ServerID: "srv1", ServerName: "wp", Mode: console.VerifyModeRecoverInputs}
	if _, err := s.begin(req, "scheduled"); err != nil {
		t.Fatal(err)
	}

	// While the run is in flight, a second admission — scheduled or manual —
	// must be refused, not stacked.
	if _, err := s.begin(req, "manual"); !errors.Is(err, console.ErrVerifyRunning) {
		t.Fatalf("concurrent begin: got %v, want ErrVerifyRunning", err)
	}
	if err := s.RunScheduled(req); !errors.Is(err, console.ErrVerifyRunning) {
		t.Fatalf("concurrent RunScheduled: got %v, want ErrVerifyRunning", err)
	}

	s.appendResult("srv1", console.VerifyTableResult{Schema: "a", Table: "b", Status: "match"})
	s.finish("srv1", nil)

	recs := hist.List("srv1")
	if len(recs) != 1 {
		t.Fatalf("want 1 history record, got %d", len(recs))
	}
	r := recs[0]
	if r.Trigger != "scheduled" || r.ServerName != "wp" || r.State != "succeeded" ||
		r.Mode != console.VerifyModeRecoverInputs || r.Summary.Match != 1 || len(r.Results) != 1 {
		t.Fatalf("history record incomplete: %+v", r)
	}

	// A failed run records its error too.
	if _, err := s.begin(req, "manual"); err != nil {
		t.Fatal(err)
	}
	s.finish("srv1", errors.New("connect index: boom"))
	recs = hist.List("srv1")
	if len(recs) != 2 || recs[0].State != "failed" || recs[0].LastError == "" || recs[0].Trigger != "manual" {
		t.Fatalf("failed run not recorded: %+v", recs)
	}
}

func TestRecordVerifySkip(t *testing.T) {
	hist, err := console.OpenVerifyHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	recordVerifySkip(hist, console.ServerEntry{ID: "s1", Name: "wp"}, "a verify run was already in flight when the schedule fired")
	recs := hist.List("s1")
	if len(recs) != 1 || recs[0].State != "skipped" || recs[0].SkipReason == "" || recs[0].Trigger != "scheduled" {
		t.Fatalf("skip not recorded: %+v", recs)
	}
	// Consecutive identical skips collapse into one record — a wedged run must
	// not flood the capped history and evict the real verdicts.
	recordVerifySkip(hist, console.ServerEntry{ID: "s1", Name: "wp"}, "a verify run was already in flight when the schedule fired")
	if recs = hist.List("s1"); len(recs) != 1 {
		t.Fatalf("identical consecutive skip was appended: %+v", recs)
	}
	recordVerifySkip(hist, console.ServerEntry{ID: "s1", Name: "wp"}, "another reason")
	if recs = hist.List("s1"); len(recs) != 2 {
		t.Fatalf("skip with a new reason must append: %+v", recs)
	}
	// nil history is a no-op, never a panic (history can be unavailable).
	recordVerifySkip(nil, console.ServerEntry{ID: "s1"}, "x")
}
