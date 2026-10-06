package console

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The journal of running jobs (#2180) is what lets a restarted daemon find
// what a killed one left: it has to survive a reopen, because the reader is
// always a DIFFERENT process from the writer.
func TestBaselineJobs_surviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.json")
	h, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.BeginJob(BaselineJob{RunID: "r1", LockPath: "/l/r1", ServerID: "s", Kind: BaselineRunRefresh, StartedAt: "2026-10-06T10:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if err := h.JobCreated("r1", BaselineJobDir{Root: "/b", ResolvedRoot: "/b", Name: "2026-10-06T10-00-00Z"}); err != nil {
		t.Fatal(err)
	}
	h2, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	jobs := h2.Jobs()
	if len(jobs) != 1 || jobs[0].RunID != "r1" || len(jobs[0].Dirs) != 1 || jobs[0].Dirs[0].Name != "2026-10-06T10-00-00Z" {
		t.Fatalf("reopened jobs = %+v, want r1 with its one directory", jobs)
	}
	// A copy: a caller editing what it got must not edit the journal.
	jobs[0].Dirs[0].Name = "changed"
	if h2.Jobs()[0].Dirs[0].Name != "2026-10-06T10-00-00Z" {
		t.Error("Jobs handed out the journal's own slice")
	}
}

// FinishJob writes the run's record AND marks the job recorded in ONE save:
// a kill between two saves would leave a finished run that the next boot
// records a second time, as interrupted.
func TestBaselineJobs_finishRecordsAndMarksInOneSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.json")
	h, _ := OpenBaselineHistory(path)
	_ = h.BeginJob(BaselineJob{RunID: "r1", LockPath: "/l", ServerID: "s", Kind: BaselineRunDump, StartedAt: "t0"})
	if err := h.FinishJob("r1", BaselineRunRecord{ServerID: "s", Kind: BaselineRunDump, StartedAt: "t0", FinishedAt: "t1"}); err != nil {
		t.Fatal(err)
	}
	h2, _ := OpenBaselineHistory(path)
	if recs := h2.List("s"); len(recs) != 1 || recs[0].FinishedAt != "t1" {
		t.Fatalf("records after FinishJob = %+v", recs)
	}
	if jobs := h2.Jobs(); len(jobs) != 1 || !jobs[0].Recorded {
		t.Fatalf("jobs after FinishJob = %+v, want r1 still present and marked recorded", jobs)
	}
}

// FinishJob for a job the journal does not hold (no journal for this run)
// still records the run: the record is the run's, the journal is optional.
func TestBaselineJobs_finishWithoutAJobStillRecords(t *testing.T) {
	h, _ := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err := h.FinishJob("", BaselineRunRecord{ServerID: "s", Kind: BaselineRunRefresh}); err != nil {
		t.Fatal(err)
	}
	if len(h.List("s")) != 1 {
		t.Fatal("a run with no journal entry was not recorded")
	}
	if len(h.Jobs()) != 0 {
		t.Fatal("FinishJob with no run id created a job")
	}
}

func TestBaselineJobs_dropWithAndWithoutARecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.json")
	h, _ := OpenBaselineHistory(path)
	_ = h.BeginJob(BaselineJob{RunID: "a", LockPath: "/a", ServerID: "s", Kind: BaselineRunDump})
	_ = h.BeginJob(BaselineJob{RunID: "b", LockPath: "/b", ServerID: "s", Kind: BaselineRunDump})
	if err := h.DropJob("a", nil); err != nil {
		t.Fatal(err)
	}
	if err := h.DropJob("b", &BaselineRunRecord{ServerID: "s", Kind: BaselineRunDump, Error: "interrupted"}); err != nil {
		t.Fatal(err)
	}
	h2, _ := OpenBaselineHistory(path)
	if len(h2.Jobs()) != 0 {
		t.Fatalf("jobs left after both drops: %+v", h2.Jobs())
	}
	recs := h2.List("s")
	if len(recs) != 1 || !strings.Contains(recs[0].Error, "interrupted") {
		t.Fatalf("records = %+v, want exactly the one DropJob was given", recs)
	}
}

// A history file from a binary before the journal has no "jobs" key and
// must load as an empty journal, and a file with jobs must still load in a
// reader that ignores the key (the key is optional and additive).
func TestBaselineJobs_oldFileLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"servers":{"s":[{"server_id":"s","kind":"dump","started_at":"a","finished_at":"b"}]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	h, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Jobs()) != 0 || len(h.List("s")) != 1 {
		t.Fatalf("old file: jobs=%v records=%v", h.Jobs(), h.List("s"))
	}
}
