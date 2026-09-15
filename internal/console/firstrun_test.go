package console

import (
	"strings"
	"testing"
)

// TestFirstRunSteps is #1606: the first-run step list says, in order, what
// has been done and what is happening now, from the supervisor's state and
// what the server's own index database holds. A step that has not started is
// waiting, never failed; a failure lands on the first step not done and says
// what to do; the list is complete once one change is indexed.
func TestFirstRunSteps(t *testing.T) {
	type want struct {
		states   string // one letter per step: w waiting, r running, d done, f failed
		complete bool
		failText string // substring of the failed step's detail
		fixText  string // substring of the failed or waiting-for-you step's fix
	}
	yes, no := true, false
	cases := []struct {
		name string
		in   firstRunInput
		want want
	}{
		{"never started: the first step waits for Start, which is not a failure",
			firstRunInput{Monitor: MonitorStatus{State: "stopped"}},
			want{states: "wwww", fixText: "Start"}},
		{"starting, no index database yet",
			firstRunInput{Monitor: MonitorStatus{State: "pending"}, IndexExists: &no},
			want{states: "rwww"}},
		{"index created, reading the table structure",
			firstRunInput{Monitor: MonitorStatus{State: "pending"}, IndexExists: &yes},
			want{states: "drww"}},
		{"structure read, capture connecting",
			firstRunInput{Monitor: MonitorStatus{State: "pending"}, IndexExists: &yes, SnapshotTaken: true},
			want{states: "ddrw"}},
		{"capture running with no change on the source yet is running, not stuck",
			firstRunInput{Monitor: MonitorStatus{State: "running"}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true},
			want{states: "dddr"}},
		{"a saved position means the table structure was read, whatever the snapshot table holds",
			firstRunInput{Monitor: MonitorStatus{State: "running"}, IndexExists: &yes, StreamStarted: true},
			want{states: "dddr"}},
		{"one change indexed completes the list",
			firstRunInput{Monitor: MonitorStatus{State: "running"}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true, EventsIndexed: 1},
			want{states: "dddd", complete: true}},
		{"a failure before the index exists lands on the first step",
			firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "Access denied for user"}, IndexExists: &no},
			want{states: "fwww", failText: "Access denied", fixText: "retr"}},
		{"a failure after the structure was read lands on capture",
			firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "binlog not found"}, IndexExists: &yes, SnapshotTaken: true},
			want{states: "ddfw", failText: "binlog not found"}},
		{"stalled with the stream started lands on the first change",
			firstRunInput{Monitor: MonitorStatus{State: "stalled", LastError: "no progress for 6m0s"}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true},
			want{states: "dddf", failText: "no progress"}},
		{"stopped after the index was created waits for Start on the next step",
			firstRunInput{Monitor: MonitorStatus{State: "stopped"}, IndexExists: &yes, SnapshotTaken: true},
			want{states: "ddww", fixText: "Start"}},
		{"the index could not be checked: no step is claimed, done or not",
			firstRunInput{Monitor: MonitorStatus{State: "running"}, CheckError: "Error 1226: max_user_connections"},
			want{states: ""}},
		{"a reset counter with changes still in the index completes the list",
			firstRunInput{Monitor: MonitorStatus{State: "running"}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true, HasEvents: true},
			want{states: "dddd", complete: true}},
		{"events after a later failure still complete the list",
			firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "x"}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true, EventsIndexed: 5},
			want{states: "dddd", complete: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := firstRunSteps(c.in)
			var states strings.Builder
			for _, s := range got.Steps {
				states.WriteByte(s.State[0])
				if s.State == firstRunFailed && c.want.failText != "" && !strings.Contains(s.Detail, c.want.failText) {
					t.Errorf("failed step %q detail = %q, want it to carry %q", s.Name, s.Detail, c.want.failText)
				}
			}
			if states.String() != c.want.states {
				t.Fatalf("states = %s, want %s (%+v)", states.String(), c.want.states, got.Steps)
			}
			if got.Complete != c.want.complete {
				t.Errorf("complete = %v, want %v", got.Complete, c.want.complete)
			}
			if c.want.fixText != "" {
				found := false
				for _, s := range got.Steps {
					if strings.Contains(s.Fix, c.want.fixText) {
						found = true
					}
				}
				if !found {
					t.Errorf("no step's fix mentions %q: %+v", c.want.fixText, got.Steps)
				}
			}
			for _, s := range got.Steps {
				if s.State == firstRunWaiting && s.Detail != "" && strings.Contains(strings.ToLower(s.Detail), "fail") {
					t.Errorf("waiting step %q reads as failed: %q", s.Name, s.Detail)
				}
				if strings.Contains(s.Name+s.Detail+s.Fix, "—") {
					t.Errorf("step %q holds an em dash", s.Name)
				}
			}
		})
	}
}

// TestFirstRunBackupStep: a first backup is listed only when the console can
// create one and this server has somewhere to put it.
func TestFirstRunBackupStep(t *testing.T) {
	yes := true
	base := firstRunInput{Monitor: MonitorStatus{State: "running"}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true}
	for _, c := range []struct {
		name   string
		backup *BaselineStatus
		states string
	}{
		{"not offered: no backup step", nil, "dddr"},
		{"offered, none yet: waiting", &BaselineStatus{State: "idle"}, "dddrw"},
		{"running", &BaselineStatus{State: "running"}, "dddrr"},
		{"published", &BaselineStatus{State: "succeeded", Published: true}, "dddrd"},
		{"failed", &BaselineStatus{State: "failed", LastError: "mydumper not found"}, "dddrf"},
		{"the fold published and only the upload failed: the backup exists", &BaselineStatus{State: "failed", Published: true, LastError: "upload"}, "dddrd"},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := base
			in.Backup = c.backup
			got := firstRunSteps(in)
			var states strings.Builder
			for _, s := range got.Steps {
				states.WriteByte(s.State[0])
			}
			if states.String() != c.states {
				t.Fatalf("states = %s, want %s (%+v)", states.String(), c.states, got.Steps)
			}
		})
	}
}

// TestFirstRunLostPositionIsShown: capture that skipped events for good is
// still running, and the step says what was lost instead of "A quiet
// database is normal".
func TestFirstRunLostPositionIsShown(t *testing.T) {
	yes := true
	got := firstRunSteps(firstRunInput{Monitor: MonitorStatus{State: "lost_position", LastError: "binlog.000003 was purged; events before it are lost"},
		IndexExists: &yes, SnapshotTaken: true, StreamStarted: true})
	s := got.Steps[3]
	if s.State != firstRunRunning || !strings.Contains(s.Detail, "events before it are lost") || strings.Contains(s.Detail, "quiet") {
		t.Fatalf("step = %+v", s)
	}
}
