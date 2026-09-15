package console

import (
	"encoding/json"
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
			want{states: "wwwww", fixText: "Start"}},
		{"starting, no index database yet",
			firstRunInput{Monitor: MonitorStatus{State: "pending"}, IndexExists: &no},
			want{states: "rwwww"}},
		{"index created, connecting to the source",
			firstRunInput{Monitor: MonitorStatus{State: "pending"}, IndexExists: &yes},
			want{states: "drwww"}},
		{"connected, reading the table structure",
			firstRunInput{Monitor: MonitorStatus{State: "pending", SourceConnected: true}, IndexExists: &yes},
			want{states: "ddrww"}},
		{"structure read, saving the first position",
			firstRunInput{Monitor: MonitorStatus{State: "pending", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true},
			want{states: "dddrw"}},
		{"capture running with no change on the source yet is running, not stuck",
			firstRunInput{Monitor: MonitorStatus{State: "running", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true},
			want{states: "ddddr"}},
		{"a saved position means the earlier steps were done, whatever this run reports",
			firstRunInput{Monitor: MonitorStatus{State: "running"}, IndexExists: &yes, StreamStarted: true},
			want{states: "ddddr"}},
		{"one change indexed completes the list",
			firstRunInput{Monitor: MonitorStatus{State: "running", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true, EventsIndexed: 1},
			want{states: "ddddd", complete: true}},
		{"a failure before the index exists lands on the first step",
			firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "Access denied for user (retrying)", Retrying: true}, IndexExists: &no},
			want{states: "fwwww", failText: "Access denied", fixText: "retries on its own"}},
		{"a failure the supervisor gave up on asks for Start, not a retry",
			firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "Access denied (gave up after 6h0m0s of crash-looping; fix the issue, then press Start to retry)"}, IndexExists: &yes},
			want{states: "dfwww", fixText: "then press Start"}},
		{"a source that cannot be reached lands on the connection",
			firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "dial tcp 10.0.0.5:3306: connection refused"}, IndexExists: &yes},
			want{states: "dfwww", failText: "connection refused"}},
		{"a failure after the structure was read lands on capture",
			firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "binlog not found"}, IndexExists: &yes, SnapshotTaken: true},
			want{states: "dddfw", failText: "binlog not found"}},
		{"stalled with the stream started lands on the first change",
			firstRunInput{Monitor: MonitorStatus{State: "stalled", LastError: "no progress for 6m0s", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true},
			want{states: "ddddf", failText: "no progress"}},
		{"stopped after the structure was read waits for Start on the next step",
			firstRunInput{Monitor: MonitorStatus{State: "stopped"}, IndexExists: &yes, SnapshotTaken: true},
			want{states: "dddww", fixText: "Start"}},
		{"a run that stalled before saving a position lands on the first change",
			firstRunInput{Monitor: MonitorStatus{State: "stalled", LastError: "no progress for 6m0s", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true},
			want{states: "ddddf", failText: "no progress"}},
		{"the index could not be checked: no step is claimed, done or not",
			firstRunInput{Monitor: MonitorStatus{State: "running"}, CheckError: "Error 1226: max_user_connections"},
			want{states: ""}},
		{"a reset counter with changes still in the index completes the list",
			firstRunInput{Monitor: MonitorStatus{State: "running"}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true, HasEvents: true},
			want{states: "ddddd", complete: true}},
		{"events after a later failure still complete the list",
			firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "x"}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true, EventsIndexed: 5},
			want{states: "ddddd", complete: true}},
		{"PostgreSQL has no structure step: a quiet source waits for its first change",
			firstRunInput{Postgres: true, Monitor: MonitorStatus{State: "running", SourceConnected: true}, IndexExists: &yes},
			want{states: "dddr"}},
		{"PostgreSQL reporting progress before it reached the source is still connecting",
			firstRunInput{Postgres: true, Monitor: MonitorStatus{State: "running"}, IndexExists: &yes},
			want{states: "drww"}},
		{"PostgreSQL connected and starting",
			firstRunInput{Postgres: true, Monitor: MonitorStatus{State: "pending", SourceConnected: true}, IndexExists: &yes},
			want{states: "ddrw"}},
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
	base := firstRunInput{Monitor: MonitorStatus{State: "running", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true}
	for _, c := range []struct {
		name   string
		backup *BaselineStatus
		states string
	}{
		{"not offered: no backup step", nil, "ddddr"},
		{"offered, none yet: waiting", &BaselineStatus{State: "idle"}, "ddddrw"},
		{"running", &BaselineStatus{State: "running"}, "ddddrr"},
		{"published", &BaselineStatus{State: "succeeded", Published: true}, "ddddrd"},
		{"failed", &BaselineStatus{State: "failed", LastError: "mydumper not found"}, "ddddrf"},
		{"the fold published and only the upload failed: the backup exists", &BaselineStatus{State: "failed", Published: true, LastError: "upload"}, "ddddrd"},
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
	got := firstRunSteps(firstRunInput{Monitor: MonitorStatus{State: "lost_position", LastError: "binlog.000003 was purged; events before it are lost", SourceConnected: true},
		IndexExists: &yes, SnapshotTaken: true, StreamStarted: true})
	s := got.Steps[4]
	if s.State != firstRunRunning || !strings.Contains(s.Detail, "events before it are lost") || strings.Contains(s.Detail, "quiet") {
		t.Fatalf("step = %+v", s)
	}
}

// TestFirstRunFixMatchesTheRetry: only a failure the supervisor is retrying
// says capture retries on its own; the error text is not what decides it.
func TestFirstRunFixMatchesTheRetry(t *testing.T) {
	yes := true
	for _, c := range []struct {
		lastErr  string
		retrying bool
	}{
		{"dial tcp: connection refused (retrying)", true},
		{"dial tcp: connection refused (gave up after 6h0m0s of crash-looping; fix the issue, then press Start to retry)", false},
		{"create index database: Access denied", false},
		{"a message that happens to end in (retrying)", false},
	} {
		got := firstRunSteps(firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: c.lastErr, Retrying: c.retrying}, IndexExists: &yes})
		fix := got.Steps[1].Fix
		if strings.Contains(fix, "retries on its own") != c.retrying || (!c.retrying && !strings.Contains(fix, "then press Start")) {
			t.Errorf("%q retrying=%v: fix = %q", c.lastErr, c.retrying, fix)
		}
	}
}

// TestHandleFirstRun drives GET /api/servers/{id}/first-run through the real
// router (#1606): which servers answer, which steps each kind of server gets,
// and that an unreadable index answers with a scrubbed error and no steps.
func TestHandleFirstRun(t *testing.T) {
	srv, _ := newBaselineTriggerServer(t)
	srv.monitorCtrl.(*stubMonitorCtrl).status = MonitorStatus{State: "stopped"}
	add := func(e ServerEntry) string {
		t.Helper()
		got, err := srv.cm.reg.Add(e)
		if err != nil {
			t.Fatalf("add %s: %v", e.Name, err)
		}
		return got.ID
	}
	get := func(id string) (int, string, FirstRunReport) {
		t.Helper()
		rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/first-run", "")
		var rep FirstRunReport
		if rec.Code == 200 {
			if err := json.Unmarshal(body, &rep); err != nil {
				t.Fatalf("decode %s: %v", body, err)
			}
		}
		return rec.Code, string(body), rep
	}
	names := func(rep FirstRunReport) string {
		var n []string
		for _, s := range rep.Steps {
			n = append(n, s.Name)
		}
		return strings.Join(n, " | ")
	}

	t.Run("an unknown server is a 404", func(t *testing.T) {
		if code, body, _ := get("nope"); code != 404 {
			t.Fatalf("code = %d, body = %s", code, body)
		}
	})
	t.Run("a server with no source is a 409", func(t *testing.T) {
		id := add(ServerEntry{Name: "nosrc", DSN: "idx:idxpw@tcp(127.0.0.1:1)/bintrail_idx_nosrc"})
		if code, body, _ := get(id); code != 409 {
			t.Fatalf("code = %d, body = %s", code, body)
		}
	})
	t.Run("a MySQL server gets the structure step and no backup step without a location", func(t *testing.T) {
		id := add(ServerEntry{Name: "my", SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/"})
		code, body, rep := get(id)
		if code != 200 || len(rep.Steps) != 5 || !strings.Contains(names(rep), "Read the table structure") || strings.Contains(names(rep), "backup") {
			t.Fatalf("code = %d, steps = %s, body = %s", code, names(rep), body)
		}
		if !strings.Contains(body, `"name":"Create the index database"`) || !strings.Contains(body, `"state":"waiting"`) {
			t.Errorf("the wire shape changed: %s", body)
		}
	})
	t.Run("a PostgreSQL server with a location gets no structure step and a backup step", func(t *testing.T) {
		id := add(ServerEntry{Name: "pg", Flavor: FlavorPostgres, SourceDSN: "postgres://u:pw@127.0.0.1:2/db",
			SourceSlot: "s", SourcePublication: "p", BaselineDir: t.TempDir()})
		code, body, rep := get(id)
		if code != 200 || strings.Contains(names(rep), "Read the table structure") || !strings.Contains(names(rep), "Take the first backup") {
			t.Fatalf("code = %d, steps = %s, body = %s", code, names(rep), body)
		}
	})
	t.Run("an index that cannot be read answers with its error, scrubbed, and no steps", func(t *testing.T) {
		id := add(ServerEntry{Name: "unreach", DSN: "idx:idxpw@tcp(127.0.0.1:1)/bintrail_idx_unreach", SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/"})
		code, body, rep := get(id)
		if code != 200 || len(rep.Steps) != 0 || !strings.Contains(body, `"check_error":`) || !strings.Contains(rep.CheckError, "127.0.0.1:1") || strings.Contains(body, "idxpw") {
			t.Fatalf("code = %d, body = %s", code, body)
		}
	})
}
