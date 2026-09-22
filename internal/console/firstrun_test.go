package console

import (
	"encoding/json"
	"strconv"
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

// TestFirstRunBackupStep: the first backup is listed with its job's state when
// the console can create one, and as waiting, with the reason, when it cannot
// (#1677). It is left out only when neither applies.
func TestFirstRunBackupStep(t *testing.T) {
	yes := true
	base := firstRunInput{Monitor: MonitorStatus{State: "running", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true}
	for _, c := range []struct {
		name       string
		backup     *BaselineStatus
		off, noLoc bool
		states     string
	}{
		{"not offered and nothing blocks it: no backup step", nil, false, false, "ddddr"},
		{"turned off in this process: waiting", nil, true, false, "ddddrw"},
		{"turned off and no location: waiting", nil, true, true, "ddddrw"},
		{"no location of its own: waiting", nil, false, true, "ddddrw"},
		{"offered, none yet: waiting", &BaselineStatus{State: "idle"}, false, false, "ddddrw"},
		{"running", &BaselineStatus{State: "running"}, false, false, "ddddrr"},
		{"published", &BaselineStatus{State: "succeeded", Published: true}, false, false, "ddddrd"},
		{"failed", &BaselineStatus{State: "failed", LastError: "mydumper not found"}, false, false, "ddddrf"},
		{"the fold published and only the upload failed: the backup exists", &BaselineStatus{State: "failed", Published: true, LastError: "upload"}, false, false, "ddddrd"},
		{"a job's state outranks a reason", &BaselineStatus{State: "running"}, true, true, "ddddrr"},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := base
			in.Backup, in.BackupOff, in.BackupNoLocation = c.backup, c.off, c.noLoc
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
	// Backups off is the bare default, and its step can never be done: the
	// list must still complete on the first captured change, or the card
	// would sit on every such Overview for good, polling.
	t.Run("a backup step that cannot be done does not hold the list open", func(t *testing.T) {
		in := base
		in.EventsIndexed, in.BackupOff = 1, true
		got := firstRunSteps(in)
		var states strings.Builder
		for _, s := range got.Steps {
			states.WriteByte(s.State[0])
		}
		if states.String() != "dddddw" || !got.Complete {
			t.Fatalf("states = %s, complete = %v, want dddddw and complete", states.String(), got.Complete)
		}
	})
}

// TestFirstRunBackupStepSaysWhyItCannotRun is #1677: a first backup the
// console cannot create is listed with the reason and what to do, in words.
// The step used to vanish, and a list with no backup step reads as an install
// that needs none. The daemon setting is named the way the Snapshots page
// labels it, never as a variable: the step shows no commands.
// mydumper is named for MySQL and MariaDB, whose full backup runs it; a
// PostgreSQL full backup runs inside DBTrail.
func TestFirstRunBackupStepSaysWhyItCannotRun(t *testing.T) {
	yes := true
	base := firstRunInput{Monitor: MonitorStatus{State: "running", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true}
	for _, c := range []struct {
		name              string
		off, noLoc, pg    bool
		detailHas, fixHas []string
		fixLacks          []string
	}{
		{"off, MySQL, location set", true, false, false,
			[]string{"turned off", "whole table"},
			[]string{"Create-backup button", "Set when DBTrail starts", PageSnapshots + " page", "Restart DBTrail", "reads every table this server captures", "mydumper"},
			[]string{"backup location"}},
		{"off, MySQL, no location: both fixes", true, true, false,
			[]string{"turned off"},
			[]string{"Create-backup button", "mydumper", "its own backup location"},
			nil},
		{"off, PostgreSQL: no mydumper", true, false, true,
			[]string{"turned off"},
			[]string{"Create-backup button", "reads every table"},
			[]string{"mydumper", "backup location"}},
		{"on, no location of its own", false, true, false,
			[]string{"no backup location of its own"},
			// One page now (#1573), so the fix names it once and then says
			// where on it — naming a second page would send the reader
			// looking for one that does not exist.
			[]string{"Backup dir or Backup S3", PageSnapshots + " page", "Where and how often"},
			[]string{"Create-backup button", "mydumper", "Restart"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := base
			in.BackupOff, in.BackupNoLocation, in.Postgres = c.off, c.noLoc, c.pg
			if c.pg {
				in.SnapshotTaken = false
			}
			got := firstRunSteps(in)
			s := got.Steps[len(got.Steps)-1]
			if s.Name != "Take the first backup" || s.State != firstRunWaiting {
				t.Fatalf("last step = %+v", s)
			}
			for _, w := range c.detailHas {
				if !strings.Contains(s.Detail, w) {
					t.Errorf("detail %q lacks %q", s.Detail, w)
				}
			}
			for _, w := range c.fixHas {
				if !strings.Contains(s.Fix, w) {
					t.Errorf("fix %q lacks %q", s.Fix, w)
				}
			}
			for _, w := range c.fixLacks {
				if strings.Contains(s.Fix, w) {
					t.Errorf("fix %q holds %q", s.Fix, w)
				}
			}
			for _, text := range []string{s.Detail, s.Fix} {
				for _, bad := range []string{"—", "BINTRAIL_", "--", "=1", " here", "this page", "  ", ".."} {
					if strings.Contains(text, bad) {
						t.Errorf("%q holds %q", text, bad)
					}
				}
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
	t.Run("a MySQL server gets the structure step, and a backup step saying it has no location", func(t *testing.T) {
		id := add(ServerEntry{Name: "my", SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/"})
		code, body, rep := get(id)
		if code != 200 || len(rep.Steps) != 6 || !strings.Contains(names(rep), "Read the table structure") {
			t.Fatalf("code = %d, steps = %s, body = %s", code, names(rep), body)
		}
		if s := rep.Steps[5]; s.Name != "Take the first backup" || s.State != firstRunWaiting || !strings.Contains(s.Detail, "no backup location of its own") {
			t.Fatalf("backup step = %+v", s)
		}
		if !strings.Contains(body, `"name":"Create the index database"`) || !strings.Contains(body, `"state":"waiting"`) {
			t.Errorf("the wire shape changed: %s", body)
		}
	})
	t.Run("a MySQL server with a location gets its backup job's state", func(t *testing.T) {
		id := add(ServerEntry{Name: "myloc", SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/", BaselineS3: "s3://b/p"})
		_, body, rep := get(id)
		if s := rep.Steps[len(rep.Steps)-1]; s.Name != "Take the first backup" || s.Fix != "Create one on the "+PageSnapshots+" page." {
			t.Fatalf("backup step = %+v, body = %s", s, body)
		}
	})
	// Both with and without a location: the precheck reports a missing
	// location before the slot, so the one with none is what pins the order.
	for _, loc := range []string{t.TempDir(), ""} {
		t.Run("a PostgreSQL server with no slot gets no backup step: capture cannot run for it (location "+strconv.Quote(loc)+")", func(t *testing.T) {
			id := add(ServerEntry{Name: "pgnoslot" + strconv.Itoa(len(loc)), Flavor: FlavorPostgres, SourceDSN: "postgres://<redacted>/db", BaselineDir: loc})
			code, body, rep := get(id)
			if code != 200 || len(rep.Steps) == 0 || strings.Contains(names(rep), "backup") {
				t.Fatalf("code = %d, steps = %s, body = %s", code, names(rep), body)
			}
		})
	}
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

// TestHandleFirstRunWithBackupsOff is #1677 through the real router: on a
// daemon that cannot create full backups, the backup step is listed and says
// so, where it used to be left out. The location sentence rides along only
// when the server also has none of its own.
func TestHandleFirstRunWithBackupsOff(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{status: MonitorStatus{State: "stopped"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name        string
		entry       ServerEntry
		wantLocText bool
	}{
		{"with a location", ServerEntry{Name: "loc", SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/", BaselineDir: t.TempDir()}, false},
		{"with none", ServerEntry{Name: "noloc", SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/"}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			e, err := srv.cm.reg.Add(c.entry)
			if err != nil {
				t.Fatal(err)
			}
			rec, body := doServersReq(t, srv, "GET", "/api/servers/"+e.ID+"/first-run", "")
			var rep FirstRunReport
			if rec.Code != 200 || json.Unmarshal(body, &rep) != nil || len(rep.Steps) == 0 {
				t.Fatalf("code = %d, body = %s", rec.Code, body)
			}
			s := rep.Steps[len(rep.Steps)-1]
			if s.Name != "Take the first backup" || s.State != firstRunWaiting || !strings.Contains(s.Detail, "turned off") {
				t.Fatalf("backup step = %+v", s)
			}
			if got := strings.Contains(s.Fix, "backup location"); got != c.wantLocText {
				t.Errorf("fix names the location = %v, want %v: %q", got, c.wantLocText, s.Fix)
			}
		})
	}
	// Off or on, a PostgreSQL server with no slot gets no backup step: capture
	// cannot run for it, and turning backups on would not help.
	t.Run("PostgreSQL with no slot", func(t *testing.T) {
		e, err := srv.cm.reg.Add(ServerEntry{Name: "pgnoslot", Flavor: FlavorPostgres, SourceDSN: "postgres://<redacted>/db"})
		if err != nil {
			t.Fatal(err)
		}
		rec, body := doServersReq(t, srv, "GET", "/api/servers/"+e.ID+"/first-run", "")
		var rep FirstRunReport
		if rec.Code != 200 || json.Unmarshal(body, &rep) != nil || len(rep.Steps) == 0 {
			t.Fatalf("code = %d, body = %s", rec.Code, body)
		}
		if strings.Contains(string(body), "backup") {
			t.Fatalf("backup step listed: %s", body)
		}
	})
}
