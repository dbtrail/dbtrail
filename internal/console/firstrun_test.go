package console

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestFirstRunSteps is #1606: the first-run step list says, in order, what
// has been done and what is happening now, from the supervisor's state and
// what the server's own index database holds. A step that has not started is
// waiting, never failed; a failure lands on the first step not done and says
// what to do. None of these cases lists a backup step, so each is complete
// once one change is indexed; the backup step's rule (#1801) is
// TestFirstRunStaysUntilASnapshotExists.
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
}

// TestFirstRunStaysUntilASnapshotExists is #1801. The list used to end at the
// first captured change and took its backup step with it, so from then on
// nothing on the Overview mentioned backups. It ends when a SNAPSHOT exists
// for the server, which is the ratified rule ("la tira de pasos no desaparece
// hasta que exista un snapshot"), and a captured change is not required: the
// first-change step is skippable on purpose, because nobody can be made to
// touch production to get on. A capture step that failed still keeps the list
// up, and so does a backup step that failed. A server that lists no backup
// step (a PostgreSQL server with no slot or publication) keeps the old rule,
// its first indexed change.
//
// This amends a #1677 subtest, "a backup step that cannot be done does not
// hold the list open", which ended the list at the first change with backups
// off because the card would otherwise "sit on every such Overview for good,
// polling". Staying is now the point (a missing snapshot stays named until it
// exists), and the polling cost moved: the Overview keeps itself current on
// its own, so the list no longer has to end for the page to show new changes.
func TestFirstRunStaysUntilASnapshotExists(t *testing.T) {
	yes := true
	running := firstRunInput{Monitor: MonitorStatus{State: "running", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true}
	failed := firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "binlog not found"}, IndexExists: &yes, SnapshotTaken: true}
	with := func(in firstRunInput, f func(*firstRunInput)) firstRunInput { f(&in); return in }
	cases := []struct {
		name     string
		in       firstRunInput
		states   string
		complete bool
		detail   string // substring of the backup step's detail
	}{
		{"a first change with no snapshot keeps the list",
			with(running, func(in *firstRunInput) { in.EventsIndexed = 1; in.Backup = &BaselineStatus{State: "idle"} }),
			"dddddw", false, ""},
		{"backups off with a first change keeps the list, and says why",
			with(running, func(in *firstRunInput) { in.EventsIndexed = 1; in.BackupOff = true }),
			"dddddw", false, "turned off"},
		{"a snapshot with no first change yet ENDS the list: seeing a change is skippable",
			with(running, func(in *firstRunInput) { in.Backup = &BaselineStatus{State: "idle"}; in.SnapshotExists = true }),
			"ddddrd", true, ""},
		{"a snapshot made elsewhere counts while backups are off",
			with(running, func(in *firstRunInput) { in.BackupOff = true; in.SnapshotExists = true }),
			"ddddrd", true, ""},
		{"a snapshot in the server's own location counts when it has no job of its own",
			with(running, func(in *firstRunInput) { in.BackupNoLocation = true; in.SnapshotExists = true }),
			"ddddrd", true, ""},
		{"a published backup ends the list",
			with(running, func(in *firstRunInput) {
				in.EventsIndexed = 1
				in.Backup = &BaselineStatus{State: "succeeded", Published: true}
			}),
			"dddddd", true, ""},
		{"a capture failure with no snapshot keeps the list, by the plain rule",
			with(failed, func(in *firstRunInput) { in.Backup = &BaselineStatus{State: "idle"} }),
			"dddfww", false, ""},
		{"a backup that FAILED keeps the list up, with its error, though an older snapshot exists",
			with(running, func(in *firstRunInput) {
				in.EventsIndexed = 1
				in.SnapshotExists = true
				in.Backup = &BaselineStatus{State: "failed", LastError: "no space left on device"}
			}),
			"dddddf", false, "no space left on device"},
		{"a snapshot ends the list even where capture FAILED: a dead stream is not this strip's job",
			with(failed, func(in *firstRunInput) { in.SnapshotExists = true; in.Backup = &BaselineStatus{State: "idle"} }),
			"dddfwd", true, ""},
		{"a backup RUNNING outranks an older snapshot, so the list follows it",
			with(running, func(in *firstRunInput) {
				in.EventsIndexed = 1
				in.SnapshotExists = true
				in.Backup = &BaselineStatus{State: "running"}
			}),
			"dddddr", false, ""},
		{"a run that published and only failed to upload is still a backup",
			with(running, func(in *firstRunInput) {
				in.EventsIndexed = 1
				in.Backup = &BaselineStatus{State: "failed", Published: true, LastError: "upload refused"}
			}),
			"dddddd", true, ""},
		{"a backup running says the location could not be checked",
			with(running, func(in *firstRunInput) {
				in.EventsIndexed = 1
				in.Backup = &BaselineStatus{State: "running"}
				in.SnapshotCheckError = "list s3://b/p: AccessDenied"
			}),
			"dddddr", false, "Could not check for an existing backup: list s3://b/p: AccessDenied"},
		{"a backup that failed says both its own error and the location it could not check",
			with(running, func(in *firstRunInput) {
				in.EventsIndexed = 1
				in.Backup = &BaselineStatus{State: "failed", LastError: "mydumper not found"}
				in.SnapshotCheckError = "read baseline directory: permission denied"
			}),
			"dddddf", false, "mydumper not found Could not check for an existing backup: read baseline directory: permission denied"},
		{"a snapshot with changes and a later failure ends the list: every step is done",
			with(failed, func(in *firstRunInput) {
				in.EventsIndexed = 5
				in.StreamStarted = true
				in.SnapshotExists = true
				in.Backup = &BaselineStatus{State: "idle"}
			}),
			"dddddd", true, ""},
		{"a location that could not be read is not a no: the list stays and says so",
			with(running, func(in *firstRunInput) {
				in.EventsIndexed = 1
				in.Backup = &BaselineStatus{State: "idle"}
				in.SnapshotCheckError = "list s3://b/p: AccessDenied"
			}),
			"dddddw", false, "Could not check for an existing backup: list s3://b/p: AccessDenied"},
		{"a location that could not be read, with backups off, keeps both reasons",
			with(running, func(in *firstRunInput) {
				in.BackupOff = true
				in.SnapshotCheckError = "read baseline directory: permission denied"
			}),
			"ddddrw", false, "turned off"},
		{"no backup step listed: the first change still ends the list",
			with(running, func(in *firstRunInput) { in.EventsIndexed = 1 }),
			"ddddd", true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := firstRunSteps(c.in)
			var states strings.Builder
			for _, s := range got.Steps {
				states.WriteByte(s.State[0])
			}
			if states.String() != c.states || got.Complete != c.complete {
				t.Fatalf("states = %s, complete = %v, want %s and %v (%+v)", states.String(), got.Complete, c.states, c.complete, got.Steps)
			}
			last := got.Steps[len(got.Steps)-1]
			if c.detail != "" && !strings.Contains(last.Detail, c.detail) {
				t.Errorf("backup step detail = %q, want it to carry %q", last.Detail, c.detail)
			}
			if last.Name == "Take the first full DB snapshot" && last.State == firstRunDone && (last.Detail != "" || last.Fix != "") {
				t.Errorf("a done backup step still carries a reason or a fix: %+v", last)
			}
		})
	}
	// Both reasons at once: the check error rides along with the off reason
	// instead of replacing it.
	got := firstRunSteps(with(running, func(in *firstRunInput) {
		in.BackupOff = true
		in.SnapshotCheckError = "read baseline directory: permission denied"
	}))
	if d := got.Steps[len(got.Steps)-1].Detail; !strings.Contains(d, "turned off") || !strings.Contains(d, "permission denied") {
		t.Errorf("detail = %q, want the off reason and the check error", d)
	}
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
			if s.Name != "Take the first full DB snapshot" || s.State != firstRunWaiting {
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
	srv, ctrl := newBaselineTriggerServer(t)
	srv.monitorCtrl.(*stubMonitorCtrl).status = MonitorStatus{State: "stopped"}
	// No test here reaches a real bucket: every location is answered here.
	var listed []string
	srv.snapshotLister = func(_ context.Context, source string) (bool, error) {
		listed = append(listed, source)
		return false, nil
	}
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
		if s := rep.Steps[5]; s.Name != "Take the first full DB snapshot" || s.State != firstRunWaiting || !strings.Contains(s.Detail, "no backup location of its own") {
			t.Fatalf("backup step = %+v", s)
		}
		if !strings.Contains(body, `"name":"Create the index database"`) || !strings.Contains(body, `"state":"waiting"`) {
			t.Errorf("the wire shape changed: %s", body)
		}
	})
	t.Run("a MySQL server with a location gets its backup job's state", func(t *testing.T) {
		id := add(ServerEntry{Name: "myloc", SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/", BaselineS3: "s3://b/p"})
		_, body, rep := get(id)
		if s := rep.Steps[len(rep.Steps)-1]; s.Name != "Take the first full DB snapshot" || s.Fix != "Create one on the "+PageSnapshots+" page." {
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
		if code != 200 || strings.Contains(names(rep), "Read the table structure") || !strings.Contains(names(rep), "Take the first full DB snapshot") {
			t.Fatalf("code = %d, steps = %s, body = %s", code, names(rep), body)
		}
	})
	t.Run("an index that cannot be read answers with its error, scrubbed, and no steps", func(t *testing.T) {
		listed = nil
		id := add(ServerEntry{Name: "unreach", DSN: "idx:idxpw@tcp(127.0.0.1:1)/bintrail_idx_unreach", SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/", BaselineS3: "s3://b/unreach"})
		code, body, rep := get(id)
		if code != 200 || len(rep.Steps) != 0 || !strings.Contains(body, `"check_error":`) || !strings.Contains(rep.CheckError, "127.0.0.1:1") || strings.Contains(body, "idxpw") {
			t.Fatalf("code = %d, body = %s", code, body)
		}
		if len(listed) != 0 {
			t.Errorf("a report that claims no step still listed the backup location: %v", listed)
		}
	})
	// The server's own locations are read even while capture has not started,
	// because a snapshot ends this list whatever the capture steps are doing.
	// What keeps that from costing a listing every few seconds is the
	// per-server window (TestSnapshotCheckIsReused), not a gate here.
	t.Run("the backup locations are read even before capture starts, and once per window", func(t *testing.T) {
		listed = nil
		dir := t.TempDir()
		id := add(ServerEntry{Name: "pending", SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/", BaselineDir: dir, BaselineS3: "s3://b/pending"})
		code, body, rep := get(id)
		if code != 200 || rep.Complete {
			t.Fatalf("code = %d, complete = %v, body = %s", code, rep.Complete, body)
		}
		if strings.Join(listed, ",") != dir+",s3://b/pending" {
			t.Fatalf("read %v, want the server's own folder and bucket", listed)
		}
		get(id)
		get(id)
		if strings.Join(listed, ",") != dir+",s3://b/pending" {
			t.Errorf("three requests in a row read %v, want one pass over the locations", listed)
		}
	})
	t.Run("a backup this process published needs no listing and is done", func(t *testing.T) {
		listed = nil
		ctrl.status = BaselineStatus{State: "succeeded", Published: true}
		defer func() { ctrl.status = BaselineStatus{State: "idle"} }()
		id := add(ServerEntry{Name: "published", SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/", BaselineS3: "s3://b/published"})
		_, body, rep := get(id)
		s := rep.Steps[len(rep.Steps)-1]
		if s.State != firstRunDone || len(listed) != 0 {
			t.Fatalf("backup step = %+v, listed %v, body = %s", s, listed, body)
		}
	})
}

// TestSnapshotCheckIsReused is #1801: the Getting started list is polled
// every few seconds while it shows, and a snapshot ends it whatever the
// capture steps are doing, so the locations have to be read while it shows.
// One answer is reused for a minute, which keeps an S3 location to one
// listing a minute per server instead of one every three seconds. An edit to
// the locations is read again rather than answered from the old place, and a
// backup this process published is never read for at all.
func TestSnapshotCheckIsReused(t *testing.T) {
	srv, _ := newBaselineTriggerServer(t)
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	srv.snapshotNow = func() time.Time { return now }
	var read []string
	srv.snapshotLister = func(_ context.Context, src string) (bool, error) {
		read = append(read, src)
		return false, nil
	}
	e := ServerEntry{ID: "srv1", BaselineDir: "/backups/one"}
	ask := func() firstRunInput {
		var in firstRunInput
		srv.checkOwnSnapshot(context.Background(), e, &in)
		return in
	}
	ask()
	ask()
	ask()
	if len(read) != 1 {
		t.Fatalf("three asks within the minute read %v, want one read", read)
	}
	now = now.Add(snapshotCheckTTL - time.Second)
	ask()
	if len(read) != 1 {
		t.Errorf("an ask just inside the window read again: %v", read)
	}
	now = now.Add(2 * time.Second)
	ask()
	if len(read) != 2 {
		t.Errorf("an ask past the window did not read again: %v", read)
	}
	// A different server with the same folder is its own answer, and an edit
	// to this server's locations is read rather than reused.
	read = nil
	other := ServerEntry{ID: "srv2", BaselineDir: "/backups/one"}
	srv.checkOwnSnapshot(context.Background(), other, &firstRunInput{})
	e.BaselineS3 = "s3://b/p"
	ask()
	if len(read) != 3 {
		t.Errorf("read %v; want the other server read on its own and the edited entry read again (folder then folder+bucket)", read)
	}
	// One entry per server, by construction: an edited location replaces the
	// answer rather than leaving the old one behind for the life of the
	// process.
	if n := len(srv.snapshotChecks); n != 2 {
		t.Errorf("two servers left %d remembered answers, want one each: %+v", n, srv.snapshotChecks)
	}
	// A backup that IS there is remembered too, which is the case that costs
	// the most: a capture step that failed keeps this list up for as long as
	// the operator leaves the page open, and every ask would otherwise list
	// the bucket again.
	srv.snapshotChecks = nil
	read = nil
	srv.snapshotLister = func(_ context.Context, src string) (bool, error) {
		read = append(read, src)
		return true, nil
	}
	if found := ask(); !found.SnapshotExists {
		t.Fatalf("the first ask = %+v, want a backup found", found)
	}
	second := ask()
	if !second.SnapshotExists || len(read) != 1 {
		t.Errorf("the second ask = %+v after reading %v, want the same answer from one read", second, read)
	}
	// The answer itself is what comes back from the cache, error and all.
	srv.snapshotChecks = nil
	srv.snapshotLister = func(context.Context, string) (bool, error) { return false, errors.New("AccessDenied") }
	first := ask()
	srv.snapshotLister = func(context.Context, string) (bool, error) { t.Error("read again inside the window"); return true, nil }
	again := ask()
	if first.SnapshotCheckError == "" || again.SnapshotCheckError != first.SnapshotCheckError || again.SnapshotExists {
		t.Errorf("the reused answer is %+v, want the same refusal as %+v", again, first)
	}
}

// TestCheckOwnSnapshot drives the server's reading of a server's OWN backup
// locations (#1801): both are read, in order, until one holds a snapshot; an
// error on one never hides a snapshot on the other; every error is reported
// and none is taken for "no snapshot"; and a backup this process published
// needs no reading at all.
func TestCheckOwnSnapshot(t *testing.T) {
	srv, _ := newBaselineTriggerServer(t)
	dir := t.TempDir()
	for _, c := range []struct {
		name      string
		entry     ServerEntry
		in        firstRunInput
		lister    func(context.Context, string) (bool, error)
		wantRead  []string
		wantFound bool
		wantErr   string
	}{
		{"both locations, in order, when neither holds one",
			ServerEntry{BaselineDir: dir, BaselineS3: "s3://b/p"}, firstRunInput{},
			func(context.Context, string) (bool, error) { return false, nil },
			[]string{dir, "s3://b/p"}, false, ""},
		{"the folder holds one: the bucket is not read",
			ServerEntry{BaselineDir: dir, BaselineS3: "s3://b/p"}, firstRunInput{},
			func(_ context.Context, src string) (bool, error) { return src == dir, nil },
			[]string{dir}, true, ""},
		{"the bucket holds one, the folder being empty",
			ServerEntry{BaselineDir: dir, BaselineS3: "s3://b/p"}, firstRunInput{},
			func(_ context.Context, src string) (bool, error) { return strings.HasPrefix(src, "s3://"), nil },
			[]string{dir, "s3://b/p"}, true, ""},
		{"a folder that cannot be read does not hide a snapshot in the bucket",
			ServerEntry{BaselineDir: dir, BaselineS3: "s3://b/p"}, firstRunInput{},
			func(_ context.Context, src string) (bool, error) {
				if strings.HasPrefix(src, "s3://") {
					return true, nil
				}
				return false, errors.New("read baseline directory: permission denied")
			},
			[]string{dir, "s3://b/p"}, true, ""},
		{"neither could be read: both reasons, and never a no",
			ServerEntry{BaselineDir: dir, BaselineS3: "s3://b/p"}, firstRunInput{},
			func(_ context.Context, src string) (bool, error) { return false, errors.New("cannot read " + src) },
			[]string{dir, "s3://b/p"}, false, "cannot read " + dir + "; cannot read s3://b/p"},
		{"a backup this process published: nothing is read",
			ServerEntry{BaselineDir: dir}, firstRunInput{Backup: &BaselineStatus{State: "succeeded", Published: true}},
			func(context.Context, string) (bool, error) { return false, nil },
			nil, false, ""},
		{"a run that published and failed to upload: nothing is read either",
			ServerEntry{BaselineDir: dir}, firstRunInput{Backup: &BaselineStatus{State: "failed", Published: true}},
			func(context.Context, string) (bool, error) { return false, nil },
			nil, false, ""},
		{"a backup that failed outright: the locations still answer for an older one",
			ServerEntry{BaselineDir: dir}, firstRunInput{Backup: &BaselineStatus{State: "failed", LastError: "disk full"}},
			func(context.Context, string) (bool, error) { return true, nil },
			[]string{dir}, true, ""},
		{"a PostgreSQL server with no slot lists no backup step, so nothing is read",
			ServerEntry{Flavor: FlavorPostgres, BaselineDir: dir}, firstRunInput{},
			func(context.Context, string) (bool, error) { return true, nil },
			nil, false, ""},
		{"an index that could not be read claims nothing, so nothing is read",
			ServerEntry{BaselineDir: dir}, firstRunInput{CheckError: "Error 1045: Access denied"},
			func(context.Context, string) (bool, error) { return true, nil },
			nil, false, ""},
		{"a server with no location of its own has nothing to read",
			ServerEntry{}, firstRunInput{},
			func(context.Context, string) (bool, error) { return true, nil },
			nil, false, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			var read []string
			// Each case is its own question; the reuse window is
			// TestSnapshotCheckIsReused.
			srv.snapshotChecks = nil
			srv.snapshotLister = func(ctx context.Context, src string) (bool, error) {
				read = append(read, src)
				return c.lister(ctx, src)
			}
			in := c.in
			srv.checkOwnSnapshot(context.Background(), c.entry, &in)
			if strings.Join(read, ",") != strings.Join(c.wantRead, ",") {
				t.Errorf("read %v, want %v", read, c.wantRead)
			}
			if in.SnapshotExists != c.wantFound {
				t.Errorf("SnapshotExists = %v, want %v", in.SnapshotExists, c.wantFound)
			}
			if in.SnapshotCheckError != c.wantErr {
				t.Errorf("SnapshotCheckError = %q, want %q", in.SnapshotCheckError, c.wantErr)
			}
		})
	}
}

// TestHasCompleteSnapshot reads REAL folders with the real listing (#1801):
// a complete snapshot is found, one still being written is not, a folder the
// first backup has not created yet is "no snapshot" and not a failure to
// check, and a snapshot whose contents cannot be read is no answer at all.
func TestHasCompleteSnapshot(t *testing.T) {
	snapshot := func(root, marker string) {
		t.Helper()
		dir := filepath.Join(root, "2026-09-22T11-52-50Z")
		if err := os.MkdirAll(filepath.Join(dir, "shop"), 0o755); err != nil {
			t.Fatal(err)
		}
		for _, f := range []string{filepath.Join(dir, "shop", "orders.parquet"), filepath.Join(dir, marker)} {
			if err := os.WriteFile(f, nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	done, partial, empty := t.TempDir(), t.TempDir(), t.TempDir()
	missing := filepath.Join(t.TempDir(), "not-created-yet")
	locked := t.TempDir()
	snapshot(done, "_SUCCESS")
	snapshot(partial, "_INCOMPLETE")
	// A snapshot whose schema folder cannot be opened: the listing skips it,
	// finds nothing else, and that is no answer rather than a "no".
	snapshot(locked, "_SUCCESS")
	lockedSchema := filepath.Join(locked, "2026-09-22T11-52-50Z", "shop")
	if err := os.Chmod(lockedSchema, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(lockedSchema, 0o755) })
	_, lockedErr := os.ReadDir(lockedSchema)

	for _, c := range []struct {
		name       string
		dir        string
		want       bool
		wantErr    string
		skipAsRoot bool
	}{
		{"a complete snapshot", done, true, "", false},
		{"a snapshot still being written", partial, false, "", false},
		{"a folder with no snapshot in it", empty, false, "", false},
		{"a folder the first backup has not created yet", missing, false, "", false},
		{"a snapshot whose contents cannot be read", locked, false, "could not be read", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.skipAsRoot && lockedErr == nil {
				t.Skip("this user reads a folder with no permissions (root)")
			}
			got, err := hasCompleteSnapshot(context.Background(), c.dir)
			if got != c.want {
				t.Errorf("hasCompleteSnapshot = %v, want %v (err %v)", got, c.want, err)
			}
			switch {
			case c.wantErr == "" && err != nil:
				t.Errorf("err = %v, want none", err)
			case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
				t.Errorf("err = %v, want one carrying %q", err, c.wantErr)
			}
		})
	}
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
			if s.Name != "Take the first full DB snapshot" || s.State != firstRunWaiting || !strings.Contains(s.Detail, "turned off") {
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
