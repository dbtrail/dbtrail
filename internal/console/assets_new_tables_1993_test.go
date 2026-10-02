package console

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
)

// #1993: what the pages say about tables created on the database after the
// snapshot an update started from. Every sentence below is PRODUCED by the
// page's own functions from payloads marshalled from the Go types the
// endpoints serve, and compared whole.

func runNodeNewTables(t *testing.T, script string) []byte {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	path := filepath.Join(t.TempDir(), "newtables.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	return out
}

func TestNewTablesNote_words(t *testing.T) {
	many := make([]string, 0, RefusedTablesCap)
	for i := range RefusedTablesCap {
		many = append(many, "app.t"+string(rune('a'+i)))
	}
	// The gate's own refusal text, as FullBackupPossible returns it when the
	// web interface may not create snapshots (the #1993 situation).
	gateErr := FullBackupPossible(ServerEntry{SourceDSN: "u:p@tcp(h:3306)/", BaselineDir: "/b"}, BackupScheduleGates{})
	if gateErr == nil {
		t.Fatal("fixture: the closed gate let a full read through")
	}
	two := []string{"demo.devices", "demo.orders"}
	at := "2026-10-01T09:05:00Z"
	st := func(action, reason string) BaselineStatus {
		return BaselineStatus{State: "succeeded", Published: true, Tables: 1, NewTables: two, NewTablesSnapshot: at,
			NewTablesAction: action, NewTablesActionReason: reason}
	}
	snapAt := func(ts time.Time, tables ...string) map[string]any {
		return map[string]any{"time": ts.Format(consoleTSFormat), "tables": tables}
	}
	same := snapAt(time.Date(2026, 10, 1, 9, 5, 0, 0, time.UTC), "demo.kept")
	later := time.Date(2026, 10, 1, 9, 30, 0, 0, time.UTC)
	type tc struct {
		Run  any `json:"run"`
		Snap any `json:"snap"`
	}
	cases := map[string]tc{
		"full_read":     {st(NewTablesActionFullRead, ""), same},
		"not_possible":  {st(NewTablesActionNotPossible, gateErr.Error()), same},
		"gave_up":       {st(NewTablesActionGaveUp, ""), same},
		"held_per_day":  {st(NewTablesActionNotPossible, NewTablesHeldReason(3, time.Date(2026, 10, 2, 9, 5, 0, 0, time.UTC))), same},
		"still_missing": {st(NewTablesActionStillMissing, ""), same},
		"no_schedule":   {st(NewTablesActionNoSchedule, ""), same},
		"old_record":    {st("", ""), same},
		"one_record": {scheduleRunFromRecord(&BaselineRunRecord{Kind: BaselineRunRefresh, SnapshotTime: at,
			NewTables: []string{"demo.orders"}, NewTablesAction: NewTablesActionFullRead}), same},
		"capped": {BaselineStatus{State: "succeeded", NewTables: many, NewTablesOmitted: 30, NewTablesSnapshot: at,
			NewTablesAction: NewTablesActionNoSchedule}, same},
		"count_only": {BaselineStatus{State: "succeeded", NewTablesOmitted: 3, NewTablesSnapshot: at, NewTablesAction: NewTablesActionFullRead}, same},
		"unchecked": {BaselineStatus{State: "succeeded", NewTablesSnapshot: at,
			NewTablesUnchecked: ScrubReason("could not ask the source which tables it has: dial tcp 10.0.0.5:3306: connect: connection refused")}, same},
		"none": {BaselineStatus{State: "succeeded", Tables: 6}, same},
		// A full read after the update holds one of the two.
		"later_holds_one": {st(NewTablesActionFullRead, ""), snapAt(later, "demo.kept", "demo.orders")},
		// A full read after the update holds both: nothing left to say.
		"later_holds_all": {st(NewTablesActionFullRead, ""), snapAt(later, "demo.kept", "demo.devices", "demo.orders")},
		// A point-in-time restore after the update, built from the old copy.
		"later_restore": {st(NewTablesActionNotPossible, gateErr.Error()), snapAt(later, "demo.kept")},
		"one_no_schedule": {BaselineStatus{State: "succeeded", NewTables: []string{"demo.orders"}, NewTablesSnapshot: at,
			NewTablesAction: NewTablesActionNoSchedule}, same},
	}
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	js := readAsset(t, "app.js")
	script := "const READ_DB_CONFIRM = 'confirm';\n" + functionBody(t, js, "function utcLabel(") + "\n" + functionBody(t, js, "function newTablesNote(") + "\n" +
		"const cases = " + string(arg) + ";\nconst out = {};\n" +
		"for (const [k, v] of Object.entries(cases)) out[k] = newTablesNote(v.run, v.snap);\n" +
		"out.nil = newTablesNote(null, null);\nprocess.stdout.write(JSON.stringify(out));\n"
	type note struct {
		Warn    bool   `json:"warn"`
		Count   int    `json:"count"`
		Title   string `json:"title"`
		Text    string `json:"text"`
		Actions []struct {
			Label   string `json:"label"`
			Run     string `json:"run"`
			Primary bool   `json:"primary"`
		} `json:"actions"`
	}
	var got map[string]*note
	raw := runNodeNewTables(t, script)
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	const head2 = "2 tables are not in your copy yet: demo.devices, demo.orders. They were created on your database after the snapshot this update started from. "
	want := map[string]struct{ text, actions string }{
		"full_read": {head2 + "A full snapshot was started to include them.", "Wait for the full snapshot*"},
		"not_possible": {head2 + "A full snapshot cannot start from here: creating snapshots from the web interface is turned off here " +
			"(BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1). Until that is fixed, they will not join the copy on their own. " +
			"Fix it, or take a full snapshot with the bintrail command line.", "OK*"},
		"held_per_day": {head2 + "A full snapshot is held back for now: DBTrail starts at most 3 full reads a day on its own to include new tables, " +
			"and that many started in the last day; the next one is allowed after 2026-10-02 09:05 UTC. The first update after that time starts one, " +
			"so they join the copy then. To include them sooner, read the database now.", "Read database now*|Later"},
		"gave_up": {head2 + "Full snapshots were started to include them and none finished, so DBTrail stopped trying on its own. " +
			"Check why the last full snapshot failed on the Snapshots page, then take one.", "Read database now*|Later"},
		"still_missing": {head2 + "A full snapshot read your database after that and still did not include them, so another one would not either. " +
			"Check that the server's schema list covers their schema and that the name is spelled the way the database lists it.", "OK*"},
		"no_schedule": {head2 + "Automatic refreshes never read your database in full, so they join the copy only when a full snapshot is taken.",
			"Read database now*|Later"},
		"old_record": {head2 + "They join the copy when a full snapshot is taken.", "Read database now*|Later"},
		"one_record": {"1 table is not in your copy yet: demo.orders. It was created on your database after the snapshot this update started from. " +
			"A full snapshot was started to include it.", "Wait for the full snapshot*"},
		"capped": {"50 tables are not in your copy yet: " + strings.Join(many, ", ") + " and 30 more. They were created on your database " +
			"after the snapshot this update started from. Automatic refreshes never read your database in full, so they join the copy only " +
			"when a full snapshot is taken.", "Read database now*|Later"},
		"count_only": {"3 tables are not in your copy yet. They were created on your database after the snapshot this update started from. " +
			"A full snapshot was started to include them.", "Wait for the full snapshot*"},
		"unchecked": {"Could not check your database for tables created since the previous snapshot, so this snapshot may be missing some. " +
			"Reason: could not ask the source which tables it has: dial tcp 10.0.0.5:3306: connect: connection refused", ""},
		"later_holds_one": {"1 table is not in your copy yet: demo.devices. It was created on your database after the snapshot this update " +
			"started from. It joins the copy when a full snapshot is taken.", "Read database now*|Later"},
		"later_restore": {head2 + "They join the copy when a full snapshot is taken.", "Read database now*|Later"},
		"one_no_schedule": {"1 table is not in your copy yet: demo.orders. It was created on your database after the snapshot this update " +
			"started from. Automatic refreshes never read your database in full, so it joins the copy only when a full snapshot is taken.",
			"Read database now*|Later"},
	}
	for k, w := range want {
		g := got[k]
		if g == nil {
			t.Errorf("%s: no note, want %q", k, w.text)
			continue
		}
		var acts []string
		for _, a := range g.Actions {
			l := a.Label
			if a.Primary {
				l += "*"
			}
			acts = append(acts, l)
		}
		if g.Text != w.text || strings.Join(acts, "|") != w.actions {
			t.Errorf("%s:\n got  %q [%s]\n want %q [%s]", k, g.Text, strings.Join(acts, "|"), w.text, w.actions)
		}
		if strings.ContainsAny(g.Text, "\u2014\u2013") {
			t.Errorf("%s: dash in the copy: %q", k, g.Text)
		}
		// A full snapshot is promised only where one was started.
		if k != "full_read" && k != "one_record" && k != "count_only" && strings.Contains(g.Text, "was started to include") {
			t.Errorf("%s promises a full snapshot that was not started: %q", k, g.Text)
		}
	}
	if got["full_read"].Title != "2 tables are not in your copy yet" || !got["full_read"].Warn || got["unchecked"].Warn {
		t.Errorf("title/tone: %+v / %+v", got["full_read"], got["unchecked"])
	}
	for _, k := range []string{"none", "nil", "later_holds_all"} {
		if got[k] != nil {
			t.Errorf("%s says something: %+v", k, got[k])
		}
	}
}

// The Overview: the copy box turns to a warning with the count, and one card
// names the tables with the two choices the update-blocked card offers. A
// refresh whose snapshot is no longer the newest says nothing (a full read
// after it holds the tables), and "not checked" is a word on the box, never a
// card and never "no new tables".
func TestOverviewFlow_newTables(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	type c = map[string]any
	// The listing's own time shape (consoleTSFormat), not RFC3339: the status's
	// "at" is RFC3339, and the two must still compare equal.
	snap := c{"time": time.Date(2026, 10, 1, 9, 5, 0, 0, time.UTC).Format(consoleTSFormat), "age_hours": 0.05, "tables": []string{"demo.kept"}, "kinds": []string{"dir"}}
	registry := c{"id": "a", "kind": "registry", "has_source": true, "source_host": "db1"}
	cov := c{"freshness": "current", "continuity": "ok", "lag_seconds": 2, "delta_to": "2026-10-01 09:06:00"}
	runDTO := func(r *BaselineRunRecord) any {
		var m any
		raw, _ := json.Marshal(scheduleRunFromRecord(r))
		_ = json.Unmarshal(raw, &m)
		return m
	}
	status := func(st BaselineStatus) any {
		var m any
		raw, _ := json.Marshal(st)
		_ = json.Unmarshal(raw, &m)
		return m
	}
	sched := func(run any) c {
		return c{"every": "5m", "runnable": true, "next_run": "2026-10-01T09:10:00Z", "last_run": run}
	}
	gateErr := FullBackupPossible(ServerEntry{SourceDSN: "u:p@tcp(h:3306)/", BaselineDir: "/b"}, BackupScheduleGates{})
	if gateErr == nil {
		t.Fatal("fixture: the closed gate let a full read through")
	}
	base := func(bl c) c {
		return c{"input": c{"coverage": cov, "server": registry, "schema": c{"state": "idle"}, "uncaptured": c{}, "baselines": bl}}
	}
	cases := map[string]c{
		// The #1993 situation: full reads may not start from the web interface.
		"scheduled-gate-closed": base(c{"configured": true, "snapshots": []any{snap}, "schedule": sched(runDTO(&BaselineRunRecord{
			Kind: BaselineRunRefresh, SnapshotTime: "2026-10-01T09:05:00Z", FinishedAt: "2026-10-01T09:05:02Z", Tables: 1,
			NewTables: []string{"demo.devices", "demo.orders"}, NewTablesAction: NewTablesActionNotPossible, NewTablesActionReason: gateErr.Error()}))}),
		"scheduled-full-read": base(c{"configured": true, "snapshots": []any{snap}, "schedule": sched(runDTO(&BaselineRunRecord{
			Kind: BaselineRunRefresh, SnapshotTime: "2026-10-01T09:05:00Z", Tables: 1,
			NewTables: []string{"demo.devices", "demo.orders"}, NewTablesAction: NewTablesActionFullRead}))}),
		"unscheduled-current": base(c{"configured": true, "snapshots": []any{snap}, "refresh": status(BaselineStatus{State: "succeeded",
			At: "2026-10-01T09:05:00Z", NewTablesSnapshot: "2026-10-01T09:05:00Z", NewTables: []string{"demo.orders"},
			NewTablesAction: NewTablesActionNoSchedule})}),
		// A later snapshot that does not hold the table (a point-in-time
		// restore built from the old copy): still said.
		"later-restore": base(c{"configured": true, "snapshots": []any{c{"time": "2026-10-01 09:30:00", "age_hours": 0.01, "tables": []string{"demo.kept"}}},
			"refresh": status(BaselineStatus{State: "succeeded", NewTablesSnapshot: "2026-10-01T09:05:00Z", NewTables: []string{"demo.orders"},
				NewTablesAction: NewTablesActionNoSchedule})}),
		// The refresh after the reporting one failed: its status keeps the list.
		"refresh-failed-after": base(c{"configured": true, "snapshots": []any{snap}, "refresh": status(BaselineStatus{State: "failed",
			At: "2026-10-01T09:10:00Z", LastError: "capture gap", NewTablesSnapshot: "2026-10-01T09:05:00Z", NewTables: []string{"demo.orders"},
			NewTablesAction: NewTablesActionNotPossible, NewTablesActionReason: gateErr.Error()})}),
		"unchecked": base(c{"configured": true, "snapshots": []any{snap}, "schedule": sched(runDTO(&BaselineRunRecord{
			Kind: BaselineRunRefresh, SnapshotTime: "2026-10-01T09:05:00Z", NewTablesUnchecked: "could not ask the source"}))}),
		// The full read the schedule took for them failed: the newest copy is
		// still the update's, and the live refresh status still says so.
		"full-failed": base(c{"configured": true, "snapshots": []any{snap},
			"refresh": status(BaselineStatus{State: "succeeded", At: "2026-10-01T09:05:00Z", NewTablesSnapshot: "2026-10-01T09:05:00Z",
				NewTables: []string{"demo.orders"}, NewTablesAction: NewTablesActionFullRead}),
			"schedule": sched(runDTO(&BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-10-01T09:05:03Z", Error: "mydumper: access denied",
				Why: NewTablesWhy(1), WhyCode: BackupWhyCode(NewTablesWhy(1))}))}),
		// A newer snapshot holds the table (a full read, scheduled or manual).
		"newer-snapshot-holds-it": base(c{"configured": true, "snapshots": []any{c{"time": "2026-10-01 09:30:00", "age_hours": 0.01, "tables": []string{"demo.kept", "demo.orders"}}},
			"refresh": status(BaselineStatus{State: "succeeded", NewTablesSnapshot: "2026-10-01T09:05:00Z", NewTables: []string{"demo.orders"},
				NewTablesAction: NewTablesActionFullRead})}),
		"after-full": base(c{"configured": true, "snapshots": []any{snap}, "schedule": sched(runDTO(&BaselineRunRecord{
			Kind: BaselineRunDump, SnapshotTime: "2026-10-01T09:05:00Z", Tables: 3,
			Why: NewTablesWhy(2), WhyCode: BackupWhyCode(NewTablesWhy(2))}))}),
	}
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + flowHarnessJS
	path := filepath.Join(t.TempDir(), "flow.js")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out map[string]flowOut
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	const bucket = 4
	newCards := func(o flowOut) []flowCardOut {
		var k []flowCardOut
		for _, c := range o.Cards {
			if c.Kind == "new-tables" {
				k = append(k, c)
			}
		}
		return k
	}

	card := func(name string) (flowPiece, []flowCardOut) {
		o, ok := out[name]
		if !ok {
			t.Fatalf("case %q missing", name)
		}
		return o.Pieces[bucket], newCards(o)
	}
	const two = "2 tables are not in your copy yet: demo.devices, demo.orders. They were created on your database after the snapshot " +
		"this update started from. "
	wantCards := map[string]struct{ sub, text, actions string }{
		"scheduled-gate-closed": {"2 new tables not in it yet", two + "A full snapshot cannot start from here: creating snapshots from the web " +
			"interface is turned off here (BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1). Until that is fixed, they will not join the copy " +
			"on their own. Fix it, or take a full snapshot with the bintrail command line.", "OK"},
		"scheduled-full-read": {"2 new tables not in it yet", two + "A full snapshot was started to include them.", "Wait for the full snapshot"},
		"unscheduled-current": {"1 new table not in it yet", "1 table is not in your copy yet: demo.orders. It was created on your database after " +
			"the snapshot this update started from. Automatic refreshes never read your database in full, so it joins the copy only when a full " +
			"snapshot is taken.", "Read database now|Later"},
		"later-restore": {"1 new table not in it yet", "1 table is not in your copy yet: demo.orders. It was created on your database after " +
			"the snapshot this update started from. It joins the copy when a full snapshot is taken.", "Read database now|Later"},
		"refresh-failed-after": {"1 new table not in it yet", "", ""},
		"full-failed":          {"1 new table not in it yet", "", ""},
	}
	for name, w := range wantCards {
		box, k := card(name)
		// A failed update with no schedule stops the drawing at the copy
		// ("update stopped"), which dims the box as before; the card stays.
		dimmed := name == "refresh-failed-after" && box.Tone == "off"
		if (!dimmed && (box.Tone != "warn" || box.Sub != w.sub)) || len(k) != 1 {
			t.Errorf("%s: box %+v cards %+v", name, box, k)
			continue
		}
		if w.text != "" && (len(k[0].Lines) != 1 || k[0].Lines[0] != w.text || strings.Join(k[0].Actions, "|") != w.actions) {
			t.Errorf("%s:\n got  %q %v\n want %q [%s]", name, k[0].Lines, k[0].Actions, w.text, w.actions)
		}
		if w.text != "" && !strings.Contains(out[name].Screen, w.text) {
			t.Errorf("%s: the card text is not on screen", name)
		}
	}
	if box, k := card("unchecked"); len(k) != 0 || box.Tone == "warn" || !strings.Contains(box.Sub, "new tables not checked") {
		t.Errorf("unchecked: box %+v cards %+v", box, k)
	}
	for _, name := range []string{"newer-snapshot-holds-it", "after-full"} {
		if box, k := card(name); len(k) != 0 || box.Tone == "warn" {
			t.Errorf("%s still warns: box %+v cards %+v", name, box, k)
		}
	}
}

// The full read's reason is shown to every session that sees the schedule,
// so it carries a count and no names; the code classifies it.
func TestNewTablesWhy(t *testing.T) {
	for n, want := range map[int]string{
		1: "new tables were created on your database after the previous snapshot (1 table)",
		7: "new tables were created on your database after the previous snapshot (7 tables)",
	} {
		if got := NewTablesWhy(n); got != want || BackupWhyCode(got) != BackupWhyCodeNewTables {
			t.Errorf("NewTablesWhy(%d) = %q (code %q)", n, got, BackupWhyCode(got))
		}
	}
}

// A session with a data profile is not handed the names on the schedule's
// last run (the one surface of this it can reach: the snapshot listing is
// refused to it outright). It keeps the COUNT, and the fact that the check did
// not run without the driver's error, which can name the source account and
// host.
func TestNewTables_withheldFromASessionWithADataProfile(t *testing.T) {
	open := httptest.NewRequest("GET", "/api/servers/a/backup-schedule", nil)
	profiled := open.WithContext(context.WithValue(open.Context(), policyCtxKey{},
		&ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}))
	if !sessionRestricted(profiled) {
		t.Fatal("the fixture session is not restricted; this test covers nothing")
	}
	mk := func() *backupScheduleDTO {
		return &backupScheduleDTO{LastRun: &backupScheduleRunDTO{NewTables: []string{"hr.salaries", "hr.bonus"}, NewTablesOmitted: 1,
			NewTablesUnchecked: "could not ask the source which tables it has: Error 1045: Access denied for user 'snap'@'10.0.0.5'"}}
	}
	if kept := withholdScheduleTables(open, mk()); len(kept.LastRun.NewTables) != 2 || kept.LastRun.NewTablesOmitted != 1 {
		t.Fatalf("a session with no profile lost the list: %+v", kept.LastRun)
	}
	cut := withholdScheduleTables(profiled, mk())
	if cut.LastRun.NewTables != nil || cut.LastRun.NewTablesOmitted != 3 {
		t.Errorf("a profiled session was handed the names, or lost the count: %+v", cut.LastRun)
	}
	if cut.LastRun.NewTablesUnchecked != "the source could not be asked" {
		t.Errorf("a profiled session was handed the driver error, or lost the fact: %q", cut.LastRun.NewTablesUnchecked)
	}
	none := withholdScheduleTables(profiled, &backupScheduleDTO{LastRun: &backupScheduleRunDTO{}})
	if none.LastRun.NewTablesUnchecked != "" || none.LastRun.NewTablesOmitted != 0 {
		t.Errorf("a checked run with nothing left out changed: %+v", none.LastRun)
	}
}

// The wire names the page reads, from the bytes.
func TestNewTablesWireNamesMatchTheFrontend(t *testing.T) {
	raw, err := json.Marshal(BaselineStatus{NewTables: []string{"s.t"}, NewTablesOmitted: 1, NewTablesUnchecked: "x",
		NewTablesAction: "a", NewTablesActionReason: "r", NewTablesSnapshot: "2026-10-01T09:05:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	body := functionBody(t, readAsset(t, "app.js"), "function newTablesNote(")
	for _, key := range []string{"new_tables", "new_tables_omitted", "new_tables_unchecked", "new_tables_action", "new_tables_action_reason", "new_tables_snapshot"} {
		if !strings.Contains(string(raw), `"`+key+`"`) {
			t.Errorf("the status does not serialise %s: %s", key, raw)
		}
		if !strings.Contains(body, "run."+key) {
			t.Errorf("newTablesNote does not read %s", key)
		}
	}
	rec, _ := json.Marshal(scheduleRunFromRecord(&BaselineRunRecord{NewTables: []string{"s.t"}, NewTablesOmitted: 1, NewTablesUnchecked: "x",
		NewTablesAction: "a", NewTablesActionReason: "r"}))
	for _, key := range []string{`"new_tables":["s.t"]`, `"new_tables_omitted":1`, `"new_tables_unchecked":"x"`, `"new_tables_action":"a"`, `"new_tables_action_reason":"r"`} {
		if !strings.Contains(string(rec), key) {
			t.Errorf("the schedule's last run does not carry %s: %s", key, rec)
		}
	}
}

// The Snapshots page: the schedule card and the refresh panel are the places
// the run is shown, rendered in node from app.js with the run as Go serves it.
func TestNewTablesSnapshotsPageRendered(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	run, _ := json.Marshal(scheduleRunFromRecord(&BaselineRunRecord{Kind: BaselineRunRefresh, StartedAt: "2026-10-01T09:05:00Z",
		FinishedAt: "2026-10-01T09:05:02Z", SnapshotTime: "2026-10-01T09:05:00Z", Tables: 1, NewTables: []string{"demo.devices", "demo.orders"}}))
	refresh, _ := json.Marshal(BaselineStatus{State: "succeeded", At: "2026-10-01T09:05:00Z", FinishedAt: "2026-10-01T09:05:02Z",
		Tables: 1, NewTables: []string{"demo.devices", "demo.orders"}, NewTablesSnapshot: "2026-10-01T09:05:00Z"})
	// Its list is about a snapshot older than the newest, and the newest
	// holds the table: nothing to say.
	stale, _ := json.Marshal(BaselineStatus{State: "succeeded", At: "2026-10-01T08:00:00Z", Tables: 1, NewTables: []string{"demo.kept"},
		NewTablesSnapshot: "2026-10-01T08:00:00Z"})
	snapTime, _ := json.Marshal(time.Date(2026, 10, 1, 9, 5, 0, 0, time.UTC).Format(consoleTSFormat))
	script := renderHarnessJS + `
vm.runInContext("capsCache = { backup_schedule: true, baseline_restore: true, baseline_trigger: false };", ctx);
const texts = (n, out = []) => { if (!n) return out; if (n.tag === "p") { out.push(n.textContent); return out; } for (const c of n.children || []) texts(c, out); return out; };
const cur = { id: "a", name: "a", kind: "registry", baseline_dir: "/var/lib/bintrail/baselines/a" };
const snaps = [{ time: ` + string(snapTime) + `, tables: ["demo.kept"] }];
const sched = { every: "5m", runnable: true, last_run: ` + string(run) + ` };
const panel = (rf) => texts(vm.runInContext("baselinesPanel", ctx)({ configured: true, snapshots: snaps, refresh: rf }, [cur], {}));
console.log(JSON.stringify({
  schedule: texts(vm.runInContext("backupScheduleCard", ctx)(cur, { configured: true, snapshots: snaps, schedule: sched })),
  panel: panel(` + string(refresh) + `),
  stale: panel(` + string(stale) + `),
}));
`
	path := filepath.Join(t.TempDir(), "cards.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct{ Schedule, Panel, Stale []string }
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	want := "2 tables are not in your copy yet: demo.devices, demo.orders. They were created on your database after the snapshot " +
		"this update started from. They join the copy when a full snapshot is taken."
	has := func(lines []string) bool {
		for _, l := range lines {
			if l == want {
				return true
			}
		}
		return false
	}
	if !has(got.Schedule) {
		t.Errorf("schedule card: %q", got.Schedule)
	}
	if !has(got.Panel) {
		t.Errorf("refresh panel: %q", got.Panel)
	}
	for _, l := range got.Stale {
		if strings.Contains(l, "not in your copy yet") {
			t.Errorf("a refresh a newer snapshot replaced still says it: %q", got.Stale)
		}
	}
}
