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
	cases := map[string]any{
		"two": BaselineStatus{State: "succeeded", Published: true, Tables: 1,
			NewTables: []string{"demo.devices", "demo.orders"}},
		"one":     scheduleRunFromRecord(&BaselineRunRecord{Kind: BaselineRunRefresh, NewTables: []string{"demo.orders"}}),
		"capped":  BaselineStatus{State: "succeeded", NewTables: many, NewTablesOmitted: 30},
		"onlyOut": BaselineStatus{State: "succeeded", NewTablesOmitted: 3},
		"unchecked": BaselineStatus{State: "succeeded",
			NewTablesUnchecked: ScrubReason("could not ask the source which tables it has: dial tcp 10.0.0.5:3306: connect: connection refused")},
		"none": BaselineStatus{State: "succeeded", Tables: 6},
	}
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	js := readAsset(t, "app.js")
	script := functionBody(t, js, "function newTablesNote(") + "\n" +
		"const cases = " + string(arg) + ";\nconst out = {};\n" +
		"for (const [k, v] of Object.entries(cases)) out[k] = newTablesNote(v);\n" +
		"out.nil = newTablesNote(null);\nprocess.stdout.write(JSON.stringify(out));\n"
	var got map[string]*struct {
		Warn  bool   `json:"warn"`
		Count int    `json:"count"`
		Title string `json:"title"`
		Text  string `json:"text"`
	}
	raw := runNodeNewTables(t, script)
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	want := map[string]string{
		"two": "2 tables are not in your copy yet: demo.devices, demo.orders. They were created on your database after the snapshot " +
			"this update started from. They join the copy at the next full snapshot.",
		"one": "1 table is not in your copy yet: demo.orders. It was created on your database after the snapshot " +
			"this update started from. It joins the copy at the next full snapshot.",
		"capped": "50 tables are not in your copy yet: " + strings.Join(many, ", ") + " and 30 more. They were created on your database " +
			"after the snapshot this update started from. They join the copy at the next full snapshot.",
		"onlyOut": "3 tables are not in your copy yet. They were created on your database after the snapshot " +
			"this update started from. They join the copy at the next full snapshot.",
		"unchecked": "Could not check your database for tables created since the previous snapshot, so this snapshot may be missing some. " +
			"Reason: could not ask the source which tables it has: dial tcp 10.0.0.5:3306: connect: connection refused",
	}
	for k, w := range want {
		if got[k] == nil || got[k].Text != w {
			t.Errorf("%s:\n got %+v\nwant %q", k, got[k], w)
		}
		if got[k] != nil && strings.ContainsAny(got[k].Text, "\u2014\u2013") {
			t.Errorf("%s: dash in the copy: %q", k, got[k].Text)
		}
	}
	if got["two"].Title != "2 tables are not in your copy yet" || !got["two"].Warn || got["unchecked"].Warn {
		t.Errorf("title/tone: two %+v unchecked %+v", got["two"], got["unchecked"])
	}
	if got["none"] != nil || got["nil"] != nil {
		t.Errorf("a run with nothing left out says something: none %+v nil %+v", got["none"], got["nil"])
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
	cases := map[string]c{
		"scheduled": {"input": c{"coverage": cov, "server": registry, "schema": c{"state": "idle"}, "uncaptured": c{},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": sched(runDTO(&BaselineRunRecord{
				Kind: BaselineRunRefresh, SnapshotTime: "2026-10-01T09:05:00Z", FinishedAt: "2026-10-01T09:05:02Z", Tables: 1,
				NewTables: []string{"demo.devices", "demo.orders"}}))}}},
		"unscheduled-current": {"input": c{"coverage": cov, "server": registry, "schema": c{"state": "idle"}, "uncaptured": c{},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "refresh": status(BaselineStatus{State: "succeeded",
				At: "2026-10-01T09:05:00Z", FinishedAt: "2026-10-01T09:05:02Z", NewTables: []string{"demo.orders"}})}}},
		"unscheduled-superseded": {"input": c{"coverage": cov, "server": registry, "schema": c{"state": "idle"}, "uncaptured": c{},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "refresh": status(BaselineStatus{State: "succeeded",
				At: "2026-10-01T08:00:00Z", NewTables: []string{"demo.orders"}})}}},
		"unchecked": {"input": c{"coverage": cov, "server": registry, "schema": c{"state": "idle"}, "uncaptured": c{},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": sched(runDTO(&BaselineRunRecord{
				Kind: BaselineRunRefresh, SnapshotTime: "2026-10-01T09:05:00Z", NewTablesUnchecked: "could not ask the source"}))}}},
		// The full read the schedule took for them failed: the newest copy
		// is still the update's, and the live refresh status still says so.
		"full-failed": {"input": c{"coverage": cov, "server": registry, "schema": c{"state": "idle"}, "uncaptured": c{},
			"baselines": c{"configured": true, "snapshots": []any{snap},
				"refresh": status(BaselineStatus{State: "succeeded", At: "2026-10-01T09:05:00Z", NewTables: []string{"demo.orders"}}),
				"schedule": sched(runDTO(&BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-10-01T09:05:03Z", Error: "mydumper: access denied",
					Why: NewTablesWhy(1), WhyCode: BackupWhyCode(NewTablesWhy(1))}))}}},
		// A newer snapshot (a manual full read) than the update's: silent.
		"newer-snapshot": {"input": c{"coverage": cov, "server": registry, "schema": c{"state": "idle"}, "uncaptured": c{},
			"baselines": c{"configured": true, "snapshots": []any{c{"time": "2026-10-01 09:30:00", "age_hours": 0.01, "tables": []string{"demo.kept", "demo.orders"}}},
				"schedule": sched(runDTO(&BaselineRunRecord{Kind: BaselineRunRefresh, SnapshotTime: "2026-10-01T09:05:00Z", NewTables: []string{"demo.orders"}}))}}},
		"after-full": {"input": c{"coverage": cov, "server": registry, "schema": c{"state": "idle"}, "uncaptured": c{},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": sched(runDTO(&BaselineRunRecord{
				Kind: BaselineRunDump, SnapshotTime: "2026-10-01T09:05:00Z", Tables: 3,
				Why: NewTablesWhy(2), WhyCode: BackupWhyCode(NewTablesWhy(2))}))}}},
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

	s := out["scheduled"]
	if p := s.Pieces[bucket]; p.Title != "Your copy" || p.Tone != "warn" || p.Sub != "2 new tables not in it yet" {
		t.Errorf("scheduled: copy box %+v", p)
	}
	k := newCards(s)
	wantText := "2 tables are not in your copy yet: demo.devices, demo.orders. They were created on your database after the snapshot " +
		"this update started from. They join the copy at the next full snapshot."
	if len(k) != 1 || k[0].Title != "2 tables are not in your copy yet" || len(k[0].Lines) != 1 || k[0].Lines[0] != wantText ||
		strings.Join(k[0].Actions, "|") != "Wait for the next full snapshot|Read database now" {
		t.Errorf("scheduled: card %+v", k)
	}
	if !strings.Contains(s.Screen, wantText) {
		t.Errorf("scheduled: the card text is not on screen: %q", s.Screen)
	}

	if k := newCards(out["unscheduled-current"]); len(k) != 1 || out["unscheduled-current"].Pieces[bucket].Tone != "warn" {
		t.Errorf("unscheduled refresh whose snapshot is the newest: cards %+v box %+v", k, out["unscheduled-current"].Pieces[bucket])
	}
	if k := newCards(out["unscheduled-superseded"]); len(k) != 0 || out["unscheduled-superseded"].Pieces[bucket].Tone == "warn" {
		t.Errorf("a refresh a newer snapshot replaced still warns: cards %+v box %+v", k, out["unscheduled-superseded"].Pieces[bucket])
	}
	u := out["unchecked"]
	if len(newCards(u)) != 0 || u.Pieces[bucket].Tone == "warn" || !strings.Contains(u.Pieces[bucket].Sub, "new tables not checked") {
		t.Errorf("unchecked: box %+v cards %+v", u.Pieces[bucket], u.Cards)
	}
	if len(newCards(out["full-failed"])) != 1 || out["full-failed"].Pieces[bucket].Tone != "warn" {
		t.Errorf("a failed full read hid the gap: %+v", out["full-failed"])
	}
	if len(newCards(out["newer-snapshot"])) != 0 || out["newer-snapshot"].Pieces[bucket].Tone == "warn" {
		t.Errorf("a newer snapshot did not silence it: %+v", out["newer-snapshot"])
	}
	if len(newCards(out["after-full"])) != 0 || out["after-full"].Pieces[bucket].Tone == "warn" {
		t.Errorf("the full read that included them still warns: %+v", out["after-full"])
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

// A session with a data profile is not handed the names, on the live status
// and on the schedule's last run; the "not checked" reason names no table and
// stays.
func TestNewTables_withheldFromASessionWithADataProfile(t *testing.T) {
	open := httptest.NewRequest("GET", "/api/servers/a/baseline/restore", nil)
	profiled := open.WithContext(context.WithValue(open.Context(), policyCtxKey{},
		&ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}))
	if !sessionRestricted(profiled) {
		t.Fatal("the fixture session is not restricted; this test covers nothing")
	}
	st := BaselineStatus{State: "succeeded", NewTables: []string{"hr.salaries"}, NewTablesOmitted: 2, NewTablesUnchecked: ""}
	if got := withholdRefusedTables(open, st); len(got.NewTables) != 1 {
		t.Fatalf("a session with no profile lost the list: %+v", got)
	}
	if got := withholdRefusedTables(profiled, st); got.NewTables != nil || got.NewTablesOmitted != 0 {
		t.Errorf("a profiled session was handed the names: %+v", got)
	}
	dto := &backupScheduleDTO{LastRun: &backupScheduleRunDTO{NewTables: []string{"hr.salaries"}, NewTablesOmitted: 1}}
	if cut := withholdScheduleTables(profiled, dto); cut.LastRun.NewTables != nil || cut.LastRun.NewTablesOmitted != 0 {
		t.Errorf("a profiled session was handed the schedule's names: %+v", cut.LastRun)
	}
	unc := BaselineStatus{NewTablesUnchecked: "could not ask the source which tables it has: Error 1045: Access denied for user 'snap'@'10.0.0.5'"}
	if got := withholdRefusedTables(profiled, unc); got.NewTablesUnchecked != "the source could not be asked" {
		t.Errorf("a profiled session was handed the driver error, or lost the fact: %q", got.NewTablesUnchecked)
	}
	if got := withholdRefusedTables(profiled, BaselineStatus{}); got.NewTablesUnchecked != "" {
		t.Errorf("a checked run became unchecked: %q", got.NewTablesUnchecked)
	}
}

// The wire names the page reads, from the bytes.
func TestNewTablesWireNamesMatchTheFrontend(t *testing.T) {
	raw, err := json.Marshal(BaselineStatus{NewTables: []string{"s.t"}, NewTablesOmitted: 1, NewTablesUnchecked: "x"})
	if err != nil {
		t.Fatal(err)
	}
	body := functionBody(t, readAsset(t, "app.js"), "function newTablesNote(")
	for _, key := range []string{"new_tables", "new_tables_omitted", "new_tables_unchecked"} {
		if !strings.Contains(string(raw), `"`+key+`"`) {
			t.Errorf("the status does not serialise %s: %s", key, raw)
		}
		if !strings.Contains(body, "run."+key) {
			t.Errorf("newTablesNote does not read %s", key)
		}
	}
	rec, _ := json.Marshal(scheduleRunFromRecord(&BaselineRunRecord{NewTables: []string{"s.t"}, NewTablesOmitted: 1, NewTablesUnchecked: "x"}))
	for _, key := range []string{`"new_tables":["s.t"]`, `"new_tables_omitted":1`, `"new_tables_unchecked":"x"`} {
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
		Tables: 1, NewTables: []string{"demo.devices", "demo.orders"}})
	stale, _ := json.Marshal(BaselineStatus{State: "succeeded", At: "2026-10-01T08:00:00Z", Tables: 1, NewTables: []string{"demo.orders"}})
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
		"this update started from. They join the copy at the next full snapshot."
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
