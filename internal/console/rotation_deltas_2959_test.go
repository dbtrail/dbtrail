package console

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// deltasServer is a watch console with table deltas on, the schedule loop
// present, and the daemon's rule for the retention: more than two update
// intervals plus one hour, comfortable one hour over that, and no rule at all
// for updates a day apart or more. The rule itself is pinned against the line
// the refresh ends chains on in consoleapp
// (TestMinRetainForTableDeltas_isTheLineTheFoldEndsChainsOn_2959); here it is
// the contract the form refuses by.
func deltasServer(t *testing.T, schedules ...string) *Server {
	t.Helper()
	srv, _ := newSupervisorServer(t)
	srv.backupSchedules = &stubScheduleReporter{full: true}
	srv.rotationDefaults = RotationDefaults{Retain: "30d", Interval: "1h", AddFuture: 3, Enabled: true}
	srv.baselineRefreshDefaults = BaselineRefreshDefaults{TableDeltas: true, TableDeltaMaxAge: 24 * time.Hour,
		MinRetainForTableDeltas: func(every time.Duration) (time.Duration, time.Duration, bool) {
			if every >= 24*time.Hour {
				return 0, 0, false
			}
			return 2*every + time.Hour, 2*every + 2*time.Hour, true
		}}
	names := []string{"orders", "billing", "audit"}
	for i, every := range schedules {
		addScheduled(t, srv, names[i], every)
	}
	return srv
}

func addScheduled(t *testing.T, srv *Server, name, every string) ServerEntry {
	t.Helper()
	e, err := srv.cm.reg.Add(ServerEntry{Name: name, DSN: "u:p@tcp(h:3306)/idx" + name, SourceDSN: "u:p@tcp(s:3306)/",
		BaselineDir: "/b/" + name, BackupSchedule: &BackupSchedule{Every: every}})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func putRotation(t *testing.T, srv *Server, retain, interval string) (int, string) {
	t.Helper()
	rec, body := doServersReq(t, srv, "PUT", "/api/rotation", `{"retain":"`+retain+`","interval":"`+interval+`","add_future":3}`)
	var e struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(body, &e)
	return rec.Code, e.Error
}

func getRotation(t *testing.T, srv *Server) rotationDTO {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/rotation", "")
	if rec.Code != 200 {
		t.Fatalf("GET code=%d body=%s", rec.Code, body)
	}
	var got rotationDTO
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

const deltasOffWords2959 = "turn table deltas off (baseline-table-deltas = false in dbtrail.cnf, or --baseline-table-deltas=false; then restart DBTrail)"

// The refusal sits exactly on the rule: at it and one hour under are refused,
// the first whole hour over it is saved, in hours and in days. The retention
// in use (30d) is none of the values asked for.
func TestRotationUpdate_refusesRetentionWithNoRoomForTableDeltas_2959(t *testing.T) {
	cases := []struct {
		every, retain string
		saved         bool
	}{
		// every 5m: more than 1h10m.
		{"5m", "1h", false},
		{"5m", "2h", true},
		{"5m", "1d", true},
		// every 20m: more than 1h40m.
		{"20m", "1h", false},
		{"20m", "2h", true},
		// every 30m: more than 2h, so 2h itself is on the line.
		{"30m", "1h", false},
		{"30m", "2h", false},
		{"30m", "3h", true},
		// every 6h: more than 13h. An interval longer than some retentions.
		{"6h", "1h", false},
		{"6h", "6h", false},
		{"6h", "13h", false},
		{"6h", "14h", true},
		{"6h", "1d", true},
		// every 23h, the longest whole-hour interval a chain can live
		// through: more than 47h.
		{"23h", "47h", false},
		{"23h", "2d", true},
	}
	for _, c := range cases {
		srv := deltasServer(t, c.every)
		code, msg := putRotation(t, srv, c.retain, "1h")
		if c.saved {
			if code != 200 {
				t.Errorf("every %s, retain %s: code=%d %q, want saved", c.every, c.retain, code, msg)
			}
			if rc, ok := srv.cm.reg.Rotation(); !ok || rc.Retain != c.retain {
				t.Errorf("every %s, retain %s: registry holds %+v", c.every, c.retain, rc)
			}
			continue
		}
		if code != 400 || !strings.Contains(msg, "too short while table deltas are on") {
			t.Errorf("every %s, retain %s: code=%d %q, want the refusal", c.every, c.retain, code, msg)
		}
		if _, ok := srv.cm.reg.Rotation(); ok {
			t.Errorf("every %s, retain %s: a refused retention was saved", c.every, c.retain)
		}
	}
}

// Updates a day apart or more: the refresh ends every chain by age whatever
// the index keeps, so no retention is refused, nothing is painted red, and
// such a server does not raise the limit of one updated every 5 minutes.
func TestRotation_noFloorForUpdatesADayApartOrMore_2959(t *testing.T) {
	for _, every := range []string{"1d", "24h", "2d", "7d"} {
		srv := deltasServer(t, every)
		srv.rotationDefaults.Retain = "48h"
		for _, retain := range []string{"1h", "24h", "10d"} {
			if code, msg := putRotation(t, srv, retain, "1h"); code != 200 {
				t.Errorf("every %s, retain %s: code=%d %q, want saved", every, retain, code, msg)
			}
		}
		if d := getRotation(t, srv).Deltas; d != nil {
			t.Errorf("every %s: the form carries %+v, want nothing", every, d)
		}
		e := srv.cm.reg.List()[0]
		dto := srv.backupScheduleDTO(context.Background(), e, time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC))
		wantNote := ""
		if every == "2d" || every == "7d" {
			wantNote = "With updates more than a day apart, every update writes every table in full: a table's changes are kept beside its file for a day at most."
		}
		if dto.DeltasWarning != "" || dto.DeltasNote != wantNote {
			t.Errorf("every %s: warning %q note %q, want no warning and note %q", every, dto.DeltasWarning, dto.DeltasNote, wantNote)
		}
	}

	// The factory retention with a daily update: saved as asked, no red.
	srv := deltasServer(t, "1d")
	srv.rotationDefaults.Retain = "48h"
	if code, msg := putRotation(t, srv, "47h", "1h"); code != 200 {
		t.Fatalf("47h under a daily update: code=%d %q", code, msg)
	}

	// A daily and a weekly server beside one updated every 5m: the 5m one
	// decides, at its own limit.
	srv = deltasServer(t, "5m", "1d", "7d")
	d := getRotation(t, srv).Deltas
	if d == nil || d.MinRetain != "2h" || d.RecommendedRetain != "3h" || !strings.Contains(d.Note, `Server "orders" updates its copy every 5m`) {
		t.Fatalf("5m beside 1d and 7d: deltas=%+v, want the 5m server's 2h", d)
	}
	if code, msg := putRotation(t, srv, "2h", "1h"); code != 200 {
		t.Fatalf("2h with a 5m, a daily and a weekly server: code=%d %q", code, msg)
	}
	if code, _ := putRotation(t, srv, "1h", "1h"); code != 400 {
		t.Fatalf("1h with a 5m server: code=%d, want refused", code)
	}
}

// The sentences, whole, from real requests.
func TestRotationUpdate_refusalWords_2959(t *testing.T) {
	_, msg := putRotation(t, deltasServer(t, "5m"), "1h", "1h")
	want := `Retention 1h is too short while table deltas are on. Server "orders" updates its copy every 5m, ` +
		`so with less than 2h every update writes every table again in full. Table deltas work well from 3h. ` +
		`Set a longer retention, or first ` + deltasOffWords2959 + `.`
	if msg != want {
		t.Fatalf("refusal:\n got %s\nwant %s", msg, want)
	}

	// The daemon-wide interval sets the limit when it asks for more than any
	// schedule, and is named as what it is. Its shortest retention is also
	// the recommended one, so nothing more is recommended.
	srv := deltasServer(t, "5m")
	srv.baselineRefreshDefaults.RefreshEvery, srv.baselineRefreshDefaults.RefreshEveryRaw = 6*time.Hour, "6h"
	code, msg := putRotation(t, srv, "13h", "1h")
	want = `Retention 13h is too short while table deltas are on. The copies are updated every 6h (--baseline-refresh-interval), ` +
		`so with less than 14h every update writes every table again in full. ` +
		`Set a longer retention, or first ` + deltasOffWords2959 + `.`
	if code != 400 || msg != want {
		t.Fatalf("daemon-wide refusal: code=%d\n got %s\nwant %s", code, msg, want)
	}
	if code, msg := putRotation(t, srv, "14h", "1h"); code != 200 {
		t.Fatalf("14h over a 6h interval: code=%d %q, want saved", code, msg)
	}
}

// The limit and the recommended retention per interval: the first whole hour
// over two intervals plus one hour, and the first from two intervals plus two.
func TestDeltaRetainFloor_shortestAndRecommended_2959(t *testing.T) {
	for every, want := range map[string][2]string{
		"5m": {"2h", "3h"}, "20m": {"2h", "3h"}, "30m": {"3h", "3h"}, "45m": {"3h", "4h"}, "6h": {"14h", "14h"}, "23h": {"48h", "48h"},
	} {
		srv := deltasServer(t, every)
		f, ok := srv.deltaRetainFloor()
		if !ok || f.shortest() != want[0] || f.recommended() != want[1] {
			t.Errorf("every %s: shortest %s recommended %s ok=%v, want %s and %s", every, f.shortest(), f.recommended(), ok, want[0], want[1])
		}
		// The shortest is saved and the hour under it is not.
		if code, msg := putRotation(t, srv, want[0], "1h"); code != 200 {
			t.Errorf("every %s: the shortest retention %s was refused: %q", every, want[0], msg)
		}
	}
}

// The copy updated least often decides, and is the one named.
func TestRotationUpdate_slowestScheduleDecides_2959(t *testing.T) {
	srv := deltasServer(t, "5m", "6h", "30m")
	code, msg := putRotation(t, srv, "13h", "1h")
	if code != 400 || !strings.Contains(msg, `Server "billing" updates its copy every 6h`) || !strings.Contains(msg, "less than 14h") {
		t.Fatalf("code=%d %q, want the 6h schedule's refusal", code, msg)
	}
}

// Everything that asks nothing of the retention saves any retention, as
// before: deltas off, no refresh in this process, no copy on a cadence, a
// PostgreSQL source, a schedule that does not parse or cannot run.
func TestRotationUpdate_noFloorWithoutTableDeltasOnACadence_2959(t *testing.T) {
	off := deltasServer(t, "6h")
	off.baselineRefreshDefaults.TableDeltas = false
	noRefresh := deltasServer(t, "6h")
	noRefresh.baselineRefreshDefaults.MinRetainForTableDeltas = nil
	noLoop := deltasServer(t, "6h")
	noLoop.backupSchedules = nil
	noCadence := deltasServer(t)
	if _, err := noCadence.cm.reg.Add(ServerEntry{Name: "plain", DSN: "u:p@tcp(h:3306)/plain", BaselineDir: "/b/plain"}); err != nil {
		t.Fatal(err)
	}
	for name, srv := range map[string]*Server{"deltas off": off, "no refresh": noRefresh, "no schedule loop": noLoop, "no cadence": noCadence} {
		if code, msg := putRotation(t, srv, "1h", "1h"); code != 200 {
			t.Errorf("%s: code=%d %q, want saved", name, code, msg)
		}
		if d := getRotation(t, srv).Deltas; d != nil {
			t.Errorf("%s: GET carries %+v, want no deltas note", name, d)
		}
	}

	srv := deltasServer(t)
	runs := func(e ServerEntry) bool { _, ok := srv.serverDeltaFloor(e); return ok }
	base := func(name string, sched BackupSchedule) ServerEntry {
		return ServerEntry{ID: name, Name: name, DSN: "u:p@tcp(h:3306)/" + name, SourceDSN: "u:p@tcp(s:3306)/", BaselineDir: "/b/" + name, BackupSchedule: &sched}
	}
	if !runs(base("ok", BackupSchedule{Every: "6h"})) {
		t.Fatal("the control of this test sets no floor: a schedule that runs")
	}
	pg := base("pg", BackupSchedule{Every: "6h"})
	pg.Flavor = FlavorPostgres
	if !pg.IsPostgres() {
		t.Fatal("the PostgreSQL entry of this test is not one")
	}
	if runs(pg) {
		t.Error("a PostgreSQL source set a floor")
	}
	if runs(base("typo", BackupSchedule{Every: "sometimes"})) {
		t.Error("a schedule that does not parse set a floor")
	}
	if runs(ServerEntry{Name: "none", DSN: "u:p@tcp(h:3306)/none", BaselineDir: "/b/none"}) {
		t.Error("a server with no schedule set a floor")
	}
	if runs(base("allfull", BackupSchedule{Every: "6h", FullEvery: "6h"})) {
		t.Error("a schedule whose every run is a full read set a floor")
	}
	if f, ok := srv.serverDeltaFloor(base("weeklyfull", BackupSchedule{Every: "6h", FullEvery: "7d"})); !ok || f.shortest() != "14h" {
		t.Errorf("an update every 6h with a weekly full read: floor %+v ok=%v, want 14h", f, ok)
	}
	// Only full reads are possible: no index to update a copy from, or no
	// local folder to update it in.
	noDir := base("nodir", BackupSchedule{Every: "6h"})
	noDir.BaselineDir, noDir.BaselineS3 = "", "s3://b/snaps"
	if runs(noDir) {
		t.Error("a schedule with no local folder, which can only take full reads, set a floor")
	}

	// A location another server writes too: neither schedule can run
	// (CheckBackupSchedule), so neither sets a floor, and any retention saves.
	shared := deltasServer(t, "6h")
	if d := getRotation(t, shared).Deltas; d == nil {
		t.Fatal("the control of this test sets no floor: one server, its own folder")
	}
	twin, err := shared.cm.reg.Add(ServerEntry{Name: "twin", DSN: "u:p@tcp(h:3306)/twin", SourceDSN: "u:p@tcp(s2:3306)/",
		BaselineDir: "/b/orders", BackupSchedule: &BackupSchedule{Every: "6h"}})
	if err != nil {
		t.Skipf("the registry refuses a second server on the same folder, so this state cannot be saved: %v", err)
	}
	if CheckBackupSchedule(twin, *twin.BackupSchedule, shared.scheduleGates()) == nil {
		t.Skip("a shared folder is not what stops a schedule in this build")
	}
	if d := getRotation(t, shared).Deltas; d != nil {
		t.Errorf("two servers on one folder, neither of which can run: deltas=%+v, want nothing", d)
	}
	if code, msg := putRotation(t, shared, "1h", "1h"); code != 200 {
		t.Errorf("no schedule can run and 1h was refused: code=%d %q", code, msg)
	}

	// The daemon-wide interval over PostgreSQL sources only keeps no chain.
	pgOnly := deltasServer(t)
	pgOnly.baselineRefreshDefaults.RefreshEvery, pgOnly.baselineRefreshDefaults.RefreshEveryRaw = 6*time.Hour, "6h"
	if _, ok := pgOnly.deltaRetainFloor(); !ok {
		t.Error("the daemon-wide interval set no floor with no server saved (the boot index is updated)")
	}
	if _, err := pgOnly.cm.reg.Add(ServerEntry{Name: "pgsrv", Flavor: FlavorPostgres, DSN: "postgres://u:p@h:5432/idx"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := pgOnly.deltaRetainFloor(); ok {
		t.Error("the daemon-wide interval set a floor over PostgreSQL sources only")
	}
	// A daemon-wide interval of a day: no floor either.
	daily := deltasServer(t)
	daily.baselineRefreshDefaults.RefreshEvery, daily.baselineRefreshDefaults.RefreshEveryRaw = 24*time.Hour, "1d"
	if _, ok := daily.deltaRetainFloor(); ok {
		t.Error("a daemon-wide interval of a day set a floor")
	}
}

// A retention already in use and too short does not lock the other fields,
// does not break the form, and can be corrected; it cannot be replaced by
// another one that is too short.
func TestRotation_retentionInUseTooShort_2959(t *testing.T) {
	for _, how := range []string{"saved", "startup"} {
		srv := deltasServer(t, "20m")
		if how == "saved" {
			if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "1h", Interval: "1h", AddFuture: 3}); err != nil {
				t.Fatal(err)
			}
		} else {
			srv.rotationDefaults.Retain = "1h"
		}
		got := getRotation(t, srv)
		want := `The retention in use, 1h, is too short for table deltas: server "orders" updates its copy every 20m, ` +
			`and with less than 2h every update writes every table again in full. Table deltas work well from 3h. ` +
			`Set a longer retention here, or ` + deltasOffWords2959 + `.`
		if got.Retain != "1h" || got.Deltas == nil || !got.Deltas.TooShort || got.Deltas.MinRetain != "2h" ||
			got.Deltas.RecommendedRetain != "3h" || got.Deltas.Note != want {
			t.Fatalf("%s: GET = %+v deltas=%+v\nwant note %s", how, got, got.Deltas, want)
		}
		// Another field changes, the retention does not: saved.
		if code, msg := putRotation(t, srv, "1h", "30m"); code != 200 {
			t.Fatalf("%s: retention unchanged, interval changed: code=%d %q", how, code, msg)
		}
		if rc, _ := srv.cm.reg.Rotation(); rc.Interval != "30m" {
			t.Fatalf("%s: the interval was not saved: %+v", how, rc)
		}
		// Corrected.
		if code, msg := putRotation(t, srv, "2h", "30m"); code != 200 {
			t.Fatalf("%s: the correction was refused: code=%d %q", how, code, msg)
		}
		// And not put back.
		if code, _ := putRotation(t, srv, "1h", "30m"); code != 400 {
			t.Fatalf("%s: 1h replaced a retention with room: code=%d", how, code)
		}
		got = getRotation(t, srv)
		if got.Deltas == nil || got.Deltas.TooShort ||
			got.Deltas.Note != `Table deltas are on. Server "orders" updates its copy every 20m: the shortest retention that can be saved is 2h, `+
				`and that close to the minimum tables are still written in full often. Table deltas work well from 3h.` {
			t.Fatalf("%s: after the correction deltas=%+v", how, got.Deltas)
		}
	}

	// The same retention written another way is the same retention; a longer
	// one that is still too short is not saved.
	srv := deltasServer(t, "6h")
	srv.rotationDefaults.Retain = "12h"
	if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "24h", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	if code, msg := putRotation(t, srv, "1d", "30m"); code != 200 {
		t.Fatalf("1d over a saved 24h: code=%d %q", code, msg)
	}
	srv = deltasServer(t, "6h")
	srv.rotationDefaults.Retain = "12h"
	if code, _ := putRotation(t, srv, "13h", "1h"); code != 400 {
		t.Fatalf("13h replaced 12h in use under a 6h schedule: code=%d", code)
	}
	if code, msg := putRotation(t, srv, "12h", "30m"); code != 200 {
		t.Fatalf("12h in use, saved again: code=%d %q", code, msg)
	}
	if got := getRotation(t, srv).Deltas; got == nil || got.Note != `The retention in use, 12h, is too short for table deltas: `+
		`server "orders" updates its copy every 6h, and with less than 14h every update writes every table again in full. `+
		`Set a longer retention here, or `+deltasOffWords2959+`.` {
		t.Fatalf("12h under 6h: deltas=%+v", got)
	}
}

// A daemon started with rotation off drops nothing on a clock it knows, so
// nothing is called too short; a retention saved there is still held to the
// rule, because the refresh measures chains against it from the next cycle.
func TestRotation_daemonRotationOff_2959(t *testing.T) {
	srv := deltasServer(t, "5m")
	srv.rotationDefaults = RotationDefaults{Retain: "off", Enabled: false}
	if d := getRotation(t, srv).Deltas; d == nil || d.TooShort {
		t.Fatalf("rotation off: deltas=%+v, want the hint and no warning", d)
	}
	if code, _ := putRotation(t, srv, "1h", "1h"); code != 400 {
		t.Fatalf("rotation off: 1h saved under a 5m schedule, code=%d", code)
	}
}

// A typo in another field is named before the retention's own refusal.
func TestRotationUpdate_otherFieldNamedFirst_2959(t *testing.T) {
	code, msg := putRotation(t, deltasServer(t, "5m"), "1h", "later")
	if code != 400 || !strings.Contains(msg, "interval must be a duration") {
		t.Fatalf("code=%d %q, want the interval's own message", code, msg)
	}
}

// The schedule card of the server whose updates pay for it says so; a server
// whose schedule has room says nothing. This is also the door the rotation
// form cannot close: a schedule made longer under a retention already saved.
func TestBackupScheduleDTO_deltasWarning_2959(t *testing.T) {
	srv := deltasServer(t, "5m", "6h")
	if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "12h", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	byName := map[string]*backupScheduleDTO{}
	for _, e := range srv.cm.reg.List() {
		byName[e.Name] = srv.backupScheduleDTO(context.Background(), e, now)
	}
	want := `Every update writes every table again in full. The index keeps 12h of changes, and with an update every 6h ` +
		`table deltas need a retention of 14h or more. Set a longer retention (Settings, Retention), or ` + deltasOffWords2959 + `.`
	if byName["orders"].DeltasWarning != "" || byName["billing"].DeltasWarning != want || byName["orders"].DeltasNote != "" || byName["billing"].DeltasNote != "" {
		t.Fatalf("orders %+v\nbilling %q / %q\nwant billing %q", byName["orders"], byName["billing"].DeltasWarning, byName["billing"].DeltasNote, want)
	}

	// With a recommended retention over the shortest one, it is said.
	srv = deltasServer(t, "20m")
	srv.rotationDefaults.Retain = "1h"
	got := srv.backupScheduleDTO(context.Background(), srv.cm.reg.List()[0], now).DeltasWarning
	want = `Every update writes every table again in full. The index keeps 1h of changes, and with an update every 20m ` +
		`table deltas need a retention of 2h or more. Table deltas work well from 3h. Set a longer retention (Settings, Retention), or ` + deltasOffWords2959 + `.`
	if got != want {
		t.Fatalf("20m under 1h:\n got %q\nwant %q", got, want)
	}
}

// The lines as the page draws them, from the answers of a real server: the
// hint in grey, the warning in red, the note for far-apart updates in grey,
// nothing when there is nothing to say.
func TestRotationDeltasLines_drawn_2959(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	ok := deltasServer(t, "5m")
	short := deltasServer(t, "6h")
	short.rotationDefaults.Retain = "12h"
	weekly := deltasServer(t, "7d")
	none := deltasServer(t)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	sch := func(srv *Server) *backupScheduleDTO {
		return srv.backupScheduleDTO(context.Background(), srv.cm.reg.List()[0], now)
	}
	in := map[string]any{
		"ok": getRotation(t, ok), "short": getRotation(t, short), "none": getRotation(t, none), "weekly": getRotation(t, weekly),
		"sch": sch(short), "schQuiet": sch(ok), "schWeekly": sch(weekly),
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const rot = vm.runInContext("rotationDeltasLine", ctx), sch = vm.runInContext("scheduleDeltasWarning", ctx);
const inp = ` + string(raw) + `;
const show = (n) => n ? n.className.trim() + "|" + n.textContent : "";
console.log(JSON.stringify({ ok: show(rot(inp.ok)), short: show(rot(inp.short)), none: show(rot(inp.none)), weekly: show(rot(inp.weekly)), nothing: show(rot(null)),
  sch: show(sch(inp.sch)), schQuiet: show(sch(inp.schQuiet)), schWeekly: show(sch(inp.schWeekly)), schNone: show(sch(null)) }));
`
	path := filepath.Join(t.TempDir(), "deltas.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got map[string]string
	s := strings.TrimSpace(string(out))
	if err := json.Unmarshal([]byte(s[strings.LastIndex(s, "\n")+1:]), &got); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	want := map[string]string{
		"ok": `form-hint|Table deltas are on. Server "orders" updates its copy every 5m: the shortest retention that can be saved is 2h, ` +
			`and that close to the minimum tables are still written in full often. Table deltas work well from 3h.`,
		"short": `form-msg err|The retention in use, 12h, is too short for table deltas: server "orders" updates its copy every 6h, ` +
			`and with less than 14h every update writes every table again in full. Set a longer retention here, or ` + deltasOffWords2959 + `.`,
		"none": "", "weekly": "", "nothing": "",
		"sch": `form-msg err|Every update writes every table again in full. The index keeps 12h of changes, and with an update every 6h ` +
			`table deltas need a retention of 14h or more. Set a longer retention (Settings, Retention), or ` + deltasOffWords2959 + `.`,
		"schQuiet":  "",
		"schWeekly": `form-hint|With updates more than a day apart, every update writes every table in full: a table's changes are kept beside its file for a day at most.`,
		"schNone":   "",
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s:\n got %q\nwant %q", k, got[k], w)
		}
	}
}
