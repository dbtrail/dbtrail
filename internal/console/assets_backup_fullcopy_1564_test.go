package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestBackupScheduleCard_fullBackupTimetable draws the real card (#1564) over
// DTOs the real endpoint produced, and pins the sentences an operator reads:
// the timetable on the state line, the full backup as the next run with its
// reason, the refusal in red before the slot (in the read-only view too), the
// rate that counts the full backups, and the reason on the last run.
func TestBackupScheduleCard_fullBackupTimetable(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	rep := &stubScheduleReporter{full: true}
	srv, id := newScheduleServer(t, rep)
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir)
	put := func(body string) json.RawMessage {
		t.Helper()
		rec, raw := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", body)
		if rec.Code != 200 {
			t.Fatalf("PUT %s: code=%d body=%s", body, rec.Code, raw)
		}
		var w struct {
			Schedule json.RawMessage `json:"schedule"`
		}
		if err := json.Unmarshal(raw, &w); err != nil {
			t.Fatal(err)
		}
		return w.Schedule
	}
	list := func() json.RawMessage {
		t.Helper()
		_, raw := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
		var w struct {
			Schedule json.RawMessage `json:"schedule"`
		}
		if err := json.Unmarshal(raw, &w); err != nil {
			t.Fatal(err)
		}
		return w.Schedule
	}
	daily := put(`{"every":"1d","at":"03:00","full_every":"1d"}`)
	odd := put(`{"every":"6h","at":"03:00","full_every":"9h"}`)
	weekly := put(`{"every":"6h","at":"03:00","full_every":"7d"}`)
	// The records below are dated before today: they belong to this
	// timetable only if it was set before them.
	backdateFullSince(t, srv, id, "2026-09-01T00:00:00Z")
	// Refused with nothing missed yet: the refusal's own note is the line's.
	rep.full = false
	refusedClean := list()
	rep.full = true
	// A full backup that another job kept from starting, then an ordinary
	// run that finished after it: the miss must still be drawn.
	sched := func(r BaselineRunRecord) BaselineRunRecord { r.ServerID, r.Trigger = id, BaselineRunTriggerScheduled; return r }
	if _, err := srv.baselineHistory.AppendSkip(sched(BaselineRunRecord{Kind: BaselineRunDump,
		SkipReason: FullCopySkipReason("another backup job was running for this server at the scheduled time"),
		StartedAt:  "2026-09-17T03:00:05Z", FinishedAt: "2026-09-17T03:00:05Z"})); err != nil {
		t.Fatal(err)
	}
	if err := srv.baselineHistory.Append(sched(BaselineRunRecord{Kind: BaselineRunRefresh, StartedAt: "2026-09-17T09:00:05Z", FinishedAt: "2026-09-17T09:02:00Z"})); err != nil {
		t.Fatal(err)
	}
	missed := list()
	rep.full = false
	refused := list()
	rep.full = true
	// A full backup of the timetable that started and failed, after the
	// miss: the line says it failed, with its error.
	if err := srv.baselineHistory.Append(sched(BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-09-18T03:00:05Z", FinishedAt: "2026-09-18T03:04:00Z",
		Why: "the schedule takes a full backup every 7d", WhyCode: BackupWhyCodeFullCopy, Error: "mydumper: exit status 2"})); err != nil {
		t.Fatal(err)
	}
	// While it is the last run, the last run's line says it; once an
	// ordinary run ends after it, the full backup's own line does.
	failedIsLast := list()
	if err := srv.baselineHistory.Append(sched(BaselineRunRecord{Kind: BaselineRunRefresh, StartedAt: "2026-09-18T09:00:05Z", FinishedAt: "2026-09-18T09:02:00Z"})); err != nil {
		t.Fatal(err)
	}
	failed := list()
	// The last run a full backup the timetable took, as the history renders it.
	var withRun map[string]any
	if err := json.Unmarshal(weekly, &withRun); err != nil {
		t.Fatal(err)
	}
	withRun["last_run"] = map[string]any{"method": "backup", "ok": true, "finished_at": "2026-09-17T03:20:00Z", "tables": 3,
		"why": "the schedule takes a full backup every 7d", "why_code": BackupWhyCode("the schedule takes a full backup every 7d")}
	lastRun, err := json.Marshal(withRun)
	if err != nil {
		t.Fatal(err)
	}

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const flat = (n, out = []) => { if (!n) return out; if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const byClass = (n, cls, out = []) => { if (!n) return out; if (String(n.className || "").includes(cls)) out.push(n); (n.children || []).forEach((c) => byClass(c, cls, out)); return out; };
const inputs = (n, out = []) => { if (!n) return out; if (n.tag === "input") out.push(n); (n.children || []).forEach((c) => inputs(c, out)); return out; };
const cur = { id: "a", name: "a", kind: "registry", baseline_dir: "/var/lib/bintrail/baselines/a" };
const draw = (caps, sched) => {
  vm.runInContext("capsCache = " + caps + ";", ctx);
  const card = vm.runInContext("backupScheduleCard", ctx)(cur, { configured: true, snapshots: [], schedule: sched });
  const st = byClass(card, "bk-card-state")[0];
  return { state: st.textContent, alarm: String(st.className).includes("alarm"),
    red: byClass(card, "form-msg err").map((n) => flat(n).join("")).filter((s) => s),
    hints: byClass(card, "form-hint").map((n) => flat(n).join("")),
    full: (inputs(card).find((i) => i.attrs["aria-label"] === "Full backup every") || {}).value };
};
console.log(JSON.stringify({
  daily: draw("{ backup_schedule: true }", ` + string(daily) + `),
  odd: draw("{ backup_schedule: true }", ` + string(odd) + `),
  missed: draw("{ backup_schedule: true }", ` + string(missed) + `),
  failed: draw("{ backup_schedule: true }", ` + string(failed) + `),
  failedIsLast: draw("{ backup_schedule: true }", ` + string(failedIsLast) + `),
  refusedClean: draw("{ backup_schedule: true }", ` + string(refusedClean) + `),
  weekly: draw("{ backup_schedule: true }", ` + string(weekly) + `),
  refused: draw("{ backup_schedule: true }", ` + string(refused) + `),
  refusedReadOnly: draw("{ backup_schedule: false }", ` + string(refused) + `),
  lastRun: draw("{ backup_schedule: true }", ` + string(lastRun) + `),
}));
`
	path := filepath.Join(t.TempDir(), "fullcopy.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type drawn struct {
		State string
		Alarm bool
		Red   []string
		Hints []string
		Full  string
	}
	var got map[string]drawn
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for _, name := range []string{"daily", "odd", "weekly", "missed", "failed", "failedIsLast", "refusedClean", "refused", "refusedReadOnly", "lastRun"} {
		d := got[name]
		t.Logf("%s state: %s", name, d.State)
		for _, r := range d.Red {
			t.Logf("%s red:   %s", name, r)
		}
		for _, h := range d.Hints {
			t.Logf("%s hint:  %s", name, h)
		}
	}
	has := func(list []string, want string) bool {
		for _, s := range list {
			if strings.Contains(s, want) {
				return true
			}
		}
		return false
	}

	d := got["daily"]
	if !strings.HasPrefix(d.State, "Every 1d at 03:00 UTC, with a full backup every 1d.") || d.Alarm {
		t.Errorf("daily state = %q (alarm %v)", d.State, d.Alarm)
	}
	if !has(d.Hints, "Next run will take a full backup from your database (the schedule takes a full backup every 1d).") {
		t.Errorf("daily: the next run is not said to be the full backup: %v", d.Hints)
	}
	if d.Full != "1d" {
		t.Errorf("daily: the form does not carry the saved full backup (%q)", d.Full)
	}
	// When the next run IS the full backup, its own "next full backup" line
	// would say the same thing twice; when it is not, it is the only place
	// the date of the next read of the database is written.
	if has(d.Hints, "Next full backup the schedule asks for") {
		t.Errorf("daily: the next full backup is announced twice: %v", d.Hints)
	}
	for _, name := range []string{"weekly", "odd"} {
		if !has(got[name].Hints, "Next full backup the schedule asks for: ") {
			t.Errorf("%s: the date of the next full backup is not written: %v", name, got[name].Hints)
		}
	}

	w := got["weekly"]
	if !strings.HasPrefix(w.State, "Every 6h at 03:00 UTC, with a full backup every 7d.") || w.Alarm {
		t.Errorf("weekly state = %q (alarm %v)", w.State, w.Alarm)
	}
	if !has(w.Hints, "About 120 backups every 30 days at this rate, each a full copy of every table. About 4 are full backups that read your whole database.") {
		t.Errorf("weekly: the rate does not count the full backups: %v", w.Hints)
	}

	// Full backups that do not land on a run are runs of their own.
	if !has(got["odd"].Hints, "About 160 backups every 30 days at this rate, each a full copy of every table. About 80 are full backups that read your whole database.") {
		t.Errorf("odd: the count does not add the full backups between runs: %v", got["odd"].Hints)
	}

	m := got["missed"]
	if !m.Alarm || !strings.HasSuffix(m.State, " The last full backup did not run.") {
		t.Errorf("missed state = %q (alarm %v), want red with the note", m.State, m.Alarm)
	}
	if !has(m.Red, "The full backup due at 2026-09-17 03:00:05 UTC did not run: another backup job was running for this server at the scheduled time. The next one is due at ") {
		t.Errorf("missed: red lines %v", m.Red)
	}
	f := got["failed"]
	if !f.Alarm || !strings.HasSuffix(f.State, " The last full backup failed.") ||
		!has(f.Red, "The full backup that started at 2026-09-18 03:00:05 UTC failed: mydumper: exit status 2. The next one is due at ") {
		t.Errorf("failed: state %q (alarm %v), red %v", f.State, f.Alarm, f.Red)
	}
	if fl := got["failedIsLast"]; strings.Count(strings.Join(fl.Red, "\n"), "mydumper: exit status 2") != 1 {
		t.Errorf("failed and still the last run: the failure is said %d times, want once: %v",
			strings.Count(strings.Join(fl.Red, "\n"), "mydumper: exit status 2"), fl.Red)
	}

	if rc := got["refusedClean"]; !rc.Alarm || !strings.HasSuffix(rc.State, " The full backup cannot run.") || strings.Count(rc.State, "cannot run") != 1 {
		t.Errorf("refused, nothing missed: state = %q (alarm %v), want red ending with the refusal's note once", rc.State, rc.Alarm)
	}
	r := got["refused"]
	// One note on the line however many facts, and a past alarm outranks a
	// forward-looking one (the card's existing rule): this server also has
	// the missed full backup above. Both facts are in the body.
	notes := strings.Count(r.State, "The full backup cannot run.") + strings.Count(r.State, "The last full backup did not run.")
	if !r.Alarm || notes != 1 || !strings.HasSuffix(r.State, " The last full backup did not run.") {
		t.Errorf("refused state = %q (alarm %v), want red with exactly one note, the missed full backup's", r.State, r.Alarm)
	}
	if !has(r.Red, "The full backup due at 2026-09-17 03:00:05 UTC did not run") {
		t.Errorf("refused: the missed full backup is not in the body: %v", r.Red)
	}
	// A refused timetable says none will run; the miss beside it must not
	// then promise the next one.
	for _, line := range r.Red {
		if strings.Contains(line, "The next one is due at") {
			t.Errorf("refused: the card promises the next full backup while saying none will run: %q", line)
		}
	}
	wantRed := "The full backup every 7d reads your database, and creating backups from the web interface is turned off on this daemon " +
		"(BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1). The full backups do not run until that changes; the other scheduled runs still do."
	if !has(r.Red, wantRed) {
		t.Errorf("refused: red lines %v, want %q", r.Red, wantRed)
	}
	if ro := got["refusedReadOnly"]; !ro.Alarm || !has(ro.Red, wantRed) {
		t.Errorf("read-only view of a refused full backup: state %q, red %v", ro.State, ro.Red)
	}

	if !has(got["lastRun"].Hints, "The schedule takes a full backup every 7d.") {
		t.Errorf("the last run's reason is not said as the schedule's own: %v", got["lastRun"].Hints)
	}
	for name, dr := range got {
		for _, s := range append(append([]string{dr.State}, dr.Red...), dr.Hints...) {
			for _, bad := range []string{"—", "undefined", "null", "NaN", "Full backup because the schedule"} {
				if strings.Contains(s, bad) {
					t.Errorf("%s draws %q: %q", name, bad, s)
				}
			}
		}
	}
}

// Saving the card sends the full-backup timetable it shows. The API keeps a
// saved timetable when the field is omitted, so a card that dropped it would
// not delete anything; but it would also never save a change to it, and a
// text search for the payload passed before the field was ever sent.
func TestBackupScheduleCard_saveSendsTheFullBackupTimetable(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
	rec, raw := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"6h","at":"03:00","full_every":"7d"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, raw)
	}
	var w struct {
		Schedule json.RawMessage `json:"schedule"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const find = (n, pred) => { if (!n) return null; if (pred(n)) return n; for (const c of n.children || []) { const f = find(c, pred); if (f) return f; } return null; };
const text = (n) => n ? (n._text || "") + (n.children || []).map(text).join("") : "";
(async () => {
  const calls = [];
  vm.runInContext("capsCache = { backup_schedule: true };", ctx);
  ctx.__calls = calls;
  vm.runInContext("api = async (p, o) => { __calls.push({ path: p, opts: o }); return { schedule: {} }; }; toast = () => {}; renderBaselines = () => {};", ctx);
  const cur = { id: "a", name: "a", kind: "registry", baseline_dir: "/x" };
  const card = vm.runInContext("backupScheduleCard", ctx)(cur, { configured: true, snapshots: [], schedule: ` + string(w.Schedule) + ` });
  const input = find(card, (n) => n.tag === "input" && n.attrs && n.attrs["aria-label"] === "Full backup every");
  input.value = " 3d ";
  const save = find(card, (n) => n.tag === "button" && /Save schedule/.test(text(n)));
  await save.onclick();
  console.log(JSON.stringify(calls));
})().catch((e) => { console.error(e); process.exit(1); });
`
	path := filepath.Join(t.TempDir(), "save.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var calls []struct {
		Path string `json:"path"`
		Opts struct {
			Method string         `json:"method"`
			Body   map[string]any `json:"body"`
		} `json:"opts"`
	}
	if err := json.Unmarshal(out, &calls); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if len(calls) != 1 || calls[0].Opts.Method != "PUT" || calls[0].Opts.Body["full_every"] != "3d" ||
		calls[0].Opts.Body["every"] != "6h" || calls[0].Opts.Body["at"] != "03:00" {
		t.Fatalf("Save sent %+v, want one PUT with every 6h, at 03:00 and full_every 3d", calls)
	}
}

// Backup settings counts the backups that reach S3 the way the Backups page
// counts them: a full backup between two runs is a backup of its own, and a
// refused timetable adds none. The two pages printed different numbers.
func TestS3RetentionCount_includesTheFullBackupTimetable(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	p, err := BackupSchedule{Every: "5h", FullEvery: "1d"}.Parse()
	if err != nil {
		t.Fatal(err)
	}
	want := int(p.BackupsPer30Days())
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const text = (n) => n ? (n._text || "") + (n.children || []).map(text).join("") : "";
const box = vm.runInContext("s3RetentionBox", ctx);
const base = { id: "a", name: "a", source: "server", baseline_s3: "s3://b/backups", schedule_every: "5h", schedule_every_minutes: 300, schedule_full_every: "1d" };
console.log(JSON.stringify({
  full: text(box(base, [base], "")),
  refused: text(box(Object.assign({}, base, { schedule_full_refusal: "off" }), [base], "")),
  none: text(box(Object.assign({}, base, { schedule_full_every: "" }), [base], "")),
}));
`
	path := filepath.Join(t.TempDir(), "s3count.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got map[string]string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	for name, n := range map[string]int{"full": want, "refused": 144, "none": 144} {
		if !strings.Contains(got[name], "About "+strconv.Itoa(n)+" backups every 30 days reach S3") {
			t.Errorf("%s: %q, want the count %d", name, got[name], n)
		}
	}
	if want == 144 {
		t.Fatal("fixture: the full backups between runs add nothing, so the count cannot tell")
	}
}
