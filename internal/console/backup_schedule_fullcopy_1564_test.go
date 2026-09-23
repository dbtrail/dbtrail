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

// #1564: a full-copy timetable beside the schedule.

func TestBackupSchedule_ParseFullCopy(t *testing.T) {
	cases := []struct {
		name    string
		in      BackupSchedule
		want    time.Duration
		wantErr string
	}{
		{"none", BackupSchedule{Every: "6h"}, 0, ""},
		{"blank is none", BackupSchedule{Every: "6h", FullEvery: "  "}, 0, ""},
		{"weekly", BackupSchedule{Every: "6h", FullEvery: "7d"}, 7 * 24 * time.Hour, ""},
		{"whitespace tolerated", BackupSchedule{Every: "6h", FullEvery: " 7d "}, 7 * 24 * time.Hour, ""},
		// Equal is every run a full copy: allowed, it is what the operator asked.
		{"as often as the schedule", BackupSchedule{Every: "6h", FullEvery: "6h"}, 6 * time.Hour, ""},
		{"more often than the schedule", BackupSchedule{Every: "6h", FullEvery: "1h"}, 0, "more often than the schedule runs"},
		{"no unit", BackupSchedule{Every: "6h", FullEvery: "7"}, 0, "full backup every:"},
		{"not an interval", BackupSchedule{Every: "6h", FullEvery: "weekly"}, 0, "full backup every:"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.in.Parse()
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.FullEvery != c.want {
				t.Fatalf("FullEvery = %s, want %s", got.FullEvery, c.want)
			}
		})
	}
}

func TestBackupSchedule_NormalizedAndIdentityWithAFullCopy(t *testing.T) {
	n, err := (BackupSchedule{Every: "6h", At: "3:00", FullEvery: " 7d "}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	if n.FullEvery != "7d" {
		t.Fatalf("Normalized full_every = %q, want 7d", n.FullEvery)
	}
	plain := BackupSchedule{Every: "6h", At: "03:00"}
	// A schedule that never had a full copy keeps the identity it always had.
	if plain.Identity() != "6h|03:00" {
		t.Fatalf("identity without a full copy = %q", plain.Identity())
	}
	// Editing only the full copy is an edit: the loop must see it.
	weekly := BackupSchedule{Every: "6h", At: "03:00", FullEvery: "7d"}
	daily := BackupSchedule{Every: "6h", At: "03:00", FullEvery: "1d"}
	if weekly.Identity() == plain.Identity() || weekly.Identity() == daily.Identity() {
		t.Fatalf("identities do not tell the full copy apart: %q %q %q", plain.Identity(), weekly.Identity(), daily.Identity())
	}
	if weekly.Identity() != (BackupSchedule{Every: " 6h", At: "3:00", FullEvery: "7d "}).Identity() {
		t.Fatal("the same full copy spelled differently has a different identity")
	}
}

// The full copies sit on the same fixed grid as the runs, anchored on the
// same At, so a whole multiple lands on a run.
func TestParsedBackupSchedule_fullCopySlots(t *testing.T) {
	p, err := (BackupSchedule{Every: "6h", At: "03:00", FullEvery: "7d"}).Parse()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC) // a Monday
	next := p.NextFullRun(now)
	if next.Weekday() != time.Thursday || next.Hour() != 3 || next.Minute() != 0 || !next.After(now) || next.Sub(now) > 7*24*time.Hour {
		t.Fatalf("next full copy = %s, want the coming Thursday 03:00 UTC (7d from the epoch, a Thursday)", next)
	}
	if !p.SlotAtOrBefore(next).Equal(next) {
		t.Fatalf("a 7d full copy on a 6h schedule at the same At is not on a run's slot: %s", next)
	}
	if got := p.FullSlotAtOrBefore(next.Add(time.Minute)); !got.Equal(next) {
		t.Fatalf("FullSlotAtOrBefore(just after) = %s, want %s", got, next)
	}
	if got := p.FullCopiesPer30Days(); got != 4 {
		t.Fatalf("full copies per 30 days = %d, want 4", got)
	}
	none, _ := (BackupSchedule{Every: "6h"}).Parse()
	if !none.NextFullRun(now).IsZero() || !none.FullSlotAtOrBefore(now).IsZero() || none.FullCopiesPer30Days() != 0 {
		t.Fatal("a schedule without a full copy has full copy slots")
	}
}

func TestFullCopyWhy_hasItsOwnCode(t *testing.T) {
	why := FullCopyWhy(BackupSchedule{Every: "6h", FullEvery: " 7d "})
	if why != "the schedule takes a full backup every 7d" {
		t.Fatalf("why = %q", why)
	}
	if code := BackupWhyCode(why); code != "full_copy" {
		t.Fatalf("code = %q, want full_copy", code)
	}
}

func TestCheckFullCopy(t *testing.T) {
	e := ServerEntry{Name: "wp", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx", SourceDSN: "src:pw@tcp(127.0.0.1:3306)/", BaselineDir: "/b"}
	weekly := BackupSchedule{Every: "6h", FullEvery: "7d"}
	on := BackupScheduleGates{LoopRunning: true, FullBackups: true}
	off := BackupScheduleGates{LoopRunning: true}
	if err := CheckFullCopy(e, BackupSchedule{Every: "6h"}, off); err != nil {
		t.Fatalf("a schedule without a full copy was refused: %v", err)
	}
	if err := CheckFullCopy(e, weekly, on); err != nil {
		t.Fatalf("full backups on: %v", err)
	}
	err := CheckFullCopy(e, weekly, off)
	if err == nil || !strings.Contains(RefusalReason(err), "the full backup every 7d reads your database, and") ||
		!strings.Contains(RefusalReason(err), "BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1") {
		t.Fatalf("full backups off: %v", err)
	}
	// The base schedule itself is fine there (an update can run): the two
	// verdicts are separate on purpose.
	if err := CheckBackupSchedule(e, weekly, off); err != nil {
		t.Fatalf("the base schedule was refused because of its full copy: %v", err)
	}
	noSource := e
	noSource.SourceDSN = ""
	if err := CheckFullCopy(noSource, weekly, on); err == nil || !strings.Contains(RefusalReason(err), "no source") {
		t.Fatalf("no source: %v", err)
	}
	// No loop: CheckBackupSchedule already says so for the whole schedule.
	if err := CheckFullCopy(e, weekly, BackupScheduleGates{}); err != nil {
		t.Fatalf("no loop: %v (the whole schedule's refusal covers it)", err)
	}
}

// Through the endpoint: saved with a full copy, refused with the reason when
// full backups cannot start, cleared by a save without one, and the listing
// reports a full copy the environment later made impossible, in red, while
// the updates stay runnable.
func TestBackupScheduleAPI_fullCopy(t *testing.T) {
	rep := &stubScheduleReporter{full: true}
	srv, id := newScheduleServer(t, rep)
	path := "/api/servers/" + id + "/backup-schedule"
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir) // a backup to update from

	rec, body := doServersReq(t, srv, "PUT", path, `{"every":"6h","at":"03:00","full_every":"7d"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	got := scheduleOf(t, body)
	if got.FullEvery != "7d" || got.NextFullRun == "" || got.FullReason != "" {
		t.Fatalf("PUT response = %+v", got)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule == nil || e.BackupSchedule.FullEvery != "7d" {
		t.Fatalf("stored = %+v", e.BackupSchedule)
	}
	if last := rep.observed[len(rep.observed)-1]; last != id+" 6h|03:00|full 7d" {
		t.Fatalf("observed %q, want the full copy in the identity", last)
	}
	// The next run is the full copy only when the full copy comes first; a
	// 7d full copy is next only on its day, so both shapes are checked
	// against the grid rather than assumed.
	nextRun, _ := time.Parse(time.RFC3339, got.NextRun)
	nextFull, _ := time.Parse(time.RFC3339, got.NextFullRun)
	if nextFull.Equal(nextRun) {
		if got.NextMethod != BackupMethodFull || got.NextMethodWhyCode != "full_copy" {
			t.Fatalf("the full copy is next and the page says %q (%q)", got.NextMethod, got.NextMethodWhyCode)
		}
	} else if got.NextMethod != BackupMethodRefresh {
		t.Fatalf("an ordinary run is next and the page says %q (%q)", got.NextMethod, got.NextMethodWhy)
	}

	// A full copy that comes before the next run is what the page says
	// comes next: every 1d at 03:00 with a 1d full copy.
	rec, body = doServersReq(t, srv, "PUT", path, `{"every":"1d","at":"03:00","full_every":"1d"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if got = scheduleOf(t, body); got.NextMethod != BackupMethodFull || got.NextMethodWhy != "the schedule takes a full backup every 1d" ||
		got.NextRun != got.NextFullRun {
		t.Fatalf("every run a full copy: %+v", got)
	}

	// Saved without the field (a page loaded before an upgrade): kept.
	rec, body = doServersReq(t, srv, "PUT", path, `{"every":"6h","at":"03:00"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule.FullEvery != "1d" {
		t.Fatalf("a save that did not mention the full copy changed it to %q", e.BackupSchedule.FullEvery)
	}
	// Saved with it empty: cleared.
	rec, body = doServersReq(t, srv, "PUT", path, `{"every":"6h","at":"03:00","full_every":""}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule.FullEvery != "" {
		t.Fatalf("a save without a full copy kept %q", e.BackupSchedule.FullEvery)
	}
	if got = scheduleOf(t, body); got.FullEvery != "" || got.NextFullRun != "" {
		t.Fatalf("response still reports a full copy: %+v", got)
	}

	// Full backups off: a save asking for full copies is refused with the
	// reason, and nothing changes.
	rep.full = false
	observed := len(rep.observed)
	rec, body = doServersReq(t, srv, "PUT", path, `{"every":"6h","at":"03:00","full_every":"7d"}`)
	if rec.Code != 400 || !strings.Contains(string(body), "the full backup every 7d reads your database") ||
		!strings.Contains(string(body), "BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1") {
		t.Fatalf("refusal: code=%d body=%s", rec.Code, body)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule.FullEvery != "" {
		t.Fatal("a refused full copy was saved anyway")
	}
	if len(rep.observed) != observed {
		t.Fatal("a refused schedule was observed by the loop")
	}
	if rec, body = doServersReq(t, srv, "PUT", path, `{"every":"6h","full_every":"1h"}`); rec.Code != 400 || !strings.Contains(string(body), "more often than the schedule runs") {
		t.Fatalf("full copy more often than runs: code=%d body=%s", rec.Code, body)
	}

	// Saved while full backups were on, then turned off (a restart without
	// the opt-in): the listing says why the full copy cannot run, the
	// schedule stays runnable, and the next run is the update.
	rep.full = true
	if rec, body = doServersReq(t, srv, "PUT", path, `{"every":"1d","at":"03:00","full_every":"1d"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	rep.full = false
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	got = scheduleOf(t, body)
	if !got.Runnable || !strings.Contains(got.FullReason, "the full backup every 1d reads your database") {
		t.Fatalf("listing = %+v, want runnable with the full copy's refusal", got)
	}
	if got.NextMethod != BackupMethodRefresh {
		t.Fatalf("with full copies impossible the next run = %q, want the update", got.NextMethod)
	}
}

// The Backup settings page summarises the schedule; it carries the full
// backup cadence, and the row's own sentence says it (drawn by the real page
// code), so the two pages cannot disagree about what the schedule does.
func TestBackupSettings_summaryCarriesTheFullBackup(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{}, "", "")
	e, err := srv.cm.reg.Add(ServerEntry{Name: "sch", DSN: "u:p@tcp(h:3306)/idx", SourceDSN: "u:p@tcp(s:3306)/",
		BaselineDir: "/b", BackupSchedule: &BackupSchedule{Every: "6h", At: "03:00", FullEvery: "7d"}})
	if err != nil {
		t.Fatal(err)
	}
	var row backupSettingsServerDTO
	for _, s := range backupSettingsGet(t, srv).Servers {
		if s.ID == e.ID {
			row = s
		}
	}
	if row.ScheduleFullEvery != "7d" {
		t.Fatalf("settings row schedule_full_every = %q, want 7d", row.ScheduleFullEvery)
	}
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const flat = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(flat).join(" ");
const r = ` + string(raw) + `;
const row = (caps) => { vm.runInContext("capsCache = " + JSON.stringify(caps) + ";", ctx);
  return flat(vm.runInContext("backupServerRow", ctx)(r, false, [r], "")); };
// Both consoles: one that runs the schedules, and one that does not.
console.log(JSON.stringify([row({}), row({ backup_schedule: true })]));
`
	path := filepath.Join(t.TempDir(), "row.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var both []string
	if err := json.Unmarshal(out, &both); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if len(both) != 2 {
		t.Fatalf("rendered %d rows, want one per console shape", len(both))
	}
	readOnly, runsThem := both[0], both[1]
	for _, text := range both {
		if want := "Scheduled backups: every 6h at 03:00, with a full backup every 7d."; !strings.Contains(text, want) {
			t.Fatalf("the settings row does not say the full backup cadence; want %q in %q", want, text)
		}
	}
	// And the sentence after it says where the timetable is CHANGED, which is
	// not the same place on both consoles (#1573). A console that runs no
	// schedules draws no card to point at: saying "the card above" there named
	// something that is not on the screen, and invited an action it refuses.
	if want := "Schedules run in the DBTrail daemon; this console cannot change them."; !strings.Contains(readOnly, want) {
		t.Errorf("a console that runs no schedules does not say so; want %q in %q", want, readOnly)
	}
	if want := "Select this server at the top of the page to change it."; !strings.Contains(runsThem, want) {
		t.Errorf("a console that runs the schedules does not say where to change one; want %q in %q", want, runsThem)
	}
	if strings.Contains(runsThem, "cannot change them") || strings.Contains(readOnly, "Select this server") {
		t.Error("the two consoles read the same: the sentence does not follow what this one can do")
	}
}

// The 30-day count adds the full backups that do not land on a run: the two
// timetables share their anchor and meet every lcm of the intervals.
func TestBackupsPer30Days_countsFullBackupsBetweenRuns(t *testing.T) {
	for _, c := range []struct {
		every, full string
		want        int64
	}{
		{"6h", "", 120},
		{"6h", "7d", 120}, // every full backup lands on a run
		{"6h", "9h", 160}, // 120 runs + 80 full backups - 40 that coincide
		{"1d", "1d", 30},
	} {
		p, err := (BackupSchedule{Every: c.every, FullEvery: c.full}).Parse()
		if err != nil {
			t.Fatal(err)
		}
		if got := p.BackupsPer30Days(); got != c.want {
			t.Errorf("every %s, full %s: %d backups per 30 days, want %d", c.every, c.full, got, c.want)
		}
	}
}

// The run history's cap keeps the full-backup timetable's newest run and
// newest miss: a short schedule's runs would otherwise evict a weekly full
// backup's record within hours, and the page's line with it.
func TestBaselineRunHistory_capKeepsTheFullBackupEvidence(t *testing.T) {
	var recs []BaselineRunRecord
	sched := func(r BaselineRunRecord) BaselineRunRecord { r.Trigger = BaselineRunTriggerScheduled; return r }
	recs = append(recs,
		sched(BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-08-01T00:00:00Z", WhyCode: BackupWhyCodeFullCopy}),
		sched(BaselineRunRecord{Kind: BaselineRunDump, SkipReason: FullCopySkipReason("another backup job was running"), FinishedAt: "2026-08-08T00:00:05Z"}))
	for i := 0; i < 2*BaselineRunHistoryCap; i++ {
		recs = capRecords(append(recs, sched(BaselineRunRecord{Kind: BaselineRunRefresh, StartedAt: "2026-08-09T00:00:00Z"})))
	}
	var fullRun, fullSkip bool
	for _, r := range recs {
		fullRun = fullRun || r.WhyCode == BackupWhyCodeFullCopy
		fullSkip = fullSkip || IsFullCopySkip(r.SkipReason)
	}
	if !fullRun || !fullSkip || len(recs) != BaselineRunHistoryCap {
		t.Fatalf("after the cap: full run kept %v, full miss kept %v, %d records", fullRun, fullSkip, len(recs))
	}
}

// Through the endpoint: a missed full backup stays on its own line after the
// next ordinary run, goes when a full backup of the timetable starts, and a
// schedule whose full backups were invalidated later can still be edited
// without removing them (a changed full backup is still refused).
func TestBackupScheduleAPI_fullBackupMissAndEdits(t *testing.T) {
	rep := &stubScheduleReporter{full: true}
	srv, id := newScheduleServer(t, rep)
	path := "/api/servers/" + id + "/backup-schedule"
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir)
	if rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1h","at":"00:00","full_every":"1d"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	backdateFullSince(t, srv, id, "2026-09-01T00:00:00Z")
	h := srv.baselineHistory
	sched := func(r BaselineRunRecord) BaselineRunRecord {
		r.ServerID, r.Trigger = id, BaselineRunTriggerScheduled
		return r
	}
	if _, err := h.AppendSkip(sched(BaselineRunRecord{Kind: BaselineRunDump,
		SkipReason: FullCopySkipReason("another backup job was running for this server at the scheduled time"),
		StartedAt:  "2026-09-10T00:00:05Z", FinishedAt: "2026-09-10T00:00:05Z"})); err != nil {
		t.Fatal(err)
	}
	if err := h.Append(sched(BaselineRunRecord{Kind: BaselineRunRefresh, StartedAt: "2026-09-10T01:00:05Z", FinishedAt: "2026-09-10T01:03:00Z"})); err != nil {
		t.Fatal(err)
	}
	_, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	got := scheduleOf(t, body)
	if got.LastFullMissed == nil || got.LastFullMissed.At != "2026-09-10T00:00:05Z" ||
		got.LastFullMissed.Reason != "another backup job was running for this server at the scheduled time" {
		t.Fatalf("last_full_missed = %+v, want the collision with the prefix taken off", got.LastFullMissed)
	}
	if got.LastSkipped != nil {
		t.Fatalf("the full backup's miss is also on the ordinary skip line: %+v", got.LastSkipped)
	}
	if err := h.Append(sched(BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-09-11T00:00:07Z", FinishedAt: "2026-09-11T00:09:00Z",
		Why: "the schedule takes a full backup every 1d", WhyCode: BackupWhyCodeFullCopy})); err != nil {
		t.Fatal(err)
	}
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if got = scheduleOf(t, body); got.LastFullMissed != nil {
		t.Fatalf("a full backup of the timetable started after the miss and the miss is still shown: %+v", got.LastFullMissed)
	}

	// Full backups turned off after the save: the rest of the schedule can
	// still be edited with the full backup as it was; changing it is refused.
	rep.full = false
	rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1h","at":"00:30","full_every":"1d"}`)
	if rec.Code != 200 {
		t.Fatalf("editing the time with the full backup unchanged: code=%d body=%s", rec.Code, body)
	}
	if got = scheduleOf(t, body); got.At != "00:30" || !strings.Contains(got.FullReason, "reads your database") {
		t.Fatalf("after the edit: %+v, want the new time and the full backup still shown as refused", got)
	}
	if rec, body = doServersReq(t, srv, "PUT", path, `{"every":"1h","at":"00:30","full_every":"7d"}`); rec.Code != 400 {
		t.Fatalf("changing a refused full backup: code=%d body=%s, want 400", rec.Code, body)
	}
}

func TestBackupSettings_fullBackupRefusalReachesTheRow(t *testing.T) {
	// A watch-shaped server (the settings fixture is the read-only console,
	// where CheckBackupSchedule already refuses the whole schedule).
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: false})
	e, _ := srv.cm.reg.Get(id)
	e.BackupSchedule = &BackupSchedule{Every: "6h", FullEvery: "7d"}
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	seen := false
	var row backupSettingsServerDTO
	for _, s := range backupSettingsGet(t, srv).Servers {
		if s.ID == e.ID {
			row = s
		}
		seen = seen || s.ID == e.ID
		if s.ID == e.ID && (s.ScheduleRefusal != "" || !strings.Contains(s.ScheduleFullRefusal, "the full backup every 7d reads your database")) {
			t.Fatalf("row = refusal %q, full refusal %q; want the schedule runnable and its full backups refused", s.ScheduleRefusal, s.ScheduleFullRefusal)
		}
	}
	if !seen {
		t.Fatal("the server is not in the settings listing")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const red = (n, out = []) => { if (!n || n.nodeType === 3) return out; if (/form-msg/.test(n.className) && /err/.test(n.className)) out.push(n.textContent); (n.children || []).forEach((c) => red(c, out)); return out; };
const r = ` + string(raw) + `;
console.log(JSON.stringify(red(vm.runInContext("backupServerRow", ctx)(r, false, [r], ""))));
`
	path := filepath.Join(t.TempDir(), "row.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var lines []string
	if err := json.Unmarshal(out, &lines); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	want := "The full backup every 7d reads your database, and creating backups from the web interface is turned off on this daemon " +
		"(BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1). The full backups do not run until that changes; the other scheduled runs still do."
	found := false
	for _, l := range lines {
		found = found || l == want
	}
	if !found {
		t.Fatalf("the settings row does not show the refused full backup in red; red lines %q", lines)
	}
}

// backdateFullSince moves the timetable's FullSince to since, so records a
// test dates in the past belong to it (the API stamps the real clock).
func backdateFullSince(t *testing.T, srv *Server, id, since string) {
	t.Helper()
	e, ok := srv.cm.reg.Get(id)
	if !ok || e.BackupSchedule == nil {
		t.Fatalf("no schedule for %s", id)
	}
	e.BackupSchedule.FullSince = since
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
}

// FullSince is stamped by the API when the timetable is set, kept across
// edits that leave it as it was (including one that does not mention it),
// restarted when it changes, and gone with it.
func TestBackupScheduleAPI_fullSince(t *testing.T) {
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
	path := "/api/servers/" + id + "/backup-schedule"
	put := func(body string) BackupSchedule {
		t.Helper()
		if rec, raw := doServersReq(t, srv, "PUT", path, body); rec.Code != 200 {
			t.Fatalf("PUT %s: code=%d body=%s", body, rec.Code, raw)
		}
		e, _ := srv.cm.reg.Get(id)
		return *e.BackupSchedule
	}
	first := put(`{"every":"6h","at":"03:00","full_every":"7d"}`)
	if _, err := time.Parse(time.RFC3339, first.FullSince); err != nil {
		t.Fatalf("full_since = %q, want a timestamp", first.FullSince)
	}
	backdateFullSince(t, srv, id, "2026-09-01T00:00:00Z")
	// An edit that leaves the full backups on their slots keeps the start:
	// every does not move them, nor does saving the same value again.
	if got := put(`{"every":"12h","at":"03:00"}`); got.FullEvery != "7d" || got.FullSince != "2026-09-01T00:00:00Z" {
		t.Fatalf("an edit that did not mention the timetable nor move it: %+v, want it and its start kept", got)
	}
	if got := put(`{"every":"6h","at":"03:00","full_every":" 7d "}`); got.FullSince != "2026-09-01T00:00:00Z" {
		t.Fatalf("the same timetable saved again restarted it: %+v", got)
	}
	// Moving the time moves every full-backup slot: the timetable starts
	// afresh, or the boot check would call a slot of the new grid before the
	// edit (served by the old grid) missed.
	if got := put(`{"every":"6h","at":"22:00"}`); got.FullEvery != "7d" || got.FullSince == "2026-09-01T00:00:00Z" || got.FullSince == "" {
		t.Fatalf("a moved timetable kept the old start: %+v", got)
	}
	backdateFullSince(t, srv, id, "2026-09-01T00:00:00Z")
	if got := put(`{"every":"6h","at":"04:00","full_every":"3d"}`); got.FullSince == "2026-09-01T00:00:00Z" || got.FullSince == "" {
		t.Fatalf("a changed timetable kept the old start: %+v", got)
	}
	if got := put(`{"every":"6h","at":"04:00","full_every":""}`); got.FullEvery != "" || got.FullSince != "" {
		t.Fatalf("a removed timetable left %+v", got)
	}
}

// A miss recorded under an earlier timetable is not the new one's: removing
// the full backup and setting it again, or deleting the whole schedule and
// saving it again, starts with a clean line.
func TestBackupScheduleAPI_reAddedFullCopyReportsNothingStale(t *testing.T) {
	for _, via := range []string{"removed from the schedule", "schedule deleted"} {
		t.Run(via, func(t *testing.T) {
			srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
			path := "/api/servers/" + id + "/backup-schedule"
			if rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1h","full_every":"1d"}`); rec.Code != 200 {
				t.Fatalf("PUT code=%d body=%s", rec.Code, body)
			}
			backdateFullSince(t, srv, id, "2026-09-01T00:00:00Z")
			if _, err := srv.baselineHistory.AppendSkip(BaselineRunRecord{ServerID: id, Trigger: BaselineRunTriggerScheduled, Kind: BaselineRunDump,
				SkipReason: FullCopySkipReason("another backup job was running for this server at the scheduled time"),
				StartedAt:  "2026-09-10T00:00:05Z", FinishedAt: "2026-09-10T00:00:05Z"}); err != nil {
				t.Fatal(err)
			}
			_, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
			if scheduleOf(t, body).LastFullMissed == nil {
				t.Fatal("fixture: the miss is not shown to begin with")
			}
			if via == "schedule deleted" {
				if rec, body := doServersReq(t, srv, "DELETE", path, ""); rec.Code/100 != 2 {
					t.Fatalf("DELETE code=%d body=%s", rec.Code, body)
				}
			} else if rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1h","full_every":""}`); rec.Code != 200 {
				t.Fatalf("PUT code=%d body=%s", rec.Code, body)
			}
			rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1h","full_every":"1d"}`)
			if rec.Code != 200 {
				t.Fatalf("PUT code=%d body=%s", rec.Code, body)
			}
			if got := scheduleOf(t, body); got.LastFullMissed != nil {
				t.Fatalf("the timetable set again shows the old one's miss: %+v", got.LastFullMissed)
			}
		})
	}
}

// A full backup of the timetable that started and FAILED stays on the line
// the way a slot that did not start does: an ordinary run finishing after
// it does not end it; a full backup that succeeds afterwards does, whatever
// started it (the timetable asks for a real read, and a manual one is one).
func TestBackupScheduleAPI_failedFullCopyStaysUntilAFullReadSucceeds(t *testing.T) {
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
	path := "/api/servers/" + id + "/backup-schedule"
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir)
	if rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1h","full_every":"1d"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	backdateFullSince(t, srv, id, "2026-09-01T00:00:00Z")
	h := srv.baselineHistory
	add := func(r BaselineRunRecord) {
		t.Helper()
		r.ServerID = id
		switch r.Trigger {
		case "":
			r.Trigger = BaselineRunTriggerScheduled
		case "manual":
			r.Trigger = "" // the history's word for "not the schedule"
		}
		if err := h.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	missed := func() *backupScheduleSkipDTO {
		t.Helper()
		_, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
		return scheduleOf(t, body).LastFullMissed
	}
	add(BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-09-10T00:00:05Z", FinishedAt: "2026-09-10T00:04:00Z",
		Why: "the schedule takes a full backup every 1d", WhyCode: BackupWhyCodeFullCopy, Error: "mydumper: exit status 2"})
	// While it is the last run, the last run's own line says it, once.
	_, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if got := scheduleOf(t, body); got.LastFullMissed != nil || got.LastRun == nil || got.LastRun.OK {
		t.Fatalf("a failed full backup that is the last run: last_run %+v, last_full_missed %+v; want it said once, by the last run", got.LastRun, got.LastFullMissed)
	}
	add(BaselineRunRecord{Kind: BaselineRunRefresh, StartedAt: "2026-09-10T01:00:05Z", FinishedAt: "2026-09-10T01:03:00Z"})
	if m := missed(); m == nil || !m.Failed || m.At != "2026-09-10T00:00:05Z" || m.Reason != "mydumper: exit status 2" {
		t.Fatalf("an ordinary update pushed the failed full backup off: %+v, want it on its own line with its error", m)
	}
	add(BaselineRunRecord{Kind: BaselineRunDump, Trigger: "manual", StartedAt: "2026-09-10T08:00:00Z", FinishedAt: "2026-09-10T08:20:00Z"})
	if m := missed(); m != nil {
		t.Fatalf("a manual full backup that succeeded afterwards left the line: %+v", m)
	}
}

// A manual full backup that was holding the server at the slot (the very
// collision the miss records) read the database when the timetable asked:
// it ends the line once it succeeds, although it started before the miss.
func TestBackupScheduleAPI_theCollidingFullBackupAnswersTheMiss(t *testing.T) {
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
	path := "/api/servers/" + id + "/backup-schedule"
	if rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1h","full_every":"1d"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	backdateFullSince(t, srv, id, "2026-09-01T00:00:00Z")
	h := srv.baselineHistory
	if _, err := h.AppendSkip(BaselineRunRecord{ServerID: id, Trigger: BaselineRunTriggerScheduled, Kind: BaselineRunDump,
		SkipReason: FullCopySkipReason("another backup job was running for this server at the scheduled time"),
		StartedAt:  "2026-09-10T00:00:05Z", FinishedAt: "2026-09-10T00:00:05Z"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Append(BaselineRunRecord{ServerID: id, Kind: BaselineRunDump,
		StartedAt: "2026-09-09T23:50:00Z", FinishedAt: "2026-09-10T00:12:00Z"}); err != nil {
		t.Fatal(err)
	}
	_, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if m := scheduleOf(t, body).LastFullMissed; m != nil {
		t.Fatalf("the full backup that held the slot and succeeded left the miss: %+v", m)
	}
}

// The loop's memory, for the history's blind spots: a full backup of the
// timetable running now hides an older miss (its record is written when it
// ends); one that failed is on the line before any record exists; and with
// no history at all, the loop's own miss is shown.
func TestBackupScheduleAPI_fullMissFromTheLoopsMemory(t *testing.T) {
	why := "the schedule takes a full backup every 1d"
	setup := func(t *testing.T) (*Server, *stubScheduleReporter, string) {
		t.Helper()
		rep := &stubScheduleReporter{full: true, state: map[string]BackupScheduleState{}}
		srv, id := newScheduleServer(t, rep)
		if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"1h","full_every":"1d"}`); rec.Code != 200 {
			t.Fatalf("PUT code=%d body=%s", rec.Code, body)
		}
		backdateFullSince(t, srv, id, "2026-09-01T00:00:00Z")
		return srv, rep, id
	}
	missed := func(t *testing.T, srv *Server, id string) *backupScheduleSkipDTO {
		t.Helper()
		_, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
		return scheduleOf(t, body).LastFullMissed
	}
	t.Run("running now", func(t *testing.T) {
		srv, rep, id := setup(t)
		if _, err := srv.baselineHistory.AppendSkip(BaselineRunRecord{ServerID: id, Trigger: BaselineRunTriggerScheduled, Kind: BaselineRunDump,
			SkipReason: FullCopySkipReason("x"), StartedAt: "2026-09-10T00:00:05Z", FinishedAt: "2026-09-10T00:00:05Z"}); err != nil {
			t.Fatal(err)
		}
		rep.state[id] = BackupScheduleState{LastStartedAt: "2026-09-11T00:00:05Z", LastMethod: BackupMethodFull, LastWhy: why,
			Last: &BaselineStatus{State: "running"}, Running: true}
		if m := missed(t, srv, id); m != nil {
			t.Fatalf("a full backup of the timetable is running and the older miss is still shown: %+v", m)
		}
	})
	t.Run("succeeded, no record yet", func(t *testing.T) {
		srv, rep, id := setup(t)
		if _, err := srv.baselineHistory.AppendSkip(BaselineRunRecord{ServerID: id, Trigger: BaselineRunTriggerScheduled, Kind: BaselineRunDump,
			SkipReason: FullCopySkipReason("x"), StartedAt: "2026-09-10T00:00:05Z", FinishedAt: "2026-09-10T00:00:05Z"}); err != nil {
			t.Fatal(err)
		}
		rep.state[id] = BackupScheduleState{LastStartedAt: "2026-09-11T00:00:05Z", LastMethod: BackupMethodFull,
			Last: &BaselineStatus{State: "succeeded", FinishedAt: "2026-09-11T00:20:00Z"}}
		if m := missed(t, srv, id); m != nil {
			t.Fatalf("a full backup the loop saw succeed left the older miss: %+v", m)
		}
	})
	t.Run("no history", func(t *testing.T) {
		srv, rep, id := setup(t)
		srv.baselineHistory = nil
		rep.state[id] = BackupScheduleState{LastFullMissedAt: "2026-09-12T00:00:05Z",
			LastFullMissedReason: FullCopySkipReason("another backup job was running for this server at the scheduled time")}
		if m := missed(t, srv, id); m == nil || m.Reason != "another backup job was running for this server at the scheduled time" {
			t.Fatalf("with no history the loop's miss = %+v", m)
		}
	})
}

// A full backup the loop owes (a busy slot) is the next run, and the page
// says so; one the daemon cannot take is not announced as next.
func TestBackupScheduleAPI_owedFullCopyIsTheNextRun(t *testing.T) {
	rep := &stubScheduleReporter{full: true, state: map[string]BackupScheduleState{}}
	srv, id := newScheduleServer(t, rep)
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"1h","full_every":"7d"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	e, _ = srv.cm.reg.Get(id)
	now := time.Date(2026, 9, 21, 10, 30, 0, 0, time.UTC)
	rep.state[id] = BackupScheduleState{FullOwed: true}
	got := srv.backupScheduleDTO(context.Background(), e, now)
	if got.NextRun != "2026-09-21T11:00:00Z" || got.NextFullRun != got.NextRun || got.NextMethod != BackupMethodFull || got.NextMethodWhyCode != "full_copy" || !got.FullOwed {
		t.Fatalf("owed: %+v, want the next run to be the full backup", got)
	}
	rep.full = false
	if got := srv.backupScheduleDTO(context.Background(), e, now); got.NextMethod == BackupMethodFull || got.NextFullRun == got.NextRun || got.FullOwed {
		t.Fatalf("owed but refused: %+v, want the update announced and the timetable's own next slot", got)
	}
}

// A full backup that falls before the next run is what comes next, found on
// the grid at a fixed instant rather than on today's calendar.
func TestBackupScheduleDTO_fullCopyBeforeTheNextRun(t *testing.T) {
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"6h","at":"03:00","full_every":"9h"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	e, _ := srv.cm.reg.Get(id)
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	for i := 0; !p.NextFullRun(now).Before(p.NextRun(now)); i++ {
		if i > 200 {
			t.Fatal("fixture: no instant where the full backup comes first")
		}
		now = now.Add(time.Hour)
	}
	got := srv.backupScheduleDTO(context.Background(), e, now)
	want := p.NextFullRun(now).Format(time.RFC3339)
	if got.NextRun != want || got.NextFullRun != want || got.NextMethod != BackupMethodFull || got.NextMethodWhyCode != "full_copy" {
		t.Fatalf("at %s: %+v, want the full backup at %s as the next run", now, got, want)
	}
}

// The newest successful full backup of any trigger survives the cap: it is
// what ends a miss's line, and evicting it would bring the alarm back.
func TestBaselineRunHistory_capKeepsTheLastFullRead(t *testing.T) {
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Append(BaselineRunRecord{ServerID: "a", Kind: BaselineRunDump,
		StartedAt: "2026-09-01T00:00:00Z", FinishedAt: "2026-09-01T00:10:00Z"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < BaselineRunHistoryCap+10; i++ {
		at := time.Date(2026, 9, 2, 0, i, 0, 0, time.UTC).Format(time.RFC3339)
		if err := h.Append(BaselineRunRecord{ServerID: "a", Kind: BaselineRunRefresh, StartedAt: at, FinishedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	if r := h.LastFullRead("a"); r == nil || r.StartedAt != "2026-09-01T00:00:00Z" {
		t.Fatalf("LastFullRead after the cap = %+v, want the manual full backup", r)
	}
}
