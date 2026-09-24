package console

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestS3OnlyBackupWarning_1659 covers the three places #1659 changed, with the
// sentences produced by the page's own functions from real inputs.
//
// Backup settings: a server whose own location is a bucket with no folder is
// told in red, next to the two fields, whether or not a schedule exists. The
// values are this server's raw ones as typed, because the scheduled run reads
// those (rebuildPossible) and a daemon default folder does not change it.
//
// Backups: the next-run line is a warning when a setting makes every run a
// full read, and stays a hint for a first backup.
func TestS3OnlyBackupWarning_1659(t *testing.T) {
	js := readAsset(t, "app.js")
	row := jsFunctionBody(t, js, "backupServerRow")
	card := functionBody(t, js, "function backupScheduleCard(")

	// Saving repaints the row, which is what takes the warning down once a
	// Backup dir is saved. Through the page's own painter since #1573, not
	// renderRoute: the route path bumps viewGen and kills the job watchers
	// this page runs.
	if save := strings.Index(row, `toast("Saved for " + (srv.name || srv.id));`); save < 0 || !strings.HasPrefix(strings.TrimSpace(stripLineComments(row[save+len(`toast("Saved for " + (srv.name || srv.id));`):])), "await renderSnapshots();") {
		t.Error("a successful save no longer repaints the row, so the S3-only warning would stay up after a Backup dir is saved")
	}
	if strings.Contains(jsFunctionSpan(t, js, "backupServerRow"), "As set up, each scheduled run takes a full backup") {
		t.Error("the old schedule-gated grey hint is still rendered next to the new red line")
	}
	// The last-run remedy is not repeated in grey under the red next-run one.
	// Matched on the declaration of everyRunCode rather than the whole `let`
	// line: #1528 added a third variable to it (alarmNote), and pinning the
	// line verbatim failed for a change that left this mechanism untouched.
	if !strings.Contains(card, `run.why_code !== everyRunCode`) || !strings.Contains(card, `everyRunCode = ""`) {
		t.Error("the last-run reason line no longer skips the remedy the next-run warning already shows")
	}

	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	script := strings.Join([]string{
		functionBody(t, js, "function s3OnlyBackupWarning("),
		// Runs to the next function, so it carries BACKUP_WHY_EVERY_RUN too.
		functionBody(t, js, "const BACKUP_WHY_REMEDY = {"),
		functionBody(t, js, "function backupFoldError("),
		functionBody(t, js, "function backupWhyLine("),
		// The remedy is shown only to a session that can act on it (#1573
		// step 7). A full-access session is the case pinned here: no
		// permission map, so every permission reads as held.
		functionBody(t, js, "function sessionMay("),
		functionBody(t, js, "function sessionMayConfigureServer("),
	}, "\n") + `
var capsCache = {};
const lines = [];
const el = (tag, o) => ({ class: o.class, text: o.text });
function nextRun(sch) {
  const body = { append: (n) => lines.push(n) };
  let alarm = false, everyRunCode = "";
  ` + nextRunBranch(t, card) + `
  return [alarm, everyRunCode];
}
const utcLabel = (s) => s, reusedCopiedNote = () => "";
function lastRun(sch, everyRunCode) {
  const out = [], body = { append: (n) => out.push(n.text) };
  let alarm = false;
  ` + lastRunBlock(t, card) + `
  return out;
}
const srv = (o) => Object.assign({ baseline_dir: "", baseline_s3: "", full_backup_possible: true, schedule_loop: true }, o);
const out = {
  warn: [
    s3OnlyBackupWarning(srv({ baseline_s3: "s3://b/p" })),
    s3OnlyBackupWarning(srv({ baseline_dir: "/var/lib/bintrail/baselines/a", baseline_s3: "s3://b/p" })),
    s3OnlyBackupWarning(srv({})),
    s3OnlyBackupWarning(srv({ baseline_dir: "/d" })),
    s3OnlyBackupWarning(srv({ baseline_s3: "s3://b/p", full_backup_possible: false })),
    s3OnlyBackupWarning(null),
    s3OnlyBackupWarning(srv({ baseline_s3: "s3://b/p", schedule_loop: false, full_backup_possible: false })),
    s3OnlyBackupWarning(srv({ baseline_s3: "s3://b/p", schedule_refusal: "creating backups from the console is turned off" })),
  ],
  alarms: [
    nextRun({ runnable: true, next_method: "full", next_method_why: "an update from the recorded changes needs a local backup directory", next_method_why_code: "no_local_dir" }),
    nextRun({ runnable: true, next_method: "full", next_method_why: "no previous backup to update", next_method_why_code: "first_backup" }),
    nextRun({ runnable: true, next_method: "refresh", next_method_why: "no load on your database" }),
    nextRun({ runnable: true, next_method: "full", next_method_why: "this server has no index connection to read the recorded changes from", next_method_why_code: "no_index" }),
    nextRun({ runnable: true, next_method: "refresh", next_method_why: "x", next_method_why_code: "no_local_dir" }),
    nextRun({ runnable: true, next_method: "full", next_method_why: "the previous backup could not be read", next_method_why_code: "previous_unreadable" }),
  ],
  lines,
  lastRun: [
    lastRun({ last_run: { ok: true, method: "full", why: "an update needs a local backup directory", why_code: "no_local_dir" } }, "no_local_dir"),
    lastRun({ last_run: { ok: true, method: "full", why: "an update needs a local backup directory", why_code: "no_local_dir" } }, "no_index"),
    lastRun({ last_run: { ok: true, method: "full", why: "an update needs a local backup directory", why_code: "no_local_dir" } }, ""),
  ],
};
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "s3only.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Warn    []string
		Alarms  [][]any
		Lines   []struct{ Class, Text string }
		LastRun [][]string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for i, w := range got.Warn {
		t.Logf("warning %d: %q", i, w)
	}
	const fullRead = "With S3 only, every scheduled backup reads your whole database. Add a Local folder so runs update from the recorded changes."
	if got.Warn[0] != fullRead {
		t.Errorf("S3 without a folder is not warned with the agreed sentence: %q", got.Warn[0])
	}
	for _, i := range []int{1, 2, 3, 5, 7} {
		if got.Warn[i] != "" {
			t.Errorf("case %d warned where it must not: %q", i, got.Warn[i])
		}
	}
	// Where no full backup is possible either, nothing runs: saying every run
	// reads the database would be false.
	if w := got.Warn[4]; w != "With S3 only, scheduled backups cannot run on this server: a full backup is not available here, and updating from the recorded changes needs a Local folder. Add one." {
		t.Errorf("S3 without a folder on a daemon that cannot take a full backup: %q", w)
	}
	wantAlarm := []bool{true, false, false, true, false, false}
	wantCode := []string{"no_local_dir", "", "", "no_index", "", ""}
	for i := range wantAlarm {
		if len(got.Alarms) <= i || got.Alarms[i][0] != wantAlarm[i] || got.Alarms[i][1] != wantCode[i] {
			t.Errorf("next-run case %d: alarm/code = %v, want %v %q", i, got.Alarms, wantAlarm[i], wantCode[i])
		}
	}
	if len(got.Lines) != 6 {
		t.Fatalf("rendered %d next-run lines, want 6: %+v", len(got.Lines), got.Lines)
	}
	for i, l := range got.Lines {
		t.Logf("next-run line %d [%s]: %s", i, l.Class, l.Text)
		if strings.Contains(l.Text, "\u2014") || strings.Contains(l.Text, "..") || strings.Contains(l.Text, "undefined") {
			t.Errorf("line %d is not clean copy: %q", i, l.Text)
		}
	}
	for _, w := range got.Warn {
		if strings.Contains(w, "\u2014") {
			t.Errorf("em dash in %q", w)
		}
	}
	if got.Lines[0].Class != "form-msg err" || !strings.Contains(got.Lines[0].Text, "Set a Local folder for this server") {
		t.Errorf("no Backup dir: not a red line naming the setting: %+v", got.Lines[0])
	}
	if got.Lines[1].Class != "form-hint" || got.Lines[2].Class != "form-hint" || got.Lines[4].Class != "form-hint" || got.Lines[5].Class != "form-hint" {
		t.Errorf("a first backup or an update is not a hint: %+v", got.Lines)
	}
	if got.Lines[3].Class != "form-msg err" || !strings.Contains(got.Lines[3].Text, "Set an index connection") {
		t.Errorf("no index connection: not a red line naming the setting: %+v", got.Lines[3])
	}
	// Where this process runs no scheduled backups, only the setting is known.
	if got.Warn[6] != "With S3 only, a scheduled backup cannot update from the recorded changes. Add a Local folder." {
		t.Errorf("no schedule loop: %q", got.Warn[6])
	}
	// The last run's full-backup reason: skipped when the next-run warning
	// carries the same remedy, in the past tense when it carries another, and
	// with its remedy when there is no warning.
	if len(got.LastRun) != 3 || len(got.LastRun[0]) != 1 || len(got.LastRun[1]) != 2 || len(got.LastRun[2]) != 2 {
		t.Fatalf("last-run lines = %q", got.LastRun)
	}
	if strings.Contains(got.LastRun[1][1], "Set a Local folder") || !strings.Contains(got.LastRun[2][1], "Set a Local folder") {
		t.Errorf("last-run reason tense: next-run warning elsewhere %q, no warning %q", got.LastRun[1][1], got.LastRun[2][1])
	}
}

// lastRunBlock cuts the last-run block out of backupScheduleCard.
func lastRunBlock(t *testing.T, card string) string {
	t.Helper()
	start := strings.Index(card, "if (run) {")
	end := strings.Index(card, "    if (fb) {")
	if start < 0 || end < 0 || end < start {
		t.Fatalf("could not find the last-run block in backupScheduleCard")
	}
	return "const run = sch.last_run, fb = sch.last_fallback;\n" + card[start:end]
}

// stripLineComments drops whole-line // comments so a check for the statement
// after another is not defeated by the comment between them.
func stripLineComments(s string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "//") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

// nextRunBranch cuts the next-run `else if` branch out of backupScheduleCard so
// the test executes the page's own code, not a copy of it.
func nextRunBranch(t *testing.T, card string) string {
	t.Helper()
	start := strings.Index(card, "if (sch.runnable && sch.next_method_error) {")
	end := strings.Index(card, "if (sch.history_unavailable) {")
	if start < 0 || end < 0 || end < start {
		t.Fatalf("could not find the next-run branch in backupScheduleCard")
	}
	return strings.Replace(card[start:end], "plainWords(sch.next_method_error)", "String(sch.next_method_error)", 1)
}

// TestBackupSettings_fullBackupPossibleReachesTheWire (#1659): the S3-only
// warning says "every scheduled backup reads your whole database" only where a
// full backup can actually start; elsewhere nothing runs, and the page needs
// the daemon's answer to say which. Driven through the real handler, with the
// schedule loop both present and absent.
func TestBackupSettings_fullBackupPossibleReachesTheWire(t *testing.T) {
	for _, tc := range []struct {
		name string
		rep  *stubScheduleReporter
		want bool
	}{
		{"full backups enabled", &stubScheduleReporter{full: true}, true},
		{"full backups off on this daemon", &stubScheduleReporter{full: false}, false},
		{"no schedule loop (read-only console)", nil, false},
		// Per-server: the daemon may take full backups, this server cannot.
		{"no source connection on this server", &stubScheduleReporter{full: true}, false},
		{"full backups refused on this daemon", &stubScheduleReporter{full: true, refusal: errors.New("lock mode refused")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newScheduleServer(t, tc.rep)
			source := "src:pw@tcp(127.0.0.1:3306)/"
			if tc.name == "no source connection on this server" {
				source = ""
			}
			s3only, err := srv.cm.reg.Add(ServerEntry{Name: "s3only", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx3",
				SourceDSN: source, BaselineS3: "s3://bucket/backups"})
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range backupSettingsGet(t, srv).Servers {
				if row.ID != s3only.ID {
					continue
				}
				if row.FullBackupPossible != tc.want {
					t.Errorf("full_backup_possible = %v, want %v", row.FullBackupPossible, tc.want)
				}
				if row.ScheduleLoop != (tc.rep != nil) {
					t.Errorf("schedule_loop = %v with reporter %v", row.ScheduleLoop, tc.rep != nil)
				}
				raw, _ := json.Marshal(row)
				if !strings.Contains(string(raw), `"full_backup_possible":`) {
					t.Errorf("the row does not serialise full_backup_possible: %s", raw)
				}
				return
			}
			t.Fatal("the S3-only server is not in the listing")
		})
	}
}

// TestBackupServerRow_s3OnlyWarningRendered renders the real Backup settings
// row: the whole app.js in a node VM with fake page elements. A shape check on
// the source could not tell a warning mounted inside a condition from one that
// is not; the rendered row can.
func TestBackupServerRow_s3OnlyWarningRendered(t *testing.T) {
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
	script := renderHarnessJS + `
vm.runInContext("globalThis.__row = backupServerRow;", ctx);
function red(n, out = []) {
  if (!n || n.nodeType === 3) return out;
  if (n.tag === "p" && /form-msg/.test(n.className) && /err/.test(n.className) && !n.hidden) { out.push(n.textContent); return out; }
  for (const c of n.children || []) red(c, out);
  return out;
}
const base = { id: "a", name: "a", kind: "registry", baseline_dir: "", baseline_s3: "s3://b/p", full_backup_possible: true, schedule_loop: true };
const rows = {
  noSchedule: base,
  withSchedule: Object.assign({}, base, { schedule_every: "1d", schedule_every_minutes: 1440 }),
  refused: Object.assign({}, base, { schedule_every: "1d", schedule_every_minutes: 1440, schedule_refusal: "creating backups from the console is turned off" }),
  noLoop: Object.assign({}, base, { schedule_loop: false, full_backup_possible: false }),
  withDir: Object.assign({}, base, { baseline_dir: "/var/lib/bintrail/baselines/a" }),
};
const out = {};
for (const [k, r] of Object.entries(rows)) out[k] = red(ctx.__row(r, false, [r], ""));
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "row.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string][]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	const fullRead = "With S3 only, every scheduled backup reads your whole database. Add a Local folder so runs update from the recorded changes."
	has := func(lines []string, want string) bool {
		for _, l := range lines {
			if l == want {
				return true
			}
		}
		return false
	}
	if !has(got["noSchedule"], fullRead) || !has(got["withSchedule"], fullRead) {
		t.Errorf("the S3-only warning is not rendered in red with and without a schedule: %q", got)
	}
	if has(got["refused"], fullRead) || len(got["refused"]) != 1 || !strings.Contains(got["refused"][0], "creating backups from the console is turned off") {
		t.Errorf("a refused schedule shows the S3-only line next to its own reason: %q", got["refused"])
	}
	if !has(got["noLoop"], "With S3 only, a scheduled backup cannot update from the recorded changes. Add a Local folder.") {
		t.Errorf("no schedule loop: %q", got["noLoop"])
	}
	if len(got["withDir"]) != 0 {
		t.Errorf("a server with a Backup dir is warned: %q", got["withDir"])
	}
}
