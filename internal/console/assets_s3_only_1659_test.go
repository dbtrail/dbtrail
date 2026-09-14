package console

import (
	"encoding/json"
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
	card := jsFunctionBody(t, js, "backupScheduleCard")

	// Mounted right after the fields and outside every schedule branch.
	grid, warn, sched := strings.Index(row, "box.append(grid);"), strings.Index(row, "box.append(s3Only);"), strings.Index(row, "if (srv.schedule_every)")
	if grid < 0 || warn < 0 || sched < 0 || !(grid < warn && warn < sched) {
		t.Errorf("the S3-only warning is not appended between the fields and the schedule block (grid %d, warning %d, schedule %d)", grid, warn, sched)
	}
	for _, want := range []string{`s3OnlyBackupWarning(dir.value, s3.value)`, `dir.addEventListener("input", showS3Only)`, `s3.addEventListener("input", showS3Only)`, `class: "form-msg err"`} {
		if !strings.Contains(row, want) {
			t.Errorf("backupServerRow lost %q", want)
		}
	}
	if strings.Contains(row, "As set up, each scheduled run takes a full backup") {
		t.Error("the old schedule-gated grey hint is still rendered next to the new red line")
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
	}, "\n") + `
const lines = [];
const el = (tag, o) => { const n = { class: o.class, text: o.text }; return n; };
function nextRun(sch) {
  const body = { append: (n) => lines.push(n) };
  let alarm = false;
  ` + nextRunBranch(t, card) + `
  return alarm;
}
const out = {
  warn: [
    s3OnlyBackupWarning("", "s3://b/p"),
    s3OnlyBackupWarning("/var/lib/bintrail/baselines", "s3://b/p"),
    s3OnlyBackupWarning("", ""),
    s3OnlyBackupWarning("  ", "s3://b/p"),
    s3OnlyBackupWarning("/d", ""),
    s3OnlyBackupWarning("", "   "),
  ],
  alarms: [
    nextRun({ runnable: true, next_method: "full", next_method_why: "an update from the recorded changes needs a local backup directory", next_method_why_code: "no_local_dir" }),
    nextRun({ runnable: true, next_method: "full", next_method_why: "no previous backup to update", next_method_why_code: "first_backup" }),
    nextRun({ runnable: true, next_method: "refresh", next_method_why: "no load on your database" }),
    nextRun({ runnable: true, next_method: "full", next_method_why: "this server has no index connection to read the recorded changes from", next_method_why_code: "no_index" }),
  ],
  lines,
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
		Warn   []string
		Alarms []bool
		Lines  []struct{ Class, Text string }
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	const want = "With S3 only, every scheduled backup reads your whole database. Add a Backup dir so runs update from the recorded changes."
	if got.Warn[0] != want || got.Warn[3] != want {
		t.Errorf("S3 without a folder (or a folder of spaces) is not warned with the agreed sentence: %q", got.Warn)
	}
	for i, w := range []int{1, 2, 4, 5} {
		if got.Warn[w] != "" {
			t.Errorf("case %d warned where it must not: %q", i, got.Warn[w])
		}
	}
	if want := []bool{true, false, false, true}; len(got.Alarms) != 4 || got.Alarms[0] != want[0] || got.Alarms[1] != want[1] || got.Alarms[2] != want[2] || got.Alarms[3] != want[3] {
		t.Errorf("alarms = %v, want %v", got.Alarms, want)
	}
	if len(got.Lines) != 4 {
		t.Fatalf("rendered %d next-run lines, want 4: %+v", len(got.Lines), got.Lines)
	}
	for i, l := range got.Lines {
		t.Logf("next-run line %d [%s]: %s", i, l.Class, l.Text)
		if strings.Contains(l.Text, "—") || strings.Contains(l.Text, "..") || strings.Contains(l.Text, "undefined") {
			t.Errorf("line %d is not clean copy: %q", i, l.Text)
		}
	}
	if got.Lines[0].Class != "form-msg err" || !strings.Contains(got.Lines[0].Text, "Set a Backup dir for this server") {
		t.Errorf("no Backup dir: not a red line naming the setting: %+v", got.Lines[0])
	}
	if got.Lines[1].Class != "form-hint" || got.Lines[2].Class != "form-hint" {
		t.Errorf("a first backup or an update is not a hint: %+v", got.Lines[1:3])
	}
	if got.Lines[3].Class != "form-msg err" || !strings.Contains(got.Lines[3].Text, "Set an index connection") {
		t.Errorf("no index connection: not a red line naming the setting: %+v", got.Lines[3])
	}
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
