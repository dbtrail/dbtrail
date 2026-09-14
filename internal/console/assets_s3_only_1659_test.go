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
	card := functionBody(t, js, "function backupScheduleCard(")

	// Mounted right after the fields, from the SAVED entry, outside every
	// schedule branch, and in red: the exact lines, because the class and the
	// call also occur elsewhere in the row.
	const call = `const s3Only = s3OnlyBackupWarning(srv);`
	const mount = `if (s3Only) box.append(el("p", { class: "form-msg err", text: s3Only }));`
	grid, c, m, sched := strings.Index(row, "box.append(grid);"), strings.Index(row, call), strings.Index(row, mount), strings.Index(row, "if (srv.schedule_every)")
	if grid < 0 || c < 0 || m < 0 || sched < 0 || !(grid < c && c < m && m < sched) {
		t.Errorf("the S3-only warning is not computed and mounted in red between the fields and the schedule block (grid %d, call %d, mount %d, schedule %d)", grid, c, m, sched)
	}
	if strings.Contains(row, "As set up, each scheduled run takes a full backup") {
		t.Error("the old schedule-gated grey hint is still rendered next to the new red line")
	}
	// The last-run remedy is not repeated in grey under the red next-run one.
	if !strings.Contains(card, `run.why_code !== everyRunCode`) || !strings.Contains(card, `let alarm = false, everyRunCode = "";`) {
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
	}, "\n") + `
const lines = [];
const el = (tag, o) => ({ class: o.class, text: o.text });
function nextRun(sch) {
  const body = { append: (n) => lines.push(n) };
  let alarm = false, everyRunCode = "";
  ` + nextRunBranch(t, card) + `
  return [alarm, everyRunCode];
}
const srv = (o) => Object.assign({ baseline_dir: "", baseline_s3: "", full_backup_possible: true }, o);
const out = {
  warn: [
    s3OnlyBackupWarning(srv({ baseline_s3: "s3://b/p" })),
    s3OnlyBackupWarning(srv({ baseline_dir: "/var/lib/bintrail/baselines/a", baseline_s3: "s3://b/p" })),
    s3OnlyBackupWarning(srv({})),
    s3OnlyBackupWarning(srv({ baseline_dir: "/d" })),
    s3OnlyBackupWarning(srv({ baseline_s3: "s3://b/p", full_backup_possible: false })),
    s3OnlyBackupWarning(null),
  ],
  alarms: [
    nextRun({ runnable: true, next_method: "full", next_method_why: "an update from the recorded changes needs a local backup directory", next_method_why_code: "no_local_dir" }),
    nextRun({ runnable: true, next_method: "full", next_method_why: "no previous backup to update", next_method_why_code: "first_backup" }),
    nextRun({ runnable: true, next_method: "refresh", next_method_why: "no load on your database" }),
    nextRun({ runnable: true, next_method: "full", next_method_why: "this server has no index connection to read the recorded changes from", next_method_why_code: "no_index" }),
    nextRun({ runnable: true, next_method: "refresh", next_method_why: "x", next_method_why_code: "no_local_dir" }),
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
		Alarms [][]any
		Lines  []struct{ Class, Text string }
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for i, w := range got.Warn {
		t.Logf("warning %d: %q", i, w)
	}
	const fullRead = "With S3 only, every scheduled backup reads your whole database. Add a Backup dir so runs update from the recorded changes."
	if got.Warn[0] != fullRead {
		t.Errorf("S3 without a folder is not warned with the agreed sentence: %q", got.Warn[0])
	}
	for _, i := range []int{1, 2, 3, 5} {
		if got.Warn[i] != "" {
			t.Errorf("case %d warned where it must not: %q", i, got.Warn[i])
		}
	}
	// Where no full backup is possible either, nothing runs: saying every run
	// reads the database would be false.
	if w := got.Warn[4]; w == "" || w == fullRead || !strings.Contains(w, "cannot run") || !strings.Contains(w, "Backup dir") {
		t.Errorf("S3 without a folder on a daemon that cannot take a full backup: %q", w)
	}
	wantAlarm := []bool{true, false, false, true, false}
	wantCode := []string{"no_local_dir", "", "", "no_index", ""}
	for i := range wantAlarm {
		if len(got.Alarms) <= i || got.Alarms[i][0] != wantAlarm[i] || got.Alarms[i][1] != wantCode[i] {
			t.Errorf("next-run case %d: alarm/code = %v, want %v %q", i, got.Alarms, wantAlarm[i], wantCode[i])
		}
	}
	if len(got.Lines) != 5 {
		t.Fatalf("rendered %d next-run lines, want 5: %+v", len(got.Lines), got.Lines)
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
	if got.Lines[0].Class != "form-msg err" || !strings.Contains(got.Lines[0].Text, "Set a Backup dir for this server") {
		t.Errorf("no Backup dir: not a red line naming the setting: %+v", got.Lines[0])
	}
	if got.Lines[1].Class != "form-hint" || got.Lines[2].Class != "form-hint" || got.Lines[4].Class != "form-hint" {
		t.Errorf("a first backup or an update is not a hint: %+v", got.Lines)
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newScheduleServer(t, tc.rep)
			s3only, err := srv.cm.reg.Add(ServerEntry{Name: "s3only", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx3",
				SourceDSN: "src:pw@tcp(127.0.0.1:3306)/", BaselineS3: "s3://bucket/backups"})
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
