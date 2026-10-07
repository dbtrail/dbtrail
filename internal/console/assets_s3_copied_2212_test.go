package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #2212: a table an S3-only update copied inside S3 was neither rewritten nor
// "written in full", and it was copied whether or not it had a chain beside
// it. The two run lines (the automatic refresh, and the schedule's last run)
// count it on its own, from the s3_copied number, and never fold it into the
// reuse note that speaks of disk. Generated with the page's own functions.
func TestRunLines_countTablesCopiedInsideS3(t *testing.T) {
	js := readAsset(t, "app.js")
	card := functionBody(t, js, "function backupScheduleCard(")
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	script := strings.Join([]string{
		functionBody(t, js, "function baselineRefreshNote("),
		functionBody(t, js, "function reusedCopiedNote("),
		functionBody(t, js, "function s3CopiedNote("),
	}, "\n") + `
const backupFoldError = (e) => e, budgetRefusedTail = () => "", utcLabel = (s) => s;
const el = (tag, o) => ({ cls: o.class, text: o.text });
const leftOutTablesBlock = () => null, newTablesBlock = () => null, b = null;
function lastRun(sch) {
  const out = [], body = { append: (n) => out.push(n.text) };
  let alarm = false, everyRunCode = "";
  const noteAt = () => {};
  ` + lastRunBlock(t, card) + `
  return out[0];
}
console.log(JSON.stringify([
  baselineRefreshNote({ state: "succeeded", finished_at: "T", tables: 12, s3_copied: 11 }).text,
  baselineRefreshNote({ state: "succeeded", finished_at: "T", tables: 12, carried: 3, carried_copied: 1 }).text,
  lastRun({ last_run: { ok: true, finished_at: "T", method: "refresh", tables: 12, s3_copied: 11, uploaded: 14 } }),
]));
`
	path := filepath.Join(t.TempDir(), "copied.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got []string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for _, g := range got {
		t.Log(g)
	}
	want := []string{
		"Automatic refresh at T: 1 table(s) refreshed, 11 unchanged and copied inside S3 without being downloaded.",
		"Automatic refresh at T: 9 table(s) refreshed, 3 unchanged and reused (1 of them written in full, which saved no disk; DBTrail's log says why).",
		"Last scheduled snapshot finished T (update from the recorded changes): 12 table(s), 11 unchanged and copied inside S3 without being downloaded, 14 file(s) uploaded.",
	}
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Errorf("line %d:\n got %q\nwant %q", i, got[i], want[i])
		}
	}
}

// The count reaches the page under the name the page reads, from the live
// status, the run history and the schedule view.
func TestS3CopiedWireName(t *testing.T) {
	for _, v := range []any{BaselineStatus{S3Copied: 2}, BaselineRunRecord{S3Copied: 2}, backupScheduleRunDTO{S3Copied: 2}} {
		b, _ := json.Marshal(v)
		if !strings.Contains(string(b), `"s3_copied":2`) {
			t.Errorf("%T marshals without s3_copied: %s", v, b)
		}
	}
}
