package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestFirstRunCardRendersTheReport is #1606: the Overview card draws the step
// list the server computes, from a report Go actually marshals. A waiting step
// never reads as failed, a failed one shows the error and the fix, and a
// complete report draws nothing.
func TestFirstRunCardRendersTheReport(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	yes := true
	marshal := func(in firstRunInput) string {
		b, err := json.Marshal(firstRunSteps(in))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	failed := marshal(firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "Access denied for user 'repl'"}, IndexExists: &yes})
	working := marshal(firstRunInput{Monitor: MonitorStatus{State: "running"}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true,
		Backup: &BaselineStatus{State: "idle"}})
	checkErr := marshal(firstRunInput{Monitor: MonitorStatus{State: "pending"}, CheckError: "dial tcp 10.0.0.1:3306: connection refused"})
	complete := marshal(firstRunInput{Monitor: MonitorStatus{State: "running"}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true, EventsIndexed: 3})

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
document.importNode = (n) => n;
const flat = (n, out = []) => { if (!n) return out; if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const rows = (n, out = []) => { if (!n) return out; if ((n.className || "").includes("fr-step ")) { out.push({ cls: n.className, text: flat(n).join(" ") }); return out; }
  for (const c of n.children || []) rows(c, out); return out; };
const card = vm.runInContext("firstRunCard", ctx);
const draw = (r) => { const c = card(r); return c ? { rows: rows(c), text: flat(c).join(" ") } : null; };
console.log(JSON.stringify({
  failed: draw(` + failed + `), working: draw(` + working + `), checkErr: draw(` + checkErr + `), complete: draw(` + complete + `),
}));
`
	path := filepath.Join(t.TempDir(), "firstrun.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type drawn struct {
		Rows []struct{ Cls, Text string }
		Text string
	}
	var got struct{ Failed, Working, CheckErr, Complete *drawn }
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}

	if got.Complete != nil {
		t.Errorf("a complete report still draws a card: %+v", got.Complete)
	}
	for name, d := range map[string]*drawn{"failed": got.Failed, "working": got.Working, "checkErr": got.CheckErr} {
		if d == nil {
			t.Fatalf("%s: no card drawn", name)
		}
	}

	if n := len(got.Failed.Rows); n != 4 {
		t.Fatalf("failed: %d step rows, want 4: %+v", n, got.Failed.Rows)
	}
	f := got.Failed.Rows[1]
	if !strings.Contains(f.Cls, "failed") || !strings.Contains(f.Text, "Access denied for user 'repl'") || !strings.Contains(f.Text, "Capture retries on its own") {
		t.Errorf("the failed step does not show its error and fix: %+v", f)
	}
	if !strings.Contains(got.Failed.Rows[0].Cls, "done") {
		t.Errorf("the index step is not drawn done: %+v", got.Failed.Rows[0])
	}
	for _, r := range got.Failed.Rows[2:] {
		if !strings.Contains(r.Cls, "waiting") || strings.Contains(strings.ToLower(r.Text), "fail") {
			t.Errorf("a step after the failure reads as anything but waiting: %+v", r)
		}
	}

	if n := len(got.Working.Rows); n != 5 {
		t.Fatalf("working: %d step rows, want 5 with the backup: %+v", n, got.Working.Rows)
	}
	if r := got.Working.Rows[3]; !strings.Contains(r.Cls, "running") || !strings.Contains(r.Text, "A quiet database is normal") {
		t.Errorf("capture waiting for its first change is not drawn as running and normal: %+v", r)
	}
	if r := got.Working.Rows[4]; !strings.Contains(r.Cls, "waiting") || !strings.Contains(r.Text, "Backups page") {
		t.Errorf("the backup step does not say where to create one: %+v", r)
	}

	if !strings.Contains(got.CheckErr.Text, "connection refused") {
		t.Errorf("a failed index check is not shown: %q", got.CheckErr.Text)
	}

	// No em dash in any literal the card holds.
	body := jsFunctionSpan(t, readAsset(t, "app.js"), "firstRunCard")
	for _, m := range regexp.MustCompile(`"([^"\n]*)"`).FindAllStringSubmatch(body, -1) {
		if strings.Contains(m[1], "—") {
			t.Errorf("firstRunCard holds an em dash in %q", m[1])
		}
	}
}

// TestOverviewPollsFirstRun: the Overview asks for the list only where the
// endpoint answers (a supervisor console), and draws it into its own slot.
func TestOverviewPollsFirstRun(t *testing.T) {
	js := readAsset(t, "app.js")
	render := jsFunctionBody(t, js, "renderOverview")
	if !strings.Contains(render, "watchFirstRun(") {
		t.Error("renderOverview does not start the first-run list")
	}
	watch := jsFunctionBody(t, js, "watchFirstRun")
	for _, want := range []string{"capsCache.monitor", "/first-run", "firstRunCard("} {
		if !strings.Contains(watch, want) {
			t.Errorf("watchFirstRun does not use %s", want)
		}
	}
}
