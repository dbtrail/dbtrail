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
	failed := marshal(firstRunInput{Monitor: MonitorStatus{State: "failed", LastError: "Access denied for user 'repl' (retrying)", Retrying: true}, IndexExists: &yes})
	working := marshal(firstRunInput{Monitor: MonitorStatus{State: "running", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true,
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
	if got.CheckErr != nil {
		t.Errorf("an index that could not be read draws steps: %+v", got.CheckErr)
	}
	for name, d := range map[string]*drawn{"failed": got.Failed, "working": got.Working} {
		if d == nil {
			t.Fatalf("%s: no card drawn", name)
		}
	}

	if n := len(got.Failed.Rows); n != 5 {
		t.Fatalf("failed: %d step rows, want 5: %+v", n, got.Failed.Rows)
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

	if n := len(got.Working.Rows); n != 6 {
		t.Fatalf("working: %d step rows, want 6 with the backup: %+v", n, got.Working.Rows)
	}
	if r := got.Working.Rows[4]; !strings.Contains(r.Cls, "running") || !strings.Contains(r.Text, "A quiet database is normal") {
		t.Errorf("capture waiting for its first change is not drawn as running and normal: %+v", r)
	}
	if r := got.Working.Rows[5]; !strings.Contains(r.Cls, "waiting") || !strings.Contains(r.Text, "Backups page") {
		t.Errorf("the backup step does not say where to create one: %+v", r)
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

// TestWatchFirstRunKeepsTrying: a failure before the list is up tries again
// instead of hiding it for good, a refusal stops the loop, a list already up
// says it could not be refreshed, and a complete report renders the page.
func TestWatchFirstRunKeepsTrying(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	yes := true
	b, _ := json.Marshal(firstRunSteps(firstRunInput{Monitor: MonitorStatus{State: "pending"}, IndexExists: &yes}))
	running := string(b)
	b, _ = json.Marshal(firstRunSteps(firstRunInput{Monitor: MonitorStatus{State: "running"}, IndexExists: &yes, StreamStarted: true, EventsIndexed: 1}))
	complete := string(b)
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
document.importNode = (n) => n;
const flat = (n, out = []) => { if (!n) return out; if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
(async () => {
  const flush = async () => { for (let i = 0; i < 10; i++) await Promise.resolve(); };
  const timers = []; ctx.setTimeout = (fn) => { timers.push(fn); return 1; };
  let queue = [], calls = 0;
  ctx.nextApi = () => { calls++; const s = queue.shift(); return s instanceof Error ? Promise.reject(s) : Promise.resolve(s); };
  ctx.rendered = 0;
  vm.runInContext("api = (p) => nextApi(p); renderRoute = () => { rendered++; }; capsCache = { monitor: true }; currentServer = 'a';", ctx);
  const watch = vm.runInContext("watchFirstRun", ctx);
  const fail = (status) => Object.assign(new Error("HTTP " + status), { status });
  const out = {};

  queue = [fail(502), ` + running + `, ` + complete + `];
  let f = { firstRunSlot: new FakeEl("div") };
  watch(f, () => true); await flush();
  out.retryAfterFirstFailure = timers.length;
  timers.shift()(); await flush();
  out.cardAfterRetry = f.firstRunSlot.children.length;
  timers.shift()(); await flush();
  out.rendered = ctx.rendered; out.timersAfterComplete = timers.length;

  timers.length = 0; queue = [fail(409)];
  f = { firstRunSlot: new FakeEl("div") };
  watch(f, () => true); await flush();
  out.timersAfterRefusal = timers.length;

  timers.length = 0; queue = [` + running + `, fail(502), fail(502), fail(502)];
  f = { firstRunSlot: new FakeEl("div") };
  watch(f, () => true); await flush();
  for (let i = 0; i < 3; i++) { timers.shift()(); await flush(); }
  out.staleText = flat(f.firstRunSlot).join(" ");
  out.stillPolling = timers.length;
  console.log(JSON.stringify(out));
})();
`
	path := filepath.Join(t.TempDir(), "watch.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		RetryAfterFirstFailure, CardAfterRetry, Rendered, TimersAfterComplete, TimersAfterRefusal, StillPolling int
		StaleText                                                                                               string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.RetryAfterFirstFailure != 1 || got.CardAfterRetry != 1 {
		t.Errorf("a failed first request does not try again and draw the list: %+v", got)
	}
	if got.Rendered != 1 || got.TimersAfterComplete != 0 {
		t.Errorf("a complete report does not render the page once and stop: %+v", got)
	}
	if got.TimersAfterRefusal != 0 {
		t.Errorf("a 409 keeps polling: %+v", got)
	}
	if !strings.Contains(got.StaleText, "Could not refresh this list") || !strings.Contains(got.StaleText, "Getting started") || got.StillPolling != 1 {
		t.Errorf("a list already up does not stay with a refresh note while it keeps trying: %+v", got)
	}
}

// TestClosingServersRendersOverview: a server added or started in the dialog
// gets its Getting started list without leaving the page.
func TestClosingServersRendersOverview(t *testing.T) {
	js := readAsset(t, "app.js")
	body := jsFunctionBody(t, js, "closeServersModal")
	if !strings.Contains(body, `routeFromLocation() === "overview"`) || !strings.Contains(body, "renderRoute()") {
		t.Error("closing the servers dialog does not render the Overview again")
	}
	// Escape empties the shared dialog slot itself; for the servers dialog it
	// must go through the same close.
	keys := jsFunctionBody(t, js, "globalKeydown")
	if !strings.Contains(keys, `querySelector("#servers-list")`) || !strings.Contains(keys, "closeServersModal()") {
		t.Error("Escape closes the servers dialog without rendering the Overview again")
	}
}
