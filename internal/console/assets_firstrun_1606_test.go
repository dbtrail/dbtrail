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

	// One step is open on the card: the failed one when there is one, the
	// running one otherwise (the rest fold behind "All N steps").
	current := func(rows []struct{ Cls, Text string }) (int, int) {
		n, at := 0, -1
		for i, r := range rows {
			if strings.Contains(r.Cls, "fr-cur") {
				n++
				at = i
			}
		}
		return n, at
	}
	if n, at := current(got.Failed.Rows); n != 1 || at != 1 {
		t.Errorf("failed: %d current step(s) at %d, want exactly the failed step (1): %+v", n, at, got.Failed.Rows)
	}
	if !strings.Contains(got.Failed.Text, "All 5 steps · 1 done") {
		t.Errorf("failed: the fold does not count the steps: %q", got.Failed.Text)
	}

	if n := len(got.Working.Rows); n != 6 {
		t.Fatalf("working: %d step rows, want 6 with the snapshot: %+v", n, got.Working.Rows)
	}
	if n, at := current(got.Working.Rows); n != 1 || at != 4 {
		t.Errorf("working: %d current step(s) at %d, want exactly the running step (4): %+v", n, at, got.Working.Rows)
	}
	if r := got.Working.Rows[4]; !strings.Contains(r.Cls, "running") || !strings.Contains(r.Text, "A quiet database is normal") {
		t.Errorf("capture waiting for its first change is not drawn as running and normal: %+v", r)
	}
	if r := got.Working.Rows[5]; !strings.Contains(r.Cls, "waiting") || !strings.Contains(r.Text, PageSnapshots+" page") {
		t.Errorf("the snapshot step does not say where to create one: %+v", r)
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
// says it could not be refreshed, and a complete report takes the list away.
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
	b, _ = json.Marshal(firstRunSteps(firstRunInput{CheckError: "Error 1045: Access denied for user 'idx'"}))
	checkErr := string(b)
	b, _ = json.Marshal(firstRunSteps(firstRunInput{Monitor: MonitorStatus{State: "pending", SourceConnected: true}, IndexExists: &yes}))
	moved := string(b)
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
  // The page bounds each request with a deadline of its own (#1801), which
  // is a timer too. This test is about the POLL cadence, so the deadline is
  // filtered out by its length, read from the page rather than repeated.
  // The residue: a poll cadence of exactly OV_REQUEST_MS would be filtered
  // out with it and stop being counted. Harmless while the backoff caps at
  // 15s and the settled wait is 2min, neither of which can reach 20s, but a
  // new cadence between them wants a different discriminator than length.
  const REQ_MS = vm.runInContext("OV_REQUEST_MS", ctx);
  const timers = [], delays = [];
  ctx.setTimeout = (fn, ms) => { if (ms !== REQ_MS) { timers.push(fn); delays.push(ms); } return 1; };
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
  out.rendered = ctx.rendered; out.timersAfterComplete = timers.length; out.slotAfterComplete = f.firstRunSlot.children.length;

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

  // A report the index could not be read for tries again before the list is up...
  timers.length = 0; queue = [` + checkErr + `, ` + running + `];
  f = { firstRunSlot: new FakeEl("div") };
  watch(f, () => true); await flush();
  out.retryAfterCheckError = timers.length;
  timers.shift()(); await flush();
  out.cardAfterCheckError = f.firstRunSlot.children.length;

  // ...and notes it right away once the list is up, without rendering the page.
  timers.length = 0; ctx.rendered = 0; queue = [` + running + `, ` + checkErr + `];
  f = { firstRunSlot: new FakeEl("div") };
  watch(f, () => true); await flush();
  timers.shift()(); await flush();
  out.checkErrorNote = flat(f.firstRunSlot).join(" ");
  out.checkErrorRendered = ctx.rendered; out.checkErrorTimers = timers.length;

  out.refusals = {};
  for (const status of [401, 403, 404]) {
    timers.length = 0; queue = [fail(status)];
    watch({ firstRunSlot: new FakeEl("div") }, () => true); await flush();
    out.refusals[status] = timers.length;
  }

  // The wait doubles while the list is unchanged and goes back to 3s when it changes.
  timers.length = 0; delays.length = 0; queue = [` + running + `, ` + running + `, ` + moved + `];
  watch({ firstRunSlot: new FakeEl("div") }, () => true); await flush();
  timers.shift()(); await flush();
  timers.shift()(); await flush();
  out.delays = delays.slice();

  // A view that is gone stops the loop: no request and no new timer.
  timers.length = 0; calls = 0; queue = [` + running + `, ` + running + `];
  let alive = true;
  watch({ firstRunSlot: new FakeEl("div") }, () => alive); await flush();
  alive = false;
  timers.shift()(); await flush();
  out.goneCalls = calls; out.goneTimers = timers.length;
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
		SlotAfterComplete                                                                                       int
		RetryAfterCheckError, CardAfterCheckError, CheckErrorRendered, CheckErrorTimers, GoneCalls, GoneTimers  int
		StaleText, CheckErrorNote                                                                               string
		Refusals                                                                                                map[string]int
		Delays                                                                                                  []int
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.RetryAfterFirstFailure != 1 || got.CardAfterRetry != 1 {
		t.Errorf("a failed first request does not try again and draw the list: %+v", got)
	}
	// A complete report takes the list away and stops, without rendering the
	// page again (#1801): the Overview keeps its own numbers current, and a
	// repaint would flash every card and reset the reader's place.
	if got.Rendered != 0 || got.TimersAfterComplete != 0 || got.SlotAfterComplete != 0 {
		t.Errorf("a complete report does not take the list away and stop without a repaint: %+v", got)
	}
	if got.TimersAfterRefusal != 0 {
		t.Errorf("a 409 keeps polling: %+v", got)
	}
	if !strings.Contains(got.StaleText, "Could not refresh this list") || !strings.Contains(got.StaleText, "Getting started") || got.StillPolling != 1 {
		t.Errorf("a list already up does not stay with a refresh note while it keeps trying: %+v", got)
	}
	if got.RetryAfterCheckError != 1 || got.CardAfterCheckError != 1 {
		t.Errorf("an unreadable index before the list is up does not try again: %+v", got)
	}
	if !strings.Contains(got.CheckErrorNote, "Could not refresh this list: Error 1045") || got.CheckErrorRendered != 0 || got.CheckErrorTimers != 1 {
		t.Errorf("an unreadable index with the list up does not note it at once and keep trying: %+v", got)
	}
	for _, status := range []string{"401", "403", "404"} {
		if n, ok := got.Refusals[status]; !ok || n != 0 {
			t.Errorf("a %s keeps polling: %+v", status, got.Refusals)
		}
	}
	if len(got.Delays) != 3 || got.Delays[0] != 3000 || got.Delays[1] != 6000 || got.Delays[2] != 3000 {
		t.Errorf("waits = %v, want [3000 6000 3000]: doubling while unchanged, back to 3s on a change", got.Delays)
	}
	if got.GoneCalls != 1 || got.GoneTimers != 0 {
		t.Errorf("a loop outlives its view: %d requests, %d timers", got.GoneCalls, got.GoneTimers)
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
