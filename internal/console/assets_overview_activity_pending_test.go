package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// While the first activity count runs, the server answers 503 with a message
// saying so. The Overview must show THAT message, not "could not be loaded",
// and ask again after the server's retry_after: on a quiet index nothing
// else would ask for minutes. Driven through the real app.js in node, with the
// message the server really writes.
const activityPendingHarnessJS = `
FakeEl.prototype.replaceWith = function () {};
const fn = (name) => vm.runInContext(name, ctx);
const msg = process.argv[3];
const out = {};
const f = fn("ovFrame")();
const e503 = Object.assign(new Error(msg), { status: 503, body: { error: msg, retry_after: 30 } });
fn("fillOvActivity")(f, null, e503);
out.warn = f.warnSlot.textContent;
out.deletes = f.statDeletes.textContent;
fn("fillOvActivity")(f, null, Object.assign(new Error("boom"), { status: 500 }));
out.warn500 = f.warnSlot.textContent;
out.retry = [{ body: { retry_after: 30 } }, { body: { retry_after: 1 } }, { body: { retry_after: 600 } }, {}].map((e) => fn("ovActivityRetryMs")(e));
const timers = [];
ctx.setTimeout = (cb, ms) => { timers.push(ms); return 1; };
vm.runInContext("api = () => Promise.reject(__e503)", Object.assign(ctx, { __e503: e503 }));
fn("loadOvActivity")(f, () => true).then(() => {
  out.timers = timers.slice();
  out.warnAfterLoad = f.warnSlot.textContent;
  process.stdout.write("\n@@RESULT@@" + JSON.stringify(out));
});
`

func TestOverviewActivityPendingShowsTheServersMessage(t *testing.T) {
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
	path := filepath.Join(t.TempDir(), "pending.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+activityPendingHarnessJS), 0o644); err != nil {
		t.Fatal(err)
	}
	// The real server text, as the handler writes it.
	msg := (&activityUnavailableError{running: true, since: time.Now().Add(-40 * time.Second)}).Error()
	raw, err := exec.Command(node, path, appJS, msg).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	_, result, found := strings.Cut(string(raw), "@@RESULT@@")
	if !found {
		t.Fatalf("no result in node output:\n%s", raw)
	}
	var out struct {
		Warn, Deletes, Warn500, WarnAfterLoad string
		Retry, Timers                         []int
	}
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatal(err)
	}
	t.Logf("tile warning: %s", out.Warn)
	if !strings.HasPrefix(out.Warn, "The activity counts for this server are still being computed (started 40s ago)") ||
		strings.Contains(out.Warn, "could not be loaded") {
		t.Errorf("503 warning = %q, want the server's message", out.Warn)
	}
	if strings.Contains(out.Warn, "—") {
		t.Errorf("warning carries an em dash: %q", out.Warn)
	}
	if !strings.Contains(out.Deletes, "—") {
		t.Errorf("deletes tile = %q, want no number while counting", out.Deletes)
	}
	if !strings.Contains(out.Warn500, "could not be loaded") {
		t.Errorf("a real failure lost its sentence: %q", out.Warn500)
	}
	if want := []int{30000, 5000, 60000, 10000}; len(out.Retry) != 4 || out.Retry[0] != want[0] || out.Retry[1] != want[1] || out.Retry[2] != want[2] || out.Retry[3] != want[3] {
		t.Errorf("retry delays = %v, want %v", out.Retry, want)
	}
	if len(out.Timers) != 1 || out.Timers[0] != 30000 {
		t.Errorf("after a 503 the page scheduled %v, want one retry in 30000 ms", out.Timers)
	}
	if out.WarnAfterLoad != out.Warn {
		t.Errorf("loadOvActivity rendered %q, want %q", out.WarnAfterLoad, out.Warn)
	}
}

// On a quiet index (the newest event id never moves) the live loop must still
// ask for the activity counts on its slow refresh: before, it asked only when
// a new event arrived, so a first count that finished on a quiet index stayed
// "not counted yet" until the page was reloaded.
const activityQuietHarnessJS = `
FakeEl.prototype.replaceWith = function () {};
const fn = (name) => vm.runInContext(name, ctx);
const calls = [];
const timers = [];
ctx.setTimeout = (cb, ms) => { timers.push(cb); return timers.length; };
let now = Date.parse("2026-10-18T19:00:00Z");
const RealDate = Date;
ctx.Date = class extends RealDate { constructor(...a) { super(...(a.length ? a : [now])); } static now() { return now; } };
const e503 = Object.assign(new Error("still being computed"), { status: 503, body: { retry_after: 10 } });
ctx.__answer = (p) => {
  calls.push(p);
  if (p.startsWith("/api/events/head")) return Promise.resolve({ newest_event_id: 5 });
  if (p.startsWith("/api/events")) return Promise.resolve({ events: [] });
  if (p === "/api/activity") return Promise.reject(e503);
  return Promise.resolve({});
};
vm.runInContext("apiWithin = (p) => __answer(p); api = (p) => __answer(p); fillOvEvents = () => {}; fillOvStatus = () => {}; fillOvCoverage = () => {}; loadOvFlow = () => Promise.resolve(); loadOvStrip = () => Promise.resolve(); ovTickAges = () => {}; ovTickStrip = () => {};", ctx);
const f = fn("ovFrame")();
const flush = () => new Promise((r) => setImmediate(r));
(async () => {
  fn("fillOvActivity")(f, null, e503); // the first paint: nothing counted yet
  fn("watchOverview")(f, () => true, null);
  for (let i = 0; i < 5; i++) await flush();
  now += 6 * 60 * 1000; // past the idle slow refresh
  const t = timers.splice(0);
  t.forEach((cb) => cb());
  for (let i = 0; i < 10; i++) await flush();
  process.stdout.write("\n@@RESULT@@" + JSON.stringify({ calls, warn: f.warnSlot.textContent, retries: timers.length }));
})();
`

func TestOverviewQuietIndexStillRefreshesActivity(t *testing.T) {
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
	path := filepath.Join(t.TempDir(), "quiet.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+activityQuietHarnessJS), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	_, result, found := strings.Cut(string(raw), "@@RESULT@@")
	if !found {
		t.Fatalf("no result in node output:\n%s", raw)
	}
	var out struct {
		Calls   []string
		Warn    string
		Retries int
	}
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, c := range out.Calls {
		if c == "/api/activity" {
			n++
		}
	}
	// One from the slow refresh, one from its 503 handing over to the
	// retrying loader.
	if n < 1 {
		t.Fatalf("the quiet index's slow refresh never asked for the activity counts: %v", out.Calls)
	}
	if !strings.Contains(out.Warn, "Still being computed") {
		t.Errorf("tiles = %q, want the server's pending message", out.Warn)
	}
	if out.Retries < 2 {
		t.Errorf("%d timers armed, want the loop's next tick and the activity retry", out.Retries)
	}
}
