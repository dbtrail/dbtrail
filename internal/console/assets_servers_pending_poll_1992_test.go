package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// #1992: a server is `pending` from launch until its first checkpoint, with
// or without a named startup step. The Servers list re-polls while ANY row is
// in a state that changes on its own (pending, failed while retrying,
// stalled) or reports a phase, stops once every row is in a state only an
// operator changes, and a timer that fires after the dialog closed neither
// fetches nor re-arms.
func TestServersListRePollsWhilePending_1992(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	payload := func(dtos ...serverDTO) string {
		b, err := json.Marshal(serversResponse{Servers: dtos, DefaultID: dtos[0].ID})
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	cases := map[string]string{
		"pendingNoPhase":   payload(serverDTO{ID: "s1", Name: "prod", MonitorState: "pending"}),
		"pendingWithPhase": payload(serverDTO{ID: "s1", Name: "prod", MonitorState: "pending", MonitorPhase: "resume_cleanup"}),
		"onePendingOfTwo":  payload(serverDTO{ID: "s1", Name: "a", MonitorState: "running"}, serverDTO{ID: "s2", Name: "b", MonitorState: "pending"}),
		"running":          payload(serverDTO{ID: "s1", Name: "prod", MonitorState: "running"}),
		"stopped":          payload(serverDTO{ID: "s1", Name: "prod", MonitorState: "stopped"}),
		"failed":           payload(serverDTO{ID: "s1", Name: "prod", MonitorState: "failed"}),
		"stalled":          payload(serverDTO{ID: "s1", Name: "prod", MonitorState: "stalled"}),
		"lostPosition":     payload(serverDTO{ID: "s1", Name: "prod", MonitorState: "lost_position"}),
		"noMonitor":        payload(serverDTO{ID: "s1", Name: "prod"}),
	}
	raw, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
let scheduled = [], fetches = 0;
ctx.setTimeout = (fn, ms) => { scheduled.push({ fn, ms }); return scheduled.length; };
ctx.clearTimeout = () => {};
const serve = (body) => { ctx.fetch = () => { fetches++; return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve(body) }); }; };
const refresh = vm.runInContext("refreshServersList", ctx);
const cases = ` + string(raw) + `;
(async () => {
  const out = {};
  for (const [name, body] of Object.entries(cases)) {
    serve(body); scheduled = []; await refresh();
    out[name] = scheduled.map((s) => s.ms);
  }
  // pending -> the armed timer fires -> running: the row flips without
  // reopening, and no timer is left behind.
  serve(cases.pendingNoPhase); scheduled = []; await refresh();
  const armed = scheduled.slice();
  serve(cases.running); scheduled = []; fetches = 0;
  if (armed[0]) await armed[0].fn(); else fetches = -1;
  out.flip = { fetched: fetches, rearmed: scheduled.length, text: (document.getElementById("servers-list") || {}).textContent || "" };
  // Dialog closed while armed: the timer neither fetches nor re-arms.
  serve(cases.pendingNoPhase); scheduled = []; await refresh();
  const armed2 = scheduled.slice();
  const byId = document.getElementById;
  document.getElementById = () => null;
  scheduled = []; fetches = 0;
  if (armed2[0]) await armed2[0].fn();
  out.closed = { fetched: fetches, rearmed: scheduled.length };
  document.getElementById = byId;
  console.log(JSON.stringify(out));
})().catch((e) => { console.log(JSON.stringify({ error: String(e && e.stack || e) })); });
`
	path := filepath.Join(t.TempDir(), "pendingpoll.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got struct {
		Error                                                      string
		PendingNoPhase, PendingWithPhase, OnePendingOfTwo          []int
		Running, Stopped, Failed, Stalled, LostPosition, NoMonitor []int
		Flip                                                       struct{ Fetched, Rearmed int }
		Closed                                                     struct{ Fetched, Rearmed int }
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if got.Error != "" {
		t.Fatalf("the harness threw: %s", got.Error)
	}
	// failed is retried after a backoff and stalled recovers on its own, so a
	// row in either keeps the list polling, like pending.
	for name, s := range map[string][]int{"pending, no phase": got.PendingNoPhase, "pending, with a phase": got.PendingWithPhase, "one pending of two": got.OnePendingOfTwo,
		"failed (retrying)": got.Failed, "stalled": got.Stalled} {
		if len(s) != 1 || s[0] <= 0 || s[0] > 15000 {
			t.Errorf("%s: scheduled %v, want exactly one re-poll within 15s", name, s)
		}
	}
	for name, s := range map[string][]int{"running": got.Running, "stopped": got.Stopped, "lost position": got.LostPosition, "no monitor": got.NoMonitor} {
		if len(s) != 0 {
			t.Errorf("%s: scheduled %v, want no timer left behind", name, s)
		}
	}
	if got.Flip.Fetched != 1 || got.Flip.Rearmed != 0 {
		t.Errorf("the armed re-poll fetched %d time(s) and re-armed %d; want one fetch and no timer once running", got.Flip.Fetched, got.Flip.Rearmed)
	}
	if got.Closed.Fetched != 0 || got.Closed.Rearmed != 0 {
		t.Errorf("a timer firing after the dialog closed fetched %d time(s) and re-armed %d; want neither", got.Closed.Fetched, got.Closed.Rearmed)
	}
}
