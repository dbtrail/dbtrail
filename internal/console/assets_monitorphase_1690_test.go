package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestMonitorChipRendersThePhase is #1690: a stream can sit in the pre-capture
// cleanup for half an hour, and the row said PENDING the whole time. The chip
// now reads the phase instead — but only for a phase this build knows, and
// without changing anything for a stream that has none.
//
// The inputs are marshalled from the real serverDTO, so the wire key the chip
// reads (monitor_phase) is pinned against the struct tag rather than retyped
// into the test.
func TestMonitorChipRendersThePhase(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	marshal := func(dto serverDTO) string {
		b, err := json.Marshal(dto)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	cleaning := marshal(serverDTO{MonitorState: "pending", MonitorPhase: "resume_cleanup"})
	plainPending := marshal(serverDTO{MonitorState: "pending"})
	running := marshal(serverDTO{MonitorState: "running"})
	lost := marshal(serverDTO{MonitorState: "lost_position"})
	// A phase a NEWER daemon might report to an older console. The chip must
	// fall back to the state, not render a blank or the raw key: the console
	// and the daemon are separate binaries and an operator can run a mixed
	// pair across an upgrade.
	unknown := marshal(serverDTO{MonitorState: "pending", MonitorPhase: "some_future_step"})

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const chip = vm.runInContext("monitorChip", ctx);
const draw = (s) => { const c = chip(s); return { text: c._text, title: (c.attrs || {}).title || c.title || "", cls: c.className }; };
console.log(JSON.stringify({
  cleaning: draw(` + cleaning + `), plainPending: draw(` + plainPending + `), running: draw(` + running + `),
  lost: draw(` + lost + `), unknown: draw(` + unknown + `),
}));
`
	path := filepath.Join(t.TempDir(), "monitorphase.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type chip struct{ Text, Title, Cls string }
	var got struct{ Cleaning, PlainPending, Running, Lost, Unknown chip }
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}

	// The phase replaces the WORD, which is the whole point: PENDING for 28
	// minutes is what sent the operator in #1690 probing for a hung process.
	if got.Cleaning.Text != "CLEANING UP" {
		t.Errorf("a stream in the resume cleanup reads %q, want CLEANING UP", got.Cleaning.Text)
	}
	if !strings.Contains(got.Cleaning.Title, "counted twice") || !strings.Contains(got.Cleaning.Title, "takes minutes") {
		t.Errorf("the phase tooltip does not say what is happening or how long: %q", got.Cleaning.Title)
	}
	// Same chip class either way: the phase is presentation on top of an
	// unchanged state, not a new state.
	if got.Cleaning.Cls != got.PlainPending.Cls {
		t.Errorf("the phase chip uses a different class (%q) from the state chip (%q)", got.Cleaning.Cls, got.PlainPending.Cls)
	}

	// Without a phase — every stream that is not mid-cleanup, which is nearly
	// all of them — nothing changed. This is the regression the refactor into
	// one shared monitorChip() could have introduced.
	for name, c := range map[string]struct {
		got  chip
		want string
	}{
		"pending":       {got.PlainPending, "PENDING"},
		"running":       {got.Running, "RUNNING"},
		"lost_position": {got.Lost, "LOST POSITION"},
		"unknown phase": {got.Unknown, "PENDING"},
	} {
		if c.got.Text != c.want {
			t.Errorf("%s renders %q, want %q", name, c.got.Text, c.want)
		}
	}
	if got.Unknown.Title != got.PlainPending.Title {
		t.Errorf("an unknown phase changed the tooltip (%q) instead of falling back (%q)", got.Unknown.Title, got.PlainPending.Title)
	}
	if got.Lost.Title == "" || !strings.Contains(got.Lost.Title, "permanently lost") {
		t.Errorf("the pre-existing state tooltips did not survive the refactor: %q", got.Lost.Title)
	}
}

// TestServersListRefreshesWhileAPhaseIsRunning: nothing else in the Servers
// dialog polls, which is fine for a state and wrong for a phase. A phase names
// a step that ENDS; a frozen CLEANING UP asserts the daemon is still stuck in
// that exact step long after it finished, which is the misreading the chip
// exists to prevent. The list must therefore re-fetch itself while a phase is
// showing — and must NOT poll otherwise, or the dialog acquires a timer every
// server in the product has to pay for.
func TestServersListRefreshesWhileAPhaseIsRunning(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	payload := func(dto serverDTO) string {
		b, err := json.Marshal(serversResponse{Servers: []serverDTO{dto}, DefaultID: dto.ID})
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	cleaning := payload(serverDTO{ID: "s1", Name: "prod", MonitorState: "pending", MonitorPhase: "resume_cleanup"})
	running := payload(serverDTO{ID: "s1", Name: "prod", MonitorState: "running"})

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
// Record what the page schedules instead of actually scheduling it.
let scheduled = [];
ctx.setTimeout = (fn, ms) => { scheduled.push(ms); return scheduled.length; };
ctx.clearTimeout = () => {};
const serve = (body) => { ctx.fetch = () => Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve(body) }); };
const refresh = vm.runInContext("refreshServersList", ctx);
(async () => {
  serve(` + "`" + cleaning + "`" + `); scheduled = []; await refresh(); const withPhase = scheduled.slice();
  serve(` + "`" + running + "`" + `);  scheduled = []; await refresh(); const without  = scheduled.slice();
  console.log(JSON.stringify({ withPhase, without }));
})().catch((e) => { console.log(JSON.stringify({ error: String(e && e.stack || e) })); });
`
	path := filepath.Join(t.TempDir(), "serverspoll.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		WithPhase []int
		Without   []int
		Error     string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Error != "" {
		t.Fatalf("the harness threw: %s", got.Error)
	}
	if len(got.WithPhase) != 1 {
		t.Errorf("a row reporting a phase scheduled %v re-fetches, want exactly 1 — a frozen chip is the #1690 misreading all over again", got.WithPhase)
	} else if got.WithPhase[0] <= 0 || got.WithPhase[0] > 15000 {
		t.Errorf("the re-fetch delay is %dms; want a positive delay no slower than the 15s this console already uses for its slowest poll", got.WithPhase[0])
	}
	if len(got.Without) != 0 {
		t.Errorf("a list with no phase scheduled %v: the dialog must not poll when there is nothing to watch", got.Without)
	}
}
