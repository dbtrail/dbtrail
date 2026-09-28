package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// #1708 on screen. Every input is marshalled from the structs the API sends
// (serverDTO, MonitorStatus) and the error text is the one the stream
// returns, so the keys and the sentences the page reads are the real ones.

func nodeOrSkip(t *testing.T) string {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	return node
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestMonitorChipSaysItWaitsForAnEarlierCleanup(t *testing.T) {
	node := nodeOrSkip(t)
	waiting := mustJSON(t, serverDTO{MonitorState: "pending", MonitorPhase: streamrun.PhaseResumeCleanupWaiting,
		MonitorPhaseDetail: "connection 812, running for 14m0s"})
	noDetail := mustJSON(t, serverDTO{MonitorState: "pending", MonitorPhase: streamrun.PhaseResumeCleanupWaiting})
	cleaning := mustJSON(t, serverDTO{MonitorState: "pending", MonitorPhase: streamrun.PhaseResumeCleanup})

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const chip = vm.runInContext("monitorChip", ctx);
const draw = (s) => { const c = chip(s); return { text: c._text, title: (c.attrs || {}).title || c.title || "" }; };
console.log(JSON.stringify({ waiting: draw(` + waiting + `), noDetail: draw(` + noDetail + `), cleaning: draw(` + cleaning + `) }));
`
	path := filepath.Join(t.TempDir(), "chip1708.js")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type chip struct{ Text, Title string }
	var got struct{ Waiting, NoDetail, Cleaning chip }
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	t.Logf("chip: %q, tooltip: %q", got.Waiting.Text, got.Waiting.Title)

	if got.Waiting.Text != "WAITING FOR CLEANUP" {
		t.Errorf("a start that waits reads %q", got.Waiting.Text)
	}
	if want := "an earlier cleanup is still running on the index; capture starts when it finishes. (connection 812, running for 14m0s)"; got.Waiting.Title != want {
		t.Errorf("tooltip = %q\n   want   %q", got.Waiting.Title, want)
	}
	if strings.Contains(got.NoDetail.Title, "(") || got.NoDetail.Text != "WAITING FOR CLEANUP" {
		t.Errorf("without a detail: %+v", got.NoDetail)
	}
	// The two phases are different facts and must not read alike: in one the
	// cleanup of this run is working, in the other it has not started.
	if got.Cleaning.Text != "CLEANING UP" || got.Cleaning.Title == got.NoDetail.Title {
		t.Errorf("the cleanup phase changed: %+v", got.Cleaning)
	}
}

func TestOverviewCardForAnEarlierCleanup(t *testing.T) {
	node := nodeOrSkip(t)
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	cause := (&streamrun.EarlierCleanupError{ConnectionID: 812, Running: 74 * time.Minute, Waited: time.Hour, Count: 1}).Error()
	type c = map[string]any
	status := func(st MonitorStatus) c {
		var m c
		if err := json.Unmarshal([]byte(mustJSON(t, st)), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	input := func(st MonitorStatus) c {
		return c{"input": c{
			"coverage":  c{"freshness": "current", "continuity": "ok", "delta_to": "2026-09-23 14:02:10"},
			"baselines": c{},
			"server":    c{"id": "a", "kind": "registry", "has_source": true, "monitor_state": st.State},
			"monitor":   status(st),
			"schema":    c{"state": "idle"}, "uncaptured": c{}}}
	}
	cases := map[string]c{
		"retrying": input(MonitorStatus{State: "failed", Retrying: true, ErrorCode: MonitorErrEarlierCleanup, LastError: cause + " (retrying)"}),
		"gave-up":  input(MonitorStatus{State: "failed", ErrorCode: MonitorErrEarlierCleanup, LastError: cause}),
		// The same words with no code: a daemon older than this console, or
		// any other error that happens to read alike. The page must not guess.
		"text-only": input(MonitorStatus{State: "failed", Retrying: true, LastError: cause + " (retrying)"}),
		// What the daemon reported before #1708, and still does when it cannot
		// look at the process list.
		"lock-timeout": input(MonitorStatus{State: "failed", Retrying: true,
			LastError: "failed to dedup events since checkpoint: delete events since checkpoint binlog.000042:5000: Error 1205 (HY000): Lock wait timeout exceeded; try restarting transaction (retrying)"}),
		"another-code": input(MonitorStatus{State: "failed", Retrying: true, ErrorCode: "something_newer", LastError: "boom"}),
	}
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "card1708.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+flowHarnessJS), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out map[string]flowOut
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	for name, o := range out {
		if len(o.Cards) != 1 || o.Cards[0].Kind != "capture-failed" || o.CardOnScreen != 1 {
			t.Fatalf("%s: cards = %+v, on screen %d", name, o.Cards, o.CardOnScreen)
		}
		t.Logf("%s: %q buttons %v", name, o.Cards[0].Lines, o.Buttons)
	}

	r := out["retrying"]
	if want := "An earlier cleanup is still running on the index. DBTrail checks again on its own, and capture starts when it finishes."; r.Cards[0].Lines[0] != want {
		t.Errorf("retrying: first line = %q\n            want   %q", r.Cards[0].Lines[0], want)
	}
	if len(r.Cards[0].Lines) != 2 || !strings.Contains(r.Cards[0].Lines[1], "connection id 812") || !strings.Contains(r.Cards[0].Lines[1], "KILL 812") {
		t.Errorf("retrying: the card does not name the connection: %q", r.Cards[0].Lines)
	}
	if hasLabel(r.Buttons, "Start") || !hasLabel(r.Buttons, "Details") {
		t.Errorf("retrying: buttons %v; this cause offers no Start", r.Buttons)
	}
	if !strings.Contains(r.Screen, "An earlier cleanup is still running") {
		t.Errorf("retrying: the sentence is not on screen: %q", r.Screen)
	}

	g := out["gave-up"]
	if want := "An earlier cleanup is still running on the index. Capture stays stopped. Start it from Servers once the cleanup finishes."; g.Cards[0].Lines[0] != want {
		t.Errorf("gave-up: first line = %q\n           want   %q", g.Cards[0].Lines[0], want)
	}
	if strings.Contains(g.Screen, "on its own") {
		t.Errorf("gave-up: the card promises a retry the daemon is not doing: %q", g.Screen)
	}
	if hasLabel(g.Buttons, "Start") {
		t.Errorf("gave-up: buttons %v; this cause offers no Start", g.Buttons)
	}

	for _, name := range []string{"text-only", "lock-timeout", "another-code"} {
		o := out[name]
		if !hasLabel(o.Buttons, "Start") {
			t.Errorf("%s: buttons %v; without the code this is any other failure, with Start", name, o.Buttons)
		}
		if len(o.Cards[0].Lines) != 1 || strings.Contains(o.Screen, "An earlier cleanup is still running on the index.") {
			t.Errorf("%s: the page decided on something other than the code: %q", name, o.Cards[0].Lines)
		}
	}
}
