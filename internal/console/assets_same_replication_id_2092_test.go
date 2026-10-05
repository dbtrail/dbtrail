package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #2092 on screen. Every input is marshalled from the structs the API sends
// (MonitorStatus, serverDTO, the first-run report), so the keys the page
// reads are the real ones. sameIDRaw is the error as a MariaDB source sends
// it to the reader it drops, with the suffix the supervisor adds.
const sameIDRaw = "ERROR 4052 (HY000): A slave with the same server_id is already connected; the first event '.' at 0, the last event read from 'binlog.000955' at 51806, the last byte read from 'binlog.000955' at 51837."

var sameIDSentences = []string{
	"Something else is reading this database's changes with the same replication id, the number a reader gives the database to identify itself. Most likely it is another DBTrail installation pointed at the same database.",
	"The database keeps one reader per id, so each one disconnects the other when it reconnects. Capture keeps being interrupted on both sides while both are connected. Nothing is lost: each one resumes where it stopped.",
	"To fix it, stop capture for this server in one of the two installations.",
}

func TestOverviewCardForAnotherReaderWithTheSameID(t *testing.T) {
	node := nodeOrSkip(t)
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	type c = map[string]any
	input := func(st MonitorStatus) c {
		var mon c
		if err := json.Unmarshal([]byte(mustJSON(t, st)), &mon); err != nil {
			t.Fatal(err)
		}
		return c{"input": c{
			"coverage":  c{"freshness": "current", "continuity": "ok", "delta_to": "2026-09-23 14:02:10"},
			"baselines": c{},
			"server":    c{"id": "a", "kind": "registry", "has_source": true, "monitor_state": st.State},
			"monitor":   mon,
			"schema":    c{"state": "idle"}, "uncaptured": c{}}}
	}
	gaveUp := sameIDRaw + " (gave up after 6h0m0s of crash-looping; fix the issue, then press Start to retry)"
	cases := map[string]c{
		"retrying": input(MonitorStatus{State: "failed", Retrying: true, ErrorCode: MonitorErrSameReplicationID, LastError: sameIDRaw + " (retrying)"}),
		"gave-up":  input(MonitorStatus{State: "failed", ErrorCode: MonitorErrSameReplicationID, LastError: gaveUp}),
		// The same text with no code: a daemon older than this console. The
		// page must not read the cause out of the words.
		"text-only":    input(MonitorStatus{State: "failed", Retrying: true, LastError: sameIDRaw + " (retrying)"}),
		"another-code": input(MonitorStatus{State: "failed", Retrying: true, ErrorCode: "something_newer", LastError: "boom"}),
	}
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	// The harness also reads back each fold in the card: its summary and what
	// it holds.
	script := renderHarnessJS + strings.Replace(flowHarnessJS, `cardOnScreen: find(sec, "flow-card").length,`,
		`cardOnScreen: find(sec, "flow-card").length,
    folds: find(sec, "flow-card").flatMap((k) => k.children.filter((n) => n.tag === "details")).map((d) => ({ summary: text(d.children[0]), body: d.children.slice(1).map(text).join(" ") })),
    keys: m.cards.map((k) => k.key),`, 1)
	if !strings.Contains(script, "folds:") {
		t.Fatal("the flow harness changed shape; the folds are not read back")
	}
	path := filepath.Join(t.TempDir(), "card2092.js")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out map[string]struct {
		flowOut
		Folds []struct{ Summary, Body string }
		Keys  []string
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	for name, o := range out {
		if len(o.Cards) != 1 || o.Cards[0].Kind != "capture-failed" || o.CardOnScreen != 1 {
			t.Fatalf("%s: cards = %+v, on screen %d", name, o.Cards, o.CardOnScreen)
		}
		t.Logf("%s: %q folds %+v buttons %v", name, o.Cards[0].Lines, o.Folds, o.Buttons)
	}

	r := out["retrying"]
	if got := r.Cards[0].Lines; len(got) != len(sameIDSentences) {
		t.Fatalf("retrying: lines = %q", got)
	}
	for i, want := range sameIDSentences {
		if got := r.Cards[0].Lines[i]; got != want {
			t.Errorf("retrying: line %d = %q\n             want    %q", i, got, want)
		}
		if !strings.Contains(r.Screen, want) {
			t.Errorf("retrying: not on screen: %q", want)
		}
	}
	// The error as reported is one click away, whole, and not among the lines.
	if len(r.Folds) != 1 || r.Folds[0].Summary != "Technical details" || r.Folds[0].Body != sameIDRaw+" (retrying)" {
		t.Errorf("retrying: folds = %+v, want the raw error under Technical details", r.Folds)
	}
	for _, l := range r.Cards[0].Lines {
		if strings.Contains(l, "4052") || strings.Contains(l, "server_id") || strings.Contains(l, "slave") {
			t.Errorf("retrying: a line carries the raw error: %q", l)
		}
	}
	// Starting here only drops the other reader sooner.
	if hasLabel(r.Buttons, "Start") || !hasLabel(r.Buttons, "Details") {
		t.Errorf("retrying: buttons %v; a retrying stream offers no Start", r.Buttons)
	}
	// The text changes on every drop (a new position); the card closed once
	// must not come back for it.
	if len(r.Keys) != 1 || strings.Contains(r.Keys[0], "51806") {
		t.Errorf("retrying: the card's key follows the error text: %q", r.Keys)
	}

	g := out["gave-up"]
	if g.Cards[0].Lines[0] != sameIDSentences[0] {
		t.Errorf("gave-up: first line = %q", g.Cards[0].Lines[0])
	}
	if want := "The database keeps one reader per id, so each one disconnected the other when it reconnected. After hours of that, this installation stopped trying. Capture resumes where it stopped once it starts again."; g.Cards[0].Lines[1] != want {
		t.Errorf("gave-up: second line = %q\n            want          %q", g.Cards[0].Lines[1], want)
	}
	if want := "To fix it, stop capture for this server in one of the two installations. If this is the one you keep, start it again."; g.Cards[0].Lines[2] != want {
		t.Errorf("gave-up: third line = %q\n            want         %q", g.Cards[0].Lines[2], want)
	}
	if strings.Contains(g.Screen, "keeps being interrupted") || !hasLabel(g.Buttons, "Start") {
		t.Errorf("gave-up: a stream nobody retries reads as retrying, or has no Start: %v %q", g.Buttons, g.Screen)
	}
	if len(g.Folds) != 1 || g.Folds[0].Body != gaveUp {
		t.Errorf("gave-up: folds = %+v", g.Folds)
	}

	for _, name := range []string{"text-only", "another-code"} {
		o := out[name]
		if !hasLabel(o.Buttons, "Start") || len(o.Cards[0].Lines) != 1 || len(o.Folds) != 0 ||
			strings.Contains(o.Screen, "Something else is reading") {
			t.Errorf("%s: the page decided on something other than the code: %q folds %+v buttons %v", name, o.Cards[0].Lines, o.Folds, o.Buttons)
		}
	}
}

// TestFirstRunAndChipForAnotherReaderWithTheSameID: the first-run list says
// the same sentences as the Overview card and keeps the error in a fold, and
// the server row's chip names the cause. Without the code both stay generic.
func TestFirstRunAndChipForAnotherReaderWithTheSameID(t *testing.T) {
	node := nodeOrSkip(t)
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	yes := true
	report := func(st MonitorStatus) string {
		return mustJSON(t, firstRunSteps(firstRunInput{Monitor: st, IndexExists: &yes}))
	}
	coded := MonitorStatus{State: "failed", Retrying: true, ErrorCode: MonitorErrSameReplicationID, LastError: sameIDRaw + " (retrying)"}
	plain := MonitorStatus{State: "failed", Retrying: true, LastError: sameIDRaw + " (retrying)"}
	if rep := report(coded); !strings.Contains(rep, `"error_code":"same_replication_id"`) || !strings.Contains(rep, `"retrying":true`) {
		t.Fatalf("the first-run report does not carry the code: %s", rep)
	}
	row := func(st MonitorStatus) string {
		return mustJSON(t, serverDTO{MonitorState: st.State, MonitorErrorCode: st.ErrorCode})
	}

	script := renderHarnessJS + `
document.importNode = (n) => n;
const walk = (n, f) => { if (!n) return; f(n); for (const c of n.children || []) walk(c, f); };
const txt = (n) => (n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(txt).join(""));
const card = vm.runInContext("firstRunCard", ctx);
const draw = (rep) => { const out = { details: [], fixes: [], folds: [] };
  walk(card(rep), (n) => { const cls = " " + (n.className || "") + " ";
    if (n.tag === "details") { out.folds.push({ summary: txt(n.children[0]), body: n.children.slice(1).map(txt).join(" ") }); return; }
    if (cls.includes(" fr-detail ")) out.details.push(txt(n));
    if (cls.includes(" fr-fix ")) out.fixes.push(txt(n)); });
  // What sits inside a fold is not on the open page.
  out.details = out.details.filter((d) => !out.folds.some((f) => f.body === d));
  return out; };
const chip = vm.runInContext("monitorChip", ctx);
const tip = (s) => { const c = chip(s); return { text: c._text, title: (c.attrs || {}).title || "" }; };
console.log(JSON.stringify({ coded: draw(` + report(coded) + `), plain: draw(` + report(plain) + `),
  chipCoded: tip(` + row(coded) + `), chipPlain: tip(` + row(plain) + `) }));
`
	path := filepath.Join(t.TempDir(), "firstrun2092.js")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type drawn struct {
		Details, Fixes []string
		Folds          []struct{ Summary, Body string }
	}
	type chip struct{ Text, Title string }
	var got struct {
		Coded, Plain         drawn
		ChipCoded, ChipPlain chip
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	t.Logf("first run: %+v", got.Coded)
	t.Logf("chip: %+v", got.ChipCoded)

	if len(got.Coded.Details) != 2 || got.Coded.Details[0] != sameIDSentences[0] || got.Coded.Details[1] != sameIDSentences[1] {
		t.Errorf("first run: explanation = %q", got.Coded.Details)
	}
	if len(got.Coded.Fixes) != 1 || got.Coded.Fixes[0] != sameIDSentences[2]+" Open Servers ›" {
		t.Errorf("first run: fix = %q, want the card's last sentence and the way to Servers", got.Coded.Fixes)
	}
	if len(got.Coded.Folds) != 1 || got.Coded.Folds[0].Summary != "Technical details" || got.Coded.Folds[0].Body != sameIDRaw+" (retrying)" {
		t.Errorf("first run: folds = %+v, want the raw error under Technical details", got.Coded.Folds)
	}
	// No code: the error as reported and the server's own fix, as before.
	if len(got.Plain.Details) != 1 || got.Plain.Details[0] != sameIDRaw+" (retrying)" || len(got.Plain.Folds) != 0 ||
		len(got.Plain.Fixes) != 1 || !strings.HasPrefix(got.Plain.Fixes[0], "Capture retries on its own.") {
		t.Errorf("first run without the code: %+v", got.Plain)
	}

	if want := "something else is reading this database's changes with the same replication id, so the two keep disconnecting each other; stop capture for this server in one of the two installations"; got.ChipCoded.Text != "FAILED" || got.ChipCoded.Title != want {
		t.Errorf("chip = %+v\n  want title %q", got.ChipCoded, want)
	}
	if got.ChipPlain.Text != "FAILED" || !strings.HasPrefix(got.ChipPlain.Title, "connection is failing and retrying automatically") {
		t.Errorf("chip without the code: %+v", got.ChipPlain)
	}
}

// The words name no setting the web interface lacks, and hold no em dash.
func TestSameReplicationIDWordsStayPlain(t *testing.T) {
	js := readAsset(t, "app.js")
	body := jsFunctionSpan(t, js, "sameReplicationIdLines")
	i := strings.Index(js, "const MON_ERROR_TITLES = {")
	if i < 0 {
		t.Fatal("MON_ERROR_TITLES is gone")
	}
	body += js[i : i+strings.Index(js[i:], "};")]
	for _, bad := range []string{"—", "–", "server_id", "server id", "server-id", "slave"} {
		if strings.Contains(body, bad) {
			t.Errorf("the words for a shared replication id hold %q", bad)
		}
	}
}
