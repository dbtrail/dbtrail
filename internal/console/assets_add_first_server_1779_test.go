package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestOverviewOffersTheFirstServer (#1779): while no server is listed on a
// console that can monitor one, the Overview carries + Add server, and the
// button opens the servers dialog with the add form already open. It drives
// the page's own renderOverview and addServerCard in node over the four
// combinations of "servers listed" and "can monitor".
func TestOverviewOffersTheFirstServer(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	js := readAsset(t, "app.js")
	// Joined with a newline: an extracted body can end inside the next
	// function's doc comment.
	src := functionBody(t, js, "function renderOverview(") + "\n" + functionBody(t, js, "function addServerCard(")
	script := `
const calls = [];
const el = (tag, attrs, ...kids) => ({ tag, attrs: attrs || {}, kids: [], append(...k) { this.kids.push(...k); } });
const never = new Promise(() => {});
const api = () => never;
const openServersModal = () => calls.push("open");
const showServerForm = (p) => calls.push("form:" + p);
const watchFirstRun = () => calls.push("watch");
const loadOvUncaptured = () => {};
const watchOverview = () => calls.push("live");
const overviewOnScreen = () => true;
let serverGen = 0, viewGen = 0, serversEmpty, capsCache, ovHead;
let slot;
const ovFrame = () => ({ firstRunSlot: slot = { kids: [], append(...k) { this.kids.push(...k); } } });
const find = (n, id) => n && (n.attrs && n.attrs.id === id ? n : (n.kids || []).map((k) => find(k, id)).find(Boolean));
` + src + `
const out = {};
for (const empty of [true, false]) for (const monitor of [true, false]) {
  serversEmpty = empty; capsCache = { monitor };
  calls.length = 0;
  renderOverview();
  const btn = find({ kids: slot.kids }, "ov-add-server");
  const r = { card: !!btn, watch: calls.includes("watch"), live: calls.includes("live") };
  if (btn) { calls.length = 0; btn.attrs.onclick(); r.click = calls.slice(); r.label = btn.attrs.text; }
  out[(empty ? "empty" : "listed") + "/" + (monitor ? "monitor" : "readonly")] = r;
}
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "addfirst.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]struct {
		Card  bool     `json:"card"`
		Watch bool     `json:"watch"`
		Live  bool     `json:"live"`
		Click []string `json:"click"`
		Label string   `json:"label"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("parse %s: %v", raw, err)
	}

	first := got["empty/monitor"]
	if !first.Card {
		t.Fatal("no + Add server on the Overview of a console with no server that can monitor one")
	}
	if first.Label != "+ Add server" {
		t.Errorf("button label = %q", first.Label)
	}
	if strings.Join(first.Click, ",") != "open,form:null" {
		t.Errorf("the button did %v; want the servers dialog opened, then its add form", first.Click)
	}
	if first.Watch {
		t.Error("the Getting started poll also started, for a server that does not exist")
	}
	// The page keeps itself current (#1801) in all four: with no server listed
	// it shows the command-line index, which can gain changes too.
	for k, r := range got {
		if !r.Live {
			t.Errorf("%s: the Overview does not keep itself current", k)
		}
	}
	for _, k := range []string{"empty/readonly", "listed/monitor", "listed/readonly"} {
		if got[k].Card {
			t.Errorf("%s: + Add server shown", k)
		}
		if !got[k].Watch {
			t.Errorf("%s: the Getting started poll did not start", k)
		}
	}
}
