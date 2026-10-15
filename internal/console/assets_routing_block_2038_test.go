package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestRoutingBlockSaysWhoAnswered renders the read-routing part of the
// Connect a SQL client panel through the real app.js, one shape per state.
//
// Since #2341 that part is routingSwitch: the word On or Off (the saved
// switch, routing_on in GET /api/flashback), the button that changes it, one
// sentence of what it means, a line when the picked server has no forwarding
// account, and who answered as two figures over a bar. The counts are the
// ROUTER's (GET /api/router), for the server picked in the sidebar, and are
// never drawn as zeros when the router said nothing. Why each side answered
// is the router's too, under "Why each side" (routerNumbers).
//
// routingBlock, which this test drew before, is gone, and with it the lines
// that named startup flags, the freshness limit, the account statements run
// as and the "Test connection" advice; the scenarios that guarded them are
// kept, each drawn in the shape that replaced it. The duration formatter is
// still pinned over the values that broke its first version ("10m0s" read
// as "1").
func TestRoutingBlockSaysWhoAnswered(t *testing.T) {
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
	// The router's document, from this package's own types.
	answered := func(readOnly bool, servers map[string]RouterServerNumbers) routerViewDTO {
		return routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-11T14:31:00Z",
			Status: &RouterStatusDoc{Version: 1, Listen: "127.0.0.1:3309", ReadOnly: readOnly, CostThreshold: 10000, ScanRows: 100000,
				RouterNumbers: RouterNumbers{CountingSince: "2026-10-04T12:00:00Z", Servers: servers}}}
	}
	one := func(n RouterServerNumbers) map[string]RouterServerNumbers {
		return map[string]RouterServerNumbers{"s1": n}
	}
	views, err := json.Marshal(map[string]routerViewDTO{
		"none":         answered(false, map[string]RouterServerNumbers{}),
		"noneRO":       answered(true, map[string]RouterServerNumbers{}),
		"zeros":        answered(false, one(RouterServerNumbers{Reasons: map[string]uint64{}})),
		"counts":       answered(false, one(RouterServerNumbers{Copy: 3, MySQL: 7, Reasons: map[string]uint64{"expensive_plan": 3, "cheap_plan": 5, "write": 2}})),
		"limits":       answered(false, one(RouterServerNumbers{Copy: 1, MySQL: 6, Reasons: map[string]uint64{"expensive_plan": 1, "bounded_limit": 4, "result_over_row_cap": 2}})),
		"busy":         answered(false, one(RouterServerNumbers{Copy: 40, MySQL: 5, Refused: 5, Reasons: map[string]uint64{"expensive_plan": 40, "copy_queue_full": 3, "copy_wait_timeout": 2}})),
		"other":        answered(false, map[string]RouterServerNumbers{"s2": {Copy: 30, MySQL: 70, Reasons: map[string]uint64{"expensive_plan": 30}}}),
		"both":         answered(false, map[string]RouterServerNumbers{"s1": {Copy: 3, MySQL: 7, Reasons: map[string]uint64{}}, "s2": {Copy: 30, MySQL: 70, Reasons: map[string]uint64{}}}),
		"readOnly":     answered(true, one(RouterServerNumbers{Copy: 3, MySQL: 7, Refused: 2, Reasons: map[string]uint64{"expensive_plan": 3, "cheap_plan": 7, "read_only": 2}})),
		"onlyRefusals": answered(true, one(RouterServerNumbers{Refused: 4, Reasons: map[string]uint64{"read_only": 4}})),
		"onlyMySQL":    answered(false, one(RouterServerNumbers{MySQL: 5, Reasons: map[string]uint64{"cheap_plan": 5}})),
		"onlyCopy":     answered(false, one(RouterServerNumbers{Copy: 8, Reasons: map[string]uint64{"expensive_plan": 8}})),
		"cannotRoute":  answered(false, one(RouterServerNumbers{Reasons: map[string]uint64{}, CannotRoute: true})),
		"refusedAcct":  answered(false, one(RouterServerNumbers{MySQL: 4, Reasons: map[string]uint64{"upstream_lost": 4}, AccountRefused: true})),
		"refusedAcct0": answered(false, one(RouterServerNumbers{Reasons: map[string]uint64{}, AccountRefused: true})),
		"down":         {Configured: true, URL: "http://router:9091/status", Error: "it could not be reached: connection refused"},
		"notSet":       {},
	})
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const flat = (n, out = []) => { if (n == null) return out; if (typeof n === "string") { out.push(n); return out; }
  if (n.nodeType === 3) { out.push(n.textContent); return out; } if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const tidy = (n) => flat(n).join(" ").replace(/\s+/g, " ").trim();
const each = (n, f) => { if (!n || typeof n === "string" || n.nodeType === 3) return; f(n); (n.children || []).forEach((c) => each(c, f)); };
const has = (x, c) => (" " + x.className + " ").includes(" " + c + " ");
const sw = vm.runInContext("routingSwitch", ctx);
const numbers = vm.runInContext("routerNumbers", ctx);
const fmt = vm.runInContext("fmtGoDuration", ctx);
const rt = ` + string(views) + `;
// draw: what the switch shows for a saved setting, a picked server and what
// the router answered. toggle is given as the panel gives it: a function
// for a session that may change settings, false for one that may not.
const draw = (on, cur, view, toggle) => {
  const o = { text: "", state: "", isOn: false, buttons: [], sentence: "", lanes: [], laneOff: [], account: "", caption: "", figs: "", bar: [], aria: "", bars: 0, wraps: 0, since: false, help: [] };
  const sides = []; let foot = "";
  const w = sw({ enabled: true, routing_on: on }, cur, view, toggle === undefined ? () => {} : toggle);
  o.text = tidy(w);
  each(w, (x) => {
    if (has(x, "cn-route-state")) { o.state = tidy(x); o.isOn = has(x, "is-on"); }
    if (x.tag === "button") o.buttons.push(tidy(x));
    // Where a statement goes, drawn: each lane, and whether it is drawn empty.
    if (has(x, "rd-lane")) { o.lanes.push(tidy(x)); o.laneOff.push(has(x, "rd-off")); }
    if (x.tag === "p" && has(x, "cn-sql-row")) { const t = tidy(x); if (t.startsWith("For ")) o.account = t; else o.sentence = t; }
    if (has(x, "cn-who-cap")) o.caption = tidy(x);
    // The two sides, drawn (#2330): one figure each, the refusals under
    // them, and a bar split between the two.
    if (has(x, "rs-wrap")) o.wraps++;
    if (has(x, "rs-side")) sides.push(tidy(x));
    if (has(x, "rs-k")) o.help.push(String((x.attrs || {}).title || x.title || ""));
    if (has(x, "rs-foot")) foot = tidy(x);
    if (has(x, "rs-bar")) { o.bars++; o.aria = x.attrs["aria-label"] || ""; o.bar = (x.children || []).map((c) => c.className + " " + String((c.attrs || {}).style || "")); }
  });
  const refused = foot.match(/\d+ refused, run by neither/);
  o.figs = sides.concat(refused ? [refused[0]] : []).join(" | ");
  o.since = /counted since \S.*when the router started/.test(foot);
  return o;
};
// why: the reasons the router gives, as drawn under "Why each side".
const why = (view, cur) => { let d = null;
  each(numbers(view, cur, () => {}), (x) => { if (x.tag === "details" && tidy((x.children || []).find((c) => c && c.tag === "summary")) === "Why each side") d = x; });
  const out = []; each(d, (x) => { if (x.tag === "li") out.push(tidy(x)); }); return out; };
// bare: a server with a source and no forwarding account. The port never
// forwards with the account it captures with (#2317), so it does not route.
const bare = { id: "s1", name: "shop", has_source: true, source_user: "repl" };
const srv = { ...bare, has_route: true, route_user: "report_ro" };
const billing = { id: "s2", name: "billing", has_source: true, has_route: true, route_user: "report_ro" };
// The button: pressed, it hands itself to toggle, once.
const pressed = [];
const tw = sw({ enabled: true, routing_on: false }, srv, rt.counts, (b) => pressed.push(b));
let tbtn = null; each(tw, (x) => { if (x.tag === "button") tbtn = x; });
tbtn.onclick();
console.log(JSON.stringify({
  draws: {
    off: draw(false, srv, rt.none),
    offCounts: draw(false, srv, rt.counts),
    offNoAccount: draw(false, bare, rt.none),
    on: draw(true, srv, rt.none),
    onReadOnly: draw(true, srv, rt.noneRO),
    onNoAnswer: draw(true, srv, rt.down),
    onNotSet: draw(true, srv, rt.notSet),
    onFailed: draw(true, srv, null),
    onNotAsked: draw(true, srv, undefined),
    lockedOff: draw(false, srv, rt.counts, false),
    lockedOn: draw(true, srv, rt.counts, false),
    noServer: draw(true, null, rt.counts),
    none: draw(true, srv, rt.none),
    zeros: draw(true, srv, rt.zeros),
    counts: draw(true, srv, rt.counts),
    other: draw(true, srv, rt.other),
    otherPicked: draw(true, billing, rt.both),
    firstPicked: draw(true, srv, rt.both),
    cannotRoute: draw(true, srv, rt.cannotRoute),
    readOnly: draw(true, srv, rt.readOnly),
    readOnlyOnlyRefusals: draw(true, srv, rt.onlyRefusals),
    onlyMySQL: draw(true, srv, rt.onlyMySQL),
    onlyCopy: draw(true, srv, rt.onlyCopy),
    oneEach: draw(true, srv, rt.limits),
    busy: draw(true, srv, rt.busy),
    forwarding: draw(true, srv, rt.none),
    forwardingReadOnly: draw(true, srv, rt.noneRO),
    forwardingNoName: draw(true, { ...bare, has_route: true }, rt.none),
    elsewhere: draw(true, { ...srv, route_host: "replica.internal", route_port: "3307" }, rt.none),
    noSource: draw(true, { id: "s1", name: "shop" }, rt.none),
    unreadable: draw(true, { ...bare, route_unreadable: true }, rt.none),
    isCapture: draw(true, { ...bare, route_is_capture: true, route_user: "repl" }, rt.none),
    noAccount: draw(true, bare, rt.none),
    noAccountReadOnly: draw(true, bare, rt.noneRO),
    noAccountCounts: draw(true, bare, rt.counts),
    noAccountDown: draw(true, bare, rt.down),
    noAccountNoName: draw(true, { id: "s1", has_source: true }, rt.none),
    noAccountPostgres: draw(true, { ...bare, flavor: "postgres" }, rt.none),
    refused: draw(true, srv, rt.refusedAcct),
    refusedNoCounts: draw(true, srv, rt.refusedAcct0),
  },
  why: { counts: why(rt.counts, srv), limits: why(rt.limits, srv), busy: why(rt.busy, srv), readOnly: why(rt.readOnly, srv), other: why(rt.other, srv), none: why(rt.none, srv) },
  lines: { refused: tidy(numbers(rt.refusedAcct, srv, () => {})), refusedNoCounts: tidy(numbers(rt.refusedAcct0, srv, () => {})), forwarding: tidy(numbers(rt.counts, srv, () => {})) },
  pressed: { times: pressed.length, same: pressed[0] === tbtn },
  missing: ["none", "own", "same", "unreadable", "nosource", "null"].map((k) => vm.runInContext("routeAccountMissing", ctx)(
    { none: bare, own: srv, same: { ...bare, route_is_capture: true }, unreadable: { ...bare, route_unreadable: true }, nosource: { id: "s1" }, null: null }[k])),
  hint: vm.runInContext("FORWARDING_HINT", ctx),
  durations: ["15m0s", "10m0s", "1h0m0s", "1h30m0s", "1m30s", "30s", "24h0m0s"].map(fmt),
}));
`
	path := filepath.Join(t.TempDir(), "routing.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type drawn struct {
		Text, State                string
		IsOn                       bool
		Buttons                    []string
		Sentence, Account, Caption string
		Lanes                      []string
		LaneOff                    []bool
		Figs                       string
		Bar                        []string
		Aria                       string
		Bars, Wraps                int
		Since                      bool
		Help                       []string
	}
	var got struct {
		Draws   map[string]drawn
		Why     map[string][]string
		Lines   map[string]string
		Pressed struct {
			Times int
			Same  bool
		}
		Missing   []string
		Hint      string
		Durations []string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	d := func(name string) drawn {
		t.Helper()
		v, ok := got.Draws[name]
		if !ok {
			t.Fatalf("%s: not drawn", name)
		}
		return v
	}
	for name, v := range got.Draws {
		t.Logf("%-20s %s | figs %q bar %q", name+":", v.Text, v.Figs, v.Bar)
		if !strings.HasPrefix(v.Text, "Engine Routing ") {
			t.Errorf("%s: does not open with Engine Routing: %s", name, v.Text)
		}
		// No shape of the switch names the capture account, shows a blank
		// made visible, or says what only the old block said.
		for _, bad := range []string{"—", "undefined", "NaN", "null", "Invalid Date", "[object", "repl", "statements run as", "--route-", "BINTRAIL_CONSOLE_ROUTE", "Experimental"} {
			if strings.Contains(v.Text, bad) {
				t.Errorf("%s: holds %q: %s", name, bad, v.Text)
			}
		}
	}
	must := func(name, text string, wants ...string) {
		t.Helper()
		for _, w := range wants {
			if !strings.Contains(text, w) {
				t.Errorf("%s: missing %q in: %s", name, w, text)
			}
		}
	}
	// Where a statement goes is DRAWN, not written: a lane to each side. The
	// one sentence left is what the drawing cannot say.
	const offSentence = "Turn it on and an application can use this port in place of its MySQL."
	offLanes := []string{"Nothing MySQL", "Every statement DuckDB, on the copy"}
	onLanes := []string{"Light statements MySQL", "Heavy SELECTs DuckDB, on the copy"}

	// The switch: one word, the button that changes it, the drawing.
	for _, name := range []string{"off", "offCounts", "offNoAccount", "lockedOff"} {
		v := d(name)
		if v.State != "Off" || v.IsOn || v.Sentence != offSentence {
			t.Errorf("%s: state %q (drawn as on: %v), sentence %q; want Off and %q", name, v.State, v.IsOn, v.Sentence, offSentence)
		}
		// Off, MySQL's lane is drawn empty and the copy's is not.
		if !reflect.DeepEqual(v.Lanes, offLanes) || !reflect.DeepEqual(v.LaneOff, []bool{true, false}) {
			t.Errorf("%s: lanes %q (drawn empty: %v), want %q with only MySQL's empty", name, v.Lanes, v.LaneOff, offLanes)
		}
	}
	for _, name := range []string{"on", "onReadOnly", "onNoAnswer", "onNotSet", "onFailed", "onNotAsked", "lockedOn", "counts", "noAccount"} {
		v := d(name)
		if v.State != "On" || !v.IsOn {
			t.Errorf("%s: state %q (drawn as on: %v); want On", name, v.State, v.IsOn)
		}
		if !reflect.DeepEqual(v.Lanes, onLanes) || !reflect.DeepEqual(v.LaneOff, []bool{false, false}) {
			t.Errorf("%s: lanes %q (drawn empty: %v), want %q, both lit", name, v.Lanes, v.LaneOff, onLanes)
		}
	}
	// The site's button where turning it on is the one thing to do; the
	// arrow is drawn by the stylesheet, so the name stays these words.
	if b := d("off").Buttons; len(b) != 1 || b[0] != "Turn routing on" {
		t.Errorf("off: buttons %q, want Turn routing on", b)
	}
	if b := d("on").Buttons; len(b) != 1 || b[0] != "Turn routing off" {
		t.Errorf("on: buttons %q, want Turn routing off", b)
	}
	// A session that may not change settings reads the state and gets no
	// button.
	for _, name := range []string{"lockedOff", "lockedOn"} {
		if b := d(name).Buttons; len(b) != 0 {
			t.Errorf("%s: buttons %q drawn for a session that cannot press them", name, b)
		}
	}
	if got.Pressed.Times != 1 || !got.Pressed.Same {
		t.Errorf("the button, pressed once: toggle ran %d time(s), with the button itself: %v", got.Pressed.Times, got.Pressed.Same)
	}
	// What a write does is the router's to say (its read_only), and is said
	// only when it answered.
	if v := d("on"); v.Sentence != "Writes run on MySQL too." {
		t.Errorf("on, a read-write router: %q", v.Sentence)
	}
	if v := d("onReadOnly"); v.Sentence != "A statement that would change the server is refused." {
		t.Errorf("on, a read-only router: %q", v.Sentence)
	}
	for _, name := range []string{"onNoAnswer", "onNotSet", "onFailed", "onNotAsked"} {
		if v := d(name); v.Sentence != "" {
			t.Errorf("%s: with no answer from a router nothing is said of writes; got %q", name, v.Sentence)
		}
	}
	for _, name := range []string{"readOnly", "readOnlyOnlyRefusals", "forwardingReadOnly", "noAccountReadOnly"} {
		if v := d(name); strings.Contains(v.Text, "Writes run on MySQL") {
			t.Errorf("%s: a read-only router is described as taking writes: %s", name, v.Text)
		}
	}
	for _, name := range []string{"counts", "none", "forwarding"} {
		if v := d(name); strings.Contains(v.Text, "is refused") || strings.Contains(v.Text, " refused") {
			t.Errorf("%s: a read-write router is described as refusing: %s", name, v.Text)
		}
	}

	// Who answered: two figures over a bar split between them, for the
	// server picked, only when the router reports counts for it.
	figures := func(name, wantFigs, wantAria string, wantBar ...string) {
		t.Helper()
		v := d(name)
		// One bar, except with nothing to split (no wantAria): then none.
		wantBars := 1
		if wantAria == "" {
			wantBars = 0
		}
		if v.Figs != wantFigs || v.Aria != wantAria || v.Bars != wantBars || v.Wraps != 1 || strings.Join(v.Bar, "|") != strings.Join(wantBar, "|") {
			t.Errorf("%s: figures %q, bar %q (%d drawn, in %d drawings) read as %q; want %q, %q read as %q", name, v.Figs, v.Bar, v.Bars, v.Wraps, v.Aria, wantFigs, wantBar, wantAria)
		}
		// Since when the router counts is said under the figures, and each
		// side says what it is when pointed at.
		if !v.Since || len(v.Help) != 2 || !strings.HasPrefix(v.Help[0], "Statements the router sent to your MySQL server") || !strings.HasPrefix(v.Help[1], "Heavy reads the router answered from DBTrail's copy") {
			t.Errorf("%s: since said: %v; what each side is: %q", name, v.Since, v.Help)
		}
		// One fact, once: the old sentence wording is not drawn beside it.
		if strings.Contains(v.Text, "by MySQL") || strings.Contains(v.Text, "by the copy") {
			t.Errorf("%s: the counts are also said the old way: %s", name, v.Text)
		}
	}
	const split73 = "The copy answered 30% of the statements"
	figures("counts", "Answered by your MySQL 7 statements | Answered by DBTrail's copy 3 statements Your MySQL did not have to run these Lag: unknown (last answer)", split73, "rs-bar-mysql flex:7", "rs-bar-copy flex:3")
	if c := d("counts").Caption; c != "Who answered for shop" {
		t.Errorf("counts: caption %q", c)
	}
	// The saved switch does not hide what the router counted.
	figures("offCounts", "Answered by your MySQL 7 statements | Answered by DBTrail's copy 3 statements Your MySQL did not have to run these Lag: unknown (last answer)", split73, "rs-bar-mysql flex:7", "rs-bar-copy flex:3")
	figures("lockedOn", "Answered by your MySQL 7 statements | Answered by DBTrail's copy 3 statements Your MySQL did not have to run these Lag: unknown (last answer)", split73, "rs-bar-mysql flex:7", "rs-bar-copy flex:3")
	figures("noAccountCounts", "Answered by your MySQL 7 statements | Answered by DBTrail's copy 3 statements Your MySQL did not have to run these Lag: unknown (last answer)", split73, "rs-bar-mysql flex:7", "rs-bar-copy flex:3")
	// Refused statements were run by neither side: said under the figures,
	// and left out of the bar and of its share.
	figures("readOnly", "Answered by your MySQL 7 statements | Answered by DBTrail's copy 3 statements Your MySQL did not have to run these Lag: unknown (last answer) | 2 refused, run by neither", split73, "rs-bar-mysql flex:7", "rs-bar-copy flex:3")
	figures("busy", "Answered by your MySQL 5 statements | Answered by DBTrail's copy 40 statements Your MySQL did not have to run these Lag: unknown (last answer) | 5 refused, run by neither", "The copy answered 89% of the statements", "rs-bar-mysql flex:5", "rs-bar-copy flex:40")
	// Refusals alone are counts too: nobody answered those, the bar has
	// nothing to split, and none is drawn (an empty bar would read as a
	// figure).
	figures("readOnlyOnlyRefusals", "Answered by your MySQL 0 statements | Answered by DBTrail's copy 0 statements | 4 refused, run by neither", "")
	figures("onlyMySQL", "Answered by your MySQL 5 statements | Answered by DBTrail's copy 0 statements", "The copy answered 0% of the statements", "rs-bar-mysql flex:5", "rs-bar-copy flex:0")
	figures("onlyCopy", "Answered by your MySQL 0 statements | Answered by DBTrail's copy 8 statements Your MySQL did not have to run these Lag: unknown (last answer)", "The copy answered 100% of the statements", "rs-bar-mysql flex:0", "rs-bar-copy flex:8")
	figures("refused", "Answered by your MySQL 4 statements | Answered by DBTrail's copy 0 statements", "The copy answered 0% of the statements", "rs-bar-mysql flex:4", "rs-bar-copy flex:0")
	// One is one statement, not one statements.
	figures("oneEach", "Answered by your MySQL 6 statements | Answered by DBTrail's copy 1 statement Your MySQL did not have to run it Lag: unknown (last answer)", "The copy answered 14% of the statements", "rs-bar-mysql flex:6", "rs-bar-copy flex:1")
	// Another server's counts never show under the picked one, and each
	// picked server gets its own.
	figures("firstPicked", "Answered by your MySQL 7 statements | Answered by DBTrail's copy 3 statements Your MySQL did not have to run these Lag: unknown (last answer)", split73, "rs-bar-mysql flex:7", "rs-bar-copy flex:3")
	figures("otherPicked", "Answered by your MySQL 70 statements | Answered by DBTrail's copy 30 statements Your MySQL did not have to run these Lag: unknown (last answer)", split73, "rs-bar-mysql flex:70", "rs-bar-copy flex:30")
	if c := d("otherPicked").Caption; c != "Who answered for billing" {
		t.Errorf("otherPicked: caption %q", c)
	}
	// No counts: no figures, no bar, and never zeros. A router that did not
	// answer, one that was not configured, a server it said nothing of, one
	// it counted nothing for, and no server picked.
	for _, name := range []string{"none", "zeros", "other", "noServer", "onNoAnswer", "onNotSet", "onFailed", "onNotAsked", "cannotRoute", "refusedNoCounts", "noAccount", "noAccountDown", "off"} {
		if v := d(name); v.Figs != "" || v.Bars != 0 || v.Wraps != 0 || v.Caption != "" || v.Since || strings.Contains(v.Text, "Who answered") || strings.Contains(v.Text, "Your MySQL") || strings.Contains(v.Text, "DBTrail's copy") ||
			strings.Contains(v.Text, "0 statements") || strings.Contains(v.Text, "by MySQL") || strings.Contains(v.Text, "by the copy") {
			t.Errorf("%s: counts are drawn where the router gave none: %s", name, v.Text)
		}
	}

	// #2317: a server with a source and no account of its own for the port
	// does not route, and the switch says so from the server itself, before
	// any client connected: none saved, the capture account saved as one, a
	// saved value that cannot be read. Each names what to do.
	account := func(name string, wants ...string) {
		t.Helper()
		v := d(name)
		if !strings.HasPrefix(v.Account, "For shop the copy still answers everything: ") || !strings.HasSuffix(v.Account, ".") || strings.HasSuffix(v.Account, "..") {
			t.Errorf("%s: the line about the server's account is %q", name, v.Account)
		}
		must(name, v.Account, wants...)
	}
	account("noAccount", "it has no forwarding account", "never forwards with the account DBTrail captures with", "create a user with SELECT", "set it as the forwarding user")
	account("noAccountReadOnly", "it has no forwarding account")
	account("isCapture", "saved forwarding account is the account DBTrail captures with", "set another forwarding user")
	account("unreadable", "saved forwarding account cannot be read", "type the forwarding user and password again")
	// What the router counted, or that it did not answer, does not take the
	// line away: the account is what to fix first.
	account("noAccountCounts", "it has no forwarding account")
	account("noAccountDown", "it has no forwarding account")
	// A PostgreSQL source is not asked for a MySQL account it cannot have.
	account("noAccountPostgres", "its source is PostgreSQL", "MySQL and MariaDB sources only")
	if v := d("noAccountPostgres"); strings.Contains(v.Account, "forwarding") {
		t.Errorf("a PostgreSQL source is told to set a forwarding account: %s", v.Account)
	}
	// A server with no name is called by its id.
	if v := d("noAccountNoName"); !strings.HasPrefix(v.Account, "For s1 the copy still answers everything: ") {
		t.Errorf("a server with no name: %q", v.Account)
	}
	// No line for a server that can route, one with no source, none picked,
	// or while routing is off (the copy answers everything then, for every
	// server).
	for _, name := range []string{"forwarding", "forwardingReadOnly", "forwardingNoName", "elsewhere", "noSource", "noServer", "counts", "offNoAccount", "refused"} {
		if v := d(name); v.Account != "" || strings.Contains(v.Text, "still answers everything") {
			t.Errorf("%s: a line about a missing account is drawn: %s", name, v.Text)
		}
	}
	if want := []string{"it has no forwarding account", "", "is the account DBTrail captures with", "cannot be read", "", ""}; len(got.Missing) != len(want) {
		t.Errorf("routeAccountMissing: %q", got.Missing)
	} else {
		for i, w := range want {
			if (w == "") != (got.Missing[i] == "") || !strings.Contains(got.Missing[i], w) {
				t.Errorf("routeAccountMissing case %d = %q, want %q in it (empty iff empty)", i, got.Missing[i], w)
			}
		}
	}

	// Why each side, most frequent first, in the router's words. The two
	// reasons a LIMIT and the copy's row cap keep a statement on MySQL
	// (#2061, #2115), and what a busy copy means on a router (#2112, #2318):
	// refused with error 1040, where the port inside the service used to
	// hand the statement to MySQL.
	reasons := func(name string, want ...string) {
		t.Helper()
		if strings.Join(got.Why[name], "\n") != strings.Join(want, "\n") {
			t.Errorf("%s: Why each side =\n  %s\nwant\n  %s", name, strings.Join(got.Why[name], "\n  "), strings.Join(want, "\n  "))
		}
	}
	reasons("counts", "5 × a cheap plan (a lookup or a small read)", "3 × an expensive plan: the copy answered", "2 × a write (INSERT, UPDATE, DELETE, DDL)")
	reasons("limits", "4 × a small LIMIT MySQL answers without reading past it",
		"2 × an expensive plan whose result is over the copy's row cap: MySQL answered, and the copy was not tried",
		"1 × an expensive plan: the copy answered")
	reasons("busy", "40 × an expensive plan: the copy answered",
		"3 × every worker was busy and thousands of statements were already waiting, more than the router lets wait: refused with error 1040 at once",
		"2 × every worker was busy: the statement waited as long as the router was told to let one wait (--max-wait), then was refused with error 1040")
	reasons("readOnly", "7 × a cheap plan (a lookup or a small read)", "3 × an expensive plan: the copy answered", "2 × refused: not a read, and the router is read-only")
	// Another server's reasons never show under the picked one.
	reasons("other")
	reasons("none")

	// The source turned the router's login away: said with the router's
	// numbers, with or without counts, and not for a server it let in.
	must("refused", got.Lines["refused"], "MySQL refused the router's login for this server.", "error 2006", "The router's log names the account")
	must("refused 0", got.Lines["refusedNoCounts"], "MySQL refused the router's login for this server.", "no statements yet")
	if strings.Contains(got.Lines["forwarding"], "refused the router's login") {
		t.Errorf("a server with no refusal shows one: %s", got.Lines["forwarding"])
	}

	// The server form's hint: which account the port forwards with (#2079),
	// never the one DBTrail captures with (#2317), and where routing is
	// turned on now that it is a switch and not a startup flag (#2341).
	must("form hint", got.Hint, "Forwarding user", "SELECT only", "must be another user than the source user", "never forwards with the account DBTrail captures with", "the copy answers alone",
		"They only matter while Engine Routing is on (Settings, MySQL clients, Connect a SQL client)")
	for _, bad := range []string{"forwards with the source user", "started with read routing", "started with Engine Routing", "router service", "whatever that account can.", "never writ", "exactly the tables", "—"} {
		if strings.Contains(got.Hint, bad) {
			t.Errorf("the form hint holds %q: %s", bad, got.Hint)
		}
	}
	if want := []string{"15m", "10m", "1h", "1h30m", "1m30s", "30s", "24h"}; strings.Join(got.Durations, ",") != strings.Join(want, ",") {
		t.Errorf("durations = %v, want %v", got.Durations, want)
	}
}
