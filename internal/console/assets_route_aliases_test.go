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

// routeAliasHarnessJS drives the real router in app.js with a location and a
// history that behave: pushState and replaceState are logged and move the
// address. The shared harness's history accepts any call and changes nothing,
// so a test there would pass whether or not the router rewrote the bar. Every
// page painter is replaced by a recorder, except the ones a scenario names in
// `real`, so a painter's own gate (Retention refusing without the watch
// daemon) runs for real where the scenario is about that gate.
const routeAliasHarnessJS = `
const loc = { pathname: "/", search: "", hash: "", origin: "http://x" };
ctx.location = loc;
const log = [];
const setURL = (u) => { const x = new URL(u, "http://x" + loc.pathname); loc.pathname = x.pathname; loc.search = x.search; loc.hash = x.hash; };
ctx.__setURL = setURL;
ctx.history = { pushState: (s, t, u) => { log.push("push " + u); setURL(u); }, replaceState: (s, t, u) => { log.push("replace " + u); setURL(u); } };
let calls = [], nav = [];
ctx.setActiveNav = (r) => nav.push(r);
const painters = ["renderOverview", "renderEvents", "renderSchemaChanges", "renderRecover", "renderStatus", "renderRetention",
  "renderDaemon", "renderSnapshots", "renderConnect", "renderAccessProfiles"];
const real = {};
for (const n of painters) real[n] = ctx[n];
const run = (sc) => {
  for (const n of painters) ctx[n] = (sc.real || []).includes(n) ? (...a) => { calls.push(n); return real[n](...a); } : () => { calls.push(n); };
  vm.runInContext("capsCache = " + JSON.stringify(sc.caps) + "; capsKnown = " + !!sc.known + ";", ctx);
  vm.runInContext("if (typeof routeArrivedFrom !== 'undefined') routeArrivedFrom = '';", ctx);
  // Each scenario is a fresh visit: the address the last dispatch painted
  // must not leak across them, or the second scenario decides whether to arm
  // the jump by comparing against the first one's address.
  vm.runInContext("if (typeof lastRouteAddress !== 'undefined') lastRouteAddress = null;", ctx);
  setURL(sc.start);
  log.length = 0; calls = []; nav = [];
  let err = "";
  try { for (const s of sc.steps) vm.runInContext(s, ctx); } catch (e) { err = String(e); }
  return { url: loc.pathname + loc.search + loc.hash, log: [...log], calls, nav, err,
    from: vm.runInContext("typeof routeArrivedFrom === 'undefined' ? null : routeArrivedFrom", ctx),
    pending: vm.runInContext("typeof scrollPending === 'undefined' ? null : scrollPending", ctx) };
};
const scenarios = JSON.parse(process.argv[3]);
const out = {};
for (const [name, sc] of Object.entries(scenarios)) out[name] = run(sc);
// Every alias lands on a live route, never on another alias, for either
// answer the capability check can give.
const targets = [];
for (const caps of [{ monitor: true }, {}]) {
  vm.runInContext("capsCache = " + JSON.stringify(caps) + "; capsKnown = true;", ctx);
  targets.push(vm.runInContext("typeof ROUTE_ALIASES === 'undefined' ? [] : Array.from(ROUTE_ALIASES.keys(), (k) => [k, aliasTarget(k)])", ctx));
}
out.__targets = { calls: [], nav: [], log: [], url: "", err: "", from: JSON.stringify({ targets, routes: vm.runInContext("ROUTES", ctx) }) };
console.log(JSON.stringify(out));
`

type routeScenario struct {
	Start string   `json:"start"`
	Steps []string `json:"steps"`
	Caps  any      `json:"caps"`
	Known bool     `json:"known"`
	Real  []string `json:"real,omitempty"`
}

type routeResult struct {
	URL     string
	Log     []string
	Calls   []string
	Nav     []string
	Err     string
	From    *string
	Pending *bool
}

func runRouteScenarios(t *testing.T, scenarios map[string]routeScenario) map[string]routeResult {
	t.Helper()
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
	path := filepath.Join(t.TempDir(), "routes.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+routeAliasHarnessJS), 0o644); err != nil {
		t.Fatal(err)
	}
	arg, err := json.Marshal(scenarios)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]routeResult
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return got
}

var (
	watchCaps = map[string]any{"monitor": true}
	serveCaps = map[string]any{}
	boot      = []string{"renderRoute()"}
)

// TestOldAddressesLandOnTheirPage: an address the console no longer has a page
// for (a bookmark, a link in an old email or doc, a Back entry) lands on the
// page that replaced it — at the section that was its page, where the new
// page has one — with the bar rewritten, and whatever else the address
// carried (query, anchor) travels along. Before, the translation for /storage
// and /sql sat in navigate(), which nothing called with those names, so a
// bookmark of either painted Overview under the old address.
func TestOldAddressesLandOnTheirPage(t *testing.T) {
	type want struct {
		url   string
		log   []string
		calls []string
		nav   string // the sidebar item lit on the first paint; "" = any
		from  string // routeArrivedFrom after the steps
	}
	for _, tc := range []struct {
		name string
		sc   routeScenario
		want want
	}{
		{"storage, watch", routeScenario{Start: "/storage", Steps: boot, Caps: watchCaps, Known: true},
			want{"/retention", []string{"replace /retention"}, []string{"renderRetention"}, "retention", "storage"}},
		// Retention refuses without the watch daemon and rewrites to Overview
		// itself: the translation must not skip that gate.
		{"storage, serve", routeScenario{Start: "/storage", Steps: boot, Caps: serveCaps, Known: true, Real: []string{"renderRetention"}},
			want{"/overview", []string{"replace /retention", "replace /overview"}, []string{"renderRetention", "renderOverview"}, "retention", "storage"}},
		// /sql goes to Connect AI (#1573), which every console has, so the
		// answer does not wait for the capability check.
		{"sql, watch", routeScenario{Start: "/sql", Steps: boot, Caps: watchCaps, Known: true},
			want{"/connect", []string{"replace /connect"}, []string{"renderConnect"}, "connect", "sql"}},
		{"sql, serve", routeScenario{Start: "/sql", Steps: boot, Caps: serveCaps, Known: true},
			want{"/connect", []string{"replace /connect"}, []string{"renderConnect"}, "connect", "sql"}},
		{"sql, capabilities unknown", routeScenario{Start: "/sql", Steps: boot, Caps: serveCaps, Known: false},
			want{"/connect", []string{"replace /connect"}, []string{"renderConnect"}, "connect", "sql"}},
		// Where /storage goes depends on the capability answer. With no answer
		// (the check failed), it does not guess: a guess written into the bar
		// would stick to that history entry after the check recovers.
		{"storage, capabilities unknown", routeScenario{Start: "/storage", Steps: boot, Caps: serveCaps, Known: false, Real: []string{"renderRetention"}},
			want{"/storage", nil, []string{"renderOverview"}, "", ""}},
		// /daemon goes to Status (#1867), which every console has: the page it
		// named was dissolved and its telemetry card lives there now.
		{"daemon, watch", routeScenario{Start: "/daemon", Steps: boot, Caps: watchCaps, Known: true},
			want{"/status", []string{"replace /status"}, []string{"renderStatus"}, "status", "daemon"}},
		{"daemon, serve", routeScenario{Start: "/daemon", Steps: boot, Caps: serveCaps, Known: true},
			want{"/status", []string{"replace /status"}, []string{"renderStatus"}, "status", "daemon"}},
		{"timetravel", routeScenario{Start: "/timetravel", Steps: boot, Caps: watchCaps, Known: true},
			want{"/recover", []string{"replace /recover"}, []string{"renderRecover"}, "recover", "timetravel"}},
		{"query and anchor travel", routeScenario{Start: "/timetravel?a=1&b=2#here", Steps: boot, Caps: watchCaps, Known: true},
			want{"/recover?a=1&b=2#here", []string{"replace /recover?a=1&b=2#here"}, []string{"renderRecover"}, "recover", "timetravel"}},
		{"trailing slash", routeScenario{Start: "/storage/", Steps: boot, Caps: watchCaps, Known: true},
			want{"/retention", []string{"replace /retention"}, []string{"renderRetention"}, "retention", "storage"}},
		{"extra segment", routeScenario{Start: "/storage/x", Steps: boot, Caps: watchCaps, Known: true},
			want{"/retention", []string{"replace /retention"}, []string{"renderRetention"}, "retention", "storage"}},
		// Routes are lowercase: an uppercase old name is an unknown address,
		// as before.
		{"uppercase is unknown", routeScenario{Start: "/STORAGE", Steps: boot, Caps: watchCaps, Known: true},
			want{"/STORAGE", nil, []string{"renderOverview"}, "", ""}},
		// A name a plain object's prototype carries is not an old address.
		{"constructor", routeScenario{Start: "/constructor", Steps: boot, Caps: watchCaps, Known: true},
			want{"/constructor", nil, []string{"renderOverview"}, "", ""}},
		{"__proto__", routeScenario{Start: "/__proto__", Steps: boot, Caps: watchCaps, Known: true},
			want{"/__proto__", nil, []string{"renderOverview"}, "", ""}},
		{"toString", routeScenario{Start: "/toString", Steps: boot, Caps: watchCaps, Known: true},
			want{"/toString", nil, []string{"renderOverview"}, "", ""}},
		// Live addresses are left alone.
		{"root", routeScenario{Start: "/", Steps: boot, Caps: watchCaps, Known: true},
			want{"/", nil, []string{"renderOverview"}, "overview", ""}},
		{"events", routeScenario{Start: "/events?q=1", Steps: boot, Caps: watchCaps, Known: true},
			want{"/events?q=1", nil, []string{"renderEvents"}, "events", ""}},
		{"snapshots", routeScenario{Start: "/snapshots", Steps: boot, Caps: watchCaps, Known: true},
			want{"/snapshots", nil, []string{"renderSnapshots"}, "snapshots", ""}},
		// The three pages that merged into Snapshots (#1573). Each old
		// address lands on the part of the page that was its page, and the
		// answer never waits for the capability check: unlike two of the
		// pages it replaces, Snapshots opens on a standalone serve too.
		{"baselines", routeScenario{Start: "/baselines", Steps: boot, Caps: watchCaps, Known: true},
			want{"/snapshots", []string{"replace /snapshots"}, []string{"renderSnapshots"}, "snapshots", "baselines"}},
		{"baselines, serve", routeScenario{Start: "/baselines", Steps: boot, Caps: serveCaps, Known: true},
			want{"/snapshots", []string{"replace /snapshots"}, []string{"renderSnapshots"}, "snapshots", "baselines"}},
		{"baselines, capabilities unknown", routeScenario{Start: "/baselines", Steps: boot, Caps: serveCaps, Known: false},
			want{"/snapshots", []string{"replace /snapshots"}, []string{"renderSnapshots"}, "snapshots", "baselines"}},
		{"verification lands on its section", routeScenario{Start: "/verification", Steps: boot, Caps: watchCaps, Known: true},
			want{"/snapshots#checks", []string{"replace /snapshots#checks"}, []string{"renderSnapshots"}, "snapshots", "verification"}},
		{"backup-settings lands on its section", routeScenario{Start: "/backup-settings", Steps: boot, Caps: serveCaps, Known: true},
			want{"/snapshots#setup", []string{"replace /snapshots#setup"}, []string{"renderSnapshots"}, "snapshots", "backup-settings"}},
		// An address that already names a part of the old page keeps it: a
		// link INTO a section must not be overwritten by the section the
		// whole page became.
		{"an anchor on the old address survives", routeScenario{Start: "/verification#past", Steps: boot, Caps: watchCaps, Known: true},
			want{"/snapshots#past", []string{"replace /snapshots#past"}, []string{"renderSnapshots"}, "snapshots", "verification"}},
		{"query and section travel together", routeScenario{Start: "/backup-settings?a=1", Steps: boot, Caps: watchCaps, Known: true},
			want{"/snapshots?a=1#setup", []string{"replace /snapshots?a=1#setup"}, []string{"renderSnapshots"}, "snapshots", "backup-settings"}},
		// A caller naming one of them pushes the new address, section and
		// all, and records no arrival: nobody followed an old link.
		{"navigate verification from events", routeScenario{Start: "/events", Steps: []string{"navigate('verification')"}, Caps: watchCaps, Known: true},
			want{"/snapshots#checks", []string{"push /snapshots#checks"}, []string{"renderSnapshots"}, "snapshots", ""}},
		// Back and Forward onto an old address.
		{"back onto storage", routeScenario{Start: "/storage", Steps: []string{"onPopState()"}, Caps: watchCaps, Known: true},
			want{"/retention", []string{"replace /retention"}, []string{"renderRetention"}, "retention", "storage"}},
		// A caller naming an old route (the palette's Time-travel does)
		// pushes the new address, and the entry you were on stays in the
		// history: a replaceState here would overwrite it.
		{"navigate storage from events", routeScenario{Start: "/events", Steps: []string{"navigate('storage')"}, Caps: watchCaps, Known: true},
			want{"/retention", []string{"push /retention"}, []string{"renderRetention"}, "retention", ""}},
		{"navigate sql from events, serve", routeScenario{Start: "/events", Steps: []string{"navigate('sql')"}, Caps: serveCaps, Known: true},
			want{"/connect", []string{"push /connect"}, []string{"renderConnect"}, "connect", ""}},
		{"navigate timetravel", routeScenario{Start: "/events", Steps: []string{"navigate('timetravel')"}, Caps: watchCaps, Known: true},
			want{"/recover", []string{"push /recover"}, []string{"renderRecover"}, "recover", ""}},
		// Where the visit came from outlives a repaint of the same visit (a
		// server switch, a save) and ends at the next navigation, forward or
		// back.
		{"arrival outlives a repaint", routeScenario{Start: "/storage", Steps: []string{"renderRoute()", "renderRoute()"}, Caps: watchCaps, Known: true},
			want{"/retention", []string{"replace /retention"}, []string{"renderRetention", "renderRetention"}, "retention", "storage"}},
		{"arrival ends at a click", routeScenario{Start: "/storage", Steps: []string{"renderRoute()", "navigate('events')"}, Caps: watchCaps, Known: true},
			want{"/events", []string{"replace /retention", "push /events"}, []string{"renderRetention", "renderEvents"}, "retention", ""}},
		{"arrival ends at Back", routeScenario{Start: "/storage", Steps: []string{"renderRoute()", "__setURL('/events')", "onPopState()"}, Caps: watchCaps, Known: true},
			want{"/events", []string{"replace /retention"}, []string{"renderRetention", "renderEvents"}, "retention", ""}},
		// Signing out ends it too: the next sign-in in the same tab, maybe as
		// someone else, did not arrive from an old address.
		{"arrival ends at sign-out", routeScenario{Start: "/storage", Steps: []string{"renderRoute()", "clearAuthState()"}, Caps: watchCaps, Known: true},
			want{"/retention", []string{"replace /retention"}, []string{"renderRetention"}, "retention", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runRouteScenarios(t, map[string]routeScenario{"s": tc.sc})["s"]
			if got.Err != "" {
				t.Fatalf("threw: %s", got.Err)
			}
			if got.URL != tc.want.url {
				t.Errorf("address bar %q, want %q", got.URL, tc.want.url)
			}
			if strings.Join(got.Log, " | ") != strings.Join(tc.want.log, " | ") {
				t.Errorf("history calls %q, want %q", got.Log, tc.want.log)
			}
			if strings.Join(got.Calls, ",") != strings.Join(tc.want.calls, ",") {
				t.Errorf("painted %q, want %q", got.Calls, tc.want.calls)
			}
			if tc.want.nav != "" && (len(got.Nav) == 0 || got.Nav[0] != tc.want.nav) {
				t.Errorf("sidebar lit %q on the first paint, want %q", got.Nav, tc.want.nav)
			}
			if got.From == nil {
				t.Fatal("routeArrivedFrom is not defined")
			}
			if *got.From != tc.want.from {
				t.Errorf("routeArrivedFrom %q, want %q", *got.From, tc.want.from)
			}
		})
	}
}

// TestRouteAliasesLandOnLiveRoutes: every old address names a live route and
// never another old one, so no chain or loop can form, whatever the
// capability check answers. A target may name a SECTION of that route
// ("snapshots#checks"), and that section has to be one the page has a
// heading for — an anchor no heading answers leaves the reader at the top of
// a page three times longer than the one they bookmarked. (Whether the
// heading is DRAWN can still depend on the console; that case is the arrival
// note's job, and TestSnapshotsOnServeDropsTheDaemonParts pins it.)
func TestRouteAliasesLandOnLiveRoutes(t *testing.T) {
	got := runRouteScenarios(t, map[string]routeScenario{})["__targets"]
	var v struct {
		Targets [][][2]string
		Routes  []string
	}
	if got.From == nil || json.Unmarshal([]byte(*got.From), &v) != nil {
		t.Fatalf("could not read the alias table: %+v", got)
	}
	live := map[string]bool{}
	for _, r := range v.Routes {
		live[r] = true
	}
	// The sections the page can draw, by the id snapshotSection() is called
	// with. Read from app.js rather than listed here: a section that is
	// renamed or dropped has to take its anchor with it.
	drawn := map[string]bool{}
	for _, m := range regexp.MustCompile(`snapshotSection\("[^"]*", "([^"]+)"\)`).FindAllStringSubmatch(readAsset(t, "app.js"), -1) {
		drawn[m[1]] = true
	}
	if len(drawn) < 2 {
		t.Fatalf("found %d drawn sections, want the ones Snapshots opens; the call shape changed and this guard reads nothing", len(drawn))
	}
	seen, anchors := 0, 0
	for _, pass := range v.Targets {
		old := map[string]bool{}
		for _, p := range pass {
			old[p[0]] = true
		}
		for _, p := range pass {
			seen++
			route, sect, hasSect := strings.Cut(p[1], "#")
			if !live[route] || old[route] {
				t.Errorf("old address %q goes to %q, want a live route that is not itself an old address", p[0], p[1])
			}
			if hasSect {
				anchors++
				if !drawn[sect] {
					t.Errorf("old address %q lands on section %q of %q, which the page never draws", p[0], sect, route)
				}
			}
		}
	}
	if seen < 12 {
		t.Fatalf("read %d alias targets, want the six old addresses under both capability answers", seen)
	}
	if anchors < 4 {
		t.Fatalf("read %d targets naming a section, want the two merged pages under both capability answers", anchors)
	}
}

// TestTheJumpToASectionIsArmedOncePerArrival: the address keeps naming a
// section for the whole visit (#checks / #setup stay in the bar), and the
// Snapshots page repaints itself through renderRoute on a saved per-server
// location, a saved schedule and a server switch. If each of those re-armed
// the jump, pressing Save at the bottom of the page would throw the reader
// back up to the section heading, every time.
//
// Driven through the real renderRoute with the painters swapped out, so what
// is measured is the arming, not what a fake screen does with it.
func TestTheJumpToASectionIsArmedOncePerArrival(t *testing.T) {
	for _, tc := range []struct {
		name string
		sc   routeScenario
		want bool
	}{
		{"arriving at a section arms it", routeScenario{Start: "/verification", Steps: boot, Caps: watchCaps, Known: true}, true},
		{"a repaint with the same address does not", routeScenario{Start: "/verification",
			Steps: []string{"renderRoute()", "renderRoute()"}, Caps: watchCaps, Known: true}, false},
		{"a page with no section never arms it", routeScenario{Start: "/events", Steps: boot, Caps: watchCaps, Known: true}, false},
		{"navigating away and back arms it again", routeScenario{Start: "/verification",
			Steps: []string{"renderRoute()", "__setURL('/events')", "renderRoute()", "__setURL('/snapshots#setup')", "renderRoute()"},
			Caps:  watchCaps, Known: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runRouteScenarios(t, map[string]routeScenario{"s": tc.sc})["s"]
			if got.Err != "" {
				t.Fatalf("threw: %s", got.Err)
			}
			if got.Pending == nil {
				t.Fatal("scrollPending is not defined; this guard measures nothing")
			}
			if *got.Pending != tc.want {
				t.Errorf("the jump is armed %v, want %v", *got.Pending, tc.want)
			}
		})
	}
}
