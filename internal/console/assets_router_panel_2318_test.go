package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestRouterPanelSaysTheRoutersNumbers renders the router's numbers (#2318)
// through the real app.js, from documents this package's own types marshal:
// no router, a router that does not answer, and its numbers in each shape a
// server can be in. The state that must never be drawn is a router that is
// down shown as a router with zero statements.
//
// Since #2341 the numbers are not a panel of their own: routerNumbers draws
// them inside the Connect a SQL client panel, folded under "The router's
// numbers" when the router answered, and in the open, in red, when it did
// not. The fold and the colour are read here too.
func TestRouterPanelSaysTheRoutersNumbers(t *testing.T) {
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
	doc := func(edit func(*RouterStatusDoc)) *RouterStatusDoc {
		d := &RouterStatusDoc{Version: 1, Listen: "0.0.0.0:3310", ReadOnly: true, CostThreshold: 10000, ScanRows: 100000,
			CopySnapshotsReadAt: "2026-10-11T14:30:50Z",
			RouterNumbers: RouterNumbers{CountingSince: "2026-10-11T09:00:00Z",
				Pool: &RouterPoolNumbers{Workers: 4, Running: 1, Waiting: 1, ThreadsEach: 2, MemoryEach: "2048MiB", WorkerProcessesStarted: 5, StatementsPerWorker: 500},
				Servers: map[string]RouterServerNumbers{
					"s1": {Copy: 4, MySQL: 9, Refused: 3, Reasons: map[string]uint64{"expensive_plan": 4, "cheap_plan": 9, "copy_queue_full": 2, "read_only": 1}, CopySnapshot: "2026-10-11T14:19:00Z"},
				}}}
		if edit != nil {
			edit(d)
		}
		return d
	}
	answered := func(d *RouterStatusDoc) routerViewDTO {
		return routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-11T14:31:00Z", Status: d}
	}
	server := func(edit func(*RouterServerNumbers)) *RouterStatusDoc {
		return doc(func(d *RouterStatusDoc) { s := d.Servers["s1"]; edit(&s); d.Servers["s1"] = s })
	}
	newer := answered(doc(func(d *RouterStatusDoc) { d.Version = 7 }))
	newer.Newer = true
	views := map[string]any{
		"none":       routerViewDTO{},
		"down":       routerViewDTO{Configured: true, URL: "http://router:9091/status", Error: "it could not be reached: dial tcp 172.18.0.5:9091: connect: connection refused"},
		"badAddress": routerViewDTO{Configured: true, Error: "the address given when DBTrail started cannot be used: BINTRAIL_CONSOLE_ROUTER_URL: the router's address names no host"},
		"counts":     answered(doc(nil)),
		"newer":      newer,
		"readWrite":  answered(doc(func(d *RouterStatusDoc) { d.ReadOnly = false })),
		"noRule":     answered(doc(func(d *RouterStatusDoc) { d.CostThreshold, d.ScanRows = 0, 0 })),
		"noRuleRW":   answered(doc(func(d *RouterStatusDoc) { d.CostThreshold, d.ScanRows, d.ReadOnly = 0, 0, false })),
		"noListen":   answered(doc(func(d *RouterStatusDoc) { d.Listen = "" })),
		"noPool":     answered(doc(func(d *RouterStatusDoc) { d.Pool = nil })),
		"empty":      answered(doc(func(d *RouterStatusDoc) { d.Servers = map[string]RouterServerNumbers{} })),
		"notReadYet": answered(doc(func(d *RouterStatusDoc) { d.CopySnapshotsReadAt = "" })),
		"stale":      answered(doc(func(d *RouterStatusDoc) { d.CopySnapshotsReadAt = "2026-10-11T14:20:00Z" })),
		"noCopy":     answered(server(func(s *RouterServerNumbers) { s.CopySnapshot = "" })),
		"cannotRoute": answered(server(func(s *RouterServerNumbers) {
			*s = RouterServerNumbers{Reasons: map[string]uint64{}, CannotRoute: true}
		})),
		"refusedAcct": answered(server(func(s *RouterServerNumbers) { s.AccountRefused = true })),
		"untracked":   answered(server(func(s *RouterServerNumbers) { s.SessionUntracked = true })),
	}
	payload, err := json.Marshal(views)
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const flat = (n, out = []) => { if (n == null) return out; if (typeof n === "string") { out.push(n); return out; }
  if (n.nodeType === 3) { out.push(n.textContent); return out; } if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const numbers = vm.runInContext("routerNumbers", ctx);
const views = ` + string(payload) + `;
const srv = { id: "s1", name: "shop" };
const walk = (n, f, inFold) => { if (!n || typeof n === "string" || n.nodeType === 3) return;
  const fold = inFold || (n.tag === "details" ? flat((n.children || []).find((c) => c && c.tag === "summary")).join("") : "");
  f(n, fold); (n.children || []).forEach((c) => walk(c, f, fold)); };
// shape: what a reader sees before unfolding anything. open counts the
// lines drawn outside any fold, red the ones drawn as an error, folded the
// ones under "The router's numbers".
const shape = {};
const draw = (k, v, cur) => { const p = numbers(v, cur, () => {});
  if (p === null) return "<no panel>";
  const s = { open: 0, red: 0, folded: 0, refresh: 0, refreshFolded: 0, whyFolded: 0 };
  walk(p, (x, fold) => {
    if (x.tag === "p") { if (fold === "The router's numbers") s.folded++; else if (!fold) s.open++; if (/(^| )cn-sql-err( |$)/.test(x.className)) s.red++; }
    if (x.tag === "button" && flat(x).join("") === "Refresh") { s.refresh++; if (fold === "The router's numbers") s.refreshFolded++; }
    if (x.tag === "li" && fold === "Why each side") s.whyFolded++;
  });
  shape[k] = s;
  return flat(p).join(" ").replace(/\s+/g, " "); };
const out = {};
for (const k of Object.keys(views)) out[k] = draw(k, views[k], srv);
out.failed = draw("failed", null, srv);
out.noServer = draw("noServer", views.counts, undefined);
out.otherServer = draw("otherServer", views.counts, { id: "s2", name: "billing" });
// Refresh asks once and cannot be pressed twice while it asks.
let asked = 0, btn = null;
walk(numbers(views.counts, srv, () => { asked++; }), (x) => { if (x.tag === "button" && flat(x).join("") === "Refresh") btn = x; });
btn.onclick();
console.log(JSON.stringify({ text: out, shape, refresh: { asked, disabled: !!btn.disabled } }));
`
	path := filepath.Join(t.TempDir(), "router.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type drawnShape struct{ Open, Red, Folded, Refresh, RefreshFolded, WhyFolded int }
	var all struct {
		Text    map[string]string
		Shape   map[string]drawnShape
		Refresh struct {
			Asked    int
			Disabled bool
		}
	}
	if err := json.Unmarshal(raw, &all); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	got, shape := all.Text, all.Shape
	must := func(name string, wants ...string) {
		t.Helper()
		text, ok := got[name]
		if !ok {
			t.Fatalf("%s: not drawn", name)
		}
		t.Logf("%-12s %s", name+":", text)
		for _, w := range wants {
			if !strings.Contains(text, w) {
				t.Errorf("%s: missing %q in: %s", name, w, text)
			}
		}
		for _, bad := range []string{"—", "undefined", "NaN", "Invalid Date", "null", "[object"} {
			if strings.Contains(text, bad) {
				t.Errorf("%s: holds %q: %s", name, bad, text)
			}
		}
	}
	mustNot := func(name string, bads ...string) {
		t.Helper()
		for _, b := range bads {
			if strings.Contains(got[name], b) {
				t.Errorf("%s: says %q: %s", name, b, got[name])
			}
		}
	}
	if got["none"] != "<no panel>" {
		t.Errorf("no router configured: drew %q, want no panel", got["none"])
	}
	must("failed", "could not ask this DBTrail", "Reload the page", "Refresh")
	// Down is not zero.
	must("down", "No numbers from the router", "http://router:9091/status", "connection refused", "counts are unknown", "Refresh")
	mustNot("down", "no statements yet", "0 by", "Who answered", "Workers")
	// A router that did not answer is said in the open, in red, never under
	// a fold a reader may not open; one that answered folds its numbers,
	// with nothing red. Either way there is one Refresh.
	for _, k := range []string{"down", "badAddress", "failed"} {
		if s := shape[k]; s.Open == 0 || s.Red != s.Open || s.Folded != 0 || s.Refresh != 1 || s.RefreshFolded != 0 {
			t.Errorf("%s: drawn as %+v; want every line in the open and red, none folded, and one Refresh in the open", k, s)
		}
		mustNot(k, "The router's numbers")
	}
	for k, s := range shape {
		if k == "down" || k == "badAddress" || k == "failed" {
			continue
		}
		if s.Folded == 0 || s.Open != 0 || s.Red != 0 || s.Refresh != 1 || s.RefreshFolded != 1 {
			t.Errorf("%s: drawn as %+v; want every line folded under The router's numbers, none red, and one Refresh with them", k, s)
		}
		must(k, "The router's numbers")
	}
	if s := shape["counts"]; s.WhyFolded != 4 {
		t.Errorf("counts: %d reasons under Why each side, want 4", s.WhyFolded)
	}
	if all.Refresh.Asked != 1 || !all.Refresh.Disabled {
		t.Errorf("Refresh pressed once: asked %d time(s), disabled %v; want one asking and the button disabled while it asks", all.Refresh.Asked, all.Refresh.Disabled)
	}
	must("counts", "The router is the service that answers this port, on port 3310.", "read-only", "Who answered for shop since", "9 by MySQL, 4 by the copy, 3 refused",
		"snapshot of", "(12m old)", "whatever its age", "1 of 4 busy, 1 statement waiting", "2 threads and 2048MiB", "error 1040",
		"plan costs at least 10000", "at least 100000 rows", "Every other read runs on MySQL", "Refresh", "These numbers were read at",
		"Why each side", "9 × a cheap plan", "4 × an expensive plan",
		"2 × every worker was busy and thousands of statements were already waiting, more than the router lets wait: refused with error 1040 at once",
		"1 × refused: not a read, and the router is read-only")
	// The words of the port inside the service must not be said of a router.
	mustNot("counts", "MySQL answered at once", "this port is read-only", "last looked", "later version", "second MySQL port")
	// A router that names no address is not given a port number.
	must("noListen", "The router is the service that answers this port. It is read-only")
	mustNot("noListen", "on port")
	must("newer", "later version than this page knows", "may be missing or read as zero", "SHOW ROUTER STATUS", "9 by MySQL")
	must("readWrite", "It is read-write: with read routing on, writes sent to it run on MySQL", "Everything else, writes included, runs on MySQL")
	mustNot("readWrite", "It is read-only", "Every other read")
	must("noRule", "No plan threshold is set", "every read runs on MySQL and nothing reaches the copy")
	must("noRuleRW", "No plan threshold is set", "every statement runs on MySQL and nothing reaches the copy")
	must("noPool", "9 by MySQL")
	mustNot("noPool", "Workers for the copy")
	// A server the router says nothing of: not sent anything, or not one of
	// its servers. The document cannot tell, so the page says both.
	must("empty", "Who answered for shop since", "nothing to show", "the router does not have this server", "The router knows no copy for shop")
	must("otherServer", "Who answered for billing", "nothing to show", "the router does not have this server", "The router knows no copy for billing")
	mustNot("otherServer", "9 by MySQL", "Why each side", "no statements yet")
	must("badAddress", "No numbers from the router", "cannot be used", "names no host")
	mustNot("badAddress", "Asked at", "Who answered")
	must("noServer", "pick a server in the left sidebar", "Workers for the copy")
	mustNot("noServer", "9 by MySQL")
	must("notReadYet", "has not read its copies yet", "9 by MySQL")
	mustNot("notReadYet", "snapshot of")
	must("stale", "snapshot of", "The router last looked 11m ago", "a newer one may exist")
	must("noCopy", "The router knows no copy for shop", "9 by MySQL")
	must("cannotRoute", "The copy answers everything for this server, the light reads too", "the router's log says why", "no statements yet")
	must("refusedAcct", "MySQL refused the router's login for this server", "error 2006", "go on rising", "9 by MySQL")
	must("untracked", "does not tell the router when a statement changes a session setting")
}

// TestRouterPanelIsOnTheConnectPage runs the whole MCP Server page, not the
// panel alone: the router's numbers are drawn from what GET /api/router
// answered, for the server picked, and a console that has no such route (an
// older one behind this page), a session that may not read settings, and a
// read that failed each leave the rest of the page drawn.
//
// Since #2341 there is one panel: the numbers, who answered and the port's
// address are all inside Connect a SQL client, and no panel is titled "The
// router". The port is on in every run (the status a real PUT answered), or
// none of this is drawn.
func TestRouterPanelIsOnTheConnectPage(t *testing.T) {
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
	view, err := json.Marshal(routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-11T14:31:00Z",
		Status: &RouterStatusDoc{Version: 1, Listen: "0.0.0.0:3310", ReadOnly: true, CostThreshold: 10000, CopySnapshotsReadAt: "2026-10-11T14:30:50Z",
			RouterNumbers: RouterNumbers{CountingSince: "2026-10-11T09:00:00Z", Servers: map[string]RouterServerNumbers{
				"a": {Copy: 4, MySQL: 9, Reasons: map[string]uint64{"expensive_plan": 4, "cheap_plan": 9}, CopySnapshot: "2026-10-11T14:19:00Z"},
				"b": {Copy: 70, MySQL: 80, Reasons: map[string]uint64{}},
			}}}})
	if err != nil {
		t.Fatal(err)
	}
	down, err := json.Marshal(routerViewDTO{Configured: true, URL: "http://router:9091/status", Error: "it could not be reached: connection refused"})
	if err != nil {
		t.Fatal(err)
	}
	// The port, on: what the real routes answer once it was turned on.
	s, _, _ := newManagedFlashback(t, Config{})
	if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`); code != 200 {
		t.Fatalf("setup: turning the port on = %d %s", code, raw)
	}
	code, fbOn, fbRaw := doFlashback(t, s, "GET", "/api/flashback", "")
	if code != 200 || fbOn["enabled"] != true || fbOn["can_manage"] != true {
		t.Fatalf("setup: the port is not on and manageable: %d %s", code, fbRaw)
	}
	script := renderHarnessJS + `
const screen = new FakeEl("main");
document.getElementById = (id) => (id === "view" ? screen : new FakeEl("div"));
const flat = (n, out = []) => { if (n == null) return out; if (typeof n === "string") { out.push(n); return out; }
  if (n.nodeType === 3) { out.push(n.textContent); return out; } if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const servers = [{ id: "a", name: "shop", kind: "registry" }, { id: "b", name: "billing", kind: "registry" }];
const each = (n, f) => { if (!n || typeof n === "string" || n.nodeType === 3) return; f(n); (n.children || []).forEach((c) => each(c, f)); };
let router = null, asked = [], toasts = [];
ctx.__api = async (path, opts) => {
  asked.push(opts ? [path, opts] : path);
  if (path === "/api/servers") return { servers };
  if (path === "/api/flashback") return ` + fbRaw + `;
  if (path === "/api/router") { if (router instanceof Error) throw router; return router; }
  return {};
};
ctx.__toast = (m) => toasts.push(String(m));
vm.runInContext("api = (p, o) => __api(p, o); gateCapabilities = async () => {}; toastError = (m) => __toast(m); toast = () => {}; icon = () => el('span', {});", ctx);
const read = () => {
  const text = flat(screen).join(" ").replace(/\s+/g, " ");
  const o = { page: text.includes("Three steps"), panel: text.includes("The router is the service that answers this port"), text,
    titles: [], figs: [], cmd: [], unconfirmed: [], routerSaid: [] };
  each(screen, (x) => {
    const t = flat(x).join(" ").replace(/\s+/g, " ").trim();
    if (x.tag === "h2") o.titles.push(t);
    if (x.className === "cn-who-figs") o.figs.push(t);
    if (x.tag === "code" && t.startsWith("mysql -h ")) o.cmd.push(t);
    if (x.tag === "p" && t.includes("not a confirmed one")) o.unconfirmed.push(t);
    // What is said of a router, outside the sentence about the port's number.
    if (x.tag === "p" && /router's numbers|No numbers from the router/.test(t)) o.routerSaid.push(t);
    if (x.tag === "summary" && t === "The router's numbers") o.routerSaid.push(t);
  });
  return o;
};
const run = async (answer, perms = {}, cur = "a") => {
  router = answer; asked = []; toasts = [];
  vm.runInContext("currentServer = " + JSON.stringify(cur) + "; capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + "; capsKnown = true;", ctx);
  await vm.runInContext("renderConnect()", ctx);
  return Object.assign(read(), { asked: asked.includes("/api/router") });
};
(async () => {
  const view = ` + string(view) + `;
  const out = {};
  out.shown = await run(view);
  out.other = await run(view, {}, "b");
  out.down = await run(` + string(down) + `);
  out.none = await run({ configured: false });
  out.oldServer = await run(Object.assign(new Error("not found"), { status: 404 }));
  out.refused = await run(Object.assign(new Error("forbidden"), { status: 403 }));
  out.failed = await run(Object.assign(new Error("boom"), { status: 500 }));
  out.noPerm = await run(view, { "settings:read": false });
  // Refresh: pressed on a painted panel. A second answer repaints with its
  // numbers; a failed one takes the old numbers off the screen.
  const press = async (answer) => {
    await run(view);
    const find = (n) => { if (!n || typeof n === "string") return null; if (n.tag === "button" && flat(n).join("") === "Refresh") return n;
      for (const c of n.children || []) { const f = find(c); if (f) return f; } return null; };
    const btn = find(screen);
    if (!btn) return { text: "<no Refresh button>" };
    router = answer; asked = []; toasts = [];
    btn.onclick();
    await new Promise((r) => setImmediate(r));
    return Object.assign(read(), { sent: asked.slice(), toasts: toasts.slice(), again: !!find(screen) });
  };
  const later = JSON.parse(JSON.stringify(view));
  later.status.servers.a.mysql = 41;
  later.status.listen = "0.0.0.0:3399";
  out.refreshed = await press(later);
  out.refreshFailed = await press(Object.assign(new Error("boom"), { status: 500 }));
  console.log(JSON.stringify(out));
})().catch((e) => console.log(JSON.stringify({ err: String((e && e.stack) || e) })));
`
	path := filepath.Join(t.TempDir(), "connect_router.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type result struct {
		Page, Panel, Asked                         bool
		Text                                       string
		Titles, Figs, Cmd, Unconfirmed, RouterSaid []string
		Sent                                       []json.RawMessage
		Toasts                                     []string
		Again                                      bool
	}
	var got struct {
		Err                                                          string
		Shown, Other, Down, None, OldServer, Refused, Failed, NoPerm result
		Refreshed, RefreshFailed                                     result
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %.400q: %v", raw, err)
	}
	if got.Err != "" {
		t.Fatalf("threw: %s", got.Err)
	}
	figs := func(r result) string { return strings.Join(r.Figs, " | ") }
	if !got.Shown.Page || !got.Shown.Panel || !strings.Contains(got.Shown.Text, "Who answered for shop") || !strings.Contains(got.Shown.Text, "9 by MySQL, 4 by the copy") ||
		figs(got.Shown) != "9 by MySQL 4 by the copy" {
		t.Errorf("a router that answers: page %v, panel %v, figures %q; want the panel with the picked server's counts in: %s", got.Shown.Page, got.Shown.Panel, got.Shown.Figs, got.Shown.Text)
	}
	// One panel: the router has no panel of its own, and its numbers are
	// inside Connect a SQL client.
	sqlPanels := 0
	for _, title := range got.Shown.Titles {
		if title == "The router" {
			t.Errorf("a panel titled The router is drawn; titles: %q", got.Shown.Titles)
		}
		if title == "Connect a SQL client" {
			sqlPanels++
		}
	}
	if sqlPanels != 1 {
		t.Errorf("%d panels titled Connect a SQL client, want one; titles: %q", sqlPanels, got.Shown.Titles)
	}
	// The address in the command is the one the router says.
	if len(got.Shown.Cmd) != 1 || !strings.Contains(got.Shown.Cmd[0], " -P 3310 ") || !strings.HasSuffix(got.Shown.Cmd[0], " -u shop -p") || len(got.Shown.Unconfirmed) != 0 {
		t.Errorf("a router that answers: command %q, unconfirmed %q; want the router's port 3310, the picked server, and no doubt about the port", got.Shown.Cmd, got.Shown.Unconfirmed)
	}
	if !got.Other.Panel || !strings.Contains(got.Other.Text, "Who answered for billing") || !strings.Contains(got.Other.Text, "80 by MySQL, 70 by the copy") || strings.Contains(got.Other.Text, "9 by MySQL") ||
		figs(got.Other) != "80 by MySQL 70 by the copy" {
		t.Errorf("another server picked: figures %q; want its own counts, not the first server's, in: %s", got.Other.Figs, got.Other.Text)
	}
	// A router that was configured and did not answer: no counts, never
	// zeros, the usual port and the reason it is not confirmed.
	if !got.Down.Page || got.Down.Panel || len(got.Down.Figs) != 0 || strings.Contains(got.Down.Text, "by MySQL") || !strings.Contains(got.Down.Text, "No numbers from the router") ||
		len(got.Down.Cmd) != 1 || !strings.Contains(got.Down.Cmd[0], " -P 3309 ") || len(got.Down.Unconfirmed) != 1 || !strings.Contains(got.Down.Unconfirmed[0], "The router that serves the port did not answer") {
		t.Errorf("a router that did not answer: figures %q, command %q, unconfirmed %q; want no counts, port 3309 and the line saying it did not answer, in: %s", got.Down.Figs, got.Down.Cmd, got.Down.Unconfirmed, got.Down.Text)
	}
	for name, r := range map[string]result{"no router configured": got.None, "a console without the route": got.OldServer, "refused": got.Refused} {
		if !r.Page || r.Panel || len(r.Figs) != 0 || len(r.Cmd) != 1 || !strings.Contains(r.Cmd[0], " -P 3309 ") {
			t.Errorf("%s: page %v, panel %v, figures %q, command %q; want the page drawn with the usual port and no counts", name, r.Page, r.Panel, r.Figs, r.Cmd)
		}
		if len(r.Unconfirmed) != 1 || !strings.Contains(r.Unconfirmed[0], "This DBTrail was not told where the router that serves the port is") {
			t.Errorf("%s: the port's number must be said to be unconfirmed, without blaming a router: %q", name, r.Unconfirmed)
		}
	}
	// No router configured: nothing is said of a router's numbers.
	if len(got.None.RouterSaid) != 0 {
		t.Errorf("no router configured: the page speaks of a router's numbers: %q", got.None.RouterSaid)
	}
	// A router that was NOT ASKED (the route answered 404 or 403, or the
	// session may not read settings) is not a router that could not be
	// asked: nothing is said of its numbers, and no Refresh is offered that
	// could only be refused.
	for name, r := range map[string]result{"a console without the route": got.OldServer, "refused": got.Refused, "no settings:read": got.NoPerm} {
		if len(r.RouterSaid) != 0 {
			t.Errorf("%s: a router that was not asked is drawn as one that could not be asked: %q", name, r.RouterSaid)
		}
	}
	// The asking failed: the unconfirmed port is said as that, not as a
	// DBTrail that was never told of a router.
	if !strings.Contains(got.Failed.Text, "This DBTrail could not ask the router that serves the port, so 3309 above") || strings.Contains(got.Failed.Text, "was not told where the router") {
		t.Errorf("the read failed: want the port said as unconfirmed because the asking failed, in: %s", got.Failed.Text)
	}
	if !got.Failed.Page || got.Failed.Panel || len(got.Failed.Figs) != 0 || !strings.Contains(got.Failed.Text, "could not ask this DBTrail for its router's numbers") {
		t.Errorf("the read failed: page %v; want the page drawn and the panel saying it could not ask, in: %s", got.Failed.Page, got.Failed.Text)
	}
	// Refresh asks the one route again, with no body, and the whole panel is
	// drawn from the answer: the lines, the figures and the port.
	if len(got.Refreshed.Sent) != 1 || string(got.Refreshed.Sent[0]) != `"/api/router"` {
		t.Errorf("Refresh sent %s, want exactly one GET /api/router", got.Refreshed.Sent)
	}
	if !strings.Contains(got.Refreshed.Text, "41 by MySQL, 4 by the copy") || strings.Contains(got.Refreshed.Text, "9 by MySQL") || figs(got.Refreshed) != "41 by MySQL 4 by the copy" ||
		len(got.Refreshed.Cmd) != 1 || !strings.Contains(got.Refreshed.Cmd[0], " -P 3399 ") || len(got.Refreshed.Toasts) != 0 || !got.Refreshed.Again {
		t.Errorf("Refresh with a new answer: figures %q, command %q, toasts %q; want the new counts and port, not the old ones, in: %s", got.Refreshed.Figs, got.Refreshed.Cmd, got.Refreshed.Toasts, got.Refreshed.Text)
	}
	if strings.Contains(got.RefreshFailed.Text, "9 by MySQL") || strings.Contains(got.RefreshFailed.Text, "Who answered") || len(got.RefreshFailed.Figs) != 0 ||
		!strings.Contains(got.RefreshFailed.Text, "could not ask this DBTrail for its router's numbers") {
		t.Errorf("Refresh that failed: the old counts must leave the screen and the panel say it could not ask, in: %s", got.RefreshFailed.Text)
	}
	// The failure is said, the port falls back to its usual number, and the
	// button is there to try again.
	if len(got.RefreshFailed.Toasts) != 1 || !strings.Contains(got.RefreshFailed.Toasts[0], "Could not refresh the router's numbers: boom") || !got.RefreshFailed.Again ||
		len(got.RefreshFailed.Cmd) != 1 || !strings.Contains(got.RefreshFailed.Cmd[0], " -P 3309 ") || len(got.RefreshFailed.Unconfirmed) != 1 {
		t.Errorf("Refresh that failed: toasts %q, Refresh still drawn %v, command %q, unconfirmed %q; want the reason said once, the button back, and port 3309 marked unconfirmed",
			got.RefreshFailed.Toasts, got.RefreshFailed.Again, got.RefreshFailed.Cmd, got.RefreshFailed.Unconfirmed)
	}
	if !got.NoPerm.Page || got.NoPerm.Panel || got.NoPerm.Asked || len(got.NoPerm.Figs) != 0 {
		t.Errorf("no settings:read: page %v, panel %v, asked %v; want the page drawn, no numbers, and the route not asked", got.NoPerm.Page, got.NoPerm.Panel, got.NoPerm.Asked)
	}
}
