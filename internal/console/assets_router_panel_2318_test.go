package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestRouterPanelSaysTheRoutersNumbers renders the router panel of the MCP
// Server page (#2318) through the real app.js, from documents this package's
// own types marshal: no router, a router that does not answer, and its
// numbers in each shape a server can be in. The state that must never be
// drawn is a router that is down shown as a router with zero statements.
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
const panel = vm.runInContext("routerPanel", ctx);
const views = ` + string(payload) + `;
const srv = { id: "s1", name: "shop" };
const draw = (v, cur) => { const p = panel(v, cur); return p === null ? "<no panel>" : flat(p).join(" ").replace(/\s+/g, " "); };
const out = {};
for (const k of Object.keys(views)) out[k] = draw(views[k], srv);
out.failed = draw(null, srv);
out.noServer = draw(views.counts, undefined);
out.otherServer = draw(views.counts, { id: "s2", name: "billing" });
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "router.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	got := map[string]string{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
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
	must("failed", "could not ask this DBTrail", "Reload the page")
	// Down is not zero.
	must("down", "No numbers from the router", "http://router:9091/status", "connection refused", "counts are unknown", "Refresh")
	mustNot("down", "no statements yet", "0 by", "Who answered", "Workers")
	must("counts", "The router", "on port 3310", "read-only", "Who answered for shop since", "9 by MySQL, 4 by the copy, 3 refused",
		"snapshot of", "(12m old)", "whatever its age", "1 of 4 busy, 1 statement waiting", "2 threads and 2048MiB", "error 1040",
		"plan costs at least 10000", "at least 100000 rows", "Every other read runs on MySQL", "Refresh", "These numbers were read at",
		"Why each side", "9 × a cheap plan", "4 × an expensive plan",
		"2 × every worker was busy and as many statements as may wait were already waiting: refused with error 1040",
		"1 × refused: not a read, and the router is read-only")
	// The words of the port inside the service must not be said of a router.
	mustNot("counts", "MySQL answered at once", "this port is read-only", "last looked", "later version")
	must("newer", "later version than this page knows", "may be missing or read as zero", "SHOW ROUTER STATUS", "9 by MySQL")
	must("readWrite", "It is read-write: writes sent to it run on MySQL", "Everything else, writes included, runs on MySQL")
	must("noRule", "No plan threshold is set", "every read runs on MySQL and nothing reaches the copy")
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
	must("cannotRoute", "The router refuses every statement for this server", "The router's log says why", "no statements yet")
	must("refusedAcct", "MySQL refused the router's login for this server", "error 2006", "go on rising", "9 by MySQL")
	must("untracked", "does not tell the router when a statement changes a session setting")
}

// TestRouterPanelIsOnTheConnectPage runs the whole MCP Server page, not the
// panel alone: the panel is drawn from what GET /api/router answered, for
// the server picked, and a console that has no such route (an older one
// behind this page), a session that may not read settings, and a read that
// failed each leave the rest of the page drawn.
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
	script := renderHarnessJS + `
const screen = new FakeEl("main");
document.getElementById = (id) => (id === "view" ? screen : new FakeEl("div"));
const flat = (n, out = []) => { if (n == null) return out; if (typeof n === "string") { out.push(n); return out; }
  if (n.nodeType === 3) { out.push(n.textContent); return out; } if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const servers = [{ id: "a", name: "shop", kind: "registry" }, { id: "b", name: "billing", kind: "registry" }];
let router = null, asked = [];
ctx.__api = async (path) => {
  asked.push(path);
  if (path === "/api/servers") return { servers };
  if (path === "/api/router") { if (router instanceof Error) throw router; return router; }
  return {};
};
vm.runInContext("api = (p, o) => __api(p, o); gateCapabilities = async () => {}; toastError = () => {}; toast = () => {}; icon = () => el('span', {});", ctx);
const run = async (answer, perms = {}, cur = "a") => {
  router = answer; asked = [];
  vm.runInContext("currentServer = " + JSON.stringify(cur) + "; capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + "; capsKnown = true;", ctx);
  await vm.runInContext("renderConnect()", ctx);
  const text = flat(screen).join(" ").replace(/\s+/g, " ");
  return { page: text.includes("Three steps"), panel: text.includes("The router is a second MySQL port"), asked: asked.includes("/api/router"), text };
};
(async () => {
  const view = ` + string(view) + `;
  const out = {};
  out.shown = await run(view);
  out.other = await run(view, {}, "b");
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
    router = answer;
    btn.onclick();
    await new Promise((r) => setImmediate(r));
    return { text: flat(screen).join(" ").replace(/\s+/g, " ") };
  };
  const later = JSON.parse(JSON.stringify(view));
  later.status.servers.a.mysql = 41;
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
		Page, Panel, Asked bool
		Text               string
	}
	var got struct {
		Err                                                    string
		Shown, Other, None, OldServer, Refused, Failed, NoPerm result
		Refreshed, RefreshFailed                               result
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %.400q: %v", raw, err)
	}
	if got.Err != "" {
		t.Fatalf("threw: %s", got.Err)
	}
	if !got.Shown.Page || !got.Shown.Panel || !strings.Contains(got.Shown.Text, "Who answered for shop") || !strings.Contains(got.Shown.Text, "9 by MySQL, 4 by the copy") {
		t.Errorf("a router that answers: page %v, panel %v; want the panel with the picked server's counts in: %s", got.Shown.Page, got.Shown.Panel, got.Shown.Text)
	}
	if !got.Other.Panel || !strings.Contains(got.Other.Text, "Who answered for billing") || !strings.Contains(got.Other.Text, "80 by MySQL, 70 by the copy") || strings.Contains(got.Other.Text, "9 by MySQL") {
		t.Errorf("another server picked: want its own counts, not the first server's, in: %s", got.Other.Text)
	}
	for name, r := range map[string]result{"no router configured": got.None, "a console without the route": got.OldServer, "refused": got.Refused} {
		if !r.Page || r.Panel || strings.Contains(r.Text, "router's numbers") {
			t.Errorf("%s: page %v, panel %v; want the page drawn and nothing said of a router", name, r.Page, r.Panel)
		}
	}
	if !got.Failed.Page || got.Failed.Panel || !strings.Contains(got.Failed.Text, "could not ask this DBTrail for its router's numbers") {
		t.Errorf("the read failed: page %v; want the page drawn and the panel saying it could not ask, in: %s", got.Failed.Page, got.Failed.Text)
	}
	if !strings.Contains(got.Refreshed.Text, "41 by MySQL, 4 by the copy") || strings.Contains(got.Refreshed.Text, "9 by MySQL") {
		t.Errorf("Refresh with a new answer: want the new counts and not the old ones, in: %s", got.Refreshed.Text)
	}
	if strings.Contains(got.RefreshFailed.Text, "9 by MySQL") || strings.Contains(got.RefreshFailed.Text, "Who answered") || !strings.Contains(got.RefreshFailed.Text, "could not ask this DBTrail for its router's numbers") {
		t.Errorf("Refresh that failed: the old counts must leave the screen and the panel say it could not ask, in: %s", got.RefreshFailed.Text)
	}
	if !got.NoPerm.Page || got.NoPerm.Panel || got.NoPerm.Asked {
		t.Errorf("no settings:read: page %v, panel %v, asked %v; want the page drawn, no panel, and the route not asked", got.NoPerm.Page, got.NoPerm.Panel, got.NoPerm.Asked)
	}
}
