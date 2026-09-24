package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestIcebergPanelLivesOnConnect (#1573): the Iceberg export panel is on
// Connect AI, built from where the selected server's snapshots live
// (GET /api/baselines?location_only=1, no walk of the storage). A session that
// may not read the server, or under a data profile, is not made to ask, since
// the server would refuse it on every open; the gated permission is the one
// that route takes (servers:read), so a session that HOLDS it is never shown
// a panel whose location lookup then 403s; no location or a refusal means no
// panel, but any other failure says so, so a missing panel never reads as "no
// backups"; the rest of the page renders either way, and a page the reader
// left while the requests were out is not painted.
func TestIcebergPanelLivesOnConnect(t *testing.T) {
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
	script := renderHarnessJS + `
const screen = new FakeEl("main");
document.getElementById = (id) => (id === "view" ? screen : new FakeEl("div"));
const flat = (n, out = []) => { if (!n) return out; if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const server = { id: "a", name: "a", kind: "registry", host: "db.internal", port: "3307", user: "reader", dbname: "idx", has_password: true };
let asked = [], loc = null, leaveOnAsk = false, serversAnswer = null, onMint = null;
const toasts = [];
ctx.__api = async (path, opts) => {
  asked.push(path);
  if (path === "/api/servers") {
    if (serversAnswer instanceof Error) throw serversAnswer;
    return serversAnswer || { servers: [server] };
  }
  if (path === "/api/mcp-token" && opts && opts.method === "POST") {
    if (onMint) onMint();
    return { token: "minted-plaintext" };
  }
  if (path === "/api/mcp-token" && opts && opts.method === "DELETE") {
    if (onMint) onMint();
    return {};
  }
  if (path.startsWith("/api/baselines")) {
    // The reader clicks another page while the location is out.
    if (leaveOnAsk) vm.runInContext("viewGen++", ctx);
    if (loc instanceof Error) throw loc;
    return loc;
  }
  return {};
};
ctx.__toasts = toasts;
// The token cases stand on /connect, where the page head adds the Docs link,
// whose icon is an SVG import this fake document cannot do; the link is not
// what this test is about.
vm.runInContext("api = (p, o) => __api(p, o); currentServer = 'a'; gateCapabilities = async () => {};" +
  "toastError = (m) => __toasts.push(m); toast = () => {}; confirm = () => true; icon = () => el('span', {});", ctx);
const run = async (perms, location, caps = {}, leave = false, servers = null, cur = "a") => {
  asked = []; loc = location; leaveOnAsk = leave; serversAnswer = servers;
  vm.runInContext("currentServer = " + JSON.stringify(cur) + "; defaultServerId = 'stale-from-an-old-load';", ctx);
  vm.runInContext("capsCache = " + JSON.stringify(Object.assign({ monitor: true, permissions: perms }, caps)) + "; capsKnown = true;", ctx);
  await vm.runInContext("renderConnect()", ctx);
  const text = flat(screen).join(" ");
  return { asked: [...asked], panel: text.includes("Keep it current with Iceberg"), page: text.includes("Connect AI"),
    steps: text.includes("Three steps"), note: text.includes("Could not check where this server's snapshots are kept"),
    cmd: (text.match(/bintrail export iceberg .*?--warehouse/) || [""])[0] };
};
const where = { configured: true, source: "/data/baselines", kind: "dir" };
(async () => {
  const out = {};
  out.dir = await run({}, { configured: true, source: "/data/baselines", kind: "dir", snapshots: [] });
  out.s3 = await run({}, { configured: true, source: "s3://bkt/base", kind: "s3", snapshots: [] });
  out.none = await run({}, { configured: false, snapshots: [] });
  out.refused = await run({}, Object.assign(new Error("snapshot listings are unavailable while an access-control profile is active"), { status: 403 }));
  out.noPerm = await run({ "servers:read": false }, { configured: true, source: "/data/baselines", kind: "dir", snapshots: [] });
  out.profile = await run({}, where, { data_profile: true });
  out.failed = await run({}, Object.assign(new Error("server error"), { status: 500 }));
  out.network = await run({}, new Error("Failed to fetch"));
  out.left = await run({}, where, {}, true);
  // The server list fails: the command has no index address, so the panel
  // cannot be drawn, and that must be said too.
  out.serversFailed = await run({}, where, {}, false, Object.assign(new Error("boom"), { status: 500 }));
  // A console with no server: the list 404s, so does the location, and there
  // is no "this server" for a note to name.
  out.empty = await run({}, Object.assign(new Error("no servers"), { status: 404 }), {}, false,
    Object.assign(new Error("no servers"), { status: 404 }), "");
  // Nothing selected: the default is the one THIS response names, not the
  // one an older load left in defaultServerId.
  out.fresh = await run({}, where, {}, false, { servers: [server], default_id: "a" }, "");
  // A token made while the reader stays on Connect repaints it; one made after
  // the reader left must not cover the page they moved to.
  const mint = async (leave, call = "mintMCPToken(false)") => {
    toasts.length = 0; asked = []; loc = where; serversAnswer = null;
    vm.runInContext("currentServer = 'a'; location.pathname = '/connect';", ctx);
    onMint = leave ? () => vm.runInContext("location.pathname = '/events'", ctx) : null;
    vm.runInContext("clear(VIEW()); VIEW().append(el('p', { text: 'Events page' }))", ctx);
    await vm.runInContext(call, ctx);
    // mintMCPToken does not await the repaint it starts; let it finish.
    await new Promise((r) => setImmediate(r));
    onMint = null;
    const text = flat(screen).join(" ");
    return { steps: text.includes("Three steps"), events: text.includes("Events page"),
      toast: toasts.join(" | "), parked: vm.runInContext("mcpMintedOnce", ctx) !== null };
  };
  out.mintStay = await mint(false);
  out.mintLeft = await mint(true);
  out.revokeStay = await mint(false, "revokeMCPToken()");
  out.revokeLeft = await mint(true, "revokeMCPToken()");
  console.log(JSON.stringify(out));
})().catch((e) => console.log(JSON.stringify({ err: String((e && e.stack) || e) })));
`
	path := filepath.Join(t.TempDir(), "connect.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type result struct {
		Asked                    []string
		Panel, Page, Steps, Note bool
		Cmd                      string
	}
	type minted struct {
		Steps, Events, Parked bool
		Toast                 string
	}
	var got struct {
		Err                                                            string
		Dir, S3, None, Refused, NoPerm, Profile, Failed, Network, Left result
		ServersFailed, Empty, Fresh                                    result
		MintStay, MintLeft, RevokeStay, RevokeLeft                     minted
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Err != "" {
		t.Fatalf("threw: %s", got.Err)
	}
	asksLocation := func(r result) bool {
		for _, p := range r.Asked {
			if p == "/api/baselines?location_only=1" {
				return true
			}
			if strings.HasPrefix(p, "/api/baselines") {
				t.Errorf("Connect asked %q: only the location_only form, never the listing that walks the storage", p)
			}
		}
		return false
	}
	if !got.Dir.Panel || !strings.Contains(got.Dir.Cmd, "--baseline-dir '/data/baselines'") || !asksLocation(got.Dir) {
		t.Errorf("a directory destination: panel %v, command %q, asked %q; want the panel with --baseline-dir", got.Dir.Panel, got.Dir.Cmd, got.Dir.Asked)
	}
	if !got.S3.Panel || !strings.Contains(got.S3.Cmd, "--baseline-s3 's3://bkt/base'") {
		t.Errorf("an S3 destination: panel %v, command %q; want the panel with --baseline-s3", got.S3.Panel, got.S3.Cmd)
	}
	for name, r := range map[string]result{"nothing configured": got.None, "refused (a data profile)": got.Refused} {
		if r.Panel || r.Note || !r.Page {
			t.Errorf("%s: panel %v, note %v, page drawn %v; want no panel, no note, and the rest of Connect drawn", name, r.Panel, r.Note, r.Page)
		}
	}
	for name, r := range map[string]result{"no servers:read": got.NoPerm, "a data profile": got.Profile} {
		if asksLocation(r) || r.Panel || r.Note || !r.Page {
			t.Errorf("%s: asked %q, panel %v, note %v, page %v; want nothing asked, no panel or note, the page drawn",
				name, r.Asked, r.Panel, r.Note, r.Page)
		}
	}
	for name, r := range map[string]result{"a server error": got.Failed, "a network error": got.Network} {
		if r.Panel || !r.Note || !r.Page {
			t.Errorf("%s: panel %v, note %v, page %v; want the one-line note in place of the panel, and the page drawn",
				name, r.Panel, r.Note, r.Page)
		}
	}
	if !asksLocation(got.Left) || got.Left.Steps || got.Left.Panel {
		t.Errorf("left while the location was out: asked %q, steps drawn %v, panel %v; want nothing painted over the next page",
			got.Left.Asked, got.Left.Steps, got.Left.Panel)
	}
	if got.ServersFailed.Panel || !got.ServersFailed.Note || !got.ServersFailed.Page {
		t.Errorf("the server list failed: panel %v, note %v, page %v; want the note, since the command lost its index address",
			got.ServersFailed.Panel, got.ServersFailed.Note, got.ServersFailed.Page)
	}
	if got.Empty.Panel || got.Empty.Note || !got.Empty.Page {
		t.Errorf("a console with no server: panel %v, note %v, page %v; want neither, and the page drawn",
			got.Empty.Panel, got.Empty.Note, got.Empty.Page)
	}
	if !got.Fresh.Panel {
		t.Errorf("nothing selected, the response names the default: panel %v, asked %q; want the panel for that server, not for the stale defaultServerId",
			got.Fresh.Panel, got.Fresh.Asked)
	}
	if !got.MintStay.Steps || got.MintStay.Toast != "" {
		t.Errorf("token made, still on Connect: steps %v, toast %q; want Connect repainted with no error", got.MintStay.Steps, got.MintStay.Toast)
	}
	if got.MintLeft.Steps || !got.MintLeft.Events || got.MintLeft.Parked || got.MintLeft.Toast == "" {
		t.Errorf("token made after the reader left: steps %v, events page kept %v, token parked %v, toast %q; want the next page untouched, the token dropped and said",
			got.MintLeft.Steps, got.MintLeft.Events, got.MintLeft.Parked, got.MintLeft.Toast)
	}
	if !got.RevokeStay.Steps {
		t.Errorf("token deleted, still on Connect: steps %v; want Connect repainted", got.RevokeStay.Steps)
	}
	if got.RevokeLeft.Steps || !got.RevokeLeft.Events {
		t.Errorf("token deleted after the reader left: steps %v, events page kept %v; want the next page untouched",
			got.RevokeLeft.Steps, got.RevokeLeft.Events)
	}
	if !got.Dir.Steps {
		t.Errorf("the steps text this test uses to see a painted page is gone from Connect; re-anchor the left-page check")
	}
}
