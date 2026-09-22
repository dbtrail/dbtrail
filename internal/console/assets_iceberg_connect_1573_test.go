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
// may not read settings is not made to ask, since the server would refuse and
// audit it on every open; no location means no panel; the rest of the page
// renders either way.
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
let asked = [], loc = null;
ctx.__api = async (path) => {
  asked.push(path);
  if (path === "/api/servers") return { servers: [server] };
  if (path.startsWith("/api/baselines")) { if (loc instanceof Error) throw loc; return loc; }
  return {};
};
vm.runInContext("api = (p) => __api(p); currentServer = 'a';", ctx);
const run = async (perms, location) => {
  asked = []; loc = location;
  vm.runInContext("capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + "; capsKnown = true;", ctx);
  await vm.runInContext("renderConnect()", ctx);
  const text = flat(screen).join(" ");
  return { asked: [...asked], panel: text.includes("Keep it current with Iceberg"), page: text.includes("Connect AI"),
    cmd: (text.match(/bintrail export iceberg .*?--warehouse/) || [""])[0] };
};
(async () => {
  const out = {};
  out.dir = await run({}, { configured: true, source: "/data/baselines", kind: "dir", snapshots: [] });
  out.s3 = await run({}, { configured: true, source: "s3://bkt/base", kind: "s3", snapshots: [] });
  out.none = await run({}, { configured: false, snapshots: [] });
  out.refused = await run({}, Object.assign(new Error("backup listings are unavailable while an access-control profile is active"), { status: 403 }));
  out.noPerm = await run({ "settings:read": false }, { configured: true, source: "/data/baselines", kind: "dir", snapshots: [] });
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
		Asked       []string
		Panel, Page bool
		Cmd         string
	}
	var got struct {
		Err                            string
		Dir, S3, None, Refused, NoPerm result
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
		if r.Panel || !r.Page {
			t.Errorf("%s: panel %v, page drawn %v; want no panel and the rest of Connect drawn", name, r.Panel, r.Page)
		}
	}
	if asksLocation(got.NoPerm) || got.NoPerm.Panel || !got.NoPerm.Page {
		t.Errorf("a session without settings:read: asked %q, panel %v, page %v; want nothing asked, no panel, the page drawn",
			got.NoPerm.Asked, got.NoPerm.Panel, got.NoPerm.Page)
	}
}
