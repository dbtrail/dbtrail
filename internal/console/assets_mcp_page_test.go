package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestMCPPageStates drives the real MCP Server page pieces (the token step,
// the install step, the drawn Claude box and the connection line) in the
// states the page has to tell apart, and reads the words and links each one
// draws.
func TestMCPPageStates(t *testing.T) {
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
const flat = (n, out = []) => { if (!n) return out; if (typeof n === "string") { out.push(n); return out; } if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const hrefs = (n, out = []) => { if (!n || typeof n === "string" || n.nodeType === 3) return out; if (n.attrs && n.attrs.href) out.push(n.attrs.href);
  for (const c of n.children || []) hrefs(c, out); return out; };
const draw = (expr, vars) => { Object.assign(ctx, vars || {}); const n = vm.runInContext(expr, ctx);
  return { text: flat(n).join(" "), hrefs: hrefs(n), cls: n.className || "" }; };
const ago = (s) => new Date(Date.now() - s * 1000).toISOString();
const ua = (s) => vm.runInContext("navigator = { userAgent: " + JSON.stringify(s) + " };", ctx);
vm.runInContext('capsCache = { mcp: true, version: "0.108.1" }; capsKnown = true;', ctx);
const servers = [{ id: "s1", name: "shop" }];
const out = {
  mintedNoStatus: draw("mcpTokenStep(null, 'dbt_x')"),
  noStatus: draw("mcpTokenStep(null, null)"),
  staticStep: draw("mcpTokenStep({ static: true, managed: false }, null)"),
  staticBox: draw("claudeAskMock(__s, { static: true, managed: false }, null, { desktop: true })", { __s: servers }),
  managedBox: draw("claudeAskMock(__s, { static: false, managed: true }, null, { desktop: true })", { __s: servers }),
  recent: draw("mcpConnLine(__t)", { __t: { managed: true, last_used_at: ago(180), counting_since: "2026-10-18T13:00:00Z" } }),
  old: draw("mcpConnLine(__t)", { __t: { managed: true, last_used_at: ago(3 * 86400), counting_since: "2026-10-15T13:00:00Z" } }),
  unused: draw("mcpConnLine(__t)", { __t: { managed: true, counting_since: "2026-10-18T13:00:00Z" } }),
  staticGone: draw("mcpConnLine(__t)", { __t: { managed: true, static: false, static_last_used_at: ago(60) } }),
  noToken: draw("mcpConnLine(__t)", { __t: { managed: false, static: false } }),
};
vm.runInContext("capsCache = { mcp: false, version: '0.108.1' };", ctx);
out.mintedCapsBehind = draw("claudeAskMock(__s, { managed: true }, 'dbt_x', { desktop: true })", { __s: servers });
vm.runInContext("capsCache = { mcp: true, version: '0.108.1' };", ctx);
for (const [k, s] of [["mac", "Mozilla/5.0 (Macintosh)"], ["linux", "Mozilla/5.0 (X11; Linux x86_64)"], ["windows", "Mozilla/5.0 (Windows NT 10.0)"]]) {
  ua(s); out[k] = draw("mcpInstallStep(mcpInstallArm())");
}
// The tunnel note, by the host this page was opened on.
const tunnel = (host, port, desktop) => {
  vm.runInContext("location.hostname = " + JSON.stringify(host) + "; location.port = " + JSON.stringify(port) + ";", ctx);
  const n = vm.runInContext("mcpTunnelNote({ desktop: " + desktop + " })", ctx);
  return n ? { text: flat(n).join(" "), hrefs: [], cls: n.className || "" } : { text: "", hrefs: [], cls: "" };
};
out.tLocal = tunnel("localhost", "8090", true);
out.tUpper = tunnel("LOCALHOST", "8090", true);
out.t127 = tunnel("127.0.1.1", "9000", true);
out.tV6 = tunnel("[::1]", "8090", true);
out.tNoPort = tunnel("localhost", "", true);
out.tWin = tunnel("127.0.0.1", "8090", false);
out.tLan = tunnel("192.168.1.20", "8090", true);
out.tLocalDomain = tunnel("db.local", "8090", true);
out.tPublic = tunnel("console.example.com", "443", true);
out.tLookalike = tunnel("localhost.example.com", "8090", true);
out.tNip = tunnel("127.0.0.1.nip.io", "8090", true);
vm.runInContext("capsCache = { mcp: true, version: 'dev' };", ctx);
ua("Mozilla/5.0 (Macintosh)"); out.devMac = draw("mcpInstallStep(mcpInstallArm())");
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "mcppage.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type drawn struct {
		Text  string
		Hrefs []string
		Cls   string
	}
	var got map[string]drawn
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	has := func(k, want string) {
		t.Helper()
		if !strings.Contains(got[k].Text, want) {
			t.Errorf("%s: %q lacks %q", k, got[k].Text, want)
		}
	}
	lacks := func(k, not string) {
		t.Helper()
		if strings.Contains(got[k].Text, not) {
			t.Errorf("%s: %q says %q", k, got[k].Text, not)
		}
	}
	// The one-time token on screen beside a failed status read: reloading
	// would throw away the only copy, so the step must not ask for it.
	has("mintedNoStatus", "shown only once")
	lacks("mintedNoStatus", "Reload")
	has("noStatus", "Reload the page")
	// A startup token was never saved in step 1.
	has("staticStep", "Token ready")
	has("staticStep", "shown only once")
	has("staticBox", "the token DBTrail was started with")
	lacks("staticBox", "saved in step 1")
	has("managedBox", "the token you saved in step 1")
	// The capability refresh after a mint failed: the token exists, so the
	// address is there to copy beside it.
	has("mintedCapsBehind", "http://x/mcp")
	lacks("mintedCapsBehind", "appears after step 1")
	// Green only while the use is recent; an old one says when it was.
	has("recent", "Connected: last request 3m ago")
	if !strings.Contains(got["recent"].Cls, "is-on") {
		t.Errorf("recent use is not drawn as connected: %q", got["recent"].Cls)
	}
	has("old", "Last request 3 days ago")
	lacks("old", "Connected")
	if strings.Contains(got["old"].Cls, "is-on") {
		t.Errorf("a use from days ago is drawn as connected: %q", got["old"].Cls)
	}
	// Unused is said of this token: after New token, the old value's
	// requests did happen.
	has("unused", "this token has made no request since DBTrail started")
	// A static stamp left from before the static token was taken away does
	// not count.
	has("staticGone", "Not connected yet")
	if got["noToken"].Text != "" {
		t.Errorf("no token, yet the line says %q", got["noToken"].Text)
	}
	// The download matches the computer, and Windows (no installer ever
	// published) and a development build get no file link that can 404.
	has("mac", "Download for Mac")
	has("linux", "Download for Linux")
	for _, k := range []string{"windows", "devMac"} {
		for _, h := range got[k].Hrefs {
			if strings.Contains(h, "/releases/download/") {
				t.Errorf("%s links a file that does not exist: %s", k, h)
			}
		}
	}
	has("windows", "There is no Windows installer yet")
	// A page opened on this computer's own address was most likely reached
	// through a tunnel: Claude can use the address only while it is open.
	for _, k := range []string{"tLocal", "tUpper", "t127", "tV6", "tNoPort", "tWin"} {
		has(k, "This address is on this computer")
	}
	has("tLocal", "only while that tunnel is open")
	has("tLocal", "ssh -N -L 8090:localhost:8090")
	has("tLocal", "AWS-StartPortForwardingSession")
	has("tLocal", "localPortNumber=8090")
	has("t127", "-L 9000:localhost:9000")
	// No port in the address: the browser's default for the scheme, never
	// the word undefined.
	has("tNoPort", "-L 80:localhost:80")
	lacks("tNoPort", "undefined")
	// claude.ai runs on Anthropic's servers, which never reach a private
	// address, tunnel or not.
	has("tWin", "claude.ai in the browser cannot reach it")
	lacks("tLocal", "cannot reach it")
	// Not a tunnel signal: Claude Desktop reaches these directly.
	for _, k := range []string{"tLan", "tLocalDomain", "tPublic", "tLookalike", "tNip"} {
		if got[k].Text != "" {
			t.Errorf("%s: a tunnel note on a host that is not this computer: %q", k, got[k].Text)
		}
	}
	found := false
	for _, h := range got["linux"].Hrefs {
		found = found || strings.HasSuffix(h, "/releases/download/v0.108.1/dbtrail-linux-amd64.mcpb")
	}
	if !found {
		t.Errorf("linux: no direct installer link in %v", got["linux"].Hrefs)
	}
}
