package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// proxyLoginsJS draws the MySQL clients page's "A proxy in front" section
// from the answers the real routes gave, presses its buttons against a
// recording api on ONE live panel, and reports what a person would read and
// what was sent.
const proxyLoginsJS = `
const whole = (n) => !n ? "" : typeof n === "string" ? n : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(whole).join(" ");
const tidy = (s) => s.replace(/\s+/g, " ").trim();
const walk = (n, f) => { if (!n || typeof n === "string" || n.nodeType === 3) return; f(n); (n.children || []).forEach((c) => walk(c, f)); };
const has = (x, c) => (" " + x.className + " ").includes(" " + c + " ");
const run = (s) => vm.runInContext(s, ctx);
const card = (wrap) => { let hit = null; walk(wrap, (x) => { if (!hit && has(x, "cn-proxyp")) hit = x; }); return hit; };
const read = (wrap) => { const c = card(wrap); const o = { text: c ? tidy(whole(c)) : "", red: [], buttons: [], inputs: [] };
  walk(c, (x) => { if (x.tag === "p" && has(x, "cn-sql-err")) o.red.push(tidy(whole(x)));
    if (x.tag === "button") o.buttons.push(x._text); if (x.tag === "input") o.inputs.push(x.attrs["aria-label"] + "=" + x.value); });
  return o; };
const find = (wrap, tag, pred) => { let hit = null; walk(card(wrap), (x) => { if (!hit && x.tag === tag && pred(x)) hit = x; }); return hit; };
(async () => {
  const d = JSON.parse(process.argv[4]);
  const out = {};
  const caps = (perms) => run("capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + ";");
  caps(null);
  const panel = run("sqlClientPanel");
  const draw = (servers, fb) => panel(servers, fb, "", null, d.router, true);
  run('currentServer = "' + d.prodID + '"; defaultServerId = "' + d.prodID + '";');
  out.off = read(draw(d.servers, d.off));
  out.none = read(draw(d.servers, d.on));
  out.withLogins = read(draw(d.serversWithLogin, d.withMonitor));
  out.monitorError = read(draw(d.servers, Object.assign({}, d.withMonitor, { monitor_error: "it is the name of server \"prod\"" })));
  run('currentServer = "default";');
  out.boot = read(draw(d.servers.concat([{ id: "default", name: "default", kind: "boot" }]), d.on));
  run('currentServer = "' + d.prodID + '";');
  caps({ "settings:write": false });
  out.locked = read(draw(d.serversWithLogin, d.withMonitor));
  caps(null);
  out.startup = read(draw(d.servers, d.startup));
  out.noMonitorPassword = read(draw(d.servers, Object.assign({}, d.withMonitor, { has_monitor_password: false })));
  out.listUnread = read(draw([], d.on));
  out.newerFile = read(draw(d.servers, Object.assign({}, d.withMonitor, { can_manage: false, source: "saved" })));
  caps({ "settings:write": false });
  run('currentServer = "default";');
  out.lockedNothing = read(draw(d.servers.concat([{ id: "default", name: "default", kind: "boot" }]), d.startup));
  run('currentServer = "' + d.prodID + '";');
  caps(null);

  ctx.__calls = []; ctx.__toasts = []; ctx.__asked = []; ctx.__answer = null; ctx.__fail = null;
  run('api = (p, o) => { if (String(p).endsWith("/route-grants") || p === "/api/router") return Promise.resolve({}); __calls.push([p, o]); return __fail ? Promise.reject(new Error(__fail)) : Promise.resolve(__answer); };');
  run('toastError = (m) => { __toasts.push(m); };');
  run('confirm = (q) => { __asked.push(q); return true; };');
  const settle = () => new Promise((r) => setImmediate(r));
  const fill = (wrap, label, v) => { const i = find(wrap, "input", (x) => x.attrs["aria-label"] === label); if (i) i.value = v; return !!i; };
  const press = (wrap, text) => { const b = find(wrap, "button", (x) => x._text === text); if (b) b.onclick(); return !!b; };
  const servers = JSON.parse(JSON.stringify(d.servers));
  const live = draw(servers, d.on);
  const step = async (answer, f) => { ctx.__answer = answer; const pressed = f(); await settle(); await settle();
    return { pressed, calls: ctx.__calls.splice(0), toasts: ctx.__toasts.splice(0), asked: ctx.__asked.splice(0), after: read(live) }; };
  out.emptySave = await step(null, () => press(live, "Save application login"));
  out.appSave = await step(d.loginAnswer, () => fill(live, "Application login username", " app1 ") && fill(live, "Application login password", "pw1") && press(live, "Save application login"));
  out.monSave = await step(d.withMonitor, () => fill(live, "Monitor login username", "monitor") && fill(live, "Monitor login password", "mon-pw") && press(live, "Save monitor login"));
  out.appRemove = await step(d.removedAnswer, () => press(live, "Remove application login"));
  out.monRemove = await step(d.on, () => press(live, "Remove monitor login"));
  out.emptyMonitor = await step(null, () => fill(live, "Monitor login username", "") && press(live, "Save monitor login"));
  ctx.__fail = "409: \"monitor\" is the name of a server";
  out.refused = await step(null, () => fill(live, "Monitor login username", "prod") && fill(live, "Monitor login password", "x") && press(live, "Save monitor login"));
  out.refusedEnabled = !find(live, "button", (x) => x._text === "Save monitor login").disabled;
  ctx.__fail = null;
  // A monitor login on the live panel, so it has a Remove to press below.
  out.monSaveAgain = await step(d.withMonitor, () => fill(live, "Monitor login username", "monitor") && fill(live, "Monitor login password", "mon-pw") && press(live, "Save monitor login"));
  run('redrawSQLPanel = () => { throw new Error("draw failed"); };');
  out.drawFailed = await step(d.withMonitor, () => fill(live, "Monitor login username", "monitor") && fill(live, "Monitor login password", "m") && press(live, "Save monitor login"));
  out.drawFailedEnabled = !find(live, "button", (x) => x._text === "Save monitor login").disabled;
  out.removeDrawFailed = await step(d.on, () => press(live, "Remove monitor login"));
  console.log(JSON.stringify(out));
})();
`

type proxyDrawn struct {
	Text    string
	Red     []string
	Buttons []string
	Inputs  []string
}

type proxyPressed struct {
	Pressed bool
	Calls   []json.RawMessage
	Toasts  []string
	Asked   []string
	After   proxyDrawn
}

// The section with the two logins a proxy needs, drawn from the real routes'
// answers and pressed on one live panel.
func TestSQLPortPanel_proxyLogins(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	s, _, _ := newManagedFlashback(t, Config{Registry: reg})
	prod := addLoginTestServer(t, reg, "prod")
	data := map[string]any{"prodID": prod.ID}
	grab := func(name, method, path, body string) {
		t.Helper()
		code, _, raw := doFlashback(t, s, method, path, body)
		if code != 200 {
			t.Fatalf("%s: %s %s = %d %s", name, method, path, code, raw)
		}
		data[name] = json.RawMessage(raw)
	}
	servers := func(name string) {
		t.Helper()
		code, _, raw := doFlashback(t, s, "GET", "/api/servers", "")
		if code != 200 {
			t.Fatalf("servers = %d %s", code, raw)
		}
		var list struct {
			Servers json.RawMessage `json:"servers"`
		}
		if err := json.Unmarshal([]byte(raw), &list); err != nil {
			t.Fatal(err)
		}
		data[name] = list.Servers
	}
	grab("off", "GET", "/api/flashback", "")
	grab("on", "PUT", "/api/flashback", `{"enabled":true}`)
	servers("servers")
	grab("loginAnswer", "PUT", "/api/servers/"+prod.ID+"/port-login", `{"user":"app1","password":"pw1"}`)
	servers("serversWithLogin")
	grab("withMonitor", "PUT", "/api/flashback/monitor-login", `{"user":"monitor","password":"mon-pw"}`)
	grab("removedAnswer", "DELETE", "/api/servers/"+prod.ID+"/port-login", "")
	// A port whose address was set at startup: shown, not managed here.
	fixed, err := New(Config{Listen: "127.0.0.1:8090", Token: fbTok, FlashbackListen: "127.0.0.1:3308", FlashbackPath: s.flashback.path, Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	fixed.ManageFlashback(&fakeFlashbackControl{fail: map[string]error{}})
	if st := fixed.flashbackStatus(); !st.Enabled || st.CanManage || st.Source != "startup" {
		t.Fatalf("setup: a startup port reports %+v", st)
	} else {
		raw, _ := json.Marshal(st)
		data["startup"] = json.RawMessage(raw)
	}
	data["router"] = routerViewDTO{Configured: true, Status: &RouterStatusDoc{Version: 1, Listen: "127.0.0.1:3309", ReadOnly: true}}
	arg, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Off, None, WithLogins, MonitorError, Boot, Locked, Startup, NoMonitorPassword, ListUnread proxyDrawn
		NewerFile, LockedNothing                                                                  proxyDrawn
		RemoveDrawFailed                                                                          proxyPressed
		EmptySave, AppSave, MonSave, AppRemove, MonRemove, EmptyMonitor, Refused, DrawFailed      proxyPressed
		DrawFailedEnabled                                                                         bool
		RefusedEnabled                                                                            bool
	}
	if err := json.Unmarshal(runNodeConnectArgs(t, renderHarnessJS+proxyLoginsJS, string(arg)), &got); err != nil {
		t.Fatal(err)
	}
	contains := func(what, text string, want ...string) {
		t.Helper()
		for _, w := range want {
			if !strings.Contains(text, w) {
				t.Errorf("%s: %q is missing %q", what, text, w)
			}
		}
	}
	if !strings.Contains(got.Off.Text, "Available once the MySQL port above is on.") || len(got.Off.Inputs) != 0 {
		t.Errorf("port off: %+v", got.Off)
	}
	contains("no logins", got.None.Text, "Application login None for prod", "Monitor login None the proxy's checks are refused")
	if len(got.None.Inputs) != 4 || strings.Contains(strings.Join(got.None.Buttons, ","), "Remove") {
		t.Errorf("no logins: inputs %v buttons %v", got.None.Inputs, got.None.Buttons)
	}
	contains("with logins", got.WithLogins.Text, "Application login app1 for prod", "Monitor login monitor answers the proxy's checks on this port")
	contains("with logins", strings.Join(got.WithLogins.Buttons, ","), "Remove application login", "Remove monitor login")
	contains("with logins", strings.Join(got.WithLogins.Inputs, ","), "Application login username=app1", "Application login password=", "Monitor login username=monitor")
	for _, d := range []proxyDrawn{got.WithLogins, got.None, got.Locked} {
		if strings.Contains(d.Text, "pw1") || strings.Contains(d.Text, "mon-pw") || strings.Contains(strings.Join(d.Inputs, ","), "pw1") {
			t.Errorf("a password is on screen: %+v", d)
		}
	}
	if len(got.MonitorError.Red) != 1 || !strings.Contains(got.MonitorError.Red[0], `it is the name of server "prod"`) {
		t.Errorf("monitor error: %+v", got.MonitorError.Red)
	}
	contains("boot server", got.Boot.Text, "the command-line server cannot have one")
	if strings.Contains(strings.Join(got.Boot.Buttons, ","), "Save application login") {
		t.Errorf("the command-line server offers an application login form: %v", got.Boot.Buttons)
	}
	if len(got.Locked.Inputs) != 0 || len(got.Locked.Buttons) != 0 {
		t.Errorf("without settings:write: inputs %v buttons %v", got.Locked.Inputs, got.Locked.Buttons)
	}
	contains("locked", got.Locked.Text, "Someone who can change settings can save them here.", "Application login app1")

	if !got.EmptySave.Pressed || len(got.EmptySave.Calls) != 0 {
		t.Errorf("an empty Save sent %v", got.EmptySave.Calls)
	}
	call := func(what string, p proxyPressed, want string) {
		t.Helper()
		if !p.Pressed || len(p.Calls) != 1 || string(p.Calls[0]) != want {
			t.Errorf("%s: pressed %v, calls %s, want [%s]; toasts %v", what, p.Pressed, p.Calls, want, p.Toasts)
		}
	}
	call("save application login", got.AppSave, `["/api/servers/`+prod.ID+`/port-login",{"method":"PUT","body":{"user":"app1","password":"pw1"}}]`)
	contains("after saving the application login", got.AppSave.After.Text, "Application login app1 for prod")
	call("save monitor login", got.MonSave, `["/api/flashback/monitor-login",{"method":"PUT","body":{"user":"monitor","password":"mon-pw"}}]`)
	contains("after saving the monitor login", got.MonSave.After.Text, "Application login app1", "Monitor login monitor")
	call("remove application login", got.AppRemove, `["/api/servers/`+prod.ID+`/port-login",{"method":"DELETE"}]`)
	if len(got.AppRemove.Asked) != 1 {
		t.Errorf("remove application login asked %v", got.AppRemove.Asked)
	}
	contains("after removing the application login", got.AppRemove.After.Text, "Application login None for prod", "Monitor login monitor")
	call("remove monitor login", got.MonRemove, `["/api/flashback/monitor-login",{"method":"DELETE"}]`)
	contains("after removing the monitor login", got.MonRemove.After.Text, "Monitor login None")
	contains("startup port", got.Startup.Text, "Monitor login Not here not available on this port",
		"The monitor login is not available when the port's address is set where DBTrail is started.")
	if strings.Contains(strings.Join(got.Startup.Buttons, ","), "Save monitor login") || !strings.Contains(strings.Join(got.Startup.Buttons, ","), "Save application login") {
		t.Errorf("startup port: buttons %v, want the application login only", got.Startup.Buttons)
	}
	contains("monitor login with no password", got.NoMonitorPassword.Text, "no password saved: the port refuses it")
	contains("server list not read", got.ListUnread.Text, "Application login Unknown the server picked is not in the server list")
	if strings.Contains(got.ListUnread.Text, "no server picked") {
		t.Errorf("server list not read says no server is picked: %q", got.ListUnread.Text)
	}
	contains("newer settings file", got.NewerFile.Text, "Monitor login monitor answers the proxy's checks on this port",
		"the port's saved setting was written by a newer version")
	if strings.Contains(strings.Join(got.NewerFile.Buttons, ","), "monitor login") {
		t.Errorf("newer settings file: a monitor form is offered: %v", got.NewerFile.Buttons)
	}
	if strings.Contains(got.LockedNothing.Text, "Someone who can change settings") {
		t.Errorf("nothing to save, yet the card says someone can save: %q", got.LockedNothing.Text)
	}
	if !got.DrawFailedEnabled {
		t.Error("a save whose drawing failed left the buttons disabled")
	}
	if !strings.Contains(got.RemoveDrawFailed.After.Text, "Removed, but the page could not be drawn again") {
		t.Errorf("a removal that went through and a drawing that failed: %q", got.RemoveDrawFailed.After.Text)
	}
	if len(got.EmptyMonitor.Calls) != 0 || !strings.Contains(got.EmptyMonitor.After.Text, "Both the username and the password are needed.") {
		t.Errorf("an empty monitor save: calls %v, text %q", got.EmptyMonitor.Calls, got.EmptyMonitor.After.Text)
	}
	if strings.Contains(got.Refused.After.Text, "Both the username and the password are needed.") || !strings.Contains(got.Refused.After.Text, "is the name of a server") {
		t.Errorf("a refused save: the form says %q", got.Refused.After.Text)
	}
	if !got.RefusedEnabled {
		t.Error("a refused save left the button disabled")
	}
	if len(got.DrawFailed.Toasts) != 0 || !strings.Contains(got.DrawFailed.After.Text, "Saved, but the page could not be drawn again: reload it.") {
		t.Errorf("a save that went through and a drawing that failed: toasts %v, text %q", got.DrawFailed.Toasts, got.DrawFailed.After.Text)
	}
	if len(got.Refused.Toasts) != 1 || !strings.Contains(got.Refused.Toasts[0], "is the name of a server") {
		t.Errorf("a refused save: toasts %v", got.Refused.Toasts)
	}
	if !strings.Contains(strings.Join(got.Refused.After.Inputs, ","), "Monitor login username=prod") {
		t.Errorf("a refused save emptied the form: %v", got.Refused.After.Inputs)
	}
}
