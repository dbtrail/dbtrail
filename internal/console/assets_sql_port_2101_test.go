package console

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
)

// sqlPortPanelJS draws the Connect panel's MySQL-port section from the
// answers the real routes gave, presses its buttons against a recording api,
// and reports what a person would read and what was sent.
const sqlPortPanelJS = `
const whole = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(whole).join(" ");
const tidy = (s) => s.replace(/\s+/g, " ").trim();
const walk = (n, f) => { if (!n || n.nodeType === 3) return; f(n); (n.children || []).forEach((c) => walk(c, f)); };
const run = (s) => vm.runInContext(s, ctx);
const read = (panel) => { const o = { lines: [], buttons: [], inputs: [] };
  walk(panel, (x) => { if (x.tag === "p" || x.className === "cn-urlrow") o.lines.push(tidy(whole(x)));
    if (x.tag === "button") o.buttons.push(x._text); if (x.tag === "input") o.inputs.push(x.value); });
  return o; };
const find = (panel, tag, text) => { let hit = null; walk(panel, (x) => { if (!hit && x.tag === tag && (text === undefined || x._text === text)) hit = x; }); return hit; };
(async () => {
  const st = JSON.parse(process.argv[4]);
  const out = {};
  run('currentServer = "s1"; defaultServerId = "s1";');
  const servers = [{ id: "s1", name: "shop", kind: "registry" }];
  const panel = run("sqlClientPanel");
  const caps = (perms) => run("capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + ";");
  caps(null);
  for (const k of Object.keys(st)) out[k] = read(panel(servers, st[k], st[k].password));
  caps({ "settings:write": false });
  out.lockedOff = read(panel(servers, st.off));
  out.lockedOn = read(panel(servers, st.later));
  caps(null);

  // The mysql line on a wildcard bind uses the name the page was opened on,
  // except the one name mysql would read as "use the socket file".
  run('location.hostname = "LocalHost";');
  out.hostLocalhost = read(panel(servers, st.wildcard)).lines.filter((l) => l.startsWith("mysql "));
  run('location.hostname = "db.example";');
  out.hostNamed = read(panel(servers, st.wildcard)).lines.filter((l) => l.startsWith("mysql "));

  // Pressing things. api records the call and answers what the route did.
  ctx.__calls = []; ctx.__toasts = []; ctx.__answer = st.created; ctx.__fail = null; ctx.__confirm = true;
  run('api = (p, o) => { __calls.push([p, o]); return __fail ? Promise.reject(new Error(__fail)) : Promise.resolve(__answer); };');
  run('toastError = (m) => { __toasts.push(m); };');
  run('confirm = () => __confirm;');
  const settle = () => new Promise((r) => setImmediate(r));

  let p = panel(servers, st.off);
  find(p, "input").value = " 10.0.0.5:3311";
  find(p, "button", "Turn on").onclick();
  await settle();
  out.turnOn = { calls: ctx.__calls.splice(0), after: read(p) };
  // The SAME panel, pressed again after it was redrawn: the second answer
  // must land on it too.
  ctx.__answer = Object.assign({}, st.created, { password: "bfp_second" });
  find(p, "button", "New password").onclick();
  await settle();
  out.secondPress = { calls: ctx.__calls.splice(0), after: read(p) };
  ctx.__answer = st.off;
  find(p, "button", "Turn off").onclick();
  await settle();
  out.thirdPress = { calls: ctx.__calls.splice(0), after: read(p) };
  ctx.__answer = st.created;

  p = panel(servers, st.off);
  ctx.__fail = "the port could not be opened on 127.0.0.1:3309: address already in use";
  const on = find(p, "button", "Turn on");
  on.onclick();
  await settle();
  out.refused = { toasts: ctx.__toasts.splice(0), disabled: !!on.disabled, after: read(p), calls: ctx.__calls.splice(0).length };
  ctx.__fail = null;

  p = panel(servers, st.later);
  ctx.__confirm = false;
  find(p, "button", "Turn off").onclick();
  find(p, "button", "New password").onclick();
  await settle();
  out.declined = ctx.__calls.splice(0).length;
  ctx.__confirm = true;
  ctx.__answer = st.off;
  find(p, "button", "Turn off").onclick();
  await settle();
  out.turnOff = { calls: ctx.__calls.splice(0), after: read(p) };

  p = panel(servers, st.later);
  ctx.__answer = st.created;
  find(p, "button", "New password").onclick();
  await settle();
  out.newPassword = { calls: ctx.__calls.splice(0), after: read(p) };
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

type sqlPortDrawn struct {
	Lines, Buttons, Inputs []string
}

// TestSQLPortPanel_TurnedOnFromTheWebInterface (#2101) draws the panel from
// what the real routes answer in each state and presses its buttons.
func TestSQLPortPanel_TurnedOnFromTheWebInterface(t *testing.T) {
	s, ctl, _ := newManagedFlashback(t, Config{})
	status := map[string]json.RawMessage{}
	grab := func(name, method, path, body string) map[string]any {
		t.Helper()
		code, got, raw := doFlashback(t, s, method, path, body)
		if code != 200 {
			t.Fatalf("%s: %s %s = %d %s", name, method, path, code, raw)
		}
		status[name] = json.RawMessage(raw)
		return got
	}
	grab("off", "GET", "/api/flashback", "")
	created := grab("created", "PUT", "/api/flashback", `{"enabled":true}`)
	pw, _ := created["password"].(string)
	if pw == "" {
		t.Fatal("setup: turning the port on returned no password")
	}
	grab("later", "GET", "/api/flashback", "")
	grab("wildcard", "PUT", "/api/flashback", `{"enabled":true,"listen":"0.0.0.0:3309"}`)

	// A port saved as on whose address was taken at startup.
	failed, err := New(Config{Listen: "127.0.0.1:8090", FlashbackPath: s.flashback.path})
	if err != nil {
		t.Fatal(err)
	}
	failed.ManageFlashback(&fakeFlashbackControl{fail: map[string]error{"0.0.0.0:3309": errors.New("cannot bind 0.0.0.0:3309: address already in use")}})
	raw, _ := json.Marshal(failed.flashbackStatus())
	status["offError"] = raw
	// An address given at startup: shown, never switched.
	fixed, err := New(Config{Listen: "127.0.0.1:8090", Token: fbTok, FlashbackListen: "127.0.0.1:3308", FlashbackPath: s.flashback.path})
	if err != nil {
		t.Fatal(err)
	}
	fixed.ManageFlashback(ctl)
	raw, _ = json.Marshal(fixed.flashbackStatus())
	status["startup"] = raw

	arg, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Off, Created, Later, Wildcard, OffError, Startup, LockedOff, LockedOn sqlPortDrawn
		HostLocalhost, HostNamed                                              []string
		TurnOn, TurnOff, NewPassword, SecondPress, ThirdPress                 struct {
			Calls []json.RawMessage
			After sqlPortDrawn
		}
		Refused struct {
			Toasts   []string
			Disabled bool
			After    sqlPortDrawn
			Calls    int
		}
		Declined int
	}
	out := runNodeConnectArgs(t, renderHarnessJS+sqlPortPanelJS, string(arg))
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %s: %v", out, err)
	}
	has := func(what string, d sqlPortDrawn, want string) {
		t.Helper()
		for _, l := range d.Lines {
			if strings.Contains(l, want) {
				return
			}
		}
		t.Errorf("%s: want a line containing %q in:\n  %s", what, want, strings.Join(d.Lines, "\n  "))
	}
	lacks := func(what string, d sqlPortDrawn, bad string) {
		t.Helper()
		for _, l := range d.Lines {
			if strings.Contains(l, bad) {
				t.Errorf("%s: the line %q must not be drawn", what, l)
			}
		}
	}

	// Off, and this session can turn it on: an address and one button.
	t.Logf("off:\n  %s", strings.Join(got.Off.Lines, "\n  "))
	has("off", got.Off, "Off. Turn it on and your usual MySQL client can read any table as it was at a chosen moment.")
	lacks("off", got.Off, "The port is set when DBTrail starts")
	if !slices.Equal(got.Off.Buttons, []string{"Turn on"}) || !slices.Equal(got.Off.Inputs, []string{"127.0.0.1:3309"}) {
		t.Errorf("off: buttons %q, inputs %q; want Turn on and the suggested address", got.Off.Buttons, got.Off.Inputs)
	}
	has("off after a failed start", got.OffError, "It was on and did not start: cannot bind 0.0.0.0:3309: address already in use")
	if !slices.Equal(got.OffError.Inputs, []string{"0.0.0.0:3309"}) {
		t.Errorf("off after a failed start: inputs %q, want the saved address", got.OffError.Inputs)
	}

	// Just turned on: the password, once, with the line to copy.
	t.Logf("created:\n  %s", strings.Join(got.Created.Lines, "\n  "))
	has("created", got.Created, "Password, shown only now. Copy it and keep it somewhere safe:")
	has("created", got.Created, pw)
	has("created", got.Created, "mysql -h 127.0.0.1 -P 3309 -u shop -p")
	has("created", got.Created, "Paste the password at the password prompt.")
	lacks("created", got.Created, "access token")
	if !slices.Contains(got.Created.Buttons, "Turn off") || !slices.Contains(got.Created.Buttons, "New password") {
		t.Errorf("created: buttons %q, want Turn off and New password", got.Created.Buttons)
	}

	// Later: the password is not there to show.
	has("later", got.Later, "Password: the one shown when it was created")
	has("later", got.Later, "It cannot be shown again.")
	lacks("later", got.Later, pw)
	lacks("later", got.Later, "undefined")

	// Set at startup: as before, and no switch.
	has("startup", got.Startup, "Password: the access token")
	has("startup", got.Startup, "Paste the token at the password prompt.")
	for _, b := range got.Startup.Buttons {
		if b == "Turn off" || b == "New password" || b == "Turn on" {
			t.Errorf("startup: the button %q is drawn for a port the web interface cannot change", b)
		}
	}

	// A session that cannot change settings gets facts and no controls.
	has("locked, off", got.LockedOff, "Off. Someone who can change settings can turn it on here.")
	if len(got.LockedOff.Buttons) != 0 || len(got.LockedOff.Inputs) != 0 {
		t.Errorf("locked, off: buttons %q, inputs %q; want none", got.LockedOff.Buttons, got.LockedOff.Inputs)
	}
	for _, b := range got.LockedOn.Buttons {
		if b == "Turn off" || b == "New password" {
			t.Errorf("locked, on: the button %q is drawn for a session that cannot press it", b)
		}
	}

	if len(got.HostLocalhost) != 1 || !strings.HasPrefix(got.HostLocalhost[0], "mysql -h 127.0.0.1 -P 3309 ") {
		t.Errorf("page opened on localhost: %q; mysql reads -h localhost as the socket file, so the line must say 127.0.0.1", got.HostLocalhost)
	}
	if len(got.HostNamed) != 1 || !strings.HasPrefix(got.HostNamed[0], "mysql -h db.example -P 3309 ") {
		t.Errorf("page opened on db.example: %q", got.HostNamed)
	}

	// What the buttons send, and what the panel shows from the answer.
	one := func(what string, calls []json.RawMessage, want string) {
		t.Helper()
		if len(calls) != 1 || string(calls[0]) != want {
			t.Errorf("%s sent %s, want exactly %s", what, calls, want)
		}
	}
	one("Turn on", got.TurnOn.Calls, `["/api/flashback",{"method":"PUT","body":{"enabled":true,"listen":" 10.0.0.5:3311"}}]`)
	has("after Turn on", got.TurnOn.After, pw)
	one("New password on a redrawn panel", got.SecondPress.Calls, `["/api/flashback/password",{"method":"POST","body":{}}]`)
	has("a redrawn panel after New password", got.SecondPress.After, "bfp_second")
	lacks("a redrawn panel after New password", got.SecondPress.After, pw)
	if !slices.Equal(got.ThirdPress.After.Buttons, []string{"Turn on"}) {
		t.Errorf("a twice-redrawn panel after Turn off: buttons %q, want Turn on", got.ThirdPress.After.Buttons)
	}
	one("Turn off", got.TurnOff.Calls, `["/api/flashback",{"method":"PUT","body":{"enabled":false}}]`)
	if !slices.Equal(got.TurnOff.After.Buttons, []string{"Turn on"}) {
		t.Errorf("after Turn off: buttons %q, want Turn on", got.TurnOff.After.Buttons)
	}
	one("New password", got.NewPassword.Calls, `["/api/flashback/password",{"method":"POST","body":{}}]`)
	has("after New password", got.NewPassword.After, "Password, shown only now.")
	if got.Declined != 0 {
		t.Errorf("a declined confirmation sent %d request(s)", got.Declined)
	}
	if got.Refused.Calls != 1 || got.Refused.Disabled || len(got.Refused.Toasts) != 1 ||
		!strings.Contains(got.Refused.Toasts[0], "address already in use") || !slices.Equal(got.Refused.After.Buttons, []string{"Turn on"}) {
		t.Errorf("a refused Turn on: %+v; want the reason shown and the button usable again", got.Refused)
	}
}
