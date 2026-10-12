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
// A string appended to a paragraph is text too (the router's lines are).
const whole = (n) => !n ? "" : typeof n === "string" ? n : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(whole).join(" ");
const tidy = (s) => s.replace(/\s+/g, " ").trim();
const walk = (n, f) => { if (!n || typeof n === "string" || n.nodeType === 3) return; f(n); (n.children || []).forEach((c) => walk(c, f)); };
const has = (x, c) => (" " + x.className + " ").includes(" " + c + " ");
const run = (s) => vm.runInContext(s, ctx);
// lines: every paragraph and the row with the command. red: the ones drawn
// as an error. who: the figures of Who answered as drawn (#2330), one per
// side and then the refusals. caption: the line over them. state: the
// routing word.
const read = (panel) => { const o = { lines: [], red: [], buttons: [], inputs: [], who: [], caption: [], state: [] };
  walk(panel, (x) => { if (x.tag === "p" || x.className === "cn-urlrow") o.lines.push(tidy(whole(x)));
    if (x.tag === "p" && has(x, "cn-sql-err")) o.red.push(tidy(whole(x)));
    if (has(x, "rs-side")) o.who.push(tidy(whole(x)));
    if (has(x, "rs-foot")) { const r = tidy(whole(x)).match(/\d+ refused, run by neither/); if (r) o.who.push(r[0]); }
    if (has(x, "cn-who-cap")) o.caption.push(tidy(whole(x)));
    if (has(x, "cn-route-state")) o.state.push(tidy(whole(x)));
    if (x.tag === "button") o.buttons.push(x._text); if (x.tag === "input") o.inputs.push(x.value); });
  return o; };
const find = (panel, tag, text) => { let hit = null; walk(panel, (x) => { if (!hit && x.tag === tag && (text === undefined || x._text === text)) hit = x; }); return hit; };
(async () => {
  const st = JSON.parse(process.argv[4]);
  const rt = JSON.parse(process.argv[5]);
  const out = {};
  run('currentServer = "s1"; defaultServerId = "s1";');
  const servers = [{ id: "s1", name: "shop", kind: "registry" }, { id: "s2", name: "billing", kind: "registry" }];
  const panel = run("sqlClientPanel");
  const caps = (perms) => run("capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + ";");
  caps(null);
  for (const k of Object.keys(st)) out[k] = read(panel(servers, st[k], st[k].password, null, rt.up));
  caps({ "settings:write": false });
  out.lockedOff = read(panel(servers, st.off, "", null, rt.up));
  out.lockedOn = read(panel(servers, st.later, "", null, rt.up));
  caps(null);

  // The mysql line. The host and the port are the router's to say. When it
  // listens on every address the line uses the name the page was opened
  // on, except the one name mysql would read as "use the socket file".
  const cmd = (view) => read(panel(servers, st.later, "", null, view)).lines.filter((l) => l.startsWith("mysql "));
  run('location.hostname = "LocalHost";');
  out.hostLocalhost = cmd(rt.wild);
  run('location.hostname = "db.example";');
  out.hostNamed = cmd(rt.wild);
  out.hostRouter = cmd(rt.named);
  out.hostV6 = cmd(rt.v6);

  // Whether the port's number is confirmed, per answer from the router.
  const port = (view) => { const r = read(panel(servers, st.later, "", null, view));
    const doubt = r.lines.filter((l) => l.includes("not a confirmed one"));
    return { cmd: r.lines.filter((l) => l.startsWith("mysql ")), doubt, red: doubt.filter((l) => r.red.includes(l)).length, who: r.who }; };
  out.portUp = port(rt.up);
  out.portDown = port(rt.down);
  out.portNone = port(rt.none);
  out.portGarbage = port(rt.garbage);
  out.portFailed = port(null);
  out.portNotAsked = port(undefined);

  // Who answered follows the server picked in the sidebar.
  run('currentServer = "s2";');
  out.whoOther = read(panel(servers, st.later, "", null, rt.up));
  run('currentServer = "s1";');
  out.whoNoCounts = read(panel(servers, st.later, "", null, rt.wild));

  // What the port says of itself while on: why nothing answers, an error.
  out.errorOn = read(panel(servers, Object.assign({}, st.later, { error: "the saved setting could not be read: unexpected end of JSON input" }), "", null, rt.up));

  // Pressing things. api records the call and answers what the route did.
  ctx.__calls = []; ctx.__toasts = []; ctx.__asked = []; ctx.__answer = st.created; ctx.__fail = null; ctx.__confirm = true;
  run('api = (p, o) => { __calls.push([p, o]); return __fail ? Promise.reject(new Error(__fail)) : Promise.resolve(__answer); };');
  run('toastError = (m) => { __toasts.push(m); };');
  run('confirm = (q) => { __asked.push(q); return __confirm; };');
  const settle = () => new Promise((r) => setImmediate(r));
  // A button that is not there to press is reported, not thrown: the test
  // then says which one, instead of node dying on a null.
  out.missing = [];
  const press = (panel, text) => { const b = find(panel, "button", text); if (b) b.onclick(); else out.missing.push(text); return b; };

  let p = panel(servers, st.off, "", null, rt.up);
  press(p, "Turn on");
  await settle();
  out.turnOn = { calls: ctx.__calls.splice(0), after: read(p) };
  // The SAME panel, pressed again after it was redrawn: the second answer
  // must land on it too.
  ctx.__answer = Object.assign({}, st.created, { password: "bfp_second" });
  press(p, "New password");
  await settle();
  out.secondPress = { calls: ctx.__calls.splice(0), after: read(p) };
  // Routing turned on while the new password is still on screen.
  ctx.__answer = st.routedOn;
  press(p, "Turn routing on");
  await settle();
  out.routingAfterPassword = { calls: ctx.__calls.splice(0), after: read(p) };
  ctx.__answer = st.off;
  press(p, "Turn off");
  await settle();
  out.thirdPress = { calls: ctx.__calls.splice(0), after: read(p) };
  ctx.__answer = st.created;

  p = panel(servers, st.off, "", null, rt.up);
  ctx.__fail = "the port could not be opened on 127.0.0.1:3309: address already in use";
  const on = press(p, "Turn on");
  await settle();
  out.refused = { toasts: ctx.__toasts.splice(0), disabled: !on || !!on.disabled, after: read(p), calls: ctx.__calls.splice(0).length };
  ctx.__fail = null;

  p = panel(servers, st.later, "", null, rt.up);
  ctx.__confirm = false; ctx.__asked.splice(0);
  press(p, "Turn off");
  press(p, "New password");
  press(p, "Turn routing on");
  await settle();
  out.declined = { calls: ctx.__calls.splice(0).length, asked: ctx.__asked.splice(0), after: read(p) };
  ctx.__confirm = true;
  ctx.__answer = st.off;
  press(p, "Turn off");
  await settle();
  out.turnOff = { calls: ctx.__calls.splice(0), after: read(p) };

  p = panel(servers, st.later, "", null, rt.up);
  ctx.__answer = st.created;
  press(p, "New password");
  await settle();
  out.newPassword = { calls: ctx.__calls.splice(0), after: read(p) };

  // Read routing: on (asks first), then off on the redrawn panel (does not
  // ask: turning it off sends less to MySQL, never more).
  p = panel(servers, st.later, "", null, rt.up);
  out.routingBefore = read(p);
  ctx.__asked.splice(0); ctx.__confirm = true; ctx.__answer = st.routedOn;
  press(p, "Turn routing on");
  await settle();
  out.routingOn = { calls: ctx.__calls.splice(0), asked: ctx.__asked.splice(0), after: read(p) };
  ctx.__confirm = false; ctx.__answer = st.routedOff;
  press(p, "Turn routing off");
  await settle();
  out.routingOff = { calls: ctx.__calls.splice(0), asked: ctx.__asked.splice(0), after: read(p) };
  ctx.__confirm = true;

  // A refused change of routing: said, and the button usable again.
  p = panel(servers, st.later, "", null, rt.up);
  ctx.__fail = "the saved setting could not be written";
  const rb = press(p, "Turn routing on");
  await settle();
  out.routingRefused = { toasts: ctx.__toasts.splice(0), disabled: !rb || !!rb.disabled, after: read(p), calls: ctx.__calls.splice(0).length };
  ctx.__fail = null;
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

type sqlPortDrawn struct {
	Lines, Red, Buttons, Inputs, Who, Caption, State []string
}

// TestSQLPortPanel_TurnedOnFromTheWebInterface (#2101) draws the panel from
// what the real routes answer in each state and presses its buttons.
//
// Since #2341 the router serves the port, so the panel asks for no address
// (Turn on sends no listen), takes the host and port of the mysql line from
// what the router says it listens on, always speaks of the port's own
// password, and carries the read-routing switch and who answered.
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
	if later := grab("later", "GET", "/api/flashback", ""); later["routing_on"] != false || later["enabled"] != true {
		t.Fatalf("setup: the port just turned on reports %v", later)
	}
	// What the route answers to the routing switch, each way.
	if on := grab("routedOn", "PUT", "/api/flashback", `{"routing":true}`); on["routing_on"] != true || on["enabled"] != true {
		t.Fatalf("setup: turning routing on answered %v", on)
	}
	if off := grab("routedOff", "PUT", "/api/flashback", `{"routing":false}`); off["routing_on"] != false || off["enabled"] != true {
		t.Fatalf("setup: turning routing off answered %v", off)
	}

	// A port saved as on whose address was taken at startup.
	failed, err := New(Config{Listen: "127.0.0.1:8090", FlashbackPath: s.flashback.path})
	if err != nil {
		t.Fatal(err)
	}
	failed.ManageFlashback(&fakeFlashbackControl{fail: map[string]error{"127.0.0.1:3309": errors.New("cannot bind 127.0.0.1:3309: address already in use")}})
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
	// On, and nothing answers: the process was told why when it started.
	const notice = "nothing answers on it: this install has no router service."
	noticed, err := New(Config{Listen: "127.0.0.1:8090", FlashbackPath: s.flashback.path, FlashbackNotice: notice})
	if err != nil {
		t.Fatal(err)
	}
	noticed.ManageFlashback(&fakeFlashbackControl{fail: map[string]error{}})
	if st := noticed.flashbackStatus(); !st.Enabled || st.Notice != notice {
		t.Fatalf("setup: a port that is on with a notice reports %+v", st)
	} else {
		raw, _ = json.Marshal(st)
		status["noticed"] = raw
	}

	arg, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	// What the router answers (GET /api/router), in each shape the panel
	// tells apart.
	view := func(listen string) routerViewDTO {
		return routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-11T14:31:00Z",
			Status: &RouterStatusDoc{Version: 1, Listen: listen, ReadOnly: true, CostThreshold: 10000}}
	}
	up := view("127.0.0.1:3309")
	up.Status.CountingSince = "2026-10-11T09:00:00Z"
	up.Status.Servers = map[string]RouterServerNumbers{
		"s1": {Copy: 4, MySQL: 9, Reasons: map[string]uint64{"expensive_plan": 4, "cheap_plan": 9}},
		"s2": {Copy: 70, MySQL: 80, Refused: 6, Reasons: map[string]uint64{}},
	}
	routers, err := json.Marshal(map[string]routerViewDTO{
		"up":      up,
		"wild":    view("0.0.0.0:3309"),
		"named":   view("10.1.2.3:3311"),
		"v6":      view("[::1]:13399"),
		"garbage": view("not-an-address"),
		"down":    {Configured: true, URL: "http://router:9091/status", Error: "it could not be reached: connection refused"},
		"none":    {},
	})
	if err != nil {
		t.Fatal(err)
	}
	type pressed struct {
		Calls []json.RawMessage
		Asked []string
		After sqlPortDrawn
	}
	type refusal struct {
		Toasts   []string
		Disabled bool
		After    sqlPortDrawn
		Calls    int
	}
	type portLine struct {
		Cmd, Doubt []string
		Red        int
		Who        []string
	}
	var got struct {
		Off, Created, Later, RoutedOn, RoutedOff, OffError, Startup, Noticed, LockedOff, LockedOn sqlPortDrawn
		HostLocalhost, HostNamed, HostRouter, HostV6                                              []string
		PortUp, PortDown, PortNone, PortGarbage, PortFailed, PortNotAsked                         portLine
		WhoOther, WhoNoCounts, ErrorOn, RoutingBefore                                             sqlPortDrawn
		TurnOn, TurnOff, NewPassword, SecondPress, ThirdPress                                     pressed
		RoutingAfterPassword, RoutingOn, RoutingOff                                               pressed
		Refused, RoutingRefused                                                                   refusal
		Missing                                                                                   []string
		Declined                                                                                  struct {
			Calls int
			Asked []string
			After sqlPortDrawn
		}
	}
	out := runNodeConnectArgs(t, renderHarnessJS+sqlPortPanelJS, string(arg), string(routers))
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %s: %v", out, err)
	}
	// The two sides as drawn for the server picked first (#2330).
	shopSides := []string{"Your MySQL 9 statements", "DBTrail's copy 4 statements"}
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
	red := func(what string, d sqlPortDrawn, want string) {
		t.Helper()
		if !slices.Contains(d.Red, want) {
			t.Errorf("%s: want the line %q drawn as an error; drawn as errors: %q; all lines:\n  %s", what, want, d.Red, strings.Join(d.Lines, "\n  "))
		}
	}
	routingButtons := func(d sqlPortDrawn) (n int) {
		for _, b := range d.Buttons {
			if strings.HasPrefix(b, "Turn routing ") {
				n++
			}
		}
		return n
	}

	// Off, and this session can turn it on: one button and no address to
	// type, since the router that serves the port has its own.
	t.Logf("off:\n  %s", strings.Join(got.Off.Lines, "\n  "))
	has("off", got.Off, "Off. Turn it on and your usual MySQL client can read any table as it was at a chosen moment.")
	has("off", got.Off, "Turning it on creates the port's password. It is shown once, right then.")
	lacks("off", got.Off, "The port is set when DBTrail starts")
	lacks("off", got.Off, "cannot be turned on or off")
	if !slices.Equal(got.Off.Buttons, []string{"Turn on"}) || len(got.Off.Inputs) != 0 || len(got.Off.Red) != 0 {
		t.Errorf("off: buttons %q, inputs %q, errors %q; want Turn on alone, no address field and no error", got.Off.Buttons, got.Off.Inputs, got.Off.Red)
	}
	t.Logf("off after a failed start:\n  %s", strings.Join(got.OffError.Lines, "\n  "))
	red("off after a failed start", got.OffError, "cannot bind 127.0.0.1:3309: address already in use")
	if !slices.Equal(got.OffError.Buttons, []string{"Turn on"}) || len(got.OffError.Inputs) != 0 {
		t.Errorf("off after a failed start: buttons %q, inputs %q; want Turn on and no address field", got.OffError.Buttons, got.OffError.Inputs)
	}

	// Just turned on: the password, once, with the line to copy.
	t.Logf("created:\n  %s", strings.Join(got.Created.Lines, "\n  "))
	has("created", got.Created, "Password, shown only now. Copy it and keep it somewhere safe:")
	has("created", got.Created, pw)
	has("created", got.Created, "mysql -h 127.0.0.1 -P 3309 -u shop -p")
	has("created", got.Created, "This password reads every schema on every server in the sidebar.")
	lacks("created", got.Created, "access token")
	lacks("created", got.Created, "Paste the")
	for _, b := range []string{"Turn off", "New password", "Turn routing on", "Refresh", "Copy"} {
		if !slices.Contains(got.Created.Buttons, b) {
			t.Errorf("created: buttons %q, want %q among them", got.Created.Buttons, b)
		}
	}
	if len(got.Created.Inputs) != 0 {
		t.Errorf("created: inputs %q; the panel asks for nothing to type", got.Created.Inputs)
	}

	// Later: the password is not there to show.
	has("later", got.Later, "Password: the one shown when it was created")
	has("later", got.Later, "It cannot be shown again.")
	lacks("later", got.Later, pw)
	lacks("later", got.Later, "undefined")
	if len(got.Later.Red) != 0 {
		t.Errorf("later: a healthy port with a router that answered draws errors: %q", got.Later.Red)
	}

	// Set at startup: the same lines as any port that is on, the port's own
	// password among them (the access token is no longer named as one), and
	// nothing to press that changes it.
	has("startup", got.Startup, "mysql -h 127.0.0.1 -P 3309 -u shop -p")
	has("startup", got.Startup, "Password: the one shown when it was created")
	lacks("startup", got.Startup, "access token")
	lacks("startup", got.Startup, "never shown in the web interface")
	lacks("startup", got.Startup, "Paste the")
	for _, b := range got.Startup.Buttons {
		if b == "Turn off" || b == "New password" || b == "Turn on" || strings.HasPrefix(b, "Turn routing ") {
			t.Errorf("startup: the button %q is drawn for a port the web interface cannot change", b)
		}
	}
	if !slices.Equal(got.Startup.State, []string{"Off"}) {
		t.Errorf("startup: the routing word is %q, want it said even with no button", got.Startup.State)
	}

	// A session that cannot change settings gets facts and no controls.
	has("locked, off", got.LockedOff, "Off. Someone who can change settings can turn it on here.")
	if len(got.LockedOff.Buttons) != 0 || len(got.LockedOff.Inputs) != 0 {
		t.Errorf("locked, off: buttons %q, inputs %q; want none", got.LockedOff.Buttons, got.LockedOff.Inputs)
	}
	for _, b := range got.LockedOn.Buttons {
		if b == "Turn off" || b == "New password" || strings.HasPrefix(b, "Turn routing ") {
			t.Errorf("locked, on: the button %q is drawn for a session that cannot press it", b)
		}
	}
	if !slices.Equal(got.LockedOn.State, []string{"Off"}) || !slices.Equal(got.LockedOn.Who, shopSides) {
		t.Errorf("locked, on: routing word %q, who answered %q; a session that cannot change settings still reads both", got.LockedOn.State, got.LockedOn.Who)
	}

	// The command's host and port are the router's to say.
	if len(got.HostLocalhost) != 1 || !strings.HasPrefix(got.HostLocalhost[0], "mysql -h 127.0.0.1 -P 3309 ") {
		t.Errorf("page opened on localhost: %q; mysql reads -h localhost as the socket file, so the line must say 127.0.0.1", got.HostLocalhost)
	}
	if len(got.HostNamed) != 1 || !strings.HasPrefix(got.HostNamed[0], "mysql -h db.example -P 3309 ") {
		t.Errorf("page opened on db.example: %q", got.HostNamed)
	}
	if len(got.HostRouter) != 1 || !strings.HasPrefix(got.HostRouter[0], "mysql -h 10.1.2.3 -P 3311 ") {
		t.Errorf("a router on one address: %q; want that address and port, whatever name the page was opened on", got.HostRouter)
	}
	if len(got.HostV6) != 1 || !strings.HasPrefix(got.HostV6[0], "mysql -h ::1 -P 13399 ") {
		t.Errorf("a router on an IPv6 address: %q; want it without brackets, and its port", got.HostV6)
	}

	// Whether the port's number is confirmed. Only a router that answered
	// confirms it; without one the usual number is drawn and said to be
	// that, in red when a router was configured and did not answer.
	const usual = "3309 above is the port's usual number, not a confirmed one."
	if p := got.PortUp; len(p.Doubt) != 0 || len(p.Cmd) != 1 || !strings.Contains(p.Cmd[0], " -P 3309 ") {
		t.Errorf("a router that answered: command %q, doubt %q; want its port and no doubt about it", p.Cmd, p.Doubt)
	}
	if p := got.PortDown; len(p.Doubt) != 1 || p.Doubt[0] != "The router that serves the port did not answer, so "+usual || p.Red != 1 || len(p.Cmd) != 1 || !strings.Contains(p.Cmd[0], " -P 3309 ") {
		t.Errorf("a router that did not answer: command %q, doubt %q (%d red); want port 3309 and, in red, that the router did not answer", p.Cmd, p.Doubt, p.Red)
	}
	if p := got.PortNone; len(p.Doubt) != 1 || p.Doubt[0] != "This DBTrail was not told where the router that serves the port is, so "+usual || p.Red != 0 || len(p.Cmd) != 1 || !strings.Contains(p.Cmd[0], " -P 3309 ") {
		t.Errorf("no router configured: command %q, doubt %q (%d red); want port 3309 and, not as an error, that no router was named", p.Cmd, p.Doubt, p.Red)
	}
	// The other ways of having no address, each in its own words, so the
	// line never contradicts what the rest of the panel says of the router:
	// an asking that failed and a router whose address cannot be read are
	// errors; a router that was not asked is not.
	for _, c := range []struct {
		name, why string
		red       int
		p         portLine
	}{
		{"a listen that is no address", "The router that serves the port did not say an address this page can read, so ", 1, got.PortGarbage},
		{"the asking failed", "This DBTrail could not ask the router that serves the port, so ", 1, got.PortFailed},
		{"not asked", "This DBTrail was not told where the router that serves the port is, so ", 0, got.PortNotAsked},
	} {
		if len(c.p.Doubt) != 1 || c.p.Doubt[0] != c.why+usual || c.p.Red != c.red || len(c.p.Cmd) != 1 || !strings.Contains(c.p.Cmd[0], " -P 3309 ") {
			t.Errorf("%s: command %q, doubt %q (%d red); want port 3309 and the one line %q (%d red)", c.name, c.p.Cmd, c.p.Doubt, c.p.Red, c.why+usual, c.red)
		}
	}

	// Who answered: the picked server's counts, from the router, and none
	// (never zeros) when the router gave none or did not answer.
	if !slices.Equal(got.Later.Who, shopSides) || !slices.Equal(got.Later.Caption, []string{"Who answered for shop"}) {
		t.Errorf("who answered for the picked server: %q under %q", got.Later.Who, got.Later.Caption)
	}
	if !slices.Equal(got.WhoOther.Who, []string{"Your MySQL 80 statements", "DBTrail's copy 70 statements", "6 refused, run by neither"}) || !slices.Equal(got.WhoOther.Caption, []string{"Who answered for billing"}) {
		t.Errorf("who answered for another picked server: %q under %q, want its own counts", got.WhoOther.Who, got.WhoOther.Caption)
	}
	// One fact, once: the counts that are drawn are not said again among
	// the router's lines; with nothing to draw, the sentence that says so
	// is there, once.
	whoSaid := func(d sqlPortDrawn) (out []string) {
		for _, l := range d.Lines {
			if strings.HasPrefix(l, "Who answered") && !slices.Contains(d.Caption, l) {
				out = append(out, l)
			}
		}
		return out
	}
	for name, d := range map[string]sqlPortDrawn{"the picked server": got.Later, "another picked server": got.WhoOther, "locked, on": got.LockedOn, "just created": got.Created} {
		if said := whoSaid(d); len(said) != 0 {
			t.Errorf("%s: the counts are drawn (%q) and said again: %q", name, d.Who, said)
		}
		lacks(name, d, "by MySQL,")
	}
	if said := whoSaid(got.WhoNoCounts); len(said) != 1 || !strings.Contains(said[0], "Who answered for shop: nothing to show.") || len(got.WhoNoCounts.Caption) != 0 {
		t.Errorf("a router with no counts for the server: said %q, caption %q; want the one sentence and nothing drawn", said, got.WhoNoCounts.Caption)
	}
	has("another picked server", got.WhoOther, "mysql -h 127.0.0.1 -P 3309 -u billing -p")
	for name, who := range map[string][]string{"a router with no counts for the server": got.WhoNoCounts.Who, "a router that did not answer": got.PortDown.Who,
		"no router": got.PortNone.Who, "the asking failed": got.PortFailed.Who, "not asked": got.PortNotAsked.Who} {
		if len(who) != 0 {
			t.Errorf("%s: who answered is drawn as %q; want nothing", name, who)
		}
	}
	lacks("a router with no counts for the server", got.WhoNoCounts, "0 by")
	lacks("a router with no counts for the server", got.WhoNoCounts, "0 statements")

	// On, and nothing answers: said first, in red, above the command.
	t.Logf("noticed:\n  %s", strings.Join(got.Noticed.Lines, "\n  "))
	red("noticed", got.Noticed, "The port is on and "+notice)
	if len(got.Noticed.Lines) == 0 || got.Noticed.Lines[0] != "The port is on and "+notice {
		t.Errorf("noticed: the notice must be the first line, above the command; first line %q", got.Noticed.Lines)
	}
	has("noticed", got.Noticed, "mysql -h 127.0.0.1 -P 3309 -u shop -p")
	lacks("later", got.Later, "The port is on and")
	red("an error while on", got.ErrorOn, "the saved setting could not be read: unexpected end of JSON input")
	has("an error while on", got.ErrorOn, "mysql -h 127.0.0.1 -P 3309 -u shop -p")

	// What the buttons send, and what the panel shows from the answer.
	if len(got.Missing) != 0 {
		t.Errorf("these buttons were not on the panel when the test went to press them: %q", got.Missing)
	}
	one := func(what string, calls []json.RawMessage, want string) {
		t.Helper()
		if len(calls) != 1 || string(calls[0]) != want {
			t.Errorf("%s sent %s, want exactly %s", what, calls, want)
		}
	}
	// No listen: the address is the router's, not this panel's to send.
	one("Turn on", got.TurnOn.Calls, `["/api/flashback",{"method":"PUT","body":{"enabled":true}}]`)
	has("after Turn on", got.TurnOn.After, pw)
	one("New password on a redrawn panel", got.SecondPress.Calls, `["/api/flashback/password",{"method":"POST","body":{}}]`)
	has("a redrawn panel after New password", got.SecondPress.After, "bfp_second")
	lacks("a redrawn panel after New password", got.SecondPress.After, pw)
	one("Turn routing on, on a twice-redrawn panel", got.RoutingAfterPassword.Calls, `["/api/flashback",{"method":"PUT","body":{"routing":true}}]`)
	if !slices.Equal(got.RoutingAfterPassword.After.State, []string{"On"}) {
		t.Errorf("a twice-redrawn panel after Turn routing on: routing word %q, want On", got.RoutingAfterPassword.After.State)
	}
	// The answer to the routing switch carries no password, so the panel
	// keeps the one on screen ("shown only now"): pressing Turn routing on
	// before copying it must not take away its only copy.
	if !slices.ContainsFunc(got.RoutingAfterPassword.After.Lines, func(l string) bool { return strings.Contains(l, "bfp_second") }) {
		t.Errorf("the password shown only now is gone after the routing switch was pressed:\n  %s", strings.Join(got.RoutingAfterPassword.After.Lines, "\n  "))
	}
	if !slices.Equal(got.ThirdPress.After.Buttons, []string{"Turn on"}) {
		t.Errorf("a three-times-redrawn panel after Turn off: buttons %q, want Turn on", got.ThirdPress.After.Buttons)
	}
	one("Turn off", got.TurnOff.Calls, `["/api/flashback",{"method":"PUT","body":{"enabled":false}}]`)
	if !slices.Equal(got.TurnOff.After.Buttons, []string{"Turn on"}) {
		t.Errorf("after Turn off: buttons %q, want Turn on", got.TurnOff.After.Buttons)
	}
	one("New password", got.NewPassword.Calls, `["/api/flashback/password",{"method":"POST","body":{}}]`)
	has("after New password", got.NewPassword.After, "Password, shown only now.")
	// Each of the three is asked about, and a no sends nothing and changes
	// nothing on screen.
	if got.Declined.Calls != 0 || len(got.Declined.Asked) != 3 || !slices.Equal(got.Declined.After.State, []string{"Off"}) || !slices.Contains(got.Declined.After.Buttons, "Turn routing on") {
		t.Errorf("declined confirmations: %d request(s) sent, %d question(s) asked (%q), routing word %q; want none sent, three asked, and the panel as it was",
			got.Declined.Calls, len(got.Declined.Asked), got.Declined.Asked, got.Declined.After.State)
	}
	if got.Refused.Calls != 1 || got.Refused.Disabled || len(got.Refused.Toasts) != 1 ||
		!strings.Contains(got.Refused.Toasts[0], "address already in use") || !slices.Equal(got.Refused.After.Buttons, []string{"Turn on"}) {
		t.Errorf("a refused Turn on: %+v; want the reason shown and the button usable again", got.Refused)
	}

	// The routing switch (#2341): the word, the button, what it sends, and
	// the panel drawn again from the answer.
	if b := got.RoutingBefore; !slices.Equal(b.State, []string{"Off"}) || routingButtons(b) != 1 || !slices.Contains(b.Buttons, "Turn routing on") {
		t.Errorf("routing off: word %q, buttons %q; want Off and the one button Turn routing on", b.State, b.Buttons)
	}
	has("routing off", got.RoutingBefore, "Turn it on and an application can use this port in place of its MySQL.")
	one("Turn routing on", got.RoutingOn.Calls, `["/api/flashback",{"method":"PUT","body":{"routing":true}}]`)
	if len(got.RoutingOn.Asked) != 1 || !strings.HasPrefix(got.RoutingOn.Asked[0], "Turn read routing on?") || !strings.Contains(got.RoutingOn.Asked[0], "forwarding account") {
		t.Errorf("Turn routing on asked %q; want one question that says statements then run on each server's MySQL with its forwarding account", got.RoutingOn.Asked)
	}
	if a := got.RoutingOn.After; !slices.Equal(a.State, []string{"On"}) || routingButtons(a) != 1 || !slices.Contains(a.Buttons, "Turn routing off") {
		t.Errorf("after Turn routing on: word %q, buttons %q; want On and the one button Turn routing off", a.State, a.Buttons)
	}
	has("after Turn routing on", got.RoutingOn.After, "A statement that would change the server is refused.")
	lacks("after Turn routing on", got.RoutingOn.After, "Turn it on and an application can use this port")
	// The redraw keeps what the router said: the command and who answered.
	has("after Turn routing on", got.RoutingOn.After, "mysql -h 127.0.0.1 -P 3309 -u shop -p")
	if !slices.Equal(got.RoutingOn.After.Who, shopSides) {
		t.Errorf("after Turn routing on: who answered %q, want the router's counts still drawn", got.RoutingOn.After.Who)
	}
	// Off again, on the redrawn panel, with confirmations declined: it is
	// not asked about, so it is sent.
	one("Turn routing off", got.RoutingOff.Calls, `["/api/flashback",{"method":"PUT","body":{"routing":false}}]`)
	if len(got.RoutingOff.Asked) != 0 {
		t.Errorf("Turn routing off asked %q; turning it off is not confirmed", got.RoutingOff.Asked)
	}
	if a := got.RoutingOff.After; !slices.Equal(a.State, []string{"Off"}) || routingButtons(a) != 1 || !slices.Contains(a.Buttons, "Turn routing on") {
		t.Errorf("after Turn routing off: word %q, buttons %q; want Off and the one button Turn routing on", a.State, a.Buttons)
	}
	if r := got.RoutingRefused; r.Calls != 1 || r.Disabled || len(r.Toasts) != 1 || !strings.Contains(r.Toasts[0], "MySQL port: the saved setting could not be written") ||
		!slices.Equal(r.After.State, []string{"Off"}) || !slices.Contains(r.After.Buttons, "Turn routing on") {
		t.Errorf("a refused Turn routing on: %+v; want the reason shown, the word still Off and the button usable again", r)
	}
	// The statuses the route itself answered to the switch draw the same.
	if !slices.Equal(got.RoutedOn.State, []string{"On"}) || !slices.Equal(got.RoutedOff.State, []string{"Off"}) {
		t.Errorf("the route's own answers: routing on draws %q, routing off draws %q", got.RoutedOn.State, got.RoutedOff.State)
	}
}
