package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestServerFormCaptureAccount runs the real server form and the real
// saveServer (#3054) in each of its four modes, a new and an edited
// PostgreSQL and a new and an edited MySQL, and reads every request the save
// makes. The box that lets the PostgreSQL port forward with the user DBTrail
// connects with is a PostgreSQL's alone, is off until somebody ticks it, and
// is sent only when it differs from what the server had: as PUT
// /api/servers/{id}/route with capture_account alone, the request `server
// add` sends from a terminal. Nothing about it ever travels for a MySQL.
func TestServerFormCaptureAccount(t *testing.T) {
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
	harness, _, ok := strings.Cut(formHarnessJS, "(async () => {")
	if !ok {
		t.Fatal("formHarnessJS no longer holds its scenario marker")
	}
	script := renderHarnessJS + harness + `
ctx.__said = [];
vm.runInContext("toast = (m) => { __said.push('toast: ' + m); }; toastError = (m) => { __said.push('stays: ' + m); }; formMsg = (m, bad) => { if (m && bad) __said.push('form: ' + m); };", ctx);
ctx.__during = null; ctx.__refuse = false;
// run saves the form with an api that records every request. route answers
// PUT /api/servers/{id}/route: "ok" stores what was asked, "fail" refuses,
// "ignored" answers without storing. base is what the server's own save
// answers with.
const run = async (f, base, route) => {
  ctx.__calls = []; ctx.__said.length = 0;
  ctx.__api = async (path, opts) => {
    const method = (opts && opts.method) || "GET";
    ctx.__calls.push({ method, path, body: (opts && opts.body) || {} });
    if (path.endsWith("/route")) {
      if (route === "fail") throw new Error("this session may not change servers");
      return { ...base, forward_with_capture_account: route === "ignored" ? !opts.body.capture_account : opts.body.capture_account };
    }
    if (ctx.__during) ctx.__during();
    if (ctx.__refuse) throw new Error("a server named shop already exists");
    return base;
  };
  vm.runInContext("api = __api; startMonitor = async () => ({ started: true });", ctx);
  ctx.__f = f;
  await vm.runInContext("saveServer(__f)", ctx);
  // Only what the requests say about the account: the path, and every key
  // of the body that names forwarding.
  return { calls: ctx.__calls.map((c) => {
    const about = Object.fromEntries(Object.entries(c.body).filter(([k]) => /route|capture|forward/.test(k)));
    return c.method + " " + c.path + " " + JSON.stringify(about) + " flavor=" + (c.body.flavor || "");
  }), said: [...ctx.__said] };
};
const on = (f, name) => { const n = f.elements[name]; let shown = true;
  // Shown: every [data-flavor] node from the field up is lit.
  walk(f, (x) => { if (x.attrs["data-flavor"] !== undefined) { let inside = false; walk(x, (y) => { if (y === n) inside = true; }); if (inside && !(" " + x.className + " ").includes(" flavor-on ")) shown = false; } });
  return shown; };
const fresh = (flavor) => { const f = show({ monitor: true }, null); f.elements.flavor.value = flavor; f.elements.flavor.fire("change");
  f.elements.name.value = "shop"; f.elements.source_host.value = "db"; f.elements.source_user.value = "dbtrail"; f.elements.source_password.value = "pw"; return f; };
const pgNew = { id: "n1", name: "shop", flavor: "postgres", source_host: "db", source_user: "dbtrail", has_source: true, has_source_password: true, monitor_state: "stopped" };
const myNew = { ...pgNew, flavor: "mysql" };
const pgSaved = { ...pgNew, id: "x", monitor_state: "running" };
const mySaved = { ...myNew, id: "y", monitor_state: "running" };
const look = (f) => ({ box: on(f, "forward_capture"), ticked: !!f.elements.forward_capture.checked, user: on(f, "route_user"), password: on(f, "route_password"),
  open: !f.querySelector("button#route-own-open").hidden, disabled: f.elements.forward_capture.attrs.disabled === true });
(async () => {
  const out = {};
  // 1. A new PostgreSQL.
  let f = fresh("postgres");
  out.pgNewLook = look(f);
  out.pgNewUnticked = await run(f, pgNew, "ok");
  f = fresh("postgres"); f.elements.forward_capture.checked = true;
  out.pgNewTicked = await run(f, pgNew, "ok");
  f = fresh("postgres"); f.elements.forward_capture.checked = true;
  out.pgNewRouteFails = await run(f, pgNew, "fail");
  f = fresh("postgres"); f.elements.forward_capture.checked = true;
  out.pgNewRouteIgnored = await run(f, pgNew, "ignored");
  f = fresh("postgres"); f.elements.forward_capture.checked = true; f.elements.route_user.value = "app_ro"; f.elements.route_password.value = "pw2";
  out.pgNewBoth = await run(f, pgNew, "ok");
  f = fresh("postgres"); f.querySelector("button#route-own-open").fire("click");
  out.pgNewOpened = look(f);
  f.elements.route_user.value = "app_ro"; f.elements.route_password.value = "pw2";
  out.pgNewOwnUser = await run(f, { ...pgNew, has_route: true, route_user: "app_ro" }, "ok");
  // 2. A PostgreSQL that is edited.
  f = show({ monitor: true }, { ...pgSaved, forward_with_capture_account: true });
  out.pgEditYesLook = look(f);
  out.pgEditYesUntouched = await run(f, { ...pgSaved, forward_with_capture_account: true }, "ok");
  f = show({ monitor: true }, { ...pgSaved, forward_with_capture_account: true }); f.elements.forward_capture.checked = false;
  out.pgEditUnticked = await run(f, { ...pgSaved, forward_with_capture_account: true }, "ok");
  f = show({ monitor: true }, pgSaved);
  out.pgEditNoLook = look(f);
  out.pgEditNoUntouched = await run(f, pgSaved, "ok");
  f = show({ monitor: true }, pgSaved); f.elements.forward_capture.checked = true;
  out.pgEditTicked = await run(f, pgSaved, "ok");
  // The source user changed in the same save: the server took the yes back.
  f = show({ monitor: true }, { ...pgSaved, forward_with_capture_account: true }); f.elements.source_user.value = "other";
  out.pgEditSourceChanged = await run(f, { ...pgSaved, source_user: "other", forward_with_capture_account: false }, "ok");
  f = show({ monitor: true }, { ...pgSaved, has_route: true, route_user: "app_ro", has_route_password: true });
  out.pgEditOwnLook = look(f);
  f.elements.forward_capture.checked = true;
  out.pgEditOwnTicked = await run(f, { ...pgSaved, has_route: true, route_user: "app_ro" }, "ok");
  f = show({ monitor: true }, { ...pgSaved, route_is_capture: true });
  out.pgEditRefusedLook = look(f);
  f = show({ monitor: true, permissions: { "servers:write": false } }, { ...pgSaved, forward_with_capture_account: true });
  out.pgReaderLook = look(f);
  out.pgReaderText = f.querySelector("div#pg-forward").textContent.replace(/\s+/g, " ").trim();
  // Removing the forwarding account says the port stops forwarding: with a
  // yes saved, the yes is taken back first, then the account is removed.
  const own = { ...pgSaved, has_route: true, route_user: "app_ro", has_route_password: true };
  f = show({ monitor: true }, { ...own, forward_with_capture_account: true }); f.elements.route_remove.checked = true;
  out.pgRemoveExtra = f.querySelector("div#route-extra").textContent.replace(/\s+/g, " ").trim();
  out.pgRemoveYes = await run(f, pgSaved, "ok");
  f = show({ monitor: true }, own); f.elements.route_remove.checked = true;
  out.pgRemoveNo = await run(f, pgSaved, "ok");
  f = show({ monitor: true }, { ...own, forward_with_capture_account: true }); f.elements.route_remove.checked = true;
  out.pgRemoveFails = await run(f, pgSaved, "fail");
  f = show({ monitor: true }, { ...own, forward_with_capture_account: true }); f.elements.route_remove.checked = true;
  out.pgRemoveKept = await run(f, { ...pgSaved, forward_with_capture_account: true }, "ok");
  // The yes is taken back, then the server's own save is refused.
  f = show({ monitor: true }, { ...own, forward_with_capture_account: true }); f.elements.route_remove.checked = true;
  ctx.__refuse = true;
  out.pgRemoveSaveRefused = await run(f, pgSaved, "ok");
  ctx.__refuse = false;
  out.pgRemoveSaveRefusedBox = { ticked: !!f.elements.forward_capture.checked, again: await run(f, pgSaved, "ok") };
  // A new MySQL whose type is put back to PostgreSQL, the box ticked, while
  // its save is on its way: what was pressed is what is saved.
  f = fresh("mysql"); ctx.__during = () => { f.elements.flavor.value = "postgres"; f.elements.forward_capture.checked = true; };
  out.myNewTypeChangedMeanwhile = await run(f, myNew, "ok");
  ctx.__during = null;
  f = show({ monitor: true }, own); f.elements.route_remove.checked = true; f.elements.forward_capture.checked = true;
  out.pgRemoveAndTick = await run(f, pgSaved, "ok");
  f = show({ monitor: true }, { ...mySaved, has_route: true, route_user: "app_ro", has_route_password: true, forward_with_capture_account: true }); f.elements.route_remove.checked = true;
  out.myRemove = await run(f, mySaved, "ok");
  // Who forwards now, from what the server answered: nobody, the account of
  // its own, the source user.
  const now = (x) => { ctx.__s = x; return vm.runInContext("pgForwardSaid(__s)", ctx); };
  out.now = { nobody: now(pgSaved), own: now(own), source: now({ ...pgSaved, forward_with_capture_account: true }) };
  // 3. A new MySQL. The box ticked while PostgreSQL was picked stays behind.
  f = fresh("mysql");
  out.myNewLook = look(f);
  out.myNew = await run(f, myNew, "ok");
  f = fresh("postgres"); f.elements.forward_capture.checked = true; f.elements.flavor.value = "mysql"; f.elements.flavor.fire("change");
  out.myNewAfterPGTick = await run(f, myNew, "ok");
  f = fresh("mariadb"); f.elements.forward_capture.checked = true;
  out.mariaNew = await run(f, { ...myNew, flavor: "mariadb" }, "ok");
  // A forwarding user typed for a MySQL, then the type changed: in sight.
  f = fresh("mysql"); f.elements.route_user.value = "app_ro"; f.elements.flavor.value = "postgres"; f.elements.flavor.fire("change");
  out.typedThenPG = look(f);
  // 4. A MySQL that is edited, one whose yes was saved from a terminal too.
  f = show({ monitor: true }, mySaved);
  out.myEditLook = look(f);
  out.myEdit = await run(f, mySaved, "ok");
  f = show({ monitor: true }, { ...mySaved, forward_with_capture_account: true });
  out.myEditYesLook = look(f);
  out.myEditYes = await run(f, { ...mySaved, forward_with_capture_account: true }, "ok");
  // The texts, each from the data that brings it.
  ctx.__s = null;
  const said = (s) => { ctx.__s = s; return vm.runInContext("captureSaid(__s)", ctx); };
  out.said = {
    yes: said({ ...pgSaved, forward_with_capture_account: true }),
    no: said(pgSaved),
    noOwn: said({ ...pgSaved, has_route: true, route_user: "app_ro" }),
    yesOwn: said({ ...pgSaved, has_route: true, route_user: "app_ro", forward_with_capture_account: true }),
    yesRefusedUser: said({ ...pgSaved, route_unreadable: true, forward_with_capture_account: true }),
    yesRefusedURL: said({ ...pgSaved, forward_with_capture_account: true, capture_account_refused: "the server's source URL asks for replication: set a separate forwarding user instead, or correct the source" }),
    noName: said({ forward_with_capture_account: true }),
    yesHides: said({ ...pgSaved, forward_with_capture_account: true, hidden: { tables: ["shop.audit"], columns: [] } }),
  };
  out.ask = vm.runInContext("PG_FORWARD_ASK", ctx);
  // The hint and the two links, read off the form a person gets.
  const hintOf = (f) => f.querySelector("p#pg-forward-hint").textContent;
  f = fresh("postgres");
  out.hint = hintOf(f);
  out.links = f.querySelectorAll("a.bks-docs").map((a) => a.attrs.href + " | " + a.textContent);
  out.ownHint = vm.runInContext("FORWARDING_HINT_PG", ctx);
  f = show({ monitor: true }, { ...pgSaved, forward_with_capture_account: true, hidden: { tables: ["shop.audit"], columns: [] } });
  out.hintHides = hintOf(f);
  console.log(JSON.stringify(out));
})();
`
	path := filepath.Join(t.TempDir(), "capture.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type ran struct {
		Calls []string
		Said  []string
	}
	type look struct {
		Box, Ticked, User, Password, Open, Disabled bool
	}
	type line struct{ Tone, Text string }
	var got struct {
		PgNewLook, PgNewOpened, PgEditYesLook, PgEditNoLook, PgEditOwnLook, PgEditRefusedLook, PgReaderLook look
		MyNewLook, TypedThenPG, MyEditLook, MyEditYesLook                                                   look
		PgNewUnticked, PgNewTicked, PgNewRouteFails, PgNewRouteIgnored, PgNewBoth, PgNewOwnUser             ran
		PgEditYesUntouched, PgEditUnticked, PgEditNoUntouched, PgEditTicked, PgEditSourceChanged            ran
		PgEditOwnTicked, MyNew, MyNewAfterPGTick, MariaNew, MyEdit, MyEditYes                               ran
		PgRemoveYes, PgRemoveNo, PgRemoveFails, PgRemoveKept, PgRemoveAndTick, MyRemove                     ran
		PgRemoveSaveRefused, MyNewTypeChangedMeanwhile                                                      ran
		PgRemoveSaveRefusedBox                                                                              struct {
			Ticked bool
			Again  ran
		}
		Now                                    map[string]string
		PgReaderText, Ask, Hint, PgRemoveExtra string
		HintHides, OwnHint                     string
		Links                                  []string
		Said                                   map[string]line
	}
	s := strings.TrimSpace(string(raw))
	if err := json.Unmarshal([]byte(s[strings.LastIndex(s, "\n")+1:]), &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	pretty, _ := json.MarshalIndent(got, "", "  ")
	t.Logf("%s", pretty)

	const yes = `PUT /api/servers/%s/route {"capture_account":true} flavor=`
	const no = `PUT /api/servers/%s/route {"capture_account":false} flavor=`
	at := func(tmpl, id string) string { return strings.Replace(tmpl, "%s", id, 1) }
	calls := func(name string, g ran, want ...string) {
		t.Helper()
		if len(g.Calls)+len(want) > 0 && !reflect.DeepEqual(g.Calls, want) {
			t.Errorf("%s sends\n  %s\nwant\n  %s", name, strings.Join(g.Calls, "\n  "), strings.Join(want, "\n  "))
		}
	}
	says := func(name string, g ran, want ...string) {
		t.Helper()
		all := strings.Join(g.Said, "\n")
		for _, w := range want {
			if !strings.Contains(all, w) {
				t.Errorf("%s says\n  %s\nwant it to hold %q", name, all, w)
			}
		}
		if strings.Contains(all, "undefined") || strings.Contains(all, "—") {
			t.Errorf("%s: holds undefined or an em dash: %s", name, all)
		}
	}
	silent := func(name string, g ran) {
		t.Helper()
		for _, line := range g.Said {
			if strings.Contains(line, "forward") || strings.HasPrefix(line, "stays: ") {
				t.Errorf("%s says something about forwarding, or an error: %q", name, line)
			}
		}
	}

	// A new PostgreSQL: the box is there, unticked, and the separate
	// forwarding user is folded.
	if got.PgNewLook != (look{Box: true, Open: true}) {
		t.Errorf("a new PostgreSQL's form = %+v, want the box shown and unticked, the separate user folded, and the button that opens it", got.PgNewLook)
	}
	if got.PgNewOpened != (look{Box: true, User: true, Password: true}) {
		t.Errorf("after the button = %+v, want the two fields shown and the button gone", got.PgNewOpened)
	}
	calls("a new PostgreSQL, box unticked", got.PgNewUnticked, `POST /api/servers {} flavor=postgres`)
	silent("a new PostgreSQL, box unticked", got.PgNewUnticked)
	calls("a new PostgreSQL, box ticked", got.PgNewTicked, `POST /api/servers {} flavor=postgres`, at(yes, "n1"))
	says("a new PostgreSQL, box ticked", got.PgNewTicked, "toast: Monitoring started. Events will appear within a minute. shop: the PostgreSQL port now sends to this PostgreSQL, read-only and as dbtrail, a SELECT the copy does not answer, when every table it reads is a table of the copy.")
	calls("a new PostgreSQL, the answer refused", got.PgNewRouteFails, `POST /api/servers {} flavor=postgres`, at(yes, "n1"))
	says("a new PostgreSQL, the answer refused", got.PgNewRouteFails, "stays: shop was saved, and the box about forwarding as the source user was not: this session may not change servers. Edit the server to try again.")
	says("a new PostgreSQL, the answer not stored", got.PgNewRouteIgnored, "stays: shop was saved, and the box about forwarding as the source user was not: DBTrail took the request and did not save the answer. Edit the server to try again.")
	for name, g := range map[string]ran{"refused": got.PgNewRouteFails, "not stored": got.PgNewRouteIgnored} {
		if strings.Contains(strings.Join(g.Said, "\n"), "now sends") {
			t.Errorf("the answer %s, and the save still says the port forwards: %v", name, g.Said)
		}
	}
	// Ticked beside a typed forwarding user: stopped before anything is sent.
	calls("a new PostgreSQL, the box and a typed forwarding user", got.PgNewBoth)
	says("a new PostgreSQL, the box and a typed forwarding user", got.PgNewBoth, "form: The box that lets the port forward as the source user is ticked, and a separate forwarding user was typed too.")
	calls("a new PostgreSQL with a forwarding user of its own", got.PgNewOwnUser, `POST /api/servers {"route_user":"app_ro","route_password":"pw2"} flavor=postgres`)

	// An edited PostgreSQL: the box shows what is saved, and only a change
	// of it is sent.
	if got.PgEditYesLook != (look{Box: true, Ticked: true, Open: true}) || got.PgEditNoLook != (look{Box: true, Open: true}) {
		t.Errorf("an edited PostgreSQL's box = %+v (saved yes), %+v (saved no)", got.PgEditYesLook, got.PgEditNoLook)
	}
	calls("an edited PostgreSQL, saved yes, untouched", got.PgEditYesUntouched, `PUT /api/servers/x {} flavor=postgres`)
	silent("an edited PostgreSQL, saved yes, untouched", got.PgEditYesUntouched)
	calls("an edited PostgreSQL, saved no, untouched", got.PgEditNoUntouched, `PUT /api/servers/x {} flavor=postgres`)
	silent("an edited PostgreSQL, saved no, untouched", got.PgEditNoUntouched)
	calls("an edited PostgreSQL, unticked", got.PgEditUnticked, `PUT /api/servers/x {} flavor=postgres`, at(no, "x"))
	says("an edited PostgreSQL, unticked", got.PgEditUnticked, "toast: Server updated. shop: the PostgreSQL port sends nothing to this PostgreSQL now. The copy answers alone.")
	calls("an edited PostgreSQL, ticked", got.PgEditTicked, `PUT /api/servers/x {} flavor=postgres`, at(yes, "x"))
	says("an edited PostgreSQL, ticked", got.PgEditTicked, "toast: Server updated. shop: the PostgreSQL port now sends")
	// The yes was about another account: it is not given again by a box
	// nobody touched, and the form says it was taken back.
	calls("an edited PostgreSQL whose source user changed", got.PgEditSourceChanged, `PUT /api/servers/x {} flavor=postgres`)
	says("an edited PostgreSQL whose source user changed", got.PgEditSourceChanged, "stays: shop: the PostgreSQL port no longer forwards as the source user. The yes was taken back: the source's user or address changed and it was given for the one before, or somebody unticked the box meanwhile. Edit the server and tick the box again to give it.")
	// A saved forwarding user of its own, or one the port refuses, is in
	// sight when the form opens.
	if got.PgEditOwnLook != (look{Box: true, User: true, Password: true}) || got.PgEditRefusedLook != (look{Box: true, User: true, Password: true}) {
		t.Errorf("a PostgreSQL with a saved forwarding user = %+v, with one the port refuses = %+v: want the fields shown", got.PgEditOwnLook, got.PgEditRefusedLook)
	}
	calls("an edited PostgreSQL with its own user, ticked", got.PgEditOwnTicked, `PUT /api/servers/x {} flavor=postgres`, at(yes, "x"))
	says("an edited PostgreSQL with its own user, ticked", got.PgEditOwnTicked, "shop: saved. The PostgreSQL port keeps forwarding as its forwarding user, app_ro, while that one is set. Once it is removed, the source user takes its place.")
	// Removing the forwarding account: the control says the port stops
	// forwarding, so the yes about the source user goes first, and the save
	// says who forwards now from what the server answered.
	const nobody = "For shop, nothing is sent to your PostgreSQL: the copy answers alone, and a statement it cannot answer gets an error. To change it, edit the server (Servers, Edit)."
	for name, g := range map[string]ran{"a yes saved": got.PgRemoveYes, "no yes saved": got.PgRemoveNo} {
		calls("removing a PostgreSQL's forwarding account, "+name, g, at(no, "x"), `PUT /api/servers/x {"route_user":""} flavor=postgres`)
		says("removing a PostgreSQL's forwarding account, "+name, g, "toast: Server updated. "+nobody)
	}
	if !strings.Contains(got.PgRemoveExtra, "Removing it also unticks, for this server, the box that lets the port forward as the source user: nothing is sent to this PostgreSQL afterwards.") {
		t.Errorf("the remove control of a PostgreSQL does not say it takes the yes back: %s", got.PgRemoveExtra)
	}
	calls("a removal whose yes cannot be taken back", got.PgRemoveFails, at(no, "x"))
	says("a removal whose yes cannot be taken back", got.PgRemoveFails, "stays: shop: the forwarding account was not removed. Removing it also turns off forwarding as the source user, and that could not be saved: this session may not change servers. Nothing was changed.", "form: Nothing was saved: the forwarding account was not removed.")
	says("a removal after which the server still says yes", got.PgRemoveKept, "stays: shop: the removal was saved, and the port still forwards. For shop, a SELECT the copy does not answer runs on your PostgreSQL, read-only and as the source user (dbtrail)")
	if strings.Contains(strings.Join(got.PgRemoveKept.Said, "\n"), "nothing is sent") {
		t.Errorf("the server still forwards, and the save says nothing is sent: %v", got.PgRemoveKept.Said)
	}
	// The yes was taken back and the server's own save was then refused: the
	// form says both, shows the box unticked, and a second Save does not
	// blame the source for it.
	calls("a removal whose own save is refused", got.PgRemoveSaveRefused, at(no, "x"), `PUT /api/servers/x {"route_user":""} flavor=postgres`)
	says("a removal whose own save is refused", got.PgRemoveSaveRefused, "form: a server named shop already exists. The forwarding account was not removed. The box that lets the port forward as the source user was unticked and that was saved: tick it again if the port should forward as the source user.")
	if got.PgRemoveSaveRefusedBox.Ticked {
		t.Error("the yes was taken back on the server, and the form still shows the box ticked")
	}
	calls("the same form saved again", got.PgRemoveSaveRefusedBox.Again, at(no, "x"), `PUT /api/servers/x {"route_user":""} flavor=postgres`)
	silent("the same form saved again", ran{Said: filterOut(got.PgRemoveSaveRefusedBox.Again.Said, "toast: ")})
	calls("a new MySQL whose type and box were changed while it was being saved", got.MyNewTypeChangedMeanwhile, `POST /api/servers {} flavor=mysql`)
	calls("remove ticked and the box newly ticked", got.PgRemoveAndTick)
	says("remove ticked and the box newly ticked", got.PgRemoveAndTick, "form: Remove the forwarding account is ticked, which stops the port forwarding for this server, and the box that lets it forward as the source user was ticked too.")
	calls("removing a MySQL's forwarding account, a yes saved from a terminal", got.MyRemove, `PUT /api/servers/y {"route_user":""} flavor=mysql`)
	silent("removing a MySQL's forwarding account", got.MyRemove)
	wantNow := map[string]string{
		"nobody": nobody,
		"own":    "For shop, every other statement, and a transaction block (BEGIN to COMMIT or ROLLBACK), runs on your PostgreSQL as its forwarding user, app_ro, read-only. To change it, edit the server (Servers, Edit).",
		"source": "For shop, a SELECT the copy does not answer runs on your PostgreSQL, read-only and as the source user (dbtrail), when every table it reads is a table of the copy; anything else is refused and not sent. To change it, edit the server (Servers, Edit).",
	}
	if !reflect.DeepEqual(got.Now, wantNow) {
		t.Errorf("who forwards now = %v\nwant %v", got.Now, wantNow)
	}
	// A session that may not change servers sees the box and cannot change it.
	if !got.PgReaderLook.Disabled || !got.PgReaderLook.Ticked || !strings.Contains(got.PgReaderText, "Changing it needs permission to change servers.") {
		t.Errorf("without servers:write = %+v, %q: want the saved answer shown, disabled, and why", got.PgReaderLook, got.PgReaderText)
	}
	if got.PgNewLook.Disabled {
		t.Error("the box is disabled for a session that may change servers")
	}

	// MySQL and MariaDB: no box, the forwarding fields as before, and nothing
	// about the capture account in any request, whatever the box holds.
	for name, l := range map[string]look{"a new MySQL": got.MyNewLook, "an edited MySQL": got.MyEditLook, "an edited MySQL with a yes saved from a terminal": got.MyEditYesLook} {
		if l != (look{User: true, Password: true, Open: l.Open}) {
			t.Errorf("%s = %+v, want no box, an unticked one, and the two forwarding fields", name, l)
		}
	}
	calls("a new MySQL", got.MyNew, `POST /api/servers {} flavor=mysql`)
	calls("a new MySQL after the box was ticked as PostgreSQL", got.MyNewAfterPGTick, `POST /api/servers {} flavor=mysql`)
	calls("a new MariaDB with the box ticked by code", got.MariaNew, `POST /api/servers {} flavor=mariadb`)
	calls("an edited MySQL", got.MyEdit, `PUT /api/servers/y {} flavor=mysql`)
	calls("an edited MySQL with a yes saved from a terminal", got.MyEditYes, `PUT /api/servers/y {} flavor=mysql`)
	for name, g := range map[string]ran{"a new MySQL": got.MyNew, "after a PostgreSQL tick": got.MyNewAfterPGTick, "MariaDB": got.MariaNew, "an edited MySQL": got.MyEdit, "an edited MySQL, yes saved": got.MyEditYes} {
		silent(name, g)
	}
	if got.TypedThenPG != (look{Box: true, User: true, Password: true}) {
		t.Errorf("a forwarding user typed, then PostgreSQL picked = %+v, want the typed user in sight", got.TypedThenPG)
	}

	// The texts, whole.
	wantSaid := map[string]line{
		"yes":            {"ok", "shop: the PostgreSQL port now sends to this PostgreSQL, read-only and as dbtrail, a SELECT the copy does not answer, when every table it reads is a table of the copy."},
		"no":             {"ok", "shop: the PostgreSQL port sends nothing to this PostgreSQL now. The copy answers alone."},
		"noOwn":          {"ok", "shop: the PostgreSQL port does not forward as the source user. Its forwarding user, app_ro, stays as it was."},
		"yesHides":       {"err", "shop: saved, and not used. This server hides tables or columns, so the port sends nothing to this PostgreSQL."},
		"yesOwn":         {"ok", "shop: saved. The PostgreSQL port keeps forwarding as its forwarding user, app_ro, while that one is set. Once it is removed, the source user takes its place."},
		"yesRefusedUser": {"err", "shop: saved, and not used yet. The server has a saved forwarding user that the port refuses, and while it is saved nothing is forwarded. Edit the server and remove it."},
		"yesRefusedURL":  {"err", "shop: saved, and not used: the server's source URL asks for replication: set a separate forwarding user instead, or correct the source."},
		"noName":         {"ok", "This server: the PostgreSQL port now sends to this PostgreSQL, read-only and as the source user, a SELECT the copy does not answer, when every table it reads is a table of the copy."},
	}
	if !reflect.DeepEqual(got.Said, wantSaid) {
		for k, w := range wantSaid {
			if got.Said[k] != w {
				t.Errorf("captureSaid %s = %+v\nwant %+v", k, got.Said[k], w)
			}
		}
	}
	if got.Ask != "Let the PostgreSQL port send to this PostgreSQL, read-only and as the source user above, a SELECT the copy does not answer" {
		t.Errorf("the box reads %q", got.Ask)
	}
	// Two short lines each, the rest behind a link to the docs site.
	if got.Hint != "Unticked: the copy answers alone, and a statement it cannot answer gets an error. Ticked: the port sends that SELECT to this PostgreSQL as the source user, read-only, and only when every table it reads is in the copy. No second user to create." {
		t.Errorf("the box's hint reads %q", got.Hint)
	}
	if got.OwnHint != "For a port that should send this PostgreSQL more than the box allows: transactions, and every statement the copy does not answer as PostgreSQL does. It must be another user than the source user: a role that can log in, with SELECT on what the port should read and nothing more. While it is set, the box above is not used." {
		t.Errorf("the separate forwarding user's hint reads %q", got.OwnHint)
	}
	// A server that hides tables or columns sends nothing: its hint does not
	// say the port sends.
	if got.HintHides != "This server hides tables or columns, so the port sends nothing to this PostgreSQL, ticked or not: the copy answers alone." {
		t.Errorf("the hint of a server that hides tables reads %q", got.HintHides)
	}
	wantLinks := []string{
		"https://dbtrail.com/docs/guides/query-with-a-postgres-client/#with-the-user-dbtrail-already-connects-with | Read more: what the box changes",
		"https://dbtrail.com/docs/guides/query-with-a-postgres-client/#with-a-forwarding-user-of-its-own | Read more: a forwarding user of its own",
	}
	if !reflect.DeepEqual(got.Links, wantLinks) {
		t.Errorf("the form's links = %q\nwant %q", got.Links, wantLinks)
	}
	for name, text := range map[string]string{"the box": got.Ask, "its hint": got.Hint, "the hint of a server that hides": got.HintHides, "the separate user's hint": got.OwnHint, "the reader's box": got.PgReaderText} {
		if strings.Contains(text, "MySQL") || strings.Contains(text, "Engine Routing") || strings.Contains(text, "—") || strings.Contains(text, "undefined") {
			t.Errorf("%s speaks of the MySQL port, or holds an em dash or undefined: %s", name, text)
		}
	}
}

// filterOut drops the lines that start with prefix.
func filterOut(lines []string, prefix string) []string {
	var out []string
	for _, l := range lines {
		if !strings.HasPrefix(l, prefix) {
			out = append(out, l)
		}
	}
	return out
}
