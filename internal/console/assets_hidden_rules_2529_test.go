package console

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"

	"github.com/dbtrail/dbtrail/internal/hiderules"
)

// hiddenRulesJS draws the "Hidden from SQL" section (#2529) from what the
// real routes answered, presses Save against a recording api, and draws the
// notes beside SQL on the copy and the downloads. It reports what a person
// would read.
const hiddenRulesJS = `
const whole = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(whole).join("");
const tidy = (s) => s.replace(/\s+/g, " ").trim();
const walk = (n, f) => { if (!n || n.nodeType === 3) return; f(n); (n.children || []).forEach((c) => walk(c, f)); };
const run = (s) => vm.runInContext(s, ctx);
const has = (x, c) => (" " + x.className + " ").includes(" " + c + " ");
const read = (box) => { const o = { lines: [], errs: [], items: [], areas: [], buttons: [] };
  walk(box, (x) => {
    if (x.tag === "p" && !x.hidden) (has(x, "err") ? o.errs : o.lines).push(tidy(whole(x)));
    if (x.tag === "span" && (has(x, "form-hint") || has(x, "bk-fact"))) o.lines.push(tidy(whole(x)));
    if (x.tag === "li") o.items.push(tidy(whole(x)));
    if (x.tag === "textarea") o.areas.push(x.attrs.name + "=" + JSON.stringify(x.value));
    if (x.tag === "button") o.buttons.push(x._text + (x.disabled ? " [off]" : "")); });
  return o; };
const find = (box, tag) => { let hit = null; walk(box, (x) => { if (!hit && x.tag === tag) hit = x; }); return hit; };
const findArea = (box, name) => { let hit = null; walk(box, (x) => { if (!hit && x.tag === "textarea" && x.attrs.name === name) hit = x; }); return hit; };
(async () => {
  const d = JSON.parse(process.argv[4]);
  const out = {};
  const caps = (perms) => run("capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + ";");
  caps(null);
  const cur = { id: "s1", name: "prod", kind: "registry" };
  ctx.__cur = cur;
  const body = (h, g) => { ctx.__h = h; ctx.__g = g; const box = run("document.createElement('div')"); ctx.__box = box;
    run("hiddenRulesBody(__box, __cur, __h, __g)"); return box; };
  const U = undefined;
  out.empty = read(body(d.empty, d.grantsNone));
  out.exposed = read(body(d.withRules, d.grantsExposed));
  out.clean = read(body(d.withRules, d.grantsClean));
  out.none = read(body(d.withRules, d.grantsNone));
  out.failed = read(body(d.withRules, null));
  out.notAsked = read(body(d.withRules, U));
  out.noRulesExposed = read(body(d.empty, d.grantsExposed));
  out.oldServer = read(body(d.withRules, d.grantsOld));
  out.grantsError = read(body(d.withRules, d.grantsError));
  out.boot = read(body(d.boot, U));
  out.unreadable = read(body(d.unreadable, U));
  out.unreadableGrants = read(body(d.unreadable, d.grantsLoadErr));
  out.unreadNull = read(body(null, U));
  out.onlyKey = read(body(d.onlyKey, d.grantsLoadErrOnly));
  out.brokenEdit = read(body(d.brokenEdit, d.grantsLoadErrBroken));
  out.capture = read(body(d.withRules, d.grantsCapture));
  out.realNone = read(body(d.withRules, d.grantsNone));
  out.grantLine = read(body(d.withRules, d.grantsGrantLine));
  out.gone = read(body(d.withRules, "gone"));
  out.unmatched = read(body(d.unmatched, d.grantsNone));
  caps({ "servers:write": false });
  out.readOnly = read(body(d.withRules, d.grantsClean));
  out.readOnlyKey = read(body(d.onlyKey, d.grantsLoadErrOnly));
  caps(null);

  // Save: what is sent, and what the section shows after.
  ctx.__calls = []; ctx.__answers = {}; ctx.__fail = null;
  run('api = (p, o) => { __calls.push([p, o || null]); if (__fail && o && o.method === "PUT") return Promise.reject(new Error(__fail)); return Promise.resolve(__answers[p]); };');
  run('currentServer = "s1";');
  const settle = () => new Promise((r) => setImmediate(r));
  ctx.__answers["/api/servers/s1/hidden"] = d.withRules;
  ctx.__answers["/api/servers/s1/route-grants"] = d.grantsExposed;
  let box = body(d.empty, d.grantsNone);
  findArea(box, "tables").value = "  shop.audit_log  \n\n\r\nshop.*\n   ";
  findArea(box, "columns").value = "shop.customers.email";
  find(box, "button").onclick();
  await settle(); await settle();
  out.saved = { calls: ctx.__calls.splice(0), after: read(box), current: run("hiddenRulesOf()"), entry: cur.hidden || null };
  // Emptying both lists sends two empty lists, and the notes go.
  ctx.__answers["/api/servers/s1/hidden"] = d.empty;
  findArea(box, "tables").value = "\n";
  findArea(box, "columns").value = "";
  find(box, "button").onclick();
  await settle(); await settle();
  out.cleared = { calls: ctx.__calls.splice(0), after: read(box), current: run("hiddenRulesOf()") };
  // Refused: the server's words, as they came.
  ctx.__fail = d.refusal;
  box = body(d.empty, U);
  findArea(box, "tables").value = "not a table";
  find(box, "button").onclick();
  await settle(); await settle();
  out.refused = { calls: ctx.__calls.splice(0), after: read(box) };
  ctx.__fail = null;
  // A server whose rules are only a newer build's key: emptying the lists
  // keeps the key, and the answer still says refused. Never "nothing is hidden".
  ctx.__answers["/api/servers/s1/hidden"] = d.onlyKeyAfterClear;
  ctx.__answers["/api/servers/s1/route-grants"] = d.grantsLoadErrOnly;
  box = body(d.onlyKey, d.grantsLoadErrOnly);
  find(box, "button").onclick();
  await settle(); await settle();
  out.onlyKeySaved = { calls: ctx.__calls.splice(0), after: read(box), current: run("hiddenRulesOf()") };
  // The sidebar switched while the save was out: the map still learns it,
  // and the old server's box is not repainted.
  ctx.__answers["/api/servers/s1/hidden"] = d.withRules;
  run("rememberHiddenRules([])");
  box = body(d.empty, d.grantsNone);
  findArea(box, "tables").value = "shop.audit_log";
  find(box, "button").onclick();
  run("serverGen++");
  await settle(); await settle();
  out.switched = { calls: ctx.__calls.splice(0), after: read(box), current: run("hiddenRulesOf()") };

  // What the grants read turns into: a 404 is a server gone, a 403 a
  // session that may not.
  const fetchWith = async (status) => {
    run('api = (p) => Promise.reject(Object.assign(new Error("x"), { status: ' + status + ' }));');
    const v = await run('hiddenGrantsFetch("s1")');
    return v === undefined ? "undefined" : v === null ? "null" : String(v);
  };
  out.fetch404 = await fetchWith(404);
  out.fetch403 = await fetchWith(403);
  out.fetch500 = await fetchWith(500);
  run('api = (p, o) => { __calls.push([p, o || null]); if (__fail && o && o.method === "PUT") return Promise.reject(new Error(__fail)); return Promise.resolve(__answers[p]); };');
  // The notes beside SQL on the copy and the downloads, drawn from the real
  // server list DTO; the per-server read answers the same.
  run("rememberHiddenRules(" + JSON.stringify(d.list) + ")");
  ctx.__answers["/api/servers/s1/hidden"] = d.withRules;
  ctx.__answers["/api/servers/s2/hidden"] = d.empty;
  ctx.__answers["/api/servers/s3/hidden"] = d.onlyKey;
  const box2 = () => run("document.createElement('div')");
  const sqlBox = box2(); ctx.__sb = sqlBox; run("renderSQLPanel(__sb)");
  await settle();
  out.sqlNote = read(sqlBox).lines.filter((l) => l.includes("hides"));
  const lap = box2(); ctx.__lap = lap; run('USE_PANELS.laptop(__lap, { cta: "ok", snap: { time: "2026-10-24T10:00:00Z" } })');
  await settle();
  out.laptop = read(lap).lines.filter((l) => l.includes("hides"));
  // views.sql alone (no query:execute): the file reads what SQL hides too.
  run("capsCache.views = true;");
  caps({ "query:execute": false }); run("capsCache.views = true;");
  const lap2 = box2(); ctx.__lap2 = lap2; run('USE_PANELS.laptop(__lap2, { cta: "ok", snap: { time: "2026-10-24T10:00:00Z" }, viewsSQL: true })');
  await settle();
  out.laptopViewsOnly = { lines: read(lap2).lines.filter((l) => l.includes("hides")), buttons: read(lap2).buttons };
  caps(null);
  const dash = run('dashPanelBody({ state: "s3", s3: "s3://b/p", snapshot: "2026-10-24T10:00:00Z", tables: 1, views_sql: "x" })');
  await settle();
  out.dash = read(dash).lines.filter((l) => l.includes("hides"));
  run("capsCache.views = true; capsCache.data_profile = false;");
  const b = { configured: true, kind: "dir", snapshots: [{ time: "2026-10-24T10:00:00Z", kinds: ["dir"] }] };
  ctx.__b = b;
  ctx.__curH = { id: "s1", kind: "registry", hidden: { tables: d.withRules.tables, columns: d.withRules.columns } };
  ctx.__curN = { id: "s2", kind: "registry" };
  out.duckHidden = read(run("backupDuckLane(__b, __curH)")).lines.filter((l) => l.includes("hides"));
  out.duckPlain = read(run("backupDuckLane(__b, __curN)")).lines.filter((l) => l.includes("hides"));
  run("capsCache.sql_export = true;");
  const ready = { sql_export: { state: "succeeded", at: "2026-10-24T10:00:00Z", bytes: 1024 } };
  ctx.__st = ready;
  out.sqlLaneHidden = read(run("backupSQLLane(__curH, __b, __st)")).lines.filter((l) => l.includes("hides"));
  out.sqlLanePlain = read(run("backupSQLLane(__curN, __b, __st)")).lines.filter((l) => l.includes("hides"));
  out.inPlaceHidden = read(run("snapshotInPlace(__curH, __b)")).lines.filter((l) => l.includes("hides"));
  out.inPlacePlain = read(run("snapshotInPlace(__curN, __b)")).lines.filter((l) => l.includes("hides"));
  ctx.__curK = d.list.find((x) => x.id === "s3");
  out.duckOnlyKey = read(run("backupDuckLane(__b, __curK)")).lines.filter((l) => l.includes("hides"));
  // An entry the page did not pass falls back to the server picked.
  run('currentServer = "s1";');
  out.duckNoCur = read(run("backupDuckLane(__b, undefined)")).lines.filter((l) => l.includes("hides"));
  run('currentServer = "s3";');
  const sqlBox3 = box2(); ctx.__sb3 = sqlBox3; run("renderSQLPanel(__sb3)");
  await settle();
  out.sqlNoteOnlyKey = read(sqlBox3).lines.filter((l) => l.includes("hides") || l.includes("hidden rules"));
  caps({ "servers:write": false });
  const sqlBox5 = box2(); ctx.__sb5 = sqlBox5; run("renderSQLPanel(__sb5)");
  await settle();
  out.sqlNoteNoWrite = read(sqlBox5).lines.filter((l) => l.includes("hidden rules"));
  caps(null);
  // A list read before someone added rules: the note appears once the
  // server's rules are read again.
  run("rememberHiddenRules([])");
  run('currentServer = "s1";');
  const sqlBox4 = box2(); ctx.__sb4 = sqlBox4; run("renderSQLPanel(__sb4)");
  out.staleBefore = read(sqlBox4).lines.filter((l) => l.includes("hides"));
  await settle(); await settle();
  out.staleAfter = read(sqlBox4).lines.filter((l) => l.includes("hides"));
  // The Download on a row of Versions.
  const detail = async () => { const bx = box2(); ctx.__dx = bx; await run('loadBackupDetail("2026-10-24T10:00:00Z", __dx)'); return read(bx).lines.filter((l) => l.includes("hides")); };
  ctx.__answers["/api/baselines/files?at=2026-10-24T10%3A00%3A00Z"] = { total_bytes: 2048, tables: [{ schema: "shop", table: "orders", files: 1, bytes: 2048 }], files: 1 };
  out.versionsHidden = await detail();
  run('currentServer = "s2";');
  out.versionsPlain = await detail();
  const sqlBox2 = box2(); ctx.__sb2 = sqlBox2; run("renderSQLPanel(__sb2)");
  await settle();
  out.sqlNotePlain = read(sqlBox2).lines.filter((l) => l.includes("hides"));
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

type hiddenDrawn struct {
	Lines, Errs, Items, Areas, Buttons []string
}

func TestHiddenRulesSection_2529(t *testing.T) {
	data := map[string]any{}
	raw := func(name string, b []byte) { data[name] = json.RawMessage(b) }

	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	base := "/api/servers/" + id + "/hidden"
	rec, body := doServersReq(t, srv, "GET", base, "")
	if rec.Code != 200 {
		t.Fatalf("GET = %d %s", rec.Code, body)
	}
	raw("empty", body)
	rec, body = doServersReq(t, srv, "PUT", base, `{"tables":["shop.audit_log","shop.*"],"columns":["shop.customers.email"]}`)
	if rec.Code != 200 {
		t.Fatalf("PUT = %d %s", rec.Code, body)
	}
	raw("withRules", body)
	rec, body = doServersReq(t, srv, "PUT", base, `{"tables":["not a table"],"columns":[]}`)
	if rec.Code != 400 {
		t.Fatalf("bad PUT = %d %s, want 400", rec.Code, body)
	}
	var refusal struct{ Error string }
	if err := json.Unmarshal(body, &refusal); err != nil || refusal.Error == "" {
		t.Fatalf("bad PUT body %s: %v", body, err)
	}
	data["refusal"] = refusal.Error
	rec, body = doServersReq(t, srv, "GET", "/api/servers/"+bootServerID+"/hidden", "")
	if rec.Code != 200 {
		t.Fatalf("boot GET = %d %s", rec.Code, body)
	}
	raw("boot", body)

	// Rules this build cannot enforce, as a registry file holds them: a newer
	// build's key beside a list (s4), ONLY a newer build's key (s3), and a hand
	// edit that does not parse (s5). s1 and s2 are the list the notes read.
	path := t.TempDir() + "/console-servers.yaml"
	entry := func(id, hidden string) string {
		return "- id: " + id + "\n  name: " + id + "\n  dsn: u:p@tcp(h:3306)/db\n" + hidden
	}
	yamlText := "version: 1\nservers:\n" +
		entry("s1", "  hidden:\n    tables:\n    - shop.audit_log\n    - shop.*\n    columns:\n    - shop.customers.email\n") +
		entry("s2", "") +
		entry("s3", "  hidden:\n    future_rule: keep-me\n") +
		entry("s4", "  hidden:\n    tables:\n    - shop.audit_log\n    future_rule: keep-me\n") +
		entry("s5", "  hidden:\n    tables:\n    - audit_log\n")
	if err := os.WriteFile(path, []byte(yamlText), 0o600); err != nil {
		t.Fatal(err)
	}
	clearStores(t)
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	fut, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	get := func(name, path string) {
		t.Helper()
		rec, body := doServersReq(t, fut, "GET", path, "")
		if rec.Code != 200 {
			t.Fatalf("%s: %d %s", path, rec.Code, body)
		}
		raw(name, body)
	}
	get("unreadable", "/api/servers/s4/hidden")
	get("onlyKey", "/api/servers/s3/hidden")
	get("brokenEdit", "/api/servers/s5/hidden")
	rec, body = doServersReq(t, fut, "GET", "/api/servers", "")
	if rec.Code != 200 {
		t.Fatalf("list: %d %s", rec.Code, body)
	}
	var list struct{ Servers []json.RawMessage }
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	data["list"] = list.Servers
	rec, body = doServersReq(t, fut, "PUT", "/api/servers/s3/hidden", `{"tables":[],"columns":[]}`)
	if rec.Code != 200 {
		t.Fatalf("clear s3: %d %s", rec.Code, body)
	}
	raw("onlyKeyAfterClear", body)

	// GET over a server whose copy SQL reads: shop.orders is in the
	// snapshot, shop.nothere is not.
	uf, _ := hiddenSQLFixture(t, &hiderules.Rules{Tables: []string{"shop.orders", "shop.nothere"}})
	if code, b := hiddenGet(t, uf.s, uf.s.cm.boot.serverID); code != 200 {
		t.Fatalf("unmatched GET = %d %s", code, b)
	} else {
		raw("unmatched", b)
	}

	// route-grants answers the real handler gives with no rules involved.
	grantsFor := func(e ServerEntry) json.RawMessage {
		t.Helper()
		b, err := json.Marshal(fut.routeGrants(t.Context(), e))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	data["grantsCapture"] = grantsFor(ServerEntry{ID: "c", SourceDSN: "repl:pw@tcp(db:3306)/", RouteDSN: "repl:pw@tcp(db:3306)/"})
	data["grantsNone"] = grantsFor(ServerEntry{ID: "n", SourceDSN: "repl:pw@tcp(db:3306)/"})

	// The grants check, in the shape GET /api/servers/{id}/route-grants
	// answers on a server with rules (field hidden: exposed and unsure).
	// The grants check from the real handler (#2541), with SHOW GRANTS
	// answered by a stand-in for the forwarding account's MySQL.
	var showGrants []string
	var showErr error
	fut.routeAccountGrants = func(context.Context, string, config.SSL, time.Duration) ([]string, error) { return showGrants, showErr }
	routed := func(id string, lines []string, err error) json.RawMessage {
		t.Helper()
		e := ServerEntry{ID: id, SourceDSN: "repl:pw@tcp(db:3306)/", RouteDSN: "report_ro:pw@tcp(db:3306)/"}
		if id != "" {
			got, ok := reg.Get(id)
			if !ok {
				t.Fatalf("%s not loaded", id)
			}
			e.Hidden = got.Hidden
		}
		showGrants, showErr = lines, err
		return grantsFor(e)
	}
	usage := "GRANT USAGE ON *.* TO `report_ro`@`%`"
	grant := "GRANT SELECT ON `shop`.* TO `report_ro`@`%`"
	fileGrant := "GRANT FILE ON *.* TO `report_ro`@`%`"
	data["grantsExposed"] = routed("s1", []string{usage, grant}, nil)
	data["grantsClean"] = routed("s1", []string{usage, "GRANT SELECT ON `sales`.* TO `report_ro`@`%`"}, nil)
	data["grantsGrantLine"] = routed("s1", []string{fileGrant}, nil)
	data["grantsLoadErr"] = routed("s4", []string{usage, grant}, nil)
	data["grantsLoadErrOnly"] = routed("s3", []string{usage, grant}, nil)
	data["grantsLoadErrBroken"] = routed("s5", []string{usage, grant}, nil)
	data["grantsOld"] = routed("", []string{usage, grant}, nil)
	data["grantsError"] = routed("s1", nil, errors.New("Error 1045: Access denied"))
	for _, k := range []string{"grantsExposed", "grantsClean", "grantsGrantLine", "grantsLoadErrOnly"} {
		if !strings.Contains(string(data[k].(json.RawMessage)), `"hidden"`) {
			t.Fatalf("%s carries no hidden check: %s", k, data[k])
		}
	}

	arg, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	outRaw := runNodeConnectArgs(t, renderHarnessJS+hiddenRulesJS, string(arg))
	var got struct {
		Empty, Exposed, Clean, None, Failed, NotAsked, NoRulesExposed, OldServer, GrantsError, Boot, Unreadable, UnreadableGrants, UnreadNull, ReadOnly, ReadOnlyKey, OnlyKey, BrokenEdit, Capture, RealNone, GrantLine, Gone, Unmatched hiddenDrawn
		OnlyKeySaved, Switched                                                                                                                                                                                                           struct {
			Calls   [][]json.RawMessage
			After   hiddenDrawn
			Current *struct {
				Tables, Columns []string
				Unreadable      bool
			}
		}
		LaptopViewsOnly                                                                                                    struct{ Lines, Buttons []string }
		Fetch404, Fetch403, Fetch500                                                                                       string
		Dash, InPlaceHidden, InPlacePlain, DuckOnlyKey, DuckNoCur, SQLNoteOnlyKey, SQLNoteNoWrite, StaleBefore, StaleAfter []string
		Saved                                                                                                              struct {
			Calls   [][]json.RawMessage
			After   hiddenDrawn
			Current *struct{ Tables, Columns []string }
			Entry   *struct{ Tables, Columns []string }
		}
		Cleared struct {
			Calls   [][]json.RawMessage
			After   hiddenDrawn
			Current *struct{ Tables, Columns []string }
		}
		Refused struct {
			Calls [][]json.RawMessage
			After hiddenDrawn
		}
		SQLNote, Laptop, DuckHidden, DuckPlain, SQLLaneHidden, SQLLanePlain, SQLNotePlain, VersionsHidden, VersionsPlain []string
	}
	if err := json.Unmarshal(outRaw, &got); err != nil {
		t.Fatalf("decode %s: %v", outRaw, err)
	}
	all := func(d hiddenDrawn) string {
		return strings.Join(append(append(append([]string{}, d.Lines...), d.Errs...), d.Items...), " | ")
	}
	does := "Hidden tables and columns disappear from SQL on the copy, the MySQL port and the AI connector's SQL tool, and the port's time travel (_flashback, _snapshot, _diff) is off for this server."
	doesNot := "The data stays in the copy's files and in the index, so time travel in this console and the AI connector's history tools still see full rows, and so do downloads and anyone with the files."

	// The form, with what it does and does not do said before Save.
	e := got.Empty
	if strings.Join(e.Areas, ",") != `tables="",columns=""` || strings.Join(e.Buttons, ",") != "Save" ||
		!strings.Contains(all(e), does) || !strings.Contains(all(e), doesNot) || strings.Contains(all(e), "Forwarding account") {
		t.Errorf("empty server: %+v", e)
	}
	if x := got.Exposed; strings.Join(x.Areas, ",") != `tables="shop.audit_log\nshop.*",columns="shop.customers.email"` ||
		!strings.Contains(all(x), "The forwarding account can read these through MySQL:") ||
		!strings.Contains(all(x), "shop.audit_log: "+grant) || !strings.Contains(all(x), "shop.customers.email: "+grant) ||
		!strings.Contains(all(x), "What its grants cannot settle:") || !strings.Contains(all(x), "SHOW GRANTS does not list MySQL's mandatory roles") {
		t.Errorf("exposed: %s", all(x))
	}
	if x := all(got.Clean); !strings.Contains(x, "No grant line names a hidden table or column.") || strings.Contains(x, "can read these") ||
		!strings.Contains(x, "What its grants cannot settle:") {
		t.Errorf("clean grants: %s", x)
	}
	if x := all(got.NotAsked); !strings.Contains(x, "Your session may not check what the forwarding account can read.") {
		t.Errorf("not asked: %s", x)
	}
	for name, d := range map[string]hiddenDrawn{"none": got.None, "real none": got.RealNone, "no rules": got.NoRulesExposed} {
		if x := all(d); strings.Contains(x, "Forwarding account") || strings.Contains(x, "grants") {
			t.Errorf("%s: the grants check must say nothing: %s", name, x)
		}
	}
	if x := strings.Join(got.Failed.Errs, "|"); !strings.Contains(x, "Could not check what the forwarding account can read") {
		t.Errorf("failed read must be said, not silent: %+v", got.Failed)
	}
	if x := strings.Join(got.OldServer.Errs, "|"); !strings.Contains(x, "did not check its grants against these rules") {
		t.Errorf("a grants answer without the check must be said: %+v", got.OldServer)
	}
	if x := strings.Join(got.GrantsError.Errs, "|"); x != "Could not check what it can read: Error 1045: Access denied." {
		t.Errorf("grants error: %q", x)
	}
	// Rules this build cannot enforce: the red line, never an all-clear,
	// never "nothing is hidden", and the grants block still lists unsure.
	for name, d := range map[string]hiddenDrawn{"only a key": got.OnlyKey, "hand edit": got.BrokenEdit} {
		x := all(d)
		if len(d.Errs) == 0 || strings.Contains(x, "No grant line names") || strings.Contains(x, "Nothing is hidden") ||
			!strings.Contains(x, "What its grants cannot settle:") {
			t.Errorf("%s: %s", name, x)
		}
	}
	if x := strings.Join(got.OnlyKey.Errs, "|"); x != "A newer DBTrail saved rules this version cannot read (future_rule); this server's SQL is refused until you upgrade." {
		t.Errorf("only a key: %q", x)
	}
	if x := strings.Join(got.BrokenEdit.Errs, "|"); !strings.HasPrefix(x, "This server's SQL is refused: The stored hidden rules do not parse: table entry \"audit_log\"") {
		t.Errorf("hand edit: %q", x)
	}
	if !strings.Contains(strings.Join(got.ReadOnlyKey.Lines, "|"), "No table or column is listed, and the rules above still apply.") ||
		strings.Contains(all(got.ReadOnlyKey), "Nothing is hidden") {
		t.Errorf("only a key, read-only: %+v", got.ReadOnlyKey)
	}
	ks := got.OnlyKeySaved
	if len(ks.Calls) != 2 || string(ks.Calls[0][1]) != `{"method":"PUT","body":{"tables":[],"columns":[]}}` ||
		!strings.Contains(strings.Join(ks.After.Lines, "|"), "Saved.") || strings.Contains(all(ks.After), "nothing is hidden") ||
		len(ks.After.Errs) == 0 || ks.Current == nil || !ks.Current.Unreadable {
		t.Errorf("only a key, saved: %+v current=%+v", ks, ks.Current)
	}
	if sw := got.Switched; len(sw.Calls) != 1 || sw.Current == nil || strings.Join(sw.Current.Tables, ",") != "shop.audit_log,shop.*" ||
		strings.Contains(strings.Join(sw.After.Lines, "|"), "Saved") {
		t.Errorf("save while the server changed: %+v current=%+v", sw, sw.Current)
	}
	if got.Fetch404 != "gone" || got.Fetch403 != "undefined" || got.Fetch500 != "null" {
		t.Errorf("route-grants refusals: 404 %q 403 %q 500 %q", got.Fetch404, got.Fetch403, got.Fetch500)
	}
	if x := strings.Join(got.Gone.Errs, "|"); x != "This server is no longer in the list." || strings.Contains(all(got.Gone), "may not check") {
		t.Errorf("route-grants 404: %+v", got.Gone)
	}
	// A rule that matches nothing in the copy (from the real GET over the SQL
	// fixture's snapshot) is a warning; a copy that could not be read says so.
	if x := all(got.Unmatched); !strings.Contains(x, "Not in this server's newest snapshot: shop.nothere. A table or column added after that snapshot still has its changes hidden; anything else, check the spelling.") || strings.Contains(x, "shop.orders.") {
		t.Errorf("unmatched: %s", x)
	}
	if x := all(got.Exposed); !strings.Contains(x, "Could not check these rules against the copy: ") || strings.Contains(x, "These match no") {
		t.Errorf("unchecked: %s", x)
	}
	// The capture account is not a failure to read: the port does not forward.
	if c := got.Capture; len(c.Errs) != 0 || !strings.Contains(all(c), "No forwarding account: the port does not forward for this server.") {
		t.Errorf("capture account: %+v", c)
	}
	// Unsure lines are kept as written: one ending in a GRANT gets no "." added.
	if x := strings.Join(got.GrantLine.Items, "|"); !strings.Contains(x, ": GRANT FILE ON *.* TO `report_ro`@`%`|") || !strings.HasPrefix(x, "This grant") {
		t.Errorf("unsure items: %q", x)
	}
	// The command-line server: its refusal, no form.
	if b := got.Boot; len(b.Areas) != 0 || len(b.Buttons) != 0 || !strings.Contains(all(b), "The command-line server cannot hide tables or columns") {
		t.Errorf("boot: %+v", b)
	}
	if x := strings.Join(got.Unreadable.Errs, "|"); x != "A newer DBTrail saved rules this version cannot read (future_rule); this server's SQL is refused until you upgrade." {
		t.Errorf("unreadable keys: %q", x)
	}
	if x := all(got.UnreadableGrants); strings.Contains(x, "No grant line names") || !strings.Contains(x, "What its grants cannot settle:") ||
		!strings.Contains(strings.Join(got.UnreadableGrants.Errs, "|"), "cannot read (future_rule)") {
		t.Errorf("unreadable rules with a grants check must not read as clear: %s", x)
	}
	if !strings.Contains(strings.Join(got.UnreadNull.Errs, "|"), "Could not read this server's hidden tables and columns") {
		t.Errorf("unreadable GET: %+v", got.UnreadNull)
	}
	if r := got.ReadOnly; len(r.Areas) != 0 || len(r.Buttons) != 0 || !strings.Contains(all(r), "Tables: shop.audit_log, shop.*") ||
		!strings.Contains(all(r), "Columns: shop.customers.email") || !strings.Contains(all(r), "Someone who can change servers can edit this.") {
		t.Errorf("read-only session: %+v", r)
	}

	// Save sends both lists, trimmed, blank lines dropped, then reads the
	// grants check again and repaints from the server's answer.
	s := got.Saved
	if len(s.Calls) != 2 || string(s.Calls[0][0]) != `"/api/servers/s1/hidden"` ||
		string(s.Calls[0][1]) != `{"method":"PUT","body":{"tables":["shop.audit_log","shop.*"],"columns":["shop.customers.email"]}}` ||
		string(s.Calls[1][0]) != `"/api/servers/s1/route-grants"` {
		t.Errorf("save calls: %s", s.Calls)
	}
	if !strings.Contains(strings.Join(s.After.Lines, "|"), "Saved.") || !strings.Contains(all(s.After), "can read these through MySQL") ||
		s.Current == nil || strings.Join(s.Current.Tables, ",") != "shop.audit_log,shop.*" || s.Entry == nil {
		t.Errorf("after save: %+v current=%+v entry=%+v", s.After, s.Current, s.Entry)
	}
	c := got.Cleared
	if len(c.Calls) != 2 || string(c.Calls[0][1]) != `{"method":"PUT","body":{"tables":[],"columns":[]}}` || c.Current != nil ||
		!strings.Contains(strings.Join(c.After.Lines, "|"), "Saved: nothing is hidden on this server now.") ||
		strings.Contains(all(c.After), "Forwarding account") {
		t.Errorf("clear: %+v current=%+v", c, c.Current)
	}
	if r := got.Refused; len(r.Calls) != 1 || strings.Join(r.After.Errs, "|") != data["refusal"] || strings.Join(r.After.Buttons, ",") != "Save" {
		t.Errorf("refused: %+v, want the server's words %q and Save on again", r, data["refusal"])
	}

	// The notes: near SQL on the copy, what is hidden; near each download,
	// that the download includes it. Nothing on a server with no rules.
	if strings.Join(got.SQLNote, "|") != "This server hides some tables and columns from SQL: shop.audit_log, shop.*, shop.customers.email. Change it in Settings, MySQL clients." ||
		len(got.SQLNotePlain) != 0 {
		t.Errorf("SQL note: %q / plain %q", got.SQLNote, got.SQLNotePlain)
	}
	note := "This server hides some tables and columns from SQL; this download includes them."
	for name, v := range map[string][]string{"laptop": got.Laptop, "duck lane": got.DuckHidden, "sql lane": got.SQLLaneHidden,
		"versions row": got.VersionsHidden, "duck lane, only a key": got.DuckOnlyKey, "duck lane, no entry passed": got.DuckNoCur} {
		if strings.Join(v, "|") != note {
			t.Errorf("%s note: %q", name, v)
		}
	}
	file := "This server hides some tables and columns from SQL; this file reads them too."
	if strings.Join(got.LaptopViewsOnly.Lines, "|") != file || strings.Join(got.LaptopViewsOnly.Buttons, ",") != "Download views.sql" {
		t.Errorf("views.sql only: %+v", got.LaptopViewsOnly)
	}
	if strings.Join(got.Dash, "|") != file {
		t.Errorf("dashboards file: %q", got.Dash)
	}
	if strings.Join(got.InPlaceHidden, "|") != "This server hides some tables and columns from SQL; the files and the export below include them." || len(got.InPlacePlain) != 0 {
		t.Errorf("use it where it lives: %q / plain %q", got.InPlaceHidden, got.InPlacePlain)
	}
	if strings.Join(got.SQLNoteOnlyKey, "|") != "This server holds hidden rules this DBTrail cannot read, so its SQL is refused. Change it in Settings, MySQL clients." {
		t.Errorf("SQL note, only a key: %q", got.SQLNoteOnlyKey)
	}
	if strings.Join(got.SQLNoteNoWrite, "|") != "This server holds hidden rules this DBTrail cannot read, so its SQL is refused." {
		t.Errorf("SQL note for a session that may not change servers: %q", got.SQLNoteNoWrite)
	}
	if len(got.StaleBefore) != 0 || len(got.StaleAfter) != 1 {
		t.Errorf("SQL note must follow the rules read when the panel opens: before %q after %q", got.StaleBefore, got.StaleAfter)
	}
	if len(got.DuckPlain)+len(got.SQLLanePlain)+len(got.VersionsPlain) != 0 {
		t.Errorf("no rules, no note: duck %q sql %q versions %q", got.DuckPlain, got.SQLLanePlain, got.VersionsPlain)
	}
}
