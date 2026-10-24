package console

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
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
  out.unreadNull = read(body(null, U));
  caps({ "servers:write": false });
  out.readOnly = read(body(d.withRules, d.grantsClean));
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

  // The notes beside SQL on the copy and the downloads.
  run("rememberHiddenRules(" + JSON.stringify([{ id: "s1", hidden: { tables: d.withRules.tables, columns: d.withRules.columns } }, { id: "s2" }]) + ")");
  const box2 = () => run("document.createElement('div')");
  const sqlBox = box2(); ctx.__sb = sqlBox; run("renderSQLPanel(__sb)");
  out.sqlNote = read(sqlBox).lines.filter((l) => l.includes("hides"));
  const lap = box2(); ctx.__lap = lap; run('USE_PANELS.laptop(__lap, { cta: "ok", snap: { time: "2026-10-24T10:00:00Z" } })');
  out.laptop = read(lap).lines.filter((l) => l.includes("hides"));
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
  // The Download on a row of Versions.
  const detail = async () => { const bx = box2(); ctx.__dx = bx; await run('loadBackupDetail("2026-10-24T10:00:00Z", __dx)'); return read(bx).lines.filter((l) => l.includes("hides")); };
  ctx.__answers["/api/baselines/files?at=2026-10-24T10%3A00%3A00Z"] = { total_bytes: 2048, tables: [{ schema: "shop", table: "orders", files: 1, bytes: 2048 }], files: 1 };
  out.versionsHidden = await detail();
  run('currentServer = "s2";');
  out.versionsPlain = await detail();
  const sqlBox2 = box2(); ctx.__sb2 = sqlBox2; run("renderSQLPanel(__sb2)");
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

	// A newer build's key under hidden:, read by this build.
	path := t.TempDir() + "/console-servers.yaml"
	if err := os.WriteFile(path, []byte("version: 1\nservers:\n- id: s1\n  name: prod\n  dsn: u:p@tcp(h:3306)/db\n"+
		"  hidden:\n    tables:\n    - shop.audit_log\n    future_rule: keep-me\n"), 0o600); err != nil {
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
	_, body = doServersReq(t, fut, "GET", "/api/servers/s1/hidden", "")
	raw("unreadable", body)

	// The grants check, in the shape GET /api/servers/{id}/route-grants
	// answers on a server with rules (field hidden: exposed and unsure).
	const always = "SHOW GRANTS does not list MySQL's mandatory roles or MariaDB's grants to PUBLIC: " +
		"check @@mandatory_roles and SHOW GRANTS FOR PUBLIC for anything that reads what this server hides"
	grant := "GRANT SELECT ON `shop`.* TO `report_ro`@`%`"
	data["grantsExposed"] = map[string]any{"user": "report_ro", "grants": []string{grant},
		"hidden": map[string]any{"exposed": []string{"shop.audit_log: " + grant, "shop.customers.email: " + grant}, "unsure": []string{always}}}
	data["grantsClean"] = map[string]any{"user": "report_ro", "grants": []string{"GRANT USAGE ON *.* TO `report_ro`@`%`"},
		"hidden": map[string]any{"unsure": []string{always}}}
	data["grantsNone"] = map[string]any{"none": "no forwarding account is set for this server"}
	data["grantsOld"] = map[string]any{"user": "report_ro", "grants": []string{grant}}
	data["grantsError"] = map[string]any{"user": "report_ro", "error": "Error 1045: Access denied"}

	arg, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	outRaw := runNodeConnectArgs(t, renderHarnessJS+hiddenRulesJS, string(arg))
	var got struct {
		Empty, Exposed, Clean, None, Failed, NotAsked, NoRulesExposed, OldServer, GrantsError, Boot, Unreadable, UnreadNull, ReadOnly hiddenDrawn
		Saved                                                                                                                         struct {
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
	does := "Hidden tables and columns disappear from SQL on the copy, the MySQL port and the AI connector, and the port's time travel (_flashback, _snapshot, _diff) is off for this server."
	doesNot := "The data stays in the copy's files and in the index, so time travel in this console (it follows its own access profiles), downloads, and anyone with the files still see it."

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
	if x := all(got.Clean); !strings.Contains(x, "Its grants do not let it read what this server hides.") || strings.Contains(x, "can read these") ||
		!strings.Contains(x, "What its grants cannot settle:") {
		t.Errorf("clean grants: %s", x)
	}
	for name, d := range map[string]hiddenDrawn{"none": got.None, "not asked": got.NotAsked, "no rules": got.NoRulesExposed} {
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
	// The command-line server: its refusal, no form.
	if b := got.Boot; len(b.Areas) != 0 || len(b.Buttons) != 0 || !strings.Contains(all(b), "The command-line server cannot hide tables or columns") {
		t.Errorf("boot: %+v", b)
	}
	if x := strings.Join(got.Unreadable.Errs, "|"); x != "A newer DBTrail saved rules this version cannot read (future_rule); this server's SQL is refused until you upgrade." {
		t.Errorf("unreadable keys: %q", x)
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
	for name, v := range map[string][]string{"laptop": got.Laptop, "duck lane": got.DuckHidden, "sql lane": got.SQLLaneHidden, "versions row": got.VersionsHidden} {
		if strings.Join(v, "|") != note {
			t.Errorf("%s note: %q", name, v)
		}
	}
	if len(got.DuckPlain)+len(got.SQLLanePlain)+len(got.VersionsPlain) != 0 {
		t.Errorf("no rules, no note: duck %q sql %q versions %q", got.DuckPlain, got.SQLLanePlain, got.VersionsPlain)
	}
}
