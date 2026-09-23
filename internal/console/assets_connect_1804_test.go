package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/status"
)

// runNodeConnect runs script with node: argv[2] is app.js, argv[3] the
// first-run scoreboard module (its banned-word list), so every sentence the
// Connect screen shows is checked against the same list the walk uses.
func runNodeConnect(t *testing.T, script string) []byte {
	t.Helper()
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
	board, err := filepath.Abs("../../test/console-e2e/first_run_scoreboard.mjs")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "connect.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, board).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	return []byte(lines[len(lines)-1])
}

// TestConnectFindingsSayEveryKind: every finding kind the check can send is
// said in plain words, with the fix it carries, and a kind the page does not
// know falls back to the check's own card instead of vanishing. The kinds come
// from doctor.Kinds(), so a tenth kind added there fails here until the screen
// says it. The table statements are the Overview card's real FixSQL output.
func TestConnectFindingsSayEveryKind(t *testing.T) {
	pkStmt := status.UncapturedTable{Schema: "shop", Table: "audit_log", Reason: metadata.ExclusionReasonNoPrimaryKey, PKColumn: "id"}.FixSQL()
	engStmt := status.UncapturedTable{Schema: "shop", Table: "legacy", Reason: metadata.ExclusionReasonNotInnoDB, PKColumn: ""}.FixSQL()
	if !strings.HasPrefix(pkStmt, "ALTER TABLE `shop`.`audit_log`") {
		t.Fatalf("setup: FixSQL gave %q; the reason string no longer maps to a key statement", pkStmt)
	}
	if !strings.Contains(engStmt, "ENGINE=InnoDB") {
		t.Fatalf("setup: FixSQL gave %q; the reason string no longer maps to an engine statement", engStmt)
	}
	checks := map[string]DoctorCheck{
		doctor.KindHostUnreachable: {Status: "fail"},
		doctor.KindPortClosed:      {Status: "fail"},
		doctor.KindTimeout:         {Status: "fail"},
		doctor.KindAccessDenied:    {Status: "fail", Detail: "Error 1045 (28000): Access denied for user 'root'@'10.0.0.9' (using password: YES)"},
		doctor.KindLoopbackInContainer: {Status: "fail", Remediation: "Nothing answered at 127.0.0.1:3306, but a MySQL server answered at host.docker.internal:3306.\n\n" +
			"If DBTrail runs in a container, localhost is the container itself, and that server is your machine. Use this as the host:\n\n  host.docker.internal\n\n" +
			"If DBTrail does not run in a container, that address belongs to another machine. Check the address of your own database instead."},
		doctor.KindMissingPrivilege: {Status: "fail", Subjects: []string{"REPLICATION CLIENT"},
			Remediation: "Run on the source MySQL as a privileged user (e.g. root):\n\n  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'10.0.%';\n  FLUSH PRIVILEGES;\n\nREPLICATION SLAVE lets bintrail stream binlog events."},
		doctor.KindBinlogSettings: {Status: "fail", Subjects: []string{"binlog_format"},
			Remediation: "Set on the source MySQL (MySQL 8.0+ — survives restart without editing my.cnf):\n\n  SET PERSIST binlog_format = 'ROW';\n\nOn MySQL 5.7 use SET GLOBAL and also add to my.cnf:\n\n  [mysqld]\n  binlog_format = ROW"},
		// Two tables, the second with no statement: it must be named, not dropped.
		doctor.KindNoPrimaryKey: {Status: "fail", Subjects: []string{"shop.audit_log", "shop.odd"}, Statements: []string{pkStmt, ""}},
		doctor.KindNotInnoDB:    {Status: "warn", Subjects: []string{"shop.legacy"}, Statements: []string{engStmt}},
	}
	for _, k := range doctor.Kinds() {
		c, ok := checks[k]
		if !ok {
			t.Fatalf("kind %q has no case in this test; add one with the data it carries", k)
		}
		c.Kind = k
		checks[k] = c
	}
	checks["unknown"] = DoctorCheck{Kind: "", Status: "fail", Name: "Something new"}
	// A privilege fix with no code block still shows the fix, never a colon
	// with nothing under it.
	checks["privNoCode"] = DoctorCheck{Kind: doctor.KindMissingPrivilege, Status: "fail", Subjects: []string{"SELECT"}, Remediation: "Ask your DBA for SELECT."}
	in, err := json.Marshal(checks)
	if err != nil {
		t.Fatal(err)
	}
	js := readAsset(t, "app.js")
	script := functionBody(t, js, "function remediationBlocks(") + "\n" +
		functionBody(t, js, "function codeOf(") + "\n" +
		functionBody(t, js, "function connectFindingParts(") + `
(async () => {
  const { bannedHits, countWords } = await import(process.argv[3]);
  const out = {};
  for (const [k, c] of Object.entries(` + string(in) + `)) {
    const p = connectFindingParts(c);
    out[k] = p ? { ...p, banned: bannedHits(p.text + " " + (p.note || "")).map((h) => h.word), words: countWords(p.text + " " + (p.note || "")) } : null;
  }
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`
	var got map[string]*struct {
		Text, Code, Rem, Note string
		Banned                []string
		Words                 int
	}
	raw := runNodeConnect(t, script)
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	for _, k := range doctor.Kinds() {
		p := got[k]
		if p == nil || p.Text == "" {
			t.Errorf("%s: no plain-words sentence", k)
			continue
		}
		t.Logf("%s: %s | %s | %s", k, p.Text, p.Code, p.Note)
		if len(p.Banned) > 0 {
			t.Errorf("%s: the sentence uses words the first run bans: %v in %q", k, p.Banned, p.Text)
		}
		if strings.Contains(p.Text, "—") {
			t.Errorf("%s: em dash in %q", k, p.Text)
		}
		if p.Words > 40 {
			t.Errorf("%s: %d words, over the 40 a step allows: %q", k, p.Words, p.Text)
		}
	}
	if got["unknown"] != nil {
		t.Errorf("a kind the page does not know was given a made-up sentence instead of the check's own card: %+v", got["unknown"])
	}
	// The fix each kind carries.
	if c := got[doctor.KindMissingPrivilege].Code; !strings.Contains(c, "TO 'repl'@'10.0.%'") {
		t.Errorf("the GRANT is not the check's own, for the account SHOW GRANTS named: %q", c)
	}
	if !strings.Contains(got[doctor.KindMissingPrivilege].Text, "REPLICATION CLIENT") {
		t.Errorf("the missing permission is not named: %q", got[doctor.KindMissingPrivilege].Text)
	}
	if c := got[doctor.KindBinlogSettings].Code; c != "SET PERSIST binlog_format = 'ROW';" {
		t.Errorf("binlog_format fix missing: %q", c)
	}
	if !strings.Contains(got[doctor.KindBinlogSettings].Text, "binlog_format must be ROW") {
		t.Errorf("binlog_format not named: %q", got[doctor.KindBinlogSettings].Text)
	}
	if p := got[doctor.KindNoPrimaryKey]; p.Code != pkStmt || !strings.Contains(p.Note, "shop.odd") {
		t.Errorf("no primary key: code %q (want the card's %q), note %q (want shop.odd named)", p.Code, pkStmt, p.Note)
	}
	if p := got[doctor.KindNotInnoDB]; p.Code != engStmt {
		t.Errorf("not InnoDB: code %q, want %q", p.Code, engStmt)
	}
	if !strings.Contains(got[doctor.KindAccessDenied].Note, "using password: YES") {
		t.Errorf("what MySQL said is dropped from the access-denied card: %+v", got[doctor.KindAccessDenied])
	}
	if p := got["privNoCode"]; p == nil || p.Code != "" || p.Rem != "Ask your DBA for SELECT." || strings.HasSuffix(p.Text, ":") {
		t.Errorf("a privilege fix without code: %+v", p)
	}
	if !strings.Contains(got[doctor.KindLoopbackInContainer].Rem, "host.docker.internal") {
		t.Errorf("the address found on this machine is not shown: %+v", got[doctor.KindLoopbackInContainer])
	}
}

// connectHarnessJS drives the REAL Connect screen in the render harness:
// showServerForm, the draft save, the check and Cancel, with api() recorded.
const connectHarnessJS = `
ctx.crypto = require("crypto").webcrypto;
const walk = (n, f) => { f(n); for (const c of n.children || []) if (c && c.nodeType === 1) walk(c, f); };
const matches = (n, sel) => {
  const m = /^([a-z]*)(?:#([\w-]+))?(?:\.([\w-]+))?(?:\[([\w-]+)(?:=([\w-]+))?\])?$/.exec(sel);
  if (!m) throw new Error("harness selector not supported: " + sel);
  const [, tag, id, cls, attr, val] = m;
  if (tag && n.tag !== tag) return false;
  if (id && n.attrs.id !== id) return false;
  if (cls && !(" " + n.className + " ").includes(" " + cls + " ")) return false;
  if (attr && (n.attrs[attr] === undefined || (val !== undefined && n.attrs[attr] !== val))) return false;
  return true;
};
FakeEl.prototype.querySelectorAll = function (sel) { const out = []; for (const c of this.children) if (c && c.nodeType === 1) walk(c, (n) => { if (matches(n, sel)) out.push(n); }); return out; };
FakeEl.prototype.querySelector = function (sel) { return this.querySelectorAll(sel)[0] || null; };
const setAttr = FakeEl.prototype.setAttribute;
FakeEl.prototype.setAttribute = function (k, v) { setAttr.call(this, k, v); if (k === "hidden") this.hidden = true; if (k === "placeholder") this.placeholder = v; if (k.startsWith("data-")) this.dataset[k.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = String(v); };
FakeEl.prototype.addEventListener = function (type, fn) { (this.__h ||= {})[type] = [...((this.__h || {})[type] || []), fn]; };
FakeEl.prototype.fire = function (type) { for (const fn of (this.__h && this.__h[type]) || []) fn({ preventDefault() {} }); };
FakeEl.prototype.focus = function () { focused = this.attrs.name || this.attrs.id; };
FakeEl.prototype.select = function () {};
Object.defineProperty(FakeEl.prototype, "isConnected", { get() { let n = this; while (n) { if (n === mount) return true; n = n.__parent; } return false; } });
const origAppend = FakeEl.prototype.append;
FakeEl.prototype.append = function (...k) { for (const x of k) if (x && typeof x === "object") x.__parent = this; return origAppend.apply(this, k); };
const origReplace = FakeEl.prototype.replaceChildren;
FakeEl.prototype.replaceChildren = function (...k) { for (const x of this.children) if (x && typeof x === "object") x.__parent = null; for (const x of k) if (x && typeof x === "object") x.__parent = this; return origReplace.apply(this, k); };
Object.defineProperty(FakeEl.prototype, "elements", { get() { const e = {}; walk(this, (n) => { if (n.attrs.name) e[n.attrs.name] = n; }); return e; } });
let focused = null;
const mount = new FakeEl("div"), wrap = new FakeEl("div");
document.getElementById = (id) => id === "server-form-mount" ? mount : id === "server-add-wrap" ? wrap : id === "server-form" ? (mount.children[0] || null) : null;
ctx.setTimeout = (fn) => { fn(); return 1; };
const form = () => mount.children[0];
const calls = [];
let putGate = null;
ctx.__api = async (path, opts) => {
  const method = (opts && opts.method) || "GET";
  calls.push(method + " " + path + (opts && opts.body ? " " + JSON.stringify(opts.body) : ""));
  if (method === "PUT") { if (putGate) await putGate; calls.push("PUT done"); return { found: true, auto_name: (opts.body.source_host || "x") + "-auto" }; }
  if (path === "/api/servers/check") return ctx.__checkAnswer;
  if (method === "GET" && path === "/api/servers/draft") return ctx.__draftAnswer;
  return {};
};
const notices = [], toasts = [];
ctx.__notice = (n) => notices.push(n.title);
ctx.__toast = (t) => toasts.push(t);
vm.runInContext("api = (p, o) => __api(p, o); refreshServersList = async () => {}; toast = (t) => __toast(t); toastError = (t) => __toast('ERR ' + t); formMsg = () => {}; openNotice = (n) => __notice(n); openServersModal = () => {};", ctx);
const setCaps = (caps) => vm.runInContext("capsCache = " + JSON.stringify(caps) + ";", ctx);
const show = (caps) => { setCaps(caps); vm.runInContext("showServerForm(null)", ctx); return form(); };
const texts = (f) => { const out = []; walk(f, (n) => { if (n.tag !== "pre" && n._text) out.push(n._text); if (n.attrs && n.attrs.placeholder) out.push(n.attrs.placeholder); }); return out; };
const flush = () => new Promise((r) => setImmediate(r));
(async () => {
  const { bannedHits } = await import(process.argv[3]);
  const out = {};
  let f = show({ monitor: true });
  out.fresh = { connect: f.attrs["data-connect"] === "1", user: f.elements.source_user.value, pwLen: f.elements.source_password.value.length,
    pwAgainHidden: !!f.querySelector("p#connect-pw-again").hidden, focused,
    fields: Object.keys(f.elements).filter((k) => !["id", "flavor"].includes(k)).sort(),
    banned: texts(f).flatMap((t) => bannedHits(t).map((h) => h.word + " in: " + t)),
    submit: f.querySelector("button[type=submit]").textContent };
  // Typing saves the draft: no password in it, the name only as typed, and
  // the answer's automatic name becomes the placeholder.
  calls.length = 0;
  f.elements.source_host.value = "db1"; f.elements.source_host.fire("input");
  await flush(); await flush();
  out.draftPut = calls.find((c) => c.startsWith("PUT")) || "";
  out.placeholder = f.elements.name.placeholder;
  // A check with no password left is refused in the page.
  f.elements.source_password.value = "";
  calls.length = 0;
  f.fire("submit"); await flush();
  out.noPasswordCalls = calls.slice();
  // A draft save still in flight is awaited before the check is sent.
  f.elements.source_password.value = "Pw-1";
  let open; putGate = new Promise((r) => { open = r; });
  calls.length = 0;
  f.elements.source_user.value = "alice"; f.elements.source_user.fire("input");
  ctx.__checkAnswer = { ok: false, name: "db1", doctor: { checks: [{ name: "Source MySQL connection", status: "fail", kind: "access_denied" }], failed: 1 } };
  f.fire("submit"); await flush();
  out.checkBeforePutDone = calls.some((c) => c.startsWith("POST /api/servers/check"));
  out.cancelOffDuringCheck = !!f.querySelector("button#server-cancel").disabled;
  open(); putGate = null; await flush(); await flush(); await flush();
  out.order = calls.map((c) => c.split(" {")[0]);
  out.checkBody = JSON.parse((calls.find((c) => c.startsWith("POST /api/servers/check")) || "x {}").slice("POST /api/servers/check ".length));
  out.failNotice = notices.slice();
  out.formStillThere = !!form();
  // A started check closes the screen.
  ctx.__checkAnswer = { ok: true, started: true, name: "db1", doctor: { checks: [], warnings: 0 } };
  f.fire("submit"); await flush(); await flush(); await flush();
  out.startedClosed = !form();
  out.toasts = toasts.slice();
  // A restored draft: no password generated, and the screen asks for it.
  setCaps({ monitor: true });
  vm.runInContext("showConnectForm({ name: '', source_host: 'db', source_port: '3307', source_user: 'alice', auto_name: 'db-3307' })", ctx);
  f = form();
  out.restored = { pw: f.elements.source_password.value, user: f.elements.source_user.value, name: f.elements.name.value, placeholder: f.elements.name.placeholder,
    pwAgainShown: !f.querySelector("p#connect-pw-again").hidden, focused, block: f.querySelector("pre[data-grant=mysql]").textContent.split("\n")[0] };
  // Cancel waits for a pending save, then deletes the draft.
  putGate = new Promise((r) => { open = r; });
  calls.length = 0;
  f.elements.source_host.value = "db2"; f.elements.source_host.fire("input");
  await flush(); // the save is on the wire, held open by putGate
  f.querySelector("button#server-cancel").fire("click"); await flush();
  out.deleteBeforePutDone = calls.some((c) => c.startsWith("DELETE"));
  open(); putGate = null; await flush(); await flush(); await flush();
  out.cancelOrder = calls.map((c) => c.split(" {")[0]);
  // A save still queued when Cancel is pressed is never sent: sent after the
  // DELETE, it would bring the form back on the next page load.
  setCaps({ monitor: true });
  vm.runInContext("showConnectForm(null)", ctx);
  f = form();
  calls.length = 0;
  f.elements.source_host.value = "db3"; f.elements.source_host.fire("input");
  f.querySelector("button#server-cancel").fire("click");
  await flush(); await flush(); await flush();
  out.queuedAfterCancel = calls.map((c) => c.split(" {")[0]);
  // Restore after reload: only for a session that may add servers.
  calls.length = 0;
  ctx.__draftAnswer = { found: true, draft: { source_host: "db9" }, auto_name: "db9" };
  setCaps({ monitor: true, permissions: { "servers:write": false } });
  await vm.runInContext("restoreConnectDraft()", ctx);
  out.readOnlyRestoreCalls = calls.slice();
  mount.replaceChildren();
  setCaps({ monitor: true });
  await vm.runInContext("restoreConnectDraft()", ctx);
  out.restoredHost = form() ? form().elements.source_host.value : null;
  // The link to the long form carries what was typed and drops the draft.
  calls.length = 0;
  f = form();
  f.elements.source_port.value = "3310";
  f.querySelector("button#connect-full-form").fire("click"); await flush(); await flush();
  out.full = { long: !!form() && form().attrs["data-connect"] === undefined && !!form().elements.host,
    host: form() ? form().elements.source_host.value : null, port: form() ? form().elements.source_port.value : null,
    deleted: calls.some((c) => c.startsWith("DELETE /api/servers/draft")) };
  // A console that only reads an index keeps the long form for an add.
  mount.replaceChildren();
  f = show({ monitor: false });
  out.serveIsLongForm = f.attrs["data-connect"] === undefined && !!f.elements.host;
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

func TestConnectScreenWiring(t *testing.T) {
	raw := runNodeConnect(t, renderHarnessJS+connectHarnessJS)
	var out struct {
		Fresh struct {
			Connect, PwAgainHidden bool
			User, Focused, Submit  string
			PwLen                  int
			Fields, Banned         []string
		}
		DraftPut, Placeholder              string
		NoPasswordCalls, Order, FailNotice []string
		CheckBeforePutDone, FormStillThere bool
		CheckBody                          map[string]any
		StartedClosed                      bool
		Toasts                             []string
		Restored                           struct {
			Pw, User, Name, Placeholder, Focused, Block string
			PwAgainShown                                bool
		}
		DeleteBeforePutDone, CancelOffDuringCheck            bool
		CancelOrder, ReadOnlyRestoreCalls, QueuedAfterCancel []string
		RestoredHost                                         *string
		ServeIsLongForm                                      bool
		Full                                                 struct {
			Long, Deleted bool
			Host, Port    *string
		}
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	t.Logf("%+v", out)

	f := out.Fresh
	if !f.Connect {
		t.Fatal("a new server on a process that captures did not open the Connect screen")
	}
	if strings.Join(f.Fields, ",") != "name,source_host,source_password,source_port,source_user" {
		t.Errorf("Connect fields = %v, want host, port, user, password and name", f.Fields)
	}
	if f.User != "dbtrail" || f.PwLen < 20 || !f.PwAgainHidden || f.Focused != "source_host" {
		t.Errorf("fresh screen: %+v", f)
	}
	if len(f.Banned) > 0 {
		t.Errorf("the screen uses words the first run bans: %v", f.Banned)
	}
	if f.Submit != "Check and connect" {
		t.Errorf("button = %q", f.Submit)
	}

	if !strings.HasPrefix(out.DraftPut, "PUT /api/servers/draft ") {
		t.Fatalf("typing did not save the draft: %q", out.DraftPut)
	}
	if strings.Contains(out.DraftPut, "source_password") || strings.Contains(out.DraftPut, "Pw-") {
		t.Errorf("the draft save sends the password: %s", out.DraftPut)
	}
	if !strings.Contains(out.DraftPut, `"name":""`) || !strings.Contains(out.DraftPut, `"flavor":"mysql"`) {
		t.Errorf("the draft save must send the name as typed (empty) and the flavor: %s", out.DraftPut)
	}
	if out.Placeholder != "db1-auto" {
		t.Errorf("the name placeholder is not the automatic name the server answered: %q", out.Placeholder)
	}

	for _, c := range out.NoPasswordCalls {
		if strings.Contains(c, "/api/servers/check") {
			t.Errorf("a check with no password was sent: %v", out.NoPasswordCalls)
		}
	}
	if out.CheckBeforePutDone {
		t.Errorf("the check went out while a draft save was in flight; a late save could bring back a finished form: %v", out.Order)
	}
	iPut, iCheck := -1, -1
	for i, c := range out.Order {
		if c == "PUT done" && iPut < 0 {
			iPut = i
		}
		if strings.HasPrefix(c, "POST /api/servers/check") {
			iCheck = i
		}
	}
	if iPut < 0 || iCheck < 0 || iCheck < iPut {
		t.Errorf("order = %v; want the draft save finished before the check", out.Order)
	}
	if out.CheckBody["source_password"] != "Pw-1" || out.CheckBody["name"] != "" || out.CheckBody["source_user"] != "alice" {
		t.Errorf("check body = %v; want the typed password, the typed (empty) name, the typed user", out.CheckBody)
	}
	if len(out.FailNotice) != 1 || out.FailNotice[0] != "Capture did not start" || !out.FormStillThere {
		t.Errorf("a failed check: notices %v, form kept %v", out.FailNotice, out.FormStillThere)
	}
	if !out.StartedClosed || len(out.Toasts) == 0 || !strings.Contains(out.Toasts[len(out.Toasts)-1], "Capture started for db1") {
		t.Errorf("a started check: closed %v, toasts %v", out.StartedClosed, out.Toasts)
	}

	r := out.Restored
	if r.Pw != "" {
		t.Errorf("a restored form filled in a password (%d chars); the account was created with the old one", len(r.Pw))
	}
	if !r.PwAgainShown || r.Focused != "source_password" || r.User != "alice" || r.Name != "" || r.Placeholder != "db-3307" {
		t.Errorf("restored form: %+v", r)
	}
	if !strings.HasPrefix(r.Block, "--") {
		t.Errorf("with no password the block must have nothing runnable, first line %q", r.Block)
	}
	if !out.CancelOffDuringCheck {
		t.Error("Cancel stays usable while a check runs; it cannot stop a start already on its way")
	}
	if out.DeleteBeforePutDone {
		t.Errorf("Cancel deleted the draft while a save was in flight: %v", out.CancelOrder)
	}
	if n := len(out.CancelOrder); n == 0 || !strings.HasPrefix(out.CancelOrder[n-1], "DELETE /api/servers/draft") {
		t.Errorf("Cancel order = %v; want the draft deleted last", out.CancelOrder)
	}
	if strings.Join(out.QueuedAfterCancel, ",") != "DELETE /api/servers/draft" {
		t.Errorf("after Cancel with a save queued: %v; want the DELETE alone", out.QueuedAfterCancel)
	}
	if len(out.ReadOnlyRestoreCalls) != 0 {
		t.Errorf("a session that may not add servers asked for the draft: %v", out.ReadOnlyRestoreCalls)
	}
	if out.RestoredHost == nil || *out.RestoredHost != "db9" {
		t.Errorf("the saved draft was not opened again after a reload: %v", out.RestoredHost)
	}
	if !out.Full.Long || !out.Full.Deleted || out.Full.Host == nil || *out.Full.Host != "db9" || *out.Full.Port != "3310" {
		t.Errorf("the link to the long form: %+v (want the long form, the typed host and port carried, the draft deleted)", out.Full)
	}
	if !out.ServeIsLongForm {
		t.Error("a console that only reads an index lost the long add form")
	}
}
