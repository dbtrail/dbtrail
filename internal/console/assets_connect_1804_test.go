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
	return runNodeConnectArgs(t, script)
}

// runNodeConnectArgs is runNodeConnect with extra arguments after the two
// paths (argv[4] onward), for a script fed data the test produced.
func runNodeConnectArgs(t *testing.T, script string, extra ...string) []byte {
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
	raw, err := exec.Command(node, append([]string{path, appJS, board}, extra...)...).CombinedOutput()
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
ctx.clearTimeout = () => {};
const form = () => mount.children[0];
const calls = [];
let putGate = null, checkGate = null;
ctx.__api = async (path, opts) => {
  const method = (opts && opts.method) || "GET";
  calls.push(method + " " + path + (opts && opts.body ? " " + JSON.stringify(opts.body) : ""));
  if (method === "PUT") { if (putGate) await putGate; calls.push("PUT done"); return { found: true, auto_name: (opts.body.source_host || "x") + "-auto" }; }
  if (path === "/api/servers/identify") return ctx.__identifyAnswer;
  if (path === "/api/servers/check") { if (checkGate) await checkGate; if (ctx.__checkThrows) throw new Error(ctx.__checkThrows); return ctx.__checkAnswer; }
  if (method === "GET" && path === "/api/servers/draft") return ctx.__draftAnswer;
  return {};
};
const notices = [], toasts = [], scheduled = [];
let serverLists = 0;
ctx.__notice = (n) => notices.push(n.title + " | " + (n.tone || "") + " | " + (n.summary || ""));
ctx.__toast = (t) => toasts.push(t);
ctx.__scheduled = scheduled;
ctx.__lists = () => { serverLists++; };
vm.runInContext("api = (p, o) => __api(p, o); refreshServersList = async () => { __lists(); }; toast = (t) => __toast(t); toastError = (t) => __toast('ERR ' + t); formMsg = () => {}; openNotice = (n) => __notice(n); openServersModal = () => {}; connectSchedule = (fn) => { __scheduled.push(fn); return __scheduled.length; };", ctx);
const setCaps = (caps) => vm.runInContext("capsCache = " + JSON.stringify(caps) + ";", ctx);
const show = (caps) => { setCaps(caps); vm.runInContext("showServerForm(null)", ctx); return form(); };
const texts = (f) => { const out = []; walk(f, (n) => { if (n.tag !== "pre" && n._text) out.push(n._text); if (n.attrs && n.attrs.placeholder) out.push(n.attrs.placeholder); if (n.attrs && n.attrs["aria-label"]) out.push(n.attrs["aria-label"]); }); return out; };
const flush = async (n = 1) => { for (let i = 0; i < n; i++) await new Promise((r) => setImmediate(r)); };
const step = (f) => f.dataset.step;
const shown = (f, n) => !f.querySelector("div[data-cx-step=" + n + "]").hidden;
const button = (f) => f.querySelector("button[type=submit]").textContent;
const lights = (f) => f.querySelector("ol#connect-lights").children.map((li) => li.className.replace("cx-light ", "") + ":" + li.children[1]._text);
const result = (f) => { const t = []; walk(f.querySelector("div#connect-result"), (n) => { if (n.tag === "p" && n._text) t.push(n._text); }); return t; };
const identified = (f) => { const t = []; walk(f.querySelector("div#connect-found"), (n) => { if ((n.tag === "strong" || n.tag === "p") && n._text) t.push(n._text); }); return t; };
const mariaRDS = { addr: "db1:3306", version: "10.11.6-MariaDB-log", flavor: "mariadb", managed: "rds" };
// toStep2 opens a fresh screen and identifies the answer given.
const toStep2 = async (host, answer) => {
  setCaps({ monitor: true });
  vm.runInContext("showConnectForm(null)", ctx);
  const f = form();
  f.elements.source_host.value = host; f.elements.source_host.fire("input");
  ctx.__identifyAnswer = answer;
  f.fire("submit"); await flush(4);
  return f;
};
(async () => {
  const { bannedHits } = await import(process.argv[3]);
  const out = {};
  let f = show({ monitor: true });
  out.fresh = { connect: f.attrs["data-connect"] === "1", step: step(f), focused, submit: button(f),
    step2Hidden: !shown(f, 2), step3Hidden: !shown(f, 3), pwAgainHidden: !!f.querySelector("p#connect-pw-again").hidden,
    fields: Object.keys(f.elements).filter((k) => !["id", "flavor"].includes(k)).sort(),
    banned: texts(f).flatMap((t) => bannedHits(t).map((h) => h.word + " in: " + t)) };
  // Typing saves the draft: no password, the name only as typed, no
  // identification before step 1 found anything.
  calls.length = 0;
  f.elements.source_host.value = "db1"; f.elements.source_host.fire("input");
  await flush(2);
  out.draftPut = calls.find((c) => c.startsWith("PUT")) || "";
  // Step 1 with no host asks for it and probes nothing.
  f.elements.source_host.value = "";
  calls.length = 0;
  f.fire("submit"); await flush();
  out.noHostCalls = calls.slice();
  // Find it: one probe, then step 2 for what answered.
  f.elements.source_host.value = "db1"; f.elements.source_port.value = "";
  ctx.__identifyAnswer = mariaRDS;
  calls.length = 0;
  f.fire("submit"); await flush(4);
  out.identifyCalls = calls.filter((c) => c.startsWith("POST /api/servers/identify"));
  out.found = { step: step(f), step2: shown(f, 2), submit: button(f), texts: identified(f), flavor: f.elements.flavor.value, fullRowHidden: !!f.querySelector("p#connect-full-row").hidden,
    managed: !!f.elements.cx_managed.checked, grant: f.querySelector("pre[data-grant]").attrs["data-grant"],
    block: f.querySelector("pre[data-grant]").textContent, user: f.elements.source_user.value, pwLen: f.elements.source_password.value.length,
    draftPut: calls.filter((c) => c.startsWith("PUT /api/servers/draft")).pop() || "",
    banned: texts(f).flatMap((t) => bannedHits(t).map((h) => h.word + " in: " + t)) };
  // A different host undoes it: back to step 1.
  f.elements.source_host.value = "db1b"; f.elements.source_host.fire("input");
  out.backToWhere = { step: step(f), step2Hidden: !shown(f, 2), found: identified(f).length };

  // A proxy: the flavor is a choice, MySQL chosen, nothing forced.
  f = await toStep2("proxy1", { addr: "proxy1:6033", version: "8.0.11", proxy: "proxysql" });
  const pick = f.querySelector("div#connect-found").querySelector("div.cx-pick").querySelectorAll("button");
  out.proxy = { step: step(f), texts: identified(f), buttons: pick.map((b) => b._text + "=" + b.attrs["aria-pressed"]), flavor: f.elements.flavor.value };
  pick.find((b) => b._text === "MariaDB").fire("click"); await flush();
  out.proxy.chosen = { flavor: f.elements.flavor.value, grant: f.querySelector("pre[data-grant]").attrs["data-grant"], title: identified(f)[0],
    buttons: pick.map((b) => b._text + "=" + b.attrs["aria-pressed"]) };
  // A failure stays on step 1 with the path drawn.
  setCaps({ monitor: true });
  vm.runInContext("showConnectForm(null)", ctx);
  f = form();
  f.elements.source_host.value = "nope.example"; ctx.__identifyAnswer = { addr: "nope.example:3306", kind: "name_not_found" };
  f.fire("submit"); await flush(4);
  const path = f.querySelector("div#connect-found").querySelector("div.cx-path");
  out.miss = { step: step(f), step2Hidden: !shown(f, 2), drawing: path ? path.attrs["aria-label"] : "", bad: path ? path.querySelectorAll("span.bad").length : 0 };
  // Step 2: I ran it with no password checks nothing.
  f = await toStep2("db1", mariaRDS);
  f.elements.source_password.value = "";
  calls.length = 0;
  f.fire("submit"); await flush();
  out.noPassword = { calls: calls.filter((c) => c.includes("/api/servers/check")), step: step(f) };
  // I ran it: step 3. A draft save in flight is awaited before the check.
  f.elements.source_password.value = "Pw-1";
  let open; putGate = new Promise((r) => { open = r; });
  calls.length = 0;
  f.elements.source_user.value = "alice"; f.elements.source_user.fire("input");
  scheduled.length = 0;
  ctx.__checkAnswer = { ok: false, name: "db1", doctor: { failed: 1, checks: [{ name: "Source MySQL connection", status: "fail", kind: "access_denied", light: "login", detail: "Access denied for user 'alice'" }] } };
  f.fire("submit"); await flush();
  out.checkBeforePutDone = calls.some((c) => c.startsWith("POST /api/servers/check"));
  out.cancelOffDuringCheck = !!f.querySelector("button#server-cancel").disabled;
  open(); putGate = null; await flush(4);
  out.order = calls.map((c) => c.split(" {")[0]);
  out.checkBody = JSON.parse((calls.find((c) => c.startsWith("POST /api/servers/check")) || "x {}").slice("POST /api/servers/check ".length));
  out.failed = { step: step(f), lights: lights(f), auto: f.querySelector("p#connect-auto")._text, scheduled: scheduled.length, notices: notices.length,
    submit: button(f), banned: texts(f).flatMap((t) => bannedHits(t).map((h) => h.word + " in: " + t)) };
  // The re-check runs by itself, and stops after CONNECT_RECHECK_MAX rounds.
  const max = vm.runInContext("CONNECT_RECHECK_MAX", ctx);
  let rounds = 1;
  while (scheduled.length && rounds < max + 5) { const fn = scheduled.shift(); calls.length = 0; fn(); await flush(4); if (calls.some((c) => c.startsWith("POST /api/servers/check"))) rounds++; }
  out.recheck = { rounds, max, auto: f.querySelector("p#connect-auto")._text, pending: scheduled.length };
  // A re-check that fires after the screen moved on does nothing.
  scheduled.length = 0;
  f.fire("submit"); await flush(4);
  const late = scheduled.shift();
  f.elements.source_host.value = "elsewhere"; f.elements.source_host.fire("input");
  calls.length = 0;
  if (late) { late(); await flush(4); }
  out.lateRecheckCalls = calls.filter((c) => c.startsWith("POST /api/servers/check"));
  // A connection that failed on the network: the rest is not reached.
  f = await toStep2("db1", mariaRDS);
  f.elements.source_password.value = "Pw-2";
  ctx.__checkAnswer = { ok: false, name: "db1", doctor: { failed: 1, checks: [{ name: "Source MySQL connection", status: "fail", kind: "timeout", light: "reach" }] } };
  f.fire("submit"); await flush(4);
  out.unreached = lights(f);
  // A key missing: the rows and permissions lights are green, keys red.
  ctx.__checkAnswer = { ok: false, name: "db1", doctor: { failed: 1, checks: [
    { name: "Source MySQL connection", status: "pass", light: "reach" },
    { name: "binlog_format=ROW", status: "pass", light: "rows" },
    { name: "REPLICATION SLAVE + CLIENT grants", status: "pass", light: "permissions" },
    { name: "Every table has a PRIMARY KEY", status: "fail", kind: "no_primary_key", light: "keys", subjects: ["shop.t"], statements: ["ALTER TABLE shop.t ADD PRIMARY KEY (id);"] }] } };
  f.fire("submit"); await flush(4);
  out.keys = lights(f);
  // Started: said in place, the button closes, no notice, the list refreshed.
  const noticesBefore = notices.length, listsBefore = serverLists;
  scheduled.length = 0;
  ctx.__checkAnswer = { ok: true, started: true, name: "db1", doctor: { warnings: 0, checks: [{ name: "Source MySQL connection", status: "pass", light: "reach" }] } };
  f.fire("submit"); await flush(6);
  out.started = { step: step(f), submit: button(f), result: result(f), lights: lights(f), notices: notices.length - noticesBefore, lists: serverLists - listsBefore,
    scheduled: scheduled.length, still: !!form(), managedNote: result(f).some((t) => t.includes("lock-all")) };
  f.fire("submit"); await flush();
  out.doneCloses = !form();
  // A start whose only findings are optional improvements folds them, and
  // counts no warning; one real warning beside it is the one counted.
  f = await toStep2("db7", { addr: "db7:3306", version: "8.4.3", flavor: "mysql" });
  f.elements.source_password.value = "Pw-7";
  ctx.__checkAnswer = { ok: true, started: true, name: "db7", doctor: { warnings: 0, optional: 1,
    checks: [{ name: "Statement capture (query_text)", status: "warn", optional: true, light: "rows", remediation: "Show the SQL statement behind each change. To turn it on:\n\n  SET PERSIST binlog_rows_query_log_events = ON;" }] } };
  f.fire("submit"); await flush(6);
  out.optionalOnly = { result: result(f), card: f.querySelector("div#connect-result").children[0].className, lights: lights(f) };
  f = await toStep2("db8", { addr: "db8:3306", version: "8.4.3", flavor: "mysql" });
  f.elements.source_password.value = "Pw-8";
  ctx.__checkAnswer = { ok: true, started: true, name: "db8", doctor: { warnings: 1, optional: 1,
    checks: ctx.__checkAnswer.doctor.checks.concat([{ name: "No FK CASCADE constraints", status: "warn", detail: "x", light: "other" }]) } };
  f.fire("submit"); await flush(6);
  out.mixed = { result: result(f), card: f.querySelector("div#connect-result").children[0].className, lights: lights(f) };
  // The start itself failed: said, with nothing re-checked.
  f = await toStep2("db6", { addr: "db6:3306", version: "8.4.3", flavor: "mysql" });
  f.elements.source_password.value = "Pw-6";
  scheduled.length = 0;
  ctx.__checkAnswer = { ok: false, name: "db6", error: "launch failed", kept: false, doctor: { checks: [{ name: "Source MySQL connection", status: "pass", light: "reach" }] } };
  f.fire("submit"); await flush(6);
  out.startFailed = { step: step(f), result: result(f), scheduled: scheduled.length, submit: button(f) };
  // The check could not be sent: said, and tried again by itself.
  scheduled.length = 0;
  ctx.__checkThrows = "network down";
  f.fire("submit"); await flush(6);
  ctx.__checkThrows = "";
  out.networkDown = { auto: f.querySelector("p#connect-auto")._text, scheduled: scheduled.length };
  // Typing while a check that ends in "started" runs is never saved after.
  f = await toStep2("db5", { addr: "db5:3306", version: "8.4.3", flavor: "mysql" });
  f.elements.source_password.value = "Pw-5";
  await flush(2);
  let openCheck; checkGate = new Promise((r) => { openCheck = r; });
  ctx.__checkAnswer = { ok: true, started: true, name: "db5", doctor: { checks: [], warnings: 0 } };
  f.fire("submit"); await flush(2);
  calls.length = 0;
  f.elements.source_user.value = "typed-during-check"; f.elements.source_user.fire("input");
  openCheck(); checkGate = null;
  await flush(6);
  out.putsAfterStarted = calls.filter((c) => c.startsWith("PUT /api/servers/draft"));
  // A restored draft with what step 1 found comes back at step 2, probing
  // nothing: no password generated, the screen asks for it.
  setCaps({ monitor: true });
  calls.length = 0;
  // A reload starts a page with no password generated yet.
  vm.runInContext("pendingSourcePassword = ''", ctx);
  vm.runInContext("showConnectForm({ name: '', source_host: 'db', source_port: '3307', source_user: 'alice', flavor: 'mariadb', auto_name: 'db-3307', identified: { version: '10.11.6-MariaDB-log', flavor: 'mariadb' } })", ctx);
  f = form();
  out.restored = { step: step(f), pw: f.elements.source_password.value, user: f.elements.source_user.value, focused, texts: identified(f),
    pwAgainShown: !f.querySelector("p#connect-pw-again").hidden, block: f.querySelector("pre[data-grant]").textContent.split("\n")[0],
    grant: f.querySelector("pre[data-grant]").attrs["data-grant"], probes: calls.filter((c) => c.includes("identify")) };
  // Without it, step 1.
  vm.runInContext("showConnectForm({ source_host: 'db', source_port: '3307', source_user: 'alice' })", ctx);
  out.restoredNoIdentity = { step: step(form()), focused };
  // Cancel waits for a pending save, then deletes the draft.
  f = form();
  putGate = new Promise((r) => { open = r; });
  calls.length = 0;
  f.elements.source_host.value = "db2"; f.elements.source_host.fire("input");
  await flush();
  f.querySelector("button#server-cancel").fire("click"); await flush();
  out.deleteBeforePutDone = calls.some((c) => c.startsWith("DELETE"));
  open(); putGate = null; await flush(3);
  out.cancelOrder = calls.map((c) => c.split(" {")[0]);
  // A save still queued when Cancel is pressed is never sent.
  setCaps({ monitor: true });
  vm.runInContext("showConnectForm(null)", ctx);
  f = form();
  calls.length = 0;
  f.elements.source_host.value = "db3"; f.elements.source_host.fire("input");
  f.querySelector("button#server-cancel").fire("click");
  await flush(3);
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
  // The link to the long form carries what was typed, the flavor included,
  // and drops the draft.
  f = await toStep2("db9", { addr: "db9:3310", version: "10.6.2-MariaDB", flavor: "mariadb" });
  calls.length = 0;
  f.elements.source_port.value = "3310";
  f.querySelector("button#connect-full-form").fire("click"); await flush(2);
  out.full = { long: !!form() && form().attrs["data-connect"] === undefined && !!form().elements.host,
    host: form() ? form().elements.source_host.value : null, port: form() ? form().elements.source_port.value : null,
    flavor: form() ? form().elements.flavor.value : null, deleted: calls.some((c) => c.startsWith("DELETE /api/servers/draft")) };
  // The pagehide stash: the fields, never the password, and a finished form
  // not at all. A stashed address that differs from the saved draft's drops
  // what step 1 found for the old one.
  const store = new Map();
  ctx.sessionStorage = { getItem: (k) => store.has(k) ? store.get(k) : null, setItem: (k, v) => store.set(k, String(v)), removeItem: (k) => store.delete(k) };
  mount.replaceChildren();
  setCaps({ monitor: true });
  vm.runInContext("showConnectForm(null)", ctx);
  f = form();
  f.elements.source_host.value = "late-host"; f.elements.source_port.value = "3399"; f.elements.source_user.value = "late_user";
  f.elements.source_password.value = "Pw-never-stashed";
  vm.runInContext("stashConnectDraft()", ctx);
  out.stash = [...store.values()].join("");
  mount.replaceChildren();
  calls.length = 0;
  ctx.__draftAnswer = { found: true, draft: { source_host: "old-host", identified: { version: "8.4.3", flavor: "mysql" } }, auto_name: "late-host" };
  await vm.runInContext("restoreConnectDraft()", ctx);
  await flush(4);
  f = form();
  out.stashRestored = f ? { host: f.elements.source_host.value, port: f.elements.source_port.value, user: f.elements.source_user.value, pw: f.elements.source_password.value, step: step(f) } : null;
  out.stashResynced = calls.some((c) => c.startsWith("PUT /api/servers/draft") && c.includes("late_user"));
  out.stashConsumed = store.size === 0;
  f.dataset.done = "1";
  vm.runInContext("stashConnectDraft()", ctx);
  out.doneNotStashed = store.size === 0;
  // A console that only reads an index keeps the long form for an add.
  mount.replaceChildren();
  f = show({ monitor: false });
  out.serveIsLongForm = f.attrs["data-connect"] === undefined && !!f.elements.host;
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

func TestConnectScreenWiring(t *testing.T) {
	raw := runNodeConnect(t, renderHarnessJS+connectHarnessJS)
	type lightsOut = []string
	var out struct {
		Fresh struct {
			Connect, Step2Hidden, Step3Hidden, PwAgainHidden bool
			Step, Focused, Submit                            string
			Fields, Banned                                   []string
		}
		DraftPut      string
		NoHostCalls   []string
		IdentifyCalls []string
		Found         struct {
			Step, Submit, Flavor, Grant, Block, User, DraftPut string
			Step2, Managed, FullRowHidden                      bool
			PwLen                                              int
			Texts, Banned                                      []string
		}
		BackToWhere struct {
			Step        string
			Step2Hidden bool
			Found       int
		}

		Proxy struct {
			Step, Flavor   string
			Texts, Buttons []string
			Chosen         struct {
				Flavor, Grant, Title string
				Buttons              []string
			}
		}
		Miss struct {
			Step, Drawing string
			Step2Hidden   bool
			Bad           int
		}
		NoPassword struct {
			Calls []string
			Step  string
		}
		CheckBeforePutDone, CancelOffDuringCheck bool
		Order                                    []string
		CheckBody                                map[string]any
		Failed                                   struct {
			Step, Auto, Submit string
			Lights, Banned     []string
			Scheduled, Notices int
		}
		Recheck struct {
			Rounds, Max, Pending int
			Auto                 string
		}
		LateRecheckCalls []string
		Unreached, Keys  lightsOut
		Started          struct {
			Step, Submit              string
			Result, Lights            []string
			Notices, Lists, Scheduled int
			Still, ManagedNote        bool
		}
		DoneCloses   bool
		OptionalOnly struct {
			Result, Lights []string
			Card           string
		}
		Mixed struct {
			Result, Lights []string
			Card           string
		}
		StartFailed struct {
			Step, Submit string
			Result       []string
			Scheduled    int
		}
		NetworkDown struct {
			Auto      string
			Scheduled int
		}
		PutsAfterStarted []string
		Restored         struct {
			Step, Pw, User, Focused, Block, Grant string
			PwAgainShown                          bool
			Texts, Probes                         []string
		}
		RestoredNoIdentity                                   struct{ Step, Focused string }
		DeleteBeforePutDone                                  bool
		CancelOrder, QueuedAfterCancel, ReadOnlyRestoreCalls []string
		RestoredHost                                         *string
		Full                                                 struct {
			Long, Deleted      bool
			Host, Port, Flavor *string
		}
		Stash                                                         string
		StashRestored                                                 *struct{ Host, Port, User, Pw, Step string }
		StashResynced, StashConsumed, DoneNotStashed, ServeIsLongForm bool
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}

	// Step 1: host and port, one button, nothing else asked yet.
	f := out.Fresh
	if !f.Connect || f.Step != "1" || !f.Step2Hidden || !f.Step3Hidden || f.Focused != "source_host" || f.Submit != "Find it" || !f.PwAgainHidden {
		t.Errorf("fresh screen: %+v", f)
	}
	if strings.Join(f.Fields, ",") != "cx_managed,name,source_host,source_password,source_port,source_user" {
		t.Errorf("Connect fields = %v", f.Fields)
	}
	if len(f.Banned) > 0 {
		t.Errorf("the screen uses words the first run bans: %v", f.Banned)
	}
	if !strings.HasPrefix(out.DraftPut, "PUT /api/servers/draft ") || strings.Contains(out.DraftPut, "source_password") ||
		!strings.Contains(out.DraftPut, `"name":""`) || strings.Contains(out.DraftPut, "identified") {
		t.Errorf("typing saves the draft with no password, the name as typed and nothing found yet: %s", out.DraftPut)
	}
	// A probe may count against DBTrail's address: only a press with a host
	// sends one, and exactly one.
	if len(out.NoHostCalls) != 0 {
		t.Errorf("Find it with no host called %v", out.NoHostCalls)
	}
	if len(out.IdentifyCalls) != 1 || out.IdentifyCalls[0] != `POST /api/servers/identify {"source_host":"db1","source_port":""}` {
		t.Errorf("identify calls = %v", out.IdentifyCalls)
	}

	// What answered opens step 2, with the block for that server.
	fd := out.Found
	if fd.Step != "2" || !fd.Step2 || fd.Submit != "I ran it" || strings.Join(fd.Texts, "|") != "MariaDB 10.11 on Amazon RDS" {
		t.Errorf("found: step %q, step 2 shown %v, button %q, tile %v", fd.Step, fd.Step2, fd.Submit, fd.Texts)
	}
	if fd.Flavor != "mariadb" || fd.Grant != "mariadb" || !fd.Managed {
		t.Errorf("found: flavor %q, block %q, managed %v; want the MariaDB block for RDS", fd.Flavor, fd.Grant, fd.Managed)
	}
	if !strings.Contains(fd.Block, "\nGRANT LOCK TABLES, SHOW VIEW ON *.* TO 'dbtrail'@'%';") || strings.Contains(fd.Block, "\nGRANT RELOAD") {
		t.Errorf("on RDS the lock-all grant is the live line and RELOAD is not:\n%s", fd.Block)
	}
	if fd.User != "dbtrail" || fd.PwLen < 20 || !strings.Contains(fd.Block, "IDENTIFIED BY '") {
		t.Errorf("found: user %q, password of %d characters, block without it", fd.User, fd.PwLen)
	}
	if !strings.Contains(fd.DraftPut, `"identified":{"version":"10.11.6-MariaDB-log","flavor":"mariadb","managed":"rds"}`) || strings.Contains(fd.DraftPut, "source_password") {
		t.Errorf("the draft keeps what step 1 found and never the password: %s", fd.DraftPut)
	}
	if len(fd.Banned) > 0 {
		t.Errorf("step 2 uses words the first run bans: %v", fd.Banned)
	}
	if b := out.BackToWhere; b.Step != "1" || !b.Step2Hidden || b.Found != 0 {
		t.Errorf("another host after step 1 found one: %+v; want step 1 again, the answer gone", b)
	}
	// Past step 1 the full-form link goes: kept, it pushes the button down.
	if !fd.FullRowHidden {
		t.Error("the link to the full form stays on step 2, pushing its button down")
	}

	// A proxy: the flavor is a choice, with MySQL chosen, so nothing is forced.
	px := out.Proxy
	if px.Step != "2" || px.Flavor != "mysql" || strings.Join(px.Buttons, ",") != "MySQL=true,MariaDB=false" ||
		len(px.Texts) != 2 || !strings.Contains(px.Texts[1], "ProxySQL") {
		t.Errorf("proxy: %+v", px)
	}
	if c := px.Chosen; c.Flavor != "mariadb" || c.Grant != "mariadb" || c.Title != "MariaDB" || strings.Join(c.Buttons, ",") != "MySQL=false,MariaDB=true" {
		t.Errorf("choosing MariaDB behind the proxy: %+v", c)
	}
	if m := out.Miss; m.Step != "1" || !m.Step2Hidden || m.Drawing != "DBTrail could not find nope.example" || m.Bad != 2 {
		t.Errorf("a name that does not exist: %+v; want step 1, the path drawn with the name part broken", m)
	}

	// Step 2 to 3.
	if len(out.NoPassword.Calls) != 0 || out.NoPassword.Step != "2" {
		t.Errorf("I ran it with no password: %+v", out.NoPassword)
	}
	if out.CheckBeforePutDone {
		t.Errorf("the check went out while a draft save was in flight: %v", out.Order)
	}
	if strings.Join(out.Order, ",") != "PUT /api/servers/draft,PUT done,POST /api/servers/check" {
		t.Errorf("order = %v; want the draft save finished before the check", out.Order)
	}
	if !out.CancelOffDuringCheck {
		t.Error("Cancel stays usable while a check runs; it cannot stop a start already on its way")
	}
	if out.CheckBody["source_password"] != "Pw-1" || out.CheckBody["name"] != "" || out.CheckBody["source_user"] != "alice" || out.CheckBody["flavor"] != "mariadb" {
		t.Errorf("check body = %v; want the typed password and user, no name, the flavor step 1 found", out.CheckBody)
	}
	fl := out.Failed
	wantFailed := "ok:DBTrail reaches it,bad:User logs in,wait:Change log keeps full rows,wait:Permissions,wait:Every table has a key"
	if fl.Step != "3" || strings.Join(fl.Lights, ",") != wantFailed || fl.Notices != 0 || fl.Submit != "Check again" {
		t.Errorf("a refused password: %+v; want the lights with login red and the rest not reached, in place", fl)
	}
	if fl.Scheduled != 1 || fl.Auto != "Checking again in 10 seconds." {
		t.Errorf("a failed round schedules one re-check and says so: %+v", fl)
	}
	if len(fl.Banned) > 0 {
		t.Errorf("step 3 uses words the first run bans: %v", fl.Banned)
	}
	if r := out.Recheck; r.Rounds != r.Max || r.Pending != 0 || r.Auto != "Stopped checking. Press Check again when it is fixed." {
		t.Errorf("the re-check loop: %+v; want exactly CONNECT_RECHECK_MAX rounds, then a stop that says so", r)
	}
	if len(out.LateRecheckCalls) != 0 {
		t.Errorf("a re-check that fired after the address changed still checked: %v", out.LateRecheckCalls)
	}
	if got := strings.Join(out.Unreached, ","); got != "bad:DBTrail reaches it,wait:User logs in,wait:Change log keeps full rows,wait:Permissions,wait:Every table has a key" {
		t.Errorf("a connection that timed out: %s", got)
	}
	if got := strings.Join(out.Keys, ","); got != "ok:DBTrail reaches it,ok:User logs in,ok:Change log keeps full rows,ok:Permissions,bad:Every table has a key" {
		t.Errorf("a table without a key: %s", got)
	}

	// Started: said in place, nothing to dismiss, the button closes.
	st := out.Started
	if st.Step != "done" || st.Submit != "Done" || !st.Still || st.Notices != 0 || st.Lists != 1 || st.Scheduled != 0 {
		t.Errorf("started: %+v", st)
	}
	if len(st.Result) < 2 || st.Result[0] != "Capture started" || !strings.Contains(st.Result[1], "Capture started for db1") || !st.ManagedNote {
		t.Errorf("started result: %v; want the start said, and the lock-all note for RDS", st.Result)
	}
	if !out.DoneCloses {
		t.Error("Done did not close the screen")
	}
	if o := out.OptionalOnly; o.Card != "notice-inline ok" || len(o.Result) < 2 || !strings.Contains(o.Result[1], "Capture started for db7") {
		t.Errorf("a start with only optional improvements: %+v; want the ok card, no warning", o)
	}
	if m := out.Mixed; m.Card != "notice-inline warn" || len(m.Result) < 2 || m.Result[1] != "Check this when you can:" || m.Lights[len(m.Lights)-1] != "warn:Other checks" {
		t.Errorf("one real warning beside an optional one: %+v", m)
	}
	if sf := out.StartFailed; sf.Step != "3" || sf.Scheduled != 0 || len(sf.Result) != 3 || sf.Result[0] != "Capture did not start" || sf.Submit != "Check again" {
		t.Errorf("a start that failed after every check passed: %+v; want it said, and no re-check", sf)
	}
	if n := out.NetworkDown; n.Scheduled != 1 || !strings.HasPrefix(n.Auto, "The checks could not run: network down.") {
		t.Errorf("a check that could not be sent: %+v; want the reason kept and a re-check", n)
	}
	if len(out.PutsAfterStarted) != 0 {
		t.Errorf("an edit typed during a check that started capture was saved afterwards: %v", out.PutsAfterStarted)
	}

	// Reload: back at step 2 with nothing probed and no password made up.
	r := out.Restored
	if r.Step != "2" || len(r.Probes) != 0 || strings.Join(r.Texts, "|") != "MariaDB 10.11" || r.Grant != "mariadb" {
		t.Errorf("restored with what step 1 found: %+v; want step 2 for MariaDB, no probe", r)
	}
	if r.Pw != "" || !r.PwAgainShown || r.Focused != "source_password" || r.User != "alice" || !strings.HasPrefix(r.Block, "--") {
		t.Errorf("restored form: %+v; the account was created with a password the page no longer has", r)
	}
	if rn := out.RestoredNoIdentity; rn.Step != "1" || rn.Focused != "source_host" {
		t.Errorf("restored with nothing found: %+v; want step 1", rn)
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
	if fu := out.Full; !fu.Long || !fu.Deleted || fu.Host == nil || *fu.Host != "db9" || *fu.Port != "3310" || *fu.Flavor != "mariadb" {
		t.Errorf("the link to the long form: %+v (want the long form, the host, port and flavor carried, the draft deleted)", fu)
	}
	if strings.Contains(out.Stash, "Pw-never-stashed") || !strings.Contains(out.Stash, "late_user") {
		t.Errorf("the pagehide stash: %s (want the fields, never the password)", out.Stash)
	}
	if sr := out.StashRestored; sr == nil || sr.Host != "late-host" || sr.Port != "3399" || sr.User != "late_user" || sr.Pw != "" || sr.Step != "1" {
		t.Errorf("a stashed address that differs from the saved one: %+v; want its fields, and step 1 (what was found was for the old address)", out.StashRestored)
	}
	if !out.StashResynced || !out.StashConsumed || !out.DoneNotStashed {
		t.Errorf("stash: resynced %v, consumed %v, a finished form not stashed %v", out.StashResynced, out.StashConsumed, out.DoneNotStashed)
	}
	if !out.ServeIsLongForm {
		t.Error("a console that only reads an index lost the long add form")
	}
}
