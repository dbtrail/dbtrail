// The SQL clients page, its sidebar entry and its hidden-rules section on an
// installation whose servers are PostgreSQL (#2773). Run with:
//
//   node --test test/console-e2e/pg_clients_page_2773.test.mjs
//
// The whole app.js is loaded into a vm context over a small stand-in for the
// DOM, and the real functions draw; what is checked is the text a person
// reads and what each panel is handed. The MySQL page is drawn too, and has
// to come out as it always did.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import vm from "node:vm";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const APP = readFileSync(path.join(HERE, "../../internal/console/assets/app.js"), "utf8");

class FakeEl {
  constructor(tag) {
    this.tag = tag; this.children = []; this.className = ""; this._text = ""; this.attrs = {}; this.hidden = false;
    this.nodeType = 1; this.value = ""; this.style = {}; this.dataset = {};
    const has = (c) => (" " + this.className + " ").includes(" " + c + " ");
    this.classList = {
      add: (c) => { if (!has(c)) this.className += " " + c; },
      remove: (c) => { this.className = this.className.split(" ").filter((x) => x && x !== c).join(" "); },
      toggle: () => false, contains: has,
    };
  }
  set textContent(v) { this._text = String(v); this.children = []; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(" "); }
  append(...k) { for (const x of k) if (x != null) this.children.push(x); }
  appendChild(x) { this.children.push(x); return x; }
  prepend(...k) { this.children.unshift(...k); }
  replaceChildren(...k) { this.children = k; }
  setAttribute(k, v) { this.attrs[k] = v; }
  getAttribute(k) { return this.attrs[k]; }
  addEventListener() {} removeEventListener() {}
  // ".name": the first descendant carrying that class.
  querySelector(sel) {
    const want = sel.startsWith(".") ? sel.slice(1) : null;
    let hit = null;
    const walk = (n) => {
      if (hit || !n || n.nodeType !== 1) return;
      if (want && (" " + n.className + " ").includes(" " + want + " ")) { hit = n; return; }
      n.children.forEach(walk);
    };
    this.children.forEach(walk);
    return hit;
  }
  querySelectorAll() { return []; }
  set innerHTML(v) { this._text = String(v); }
  get offsetWidth() { return 0; }
}

// load gives a fresh context with app.js run in it. The sidebar's entry for
// the SQL clients page is a real element here, found by the selector the
// code uses, so its label can be read back.
function load() {
  const view = new FakeEl("main");
  const navLabel = new FakeEl("span");
  navLabel.textContent = "MySQL clients";
  const permissive = () => new Proxy(function () {}, {
    get: (t, p) => (p === Symbol.toPrimitive ? () => "" : p === "then" ? undefined : permissive()),
    apply: () => permissive(), construct: () => permissive(),
  });
  const document = {
    createElement: (t) => new FakeEl(t), createTextNode: (s) => ({ nodeType: 3, textContent: String(s) }),
    createElementNS: (n, t) => new FakeEl(t), addEventListener() {},
    querySelector: (sel) => (sel === '.nav-item[data-route="mysql-clients"] span:last-child' ? ctx.__navLabel : null),
    querySelectorAll: () => [], body: new FakeEl("body"), documentElement: new FakeEl("html"),
    getElementById: (id) => (id === "view" ? view : new FakeEl("div")), importNode: (n) => n, readyState: "loading",
  };
  const ctx = {
    document, console, setTimeout: () => 0, clearTimeout() {}, setInterval: () => 0, clearInterval() {},
    URLSearchParams, URL, Intl, Date, Math, JSON, Set, Map, WeakMap, WeakSet, RegExp, Promise, AbortController, Event, EventTarget,
    location: { pathname: "/mysql-clients", search: "", hash: "", origin: "http://x", hostname: "dbtrail.example" },
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} }, sessionStorage: { getItem: () => null, setItem() {} },
    navigator: permissive(), history: permissive(), fetch: () => new Promise(() => {}),
    matchMedia: () => ({ matches: false, addEventListener() {} }),
    DOMParser: function () { this.parseFromString = () => ({ documentElement: new FakeEl("svg") }); },
    requestAnimationFrame: () => 0, MutationObserver: function () { this.observe = () => {}; },
    ResizeObserver: function () { this.observe = () => {}; }, IntersectionObserver: function () { this.observe = () => {}; },
    getComputedStyle: () => ({}), CSS: { supports: () => false }, performance: { now: () => 0 }, EventSource: function () {},
  };
  ctx.window = ctx; ctx.globalThis = ctx; ctx.self = ctx;
  ctx.__navLabel = navLabel;
  vm.createContext(ctx);
  vm.runInContext(APP, ctx);
  const run = (s) => vm.runInContext(s, ctx);
  // Every permission, so every section is drawn; no request leaves.
  run("capsCache = { monitor: true, permissions: null };");
  run("api = () => new Promise(() => {});");
  return { ctx, run, view, navLabel };
}

const pg = (name) => ({ id: name, name, flavor: "postgres", has_source: true });
const my = (name) => ({ id: name, name, flavor: "mysql", has_source: true });
const tidy = (s) => s.replace(/\s+/g, " ").trim();

// draw runs the real buildMySQLClients over servers and reads the page. The
// panels it mounts are wrapped, so what each one was handed is kept.
function draw(servers, { failed = false } = {}) {
  const { ctx, run, view } = load();
  ctx.__servers = servers;
  ctx.__rt = { configured: true, status: { pool: { memory_each: "512 MiB" }, servers: {} } };
  ctx.__mem = { memory: "1GB", memory_bytes: 1073741824 };
  ctx.__fb = { enabled: true, can_manage: true };
  ctx.__seen = {};
  run(`
    currentServer = ${JSON.stringify(servers && servers[0] ? servers[0].id : "")};
    { const sqlP = sqlClientPanel, memP = sqlMemoryPanel, hidP = hiddenRulesPanel;
      sqlClientPanel = (...a) => { __seen.sqlRT = a[4]; return sqlP(...a); };
      sqlMemoryPanel = (...a) => { __seen.memRT = a[2]; return memP(...a); };
      hiddenRulesPanel = (cur) => { __seen.hidden = cur && cur.id; return hidP(cur); }; }
    buildMySQLClients(__servers, __fb, __mem, __rt, ${failed});
  `);
  let title = "", sub = "", docs = "";
  const walk = (n) => {
    if (!n || n.nodeType !== 1) return;
    if (n.tag === "h1") title = tidy(n.textContent);
    if (n.tag === "p" && n.className.includes("page-sub")) sub = tidy(n.textContent);
    if (n.tag === "a" && n.className.includes("page-docs")) docs = n.attrs.href || n.href || "";
    n.children.forEach(walk);
  };
  walk(view);
  return { title, sub, docs, text: tidy(view.textContent), seen: ctx.__seen, rt: ctx.__rt };
}

test("the sidebar and palette name: PostgreSQL clients only when the servers are PostgreSQL", () => {
  const { ctx } = load();
  assert.equal(vm.runInContext("sqlClientsNavName(true)", ctx), "PostgreSQL clients");
  assert.equal(vm.runInContext("sqlClientsNavName(false)", ctx), "MySQL clients");
});

test("the sidebar entry follows the server list, both ways", () => {
  const { ctx, run, navLabel } = load();
  const name = (servers) => { ctx.__s = servers; run("nameSQLClientsNav(__s)"); return navLabel.textContent; };
  const cases = [
    ["PostgreSQL servers", [pg("a"), pg("b")], "PostgreSQL clients"],
    ["back to no server", [], "MySQL clients"],
    ["PostgreSQL again", [pg("a")], "PostgreSQL clients"],
    ["then a MySQL one", [my("a")], "MySQL clients"],
    ["both families (from before one family per installation)", [pg("a"), my("b")], "MySQL clients"],
    ["the list was not read", null, "MySQL clients"],
    ["only entries with no source", [{ id: "x", flavor: "postgres" }], "MySQL clients"],
    ["a PostgreSQL and an entry with no source", [pg("a"), { id: "x", flavor: "mysql" }], "PostgreSQL clients"],
  ];
  for (const [what, servers, want] of cases) assert.equal(name(servers), want, what);
  // The command palette reads the same answer as the sidebar.
  name([pg("a")]);
  assert.equal(run("sqlClientsNavName(sqlClientsPG)"), "PostgreSQL clients");
  name([my("a")]);
  assert.equal(run("sqlClientsNavName(sqlClientsPG)"), "MySQL clients");
  // A page with no sidebar entry (an older index.html) is not an error.
  ctx.__navLabel = null;
  assert.doesNotThrow(() => run("nameSQLClientsNav([])"));
});

test("the page for PostgreSQL servers", () => {
  const got = draw([pg("shop")]);
  assert.equal(got.title, "PostgreSQL clients");
  assert.equal(got.sub, "Read your copy from a PostgreSQL client, on the PostgreSQL port.");
  assert.equal(got.docs, "https://dbtrail.com/docs/guides/query-with-a-postgres-client/");
  // The MySQL router's numbers are about the MySQL port: no panel is handed them.
  assert.equal(got.seen.sqlRT, null);
  assert.equal(got.seen.memRT, null);
  assert.equal(got.seen.hidden, "shop");
  for (const want of ["port=5433 dbname=shop", "Applies to the copy", "Memory for SQL on the copy",
    "SQL on the copy, the PostgreSQL port and the AI connector's SQL tool do not show"]) {
    assert.ok(got.text.includes(want), "lacks " + JSON.stringify(want) + ":\n" + got.text);
  }
  // Not one word about the MySQL port: not its name, not its router's memory.
  for (const not of ["MySQL port", "mysql -h", "3309", "Two ways to use", "Applies to both", "512 MiB", "restart the router"]) {
    assert.ok(!got.text.includes(not), "says " + JSON.stringify(not) + ":\n" + got.text);
  }
});

test("the page for PostgreSQL servers says a list it could not read", () => {
  const got = draw([pg("shop")], { failed: true });
  assert.ok(got.text.includes("Could not read the list of servers"), got.text);
});

test("the page for MySQL servers, and for every list that is not PostgreSQL, as it always was", () => {
  for (const [what, servers] of [["MySQL", [my("shop")]], ["both families", [my("shop"), pg("crm")]], ["no server", []], ["MariaDB", [{ ...my("shop"), flavor: "mariadb" }]]]) {
    const got = draw(servers);
    assert.equal(got.title, "MySQL clients", what);
    assert.equal(got.sub, "Two ways to use one MySQL port.", what);
    assert.equal(got.docs, "https://dbtrail.com/docs/guides/query-with-a-mysql-client/", what);
    assert.equal(got.seen.sqlRT, got.rt, what + ": the SQL client panel is handed the router's numbers");
    assert.equal(got.seen.memRT, got.rt, what + ": the memory panel is handed the router's numbers");
    assert.ok(got.text.includes("Applies to both"), what);
    assert.ok(!got.text.includes("PostgreSQL clients"), what);
    if (servers.length) {
      assert.ok(got.text.includes("SQL on the copy, the MySQL port and the AI connector's SQL tool do not show"), what);
    }
  }
});

// hiddenRulesBody, drawn for one server with rules and a grants answer.
function hiddenBody(cur, g) {
  const { ctx, run } = load();
  ctx.__cur = cur; ctx.__g = g;
  ctx.__h = { tables: ["shop.audit_log"], columns: [], editable: true };
  run("__box = document.createElement('div'); hiddenRulesBody(__box, __cur, __h, __g);");
  return tidy(ctx.__box.textContent);
}

test("hidden rules on a PostgreSQL server: the PostgreSQL port, no MySQL grants check", () => {
  // Whatever the grants answer is (the route says none for PostgreSQL; a
  // failed read or a session that may not ask is null or undefined), the
  // MySQL check is not drawn for a PostgreSQL server.
  for (const g of [{ none: "a PostgreSQL server: the MySQL port does not forward to it" }, null, undefined]) {
    const text = hiddenBody(pg("shop"), g);
    for (const want of ["disappear from SQL on the copy, the PostgreSQL port and the AI connector's SQL tool",
      "the PostgreSQL port sends nothing to your PostgreSQL for it: the copy answers alone",
      "The PostgreSQL port sends nothing to your PostgreSQL for this server now"]) {
      assert.ok(text.includes(want), "lacks " + JSON.stringify(want) + " (g=" + JSON.stringify(g) + "):\n" + text);
    }
    for (const not of ["MySQL port", "_flashback", "Forwarding account", "Could not check", "may not check"]) {
      assert.ok(!text.includes(not), "says " + JSON.stringify(not) + " (g=" + JSON.stringify(g) + "):\n" + text);
    }
  }
});

test("hidden rules on a MySQL server, as they always were", () => {
  const text = hiddenBody(my("shop"), null);
  assert.ok(text.includes("disappear from SQL on the copy, the MySQL port and the AI connector's SQL tool, and the port's time travel"), text);
  assert.ok(text.includes("Could not check what the forwarding account can read"), text);
  assert.ok(!text.includes("PostgreSQL"), text);
});

test("the SQL note names the page the sidebar names", () => {
  const { ctx, run } = load();
  const note = (servers) => {
    ctx.__s = servers;
    run("nameSQLClientsNav(__s)");
    return run('hiddenSQLNoteText({ tables: ["shop.audit_log"], columns: [], unreadable: false })');
  };
  assert.equal(note([pg("a")]), "This server hides some tables and columns from SQL: shop.audit_log. Change it in Settings, PostgreSQL clients.");
  assert.equal(note([my("a")]), "This server hides some tables and columns from SQL: shop.audit_log. Change it in Settings, MySQL clients.");
});
