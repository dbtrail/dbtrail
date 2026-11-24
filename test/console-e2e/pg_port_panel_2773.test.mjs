// The port panel for PostgreSQL servers (#2773). Run with:
//
//   node --test test/console-e2e/pg_port_panel_2773.test.mjs
//
// It runs the real postgresInstall and pgPortLine from app.js in a vm context.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import vm from "node:vm";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const APP = readFileSync(path.join(HERE, "../../internal/console/assets/app.js"), "utf8");
function fn(name) {
  const start = APP.indexOf("\nfunction " + name + "(");
  assert.ok(start >= 0, name + " is not in app.js");
  let depth = 0;
  for (let i = APP.indexOf("{", start); i < APP.length; i++) {
    if (APP[i] === "{") depth++;
    else if (APP[i] === "}" && --depth === 0) return APP.slice(start, i + 1);
  }
  throw new Error("unbalanced " + name);
}
const ctx = vm.createContext({});
vm.runInContext(fn("postgresInstall") + fn("pgPortLine"), ctx);

const pg = (name) => ({ id: name, name, flavor: "postgres", has_source: true });
const my = (name) => ({ id: name, name, flavor: "mysql", has_source: true });

test("what counts as PostgreSQL servers", () => {
  const cases = [
    ["PostgreSQL servers", [pg("a"), pg("b")], true],
    ["one PostgreSQL and an entry with no source", [pg("a"), { id: "x", flavor: "mysql" }], true],
    ["MySQL servers", [my("a")], false],
    ["MariaDB", [{ ...my("a"), flavor: "mariadb" }], false],
    // From before one family per installation: the MySQL panel, as always.
    ["both families", [pg("a"), my("b")], false],
    ["no server", [], false],
    ["the list was not read", null, false],
    ["the list was not read (undefined)", undefined, false],
    ["only entries with no source", [{ id: "x", flavor: "postgres" }], false],
    ["a server that does not say its family", [{ id: "x", has_source: true }], false],
    ["a hole in the list", [null, pg("a")], true],
  ];
  for (const [name, servers, want] of cases) {
    assert.equal(ctx.postgresInstall(servers), want, name);
  }
});

test("the psql line", () => {
  assert.equal(ctx.pgPortLine("10.0.0.5", "shop"), 'psql "host=10.0.0.5 port=5433 dbname=shop user=app sslmode=require"');
  assert.equal(ctx.pgPortLine("", "shop.eu-1"), 'psql "host=127.0.0.1 port=5433 dbname=shop.eu-1 user=app sslmode=require"');
  // Nothing that could end the quotes, or be read by the shell inside them,
  // is printed: the name is left to the person.
  for (const bad of ['a b', 'a"b', "a'b", "a$b", "a`b", "a\\b", "", undefined, "a=b"]) {
    assert.equal(ctx.pgPortLine("h", bad), 'psql "host=h port=5433 dbname=<server-name> user=app sslmode=require"', String(bad));
  }
});

// The panel's own wiring, read off the source: the PostgreSQL branch asks no
// router and draws no routing, and the mysql line is still there for the
// other family.
test("the panel draws psql for PostgreSQL servers and keeps the mysql line", () => {
  const panel = fn("sqlClientPanel");
  assert.match(panel, /const pg = postgresInstall\(servers\);/);
  assert.match(panel, /const line = pg \? pgPortLine\(flashbackHost\(at\), user\)\s*\n\s*: "mysql -h "/);
  // The PostgreSQL branch returns before the routing switch is drawn.
  const ret = panel.indexOf('return done("card");');
  const routing = panel.indexOf("routingSwitch(");
  assert.ok(ret > 0 && routing > ret, "the PostgreSQL branch must return before Engine Routing is drawn");
  assert.ok(panel.slice(0, ret).lastIndexOf("if (pg) {") > 0);
  // No write of its own: the buttons are the MySQL panel's.
  assert.equal((panel.match(/api\("\/api\/flashback/g) || []).length, 4, "the panel's writes changed: see save_controls.mjs");
});

// The panel itself, drawn by the real sqlClientPanel over a small stand-in
// for the DOM, and read as the text a person sees.
function draw({ servers, fb, reveal = "", split = false, picked = "shop" }) {
  class Node {
    constructor(tag) { this.tag = tag; this.kids = []; this.attrs = {}; this.className = ""; this.own = ""; this.nodeType = 1; }
    append(...k) { for (const x of k) if (x != null) this.kids.push(x); }
    setAttribute(k, v) { this.attrs[k] = v; }
    addEventListener() {}
    replaceChildren(...k) { this.kids = [...k]; this.own = ""; }
    get children() { return this.kids.filter((k) => k.nodeType === 1); }
    set textContent(v) { this.kids = []; this.own = String(v); }
    get textContent() { return this.own + this.kids.map((k) => k.textContent).join(" "); }
  }
  const c = vm.createContext({
    document: { createElement: (t) => new Node(t), createTextNode: (s) => ({ nodeType: 3, textContent: s }) },
    location: { hostname: "dbtrail.example" },
    window: {},
    sessionMay: () => true,
    api: () => { throw new Error("the panel wrote something while being drawn"); },
    copyText: () => {},
    toastError: () => {},
  });
  vm.runInContext("var currentServer = " + JSON.stringify(picked) + ", defaultServerId = '', capsCache = { monitor: true };", c);
  for (const name of ["el", "mcpSelector", "shellWord", "flashbackHost", "sqlPortAddress", "cnSection", "cnFact", "cnFine", "utcLabel",
    "postgresInstall", "pgPortLine", "pgForwardSaid", "sqlClientPanel"]) {
    vm.runInContext(fn(name), c);
  }
  c.args = { servers, fb, reveal, split };
  return vm.runInContext("sqlClientPanel(args.servers, args.fb, args.reveal, null, undefined, args.split).textContent", c);
}

test("drawn for a PostgreSQL server: on", () => {
  const on = { enabled: true, can_manage: true, password_created_at: "2026-11-15T10:00:00Z" };
  const text = draw({ servers: [pg("shop")], fb: on, reveal: "bfp_secret" });
  for (const want of ['psql "host=dbtrail.example port=5433 dbname=shop user=app sslmode=require"', "Database", "5433",
    "the PostgreSQL router's usual number", "bfp_secret", "Nothing is sent to your PostgreSQL for a server until its form (Servers, Edit) lets the port forward", "The user can be anything"]) {
    assert.ok(text.includes(want), "lacks " + JSON.stringify(want) + ":\n" + text);
  }
  for (const not of ["mysql -h", "3309", "Engine Routing", "forwarding account", "_flashback", "MySQL"]) {
    assert.ok(!text.includes(not), "says " + JSON.stringify(not) + ":\n" + text);
  }
});

test("drawn for a PostgreSQL server: the page with two sections", () => {
  const text = draw({ servers: [pg("shop")], fb: { enabled: true, can_manage: true }, split: true });
  for (const want of ["PostgreSQL client", "Read your copy from a PostgreSQL client", "port=5433 dbname=shop",
    "On the PostgreSQL port, the copy answers what it is known to answer", "The PostgreSQL port does not serve shop"]) {
    assert.ok(text.includes(want), "lacks " + JSON.stringify(want) + ":\n" + text);
  }
  for (const not of ["mysql -h", "3309", "Run MySQL and DuckDB", "Put ProxySQL in front", "Read your copy from a MySQL client"]) {
    assert.ok(!text.includes(not), "says " + JSON.stringify(not) + ":\n" + text);
  }
});

test("drawn for a PostgreSQL server: off, and no server picked", () => {
  const off = draw({ servers: [pg("shop")], fb: { enabled: false, can_manage: true } });
  assert.ok(off.includes("your usual PostgreSQL client can read the copy of your tables"), off);
  assert.ok(!off.includes("MySQL client"), off);
  const none = draw({ servers: [pg("shop"), pg("crm")], fb: { enabled: true, can_manage: true }, picked: "" });
  assert.ok(none.includes("dbname=<server-name>"), none);
});

test("drawn for a MySQL server: off, as it always was", () => {
  const off = draw({ servers: [my("shop")], fb: { enabled: false, can_manage: true } });
  assert.ok(off.includes("your usual MySQL client can read any table as it was at a chosen moment"), off);
  assert.ok(!off.includes("PostgreSQL"), off);
});

// What the port sends to the picked server's PostgreSQL (#3054): each state a
// server can be in, drawn by the real panel and read whole.
test("drawn for a PostgreSQL server: what the port forwards as", () => {
  const fb = { enabled: true, can_manage: true };
  const line = (server, picked = "shop") => {
    const text = draw({ servers: [server], fb, split: true, picked });
    const at = text.indexOf("On the PostgreSQL port, the copy answers what it is known to answer as PostgreSQL does. ");
    assert.ok(at >= 0, text);
    return text.slice(at + "On the PostgreSQL port, the copy answers what it is known to answer as PostgreSQL does. ".length).trim();
  };
  const shop = { ...pg("shop"), source_user: "dbtrail" };
  const cases = [
    ["nobody said yes", shop,
      "The PostgreSQL port does not serve shop: a client that connects gets an error that says why. Tick the box about the source user, or set a separate forwarding user. To change it, edit the server (Servers, Edit)."],
    ["the box is ticked", { ...shop, forward_with_capture_account: true },
      "For shop, a SELECT the copy does not answer runs on your PostgreSQL, read-only and as the source user (dbtrail), when every table it reads is a table of the copy; anything else is refused and not sent. To change it, edit the server (Servers, Edit)."],
    ["a forwarding user of its own", { ...shop, has_route: true, route_user: "app_ro", forward_with_capture_account: true },
      "For shop, every other statement, and a transaction block (BEGIN to COMMIT or ROLLBACK), runs on your PostgreSQL as its forwarding user, app_ro, read-only. To change it, edit the server (Servers, Edit)."],
    ["a saved forwarding user that is the source user, and the box ticked", { ...shop, route_is_capture: true, forward_with_capture_account: true },
      "For shop, a SELECT the copy does not answer runs on your PostgreSQL, read-only and as the source user (dbtrail), when every table it reads is a table of the copy; anything else is refused and not sent. To change it, edit the server (Servers, Edit)."],
    ["a saved forwarding user that is the source user, the box unticked", { ...shop, route_is_capture: true },
      "The PostgreSQL port does not serve shop: its saved forwarding user is one the port refuses. Edit the server (Servers, Edit) and tick the box about the source user, or remove that user and set another."],
    ["ticked, on a server that hides a table", { ...shop, forward_with_capture_account: true, hidden: { tables: ["shop.audit"], columns: [] } },
      "For shop, nothing is sent to your PostgreSQL: this server hides tables or columns, which your PostgreSQL would not hide, so the copy answers alone."],
    ["a forwarding user of its own, on a server that hides a column", { ...shop, has_route: true, route_user: "app_ro", hidden: { tables: [], columns: ["shop.users.email"] } },
      "For shop, nothing is sent to your PostgreSQL: this server hides tables or columns, which your PostgreSQL would not hide, so the copy answers alone."],
    ["ticked, and the source URL cannot be forwarded with", { ...shop, forward_with_capture_account: true, capture_account_refused: "the server's source URL asks for replication: set a separate forwarding user instead, or correct the source" },
      "The PostgreSQL port does not serve shop: the server's source URL asks for replication: set a separate forwarding user instead, or correct the source."],
  ];
  for (const [name, server, want] of cases) assert.equal(line(server), want, name);
  assert.equal(line(shop, ""), "What it does not answer goes to your PostgreSQL, read-only. A server the port cannot forward for is not served: a client that connects gets an error that says why.", "no server picked");
  for (const [name, server] of cases) {
    const said = line(server);
    assert.ok(!/undefined|null|MySQL|\u2014/.test(said), name + ": " + said);
  }
});
