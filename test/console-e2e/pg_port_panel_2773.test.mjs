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
