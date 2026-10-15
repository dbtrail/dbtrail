// The routing card's forwarding-account block (#2415). Run with:
//
//   node --test test/console-e2e/route_grants.test.mjs
//
// It runs the real routeGrantsModel from app.js in a vm context.
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
vm.runInContext(fn("routeGrantsModel"), ctx);
const model = (v, ro) => JSON.parse(JSON.stringify(ctx.routeGrantsModel(v, ro)));
const LINES = ["GRANT USAGE ON *.* TO `r`@`%`", "GRANT SELECT ON `shop`.* TO `r`@`%`"];

test("only reads: read-only, no note", () => {
  const m = model({ user: "r", grants: LINES }, true);
  assert.equal(m.tone, "ok");
  assert.equal(m.verdict, "Read-only: every privilege only reads");
  assert.deepEqual(m.notes, []);
  assert.deepEqual(m.lines, LINES);
});

test("administration privileges are never read-only", () => {
  const m = model({ user: "r", grants: LINES, other: ["BINLOG_ADMIN", "SUPER"] }, true);
  assert.equal(m.tone, "warn");
  assert.equal(m.verdict, "Not read-only: BINLOG_ADMIN, SUPER");
});

test("EXECUTE: a SELECT can run a function that writes, past a read-only router", () => {
  const m = model({ user: "r", grants: LINES, execute: true }, true);
  assert.equal(m.tone, "warn");
  assert.equal(m.verdict, "Can run stored routines (EXECUTE)");
  assert.match(m.notes.join(" "), /The router sees only the SELECT, so it lets it through/);
  assert.match(model({ user: "r", grants: LINES, writes: ["INSERT"], execute: true }, true).notes.join(" "), /stored function/);
});

test("writes under a read-only router: refused before MySQL", () => {
  const m = model({ user: "r", grants: LINES, writes: ["INSERT", "UPDATE"] }, true);
  assert.equal(m.tone, "bad");
  assert.equal(m.verdict, "Can change data: INSERT, UPDATE");
  assert.match(m.notes[0], /read-only: it refuses a statement that changes data before it reaches MySQL/);
});

test("writes under a read-write router: they run on MySQL", () => {
  assert.match(model({ user: "r", grants: LINES, writes: ["DELETE"] }, false).notes[0], /read-write: a statement that needs them runs on your MySQL/);
});

test("writes with the router's rule unknown: no claim about it", () => {
  assert.deepEqual(model({ user: "r", grants: LINES, writes: ["DELETE"] }, null).notes, []);
});

test("roles without writes: never called read-only", () => {
  const one = model({ user: "r", grants: LINES, roles: ["`reader`@`%`"] }, true);
  assert.equal(one.tone, "warn");
  assert.equal(one.verdict, "Has a role: SHOW GRANTS does not list what it allows");
  assert.equal(model({ user: "r", grants: LINES, roles: ["a", "b"] }, true).verdict, "Has 2 roles: SHOW GRANTS does not list what they allow");
});

test("writes win over roles", () => {
  assert.equal(model({ user: "r", grants: LINES, writes: ["INSERT"], roles: ["a"] }, true).tone, "bad");
});

test("none, skipped and error say why, capitalized and with a period", () => {
  assert.deepEqual(model({ none: "no forwarding account: the copy answers every statement for this server" }, true),
    { user: "", state: "none", text: "No forwarding account: the copy answers every statement for this server." });
  assert.equal(model({ user: "r", skipped: "this DBTrail process has no MySQL client to log in with" }).text,
    "Its grants were not read: this DBTrail process has no MySQL client to log in with.");
  assert.equal(model({ user: "r", error: "Access denied for user 'r'" }).state, "error");
});

test("no lines is never read-only", () => {
  assert.equal(model(null, true).state, "error");
  assert.equal(model({ user: "r", grants: [] }, true).state, "error");
});
