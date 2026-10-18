// How a schema_changes row reads on the Schema changes page (#2441). Run with:
//
//   node --test test/console-e2e/schema_change_display.test.mjs
//
// It runs the real scDisplay from app.js in a vm context.
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
vm.runInContext(fn("scDisplay"), ctx);

test("a MySQL row keeps file:position and its statement", () => {
  const d = ctx.scDisplay({ binlog_file: "binlog.000042", binlog_pos: 1234, statement: "ALTER TABLE t ADD c INT" });
  assert.equal(d.pos, "binlog.000042:1234");
  assert.equal(d.stmt, "ALTER TABLE t ADD c INT");
  assert.equal(d.title, null);
});

test("an inferred PostgreSQL row reads in words, the marker in the title", () => {
  const q = "/* dbtrail: inferred from relation shape; oid=16411; instant=at-or-before; low=0/16B3748@2026-10-17T10:00:00Z; high=0/16B9A20@2026-10-17T10:20:00.5Z */ ALTER TABLE shop.orders: added note (text)";
  const d = ctx.scDisplay({ binlog_file: "0/16B9A20", binlog_pos: 23829024, statement: q });
  console.log(d.pos + " | " + d.stmt);
  assert.equal(d.pos, "LSN 0/16B9A20");
  assert.equal(d.stmt, "Inferred from the change stream, between 2026-10-17 10:00:00 UTC and 2026-10-17 10:20:00 UTC. ALTER TABLE shop.orders: added note (text)");
  assert.equal(d.title, q);
});

test("an inferred row with no lower bound says at or before", () => {
  const q = "/* dbtrail: inferred from relation shape; oid=7; instant=at-or-before; low=none; high=0/2@2026-10-17T10:20:00Z */ CREATE TABLE s.a (first seen in the stream: created, or added to the publication)";
  const d = ctx.scDisplay({ binlog_file: "0/2", binlog_pos: 2, statement: q });
  assert.match(d.stmt, /^Inferred from the change stream, at or before 2026-10-17 10:20:00 UTC\. CREATE TABLE s\.a/);
});

test("a PostgreSQL TRUNCATE shows its LSN and its statement", () => {
  const d = ctx.scDisplay({ binlog_file: "0/19DF9E8", binlog_pos: 27130344, statement: "TRUNCATE TABLE s.a" });
  assert.equal(d.pos, "LSN 0/19DF9E8");
  assert.equal(d.stmt, "TRUNCATE TABLE s.a");
});

test("an inferred row with an exact instant says when it committed", () => {
  const q = "/* dbtrail: inferred from relation shape; oid=16411; instant=exact; at=2026-10-17T10:12:03.25Z; low=0/16B3748@2026-10-17T10:00:00Z; high=0/16B9A20@2026-10-17T10:20:00Z */ ALTER TABLE shop.orders: added note (text)";
  const d = ctx.scDisplay({ binlog_file: "0/16B9A20", binlog_pos: 23829024, statement: q });
  console.log(d.pos + " | " + d.stmt);
  assert.equal(d.stmt, "Inferred from the change stream, committed at 2026-10-17 10:12:03 UTC. ALTER TABLE shop.orders: added note (text)");
  assert.equal(d.title, q);
});
