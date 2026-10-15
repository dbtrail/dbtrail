// The Updated card's line for a failed update (#2410). Run with:
//
//   node --test test/console-e2e/hero_fail.test.mjs
//
// It runs the real heroFailWhy from app.js (with the two helpers it calls) in
// a vm context: no daemon, no browser.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import vm from "node:vm";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const APP = readFileSync(path.join(HERE, "../../internal/console/assets/app.js"), "utf8");

// fn returns the source of a top-level function of app.js, by brace count.
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
vm.runInContext(["plainWords", "copyJobFirstLine", "heroFailWhy"].map(fn).join("\n"), ctx);
const why = (run, err) => ctx.heroFailWhy(run, err);

// The reason the blog box gave on 2026-10-15, one table of seventeen.
const GAP = "fetch events: query covers hours with no data (rotated and not archived): 2026-10-15 14:00 – 2026-10-15 14:00 — the delta window starts at this table's newest usable baseline anchor (2026-10-15T10:45:13Z); if archive_state drifted, `bintrail archive reconcile --repair` is the cheap fix, and if the missing hours were rotated out unarchived, take a fresh baseline (bintrail dump + bintrail baseline)";

test("seventeen refused tables: one line, the count and the first", () => {
  const tables = Array.from({ length: 10 }, (_, i) => ({ name: "demo.t" + i, verdict: "refused", reason: GAP }));
  const s = why({ refused: 17, refused_tables: tables, refused_tables_omitted: 7 }, tables.map((t) => t.name + ": " + GAP).join("\n"));
  assert.match(s, /^The last update failed: 17 tables were refused\. The first, demo\.t0: fetch events: query covers hours with no data/);
  assert.match(s, /\.\.\. Settings lists each table and why\.$/);
  assert.ok(s.length < 260, "one line, got " + s.length + " characters: " + s);
  assert.ok(!s.includes("—"), "no em dash");
});

test("one refused table says it once, without 'the first'", () => {
  assert.equal(why({ refused: 1, refused_tables: [{ name: "a.b", reason: "schema changed." }] }, "a.b: schema changed."),
    "The last update failed: 1 table was refused. a.b: schema changed. Settings lists each table and why.");
});

test("a count with no names, a name with no reason", () => {
  assert.equal(why({ refused: 3 }, "x"), "The last update failed: 3 tables were refused. Settings lists each table and why.");
  assert.equal(why({ refused: 2, refused_tables: [{ name: "a.b" }, { name: "c.d" }] }, ""),
    "The last update failed: 2 tables were refused. The first, a.b. Settings lists each table and why.");
});

test("the omitted ones count when refused is missing", () => {
  assert.match(why({ refused_tables: [{ name: "a.b", reason: "r" }], refused_tables_omitted: 4 }, ""), /^The last update failed: 5 tables were refused\./);
});

test("no refusal: the error's first line, capped", () => {
  assert.equal(why({}, ""), "The last update failed.");
  assert.equal(why(null, null), "The last update failed.");
  assert.equal(why({ refused: 0 }, "disk full\nmore detail"), "The last update failed: disk full.");
  assert.equal(why({}, "it stopped."), "The last update failed: it stopped.");
  const long = why({}, "x".repeat(500));
  assert.ok(long.length < 170, long.length);
});

test("junk in refused_tables is skipped", () => {
  assert.equal(why({ refused: 1, refused_tables: [null, { reason: "no name" }, { name: "a.b", reason: "r" }] }, ""),
    "The last update failed: 1 table was refused. a.b: r. Settings lists each table and why.");
});
