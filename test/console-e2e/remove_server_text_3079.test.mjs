// The question asked before a server is removed (#3079). Run with:
//
//   node --test test/console-e2e/remove_server_text_3079.test.mjs
//
// It needs no daemon and no browser: it takes the function out of app.js.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const APP = readFileSync(path.join(HERE, "../../internal/console/assets/app.js"), "utf8");
const start = APP.indexOf("function removeServerPrompt(s) {");
assert.ok(start >= 0, "removeServerPrompt is not in app.js");
const end = APP.indexOf("\n}\n", start);
const prompt = new Function(APP.slice(start, end + 2) + "\nreturn removeServerPrompt;")();

const dropOf = (text) => text.slice(text.indexOf("SELECT"));

test("MySQL, MariaDB and a server with no flavor keep their text, byte for byte", () => {
  const want = 'Remove server "db1"? This only removes the saved connection; nothing happens to the server itself.';
  for (const flavor of ["mysql", "mariadb", "", undefined]) {
    assert.equal(prompt({ name: "db1", flavor, source_slot: "ignored" }), want);
  }
});

test("PostgreSQL names the slot, what it does and the statement", () => {
  for (const flavor of ["postgres", "postgresql"]) {
    const text = prompt({ name: "shop", flavor, source_slot: "bintrail_shop" });
    assert.ok(!text.includes("nothing happens"), text);
    assert.ok(text.includes('replication slot "bintrail_shop"'), text);
    assert.ok(text.includes("keeps holding WAL"), text);
    assert.equal(dropOf(text), "SELECT pg_drop_replication_slot('bintrail_shop');");
  }
});

const LIST = "SELECT slot_name, active FROM pg_replication_slots;";

test("a saved value PostgreSQL would not take as a slot name is not repeated", () => {
  for (const source_slot of [
    "a'); DROP TABLE t; --", "a'b", "a\\b", "a\\'); DROP TABLE t; --", "a\nb", "a\r\nSELECT 1", "ok\n",
    " ok", "ok ", "Upper", "ñandú", "a-b", "a.b", "x".repeat(64), 7, {},
  ]) {
    const text = prompt({ name: "x", flavor: "postgres", source_slot });
    assert.ok(!text.includes("pg_drop_replication_slot"), JSON.stringify(source_slot));
    assert.equal(dropOf(text), LIST);
    assert.ok(text.includes("not a valid PostgreSQL slot name"), text);
    if (typeof source_slot === "string") assert.ok(!text.includes(source_slot), JSON.stringify(source_slot));
  }
});

test("the longest and shortest valid names are used as they are", () => {
  for (const slot of ["a", "0", "_", "x".repeat(63), "bintrail_shop_2"]) {
    assert.equal(dropOf(prompt({ name: "x", flavor: "postgres", source_slot: slot })), "SELECT pg_drop_replication_slot('" + slot + "');");
  }
});

test("with no saved slot the text invents no name", () => {
  for (const source_slot of [undefined, null, ""]) {
    const text = prompt({ name: "shop", flavor: "postgres", source_database: "shop", source_slot });
    assert.ok(!text.includes("pg_drop_replication_slot"), text);
    assert.ok(!text.includes("bintrail_shop"), text);
    assert.ok(text.includes("no slot name saved"), text);
    assert.equal(dropOf(text), LIST);
  }
});

const nameIn = (text) => text.slice('Remove server "'.length, text.indexOf('"? This removes'));

test("a server name cannot imitate the PostgreSQL text", () => {
  for (const [name, want] of [
    ['a"b\'c', "a b c"],
    ["x\n\ny", "x  y"],
    ["a\u200eb\u200fc\u202ed\u2066e\u061cf\u2028g", "a b c d e f g"],
    ["x'); SELECT pg_drop_replication_slot('other');", "x    SELECT pg_drop_replication_slot  other   "],
    ["Ñandú 日本 db-1_a.b", "Ñandú 日本 db-1_a.b"],
  ]) {
    const text = prompt({ name, flavor: "postgres", source_slot: "ok_slot" });
    assert.equal(nameIn(text), want);
    assert.equal(text.split("\n").length, 3, text);
    assert.ok(text.endsWith("\n\nSELECT pg_drop_replication_slot('ok_slot');"), text);
  }
});

test("a long server name is cut to 64 characters", () => {
  assert.equal(nameIn(prompt({ name: "n".repeat(200), flavor: "postgres", source_slot: "s" })), "n".repeat(64) + "…");
  assert.equal(nameIn(prompt({ name: "n".repeat(64), flavor: "postgres", source_slot: "s" })), "n".repeat(64));
  assert.equal(nameIn(prompt({ name: "日".repeat(65), flavor: "postgres", source_slot: "s" })), "日".repeat(64) + "…");
});

test("a MySQL server's name is left exactly as it was", () => {
  for (const flavor of ["mysql", "mariadb"]) {
    const name = 'a"\nb\u202e' + "n".repeat(200);
    assert.equal(prompt({ name, flavor }), 'Remove server "' + name + '"? This only removes the saved connection; nothing happens to the server itself.');
  }
});
