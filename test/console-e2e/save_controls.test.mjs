// The guard that keeps the Save-control list complete (#1883). Run with:
//
//   node --test test/console-e2e/save_controls.test.mjs
//
// It needs no daemon and no browser: it reads app.js, finds every call that
// sends a writing method, and fails when one is not on the list in
// save_controls.mjs (or the list names one app.js no longer makes). A new
// Save control therefore fails CI until its scene exists. run.sh runs it
// before the browser suite.
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { WRITES, extractWrites, checkInventory, checkEntries, checkScenesRan } from "./save_controls.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const APP = readFileSync(path.join(HERE, "../../internal/console/assets/app.js"), "utf8");
const E2E = readFileSync(path.join(HERE, "console_e2e.mjs"), "utf8");
const SCENES = readFileSync(path.join(HERE, "save_controls.mjs"), "utf8");

const keys = (writes) => writes.map((w) => w.method + " " + w.path).sort();

describe("the list against app.js", () => {
  test("every write in app.js is on the list, and every entry is still in app.js", () => {
    const { unlisted, stale } = checkInventory(extractWrites(APP));
    assert.deepEqual(unlisted, [], "writes with no scene and no reason; add each to WRITES in save_controls.mjs:\n  " + unlisted.join("\n  "));
    assert.deepEqual(stale, [], "entries app.js no longer makes; update WRITES:\n  " + stale.join("\n  "));
  });

  test("the scan finds writes at all (a scan that finds nothing would pass the test above only with an empty list)", () => {
    const n = extractWrites(APP).length;
    const listed = WRITES.reduce((a, e) => a + e.count, 0);
    assert.equal(n, listed);
    assert.ok(n >= 30, `only ${n} writes found in app.js`);
  });

  test("every entry says how it is covered", () => {
    assert.deepEqual(checkEntries(), []);
  });

  test("an entry deleted from the list turns the check red", () => {
    const without = WRITES.filter((e) => !(e.method === "PUT" && e.path === "/api/rotation"));
    const { unlisted } = checkInventory(extractWrites(APP), without);
    assert.equal(unlisted.length, 1);
    assert.match(unlisted[0], /^PUT \/api\/rotation /);
  });

  test("a new PUT in app.js turns the check red", () => {
    const extra = APP + '\nasync function saveNewThing(v) { await api("/api/new-thing", { method: "PUT", body: { v } }); }\n';
    const { unlisted } = checkInventory(extractWrites(extra));
    assert.equal(unlisted.length, 1);
    assert.match(unlisted[0], /^PUT \/api\/new-thing /);
  });

  test("a second call site of a listed write turns the check red", () => {
    const extra = APP + '\nfunction again() { return api("/api/rotation", { method: "PUT", body: {} }); }\n';
    const { unlisted } = checkInventory(extractWrites(extra));
    assert.deepEqual(unlisted.map((u) => u.split(" (")[0]), ["PUT /api/rotation"]);
  });

  test("an entry app.js no longer makes turns the check red", () => {
    const { stale } = checkInventory(extractWrites(APP), [...WRITES, { method: "DELETE", path: "/api/gone", count: 1, kind: "action", reason: "x" }]);
    assert.deepEqual(stale, ["DELETE /api/gone (listed 1, found 0)"]);
  });
});

describe("reading call shapes", () => {
  const one = (src) => keys(extractWrites(src));

  test("a literal method and a literal path", () => {
    assert.deepEqual(one('api("/api/a", { method: "POST", body: {} });'), ["POST /api/a"]);
  });
  test("GET and no method are not writes", () => {
    assert.deepEqual(one('api("/api/a"); api("/api/b", { method: "GET" }); api("/api/c", { signal: s });'), []);
  });
  test("a lowercase method is still a write (api() uppercases it)", () => {
    assert.deepEqual(one("api('/api/a', { method: 'put' });"), ["PUT /api/a"]);
  });
  test("a method with no space after the colon, and options on several lines", () => {
    assert.deepEqual(one('api("/api/a",{method:"DELETE"}); api("/api/b", {\n  body,\n  method: "PUT",\n});'), ["DELETE /api/a", "PUT /api/b"]);
  });
  test("a path built with encodeURIComponent and a template literal become {id}", () => {
    assert.deepEqual(one('api("/api/s/" + encodeURIComponent(id) + "/x", { method: "POST" }); api(`/api/t/${id}/y`, { method: "PUT" });'),
      ["POST /api/s/{id}/x", "PUT /api/t/{id}/y"]);
  });
  test("a ternary path with a ternary method is paired branch by branch, not crossed", () => {
    assert.deepEqual(one('api(id ? "/api/s/" + encodeURIComponent(id) : "/api/s", { method: id ? "PUT" : "POST", body });'),
      ["POST /api/s", "PUT /api/s/{id}"]);
  });
  test("a ternary path with one method gives that method to both", () => {
    assert.deepEqual(one('api(id ? "/api/s/" + encodeURIComponent(id) + "/test" : "/api/s/test", { method: "POST", body });'),
      ["POST /api/s/test", "POST /api/s/{id}/test"]);
  });
  test("a raw fetch with a method counts, a raw fetch without one does not", () => {
    assert.deepEqual(one('await fetch("/api/login", { method: "POST", headers }); await fetch("/api/dl?at=" + at, { headers });'),
      ["POST /api/login"]);
  });
  test("a wrapper's writes are read at its call sites", () => {
    const src = 'async function mutate(path, body) { await api(path, { method: "POST", body }); }\n' +
      'mutate("/api/x/add", {}); mutate("/api/x/remove", {});';
    assert.deepEqual(one(src), ["POST /api/x/add", "POST /api/x/remove"]);
  });
  test("the api() helper itself, forwarding opts.method, is not a call site", () => {
    assert.deepEqual(one('async function api(path, opts = {}) { const res = await fetch(path, { method: opts.method || "GET", headers }); }'), []);
  });
  test("a name that only ends in api or fetch is not a call", () => {
    assert.deepEqual(one('myapi("/api/a", { method: "POST" }); prefetch("/api/b", { method: "POST" }); x.api("/api/c", { method: "PUT" });'), []);
  });
  test("a comma or bracket inside a string does not split the arguments", () => {
    assert.deepEqual(one('api("/api/a,(b)", { method: "PUT", body: { t: "x, y)" } });'), ["PUT /api/a,(b)"]);
  });
});

describe("every covered entry has a scene that can report", () => {
  test("each scene id appears in a check(...) of save_controls.mjs", () => {
    const ids = [...new Set(WRITES.filter((e) => e.kind === "scene").map((e) => e.id))];
    const missing = ids.filter((id) => !SCENES.includes(`scene("${id}"`) || !SCENES.includes(`check("${id}"`));
    assert.deepEqual(missing, []);
  });
  test("each elsewhere label is a result name in console_e2e.mjs", () => {
    const missing = WRITES.filter((e) => e.kind === "elsewhere" && !E2E.includes(`ok("${e.label}")`)).map((e) => e.label);
    assert.deepEqual([...new Set(missing)], []);
  });
  test("console_e2e.mjs runs the scenes and the runtime check", () => {
    assert.match(E2E, /runSaveScenes\(/);
    assert.match(E2E, /checkScenesRan\(/);
  });
});

describe("the runtime check", () => {
  const inv = [
    { method: "PUT", path: "/a", count: 1, kind: "scene", id: "a" },
    { method: "PUT", path: "/b", count: 1, kind: "elsewhere", label: "b: stored" },
    { method: "POST", path: "/c", count: 1, kind: "action", reason: "job" },
  ];
  test("passes when every scene and label reported, pass or fail", () => {
    assert.deepEqual(checkScenesRan([{ name: "save a: x", pass: false }, { name: "b: stored", pass: true }], { inventory: inv }), []);
  });
  test("names a scene that reported nothing and a label that never ran", () => {
    assert.deepEqual(checkScenesRan([{ name: "save ab: x", pass: true }], { inventory: inv }), ["save a", "b: stored"]);
  });
  test("a declared skip is allowed off CI and refused in CI", () => {
    const r = [{ name: "b: stored", pass: true }];
    assert.deepEqual(checkScenesRan(r, { inventory: inv, skipped: ["a"], inCI: false }), []);
    assert.deepEqual(checkScenesRan(r, { inventory: inv, skipped: ["a"], inCI: true }), ["save a"]);
  });
});
