// The Access profiles page is shown only where the build turns it on
// (capabilities access_profiles). Run with:
//
//   node --test test/console-e2e/access_profiles_hidden.test.mjs
//
// It runs the real cmdkCommands, renderAccessProfiles and hiddenDoesNot from
// app.js in a vm context.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import vm from "node:vm";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const APP = readFileSync(path.join(HERE, "../../internal/console/assets/app.js"), "utf8");
function fn(name, prefix = "function ") {
  const start = APP.indexOf("\n" + prefix + name + "(");
  assert.ok(start >= 0, name + " is not in app.js");
  let depth = 0;
  for (let i = APP.indexOf("{", start); i < APP.length; i++) {
    if (APP[i] === "{") depth++;
    else if (APP[i] === "}" && --depth === 0) return APP.slice(start, i + 1);
  }
  throw new Error("unbalanced " + name);
}
function constDecl(name) {
  const start = APP.indexOf("\nconst " + name + " = ");
  assert.ok(start >= 0, name + " is not in app.js");
  return APP.slice(start, APP.indexOf(";\n", start) + 1);
}

function load(caps) {
  const calls = [];
  const ctx = vm.createContext({
    capsCache: caps,
    serverGen: 0, viewGen: 0,
    history: { replaceState: (_s, _t, url) => calls.push("replace " + url) },
    renderRoute: () => calls.push("renderRoute"),
    viewLoading: () => calls.push("viewLoading"),
    api: () => new Promise(() => {}),
  });
  vm.runInContext(fn("cmdkCommands") + fn("renderAccessProfiles", "async function ") + constDecl("hiddenDoesNot"), ctx);
  return { ctx, calls };
}
const labels = (ctx) => ctx.cmdkCommands().map((c) => c.label);

test("stock build: no palette entry", () => {
  assert.ok(!labels(load({}).ctx).includes("Access profiles"));
});

test("turned on: the palette offers it", () => {
  assert.ok(labels(load({ access_profiles: true }).ctx).includes("Access profiles"));
});

test("stock build: a typed /access-profiles rewrites to the Overview and loads nothing", () => {
  const { ctx, calls } = load({});
  ctx.renderAccessProfiles();
  assert.deepEqual(calls, ["replace /overview", "renderRoute"]);
});

test("turned on: the page loads", () => {
  const { ctx, calls } = load({ access_profiles: true });
  ctx.renderAccessProfiles();
  assert.deepEqual(calls, ["viewLoading"]);
});

test("the hidden-rules aside names access profiles only where the page exists", () => {
  const off = vm.runInContext("hiddenDoesNot()", load({}).ctx);
  const on = vm.runInContext("hiddenDoesNot()", load({ access_profiles: true }).ctx);
  assert.equal(off, "The data stays in the copy's files and in the index, so time travel in this console and the AI connector's history tools still see full rows, and so do downloads and anyone with the files.");
  assert.equal(on, "The data stays in the copy's files and in the index, so time travel in this console and the AI connector's history tools (both follow access profiles) still see full rows, and so do downloads and anyone with the files.");
});
