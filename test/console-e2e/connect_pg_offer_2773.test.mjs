// The Connect screen's way to the full form for a PostgreSQL (#2773). Run with:
//
//   node --test test/console-e2e/connect_pg_offer_2773.test.mjs
//
// The whole app.js is loaded into a vm context over a small stand-in for the
// DOM; the real showNotFound draws step 1's answer, and the button it draws
// is pressed.
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
    this.nodeType = 1; this.value = ""; this.style = {}; this.dataset = {}; this.id = "";
    const has = (c) => (" " + this.className + " ").includes(" " + c + " ");
    this.classList = { add: (c) => { if (!has(c)) this.className += " " + c; }, remove() {}, toggle: () => false, contains: has };
  }
  set textContent(v) { this._text = String(v); this.children = []; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(" "); }
  append(...k) { for (const x of k) if (x != null) this.children.push(x); }
  appendChild(x) { this.children.push(x); return x; }
  replaceChildren(...k) { this.children = k; }
  setAttribute(k, v) { this.attrs[k] = v; if (k === "id") this.id = v; }
  getAttribute(k) { return this.attrs[k]; }
  // el() wires onclick through addEventListener: kept, so a test can press.
  addEventListener(ev, fn) { if (ev === "click") this.onclick = fn; } removeEventListener() {}
  all() { const out = []; const walk = (n) => { if (!n || n.nodeType !== 1) return; out.push(n); n.children.forEach(walk); }; this.children.forEach(walk); return out; }
  // "#id", "tag#id" and "tag[attr]" are what the Connect code asks for.
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  querySelectorAll(sel) {
    const m = /^([a-z]*)(?:#([\w-]+))?(?:\[([\w-]+)(?:=(\w+))?\])?$/.exec(sel);
    if (!m) return [];
    return this.all().filter((n) => (!m[1] || n.tag === m[1]) && (!m[2] || n.id === m[2] || n.attrs.id === m[2])
      && (!m[4] || n.attrs[m[3]] === m[4])
      && (!m[3] || (m[3] in n.attrs) || (m[3].startsWith("data-") && n.dataset[m[3].slice(5).replace(/-(\w)/g, (_, c) => c.toUpperCase())] !== undefined)));
  }
}

function load() {
  const permissive = () => new Proxy(function () {}, {
    get: (t, p) => (p === Symbol.toPrimitive ? () => "" : p === "then" ? undefined : permissive()),
    apply: () => permissive(), construct: () => permissive(),
  });
  const document = {
    createElement: (t) => new FakeEl(t), createTextNode: (s) => ({ nodeType: 3, textContent: String(s) }),
    createElementNS: (n, t) => new FakeEl(t), addEventListener() {}, querySelector: () => null, querySelectorAll: () => [],
    body: new FakeEl("body"), documentElement: new FakeEl("html"), getElementById: () => new FakeEl("div"),
    importNode: (n) => n, readyState: "loading",
  };
  const ctx = {
    document, console, setTimeout: () => 0, clearTimeout() {}, setInterval: () => 0, clearInterval() {},
    URLSearchParams, URL, Intl, Date, Math, JSON, Set, Map, WeakMap, WeakSet, RegExp, Promise, AbortController, Event, EventTarget,
    location: { pathname: "/overview", search: "", hash: "", origin: "http://x", hostname: "x" },
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} }, sessionStorage: { getItem: () => null, setItem() {} },
    navigator: permissive(), history: permissive(), fetch: () => new Promise(() => {}),
    matchMedia: () => ({ matches: false, addEventListener() {} }),
    DOMParser: function () { this.parseFromString = () => ({ documentElement: new FakeEl("svg") }); },
    requestAnimationFrame: () => 0, MutationObserver: function () { this.observe = () => {}; },
    ResizeObserver: function () { this.observe = () => {}; }, IntersectionObserver: function () { this.observe = () => {}; },
    getComputedStyle: () => ({}), CSS: { supports: () => false }, performance: { now: () => 0 }, EventSource: function () {},
  };
  ctx.window = ctx; ctx.globalThis = ctx; ctx.self = ctx;
  vm.createContext(ctx);
  vm.runInContext(APP, ctx);
  const run = (s) => vm.runInContext(s, ctx);
  run("api = () => new Promise(() => {}); capsCache = { monitor: true, permissions: null };");
  return { ctx, run };
}

// notFound draws step 1's answer id for host:port on an installation of the
// given family, and returns its text, the PostgreSQL button and what pressing
// it sent to the full form.
function notFound(id, family, port = "5432") {
  const { ctx, run } = load();
  const form = new FakeEl("form");
  const field = (name, value) => { const i = new FakeEl("input"); i.attrs.name = name; i.value = value; return i; };
  const elements = { source_host: field("source_host", "db.internal"), source_port: field("source_port", port),
    source_user: field("source_user", ""), source_password: field("source_password", ""), flavor: field("flavor", "mysql") };
  form.elements = elements;
  const found = new FakeEl("div"); found.id = "connect-found";
  form.append(found);
  ctx.__form = form; ctx.__id = id; ctx.__opened = [];
  run(`installFamily = { family: ${JSON.stringify(family)}, mixed: null };
    openFullForm = (f) => { __opened.push(f.elements.flavor.value); return Promise.resolve(); };
    showNotFound(__form, __id);`);
  const btn = found.all().find((n) => n.tag === "button" && n.id === "connect-as-pg") || null;
  return { text: found.textContent.replace(/\s+/g, " ").trim(), btn, opened: ctx.__opened, flavor: () => elements.flavor.value };
}

const silent = { kind: "not_mysql", answer: "silent" };

test("a silent answer on an installation that can add a PostgreSQL: the way to the full form", () => {
  for (const family of ["", "postgres"]) {
    for (const port of ["5432", "6543"]) {
      const got = notFound(silent, family, port);
      assert.ok(got.btn, "no button for family " + JSON.stringify(family) + " port " + port + ":\n" + got.text);
      assert.equal(got.btn._text, "Add it as PostgreSQL");
      assert.ok(got.text.includes("If it is a PostgreSQL: this screen adds MySQL and MariaDB servers."), got.text);
      // The MySQL answer above it is still said, as before.
      assert.ok(got.text.includes("Something answered, but not MySQL or MariaDB. Check Port."), got.text);
      got.btn.onclick();
      assert.deepEqual(got.opened, ["postgres"], "the full form is opened with PostgreSQL picked");
    }
  }
});

test("nothing offered where a PostgreSQL cannot be, or cannot be added", () => {
  const cases = [
    ["an installation that serves MySQL and MariaDB", silent, "mysql"],
    ["a connection closed at once (a port forward)", { kind: "not_mysql", answer: "closed" }, ""],
    ["something that spoke first, not MySQL", { kind: "not_mysql", answer: "other" }, ""],
    ["a not_mysql answer with no answer field (an older DBTrail)", { kind: "not_mysql" }, ""],
    ["nothing listens", { kind: "port_closed" }, ""],
    ["no answer at all", { kind: "timeout" }, ""],
    ["no such name", { kind: "name_not_found" }, ""],
  ];
  for (const [what, id, family] of cases) {
    const got = notFound(id, family);
    assert.equal(got.btn, null, what + ":\n" + got.text);
    assert.ok(!got.text.includes("If it is a PostgreSQL"), what + ":\n" + got.text);
    assert.equal(got.flavor(), "mysql", what);
  }
});

test("the rule alone", () => {
  const { ctx } = load();
  const offer = (id, family) => vm.runInContext("connectPGOffer(" + JSON.stringify(id) + ", " + JSON.stringify(family) + ")", ctx);
  assert.equal(offer(silent, ""), true);
  assert.equal(offer(silent, "postgres"), true);
  assert.equal(offer(silent, "mysql"), false);
  assert.equal(offer(null, ""), false);
  assert.equal(offer({ kind: "not_mysql", answer: "closed" }, ""), false);
});
