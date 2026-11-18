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
    this.nodeType = 1; this.style = {}; this.dataset = {}; this.id = ""; this.isConnected = true;
    const has = (c) => (" " + this.className + " ").includes(" " + c + " ");
    this.classList = { add: (c) => { if (!has(c)) this.className += " " + c; }, remove() {}, toggle: () => false, contains: has };
  }
  set textContent(v) { this._text = String(v); this.children = []; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(" "); }
  append(...k) { for (const x of k) if (x != null) this.children.push(x); }
  appendChild(x) { this.children.push(x); return x; }
  replaceChildren(...k) { this.children = k; }
  setAttribute(k, v) {
    this.attrs[k] = v;
    if (k === "id") this.id = v;
    if (k === "value") this._value = String(v);
    if (k.startsWith("data-")) this.dataset[k.slice(5).replace(/-(\w)/g, (_, c) => c.toUpperCase())] = String(v);
  }
  // A select holds only one of its options' values, as a browser's does.
  set value(v) {
    if (this.tag === "select") { const o = this.all().find((n) => n.tag === "option" && n.attrs.value === String(v)); this._value = o ? String(v) : ""; }
    else this._value = String(v);
  }
  get value() {
    if (this.tag === "select" && this._value === undefined) { const o = this.all().find((n) => n.tag === "option"); return o ? o.attrs.value : ""; }
    return this._value === undefined ? "" : this._value;
  }
  get elements() {
    const out = {};
    for (const n of this.all()) if (n.attrs.name && !(n.attrs.name in out)) out[n.attrs.name] = n;
    return out;
  }
  focus() {} select() {} blur() {}
  getAttribute(k) { return this.attrs[k]; }
  // el() wires onclick through addEventListener: kept, so a test can press.
  addEventListener(ev, fn) { if (ev === "click") this.onclick = fn; } removeEventListener() {}
  all() { const out = []; const walk = (n) => { if (!n || n.nodeType !== 1) return; out.push(n); n.children.forEach(walk); }; this.children.forEach(walk); return out; }
  // "#id", "tag#id" and "tag[attr]" are what the Connect code asks for.
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  querySelectorAll(sel) {
    if (sel.startsWith(".")) return this.all().filter((n) => (" " + n.className + " ").includes(" " + sel.slice(1) + " "));
    const m = /^([a-z]*)(?:#([\w-]+))?(?:\[([\w-]+)(?:=(\w+))?\])?$/.exec(sel);
    if (!m) return [];
    return this.all().filter((n) => (!m[1] || n.tag === m[1]) && (!m[2] || n.id === m[2] || n.attrs.id === m[2])
      && (!m[4] || n.attrs[m[3]] === m[4])
      && (!m[3] || (m[3] in n.attrs) || (m[3].startsWith("data-") && n.dataset[m[3].slice(5).replace(/-(\w)/g, (_, c) => c.toUpperCase())] !== undefined)));
  }
}

function load() {
  const slots = { "server-add-wrap": new FakeEl("div"), "server-form-mount": new FakeEl("div"), "server-form-msg": new FakeEl("p") };
  const permissive = () => new Proxy(function () {}, {
    get: (t, p) => (p === Symbol.toPrimitive ? () => "" : p === "then" ? undefined : permissive()),
    apply: () => permissive(), construct: () => permissive(),
  });
  const document = {
    createElement: (t) => new FakeEl(t), createTextNode: (s) => ({ nodeType: 3, textContent: String(s) }),
    createElementNS: (n, t) => new FakeEl(t), addEventListener() {}, querySelector: () => null, querySelectorAll: () => [],
    body: new FakeEl("body"), documentElement: new FakeEl("html"),
    // The servers modal's slots, and any element under them by id.
    getElementById: (id) => {
      if (slots[id]) return slots[id];
      for (const root of Object.values(slots)) { const hit = root.all().find((n) => n.id === id); if (hit) return hit; }
      return null;
    },
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
  return { ctx, run, slots };
}

// notFound draws step 1's answer id for host:port on an installation of the
// given family, and returns its text, the PostgreSQL button and what pressing
// it sent to the full form.
function notFound(id, family, port = "5432") {
  const { ctx, run } = load();
  const form = new FakeEl("form");
  const field = (name, value) => { const i = new FakeEl("input"); i.setAttribute("name", name); i.value = value; return i; };
  const elements = { source_host: field("source_host", "db.internal"), source_port: field("source_port", port),
    source_user: field("source_user", ""), source_password: field("source_password", ""), flavor: field("flavor", "mysql") };
  Object.defineProperty(form, "elements", { value: elements });
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

// The real way to the full form, nothing stubbed but the requests: what the
// person finds in it after pressing the button.
async function pressed({ port = "5432", user = "", password = "", family = "", flavorAfter } = {}) {
  const { ctx, run, slots } = load();
  const connect = new FakeEl("form");
  connect.setAttribute("data-connect", "1"); connect.setAttribute("id", "server-form");
  const field = (name, value) => { const i = new FakeEl("input"); i.setAttribute("name", name); i.value = value; return i; };
  const elements = { source_host: field("source_host", "db.internal"), source_port: field("source_port", port),
    source_user: field("source_user", user), source_password: field("source_password", password), flavor: field("flavor", "mysql") };
  Object.defineProperty(connect, "elements", { value: elements });
  const found = new FakeEl("div"); found.id = "connect-found";
  connect.append(found);
  ctx.__form = connect; ctx.__id = silent;
  run(`installFamily = { family: ${JSON.stringify(family)}, mixed: null }; showNotFound(__form, __id);`);
  const btn = found.all().find((n) => n.tag === "button" && n.id === "connect-as-pg");
  // flavorAfter: the family the installation turns out to serve by the time
  // the full form is built.
  if (flavorAfter !== undefined) run(`installFamily = { family: ${JSON.stringify(flavorAfter)}, mixed: null };`);
  await run("openFullFormAsPG(__form)");
  const full = slots["server-form-mount"].all().find((n) => n.tag === "form");
  const e = full.elements;
  return { flavor: e.flavor.value, port: e.source_port.value, user: e.source_user.value, password: e.source_password.value,
    host: e.source_host.value, msg: slots["server-form-msg"].textContent, btn };
}

test("the full form it opens: PostgreSQL, the probed address, no MySQL account", async () => {
  const got = await pressed();
  assert.equal(got.flavor, "postgres");
  assert.equal(got.host, "db.internal");
  assert.equal(got.port, "5432");
  // The MySQL grant block's account (dbtrail and a generated password) is
  // not left in a PostgreSQL form.
  assert.equal(got.user, "");
  assert.equal(got.password, "");
  assert.equal(got.msg, "");
});

test("the full form it opens: a blank Port is the 3306 that was probed", async () => {
  const got = await pressed({ port: "" });
  assert.equal(got.port, "3306");
});

test("the full form it opens: what was typed stays", async () => {
  const got = await pressed({ user: "replicator", password: "s3cret" });
  assert.equal(got.user, "replicator");
  assert.equal(got.password, "s3cret");
});

test("the full form it opens: an installation found to serve MySQL meanwhile says so", async () => {
  const got = await pressed({ flavorAfter: "mysql" });
  assert.notEqual(got.flavor, "postgres");
  assert.ok(got.msg.includes("This installation does not add PostgreSQL servers now."), got.msg);
});

test("nothing offered on an installation that holds both families", () => {
  const { ctx } = load();
  assert.equal(vm.runInContext("connectPGOffer({ kind: 'not_mysql', answer: 'silent' }, '', { mysql: ['a'], postgres: ['b'] })", ctx), false);
});
