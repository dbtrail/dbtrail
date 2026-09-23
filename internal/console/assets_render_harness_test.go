package console

// renderHarnessJS is the node prelude that loads the real app.js (argv[2])
// into a vm context with a fake DOM, so a test can call a page function and
// read the text it renders. Page globals (capsCache and friends) are set with
// vm.runInContext, because top-level let bindings are not properties of ctx.
const renderHarnessJS = `const fs = require("fs"), vm = require("vm");
class FakeEl {
  constructor(tag) { this.tag = tag; this.children = []; this.className = ""; this._text = ""; this.attrs = {}; this.hidden = false; this.nodeType = 1; this.value = ""; this.checked = false; this.style = {}; this.dataset = {};
    this.classList = { add: (c) => { this.className += " " + c; }, remove() {}, toggle() {}, contains: () => false }; }
  set textContent(v) { this._text = String(v); this.children = []; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  append(...k) { for (const x of k) if (x != null) this.children.push(x); }
  appendChild(x) { this.children.push(x); return x; }
  prepend(...k) { this.children.unshift(...k); }
  replaceChildren(...k) { this.children = k; }
  setAttribute(k, v) { this.attrs[k] = v; if (k === "value") this.value = v; }
  getAttribute(k) { return this.attrs[k]; }
  addEventListener() {} removeEventListener() {}
  querySelector() { return null; } querySelectorAll() { return []; }
  set innerHTML(v) { this._text = String(v); }
}
const permissive = () => new Proxy(function () {}, { get: (t, p) => p === Symbol.toPrimitive ? () => "" : (p === "then" ? undefined : permissive()), apply: () => permissive(), construct: () => permissive() });
const document = { createElement: (t) => new FakeEl(t), createTextNode: (s) => ({ nodeType: 3, textContent: String(s) }), addEventListener() {}, querySelector: () => null, querySelectorAll: () => [], body: new FakeEl("body"), documentElement: new FakeEl("html"), getElementById: () => new FakeEl("div"), readyState: "loading", createElementNS: (n, t) => new FakeEl(t) };
const ctx = { document, console, setTimeout: () => 0, clearTimeout() {}, setInterval: () => 0, clearInterval() {}, URLSearchParams, URL, Intl, Date, Math, JSON, Set, Map, RegExp, Promise, AbortController, Event, EventTarget,
  location: { pathname: "/settings", search: "", hash: "", origin: "http://x" }, localStorage: { getItem: () => null, setItem() {}, removeItem() {} }, sessionStorage: { getItem: () => null, setItem() {} },
  navigator: permissive(), history: permissive(), fetch: () => new Promise(() => {}), matchMedia: () => ({ matches: false, addEventListener() {} }), DOMParser: function () { this.parseFromString = () => ({ documentElement: new FakeEl("svg") }); },
  requestAnimationFrame: () => 0, MutationObserver: function () { this.observe = () => {}; }, ResizeObserver: function () { this.observe = () => {}; }, IntersectionObserver: function () { this.observe = () => {}; }, getComputedStyle: () => ({}), CSS: { supports: () => false }, performance: { now: () => 0 }, EventSource: function () {} };
ctx.window = ctx; ctx.globalThis = ctx; ctx.self = ctx;
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(process.argv[2], "utf8"), ctx);
`
