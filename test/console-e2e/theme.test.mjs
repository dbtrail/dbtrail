// The theme loader's edge cases (#1969). Run with:
//
//   node --test test/console-e2e/theme.test.mjs
//
// It needs no daemon and no browser: theme.js is a classic script, so each
// case runs the real file in a fresh vm context with a fake window. The
// browser suite covers the wiring (the control in the sidebar, the paint);
// this covers what a browser cannot be made to do cheaply: storage that
// throws, a stored value nobody wrote, the system flipping mid-visit.
// run.sh runs it before the browser suite.
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import vm from "node:vm";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SRC = readFileSync(path.join(HERE, "../../internal/console/assets/theme.js"), "utf8");
const KEY = "dbtrail.theme";

// boot runs theme.js the way a page load does and returns the handles a case
// needs. `stored` seeds storage; `systemDark` is what the OS says; the two
// throw flags make storage fail the way a blocked-site-data browser does.
function boot({ stored, systemDark = false, getThrows = false, setThrows = false, noMatchMedia = false, noStorage = false, oldSafari = false } = {}) {
  const store = new Map();
  if (stored !== undefined) store.set(KEY, stored);
  const attrs = new Map();
  const mq = {
    matches: systemDark,
    listeners: [],
  };
  // Safari before 14: the media query list has addListener and nothing else.
  if (oldSafari) mq.addListener = (fn) => mq.listeners.push(fn);
  else mq.addEventListener = (type, fn) => { if (type === "change") mq.listeners.push(fn); };
  const pageListeners = { storage: [], pageshow: [] };
  const window = {
    addEventListener: (type, fn) => { if (pageListeners[type]) pageListeners[type].push(fn); },
    document: {
      documentElement: {
        setAttribute: (k, v) => attrs.set(k, v),
        getAttribute: (k) => attrs.get(k),
      },
    },
  };
  if (!noMatchMedia) window.matchMedia = (q) => { assert.equal(q, "(prefers-color-scheme: dark)"); return mq; };
  if (!noStorage) {
    window.localStorage = {
      getItem: (k) => { if (getThrows) throw new Error("blocked"); return store.has(k) ? store.get(k) : null; },
      setItem: (k, v) => { if (setThrows) throw new Error("blocked"); store.set(k, String(v)); },
    };
  }
  window.window = window;
  vm.runInNewContext(SRC, window);
  return {
    theme: window.dbtrailTheme,
    painted: () => attrs.get("data-theme"),
    store,
    flipSystem(dark) { mq.matches = dark; for (const fn of mq.listeners) fn({ matches: dark }); },
    // another tab wrote `value` under `key` (the browser has already stored it)
    otherTab(key, value) { if (key !== null) store.set(key, value); for (const fn of pageListeners.storage) fn({ key }); },
    pageshow(persisted) { for (const fn of pageListeners.pageshow) fn({ persisted }); },
  };
}

describe("first paint", () => {
  test("nothing stored: follows the system, light and dark", () => {
    const light = boot();
    assert.equal(light.theme.choice, "system");
    assert.equal(light.painted(), "light");
    const dark = boot({ systemDark: true });
    assert.equal(dark.theme.choice, "system");
    assert.equal(dark.painted(), "dark");
  });

  test("a stored choice wins over the system, both ways", () => {
    assert.equal(boot({ stored: "dark", systemDark: false }).painted(), "dark");
    assert.equal(boot({ stored: "light", systemDark: true }).painted(), "light");
    assert.equal(boot({ stored: "system", systemDark: true }).painted(), "dark");
  });

  test("a stored value nobody wrote reads as system", () => {
    for (const junk of ["", "Dark", "DARK", " dark", "dark ", "dark\n", "blue", "null", "0", "light dark"]) {
      const t = boot({ stored: junk, systemDark: true });
      assert.equal(t.theme.choice, "system", JSON.stringify(junk));
      assert.equal(t.painted(), "dark", JSON.stringify(junk));
    }
  });

  test("storage that throws on read, or is missing, reads as system and does not throw", () => {
    assert.equal(boot({ getThrows: true, systemDark: true }).painted(), "dark");
    assert.equal(boot({ getThrows: true }).painted(), "light");
    assert.equal(boot({ noStorage: true, systemDark: true }).painted(), "dark");
  });

  test("no matchMedia: light, and the choice still works", () => {
    const t = boot({ noMatchMedia: true });
    assert.equal(t.painted(), "light");
    t.theme.set("dark");
    assert.equal(t.painted(), "dark");
  });
});

describe("choosing", () => {
  test("each choice paints at once and is remembered", () => {
    const t = boot();
    t.theme.set("dark");
    assert.equal(t.painted(), "dark");
    assert.equal(t.store.get(KEY), "dark");
    t.theme.set("light");
    assert.equal(t.painted(), "light");
    assert.equal(t.store.get(KEY), "light");
    t.flipSystem(true);
    t.theme.set("system");
    assert.equal(t.painted(), "dark");
    assert.equal(t.store.get(KEY), "system");
    assert.equal(t.theme.choice, "system");
  });

  test("a remembered choice survives the next load", () => {
    const first = boot();
    first.theme.set("dark");
    assert.equal(boot({ stored: first.store.get(KEY) }).painted(), "dark");
  });

  test("storage that throws on write: the choice holds for this visit", () => {
    const t = boot({ setThrows: true });
    t.theme.set("dark");
    assert.equal(t.painted(), "dark");
    assert.equal(t.theme.choice, "dark");
    assert.equal(t.store.has(KEY), false);
  });

  test("a value outside the three choices changes nothing", () => {
    const t = boot({ stored: "dark" });
    for (const junk of ["Dark", "", "blue", undefined, null, 1]) t.theme.set(junk);
    assert.equal(t.theme.choice, "dark");
    assert.equal(t.painted(), "dark");
    assert.equal(t.store.get(KEY), "dark");
  });
});

describe("the system flips mid-visit", () => {
  test("on system: the page follows, both ways", () => {
    const t = boot();
    t.flipSystem(true);
    assert.equal(t.painted(), "dark");
    t.flipSystem(false);
    assert.equal(t.painted(), "light");
  });

  test("on an explicit choice: the page stays", () => {
    const t = boot({ stored: "light" });
    t.flipSystem(true);
    assert.equal(t.painted(), "light");
    t.theme.set("dark");
    t.flipSystem(false);
    assert.equal(t.painted(), "dark");
  });

  test("back on system after an explicit choice: the page follows again", () => {
    const t = boot({ stored: "light" });
    t.flipSystem(true);
    t.theme.set("system");
    assert.equal(t.painted(), "dark");
    t.flipSystem(false);
    assert.equal(t.painted(), "light");
  });
});

describe("the system flips mid-visit, old Safari", () => {
  test("addListener only: the page still follows", () => {
    const t = boot({ oldSafari: true });
    t.flipSystem(true);
    assert.equal(t.painted(), "dark");
  });
});

describe("the choice changes outside this page", () => {
  test("another tab picks a theme: this page repaints and tells the control", () => {
    const t = boot({ stored: "light" });
    const told = [];
    t.theme.onchange = (c) => told.push(c);
    t.otherTab(KEY, "dark");
    assert.equal(t.painted(), "dark");
    assert.equal(t.theme.choice, "dark");
    assert.deepEqual(told, ["dark"]);
  });

  test("another tab writes some other key, or the same choice: nothing happens", () => {
    const t = boot({ stored: "light", systemDark: true });
    const told = [];
    t.theme.onchange = (c) => told.push(c);
    t.otherTab("something.else", "dark");
    t.otherTab(KEY, "light");
    assert.equal(t.painted(), "light");
    assert.deepEqual(told, []);
  });

  test("another tab clears storage, or writes junk: back to system", () => {
    const cleared = boot({ stored: "light", systemDark: true });
    cleared.store.delete(KEY);
    cleared.otherTab(null);
    assert.equal(cleared.painted(), "dark");
    const junk = boot({ stored: "light", systemDark: true });
    junk.otherTab(KEY, "Dark");
    assert.equal(junk.theme.choice, "system");
    assert.equal(junk.painted(), "dark");
  });

  test("this page's own choice does not call the listener", () => {
    const t = boot();
    const told = [];
    t.theme.onchange = (c) => told.push(c);
    t.theme.set("dark");
    assert.deepEqual(told, []);
  });

  test("back from the back/forward cache: the stored choice is read again; a plain load is not", () => {
    const t = boot({ stored: "light" });
    t.store.set(KEY, "dark");
    t.pageshow(false);
    assert.equal(t.painted(), "light");
    t.pageshow(true);
    assert.equal(t.painted(), "dark");
  });

  test("a choice storage refused to keep is not undone by what is stored", () => {
    const t = boot({ stored: "light", setThrows: true });
    t.theme.set("dark");
    t.pageshow(true);
    t.otherTab(null);
    assert.equal(t.painted(), "dark");
    assert.equal(t.theme.choice, "dark");
  });
});
