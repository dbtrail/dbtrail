// The first-run walk (#1800): a person's first hour with DBTrail, driven in a
// real browser against a fresh `watch` daemon and a stock MySQL 8.4, measured
// into a scoreboard. first-run-walk.sh starts everything and runs this file;
// the scoreboard rules live in first_run_scoreboard.mjs and the in-page
// measurement in first_run_measure.js.
//
// The walk follows the path a person takes on today's interface, the same
// path the 2026-09-22 first-run inventory recorded by hand: create the login,
// add the server (testing it first), see the first change, press Undo, look at
// Backups, set a location on Backup settings, and take the first snapshot.
// When a later change reshapes a screen, the step that drives it changes in
// the same PR, and the scoreboard shows what the change bought.
//
// Every click, typed field, forced choice and trip out of the browser is one
// entry in an event log; every column is computed from that log. The harness
// may WAIT for the product (a person reads a notice for a few seconds) but
// never acts for the person without logging it.
//
// One exemption, stated here because it is the only one: pressing a Copy
// button is the test READING what that button puts on the clipboard, not a
// step of the walk, and it is not counted as a click. Nothing else the walk
// does to take a measurement touches the page.
//
// Modes (FIRST_RUN_WALK_MODE):
//   ratchet         (default) fail when any column is worse than the
//                   committed baseline, first_run_baseline.json
//   target          fail unless every column meets the issue's target
//   write-baseline  write the measured values to the baseline file
import { chromium } from "playwright";
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import {
  deriveRun, compareRatchet, compareTarget, loadBaseline, baselineFrom, renderScoreboard, RUNS,
  parseQuickstartBlock,
} from "./first_run_scoreboard.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const need = (k) => {
  const v = process.env[k];
  if (!v) { console.error(`first-run walk: ${k} is not set (run it through first-run-walk.sh)`); process.exit(2); }
  return v;
};
const CONSOLE_URL = need("CONSOLE_URL");
const SRC_CONTAINER = need("FRW_SRC_CONTAINER");
const SRC_ROOT_PW = need("FRW_SRC_ROOT_PASSWORD");
const SRC_PORT = need("FRW_SRC_PORT");
const SRC_HOST = process.env.FRW_SRC_HOST || "127.0.0.1";
const SCRATCH = need("FRW_SCRATCH");
const REPO_ROOT = need("FRW_REPO_ROOT");
const DOCKER = process.env.DOCKER || "docker";
const MODE = process.env.FIRST_RUN_WALK_MODE || "ratchet";
const ART = process.env.E2E_ARTIFACT_DIR || "/tmp";
const SHOTS = process.env.FIRST_RUN_WALK_SHOTS || "";
const BASELINE_FILE = process.env.FIRST_RUN_WALK_BASELINE || path.join(HERE, "first_run_baseline.json");
const VIEWPORT = { width: 1280, height: 720 };
// How long a change may take to show up on the Overview before it counts as
// "not visible without reloading" — the same 120 s the walk recorded by hand
// used. The first change honestly takes ~20 s today (the list polls at up to
// 15 s once it has been still, and capture writes on its 10 s tick), so a
// tighter window would turn one slow poll on a shared CI machine into "the
// product stopped showing changes", which is the cry-wolf this column exists
// to avoid. The latency itself is not lost: its 5 s sibling reports it, and
// that one is shown rather than ratcheted.
const CHANGE_WAIT_MS = 120000;
// Where the walk asks for snapshots to be written. It does not exist yet: a
// person types a new folder name, which is what the inventory did.
const SNAP_DIR = path.join(SCRATCH, "snapshots");
const LOGIN_PASSWORD = "first-run-walk-pw";

// THE ONLY PLACE IN THIS FILE THAT NAMES A CONSOLE ROUTE. Everything else
// reaches a page through the sidebar entry or the button a person would
// click, found by the words on it.
//
// The Snapshots merge folds the three backup pages into ONE page, "snapshots",
// with sections #checks and #setup, and the sidebar keeps a single entry for
// them. So each destination lists the routes it may live under, the new name
// first, and the walk clicks whichever sidebar entry that build has. When the
// merge lands, drop the old names here; nothing else in this file changes.
//
// One consequence is deliberate: with both destinations on one page, the
// walk's second visit finds what it needs already on screen and records NO
// click, so the merge shows up in the scoreboard as one click fewer rather
// than as a broken test.
const ROUTES = {
  overview: ["overview"],
  snapshotList: ["snapshots", "baselines"],
  snapshotSetup: ["snapshots", "backup-settings"],
};
// The button that takes the first snapshot, by the words a person reads on
// it. "Read database now" is the name the redesign gives it.
const takeSnapshotButton = () => page.getByRole("button", { name: /^(Create backup|Create snapshot|Read database now)$/ });

if (!["ratchet", "target", "write-baseline"].includes(MODE)) {
  console.error("first-run walk: FIRST_RUN_WALK_MODE must be ratchet, target or write-baseline, got " + MODE);
  process.exit(2);
}

// ── outside the browser ─────────────────────────────────────────────────────

// srcSQL runs SQL on the source MySQL as root, the way a person would in
// their own SQL client. MYSQL_PWD keeps the password off the command line.
function srcSQL(sql) {
  return execFileSync(DOCKER, ["exec", "-i", "-e", "MYSQL_PWD=" + SRC_ROOT_PW, SRC_CONTAINER,
    "mysql", "-uroot", "-N", "--protocol=TCP", "-h127.0.0.1"], { input: sql, encoding: "utf8" });
}

// The password the walk puts where the quickstart says <choose a password>.
// It meets MySQL's MEDIUM validate_password policy — eight or more, upper,
// lower, digit, special — so a source with the plugin on measures the page
// and not a password rule. It carries no quote or backslash, which
// parseQuickstartBlock refuses, because it is pasted into SQL as a literal.
const SOURCE_PASSWORD = "Frw-walk-9pw";

// The permissions block, read from the command-line quickstart exactly as it
// is published there (the web form shows the same statements), so the walk
// runs what a reader would run. The one edit is the one the page itself asks
// for: a password of our own in quotes where it says <choose a password>.
// parseQuickstartBlock refuses a block that publishes a runnable password.
function quickstartBlock() {
  const md = readFileSync(path.join(REPO_ROOT, "docs", "quickstart.md"), "utf8");
  return parseQuickstartBlock(md, SOURCE_PASSWORD);
}
const blockFor = (b, user) => b.sql.split("'" + b.user + "'@").join("'" + user + "'@");

// ── the event log ───────────────────────────────────────────────────────────

const logs = {};
let log = null;
const rec = (e) => { log.push(e); };
const now = () => Date.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let browser, context, page, chromiumVersion = "";
const jsErrors = [];

async function openBrowser() {
  browser = process.env.PW_WS_ENDPOINT
    ? await chromium.connect(process.env.PW_WS_ENDPOINT)
    : await chromium.launch({ headless: true, channel: process.env.PW_CHANNEL || undefined });
  chromiumVersion = browser.version();
  context = await browser.newContext({ viewport: VIEWPORT });
  await context.grantPermissions(["clipboard-read", "clipboard-write"], { origin: new URL(CONSOLE_URL).origin }).catch(() => {});
  await context.addInitScript({ path: path.join(HERE, "first_run_measure.js") });
  page = await context.newPage();
  page.on("pageerror", (e) => jsErrors.push(String(e)));
}

async function click(locator, label, optional = false) {
  await locator.click();
  rec({ kind: "click", label, optional });
}
async function reload(label, optional) {
  await page.reload();
  rec({ kind: "click", label: "reload the page (" + label + ")", optional });
}
// A field is typed only when it does not already hold what the walk needs,
// so a default the interface fills in is never counted against it.
async function type(locator, value, field, step) {
  if ((await locator.inputValue()) === value) return;
  await locator.fill(value);
  rec({ kind: "type", field, step });
}
const outOfBrowser = (label, fn) => { fn(); rec({ kind: "out_of_browser", label }); };

// navTo clicks the sidebar entry for a destination and records the click
// under the words on that entry. `already` says the destination is on screen
// already, in which case a person clicks nothing and nothing is counted.
async function navTo(routes, optional, already) {
  if (already && await already()) { await settle(); return false; }
  for (const route of routes) {
    const item = page.locator('.nav-item[data-route="' + route + '"]');
    if ((await item.count()) && (await item.first().isVisible())) {
      const label = (await item.first().innerText()).replace(/\s+/g, " ").trim();
      await click(item.first(), label + " (sidebar)", optional);
      await settle();
      return true;
    }
  }
  throw new Error("no sidebar entry for any of these routes: " + routes.join(", "));
}

// settle waits for the page to stop moving: no loading placeholders, no
// finite animation running, and the same text on two reads in a row.
async function settle(timeout = 20000) {
  const deadline = now() + timeout;
  let last = null;
  while (now() < deadline) {
    const state = await page.evaluate(() => {
      const busy = Array.from(document.querySelectorAll(".skel-line, .skel-note, .skel-stat, .ev-skel-row, .ev-skel-note, .view-loading"))
        .some((e) => e.getClientRects().length > 0);
      const anim = document.getAnimations().some((a) => a.playState === "running" &&
        a.effect && a.effect.getTiming && a.effect.getTiming().iterations !== Infinity);
      return { busy, anim, text: document.body.innerText };
    });
    if (!state.busy && !state.anim && state.text === last) return true;
    last = state.text;
    await sleep(250);
  }
  return false;
}

let shotN = 0;
async function measure(name, stepKind, primary) {
  await page.evaluate(() => document.fonts.ready);
  const settled = await settle();
  const handle = primary ? await primary.locator.elementHandle({ timeout: 5000 }).catch(() => null) : null;
  const m = await page.evaluate(([h, label, wanted]) => window.__firstRunMeasure(wanted ? { primary: h || "#__first_run_missing__", primaryLabel: label } : {}),
    [handle, primary ? primary.label : "", !!primary]);
  m.settled = settled;
  rec({ kind: "step", name, stepKind, measure: m });
  // A page still moving was measured mid-paint, and half a page is a SMALLER
  // number on every column where smaller is better — it would read as an
  // improvement. The run fails instead, which makes every one of its columns
  // say it did not finish.
  if (!settled) throw new Error('the page never stopped changing on step "' + name + '", so what was measured is not what a person would see');
  if (SHOTS) {
    mkdirSync(SHOTS, { recursive: true });
    await page.screenshot({ path: path.join(SHOTS, String(++shotN).padStart(2, "0") + "-" + log.run + "-" + name + ".png") });
  }
  return m;
}

// harnessGet reads the daemon's API with this tab's own session, to WAIT for
// the product; it never changes anything.
function harnessGet(apiPath, serverId) {
  return page.evaluate(async ([p, id]) => {
    const headers = {};
    if (typeof TOKEN !== "undefined" && TOKEN) headers.Authorization = "Bearer " + TOKEN;
    if (id) headers["X-Bintrail-Server"] = id;
    const res = await fetch(p, { headers });
    return res.ok ? res.json() : { __status: res.status };
  }, [apiPath, serverId || ""]);
}
async function serverId(name) {
  const r = await harnessGet("/api/servers");
  const s = (r.servers || []).find((x) => x.name === name);
  if (!s) throw new Error("server " + name + " is not in /api/servers");
  return s.id;
}
async function waitFor(fn, what, timeout) {
  const deadline = now() + timeout;
  for (;;) {
    // A probe that throws is almost always a page repainting under it (an
    // element detached, an evaluate whose context went away). That is a tick
    // to retry, not the end of the run; only running out of time ends it.
    let v = null;
    try { v = await fn(); } catch (err) { if (err && err.__fatal) throw err; }
    if (v) return v;
    if (now() > deadline) throw new Error("timed out after " + Math.round(timeout / 1000) + " s waiting for " + what);
    await sleep(250);
  }
}

// ── checks that read what is on screen ─────────────────────────────────────

// copyChecks: every block the step offers to copy. Two ways a copy can
// differ from what is shown: its Copy button puts other text on the
// clipboard, or it creates a login whose password is not the one in the
// step's Password field (the person copies one password and the form holds
// another, or none).
async function copyChecks(step) {
  const blocks = await page.evaluate(() => {
    const layer = ["#login-mount", "#notice-mount", "#modal"].map((s) => document.querySelector(s)).find((e) => e && e.firstElementChild) || document.getElementById("view");
    const shown = (e) => e.getClientRects().length > 0 && getComputedStyle(e).visibility !== "hidden";
    const pw = Array.from(layer.querySelectorAll("label")).find((l) => /password/i.test(l.textContent) && l.querySelector("input") && shown(l.querySelector("input")));
    return Array.from(layer.querySelectorAll("pre")).filter(shown).map((p, i) => (p.setAttribute("data-frw-pre", String(i)), {
      i, text: p.innerText,
      password: (p.innerText.match(/IDENTIFIED BY '([^']*)'/) || [])[1] ?? null,
      field: pw ? pw.querySelector("input").value : null,
      copyButton: !!(p.parentElement && Array.from(p.parentElement.querySelectorAll("button")).some((b) => /^Copy\b/.test(b.textContent.trim()))),
    }));
  });
  for (const b of blocks) {
    if (b.password !== null) {
      const mismatch = b.field === null || b.field !== b.password;
      rec({ kind: "copy_check", label: step + ": permissions block", mismatch,
        detail: mismatch ? `the block sets the password '${b.password}', the Password field holds ${b.field === null ? "no field" : b.field === "" ? "nothing" : "another value"}` : "" });
    } else if (b.text.includes("CREATE USER")) {
      // The block still creates a login but no longer says with which
      // password, so the comparison this column exists for cannot be made.
      // Silence here would read as "they match" — the defect fixed.
      rec({ kind: "copy_check", label: step + ": permissions block", mismatch: null,
        detail: "the block creates a login but no longer names a password on screen, so it cannot be compared with the form" });
    }
    if (b.copyButton) {
      const pre = page.locator(`pre[data-frw-pre="${b.i}"]`);
      await pre.locator("xpath=..").getByRole("button", { name: /^Copy/ }).first().click();
      await sleep(300);
      let clip = null, unreadable = "";
      try { clip = await page.evaluate(() => navigator.clipboard.readText()); }
      catch (e) { unreadable = (e && e.message) || String(e); }
      const norm = (t) => String(t).replace(/[ \t]+$/gm, "").trim();
      if (unreadable) {
        // Not a mismatch: the comparison did not happen. Called a mismatch it
        // would send a reviewer hunting a Copy button that works.
        rec({ kind: "copy_check", label: step + ": Copy button", mismatch: null, detail: "the clipboard could not be read: " + unreadable });
      } else {
        const mismatch = norm(clip) !== norm(b.text);
        rec({ kind: "copy_check", label: step + ": Copy button", mismatch, detail: mismatch ? "the clipboard differs from the block on screen" : "" });
      }
      // Let the "copied" toast go before the next screen is read.
      await page.locator("#toast").waitFor({ state: "hidden", timeout: 10000 }).catch(() => {});
    }
  }
}

// ── the runs ───────────────────────────────────────────────────────────────

function startRun(name) {
  log = logs[name] = [];
  log.run = name;
}

// Where the snapshot location is typed, wherever that now lives: the field is
// found by its NAME and its Save by the word on it inside the same card. Only
// one server exists when the clean run reaches this, so there is one field.
function snapshotLocation() {
  // Scoped to the page. The add-server form carries a HIDDEN input of the
  // same name (it has to post the value back on save), so an unscoped lookup
  // picks the visible one only by the order the two mounts happen to sit in
  // the document — and a build that moves this editor into a dialog would
  // silently start reading the hidden one, which answers "" to everything.
  // The candidates are scoped to the page; the `has:` locators are NOT, on
  // purpose — Playwright matches those relative to each candidate, so one
  // built from #view would be looked for inside the card and never match.
  const view = page.locator("#view");
  const card = view.locator("form, section, div")
    .filter({ has: page.locator("input[name=baseline_dir]") })
    .filter({ has: page.getByRole("button", { name: /^Save$/ }) })
    .last();
  return {
    dir: view.locator("input[name=baseline_dir]").first(),
    s3: view.locator("input[name=baseline_s3]").first(),
    save: card.getByRole("button", { name: /^Save$/ }).first(),
  };
}

const noticeTitle = () => page.locator("#notice-mount #notice-title").textContent({ timeout: 1000 }).catch(() => "");
const noticeOpen = () => page.locator("#notice-mount .notice").isVisible().catch(() => false);
const recentRow = (pk, type) => page.locator(".ov-evlist .ov-ev").filter({ hasText: "#" + pk }).filter({ has: page.locator(".badge", { hasText: type }) });

async function signIn() {
  await page.goto(CONSOLE_URL + "/");
  const form = page.locator("#login-form");
  await form.waitFor({ timeout: 30000 });
  const submit = form.locator("button[type=submit]");
  await measure("sign-in", "step", { locator: submit, label: "Create & sign in" });
  await type(form.locator("input[name=username]"), "admin", "username", "sign-in");
  await type(form.locator("input[name=password]"), LOGIN_PASSWORD, "password", "sign-in");
  await type(form.locator("input[name=confirm]"), LOGIN_PASSWORD, "confirm password", "sign-in");
  await click(submit, "Create & sign in");
  await form.waitFor({ state: "detached", timeout: 30000 });
}

// addServer drives the add-server form from the moment it is open until the
// save's outcome. Returns the notice title (or "" when no notice opened).
async function fillServer(step, name, user, password) {
  const f = page.locator("#server-form");
  await type(f.locator("input[name=name]"), name, "name", step);
  await type(f.locator("input[name=source_host]"), SRC_HOST, "host", step);
  await type(f.locator("input[name=source_port]"), SRC_PORT, "port", step);
  await type(f.locator("input[name=source_user]"), user, "user", step);
  await type(f.locator("input[name=source_password]"), password, "password", step);
}
async function saveServer() {
  await click(page.locator("#server-form-mount button[type=submit]"), "Save");
  // Save runs the startup checks and starts capture; a person waits for the
  // answer. A clean start with no warning closes the form with a toast and
  // opens no notice.
  await waitFor(async () => (await noticeOpen()) || !(await page.locator("#server-form").isVisible()), "the result of Save", 120000);
  return (await noticeOpen()) ? noticeTitle() : "";
}
async function closeServersDialog() {
  await click(page.locator("#modal .modal-x"), "close the Servers dialog (✕)");
  await page.locator("#modal .modal-scrim").waitFor({ state: "detached", timeout: 10000 });
}
async function selectServer(name) {
  const sel = page.locator("#server-select");
  const current = await sel.evaluate((s) => (s.selectedOptions[0] ? s.selectedOptions[0].textContent : ""));
  if (current.trim() === name) return;
  const value = await sel.evaluate((s, n) => { const o = Array.from(s.options).find((x) => x.textContent.trim() === n); return o ? o.value : null; }, name);
  if (value === null) throw new Error("server " + name + " is not in the server selector");
  await sel.selectOption(value);
  rec({ kind: "click", label: "choose " + name + " in the server selector", optional: false });
}
// A harness wait, not a person's action: capture has written its first
// checkpoint, so the Overview drawn next shows a settled state rather than
// whichever instant of start-up the page happened to catch.
async function waitCaptureSettled(id) {
  await waitFor(async () => {
    const c = await harnessGet("/api/coverage", id);
    return c && (c.freshness === "idle" || c.freshness === "current");
  }, "capture's first checkpoint", 90000);
}
async function waitFirstRunList() {
  // The Getting started list redraws itself while capture starts. ONE stated
  // condition, not "this or a change already arrived": those are two
  // different screens with different word counts, and whichever won the race
  // would decide the number.
  await waitFor(async () => (await page.locator(".fr-card .fr-step.done").count()) >= 4,
    "the four start-up steps on the Getting started list to be done", 90000);
}

async function runClean(block) {
  startRun("clean");
  // Before the browser, as the quickstart says: create the capture user.
  outOfBrowser("create the capture user: run the permissions block on MySQL", () => srcSQL(block.sql));

  await signIn();
  const add = page.locator("#ov-add-server");
  await add.waitFor({ timeout: 30000 });
  await measure("after-sign-in", "step", { locator: add, label: "+ Add server" });
  await click(add, "+ Add server");
  await page.locator("#server-form").waitFor({ timeout: 10000 });
  const save = page.locator("#server-form-mount button[type=submit]");
  await measure("connect", "step", { locator: save, label: "Save" });
  await copyChecks("connect");
  await fillServer("connect", "shop-db", block.user, block.password);

  // The form offers Test connection next to Save; the inventory pressed it,
  // as a careful person does. Not needed for the snapshot, so optional.
  await click(page.locator("#server-test"), "Test connection", true);
  await waitFor(noticeOpen, "the Test connection notice", 60000);
  await measure("test-result", "step", { locator: page.locator("#notice-close"), label: await page.locator("#notice-close").textContent() });
  await click(page.locator("#notice-close"), "Back to the form", true);

  const title = await saveServer();
  if (/did not|could not/i.test(title)) throw new Error("capture did not start on a clean MySQL: notice \"" + title + "\"");
  if (title) {
    await measure("save-result", "step", { locator: page.locator("#notice-close"), label: await page.locator("#notice-close").textContent() });
    await click(page.locator("#notice-close"), "OK");
  }
  const id = await serverId("shop-db");
  await waitCaptureSettled(id);
  await measure("after-ok", "step");
  await closeServersDialog();
  await selectServer("shop-db");
  await waitFirstRunList();
  await measure("first-change-waiting", "step");

  // Three changes, one at a time, each watched for on the Overview without
  // a reload.
  // The first change alone, then the next two a moment apart and watched
  // together: each is timed from its own commit, and the walk waits once.
  const watchChanges = async (batch) => {
    for (const c of batch) {
      if (c.gap) await sleep(c.gap);
      srcSQL(c.sql);
      c.t0 = now();
    }
    const deadline = batch[batch.length - 1].t0 + CHANGE_WAIT_MS;
    while (now() < deadline && batch.some((c) => c.seconds === undefined)) {
      for (const c of batch) {
        if (c.seconds === undefined && (await recentRow(c.pk, c.op).count())) c.seconds = Math.round((now() - c.t0) / 100) / 10;
      }
      await sleep(200);
    }
    for (const c of batch) rec({ kind: "change", op: c.op, visible: c.seconds !== undefined, seconds: c.seconds === undefined ? null : c.seconds });
  };
  await watchChanges([{ op: "INSERT", sql: "INSERT INTO shop.orders VALUES (6, 2, 'new', 31.00);", pk: "6" }]);
  await watchChanges([
    { op: "UPDATE", sql: "UPDATE shop.orders SET status = 'shipped' WHERE id = 2;", pk: "2" },
    { op: "DELETE", sql: "DELETE FROM shop.orders WHERE id = 3;", pk: "3", gap: 2000 },
  ]);
  if (!logs.clean.filter((e) => e.kind === "change").every((e) => e.visible)) {
    await reload("to see the later changes", true);
    await waitFor(async () => (await recentRow("3", "DELETE").count()) > 0, "the DELETE after a reload", 60000);
  }
  await measure("changes", "step");

  // Undo on the DELETE. The button only appears under the mouse.
  const delRow = recentRow("3", "DELETE").first();
  await delRow.hover();
  await click(delRow.locator(".ov-ev-undo"), "Undo", true);
  await waitFor(async () => /INSERT INTO/.test(await page.locator("#view").innerText()), "the undo SQL", 30000);
  await measure("undo", "step");
  await copyChecks("undo");

  // Where a person goes looking for the first snapshot, then where its
  // location is set. Two pages today, one page after the Snapshots merge.
  await navTo(ROUTES.snapshotList, true);
  await measure("snapshot-list-first-look", "step");
  await navTo(ROUTES.snapshotSetup, false, () => snapshotLocation().dir.isVisible().catch(() => false));
  const loc = snapshotLocation();
  await loc.dir.waitFor({ timeout: 30000 });
  await measure("snapshot-location", "step", { locator: loc.save, label: "Save (the snapshot location)" });
  if (!(await loc.dir.inputValue()) && !(await loc.s3.inputValue())) {
    rec({ kind: "forced_choice", label: "a folder or a bucket for the snapshots: two empty places and no default" });
  }
  await type(loc.dir, SNAP_DIR, "snapshot folder", "snapshot-location");
  await click(loc.save, "Save (the snapshot location)");
  await waitFor(async () => (await snapshotLocation().dir.inputValue()) === SNAP_DIR &&
    (await snapshotLocation().save.isDisabled()), "the saved snapshot location", 30000);
  await measure("snapshot-location-saved", "step");

  await navTo(ROUTES.snapshotList, false, () => takeSnapshotButton().first().isVisible().catch(() => false));
  const create = takeSnapshotButton().first();
  if (await create.isVisible().catch(() => false)) {
    // The folder the location named did not have to exist for this to work,
    // so there is no error screen in this run and no trip out of the browser.
    // Recorded, so a lost error-screen measurement is told apart from one
    // that no longer happens.
    rec({ kind: "condition_absent", what: "error", why: "the snapshot could be taken without creating the folder first" });
  } else {
    await measure("snapshot-folder-missing", "error");
    const text = await page.locator("#view").innerText();
    if (!/no such file or directory/i.test(text)) throw new Error("no way to take the first snapshot on this page: " + text.slice(0, 300));
    // The page names a folder that does not exist and offers nothing to do
    // in the browser: the person leaves to create it, then comes back.
    outOfBrowser("create the snapshot folder (mkdir)", () => mkdirSync(SNAP_DIR, { recursive: true }));
    await reload("after creating the folder", false);
    await create.waitFor({ timeout: 30000 });
  }
  const createLabel = (await create.innerText()).trim();
  await measure("snapshot-ready", "step", { locator: create, label: createLabel });
  await click(create, createLabel);
  // The finish line has THREE halves, and the third is the one that matters.
  // The daemon listing the snapshot is not enough, and neither is the button
  // coming back to its own name: the page re-enables that button before its
  // last repaint, so both can be true while the screen still carries the
  // "creating" card from the start of the run — a screen nobody ends on, with
  // one more RUNNING chip and a different set of sentences than the finished
  // page. So the walk also waits until the page SHOWS the snapshot that now
  // exists, by the daemon's own timestamp for it. That is what a person reads
  // to know it worked, it needs no layout to find, and it is the page's
  // drawing checked against the data behind it.
  //
  // A build that stops showing that timestamp, or takes the button away for
  // good, makes this time out and say which half failed: whoever reshapes
  // that screen updates this step in the same change.
  await waitFor(async () => {
    if (await page.locator("#toast-error").isVisible()) {
      const err = new Error("the snapshot failed: " + (await page.locator("#toast-error").innerText()));
      err.__fatal = true; // not a repaint to retry: the product said it failed
      throw err;
    }
    const b = await harnessGet("/api/baselines", id);
    const newest = (b.snapshots || [])[0];
    if (!newest) return false;
    const btn = takeSnapshotButton().first();
    if (!((await btn.count()) >= 1 && (await btn.isEnabled().catch(() => false)))) return false;
    // The page's own "a job is running" chip, the same class the yellow-red
    // list counts. The page paints the run region from a status it fetched
    // while the run was still going, and replaces it on its next poll, so
    // this is what separates the finished screen from the one still carrying
    // the start of the run.
    if (await page.locator("#view .chip-live").first().isVisible().catch(() => false)) return false;
    return page.evaluate((t) => (document.getElementById("view") || document.body).innerText.includes(t), newest.time);
  }, "the first snapshot to finish (the daemon to list it, the button that started it to come back to \"" + createLabel + "\" enabled, no job still marked running, and the page to show the snapshot's own time)", 300000);
  rec({ kind: "milestone", name: "first_snapshot_done" });
  const final = await measure("first-snapshot", "step");

  // The final screen should state how far back changes are kept, and that
  // must be the server's real retention. Measured only once such a sentence
  // exists; until then the column says so.
  const finalText = await page.locator("#view").innerText();
  const kept = finalText.match(/Changes are kept (?:for )?(\d+(?:\.\d+)?)\s*(minute|hour|day|week)s?/i);
  if (!kept) {
    rec({ kind: "retention_check", matches: null, reason: "the final screen states no retention (no \"Changes are kept N days\" sentence)" });
  } else {
    const rot = await harnessGet("/api/rotation", id);
    const hours = (s) => { const m = String(s || "").match(/^(\d+)([hdm])$/); return m ? Number(m[1]) * { h: 1, d: 24, m: 1 / 60 }[m[2]] : null; };
    const real = hours(rot.index_retain || rot.retain);
    const shown = Number(kept[1]) * { minute: 1 / 60, hour: 1, day: 24, week: 168 }[kept[2].toLowerCase()];
    rec({ kind: "retention_check", matches: real === null ? null : Math.abs(real - shown) < 1e-9,
      reason: real === null ? "the daemon's retention is unreadable (" + JSON.stringify(rot) + ")" : `screen says ${kept[0]}, the server keeps ${rot.index_retain || rot.retain}` });
  }
  return { id, finalChunks: final.chunks, finalText };
}

// The "kept up to date" sentence of the snapshot step may only be shown when
// DBTrail cannot read whole databases on its own. In this walk it can (the
// install turns full reads on), so the sentence must not appear.
function refusedUpdate(final) {
  startRun("refused-update");
  const re = /up to date from the changes|never reads all your tables again/gi;
  const hits = (final.finalText.match(re) || []).length;
  // What this reads is the final screen of the clean walk. That install
  // allows full reads, so a sentence promising the snapshot keeps itself up
  // to date would be false there — but the walk never CLOSED that door, so a
  // zero is "the sentence was not on that screen", not "it is suppressed when
  // it should be". It is reported as such until a run can set the door.
  rec({ kind: "renewal_check", count: hits, exercised: false,
    detail: hits ? "the sentence is on the final screen, where reading whole databases is allowed"
      : "not on the final screen, but nothing here closed the door to reading whole databases, so this is not yet a test of the rule" });
  rec({ kind: "refusal_check", named: null, reason: "nothing on screen reports a snapshot update yet: an update that cannot be built from the changes today falls back to a full read, which this install allows, so there is no refusal to show" });
}

// Each run after the first starts on the Overview of the server the person
// already has, as the inventory's second run did. Moving there is the
// harness setting the scene between runs, not a step of the next run.
async function toOverview() {
  await page.locator('.nav-item[data-route="' + ROUTES.overview[0] + '"]').first().click();
  await settle();
}

async function reloadMidConnect() {
  await toOverview();
  startRun("reload-mid-connect");
  await click(page.locator("#manage-servers"), "Manage servers");
  await click(page.locator("#server-add"), "+ Add server");
  await page.locator("#server-form").waitFor({ timeout: 10000 });
  const draft = { host: "draft-host.invalid", port: "3399", user: "draft_user", password: "draft-pass-1" };
  const f = page.locator("#server-form");
  await type(f.locator("input[name=source_host]"), draft.host, "host", "connect");
  await type(f.locator("input[name=source_port]"), draft.port, "port", "connect");
  await type(f.locator("input[name=source_user]"), draft.user, "user", "connect");
  await type(f.locator("input[name=source_password]"), draft.password, "password", "connect");
  // Where the person leaves to run the permissions block, and comes back.
  await reload("mid-Connect", false);
  await settle();
  const values = await page.evaluate(() => Array.from(document.querySelectorAll("input")).filter((i) => i.getClientRects().length).map((i) => i.value));
  const kept = Object.entries(draft).filter(([, v]) => values.includes(v)).map(([k]) => k);
  rec({ kind: "draft_check", kept: kept.length, detail: kept.length ? "kept: " + kept.join(", ") : "every typed field was lost" });
  if (await page.locator("#modal .modal-scrim").isVisible().catch(() => false)) await closeServersDialog();
}

// A server added once the first one exists: the same steps without Sign in.
async function addAnotherServer(runName, name, user, block, onRefused) {
  await toOverview();
  startRun(runName);
  outOfBrowser("create the capture user " + user + ": run the permissions block on MySQL", () => srcSQL(blockFor(block, user)));
  await click(page.locator("#manage-servers"), "Manage servers");
  await click(page.locator("#server-add"), "+ Add server");
  await page.locator("#server-form").waitFor({ timeout: 10000 });
  await measure("connect", "step", { locator: page.locator("#server-form-mount button[type=submit]"), label: "Save" });
  await fillServer("connect", name, user, block.password);
  let title = await saveServer();
  if (/did not/i.test(title)) {
    await measure("save-refused", "error", { locator: page.locator("#notice-close"), label: await page.locator("#notice-close").textContent() });
    if (!onRefused) throw new Error(runName + ": capture did not start: " + title);
    await onRefused();
    await click(page.locator("#notice-close"), "Back to the form");
    title = await saveServer();
    if (/did not|could not/i.test(title)) throw new Error(runName + ": capture still did not start: " + title);
  } else if (/could not/i.test(title)) {
    throw new Error(runName + ": " + title);
  }
  if (title) {
    await measure("save-result", "step", { locator: page.locator("#notice-close"), label: await page.locator("#notice-close").textContent() });
    await click(page.locator("#notice-close"), "OK");
  }
  const id = await serverId(name);
  await waitCaptureSettled(id);
  await measure("after-ok", "step");
  await closeServersDialog();
  await selectServer(name);
  await waitFirstRunList();
  rec({ kind: "milestone", name: "capture_running" });
  await measure("overview", "step");
}

async function noPrimaryKey(block) {
  // The person's own table, already on their MySQL before DBTrail arrives.
  srcSQL("CREATE TABLE shop.audit_log (happened_at DATETIME NOT NULL, actor VARCHAR(64), action VARCHAR(64));\n" +
    "INSERT INTO shop.audit_log VALUES (NOW(), 'ana', 'login'), (NOW(), 'bo', 'export'), (NOW(), 'ana', 'logout');\n");
  await addAnotherServer("no-primary-key", "shop-audit", "dbtrail3", block, async () => {
    // What the refusal asks for: SQL on the database. When it offers more
    // than one statement for the same table, the person has to pick one.
    const alters = await page.evaluate(() => Array.from(document.querySelectorAll("#notice-mount pre, #notice-mount code"))
      .filter((p) => p.getClientRects().length && /ALTER TABLE/i.test(p.innerText)).length);
    // Always say what was seen, zero included: capture WAS refused for a
    // table with no primary key, so a screen with no statement on it means
    // this reading lost its footing, not that the choice went away.
    rec({ kind: "forced_choice_probe", what: "which ALTER TABLE form to run", count: alters });
    if (alters >= 2) rec({ kind: "forced_choice", label: "which of " + alters + " ALTER TABLE forms to run" });
    outOfBrowser("add a primary key to shop.audit_log on MySQL", () =>
      srcSQL("ALTER TABLE shop.audit_log ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;\n"));
  });
}

// ── main ───────────────────────────────────────────────────────────────────

const runErrors = [];
async function attempt(name, fn) {
  try { await fn(); }
  catch (err) {
    runErrors.push(name + ": " + (err && err.message ? err.message : err));
    if (logs[name]) logs[name].push({ kind: "error", message: String(err && err.message ? err.message : err) });
    await page.screenshot({ path: path.join(ART, "first-run-walk-failure-" + name + ".png") }).catch(() => {});
  }
}

const block = quickstartBlock();
await openBrowser();
let final = null;
try {
  await attempt("clean", async () => { final = await runClean(block); });
  if (final) refusedUpdate(final);
  await attempt("reload-mid-connect", reloadMidConnect);
  await attempt("second-server", () => addAnotherServer("second-server", "shop-db-2", "dbtrail2", block, null));
  await attempt("no-primary-key", () => noPrimaryKey(block));
} finally {
  await browser.close().catch(() => {});
}

let commit = "";
try { commit = execFileSync("git", ["-C", REPO_ROOT, "rev-parse", "--short", "HEAD"], { encoding: "utf8" }).trim(); } catch (_) { /* not a checkout */ }
// The platform the layout-dependent numbers were measured on. Fonts decide
// where text wraps, wrapping decides what is above the fold, so a word count
// or a below-the-fold distance from one machine is not evidence about
// another. Recorded here and compared by the ratchet.
const platform = process.platform + "/" + (chromiumVersion || "chromium");
const scoreboard = { schema: 1, commit, platform, viewport: VIEWPORT.width + "x" + VIEWPORT.height, runs: {} };
for (const name of Object.keys(RUNS)) {
  if (logs[name]) scoreboard.runs[name] = deriveRun(name, logs[name]);
}
scoreboard.errors = runErrors;
scoreboard.js_errors = jsErrors;
writeFileSync(path.join(ART, "first-run-scoreboard.json"), JSON.stringify({ scoreboard, logs }, null, 2) + "\n");

let baseline = null;
try { baseline = loadBaseline(BASELINE_FILE); } catch (err) { if (MODE !== "write-baseline") { console.error("first-run walk: " + err.message); process.exitCode = 1; } }
console.log(renderScoreboard(scoreboard, baseline));
console.log("\nScoreboard with evidence: " + path.join(ART, "first-run-scoreboard.json"));

let failed = false;
if (runErrors.length) {
  failed = true;
  console.log("\nTHE WALK DID NOT FINISH:\n  " + runErrors.join("\n  "));
}
if (jsErrors.length) {
  failed = true;
  console.log("\nUncaught errors in the page:\n  " + jsErrors.join("\n  "));
}

if (MODE === "write-baseline") {
  if (failed) {
    console.log("\nNot writing the baseline: the walk did not finish cleanly.");
  } else {
    writeFileSync(BASELINE_FILE, JSON.stringify(baselineFrom(scoreboard, { measured_at_commit: commit, platform, viewport: scoreboard.viewport }), null, 2) + "\n");
    console.log("\nWrote " + BASELINE_FILE);
  }
} else if (MODE === "target") {
  const t = compareTarget(scoreboard);
  console.log(`\nTARGETS (#1800): ${t.passed.length} met, ${t.failures.length} not met`);
  for (const f of t.failures) console.log("  NOT MET  " + f);
  for (const p of t.passed) console.log("  met      " + p);
  if (t.failures.length) failed = true;
} else if (baseline) {
  const r = compareRatchet(scoreboard, baseline);
  const t = compareTarget(scoreboard);
  console.log(`\nRATCHET against ${path.basename(BASELINE_FILE)} (recorded on ${baseline.platform || "an unrecorded platform"}, running on ${platform}): ` +
    `${r.failures.length} worse, ${r.loosened.length} better, ${r.passed.length} unchanged, ${r.notMeasurable.length} not measurable, ${r.notCompared.length} not compared`);
  for (const f of r.failures) console.log("  WORSE    " + f);
  for (const l of r.loosened) console.log("  BETTER   " + l);
  for (const n of r.notMeasurable) console.log("  N/M      " + n);
  for (const n of r.notCompared) console.log("  SHOWN    " + n);
  if (r.notCompared.length) {
    console.log("  (to have those compared too, record a baseline on this platform:\n" +
      "   FIRST_RUN_WALK_MODE=write-baseline make console-first-run-walk)");
  }
  console.log(`(targets: ${t.passed.length} of ${t.passed.length + t.failures.length} met; FIRST_RUN_WALK_MODE=target lists them)`);
  if (r.failures.length) failed = true;
}
if (failed) process.exitCode = 1;
