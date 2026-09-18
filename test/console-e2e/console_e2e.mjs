// Headless-Chrome E2E regression guard for the bintrail-console frontend.
//
// Why this exists: `go test` never renders the embedded SPA (assets/*.js,
// *.css), so a whole class of bugs is invisible to the Go suite — CSS cascade
// (an invisible button), DOM/state logic (a form that auto-expands), and the
// way the UI presents a backend state (a raw 1049 error wall, the control
// plane vanishing when the selected server's index is missing). Each scenario
// below pins a bug that shipped in 0.13.3 and reached a user.
//
// It drives an ALREADY-RUNNING console (run.sh starts the daemon + seeds a
// monitored source whose per-source index is NOT provisioned — the exact
// lifecycle state that broke everything). Env: CONSOLE_URL, CONSOLE_TOKEN,
// optional PW_CHANNEL (e.g. "chrome" to use system Chrome instead of the
// playwright-managed chromium).
import { chromium } from "playwright";
import zlib from "node:zlib";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";

const URL = process.env.CONSOLE_URL || "http://127.0.0.1:8090";
const TOKEN = process.env.CONSOLE_TOKEN || "";
const CHANNEL = process.env.PW_CHANNEL || undefined; // undefined → bundled chromium
const ART = process.env.E2E_ARTIFACT_DIR || "/tmp";
// Read-fixture coordinates (seeded by run.sh into the boot index; see the
// fixture block there). FIX is the seeded schema; TT_AT is a timestamp just
// after the last seeded event, inside the hour whose partition exists — using
// the default `at` (now) would race the top-of-hour partition boundary.
const FIX = process.env.E2E_FIX_SCHEMA || "e2eshop";
const TT_AT = process.env.E2E_TT_AT || "";
// The #1365 archive fixture (run.sh): an index whose history is mostly
// rotated into Parquet, with a manufactured coverage-gap hour. Deliberately
// no fallback default — an unset env must fail the scenario loudly, never
// skip it.
const ARC_DB = process.env.E2E_ARC_DB || "";
const ARC_GAP_SINCE = process.env.E2E_ARC_GAP_SINCE || "";
const ARC_GAP_UNTIL = process.env.E2E_ARC_GAP_UNTIL || "";
// The query_text canary run.sh embeds in the seeded UPDATE event. query_text /
// query_hash are captured index data but NEVER cross the DTO layer (#699);
// this string appearing anywhere in the page or an export is a leak.
const CANARY = "e2e-canary-query-text";

const results = [];
const ok = (name) => results.push({ name, pass: true });
const bad = (name, detail) => results.push({ name, pass: false, detail });

// Playwright's signature is waitForFunction(pageFunction, arg, options) —
// options are the THIRD parameter, unlike Puppeteer's second. Passing
// { timeout: N } in the arg slot serializes it as the page function's
// argument and discards it, so the wait silently runs on the 30-second
// default: measured at 30011 ms for the two-arg form against 2003 ms for the
// three-arg one (#1589).
//
// Every budget in this file was written that way, so none of the stated
// numbers had ever run. Adopting them at the position fix would have enabled
// ~25 untested budgets at once on a suite whose flakes are load-sensitive, so
// the ones below 30s were DROPPED instead of moved: the default is what the
// suite is actually green with, and a number that has never been exercised is
// worse than no number. Only the 60s verification wait was moved to the third
// slot, because that one was being silently SHORTENED to 30s — the single
// case where the bug could cause a flake rather than mask one.
//
// This guard keeps the shape from coming back. It reads its own source, so a
// call written in the two-arg form fails the run with the line number instead
// of quietly waiting 30s. It counts ARGUMENTS, not commas: the first census
// counted commas and walked past a two-arg call with a trailing comma, so the
// bug it was written against survived it once. It is a bracket walk, not a
// parser: a comment, string or regex literal inside a call's argument span
// takes part in the depth and comma counts, so an unbalanced bracket or a
// top-level comma inside one skews the argument count in either direction.
// waitForSelector is deliberately not scanned: its options ARE the second
// parameter.
{
  // fileURLToPath, not `new URL`: this file shadows the global URL with the
  // console's address (line 20), so the constructor form throws here.
  const src = readFileSync(fileURLToPath(import.meta.url), "utf8");
  // Assembled rather than written whole: the scan reads this very file, and a
  // literal here would be a call site of its own to parse.
  const NEEDLE = "waitFor" + "Function(";
  const scan = (text) => {
  const misplaced = [];
  let i = 0;
  while ((i = text.indexOf(NEEDLE, i)) >= 0) {
    let depth = 0, j = i + NEEDLE.length - 1;
    const cuts = [j];
    for (; j < text.length; j++) {
      const c = text[j];
      if (c === "(" || c === "{" || c === "[") depth++;
      else if (c === ")" || c === "}" || c === "]") { depth--; if (depth === 0) { break; } }
      else if (c === "," && depth === 1) cuts.push(j);
    }
    cuts.push(j);
    j++;
    // Arguments, not commas: a trailing comma before `)` is a third cut with
    // nothing after it, and the shape it decorates is still two arguments.
    const args = [];
    for (let k = 0; k + 1 < cuts.length; k++) args.push(text.slice(cuts[k] + 1, cuts[k + 1]).trim());
    while (args.length && args[args.length - 1] === "") args.pop();
    // Two arguments and a timeout in the second one: the options object is
    // sitting in the arg slot.
    if (args.length === 2 && /\btimeout\b/.test(args[1])) {
      misplaced.push(text.slice(0, i).split("\n").length);
    }
    i = j;
  }
  return misplaced;
  };
  // Positive control: the scanner must see the bad shape in both spellings it
  // has been written in (single line, and multi-line with a trailing comma)
  // and must pass the three-argument form. Without this a scanner that stops
  // matching anything reads as "all clear".
  const fixture = [
    `await page.${NEEDLE}() => x, { timeout: 5000 });`,
    `await page.${NEEDLE}\n  () => x,\n  { timeout: 5000 },\n);`,
    `await page.${NEEDLE}(v) => x, v, { timeout: 5000 });`,
  ].join("\n");
  const control = scan(fixture);
  if (control.length !== 2 || control[0] !== 1 || control[1] !== 2) {
    bad("suite: the waitForFunction scanner detects the two-arg shape", `control flagged lines ${JSON.stringify(control)}, expected [1,2]`);
  } else {
    const misplaced = scan(src);
    misplaced.length === 0
      ? ok("suite: every waitForFunction timeout is in the options slot")
      : bad("suite: every waitForFunction timeout is in the options slot",
          `lines ${misplaced.join(", ")} pass options as the page-function argument, so the stated timeout never applies`);
  }
}

const browser = await chromium.launch({ headless: true, channel: CHANNEL });
const page = await browser.newPage({ viewport: { width: 1300, height: 1000 } });
const jsErrors = [];
page.on("pageerror", (e) => jsErrors.push(String(e)));

try {
  await page.goto(`${URL}/?token=${encodeURIComponent(TOKEN)}`, { waitUntil: "networkidle" });
  await page.waitForFunction(
    () => typeof openServersModal === "function" && typeof renderError === "function",
  );

  // Scenario 0 — the page loads and the monitor capability is reported. A
  // regression here (caps fetch failing for the default server — even a
  // broken, unprovisioned one — and degrading to {}) would itself hide the
  // control plane. The fetch is async, so wait for it rather than racing it.
  // capsCache is a module-scoped `let` (not window.capsCache): reachable by
  // bare name in evaluate's global scope, never as a window property.
  let monitor = false;
  try {
    await page.waitForFunction(() => typeof capsCache !== "undefined" && capsCache.monitor === true);
    monitor = true;
  } catch (_) {
    monitor = await page.evaluate(() => typeof capsCache !== "undefined" && !!capsCache.monitor);
  }
  monitor ? ok("boot: monitor capability reported") : bad("boot: monitor capability reported", "capsCache.monitor !== true after caps load");

  // Scenario 0b — the sidebar footer reports the running build (#1221). This
  // harness builds the daemon without -ldflags, so the server reports "dev";
  // the assertion is against the SERVER's own value, not a hardcoded string,
  // so a CONSOLE_BIN=<released build> run still passes. What it really guards
  // is the blank/garbage artifact: an empty row, or a label glued together as
  // "v" + a missing value.
  const sideVer = await page.evaluate(() => ({
    label: (document.querySelector("#meta-version b") || {}).textContent || "",
    reported: typeof capsCache !== "undefined" ? capsCache.version : undefined,
  }));
  const wantVer = /^v?\d+\.\d+\.\d+/.test(String(sideVer.reported || ""))
    ? "v" + String(sideVer.reported).replace(/^v/, "")
    : (String(sideVer.reported || "") || "dev");
  sideVer.label === wantVer
    ? ok("sidebar: running version shown")
    : bad("sidebar: running version shown", `label=${JSON.stringify(sideVer.label)} want=${JSON.stringify(wantVer)} (capabilities version=${JSON.stringify(sideVer.reported)})`);

  // Scenario 0c — the shapes the live daemon cannot produce, driven through the
  // REAL function (same technique as the ext-view scenarios below: stub
  // capsCache, call the production code, read the DOM). This harness builds
  // without -ldflags, so the server always reports the literal "dev" and 0b
  // above only ever exercises that branch; the released-build expectation has
  // to come from a FIXED input with a HARDCODED expectation, or dropping the
  // "v" prefix — or a regex that stops matching semver — ships green. (It does
  // NOT cover the leading-"v" strip: that is classification-only and produces
  // an identical string either way, which is why app.js says so in prose
  // instead of pretending a test pins it.) The other two shapes: `version` is
  // omitempty, so a Config with an empty Version sends no key at all and must
  // read "dev", never "" or "vundefined"; and a capabilities fetch that FAILED
  // is a different state — it must keep the "—" placeholder rather than report
  // "dev" for a build it never read.
  const degraded = await page.evaluate(() => {
    const keep = capsCache.version;
    const read = () => (document.querySelector("#meta-version b") || {}).textContent;
    capsCache.version = "1.2.3";
    updateSideVersion(true);
    const semver = read();
    delete capsCache.version;
    updateSideVersion(true);
    const unversioned = read();
    updateSideVersion(false);
    const unknown = read();
    capsCache.version = keep;
    updateSideVersion(true);
    return { semver, unversioned, unknown, restored: read() };
  });
  degraded.semver === "v1.2.3"
    ? ok("sidebar: a released version renders as vX.Y.Z")
    : bad("sidebar: a released version renders as vX.Y.Z", `got ${JSON.stringify(degraded.semver)} for capabilities version "1.2.3"`);
  degraded.unversioned === "dev"
    ? ok("sidebar: unversioned build falls back to 'dev'")
    : bad("sidebar: unversioned build falls back to 'dev'", `got ${JSON.stringify(degraded.unversioned)}`);
  degraded.unknown === "—"
    ? ok("sidebar: failed capabilities fetch never claims a version")
    : bad("sidebar: failed capabilities fetch never claims a version", `got ${JSON.stringify(degraded.unknown)}`);
  degraded.restored === wantVer
    ? ok("sidebar: version restored after the degraded probes")
    : bad("sidebar: version restored after the degraded probes", `got ${JSON.stringify(degraded.restored)}`);

  // Scenario 0d — the WIRING, not the function. 0c calls updateSideVersion(false)
  // directly, which proves the "—" branch exists but says nothing about the
  // caller ever reaching it: hardcoding updateSideVersion(true), or initialising
  // capsOK to true, passes 0b and 0c untouched and ships a console that reports
  // "dev" for a build it never read. So drive the real gateCapabilities() with a
  // FAILING /api/capabilities.
  // Two traps this is shaped around. The stub must answer 500, never 401 —
  // gateCapabilities RETHROWS a 401, and an unhandled rejection inside
  // page.evaluate surfaces as a harness crash instead of a failed assertion. And
  // it must restore with a real gateCapabilities() before returning: the failure
  // path sets capsCache = {}, which strips every cap-on class and would break the
  // control-plane scenarios below (scenario 10 models the same restore).
  const wiring = await page.evaluate(async () => {
    const realFetch = window.fetch;
    window.fetch = (p, o) => (typeof p === "string" && p.startsWith("/api/capabilities")
      ? Promise.resolve(new Response('{"error":"boom"}', { status: 500, headers: { "Content-Type": "application/json" } }))
      : realFetch(p, o));
    await gateCapabilities();
    window.fetch = realFetch;
    const onFailure = (document.querySelector("#meta-version b") || {}).textContent;
    await gateCapabilities();
    return { onFailure, restored: (document.querySelector("#meta-version b") || {}).textContent };
  });
  wiring.onFailure === "—"
    ? ok("sidebar: a failing capabilities fetch actually reaches the '—' branch")
    : bad("sidebar: a failing capabilities fetch actually reaches the '—' branch", `got ${JSON.stringify(wiring.onFailure)} — capsOK never false?`);
  wiring.restored === wantVer
    ? ok("sidebar: version repainted once capabilities recover")
    : bad("sidebar: version repainted once capabilities recover", `got ${JSON.stringify(wiring.restored)}`);

  // Scenario 0e — the coverage card's capture-liveness rendering (#1227),
  // driven through the REAL covCard with stub payloads (the technique the
  // ext-view scenarios use). The thing under test is not "a chip appeared" but
  // that the SAME lag number renders differently depending on the verdict: a
  // dead daemon and a quiet source used to paint identical amber, which is the
  // ambiguity this whole epic exists to remove.
  const cov = await page.evaluate(() => {
    const read = (c) => {
      const card = covCard(c);
      const chips = Array.from(card.querySelectorAll(".cov-chip")).map((n) => ({ text: n.textContent, cls: n.className }));
      const lines = Array.from(card.querySelectorAll(".cov-line")).map((n) => ({ text: n.textContent, cls: n.className }));
      return { chips, lines };
    };
    const base = { delta_from: "2026-08-01 00:00:00", delta_to: "2026-08-07 00:00:00", continuity: "ok", lag_seconds: 3600 };
    return {
      stalled: read({ ...base, freshness: "stalled", checkpoint_age_seconds: 3600 }),
      idle: read({ ...base, freshness: "idle" }),
      current: read({ ...base, freshness: "current", lag_seconds: 5 }),
      none: read({ delta_from: "2026-08-01 00:00:00", delta_to: "2026-08-07 00:00:00", continuity: "none", freshness: "none" }),
    };
  });
  const chipFor = (r, prefix) => r.chips.find((c) => c.text.startsWith(prefix)) || { text: "", cls: "" };

  // stalled: the lag chip must go RED, not amber — the window's upper edge is
  // frozen and changes since are not recoverable.
  chipFor(cov.stalled, "capture lag").cls.includes("bad")
    ? ok("coverage: a stalled stream paints the lag chip as an error")
    : bad("coverage: a stalled stream paints the lag chip as an error", `cls=${chipFor(cov.stalled, "capture lag").cls}`);
  cov.stalled.lines.some((l) => l.cls.includes("bad") && /STALLED/.test(l.text))
    ? ok("coverage: stalled renders an explicit error line")
    : bad("coverage: stalled renders an explicit error line", JSON.stringify(cov.stalled.lines));

  // idle: the SAME 3600s lag must NOT read as an error, and the card must say
  // it cannot tell a quiet source from a lagging one.
  chipFor(cov.idle, "capture lag").cls.includes("bad")
    ? bad("coverage: an idle stream is not an error state", "the lag chip is red — idle is not a fault")
    : ok("coverage: an idle stream is not an error state");
  cov.idle.lines.some((l) => /identical/.test(l.text))
    ? ok("coverage: idle admits it cannot tell quiet from lagging")
    : bad("coverage: idle admits it cannot tell quiet from lagging", JSON.stringify(cov.idle.lines));

  chipFor(cov.current, "capture lag").cls.includes("ok")
    ? ok("coverage: a current stream paints the lag chip green")
    : bad("coverage: a current stream paints the lag chip green", `cls=${chipFor(cov.current, "capture lag").cls}`);

  // "none" is a NON-CLAIM (a file-mode index ran no capture). Green there would
  // paint the absence of a claim as assurance — the same rule continuity follows.
  chipFor(cov.none, "capture ").cls.includes("ok")
    ? bad("coverage: a file-mode index never claims healthy capture", "the freshness chip is green for a no-claim")
    : ok("coverage: a file-mode index never claims healthy capture");

  await page.evaluate(() => openServersModal());
  await page.waitForSelector("#servers-list", { timeout: 5000 });
  await page.waitForTimeout(400);

  // Scenario 1 — control plane survives a broken (unprovisioned) selected
  // server: Start button + the "monitor" copy must render. Guards the
  // /api/capabilities 502 cascade (a per-source index that doesn't exist must
  // not zero out the process-level Monitor capability).
  const cp = await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("#servers-list button"));
    const start = btns.find((b) => b.textContent.trim() === "Start");
    const copy = document.querySelector('.modal-desc [data-capability="monitor"]');
    return { start: !!start, copy: copy ? getComputedStyle(copy).display !== "none" : false };
  });
  cp.start ? ok("control plane: Start button present") : bad("control plane: Start button present", "missing — caps cascade?");
  cp.copy ? ok("control plane: monitor copy visible") : bad("control plane: monitor copy visible", "hidden — caps cascade?");

  // Scenario 2 — primary button stays visible on hover (gradient survives;
  // .btn:hover must not win the background and leave white text on light gray).
  await page.hover("#server-add");
  await page.waitForTimeout(150);
  const hov = await page.evaluate(() => {
    const b = document.getElementById("server-add");
    const cs = getComputedStyle(b);
    return { bgImage: cs.backgroundImage, bgColor: cs.backgroundColor, color: cs.color };
  });
  // Visibility is contrast: the gradient must survive AND the text must stay
  // white. Checking only the background would miss a light-text regression.
  /gradient/.test(hov.bgImage)
    ? ok("button: primary keeps gradient on hover")
    : bad("button: primary keeps gradient on hover", `bgImage=${hov.bgImage} bgColor=${hov.bgColor}`);
  hov.color === "rgb(255, 255, 255)"
    ? ok("button: primary text stays white on hover")
    : bad("button: primary text stays white on hover", `color=${hov.color}`);

  // Scenario 3 — editing a monitored source keeps the optional "bring your own
  // index" section COLLAPSED (its index DSN is auto-derived; expanding it shows
  // a per-source index the operator never typed).
  await page.evaluate(() => {
    const edit = Array.from(document.querySelectorAll("#servers-list button")).find((b) => b.textContent.trim() === "Edit");
    edit.click();
  });
  await page.waitForSelector("#server-advanced", { timeout: 5000 });
  await page.waitForTimeout(200);
  const form = await page.evaluate(() => {
    const adv = document.getElementById("server-advanced");
    const src = document.querySelector('input[name="source_user"]');
    return { advOpen: adv.open, srcVisible: src ? src.offsetParent !== null : false };
  });
  !form.advOpen ? ok("form: advanced section collapsed for a source entry") : bad("form: advanced section collapsed for a source entry", "auto-expanded");
  form.srcVisible ? ok("form: source fields visible") : bad("form: source fields visible", "hidden");

  // #1605 / #1608: the form answers where the operator is looking. The message
  // and the startup-check cards precede the button row in the DOM, and Test
  // writes its result into the row's own slot, beside its button, within a
  // few seconds; nothing lands below the buttons.
  const order = await page.evaluate(() => {
    const foot = document.querySelector("#server-form-mount .modal-foot");
    const msg = document.getElementById("server-form-msg");
    const cards = document.getElementById("doctor-cards");
    const slot = document.getElementById("server-test-result");
    const before = (n) => !!(n && foot && (n.compareDocumentPosition(foot) & Node.DOCUMENT_POSITION_FOLLOWING));
    return { msg: before(msg), cards: before(cards), slotInFoot: !!(slot && foot && foot.contains(slot)) };
  });
  order.msg && order.cards ? ok("form: message and check cards precede the button row") : bad("form: message and check cards precede the button row", JSON.stringify(order));
  order.slotInFoot ? ok("form: Test result slot sits in the button row") : bad("form: Test result slot sits in the button row", JSON.stringify(order));
  await page.click("#server-test");
  let testText = "";
  for (let i = 0; i < 40 && !/[✓✗○]/.test(testText); i++) {
    await page.waitForTimeout(250);
    testText = await page.evaluate(() => (document.getElementById("server-test-result") || {}).textContent || "");
  }
  /[✓✗○]/.test(testText) ? ok("form: Test connection answers beside its button") : bad("form: Test connection answers beside its button", `slot=${JSON.stringify(testText)}`);
  // Only meaningful once the answer arrived; sampled mid-flight these would
  // blame the button for a slow request.
  if (/[✓✗○]/.test(testText)) {
    const after = await page.evaluate(() => ({
      enabled: !document.getElementById("server-test").disabled,
      msg: (document.getElementById("server-form-msg") || {}).textContent || "",
    }));
    after.enabled ? ok("form: Test button is usable again after the answer") : bad("form: Test button is usable again after the answer", "still disabled");
    after.msg === "" ? ok("form: Test leaves the message line empty") : bad("form: Test leaves the message line empty", JSON.stringify(after.msg));
  }

  // Scenario 4 — the REAL missing-index path (not a fabricated string): query
  // a data endpoint against the default (unprovisioned wp) server, take the
  // ACTUAL backend error, and feed it to renderError. This proves the backend
  // 1049 reaches the frontend in the shape the empty-state needs — i.e. that
  // scrubDSNError preserves the index db name and the 1049 text survives.
  await page.evaluate(() => closeServersModal());
  const es = await page.evaluate(async () => {
    let errMsg = null;
    try { await api("/api/status"); } catch (e) { errMsg = (e && e.message) || String(e); }
    if (!errMsg) return { errMsg: null };
    const v = document.getElementById("view") || document.querySelector(".view") || document.body;
    renderError(v, new Error(errMsg));
    const empty = document.querySelector(".empty");
    return { errMsg, empty: empty ? empty.textContent : null };
  });
  if (!es.errMsg) {
    bad("error: real 1049 reaches the frontend", "/api/status did not error for the unprovisioned default server");
  } else if (!/Unknown database '(bintrail_idx_[^']+)'/.test(es.errMsg)) {
    bad("error: real 1049 reaches the frontend", `backend error not the expected 1049 shape: ${es.errMsg}`);
  } else {
    ok("error: real 1049 reaches the frontend");
    const t = es.empty || "";
    !es.empty ? bad("error: 1049 renders friendly empty state", "no .empty element produced") : ok("error: 1049 renders friendly empty state");
    /indexing yet/.test(t) ? ok("error: 1049 empty state has friendly title") : bad("error: 1049 empty state has friendly title", t);
    /never lives on the source/.test(t) ? ok("error: 1049 empty state clarifies source-vs-index") : bad("error: 1049 empty state clarifies source-vs-index", t);
    /bintrail_idx_/.test(t) ? ok("error: 1049 empty state names the index db") : bad("error: 1049 empty state names the index db", t);
  }

  // Scenario 5 — a pure BYO-index entry (no source) must auto-EXPAND the
  // advanced section: the index IS the whole form. This is the other arm of
  // byoIndex (scenario 3 covers the collapse-for-source arm). Its dbname points
  // at the daemon's own (existing) boot index, so it resolves cleanly.
  const byoId = await page.evaluate(async (baselineDir) => {
    const res = await api("/api/servers", { method: "POST", body: {
      name: "byo-idx", host: "127.0.0.1", port: "13306", user: "root", password: "testroot", dbname: "bintrail_e2e_idx",
      baseline_dir: baselineDir,
    } });
    return res.id;
  }, process.env.E2E_BASELINE_DIR || "");
  await page.evaluate(() => openServersModal());
  await page.waitForSelector("#servers-list", { timeout: 5000 });
  await page.waitForTimeout(300);
  await page.evaluate((id) => editServer(id), byoId);
  await page.waitForSelector("#server-advanced", { timeout: 5000 });
  await page.waitForTimeout(200);
  const byoOpen = await page.evaluate(() => document.getElementById("server-advanced").open);
  byoOpen
    ? ok("form: advanced section expanded for a BYO-index entry")
    : bad("form: advanced section expanded for a BYO-index entry", "collapsed — the byoIndex open-arm regressed");


  // Scenario 6 — Recover/Cascade merge. Cascade recovery is no longer a separate
  // tab: it is auto-detected inside the single Recover flow (the backend routes by
  // detection and folds the invisible children into one script when the target is
  // an FK parent). Pin the REMOVAL here — a stray cascade nav item, route, palette
  // entry, or render function would be a merge regression — and confirm Recover is
  // the sole Resolve tab and still renders its form.
  await page.evaluate(() => closeServersModal());
  const merged = await page.evaluate(() => {
    const cmds = (typeof cmdkCommands === "function") ? cmdkCommands().map((c) => c.label) : [];
    return {
      cascadeNavGone: !document.querySelector('.nav-item[data-route="cascade"]'),
      cascadeNotInPalette: !cmds.includes("Cascade recovery"),
      cascadeRouteGone: typeof ROUTES !== "undefined" ? !ROUTES.includes("cascade") : true,
      recoverNavPresent: !!document.querySelector('.nav-item[data-route="recover"]'),
      renderCascadeGone: typeof renderCascade === "undefined",
    };
  });
  merged.cascadeNavGone ? ok("merge: cascade nav item removed") : bad("merge: cascade nav item removed", "still present — merge regression");
  merged.cascadeNotInPalette ? ok("merge: cascade command-palette entry removed") : bad("merge: cascade command-palette entry removed", "still present");
  merged.cascadeRouteGone ? ok("merge: /cascade route removed from ROUTES") : bad("merge: /cascade route removed from ROUTES", "still routable");
  merged.recoverNavPresent ? ok("merge: Recover is the sole Resolve tab") : bad("merge: Recover is the sole Resolve tab", "recover nav missing");
  merged.renderCascadeGone ? ok("merge: renderCascade function removed") : bad("merge: renderCascade function removed", "still defined");

  await page.evaluate(() => navigate("recover"));
  await page.waitForSelector("#recover-form", { timeout: 5000 });
  const rform = await page.evaluate(() => {
    const f = document.getElementById("recover-form");
    if (!f) return { form: false };
    return {
      form: true,
      onRoute: location.pathname === "/recover",
      schema: !!f.querySelector('[name="schema"]'),
      table: !!f.querySelector('[name="table"]'),
      pk: !!f.querySelector('[name="pk"]'),
      noCascadeForm: !document.getElementById("cascade-form"),
    };
  });
  rform.form ? ok("merge: recover form renders") : bad("merge: recover form renders", "#recover-form missing");
  rform.onRoute ? ok("merge: navigates to /recover") : bad("merge: navigates to /recover", "wrong route");
  (rform.schema && rform.table && rform.pk) ? ok("merge: recover schema/table/pk fields present") : bad("merge: recover schema/table/pk fields present", JSON.stringify(rform));
  rform.noCascadeForm ? ok("merge: no standalone cascade form remains") : bad("merge: no standalone cascade form remains", "#cascade-form still present");

  // Scenario 7 — PostgreSQL replication-health panel (#599). pgHealthCard is the
  // load-bearing anti-silent-failure surface: a frozen snapshot over a stopped daemon
  // must degrade to muted/warn (never render healthy-green), a missing/unparseable
  // checked_at must read as stale (fail-safe), a recorded probe failure must show as
  // "probe failing" (not a blank panel), and an absent slot reads "not found yet".
  // Driven directly with fixtures — the seeded console is MySQL-sourced, so this
  // unit-drives the render function rather than needing a live PG source.
  const ph = await page.evaluate(() => {
    const iso = (msAgo) => new Date(Date.now() - msAgo).toISOString();
    const base = { exists: true, active: true, wal_status: "reserved", retained_bytes: 16384,
      safe_wal_size: 1073741824, replica_identity_not_full: [] };
    const fresh = pgHealthCard({ ...base, checked_at: iso(3000) });
    const stale = pgHealthCard({ ...base, safe_wal_size: null, checked_at: iso(300000) });
    const noTs = pgHealthCard({ ...base }); // missing checked_at
    const lost = pgHealthCard({ ...base, wal_status: "lost", safe_wal_size: null, checked_at: iso(3000) });
    const perr = pgHealthCard({ exists: false, replica_identity_not_full: [], checked_at: iso(2000), probe_error: "recovery is in progress" });
    const nofound = pgHealthCard({ exists: false, replica_identity_not_full: [], checked_at: iso(2000) });
    return {
      freshGreen: !fresh.classList.contains("card-stale") && /checked \d+s ago/.test(fresh.textContent) && !!fresh.querySelector(".hstat-ok"),
      staleMuted: stale.classList.contains("card-stale") && /daemon may be stopped/.test(stale.textContent),
      missingTsStale: noTs.classList.contains("card-stale"),
      lostRed: !!lost.querySelector(".hstat-err") && /lost/.test(lost.textContent),
      probeErrVisible: /probe failing/i.test(perr.textContent) && /recovery is in progress/.test(perr.textContent) && !!perr.querySelector(".hstat-err"),
      notFoundShown: /not found yet/.test(nofound.textContent),
    };
  });
  ph.freshGreen ? ok("pg-health: fresh snapshot renders healthy + 'checked Ns ago'") : bad("pg-health: fresh snapshot renders healthy + 'checked Ns ago'", "missing fresh/ok state");
  ph.staleMuted ? ok("pg-health: stale snapshot degrades to muted/warn (never silent-green)") : bad("pg-health: stale snapshot degrades to muted/warn (never silent-green)", "no card-stale / warn footer");
  ph.missingTsStale ? ok("pg-health: missing checked_at reads as stale (fail-safe)") : bad("pg-health: missing checked_at reads as stale (fail-safe)", "rendered fresh");
  ph.lostRed ? ok("pg-health: a lost slot renders with the red error chip (critical state never benign)") : bad("pg-health: a lost slot renders with the red error chip (critical state never benign)", "wal_status=lost not .hstat-err");
  ph.probeErrVisible ? ok("pg-health: probe failure shows 'probe failing' (not a blank panel)") : bad("pg-health: probe failure shows 'probe failing' (not a blank panel)", "probe_error not surfaced");
  ph.notFoundShown ? ok("pg-health: absent slot shows 'not found yet'") : bad("pg-health: absent slot shows 'not found yet'", "missing not-found state");

  // Scenario 8 — stream-continuity surface (#645). Fixture-drives continuityBox
  // (pure, like pgHealthCard): the green "no gaps" affirmation must RENDER as the
  // green ok-box (color actually applied, not just the class present), a stamped
  // gap must render the red error-box and take precedence over a stale ok, and the
  // unknown/legacy/missing/nil cases must show NEITHER box (no clean verdict from
  // un-evaluated data). This is the only check that the new green badge displays
  // correctly — the Go suite sees the JSON contract, never the pixels.
  const cont = await page.evaluate(() => {
    const okBox = continuityBox({ continuity: { status: "ok" } }, false);
    const gapBox = continuityBox({ gap_lost: { at: "2026-06-22 12:00:00", detail: "unfillable binlog gap" } }, false);
    const gapWins = continuityBox({ gap_lost: { at: "t", detail: "d" }, continuity: { status: "ok" } }, false);
    const unknownBox = continuityBox({ continuity: { status: "unknown" } }, false);
    const missingBox = continuityBox({ mode: "gtid" }, false); // legacy backend: no continuity field
    const nilBox = continuityBox(null, false);
    // The green box must actually be GREEN — append it and read the computed border
    // color, so a CSS-cascade break (class present, color not applied) is caught.
    let okBorder = "";
    if (okBox) { document.body.appendChild(okBox); okBorder = getComputedStyle(okBox).borderColor; okBox.remove(); }
    return {
      okGreenClass: !!okBox && okBox.classList.contains("ok-box"),
      okGreenText: !!okBox && /No gaps in captured stream/.test(okBox.textContent) && /does not mean the stream is running/.test(okBox.textContent),
      okBorder,
      gapRed: !!gapBox && gapBox.classList.contains("error-box") && /permanently lost/i.test(gapBox.textContent),
      gapPrecedence: !!gapWins && gapWins.classList.contains("error-box"),
      unknownNeither: unknownBox === null,
      missingNeither: missingBox === null,
      nilNeither: nilBox === null,
    };
  });
  cont.okGreenClass ? ok("continuity: ok renders the green ok-box") : bad("continuity: ok renders the green ok-box", "no .ok-box");
  cont.okGreenText ? ok("continuity: green box scoped to contiguity (not a liveness claim)") : bad("continuity: green box scoped to contiguity (not a liveness claim)", "wording missing/overclaims");
  // any non-default, non-transparent border color proves the .ok-box class resolved (--insert green).
  (cont.okBorder && cont.okBorder !== "rgba(0, 0, 0, 0)" && cont.okBorder !== "rgb(0, 0, 0)")
    ? ok("continuity: green ok-box actually renders green (CSS applied)")
    : bad("continuity: green ok-box actually renders green (CSS applied)", `borderColor=${cont.okBorder}`);
  cont.gapRed ? ok("continuity: gap_lost renders the red error-box") : bad("continuity: gap_lost renders the red error-box", "no .error-box");
  cont.gapPrecedence ? ok("continuity: gap_lost takes precedence over a stale ok") : bad("continuity: gap_lost takes precedence over a stale ok", "green won over a gap");
  cont.unknownNeither ? ok("continuity: unknown shows neither box") : bad("continuity: unknown shows neither box", "rendered a box for unknown");
  cont.missingNeither ? ok("continuity: missing continuity (legacy backend) shows no green") : bad("continuity: missing continuity (legacy backend) shows no green", "rendered green without continuity");
  cont.nilNeither ? ok("continuity: nil stream shows neither box") : bad("continuity: nil stream shows neither box", "rendered a box for nil stream");

  // Scenario 8c — index disk (#1444). capacityCard/capacityBox are pure and
  // fixture-drivable like continuityBox: the doctor's grade arrives as
  // `status`/`reason` and the copy keys on the reason. Pinned here, in a real
  // browser: the fail grade renders the red error-box (color applied, not just
  // the class), a warn grade the warn-box, pass/skip no box at all; free space
  // the backend could not measure is WORDED, never shown as a number; the
  // standalone console's unknown retention is worded too; a failed fetch
  // renders a note inside the card, not a blank; and no rendered string
  // carries an em dash (copy rule).
  const idxDisk = await page.evaluate(() => {
    const base = { measured: true, sample_hours: 6, current_bytes: 6000000, events_per_day: 24000, bytes_per_event: 1000,
      growth_bytes_per_day: 24000000, projected_bytes: 720000000, remaining_bytes: 714000000,
      retention: { known: true, retain: "30d", source: "default", enabled: true } };
    const fail = { ...base, status: "fail", reason: "growth_exceeds_free", free_known: true, free_bytes: 10000000, days_until_full: 0.42 };
    const warn = { ...base, status: "warn", reason: "free_under_floor", remaining_bytes: 0, free_known: true, free_bytes: 50000000, days_until_full: 2.1 };
    const pass = { ...base, status: "pass", reason: "ok", free_known: true, free_bytes: 2000000000, days_until_full: 83.3 };
    const freeUnknown = { ...base, status: "skip", reason: "free_unknown", free_known: false, free_bytes: 0, free_reason: "mount_unset" };
    const freeRemote = { ...base, status: "skip", reason: "free_unknown", free_known: false, free_bytes: 0, free_reason: "index_not_local" };
    const freeTunnel = { ...base, status: "skip", reason: "free_unknown", free_known: false, free_bytes: 0, free_reason: "host_unconfirmed" };
    const freeLegacy = { ...base, status: "skip", reason: "free_unknown", free_known: false, free_bytes: 0 };
    const serve = { ...base, status: "skip", reason: "retention_unknown", projected_bytes: 0, remaining_bytes: 0,
      retention: { known: false, enabled: false }, free_known: true, free_bytes: 2000000000, days_until_full: 83.3 };
    const failBox = capacityBox(fail), warnBox = capacityBox(warn), passBox = capacityBox(pass), unknownBox = capacityBox(freeUnknown), serveBox = capacityBox(serve);
    const failCard = capacityCard(fail), passCard = capacityCard(pass), unknownCard = capacityCard(freeUnknown), serveCard = capacityCard(serve), errCard = capacityCard({ error: "boom" });
    const remoteCard = capacityCard(freeRemote), legacyCard = capacityCard(freeLegacy), tunnelCard = capacityCard(freeTunnel);
    let failBorder = "";
    document.body.appendChild(failBox); failBorder = getComputedStyle(failBox).borderColor; failBox.remove();
    const texts = [failBox, warnBox, failCard, passCard, unknownCard, serveCard, errCard, remoteCard, legacyCard, tunnelCard].map((n) => n.textContent).join("\n");
    const unknownState = unknownCard.querySelector(".hstat");
    return {
      failRed: failBox.classList.contains("error-box") && /will fill before rotation/.test(failBox.textContent) && /under a day/.test(failBox.textContent),
      failBorder,
      warnOrange: warnBox.classList.contains("warn-box") && /Little free space/.test(warnBox.textContent),
      passNone: passBox === null && unknownBox === null && serveBox === null,
      failState: !!failCard.querySelector(".hstat-err") && /will fill/.test(failCard.textContent),
      passNote: /Rotation caps the index/.test(passCard.textContent) && /1\.9 GB/.test(passCard.textContent),
      unknownWorded: /not measurable from here/.test(unknownCard.textContent) && !/0 B/.test(unknownCard.textContent) && !!unknownState && unknownState.classList.contains("hstat-muted"),
      // #1527: the reason the doctor landed on, said as a fix when there is
      // one, and never as a guess about where the index runs.
      unknownNamesTheFix: /BINTRAIL_INDEX_DATADIR_RO/.test(unknownCard.textContent) && !/another host or container/.test(unknownCard.textContent),
      remoteOffersNoFix: /another address/.test(remoteCard.textContent) && !/BINTRAIL_INDEX_DATADIR_RO/.test(remoteCard.textContent),
      legacyStaysHonest: /not measured/.test(legacyCard.textContent) && !/another host or container/.test(legacyCard.textContent),
      // A local address whose server is not confirmed to be this machine (a
      // port-forward looks exactly like this): the mount is offered WITH the
      // precondition, or an operator points it at a local mysqld that is not
      // the index and gets a measured number for the wrong volume.
      tunnelQualifiesTheMount: /cannot confirm/.test(tunnelCard.textContent) && /BINTRAIL_INDEX_DATADIR_RO/.test(tunnelCard.textContent) && /nothing else/.test(tunnelCard.textContent),
      serveWorded: /not known here/.test(serveCard.textContent) && !/steady size/.test(serveCard.textContent),
      errNote: /Could not measure the index disk: boom/.test(errCard.textContent),
      noEmDash: !/—/.test(texts),
    };
  });
  idxDisk.failRed ? ok("index disk: fail renders the red error-box with days until full") : bad("index disk: fail renders the red error-box with days until full", "no .error-box or wording missing");
  (idxDisk.failBorder && idxDisk.failBorder !== "rgba(0, 0, 0, 0)" && idxDisk.failBorder !== "rgb(0, 0, 0)")
    ? ok("index disk: red error-box actually renders (CSS applied)")
    : bad("index disk: red error-box actually renders (CSS applied)", `borderColor=${idxDisk.failBorder}`);
  idxDisk.warnOrange ? ok("index disk: warn renders the warn-box") : bad("index disk: warn renders the warn-box", "no .warn-box");
  idxDisk.passNone ? ok("index disk: pass and skip grades render no box") : bad("index disk: pass and skip grades render no box", "a box rendered for pass/skip");
  idxDisk.failState ? ok("index disk: card state chip is red on fail") : bad("index disk: card state chip is red on fail", "no .hstat-err");
  idxDisk.passNote ? ok("index disk: pass card reads the free space and the cap") : bad("index disk: pass card reads the free space and the cap", "note or free figure missing");
  idxDisk.unknownWorded ? ok("index disk: unmeasurable free space is worded, never a number") : bad("index disk: unmeasurable free space is worded, never a number", "showed 0 B or no muted chip");
  idxDisk.unknownNamesTheFix ? ok("index disk: a missing datadir mount names the mount, not a topology") : bad("index disk: a missing datadir mount names the mount, not a topology", "no BINTRAIL_INDEX_DATADIR_RO, or still asserts another host");
  idxDisk.remoteOffersNoFix ? ok("index disk: an index at another address offers no local mount fix") : bad("index disk: an index at another address offers no local mount fix", "offered a mount that would measure the wrong volume");
  idxDisk.legacyStaysHonest ? ok("index disk: a response with no free_reason still says what happened") : bad("index disk: a response with no free_reason still says what happened", "blank or guessed reason");
  idxDisk.tunnelQualifiesTheMount ? ok("index disk: an unconfirmed local host gets the mount fix with its precondition") : bad("index disk: an unconfirmed local host gets the mount fix with its precondition", "advice missing, or offered without the warning");
  idxDisk.serveWorded ? ok("index disk: standalone console says the window is not known here") : bad("index disk: standalone console says the window is not known here", "claimed a window or a steady size");
  idxDisk.errNote ? ok("index disk: a failed fetch renders a note inside the card") : bad("index disk: a failed fetch renders a note inside the card", "card blank on error");
  idxDisk.noEmDash ? ok("index disk: rendered copy carries no em dash") : bad("index disk: rendered copy carries no em dash", "em dash in rendered text");

  // Scenario 8b — capture-degraded banner (#1296). captureHealthBox is pure
  // like continuityBox. What it must NOT do again: render advice written in
  // this file. The cause/remedy/scope prose is built by the backend
  // (status.ExplainCaptureSkips) and shipped in capture_health.explanation, so
  // `bintrail status` and the console cannot drift — and the half that drifted
  // would be the one saying what a remedy does NOT recover. The fixture drives
  // both the current backend (explanation present) and an older one (absent).
  const cap = await page.evaluate(() => {
    // schemaSnapshotButton() is gated on the capability and on a REAL registry
    // server (the reserved "default" is the daemon's own CLI stream, which the
    // control plane refuses). Force both, restored below: without them the
    // button is null and the placement assertions would pass by rendering
    // nothing, which is the shape this whole scenario exists to catch.
    const keepCaps = capsCache.schema_snapshot_trigger, keepServer = currentServer;
    capsCache.schema_snapshot_trigger = true;
    currentServer = "e2e-registry-server";
    const withExpl = captureHealthBox({
      capture_health: {
        status: "degraded", total_skipped: 3, last_skip_at: "2026-08-04 19:49:33",
        skipped: { table_not_in_snapshot: { count: 3, tables: ["shop.plugin_log"] } },
        explanation: [
          "shop.plugin_log changed on the source but is missing from the schema snapshot.",
          "None of this recovers what was already skipped.",
        ],
      },
    });
    const legacy = captureHealthBox({
      capture_health: { status: "degraded", total_skipped: 3, skipped: { table_not_in_snapshot: { count: 3 } } },
    });
    const okBox = captureHealthBox({ capture_health: { status: "ok" } });
    const nilBox = captureHealthBox(null);
    // Historic vs active (#1312): the SAME tally, distinguished only by the
    // backend's skips_predate_snapshot. Historic must go quiet without going
    // away — the events are still permanently missing.
    const historic = captureHealthBox({
      capture_health: {
        status: "degraded", total_skipped: 3, last_skip_at: "2026-08-04 19:49:33",
        snapshot_at: "2026-08-11 12:00:00", skips_predate_snapshot: true,
        skipped: { table_not_in_snapshot: { count: 3 } },
        explanation: ["Nothing has been skipped since the current schema snapshot was taken (2026-08-11 12:00:00)."],
      },
    });
    // Acknowledged (#1314): the same permanent loss, retired by an operator.
    // It must go MUTED (not the green "nothing since the snapshot" box — that
    // one is a claim about capture, this one is a claim about a human), keep
    // stating the loss, and stop offering the button it already consumed.
    const acked = captureHealthBox({
      capture_health: {
        status: "degraded", total_skipped: 3, last_skip_at: "2026-08-04 19:49:33",
        acknowledged: true, acknowledged_at: "2026-08-11 20:14:00",
        skipped: { table_not_in_snapshot: { count: 3 } },
        explanation: ["Nothing has been skipped since the current schema snapshot was taken (2026-08-11 12:00:00)."],
      },
    });
    // Render it to confirm the per-paragraph spacing rule resolves — without it
    // the caveat merges into the wall of text above it. The lines now live in a
    // <details>, so it must be opened before measuring: a closed disclosure has
    // no layout, and a marginTop read from it would prove nothing.
    let lineGap = "", detailsClosedByDefault = null, buttonAtTopLevel = null, historicButtonInDetails = null;
    if (withExpl) {
      document.body.appendChild(withExpl);
      const det = withExpl.querySelector("details");
      detailsClosedByDefault = !!det && !det.open;
      // The action stays at the top level while the skips are ACTIVE — that is
      // the state where pressing it is the fix.
      buttonAtTopLevel = !!withExpl.querySelector(":scope > .warn-actions");
      if (det) det.open = true;
      const line = withExpl.querySelector(".warn-line");
      lineGap = line ? getComputedStyle(line).marginTop : "";
      withExpl.remove();
    }
    if (historic) {
      document.body.appendChild(historic);
      // ...and moves inside the disclosure once they are HISTORIC: it has been
      // pressed already, and a button under the headline invites pressing it
      // forever against a tally that will never move.
      historicButtonInDetails = !historic.querySelector(":scope > .warn-actions")
        && !!historic.querySelector("details .warn-actions");
      historic.remove();
    }
    let ackedGap = "", ackedPaint = null;
    if (acked) {
      document.body.appendChild(acked);
      const det = acked.querySelector("details");
      if (det) det.open = true;
      const line = acked.querySelector(".warn-line");
      ackedGap = line ? getComputedStyle(line).marginTop : "";
      // The whole point of the muted box is that it is QUIET, and a CSS
      // custom property that does not exist resolves to nothing — the rule
      // matches, the spacing assertion above passes, and the box paints with
      // the page's default colours. (That is not hypothetical: the first
      // draft of this rule referenced three tokens this stylesheet has never
      // defined.) Read the real computed paint and compare it against the
      // orange alarm's.
      const mc = getComputedStyle(acked);
      let wc = null;
      if (withExpl) {
        document.body.appendChild(withExpl);
        const w = getComputedStyle(withExpl);
        wc = { color: w.color, bg: w.backgroundColor, border: w.borderTopColor };
        withExpl.remove();
      }
      ackedPaint = {
        color: mc.color, bg: mc.backgroundColor, border: mc.borderTopColor,
        transparentBg: mc.backgroundColor === "rgba(0, 0, 0, 0)" || mc.backgroundColor === "transparent",
        differsFromAlarm: !!wc && (mc.color !== wc.color && mc.backgroundColor !== wc.bg && mc.borderTopColor !== wc.border),
      };
      acked.remove();
    }
    capsCache.schema_snapshot_trigger = keepCaps;
    currentServer = keepServer;
    return {
      showsBackendProse: !!withExpl && /shop\.plugin_log/.test(withExpl.textContent)
        && /recovers what was already skipped/.test(withExpl.textContent),
      noOperatorBlame: !!withExpl && !/take a fresh snapshot on the source/.test(withExpl.textContent)
        && !/check the capture log/.test(withExpl.textContent),
      legacyFallback: !!legacy && /too old to say why/.test(legacy.textContent)
        && !/recovers what was already skipped/.test(legacy.textContent),
      okNone: okBox === null,
      nilNone: nilBox === null,
      lineGap,
      detailsClosedByDefault,
      buttonAtTopLevel,
      historicButtonInDetails,
      activeIsOrange: !!withExpl && withExpl.classList.contains("warn-box"),
      historicIsQuiet: !!historic && historic.classList.contains("ok-box")
        && !historic.classList.contains("warn-box"),
      historicStillStatesLoss: !!historic && /missing from the index for good/.test(historic.textContent),
      historicDatesTheSnapshot: !!historic && /2026-08-11 12:00:00/.test(historic.textContent),
      // An old daemon sends no anchor: no claim, so it must stay loud.
      legacyStaysLoud: !!legacy && legacy.classList.contains("warn-box"),
      // Mark-as-read (#1314). The button must be reachable WITHOUT opening the
      // disclosure while the record is unacknowledged: it is the one thing an
      // operator staring at a permanent record actually wants.
      ackButtonAtTopLevel: !!withExpl && !!withExpl.querySelector(":scope > .ack-actions button"),
      ackedIsMuted: !!acked && acked.classList.contains("muted-box")
        && !acked.classList.contains("warn-box") && !acked.classList.contains("ok-box"),
      // ...and gone once acknowledged: pressing it again would 400.
      ackedHasNoAckButton: !!acked && !acked.querySelector(".ack-actions"),
      ackedStillStatesLoss: !!acked && /missing from the index for good/.test(acked.textContent),
      ackedNamesTheMoment: !!acked && /2026-08-11 20:14:00/.test(acked.textContent),
      // The muted box must inherit the paragraph spacing, or its disclosure is
      // the wall of text the orange one stopped being.
      ackedGap,
      ackedPaint,
    };
  });
  // Scenario 8b-bis — /api/events warnings must REACH the screen (#1311). The
  // server computed the archive-exclusion notice correctly and the events view
  // dropped `data.warnings` on the floor, so the flagship case -- a profiled
  // session's default browse -- was silent in the UI while the API said the
  // right thing. A server-side test cannot see that; this can.
  const evWarn = await page.evaluate(() => {
    const box = document.createElement("div");
    box.id = "probe-warnings";
    document.body.appendChild(box);
    renderWarnings(box, ["LIVE INDEX ONLY probe notice"]);
    const rendered = /LIVE INDEX ONLY probe notice/.test(box.textContent);
    box.remove();
    return {
      rendered,
      // The container the events view must build, by id. Without it
      // renderWarnings has nowhere to write and the notice is lost.
      hasEventsContainer: /id:\s*"ev-warnings"/.test(renderEvents.toString()),
      // ...and it must actually be filled from the response.
      wiredToResponse: /#ev-warnings/.test(runEventsQuery.toString())
        && /data\.warnings/.test(runEventsQuery.toString()),
      // previewRecover shares #recover-warnings with generateUndo; clearing it
      // made the notice vanish the moment a filter was adjusted.
      previewKeepsServerWarnings: /data\.warnings/.test(previewRecover.toString())
        && !/clear\(warns\)/.test(previewRecover.toString()),
    };
  });
  evWarn.rendered ? ok("warnings: renderWarnings paints a notice into a container") : bad("warnings: renderWarnings paints a notice into a container", JSON.stringify(evWarn));
  evWarn.hasEventsContainer ? ok("warnings: the events view builds a warnings container") : bad("warnings: the events view builds a warnings container", "no #ev-warnings — an API notice has nowhere to land");
  evWarn.wiredToResponse ? ok("warnings: the events query renders the response's own warnings") : bad("warnings: the events query renders the response's own warnings", "data.warnings is computed server-side and dropped at the browser");
  evWarn.previewKeepsServerWarnings ? ok("warnings: Preview keeps the server's notices instead of clearing them") : bad("warnings: Preview keeps the server's notices instead of clearing them", "re-previewing wipes the scope caveat");

  cap.showsBackendProse ? ok("capture health: banner renders the backend's explanation (table named)") : bad("capture health: banner renders the backend's explanation (table named)", JSON.stringify(cap));
  cap.noOperatorBlame ? ok("capture health: the old blame-the-operator wording is gone") : bad("capture health: the old blame-the-operator wording is gone", "old copy still rendered");
  cap.legacyFallback ? ok("capture health: an old backend gets a fallback that promises nothing") : bad("capture health: an old backend gets a fallback that promises nothing", "fallback missing or invents advice");
  (cap.okNone && cap.nilNone) ? ok("capture health: ok/nil render no banner") : bad("capture health: ok/nil render no banner", `ok=${cap.okNone} nil=${cap.nilNone}`);
  (cap.lineGap && cap.lineGap !== "0px") ? ok("capture health: explanation paragraphs are spaced apart (CSS applied)") : bad("capture health: explanation paragraphs are spaced apart (CSS applied)", `marginTop=${cap.lineGap}`);
  cap.detailsClosedByDefault ? ok("capture health: the long explanation starts collapsed") : bad("capture health: the long explanation starts collapsed", "details rendered open — the banner is a wall of text again");
  cap.activeIsOrange ? ok("capture health: an active skip stays the orange alarm") : bad("capture health: an active skip stays the orange alarm", "active skips lost their alarm styling");
  cap.historicIsQuiet ? ok("capture health: skips that predate the current snapshot go quiet") : bad("capture health: skips that predate the current snapshot go quiet", "historic skips still render as a live alarm");
  cap.historicStillStatesLoss ? ok("capture health: the quiet box still states the events are gone for good") : bad("capture health: the quiet box still states the events are gone for good", "going quiet dropped the permanent-loss statement");
  cap.historicDatesTheSnapshot ? ok("capture health: the quiet box names the snapshot it compared against") : bad("capture health: the quiet box names the snapshot it compared against", "no snapshot date — the operator cannot check the claim");
  cap.legacyStaysLoud ? ok("capture health: a backend with no anchor stays loud") : bad("capture health: a backend with no anchor stays loud", "missing anchor was read as 'quiet' — that hides a live failure");
  cap.buttonAtTopLevel ? ok("capture health: the fix button is under the headline while skips are active") : bad("capture health: the fix button is under the headline while skips are active", "the actionable state buried its action");
  cap.historicButtonInDetails ? ok("capture health: the quiet box moves the button into the disclosure") : bad("capture health: the quiet box moves the button into the disclosure", "a pressed button still sits under the headline");
  cap.ackButtonAtTopLevel ? ok("capture health: Mark-as-read is reachable without opening the disclosure") : bad("capture health: Mark-as-read is reachable without opening the disclosure", "the only action on a permanent record is buried");
  cap.ackedIsMuted ? ok("capture health: an acknowledged record renders muted, not as an alarm") : bad("capture health: an acknowledged record renders muted, not as an alarm", "acknowledging did not calm the box");
  cap.ackedHasNoAckButton ? ok("capture health: an acknowledged record stops offering Mark-as-read") : bad("capture health: an acknowledged record stops offering Mark-as-read", "a button that would now 400 is still on screen");
  cap.ackedStillStatesLoss ? ok("capture health: acknowledging keeps the permanent-loss statement on screen") : bad("capture health: acknowledging keeps the permanent-loss statement on screen", "going quiet erased the evidence");
  cap.ackedNamesTheMoment ? ok("capture health: the acknowledged box names when it was acknowledged") : bad("capture health: the acknowledged box names when it was acknowledged", "no timestamp — the operator cannot tell who saw what when");
  (cap.ackedGap && cap.ackedGap !== "0px") ? ok("capture health: the muted box inherits the paragraph spacing (CSS applied)") : bad("capture health: the muted box inherits the paragraph spacing (CSS applied)", `marginTop=${cap.ackedGap}`);
  (cap.ackedPaint && !cap.ackedPaint.transparentBg && cap.ackedPaint.differsFromAlarm)
    ? ok("capture health: the muted box paints its own quiet palette (every token resolves)")
    : bad("capture health: the muted box paints its own quiet palette (every token resolves)", JSON.stringify(cap.ackedPaint));

  // Scenario 8c — the remedy is reachable from the UI (#1296). The whole point
  // of the issue: the banner named an action with no button anywhere. It must
  // appear for a monitored REGISTRY server, and never for the reserved boot
  // entry (which the endpoint refuses — a button that 409s is worse than none).
  const capBtn = await page.evaluate(async () => {
    const servers = (await api("/api/servers")).servers || [];
    const registryServer = servers.find((s) => s.kind !== "ephemeral");
    const before = currentServer;
    const probe = (id) => {
      currentServer = id;
      const box = captureHealthBox({ capture_health: { status: "degraded", total_skipped: 1, skipped: {} } });
      const btn = box ? Array.from(box.querySelectorAll("button")).find((b) => /Refresh schema snapshot/.test(b.textContent)) : null;
      return !!btn;
    };
    try {
      return {
        capOn: !!capsCache.schema_snapshot_trigger,
        // Reported, not assumed: with no registry server the button check would
        // otherwise pass by testing nothing.
        foundRegistryServer: !!registryServer,
        onRegistry: registryServer ? probe(registryServer.id) : false,
        onBoot: probe("default"),
      };
    } finally {
      // Restore even on a throw: page.evaluate throwing crashes this harness,
      // but a half-restored selection would silently point every later scenario
      // at the wrong server.
      currentServer = before;
    }
  });
  capBtn.capOn ? ok("capture health: schema_snapshot_trigger capability reaches the frontend") : bad("capture health: schema_snapshot_trigger capability reaches the frontend", "capsCache.schema_snapshot_trigger falsy");
  (capBtn.foundRegistryServer && capBtn.onRegistry)
    ? ok("capture health: Refresh-schema-snapshot button renders for a registry server")
    : bad("capture health: Refresh-schema-snapshot button renders for a registry server",
      capBtn.foundRegistryServer ? "no button in the banner" : "no registry server in the fixture — the check tested nothing");
  !capBtn.onBoot ? ok("capture health: no Refresh button for the command-line entry") : bad("capture health: no Refresh button for the command-line entry", "offered an action the endpoint refuses");

  // Scenario 9 — Storage page AWS-credentials card (#681). credentialsCard is
  // pure (like pgHealthCard/continuityBox): it must lead with a plain-language
  // summary of which credential source is active, never leave the raw signals
  // as the only content, and each of the mutually-favored signals (static
  // env keys > IAM role > shared config > none) must produce distinct copy —
  // an IAM-role or shared-config setup must never read as "no credentials".
  const cred = await page.evaluate(() => {
    const mk = (aws) => credentialsCard({ aws });
    const none = mk({ access_key_env: false, profile: "", region_env: "", shared_config: false, container_creds: false, web_identity: false });
    const keys = mk({ access_key_env: true, profile: "", region_env: "", shared_config: false, container_creds: false, web_identity: false });
    const ecs = mk({ access_key_env: false, profile: "", region_env: "", shared_config: false, container_creds: true, web_identity: false });
    // The healthy IRSA shape (#1534): token readable AND a role named. The
    // broken shapes (unreadable token, missing role ARN) are pinned at the
    // unit tier; this scenario keeps the register check — a healthy role
    // setup must never read as "no credentials".
    const irsa = mk({ access_key_env: false, profile: "", region_env: "", shared_config: false, container_creds: false, web_identity: true, web_identity_token_readable: true, web_identity_role_arn: true });
    // The two broken IRSA shapes run through the REAL branches here on
    // purpose: the unit tier pins their strings, and a string survives a
    // dead branch — `if (false)` around either arm keeps the Go guard green
    // while the card falls back to the healthy sentence.
    const irsaBroken = mk({ access_key_env: false, profile: "", region_env: "", shared_config: false, container_creds: false, web_identity: true, web_identity_token_readable: false });
    const irsaNoArn = mk({ access_key_env: false, profile: "", region_env: "", shared_config: false, container_creds: false, web_identity: true, web_identity_token_readable: true, web_identity_role_arn: false });
    const shared = mk({ access_key_env: false, profile: "default", region_env: "", shared_config: true, container_creds: false, web_identity: false });
    const adv = none.querySelector("details.form-advanced");
    return {
      noneSummary: /No credentials set directly/.test(none.textContent),
      keysSummary: /access key ID set in an environment variable/.test(keys.textContent),
      ecsSummary: /ECS task-role endpoint/.test(ecs.textContent),
      irsaSummary: /EKS service-account role/.test(irsa.textContent) && !/cannot sign/.test(irsa.textContent),
      irsaBrokenSummary: /cannot be read/.test(irsaBroken.textContent) && /cannot sign/.test(irsaBroken.textContent),
      irsaNoArnSummary: /AWS_ROLE_ARN is not set/.test(irsaNoArn.textContent),
      sharedSummary: /shared ~\/\.aws config file/.test(shared.textContent),
      hasDisclosure: !!adv,
      disclosureCollapsed: !!adv && !adv.open,
      hasToggle: !!none.querySelector("summary.form-adv-summary"),
      rawRowCount: none.querySelectorAll(".kv").length,
    };
  });
  cred.noneSummary ? ok("aws-creds: no signals renders the ambient-chain summary") : bad("aws-creds: no signals renders the ambient-chain summary", "wording missing");
  cred.keysSummary ? ok("aws-creds: static env keys take priority in the summary, reported as the ID that was probed") : bad("aws-creds: static env keys take priority in the summary, reported as the ID that was probed", "wording missing");
  cred.ecsSummary ? ok("aws-creds: ECS arm names the endpoint variable it saw") : bad("aws-creds: ECS arm names the endpoint variable it saw", "wording missing");
  cred.irsaSummary ? ok("aws-creds: a healthy EKS role reads as a role, never as no credentials") : bad("aws-creds: a healthy EKS role reads as a role, never as no credentials", "wording missing or a broken-shape sentence leaked");
  cred.irsaBrokenSummary ? ok("aws-creds: an unreadable IRSA token says so instead of claiming a role") : bad("aws-creds: an unreadable IRSA token says so instead of claiming a role", "the branch did not render");
  cred.irsaNoArnSummary ? ok("aws-creds: a token without AWS_ROLE_ARN names the missing half") : bad("aws-creds: a token without AWS_ROLE_ARN names the missing half", "the branch did not render");
  cred.sharedSummary ? ok("aws-creds: shared ~/.aws config/profile never reads as 'no credentials'") : bad("aws-creds: shared ~/.aws config/profile never reads as 'no credentials'", "wording missing/contradicts raw signal");
  cred.hasDisclosure ? ok("aws-creds: raw signals are folded behind a details disclosure") : bad("aws-creds: raw signals are folded behind a details disclosure", "no details.form-advanced");
  cred.disclosureCollapsed ? ok("aws-creds: raw-signals disclosure is collapsed by default") : bad("aws-creds: raw-signals disclosure is collapsed by default", "rendered open");
  cred.hasToggle ? ok("aws-creds: disclosure uses the app's toggle style") : bad("aws-creds: disclosure uses the app's toggle style", "missing summary.form-adv-summary");
  cred.rawRowCount === 4 ? ok("aws-creds: all four raw signal rows still render") : bad("aws-creds: all four raw signal rows still render", `rawRowCount=${cred.rawRowCount}`);


  // Scenario 10 — extension views (embedding builds). The stock binary ships no
  // ConsoleViewProvider, so this fixture-drives the pure client wiring (like
  // scenarios 7-9): inject an advertised view into capsCache, then assert the
  // nav item appears, the route "ext-<id>" mounts the module and calls its
  // render(mount, {apiBase, api}), and a server switch both drops the nav item
  // and abandons an in-flight render (serverGen staleness). Script() is stubbed
  // with a blob-URL ES module — a same-document dynamic import, which works
  // because the console's Content-Security-Policy allows script-src 'self'
  // blob: (securityHeaders, #848); real extension views are same-origin
  // ('self', ext.ConsoleViewProvider.Script), and blob: keeps this stub and
  // client-side-minted module URLs importable. Dropping blob: from script-src
  // breaks this scenario — that is the contract this test pins.
  const extv = await page.evaluate(async () => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const modURL = (marker) => URL.createObjectURL(new Blob(
      // The stub exercises the CONTRACT, not just its key names: it calls
      // ctx.ui.dateField and inspects what comes back. A presence check would
      // have passed with the binding moved to a sibling key, or with the
      // builder's parameters reordered — both of which hand an extension a
      // broken widget and no error. Only a real call can tell.
      [`export function render(mount, ctx){
         const f = ctx && ctx.ui && ctx.ui.dateField;
         const node = typeof f === "function" ? f("Since (UTC)", "since", "md", "YYYY-MM-DD HH:MM:SS") : null;
         window.${marker} = !!(mount && ctx && typeof ctx.api === "function" && ctx.apiBase);
         window.${marker}_ui = !!(node
           && node.classList.contains("field")
           && node.querySelector(".dt-trigger")
           && node.querySelector("input") && node.querySelector("input").name === "since"
           && node.querySelector(".field-label")
           && node.querySelector(".field-label").textContent.includes("Since (UTC)"));
         mount.append(document.createTextNode("ext-content"));
       }`],
      { type: "text/javascript" },
    ));

    // 0) Exercise gateCapabilities END-TO-END: the manual steps below fixture-drive
    //    capsCache/extViews directly, which BYPASSES the one real backend→frontend
    //    wiring line (extViews = capsCache.extension_views inside gateCapabilities).
    //    Stub /api/capabilities so it advertises an extension view, call the real
    //    gateCapabilities(), and assert extViews + the nav were populated FROM the
    //    parsed response — so a one-sided read of a different property (or a rename)
    //    fails a test instead of silently disabling the feature.
    const realFetch = window.fetch;
    window.fetch = (path, opts) => {
      if (typeof path === "string" && path.startsWith("/api/capabilities")) {
        // A realistic caps shape (incl. the always-present `auth` object) so the
        // whole gateCapabilities() tail — applyAuthGate/updateSrvNote — runs as it
        // would against the real backend, not just the extension_views read.
        return Promise.resolve(new Response(
          JSON.stringify({
            monitor: true,
            auth: { password_set: false, auth_kind: "token" },
            extension_views: [{ id: "gated", label: "Gated View", script: modURL("__extgate") }],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ));
      }
      return realFetch(path, opts);
    };
    await gateCapabilities();
    window.fetch = realFetch;
    const gatedFromCaps = extViews.length === 1 && extViews[0].id === "gated";
    const gatedNav = !!document.querySelector('.nav-item[data-route="ext-gated"]');

    // 1) Advertise one view and sync the nav (mirrors gateCapabilities).
    const script = modURL("__ext1");
    capsCache = { ...capsCache, extension_views: [{ id: "demo", label: "Demo View", script }] };
    extViews = capsCache.extension_views;
    window.__ext1 = undefined;
    window.__ext1_ui = undefined;
    syncExtNav();
    const navItem = document.querySelector('.nav-item[data-route="ext-demo"]');
    const navText = navItem ? navItem.textContent.trim() : null;
    const known = isKnownRoute("ext-demo");

    // 2) Navigate → the module mounts and render() runs with apiBase + api.
    navigate("ext-demo");
    await sleep(400);
    const onRoute = location.pathname === "/ext-demo";
    const mount = document.querySelector(".ext-view-mount");
    const mountedText = mount ? mount.textContent : "";
    // #1450: extension views are out of scope for the header Docs link, and a
    // view with no page must get NO link rather than one to the docs index.
    const docsLinkAbsent = !document.querySelector(".page-head .page-docs");
    const rendered = window.__ext1;
    const renderedUI = window.__ext1_ui;
    const navActive = navItem ? navItem.classList.contains("active") : false;

    // 2b) The OTHER extension surface. Settings panels take the same contract
    //     from the same builder, and until now nothing here drove them at all
    //     — which mattered because the settings surface is the one that was
    //     forgotten when the shared builder was introduced: the view's call
    //     was widened and the panel's was left hand-rolled. A guard comment
    //     claiming "the browser tier covers the shape" was true of the view
    //     and false of this, so the leg exists rather than the caveat.
    //
    //     Driven through renderExtensionSettings directly: the panel's nav
    //     lives under Settings and its routing is exercised elsewhere; what
    //     is unproven is the contract it hands its module.
    window.__extset = undefined;
    window.__extset_ui = undefined;
    await renderExtensionSettings({ id: "demoset", label: "Demo Panel", script: modURL("__extset") });
    await sleep(200);
    const setRendered = window.__extset;
    const setRenderedUI = window.__extset_ui;

    // 3) Server switch → the nav item is dropped and the now-unknown ext route
    //    redirects to overview (unmount across a switch).
    serverGen++;
    extViews = [];
    capsCache = { ...capsCache, extension_views: [] };
    syncExtNav();
    const navGone = !document.querySelector('.nav-item[data-route="ext-demo"]');
    const redirected = routeFromLocation() === "overview";

    // 4) Mid-import staleness: start a render, bump serverGen synchronously
    //    (before the dynamic import resolves), and assert render() never runs.
    extViews = [{ id: "demo2", label: "Demo Two", script: modURL("__ext2") }];
    window.__ext2 = undefined;
    const p = renderExtensionView(extViews[0]); // captures gen synchronously
    serverGen++;                                // switch before the import settles
    await p;
    const staleAborted = window.__ext2 === undefined;

    // Restore the console's REAL capabilities (the stock binary ships no provider,
    // so this clears the stubbed ext nav) and leave the UI on a known-good route
    // for the final no-errors assertion.
    await gateCapabilities();
    navigate("overview");
    return { gatedFromCaps, gatedNav, navText, known, onRoute, mounted: !!mount, mountedText, docsLinkAbsent, rendered, renderedUI, setRendered, setRenderedUI, navActive, navGone, redirected, staleAborted };
  });
  extv.gatedFromCaps ? ok("ext-view: gateCapabilities populates extViews from /api/capabilities.extension_views") : bad("ext-view: gateCapabilities populates extViews from /api/capabilities.extension_views", "extViews not set from the parsed caps response");
  extv.gatedNav ? ok("ext-view: gateCapabilities injects the nav item from the fetched caps") : bad("ext-view: gateCapabilities injects the nav item from the fetched caps", "no ext-gated nav item after gateCapabilities");
  extv.navText === "Demo View" ? ok("ext-view: nav item injected with the provider label") : bad("ext-view: nav item injected with the provider label", `navText=${extv.navText}`);
  extv.known ? ok("ext-view: isKnownRoute accepts a live ext-<id> route") : bad("ext-view: isKnownRoute accepts a live ext-<id> route", "ext-demo not known");
  extv.onRoute ? ok("ext-view: navigate routes to /ext-<id>") : bad("ext-view: navigate routes to /ext-<id>", "wrong route");
  extv.mounted ? ok("ext-view: a mount node is created") : bad("ext-view: a mount node is created", "no .ext-view-mount");
  extv.docsLinkAbsent ? ok("docs link: an extension view (no docs page) carries no header link") : bad("docs link: an extension view (no docs page) carries no header link", "a .page-docs link rendered on /ext-demo");
  extv.rendered ? ok("ext-view: the module render() runs with {apiBase, api, ui}") : bad("ext-view: the module render() runs with {apiBase, api, ui}", `rendered=${extv.rendered}`);
  // The widget half, asserted by USING it. The Go guards pin the binding's
  // spelling in app.js; only this can say the extension receives something
  // that builds the console's own date field — right wrapper, right label,
  // right input name, calendar attached.
  extv.renderedUI ? ok("ext-view: ctx.ui.dateField builds the console's own date field") : bad("ext-view: ctx.ui.dateField builds the console's own date field", `renderedUI=${extv.renderedUI}`);
  // Same two assertions for the settings surface, because "both surfaces get
  // the same contract" is the claim and one of them was already forgotten once.
  extv.setRendered ? ok("ext-settings: the panel module render() runs with {apiBase, api, ui}") : bad("ext-settings: the panel module render() runs with {apiBase, api, ui}", `setRendered=${extv.setRendered}`);
  extv.setRenderedUI ? ok("ext-settings: ctx.ui.dateField builds the console's own date field") : bad("ext-settings: ctx.ui.dateField builds the console's own date field", `setRenderedUI=${extv.setRenderedUI}`);
  /ext-content/.test(extv.mountedText) ? ok("ext-view: module content lands in the mount") : bad("ext-view: module content lands in the mount", `mountedText=${extv.mountedText}`);
  extv.navActive ? ok("ext-view: the nav item is marked active on its route") : bad("ext-view: the nav item is marked active on its route", "not .active");
  extv.navGone ? ok("ext-view: server switch removes the ext nav item (idempotent)") : bad("ext-view: server switch removes the ext nav item (idempotent)", "stale nav item remained");
  extv.redirected ? ok("ext-view: an ext route the new server lacks redirects to overview") : bad("ext-view: an ext route the new server lacks redirects to overview", "did not redirect");
  extv.staleAborted ? ok("ext-view: a server switch mid-import abandons the render (serverGen staleness)") : bad("ext-view: a server switch mid-import abandons the render (serverGen staleness)", "render ran after the switch");


  // Scenario 11 — CSV formula-injection guard (#965). The events CSV export
  // includes attacker-controlled values (pk_values/row_before/row_after from
  // the monitored DB); a leading =, +, -, @, tab or CR must be neutralized
  // with a leading quote or Excel/Sheets executes it on open. Calls the REAL
  // embedded csvCell — deleting the guard line in app.js turns this red.
  const csv = await page.evaluate(() => ({
    formula: csvCell("=HYPERLINK(\"http://evil.example\",\"click\")"),
    plus: csvCell("+1"),
    minus: csvCell("-2"),
    at: csvCell("@cmd"),
    tab: csvCell("\tx"),
    cr: csvCell("\rx"),
    plain: csvCell("hello"),
    number: csvCell(42),
    quoted: csvCell('a,"b'),
    objFormula: csvCell({ id: 1 }), // JSON starts with { — must NOT be prefixed
  }));
  csv.formula === '"\'=HYPERLINK(""http://evil.example"",""click"")"' ? ok("csv: leading = is quote-prefixed (and CSV-quoted)") : bad("csv: leading = is quote-prefixed (and CSV-quoted)", csv.formula);
  csv.plus === "'+1" && csv.minus === "'-2" && csv.at === "'@cmd" ? ok("csv: leading +/-/@ are quote-prefixed") : bad("csv: leading +/-/@ are quote-prefixed", JSON.stringify([csv.plus, csv.minus, csv.at]));
  csv.tab === "'\tx" ? ok("csv: leading tab is quote-prefixed") : bad("csv: leading tab is quote-prefixed", JSON.stringify(csv.tab));
  csv.cr === '"\'\rx"' ? ok("csv: leading CR is quote-prefixed and still CSV-quoted") : bad("csv: leading CR is quote-prefixed and still CSV-quoted", JSON.stringify(csv.cr));
  csv.plain === "hello" && csv.number === "42" ? ok("csv: benign values pass through untouched") : bad("csv: benign values pass through untouched", JSON.stringify([csv.plain, csv.number]));
  csv.quoted === '"a,""b"' ? ok("csv: existing quoting logic unchanged") : bad("csv: existing quoting logic unchanged", JSON.stringify(csv.quoted));
  csv.objFormula === '"{""id"":1}"' ? ok("csv: JSON object cells are not prefixed") : bad("csv: JSON object cells are not prefixed", JSON.stringify(csv.objFormula));

  // ── Scenarios 12-15: the primary READ workflows over REAL indexed data ─────
  // (#970/#686/#619). Everything below runs against the "byo-idx" server from
  // scenario 5 — its index is the boot db run.sh provisioned with `bintrail
  // init`, seeded events, and a real `bintrail baseline` snapshot (which the
  // daemon-level --baseline-dir exposes to every server, making this one
  // Time-travel-capable).
  await page.evaluate(async (id) => { await switchServer(id); }, byoId);

  // Scenario 11ap — Access profiles (#1445): the flags/profiles/rules that a
  // data profile enforces, authored through the REAL forms against byo-idx
  // (its index was provisioned by `bintrail init`, so the three RBAC tables
  // exist). Adds one column flag, one profile and one deny rule, reads the
  // rows the page repaints from the server's document, then removes all
  // three through their buttons (each asks first; the dialog is accepted
  // here) so nothing is left behind for the scenarios after it. The page is
  // NOT monitor-gated: it must be reachable through the sidebar, not only
  // by navigate().
  const acceptDialog = (d) => d.accept();
  page.on("dialog", acceptDialog);
  try {
    const apNav = await page.evaluate(() => {
      const a = document.querySelector('.nav-item[data-route="access-profiles"]');
      if (!a) return { present: false };
      const cs = getComputedStyle(a);
      a.click();
      return { present: true, visible: cs.display !== "none" && cs.visibility !== "hidden" };
    });
    (apNav.present && apNav.visible)
      ? ok("access profiles: sidebar entry present and visible")
      : bad("access profiles: sidebar entry present and visible", JSON.stringify(apNav));
    await page.waitForSelector("#ap-flags", { timeout: 10000 });
    const apEmpty = await page.evaluate(() => ({
      flags: document.querySelectorAll("#ap-flags .ap-row").length,
      profiles: document.querySelectorAll("#ap-profiles .ap-row").length,
      rules: document.querySelectorAll("#ap-rules .ap-row").length,
      ruleAddDisabled: !!document.querySelector('#ap-rule-form button[type="submit"]').disabled,
    }));
    (apEmpty.flags === 0 && apEmpty.profiles === 0 && apEmpty.rules === 0 && apEmpty.ruleAddDisabled)
      ? ok("access profiles: fresh index renders three empty panels, Add rule disabled until a profile exists")
      : bad("access profiles: fresh index renders three empty panels, Add rule disabled until a profile exists", JSON.stringify(apEmpty));

    await page.fill('#ap-flag-form input[name="flag"]', "pii");
    await page.fill('#ap-flag-form input[name="schema"]', FIX);
    await page.fill('#ap-flag-form input[name="table"]', "customers");
    await page.fill('#ap-flag-form input[name="column"]', "email");
    await page.click('#ap-flag-form button[type="submit"]');
    await page.waitForSelector("#ap-flags .ap-row", { timeout: 10000 });
    await page.fill('#ap-profile-form input[name="name"]', "e2e-marketing");
    await page.fill('#ap-profile-form input[name="description"]', "E2E profile");
    await page.click('#ap-profile-form button[type="submit"]');
    await page.waitForSelector("#ap-profiles .ap-row", { timeout: 10000 });
    await page.selectOption('#ap-rule-form select[name="profile"]', "e2e-marketing");
    await page.fill('#ap-rule-form input[name="flag"]', "pii");
    await page.selectOption('#ap-rule-form select[name="permission"]', "deny");
    await page.click('#ap-rule-form button[type="submit"]');
    await page.waitForSelector("#ap-rules .ap-row", { timeout: 10000 });
    const apRows = await page.evaluate(async (FIX) => {
      const text = (sel) => Array.from(document.querySelectorAll(sel)).map((n) => n.textContent.replace(/\s+/g, " ").trim());
      const doc = await api("/api/access-profiles");
      return {
        flagRows: text("#ap-flags .ap-row"),
        profileRows: text("#ap-profiles .ap-row"),
        ruleRows: text("#ap-rules .ap-row"),
        wire: doc,
        flagOK: doc.flags.length === 1 && doc.flags[0].schema === FIX && doc.flags[0].table === "customers" && doc.flags[0].column === "email" && doc.flags[0].flag === "pii",
        ruleOK: doc.rules.length === 1 && doc.rules[0].profile === "e2e-marketing" && doc.rules[0].flag === "pii" && doc.rules[0].permission === "deny",
      };
    }, FIX);
    (apRows.flagOK && apRows.ruleOK && apRows.wire.profiles.length === 1)
      ? ok("access profiles: flag, profile and deny rule authored through the forms land on the index")
      : bad("access profiles: flag, profile and deny rule authored through the forms land on the index", JSON.stringify(apRows.wire));
    (apRows.flagRows.length === 1 && /customers/.test(apRows.flagRows[0]) && /email/.test(apRows.flagRows[0]) && /pii/.test(apRows.flagRows[0])
      && apRows.profileRows.length === 1 && /e2e-marketing/.test(apRows.profileRows[0]) && /1 rule/.test(apRows.profileRows[0])
      && apRows.ruleRows.length === 1 && /may not see/.test(apRows.ruleRows[0]) && /DENY/.test(apRows.ruleRows[0]))
      ? ok("access profiles: the rows say what was authored (table, column, flag; rule count; may not see + DENY)")
      : bad("access profiles: the rows say what was authored (table, column, flag; rule count; may not see + DENY)", JSON.stringify([apRows.flagRows, apRows.profileRows, apRows.ruleRows]));

    // A refusal shows the server's own words (the shared package's, the
    // words the command line refuses with) and writes nothing.
    await page.fill('#ap-flag-form input[name="flag"]', "orphan");
    await page.click('#ap-flag-form button[type="submit"]');
    await page.waitForFunction(() => !document.getElementById("toast-error").hidden);
    const apRefusal = await page.evaluate(async () => ({
      toast: document.getElementById("toast-error").textContent,
      flags: (await api("/api/access-profiles")).flags.length,
    }));
    (/schema is required/.test(apRefusal.toast) && apRefusal.flags === 1)
      ? ok("access profiles: a refused add shows the shared message and writes nothing")
      : bad("access profiles: a refused add shows the shared message and writes nothing", JSON.stringify(apRefusal));

    // Tear down through the buttons: the rule, the profile, then the flag.
    await page.click("#ap-rules .ap-row button");
    await page.waitForFunction(() => document.querySelectorAll("#ap-rules .ap-row").length === 0);
    await page.click("#ap-profiles .ap-row button");
    await page.waitForFunction(() => document.querySelectorAll("#ap-profiles .ap-row").length === 0);
    await page.click("#ap-flags .ap-row button");
    await page.waitForFunction(() => document.querySelectorAll("#ap-flags .ap-row").length === 0);
    const apAfter = await page.evaluate(() => api("/api/access-profiles"));
    (apAfter.flags.length === 0 && apAfter.profiles.length === 0 && apAfter.rules.length === 0)
      ? ok("access profiles: the Remove buttons take the rule, the profile and the flag back off the index")
      : bad("access profiles: the Remove buttons take the rule, the profile and the flag back off the index", JSON.stringify(apAfter));
  } finally {
    page.off("dialog", acceptDialog);
  }

  // Scenario 12 — Events renders real rows, and the redaction contract holds
  // end-to-end (#970): query_text/query_hash are captured index data that must
  // NEVER reach the browser (#699 — the canary is IN the seeded UPDATE row),
  // while connection_id must PASS THROUGH (#701 D1 un-gated it; asserting
  // absence here would pin the stale pre-#701 contract).
  await page.evaluate(() => navigate("events"));
  await page.waitForSelector("#ev-rows .ev-row", { timeout: 10000 });
  const evr = await page.evaluate(({ FIX, CANARY }) => {
    const rows = Array.from(document.querySelectorAll("#ev-rows .ev-row"));
    const updRow = rows.find((r) => r.textContent.includes(FIX + ".orders") && r.textContent.includes("UPDATE"));
    let diffText = "";
    if (updRow) {
      updRow.click(); // expands and lazy-renders the before/after diff
      diffText = updRow.nextElementSibling ? updRow.nextElementSibling.textContent : "";
    }
    const upd = lastEvents.find((e) => e.event_type === "UPDATE" && e.schema_name === FIX && e.table_name === "orders") || {};
    return {
      rowCount: rows.length,
      updFound: !!updRow,
      diffText,
      domCanary: document.body.textContent.includes(CANARY),
      leakedKeys: lastEvents.filter((e) => ("query_text" in e) || ("query_hash" in e)).length,
      connId: upd.connection_id,
    };
  }, { FIX, CANARY });
  evr.rowCount === 6 ? ok("events: all 6 seeded events render") : bad("events: all 6 seeded events render", `rowCount=${evr.rowCount}`);
  evr.updFound ? ok("events: the seeded orders UPDATE row renders") : bad("events: the seeded orders UPDATE row renders", "row not found");
  (/shipped/.test(evr.diffText) && /new/.test(evr.diffText))
    ? ok("events: expanding a row shows the before/after diff")
    : bad("events: expanding a row shows the before/after diff", `diffText=${evr.diffText.slice(0, 120)}`);
  !evr.domCanary ? ok("events: query_text never reaches the DOM") : bad("events: query_text never reaches the DOM", "canary found in page text");
  evr.leakedKeys === 0 ? ok("events: no query_text/query_hash keys in the wire events") : bad("events: no query_text/query_hash keys in the wire events", `${evr.leakedKeys} event(s) carry them`);
  evr.connId === 777 ? ok("events: connection_id passes through (#701 D1)") : bad("events: connection_id passes through (#701 D1)", `connection_id=${JSON.stringify(evr.connId)}`);

  // Scenario 12tz — timezone discipline on Events (#1354): the column header
  // declares the zone once, and each row keeps the EXACT wire text — so it
  // copy-pastes into Since/Until/At unchanged — with the viewer's local
  // equivalent as a hover tooltip. Never an unlabeled browser-local render.
  const evtz = await page.evaluate(() => {
    const head = document.querySelector(".ev-head span");
    const t = document.querySelector("#ev-rows .ev-time");
    return {
      headCol: head ? head.textContent : "",
      text: t ? t.textContent : "",
      title: t ? t.getAttribute("title") || "" : "",
    };
  });
  evtz.headCol === "time (UTC)"
    ? ok("events: the time column declares UTC")
    : bad("events: the time column declares UTC", JSON.stringify(evtz));
  (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(evtz.text) && evtz.title.startsWith("UTC; in your local time:"))
    ? ok("events: rows keep exact UTC text with a local-time tooltip")
    : bad("events: rows keep exact UTC text with a local-time tooltip", JSON.stringify(evtz));

  // Scenario 12b — the export path (#970): drive the REAL JSON/CSV buttons and
  // capture the blobs downloadBlob mints. Export is over the on-screen
  // redacted DTOs — so it must stay query_text-free WITH connection_id, and
  // the CSV header must stay in lockstep with EVENT_CSV_COLUMNS.
  const exp = await page.evaluate(async (CANARY) => {
    const btns = Array.from(document.querySelectorAll(".result-bar button"));
    const jsonBtn = btns.find((b) => b.textContent.trim() === "Export JSON");
    const csvBtn = btns.find((b) => b.textContent.trim() === "Export CSV");
    const blobs = [];
    const orig = URL.createObjectURL;
    URL.createObjectURL = (b) => { blobs.push(b); return orig.call(URL, b); };
    jsonBtn.click();
    csvBtn.click();
    // Export re-fetches the whole filtered set (#1297), so the blob appears a
    // round-trip after the click — poll instead of reading it synchronously.
    for (let i = 0; i < 100 && blobs.length < 2; i++) await new Promise((r) => setTimeout(r, 50));
    URL.createObjectURL = orig;
    const [jsonText, csvText] = await Promise.all(blobs.map((b) => b.text()));
    const arr = JSON.parse(jsonText);
    const header = csvText.split("\r\n")[0];
    return {
      n: arr.length,
      jsonLeak: arr.filter((e) => ("query_text" in e) || ("query_hash" in e)).length,
      jsonConn: arr.every((e) => "connection_id" in e),
      jsonCanary: jsonText.includes(CANARY),
      headerLockstep: header === EVENT_CSV_COLUMNS.join(","),
      headerConn: header.split(",").includes("connection_id"),
      headerLeak: /query_text|query_hash/.test(header),
      csvCanary: csvText.includes(CANARY),
      csvConnVal: csvText.split("\r\n").some((l) => l.split(",").includes("777")),
    };
  }, CANARY);
  exp.n === 6 ? ok("export: JSON blob carries every match of the search") : bad("export: JSON blob carries every match of the search", `n=${exp.n}`);
  (exp.jsonLeak === 0 && !exp.jsonCanary) ? ok("export: JSON blob is query_text/query_hash-free") : bad("export: JSON blob is query_text/query_hash-free", `leak=${exp.jsonLeak} canary=${exp.jsonCanary}`);
  exp.jsonConn ? ok("export: JSON blob keeps connection_id") : bad("export: JSON blob keeps connection_id", "some entry lacks the key");
  exp.headerLockstep ? ok("export: CSV header in lockstep with EVENT_CSV_COLUMNS") : bad("export: CSV header in lockstep with EVENT_CSV_COLUMNS", "header drifted");
  (exp.headerConn && !exp.headerLeak) ? ok("export: CSV columns include connection_id, never query_text") : bad("export: CSV columns include connection_id, never query_text", `conn=${exp.headerConn} leak=${exp.headerLeak}`);
  (!exp.csvCanary && exp.csvConnVal) ? ok("export: CSV rows redacted but carry the connection_id value") : bad("export: CSV rows redacted but carry the connection_id value", `canary=${exp.csvCanary} conn777=${exp.csvConnVal}`);

  // Scenario 12p — progressive Events (#1414): a scope=live phase paints
  // first with a LOUD partial marker, the merged phase replaces it wholesale
  // and clears the marker, and a failed phase 2 leaves the marker up naming
  // the failure. Driven by stubbing `api` with a gated phase-2 promise so the
  // ordering is deterministic — the claim is about what the DOM says BETWEEN
  // the phases, which no server fixture can hold still long enough to read.
  const progressiveRead = await page.evaluate(async () => {
    const real = api;
    const out = { calls: [] };
    let releasePhase2;
    const phase2Gate = new Promise((r) => { releasePhase2 = r; });
    const liveEvent = { event_timestamp: "2026-08-21 10:00:00", schema_name: "e2eshop", table_name: "orders",
      event_type: "UPDATE", pk_values: "1", changed_columns: ["status"], row_before: { status: "a" },
      row_after: { status: "b" }, anchor: "2026-08-21T10:00:00Z|11" };
    const archEvent = { event_timestamp: "2026-08-20 09:00:00", schema_name: "e2eshop", table_name: "orders",
      event_type: "DELETE", pk_values: "2", changed_columns: [], row_before: { status: "z" },
      row_after: null, anchor: "2026-08-20T09:00:00Z|7" };
    api = async (path, opts) => {
      if (path.startsWith("/api/events?")) {
        out.calls.push(path);
        if (path.includes("scope=live")) {
          return { events: [liveEvent], count: 1, limit: 100, has_more: false,
                   scope: "live", archives_pending: true,
                   warnings: ["Live-index scope (scope=live): 1 registered archive source(s) were NOT read. This list is PARTIAL wherever the window reaches into archived history; a full read (without scope=live) completes it."] };
        }
        await phase2Gate;
        return { events: [liveEvent, archEvent], count: 2, limit: 100, has_more: false };
      }
      return real(path, opts);
    };
    const sample = () => ({
      rows: document.querySelectorAll("#ev-rows .ev-row").length,
      warn: (document.getElementById("ev-warnings") || {}).textContent || "",
    });
    try {
      navigate("events", { q: "" });
      await new Promise((r) => setTimeout(r, 50));
      const form = document.getElementById("ev-form");
      await runEventsQuery(form);
      out.phase1 = sample();
      releasePhase2();
      await new Promise((r) => setTimeout(r, 50));
      out.phase2 = sample();

      // The failure leg: phase 2 rejects, the marker must STAY and say so.
      api = async (path, opts) => {
        if (path.startsWith("/api/events?")) {
          if (path.includes("scope=live")) {
            return { events: [liveEvent], count: 1, limit: 100, has_more: false,
                     scope: "live", archives_pending: true,
                     warnings: ["Live-index scope (scope=live): 1 registered archive source(s) were NOT read. This list is PARTIAL wherever the window reaches into archived history; a full read (without scope=live) completes it."] };
          }
          throw new Error("simulated archive outage");
        }
        return real(path, opts);
      };
      await runEventsQuery(form);
      await new Promise((r) => setTimeout(r, 50));
      out.failed = sample();
    } finally {
      api = real;
    }
    // Hand the view back to the real server for the scenarios below.
    navigate("events", { q: "" });
    await new Promise((r) => setTimeout(r, 150));
    return out;
  });
  (progressiveRead.phase1 && progressiveRead.phase1.rows === 1 &&
    /PARTIAL/.test(progressiveRead.phase1.warn) && /background/.test(progressiveRead.phase1.warn))
    ? ok("progressive events: the live phase paints first, loudly marked partial and in-progress")
    : bad("progressive events: the live phase paints first, loudly marked partial and in-progress",
        JSON.stringify(progressiveRead.phase1) + " calls=" + JSON.stringify(progressiveRead.calls));
  (progressiveRead.phase2 && progressiveRead.phase2.rows === 2 && !/PARTIAL|background/.test(progressiveRead.phase2.warn))
    ? ok("progressive events: the merged phase completes the list and clears the marker — only then")
    : bad("progressive events: the merged phase completes the list and clears the marker — only then",
        JSON.stringify(progressiveRead.phase2));
  (progressiveRead.failed && progressiveRead.failed.rows === 1 && /FAILED/.test(progressiveRead.failed.warn) &&
    /live-only/.test(progressiveRead.failed.warn))
    ? ok("progressive events: a failed archive read leaves the marker up and names the failure")
    : bad("progressive events: a failed archive read leaves the marker up and names the failure",
        JSON.stringify(progressiveRead.failed));
  (progressiveRead.calls.length >= 2 && progressiveRead.calls[0].includes("scope=live") &&
    !progressiveRead.calls[1].includes("scope=live"))
    ? ok("progressive events: phase 1 asks scope=live, phase 2 asks the full read")
    : bad("progressive events: phase 1 asks scope=live, phase 2 asks the full read",
        JSON.stringify(progressiveRead.calls));

  // Scenario 12b — Events keyset paging (#1297). The view used to render one
  // window and stop: event 101 was unreachable except by inventing a filter,
  // and the header ("100 event(s) in the newest 100 events") restated the limit
  // instead of saying whether anything was behind it. The fixture seeds 6
  // events, so a page size of 5 splits them 5 + 1 — enough to prove the cursor
  // hands off without repeating or skipping a row, which is the only thing a
  // paging bug ever does.
  const pg1 = await page.evaluate(async () => {
    const f = document.getElementById("ev-form");
    f.elements.limit.value = "5";
    f.requestSubmit();
    // Wait for the re-render to settle on the smaller page.
    for (let i = 0; i < 60; i++) {
      await new Promise((r) => setTimeout(r, 50));
      if (document.querySelectorAll("#ev-rows .ev-row").length === 5) break;
    }
    return {
      rows: document.querySelectorAll("#ev-rows .ev-row").length,
      ids: lastEvents.map((e) => e.event_id),
      note: ($("#ev-count-note", VIEW()) || {}).textContent || "",
      prevDisabled: document.getElementById("ev-prev").disabled,
      nextDisabled: document.getElementById("ev-next").disabled,
    };
  });
  pg1.rows === 5 ? ok("events paging: a page-size of 5 renders 5 of the 6 seeded events") : bad("events paging: a page-size of 5 renders 5 of the 6 seeded events", `rows=${pg1.rows}`);
  pg1.prevDisabled ? ok("events paging: Newer is disabled on the first page") : bad("events paging: Newer is disabled on the first page", "enabled");
  !pg1.nextDisabled ? ok("events paging: Older is enabled while events remain") : bad("events paging: Older is enabled while events remain", "disabled — event 6 is unreachable");
  /showing 1–5 of more/.test(pg1.note)
    ? ok("events paging: the header states a position, not the limit restated")
    : bad("events paging: the header states a position, not the limit restated", `note=${pg1.note}`);
  !/in the newest 5 events/.test(pg1.note)
    ? ok("events paging: the circular 'in the newest N' phrasing is gone")
    : bad("events paging: the circular 'in the newest N' phrasing is gone", `note=${pg1.note}`);

  const pg2 = await page.evaluate(async () => {
    document.getElementById("ev-next").click();
    for (let i = 0; i < 60; i++) {
      await new Promise((r) => setTimeout(r, 50));
      if (document.querySelectorAll("#ev-rows .ev-row").length === 1) break;
    }
    return {
      rows: document.querySelectorAll("#ev-rows .ev-row").length,
      ids: lastEvents.map((e) => e.event_id),
      note: ($("#ev-count-note", VIEW()) || {}).textContent || "",
      prevDisabled: document.getElementById("ev-prev").disabled,
      nextDisabled: document.getElementById("ev-next").disabled,
    };
  });
  pg2.rows === 1 ? ok("events paging: Older reaches the 6th event") : bad("events paging: Older reaches the 6th event", `rows=${pg2.rows}`);
  pg2.ids.every((id) => !pg1.ids.includes(id))
    ? ok("events paging: page 2 repeats no event from page 1 (keyset cut is exclusive)")
    : bad("events paging: page 2 repeats no event from page 1 (keyset cut is exclusive)", `p1=${pg1.ids} p2=${pg2.ids}`);
  pg1.ids.length + pg2.ids.length === 6
    ? ok("events paging: the two pages together cover all 6 events (nothing skipped)")
    : bad("events paging: the two pages together cover all 6 events (nothing skipped)", `p1=${pg1.ids} p2=${pg2.ids}`);
  pg2.nextDisabled ? ok("events paging: Older is disabled at the end of the stream") : bad("events paging: Older is disabled at the end of the stream", "still enabled past the last event");
  !pg2.prevDisabled ? ok("events paging: Newer is enabled off the first page") : bad("events paging: Newer is enabled off the first page", "disabled — the operator cannot get back");
  /showing 6–6 of 6 \(end\)/.test(pg2.note)
    ? ok("events paging: the last page reports the exact total it walked")
    : bad("events paging: the last page reports the exact total it walked", `note=${pg2.note}`);

  // The export decision (#1297): the buttons export every match of the SEARCH,
  // not the page on screen. Paged to page 2 (1 row rendered), an export must
  // still carry all 6 — silently handing an operator a sixth of their evidence
  // would be worse than the un-paged behavior this replaces.
  const expAll = await page.evaluate(async () => {
    const btns = Array.from(document.querySelectorAll(".result-bar button"));
    const jsonBtn = btns.find((b) => b.textContent.trim() === "Export JSON");
    const blobs = [];
    const orig = URL.createObjectURL;
    URL.createObjectURL = (b) => { blobs.push(b); return orig.call(URL, b); };
    jsonBtn.click();
    for (let i = 0; i < 100 && blobs.length < 1; i++) await new Promise((r) => setTimeout(r, 50));
    URL.createObjectURL = orig;
    return {
      n: blobs.length ? JSON.parse(await blobs[0].text()).length : -1,
      rendered: document.querySelectorAll("#ev-rows .ev-row").length,
      label: jsonBtn.textContent.trim(),
      title: jsonBtn.title,
    };
  });
  expAll.n === 6 && expAll.rendered === 1
    ? ok("export: exports every match of the search, not the 1 row on screen")
    : bad("export: exports every match of the search, not the 1 row on screen", `n=${expAll.n} rendered=${expAll.rendered}`);
  (expAll.label === "Export JSON" && /all matches/i.test(expAll.title))
    ? ok("export: the button states its scope in the UI")
    : bad("export: the button states its scope in the UI", `label=${expAll.label} title=${expAll.title}`);

  // A filter edit must DROP the cursor: a cursor names a row that need not
  // exist in the next search, so keeping it would serve a page from the middle
  // of a search the operator never ran.
  const reset = await page.evaluate(async () => {
    const f = document.getElementById("ev-form");
    f.elements.limit.value = "";
    f.requestSubmit();
    for (let i = 0; i < 60; i++) {
      await new Promise((r) => setTimeout(r, 50));
      if (document.querySelectorAll("#ev-rows .ev-row").length === 6) break;
    }
    return {
      rows: document.querySelectorAll("#ev-rows .ev-row").length,
      prevDisabled: document.getElementById("ev-prev").disabled,
    };
  });
  (reset.rows === 6 && reset.prevDisabled)
    ? ok("events paging: a filter edit resets to page 1")
    : bad("events paging: a filter edit resets to page 1", `rows=${reset.rows} prevDisabled=${reset.prevDisabled}`);

  // Scenario 13 — Recover actually SUBMITS and renders the reversal SQL
  // (#970). Scenario 6 above only checks the form's DOM exists.
  await page.evaluate(() => navigate("recover"));
  await page.waitForSelector("#recover-form", { timeout: 8000 });
  await page.waitForFunction((FIX) => {
    const f = document.getElementById("recover-form");
    return f && Array.from(f.elements.schema.options).some((o) => o.value === FIX);
  }, FIX, { timeout: 8000 });
  await page.evaluate((FIX) => {
    const f = document.getElementById("recover-form");
    f.elements.schema.value = FIX;
    f.elements.table.value = "orders";
    f.elements.pk.value = "1";
    f.requestSubmit();
  }, FIX);
  await page.waitForSelector("#recover-out #sql-panel", { timeout: 8000 });
  const rec = await page.evaluate(() => ({
    sql: (document.querySelector("#recover-out #sql-panel .code") || {}).textContent || "",
    meta: (document.querySelector("#recover-out #sql-panel .lbl") || {}).textContent || "",
    cascadeBanner: !!document.querySelector("#recover-out .ctx-banner"),
  }));
  (/UPDATE/.test(rec.sql) && rec.sql.includes("'new'"))
    ? ok("recover: submit renders the reversal SQL")
    : bad("recover: submit renders the reversal SQL", `sql=${rec.sql.slice(0, 160)}`);
  /1 statement\(s\) from 1 event\(s\)/.test(rec.meta)
    ? ok("recover: meta line reports statement/event counts")
    : bad("recover: meta line reports statement/event counts", rec.meta);
  !rec.cascadeBanner ? ok("recover: a plain undo shows no CASCADE banner") : bad("recover: a plain undo shows no CASCADE banner", "banner rendered for a non-parent target");

  // Scenario 13b — the cascade-detected banner (#619): the seeded parent DELETE
  // has two child INSERTs behind an ON DELETE CASCADE fk_constraints row, so
  // /api/recover auto-detects and the positive-half rendering (banner + counts
  // meta) must run. During #617 a missing ')' in this exact block broke the
  // whole SPA and only a manual boot probe caught it — this is that guard.
  await page.evaluate(() => {
    const f = document.getElementById("recover-form");
    f.elements.table.value = "parent";
    f.elements.pk.value = "";
    f.requestSubmit();
  });
  await page.waitForFunction(() => {
    const b = document.querySelector("#recover-out .ctx-banner .badge");
    return b && b.textContent === "CASCADE";
  });
  const cas = await page.evaluate(() => ({
    banner: (document.querySelector("#recover-out .ctx-banner") || {}).textContent || "",
    sql: (document.querySelector("#recover-out #sql-panel .code") || {}).textContent || "",
    meta: (document.querySelector("#recover-out #sql-panel .lbl") || {}).textContent || "",
  }));
  cas.banner.includes("restores 2 related row(s)")
    ? ok("cascade: banner counts the repaired child rows")
    : bad("cascade: banner counts the repaired child rows", cas.banner);
  (cas.meta.includes("2 cascade child row(s)") && cas.meta.includes("0 SET NULL restore(s)"))
    ? ok("cascade: meta line carries the cascade-aware counts")
    : bad("cascade: meta line carries the cascade-aware counts", cas.meta);
  (/INSERT INTO/.test(cas.sql) && cas.sql.includes("child") && cas.sql.includes("10") && cas.sql.includes("11"))
    ? ok("cascade: script re-inserts both cascade-deleted children")
    : bad("cascade: script re-inserts both cascade-deleted children", cas.sql.slice(0, 200));

  // Scenario 14 — the row-state half over the fixture baseline (#970), now
  // inside Restore (#1298). Pin the merge itself first: /timetravel must fold
  // into /recover (old bookmarks and Back entries), and a server WITHOUT a
  // baseline must still render the panel with an explanation instead of
  // silently dropping it — an operator who cannot find it has no way to learn
  // that a baseline is what is missing.
  const ttGate = await page.evaluate(() => {
    const had = !!capsCache.reconstruct;
    navigate("timetravel");
    const merged = location.pathname === "/recover";
    capsCache.reconstruct = false;
    navigate("recover");
    const sec = document.querySelector(".state-section");
    const explains = !!sec && !document.querySelector('[name="state_at"]') && !!sec.querySelector(".state-note");
    capsCache.reconstruct = had;
    return { had, merged, explains };
  });
  ttGate.had ? ok("restore: a baseline-configured server reports the reconstruct capability") : bad("restore: a baseline-configured server reports the reconstruct capability", "capsCache.reconstruct falsy on byo-idx");
  ttGate.merged ? ok("restore: /timetravel folds into /recover") : bad("restore: /timetravel folds into /recover", "did not land on /recover");
  ttGate.explains ? ok("restore: no baseline explains itself instead of vanishing") : bad("restore: no baseline explains itself instead of vanishing", "panel absent or still offering controls");

  await page.evaluate(() => navigate("recover"));
  await page.waitForSelector("#recover-form", { timeout: 8000 });
  await page.waitForFunction((FIX) => {
    const f = document.getElementById("recover-form");
    return f && Array.from(f.elements.schema.options).some((o) => o.value === FIX);
  }, FIX, { timeout: 8000 });
  // pk=1: baseline row (status new) + a later UPDATE (status shipped) — the
  // event fold half. email only ever existed in the baseline's full row image.
  // The target is entered ONCE, on the undo form — that single target driving
  // both halves is the merge (#1298).
  await page.evaluate(async ({ FIX, TT_AT }) => {
    const f = document.getElementById("recover-form");
    f.elements.schema.value = FIX;
    await loadTables(f);
    f.elements.table.value = "orders";
    f.elements.pk.value = "1";
    const at = document.querySelector('[name="state_at"]');
    if (TT_AT && at) at.value = TT_AT;
    await runState(f, false);
  }, { FIX, TT_AT });
  await page.waitForSelector("#modal .state-modal .statetable", { timeout: 10000 });
  const tt1 = await page.evaluate(() => {
    const cells = {};
    document.querySelectorAll("#modal .state-modal .statetable tr").forEach((tr) => {
      cells[tr.querySelector("th").textContent] = tr.querySelector("td").textContent;
    });
    return { cells, meta: (document.querySelector("#modal .state-modal .meta-line") || {}).textContent || "" };
  });
  (tt1.cells.status === "shipped" && tt1.cells.email === "a@example.com")
    ? ok("restore: reconstructed row folds the event over the baseline")
    : bad("restore: reconstructed row folds the event over the baseline", JSON.stringify(tt1.cells));
  /backup /.test(tt1.meta) ? ok("restore: meta line names the backup anchor") : bad("restore: meta line names the backup anchor", tt1.meta);

  // pk=4: exists ONLY in the baseline (no events) — a binlog-only reconstruct
  // cannot resolve it, so this pins the baseline half of baseline+deltas.
  await page.evaluate(async () => { const f = document.getElementById("recover-form"); f.elements.pk.value = "4"; await runState(f, false); });
  await page.waitForFunction(() => /d@example\.com/.test((document.querySelector("#modal .state-modal") || {}).textContent || ""));
  const tt4 = await page.evaluate(() => {
    const cells = {};
    document.querySelectorAll("#modal .state-modal .statetable tr").forEach((tr) => {
      cells[tr.querySelector("th").textContent] = tr.querySelector("td").textContent;
    });
    return cells;
  });
  (tt4.status === "new" && tt4.email === "d@example.com")
    ? ok("restore: a never-touched row resolves from the baseline alone")
    : bad("restore: a never-touched row resolves from the baseline alone", JSON.stringify(tt4));

  // pk=2: in the baseline, then DELETEd — must render the deleted note, not an
  // empty table and not the stale baseline value.
  await page.evaluate(async () => { const f = document.getElementById("recover-form"); f.elements.pk.value = "2"; await runState(f, false); });
  await page.waitForFunction(() => !!document.querySelector("#modal .state-modal .deleted-note"));
  const tt2 = await page.evaluate(() => (document.querySelector("#modal .state-modal .deleted-note") || {}).textContent || "");
  /Row was deleted/.test(tt2)
    ? ok("restore: a deleted row renders the deleted note")
    : bad("restore: a deleted row renders the deleted note", tt2);

  // The bridge that makes the two halves one errand: the state on screen
  // becomes the undo window that produces it. since = at + 1s reverses
  // precisely the events AFTER `at`, because reconstruct applies <= at while
  // recover reverses >= since. Passing `at` itself would be off by every event
  // sharing that second — these indexes routinely carry dozens.
  const bridge = await page.evaluate(async () => {
    const f = document.getElementById("recover-form");
    f.elements.pk.value = "1";
    await runState(f, false);
    const btn = document.querySelector(".state-actions .btn-primary");
    if (!btn) return { err: "no Restore-to-this-state button on a found row" };
    const at = (document.querySelector("#modal .state-modal .meta-line") || {}).textContent || "";
    // #1405: the result is in the dialog and NOT also inline. Retargeting the
    // selectors above would pass just as well if it rendered in both places.
    const inline = !!document.querySelector("#state-out .statetable");
    // #1404: arrive carrying the Undo bridge's per-row cap, which is what an
    // operator who used Undo first would have in the field.
    f.elements.limit_per_pk.value = "1";
    // #1411: …and its event anchor, which is the stronger half of the same
    // hazard. The cap reverses the newest change after the instant; a leftover
    // anchor reverses ONE event chosen minutes ago in Events and nothing else,
    // under a button naming the state as of `at`. Set to a well-formed token so
    // a clear is the only thing that can empty it.
    f.elements.event.value = "2026-08-21T20:08:36Z|403440";
    // #1405: previewRecover is stubbed for the duration of the click, and that
    // is the whole assertion rather than a convenience. aimUndoAtInstant calls
    // it synchronously, previewRecover opens the busy dialog, and openBusyModal
    // clears the shared #modal mount — so the state dialog disappears whether
    // or not restoreToStateAction ever calls onDone. Review showed the check
    // below passing on that side effect alone.
    const realPreview = previewRecover;
    previewRecover = () => {};
    btn.click();
    const stillOpen = !!document.querySelector("#modal .state-modal");
    previewRecover = realPreview;
    return { since: f.elements.since.value, until: f.elements.until.value,
             cap: f.elements.limit_per_pk.value, anchor: f.elements.event.value,
             at, inline, stillOpen };
  });
  {
    const m = /as of (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/.exec(bridge.at || "");
    const want = m ? new Date(Date.parse(m[1].replace(" ", "T") + "Z") + 1000).toISOString().slice(0, 19).replace("T", " ") : null;
    (want && bridge.since === want && bridge.until === "")
      ? ok("restore: 'Restore to this state' sets the undo window to at+1s")
      : bad("restore: 'Restore to this state' sets the undo window to at+1s", JSON.stringify({ ...bridge, want }));
    // #1405. Retargeting the selectors above would pass just as well if the
    // result rendered in BOTH places, so state the negative separately.
    (bridge.inline === false)
      ? ok("restore: the reconstructed state renders in the dialog, not inline under the form")
      : bad("restore: the reconstructed state renders in the dialog, not inline under the form", JSON.stringify(bridge));
    (bridge.stillOpen === false)
      ? ok("restore: 'Restore to this state' closes the dialog before retargeting the form")
      : bad("restore: 'Restore to this state' closes the dialog before retargeting the form", JSON.stringify(bridge));
    // The two bridges set opposite scopes on one form. This action reverses
    // EVERY change after the instant — that is what makes the row land on the
    // state shown — so a cap inherited from Undo would reverse only the newest
    // and land it elsewhere, silently, with the button still naming the state
    // it did not produce.
    bridge.cap === ""
      ? ok("restore: 'Restore to this state' clears the Undo bridge's per-row cap")
      : bad("restore: 'Restore to this state' clears the Undo bridge's per-row cap", `limit_per_pk=${bridge.cap}`);
    bridge.anchor === ""
      ? ok("restore: 'Restore to this state' clears the Undo bridge's event anchor")
      : bad("restore: 'Restore to this state' clears the Undo bridge's event anchor", `event=${bridge.anchor}`);
  }

  // The other direction: arriving through Undo must land the cap in the field,
  // because that prefill is what makes the button reverse ONE change instead
  // of the row's whole history. Driven through the real bridge — pendingRecover
  // plus a navigate — rather than by calling the prefill, since the prefill
  // runs inside setSelectWhenReady's schema callback and a direct call would
  // skip the path that can actually break.
  //
  // And watch the POST while it happens. Every other check here reads
  // `limit_per_pk` off the form, which is order-blind: swapping the prefill
  // and the generateUndo call beneath it restores the original #1404 defect —
  // a request with no cap — while leaving the field holding "1" by the time
  // anything polls it. generateUndo snapshots the form synchronously at entry,
  // so the only witness that dies on that swap is the body it sent.
  const sentBodies = [];
  await page.route("**/api/recover", async (route) => {
    sentBodies.push(route.request().postData() || "");
    await route.continue();
  });
  const undoBridge = await page.evaluate(async (fixSchema) => {
    // Snapshot every field, not the schema alone: the scenarios below inherit
    // whatever this form already held (the timeline one sets only `pk` and
    // relies on schema+table being filled), and a re-render blanks all of it.
    {
      const f0 = document.getElementById("recover-form");
      window.__preUndo = {};
      if (f0) for (const n of ["schema", "table", "pk", "limit_per_pk", "since", "until", "event"]) {
        window.__preUndo[n] = f0.elements[n] ? f0.elements[n].value : "";
      }
    }
    pendingRecover = { schema: fixSchema, table: "orders", pk: "1", type: "update",
                       time: "2026-01-01 00:00:00", anchor: "2026-01-01T00:00:00Z|4242" };
    navigate("recover");
    for (let i = 0; i < 40; i++) {
      const f = document.getElementById("recover-form");
      if (f && f.elements.pk.value === "1") break;
      await new Promise((r) => setTimeout(r, 100));
    }
    const f = document.getElementById("recover-form");
    if (!f) return { err: "no recover form" };
    const eyebrow = (document.querySelector(".ctx-banner .ctx-eyebrow") || {}).textContent || "";
    const detail = Array.from(document.querySelectorAll(".ctx-banner .ctx-detail")).map((n) => n.textContent).join(" ");
    return { cap: f.elements.limit_per_pk.value, anchor: f.elements.event.value,
             until: f.elements.until.value, eyebrow, detail };
  }, FIX);
  // #1411: the anchor is what the bridge prefills now, and the cap must NOT
  // come back with it — two mechanisms narrowing one scope drift, and the cap
  // is the one the banner no longer mentions.
  (undoBridge.anchor === "2026-01-01T00:00:00Z|4242" && undoBridge.cap === "" && undoBridge.until === "2026-01-01 00:00:00")
    ? ok("restore: the Undo bridge prefills the event anchor alongside the ceiling, and no per-row cap")
    : bad("restore: the Undo bridge prefills the event anchor alongside the ceiling, and no per-row cap", JSON.stringify(undoBridge));
  // Give the generate its POST, then stop intercepting.
  for (let i = 0; i < 40 && !sentBodies.length; i++) await new Promise((r) => setTimeout(r, 100));
  await page.unroute("**/api/recover");
  {
    const parsed = sentBodies.map((b) => { try { return JSON.parse(b); } catch { return {}; } });
    const anchored = parsed.filter((b) => b.event === "2026-01-01T00:00:00Z|4242");
    (sentBodies.length > 0 && anchored.length === parsed.length)
      ? ok("restore: the Undo bridge's generate SENDS the anchor — the identity is on the wire, not just in the field")
      : bad("restore: the Undo bridge's generate SENDS the anchor — the identity is on the wire, not just in the field",
            JSON.stringify({ sent: sentBodies }));
  }
  // The prefill changes what the button reverses, so the banner has to say it.
  // A prefill the banner does not mention is a silent narrowing.
  (/one change/.test(undoBridge.eyebrow) && /exactly this/.test(undoBridge.detail)
    && /the one you clicked/.test(undoBridge.detail) && /Clear/.test(undoBridge.detail)
    && !/not necessarily the one you clicked/.test(undoBridge.detail))
    ? ok("restore: the Undo banner claims the clicked event, and no longer carries the retired same-second caveat")
    : bad("restore: the Undo banner claims the clicked event, and no longer carries the retired same-second caveat", JSON.stringify(undoBridge));
  // The two bridges collide one level above the fields. With the Undo banner
  // on screen — the only place in the run where it really is — driving
  // "Restore to this state" must retire it: the banner asserts that exactly the
  // clicked event is reversed, and that action clears the anchor. Left up, the
  // operator reads a one-event scope over a script that reverses everything
  // after the instant.
  const staleBanner = await page.evaluate(async () => {
    const f = document.getElementById("recover-form");
    const before = !!document.getElementById("undo-ctx-banner");
    f.elements.pk.value = "1";
    await runState(f, false);
    const btn = document.querySelector(".state-actions .btn-primary");
    if (!btn) return { err: "no Restore-to-this-state button on a found row" };
    btn.click();
    return { before, after: !!document.getElementById("undo-ctx-banner"),
             cap: f.elements.limit_per_pk.value };
  });
  if (staleBanner.err) {
    bad("restore: 'Restore to this state' retires the Undo banner it contradicts", staleBanner.err);
  } else {
    // `before` is asserted too: if the banner were never up, `after === false`
    // would pass while proving nothing.
    (staleBanner.before === true && staleBanner.after === false && staleBanner.cap === "")
      ? ok("restore: 'Restore to this state' retires the Undo banner it contradicts")
      : bad("restore: 'Restore to this state' retires the Undo banner it contradicts", JSON.stringify(staleBanner));
  }

  // Leave the page as this scenario found it. pendingRecover survives a
  // re-render, and a leftover Undo prefill makes later scenarios answer the
  // wrong question — historically the cap 400'd every generate that cleared
  // the PK, and since #1411 a leftover anchor pins them all to one old event.
  // Either way: a correct server response, and a wrong reason for fourteen
  // unrelated checks to go red.
  const undoRestore = await page.evaluate(async (fixSchema) => {
    pendingRecover = null;
    navigate("recover");
    for (let i = 0; i < 40; i++) {
      const f = document.getElementById("recover-form");
      if (f && !document.querySelector(".ctx-banner")) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    const f = document.getElementById("recover-form");
    if (!f || !window.__preUndo) return { schemaReady: false, want: null, got: null };
    // Schema first and on its own tick: the cascade listener repopulates the
    // table datalist and CLEARS a stale table value, so restoring table before
    // the cascade settles loses it again.
    // Through setSelectWhenReady, not by assigning .value: populateSchemas
    // fills the options asynchronously after the re-render, and assigning a
    // value the select does not yet carry silently leaves it blank — which is
    // what the Time-travel scenarios below then read as "no rows". The app
    // already owns this wait; using it is also what the real prefill does.
    const schemaReady = await new Promise((resolve) => {
      setSelectWhenReady(f, "schema", fixSchema, () => resolve(true));
      setTimeout(() => resolve(false), 5000);
    });
    await new Promise((r) => setTimeout(r, 300));
    for (const n of ["table", "pk", "limit_per_pk", "since", "until"]) {
      if (f.elements[n]) f.elements[n].value = window.__preUndo[n] || "";
    }
    // Report what the restore actually achieved instead of proceeding as if it
    // had. The schema is compared against the SNAPSHOT, not against fixSchema:
    // reinstalling a hardcoded value while claiming to restore is how this
    // silently installs the wrong schema the day a preceding scenario picks
    // another one.
    return { schemaReady, want: window.__preUndo.schema, got: f.elements.schema.value };
  }, FIX);
  (undoRestore && undoRestore.schemaReady && undoRestore.got === undoRestore.want)
    ? ok("restore: the Undo scenario hands the form back as it found it")
    : bad("restore: the Undo scenario hands the form back as it found it — the checks below inherit it",
          JSON.stringify(undoRestore));

  // #1405's hard constraint, and the one the change is most likely to lose:
  // the warnings travel INSIDE the dialog. They used to fill a sibling of
  // #state-out that would now sit behind the scrim, and a reconstruct can
  // return stale_baseline or a capture-gap caveat — a state read with its
  // caveat hidden is worse than the old layout, not better.
  //
  // Driven by stubbing `api` rather than by finding a fixture that warns: the
  // claim is about WHERE renderWarnings puts its output, and a canned response
  // exercises the real runState → openModal → renderWarnings path while making
  // the warning unconditional. Review showed deleting the whole warnings block
  // left both suites green, so nothing was holding this.
  const warnPlacement = await page.evaluate(async () => {
    const real = api;
    api = async (path, opts) => {
      if (path.startsWith("/api/reconstruct")) {
        return { schema: "e2eshop", table: "orders", pk: "1", at: "2026-01-01 00:00:00",
                 found: true, state: { id: 1, status: "shipped" },
                 warnings: ["stale_baseline: newest snapshot has no orders, fell back to an older one"] };
      }
      return real(path, opts);
    };
    try {
      const f = document.getElementById("recover-form");
      f.elements.pk.value = "1";
      await runState(f, false);
    } finally {
      api = real;
    }
    return {
      inDialog: !!document.querySelector("#modal .state-modal .warn-item"),
      onPage: !!document.querySelector("#state-warnings .warn-item"),
      text: (document.querySelector("#modal .state-modal .warn-item") || {}).textContent || "",
    };
  });
  (warnPlacement.inDialog && !warnPlacement.onPage && /stale_baseline/.test(warnPlacement.text))
    ? ok("restore: a reconstruct warning renders inside the dialog, not on the page behind it")
    : bad("restore: a reconstruct warning renders inside the dialog, not on the page behind it",
          JSON.stringify(warnPlacement));

  // The dialog's own dismissal controls. openModal wires the ✕ and the scrim
  // click and relies on globalKeydown for Escape; none of the three had a line
  // of executed coverage, and the one assertion that named dismissal was
  // satisfied by a side effect (see the stub above).
  const dismiss = await page.evaluate(async () => {
    const f = document.getElementById("recover-form");
    const out = {};
    await runState(f, false);
    out.openedForX = !!document.querySelector("#modal .state-modal");
    const x = document.querySelector("#modal .state-modal .modal-x");
    if (x) x.click();
    out.afterX = !!document.getElementById("modal").firstChild;

    await runState(f, false);
    const scrim = document.querySelector("#modal .modal-scrim");
    // On the scrim itself, not on the panel: openModal only dismisses when the
    // event target IS the scrim, so a click inside the dialog must not close it.
    if (scrim) scrim.querySelector(".modal").dispatchEvent(new MouseEvent("click", { bubbles: true }));
    out.afterInsideClick = !!document.getElementById("modal").firstChild;
    if (scrim) scrim.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    out.afterScrimClick = !!document.getElementById("modal").firstChild;

    await runState(f, false);
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    out.afterEscape = !!document.getElementById("modal").firstChild;
    return out;
  });
  (dismiss.openedForX && dismiss.afterX === false)
    ? ok("restore: the dialog's ✕ closes it")
    : bad("restore: the dialog's ✕ closes it", JSON.stringify(dismiss));
  (dismiss.afterInsideClick === true && dismiss.afterScrimClick === false)
    ? ok("restore: clicking the scrim closes the dialog, clicking inside it does not")
    : bad("restore: clicking the scrim closes the dialog, clicking inside it does not", JSON.stringify(dismiss));
  (dismiss.afterEscape === false)
    ? ok("restore: Escape closes the dialog")
    : bad("restore: Escape closes the dialog", JSON.stringify(dismiss));

  // The timeline is how an operator picks an instant without leaving to read
  // one off Events. Its own restore button used to route through undoEvent,
  // which sets `until` — reversing everything UP TO that point and landing the
  // row before its whole history, the opposite end from the state the button
  // names. Pin both actions against a real render.
  const tl = await page.evaluate(async () => {
    const f = document.getElementById("recover-form");
    f.elements.pk.value = "1";
    f.elements.since.value = "";
    f.elements.until.value = "2000-01-01 00:00:00"; // stale bound that must be cleared
    await runState(f, true);
    const nodes = Array.from(document.querySelectorAll(".tl-node"));
    if (!nodes.length) return { err: "no timeline nodes" };
    const last = nodes[nodes.length - 1];
    const time = (last.querySelector(".tl-time") || {}).textContent || "";
    const restore = last.querySelector(".tl-restore");
    const use = last.querySelector(".tl-use");
    if (!use) return { err: "no 'Use this moment' action" };
    use.click();
    const at = (document.querySelector('[name="state_at"]') || {}).value || "";
    if (!restore) return { time, at, skippedRestore: true };
    // Stubbed for the click, for the same reason the state panel's bridge is:
    // aimUndoAtInstant calls previewRecover synchronously, previewRecover opens
    // the busy dialog, and openBusyModal clears the shared #modal mount — so
    // the timeline dialog vanishes whether or not renderTimeline passed the
    // caller's close down to this button. Without the stub the assertion below
    // is satisfied by that side effect and proves nothing, which is exactly how
    // the state panel's version got shipped vacuous.
    const realPreview = previewRecover;
    previewRecover = () => {};
    restore.click();
    const stillOpen = !!document.querySelector("#modal .state-modal");
    previewRecover = realPreview;
    return { time, at, since: f.elements.since.value, until: f.elements.until.value, stillOpen };
  });
  if (tl.err) {
    bad("restore: timeline offers per-node actions", tl.err);
  } else {
    (tl.at === tl.time)
      ? ok("restore: 'Use this moment' fills the At field from the timeline")
      : bad("restore: 'Use this moment' fills the At field from the timeline", JSON.stringify(tl));
    if (tl.skippedRestore) {
      ok("restore: timeline restore skipped (baseline-only node)");
    } else {
      const want = new Date(Date.parse(tl.time.replace(" ", "T") + "Z") + 1000).toISOString().slice(0, 19).replace("T", " ");
      (tl.since === want && tl.until === "")
        ? ok("restore: timeline 'Restore to this state' aims AFTER the instant, not before it")
        : bad("restore: timeline 'Restore to this state' aims AFTER the instant, not before it", JSON.stringify({ ...tl, want }));
      // The history half of the dismissal. Show state's action is a footer on
      // the panel and closes; each timeline node carries its OWN button, and
      // those were left retargeting the form under a scrim that stayed up on
      // either of previewRecover's two early returns.
      (tl.stillOpen === false)
        ? ok("restore: a timeline node's 'Restore to this state' closes the dialog too")
        : bad("restore: a timeline node's 'Restore to this state' closes the dialog too", JSON.stringify(tl));
    }
  }


  // ── Scenario 17d — the JavaScript half of reduced motion (#1392) ──
  // Placed here, out of numeric order, because this is the one point in the
  // run with a REAL rendered timeline. The stagger under test is scheduled by
  // renderTimeline's own rAF loop; the synthesised DOM 17c uses never runs it.
  //
  // The Go guard and 17c both stop at the stylesheet. This stagger is driven
  // from app.js — one setTimeout per node — so honouring the preference in CSS
  // alone removes the SMOOTHNESS while the loop still moves every node: under
  // `reduce` a long timeline snapped one node at a time across seconds of
  // shifting content, a jumpier version of the motion the preference asked to
  // remove. Only matchMedia can see this, and only at run time.
  //
  // NOT covered: the two scrollIntoView call sites gated on the same helper.
  // Their smooth/auto difference is a browser-internal scroll with no
  // observable DOM state, so this scenario makes no claim about them.
  const staggerProbe = async (mode) => {
    await page.emulateMedia({ reducedMotion: mode });
    return page.evaluate(async () => {
      const f = document.getElementById("recover-form");
      f.elements.pk.value = "1";
      f.elements.since.value = "";
      f.elements.until.value = "2000-01-01 00:00:00";
      await runState(f, true);
      // Record the poll tick at which each node FIRST carries `.in`, instead
      // of sampling once at a fixed instant.
      //
      // What is under test is a relative ordering — setTimeout(60) fires
      // before setTimeout(115) however loaded the runner is — and an ordering
      // survives a stall that a fixed window does not. The single snapshot
      // this replaces needed the second node's 115ms to still be in the
      // future when it read, and this fixture renders exactly two nodes, so
      // that margin was the whole assertion. It also gets STRONGER as the
      // fixture grows rather than weaker.
      const firstSeen = [];
      const gapAround = [];
      let arrived = 0, total = 0;
      let prev = performance.now();
      for (let tick = 0; tick < 90; tick++) {
        const nodes = document.querySelectorAll(".tl-node");
        total = nodes.length;
        arrived = 0;
        nodes.forEach((n, i) => {
          if (!n.classList.contains("in")) return;
          arrived++;
          if (firstSeen[i] === undefined) firstSeen[i] = tick;
        });
        // How long this observation was blind for. The ordering claim is only
        // readable if consecutive polls are closer together than the stagger
        // step; a stall longer than that hides the order from the OBSERVER
        // without saying anything about the code.
        const nowT = performance.now();
        gapAround.push(Math.round(nowT - prev));
        prev = nowT;
        if (total > 0 && arrived === total) break;
        await new Promise((r) => setTimeout(r, 10));
      }
      return { total, arrived, firstSeen, maxGap: Math.max(0, ...gapAround) };
    });
  };
  // Retried on READINESS, like the no-preference arm below and for the same
  // reason: a runner loaded enough to starve runState leaves the probe with
  // zero nodes to look at, and `total: 0` was reported as a missing-stagger
  // regression (#1463). Readiness only — not the assertion — so a real JS
  // stagger under reduced motion satisfies rmRead on the first attempt and
  // still fails the same-tick check below, every attempt.
  const rmRead = (r) => r.total >= 2 && r.arrived === r.total;
  let rmTl = await staggerProbe("reduce");
  for (let attempt = 0; attempt < 2 && !rmRead(rmTl); attempt++) {
    rmTl = await staggerProbe("reduce");
  }
  // The no-preference arm reads an ORDER out of a 55ms step by polling every
  // 10ms, so a loaded runner can hide it: if the loop stalls longer than one
  // step, both nodes are first seen on the same poll and the arm reports a
  // missing stagger that is really a blind observer. Seen twice on a machine
  // running builds alongside the suite, with the trace showing exactly that —
  // and it took a bisect against main to find out it was the runner and not
  // the code, which is a false alarm expensive enough to be worth preventing.
  //
  // Retried rather than loosened: a real regression fails every attempt, so
  // the assertion keeps its teeth. maxGap travels into the failure message so
  // the next person can classify it in one look instead of a bisect.
  let npTl = await staggerProbe("no-preference");
  const staggerRead = (r) => r.total >= 2 && r.arrived === r.total && r.firstSeen[1] > r.firstSeen[0];
  for (let attempt = 0; attempt < 2 && !staggerRead(npTl); attempt++) {
    npTl = await staggerProbe("no-preference");
  }
  await page.emulateMedia({ reducedMotion: null });

  (rmTl.total > 0 && rmTl.arrived === rmTl.total
    && rmTl.firstSeen.every((t) => t === rmTl.firstSeen[0]))
    ? ok("reduced motion: every timeline node arrives on the same tick — no JS stagger")
    : bad("reduced motion: every timeline node arrives on the same tick — no JS stagger", JSON.stringify(rmTl));
  // The other half, and the reason the arm above is not satisfied by a broken
  // render: under no-preference the nodes must arrive IN SEQUENCE.
  //
  // Two nodes are enough for an order claim, so unlike the snapshot version
  // there is no fixture size at which this quietly stops asserting — and a
  // fixture that drops below two is reported as a failure rather than waved
  // through, because at that point the scenario has proved nothing.
  staggerRead(npTl)
    ? ok("reduced motion: under no-preference the timeline arrives one node at a time")
    : bad("reduced motion: under no-preference the timeline arrives one node at a time — "
        + (npTl.maxGap > 55 ? `NOTE: the poll stalled ${npTl.maxGap}ms, longer than the 55ms stagger step, on all 3 attempts` : "the poll kept up, so this is the code"),
        JSON.stringify(npTl));

  // Time-travel results now live in a dialog (#1405), and the scenarios above
  // leave the last one open. Dismiss it before moving on: a scrim spanning the
  // viewport is invisible to page.evaluate but intercepts every real click,
  // and leaving shared state behind for later scenarios is the failure this
  // suite has already been bitten by once.
  await page.evaluate(() => { const m = document.getElementById("modal"); if (m) m.replaceChildren(); });

  // Scenario 14c — the Overview against the LIVE daemon (#1300). The fixture
  // scenario below drives buildOverview directly, so it cannot catch a route
  // that was never registered, an authz table that refuses it, or a JSON shape
  // that drifted from the struct. This one renders the real page: two tiles
  // must come back carrying a real period, and the window line must name the
  // aggregate's own bounds rather than the "—" a failed fetch would leave.
  await page.evaluate(() => navigate("overview"));
  // Wait on the TILES, not on the scope lines: waiting on the thing under test
  // turns a missing scope into a driver timeout that aborts the rest of the run
  // instead of one legible failure.
  await page.waitForFunction(() => document.querySelectorAll(".ov-stat").length === 4);
  // The coverage card fills independently (#1352); its freshness clock is part
  // of what this scenario pins, so wait for that fill too.
  await page.waitForFunction(() => !!document.querySelector(".cov-card .cov-asof"));
  const ovLive = await page.evaluate(() => ({
    scopes: Array.from(document.querySelectorAll(".ov-stat")).map((n) => (n.querySelector(".ov-stat-scope") || {}).textContent || ""),
    win: (document.querySelector(".ov-coverage") || {}).textContent || "",
    warns: Array.from(document.querySelectorAll(".warn-item")).map((n) => n.textContent),
    asof: (document.querySelector(".cov-card .cov-asof") || {}).textContent || "",
    tzChips: document.querySelectorAll(".tz-chip").length,
  }));
  // #1421: the tint layer is RENDERED, not merely classed. Asserted as
  // computed background because that is the only thing the operator sees — a
  // cascade rule beating the tint would leave the classes in the DOM and the
  // panels white, and no grep can see a cascade. The two panels must differ
  // from each other (violet vs sun) and from plain white; the pill eyebrow
  // must render on a distinct surface from its card.
  const tint = await page.evaluate(() => {
    const bg = (sel) => { const n = document.querySelector(sel); return n ? getComputedStyle(n).backgroundColor : null; };
    return { violet: bg(".ov-panel.tcard-violet"), sun: bg(".ov-panel.tcard-sun"),
             pill: bg(".ov-panel.tcard-violet .tag-pill"), white: "rgb(255, 255, 255)" };
  });
  (tint.violet && tint.sun && tint.violet !== tint.white && tint.sun !== tint.white && tint.violet !== tint.sun)
    ? ok("overview (live): the two panels render the home tint layer (violet ≠ sun ≠ white)")
    : bad("overview (live): the two panels render the home tint layer (violet ≠ sun ≠ white)", JSON.stringify(tint));
  (tint.pill && tint.pill !== tint.violet)
    ? ok("overview (live): the pill eyebrow renders on its own surface inside the tinted card")
    : bad("overview (live): the pill eyebrow renders on its own surface inside the tinted card", JSON.stringify(tint));
  // Identity, not just difference (#1423 review): a wrong-but-different color
  // passed the check above. The violet ground IS the home page's --violet-tint
  // (#EFE9FF) — home fidelity is the point of #1421, so the exact value is the
  // contract. The two LITERAL copies are this pin and the token; the Go test
  // reads the token live, so on a palette move its floors ring on their own —
  // what needs re-measuring by hand is the recorded figures in style.css's
  // comments (the skeleton peak, the ink-3 note, the divider note).
  (tint.violet === "rgb(239, 233, 255)")
    ? ok("overview (live): the violet panel renders the home page's own tint value")
    : bad("overview (live): the violet panel renders the home page's own tint value", JSON.stringify(tint));
  // The WIRING the Go token test cannot see (#1423 review: .ov-ev-pk sat on
  // the violet tint wearing --ink-3 at 4.20:1 while every token-level check
  // was green). Synthetic probes inside the REAL panels: computed style
  // answers for the class whether or not live rows have rendered yet.
  const tintText = await page.evaluate(() => {
    const vp = document.querySelector(".ov-panel.tcard-violet");
    const sp = document.querySelector(".ov-panel.tcard-sun");
    if (!vp || !sp) return null;
    const cs = getComputedStyle;
    // Canvas pixels, not string parsing: the ink ramp computes to oklch(...)
    // strings, which no rgb() regex can read — the first draft measured 0 for
    // every text ratio and only the hex-declared dividers parsed. Same
    // technique as brandProbe, for the same reason.
    const cv = document.createElement("canvas");
    cv.width = cv.height = 1;
    const ctx = cv.getContext("2d", { willReadFrequently: true });
    const lum = (str) => {
      ctx.fillStyle = "#010203";
      ctx.fillStyle = str;
      if (ctx.fillStyle === "#010203") return null; // unparseable, fail loud
      ctx.clearRect(0, 0, 1, 1);
      ctx.fillRect(0, 0, 1, 1);
      const d = ctx.getImageData(0, 0, 1, 1).data;
      const c = (v) => { v /= 255; return v <= 0.04045 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); };
      return 0.2126 * c(d[0]) + 0.7152 * c(d[1]) + 0.0722 * c(d[2]);
    };
    const ratio = (a, b) => {
      const la = lum(a), lb = lum(b);
      if (la === null || lb === null) return 0;
      const [hi, lo] = [la, lb].sort((x, y) => y - x);
      return (hi + 0.05) / (lo + 0.05);
    };
    const probe = (parent, cls, tag) => {
      const n = document.createElement(tag || "div");
      n.className = cls;
      parent.appendChild(n);
      return n;
    };
    const vbg = cs(vp).backgroundColor, sbg = cs(sp).backgroundColor;
    const pk = probe(vp, "ov-ev-pk", "span");
    const ev = probe(vp, "ov-ev");
    const cov = probe(sp, "ov-coverage");
    const trow = probe(sp, "ov-tablerow");
    const out = {
      pkRatio: ratio(cs(pk).color, vbg),
      evDivider: ratio(cs(ev).borderTopColor, vbg),
      covRatio: ratio(cs(cov).color, sbg),
      trowDivider: ratio(cs(trow).borderTopColor, sbg),
    };
    [pk, ev, cov, trow].forEach((n) => n.remove());
    return out;
  });
  (tintText && tintText.pkRatio >= 4.5 && tintText.covRatio >= 4.5)
    ? ok("overview (live): body text on the tinted panels holds the 4.5:1 floor as rendered")
    : bad("overview (live): body text on the tinted panels holds the 4.5:1 floor as rendered", JSON.stringify(tintText));
  // 1.15 = the hairline idiom's own floor (--line-soft on white is 1.17).
  // Deleting the tint-aware divider override lands the violet list at 1.01 —
  // no dividers at all — and rings here, not in any token test.
  (tintText && tintText.evDivider >= 1.15 && tintText.trowDivider >= 1.15)
    ? ok("overview (live): row dividers stay visible on the tinted panels")
    : bad("overview (live): row dividers stay visible on the tinted panels", JSON.stringify(tintText));
  ovLive.scopes.every((s) => s.trim() !== "")
    ? ok("overview (live): every rendered tile carries a scope line")
    : bad("overview (live): every rendered tile carries a scope line", JSON.stringify(ovLive.scopes));
  // The window is the live retention (#1352): "live retention · ~N h/d" when a
  // dated partition names the floor, the stated 24 h fallback otherwise.
  (ovLive.scopes.filter((s) => /^(live retention · ~\d+ [hd]|last 24 h)/.test(s)).length === 2)
    ? ok("overview (live): the window tiles carry a real window from /api/activity")
    : bad("overview (live): the window tiles carry a real window from /api/activity", JSON.stringify(ovLive));
  // The aggregate is a server-side materialization (#1352): its freshness must
  // be rendered with its numbers, or a stale count reads as live.
  (ovLive.scopes.filter((s) => / · as of \d{2}:\d{2}:\d{2} UTC/.test(s)).length === 2)
    ? ok("overview (live): the window tiles disclose the aggregate's refresh time, labeled UTC")
    : bad("overview (live): the window tiles disclose the aggregate's refresh time, labeled UTC", JSON.stringify(ovLive));
  (/window \(UTC\)\s+\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(ovLive.win) && !ovLive.warns.some((w) => /window counts could not be loaded/.test(w)))
    ? ok("overview (live): the window line states the aggregate's own bounds and their zone")
    : bad("overview (live): the window line states the aggregate's own bounds and their zone", JSON.stringify(ovLive));
  // Timezone discipline (#1354): the freshness clock is the labeled UTC clock
  // — nowClock() was toLocaleTimeString(), which put an unlabeled browser-local
  // time directly above data timestamps that are all UTC, so the same instant
  // read hours apart on one page. And the sections that render bare data
  // timestamps each carry a UTC chip.
  /as of \d{2}:\d{2}:\d{2} UTC$/.test(ovLive.asof)
    ? ok("overview (live): the freshness clock is UTC and says so")
    : bad("overview (live): the freshness clock is UTC and says so", JSON.stringify(ovLive.asof));
  (ovLive.tzChips >= 2)
    ? ok("overview (live): timestamp-bearing sections carry a UTC chip")
    : bad("overview (live): timestamp-bearing sections carry a UTC chip", `tz chips: ${ovLive.tzChips}`);

  // Scenario 15 — Overview scope honesty (#686 + #1300): fixture-drive the REAL
  // buildOverview and pin that (a) every tile states its OWN scope, (b) the
  // period-scoped numbers come from the /api/activity aggregate rather than
  // from the handful of events the Recent-changes list renders, and (c) the
  // window line uses the AGGREGATE's bounds — a reintroduced
  // status.coverage.oldest fallback (#679/#684) fails here and nowhere else,
  // since the Go suite never renders app.js.
  //
  // The fixture is built so the two sources DISAGREE on purpose: the events
  // array holds 1 delete across 1 table, the aggregate says 17 deletes across
  // 5. A tile that recomputes from the events (the pre-#1300 behaviour) prints
  // 1 and 1 and fails.
  const mkev = (ts, type, pk) => ({ event_timestamp: ts, schema_name: "ovfix", table_name: "t", event_type: type, pk_values: pk, changed_columns: [] });
  const ovFix = {
    events: [mkev("2026-03-01 10:30:00", "DELETE", "9"), mkev("2026-03-01 10:00:00", "INSERT", "8")],
    // oldest is a decade before the aggregate's window: if it ever reaches the
    // window line or a tile, the number is attributed to the wrong span.
    status: { coverage: { oldest: "2020-01-01 00:00:00", newest: "2026-06-30 00:00:00", total_events: 123456 }, total_events_estimate: 3121 },
    activity: {
      label: "live retention · ~24 h", refreshed_at: "2026-03-02 09:00:00",
      since: "2026-03-01 09:00:00", until: "2026-03-02 09:00:00",
      total: 640, inserts: 400, updates: 223, deletes: 17, other: 0,
      tables: 5, complete: true,
      top_tables: [{ schema: "ovfix", table: "orders", insert: 300, update: 200, delete: 10, total: 510 }],
    },
  };

  const ov = await page.evaluate((fx) => {
    const readTiles = () => Array.from(document.querySelectorAll(".ov-stat")).map((n) => ({
      v: (n.querySelector(".ov-stat-v") || {}).textContent || "",
      k: (n.querySelector(".ov-stat-k") || {}).textContent || "",
      scope: (n.querySelector(".ov-stat-scope") || {}).textContent || "",
    }));
    const notes = () => Array.from(document.querySelectorAll(".warn-item")).map((n) => n.textContent);
    buildOverview(fx.status, { events: fx.events }, null, fx.activity);
    const full = {
      tiles: readTiles(),
      win: (document.querySelector(".ov-coverage") || {}).textContent || "",
    };
    // Degraded arms, rendered through the SAME real function: a partial
    // aggregate must SAY partial on the tiles it scopes (a narrower number
    // under a wider label is the bug), and a failed aggregate must render "—"
    // rather than a zero nobody measured.
    buildOverview(fx.status, { events: fx.events }, null,
      Object.assign({}, fx.activity, { complete: false, truncated: true, notes: ["This index has more than 20000 table/event-type groups in the window; the counts below are a floor, not the total."] }));
    const partial = { tiles: readTiles(), notes: notes() };
    buildOverview(fx.status, { events: fx.events }, null, null);
    const missing = { tiles: readTiles(), notes: notes() };
    return { full, partial, missing };
  }, ovFix);

  const tileBy = (tiles, key) => tiles.find((t) => t.k === key) || { v: "", k: "", scope: "" };
  (ov.full.tiles.length === 4 && ov.full.tiles.every((t) => t.scope.trim() !== ""))
    ? ok("overview: every tile states its own scope")
    : bad("overview: every tile states its own scope", JSON.stringify(ov.full.tiles));
  (tileBy(ov.full.tiles, "deletes").v === "17" && tileBy(ov.full.tiles, "deletes").scope.includes("live retention") && tileBy(ov.full.tiles, "deletes").scope.includes("as of 09:00:00 UTC"))
    ? ok("overview: the deletes tile is the server aggregate, labelled with its window and freshness")
    : bad("overview: the deletes tile is the server aggregate, labelled with its window and freshness", JSON.stringify(tileBy(ov.full.tiles, "deletes")));
  (tileBy(ov.full.tiles, "tables touched").v === "5" && tileBy(ov.full.tiles, "tables touched").scope.includes("live retention"))
    ? ok("overview: the tables-touched tile is the server aggregate, labelled with its window")
    : bad("overview: the tables-touched tile is the server aggregate, labelled with its window", JSON.stringify(tileBy(ov.full.tiles, "tables touched")));
  // total_events_estimate is information_schema TABLE_ROWS — an InnoDB
  // estimate. It sits beside three exact counts, so the tile has to say so.
  (tileBy(ov.full.tiles, "changes indexed").scope.includes("all time") && /estimate/i.test(tileBy(ov.full.tiles, "changes indexed").scope))
    ? ok("overview: the all-time tile declares both its scope and that it is an estimate")
    : bad("overview: the all-time tile declares both its scope and that it is an estimate", JSON.stringify(tileBy(ov.full.tiles, "changes indexed")));
  (ov.full.win.includes("2026-03-01 09:00:00") && ov.full.win.includes("2026-03-02 09:00:00") && !ov.full.win.includes("2020-01-01"))
    ? ok("overview: window line uses the aggregate's own bounds")
    : bad("overview: window line uses the aggregate's own bounds", ov.full.win);
  (tileBy(ov.partial.tiles, "deletes").scope.includes("partial") && ov.partial.notes.some((n) => /floor/.test(n)))
    ? ok("overview: an incomplete window is marked partial on the tile and explained")
    : bad("overview: an incomplete window is marked partial on the tile and explained", JSON.stringify(ov.partial));
  (tileBy(ov.missing.tiles, "deletes").v === "—" && tileBy(ov.missing.tiles, "tables touched").v === "—" && ov.missing.notes.length > 0)
    ? ok("overview: a failed aggregate shows no number, never a zero")
    : bad("overview: a failed aggregate shows no number, never a zero", JSON.stringify(ov.missing));

  // Scenario 15b — Baselines page live (#686, moved off Storage by #1384):
  // with the daemon opted in (BINTRAIL_CONSOLE_BASELINE_TRIGGER=1) and this
  // server baseline-configured, the Create-baseline button must render
  // enabled, and the fixture snapshot (1 table, anchored at
  // binlog.000001:50) must be listed.
  await page.evaluate(() => navigate("baselines"));
  await page.waitForFunction(() => Array.from(document.querySelectorAll(".stg-row")).some((r) => r.textContent.includes("binlog.000001:50")));
  const stg = await page.evaluate(() => {
    // #1415: the Create action moved to the context strip (page level); the
    // uniform table count moved there too, so the row carries only what
    // varies (the binlog anchor) plus the newest treatment.
    const strip = document.querySelector(".ctx-strip");
    const btn = strip ? Array.from(strip.querySelectorAll("button")).find((b) => b.textContent === "Create backup") : null;
    const row = Array.from(document.querySelectorAll(".stg-row")).find((r) => r.textContent.includes("binlog.000001:50"));
    return { capOn: !!capsCache.baseline_trigger, stripPresent: !!strip,
      stripText: strip ? strip.textContent : "",
      btnPresent: !!btn, btnEnabled: btn ? !btn.disabled : false,
      rowText: row ? row.textContent : "",
      rowIsLatest: row ? row.classList.contains("stg-row-latest") : false,
      sourceOneLine: strip && strip.querySelector(".ctx-source") ?
        getComputedStyle(strip.querySelector(".ctx-source")).whiteSpace === "nowrap" : false };
  });
  stg.capOn ? ok("baselines: baseline_trigger capability reaches the frontend") : bad("baselines: baseline_trigger capability reaches the frontend", "capsCache.baseline_trigger falsy");
  (stg.stripPresent && stg.btnPresent && stg.btnEnabled) ? ok("baselines: Create baseline is a page action on the context strip, enabled when both gates pass") : bad("baselines: Create baseline is a page action on the context strip, enabled when both gates pass", `strip=${stg.stripPresent} present=${stg.btnPresent} enabled=${stg.btnEnabled}`);
  // The facts moved, they did not vanish: table count and source live on the
  // strip; the row keeps the anchor and gains the newest treatment.
  (/1 per backup/.test(stg.stripText) && stg.sourceOneLine)
    ? ok("baselines: the uniform table count and the one-line source render on the strip")
    : bad("baselines: the uniform table count and the one-line source render on the strip", stg.stripText);
  (stg.rowIsLatest && /ago/.test(stg.rowText) && !/table\(s\)/.test(stg.rowText))
    ? ok("baselines: the newest row wears the treatment, carries relative age, and drops the constant column")
    : bad("baselines: the newest row wears the treatment, carries relative age, and drops the constant column", stg.rowText);

  // Scenario 15c — the button's other gate arms, fixture-driven through the
  // REAL baselinesPanel (destination-missing can't exist live once the daemon
  // sets a default --baseline-dir): no destination → no button + the setup
  // empty state; capability off → no button even with a destination.
  const gates = await page.evaluate(() => {
    const servers = [{ id: "srv-fix", name: "fixture", kind: "registry" }];
    const cur = servers[0];
    const keepCur = currentServer;
    currentServer = "srv-fix";
    // The button lives on the STRIP since #1415 — drive both builders so the
    // gate holds where the button actually is AND the panel keeps its empty
    // states.
    const cfgOff = baselinesPanel({ configured: false }, servers);
    const cfgOffStrip = baselineContextStrip({ configured: false }, cur);
    const keepCap = capsCache.baseline_trigger;
    capsCache.baseline_trigger = false;
    const capOff = baselinesPanel({ configured: true, source: "/tmp/baselines", snapshots: [] }, servers);
    const capOffStrip = baselineContextStrip({ configured: true, source: "/tmp/baselines", snapshots: [] }, cur);
    capsCache.baseline_trigger = keepCap;
    currentServer = keepCur;
    const hasBtn = (n) => Array.from(n.querySelectorAll("button")).some((b) => b.textContent === "Create backup");
    // #1677: where the button would be, the strip says creation is off.
    const offNote = (n) => /CREATE BACKUP/.test(n.textContent) && /set when DBTrail starts/.test(n.textContent);
    return {
      cfgOffBtn: hasBtn(cfgOff) || hasBtn(cfgOffStrip),
      cfgOffEmpty: /No backups configured/.test(cfgOff.textContent),
      cfgOffNote: offNote(cfgOffStrip),
      capOffBtn: hasBtn(capOff) || hasBtn(capOffStrip),
      capOffEmpty: /no backups found/.test(capOff.textContent),
      capOffNote: offNote(capOffStrip),
    };
  });
  (!gates.cfgOffBtn && gates.cfgOffEmpty)
    ? ok("baselines: no baseline destination → no button, setup empty state")
    : bad("baselines: no baseline destination → no button, setup empty state", JSON.stringify(gates));
  (!gates.capOffBtn && gates.capOffEmpty)
    ? ok("baselines: baseline_trigger off → no button even with a destination")
    : bad("baselines: baseline_trigger off → no button even with a destination", JSON.stringify(gates));
  (gates.capOffNote && !gates.cfgOffNote && !/CREATE BACKUP/.test(stg.stripText))
    ? ok("baselines: baseline_trigger off → the strip says creation is off where the button would be, and only then")
    : bad("baselines: baseline_trigger off → the strip says creation is off where the button would be, and only then", JSON.stringify({ gates, live: stg.stripText }));

  // Scenario 15e — the Backups feature set: rename, per-row detail with real
  // sizes, the tar.gz download wire, the restore card's gate + inline refusal,
  // and the in-progress region. All against the REAL fixture snapshot the
  // runner produced with `bintrail baseline`.
  await page.evaluate(() => navigate("baselines"));
  await new Promise((r) => setTimeout(r, 600));
  const bk = await page.evaluate(async () => {
    const out = {};
    const v = document.querySelector(".view");
    out.title = (v.querySelector("h1") || {}).textContent || "";
    out.stripLabels = Array.from(v.querySelectorAll(".ctx-label")).map((n) => n.textContent);
    // Expand the newest row: the detail must load the REAL files endpoint.
    const row = v.querySelector(".stg-row.bk-expandable");
    out.expandable = !!row;
    if (row) {
      row.click();
      for (let i = 0; i < 40 && !/file\(s\)/.test(v.querySelector(".bk-detail").textContent); i++) {
        await new Promise((r) => setTimeout(r, 100));
      }
      const det = v.querySelector(".bk-detail");
      out.detailText = det ? det.textContent : "";
      out.detailTables = det ? det.querySelectorAll(".bk-table tbody tr").length : 0;
      out.detailHasDownload = det ? Array.from(det.querySelectorAll("button")).some((b) => /Download/.test(b.textContent)) : false;
    }
    // The download wire: real bytes, gzip magic, attachment filename.
    const at = (v.querySelector(".stg-row .stg-name") || {}).textContent || "";
    const headers = TOKEN ? { Authorization: ["Bearer", TOKEN].join(" ") } : {};
    if (currentServer) headers["X-Bintrail-Server"] = currentServer;
    const res = await fetch("/api/baselines/download?at=" + encodeURIComponent(at.trim()), { headers });
    out.dlStatus = res.status;
    out.dlDisposition = res.headers.get("Content-Disposition") || "";
    const buf = new Uint8Array(await res.arrayBuffer());
    out.dlMagic = buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b;
    out.dlBytes = buf.length;
    if (res.status !== 200) out.dlBody = new TextDecoder().decode(buf).slice(0, 200);
    return out;
  });
  /^Backups$/.test(bk.title.trim())
    ? ok("backups: the page is named Backups")
    : bad("backups: the page is named Backups", bk.title);
  bk.stripLabels.includes("BACKUPS")
    ? ok("backups: the strip counts BACKUPS, not snapshots")
    : bad("backups: the strip counts BACKUPS, not snapshots", JSON.stringify(bk.stripLabels));
  (bk.expandable && bk.detailTables >= 1 && /B/.test(bk.detailText) && bk.detailHasDownload)
    ? ok("backups: a row expands into real tables, sizes and a download action")
    : bad("backups: a row expands into real tables, sizes and a download action", JSON.stringify({ e: bk.expandable, t: bk.detailTables, d: bk.detailHasDownload, txt: (bk.detailText || "").slice(0, 120) }));
  (bk.dlStatus === 200 && bk.dlMagic && /dbtrail-backup-.*\.tar\.gz/.test(bk.dlDisposition) && bk.dlBytes > 100 && !bk.dlBody)
    ? ok("backups: the download endpoint streams a gzip archive with an attachment name")
    : bad("backups: the download endpoint streams a gzip archive with an attachment name", JSON.stringify({ s: bk.dlStatus, m: bk.dlMagic, cd: bk.dlDisposition, n: bk.dlBytes }));

  // 15e-2: the restore card. Gated on the capability the daemon advertises;
  // a bad instant is refused INLINE (the server's 400 lands next to the
  // input, not in a toast that outlives nothing).
  const bkRestore = await page.evaluate(async () => {
    const out = { cap: !!capsCache.baseline_restore };
    const v = document.querySelector(".view");
    // Two cards wear .bk-restore since the sql-export card landed; pick the
    // restore one by its summary or this guard drifts to the wrong card the
    // first time their gates diverge.
    const card = Array.from(v.querySelectorAll(".bk-restore")).find((c) =>
      /Restore to a moment/.test((c.querySelector(".form-adv-summary") || {}).textContent || ""));
    out.card = !!card;
    if (!card) return out;
    card.open = true;
    const input = card.querySelector("input");
    out.prefilled = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(input.value);
    input.value = "not-a-time";
    card.querySelector("button").click();
    for (let i = 0; i < 40; i++) {
      const msg = card.querySelector(".form-msg.err");
      if (msg && !msg.hidden && msg.textContent) { out.inlineErr = msg.textContent; break; }
      await new Promise((r) => setTimeout(r, 100));
    }
    return out;
  });
  (bkRestore.cap && bkRestore.card && bkRestore.prefilled)
    ? ok("backups: the restore card renders under its capability, prefilled with the newest backup time")
    : bad("backups: the restore card renders under its capability, prefilled with the newest backup time", JSON.stringify(bkRestore));
  (bkRestore.inlineErr && /UTC time/.test(bkRestore.inlineErr))
    ? ok("backups: a bad restore instant is refused inline with the server's words")
    : bad("backups: a bad restore instant is refused inline with the server's words", JSON.stringify(bkRestore.inlineErr));

  // 15e-3: the in-progress region, driven through the real builders (a live
  // run cannot be photographed deterministically; same pattern as the
  // verification running-state scenario).
  const bkRun = await page.evaluate(() => {
    const runs = backupRunsInFlight(
      { baseline: { state: "running", since: "2026-06-10T12:00:00Z" } },
      { restore: { state: "running", at: "2026-06-01T00:00:00Z" } },
      { refresh: { state: "running" } });
    const region = backupRunRegion(runs);
    return {
      count: runs.length,
      live: region.querySelectorAll(".chip-live").length,
      progress: !!region.querySelector(".vfy-progress"),
      text: region.textContent,
    };
  });
  (bkRun.count === 3 && bkRun.live === 3 && bkRun.progress && /Creating a backup/.test(bkRun.text) && /Restoring to/.test(bkRun.text))
    ? ok("backups: every running kind renders a live chip, words, and the motion strip")
    : bad("backups: every running kind renders a live chip, words, and the motion strip", JSON.stringify(bkRun));

  // 15e-3b (#1725): a full backup that is published on this machine while
  // its copy to the destination still runs is in flight too, in different
  // words: it must not read as "still creating" (a refresh may start now)
  // nor vanish from the region as if it were done.
  const bkUp = await page.evaluate(() => {
    const runs = backupRunsInFlight({ baseline: { state: "succeeded", uploading: true, tables: 4 } }, null, null, null);
    const settled = backupRunsInFlight({ baseline: { state: "succeeded", tables: 4 } }, null, null, null);
    return { count: runs.length, kind: runs[0] && runs[0].kind, text: runs[0] && runs[0].text, settled: settled.length };
  });
  (bkUp.count === 1 && bkUp.kind === "dump" && /saved on this machine: 4 table/.test(bkUp.text) && /copying it to the backup destination/.test(bkUp.text) && !/Creating a backup/.test(bkUp.text) && bkUp.settled === 0)
    ? ok("backups: a published backup still uploading is in flight in its own words, and settles once uploaded")
    : bad("backups: a published backup still uploading is in flight in its own words, and settles once uploaded", JSON.stringify(bkUp));

  // 15e-4: fold refusals are rewritten for this page. The engine's errors are
  // per-table and newline-joined; the rewrite must strip every CLI remedy
  // WITHOUT eating the next table's identity (the first draft's non-global,
  // newline-crossing regex did exactly that).
  const bkErr = await page.evaluate(() => {
    const twoTables = "orders: stamped capture gap for shop.orders; pass --allow-gaps to proceed with a known-incomplete reconstruction: gap\n" +
      "customers: stamped capture gap for shop.customers; pass --allow-gaps to proceed with a known-incomplete reconstruction: gap";
    const out = backupFoldError(twoTables);
    return {
      noFlags: !/--allow-gaps/.test(out) && !/--at/.test(backupFoldError("remove it, or target a different instant with --at")),
      bothTables: /shop\.orders/.test(out) && /shop\.customers/.test(out),
      terminated: /[.!?]$/.test(out.trim()),
    };
  });
  (bkErr.noFlags && bkErr.bothTables && bkErr.terminated)
    ? ok("backups: fold refusals lose their CLI flags without losing table identities")
    : bad("backups: fold refusals lose their CLI flags without losing table identities", JSON.stringify(bkErr));

  // Scenario 15f — the made-to-measure .sql backup: pick an instant, the
  // daemon folds the nearest earlier backup forward through the index and
  // hands out a mydumper-format dump. Driven END TO END against the fixture:
  // baseline rows 1,2,4 + INSERT id 3 + UPDATE id 1 (-> shipped) + DELETE
  // id 2, built at TT_AT (after all three), so the dump must carry the folded
  // state — not the baseline, not the raw events.
  // The two download lanes (#1572). The Go guards read the SOURCE: they can
  // see that backupLane derives its count word and that backupFilesShape
  // draws one tile per file, but not that a reader ends up looking at two
  // tiles. This is the only check that renders them.
  const lanes = await page.evaluate(() => {
    const out = { views: !!capsCache.views, lanes: [] };
    document.querySelectorAll(".bk-take .bk-lane").forEach((l) => {
      const t = l.querySelector(".bk-lane-t");
      out.lanes.push({
        title: t ? t.textContent : "",
        tiles: l.querySelectorAll(".bk-file").length,
        lead: (l.querySelector(".bk-lane-lead") || {}).textContent || "",
        views: Array.from(l.querySelectorAll("button")).some((b) => /views\.sql/.test(b.textContent)),
      });
    });
    return out;
  });
  const duck = lanes.lanes.find((l) => /DuckDB/.test(l.title));
  const mysql = lanes.lanes.find((l) => /MySQL/.test(l.title));
  (duck && mysql)
    ? ok("take-away: both download lanes render")
    : bad("take-away: both download lanes render", JSON.stringify(lanes));
  // The drawing and the sentence come from one source, so the rendered tile
  // count must match the word the lane prints. A drawing that outlived its
  // lane is the failure this pins.
  const counts = { 1: "One download", 2: "Two downloads" };
  (duck && counts[duck.tiles] && duck.lead.startsWith(counts[duck.tiles]))
    ? ok("take-away: the DuckDB lane's drawing and its sentence agree")
    : bad("take-away: the DuckDB lane's drawing and its sentence agree", JSON.stringify(duck));
  (mysql && mysql.tiles === 1 && mysql.lead.startsWith("One download"))
    ? ok("take-away: the MySQL lane draws one download and says so")
    : bad("take-away: the MySQL lane draws one download and says so", JSON.stringify(mysql));
  // The views half is a promise this lane does not keep alone: the file comes
  // from the card below the list (#1581), gated on the same capability.
  (duck && duck.views === lanes.views && duck.tiles === (lanes.views ? 2 : 1))
    ? ok("take-away: the views file is offered only when the schema card can produce it")
    : bad("take-away: the views file is offered only when the schema card can produce it", JSON.stringify({ caps: lanes.views, duck: duck }));

  // The GATED arm, fixture-driven through the real builder. The assertions
  // above run on a stack whose views capability is ON, so the one-tile arm --
  // its sentence, its missing button -- is drawn by nothing otherwise. Same
  // shape as sqlxGateOff below: flip the capability, call the builder,
  // restore. Without this, inverting the gate is caught only by the assertion
  // above, and the arm itself is never rendered at all.
  const duckOff = await page.evaluate(() => {
    const b = { configured: true, snapshots: [{ time: "2026-06-10 12:00:00" }] };
    const keep = capsCache.views;
    capsCache.views = false;
    const off = backupDuckLane(b);
    capsCache.views = true;
    const on = backupDuckLane(b);
    capsCache.views = keep;
    const read = (lane) => lane && ({
      tiles: lane.querySelectorAll(".bk-file").length,
      lead: (lane.querySelector(".bk-lane-lead") || {}).textContent || "",
      views: Array.from(lane.querySelectorAll("button")).some((x) => /views\.sql/.test(x.textContent)),
      text: lane.textContent,
    });
    return { off: read(off), on: read(on) };
  });
  (duckOff.off && duckOff.off.tiles === 1 && !duckOff.off.views && duckOff.off.lead.startsWith("One download"))
    ? ok("take-away: with no views capability the lane draws one download and offers no views file")
    : bad("take-away: with no views capability the lane draws one download and offers no views file", JSON.stringify(duckOff.off));
  // And it must say WHY, rather than dressing a console setting as a property
  // of the data: the same folder still yields the file from the command line.
  (duckOff.off && /not offered here/.test(duckOff.off.text) && /decimal columns arrive as text/.test(duckOff.off.text))
    ? ok("take-away: the withheld views file is explained, not implied away")
    : bad("take-away: the withheld views file is explained, not implied away", JSON.stringify(duckOff.off && duckOff.off.text));
  // The on-arm is the vacuousness control: a builder that always drew one
  // tile would pass the off-arm alone.
  (duckOff.on && duckOff.on.tiles === 2 && duckOff.on.views)
    ? ok("take-away: with the capability on, the same builder draws both downloads")
    : bad("take-away: with the capability on, the same builder draws both downloads", JSON.stringify(duckOff.on));

  // The Download-a-DuckDB-schema card itself, which #1581 moved to this page
  // from Connect AI. These are the drawing and budget checks that ran on the
  // Connect scenario while the card lived there; the page changed, the
  // contract did not. Placement is part of the contract: below the list, the
  // card reads as "and this is how you open them" — above it, it shoves the
  // listing (the page's answer) below the fold.
  const dk = await page.evaluate(() => {
    const c = document.querySelector(".view .cn-dk");
    if (!c) return { present: false };
    const kids = Array.from(document.querySelector(".view").children);
    const list = kids.findIndex((n) => /^Backups\b/.test((n.querySelector(".ov-panel-title") || {}).textContent || ""));
    return {
      present: true,
      // The card's own budget (300): enough for a title, one control and a
      // button, and not enough to start explaining. It holds on THIS page or
      // the move quietly re-opened the prose door #1549 closed.
      chars: c.innerText.length,
      tiles: c.querySelectorAll(".dk-shape .dk-tile").length,
      bars: c.querySelectorAll(".dk-shape .dk-bar").length,
      lit: c.querySelectorAll(".dk-shape .dk-part:not(.dk-off)").length,
      belowList: list >= 0 && kids.indexOf(c) > list,
    };
  });
  (dk.present && dk.chars > 0 && dk.chars <= 300)
    ? ok("backups: the DuckDB schema card renders here under its 300-character budget")
    : bad("backups: the DuckDB schema card renders here under its 300-character budget", JSON.stringify(dk));
  (dk.present && dk.tiles > 0 && dk.bars > dk.tiles && dk.lit === 1)
    ? ok("backups: the DuckDB card draws the shape, with the change log unlit until asked for")
    : bad("backups: the DuckDB card draws the shape, with the change log unlit until asked for",
        JSON.stringify({ tiles: dk.tiles, bars: dk.bars, lit: dk.lit }));
  (dk.present && dk.belowList)
    ? ok("backups: the DuckDB card sits below the snapshot listing")
    : bad("backups: the DuckDB card sits below the snapshot listing", JSON.stringify(dk));

  // Paging, EXECUTED through the real panel. The Go guards test the window
  // function and the clamp in isolation and read the call site as text; their
  // COMPOSITION is what a literal page argument breaks, and only rendering
  // catches that. The fixture index carries one snapshot, so the list is
  // handed eight synthetic ones instead.
  const paged = await page.evaluate(() => {
    const snaps = [];
    for (let i = 0; i < 8; i++) snaps.push({ time: "2026-06-1" + i + " 12:00:00", age_hours: i });
    const keep = backupsPage;
    const read = (idx) => {
      backupsPage = { server: currentServer, index: idx };
      const panel = baselinesPanel({ configured: true, snapshots: snaps }, [], {});
      return {
        rows: Array.from(panel.querySelectorAll(".stg-row .stg-name")).map((n) => n.textContent),
        latest: panel.querySelectorAll(".stg-row-latest").length,
        pager: (panel.querySelector(".bk-pager-n") || {}).textContent || "",
      };
    };
    const p0 = read(0), p1 = read(1);
    backupsPage = keep;
    return { p0: p0, p1: p1 };
  });
  (paged.p0.rows.length === 5 && paged.p1.rows.length === 3)
    ? ok("backups paging: five rows a page, and the last page holds the remainder")
    : bad("backups paging: five rows a page, and the last page holds the remainder",
        JSON.stringify({ p0: paged.p0.rows.length, p1: paged.p1.rows.length }));
  // The composition is what a literal page argument breaks: the window has to
  // MOVE. Page two showing page one's rows is the shape that passed every
  // source assertion and both isolated node tests.
  (paged.p1.rows[0] && paged.p1.rows[0] !== paged.p0.rows[0])
    ? ok("backups paging: page two continues the list instead of restarting it")
    : bad("backups paging: page two continues the list instead of restarting it",
        JSON.stringify({ p0first: paged.p0.rows[0], p1first: paged.p1.rows[0] }));
  // The treatment marks the backup a restore reads, so it belongs to the
  // whole list, not to a page.
  (paged.p0.latest === 1 && paged.p1.latest === 0)
    ? ok("backups paging: only the real newest backup wears the treatment")
    : bad("backups paging: only the real newest backup wears the treatment",
        JSON.stringify({ p0: paged.p0.latest, p1: paged.p1.latest }));
  (/^1.5 of 8$/.test(paged.p0.pager) && /^6.8 of 8$/.test(paged.p1.pager))
    ? ok("backups paging: the pager says which rows are on screen")
    : bad("backups paging: the pager says which rows are on screen",
        JSON.stringify({ p0: paged.p0.pager, p1: paged.p1.pager }));
  const sqlxGate = await page.evaluate(async () => {
    const out = { cap: !!capsCache.sql_export };
    const v = document.querySelector(".view");
    const card = Array.from(v.querySelectorAll(".bk-lane")).find((c) =>
      /To load into MySQL/.test((c.querySelector(".bk-lane-t") || {}).textContent || ""));
    out.card = !!card;
    if (!card) return out;
    const input = card.querySelector("input");
    // The lane renders neither input nor Build while a build is running (it
    // hands off to the run region). Say so, rather than dereferencing null
    // and turning a named failure into a stack trace.
    if (!input) { out.running = true; return out; }
    out.prefilled = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(input.value);
    // Before any build the download must refuse with words, not stream bytes.
    const headers = TOKEN ? { Authorization: ["Bearer", TOKEN].join(" ") } : {};
    if (currentServer) headers["X-Bintrail-Server"] = currentServer;
    const res = await fetch("/api/servers/" + encodeURIComponent(currentServer) + "/sql-export/download", { headers });
    out.preStatus = res.status;
    try { out.preBody = (await res.json()).error || ""; } catch (_) { out.preBody = ""; }
    // A bad instant is refused inline, next to the input.
    input.value = "not-a-time";
    Array.from(card.querySelectorAll("button")).find((b) => b.textContent === "Build").click();
    for (let i = 0; i < 40; i++) {
      const msg = card.querySelector(".form-msg.err");
      if (msg && !msg.hidden && msg.textContent) { out.inlineErr = msg.textContent; break; }
      await new Promise((r) => setTimeout(r, 100));
    }
    return out;
  });
  (sqlxGate.cap && sqlxGate.card && sqlxGate.prefilled)
    ? ok("sqlx: the build card renders under its capability, prefilled with the newest backup time")
    : bad("sqlx: the build card renders under its capability, prefilled with the newest backup time", JSON.stringify(sqlxGate));
  (sqlxGate.preStatus === 409 && /build one first/.test(sqlxGate.preBody))
    ? ok("sqlx: downloading before any build refuses with words")
    : bad("sqlx: downloading before any build refuses with words", JSON.stringify({ s: sqlxGate.preStatus, b: sqlxGate.preBody }));
  (sqlxGate.inlineErr && /UTC time/.test(sqlxGate.inlineErr))
    ? ok("sqlx: a bad instant is refused inline")
    : bad("sqlx: a bad instant is refused inline", JSON.stringify(sqlxGate.inlineErr));

  // The gate arms, fixture-driven through the REAL builder (a daemon without
  // the exporter cannot be photographed from this one). The on-arm is the
  // vacuousness control: a builder that always returned null would pass the
  // off-arm alone.
  const sqlxGateOff = await page.evaluate(() => {
    const cur = { id: "srv-fix", kind: "registry" };
    const b = { configured: true, snapshots: [{ time: "2026-06-10 12:00:00" }] };
    const st = { sql_export: { state: "idle" } };
    const keep = capsCache.sql_export;
    capsCache.sql_export = false;
    const off = backupSQLLane(cur, b, st);
    capsCache.sql_export = keep;
    return { off: !!off, on: !!backupSQLLane(cur, b, st) };
  });
  (!sqlxGateOff.off && sqlxGateOff.on)
    ? ok("sqlx: the capability gates the lane (off absent, on present)")
    : bad("sqlx: the capability gates the lane (off absent, on present)", JSON.stringify(sqlxGateOff));

  // The real build. TT_AT sits after every fixture event; the fold reads the
  // baseline AND the index. Poll the status endpoint, not the DOM — the page
  // re-renders itself while the run region is up.
  const sqlxRun = await page.evaluate(async (TT_AT) => {
    const v = document.querySelector(".view");
    const card = Array.from(v.querySelectorAll(".bk-lane")).find((c) =>
      /To load into MySQL/.test((c.querySelector(".bk-lane-t") || {}).textContent || ""));
    if (!card) return { err: "the .sql lane vanished" };
    const input = card.querySelector("input");
    input.value = TT_AT;
    Array.from(card.querySelectorAll("button")).find((b) => b.textContent === "Build").click();
    for (let i = 0; i < 240; i++) {
      await new Promise((r) => setTimeout(r, 500));
      try {
        const st = await api("/api/servers/" + encodeURIComponent(currentServer) + "/sql-export");
        if (st.sql_export && st.sql_export.state !== "running" && st.sql_export.state !== "idle") return st.sql_export;
      } catch (_) { /* transient; keep polling */ }
    }
    return { err: "build never settled" };
  }, TT_AT);
  (sqlxRun.state === "succeeded" && sqlxRun.rows === 3 && sqlxRun.tables === 1 && sqlxRun.bytes > 0)
    ? ok("sqlx: the fold succeeds over the real fixture with the folded row count (1+3+4, not 4 rows) and a byte count")
    : bad("sqlx: the fold succeeds over the real fixture with the folded row count (1+3+4, not 4 rows) and a byte count", JSON.stringify(sqlxRun));

  // Settle: the watcher re-renders when the run finishes; the card must come
  // back wearing the Ready line and a download action.
  await page.waitForFunction(() => {
    const v = document.querySelector(".view");
    if (!v) return false;
    const card = Array.from(v.querySelectorAll(".bk-lane")).find((c) =>
      /To load into MySQL/.test((c.querySelector(".bk-lane-t") || {}).textContent || ""));
    return !!card && /Ready: every table as of/.test(card.textContent) &&
      Array.from(card.querySelectorAll("button")).some((b) => /Download \.sql backup/.test(b.textContent));
  });
  ok("sqlx: after the run settles the card offers the finished build for download");

  // The download wire: gunzip the archive in node and read the actual SQL.
  const sqlxDl = await page.evaluate(async () => {
    const headers = TOKEN ? { Authorization: ["Bearer", TOKEN].join(" ") } : {};
    if (currentServer) headers["X-Bintrail-Server"] = currentServer;
    const res = await fetch("/api/servers/" + encodeURIComponent(currentServer) + "/sql-export/download", { headers });
    const buf = new Uint8Array(await res.arrayBuffer());
    return { status: res.status, cd: res.headers.get("Content-Disposition") || "", bytes: Array.from(buf) };
  });
  let sqlxText = "";
  try { sqlxText = zlib.gunzipSync(Buffer.from(sqlxDl.bytes)).toString("latin1"); } catch (_) { /* asserted below */ }
  (sqlxDl.status === 200 && /dbtrail-sql-.*\.tar\.gz/.test(sqlxDl.cd) && sqlxText.length > 0)
    ? ok("sqlx: the download streams a gzip archive with an attachment name")
    : bad("sqlx: the download streams a gzip archive with an attachment name", JSON.stringify({ s: sqlxDl.status, cd: sqlxDl.cd, n: sqlxDl.bytes.length }));
  (/-schema\.sql/.test(sqlxText) && /CREATE TABLE/.test(sqlxText) && /INSERT INTO/.test(sqlxText) && /metadata/.test(sqlxText))
    ? ok("sqlx: the archive holds loadable SQL — schema, rows and the coordinates file")
    : bad("sqlx: the archive holds loadable SQL — schema, rows and the coordinates file", sqlxText.slice(0, 200));
  (/shipped/.test(sqlxText) && !/b@example\.com/.test(sqlxText) && !/_SUCCESS/.test(sqlxText))
    ? ok("sqlx: the dump carries the FOLDED state (the update landed, the deleted row is gone, the build markers stay out)")
    : bad("sqlx: the dump carries the FOLDED state (the update landed, the deleted row is gone, the build markers stay out)", JSON.stringify({ shipped: /shipped/.test(sqlxText), deleted: /b@example\.com/.test(sqlxText), marker: /_SUCCESS/.test(sqlxText) }));

  // 15f gap arm — the fail-closed contract on the REAL path: stamp a
  // permanent capture gap inside the window, and the build must refuse
  // (AllowGaps is hardwired false at the call site — this is the guard that
  // goes red if anyone ever plumbs it through), the failure must render
  // inline WITHOUT the engine's CLI flag, and the previous artifact must no
  // longer be downloadable (the failed build's wipe revoked it).
  const IDX_DB = process.env.E2E_IDX_DB || "";
  const MYSQL_CTR = process.env.E2E_MYSQL_CONTAINER || "";
  if (IDX_DB && MYSQL_CTR) {
    // STRICTLY inside the window (snapshot, TT_AT]: TT_AT itself sits on the
    // inclusive upper bound, so stamping there keeps working only for as
    // long as that bound stays inclusive — an interior instant does not
    // depend on it.
    const gapAt = new Date(new Date(TT_AT.replace(" ", "T") + "Z").getTime() - 60e3)
      .toISOString().slice(0, 19).replace("T", " ");
    const mysqlIdx = (sql) => {
      try {
        return execSync(
          ["docker", "exec", "-i", MYSQL_CTR, "mysql", "-uroot", "-ptestroot", IDX_DB].join(" "),
          { input: sql, stdio: ["pipe", "pipe", "pipe"] });
      } catch (e) {
        e.message += " :: " + String(e.stderr || "").slice(0, 400);
        throw e;
      }
    };
    try {
      mysqlIdx("INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id, gap_lost_at, gap_lost_detail) " +
        "VALUES (1,'position','binlog.000001',400,NOW(),99,'" + gapAt + "','e2e manufactured gap');");
      const gapRun = await page.evaluate(async (TT_AT) => {
        await api("/api/servers/" + encodeURIComponent(currentServer) + "/sql-export",
          { method: "POST", body: { at: TT_AT } });
        for (let i = 0; i < 240; i++) {
          await new Promise((r) => setTimeout(r, 500));
          try {
            const st = await api("/api/servers/" + encodeURIComponent(currentServer) + "/sql-export");
            if (st.sql_export && st.sql_export.state !== "running") return st.sql_export;
          } catch (_) { /* transient */ }
        }
        return { err: "never settled" };
      }, TT_AT);
      (gapRun.state === "failed" && /capture gap/.test(gapRun.last_error || ""))
        ? ok("sqlx: a stamped capture gap fails the build closed")
        : bad("sqlx: a stamped capture gap fails the build closed", JSON.stringify(gapRun));
      const gapDl = await page.evaluate(async () => {
        const headers = TOKEN ? { Authorization: ["Bearer", TOKEN].join(" ") } : {};
        if (currentServer) headers["X-Bintrail-Server"] = currentServer;
        const res = await fetch("/api/servers/" + encodeURIComponent(currentServer) + "/sql-export/download", { headers });
        return res.status;
      });
      gapDl === 409
        ? ok("sqlx: the failed build revoked the previous artifact (download refuses)")
        : bad("sqlx: the failed build revoked the previous artifact (download refuses)", "status " + gapDl);
      await page.evaluate(() => navigate("baselines"));
      await page.waitForFunction(() => {
        const v = document.querySelector(".view");
        if (!v) return false;
        const card = Array.from(v.querySelectorAll(".bk-lane")).find((c) =>
          /To load into MySQL/.test((c.querySelector(".bk-lane-t") || {}).textContent || ""));
        return !!card && /Last build failed/.test(card.textContent);
      });
      const gapMsg = await page.evaluate(() => {
        const v = document.querySelector(".view");
        const card = Array.from(v.querySelectorAll(".bk-lane")).find((c) =>
          /To load into MySQL/.test((c.querySelector(".bk-lane-t") || {}).textContent || ""));
        return card.textContent;
      });
      (/capture gap/.test(gapMsg) && !/--allow-gaps/.test(gapMsg))
        ? ok("sqlx: the gap refusal renders inline without the engine's CLI flag")
        : bad("sqlx: the gap refusal renders inline without the engine's CLI flag", gapMsg.slice(0, 300));
    } finally {
      // Scenario 15v's verify reads this table; a cleanup failure must be its
      // own loud finding, not a mask over the in-flight one.
      try { mysqlIdx("DELETE FROM stream_state;"); }
      catch (e) { bad("sqlx: gap fixture cleanup failed", String((e && e.message) || e)); }
    }
  } else {
    bad("sqlx: gap arm env missing", "E2E_IDX_DB/E2E_MYSQL_CONTAINER not passed by run.sh");
  }

  // Scenario 15d — Protect group (#1384). Baselines and verification moved off
  // Settings > Storage into their own routes. Two halves must hold TOGETHER,
  // and only the first is obvious: each route renders its panel, AND Storage
  // no longer carries it. A move that left a copy behind would sail past a
  // "does /baselines work" check — which is the shape of the regression 15b
  // caught when this scenario did not exist.
  //
  // waitForFunction rather than a sleep: renderBaselines awaits two fetches,
  // and a fixed delay is the classic way this suite goes intermittently red.
  const protectNav = await page.evaluate(() => ({
    baselines: !!document.querySelector('.nav-item[data-route="baselines"]'),
    verification: !!document.querySelector('.nav-item[data-route="verification"]'),
  }));
  (protectNav.baselines && protectNav.verification)
    ? ok("protect: both nav entries render")
    : bad("protect: both nav entries render", JSON.stringify(protectNav));

  await page.evaluate(() => navigate("baselines"));
  await page.waitForFunction(() => location.pathname === "/baselines"
    && Array.from(document.querySelectorAll(".ov-panel-title")).some((h) => /Backups/.test(h.textContent)));
  ok("protect: /baselines renders the snapshot panel");

  await page.evaluate(() => navigate("verification"));
  await page.waitForFunction(() => location.pathname === "/verification"
    && Array.from(document.querySelectorAll("h1.page-title")).some((h) => /Verification/.test(h.textContent))
    && document.querySelectorAll(".vfy-region").length >= 3);
  ok("protect: /verification renders its three regions");

  // Scenario 15w — the page-header Docs link (#1450). One route → slug table
  // in app.js; the Go asset guard pins that table to the site's real pages,
  // and this leg proves the link is actually painted, follows a ROUTE CHANGE
  // (pageHead reads the route per render, not once at boot), and opens in a
  // new tab without handing the docs site window.opener. Verification is on
  // screen right now, so it is the first probe.
  const docsLinkOf = () => page.evaluate(() => {
    const links = Array.from(document.querySelectorAll(".page-head .page-docs"));
    const a = links[0];
    return { n: links.length, href: a && a.getAttribute("href"), target: a && a.getAttribute("target"),
      rel: a && a.getAttribute("rel"), text: a && a.textContent.trim() };
  });
  const docsVfy = await docsLinkOf();
  docsVfy.n === 1 && docsVfy.href === "https://www.dbtrail.com/docs/guides/verify/" && docsVfy.target === "_blank"
    && /\bnoopener\b/.test(docsVfy.rel || "") && docsVfy.text === "Docs"
    ? ok("docs link: Verification header links to /docs/guides/verify/ in a new tab")
    : bad("docs link: Verification header links to /docs/guides/verify/ in a new tab", JSON.stringify(docsVfy));
  await page.evaluate(() => navigate("events"));
  await page.waitForFunction(() => location.pathname === "/events"
    && Array.from(document.querySelectorAll("h1.page-title")).some((h) => /Events/.test(h.textContent)));
  const docsEvents = await docsLinkOf();
  docsEvents.n === 1 && docsEvents.href === "https://www.dbtrail.com/docs/guides/recovery/"
    ? ok("docs link: follows the route change to Events")
    : bad("docs link: follows the route change to Events", JSON.stringify(docsEvents));
  // Put the page back: 15v below reads the Verification form as it found it.
  await page.evaluate(() => navigate("verification"));
  await page.waitForFunction(() => location.pathname === "/verification"
    && document.querySelectorAll(".vfy-region").length >= 3);

  // Scenario 15v — the verification page rework (#1417/#1418/#1419/#1420),
  // driven END TO END against the real daemon: a real recover-inputs run over
  // the fixture index, then the history it persists.
  //
  // (a) structure + mode help (#1418/#1419): three separated regions; the
  // help swaps with the select and describes the selected mode.
  const vfyStruct = await page.evaluate(() => {
    const regions = document.querySelectorAll(".vfy-region");
    const sel = document.querySelector(".vfy-mode");
    const help = document.querySelector(".vfy-modehelp");
    const helpBefore = help ? help.textContent : "";
    sel.value = "recover-inputs";
    sel.dispatchEvent(new Event("change"));
    return {
      regionCount: regions.length,
      controlTinted: regions[0] ? regions[0].classList.contains("tcard-violet") : false,
      subDescribesAll: !/prove a snapshot still reconstructs/i.test(document.querySelector(".page-sub").textContent),
      helpBefore, helpAfter: help ? help.textContent : "",
      // Measured, not scrollWidth: Chrome reports scrollWidth == clientWidth
      // for a <select> at ANY width (the closed control clips its text and
      // never scrolls; the popup sizes itself) — the first cut of this
      // assertion stayed green with the old 260px cap restored. Render the
      // longest option's text in the select's own font and compare real
      // pixels, leaving room for the chrome.
      selWide: (() => {
        if (!sel) return { ok: false, why: "no select" };
        const cs = getComputedStyle(sel);
        const probe = document.createElement("span");
        probe.style.cssText = "position:absolute;visibility:hidden;white-space:nowrap;font:" + cs.font;
        document.body.appendChild(probe);
        let widest = 0;
        Array.from(sel.options).forEach((o) => {
          probe.textContent = o.textContent;
          widest = Math.max(widest, probe.getBoundingClientRect().width);
        });
        probe.remove();
        return { ok: sel.clientWidth >= widest + 30,
          why: "clientWidth=" + sel.clientWidth + " vs widest+30=" + Math.round(widest + 30) };
      })(),
    };
  });
  (vfyStruct.regionCount >= 3 && vfyStruct.controlTinted)
    ? ok("verification: control / current / history are separate surfaces, control wears the structure tint")
    : bad("verification: control / current / history are separate surfaces, control wears the structure tint", JSON.stringify(vfyStruct));
  (vfyStruct.helpBefore && vfyStruct.helpAfter && vfyStruct.helpBefore !== vfyStruct.helpAfter
    && /never touches your database/.test(vfyStruct.helpAfter))
    ? ok("verification: the mode help swaps with the select and states proof, prerequisite, cost")
    : bad("verification: the mode help swaps with the select and states proof, prerequisite, cost", JSON.stringify({ b: vfyStruct.helpBefore, a: vfyStruct.helpAfter }));
  (vfyStruct.selWide && vfyStruct.selWide.ok)
    ? ok("verification: the mode select no longer truncates its own options")
    : bad("verification: the mode select no longer truncates its own options",
        vfyStruct.selWide ? vfyStruct.selWide.why : "probe missing");

  // (b) the running treatment (#1420), pinned through the REAL renderer with
  // a synthetic in-flight status — the fixture run below is too fast to
  // photograph mid-flight deterministically.
  const vfyRunning = await page.evaluate(() => {
    const tmp = document.createElement("div");
    document.body.appendChild(tmp);
    renderVerifyResults(tmp, { state: "running", mode: "recover-inputs",
      results: [{ schema: "a", table: "b", status: "match" }],
      summary: { match: 1, mismatch: 0, inconclusive: 0, error: 0, total: 1 } }, "x");
    const out = {
      liveChip: !!tmp.querySelector(".chip-live"),
      progress: !!tmp.querySelector(".vfy-progress"),
      soFar: /so far/.test(tmp.textContent),
      noVerdict: !tmp.querySelector(".vfy-verdict-sentence"),
      ageChipDistinct: !tmp.querySelector(".chip-age"),
    };
    tmp.remove();
    return out;
  });
  (vfyRunning.liveChip && vfyRunning.progress && vfyRunning.soFar && vfyRunning.noVerdict)
    ? ok("verification: a running status renders motion, progress-so-far framing, and no early verdict")
    : bad("verification: a running status renders motion, progress-so-far framing, and no early verdict", JSON.stringify(vfyRunning));

  // (c) worst-first ordering (#1419 §3), pinned through the real renderer.
  const vfyOrder = await page.evaluate(() => {
    const tmp = document.createElement("div");
    // Attached to the real view so the rows lay out at page width; a detached
    // node cannot measure overlap.
    document.querySelector(".view").appendChild(tmp);
    renderVerifyResults(tmp, { state: "succeeded", mode: "recover-inputs",
      results: [
        { schema: "s", table: "aaa_clean", status: "match", events_checked: 200000, chains_checked: 100000,
          reason: "checked 200000 change(s) on 100000 row(s); settled 100000 comparison(s) between one change and the next, all matched" },
        { schema: "s", table: "bbb_quiet", status: "inconclusive", inconclusive_kind: "no-activity" },
        { schema: "s", table: "mmm_broken", status: "mismatch", reason: "chain break" },
        { schema: "s", table: "ccc_hard", status: "inconclusive" },
      ],
      summary: { match: 1, mismatch: 1, inconclusive: 2, inconclusive_nothing_to_check: 1, error: 0, total: 4 } }, "x");
    const order = Array.from(tmp.querySelectorAll(".vfy-row .vfy-tbl")).map((n) => n.textContent);
    const verdicts = Array.from(tmp.querySelectorAll(".vfy-row .vfy-verdict")).map((n) => n.textContent);
    // Overflow guard (user report, 2026-08-22): a six-digit counts cell used
    // to paint over the reason beside it. NOT measured with bounding rects:
    // a fixed grid track clamps the BOX to the track width and the text
    // paints outside it as ink, which rects cannot see (the first draft of
    // this guard stayed green with the exact broken CSS restored, twice).
    // scrollWidth vs clientWidth sees both failure shapes: ink overflowing
    // an unclipped cell AND a clipped cell truncating the number.
    const row = Array.from(tmp.querySelectorAll(".vfy-row")).find((r) => r.querySelector(".vfy-counts").textContent);
    const cell = row.querySelector(".vfy-counts");
    const fits = cell.scrollWidth <= cell.clientWidth + 1;
    const countsText = cell.textContent;
    const geom = "scrollWidth=" + cell.scrollWidth + " clientWidth=" + cell.clientWidth;
    tmp.remove();
    return { order, verdicts, fits, countsText, geom };
  });
  (vfyOrder.order[0] === "s.mmm_broken" && vfyOrder.order[3] === "s.aaa_clean"
    && vfyOrder.verdicts.includes("nothing to check"))
    ? ok("verification: rows sort worst-first and the benign verdict is written in words")
    : bad("verification: rows sort worst-first and the benign verdict is written in words", JSON.stringify(vfyOrder));
  (vfyOrder.fits && /200,000 changes/.test(vfyOrder.countsText))
    ? ok("verification: a six-digit counts cell fits its column whole and reads with separators")
    : bad("verification: a six-digit counts cell fits its column whole and reads with separators",
        vfyOrder.geom + " text=" + vfyOrder.countsText);

  // (d) a REAL run end to end: recover-inputs over the fixture index.
  await page.evaluate(() => {
    // Self-contained: leg (a) set the mode, but this leg's premise (a run
    // that reads only the index) must not depend on assertion ordering.
    const sel = document.querySelector(".vfy-mode");
    if (sel.value !== "recover-inputs") { sel.value = "recover-inputs"; sel.dispatchEvent(new Event("change")); }
    document.querySelector(".vfy-run").click();
  });
  await page.waitForFunction(() => document.querySelector(".vfy-results .chip-done") !== null, undefined, { timeout: 60000 });
  const vfyDone = await page.evaluate(() => ({
    verdictSentence: (document.querySelector(".vfy-results .form-hint") || {}).textContent || "",
    rows: document.querySelectorAll(".vfy-results .vfy-row").length,
    failChip: !!document.querySelector(".vfy-results .chip-fail"),
  }));
  (vfyDone.rows > 0 && vfyDone.verdictSentence.length > 0 && !vfyDone.failChip)
    ? ok("verification: a real recover-inputs run completes with structured rows and a verdict sentence")
    : bad("verification: a real recover-inputs run completes with structured rows and a verdict sentence", JSON.stringify(vfyDone));

  // (e) history (#1417): the finished run is a disclosure row that expands to
  // its per-table detail — data the old renderer dropped on the floor — and
  // LAST VERIFIED wears the age treatment, not the live one (#1420).
  await page.waitForFunction(() => document.querySelectorAll(".vfy-histrow").length > 0);
  const vfyHist = await page.evaluate(() => {
    const row = document.querySelector(".vfy-histrow");
    row.click();
    const detail = row.nextElementSibling;
    return {
      expanded: row.getAttribute("aria-expanded") === "true",
      detailRows: detail ? detail.querySelectorAll(".vfy-row").length : 0,
      detailExplainBtns: detail ? Array.from(detail.querySelectorAll("button")).filter((b) => b.textContent === "Explain").length : -1,
      lastChipAge: !!document.querySelector(".vfy-history .chip-age"),
      lastChipNotLive: !document.querySelector(".vfy-history .chip-live"),
    };
  });
  (vfyHist.expanded && vfyHist.detailRows > 0 && vfyHist.detailExplainBtns === 0)
    ? ok("verification: a history row expands to per-table detail, with no dead Explain buttons")
    : bad("verification: a history row expands to per-table detail, with no dead Explain buttons", JSON.stringify(vfyHist));
  // The dead-button rule pinned where the fixture cannot: recover-inputs
  // results are never explainable, so the real history above holds this
  // assertion vacuously. A synthetic explainable mismatch through the REAL
  // renderer is the only shape that can see the {history:true} suppression —
  // the mutation dropping it stayed green against the fixture alone.
  const vfyDeadBtn = await page.evaluate(() => {
    const mk = (opts) => {
      const tmp = document.createElement("div");
      document.body.appendChild(tmp);
      renderVerifyResults(tmp, { state: "succeeded", mode: "baseline-anchored",
        results: [{ schema: "s", table: "t", status: "mismatch", reason: "digest differs", explainable: true }],
        summary: { match: 0, mismatch: 1, inconclusive: 0, error: 0, total: 1 } }, "x", opts);
      const n = Array.from(tmp.querySelectorAll("button")).filter((b) => b.textContent === "Explain").length;
      tmp.remove();
      return n;
    };
    // The third leg is the derivation: a RECORD (trigger present) with the
    // option forgotten must still suppress the button — this is what makes
    // the call-site mutation structurally impossible.
    const tmp = document.createElement("div");
    document.body.appendChild(tmp);
    renderVerifyResults(tmp, { state: "succeeded", mode: "baseline-anchored", trigger: "manual",
      results: [{ schema: "s", table: "t", status: "mismatch", reason: "digest differs", explainable: true }],
      summary: { match: 0, mismatch: 1, inconclusive: 0, error: 0, total: 1 } }, "x");
    const derived = Array.from(tmp.querySelectorAll("button")).filter((b) => b.textContent === "Explain").length;
    tmp.remove();
    return { live: mk(undefined), history: mk({ history: true }), derived };
  });
  (vfyDeadBtn.live === 1 && vfyDeadBtn.history === 0 && vfyDeadBtn.derived === 0)
    ? ok("verification: Explain renders on a live run and never on a history record — even when the option is forgotten")
    : bad("verification: Explain renders on a live run and never on a history record — even when the option is forgotten", JSON.stringify(vfyDeadBtn));
  (vfyHist.lastChipAge && vfyHist.lastChipNotLive)
    ? ok("verification: LAST VERIFIED wears the age treatment, distinct from RUNNING")
    : bad("verification: LAST VERIFIED wears the age treatment, distinct from RUNNING", JSON.stringify(vfyHist));

  // Storage became Retention (#1543). Navigating to the OLD route is the
  // check: it must land on the new page with the URL rewritten, or every
  // existing link and bookmark drops the operator on Overview.
  await page.evaluate(() => navigate("storage"));
  await page.waitForFunction(() => location.pathname === "/retention"
    && Array.from(document.querySelectorAll(".ov-panel-title")).some((h) => /S3 archiving/.test(h.textContent)));
  const storageTitles = await page.evaluate(() =>
    Array.from(document.querySelectorAll(".ov-panel-title")).map((h) => h.textContent));
  (!storageTitles.some((t) => /Baseline snapshots|Backups|Verification/.test(t)))
    ? ok("protect: the old /storage link lands on Retention, without the moved panels")
    : bad("protect: the old /storage link lands on Retention, without the moved panels", JSON.stringify(storageTitles));

  // The split itself: each half holds only its own concern, photographed
  // rather than grepped. Retention must NOT carry the daemon's cards.
  const halves = await page.evaluate(async () => {
    const titles = () => Array.from(document.querySelectorAll(".card-title")).map((h) => h.textContent);
    const retention = titles();
    navigate("daemon");
    await new Promise((r) => setTimeout(r, 1200));
    return { retention: retention, daemon: titles(), path: location.pathname };
  });
  (halves.path === "/daemon"
    && !halves.retention.some((t) => /AWS credentials|Usage telemetry|File reuse/.test(t))
    && halves.daemon.some((t) => /AWS credentials/.test(t))
    && halves.daemon.some((t) => /Usage telemetry/.test(t))
    && !halves.daemon.some((t) => /Rotation/.test(t)))
    ? ok("storage split: Retention holds the data lifecycle, This daemon holds the process")
    : bad("storage split: Retention holds the data lifecycle, This daemon holds the process", JSON.stringify(halves));

  // Scenario 15e — motion that cannot be seen from Go (#1385).
  //
  // Both of these shipped BROKEN and a text-grep guard in the Go suite passed
  // them. That is the whole reason they live here: both are CASCADE-PRECEDENCE
  // bugs, a class no source-text assertion can observe. ci.yml says as much
  // about this suite — "go test never renders assets/*, so CSS/DOM/presentation
  // bugs are invisible to the Go suite".
  //
  //  1. `animation: … both` filled `transform: none` forward, and animations
  //     outrank normal author declarations, so `.ov-stat:hover { transform }`
  //     never applied — measured matrix(1,0,0,1,0,0). The box-shadow half DID
  //     apply, so it looked half-alive rather than broken.
  //  2. Re-declaring the `transition` SHORTHAND on `.nav-item .ni-icon` reset
  //     transition-property to `transform` alone, so the icon colour snapped.
  await page.evaluate(() => navigate("overview"));
  await page.waitForSelector(".ov-stat", { timeout: 10000 });
  // Let the entrance finish first: a RUNNING animation outranks the author rule
  // too, so hovering early measures the animation rather than the bug.
  await page.waitForTimeout(700);
  await page.hover(".ov-stat");
  await page.waitForTimeout(400); // the lift transitions over .22s
  const lift = await page.evaluate(() => getComputedStyle(document.querySelector(".ov-stat")).transform);
  // translateY(-3px) is the last component of the 2D matrix.
  /-3\)$/.test(lift.replace(/\s/g, ""))
    ? ok("motion: hovering a stat tile actually lifts it")
    : bad("motion: hovering a stat tile actually lifts it",
        `${lift} — an animation filling transform forward outranks the :hover rule`);

  const iconTransition = await page.evaluate(() => {
    const icon = document.querySelector(".nav-item .ni-icon");
    return icon ? getComputedStyle(icon).transitionProperty : "(no icon)";
  });
  (iconTransition.includes("color") && iconTransition.includes("transform"))
    ? ok("motion: the nav icon still transitions colour as well as transform")
    : bad("motion: the nav icon still transitions colour as well as transform",
        `${iconTransition} — re-declaring the transition shorthand resets transition-property`);

  // Scenario 16 — Restore Table combobox (#1364). The Table field must be an
  // input + datalist fed from /api/schemas?schema=… (the same endpoint the
  // events table picker consumes): suggestions from the fixture's tables,
  // swapped on a schema switch, a stale value cleared on switch — and still
  // free text, because recover legitimately targets dropped tables whose
  // events are indexed (that half is pinned in scenario 17's typed-submit leg).
  await page.evaluate(() => navigate("recover"));
  await page.waitForSelector("#recover-form", { timeout: 8000 });
  await page.waitForFunction((FIX) => {
    const f = document.getElementById("recover-form");
    return f && Array.from(f.elements.schema.options).some((o) => o.value === FIX);
  }, FIX, { timeout: 8000 });
  const combo = await page.evaluate(async (FIX) => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const f = document.getElementById("recover-form");
    const input = f.elements.table;
    const listId = input.getAttribute("list");
    const dl = listId ? document.getElementById(listId) : null;
    const read = () => (dl ? Array.from(dl.options).map((o) => o.value).sort() : []);
    const hintText = () => (f.querySelector(".combo-hint") || {}).textContent || "";
    const waitFor = async (pred) => { for (let i = 0; i < 80 && !pred(); i++) await sleep(50); return pred(); };
    const pick = (schema) => { f.elements.schema.value = schema; f.elements.schema.dispatchEvent(new Event("change")); };

    // Hint geometry (#1369): the hint is out of flow, so with the hint EMPTY
    // the combo field's box must match its siblings (under .filters'
    // align-items:flex-end an in-flow empty hint pushed the label+input above
    // the row), and SHOWING the hint must not change the field's box. Deltas
    // are relative to the schema field so the view-enter animation (which
    // translates the whole filter panel) can't skew a comparison across time.
    const geom = () => {
      const c = input.closest(".field").getBoundingClientRect();
      const s = f.elements.schema.closest(".field").getBoundingClientRect();
      return { dTop: c.top - s.top, comboH: c.height, sibH: s.height };
    };
    const geomEmpty = geom(); // fresh form: hint is empty

    // The dropped-table flow: a name typed BEFORE the first schema selection
    // must survive that selection — clearing here would make the one recovery
    // a closed <select> can't do impossible from this combobox too.
    input.value = "preseeded_tbl";
    pick(FIX);
    await waitFor(() => read().length > 0);
    const tablesA = read();
    const preseededKept = input.value === "preseeded_tbl";

    // A value from the old schema not in the new listing: suggestions must
    // swap and the stale value must clear (never silently kept).
    input.value = "child";
    pick("e2estock");
    await waitFor(() => read().length > 0 && read().join() !== tablesA.join());
    const tablesB = read();
    const clearedOnSwitch = input.value === "";

    // The keep-arm: "orders" exists in BOTH schemas (shared fixture name), so
    // switching must KEEP it — an unconditional clear-on-switch fails here.
    pick(FIX);
    await waitFor(() => read().join() === tablesA.join());
    input.value = "orders";
    pick("e2estock");
    await waitFor(() => read().join() === tablesB.join());
    const sharedKept = input.value === "orders";

    // A → "— select —" → B must still clear: the empty option must not reset
    // the switch marker and launder B into a "first selection".
    pick(FIX);
    await waitFor(() => read().join() === tablesA.join());
    input.value = "parent";
    pick("");
    await sleep(120);
    const keptOnEmpty = input.value === "parent"; // deselecting alone never clears
    pick("e2estock");
    await waitFor(() => read().join() === tablesB.join());
    const clearedViaEmpty = input.value === "";

    // A FAILED listing: value intact, field usable, aria-busy gone, and the
    // persistent aria-live hint says to type the name (the toast is 2.2s).
    const realFetch = window.fetch;
    window.fetch = (p, o) => (typeof p === "string" && p.startsWith("/api/schemas")
      ? Promise.resolve(new Response('{"error":"boom"}', { status: 500, headers: { "Content-Type": "application/json" } }))
      : realFetch(p, o));
    tablesCache.delete(FIX);
    input.value = "typed_under_failure";
    pick(FIX);
    await waitFor(() => /type the table name/.test(hintText()));
    const geomHint = geom(); // the persistent failure note is showing
    const failed = {
      valueKept: input.value === "typed_under_failure",
      enabled: !input.disabled,
      ariaBusyGone: !input.hasAttribute("aria-busy"),
      hint: hintText(),
    };
    window.fetch = realFetch;
    // ...and a submit still reaches the backend despite the failed listing.
    document.getElementById("recover-out").replaceChildren();
    f.elements.pk.value = "";
    f.elements.since.value = ""; f.elements.until.value = "";
    f.requestSubmit();
    await waitFor(() => !!document.querySelector("#recover-out #sql-panel") && !document.querySelector("#modal .busy-modal"));
    const failedSubmit = !!document.querySelector("#recover-out #sql-panel");
    return {
      isInput: input.tagName === "INPUT",
      hasList: !!dl,
      hasHint: !!f.querySelector(".combo-hint"),
      disabled: input.disabled,
      preseededKept, tablesA, tablesB, clearedOnSwitch, sharedKept,
      keptOnEmpty, clearedViaEmpty, failed, failedSubmit,
      geomEmpty, geomHint,
    };
  }, FIX);
  (Math.abs(combo.geomEmpty.dTop) <= 1 && Math.abs(combo.geomEmpty.comboH - combo.geomEmpty.sibH) <= 1)
    ? ok("restore combo: empty hint reserves no height — field box matches its siblings (#1369)")
    : bad("restore combo: empty hint reserves no height — field box matches its siblings (#1369)", JSON.stringify(combo.geomEmpty));
  (Math.abs(combo.geomHint.dTop) <= 1 && Math.abs(combo.geomHint.comboH - combo.geomEmpty.comboH) <= 1)
    ? ok("restore combo: a showing hint never changes the field box or moves the row (#1369)")
    : bad("restore combo: a showing hint never changes the field box or moves the row (#1369)", JSON.stringify({ empty: combo.geomEmpty, hint: combo.geomHint }));
  (combo.isInput && combo.hasList)
    ? ok("restore combo: Table is an input+datalist combobox, not a closed select")
    : bad("restore combo: Table is an input+datalist combobox, not a closed select", JSON.stringify(combo));
  combo.preseededKept
    ? ok("restore combo: a name typed before the first schema selection survives it")
    : bad("restore combo: a name typed before the first schema selection survives it", "first listing load cleared the typed value");
  combo.tablesA.join(",") === "child,orders,parent"
    ? ok("restore combo: selecting a schema populates suggestions from its listing")
    : bad("restore combo: selecting a schema populates suggestions from its listing", JSON.stringify(combo.tablesA));
  combo.tablesB.join(",") === "inventory,orders"
    ? ok("restore combo: switching schemas swaps the suggestions")
    : bad("restore combo: switching schemas swaps the suggestions", JSON.stringify(combo.tablesB));
  combo.clearedOnSwitch
    ? ok("restore combo: a stale table value is cleared on schema switch")
    : bad("restore combo: a stale table value is cleared on schema switch", "kept the previous schema's table");
  combo.sharedKept
    ? ok("restore combo: a value present in the NEW schema's listing survives the switch")
    : bad("restore combo: a value present in the NEW schema's listing survives the switch", "cleared a value that belongs to the new schema");
  (combo.keptOnEmpty && combo.clearedViaEmpty)
    ? ok("restore combo: routing a switch through '— select —' still clears the stale value")
    : bad("restore combo: routing a switch through '— select —' still clears the stale value", JSON.stringify({ keptOnEmpty: combo.keptOnEmpty, clearedViaEmpty: combo.clearedViaEmpty }));
  (combo.failed.valueKept && combo.failed.enabled && combo.failed.ariaBusyGone)
    ? ok("restore combo: a failed listing keeps the value and the field usable")
    : bad("restore combo: a failed listing keeps the value and the field usable", JSON.stringify(combo.failed));
  /type the table name/.test(combo.failed.hint)
    ? ok("restore combo: a failed listing shows the persistent aria-live hint")
    : bad("restore combo: a failed listing shows the persistent aria-live hint", JSON.stringify(combo.failed.hint));
  combo.failedSubmit
    ? ok("restore combo: a submit still reaches the backend after a failed listing")
    : bad("restore combo: a submit still reaches the backend after a failed listing", "no reversal panel rendered");
  !combo.disabled
    ? ok("restore combo: the field is never disabled")
    : bad("restore combo: the field is never disabled", "input.disabled");

  // Scenario 17 — Generate-undo busy modal (#1363). A slowed /api/recover must
  // open the modal immediately (dialog semantics, facts stated, button
  // disabled), block a second click, close on completion with focus on the
  // reversal.sql header; Cancel and ESC must abort the fetch and restore the
  // pre-click state; an error must render IN the modal, then dismiss cleanly.
  // The stub honors AbortController like real fetch. That alone proves
  // nothing — teardown is synchronous, so the modal closes either way; the
  // wiring is proven by leg B sleeping PAST the stub's delay after Cancel:
  // if api() dropped opts.signal, the un-aborted response lands, renders a
  // panel and moves focus, and the post-delay assertions fail.
  const busyRun = await page.evaluate(async (FIX) => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const f = document.getElementById("recover-form");
    const genBtn = f.querySelector('button[type="submit"]');
    const prevBtn = Array.from(f.querySelectorAll(".filter-actions button")).find((b) => b.textContent === "Preview rows");
    const modalSel = () => document.querySelector("#modal .busy-modal");
    const realFetch = window.fetch;
    const calls = [];
    let mode = "delay", delay = 800;
    window.fetch = (p, o) => {
      if (!(typeof p === "string" && p.startsWith("/api/recover"))) return realFetch(p, o);
      calls.push(o && o.body ? String(o.body) : "");
      if (mode === "error") {
        return Promise.resolve(new Response('{"error":"the index server went away mid-read — check the connection and retry"}',
          { status: 500, headers: { "Content-Type": "application/json" } }));
      }
      return new Promise((resolve, reject) => {
        const t = setTimeout(() => resolve(realFetch(p, o)), delay);
        if (o && o.signal) o.signal.addEventListener("abort", () => { clearTimeout(t); reject(new DOMException("Aborted", "AbortError")); });
      });
    };
    try {
      // A) slow generate: modal + facts + disabled button + second click inert.
      f.elements.schema.value = FIX;
      f.elements.table.value = "orders";
      f.elements.pk.value = "1";
      f.elements.since.value = ""; f.elements.until.value = "";
      document.getElementById("recover-out").replaceChildren();
      genBtn.focus(); genBtn.click();
      await sleep(120);
      const m = modalSel();
      const open = {
        present: !!m,
        role: m ? m.getAttribute("role") : "",
        busyAttr: m ? m.getAttribute("aria-busy") : "",
        facts: m ? m.textContent : "",
        btnDisabled: genBtn.disabled,
        prevDisabled: prevBtn ? prevBtn.disabled : false,
        focusInModal: m ? m.contains(document.activeElement) : false,
      };
      genBtn.click();    // disabled click — inert
      f.requestSubmit(); // a keyboard/programmatic submit must hit the re-entry guard
      await sleep(80);
      const secondBlocked = calls.length === 1;
      for (let i = 0; i < 100 && modalSel(); i++) await sleep(50);
      const done = {
        modalGone: !modalSel(),
        panel: !!document.querySelector("#recover-out #sql-panel"),
        calls: calls.length,
        btnEnabled: !genBtn.disabled,
        focusOnResult: !!document.activeElement && document.activeElement.classList.contains("code-head"),
      };

      // B) Cancel restores the pre-click state — and the abort is PROVEN by
      // sleeping past the stub's short delay afterwards: an un-aborted fetch
      // resolves then, renders a panel and moves focus, failing below.
      document.getElementById("recover-out").replaceChildren();
      delay = 600;
      genBtn.focus(); genBtn.click();
      await sleep(120);
      const cbtn = Array.from((modalSel() || document.createElement("i")).querySelectorAll("button")).find((b) => b.textContent === "Cancel");
      if (cbtn) cbtn.click();
      await sleep(150);
      const afterCancel = {
        modalGone: !modalSel(),
        noPanel: !document.querySelector("#recover-out #sql-panel"),
        btnEnabled: !genBtn.disabled,
        focusBack: document.activeElement === genBtn,
        calls: calls.length, // 2: the canceled call fired once, never retried
      };
      await sleep(900); // PAST the stub's delay
      afterCancel.noPanelAfterDelay = !document.querySelector("#recover-out #sql-panel");
      afterCancel.focusStillBack = document.activeElement === genBtn;

      // B2) ESC aborts the same way (same past-delay proof).
      genBtn.focus(); genBtn.click();
      await sleep(120);
      const escOpened = !!modalSel();
      document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
      await sleep(150);
      const afterEsc = { opened: escOpened, modalGone: !modalSel(), btnEnabled: !genBtn.disabled, noPanel: !document.querySelector("#recover-out #sql-panel") };
      await sleep(900); // PAST the stub's delay
      afterEsc.noPanelAfterDelay = !document.querySelector("#recover-out #sql-panel");

      // C) an error renders IN the modal — and a FAILED generation must not
      // leave the PREVIOUS run's script on screen with Download live (those
      // bytes answer a filter nobody named). Seed a successful result first,
      // then fail the next generation and assert the stale panel and the
      // download buffer are gone.
      mode = "delay"; delay = 0;
      genBtn.focus(); genBtn.click();
      for (let i = 0; i < 100 && (modalSel() || !document.querySelector("#recover-out #sql-panel")); i++) await sleep(50);
      const seeded = { panel: !!document.querySelector("#recover-out #sql-panel"), sql: lastSQL.length > 0 };
      mode = "error";
      f.elements.pk.value = "2"; // a DIFFERENT filter than the seeded script answers
      genBtn.focus(); genBtn.click();
      await sleep(200);
      const em = modalSel();
      const errShown = {
        seeded,
        stillOpen: !!em,
        busyAttr: em ? em.getAttribute("aria-busy") : "",
        text: em ? em.textContent : "",
        stalePanelCleared: !document.querySelector("#recover-out #sql-panel"),
        staleSQLCleared: lastSQL === "",
      };
      const dismiss = em ? Array.from(em.querySelectorAll("button")).find((b) => b.textContent === "Dismiss") : null;
      if (dismiss) dismiss.click();
      await sleep(80);
      const afterErr = { modalGone: !modalSel(), noPanel: !document.querySelector("#recover-out #sql-panel"), btnEnabled: !genBtn.disabled };

      // D) a hand-typed table NOT in the listing still submits (#1364) and
      // reaches the backend — dropped tables with indexed events are a
      // legitimate recover target.
      mode = "delay"; delay = 0;
      f.elements.table.value = "vanished_tbl";
      f.elements.pk.value = "";
      genBtn.focus(); genBtn.click();
      for (let i = 0; i < 100 && modalSel(); i++) await sleep(50);
      const typed = {
        submitted: calls.some((c) => c.includes('"table":"vanished_tbl"')),
        panel: !!document.querySelector("#recover-out #sql-panel"),
      };

      // E) Preview rows adopts the same busy mechanism (same latency source).
      window.fetch = (p, o) => {
        if (!(typeof p === "string" && p.startsWith("/api/events"))) return realFetch(p, o);
        return new Promise((resolve, reject) => {
          const t = setTimeout(() => resolve(realFetch(p, o)), 700);
          if (o && o.signal) o.signal.addEventListener("abort", () => { clearTimeout(t); reject(new DOMException("Aborted", "AbortError")); });
        });
      };
      f.elements.table.value = "orders";
      f.elements.pk.value = "1";
      prevBtn.focus(); prevBtn.click();
      await sleep(120);
      const pm = modalSel();
      const previewBusy = { modalOpen: !!pm, title: pm ? pm.textContent : "" };
      for (let i = 0; i < 100 && modalSel(); i++) await sleep(50);
      const previewDone = {
        modalGone: !modalSel(),
        rendered: !!document.querySelector("#recover-preview .events"),
        btnEnabled: !prevBtn.disabled,
      };
      window.fetch = (p, o) => {
        if (!(typeof p === "string" && p.startsWith("/api/recover"))) return realFetch(p, o);
        calls.push(o && o.body ? String(o.body) : "");
        return new Promise((resolve, reject) => {
          const t = setTimeout(() => resolve(realFetch(p, o)), delay);
          if (o && o.signal) o.signal.addEventListener("abort", () => { clearTimeout(t); reject(new DOMException("Aborted", "AbortError")); });
        });
      };

      // F) the ⌘K palette stacks ABOVE the dialog in its own mount and owns
      // its keys: Escape with the palette open must close the palette ONLY —
      // neither the modal's capture trap (which would beat the palette
      // input's own handler to the event) nor globalKeydown's generic
      // #modal-emptying fall-through (the same Escape that closed the
      // palette bubbles on) may touch the busy dialog.
      delay = 2000;
      f.elements.table.value = "orders"; f.elements.pk.value = "1";
      genBtn.focus(); genBtn.click();
      await sleep(100);
      const cmdkMount = document.getElementById("cmdk-mount");
      document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "k", metaKey: true, bubbles: true, cancelable: true }));
      await sleep(100);
      const paletteOpened = !!cmdkMount.firstChild;
      document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
      await sleep(100);
      const cmdkLeg = {
        paletteOpened,
        paletteClosed: !cmdkMount.firstChild,
        modalSurvived: !!modalSel(),
      };
      document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
      await sleep(150);
      cmdkLeg.canceledAfter = !modalSel();
      cmdkLeg.btnEnabled = !genBtn.disabled;

      // G) another dialog CLOBBERING the shared #modal slot (openServersModal
      // replaces its children without our teardown) must dissolve the trap on
      // the next keystroke: buttons re-enabled, re-entry flag reset, and the
      // occupying dialog left alone.
      delay = 1200;
      document.getElementById("recover-out").replaceChildren();
      genBtn.focus(); genBtn.click();
      await sleep(100);
      const clobberBefore = !!modalSel();
      openServersModal();
      for (let i = 0; i < 60 && !document.getElementById("servers-list"); i++) await sleep(50);
      const busyGone = !modalSel();
      // The click path can arrive before any keystroke: the re-entry guard
      // must self-heal on read when the flag is set but no .busy-modal is in
      // the slot.
      const selfHealed = !busyModalActive() && !busyModalOpen;
      document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "Tab", bubbles: true, cancelable: true }));
      await sleep(80);
      const clobberLeg = {
        before: clobberBefore,
        busyGone,
        selfHealed,
        btnEnabled: !genBtn.disabled,
        flagReset: !busyModalOpen,
        serversIntact: !!document.getElementById("servers-list"),
      };
      closeServersModal();
      // Let the orphaned (never-aborted) request land before the next leg.
      for (let i = 0; i < 100 && !document.querySelector("#recover-out #sql-panel"); i++) await sleep(50);

      // H) a server switch mid-flight closes the dialog and renders nothing.
      delay = 700;
      document.getElementById("recover-out").replaceChildren();
      genBtn.focus(); genBtn.click();
      await sleep(100);
      serverGen++;
      for (let i = 0; i < 60 && modalSel(); i++) await sleep(50);
      const switchLeg = {
        modalGone: !modalSel(),
        noPanel: !document.querySelector("#recover-out #sql-panel"),
        btnEnabled: !genBtn.disabled,
      };
      return { open, secondBlocked, done, afterCancel, afterEsc, errShown, afterErr, typed, previewBusy, previewDone, cmdkLeg, clobberLeg, switchLeg };
    } finally {
      window.fetch = realFetch;
    }
  }, FIX);
  (busyRun.open.present && busyRun.open.role === "dialog" && busyRun.open.busyAttr === "true")
    ? ok("busy modal: opens immediately as an aria-busy dialog")
    : bad("busy modal: opens immediately as an aria-busy dialog", JSON.stringify(busyRun.open));
  (busyRun.open.facts.includes(FIX + ".orders") && /pk/.test(busyRun.open.facts))
    ? ok("busy modal: states what is being generated (target + pk)")
    : bad("busy modal: states what is being generated (target + pk)", busyRun.open.facts.slice(0, 200));
  (busyRun.open.btnDisabled && busyRun.open.prevDisabled)
    ? ok("busy modal: the Restore action buttons are disabled while open")
    : bad("busy modal: the Restore action buttons are disabled while open", JSON.stringify(busyRun.open));
  busyRun.open.focusInModal
    ? ok("busy modal: keyboard focus moves into the dialog")
    : bad("busy modal: keyboard focus moves into the dialog", "focus stayed outside");
  busyRun.secondBlocked
    ? ok("busy modal: a second click/submit queues no second generation")
    : bad("busy modal: a second click/submit queues no second generation", "a second /api/recover fired");
  (busyRun.done.modalGone && busyRun.done.panel && busyRun.done.calls === 1)
    ? ok("busy modal: closes on completion with the reversal panel rendered (one call total)")
    : bad("busy modal: closes on completion with the reversal panel rendered (one call total)", JSON.stringify(busyRun.done));
  (busyRun.done.btnEnabled && busyRun.done.focusOnResult)
    ? ok("busy modal: success re-enables the button and focuses the reversal.sql header")
    : bad("busy modal: success re-enables the button and focuses the reversal.sql header", JSON.stringify(busyRun.done));
  (busyRun.afterCancel.modalGone && busyRun.afterCancel.noPanel && busyRun.afterCancel.btnEnabled && busyRun.afterCancel.calls === 2)
    ? ok("busy modal: Cancel aborts the fetch and restores the pre-click state")
    : bad("busy modal: Cancel aborts the fetch and restores the pre-click state", JSON.stringify(busyRun.afterCancel));
  (busyRun.afterCancel.noPanelAfterDelay && busyRun.afterCancel.focusStillBack)
    ? ok("busy modal: the abort actually reaches the fetch (nothing renders after the stub's delay)")
    : bad("busy modal: the abort actually reaches the fetch (nothing renders after the stub's delay)", JSON.stringify(busyRun.afterCancel));
  busyRun.afterCancel.focusBack
    ? ok("busy modal: Cancel restores focus to the Generate button")
    : bad("busy modal: Cancel restores focus to the Generate button", "focus elsewhere");
  (busyRun.afterEsc.opened && busyRun.afterEsc.modalGone && busyRun.afterEsc.btnEnabled && busyRun.afterEsc.noPanel && busyRun.afterEsc.noPanelAfterDelay)
    ? ok("busy modal: ESC aborts and restores like Cancel")
    : bad("busy modal: ESC aborts and restores like Cancel", JSON.stringify(busyRun.afterEsc));
  (busyRun.errShown.stillOpen && busyRun.errShown.busyAttr === "false" && /went away mid-read/.test(busyRun.errShown.text))
    ? ok("busy modal: an error renders the server's actionable text IN the modal")
    : bad("busy modal: an error renders the server's actionable text IN the modal", JSON.stringify(busyRun.errShown).slice(0, 300));
  (busyRun.errShown.seeded.panel && busyRun.errShown.seeded.sql && busyRun.errShown.stalePanelCleared && busyRun.errShown.staleSQLCleared)
    ? ok("busy modal: a failed generation clears the previous script and its download buffer")
    : bad("busy modal: a failed generation clears the previous script and its download buffer", JSON.stringify(busyRun.errShown).slice(0, 300));
  (busyRun.afterErr.modalGone && busyRun.afterErr.noPanel && busyRun.afterErr.btnEnabled)
    ? ok("busy modal: Dismiss after an error returns to the pre-click state")
    : bad("busy modal: Dismiss after an error returns to the pre-click state", JSON.stringify(busyRun.afterErr));
  (busyRun.typed.submitted && busyRun.typed.panel)
    ? ok("restore combo: a typed table not in the listing still submits end-to-end")
    : bad("restore combo: a typed table not in the listing still submits end-to-end", JSON.stringify(busyRun.typed));

  // ── persistent failure notices (#1381) ───────────────────────────────────
  // A failure notice that fades is a failure nobody saw. The baseline privilege
  // refusal is ~550 characters of remediation, and at the old 2.2s auto-hide it
  // was unreadable and unrecoverable — it was reported from a screenshot caught
  // by luck. These assertions exist so a re-added setTimeout, or an ESC handler
  // that stops discriminating, cannot ship silently.
  const toastRun = await page.evaluate(async () => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const t = document.getElementById("toast-error");
    const transient = document.getElementById("toast");
    const shown = () => !t.hidden;

    toastError("Refusal one: grant LOCK TABLES.");
    const immediately = shown();
    // Comfortably past the 2.2s auto-hide a transient toast would have used.
    await sleep(2600);
    const survivesAutoHide = shown();

    // A success notice must not silently delete an unread failure — and must
    // itself still render, which the shared-node version could not guarantee.
    toast("Baseline started…");
    const survivesSuccess = shown() && /Refusal one/.test(t.textContent);
    const successStillRenders = !transient.hidden && /Baseline started/.test(transient.textContent);

    // A second failure stacks rather than overwrites.
    toastError("Refusal two: BACKUP_ADMIN cannot be granted on RDS.");
    const stacked = /Refusal one/.test(t.textContent) && /Refusal two/.test(t.textContent);

    // ESC belongs to an open dialog first. Put something in the shared modal
    // slot and confirm the notice survives that Escape.
    const modalMount = document.getElementById("modal");
    modalMount.append(document.createElement("div"));
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await sleep(50);
    const survivesModalEsc = shown();
    modalMount.replaceChildren();

    // Third door: the date picker renders into document.body, not #modal, and
    // closes itself on ESC without stopping propagation. Enumerating #modal and
    // #cmdk-mount was not enough — this is the case that got through.
    const pop = document.createElement("div");
    pop.className = "dt-pop";
    document.body.append(pop);
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await sleep(50);
    const survivesDatePickerEsc = shown();
    pop.remove();

    // With nothing else open, ESC dismisses it.
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await sleep(50);
    const escDismisses = t.hidden;

    // And so does the close button.
    toastError("Refusal three.");
    const closeBtn = t.querySelector(".toast-close");
    if (closeBtn) closeBtn.click();
    await sleep(50);
    // Only `hidden` decides visibility: #toast-error carries the toast-error
    // class permanently in index.html now that failures have their own node,
    // so asserting the class is absent would assert the markup is wrong.
    const closeDismisses = t.hidden && t.textContent === "";

    // A repeat of a message already showing is counted, not dropped: several
    // failures carry no server name, so a second one would otherwise change
    // nothing on screen and read as success.
    toastError("Refusal four.");
    toastError("Refusal four.");
    const counted = /\(×2\)/.test(t.textContent);
    dismissToast();

    // A success notice still fades on its own.
    toast("Done");
    const successShown = !transient.hidden;
    await sleep(2600);
    return {
      immediately, survivesAutoHide, survivesSuccess, successStillRenders, stacked, counted,
      survivesModalEsc, survivesDatePickerEsc, escDismisses, closeDismisses,
      successShown, successFaded: transient.hidden,
    };
  });

  (toastRun.immediately && toastRun.survivesAutoHide)
    ? ok("toast: a failure notice is still on screen well past the transient auto-hide")
    : bad("toast: a failure notice is still on screen well past the transient auto-hide", JSON.stringify(toastRun));
  (toastRun.survivesSuccess && toastRun.successStillRenders)
    ? ok("toast: a success notice neither overwrites an unread failure nor is suppressed by it")
    : bad("toast: a success notice neither overwrites an unread failure nor is suppressed by it", JSON.stringify(toastRun));
  toastRun.counted
    ? ok("toast: a repeated failure is counted rather than silently dropped")
    : bad("toast: a repeated failure is counted rather than silently dropped", JSON.stringify(toastRun));
  toastRun.stacked
    ? ok("toast: concurrent failures stack instead of replacing each other")
    : bad("toast: concurrent failures stack instead of replacing each other", JSON.stringify(toastRun));
  (toastRun.survivesModalEsc && toastRun.survivesDatePickerEsc)
    ? ok("toast: an ESC meant for an open dialog or popover does not destroy the failure notice")
    : bad("toast: an ESC meant for an open dialog or popover does not destroy the failure notice", JSON.stringify(toastRun));
  (toastRun.escDismisses && toastRun.closeDismisses)
    ? ok("toast: ESC and the close button both dismiss the failure notice")
    : bad("toast: ESC and the close button both dismiss the failure notice", JSON.stringify(toastRun));
  (toastRun.successShown && toastRun.successFaded)
    ? ok("toast: success notices still fade on their own")
    : bad("toast: success notices still fade on their own", JSON.stringify(toastRun));
  (busyRun.previewBusy.modalOpen && /Previewing affected rows/.test(busyRun.previewBusy.title))
    ? ok("busy modal: Preview rows adopts the same busy affordance")
    : bad("busy modal: Preview rows adopts the same busy affordance", JSON.stringify(busyRun.previewBusy).slice(0, 200));
  (busyRun.previewDone.modalGone && busyRun.previewDone.rendered && busyRun.previewDone.btnEnabled)
    ? ok("busy modal: preview closes on completion with the rows rendered")
    : bad("busy modal: preview closes on completion with the rows rendered", JSON.stringify(busyRun.previewDone));
  (busyRun.cmdkLeg.paletteOpened && busyRun.cmdkLeg.paletteClosed && busyRun.cmdkLeg.modalSurvived)
    ? ok("busy modal: Escape over the ⌘K palette closes the palette only — the dialog survives")
    : bad("busy modal: Escape over the ⌘K palette closes the palette only — the dialog survives", JSON.stringify(busyRun.cmdkLeg));
  (busyRun.cmdkLeg.canceledAfter && busyRun.cmdkLeg.btnEnabled)
    ? ok("busy modal: the next Escape (palette closed) cancels the dialog as usual")
    : bad("busy modal: the next Escape (palette closed) cancels the dialog as usual", JSON.stringify(busyRun.cmdkLeg));
  (busyRun.clobberLeg.before && busyRun.clobberLeg.busyGone && busyRun.clobberLeg.serversIntact)
    ? ok("busy modal: another dialog can claim the shared #modal slot (fixture precondition)")
    : bad("busy modal: another dialog can claim the shared #modal slot (fixture precondition)", JSON.stringify(busyRun.clobberLeg));
  (busyRun.clobberLeg.btnEnabled && busyRun.clobberLeg.flagReset)
    ? ok("busy modal: a clobbered dialog dissolves its trap — buttons re-enabled, re-entry flag reset")
    : bad("busy modal: a clobbered dialog dissolves its trap — buttons re-enabled, re-entry flag reset", JSON.stringify(busyRun.clobberLeg));
  busyRun.clobberLeg.selfHealed
    ? ok("busy modal: the re-entry guard self-heals on read after a clobber")
    : bad("busy modal: the re-entry guard self-heals on read after a clobber", JSON.stringify(busyRun.clobberLeg));
  (busyRun.switchLeg.modalGone && busyRun.switchLeg.noPanel && busyRun.switchLeg.btnEnabled)
    ? ok("busy modal: a server switch mid-flight closes the dialog and renders nothing")
    : bad("busy modal: a server switch mid-flight closes the dialog and renders nothing", JSON.stringify(busyRun.switchLeg));

  // Scenario 17b — the reduced-motion arm (#1363, same rule as the #1353
  // skeletons): under prefers-reduced-motion the CSS animation is replaced by
  // the static note; under no-preference the animation shows and the note
  // hides. Driven through the REAL openBusyModal against emulated media.
  const rmProbe = async () => page.evaluate(() => {
    const busy = openBusyModal(document.getElementById("recover-form"),
      { title: "probe", errTitle: "probe", facts: [], onCancel: () => {} });
    const res = {
      anim: getComputedStyle(document.querySelector("#modal .busy-anim")).display,
      note: getComputedStyle(document.querySelector("#modal .busy-static")).display,
    };
    busy.close(false);
    return res;
  });
  await page.emulateMedia({ reducedMotion: "no-preference" });
  const rmOff = await rmProbe();
  await page.emulateMedia({ reducedMotion: "reduce" });
  const rmOn = await rmProbe();
  await page.emulateMedia({ reducedMotion: null });
  (rmOff.anim === "flex" && rmOff.note === "none")
    ? ok("busy modal: animation renders under no-preference (static note hidden)")
    : bad("busy modal: animation renders under no-preference (static note hidden)", JSON.stringify(rmOff));
  (rmOn.anim === "none" && rmOn.note === "block")
    ? ok("busy modal: prefers-reduced-motion collapses the animation to the static note")
    : bad("busy modal: prefers-reduced-motion collapses the animation to the static note", JSON.stringify(rmOn));

  // Scenario 17c — resting states under prefers-reduced-motion (#1392).
  //
  // The whole-file guard in assets_reducedmotion_test.go proves every
  // animation SITS inside the reduced-motion block. It cannot prove the rule
  // left OUTSIDE is a usable resting state, and that is the failure that costs
  // information rather than polish: `ul` defines only a `to` frame, so leaving
  // its scaleX(0) start on the base rule WOULD have hidden the underline
  // marking WHICH value changed — permanently, and with the text guard
  // green. A text checker cannot see that; it is a cascade question, so it is
  // asked of a real browser.
  //
  // The elements are synthesised rather than driven to through the UI on
  // purpose: the claim is about the stylesheet, and reaching a timeline with a
  // changed value through five navigations would test the route, not the CSS.
  const restProbe = async () => page.evaluate(() => {
    const host = document.createElement("div");
    host.innerHTML = '<div class="tl-node in"><span class="tl-dot"></span>'
      + '<div class="tl-body"><span class="pv changed">v</span></div></div>'
      + '<nav class="nav"><a class="nav-item active"><span>x</span></a></nav>'
      + '<button class="cov-refresh-btn spin"><span class="cov-refresh-ico">'
      + '<svg viewBox="0 0 24 24"></svg></span></button>'
      + '<div class="view-enter"><div>panel</div></div>'
      + '<div class="ov-stat"><div class="ov-stat-v">1</div></div>';
    document.body.appendChild(host);
    const cs = (sel, pseudo) => getComputedStyle(host.querySelector(sel), pseudo || null);
    const res = {
      underline: cs(".pv.changed", "::after").transform,
      underlineAnim: cs(".pv.changed", "::after").animationName,
      node: cs(".tl-node.in").transform,
      dot: cs(".tl-dot").transform,
      railHeight: cs(".nav-item.active", "::before").height,
      railAnim: cs(".nav-item.active", "::before").animationName,
      spinAnim: cs(".cov-refresh-ico svg").animationName,
      spinOpacity: cs(".cov-refresh-btn.spin").opacity,
      riseAnim: cs(".view-enter > div").animationName,
      // The Go orphan check anchors on @keyframes, which leaves two blind
      // spots these four fields cover. A guarded TRANSITION has no keyframes
      // to orphan; and `rise` is declared on BOTH .view-enter > * and
      // .ov-stat, so deleting either one leaves the name still "driven".
      nodeTransDur: cs(".tl-node.in").transitionDuration,
      dotTransDur: cs(".tl-dot").transitionDuration,
      dotAnim: cs(".tl-dot").animationName,
      ovAnim: cs(".ov-stat").animationName,
      ovTransDur: cs(".ov-stat").transitionDuration,
    };
    host.remove();
    return res;
  });
  await page.emulateMedia({ reducedMotion: "reduce" });
  const rest = await restProbe();
  await page.emulateMedia({ reducedMotion: "no-preference" });
  const moved = await restProbe();
  await page.emulateMedia({ reducedMotion: null });

  // Every element must ARRIVE without its animation. "none" is the computed
  // transform of an element with none declared — i.e. scaleX(1), full size,
  // final position.
  (rest.underline === "none" && rest.underlineAnim === "none")
    ? ok("reduced motion: the changed-value underline is VISIBLE without its animation")
    : bad("reduced motion: the changed-value underline is VISIBLE without its animation", JSON.stringify(rest));
  (rest.node === "none" && rest.dot === "none" && rest.dotAnim === "none"
    && rest.nodeTransDur === "0s" && rest.dotTransDur === "0s")
    ? ok("reduced motion: timeline node and dot rest at their final position, untimed")
    : bad("reduced motion: timeline node and dot rest at their final position, untimed", JSON.stringify(rest));
  (rest.ovAnim === "none" && rest.ovTransDur === "0s")
    ? ok("reduced motion: the stat tile neither enters nor transitions")
    : bad("reduced motion: the stat tile neither enters nor transitions", JSON.stringify(rest));
  rest.railHeight !== "0px"
    ? ok("reduced motion: the active rail keeps its height without railpop")
    : bad("reduced motion: the active rail keeps its height without railpop", rest.railHeight);
  (rest.railAnim === "none" && rest.riseAnim === "none")
    ? ok("reduced motion: neither the rail nor the view entrance animates")
    : bad("reduced motion: neither the rail nor the view entrance animates", JSON.stringify(rest));
  // The spin carries nearly all the in-flight signal — the :disabled dim is
  // subtle and the cursor change invisible without a pointer — so its guard
  // owes a replacement rather than a plain removal.
  (rest.spinAnim === "none" && rest.spinOpacity === "0.55")
    ? ok("reduced motion: the refresh spinner is replaced by a dimmed button, not by nothing")
    : bad("reduced motion: the refresh spinner is replaced by a dimmed button, not by nothing", JSON.stringify(rest));

  // The other half: moving the rules into the guard must not have deleted the
  // motion for everyone else. Without this, emptying the block passes above.
  (moved.underlineAnim === "ul" && moved.railAnim === "railpop" && moved.spinAnim === "covspin"
    && moved.riseAnim === "rise" && moved.dotAnim === "dotpop")
    ? ok("reduced motion: under no-preference every animation this probe covers still runs")
    : bad("reduced motion: under no-preference every animation this probe covers still runs", JSON.stringify(moved));
  // Asserted as "nonzero", not as an exact duration. The exact form caught the
  // deletion this exists for, but it ALSO went red on a legitimate retune
  // (.45s -> .5s) with a message accusing the author of a deletion that never
  // happened — the cry-wolf shape this file argues against everywhere else.
  (parseFloat(moved.nodeTransDur) > 0 && parseFloat(moved.dotTransDur) > 0
    && parseFloat(moved.ovTransDur) > 0)
    ? ok("reduced motion: the guarded transitions still have a duration under no-preference")
    : bad("reduced motion: the guarded transitions still have a duration under no-preference", JSON.stringify(moved));
  moved.ovAnim === "rise"
    ? ok("reduced motion: the stat tile keeps its own entrance (rise has a second driver)")
    : bad("reduced motion: the stat tile keeps its own entrance (rise has a second driver)", moved.ovAnim);
  moved.spinOpacity === "1"
    ? ok("reduced motion: the dimmed-button stand-in does not leak into no-preference")
    : bad("reduced motion: the dimmed-button stand-in does not leak into no-preference", moved.spinOpacity);

  // ── Scenario 17e — who may wear the brand gradient (#1385) ──
  //
  // The Go guards in assets_brandpaint_test.go prove the gradient is opt-in,
  // that its transparent ink stays behind @supports, and that app.js grants the
  // class from exactly one place. None of them can prove the gate in that one
  // place points the right way: rewriting it to grant the class
  // unconditionally keeps the count at one and leaves every text guard green.
  // Verified by mutation, which is why this scenario exists.
  //
  // What is at stake is not decoration. A gradient has to satisfy its contrast
  // bar at every point along the sweep, and this one bottoms out at 3.53:1 on
  // the studio tile ground it actually paints on (3.70:1 on white) — so it can
  // be relied on at WCAG's LARGE-text bar of 3:1 and never at the 4.5:1 body
  // bar. (Its violet stop alone would clear 4.5:1; that buys nothing, the
  // other stops share the sweep.) A tile that takes the gradient without being large text is
  // unreadable, and the deletes tile additionally carries the semantic
  // --delete that the brand palette must never repaint.
  //
  // ovStat is called for real rather than synthesised: the gate lives inside
  // it, so a hand-built div would test the stylesheet and skip the bug.
  //
  // Two things this deliberately does NOT reach, so nobody reads more into a
  // green run than it earns. The scenario passes "danger" itself, so it proves
  // the GATE honours the modifier and not that fillOvActivity still sends one
  // — drop that argument at its call site and the deletes tile takes the
  // gradient with every check green. And the tiles are mounted on document.body
  // rather than inside a rendered Overview, so a rule scoped to an .ov-stats
  // or .view ancestor is invisible here in both directions. The wordmark
  // assertion below has neither limit: it reads the real element in place.
  const brandProbe = () => page.evaluate(() => {
    const host = document.createElement("div");
    document.body.appendChild(host);
    const tile = (v, key, mod) => {
      const n = ovStat(v, key, mod, "last 24 h");
      host.appendChild(n);
      return n.querySelector(".ov-stat-v");
    };
    const plain = tile("42", "changes indexed", "");
    const danger = tile("7", "deletes", "danger");
    // The `small` variant is built inline in fillOvEvents, not through ovStat.
    // Mirrored here because it is the surface the gradient would hurt MOST:
    // 19px at weight 500 is body text, held to 4.5:1, which the sweep as a
    // whole does not hold. It is also the exclusion the CSS cascade does NOT
    // protect — `.ov-stat-v.small` sets no colour, so the gradient rule would
    // win outright over it. Built by hand here on purpose, unlike the two
    // tiles above: this row asserts the STYLESHEET, that a bare
    // `.ov-stat-v.small` is left alone, which is the half no gate can supply.
    const small = el("div", { class: "ov-stat-v small", text: "2026-08-20 11:00:00" });
    host.appendChild(small);
    // A reference for the semantic colour, computed through the same pipeline
    // as the tile so the two strings are comparable whatever form the engine
    // serialises oklch into.
    const ref = el("span", { text: "x" });
    ref.style.color = "var(--delete)";
    host.appendChild(ref);
    const cs = (n) => getComputedStyle(n);
    const res = {
      plainImage: cs(plain).backgroundImage,
      plainColor: cs(plain).color,
      dangerImage: cs(danger).backgroundImage,
      dangerColor: cs(danger).color,
      smallImage: cs(small).backgroundImage,
      smallColor: cs(small).color,
      wordImage: cs(document.querySelector(".brand-name")).backgroundImage,
      wordColor: cs(document.querySelector(".brand-name")).color,
      deleteRef: cs(ref).color,
    };

    // The warmed loading bar (#1385). Measured off the ELEMENT, which is the
    // whole point and was got wrong first: an earlier version read --skel-warm
    // and --surface-2 off :root and compared those. That measures a token pair
    // the bar need not use — repointing .skel-line back at var(--line), the
    // literal state before the change, left it green.
    //
    // So: pull the first colour stop out of the bar's own computed gradient,
    // and composite it over the first painted ancestor rather than over black,
    // which keeps the arithmetic honest if the stop ever carries alpha.
    // Canvas rather than string comparison because the two values are a
    // color-mix and an oklch — they can never compare equal, so a string check
    // would be vacuous rather than merely different; only rendered pixels say
    // how far apart they are.
    const pending = ovStatPending("changes indexed", "all time");
    host.appendChild(pending);
    const bar = pending.querySelector(".skel-line");
    res.skelPainted = cs(bar).backgroundImage;
    res.skelStop = (res.skelPainted.match(/\b(?:rgba?|color|oklch|oklab|hsla?)\([^)]*\)|#[0-9a-fA-F]{3,8}\b/) || [])[0] || "";
    let ground = "rgba(0, 0, 0, 0)";
    for (let n = bar; n && (ground === "rgba(0, 0, 0, 0)" || ground === "transparent"); n = n.parentElement) {
      ground = cs(n).backgroundColor;
    }
    // Belt matching the fillStyle one below, for the same reason: if every
    // ancestor came back transparent the ground's luminance is 0 and the ratio
    // INFLATES past any floor, so the failure direction is a silent pass.
    // Unreachable while body paints --bg, which is exactly when a belt is
    // cheap. Recorded as null so the assertion has something to test.
    res.skelGround = (ground === "rgba(0, 0, 0, 0)" || ground === "transparent") ? null : ground;

    const cv = document.createElement("canvas");
    cv.width = cv.height = 1;
    const ctx = cv.getContext("2d", { willReadFrequently: true });
    // An unparseable fillStyle is IGNORED, leaving the previous value in place
    // — and the previous value would be the ground, so a rejected stop would
    // measure 1.0 and read as a failure, or worse against black would inflate
    // the ratio and pass. Checked explicitly instead of hoped for.
    ctx.fillStyle = "#010203";
    ctx.fillStyle = res.skelStop;
    res.skelParsed = ctx.fillStyle !== "#010203";
    const px = (c, over) => {
      ctx.clearRect(0, 0, 1, 1);
      ctx.fillStyle = over;
      ctx.fillRect(0, 0, 1, 1);
      ctx.fillStyle = c;
      ctx.fillRect(0, 0, 1, 1);
      return ctx.getImageData(0, 0, 1, 1).data;
    };
    const relLum = (d) => {
      const c = (v) => { v /= 255; return v <= 0.04045 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); };
      return 0.2126 * c(d[0]) + 0.7152 * c(d[1]) + 0.0722 * c(d[2]);
    };
    const [hi, lo] = [relLum(px(res.skelStop, ground)), relLum(px(ground, ground))].sort((a, b) => b - a);
    res.skelRatio = (hi + 0.05) / (lo + 0.05);

    // The same bar on the VIOLET PANEL ground (#1421/#1423 review): since the
    // stat tiles went white-bento, the worst ground these bars stand on is the
    // tinted Recent-changes panel (ovSkelLines renders straight into
    // .ov-evlist there), and the studio-tile measurement above no longer
    // covers the worst case. A bare .skel-line inside a .tcard-violet host is
    // the stylesheet half; the panel's real markup carries no gate.
    const vhost = document.createElement("div");
    vhost.className = "tcard-violet";
    document.body.appendChild(vhost);
    const vbar = document.createElement("div");
    vbar.className = "skel-line";
    vhost.appendChild(vbar);
    const vstop = (cs(vbar).backgroundImage.match(/\b(?:rgba?|color|oklch|oklab|hsla?)\([^)]*\)|#[0-9a-fA-F]{3,8}\b/) || [])[0] || "";
    const vground = cs(vhost).backgroundColor;
    ctx.fillStyle = "#010203";
    ctx.fillStyle = vstop;
    res.skelVioletParsed = ctx.fillStyle !== "#010203";
    const [vhi, vlo] = [relLum(px(vstop, vground)), relLum(px(vground, vground))].sort((a, b) => b - a);
    res.skelVioletRatio = (vhi + 0.05) / (vlo + 0.05);
    vhost.remove();

    host.remove();
    return res;
  });

  const brand = await brandProbe();
  const transparent = "rgba(0, 0, 0, 0)";

  (brand.plainImage.includes("gradient") && brand.plainColor === transparent)
    ? ok("brand paint: the 32px Overview count wears the headline gradient")
    : bad("brand paint: the 32px Overview count wears the headline gradient", JSON.stringify(brand));
  // Both halves matter. No gradient is the gate doing its job; a colour that
  // is still --delete is the cascade doing its job if the gate ever stops.
  (brand.dangerImage === "none" && brand.dangerColor === brand.deleteRef)
    ? ok("brand paint: the deletes tile keeps the semantic --delete and takes no gradient")
    : bad("brand paint: the deletes tile keeps the semantic --delete and takes no gradient", JSON.stringify(brand));
  (brand.smallImage === "none" && brand.smallColor !== transparent)
    ? ok("brand paint: the 19px/500 timestamp tile is left as plain readable ink")
    : bad("brand paint: the 19px/500 timestamp tile is left as plain readable ink", JSON.stringify(brand));
  // The other painted surface. Read off the REAL sidebar wordmark rather than
  // a copy — it is static markup that has been on screen since the first
  // navigation, so there is nothing to synthesise. Dropping .brand-name from
  // the gradient rule used to leave every check green.
  (brand.wordImage.includes("gradient") && brand.wordColor === transparent)
    ? ok("brand paint: the sidebar wordmark wears the headline gradient")
    : bad("brand paint: the sidebar wordmark wears the headline gradient", JSON.stringify(brand));
  // A floor placed BETWEEN the two measured states, not at the measurement:
  // the warmed stop renders 1.60:1 on the studio tile and the plain --line it
  // replaced rendered 1.23:1, so 1.3 separates them. That is what makes the
  // check bind — reverting the declaration lands at 1.23 and rings. It is not
  // a general washout detector: the mix has to fall to roughly 5% pink before
  // 1.3 fires on its own, so a "make it subtler" retune is NOT caught here.
  //
  // Studio is measured because it is the shipped direction and also the worst
  // of the three grounds; paper and trail resolve to white, where the same
  // stop sits at 1.68:1.
  (brand.skelParsed && brand.skelGround && brand.skelPainted.includes("gradient") && brand.skelRatio >= 1.3)
    ? ok("brand paint: the warmed loading bar's stop stays clear of its panel")
    : bad("brand paint: the warmed loading bar's stop stays clear of its panel",
        JSON.stringify({ stop: brand.skelStop, ground: brand.skelGround || "NONE FOUND",
          ratio: Number(brand.skelRatio.toFixed(3)), parsed: brand.skelParsed }));
  // The violet panel is the worst ground the bars stand on since the tiles
  // went white (measured 1.43 there vs 1.60 on the studio tile). Same 1.3
  // floor: reverting the bar to plain --line lands ~1.08 on violet and rings.
  (brand.skelVioletParsed && brand.skelVioletRatio >= 1.3)
    ? ok("brand paint: the warmed loading bar stays clear of the violet panel ground")
    : bad("brand paint: the warmed loading bar stays clear of the violet panel ground",
        JSON.stringify({ ratio: Number((brand.skelVioletRatio || 0).toFixed(3)), parsed: brand.skelVioletParsed }));

  // ── Scenario 17t — the tropical pass: the console wears the site's
  // sunset for real. Five independent guards, each on the surface where the
  // colour actually lands: the sidebar's tinted morning ground (light but
  // NEVER white, and never back to the dark night), its dark text, the
  // active pill, the gradient page title, and the tinted card rotation. Computed style, not declarations — a scoped token remap that
  // stops resolving (the exact way this pass could silently die) leaves
  // declarations intact and only the computed values change.
  const trop = await page.evaluate(() => {
    const side = document.querySelector(".side");
    const item = document.querySelector(".nav-item:not(.active)");
    const active = document.querySelector(".nav-item.active");
    const title = document.querySelector(".page-title");
    // Normalized through a canvas: computed colors come back as authored
    // (oklch on this branch, rgb elsewhere), and a parse-only reader would
    // go red on FORMAT instead of value. Cheap gamma-encoded luma, NOT the
    // WCAG luminance 17e computes — 0.6 is a coarse light/dark split, never
    // a contrast floor. Fails safe: a rejected color leaves the canvas
    // black and reads as dark.
    const lum = (c) => {
      const cv = document.createElement("canvas");
      cv.width = cv.height = 1;
      const ctx = cv.getContext("2d");
      ctx.fillStyle = c;
      ctx.fillRect(0, 0, 1, 1);
      const d = ctx.getImageData(0, 0, 1, 1).data;
      return (0.2126 * d[0] + 0.7152 * d[1] + 0.0722 * d[2]) / 255;
    };
    const sideCS = getComputedStyle(side);
    // the linear layer's first stop IS the ground's identity: light but not
    // white. Serialized computed backgroundImage keeps the authored oklch.
    const linear = sideCS.backgroundImage.split("linear-gradient")[1] || "";
    const stop = (/oklch\([^)]+\)/.exec(linear) || [""])[0];
    return {
      groundImage: sideCS.backgroundImage,
      stopLum: stop ? lum(stop) : -1,
      itemLum: item ? lum(getComputedStyle(item).color) : -1,
      activeImage: active ? getComputedStyle(active).backgroundImage : "",
      activeColor: active ? getComputedStyle(active).color : "",
      titleClip: title ? getComputedStyle(title).webkitBackgroundClip : "",
      titleColor: title ? getComputedStyle(title).color : "",
      titleImage: title ? getComputedStyle(title).backgroundImage : "",
    };
  });
  (/radial-gradient/.test(trop.groundImage) && /linear-gradient/.test(trop.groundImage))
    ? ok("tropical: the sidebar wears the tinted ground (radial glows over the linear base)")
    : bad("tropical: the sidebar wears the tinted ground (radial glows over the linear base)", trop.groundImage.slice(0, 120));
  (trop.stopLum > 0.80 && trop.stopLum < 0.985)
    ? ok("tropical: the sidebar ground is light but never white (and never the night)")
    : bad("tropical: the sidebar ground is light but never white (and never the night)", "stop luminance " + trop.stopLum.toFixed(3));
  (trop.itemLum >= 0 && trop.itemLum < 0.4)
    ? ok("tropical: sidebar text stays dark ink on the light ground")
    : bad("tropical: sidebar text stays dark ink on the light ground", "luminance " + trop.itemLum.toFixed(3));
  (/linear-gradient/.test(trop.activeImage) && trop.activeColor === "rgb(255, 255, 255)")
    ? ok("tropical: the active page is the sunset pill with white text")
    : bad("tropical: the active page is the sunset pill with white text", JSON.stringify({ i: trop.activeImage.slice(0, 80), c: trop.activeColor }));
  (trop.titleClip === "text" && trop.titleColor === "rgba(0, 0, 0, 0)" && /gradient/.test(trop.titleImage))
    ? ok("tropical: page titles wear the headline gradient")
    : bad("tropical: page titles wear the headline gradient", JSON.stringify({ clip: trop.titleClip, c: trop.titleColor }));

  // Two guards that outlived the night version. Selection: ::selection
  // resolves var() against the originating element, so any future ink
  // remap inside .side puts light glyphs on the sun highlight (the night
  // draft measured 1.32:1) — dark ink must hold. Haze:
  // background-attachment local is what keeps scrolled list headers off
  // the haze peak; a background shorthand edit on .main silently resets
  // it to scroll.
  const tropSide = await page.evaluate(() => {
    const lum = (c) => {
      const cv = document.createElement("canvas");
      cv.width = cv.height = 1;
      const ctx = cv.getContext("2d");
      ctx.fillStyle = c;
      ctx.fillRect(0, 0, 1, 1);
      const d = ctx.getImageData(0, 0, 1, 1).data;
      return (0.2126 * d[0] + 0.7152 * d[1] + 0.0722 * d[2]) / 255;
    };
    const meta = document.querySelector(".side-meta");
    return {
      selLum: meta ? lum(getComputedStyle(meta, "::selection").color) : -1,
      attachment: getComputedStyle(document.querySelector(".main")).backgroundAttachment,
    };
  });
  (tropSide.selLum >= 0 && tropSide.selLum < 0.4)
    ? ok("tropical: sidebar text selection keeps dark ink on the sun highlight")
    : bad("tropical: sidebar text selection keeps dark ink on the sun highlight", "lum " + tropSide.selLum.toFixed(3));
  /^local/.test(tropSide.attachment)
    ? ok("tropical: the haze is anchored to scroll content, not the viewport")
    : bad("tropical: the haze is anchored to scroll content, not the viewport", tropSide.attachment);


  // The card tint rotation, on the page from the user's own screenshot. Two
  // distinct tinted grounds prove rotation; "not white" alone would pass a
  // single flat tint.
  // On This daemon, which is the half that still carries two or more
  // unconditional cards after the split (#1543); Retention has one.
  await page.evaluate(() => navigate("daemon"));
  await page.waitForFunction(() => location.pathname === "/daemon" && document.querySelectorAll(".cards .card").length >= 2);
  const tints = await page.evaluate(() => {
    const cards = Array.from(document.querySelectorAll(".cards .card")).slice(0, 2);
    return cards.map((c) => getComputedStyle(c).backgroundColor);
  });
  (tints.length === 2 && tints[0] !== tints[1] && !tints.includes("rgb(255, 255, 255)") && !tints.includes("rgba(0, 0, 0, 0)"))
    ? ok("tropical: config cards rotate through the home's tint palette")
    : bad("tropical: config cards rotate through the home's tint palette", JSON.stringify(tints));

  // ── Scenario 17g2 — a .cards row leaves no empty track ──
  // The grid was repeat(3, 1fr), which is right only for the pages that carry
  // three cards. After the #1543 split Retention and Backups carry one and
  // This daemon two, so a third of the row was card and the rest was nothing.
  //
  // Measured on the RENDERED boxes, not on the rule: a stylesheet that reads
  // auto-fit still leaves a hole when its min track is wider than the space a
  // lone card can stretch into, and only the geometry can tell those apart.
  // The property is the one a reader sees: the cards on the first row, plus
  // the gaps between them, add up to the grid they sit in.
  //
  // Retention runs FIRST and daemon LAST, because Scenario 17h below picks up
  // "still on /daemon".
  const rowFill = [];
  for (const route of ["retention", "daemon"]) {
    await page.evaluate((r) => navigate(r), route);
    await page.waitForFunction(() => document.querySelectorAll(".cards .card").length >= 1);
    rowFill.push(await page.evaluate((r) => {
      const g = document.querySelector(".cards");
      const gw = g.getBoundingClientRect().width;
      const boxes = Array.from(g.children).map((c) => c.getBoundingClientRect());
      const top = Math.min(...boxes.map((b) => Math.round(b.top)));
      const row = boxes.filter((b) => Math.round(b.top) === top);
      const gap = parseFloat(getComputedStyle(g).columnGap) || 0;
      const used = row.reduce((s, b) => s + b.width, 0) + gap * (row.length - 1);
      return { route: r, cards: row.length, left: Math.round(gw - used) };
    }, route));
  }
  // 2px of slack for sub-pixel track rounding; the failure this catches is a
  // whole empty track (361px on a 1084px grid), not a rounding remainder.
  // Scoped to what auto-fit actually promises. It collapses a track only when
  // the track is empty across the whole grid, so a page with MORE cards than
  // tracks still ends on a ragged last row exactly as before. These two routes
  // are the ones the #1543 split left under-filled, at one card and two.
  rowFill.every((r) => r.cards >= 1 && r.left <= 2)
    ? ok("layout: a page with fewer cards than tracks still fills its row")
    : bad("layout: a page with fewer cards than tracks still fills its row", JSON.stringify(rowFill));

  // The other half, and the one auto-fit does NOT give for free: between the
  // 880px breakpoint and the width where three tracks fit again, a three-card
  // page wraps to two columns and its third card is alone on the last row.
  // Without the span rule that leaves 287 to 387px of nothing beside it, which
  // is the same hole moved to a laptop width. Measured on the LAST row, at a
  // width inside the band, on the page that carries three cards.
  //
  // The suite's viewport is restored before moving on: every scenario after
  // this one assumes 1300.
  await page.setViewportSize({ width: 1000, height: 1000 });
  await page.evaluate(() => navigate("connect"));
  await page.waitForFunction(() => location.pathname === "/connect"
    && document.querySelectorAll(".cards .card").length >= 3);
  const bandRow = await page.evaluate(() => {
    const g = document.querySelector(".cards");
    const gw = g.getBoundingClientRect().width;
    const boxes = Array.from(g.children).map((c) => c.getBoundingClientRect());
    const tops = [...new Set(boxes.map((b) => Math.round(b.top)))].sort((a, z) => a - z);
    const last = boxes.filter((b) => Math.round(b.top) === tops[tops.length - 1]);
    const gap = parseFloat(getComputedStyle(g).columnGap) || 0;
    const used = last.reduce((s, b) => s + b.width, 0) + gap * (last.length - 1);
    return { cards: boxes.length, rows: tops.length, onLast: last.length, left: Math.round(gw - used) };
  });
  await page.setViewportSize({ width: 1300, height: 1000 });
  // Back to /daemon, and WAIT for it: Scenario 17h below picks up "still on
  // /daemon" and looks for a card by title there. Leaving the page on /connect
  // failed it with {"found":false}, which reads as a telemetry bug and is not
  // one.
  await page.evaluate(() => navigate("daemon"));
  await page.waitForFunction(() => location.pathname === "/daemon"
    && document.querySelectorAll(".cards .card").length >= 2);
  // rows > 1 asserts the band is actually the wrapped case, so a future width
  // change that stops wrapping here cannot pass this vacuously.
  (bandRow.rows > 1 && bandRow.left <= 2)
    ? ok("layout: a card left alone on the last row spans it")
    : bad("layout: a card left alone on the last row spans it", JSON.stringify(bandRow));

  // ── Scenario 17g3 — the disk-space card, every state it can be in ──
  // Calls the REAL backupRefreshCard with each of the 144 DTOs the daemon can
  // serve and reads the rendered element. A Go guard over the source can only
  // see that both `br.enabled` and `br.scheduled` appear somewhere in the
  // function, so inverting either condition survives it while the card tells
  // the operator the opposite of the truth. Rendering is the only view that
  // separates those.
  //
  // It also pins the sentence about WHERE the saving applies, which the Go
  // guard deliberately leaves alone: that one asserts the claim is tied to the
  // rule in reconstruct, not what the claim says. Keyed to the PRODUCER, since
  // only the per-server schedule passes BaselineS3 to the fold; the interval
  // loop and every restore read the local directory and do reuse files.
  const cardStates = await page.evaluate(() => {
    const rows = [];
    for (const on of [false, true]) {
      for (const enabled of [false, true]) {
        for (const scheduled of [false, true]) {
          for (const source of ["default", "override"]) {
          // The two #1579 dimensions. `targets` is absent off a watch daemon
          // (no loop to count) and a real 0 on one whose loop covers nothing;
          // the alarm must separate those, so undefined and 0 are distinct
          // values here, not one falsy case.
          for (const targets of [undefined, 0, 1]) {
          // 0, 1 and 2: a predicate that agrees with "> 0" on {0, 2} but hides
          // the note for a SINGLE skipped server survives a two-value axis, and
          // one S3-only server is both the common deployment and the value a
          // later pluralization edit is most likely to special-case.
          for (const skipped of [0, 1, 2]) {
            const el = backupRefreshCard({ carry_forward_unchanged: on, enabled, scheduled, source,
              targets, skipped_s3_only: skipped });
            const t = el.innerText || el.textContent || "";
            // What sits INSIDE the compact block. Detached, innerText is
            // textContent, so `t` alone cannot tell visible from compact;
            // the floor below needs the split (#1603).
            const fine = Array.from(el.querySelectorAll("details.cn-fine")).map((d) => d.textContent).join("\n");
            rows.push({
              // The floor: what must stay in plain view is not inside the
              // compact block, and what moved there by design is.
              hiddenAlarm: fine.includes("no server can be refreshed"),
              hiddenDormant: fine.includes("Nothing uses this yet"),
              // The COUNTED sentence, so the compact block's own "keeps
              // backups only in S3" wording cannot false-positive.
              hiddenSkip: fine.includes(skipped + " server(s) keep backups only in S3"),
              compactSaving: fine.includes("only when the last backup is read from this machine"),
              compactChose: fine.includes("You chose this here"),
              on, enabled, scheduled, source, targets, skipped,
              alarm: t.includes("no server can be refreshed"),
              // The count is read back, not just the sentence: a card that
              // says "server(s)" without the number tells an operator nothing
              // about how much is uncovered.
              skipNote: t.includes(skipped + " server(s) keep backups only in S3"),
              // The skip note may MENTION the cheaper update, but only to DENY
              // it: these servers cannot get one, because the fold writes its
              // Parquet to the local directory they do not have. An unnegated
              // mention is the promise the first version of this note made
              // ("their scheduled backups read the bucket instead"). Read from
              // the skip PARAGRAPH, not the card: the saving sentence above
              // legitimately says "reuses nothing" about the same servers.
              skipPromisesReuse: ((line) => /read the bucket/i.test(line)
                || (/(reus|recorded changes)/i.test(line) && !/\b(never|cannot|no)\b/i.test(line)))(
                Array.from(el.querySelectorAll("p")).map((p) => p.textContent)
                  .find((x) => x.includes("keep backups only in S3")) || ""),
              pill: (el.querySelector(".bkr-state") || {}).textContent || "",
              dormant: t.includes("Nothing uses this yet"),
              middle: t.includes("Nothing refreshes all servers on one timer"),
              chose: t.includes("You chose this here"),
              saving: t.includes("only when the last backup is read from this machine"),
              // A WORD test, not the literal "(live". The old shape put the
              // word in parentheses, so a check for that string passes on a pill
              // reading "On, running now" beside "Nothing uses this yet", which
              // is the contradiction this exists to catch. The card says "the
              // next time dbtrail runs", so \brunning\b does not match it.
              live: /\b(live|running)\b/i.test(t),
              buttons: Array.from(el.querySelectorAll("button")).map((b) => b.textContent),
            });
          }
          }
          }
        }
      }
    }
    return rows;
  });
  const cardBad = cardStates.filter((r) =>
    // the value is on screen, and it is the value
    r.pill !== (r.on ? "On" : "Off")
    // the compact block never swallows a fault or the dormancy note, and
    // it does hold the qualifiers that moved there (#1603): a "compact" that
    // hid the alarm would pass every presence check above
    || r.hiddenAlarm || r.hiddenDormant || r.hiddenSkip
    || !r.compactSaving || r.compactChose !== (r.source === "override")
    // dormant is said when nothing consumes the setting, and only then
    || r.dormant !== !r.enabled
    // the middle state is the --baseline-trigger daemon: live for restores,
    // nothing on a timer. Collapsing it into either neighbour is the misreport
    // the card exists to avoid.
    || r.middle !== (r.enabled && !r.scheduled)
    // provenance answers who chose, never whether it runs
    || r.chose !== (r.source === "override")
    || r.live
    // the saving never appears without the condition it actually has
    || !r.saving
    // #1579: the alarm fires exactly when a timer is on over zero refreshable
    // servers. An ABSENT count (no loop in this daemon) is not zero, and that
    // is the whole point of the strict compare in the card.
    || r.alarm !== (r.scheduled && r.targets === 0)
    // the skip note appears with its count, and only when servers were skipped
    || r.skipNote !== (r.skipped > 0)
    // and it never promises those servers an update from the bucket, which
    // needs the local directory they lack
    || r.skipPromisesReuse
    // the hand-it-back button appears only where there is something to hand back
    || (r.buttons.length === 2) !== (r.source === "override"));
  cardStates.length === 144 && cardBad.length === 0
    ? ok("backups: the disk-space card reports every state it can be in")
    : bad("backups: the disk-space card reports every state it can be in",
        JSON.stringify({ n: cardStates.length, wrong: cardBad.slice(0, 4) }));

  // ── Scenario 17h — the telemetry card shows the exact sample event (#1447) ──
  // Still on /daemon. The "Show a sample event" fold must be closed by
  // default, open on click, and carry the daemon's `sample_event` string
  // VERBATIM in a read-only <pre>: the daemon renders it through the same
  // function `bintrail telemetry show` prints through, and a JSON.stringify
  // in the frontend would be a second renderer free to drift. Each fetch
  // draws a fresh run_id (as each `show` run does), so that one line is
  // normalised before the bytes are compared, and its presence is asserted so
  // an empty normalisation cannot pass. run.sh builds without -ldflags, so
  // this photographs the no-endpoint arm, where the fold still renders.
  const telSample = await page.evaluate(async () => {
    const card = Array.from(document.querySelectorAll(".cards .card"))
      .find((c) => (c.querySelector(".card-title") || {}).textContent === "Usage telemetry");
    if (!card) return { found: false };
    const d = card.querySelector("details.tel-sample");
    if (!d) return { found: true, details: false };
    const closedBefore = !d.open;
    // checkVisibility, not a rect: Chromium folds a closed <details> with
    // content-visibility rather than display:none, so the <pre> keeps its
    // box (15px measured while folded) and only checkVisibility sees the fold.
    const preBefore = d.querySelector("pre");
    const hiddenBefore = preBefore ? !preBefore.checkVisibility() : false;
    d.querySelector("summary").click();
    const pre = d.querySelector("pre");
    const shown = pre ? pre.textContent : "";
    const state = await api("/api/telemetry");
    let parsed = null;
    try { parsed = JSON.parse(shown); } catch (_) {}
    return {
      found: true, details: true, closedBefore, hiddenBefore, openAfter: d.open,
      visible: pre ? pre.checkVisibility() : false,
      shown, fromApi: state.sample_event || "",
      parsedType: parsed && parsed.event_type,
      readOnly: pre ? !pre.isContentEditable && pre.querySelectorAll("input,textarea").length === 0 : false,
    };
  });
  const runIDLine = /"run_id": "[0-9a-f-]{36}"/g;
  const normRunID = (s) => s.replace(runIDLine, '"run_id": "<run_id>"');
  (telSample.found && telSample.details && telSample.closedBefore && telSample.hiddenBefore)
    ? ok("telemetry: the sample event is folded by default")
    : bad("telemetry: the sample event is folded by default", JSON.stringify(telSample));
  (telSample.openAfter && telSample.visible && telSample.readOnly && telSample.parsedType === "command_run")
    ? ok("telemetry: opening the fold shows the daemon's event as read-only JSON")
    : bad("telemetry: opening the fold shows the daemon's event as read-only JSON", JSON.stringify(telSample));
  (telSample.shown && (telSample.shown.match(runIDLine) || []).length === 1
    && (telSample.fromApi.match(runIDLine) || []).length === 1
    && normRunID(telSample.shown) === normRunID(telSample.fromApi))
    ? ok("telemetry: the shown bytes are the daemon's sample_event verbatim")
    : bad("telemetry: the shown bytes are the daemon's sample_event verbatim", JSON.stringify({ shown: telSample.shown, fromApi: telSample.fromApi }));

  // ── Scenario 17i — the Backup settings page (#1582, #1603) ──
  // The page's job is provenance, and since #1603 it SHOWS the three kinds
  // of setting instead of describing them: the disk-space switch and the
  // per-server rows under "Change here", the daemon's own values under "Set
  // when dbtrail starts" on a plain card with ONE restart chip, and two
  // drawings where paragraphs used to be. Driven LIVE against the watch
  // daemon. Guards, in the order the issue ranked them:
  //   1. the name: nav item and page head both read Backup settings;
  //   2. the kinds: two section labels, one card-level chip and zero per-row
  //      chips, no "not set" anywhere (an empty value renders as the word
  //      for what applies);
  //   3. the budget and the drawings: visible text under a cap with the
  //      fine print compact (closed <details>), the switch drawn as tiles,
  //      and the location drawing held against the API: every server has
  //      exactly one current case, stamped with the source the API returned.
  // innerText is the budget's measuring stick, as on Connect: a closed
  // <details> contributes only its summary line. The per-server panel is
  // EXCLUDED from the count because it scales with the registry (this run
  // seeds several servers), which would make the cap measure the fixture.
  // The cap (1300) sits ~45% above the rewritten page (906 measured here,
  // with the harness's daemon flags; the nine rows' labels and flag names
  // are most of it) and ~25% below the pre-#1603 page (1719 measured by the
  // same method with zero compact blocks, RED verified), so a copy edit
  // breathes but a wall of text rings.
  await page.evaluate(() => navigate("backup-settings"));
  // Options are the THIRD waitForFunction parameter; an options object in
  // the arg slot is serialized to the predicate and silently discarded
  // (#1589), leaving the 30s default in force. Stated as 30s explicitly.
  await page.waitForFunction(() => location.pathname === "/backup-settings"
    && document.querySelectorAll(".bks-row").length >= 5, undefined, { timeout: 30000 });
  const bksAPI = await page.evaluate(() => api("/api/backup-settings"));
  const bks = await page.evaluate(() => {
    const view = document.querySelector(".view");
    const rows = Array.from(view.querySelectorAll(".bks-row")).map((r) => ({
      chips: r.querySelectorAll(".bks-restart").length,
      cli: (r.querySelector(".bks-cli") || {}).textContent || "",
      value: (r.querySelector(".bks-value") || {}).textContent || "",
    }));
    const boot = view.querySelector(".card.bks-boot");
    const perServer = view.querySelector(".ov-panel");
    const fine = Array.from(view.querySelectorAll("details.cn-fine"));
    const visible = view.innerText;
    return {
      head: (view.querySelector(".page-title") || {}).textContent || "",
      nav: (document.querySelector('.nav-item[data-route="backup-settings"] span:last-child') || {}).textContent || "",
      docsLink: !!view.querySelector(".page-docs"),
      rows: rows.length,
      rowChips: rows.reduce((n, r) => n + r.chips, 0),
      cardChips: boot ? boot.querySelectorAll(".card-title .bks-restart").length : -1,
      bootInGrid: !!view.querySelector(".cards .bks-boot"),
      sections: Array.from(view.querySelectorAll(".bks-sect")).map((n) => n.textContent),
      allNamed: rows.every((r) => /^\(CLI: (--|BINTRAIL_)/.test(r.cli)),
      // run.sh starts watch with --baseline-dir, so the first row must carry
      // a real path, not the word for an empty value.
      configuredValued: rows.length > 0 && rows[0].value.startsWith("/"),
      notSet: /not set/i.test(visible),
      emDash: /—/.test(visible),
      visibleChars: visible.length - (perServer ? perServer.innerText.length : 0),
      fine: fine.length,
      fineOpen: fine.filter((d) => d.open).length,
      refreshCardHere: !!view.querySelector(".bkr-head"),
      cfTiles: view.querySelectorAll(".bkr-head ~ .cf-shape .cf-row").length,
      legend: Array.from(view.querySelectorAll(".bl-legend .bl-case")).map((c) => c.dataset.source),
      current: Array.from(view.querySelectorAll(".bks-server")).map((b) => ({
        name: (b.querySelector(".bks-server-name") || {}).textContent || "",
        cases: Array.from(b.querySelectorAll(".bl-case.is-current")).map((c) => c.dataset.source),
      })),
      moreLinks: Array.from(view.querySelectorAll("details.cn-fine a.bks-docs")).map((a) => a.href),
    };
  });
  // The measurement itself, so a cap can be re-derived from a run's log.
  console.log("backup-settings: measured " + JSON.stringify({ visibleChars: bks.visibleChars, fine: bks.fine }));
  (bks.head === "Backup settings" && bks.nav === "Backup settings" && bks.docsLink)
    ? ok("backup-settings: named Backup settings in the nav and the head, with a Docs link")
    : bad("backup-settings: named Backup settings in the nav and the head, with a Docs link", JSON.stringify({ head: bks.head, nav: bks.nav, docsLink: bks.docsLink }));
  (bks.rows === 9 && bks.allNamed && bks.configuredValued && !bks.notSet)
    ? ok("backup-settings: nine daemon rows, each named, the configured one valued, none reading not set")
    : bad("backup-settings: nine daemon rows, each named, the configured one valued, none reading not set", JSON.stringify(bks));
  (bks.cardChips === 1 && bks.rowChips === 0 && !bks.bootInGrid && bks.sections.length === 2)
    ? ok("backup-settings: the three kinds are drawn apart: two sections, one card-level restart chip, the daemon card outside the tinted grid")
    : bad("backup-settings: the three kinds are drawn apart: two sections, one card-level restart chip, the daemon card outside the tinted grid",
        JSON.stringify({ cardChips: bks.cardChips, rowChips: bks.rowChips, bootInGrid: bks.bootInGrid, sections: bks.sections }));
  (bks.visibleChars > 0 && bks.visibleChars < 1300 && bks.fine >= 2 && bks.fineOpen === 0 && !bks.emDash)
    ? ok("backup-settings: visible text stays under budget with the fine print compact")
    : bad("backup-settings: visible text stays under budget with the fine print compact",
        JSON.stringify({ chars: bks.visibleChars, fine: bks.fine, open: bks.fineOpen, emDash: bks.emDash }));
  (bks.refreshCardHere && bks.cfTiles === 2)
    ? ok("backup-settings: the carry-forward card moved here and draws two backups")
    : bad("backup-settings: the carry-forward card moved here and draws two backups", JSON.stringify({ here: bks.refreshCardHere, rows: bks.cfTiles }));
  // The drawing cannot lie: the legend draws exactly the three verdicts
  // (the Go side pins that same set to the handler's constants, so a fourth
  // fails there first), and each seeded server's current case is the source
  // the API returned for it, held against the response rather than a list
  // typed here.
  const apiSources = (bksAPI.servers || []).map((s) => ({ name: s.name || s.id, source: s.source }));
  const legendSet = bks.legend.slice().sort().join(",");
  const currentMatches = apiSources.length > 0 && apiSources.length === bks.current.length
    && apiSources.every((s, i) => bks.current[i].name === s.name && bks.current[i].cases.length === 1 && bks.current[i].cases[0] === s.source);
  (legendSet === "default,none,server" && currentMatches)
    ? ok("backup-settings: the location drawing shows the three cases and marks each server's own as the API reports it")
    : bad("backup-settings: the location drawing shows the three cases and marks each server's own as the API reports it",
        JSON.stringify({ legend: bks.legend, api: apiSources, current: bks.current }));
  (bks.moreLinks.length >= 3 && bks.moreLinks.every((h) => h.startsWith("https://www.dbtrail.com/docs/guides/")))
    ? ok("backup-settings: the compact blocks link into the docs guides")
    : bad("backup-settings: the compact blocks link into the docs guides", JSON.stringify(bks.moreLinks));
  // The states this harness never puts on screen, rendered DETACHED through
  // the real functions (#1603): the switch drawn ON (three kept, two written,
  // the kept swatch in the key) and OFF (five written, no kept swatch), a
  // refused daemon value (loud line under the row, outside the compact
  // block, value marked), and the serve-mode page (no sections, no daemon
  // card, its own sub line). Detached so nothing on the live page changes.
  const bksStates = await page.evaluate(() => {
    const shape = (on) => {
      const s = cfShape(on);
      const rows = Array.from(s.querySelectorAll(".cf-row")).map((r) => ({
        tiles: r.querySelectorAll(".cf-tile").length, kept: r.querySelectorAll(".cf-tile.cf-kept").length }));
      return { rows, key: (s.querySelector(".cf-key") || {}).textContent || "",
        img: s.getAttribute("role") === "img" && (s.getAttribute("aria-label") || "").length > 20 };
    };
    const refused = backupDaemonCard([{ key: "lock_mode", value: "ftwrl", err: "bad mode; MySQL dumps are refused until it is fixed", cli: "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE", needs_restart: true }]);
    const loud = Array.from(refused.querySelectorAll("p.form-msg.err"));
    const view = document.querySelector(".view");
    const keep = { monitor: capsCache.monitor, html: view.innerHTML };
    capsCache.monitor = false;
    let serve;
    try {
      buildBackupSettings({ daemon: [], servers: [{ id: "x", name: "x", source: "none" }], registry_read_only: false }, null);
      serve = { sections: view.querySelectorAll(".bks-sect").length, boot: !!view.querySelector(".bks-boot"),
        sub: (view.querySelector(".page-sub") || {}).textContent || "", current: (view.querySelector(".bl-case.is-current") || {}).dataset };
    } finally { capsCache.monitor = keep.monitor; view.innerHTML = keep.html; }
    return { on: shape(true), off: shape(false),
      refused: { marked: !!refused.querySelector(".bks-value.bks-refused"), loud: loud.length === 1 && loud[0].textContent.includes("bad mode"),
        outside: loud.length === 1 && !loud[0].closest("details") },
      serve };
  });
  const cf = bksStates.on, cfo = bksStates.off;
  (cf.rows.length === 2 && cf.rows.every((r) => r.tiles === 5) && cf.rows[0].kept === 0 && cf.rows[1].kept === 3 && /kept/.test(cf.key) && cf.img
    && cfo.rows.length === 2 && cfo.rows.every((r) => r.tiles === 5) && cfo.rows[1].kept === 0 && !/kept/.test(cfo.key) && cfo.img)
    ? ok("backup-settings: the switch drawn on keeps three of five and written two; off writes all five")
    : bad("backup-settings: the switch drawn on keeps three of five and written two; off writes all five", JSON.stringify({ on: cf, off: cfo }));
  (bksStates.refused.marked && bksStates.refused.loud && bksStates.refused.outside)
    ? ok("backup-settings: a refused daemon value is marked and its reason stays in plain view")
    : bad("backup-settings: a refused daemon value is marked and its reason stays in plain view", JSON.stringify(bksStates.refused));
  (bksStates.serve.sections === 0 && !bksStates.serve.boot && bksStates.serve.sub === "Where each server keeps its backups." && bksStates.serve.current && bksStates.serve.current.source === "none")
    ? ok("backup-settings: on serve the page is the per-server half alone, with its own sub line")
    : bad("backup-settings: on serve the page is the per-server half alone, with its own sub line", JSON.stringify(bksStates.serve));
  // Restored the live page above; re-render it so the next assertion reads
  // the real thing, not the restored HTML with its listeners gone.
  await page.evaluate(() => renderRoute());
  await page.waitForFunction(() => document.querySelectorAll(".bks-server input[name=baseline_dir]").length >= 1, undefined, { timeout: 30000 });
  // Save wakes up on a change and sleeps again when the field is put back;
  // a Save that stays asleep is an operator who cannot set a backup location.
  const dirtySave = await page.evaluate(async () => {
    const box = document.querySelector(".bks-server");
    const input = box.querySelector("input[name=baseline_dir]");
    const save = Array.from(box.querySelectorAll("button")).find((b) => b.textContent === "Save");
    const was = input.value;
    const fire = () => input.dispatchEvent(new Event("input", { bubbles: true }));
    const atRest = save.disabled;
    input.value = was + "x"; fire();
    const afterEdit = save.disabled;
    input.value = was; fire();
    return { atRest, afterEdit, afterRestore: save.disabled };
  });
  (dirtySave.atRest && !dirtySave.afterEdit && dirtySave.afterRestore)
    ? ok("backup-settings: Save wakes on a change and sleeps again when the field is put back")
    : bad("backup-settings: Save wakes on a change and sleeps again when the field is put back", JSON.stringify(dirtySave));
  // Settled on the LISTING panel, not a timer: the whole page builds in one
  // pass, so once the panel title is up, a mounted card would be too — a
  // sleep could pass this vacuously against a half-rendered page.
  await page.evaluate(() => navigate("baselines"));
  await page.waitForFunction(() => location.pathname === "/baselines"
    && document.querySelector(".view .ov-panel-title"));
  const dupCard = await page.evaluate(() => !!document.querySelector(".view .bkr-head"));
  (!dupCard)
    ? ok("backups: the carry-forward card is not duplicated on the Backups page")
    : bad("backups: the carry-forward card is not duplicated on the Backups page", "found .bkr-head on /baselines");

  // ── Scenario 17g — Connect AI is three short steps with a drawn dialog ──
  // The audience is Claude users, mostly non-technical. The first rewrite
  // (#1430) made the steps explicit but drowned them in prose; the verdict on
  // the live page was "demasiado texto". This pass folds every contingency
  // into <details> and DRAWS the install dialog instead of describing it.
  // Guards: the three numbered badges in order, the mock whose field labels
  // are VERBATIM from build/packaging/mcpb/manifest.template.json, and a hard
  // budget on the VISIBLE text. innerText is the budget's measuring stick on
  // purpose: it skips a closed <details>' folded content (each summary line
  // still counts), so fine print stays free while anything unfolded on the
  // open page counts against the cap. The cap (1500)
  // sits ~50% above the measured page (869-985 chars across the token states)
  // and 35% below the pre-simplify page (2304 measured, RED verified), so a
  // copy edit breathes but a wall of text rings. The SQL client panel below
  // the steps (#1446) is EXCLUDED from that measurement: the budget was
  // calibrated on the three MCP steps, and the panel's enabled shape alone
  // measures ~330 chars (1316 with it, 985 without), which would leave ~12%
  // headroom and make a legit copy edit on the panel ring a guard about a
  // different page. The panel has its own assertion further down.
  // Limit worth naming: run.sh builds without -ldflags, so this only ever
  // photographs the UNVERSIONED bundle arm.
  await page.evaluate(() => navigate("connect"));
  // Wait on .cn-card, a connect-only marker. ".card" is not connect-specific:
  // anything else that paints cards into the shared #view container (another
  // view's late async paint — the route-clobber class scenario 17c exists
  // for) can satisfy it, and the preview loop for this scenario photographed
  // exactly one such half-built page. .cn-card can only exist after
  // buildConnect ran, and === 3 also rings if a card is dropped.
  // Tolerant wait: against a page with no .cn-card at all (the M1 mutation,
  // assets reverted to the pre-simplify page) a bare waitForFunction timeout
  // ABORTS the suite at this line, hiding the four assertions below and every
  // later scenario. Catch it and let the assertions report the actual shape.
  await page.waitForFunction(() => location.pathname === "/connect"
    && document.querySelectorAll(".view .cn-card").length === 3)
    .catch(() => {});
  // The mock's field labels come from the REAL bundle manifest, not from a
  // second hand-maintained copy: renaming either side alone rings below. A
  // failed read degrades to null titles (a loud label mismatch carrying the
  // error) instead of throwing here, which would abort 17f and 18 with it.
  // globalThis.URL because this file shadows URL with the console's
  // base-address string.
  let mcpbTitles = [null, null], mcpbReadErr = "";
  try {
    const mcpbCfg = JSON.parse(readFileSync(new globalThis.URL("../../build/packaging/mcpb/manifest.template.json", import.meta.url), "utf8")).user_config;
    mcpbTitles = [mcpbCfg.console_url.title, mcpbCfg.token.title];
  } catch (e) { mcpbReadErr = String(e); }
  const cn = await page.evaluate(() => {
    const badges = Array.from(document.querySelectorAll(".view .card .cn-num")).map((n) => n.textContent).join("");
    const labels = Array.from(document.querySelectorAll(".cn-mock .cn-mock-label")).map((n) => n.textContent);
    const addrCard = document.querySelectorAll(".view .cn-card")[1];
    const view = document.querySelector(".view") || { innerText: "", querySelector: () => null };
    const visible = view.innerText;
    const sqlPanel = view.querySelector(".cn-sql");
    return {
      badges,
      labels,
      // Minus the SQL client panel's own text (see the budget note above).
      visibleChars: visible.length - (sqlPanel ? sqlPanel.innerText.length : 0),
      fine: document.querySelectorAll(".view details.cn-fine").length,
      addrCopy: addrCard ? Array.from(addrCard.querySelectorAll("button")).some((b) => b.textContent === "Copy") : false,
      once: /shown only once/.test(visible),
      // The 404-honesty rule's photographable half: this run is the
      // unversioned arm, where a direct release-asset link can only 404.
      downloadLinks: document.querySelectorAll('.view a[href*="/releases/download/"]').length,
      // The DuckDB schema card moved to Backups (#1581). On this stack the
      // monitor capability is on, so the serve-only fallback must NOT render
      // here — one surface at a time, never a duplicate. views is the anchor
      // that keeps the negative assertion honest: absent because the gate
      // withheld it, not because the capability was off all along.
      duckHere: !!document.querySelector(".view .cn-dk"),
      duckViewsCap: !!capsCache.views,
    };
  });
  (cn.badges === "123")
    ? ok("connect: three numbered step badges in order")
    : bad("connect: three numbered step badges in order", JSON.stringify(cn.badges));
  (cn.labels.length === 2 && cn.labels[0] === mcpbTitles[0] && cn.labels[1] === mcpbTitles[1])
    ? ok("connect: the drawn dialog carries the manifest's field names verbatim")
    : bad("connect: the drawn dialog carries the manifest's field names verbatim", JSON.stringify({ drawn: cn.labels, manifest: mcpbTitles, readErr: mcpbReadErr }));
  (cn.visibleChars > 0 && cn.visibleChars < 1500 && cn.fine >= 1)
    ? ok("connect: visible text stays under budget with fine print folded")
    : bad("connect: visible text stays under budget with fine print folded", "chars " + cn.visibleChars + " fine " + cn.fine);
  // The DuckDB schema card lives on Backups since #1581; with monitor on,
  // the Connect fallback must stay unmounted. Asserted here, photographed on
  // the Backups scenario (which carries the drawing and budget checks).
  (cn.duckViewsCap && !cn.duckHere)
    ? ok("connect: the DuckDB schema card is on Backups, not duplicated here")
    : bad("connect: the DuckDB schema card is on Backups, not duplicated here",
        JSON.stringify({ views: cn.duckViewsCap, rendered: cn.duckHere }));
  // "shown only once" is carried by the fresh state and the managed state
  // (except managed read_only, which drops the Lost-it clause and the phrase
  // with it); this run exercises the fresh one (no scenario mints a token).
  (cn.once && cn.addrCopy && cn.downloadLinks === 0)
    ? ok("connect: one-time warning visible, address one click to copy, no 404able download link")
    : bad("connect: one-time warning visible, address one click to copy, no 404able download link", JSON.stringify({ once: cn.once, addrCopy: cn.addrCopy, downloadLinks: cn.downloadLinks }));
  // The SQL client panel (#1446). run.sh opens the daemon's time-travel port,
  // so the ENABLED shape is what gets photographed: a mysql line carrying the
  // port and the selected server as its user (never a placeholder), with
  // the console token nowhere on the page. The panel is NOT a .cn-card, so
  // the three-badge assertion above stays at three.
  const fbPort = process.env.E2E_FLASHBACK_PORT || "13308";
  const sq = await page.evaluate(() => {
    const p = document.querySelector(".view .cn-sql");
    const code = p ? p.querySelector("code.cn-url") : null;
    return { present: !!p, line: code ? code.textContent : "", text: p ? p.innerText : "" };
  });
  const sqLineRE = new RegExp("^mysql -h 127\\.0\\.0\\.1 -P " + fbPort + " -u \\S+ -p$");
  (sq.present && sqLineRE.test(sq.line) && !sq.line.includes("<server") && !(TOKEN && sq.text.includes(TOKEN)))
    ? ok("connect: SQL client panel shows the time-travel port with a mysql line for the selected server")
    : bad("connect: SQL client panel shows the time-travel port with a mysql line for the selected server", JSON.stringify({ present: sq.present, line: sq.line }));

  // ── Scenario 17f — the Events skeleton is visible (#1397) ──
  //
  // .ev-skel-bar stood in for event rows at 1.09:1 against the page — fainter
  // than the 1.17:1 --line-soft hairline separating the rows it stood in for.
  // A loading list therefore rendered as an empty list with dividers: the
  // "blank list" that the #1353 comment introducing this feature says must
  // never be the busy state, on the page an operator opens mid-incident.
  //
  // This reads SCREENSHOT PIXELS, not computed style, and that is the whole
  // design. Two earlier drafts asserted a declared colour, and each time
  // review produced a one-declaration edit that re-broke the rendering while
  // the assertion sat unchanged at 1.64. `.ev-loading { opacity: .5 }` is the
  // one that settled it: opacity does NOT inherit — it composites the group —
  // so the bar's own computed opacity stays 1 while it renders at 1.267.
  // Chasing that with an ancestor walk would have left `filter`, a covering
  // ::after, and a background-image over the fill. A pixel has no such list.
  //
  // Every bar is measured (40 of them) because a rule can repaint SOME: the
  // nth-child rule reaches only even rows, and the five .ev-skel-* width
  // classes sit on the same element as .ev-skel-bar, so they repaint the bar
  // without naming it. All three directions are measured because stripping
  // fills is `paper`'s idiom — five existing rules do exactly that.
  //
  // The pulse is sampled rather than modelled. An earlier draft read the
  // keyframes and folded the minimum opacity into the maths, which is blind
  // to a pulse that animates any other property. Instead the animation is
  // frozen at five phases with a negative delay and photographed, so whatever
  // it animates is simply in the picture.
  //
  // Ground is sampled too, from the row's own padding a few pixels above the
  // bar, so both halves of the ratio come from the same photograph.
  //
  // The limit worth naming, since nothing here reaches it: renderEventsLoading
  // is called directly rather than through runEventsQuery. This proves the
  // skeleton is visible, not that the fetch path still paints one.
  const skelShot = async () => {
    const png = (await page.screenshot({ fullPage: true })).toString("base64");
    // Decoded by the browser's own PNG reader rather than a hand-rolled one:
    // the image goes back into the page, onto a canvas, and comes out as bytes.
    return page.evaluate(async (b64) => {
      const img = new Image();
      img.src = "data:image/png;base64," + b64;
      await img.decode();
      const cv = document.createElement("canvas");
      cv.width = img.width; cv.height = img.height;
      cv.getContext("2d").drawImage(img, 0, 0);
      const ctx = cv.getContext("2d", { willReadFrequently: true });
      const at = (x, y) => [...ctx.getImageData(Math.round(x), Math.round(y), 1, 1).data].slice(0, 3);
      // Document coordinates, to match a full-page shot. The ground point sits
      // inside the row's top padding, above the bar and well clear of the
      // bottom border, so it photographs whatever the bar is drawn on.
      return [...document.querySelectorAll(".ev-skel-bar")].map((b) => {
        const r = b.getBoundingClientRect();
        const row = b.closest(".ev-skel-row").getBoundingClientRect();
        const x = r.left + r.width / 2 + scrollX;
        return { bar: at(x, r.top + r.height / 2 + scrollY), ground: at(x, row.top + 3 + scrollY) };
      });
    }, png);
  };
  const relLum = (d) => {
    const c = (v) => { v /= 255; return v <= 0.04045 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); };
    return 0.2126 * c(d[0]) + 0.7152 * c(d[1]) + 0.0722 * c(d[2]);
  };
  const skelWorst = (samples) => samples.length === 0 ? null : samples.reduce((worst, s) => {
    const [hi, lo] = [relLum(s.bar), relLum(s.ground)].sort((a, b) => b - a);
    const ratio = (hi + 0.05) / (lo + 0.05);
    return !worst || ratio < worst.ratio ? { ratio, n: samples.length, bar: s.bar, ground: s.ground } : worst;
  }, null);

  // Resting first, under emulated reduced motion — a determinism choice, since
  // mid-pulse a photograph catches whatever frame it lands on. It is also the
  // honest worst case: the pulse is gated behind the same query, so a
  // reduced-motion user gets no movement at all to suggest the bar is there,
  // and the static value was the invisible one.
  await page.emulateMedia({ reducedMotion: "reduce" });
  // Repainted before every shot, not once for the run: renderEvents ends by
  // firing its own query, and that resolve calls buildEventRows, which clears
  // #ev-rows. The paint therefore waits for the view's OWN query to SETTLE —
  // both progressive phases, the background archive read included — before
  // painting the skeleton by hand (#1580). The earlier fixed 300ms sleep was
  // a bet on machine speed: under load the archive phase resolved between
  // the hand paint and the photograph, cleared the bars, and four scenarios
  // failed together on a box that re-ran green. Settled means the rows are
  // painted and no background-read transient remains, which is a state, not
  // a moment: once true it stays true, so nothing can clear the skeleton
  // painted after it.
  const skelPaint = async () => {
    await page.evaluate(() => navigate("events"));
    await page.waitForFunction(() => {
      if (location.pathname !== "/events") return false;
      const rows = document.querySelector("#ev-rows");
      // Painted rows OR a painted empty state — either is a finished query;
      // requiring rows alone would hang this wait on a server with nothing
      // in the window.
      if (!rows || !(rows.querySelector(".ev-row") || rows.querySelector(".ev-empty"))) return false;
      return !Array.from(document.querySelectorAll("#ev-warnings .warn-item"))
        .some((n) => /Reading archived history in the background/.test(n.textContent));
      // Options are the THIRD parameter (waitForFunction(fn, arg, options));
      // an options object in the arg slot is serialized to the predicate and
      // silently discarded, leaving the 30s default in force.
    }, undefined, { timeout: 30000 });
    return page.evaluate(() => {
      const rows = document.querySelector("#ev-rows");
      renderEventsLoading(rows);
      return document.querySelectorAll(".ev-skel-bar").length;
    });
  };
  const skelSetDir = (dir) => page.evaluate((d) => {
    const root = document.documentElement;
    d === null ? root.removeAttribute("data-dir") : root.setAttribute("data-dir", d);
  }, dir);

  const skelPrevDir = await page.evaluate(() => document.documentElement.getAttribute("data-dir"));
  let skelBars = 0;
  const skelRest = {};
  for (const dir of ["studio", "paper", "trail"]) {
    await skelSetDir(dir);
    skelBars = await skelPaint();
    skelRest[dir] = skelWorst(await skelShot());
  }
  await skelSetDir(skelPrevDir);

  // Then the pulse, sampled rather than modelled: freeze the animation with a
  // negative delay and photograph it. Whatever it animates is in the picture,
  // so swapping the animated property cannot slip past — an earlier draft read
  // the keyframes' opacity and was blind to exactly that.
  //
  // A uniform tenth-of-a-cycle grid rather than the phases this keyframe
  // happens to use, since reading the offsets would put the sampler back
  // inside the model it is meant to escape. The cost is stated rather than
  // hidden: a dip narrower than a tenth of a cycle can fall between samples.
  // An eased pulse is flat around its extreme, so the grid finds today's, and
  // it would find one moved off 50% — which the previous five-phase grid,
  // clustered around this keyframe's own minimum, would not have.
  //
  // One direction, not three: the pulse dims whatever the fill is, so sweeping
  // it per direction would re-measure the same multiplier. A direction that
  // strips the fill is caught by the resting floor above, in the direction
  // that strips it.
  //
  // The freeze is verified rather than trusted. If the paused state or the
  // delay silently failed to take, every photograph would land on the RESTING
  // pixel, clear the floor, and report that the dimmest phase had been
  // measured when nothing was. That is a vacuous pass — the failure this
  // scenario exists to make impossible — so the computed values are read back
  // after being set, which checks the instrument rather than the outcome.
  await page.emulateMedia({ reducedMotion: "no-preference" });
  await skelPaint();
  const skelPhases = [];
  for (const phase of [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]) {
    const froze = await page.evaluate((p) => {
      let name = "none", frozen = true;
      for (const b of document.querySelectorAll(".ev-skel-bar")) {
        const before = getComputedStyle(b);
        name = before.animationName;
        const secs = parseFloat(before.animationDuration) || 0;
        b.style.animationPlayState = "paused";
        b.style.animationDelay = `${-p * secs}s`;
        const after = getComputedStyle(b);
        if (after.animationPlayState !== "paused") frozen = false;
        if (Math.abs(parseFloat(after.animationDelay) + p * secs) > 1e-6) frozen = false;
      }
      return { name, frozen };
    }, phase);
    skelPhases.push({ phase, ...froze, ...(skelWorst(await skelShot()) || {}) });
  }
  await page.emulateMedia({ reducedMotion: null });
  await page.evaluate(() => { const r = document.querySelector("#ev-rows"); if (r) clear(r); });

  const skelShots = Object.entries(skelRest);
  const skelEnough = (s) => s && s.n >= 30;
  const skelRestWorst = skelShots.every(([, s]) => skelEnough(s))
    ? skelShots.reduce((w, [dir, s]) => (!w || s.ratio < w.ratio ? { dir, ...s } : w), null)
    : null;
  const skelPulseWorst = skelPhases.every(skelEnough)
    ? skelPhases.reduce((w, s) => (!w || s.ratio < w.ratio ? s : w), null)
    : null;
  // ratio can be ABSENT (a phase whose screenshot sampled no bars spreads {}
  // into the entry) — and this reporter runs precisely when that happens, so
  // it must render the hole as null, not abort the whole suite reporting it.
  const skelR = (s) => (s && s.ratio != null) ? +s.ratio.toFixed(3) : null;
  const skelDetail = () => JSON.stringify({
    bars: skelBars,
    rest: Object.fromEntries(skelShots.map(([d, s]) => [d, skelR(s)])),
    pulse: skelPhases.map((p) => ({ at: p.phase, r: skelR(p), frozen: p.frozen, anim: p.name })),
    worstRest: skelRestWorst && { dir: skelRestWorst.dir, ratio: skelR(skelRestWorst) },
    worstPulse: skelPulseWorst && { at: skelPulseWorst.phase, ratio: skelR(skelPulseWorst) },
  });

  // Hoisted out of the two floors below so a broken fixture reads as a broken
  // fixture. An empty sample list has no worst member, and every floor is
  // satisfied by nothing at all — the vacuous direction. Each shot must also
  // carry its own bars, since the list is repainted per direction.
  (skelBars >= 30 && skelRestWorst && skelPulseWorst)
    ? ok("events skeleton: the loading state paints bars this scenario can photograph")
    : bad("events skeleton: the loading state paints bars this scenario can photograph", skelDetail());

  // A floor placed BETWEEN the two measured states rather than at either:
  // --surface-3 rendered 1.09 and the neutral mix renders 1.64, so 1.35 is
  // what makes this bind. Reverting rings, and so does reaching for any
  // weaker token on the ramp (--line-soft 1.17, --line 1.28). It tolerates an
  // ordinary retune of the mix's two inputs — 80/20 is 1.52, 90/10 is 1.40 —
  // and, like 17e's floor, it is NOT a general washout detector: a deliberate
  // "make it subtler" edit has room to reach roughly 91% --line first — the
  // PULSE floor below is what binds there, one point before this one. It has
  // no opinion about hue either; --skel-warm measures 1.68 and would sail
  // through, which is deliberate and explained at the token.
  //
  // Three directions, not four axes: [data-accent] is NOT swept, so an
  // accent-scoped rule on these bars would go unphotographed. Said plainly
  // because the directions ARE swept and a reader could assume the rest.
  (skelRestWorst && skelRestWorst.ratio >= 1.35)
    ? ok("events skeleton: every bar stays visible at rest, in every direction")
    : bad("events skeleton: every bar stays visible at rest, in every direction", skelDetail());
  // The pulse's own floor, separate because it fails to a different edit —
  // deepening the dip rather than weakening the fill. 1.15 sits between the
  // old RESTING value (1.09) and this bar's dip (1.24), so the claim that the
  // animation no longer carries the bar's visibility is held by a measurement
  // rather than by the comment making it. Deepening 0.45 -> 0.12 photographs
  // at 1.06, below where this whole change started.
  //
  // A removed pulse is a legitimate pass, not a hole: with no animation the
  // resting floor above already describes every frame there is. What is NOT
  // allowed is an animation that exists while the freeze did not take.
  (skelPulseWorst && skelPhases.every((p) => p.name === "none" || p.frozen) &&
    skelPulseWorst.ratio >= 1.15)
    ? ok("events skeleton: the pulse's dimmest phase stays above where the bar started")
    : bad("events skeleton: the pulse's dimmest phase stays above where the bar started", skelDetail());

  // ── Scenario 18 — advisory severity split on the archive fixture (#1365) ──
  // The archive-elision record must render in the INFO register (muted line,
  // no ⚠ icon, no amber, no alert classes) while a real coverage-gap warning
  // keeps the ALERT register — both driven end to end against the run.sh
  // archive fixture (real rotation, real gap). One visual register for both
  // was the bug: a note saying "nothing is missing" read as an incident.
  const arcId = await page.evaluate(async (db) => {
    const res = await api("/api/servers", { method: "POST", body: {
      name: "arc-idx", host: "127.0.0.1", port: "13306", user: "root", password: "testroot", dbname: db,
    } });
    return res.id;
  }, ARC_DB);
  await page.evaluate(async (id) => { await switchServer(id); }, arcId);

  // The wire shape first: the elision fact lands in `notes` on a filled fast
  // page (warnings clean), and a large page really reads the archives (the
  // premise that makes the elision meaningful) with no elision claimed.
  const arcWire = await page.evaluate(async () => {
    const fast = await api("/api/events?limit=5");
    const slow = await api("/api/events?limit=500");
    return {
      fastCount: fast.count,
      fastNotes: fast.notes || [],
      fastWarnings: fast.warnings || [],
      slowCount: slow.count,
      slowNotes: slow.notes || [],
    };
  });
  arcWire.fastCount === 5 && arcWire.slowCount > arcWire.fastCount
    ? ok("severity split: archive fixture premise holds (fast page filled, slow page reads archives)")
    : bad("severity split: archive fixture premise holds (fast page filled, slow page reads archives)", JSON.stringify(arcWire));
  arcWire.fastNotes.some((n) => /answered from the live index/.test(n))
    ? ok("severity split: the elision fact rides the response `notes` list")
    : bad("severity split: the elision fact rides the response `notes` list", JSON.stringify(arcWire.fastNotes));
  arcWire.fastWarnings.length === 0
    ? ok("severity split: the elision is NOT a warning on the wire")
    : bad("severity split: the elision is NOT a warning on the wire", JSON.stringify(arcWire.fastWarnings));
  arcWire.slowNotes.length === 0
    ? ok("severity split: no elision claimed on a page that read the archives")
    : bad("severity split: no elision claimed on a page that read the archives", JSON.stringify(arcWire.slowNotes));

  // The INFO register in the real DOM: a filled 5-event browse.
  await page.evaluate(() => navigate("events"));
  await page.waitForSelector("#ev-rows .ev-row", { timeout: 10000 });
  const arcInfo = await page.evaluate(async () => {
    const f = document.getElementById("ev-form");
    f.elements.since.value = ""; f.elements.until.value = "";
    f.elements.limit.value = "5";
    // The submit's synchronous prefix clears only #ev-rows; notes and
    // warnings are repainted by paint() AFTER the fetch resolves, so the
    // PREVIOUS query's note could satisfy the loop below and every
    // assertion would then inspect the wrong query's DOM. Clear both here
    // so the loop can only match what THIS query paints.
    document.getElementById("ev-notes").textContent = "";
    document.getElementById("ev-warnings").textContent = "";
    f.requestSubmit();
    // The note this scenario is about, not any note, and a budget sized for
    // a loaded box (#1580): the old three-second cap was of the same class
    // as the gap-warning race next door.
    for (let i = 0; i < 150; i++) {
      await new Promise((r) => setTimeout(r, 100));
      const n = document.querySelector("#ev-notes .note-item");
      if (n && /answered from the live index/.test(n.textContent)) break;
    }
    const note = document.querySelector("#ev-notes .note-item");
    const cs = note ? getComputedStyle(note) : null;
    const bar = document.querySelector(".result-bar");
    const notesBox = document.getElementById("ev-notes");
    return {
      noteCount: document.querySelectorAll("#ev-notes .note-item").length,
      noteText: note ? note.textContent : "",
      warnCount: document.querySelectorAll("#ev-warnings .warn-item").length,
      hasIcon: note ? !!note.querySelector("svg") : true,
      alertClass: note ? /warn|alert|error/.test(note.className) : true,
      inAlertContainer: note ? !!note.closest(".warnings, .warn-box, .error-box") : true,
      bg: cs ? cs.backgroundColor : "",
      border: cs ? cs.borderTopStyle : "",
      underCountLine: !!(bar && notesBox && (bar.compareDocumentPosition(notesBox) & Node.DOCUMENT_POSITION_FOLLOWING)),
    };
  });
  arcInfo.noteCount === 1 && /answered from the live index/.test(arcInfo.noteText)
    ? ok("severity split: the elision note renders on the Events view")
    : bad("severity split: the elision note renders on the Events view", JSON.stringify(arcInfo));
  (!arcInfo.hasIcon && !arcInfo.alertClass && !arcInfo.inAlertContainer)
    ? ok("severity split: the note carries no alert classes and no ⚠ icon")
    : bad("severity split: the note carries no alert classes and no ⚠ icon", JSON.stringify(arcInfo));
  (arcInfo.bg === "rgba(0, 0, 0, 0)" && arcInfo.border === "none")
    ? ok("severity split: the note paints muted — no amber background, no border")
    : bad("severity split: the note paints muted — no amber background, no border", `bg=${arcInfo.bg} border=${arcInfo.border}`);
  arcInfo.warnCount === 0
    ? ok("severity split: no alert component renders for the benign fact")
    : bad("severity split: no alert component renders for the benign fact", `warnCount=${arcInfo.warnCount}`);
  arcInfo.underCountLine
    ? ok("severity split: the note sits under the result-count line")
    : bad("severity split: the note sits under the result-count line", "notes container precedes the result bar");

  // The ALERT register: a time range covering the manufactured gap hour must
  // keep the warning component — amber box, ⚠ icon — and claim no elision.
  const arcWarn = await page.evaluate(async ({ since, until }) => {
    const f = document.getElementById("ev-form");
    f.elements.since.value = since;
    f.elements.until.value = until;
    f.elements.limit.value = "50";
    f.requestSubmit();
    // Progressive events (#1414) answer in two phases, and phase 1 paints its
    // own TRANSIENT scope=live partial warning before phase 2 replaces it
    // with the final set. Breaking on ANY .warn-item samples whichever phase
    // got there first — a race this scenario lost twice in one session — so
    // wait for the TERMINAL state the assertions below are about: the gap
    // warning up AND no transient left beside it. Waiting for the warning
    // alone was still a race (#1580): on a loaded box the archive phase
    // outlived the old six-second budget, the loop expired mid-phase-1, and
    // four scenarios failed on a snapshot of the transient. The terminal
    // state is stable — once phase 2 lands it cannot regress — so a long
    // budget costs nothing on a healthy run.
    let w = null;
    for (let i = 0; i < 300; i++) {
      await new Promise((r) => setTimeout(r, 100));
      const items = Array.from(document.querySelectorAll("#ev-warnings .warn-item"));
      const transient = items.some((n) =>
        /Reading archived history in the background|scope=live/.test(n.textContent));
      w = items.find((n) => /rotated and not archived/.test(n.textContent)) || null;
      if (w && !transient) break;
    }
    const cs = w ? getComputedStyle(w) : null;
    return {
      warnCount: document.querySelectorAll("#ev-warnings .warn-item").length,
      warnText: w ? w.textContent : "",
      warnTexts: Array.from(document.querySelectorAll("#ev-warnings .warn-item")).map((n) => n.textContent),
      hasIcon: w ? !!w.querySelector("svg") : false,
      bg: cs ? cs.backgroundColor : "",
      border: cs ? cs.borderTopStyle : "",
      noteCount: document.querySelectorAll("#ev-notes .note-item").length,
    };
  }, { since: ARC_GAP_SINCE, until: ARC_GAP_UNTIL });
  // The matcher that found `w` makes /rotated and not archived/ true by
  // construction, so the real claim here is the second half: once the gap
  // warning is up, no phase-1 transient (scope=live partial, background-read
  // notice) and no failed-archive-read notice may share the box with it.
  (arcWarn.warnCount >= 1 && /rotated and not archived/.test(arcWarn.warnText)
    && !arcWarn.warnTexts.some((t) => /Reading archived history in the background|archive read FAILED|scope=live/.test(t)))
    ? ok("severity split: a real coverage-gap warning renders, with no transient sharing the box")
    : bad("severity split: a real coverage-gap warning renders, with no transient sharing the box", JSON.stringify(arcWarn));
  (arcWarn.hasIcon && arcWarn.bg !== "rgba(0, 0, 0, 0)" && arcWarn.border === "solid")
    ? ok("severity split: the gap warning keeps the alert register (⚠, amber, border)")
    : bad("severity split: the gap warning keeps the alert register (⚠, amber, border)", `icon=${arcWarn.hasIcon} bg=${arcWarn.bg} border=${arcWarn.border}`);
  arcWarn.noteCount === 0
    ? ok("severity split: no elision note on a time-ranged read into the archives")
    : bad("severity split: no elision note on a time-ranged read into the archives", `noteCount=${arcWarn.noteCount}`);

  // Scenario 12s — Schema changes view (#1443). Runs after the Events
  // scenarios: they read the Events DOM in place, and this one navigates
  // away from it. The sidebar entry routes to
  // the DDL history, the seeded rows render newest-binlog-position first (the
  // two DDLs share one second and CREATE was inserted first, so a listing
  // ordered by detected_at alone would put it on top), the time column
  // declares UTC, and the type filter narrows through the real API. Back on
  // the byo-idx server first: the archive scenarios above leave the ARC
  // index selected, and its schema_changes table is empty.
  await page.evaluate(async (id) => { await switchServer(id); }, byoId);
  await page.evaluate(() => navigate("schema-changes"));
  await page.waitForSelector("#sc-rows .sc-row", { timeout: 10000 });
  const scr = await page.evaluate(({ FIX }) => {
    const rows = Array.from(document.querySelectorAll("#sc-rows .sc-row"));
    const nav = document.querySelector('.nav-item[data-route="schema-changes"]');
    const head = document.querySelector(".sc-head span");
    const t = document.querySelector("#sc-rows .ev-time");
    return {
      rowCount: rows.length,
      firstIsAlter: !!rows[0] && rows[0].textContent.includes("e2e-ddl-alter"),
      secondIsCreate: !!rows[1] && rows[1].textContent.includes("e2e-ddl-create"),
      table: rows[0] ? rows[0].textContent.includes(FIX + ".orders") : false,
      navActive: !!nav && nav.classList.contains("active"),
      headCol: head ? head.textContent : "",
      tsTitle: t ? (t.getAttribute("title") || "") : "",
      count: (document.querySelector("#sc-count") || {}).textContent,
    };
  }, { FIX });
  scr.rowCount === 2 ? ok("schema changes: both seeded DDLs render") : bad("schema changes: both seeded DDLs render", `rowCount=${scr.rowCount}`);
  (scr.firstIsAlter && scr.secondIsCreate)
    ? ok("schema changes: same-second DDLs list in binlog order (ALTER above CREATE)")
    : bad("schema changes: same-second DDLs list in binlog order (ALTER above CREATE)", JSON.stringify(scr));
  scr.table ? ok("schema changes: rows name schema.table") : bad("schema changes: rows name schema.table", JSON.stringify(scr));
  scr.navActive ? ok("schema changes: the sidebar entry is active on its route") : bad("schema changes: the sidebar entry is active on its route", "no active .nav-item[data-route=schema-changes]");
  scr.headCol === "time (UTC)" ? ok("schema changes: the time column declares UTC") : bad("schema changes: the time column declares UTC", JSON.stringify(scr.headCol));
  scr.tsTitle.startsWith("UTC; in your local time:") ? ok("schema changes: rows carry the local-time tooltip") : bad("schema changes: rows carry the local-time tooltip", JSON.stringify(scr.tsTitle));
  scr.count === "2" ? ok("schema changes: the count line says 2") : bad("schema changes: the count line says 2", JSON.stringify(scr.count));
  await page.selectOption('#sc-form select[name="ddl_type"]', "CREATE");
  await page.waitForFunction(() => document.querySelectorAll("#sc-rows .sc-row").length === 1);
  const scf = await page.evaluate(() => Array.from(document.querySelectorAll("#sc-rows .sc-row")).map((r) => r.textContent).join(" | "));
  (scf.includes("e2e-ddl-create") && !scf.includes("e2e-ddl-alter"))
    ? ok("schema changes: the type filter narrows to the CREATE through the API")
    : bad("schema changes: the type filter narrows to the CREATE through the API", scf.slice(0, 200));
  await page.selectOption('#sc-form select[name="ddl_type"]', "TRUNCATE");
  await page.waitForSelector("#sc-rows .empty", { timeout: 10000 });
  const sce = await page.evaluate(() => (document.querySelector("#sc-rows .empty") || {}).textContent || "");
  /No schema changes found/.test(sce)
    ? ok("schema changes: an empty result renders the empty state")
    : bad("schema changes: an empty result renders the empty state", sce.slice(0, 200));
  await page.selectOption('#sc-form select[name="ddl_type"]', "");

  // No uncaught JS errors over the whole run.
  jsErrors.length === 0 ? ok("no uncaught JS errors") : bad("no uncaught JS errors", JSON.stringify(jsErrors));
} catch (err) {
  bad("driver", String((err && err.stack) || err));
  try { await page.screenshot({ path: `${ART}/console-e2e-failure.png`, fullPage: true }); } catch (_) {}
}

const failed = results.filter((r) => !r.pass);
for (const r of results) console.log(`${r.pass ? "PASS" : "FAIL"}  ${r.name}${r.pass ? "" : "  — " + r.detail}`);
console.log(`\n${results.length - failed.length}/${results.length} passed`);

if (failed.length) {
  try { await page.screenshot({ path: `${ART}/console-e2e-failure.png`, fullPage: true }); } catch (_) {}
}
await browser.close();
process.exit(failed.length ? 1 : 0);
