// Every control in the console that writes something, and the browser scene
// that proves the server stored it (#1883).
//
// Why this exists: a saved backup setting once reached the server encoded
// twice, and the server refused it (#1871). Unit tests on both sides passed,
// because each side was tested against its own idea of the other. Only a
// scene that presses the real control against the real daemon, and then
// reads the value back from the server, tells the two apart. A request that
// answers 200 and stores nothing passes every check that stops at the
// response, so each scene here reads the stored value back through the API.
//
// Three parts:
//
//   - WRITES: the list. One entry per write the page makes (method + path),
//     with the number of call sites in app.js. Each one is either a "scene"
//     (run by runSaveScenes below), "elsewhere" (a scene in console_e2e.mjs
//     that already reads the value back, named by its exact result label),
//     or an "action" (it starts a job or signs in; it saves no setting) with
//     the reason written down.
//   - extractWrites: reads app.js and finds every call with a writing method.
//     save_controls.test.mjs compares the two both ways, so a new Save
//     control fails the suite until it is on the list, and a control that
//     left app.js fails it until the list says so.
//   - runSaveScenes: the scenes, run by console_e2e.mjs against the daemon
//     run.sh starts. checkScenesRan then fails the run for any entry whose
//     scene did not report, so a scene that stops early cannot pass quietly.

import { execSync } from "node:child_process";
import { mkdirSync } from "node:fs";

// ── the list ────────────────────────────────────────────────────────────────

// id: the scene name, the prefix of every result the scene reports
// ("save <id>: ..."). label (elsewhere only): the exact result name of the
// scene in console_e2e.mjs.
export const WRITES = [
  // Settings and saved state.
  { method: "PUT", path: "/api/backup-settings/daemon/{id}", count: 1, kind: "elsewhere",
    label: "backup-settings: Save sends the value as a JSON object, the server stores it, and Use the startup value puts it back" },
  { method: "PUT", path: "/api/backup-settings/servers/{id}", count: 1, kind: "scene", id: "location" },
  { method: "PUT", path: "/api/rotation", count: 1, kind: "scene", id: "rotation" },
  { method: "POST", path: "/api/servers", count: 1, kind: "scene", id: "server-form" },
  { method: "PUT", path: "/api/servers/{id}", count: 1, kind: "scene", id: "server-form" },
  { method: "DELETE", path: "/api/servers/{id}", count: 1, kind: "scene", id: "server-delete" },
  { method: "PUT", path: "/api/servers/{id}/backup-schedule", count: 1, kind: "scene", id: "schedule" },
  { method: "DELETE", path: "/api/servers/{id}/backup-schedule", count: 1, kind: "scene", id: "schedule" },
  { method: "POST", path: "/api/telemetry", count: 1, kind: "scene", id: "telemetry" },
  { method: "POST", path: "/api/mcp-token", count: 1, kind: "scene", id: "mcp-token" },
  { method: "DELETE", path: "/api/mcp-token", count: 1, kind: "scene", id: "mcp-token" },
  { method: "PUT", path: "/api/servers/draft", count: 1, kind: "scene", id: "draft" },
  { method: "DELETE", path: "/api/servers/draft", count: 2, kind: "scene", id: "draft" },
  { method: "POST", path: "/api/capture-skips/ack", count: 1, kind: "scene", id: "capture-skips" },
  { method: "POST", path: "/api/auth/password", count: 1, kind: "scene", id: "password" },
  // Check and connect saves the server when every check passes. Its success
  // path starts capture against a real source, which is what the first-run
  // walk (first_run_walk.mjs) does from a clean install; here the refusal is
  // checked to store nothing.
  { method: "POST", path: "/api/servers/check", count: 1, kind: "scene", id: "connect-check" },
  // The access profile forms, through their one POST wrapper.
  { method: "POST", path: "/api/access-profiles/flags", count: 1, kind: "elsewhere",
    label: "access profiles: flag, profile and deny rule authored through the forms land on the index" },
  { method: "POST", path: "/api/access-profiles/profiles", count: 1, kind: "elsewhere",
    label: "access profiles: flag, profile and deny rule authored through the forms land on the index" },
  { method: "POST", path: "/api/access-profiles/rules", count: 1, kind: "elsewhere",
    label: "access profiles: flag, profile and deny rule authored through the forms land on the index" },
  { method: "POST", path: "/api/access-profiles/flags/remove", count: 1, kind: "elsewhere",
    label: "access profiles: the Remove buttons take the rule, the profile and the flag back off the index" },
  { method: "POST", path: "/api/access-profiles/profiles/remove", count: 1, kind: "elsewhere",
    label: "access profiles: the Remove buttons take the rule, the profile and the flag back off the index" },
  { method: "POST", path: "/api/access-profiles/rules/remove", count: 1, kind: "elsewhere",
    label: "access profiles: the Remove buttons take the rule, the profile and the flag back off the index" },

  // Actions: each one starts something or signs in. None stores a setting a
  // later read could get back wrong.
  { method: "POST", path: "/api/auth/setup", count: 1, kind: "action",
    reason: "creates the first login; the first-run walk signs up through it on a clean install" },
  { method: "POST", path: "/api/auth/login", count: 1, kind: "action", reason: "signs in; stores nothing but the session" },
  { method: "POST", path: "/api/auth/logout", count: 1, kind: "action", reason: "signs out; stores nothing" },
  { method: "POST", path: "/api/recover", count: 1, kind: "action", reason: "writes recovery SQL to the page; stores nothing" },
  { method: "POST", path: "/api/servers/test", count: 1, kind: "action", reason: "tests a connection; stores nothing" },
  { method: "POST", path: "/api/servers/{id}/test", count: 2, kind: "action", reason: "tests a connection; stores nothing" },
  { method: "POST", path: "/api/servers/{id}/schema-snapshot", count: 1, kind: "action", reason: "starts a schema snapshot job" },
  { method: "POST", path: "/api/servers/{id}/baseline", count: 1, kind: "action", reason: "starts a snapshot job" },
  { method: "POST", path: "/api/servers/{id}/baseline/restore", count: 1, kind: "action", reason: "starts a restore job" },
  { method: "POST", path: "/api/servers/{id}/sql-export", count: 1, kind: "action", reason: "starts a .sql export job" },
  { method: "POST", path: "/api/servers/{id}/verify", count: 1, kind: "action", reason: "starts a verification job" },
  { method: "POST", path: "/api/servers/{id}/monitor/start", count: 1, kind: "action",
    reason: "starts capture; the first-run walk starts it on a real source" },
  { method: "POST", path: "/api/servers/{id}/monitor/stop", count: 1, kind: "action", reason: "stops capture" },
];

// ── reading app.js ──────────────────────────────────────────────────────────

// DYNAMIC stands for a method this scan cannot read (a variable, a spread
// options object): it may be a write, so it has to be on the list too.
const DYNAMIC = "{DYNAMIC}";
const WRITE_VERBS = new Set(["PUT", "POST", "DELETE", "PATCH", DYNAMIC]);

// skipString returns the index just past the string literal that opens at i.
// Template literals are skipped whole, their ${} parts included: a path built
// inside one is normalized from its raw text by normPiece.
function skipString(text, i) {
  const q = text[i];
  let j = i + 1;
  while (j < text.length && text[j] !== q) {
    if (text[j] === "\\") j++;
    j++;
  }
  return j + 1;
}

// callArgs returns the argument texts of the call whose "(" is at open, split
// at top-level commas, and the index of its closing ")". Strings are skipped,
// so a comma or bracket inside one does not count.
function callArgs(text, open) {
  const args = [];
  let depth = 0, start = open + 1, j = open;
  for (; j < text.length; j++) {
    const c = text[j];
    if (c === '"' || c === "'" || c === "`") { j = skipString(text, j) - 1; continue; }
    if (c === "(" || c === "{" || c === "[") depth++;
    else if (c === ")" || c === "}" || c === "]") {
      depth--;
      if (depth === 0) { args.push(text.slice(start, j).trim()); break; }
    } else if (c === "," && depth === 1) { args.push(text.slice(start, j).trim()); start = j + 1; }
  }
  while (args.length && args[args.length - 1] === "") args.pop();
  return { args, end: j };
}

// splitTop splits text at the top-level occurrences of sep (a single
// character), outside strings and brackets.
function splitTop(text, sep) {
  const out = [];
  let depth = 0, start = 0;
  for (let j = 0; j < text.length; j++) {
    const c = text[j];
    if (c === '"' || c === "'" || c === "`") { j = skipString(text, j) - 1; continue; }
    if (c === "(" || c === "{" || c === "[") depth++;
    else if (c === ")" || c === "}" || c === "]") depth--;
    else if (c === sep && depth === 0) { out.push(text.slice(start, j).trim()); start = j + 1; }
  }
  out.push(text.slice(start).trim());
  return out;
}

// ternary splits "a ? b : c" at the top level; null when text is not one.
function ternary(text) {
  const q = splitTop(text, "?");
  if (q.length !== 2) return null;
  const c = splitTop(q[1], ":");
  if (c.length !== 2) return null;
  return [c[0], c[1]];
}

// normPiece turns one "+" piece of a path expression into path text: a
// string literal gives its content, encodeURIComponent(...) or any other
// expression gives {id}.
function normPiece(p) {
  if (/^"[^"]*"$|^'[^']*'$/.test(p)) return p.slice(1, -1);
  if (/^`[^`]*`$/.test(p)) return p.slice(1, -1).replace(/\$\{[^}]*\}/g, "{id}");
  return "{id}";
}

// normPath turns a path expression into its route shape, or null when it is
// not built from string literals at all (a variable passed in, as in a
// wrapper function).
function normPath(expr) {
  const pieces = splitTop(expr, "+");
  if (!pieces.some((p) => /^["'`]/.test(p))) return null;
  return pieces.map(normPiece).join("");
}

// optionMethod reads the method out of an options object text. It returns the
// list of verbs the value can take ("PUT", or ["PUT", "POST"] for a ternary,
// in branch order), or [] when the value is not a literal: api() itself
// forwards opts.method, and that is not a call site.
function optionMethod(opts) {
  if (!opts) return [];
  // Options held in a variable: the method cannot be read here, so it may
  // be a write.
  if (!opts.startsWith("{")) return [DYNAMIC];
  const inner = opts.slice(1, -1);
  for (const prop of splitTop(inner, ",")) {
    if (/^method$/.test(prop) || /^\.\.\./.test(prop)) return [DYNAMIC];
    const m = /^method\s*:\s*([\s\S]*)$/.exec(prop);
    if (!m) continue;
    const val = m[1].trim();
    const lit = (s) => (/^["'`]([A-Za-z]+)["'`]$/.exec(s.trim()) || [])[1];
    const t = ternary(val);
    if (t) return t.map((b) => (lit(b) || DYNAMIC).toUpperCase());
    const v = lit(val);
    return [v ? v.toUpperCase() : DYNAMIC];
  }
  return [];
}

// enclosingFunction names the function whose body holds index i: the last
// "function NAME(" that opens before it.
function enclosingFunction(text, i) {
  const re = /function\s+([A-Za-z_$][\w$]*)\s*\(/g;
  let name = null, m;
  while ((m = re.exec(text)) && m.index < i) name = m[1];
  return name;
}

function lineOf(text, i) { return text.slice(0, i).split("\n").length; }

// extractWrites lists every call in src that sends a writing method, as
// { method, path, line }. It finds three shapes:
//
//   - api(path, { method: "PUT", ... }) and fetch(path, { method: ... });
//   - a ternary on both sides, api(id ? A : B, { method: id ? "PUT" : "POST" }),
//     paired branch by branch (a cross product would invent two routes);
//   - a wrapper: a function that calls api(<its own parameter>, { method })
//     with a variable path. Each call of the wrapper with a literal path is
//     one write of that method.
//
// A method it cannot read (a variable, "{ method }", options held in a
// variable or spread) is listed as {DYNAMIC}: it may be a write, so it has
// to be on the list like any other. The one exception is api() itself, whose
// fetch forwards the caller's method.
//
// It is a bracket walk, not a parser: a call written inside a comment counts
// too, which errs on the side of listing one write too many.
export function extractWrites(src) {
  const writes = [];
  const wrappers = new Map();
  const re = /(^|[^\w$.])(api)\(|(^|[^\w$])(fetch)\(/g;
  let m;
  while ((m = re.exec(src))) {
    const open = m.index + m[0].length - 1;
    // A definition ("function api(path, opts)") is not a call.
    if (/function\s*$/.test(src.slice(Math.max(0, m.index - 20), m.index + (m[1] || m[3] || "").length))) continue;
    const { args } = callArgs(src, open);
    if (args.length < 2) continue;
    // Inside api() itself the method is the caller's, forwarded: every
    // caller is a call site of its own.
    if (m[4] === "fetch" && enclosingFunction(src, open) === "api") continue;
    const verbs = optionMethod(args[1]).filter((v) => v !== "");
    if (!verbs.some((v) => WRITE_VERBS.has(v))) continue;
    const line = lineOf(src, open);
    const pathT = ternary(args[0]);
    if (verbs.length === 2) {
      // A ternary method: pair it with a ternary path, or give both verbs the
      // one path.
      const paths = pathT ? pathT.map(normPath) : [normPath(args[0]), normPath(args[0])];
      verbs.forEach((v, k) => { if (WRITE_VERBS.has(v) && paths[k]) writes.push({ method: v, path: paths[k], line }); });
      continue;
    }
    const verb = verbs[0];
    if (pathT) {
      for (const b of pathT) { const p = normPath(b); if (p) writes.push({ method: verb, path: p, line }); }
      continue;
    }
    const p = normPath(args[0]);
    if (p) { writes.push({ method: verb, path: p, line }); continue; }
    // A variable path: the enclosing function is a wrapper.
    const fn = enclosingFunction(src, open);
    if (fn) wrappers.set(fn, verb);
    else writes.push({ method: verb, path: "{unknown}", line });
  }
  for (const [fn, verb] of wrappers) {
    const callRe = new RegExp("(^|[^\\w$.])" + fn.replace(/\$/g, "\\$") + "\\(", "g");
    while ((m = callRe.exec(src))) {
      const open = m.index + m[0].length - 1;
      // Skip the definition itself.
      if (/function\s*$/.test(src.slice(Math.max(0, m.index - 20), m.index + m[1].length))) continue;
      const { args } = callArgs(src, open);
      if (!args.length) continue;
      const p = normPath(args[0]);
      writes.push({ method: verb, path: p || "{unknown}", line: lineOf(src, open), via: fn });
    }
  }
  return writes;
}

// checkInventory compares the writes found in app.js with the list. Both
// directions: a write the list does not carry (or carries fewer times) is
// unlisted; an entry app.js no longer has (or has fewer times) is stale.
export function checkInventory(writes, inventory = WRITES) {
  const found = new Map();
  for (const w of writes) {
    const k = w.method + " " + w.path;
    if (!found.has(k)) found.set(k, []);
    found.get(k).push(w.line);
  }
  const listed = new Map();
  for (const e of inventory) listed.set(e.method + " " + e.path, (listed.get(e.method + " " + e.path) || 0) + e.count);
  const unlisted = [], stale = [];
  for (const [k, lines] of found) {
    const want = listed.get(k) || 0;
    if (lines.length > want) unlisted.push(`${k} (found ${lines.length} at lines ${lines.join(", ")}, listed ${want})`);
  }
  for (const [k, want] of listed) {
    const got = (found.get(k) || []).length;
    if (got < want) stale.push(`${k} (listed ${want}, found ${got})`);
  }
  return { unlisted, stale };
}

// checkEntries reports list entries that cannot be checked: a scene with no
// id, an elsewhere with no label, an action with no reason.
export function checkEntries(inventory = WRITES) {
  const problems = [];
  for (const e of inventory) {
    const k = e.method + " " + e.path;
    if (!Number.isInteger(e.count) || e.count < 1) problems.push(k + ": count must be a whole number of call sites");
    if (e.kind === "scene" && !e.id) problems.push(k + ": a scene needs an id");
    else if (e.kind === "elsewhere" && !e.label) problems.push(k + ": elsewhere needs the exact result label");
    else if (e.kind === "action" && !(e.reason && e.reason.trim())) problems.push(k + ": an action needs its reason");
    else if (!["scene", "elsewhere", "action"].includes(e.kind)) problems.push(k + ": unknown kind " + e.kind);
  }
  return problems;
}

// checkScenesRan is the runtime half: every scene id and every elsewhere
// label on the list must have reported at least one result in this run. A
// scene that threw before its first check, or a scene deleted from
// console_e2e.mjs, fails here. skipped holds scene ids that declared
// themselves not run (telemetry off CI); in CI nothing may be skipped.
export function checkScenesRan(results, { skipped = [], inCI = false, inventory = WRITES } = {}) {
  const names = results.map((r) => r.name);
  const missing = [];
  for (const e of inventory) {
    if (e.kind === "scene") {
      if (skipped.includes(e.id) && !inCI) continue;
      if (!names.some((n) => n.startsWith("save " + e.id + ":"))) missing.push("save " + e.id);
    } else if (e.kind === "elsewhere") {
      if (!names.includes(e.label)) missing.push(e.label);
    }
  }
  return [...new Set(missing)];
}

// ── the scenes ──────────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// until polls fn until it returns something truthy, or the budget runs out;
// it returns the last value either way, so the report shows what was seen.
async function until(fn, ms = 10000, step = 200) {
  const t0 = Date.now();
  let v;
  for (;;) {
    v = await fn();
    if (v || Date.now() - t0 > ms) return v;
    await sleep(step);
  }
}

// readAs is the read-back: a GET of the same resource, sent as the page sends
// it (the page's credential), for the server named, never the page's current
// selection. It returns the status and the parsed body.
async function readAs(page, path, serverId) {
  return page.evaluate(async ({ path, serverId }) => {
    const h = TOKEN ? { Authorization: "Bearer " + TOKEN } : {};
    if (serverId) h["X-Bintrail-Server"] = serverId;
    const r = await fetch(path, { headers: h });
    let body = null;
    try { body = await r.json(); } catch (_) { /* reported by the caller */ }
    return { status: r.status, body };
  }, { path, serverId: serverId || "" });
}

// The daemon's index database, reached the way the other fixture scenes reach
// it (docker exec into the test MySQL). Only the capture-skip scene needs it.
function mysqlIdx(sql) {
  const db = process.env.E2E_IDX_DB || "", ctr = process.env.E2E_MYSQL_CONTAINER || "";
  if (!db || !ctr) throw new Error("E2E_IDX_DB / E2E_MYSQL_CONTAINER not passed by run.sh");
  try {
    return execSync(["docker", "exec", "-i", ctr, "mysql", "-uroot", "-ptestroot", db].join(" "),
      { input: sql, stdio: ["pipe", "pipe", "pipe"] }).toString();
  } catch (e) {
    e.message += " :: " + String(e.stderr || "").slice(0, 400);
    throw e;
  }
}

// runSaveScenes runs one scene per saving control. ctx: { browser, page, ok,
// bad, url, token, jsErrors, byoId, tmpDir }. It returns the scene ids that
// declared themselves not run.
export async function runSaveScenes(ctx) {
  const { browser, page, ok, bad, url, token, jsErrors, byoId, tmpDir } = ctx;
  const skipped = [];
  const check = (id, what, pass, detail) => (pass ? ok(`save ${id}: ${what}`) : bad(`save ${id}: ${what}`, detail));
  const scene = async (id, fn) => {
    try { await fn(); } catch (e) { bad(`save ${id}: the scene ran to the end`, String((e && e.stack) || e)); }
  };
  const acceptDialog = (d) => d.accept();

  // A second tab opened on the address: nothing typed in the first tab (and
  // nothing in its sessionStorage) can reach it, so whatever it shows came
  // from the server. It selects the server named before it is read.
  const freshTab = async (serverId) => {
    const tab = await browser.newPage({ viewport: { width: 1300, height: 1000 } });
    tab.on("pageerror", (e) => jsErrors.push("save tab: " + String(e)));
    tab.on("dialog", acceptDialog);
    await tab.goto(`${url}/?token=${encodeURIComponent(token)}`, { waitUntil: "networkidle" });
    await tab.waitForFunction(() => typeof openServersModal === "function" && typeof capsCache !== "undefined" && capsCache.monitor === true);
    if (serverId) {
      await tab.evaluate(async (id) => { await switchServer(id); }, serverId);
      const sel = await tab.evaluate(() => currentServer);
      if (sel !== serverId) throw new Error(`fresh tab selected ${JSON.stringify(sel)}, wanted ${serverId}`);
    }
    return tab;
  };

  // The throwaway server the per-server scenes write to. Its index is the
  // daemon's own boot index (the one byo-idx reads), so it resolves, and it
  // has no source, so saving it starts no capture. Created through the form
  // by the server-form scene; the scenes after it need its id.
  let srvId = "";
  // What the page shows as an error after a Save: the form's own line, an
  // error notice, or the error toast. Cleared before each Save so an older
  // one does not count.
  const clearErrors = () => page.evaluate(() => { document.getElementById("toast-error").hidden = true; });
  const shownErrors = () => page.evaluate(() => {
    const out = [];
    const m = document.getElementById("server-form-msg");
    if (m && m.classList.contains("err") && m.textContent) out.push("form: " + m.textContent);
    const n = document.querySelector("#notice-mount .notice");
    if (n && /Could not|did not/.test(n.textContent)) out.push("notice: " + n.textContent.slice(0, 200));
    const t = document.getElementById("toast-error");
    if (t && !t.hidden && t.textContent) out.push("toast: " + t.textContent);
    return out.join(" | ");
  });
  const SRV_NAME = "e2e save target";
  const SRV_EDITED = "e2e save edited";

  page.on("dialog", acceptDialog);
  try {
    // ── the add and edit server form ─────────────────────────────────────
    await scene("server-form", async () => {
      await page.evaluate(() => { closeServersModal(); openServersModal(); });
      await page.waitForSelector("#server-add", { timeout: 5000 });
      await page.click("#server-add");
      await page.waitForSelector("#connect-full-form", { timeout: 5000 });
      await page.click("#connect-full-form");
      await page.waitForSelector('#server-form:not([data-connect]) input[name="dbname"]', { state: "attached", timeout: 5000 });
      const fill = async (vals) => {
        await page.evaluate(() => { const a = document.getElementById("server-advanced"); if (a) a.open = true; });
        for (const [k, v] of Object.entries(vals)) await page.fill(`#server-form-mount [name="${k}"]`, v);
        // The full form arrives with the source account the Connect screen
        // generated. This entry has no source, so it goes: sent without a
        // source host, a typed account is refused as a source with no host.
        await page.evaluate(() => {
          const f = document.querySelector("#server-form-mount #server-form").elements;
          f.source_user.value = "";
          f.source_password.value = "";
        });
      };
      // Spaces around the name: the form trims it, and the stored name must
      // be the trimmed one.
      await fill({ name: "  " + SRV_NAME + "  ", host: "127.0.0.1", port: "13306", user: "root", password: "testroot", dbname: "bintrail_e2e_idx" });
      await clearErrors();
      await page.click("#server-form-mount button[type=submit]");
      const created = await until(async () => {
        const r = await readAs(page, "/api/servers");
        return r.body && (r.body.servers || []).find((s) => s.name === SRV_NAME);
      });
      srvId = (created && created.id) || "";
      check("server-form", "Save on the add form stores the server, name trimmed, index fields as typed", !!created
        && created.host === "127.0.0.1" && String(created.port) === "13306" && created.user === "root"
        && created.dbname === "bintrail_e2e_idx" && created.has_password === true,
      JSON.stringify(created));
      const errAfterAdd = await shownErrors();
      check("server-form", "no error is shown after the add", errAfterAdd === "", errAfterAdd);

      // The same name again: the registry refuses a duplicate, the form shows
      // the server's own words, and nothing is added.
      await page.evaluate(() => openServersModal());
      await page.waitForSelector("#server-add", { timeout: 5000 });
      await page.click("#server-add");
      await page.waitForSelector("#connect-full-form", { timeout: 5000 });
      await page.click("#connect-full-form");
      await page.waitForSelector('#server-form:not([data-connect]) input[name="dbname"]', { state: "attached", timeout: 5000 });
      await fill({ name: SRV_NAME, host: "127.0.0.1", port: "13306", user: "root", password: "testroot", dbname: "bintrail_e2e_idx" });
      await page.click("#server-form-mount button[type=submit]");
      const dupMsg = await until(() => page.evaluate(() => {
        const m = document.getElementById("server-form-msg");
        return m && m.classList.contains("err") ? m.textContent : "";
      }));
      await page.keyboard.press("Escape");
      const afterDup = await readAs(page, "/api/servers");
      const sameName = ((afterDup.body && afterDup.body.servers) || []).filter((s) => s.name === SRV_NAME).length;
      check("server-form", "a duplicate name is refused in the server's words and adds nothing",
        /already|exists|in use|duplicate/i.test(dupMsg) && sameName === 1, JSON.stringify({ dupMsg, sameName }));
      await page.click("#server-cancel").catch(() => {});

      // Edit: Save on the edit form is a PUT of this id. The password field
      // comes back blank and blank means keep, so the stored password stays.
      if (!srvId) throw new Error("no server id from the add step");
      await page.evaluate((id) => editServer(id), srvId);
      await page.waitForSelector('#server-form-mount input[name="name"]', { timeout: 5000 });
      const prefilled = await page.evaluate(() => document.querySelector('#server-form-mount input[name="name"]').value);
      await page.fill('#server-form-mount input[name="name"]', SRV_EDITED);
      await clearErrors();
      await page.click("#server-form-mount button[type=submit]");
      const edited = await until(async () => {
        const r = await readAs(page, "/api/servers/" + encodeURIComponent(srvId));
        return r.body && r.body.name === SRV_EDITED ? r.body : null;
      });
      const errAfterEdit = await shownErrors();
      check("server-form", "no error is shown after the edit", errAfterEdit === "", errAfterEdit);
      check("server-form", "Save on the edit form stores the new name and keeps the password it did not show",
        prefilled === SRV_NAME && !!edited && edited.has_password === true && edited.dbname === "bintrail_e2e_idx",
        JSON.stringify({ prefilled, edited }));

      // Two saves in a row without a reload: the second one lands too.
      await page.evaluate((id) => editServer(id), srvId);
      await page.waitForSelector('#server-form-mount input[name="name"]', { timeout: 5000 });
      await page.fill('#server-form-mount input[name="name"]', SRV_EDITED + " 2");
      await page.click("#server-form-mount button[type=submit]");
      await until(async () => (await readAs(page, "/api/servers/" + encodeURIComponent(srvId))).body?.name === SRV_EDITED + " 2");
      await page.evaluate((id) => editServer(id), srvId);
      await page.waitForSelector('#server-form-mount input[name="name"]', { timeout: 5000 });
      await page.fill('#server-form-mount input[name="name"]', SRV_EDITED);
      await page.click("#server-form-mount button[type=submit]");
      const twice = await until(async () => {
        const r = await readAs(page, "/api/servers/" + encodeURIComponent(srvId));
        return r.body && r.body.name === SRV_EDITED ? r.body : null;
      });
      check("server-form", "two saves in a row both land", !!twice, JSON.stringify(twice));

      // A fresh tab lists the server under the saved name.
      const tab = await freshTab();
      try {
        await tab.evaluate(() => openServersModal());
        await tab.waitForSelector("#servers-list .srv-item", { timeout: 10000 });
        const listed = await until(() => tab.evaluate((n) => Array.from(document.querySelectorAll("#servers-list .srv-item"))
          .some((r) => r.textContent.includes(n)), SRV_EDITED));
        check("server-form", "a fresh tab lists the server under its saved name", !!listed, "not in #servers-list");
      } finally { await tab.close(); }
      await page.evaluate(() => closeServersModal());
    });

    // ── the per-server snapshot location ────────────────────────────────
    await scene("location", async () => {
      if (!srvId) throw new Error("no server from the server-form scene");
      await page.evaluate(async (id) => { await switchServer(id); }, srvId);
      const cur = await page.evaluate(() => currentServer);
      if (cur !== srvId) throw new Error(`page selected ${JSON.stringify(cur)}, wanted ${srvId}`);
      // Whatever folder a new server starts with (none, or one of its own),
      // the refused save below must leave it as it was.
      let st = await readAs(page, "/api/backup-settings");
      let own = st.body && (st.body.servers || []).find((s) => s.id === srvId);
      const startDir = own ? own.baseline_dir : undefined;
      await page.evaluate(() => navigate("snapshots"));
      await page.waitForSelector('.bks-server input[name="baseline_dir"]', { timeout: 15000 });
      // A relative folder is refused by the server, in its words.
      await page.fill('.bks-server input[name="baseline_dir"]', "relative/snaps");
      await page.click(".bks-server .stg-cardfoot .btn-primary");
      const refused = await until(() => page.evaluate(() => {
        const m = Array.from(document.querySelectorAll(".bks-server > p.form-msg.err")).find((p) => !p.hidden && p.textContent);
        return m ? m.textContent : "";
      }));
      st = await readAs(page, "/api/backup-settings");
      own = st.body && (st.body.servers || []).find((s) => s.id === srvId);
      check("location", "a relative folder is refused in the server's words and not stored",
        /full path/.test(refused) && typeof startDir === "string" && !!own && own.baseline_dir === startDir,
        JSON.stringify({ refused, startDir, own }));

      // A folder with spaces around it and a slash at the end: the page trims
      // the spaces, the server stores the folder as sent.
      const want = tmpDir + "/e2e-save-loc/";
      mkdirSync(want, { recursive: true });
      await page.fill('.bks-server input[name="baseline_dir"]', "  " + want + "  ");
      await page.click(".bks-server .stg-cardfoot .btn-primary");
      own = await until(async () => {
        st = await readAs(page, "/api/backup-settings");
        const o = st.body && (st.body.servers || []).find((s) => s.id === srvId);
        return o && o.baseline_dir === want ? o : null;
      });
      check("location", "Save stores the folder, trimmed, slash kept", !!own, JSON.stringify(st.body && st.body.servers));

      const tab = await freshTab(srvId);
      try {
        await tab.evaluate(() => navigate("snapshots"));
        await tab.waitForSelector('.bks-server input[name="baseline_dir"]', { timeout: 15000 });
        const shown = await tab.evaluate(() => document.querySelector('.bks-server input[name="baseline_dir"]').value);
        check("location", "a fresh tab shows the saved folder", shown === want, JSON.stringify(shown));
      } finally { await tab.close(); }
    });

    // ── the snapshot schedule, saved and turned off ─────────────────────
    await scene("schedule", async () => {
      if (!srvId) throw new Error("no server from the server-form scene");
      await page.evaluate(async (id) => { await switchServer(id); navigate("snapshots"); }, srvId);
      await page.waitForSelector(".bk-schedule .bk-restore-row .btn-primary", { timeout: 15000 });
      // A time of day the server cannot read: refused in its words, nothing
      // stored.
      await page.click(".bk-schedule .sch-pill[data-value='1d']");
      await page.fill(".bk-schedule input[aria-label='At (UTC)']", "25:99");
      await page.click(".bk-schedule .bk-restore-row .btn-primary");
      const refusal = await until(() => page.evaluate(() => {
        const m = document.querySelector(".bk-schedule p.form-msg.err[data-sched-edit]");
        return m && !m.hidden ? m.textContent : "";
      }));
      const none = await readAs(page, "/api/baselines", srvId);
      check("schedule", "a time of day the server cannot read is refused in its words and stores nothing",
        /not a time of day/.test(refusal) && none.status === 200 && !none.body.schedule,
        JSON.stringify({ refusal, status: none.status, schedule: none.body && none.body.schedule }));

      const at = farHour();
      await page.click(".bk-schedule .sch-pill[data-value='1d']");
      await page.fill(".bk-schedule input[aria-label='At (UTC)']", " " + at + " ");
      await page.click(".bk-schedule .bk-restore-row .btn-primary");
      const saved = await until(async () => {
        const r = await readAs(page, "/api/baselines", srvId);
        return r.body && r.body.schedule ? r.body.schedule : null;
      });
      check("schedule", "Turn on stores the schedule: every 1d at the typed hour",
        !!saved && /^(1d|24h)$/.test(String(saved.every)) && saved.at === at, JSON.stringify(saved));

      const tab = await freshTab(srvId);
      try {
        await tab.evaluate(() => navigate("snapshots"));
        await tab.waitForSelector(".bk-schedule .bk-card-state", { timeout: 15000 });
        const line = await until(() => tab.evaluate(() => (document.querySelector(".bk-schedule .bk-card-state") || {}).textContent || ""));
        check("schedule", "a fresh tab states the saved schedule", line.includes("at " + at), JSON.stringify(line));
      } finally { await tab.close(); }

      await page.evaluate(() => navigate("snapshots"));
      await page.waitForSelector(".bk-schedule .bk-restore-row .btn-ghost", { timeout: 15000 });
      await page.click(".bk-schedule .bk-restore-row .btn-ghost");
      const off = await until(async () => {
        const r = await readAs(page, "/api/baselines", srvId);
        return r.status === 200 && r.body && !r.body.schedule ? r : null;
      });
      check("schedule", "Turn off removes the stored schedule", !!off, JSON.stringify(off));
    });

    // ── deleting the server ─────────────────────────────────────────────
    await scene("server-delete", async () => {
      if (!srvId) throw new Error("no server from the server-form scene");
      await page.evaluate(() => openServersModal());
      await page.waitForSelector("#servers-list .srv-item", { timeout: 10000 });
      const clicked = await until(() => page.evaluate((n) => {
        const row = Array.from(document.querySelectorAll("#servers-list .srv-item")).find((r) => r.textContent.includes(n));
        const b = row && Array.from(row.querySelectorAll("button")).find((x) => x.textContent === "Delete");
        if (!b) return false;
        b.click();
        return true;
      }, SRV_EDITED));
      const gone = await until(async () => {
        const r = await readAs(page, "/api/servers");
        return r.body && !(r.body.servers || []).some((s) => s.id === srvId);
      });
      check("server-delete", "Delete removes the server from the stored list", !!clicked && !!gone,
        JSON.stringify({ clicked, gone }));
      await page.evaluate(() => closeServersModal());
    });

    // ── rotation ────────────────────────────────────────────────────────
    // No route puts rotation back to the daemon's defaults, so this scene
    // leaves an override behind. The values keep every seeded hour: a 60 to
    // 90 day window drops nothing this fixture holds.
    await scene("rotation", async () => {
      const before = (await readAs(page, "/api/rotation")).body;
      await page.evaluate(() => showRotationDialog());
      await page.waitForSelector('#modal form input[name="interval"]', { timeout: 5000 });
      await page.fill('#modal form input[name="interval"]', "abc");
      await page.click("#modal form button[type=submit]");
      const refused = await until(() => page.evaluate(() => {
        const m = document.querySelector("#modal form .form-msg.err");
        return m ? m.textContent : "";
      }));
      const afterBad = (await readAs(page, "/api/rotation")).body;
      check("rotation", "a bad interval is refused in the server's words and changes nothing",
        /interval must be a duration/.test(refused) && JSON.stringify(afterBad) === JSON.stringify(before),
        JSON.stringify({ refused, before, afterBad }));

      await page.fill('#modal form input[name="retain"]', "  90d  ");
      await page.fill('#modal form input[name="interval"]', "24h");
      await page.fill('#modal form input[name="add_future"]', "4");
      await page.click("#modal form button[type=submit]");
      const saved = await until(async () => {
        const r = (await readAs(page, "/api/rotation")).body;
        return r && r.retain === "90d" ? r : null;
      });
      check("rotation", "Save stores retention, interval and future partitions as a custom setting",
        !!saved && saved.interval === "24h" && saved.add_future === 4 && saved.source === "override", JSON.stringify(saved));

      // A second save right away, without reloading.
      await page.evaluate(() => showRotationDialog());
      await page.waitForSelector('#modal form input[name="retain"]', { timeout: 5000 });
      await page.fill('#modal form input[name="retain"]', "60d");
      await page.click("#modal form button[type=submit]");
      const second = await until(async () => {
        const r = (await readAs(page, "/api/rotation")).body;
        return r && r.retain === "60d" ? r : null;
      });
      check("rotation", "a second save right after the first lands too", !!second, JSON.stringify(second));

      const tab = await freshTab();
      try {
        await tab.evaluate(() => showRotationDialog());
        await tab.waitForSelector('#modal form input[name="retain"]', { timeout: 5000 });
        const shown = await tab.evaluate(() => {
          const f = document.querySelector("#modal form");
          return { retain: f.elements.retain.value, interval: f.elements.interval.value, add_future: f.elements.add_future.value };
        });
        check("rotation", "a fresh tab opens the dialog on the saved values",
          shown.retain === "60d" && shown.interval === "24h" && shown.add_future === "4", JSON.stringify(shown));
      } finally { await tab.close(); }
    });

    // ── the Connect draft ───────────────────────────────────────────────
    // An autosave, but a write: what was typed is stored on the server so a
    // reload brings it back. The password never is.
    await scene("draft", async () => {
      const typeDraft = async (tab) => {
        await tab.evaluate(() => { closeServersModal(); openServersModal(); });
        await tab.waitForSelector("#server-add", { timeout: 5000 });
        await tab.click("#server-add");
        await tab.waitForSelector('#server-form[data-connect] input[name="source_host"]', { timeout: 5000 });
        await tab.fill('#server-form-mount input[name="source_host"]', "db-e2e.example.com");
        await tab.fill('#server-form-mount input[name="source_port"]', "3307");
        await tab.fill('#server-form-mount input[name="source_user"]', "e2e_user");
        await tab.fill('#server-form-mount input[name="source_password"]', "e2e-draft-secret");
        await tab.fill('#server-form-mount input[name="name"]', "e2e draft name");
      };
      await typeDraft(page);
      const draft = await until(async () => {
        const r = (await readAs(page, "/api/servers/draft")).body;
        return r && r.found && r.draft && r.draft.name === "e2e draft name" && r.draft.source_user === "e2e_user" ? r : null;
      });
      const raw = JSON.stringify(draft || {});
      check("draft", "typing stores the draft on the server, without the password",
        !!draft && draft.draft.source_host === "db-e2e.example.com" && draft.draft.source_port === "3307"
        && !/e2e-draft-secret/.test(raw) && !("source_password" in (draft.draft || {})), raw);

      // A fresh tab has no copy of its own, so the form it reopens came from
      // the server. Its full-form link throws the draft away (a DELETE).
      const tab = await freshTab();
      try {
        await tab.waitForSelector('#server-form[data-connect] input[name="source_host"]', { timeout: 10000 });
        const shown = await tab.evaluate(() => {
          const f = document.getElementById("server-form").elements;
          return { host: f.source_host.value, port: f.source_port.value, user: f.source_user.value, name: f.name.value, pw: f.source_password.value };
        });
        check("draft", "a fresh tab reopens Connect filled from the server, password empty",
          shown.host === "db-e2e.example.com" && shown.port === "3307" && shown.user === "e2e_user"
          && shown.name === "e2e draft name" && shown.pw === "", JSON.stringify(shown));
        await tab.click("#connect-full-form");
        const gone = await until(async () => (await readAs(tab, "/api/servers/draft")).body?.found === false);
        check("draft", "Open the full form throws the stored draft away", !!gone, "draft still found");
        await tab.click("#server-cancel").catch(() => {});
      } finally { await tab.close(); }

      // Cancel on Connect is the other DELETE.
      await typeDraft(page);
      await until(async () => (await readAs(page, "/api/servers/draft")).body?.found === true);
      await page.click("#server-cancel");
      const cancelled = await until(async () => (await readAs(page, "/api/servers/draft")).body?.found === false);
      check("draft", "Cancel throws the stored draft away", !!cancelled, "draft still found");
      await page.evaluate(() => closeServersModal());
    });

    // ── Check and connect, refused ──────────────────────────────────────
    // console_e2e.mjs drives a refused check ("connect: a wrong password is
    // refused in plain words") and reads the notice. What it does not read
    // is the list: a refused check must not leave a server behind.
    await scene("connect-check", async () => {
      const r = await readAs(page, "/api/servers");
      const left = ((r.body && r.body.servers) || []).filter((s) => s.name === "e2e-draft");
      check("connect-check", "a refused Check and connect stores no server", r.status === 200 && left.length === 0,
        JSON.stringify({ status: r.status, left }));
    });

    // ── the MCP token ───────────────────────────────────────────────────
    await scene("mcp-token", async () => {
      // /mcp answers 401 to a credential it does not hold. Any other status
      // means the token was accepted, which is all this needs to know.
      const mcpStatus = (tok) => page.evaluate(async (tok) => {
        const r = await fetch("/mcp", { method: "POST", headers: { Authorization: "Bearer " + tok,
          "Content-Type": "application/json", Accept: "application/json, text/event-stream" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "e2e", version: "0" } } }) });
        return r.status;
      }, tok);
      const before = (await readAs(page, "/api/mcp-token")).body;
      await page.evaluate(() => navigate("connect"));
      const gen = await until(() => page.evaluate(() => {
        const b = Array.from(document.querySelectorAll("button")).find((x) => x.textContent === "Generate token");
        if (b) { b.click(); return true; }
        return false;
      }));
      // The plaintext, read from the token card only: the page has other
      // copyable rows (the address) drawn the same way.
      const shownToken = () => page.evaluate(() => {
        const card = Array.from(document.querySelectorAll(".cn-card")).find((c) => /Create a token/.test((c.querySelector(".cn-title") || {}).textContent || ""));
        return (card && (card.querySelector(".cn-urlrow code.cn-url") || {}).textContent) || "";
      });
      const minted1 = await until(shownToken);
      const st1 = (await readAs(page, "/api/mcp-token")).body;
      const auth1 = minted1 ? await mcpStatus(minted1) : 0;
      check("mcp-token", "Generate token stores a token the MCP endpoint accepts",
        before && before.managed === false && gen && !!minted1 && st1 && st1.managed === true && !!st1.created_at && auth1 !== 401,
        JSON.stringify({ before, gen, minted: !!minted1, st1, auth1 }));

      // New token replaces it: the old value stops working at once.
      await until(() => page.evaluate(() => {
        const b = Array.from(document.querySelectorAll("button")).find((x) => x.textContent === "New token");
        if (b) { b.click(); return true; }
        return false;
      }));
      const minted2 = await until(async () => {
        const t = await shownToken();
        return t && t !== minted1 ? t : "";
      });
      const auth2 = minted2 ? await mcpStatus(minted2) : 0;
      const authOld = minted1 ? await mcpStatus(minted1) : 0;
      check("mcp-token", "New token stores a new value and the old one stops working",
        !!minted2 && auth2 !== 401 && authOld === 401, JSON.stringify({ minted2: !!minted2, auth2, authOld }));

      const tab = await freshTab();
      try {
        await tab.evaluate(() => navigate("connect"));
        const said = await until(() => tab.evaluate(() => Array.from(document.querySelectorAll(".cn-card p"))
          .some((p) => /You already have a token/.test(p.textContent))));
        check("mcp-token", "a fresh tab says a token exists", !!said, "no 'You already have a token' line");
      } finally { await tab.close(); }

      await until(() => page.evaluate(() => {
        const b = Array.from(document.querySelectorAll("button")).find((x) => x.textContent === "Delete token");
        if (b) { b.click(); return true; }
        return false;
      }));
      const st3 = await until(async () => {
        const b = (await readAs(page, "/api/mcp-token")).body;
        return b && b.managed === false ? b : null;
      });
      const auth3 = minted2 ? await mcpStatus(minted2) : 0;
      check("mcp-token", "Delete token removes the stored token and it stops working", !!st3 && auth3 === 401,
        JSON.stringify({ st3, auth3 }));
    });

    // ── telemetry ───────────────────────────────────────────────────────
    // The switch writes the machine's consent file under $HOME, so it runs
    // where $HOME is a throwaway: in CI. A local run says loudly that it did
    // not run, and checkScenesRan refuses that skip in CI. This build has no
    // telemetry endpoint compiled in, so the card draws no switch: the scene
    // calls the function the switch calls.
    await scene("telemetry", async () => {
      if (!process.env.CI) {
        console.log("NOT RUN  save telemetry: it writes ~/.config/bintrail/telemetry.json; set CI=1 to run it on a throwaway home");
        skipped.push("telemetry");
        return;
      }
      const before = (await readAs(page, "/api/telemetry")).body;
      if (before && before.overridden) {
        // Owned by an environment variable: the server refuses, and the
        // stored choice does not move.
        const r = await page.evaluate(async (v) => { try { await api("/api/telemetry", { method: "POST", body: { enabled: v } }); return "ok"; } catch (e) { return e.message; } }, !before.consent);
        const after = (await readAs(page, "/api/telemetry")).body;
        check("telemetry", "with an environment variable in charge, the server refuses and nothing changes",
          /environment variable or launch flag/.test(r) && after.consent === before.consent, JSON.stringify({ r, before, after }));
        return;
      }
      await page.evaluate((v) => setTelemetry(v), !before.consent);
      const flipped = (await readAs(page, "/api/telemetry")).body;
      await page.evaluate((v) => setTelemetry(v), before.consent);
      const back = (await readAs(page, "/api/telemetry")).body;
      check("telemetry", "the switch stores the choice, and a second press stores it back",
        flipped.consent === !before.consent && back.consent === before.consent, JSON.stringify({ before, flipped, back }));
    });

    // ── Mark as read on capture skips ───────────────────────────────────
    // A tally is seeded on the boot index for this scene only, and removed
    // after: earlier scenes read that index's continuity.
    await scene("capture-skips", async () => {
      const at = new Date().toISOString().replace(/\.\d+Z$/, "Z");
      const seed = (n) => mysqlIdx("INSERT INTO stream_state (id, mode, last_checkpoint, server_id, capture_skips, capture_skips_ack) " +
        `VALUES (1,'position',NOW(),99,'{"column_count_mismatch":{"count":${n},"last_at":"${at}"}}',NULL) ` +
        `ON DUPLICATE KEY UPDATE capture_skips='{"column_count_mismatch":{"count":${n},"last_at":"${at}"}}', capture_skips_ack=NULL;`);
      try {
        seed(3);
        await page.evaluate(async (id) => { await switchServer(id); navigate("status"); }, byoId);
        const shown = await until(() => page.evaluate(() =>
          !!Array.from(document.querySelectorAll(".ack-actions button")).find((b) => b.textContent === "Mark as read")), 15000);
        if (!shown) throw new Error("no Mark as read button on Status for the seeded tally");
        // The tally moves after the page read it: the server refuses the stale
        // acknowledgement and says why.
        seed(5);
        await page.evaluate(() => { document.getElementById("toast-error").hidden = true; });
        await page.click(".ack-actions button");
        const stale = await until(() => page.evaluate(() => {
          const t = document.getElementById("toast-error");
          return t && !t.hidden ? t.textContent : "";
        }));
        const afterStale = (await readAs(page, "/api/status", byoId)).body;
        const h1 = afterStale && afterStale.stream && afterStale.stream.capture_health;
        check("capture-skips", "a tally that moved after the page read it is refused in the server's words",
          /more events were skipped/.test(stale) && !!h1 && h1.acknowledged !== true, JSON.stringify({ stale, h1 }));

        await page.evaluate(() => renderStatus());
        await until(() => page.evaluate(() =>
          !!Array.from(document.querySelectorAll(".ack-actions button")).find((b) => b.textContent === "Mark as read")), 15000);
        await page.click(".ack-actions button");
        const acked = await until(async () => {
          const s = (await readAs(page, "/api/status", byoId)).body;
          const h = s && s.stream && s.stream.capture_health;
          return h && h.acknowledged === true ? h : null;
        });
        check("capture-skips", "Mark as read stores the acknowledgement", !!acked, JSON.stringify(acked));
      } finally {
        try { mysqlIdx("DELETE FROM stream_state;"); } catch (_) { /* the run's teardown drops the database */ }
      }
    });

    // ── the console password ────────────────────────────────────────────
    // Last: setting a password ends every other session and swaps this
    // tab's credential. run.sh points the daemon at a throwaway auth file,
    // so no developer's own login is touched.
    await scene("password", async () => {
      const login = (pw) => page.evaluate(async (pw) => (await fetch("/api/auth/login", { method: "POST",
        headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username: "admin", password: pw }) })).status, pw);
      const P1 = "e2e-save-password-one", P2 = "e2e-save-password-two";
      // The first password can only be set from the access token, and only
      // when none is set: say so if the run did not start there.
      const auth0 = await page.evaluate(async () => { await gateCapabilities(); return capsCache.auth || null; });
      if (!auth0 || auth0.auth_kind !== "token" || auth0.password_set !== false) {
        throw new Error("the password scene needs a token session and no password yet; capabilities.auth = " + JSON.stringify(auth0));
      }
      await page.evaluate(() => showPasswordDialog());
      await page.waitForSelector('#login-mount input[name="next"]', { timeout: 5000 });
      const first = await page.evaluate(() => !document.querySelector('#login-mount input[name="current"]'));
      await page.fill('#login-mount input[name="next"]', P1);
      await page.fill('#login-mount input[name="confirm"]', P1);
      await page.click("#login-mount button[type=submit]");
      await until(() => page.evaluate(() => !document.querySelector('#login-mount input[name="next"]')));
      const ok1 = await login(P1);
      check("password", "Set password stores it: signing in with it works", first && ok1 === 200, JSON.stringify({ first, ok1 }));

      // A wrong current password: the dialog shows the server's words and
      // the stored password stays the first one.
      await page.evaluate(async () => { await gateCapabilities(); showPasswordDialog(); });
      await page.waitForSelector('#login-mount input[name="current"]', { timeout: 5000 });
      await page.fill('#login-mount input[name="current"]', "not-the-password");
      await page.fill('#login-mount input[name="next"]', P2);
      await page.fill('#login-mount input[name="confirm"]', P2);
      await page.click("#login-mount button[type=submit]");
      const msg = await until(() => page.evaluate(() => (document.querySelector("#login-mount .form-msg.err") || {}).textContent || ""));
      const withNew = await login(P2);
      const withOld = await login(P1);
      check("password", "a wrong current password is refused in the server's words and changes nothing",
        /invalid current password/.test(msg) && withNew === 401 && withOld === 200, JSON.stringify({ msg, withNew, withOld }));
      await page.evaluate(() => closeLoginOverlay());
    });
  } finally {
    page.off("dialog", acceptDialog);
  }
  return skipped;
}

// farHour is a daily slot twelve hours away (HH:00 UTC), so a schedule saved
// by the scenes cannot fire during the run.
function farHour() {
  const h = (new Date().getUTCHours() + 12) % 24;
  return String(h).padStart(2, "0") + ":00";
}
