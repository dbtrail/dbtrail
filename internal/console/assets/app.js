// dbtrail console — vanilla-JS SPA over the read-only JSON API.
//
// No frameworks, no bundler, no third-party code (see assets/VENDOR.md). The
// design system lives in style.css; this file renders the sidebar-shell UI into
// #view, talks to /api/* with a bearer token, and selects a server per-request
// via the X-Bintrail-Server header.
//
// Security invariants kept from the prior frontend (do not regress):
//  1. The token comes from ?token= on first load, is stashed in sessionStorage,
//     and is stripped from the URL — it never lingers in the address bar.
//     Logins ALSO set an HttpOnly session cookie server-side (#1370) so a new
//     tab is already signed in; when sessionStorage is empty this file PROBES
//     a cheap authenticated endpoint and, on 200, runs cookie-only (no
//     Authorization header — the middleware accepts the cookie). Every
//     non-GET request must then send Content-Type: application/json: that is
//     the server's cookie-CSRF marker, and api() sets it unconditionally.
//  2. The X-Bintrail-Server header is captured INSIDE api() at dispatch time, so
//     an in-flight request keeps the server it was fired for.
//  3. Every async render captures `serverGen` before its await and bails if a
//     server switch happened mid-flight (no cross-server repaint).
//  4. Capability gating toggles the `.cap-on` class on [data-capability] nodes.
//  5. CSV export (EVENT_CSV_COLUMNS) and the JSON view stay in lockstep: both
//     mirror whatever eventDTO serializes server-side (including connection_id,
//     per epic #701 D1) — CSV is not a separate, narrower boundary to maintain.
//  6. The DOM is built with el()/textContent only — no innerHTML anywhere. The
//     one string→DOM path is svgEl(), which DOMParses STATIC icon constants
//     (never data) into SVG nodes. No value from the API touches markup.
"use strict";

// ── constants ──────────────────────────────────────────────────────────────

const TOKEN_KEY = "bintrail_console_token";
const SERVER_KEY = "bintrail_console_server";
const ONBOARD_KEY = "bintrail_console_onboarded";

// The generated DuckDB views file, named in two places that must not drift:
// the card that builds it (on Connect AI, #1573) and the take-away lane on
// Snapshots that downloads the default one. Shared rather than guarded -- a
// constant cannot disagree with itself, and a test comparing two literals
// only reports the drift after someone ships it.
const DUCKDB_VIEWS_FILE = "views.sql";

// The card's own class, worn by duckdbPanel and resolved by the browser test.
// The take-away lane's jump that also resolved it is gone (#1573: the lane
// downloads the file itself), but one declaration still beats two literals
// that drift the first time the card is restyled. The bare
// class is a pure JS/e2e query hook; what style.css addresses is the DERIVED
// `-body` class (the card body's padding), so a rename also walks through
// there and through the e2e's selectors.
const DUCKDB_CARD_CLASS = "cn-dk";

// Export columns. connection_id is INCLUDED (epic #701 D1 — no longer a
// gated field on the events API; CSV mirrors the JSON view exactly).
const EVENT_CSV_COLUMNS = [
  "event_id", "event_timestamp", "schema_name", "table_name", "event_type",
  "pk_values", "changed_columns", "gtid", "connection_id", "binlog_file",
  "start_pos", "end_pos", "row_before", "row_after",
];

// event_type → badge modifier class.
// Ceiling for an Events export (#1297). Must track eventsMaxLimit in
// internal/console/api.go — the server clamps to it regardless, so a larger
// number here would only make the UI promise more than it delivers.
const EVENT_EXPORT_MAX = 1000;
const BADGE_CLASS = { UPDATE: "b-update", INSERT: "b-insert", DELETE: "b-delete" };
function badgeClass(t) { return BADGE_CLASS[t] || "b-baseline"; }

const ROUTES = ["overview", "events", "schema-changes", "timetravel", "recover", "sql", "status", "storage",
  // Storage was a drawer: seven cards from five unrelated concerns (#1543).
  // What happens to your data over time. Old /storage addresses land on
  // Retention through ROUTE_ALIASES; its other half, This daemon, was
  // dissolved in #1867 and /daemon lands on Status.
  "retention", "connect",
  // Access profiles (#1445): author the flags/profiles/rules a data profile
  // enforces. Not monitor-gated: the standalone serve can author too, the
  // write goes to the selected server's index, not to daemon state.
  "access-profiles",
  // Snapshots (#1573). One page for the three that used to answer one
  // question between them: what copies do I have (/baselines), are they
  // restorable (/verification), and where and how often are they made
  // (/backup-settings). Nobody could tell whether their data was safe
  // without visiting all three, and each one alone read as complete. The
  // three old addresses are gone from here on purpose: ROUTE_ALIASES sends
  // each where its page went — two to a section of this one, /baselines to
  // its top — before anything asks whether it is a route, so they never
  // render as themselves again.
  "snapshots"];

// DOCS_PAGES maps a route to its page on www.dbtrail.com/docs (#1450), the
// separately authored docs site. NOT this repo's docs/*.md: the site does not
// serve those, and it answers HTTP 200 with a small shell for ANY /docs/
// path, so neither a repo file nor a status code proves a link resolves.
// assets_docs_links_test.go pins this table exactly and, with
// BINTRAIL_CHECK_DOCS_LINKS=1 (and a daily workflow), fetches each page and
// requires its identity tag naming that slug, refusing redirects (#1645).
// pageHead renders one plain link beside the title. Nothing here fetches:
// air-gapped consoles are a first-class deployment, and a link is inert
// offline. A view without a page of its own gets NO link on purpose
// (Overview, Status, SQL): a link to the docs index would teach people the
// button is noise. Extension views are out of scope.
const DOCS_BASE = "https://www.dbtrail.com/docs/";
const DOCS_PAGES = {
  events: "guides/recovery",
  recover: "guides/recovery",
  // The three merged pages had three pages of their own on the site
  // (guides/backup-strategy, guides/verify, settings/backups). Until the
  // site carries one page for Snapshots, the header link opens the strategy
  // guide, which covers the whole job and links to the other two; the
  // Checks section and the per-server rows link to them directly as well,
  // which is what keeps the daily check fetching them. That check is also
  // why this cannot point at a page that is not written yet.
  snapshots: "guides/backup-strategy",
  storage: "guides/capacity-planning",
  connect: "claude/setup",
};

const MON_STATE_TITLES = {
  failed: "connection is failing and retrying automatically; press Start for details",
  stalled: "connected, but hasn't made progress for several minutes",
  lost_position: "some old changes were deleted before DBTrail could capture them; those are permanently lost, but current changes are still being captured",
};

// A startup step a "pending" stream can sit in for minutes. It replaces the
// chip's word so the row says what is happening instead of "PENDING" for half
// an hour; the underlying state is untouched, so Start/Stop still behave as
// they do for any pending stream.
const MON_PHASES = {
  resume_cleanup: { text: "CLEANING UP", title: "clearing changes the previous run had already saved, so they are not counted twice; capture starts when it finishes. On a large index this takes minutes." },
  // #1708: no cleanup of this run has started. One from an earlier run is
  // still running on the index, and a second one would only fail on its locks.
  resume_cleanup_waiting: { text: "WAITING FOR CLEANUP", title: "an earlier cleanup is still running on the index; capture starts when it finishes." },
};

// A failed state whose cause the daemon names (monitor_error_code) says that
// cause instead of the generic line. Keyed by the code, never by error text.
const MON_ERROR_TITLES = {
  same_replication_id: "something else is reading this database's changes with the same replication id, so the two keep disconnecting each other; stop capture for this server in one of the two installations",
};

// monitorChip renders the monitoring chip for a server row, phase included.
// Both the Servers list and the Settings list call it, so the two cannot drift.
function monitorChip(s) {
  const phase = s.monitor_phase && MON_PHASES[s.monitor_phase];
  if (phase) return el("span", { class: "chip chip-mon", text: phase.text, title: phase.title + (s.monitor_phase_detail ? " (" + s.monitor_phase_detail + ")" : "") });
  const cause = s.monitor_state === "failed" && MON_ERROR_TITLES[s.monitor_error_code];
  return el("span", { class: "chip chip-mon", text: s.monitor_state.replace("_", " ").toUpperCase(), title: cause || MON_STATE_TITLES[s.monitor_state] || ("monitoring " + s.monitor_state) });
}

// sameReplicationIdLines are the words for a stream the source dropped
// because another reader connected with its replication id (error code
// "same_replication_id"): what is going on, what it means, what to do. One
// place, so the Overview card and the first-run list cannot drift. The cause
// is named as likely, never as certain: any reader with the same id gets the
// source to send this error. retrying false is a supervisor that gave up, so
// nothing is interrupting anyone any more and capture waits for Start.
function sameReplicationIdLines(retrying) {
  return [
    "Something else is reading this database's changes with the same replication id, the number a reader gives the database to identify itself. Most likely it is another DBTrail installation pointed at the same database.",
    retrying
      ? "The database keeps one reader per id, so each one disconnects the other when it reconnects. Capture keeps being interrupted on both sides while both are connected. Nothing is lost: each one resumes where it stopped."
      : "The database keeps one reader per id, so each one disconnected the other when it reconnected. After hours of that, this installation stopped trying. Capture resumes where it stopped once it starts again.",
    retrying
      ? "To fix it, stop capture for this server in one of the two installations."
      : "To fix it, stop capture for this server in one of the two installations. If this is the one you keep, start it again.",
  ];
}

// rawErrorFold keeps an error as the daemon reported it one click away, under
// the words that explain it.
function rawErrorFold(text) {
  return el("details", { class: "flow-recipe" }, el("summary", { text: "Technical details" }), el("div", { class: "fr-detail", text: text }));
}

// Static decorative SVGs (module constants — parsed by svgEl via DOMParser).
const ICONS = {
  search: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round"><circle cx="11" cy="11" r="7"></circle><path d="M21 21l-4.3-4.3"></path></svg>`,
  caret: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 6l6 6-6 6"></path></svg>`,
  file: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" style="width:16px;height:16px"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"></path><path d="M14 3v5h5"></path></svg>`,
  warn: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" style="width:15px;height:15px"><path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/><path d="M12 9v4M12 17h.01"/></svg>`,
  calendar: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="16" rx="2"></rect><path d="M8 3v4M16 3v4M3 10h18"></path></svg>`,
  ext: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>`,
  external: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M14 4h6v6"></path><path d="M20 4l-9 9"></path><path d="M19 14v4a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h4"></path></svg>`,
  refresh: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><path d="M21 3v6h-6"/></svg>`,
  // The flow's boxes (Overview): a bucket and a duck drawn here for the
  // reader; the MySQL box shows the vendor's logo (assets/mysql-logo.png,
  // see VENDOR.md) and the DBTrail box the brand lockup in white.
  duck: `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M14.2 3.2c2.3 0 4.1 1.8 4.1 4.1 0 .9-.3 1.7-.8 2.4l3.2-.4c.7-.1 1 .8.5 1.2l-2.1 1.5c.4.9.6 1.9.6 2.9 0 3.6-3.2 6.4-7.4 6.4H8.6C5.4 21.3 3 19 3 16.2c0-2.6 2.1-4.7 4.8-4.9h2.3V7.3c0-2.3 1.8-4.1 4.1-4.1zm.6 3.1c-.5 0-.9.4-.9.9s.4.9.9.9.9-.4.9-.9-.4-.9-.9-.9z"/></svg>`,
  layers: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="6" rx="2"/><rect x="3" y="14" width="18" height="6" rx="2"/></svg>`,
  pin: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 21s-6-5.2-6-10a6 6 0 0 1 12 0c0 4.8-6 10-6 10z"/><circle cx="12" cy="11" r="2.2"/></svg>`,
  clock: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>`,
  shield: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l7 3v5c0 5-3.5 8.5-7 10-3.5-1.5-7-5-7-10V6z"/><path d="M9 12l2 2 4-4"/></svg>`,
  folder: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/></svg>`,
  check: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M8 12.5l2.6 2.6L16.5 9"/></svg>`,
  cross: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M9 9l6 6M15 9l-6 6"/></svg>`,
  bucket: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5.5" rx="8.5" ry="2.8"/><path d="M3.5 5.5l2 13.2c.2 1.3 3.1 2.3 6.5 2.3s6.3-1 6.5-2.3l2-13.2"/></svg>`,
};

// ── module state ─────────────────────────────────────────────────────────────

let TOKEN = "";
let currentServer = "";       // X-Bintrail-Server target ("" = backend default)
let defaultServerId = "";
let serverGen = 0;            // bumped on every server switch (staleness guard)
let viewGen = 0;              // bumped on every route render (staleness guard)
let capsCache = {};           // last /api/capabilities for the selected server
let capsKnown = false; // whether capsCache came from a payload we actually read (a failed check degrades to {})
// noCaptureNotes remembers, per server id, why a save did not start capture
// (#1607), so the row keeps its reason across list rebuilds in this session;
// serverRow re-derives the condition before showing it, so a server that
// later gains a source or starts streaming loses the note on its own.
const noCaptureNotes = {};
let extViews = [];            // extension views advertised for the selected server (embedding builds)
let extSettings = [];         // extension settings panels advertised for this SESSION (permission-gated, not per-server)
let lastSQL = "";             // last generated undo SQL (for copy/download)
let lastEvents = [];          // last rendered (filtered, capped) event page
// Events keyset paging (#1297). evPages[k] is the `before` cursor that opens
// page k (page 0 opens with none) plus how many events precede it, so the
// header can say "showing 201–300" without an OFFSET. Cursors are echoed back
// from the server, never built here.
let evPages = [{ before: null, offset: 0 }];
let evPageIdx = 0;
// The last Events request (API params + client-side refine terms), captured so
// Export can re-run the SAME search across every page instead of dumping only
// the page on screen. See exportEvents.
let evLastQuery = null;
let pendingRecover = null;    // event context carried into Recover via "Undo"
let schemaCache = null;       // cached schema list for the selected server
const tablesCache = new Map();// schema → tables[]
let cursorIdx = -1;           // keyboard cursor row on Events
let serversEmpty = false;     // no listed servers (hidden-boot fresh install)

// ── token bootstrap ────────────────────────────────────────────────────────

(function bootstrapToken() {
  const urlToken = new URLSearchParams(location.search).get("token");
  if (urlToken) {
    // Assign and strip the URL BEFORE touching storage: with storage
    // disabled the old ordering threw first, leaving the token both unused
    // and sitting in the address bar.
    TOKEN = urlToken;
    history.replaceState(null, "", location.pathname);
    try { sessionStorage.setItem(TOKEN_KEY, urlToken); } catch (_) { /* in-memory only */ }
  } else {
    try { TOKEN = sessionStorage.getItem(TOKEN_KEY) || ""; } catch (_) {}
  }
  try { currentServer = sessionStorage.getItem(SERVER_KEY) || ""; } catch (_) {}
})();

function setCurrentServer(id) {
  currentServer = id || "";
  try { sessionStorage.setItem(SERVER_KEY, currentServer); } catch (_) {}
}

// ── tiny DOM helpers (no data ever assigned as HTML) ─────────────────────────

// prefersReducedMotion reports the OS-level setting for motion started from
// JavaScript, which no media query in style.css can reach.
//
// Read at CALL time rather than cached once: the setting can change while the
// console is open — both macOS and Windows apply it live — and this console
// stays open for hours during an incident.
function prefersReducedMotion() {
  return !!(window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches);
}

function el(tag, attrs, ...kids) {
  const n = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (v == null || v === false) continue;
      if (k === "class") n.className = v;
      else if (k === "text") n.textContent = v;
      else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
      else n.setAttribute(k, v);
    }
  }
  for (const kid of kids) {
    if (kid == null) continue;
    n.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
  }
  return n;
}
function opt(value, label) { const o = el("option", { value }); o.textContent = label; return o; }
function clear(n) { if (n) n.replaceChildren(); }
function $(sel, root = document) { return root.querySelector(sel); }
function $all(sel, root = document) { return Array.from(root.querySelectorAll(sel)); }
// svgEl parses a STATIC, trusted SVG constant into a detached SVG node. It is
// NOT an innerHTML sink: image/svg+xml parsing never executes script, and the
// argument is always a module constant — never server or user data.
// XML parsing applies no default namespace, so a constant without an explicit
// xmlns yields a namespace-less <svg>: it takes up its CSS box and paints
// NOTHING. Every icon here was in that state — the extension nav item, for one,
// has been rendering label-only. Inject the namespace rather than requiring
// each constant to remember it, so the next icon added cannot reintroduce this.
const SVG_NS = `xmlns="http://www.w3.org/2000/svg"`;
function svgEl(s) {
  if (!s.includes("xmlns=")) s = s.replace("<svg", "<svg " + SVG_NS);
  const doc = new DOMParser().parseFromString(s, "image/svg+xml");
  return document.importNode(doc.documentElement, true);
}
// icon(name) → a <span> wrapping the named static SVG.
function icon(name, cls) {
  const span = el("span", { class: cls || "" });
  if (ICONS[name]) span.append(svgEl(ICONS[name]));
  return span;
}

function valueToString(v) {
  if (v === null || v === undefined) return "NULL";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

const VIEW = () => document.getElementById("view");

// ── api ──────────────────────────────────────────────────────────────────────

async function api(path, opts = {}) {
  // No Authorization header without a token: a cookie-bootstrapped tab (fresh
  // tab, HttpOnly session cookie, empty sessionStorage) authenticates via the
  // cookie the browser attaches on its own (same-origin fetch sends cookies
  // by default).
  const headers = TOKEN ? { Authorization: "Bearer " + TOKEN } : {};
  if (currentServer) headers["X-Bintrail-Server"] = currentServer; // captured at dispatch
  // Content-Type on EVERY state-changing request, body or not (a body-less
  // POST like logout included): the server's cookie-auth CSRF belt requires
  // the application/json marker on non-GET methods, and sending it under
  // Bearer too keeps the request shape uniform.
  const method = (opts.method || "GET").toUpperCase();
  if (opts.body || (method !== "GET" && method !== "HEAD")) headers["Content-Type"] = "application/json";
  const res = await fetch(path, {
    method: opts.method || "GET",
    headers,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
    // AbortController support (#1363): callers that show a cancelable busy
    // affordance pass a signal; an abort rejects with err.name "AbortError".
    signal: opts.signal,
  });
  const text = await res.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch (_) {
      // A non-JSON body is the server's error text on a non-OK response, or a
      // server malfunction on an OK one — surface it; never let a stray HTML
      // error page render as an empty success. (An EMPTY body stays null: the
      // 204 from DELETE /api/servers/{id} is a legitimate no-content success.)
      if (!res.ok) throw apiError(res.status, text || "HTTP " + res.status);
      throw new Error("malformed response from " + path);
    }
  }
  if (!res.ok) {
    // 401 = the bearer credential is dead (expired session, rotated token).
    // One central chokepoint clears state and raises the sign-in gate; every
    // in-flight render bails on the serverGen bump.
    if (res.status === 401) handleUnauthorized();
    throw apiError(res.status, (data && data.error) || "HTTP " + res.status);
  }
  return data;
}

// apiText is api() for a text/plain endpoint. Same auth, same server header,
// same central 401 handling — only the parsing differs, because views.sql is a
// SQL file and api()'s JSON.parse would reject it as malformed.
async function apiText(path) {
  const headers = TOKEN ? { Authorization: "Bearer " + TOKEN } : {};
  if (currentServer) headers["X-Bintrail-Server"] = currentServer;
  const res = await fetch(path, { headers });
  const text = await res.text();
  if (!res.ok) {
    if (res.status === 401) handleUnauthorized();
    // An error body here is the API's JSON {error}; fall back to the raw text
    // so a proxy's HTML error page still says something useful.
    let msg = text || "HTTP " + res.status;
    try { const j = JSON.parse(text); if (j && j.error) msg = j.error; } catch (_) { /* raw text it is */ }
    throw apiError(res.status, msg);
  }
  return text;
}

// OV_REQUEST_MS bounds one request the Overview makes on its own. The console
// sets no write timeout, on purpose, so nothing else ends a request that
// neither answers nor fails: an ALTER holding a table's metadata lock (MySQL
// waits a year by default) or an index host that went away (minutes of TCP
// retransmission) would leave this page sitting in a turn that never ends,
// with no next turn armed, no failure counted, and the tab coming back
// finding it busy. A rejection is something the loop already reports well.
const OV_REQUEST_MS = 20000;

// apiWithin is api() with a deadline. The abort surfaces as a rejection with
// a sentence the page can show, rather than the browser's own wording.
function apiWithin(path, ms) {
  const ctl = new AbortController();
  let timedOut = false;
  const t = setTimeout(() => { timedOut = true; ctl.abort(); }, ms);
  return api(path, { signal: ctl.signal }).then((d) => { clearTimeout(t); return d; }, (err) => {
    clearTimeout(t);
    throw timedOut ? apiError(0, "the server did not answer within " + Math.round(ms / 1000) + "s") : err;
  });
}

function apiError(status, message) {
  const err = new Error(message);
  err.status = status;
  return err;
}

// ── auth: login overlay, logout, password dialog ─────────────────────────────
//
// Password login is OPTIONAL (configured with `bintrail-console user
// set-password`); the ?token= bootstrap stays the default. A successful login
// returns a session token that drops into the SAME `TOKEN` slot the static
// token uses — api() and the X-Bintrail-Server flow never know the difference.
// The server reports how this tab authenticated (capabilities.auth.auth_kind),
// which gates the logout affordance via [data-auth]/.auth-on.

let unauthorizedHandled = false; // first 401 wins; later ones no-op
let loginGateRaised = false;     // the sign-in gate owns the screen — ⌘K and onboarding stay inert

// fetchAuthInfo asks the unauthenticated probe whether password login exists.
// Raw fetch: no bearer yet, and its failure must not recurse into the 401
// chokepoint.
async function fetchAuthInfo() {
  const res = await fetch("/api/auth");
  if (!res.ok) throw new Error("HTTP " + res.status);
  return res.json();
}

// probeCookieSession: a fresh tab has no sessionStorage token (it is per-tab),
// but a login in another tab left the HttpOnly session cookie — try one cheap
// authenticated GET before raising the sign-in gate. /api/servers is the
// lightest authenticated read (registry only, touches no index DB), the
// browser attaches the cookie on its own, and a 200 means the middleware
// accepted it: the tab then runs cookie-only with TOKEN empty. Raw fetch on
// purpose — a 401 here is the NORMAL signed-out case and must not recurse
// into handleUnauthorized's "session expired" messaging.
async function probeCookieSession() {
  try {
    const res = await fetch("/api/servers");
    return res.ok;
  } catch (_) { return false; }
}

// handleUnauthorized is api()'s 401 chokepoint: the bearer is dead, so clear
// every credential-scoped cache (same hygiene as switchServer), invalidate
// in-flight renders, and raise the sign-in gate.
async function handleUnauthorized() {
  if (unauthorizedHandled) return;
  unauthorizedHandled = true;
  clearAuthState();
  let auth = {};
  try { auth = await fetchAuthInfo(); } catch (_) {}
  // After a `user remove` the console can be back in first-run setup.
  if (auth.setup) { showLoginOverlay({ setup: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
  // Token mode has no session to expire and no form to "sign in" to — say what
  // actually happened (the stored token is no longer accepted). An advertised
  // external provider also mints real sessions, so with one present the
  // session-expired copy is the accurate one (SSO-only deployments never had
  // an access token at all).
  const msg = auth.password_login || auth.sso_start ? "Session expired; sign in again." : "This access token is no longer valid.";
  showLoginOverlay({ passwordLogin: !!auth.password_login, message: msg, ssoName: auth.sso_name, ssoStart: auth.sso_start });
}

// clearAuthState drops every credential-scoped cache on sign-out. capsCache
// MUST be cleared too: a stale capsCache.auth would keep the command palette
// offering "Change password…"/"Log out" in a signed-out tab, and
// running either evicts the gate from #login-mount.
function clearAuthState() {
  TOKEN = "";
  try { sessionStorage.removeItem(TOKEN_KEY); } catch (_) {}
  serverGen++;
  schemaCache = null;
  tablesCache.clear();
  pendingRecover = null;
  lastSQL = "";
  lastEvents = [];
  capsCache = {};
  capsKnown = false;
  routeArrivedFrom = "";
  vfyEpoch++;
  vfyLive.clear();
  vfyFollowing.clear();
  vfyAnnounce.clear();
  vfyView = null;
  applyAuthGate();
}

// showLoginOverlay raises the sign-in gate in #login-mount. Three modes:
//   - setup: true        → first-run "create your password" form (no auth yet)
//   - passwordLogin: true → the username/password sign-in form
//   - passwordLogin: false → a pointer at the printed ?token= URL (token mode)
// The scrim deliberately does NOT close on outside-click — it is a gate.
function showLoginOverlay(opts) {
  opts = opts || { passwordLogin: true };
  loginGateRaised = true;
  // A notice would keep the gate inert under it (#1769): a 401 on Save or Test
  // opens "Could not save" first and raises the gate a moment later.
  closeNotice();
  // Clear the workspace: the prior session's events/recover SQL must not stay
  // readable behind the blurred scrim (or after the gate is dismissed by a
  // dialog mounted in the same slot).
  clear(VIEW());
  const mount = document.getElementById("login-mount");
  // .login-gate marks THIS scrim as the full-screen brand canvas (#1371).
  // showPasswordDialog() mounts its panel in the same slot but is a task modal
  // over an authenticated workspace, so it keeps the ordinary translucent scrim.
  const scrim = el("div", { class: "modal-scrim show login-gate" });
  const panel = el("div", { class: "modal login-panel", role: "dialog", "aria-label": opts.setup ? "Set up DBTrail" : "Sign in" });
  panel.append(el("h2", { class: "modal-title", text: "DBTrail" }));

  if (opts.setup) {
    panel.append(el("p", { class: "modal-desc", text: "First run: create a username and password for DBTrail." }));
    const form = el("form", { class: "login-form", id: "login-form" });
    form.append(el("label", { class: "field" },
      el("span", { class: "field-label", text: "Username" }),
      el("input", { class: "input", name: "username", value: "admin", autocomplete: "username", spellcheck: "false" })));
    form.append(el("label", { class: "field" },
      el("span", { class: "field-label", text: "Password" }),
      el("input", { class: "input", name: "password", type: "password", autocomplete: "new-password" })));
    form.append(el("label", { class: "field" },
      el("span", { class: "field-label", text: "Confirm password" }),
      el("input", { class: "input", name: "confirm", type: "password", autocomplete: "new-password" })));
    const msg = el("div", { class: "form-msg", id: "login-msg" });
    const foot = el("div", { class: "modal-foot" });
    foot.append(el("button", { class: "btn btn-primary", type: "submit", text: "Create & sign in" }));
    form.append(foot);
    form.append(msg);
    form.addEventListener("submit", (e) => { e.preventDefault(); submitSetup(form, msg); });
    panel.append(form);
    appendSSOEntry(panel, opts);
    scrim.append(panel);
    mount.replaceChildren(scrim);
    form.elements.password.focus();
    return;
  }

  if (!opts.passwordLogin) {
    if (opts.message) panel.append(el("p", { class: "modal-desc", text: opts.message }));
    // The printed-?token=-link hint only fits token mode. When the probe
    // advertises an external provider, that provider may be the SOLE
    // credential (no token, no password — the server allows it), so pointing
    // at a nonexistent token link would be false guidance; the SSO entry
    // below is the sign-in path.
    if (opts.ssoStart) {
      panel.append(el("p", { class: "modal-desc", text: "Sign in with the provider below to continue." }));
    } else {
      panel.append(el("p", { class: "modal-desc", text: "Open the link that bintrail-console printed when it started. It carries the access token the web interface needs." }));
    }
    appendSSOEntry(panel, opts);
    scrim.append(panel);
    mount.replaceChildren(scrim);
    return;
  }

  panel.append(el("p", { class: "modal-desc", text: "Sign in. This DBTrail is read-only." }));
  const form = el("form", { class: "login-form", id: "login-form" });
  form.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "Username" }),
    el("input", { class: "input", name: "username", value: "admin", autocomplete: "username", spellcheck: "false" })));
  form.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "Password" }),
    el("input", { class: "input", name: "password", type: "password", autocomplete: "current-password" })));
  // Its own message node: formMsg() is hard-wired to #server-form-msg.
  const msg = el("div", { class: "form-msg", id: "login-msg" });
  if (opts.message) { msg.classList.add("err"); msg.textContent = opts.message; }
  const foot = el("div", { class: "modal-foot" });
  foot.append(el("button", { class: "btn btn-primary", type: "submit", text: "Sign in" }));
  form.append(foot);
  form.append(msg);
  form.addEventListener("submit", (e) => { e.preventDefault(); submitLogin(form, msg); });
  panel.append(form);
  appendSSOEntry(panel, opts);
  scrim.append(panel);
  mount.replaceChildren(scrim);
  form.elements.password.focus();
}

// appendSSOEntry adds the external-login entry ("Continue with <name>") under
// whatever the gate mode rendered, when the /api/auth probe advertised a
// provider (sso_start/sso_name → opts.ssoStart/opts.ssoName). A plain <a> on
// purpose — full navigation, never fetch: the provider owns the whole flow
// and lands back on /?token=<session>, reusing the existing token bootstrap.
function appendSSOEntry(panel, opts) {
  if (!opts.ssoStart) return;
  panel.append(el("div", { class: "login-divider", text: "or" }));
  panel.append(el("a", { class: "btn login-sso", href: opts.ssoStart, text: "Continue with " + (opts.ssoName || "single sign-on") }));
}

// submitSetup posts the first-run credential to /api/auth/setup (raw fetch:
// no bearer yet) and, on success, drops the gate on the returned session.
async function submitSetup(form, msg) {
  const password = form.elements.password.value;
  if (password !== form.elements.confirm.value) { loginMsg(msg, "Passwords do not match."); return; }
  const body = { username: form.elements.username.value.trim(), password };
  let res;
  try {
    res = await fetch("/api/auth/setup", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (_) { loginMsg(msg, "Network error. Is DBTrail still running?"); return; }
  if (res.status === 403) {
    // Setup closed under us (a concurrent `user set-password`, another tab, or
    // a CLI set it first). Unlike login, the setup endpoint self-disables —
    // re-probe and switch the gate to the sign-in form instead of leaving the
    // operator stuck re-posting to a now-closed endpoint.
    let auth = {};
    try { auth = await fetchAuthInfo(); } catch (_) {}
    showLoginOverlay({ passwordLogin: !!auth.password_login, message: "A password was already created. Sign in.", ssoName: auth.sso_name, ssoStart: auth.sso_start });
    return;
  }
  if (!res.ok) {
    let m = "Could not set the password.";
    try { m = (await res.json()).error || m; } catch (_) {}
    if (res.status === 429) m = "Too many attempts; wait " + (res.headers.get("Retry-After") || "60") + "s.";
    loginMsg(msg, m);
    return;
  }
  let data;
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Unexpected response from the server; try again."); return; }
  TOKEN = data.token || "";
  try { sessionStorage.setItem(TOKEN_KEY, TOKEN); } catch (_) {}
  unauthorizedHandled = false;
  loginGateRaised = false;
  closeLoginOverlay();
  await bootSequence();
}

// closeLoginOverlay empties the #login-mount slot. It is used both to dismiss
// the password dialog (authenticated; the gate was never up) and, after a
// successful login, by submitLogin. It does NOT lower loginGateRaised on its
// own — only authenticating does, so the password dialog can never be the
// thing that drops the gate: showPasswordDialog bails on `if (loginGateRaised)
// return`, and it is only reachable from ⌘K, which itself no-ops while the gate
// is up and is only offered once authenticated (capsCache.auth populated).
function closeLoginOverlay() { document.getElementById("login-mount").replaceChildren(); }

function loginMsg(node, text) { node.classList.add("err"); node.textContent = text; }

// submitLogin posts the credentials with a RAW fetch: there is no bearer yet,
// and a 401 here means "wrong password", which must not recurse into
// handleUnauthorized.
async function submitLogin(form, msg) {
  const body = {
    username: form.elements.username.value.trim(),
    password: form.elements.password.value,
  };
  let res;
  try {
    res = await fetch("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (_) { loginMsg(msg, "Network error. Is DBTrail still running?"); return; }
  if (res.status === 429) {
    const retry = res.headers.get("Retry-After");
    loginMsg(msg, "Too many attempts; wait " + (retry ? retry + "s" : "a minute") + " and retry.");
    return;
  }
  if (!res.ok) {
    let m = "Invalid username or password.";
    if (res.status !== 401) { try { m = (await res.json()).error || m; } catch (_) {} }
    loginMsg(msg, m);
    return;
  }
  let data;
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Unexpected response from the server; try again."); return; }
  TOKEN = data.token || "";
  try { sessionStorage.setItem(TOKEN_KEY, TOKEN); } catch (_) {}
  unauthorizedHandled = false;
  loginGateRaised = false; // authenticating is the ONLY thing that drops the gate
  closeLoginOverlay();
  await bootSequence();
}

async function doLogout() {
  try { await api("/api/auth/logout", { method: "POST" }); } catch (_) { /* dead session = already out */ }
  clearAuthState();
  unauthorizedHandled = false;
  // Session auth no longer implies password login: an external provider mints
  // normal sessions too, so re-probe the gate mode like handleUnauthorized
  // does — an SSO-only deployment must get its SSO entry back (not a password
  // form every submit 401s), and after a `user remove` the console can be in
  // first-run setup again. Probe failure falls back to the password form.
  let auth = null;
  try { auth = await fetchAuthInfo(); } catch (_) {}
  if (!auth) { showLoginOverlay({ passwordLogin: true, message: "Signed out." }); return; }
  if (auth.setup) { showLoginOverlay({ setup: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
  showLoginOverlay({ passwordLogin: !!auth.password_login, message: "Signed out.", ssoName: auth.sso_name, ssoStart: auth.sso_start });
}

// applyAuthGate mirrors the [data-capability]/.cap-on pattern for auth-kind
// gated surfaces (the logout button). Server-derived: capabilities.auth tells
// this tab how it authenticated, so the affordance survives reloads.
function applyAuthGate() {
  const kind = (capsCache.auth && capsCache.auth.auth_kind) || "";
  $all("[data-auth]").forEach((n) => n.classList.toggle("auth-on", n.dataset.auth === kind));
}

// showPasswordDialog sets (token bootstrap) or rotates the console password.
// Mounted in #login-mount — never coexists with the login gate: it requires an
// authenticated tab, so it refuses while the gate is up (defense in depth —
// clearAuthState also strips the cmdk entries that could reach it signed-out).
function showPasswordDialog() {
  if (loginGateRaised) return;
  const firstSet = !(capsCache.auth && capsCache.auth.password_set);
  const mount = document.getElementById("login-mount");
  const scrim = el("div", { class: "modal-scrim show" });
  const panel = el("div", { class: "modal login-panel", role: "dialog", "aria-label": "Password" });
  panel.append(el("h2", { class: "modal-title", text: firstSet ? "Set password" : "Change password" }));
  panel.append(el("p", { class: "modal-desc", text: firstSet
    ? "Lets you sign in with a username and password instead of just the access token."
    : "Changing your password signs you out of every other open session." }));

  const form = el("form", { class: "login-form" });
  if (!firstSet) {
    form.append(el("label", { class: "field" },
      el("span", { class: "field-label", text: "Current password" }),
      el("input", { class: "input", name: "current", type: "password", autocomplete: "current-password" })));
  }
  form.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "New password" }),
    el("input", { class: "input", name: "next", type: "password", autocomplete: "new-password" })));
  form.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "Retype new password" }),
    el("input", { class: "input", name: "confirm", type: "password", autocomplete: "new-password" })));
  const msg = el("div", { class: "form-msg" });
  const foot = el("div", { class: "modal-foot" });
  foot.append(el("button", { class: "btn btn-primary", type: "submit", text: firstSet ? "Set password" : "Change password" }));
  foot.append(el("button", { class: "btn btn-ghost", type: "button", text: "Cancel", onclick: closeLoginOverlay }));
  form.append(foot);
  form.append(msg);
  form.addEventListener("submit", (e) => { e.preventDefault(); submitPasswordChange(form, msg, firstSet); });
  panel.append(form);
  scrim.append(panel);
  mount.replaceChildren(scrim);
}

// submitPasswordChange uses a RAW fetch with the live bearer: the endpoint
// answers 401 for "wrong current password", which must not trip api()'s
// dead-credential chokepoint.
async function submitPasswordChange(form, msg, firstSet) {
  const next = form.elements.next.value;
  if (next !== form.elements.confirm.value) { loginMsg(msg, "Passwords do not match."); return; }
  const body = {
    current_password: firstSet ? "" : form.elements.current.value,
    new_password: next,
  };
  let res;
  try {
    // Cookie-bootstrapped tabs have no TOKEN — the session cookie carries the
    // credential, and the JSON Content-Type doubles as the CSRF marker.
    const headers = TOKEN
      ? { Authorization: "Bearer " + TOKEN, "Content-Type": "application/json" }
      : { "Content-Type": "application/json" };
    res = await fetch("/api/auth/password", {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
  } catch (_) { loginMsg(msg, "Network error. Is DBTrail still running?"); return; }
  if (!res.ok) {
    let m = "HTTP " + res.status;
    try { m = (await res.json()).error || m; } catch (_) {}
    if (res.status === 429) m = "Too many attempts; wait " + (res.headers.get("Retry-After") || "60") + "s.";
    loginMsg(msg, m);
    return;
  }
  let data;
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Unexpected response from the server; try again."); return; }
  // Every other session just died; this tab continues on the fresh one.
  TOKEN = data.token || TOKEN;
  try { sessionStorage.setItem(TOKEN_KEY, TOKEN); } catch (_) {}
  closeLoginOverlay();
  toast("Password " + (firstSet ? "set" : "updated"));
  // password_set (and possibly auth_kind) changed server-side.
  try { await gateCapabilities(); } catch (_) {}
}

// ── toast / errors / warnings ─────────────────────────────────────────────────

// toast shows a transient notice. Use it for things that went RIGHT, or for
// neutral progress. Failures must not fade — see toastError.
//
// It writes to its OWN node, never the error node. An earlier attempt shared
// one element and had toast() yield to a visible error, which silently dropped
// messages this function also carries: "rotate your MCP token" after a display
// interruption (the token is already unrecoverable by then), "capture did NOT
// restart onto the new snapshot", export-truncation warnings. Each is reported
// nowhere else, and one undismissed error would have suppressed them all for
// the rest of the session. Two nodes, no contention.
function toast(msg) {
  const t = document.getElementById("toast");
  if (!t) return;
  clearTimeout(toast._t);
  t.textContent = msg;
  t.hidden = false;
  toast._t = setTimeout(() => { t.hidden = true; }, 2200);
}

// toastError shows a failure that stays until the operator dismisses it.
//
// A failure that disappears on its own is a failure nobody saw. The baseline
// privilege refusal is ~550 characters of remediation — which privilege to
// grant, the exact GRANT statement, and the alternative modes — and at the
// 2.2s auto-hide it was not merely easy to miss, it was unreadable. The
// operator was left with a button that did nothing and no way to recover the
// reason. Nothing here starts a timer.
//
// extra is an optional node shown under msg in the same entry: the snapshot
// failure card (#1986), whose GRANT and Copy button cannot be plain text.
// Entries are matched on msg AND extra's text, so two servers failing for
// different reasons under the same headline stay two entries, and each
// entry's node is carried over when a later failure rebuilds the stack.
function toastError(msg, extra) {
  const t = document.getElementById("toast-error");
  if (!t) return;
  // A second failure STACKS rather than replaces. These never auto-hide, so a
  // silent overwrite would destroy an unread failure with nothing to hint one
  // existed — two servers' baselines refusing together is the ordinary case.
  //
  // A REPEAT of a message already showing gets a count, not a dropped call.
  // Dropping it was wrong: several of these messages carry no server name
  // ("Startup checks failed", "Copy failed."), so failing on server B after
  // server A produced NO change on screen — indistinguishable from success.
  const prior = t.hidden ? [] : $all(".toast-msg", t).map((n) => ({
    text: n.dataset.msg || n.textContent,
    n: Number(n.dataset.count || "1"),
    extra: n.toastExtra || null,
  }));
  const same = (p) => p.text === msg && (p.extra ? p.extra.textContent : "") === (extra ? extra.textContent : "");
  const dupe = prior.find(same);
  if (dupe) dupe.n += 1;
  else prior.push({ text: msg, n: 1, extra: extra || null });
  // role=alert so a screen reader announces it; the visual persistence is
  // useless to someone who cannot see it fade. The node is unhidden BEFORE the
  // text lands: a live region that appears with its content already in place
  // is the shape screen readers routinely fail to announce.
  t.setAttribute("role", "alert");
  t.replaceChildren();
  t.hidden = false;
  const body = el("div", { class: "toast-body" });
  for (const m of prior) {
    const span = el("span", { class: "toast-msg", text: m.n > 1 ? m.text + "  (\u00d7" + m.n + ")" : m.text });
    span.dataset.msg = m.text;
    span.dataset.count = String(m.n);
    if (m.extra) { span.toastExtra = m.extra; span.append(m.extra); }
    body.append(span);
  }
  t.append(body);
  const close = el("button", { class: "toast-close", type: "button", "aria-label": "Dismiss all" });
  close.textContent = "\u2715";
  close.addEventListener("click", () => dismissToast());
  t.append(close);
}

function dismissToast() {
  const t = document.getElementById("toast-error");
  if (!t) return;
  t.hidden = true;
  t.removeAttribute("role");
  t.textContent = "";
}

// ESC dismisses a persistent error, matching every other dismissible surface
// in this console — but only as the LAST of them.
//
// cmdkKeydown closes the ⌘K palette WITHOUT stopping propagation
// (deliberately, see globalKeydown), so the same Escape reaches the document
// and would also destroy an unread error — the failure this whole change
// exists to prevent, arriving through a different door.
//
// Hence the CAPTURE phase (registered with `true` in init). Deciding on the
// bubble phase cannot work: by then globalKeydown has already emptied #modal
// and cmdkKeydown has already emptied #cmdk-mount, so the very state this
// guard reads to yield the key has been erased by the handlers it is yielding
// to, and it would dismiss the notice anyway. Verified in a browser: on the
// bubble phase one Escape closes a modal AND wipes the notice behind it.
//
// It never calls preventDefault or stopPropagation, so yielding is all it does
// — the dialog handlers still run normally on the same event.
function toastEscape(e) {
  if (e.key !== "Escape") return;
  if (loginGateRaised) return;
  if (noticeOpen()) return;
  const cmdk = document.getElementById("cmdk-mount");
  if (cmdk && cmdk.firstChild) return;
  const modalMount = document.getElementById("modal");
  if (modalMount && modalMount.firstChild) return;
  // Every surface that consumes Escape must be listed here, including ones
  // that are NOT inside #modal. The date picker renders into document.body
  // (see toggleDatePicker) and closes itself on Escape without stopping
  // propagation, so closing a calendar popover used to destroy the notice —
  // the same defect as the ⌘K palette, through a third door. A fourth surface
  // will not announce itself; if you add one that handles Escape, add it here.
  if (document.querySelector(ESCAPE_OWNING_POPOVERS)) return;
  const t = document.getElementById("toast-error");
  if (t && !t.hidden) dismissToast();
}

// Popovers that live outside #modal and consume Escape themselves.
const ESCAPE_OWNING_POPOVERS = ".dt-pop";

// INDEX_EMPTY_ART: the path with its middle station not yet there, drawn
// (source, a dashed DBTrail box, the copy). Static, so svgEl is right here.
const INDEX_EMPTY_ART = `<svg viewBox="0 0 160 72" aria-hidden="true"><rect x="2" y="20" width="40" height="32" rx="8" fill="var(--raised)" stroke="var(--line)"/><rect x="60" y="14" width="40" height="44" rx="8" fill="none" stroke="var(--ink-4)" stroke-width="1.5" stroke-dasharray="4 3"/><rect x="118" y="20" width="40" height="32" rx="8" fill="var(--mint-tint)" stroke="var(--ok)"/><path d="M44 36h12M100 36h14" stroke="var(--ink-4)" stroke-width="2" stroke-dasharray="3 3"/><path d="M53 32l4 4-4 4M111 32l4 4-4 4" fill="none" stroke="var(--ink-4)" stroke-width="2"/></svg>`;

function renderError(container, err) {
  if (!container) return;
  clear(container);
  const msg = String((err && err.message) || err);
  // A server whose index database doesn't exist yet (MySQL 1049) is the normal
  // pre-monitoring state, not a fault — and the #1 source of confusion: the
  // index DB lives on the INDEX server and is created when monitoring starts;
  // it is NEVER expected on the source. Render an actionable empty state, not
  // a raw red error wall.
  const m = indexMissingFrom(msg);
  if (m) {
    const box = el("div", { class: "empty" });
    box.append(el("div", { class: "empty-art", "aria-hidden": "true" }, svgEl(INDEX_EMPTY_ART)));
    box.append(el("h3", { text: "This server isn't indexing yet", title: indexMissingDetail(m) }));
    box.append(el("p", { text: indexMissingWords() }));
    box.append(el("button", { class: "btn btn-sm", type: "button", text: "Servers",
      onclick: () => openServersModal() }));
    container.append(box);
    return;
  }
  container.append(el("div", { class: "error-box", text: msg }));
}

// indexMissingFrom is the one reading of MySQL 1049 on a server's index: the
// database DBTrail creates when capture starts is not there yet. Returns
// its name, or "" for any other error. The pages that show it say what to
// set up and where (INDEX_MISSING_WORDS) instead of the driver's sentence.
function indexMissingFrom(msg) {
  const m = String(msg || "").match(/Unknown database '([^']+)'/);
  return m ? m[1] : "";
}
// One sentence, the action: where to go and what to press. The technical
// fact (the database's name, and that it is created on the index server
// when capture starts, never on the source) rides as the tooltip for
// whoever wants it (indexMissingDetail).
function indexMissingWords() {
  return "Go to Servers and press the Start button.";
}
function indexMissingDetail(name) {
  return "Its index database \"" + name + "\" is created on the index server when capture starts; it never lives on the source MySQL.";
}

function renderWarnings(node, warnings) {
  if (!node) return;
  clear(node);
  (warnings || []).forEach((w) => node.append(
    el("div", { class: "warn-item" }, icon("warn"), el("span", { text: w }))
  ));
}

// renderNotes is renderWarnings' quiet sibling (#1365): the response `notes`
// list carries benign informational audit facts (the archive-elision record,
// #1353) whose job is auditability, not attention. Muted ink, no icon, no
// amber — rendering these through the alert component is exactly the bug
// #1365 fixed (a note saying "nothing is missing" read as an incident).
function renderNotes(node, notes) {
  if (!node) return;
  clear(node);
  (notes || []).forEach((n) => node.append(el("div", { class: "note-item", text: n })));
}

// renderEventNotes is how the Events view shows the response `notes` list
// (#1950): as a state, not as a mechanism. The notes this view receives say
// what the list was read from (the live index alone, because nothing else
// exists to read or because the archives could add nothing), so they fold
// into one quiet chip beside the count, "live index only", and the server's
// own sentences sit one click away, word for word. The meaning is the
// server's and is not rewritten here; a note that says something else gets
// a plain "note" chip over the same disclosure. Warnings never come this
// way: they stay in the alert box above the list.
function renderEventNotes(node, notes) {
  if (!node) return;
  clear(node);
  notes = notes || [];
  if (!notes.length) return;
  const live = notes.every((n) => /live[- ]index/i.test(n));
  const d = el("details", { class: "ev-scope" });
  d.append(el("summary", { class: "chip chip-unknown ev-scope-chip",
    text: live ? "live index only" : notes.length === 1 ? "1 note" : notes.length + " notes" }));
  const box = el("div", { class: "ev-scope-detail" });
  notes.forEach((n) => box.append(el("div", { class: "note-item", text: n })));
  d.append(box);
  node.append(d);
}

// ── badge / page-head builders ────────────────────────────────────────────────

function badge(type) { return el("span", { class: "badge " + badgeClass(type), text: type }); }

// docsLink is the page-header Docs link for a route (#1450), or null when
// DOCS_PAGES has no page for it. A plain anchor: no request, no probe.
function docsLink(route, name) {
  const slug = DOCS_PAGES[route];
  if (!slug) return null;
  const a = el("a", { class: "chip page-docs", href: DOCS_BASE + slug + "/", target: "_blank", rel: "noopener",
    title: typeof name === "string" && name ? "Open the " + name + " docs in a new tab" : "Open the docs in a new tab" });
  a.append(el("span", { text: "Docs" }), icon("external"));
  return a;
}

function pageHead(title, subNode, actions) {
  // The route is read from the location on every call, so the link follows
  // every route change and re-render, not only the first paint. It sits
  // BESIDE the h1, not inside it: the title is read as a heading, and
  // "Events Docs" is not the page's name.
  const row = el("div", { class: "page-title-row" },
    el("h1", { class: "page-title", text: title }), docsLink(routeFromLocation(), title));
  // Header actions sit right-aligned on the title row (#1950): one slot, so
  // every view that grows an action puts it in the same place.
  if (actions && actions.length) row.append(el("div", { class: "page-actions" }, ...actions));
  const head = el("div", { class: "page-head" }, row);
  if (subNode) head.append(subNode);
  return head;
}

function viewLoading() {
  const v = VIEW();
  clear(v);
  const loading = el("div", { class: "view-loading", text: "Loading…" });
  v.append(loading);
  v.classList.remove("view-enter");
  return loading;
}
function viewEnter() { const v = VIEW(); v.classList.remove("view-enter"); void v.offsetWidth; v.classList.add("view-enter"); }

// ── router ─────────────────────────────────────────────────────────────────

// isKnownRoute accepts the built-in ROUTES plus any live extension-view route
// ("ext-<id>" advertised in the current server's capabilities). An "ext-" route
// for a view the selected server does not expose is treated as unknown (the
// caller redirects to overview), the same gating Time-travel/Storage get.
function isKnownRoute(route) {
  if (ROUTES.includes(route)) return true;
  // "extset-<id>" is checked first: it does NOT start with "ext-" (the fourth
  // character is "s", not "-"), so the two families never claim each other's
  // routes, and a panel the session may not reach is unknown here exactly like
  // an unadvertised view.
  if (route.startsWith("extset-")) return extSettings.some((p) => "extset-" + p.id === route);
  return route.startsWith("ext-") && extViews.some((v) => "ext-" + v.id === route);
}

// routeSegment is the first path segment of the address: the route name, as
// typed or bookmarked ("" for the root).
function routeSegment() {
  return location.pathname.replace(/^\//, "").split("/")[0];
}

function routeFromLocation() {
  const path = routeSegment() || "overview";
  return isKnownRoute(path) ? path : "overview";
}

// A page that moved leaves its old address in bookmarks, emails, docs and
// Back entries. ROUTE_ALIASES maps each old route to where its page lives
// now, and both ways in go through it: navigate() (clicks, the palette)
// translates before pushing, and renderRoute() (a direct load, Back,
// Forward) rewrites the bar before painting. Before, only navigate()
// translated, so a bookmark of /storage or /sql painted Overview under the
// old address. A Map, so an address like /constructor is not an old page.
// A target may depend on the capability check; "" means it has not answered,
// and the address is left alone rather than rewritten on a guess, which
// would stay on that history entry after the check recovers.
const ROUTE_ALIASES = new Map([
  // Time-travel merged into Restore (#1298).
  ["timetravel", () => "recover"],
  // Storage split into Retention and This daemon (#1543). Retention needs
  // the watch daemon and rewrites the bar to Overview without it, so this is
  // a capability answer too.
  ["storage", () => (capsKnown ? "retention" : "")],
  // This daemon was dissolved (#1867): its telemetry card is on Status, its
  // credential signals and staged downloads on Snapshots. A bookmark lands
  // on Status, which every console has, so no capability answer is needed.
  ["daemon", () => "status"],
  // The SQL page was removed (#1549); its DuckDB schema card lives on
  // Connect AI (#1573), which every console has, so no capability answer is
  // needed to know where to send it.
  ["sql", () => "connect"],
  // The three backup pages merged into Snapshots (#1573). Each lands where
  // its page went, so a bookmark, a link in an email or a Back entry still
  // shows what it named. No capability answer is needed: Snapshots opens on
  // a standalone serve too, where two of these three pages did not exist —
  // the listing and the backup location are readable there. Whether the
  // SECTION an address names is drawn is a separate question, and the
  // arrival note answers it on the page.
  ["baselines", () => "snapshots"],
  ["verification", () => "snapshots#checks"],
  ["backup-settings", () => "snapshots#setup"],
]);

// aliasTarget returns where an old route moved to, or "" for any other. A
// target may name a section of the new page ("snapshots#checks"): three
// pages merged into one, and each one's readers have to land on their part
// of it, not at the top of a page three times longer than what they knew.
function aliasTarget(route) {
  const to = ROUTE_ALIASES.get(route);
  return to ? to() : "";
}

// splitTarget cuts a target into [route, "#section"], with "" for a target
// that names no section.
function splitTarget(target) {
  const i = target.indexOf("#");
  return i < 0 ? [target, ""] : [target.slice(0, i), target.slice(i)];
}

// lastRouteAddress is the address the last dispatch painted, so a repaint
// that goes through renderRoute with the address unchanged (a save, a server
// switch) can be told from a real navigation.
let lastRouteAddress = null;

// routeArrivedFrom names the old address the page on screen was reached
// through ("" when it was not). It lives here, never in the address, which a
// visitor would bookmark again. It survives a repaint of the same visit (a
// server switch, a save, a gate re-dispatching) and ends at the next
// navigation (navigate() and onPopState() clear it) and at sign-out.
let routeArrivedFrom = "";

function navigate(route, params, push = true) {
  // An old route from a stale caller goes straight to its new page, so the
  // entry pushed below already carries the new address. Rewriting with
  // replaceState here would overwrite the entry you were on instead.
  // A target may name a section ("snapshots#setup") whether it is an old
  // alias or the page's own address: the section is cut off before the
  // route is checked, or a direct target with a section would read as an
  // unknown route and land on the Overview (#1853's "Set a schedule" did).
  let hash = "";
  [route, hash] = splitTarget(aliasTarget(route) || route);
  if (!isKnownRoute(route)) [route, hash] = ["overview", ""];
  // Both halves are watch-daemon surfaces (rotation, archiving, staging).
  if (route === "retention" && !capsCache.monitor) route = "overview";
  // Snapshots is NOT gated, where two of the three pages it replaces were
  // (#1573). On a standalone serve it leaves out what only the daemon can
  // DO — taking a backup, running a check — and keeps what serve can
  // answer: the list of copies, where this server keeps them, and a
  // timetable somebody saved, which it shows with the reason nothing here
  // is running it (that card is deliberately not hidden where it cannot
  // run: a saved schedule nothing executes is the silent failure it exists
  // to prevent).
  const qs = params && Object.keys(params).length
    ? "?" + new URLSearchParams(params).toString() : "";
  routeArrivedFrom = "";
  // A click on the page you are already on (Snapshots, from its own #setup
  // section) goes to its top. The page scrolls inside .main, and nothing
  // reset it, so that click left the reader scrolled down past the button at
  // the top (#1681's first-run walk measured it). Only the SAME page: moving
  // between pages keeps today's behavior, which other measurements rely on.
  // An address with a section keeps its own jump (scrollToSection).
  const samePage = routeFromLocation() === route;
  if (push) history.pushState({ route }, "", "/" + route + qs + hash);
  if (push && samePage && !hash) {
    const main = document.querySelector(".main");
    if (main) main.scrollTop = 0;
  }
  renderRoute();
}

// onPopState handles Back and Forward: a new navigation, so the previous
// visit's origin ends here, and renderRoute records a new one if the entry
// is an old address.
function onPopState() {
  routeArrivedFrom = "";
  renderRoute();
}

function renderRoute() {
  // The date-picker popover lives in document.body (position:fixed), outside
  // the #view subtree route changes normally clear — there's no per-view
  // teardown hook in this codebase to hang that cleanup on otherwise.
  closeDatePicker();
  // Route-level staleness: the full-view async renderers (overview / status /
  // storage) fetch before painting, so navigating away while their fetches are
  // in flight would let the OLD view's completion clear and repaint over the
  // new one (nav highlighting one route, content showing another). serverGen
  // only covers server switches; this covers same-server navigation.
  viewGen++;
  const old = routeSegment();
  const target = aliasTarget(old);
  if (target) {
    const [to, sect] = splitTarget(target);
    routeArrivedFrom = old;
    // The section that old page became — unless the address already names
    // one, which a link INTO a part of that page carries and must keep.
    history.replaceState({ route: to }, "", "/" + to + location.search + (location.hash || sect));
  }
  // One arrival, one jump. Armed only when the ADDRESS CHANGED: a saved
  // setting, a saved schedule and a server switch all repaint through here
  // with the same address, and re-arming there would pull the reader back
  // to the section heading every time they pressed Save. The painter
  // clears it, so the repaints a page does on its own never move anyone.
  const addr = location.pathname + location.hash;
  scrollPending = !!location.hash && addr !== lastRouteAddress;
  lastRouteAddress = addr;
  const route = routeFromLocation();
  setActiveNav(route);
  cursorIdx = -1;
  const params = Object.fromEntries(new URLSearchParams(location.search));
  // Extension views (embedding builds): "ext-<id>" dispatches to the provider's
  // frontend module. routeFromLocation already redirected an unknown/ungated
  // ext route to overview, so a match here is always a live view.
  if (route.startsWith("extset-")) {
    const panel = extSettings.find((p) => "extset-" + p.id === route);
    return panel ? renderExtensionSettings(panel) : renderOverview();
  }
  if (route.startsWith("ext-")) {
    const view = extViews.find((v) => "ext-" + v.id === route);
    return view ? renderExtensionView(view) : renderOverview();
  }
  switch (route) {
    case "overview": return renderOverview();
    case "events": return renderEvents(params);
    case "schema-changes": return renderSchemaChanges(params);
    case "timetravel": return renderTimetravel(params);
    case "recover": return renderRecover(params);
    case "status": return renderStatus();
    case "retention": return renderRetention();
    case "snapshots": return renderSnapshots();
    case "connect": return renderConnect();
    case "access-profiles": return renderAccessProfiles();
    default: return renderOverview();
  }
}

function setActiveNav(route) {
  $all(".nav-item").forEach((a) => a.classList.toggle("active", a.dataset.route === route));
}

// ── Timezone discipline (#1354) ──────────────────────────────────────────────
// Every time this console renders is UTC — the zone the wire speaks
// (consoleTSFormat / status.TSFmt) and the zone the Since/Until/At filters and
// `--since`/`--until`/`AS OF` parse. So the DISPLAYED text of a data timestamp
// is the exact wire string (copy-pasteable into those inputs unchanged), and
// the zone is DECLARED next to it instead of suffixed onto every row: a
// section-level chip or "(UTC)" label, plus a hover tooltip carrying the
// viewer's local equivalent. Freshness/metadata stamps ("as of …", "created …")
// are not paste targets, so those carry an inline " UTC" label directly.
// Never render an unlabeled browser-local time.

// utcLocalTitle builds the hover tooltip for a UTC stamp: it names the zone of
// the displayed value and gives the viewer's local equivalent. "" for
// non-stamps ("—", empty), so callers can set title unconditionally.
function utcLocalTitle(stamp) {
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/.exec(String(stamp || ""));
  if (!m) return "";
  const t = new Date(m[1] + "T" + m[2] + "Z");
  if (isNaN(t)) return "";
  return "UTC; in your local time: " + t.toLocaleString();
}

// tsSpan renders one data timestamp: exact wire text, local-time tooltip.
function tsSpan(cls, stamp) {
  return el("span", { class: cls, text: stamp, title: utcLocalTitle(stamp) || null });
}

// utcLabel normalizes a wire timestamp (RFC3339 "…T…Z" or the bare
// "YYYY-MM-DD HH:MM:SS" UTC form) into the labeled display shape
// "YYYY-MM-DD HH:MM:SS UTC", for prose and freshness lines that are read, not
// copy-pasted. Non-stamps pass through unlabeled — never label a value as UTC
// unless it parses as one.
function utcLabel(stamp) {
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z)?$/.exec(String(stamp || ""));
  return m ? m[1] + " " + m[2] + " UTC" : String(stamp || "");
}

// utcBare is utcLabel without the zone word, for a page that states the zone
// once in its header (#1950): the Status view.
function utcBare(stamp) { return utcLabel(stamp).replace(/ UTC$/, ""); }

// tzChip is the section-level zone declaration: a small "UTC" chip for the
// head of a card/panel whose body renders bare timestamps.
function tzChip() {
  return el("span", { class: "tz-chip", text: "UTC",
    title: "All times in this section are UTC, ready to paste into the Since/Until/At filters. Hover a timestamp for your local time." });
}

// nowClock is the freshness clock ("as of …"). UTC and labeled, NOT
// toLocaleTimeString: it sits beside data timestamps that are all UTC, and an
// unlabeled browser-local clock made the same instant appear hours apart on
// one page (#1354).
function nowClock() { return new Date().toISOString().slice(11, 19) + " UTC"; }

// plainDuration spells a number of seconds the way a person says it: the two
// largest units, with a zero second unit left out ("45s", "2m 26s", "1h",
// "1h 14m", "3d 4h"). A bare "4445s" takes arithmetic to read (#1794).
function plainDuration(sec) {
  const s = Math.max(0, Math.floor(Number(sec) || 0));
  const units = [["d", 86400], ["h", 3600], ["m", 60], ["s", 1]];
  for (let i = 0; i < units.length - 1; i++) {
    const [u, n] = units[i];
    if (s < n) continue;
    const [u2, n2] = units[i + 1];
    const small = Math.floor((s % n) / n2);
    return Math.floor(s / n) + u + (small ? " " + small + u2 : "");
  }
  return s + "s";
}

// ── Overview ─────────────────────────────────────────────────────────────────

// covLast is the payload the visible coverage card was built from, so a FAILED
// refresh can re-render the same numbers instead of blanking them — and label
// them stale, which is the part that keeps that honest. One Overview is on
// screen at a time, so one slot is enough.
let covLast = null; // { data, at } — `at` is the UTC clock time of the fetch

// covRefresh builds the card's refresh control: a fetch-time stamp plus the
// button. `stamp` is {at, error} — a refresh that failed keeps the previous
// numbers on screen and says when they are from, rather than replacing real
// values with an "unavailable" card or, worse, leaving them looking current.
function covRefresh(stamp) {
  const wrap = el("div", { class: "cov-refresh" });
  if (stamp && stamp.error) {
    wrap.append(el("span", { class: "cov-asof bad", text: "refresh failed · showing " + stamp.at }));
  } else if (stamp && stamp.at) {
    wrap.append(el("span", { class: "cov-asof", text: "as of " + stamp.at }));
  }
  const btn = el("button", {
    class: "btn btn-icon btn-sm btn-ghost cov-refresh-btn", type: "button",
    title: "Re-read the restore window and capture state",
    "aria-label": "Refresh restore coverage",
    onclick: (e) => refreshCovCard(e.currentTarget),
  });
  btn.append(icon("refresh", "cov-refresh-ico"));
  wrap.append(btn);
  return wrap;
}

// refreshCovCard re-reads /api/coverage and rebuilds ONLY this card. The values
// on screen stay put until the response lands, so there is no flash of empty
// chips, and the rebuild goes back through covCard so the freshness-driven
// tones (#1227) and the never-green rules apply to refreshed values exactly as
// they do on first paint.
async function refreshCovCard(btn) {
  const card = btn.closest(".cov-card");
  if (!card || btn.disabled) return;
  const gen = serverGen, vgen = viewGen;
  btn.disabled = true;
  btn.classList.add("spin");
  let next = null, failed = false;
  try {
    next = await api("/api/coverage");
  } catch (err) {
    console.error("coverage refresh failed", err);
    failed = true;
  }
  // Switched server or route mid-flight: whatever came back describes a view
  // the operator is no longer looking at.
  if (gen !== serverGen || vgen !== viewGen || !card.isConnected) return;
  if (failed) {
    const prev = covLast || { data: { continuity: "unavailable" }, at: "" };
    card.replaceWith(covCard(prev.data, { at: prev.at, error: true }));
    return;
  }
  covLast = { data: next, at: nowClock() };
  const stamp = { at: covLast.at };
  let shown = covCard(next, stamp);
  card.replaceWith(shown);
  // The drawing reads the same coverage as the card, now and when the
  // source answers (#1794).
  ovFlowRepaint(next);
  ovCaptureAsk(next, () => {
    ovFlowRepaint(next);
    if (!covLast || covLast.data !== next || !shown.isConnected) return;
    const again = covCard(next, stamp);
    shown.replaceWith(again);
    shown = again;
  });
}

// covCaptureState reads what the source answered about an idle capture
// (#1794): "up_to_date", "behind", or "" for everything else. The card and
// the flow drawing both read it here, so they cannot say two things. Only
// the two exact words count: no answer, a failed read, a capture that cannot
// be compared and a state this page does not know are all "".
function covCaptureState(c) {
  if (!c || c.freshness !== "idle" || !c.capture) return "";
  const s = c.capture.state;
  return s === "up_to_date" || s === "behind" ? s : "";
}

// OV_CAPTURE_MS bounds the ask below. The daemon bounds its read of the
// source at three seconds.
const OV_CAPTURE_MS = 10000;

// ovCaptureAsk asks the daemon whether capture is caught up, for one
// coverage read c, and calls draw when an answer for it arrives. Asked after
// the card is drawn, never before: the page does not wait on the source.
// Only while capture is idle, and only where the daemon is connected to
// sources. One request per coverage read however many ask, and one more
// when the daemon says when. An answer about another server is dropped.
function ovCaptureAsk(c, draw) {
  if (!c || c.freshness !== "idle" || !capsCache.monitor) return;
  if (c.captureDraws) {
    c.captureDraws.push(draw);
    if (c.capture) draw();
    return;
  }
  c.captureDraws = [draw];
  const gen = serverGen, id = currentServer || defaultServerId;
  const ask = (again) => {
    if (gen !== serverGen) return;
    apiWithin("/api/capture-status", OV_CAPTURE_MS).then((a) => {
      if (gen !== serverGen || !a || a.server_id !== id) return;
      c.capture = a;
      c.captureDraws.forEach((d) => d());
      const wait = Number(a.retry_in_seconds);
      if (again && a.state === "unknown" && wait > 0) setTimeout(() => ask(false), wait * 1000);
    }, (err) => console.error("capture status unavailable", err));
  };
  ask(true);
}

// covCard renders the live RPO statement (#1194). The window's upper edge is
// the last INDEXED event on purpose — "restorable up to now" with a dead
// stream would be false assurance; the lag chip is what says "now". Degrades
// loudly: gap_lost/unavailable red, unknown amber, empty index explicit.
function covCard(c, stamp) {
  const card = el("section", { class: "ov-panel cov-card" });
  card.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Restore coverage" }),
    tzChip(),
    covRefresh(stamp)));
  const cont = c.continuity || "unknown";
  const bad = cont === "gap_lost" || cont === "unavailable";
  const warn = cont === "unknown";
  if (bad && !c.delta_to) {
    // Unreachable/broken backend: say nothing about the window — "no events
    // yet" would be a positive factual claim about an index we couldn't read.
  } else if (!c.delta_to) {
    // Neutral while capture runs and the server is quiet (#1794): that is how
    // every new server starts, and the Getting started list beside it says a
    // quiet database is normal.
    card.append(el("p", { class: "cov-line" + (c.freshness === "idle" ? "" : " warn"), text: "No indexed events yet, so there is nothing to restore from." }));
  } else if (!c.delta_from) {
    // Unknown floor: don't assert a bounded window whose start we don't know.
    card.append(el("p", { class: "cov-line" },
      "Restorable up to ", el("b", { text: c.delta_to, title: utcLocalTitle(c.delta_to) || null }), "; the window start could not be determined."));
  } else {
    card.append(covTimeline(c));
  }
  const chips = el("div", { class: "cov-chips" });
  // Freshness (#1227) is what makes the number readable, so it decides the
  // chip's colour instead of a bare threshold. The same hour means a DEAD
  // DAEMON under "stalled" and a server nobody wrote to under "idle", and
  // those need opposite responses — the old unconditional amber said neither.
  // The number is now minus the newest captured change in every state, so the
  // chip says that, in hours and minutes (#1794): "capture lag 4445s" on a
  // quiet server read as lag and took arithmetic to read. Idle is neutral:
  // nobody writing to a server is not a fault.
  const fresh = c.freshness || "unknown";
  if (typeof c.lag_seconds === "number") {
    const lagTone = fresh === "stalled" ? " bad" : fresh === "current" ? " ok" : fresh === "idle" ? "" : " warn";
    chips.append(el("span", { class: "cov-chip" + lagTone, text: "last change " + plainDuration(c.lag_seconds) + " ago" }));
  }
  // "none" (file-mode: no capture ran) and "idle" stay NEUTRAL, and
  // "unknown"/"unavailable" amber or red: never green, which would paint a
  // non-claim as assurance.
  const freshTone = fresh === "stalled" || fresh === "unavailable" ? " bad"
    : fresh === "current" ? " ok"
    : fresh === "none" || fresh === "idle" ? "" : " warn";
  // The source was asked and is ahead (#1794): amber, and said on the chip.
  const asked = covCaptureState(c);
  chips.append(asked === "behind"
    ? el("span", { class: "cov-chip warn", text: "capture behind" })
    : el("span", { class: "cov-chip" + freshTone, text: "capture " + fresh }));
  // "none" (file-mode: no capture ran) stays NEUTRAL — green would paint a
  // non-claim as assurance.
  chips.append(el("span", { class: "cov-chip" + (bad ? " bad" : warn ? " warn" : cont === "ok" ? " ok" : ""), text: "continuity " + cont }));
  card.append(chips);
  if (cont === "gap_lost") {
    card.append(el("p", { class: "cov-line bad", text: "Events were lost for good: the window has a hole, and points past it need a fresh snapshot." }));
  } else if (cont === "unavailable") {
    card.append(el("p", { class: "cov-line bad", text: "Continuity could not be read. Treat the window as unverified." }));
  } else if (warn) {
    card.append(el("p", { class: "cov-line warn", text: "This index cannot report continuity, so the window may have undetected holes." }));
  }
  // The freshness explanation lines. "stalled" is the only one that is an
  // error state: the checkpoint ticker runs even with no traffic, so a stale
  // checkpoint is the daemon, never the workload.
  if (fresh === "stalled") {
    const age = typeof c.checkpoint_age_seconds === "number" ? " for " + plainDuration(c.checkpoint_age_seconds) : "";
    card.append(el("p", { class: "cov-line bad", text:
      "Capture is STALLED: DBTrail has not checkpointed" + age + ". The window's upper edge is frozen: changes since then are NOT recoverable. Check that the stream is running." }));
  } else if (asked === "up_to_date") {
    // Said only when the source was asked and holds nothing capture has not
    // recorded. "captured" because the source may have written to schemas
    // or tables capture does not read. The time is the newest captured change, in UTC like the
    // rest of the card; past a day it carries its date.
    const since = String(c.delta_to || "");
    const at = c.lag_seconds >= 86400 ? since : since.slice(11, 19);
    card.append(typeof c.lag_seconds === "number" && at
      ? el("p", { class: "cov-line" }, "Up to date. No captured changes since ", el("b", { text: at, title: utcLocalTitle(since) || null }), " (" + plainDuration(c.lag_seconds) + " ago).")
      : el("p", { class: "cov-line", text: "Up to date. No captured changes yet." }));
  } else if (asked === "behind") {
    card.append(el("p", { class: "cov-line warn", text: "Behind: the source has changes capture has not read yet." }));
  } else if (fresh === "idle") {
    // Neutral, and no metric named (#1794): the page gives no way to read
    // one, and from the index alone a quiet server and a capture that fell
    // behind look the same, so the line says exactly that much.
    const quiet = typeof c.lag_seconds === "number" ? "Nothing captured for " + plainDuration(c.lag_seconds) + "." : "Nothing captured yet.";
    card.append(el("p", { class: "cov-line", text:
      quiet + " Either nothing changed on this server, or capture fell behind, and DBTrail cannot tell which." }));
  } else if (fresh === "unavailable") {
    card.append(el("p", { class: "cov-line bad", text: "Capture liveness could not be read. Treat the window's upper edge as unverified." }));
  }
  return card;
}

// covTimeline draws the restore window (#1950): the earliest point, the
// changes kept since, the latest one. It replaced the sentence "Any point
// between A and B is restorable" and carries the same claim as its text
// alternative (aria-label), so a screen reader hears what the drawing says.
// The two stamps are data, in mono; the three words under them are the only
// prose. Its upper edge is the last INDEXED change, never "now": with a dead
// stream that would be false assurance, and the chips beside it say how far
// behind "latest" is.
function covTimeline(c) {
  const fig = el("div", { class: "cov-tl", role: "img",
    "aria-label": "Any point between " + c.delta_from + " and " + c.delta_to + " (UTC) is restorable." });
  fig.append(el("span", { class: "cov-tl-end" },
    el("b", { class: "cov-tl-stamp", text: c.delta_from, title: utcLocalTitle(c.delta_from) || null }),
    el("span", { class: "cov-tl-k", text: "earliest" })));
  fig.append(el("span", { class: "cov-tl-bar", "aria-hidden": "true" }, el("span", { class: "cov-tl-k", text: "changes kept" })));
  fig.append(el("span", { class: "cov-tl-end" },
    el("b", { class: "cov-tl-stamp", text: c.delta_to, title: utcLocalTitle(c.delta_to) || null }),
    el("span", { class: "cov-tl-k", text: "latest" })));
  return fig;
}

// ── Overview: progressive render (#1352) ─────────────────────────────────────
// The page frame and per-card skeletons paint SYNCHRONOUSLY; each card fills as
// ITS fetch lands. The pre-#1352 Promise.all gated first paint on the slowest
// of four endpoints, so an archive-heavy source sat on a bare "Loading…" for
// tens of seconds while /api/events had answered in under one. buildOverview()
// composes the same per-card fills from already-fetched payloads — the seam the
// e2e fixture drives — so the progressive path and the fixture path render
// through identical code.

// ovSkelLines: shimmer placeholder block for a still-loading card body.
function ovSkelLines(n) {
  const box = el("div", { class: "skel-box" });
  for (let i = 0; i < n; i++) box.append(el("div", { class: "skel-line" }));
  return box;
}

// ovPendingCard: a card's loading state — its REAL title plus shimmer lines and
// a label naming what is still being computed, so a slow aggregate reads as
// work in progress instead of a hang (and the page layout shows immediately).
function ovPendingCard(title, waiting, cls) {
  const card = el("section", { class: "ov-panel" + (cls ? " " + cls : "") });
  card.append(el("div", { class: "ov-panel-head" }, el("h2", { class: "ov-panel-title", text: title })));
  card.append(ovSkelLines(3));
  card.append(el("div", { class: "skel-note", text: waiting }));
  return card;
}

// ovStatPending: one tile in its loading state — key and scope already legible,
// value shimmering.
function ovStatPending(key, scope) {
  return el("div", { class: "ov-stat" },
    el("div", { class: "ov-stat-v" }, el("div", { class: "skel-line skel-stat" })),
    el("div", { class: "ov-stat-k", text: key }),
    el("div", { class: "ov-stat-scope", text: scope || "" }));
}

// ovFrame builds the whole page skeleton synchronously and returns handles to
// every slot a fetch will fill. Nothing here waits on the network. Every fill
// can run again over the same frame, which is how the page keeps itself
// current (#1801) without repainting under the reader.
function ovFrame() {
  const v = VIEW(); clear(v);
  const f = {};
  f.head = ovHead = pageHead("Overview", null);
  v.append(f.head);

  // Where the page says it stopped keeping itself current.
  f.liveSlot = el("div");
  v.append(f.liveSlot);
  f.firstRunSlot = el("div");
  v.append(f.firstRunSlot);
  // The path from the source to the copy (the page's first answer): filled
  // by fillOvFlow once the four reads behind it land, and again on the slow
  // refresh cycle. Not painted on a console that lists no server yet: the
  // add-server card above is that page's whole first screen.
  f.flowSlot = el("div");
  f.flowSlot.append(el("section", { class: "flow flow-pending" }, ovSkelLines(2), el("div", { class: "skel-note", text: "reading the path from your database to its copy…" })));
  v.append(f.flowSlot);
  // Use your copy (#1950): the four ways to read the copy, one card each,
  // under the flow. The shape is static; it shows once the flow's reads say
  // a copy exists, and its figures (the table count, the newest copy behind
  // the download) move with those reads and again on the slow refresh,
  // without repainting the cards, so a panel a reader opened stays open.
  // Cleared where the flow is: a console that lists no server has nothing
  // to use.
  f.use = useCopySection();
  f.useSlot = el("div");
  f.useSlot.append(f.use.section);
  v.append(f.useSlot);
  // One grey line under the drawing (#1860): does the copy carry life? Two
  // numbers over the index's own window, filled by fillOvActivity; the
  // per-table figures live in the fold below.
  f.actLine = el("div", { class: "ov-actline" });
  v.append(f.actLine);
  // The restore window, the table-coverage check and the four tiles are
  // the page's recovery-era half. They keep their slots and fills (the
  // live-refresh loop and every fill are untouched), but hang under one
  // closed fold at the foot of the page: the flow above answers the
  // questions a replica's operator brings, and these answer "can I undo",
  // which is the History pages' question.
  f.covSlot = el("div");
  f.covSlot.append(ovPendingCard("Restore coverage", "computing restore coverage…", "cov-card"));
  f.uncapSlot = el("div");
  f.uncapSlot.append(ovPendingCard("Table coverage", "checking which tables are captured…", "uncap-card"));

  const stats = el("div", { class: "ov-stats" });
  f.statTotal = ovStatPending("changes indexed", "all time · estimate");
  f.statDeletes = ovStatPending("deletes", "");
  f.statTables = ovStatPending("tables touched", "");
  f.statLatest = ovStatPending("most recent change", "point in time (UTC)");
  stats.append(f.statTotal, f.statDeletes, f.statTables, f.statLatest);

  // Where the tiles say their figures could not be refreshed, above the
  // notes the aggregate itself carries (which fillOvActivity clears).
  f.sideSlot = el("div");
  // Whatever the aggregate could not account for lands here, at the point of
  // use — between the tiles and the panels, where the old layout put it.
  f.warnSlot = el("div");

  // The two panels carry the home's tint layer (#1421): violet and sun, the
  // structure tints. The pill is the eyebrow — the title stays an h2 for the
  // document outline; the pill is presentation, not the heading.
  //
  // Both hang under a closed fold of one line each (#1860): the drawing
  // above is the page's answer, and two tinted panels with an Undo per row
  // under it read as an undo list with a diagram on top. The fold's line
  // carries the count, so what is inside is never in doubt; Undo stays
  // where it was, one click down (D6). Fold state lives on the node.
  f.recentPanel = el("section", { class: "ov-panel tcard-violet" });
  f.recentPanel.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title" }, el("span", { class: "tag-pill", text: "Recent changes" })),
    tzChip(),
    el("a", { class: "btn btn-sm btn-ghost", href: "/events",
      onclick: (e) => { e.preventDefault(); navigate("events"); }, text: "Browse all events" })));
  f.recentBody = el("div", { class: "ov-evlist" });
  f.recentBody.append(ovSkelLines(4), el("div", { class: "skel-note", text: "loading recent changes…" }));
  f.recentPanel.append(f.recentBody);
  f.recentFold = el("details", { class: "ov-fold ov-fold-panel ov-fold-recent" });
  f.recentSummary = el("summary", { text: "Recent changes" });
  f.recentFold.append(f.recentSummary, f.recentPanel);
  ovFoldRemember(f.recentFold, "recent");
  v.append(f.recentFold);

  f.tablesPanel = el("section", { class: "ov-panel tcard-sun" });
  // The "as of" stamp has a slot of its own, so a refill replaces it instead
  // of adding a second one beside it.
  f.tablesAsOf = el("span");
  f.tablesHead = el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title" }, el("span", { class: "tag-pill", text: "Activity by table" })), f.tablesAsOf);
  f.tablesPanel.append(f.tablesHead);
  f.tablesBody = el("div", { class: "ov-tables" });
  f.tablesBody.append(ovSkelLines(4), el("div", { class: "skel-note", text: "computing window activity…" }));
  f.tablesPanel.append(f.tablesBody);
  f.tablesFoot = el("div", { class: "ov-coverage" });
  f.tablesPanel.append(f.tablesFoot);
  f.tablesFold = el("details", { class: "ov-fold ov-fold-panel ov-fold-tables" });
  f.tablesSummary = el("summary", { text: "Activity by table" });
  f.tablesFold.append(f.tablesSummary, f.tablesPanel);
  ovFoldRemember(f.tablesFold, "tables");
  v.append(f.tablesFold);

  const fold = el("details", { class: "ov-fold ov-fold-figures" });
  fold.append(el("summary", { text: "Restore window and figures" }));
  fold.append(f.covSlot, f.uncapSlot, stats, f.sideSlot, f.warnSlot);
  v.append(fold);
  viewEnter();
  return f;
}

// fillOvCoverage — the live RPO statement (#1194). Best-effort: a null payload
// removes the pending card and renders nothing, never a fabricated window (the
// fetch path substitutes {continuity:"unavailable"} on failure, which renders
// the red card).
function fillOvCoverage(f, coverage) {
  clear(f.covSlot);
  if (!coverage) return;
  covLast = { data: coverage, at: nowClock() };
  // Also on the frame: covLast is module-wide (the card's own refresh button
  // reads it), so a refresh that fails must re-render THIS Overview's last
  // payload, not whichever one filled that global last.
  f.covLast = covLast;
  const stamp = { at: covLast.at };
  f.covSlot.append(covCard(coverage, stamp));
  // Asked once the card is on screen (#1794). The answer redraws the card
  // this fill drew, if it is still the one shown, and the drawing.
  ovCaptureAsk(coverage, () => {
    ovFlowRepaint(coverage);
    if (f.covLast && f.covLast.data === coverage) { clear(f.covSlot); f.covSlot.append(covCard(coverage, stamp)); }
  });
}

// fillOvStatus fills the all-time tile. "changes indexed" is
// status.total_events_estimate — information_schema TABLE_ROWS, an InnoDB
// ESTIMATE. Say so on the tile: presenting a sampled number in the same type
// as three exact ones is its own quiet lie.
function fillOvStatus(f, status) {
  if (status) updateSideMeta(status);
  const cov = (status && status.coverage) || {};
  const total = status ? (status.total_events_estimate || cov.total_events || "—") : "—";
  f.statTotal.replaceWith(f.statTotal = ovStat(String(total), "changes indexed", "", "all time · estimate"));
}

// fillOvEvents fills the Recent-changes panel and the most-recent-change tile.
// A failed fetch renders a red error box in the panel — never a swallowed
// blank list. A refill with the same changes leaves the panel alone, and one
// with a newer change keeps the focus on the Undo it was on (#1801): the list
// refreshes on its own now, and a keyboard user tabbing to Undo must not be
// sent back to the top of the page by a change landing above.
function fillOvEvents(f, eventsData, err) {
  const events = ((eventsData && eventsData.events) || []).slice(0, 8);
  const key = err ? null : events.map((e) => e.anchor).join("\n");
  if (key !== null && key === f.eventsKey) return;
  f.eventsKey = key;
  const latest = (events[0] && events[0].event_timestamp) || "—";
  const wide = el("div", { class: "ov-stat" },
    el("div", { class: "ov-stat-v small", text: latest, title: utcLocalTitle(latest) || null }),
    el("div", { class: "ov-stat-k", text: "most recent change" }),
    el("div", { class: "ov-stat-scope", text: "point in time (UTC)" }));
  f.statLatest.replaceWith(f.statLatest = wide);
  let focused = null;
  for (const [anchor, btn] of f.undoByAnchor || []) if (btn === document.activeElement) focused = anchor;
  f.undoByAnchor = new Map();
  clear(f.recentBody);
  if (err) {
    f.recentBody.append(el("div", { class: "error-box", text: "Recent changes unavailable: " + (err.message || err) }));
    return;
  }
  if (!events.length) {
    f.recentBody.append(el("div", { class: "ev-empty", text: "No changes yet." }));
  }
  events.forEach((e) => {
    const row = ovEventRow(e);
    f.undoByAnchor.set(e.anchor, row.undoButton);
    f.recentBody.append(row);
  });
  const again = focused && f.undoByAnchor.get(focused);
  if (again) again.focus();
  // The fold's one line: what is inside, and what it is for. "8 newest" is
  // what the list holds (the response has no total); "every version kept"
  // is the other half of what the copy is.
  // A zero is said in words, never as "0 newest".
  if (f.recentSummary) f.recentSummary.textContent = ovFoldLine("Recent changes", err ? "" : events.length ? events.length + " newest" : "no changes yet", "every version kept, undo a row");
}

// ovFoldLine joins a fold's title with its count and its purpose: the parts
// that are known, separated by a middle dot.
function ovFoldLine(title, count, purpose) {
  return [title, count, purpose].filter(Boolean).join(" · ");
}

// ovFoldRemember keeps a fold the way this browser left it (#1860): closed
// the first time (open, for a fold that says so), then as it was. A
// per-browser convenience, so browser storage; it may be absent or refused
// (a private window), and the fold then simply starts as it does the first
// time.
const OV_FOLD_KEY = "dbtrail.overview.fold.";
function ovFoldRemember(details, name, openByDefault) {
  if (openByDefault) details.open = true;
  try {
    const was = localStorage.getItem(OV_FOLD_KEY + name);
    if (was === "open") details.open = true;
    else if (was === "closed") details.open = false;
  } catch (e) { /* no storage: starts as it does the first time */ }
  details.addEventListener("toggle", () => {
    try { localStorage.setItem(OV_FOLD_KEY + name, details.open ? "open" : "closed"); } catch (e) { /* not remembered */ }
  });
}

// ovSinceLabel says where a window starts, at a glance: the time alone when
// it opened today, "yesterday" when it opened the day before, the date
// otherwise. Both stamps are the index's own, in UTC.
function ovSinceLabel(since, until) {
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})/.exec(String(since || ""));
  if (!m) return "";
  const day = (stamp) => String(stamp || "").slice(0, 10);
  const untilDay = day(until);
  if (m[1] === untilDay) return "Since " + m[2];
  const prev = new Date(Date.parse(untilDay + "T00:00:00Z") - 86400000);
  if (untilDay && m[1] === prev.toISOString().slice(0, 10)) return "Since " + m[2] + " yesterday";
  return "Since " + m[1] + " " + m[2];
}

// ovActivityLine is the one grey line's text: two numbers over the index's
// window, or the zero said in words with the hour it holds since.
function ovActivityLine(activity) {
  if (!activity) return "";
  const since = ovSinceLabel(activity.since, activity.until);
  const total = Number(activity.total) || 0;
  const tables = Number(activity.tables) || 0;
  if (total === 0) return "No changes" + (since ? " " + since.charAt(0).toLowerCase() + since.slice(1) : "");
  return (since ? since + " · " : "") + total.toLocaleString("en-US") + (total === 1 ? " change" : " changes") + " in " + tables + (tables === 1 ? " table" : " tables");
}

// ovAgeText is the copy's age from its own stamp, so the large number keeps
// moving between two reads of the listing (a still number reads as a frozen
// page). The stamp is the index's, in UTC, with or without a zone suffix.
function ovAgeText(stamp, nowMs) {
  const t = Date.parse(String(stamp || "").replace(" ", "T").replace(/Z?$/, "Z"));
  if (!isFinite(t)) return "";
  return plainDuration((nowMs - t) / 1000) + " ago";
}

// ovTickAges re-reads every large age on the drawing off its stamp.
function ovTickAges(root) {
  if (!root || typeof root.querySelectorAll !== "function") return;
  const now = Date.now();
  for (const n of root.querySelectorAll(".flow-big[data-stamp]")) {
    const text = ovAgeText(n.getAttribute("data-stamp"), now);
    if (text && n.textContent !== text) n.textContent = text;
  }
}

// fillOvActivity fills every window-scoped surface from the /api/activity
// materialization (#1352): the deletes/tables tiles, the warning items, the
// Activity-by-table panel, and the window footer. The aggregate is precomputed
// server-side, so its refreshed_at is rendered wherever its numbers appear
// ("as of …") — a stale count must be visibly stale, never silently so. The
// window is the LIVE RETENTION, derived server-side from the oldest live
// partition; the label travels in the payload so the tile and the measurement
// can never disagree.
function fillOvActivity(f, activity) {
  // Identical bytes leave the panel alone (#1801). The aggregate behind it is
  // a server-side cache with a 30-minute life, so a busy server sends the
  // same payload turn after turn; rebuilding it every five seconds would
  // destroy and recreate the same rows ~720 times an hour. Each row is a
  // link, so one swapped between a mouse going down and coming up loses the
  // click, and hovering it would flicker. fillOvEvents does the same.
  const key = JSON.stringify(activity || null);
  if (key === f.activityKey) return;
  f.activityKey = key;
  const deletes = activity ? activity.deletes : null;
  const tableCount = activity ? activity.tables : null;
  const refreshed = (activity && activity.refreshed_at) || "";
  // Tiles carry the compact time-of-day stamp; the footer carries the full
  // one. Both are labeled: the freshness clock beside them (nowClock) says
  // "UTC", and an unlabeled sibling would read as a different zone (#1354).
  const asofShort = refreshed ? " · as of " + (refreshed.length > 11 ? refreshed.slice(11) : refreshed) + " UTC" : "";
  // The window scope printed on every window-scoped tile. "partial" is not
  // decoration: the server sets complete=false when the counts are knowably a
  // floor, and a narrower number under a wider label is the bug this page is
  // fixing.
  const winScope = activity ? (activity.label + (activity.complete ? "" : " · partial") + asofShort) : "unavailable";
  f.statDeletes.replaceWith(f.statDeletes =
    ovStat(deletes === null ? "—" : String(deletes), "deletes", deletes ? "danger" : "", winScope));
  f.statTables.replaceWith(f.statTables =
    ovStat(tableCount === null ? "—" : String(tableCount), "tables touched", "", winScope));

  clear(f.warnSlot);
  if (!activity) {
    f.warnSlot.append(el("div", { class: "warn-item" }, icon("warn"),
      el("span", { text: "The window counts could not be loaded, so the deletes and tables tiles show no number rather than a zero." })));
  }
  (activity && activity.notes || []).forEach((n) => {
    f.warnSlot.append(el("div", { class: "warn-item" }, icon("warn"), el("span", { text: n })));
  });

  clear(f.tablesAsOf);
  if (refreshed) {
    f.tablesAsOf.append(el("span", { class: "cov-asof", text: "as of " + utcLabel(refreshed) }));
  }
  if (f.tablesSummary) f.tablesSummary.textContent = ovFoldLine("Activity by table", tableCount === null ? "" : tableCount === 0 ? "no changes in this window" : tableCount + (tableCount === 1 ? " table" : " tables"), "");
  if (f.actLine) {
    clear(f.actLine);
    const line = ovActivityLine(activity);
    if (line) f.actLine.append(el("a", { class: "ov-actline-link", href: "/events", text: line + " ›", onclick: (e) => { e.preventDefault(); navigate("events"); } }));
  }
  clear(f.tablesBody);
  const tables = (activity && activity.top_tables || []).map((t) => ({
    key: t.schema + "." + t.table, insert: t.insert, update: t.update, delete: t.delete, total: t.total,
  }));
  if (!tables.length) {
    f.tablesBody.append(el("div", { class: "ev-empty", text: activity ? "No changes in this window." : "Window activity unavailable." }));
  }
  tables.forEach((s) => f.tablesBody.append(ovTableRow(s, activity && activity.since)));
  // The footer states the AGGREGATE's own bounds. It must never fall back to
  // status.coverage.oldest (#679/#684/#686): that is the index's whole history,
  // and printing it under "window" would attribute counts from one span to a
  // far wider one — the same class of mismatch as the tiles' (#1300).
  clear(f.tablesFoot);
  f.tablesFoot.append(
    el("span", { text: "window (UTC)" }), " ",
    el("b", { text: activity ? activity.since : "—", title: activity ? utcLocalTitle(activity.since) || null : null }),
    " → ",
    el("b", { text: activity ? activity.until : "—", title: activity ? utcLocalTitle(activity.until) || null : null }),
    el("span", { text: activity ? " · " + winScope : "" }));
}


// ── The copy flow ────────────────────────────────────────────────────────────
//
// The Overview's first screen is the path itself: your MySQL, the binlog it
// ships, DBTrail, the timetable that writes the Parquet copy, the bucket the
// copy lives in, and whatever reads it. Every arrow and box carries its live
// state, so the three questions an operator brings (is the binlog being
// read, do the table definitions still hold, how far behind is what I query)
// are read off the drawing instead of off three sentences.
//
// One rule keeps it honest: the state goes ON THE ARROW THAT BROKE, and every
// piece downstream of it goes grey with "as of HH:MM", never red. What is
// downstream is still valid, only old — and a green "5 min ago" on the copy
// arrow while capture is stopped would be a false green, because the update
// folded a frozen index.
//
// ovFlowModel is pure: raw payloads in, the seven pieces and the decision
// cards out. It never fetches and never touches the DOM, so every state has
// a fixture test in assets_overview_flow_test.go. Tones: ok (green), warn
// (amber), bad (red), off (grey: downstream of a break), none (neutral: not
// known, and NOT an alarm). Nothing here is ever ok without the payload that
// earns it.

// flowHHMM: "HH:MM" of a wire stamp (RFC3339 or "YYYY-MM-DD HH:MM:SS"), for
// the labels that are read at a glance; the full stamp goes in the tooltip.
function flowHHMM(stamp) {
  const m = /^\d{4}-\d{2}-\d{2}[ T](\d{2}:\d{2})/.exec(String(stamp || ""));
  return m ? m[1] : "";
}

// flowEveryMinutes parses a schedule's "every" ("5m", "6h", "1d") into
// minutes, or 0 when it cannot: 0 never colours the copy arrow.
function flowEveryMinutes(every) {
  const m = /^(\d+)\s*([mhd])$/.exec(String(every || "").trim());
  if (!m) return 0;
  const n = Number(m[1]);
  return m[2] === "m" ? n : m[2] === "h" ? n * 60 : n * 1440;
}

// flowEveryLabel says the interval the way a person does: "every 5 min",
// "every 6 h", "every 24 h" (a day is said in hours: "every 1 d" reads as a
// typo).
function flowEveryLabel(every) {
  const min = flowEveryMinutes(every);
  if (!min) return "";
  if (min % 60) return "every " + min + " min";
  return "every " + (min / 60) + " h";
}

function ovFlowModel(inp) {
  const cov = inp.coverage || {};
  const bl = inp.baselines || {};
  const srv = inp.server || null;
  // The server list could not be read: the supervisor's word is unknown, so
  // the capture arrow cannot be green off the index's verdict alone — the
  // index can still say "current" for a stream that crashed seconds ago.
  const serverUnknown = !!inp.serverUnknown;
  const mon = inp.monitor || {};
  const schema = inp.schema || {};
  const unc = inp.uncaptured || {};
  const may = inp.may || (() => true);
  const monitorCap = !!inp.monitorCap;
  const registry = !!(srv && srv.kind === "registry" && srv.has_source);
  const sid = srv ? srv.id : "";
  const piece = (title, tone, line, sub, extra) => Object.assign({ title, tone, line: line || "", sub: sub || "" }, extra || {});
  const cards = [];

  // Capture: the supervisor's word first (it knows a crashed stream before
  // the index shows it), then the index's own freshness verdict.
  const lastIndexed = flowHHMM(cov.delta_to);
  let capture, cut = null;
  const mstate = registry ? (srv.monitor_state || "") : "";
  if (serverUnknown || cov.continuity === "unavailable") {
    capture = piece("binlog", "warn", "state could not be read", serverUnknown ? "the server list did not answer" : "");
  } else if (mstate === "failed") {
    capture = piece("binlog", "bad", "stopped", mon.since ? "since " + flowHHMM(mon.since) : "");
    cut = { at: lastIndexed, piece: "capture" };
    // #1708: decided by the code the daemon sends, never by the error text.
    // No Start for this cause: starting again does not end the cleanup it is
    // waiting on. "On its own" is said only when the daemon says it retries.
    const earlierCleanup = mon.error_code === "earlier_cleanup_running";
    // Another reader with this stream's replication id. The stream fails,
    // reconnects and fails again with a new position in its text each time,
    // so the key leaves the stamp and the text out: a card closed once stays
    // closed. No Start while the daemon retries: starting here only
    // disconnects the other reader sooner.
    const sameId = mon.error_code === "same_replication_id";
    cards.push({ kind: "capture-failed", key: sameId ? sid + "|failed|same_replication_id" : sid + "|failed|" + (mon.since || "") + "|" + (mon.last_error || ""), tone: "bad",
      title: "Capture stopped" + (lastIndexed ? " " + lastIndexed : ""),
      lines: sameId ? sameReplicationIdLines(!!mon.retrying)
        : earlierCleanup
        ? ["An earlier cleanup is still running on the index. " + (mon.retrying
          ? "DBTrail checks again on its own, and capture starts when it finishes."
          : "Capture stays stopped. Start it from Servers once the cleanup finishes.")].concat(mon.last_error ? [mon.last_error] : [])
        : [mon.last_error || "DBTrail reported no error text."],
      raw: sameId ? (mon.last_error || "") : "",
      actions: earlierCleanup || (sameId && mon.retrying) ? [{ label: "Details", run: "status" }]
        : [{ label: "Start", primary: true, run: "start" }, { label: "Details", run: "status" }] });
  } else if (mstate === "stalled" || mstate === "lost_position") {
    capture = piece("binlog", "bad", mstate === "stalled" ? "stalled" : "position lost", lastIndexed ? "last change " + lastIndexed : "");
    cut = { at: lastIndexed, piece: "capture" };
    cards.push({ kind: "capture-stalled", key: sid + "|" + mstate + "|" + lastIndexed, tone: "bad",
      title: mstate === "stalled" ? "Capture is not making progress" : "Capture lost its position in the binlog",
      lines: [mstate === "stalled"
        ? "The stream is alive but nothing new has reached the index" + (lastIndexed ? " since " + lastIndexed : "") + ". It restarts on its own; there is no button for this."
        : "Events between the saved position and the oldest binlog still on the source are gone for good. Read the database again to make the copy whole."],
      recipe: ["Is the DBTrail process alive, and is its clock right?",
        "The last 100 lines of DBTrail's log: a stuck batch names itself there.",
        "SHOW BINARY LOGS on the source: if the file the saved position points at is gone, capture ends in a permanent gap.",
        "Free disk and write errors on the index database."],
      actions: [{ label: "Details", run: "status" }] });
  } else if (mstate === "stopped") {
    capture = piece("binlog", "none", "stopped", "not capturing");
    cut = { at: lastIndexed, piece: "capture" };
    cards.push({ kind: "capture-stopped", key: sid + "|stopped|" + (mon.since || lastIndexed), tone: "none", title: "Capture is stopped for this server",
      lines: ["Nothing new reaches the copy until it starts again."],
      actions: [{ label: "Start", primary: true, run: "start" }] });
  } else if (mstate === "pending") {
    capture = piece("binlog", "warn", "starting", srv.monitor_phase || "");
  } else {
    const fresh = cov.freshness || "";
    const cont = cov.continuity || "";
    if (cont === "gap_lost") {
      capture = piece("binlog", "bad", "changes lost for good", lastIndexed ? "last change " + lastIndexed : "");
      cut = { at: lastIndexed, piece: "capture" };
    } else if (fresh === "current") {
      capture = piece("binlog", "ok", typeof cov.lag_seconds === "number" ? plainDuration(cov.lag_seconds) + " behind" : "reading", "");
    } else if (fresh === "idle") {
      // Green, and "connected": the daemon is alive and checkpointing; what
      // it cannot tell apart is a quiet server from capture fallen far
      // behind, so the word is never "up to date".
      capture = piece("binlog", "ok", "connected", lastIndexed ? "nothing new since " + lastIndexed : "nothing new yet");
      // Unless the source was asked (#1794): behind is amber, and nothing
      // is cut, since capture is running.
      if (covCaptureState(cov) === "behind") capture = piece("binlog", "warn", "behind", lastIndexed ? "last change " + lastIndexed : "");
    } else if (fresh === "stalled") {
      capture = piece("binlog", "bad", "stopped" + (lastIndexed ? " " + lastIndexed : ""),
        typeof cov.checkpoint_age_seconds === "number" ? "position saved " + plainDuration(cov.checkpoint_age_seconds) + " ago" : "");
      cut = { at: lastIndexed, piece: "capture" };
    } else if (fresh === "unknown" || fresh === "unavailable") {
      capture = piece("binlog", "warn", "state could not be read", "");
    } else if (fresh === "none") {
      capture = piece("binlog", "none", "not capturing from here", "");
      // Three causes wear this label, and two have a fix from this page
      // (#1853): a server with no source database, and the daemon's own
      // index entry, which is not a server. A read-only console (no
      // daemon) is the third, and the DBTrail box already says what to
      // run. The arrow keeps naming the state; the link names the fix.
      if (srv && monitorCap && srv.kind === "registry" && !srv.has_source) {
        if (may("servers:write")) capture.action = { label: "Add the source", run: "add-source" };
        cards.push({ kind: "capture-no-source", key: sid + "|no-source", tone: "none",
          title: "This server has no database to capture from",
          lines: ["Add its connection and DBTrail starts reading its changes."],
          actions: [{ label: "Add the source", primary: true, run: "add-source" }] });
      } else if (srv && monitorCap && srv.kind !== "registry") {
        if (may("servers:write")) capture.action = { label: "Add a server", run: "add-server" };
        cards.push({ kind: "capture-boot-index", key: sid + "|boot-index", tone: "none",
          title: "This is DBTrail's own index, not a monitored server",
          lines: ["Add the database you want to protect."],
          actions: [{ label: "Add a server", primary: true, run: "add-server" }] });
      }
    } else {
      capture = piece("binlog", "none", "no data yet", "");
    }
  }

  // The Source type saved with a server is a hint: capture follows what the
  // server reports, and says so when the two disagree. Never a failure.
  if (srv && srv.monitor_warning) {
    cards.push({ kind: "source-type", key: sid + "|source-type|" + srv.monitor_warning, tone: "warn",
      title: "Source type does not match the server", lines: [srv.monitor_warning], actions: [] });
  }

  // The source box says "up to date", like the card, only when the source
  // was asked and capture holds all it wrote (#1794). Not "quiet": equal
  // sets prove capture read all it can, not that nothing was written to
  // what it does not read. When it could not be asked the box says no word.
  const source = piece("Your MySQL", "none", srv && srv.source_host ? srv.source_host : "", "");
  if (!cut && capture.tone === "ok" && capture.line === "connected" && covCaptureState(cov) === "up_to_date") source.line = "up to date";

  // Table definitions (the DBTrail box): what the schema snapshot last did,
  // the count of captured tables, and a schema change that stopped the copy.
  const tablesWord = (n) => n + (n === 1 ? " table" : " tables");
  // Before the definitions were read once, "0 tables" is not a count, it is
  // a wait: said in words (#1860). A finished read that found none says 0.
  const captured = typeof unc.tables_captured === "number"
    ? (unc.tables_captured === 0 && schema.state !== "succeeded" ? "first read pending" : tablesWord(unc.tables_captured)) : "";
  const sch = bl.schedule || null;
  const run = sch && sch.last_run;
  const fb = sch && sch.last_fallback;
  const foldCode = run && run.why_code;
  const foldRefused = !!(fb && run && (foldCode === "fold_refused" || foldCode === "fold_crashed"));
  const refreshFailed = !!(!sch && bl.refresh && bl.refresh.state === "failed");
  const showReason = may("query:execute");
  let engine;
  if (schema.unavailable && schema.status && schema.status !== 403) {
    engine = piece("DBTrail", "warn", captured, "definitions could not be read");
  } else if (schema.unavailable) {
    engine = piece("DBTrail", "none", captured, monitorCap ? "definitions: not checked from here" : "run bintrail-console watch to check definitions");
  } else if (schema.state === "failed") {
    engine = piece("DBTrail", "bad", "definitions could not be refreshed", showReason ? (schema.last_error || "") : "", { schemaRetry: true });
  } else if (schema.state === "running") {
    engine = piece("DBTrail", "warn", "refreshing table definitions", schema.since ? "since " + flowHHMM(schema.since) : "");
  } else if (schema.state === "succeeded") {
    engine = piece("DBTrail", "ok", captured || "definitions read", schema.finished_at ? "definitions read " + flowHHMM(schema.finished_at) : "");
  } else {
    engine = piece("DBTrail", "none", captured, "");
  }
  if (foldRefused && run.ok === true) {
    // The schedule already answered the change with a full read: say so,
    // no decision to make.
    engine = piece("DBTrail", "warn", "a schema change", "copy read in full" + (run.finished_at ? " " + flowHHMM(run.finished_at) : ""));
  }

  // The copy arrow: how the copy moves, and how old the newest one is. The
  // number is the copy's AGE, never "behind the source": the copy stands on
  // the index as it was when the update started, and the index was itself
  // behind by then, so a single "behind" figure would understate. "behind"
  // is said once on this page, on the binlog arrow.
  const blUnknown = !!bl.unavailable;
  const snap = (bl.snapshots || [])[0] || null;
  const snapAt = snap ? flowHHMM(snap.time) : "";
  const everyMin = sch ? flowEveryMinutes(sch.every) : 0;
  const everyLabel = sch ? flowEveryLabel(sch.every) : (bl.refresh ? "on DBTrail's timetable" : "no schedule set");
  const nextAt = sch && sch.runnable && sch.next_run ? flowHHMM(sch.next_run) : "";
  const ageMin = snap && typeof snap.age_hours === "number" ? snap.age_hours * 60 : -1;
  let update = piece(everyLabel || "no schedule set", "none", "", "");
  // No schedule and nothing else moving the copy: the fix is one page away
  // (#1853). Only for a server the schedule can be set on, where this
  // console can run one, and for a session that may set it.
  if (srv && !sch && !bl.refresh && !blUnknown && monitorCap && may("settings:write")) update.action = { label: "Set a schedule", run: "schedule" };
  if (snap) {
    update.big = ageMin >= 0 ? plainDuration(ageMin * 60) + " ago" : "";
    // "read" touches the database, "refresh" only the recorded changes. The
    // word is the daemon's plan as of this page load (next_method): a refresh
    // that fails at run time can still fall back to a read, as the schedule
    // card says. A run that cannot start gets no time at all, because the
    // daemon fills next_method even then (next_method_error says why).
    const nextWord = sch && sch.next_method === "refresh" ? "next refresh " : sch && sch.next_method ? "next read " : "next ";
    const nextPart = sch && sch.next_method_error ? "next run cannot start" : nextAt ? nextWord + nextAt : "";
    update.sub = (snapAt ? "copy from " + snapAt : "") + (nextPart ? (snapAt ? " · " : "") + nextPart : "");
    update.stamp = snap.time;
    if (everyMin && ageMin >= 0) {
      const ratio = ageMin / everyMin;
      update.tone = ratio < 2 ? "ok" : ratio <= 3 ? "warn" : "bad";
    }
  } else {
    update.line = blUnknown ? "could not be read" : "no copy yet";
    if (blUnknown) { update.tone = "warn"; update.title = "copy"; }
  }
  // An unknown capture state cannot vouch for the copy: keep the age, drop
  // the colour, so a fresh snapshot never reads green while nobody knows
  // whether capture is stopped.
  if (capture.tone === "warn" && capture.line === "state could not be read") update.tone = "none";
  if (sch && sch.runnable === false && sch.reason) {
    update.tone = "warn";
    update.line = "schedule cannot run";
    update.sub = showReason ? sch.reason : "";
  }
  const blocked = (foldRefused && run.ok !== true) || refreshFailed;
  if (blocked && !cut) {
    const stamp = foldRefused ? (run.finished_at || run.started_at || "") : (bl.refresh.finished_at || "");
    const at = flowHHMM(stamp);
    update.tone = "bad";
    update.big = "";
    update.line = "update stopped" + (at ? " " + at : "");
    update.sub = snapAt ? "copy from " + snapAt : "";
    cut = { at: snapAt, piece: "update" };
    const why = foldRefused ? run.why : bl.refresh.last_error;
    const said = showReason && why ? (foldRefused ? backupWhyLine(why, foldCode, false) : backupFoldError(why)) : "";
    cards.push({ kind: "update-blocked", key: sid + "|blocked|" + stamp, tone: "bad",
      title: "A schema change stopped the update from changes",
      lines: [said || "A table changed shape. The copy cannot be updated from the recorded changes until that table is read again from the database."],
      cost: "Reading the database takes longer than a refresh, and writes may wait while it starts.",
      actions: [
        nextAt ? { label: "Wait for the scheduled read at " + nextAt, primary: true, run: "dismiss" } : { label: "Wait", primary: true, run: "dismiss" },
        { label: "Read database now", run: "read", confirm: READ_DB_CONFIRM },
      ] });
  }

  // The bucket, and the reader.
  let bucket;
  if (blUnknown) bucket = piece("Your copy", "warn", "could not be read", "");
  else if (bl.configured === false) bucket = piece("Your copy", "none", "no copy location set", "");
  else if (snap) {
    const kinds = (snap.kinds || []).map((k) => (k === "dir" ? "disk" : k === "s3" ? "S3" : k));
    // What this machine keeps is the one retention figure the API carries
    // per server (local_retention, #1681); the S3 rule lives on the bucket.
    const keep = bl.local_retention && bl.local_retention.keep_newest;
    // "snapshot" is one version of the copy (D10); "copies" read as several
    // copies of the database.
    const keeps = keep > 0 ? "keeps " + keep + (keep === 1 ? " snapshot" : " snapshots") : "";
    bucket = piece("Your copy", "none", tablesWord((snap.tables || []).length), [kinds.join(" + "), keeps].filter(Boolean).join(" · "));
  } else bucket = piece("Your copy", "none", bl.snapshots ? "no copy yet" : "", "");
  // Tables created after the snapshot an update started from (#1993): the
  // newest copy does not hold them, and the count alone above read as the
  // whole database. The live refresh status first (it keeps the list while
  // later updates fail), then the schedule's last run when it was an update;
  // newTablesNote checks the list against the newest snapshot's own tables.
  const gapRun = bl.refresh && (bl.refresh.new_tables || bl.refresh.new_tables_omitted || bl.refresh.new_tables_unchecked) ? bl.refresh
    : (run && run.method === "refresh" ? run : null);
  const gap = snap && !blUnknown ? newTablesNote(gapRun, snap) : null;
  if (gap && gap.warn) {
    bucket.tone = "warn";
    bucket.sub = (gap.count === 1 ? "1 new table" : gap.count + " new tables") + " not in it yet";
    cards.push({ kind: "new-tables", key: sid + "|new-tables|" + (gapRun.new_tables_snapshot || gapRun.snapshot_time || "") + "|" + (gapRun.new_tables_action || ""), tone: "warn",
      title: gap.title,
      lines: [gap.text],
      cost: gap.actions.some((a) => a.run === "read") ? "Reading the database takes longer than a refresh, and writes may wait while it starts." : "",
      actions: gap.actions });
  } else if (gap) {
    bucket.sub = [bucket.sub, "new tables not checked"].filter(Boolean).join(" · ");
  }
  // How far back a row can be taken (#1950): the start of the window the
  // coverage read reports, the time alone when it opened the same day the
  // newest change landed, the date with it otherwise. Only beside a copy
  // that exists; greyed away with the rest downstream of a break.
  const rewindFrom = String(cov.delta_from || "");
  if (snap && !blUnknown && /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}/.test(rewindFrom)) {
    const sameDay = rewindFrom.slice(0, 10) === String(cov.delta_to || "").slice(0, 10);
    bucket.rewind = "rewind to " + (sameDay ? flowHHMM(rewindFrom) : rewindFrom.slice(0, 16).replace("T", " "));
  }
  // The SQL wire names the four use cards under the drawing (#1950): the
  // cards are the actions, so the drawing carries no action row of its own.
  const sql = piece("SQL", "none", "4 ways", "");
  // Whether a copy exists to take (the laptop card reads it): "button" where
  // one does, "setup" where none does yet, "none" where the listing could
  // not be read (a download over an unknown would promise what the page
  // cannot see). views.sql follows the session: it needs settings:read.
  const cta = blUnknown ? "none" : (bl.configured === false || !snap) ? "setup" : "button";
  const viewsSQL = may("settings:read");
  const reader = piece("Any reader", "none", "DuckDB here", "your tools");

  // Downstream of a break: grey, "as of" the same stamp on every piece.
  if (cut) {
    // No stamp (nothing was ever indexed): "not updating" says the same
    // without implying a change that never happened.
    const asOf = cut.at ? "as of " + cut.at : "not updating";
    const dim = (p) => { p.tone = "off"; p.big = ""; p.line = asOf; p.sub = ""; p.rewind = ""; p.schemaRetry = false; };
    if (cut.piece === "capture") { dim(engine); dim(update); }
    dim(bucket);
  }
  return { pieces: [source, capture, engine, update, bucket, sql, reader], cards, cut, cta, viewsSQL };
}

// ovFlowDismissed remembers the decision cards an operator closed, by server,
// kind and the stamp that raised them: the same alarm does not come back on
// the next refresh, a NEW one (another stamp) does.
const ovFlowDismissed = new Set();
// ovFlowSeq: the newest loadOvFlow issued. An older read landing after a
// newer one painted must not repaint the slot with what it found first.
let ovFlowSeq = 0;

// flowVerdict is the one word at the head of the strip: what the path is
// doing, in the tone of the piece that decides it. Never "All good" without
// a green capture AND a green copy: a fresh copy under a stopped capture is
// the false green the whole drawing exists to refuse.
function flowVerdict(model) {
  const capture = model.pieces[1], update = model.pieces[3];
  const tones = model.pieces.map((p) => p.tone);
  if (model.cut) {
    const word = model.cut.piece === "capture" ? "Capture stopped" : "Update stopped";
    return { tone: capture.tone === "none" ? "none" : "bad", word };
  }
  if (tones.includes("bad")) return { tone: "bad", word: "Needs attention" };
  if (tones.includes("warn")) return { tone: "warn", word: "Check the path" };
  if (capture.tone === "ok") return { tone: "ok", word: update.tone === "ok" ? "All good" : "Capturing" };
  if (/not capturing/.test(capture.line)) return { tone: "none", word: "Not capturing" };
  return { tone: "none", word: "No data yet" };
}

// flowSummaryLine is what the folded strip says in place of the drawing:
// the same figures, in order, each said once.
function flowSummaryLine(model) {
  const [, capture, engine, update, bucket] = model.pieces;
  const parts = [];
  const add = (s) => { if (s && !parts.includes(s)) parts.push(s); };
  add(capture.line);
  add(update.big ? "copy " + update.big : update.line);
  add(engine.line);
  add(bucket.sub);
  return parts.length ? " · " + parts.join(" · ") : "";
}

// flowGrid draws the seven pieces of the path (four stations, three
// arrows) from a model's pieces. Shared by the Overview flow and the
// Status strip (#1950): one drawing language, fed by each page's own read.
// A sub marked mono is data (a binlog position, an LSN) and is set in the
// data face; everything else stays in the UI face.
function flowGrid(pieces, ctx) {
  const grid = el("div", { class: "flow-grid" });
  const isArrow = (i) => i % 2 === 1;
  pieces.forEach((p, i) => {
    const node = el("div", { class: (isArrow(i) ? "flow-arrow" : "flow-box") + " " + (p.tone || "none") });
    if (isArrow(i)) {
      node.append(el("span", { class: "flow-label", text: String(p.title || "") }));
      node.append(el("span", { class: "flow-line", "aria-hidden": "true" }));
      const val = el("div", { class: "flow-val" });
      if (p.big) val.append(el("div", { class: "flow-big", text: p.big, title: p.stamp ? utcLocalTitle(p.stamp) || null : null, "data-stamp": p.stamp || null }));
      if (p.line) val.append(el("div", { class: "flow-state" }, el("span", { class: "health-dot " + (p.tone || "none") }), " " + p.line));
      if (p.sub) val.append(el("div", { class: "flow-sub" + (p.mono ? " flow-mono" : ""), text: p.sub }));
      if (p.action) {
        val.append(el("a", { class: "flow-link flow-fix", href: "#", text: p.action.label + " ›",
          onclick: (e) => { e.preventDefault(); runFlowAction(p.action.run, ctx); } }));
      }
      node.append(val);
    } else {
      const head = el("div", { class: "flow-box-head" });
      const marks = { 0: "mysql", 2: "logo", 4: "bucket", 6: "duck" };
      // The DBTrail box carries the white lockup in place of its title; the
      // source box the MySQL logo beside it.
      const dot = p.tone !== "none" ? el("span", { class: "health-dot " + p.tone }) : null;
      if (marks[i] === "logo") {
        const wrap = el("span", { class: "flow-lockup-wrap" }, el("img", { class: "flow-lockup", src: "/dbtrail-lockup-white.png", alt: p.title, width: "79", height: "30" }));
        if (dot) wrap.append(dot);
        head.append(wrap);
      } else {
        if (marks[i] === "mysql") head.append(el("img", { class: "flow-ico flow-ico-mysql", src: "/mysql-logo.png", alt: "MySQL", width: "46", height: "30" }));
        else if (marks[i]) head.append(icon(marks[i], "flow-ico"));
        const title = el("b", { text: p.title });
        if (dot) title.append(dot);
        head.append(title);
      }
      node.append(head);
      if (p.line) node.append(el("div", { class: "flow-state", text: p.line }));
      if (p.sub) node.append(el("div", { class: "flow-sub" + (p.mono ? " flow-mono" : ""), text: p.sub }));
      if (p.rewind) node.append(el("div", { class: "flow-sub", text: p.rewind }));
      if (p.schemaRetry) { const b = schemaSnapshotButton(); if (b) node.append(b); }
    }
    grid.append(node);
  });
  return grid;
}

// flowSection paints one model as a foldable strip (#1950): a summary line
// (the verdict, and the figures when folded) over stations and wires in one
// grid (style.css .flow), about 180px tall so the use cards under it stay
// above the fold. Plain HTML: the decision card under a broken wire carries
// buttons and wrapping text, which an SVG cannot hold, and under 880 px the
// grid stacks. Colour and label change by class and text; nothing is drawn.
// Open the first time, then as this browser left it.
function flowSection(model, ctx) {
  const sec = el("details", { class: "flow" + (model.cut ? " flow-cut" : ""), "aria-label": "The path from your database to its copy" });
  const top = el("summary", { class: "flow-top" });
  const verdict = flowVerdict(model);
  top.append(el("span", { class: "flow-verdict " + verdict.tone },
    el("span", { class: "health-dot " + verdict.tone, "aria-hidden": "true" }), " " + verdict.word));
  const sumline = el("span", { class: "flow-sumline" });
  top.append(sumline);
  top.append(el("span", { class: "flow-foldhint", "aria-hidden": "true" }));
  sec.append(top);
  const grid = flowGrid(model.pieces, ctx);
  sec.append(grid);
  // One decision at a time: the card of the piece that broke first.
  const card = model.cards.find((c) => !ovFlowDismissed.has(c.key));
  if (card) sec.append(flowCard(card, ctx, () => { ovFlowDismissed.add(card.key); sec.replaceWith(flowSection(model, ctx)); }));
  // Folded, the summary line carries the figures the drawing would show;
  // open, only the verdict, so nothing is said twice on one screen.
  const fill = () => { sumline.textContent = sec.open ? "" : flowSummaryLine(model); };
  ovFoldRemember(sec, "flow", true);
  sec.addEventListener("toggle", fill);
  fill();
  return sec;
}

// runFlowAction is the fix an arrow link or a card button names (#1853):
// the server form for the selected server (its source is missing), the add
// form (the selected entry is the daemon's own index), or the Snapshots
// setup (no schedule). One place, so the link and the button cannot drift.
function runFlowAction(run, ctx) {
  if (run === "add-source") { openServersModal(); editServer(ctx.serverId); return; }
  if (run === "add-server") { openServersModal(); showServerForm(null); return; }
  if (run === "schedule") { navigate("snapshots#setup"); return; }
}

// flowCard is one decision: what happened, what it costs, and buttons with a
// verb each. The expensive option is never the primary button; every button
// closes the card.
function flowCard(card, ctx, close) {
  const box = el("div", { class: "flow-card " + (card.tone === "bad" ? "bad-box" : card.tone === "warn" ? "warn-box" : "muted-box"), role: "region", "aria-label": card.title });
  box.append(el("b", { text: card.title }));
  (card.lines || []).forEach((l) => box.append(el("div", { class: "warn-line", text: l })));
  if (card.raw) box.append(rawErrorFold(card.raw));
  if (card.recipe) {
    const d = el("details", { class: "flow-recipe" });
    d.append(el("summary", { text: "See what to check" }));
    const ul = el("ul");
    card.recipe.forEach((r) => ul.append(el("li", { text: r })));
    d.append(ul);
    box.append(d);
  }
  const acts = el("div", { class: "warn-actions" });
  (card.actions || []).forEach((a) => {
    if (a.run === "start" && !(ctx.registry && ctx.monitorCap)) return;
    if (a.run === "read" && !(ctx.registry && ctx.monitorCap && sessionMay(PERM_SNAPSHOT_CREATE))) return;
    if ((a.run === "add-source" || a.run === "add-server") && !(ctx.monitorCap && sessionMay("servers:write"))) return;
    const b = el("button", { class: "btn btn-sm" + (a.primary ? " btn-primary" : " btn-ghost"), type: "button", text: a.label });
    b.onclick = () => {
      // A guarded action needs a real yes; where confirm is missing the
      // expensive read does not run on a missing answer.
      if (a.confirm && !(typeof window.confirm === "function" && window.confirm(a.confirm))) return;
      if (a.run === "status") { navigate("status"); return; }
      if (a.run === "start") { startMonitorRow(ctx.serverId); close(); return; }
      if (a.run === "add-source" || a.run === "add-server") { runFlowAction(a.run, ctx); close(); return; }
      if (a.run === "read") {
        // The card stays until the next repaint says what the read did:
        // closing it here would hide the decision when the POST fails
        // (createBaseline only toasts). The button says it was pressed.
        b.disabled = true; b.textContent = "Reading…";
        createBaseline(ctx.serverId);
        return;
      }
      close();
    };
    acts.append(b);
  });
  if (acts.children.length) box.append(acts);
  if (card.cost) box.append(el("div", { class: "flow-cost", text: card.cost }));
  return box;
}

// loadOvFlow gathers the reads the flow needs and paints it. The coverage
// payload comes from the caller (the page already reads it; a second read per
// cycle would double the count the live test pins). The three others are
// best-effort: a 403 on the schema snapshot (a serve without the daemon) or a
// missing monitor status paints "not known", never a colour.
// coverageP is the /api/coverage read IN FLIGHT, not its answer: the flow's
// own reads (servers, the snapshot list, the tables left out) go out at
// once, beside it, and the paint waits for all of them together. Chained
// after the coverage answer, the snapshot list started only when coverage
// landed, and the head of the page took the two latencies in a row (#1847).
// A coverageP that resolves null (the refresh loop's failed read) paints
// nothing, the way that loop never called this on a failure, and it must
// not cancel a paint in flight either: the sequence that drops a late
// paint is taken once coverage has answered, not when the reads go out.
function loadOvFlow(f, live, coverageP) {
  if (serversEmpty) { clear(f.flowSlot); if (f.useSlot) clear(f.useSlot); return Promise.resolve(); }
  const id = currentServer || defaultServerId;
  const read = (path) => apiWithin(path, OV_REQUEST_MS);
  // A read that fails is said as a failure by the model, never as a fact
  // ("no copy yet") and never as a colour: the marker carries the status so
  // a 403 (serve, or a session without the permission) can read "not from
  // here" while a 500 or a timeout reads "could not be read".
  const servers = read("/api/servers").then((d) => ({ srv: ((d && d.servers) || []).find((s) => s.id === id) || null }), (err) => ({ srv: null, unknown: true, err }));
  const baselines = read("/api/baselines").then((d) => d || {}, () => ({ unavailable: true }));
  const uncaptured = read("/api/uncaptured-tables").then((d) => d || {}, () => ({}));
  return Promise.all([coverageP, servers]).then(([coverage, { srv, unknown, err }]) => {
    if (coverage === null) return;
    const seq = ++ovFlowSeq;
    if (unknown) console.error("flow: server list unavailable", err);
    const registry = !!(srv && srv.kind === "registry" && srv.has_source);
    const wantMonitor = registry && /^(failed|stalled|lost_position|stopped)$/.test(srv.monitor_state || "");
    const monitor = wantMonitor ? read("/api/servers/" + encodeURIComponent(id) + "/monitor").then((d) => (d && d.monitor) || {}, () => ({})) : Promise.resolve({});
    const schema = registry && capsCache.monitor
      ? read("/api/servers/" + encodeURIComponent(id) + "/schema-snapshot").then((d) => (d && d.schema_snapshot) || { unavailable: true, status: 0 }, (e) => ({ unavailable: true, status: (e && e.status) || 0 }))
      : Promise.resolve({ unavailable: true, status: 403 });
    return Promise.all([baselines, uncaptured, monitor, schema]).then(([bl, unc, mon, sch]) => {
      if (!live() || seq !== ovFlowSeq) return;
      const inp = { coverage, baselines: bl, server: srv, serverUnknown: !!unknown, monitor: mon, schema: sch, uncaptured: unc,
        monitorCap: !!capsCache.monitor, may: sessionMay };
      const pctx = { serverId: id, registry, monitorCap: !!capsCache.monitor };
      ovFlowLast = { inp, pctx, slot: f.flowSlot, use: f.use || null, gen: serverGen };
      const model = ovFlowModel(inp);
      clear(f.flowSlot);
      f.flowSlot.append(flowSection(model, pctx));
      if (f.use) f.use.update(model, inp);
    });
  });
}

// ovFlowLast is what the drawing on screen was painted from, so that a
// coverage read that changes after the paint (the card's own refresh, the
// source's answer about capture, #1794) repaints it without reading
// anything again.
let ovFlowLast = null;

// ovFlowRepaint paints the drawing again with `coverage` in place of the
// one it was painted from. Nothing to do when no drawing is on screen, or
// the one on screen is another server's.
function ovFlowRepaint(coverage) {
  const last = ovFlowLast;
  if (!last || !coverage || last.gen !== serverGen || !last.slot.isConnected) return;
  last.inp = Object.assign({}, last.inp, { coverage });
  const model = ovFlowModel(last.inp);
  clear(last.slot);
  last.slot.append(flowSection(model, last.pctx));
  if (last.use) last.use.update(model, last.inp);
}

// ── Use your copy (#1950) ────────────────────────────────────────────────────
//
// Four ways to read the copy, one card each, under the flow. The question a
// reader brings is "now or fast?" and "always current or offline?", so every
// card answers both in its tags before its sentence, and carries ONE action.
// The card that is picked opens its panel under the row, one at a time. The
// cards never list tables: the row reads the same on a server with 20 tables
// and one with 2,000, and table names live inside a panel, behind a filter.
// The drawings are static constants (svgEl: never data); the words on the
// cards are the same on every server, and only the figures move
// (useCopySection.update): the table count in the header, the newest copy
// behind the download.

// USE_ART: one drawing per card, 120x64, in the page's tokens so it follows
// the theme. No text inside: a drawing is not prose.
const USE_ART = {
  sql: `<svg viewBox="0 0 120 64" aria-hidden="true"><rect x="4" y="4" width="112" height="56" rx="8" fill="var(--surface)" stroke="var(--line)"/><circle cx="14" cy="13" r="2.5" fill="var(--pink)"/><circle cx="22" cy="13" r="2.5" fill="var(--sun)"/><circle cx="30" cy="13" r="2.5" fill="var(--ok)"/><rect x="12" y="22" width="52" height="4" rx="2" fill="var(--pink-deep)"/><rect x="12" y="30" width="34" height="4" rx="2" fill="var(--ink-4)"/><rect x="12" y="42" width="96" height="3" rx="1.5" fill="var(--line)"/><rect x="12" y="49" width="70" height="3" rx="1.5" fill="var(--line)"/><rect x="88" y="21" width="20" height="6" rx="3" fill="var(--ink)"/></svg>`,
  laptop: `<svg viewBox="0 0 120 64" aria-hidden="true"><rect x="22" y="10" width="76" height="40" rx="5" fill="var(--surface)" stroke="var(--ok)" stroke-width="1.5"/><rect x="12" y="50" width="96" height="6" rx="3" fill="var(--ok)"/><rect x="34" y="20" width="28" height="4" rx="2" fill="var(--line)"/><rect x="34" y="28" width="40" height="4" rx="2" fill="var(--line)"/><rect x="34" y="36" width="22" height="4" rx="2" fill="var(--line)"/><path d="M82 18v14m-6-5 6 6 6-6" fill="none" stroke="var(--ok)" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
  dash: `<svg viewBox="0 0 120 64" aria-hidden="true"><rect x="10" y="6" width="100" height="52" rx="7" fill="var(--surface)" stroke="var(--line)"/><rect x="22" y="36" width="10" height="14" rx="2" fill="var(--orange)"/><rect x="36" y="26" width="10" height="24" rx="2" fill="var(--sun)"/><rect x="50" y="32" width="10" height="18" rx="2" fill="var(--orange)"/><path d="M70 44 82 30l10 8 10-16" fill="none" stroke="var(--ink)" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/><rect x="22" y="14" width="30" height="4" rx="2" fill="var(--ink-4)"/></svg>`,
  client: `<svg viewBox="0 0 120 64" aria-hidden="true"><rect x="6" y="6" width="108" height="52" rx="7" fill="var(--strip)"/><rect x="14" y="18" width="14" height="4" rx="2" fill="var(--pink-mid)"/><rect x="32" y="18" width="52" height="4" rx="2" fill="var(--strip-ink)" opacity=".85"/><rect x="14" y="28" width="14" height="4" rx="2" fill="var(--pink-mid)"/><rect x="32" y="28" width="30" height="4" rx="2" fill="var(--sun)"/><rect x="14" y="40" width="90" height="3" rx="1.5" fill="var(--strip-ink)" opacity=".35"/><rect x="14" y="47" width="60" height="3" rx="1.5" fill="var(--strip-ink)" opacity=".35"/></svg>`,
};

// USE_CARDS: title, the tags that answer the two questions (the green ones
// are the yes), one sentence, one action. The `cap` card exists only where
// the server reports the capability.
const USE_CARDS = [
  { id: "sql", title: "Ask it here", cap: "sql", action: "Open SQL", primary: true,
    tags: [["now", true], ["always current", true], ["nothing to install", false]],
    line: () => ["Write SQL in this page. It runs on DBTrail's copy, not on MySQL."] },
  { id: "laptop", title: "Take it to your laptop", action: "Download",
    tags: [["fast", true], ["works offline", true], ["the copy at one moment", false]],
    line: () => ["One download: your tables plus ", el("code", { text: DUCKDB_VIEWS_FILE }), ". Open it in DuckDB."] },
  { id: "dash", title: "Dashboards for the team", action: "Set it up",
    tags: [["fast", true], ["always current", true], ["reads from S3", false]],
    line: () => ["Any DuckDB, Metabase included, reads the copy straight from your S3."] },
  { id: "client", title: "From your MySQL client", action: "Show host and port",
    tags: [["now", true], ["always current", true], ["one row or table, as it was", false]],
    line: () => ["Point your client at the time-travel port and add ", el("code", { text: "AS OF" }), "."] },
];

// ── SQL on the copy (#1952) ──────────────────────────────────────────────────
//
// The panel "Ask it here" opens: a list of the tables the copy defines, a
// plain textarea, Run, and the result as a table. The statement runs on the
// server in a separate sandboxed process over the Parquet copy (POST
// /api/sql), never on MySQL. Nothing here parses or highlights SQL, and
// nothing is fetched from anywhere but this console.
//
// The pieces that decide what a person reads are pure functions, so they are
// tested with plain values (assets_sql_panel_test.go).

// SQL_LIST_MAX is how many table names the list paints at once. A copy can
// define thousands of views; the filter narrows, the list never grows.
const SQL_LIST_MAX = 50;
// SQL_CELL_SHOW is how many characters of one cell the table paints. The
// full value stays in the CSV; a result table is for reading, not storing.
const SQL_CELL_SHOW = 300;

// sqlColumnIsNumeric: whether a DuckDB column type is a number, so its
// column right-aligns. By TYPE, never by looking at the values: a text
// column full of digits (a zip code) stays left.
function sqlColumnIsNumeric(type) {
  return /^(U?(TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT)|FLOAT|DOUBLE|REAL|DECIMAL(\(.*\))?)$/i.test(String(type || "").trim());
}

// sqlAgo: "3 min ago" for an ISO time, "" when it cannot be read.
function sqlAgo(iso, nowMs) {
  const t = Date.parse(iso);
  if (!isFinite(t)) return "";
  const sec = Math.max(0, (nowMs - t) / 1000);
  if (sec < 60) return "just now";
  if (sec < 3600) return Math.round(sec / 60) + " min ago";
  if (sec < 86400) return Math.round(sec / 3600) + " h ago";
  const d = Math.round(sec / 86400);
  return d + (d === 1 ? " day ago" : " days ago");
}

// sqlStatusLine: the one line that says what a query runs on and under
// which limits, from what the server reported (GET /api/sql), never from
// numbers written here.
function sqlStatusLine(info, nowMs) {
  const parts = [];
  const ago = info && info.copy_updated_at ? sqlAgo(info.copy_updated_at, nowMs) : "";
  parts.push(ago ? "runs on the copy updated " + ago : "runs on DBTrail's copy");
  parts.push("read-only");
  const lim = (info && info.limits) || {};
  if (lim.timeout_seconds) parts.push(lim.timeout_seconds + " s limit");
  return parts.join(" · ");
}

// sqlFilterViews: the names to paint for a filter, at most `max`, and how
// many more match. Case-insensitive substring; an empty filter is the head
// of the list.
function sqlFilterViews(names, filter, max) {
  const f = String(filter || "").trim().toLowerCase();
  const all = (names || []).filter((n) => !f || String(n).toLowerCase().includes(f));
  return { shown: all.slice(0, max), more: Math.max(0, all.length - max), matched: all.length };
}

// sqlStarterQuery: a first query over the first table (a table of the
// source, named as on the source, e.g. demo.prices, when there is one; else
// whatever the copy defines, which is the events view).
function sqlStarterQuery(names) {
  const list = names || [];
  const first = list.find((n) => n !== "events") || list[0];
  return first ? "SELECT * FROM " + first + " LIMIT 100" : "";
}

// sqlCell: how one result cell is shown. NULL is its own token (isNull),
// distinct from the text "NULL" and from an empty string. A nested value
// (LIST, STRUCT, JSON) is its compact JSON text. Nothing here parses a
// string cell: a long JSON value may have been cut on the server and is no
// longer valid JSON.
function sqlCell(v) {
  if (v === null || v === undefined) return { text: "NULL", isNull: true };
  if (typeof v === "object") return { text: JSON.stringify(v), isNull: false };
  return { text: String(v), isNull: false };
}

// sqlCountLine: "3 rows in 184 ms". The time is the round trip the person
// waited, measured in the page (the worker's own clock covers the statement
// alone, not starting the process or installing the views, and reads "0 ms"
// for a small query that took a fifth of a second to come back).
function sqlCountLine(res, ms) {
  const n = ((res && res.rows) || []).length;
  const t = Math.round(ms || 0);
  const took = t < 1 ? "under 1 ms" : t < 1000 ? t + " ms" : (t / 1000).toFixed(1) + " s";
  return n.toLocaleString("en-US") + (n === 1 ? " row" : " rows") + " in " + took;
}

// sqlResultNotes: what was cut, said out loud. A result cut at the row cap,
// and long values cut in place (the count says how many, not which).
function sqlResultNotes(res, exactInts) {
  const notes = [];
  const n = ((res && res.rows) || []).length;
  // A browser that cannot hand the page a number's own digits rounds whole
  // numbers past 2^53 (an id of nineteen digits). Said where it can happen:
  // a BIGINT or UBIGINT column on such a browser.
  if (!exactInts && ((res && res.columns) || []).some((c) => /^U?BIGINT$/i.test(String(c.type || "").trim()))) {
    notes.push("Large whole numbers may be rounded on this browser. Download CSV has every digit.");
  }
  if (res && res.truncated) notes.push("Showing the first " + n.toLocaleString("en-US") + " rows. The result has more: add a WHERE or a LIMIT to see a different part.");
  const cut = (res && res.truncated_cells) || 0;
  if (cut > 0) notes.push(cut === 1 ? "1 long value was cut." : cut.toLocaleString("en-US") + " long values were cut.");
  return notes;
}

// sqlErrorView: one plain sentence per way a query can fail, and, where the
// server's own words are the useful part (what DuckDB said, why a statement
// was refused), those words verbatim as `detail`.
function sqlErrorView(status, message, limits) {
  const msg = String(message || "");
  const lim = limits || {};
  switch (status) {
    case 400: return { text: "The request was not understood.", detail: msg };
    case 403:
      if (/data profile/.test(msg)) return { text: "SQL is off while a data profile is active. The profile filters what the web interface shows, and SQL reads the raw files, which it cannot filter.", detail: "" };
      return { text: "Your session is not allowed to run SQL.", detail: "" };
    case 409:
      if (/only on S3/.test(msg)) return { text: "The copy for this server is only on S3. SQL in the web interface needs a local copy.", detail: "" };
      if (/no copy|no view/.test(msg)) return { text: "There is no copy to run SQL on yet.", detail: "" };
      return { text: "This copy cannot be queried from the web interface.", detail: msg };
    case 422: return { text: "The query did not run.", detail: msg };
    case 429: return { text: "SQL on the copy is busy. Try again in a moment.", detail: msg };
    case 504: return { text: "The query ran longer than the " + (lim.timeout_seconds ? lim.timeout_seconds + " s " : "time ") + "limit and was stopped. Narrow it: a WHERE on a table, or a smaller window on events.", detail: "" };
    case 500: return { text: "The query could not be run. DBTrail's log has the details.", detail: "" };
    case 502: return { text: "The copy could not be read.", detail: msg };
    default: return { text: status ? "The query failed." : "The server did not answer.", detail: msg };
  }
}

// SQL_EXACT_INTS: whether this engine hands a JSON reviver the number's
// source text. Asked once; without it sqlParseResult cannot keep the digits
// of a whole number past 2^53, and sqlResultNotes says so on the result.
const SQL_EXACT_INTS = (() => {
  try { return JSON.parse("1", function (k, v, c) { return !!(c && typeof c.source === "string"); }) === true; } catch (_) { return false; }
})();

// sqlParseResult reads the JSON result keeping every digit of an integer
// JavaScript cannot hold (a BIGINT past 2^53 comes back as its digits, as
// text) where the engine exposes the source text to the reviver.
function sqlParseResult(text) {
  return JSON.parse(text, function (k, v, c) {
    return typeof v === "number" && c && typeof c.source === "string" && !Number.isSafeInteger(v) && /^-?\d+$/.test(c.source) ? c.source : v;
  });
}

// sqlPost sends one statement. JSON by default; with csv it asks for the
// CSV form (Accept: text/csv) and returns the file's text. Same auth, server
// header and 401 handling as api().
async function sqlPost(statement, signal, csv) {
  const headers = { "Content-Type": "application/json" };
  if (csv) headers.Accept = "text/csv";
  if (TOKEN) headers.Authorization = "Bearer " + TOKEN;
  if (currentServer) headers["X-Bintrail-Server"] = currentServer;
  const res = await fetch("/api/sql", { method: "POST", headers, body: JSON.stringify({ sql: statement }), signal });
  const text = await res.text();
  if (!res.ok) {
    if (res.status === 401) handleUnauthorized();
    let msg = text || "HTTP " + res.status;
    try { const j = JSON.parse(text); if (j && j.error) msg = j.error; } catch (_) { /* raw text it is */ }
    throw apiError(res.status, msg);
  }
  if (csv) return text;
  try { return sqlParseResult(text); } catch (_) { throw new Error("malformed response from /api/sql"); }
}

// sqlResultTable paints the result: a real table with a caption, a header
// that stays while the rows scroll, numbers right-aligned by column type.
function sqlResultTable(res, ms) {
  const cols = res.columns || [];
  const numeric = cols.map((c) => sqlColumnIsNumeric(c.type));
  const table = el("table", { class: "sqlp-table" });
  table.append(el("caption", { class: "sqlp-sr", text: "Query result, " + sqlCountLine(res, ms) }));
  const hr = el("tr");
  cols.forEach((c, i) => hr.append(el("th", { scope: "col", class: numeric[i] ? "num" : "", text: c.name, title: c.type })));
  table.append(el("thead", null, hr));
  const body = el("tbody");
  (res.rows || []).forEach((row) => {
    const tr = el("tr");
    row.forEach((v, i) => {
      const cell = sqlCell(v);
      const td = el("td", { class: numeric[i] ? "num" : "" });
      if (cell.isNull) td.append(el("span", { class: "sqlp-null", text: "NULL" }));
      else if (cell.text.length > SQL_CELL_SHOW) { td.textContent = cell.text.slice(0, SQL_CELL_SHOW) + "…"; td.title = cell.text.slice(0, 2000); }
      else td.textContent = cell.text;
      tr.append(td);
    });
    body.append(tr);
  });
  table.append(body);
  return el("div", { class: "sqlp-scroll" }, table);
}

// renderSQLPanel paints the panel into `box` and returns true. One query at
// a time: Run is off while one runs, and the previous result is cleared, not
// left on screen under a new statement. A running query is aborted by
// Cancel, and also when the panel goes away under it (Close, another card,
// another server, another page): the server stops the worker when the
// request is dropped, so the next Run does not wait behind the abandoned one.
function renderSQLPanel(box) {
  const gen = serverGen;
  const st = { info: null, ctl: null, lastSQL: "" };
  const alive = () => gen === serverGen && box.isConnected;

  const filter = el("input", { class: "sqlp-filter", type: "search", placeholder: "Filter tables", "aria-label": "Filter tables", autocomplete: "off" });
  const list = el("div", { class: "sqlp-list", role: "list" });
  const tables = el("div", { class: "sqlp-tables" }, filter, list);

  const ta = el("textarea", { class: "sqlp-editor", id: "sqlp-sql", rows: "6", spellcheck: "false", autocomplete: "off", autocapitalize: "off" });
  const run = el("button", { class: "btn btn-sm btn-primary sqlp-run", type: "button", text: "Run", title: "Ctrl+Enter or ⌘+Enter" });
  const cancel = el("button", { class: "btn btn-sm sqlp-cancel", type: "button", text: "Cancel" });
  cancel.hidden = true;
  const count = el("span", { class: "sqlp-count" });
  const csv = el("button", { class: "btn btn-sm btn-ghost sqlp-csv", type: "button", text: "Download CSV" });
  csv.disabled = true;
  const meta = el("span", { class: "sqlp-meta", text: sqlStatusLine(null, Date.now()) });
  const msg = el("div", { class: "sqlp-msg", role: "status", "aria-live": "polite" });
  const results = el("div", { class: "sqlp-results", tabindex: "-1", role: "region", "aria-label": "Query results" });
  const right = el("div", { class: "sqlp-q" },
    el("label", { class: "sqlp-sr", for: "sqlp-sql", text: "SQL statement" }), ta,
    el("div", { class: "sqlp-bar" }, run, cancel, count, csv), msg, results);
  box.append(el("div", { class: "sqlp" }, tables, right), el("p", { class: "sqlp-foot" }, meta));

  const insert = (name) => {
    const a = ta.selectionStart || 0, b = ta.selectionEnd || 0;
    ta.value = ta.value.slice(0, a) + name + ta.value.slice(b);
    ta.focus();
    ta.selectionStart = ta.selectionEnd = a + name.length;
  };
  const paintList = () => {
    clear(list);
    const names = (st.info && st.info.views) || [];
    filter.placeholder = names.length ? "Filter " + names.length.toLocaleString("en-US") + (names.length === 1 ? " table" : " tables") : "Filter tables";
    const f = sqlFilterViews(names, filter.value, SQL_LIST_MAX);
    f.shown.forEach((n) => list.append(el("button", { class: "sqlp-name", type: "button", role: "listitem", text: n, title: "Insert " + n, onclick: () => insert(n) })));
    if (f.more > 0) list.append(el("div", { class: "sqlp-more", text: "… " + f.more.toLocaleString("en-US") + " more. Filter to narrow." }));
    if (names.length && f.matched === 0) list.append(el("div", { class: "sqlp-more", text: "No table matches." }));
    // Tables listed under another name, or not at all, and why (#2013).
    if (!filter.value) ((st.info && st.info.notes) || []).forEach((n) => list.append(el("div", { class: "sqlp-more sqlp-note", text: n })));
  };
  filter.addEventListener("input", paintList);

  const showError = (err) => {
    clear(msg);
    const v = sqlErrorView((err && err.status) || 0, (err && err.message) || "", st.info && st.info.limits);
    msg.append(el("p", { class: "sqlp-err", text: v.text }));
    if (v.detail) msg.append(el("pre", { class: "sqlp-detail", text: v.detail }));
  };
  const busy = (on) => {
    run.disabled = on;
    cancel.hidden = !on;
    ta.readOnly = on;
  };

  const runNow = async () => {
    if (st.ctl) return;
    const statement = ta.value;
    if (!statement.trim()) { clear(msg); msg.append(el("p", { class: "sqlp-err", text: "Write a query first." })); return; }
    clear(msg); clear(results);
    count.textContent = "";
    csv.disabled = true;
    st.lastSQL = "";
    results.append(ovSkelLines(4));
    const ctl = new AbortController();
    st.ctl = ctl;
    busy(true);
    // The panel can be taken away while the query runs (Close, another
    // card, a server switch, another page). Nothing tells it so, so it
    // looks: the moment it is gone the request is dropped, which is what
    // stops the worker on the server. Removing the panel is a change to
    // the page, so the observer sees it at once; the timer is for a server
    // switch, which changes the server first and the page a moment later.
    const dropIfGone = () => { if (!alive()) ctl.abort(); };
    const seen = typeof MutationObserver === "function" ? new MutationObserver(dropIfGone) : null;
    if (seen) seen.observe(document.body, { childList: true, subtree: true });
    const gone = setInterval(dropIfGone, 200);
    const t0 = performance.now();
    try {
      const res = await sqlPost(statement, ctl.signal, false);
      if (!alive()) return;
      const ms = performance.now() - t0;
      clear(results);
      st.lastSQL = statement;
      count.textContent = sqlCountLine(res, ms);
      sqlResultNotes(res, SQL_EXACT_INTS).forEach((n) => results.append(el("p", { class: "sqlp-note", text: n })));
      if ((res.columns || []).length) results.append(sqlResultTable(res, ms));
      csv.disabled = false;
      // The copy a later query runs on may be newer than the one the panel
      // opened on: the answer carries the snapshot it actually used.
      if (st.info && res.copy_updated_at) { st.info.copy_updated_at = res.copy_updated_at; meta.textContent = sqlStatusLine(st.info, Date.now()); }
      results.focus();
    } catch (err) {
      if (!alive()) return;
      clear(results);
      if (err && err.name === "AbortError") { clear(msg); msg.append(el("p", { class: "sqlp-note", text: "Cancelled." })); }
      else showError(err);
    } finally {
      clearInterval(gone);
      if (seen) seen.disconnect();
      if (st.ctl === ctl) st.ctl = null;
      if (alive()) busy(false);
    }
  };
  run.onclick = runNow;
  cancel.onclick = () => { if (st.ctl) st.ctl.abort(); };
  ta.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) { e.preventDefault(); runNow(); }
  });
  csv.onclick = async () => {
    if (!st.lastSQL) return;
    csv.disabled = true;
    try {
      const text = await sqlPost(st.lastSQL, undefined, true);
      downloadBlob("dbtrail-sql.csv", text, "text/csv");
    } catch (err) {
      if (alive()) showError(err);
    } finally {
      if (alive()) csv.disabled = !st.lastSQL;
    }
  };

  // What the copy defines, when it was updated, and the real limits: one
  // cheap read (no query runs), so the list and the status line are true
  // before the first Run.
  list.append(ovSkelLines(3));
  api("/api/sql").then((info) => {
    if (!alive()) return;
    st.info = info || {};
    meta.textContent = sqlStatusLine(st.info, Date.now());
    paintList();
    if (!ta.value) ta.value = sqlStarterQuery(st.info.views);
  }, (err) => {
    if (!alive()) return;
    clear(list);
    list.append(el("div", { class: "sqlp-more", text: "The table list could not be read." }));
    showError(err);
  });
  return true;
}

// USE_PANELS: what each card opens under the row. Each reuses the surface
// that already does the job elsewhere in the console, so the Overview never
// grows a second way to download, set up or connect.
const USE_PANELS = {
  sql(body) { renderSQLPanel(body); },
  laptop(body, st) {
    if (st.cta === "none") {
      body.append(el("p", { class: "use-note", text: "The list of copies could not be read. Reload the page to try again." }));
      return;
    }
    // The row hides while no copy exists; this is the panel left open when
    // the newest copy went away under it.
    if (st.cta === "setup" || !st.snap) {
      body.append(el("p", { class: "use-note", text: "No copy to take yet." }));
      return;
    }
    body.append(el("p", { class: "use-note" }, "The newest copy, from ",
      el("b", { text: st.snap.time, title: utcLocalTitle(st.snap.time) || null }), " UTC. Open the folder in DuckDB and run ",
      el("code", { text: DUCKDB_VIEWS_FILE }), "."));
    const row = el("div", { class: "use-acts" });
    const msg = el("p", { class: "form-msg err" });
    msg.hidden = true;
    // Row data takes query:execute, like the same download on Snapshots.
    if (sessionMay("query:execute")) {
      const dl = el("button", { class: "btn", type: "button", text: "Download the data" });
      dl.onclick = async () => {
        const err = await downloadNewestBackup(st.snap.time, dl);
        if (err && !msg.isConnected) { toastError(err); return; }
        msg.textContent = err;
        msg.hidden = !err;
      };
      row.append(dl);
    } else {
      body.append(el("p", { class: "use-note", text: "Your session may not download row data." }));
    }
    if (st.viewsSQL && capsCache.views) {
      const vb = el("button", { class: "btn", type: "button", text: "Download " + DUCKDB_VIEWS_FILE });
      vb.onclick = async () => {
        vb.disabled = true;
        try { await downloadViewsSQL({}); }
        catch (err) { toastError("could not generate views: " + ((err && err.message) || err)); }
        finally { vb.disabled = false; }
      };
      row.append(vb);
    }
    body.append(row, msg);
  },
  dash(body) {
    // The always-current path (#2014): see dashPanelBody. The local folder's
    // schema card stays on MCP Server, where the dashboards guide sends a tool
    // that runs on this machine.
    if ((capsCache.permissions || {})["settings:read"] === false) {
      body.append(el("p", { class: "use-note", text: dashErrorText(403, "") }));
      return;
    }
    body.append(ovSkelLines(3));
    const gen = serverGen;
    api("/api/dashboards").then((doc) => {
      if (gen !== serverGen || !body.isConnected) return;
      clear(body);
      body.append(dashPanelBody(doc || {}));
    }, (err) => {
      if (gen !== serverGen || !body.isConnected) return;
      clear(body);
      body.append(el("p", { class: "use-note", text: dashErrorText(err && err.status, (err && err.message) || String(err)) }));
    });
  },
  client(body) {
    // The same panel MCP Server carries, read when the card is opened and
    // not before: two requests nobody asked for would run on every paint.
    body.append(ovSkelLines(3));
    const gen = serverGen;
    Promise.all([
      api("/api/flashback").catch(() => null),
      api("/api/servers").then((d) => (d && d.servers) || [], () => []),
    ]).then(([fb, servers]) => {
      if (gen !== serverGen || !body.isConnected) return;
      clear(body);
      body.append(sqlClientPanel(servers, fb));
    });
  },
};

// useCopySection builds the row once and returns { section, update }: the
// cards and the panel slot are static, update() moves the figures.
// Dashboards for the team (#2014). The always-current path: a small views
// file whose views read the server's S3 snapshot location, so a teammate's
// DuckDB (or Metabase on DuckDB) reads the newest snapshot with nothing
// downloaded. Only S3 can be reached from their machine, so a server that keeps
// its snapshots only here is told so and sent to add a bucket, instead of being
// handed a file whose every path names this host.
//
// The file arrives inside GET /api/dashboards, beside the facts shown here, so
// the card cannot describe one snapshot and save a file made against another.
const DASH_DB = "team.duckdb";

function dashErrorText(status, message) {
  if (status === 403) {
    if (/profile/.test(String(message || ""))) return "Not available while a data profile is active. The file reads the raw files, which the profile cannot filter.";
    return "Your session is not allowed to get this file.";
  }
  return "The file for this server could not be prepared: " + (message || "no answer") + ".";
}

function dashTablesWord(n) {
  return n === 1 ? "1 table" : n + " tables";
}

function dashPanelBody(doc) {
  const box = el("div", { class: "dash-body" });
  const note = (text) => el("p", { class: "use-note", text });
  const hint = (...kids) => el("p", { class: "form-hint" }, ...kids);
  const go = (label, target) => el("div", { class: "use-acts" },
    el("button", { class: "btn btn-primary", type: "button", text: label, onclick: () => navigate(target) }));
  switch (doc.state) {
    case "s3": break;
    case "s3_empty":
      box.append(note("Snapshots for this server go to S3, at " + doc.s3 + ", and none has finished uploading there yet. The file is offered here once one has."),
        go("See Snapshots", "snapshots"));
      return box;
    case "s3_pattern_chars":
      box.append(note("The name of the S3 location, " + doc.s3 + ", contains [, *, ? or {. DuckDB reads those as wildcards, so a file for this location would not find the snapshots. Use an S3 location without them."),
        go("Change the S3 location", "snapshots#setup"));
      return box;
    case "s3_unreadable":
      box.append(note("DBTrail could not prepare the file for the S3 location " + doc.s3 + ". The reason is below."));
      if (doc.error) box.append(hint(doc.error));
      box.append(go("See Snapshots", "snapshots"));
      return box;
    case "local_only":
      // Snapshots page, "Where and how often": the S3 field left the server
      // form for that page (#1582).
      box.append(note("This server keeps its snapshots only on this machine, so a teammate's DuckDB cannot reach them."),
        note("Add an S3 location for this server, and this card gives a small file that reads the newest snapshot from anywhere."),
        go("Add an S3 location", "snapshots#setup"),
        hint("A tool running on this same machine can read the local folder instead. The guide has the steps."),
        docsMore("guides/dashboards", "", "dashboards on the copy"));
      return box;
    case "s3_timeout":
      box.append(note("S3 did not answer within " + (doc.timeout_seconds || 0) + " s, so there is no file yet. Open the card again in a moment. If it keeps happening, check the S3 location on Snapshots."),
        go("See Snapshots", "snapshots"));
      return box;
    case "no_archive":
      box.append(note("This server is set not to read its copy's files, so there is no file to give. An admin can turn that setting off in the server's configuration."));
      return box;
    case "no_archive_profile":
      box.append(note("This console runs under a data profile, which turns off reading the copy's files for every server: a file read outside DBTrail could not be filtered by the profile. Start the console without the profile to offer the file."));
      return box;
    default:
      box.append(note("No snapshot yet. Set up snapshots with an S3 location, and this card gives a file your team can open."),
        go("Set up snapshots", "snapshots#setup"));
      return box;
  }
  box.append(note(doc.follows
    ? "Your team reads the copy straight from S3, in their own DuckDB. Nothing is downloaded, and each new session reads the newest snapshot."
    : "Your team reads the copy straight from S3, in their own DuckDB. This file stays on the snapshot below: download it again to see a newer one."));
  box.append(el("p", { class: "use-note" }, "Newest snapshot on S3: ",
    el("b", { text: doc.snapshot, title: utcLocalTitle(doc.snapshot) || null }), " UTC, " + dashTablesWord(doc.tables || 0) + "."));
  if (doc.newer_local) box.append(hint("A newer snapshot, from " + doc.newer_local + " UTC, is on this machine and not on S3 yet. The file sees it once it is uploaded."));
  // The one primary action. The file is the answer already in hand, so the
  // click saves it and asks the server nothing.
  const dl = el("button", { class: "btn btn-primary", type: "button", text: "Download " + DUCKDB_VIEWS_FILE,
    onclick: () => downloadBlob(DUCKDB_VIEWS_FILE, doc.views_sql || "", "text/plain") });
  box.append(el("div", { class: "use-acts" }, dl));
  box.append(hint("Then open it in DuckDB:"), duckdbCommandLine(DUCKDB_VIEWS_FILE, DASH_DB));
  box.append(hint("In a DuckDB session already open: ", el("code", { text: ".read " + DUCKDB_VIEWS_FILE }), "."));
  // Every session, not once: the S3 secret and the snapshot the views read
  // live in the session, and a database file keeps neither.
  box.append(hint("Run the file at the start of every session: it sets up S3 for that session" + (doc.follows ? " and picks the newest snapshot" : "") + ". In Metabase, paste it into Init SQL."));
  const needs = el("ul", { class: "form-hint" });
  needs.append(el("li", null, "Read access to ", el("code", { text: doc.s3 }), "."));
  needs.append(el("li", { text: doc.region
    ? "The bucket's region, " + doc.region + ". The file already names it."
    : "Their AWS setup naming the bucket's region." }));
  if (doc.endpoint) needs.append(el("li", { text: "Network access to " + doc.endpoint + ", where the bucket lives." }));
  needs.append(el("li", { text: "AWS credentials on their own machine, found the usual way: environment, ~/.aws, or SSO. The file holds none, and DBTrail never hands out its own." }));
  box.append(el("p", { class: "use-note", text: "What each reader needs:" }), needs);
  const left = doc.left_out_tables || [];
  const more = doc.left_out_tables_omitted || 0;
  if (left.length || more) {
    const n = left.length + more;
    box.append(el("p", { class: "use-note", text: (n === 1 ? "1 table is" : n + " tables are") + " not in this snapshot, so not in the file:" }));
    const ul = el("ul", { class: "form-hint" });
    for (const t of left) ul.append(el("li", null, el("code", { text: t.name }), t.reason ? ": " + t.reason : ""));
    if (more) ul.append(el("li", { text: "and " + more + " more" }));
    box.append(ul);
  }
  box.append(hint("The list of tables comes from this snapshot. Download the file again after a table is added or dropped."),
    docsMore("guides/dashboards", "", "dashboards on the copy"));
  return box;
}

function useCopySection() {
  const section = el("section", { class: "use", "aria-label": "Use your copy" });
  // Hidden until the flow's reads say a copy exists: before the first copy
  // nothing is "ready to query", and the Getting started list is the page's
  // one task. The drawing's own links carry the setup until then.
  section.hidden = true;
  const head = el("div", { class: "use-h" });
  const lead = el("p", { class: "use-lead" });
  const leadText = (n) => (n ? "Your " + n + (n === 1 ? " table" : " tables") : "Your tables") + ", ready to query. Now or fast? Always current or offline? Each card says.";
  lead.textContent = leadText(0);
  head.append(el("div", { class: "use-h-text" }, el("h2", { class: "use-title", text: "Use your copy" }), lead));
  head.append(el("a", { class: "use-claude", href: "/connect", text: "Connect Claude ›",
    onclick: (e) => { e.preventDefault(); navigate("connect"); } }));
  section.append(head);
  const grid = el("div", { class: "use-cards" });
  const panel = el("div", { class: "use-panel" });
  panel.hidden = true;
  const state = { open: "", snap: null, cta: "none", viewsSQL: false, cards: {}, buttons: {} };
  const mark = () => {
    for (const [id, card] of Object.entries(state.cards)) {
      card.className = "use-card" + (id === state.open ? " on" : "");
      state.buttons[id].setAttribute("aria-expanded", id === state.open ? "true" : "false");
    }
  };
  const close = () => { state.open = ""; panel.hidden = true; clear(panel); mark(); };
  const open = (id) => {
    if (state.open === id) { close(); return; }
    state.open = id;
    mark();
    const def = USE_CARDS.find((c) => c.id === id);
    clear(panel);
    const title = el("b", { class: "use-panel-title", text: def.title, tabindex: "-1" });
    panel.append(el("div", { class: "use-panel-h" }, title,
      el("button", { class: "btn btn-sm btn-ghost use-panel-x", type: "button", text: "Close", onclick: close })));
    const body = el("div", { class: "use-panel-body" });
    panel.append(body);
    USE_PANELS[id](body, state);
    panel.hidden = false;
    title.focus();
  };
  USE_CARDS.forEach((def) => {
    const card = el("div", { class: "use-card", "data-use": def.id });
    const art = el("div", { class: "use-art use-art-" + def.id, "aria-hidden": "true" });
    if (USE_ART[def.id]) art.append(svgEl(USE_ART[def.id]));
    card.append(art);
    card.append(el("h3", { class: "use-card-t", text: def.title }));
    const tags = el("div", { class: "use-tags" });
    def.tags.forEach(([t, yes]) => tags.append(el("span", { class: "use-tag" + (yes ? " y" : ""), text: t })));
    card.append(tags);
    card.append(el("p", { class: "use-line" }, ...def.line()));
    const btn = el("button", { class: "btn use-act" + (def.primary ? " btn-primary" : ""), type: "button", text: def.action, "aria-expanded": "false",
      onclick: (e) => { e.stopPropagation(); open(def.id); } });
    card.append(btn);
    card.addEventListener("click", () => open(def.id));
    if (def.cap && !capsCache[def.cap]) card.hidden = true;
    state.cards[def.id] = card;
    state.buttons[def.id] = btn;
    grid.append(card);
  });
  section.append(grid, panel);
  const update = (model, inp) => {
    const unc = (inp && inp.uncaptured) || {};
    lead.textContent = leadText(typeof unc.tables_captured === "number" ? unc.tables_captured : 0);
    const bl = (inp && inp.baselines) || {};
    state.snap = (bl.snapshots || [])[0] || null;
    state.cta = (model && model.cta) || "none";
    state.viewsSQL = !!(model && model.viewsSQL);
    section.hidden = state.cta === "setup";
    // The SQL card follows the capability of the server on screen.
    state.cards.sql.hidden = !capsCache.sql;
    // capsCache is read when the server is picked, and the capability turns
    // on only once a local copy exists: a server picked before its first copy
    // kept the card hidden until a reload. So when this refresh sees a newest
    // copy the capabilities were not asked about yet, ask again, once per
    // copy (a server whose SQL stays off for another reason is not asked on
    // every refresh). A failed ask is not remembered, so the next refresh
    // retries; an answer for a server no longer on screen is dropped.
    const snapAt = state.snap && state.snap.time;
    if (!capsCache.sql && snapAt && snapAt !== state.sqlAskedAt && !state.sqlAsking) {
      const gen = serverGen;
      state.sqlAsking = true;
      api("/api/capabilities").then((caps) => {
        state.sqlAsking = false;
        if (gen !== serverGen) return;
        state.sqlAskedAt = snapAt;
        if (caps && caps.sql) {
          capsCache.sql = true;
          state.cards.sql.hidden = false;
        }
      }, () => { state.sqlAsking = false; });
    }
  };
  return { section, update };
}

function renderOverview() {
  const gen = serverGen;
  const f = ovFrame();
  // Asked of the page, not of the address (#1797): this paint's heading is
  // still on screen, for the same server. A server switch or another page
  // drops a late payload instead of painting it over the new view, and stops
  // this paint's loops.
  const live = () => gen === serverGen && f.head === ovHead && overviewOnScreen();
  // Independent fetches; each fills its card as it lands. No Promise.all:
  // the slowest aggregate must not hold the frame or its siblings hostage —
  // the #1352 target (p95 < 5 s to a useful first paint) is about the PAINT,
  // not the backend.
  let firstRun = null;
  if (serversEmpty && capsCache.monitor && sessionMay("servers:write")) f.firstRunSlot.append(addServerCard());
  else firstRun = watchFirstRun(f, live);

  api("/api/status").catch(() => null)
    .then((status) => { if (live()) fillOvStatus(f, status); });
  // Only the Recent-changes list needs event ROWS, and it renders 8 of them.
  // The tiles' counts come from /api/activity (#1300), so this page never
  // pulls row images to derive four integers. The list is read by the loop
  // that keeps the page current, which asks first for the newest event id:
  // read in that order, the list holds at least everything that id counts,
  // and the next change moves the id.
  watchOverview(f, live, firstRun);
  // A failed fetch must render the same red "unavailable" card the nil-db
  // path gets — a swallowed null would make a broken endpoint
  // indistinguishable from a console without the feature.
  const coverageP = api("/api/coverage").catch((err) => { console.error("coverage fetch failed", err); return { continuity: "unavailable" }; });
  coverageP.then((coverage) => { if (live()) fillOvCoverage(f, coverage); });
  loadOvFlow(f, live, coverageP).catch((err) => console.error("flow paint failed", err));
  loadOvUncaptured(f, live);
  // null on failure, never {} — the fill renders "—" for a missing aggregate.
  // A zero-filled fallback would print "0 deletes", an assurance nobody
  // measured. No period parameter: the server derives the window from the
  // live retention (#1352) and names it in the payload's label.
  api("/api/activity").catch((err) => { console.error("activity fetch failed", err); return null; })
    .then((activity) => { if (live()) fillOvActivity(f, activity); });
}

// addServerCard is the first step on a console that lists no server yet
// (#1779). The installer and the guide both say to add one, and the page was a
// dashboard of zeros with the form two clicks away, behind Manage servers.
// The button opens that same dialog with the add form already open. Shown
// only where a server can be monitored from here, like the sidebar note: a
// read-only console cannot start capturing a new source.
// ADD_FIRST_ART: the path with its first station not added yet: a dashed
// database, a dashed wire, DBTrail and the copy (static, so svgEl is right).
const ADD_FIRST_ART = `<svg viewBox="0 0 200 52" aria-hidden="true"><ellipse cx="24" cy="12" rx="18" ry="6" fill="none" stroke="var(--ink-4)" stroke-width="1.5" stroke-dasharray="4 3"/><path d="M6 12v26c0 3.3 8 6 18 6s18-2.7 18-6V12" fill="none" stroke="var(--ink-4)" stroke-width="1.5" stroke-dasharray="4 3"/><path d="M50 28h26" stroke="var(--ink-4)" stroke-width="2" stroke-dasharray="4 4"/><rect x="84" y="10" width="44" height="36" rx="9" fill="var(--pink-tint)" stroke="var(--pink-mid)" stroke-width="1.5"/><path d="M96 34l8-8 6 5 8-9" fill="none" stroke="var(--pink-mid)" stroke-width="2"/><path d="M136 28h22" stroke="var(--ink-4)" stroke-width="2"/><rect x="164" y="12" width="32" height="32" rx="8" fill="var(--mint-tint)" stroke="var(--ok)" stroke-width="1.5"/></svg>`;

function addServerCard() {
  const card = el("section", { class: "ov-panel fr-card add-first" });
  card.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title" }, el("span", { class: "tag-pill", text: "Getting started" }))));
  // What is missing, drawn (#1950): your database, dashed, then DBTrail and
  // the copy. The sentence stays: it says what Add server will do.
  card.append(el("div", { class: "empty-art", "aria-hidden": "true" }, svgEl(ADD_FIRST_ART)));
  card.append(el("p", { class: "add-first-lead",
    text: "Add the database you want to protect. DBTrail checks that it is ready, sets up its index and starts capturing its changes." }));
  card.append(el("button", { class: "btn btn-primary", type: "button", id: "ov-add-server", text: "+ Add server",
    onclick: () => { openServersModal(); showServerForm(null); } }));
  return card;
}

// firstRunCard draws the steps from adding a server to its first indexed
// change (#1606) as GET /api/servers/{id}/first-run computes them, or nothing
// once the list is complete. Waiting is its own mark, so a step that has not
// started never looks like one that failed.
//
// id and onStarted are for the snapshot step's own button: the server the
// snapshot is taken of, and what to do once the run is accepted (ask for the
// list again, which then shows the step running).
function firstRunCard(rep, id, onStarted) {
  if (!rep || rep.complete || !Array.isArray(rep.steps)) return null;
  const marks = { done: "✓", running: "…", waiting: "○", failed: "✗" };
  const card = el("section", { class: "ov-panel fr-card" });
  card.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title" }, el("span", { class: "tag-pill", text: "Getting started" }))));
  // One step is shown open: the one that needs the operator (failed or
  // running, else the first not done). The others stay on the page, folded,
  // so the card asks for one thing at a time instead of a list of six.
  const stateOf = (s) => (marks[s.state] ? s.state : "waiting");
  let cur = rep.steps.findIndex((s) => stateOf(s) === "failed");
  if (cur < 0) cur = rep.steps.findIndex((s) => stateOf(s) === "running");
  if (cur < 0) cur = rep.steps.findIndex((s) => stateOf(s) !== "done");
  const list = el("ol", { class: "fr-steps" });
  rep.steps.forEach((s, i) => {
    const state = stateOf(s);
    const body = el("div", { class: "dc-body" }, el("div", { class: "dc-name", text: s.name }));
    const known = s.snapshot_failed && s.failure && ((s.failure.kind === "missing_permission" && s.failure.grant) ||
      (s.failure.kind === "mydumper_too_old" && s.failure.min_version));
    // The button is named only when this session could press it, for a run
    // a person started; otherwise the next try is the schedule's.
    const pressable = !!capsCache.baseline_trigger && sessionMay(PERM_SNAPSHOT_CREATE) && !(s.failure && s.failure.scheduled);
    // A capture failure the daemon names by code is said in the Overview
    // card's words, the last of which is the fix, with the error as reported
    // in a fold. The server's generic fix would send the operator to Start.
    const sameId = state === "failed" && s.error_code === "same_replication_id";
    if (s.snapshot_failed) body.append(snapshotFailureCard(s.failure, s.detail || "", pressable ? "overview" : "scheduled", s.note || ""));
    else if (sameId) {
      const lines = sameReplicationIdLines(!!s.retrying);
      lines.slice(0, -1).forEach((l) => body.append(el("div", { class: "fr-detail", text: l })));
      body.append(el("div", { class: "fr-fix", text: lines[lines.length - 1] }, " ",
        el("a", { class: "fr-go", href: "#servers", text: "Open Servers ›", onclick: (e) => { e.preventDefault(); openServersModal(); } })));
      if (s.detail) body.append(rawErrorFold(s.detail));
    } else if (s.detail) body.append(el("div", { class: "fr-detail", text: s.detail }));
    // A card that names its fix, or names the schedule as the next try,
    // already says where to try again.
    if (s.fix && !known && !sameId && !(s.snapshot_failed && !pressable)) {
      // A fix that names a page carries the way there: "press Start in
      // Servers" opens the Servers dialog, "on the Snapshots page" goes to
      // that page. The sentence itself is the server's and stays as sent.
      const fix = el("div", { class: "fr-fix", text: s.fix });
      if (/\bServers\b/.test(s.fix)) {
        fix.append(" ", el("a", { class: "fr-go", href: "#servers", text: "Open Servers ›",
          onclick: (e) => { e.preventDefault(); openServersModal(); } }));
      } else if (/\bunder Set at startup\b/.test(s.fix)) {
        fix.append(" ", el("a", { class: "fr-go", href: DOCS_BASE + "settings/backups/#set-at-startup",
          target: "_blank", rel: "noopener", text: "Read the docs ›" }));
      } else if (/\bthe Snapshots page\b/.test(s.fix)) {
        fix.append(" ", el("a", { class: "fr-go", href: "/snapshots", text: "Open Snapshots ›",
          onclick: (e) => { e.preventDefault(); navigate("snapshots"); } }));
      }
      body.append(fix);
      // The step that only waits for someone to take the snapshot carries
      // the button itself. The server sends this fix only when a snapshot
      // can be written (blockedBackupStep covers the rest), and the button
      // asks the same question as the one on the Snapshots page.
      if (id && pressable && state === "waiting" && /^Create one on the Snapshots page/.test(s.fix)) {
        const btn = el("button", { class: "btn btn-primary fr-snap", type: "button", text: "Take snapshot now" });
        btn.onclick = () => { if (typeof window.confirm === "function" && window.confirm(READ_DB_CONFIRM)) createBaseline(id, btn, onStarted); };
        body.append(btn);
      }
    }
    list.append(el("li", { class: "fr-step " + state + (i === cur ? " fr-cur" : "") }, el("span", { class: "dc-mark", text: marks[state], "aria-label": state }), body));
  });
  card.append(list);
  if (rep.steps.length > 1) {
    const done = rep.steps.filter((s) => stateOf(s) === "done").length;
    const toggle = el("button", { class: "fr-fold", type: "button", "aria-expanded": "false",
      text: "All " + rep.steps.length + " steps · " + done + " done" });
    toggle.onclick = () => { const open = card.classList.toggle("fr-open"); toggle.setAttribute("aria-expanded", open ? "true" : "false"); };
    card.append(toggle);
  }
  return card;
}

// watchFirstRun shows the first-run steps at the top of the Overview until a
// snapshot exists for the selected server (#1801; the server decides, see
// firstRunSteps), and polls while it shows. When the list completes it leaves
// and the page is NOT rendered again: the Overview keeps its own numbers
// current (watchOverview), and the list used to end at the first change,
// which is also where the page stopped updating. Only a supervisor console
// answers the endpoint: a server with no source, the command-line server, a
// read-only console or a session without the permission answer with an error
// that stops the loop and draws nothing. Any other failure (a 502, a network
// blip, an index that could not be read) tries again, so a first request that
// fails does not hide the list for good. A list already up stays, with a note
// that it could not be refreshed: right away when the index could not be
// read, after three failed requests in a row otherwise. The wait between
// requests goes 3, 6, 12, then 15 seconds, and back to 3 when the list
// changes. A hidden tab asks nothing.
//
// A list nothing on it can change by itself waits two minutes between asks
// instead of fifteen seconds (firstRunSettled): every capture step done, and
// the backup step waiting for a backup nobody here is running. Each ask reads
// that server's backup locations, which for an S3 one is a listing over the
// network, and a server whose operator never takes a backup would ask for as
// long as the page is open. Rarely rather than never, because a backup can
// appear from somewhere this page cannot see (the schedule, the command line,
// another window), and a list that then kept saying a backup is owed would be
// exactly the stale claim this change is about. A change landing and the tab
// coming back still ask at once.
//
// Returns { poke }, which asks again now, or null when nothing is watched.
function watchFirstRun(f, live) {
  const id = currentServer || defaultServerId;
  if (!capsCache.monitor || !id) return null;
  let shown = false, last = "", delay = 3000, failures = 0;
  // armed names the one timer that may still run: a poke replaces the wait
  // instead of starting a second loop beside it. stopped is final (the list
  // is done, or the server refuses it); settled still answers a poke.
  let armed = 0, busy = false, stopped = false, settled = false, pokeAfter = false;
  const arm = (ms) => { const t = ++armed; setTimeout(() => { if (t === armed) tick(); }, ms); };
  const again = () => {
    if (pokeAfter) { pokeAfter = false; delay = 3000; arm(0); return; }
    arm(delay); delay = Math.min(delay * 2, 15000);
  };
  const stale = (msg) => {
    if (!shown) return;
    const card = f.firstRunSlot.children[0];
    clear(f.firstRunSlot);
    f.firstRunSlot.append(card, el("div", { class: "warn-item fr-stale" }, icon("warn"),
      el("span", { text: "Could not refresh this list: " + msg })));
  };
  const tick = () => {
    if (stopped || !live()) return;
    if (document.hidden) { arm(OV_HIDDEN_MS); return; }
    busy = true;
    apiWithin("/api/servers/" + encodeURIComponent(id) + "/first-run", OV_REQUEST_MS).then((rep) => {
      busy = false;
      if (!live()) return;
      if (rep && rep.check_error) { stale(rep.check_error); again(); return; }
      failures = 0;
      const card = firstRunCard(rep, id, () => { if (stopped || !live()) return; delay = 3000; arm(0); });
      if (!card) { stopped = true; clear(f.firstRunSlot); return; }
      const key = JSON.stringify(rep);
      if (key !== last) delay = 3000;
      last = key;
      clear(f.firstRunSlot);
      f.firstRunSlot.append(card);
      shown = true;
      settled = firstRunSettled(rep);
      if (settled) arm(OV_SETTLED_MS);
      else again();
    }, (err) => {
      busy = false;
      if (!live()) return;
      if ([401, 403, 404, 409].includes(err && err.status)) { stopped = true; return; }
      if (++failures >= 3) stale((err && err.message) || String(err));
      again();
    });
  };
  tick();
  return {
    poke(reason) {
      if (stopped || !live()) return;
      // A settled list is waiting for a backup, and a change cannot produce
      // one: on a server being written to steadily, taking every change
      // would put this endpoint (and its reading of the backup locations)
      // straight back to one ask per change. The tab coming back is a person
      // returning, so it asks; arm() replaces the pending wait rather than
      // adding one, so repeated shows collapse into a single ask.
      if (settled) { if (reason === "wake") arm(1000); return; }
      if (busy) { pokeAfter = true; return; }
      // settled is not cleared here: every answer recomputes it, and a
      // poke's own answer is the next one.
      delay = 3000;
      armed++;
      tick();
    },
  };
}

// firstRunSettled: nothing on this list can change without something else
// happening first. Every step but the last is done, and the last one is
// waiting: a step that is running or failed does change on its own (the first
// change arrives, a stream reconnects, a backup this console started ends),
// so those keep the list asking.
function firstRunSettled(rep) {
  const steps = (rep && rep.steps) || [];
  if (!steps.length) return false;
  return steps.every((s, i) => (i === steps.length - 1 ? s.state === "waiting" : s.state === "done"));
}

// ── Overview: keeping itself current (#1801) ─────────────────────────────────
// The Overview used to check for news only while the Getting started list
// showed. That list ended at the first change, so the first INSERT appeared
// on its own and the UPDATE and DELETE after it never did until a reload.

// ovHead is the Overview's heading from its latest paint. The page's loops
// run while it is on screen, asked of the page and not of the address, the
// same question backupsOnScreen asks (#1797).
let ovHead = null;
function overviewOnScreen() { return !!(ovHead && ovHead.isConnected); }

const OV_TICK_MS = 5000;      // how often a visible Overview asks whether anything changed
const OV_HIDDEN_MS = 30000;   // a hidden tab asks nothing; it checks again this often whether it is shown
// The two expensive reads (a count over the whole index, and a coverage card
// that lists every backup location) are rate-limited to OV_SLOW_MS while
// changes keep arriving, and run on their own only every OV_IDLE_SLOW_MS: on
// a server nobody is writing to, once a minute is 480 of each overnight per
// open tab for numbers that did not move. The idle interval matches the five
// minutes after which capture counts as stalled, so that still surfaces, and
// the card prints the time it was read.
const OV_SLOW_MS = 60000;
const OV_IDLE_SLOW_MS = 300000;
const OV_RETRY_MAX_MS = 30000;
// How long a first-run list waiting only for a backup waits between asks.
const OV_SETTLED_MS = 120000;

// ovLive is the loop of the Overview on screen, so the tab coming back can
// wake it at once instead of at its next turn.
let ovLive = null;
function ovVisibilityChanged() { if (!document.hidden && ovLive) ovLive.wake(); }

// watchOverview keeps a painted Overview current. Every 5 s it asks for the
// newest event id (GET /api/events/head, no row data), and only when that
// moved does it read the Recent changes list and the window counts again, so
// an Overview left open does not re-read, and audit, the same eight rows
// every five seconds. Coverage and the all-time count are read again when
// the first change lands on an empty index and otherwise at most once a
// minute: a profile-restricted session records a refusal on every coverage
// read, and the coverage card lists every backup location. A hidden tab asks
// nothing. Three failures in a row say on the page since when it has not
// updated, and it keeps trying, more slowly; a refusal (403, 404) says it
// stopped and stops. The first turn runs at once and reads the list after
// the id, which is how the paint gets its Recent changes.
function watchOverview(f, live, firstRun) {
  const me = {};
  ovLive = me;
  // head: the newest event id whose list is on screen. drawn: the list was
  // drawn at least once, from data or with the reason it could not be read.
  let head = null, drawn = false, armed = 0, busy = false, stopped = false, failures = 0;
  let delay = OV_TICK_MS, slowAt = Date.now() + OV_SLOW_MS, idleSlowAt = Date.now() + OV_IDLE_SLOW_MS;
  let okAt = nowClock(), sideOkAt = nowClock();
  const on = () => !stopped && ovLive === me && live();
  const arm = (ms) => { const t = ++armed; setTimeout(() => { if (t === armed) tick(); }, ms); };
  const note = (msg) => {
    clear(f.liveSlot);
    if (msg) f.liveSlot.append(el("div", { class: "warn-item ov-live-note" }, icon("warn"), el("span", { text: msg })));
  };
  // The tiles and the table activity have reads of their own, and they fail
  // on their own: the rows can be arriving while the counts are not. Their
  // last good figures stay on screen, and this says they are from before,
  // the way the coverage card does. Without it the all-time total sat there
  // labelled as current for as long as those reads kept failing.
  const sideNote = (msg) => {
    clear(f.sideSlot);
    if (msg) f.sideSlot.append(el("div", { class: "warn-item ov-side-note" }, icon("warn"),
      el("span", { text: "The counts above could not be refreshed: " + msg + ". They are the ones read at " + sideOkAt + "." })));
  };
  const failed = (err) => {
    const msg = (err && err.message) || String(err);
    if (err && err.status === 401) { stopped = true; return; } // the sign-in gate is up
    if (err && (err.status === 403 || err.status === 404)) {
      stopped = true;
      note("The Overview stopped updating: " + msg + ". Reload the page to try again.");
      return;
    }
    // Only once something has been drawn: before that the panel carries the
    // reason itself, and during setup the index does not exist yet, so a
    // "has not updated since" over a list whose first step reads "Creating
    // it now" would be two voices for one state.
    if (++failures >= 3 && drawn) note("The Overview has not updated since " + okAt + ": " + msg + ". Trying again.");
    delay = Math.min(delay * 2, OV_RETRY_MAX_MS);
  };
  // woke: the tab was just shown again. The Getting started list is asked
  // again once per turn, after the id, whether the turn was a wake or a
  // change: asking for both at once sent two identical requests.
  const pull = async (woke) => {
    busy = true;
    // What the Getting started list is asked for at the end of the turn: a
    // change, or the tab coming back. A list waiting only for a backup takes
    // the second and not the first (watchFirstRun).
    let askList = woke ? "wake" : "";
    try {
      let next, headErr = null;
      try { next = (await apiWithin("/api/events/head", OV_REQUEST_MS)).newest_event_id; } catch (err) { headErr = err; }
      if (!on()) return;
      if (headErr && drawn) { failed(headErr); return; }
      const prev = head;
      // Until the list is drawn it is read whatever the id said, so the
      // panel shows the rows or why they could not be read, as the paint
      // always did.
      if (headErr || next !== head) {
        let d;
        try { d = await apiWithin("/api/events?limit=8&order=DESC", OV_REQUEST_MS); } catch (err) {
          if (!on()) return;
          console.error("events fetch failed", err);
          // A later failure keeps the rows on screen and counts as a failed
          // refresh.
          if (!drawn) fillOvEvents(f, null, err);
          failed(headErr || err);
          return;
        }
        if (!on()) return;
        fillOvEvents(f, d, null);
        drawn = true;
        if (headErr) { failed(headErr); return; }
        head = next;
      }
      failures = 0; delay = OV_TICK_MS; okAt = nowClock();
      note("");
      // The paint read everything else a moment ago.
      if (prev === null) return;
      const changed = next !== prev;
      const now = Date.now();
      if (changed) {
        askList = "change";
        apiWithin("/api/activity", OV_REQUEST_MS).then((a) => {
          if (!on()) return;
          if (a) { fillOvActivity(f, a); sideOkAt = nowClock(); sideNote(""); }
        }, (err) => { console.error("activity refresh failed", err); if (on()) sideNote((err && err.message) || String(err)); });
      }
      // The two expensive reads: on the first change to an index that had
      // none (the window goes from "nothing to restore" to a real one), and
      // otherwise rate-limited while changes arrive, on a long interval when
      // nothing is.
      if ((changed && !prev) || (changed && now >= slowAt) || now >= idleSlowAt) {
        slowAt = now + OV_SLOW_MS;
        idleSlowAt = now + OV_IDLE_SLOW_MS;
        apiWithin("/api/status", OV_REQUEST_MS).then((st) => {
          if (!on()) return;
          if (st) { fillOvStatus(f, st); sideOkAt = nowClock(); sideNote(""); }
        }, (err) => { console.error("status refresh failed", err); if (on()) sideNote((err && err.message) || String(err)); });
        const coverageP = apiWithin("/api/coverage", OV_REQUEST_MS);
        coverageP.then((c) => { if (on()) fillOvCoverage(f, c); }, (err) => {
          console.error("coverage refresh failed", err);
          // The card keeps its numbers and says they are from before, the
          // way its own refresh button does. From THIS paint's fill, never
          // the module-wide one, which belongs to whichever Overview filled
          // it last.
          if (on() && f.covLast) { clear(f.covSlot); f.covSlot.append(covCard(f.covLast.data, { at: f.covLast.at, error: true })); }
        });
        // The flow's reads go out beside the coverage read; a failed one
        // leaves the flow as it was (null paints nothing).
        loadOvFlow(f, on, coverageP.then((c) => c, () => null)).catch((err) => console.error("flow refresh failed", err));
      }
    } finally {
      busy = false;
      if (on() && askList && firstRun) firstRun.poke(askList === "wake" ? "wake" : "change");
      if (on()) arm(document.hidden ? OV_HIDDEN_MS : delay);
    }
  };
  const tick = () => {
    if (!on()) return;
    if (document.hidden) { arm(OV_HIDDEN_MS); return; }
    ovTickAges(f.flowSlot);
    pull();
  };
  me.wake = () => {
    if (!on()) return;
    // A turn already on its way ends by arming the next one; only the list,
    // which a hidden tab left waiting, is asked now.
    if (busy) { if (firstRun) firstRun.poke("wake"); return; }
    armed++;
    pull(true);
  };
  pull();
  return me;
}

// ── Overview: tables left out of capture (#1802) ─────────────────────────────
// A schema read after capture started leaves out a table it cannot key (no
// primary key, not InnoDB), and before #1802 only the capture log said so. The
// server builds every sentence and statement (status.TableCapture.View), so
// this block only draws them: the page, `bintrail status` and the MCP status
// tool cannot word the same table two ways.

// Rows shown before the rest fold away: enough to read, never a wall.
const UNCAP_SHOWN = 3;

// loadOvUncaptured fetches the list and draws it. A request that FAILS says so
// on screen, in the same words a read that failed inside the endpoint gets:
// drawing nothing would rebuild, one layer up, the silence this whole section
// exists to end, and the page around it would look healthy while nobody knows
// which tables are captured. Same rule as the coverage card above.
function loadOvUncaptured(f, live) {
  api("/api/uncaptured-tables").then(
    (v) => { if (live()) fillOvUncaptured(f, v); },
    (err) => {
      console.error("uncaptured tables fetch failed", err);
      if (live()) fillOvUncaptured(f, { state: "unavailable", error: (err && err.message) || String(err), uncaptured: [] });
    });
}

function fillOvUncaptured(f, v) {
  clear(f.uncapSlot);
  const card = uncapturedCard(v);
  if (card) f.uncapSlot.append(card);
}

// uncapturedCard draws the count and one row per table left out, or null when
// there is nothing to say (no schema read yet, a PostgreSQL server, a state
// this page does not know). It never draws "every table is captured" from
// something it could not read: "not checked" and "unavailable" say so.
function uncapturedCard(v) {
  if (!v) return null;
  const card = el("section", { class: "ov-panel uncap-card" });
  const head = (extra) => card.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Table coverage" }), extra || null));
  if (v.state === "unavailable") {
    card.append(el("div", { class: "warn-item" }, icon("warn"),
      el("span", { text: "Could not check which tables are captured: " + (v.error || "unknown error") })));
    return card;
  }
  if (v.state !== "checked" && v.state !== "not_checked") return null;
  head();
  // The count, or why there is none. Never both, and never a count nobody
  // could verify: with the capture scope unknown the note takes its place.
  if (v.headline || v.coverage_note) {
    card.append(el("p", { class: "uncap-lead", text: v.headline || v.coverage_note }));
  }
  // The line that answers "and nothing else?" — the same one the terminal
  // prints, from the same field, so the two cannot answer differently.
  if (v.all_captured_note) card.append(el("p", { class: "uncap-note", text: v.all_captured_note }));
  if (v.state === "not_checked") {
    // NOT "an older version": a current build that has not migrated this
    // server's data leaves the same shape, and the console never migrates a
    // server it did not start from the command line.
    card.append(el("p", { class: "uncap-note",
      text: "Whether any table is left out is not known: this server has not recorded it. The first schema read that leaves one out records it, and this card names it from then on." }));
    return card;
  }
  const rows = (v.uncaptured || []).map(uncapturedRow);
  // The lines that say the list is INCOMPLETE never go inside the fold: they
  // are the reason to open it, and with more than a few tables they were the
  // ones being hidden.
  const notices = [];
  if (v.omitted_summary) notices.push(el("div", { class: "uncap needs-decision" }, el("span", { class: "uncap-text", text: v.omitted_summary })));
  if (v.withheld_summary) notices.push(el("div", { class: "uncap needs-decision" }, el("span", { class: "uncap-text", text: v.withheld_summary })));
  if (!rows.length && !notices.length) return card;
  const list = el("div", { class: "uncap-list" });
  list.append(...rows.slice(0, UNCAP_SHOWN));
  if (rows.length > UNCAP_SHOWN) {
    const more = el("details", { class: "uncap-more" },
      el("summary", { text: "Show " + (rows.length - UNCAP_SHOWN) + " more" }));
    more.append(...rows.slice(UNCAP_SHOWN));
    list.append(more);
  }
  list.append(...notices);
  card.append(list);
  return card;
}

// uncapturedRow draws one table as a dashed chip: the dash says it is not
// captured, the color whether someone still has to decide (red) or already
// did (gray, once "Leave it out" records a decision, #1805). The fix starts
// folded; Copy copies exactly the statement on screen.
function uncapturedRow(t) {
  const tone = t.decision ? "decided" : "needs-decision";
  const row = el("div", { class: "uncap " + tone });
  const line = el("div", { class: "uncap-line" }, el("span", { class: "uncap-text", text: t.summary }));
  row.append(line);
  if (!t.fix_sql) {
    // No statement to hand over (a table whose columns nothing recorded, so
    // a guessed column name could collide). The words still carry what
    // happens to the data and the step that produces a statement.
    if (t.fix_intro) row.append(el("p", { class: "uncap-intro", text: t.fix_intro }));
    return row;
  }
  const fix = el("div", { class: "uncap-fix" },
    el("p", { class: "uncap-intro", text: t.fix_intro || "" }),
    el("pre", { class: "uncap-sql", text: t.fix_sql }),
    el("button", { class: "btn btn-sm", type: "button", text: "Copy",
      onclick: (e) => { e.stopPropagation(); copyText(t.fix_sql, "SQL"); } }));
  fix.hidden = true;
  const toggle = el("button", { class: "btn btn-sm btn-ghost uncap-toggle", type: "button", text: "Show fix",
    "aria-expanded": "false",
    onclick: (e) => {
      e.stopPropagation();
      fix.hidden = !fix.hidden;
      toggle.textContent = fix.hidden ? "Show fix" : "Hide fix";
      toggle.setAttribute("aria-expanded", fix.hidden ? "false" : "true");
    } });
  line.append(toggle);
  row.append(fix);
  return row;
}

// buildOverview renders the dashboard from already-fetched payloads — the
// composition seam the e2e fixture drives directly, sharing every fill with
// the progressive path above. status and activity may each be null (their
// fetches are best-effort); when one is, the tiles it feeds read "—". Every
// tile carries its OWN scope line, because these numbers get screenshotted
// into incident channels without the page around them (#1300): "N deletes"
// beside "N changes indexed" invites reading the first as a share of the
// second, and before this they were different denominators.
function buildOverview(status, eventsData, coverage, activity, uncaptured) {
  const f = ovFrame();
  fillOvStatus(f, status);
  fillOvEvents(f, eventsData, null);
  fillOvCoverage(f, coverage);
  fillOvActivity(f, activity);
  fillOvUncaptured(f, uncaptured || null);
}

// ovStat renders one tile. scope is REQUIRED for any tile carrying a number:
// the tiles are visually identical, so without it a reader cannot tell an
// all-time total from a window count — which is precisely how "53 deletes" got
// read as a share of "3121 changes indexed" (#1300).
function ovStat(value, key, mod, scope) {
  // Tiles animate their ARRIVAL (a rise, in style.css), never their VALUE. A
  // count-up was tried and removed: it writes intermediate numbers into the
  // DOM, so for the length of the animation the tile states something untrue —
  // the console-e2e read "0" from the deletes tile mid-flight. A forensics
  // surface that briefly reports zero deletions is a small lie, and the
  // entrance animation already supplies the sense of arrival without one.
  // The brand gradient (#1385) is opt-in per tile, and this is the only place
  // that grants it. `mod` already names the exact set that must not have it,
  // so the gate needs no list of its own: "danger" paints the deletes count in
  // the semantic --delete, and the `small` variant is 19px at weight 500 —
  // below WCAG's large-text bar, which is the only bar the gradient's stops
  // clear. (`small` is built inline elsewhere and has never reached ovStat;
  // gating on `mod` rather than on the name covers it if it ever does.)
  const brand = mod ? " " + mod : " ov-stat-num";
  return el("div", { class: "ov-stat" },
    el("div", { class: "ov-stat-v" + brand, text: value }),
    el("div", { class: "ov-stat-k", text: key }),
    el("div", { class: "ov-stat-scope", text: scope || "" }));
}

function colsSummary(cols, highlight) {
  cols = cols || [];
  if (cols.length > 2) return [el("span", { text: cols.length + " cols" })];
  const out = [];
  cols.forEach((c, i) => {
    if (i) out.push(document.createTextNode(", "));
    out.push(highlight ? el("span", { class: "hl", text: c }) : el("span", { text: c }));
  });
  return out;
}

function ovEventRow(e) {
  const row = el("div", { class: "ov-ev",
    onclick: () => navigate("events", { q: "pk:" + e.pk_values }) });
  row.append(tsSpan("ov-ev-time", e.event_timestamp));
  row.append(badge(e.event_type));
  const tbl = el("span", { class: "ov-ev-tbl", text: e.schema_name + "." + e.table_name + " " });
  tbl.append(el("span", { class: "ov-ev-pk", text: "#" + e.pk_values }));
  row.append(tbl);
  row.append(el("span", { class: "ov-ev-cols" }, ...colsSummary(e.changed_columns, false)));
  const undo = el("button", { class: "btn btn-sm ov-ev-undo", type: "button", text: "Undo", "data-anchor": e.anchor,
    onclick: (ev) => { ev.stopPropagation(); undoEvent(e); } });
  row.append(undo);
  // fillOvEvents moves the focus back to this button after a refresh.
  row.undoButton = undo;
  return row;
}

function ovTableRow(s, winSince) {
  // The click carries the widget's OWN window (#1414): the count was computed
  // over the live retention, so the search it opens states that bound — as a
  // visible since: token, not a hidden field — and the server's window proof
  // (windowSatisfiedLive) can then skip the archive scan that structurally
  // cannot contribute. Spelled RFC3339 (T...Z) because the smart-search
  // tokenizer splits on spaces.
  const q = s.key + (winSince ? " since:" + winSince.replace(" ", "T") + "Z" : "");
  const row = el("a", { class: "ov-tablerow",
    onclick: () => navigate("events", { q }) });
  row.append(el("span", { class: "ov-tname", text: s.key }));
  const bar = el("span", { class: "ov-bar" });
  if (s.insert) bar.append(el("span", { class: "ov-seg i", style: "flex:" + s.insert }));
  if (s.update) bar.append(el("span", { class: "ov-seg u", style: "flex:" + s.update }));
  if (s.delete) bar.append(el("span", { class: "ov-seg d", style: "flex:" + s.delete }));
  row.append(bar);
  row.append(el("span", { class: "ov-total", text: String(s.total) }));
  return row;
}

// ── Events ─────────────────────────────────────────────────────────────────

function renderEvents(params) {
  const v = VIEW(); clear(v);
  v.append(pageHead("Events", el("p", { class: "page-sub", text: "Browse indexed row events with full before / after images." })));

  // Connection-id availability note (#595): PostgreSQL logical replication
  // (pgoutput) carries no backend connection id, so the connection_id column
  // stays empty for PG sources — unlike MySQL, it cannot be recovered upstream
  // at all, so say so here on the Events page rather than leave the empty
  // column an unexplained gap. capsCache.source is resolved before this paints.
  if (capsCache.source === "postgresql") {
    v.append(el("div", { class: "warn-item" }, icon("warn"),
      el("span", { text: "Postgres's replication stream does not say who made each change, so that column stays empty for PostgreSQL sources." })));
  }

  const form = el("form", { id: "ev-form" });
  // ONE toolbar row (#1950): the search field with Filters, then the
  // keyboard hint, the pager and the exports, grouped right. The buttons are
  // type=button, so living inside the form submits nothing.
  const toolbar = el("div", { class: "ev-toolbar" });
  const searchwrap = el("div", { class: "ev-searchwrap" });
  searchwrap.append(icon("search", "ev-search-ic"));
  const search = el("input", { class: "ev-search", id: "ev-search", name: "q",
    autocomplete: "off", spellcheck: "false",
    placeholder: 'Search changes. Try "orders", "type:delete", "pk:1006", "col:email"' });
  if (params && params.q) search.value = params.q;
  searchwrap.append(search);
  const advBtn = el("button", { class: "btn btn-sm ev-advbtn", type: "button", text: "Filters",
    "aria-expanded": "false", "aria-controls": "ev-advanced",
    onclick: () => {
      const a = $("#ev-advanced", VIEW());
      a.toggleAttribute("hidden");
      const on = !a.hasAttribute("hidden");
      advBtn.classList.toggle("on", on);
      advBtn.setAttribute("aria-expanded", on ? "true" : "false");
    } });
  searchwrap.append(advBtn);
  toolbar.append(searchwrap);
  // The keys are data and wear mono; the words beside them do not.
  const bar = el("div", { class: "result-bar" });
  bar.append(el("span", { class: "kbd-hint" },
    el("b", { text: "j" }), "/", el("b", { text: "k" }), " move · ",
    el("b", { text: "↵" }), " expand · ", el("b", { text: "u" }), " undo"));
  // Paging controls (#1297). Disabled rather than hidden so the affordance is
  // discoverable on page 1 — the bug this fixes was an operator having no way
  // to know event 101 was reachable at all.
  bar.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", id: "ev-prev", text: "‹ Newer",
    disabled: "", onclick: () => eventsGoPage(-1) }));
  bar.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", id: "ev-next", text: "Older ›",
    disabled: "", onclick: () => eventsGoPage(1) }));
  // Labels name their SCOPE: these export every match of the current search
  // (up to the server's cap), not the page on screen. See exportEvents.
  bar.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Export JSON",
    title: "Export all matches of this search, not just this page (max 1000 events)",
    onclick: (e) => exportEvents("json", e.target) }));
  bar.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Export CSV",
    title: "Export all matches of this search, not just this page (max 1000 events)",
    onclick: (e) => exportEvents("csv", e.target) }));
  toolbar.append(bar);
  form.append(toolbar);

  // advanced panel
  const adv = el("div", { class: "ev-advanced", id: "ev-advanced", hidden: "" });
  adv.append(fieldSelect("Schema", "schema", "md", true));
  adv.append(fieldSelect("Table", "table", "md", false, true));
  adv.append(fieldInput("PK", "pk", "sm", "1006"));
  adv.append(fieldSelect("Type", "event_type", "sm", false, false, ["", "INSERT", "UPDATE", "DELETE"], "any"));
  adv.append(fieldInput("Changed column", "changed_column", "md", "email"));
  adv.append(fieldDateInput("Since (UTC)", "since", "md", "YYYY-MM-DD HH:MM:SS"));
  adv.append(fieldDateInput("Until (UTC)", "until", "md", "YYYY-MM-DD HH:MM:SS"));
  adv.append(fieldInput("Limit", "limit", "sm", "100"));
  form.append(adv);
  v.append(form);

  // The count line: how many, and what the list was read from. The response
  // `notes` (benign audit facts: the live-index scope, the archive-elision
  // record, #1365) are a quiet chip beside the count with the server's
  // sentence one click away (renderEventNotes), never a full-width line and
  // never the alert component.
  const countline = el("div", { class: "ev-countline" });
  countline.append(el("span", { class: "result-count" }, el("b", { id: "ev-count", text: "…" }), el("span", { id: "ev-count-note", text: " event(s)" })));
  countline.append(el("span", { id: "ev-notes", class: "notes" }));
  v.append(countline);
  // Scope/coverage notices for this result set (#1311). The response has
  // carried a `warnings` array all along and this view dropped it, which meant
  // the default browse -- the exact case a profiled session reads live-index
  // only with no time filter -- computed the right sentence server-side and
  // threw it away at the browser. Above the list on purpose: a caveat about
  // what a result does NOT include is worthless below the result.
  v.append(el("div", { id: "ev-warnings", class: "warnings" }));

  // events list
  const list = el("div", { class: "events", id: "events-list" });
  const head = el("div", { class: "ev-head" });
  ["time (UTC)", "table", "type", "pk", "changed columns"].forEach((h) => head.append(el("span", { text: h })));
  list.append(head);
  list.append(el("div", { id: "ev-rows" }));
  v.append(list);

  // wire search (debounced)
  let t = null;
  const run = () => runEventsQuery(form);
  form.addEventListener("input", () => { clearTimeout(t); t = setTimeout(run, 200); });
  form.addEventListener("change", run);
  form.addEventListener("submit", (e) => { e.preventDefault(); run(); });
  wireSchemaCascade(form);

  populateSchemas(form);
  run();
  viewEnter();
}

function fieldLabel(label, required) {
  const lbl = el("label", { class: "field-label" }, label);
  if (required) lbl.append(el("span", { class: "field-required", title: "Required", text: " *" }));
  return lbl;
}
function fieldInput(label, name, size, placeholder, required) {
  return el("div", { class: "field field--" + size },
    fieldLabel(label, required),
    el("input", { class: "input", name, placeholder: placeholder || "" }));
}
function fieldSelect(label, name, size, isSchema, isTable, options, anyLabel, required) {
  const sel = el("select", { class: "select" + (isSchema ? " schema-select" : "") + (isTable ? " table-select" : ""), name });
  if (options) options.forEach((o) => sel.append(opt(o, o === "" ? (anyLabel || "any") : o)));
  else sel.append(opt("", "any"));
  return el("div", { class: "field field--" + size },
    fieldLabel(label, required), sel);
}

// fieldTableCombo is an input + datalist combobox for a table name (#1364):
// suggestions come from the selected schema's listing (loadTables fills the
// datalist on schema change), but a hand-typed name still submits — recover
// can legitimately target a dropped table whose events are indexed, and a
// closed <select> would make that recovery impossible from the UI. The
// combo-hint span is the brief busy note while a listing loads; field--combo
// anchors it OUT of flow (#1369) so the empty hint reserves no height — under
// .filters' align-items:flex-end it pushed the label+input above the row.
let tableComboSeq = 0;
function fieldTableCombo(label, name, size, placeholder) {
  const listId = "table-combo-list-" + (++tableComboSeq);
  return el("div", { class: "field field--combo field--" + size },
    fieldLabel(label),
    el("input", { class: "input table-combo", name, placeholder: placeholder || "",
      list: listId, autocomplete: "off", spellcheck: "false" }),
    el("datalist", { id: listId }),
    el("span", { class: "combo-hint", "aria-live": "polite" }));
}

// ── date/time picker (calendar + clock) for Since/Until/At filter fields ────
// event_timestamp round-trips through consoleTSFormat under the UTC location
// config.Connect forces on every index-DB connection (internal/config), so
// "today"/"now" here mean UTC today/now — using local Date getters would
// silently offset every filter the picker writes relative to the UTC values
// the events list shows.
const DT_DOW = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];
const DT_MON = ["January", "February", "March", "April", "May", "June", "July",
  "August", "September", "October", "November", "December"];

function pad2(n) { return String(n).padStart(2, "0"); }
function fmtDT(y, mo, d, h, mi) { return `${y}-${pad2(mo + 1)}-${pad2(d)} ${pad2(h)}:${pad2(mi)}:00`; }
function daysInMonth(y, mo) { return new Date(Date.UTC(y, mo + 1, 0)).getUTCDate(); }

// Seeds the picker from whatever's already typed. Matches the shape of the
// formats cliutil.ParseTime accepts (MySQL datetime, RFC3339, date-only) —
// not full semantic equivalence: a non-UTC RFC3339 offset is ignored, and
// out-of-range components are rejected here even though a trailing offset
// would shift them back in range on the Go side. Anything that doesn't match
// or fails range validation (or an empty field) leaves the typed text alone
// and just opens on today's UTC date — Date.UTC() silently rolls over
// invalid values instead of erroring, so an unchecked "2026-02-30" would
// otherwise render a header/grid for a different month than the day count
// implies.
function parseDTValue(s) {
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?/.exec((s || "").trim());
  if (!m) return null;
  const y = +m[1], mo = +m[2] - 1, d = +m[3];
  const h = m[4] ? +m[4] : 0, mi = m[5] ? +m[5] : 0;
  if (mo < 0 || mo > 11 || h > 23 || mi > 59) return null;
  if (d < 1 || d > daysInMonth(y, mo)) return null;
  return { y, mo, d, h, mi };
}

let openDT = null; // { pop, trigger, cleanup() } — one date picker open at a time

function closeDatePicker() {
  if (!openDT) return;
  openDT.cleanup();
  openDT.pop.remove();
  openDT = null;
}

// Setting .value programmatically doesn't fire native input/change events.
// The Events view's debounced auto-search relies on them (form listeners at
// runEventsQuery's call site); Recover/Time-travel are submit-only
// and ignore them, so the dispatch is a harmless no-op there — kept for
// consistency rather than because every caller needs it.
function applyDTValue(input, y, mo, d, h, mi) {
  input.value = fmtDT(y, mo, d, h, mi);
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

function clearDTValue(input) {
  input.value = "";
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

function toggleDatePicker(input, trigger) {
  if (openDT && openDT.trigger === trigger) { closeDatePicker(); return; }
  closeDatePicker();

  const now = new Date();
  const seed = parseDTValue(input.value) || {
    y: now.getUTCFullYear(), mo: now.getUTCMonth(), d: now.getUTCDate(), h: 0, mi: 0,
  };
  const state = { view: { y: seed.y, mo: seed.mo }, sel: { y: seed.y, mo: seed.mo, d: seed.d }, h: seed.h, mi: seed.mi };

  // Rendered into document.body, not in-flow — see the .dt-pop rule in
  // style.css for why (an ancestor overflow:hidden would clip it otherwise).
  const pop = el("div", { class: "dt-pop" });
  document.body.append(pop);
  renderDTPop(pop, state, input);

  const rect = trigger.getBoundingClientRect();
  const pw = pop.getBoundingClientRect().width;
  let left = rect.left + window.scrollX;
  const maxLeft = window.scrollX + window.innerWidth - pw - 8;
  if (left > maxLeft) left = Math.max(window.scrollX + 8, maxLeft);
  pop.style.top = (rect.bottom + window.scrollY + 6) + "px";
  pop.style.left = left + "px";

  // Self-cleaning: if the view was rebuilt out from under us (route change
  // via renderRoute already calls closeDatePicker, but this is the backstop)
  // the listener removes itself instead of acting on stale state.
  const onDocClick = (e) => {
    if (!pop.isConnected) { document.removeEventListener("click", onDocClick, true); return; }
    if (pop.contains(e.target) || e.target === trigger || trigger.contains(e.target)) return;
    closeDatePicker();
  };
  const onKey = (e) => {
    if (!pop.isConnected) { document.removeEventListener("keydown", onKey, true); return; }
    if (noticeOpen()) return; // the notice above owns Escape (#1769)
    if (e.key === "Escape") closeDatePicker();
  };
  const onDismiss = () => { if (pop.isConnected) closeDatePicker(); };
  document.addEventListener("click", onDocClick, true);
  document.addEventListener("keydown", onKey, true);
  window.addEventListener("scroll", onDismiss, true);
  window.addEventListener("resize", onDismiss);

  openDT = {
    pop, trigger,
    cleanup() {
      document.removeEventListener("click", onDocClick, true);
      document.removeEventListener("keydown", onKey, true);
      window.removeEventListener("scroll", onDismiss, true);
      window.removeEventListener("resize", onDismiss);
    },
  };
}

function renderDTPop(pop, state, input) {
  clear(pop);

  const head = el("div", { class: "dt-head" });
  head.append(
    el("button", { class: "btn btn-icon btn-sm btn-ghost dt-nav dt-nav-prev", type: "button", "aria-label": "Previous month", onclick: () => {
      state.view.mo--; if (state.view.mo < 0) { state.view.mo = 11; state.view.y--; }
      renderDTPop(pop, state, input);
    } }, icon("caret", "dt-nav-ic")),
    el("span", { class: "dt-month", text: `${DT_MON[state.view.mo]} ${state.view.y}` }),
    el("button", { class: "btn btn-icon btn-sm btn-ghost dt-nav", type: "button", "aria-label": "Next month", onclick: () => {
      state.view.mo++; if (state.view.mo > 11) { state.view.mo = 0; state.view.y++; }
      renderDTPop(pop, state, input);
    } }, icon("caret", "dt-nav-ic")));
  pop.append(head);

  const grid = el("div", { class: "dt-grid" });
  DT_DOW.forEach((d) => grid.append(el("span", { class: "dt-dow", text: d })));

  const firstDow = new Date(Date.UTC(state.view.y, state.view.mo, 1)).getUTCDay();
  const nDays = daysInMonth(state.view.y, state.view.mo);
  const prevY = state.view.mo === 0 ? state.view.y - 1 : state.view.y;
  const prevMo = state.view.mo === 0 ? 11 : state.view.mo - 1;
  const prevNDays = daysInMonth(prevY, prevMo);
  const nextY = state.view.mo === 11 ? state.view.y + 1 : state.view.y;
  const nextMo = state.view.mo === 11 ? 0 : state.view.mo + 1;
  const today = new Date();

  const dayCell = (y, mo, d, muted) => {
    const isToday = y === today.getUTCFullYear() && mo === today.getUTCMonth() && d === today.getUTCDate();
    const isSel = y === state.sel.y && mo === state.sel.mo && d === state.sel.d;
    return el("button", {
      class: "dt-day" + (muted ? " is-muted" : "") + (isToday ? " is-today" : "") + (isSel ? " is-sel" : ""),
      type: "button", text: String(d),
      onclick: () => { state.sel = { y, mo, d }; state.view = { y, mo }; renderDTPop(pop, state, input); },
    });
  };
  for (let i = firstDow - 1; i >= 0; i--) grid.append(dayCell(prevY, prevMo, prevNDays - i, true));
  for (let d = 1; d <= nDays; d++) grid.append(dayCell(state.view.y, state.view.mo, d, false));
  const trailing = (7 - ((firstDow + nDays) % 7)) % 7;
  for (let d = 1; d <= trailing; d++) grid.append(dayCell(nextY, nextMo, d, true));
  pop.append(grid);

  const timeRow = el("div", { class: "dt-time" });
  const hSel = el("select", { class: "select dt-time-select", "aria-label": "Hour" });
  for (let h = 0; h < 24; h++) hSel.append(opt(String(h), pad2(h)));
  hSel.value = String(state.h);
  hSel.addEventListener("change", () => { state.h = +hSel.value; });
  const miSel = el("select", { class: "select dt-time-select", "aria-label": "Minute" });
  for (let mi = 0; mi < 60; mi++) miSel.append(opt(String(mi), pad2(mi)));
  miSel.value = String(state.mi);
  miSel.addEventListener("change", () => { state.mi = +miSel.value; });
  timeRow.append(hSel, el("span", { class: "dt-time-sep", text: ":" }), miSel,
    el("span", { class: "dt-tz", text: "UTC" }));
  pop.append(timeRow);

  const foot = el("div", { class: "dt-foot" });
  foot.append(
    el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Now", onclick: () => {
      const n = new Date();
      applyDTValue(input, n.getUTCFullYear(), n.getUTCMonth(), n.getUTCDate(), n.getUTCHours(), n.getUTCMinutes());
      closeDatePicker();
    } }),
    el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Clear", onclick: () => {
      clearDTValue(input);
      closeDatePicker();
    } }),
    el("button", { class: "btn btn-sm", type: "button", text: "Apply", onclick: () => {
      applyDTValue(input, state.sel.y, state.sel.mo, state.sel.d, state.h, state.mi);
      closeDatePicker();
    } }));
  pop.append(foot);
}

// fieldDateInput builds the same field chrome as fieldInput but wires a
// calendar+clock popover to a trailing button — manual typing still works,
// the picker is a progressive-enhancement affordance over the same input.
function fieldDateInput(label, name, size, placeholder, required) {
  const input = el("input", { class: "input dt-input", name, placeholder: placeholder || "" });
  const trigger = el("button", { class: "btn btn-icon btn-sm btn-ghost dt-trigger", type: "button", "aria-label": "Open calendar" },
    icon("calendar", "dt-trigger-ic"));
  trigger.addEventListener("click", (e) => { e.preventDefault(); toggleDatePicker(input, trigger); });
  const wrap = el("div", { class: "dt-wrap" }, input, trigger);
  return el("div", { class: "field field--" + size }, fieldLabel(label, required), wrap);
}

// parseSmartQuery turns "type:delete pk:1006 orders" into structured filters +
// leftover free terms. Mirrors the prototype's parseQuery intent.
function parseSmartQuery(q) {
  const c = { terms: [] };
  const known = { table: 1, pk: 1, type: 1, col: 1, column: 1, schema: 1, since: 1, until: 1, gtid: 1, limit: 1 };
  for (const tok of (q || "").trim().split(/\s+/).filter(Boolean)) {
    const i = tok.indexOf(":");
    const k = i > 0 ? tok.slice(0, i).toLowerCase() : "";
    if (k && known[k]) {
      const val = tok.slice(i + 1);
      if (k === "col" || k === "column") c.changed_column = val;
      else if (k === "type") c.event_type = val.toUpperCase();
      else c[k] = val;
    } else if (tok.includes(".") && !c.schema && !c.table) {
      const [s, tb] = tok.split(".");
      if (s) c.schema = s;
      if (tb) c.table = tb;
    } else {
      // A bare word is a free-text term, refined client-side over the fetched
      // page (like the prototype). We do NOT map it to an exact table filter —
      // that would silently return 0 rows for a value/column search.
      c.terms.push(tok.toLowerCase());
    }
  }
  return c;
}

// keepPage is set ONLY by the Prev/Next buttons. Every other caller — the
// debounced `input` handler, `change`, `submit` — is a filter edit, and a
// filter edit must drop the cursor: a cursor from the previous search names a
// row that need not exist in the new one, so keeping it would serve a page
// from the middle of a search the operator never ran.
async function runEventsQuery(form, keepPage) {
  if (!keepPage) { evPages = [{ before: null, offset: 0 }]; evPageIdx = 0; }
  const gen = serverGen;
  const rowsEl = $("#ev-rows", VIEW());
  const countEl = $("#ev-count", VIEW());
  if (!rowsEl) return;

  // Merge smart-search tokens with the advanced panel (structured fields win).
  const parsed = parseSmartQuery(form.elements.q ? form.elements.q.value : "");
  const f = Object.fromEntries(new FormData(form).entries());
  const merged = Object.assign({}, parsed);
  ["schema", "table", "pk", "event_type", "changed_column", "since", "until", "gtid", "limit"].forEach((k) => {
    if (f[k] && f[k].trim() && f[k] !== "any") merged[k] = f[k].trim();
  });

  // Build API params. pk / changed_column require schema+table server-side, so
  // when they are not both present we apply them client-side instead of 400ing.
  const apiParams = {};
  const hasScope = merged.schema && merged.table;
  ["schema", "table", "event_type", "since", "until", "gtid", "limit"].forEach((k) => {
    if (merged[k]) apiParams[k] = merged[k];
  });
  if (hasScope) {
    if (merged.pk) apiParams.pk = merged.pk;
    if (merged.changed_column) apiParams.changed_column = merged.changed_column;
  }

  // Client-side refine terms are computed before the fetch so export can reuse
  // the exact same pair (server filters + refine) the page was built from.
  const refine = [];
  if (!hasScope && merged.pk) refine.push(merged.pk.toLowerCase());
  if (!hasScope && merged.changed_column) refine.push(merged.changed_column.toLowerCase());
  refine.push(...parsed.terms);
  evLastQuery = { apiParams, refine };

  const pageParams = Object.assign({ order: "DESC" }, apiParams);
  const before = evPages[evPageIdx] && evPages[evPageIdx].before;
  if (before) pageParams.before = before;

  // Loading state (#1353): skeleton rows from the moment the fetch starts — a
  // blank list must never be the "busy" rendering (it reads as "no events" or
  // "broken", on the page an operator opens mid-incident). Past ~2s the
  // skeleton names what is happening, so a slow archive read looks like work
  // instead of a hang. The skeleton lives inside #ev-rows, so the success path
  // (buildEventRows) and the error path (renderError) both sweep it away with
  // their own clear() — a failed fetch can never leave a stuck skeleton. The
  // isConnected guard keeps a superseded search's timer from touching the
  // newer render.
  const skel = renderEventsLoading(rowsEl);
  const slowT = setTimeout(() => { if (skel.isConnected) skel.classList.add("slow"); }, 2000);
  if (countEl) countEl.textContent = "…";

  // Progressive read (#1414): the first page of a browse fetches the LIVE
  // index only (scope=live) and paints immediately — rows already sitting in
  // binlog_events must not wait behind an S3 scan — then the full merged read
  // completes it in the background. Cursor pages keep the single full fetch:
  // a deep page is usually IN the archives, so there is no fast half to show
  // first.
  const progressive = !before;
  const myQuery = evLastQuery;

  // paint renders one response. It runs twice on a progressive read — the
  // live phase, then the merged phase — so everything it derives (cursors,
  // counts, advisory lists) is REPLACED by phase 2 wholesale rather than
  // merged client-side: the phase-2 response is the same authoritative shape
  // a plain fetch returns, which is what makes reordering impossible even
  // when plan.ArchivesBelowLive is false (#1037 backfills). Row identity for
  // the keyboard cursor and expanded diffs survives the repaint keyed on
  // eventDTO.anchor (#1411).
  const paint = (data, partialPending) => {
    let warnings = data.warnings || [];
    if (partialPending) {
      // The server's PARTIAL warning states the fact; this line states the
      // in-progress half — it exists only while phase 2 is in flight and is
      // swept by phase 2's own paint (or replaced by the failure line).
      warnings = warnings.concat("Reading archived history in the background; the list below will complete itself.");
    }
    renderWarnings($("#ev-warnings", VIEW()), warnings);
    renderEventNotes($("#ev-notes", VIEW()), data.notes);

    // Client-side refine: unscoped pk/col + free terms.
    const events = refineEvents(data.events || [], refine);

    // Remember where the NEXT page starts before rendering this one. The cursor
    // comes from the server (data.next_before), which derives it from the last
    // row it actually served — not from `events`, which the refine above may
    // have dropped the boundary row from. Deriving it here would skip whatever
    // the refine hid.
    if (data.has_more && data.next_before) {
      evPages[evPageIdx + 1] = { before: data.next_before, offset: evPages[evPageIdx].offset + data.count };
    } else {
      evPages.length = evPageIdx + 1;
    }

    // Carry the operator's place across the phase-2 repaint: the focused row
    // and any expanded diffs, keyed on anchor — event_id alone is per-index,
    // which suffices here (both phases read one index), but anchor is the
    // identity Undo already relies on.
    const openAnchors = {}, focusedAnchor = { v: null };
    // The j/k cursor is a THIRD identity channel (review pass 1): cursorIdx
    // is module state that survives the repaint while its .cursor class does
    // not — `u` would then fire undoEvent(lastEvents[cursorIdx]) against a
    // reindexed list with no highlight anywhere on screen. Captured by
    // anchor with the rest and re-seated below; unresolvable → -1, the same
    // reset eventsGoPage does for the same reason.
    const cursorAnchor = (cursorIdx >= 0 && lastEvents[cursorIdx] && lastEvents[cursorIdx].anchor) || null;
    if (lastEvents.length) {
      rowsEl.querySelectorAll(".ev-row").forEach((r) => {
        const ev = lastEvents[Number(r.dataset.ev)];
        if (!ev || !ev.anchor) return;
        if (r.classList.contains("open")) openAnchors[ev.anchor] = true;
        if (r === document.activeElement) focusedAnchor.v = ev.anchor;
      });
    }

    lastEvents = events;
    // Honest scope (#966 + #1297): free terms / unscoped pk are refined
    // client-side over ONE fetched page, so a refined count is page-local. The
    // window note no longer restates the limit back at the reader ("100 events in
    // the newest 100" answered nothing about whether 101 or ten million sat
    // behind it) — has_more, one probe row on the server, says which.
    const refining = refine.length > 0;
    const from = evPages[evPageIdx].offset + 1;
    const to = evPages[evPageIdx].offset + data.count;
    let scopeNote = "";
    if (data.has_more) {
      scopeNote = " · showing " + from + "–" + to + " of more; page older for the rest";
    } else if (evPageIdx > 0) {
      // Paged to the end, so the total IS known exactly: it is what we walked.
      scopeNote = " · showing " + from + "–" + to + " of " + to + " (end)";
    }
    if (refining && data.count !== events.length) {
      // The refine ran over this page only; say so rather than let the number
      // read as an index-wide match count.
      scopeNote += " · refined within this page";
    }
    if (countEl) countEl.textContent = String(events.length);
    const noteEl = $("#ev-count-note", VIEW());
    if (noteEl) noteEl.textContent = (refining ? " match(es)" : " event(s)") + scopeNote;
    const prevBtn = $("#ev-prev", VIEW());
    const nextBtn = $("#ev-next", VIEW());
    if (prevBtn) prevBtn.disabled = evPageIdx === 0;
    if (nextBtn) nextBtn.disabled = !data.has_more;
    buildEventRows(rowsEl, events, scopeNote);

    cursorIdx = -1;
    if (focusedAnchor.v || cursorAnchor || Object.keys(openAnchors).length) {
      rowsEl.querySelectorAll(".ev-row").forEach((r) => {
        const ev = events[Number(r.dataset.ev)];
        if (!ev || !ev.anchor) return;
        if (openAnchors[ev.anchor] && !r.classList.contains("open")) r.click();
        if (ev.anchor === focusedAnchor.v) r.focus();
        if (ev.anchor === cursorAnchor) { cursorIdx = Number(r.dataset.ev); r.classList.add("cursor"); }
      });
    }
  };

  let data;
  try {
    const p1 = progressive ? Object.assign({}, pageParams, { scope: "live" }) : pageParams;
    data = await api("/api/events?" + new URLSearchParams(p1).toString());
  } catch (err) {
    if (gen !== serverGen) return;
    clear(rowsEl); renderError(rowsEl, err);
    // Clear stale advisories along with the rows: a lingering "nothing is
    // missing here" (or an old warning) beside an error is misleading (#1365).
    renderWarnings($("#ev-warnings", VIEW()), []);
    renderEventNotes($("#ev-notes", VIEW()), []);
    if (countEl) countEl.textContent = "0";
    return;
  } finally {
    clearTimeout(slowT);
  }
  if (gen !== serverGen) return;
  const pending = progressive && !!data.archives_pending;
  paint(data, pending);
  if (!pending) return;

  // Phase 2: the same search, full scope. Applied only if this search is
  // still the one on screen — a filter edit or page step replaces
  // evLastQuery, a server switch bumps serverGen, and either bails this
  // stale completion out. On failure the partial marker STAYS UP and names
  // the failure: a live-only list must never quietly present as complete.
  api("/api/events?" + new URLSearchParams(pageParams).toString()).then((full) => {
    if (gen !== serverGen || evLastQuery !== myQuery || !rowsEl.isConnected) return;
    paint(full, false);
  }, (err) => {
    if (gen !== serverGen || evLastQuery !== myQuery || !rowsEl.isConnected) return;
    renderWarnings($("#ev-warnings", VIEW()), (data.warnings || []).concat(
      "The archive read FAILED: this list remains live-only and may be missing archived history: " +
      ((err && err.message) || err)));
  });
}

// renderEventsLoading paints the Events list's busy state (#1353): skeleton
// rows in the shape of the real ones, plus a what's-happening note that stays
// hidden until the caller's slow-fetch timer flips the wrapper to "slow".
// Returns the wrapper so that timer can check it is still on screen
// (isConnected) before speaking — a superseded search repaints its own
// skeleton, and the stale timer must not touch it.
function renderEventsLoading(container) {
  clear(container);
  const wrap = el("div", { class: "ev-loading", role: "status", "aria-label": "Loading events" });
  for (let i = 0; i < 8; i++) {
    const r = el("div", { class: "ev-skel-row" });
    ["time", "table", "type", "pk", "cols"].forEach((c) => r.append(el("span", { class: "ev-skel-bar ev-skel-" + c })));
    wrap.append(r);
  }
  wrap.append(el("div", { class: "ev-skel-note",
    text: "Still working: reading history. Archived hours can take a while…" }));
  container.append(wrap);
  return wrap;
}

// refineEvents applies the client-side free-text / unscoped-pk refine. Shared
// by the rendered page and by export so the two can never diverge on what
// "matches this search" means.
function refineEvents(events, refine) {
  if (!refine.length) return events;
  return events.filter((e) => {
    const hay = (e.schema_name + "." + e.table_name + " " + e.event_type + " " + e.pk_values + " " +
      (e.changed_columns || []).join(" ") + " " +
      valueToString(e.row_before) + " " + valueToString(e.row_after)).toLowerCase();
    return refine.every((t) => hay.includes(t));
  });
}

// eventsGoPage steps the Events view one keyset page in `dir` (+1 = older).
function eventsGoPage(dir) {
  const next = evPageIdx + dir;
  if (next < 0 || next >= evPages.length) return;
  evPageIdx = next;
  cursorIdx = -1; // the keyboard cursor indexes into the old page's rows
  runEventsQuery($("#ev-form", VIEW()), true);
}

// exportEvents downloads EVERY match of the current search, not just the page
// on screen.
//
// The decision, and why: once the view pages, "export what is rendered" means
// an operator who filtered to one table, walked four pages of an incident and
// hit Export silently gets a quarter of their evidence. That is worse than the
// un-paged behavior it replaces. Exporting the whole filtered set costs one
// cursor-less request at the limit the endpoint ALREADY sanctions
// (eventsMaxLimit, 1000) — it grants no capability the caps did not already
// permit, and deliberately does NOT page the export loop, which is exactly how
// a download button would become the way to pull an index into a browser.
// The cap is not silent: a result that comes back at the ceiling says so.
async function exportEvents(kind, btn) {
  if (!evLastQuery) return;
  const label = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "…"; }
  try {
    const params = Object.assign({}, evLastQuery.apiParams, { order: "DESC", limit: String(EVENT_EXPORT_MAX) });
    const data = await api("/api/events?" + new URLSearchParams(params).toString());
    const rows = refineEvents(data.events || [], evLastQuery.refine);
    if (kind === "csv") downloadEventsCSV(rows); else downloadEventsJSON(rows);
    if (data.has_more) {
      // rows.length, not data.count: the client-side refine runs after the
      // fetch, so naming the server's count would report a number that is not
      // the row count of the file just downloaded — the same species of
      // misleading figure this whole change set out to remove.
      toast("Exported the newest " + rows.length + " matches; the search has more. Add a time range to export the rest.");
    }
  } catch (err) {
    toastError("Export failed: " + (err && err.message ? err.message : String(err)));
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = label; }
  }
}

// EVENTS_EMPTY_ART: what the list will hold, drawn (three rows of changes:
// a time, a table, the kind of change, a key). A static constant in the
// page's tokens; decoration beside the words, so it carries no text.
const EVENTS_EMPTY_ART = `<svg viewBox="0 0 160 72" aria-hidden="true"><rect x="1" y="1" width="158" height="70" rx="8" fill="var(--surface)" stroke="var(--line)"/><rect x="12" y="13" width="34" height="5" rx="2.5" fill="var(--ink-4)"/><rect x="54" y="13" width="40" height="5" rx="2.5" fill="var(--line)"/><rect x="102" y="10" width="26" height="11" rx="5.5" fill="var(--insert-bg)" stroke="var(--insert)"/><rect x="136" y="13" width="12" height="5" rx="2.5" fill="var(--ink-4)"/><rect x="12" y="34" width="34" height="5" rx="2.5" fill="var(--ink-4)"/><rect x="54" y="34" width="52" height="5" rx="2.5" fill="var(--line)"/><rect x="102" y="31" width="26" height="11" rx="5.5" fill="var(--update-bg)" stroke="var(--update)"/><rect x="136" y="34" width="12" height="5" rx="2.5" fill="var(--ink-4)"/><rect x="12" y="55" width="34" height="5" rx="2.5" fill="var(--ink-4)"/><rect x="54" y="55" width="32" height="5" rx="2.5" fill="var(--line)"/><rect x="102" y="52" width="26" height="11" rx="5.5" fill="var(--delete-bg)" stroke="var(--delete)"/><rect x="136" y="55" width="12" height="5" rx="2.5" fill="var(--ink-4)"/></svg>`;

// eventsEmptyState is the list with nothing in it (#1950): the drawing of
// what will appear, one line, and the one action that makes it appear. Two
// cases that need opposite actions: a search or filter that matches nothing
// (clear it), and a list that is empty with nothing narrowing it (no change
// was captured yet, so the way forward is the capture's own page).
function eventsEmptyState(scopeNote) {
  const form = $("#ev-form", VIEW());
  const narrowed = !!form && Array.from(new FormData(form).values()).some((v) => String(v).trim() && v !== "any");
  const box = el("div", { class: "empty ev-empty" });
  box.append(el("div", { class: "empty-art", "aria-hidden": "true" }, svgEl(EVENTS_EMPTY_ART)));
  if (narrowed) {
    box.append(el("h3", { text: "No changes match your search" }));
    // The paging scope travels with the verdict: "nothing here" on page 3 of
    // a refined search is a statement about that page, not about the index.
    if (scopeNote) box.append(el("p", { text: scopeNote.replace(/^ · /, "") }));
    box.append(el("button", { class: "btn", type: "button", text: "Clear the search",
      onclick: () => { form.reset(); runEventsQuery(form); } }));
  } else {
    box.append(el("h3", { text: "No changes yet" }));
    box.append(el("p", { text: "Rows appear here as your database writes them." }));
    box.append(el("button", { class: "btn", type: "button", text: "Check capture", onclick: () => navigate("status") }));
  }
  return box;
}

function buildEventRows(container, events, scopeNote) {
  clear(container);
  // Keys line up on the right only when every key on screen is a number; one
  // composite or text key and the column reads left, like text.
  const list = container.parentElement;
  if (list && list.classList) {
    list.classList.toggle("pk-num", events.length > 0 && events.every((e) => /^-?\d+$/.test(String(e.pk_values))));
  }
  if (!events.length) {
    container.append(eventsEmptyState(scopeNote));
    return;
  }
  events.forEach((e, i) => {
    const row = el("div", { class: "ev-row", "data-ev": i, tabindex: "0", role: "button", "aria-expanded": "false" });
    row.append(icon("caret", "ev-caret"));
    row.append(tsSpan("ev-time", e.event_timestamp));
    row.append(el("span", { class: "ev-table", text: e.schema_name + "." + e.table_name }));
    row.append(el("span", {}, badge(e.event_type)));
    row.append(el("span", { class: "ev-pk", text: e.pk_values }));
    row.append(el("span", { class: "ev-cols" }, ...colsSummary(e.changed_columns, true)));
    const wrap = el("div", { class: "diff-wrap", id: "diff-" + i });
    let loaded = false;
    row.addEventListener("click", () => {
      const open = row.classList.toggle("open");
      row.setAttribute("aria-expanded", open ? "true" : "false");
      if (open && !loaded) { clear(wrap); wrap.append(renderDiff(e)); loaded = true; }
    });
    // Keyboard activation (#968): the row is a focusable expando, so Enter and
    // Space must toggle it like a real button would.
    row.addEventListener("keydown", (ke) => {
      if (ke.key === "Enter" || ke.key === " ") { ke.preventDefault(); row.click(); }
    });
    container.append(row);
    container.append(wrap);
  });
}

function renderDiff(ev) {
  const before = ev.row_before || {};
  const after = ev.row_after || {};
  const cols = Array.from(new Set([...Object.keys(before), ...Object.keys(after)])).sort();
  const changed = new Set(ev.changed_columns || []);
  const wholeRow = ev.event_type === "INSERT" || ev.event_type === "DELETE";

  const grid = el("div", { class: "diff-grid" });
  grid.append(el("div", { class: "diff-h", text: "column" }));
  grid.append(el("div", { class: "diff-h", text: "before" }));
  grid.append(el("div", { class: "diff-h", text: "after" }));
  if (!cols.length) {
    grid.append(el("div", { class: "diff-col", text: "(no row image)" }));
    grid.append(el("div", { class: "diff-before" }));
    grid.append(el("div", { class: "diff-after" }));
  } else {
    cols.forEach((c) => {
      const isCh = wholeRow || changed.has(c);
      const m = isCh ? " diff-row-changed" : "";
      grid.append(el("div", { class: "diff-col" + m, text: c }));
      grid.append(el("div", { class: "diff-before" + m, text: c in before ? valueToString(before[c]) : "∅" }));
      grid.append(el("div", { class: "diff-after" + m, text: c in after ? valueToString(after[c]) : "∅" }));
    });
  }

  const foot = el("div", { class: "diff-foot" });
  foot.append(el("span", { class: "diff-foot-note", text: "Generates undo SQL. Nothing runs on its own." }));
  const label = ev.event_type === "DELETE" ? "Restore this row" : ev.event_type === "INSERT" ? "Undo this insert" : "Undo this change";
  foot.append(el("button", { class: "btn btn-sm btn-primary", type: "button", text: label, onclick: () => undoEvent(ev) }));

  return el("div", { class: "diff" }, grid, foot);
}

// ── keyboard cursor on Events (j/k/↵/u) ──────────────────────────────────────

function moveCursor(delta) {
  const rows = $all(".ev-row", VIEW());
  if (!rows.length) return;
  if (cursorIdx >= 0 && rows[cursorIdx]) rows[cursorIdx].classList.remove("cursor");
  cursorIdx = Math.max(0, Math.min(rows.length - 1, cursorIdx + delta));
  const row = rows[cursorIdx];
  row.classList.add("cursor");
  row.scrollIntoView({ block: "nearest" });
}

// ── exports (client-side, over the redacted rows on screen) ──────────────────────────────────────────────

function downloadBlob(filename, content, mime) {
  try {
    const url = URL.createObjectURL(new Blob([content], { type: mime }));
    const a = el("a", { href: url, download: filename });
    document.body.append(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
  } catch (err) { toastError("download failed: " + ((err && err.message) || err)); }
}
function csvCell(v) {
  if (v === null || v === undefined) return "";
  let s = typeof v === "object" ? JSON.stringify(v) : String(v);
  // Formula-injection guard (OWASP): a leading =, +, -, @, tab or CR would be
  // interpreted as a formula by Excel/Sheets — prefix a quote to neutralize.
  if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}
function downloadEventsJSON(events) {
  downloadBlob("dbtrail-events.json", JSON.stringify(events || [], null, 2), "application/json");
}
function downloadEventsCSV(events) {
  const lines = [EVENT_CSV_COLUMNS.join(",")];
  (events || []).forEach((ev) => lines.push(EVENT_CSV_COLUMNS.map((c) => csvCell(ev[c])).join(",")));
  downloadBlob("dbtrail-events.csv", lines.join("\r\n"), "text/csv");
}

// ── Recover ────────────────────────────────────────────────────────────────

function renderRecover(params) {
  const v = VIEW(); clear(v);
  // No opening sentence (#1950): the drawing above the script shows what an
  // undo does, and "nothing runs on its own" is the SQL card's footer, where
  // the script is.
  v.append(pageHead("Restore", null));

  // Context banner when arriving via an event "Undo" (pendingRecover).
  const ctx = pendingRecover;
  if (ctx) {
    const banner = el("div", { class: "ctx-banner", id: "undo-ctx-banner" });
    banner.append(el("span", { class: "badge " + badgeClass(ctx.type), text: ctx.type }));
    banner.append(el("div", { class: "ctx-main" },
      // Scope, and since #1411 the scope is an EVENT, not a window of time.
      // The prefill sets form.elements.event from ctx.anchor — the server's own
      // `<RFC3339Nano>|<event_id>` token for the clicked row — and the engine
      // filters on that identity, so exactly one event is reversed and it is
      // the one pointed at.
      //
      // What this block used to have to explain, kept because it is the reason
      // the anchor exists rather than history for its own sake: with only
      // `until` + latest-per-row = 1, the ceiling was the END OF A SECOND and
      // the cap kept the last event inside it. On a row INSERTed and DELETEd
      // within one second that is the DELETE whichever row was clicked, and
      // reversing a DELETE re-creates the row — so Undo on the INSERT put the
      // row BACK while the badge read INSERT. The banner disclosed it in
      // words; disclosure was the right immediate move and a poor permanent
      // one.
      //
      // `until` stays prefilled and is still worth stating: it is how the
      // operator reads WHEN, and it is what remains as the scope if they press
      // Clear. It no longer decides WHICH.
      //
      // Worth knowing about the guard: assets_recover_banner_test.go reads
      // string literals only, so no test can see this prose. The same-second
      // caveat it used to pin as REQUIRED text is now false and was removed in
      // the same commit that made it false — dropping one without the other
      // either fails the build or ships a lie.
      el("span", { class: "ctx-eyebrow", text: "Undoing one change" }),
      el("span", { class: "ctx-title", text: ctx.schema + "." + ctx.table + " \u00b7 pk " + ctx.pk }),
      el("span", { class: "ctx-detail", text: "reverses exactly this " + ctx.type
        + ", the one you clicked (" + ctx.time + " UTC), not the rest of the row's history, and not other changes sharing that second" }),
      el("span", { class: "ctx-detail", text: "Clear to search this row freely instead; the time you clicked stays as the upper bound." })));
    banner.append(el("span", { class: "spacer" }));
    // Clear retires the SELECTION, not the form.
    //
    // It used to `navigate("recover")`, which re-renders the route and builds a
    // fresh EMPTY form — so the one control labelled Clear wiped the target and
    // the upper bound, while the sentence above it promised the opposite
    // ("search this row freely… the time you clicked stays as the upper
    // bound"). The mechanism that sentence describes already existed, as
    // clearUndoAnchor; it just had no name in the UI. Calling it here is what
    // makes the copy true.
    banner.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Clear",
      onclick: () => clearUndoAnchor(form) }));
    v.append(banner);
  }

  // Manual filter form
  const form = el("form", { class: "filters", id: "recover-form" });
  form.append(fieldSelect("Schema", "schema", "md", true, false, null, "— select —"));
  // Input + datalist, not a closed select (#1364): suggestions come from the
  // selected schema, but recover legitimately targets dropped tables whose
  // events are still indexed — a typed name must always submit.
  form.append(fieldTableCombo("Table", "table", "md", "orders"));
  form.append(fieldInput("PK", "pk", "sm", "42 or 42|7"));
  // The undo anchor rides in the form rather than in a module variable so the
  // two request builders pick it up the same way every other filter does (both
  // read the form through FormData). That is what keeps "Preview rows" and
  // "Generate undo SQL" showing the same events — the promise previewRecover
  // makes. Hidden because it is an identity, not a filter an operator can
  // usefully type: it is set by Undo and removed by the banner's Clear.
  form.append(el("input", { type: "hidden", name: "event" }));
  // Latest-per-row sits beside PK because it is meaningless without one (the
  // server refuses the pair). It caps a row's HISTORY — it does not name an
  // event: this comment used to claim it was "the only filter that can
  // separate events sharing a timestamp", which was true of the time filters
  // and false as a general claim, and Undo relied on it (#1411). The hidden
  // `event` field above is what separates events sharing a second.
  form.append(fieldInput("Latest per row", "limit_per_pk", "sm", "all"));
  form.append(fieldDateInput("Since (UTC)", "since", "md", "YYYY-MM-DD HH:MM:SS"));
  form.append(fieldDateInput("Until (UTC)", "until", "md", "YYYY-MM-DD HH:MM:SS"));
  const actions = el("div", { class: "filter-actions" });
  actions.append(el("button", { class: "btn btn-ghost", type: "button", text: "Preview rows",
    onclick: () => previewRecover(form) }));
  actions.append(el("button", { class: "btn btn-primary", type: "submit", text: "Generate undo SQL" }));
  form.append(actions);
  v.append(form);

  v.append(stateSection(form));
  v.append(el("div", { id: "recover-warnings", class: "warnings" }));
  v.append(el("div", { id: "recover-notes", class: "notes" }));
  v.append(el("div", { id: "recover-preview" }));
  const out = el("div", { id: "recover-out" });
  // Empty until a change is picked (#1950): what will appear here, drawn.
  if (!ctx) out.append(undoEmptyState());
  v.append(out);

  form.addEventListener("submit", (e) => { e.preventDefault(); generateUndo(form); });
  // Editing the target retires the anchor, and the banner with it.
  //
  // The anchor names one event of one row. Retype the PK and it still names
  // the OLD row's event, so the request comes back empty — a 200 with no
  // statements, which reads as "this row has no history to undo". It fails
  // closed rather than producing wrong SQL, but the narrowing moved from a
  // VISIBLE field the banner named ("Latest per row is set to 1 — clear it
  // to…") into a hidden one nothing names, so an operator has nothing to
  // notice. Retiring it on a target edit is what keeps the form's visible
  // state and its actual scope the same thing.
  //
  // Bound to `change`, not `input`: a keystroke-by-keystroke clear would drop
  // the anchor while the operator is still typing over a value they mean to
  // restore.
  for (const name of ["schema", "table", "pk"]) {
    const field = form.elements[name];
    if (field) field.addEventListener("change", () => clearUndoAnchor(form));
  }
  wireSchemaCascade(form);
  populateSchemas(form);

  // Prefill from context and auto-generate.
  if (ctx) {
    setSelectWhenReady(form, "schema", ctx.schema, () => {
      form.elements.table.value = ctx.table;
      form.elements.pk.value = ctx.pk;
      if (ctx.time) form.elements.until.value = ctx.time;
      // Undo means "undo THIS change", and it now says which one. #1404 got
      // the scope down to a single event with `until` + latest-per-row = 1,
      // which is one event but not necessarily the CLICKED one: both filters
      // are second-granular between them, so a row INSERTed and DELETEd inside
      // one second resolved to the DELETE whichever of the two was clicked —
      // and reversing a DELETE re-creates the row, inverting the outcome
      // (#1411). The anchor names the event itself, so no cap is needed and
      // none is set: two mechanisms narrowing the same scope is how they drift.
      //
      // `until` is left prefilled even though the anchor makes it redundant for
      // membership. It is what the operator reads to know WHEN, it bounds the
      // engine's partition scan, and clearing the anchor (banner → Clear) must
      // leave a sane window behind rather than the whole index.
      if (ctx.anchor) form.elements.event.value = ctx.anchor;
      generateUndo(form);
    });
  }
  // Restore to a moment (D9): rebuilding every table as it was is a
  // restore, so its card lives here, under the row-level one, and its run
  // reports on Snapshots where the copies are.
  const slot = el("div", { class: "rc-restore-slot" });
  v.append(slot);
  loadRestoreToMoment(slot);
  viewEnter();
}

// loadRestoreToMoment fills the Restore page's slot with the restore card
// once the three reads it needs are in (the server row, the copies, the last
// restore's state); nothing while the daemon has no restore feature. Where
// this server cannot restore, the card is still drawn, switched off, with the
// reason (#1684). A generation guard drops a late answer after the reader
// moved on.
async function loadRestoreToMoment(slot) {
  if (!capsCache.baseline_restore) return;
  const id = currentServer || defaultServerId;
  if (!id) return;
  const gen = serverGen, vgen = viewGen;
  const [srvRes, b, rst] = await Promise.all([
    // Both errors are kept: the switched-off card says them.
    api("/api/servers").catch((err) => ({ error: (err && err.message) || String(err) })),
    api("/api/baselines").catch((err) => ({ error: (err && err.message) || String(err) })),
    api("/api/servers/" + encodeURIComponent(id) + "/baseline/restore").catch(() => null),
  ]);
  if (gen !== serverGen || vgen !== viewGen) return;
  if (srvRes && srvRes.error) {
    // Which server this is could not be read, so nothing below can decide.
    if (sessionMay(PERM_SNAPSHOT_CREATE)) slot.append(restoreOffCard("list-error", "The server list could not be read: " + srvRes.error));
    return;
  }
  const cur = srvRes && Array.isArray(srvRes.servers) ? srvRes.servers.find((s) => s.id === id) : null;
  const running = !!(rst && rst.restore && rst.restore.state === "running");
  const card = running ? null : backupRestoreCard(cur, b, rst);
  if (!card && !running) return;
  if (card && card.dataset.why) {
    // Switched off: the run note would point at a run that cannot happen.
    // Where the fix is on Snapshots, the note is the way there.
    if (card.dataset.why === "no-folder" || card.dataset.why === "no-snapshot" || card.dataset.why === "shared") {
      card.append(el("p", { class: "form-hint rc-restore-note" },
        el("a", { href: "/snapshots", text: "Snapshots ›", onclick: (e) => { e.preventDefault(); navigate("snapshots"); } })));
    }
    slot.append(card);
    return;
  }
  const note = el("p", { class: "form-hint rc-restore-note" },
    running ? "A restore is running for this server. " : "The run and its result show on ",
    el("a", { href: "/snapshots", text: running ? "Follow it on Snapshots ›" : "Snapshots ›", onclick: (e) => { e.preventDefault(); navigate("snapshots"); } }));
  if (card) { card.append(note); slot.append(card); } else slot.append(el("section", { class: "ov-panel bk-restore" }, note));
}

// setSelectWhenReady fills a schema select once its options have loaded, then
// loads its tables and runs cb. Lets the undo-bridge prefill survive the async
// schema fetch without racing it.
function setSelectWhenReady(form, name, value, cb) {
  const gen = serverGen;
  const sel = form.elements[name];
  const tryset = () => {
    if (Array.from(sel.options).some((o) => o.value === value)) {
      sel.value = value;
      loadTables(form).then(() => cb && cb());
      return true;
    }
    return false;
  };
  if (tryset()) return;
  let n = 0;
  const iv = setInterval(() => {
    // Bail if the operator switched servers or navigated away — otherwise a
    // late tick would auto-generate undo SQL against a detached/other form.
    if (gen !== serverGen || !document.contains(form)) { clearInterval(iv); return; }
    if (tryset() || ++n > 40) clearInterval(iv);
  }, 50);
}

// ── busy modal for long actions (#1363, generalized in #1375) ────────────────
//
// Originally Restore-only, hence the recover-flavored default note. Verify's
// Explain reuses it (#1375) because it has the identical shape: one click
// starts minutes of work with nothing to show, and the failure mode is an
// operator concluding the button is dead. Callers that are not a form pass
// opts.disable (the elements to hold disabled) instead of a form element.
//
// Generate-undo (and Preview, which sits on the same latency) used to give no
// feedback until the fetch returned — seconds, when the window reaches
// archived hours — so a user who suspected the click didn't register clicked
// again and queued a second generation. The modal opens the moment the button
// is clicked: it states what is being generated (the same facts as the blue
// context banner), animates with pure-CSS keyframes on the brand accent
// (never the semantic insert/update/delete colors — those are data, not
// decoration), collapses to a static note under prefers-reduced-motion (the
// #1353 skeleton rule), blocks re-entry while open, and Cancel/ESC abort the
// in-flight fetch via AbortController. Errors render IN the modal with the
// server's own actionable text — never a silently-vanished overlay.

let busyModalOpen = false; // one click = one generation (re-entry guard)

// busyModalActive is the re-entry guard READ, and it self-heals: the shared
// #modal slot has other occupants (Manage servers, the rotation dialog) that
// replace its children without our teardown, so a set flag with no
// .busy-modal actually in the slot is stale — honoring it would wedge every
// future Generate/Preview click silently. The isConnected guards in
// openBusyModal tear down on the next keystroke; this covers the click
// path that arrives first.
function busyModalActive() {
  if (!busyModalOpen) return false;
  if (document.querySelector("#modal .busy-modal")) return true;
  busyModalOpen = false;
  return false;
}

// recoverBusyFacts mirrors the context banner's facts: target, pk, window.
// parseLatestPerRow normalises the "Latest per row" field. Returns null for
// anything that is not a whole number >= 0 — the caller reports that rather
// than sending it, because a silently dropped filter here means reversing MORE
// events than the operator asked for. Blank is 0 (all).
function parseLatestPerRow(raw) {
  const s = (raw || "").trim();
  if (s === "") return 0;
  const n = Number(s);
  if (!Number.isInteger(n) || n < 0) return null;
  return n;
}

function recoverBusyFacts(f) {
  const facts = [];
  facts.push(["target", f.schema ? f.schema + (f.table ? "." + f.table : "") : "(all schemas)"]);
  if (f.pk) facts.push(["pk", f.pk]);
  if (f.since) facts.push(["since", f.since + " UTC"]);
  if (f.until) facts.push(["until", f.until + " UTC"]);
  // Both call sites pass their own request object — generateUndo a number,
  // previewRecover a string — so stringify rather than assume either.
  if (f.limit_per_pk) facts.push(["latest per row", String(f.limit_per_pk)]);
  // The anchor is the filter that most determines the scope and the only one
  // with no visible field, so the busy modal is where an operator can see it
  // at all. Shown as the event id rather than the raw token: the timestamp is
  // already on the `until` line above, and the id is what the Events view
  // shows beside the row they clicked.
  if (f.event) facts.push(["single event", String(f.event).split("|").pop()]);
  return facts;
}

// openBusyModal opens the busy dialog over a long-running request. form, when
// given, supplies the action row to disable; a caller with no form (Explain)
// passes null and names its own elements in opts.disable. opts.note adds a
// line under the title explaining what the wait is. Returns {close(refocus),
// showError(err)}; Cancel and ESC run opts.onCancel (the fetch abort) and
// restore focus to the element that had it. Accessibility: role="dialog",
// aria-busy while working, focus trapped inside, focus restored on
// cancel/dismiss.
function openBusyModal(form, opts) {
  busyModalOpen = true;
  const mount = document.getElementById("modal");
  const trigger = document.activeElement;
  // A form caller disables its action row; a non-form caller (Explain) names
  // the elements itself. Either may be empty — the scrim is what actually
  // blocks input; this is the visible confirmation on top of it.
  const actions = opts.disable || (form ? $all(".filter-actions button", form) : []);
  actions.forEach((b) => { b.disabled = true; });

  const scrim = el("div", { class: "modal-scrim show" });
  const panel = el("div", { class: "modal busy-modal", role: "dialog", "aria-label": opts.title, "aria-busy": "true" });
  panel.append(el("div", { class: "modal-head" }, el("h2", { class: "modal-title", text: opts.title })));
  const body = el("div", { class: "modal-body" });
  body.append(el("div", { class: "busy-anim", "aria-hidden": "true" }, el("span"), el("span"), el("span")));
  // The reduced-motion arm: CSS swaps the animation for this static note.
  body.append(el("p", { class: "busy-static", text: "Working. This closes when the result is ready." }));
  const facts = el("div", { class: "busy-facts" });
  (opts.facts || []).forEach(([k, v]) => facts.append(el("div", { class: "busy-fact" },
    el("span", { class: "bf-k", text: k }), el("span", { class: "bf-v", text: v }))));
  body.append(facts);
  body.append(el("p", { class: "busy-note", text: opts.note ||
    "Reading indexed changes: a window that reaches archived hours can take a few seconds." }));
  const foot = el("div", { class: "modal-foot" });
  const cancelBtn = el("button", { class: "btn", type: "button", text: "Cancel", onclick: () => cancel() });
  foot.append(cancelBtn);
  body.append(foot);
  panel.append(body);
  scrim.append(panel);
  clear(mount);
  mount.append(scrim);
  cancelBtn.focus();

  let closed = false;
  // Capture phase, so this runs BEFORE globalKeydown's generic #modal-emptying
  // Escape handler — closing that way would leave the fetch in flight and the
  // form's buttons disabled. Tab is trapped inside the dialog.
  const onKey = (e) => {
    if (closed) return;
    // A notice above this dialog owns the keys (#1769): Escape there must not
    // cancel this request unseen.
    if (noticeOpen()) return;
    // The ⌘K palette stacks ABOVE this dialog in its own mount and handles
    // its own keys (its Escape handler sits on the palette input, which this
    // capture-phase listener would otherwise beat to the event) — while it is
    // open, bail entirely: same check globalKeydown does.
    const cmdk = document.getElementById("cmdk-mount");
    if (cmdk && cmdk.firstChild) return;
    // Another dialog clobbered the shared #modal slot (openServersModal /
    // showRotationDialog replace its children without our teardown): the trap
    // must dissolve, not keep intercepting Tab over a detached dialog and
    // holding the form's buttons disabled.
    if (!scrim.isConnected) { teardown(); return; }
    if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); cancel(); return; }
    if (e.key !== "Tab") return;
    const foci = $all("button, input, select, textarea, a[href]", panel).filter((n) => !n.disabled);
    if (!foci.length) { e.preventDefault(); return; }
    const first = foci[0], last = foci[foci.length - 1];
    if (!panel.contains(document.activeElement)) { e.preventDefault(); first.focus(); }
    else if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
  };
  document.addEventListener("keydown", onKey, true);

  const teardown = () => {
    if (closed) return false;
    closed = true;
    document.removeEventListener("keydown", onKey, true);
    scrim.remove();
    actions.forEach((b) => { b.disabled = false; });
    busyModalOpen = false;
    return true;
  };
  const restoreFocus = () => {
    if (trigger && document.contains(trigger) && typeof trigger.focus === "function") trigger.focus();
  };
  const cancel = () => {
    if (!teardown()) return;
    if (opts.onCancel) opts.onCancel();
    restoreFocus();
  };
  return {
    close(refocus) { if (teardown() && refocus) restoreFocus(); },
    showError(err) {
      if (closed) return;
      const msg = String((err && err.message) || err);
      if (!scrim.isConnected) {
        // Another dialog clobbered the shared #modal slot mid-flight:
        // rendering into the detached panel would be exactly the silently
        // vanished failure this modal exists to prevent. Tear down and
        // surface the error where it can be seen.
        teardown();
        toastError(opts.errTitle + ": " + msg);
        return;
      }
      panel.setAttribute("aria-busy", "false");
      panel.classList.add("busy-failed");
      body.insertBefore(el("div", { class: "busy-error", role: "alert" },
        el("p", { class: "busy-error-title", text: opts.errTitle }),
        el("p", { class: "busy-error-msg", text: msg })), foot);
      cancelBtn.textContent = "Dismiss";
      cancelBtn.focus();
    },
  };
}

async function previewRecover(form) {
  if (busyModalActive()) return; // shares the one-in-flight guard with Generate (#1363)
  const gen = serverGen;
  const container = $("#recover-preview", VIEW());
  const warns = $("#recover-warnings", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  const params = {};
  ["schema", "table", "pk", "since", "until", "event"].forEach((k) => { if (f[k] && f[k].trim()) params[k] = f[k].trim(); });
  // Part of mirroring recover's effective window (see below): without this the
  // preview would list events the generated script will not touch. The anchor
  // above is here for the same reason and matters more — unmirrored, the
  // preview would list every event in the clicked second while the script
  // reverses exactly one of them.
  const plpp = parseLatestPerRow(f.limit_per_pk);
  if (plpp === null) { renderError(container, "Latest per row must be a whole number, 0 or more."); return; }
  if (plpp > 0) params.limit_per_pk = String(plpp);
  // Mirror /api/recover's EFFECTIVE fetch window (#967) so the preview shows
  // the same events the undo script will actually reverse: newest-first, same
  // limit as recoverDefaultLimit in internal/console/api.go. Hardcoded here —
  // there is no Go-to-JS constant-sharing mechanism in this codebase.
  params.limit = "1000";
  params.order = "desc";
  const ctrl = new AbortController();
  const busy = openBusyModal(form, {
    title: "Previewing affected rows",
    errTitle: "Couldn't preview the rows",
    facts: recoverBusyFacts(params),
    onCancel: () => ctrl.abort(),
  });
  try {
    const data = await api("/api/events?" + new URLSearchParams(params).toString(), { signal: ctrl.signal });
    if (gen !== serverGen) { busy.close(false); return; }
    if (!container || !container.isConnected) {
      // The view was rebuilt mid-flight (palette navigation): don't render
      // into detached nodes — close and say what happened.
      busy.close(false);
      toast("The page changed while previewing. Run Preview rows again.");
      return;
    }
    clear(container);
    container.append(el("div", { class: "meta-line" }, el("b", { text: String(data.count) }), " affected event(s) · limit " + data.limit));
    const list = el("div", { class: "events" });
    const head = el("div", { class: "ev-head" });
    ["time (UTC)", "table", "type", "pk", "changed columns"].forEach((h) => head.append(el("span", { text: h })));
    list.append(head);
    const rows = el("div");
    buildEventRows(rows, data.events || []);
    list.append(rows);
    container.append(list);
    // Truncation warning (#967): more matches than the preview's limit means
    // the actual undo script (same limit, applied server-side) may cover more
    // events than are shown here.
    // Preview and Generate-undo share #recover-warnings, so this must render
    // the server's OWN warnings (the archive-exclusion notice among them)
    // alongside its truncation note -- not clear the box. Clearing it made the
    // caveat vanish the moment an operator adjusted a filter and re-previewed,
    // which is exactly when it is most load-bearing.
    const warnList = (data.warnings || []).slice();
    if (data.count >= data.limit) {
      warnList.push("Only the newest " + data.limit + " events are shown. The actual undo script may include more if you increase the limit.");
    }
    renderWarnings(warns, warnList);
    // The info half (#1365): /api/events carries `notes` (the archive-elision
    // record among them) — muted register, same container the undo response
    // fills, so a re-preview updates rather than duplicates it. Rendered on
    // the same success path as the warnings (past the isConnected guard), and
    // like #recover-warnings it is deliberately NOT cleared on a failed
    // preview (#1311: server notices must survive a re-preview).
    renderNotes($("#recover-notes", VIEW()), data.notes);
    busy.close(true); // success: back to the button that started the preview
  } catch (err) {
    // Cancel/ESC = a WITHDRAWN request: the modal is already closed and the
    // page keeps its pre-click state, previous preview included.
    if (err && err.name === "AbortError") return;
    if (gen !== serverGen) { busy.close(false); return; }
    // A FAILED preview must not leave the previous run's rows on screen as
    // if they answered the current filters; the error renders in the modal.
    // #recover-warnings is deliberately NOT cleared (#1311: server notices
    // must survive a re-preview).
    clear(container);
    busy.showError(err);
  }
}

// formatGeneratedIn renders the server's generation time as a trailing meta
// clause. Absence and zero are DIFFERENT answers and must not collapse: a
// server that predates generated_in_ms sends nothing (render nothing), while a
// recover served entirely from live partitions can legitimately round to 0 ms
// (render "<0.1s" — fast, measured). Hence the typeof check rather than a
// falsy one, which would misreport the fast case as unreported.
function formatGeneratedIn(ms) {
  if (typeof ms !== "number" || !isFinite(ms) || ms < 0) return "";
  // Coarse on purpose: an orientation signal, not a benchmark. Sub-100ms is
  // reported as a floor rather than a spuriously precise 0.04s.
  return " · generated in " + (ms < 100 ? "<0.1s" : (ms / 1000).toFixed(1) + "s");
}

async function generateUndo(form) {
  if (busyModalActive()) return; // one click = one generation (#1363)
  const gen = serverGen;
  const warns = $("#recover-warnings", VIEW());
  const out = $("#recover-out", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  const body = {};
  ["schema", "table", "pk", "since", "until", "event"].forEach((k) => { if (f[k] && f[k].trim()) body[k] = f[k].trim(); });
  if (!body.schema) { renderError(out, "Choose at least a schema to search."); return; }
  // Sent as a NUMBER: the field is an int on the wire, and a string would be
  // rejected by the decoder rather than silently ignored. Only when > 0 —
  // blank and 0 both mean "all", so neither needs to travel.
  const lpp = parseLatestPerRow(f.limit_per_pk);
  if (lpp === null) { renderError(out, "Latest per row must be a whole number, 0 or more."); return; }
  if (lpp > 0) body.limit_per_pk = lpp;
  const ctrl = new AbortController();
  const busy = openBusyModal(form, {
    title: "Generating undo SQL",
    errTitle: "Couldn't generate the undo SQL",
    facts: recoverBusyFacts(body),
    onCancel: () => ctrl.abort(),
  });
  try {
    const data = await api("/api/recover", { method: "POST", body, signal: ctrl.signal });
    if (gen !== serverGen) { busy.close(false); return; }
    if (!out || !out.isConnected) {
      // The view was rebuilt mid-flight (palette navigation): don't render
      // into detached nodes — close and say what happened.
      busy.close(false);
      toast("The page changed while generating. Run Generate undo SQL again.");
      return;
    }
    renderWarnings(warns, data.warnings);
    renderNotes($("#recover-notes", VIEW()), data.notes);
    lastSQL = data.sql || "";
    clear(out);
    // When the target is auto-detected as a foreign-key parent, the script also
    // repairs the child rows InnoDB changed below the binlog: rows a delete
    // cascade removed, references it cleared, and references an ON UPDATE
    // cascade re-pointed. Surface all three so the larger script isn't a
    // surprise — and so a script that is ENTIRELY reference repairs never shows
    // a bare "0 child row(s)" (coverage caveats, if any, are in the warnings
    // above).
    //
    // cascade_detected implies at least one repair: the server falls back to the
    // plain script/response when the synthesis produced nothing (an UPDATE undo
    // that turned out not to have moved a referenced key), so `parts` is never
    // empty here and there is no "nothing was repaired" branch to render.
    if (data.cascade_detected) {
      const victims = data.victim_count || 0;
      const setNulls = data.set_null_count || 0;
      const keyRestores = data.key_restore_count || 0;
      const parts = [];
      if (victims) parts.push("restores " + victims + " related row(s) that MySQL deleted automatically");
      if (setNulls) parts.push("fixes " + setNulls + " reference(s) that were cleared automatically");
      if (keyRestores) parts.push("fixes " + keyRestores + " reference(s) that MySQL re-pointed automatically");
      // One line (#1950): the drawing's child stack says the rest.
      out.append(el("div", { class: "ctx-banner" },
        el("span", { class: "badge b-baseline", text: "CASCADE" }),
        el("div", { class: "ctx-main" },
          el("span", { class: "ctx-detail", text: "This script also " + parts.join(", ") + "." }))));
    }
    // The drawing (#1950): only when the change was picked on Events, where
    // its row images are; a free search reverses many events and has the
    // preview list instead.
    const ctx = pendingRecover;
    if (ctx && (ctx.before || ctx.after) && form.elements.event && form.elements.event.value) out.append(undoDrawing(ctx, data));
    const meta = (data.cascade_detected
      ? data.statement_count + " statement(s) · " + (data.victim_count || 0) + " cascade child row(s) · " +
        (data.set_null_count || 0) + " SET NULL restore(s) · " + (data.key_restore_count || 0) + " FK restore(s)"
      : data.statement_count + " statement(s) from " + data.row_count + " event(s)")
      + formatGeneratedIn(data.generated_in_ms);
    out.append(codePanel(lastSQL, meta));
    // Success: close the busy dialog and land keyboard focus on the result —
    // the reversal.sql panel header (#1363).
    busy.close(false);
    const head = $("#sql-panel .code-head", out);
    if (head) head.focus();
  } catch (err) {
    // Cancel/ESC = a WITHDRAWN request: the modal is already closed and the
    // page keeps its pre-click state, previous result included.
    if (err && err.name === "AbortError") return;
    if (gen !== serverGen) { busy.close(false); return; }
    // A FAILED generation is different: the previous run's script must not
    // stay on screen with Copy/Download live — those bytes answer a filter
    // nobody named. Clear the result and the download buffer, then render
    // the error IN the modal (with a Dismiss), never as a silently vanished
    // overlay.
    clear(warns);
    // The info notes are cleared with the warnings (#1365): a lingering
    // "nothing is missing here" would caption a script that no longer exists.
    clear($("#recover-notes", VIEW()));
    clear(out);
    lastSQL = "";
    busy.showError(err);
  }
}

function codePanel(sql, metaLabel) {
  const panel = el("div", { class: "codepanel", id: "sql-panel" });
  // tabindex -1: programmatic focus target — the busy modal moves keyboard
  // focus here when a generation succeeds (#1363).
  const head = el("div", { class: "code-head", tabindex: "-1" });
  head.append(icon("file"));
  const lbl = el("span", { class: "lbl" }, el("b", { text: "reversal.sql" }), " · " + (metaLabel || "read-only preview"));
  head.append(lbl);
  head.append(el("span", { class: "spacer" }));
  head.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Copy", onclick: copySQL }));
  head.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Download", onclick: downloadSQL }));
  panel.append(head);
  panel.append(el("pre", { class: "code", text: sql }));
  // The card's footer (#1950): the promise the page used to open with.
  panel.append(el("div", { class: "code-foot", text: "Nothing runs on its own. Copy or download the script and apply it yourself after review." }));
  return panel;
}
function copySQL() {
  navigator.clipboard.writeText(lastSQL).then(() => toast("SQL copied to clipboard"), () => toastError("Copy failed."));
}
function downloadSQL() { downloadBlob("dbtrail-undo.sql", lastSQL, "application/sql"); }

// UNDO_EMPTY_ART: a row now, an arrow back, the row as it was, drawn
// (static, so svgEl is right here).
const UNDO_EMPTY_ART = `<svg viewBox="0 0 160 72" aria-hidden="true"><rect x="2" y="14" width="58" height="44" rx="8" fill="var(--surface)" stroke="var(--line)"/><rect x="12" y="26" width="26" height="5" rx="2.5" fill="var(--ink-4)"/><rect x="12" y="40" width="38" height="5" rx="2.5" fill="var(--diff-del-bg)" stroke="var(--diff-del)"/><rect x="100" y="14" width="58" height="44" rx="8" fill="var(--surface)" stroke="var(--line)"/><rect x="110" y="26" width="26" height="5" rx="2.5" fill="var(--ink-4)"/><rect x="110" y="40" width="38" height="5" rx="2.5" fill="var(--diff-add-bg)" stroke="var(--diff-add)"/><path d="M64 36h30" stroke="var(--ink-3)" stroke-width="2"/><path d="M88 30l7 6-7 6" fill="none" stroke="var(--ink-3)" stroke-width="2"/></svg>`;

function undoEmptyState() {
  const box = el("div", { class: "empty undo-empty" });
  box.append(el("div", { class: "empty-art", "aria-hidden": "true" }, svgEl(UNDO_EMPTY_ART)));
  box.append(el("h3", { text: "Pick a change to undo" }));
  box.append(el("p", { text: "Undo on any row in Events, or fill in the target above. The script that puts the row back appears here." }));
  return box;
}

// UNDO_VERB: what the undo of each change does, the word the arrow carries.
const UNDO_VERB = { INSERT: "DELETE", UPDATE: "UPDATE back", DELETE: "re-INSERT" };

// undoDrawing draws the affected row before and after the undo (#1950): the
// row as it is now (the change's after-image), the arrow with what the
// script does, and the row as the script leaves it (the before-image). A
// deleted row is a dashed empty tile. Changed columns come first and wear
// the diff tokens; the rest follow in the page's ink, capped, so the drawing
// stays a glance. In the cascade case the child rows the script also puts
// back stand beside the result as a stack. Built with el(); the text
// alternative says the same in words.
function undoDrawing(ctx, data) {
  const type = String(ctx.type || "").toUpperCase();
  const before = ctx.before || {}, after = ctx.after || {};
  const changed = new Set(ctx.changed || []);
  const whole = type === "INSERT" || type === "DELETE";
  const cols = Array.from(new Set([...Object.keys(before), ...Object.keys(after)])).sort();
  const ordered = cols.filter((c) => changed.has(c)).concat(cols.filter((c) => !changed.has(c)));
  const shown = ordered.slice(0, 5), more = ordered.length - shown.length;
  const short = (v) => { const s = valueToString(v); return s.length > 22 ? s.slice(0, 21) + "\u2026" : s; };
  const tile = (title, row, gone, tone) => {
    const t = el("div", { class: "undo-tile" + (gone ? " undo-gone" : "") });
    t.append(el("div", { class: "undo-tile-h", text: title }));
    if (gone) { t.append(el("div", { class: "undo-none", text: "no row" })); return t; }
    shown.forEach((c) => {
      const hot = whole || changed.has(c);
      t.append(el("div", { class: "undo-cell" + (hot ? " " + tone : "") },
        el("span", { class: "undo-k", text: c }), el("span", { class: "undo-v", text: short(row[c]), title: valueToString(row[c]) })));
    });
    if (more > 0) t.append(el("div", { class: "undo-more", text: "+" + more + " more column" + (more === 1 ? "" : "s") }));
    return t;
  };
  const now = tile("now", after, type === "DELETE", "undo-del");
  const then = tile("after the undo", before, type === "INSERT", "undo-add");
  const arrow = el("div", { class: "undo-arrow" }, el("span", { class: "undo-verb", text: UNDO_VERB[type] || "undo" }), el("span", { class: "undo-line", "aria-hidden": "true" }));
  const fig = el("div", { class: "undo-draw" }, now, arrow, then);
  const kids = data && data.cascade_detected ? (data.victim_count || 0) : 0;
  if (kids) {
    const stack = el("div", { class: "undo-kids" });
    for (let i = 0; i < Math.min(kids, 3); i++) stack.append(el("span", { class: "undo-kid" }));
    stack.append(el("span", { class: "undo-kids-t", text: "+" + kids + " related row" + (kids === 1 ? "" : "s") + " MySQL deleted, re-inserted" }));
    fig.append(stack);
  }
  const said = whole ? "" : shown.filter((c) => changed.has(c)).map((c) => c + " " + short(after[c]) + " back to " + short(before[c])).join(", ");
  fig.setAttribute("role", "img");
  fig.setAttribute("aria-label", "Undo of the " + type + " on " + ctx.schema + "." + ctx.table + " pk " + ctx.pk + ": " + (UNDO_VERB[type] || "undo") + (said ? ", " + said : "") + (kids ? "; " + kids + " related rows re-inserted" : "") + ".");
  return fig;
}

// Bridge: an event → Recover, scoped to that row up to the event's timestamp.
function undoEvent(e) {
  pendingRecover = {
    schema: e.schema_name, table: e.table_name, pk: e.pk_values,
    type: e.event_type, time: e.event_timestamp,
    // The row images, for the drawing above the script (#1950).
    before: e.row_before || null, after: e.row_after || null, changed: e.changed_columns || [],
    // The server's own identity token for this row, echoed back verbatim
    // (eventDTO.Anchor). Never rebuilt from `time`: that one is second-
    // granular and offset-less, which is exactly the ambiguity the anchor
    // exists to remove (#1411).
    anchor: e.anchor,
  };
  navigate("recover");
}

// ── Row state, inside Restore (#1298) ────────────────────────────────────────

// stateSection is the folded-in Time-travel half: the SAME target the undo
// form already carries (schema/table/pk), plus an instant. Two views that used
// to be two destinations with identical forms — the operator read a timestamp
// off Events, retyped the target into one, looked, then retyped it into the
// other. Here the target is entered once.
//
// Gated twice, for two different reasons. capsCache.reconstruct is a per-server
// capability (no baseline configured, nothing to reconstruct from), and the
// panel says so rather than vanishing — an operator who cannot find it has no
// way to learn that a baseline is what is missing. The permission gate mirrors
// the nav's [data-perm]: the server's 403 is the real boundary, this only
// spares a scoped session a control that would always error.
function stateSection(form) {
  const wrap = el("section", { class: "state-section", "data-perm": "reconstruct:execute" });
  wrap.append(el("h2", { class: "state-title", text: "Row at a point in time" }));

  if (!capsCache.reconstruct) {
    wrap.append(el("p", { class: "state-note", text:
      "Configure a snapshot for this server to see a row's earlier state. Undo SQL below works without one; it reverses recorded changes, so it cannot show a row nothing has touched." }));
    return wrap;
  }

  wrap.append(el("p", { class: "state-note", text:
    "Uses the schema, table and PK above. Shows the row as of an instant: your latest snapshot plus every change since." }));

  const bar = el("div", { class: "state-bar" });
  const at = fieldDateInput("At (UTC)", "state_at", "md", "now");
  bar.append(at);
  bar.append(el("button", { class: "btn btn-ghost", type: "button", text: "Show state",
    onclick: () => runState(form, false) }));
  bar.append(el("button", { class: "btn btn-ghost", type: "button", text: "Show history",
    onclick: () => runState(form, true) }));
  wrap.append(bar);
  wrap.append(el("div", { id: "state-warnings", class: "warnings" }));
  wrap.append(el("div", { id: "state-out" }));
  return wrap;
}

// runState reads the target from the undo form — one target, two questions.
async function runState(form, history) {
  const gen = serverGen;
  const out = $("#state-out", VIEW());
  const warns = $("#state-warnings", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  const atField = $('[name="state_at"]', VIEW());
  if (!f.schema || !f.table || !f.pk) {
    clear(warns);
    renderError(out, "Schema, table and PK are all required; fill them in above.");
    return;
  }
  const params = { schema: f.schema, table: f.table, pk: f.pk };
  const atVal = atField && atField.value.trim();
  if (atVal) params.at = atVal;
  if (history) params.history = "true";
  clear(out);
  clear(warns);
  try {
    const data = await api("/api/reconstruct?" + new URLSearchParams(params).toString());
    if (gen !== serverGen) return;
    // Into a modal, not between the filter form and the reversal panel
    // (#1405). The output is unbounded — a busy row's history is a long table
    // — and it is consulted on the way to the script, so rendering it inline
    // pushed the artifact the page exists to produce off screen.
    //
    // The WARNINGS come with it. They used to render into a sibling container
    // that would now sit behind the scrim, and a reconstruct can return
    // stale_baseline or a capture-gap caveat: an operator reading a row state
    // with the caveat hidden behind the dialog is worse off than before this
    // change, not better.
    const dlg = openModal({
      class: "state-modal",
      label: history ? "Row history" : "Row at a point in time",
      title: f.schema + "." + f.table + " · pk " + f.pk,
      desc: [history
        ? "Every recorded change to this row, newest last."
        : "The row as of " + (atVal || "now") + " UTC: your latest snapshot plus every change since."],
    });
    const mwarns = el("div", { class: "warnings" });
    dlg.body.append(mwarns);
    renderWarnings(mwarns, data.warnings);
    const mnotes = el("div", { class: "notes" });
    dlg.body.append(mnotes);
    renderNotes(mnotes, data.notes);
    if (history) renderTimeline(dlg.body, data, dlg.close);
    else {
      renderStateAt(dlg.body, data);
      // The action retargets the form UNDERNEATH this dialog, so the dialog
      // has to get out of the way — leaving it open hides the change it just
      // made, which is the same complaint that moved this output in here.
      const action = restoreToStateAction(form, data, dlg.close);
      // Empty on a not-found/deleted row, and an empty footer is a stray
      // divider with padding under it, so only mount one that has a button.
      if (action.firstChild) dlg.panel.append(action);
    }
  } catch (err) {
    if (gen !== serverGen) return;
    // The fetch failed rather than the form being wrong, so the answer belongs
    // where the answer would have been. A 422 gap refusal is a result about
    // the request, not a hint about the fields.
    const dlg = openModal({
      class: "state-modal",
      label: history ? "Row history" : "Row at a point in time",
      title: f.schema + "." + f.table + " · pk " + f.pk,
    });
    renderError(dlg.body, err);
  }
}

// clearUndoAnchor retires the single-event selection and the banner that
// describes it, leaving the visible filters untouched.
//
// Shared by the target-edit listeners and available to anything else that
// widens the scope. It does NOT touch `until`: that is the scope the operator
// is left with, and blanking it here would silently widen a retargeted search
// to the whole index.
function clearUndoAnchor(form) {
  if (!form.elements.event || !form.elements.event.value) return;
  form.elements.event.value = "";
  pendingRecover = null;
  const banner = document.getElementById("undo-ctx-banner");
  if (banner) banner.remove();
}

// aimUndoAtInstant points the undo window at the state as of `at`: reverse
// everything AFTER that instant, so the row lands exactly on what was shown.
//
// since = at + 1s is exact, not approximate. reconstruct applies events
// timestamped <= at; recover reverses events timestamped >= since and leaves
// the row before the earliest of them. event_timestamp is DATETIME(0), so no
// event can hide between at and at+1s — while passing `at` itself would be off
// by every event sharing that second, and these indexes routinely carry dozens
// from a single write burst.
//
// Clearing `until` matters just as much: a leftover upper bound (the Undo
// bridge from Events sets one) would drop the newest damage out of the window
// and quietly restore to the wrong place.
function aimUndoAtInstant(form, at) {
  const since = shiftSeconds(at, 1);
  if (!since) { toastError("Could not read the selected instant."); return; }
  form.elements.since.value = since;
  form.elements.until.value = "";
  // Cleared for the same reason `until` is, and it became necessary with the
  // same change that made `until` matter here: since #1404 the Undo bridge
  // prefills limit_per_pk = 1, so an operator who used Undo first would arrive
  // with a leftover cap. This action reverses EVERY change after `at` — that
  // is what makes the row land on the state shown — and a cap of 1 would
  // reverse only the newest of them and land it somewhere else entirely,
  // silently, with the button still naming the state it did not produce.
  form.elements.limit_per_pk.value = "";
  // The undo anchor goes for the strongest version of the same reason: it
  // names ONE event, and this action reverses every change after `at`. Left
  // set, the generated script would reverse exactly the event the operator
  // clicked minutes ago in Events and nothing else, while the button that
  // produced it named a completely different outcome.
  form.elements.event.value = "";
  // …and retire the banner that describes the scope just replaced. It asserts
  // that exactly the clicked event is reversed, and the lines above make that
  // false: this action reverses EVERYTHING after `at`. (Before #1411 the same
  // contradiction ran through the cap — the banner then read "Latest per row is
  // set to 1", which the clear above falsified.) The stale-label half of the
  // same bug, one level up from the fields.
  //
  // By ID, not by `.ctx-banner`: generateUndo appends a second node with that
  // class into #recover-out for the cascade notice, and an unscoped remove
  // would take whichever came first.
  pendingRecover = null;
  const staleBanner = document.getElementById("undo-ctx-banner");
  if (staleBanner) staleBanner.remove();
  previewRecover(form);
  form.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "start" });
  toast("Undo window set to everything after " + at + " UTC");
}

// restoreToStateAction is the bridge that makes the two halves one errand:
// the state on screen becomes the undo window that produces it.
//
// The arithmetic is exact, not approximate. reconstruct applies events
// timestamped <= at; recover reverses events timestamped >= since and leaves
// the row before the earliest of them. Setting since = at + 1s therefore
// reverses precisely the events AFTER `at`, landing the row on the state shown
// — event_timestamp is DATETIME(0), so no event can hide between at and at+1s.
//
// Passing `at` itself would be off by every event sharing that second, and
// these indexes routinely carry dozens (a single write burst stamps them all
// alike). Until is cleared for the same reason it is set by the Undo bridge:
// a leftover upper bound would silently drop the newest damage from the window.
function restoreToStateAction(form, data, onDone) {
  const row = el("div", { class: "state-actions" });
  if (!data.found) return row;
  row.append(el("button", {
    class: "btn btn-primary", type: "button", text: "Restore to this state",
    title: "Reverse every change after " + data.at + " UTC, leaving the row exactly as shown",
    onclick: () => {
      // Close BEFORE retargeting: aimUndoAtInstant scrolls the form into view
      // and previews the rows, and both are invisible behind a scrim.
      if (onDone) onDone();
      aimUndoAtInstant(form, data.at);
    },
  }));
  row.append(el("span", { class: "state-actions-note", text:
    "Sets the undo window to every change after this instant. Review the rows, then generate the SQL." }));
  return row;
}

// useTimelineInstant fills the At field from a timeline node and re-runs the
// state view. This is what replaces typing a timestamp: the operator points at
// the change that broke things instead of transcribing its time from Events.
function useTimelineInstant(at) {
  const field = $('[name="state_at"]', VIEW());
  const form = document.getElementById("recover-form");
  if (!field || !form) return;
  field.value = at;
  runState(form, false);
  field.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "center" });
}

// shiftSeconds adds n seconds to a "YYYY-MM-DD HH:MM:SS" UTC stamp, returning
// the same shape. Parsed as UTC explicitly: bare "YYYY-MM-DD HH:MM:SS" is
// LOCAL time to Date(), which would shift the window by the browser's offset.
function shiftSeconds(stamp, n) {
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/.exec(String(stamp || ""));
  if (!m) return "";
  const t = Date.parse(m[1] + "T" + m[2] + "Z");
  if (Number.isNaN(t)) return "";
  return new Date(t + n * 1000).toISOString().slice(0, 19).replace("T", " ");
}

// ── Time-travel (reconstruct) ─────────────────────────────────────────────────

function renderTimetravel(params) {
  // Merged into Restore (#1298). Rewrite the URL then re-dispatch: calling
  // navigate(push=false) here would leave the URL on /timetravel and re-resolve
  // straight back into this guard (infinite recursion).
  history.replaceState({}, "", "/recover");
  renderRoute();
  return;
  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub" },
    "See what a row looked like at any moment in the past: your latest full snapshot plus every change since. Pick a row and a time to see its value then, or see its entire history.");
  v.append(pageHead("Time-travel", sub));

  const form = el("form", { class: "filters", id: "tt-form" });
  form.append(fieldSelect("Schema", "schema", "md", true, false, null, "— select —"));
  form.append(fieldSelect("Table", "table", "md", false, true, null, null, true));
  form.append(fieldInput("PK", "pk", "sm", "42 or 42|7", true));
  form.append(fieldDateInput("As of (UTC)", "at", "md", "YYYY-MM-DD HH:MM:SS (default: now)"));
  const gapsField = el("div", { class: "field", style: "justify-content:flex-end" },
    el("label", { class: "check" }, el("input", { type: "checkbox", name: "allow_gaps" }), el("span", { text: "Continue even if some history is missing" })));
  form.append(gapsField);
  const actions = el("div", { class: "filter-actions" });
  actions.append(el("button", { class: "btn btn-ghost", type: "button", text: "Full history", onclick: () => runReconstruct(form, true) }));
  actions.append(el("button", { class: "btn", type: "submit", text: "Value at that time" }));
  form.append(actions);
  v.append(form);

  v.append(el("div", { id: "tt-warnings", class: "warnings" }));
  v.append(el("div", { id: "tt-notes", class: "notes" }));
  const out = el("div", { id: "tt-out" });
  out.append(el("div", { class: "tt-meta", text: "Fill in a row above and pick a button to see what it looked like." }));
  v.append(out);

  form.addEventListener("submit", (e) => { e.preventDefault(); runReconstruct(form, false); });
  wireSchemaCascade(form);
  populateSchemas(form);
  viewEnter();
}

// runReconstruct is the standalone Time-travel view, NOT the Restore view's
// embedded panel. It renders inline and keeps doing so: #1405 moved runState
// into a dialog because its output was wedged between a filter form and the
// reversal panel it was pushing off screen. Here the reconstructed state IS
// the page, with nothing below it to displace, so a dialog would add a scrim
// and buy nothing.
async function runReconstruct(form, history) {
  const gen = serverGen;
  const warns = $("#tt-warnings", VIEW());
  const ttNotes = $("#tt-notes", VIEW());
  const out = $("#tt-out", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  if (!f.schema || !f.table || !f.pk) { clear(warns); renderNotes(ttNotes, []); renderError(out, "Schema, table, and PK are all required."); return; }
  const params = { schema: f.schema, table: f.table, pk: f.pk };
  if (f.at && f.at.trim()) params.at = f.at.trim();
  if (form.elements.allow_gaps && form.elements.allow_gaps.checked) params.allow_gaps = "true";
  if (history) params.history = "true";
  try {
    const data = await api("/api/reconstruct?" + new URLSearchParams(params).toString());
    if (gen !== serverGen) return;
    renderWarnings(warns, data.warnings);
    renderNotes(ttNotes, data.notes);
    clear(out);
    if (history) renderTimeline(out, data);
    else renderStateAt(out, data);
  } catch (err) {
    if (gen !== serverGen) return;
    // Sweep BOTH registers (#1365, same rule the events catch states): a
    // lingering "nothing is missing here" elision note beside an error
    // belongs to a different query and reads as reassurance about this one.
    clear(warns); renderNotes(ttNotes, []); renderError(out, err);
  }
}

function reconstructMeta(data, label) {
  return el("div", { class: "meta-line" },
    el("b", { text: data.schema + "." + data.table + " pk=" + data.pk }),
    " · " + label + " · snapshot " + data.baseline_time + " · " + data.event_count + " event(s)",
    tzChip());
}

function renderStateAt(container, data) {
  container.append(reconstructMeta(data, "as of " + data.at));
  if (!data.found) { container.append(el("div", { class: "deleted-note", text: "No row with this primary key existed at or before the selected time." })); return; }
  if (data.deleted) { container.append(el("div", { class: "deleted-note", text: "Row was deleted as of " + data.at + " UTC." })); return; }
  container.append(stateTable(data.state || {}));
}

function stateTable(state) {
  const table = el("table", { class: "statetable" });
  Object.keys(state).forEach((k) => {
    table.append(el("tr", {}, el("th", { text: k }), el("td", { text: valueToString(state[k]) })));
  });
  return table;
}

// onDone, when given, is the caller's dismissal — the timeline is rendered
// into a dialog from runState and inline from runReconstruct, and only the
// first has anything to close. Each node carries its OWN restore button, so
// unlike the state panel there is no single footer to pin outside the scroll;
// the button travels with the node it names, which is where it belongs.
function renderTimeline(container, data, onDone) {
  const entries = data.history || [];
  container.append(reconstructMeta(data, "history through " + data.at + " · " + entries.length + " state(s)"));
  if (!entries.length) { container.append(el("div", { class: "deleted-note", text: "No history for this primary key in the time range that's been indexed." })); return; }

  const tl = el("div", { class: "timeline", id: "timeline" });
  let prev = null;
  entries.forEach((e) => {
    const node = el("div", { class: "tl-node" });
    const kind = e.source === "baseline" ? "baseline" : e.source.toLowerCase();
    node.append(el("span", { class: "tl-dot " + kind }));
    const head = el("div", { class: "tl-head" });
    head.append(el("span", { class: "badge " + (e.source === "baseline" ? "b-baseline" : badgeClass(e.source)), text: e.source }));
    head.append(tsSpan("tl-time", e.time));
    node.append(head);

    const body = el("div", { class: "tl-body" });
    if (e.deleted || !e.state) {
      body.append(el("span", { class: "pair" }, el("span", { class: "pk", text: "(row deleted)" })));
    } else {
      const changed = new Set();
      if (prev) for (const k of Object.keys(e.state)) if (valueToString(e.state[k]) !== valueToString(prev[k])) changed.add(k);
      Object.keys(e.state).forEach((k) => {
        body.append(el("span", { class: "pair" },
          el("span", { class: "pk", text: k + "=" }),
          el("span", { class: "pv" + (e.source !== "baseline" && changed.has(k) ? " changed" : ""), text: valueToString(e.state[k]) })));
      });
      prev = e.state;
    }
    node.append(body);

    // Both actions are "pick this moment" — the timeline is how an operator
    // chooses an instant without leaving to read one off Events and retype it.
    //
    // The restore button used to route through undoEvent, which sets `until`:
    // that reverses everything UP TO this point and lands the row before its
    // whole recorded history — the opposite end from the state the button is
    // pointing at. It shares the state panel's exact bridge now, so the label
    // and the SQL finally agree.
    const acts = el("div", { class: "tl-actions" });
    acts.append(el("button", {
      class: "btn btn-sm btn-ghost tl-use", type: "button", text: "Use this moment",
      title: "Set the At field above to " + e.time + " UTC",
      onclick: () => useTimelineInstant(e.time),
    }));
    if (e.source !== "baseline") {
      acts.append(el("button", {
        class: "btn btn-sm tl-restore", type: "button", text: "Restore to this state",
        title: "Reverse every change after " + e.time + " UTC, leaving the row as this entry shows it",
        onclick: () => {
          // Same reason the state panel's action closes first: aimUndoAtInstant
          // scrolls the form into view and previews the rows, and both are
          // invisible behind a scrim. This one used to be carried by accident —
          // previewRecover's busy dialog happens to replace the mount — which
          // held right up until previewRecover took one of its two early
          // returns and left the error rendering behind the scrim.
          if (onDone) onDone();
          const form = document.getElementById("recover-form");
          if (form) aimUndoAtInstant(form, e.time);
        },
      }));
    }
    node.append(acts);
    tl.append(node);
  });
  container.append(tl);

  // "draws itself" — progressive-enhancement reveal (decorative).
  //
  // The stagger is gated here and not only in CSS, because the CSS guard
  // removes the SMOOTHNESS while this loop still schedules the position
  // change: with the transition gone, a 50-row timeline snapped one node at a
  // time across ~2.8s of content shifting under the reader — a jumpier version
  // of the motion the preference asked to remove. Under reduce every node
  // arrives at once and nothing moves.
  requestAnimationFrame(() => {
    tl.classList.add("drawn");
    const nodes = $all(".tl-node", tl);
    if (prefersReducedMotion()) { nodes.forEach((n) => n.classList.add("in")); return; }
    nodes.forEach((n, i) => setTimeout(() => n.classList.add("in"), 60 + i * 55));
  });
}

// ── Status ─────────────────────────────────────────────────────────────────

// flowBehind: an age in seconds as the short figure the arrow carries.
function flowBehind(sec) {
  if (typeof sec !== "number" || !isFinite(sec) || sec < 0) return "";
  if (sec < 60) return Math.round(sec) + " s";
  if (sec < 3600) return Math.round(sec / 60) + " min";
  if (sec < 172800) return (sec / 3600).toFixed(sec < 36000 ? 1 : 0) + " h";
  return Math.round(sec / 86400) + " d";
}

// flowSpan: how far back the index reaches, from its earliest to its latest
// event, as a short figure.
function flowSpan(from, to) {
  const a = Date.parse(String(from || "").replace(" ", "T") + (/(Z|[+-]\d\d:\d\d)$/.test(String(from || "")) ? "" : "Z"));
  const b = Date.parse(String(to || "").replace(" ", "T") + (/(Z|[+-]\d\d:\d\d)$/.test(String(to || "")) ? "" : "Z"));
  if (isNaN(a) || isNaN(b) || b < a) return "";
  return flowBehind((b - a) / 1000);
}

// statusFlowModel is the capture path as the Status read sees it (#1950):
// the same seven pieces the Overview draws, each station saying its live
// state. It replaced the page's opening sentence and the green "no gaps" box:
// what they said, the stations now show, and the two claims the old box had
// to keep apart in words (no gaps inside what was captured; whether capture
// is running) sit on two different pieces, the DBTrail station and the
// capture arrow, so neither can be read as the other. The permanent-loss
// alarm stays a red box below: an alarm with a stamp is not a state.
// `copy` is the /api/baselines answer, or { unavailable: true } when that
// read failed: the snapshot list the Overview and the Snapshots page read.
// /api/status carries no snapshot fields in the console (only the CLI's
// status --baseline-dir fills baselines/baseline_staleness), so reading them
// there drew "no snapshot yet" on every server, snapshots or not (#1950).
function statusFlowModel(data, capacity, caps, copy) {
  data = data || {};
  caps = caps || {};
  copy = copy || { unavailable: true };
  const pg = caps.source === "postgresql";
  const stream = data.stream || null;
  const cov = data.coverage || {};
  const arch = data.archives || null;
  const servers = (data.servers || []).filter((s) => !s.decommissioned_at);
  const srv = servers[0] || null;
  const piece = (title, tone, line, sub, extra) => Object.assign({ title, tone, line: line || "", sub: sub || "" }, extra || {});
  const n = (x) => Number(x || 0).toLocaleString();
  const pieces = [];

  // 0. the source
  pieces.push(piece(pg ? "Your PostgreSQL" : "Your MySQL", "none",
    srv ? srv.host + ":" + srv.port : "source not recorded",
    servers.length > 1 ? servers.length + " sources in this index" : "", { mono: !!srv }));

  // 1. capture: liveness, from the daemon's checkpoint
  const label = pg ? "WAL" : "binlog";
  if (data.stream_error) {
    pieces.push(piece(label, "warn", "state could not be read", data.stream_error.error || ""));
  } else if (!stream) {
    const files = (data.files || []).length;
    pieces.push(piece(label, "off", "no live capture", files ? files + " file" + (files === 1 ? "" : "s") + " indexed" : ""));
  } else {
    const f = stream.freshness || {};
    const tone = f.status === "current" || f.status === "idle" ? "ok" : f.status === "stalled" ? "bad" : "warn";
    const word = f.status === "current" ? "capturing" : f.status === "idle" ? "idle, nothing new" : f.status === "stalled" ? "stalled" : "liveness not known";
    const pos = pg ? (stream.binlog_file ? "LSN " + stream.binlog_file : "")
      : (stream.binlog_file ? stream.binlog_file + ":" + stream.binlog_position : (stream.mode || ""));
    pieces.push(piece(label, tone, word, pos, { big: flowBehind(f.checkpoint_age_seconds), mono: true }));
  }

  // 2. DBTrail: what the index holds, and whether it holds all of it
  const events = cov.total_events !== undefined ? cov.total_events : data.total_events_estimate;
  const parts = (data.partitions || []).length;
  const held = n(events) + " events · " + parts + " hour" + (parts === 1 ? "" : "s");
  const ch = stream && stream.capture_health;
  let dbt;
  if (stream && stream.gap_lost) dbt = piece("DBTrail", "bad", "events permanently lost", held);
  else if (ch && ch.status === "degraded" && !ch.acknowledged) dbt = piece("DBTrail", "warn", n(ch.total_skipped) + " changes skipped", held);
  else if (stream && stream.continuity && stream.continuity.status === "ok") dbt = piece("DBTrail", "ok", "no gaps", held);
  else if (stream) dbt = piece("DBTrail", "none", "gaps not checkable", held);
  else dbt = piece("DBTrail", "none", held, "");
  pieces.push(dbt);

  // 3. history: how far back the copy can go
  const span = flowSpan(cov.earliest_event, cov.latest_event);
  const ret = data.retention || {};
  const bounds = cov.earliest_event && cov.latest_event ? utcBare(cov.earliest_event) + " to " + utcBare(cov.latest_event).slice(11) : "";
  pieces.push(piece("history", "none", span ? "of history" + (ret.retain ? ", kept " + ret.retain : "") : (cov.earliest_event ? "" : "nothing indexed yet"),
    bounds, { big: span, mono: true }));

  // 4. the copy: snapshots (from /api/baselines, see above) and archives.
  // A failed read says so; it is never "no snapshot yet". The grade is the
  // listing's own headline, the one the Snapshots page shows.
  const snaps = copy.snapshots || [];
  const st = copy.staleness || "";
  const archLine = arch ? n(arch.total_files) + " archive file" + (arch.total_files === 1 ? "" : "s") + (arch.total_size_human ? " · " + arch.total_size_human : "")
    : (data.archives_error ? "archives could not be read" : "no archives yet");
  if (copy.unavailable) {
    pieces.push(piece("Your copy", "warn", "snapshots could not be read", archLine));
  } else if (snaps.length) {
    // A listing that could not read every location is a subset: at least a warning.
    const tone = st === "broken" ? "bad" : (st === "aging" || st === "unknown" || copy.incomplete) ? "warn" : st === "ok" ? "ok" : "none";
    const word = st === "ok" ? "up to date" : st === "aging" ? "aging" : st === "broken" ? "behind" : st === "unknown" ? "staleness not evaluable" : "";
    const count = snaps.length + (copy.truncated ? "+" : "") + " snapshot" + (snaps.length === 1 && !copy.truncated ? "" : "s");
    const newest = flowHHMM(snaps[0].time);
    pieces.push(piece("Your copy", tone, count + (word ? ", " + word : ""),
      (newest ? "newest " + newest + " · " : "") + archLine));
  } else {
    pieces.push(piece("Your copy", "off", "no snapshot yet", archLine));
  }

  // 5. SQL: how the copy is read
  pieces.push(piece("SQL", "none", caps.reconstruct ? "time travel on" : "", ""));

  // 6. the reader
  pieces.push(piece("DuckDB", "none", caps.views ? DUCKDB_VIEWS_FILE + " ready" : "Parquet files", ""));
  return { pieces };
}

// statusFlow draws the strip with a text alternative: every station's title
// and state, so a screen reader hears what the drawing shows.
function statusFlow(model) {
  const alt = model.pieces.map((p) => p.title + ": " + [p.big, p.line, p.sub].filter(Boolean).join(", ")).join("; ") + ".";
  const sec = el("section", { class: "flow flow-static", role: "img", "aria-label": "The capture path. " + alt });
  sec.append(flowGrid(model.pieces, {}));
  return sec;
}

async function renderStatus() {
  const gen = serverGen, vgen = viewGen;
  viewLoading();
  let data, capacity, telemetry, copy;
  // The index-disk read degrades independently (as the Storage panels do): a
  // failed /api/capacity renders its own note inside the card, never blanking
  // the health page it sits on. Telemetry (#1867, from the dissolved This
  // daemon page) the same way, and not asked at all for a session that may
  // not read settings: a 403 would be a red card about a setting this reader
  // was never meant to see.
  const asErr = (err) => ({ error: (err && err.message) || String(err) });
  try {
    // The snapshot list, as the Overview reads it: bounded, because a slow
    // S3 listing must not hold the whole page, and a failure is drawn as one.
    [data, capacity, telemetry, copy] = await Promise.all([api("/api/status"), api("/api/capacity").catch(asErr),
      sessionMay("settings:read") ? api("/api/telemetry").catch(asErr) : Promise.resolve(null),
      apiWithin("/api/baselines", OV_REQUEST_MS).then((d) => d || {}, () => ({ unavailable: true }))]);
  }
  catch (err) { if (gen !== serverGen || vgen !== viewGen) return; const v = VIEW(); clear(v); v.append(pageHead("Status", null)); renderError(v, err); return; }
  if (gen !== serverGen || vgen !== viewGen) return;
  updateSideMeta(data);

  const v = VIEW(); clear(v);
  // The zone once, for the page (#1950): every timestamp below is UTC, and
  // one "as of" for the read that painted all of it. Refresh is this view's
  // one action, and its primary.
  v.append(pageHead("Status", null, [
    el("span", { class: "page-asof" }, tzChip(), el("span", { class: "cov-asof", text: "as of " + nowClock().replace(" UTC", "") })),
    el("button", { class: "btn btn-primary btn-sm", type: "button", text: "Refresh", onclick: () => renderStatus() }),
  ]));
  v.append(statusFlow(statusFlowModel(data, capacity, capsCache, copy)));

  const cards = el("div", { class: "cards" });
  const cov = data.coverage || {};
  const stream = data.stream || null;
  const arch = data.archives || null;
  // Source-aware presentation: a PostgreSQL stream's cursor is an LSN, written
  // across binlog_file (the "X/Y" string form, the one shown here) and
  // binlog_position (the same value as a uint64, for resume); mode='gtid' is an
  // internal detail. MySQL vocabulary would mislabel all of it. capsCache.source
  // is resolved before this paints (bootSequence/switchServer await
  // gateCapabilities → renderRoute).
  const pg = capsCache.source === "postgresql";

  // The Summary and Coverage cards went with the strip (#1950): the DBTrail
  // station says the events and the partitions, the history arrow the span
  // and its two bounds, the capture arrow the position and the indexed
  // files. The cards keep what the strip does not show.
  if (stream) cards.append(statusCard(pg ? "Stream · PostgreSQL" : "Stream", pg ? [
    ["source", "PostgreSQL · logical replication"],
    ["events indexed", stream.events_indexed],
    ["last checkpoint", utcBare(stream.last_checkpoint)],
  ] : [
    ["mode", stream.mode],
    ["events indexed", stream.events_indexed],
    ["last checkpoint", utcBare(stream.last_checkpoint)],
  ]));
  if (arch) cards.append(statusCard("Archives", [
    ["rows", arch.total_rows],
    ["local files", arch.local_files],
    ["S3 files", arch.s3_files],
  ]));
  cards.append(capacityCard(capacity));
  // Usage telemetry (#1867): what this process sends is a fact about the
  // process, like everything else on this page. Last, after the data.
  if (telemetry) cards.append(telemetryCard(telemetry));
  // Replication-health panel (#599): the streaming daemon polls the PostgreSQL source
  // (slot wal_status/lag + REPLICA IDENTITY coverage) and persists a snapshot to the
  // index; this renders it. Gated on source==postgresql AND a snapshot existing.
  if (pg && stream && stream.source_health) cards.append(pgHealthCard(stream.source_health));
  // Stream-continuity surface (see continuityBox) — appended above the cards so a
  // permanent-loss alarm, or the affirmative all-clear, is the first thing read.
  const continuity = continuityBox(stream, pg);
  if (continuity) v.append(continuity);
  // Capture-health surface (#1034): the continuity box's sibling for in-stream
  // discards — events the daemon read and chose to drop. Only the degraded
  // state renders (below the continuity box, above the cards); "ok" adds no
  // second green box.
  const captureHealth = captureHealthBox(stream);
  if (captureHealth) v.append(captureHealth);
  // Index-disk surface (#1444): only the warn/fail grades render a box, so a
  // filling index volume is read before the cards, next to the other alarms.
  const capBox = capacityBox(capacity);
  if (capBox) v.append(capBox);
  v.append(cards);
  viewEnter();
}

function statusCard(title, rows) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: title }));
  rows.forEach(([k, val, big]) => {
    card.append(el("div", { class: "kv" },
      el("span", { class: "kv-k", text: k }),
      el("span", { class: "kv-v" + (big ? " big" : ""), text: val === null || val === undefined ? "—" : String(val) })));
  });
  return card;
}

// ── Index disk (#1444) ──
//
// capacityCard and capacityBox render GET /api/capacity: the projection
// `bintrail doctor` computes (write rate from the last 24 hours of partition
// statistics, steady-state size over the retention window, free space on the
// index volume when this process can measure it). The GRADE is the doctor's,
// carried in `status`; the copy keys on `reason` and never re-derives a
// threshold, so the console and the CLI cannot disagree about the same disk.
// Two honesty rules the backend enforces and the copy must keep: free space
// that is not measurable from here is said so, never shown as a number; and
// the read-only console, which does not run rotation, gets no "grows without
// limit" verdict (retention.known is false there). Both pure and
// fixture-drivable, like continuityBox.

// retentionBasis is the phrase beside a retention window that says where it
// came from. With no console override, each server's index keeps the window it
// was created under, so the number alone would read as the daemon's setting
// for every server (#1709).
function retentionBasis(ret) {
  if (!ret || ret.source === "override") return "";
  switch (ret.basis) {
    case "recorded": return " (set when this server was added)";
    case "legacy": return " (kept from before the default changed)";
    case "unreadable": return " (its own setting could not be read, so the older default is used)";
    default: return " (DBTrail's default)";
  }
}

function daysText(d) {
  if (d === null || d === undefined || !isFinite(d)) return "";
  if (d < 1) return "under a day";
  if (d < 10) return "about " + d.toFixed(1) + " days";
  return "about " + Math.round(d) + " days";
}

function capacityStateClass(status) {
  switch (status) {
    case "pass": return "hstat-ok";
    case "warn": return "hstat-warn";
    case "fail": return "hstat-err";
    default: return "hstat-muted";
  }
}

function capacityChipClass(status) {
  switch (status) {
    case "pass": return "chip-ok";
    case "warn": return "chip-warn";
    case "fail": return "chip-error";
    default: return "chip-unknown";
  }
}

function capacityStateText(cap) {
  switch (cap.reason) {
    case "ok": return "ok";
    case "headroom_low": return "tight headroom";
    case "free_under_floor": return "little free space";
    case "growth_exceeds_free": return "will fill";
    case "no_retention": return "grows without limit";
    case "free_unknown": return "free space unknown";
    case "retention_unknown": return "no window known here";
    case "not_enough_history": return "measuring";
    case "not_initialized": return "no index yet";
    default: return cap.status || "unknown";
  }
}

// capacityNote is the one-line reading under the numbers: what the grade
// means for this server, or why there is no grade.
function capacityNote(cap) {
  switch (cap.reason) {
    case "ok":
      return "Fits with room to spare: about " + humanBytes(cap.remaining_bytes) + " of growth ahead, " + humanBytes(cap.free_bytes) + " free. Rotation caps the index before the disk fills.";
    case "free_unknown":
      return "Without free space DBTrail cannot grade the disk. Keep about 30% headroom above the steady size on the index volume.";
    case "retention_unknown":
      return "This DBTrail is read-only and does not run rotation, so it cannot tell how long the index keeps history or what size it settles at. Run the check where rotation runs (CLI: bintrail doctor --retain).";
    case "not_enough_history":
      return "A write rate needs at least 3 recent hours with events (" + (cap.sample_hours || 0) + " so far). Check back after a few hours of capture.";
    case "not_initialized":
      return "The index has no events table yet. It appears once capture starts.";
    default:
      return "";
  }
}

// capacityFreeNote explains a free-space figure the backend could not
// measure: which fallback the check landed on (free_reason) and what would
// make it measurable, when that is knowable (#1527). It never says where the
// index runs, which the check cannot know: the old copy asserted "another
// host or container" and read as plainly false on a single-machine install
// whose index data directory is simply not mounted into the console. The
// index_not_local branch carries no mount fix on purpose, since a mount that
// is not the index's would report the wrong volume's free space, and the
// host_unconfirmed branch (a local address whose server is not confirmed to
// be this machine, which is what a port-forward or a tunnel looks like)
// carries the fix WITH its precondition: a local mysqld's datadir may be
// sitting right there, and pointed at it the card would show a measured
// number for a volume that is not the index's.
function capacityFreeNote(cap) {
  if (!cap || cap.free_known) return "";
  switch (cap.free_reason) {
    case "mount_unset":
      return "DBTrail cannot see the index volume from here, and no read-only copy of the index data directory is set up. To measure free space, mount that directory into DBTrail read-only and set BINTRAIL_INDEX_DATADIR_RO to the mount point. The bundled docker-compose.yml wires both.";
    case "mount_unusable":
      return "BINTRAIL_INDEX_DATADIR_RO points at a path DBTrail cannot read, so free space was not measured. Check that the index data directory is still mounted there.";
    case "host_unconfirmed":
      return "The index answers on a local address, but DBTrail cannot confirm the server runs on this machine, so it cannot tell whether the index data directory is here. If it is, mount that directory into DBTrail read-only and set BINTRAIL_INDEX_DATADIR_RO to the mount point. Point it at the index's own data directory and nothing else: any other volume would be shown as the index's free space.";
    case "index_not_local":
      return "The index answers at another address, so DBTrail cannot see its volume, and measuring a disk here would report the wrong one. Watch free space where the index runs.";
    default:
      return "DBTrail cannot see the index volume from here, so free space was not measured.";
  }
}

function capacityCard(cap) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Index disk" }));
  if (!cap || cap.error) {
    card.append(el("p", { class: "form-hint", text: "Could not measure the index disk" + (cap && cap.error ? ": " + cap.error : ".") }));
    return card;
  }
  const ret = cap.retention || {};
  // A row's value is a figure (mono) or words (the UI face, #1950): "not
  // enough history yet" is a sentence, not data.
  const rows = [["index size", humanBytes(cap.current_bytes), true]];
  rows.push(["write rate", cap.measured
    ? humanBytes(cap.growth_bytes_per_day) + " a day (" + Math.round(cap.events_per_day).toLocaleString() + " events)"
    : "not enough history yet", false, !cap.measured]);
  const basis = ret.known && ret.enabled ? retentionBasis(ret) : "";
  rows.push(["keeps for", !ret.known ? "not known here" : (ret.enabled ? ret.retain + basis : "rotation is off"), false, !ret.known || !ret.enabled || !!basis]);
  if (cap.measured && cap.projected_bytes > 0) rows.push(["steady size", humanBytes(cap.projected_bytes)]);
  rows.push(["free on disk", cap.free_known ? humanBytes(cap.free_bytes) : "not measurable from here", false, !cap.free_known]);
  if (cap.days_until_full !== null && cap.days_until_full !== undefined) rows.push(["free space lasts", daysText(cap.days_until_full) + " at this rate", false, true]);
  rows.forEach(([k, val, big, words]) => {
    card.append(el("div", { class: "kv" },
      el("span", { class: "kv-k", text: k }),
      el("span", { class: "kv-v" + (big ? " big" : "") + (words ? " words" : ""), text: val })));
  });
  // The state is a chip of the status family (#1950); the hstat-* classes
  // stay as the grade's name for the tests that read it.
  card.append(healthKV("state", el("span", { class: "chip chip-sm " + capacityChipClass(cap.status) + " hstat " + capacityStateClass(cap.status), text: capacityStateText(cap) })));
  const note = capacityNote(cap);
  if (note) card.append(el("p", { class: "form-hint", text: note }));
  // Unmeasurable free space is explained under every grade, not just the
  // free_unknown one: the row above reads "not measurable from here" for a
  // fresh index and a short history too.
  const freeNote = capacityFreeNote(cap);
  if (freeNote) card.append(el("p", { class: "form-hint", text: freeNote }));
  return card;
}

// capacityBox is the alarm: rendered only for the doctor's warn and fail
// grades, with the plain reading of the numbers and what fixes it.
function capacityBox(cap) {
  if (!cap || cap.error || (cap.status !== "warn" && cap.status !== "fail")) return null;
  const growth = humanBytes(cap.growth_bytes_per_day) + " a day";
  const free = humanBytes(cap.free_bytes);
  const days = daysText(cap.days_until_full);
  const ahead = humanBytes(cap.remaining_bytes);
  const window = (cap.retention && cap.retention.retain) || "";
  const stops = " A full disk stops capture, and once the source deletes its binlogs those changes are gone for good.";
  const shrink = "Grow the index volume, or shorten how long the index keeps history (CLI: --rotate-retain), so capture keeps running.";
  let head, body, help;
  switch (cap.reason) {
    case "growth_exceeds_free":
      head = "⚠ The index disk will fill before rotation caps the index";
      body = "The index still grows by about " + ahead + " before the " + window + " window holds it steady, but only " + free + " is free. At " + growth + " that is " + days + "." + stops;
      help = "Free space now: shorten the window and rotate right away (CLI: bintrail rotate --retain 7d), then grow the volume or keep the shorter window. Archive to Parquet first to keep the history.";
      break;
    case "free_under_floor":
      head = "⚠ Little free space left on the index disk";
      body = "Only " + free + " is free: " + days + " of writes at " + growth + ". Rotation normally frees space in time, but a stalled rotation or a burst of writes fills the disk and stops capture.";
      help = shrink;
      break;
    case "headroom_low":
      head = "⚠ The index disk is getting tight";
      body = "The index still grows by about " + ahead + " before it settles, which uses over 70% of the " + free + " free. A burst of writes could fill the disk and stop capture.";
      help = shrink;
      break;
    case "no_retention":
      head = "⚠ Nothing caps the index: it grows without limit";
      body = "DBTrail runs with rotation off, so the index grows by " + growth + " at the current rate" +
        (cap.free_known ? ", and the disk fills in " + days + " (" + free + " free)" : "") + "." + stops;
      help = "Turn rotation on (CLI: --rotate-retain 48h) so old partitions are dropped and the index stays bounded. Archive to Parquet first to keep the history.";
      break;
    default:
      return null;
  }
  const box = el("div", { class: cap.status === "fail" ? "error-box" : "warn-box" });
  box.append(el("b", { text: head }));
  box.append(el("div", { text: body }));
  box.append(el("div", { class: "warn-line", text: help }));
  return box;
}

// continuityBox renders the stream-continuity surface, or null when there is
// nothing to assert. The red error-box is the durable permanent-loss record: an
// unfillable binlog gap (MySQL) or an invalidated/lost replication slot
// (PostgreSQL, #532); the index is valid only up to that point and capture must
// be re-baselined to resume. It keys on gap_lost, emitted independently of
// continuity — so a legacy backend that omits continuity still shows it on a lost
// stream. The affirmative counterpart (continuity.status === "ok") is the
// strip's DBTrail station since #1950, not a box; "unknown" (legacy index)
// and a missing continuity field draw no verdict anywhere. Pure and
// fixture-drivable, mirroring pgHealthCard: the console-e2e harness pins the
// gap_lost/neither states here and the ok state on statusFlowModel.
function continuityBox(stream, pg) {
  if (!stream) return null;
  if (stream.gap_lost) {
    const lost = el("div", { class: "error-box" });
    lost.append(el("b", { text: "⚠ Events permanently lost" }));
    lost.append(el("div", { text: stream.gap_lost.detail ||
      (pg ? "The replication slot PostgreSQL was using got invalidated. To keep capturing changes, create a new snapshot and start over."
          : "A gap in the binlog can't be filled; some history is permanently missing. To keep capturing changes, create a new snapshot and start over.") }));
    lost.append(el("div", { text: "Detected: " + utcLabel(stream.gap_lost.at) }));
    return lost;
  }
  // A clean verdict is a STATE, and states live on the strip: the DBTrail
  // station says "no gaps" while the capture arrow says whether capture is
  // running, so the two claims cannot be read as one (#1950). Only the alarm
  // is a box.
  return null;
}

// captureHealthBox renders the capture-health surface (#1034), or null when
// there is nothing to warn about. It keys on stream.capture_health.status ===
// "degraded" (newer backends only): the daemon READ these events off the
// stream and chose to DROP them — e.g. the column-count guard rejecting every
// row against a stale schema snapshot — so the stream can look "active" (fresh
// checkpoint, green continuity) while indexing nothing. "ok", "unknown"
// (missing field) and no stream all render nothing; the continuity box remains
// the affirmative/loss surface. Pure and fixture-drivable like continuityBox.
// Two things this renders that the tally alone cannot say (#1312):
//
//   - HISTORIC vs ACTIVE. The tally is monotonic, so a successful re-snapshot
//     leaves it byte-identical and the button this box offers had no visible
//     effect at all — an operator clicked it, reloaded, and saw the same orange
//     alarm. `skips_predate_snapshot` (computed by the backend against the
//     schema snapshot's own timestamp) separates "still dropping rows" from "a
//     record of something that stopped". Historic goes quiet; it does NOT go
//     away, because those events are permanently missing and a dismissed box
//     would trade an annoyance for lost evidence.
//   - The long form is one click away instead of five paragraphs down. ~250
//     words of cause/remedy/scope in an alarm box is text nobody reads.
//
// An old daemon sends neither field: no anchor, so the box stays loud and open
// — the pre-#1312 rendering, which is the safe direction.
function captureHealthBox(stream) {
  if (!stream || !stream.capture_health || stream.capture_health.status !== "degraded") return null;
  const h = stream.capture_health;
  const acked = h.acknowledged === true;
  const historic = h.skips_predate_snapshot === true;
  const box = el("div", { class: acked ? "muted-box" : (historic ? "ok-box" : "warn-box") });
  box.append(el("b", { text: acked
    ? "Capture gap on record (acknowledged)"
    : historic
      ? "Capture gap on record: nothing skipped since the current snapshot"
      : "⚠ Capture incomplete: some changes were not indexed" }));
  const reasons = Object.keys(h.skipped || {}).sort().join(", ");
  box.append(el("div", { text: h.total_skipped + " event(s) were read from the stream but not indexed" +
    (reasons ? " (" + reasons + ")" : "") + (h.last_skip_at ? "; last " + utcLabel(h.last_skip_at) : "") +
    ". Those changes are missing from the index for good." +
    (acked && h.acknowledged_at ? " Acknowledged " + utcLabel(h.acknowledged_at) + "; anything skipped after that count raises this again." : "") +
    (!acked && historic && h.snapshot_at ? " The schema snapshot in force since " + utcLabel(h.snapshot_at) + " has recorded none." : "") }));
  // Cause, remedy and scope come from the backend (status.ExplainCaptureSkips),
  // the same strings `bintrail status` prints — the console must not re-author
  // this advice in JavaScript, because the half that drifts is always the half
  // saying what a remedy does NOT recover. The fallback covers a daemon too old
  // to send the field, and deliberately promises nothing on its behalf.
  const lines = (Array.isArray(h.explanation) && h.explanation.length) ? h.explanation
    : ["Changes in those events are missing from the index. This version of DBTrail is too old to say why; run `bintrail status` against this index for the reason and the fix."];
  const details = el("details", { class: "warn-details" }, el("summary", { text: "Why this happened, and what fixes it" }));
  lines.forEach((t) => details.append(el("div", { class: "warn-line", text: t })));
  const action = schemaSnapshotButton();
  // The action sits inside the disclosure once the skips are historic: it has
  // already been pressed, and leaving it under the headline invites pressing it
  // again forever against a tally that will never move.
  if (action) (historic || acked ? details : box).append(action);
  // Mark-as-read (#1314) is the only action on an already-acknowledged record,
  // so it is absent once acked — and it stays OUTSIDE the disclosure while
  // unacknowledged, because it is the one thing an operator looking at a
  // permanent record actually wants and it must not be five paragraphs down.
  //
  // seen_total is the count THIS render displayed: the endpoint refuses the
  // acknowledgement if the live tally has since gone higher, so a tab left
  // open cannot retire skips that happened while nobody was looking.
  if (!acked) {
    // A class of its own, NOT .warn-actions: the e2e pins where the
    // schema-snapshot action sits by querying ":scope > .warn-actions", and a
    // second wrapper sharing that class would make the assertion pass no
    // matter where the snapshot button went.
    const wrap = el("div", { class: "ack-actions" });
    const ackBtn = el("button", { class: "btn btn-sm", type: "button", text: "Mark as read" });
    ackBtn.onclick = () => acknowledgeCaptureSkips(h.total_skipped, ackBtn);
    wrap.append(ackBtn);
    box.append(wrap);
  }
  box.append(details);
  return box;
}

// acknowledgeCaptureSkips records that the operator has seen this tally, so the
// box stops being an alarm (#1314). It does not clear the tally and does not
// pretend the events came back: the record stays, quietly, and a later skip
// pushes the count above what was acknowledged and turns the alarm back on by
// itself. That is why this is safe to offer as a plain button — it can retire a
// record, it cannot suppress the next incident.
async function acknowledgeCaptureSkips(total, btn) {
  if (btn) { btn.disabled = true; btn.textContent = "Marking…"; }
  try {
    await api("/api/capture-skips/ack", { method: "POST", body: { seen_total: total } });
  } catch (err) {
    // The 409 here is the stale-tab refusal, and its message already says to
    // reload — surfacing the server's own text keeps the two in one voice.
    toastError("Could not mark as read: " + ((err && err.message) || err));
    if (btn) { btn.disabled = false; btn.textContent = "Mark as read"; }
    return;
  }
  toast("Marked as read. The record stays; if anything gets skipped from now on, the warning comes back.");
  renderStatus();
}

// schemaSnapshotButton renders the Refresh-schema-snapshot action for the
// selected server, or null when this console cannot perform it (#1296). It
// exists because the old banner named a remedy with no button anywhere in the
// UI, leaving the CLI — in a different container — as the only route.
//
// Gated on a real REGISTRY server: the reserved "default" entry is the daemon's
// own command-line stream, which the control plane does not supervise and the
// endpoint refuses with 409. The label never says just "snapshot": the button
// next to it creates a BASELINE (a full copy of the data), and the two artifacts
// were already being confused.
function schemaSnapshotButton() {
  const id = currentServer || defaultServerId;
  if (!capsCache.schema_snapshot_trigger || !id || id === "default") return null;
  const wrap = el("div", { class: "warn-actions" });
  const btn = el("button", { class: "btn btn-sm", type: "button", text: "Refresh schema snapshot" });
  btn.onclick = () => refreshSchemaSnapshot(id, btn);
  wrap.append(btn);
  return wrap;
}

// refreshSchemaSnapshot re-reads the source's column layout and restarts that
// server's capture stream onto it, then reports what actually happened. The
// three outcomes are reported separately on purpose: a failed snapshot, a
// snapshot whose stream did NOT reload (capture is still on the old layout —
// nothing is fixed yet), and tables validation excluded (those stay uncaptured
// no matter how often this runs).
async function refreshSchemaSnapshot(id, btn) {
  if (btn) { btn.disabled = true; btn.textContent = "Refreshing…"; }
  const restore = () => { if (btn) { btn.disabled = false; btn.textContent = "Refresh schema snapshot"; } };
  try {
    await api("/api/servers/" + encodeURIComponent(id) + "/schema-snapshot", { method: "POST", body: {} });
  } catch (err) {
    toastError("Schema snapshot failed: " + ((err && err.message) || err));
    restore();
    return;
  }
  toast("Reading the source's table layout…");
  const done = await pollSchemaSnapshot(id);
  restore();
  if (!done) { toast("Schema snapshot is still running. Check back shortly."); return; }
  if (done.state !== "succeeded") {
    toastError("Schema snapshot failed: " + (done.last_error || "unknown error"));
    return;
  }
  let msg = "Schema snapshot updated: " + (done.tables || 0) + " table(s).";
  // Never assert WHICH state capture ended in when the reload failed: the
  // reload can fail with the old stream still running (registry gone) or with
  // it stopped (it did not shut down in time). Print the daemon's own account
  // instead of guessing — "still using the previous snapshot" would be a lie
  // for the stopped case.
  msg += done.stream_reloaded
    ? " Capture restarted on it."
    : " Capture did NOT restart onto it: " + (done.reload_error || "restart this server's capture to pick it up") + ".";
  if ((done.excluded_tables || []).length) {
    msg += " Still not captured (no primary key / not InnoDB): " + done.excluded_tables.join(", ") + ".";
  }
  // The banner is driven by a monotonic tally, so it stays up after a working
  // fix. Say so here or the operator concludes the button did nothing.
  msg += " Events already skipped stay missing, and this warning stays up: it counts skips that happened; use \u201cMark as read\u201d once you have seen the count.";
  toast(msg);
  renderStatus();
}

// pollSchemaSnapshot polls until the job leaves "running" (or a ~2-minute cap:
// a snapshot is an information_schema read plus a stream restart, not a dump).
// Returns the terminal status, or null if it never settled. Transient poll
// errors are retried — the stream restart briefly disturbs nothing else, but a
// blip must not be reported as a failed snapshot.
async function pollSchemaSnapshot(id) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  for (let i = 0; i < 60; i++) {
    await sleep(2000);
    let st;
    try {
      st = (await api("/api/servers/" + encodeURIComponent(id) + "/schema-snapshot")).schema_snapshot;
    } catch (_) {
      continue;
    }
    if (st && st.state !== "running") return st;
  }
  return null;
}

// PG_HEALTH_STALE_SEC: a source_health snapshot older than this reads as STALE. The
// daemon polls every 30s, so 90s ≈ 3 missed polls → likely stopped. Load-bearing, not
// decoration: an index-only console cannot tell a frozen "reserved" from a live one, so
// a stale snapshot must never render as healthy green (#599).
const PG_HEALTH_STALE_SEC = 90;

// pgHealthCard renders the persisted PostgreSQL replication-health snapshot. h is the
// parsed stream.source_health object {exists,active,wal_status,retained_bytes,
// safe_wal_size,restart_lsn,confirmed_flush_lsn,replica_identity_not_full,checked_at,
// probe_error}. probe_error is the failed-probe discriminator: when set, this snapshot
// records a probe failure (the slot fields are absent) and the card shows "probe failing".
function pgHealthCard(h) {
  const card = el("div", { class: "card" });
  card.append(el("div", { class: "card-title", text: "Replication health" }));

  // Staleness drives everything: an unparseable/old checked_at mutes the whole card and
  // labels it, so a dead poller never reads healthy.
  const checked = h.checked_at ? Date.parse(h.checked_at) : NaN;
  const ageSec = isNaN(checked) ? Infinity : Math.max(0, (Date.now() - checked) / 1000);
  const stale = ageSec >= PG_HEALTH_STALE_SEC;
  if (stale) card.classList.add("card-stale");

  // probe_error: the daemon could not read source health (e.g. a standby source, or a
  // query-DSN failure). Show it explicitly — a recorded failure, never a blank panel or
  // a misleading "all FULL". The slot/RI sections are skipped (their fields are absent).
  if (h.probe_error) {
    card.append(healthKV("status", el("span", { class: "hstat hstat-err", text: "probe failing" })));
    card.append(el("div", { class: "hlist", text: h.probe_error }));
  } else {
    if (!h.exists) {
      card.append(healthKV("slot", el("span", { class: "hstat hstat-muted", text: "not found yet" })));
    } else {
      card.append(healthKV("WAL status", el("span", { class: "hstat " + walStatusClass(h.wal_status), text: h.wal_status || "—" })));
      card.append(healthKV("retained WAL", el("span", { class: "kv-v", text: humanBytes(h.retained_bytes) })));
      card.append(healthKV("safe margin", el("span", { class: "kv-v", text: h.safe_wal_size == null ? "unlimited" : humanBytes(h.safe_wal_size) })));
      card.append(healthKV("consumer", el("span", { class: "kv-v", text: h.active ? "connected" : "—" })));
    }

    const nf = h.replica_identity_not_full || [];
    if (nf.length === 0) {
      card.append(healthKV("replica identity", el("span", { class: "hstat hstat-ok", text: "all FULL ✓" })));
    } else {
      card.append(healthKV("replica identity", el("span", { class: "hstat hstat-warn", text: "⚠ " + nf.length + " not FULL" })));
      const list = el("div", { class: "hlist" });
      nf.forEach((t) => list.append(el("div", { text: t })));
      card.append(list);
    }
  }

  const foot = el("div", { class: "hstale" + (stale ? " hstale-warn" : "") });
  foot.append(stale
    ? "stale; last checked " + agoText(ageSec) + " (DBTrail may be stopped)"
    : "checked " + agoText(ageSec));
  card.append(foot);
  return card;
}

function healthKV(k, valNode) {
  return el("div", { class: "kv" }, el("span", { class: "kv-k", text: k }), valNode);
}

function walStatusClass(s) {
  switch (s) {
    case "reserved": return "hstat-ok";
    case "extended": return "hstat-warn";
    case "unreserved": return "hstat-warn hstat-strong";
    case "lost": return "hstat-err";
    default: return "hstat-muted";
  }
}

function humanBytes(n) {
  n = Number(n) || 0;
  if (n < 1024) return n + " B";
  const u = ["KB", "MB", "GB", "TB"];
  let i = -1;
  do { n /= 1024; i++; } while (n >= 1024 && i < u.length - 1);
  return n.toFixed(1) + " " + u[i];
}

function agoText(sec) {
  if (!isFinite(sec)) return "unknown";
  if (sec < 60) return Math.round(sec) + "s ago";
  if (sec < 3600) return Math.round(sec / 60) + "m ago";
  return Math.round(sec / 3600) + "h ago";
}

function updateSideMeta(status) {
  const servers = status.servers || [];
  const s0 = servers[0];
  const conn = s0 ? (s0.username + "@" + s0.host + ":" + s0.port) : "—";
  const connEl = document.getElementById("meta-conn");
  if (connEl) connEl.textContent = serversEmpty && capsCache.monitor ? "internal index" : conn;
  const streamEl = document.getElementById("meta-stream");
  if (streamEl) {
    clear(streamEl);
    streamEl.append("stream ");
    if (status.stream) {
      // PG stores its LSN cursor in binlog_file; "gtid" mode is an internal detail.
      const pg = capsCache.source === "postgresql";
      streamEl.append(el("b", { text: pg ? "PostgreSQL" : status.stream.mode }));
      if (status.stream.binlog_file) streamEl.append((pg ? " · LSN " : " · ") + status.stream.binlog_file);
    } else {
      streamEl.append(el("b", { text: "—" }));
    }
  }
}

// ── Storage (rotation · S3 archiving · credentials · telemetry) ──────────────

async function renderRetention() {
  // Gated like Time-travel: a direct URL / Back with the capability off must
  // REWRITE the URL (replaceState) before re-dispatching — see renderTimetravel.
  if (!capsCache.monitor) { history.replaceState({}, "", "/overview"); renderRoute(); return; }
  const gen = serverGen, vgen = viewGen;
  viewLoading();
  // Each fetch degrades independently: a panel renders its own failure note
  // instead of one error wiping the whole page. (A 401 inside api() raises the
  // sign-in gate and bumps serverGen, so the stale-render guard below bails.)
  const asErr = (err) => ({ error: (err && err.message) || String(err) });
  const [serversRes, rotation] = await Promise.all([
    api("/api/servers").catch(asErr),
    api("/api/rotation").catch(asErr),
  ]);
  if (gen !== serverGen || vgen !== viewGen) return;
  // Same guard as renderOverview: a throw inside the build must show an
  // error, never leave the "Loading…" skeleton up forever.
  try {
    buildRetention(serversRes, rotation);
  } catch (err) {
    const v = VIEW(); clear(v); v.append(pageHead("Retention", null)); renderError(v, err);
  }
}

// Retention answers one question: what happens to your data as it ages. The
// rotation policy decides when index partitions are archived and dropped, and
// the archiving panel says where each source's archives go. Nothing else on
// the old Storage page answered that (#1543).
function buildRetention(serversRes, rotation) {
  // serversRes is the raw /api/servers payload or {error} — archivingPanel
  // must be able to tell "failed to load" from "genuinely no sources", or a
  // transient 500 would render the affirmative "No monitored sources yet" lie.
  const servers = (serversRes && serversRes.servers) || [];
  const serversErr = serversRes && serversRes.error;
  const v = VIEW(); clear(v);
  // No opening sentence (#1950): the rotation card draws how long data is
  // kept and where it goes.
  v.append(pageHead("Retention", null));

  const cards = el("div", { class: "cards" });
  const sources = servers.filter((s) => s.has_source);
  cards.append(rotationCard(rotation, sources.length ? sources.some((s) => !!s.archive_s3) : undefined));
  v.append(cards);

  const grid = el("div", { class: "ov-grid", style: "margin-top:18px" });
  grid.append(archivingPanel(servers, serversErr));
  v.append(grid);
  viewEnter();
}

// ── Protect: baselines and verification (#1384) ──
//
// Both gate on capsCache.monitor and both use the replaceState + re-dispatch
// pattern renderStorage/renderTimetravel share: a direct URL or Back with the
// capability off must REWRITE the URL before re-rendering, or the address bar
// keeps pointing at a view the session cannot show.

// ── Snapshots (#1573) ────────────────────────────────────────────────────
// One page for what used to be three: the copies this server has, whether
// they are restorable, and where and how often they are made. Split across
// three addresses, each one read as the whole answer, so nobody could tell
// whether their data was safe without visiting all three — and two of them
// did not exist at all on a standalone serve.
//
// The order is the reading order: what do I have, is it good, how is it
// kept. The two lower parts carry the anchors the old addresses land on
// (#checks, #setup), so a bookmark still shows what it named.

// SNAPSHOT_MOVED names the page each old address was, for the one line a
// visitor who followed one of them reads on arrival. A Map, like
// ROUTE_ALIASES and for the same reason: a plain object answers for the
// names every object carries ("constructor", "toString"), and a lookup that
// hits one of those would put a function in the sentence.
const SNAPSHOT_MOVED = new Map([
  ["baselines", "Backups"],
  ["verification", "Verification"],
  ["backup-settings", "Backup settings"],
]);

// The notices this visitor closed. They live in the BROWSER and only there:
// the console keeps nothing per person on the server, and this is a fact
// about one reader's bookmarks, not about the installation. Every access is
// wrapped, because reading localStorage THROWS where site data is blocked
// (a private window, an enterprise policy, a file:// page) — a dismissed
// notice must never be able to break the page it sits on. The in-memory set
// is the fallback: the notice then stays closed for this visit.
const MOVED_KEY = "dbtrail.moved.";
const movedClosed = new Set();
function movedIsClosed(from) {
  if (movedClosed.has(from)) return true;
  try { return !!window.localStorage.getItem(MOVED_KEY + from); } catch (e) { return false; }
}
function closeMoved(from) {
  movedClosed.add(from);
  try { window.localStorage.setItem(MOVED_KEY + from, "1"); } catch (e) { /* this visit only */ }
}

// snapshotsMovedNotice is that one line: the page they asked for, and where
// it is now. One per OLD ADDRESS, so closing the one for Verification does
// not hide the one a bookmark of Backup settings would show — they are
// different readers' habits, and each is told once.
//
// `missing` says the part their page became is NOT on this page, and why:
// "daemon" for a read-only console (the checks run in the watch daemon, so
// it draws no section for them), "unknown" when the capability check failed
// and we cannot tell. Telling someone their page is "part of Snapshots now"
// on a page with no trace of it reads as a feature that was removed, which
// is worse than the bounce to Overview it replaced — and telling a watch
// daemon it is read-only because a request failed is a false statement about
// their installation. "" when the section is here.
//
// "daemon" names the checks, the only section that can be absent for a
// reason worth a sentence. The setup section can also be absent, by
// permission, and renderSnapshots then shows no note at all (setupHidden):
// hidden by permission says nothing.
function snapshotsMovedNotice(missing) {
  const from = routeArrivedFrom;
  const was = SNAPSHOT_MOVED.get(from);
  if (!was || movedIsClosed(from)) return null;
  const why = missing === "daemon" ? " Its section is not in this web interface: checks run in the DBTrail service, and this one is read-only."
    : missing === "unknown" ? " Its section is missing because the capability check failed when the Snapshots page loaded; reload the page to get it back."
    : "";
  const box = el("div", { class: "snap-moved" });
  box.append(el("span", { class: "snap-moved-text", text: was + " is part of Snapshots now." + why }));
  box.append(el("button", {
    class: "btn btn-icon btn-sm btn-ghost snap-moved-x", type: "button", title: "Dismiss", "aria-label": "Dismiss",
    onclick: () => { closeMoved(from); box.remove(); },
  }, "×"));
  return box;
}

// snapshotSection is a heading an address can land on: its id is the anchor
// ROUTE_ALIASES sends an old page's readers to.
function snapshotSection(title, id) {
  return el("h2", { class: "snap-sect", id: id, text: title });
}

// snapTab is the tab the reader last opened, kept across the page's own
// repaints (a job that settles repaints the page); an arrival from another
// page opens Versions. The address wins over both: /snapshots#checks and
// /snapshots#setup, the two old pages' addresses, and #versions/#settings,
// the tabs' own, open their tab.
let snapTab = "versions";
// snapTabAuto: the tab on screen was picked by the page (nothing to list
// yet, so Settings), not by the reader or the address. A repaint may then
// move on: once the first snapshot exists the list is what to show.
let snapTabAuto = false;
const SNAP_TAB_IDS = { versions: "versions", checks: "checks", setup: "settings", settings: "settings" };
function snapTabFromHash() {
  return SNAP_TAB_IDS[(location.hash || "").slice(1)] || "";
}

// snapshotTabs builds the bar and the panels (Versions, Checks, Settings).
// A panel the caller does not have is not offered. select() shows one
// panel, marks its tab and writes the address (a hash, which the router
// keeps); with `quiet` it leaves the address alone.
function snapshotTabs(has) {
  const bar = el("div", { class: "tabs snap-tabs" });
  const list = el("div", { class: "tablist snap-tablist", role: "tablist", "aria-label": "Snapshots" });
  bar.append(list);
  const panels = {}, buttons = {}, order = [];
  const names = [["versions", "Versions"], ["checks", "Checks"], ["settings", "Settings"]];
  for (const [id, label] of names) {
    if (id !== "versions" && !has[id]) continue;
    panels[id] = el("div", { class: "tabpanel snap-panel", id: "snap-" + id, role: "tabpanel", "aria-labelledby": "snap-tab-" + id });
    panels[id].hidden = true;
    const btn = el("button", { class: "tab snap-tab", type: "button", role: "tab", id: "snap-tab-" + id, "aria-controls": "snap-" + id,
      "aria-selected": "false", tabindex: "-1", "data-tab": id, text: label });
    btn.onclick = () => select(id);
    buttons[id] = btn;
    order.push(id);
    list.append(btn);
  }
  // Arrow keys move between tabs, as the tabs pattern says; one tab stop
  // for the whole list.
  list.addEventListener("keydown", (e) => {
    const i = order.indexOf(snapTab);
    if (i < 0) return;
    let to = -1;
    if (e.key === "ArrowRight") to = (i + 1) % order.length;
    else if (e.key === "ArrowLeft") to = (i - 1 + order.length) % order.length;
    else if (e.key === "Home") to = 0;
    else if (e.key === "End") to = order.length - 1;
    if (to < 0) return;
    e.preventDefault();
    select(order[to]);
    buttons[order[to]].focus();
  });
  const actions = el("div", { class: "tab-actions snap-tab-actions" });
  bar.append(actions);
  function select(id, quiet) {
    if (!panels[id]) id = "versions";
    for (const k of Object.keys(panels)) {
      panels[k].hidden = k !== id;
      buttons[k].setAttribute("aria-selected", k === id ? "true" : "false");
      buttons[k].setAttribute("tabindex", k === id ? "0" : "-1");
      buttons[k].classList.toggle("is-on", k === id);
    }
    snapTab = id;
    const dl = actions.querySelector ? actions.querySelector(".snap-dl") : null;
    if (dl) dl.classList.toggle("btn-primary", id === "versions");
    if (!quiet) snapTabAuto = false;
    if (!quiet && typeof history !== "undefined" && history.replaceState) {
      history.replaceState(history.state, "", location.pathname + location.search + "#" + id);
      // The router arms its one arrival scroll when the address CHANGED
      // since it last painted; a tab click is not an arrival.
      lastRouteAddress = location.pathname + location.hash;
    }
  }
  return { bar: bar, panels: panels, actions: actions, select: select };
}

// snapshotHeroTone grades the copy, worst fact first, and says which page
// fixes it (link: "overview" for capture, "settings" for the schedule, ""
// when nothing here can):
//   the supervisor's word (a stopped or crashed stream is known before the
//   index shows it), then the index's (stalled, position lost); a capture
//   state that could not be read is never mint; a failed run, manual or
//   scheduled; a saved schedule the daemon cannot run, or whose next slot
//   will not start, or that skipped its last one; then the age against the
//   interval: mint inside one and a half intervals, sun up to three, pink
//   past that. No schedule, or no age: no colour.
function snapshotHeroTone(b, cov, cur) {
  const snap = b && b.snapshots && b.snapshots[0];
  if (!snap) return { tone: "none", why: "", link: "" };
  const sch = b.schedule;
  const mstate = (cur && cur.kind === "registry" && cur.monitor_state) || "";
  if (/^(failed|stalled|lost_position|stopped)$/.test(mstate)) {
    return { tone: "bad", why: "Capture is " + (mstate === "lost_position" ? "lost" : mstate === "stalled" ? "stalled" : "stopped") + ", so nothing new reaches the copy.", link: "overview" };
  }
  if (!cov || cov.continuity === "unavailable" || cov.freshness === "unavailable" || cov.freshness === "unknown") {
    return { tone: "warn", why: "Capture state could not be read.", link: "overview" };
  }
  if (cov.freshness === "stalled" || cov.continuity === "gap_lost") {
    return { tone: "bad", why: "Capture is " + (cov.continuity === "gap_lost" ? "lost" : "stalled") + ", so nothing new reaches the copy.", link: "overview" };
  }
  const lastRun = sch && sch.last_run;
  if (lastRun && lastRun.ok === false) {
    return { tone: "bad", why: "The last update failed" + (lastRun.error ? ": " + plainWords(lastRun.error) : "."), link: "settings" };
  }
  if (b.refresh && b.refresh.state === "failed") {
    return { tone: "bad", why: "The last update failed" + (b.refresh.last_error ? ": " + plainWords(b.refresh.last_error) : "."), link: "settings" };
  }
  if (sch && !sch.runnable) {
    const why = plainWords(sch.reason || "unknown reason");
    return { tone: "bad", why: "The schedule cannot run: " + why + (/[.!?]$/.test(why) ? "" : "."), link: "settings" };
  }
  if (sch && sch.next_method_error) {
    return { tone: "bad", why: "The next run cannot start: " + plainWords(sch.next_method_error), link: "settings" };
  }
  const skip = sch && sch.last_skipped;
  if (skip && (!lastRun || (skip.at || "") >= (lastRun.finished_at || ""))) {
    return { tone: "bad", why: "A scheduled run did not start: " + plainWords(skip.reason || "unknown reason"), link: "settings" };
  }
  const every = sch ? flowEveryMinutes(sch.every) : 0;
  if (!every) return { tone: "none", why: "", link: "" };
  if (typeof snap.age_hours !== "number" || isNaN(snap.age_hours)) return { tone: "none", why: "", link: "" };
  const age = snap.age_hours * 60;
  if (age <= every * 1.5) return { tone: "ok", why: "", link: "" };
  if (age <= every * 3) return { tone: "warn", why: "Later than its schedule.", link: "settings" };
  return { tone: "bad", why: "Much later than its schedule.", link: "settings" };
}

// snapshotHero draws the copy: age, where it lives, seven days of rhythm,
// the last check, and the two actions. Everything it shows comes from the
// listing and the coverage read already on the page; the check verdict is
// filled in by loadSnapshotVerdict once its own read answers.
// acts.download / acts.settings: what the Download button and the
// "Settings ›" links do (null = not offered). acts.mount: where the buttons
// go (the tab bar's right end, so they stay one click away on every tab);
// without it they sit in the hero. The reason a button is missing stays in
// the hero either way.
function snapshotHero(b, cov, cur, acts) {
  const hero = el("section", { class: "snap-hero", "aria-label": "The copy" });
  const settingsLink = () => acts && acts.settings
    ? el("a", { href: "#settings", class: "hero-link", text: "Settings ›", onclick: (e) => { e.preventDefault(); acts.settings(); } })
    : null;
  // Where the fix is: the schedule lives on this page's Settings tab, the
  // capture on Overview; a state that could not be read has no fix here.
  const fixLink = (link) => link === "settings" ? settingsLink()
    : link === "overview" ? el("a", { href: "/overview", class: "hero-link", text: "Overview ›", onclick: (e) => { e.preventDefault(); navigate("overview"); } })
    : null;
  const missingDB = b && b.error ? indexMissingFrom(b.error) : "";
  if (missingDB) {
    // Not a fault: the server was added and capture has not started. Say
    // what to do and where, in the grey of "nothing yet", never in pink.
    hero.append(el("div", { class: "hero-card hero-age none" }, el("div", { class: "hero-k", text: "Snapshots" }),
      el("div", { class: "hero-big", text: "not indexing yet", title: indexMissingDetail(missingDB) }),
      el("div", { class: "hero-sub" }, indexMissingWords() + " ",
        el("a", { href: "#servers", class: "hero-link", text: "Servers ›", onclick: (e) => { e.preventDefault(); openServersModal(); } }))));
    return hero;
  }
  if (!b || b.error) {
    hero.append(el("div", { class: "hero-card hero-age bad" }, el("div", { class: "hero-k", text: "Snapshots" }),
      el("div", { class: "hero-big", text: "could not load" }), el("div", { class: "hero-sub", text: (b && b.error) || "unavailable" })));
    return hero;
  }
  if (!b.configured) {
    const sub = el("div", { class: "hero-sub" }, "Set where the copy lives");
    const link = settingsLink();
    if (link) sub.append(" under ", link); else sub.append(".");
    hero.append(el("div", { class: "hero-card hero-age none" }, el("div", { class: "hero-k", text: "Snapshots" }),
      el("div", { class: "hero-big", text: "not set up" }), sub));
    return hero;
  }
  const snaps = b.snapshots || [];
  const snap = snaps[0] || null;
  const grade = snapshotHeroTone(b, cov, cur);
  const sch = b.schedule;

  // Updated: the age, big, with the dot; the sub-line says the rhythm, or
  // what is wrong.
  const age = el("div", { class: "hero-card hero-age " + grade.tone });
  age.append(el("div", { class: "hero-k", text: "Updated" }));
  age.append(el("div", { class: "hero-big-row" }, el("span", { class: "hero-dot " + grade.tone }),
    el("span", { class: "hero-big", text: snap ? formatAge(snap.age_hours) + " ago" : "never", title: snap ? utcLocalTitle(snap.time) || null : null })));
  const sub = el("div", { class: "hero-sub" });
  const link = settingsLink();
  if (grade.why) {
    sub.append(grade.why + " ");
    const fix = fixLink(grade.link);
    if (fix) sub.append(fix);
  } else if (sch && sch.runnable && flowEveryLabel(sch.every)) {
    sub.append(flowEveryLabel(sch.every) + (sch.next_run ? " · next " + flowHHMM(sch.next_run) + " UTC" : ""));
  } else {
    sub.append("Not on a schedule. ");
    if (link) sub.append(link);
  }
  age.append(sub);
  hero.append(age);

  // Where it lives: a tile per place. Lit when the newest copy is there,
  // dim when the place is set but the newest copy is not there yet, and
  // "No S3" / "No local copy" when no such place is set.
  const srcs = b.sources && b.sources.length ? b.sources : (b.kind ? [{ kind: b.kind, source: b.source }] : []);
  // "on" means the newest copy IS there. No copy, or a listing that does
  // not say where the newest one is: the place is set but not lit.
  const kinds = (snap && snap.kinds) || [];
  const tile = (kind, label, off, ico) => {
    const src = srcs.find((x) => x.kind === kind);
    const state = !src ? "off" : kinds.includes(kind) ? "on" : "dim";
    return el("div", { class: "hero-card hero-tile " + state, title: src ? src.source : null },
      icon(ico, "hero-ico"), el("span", { class: "hero-tile-t", text: src ? label : off }));
  };
  hero.append(el("div", { class: "hero-where" },
    tile("dir", "On disk", "No local copy", "folder"),
    tile("s3", "In S3", "No S3", "bucket")));

  // Seven days of rhythm: one tick per snapshot, the newest pink, a gap
  // visible without reading. The listing is capped, so a long history says
  // "50+" and the strip still ends today.
  const days = el("div", { class: "hero-card hero-days" });
  days.append(el("div", { class: "hero-k", text: "Last 7 days" }));
  const strip = el("div", { class: "hero-strip" });
  const now = Date.now(), span = 7 * 86400000;
  let shown = 0;
  snaps.forEach((sn, i) => {
    const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/.exec(String(sn.time || ""));
    const t = m ? Date.parse(m[1] + "T" + m[2] + "Z") : NaN;
    if (isNaN(t) || now - t > span || t > now) return;
    shown++;
    const left = ((t - (now - span)) / span) * 100;
    strip.append(el("span", { class: "hero-tick" + (i === 0 ? " newest" : ""), style: "left:" + left.toFixed(2) + "%", title: utcLocalTitle(sn.time) || null }));
  });
  days.append(strip);
  const foot = el("div", { class: "hero-days-foot" });
  foot.append(el("span", { text: shown ? shown + (b.truncated && shown === snaps.length ? "+" : "") + " this week" : "none this week" }));
  foot.append(el("span", { text: snaps.length ? snaps.length + (b.truncated ? "+" : "") + " in all" : "" }));
  days.append(foot);
  hero.append(days);

  // The last check: filled in when its read answers; absent where no daemon
  // can run one.
  // Only where a check can run: on a daemon with checks off the history
  // read answers 403, which is not a failure to report.
  if (capsCache.verify_trigger && cur && cur.id) {
    const check = el("div", { class: "hero-card hero-tile hero-check none" }, icon("check", "hero-ico"), el("span", { class: "hero-tile-t", text: "Checking…" }));
    hero.append(check);
    loadSnapshotVerdict(cur.id, check);
  }

  // The actions: Download opens the take-away lane; Read database now keeps
  // the gating the context strip had (#1677), and says why when it cannot
  // run. Only for a session that may create one: the note names a
  // configuration fix, and sending a reader who lacks the permission to fix
  // a setting they cannot touch is worse than saying nothing.
  const actions = (acts && acts.mount) || el("div", { class: "hero-actions" });
  // One filled button per tab (#1950): Download is it on Versions, the tab
  // it acts on; on Checks that is Run verification, and on Settings the Save
  // of the form being changed (snapshotTabs.select keeps this in step).
  let canRead = false;
  if (acts && acts.download) actions.append(el("button", { class: "btn snap-dl" + (snapTab && snapTab !== "versions" ? "" : " btn-primary"), type: "button", text: "Download", onclick: acts.download }));
  if (cur && cur.id && cur.kind === "registry" && sessionMay(PERM_SNAPSHOT_CREATE)) {
    // cur is the RAW registry entry, while b.configured also counts the
    // daemon-wide default, which a backup refuses to write to. The precheck
    // reads the raw fields (hasOwnBackupLocation), so this does too.
    const ownLoc = !!(cur.baseline_dir || cur.baseline_s3);
    const off = !capsCache.baseline_trigger;
    if (!off && ownLoc && cur.write_refusal) {
      // The location is shared (#1684): the dump would be refused, so the
      // refusal is shown instead of a button that answers 409.
      hero.append(el("div", { class: "hero-note", text: "Read database now: " + cur.write_refusal }));
    } else if (!off && ownLoc) {
      const btn = el("button", { class: "btn", type: "button", text: "Read database now" });
      btn.onclick = () => { if (typeof window.confirm === "function" && window.confirm(READ_DB_CONFIRM)) createBaseline(cur.id, btn); };
      actions.append(btn);
      canRead = true;
    } else if (cur.has_source) {
      const why = [];
      if (off) why.push("turned off at startup");
      if (!ownLoc) why.push("needs this server's own snapshot location");
      hero.append(el("div", { class: "hero-note", text: "Read database now: " + why.join(", and ") + (sessionMayConfigureServer() && acts && acts.settings ? " (under Settings)" : "") }));
    }
  }
  const pit = newestCopyLine(snaps, canRead, sessionMay(PERM_SNAPSHOT_CREATE));
  if (pit) hero.append(el("div", { class: "hero-note snap-pit", text: pit }));
  if (actions.children.length && !(acts && acts.mount)) hero.append(actions);
  return hero;
}

// loadSnapshotVerdict fills the hero's check tile from the verify history
// on first paint; loadVerifyHistory refills it when a run ends, so the hero
// and the Checks card never disagree about the same record.
async function loadSnapshotVerdict(id, tile) {
  let recs;
  try {
    recs = (await api("/api/servers/" + encodeURIComponent(id) + "/verify/history")).history || [];
  } catch (err) {
    snapshotCheckTileFill(tile, null, "unreadable");
    return;
  }
  if (!tile.isConnected) return;
  snapshotCheckTileFill(tile, recs.find((r) => r.state === "succeeded" || r.state === "failed") || null, "none");
}

// snapshotCheckTileFill paints the hero's check tile with the same words the
// Checks card uses (vfyVerdictWords): a tick only for a run that proved the
// copy matches, never for one that compared nothing.
function snapshotCheckTileFill(tile, latest, whyNone) {
  if (!tile) return;
  const w = vfyVerdictWords(latest, whyNone);
  tile.className = "hero-card hero-tile hero-check " + w.state;
  clear(tile);
  tile.append(icon(w.mark, "hero-ico"), el("span", { class: "hero-tile-t", text: w.title }));
  if (w.when) tile.append(el("span", { class: "hero-tile-s", text: w.when }));
}

// scrollPending is set by renderRoute when the address it is about to paint
// names a section, and cleared by the first paint that honors it. It is what
// keeps the jump to a SINGLE arrival: this page repaints itself on its own —
// when a job settles, on a saved setting, on a page of the list — and the
// hash stays in the address bar, so a scroll at the tail of every paint
// would drag a reader back down to #setup for as long as they stayed.
let scrollPending = false;

// scrollToSection brings the part of the page the address names into view,
// once. The browser does this itself only for a hash present when the
// DOCUMENT loaded; this page paints its sections after two round trips, long
// after that, so the element the hash names does not exist yet at that
// moment. A hash naming nothing (a section this console does not have, as on
// serve) is left alone: the reader stays at the top of the page, where the
// arrival note tells them why. `top` is passed ONLY when the note was placed
// beside the section this address names — scrolling the heading to the top
// edge would put that note just above the viewport, unread. Any other
// arrival passes nothing, and the heading itself is the target.
function scrollToSection(top) {
  if (!scrollPending) return;
  scrollPending = false;
  const id = (location.hash || "").slice(1);
  if (!id) return;
  const section = document.getElementById(id);
  if (!section) return;
  const target = top && top.isConnected ? top : section;
  if (target.scrollIntoView) target.scrollIntoView();
}

async function renderSnapshots() {
  const gen = serverGen, vgen = viewGen;
  // A repaint of the page already on screen, FOR THE SAME SERVER, does not
  // blank it. Those calls come from a job that finished, a setting that
  // saved or a page of the list — the reader is mid-page, and "Loading…" in
  // place of everything would drop them at the top of a page three times
  // longer than the one it replaced. (The job watcher POLLS every 2s; it
  // repaints once, when the run settles.) The swap below happens in one
  // turn, so the page is never empty between the two and the browser keeps
  // the reader where they were.
  //
  // A SERVER SWITCH is not that, and must blank: what is on screen is
  // another server's list, location and schedule, and its buttons are
  // closed over that server's id — a click in the gap (three or four
  // requests, then up to three more, one of them an S3 listing) would start
  // a backup, a restore or a .sql build on the server the reader just left. Arriving from another page
  // blanks too: there the view is somebody else's.
  const paintFor = gen + ":" + (currentServer || defaultServerId || "");
  // A repaint of the page already on screen (a job settled, a permission
  // probe) keeps the tab the reader opened; an arrival opens Versions unless
  // the address names a tab.
  const keepTab = backupsOnScreen() && backupsPaintedFor === paintFor;
  if (!keepTab) backupsHead = viewLoading();
  // Independent degradation, as on Storage: a panel renders its own failure
  // note rather than one error blanking the page. The same discipline covers
  // the DRAWING below, where `part` keeps one throwing section from taking
  // the other two with it.
  const asErr = (err) => ({ error: (err && err.message) || String(err) });
  const [serversRes, baselines, coverage, settings, storage] = await Promise.all([
    api("/api/servers").catch(asErr),
    api("/api/baselines").catch(asErr),
    // The hero colours the copy's age by the capture's state (a young copy
    // of a stopped capture is pink, not mint); a failed read paints nothing
    // about capture rather than a false green.
    api("/api/coverage").catch(() => null),
    // The bottom half is fetched with the rest, not after the first paint:
    // a page that grows a section under a reader who is already reading it
    // moves what they were looking at.
    // Only for a session that may read settings: for one that may not, the
    // answer is a 403 that drew a red "Could not load settings" box on every
    // visit, about a section this reader was never meant to see.
    sessionMay("settings:read") ? api("/api/backup-settings").catch(asErr) : Promise.resolve(null),
    // What signs S3 requests on this machine and what .sql builds wait on
    // its disk (#1867, from the dissolved This daemon page): read beside
    // the settings, under the same permission, and drawn where each is
    // asked about (the S3 field, the .sql lane).
    sessionMay("settings:read") ? api("/api/storage").catch(asErr) : Promise.resolve(null),
  ]);
  if (gen !== serverGen || vgen !== viewGen) return;
  snapStorage = storage;
  snapSharedWith = (baselines && baselines.shared_with) || [];
  snapRegistry = (serversRes && serversRes.servers) || [];
  // Run states for the selected server: only the endpoints this daemon
  // actually serves (each 403s when its feature is off).
  const selId = currentServer || defaultServerId;
  const [dumpSt, restoreSt, sqlSt] = await Promise.all([
    (capsCache.baseline_trigger && selId) ? api("/api/servers/" + encodeURIComponent(selId) + "/baseline").catch(() => null) : null,
    (capsCache.baseline_restore && selId) ? api("/api/servers/" + encodeURIComponent(selId) + "/baseline/restore").catch(() => null) : null,
    // asErr, not a swallow: the .sql build's outcome appears in exactly one
    // place on this page, so a failed status read has to reach the panel as
    // a failure rather than as "no build has ever run here".
    (capsCache.sql_export && selId) ? api("/api/servers/" + encodeURIComponent(selId) + "/sql-export").catch(asErr) : null,
  ]);
  if (gen !== serverGen || vgen !== viewGen) return;
  try {
    const servers = (serversRes && serversRes.servers) || [];
    // A failed /api/servers must be REPORTED, not absorbed into an empty list:
    // with servers=[] the Create baseline button silently disappears and the
    // empty state advises "Add a server first" — on the page that owns the
    // button. buildStorage keeps serversErr for the same reason.
    const serversErr = serversRes && serversRes.error;
    const v = VIEW(); clear(v);
    // One line under the title says what the page is about (round 3): the
    // word is new to most readers, and the hero below SHOWS the rest.
    v.append(backupsHead = pageHead("Snapshots", el("p", { class: "page-sub", text: "A snapshot is a copy of every table." })));
    backupsPaintedFor = paintFor;
    // Where this reader's old page went, read from the ALIAS TABLE rather
    // than a second list here, so the note and the address can never
    // disagree about which section they are talking about.
    const drawChecks = !!capsCache.monitor;
    const cur = servers.find((s) => s.id === (currentServer || defaultServerId));
    // The setup section is drawn when it has something to hold: the settings
    // half (settings:read), or the schedule card (which a view-only session
    // still gets whenever a schedule exists, for its run status). Built HERE
    // rather than inside its part because the arrival note below has to know
    // whether the section exists; a throw is kept and re-thrown inside the
    // part, so it still lands in that part's error box and nowhere else.
    let scheduleCard = null, scheduleErr = null;
    try { scheduleCard = backupScheduleCard(cur, baselines); } catch (err) { scheduleErr = err; }
    const drawSetup = !!settings || !!scheduleCard || !!scheduleErr;
    const movedTo = splitTarget(aliasTarget(routeArrivedFrom) || "")[1].replace("#", "");
    const sectionDrawn = (movedTo === "setup" && drawSetup) || (movedTo === "checks" && drawChecks);
    // The note goes BESIDE the section that reader asked for — but only while
    // the ADDRESS still points there, because the scroll follows the address:
    // a link into a part of the old page (/verification#past) keeps its own
    // anchor, and a note left beside Checks would be somewhere the reader
    // never looks. Then it leads the page instead.
    const beside = sectionDrawn && (location.hash || "").slice(1) === movedTo;
    // What to say when the section that reader's page became is not here:
    // the daemon runs the checks, unless we could not read what this server
    // supports at all, in which case saying "read-only" would be a false
    // statement about their installation.
    const missing = movedTo !== "" && !sectionDrawn ? (capsKnown ? "daemon" : "unknown") : "";
    // A setup section that is absent here is absent by PERMISSION (with the
    // capabilities read, settings are fetched whenever the session may read
    // them, and the section then always has content). Hidden by permission
    // says nothing, so there is no note promising what moved where.
    const setupHidden = movedTo === "setup" && !drawSetup && capsKnown;
    const moved = setupHidden ? null : snapshotsMovedNotice(missing);
    if (moved && !beside) v.append(moved);
    if (!capsKnown) v.append(el("div", { class: "error-box", text:
      "Parts of the Snapshots page are missing: the capability check failed when it loaded, so DBTrail does not know what this server supports. Reload the page." }));
    if (serversErr) v.append(el("div", { class: "error-box", text: "Could not load servers: " + serversErr }));
    // Three parts, three blast radii — as when they were three pages. A
    // verify record this build cannot read must not take the list of copies
    // down with it, and a settings row must not take both: an operator left
    // with one red box cannot tell whether their backups exist, let alone
    // which of the three things broke. Inside a part the pieces still share
    // one, exactly as they did on the page each came from.
    const part = (name, draw) => {
      try { draw(); } catch (err) {
        v.append(el("div", { class: "error-box", text: name + " could not be drawn: " + ((err && err.message) || String(err)) }));
      }
    };
    // The hero (round 3): where the copy lives, how fresh it is, its rhythm
    // over seven days and the last check, drawn, with the two actions.
    // Below it the three tabs (Versions, Checks, Settings); a tab this
    // daemon or session cannot serve is not offered.
    const takeAway = backupTakeAway(cur, baselines, sqlSt);
    const tabs = snapshotTabs({ checks: drawChecks, settings: drawSetup });
    part("The copy", () => {
      v.append(snapshotHero(baselines, coverage, cur, {
        // Download: one button for the newest copy (D1). It opens the
        // take-away lane on Versions, where the two downloads live.
        download: takeAway ? () => {
          tabs.select("versions");
          takeAway.open = true; takeAwayOpen = true;
          if (takeAway.scrollIntoView) takeAway.scrollIntoView({ block: "start" });
        } : null,
        settings: drawSetup ? () => tabs.select("settings") : null,
        mount: tabs.actions,
      }));
      // A visible in-progress region (mirrors the verification page): while a
      // backup is being created or restored, the page must look like a page
      // doing work, not a stale list. Above the tabs: a run shows whichever
      // tab is open.
      const running = backupRunsInFlight(dumpSt, restoreSt, baselines, sqlSt);
      if (running.length) {
        v.append(backupRunRegion(running));
        // One watcher at a time: renderSnapshots() outside renderRoute does not
        // bump viewGen, so without its own generation every re-render (its own
        // settle path included) would stack another 2s poller. The function
        // owns the counter, so a no-op spawn (no server id) cannot kill a
        // live watcher.
        watchBackupRuns(cur && cur.id, vgen, running.map((r) => r.kind));
      }
    });
    v.append(tabs.bar);
    // Versions: what you can do with a copy, above the list of copies — the
    // two lanes are the answer to why a reader opened it (what do I download
    // to open this in DuckDB, what do I download to load it into MySQL), and
    // the list is how you pick a different copy.
    part("The list of copies", () => {
      if (takeAway) tabs.panels.versions.append(takeAway);
      tabs.panels.versions.append(baselinesPanel(baselines, servers, { serversErr: serversErr }));
    });
    v.append(tabs.panels.versions);
    // Checks — the verification page, whole. Its runner is daemon-side, so
    // on a standalone serve the tab is absent rather than present and
    // unable to answer. The section heading keeps its id: /snapshots#checks
    // is the old page's address, and the moved note lands beside it.
    if (drawChecks) {
      const panel = tabs.panels.checks;
      if (moved && beside && movedTo === "checks") panel.append(moved);
      panel.append(snapshotSection("Checks", "checks"));
      // Three regions with visible separation (#1419): what you can run,
      // what is running or just ran, what ran before.
      part("Checks", () => {
        // Two columns (round 3): what you can run and what is running on
        // the left, what ran before on the right, so the verdict and the
        // list of past runs are read together.
        const regions = verifyRegions(servers, { serversErr: serversErr });
        const isHist = (r) => (" " + r.className + " ").includes(" vfy-histcard ");
        const left = el("div", { class: "snap-checks-l" }), right = el("div", { class: "snap-checks-r" });
        regions.forEach((r) => (isHist(r) ? right : left).append(r));
        panel.append(el("div", { class: "snap-checks" }, left, right.children.length ? right : null));
        // The verify guide, AFTER the section it describes: it lost its only
        // link when the three page headers became one (#1573) — the header
        // now opens the backup-strategy guide — and a page nothing links to
        // also stops being fetched by the daily link check, so the site
        // could move it and nobody would know.
        panel.append(docsMore("guides/verify", "", "what each check proves"));
      });
      v.append(panel);
    }
    // Settings — the schedule (#1442) first, because it is what makes the
    // list keep growing on its own and a failed scheduled run has to be
    // visible without opening anything; then every setting that shapes a
    // backup, beside where its value lives.
    if (drawSetup) {
      const panel = tabs.panels.settings;
      if (moved && beside && movedTo === "setup") panel.append(moved);
      panel.append(snapshotSection("Where and how often", "setup"));
      part("Where and how often", () => {
        if (scheduleErr) throw scheduleErr;
        // Whenever the session may read settings, this half always has
        // something to say: at the very least where this server keeps its
        // copies, or why that could not be read. The settings half needs
        // settings:read. Without it nothing was fetched (see above) and
        // nothing is drawn: hidden by permission says nothing, unlike a part
        // missing for a reason the reader can fix.
        const nodes = settings ? snapshotSetupSections(settings) : [];
        // The two cards about time, the schedule and the age retention,
        // share one row: the section label stays above it.
        const isCards = (n) => (" " + n.className + " ").includes(" cards ");
        const ci = nodes.findIndex(isCards);
        if (ci >= 0) {
          const row = el("div", { class: "stg-two stg-top" });
          if (scheduleCard) row.append(scheduleCard);
          nodes[ci].children.length && row.append(...Array.from(nodes[ci].children));
          nodes[ci] = row;
        } else if (scheduleCard) {
          nodes.unshift(scheduleCard);
        }
        nodes.forEach((n) => panel.append(n));
      });
      v.append(panel);
    }
    // The schedule card's alarms live on a tab that is closed by default:
    // the tab says so with a dot, and the hero says the fact itself
    // (snapshotHeroTone reads the same schedule).
    if (scheduleCard && scheduleCard.querySelector && scheduleCard.querySelector(".bk-card-state.alarm") && tabs.panels.settings) {
      const t = tabs.bar.querySelector('.snap-tab[data-tab="settings"]');
      if (t) { t.classList.add("has-alarm"); t.setAttribute("title", "The schedule needs attention"); }
    }
    // The tab the address names (the old pages' addresses, #checks and
    // #setup, open theirs), else the one the reader last opened. Quiet: the
    // address is the reader's, and a repaint must not rewrite it.
    // A copy with nothing in it yet (no place set, or no snapshot taken)
    // has nothing to list: the page opens on Settings, where the place and
    // the schedule are set, unless the address or the reader says otherwise.
    const nothingYet = baselines && !baselines.error && (!baselines.configured || !(baselines.snapshots || []).length);
    const first = nothingYet && drawSetup ? "settings" : "versions";
    const byReader = keepTab && !snapTabAuto;
    const fromHash = snapTabFromHash();
    tabs.select(fromHash || (byReader ? snapTab : first), true);
    snapTabAuto = !fromHash && !byReader;
    viewEnter();
    // Last: the sections exist now, so an address that names one can be
    // honored. Before this, there is nothing to scroll to.
    scrollToSection(beside ? moved : null);
  } catch (err) {
    const v = VIEW(); clear(v); v.append(pageHead("Snapshots", null)); renderError(v, err);
    // renderError clears the view first, heading included: what it leaves is
    // the page now, and a job that ends must still repaint it.
    backupsHead = v.lastElementChild;
    backupsPaintedFor = paintFor;
  }
}

// words: the value is a sentence, not a figure, so it takes the UI face (#1950).
function kvRow(card, k, val, words) {
  card.append(el("div", { class: "kv" },
    el("span", { class: "kv-k", text: k }),
    el("span", { class: "kv-v" + (words ? " words" : ""), text: val === null || val === undefined || val === "" ? "—" : String(val) })));
}

// rotationTiles draws what rotation does (#1950): the hours the index keeps
// (the newest pink), the ones it dropped (dashed, past the cut), where a
// dropped hour goes (the S3 tile: lit when a source archives, dashed when
// old data is deleted and not saved), and the partitions made ahead of time
// (dashed, after now). The keep-shape tiles of Snapshots, one drawing
// language. Fed by the settings it illustrates, so the dialog redraws it as
// the fields change. The text alternative says the same in words.
function rotationTiles(retain, interval, addFuture, archived) {
  const retainMin = flowEveryMinutes(retain), everyMin = flowEveryMinutes(interval) || 60;
  const kept = retainMin ? Math.max(1, Math.round(retainMin / everyMin)) : 0;
  const shown = Math.min(kept, 6), extra = kept - shown;
  const ahead = Math.max(0, Math.min(Number(addFuture) || 0, 4));
  const box = el("div", { class: "keep-shape rot-shape" });
  const row = el("div", { class: "ks-row" });
  // Lit (a kept tile, lower) when a source archives; the faded dashed tile of
  // a dropped hour when old data is deleted and not saved, or nothing is known.
  row.append(el("span", { class: archived === true ? "ks-tile ks-s3" : "ks-tile ks-gone", title: archived === true ? "archived to S3" : archived === false ? "not archived" : null }));
  row.append(el("span", { class: "ks-more", text: "←" }));
  row.append(el("span", { class: "ks-tile ks-gone" }), el("span", { class: "ks-tile ks-gone" }), el("span", { class: "ks-cut" }));
  if (extra > 0) row.append(el("span", { class: "ks-more", text: "+" + extra }));
  for (let i = 0; i < shown; i++) row.append(el("span", { class: "ks-tile" + (i === shown - 1 ? " ks-newest" : "") }));
  for (let i = 0; i < ahead; i++) row.append(el("span", { class: "ks-tile ks-next" }));
  if ((Number(addFuture) || 0) > ahead) row.append(el("span", { class: "ks-more", text: "+" + ((Number(addFuture) || 0) - ahead) }));
  box.append(row);
  const dropped = "dropped every " + (interval || "hour") + (archived === true ? ", archived to S3" : archived === false ? ", not saved" : "");
  box.append(el("div", { class: "ks-axis" },
    el("span", { class: "ks-left", text: dropped + (retain ? " · kept " + retain : "") }),
    el("span", { class: "ks-right", text: "now" + (ahead ? " · " + (Number(addFuture) || 0) + " ahead" : "") })));
  const alt = "Rotation: the index keeps " + (retain || "its window") + " of hourly partitions and drops the older ones every " + (interval || "hour")
    + (archived === true ? ", saving each to S3 before it is dropped" : archived === false ? "; a dropped hour is deleted, not saved" : "")
    + ((Number(addFuture) || 0) ? ", with " + addFuture + " partitions made ahead of time" : "") + ".";
  box.setAttribute("role", "img");
  box.setAttribute("aria-label", alt);
  return box;
}

function rotationCard(rot, archived) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Rotation" }));
  if (!rot || rot.error) {
    card.append(el("p", { class: "form-hint", text: "Could not load the rotation policy" + (rot && rot.error ? ": " + rot.error : ".") }));
    return card;
  }
  card.append(rotationTiles(rot.retain, rot.interval, rot.add_future, archived));
  kvRow(card, "retention", rot.retain + (rot.source === "override" ? "" : " for a new server"), rot.source !== "override");
  if (rot.source !== "override" && rot.index_retain && rot.index_retain !== rot.retain) {
    kvRow(card, "this server keeps", rot.index_retain + retentionBasis({ source: rot.source, basis: rot.index_basis }), true);
  }
  kvRow(card, "interval", rot.interval);
  kvRow(card, "future partitions", rot.add_future);
  kvRow(card, "policy", rot.source === "override"
    ? ("set in the web interface" + (rot.enabled ? " (live)" : ""))
    : "DBTrail's defaults", true);
  if (!rot.enabled) card.append(el("p", { class: "form-hint", text: "Rotation is turned off. Changes saved with Edit rotation take effect only after DBTrail restarts." }));
  card.append(el("div", { class: "stg-cardfoot" },
    el("button", { class: "btn btn-sm", type: "button", text: "Edit rotation…", onclick: showRotationDialog })));
  return card;
}

// docsMore is one plain link into the docs site for a compact block: the
// page under DOCS_BASE, optionally a section on it. Inert offline, like the
// header Docs link (air-gapped consoles are a first-class deployment).
// assets_docs_links_test.go requires every slug used here to be a page the
// header table already carries, so the network check covers it.
// ── S3 backup retention (#1622) ──────────────────────────────────────────────
//
// dbtrail never deletes an object from S3 and never sets a rule on a bucket
// (docs/s3-iam-policy.md: s3:DeleteObject is optional; docs/object-lock.md
// relies on it). So retention for uploaded backups is a bucket lifecycle
// rule the OPERATOR applies, and this page's job is to say how fast the
// bucket grows, generate the rule scoped to the backup prefix only, refuse
// it wherever it would reach anything else, and name what such a rule
// cannot do. Decided on #1622.

// s3Parts splits s3://bucket/prefix the way the daemon builds object keys
// (storage.BuildS3Key trims exactly ONE trailing slash and nothing else):
// no trimming of spaces, no collapsing of slashes. A prefix the page
// tidied up would name objects the daemon never wrote, and the rule would
// match nothing while the page said old backups expire. "" at the root.
function s3Parts(url) {
  const m = /^s3:\/\/([^\/]+)\/?(.*)$/.exec(String(url || ""));
  if (!m) return null;
  return { bucket: m[1], prefix: m[2].replace(/\/$/, "") };
}

// retentionTooShort: an age rule shorter than the schedule expires the
// newest complete backup before the next one lands, leaving moments with
// no backup at all. Equal counts: a 1-day rule under a 1-day schedule
// expires at the very moment the next run is due, not after it.
function retentionTooShort(everyMinutes, days) {
  return everyMinutes > 0 && days * 1440 <= everyMinutes;
}

// s3PrefixCovers reports whether a rule on `outer` (s3://bucket/prefix)
// would also expire objects under `inner`: same bucket, and inner's key
// prefix equals or sits under outer's, compared with the slash so that
// "dbtrail" does not cover "dbtrail-archives".
function s3PrefixCovers(outer, inner) {
  const o = s3Parts(outer), i = s3Parts(inner);
  if (!o || !i || o.bucket !== i.bucket) return false;
  return (i.prefix + "/").startsWith(o.prefix + "/");
}

// s3RetentionConflicts lists what else a rule on this server's backup
// prefix would expire (#1622): archived changes, this server's or any
// other's, which is a refusal (the evidence recovery is built from); and
// other servers' backups, by their RESOLVED destination so a server on the
// daemon default counts, plus the daemon default itself (the boot entry
// backs up there and is not in the list), which is said, not refused.
function s3RetentionConflicts(srv, servers, daemonS3) {
  const own = srv.baseline_s3;
  const archives = [], backups = [];
  if (srv.archive_s3 && s3PrefixCovers(own, srv.archive_s3)) archives.push("this server");
  for (const o of servers || []) {
    if (o.id === srv.id) continue;
    if (o.archive_s3 && s3PrefixCovers(own, o.archive_s3)) archives.push(o.name);
    if (o.resolved_s3 && s3PrefixCovers(own, o.resolved_s3)) backups.push(o.name);
  }
  if (daemonS3 && s3PrefixCovers(own, daemonS3)) backups.push("DBTrail's default (" + daemonS3 + ")");
  return { archives, backups };
}

// lifecycleRuleFor renders the bucket rule that expires objects under the
// backup prefix after `days`. Null at the bucket root: a rule with an empty
// prefix would expire everything in the bucket, the archived changes
// included, and this page must never hand that out. Noncurrent versions
// expire too: on a versioned bucket (Object Lock needs one) an Expiration
// alone only writes a delete marker and every byte stays.
function lifecycleRuleFor(url, days) {
  const s = s3Parts(url);
  if (!s || !s.prefix || !Number.isInteger(days) || days < 1) return null;
  return JSON.stringify({ Rules: [{
    ID: "dbtrail-backups-expire-" + days + "d",
    Status: "Enabled",
    Filter: { Prefix: s.prefix + "/" },
    Expiration: { Days: days },
    NoncurrentVersionExpiration: { NoncurrentDays: days },
  }] }, null, 2);
}

// s3ExpiryWords turns the server's read of the bucket's rules (#1680) into
// the sentence under the retention block, and whether it is a warning. Three
// states, and the third is its own: a bucket that could not be read is never
// written as "No rule" (a missing permission would raise an alarm on a
// bucket that is fine), and neither is an answer this page does not know.
// The covering test ran in the server (its twin of s3PrefixCovers, pinned
// against this file's by a test); the too-short test is retentionTooShort.
function s3ExpiryWords(v, srv, per30) {
  const days = (n) => n + " day" + (n === 1 ? "" : "s");
  const named = (id) => (id ? " (" + id + ")" : "");
  const state = v && typeof v === "object" ? v.state : "";
  if (state === "not_applicable") return { text: "", warn: false };
  if (state === "unreadable") {
    const unknown = ", so it is not known whether old snapshots expire";
    if (v.reason === "unsupported") return { text: "This S3 store does not answer when asked for its rules" + unknown + ".", warn: false };
    if (v.reason === "denied") {
      return { text: "Could not read this bucket's rules" + unknown + ". The likely reason is a missing permission: s3:GetBucketLifecycleConfiguration.", warn: false };
    }
    const why = String(v.error || "").replace(/[.\s]+$/, "");
    return { text: "Could not read this bucket's rules" + unknown + (why ? ": " + why : "") +
      ". If access was refused, the missing permission is s3:GetBucketLifecycleConfiguration.", warn: false };
  }
  if (state === "none") {
    const c = v.conditional || 0;
    return { warn: true, text: "No rule in the bucket expires these snapshots. " +
      (per30 > 0 ? "About " + per30 + (per30 === 1 ? " arrives" : " arrive") + " every 30 days and none leaves" : "Each one stays") +
      ": the bucket grows without limit." +
      (c ? " " + c + (c === 1 ? " rule on this prefix applies" : " rules on this prefix apply") +
        " only to objects with a tag or a size limit, and " + (c === 1 ? "is" : "are") + " not counted." : "") };
  }
  if (state !== "in_force" || !(v.days > 0 || v.expires_on)) {
    return { text: "Could not tell whether old snapshots expire.", warn: false };
  }
  const minutes = (srv && srv.schedule_every_minutes) || 0;
  let text = "", warn = false;
  if (v.days > 0) {
    text = "A rule " + (v.whole_bucket ? "on the whole bucket" : "in the bucket") + named(v.rule_id) +
      " expires these snapshots after " + days(v.days) + ".";
    if (v.old_versions_stay) text += " If the bucket keeps versions, the old ones stay: this rule does not expire them.";
    if (retentionTooShort(minutes, v.days)) {
      warn = true;
      text += " That is too short for snapshots every " + srv.schedule_every +
        ": the newest one expires before the next exists, so there are moments with no snapshot in S3 at all. The rule needs at least " +
        days(Math.floor(minutes / 1440) + 1) + ".";
    }
  }
  if (v.expires_on) {
    const who = (text ? " Another rule" : "A rule in the bucket") + named(v.date_rule_id);
    if (v.date_passed) {
      warn = true;
      text += who + " has been expiring every snapshot here since " + v.expires_on +
        ", whatever its age: a snapshot is removed soon after it arrives.";
    } else {
      text += who + " expires every snapshot here on " + v.expires_on + ", whatever its age.";
    }
  }
  return { text, warn };
}

// S3_EXPIRY_MS bounds the page's wait for that read. The server gives the
// bucket 5 s, so this only ends a request the server itself never answers.
const S3_EXPIRY_MS = 8000;

// s3ExpiryLine is the line itself: drawn at once with its waiting words and
// filled when the answer arrives, so the row never waits on a bucket. A
// request that fails says so; it is not an answer about the rules.
function s3ExpiryLine(srv, per30) {
  const line = el("p", { class: "form-hint s3-expiry", text: "Checking whether the bucket expires old snapshots." });
  const show = (w) => {
    line.hidden = !w.text;
    line.className = (w.warn ? "form-msg err" : "form-hint") + " s3-expiry";
    line.textContent = w.text;
  };
  // then().catch(), not then(a, b): an answer the sentence cannot be built
  // from must end as "could not" too, not leave the waiting words up.
  apiWithin("/api/servers/" + encodeURIComponent(srv.id) + "/snapshot-expiry", S3_EXPIRY_MS).then(
    (v) => show(s3ExpiryWords(v, srv, per30))).catch(
    (err) => show({ warn: false, text: "Could not ask whether the bucket expires old snapshots: " +
      String((err && err.message) || "no answer").replace(/[.\s]+$/, "") + "." }));
  return line;
}

// s3RetentionBox is the settings-row block for a server with its OWN S3
// destination (the raw entry, not the daemon default every server shares):
// the growth line when a schedule can run, then the rule behind a fold.
// Mounted whether or not a schedule runs: the Create backup button, a
// restore and the daemon-wide refresh all upload too, and a schedule that
// stopped leaves its backups sitting there.
function s3RetentionBox(srv, servers, daemonS3) {
  const wrap = el("div", { class: "s3-retention" });
  const s = s3Parts(srv.baseline_s3);
  // The stored interval gates the retention check whether or not the
  // schedule can run right now: a refusal is usually daemon-wide and
  // transient (lock mode, read-only console), and a 1-day rule handed out
  // while it lasts is exactly the rule the gate exists to refuse.
  const minutes = srv.schedule_every_minutes || 0;
  const runs = minutes > 0 && !srv.schedule_refusal ? Math.floor(30 * 1440 / minutes) : 0;
  // Full backups the timetable takes between two runs are backups of their
  // own (#1564), counted the way the Snapshots page counts them; one refused
  // takes none.
  const fulls = runs && srv.schedule_full_every && !srv.schedule_full_refusal ? backupsPer30Days(srv.schedule_full_every) : 0;
  const n = fulls ? runs + fulls - backupsPer30Days(lcmInterval(srv.schedule_every, srv.schedule_full_every)) : runs;
  wrap.append(el("p", { class: "form-hint", text:
    "Every snapshot sent to S3 is a full copy of every table, and DBTrail never removes one: each stays in the bucket until a rule in the bucket expires old snapshots." }));
  if (!s) {
    wrap.append(el("p", { class: "form-msg err", text:
      "This is not an s3://bucket/prefix destination, so no snapshot can be uploaded to it and no bucket rule applies." }));
    return wrap;
  }
  // Whether the bucket already expires them (#1680), read after the row is
  // drawn. Before the refusals below: a whole-bucket rule covers a
  // destination this page hands no rule out for.
  wrap.append(s3ExpiryLine(srv, n));
  const details = el("details", { class: "form-advanced s3-retention-rule" });
  details.append(el("summary", { class: "form-adv-summary", text: "Bucket rule to expire old snapshots" }));
  const body = el("div");
  const refuse = (text) => { body.append(el("p", { class: "form-msg err", text })); details.append(body); wrap.append(details); return wrap; };
  if (!s.prefix) {
    // The one shape refused outright: see lifecycleRuleFor.
    return refuse("These snapshots sit at the bucket root, which archived changes may share. A rule there would expire everything in the bucket. Put the snapshots under a prefix (for example s3://" + s.bucket + "/backups) before applying an expiry rule.");
  }
  // The archived changes are the evidence recovery is built from and are
  // never expired by dbtrail. A rule on a prefix that covers them (the
  // same prefix, or the archives nested under it), any server's, would.
  const conflicts = s3RetentionConflicts(srv, servers, daemonS3);
  if (conflicts.archives.length) {
    return refuse("The archived changes of " + conflicts.archives.join(", ") + " sit under the same prefix as these snapshots. A rule on " + s.prefix + "/ would expire the archived changes too. Move the snapshots or the archives to their own prefix first.");
  }
  // Other backups nested under this prefix would expire under THIS
  // server's retention. Said, not refused: the operator may want that.
  const nested = conflicts.backups;
  const days = el("input", { class: "input", type: "number", min: "1", step: "1", value: "30" });
  const row = el("label", { class: "field field--sm" });
  row.append(el("span", { class: "field-label", text: "Keep snapshots for (days)" }), days);
  const warn = el("p", { class: "form-msg err" });
  const rule = el("pre", { class: "stg-code" });
  const cmd = el("pre", { class: "stg-code" });
  const render = () => {
    // Digits only: "1e2" is a valid number-input value that would read as
    // 100 while the box shows 1e2.
    const d = /^\d+$/.test(days.value) ? Number(days.value) : NaN;
    const text = lifecycleRuleFor(srv.baseline_s3, d);
    if (!text) {
      rule.textContent = ""; cmd.textContent = "";
      warn.hidden = false;
      warn.textContent = "Enter a whole number of days, 1 or more.";
      return;
    }
    if (retentionTooShort(minutes, d)) {
      // Refused, not warned beside a payload the operator could still
      // copy: the rule would leave moments with no backup in S3 at all.
      const least = Math.floor(minutes / 1440) + 1;
      rule.textContent = ""; cmd.textContent = "";
      warn.hidden = false;
      warn.textContent = "With snapshots every " + srv.schedule_every + (srv.schedule_refusal ? " (the schedule is stored; it cannot run right now)" : "") +
        " and this rule at " + d + " day" + (d === 1 ? "" : "s") +
        ", the newest complete snapshot expires before the next one exists: there would be moments with no snapshot in S3 at all. Use at least " + least + " days.";
      return;
    }
    warn.hidden = true;
    rule.textContent = text;
    cmd.textContent = "aws s3api get-bucket-lifecycle-configuration --bucket " + s.bucket + "\n" +
      "aws s3api put-bucket-lifecycle-configuration --bucket " + s.bucket + " --lifecycle-configuration file://dbtrail-backups-rule.json";
  };
  days.addEventListener("input", render);
  render();
  body.append(row, warn);
  if (nested.length) {
    body.append(el("p", { class: "form-msg err", text:
      "The snapshots of " + nested.join(", ") + " sit under this prefix too and would expire under this rule." }));
  }
  body.append(
    el("p", { class: "form-hint", text: "Save the rule as dbtrail-backups-rule.json. The first command shows the rules the bucket already has; merge this one into them, since the second command replaces every rule on the bucket, such as one that aborts unfinished uploads, or the one-year rule that bintrail init --s3-bucket sets when it creates a bucket." }),
    rule, cmd,
    el("p", { class: "form-hint", text:
      "The rule applies to " + s.prefix + "/ only, and to no archived changes configured on the Snapshots page. It expires by age alone: it cannot spare the only complete copy, nor a snapshot a restore is reading, and if the schedule stops it keeps expiring until none is left." }),
    el("p", { class: "form-hint", text:
      "On a bucket with versioning the rule also expires old versions after the same number of days; under an Object Lock retention nothing can be expired before that retention ends." }),
    el("p", { class: "form-hint", text: "DBTrail never deletes from S3 and never changes a bucket's rules; this one is yours to apply." }));
  details.append(body);
  wrap.append(details);
  return wrap;
}

function docsMore(slug, section, label) {
  return el("p", { class: "form-hint bks-more" },
    el("a", { class: "bks-docs", href: DOCS_BASE + slug + "/" + (section ? "#" + section : ""),
      target: "_blank", rel: "noopener", text: "Read more: " + label }));
}

// ── Backup settings (#1582, #1603) ──────────────────────────────────────────
//
// The one page that owns backup and snapshot parameters. Its job is
// PROVENANCE: the precedence (per server, then daemon flag, then nothing) is
// real and used to be invisible — a server backed by the daemon's backup
// folder showed an empty field, indistinguishable from a server with no
// backup location at all.
//
// Two kinds of setting live here, and the LAYOUT tells them apart (#1603),
// not a sentence: the per-server rows change on this page and apply at once;
// the daemon's own values were set when the process started and change on
// restart, on a plain card outside the tinted grid with ONE restart chip at
// card level. Prose a reader does not need in order to act is compact by
// default (cnFine), never cut, and which location is in force is drawn
// (blCase).
//
// The disk-space card is gone (#1681). Once reusing an unchanged table became
// unconditional its "On" said nothing, and the saving it promised is only
// true for a server that keeps a copy on this machine, so the saving is now
// said beside that server's own yes/no, where it is true or not.

// snapshotSetupSections builds the "where and how often" half of Snapshots:
// every setting that shapes a backup, beside where its value lives. It
// RETURNS nodes instead of painting the view, because it is a part of a page
// now and not a page — the caller decides what comes before and after it,
// and whether the section heading is drawn at all.
function snapshotSetupSections(settings) {
  const out = [];
  const broken = settings && settings.error;
  if (broken) out.push(el("div", { class: "error-box", text: "Could not load settings: " + settings.error }));
  const sect = (t) => el("div", { class: "bks-sect", text: t });
  // The daemon-side cards are monitor-gated, not the page: serve runs no
  // refresh loop and its daemon rows would render as an unconfigured
  // install. The per-server panel is the page's serve-reachable half — the
  // only editor of the registry's backup fields. The section labels exist
  // only where there are two kinds to tell apart.
  const daemonRows = (settings && settings.daemon) || [];
  // The split is now by WHERE THE VALUE LIVES, which is what the operator can
  // act on: rows this interface can save sit under "Change here" beside the
  // other editable settings; only what still lives in the launch command is
  // under "Set when DBTrail starts". The second card disappears when it is
  // empty (#1682).
  // A session that may read settings but not write them still sees every
  // value, in the SAME card and with the same provenance line ("Saved here.
  // The command line says ..."), only locked and without its buttons. An
  // earlier cut moved these rows into the startup card, whose title and fine
  // print say "set at startup, change the flag and restart" -- false for a
  // value saved in this console.
  const mayEdit = sessionMay("settings:write");
  // Of the daemon rows only retention is one of the three settings the page
  // offers (D13); the rest live in the launch command and its docs.
  const editableRows = daemonRows.filter((row) => row.editable && SNAPSHOT_SETTING_KEYS.has(row.key));
  if (capsCache.monitor && !broken && editableRows.length) {
    if (!mayEdit) out.push(sect("Current settings"));
    out.push(el("div", { class: "cards cards-plain" }, backupDaemonEditCard(editableRows, !mayEdit)));
  }
  if (!broken) out.push(backupServersPanel(settings));
  return out;
}

// What each daemon-wide key means, in words a reader who never saw the flag
// can act on. The flag itself rides beside the value in the (CLI: ...) form.
// Vocabulary matches the per-server fields and the list of copies: Local
// folder, S3 location, refresh.
const SNAPSHOT_SETTING_KEYS = new Set(["baseline_retain"]);
const BACKUP_DAEMON_ROWS = {
  baseline_dir: "Default local folder",
  baseline_s3: "Default S3 location",
  baseline_retain: "Delete local snapshots older than",
  refresh_every: "Refresh snapshots every",
  lock_mode: "Lock while dumping",
  trigger: "Read database now button",
  staging_dir: ".sql build folder",
  verify_interval: "Verify every",
  verify_tables: "Verify only these tables",
};

// What an EMPTY daemon value means, per key (#1603). One word for all nine
// would lie: an empty Backup dir is no shared location at all, an empty
// interval is a loop that never runs, an empty table filter is every table.
// The word renders as a value in the muted style; "not set" read as a fault
// on nine rows of a healthy install. An empty lock_mode is the automatic
// choice (#1986: lock-all for an RDS or Aurora host, ftwrl elsewhere), not a
// missing value. trigger is a boolean; it carries an entry so the table stays
// one-to-one with the rows.
const BACKUP_DAEMON_EMPTY = {
  baseline_dir: "none",
  baseline_s3: "none",
  baseline_retain: "off",
  refresh_every: "off",
  lock_mode: "automatic",
  trigger: "Off",
  staging_dir: "temp folder",
  verify_interval: "off",
  verify_tables: "all tables",
};

// backupDaemonEditCard renders the daemon-wide rows this interface can save
// (#1682). One row, one input, one Save — and, when a value is saved, the way
// back to what the process was started with, because a setting that can only
// be overridden once is a trap.
//
// "Restart to change" survives here per row: saving a value and applying it
// are different facts. A row this daemon reads per job says nothing; a row it
// read once at boot says so beside its input, under its own value, so the
// operator knows the save landed and the effect has not.
function backupDaemonEditCard(rows, locked) {
  const card = el("div", { class: "card stg-card" });
  card.append(el("div", { class: "card-title stg-card-t" }, icon("clock", "stg-ico stg-ico-sun"), el("span", { text: "Delete by age" })));
  for (const row of rows) {
    card.append(backupDaemonEditRow(row, locked));
  }
  card.append(cnFine("More about these settings",
    el("p", { class: "form-hint", text:
      "Saved in DBTrail's own settings file, which wins over the command line and the environment." +
      (locked ? "" : " Use the startup value to go back to what the process was started with.") }),
    docsMore("settings/backups", "set-at-startup", "settings that need a restart")));
  return card;
}

function backupDaemonEditRow(row, locked) {
  const label = BACKUP_DAEMON_ROWS[row.key] || row.key;
  const wrap = el("div", { class: "bks-erow" });
  const head = el("label", { class: "form-label", for: "bks-" + row.key }, label);
  if (row.needs_restart) head.append(el("span", { class: "tag-pill bks-restart", text: "restart to apply" }));
  wrap.append(head);
  // The age retention shows an example instead of "off": a reader has to
  // know the grammar (days or hours) to type one.
  const input = el("input", { class: "input", id: "bks-" + row.key, type: "text",
    value: row.value || "", placeholder: row.key === "baseline_retain" ? "e.g. 7d" : (BACKUP_DAEMON_EMPTY[row.key] || "") });
  const msg = el("p", { class: "form-msg" });
  const save = el("button", { class: "btn btn-sm", type: "button", text: "Save" });
  const revert = row.source === "saved"
    ? el("button", { class: "btn btn-ghost btn-sm", type: "button", text: "Use the startup value" })
    : null;
  const send = async (body, button) => {
    button.disabled = true;
    if (revert) revert.disabled = true;
    save.disabled = true;
    try {
      // api() serializes the body itself; stringifying here too sent the
      // server a JSON string, which it refused as "cannot unmarshal string".
      await api("/api/backup-settings/daemon/" + encodeURIComponent(row.key), {
        method: "PUT", body });
      await renderSnapshots();
    } catch (err) {
      msg.className = "form-msg err";
      msg.textContent = (err && err.message) || String(err);
      save.disabled = false;
      if (revert) revert.disabled = false;
    }
  };
  save.addEventListener("click", () => send({ value: input.value }, save));
  if (revert) revert.addEventListener("click", () => send({ use_startup: true }, revert));
  // locked: a session without settings:write sees the value and where it
  // comes from, and no control that could only be refused.
  if (locked) input.disabled = true;
  wrap.append(el("div", { class: "bks-erow-in" }, input, locked ? null : save, locked ? null : revert));
  if (row.key === "baseline_retain") {
    wrap.append(el("p", { class: "form-hint", text: "Days or hours: 7d, 36h. Empty keeps them all. Deletes only where snapshots also go to S3, once S3 has each one. Without S3, copies younger than this stay even past the count." }));
  }
  // Provenance, in one line: what is winning, and what it is winning over.
  wrap.append(el("p", { class: "form-hint", text: row.source === "saved"
    ? "Saved in the web interface. The command line says " + (row.startup || "nothing") + "."
    : "From the command line or the environment (" + row.cli + ")." }));
  if (row.err) {
    wrap.append(el("p", { class: "form-msg err", text:
      "The value you set was refused and is not in force: " + row.err }));
  }
  wrap.append(msg);
  return wrap;
}


// BACKUP_SOURCE_CASES draws the answers to "where do this server's snapshots
// live", keyed by the EXACT Source values backup_settings_api.go emits;
// assets_backupsettings_test.go pins the two sets against each other, so a
// verdict added on either side rings instead of leaving a picture that shows
// something else. Two since #1684: a location of its own, or none. The
// middle case, reading DBTrail's startup folder, is gone with the fallback
// it explained.
const BACKUP_SOURCE_CASES = {
  server: { name: "Own location", reads: true, writes: true, say: "Snapshots, restores and time travel all work here." },
  none: { name: "No location", reads: false, writes: false, say: "This server keeps no snapshots, so time travel and restores do not work." },
};

// blCase renders one case row: the name, then a tick or a cross per lane.
// Marks are characters, not colour alone. `current` marks the row as this
// server's answer, which since #1573 is the only way it is drawn (the
// three-row legend that rendered all three unmarked is gone).
// fix: the closing instruction, only for a session that can act on it.
function blCase(source, current, fix) {
  const c = BACKUP_SOURCE_CASES[source];
  // A verdict this build does not know draws as unknown, with no lanes: two
  // crosses would read as "no location", which is a claim, not an unknown.
  if (!c) {
    return el("div", { class: "bl-case bl-unknown" + (current ? " is-current" : ""), "data-source": source || "",
      "aria-current": current ? "true" : null },
      el("span", { class: "bl-name", text: "Unknown" }),
      el("span", { class: "bl-lane", text: "DBTrail cannot read this server's snapshot state; update it" }));
  }
  // One sentence, not two lanes of ticks: what works from here and what
  // does not, in words. The mark says which case this is at a glance.
  return el("div", { class: "bl-case bl-" + (c.writes ? "ok" : c.reads ? "part" : "no") + (current ? " is-current" : ""), "data-source": source,
    "aria-current": current ? "true" : null },
    el("span", { class: "bl-mark", text: c.writes ? "✓" : c.reads ? "!" : "✗" }),
    el("span", { class: "bl-name", text: c.name }),
    el("span", { class: "bl-say", text: c.say + (fix && !c.writes ? " " + fix : "") }));
}

// backupServersPanel is the per-server half: the editable backup location and
// archive toggle, each with the provenance the servers API never showed. Each
// server shows its own case; the three-row legend that drew all of them once
// is gone (#1573 redesign).
// One server: the one chosen at the top of the page, so the count and the
// places on this card are about the same server as the list on Versions.
// Every server's row used to stack here, and "keep 3" under one name beside
// a list of eight under another was read as one page contradicting itself.
function backupServersPanel(settings) {
  const panel = el("section", { class: "ov-panel" });
  const servers = settings.servers || [];
  const selId = currentServer || defaultServerId;
  const own = servers.find((x) => x.id === selId);
  panel.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "This server" + (own ? " · " + (own.name || own.id) : "") })));
  if (!servers.length) {
    panel.append(el("p", { class: "form-hint", text: "No servers in the registry yet. Add one on the Servers page." }));
    return panel;
  }
  if (!own) {
    panel.append(el("p", { class: "form-hint", text:
      "The command-line server keeps its snapshots where DBTrail was started. Pick a server at the top of the page to set where its own copies live." }));
    return panel;
  }
  if (settings.registry_read_only) {
    panel.append(el("p", { class: "form-msg err", text:
      "The server registry was written by a newer version, so this DBTrail can only read it; values are shown but cannot be saved." }));
  }
  const unsaved = locationMigrationWords(settings.location_migration);
  if (unsaved) panel.append(el("p", { class: "form-msg err", text: unsaved }));
  // The daemon default S3 destination, for the retention block: the boot
  // entry backs up there and is not in the list.
  const daemonS3 = ((settings.daemon || []).find((r) => r.key === "baseline_s3") || {}).value || "";
  panel.append(backupServerRow(own, settings.registry_read_only, servers, daemonS3, !!settings.reuse_unchanged));
  if (servers.length > 1) {
    panel.append(el("p", { class: "form-hint bks-others", text:
      (servers.length - 1) + " other " + (servers.length === 2 ? "server keeps" : "servers keep") + " settings of their own: pick one at the top of the page to see them." }));
  }
  return panel;
}

// locationMigrationWords says that this start gave servers DBTrail's startup
// snapshot location as their own (#1684) and the registry file could not
// keep it, or "" when there is nothing to say. They read it now, and every
// start gives it to them again while the file stays as it is; a start
// without that location leaves them with none.
function locationMigrationWords(m) {
  if (!m || !m.not_saved) return "";
  const names = (m.servers || []).join(", ");
  return "The startup snapshot location was given to " + (names || "some servers") +
    ", but the server registry could not be saved: " + m.not_saved +
    ". They use it for now. If DBTrail starts without that location, they will have none.";
}

// s3OnlyBackupWarning is the Backup settings warning for a server whose own
// saved location is a bucket with no folder (#1659), or "" when it does not
// apply. An update from the recorded changes writes files, so it needs this
// server's Backup dir; without one a scheduled run can only be a full backup,
// and where this daemon cannot take one either, nothing runs at all. The
// values are compared as stored, untrimmed, the way the daemon reads them.
function s3OnlyBackupWarning(srv, fix = true) {
  // A saved schedule that cannot run already says why, more precisely.
  if (!srv || srv.baseline_dir || !srv.baseline_s3 || srv.schedule_refusal) return "";
  // fix: the closing instruction is for a session that can save this row.
  // The problem itself is said to everyone who can see the row.
  const then = (t) => fix ? " " + t : "";
  // Where this process runs no scheduled backups, only the setting is known.
  if (!srv.schedule_loop) return "With S3 only, a scheduled snapshot cannot update from the recorded changes." + then("Add a Local folder.");
  if (!srv.full_backup_possible) {
    return "With S3 only, scheduled snapshots cannot run on this server: a full read is not available here, and updating from the recorded changes needs a Local folder." + then("Add one.");
  }
  return "With S3 only, every scheduled snapshot reads your whole database." + then("Add a Local folder so runs update from the recorded changes.");
}

// localCopyWords is what the per-server yes/no means right now (#1681), from
// the form's CURRENT values, so the sentence follows the reader's clicks
// before a save. It returns the lines to show and whether each is a fault.
//
//   local  the yes/no as clicked         s3    the S3 field as typed
//   keep   the count as typed (0 = all)  loop  this process removes snapshots
//   reuse  unchanged tables keep their last file on this daemon
//   was    the saved answer, folder and provenance, and whether that folder is one
//          the prune never counts (shared, or DBTrail's own), or held (it
//          was shared and still holds the other server's snapshots)
//   reach  how far back the count reaches (localReachWords): the count in
//          force, the snapshot interval and the age retention, in minutes
//
// The "no" sentence is the issue's own words: no is not "no snapshots", it is
// "only in S3, and every run writes every table". The saving is promised
// only for a server whose copies are all here: with a bucket as well, a
// scheduled run reuses only when the folder holds the newest snapshot and a
// restore never does, which one sentence cannot carry without being false.
function localCopyWords(local, s3, keep, loop, reuse, was, reach) {
  const out = [];
  const say = (text, err) => out.push({ text, err: !!err });
  // First, and whatever the answers below are: it is about the place as
  // saved, local or S3, and it is the one line here that is about data.
  for (const w of was.shared || []) say(sharedLocationWords(w), true);
  if (!local) {
    if (!s3) {
      say("Set an S3 destination below first. With neither, this server keeps no snapshots at all.", true);
      return out;
    }
    say("Snapshots live only in S3, and every run writes every table.");
    if (was.local && was.dir) say("The snapshots already in " + was.dir + " stay there. DBTrail stops listing them and never removes them.");
    return out;
  }
  if (s3) {
    // With S3 the count does not apply: age does ("Delete by age"), and only
    // to a local copy S3 has confirmed (2026-10-01). Said for the place as
    // saved; a destination typed and not saved yet gets the rule, no numbers.
    const age = was.age || {};
    if (age.s3Only) say("No copy on this machine yet.");
    else if (!age.asSaved) say("After you save, Delete by age applies here.");
    else if (!loop) say("Kept here: this DBTrail removes nothing.");
    else if (age.minutes > 0) say("Deleted here when older than " + exactSpan(age.minutes) + ", once S3 has it. DBTrail leaves S3 alone.");
    else say("Kept here: Delete by age is empty.");
    // The count is the last age prune's, so it is shown only where that
    // prune still runs (this daemon prunes, an age is set) and with its date:
    // with no age prune nothing refreshes it, and an old number would read
    // as today's.
    const live = age.asSaved && loop && age.minutes > 0;
    const n = live && age.notIn ? age.notIn.count : 0;
    if (n > 0) say("Not in S3, so kept here: " + n + " older " + (n === 1 ? "copy" : "copies") + " (checked " + utcLabel(age.notIn.at) + "). The log says why.", true);
    if (live && age.notInError) say("S3 state unknown: " + age.notInError, true);
    return out;
  }
  if (reuse) say("A table that did not change keeps its last file, so a new snapshot only costs the tables that changed.");
  if (was.held) {
    say("Another server's snapshots are still in this folder, so nothing here is removed. To keep only the newest, use a new empty folder.");
  } else if (was.blocked) {
    say("This folder is shared with another server or is DBTrail's startup folder, so nothing in it is removed, whatever the count says.");
  } else if (!keep) {
    say("Every snapshot stays on this machine; nothing removes them. Set a number to keep only the newest.");
  } else if (loop) {
    // Said even when only the count changed: saving a number over a folder
    // that already holds snapshots IS the choice to prune them (a new folder
    // or a moved one is refused instead), so the row says what that means.
    // The number itself is in the field beside this and, once in force, in
    // the line above the snapshot list (snapshotRetentionLines); said a third
    // time here it would only be the same fact again.
    say("Older ones past that number are removed from this folder at the next hourly cleanup, never a table's only copy.");
    say(localReachWords(keep, reach || {}));
  } else {
    say("Keeps the newest " + keep + " where DBTrail takes the snapshots. This copy of DBTrail removes nothing.");
  }
  return out;
}

// sharedLocationWords says that a place holds snapshots of more than one
// installation (#1762), from one entry of GET /api/baselines shared_with:
// who the other one is, by the id it signs with, and what to do. Nothing is
// refused, so it says what can go wrong rather than what was stopped.
function sharedLocationWords(w) {
  const ids = (w.others || []).join(", ");
  const who = w.own
    ? (w.others.length === 1 ? "Another DBTrail writes its snapshots here too: " : "Other copies of DBTrail write their snapshots here too: ") + ids + "."
    : "More than one DBTrail writes its snapshots here: " + ids + ".";
  return who + " Their snapshots mix in " + w.source + ", and a read can return the other one's data. Give each its own folder or S3 prefix.";
}

// keepShapeDraw draws what "keep the newest N" means: N tiles, the newest
// lit at the right, two faded tiles past the count on the left (the ones
// the cleanup removes), and under them how far back the oldest kept
// reaches: a span when the copy is on a schedule, "the oldest kept"
// when it is not. A count of 0 keeps them all: one open row, no faded
// tiles. Built with el() and CSS like duckdbShape, never svgEl.
function keepShapeDraw(box, keep, everyMin, retainMin) {
  const row = el("div", { class: "ks-row" });
  const all = keep === 0;
  if (!all) {
    row.append(el("span", { class: "ks-tile ks-gone" }), el("span", { class: "ks-tile ks-gone" }));
    row.append(el("span", { class: "ks-cut" }));
  }
  const shown = all ? 5 : Math.min(keep, 6);
  const extra = all ? 0 : keep - shown;
  if (extra > 0) row.append(el("span", { class: "ks-more", text: "+" + extra }));
  for (let i = 0; i < shown; i++) row.append(el("span", { class: "ks-tile" + (i === shown - 1 ? " ks-newest" : "") + (all && i === 0 ? " ks-open" : "") }));
  box.append(row);
  const mins = keep && everyMin ? Math.max(keep * everyMin, 60, retainMin || 0) : 0;
  box.append(el("div", { class: "ks-axis" },
    el("span", { class: "ks-left", text: all ? "every snapshot, kept" : (mins ? "about " + reachSpan(mins) + " ago" : "the oldest kept") }),
    el("span", { class: "ks-right", text: "now" })));
}

// localReachWords is how far back a server can go with the newest `keep`
// snapshots kept (#1681), from its real schedule:
//
//   keep      the count as typed
//   inForce   the count the prune applies now (keep_in_force, the listing's
//             local_retention.keep_newest); a different number means the
//             typed one is not saved or not applied yet, and the line says so
//   every     how often snapshots are taken on their own, in minutes: the
//             schedule and the refresh loop together, from the server
//             (snapshot_every_minutes); 0 = nothing takes them on its own
//   retain    the age retention in minutes (0 = none)
//
// keep x every, but never under the hour the prune always leaves alone, nor
// under the age retention, which keeps younger snapshots outside the count.
// "Up to": with one kept, right after a new snapshot the window is shorter.
// Without a schedule there is no number to give, and the line says what
// decides it instead.
function localReachWords(keep, reach) {
  // "After you save" while the typed number is not the one in force yet.
  const lead = reach.inForce === keep ? "You can" : "After you save, you can";
  if (!reach.every) {
    return lead + " restore back to " + (keep === 1 ? "the one snapshot kept" : "the oldest of the " + keep + " kept") +
      ". Put the copy on a schedule (Update the copy, above) and this becomes a number of hours.";
  }
  const mins = Math.max(keep * reach.every, 60, reach.retain || 0);
  return lead + " restore back to about " + reachSpan(mins) + " ago: the oldest of the " + keep + " kept is that old.";
}

// reachSpan says a number of minutes the way a person would: hours below a
// day, whole days as days, hours again up to two days, days above.
// exactSpan states a threshold the prune applies exactly (Delete by age), so
// it never rounds: whole days only when the value is whole days, else hours,
// else minutes. reachSpan below is the rounded "about how far back" wording.
function exactSpan(mins) {
  const plural = (n, w) => n + " " + w + (n === 1 ? "" : "s");
  if (mins % 1440 === 0) return plural(mins / 1440, "day");
  if (mins % 60 === 0) return plural(mins / 60, "hour");
  return plural(mins, "minute");
}

function reachSpan(mins) {
  const day = 24 * 60;
  if (mins < day || (mins % day !== 0 && mins < 2 * day)) {
    const h = Math.max(1, Math.round(mins / 60));
    return h + (h === 1 ? " hour" : " hours");
  }
  const d = Math.round(mins / day);
  return d + (d === 1 ? " day" : " days");
}

function backupServerRow(srv, readOnly, servers, daemonS3, reuse) {
  const box = el("div", { class: "bks-server" });
  box.append(el("h3", { class: "bks-server-name", text: srv.name || srv.id }));
  // The one question (#1681): does this server keep a copy of its snapshots
  // on this machine. Yes shows where, and how many are kept when there is no
  // S3 destination; no hides both and is never SENT unless it was clicked.
  const q = "bks-local-" + srv.id;
  const yes = el("input", { type: "radio", name: q, value: "yes" });
  const no = el("input", { type: "radio", name: q, value: "no" });
  // The local copy is always yes (D13): the question left the page, and a
  // save from here always says so. The two inputs stay as the form's state,
  // in the row but hidden.
  yes.checked = true;
  no.checked = false;
  box.append(el("fieldset", { class: "bks-q", hidden: true },
    el("legend", { class: "field-label", text: "Keep a copy of this server's snapshots on this machine?" }),
    el("label", { class: "check" }, yes, el("span", { text: "Yes" })),
    el("label", { class: "check" }, no, el("span", { text: "No, only in S3" }))));
  // Two cards (round 3): the count to keep, as a stepper around the number,
  // and the two places the copy lives, as tiles the folder and the bucket
  // are typed into. The inputs keep their names: the save reads them, the
  // browser tests find them by name.
  const grid = el("div", { class: "stg-two" });
  // The folder a yes uses: this server's own, or the default one named after
  // its id when it has none yet. Shown, never silently sent: a no omits it.
  const dirWas = (srv.baseline_dir || srv.default_dir || "").trim();
  const dir = el("input", { class: "input where-in", name: "baseline_dir", value: dirWas, placeholder: srv.default_dir || "/full/path/to/a/folder" });
  const s3 = el("input", { class: "input where-in", name: "baseline_s3", value: srv.baseline_s3 || "", placeholder: "s3://bucket/prefix/" });
  const keep = el("input", { class: "input keep-n", name: "keep_newest", type: "number", min: "0", step: "1",
    value: srv.keep_newest ? String(srv.keep_newest) : "", placeholder: "all", "aria-label": "Keep the newest" });
  const step = (d) => {
    const n = /^\d+$/.test(keep.value.trim()) ? Number(keep.value.trim()) : 0;
    keep.value = Math.max(0, n + d) ? String(Math.max(0, n + d)) : "";
    keep.dispatchEvent(new Event("input"));
  };
  const minus = el("button", { class: "btn btn-icon keep-btn", type: "button", "aria-label": "Keep one fewer", text: "\u2212", onclick: () => step(-1) });
  const plus = el("button", { class: "btn btn-icon keep-btn", type: "button", "aria-label": "Keep one more", text: "+", onclick: () => step(1) });
  const unit = el("span", { class: "keep-unit", text: "snapshots" });
  // One card, one rule (2026-10-01): a count where the copies live only on
  // this machine, age (Delete by age) where they also go to S3. The title and
  // the stepper follow the mode, so a server never shows a rule it does not obey.
  const keepTitle = el("span", { class: "field-label", text: "Keep by count" });
  const keepStep = el("div", { class: "keep-step" }, el("span", { class: "keep-lead", text: "the newest" }), minus, keep, plus, unit);
  const keepField = el("div", { class: "stg-card keep-card" },
    el("div", { class: "stg-card-t" }, icon("layers", "stg-ico stg-ico-mint"), keepTitle), keepStep);
  // The drawing: the copies kept, newest at the right, the ones past the
  // count fading out on the left, and how far back the oldest reaches.
  const shape = el("div", { class: "keep-shape" });
  keepField.append(shape);
  const words = el("div", { class: "bks-local-words" });
  keepField.append(words);
  const wordsMore = el("div", { class: "bks-local-words" });
  // What goes under "More about this server", in the order it is pushed.
  const more = [wordsMore];
  const tile = (ico, label, input) => el("label", { class: "where-tile" }, icon(ico, "where-ico"),
    el("span", { class: "where-body" }, el("span", { class: "field-label", text: label }), input));
  const dirField = tile("folder", "Local folder", dir);
  const s3Field = tile("bucket", "S3 location", s3);
  const whereCard = el("div", { class: "stg-card where-card" },
    el("div", { class: "stg-card-t" }, icon("pin", "stg-ico stg-ico-orange"), el("span", { class: "field-label", text: "Where it lives" })),
    dirField, s3Field);
  grid.append(keepField, whereCard);
  box.append(grid);
  // What signs S3 requests, under the field the bucket is typed into (#1867):
  // shown only while an S3 location is in play, so a folder-only server
  // never reads about AWS.
  const signing = s3SigningNote(s3, srv, snapStorage, snapRegistry);
  if (signing) box.append(signing);
  // S3 without a folder (#1659): said in red next to the two fields, schedule
  // or not. From the SAVED values, the ones the schedule reads
  // (rebuildPossible): a daemon default folder does not save this server,
  // and a warning that followed the typing would vanish on a save that
  // failed. The page redraws after a successful save.
  const s3Only = s3OnlyBackupWarning(srv, sessionMay("servers:write"));
  if (s3Only) box.append(el("p", { class: "form-msg err", text: s3Only }));
  // The archive toggle is not one of the three settings (D13): it keeps its
  // saved value and stays off the page (CLI: --no-archive).
  const noArch = el("input", { type: "checkbox", name: "no_archive" });
  noArch.checked = !!srv.no_archive;

  // Whether this server has a location at all, drawn as its own case row.
  // Since #1684 there is no third answer: reads and writes use the same
  // saved entry, so the row can never promise time travel from a place the
  // writes refuse. The line under it says what to do.
  const src = srv.source;
  whereCard.append(blCase(src, true, sessionMay("servers:write") ? "Type one above and Save." : ""));
  // The refusal beats the prediction: the schedule reads the RAW entry, so
  // clearing the dir on this very page leaves a stored schedule that will
  // refuse every slot. Never compact.
  if (srv.schedule_refusal) {
    box.append(el("p", { class: "form-msg err", text:
      "The schedule cannot run as things stand: " + srv.schedule_refusal }));
  }
  const p = (t) => el("p", { class: "form-hint", text: t });
  if (srv.schedule_every) {
    more.push(p("Scheduled snapshots: every " + srv.schedule_every + (srv.schedule_at ? " at " + srv.schedule_at : "") +
      (srv.schedule_full_every ? ", with a full read every " + srv.schedule_full_every : "") +
      // Where the timetable is CHANGED, and only where it can be: the card
      // it lives on is drawn for the SELECTED server, and only where this
      // process runs the schedules. Pointing at "the card above" on a
      // read-only console named something that is not on the screen.
      (!capsCache.backup_schedule
        ? ". Schedules run in the DBTrail service; this web interface cannot change them."
        : sessionMay("servers:write") ? ". Select this server at the top of the page to change it." : ".")));
    // Red here as on the schedule card (#1564): a grey summary beside a red
    // card would be the same page disagreeing with itself about one schedule.
    if (srv.schedule_full_refusal && !srv.schedule_refusal) {
      const why = String(srv.schedule_full_refusal);
      more.push(el("p", { class: "form-msg err", text: why.charAt(0).toUpperCase() + why.slice(1) +
        (/[.!?]$/.test(why) ? "" : ".") + " The full reads do not run until that changes; the other scheduled runs still do." }));
    }
  } else {
    more.push(p(!capsCache.backup_schedule
      ? "No scheduled snapshots. Setting one needs the DBTrail service; this web interface is read-only."
      : sessionMay("servers:write") ? "No scheduled snapshots. Select this server at the top of the page to set one." : "No schedule."));
  }
  // S3 keeps every uploaded backup forever unless the BUCKET expires it
  // (#1622): say how fast it grows, and hand over the rule to apply. The
  // server's OWN destination only: the daemon default is shared by every
  // server, and a rule on it is not this row's to hand out.
  if (srv.source === "server" && srv.baseline_s3) more.push(s3RetentionBox(srv, servers, daemonS3));
  more.push(docsMore("settings/backups", "per-server", "snapshot locations per server"),
    // The strategy guide is the page the header table names for Snapshots;
    // the card that linked it from the setup half is gone (#1681).
    docsMore("guides/backup-strategy", "", "how DBTrail backs up your database"));

  const msg = el("p", { class: "form-msg err" });
  msg.hidden = true;
  // Filled only while this form differs from what was loaded (sync): the
  // Settings tab holds two Saves, and the one being changed is the one the
  // eye should find (#1950). data-save is the hook the browser scenes press.
  const save = el("button", { class: "btn", type: "button", text: "Save", "data-save": "location" });
  // Two different reasons to be read-only. registry_read_only is the
  // registry file itself (a newer version wrote it), and the Save stays
  // visible, disabled, beside the reason. A session without servers:write
  // (the save goes to PUT /api/backup-settings/servers/{id}) gets the
  // answers locked and no Save at all: hidden by permission.
  const mayWrite = sessionMay("servers:write");
  const locked = !!readOnly || !mayWrite;
  if (locked) { dir.disabled = s3.disabled = noArch.disabled = yes.disabled = no.disabled = keep.disabled = minus.disabled = plus.disabled = true; }
  // Save wakes up when something differs from what was loaded, so a click
  // always means a change; Enter in a field saves too.
  // Trimmed on both sides: the PUT trims, so a stored value with stray
  // whitespace is not a change waiting to be saved.
  // local: true, whatever was stored: the answer is always yes now, so a
  // server saved as S3-only does not wake Save on its own; its next save from
  // here turns the local copy on along with whatever changed.
  const was = { local: true, dir: dirWas, rawDir: (srv.baseline_dir || "").trim(),
    s3: (srv.baseline_s3 || "").trim(), keep: srv.keep_newest || 0, noArch: !!srv.no_archive };
  // The count as typed: "" is 0 (keep them all); anything else must be a
  // whole number, or it is null and Save refuses it.
  const keepNow = () => {
    const t = keep.value.trim();
    if (t === "") return 0;
    return /^\d+$/.test(t) ? Number(t) : null;
  };
  const dirty = () => yes.checked !== was.local || s3.value.trim() !== was.s3 || noArch.checked !== was.noArch ||
    (yes.checked && (dir.value.trim() !== was.dir || keepNow() !== was.keep));
  const paint = () => {
    const local = yes.checked;
    const s3v = s3.value.trim();
    dirField.hidden = !local;
    // A tile lights when the place is this server's OWN: a folder typed here
    // (the daemon's default, shown as the placeholder value, is not), or a
    // bucket. The verdict row under them says the same in words.
    const ownDir = dir.value.trim() !== "" && (was.rawDir !== "" || dir.value.trim() !== (srv.default_dir || "").trim());
    dirField.classList.toggle("on", local && ownDir);
    s3Field.classList.toggle("on", !!s3v);
    // With S3 the count does nothing (S3 keeps everything), so the stepper
    // goes and the words say what happens instead.
    minus.hidden = plus.hidden = keep.hidden = unit.hidden = !local || !!s3v;
    keepStep.hidden = keep.hidden;
    // Saved with S3 and no folder of its own, untouched: the folder field
    // shows the default only as a suggestion (Save stays asleep), so nothing
    // is on this machine and the card must not promise "after you save".
    const s3OnlyAtRest = !was.rawDir && !!was.s3 && s3v === was.s3 && dir.value.trim() === was.dir;
    keepTitle.textContent = !local ? (s3v ? "Only in S3" : "No snapshots") : s3OnlyAtRest ? "Only in S3" : s3v ? "On this machine" : "Keep by count";
    clear(words);
    // The reach is said for the count as it applies to the folder as saved:
    // a typed folder or destination makes it a number that does not apply yet.
    const asSaved = dir.value.trim() === was.dir && s3v === was.s3;
    // The reuse note stays off this card (the refresh line on Versions
    // says it): this card is about the count and only the count.
    clear(wordsMore);
    // The card keeps the refusals and the one line that answers "how far
    // back can I go"; the rest of what the count means waits under "More
    // about this server", so the card is read at a glance.
    const said = localCopyWords(local, s3v, keepNow() || 0, !!srv.prune_loop, !!reuse && !!capsCache.monitor,
      { local: was.local, dir: was.rawDir, source: srv.source, blocked: !!srv.keep_blocked, held: !!srv.keep_held,
        age: { asSaved: asSaved && !!was.s3 && !!was.rawDir, s3Only: s3OnlyAtRest, minutes: srv.delete_after_minutes || 0, notIn: srv.not_in_s3 || null, notInError: srv.not_in_s3_error || "" },
        shared: asSaved ? snapSharedWith : [] },
      { inForce: asSaved ? (srv.keep_in_force || 0) : -1,
        every: srv.snapshot_every_minutes || 0, retain: srv.prune_retain_minutes || 0 });
    const infos = said.filter((w) => !w.err);
    said.forEach((w) => {
      const keepHere = w.err || w === infos[infos.length - 1];
      (keepHere ? words : wordsMore).append(el("p", { class: w.err ? "form-msg err" : (keepHere ? "form-hint keep-reach" : "form-hint"), text: w.text }));
    });
    clear(shape);
    shape.hidden = keep.hidden;
    if (!shape.hidden) keepShapeDraw(shape, keepNow() || 0, srv.snapshot_every_minutes || 0, srv.prune_retain_minutes || 0);
  };
  const sync = () => { paint(); save.disabled = locked || !dirty(); save.classList.toggle("btn-primary", !save.disabled); };
  for (const input of [dir, s3, keep]) {
    input.addEventListener("input", sync);
    input.addEventListener("keydown", (e) => { if (e.key === "Enter" && !save.disabled) save.click(); });
  }
  for (const r of [yes, no]) r.addEventListener("change", sync);
  noArch.addEventListener("change", sync);
  sync();
  save.onclick = async () => {
    save.disabled = true;
    msg.hidden = true;
    // What is sent follows the answer, not the fields on screen: a no sends
    // local_copy false and never the folder or the count still sitting in
    // the hidden inputs; a yes always says so, so an emptied folder means
    // "the default one" and never "no copy anywhere".
    const body = { baseline_s3: s3.value.trim(), no_archive: noArch.checked };
    if (yes.checked) {
      body.local_copy = true;
      body.baseline_dir = dir.value.trim();
      // The count only while its field is shown (no S3): hidden, it does
      // nothing, so it is neither checked nor sent.
      if (!s3.value.trim()) {
        const k = keepNow();
        if (k === null) {
          msg.textContent = "Keep the newest takes a whole number, or nothing to keep them all.";
          msg.hidden = false;
          sync();
          return;
        }
        if (k !== was.keep) body.keep_newest = k;
      }
    } else if (was.local) {
      body.local_copy = false;
    }
    try {
      await api("/api/backup-settings/servers/" + encodeURIComponent(srv.id), {
        method: "PUT",
        body: body,
      });
    } catch (err) {
      msg.textContent = (err && err.message) || String(err);
      msg.hidden = false;
      sync();
      return;
    }
    toast("Saved for " + (srv.name || srv.id));
    // Repaint from the server's answer, not from what was clicked: the
    // provenance row depends on the resolution the daemon just recomputed.
    // renderSnapshots(), like the daemon-row Save beside it: the route path
    // bumps viewGen, which kills the job watchers this page is running.
    await renderSnapshots();
  };
  // Save comes after the drawing and any refusal, above the compact block,
  // as on the disk-space card: the block is for reading, not for acting.
  if (mayWrite) box.append(el("div", { class: "stg-cardfoot" }, save), msg);
  box.append(cnFine("More about this server", ...more));
  return box;
}

// awsSigningSummary is the one sentence about what signs this machine's S3
// requests, from the credential signals GET /api/storage reports.
// Presence, not use: each arm reports the signal it saw and nothing about
// whether that signal is what the AWS chain resolves to.
function awsSigningSummary(aws) {
  let summary = "No credentials set directly; DBTrail relies on your AWS environment (for example, an EC2 instance role) to provide them automatically.";
  // AccessKeyEnv is AWS_ACCESS_KEY_ID ALONE (storage_api.go), so an ID
  // exported without its secret used to render "Using access keys" while the
  // SDK's env provider yields nothing and the chain walks on past it.
  if (aws.access_key_env) summary = "Found an access key ID set in an environment variable. Its secret key is not checked here, so this may not be what signs the requests.";
  // The ECS arm stays env-var presence: probing the endpoint is a network
  // call and a separate decision (#1534), so the copy claims exactly what
  // was seen and no more.
  else if (aws.container_creds) summary = "Found the ECS task-role endpoint in the environment. Whether the endpoint answers is not checked here, so this may not be what signs the requests.";
  // The web-identity arm is PROBED since #1534: the provider needs the token
  // file readable AND AWS_ROLE_ARN, and asserting a role from the variable
  // alone rendered "Using an IAM role" over a stale or unmounted token, on
  // the page an operator opens precisely because S3 is not working. The two
  // broken shapes speak first, because they are the ones with a fix to name.
  else if (aws.web_identity) {
    if (!aws.web_identity_token_readable) {
      summary = "An EKS service-account role is configured, but its token file cannot be read (missing, not a file, or not readable by DBTrail), so it cannot sign anything.";
    } else if (!aws.web_identity_role_arn) {
      summary = "An EKS service-account token is readable, but AWS_ROLE_ARN is not set. The provider needs both, so this cannot sign requests.";
    } else {
      summary = "Found an EKS service-account role: the token file is readable and a role is named. Whether AWS accepts it is not checked here.";
    }
  }
  // TWO independent probes select this arm, and the sentence has to name
  // both: hasSharedAWSConfig() stats a file; aws.profile is AWS_PROFILE in
  // the environment. An EC2 box with an instance role and a region-only
  // ~/.aws/config (what `aws configure set region` writes) lands here with
  // no credentials in either place.
  else if (aws.shared_config || aws.profile) summary = "Found an AWS profile name or a shared ~/.aws config file, which may hold credentials or only a region. An IAM role on this machine can still be what signs the requests.";
  return summary;
}

// awsSignalsFold lists the raw signals behind the sentence. Each row names
// the ONE variable that was read: "access keys (env): set" under a sentence
// saying the secret key was not checked was a self-contradiction.
function awsSignalsFold(aws) {
  const adv = el("details", { class: "form-advanced" },
    el("summary", { class: "form-adv-summary", text: "Raw signals" }));
  kvRow(adv, "access key ID (env)", aws.access_key_env ? "set" : "not set");
  kvRow(adv, "profile (env)", aws.profile || "not set");
  kvRow(adv, "region (env)", aws.region_env || "not set");
  kvRow(adv, "~/.aws config", aws.shared_config ? "present" : "absent");
  if (aws.container_creds) kvRow(adv, "ECS task role", "endpoint variable set");
  if (aws.web_identity) {
    kvRow(adv, "EKS IRSA token", aws.web_identity_token_readable ? "readable" : "unreadable");
    kvRow(adv, "role ARN (env)", aws.web_identity_role_arn ? "set" : "not set");
  }
  return adv;
}

// s3SigningNote answers "why does nothing reach my bucket?" where the bucket
// is typed (#1867; the card was on the This daemon page). Hidden while the
// S3 field is empty and shown as soon as one is typed, so a folder-only
// server never reads about AWS. A server with its own access key (Manage
// servers) signs with it, and the machine's signals do not apply to it.
// null when the storage signals were not read (a session that may not read
// settings), never a note about a missing note.
function s3SigningNote(s3Input, srv, storage, registry) {
  if (!storage) return null;
  const note = el("div", { class: "s3-signing" });
  const sync = () => { note.hidden = !(s3Input.value || "").trim(); };
  s3Input.addEventListener("input", sync);
  sync();
  const entry = (registry || []).find((r) => r.id === srv.id);
  if (entry && entry.s3_access_key_id) {
    note.append(el("p", { class: "form-hint", text: "S3 requests for this server are signed with its own access key, set in Manage servers." }));
    return note;
  }
  const aws = storage.aws;
  if (!aws) {
    note.append(el("p", { class: "form-hint", text: "Could not read what signs S3 requests on this machine" + (storage.error ? ": " + storage.error : ".") }));
    return note;
  }
  note.append(el("p", { class: "form-hint" }, el("b", { text: "S3 requests from this machine: " }), el("span", { text: awsSigningSummary(aws) })));
  note.append(awsSignalsFold(aws));
  return note;
}

// stagedDownloadsNote says what .sql builds wait on this machine's disk
// (#1448; on the This daemon page until #1867): every .sql export built from
// this page waits there for its download, and that space used to be
// invisible until someone ran du. Under the lane that starts the builds, so
// the disk a build takes is read where the build is asked for. null when
// this process cannot build .sql exports (no staging exists), when the
// storage signals were not read, and when nothing is staged: the lane's own
// lines already say a build is removed once downloaded, and the Snapshots
// first screen has a word budget this sentence would spend on nothing.
function stagedDownloadsNote(storage, cur, servers) {
  const stg = storage && storage.staging;
  if (!stg) return null;
  const hours = Math.round(stg.ttl_hours || 0);
  const builds = stg.builds || [];
  if (!builds.length) return null;
  const box = el("div", { class: "stg-staged" });
  const others = builds.filter((b) => b.server_id !== cur.id).length;
  box.append(el("p", { class: "form-hint", text:
    humanBytes(stg.bytes || 0) + " staged on this machine in " + builds.length + (builds.length === 1 ? " build" : " builds") +
    (others ? " (" + others + " for " + (others === 1 ? "another server" : "other servers") + ")" : "") +
    (builds.some((b) => !b.bytes_known) ? ", not counting builds whose size could not be measured" : "") +
    ". Each is removed once downloaded, or " + hours + " hours after it finished." }));
  const d = el("details", { class: "form-advanced" },
    el("summary", { class: "form-adv-summary", text: "Staged builds" }));
  for (const b of builds) {
    const name = b.server_name || ((servers || []).find((x) => x.id === b.server_id) || {}).name || b.server_id;
    let what;
    if (b.staging_error) what = "could not be removed or read: " + b.staging_error;
    else if (b.state === "running") what = "building" + (b.at ? " as of " + utcLabel(b.at) : "");
    else if (b.state === "failed") what = "failed build, being removed";
    else what = "ready" + (b.at ? " as of " + utcLabel(b.at) : "") + (b.expires_at ? ", removed at " + utcLabel(b.expires_at) + " if not downloaded" : "");
    kvRow(d, name, (b.bytes_known ? humanBytes(b.bytes || 0) : "size unknown") + ", " + what);
  }
  kvRow(d, "location", stg.dir || "");
  box.append(d);
  return box;
}

// telemetryCard shows the machine-wide usage-telemetry state and an opt-out
// toggle. Turning it off here stops THIS running daemon's beacons immediately
// (the daemon wired its live client) and persists the choice for every bintrail
// process on the machine.
// duckdbShape draws what the downloaded file will contain, instead of
// describing it (#1549 follow-up). The card used to carry three checkboxes and
// three multi-line caveats; the one decision a reader actually makes is whether
// to include the change log, and its whole cost is a shape: your tables are one
// file each, the change log is one file per archived hour and grows forever.
//
// Ticking the box LIGHTS THE STRIP. That is the cost statement: three tiles
// against a run of bars that is CLIPPED on purpose and fades off the edge,
// because the claim is that it grows without end -- a row stopping flush at a
// right edge draws a closed set, and the reader counts it. Built with el() and not svgEl,
// which only DOMParses static icon constants (see the note at the top of this
// file).
//
// The picture can lie in a way prose cannot, so it is pinned: the strip is
// rendered only for the parameter the card can actually send, and the guard in
// assets_duckdbcard_test.go fails if the two come apart.
function duckdbShape() {
  const tiles = (n, cls, rowCls) => {
    const row = el("div", { class: "dk-row" + (rowCls ? " " + rowCls : "") });
    for (let i = 0; i < n; i++) row.append(el("span", { class: cls }));
    return row;
  };
  const state = el("div", { class: "dk-part" },
    tiles(3, "dk-tile"),
    el("div", { class: "dk-cap", text: "your tables" }));
  const events = el("div", { class: "dk-part dk-off" },
    tiles(60, "dk-bar", "dk-strip"),
    el("div", { class: "dk-cap", text: "every change, hour by hour" }));
  return { el: el("div", { class: "dk-shape" }, state, events), events };
}

// duckdbViewNames pulls the view names out of a generated views.sql.
//
// Read off the file the user just downloaded, rather than asked for separately.
// The names are settled by the generator (a case collision renames one table, a
// schema DuckDB keeps for itself drops one), so a second source for them is a
// second opinion that can drift; these ARE the names in the file, or there is no
// file. It also costs nothing: the bytes are already here, where asking the
// server would have re-listed storage to answer.
//
// The generator writes every one as CREATE OR REPLACE VIEW "<schema>"."<table>"
// (or "events"), each part quoted with any quote inside it doubled. The list
// shows each the way a person types it: demo.prices, and demo."order.items" for
// a part DuckDB cannot read bare (duckdbTypedIdent). assets_duckdbcard_test.go
// pins this against views.Generate and DefinedViews, so neither the statement
// shape nor the typing rule can quietly stop matching.
function duckdbViewNames(sql) {
  const out = [];
  const re = /^CREATE OR REPLACE VIEW ((?:"(?:[^"]|"")*"\.)*"(?:[^"]|"")*") AS$/gm;
  let m;
  while ((m = re.exec(sql)) !== null) {
    const parts = [];
    const part = /"((?:[^"]|"")*)"/g;
    let p;
    while ((p = part.exec(m[1])) !== null) parts.push(duckdbTypedIdent(p[1].replace(/""/g, '"')));
    out.push(parts.join("."));
  }
  return out;
}

// DUCKDB_BARE_KEYWORDS: the SQL keywords DuckDB cannot read unquoted as a name.
// A copy of views.BareKeywords, pinned to it by assets_duckdbcard_test.go.
const DUCKDB_BARE_KEYWORDS = new Set([
  "all", "analyse", "analyze", "and", "anti", "any", "array", "as", "asc",
  "asof", "asymmetric", "at", "authorization", "binary", "both", "by", "case",
  "cast", "check", "collate", "collation", "column", "concurrently",
  "constraint", "create", "cross", "default", "deferrable", "desc", "describe",
  "distinct", "do", "else", "end", "except", "false", "fetch", "for",
  "foreign", "freeze", "from", "full", "glob", "group", "having", "ilike",
  "in", "initially", "inner", "intersect", "into", "is", "isnull", "join",
  "lambda", "lateral", "leading", "left", "like", "limit", "natural", "not",
  "notnull", "null", "offset", "on", "only", "or", "order", "outer",
  "overlaps", "pivot", "pivot_longer", "pivot_wider", "placing", "positional",
  "primary", "qualify", "references", "returning", "right", "select", "semi",
  "show", "similar", "some", "summarize", "symmetric", "table", "tablesample",
  "then", "to", "trailing", "true", "union", "unique", "unpack", "unpivot",
  "using", "variadic", "verbose", "when", "where", "window", "with"
]);

// duckdbTypedIdent: one name part the way a person types it, bare when DuckDB
// reads it bare and quoted otherwise. The same rule as the generator's
// typedIdent.
function duckdbTypedIdent(s) {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(s) && !DUCKDB_BARE_KEYWORDS.has(s.toLowerCase())) return s;
  return '"' + s.replace(/"/g, '""') + '"';
}

// duckdbCommandLine renders the one command the reader has to reproduce, with a
// Copy button.
//
// Real text, not a drawing. A picture of a terminal was built here first and
// thrown away: at card width it wrapped under the command and read as a loading
// skeleton, and unlike claudeAskMock it depicted nothing the server sends, so
// there was nothing to pin it against. The command itself is the part that has
// to be exact, and it is the part a drawing cannot carry.
function duckdbCommandLine(file, db) {
  const cmd = "duckdb -init " + file + " " + (db || "lake.db");
  return el("div", { class: "dk-run" },
    el("code", { class: "dk-cmd", text: cmd }),
    el("button", { class: "btn btn-sm dk-copy", type: "button", text: "Copy", onclick: () => copyText(cmd, "command") }));
}

// duckdbNameList renders what the reader can now query, by name.
//
// After the download, not before: on first visit the names are noise, and the
// card has a text budget it keeps by drawing rather than explaining. At this
// moment they are the one thing the reader needs and cannot guess, since the
// only documented way to recover them is SHOW TABLES, the query that took 63
// seconds on the surface this card replaced.
function duckdbNameList(names) {
  const box = el("div", { class: "dk-names" });
  // The count only. The command box above already says how to load the file,
  // and naming a second way to do it (.read, for a session already open) beside
  // the first put two loading instructions on screen with nothing to tell them
  // apart.
  box.append(el("div", { class: "dk-names-head" },
    el("span", { class: "dk-names-count", text: names.length + " views" })));
  const list = el("div", { class: "dk-names-list" });
  for (const n of names) {
    const row = el("button", { class: "dk-name", type: "button", text: n, title: "Copy" });
    row.onclick = () => copyText(n, "view name");
    list.append(row);
  }
  box.append(list);
  return box;
}

// downloadViewsSQL fetches the generated DuckDB schema and saves it, returning
// the file name and its text. The one download path for the card and the
// take-away lane on Snapshots, so the name and the parameters cannot drift apart.
async function downloadViewsSQL(opts) {
  const q = [];
  if (opts.events) q.push("include_events=1");
  if (opts.portable) q.push("portable_baseline=1");
  // Its own name. The browser saves into one folder and overwrites a
  // repeated name without asking, so downloading both would leave the
  // reader with whichever came last and no way to tell which.
  const name = opts.portable ? "views-portable.sql" : DUCKDB_VIEWS_FILE;
  const sql = await apiText("/api/views.sql" + (q.length ? "?" + q.join("&") : ""));
  downloadBlob(name, sql, "text/plain");
  return { name: name, sql: sql };
}

// duckdbPanel offers the generated DuckDB schema for this server's Parquet.
//
// It is NOT the console SQL page, which #1549 removed: nothing here executes.
// The title is load-bearing beyond this panel and must not be renamed casually
// (it was "Query in DuckDB" until #1528). Mounted on Connect AI (#1573); it
// sat on Backups from #1581 to #1573, where the take-away lane still
// downloads the default file through downloadViewsSQL.
function duckdbPanel() {
  // A section, not a .cn-card, wherever it mounts. On Connect, sqlClientPanel
  // already drew this line for the identical case: "the three numbered cards
  // are the Connect AI how-to, and this is a different client" — and .cards
  // tints every child by position, so inside the grid the card took amber and
  // its tint ate the drawing (style.css records --surface-3 at 1.021 against
  // orange-tint, under the 1.02 identity floor, exactly the fill the tiles
  // and bars are drawn in). Beside the SQL client and Iceberg panels every
  // sibling is the same bare section, so the shape needs no translation. The
  // cn- class prefix fits the page again: style.css styles the derived -body
  // class, and the bare class is the query hook the e2e resolves.
  const card = el("section", { class: "ov-panel " + DUCKDB_CARD_CLASS, style: "margin-top:18px" });
  card.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Download a DuckDB schema" })));
  const body = el("div", { class: DUCKDB_CARD_CLASS + "-body" });
  card.append(body);
  const shape = duckdbShape();
  body.append(shape.el);

  // One checkbox. --pin-snapshot and --include-live are still route parameters
  // and CLI flags; they are not decisions to put in front of a first-time
  // reader. The live leg in particular can compete with capture on the source,
  // which is not something to offer two clicks from a page that explains
  // nothing.
  const events = el("input", { type: "checkbox", name: "include_events" });
  body.append(el("label", { class: "check" }, events,
    el("span", { text: "Include the change log" })));
  events.onchange = () => shape.events.classList.toggle("dk-off", !events.checked);
  events.onchange();

  // The second box, and only for a server whose backups are in two places
  // (#1551). With one location there is nothing to decide, and a box that
  // changes nothing to a file is worse than no box at all.
  //
  // Phrased as where the file will be OPENED, not as which storage it names:
  // the reader downloading through a browser knows which machine they are
  // taking it to, and does not necessarily know where their backups live.
  let portable = null;
  if (capsCache.views_portable_baseline) {
    portable = el("input", { type: "checkbox", name: "portable_baseline" });
    body.append(el("label", { class: "check" }, portable,
      el("span", { text: "Works on another machine" })));
  }

  body.append(cnFine("More about this file",
    el("p", { class: "form-hint", text:
      "Runs in your own DuckDB, not in DBTrail, and no credentials are in the file." }),
    el("p", { class: "form-hint", text:
      "The views follow your newest snapshot where the snapshot folder supports it. (CLI: bintrail views)" })));

  const btn = el("button", { class: "btn btn-sm", type: "button", text: "Download " + DUCKDB_VIEWS_FILE });
  btn.onclick = async () => {
    btn.disabled = true;
    try {
      const got = await downloadViewsSQL({ events: events.checked, portable: !!(portable && portable.checked) });
      const name = got.name, sql = got.sql;
      // The instruction used to be a toast, which is the wrong container for the
      // only handoff in this flow: it names a command for a session the reader
      // has not opened yet, and then disappears. This stays on the card.
      for (const sel of [".dk-names", ".dk-run"]) {
        const older = body.querySelector(sel);
        if (older) older.remove();
      }
      // Above the button, not appended after it. The button is the action and
      // stays at the foot of the card; a list that lands below it pushes the
      // action into the middle of its own result.
      const foot = body.querySelector(".stg-cardfoot");
      body.insertBefore(duckdbCommandLine(name), foot);
      body.insertBefore(duckdbNameList(duckdbViewNames(sql)), foot);
    } catch (err) {
      toastError("could not generate views: " + ((err && err.message) || err));
    } finally {
      btn.disabled = false;
    }
  };
  body.append(el("div", { class: "stg-cardfoot" }, btn));
  return card;
}

function telemetryCard(t) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Usage telemetry" }));
  if (!t || t.error) {
    card.append(el("p", { class: "form-hint", text: "Could not read telemetry state" + (t && t.error ? ": " + t.error : ".") }));
    return card;
  }
  if (!t.endpoint_set) {
    card.append(el("p", { class: "stg-hint", text: "This build sends no telemetry; no endpoint is compiled in." }));
    card.append(telemetrySampleSection(t));
    return card;
  }
  card.append(el("p", { class: "stg-hint", text: t.reporting
    ? "Sending metadata-only usage stats (command names, version, OS/arch, a bounded error class) to help prioritize the roadmap. Never your data, schemas, tables, DSNs, IPs, or any identifier."
    : "Not sending any usage telemetry." }));
  kvRow(card, "status", (t.reporting ? "On" : "Off") + (t.ci_detected ? " (suppressed: CI detected)" : ""));
  if (t.overridden) {
    const by = t.decided_by === "DO_NOT_TRACK" ? "the DO_NOT_TRACK environment variable"
      : t.decided_by === "BINTRAIL_TELEMETRY" ? "the BINTRAIL_TELEMETRY environment variable"
      : "the --telemetry flag";
    card.append(el("p", { class: "form-hint", text: "Set by " + by + " where DBTrail was started, which overrides this toggle. Change it there." }));
    card.append(telemetrySampleSection(t));
    return card;
  }
  card.append(telemetrySampleSection(t));
  card.append(el("div", { class: "stg-cardfoot" },
    el("button", { class: "btn btn-sm", type: "button",
      text: t.consent ? "Turn telemetry off" : "Turn telemetry on",
      onclick: () => setTelemetry(!t.consent) })));
  return card;
}

// telemetrySampleSection folds the exact JSON one event would carry (#1447).
// The text comes from the daemon verbatim (`sample_event`, rendered by the same
// function the CLI's `telemetry show` prints through), so the card never
// re-draws or re-orders it: it is a read-only <pre>, and JSON.stringify would
// be a second renderer that could drift from the CLI. Opening it sends nothing.
function telemetrySampleSection(t) {
  const d = el("details", { class: "form-advanced tel-sample" },
    el("summary", { class: "form-adv-summary", text: "Show a sample event" }));
  if (!t.sample_event) {
    d.append(el("p", { class: "form-hint", text:
      "DBTrail could not render the sample event. The command line prints the same event (CLI: bintrail telemetry show)." }));
    return d;
  }
  d.append(el("p", { class: "form-hint", text:
    "The exact event this machine would send. Opening this sends nothing. (CLI: bintrail telemetry show)" }));
  d.append(el("pre", { class: "stg-code tel-sample-pre", text: t.sample_event }));
  return d;
}

async function setTelemetry(enabled) {
  try {
    await api("/api/telemetry", { method: "POST", body: { enabled: enabled } });
  } catch (e) {
    toastError("Could not change telemetry: " + ((e && e.message) || e));
    return;
  }
  toast(enabled ? "Telemetry turned on." : "Telemetry turned off. Sending stops now.");
  renderStatus();
}

// baselineConfigHint: the boot (cli) entry is not editable from the UI — its
// baseline comes only from --baseline-dir/--baseline-s3 (or the BINTRAIL_
// CONSOLE_BASELINE_DIR/_S3 env; BASELINE_DIR in the compose stack) — so the
// "edit the server" instruction would point it at a dead end.
function baselineConfigHint(cur, serversErr) {
  // A failed server list is not an empty one. "Add a server first" told an
  // operator who has servers to create another, and hid the real cause.
  if (serversErr) return "The server list could not be loaded (" + serversErr + "), so this cannot be checked.";
  if (!cur) return "Add a server first (Manage servers).";
  if (cur.kind === "ephemeral") {
    return "Restart DBTrail with --baseline-dir or --baseline-s3 (compose: BASELINE_DIR in .env).";
  }
  return "Set a Local folder or an S3 location below, under Where and how often.";
}

function formatAge(hours) {
  if (hours == null) return "—";
  if (hours < 1) return Math.max(1, Math.round(hours * 60)) + " min";
  if (hours < 48) return Math.round(hours) + " h";
  return Math.round(hours / 24) + " day(s)";
}

function archivingPanel(servers, serversErr) {
  const panel = el("section", { class: "ov-panel" });
  panel.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "S3 archiving per source" }),
    el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Manage servers", onclick: openServersModal })));
  const list = el("div", { class: "stg-list" });
  const sources = servers.filter((s) => s.has_source);
  if (serversErr) {
    list.append(el("div", { class: "ev-empty", text: "Could not load servers: " + serversErr }));
  } else if (!sources.length) {
    list.append(el("div", { class: "ev-empty", text: "No monitored sources yet. Add one under Manage servers." }));
  } else {
    sources.forEach((s) => {
      const row = el("div", { class: "stg-row" });
      row.append(el("span", { class: "stg-name", text: s.name }));
      // A chip of the status family (#1950): off is a state, not a fault.
      row.append(s.archive_s3 ? el("span", { class: "stg-dest", text: s.archive_s3 }) : el("span", { class: "chip chip-off", text: "not archived" }));
      if (s.monitor_state) row.append(monitorChip(s));
      row.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Configure",
        onclick: () => { openServersModal(); editServer(s.id); } }));
      list.append(row);
    });
  }
  panel.append(list);
  return panel;
}

// reusedCopiedNote qualifies a "reused" count with the reuses that were
// written as full byte copies. The bare counter reads as a disk saving, and
// for a copied reuse that saving did not happen: the fold was skipped but
// every byte was written again (no hard link; the daemon log names the
// cause). Confirming a saving the daemon log denies is the bug this splits
// away (#1578).
function reusedCopiedNote(copied) {
  if (!copied) return "";
  return " (" + copied + " of them written in full, which saved no disk; DBTrail's log says why)";
}

// budgetRefusedTail (#1107): an update refused for too many changed rows
// starts again from the same backup over a LONGER window, so "the next run
// retries" would be false. Worded as a condition, not an order: it stays true
// after the operator takes that full backup, and on a console that cannot
// create one it does not point at a button that is not there.
// The subject is "update", not "automatic refresh": a scheduled update writes
// the same status slot.
function budgetRefusedTail() {
  return " Nothing was overwritten. Until a newer full read exists, every update from the recorded changes" +
    " starts from the same snapshot and is refused again" +
    (capsCache.baseline_trigger ? "." : "; creating snapshots from the web interface is turned off here.");
}

// scheduleSkipTail: a skipped slot retries at the next one, which falls back
// to a full backup on its own when it may. The one skip that repeats is a
// budget refusal whose full backup cannot start here: its reason already says
// why, so the tail does not repeat it.
function scheduleSkipTail(reason) {
  return touchedRowBudgetText(reason) && /a full (backup|read) cannot start here/.test(reason || "")
    ? " Until a newer full read exists, every scheduled update is refused the same way."
    : " It will try again at the next scheduled time.";
}

// touchedRowBudgetText: the schedule's skips keep only the error text, not
// the status flag, so the skip line matches the refusal's fixed opening (reconstruct.ErrTouchedRowBudget), pinned by a test that
// feeds this the Go error.
function touchedRowBudgetText(s) {
  return /too many changed rows to build this from the recorded changes/.test(s || "");
}

// restoreRefusedLine: a restore that published nothing. A budget refusal
// needs a closer moment, which is this card's own input.
function restoreRefusedLine(rst) {
  return "Last restore published nothing: " + backupFoldError(rst.last_error || "unknown error") +
    (rst.too_many_changes ? " Nothing was overwritten. Pick a moment closer to an existing snapshot." : " Nothing was overwritten.");
}

// REFUSED_TABLE_WORDS: what each verdict of a refused table is called here,
// and what fixes it. The verdicts are the ones `bintrail baseline refresh`
// prints. One this page does not know is shown as it was written.
const REFUSED_TABLE_WORDS = {
  "refused-ddl": ["schema changed", "Needs a new read of this table."],
  "refused-gap": ["changes missing", "Needs a full snapshot."],
  "refused": ["refused", ""],
};

// refusedTableRows (#1653): the tables that stopped an update, as words. run
// is a refresh status, a schedule's last run or its last fallback; tail says
// what happened next. Publishing is all or nothing, so each table here
// stopped the copy of every table, and the first line says so. null when the
// run carries no list: a run recorded before the list existed has only its
// count, and the caller keeps saying that count.
function refusedTableRows(run, tail) {
  const list = run && Array.isArray(run.refused_tables) ? run.refused_tables : [];
  if (!list.length || run.ok === true || run.state === "succeeded" || run.state === "running") return null;
  const clip = (v, n) => {
    const s = String(v == null ? "" : v).replace(/\s+/g, " ").trim();
    // By character, not by code unit: a cut must not split one in two.
    const chars = Array.from(s);
    return chars.length > n ? chars.slice(0, n - 3).join("") + "..." : s;
  };
  // The engine writes what happened, then a remedy in command line words.
  // The row has its own fix, so the reason stops where that remedy starts.
  const fact = (s) => s.split(/\s\u2014\s|;\s*pass --/)[0].replace(/^(full-table )?reconstruct:\s*/, "");
  const left = Math.max(0, Math.floor(Number(run.refused_tables_omitted)) || 0);
  const n = Math.max(list.length + left, Math.floor(Number(run.refused)) || 0);
  const all = Math.floor(Number(run.tables)) || 0;
  const rows = list.map((row) => {
    const t = row && typeof row === "object" ? row : {};
    const verdict = clip(t.verdict, 40);
    const words = Object.prototype.hasOwnProperty.call(REFUSED_TABLE_WORDS, verdict) ? REFUSED_TABLE_WORDS[verdict] : null;
    const reason = clip(t.reason, 400);
    return {
      name: clip(t.name, 130) || "(no name)",
      what: words ? words[0] : (verdict || "refused"),
      fix: words ? words[1] : "",
      // A row with no fix of its own keeps the whole reason: the engine's
      // remedy is then the only one on the page.
      reason: !reason ? "" : words && words[1] ? clip(backupFoldError(fact(reason)), 200) : clip(backupFoldError(reason), 300),
    };
  });
  return {
    head: (n === all ? (n === 1 ? "The only table" : "All " + n + " tables")
      : (n === 1 ? "1 table" : n + " tables")) +
      " stopped the update" + (all > n ? " of all " + all : "") + ". " + (tail || "Nothing was published."),
    rows: rows,
    more: n > rows.length ? (n - rows.length) + " more not listed." : "",
  };
}

// leftOutWords (#2006): the tables a full read left out of its snapshot
// because their real name could not be read back from the dump or cannot be
// stored, as words. Everything else was copied. A session whose data profile
// hides table names gets the count only (left_out_tables_omitted). null when
// none was left out.
function leftOutWords(run) {
  const list = run && Array.isArray(run.left_out_tables) ? run.left_out_tables : [];
  const more = Math.max(0, Math.floor(Number(run && run.left_out_tables_omitted)) || 0);
  const n = list.length + more;
  if (!n) return null;
  const clip = (v, k) => {
    const c = Array.from(String(v == null ? "" : v).replace(/\s+/g, " ").trim());
    return c.length > k ? c.slice(0, k - 3).join("") + "..." : c.join("");
  };
  return {
    head: (n === 1 ? "1 table is" : n + " tables are") + " not in this snapshot. DBTrail could not read back or store " +
      (n === 1 ? "its real name, so it was" : "their real names, so they were") +
      " left out; every other table was copied. Changing the name on the database fixes it at the next full read.",
    rows: list.map((t) => ({ name: clip(t && t.name, 130) || "(no name)", reason: clip(backupFoldError(String((t && t.reason) || "")), 300) })),
    more: list.length && more ? more + " more not listed." : "",
  };
}

// leftOutTablesBlock draws leftOutWords, every value as text.
function leftOutTablesBlock(run) {
  const d = leftOutWords(run);
  if (!d) return null;
  const list = el("ul", { class: "refused-list" });
  for (const r of d.rows) {
    list.append(el("li", null, el("code", { text: r.name }), el("span", { class: "refused-what", text: "left out" }),
      r.reason ? el("span", { class: "refused-why", text: r.reason }) : null));
  }
  return el("div", { class: "refused-tables" },
    el("p", { class: "form-msg err", text: d.head }),
    d.rows.length ? list : null,
    d.more ? el("p", { class: "form-hint", text: d.more }) : null);
}

// refusedTablesBlock draws refusedTableRows: one row per table, its name,
// what happened and the fix, the reason under it. Every value is set as
// text, never parsed as markup: names and reasons come from a server.
function refusedTablesBlock(run, tail) {
  const d = refusedTableRows(run, tail);
  if (!d) return null;
  const list = el("ul", { class: "refused-list" });
  for (const r of d.rows) {
    list.append(el("li", null,
      el("code", { text: r.name }),
      el("span", { class: "refused-what", text: r.what }),
      r.fix ? el("span", { class: "refused-fix", text: r.fix }) : null,
      r.reason ? el("span", { class: "refused-why", text: r.reason }) : null));
  }
  return el("div", { class: "refused-tables" },
    el("p", { class: "form-msg err", text: d.head }),
    list,
    d.more ? el("p", { class: "form-hint", text: d.more }) : null);
}

// baselineRefreshNote renders the last automatic refresh for the selected
// server.
// newTablesNote (#1993): what a published update says about tables created
// on the database after the snapshot it started from. An update brings the
// tables it already has forward, so a new table is not in its snapshot until
// a full read takes it. What comes next is the daemon's recorded decision
// (new_tables_action), never a guess: a full snapshot is promised only when
// one was started. snap is the newest snapshot listed: a later one that holds
// a named table (a full read) takes it off the list; one that does not (a
// point-in-time restore, a full read that still missed it) leaves it on.
// Returns { warn, count, title, text, actions } or null. "Not checked" is its
// own sentence and never reads as "no new tables".
function newTablesNote(run, snap) {
  if (!run) return null;
  let names = Array.isArray(run.new_tables) ? run.new_tables.map((n) => String(n == null ? "" : n)).filter(Boolean) : [];
  let left = Math.max(0, Math.floor(Number(run.new_tables_omitted)) || 0);
  const about = utcLabel(run.new_tables_snapshot || run.snapshot_time || "");
  const newest = snap ? utcLabel(snap.time) : "";
  const later = / UTC$/.test(about) && / UTC$/.test(newest) && newest > about;
  if (later) {
    if (!Array.isArray(snap.tables)) return null;
    const have = new Set(snap.tables.map(String));
    const still = names.filter((n) => !have.has(n));
    // Every named table is in the newer snapshot: it read the database, and
    // so took the unnamed rest too.
    if (names.length && !still.length) return null;
    if (!names.length && left) return null;
    names = still;
  }
  const n = names.length + left;
  const READ = { label: "Read database now", run: "read", confirm: READ_DB_CONFIRM };
  if (n > 0) {
    const one = n === 1;
    const they = one ? "It" : "They";
    const join = one ? " joins" : " join";
    const list = names.length ? ": " + names.join(", ") + (left ? " and " + left + " more" : "") : "";
    const head = (one ? "1 table is" : n + " tables are") + " not in your copy yet";
    const made = (one ? "It was" : "They were") + " created on your database after the snapshot this update started from.";
    let next, actions;
    switch (later ? "" : String(run.new_tables_action || "")) {
      case "full_read":
        next = "A full snapshot was started to include " + (one ? "it." : "them.");
        actions = [{ label: "Wait for the full snapshot", primary: true, run: "dismiss" }];
        break;
      case "not_possible": {
        const why = String(run.new_tables_action_reason || "").replace(/\s+/g, " ").trim();
        // Held by the daily bound on full reads for new tables: nothing to
        // fix, the first update after that time starts one.
        if (/the next one is allowed after/.test(why)) {
          next = "A full snapshot is held back for now: " + why.replace(/[.\s]+$/, "") + ". The first update after that time starts one, so " +
            they.toLowerCase() + join + " the copy then. To include " + (one ? "it" : "them") + " sooner, read the database now.";
          actions = [Object.assign({ primary: true }, READ), { label: "Later", run: "dismiss" }];
          break;
        }
        next = "A full snapshot cannot start from here" + (why ? ": " + why.replace(/[.\s]+$/, "") : "") + ". Until that is fixed, " +
          they.toLowerCase() + " will not join the copy on " + (one ? "its" : "their") + " own. Fix it, or take a full snapshot with the bintrail command line.";
        actions = [{ label: "OK", primary: true, run: "dismiss" }];
        break;
      }
      case "gave_up":
        next = "Full snapshots were started to include " + (one ? "it" : "them") + " and none finished, so DBTrail stopped trying on its own. " +
          "Check why the last full snapshot failed on the Snapshots page, then take one.";
        actions = [Object.assign({ primary: true }, READ), { label: "Later", run: "dismiss" }];
        break;
      case "still_missing":
        next = "A full snapshot read your database after that and still did not include " + (one ? "it" : "them") +
          ", so another one would not either. Check that the server's schema list covers " + (one ? "its" : "their") +
          " schema and that the name is spelled the way the database lists it.";
        actions = [{ label: "OK", primary: true, run: "dismiss" }];
        break;
      case "no_schedule":
        next = "Automatic refreshes never read your database in full, so " + they.toLowerCase() + join + " the copy only when a full snapshot is taken.";
        actions = [Object.assign({ primary: true }, READ), { label: "Later", run: "dismiss" }];
        break;
      default:
        // An older record, a newer snapshot that still lacks them, or an
        // update whose snapshot did not reach its destination: no decision
        // to report, and no promise.
        next = they + join + " the copy when a full snapshot is taken.";
        actions = [Object.assign({ primary: true }, READ), { label: "Later", run: "dismiss" }];
    }
    return { warn: true, count: n, title: head, text: head + list + ". " + made + " " + next, actions: actions };
  }
  const why = later ? "" : String(run.new_tables_unchecked || "").replace(/\s+/g, " ").trim();
  if (why) {
    return { warn: false, count: 0, title: "", actions: [],
      text: "Could not check your database for tables created since the previous snapshot, so this snapshot may be missing some. Reason: " + why };
  }
  return null;
}

function newTablesBlock(run, snap) {
  const d = newTablesNote(run, snap);
  if (!d) return null;
  return el("p", { class: d.warn ? "form-msg err" : "form-hint", text: d.text });
}

function baselineRefreshNote(rf) {
  // finished_at/since are RFC3339 UTC on the wire; utcLabel renders them in
  // the console's labeled shape ("YYYY-MM-DD HH:MM:SS UTC", #1354).
  const when = utcLabel(rf.finished_at || rf.since || "");
  let text;
  switch (rf.state) {
    case "running":
      text = "Automatic refresh running" + (rf.since ? " since " + utcLabel(rf.since) : "") + "…";
      break;
    case "succeeded":
      // The two numbers PARTITION the run: rf.tables is every table, rf.carried
      // the subset that was reused, so "refreshed" has to be the difference.
      // Printing the total next to the subset read as "5 refreshed, 5 of them
      // reused", which asks the operator to subtract and contradicts the rule
      // the CLI summary follows: a reused table is not a refreshed one, and
      // which tables actually cost a rewrite is the number worth seeing.
      const reused = rf.carried || 0;
      const rewritten = Math.max(0, (rf.tables || 0) - reused);
      text = "Automatic refresh" + (when ? " at " + when : "") + ": " +
        rewritten + " table(s) refreshed" +
        (reused ? ", " + reused + " unchanged and reused" + reusedCopiedNote(rf.carried_copied || 0) : "") + ".";
      break;
    case "failed":
      // published means the fold finished and marked the snapshot, and only
      // sending it to the backup destination failed. Saying "published
      // nothing" there is false, and so is "the next run retries": the next
      // run folds a NEW snapshot rather than re-sending this one.
      text = rf.published
        ? "Automatic refresh" + (when ? " at " + when : "") +
          " wrote the snapshot on this machine but could not send it to the snapshot destination" +
          (rf.last_error ? ": " + backupFoldError(rf.last_error) : ".") +
          " The snapshot is on disk and can be restored from. The next run folds a new one."
        : "Automatic refresh published nothing" + (when ? " at " + when : "") +
          // With the list, the rows drawn under this line say which tables
          // and why (refusedTablesBlock). Without it, the count as before.
          (Array.isArray(rf.refused_tables) && rf.refused_tables.length ? "."
            : (rf.refused ? "; " + rf.refused + " table(s) refused" : "") +
              (rf.last_error ? ": " + backupFoldError(rf.last_error) : "")) +
          (rf.too_many_changes
            ? budgetRefusedTail()
            : " Nothing was overwritten; the next run retries.");
      break;
    default:
      return el("p", { class: "form-hint", text: "Automatic refresh is enabled; it has not run yet." });
  }
  // A failed refresh is a failure: red, like every other failed run on this
  // page. It used to share the grey of a successful one.
  return el("p", { class: rf.state === "failed" ? "form-msg err" : "form-hint", text: text });
}

// snapshotTablesUniform: the per-snapshot table count, when EVERY snapshot
// has the same one — else null. A value identical in all 22 rows was the
// list's widest column saying nothing (#1415); uniform, it is a fact about
// the collection and belongs in the context strip.
function snapshotTablesUniform(snaps, truncated) {
  // A TRUNCATED listing cannot decide either way: promoting the visible
  // rows' count to "N per snapshot" claims snapshots the API never showed,
  // and suppressing the per-row column does the same in the other
  // direction — under truncation the rows keep their column.
  if (truncated || !snaps || !snaps.length) return null;
  const n = (snaps[0].tables || []).length;
  return snaps.every((sn) => (sn.tables || []).length === n) ? n : null;
}

// opts.serversErr: when /api/servers failed, `servers` is empty for the WRONG
// reason. Without it this panel derives affirmative claims from a failure —
// the empty state advises adding a server that exists, and the `owner`
// fallback below attributes the snapshots to the daemon when the selected
// server may have its own baseline_s3.
// ── Keep it current with Iceberg (#1466) ─────────────────────────────────────
//
// The export turns these backups, plus every change recorded since, into
// Apache Iceberg tables. It is shown as a command rather than a button on
// purpose: it writes a new copy of the data, and it is deliberately kept out
// of the process that captures changes, so nothing here can start one. The
// panel is display only: the command is built from /api/servers and from the
// backup location Connect AI asks for (GET /api/baselines?location_only=1,
// #1573), which reads neither the storage nor the server's index. The
// password is elided the same way the SQL-client panel elides the console
// token.

// icebergExportCommand renders the command for the selected server, or null
// when this page cannot write a correct one: no server, no backup destination,
// or an index this console reaches over a unix socket. A command with a hole
// in it is worse than no panel.
function icebergExportCommand(cur, baselines) {
  const src = baselines && baselines.source;
  // The PORT is the socket test, not the host. A TCP connection always carries
  // one by the time the server describes it, so an empty port means the
  // console reaches its index over a path, which names only its own machine
  // and cannot go in this command. Defaulting to 3306 there printed
  // tcp(/var/run/mysqld/mysqld.sock:3306): it parses, then dies at dial.
  //
  // The views.sql download refuses a socket-reached index too, but it reads
  // the DSN itself, so the two tests are not the same test: this one sees a
  // decomposed address and infers the socket from the missing port. A socket
  // path that happens to end in ":<digits>" would slip past here and be
  // refused there. Not worth a DTO field: socket paths are not written that
  // way, and the failure is a command that does not connect.
  if (!cur || !src || !cur.host || !cur.port || !cur.dbname) return null;
  // The RESOLVED destination decides the flag, not the two fields on the
  // server: local wins over S3 when both are set, and a server that inherits
  // the daemon's destination carries neither of its own.
  const flag = baselines.kind === "s3" ? "--baseline-s3" : "--baseline-dir";
  // An IPv6 address arrives without its brackets (the server splits host from
  // port and keeps the host bare), and 2001:db8::1:3306 is a different
  // address, not the same one with a port.
  const host = cur.host.includes(":") ? "[" + cur.host + "]" : cur.host;
  const dsn = (cur.user || "user") + (cur.has_password ? ":***" : "") +
    "@tcp(" + host + ":" + cur.port + ")/" + cur.dbname;
  // Quoted like every other value here: a database or user name may legally
  // hold a $, a backtick or a quote, and this line is meant to be pasted into
  // a shell.
  return "bintrail export iceberg --index-dsn " + shellWord(dsn) + " " +
    flag + " " + shellWord(src) + " --warehouse /path/to/warehouse";
}

// icebergComposeNote says what the Docker route exports, which is not always
// what this panel prints.
//
// The panel is per selected server: the command above carries THAT server's
// index and THAT server's resolved backup destination. The compose one-shot
// carries neither. It defaults to the stack's own bundled index and
// /var/lib/bintrail/baselines, so for a server whose backups live in S3 the
// panel prints --baseline-s3 and the Docker line would export the local
// directory instead, successfully and with nothing to see. The service reads
// INDEX_DSN, BASELINE_DIR and BASELINE_S3, so pointing it is a matter of
// naming them.
//
// "ephemeral" is the entry seeded from the command line, which in the shipped
// stack IS the bundled index; every other entry comes from the registry.
function icebergComposeNote(cur) {
  const bundled = cur && cur.kind === "ephemeral";
  return "The Docker route below runs against this stack's own index and snapshots" +
    (bundled ? ". " : ", not the server picked in the left sidebar. ") +
    "To point it " + (bundled ? "somewhere else" : "at this server") +
    ", set INDEX_DSN and BASELINE_DIR or BASELINE_S3 in the stack's .env file.";
}

// The payoff, drawn as the names an analyst already knows rather than claimed
// in a sentence — and SPLIT, because "read them directly" is true of two of the
// five. docs/iceberg-export.md says it plainly: "DuckDB and Spark read such a
// directory directly. Trino, Athena and Snowflake read Iceberg through a
// catalog, so with them you register the table's current metadata file ...
// rather than pointing at the path."
//
// The old prose said all five read them directly and got away with it as a
// hedgeable sentence. Rendered as five chips under a heading that IS the claim,
// it stops being hedgeable. A guard ties both lists to that doc paragraph, so
// the drawing cannot drift away from what we tell people elsewhere.
const ICEBERG_ENGINES_DIRECT = ["DuckDB", "Spark"];
const ICEBERG_ENGINES_CATALOG = ["Trino", "Athena", "Snowflake"];

// ICEBERG_PLACEHOLDERS labels the two blanks in the command instead of
// describing them in a paragraph under it.
//
// Each key must appear VERBATIM in what icebergExportCommand builds, and a
// test asserts exactly that: a legend pointing at text that is not on the line
// above it is worse than no legend, because the reader hunts for it. This is
// the "a drawing can lie in a way prose cannot" discipline that claudeAskMock
// carries against the bundle manifest.
//
// Filtered by what the rendered command actually holds, because the password
// blank is absent for an index that needs none, and a legend for a blank that
// is not there describes somebody else's command.
const ICEBERG_PLACEHOLDERS = [
  ["***", "your index password"],
  ["/path/to/warehouse", "a folder you pick"],
];

// icebergFlow draws what the export makes and who reads it: three stages, left
// to right. It replaces the opening paragraph, which said the same thing in
// three sentences that a reader skimming a settings page does not read.
function icebergFlow() {
  const stage = (title, sub) => el("div", { class: "ice-stage" },
    el("div", { class: "ice-stage-t", text: title }),
    el("div", { class: "ice-stage-s", text: sub }));
  const arrow = () => el("div", { class: "ice-arrow", text: "\u2192" });
  const chips = (names) => el("div", { class: "ice-eng" },
    ...names.map((n) => el("span", { class: "ice-eng-n", text: n })));
  const engines = el("div", { class: "ice-stage" },
    el("div", { class: "ice-stage-t", text: "Read by" }),
    el("div", { class: "ice-eng-l", text: "straight off the folder" }),
    chips(ICEBERG_ENGINES_DIRECT),
    el("div", { class: "ice-eng-l", text: "through a catalog" }),
    chips(ICEBERG_ENGINES_CATALOG));
  return el("div", { class: "ice-flow" },
    // "newest snapshot", not "the whole snapshot": the old paragraph said WHICH
    // backup the first run consumes, and a reader looking at a list of them on
    // this very page cannot work that out from "snapshot". One word for one
    // object, too — the fold below and the rest of this page say "backup".
    stage("This server's history", "the newest snapshot, plus every change since"),
    arrow(),
    // The egress answer, kept in the open. It was a sentence under the command
    // ("Nothing is sent anywhere: the tables are written where you point it")
    // and drawing the panel dropped it. For an EXPORT feature it is the one
    // thing a cautious operator asks while standing here, so it belongs in the
    // picture rather than in the docs it also appears in.
    stage("Iceberg tables", "written where you point it, nowhere else"),
    arrow(),
    engines);
}

// icebergRuns draws the incremental behaviour as two bars, because the shape
// IS the message: the first run is the expensive one and every run after it is
// small. That is what makes "as fresh as you schedule it" believable, and it
// was the clause of the old paragraph most likely to be skipped.
//
// The two widths are SCHEMATIC. Nothing measures them and nothing should: the
// ratio between a first run and an incremental one is a property of the
// operator's data, not of this page, and a drawn ratio that looked derived
// would be the drawing lying in the way this panel's own comments warn about.
// They are painted in one neutral ink for the same reason — the widths carry
// the whole comparison, and style.css's brand rule is explicit that the warm
// palette never encodes data.
function icebergRuns() {
  const row = (label, barClass, note) => [
    el("span", { class: "ice-run-l", text: label }),
    el("span", { class: "ice-run-b" },
      el("span", { class: "ice-bar " + barClass }),
      el("span", { class: "ice-run-n", text: note })),
  ];
  return el("div", { class: "ice-runs" },
    ...row("First run", "ice-bar-full", "loads the newest snapshot"),
    ...row("Every run after", "ice-bar-delta", "adds only what changed"));
}

// icebergKeys labels the blanks in the command that was just printed.
function icebergKeys(cmd) {
  const keys = ICEBERG_PLACEHOLDERS.filter(([k]) => cmd.includes(k));
  if (!keys.length) return null;
  return el("div", { class: "ice-keys" },
    ...keys.map(([k, what]) => el("span", { class: "ice-key" },
      el("code", { class: "stg-code", text: k }),
      el("span", { text: what }))));
}

// icebergExportPanel: a drawing, the command, and one warning.
//
// It used to open with two paragraphs before the command and keep three after
// it, the last of them the compose note. Everything that was an EXPLANATION is
// now either drawn (what it
// makes, who reads it, what a run costs) or moved into "How to run it", where
// a reader who is actually about to run it will look. Only one paragraph stays
// in the open, and it is the only one that is a HAZARD rather than a
// description: the Docker route can export a different dataset than the one
// this panel just described, and succeed while doing it.
function icebergExportPanel(cur, baselines) {
  const cmd = icebergExportCommand(cur, baselines);
  if (!cmd) return null;
  // cn-sql for the look it shares with the SQL client panel; cn-ice so a
  // reader can tell the two apart (the Connect text budget leaves both out).
  const panel = el("section", { class: "ov-panel cn-sql cn-ice", style: "margin-top:18px" });
  panel.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Keep it current with Iceberg" })));
  const body = el("div", { class: "cn-sql-body" });
  panel.append(body);
  body.append(icebergFlow());
  body.append(icebergRuns());
  body.append(el("div", { class: "cn-urlrow" },
    el("code", { class: "stg-code cn-url", text: cmd }),
    el("button", { class: "btn btn-sm", type: "button", text: "Copy",
      onclick: () => copyText(cmd, "Export command") })));
  const keys = icebergKeys(cmd);
  if (keys) body.append(keys);
  // In the visible body, not inside the collapsed block: for a server that is
  // not the stack's own, the Docker route exports a DIFFERENT dataset and
  // succeeds while doing it, which is the one failure here nobody would see.
  body.append(el("p", { class: "cn-sql-row", text: icebergComposeNote(cur) }));
  body.append(cnFine("How to run it",
    // Moved down from the open body. Both are answers to questions a reader
    // has only once they are about to run the line, and one of them ("why is
    // this not a button") is a justification, which is the first thing to go
    // when the page is too dense to read.
    el("p", { class: "form-hint", text:
      "It is a command and not a button because the export writes a new copy of your data, and it is kept out " +
      "of the process that captures changes so a long export can never slow capture down." }),
    el("p", { class: "form-hint", text:
      "The line carries the index host, port, database and user, and nothing else about the connection. " +
      "If your index needs settings DBTrail was given, such as TLS or a timeout, add them yourself." }),
    // The address and the path are as THIS process sees them, and in the
    // bundled stack both are container-scoped (index-mysql:3306,
    // /var/lib/bintrail/baselines): pasted into a host shell they resolve to
    // nothing. Same hazard the generated views.sql warns about for a loopback
    // index host, and the compose profile is the answer for that operator.
    el("p", { class: "form-hint", text:
      "The address and folder above are the ones DBTrail uses. Run the command somewhere that can reach " +
      "both. If DBTrail runs in Docker, run it there too, with the command below." }),
    el("code", { class: "stg-code cn-snippet", text:
      "docker compose --profile iceberg-export run --rm iceberg-export" }),
    el("p", { class: "form-hint", text:
      "Run it where bintrail is installed and can reach this index and these snapshots. Run it again whenever " +
      "you want the tables brought forward; it picks up where it left off, and a run that dies partway leaves " +
      "the last good copy in place." }),
    el("p", { class: "form-hint", text:
      "Hourly from cron. The Docker line keeps the index password out of your crontab, since the container " +
      "reads it from the stack; use it when the stack holds the index and snapshots you want exported, per the " +
      "note above." }),
    el("code", { class: "stg-code cn-snippet", text:
      "17 * * * * cd /path/to/stack && docker compose --profile iceberg-export run --rm iceberg-export" }),
    el("p", { class: "form-hint", text:
      "Running bintrail yourself instead? Use the same schedule with the command above. Keep the password " +
      "out of the crontab line: put it in a file only you can read and have the job read it from there." }),
    el("p", { class: "form-hint", text:
      "Each table is written to <warehouse>/<schema>/<table>/. The export refuses to advance a table whose " +
      "columns changed, or whose history has a gap, and says which; to load one again from a fresh snapshot, " +
      "remove its folder." })));
  return panel;
}

// backupSourceList renders every location the listing read, which is more than
// one whenever a server has a local directory AND an S3 destination (#1542).
// The bucket is the bundle's per-table fallback, so it always held snapshots the
// page could resolve through Time-travel and never showed.
function backupSourceList(b) {
  const srcs = (b && b.sources) || [];
  if (srcs.length < 2) return el("code", { class: "stg-code", text: (b && b.source) || "" });
  return el("div", { class: "bk-srcs" }, ...srcs.map((s) => el("div", { class: "bk-src" },
    el("span", { class: "bk-src-k", text: backupKindWord(s.kind) }),
    el("code", { class: "stg-code", text: s.source }),
    s.error
      ? el("span", { class: "chip chip-mon", text: "unreadable" })
      : (s.skipped > 0
        ? el("span", { class: "chip chip-mon", text: "listed in part", title: s.skipped + " folder(s) under it could not be read" })
        : el("span", { class: "bk-src-n", text: s.count + " file(s)" })))));
}

function backupKindWord(kind) { return kind === "s3" ? "S3" : "disk"; }

// firstLine trims a backend error to its opening line. An S3 listing failure
// arrives as a DuckDB error carrying the whole generated SQL statement across
// several lines; pasted into the page it is a wall, and the wall is what a
// reader skips. The first line is the cause ("HTTP 404", "AccessDenied"); the
// server keeps the rest.
function firstLine(msg) {
  const s = String(msg || "").split("\n")[0].trim();
  return s.length > 180 ? s.slice(0, 177) + "\u2026" : s;
}

// backupWhereChip says which of the two locations a snapshot was found in.
// Rendered only when there IS more than one, since a chip repeating the single
// configured source on every row is noise, not information.
function backupWhereChip(b, sn) {
  const srcs = (b && b.sources) || [];
  if (srcs.length < 2) return null;
  const kinds = (sn && sn.kinds) || [];
  if (!kinds.length) return null;
  return el("span", { class: "bk-where", text: kinds.map(backupKindWord).join(" + ") });
}

// backupIncompleteNotice is a HAZARD, not a status line: the list under it is a
// subset, and the failure this endpoint had was a short list read as the whole
// set. Named per location, because "one of your sources failed" sends the
// operator to check both.
function backupIncompleteNotice(b) {
  if (!b || !b.incomplete) return null;
  // A location that answered in PART (#1601: a snapshot folder it could not
  // open, skipped and counted) is as much a hazard as one that failed: the
  // list is still a subset. Keying on error alone printed "0 of 1 locations
  // could not be read" over such a listing and named nothing.
  const bad = ((b.sources) || []).filter((s) => s.error || s.skipped > 0);
  const box = el("div", { class: "error-box" },
    el("div", { text: "Some snapshots are not listed: " + bad.length +
      " of " + ((b.sources) || []).length + " locations could not be read in full." }));
  bad.forEach((s) => box.append(el("div", { class: "bk-src" },
    el("span", { class: "bk-src-k", text: backupKindWord(s.kind) }),
    el("code", { class: "stg-code", text: s.source }),
    el("span", { class: "bk-src-n", text: s.error ? firstLine(s.error) : "listed in part: " + s.skipped + " folder(s) could not be read" }))));
  return box;
}

// BACKUPS_PAGE_SIZE caps how many backups a page shows.
//
// The list is a recency view, not an inventory: what an operator comes here for
// is the newest few and whether they are fresh. Twenty-five rows pushed the
// per-row Download and the restore controls below the fold, so the panel read
// as a wall with nothing to do in it.
const BACKUPS_PAGE_SIZE = 5;

// backupsPage survives a re-render on purpose. The panel repaints every ~10s
// while a backup run is in flight (watchBackupRuns), and page state held in the
// DOM would snap back to the first page under a reader who had paged away.
// Keyed by server so switching does not land you on page 4 of a list that has
// two.
let backupsPage = { server: null, index: 0 };

// backupsHead is the Snapshots page's element from its latest paint: the
// loading notice while it fetches, then its heading. A job that finishes, a
// schedule saved or a restore started repaints the page only while that
// element is still on screen. The question is asked of the page, not of the
// address: ten checks compared the address with "/baselines", which would
// all answer "no" in silence once the page moves to another address, and a
// constant holding the new one would answer "yes" for every section of a
// page that holds several. The loading notice counts, so a job that ends
// while the page is still fetching repaints it with the newer state.
let backupsHead = null;
// backupsPaintedFor is the server (and server generation) the page on screen
// was painted for, so a repaint can tell "the same page again" from "another
// server's page". Only the first tells it to keep what the reader is
// looking at.
let backupsPaintedFor = "";
// The machine's storage signals (GET /api/storage) and the registry entries
// read by the last Snapshots paint, for the two notes drawn deep inside the
// page (#1867): what signs S3 requests, under the S3 field, and what .sql
// builds wait on disk, under the .sql lane. null when the session may not
// read settings, and then neither note is drawn.
let snapStorage = null;
// The places of the selected server that hold snapshots of more than one
// installation (#1762), from the listing the page just read; the server's
// settings row says it.
let snapSharedWith = [];
let snapRegistry = [];
function backupsOnScreen() { return !!(backupsHead && backupsHead.isConnected); }

function backupsPageIndex(serverId, pages) {
  if (backupsPage.server !== serverId) backupsPage = { server: serverId, index: 0 };
  // Clamped on READ rather than on write: the list shrinks under you when
  // retention prunes, and a stored index past the end would render an empty
  // page with no way back.
  if (backupsPage.index > pages - 1) backupsPage.index = Math.max(0, pages - 1);
  return backupsPage.index;
}

// backupsPager draws the two controls and the position. Rendered only when
// there is more than one page: a pager under five rows is furniture.
// truncated: /api/baselines caps its listing (baselinesMaxSnapshots) and says
// so. The pager's one job is to tell a reader where they are, so it must not
// render "46-50 of 50" for a server with 200 backups -- that is the one line
// on the page asserting an inventory the API explicitly disclaimed.
// backupsPageSlice is the page's window into the list, pulled out of the
// panel so a test can EXECUTE it. The guard it replaces asserted the source
// text contained ".slice(start," and "const idx = start + i" -- both of which
// survive rewriting `start` to a constant 0, which leaves Older a dead
// control, the pager still reporting "6-10 of 25", and every page re-crowning
// its first row "Newest". Spelling is not behaviour.
function backupsPageSlice(snapshots, page) {
  const start = page * BACKUPS_PAGE_SIZE;
  return { start: start, rows: snapshots.slice(start, start + BACKUPS_PAGE_SIZE) };
}

function backupsPager(total, page, pages, onGo, truncated) {
  if (pages < 2) return null;
  const from = page * BACKUPS_PAGE_SIZE + 1;
  const to = Math.min(total, (page + 1) * BACKUPS_PAGE_SIZE);
  const of = truncated ? " of the newest " + total : " of " + total;
  const btn = (label, target, disabled) => {
    const el2 = el("button", { class: "btn btn-sm", type: "button", text: label });
    if (disabled) el2.disabled = true;
    else el2.onclick = () => onGo(target);
    return el2;
  };
  return el("div", { class: "bk-pager" },
    btn("Newer", page - 1, page === 0),
    el("span", { class: "bk-pager-n", text: from + "–" + to + of }),
    btn("Older", page + 1, page >= pages - 1));
}

// snapshotRetentionLines says what this machine does with older copies
// (#1681): how many it keeps, the last time it removed some, and in red when
// that record cannot be read or the last attempt failed. Every field is
// OMITTED by the server when it does not apply, so an absent field draws
// nothing, never "0" or "never". A cleanup that removed nothing is not
// recorded and not said. A failure's reason carries one cause per line
// (path and error each), so each line is drawn on its own; a path or an
// error text can contain "; ", which is why the split is on newlines only.
function snapshotRetentionLines(b) {
  const out = [];
  const keep = b.local_retention && b.local_retention.keep_newest;
  const facts = [];
  if (keep > 0) {
    facts.push(keep === 1 ? "Keeps only the newest snapshot on this machine." : "Keeps the newest " + keep + " snapshots on this machine.");
  }
  const lp = b.last_prune;
  if (lp && lp.removed > 0 && lp.at) {
    facts.push("Removed " + lp.removed + " older " + (lp.removed === 1 ? "copy" : "copies") + " on " + utcLabel(lp.at) + ".");
  }
  if (facts.length) out.push(el("p", { class: "form-hint bk-retention", text: facts.join(" ") }));
  if (b.last_prune_error) {
    out.push(el("p", { class: "form-msg err bk-retention-err", text:
      "The record of older copies removed here could not be read: " + firstLine(b.last_prune_error) }));
  }
  const fail = b.last_prune_failure;
  if (fail) {
    const causes = String(fail.reason || "").split("\n").map((c) => c.trim()).filter(Boolean);
    const box = el("div", { class: "form-msg err bk-retention-err" },
      el("div", { text: "Removing older copies failed" + (fail.at ? " on " + utcLabel(fail.at) : "") + (causes.length ? ":" : ", with no reason recorded.") }));
    causes.forEach((c) => box.append(el("div", { class: "bk-retention-cause", text: c })));
    out.push(box);
  }
  return out;
}

function baselinesPanel(b, servers, opts) {
  // Full-width (#1415): this list is the page. The Create-baseline action
  // moved to the context strip — at page level it is a page action; inside
  // this header it read as scoped to the list.
  const panel = el("section", { class: "ov-panel" });
  const cur = (servers || []).find((s) => s.id === (currentServer || defaultServerId));
  let owner = cur ? serverLabel(cur) : "";
  if (!owner && b && !b.error && b.configured && !(opts && opts.serversErr)) {
    owner = "command-line server (--baseline-dir / --baseline-s3)";
  }
  const head = el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Snapshots" + (owner ? " · " + owner : "") }),
    tzChip());
  panel.append(head);
  // The daemon's periodic refresh (#1171). Shown next to the list because the
  // question it answers — "is this list going to keep moving on its own?" — is
  // only meaningful here. A failed refresh is reported plainly: it usually means
  // the fold refused (a capture gap, a schema change), which is the fail-closed
  // contract working, not a broken daemon.
  //
  // Gated on the PAYLOAD, never on capsCache.baseline_trigger: the refresh and
  // the mydumper dump are independently opt-in, so a refresh-only daemon reports
  // here with baseline_trigger false, and a capability gate would render nothing.
  if (b && !b.error && b.refresh) {
    panel.append(baselineRefreshNote(b.refresh), refusedTablesBlock(b.refresh) || "");
    // The list the last published update left (#1993), checked against the
    // newest snapshot's own tables.
    panel.append(newTablesBlock(b.refresh, (b.snapshots || [])[0]) || "");
  }
  if (b && !b.error) snapshotRetentionLines(b).forEach((line) => panel.append(line));
  const list = el("div", { class: "stg-list" });
  if (!b || b.error) {
    // A missing index database is the pre-capture state, and renderError
    // draws it as what to do and where; any other failure stays a failure.
    if (b && b.error && indexMissingFrom(b.error)) renderError(list, b.error);
    else list.append(el("div", { class: "ev-empty", text: "Could not list snapshots: " + ((b && b.error) || "unavailable") }));
  } else if (!b.configured) {
    list.append(el("div", { class: "stg-empty" },
      el("p", { class: "stg-empty-lead", text: "No snapshots configured." }),
      el("p", { class: "stg-empty-sub", text: "A snapshot is a full copy of your tables. With one, Time-travel can show complete rows, not just the ones that changed lately." }),
      ...(sessionMayConfigureServer() ? [el("p", { class: "stg-empty-sub", text: "1. Create snapshots:" }),
      el("code", { class: "stg-code", text: "docker compose --profile baseline run --rm baseline" }),
      el("p", { class: "stg-empty-sub", text: "2. " + baselineConfigHint(cur, opts && opts.serversErr) })] : [])));
  } else if (!(b.snapshots || []).length) {
    // A source that failed reaches HERE too, and "no snapshots found" would be a
    // flat lie about a bucket nobody could read.
    const emptyWarn = backupIncompleteNotice(b);
    if (emptyWarn) list.append(emptyWarn);
    list.append(el("div", { class: "stg-empty" },
      el("p", { class: "stg-empty-lead", text: "Source configured, no snapshots found." }),
      backupSourceList(b),
      el("p", { class: "stg-empty-sub", text: "Run bintrail dump and bintrail baseline to create your first snapshot. The path must point at the folder that contains the snapshots, not a specific file (<timestamp>/<schema>/<table>.parquet)." })));
  } else {
    // Panel headline: the newest-per-table rollup. Older snapshots being past
    // coverage is routine (superseded) — only the headline and the newest
    // row's verdict are actionable, so only those get a chip.
    const incomplete = backupIncompleteNotice(b);
    if (incomplete) list.append(incomplete);
    if (b.staleness && b.staleness !== "ok") {
      list.append(el("div", { class: "vfy-summary" },
        // broken is the worst state on this page (a full-table restore will
        // not work), so it is red; aging and unknown stay the warning colour.
        el("span", { class: b.staleness === "broken" ? "chip chip-fail" : "chip chip-mon", text: b.staleness === "broken"
          ? "⚠ SNAPSHOT STALE: full-table restore broken; take a fresh snapshot"
          : "SNAPSHOT " + b.staleness.toUpperCase() })));
    }
    // Row hierarchy (#1415): the newest snapshot is what Time-travel and a
    // restore actually use — it gets the treatment; the rest are history and
    // read denser. Relative age sits NEXT TO the absolute time (two facts
    // about the same instant, formerly ~650px apart), and the table count
    // appears per-row only when it VARIES — a value identical in every row
    // is a fact about the collection and lives in the context strip.
    const uniformTables = snapshotTablesUniform(b.snapshots, b.truncated);
    // The count in force (Settings, "Keep by count"): every local copy past
    // it goes at the next hourly cleanup, and the row says so instead of
    // sitting in the list as if it were staying. Copies only in S3 are not
    // touched by the cleanup, so they are not marked.
    const keepInForce = (b.local_retention && b.local_retention.keep_newest) || 0;
    let localSeen = 0;
    const goingIdx = new Set();
    if (keepInForce > 0) {
      b.snapshots.forEach((sn, i) => {
        if (sn.kinds && sn.kinds.length && !sn.kinds.includes("dir")) return;
        localSeen++;
        if (localSeen > keepInForce) goingIdx.add(i);
      });
    }
    const total = b.snapshots.length;
    const pages = Math.max(1, Math.ceil(total / BACKUPS_PAGE_SIZE));
    const page = backupsPageIndex(currentServer || defaultServerId, pages);
    const pageWindow = backupsPageSlice(b.snapshots, page);
    // idx is the index in the WHOLE list, not in the page: it decides the
    // "Newest" treatment, and paging must not promote the first row of page two.
    pageWindow.rows.forEach((sn, i) => {
      const idx = pageWindow.start + i;
      const row = el("div", { class: "stg-row" + (idx === 0 ? " stg-row-latest" : "") });
      if (idx === 0) row.append(el("span", { class: "chip chip-newest", text: "Newest" }));
      const when = tsSpan("stg-name mono", sn.time);
      // The binlog coordinates are for whoever debugs a copy, not for the
      // list: they ride as the tooltip of the time.
      if (sn.binlog_file) when.title = "binlog " + sn.binlog_file + ":" + sn.binlog_pos;
      row.append(when);
      row.append(el("span", { class: "stg-rel", text: formatAge(sn.age_hours) + " ago" }));
      if (goingIdx.has(idx)) {
        row.classList.add("stg-row-going");
        row.append(el("span", { class: "chip chip-unknown", text: "goes at the next cleanup", title: "Past the newest " + keepInForce + " kept (Settings, Keep by count). Removed from this machine at the next hourly cleanup, never a table's only copy." }));
      }
      // With skipped views the row says both counts (#1879), whether or not
      // the table count varies: the views are what the row is there to show.
      const skippedViews = snapshotViewsText(sn);
      if (skippedViews || uniformTables === null) row.append(el("span", { class: "stg-dest", text: skippedViews || (sn.tables || []).length + " table(s)" }));
      const lockPill = snapshotLockPill(sn.lock);
      if (lockPill) row.append(lockPill);
      if (idx === 0 && sn.staleness && sn.staleness !== "ok") {
        row.append(el("span", { class: "chip chip-mon", text:
          sn.staleness === "broken" ? "⚠ STALE: restore broken" : sn.staleness.toUpperCase() }));
      }
      const where = backupWhereChip(b, sn);
      if (where) row.append(where);
      // Click to expand: tables, sizes and how long the backup took. Loaded
      // once per row, on first open.
      const detail = el("div", { class: "bk-detail" });
      detail.hidden = true;
      // A chevron, because the only thing that said "this opens" was a hover
      // background and a pointer cursor. The per-row Download lives inside
      // this fold, so an invisible affordance hid the whole feature.
      row.append(el("span", { class: "bk-chev", text: "›" }));
      row.classList.add("bk-expandable");
      row.setAttribute("role", "button");
      row.setAttribute("aria-expanded", "false");
      row.onclick = () => {
        detail.hidden = !detail.hidden;
        row.setAttribute("aria-expanded", detail.hidden ? "false" : "true");
        if (!detail.hidden && !detail.dataset.loaded) {
          detail.dataset.loaded = "1";
          loadBackupDetail(sn.time, detail);
        }
      };
      list.append(row, detail);
    });
    const pager = backupsPager(total, page, pages, (target) => {
      backupsPage = { server: currentServer || defaultServerId, index: target };
      // renderSnapshots(), not renderRoute(): the route path bumps viewGen,
      // which kills the job watchers this page has running. This is the same
      // repaint watchBackupRuns uses, and it keeps the reader where they are.
      renderSnapshots();
    }, !!b.truncated);
    if (pager) list.append(pager);
    // Only when there is no pager to carry it: with one, "of the newest 50"
    // already says it, and this line sitting under page 1 read as "backups 6
    // and up are not shown" when they are on page 2.
    if (b.truncated && !pager) list.append(el("div", { class: "ev-empty", text: "…older snapshots not shown." }));
  }
  panel.append(list);
  return panel;
}

// createBaseline triggers an in-process baseline (dump→convert→upload) on the
// daemon for the selected server, then polls until it finishes and refreshes the
// Storage view so the new snapshot appears. The button is disabled while in flight.
// READ_DB_CONFIRM is asked before every "Read database now": the read is the
// one action on these pages that reaches production. "may": writes wait
// while a locked read starts (longer behind a long-running query, docs
// #1987), but not on a PostgreSQL source or a read that takes no lock.
const READ_DB_CONFIRM = "Read every table from your database now?\n\nBest at a quiet time: writes may wait while it starts, longer if a long query is running.";

// newestCopyLine is the line beside Read database now (2026-10-01): what the
// newest copy says about being point-in-time, and the way out. It reads the
// NEWEST copy only, because older copies keep their mark until retention
// removes them, so a line about all of them would never change after a read.
// "" when there is nothing to say: no copy, a point-in-time newest copy with
// no marked older one, or a copy this list did not check (S3). The last one is
// silent on purpose: on a server that keeps its copies in S3 it would be a
// line that never goes away, and the row's own "not checked" chip says it.
function newestCopyLine(snaps, canRead, mayRead) {
  const newest = (snaps || [])[0];
  if (!newest) return "";
  const key = snapshotLockKey(newest.lock);
  if (key === "") {
    const marked = snaps.slice(1).some((s) => { const k = snapshotLockKey(s.lock); return k === "torn" || k === "unknown"; });
    return marked ? "Newest copy: point-in-time. Older copies keep their mark." : "";
  }
  if (key === "torn") return "Newest copy: different points-in-time.";
  if (key !== "unknown") return "";
  const head = "Newest copy: point-in-time unknown.";
  if (canRead) return head + " Read database now records it.";
  return mayRead ? head : head + " Ask an admin to read the database to record it.";
}

// SNAPSHOT_FAILED_HEAD is the first line of every failed full read the page
// explains (#1986): true whatever went wrong, because a MySQL or MariaDB
// snapshot only reads.
const SNAPSHOT_FAILED_HEAD = "Snapshot did not finish. Your database was not changed.";

// snapshotFailureHead is that line for one failure. A Postgres snapshot
// creates a replication slot on the source that a failure can leave behind,
// so it does not say the database was not changed. name, when given, says
// which server: two failures at once must not read the same.
function snapshotFailureHead(failure, name) {
  const pg = !!(failure && failure.postgres);
  const head = name ? "Snapshot of " + name + " did not finish." : "Snapshot did not finish.";
  return pg ? head : head + " Your database was not changed.";
}

// snapshotFailureBody is what follows the headline: the one fix the daemon
// could name for certain (failure.kind, never read from the error text), or,
// with none, the error's first line (failure.summary, without mydumper's
// output) as the hint; then the full error in a closed "Technical details"
// fold. where says how the next try is named: "now" beside the Read
// database now button, "overview" away from it, "scheduled" when the next
// try is the schedule's. note is a line that stays visible (a low disk, a
// location that could not be checked). No failure at all (an older daemon,
// an old run record) gets the fold alone.
function snapshotFailureBody(failure, raw, where, note) {
  const f = failure || {};
  const box = el("div", { class: "snap-fail" });
  const retry = where === "scheduled" ? "the next scheduled snapshot will use it"
    : "press Read database now again" + (where === "overview" ? " on the Snapshots page" : "");
  if (f.kind === "missing_permission" && f.grant) {
    box.append(el("p", { class: "snap-fail-fix", text: "The database user needs " + (f.privileges === 1 ? "one more permission" : "more permissions") +
      ". Run this on your database, then " + retry + ":" }));
    const pre = el("pre", { class: "form-code snap-fail-grant", text: f.grant });
    box.append(pre, el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(f.grant, "SQL") }));
    // The daemon gives no statement for this on a host named as Amazon RDS
    // or Aurora, but one reached by an address or another name looks
    // self-hosted to it, and RDS refuses this permission to every user. The
    // mode that needs it is only ever tried last because an operator chose
    // it (automatic retries another), and the page has no control for that
    // choice, so the card points at where the details name it.
    if (/\bBACKUP_ADMIN\b/.test(f.grant)) {
      box.append(el("p", { class: "snap-fail-note", text: "On Amazon RDS or Aurora this permission cannot be granted." +
        (f.mode_chosen ? " DBTrail was started with a setting that forces this way of reading; the technical details below say where it is." : "") }));
    }
  } else if (f.kind === "mydumper_too_old" && f.min_version) {
    box.append(el("p", { class: "snap-fail-fix", text: "Install mydumper " + f.min_version + " or newer where DBTrail runs, then " + retry + "." +
      (f.mode_chosen ? " The technical details below show another way." : "") }));
  } else {
    if (f.summary) box.append(el("p", { class: "snap-fail-why", text: f.summary }));
    if (where === "scheduled") box.append(el("p", { class: "snap-fail-fix", text: "The next scheduled snapshot tries again." }));
  }
  if (note) box.append(el("p", { class: "snap-fail-note", text: note }));
  if (raw) {
    box.append(el("details", { class: "snap-fail-details" }, el("summary", { text: "Technical details" }),
      el("div", { class: "snap-fail-raw", text: raw })));
  }
  return box;
}

// snapshotFailureCard is the headline and the body in one block, for the
// places that are not a toast (the toast's own line is the headline).
function snapshotFailureCard(failure, raw, where, note, name) {
  const box = snapshotFailureBody(failure, raw, where, note);
  box.prepend(el("p", { class: "snap-fail-head", text: snapshotFailureHead(failure, name) }));
  return box;
}

async function createBaseline(id, btn, onStarted) {
  const label = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "Creating…"; }
  const restore = () => { if (btn) { btn.disabled = false; btn.textContent = label; } };
  try {
    await api("/api/servers/" + encodeURIComponent(id) + "/baseline", { method: "POST", body: {} });
  } catch (err) {
    toastError("Snapshot failed: " + ((err && err.message) || err));
    restore();
    return;
  }
  toast("Snapshot started: copying your data and uploading it…");
  if (onStarted) onStarted();
  if (backupsOnScreen()) renderSnapshots();
  let done = await pollBaseline(id, false);
  if (done && done.state === "succeeded" && done.uploading) {
    // Published locally; the copy to the destination is still running and
    // no longer holds the schedule (#1725). Say so, and wait for it.
    toast("Snapshot saved locally: " + (done.tables || 0) + " table(s). Still copying it to the snapshot destination…");
    if (backupsOnScreen()) renderSnapshots();
    done = await pollBaseline(id, true);
  }
  restore();
  // A low disk (#1938) stays on screen: the read ran, and the next one may not.
  const lowDisk = done && done.disk_check === "low" && done.disk_note ? done.disk_note : "";
  const unchecked = done && done.disk_check === "unchecked" && done.disk_note ? ". " + done.disk_note : "";
  // A read that reached the source without encryption says so (#1996).
  const cleartext = done && done.transport_note ? " " + done.transport_note : "";
  if (done && done.state === "succeeded" && !done.uploading) {
    toast("Snapshot complete: " + (done.tables || 0) + " table(s)" +
      (done.uploaded ? ", " + done.uploaded + " file(s) uploaded" : "") +
      (done.swept ? ", " + done.swept + " earlier snapshot(s) sent too" : "") + unchecked + cleartext);
    if (lowDisk) toastError(lowDisk);
    const left = leftOutWords(done);
    if (left) toastError(left.head + (left.rows.length ? " " + left.rows.map((r) => r.name + ": " + r.reason).join(" ") : ""));
  } else if (done && done.uploading) {
    // The poll's cap hit mid-copy: say what is true, not "complete".
    toast("Snapshot saved on this machine. The copy to the snapshot destination is still running; the Snapshots page shows when it finishes.");
    if (lowDisk) toastError(lowDisk);
  } else if (done && !done.published) {
    // The failure toast never fades, so the whole card fits in it (#1986):
    // the headline is its line, the fix and the folded error sit under it.
    toastError(snapshotFailureHead(done.failure, done.failure && done.failure.server), snapshotFailureBody(done.failure, done.last_error || "unknown error", "now", lowDisk));
  } else if (done) {
    // Published, then the copy to the destination failed: a snapshot
    // exists, so "did not finish" would be false.
    const why = done.last_error || "unknown error";
    toastError("Snapshot failed: " + why + (lowDisk ? (/[.!?]$/.test(why) ? " " : ". ") + lowDisk : ""));
  } else {
    toast("The snapshot is still running. Check back shortly.");
  }
  // Only the Snapshots page needs the refresh: the button lives in
  // the hero (#1415 moved it out of baselinesPanel; round 3 made it the hero), and both the
  // strip and the snapshot list render only on this page.
  if (backupsOnScreen()) renderSnapshots();
}

// pollBaseline polls the per-server baseline status until it leaves "running"
// (or a ~20-minute cap). With throughUpload, a status published with the
// upload still running (#1725) is waited out too, so the caller can say
// "saved locally" the moment it is true and "complete" only once it is.
// Returns the settled status — with `uploading` still set if the cap hits
// during the upload — or null if it never left "running" within the cap.
// Transient poll errors are ignored and retried.
async function pollBaseline(id, throughUpload) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  let last = null;
  for (let i = 0; i < 600; i++) {
    await sleep(2000);
    let st;
    try {
      st = (await api("/api/servers/" + encodeURIComponent(id) + "/baseline")).baseline;
    } catch (_) {
      continue; // a blip mid-dump shouldn't abort the wait
    }
    if (!st || st.state === "running") continue;
    last = st;
    if (!throughUpload || !st.uploading) return st;
  }
  return last;
}

// ── Backups: per-row detail, download, point-in-time restore (#backups) ──

// fmtSeconds renders a duration in the shortest honest unit.
function fmtSeconds(sec) {
  if (sec < 90) return Math.round(sec) + "s";
  if (sec < 5400) return Math.floor(sec / 60) + "m " + Math.round(sec % 60) + "s";
  return Math.floor(sec / 3600) + "h " + Math.round((sec % 3600) / 60) + "m";
}

// "restore to a past time", not "point-in-time restore": point-in-time is the
// word for a copy whose rows are all from one instant (SNAPSHOT_LOCK), and a
// restore can inherit "different points-in-time" from the copy it starts on.
const BACKUP_KIND_LABEL = { dump: "database read", refresh: "automatic refresh", restore: "restore to a past time" };

// Why a scheduled run read the database in full instead of updating the
// previous backup (#1604), keyed by the code the daemon fixed when the run
// happened. The first two are the setting to change, said as such; the
// rest carry the run's own reason, which names the error.
const BACKUP_WHY_REMEDY = {
  no_index: "Set an index connection for this server (Servers) and the next run updates from the recorded changes instead of reading your database in full; without one there are no recorded changes to update from.",
  no_local_dir: "Set a Local folder for this server (under Where and how often) and the next run updates from the recorded changes instead of reading your database in full.",
  first_backup: "First snapshot: there was nothing to update from yet. The next run updates from it.",
};
// The codes whose cause is a setting, so every run until it changes is a
// full read of the database (#1659). first_backup is not one: the next run
// after it updates.
const BACKUP_WHY_EVERY_RUN = new Set(["no_index", "no_local_dir"]);
// The same three, as a FACT about a past run, for the detail of a backup
// that may be months old: "the next run updates" is false there (it ran
// long ago) and "this server has no index connection" may no longer hold.
const BACKUP_WHY_FACT = {
  no_index: "Full read: this server had no index connection at the time, so there were no recorded changes to update from.",
  no_local_dir: "Full read: an update from the recorded changes needed a local snapshot directory, which this server did not have at the time.",
  first_backup: "Full read: the first one, with nothing to update from yet.",
};
// remedy: true on the schedule card (this IS the last run, and the setting
// to change is the point); false on a snapshot's detail (the fact only).
function backupWhyLine(why, code, remedy) {
  if (!why) return "";
  const fixed = (remedy ? BACKUP_WHY_REMEDY : BACKUP_WHY_FACT)[code];
  if (fixed) return fixed;
  // The fold's own refusal rides inside the parentheses of a fallback
  // reason; it is the part backupFoldError knows how to say (its bare
  // --allow-gaps hint is a CLI flag), the wrapper is said here.
  // [\s\S], not ".": a fold refused on several tables joins one line per
  // table, and "." stops at the first newline, which skipped this branch
  // and showed the raw text (a CLI flag inside) for the common gap case.
  const inner = /^[^(]*\(([\s\S]*)\)$/.exec(why);
  // The fold's message follows as its own sentence rather than inside
  // parentheses: backupFoldError may turn it into two sentences. Its
  // "pick a later moment" advice is Time-travel's, not a schedule's.
  const said = (t) => backupFoldError(t).replace(/; pick a later moment\.?(?=\n|$)/g, ".");
  let out;
  if (code === "fold_refused" && inner) {
    out = "The update from the recorded changes was refused, so a full read was taken instead. Reason: " + said(inner[1]);
  } else if (code === "fold_crashed" && inner) {
    out = "The update from the recorded changes hit an internal error, so a full read was taken instead. Error: " + said(inner[1].replace(/^internal error:?\s*/, ""));
  } else if (code === "previous_unreadable" || code === "full_copy") {
    // A full backup the schedule's own timetable asked for (#1564) is its
    // own reason, not a fault: "Full backup because the schedule takes a
    // full backup" would say the words twice.
    out = why.charAt(0).toUpperCase() + why.slice(1);
  } else if (code === "window_measured" || code === "window_age") {
    // The daemon's own numbers (#1721): events, the estimate, the last full
    // backup's duration or the anchor's age. Said as recorded.
    out = "Full read instead of an update: " + why;
  } else {
    out = "Full read because " + why;
  }
  if (!/[.!?]$/.test(out.trim())) out = out.trim() + ".";
  return out;
}

// loadBackupDetail fills a row's expansion: tables with sizes, total weight,
// and duration. The recorded run (this daemon performed it) gives the exact
// duration; otherwise the file timestamps bound it, labeled as such.
// MADE_BY says, in the operator's terms, how a table's rows got into a backup
// (#1545). The three routes have different trust, which is the whole reason to
// show it: a full copy is independent evidence read from the source; the other
// two never touched it.
//
// Plain words rather than the wire values. "fold" and "carried_forward" are
// this codebase's names for its own mechanics, and nobody reading a backup page
// knows them.
const MADE_BY = {
  dump: ["database read", "A full copy read from your database."],
  fold: ["refreshed from changes", "The previous copy brought forward over the recorded changes. Your database was not read."],
  carried_forward: ["reused unchanged", "Nothing changed in this table, so the previous copy was reused as is."],
  // No cause named. This verdict is reached by a backup old enough to predate
  // the record, by a newer version writing a value this build does not know,
  // and by a footer field that would not parse. Picking one of them for the
  // tooltip would be the reader told something nobody checked.
  unknown: ["not recorded", "This snapshot does not say how it was made."],
};

function madeByCell(t) {
  const entry = MADE_BY[t.produced_by];
  if (!entry) return el("span", { class: "bk-made-none", text: "—" });
  // The two derived verdicts name WHEN the database was last really read
  // (#1570), inherited through every update: that is the fact that says how
  // much this copy rests on the recorded changes. The immediate ancestor,
  // which is all the page could name before, is one step back and says
  // nothing about the steps before it; it moves to the tooltip (a reused
  // table: whose file, and so whose date; an update: the backup its file
  // came from), and stays in the cell only for a backup too old to record a
  // read. With changes kept beside the table (#1638) an update records the
  // backup its chain started from, not the one just before it, so the
  // sentence names the file plus the changes since, which is exact either
  // way; when that backup IS the last read, the next sentence already says so.
  let title = entry[1];
  if (t.produced_by === "carried_forward" && t.from) title += " Reused from the snapshot of " + utcLabel(t.from) + ".";
  if (t.produced_by === "fold" && t.from && t.from !== t.source_read_at) {
    title += " Built from the snapshot of " + utcLabel(t.from) + " plus the changes recorded since.";
  }
  if (t.produced_by !== "dump" && t.source_read_at) {
    title += " Last real read of the database: " + utcLabel(t.source_read_at) +
      (t.folds_since_read > 0 ? ", updated " + timesText(t.folds_since_read) + " from the recorded changes since." : ".");
  }
  const cell = el("span", { class: "bk-made", title, text: entry[0] });
  if (t.produced_by !== "dump" && t.source_read_at) {
    cell.append(el("span", { class: "bk-made-from", text: " · last read " + t.source_read_at }));
  } else if (t.from) {
    // A backup written before the read was recorded still names the backup
    // it came from, as it did before #1570: less than the last read, more
    // than nothing.
    cell.append(el("span", { class: "bk-made-from", text: " · " + t.from }));
  }
  return cell;
}

// timesText says a count of updates in words: "once", "twice", "3 times".
function timesText(n) {
  return n === 1 ? "once" : n === 2 ? "twice" : n + " times";
}

// fmtAge is a distance in time for a sentence: seconds, minutes, hours, and
// days once it passes two of them.
function fmtAge(sec) {
  if (sec < 90) return Math.round(sec) + "s";
  if (sec < 5400) return Math.round(sec / 60) + "m";
  if (sec < 172800) return Math.round(sec / 3600) + "h";
  return Math.round(sec / 86400) + " days";
}

// sourceReadLine (#1570) is the snapshot's own line: how far back the last
// real read of the database under this backup goes. An update rebuilds a
// backup from the previous one and the recorded changes, never reading the
// database, so a backup that looks recent can rest on a read days older, and
// a check between two such backups proves less than between two full copies.
// The oldest read among the tables is the one said, since the backup is only
// as independent as its least recently read table, and tables that do not
// record a read are counted, never spoken for. "" when nothing was looked up.
function sourceReadLine(d) {
  const missing = d.source_read_missing || 0;
  const uncounted = d.source_read_uncounted || 0;
  // Two different gaps, said apart: a table that does not record when it was
  // read, and one that records when but not how many updates since (a chain
  // written before the count existed). The maximum is left out whenever any
  // table is uncounted, because a most that skips tables is not the most.
  const tail = (missing ? " " + missing + (missing === 1 ? " table does" : " tables do") + " not record when." : "") +
    (uncounted ? " How many updates were built since is not recorded for " + uncounted + (uncounted === 1 ? " table." : " tables.") : "");
  if (!d.source_read_at) {
    return missing ? "When your database was last read for this snapshot is not recorded." : "";
  }
  const age = d.source_read_age_seconds || 0;
  const folds = d.max_folds_since_read;
  if (age < 1 && folds === 0) return "Read from your database when it was taken." + tail;
  return "Last real read of your database: " + utcLabel(d.source_read_at) +
    (age >= 1 ? ", " + fmtAge(age) + " before this snapshot" : "") +
    (folds > 0 ? ". Updated from the recorded changes " + (folds > 1 ? "up to " : "") + timesText(folds) + " since" : "") +
    "." + tail;
}

// SNAPSHOT_LOCK (#1380) is what a snapshot says about the locks taken when
// the database was read: a short label and one sentence. A snapshot read with
// no locks copies its rows at different moments, so they may not agree with
// each other, and every snapshot updated from it inherits that.
//
// "consistent" draws nothing on a row: it is the normal state. Every other
// answer draws, and so does no answer at all, because a row with nothing on
// it reads as a good one.
//
// The words name the result, never the method (2026-10-01): a safe-no-lock
// read and a PostgreSQL read are point-in-time without taking a lock, so
// "read with locks" was false for them, and "lock" reads to a DBA as "you
// stopped production".
const SNAPSHOT_LOCK = {
  torn: ["different points-in-time", "Its tables were read at different points in time, so their rows may not agree with each other."],
  unknown: ["point-in-time unknown", "It comes from a database read that did not record whether it was point-in-time."],
  unread: ["not checked", "Some tables are only in S3, and this list does not open S3 files to check."],
};

// snapshotLockKey maps the wire value to a key of SNAPSHOT_LOCK, or "" for
// consistent. Anything that is not one of the three words is "unread".
function snapshotLockKey(lock) {
  if (lock === "consistent") return "";
  return lock === "torn" || lock === "unknown" ? lock : "unread";
}

// snapshotLockPill is the mark on a snapshot's row; null for consistent.
function snapshotLockPill(lock) {
  const entry = SNAPSHOT_LOCK[snapshotLockKey(lock)];
  if (!entry) return null;
  // A chip of the status family (#1950): read with no locks is a warning,
  // not recorded and not checked are unknowns.
  const key = snapshotLockKey(lock);
  return el("span", { class: "chip snap-lock " + (key === "torn" ? "chip-warn" : key === "unknown" ? "chip-unknown snap-lock-dashed" : "chip-unknown"), title: entry[1], text: entry[0] });
}

// snapshotLockLine is the detail's own line. A snapshot whose files were not
// read says so: no line would read as a good one. "" only with no detail, or
// no table to speak of.
function snapshotLockLine(d) {
  const total = ((d && d.tables) || []).length;
  const count = (n) => n + " of " + total + (total === 1 ? " table" : " tables");
  const torn = (d && d.lock_torn) || 0;
  const unknown = (d && d.lock_unknown) || 0;
  // No table listed: nothing to say about locks, and above all not "S3".
  if (!d || !(d.tables || []).length) return "";
  if (snapshotLockKey(d.lock) === "unread") return "Not checked: " + SNAPSHOT_LOCK.unread[1];
  if (d.lock === "consistent") return "Point-in-time copy: every row is from the same instant.";
  const parts = [];
  if (torn) parts.push("Different points-in-time: " + count(torn) + ". Their rows were read at different instants and may not agree with each other. A refresh keeps this mark.");
  if (unknown) parts.push("Point-in-time unknown: " + count(unknown) + ". They come from a database read that did not record it. A new read records it.");
  return parts.join(" ");
}

// tableLockMark is the mark beside one table of the detail; null when the
// table is consistent or was not looked up.
function tableLockMark(t) {
  if (!t || !t.lock || t.lock === "consistent") return null;
  const entry = SNAPSHOT_LOCK[snapshotLockKey(t.lock)];
  return el("span", { class: "bk-made-from snap-lock-" + snapshotLockKey(t.lock), title: entry[1], text: " · " + entry[0] });
}

// viewsSkippedCount reads a count of skipped views off the wire: a whole
// number above zero, or 0 for anything else. Nothing recorded arrives as no
// key at all, and that must never be drawn as "0 views skipped".
function viewsSkippedCount(v) {
  return typeof v === "number" && isFinite(v) && v >= 1 ? Math.floor(v) : 0;
}

// viewsSkippedAsOf says of WHEN a carried list is. A snapshot updated from
// the recorded changes read no views: its list is the one of the last full
// read, so it is dated and never said as the present.
function viewsSkippedAsOf(carried, readAt) {
  if (!carried) return "";
  return readAt ? " as of the full read of " + utcLabel(readAt) : " as of an earlier full read";
}

// snapshotViewsText (#1879) is a snapshot row's count: "2 tables, 1 view
// skipped". "" when the snapshot records no skipped view, which is every
// snapshot from before the record and every source with no views.
function snapshotViewsText(sn) {
  const n = viewsSkippedCount(sn && sn.views_skipped);
  if (!n) return "";
  const tables = ((sn && sn.tables) || []).length;
  return (tables === 1 ? "1 table" : tables + " tables") + ", " + (n === 1 ? "1 view" : n + " views") + " skipped" +
    viewsSkippedAsOf(sn.views_carried === true, sn.views_read_at);
}

// viewsSkippedWords is the detail of a snapshot's skipped views: a line, the
// names, and how many are not listed. null when nothing is recorded.
function viewsSkippedWords(v) {
  const n = viewsSkippedCount(v && v.count);
  if (!n) return null;
  const clip = (name) => {
    const chars = Array.from(String(name == null ? "" : name).replace(/\s+/g, " ").trim());
    return chars.length > 130 ? chars.slice(0, 127).join("") + "..." : chars.join("");
  };
  const names = (Array.isArray(v.names) ? v.names : []).map(clip).filter((name) => name).slice(0, 20);
  const left = Math.max(0, n - names.length);
  const carried = v.carried === true;
  return {
    head: (n === 1 ? "1 view" : n + " views") + " skipped" + viewsSkippedAsOf(carried, v.read_at) +
      ". A view holds no rows to copy." +
      (carried ? " This snapshot was updated from the recorded changes and did not read the views again: a view created or dropped since is not shown." : ""),
    names: names,
    more: !left ? ""
      : names.length ? left + " more not listed. The log of the full read names every view."
      : "The log of the full read names " + (n === 1 ? "it." : "each one."),
  };
}

// viewsSkippedBlock draws viewsSkippedWords. Every name is set as text,
// never parsed as markup: it comes from a server.
function viewsSkippedBlock(v) {
  const d = viewsSkippedWords(v);
  if (!d) return null;
  const names = el("ul", { class: "views-skipped-names" });
  for (const name of d.names) names.append(el("li", null, el("code", { text: name })));
  return el("div", { class: "views-skipped" },
    el("p", { class: "form-hint", text: d.head }),
    d.names.length ? names : null,
    d.more ? el("p", { class: "form-hint", text: d.more }) : null);
}

async function loadBackupDetail(at, box) {
  box.textContent = "Loading…";
  let d;
  try {
    d = await api("/api/baselines/files?at=" + encodeURIComponent(at));
  } catch (err) {
    box.textContent = "Could not load the snapshot detail: " + ((err && err.message) || err);
    delete box.dataset.loaded; // a transient failure must not pin the row
    return;
  }
  clear(box);
  const facts = el("div", { class: "bk-facts" });
  // One fact per line, wrapped, never cut (#2002): the why, read and
  // point-in-time lines are whole sentences, and the ellipsis meant for a
  // path (.stg-dest) once showed "Point-in-time copy: every r..." with no
  // way to read the rest. The list takes the room; the Download button
  // keeps its place at its right, or under it when the width runs out.
  const list = el("div", { class: "bk-fact-list" });
  facts.append(list);
  list.append(el("span", { class: "bk-fact", text: humanBytes(d.total_bytes || 0) + " in " + (d.files || 0) + " file(s)" }));
  if (d.run && d.run.seconds > 0) {
    list.append(el("span", { class: "bk-fact", text:
      "took " + fmtSeconds(d.run.seconds) + " (" + (BACKUP_KIND_LABEL[d.run.kind] || d.run.kind) +
      (d.run.rows ? ", " + Number(d.run.rows).toLocaleString("en-US") + " rows" : "") + ")" }));
  } else if (d.write_span_seconds > 0) {
    list.append(el("span", { class: "bk-fact", text:
      "files written over about " + fmtSeconds(d.write_span_seconds) + " (from file timestamps; the real run took longer)" }));
  }
  // Outside the duration branch: a run stamped within one second has no
  // duration to show and still has its reason (#1604).
  if (d.run && d.run.why) list.append(el("span", { class: "bk-fact", text: backupWhyLine(d.run.why, d.run.why_code, false) }));
  const readLine = sourceReadLine(d);
  if (readLine) list.append(el("span", { class: "bk-fact", text: readLine }));
  const lockLine = snapshotLockLine(d);
  if (lockLine) list.append(el("span", { class: "bk-fact", text: lockLine }));
  const dl = el("button", { class: "btn", type: "button",
    text: "Download (.tar.gz) · " + humanBytes(d.total_bytes || 0) });
  if (d.incomplete) dl.disabled = true;
  dl.onclick = (ev) => { ev.stopPropagation(); downloadBackup(at, dl, d.total_bytes || 0); };
  // The download hands over every row, unredacted: query:execute.
  if (sessionMay("query:execute")) facts.append(dl);
  box.append(facts);
  if (d.incomplete) box.append(el("p", { class: "form-msg err", text: "This snapshot is marked incomplete (a failed or unfinished run); it cannot be downloaded or restored from." }));
  // The full read's disk check (#1938): a low disk in the error style, the
  // rest as a plain line.
  if (d.run && d.run.disk_note) box.append(el("p", { class: d.run.disk_check === "low" ? "form-msg err" : "form-hint", text: d.run.disk_note }));
  // A read made without encryption (#1996), as a plain line.
  if (d.run && d.run.transport_note) box.append(el("p", { class: "form-hint", text: d.run.transport_note }));
  const skipped = viewsSkippedBlock(d.views_skipped);
  if (skipped) box.append(skipped);
  const leftOut = leftOutTablesBlock(d.run);
  if (leftOut) box.append(leftOut);
  const tbl = el("table", { class: "bk-table" });
  // The "Made by" column is only rendered when a row actually carries a verdict
  // (#1545). An S3 source does not look it up, and a header over a column of
  // dashes reads as "we checked and there is nothing", which is a different
  // answer from "we did not check".
  const anyProv = (d.tables || []).some((t) => t.produced_by);
  const head = el("tr", {}, el("th", { text: "Table" }), el("th", { text: "Size" }));
  if (anyProv) head.append(el("th", { text: "Made by" }));
  tbl.append(el("thead", {}, head));
  const tb = el("tbody");
  (d.tables || []).forEach((t) => {
    const row = el("tr", {},
      el("td", {}, el("span", { class: "mono", text: t.schema + "." + t.table }), tableLockMark(t)),
      el("td", { text: humanBytes(t.size_bytes || 0) }));
    if (anyProv) row.append(el("td", {}, madeByCell(t)));
    tb.append(row);
  });
  tbl.append(tb);
  box.append(tbl);
}

let backupDownloadBusy = false;

// downloadBackup streams the whole snapshot as one tar.gz. Fetch + blob
// because the API authenticates via header; a plain link would arrive
// tokenless on token-auth consoles. The blob buffers the WHOLE archive in
// browser memory before the save starts — fine for the common case, and the
// confirm below makes the operator choose it knowingly for a huge one.
async function downloadBackup(at, btn, totalBytes) {
  // One at a time, process-wide, and checked before the size prompt: a
  // repaint (a settling run, or paging) replaces the lane while a download
  // is still buffering, which leaves the NEW button enabled and a second
  // click starting a second full archive, both held whole in browser memory.
  // The flag outlives the node the click came from.
  if (backupDownloadBusy) {
    toastError("A snapshot is already downloading. Wait for that one to finish before starting another.");
    return;
  }
  if (totalBytes > 1 << 30 &&
      !window.confirm("This snapshot weighs " + humanBytes(totalBytes) +
        ". The browser holds all of it in memory before saving. Download anyway?")) {
    return;
  }
  backupDownloadBusy = true;
  // Restore the caller's OWN label. This used to re-assert the per-row
  // button's text, which is right for that button and renames every other
  // one: the DuckDB lane's "Download the data" became "Download (.tar.gz) ·
  // 210 MB" after a single click, and the next click captured the clobbered
  // name as its own.
  const btnLabel = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "Preparing…"; }
  try {
    const headers = TOKEN ? { Authorization: ["Bearer", TOKEN].join(" ") } : {};
    if (currentServer) headers["X-Bintrail-Server"] = currentServer;
    const res = await fetch("/api/baselines/download?at=" + encodeURIComponent(at), { headers });
    if (res.status === 401) { handleUnauthorized(); return; }
    if (!res.ok) {
      let msg = "HTTP " + res.status;
      try { msg = (await res.json()).error || msg; } catch (_) { /* non-JSON error body */ }
      throw new Error(msg);
    }
    const cd = res.headers.get("Content-Disposition") || "";
    const m = /filename="([^"]+)"/.exec(cd);
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = m ? m[1] : "dbtrail-backup.tar.gz";
    document.body.appendChild(a);
    a.click();
    a.remove();
    // Delayed: Firefox has cancelled downloads whose object URL was revoked
    // synchronously after click.
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  } catch (err) {
    toastError("Download failed: " + ((err && err.message) || err));
  } finally {
    backupDownloadBusy = false;
    // Never revive a detached button: it belongs to a repainted lane and
    // nobody will ever see it again.
    if (btn && btn.isConnected) { btn.disabled = false; btn.textContent = btnLabel; }
  }
}

// backupRunsInFlight collects what is running right now, for the region.
function backupRunsInFlight(dumpSt, restoreSt, b, sqlSt) {
  const running = [];
  const dump = dumpSt && dumpSt.baseline;
  if (dump && dump.state === "running") {
    running.push({ kind: "dump", text: "Creating a snapshot: copying your data" + (dump.since ? ", since " + utcLabel(dump.since) : "") + "…" });
  } else if (dump && dump.state === "succeeded" && dump.uploading) {
    // Published on this machine; the copy to the backup destination is still
    // running and no longer holds the schedule (#1725): a refresh may start
    // meanwhile, so this line must not read as "the snapshot is still running".
    running.push({ kind: "dump", text: "Snapshot saved on this machine" + (dump.tables ? ": " + dump.tables + " table(s)" : "") +
      ". Still copying it to the snapshot destination…" });
  }
  const rst = restoreSt && restoreSt.restore;
  if (rst && rst.state === "running") {
    running.push({ kind: "restore", text: "Restoring to " + (rst.at ? utcLabel(rst.at) : "the chosen moment") + ": building a new snapshot…" });
  }
  const sq = sqlSt && sqlSt.sql_export;
  if (sq && sq.state === "running") {
    running.push({ kind: "sql-export", text: "Building a .sql export" + (sq.at ? " for " + utcLabel(sq.at) : "") + "\u2026" });
  }
  const rf = b && !b.error && b.refresh;
  if (rf && rf.state === "running") {
    running.push({ kind: "refresh", text: "Automatic refresh running" + (rf.since ? " since " + utcLabel(rf.since) : "") + "…" });
  }
  return running;
}

// backupRunRegion mirrors the verification page's in-progress treatment:
// a live chip, what is happening in words, and the motion strip.
function backupRunRegion(running) {
  const card = el("section", { class: "tcard vfy-region bk-run" });
  running.forEach((r) => {
    card.append(el("div", { class: "vfy-summary" },
      el("span", { class: "chip chip-live", text: "RUNNING" }),
      el("span", { class: "stg-age", text: r.text })));
  });
  card.append(el("div", { class: "vfy-progress" }, el("span", { class: "vfy-progress-bar" })));
  return card;
}

let backupWatchGen = 0;

// watchBackupRuns re-renders the page when the in-flight run settles, so the
// region clears and the new backup appears without a manual reload.
//
// It polls ONLY the kinds that raised the region — literally the sources
// backupRunsInFlight read. The first draft skipped the refresh, so during
// every periodic refresh the watcher woke "settled" after 2s and re-rendered
// in a loop; the second draft polled the per-server endpoints even when the
// selection cannot answer them (the boot entry 409s the monitor verbs), so
// pollFailed held a settled region up for the full 30-minute cap. The
// refresh state only exists on the listing endpoint, which enumerates the
// store (billed LISTs on S3), so it is polled on a slower cadence.
async function watchBackupRuns(id, vgen, kinds) {
  if (!id) return;
  const wgen = ++backupWatchGen;
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  let refreshBusy = kinds.includes("refresh"); // it WAS running at spawn
  for (let i = 0; i < 900; i++) {
    await sleep(2000);
    if (wgen !== backupWatchGen || vgen !== viewGen || !backupsOnScreen()) return;
    let busy = false;
    let pollFailed = false;
    // Only a transport failure or a server fault says "unknown"; a 4xx is a
    // definitive answer about THIS selection and must not hold the region up.
    const unknown = (e) => !e || !e.status || e.status >= 500;
    if (kinds.includes("dump")) {
      try {
        const st = await api("/api/servers/" + encodeURIComponent(id) + "/baseline");
        // Still in flight while the published snapshot is being copied to
        // the destination (#1725): the region's line changes, not ends.
        if (st.baseline && (st.baseline.state === "running" || (st.baseline.state === "succeeded" && st.baseline.uploading))) busy = true;
      } catch (e) { if (unknown(e)) pollFailed = true; }
    }
    if (kinds.includes("restore")) {
      try {
        const st = await api("/api/servers/" + encodeURIComponent(id) + "/baseline/restore");
        if (st.restore && st.restore.state === "running") busy = true;
      } catch (e) { if (unknown(e)) pollFailed = true; }
    }
    if (kinds.includes("sql-export")) {
      try {
        const st = await api("/api/servers/" + encodeURIComponent(id) + "/sql-export");
        if (st.sql_export && st.sql_export.state === "running") busy = true;
      } catch (e) { if (unknown(e)) pollFailed = true; }
    }
    if (kinds.includes("refresh") && i % 5 === 4) { // every ~10s
      try {
        const b = await api("/api/baselines");
        refreshBusy = !!(b.refresh && b.refresh.state === "running");
      } catch (e) {
        // A definitive 4xx means this selection cannot answer; keeping the
        // primed true would hold the region up for the full cap.
        if (unknown(e)) pollFailed = true; else refreshBusy = false;
      }
    }
    if (refreshBusy) busy = true;
    // A failed poll says nothing about the run; treating it as "settled"
    // would clear the RUNNING region while the fold is still going.
    if (pollFailed && !busy) continue;
    if (!busy) {
      if (wgen === backupWatchGen && vgen === viewGen && backupsOnScreen()) renderSnapshots();
      return;
    }
  }
  // Cap expiry: re-render once so a stale RUNNING region does not outlive
  // the watcher silently.
  if (wgen === backupWatchGen && vgen === viewGen && backupsOnScreen()) renderSnapshots();
}

// backupFoldError rewrites a fold refusal for this page: the engine's
// remedies name CLI flags an operator here cannot type (the MCP surface
// avoids the same wording by composing its own remedy at the check site).
// A fold failure is per-table and errors.Join'd, so last_error is usually
// MULTI-LINE: the patterns are global and never cross a line, or one
// table's remedy would eat the next table's identity.
// backupsPer30Days mirrors ParsedBackupSchedule.BackupsPer30Days for the
// grammar the form accepts (a whole number of m, h or d); 0 when it cannot
// be read, and the server's refusal then says why.
// SCHEDULE_CHOICES is the list the Update-the-copy form offers (D13): five
// minutes is the floor the daemon accepts. scheduleChoice maps a saved
// interval to the list's spelling of it (15m and 15min are one choice).
const SCHEDULE_CHOICES = [["5m", "5 min"], ["15m", "15 min"], ["30m", "30 min"], ["1h", "1 h"], ["6h", "6 h"], ["1d", "24 h"]];
function scheduleChoice(every) {
  const m = /^(\d+)\s*(m|min|h|d)$/.exec(String(every || "").trim());
  if (!m) return String(every || "");
  const n = Number(m[1]), u = m[2] === "min" ? "m" : m[2];
  if (u === "h" && n === 24) return "1d";
  if (u === "m" && n === 60) return "1h";
  return n + u;
}

function backupsPer30Days(every) {
  const minutes = intervalMinutes(every);
  return minutes > 0 ? Math.floor(30 * 1440 / minutes) : 0;
}

// intervalMinutes reads the schedule grammar (a whole number of m, h or d)
// as minutes; 0 when it cannot be read.
function intervalMinutes(every) {
  const m = /^\s*(\d+)\s*([mhd])\s*$/.exec(String(every || ""));
  return m ? Number(m[1]) * ({ m: 1, h: 60, d: 1440 })[m[2]] : 0;
}

// lcmInterval is the least common multiple of two intervals, as an interval
// ("Nm"): how often two timetables on the same anchor meet (#1564). "" when
// either cannot be read, which backupsPer30Days reads as 0.
function lcmInterval(a, b) {
  const x = intervalMinutes(a), y = intervalMinutes(b);
  if (!x || !y) return "";
  let p = x, q = y;
  while (q) [p, q] = [q, p % q];
  return (x / p * y) + "m";
}

// plainWords is the copy rule for reasons the daemon assembles at runtime:
// they never appear in this file, so the source guard cannot see an em dash
// in them, and one does ride in on fold errors.
function plainWords(msg) {
  return String(msg == null ? "" : msg).replace(/\u2014/g, "-");
}

function backupFoldError(msg) {
  let out = String(msg)
    .replace(/;?[ \t]*pass --allow-gaps to proceed[^.;\n]*/g,
      ". The recorded history has a permanent gap in that window, so the snapshot would be incomplete; pick a later moment")
    .replace(/,?[ \t]*or target a different instant with --at/g, ", or pick another second")
    .replace(/ \u2014 a snapshot emitted from it would carry the OLD CREATE TABLE forward and project every row onto the old columns and types, so every reconstruct anchored on it would be wrong\. Take a real snapshot instead: `bintrail dump` \+ `bintrail baseline`\. \(If the schema snapshot is what is stale, run `bintrail snapshot` first and retry\.\): schema changed since the baseline/g,
      ". Updating it from the recorded changes needs a snapshot taken after that change")
    // The engine's advice names a command; the console says it in its own
    // words, and only as a possibility (#2006).
    .replace(/;?\s*consider re-running .bintrail snapshot./g,
      "; if the table was created after the last schema snapshot, a new schema snapshot may fix this")
    .replace(/\u2014/g, "-");
  if (!/[.!?]$/.test(out.trim())) out = out.trim() + ".";
  return out;
}

// choicePills draws a closed set of choices as pills, one lit, and behaves
// like the select it replaces: `value` reads and sets the lit one, a click
// lights it and calls onChange. Pills say the whole question at once; a
// dropdown says one answer and hides the rest.
function choicePills(choices, value, label, onChange) {
  const group = el("div", { class: "sch-pills", role: "radiogroup", "aria-label": label });
  const pills = [];
  let current = value;
  const paint = () => {
    for (const p of pills) {
      const on = p.dataset.value === current;
      p.classList.toggle("is-on", on);
      p.setAttribute("aria-checked", on ? "true" : "false");
      p.setAttribute("tabindex", on ? "0" : "-1");
    }
  };
  for (const [v, t] of choices) {
    const pill = el("button", { class: "sch-pill", type: "button", role: "radio", "aria-checked": "false", tabindex: "-1", text: t });
    pill.dataset.value = v;
    pill.onclick = () => { current = v; paint(); if (onChange) onChange(); };
    pills.push(pill);
    group.append(pill);
  }
  group.addEventListener("keydown", (e) => {
    const i = pills.findIndex((p) => p.dataset.value === current);
    let to = -1;
    if (e.key === "ArrowRight" || e.key === "ArrowDown") to = (i + 1) % pills.length;
    else if (e.key === "ArrowLeft" || e.key === "ArrowUp") to = (i - 1 + pills.length) % pills.length;
    if (to < 0) return;
    e.preventDefault();
    pills[to].onclick();
    pills[to].focus();
  });
  Object.defineProperty(group, "value", { get: () => current, set: (v) => { current = v; paint(); } });
  paint();
  return group;
}

// backupScheduleCard (#1442): the per-server backup timer. The state line
// under the heading carries the schedule and the next run, so the state
// reads without going into the body; the body is the form plus what the
// schedule last did. A run that cannot start, was skipped or failed turns
// that line red AND names the fact on it: a schedule that fails quietly is
// worse than none, and a red line whose words say nothing is wrong is the
// same silence in a louder colour.
//
// The operator picks WHEN. How each run is made (a full backup from the
// database, or updating the latest backup from the recorded changes) is the
// daemon's call per run, and the card SAYS which one comes next and why, so
// nobody has to understand the machinery to schedule a backup.
//
// Only the FORM is gated on the capability. A saved schedule is rendered
// whenever the listing carries one, capability or not: a daemon restarted
// with every backup feature off still has the schedule in its file, the API
// reports it as not runnable with the reason, and hiding the card there
// would hide exactly the message this feature exists to show.
// momentField is the "as of" field of the restore card and the .sql lane:
// the date-time component (the mono field with its calendar button, the
// same one the Events filters use), never a bare text input. Typing still
// works; the picker only fills the same text. Returns { wrap, input }.
function momentField(value) {
  const input = el("input", { class: "input dt-input", type: "text", spellcheck: "false",
    placeholder: "YYYY-MM-DD HH:MM:SS", "aria-label": "Moment (UTC)" });
  input.value = value || "";
  const trigger = el("button", { class: "btn btn-icon btn-sm btn-ghost dt-trigger", type: "button", "aria-label": "Open calendar" },
    icon("calendar", "dt-trigger-ic"));
  trigger.addEventListener("click", (e) => { e.preventDefault(); toggleDatePicker(input, trigger); });
  return { wrap: el("div", { class: "dt-wrap bk-moment" }, input, trigger), input, trigger };
}

// scheduleChainDraw draws how the copy moves (#1950): one read of the
// database, then updates built from the recorded changes (each a pair of
// change files), and the next link dashed: a pair when the next run is an
// update, a tile when it is a full read. It replaced the healthy "Next run
// will ..." sentence and carries it, reason included, as its text
// alternative and tooltip. The keep-shape drawing's own tiles (ks-), so the
// page speaks one drawing language. A next run that is a WARNING (every run
// reads the database in full) is never drawn: it stays a red sentence.
function scheduleChainDraw(sch, alt) {
  const full = sch.next_method !== "refresh";
  const box = el("div", { class: "ks-chain", role: "img", "aria-label": alt, title: alt });
  const row = el("div", { class: "ks-row" });
  const pair = (cls) => el("span", { class: "ks-pair" + (cls || "") }, el("i"), el("i"));
  row.append(el("span", { class: "ks-tile" }), pair(), pair(), pair());
  row.append(full ? el("span", { class: "ks-tile ks-next" }) : pair(" ks-next"));
  box.append(row);
  box.append(el("div", { class: "ks-axis" },
    el("span", { class: "ks-left", text: "a full read, then updates" }),
    el("span", { class: "ks-right", text: "next: " + (full ? "full read" : "update") + (sch.next_run ? " " + flowHHMM(sch.next_run) : "") })));
  return box;
}

function backupScheduleCard(cur, b) {
  if (!cur || !cur.id || cur.kind !== "registry") return null;
  if (!b || b.error) return null;
  const sch = b.schedule || null;
  // Two different reasons this card cannot be edited, handled differently.
  // The FEATURE being off is the read-only console: the early return below,
  // with the line that says where to turn it on. The session lacking
  // servers:write is not a reason to show less: the card still draws every
  // fact about how the schedule is running — the last failed run, a run that
  // cannot start, backups that did not reach the destination — exactly as
  // the editable view does, and only the form is taken out at the end. A
  // first cut returned early for both, and a view-only reader saw a failed
  // 03:00 run summarised in the grey of a healthy schedule.
  const canEdit = !!capsCache.backup_schedule;
  const mayWrite = sessionMay("servers:write");
  if (!sch && !(canEdit && mayWrite)) return null;
  // A card, not a fold (#1528). Putting backups on a timetable is the thing
  // this page is named after, and it sat behind a line of small caps that had
  // to be clicked. What the summary carried is now the card's state line, so
  // the fact that used to be readable in passing is still readable in
  // passing, and the form behind it no longer costs a click to find.
  const card = el("section", { class: "ov-panel bk-restore bk-schedule stg-card" });
  card.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title stg-card-t" }, icon("refresh", "stg-ico stg-ico-pink"), "Update the copy")));
  const state = el("p", { class: "form-hint bk-card-state" });
  card.append(state);
  const body = el("div", { class: "bk-card-body" });

  // The state line: what an operator reads without stopping.
  // The full backup a schedule asks for on its own timetable (#1564) and,
  // when it cannot start, why: in red BEFORE its slot, in every view of the
  // card, the read-only one included. The rest of the schedule keeps running,
  // which the line says, so a refused full backup is not read as a stopped
  // schedule.
  let fullWarn = null;
  if (sch && sch.runnable && sch.full_reason) {
    const why = plainWords(sch.full_reason);
    fullWarn = el("p", { class: "form-msg err", text:
      why.charAt(0).toUpperCase() + why.slice(1) + (/[.!?]$/.test(why) ? "" : ".") +
      " The full reads do not run until that changes; the other scheduled runs still do." });
  }
  if (!sch) {
    state.textContent = "Not on a schedule. Pick how often the copy updates.";
  } else {
    let line = "Every " + sch.every + " at " + sch.at + " UTC" +
      (sch.full_every ? ", with a full read every " + sch.full_every : "") + ".";
    if (sch.runnable && sch.next_run) line += " Next: " + utcLabel(sch.next_run) + ".";
    if (!sch.runnable) {
      // Terminated, the same way the next-run warning below terminates its
      // reason: the daemon's refusals end bare, and since #1528 a note can
      // follow this text on the same line, which ran the two sentences
      // together ("... turned off (under Where and how often) The last run
      // failed.").
      const why = plainWords(sch.reason || "unknown reason");
      line += " Cannot run: " + why + (/[.!?]$/.test(why) ? "" : ".");
    }
    state.textContent = line;
    // Red here, BEFORE the read-only return below. That path renders no run
    // history at all, so a refusal is the only alarm it can carry, and it is
    // the exact case the comment above says this card exists for.
    if (!sch.runnable) state.classList.add("alarm");
    if (fullWarn) state.classList.add("alarm");
  }

  if (!canEdit) {
    // The read-only console, or a daemon with every backup feature off:
    // nothing here can change the schedule, and the state line already says
    // why it is not running.
    // No run history renders here, so the refusal's note goes on the state
    // line directly (the editable view ranks it with the other alarms).
    if (fullWarn) {
      state.textContent += " The full read cannot run.";
      body.append(fullWarn);
    }
    body.append(el("p", { class: "form-hint", text:
      "This schedule can be changed from the DBTrail service's web interface (CLI: bintrail-console watch) once its snapshot features are on." }));
    card.append(body);
    return card;
  }

  // The form (D13): one list of intervals, five minutes to a day. The UTC
  // hour a daily run lines up on shows only for the daily choice; the full
  // read timetable (#1564) is kept as saved, never edited here.
  // The choice is a row of pills, one lit: a reader sees the interval in
  // force and the alternatives at once, where a closed dropdown showed one
  // value and hid the question. A saved interval outside the list gets its
  // own pill, so the form never shows a choice the schedule does not have.
  const saved = sch ? scheduleChoice(sch.every) : "1d";
  const choices = SCHEDULE_CHOICES.slice();
  if (!choices.some(([v]) => v === saved)) choices.push([saved, sch.every]);
  const at = el("input", { class: "input", type: "text", spellcheck: "false", placeholder: "03:00", "aria-label": "At (UTC)" });
  at.value = sch ? sch.at : "03:00";
  at.style.maxWidth = "90px";
  const atWrap = el("span", { class: "bk-sched-at" }, el("span", { class: "form-hint", text: "at" }), at, el("span", { class: "form-hint", text: "UTC" }));
  const syncAt = () => { atWrap.hidden = every.value !== "1d"; };
  // Filled once the form is touched, like the server card's Save (#1950):
  // at rest neither Save on this tab asks for the eye.
  const touched = () => save.classList.add("btn-primary");
  const every = choicePills(choices, saved, "Update the copy every", () => { syncAt(); touched(); });
  syncAt();
  const save = el("button", { class: "btn", type: "button", text: sch ? "Save" : "Turn on", "data-save": "schedule" });
  at.addEventListener("input", touched);
  const msg = el("p", { class: "form-msg err" });
  msg.hidden = true;
  save.onclick = () => saveBackupSchedule(cur.id, { every: every.value, at: at.value.trim(), full_every: sch && sch.full_every ? sch.full_every : "" }, save, msg);
  const row = el("div", { class: "bk-restore-row", "data-sched-edit": "1" }, atWrap, save);
  if (sch) {
    const remove = el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Turn off" });
    remove.onclick = () => removeBackupSchedule(cur.id, remove, msg);
    row.append(remove);
  }
  every.setAttribute("data-sched-edit", "1");
  msg.setAttribute("data-sched-edit", "1");
  body.append(every, row, msg);

  // What the schedule will do next, and what it last did. The skip is
  // shown when it is the newest fact: a slot that could not start after
  // the last good run is exactly what the operator needs to see.
  if (sch) {
    // alarmNote is the few words the STATE LINE gets when one of these
    // branches raises the alarm. The red alone used to mean "the fold below
    // is open, the reason is in it"; with the body always visible the colour
    // would be a verdict with no words, which is the noise CONTRIBUTING's
    // rule 4 names.
    //
    // Only one note fits, and POSITION IN THIS FUNCTION MUST NOT DECIDE IT.
    // The two forward-looking warnings are written first and any past alarm
    // replaces them. The past ones carry a timestamp and go through noteAt,
    // which keeps the NEWEST: the daemon records a fallback when the full
    // backup STARTS, so a plain last-one-wins let the fallback outrank the
    // FAILURE of that very backup, and the line read "a full backup ran
    // instead" while the body said the backup failed and nothing was
    // written. A false all-clear on the backups page is worse than the
    // wordless red this note exists to replace.
    //
    // noteAt takes the first alarm unconditionally, so a raised alarm can
    // never end up with no words at all. After that the stamps decide, and
    // on a TIE the outcome beats the start. The stamps are whole seconds
    // (RFC3339, no fraction) and the daemon writes ONE stamp for both the
    // fallback and the run it starts, so a fallback whose own full backup
    // fails inside that second ties exactly, every time, not by luck. The
    // fallback therefore needs a strictly newer stamp to speak (startsRun),
    // while every other fact needs only an equal one: a skip recorded in the
    // second a run finished is the newer fact, which is the rule the skip
    // branch below was already written with.
    //
    // alarmAt starts null, not "": an undated fact must not leave the slot
    // looking empty, or the next one would overwrite it and the ranking
    // would quietly fall back to whoever is written last in this function.
    let alarm = false, alarmNote = "", alarmAt = null, everyRunCode = "";
    const noteAt = (at, note, startsRun) => {
      const a = at || "";
      if (alarmAt === null || (startsRun ? a > alarmAt : a >= alarmAt)) { alarmNote = note; alarmAt = a; }
    };
    if (sch.runnable && sch.next_method_error) {
      // Runnable in principle, but the next slot will be skipped as things
      // stand: said in red BEFORE the slot, not discovered after it.
      alarm = true;
      alarmNote = "The next run cannot start.";
      body.append(el("p", { class: "form-msg err", text:
        "The next run cannot start: " + plainWords(sch.next_method_error) + (/[.!?]$/.test(sch.next_method_error) ? "" : ".") }));
    } else if (sch.runnable && sch.next_method) {
      const how = sch.next_method === "refresh" ? "will refresh the latest snapshot from the recorded changes" : "will read your whole database";
      // A setting that makes EVERY run a full read (no Backup dir, no index
      // connection) is a warning, not the grey of a healthy prediction
      // (#1659); a first backup or a one-off unreadable bucket stays a hint.
      const everyRun = sch.next_method !== "refresh" && BACKUP_WHY_EVERY_RUN.has(sch.next_method_why_code);
      if (everyRun) {
        alarm = true;
        alarmNote = "Every run reads your database in full.";
        everyRunCode = sch.next_method_why_code;
      }
      const nextLine = "Next run " + how + (sch.next_method_why ? " (" + sch.next_method_why + ")." : ".");
      if (everyRun) {
        body.append(el("p", { class: "form-msg err", text:
          nextLine + (sessionMayConfigureServer() ? " " + BACKUP_WHY_REMEDY[sch.next_method_why_code] : "") }));
      } else {
        body.append(scheduleChainDraw(sch, nextLine));
      }
    }
    if (sch.history_unavailable) {
      // Without the run history only what this daemon started since boot is
      // known; "it has not run yet" would be a guess, so say what is missing.
      alarm = true;
      alarmNote = "The run history could not be opened.";
      body.append(el("p", { class: "form-msg err", text:
        "The snapshot run history could not be opened, so runs from before DBTrail started are not shown. Check its log." }));
    }
    // The full backup the schedule asks for (#1564): its refusal, or when
    // the next one is due if that is not the next run already said above.
    // The refusal is forward-looking like the next-run warning: its note is
    // kept only while no other alarm has claimed the line, and any past
    // alarm replaces it.
    if (fullWarn) {
      alarm = true;
      if (!alarmNote) alarmNote = "The full read cannot run.";
      body.append(fullWarn);
    } else if (sch.runnable && sch.next_full_run && sch.next_full_run !== sch.next_run) {
      body.append(el("p", { class: "form-hint", text: "Next full read the schedule asks for: " + utcLabel(sch.next_full_run) + "." }));
    }
    if (sch.running) {
      body.append(el("p", { class: "form-hint", text: "A scheduled snapshot is running now." }));
    }
    const run = sch.last_run, skip = sch.last_skipped, fb = sch.last_fallback;
    if (run) {
      const when = utcLabel(run.finished_at || run.started_at || "");
      const what = run.method === "refresh" ? "update from the recorded changes" : "full read";
      if (run.ok) {
        const reused = run.carried || 0;
        body.append(el("p", { class: "form-hint", text:
          "Last scheduled snapshot finished " + when + " (" + what + "): " + (run.tables || 0) + " table(s)" +
          (reused ? ", " + reused + " unchanged and reused" + reusedCopiedNote(run.carried_copied || 0) : "") +
          (run.uploaded ? ", " + run.uploaded + " file(s) uploaded" : "") + "." }));
      } else {
        alarm = true;
        noteAt(run.finished_at || run.started_at || "",
          run.snapshot_time ? "The last run could not send its snapshot." : "The last run failed.");
        // A failed run that still names a snapshot is the one shape where the
        // backup exists: the fold finished and only the upload failed. Telling
        // that operator nothing was written would send them looking for a
        // backup they already have.
        if (!run.snapshot_time && !run.published && run.method !== "refresh") {
          // A full read that published nothing: the same card the toast and
          // the Overview draw (#1986), with the error folded.
          body.append(el("p", { class: "form-msg err", text: "Last scheduled snapshot failed " + when + " (" + what + ")." }),
            snapshotFailureCard(run.failure, run.error || "unknown error", "scheduled", "", (run.failure && run.failure.server) || cur.name || ""));
        } else {
          body.append(el("p", { class: "form-msg err", text: run.snapshot_time
            ? "Last scheduled snapshot " + when + " (" + what + ") wrote the snapshot on this machine but could not " +
              "send it to the snapshot destination: " + backupFoldError(run.error || "unknown error") +
              " The snapshot is on disk and can be restored from. The next scheduled run folds a new one."
            : "Last scheduled snapshot failed " + when + " (" + what + ")" +
              (refusedTableRows(run) ? "." : ": " + backupFoldError(run.error || "unknown error")) +
              " Nothing was overwritten; the next scheduled run tries again." }));
        }
        const stopped = refusedTablesBlock(run);
        if (stopped) body.append(stopped);
      }
      // Tables a full read left out of its snapshot (#2006), in red.
      const leftOut = leftOutTablesBlock(run);
      if (leftOut) { alarm = true; noteAt(run.finished_at || run.started_at || "", "A table is not in the last snapshot."); body.append(leftOut); }
      // Tables the update left out (#1993): said on a published update, the
      // one whose snapshot lacks them.
      if (run.method === "refresh") {
        const left = newTablesBlock(run, ((b && b.snapshots) || [])[0]);
        if (left) body.append(left);
      }
      // A full read that ran on a low disk, or without its disk check
      // (#1938): nobody clicked, so the card is where it is said.
      if (run.disk_note && run.disk_check !== "ok") {
        body.append(el("p", { class: run.disk_check === "low" ? "form-msg err" : "form-hint", text: run.disk_note }));
      }
      // A full read that reached the source without encryption (#1996).
      if (run.transport_note) body.append(el("p", { class: "form-hint", text: run.transport_note }));
      // The reason a full backup was taken, as recorded when it ran, and
      // the setting that turns the next one into an update (#1604). After
      // BOTH branches: a full read that then failed is the run whose
      // operator most needs to know why it was a full read.
      // Not for a fallback: the red alarm below already carries the same
      // refusal, and the card is in alarm precisely then.
      // Nor when the next-run warning above already says the same remedy.
      if (run.method !== "refresh" && run.why && run.why_code !== everyRunCode &&
          !(fb && (run.why_code === "fold_refused" || run.why_code === "fold_crashed"))) {
        // In the past tense when the next-run warning carries a remedy: a
        // second remedy for a different setting would read as a contradiction.
        body.append(el("p", { class: "form-hint", text: backupWhyLine(run.why, run.why_code, !everyRunCode && sessionMayConfigureServer()) }));
      }
    }
    if (fb) {
      // Always shown while the daemon remembers it, in red: an update that
      // is refused at every slot means the no-load half of this feature is
      // dead and production is being read in full instead, and a green
      // "last snapshot finished" line would hide exactly that. A crash is
      // named as one, not as a refusal. The daemon records a fallback only
      // once the full backup actually started, so "started" is a fact.
      alarm = true;
      if (fb.stopped_at) {
        // #2006: the update was refused again for the same tables right
        // after a full read went through, so the daemon takes no more full
        // reads for it. Said apart from the fallback line below: nothing is
        // being published, and reading the database again will not change
        // that.
        noteAt(fb.stopped_at, "Updates are refused, and a full read does not fix it.");
        // The list is withheld from a session whose data profile hides
        // table names: then the count stands in for it.
        const listed = (fb.refused_tables || []).length;
        const count = Math.max(listed + (Math.floor(Number(fb.refused_tables_omitted)) || 0), Math.floor(Number(fb.refused)) || 0);
        const which = listed ? (count === 1 ? "the table below" : "the tables below") : (count === 1 ? "1 table" : count + " tables");
        body.append(el("p", { class: "form-msg err", text:
          "No new snapshot is being published for this server. The update from the recorded changes was refused for " +
          which + ", and refused the same way again right after a full read, so another full read would not fix it. " +
          "Since " + utcLabel(fb.stopped_at) + " DBTrail takes no full read in its place, except one a day to check. " +
          "The update keeps running at each scheduled time and reads only the recorded changes; the first one that goes through " +
          "ends this." + (listed ? " " + (count === 1 ? "The reason under it" : "The reason under each table") + " says what has to change." : "") }));
        const stuck = refusedTablesBlock(fb, "No full read is taken in its place.");
        if (stuck) body.append(stuck);
      } else {
        // startsRun: this stamp is when the full backup STARTED, so it must
        // never outrank that backup's own outcome recorded in the same second.
        noteAt(fb.at, "An update was refused, so a full read ran instead.", true);
        const crashed = /^internal error/.test(fb.reason || "");
        const why = backupFoldError(crashed ? fb.reason.replace(/^internal error:?\s*/, "") : fb.reason);
        body.append(el("p", { class: "form-msg err", text:
          "At " + utcLabel(fb.at) + " the update from the recorded changes " + (crashed ? "hit an internal error" : "was refused") +
          (refusedTableRows(fb) ? "," : " (" + why + ")") +
          " so a full read was started instead. If this repeats, the recorded changes cannot be used for this server; check the reason." }));
        const stopped = refusedTablesBlock(fb, "A full read was started instead.");
        if (stopped) body.append(stopped);
      }
    }
    // A full backup of the schedule's own timetable that did not start, or
    // started and failed (#1564): red until a full backup succeeds, not
    // until the next run ends. A missed run is made up by the next one; a
    // missed weekly full backup by nothing for a week, and a grey card
    // saying when the next is due would hide that this one never happened.
    // The next one's time only when it will run: a refused timetable already
    // says in red that none will until that changes.
    const fm = sch.last_full_missed;
    if (fm) {
      alarm = true;
      noteAt(fm.at, fm.failed ? "The last full read failed." : "The last full read did not run.");
      const cause = backupFoldError(fm.reason || "unknown reason");
      const next = !(sch.runnable && !sch.full_reason && sch.next_full_run) ? ""
        : sch.full_owed ? " The next scheduled run takes it, at " + utcLabel(sch.next_full_run) + "."
        : " The next one is due at " + utcLabel(sch.next_full_run) + ".";
      // Lowercased to continue the sentence, unless the first word is a name
      // or an acronym ("DBTrail was not running..." must not read "dBTrail").
      const lead = /^[A-Z][a-z]/.test(cause) ? cause.charAt(0).toLowerCase() + cause.slice(1) : cause;
      body.append(el("p", { class: "form-msg err", text:
        (fm.failed ? "The full read that started at " + utcLabel(fm.at) + " failed: " : "The full read due at " + utcLabel(fm.at) + " did not run: ") +
        lead + next }));
    }
    // >= not >: the stamps are whole seconds, and a skip recorded in the
    // same second a run finished (the fallback's collision case) is the
    // newer fact, not an older one.
    if (skip && (!run || skip.at >= (run.finished_at || ""))) {
      alarm = true;
      // The daily cap on full reads the schedule takes on its own (#2006):
      // the update DID run; what waits is the full read in its place.
      const capped = /at most once a day/.test(skip.reason || "");
      noteAt(skip.at, capped ? "A full read was held back: at most one a day." : "A scheduled run did not start.");
      // backupFoldError, not plainWords: a fold error rides in here too,
      // with its bare --allow-gaps hint.
      body.append(el("p", { class: "form-msg err", text: capped
        ? "At " + utcLabel(skip.at) + " " + backupFoldError(skip.reason)
        : "Did not run at " + utcLabel(skip.at) + ": " + backupFoldError(skip.reason) +
          scheduleSkipTail(skip.reason) }));
    }
    if (!run && !skip && !fm && !sch.running && !sch.history_unavailable) {
      body.append(el("p", { class: "form-hint", text: "It has not run yet." }));
    }
    // Nothing to open any more, so the alarm moves to the state line: a
    // schedule whose last word is a refusal must not be summarised in the
    // grey of a healthy one, and the red line at the top is what a reader
    // scanning the page sees before any of the detail below it. The note
    // goes with the colour, never instead of it: colour alone is a verdict
    // a screen reader cannot read out and a colour-blind operator cannot
    // see. classList.add, not a className rewrite: the line's own class
    // carries its gutter, and rebuilding the list by hand would drop
    // whatever is added at the top of this function later.
    if (alarm) state.classList.add("alarm");
    if (alarmNote) state.textContent += " " + alarmNote;
  }
  // Without servers:write, the form goes and everything it would have
  // changed stays: the status lines above were drawn exactly as for an
  // editor. Hidden by permission, so nothing says the form was here.
  if (!mayWrite) body.querySelectorAll("[data-sched-edit]").forEach((n) => n.remove());
  card.append(body);
  return card;
}

async function saveBackupSchedule(id, sched, btn, msgEl) {
  msgEl.hidden = true;
  btn.disabled = true;
  let saved;
  try {
    saved = await api("/api/servers/" + encodeURIComponent(id) + "/backup-schedule", { method: "PUT", body: sched });
  } catch (err) {
    msgEl.textContent = (err && err.message) || String(err);
    msgEl.hidden = false;
    btn.disabled = false;
    return;
  }
  btn.disabled = false;
  // The response already knows whether the next slot can start; a toast
  // promising a run above a red line saying it cannot would be a lie.
  const next = saved && saved.schedule;
  toast(next && next.next_method_error ? "Snapshot schedule saved, but the next run cannot start yet. See the reason on the page."
    : "Snapshot schedule saved. It runs at the next scheduled time.");
  if (backupsOnScreen()) renderSnapshots();
}

async function removeBackupSchedule(id, btn, msgEl) {
  msgEl.hidden = true;
  btn.disabled = true;
  try {
    await api("/api/servers/" + encodeURIComponent(id) + "/backup-schedule", { method: "DELETE" });
  } catch (err) {
    msgEl.textContent = (err && err.message) || String(err);
    msgEl.hidden = false;
    btn.disabled = false;
    return;
  }
  toast("Snapshot schedule removed.");
  if (backupsOnScreen()) renderSnapshots();
}

// backupRestoreCard offers the point-in-time restore: pick a past moment, get
// a NEW backup showing every table as it was then. A card, not a fold
// (#1528): behind a summary, the one sentence that says your database is not
// touched was the thing a reader had to click to find. The last restore's
// outcome is the card's state line, under the heading, where the fold used
// to force itself open on a failure; as a card it has no open to force, and
// leaving that line at the bottom of the body would have put the failure in
// the least-read place on the card.
function backupRestoreCard(cur, b, restoreSt) {
  if (!capsCache.baseline_restore || !cur || !cur.id) return null;
  // Without baseline:create the form goes, the outcome of the last restore
  // stays: GET .../baseline/restore is servers:read, and "the restore wrote
  // the copy here but could not send it to S3" is a fact about whether a
  // copy is safe, not a control. See the end of this function.
  const mayCreate = sessionMay(PERM_SNAPSHOT_CREATE);
  // The cases where this server cannot restore draw the card switched off,
  // with the reason (#1684); it used to vanish without a word. Only for a
  // session that could restore otherwise: to one that cannot, a switched-off
  // control says nothing it needs.
  const off = (why, text) => mayCreate ? restoreOffCard(why, text) : null;
  // Registry servers only: the CLI (ephemeral) entry is refused by the
  // monitor verbs with a message about monitoring, not restores.
  if (cur.kind !== "registry") return off("cli", "Restores are for servers added in DBTrail. The server given on the command line has no snapshot folder of its own to save the result in.");
  // The server needs its OWN local backup directory to build INTO: a
  // restore saves a new snapshot there.
  if (!cur.baseline_dir) return off("no-folder", "A restore saves its result in this server's own snapshot folder, and it has none. Set one on Snapshots.");
  if (cur.write_refusal) return off("shared", cur.write_refusal);
  if (!b || b.error) return off("list-error", "The snapshot list could not be read" + (b && b.error ? ": " + b.error : "."));
  if (!b.configured) return off("no-folder", "A restore saves its result in this server's own snapshot folder, and it has none. Set one on Snapshots.");
  // Where the restore READS is the other half (#1541): this server's S3
  // backups when it has them, else that directory — the same rule the
  // scheduled update follows (BaselineFoldSource), and the same one the
  // coverage card reports as restore_reads. Offering the local-only rows on an
  // S3-backed server would prefill a moment the fold then refuses.
  const reads = cur.baseline_s3 ? "s3" : "dir";
  const usable = backupSnapshotsFor(b, reads);
  if (!usable.length) {
    return off("no-snapshot", "There is no snapshot to restore from yet" +
      (reads === "s3" ? " in this server's S3 location" : " in this server's folder") + ". Take one on Snapshots.");
  }
  const rst = restoreSt && restoreSt.restore;
  if (rst && rst.state === "running") return null; // the run region owns it
  const card = el("section", { class: "ov-panel bk-restore" });
  card.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: mayCreate ? "Restore to a moment" : "Last restore" })));
  const state = el("p", { class: "form-hint bk-card-state" });
  state.hidden = true;
  card.append(state);
  const body = el("div", { class: "bk-card-body" });
  body.append(el("p", { class: "form-hint", text:
    "Pick a past moment. DBTrail rebuilds every table as it was then and saves the result as a new snapshot on the Snapshots page. Your database is not touched." }));
  const moment = momentField((usable[0] && usable[0].time) || "");
  const input = moment.input;
  const go = el("button", { class: "btn", type: "button", text: "Restore" });
  const msg = el("p", { class: "form-msg err" });
  msg.hidden = true;
  go.onclick = () => startBackupRestore(cur.id, input.value.trim(), go, msg);
  body.append(el("div", { class: "bk-restore-row" }, moment.wrap, go), msg);
  const elsewhere = backupElsewhereNote(b, usable, reads);
  if (elsewhere) body.append(elsewhere);
  if (rst && rst.state === "failed") {
    // published means the fold finished and the snapshot is on disk, and only
    // sending it to S3 failed (#1541). "Published nothing" there is false: the
    // backup is in the list below and can be restored from.
    state.hidden = false;
    state.classList.add("alarm");
    state.textContent = rst.published
      ? "Last restore wrote the snapshot on this machine but could not send it to S3: " + backupFoldError(rst.last_error || "unknown error") + " The snapshot is in the list below. A full read sends it along with the rest."
      : restoreRefusedLine(rst);
  } else if (rst && rst.state === "succeeded") {
    // The reused count belongs here for the same reason it belongs on the
    // refresh note: a restore consumes the same reuse setting, and without the
    // number nothing on this page confirms the setting did anything.
    state.hidden = false;
    state.textContent =
      "Last restore finished" + (rst.at ? ": the snapshot at " + utcLabel(rst.at) : "") + " is in the list below." +
      (rst.carried ? " " + rst.carried + " table(s) reused an unchanged file" + reusedCopiedNote(rst.carried_copied || 0) + "." : "");
  }
  if (!mayCreate) {
    // Only the outcome line, and no card at all when there is none: an
    // empty "Restore to a moment" panel offers nothing to a reader who
    // cannot restore.
    if (state.hidden) return null;
    return card;
  }
  card.append(body);
  return card;
}

// restoreOffCard is the restore card switched off: the same heading and
// controls, disabled, with why in place of the explanation. data-why names
// the case for the page and the tests.
function restoreOffCard(why, text) {
  const card = el("section", { class: "ov-panel bk-restore bk-restore-off" });
  card.dataset.why = why;
  card.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Restore to a moment" })));
  card.append(el("p", { class: "form-hint bk-card-state", text }));
  const moment = momentField("");
  const go = el("button", { class: "btn", type: "button", text: "Restore" });
  moment.input.disabled = moment.trigger.disabled = go.disabled = true;
  card.append(el("div", { class: "bk-restore-row" }, moment.wrap, go));
  return card;
}

async function startBackupRestore(id, at, btn, msgEl) {
  msgEl.hidden = true;
  if (!at) { msgEl.textContent = "Enter a UTC time, YYYY-MM-DD HH:MM:SS."; msgEl.hidden = false; return; }
  btn.disabled = true;
  try {
    await api("/api/servers/" + encodeURIComponent(id) + "/baseline/restore", { method: "POST", body: { at: at } });
  } catch (err) {
    msgEl.textContent = (err && err.message) || String(err);
    msgEl.hidden = false;
    btn.disabled = false;
    return;
  }
  btn.disabled = false;
  toast("Restore started: building a snapshot as of " + at + " UTC…");
  if (backupsOnScreen()) renderSnapshots();
}

// ── Take a copy with you ───────────────────────────────────────────────────
//
// This page had two downloads and neither could be found. The Parquet
// .tar.gz sat inside a row's expand with nothing saying the row opened, and
// the .sql builder was a <details> summary in small caps below the page's
// other folds (#1528 has since made two of those cards, leaving the count
// here as history rather than a description of the page). A reader who
// wants a copy arrives with one question -- what do I
// download to open this in DuckDB, what do I download to load it into MySQL
// -- and had to open two folds to learn there was an answer at all.
//
// The two lanes are deliberately NOT symmetrical, and the drawing is what
// says so before any text does. DuckDB takes two files — the data here, and
// the views file — so that lane downloads both. MySQL takes one file that does not exist until you
// pick a moment, so that lane asks for the moment. Dressing them as a
// matched pair would be a lie about the work each one is.
// The .sql build states that owe the reader nothing, and so leave the
// take-away fold closed on arrival. Everything else — including a state this
// build has never heard of — opens it. See backupTakeAway.
const SQL_EXPORT_QUIET = new Set(["idle", "downloaded", "expired"]);
// Every state the daemon documents today (BaselineStatus). Used only to name
// one it does not, which the lane below has no branch to show.
const SQL_EXPORT_KNOWN = new Set(["idle", "running", "succeeded", "failed", "downloaded", "expired"]);

function backupTakeAway(cur, b, sqlSt) {
  const duck = backupDuckLane(b);
  const sql = backupSQLLane(cur, b, sqlSt);
  if (!duck && !sql) return null;
  // FOLDED (#1573). Measured on this page with a real snapshot, these two
  // lanes explaining file formats were most of the words visible at first
  // sight (the e2e prints the live count) — the answer to "what do I download", which is not the question
  // the page is opened with. Folded, the same answer is one click away and
  // the list of copies starts on the first screen.
  //
  // It opens ITSELF whenever the .sql build has anything owed to the reader.
  // A build nobody is watching must never be hidden behind a fold — that is
  // the failure this page exists to prevent, one level down. This is the
  // ONLY place on the page a build's outcome appears, so the rule below is
  // load-bearing rather than cosmetic.
  //
  // Which is why it names the states that keep it CLOSED rather than the
  // ones that open it (an empty state included: that is unknown too). The documented set is closed today — BaselineStatus
  // in baseline_trigger.go lists six, and the lane below has a branch for
  // each — so this guards the day it stops being closed: a state added
  // server-side before the frontend learns it is far likelier to be a new
  // way of failing than a new kind of nothing. An allow-list would hide that
  // one by default. Opening is not enough on its own, though: the lane has
  // no branch for a state it does not know and draws a plain build form, so
  // the panel also names the state below. (What such an addition looks
  // like already exists one endpoint over: `replaced` is a real state on the
  // Storage page's own DTO, produced in consoleapp/sql_export.go, and it can
  // NOT reach this code — do not read the guard as being about that value.)
  // A Set rather than an object literal costs nothing and avoids `state`
  // values like "constructor" answering truthy through the prototype.
  //
  // Beyond the state, two more things open it, and both only when the MySQL
  // lane is actually drawn: that lane is where a build's outcome and its
  // staging problem are shown, so without it there is nothing the fold could
  // be hiding. That gate is not decoration. On the command-line server's
  // entry the lane is never drawn (builds run on registry servers only), and
  // its status endpoint answers 409 — counted, that opened the fold on every
  // visit with a red line about a build that cannot exist there.
  //
  //   staging_error is composed independently of state (the daemon folds in
  //   every orphan it could not delete, and consoleapp/sql_export.go says in
  //   as many words that clearing one never erases the other). So a build
  //   that was downloaded — a quiet state — can carry a red line saying the
  //   daemon cannot clear staged full dumps off its own disk, the disk
  //   capture shares.
  //
  //   An unreadable status is the case we know LEAST about, and hiding it
  //   would invert the reasoning above.
  //
  // "expired" stays quiet, but it is not silent. It means the build is gone
  // without being downloaded (a downloaded build is "downloaded"), by one of
  // two routes in consoleapp/sql_export.go: its deadline passed, or its
  // files were removed from under it (the staging root defaults to the
  // system temp directory, which some hosts clean). The summary line names
  // both, so a reader who started one and came back learns it is gone, and
  // one whose host keeps eating builds is not sent to rebuild into the same
  // hole, without the panel sitting open on every visit until the next
  // build.
  //
  // What the READER opened stays open across the repaints this page does on
  // its own (kept outside the node, like the verify help's). A <details>
  // created with `open` fires one toggle event for that creation, and
  // recording it latched every automatic open as the reader's choice, so the
  // panel stayed open for the rest of the tab. That one event is skipped;
  // every later toggle is recorded. Toggle rather than a click on the
  // summary, because the browser also opens a fold by itself for
  // find-in-page and text-fragment links, and a click listener missed those:
  // the next repaint shut the panel under the reader. A loud state still
  // re-opens a panel the reader closed, since the one thing this fold may
  // never do is hide an outcome.
  const st = sqlSt && sqlSt.sql_export;
  const stErr = sql && sqlSt && sqlSt.error;
  const owed = !!(sql && st && (!SQL_EXPORT_QUIET.has(st.state || "") || st.staging_error)) || !!stErr;
  const panel = el("details", { class: "ov-panel bk-take", open: owed || takeAwayOpen || null });
  const summary = el("summary", { class: "ov-panel-head bk-take-sum" },
    el("h2", { class: "ov-panel-title", text: "Take a copy with you" }));
  if (sql && st && st.state === "expired") {
    summary.append(el("span", { class: "bk-take-note", text: "The last .sql copy is gone: nobody downloaded it before its deadline, or its files were removed." }));
  }
  let creation = !!panel.open;
  panel.addEventListener("toggle", () => {
    if (creation) { creation = false; return; }
    takeAwayOpen = !!panel.open;
  });
  panel.append(summary);
  // Said out loud, and above the lanes it qualifies, rather than rendered as
  // a build form with nothing in it: every state branch in the lane reads
  // `st`, so an unreadable status drew the same thing as "no build has ever
  // run here".
  if (stErr) {
    panel.append(el("p", { class: "form-msg err", text:
      "The state of the .sql build could not be read: " + String(stErr).replace(/[.\s]+$/, "") +
      ". A build may be running or finished that DBTrail cannot show" +
      (sessionMay(PERM_SNAPSHOT_CREATE) ? ", so Build is off until it can be read." : ".") }));
  } else if (sql && st && st.state && !SQL_EXPORT_KNOWN.has(st.state)) {
    panel.append(el("p", { class: "form-msg err", text:
      "The last .sql build reports a state this web interface does not recognise: " + st.state + ". Update DBTrail, or check its log." }));
  }
  const lanes = el("div", { class: "bk-lanes" });
  if (duck) lanes.append(duck);
  if (sql) lanes.append(sql);
  panel.append(lanes);
  return panel;
}

// Whether the reader opened the take-away panel themselves. Outside the node
// for the reason vfyHelpOpen is: the repaints this page does on its own
// replace the <details> that would have held it. See backupTakeAway.
let takeAwayOpen = true;

// backupFilesShape draws the count instead of stating it: on the DuckDB lane
// two tiles when the server can make the views file and one when it cannot,
// one on the MySQL lane. A reader who takes nothing else off
// this panel should still leave knowing that much. Built with el() and CSS
// like duckdbShape(), never svgEl -- that path is for static icon constants.
function backupFilesShape(files) {
  const shape = el("div", { class: "bk-shape" });
  files.forEach((f, i) => {
    if (i) shape.append(el("span", { class: "bk-shape-plus", text: "+" }));
    shape.append(el("div", { class: "bk-file" },
      el("span", { class: "bk-file-n", text: f.name }),
      el("span", { class: "bk-file-c", text: f.cap })));
  });
  return shape;
}

// backupLane DERIVES the count word from the tiles it is handed, so the
// drawing and the sentence cannot disagree. They did: an earlier cut passed
// the lead as a whole string, and gating the views tile below would have left
// one tile under the words "Two files."
//
// "downloads", not "files": a tile is one thing you fetch, and the Parquet
// tile is a whole folder arriving as one .tar.gz. Counting files would be
// wrong by hundreds.
const LANE_COUNT_WORD = ["No", "One", "Two", "Three"];
function backupLane(title, files, tail, ico) {
  const n = files.length;
  const word = (LANE_COUNT_WORD[n] || String(n)) + " download" + (n === 1 ? "" : "s");
  return el("div", { class: "bk-lane" },
    el("div", { class: "stg-card-t" }, ico ? icon(ico, "stg-ico stg-ico-pink") : null, el("h3", { class: "bk-lane-t", text: title })),
    backupFilesShape(files),
    el("p", { class: "bk-lane-lead", text: word + tail }));
}

// The DuckDB lane. Downloading what is already stored asks for no capability,
// so the DATA half is gated on there being a backup and nothing else.
//
// The VIEWS half is a different promise and needs its own gate. The button
// downloads the file (#1573; it used to jump to the card), and the server
// makes it only under capsCache.views: viewsAvailable() is false whenever the
// selected server has archived data turned off (a checkbox on this console's
// own server form), among other reasons. Ungated, this lane drew a views.sql
// tile and a button that only fails. views_api.go puts it plainly: "a button
// that only 404s is a lie, and this codebase already refuses that trade for
// reconstruct and verify." Naming the file from a shared constant pinned its
// NAME; only this pins its EXISTENCE.
function backupDuckLane(b) {
  if (!b || b.error || !b.configured) return null;
  // The lane IS a download of row data, which takes query:execute; the
  // views file beside it is GET /api/views.sql, which takes settings:read.
  if (!sessionMay("query:execute")) return null;
  const snaps = (b.snapshots || []);
  if (!snaps.length) return null;
  const hasViews = !!capsCache.views && sessionMay("settings:read");
  const files = [{ name: "Parquet files", cap: "your tables" }];
  if (hasViews) files.push({ name: DUCKDB_VIEWS_FILE, cap: "how to read them" });
  const lane = backupLane("To open in DuckDB", files, hasViews
    ? ". The data, and the file that tells DuckDB how to read it."
    : ". The data, on its own.", "duck");
  const msg = el("p", { class: "form-msg err" });
  msg.hidden = true;
  const dl = el("button", { class: "btn", type: "button", text: "Download the data" });
  dl.onclick = async () => {
    const err = await downloadNewestBackup(snaps[0].time, dl);
    // The lane may have been repainted while the request was in flight, which
    // leaves msg detached and the error written nowhere. Fall back to the
    // toast the rest of the page already uses for exactly that.
    if (err && !msg.isConnected) { toastError(err); return; }
    msg.textContent = err;
    msg.hidden = !err;
  };
  const go = el("div", { class: "bk-lane-go" }, dl);
  if (hasViews) {
    const views = el("button", { class: "btn btn-ghost", type: "button", text: "Download " + DUCKDB_VIEWS_FILE });
    // A download, not a jump: the card with the options moved to Connect AI
    // (#1573). This saves the default file (no change log, for this machine)
    // and then shows the one command a reader cannot guess. Errors land in
    // the lane's own line, or in a toast when the lane was repainted.
    views.onclick = async () => {
      views.disabled = true;
      try {
        const got = await downloadViewsSQL({});
        msg.hidden = true;
        const older = lane.querySelector(".dk-run");
        if (older) older.remove();
        lane.append(duckdbCommandLine(got.name));
      } catch (err) {
        const text = "Could not make " + DUCKDB_VIEWS_FILE + ": " + ((err && err.message) || err);
        if (!msg.isConnected) { toastError(text); return; }
        msg.textContent = text;
        msg.hidden = false;
      } finally {
        views.disabled = false;
      }
    };
    go.append(views);
  }
  lane.append(go, msg);
  // b.incomplete means at least one configured location would not answer, so
  // the list is a SUBSET and "the newest" is only the newest of what was read.
  lane.append(el("p", { class: "form-hint", text:
    (b.incomplete ? "The newest snapshot that could be read, from " : "The newest snapshot, from ") +
    utcLabel(snaps[0].time) + ". Any other one downloads from its row below." }));
  // Say WHY the second file is missing, and do not dress it as a property of
  // the data. It is withheld by this console's own setting, not by the
  // backup: the same folder still yields the file from the command line. And
  // it is not a convenience -- money columns are stored as text in Parquet,
  // so without it the first SUM a reader writes fails to bind.
  // Only for the CONFIGURATION reason. When views.sql is held back because
  // this session may not read settings, saying "DBTrail is set not to read
  // archived data" is a false statement about the installation.
  if (!capsCache.views) {
    lane.append(el("p", { class: "form-hint", text:
      "The file that describes these tables is not offered, because DBTrail is set not to read archived data. " +
      "DuckDB still opens the Parquet files, but decimal columns arrive as text, so totals will not add up until you cast them." }));
  }
  return lane;
}

// downloadNewestBackup resolves the size before handing off, because
// downloadBackup warns above 1 GB (the browser holds the whole archive in
// memory before the save starts) and that warning is driven by a number the
// LISTING does not carry. Calling it with a zero would drop the warning
// silently, which is the failure a shortcut like this invites.
async function downloadNewestBackup(at, btn) {
  const label = btn.textContent;
  btn.disabled = true;
  btn.textContent = "Checking size…";
  let d;
  try {
    d = await api("/api/baselines/files?at=" + encodeURIComponent(at));
  } catch (err) {
    btn.disabled = false;
    btn.textContent = label;
    return "Could not read that snapshot: " + ((err && err.message) || err);
  }
  btn.disabled = false;
  btn.textContent = label;
  if (d.incomplete) {
    return "The newest snapshot is marked incomplete, so it cannot be downloaded. Open another one in the list below.";
  }
  downloadBackup(at, btn, d.total_bytes || 0);
  return "";
}

// backupSQLLane offers the made-to-measure .sql backup: pick any past
// moment, the console folds the nearest earlier backup forward to it and
// packages the result as plain SQL files. Unlike the point-in-time restore it
// publishes NOTHING: the build lives in a staging directory until it is
// downloaded, until its download deadline passes (the status carries
// expires_at), or until the next build or a daemon restart replaces it
// (#1448). The two terminal states after that, "downloaded" and "expired",
// both mean the same thing to the operator: the file is gone, build again.
// S3-backed backups qualify too (the fold engine reads them directly), which
// is why this lane has no b.kind === "dir" gate.
// backupSnapshotsFor narrows the listing to the snapshots a JOB can actually
// fold from (#1542).
//
// The Backups list merges every location, but each job reads ONE: the restore
// reads this server's S3 backups when it has them, else its directory
// (#1541, the scheduled update's rule), and the .sql export picks the local
// directory whenever the server has one. So a card prefilled with the newest
// row could name a snapshot that exists only in the other location, and the
// job would refuse it while the page above says it is right there. Listing
// more must not mean offering more than the job can do: each lane offers
// what its job reads and says what it left out.
function backupSnapshotsFor(b, kind) {
  return ((b && b.snapshots) || []).filter((s) => !s.kinds || !s.kinds.length || s.kinds.includes(kind));
}

// backupElsewhereNote names the snapshots a caller had to skip, so a shorter
// dropdown than the list above it is explained rather than merely odd. kind is
// what the caller's job reads; the skipped ones are in the other location.
function backupElsewhereNote(b, usable, kind) {
  const all = ((b && b.snapshots) || []).length;
  if (!all || usable.length >= all) return null;
  const n = all - usable.length;
  const one = n === 1;
  return el("p", { class: "form-hint", text: kind === "s3"
    ? n + " snapshot" + (one ? " is" : "s are") + " kept only on this host, not in S3. This builds from this server's S3 snapshots, so it cannot start from " + (one ? "that one" : "those") + "."
    : n + " snapshot" + (one ? " is" : "s are") + " kept only in S3. This builds from the local folder, so it cannot start from " + (one ? "that one" : "those") + "." });
}

function backupSQLLane(cur, b, sqlSt) {
  if (!capsCache.sql_export || !cur || !cur.id || cur.kind !== "registry") return null;
  // Starting a build takes baseline:create; taking the finished file home is
  // a download of row data (query:execute). Neither decides whether the lane
  // exists: GET .../sql-export is servers:read, and how the last build ended
  // (failed, ready, a staging problem on the disk capture shares) is a fact
  // for anyone who may look. Without baseline:create the lane is its outcome
  // alone, and no lane at all when there is none: a status that could not be
  // read, or a state this console does not know, counts as an outcome (see
  // the guard at the end). backupTakeAway's own
  // signals (opening by itself, the unreadable-status line) hang off this
  // lane being drawn, which is one more reason it must not vanish with its
  // button.
  const mayCreate = sessionMay(PERM_SNAPSHOT_CREATE);
  const mayDownload = sessionMay("query:execute");
  const again = (t) => mayCreate ? " " + t : "";
  if (!b || b.error || !b.configured) return null;
  // b.kind, NOT cur.baseline_dir. cur is the RAW registry entry; the export
  // resolves through withBaselineDefaults (#1010), so an entry that inherits
  // the daemon-wide backup location has an empty baseline_dir and still builds
  // from a directory. b.kind comes off the bundle, which applies the same
  // defaulting and the same dir-over-S3 preference the export does, so it is
  // exactly "the build will read a directory".
  //
  // Borrowing the restore card's gate was the mistake: cur.baseline_dir is
  // right THERE, because the restore endpoint refuses the shared daemon store
  // on purpose (the fold would mix servers). This one accepts it, and says so.
  const reads = b.kind === "dir" ? "dir" : "s3";
  const usable = backupSnapshotsFor(b, reads);
  if (!usable.length) return null;
  const st = sqlSt && sqlSt.sql_export;
  const lane = backupLane("To load into MySQL",
    [{ name: ".sql files", cap: "your tables" }],
    mayCreate ? ", built for whatever moment you pick." : ", built for a moment someone picks.", "file");
  const body = el("div", { class: "bk-restore-body" });
  // Running used to return null and take the whole card off the page. In a
  // two-lane panel that leaves a hole where an answer was, so the lane stays
  // and hands off to the run region above instead of vanishing.
  if (st && st.state === "running") {
    body.append(el("p", { class: "form-hint", text: "A build is running. Its progress is above." }));
    lane.append(body);
    return lane;
  }
  if (mayCreate) body.append(el("p", { class: "form-hint", text:
    "Plain SQL in mydumper format. Your database is never touched." }));
  const moment = momentField((usable[0] && usable[0].time) || "");
  const input = moment.input;
  const go = el("button", { class: "btn", type: "button", text: "Build" });
  const msg = el("p", { class: "form-msg err" });
  msg.hidden = true;
  go.onclick = () => startSQLExport(cur.id, input.value.trim(), go, msg);
  // With the status unreadable this lane cannot see a finished build, and a
  // new build REPLACES a finished one: the daemon refuses only while one is
  // running. One click here would delete a ready copy the reader cannot see.
  // backupTakeAway says why the button is off.
  if (sqlSt && sqlSt.error) go.disabled = true;
  if (mayCreate) {
    body.append(el("div", { class: "bk-restore-row" }, moment.wrap, go), msg);
    if (b.kind === "dir") {
      const elsewhere = backupElsewhereNote(b, usable, reads);
      if (elsewhere) body.append(elsewhere);
    }
  }
  if (st && st.state === "failed") {
    body.append(el("p", { class: "form-msg err", text:
      "Last build failed: " + backupFoldError(st.last_error || "unknown error") + " Nothing was built." }));
  } else if (st && st.state === "succeeded" && !mayDownload && !st.removal_owed) {
    // A finished build this session cannot take home: say it exists and
    // when it goes, never "Ready" over a button that is not there.
    body.append(el("p", { class: "form-hint", text:
      "Built for " + utcLabel(st.at || "") + (st.bytes ? " (" + humanBytes(st.bytes) + " on this machine)" : "") + "." +
      (st.expires_at ? " It stays until " + utcLabel(st.expires_at) + ", or until it is downloaded or a new build starts." : "") }));
  } else if (st && st.state === "succeeded") {
    const dl = el("button", { class: "btn", type: "button", text: "Download .sql export (.tar.gz)" });
    dl.onclick = () => downloadSQLExport(cur.id, dl, st.bytes || 0);
    // The lead has to agree with whether the button is there. Adding the
    // explanation below was not enough: this line still opened with "Ready"
    // and the paragraph after it still quoted a download deadline, so a build
    // that cannot be fetched was announced by three sentences, two of which
    // said it could.
    const lead = st.removal_owed
      ? "Built for " + utcLabel(st.at || "") + ", and already being cleaned up."
      : "Ready: every table as of " + utcLabel(st.at || "") +
        (st.bytes ? " (" + humanBytes(st.bytes) + " on this machine)" : "") + ".";
    const row = el("div", { class: "bk-restore-row" },
      el("span", { class: "stg-age", text: lead }));
    // removal_owed means the staged files are owed a removal, so the build is
    // no longer handed out. Withholding the button under a line that still
    // leads with "Ready" left the reader with a deadline for a file they
    // cannot fetch, and the held-download case reaches here with no
    // staging_error to explain it either.
    if (st.removal_owed) {
      row.append(el("span", { class: "form-hint", text: "This build is being cleaned up and can no longer be downloaded." + again("Build again for a fresh copy.") }));
    } else {
      row.append(dl);
    }
    body.append(row);
    if (!st.removal_owed) {
      body.append(el("p", { class: "form-hint", text:
        (st.expires_at ? "The download stays available until " + utcLabel(st.expires_at) + ". " : "") +
        "The file is removed from this machine once you download it, when that time passes, or when a new build starts." }));
    }
  } else if (st && st.state === "downloaded") {
    body.append(el("p", { class: "form-hint", text:
      "Downloaded" + (st.downloaded_at ? " at " + utcLabel(st.downloaded_at) : "") +
      ": the snapshot as of " + utcLabel(st.at || "") + " was handed over and its file was removed from this machine." + again("Build again for another copy.") }));
  } else if (st && st.state === "expired") {
    body.append(el("p", { class: "form-hint", text:
      "The snapshot built for " + utcLabel(st.at || "") + " is no longer on this machine: it was not downloaded before its deadline, or its files were removed." + again("Build again for a fresh copy.") }));
  }
  // The state follows the disk: a removal that failed keeps the build in
  // its previous state and says so here, over a download button that would
  // only answer "not ready".
  if (st && st.staging_error) {
    body.append(el("p", { class: "form-msg err", text:
      "Staging problem: " + st.staging_error + ". DBTrail retries every minute; check the staging directory on the machine it runs on." }));
  }
  // Without baseline:create and nothing to report, there is no lane. An
  // unreadable status and a state this console does not know ARE something
  // to report: backupTakeAway draws those lines, and opens by itself for
  // them, only when this lane exists. A first version of this guard counted
  // only the lines drawn here, and a view-only reader whose status read
  // failed saw a page identical to "no build has ever run".
  const unknownState = !!(st && st.state && !SQL_EXPORT_KNOWN.has(st.state));
  if (!mayCreate && !body.childNodes.length && !(sqlSt && sqlSt.error) && !unknownState) return null;
  const staged = stagedDownloadsNote(snapStorage, cur, snapRegistry);
  if (staged) body.append(staged);
  lane.append(body);
  return lane;
}

async function startSQLExport(id, at, btn, msgEl) {
  msgEl.hidden = true;
  if (!at) { msgEl.textContent = "Enter a UTC time, YYYY-MM-DD HH:MM:SS."; msgEl.hidden = false; return; }
  btn.disabled = true;
  try {
    await api("/api/servers/" + encodeURIComponent(id) + "/sql-export", { method: "POST", body: { at: at } });
  } catch (err) {
    msgEl.textContent = (err && err.message) || String(err);
    msgEl.hidden = false;
    btn.disabled = false;
    return;
  }
  btn.disabled = false;
  toast("Build started: a .sql export as of " + at + " UTC\u2026");
  if (backupsOnScreen()) renderSnapshots();
}

// downloadSQLExport mirrors downloadBackup: fetch + blob because the API
// authenticates via header, with the same over-1-GiB memory confirm (the
// status carries the finished build's byte count).
async function downloadSQLExport(id, btn, totalBytes) {
  if (totalBytes > 1 << 30 &&
      !window.confirm("This snapshot weighs " + humanBytes(totalBytes) +
        ". The browser holds all of it in memory before saving. Download anyway?")) {
    return;
  }
  if (btn) { btn.disabled = true; btn.textContent = "Preparing\u2026"; }
  try {
    const headers = TOKEN ? { Authorization: ["Bearer", TOKEN].join(" ") } : {};
    if (currentServer) headers["X-Bintrail-Server"] = currentServer;
    const res = await fetch("/api/servers/" + encodeURIComponent(id) + "/sql-export/download", { headers });
    if (res.status === 401) { handleUnauthorized(); return; }
    if (!res.ok) {
      let msg = "HTTP " + res.status;
      try { msg = (await res.json()).error || msg; } catch (_) { /* non-JSON error body */ }
      throw new Error(msg);
    }
    const cd = res.headers.get("Content-Disposition") || "";
    const m = /filename="([^"]+)"/.exec(cd);
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = m ? m[1] : "dbtrail-sql-backup.tar.gz";
    document.body.appendChild(a);
    a.click();
    a.remove();
    // Delayed: Firefox has cancelled downloads whose object URL was revoked
    // synchronously after click.
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  } catch (err) {
    toastError("Download failed: " + ((err && err.message) || err));
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "Download .sql export (.tar.gz)"; }
  }
}

// verifyRegions (#677, restructured #1419): trigger/poll/explain the
// recovery-chain verification engine (`bintrail verify`) for the selected
// server. Its own capability gate (verify_trigger, process-global, like
// baseline_trigger) plus a per-server precondition (verify: a baseline is
// configured; verify_live_source: a source DSN is also configured), both
// re-enforced server-side so this gating is UX only.
//
// The three regions of /verification — control,
// current run, history — as separate surfaces. The gating branches (feature
// off, no server) collapse to a single explanatory card, since with nothing
// runnable the other two regions have nothing to hold.
function verifyRegions(servers, opts) {
  const cur = (servers || []).find((s) => s.id === (currentServer || defaultServerId));

  if (!capsCache.verify_trigger) {
    const card = el("section", { class: "tcard vfy-region" },
      el("div", { class: "stg-empty" },
        el("p", { class: "stg-empty-lead", text: "Verification from the web interface is turned off." }),
        el("p", { class: "stg-empty-sub", text:
          "Ask whoever manages this server to turn it on (set BINTRAIL_CONSOLE_VERIFY_TRIGGER=1 and restart). Already on the default setup? Take the current docker-compose.yml beside yours and merge your edits in, because \"docker compose pull\" alone does not add new settings to a file you already have." })));
    return [card];
  }
  if (!cur || !cur.id) {
    // A failed /api/servers arrives here as an empty list, and "Select a
    // server" is then an instruction to do something the operator already did.
    const card = el("section", { class: "tcard vfy-region" },
      el("div", { class: "ev-empty", text: (opts && opts.serversErr)
        ? "Could not load the server list, so verification cannot target one: " + opts.serversErr
        : "Select a server to run verification." }));
    return [card];
  }

  // ── Region 1: what you can run ──
  const control = el("section", { class: "tcard vfy-region vfy-control" });
  // The verdict first (round 3): what the last check found, as an icon and
  // a headline, filled in from the history read below. Then the question,
  // "which check", and the one button. A reader who takes nothing else
  // from this card leaves knowing whether the copy matched.
  const verdict = el("div", { class: "vfy-state none" },
    el("span", { class: "vfy-state-ico" }, icon("check", "vfy-vico")),
    el("div", { class: "vfy-state-w" },
      el("div", { class: "vfy-state-t", text: "Checking…" }),
      el("div", { class: "vfy-state-s", text: "" })));
  // A session that may run a check reads the verdict above the control; a
  // view-only session gets no control card, so the verdict leads the
  // current-run card instead (see the return at the end).
  const mayRun = sessionMay(PERM_SNAPSHOT_CREATE);
  if (mayRun) control.append(verdict);
  control.append(el("div", { class: "vfy-region-head" },
    el("h2", { class: "ov-panel-title", text: "Run a check" })));
  const modeSel = el("select", { class: "select vfy-mode" },
    el("option", { value: "baseline-anchored", text: "Compare two saved snapshots" }));
  if (capsCache.verify_live_source) {
    modeSel.append(el("option", { value: "live-source", text: "Compare against your live database (slower)" }));
  }
  modeSel.append(el("option", { value: "recover-inputs", text: "Check recovery inputs (no snapshot needed)" }));

  const results = el("div", { class: "vfy-results" });
  const btn = el("button", { class: "btn btn-primary vfy-run", type: "button", text: "Run verification" });
  const configured = !!capsCache.verify;
  // The snapshot-comparison modes need a baseline location; the
  // recover-inputs check reads only the index, so it stays runnable on a
  // server with no baseline configured.
  const help = el("p", { class: "form-hint vfy-modehelp" });
  // The mode help is a FOLD since #1573. Open on arrival it was the largest
  // block left on the first screen — an explanation of a check nobody asked
  // for yet, on the screen whose job is "what copies do I have". Closed it
  // costs its four-word summary, and it opens BY ITSELF the moment the reader browses the picker, which
  // is the moment #1418 wrote it for. The text still swaps while closed, so
  // whoever opens it afterwards reads the mode that is selected now.
  const helpFold = el("details", { class: "vfy-helpfold", open: vfyHelpOpen || null },
    el("summary", { class: "form-hint vfy-helpsum", text: "What this check proves" }), help);
  helpFold.addEventListener("toggle", () => { vfyHelpOpen = !!helpFold.open; });
  const updateMode = () => {
    const live = vfyLive.get(cur.id);
    btn.disabled = (!!live && live.state === "running") || (!configured && modeSel.value !== "recover-inputs");
    help.textContent = VFY_MODE_HELP[modeSel.value] || "";
  };
  // Browsing the picker opens the help; vfyHelpOpen carries that (and a
  // later close by hand) across the repaints this page does on its own.
  modeSel.onchange = () => { vfyHelpOpen = true; helpFold.open = true; updateMode(); };
  updateMode();
  btn.onclick = () => createVerify(cur.id, modeSel.value);
  control.append(el("div", { class: "vfy-actions" }, modeSel, btn));
  control.append(helpFold);
  if (!configured) {
    control.append(el("p", { class: "form-hint", text:
      "No snapshot set up for this server yet. The two snapshot modes need one" +
      (sessionMayConfigureServer() ? " (set one under Where and how often, then create at least two snapshots)" : "") +
      ". \"Check recovery inputs\" works without one: it only reads the index." }));
  }

  // ── Region 2: what is running or just ran ──
  const current = el("section", { class: "tcard vfy-region vfy-current" });
  if (!mayRun) current.append(verdict);
  current.append(el("div", { class: "vfy-region-head" },
    el("h2", { class: "ov-panel-title", text: "Current run" })));
  vfyView = { id: cur.id, results, btn, updateMode };
  vfyDraw(vfyView);
  vfyProbe(cur.id);
  current.append(results);
  // The glossary of the per-row nouns (#1419 §5) moved to this repo's
  // docs/console.md, Verification (#1573 redesign). The page's Docs link does
  // NOT reach it: it opens the site's guides/verify page, which lives in
  // another repo and is given the same four definitions in the docs pass.

  // ── Region 3: what ran before ──
  const historyCard = el("section", { class: "tcard vfy-region vfy-histcard" });
  historyCard.append(el("div", { class: "vfy-region-head" },
    el("h2", { class: "ov-panel-title", text: "Past checks" }),
    tzChip()));
  const history = el("div", { class: "vfy-history" });
  historyCard.append(history);
  loadVerifyHistory(cur.id, history, verdict);

  // Running a check takes baseline:create. Without it the region that
  // offers one is left out; what is running and what ran before stay, since
  // reading them takes only servers:read.
  return mayRun ? [control, current, historyCard] : [current, historyCard];
}

// VFY_MODE_HELP (#1418): what each mode proves, what it needs, what it costs
// — one compressed sentence set per mode, swapped under the select as the
// operator browses. Source of truth for the long form is the issue; keep
// these three claims per entry: proof, prerequisite, cost.
const VFY_MODE_HELP = {
  "baseline-anchored": "For each table, takes the last snapshot that read it from your database and the snapshot before that one, replays the recorded changes from the older one forward, and compares the result with what your database held at that read. A table whose last read is no longer kept, or has no snapshot before it, is reported as not checked. Never touches your database.",
  "live-source": "Rebuilds each table from a snapshot plus the recorded changes, then compares it row by row against the real table. The strongest content check, and the only one that reads your database: it takes time, adds load, and needs a quiet table, because writes that land during the scan show up as mismatches. Run it outside busy hours.",
  "recover-inputs": "Reads the index's own record of each change and checks that every row's history holds together from one change to the next. This is the data an undo script is built from. Needs no snapshot and never touches your database.",
};

// A verification run's state lives here, per server, and never in the box
// that was on screen when it started. The page repaints (Back to it, a server
// switch, and since the backup pages merged, any job that finishes); a run that
// wrote into the box it saved at the start kept writing off screen while the
// new box said "No run yet" and offered another run, and the button it
// re-enabled at the end was the detached one. A run the schedule started
// (--verify-interval) had no box at all.
//   vfyLive      the newest status this session read for each server: a run
//                going, or the one that just ended.
//   vfyFollowing the servers with a poll loop, so a click and a repaint never
//                start two (two loops would draw every tick twice and say
//                the ending twice).
//   vfyView      the box and button on screen now, and whose they are.
//   vfyAnnounce  the servers whose run this tab started: only those end with
//                a message, as before; a run the schedule started is shown
//                on the page but does not pop a message on whatever page
//                the operator is on.
// Sign-out clears them (clearAuthState): a status can list tables the next
// session's profile withholds, and the server refuses that session the read.
// It also bumps vfyEpoch, and every answer is checked against the epoch its
// request was sent in: a status that was in flight when one session signed
// out lands after the clear, and without the check it would write that
// session's run back for the next one to see. vfyFollowing maps a server to
// its loop's token, so an old session's loop ending cannot unmark the new
// session's loop for the same server.
const vfyLive = new Map();
const vfyFollowing = new Map();
const vfyAnnounce = new Set();
let vfyView = null;
let vfyEpoch = 0;
// Whether the mode help is open, kept out of the box for the same reason the
// run state is: this page repaints on its own (a job settling, a server
// switch, a page of the list), and a fold whose state lived in the node it
// replaces would close under a reader mid-sentence. One flag, not one per
// server — it is a reading preference, not a fact about a server.
let vfyHelpOpen = false;

// vfyDraw draws a server's state into a view: the box, and the button busy
// while a run goes or back to what the chosen mode allows.
function vfyDraw(view, opts) {
  const st = vfyLive.get(view.id) || null;
  renderVerifyResults(view.results, st, view.id, opts);
  if (st && st.state === "running") {
    view.btn.disabled = true;
    view.btn.textContent = "Running…";
  } else {
    view.btn.textContent = "Run verification";
    view.updateMode();
  }
}

// vfyShow draws a server's state into the view on screen, if that view is
// the server's and still attached; a run on another server draws nothing.
function vfyShow(id, opts) {
  const view = vfyView;
  if (view && view.id === id && view.results.isConnected) vfyDraw(view, opts);
}

// vfyProbe asks, once per paint, what the server holds for this server's
// verification, and decides whether that is newer than what the page shows:
//   - a run going (one the schedule started, or one this tab started before
//     the page repainted): show it and follow it; a loop already following
//     it absorbs the second follow;
//   - a finished run other than the one shown (a scheduled run, another tab):
//     show it, so an older green run never sits over a newer mismatch;
//   - nothing held (the daemon restarted): a run the page last saw going is
//     over; a run that ended stays;
//   - a page that has seen nothing keeps "No run yet".
// An answer that lands after a newer one (a tick of the poll loop, or its
// end) is dropped: without that, a slow probe could bring RUNNING back over a
// finished run and start a second loop. A 403 means the server will not show
// this session the status, so the page drops what it holds too.
async function vfyProbe(id) {
  const before = vfyLive.get(id), epoch = vfyEpoch;
  let st;
  try {
    st = (await api("/api/servers/" + encodeURIComponent(id) + "/verify")).verify;
  } catch (err) {
    if (vfyEpoch !== epoch) return;
    if (err && err.status === 403) {
      if (vfyLive.get(id) === before && before) { vfyLive.delete(id); vfyShow(id); }
      return;
    }
    // 404: the server is gone. 409: the command-line server, which monitor
    // verbs do not apply to. Neither is a failure to report.
    if (!(err && (err.status === 404 || err.status === 409))) console.warn("could not read the verification status", err);
    return;
  }
  if (vfyEpoch !== epoch || !st || vfyLive.get(id) !== before) return;
  if (st.state === "running") {
    vfyLive.set(id, st);
    vfyShow(id);
    followVerify(id);
    return;
  }
  if (st.state === "idle") {
    if (before && before.state === "running") { vfyLive.delete(id); vfyShow(id); }
    return;
  }
  if (!before) return;
  if (before.state !== "running" && before.since === st.since && before.finished_at === st.finished_at) return;
  vfyLive.set(id, st);
  vfyShow(id);
}

// followVerify polls a server's run until it ends and is the one owner of
// what that run puts on screen: each tick draws into the view on screen at
// that moment, and the ending (highlight, history, message) happens once.
async function followVerify(id) {
  if (vfyFollowing.has(id)) return;
  const epoch = vfyEpoch, token = {};
  const alive = () => vfyEpoch === epoch;
  vfyFollowing.set(id, token);
  let done;
  try {
    done = await pollVerify(id, (st) => { if (alive()) { vfyLive.set(id, st); vfyShow(id); } }, alive);
  } finally {
    if (vfyFollowing.get(id) === token) vfyFollowing.delete(id);
  }
  // Signed out meanwhile: this run belongs to the previous session.
  if (!alive()) return;
  // The poll gives up after ~20 minutes. While the page shows this server,
  // follow on, or the box would freeze on RUNNING with the button disabled
  // after the run ends; off screen, stop, and the next paint's probe picks
  // the run up again.
  if (!done && vfyView && vfyView.id === id && vfyView.results.isConnected) {
    followVerify(id);
    return;
  }
  // justFinished: the running→done transition gets a one-shot highlight so
  // completion is perceptible off-chip (#1420); the toast below is the other
  // half for an operator who looked away.
  const signal = vfyFinishSignal(done);
  if (done) vfyLive.set(id, done);
  vfyShow(id, { justFinished: !!done && signal.flash });
  // The finished run is now in the persisted history too: refresh the list
  // on screen, if it is this server's.
  const histBox = document.querySelector(".vfy-history");
  if (histBox && vfyView && vfyView.id === id && histBox.isConnected) loadVerifyHistory(id, histBox);
  if (vfyAnnounce.delete(id)) (signal.sticky ? toastError : toast)(signal.message);
}

// createVerify starts an in-process verify run on the daemon for a server and
// hands it to followVerify, which draws results "as they land" (#677): the
// engine has no progress callback, so the console's own poll loop is the only
// source of incremental updates.
async function createVerify(id, mode) {
  const view = vfyView, epoch = vfyEpoch;
  if (view && view.id === id) { view.btn.disabled = true; view.btn.textContent = "Running…"; }
  let status;
  try {
    status = (await api("/api/servers/" + encodeURIComponent(id) + "/verify", { method: "POST", body: { mode } })).verify;
  } catch (err) {
    if (vfyEpoch !== epoch) return;
    // A 409 is a run already going (the schedule started one, or a second
    // click) OR a server this page cannot run checks on (the command-line
    // server refuses monitor verbs with its own reason). Ask which before
    // saying anything.
    if (err && err.status === 409) {
      let st = null;
      try { st = (await api("/api/servers/" + encodeURIComponent(id) + "/verify")).verify; } catch (_) { st = null; }
      if (vfyEpoch !== epoch) return;
      if (st && st.state === "running") {
        toast("A verification is already running on this server. Showing it.");
        vfyAnnounce.add(id);
        vfyLive.set(id, st);
        vfyShow(id);
        await followVerify(id);
        return;
      }
    }
    toastError("Verify failed: " + ((err && err.message) || err));
    vfyShow(id);
    return;
  }
  if (vfyEpoch !== epoch) return;
  vfyLive.set(id, status);
  vfyShow(id);
  toast("Verification started…");
  vfyAnnounce.add(id);
  await followVerify(id);
}

// vfyFinishSignal is how a run's end reaches an operator who looked away, by
// its verdict: only a verified run gets the green flash, and a run that found
// a difference, hit errors or proved nothing gets a message that stays until
// dismissed. "succeeded" only says the run reached its end; a green flash and
// a toast gone in two seconds over a mismatch said the opposite of the chip.
function vfyFinishSignal(done) {
  if (!done) return { flash: false, sticky: false, message: "Verification is still running. Check back shortly." };
  if (done.state !== "succeeded") {
    return { flash: false, sticky: true, message: "Verification failed: " + (done.last_error || "unknown error") };
  }
  if (done.verdict === "verified") {
    return { flash: true, sticky: false, message: "Verification complete: " + vfySummaryText(done.summary || {}) };
  }
  if (done.verdict === "no_predecessor") {
    return { flash: false, sticky: false, message: done.note || "Only one snapshot so far, nothing to compare yet." };
  }
  return { flash: false, sticky: true, message: "Check finished, " + vfyHeadline(done) };
}

// pollVerify polls the per-server verify status until it leaves "running" (or
// a ~20-minute cap), invoking onTick after every poll so the caller can
// re-render mid-run progress. Returns the terminal status, or null if it
// never settled within the cap. Transient poll errors are ignored and retried.
async function pollVerify(id, onTick, alive) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  for (let i = 0; i < 600; i++) {
    await sleep(2000);
    if (alive && !alive()) return null;
    let st;
    try {
      st = (await api("/api/servers/" + encodeURIComponent(id) + "/verify")).verify;
    } catch (err) {
      // 403/404 are durable (the feature got disabled, an RBAC profile came
      // on, or the server was deleted mid-run) — the run this poll is
      // watching is gone or unreachable, so retrying for the full ~20-minute
      // cap would just leave the button stuck on "Running…". Everything else
      // (network blip, 5xx) is presumed transient and retried. 401 is already
      // handled centrally by api()'s sign-in gate.
      if (err && (err.status === 403 || err.status === 404)) {
        return { state: "failed", last_error: (err && err.message) || String(err) };
      }
      continue;
    }
    if (st && onTick) onTick(st);
    if (st && st.state !== "running") return st;
  }
  return null;
}

const VFY_STATUS_CLASS = { match: "pass", mismatch: "fail", error: "fail", inconclusive: "warn" };

// The benign inconclusive kinds (#1416): quiet or append-only tables, where
// zero assertions is the expected and PERMANENT outcome — not a finding. They
// render neutral, never amber: 20 warning-coloured rows on a healthy server is
// how operators learn to stop reading this page. The mapping is by KIND, not by
// status — an inconclusive with no kind (content modes, older runs) stays
// amber, because defaulting the unknown to benign is the direction a verify
// surface must never round.
const VFY_BENIGN_KINDS = { "no-activity": true, "nothing-to-assert": true };
function vfyCardClass(r) {
  if (r.status === "inconclusive" && VFY_BENIGN_KINDS[r.inconclusive_kind]) return "note";
  return VFY_STATUS_CLASS[r.status] || "fail";
}
const VFY_STATUS_MARK = { pass: "✓", fail: "✗", warn: "!", note: "–" };
// vfySummaryText renders the summary counts, splitting the inconclusive
// bucket when the split exists (#1416): "20 inconclusive" was unreadable when
// 18 were quiet or append-only tables. The attention-worthy number is the
// REMAINDER, so an unclassified inconclusive lands on the attention side.
function vfySummaryText(s) {
  let inc = s.inconclusive + " inconclusive";
  if (s.inconclusive_nothing_to_check > 0) {
    inc = s.inconclusive + " inconclusive (" + s.inconclusive_nothing_to_check
      + " nothing to check · " + (s.inconclusive - s.inconclusive_nothing_to_check) + " unproven)";
  }
  return s.match + " match · " + s.mismatch + " mismatch · " + inc + " · " + s.error + " error";
}

// vfyVerdictSentence answers "is my restore sound?" in words. It never claims
// more than was proven: benign inconclusives are named as not-applicable, and
// any unproven remainder is called out rather than absorbed.
function vfyVerdictSentence(s) {
  const unproven = s.inconclusive - (s.inconclusive_nothing_to_check || 0);
  if (s.mismatch > 0) return s.mismatch + " table(s) failed. Read those tables first.";
  if (s.error > 0) return "Errors stopped the check on " + s.error + " table(s).";
  const parts = [];
  if (s.match > 0) parts.push(s.match + " table(s) checked out clean");
  if (s.inconclusive_nothing_to_check > 0) parts.push(s.inconclusive_nothing_to_check + " had nothing to check (no changes in the window, or only new rows; that is normal)");
  if (unproven > 0) parts.push("the check could not prove " + unproven + " table(s); worth a look");
  if (!parts.length) return "Nothing was verified.";
  return parts.join("; ") + ".";
}

// vfyHeadline says what one finished run proved, in counts, by the verdict
// the server computed with the rule `bintrail verify` exits on (the record's
// verdict field). Never a lone "verified": a run whose tables all came back
// not checked says so, and one that proved some says how many were not.
function vfyHeadline(rec) {
  if (rec.state === "failed") return "failed: " + (rec.last_error || "unknown error");
  const s = rec.summary || {};
  const benign = s.inconclusive_nothing_to_check || 0;
  const notChecked = (s.inconclusive || 0) - benign;
  switch (rec.verdict) {
    case "no_predecessor":
      return "only one snapshot so far, nothing to compare yet";
    case "mismatch":
    case "error":
      return [s.match + " match", s.mismatch + " mismatch", s.error + " error"]
        .concat(notChecked > 0 ? [notChecked + " not checked"] : []).join(" · ");
    case "differs": {
      // Tables that differ from a snapshot read with no locks (#1380): a
      // difference was found, so the run failed; the rest are said apart.
      const differs = s.inconclusive_differs || 0;
      const other = notChecked - differs;
      return [s.match + " match", differs + (differs === 1 ? " differs" : " differ") + " from a snapshot with different points-in-time"]
        .concat(other > 0 ? [other + " not checked"] : []).join(" · ");
    }
  }
  const parts = [];
  if (s.match > 0) parts.push(s.match + " match");
  if (notChecked > 0) parts.push(notChecked + " not checked");
  if (benign > 0) parts.push(benign + " nothing to check");
  if (!parts.length) parts.push("no table was compared");
  return (rec.verdict === "unproven" ? "nothing proven: " : "") + parts.join(" · ");
}

const VFY_MODE_LABEL = { "baseline-anchored": "compared two saved snapshots", "live-source": "compared against the live database", "recover-inputs": "checked recovery inputs in the index" };

// loadVerifyHistory renders the persisted run history into box: a "LAST
// CHECK" headline saying what the newest run proved, plus the most recent runs (newest first; the server
// stores up to 20 per server, this list shows up to 8). Manual runs,
// scheduled runs and scheduled skips all appear — the daemon's
// --verify-interval loop writes the same store. On a fetch error (including
// the 403 feature-off case) the box keeps whatever it already shows; the
// trigger UI above explains how to enable verification.
// vfyTally draws a past run's counts as chips of the status family (#1950):
// only the verdicts that happened, so the row fits one line and a failure is
// the one red thing on it. The full counts stay in the tooltip.
function vfyTally(r) {
  const s = r.summary || {};
  const n = (x) => Number(x || 0);
  const unproven = n(s.inconclusive) - n(s.inconclusive_nothing_to_check);
  const box = el("span", { class: "vfy-tally", title: vfySummaryText(Object.assign({ match: 0, mismatch: 0, inconclusive: 0, error: 0 }, s)) });
  let drawn = 0;
  [[n(s.match), "ok", "match"], [n(s.mismatch), "fail", "mismatch"],
    [n(s.inconclusive), unproven > 0 ? "warn" : "unknown", "inconclusive"], [n(s.error), "error", "error"]].forEach(([count, tone, word]) => {
    if (!count) return;
    drawn++;
    box.append(el("span", { class: "chip chip-sm chip-" + tone, text: count + " " + word }));
  });
  if (!drawn) box.append(el("span", { class: "stg-age", text: r.verdict === "no_predecessor" ? "nothing to compare" : "no table was compared" }));
  return box;
}

async function loadVerifyHistory(id, box, verdictTile) {
  // The two tiles that say the verdict: the Checks card's (handed in on
  // paint, else the one on screen: a run that just ended refreshes the
  // history from followVerify) and the hero's, so they never disagree.
  const tile = verdictTile || document.querySelector(".snap-checks .vfy-state");
  const heroTile = document.querySelector(".snap-hero .hero-check");
  let recs;
  try {
    recs = (await api("/api/servers/" + encodeURIComponent(id) + "/verify/history")).history || [];
  } catch (err) {
    vfyVerdictFill(tile, null, "unreadable");
    snapshotCheckTileFill(heroTile, null, "unreadable");
    return;
  }
  clear(box);
  if (!recs.length) {
    box.append(el("div", { class: "ev-empty", text: "No past runs yet." }));
    vfyVerdictFill(tile, null, "none");
    snapshotCheckTileFill(heroTile, null, "none");
    return;
  }
  const latest = recs.find((r) => r.state === "succeeded" || r.state === "failed");
  vfyVerdictFill(tile, latest || null, "none");
  snapshotCheckTileFill(heroTile, latest || null, "none");
  if (latest && latest.finished_at) {
    const sec = (Date.now() - Date.parse(latest.finished_at)) / 1000;
    // chip-age, NOT chip-mon and NOT the live treatment: this is a staleness
    // age, and it used to wear the same amber as RUNNING (#1420) — a live
    // state and an old fact were indistinguishable at a glance. LAST CHECK,
    // not LAST VERIFIED: it headed runs that proved nothing too, while the
    // CLI, the webhook and the metric all called them unproven. What the
    // run proved is the text beside it (vfyHeadline), by the server's verdict.
    box.append(el("div", { class: "vfy-summary" },
      el("span", { class: "chip chip-age", text: "LAST CHECK " + agoText(sec) }),
      el("span", { class: "stg-age", text: vfyHeadline(latest) })));
  }
  recs.slice(0, 8).forEach((r, i) => {
    let outcome = "";
    if (r.state === "skipped") outcome = "skipped: " + (r.skip_reason || "");
    else if (r.state === "failed") outcome = "failed: " + (r.last_error || "unknown error");
    const when = utcLabel(r.finished_at || r.since || "");
    // Expandable (#1417): the per-table detail is ALREADY in this record —
    // VerifyRunRecord embeds VerifyStatus, results included — the old
    // renderer just dropped it on the floor. Disclosure, not navigation:
    // the history is short and comparing runs side by side is the point.
    const detailID = "vfy-hist-" + i;
    const mode = (VFY_MODE_LABEL[r.mode] || r.mode || "") + (r.trigger === "scheduled" ? " (scheduled)" : "");
    const row = el("button", { class: "stg-row vfy-histrow", type: "button", "aria-expanded": "false", "aria-controls": detailID });
    // A mark before the date says the outcome without reading the counts:
    // a tick, a cross, or a dash for a run that proved nothing or was
    // skipped.
    const mark = r.state === "failed" || r.verdict === "mismatch" || r.verdict === "error" || r.verdict === "differs" ? "bad"
      : r.state === "succeeded" && r.verdict === "verified" ? "ok" : "none";
    row.append(
      el("span", { class: "vfy-hmark " + mark, text: mark === "ok" ? "\u2713" : mark === "bad" ? "\u2715" : "\u2013" }),
      icon("caret", "ev-caret"),
      el("span", { class: "stg-name mono", text: when }),
      el("span", { class: "vfy-hmode", text: mode, title: mode }),
      outcome ? el("span", { class: "stg-age vfy-hout", text: outcome, title: outcome }) : vfyTally(r));
    const detail = el("div", { class: "vfy-histdetail", id: detailID, hidden: "" });
    let rendered = false;
    row.onclick = () => {
      const open = row.classList.toggle("open");
      row.setAttribute("aria-expanded", open ? "true" : "false");
      detail.hidden = !open;
      if (open && !rendered) {
        rendered = true;
        if ((r.results || []).length) {
          renderVerifyResults(detail, r, id, { history: true });
        } else {
          detail.append(el("div", { class: "ev-empty", text: "This run recorded no per-table detail" +
            (r.state === "skipped" ? "; it was skipped before any table was checked." : ".") }));
        }
      }
    };
    box.append(row, detail);
  });
}

// vfyVerdictWords is what one finished run means, for the two tiles that
// show it (the Checks card and the hero): a state, a headline, the counts
// and when, and the mark. Never more than was proven: a run whose tables
// all came back not checked is "Nothing proven", never a tick.
function vfyVerdictWords(latest, whyNone) {
  if (!latest || !latest.finished_at) {
    return { state: "none", title: whyNone === "unreadable" ? "Past checks could not be read" : "Never checked", line: whyNone === "unreadable" ? "" : "Run one below.", when: "", mark: "check" };
  }
  const s = latest.summary || {};
  const when = agoText((Date.now() - Date.parse(latest.finished_at)) / 1000);
  const n = (x) => x || 0;
  if (latest.state === "failed") return { state: "bad", title: "The check failed", line: latest.last_error || "unknown error", when: when, mark: "cross" };
  switch (latest.verdict) {
    case "verified":
      return { state: "ok", title: "The copy matches", line: n(s.match) + " of " + (n(s.match) + n(s.inconclusive) + n(s.error)) + " tables", when: when, mark: "check" };
    case "mismatch":
      return { state: "bad", title: n(s.mismatch) + (s.mismatch === 1 ? " table differs" : " tables differ"), line: n(s.match) + " match", when: when, mark: "cross" };
    case "error":
      return { state: "bad", title: "Errors on " + n(s.error) + (s.error === 1 ? " table" : " tables"), line: n(s.match) + " match", when: when, mark: "cross" };
    case "differs":
      return { state: "bad", title: n(s.inconclusive_differs) + (s.inconclusive_differs === 1 ? " table differs" : " tables differ"), line: "from a snapshot with different points-in-time · " + n(s.match) + " match", when: when, mark: "cross" };
    case "no_predecessor":
      return { state: "none", title: "Nothing to compare yet", line: "Only one snapshot so far", when: when, mark: "check" };
  }
  return { state: "warn", title: "Nothing proven", line: vfyHeadline(latest), when: when, mark: "check" };
}

// vfyVerdictFill paints the Checks card's verdict tile from vfyVerdictWords.
function vfyVerdictFill(tile, latest, whyNone) {
  if (!tile) return;
  const t = tile.querySelector(".vfy-state-t"), sub = tile.querySelector(".vfy-state-s"), ico = tile.querySelector(".vfy-state-ico");
  if (!t || !sub || !ico) return;
  const w = vfyVerdictWords(latest, whyNone);
  tile.className = "vfy-state " + w.state;
  t.textContent = w.title;
  sub.textContent = w.line + (w.when ? (w.line ? " · " : "") + "checked " + w.when : "");
  clear(ico);
  ico.append(icon(w.mark, "vfy-vico"));
}

// vfySortResults: worst verdict first (#1419 §3) — a mismatch must not sit
// at alphabetical position 31 below the fold, visually identical to the 46
// clean rows around it. Within a band the incoming order is kept
// (stable sort) — alphabetical in practice, because both result enumerators
// sort; the ORDER BY is theirs, not this function's. Display-only: the wire order is the
// engine's completion order and the summary counts are order-free.
const VFY_SORT_BAND = { warn: 2, note: 3, pass: 4 }; // fail band ranks 0/1 inline (mismatch first)
function vfySortResults(results) {
  return results.map((r, i) => [r, i]).sort((a, b) => {
    const ba = vfyCardClass(a[0]), bb = vfyCardClass(b[0]);
    // error shares the "fail" class with mismatch; keep mismatch first.
    const rank = (r, band) => band === "fail" ? (r.status === "mismatch" ? 0 : 1) : VFY_SORT_BAND[band];
    const d = rank(a[0], ba) - rank(b[0], bb);
    return d !== 0 ? d : a[1] - b[1];
  }).map((p) => p[0]);
}

// vfyComparedToLine says which read of the database the compared tables were
// checked against, as the CLI's text report does: the newest snapshot can be
// days newer than that read, so a "match" alone reads as "the newest snapshot
// is verified". "" when no table was compared.
function vfyComparedToLine(results) {
  const times = [];
  (results || []).forEach((r) => {
    if (r.compared_to && !times.includes(r.compared_to)) times.push(r.compared_to);
  });
  if (!times.length) return "";
  if (times.length === 1) return "Compared against the last read of your database, at " + utcLabel(times[0]) + ".";
  return "Compared against each table's last read of your database, at " + times.length + " different times. Hover a table to see its read.";
}

// vfyCountsText: the per-table counters as a compact fixed column (#1419 §2).
// The wire carries them only for recover-inputs rows (toWireResult copies the
// walk's counters; the content modes never set them) — review caught the
// first cut reading keys the DTO did not carry at all, rendering the column
// permanently blank.
function vfyCountsText(r) {
  if (r.events_checked === undefined && r.chains_checked === undefined) return "";
  const n = (v) => Number(v || 0).toLocaleString("en-US");
  return n(r.events_checked) + " changes · " + n(r.chains_checked) + " rows";
}

// vfyVerdictDrawing draws what a finished check found as the four verdicts a
// table can get, each with how many tables got it (#1950): the same four the
// docs draw (match, mismatch, inconclusive, error). It replaced the counts
// line and the verdict sentence, and carries that sentence as its text
// alternative, so a screen reader hears the answer the drawing shows. A
// verdict nobody got is drawn dim, so the eye lands on the ones that
// happened; the inconclusive tile is a warning only for the part the check
// could not prove, never for the tables that had nothing to check.
function vfyVerdictDrawing(s) {
  s = s || {};
  const n = (x) => Number(x || 0);
  const benign = n(s.inconclusive_nothing_to_check);
  const unproven = n(s.inconclusive) - benign;
  const incNote = benign && unproven > 0 ? benign + " nothing to check, " + unproven + " not proven"
    : benign ? "nothing to check, that is normal"
    : unproven > 0 ? "not proven, worth a look" : "not proven either way";
  const tiles = [
    ["match", n(s.match), "ok", "Match", "proven"],
    ["mismatch", n(s.mismatch), "bad", "Mismatch", n(s.mismatch) ? "read these tables first" : "would not restore"],
    ["inconclusive", n(s.inconclusive), unproven > 0 ? "warn" : "none", "Inconclusive", incNote],
    ["error", n(s.error), "bad", "Error", "the check failed"],
  ];
  const fig = el("div", { class: "vfy-verdicts", role: "img", "aria-label": vfyVerdictSentence(Object.assign({ match: 0, mismatch: 0, inconclusive: 0, error: 0 }, s)) });
  tiles.forEach(([key, count, tone, label, note]) => {
    fig.append(el("div", { class: "vfy-vt vfy-vt-" + key + " " + (count ? tone : "zero") },
      el("span", { class: "vfy-vt-n", text: String(count) }),
      el("span", { class: "vfy-vt-k", text: label }),
      el("span", { class: "vfy-vt-c", text: note })));
  });
  return fig;
}

// renderVerifyResults draws one run's summary + per-table rows into
// container. Used by the live poll loop (results appear as they land) AND by
// an expanded history record (#1417) — a VerifyRunRecord embeds VerifyStatus,
// so the shapes are compatible by construction.
//
// opts.history: rendering a PAST run — no Explain buttons (a new verify run
// discards the previous run's drill-down artifacts, so the button would 404
// or answer about a different run), and no "no run yet" placeholder. Also
// DERIVED from the record itself: every persisted VerifyRunRecord carries
// `trigger` ("manual"/"scheduled", no omitempty) and the live VerifyStatus
// never does, so a call site that forgets the option cannot resurrect the
// dead buttons — deriving beats threading, and the fixture cannot red-check
// the threaded half (recover-inputs rows are never explainable).
// opts.justFinished: the completion transition (#1420) — a one-shot highlight
// on the summary row so the running→done change is perceptible to someone
// not staring at the chip.
function renderVerifyResults(container, status, id, opts) {
  clear(container);
  const history = (opts && opts.history) || (status && status.trigger !== undefined);
  if (!status || status.state === "idle") {
    if (!history) {
      container.append(el("div", { class: "ev-empty", text: "No run yet. Results land here, table by table." }));
    }
    return;
  }
  const running = status.state === "running";
  // RUNNING is a live state and gets a live treatment — animated, distinct
  // from every age/staleness chip on the page (#1420): the old amber
  // chip-mon was also the LAST VERIFIED treatment, so a glance could not
  // tell "in flight" from "13h old".
  // A finished run wears its VERDICT, not its state: "succeeded" only means
  // it ran to the end, and a green DONE over a run that proved no table, or
  // found a difference, told the operator the opposite of the rows below.
  // The verdict is the server's, the rule `bintrail verify` exits on; no
  // second rule here, so a run whose tables all had nothing to check is
  // NOTHING PROVEN on this page as it is a non-zero exit there. Green only for "verified": a verdict this page does
  // not know is a neutral FINISHED, never a pass.
  const VERDICT_CHIP = {
    verified: ["chip chip-done", "DONE"],
    mismatch: ["chip chip-fail", "MISMATCH"],
    error: ["chip chip-fail", "ERRORS"],
    differs: ["chip chip-fail", "DIFFERS"],
    unproven: ["chip chip-fail", "NOTHING PROVEN"],
    no_predecessor: ["chip chip-age", "NOTHING TO COMPARE"],
  };
  const byVerdict = status.state === "succeeded" && (VERDICT_CHIP[status.verdict] || ["chip chip-age", "FINISHED"]);
  const chipCls = byVerdict ? byVerdict[0] : ({ running: "chip chip-live", succeeded: "chip chip-done", failed: "chip chip-fail" }[status.state] || "chip chip-mon");
  const stateLabel = byVerdict ? byVerdict[1] : ({ running: "RUNNING", succeeded: "DONE", failed: "FAILED" }[status.state] || status.state.toUpperCase());
  const summaryRow = el("div", { class: "vfy-summary" + ((opts && opts.justFinished) ? " vfy-flash" : "") },
    el("span", { class: chipCls, text: stateLabel }));
  if (status.mode) summaryRow.append(el("span", { class: "stg-age", text: VFY_MODE_LABEL[status.mode] || status.mode }));
  const s = status.summary || {};
  const done = (status.results || []).length;
  if (running) {
    // Progress, not a tally (#1420): the engine has no planned-total, so the
    // honest number is tables completed so far — framed as progress, because
    // a partial count read as final says "20 inconclusive" about a run that
    // has not finished (sharpest for inconclusive, #1416).
    summaryRow.append(el("span", { class: "stg-age", text: done + " table(s) checked so far" }));
    if (done) summaryRow.append(el("span", { class: "stg-age vfy-sofar", text: vfySummaryText(s) + " (so far)" }));
  } else if (done && status.state !== "succeeded") {
    // A run that failed part-way keeps its tally in words; a finished one
    // draws it (below).
    summaryRow.append(el("span", { class: "stg-age", text: vfySummaryText(s) }));
  }
  container.append(summaryRow);
  if (running) {
    // Motion (#1420): the page must look like a page doing work. The strip is
    // CSS-animated behind prefers-reduced-motion, like every other motion here.
    container.append(el("div", { class: "vfy-progress" }, el("span", { class: "vfy-progress-bar" })));
  }
  // The verdict (#1416), drawn since #1950: the answer to the operator's
  // question at a glance, so it does not have to be derived from 28 rows.
  // Only on a FINISHED run: a partial tally must not be read as a verdict
  // (#1420).
  if (status.state === "succeeded") container.append(vfyVerdictDrawing(s));
  const comparedTo = vfyComparedToLine(status.results);
  if (comparedTo) container.append(el("p", { class: "form-hint vfy-compared-to", text: comparedTo }));
  if (status.note) container.append(el("p", { class: "form-hint", text: status.note }));
  if (status.last_error) container.append(el("p", { class: "form-msg err", text: status.last_error }));

  // Structured rows (#1419 §2), worst first (§3): table, verdict and counts
  // are columns the eye can scan; the detail sentence is the LAST, flexible
  // column, ellipsized with the full text a click (or hover title) away.
  const rows = el("div", { class: "vfy-rows" });
  vfySortResults(status.results || []).forEach((r) => {
    // Statuses are normalized server-side (verify.NormalizeStatus), so only
    // the four keys above can arrive; if that ever breaks, fail — never reassure.
    const cls = vfyCardClass(r);
    const row = el("div", { class: "vfy-row " + cls });
    row.append(el("span", { class: "vfy-mark", text: VFY_STATUS_MARK[cls] || "?" }));
    row.append(el("span", { class: "vfy-tbl", text: r.schema + "." + r.table,
      title: r.compared_to ? "Compared against the read of " + utcLabel(r.compared_to) : null }));
    const verdict = r.status === "inconclusive" && VFY_BENIGN_KINDS[r.inconclusive_kind]
      ? "nothing to check" : r.status;
    row.append(el("span", { class: "vfy-verdict", text: verdict }));
    row.append(el("span", { class: "vfy-counts", text: vfyCountsText(r) }));
    const reason = el("span", { class: "vfy-reason", text: r.reason || "", title: r.reason || "" });
    reason.onclick = () => reason.classList.toggle("wrap");
    row.append(reason);
    if (r.explainable && !history) {
      const explainBtn = el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Explain" });
      explainBtn.onclick = () => openVerifyExplain(id, r.schema, r.table, explainBtn);
      row.append(explainBtn);
    }
    rows.append(row);
  });
  container.append(rows);
}

// openVerifyExplain shows the row-level drill-down for one mismatched table,
// re-using the modal chrome showRotationDialog established. The server
// computes it in the background (#1375), so most of this is the wait: a busy
// dialog with Cancel, a ~20-minute poll, and the rules for which failures are
// worth retrying.
async function openVerifyExplain(id, schema, table, btn) {
  if (busyModalActive()) return; // one drill-down at a time (#1375)
  const gen = serverGen;
  const ctrl = new AbortController();
  const busy = openBusyModal(null, {
    title: "Working out what differs",
    errTitle: "Couldn't explain this mismatch",
    facts: [["table", schema + "." + table]],
    note: "Rebuilding this table from the older snapshot and its change log to diff it row by row: minutes on a large table. " +
      "The work continues on the server; closing this only stops the waiting. " +
      "A new verify run discards drill-downs from the previous one; Explain is unavailable until a baseline-anchored run reports this table as a mismatch again.",
    disable: btn ? [btn] : [],
    onCancel: () => ctrl.abort(),
  });

  // The server answers 202 while the reconstruction runs and 200 with the
  // drill-down once it lands (#1375), so this polls instead of holding one
  // long request open — a synchronous answer could not outlive a fronting
  // proxy's read timeout, which is what made this button look dead.
  const url = "/api/servers/" + encodeURIComponent(id) + "/verify/explain" +
    "?schema=" + encodeURIComponent(schema) + "&table=" + encodeURIComponent(table);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  let ex;
  // Consecutive transport failures tolerated before giving up. A dropped
  // poll does not affect the daemon's job, and the whole reason this endpoint
  // went async is that long waits sit behind proxies that hiccup.
  let misses = 0;
  // ~20 minutes at 2s, the same cap and cadence pollVerify uses.
  for (let i = 0; i < 600; i++) {
    let res;
    try {
      res = await api(url, { signal: ctrl.signal });
    } catch (err) {
      if (err && err.name === "AbortError") return; // Cancel/ESC already tore the modal down
      // A 401 already raised the sign-in gate inside api() and bumped
      // serverGen. Bail on that here, not only in the success branch below:
      // a dead session never reaches the success branch, and retrying would
      // end in a red "Couldn't explain this mismatch: session expired" that
      // blames the drill-down and pulls focus off the password field.
      if (gen !== serverGen) { busy.close(); return; }
      // Only a MISSING response or a gateway status is the proxy hiccup this
      // loop tolerates. Any other status is an answer the console actually
      // received, and retrying it is worse than useless: the 500 that carries
      // the drill-down's own failure was CONSUMED by the read that produced
      // it, so the next poll starts a whole new reconstruction and answers
      // 202 — which resets the miss count. A failing drill-down would loop
      // for the full 20 minutes and never show the operator the error.
      // Durable 403/404 are terminal for the same reason pollVerify treats
      // them so. Re-clicking Explain is the retry.
      const gateway = err && (err.status === 502 || err.status === 503 || err.status === 504);
      if (err && err.status && !gateway) { busy.showError(err); return; }
      if (++misses > 5) { busy.showError(err); return; }
      await sleep(2000);
      continue;
    }
    misses = 0;
    if (gen !== serverGen) { busy.close(); return; }
    if (res && res.explain) { ex = res.explain; break; }
    // api() does not throw on a 202: it returns the {state:"running"} body,
    // which has no .explain, so the loop falls through to the sleep.
    await sleep(2000);
  }
  if (!ex) {
    // NOT showError: this loop cannot distinguish a STUCK reconstruction from
    // a merely slow one, and it may have spent its last ticks on tolerated
    // poll failures rather than 202s (misses resets on any success), so it
    // cannot even assert the daemon answered recently. Either way the red
    // "Couldn't explain this mismatch" treatment would claim a failure it has
    // not seen. Mirrors pollBaseline's neutral "check back" toast. The
    // wording promises nothing about reopening: a scheduled run may have
    // discarded the result, and the daemon log is where a repeat belongs.
    busy.close();
    toast("Still waiting after 20 minutes; the work continues on the server. Reopen Explain to try again, and check DBTrail's log if this repeats.");
    return;
  }
  busy.close();
  const mount = document.getElementById("modal");
  const scrim = el("div", { class: "modal-scrim show" });
  const modal = el("div", { class: "modal vfy-explain-modal", role: "dialog", "aria-label": "Verify mismatch drill-down" });
  const head = el("div", { class: "modal-head" });
  head.append(el("h2", { class: "modal-title", text: ex.schema + "." + ex.table + " doesn't match" }));
  head.append(el("p", { class: "modal-desc", text:
    (ex.total === 1 ? "1 row differs" : ex.total + " rows differ") +
    "; checked against binlog position " + ex.anchor + "." }));
  head.append(el("p", { class: "modal-desc", text:
    "Recovered = what replaying the change log on top of the older snapshot produced. Snapshot (real) = the actual values from the newer, trusted snapshot." }));
  head.append(el("button", { class: "btn btn-icon btn-ghost modal-x", type: "button", text: "✕", onclick: closeVerifyExplain }));
  modal.append(head);

  const body = el("div", { class: "vfy-explain-body" });
  if (!ex.diffs || !ex.diffs.length) {
    body.append(el("p", { class: "form-hint", text:
      "The row count differs, but no per-row content difference was found; see the raw output below." }));
  } else {
    ex.diffs.forEach((d) => body.append(verifyDiffCard(d)));
    if (ex.total > ex.diffs.length) {
      body.append(el("p", { class: "form-hint", text:
        "…and " + (ex.total - ex.diffs.length) + " more differing row(s), not shown here." }));
    }
  }
  modal.append(body);

  const raw = el("details", { class: "form-advanced vfy-explain-raw" },
    el("summary", { class: "form-adv-summary", text: "Raw output" }));
  raw.append(el("pre", { class: "vfy-explain-pre", text: ex.rendered }));
  modal.append(raw);

  const foot = el("div", { class: "modal-foot" });
  foot.append(el("button", { class: "btn btn-ghost", type: "button", text: "Close", onclick: closeVerifyExplain }));
  modal.append(foot);
  scrim.append(modal);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) closeVerifyExplain(); });
  mount.replaceChildren(scrim);
  focusModal(scrim);
}

function closeVerifyExplain() { document.getElementById("modal").replaceChildren(); }

// openModal builds the scrim/panel/head boilerplate the console's dialogs all
// repeat, and returns the body to fill plus the close it wired.
//
// Extracted rather than copied a seventh time (#1405), and it has ONE caller
// today — which is worth stating plainly rather than dressing up as a shared
// abstraction.
//
// The six existing dialogs are not migrated, and the reason is structural, not
// caution: they append their body, footer and extras as SIBLINGS of the panel,
// while this returns a body div nested inside it. Adopting it would move their
// content one level deeper and put their .modal-foot and body rules on
// different ancestors — a DOM change to working recovery-adjacent UI, for
// tidiness, with no browser coverage of most of them to catch a layout break.
//
// What it does own is the part that is genuinely identical everywhere: the
// mount, the scrim, scrim-click dismissal, the ✕, and the focus handoff.
// Escape still comes from globalKeydown keyed off the shared #modal slot, so
// nothing here re-implements it.
function openModal(opts) {
  const mount = document.getElementById("modal");
  const scrim = el("div", { class: "modal-scrim show" });
  const modal = el("div", { class: "modal" + (opts.class ? " " + opts.class : ""),
    role: "dialog", "aria-label": opts.label });
  const close = () => {
    if (mount) mount.replaceChildren();
    if (opts.onClose) opts.onClose();
  };
  const head = el("div", { class: "modal-head" });
  if (opts.title) head.append(el("h2", { class: "modal-title", text: opts.title }));
  for (const d of opts.desc || []) head.append(el("p", { class: "modal-desc", text: d }));
  head.append(el("button", { class: "btn btn-icon btn-ghost modal-x", type: "button", text: "✕", onclick: close }));
  modal.append(head);
  const body = el("div", { class: "modal-body" });
  modal.append(body);
  scrim.append(modal);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) close(); });
  if (mount) mount.replaceChildren(scrim);
  focusModal(scrim);
  // The PANEL comes back alongside the body because a footer must not live
  // inside a scrolling body: a wide row's state table is taller than the
  // viewport, and an action rendered after it scrolls out of reach. Callers
  // append their own .modal-foot-ish row as a sibling, which is also how the
  // dialogs this did not absorb are built.
  return { body, panel: modal, close };
}

// focusModal moves keyboard focus into a freshly-opened dialog (#968): the
// first form field when there is one, else the dialog panel itself
// (tabindex -1: a programmatic target, not a tab stop). It used to land on
// the first button, which put the focus ring on the close X the moment a
// dialog with nothing to type into opened (#1950); the panel holds focus
// without drawing anything, and Tab from it reaches the first control.
// Escape-to-close lives in globalKeydown, keyed off the shared #modal slot,
// so no per-dialog wiring is needed.
function focusModal(scrim) {
  const f = scrim.querySelector("input, select, textarea");
  if (f) { f.focus(); return; }
  const panel = scrim.querySelector('[role="dialog"]') || scrim;
  if (!panel.hasAttribute("tabindex")) panel.setAttribute("tabindex", "-1");
  panel.focus();
}

const VFY_KIND_LABEL = {
  changed: "Value differs",
  missing: "Missing from recovery",
  extra: "Unexpected in recovery",
};
const VFY_KIND_CLASS = { changed: "warn", missing: "fail", extra: "fail" };
const VFY_KIND_NOTE = {
  missing: "This row exists in the real snapshot, but replaying the change log never reproduced it.",
  extra: "Replaying the change log produced this row, but it isn't in the real snapshot.",
};

// verifyDiffCard renders one RowDiff, reusing the doctor-preflight card
// styling already established for verify's own match/mismatch/inconclusive
// results — same "named check + status + detail" shape.
function verifyDiffCard(d) {
  const cls = VFY_KIND_CLASS[d.kind] || "warn";
  const card = el("div", { class: "doctor-card " + cls });
  card.append(el("span", { class: "dc-mark", text: cls === "fail" ? "✗" : "!" }));
  const bodyEl = el("div", { class: "dc-body" },
    el("div", { class: "dc-name", text: (VFY_KIND_LABEL[d.kind] || d.kind) + " · " + d.pk }));
  if (VFY_KIND_NOTE[d.kind]) {
    bodyEl.append(el("p", { class: "form-hint", text: VFY_KIND_NOTE[d.kind] }));
  } else if (d.cells && d.cells.length) {
    bodyEl.append(verifyDiffCellsTable(d.cells));
  }
  card.append(bodyEl);
  return card;
}

function verifyDiffCellsTable(cells) {
  const table = el("table", { class: "vfy-diff-table" });
  table.append(el("thead", {}, el("tr", {},
    el("th", { text: "Column" }), el("th", { text: "Recovered" }), el("th", { text: "Snapshot (real)" }))));
  const tbody = el("tbody");
  cells.forEach((c) => tbody.append(el("tr", {},
    el("td", { text: c.column }),
    el("td", {}, verifyDiffValue(c.recovery)),
    el("td", {}, verifyDiffValue(c.baseline)))));
  table.append(tbody);
  return table;
}

// verifyDiffValue styles a NULL cell distinctly from the literal text "NULL"
// a real value could (rarely) contain — a display-only best effort, not a
// data distinction: the underlying comparison that flagged this diff already
// happened server-side against the real NULL-vs-empty-aware bytes.
function verifyDiffValue(v) {
  if (v === "NULL") return el("span", { class: "vfy-null", text: "NULL" });
  return document.createTextNode(v);
}

// ── schema changes (DDL history, #1443) ─────────────────────────────────────
// A dedicated read-only view over the index's schema_changes table, under
// Investigate beside Events. Its own view rather than a DDL mode of the
// Events list: the result shape (statement, binlog coordinate, no row image)
// is different, and mixing shapes into one table makes both harder to read.
// Filters mirror the Events panel and the MCP list_schema_changes tool; the
// cap model is the same (default 100, max 1000, has_more from one probe row).

const SC_DDL_TYPES = ["CREATE", "ALTER", "DROP", "RENAME", "TRUNCATE"];
// Badge tint by what the statement does to the table: something new, a change
// in place, or something gone. Anything else keeps the neutral tint. MariaDB's
// CREATE OR REPLACE TABLE drops an existing table's rows, so it reads as REPLACE.
const SC_BADGE_CLASS = { CREATE: "b-insert", ALTER: "b-update", RENAME: "b-update", DROP: "b-delete", TRUNCATE: "b-delete", REPLACE: "b-delete" };
function scBadge(ddlType) {
  const upper = String(ddlType || "").toUpperCase();
  const word = /^CREATE\s+OR\s+REPLACE\b/.test(upper) ? "REPLACE" : upper.split(/\s+/)[0];
  return el("span", { class: "badge " + (SC_BADGE_CLASS[word] || "b-baseline"), text: word || "DDL", title: ddlType || null });
}

let scLastQuery = null;

function renderSchemaChanges(params) {
  const v = VIEW(); clear(v);
  // The zone once, in the header (#1950): every timestamp below is UTC.
  v.append(pageHead("Schema changes",
    el("p", { class: "page-sub", text: "Every CREATE, ALTER, DROP, RENAME and TRUNCATE the stream recorded, newest first." }),
    [el("span", { class: "page-asof" }, tzChip())]));

  const form = el("form", { class: "filters", id: "sc-form" });
  form.append(fieldSelect("Schema", "schema", "md", true));
  form.append(fieldSelect("Table", "table", "md", false, true));
  form.append(fieldSelect("Type", "ddl_type", "sm", false, false, [""].concat(SC_DDL_TYPES), "any"));
  form.append(fieldDateInput("Since (UTC)", "since", "md", "YYYY-MM-DD HH:MM:SS"));
  form.append(fieldDateInput("Until (UTC)", "until", "md", "YYYY-MM-DD HH:MM:SS"));
  form.append(fieldInput("Limit", "limit", "sm", "100"));
  if (params) {
    ["schema", "table", "ddl_type", "since", "until"].forEach((k) => { if (params[k] && form.elements[k]) form.elements[k].value = params[k]; });
  }
  v.append(form);

  const bar = el("div", { class: "result-bar" });
  bar.append(el("span", { class: "result-count" }, el("b", { id: "sc-count", text: "…" }), el("span", { id: "sc-count-note", text: " change(s)" })));
  bar.append(el("span", { class: "spacer" }));
  v.append(bar);
  v.append(el("div", { id: "sc-warnings", class: "warnings" }));

  const list = el("div", { class: "events", id: "sc-list" });
  const head = el("div", { class: "sc-head" });
  ["time", "table", "type", "binlog position"].forEach((h) => head.append(el("span", { text: h })));
  list.append(head);
  list.append(el("div", { id: "sc-rows" }));
  v.append(list);

  let t = null;
  const run = () => runSchemaChangesQuery(form);
  form.addEventListener("input", () => { clearTimeout(t); t = setTimeout(run, 200); });
  form.addEventListener("change", run);
  form.addEventListener("submit", (e) => { e.preventDefault(); run(); });
  wireSchemaCascade(form);

  populateSchemas(form);
  run();
  viewEnter();
}

async function runSchemaChangesQuery(form) {
  const gen = serverGen;
  const rowsEl = $("#sc-rows", VIEW());
  const countEl = $("#sc-count", VIEW());
  const noteEl = $("#sc-count-note", VIEW());
  if (!rowsEl) return;

  const f = Object.fromEntries(new FormData(form).entries());
  const apiParams = {};
  ["schema", "table", "ddl_type", "since", "until", "limit"].forEach((k) => {
    if (f[k] && f[k].trim() && f[k] !== "any") apiParams[k] = f[k].trim();
  });
  const myQuery = apiParams;
  scLastQuery = myQuery;

  clear(rowsEl);
  rowsEl.append(el("div", { class: "view-loading", role: "status", text: "Loading…" }));
  if (countEl) countEl.textContent = "…";

  let data;
  try {
    data = await api("/api/schema-changes?" + new URLSearchParams(apiParams).toString());
  } catch (err) {
    if (gen !== serverGen || scLastQuery !== myQuery) return;
    clear(rowsEl); renderError(rowsEl, err);
    renderWarnings($("#sc-warnings", VIEW()), []);
    if (countEl) countEl.textContent = "0";
    return;
  }
  if (gen !== serverGen || scLastQuery !== myQuery) return;

  const changes = data.changes || [];
  if (countEl) countEl.textContent = String(changes.length);
  // The cap notice, in the same place Events puts its position line: a
  // capped list must say it is a prefix, or an empty-looking tail reads as
  // "nothing else happened".
  let note = " change(s)";
  if (data.has_more) note += " · the newest " + changes.length + " shown; narrow the time range or raise the limit (max 1000) to see more";
  if (noteEl) noteEl.textContent = note;
  renderWarnings($("#sc-warnings", VIEW()), data.warnings || []);
  buildSchemaChangeRows(rowsEl, changes, Object.keys(apiParams).some((k) => k !== "limit"), !!data.statement_withheld);
}

// withheld: the server dropped every statement because an access profile is
// active (the response warning says why); the cell says so instead of
// rendering as an empty statement.
// SCHEMA_EMPTY_ART: a table with a column being added, drawn (static, so
// svgEl is right here).
const SCHEMA_EMPTY_ART = `<svg viewBox="0 0 160 72" aria-hidden="true"><rect x="14" y="8" width="96" height="56" rx="8" fill="var(--surface)" stroke="var(--line)"/><path d="M14 24h96M46 8v56M78 8v56" stroke="var(--line)"/><rect x="22" y="13" width="16" height="5" rx="2.5" fill="var(--ink-4)"/><rect x="54" y="13" width="16" height="5" rx="2.5" fill="var(--ink-4)"/><rect x="86" y="13" width="16" height="5" rx="2.5" fill="var(--ink-4)"/><rect x="118" y="8" width="28" height="56" rx="8" fill="var(--update-bg)" stroke="var(--update)" stroke-dasharray="4 3"/><path d="M132 30v12M126 36h12" stroke="var(--update)" stroke-width="2"/></svg>`;

function buildSchemaChangeRows(container, changes, filtered, withheld) {
  clear(container);
  if (!changes.length) {
    const box = el("div", { class: "empty" });
    box.append(el("div", { class: "empty-art", "aria-hidden": "true" }, svgEl(SCHEMA_EMPTY_ART)));
    box.append(el("h3", { text: "No schema changes found" }));
    box.append(el("p", { text: filtered
      ? "No DDL matched these filters. Widen the time range or clear a filter to see more."
      : "No CREATE, ALTER, DROP, RENAME or TRUNCATE has been recorded for this server yet. Changes are recorded as the stream sees them." }));
    container.append(box);
    return;
  }
  changes.forEach((c, i) => {
    // Expandable: the statement is clamped to a few lines and opens in full
    // on click, so a long ALTER does not push the next row off screen.
    const row = el("div", { class: "sc-row", "data-sc": i, tabindex: "0", role: "button", "aria-expanded": "false" });
    row.append(tsSpan("ev-time", c.detected_at));
    // Rows indexed before #1435 recorded unqualified DDL with no schema
    // (new rows carry the session default), so the bare-table rendering and
    // its tooltip are the historical-row path; show the table alone rather
    // than ".users".
    row.append(el("span", { class: "ev-table", text: c.schema_name ? c.schema_name + "." + c.table_name : c.table_name,
      title: c.schema_name ? null : "Schema not recorded: the statement did not name it" }));
    row.append(el("span", {}, scBadge(c.ddl_type)));
    row.append(el("span", { class: "sc-pos", text: c.binlog_file + ":" + c.binlog_pos }));
    row.append(withheld
      ? el("pre", { class: "sc-stmt sc-stmt-withheld", text: "(statement withheld under the active access profile)" })
      : el("pre", { class: "sc-stmt", text: c.statement || "" }));
    row.addEventListener("click", () => {
      const open = row.classList.toggle("open");
      row.setAttribute("aria-expanded", open ? "true" : "false");
    });
    row.addEventListener("keydown", (ke) => {
      if (ke.key === "Enter" || ke.key === " ") { ke.preventDefault(); row.click(); }
    });
    container.append(row);
  });
}

// ── schemas / tables cascade ──────────────────────────────────────────────────

async function loadSchemas() {
  if (schemaCache) return schemaCache;
  const gen = serverGen;
  const data = await api("/api/schemas");
  // snapshot_only marks schemas listed via the schema snapshot with no live
  // events observed; snapshot_unavailable means the snapshot half was skipped
  // because the resolver failed to load (#1071). Both are provenance for the
  // pickers — `schemas` stays the full selectable union.
  const result = {
    schemas: data.schemas || [],
    snapshotOnly: data.snapshot_only || [],
    snapshotUnavailable: !!data.snapshot_unavailable,
  };
  // Guard the cache WRITE, not just the render: a response in flight when the
  // operator switches servers must not poison the freshly-cleared cache with
  // the previous server's schemas.
  if (gen === serverGen) schemaCache = result;
  return result;
}

async function populateSchemas(root) {
  const gen = serverGen;
  const selects = $all(".schema-select", root || document);
  if (!selects.length) return;
  let data;
  try { data = await loadSchemas(); }
  catch (err) {
    if (gen !== serverGen) return;
    selects.forEach((sel) => { const keep = sel.value; clear(sel); sel.append(opt("", "(error: " + ((err && err.message) || err) + ")")); sel.value = keep; });
    return;
  }
  if (gen !== serverGen) return;
  selects.forEach((sel) => {
    const keep = sel.value;
    clear(sel);
    sel.append(opt("", "— select —"));
    data.schemas.forEach((s) => {
      const snapOnly = data.snapshotOnly.includes(s);
      const o = opt(s, snapOnly ? s + " (snapshot only)" : s);
      if (snapOnly) o.title = "Listed by the schema snapshot only: no live events indexed; queries may return nothing.";
      sel.append(o);
    });
    if (data.snapshotUnavailable) {
      // Otherwise an empty (or truncated) picker is indistinguishable from a
      // healthy index with no schemas — the very ambiguity #1065 fixed.
      const note = opt("", "(schema snapshot unreadable; archive-only schemas may be missing; see server log)");
      note.disabled = true;
      sel.append(note);
    }
    if (keep) sel.value = keep;
  });
}

async function loadTables(form) {
  const gen = serverGen;
  const sel = form.querySelector(".schema-select");
  const tsel = form.querySelector(".table-select");
  // A form carries either a closed table <select> (query-style filters, where
  // "any" is meaningful) or a table-combo input+datalist (#1364, Restore —
  // where a hand-typed dropped table must still submit).
  const combo = form.querySelector(".table-combo");
  if (!tsel && !combo) return;
  const schema = sel ? sel.value : "";
  if (tsel) {
    clear(tsel);
    tsel.append(opt("", "— any —"));
  }
  const dl = combo ? document.getElementById(combo.getAttribute("list")) : null;
  const hint = combo ? combo.parentElement.querySelector(".combo-hint") : null;
  // The schema the current combo value was entered under — read BEFORE the
  // fetch so a slow listing still knows whether this is a schema SWITCH.
  // The marker only advances on a REAL selection: letting "— select —"
  // overwrite it would launder A → "— select —" → B into a first selection
  // and carry A's table silently into B.
  const prevSchema = combo ? (combo.dataset.schema || "") : "";
  if (combo && schema) combo.dataset.schema = schema;
  if (dl) clear(dl);
  if (hint) hint.textContent = "";
  if (!schema) return;
  let tables = tablesCache.get(schema);
  try {
    if (!tables) {
      if (hint) hint.textContent = "loading tables…";
      if (combo) combo.setAttribute("aria-busy", "true");
      const data = await api("/api/schemas?schema=" + encodeURIComponent(schema));
      tables = data.tables || [];
      if (gen === serverGen) tablesCache.set(schema, tables); // don't cache under a server we've since switched away from
    }
  } catch (err) {
    if (combo) combo.removeAttribute("aria-busy");
    // Persistent, announced (aria-live) failure note — the toast alone lasts
    // 2.2s. Set BEFORE the serverGen bail so no path strands "loading…".
    if (hint) hint.textContent = "couldn't load suggestions; type the table name";
    if (gen !== serverGen) return;
    if (tsel) tsel.append(opt("", "(error loading tables)"));
    // A failed listing leaves the combo as usable free text with its value
    // intact — we cannot know whether the value belongs to the new schema,
    // and a dead or emptied field would be worse than a stale suggestion.
    toastError("failed to load tables: " + ((err && err.message) || err));
    return;
  }
  if (combo) combo.removeAttribute("aria-busy");
  if (gen !== serverGen) return;
  if (hint) hint.textContent = "";
  // A newer selection may have superseded this fetch (rapid schema switching):
  // the last resolver must not repopulate under the wrong schema.
  if ((sel ? sel.value : "") !== schema) return;
  if (tsel) tables.forEach((t) => tsel.append(opt(t, t)));
  if (dl) tables.forEach((t) => dl.append(opt(t, t)));
  // Switching schema clears a stale table value that doesn't belong to the new
  // schema (#1364) — but only on a SWITCH (previous schema non-empty and
  // different): a name typed before the FIRST schema selection is the
  // dropped-table flow and must survive. A value present in the new schema's
  // own listing belongs there and is kept.
  if (combo && prevSchema && prevSchema !== schema && combo.value && !tables.includes(combo.value)) {
    combo.value = "";
  }
}

function wireSchemaCascade(root) {
  $all(".schema-select", root).forEach((sel) => sel.addEventListener("change", () => loadTables(sel.closest("form"))));
}

// updateSrvNote labels where header-less data comes from when no servers are
// listed. The hidden boot index is NOT guaranteed empty — a daemon restarted
// without its previous SOURCE_DSN, or pointed at a reused index DB, renders
// real history here — so the origin must be attributed right under the
// "no servers yet" switcher, not only in the docs. Monitor-gated: on a
// registry-only serve an empty list 404s instead, where this label would lie.
function updateSrvNote() {
  const n = document.getElementById("srv-note");
  if (n) n.hidden = !(serversEmpty && capsCache.monitor);
}

// ── Connect AI client (#1041, managed token #1052) ───────────────────────────
//
// Settings view for wiring an MCP client (Claude Desktop, claude.ai custom
// connectors, any Streamable-HTTP client) to this console's /mcp endpoint.
// Availability comes from capabilities.mcp — a static or UI-managed token is
// configured. Token VALUES are never rendered here, with one deliberate
// exception: the just-minted plaintext (mcpMintedOnce), displayed exactly
// once right after generation and never re-displayable.

// mcpMintedOnce holds a just-generated token for exactly one render of the
// Connect AI view (#1052) — consumed and cleared by renderConnect so a later
// navigation back to the view can never re-display it.
let mcpMintedOnce = null;

async function renderConnect() {
  const gen = serverGen, vgen = viewGen;
  // Consume the one-time plaintext FIRST — before any await or early return —
  // so a server-switch mid-load can never leave it parked in the module
  // global to be re-displayed (stale) on a later visit.
  const minted = mcpMintedOnce;
  mcpMintedOnce = null;
  viewLoading();
  // The server list only picks between /mcp and /mcp/{id-or-name}; a failure
  // (or the registry-only 404 on an empty console) degrades to the bare
  // default-server URL instead of blanking the page.
  // serversFailed covers what that degrade leaves out: the Iceberg command
  // needs the selected server's index address, and a failure other than a
  // refusal or the empty console must say why the panel is missing. The
  // default id is THIS response's, the one a header-less request resolves to
  // right now, not the one the last loadServers left behind.
  let servers = [], serversDefault = "", serversFailed = false;
  try {
    const r = await api("/api/servers");
    servers = r.servers || [];
    serversDefault = r.default_id || "";
  } catch (err) { serversFailed = err.status !== 403 && err.status !== 404; }
  // Token status (#1052): presence/provenance only, never a value. null on
  // failure — the card degrades to a reload hint instead of blanking the page.
  let tokStatus = null;
  try { tokStatus = await api("/api/mcp-token"); } catch (_) {}
  // Time-travel port status (#1446): on/off plus its address, never the token.
  // null on failure — the SQL client panel says it could not check.
  let fbStatus = null;
  try { fbStatus = await api("/api/flashback"); } catch (_) {}
  // Where the selected server's snapshots live, for the Iceberg export
  // command (#1573): location_only is the Backups listing's own resolution,
  // answered without reading the storage or opening the server's index. Not
  // asked when the session may not read this server or a data profile is
  // active (data_profile, the key the server refuses on): the server would
  // refuse it on every visit, auditing a denial under a profile, since the
  // command hands out unredacted data. A refusal draws no panel; any other
  // failure says so in one line, so a missing panel never reads as "this
  // server keeps no backups".
  //
  // The permission is servers:read because that is what the route takes:
  // listing a server's snapshots is a read about that server, not console
  // administration. It must track the route — gating on settings:read here
  // would draw the panel for a settings-only session and then 403 its
  // location lookup, which is a button that fails rather than one absent.
  let bLoc = null, bLocFailed = false;
  if ((capsCache.permissions || {})["servers:read"] !== false && !capsCache.data_profile) {
    try { bLoc = await api("/api/baselines?location_only=1"); } catch (err) { bLocFailed = err.status !== 403; }
  }
  // vgen: navigating away while these requests are out must not let this
  // page paint over the next one.
  if (gen !== serverGen || vgen !== viewGen) {
    // The consumed plaintext cannot be re-shown; say so instead of losing it
    // silently (the user must rotate to get a usable value).
    if (minted) toastError("Token display interrupted; the plain token is gone. Click New token to get a fresh one");
    return;
  }
  try {
    // No note for a console with no server at all: the location 404s there
    // too, and "this server" would name nothing.
    const cur = servers.find((s) => s.id === (currentServer || serversDefault));
    buildConnect(servers, tokStatus, minted, fbStatus,
      { cur: cur, loc: bLoc, failed: (bLocFailed && !!cur) || serversFailed });
  } catch (err) {
    if (minted) toastError("Token display interrupted; the plain token is gone. Click New token to get a fresh one");
    const v = VIEW(); clear(v); v.append(pageHead("MCP Server", null)); renderError(v, err);
  }
}

// mcpSelector maps a server entry to its /mcp/{id-or-name} path selector: the
// registry display name when set (readable), the id otherwise, and the
// reserved "default" selector for the ephemeral boot (cli) entry.
function mcpSelector(entry) {
  if (!entry) return "";
  if (entry.kind === "ephemeral") return "default";
  return entry.name || entry.id || "";
}

// mcpURL builds the ready-to-copy endpoint URL from the BROWSER's origin, so
// it is correct behind a reverse proxy for the common case. With one listed
// server the bare /mcp (the console's default server) is enough; with several,
// the per-server form pins the server currently selected in the sidebar.
function mcpURL(servers) {
  let path = "/mcp";
  if ((servers || []).length > 1) {
    const cur = servers.find((s) => s.id === (currentServer || defaultServerId));
    const sel = mcpSelector(cur);
    if (sel) path += "/" + encodeURIComponent(sel);
  }
  return location.origin + path;
}

function copyText(text, what) {
  // navigator.clipboard does not exist outside a secure context (plain http on
  // anything but localhost), where this threw and the button looked like it
  // had worked. Say what to do instead, rather than failing in the console.
  const clip = navigator.clipboard;
  if (!clip || !clip.writeText) {
    toastError("Copying needs https or localhost. Select the text and copy it by hand.");
    return;
  }
  clip.writeText(text).then(() => toast(what + " copied to clipboard"), () => toastError("Copy failed."));
}

function buildConnect(servers, tokStatus, minted, fbStatus, ice) {
  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub" },
    "Three steps and Claude can answer questions about your database history. It can only read; it can never change anything.");
  v.append(pageHead("MCP Server", sub));

  const cards = el("div", { class: "cards" });
  cards.append(mcpTokenCard(tokStatus, minted));
  cards.append(mcpEndpointCard(servers));
  cards.append(bundleCard());
  v.append(cards);
  if (capsCache.mcp) v.append(otherClientsPanel(servers));
  v.append(sqlClientPanel(servers, fbStatus));
  // The DuckDB schema card, on this page with or without the watch daemon
  // (#1573; it sat on Backups from #1581, and here only on a serve-only
  // console). GET /api/views.sql needs settings:read, so a session denied it
  // gets no card whose button could only be refused (on the old Backups page
  // the listing's own permission hid it). The views capability is already false under a
  // data profile.
  if (capsCache.views && (capsCache.permissions || {})["settings:read"] !== false) v.append(duckdbPanel());
  // Last: what the selected server's snapshots can become, for a reader who
  // wants them in front of a reporting engine (#1466). It was the bottom of
  // the old Backups page, a third answer to "what do I download" there — the
  // page that has since merged into Snapshots, which never carried it (#1573).
  const iceberg = icebergExportPanel(ice.cur, ice.loc);
  if (iceberg) v.append(iceberg);
  else if (ice.failed) {
    v.append(el("p", { class: "form-hint cn-ice-err", style: "margin-top:18px", text:
      "Could not check where this server's snapshots are kept, so the Iceberg export command is not shown. Reload the page to try again." }));
  }
  viewEnter();
}

// ── Access profiles (#1445) ──────────────────────────────────────────────────
//
// The flags, profiles and rules that `--profile` (and a session's data
// profile) enforce, authored here instead of only from the command line.
// Every mutation POSTs to the selected server's index and answers with the
// whole document, which is what the page repaints from: it never shows what
// it thinks it just did, only what the server now holds. Mutation controls
// carry data-perm="settings:write" so a session without that permission does
// not see buttons that would only 403.

async function renderAccessProfiles() {
  const gen = serverGen, vgen = viewGen;
  viewLoading();
  let doc;
  try {
    doc = await api("/api/access-profiles");
  } catch (err) {
    if (gen !== serverGen || vgen !== viewGen) return;
    const v = VIEW(); clear(v); v.append(accessProfilesHead()); renderError(v, err);
    return;
  }
  if (gen !== serverGen || vgen !== viewGen) return;
  try {
    buildAccessProfiles(doc);
  } catch (err) {
    const v = VIEW(); clear(v); v.append(accessProfilesHead()); renderError(v, err);
  }
}

// accessHead: the page head, with the one line the drawing cannot say (#1950).
function accessProfilesHead() {
  const sub = el("p", { class: "page-sub", text: "Decide who sees what. The same settings as the command line (CLI: bintrail flag, profile, access), stored in this server's index." });
  return pageHead("Access profiles", sub);
}

// accessDrawing draws how the three lists work together (#1950): a flag on a
// column, a profile, a deny rule, and what a query under that profile sees
// (the column blanked). It replaced the paragraph that said so. Fed by the
// first deny rule whose flag is on a table; anything short of that (no deny
// rule, or a rule whose flag labels nothing yet) is drawn as an EXAMPLE, from
// the placeholders the forms show: dimmed dashed tiles, "For example," on the
// first label and in the text alternative, so example data never reads as a
// denial that exists. Built with el().
function accessDrawing(doc) {
  const rules = (doc && doc.rules) || [], flags = (doc && doc.flags) || [];
  const denies = rules.filter((x) => x.permission === "deny");
  const r = denies.find((x) => flags.some((y) => y.flag === x.flag)) || null;
  const f = r ? flags.find((x) => x.flag === r.flag) : null;
  const example = !f;
  const flag = example ? "pii" : r.flag, profile = example ? "marketing" : r.profile;
  const where = example ? "shop.customers" : f.schema + "." + f.table, col = example ? "email" : (f.column || "");
  const step = (k, body) => el("div", { class: "ap-step" }, el("span", { class: "ap-step-k", text: k }), body);
  const arrow = () => el("span", { class: "ap-arrow", "aria-hidden": "true" });
  const fig = el("div", { class: "ap-draw" + (example ? " ap-draw-example" : "") },
    step(example ? "For example, flag" : "flag", el("span", { class: "ap-cell" }, el("span", { class: "mono", text: where + (col ? "." + col : "") }), el("span", { class: "chip chip-tt", text: flag }))),
    arrow(),
    step("profile", el("span", { class: "ap-cell" }, el("b", { text: profile }), el("span", { class: "chip chip-fail", text: "DENY " + flag }))),
    arrow(),
    step("a query sees", el("span", { class: "ap-cell" }, el("span", { class: "mono", text: col ? col + ": " : where }), el("span", { class: "ap-blank", text: col ? "blank" : "hidden" }))));
  fig.setAttribute("role", "img");
  fig.setAttribute("aria-label", (example ? "For example: a flag " : "A flag ") + flag + " on " + where + (col ? " column " + col : "") + "; the profile " + profile + " denies " + flag + "; a query run under " + profile + (col ? " sees that column blanked." : " does not see that table."));
  return fig;
}

function buildAccessProfiles(doc) {
  const v = VIEW(); clear(v);
  v.append(accessProfilesHead());
  v.append(accessDrawing(doc));
  const stack = el("div", { class: "ap-stack", id: "ap-stack" });
  stack.append(accessFlagsPanel(doc));
  stack.append(accessProfilesPanel(doc));
  stack.append(accessRulesPanel(doc));
  v.append(stack);
  // Nodes built after the capabilities fetch are not gated by it; gate them
  // now, the same way a server switch re-gates the sidebar.
  gatePermissions();
  viewEnter();
}

// accessMutate posts one verb and repaints from the document it returns.
// The server's own error text is shown as-is: it is the shared package's
// message, the words the command line would refuse with. The generations
// are captured at dispatch: a server switch or a navigation while the POST
// is in flight means the document that comes back describes an index the
// view no longer shows, so it must not be painted over whatever is there.
async function accessMutate(path, body, okMsg) {
  const gen = serverGen, vgen = viewGen;
  let doc;
  try {
    doc = await api(path, { method: "POST", body });
  } catch (err) {
    if (gen !== serverGen || vgen !== viewGen) return false;
    toastError((err && err.message) || String(err));
    return false;
  }
  if (gen !== serverGen || vgen !== viewGen) return false;
  if (okMsg) toast(okMsg);
  buildAccessProfiles(doc);
  return true;
}

function accessRemoveButton(label, onclick) {
  return el("button", { class: "btn btn-sm btn-ghost", type: "button", text: label, "data-perm": "settings:write", onclick });
}

function accessField(label, input) {
  return el("label", { class: "field" }, el("span", { class: "field-label", text: label }), input);
}

function accessInput(name, placeholder, opts) {
  return el("input", Object.assign({ class: "input", name, placeholder, spellcheck: "false", autocomplete: "off" }, opts || {}));
}

// accessPanel is the shared frame: a titled panel with a count, a list of
// rows (or an empty line), an add form and a one-line hint.
// ACCESS_EMPTY_ART: an empty list, three dashed rows (static).
const ACCESS_EMPTY_ART = `<svg viewBox="0 0 120 40" aria-hidden="true"><rect x="2" y="2" width="116" height="10" rx="5" fill="none" stroke="var(--ink-4)" stroke-dasharray="4 3"/><rect x="2" y="15" width="116" height="10" rx="5" fill="none" stroke="var(--line)" stroke-dasharray="4 3"/><rect x="2" y="28" width="116" height="10" rx="5" fill="none" stroke="var(--line)" stroke-dasharray="4 3"/></svg>`;

function accessPanel(id, title, count, rows, emptyText, form, hint) {
  const panel = el("section", { class: "ov-panel", id });
  panel.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: title }),
    el("span", { class: "chip chip-age", text: String(count) })));
  const list = el("div", { class: "stg-list" });
  if (!rows.length) {
    list.append(el("div", { class: "empty ev-empty ap-empty" },
      el("div", { class: "empty-art", "aria-hidden": "true" }, svgEl(ACCESS_EMPTY_ART)),
      el("p", { text: emptyText })));
  }
  rows.forEach((r) => list.append(r));
  panel.append(list);
  if (form) {
    // Plain at rest; the form being filled gets the filled button (#1950),
    // never a disabled one.
    form.addEventListener("input", () => {
      const b = form.querySelector('button[type="submit"]');
      if (b && !b.disabled) b.classList.add("btn-primary");
    });
    panel.append(form);
  }
  panel.append(el("p", { class: "form-hint stg-foot", text: hint }));
  return panel;
}

function accessFlagsPanel(doc) {
  const flags = doc.flags || [];
  const rows = flags.map((f) => {
    const row = el("div", { class: "stg-row ap-row" });
    row.append(el("span", { class: "stg-name mono", text: f.schema + "." + f.table }));
    row.append(el("span", { class: "stg-dest" + (f.column ? " mono" : " muted"), text: f.column || "whole table" }));
    row.append(el("span", { class: "chip chip-tt", text: f.flag }));
    row.append(accessRemoveButton("Remove", () => {
      const where = f.schema + "." + f.table + (f.column ? " (" + f.column + ")" : "");
      if (!window.confirm("Remove the flag " + f.flag + " from " + where + "? Every deny on " + f.flag + " stops covering it.")) return;
      accessMutate("/api/access-profiles/flags/remove",
        { flag: f.flag, schema: f.schema, table: f.table, column: f.column || "" },
        "Flag " + f.flag + " removed from " + where + ".");
    }));
    return row;
  });
  const form = el("form", { class: "ap-form", id: "ap-flag-form", "data-perm": "settings:write" });
  const name = accessInput("flag", "pii");
  const schema = accessInput("schema", "shop");
  const table = accessInput("table", "customers");
  const column = accessInput("column", "leave empty for the whole table");
  form.append(accessField("Flag", name), accessField("Schema", schema), accessField("Table", table), accessField("Column (optional)", column));
  form.append(el("button", { class: "btn btn-sm", type: "submit", text: "Add flag" }));
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const body = { flag: name.value.trim(), schema: schema.value.trim(), table: table.value.trim(), column: column.value.trim() };
    const where = body.schema + "." + body.table + (body.column ? " (" + body.column + ")" : "");
    await accessMutate("/api/access-profiles/flags", body, "Flag " + body.flag + " added to " + where + ".");
  });
  return accessPanel("ap-flags", "Flags", flags.length, rows,
    "No flags yet. A flag is a label like pii or billing on a table or on one column.", form,
    "A flag on a whole table hides that table from any profile that denies the flag. A flag on one column blanks that column and leaves the rest of the row. (CLI: bintrail flag add)");
}

function accessProfilesPanel(doc) {
  const profiles = doc.profiles || [];
  const rules = doc.rules || [];
  const rows = profiles.map((p) => {
    const n = rules.filter((r) => r.profile === p.name).length;
    const row = el("div", { class: "stg-row ap-row" });
    row.append(el("span", { class: "stg-name", text: p.name }));
    row.append(el("span", { class: "stg-dest" + (p.description ? "" : " muted"), text: p.description || "no description" }));
    row.append(el("span", { class: "stg-age", text: n + (n === 1 ? " rule" : " rules") }));
    row.append(accessRemoveButton("Remove", () => {
      const tail = n ? " Its " + n + (n === 1 ? " rule goes" : " rules go") + " with it." : "";
      if (!window.confirm("Remove the profile " + p.name + "?" + tail)) return;
      accessMutate("/api/access-profiles/profiles/remove", { name: p.name }, "Profile " + p.name + " removed.");
    }));
    return row;
  });
  const form = el("form", { class: "ap-form", id: "ap-profile-form", "data-perm": "settings:write" });
  const name = accessInput("name", "marketing");
  const desc = accessInput("description", "Marketing analysts");
  form.append(accessField("Profile", name), accessField("Description (optional)", desc));
  form.append(el("button", { class: "btn btn-sm", type: "submit", text: "Add profile" }));
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const body = { name: name.value.trim(), description: desc.value.trim() };
    await accessMutate("/api/access-profiles/profiles", body, "Profile " + body.name + " added.");
  });
  return accessPanel("ap-profiles", "Profiles", profiles.length, rows,
    "No profiles yet. A profile is a named group of people, like marketing or support.", form,
    "Queries run under a profile see only what its rules allow. Adding a profile that exists updates its description. (CLI: bintrail profile add)");
}

function accessRulesPanel(doc) {
  const rules = doc.rules || [];
  const profiles = doc.profiles || [];
  const flagNames = Array.from(new Set((doc.flags || []).map((f) => f.flag))).sort();
  const rows = rules.map((r) => {
    const row = el("div", { class: "stg-row ap-row" });
    row.append(el("span", { class: "stg-name", text: r.profile }));
    row.append(el("span", { class: "stg-dest", text: r.permission === "deny" ? "may not see" : "may see" }));
    row.append(el("span", { class: "chip chip-tt", text: r.flag }));
    row.append(el("span", { class: "chip " + (r.permission === "deny" ? "chip-fail" : "chip-done"), text: r.permission.toUpperCase() }));
    row.append(accessRemoveButton("Remove", () => {
      const warn = r.permission === "deny" ? " People using that profile will see it again." : "";
      if (!window.confirm("Remove the " + r.permission + " rule on " + r.flag + " for " + r.profile + "?" + warn)) return;
      accessMutate("/api/access-profiles/rules/remove", { profile: r.profile, flag: r.flag },
        "Rule on " + r.flag + " removed from " + r.profile + ".");
    }));
    return row;
  });
  const form = el("form", { class: "ap-form", id: "ap-rule-form", "data-perm": "settings:write" });
  const profile = el("select", { class: "select", name: "profile" });
  if (!profiles.length) profile.append(opt("", "add a profile first"));
  profiles.forEach((p) => profile.append(opt(p.name, p.name)));
  // Known flag names are offered; a rule may still name a flag no table
  // carries yet, as the command line allows, so this stays a text field.
  const flag = accessInput("flag", flagNames[0] || "pii", { list: "ap-flag-names" });
  const datalist = el("datalist", { id: "ap-flag-names" });
  flagNames.forEach((n) => datalist.append(opt(n, n)));
  const perm = el("select", { class: "select", name: "permission" });
  perm.append(opt("deny", "deny"), opt("allow", "allow"));
  form.append(accessField("Profile", profile), accessField("Flag", flag), datalist, accessField("Permission", perm));
  form.append(el("button", { class: "btn btn-sm", type: "submit", text: "Add rule", disabled: !profiles.length }));
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const body = { profile: profile.value, flag: flag.value.trim(), permission: perm.value };
    await accessMutate("/api/access-profiles/rules", body,
      "Rule added: " + body.profile + (body.permission === "deny" ? " may not see " : " may see ") + body.flag + ".");
  });
  return accessPanel("ap-rules", "Rules", rules.length, rows,
    "No rules yet. A rule says whether a profile may see the tables and columns that carry a flag.", form,
    "Only deny changes what a profile sees. An allow rule records intent and changes nothing. Adding a rule for a pair that has one replaces its permission. (CLI: bintrail access add)");
}

// mintMCPToken generates (or rotates) the managed token, refreshes the
// capability gate (the endpoint may have just become usable), and re-renders
// the view with the plaintext displayed once.
async function mintMCPToken(rotate) {
  if (rotate && !window.confirm("Replace the token? The current one stops working right away, and every AI client that used it will need the new one. The new value appears on the next screen, ready to copy.")) return;
  // Only the mutation itself may report failure — once the POST succeeded the
  // token EXISTS (and on rotate the old one is already dead), so a failing
  // follow-up refresh must never toast "generation failed".
  let res;
  try {
    res = await api("/api/mcp-token", { method: "POST" });
  } catch (err) {
    toastError("Token generation failed: " + (err.message || err));
    return;
  }
  mcpMintedOnce = (res && res.token) || null;
  try { await gateCapabilities(); } catch (_) {} // 401 already raised the sign-in gate
  // The reader may have left Connect while the token was being made; painting
  // it now would cover the page they moved to. The route, not viewGen: a
  // server switch re-renders Connect in place, and the token is not per
  // server. Dropped rather than parked for a later visit (the rule
  // renderConnect keeps), and said, as an interrupted display is.
  if (routeSegment() !== "connect") {
    if (mcpMintedOnce) toastError("Token display interrupted; the plain token is gone. Click New token to get a fresh one");
    mcpMintedOnce = null;
    return;
  }
  renderConnect();
}

async function revokeMCPToken() {
  if (!window.confirm("Delete the token? Every AI client using it stops working right away. You can create a new one on the MCP Server page whenever you want.")) return;
  try {
    await api("/api/mcp-token", { method: "DELETE" });
  } catch (err) {
    toastError("Could not delete the token: " + (err.message || err));
    return;
  }
  toast("Token deleted. AI clients that used it are disconnected");
  try { await gateCapabilities(); } catch (_) {}
  // Same rule as mintMCPToken: never paint Connect over a page the reader
  // moved to while the request was out.
  if (routeSegment() !== "connect") return;
  renderConnect();
}

// cnCard: a step card whose number is a BADGE, not prose. The three cards are
// the whole how-to, so each stays at one action plus one short line per state;
// parenthetical and rare facts fold into cnFine below.
function cnCard(num, title) {
  return el("div", { class: "card cn-card" },
    el("div", { class: "card-title cn-title" },
      el("span", { class: "cn-num", text: String(num) }),
      title));
}

// cnFine: collapsed fine print. The audience is non-technical, and the #1430
// step rewrite proved that spelling every contingency out in the open reads
// as a wall of text; the facts stay on the page, but folded.
function cnFine(label, ...kids) {
  const d = el("details", { class: "form-advanced cn-fine" },
    el("summary", { class: "form-adv-summary", text: label }));
  for (const k of kids) d.append(k);
  return d;
}

// mcpTokenCard (#1052): generate, rotate, and revoke the managed MCP token
// without leaving the UI. The token value renders exactly once — right after
// generation — and is otherwise represented only by its creation date.
// TOKEN_EMPTY_ART: a key with an empty dashed slot beside it (static).
const TOKEN_EMPTY_ART = `<svg viewBox="0 0 120 36" aria-hidden="true"><circle cx="16" cy="18" r="9" fill="none" stroke="var(--ink-3)" stroke-width="2"/><path d="M25 18h30M47 18v7M40 18v5" stroke="var(--ink-3)" stroke-width="2"/><rect x="66" y="8" width="50" height="20" rx="6" fill="none" stroke="var(--ink-4)" stroke-width="1.5" stroke-dasharray="4 3"/></svg>`;

function mcpTokenCard(tok, minted) {
  const card = cnCard(1, "Create a token");
  // The one-time plaintext renders UNCONDITIONALLY: a failed status fetch
  // must never swallow a token that was just minted (after a rotate, it is
  // the only valid credential and cannot be re-displayed).
  if (minted) {
    card.append(el("p", { class: "stg-hint", text: "Copy it now and keep it somewhere safe. It will never be shown again:" }));
    card.append(el("div", { class: "cn-urlrow" },
      el("code", { class: "stg-code cn-url", text: minted }),
      el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(minted, "Access token") })));
  }
  if (!tok) {
    card.append(el("p", { class: "stg-hint", text: "We could not check whether a token exists. Reload the page to try again." }));
    return card;
  }
  if (tok.managed) {
    if (!minted) {
      card.append(el("p", { class: "stg-hint", text:
        "You already have a token" + (tok.created_at ? ", created " + utcLabel(tok.created_at) : "") +
        ". It cannot be shown again." +
        // read_only hides the buttons below, so never tell that user to click one
        (tok.read_only ? "" : " Lost it? New token gives you a fresh value, shown only once; the old one stops working.") }));
    }
    if (tok.read_only) {
      card.append(cnFine("Why are there no New token and Delete token buttons?",
        el("p", { class: "form-hint", text: "This token was created by a newer version of DBTrail. It keeps working, but this version cannot replace or delete it; upgrading brings those buttons back." })));
    } else {
      card.append(el("div", { class: "cn-links" },
        el("button", { class: "btn btn-sm", type: "button", text: "New token", onclick: () => mintMCPToken(true) }),
        el("button", { class: "btn btn-sm btn-danger", type: "button", text: "Delete token", onclick: revokeMCPToken })));
    }
  } else if (!minted) {
    // No token yet (#1950): a small key, and Generate token is the page's one
    // filled button, unless a token set at startup already works, where a
    // filled button would push people to replace a working one.
    card.append(el("div", { class: "empty-art cn-key", "aria-hidden": "true" }, svgEl(TOKEN_EMPTY_ART)));
    card.append(el("p", { class: "stg-hint", text: "The token is Claude's password for DBTrail. It is shown only once, so copy it right away." }));
    card.append(el("div", { class: "cn-links" },
      el("button", { class: "btn btn-sm" + (tok.static ? "" : " btn-primary"), type: "button", text: "Generate token", onclick: () => mintMCPToken(false) })));
  }
  if (tok.static) {
    card.append(cnFine("A token was set at startup, and it already works",
      el("p", { class: "form-hint" },
        "That fixed token also works (CLI: ",
        el("code", { text: "--token" }), " or ", el("code", { text: "BINTRAIL_CONSOLE_TOKEN" }),
        "). It is managed wherever it was set up, not in the web interface.")));
  }
  return card;
}

// mcpEndpointCard: the ready-to-copy URL when the endpoint is usable, or the
// how-to-enable explanation when no token is configured (capabilities.mcp) —
// never a URL presented as ready that would only ever answer 403.
function mcpEndpointCard(servers) {
  const card = cnCard(2, "Copy the address");
  if (!capsCache.mcp) {
    card.append(el("p", { class: "stg-hint", text: "Finish step 1 first; DBTrail's address then appears below, ready to copy." }));
    return card;
  }
  const url = mcpURL(servers);
  card.append(el("div", { class: "cn-urlrow" },
    el("code", { class: "stg-code cn-url", text: url }),
    el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(url, "Web address") })));
  card.append(el("p", { class: "stg-hint", text: "Claude will ask for a URL; this is the one to paste." }));
  if ((servers || []).length > 1) {
    card.append(cnFine("Connecting a different server?",
      el("p", { class: "form-hint", text: "This address points at the server picked in the left sidebar (the Server box at the top). Pick a different server there and copy again; each server has its own address." })));
  }
  return card;
}

// claudeAskMock: a drawn miniature of Claude Desktop's install dialog, so the
// user recognizes the two fields instead of reading a paragraph about them.
// The field labels are quoted VERBATIM from
// build/packaging/mcpb/manifest.template.json; if the bundle renames them the
// picture lies. e2e 17g reads the manifest's user_config titles and compares
// the drawn labels against them, so renaming either side alone rings.
function claudeAskMock() {
  const field = (n, label, hint) => el("div", { class: "cn-mock-row" },
    el("div", { class: "cn-mock-label", text: label }),
    el("div", { class: "cn-mock-field" },
      el("span", { class: "cn-chip", text: String(n) }),
      el("span", { class: "cn-mock-paste", text: hint })));
  return el("div", { class: "cn-mock" },
    el("div", { class: "cn-mock-bar" }, el("span", { class: "cn-mock-dots" }), "Claude Desktop"),
    field(2, "Web address / MCP endpoint", "paste the address from step 2"),
    field(1, "Access token", "paste the token from step 1"));
}

// askExample: the payoff line, drawn as the chat message it becomes.
function askExample() {
  return el("div", { class: "cn-ask" },
    "Then open a new chat and ask: ",
    el("b", { text: "\u201Cwhat changed in my database in the last hour?\u201D" }));
}

// bundleCard links the .mcpb bundle (one-click Claude Desktop install) for the
// RUNNING version. Honesty rules carried from #1430: never render a download
// that can only 404 (no Windows bundle has ever been published, and an
// unversioned build has no matching asset), and the dialog mock's field
// labels come verbatim from the manifest.
function bundleCard() {
  const card = cnCard(3, "Add it to Claude");
  const ver = String(capsCache.version || "").replace(/^v/, "");
  const released = /^\d+\.\d+\.\d+$/.test(ver);
  const plat = guessPlatform();
  const relTag = "https://github.com/dbtrail/dbtrail/releases/tag/v" + ver;
  if (released && plat) {
    const asset = "dbtrail-" + plat + ".mcpb";
    card.append(el("p", { class: "stg-hint", text: "Get the Claude Desktop app (claude.ai/download), then download this installer and double-click it:" }));
    card.append(el("div", { class: "cn-links" },
      el("a", { class: "btn btn-sm", href: "https://github.com/dbtrail/dbtrail/releases/download/v" + ver + "/" + asset, target: "_blank", rel: "noopener", text: "Download the installer" }),
      el("a", { class: "btn btn-sm btn-ghost", href: relTag, target: "_blank", rel: "noopener", text: "All downloads" })));
    card.append(el("p", { class: "stg-hint", text: "Claude opens and asks to install DBTrail; accept, then fill in two things:" }));
    card.append(claudeAskMock());
    card.append(askExample());
    card.append(cnFine("Intel Mac, Windows, or claude.ai in the browser?",
      el("p", { class: "form-hint", text: "The download button guesses this computer from the browser, and Macs are assumed to have an Apple chip. On an Intel Mac, use All downloads and take dbtrail-darwin-amd64.mcpb (v" + ver + " matches DBTrail)." }),
      el("p", { class: "form-hint", text: "If the download lands on Not Found, that release shipped without the installer; take the newest release's file from All downloads instead." }),
      el("p", { class: "form-hint", text: "Windows has no installer yet. Use claude.ai in the browser: if DBTrail is reachable from the internet, open Settings, then Connectors, then Add custom connector, and paste the same address and token. On a private network (only reachable from inside), the browser path cannot reach it; use the desktop app on a Mac or Linux machine instead." })));
  } else if (released) {
    // Windows on a released build: the desktop bundle does not exist, so the
    // browser connector IS the path, not a footnote.
    card.append(el("p", { class: "stg-hint", text: "There is no Windows installer yet. If DBTrail is reachable from the internet, connect from claude.ai in the browser: open Settings, then Connectors, then Add custom connector, and paste the address and token from steps 1 and 2." }));
    card.append(askExample());
    card.append(cnFine("DBTrail on a private network?",
      el("p", { class: "form-hint", text: "On a private network (only reachable from inside), claude.ai cannot reach DBTrail; install the desktop bundle on a Mac or Linux machine instead. All downloads below has the files." }),
      el("p", { class: "form-hint" }, el("a", { href: relTag, target: "_blank", rel: "noopener", text: "All downloads for v" + ver }))));
  } else if (plat) {
    card.append(el("p", { class: "stg-hint", text: "Get the Claude Desktop app (claude.ai/download), then download the newest installer for this computer and double-click it:" }));
    card.append(el("div", { class: "cn-links" },
      el("a", { class: "btn btn-sm", href: "https://github.com/dbtrail/dbtrail/releases", target: "_blank", rel: "noopener", text: "Open the releases page" })));
    card.append(el("p", { class: "stg-hint", text: "Claude opens and asks to install DBTrail; accept, then fill in two things:" }));
    card.append(claudeAskMock());
    card.append(askExample());
    card.append(cnFine("Which file is for this computer?",
      el("p", { class: "form-hint", text: "This is a development build, so there is no matching download link. In the newest release: Mac with Apple chip is dbtrail-darwin-arm64.mcpb, Intel Mac is dbtrail-darwin-amd64.mcpb, Linux is dbtrail-linux-amd64.mcpb (or -arm64). Windows has no installer yet." }),
      el("p", { class: "form-hint", text: "Using claude.ai in the browser instead of the desktop app? If DBTrail is reachable from the internet, open Settings, then Connectors, then Add custom connector, and paste the same address and token. On a private network (only reachable from inside), use the desktop app." })));
  } else {
    // Development build ON WINDOWS: no installer exists at any version, so
    // the browser connector is the only path here too.
    card.append(el("p", { class: "stg-hint", text: "There is no Windows installer. If DBTrail is reachable from the internet, connect from claude.ai in the browser: open Settings, then Connectors, then Add custom connector, and paste the address and token from steps 1 and 2." }));
    card.append(askExample());
    card.append(cnFine("DBTrail on a private network?",
      el("p", { class: "form-hint", text: "On a private network (only reachable from inside), claude.ai cannot reach DBTrail; install the desktop bundle on a Mac or Linux machine instead." }),
      el("p", { class: "form-hint" }, el("a", { href: "https://github.com/dbtrail/dbtrail/releases", target: "_blank", rel: "noopener", text: "Open the releases page" }))));
  }
  return card;
}

// guessPlatform maps the browser's environment to a release-artifact os-arch
// pair. Best-effort presentation only (Apple Silicon is assumed on macOS); the
// all-downloads link covers every other combination.
function guessPlatform() {
  const ua = navigator.userAgent || "";
  // Empty on Windows ON PURPOSE: no Windows .mcpb has ever been published
  // (the release matrix builds the bridge for linux; darwin is attached by
  // hand), so synthesizing the name renders a download button whose link
  // can only ever 404 while the hint below says the installer does not
  // exist. The caller treats empty as "no direct download".
  if (/Windows/i.test(ua)) return "";
  if (/Mac/i.test(ua)) return "darwin-arm64";
  return "linux-amd64";
}

// otherClientsPanel: collapsed raw-config fallback for MCP clients that don't
// install .mcpb bundles. The snippet carries a PLACEHOLDER for the token — the
// real value is never rendered.
function otherClientsPanel(servers) {
  const url = mcpURL(servers);
  const snippet = JSON.stringify({
    mcpServers: {
      dbtrail: { command: "bintrail-mcp", args: ["--connect", url, "--token", "YOUR_ACCESS_TOKEN"] },
    },
  }, null, 2);
  const panel = el("section", { class: "ov-panel cn-other", style: "margin-top:18px" });
  const adv = el("details", { class: "form-advanced", style: "margin-top:0" },
    el("summary", { class: "form-adv-summary", text: "Other AI tools (technical)" }));
  adv.append(el("p", { class: "form-hint", text:
    "For claude_desktop_config.json, or any client that launches stdio MCP servers: bintrail-mcp bridges stdio to DBTrail. The token travels as an Authorization: Bearer header. Replace the placeholder with your access token:" }));
  adv.append(el("pre", { class: "stg-code cn-snippet", text: snippet }));
  adv.append(el("button", { class: "btn btn-sm", type: "button", text: "Copy snippet", onclick: () => copyText(snippet, "Config snippet") }));
  adv.append(el("p", { class: "form-hint", text:
    "If DBTrail is reachable over public HTTPS, the same URL also works directly as a claude.ai custom connector; no bridge needed." }));
  panel.append(adv);
  return panel;
}

// ── Connect a SQL client (#1446) ─────────────────────────────────────────────
//
// The embedded time-travel port (`watch --flashback-listen`, #996) serves the
// _flashback / _snapshot / _diff schemas for every monitored server over the
// MySQL protocol, routed by the connection USERNAME (the server's registry
// name or id, "default" for the boot entry: the same selector /mcp uses, so
// mcpSelector is reused) and authenticated with the console token or with the
// port's own password. A daemon that was not given the port's address at
// startup lets this panel turn the port on and off (#2101); one that was
// shows where it is and never toggles it. Status comes from GET
// /api/flashback; a null status means the call failed, and the panel says so
// instead of guessing an address.

// shellWord quotes a value for the copy-paste mysql line when it carries
// anything a shell would split or expand (a display name with a space).
function shellWord(s) {
  return /^[A-Za-z0-9_.:@-]+$/.test(s) ? s : "'" + String(s).replace(/'/g, "'\\''") + "'";
}

// flashbackHost picks the host for the mysql line: the bind host when the
// daemon named one; on a wildcard bind (host empty) the name this page was
// opened on, which is the one name known to reach the daemon's machine.
function flashbackHost(fb) {
  // location.hostname keeps the brackets of an IPv6 literal ("[::1]"), while
  // a named bind comes through Go's SplitHostPort bare ("::1"); strip them so
  // both shapes print the same way.
  // "localhost" is the one name that must not be printed: the mysql client
  // reads it as "use the local socket file" and never tries the port.
  const page = (location.hostname || "").replace(/^\[|\]$/g, "");
  return fb.host || (page.toLowerCase() === "localhost" ? "" : page) || "127.0.0.1";
}

// changeSQLPort sends one change to the port (on, off, new password) and
// draws the panel again from the answer, in place. send makes the request:
// each caller spells its own method and path, so the list of everything the
// page writes (test/console-e2e/save_controls.mjs) can see them.
function changeSQLPort(panel, servers, send, btn) {
  if (btn) btn.disabled = true;
  return send().then((rep) => {
    // The fresh render's buttons are wired to panel, the element on the
    // page, not to the detached section they were built in: otherwise the
    // second press would redraw something nobody is looking at.
    const fresh = sqlClientPanel(servers, rep, rep && rep.password, panel);
    panel.replaceChildren(...Array.from(fresh.children));
  }, (err) => {
    if (btn) btn.disabled = false;
    toastError("MySQL port: " + ((err && err.message) || err));
  });
}

// sqlClientPanel renders the port's state in one of four shapes: could not
// check, off on a read-only console (the port belongs to watch), off on a
// watch daemon (name the flag and env var, as daemon configuration), or on
// (address, user rule, password rule, and the ready-to-copy mysql line for
// the server picked in the sidebar). Not a .cn-card: the three numbered cards
// are the Connect AI how-to, and this is a different client.
function sqlClientPanel(servers, fb, reveal, live) {
  const panel = el("section", { class: "ov-panel cn-sql", style: "margin-top:18px" });
  // live: the panel already on the page that this render will be moved
  // into (changeSQLPort); the buttons act on that one.
  live = live || panel;
  panel.append(el("div", { class: "ov-panel-head" }, el("h2", { class: "ov-panel-title", text: "Connect a SQL client" })));
  const body = el("div", { class: "cn-sql-body" });
  panel.append(body);
  if (!fb) {
    body.append(el("p", { class: "cn-sql-row", text: "We could not check whether the time-travel port is on. Reload the page to try again." }));
    return panel;
  }
  if (!fb.enabled) {
    if (!capsCache.monitor) {
      // serve: there is no daemon here to carry the port, so naming the
      // watch flag as "how to turn it on" would send the reader to a flag
      // this process does not have.
      body.append(el("p", { class: "cn-sql-row" },
        "Not available: this DBTrail is read-only. The time-travel port is part of the DBTrail service (CLI: ",
        el("code", { text: "bintrail-console watch --flashback-listen" }),
        "). Start DBTrail that way and your usual MySQL client can read any table as it was at a chosen moment."));
      return panel;
    }
    if (fb.can_manage) {
      if (!sessionMay("settings:write")) {
        body.append(el("p", { class: "cn-sql-row", text: "Off. Someone who can change settings can turn it on here." }));
        return panel;
      }
      body.append(el("p", { class: "cn-sql-row", text: "Off. Turn it on and your usual MySQL client can read any table as it was at a chosen moment." }));
      if (fb.error) body.append(el("p", { class: "cn-sql-row cn-sql-err", text: "It was on and did not start: " + fb.error }));
      const addr = el("input", { class: "input cn-sql-addr", type: "text", value: fb.suggested_listen || "", "aria-label": "Port address", spellcheck: "false", autocomplete: "off" });
      const on = el("button", { class: "btn btn-sm btn-primary", type: "button", text: "Turn on" });
      on.onclick = () => changeSQLPort(live, servers, () => api("/api/flashback", { method: "PUT", body: { enabled: true, listen: addr.value } }), on);
      body.append(el("div", { class: "cn-urlrow" }, el("span", { class: "cn-sql-lbl", text: "Address" }), addr, on));
      body.append(cnFine("Which address, and the password",
        el("p", { class: "form-hint", text: "127.0.0.1 answers only on the machine DBTrail runs on. 0.0.0.0 answers on every network address of that machine. In the Docker install keep 0.0.0.0. Who can reach the port is then decided by docker-compose.yml: a new install publishes it to the machine itself only, and one installed before this existed does not publish it until its ports line is added." }),
        el("p", { class: "form-hint", text: "Turning it on creates the port's password. It is shown once, right then." })));
      return panel;
    }
    body.append(el("p", { class: "cn-sql-row", text: "Off. The port is set when DBTrail starts, not from the web interface." }));
    body.append(cnFine("How to turn it on",
      el("p", { class: "form-hint" },
        "Start DBTrail with a port address (CLI: ", el("code", { text: "--flashback-listen 127.0.0.1:3308" }),
        ", or the environment variable ", el("code", { text: "BINTRAIL_CONSOLE_FLASHBACK_LISTEN" }),
        ") and an access token (CLI: ", el("code", { text: "--console-token" }), " or ", el("code", { text: "BINTRAIL_CONSOLE_TOKEN" }),
        "). This panel then shows the address and a ready to copy mysql line, and your usual MySQL client can read any table as it was at a chosen moment.")));
    return panel;
  }
  const cur = (servers || []).find((s) => s.id === (currentServer || defaultServerId));
  const user = mcpSelector(cur);
  // A wildcard bind (":3308") reads as a typo to a non-technical reader; say
  // what it means and let the mysql line carry the name this page uses.
  body.append(el("p", { class: "cn-sql-row" }, "Address ",
    el("code", { text: fb.host ? fb.listen : "port " + (fb.port || fb.listen) }),
    fb.host ? "" : " on every network address of the machine DBTrail runs on"));
  body.append(el("p", { class: "cn-sql-row" }, "User ",
    user ? el("code", { text: user }) : el("code", { text: "<server name>" }),
    user ? ", the server picked in the left sidebar" : ", the name of a server in the left sidebar (none yet)"));
  const own = fb.source === "saved";
  if (own && reveal) {
    // Drawn whatever else the answer says: after a new password this is the
    // only place the working one exists.
    body.append(el("p", { class: "cn-sql-row", text: "Password, shown only now. Copy it and keep it somewhere safe:" }));
    body.append(el("div", { class: "cn-urlrow" },
      el("code", { class: "stg-code cn-url", text: reveal }),
      el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(reveal, "Port password") })));
  } else if (own) {
    body.append(el("p", { class: "cn-sql-row", text: "Password: the one shown when it was created" +
      (fb.password_created_at ? ", " + utcLabel(fb.password_created_at) : "") + ". It cannot be shown again." }));
  } else {
    body.append(el("p", { class: "cn-sql-row" },
      "Password: the access token (CLI: ", el("code", { text: "--console-token" }), " or ", el("code", { text: "BINTRAIL_CONSOLE_TOKEN" }), "), never shown in the web interface"));
  }
  if (fb.port) {
    const line = "mysql -h " + shellWord(flashbackHost(fb)) + " -P " + fb.port + " -u " + (user ? shellWord(user) : "<server-name>") + " -p";
    body.append(el("div", { class: "cn-urlrow" },
      el("code", { class: "stg-code cn-url", text: line }),
      el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(line, "mysql command") })));
    body.append(el("p", { class: "cn-sql-row", text: own ? "Paste the password at the password prompt." : "Paste the token at the password prompt." }));
  }
  // The port does not filter by schema (#1685): say so, so it is not taken
  // for a path that scopes access per schema.
  body.append(el("p", { class: "cn-sql-row", text: "This password reads every schema on every server in the sidebar." }));
  if (fb.can_manage && sessionMay("settings:write")) {
    const ask = (q) => typeof window.confirm !== "function" || window.confirm(q);
    const fresh = el("button", { class: "btn btn-sm", type: "button", text: "New password" });
    fresh.onclick = () => { if (ask("Create a new password?\n\nThe current one stops working for new connections.")) changeSQLPort(live, servers, () => api("/api/flashback/password", { method: "POST", body: {} }), fresh); };
    const off = el("button", { class: "btn btn-sm btn-danger", type: "button", text: "Turn off" });
    off.onclick = () => { if (ask("Turn the port off?\n\nMySQL clients connected to it are disconnected.")) changeSQLPort(live, servers, () => api("/api/flashback", { method: "PUT", body: { enabled: false } }), off); };
    body.append(el("div", { class: "cn-links" }, fresh, off));
  }
  body.append(routingBlock(fb, cur));
  body.append(cnFine("What to run, and other machines",
    el("p", { class: "form-hint" }, "Ask for a table as it was: ",
      el("code", { text: "SELECT * FROM _flashback.orders AS OF '10 minutes ago' WHERE id = 1;" }),
      " Use _snapshot for the whole table (needs a snapshot) and _diff for what changed between two moments. The user picks the server, so each server has its own line; pick another in the sidebar and copy again."),
    el("p", { class: "form-hint", text: fb.host
      ? "The port answers on that address only. Run mysql where it can reach it (on the machine DBTrail runs on when it is 127.0.0.1), or open a tunnel to it."
      : "The port answers on every network address of the machine DBTrail runs on; the command uses the name the web interface was opened with. If that name is a reverse proxy in front of DBTrail, it does not pass this port through, so use that machine's own name or address instead. In the Docker install, docker-compose.yml decides who can reach it: a new install publishes it to the machine itself only." })));
  return panel;
}

// ROUTE_REASON_TEXT turns the server's closed reason vocabulary (the shim's
// RouteReason* constants, also the metric's reason label) into a phrase. An
// unknown key (a newer daemon) is shown as is rather than dropped.
const ROUTE_REASON_TEXT = {
  expensive_plan: "an expensive plan: the copy answered",
  cheap_plan: "a cheap plan (a lookup or a small read)",
  bounded_limit: "a small LIMIT MySQL answers without reading past it",
  not_a_select: "not a SELECT (SHOW, BEGIN, COMMIT, …)",
  write: "a write (INSERT, UPDATE, DELETE, DDL)",
  session_setting: "a SET statement",
  settings_set: "a SET ran earlier on that connection",
  in_transaction: "inside a transaction",
  veto: "uses something the copy answers differently (LIKE, NOW(), @variables, …)",
  explain_failed: "MySQL could not explain it",
  copy_age_unknown: "the copy's age is unknown",
  copy_too_old: "the copy was older than the limit",
  copy_refused: "the copy refused it (a construct it lacks, a table it does not have, the row cap)",
  copy_columns_differ: "SELECT * or NATURAL JOIN over a table whose columns on the copy are not MySQL's",
  show_warnings: "SHOW WARNINGS after a MySQL statement",
  upstream_lost: "nobody answered: the port's connection to the source was lost (the client got error 2006)",
  routing_off: "routing off",
  read_only: "refused: not a read, and this port is read-only",
};

// fmtGoDuration shortens a Go duration string for display by dropping the
// zero units Go prints after a non-zero one: "15m0s" → "15m", "10m0s" →
// "10m", "1h0m0s" → "1h", "1h30m0s" → "1h30m"; "30s" and "1m30s" are shown
// as sent. The unit must precede the dropped part, or the pattern eats the
// trailing zero DIGIT of "10m0s".
function fmtGoDuration(d) { return String(d || "").replace(/(\d[hm])(?:0m)?0s$/, "$1"); }

// routingBlock (#2038) says who answers on the port: with read routing off,
// the copy, always (and how to turn routing on); with it on, the rule and,
// for the server picked in the sidebar, how many statements each side has
// answered since the daemon started and why MySQL took the ones it took. The
// counts travel with /api/flashback, so Refresh re-reads that one route and
// repaints this block alone; nothing else on the page moves.
function routingBlock(fb, cur) {
  const wrap = el("div", { class: "cn-route" });
  const paint = (status) => {
    clear(wrap);
    const r = status && status.routing;
    if (!r) return;
    if (!r.enabled) {
      wrap.append(el("p", { class: "cn-sql-row", text: "Who answers: the copy, always. Read routing is off." }));
      wrap.append(cnFine("How to let MySQL answer too",
        el("p", { class: "form-hint" },
          "Start DBTrail with a freshness limit for the copy (CLI: ", el("code", { text: "--route-max-copy-age 15m" }),
          ", or the environment variable ", el("code", { text: "BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE" }),
          "). Then everything sent to this port runs on the server's own MySQL, writes included, except the heavy SELECTs, which run on the copy while it is at most that old. To refuse writes, add ", el("code", { text: "--route-read-only" }),
          " (or ", el("code", { text: "BINTRAIL_CONSOLE_ROUTE_READ_ONLY=1" }), "). The mysql line above stays the same. Experimental.")));
      return;
    }
    const t = (cur && r.servers && r.servers[cur.id]) || { copy: 0, mysql: 0, reasons: {} };
    const total = (t.copy || 0) + (t.mysql || 0) + (t.refused || 0);
    const refresh = el("button", { class: "btn btn-sm", type: "button", text: "Refresh",
      onclick: () => api("/api/flashback").then(paint, (e) => toastError("Could not refresh who answered: " + ((e && e.message) || e))) });
    if (!cur) {
      wrap.append(el("p", { class: "cn-sql-row", text: "Who answered: pick a server in the left sidebar to see its counts." }));
      return;
    }
    if (t.unavailable) {
      // A connection to this server found it cannot route (no source to
      // forward to, or no SQL on the copy): the copy answers everything
      // sent under its name, and the rule below does not apply to it.
      const p = el("p", { class: "cn-sql-row" });
      p.append("Who answered", cur ? " for " : "", cur ? el("code", { text: cur.name || cur.id }) : "",
        ": the copy, always. Routing is off for this server: " + t.unavailable + ". ", refresh);
      wrap.append(p);
      return;
    }
    const since = r.since ? new Date(r.since) : null;
    const sinceText = since && !isNaN(since) ? " since " + since.toLocaleString() : "";
    const head = el("p", { class: "cn-sql-row" });
    head.append("Who answered", cur ? " for " : "", cur ? el("code", { text: cur.name || cur.id }) : "", sinceText + ": ");
    if (total === 0) {
      head.append(el("b", { text: "no statements yet" }));
    } else {
      head.append(el("b", { text: String(t.mysql || 0) }), " by MySQL, ", el("b", { text: String(t.copy || 0) }), " by the copy");
      if (t.refused) head.append(", ", el("b", { text: String(t.refused) }), " refused");
    }
    head.append(" ", refresh);
    wrap.append(head);
    const rules = [];
    if (r.cost_threshold > 0) rules.push("MySQL's plan costs at least " + r.cost_threshold);
    if (r.scan_rows > 0) rules.push("it scans a whole table of at least " + r.scan_rows + " rows");
    wrap.append(el("p", { class: "cn-sql-row" }, rules.length
      ? "A SELECT runs on the copy when " + rules.join(" or ") + ", while the copy is at most " + fmtGoDuration(r.max_copy_age) + " old. " +
        (r.read_only ? "Every other read runs on MySQL." : "Everything else, writes included, runs on MySQL.")
      : "No plan threshold is set (both are 0), so every " + (r.read_only ? "read" : "statement") + " runs on MySQL and nothing reaches the copy."));
    // Which mode the port is in (#2079), said either way: read-write is the
    // one a reader must not have to infer from a missing line.
    wrap.append(r.read_only
      ? el("p", { class: "cn-sql-row" }, el("b", { text: "This port is read-only." }),
        " A statement that would change the server (INSERT, UPDATE, DELETE, CREATE, DROP, GRANT and the like) is refused and never sent to MySQL. The check reads the statement's text; the permissions of the account below bound everything else.")
      : el("p", { class: "cn-sql-row" }, el("b", { text: "This port is read-write." }),
        " Writes sent to it run on MySQL. To refuse them, start DBTrail with ", el("code", { text: "--route-read-only" }), "."));
    // Which account runs what MySQL answers (#2079): the user name only.
    // has_route is the server's own verdict that the account is a separate
    // one: a saved value that is the capture account itself, or that cannot
    // be read, is said as what it is and never as a separation.
    if (cur.has_route) {
      // route_host is sent only when the account connects somewhere else
      // than the source.
      const where = cur.route_host ? " It connects to " + cur.route_host + (cur.route_port ? ":" + cur.route_port : "") + ", not the source's address." : "";
      wrap.append(el("p", { class: "cn-sql-row" }, "On MySQL, statements run as ",
        cur.route_user ? el("code", { text: cur.route_user }) : "this server's forwarding account",
        (cur.route_user ? ", this server's forwarding user." : ".") + where + " The account DBTrail captures with is not used by this port."));
    } else if (cur.route_unreadable) {
      wrap.append(el("p", { class: "cn-sql-row" }, el("b", { text: "This server's saved forwarding account cannot be read," }),
        " so this port cannot forward for it and does not fall back to the account DBTrail captures with. Edit the server and type the forwarding user and password again, or remove the forwarding account."));
    } else if (cur.has_source) {
      wrap.append(el("p", { class: "cn-sql-row" }, "On MySQL, statements run as ",
        cur.source_user ? el("code", { text: cur.source_user }) : "the source user",
        ", the account DBTrail captures with: this port can do whatever that account can. " +
        (cur.route_is_capture ? "The saved forwarding account is that same account, so nothing is separated. To separate them, edit the server and set another forwarding user."
          : "To use another one, edit the server and set a forwarding user.")));
    }
    // The source turned the port's login away (#2079): every statement of
    // such a connection fails with error 2006, which says nothing of why.
    if (t.account_refused) {
      wrap.append(el("p", { class: "cn-sql-row" }, el("b", { text: "MySQL refused the login of " + t.account_refused + "." }),
        " Clients of this port get error 2006 for this server until that is fixed. " +
        (cur.has_route ? "Edit the server to correct the forwarding account. Test connection on that form tries its login, and one that works clears this message."
          : "Edit the server to correct the source user or its password. This message goes when a client of the port logs in.")));
    }
    if (total > 0) {
      const reasons = Object.entries(t.reasons || {}).sort((x, y) => y[1] - x[1]);
      const ul = el("ul", { class: "form-hint", style: "margin:4px 0 0 18px; padding:0" });
      for (const [key, n] of reasons) {
        ul.append(el("li", { text: n + " × " + (ROUTE_REASON_TEXT[key] || key) }));
      }
      wrap.append(cnFine("Why each side", ul));
    }
  };
  paint(fb);
  return wrap;
}

// ── capabilities gating ────────────────────────────────────────────────────

async function gateCapabilities() {
  const gen = serverGen;
  let caps = {};
  // capsOK distinguishes "the server reported no version" from "we could not
  // ask" — the version row must never claim a build we failed to read (#1221).
  // It tracks a payload we actually READ, not merely a call that did not throw:
  // api() returns null for an empty body (a legitimate 204 elsewhere), which
  // would otherwise land here as a successful read of nothing.
  let capsOK = false;
  // Degrading to {} hides capability-gated UI (Time-travel tab, the source
  // section of the server form) — warn so a wrongly-shaped UI is diagnosable.
  // A 401 is NOT capability loss: rethrow so session expiry surfaces as the
  // sign-in gate (api() already raised it), never as silently vanished tabs.
  try { caps = await api("/api/capabilities"); capsOK = !!caps; } catch (err) {
    if (err && err.status === 401) throw err;
    console.warn("capabilities check failed; UI degrades to no-capability gating", err);
    caps = {};
  }
  if (gen !== serverGen) return;
  capsCache = caps || {};
  capsKnown = capsOK;
  // Extension views advertised for this server (embedding builds; empty in the
  // stock binary and under any active profile — the backend omits them there).
  // Rebuild the nav before the route renders so a deep-linked ext route resolves.
  extViews = Array.isArray(capsCache.extension_views) ? capsCache.extension_views : [];
  extSettings = Array.isArray(capsCache.extension_settings) ? capsCache.extension_settings : [];
  syncExtNav();
  syncExtSettingsNav();
  $all("[data-capability]").forEach((node) => node.classList.toggle("cap-on", !!capsCache[node.dataset.capability]));
  gatePermissions();
  applyAuthGate();
  updateSrvNote(); // capsCache.monitor may have just changed
  updateSideVersion(capsOK);
}

// updateSideVersion paints the running build into the sidebar footer (#1221),
// reading the capabilities payload already fetched above — no extra request.
// Two producers, both of which must survive without emitting "vundefined" or
// "vdev": a plain `go build` sends the LITERAL "dev" (cmd/bintrail-console
// defaults Version to it and consoleapp.Main passes it through unexamined),
// while `version` is also `omitempty`, so a Config with an empty Version — an
// embedder handing consoleapp.Main "" — sends no key at all.
// `known` is false when the capabilities fetch itself failed — the row keeps
// its "—" placeholder there instead of reporting "dev" for a build whose real
// version we never read (a wrong version in a bug report is worse than none).
// The leading-"v" strip is CLASSIFICATION, not formatting: it has no effect on
// the rendered string (a "v1.2.3" that skips the released branch is echoed
// verbatim by the fallback and reads the same), but it keeps a tag-shaped
// -ldflags value on the same side of the released/unreleased split that
// bundleCard() puts it on. That sibling anchors its regex and this one does
// not, deliberately: bundleCard derives a download URL that has to resolve, so
// it rejects "1.2.3-rc1"; this row only echoes what the server reported.
function updateSideVersion(known) {
  const b = $("#meta-version b");
  if (!b) return;
  const ver = String(capsCache.version || "").replace(/^v/, "");
  if (!known) b.textContent = "—";
  else b.textContent = /^\d+\.\d+\.\d+/.test(ver) ? "v" + ver : (ver || "dev");
}

// gatePermissions hides a [data-perm] surface when the session's policy denies
// that permission (per-session RBAC, #1074). Default is VISIBLE: a policy-less
// session — the static token, the password login, every OSS session — reports
// every permission true, so nothing is hidden and the UI is unchanged. A missing
// permissions map (a degraded {} capabilities response) also leaves everything
// visible: only an explicit `false` hides. The server's 403 is the real gate;
// this just spares a scoped user a tab that would only error.
// sessionMay reports whether this session holds a permission, for the parts
// of a page that are drawn in code rather than tagged data-perm. Same
// convention as gatePermissions: only an explicit false hides. A session with
// no access policy is sent every permission as true, and a capability read
// that failed is reported by its own line on the page, not by hiding
// everything (#1573 step 7).
//
// Hiding here is presentation only. The server refuses every one of these
// routes on its own; what this saves is a reader being offered a button that
// can only fail, or a red box about a section they were never meant to see.
function sessionMay(p) {
  return (capsCache.permissions || {})[p] !== false;
}

// sessionMayConfigureServer: whether this session can reach AND change a
// server's snapshot location — it has to read the settings half of the page
// to see the row, and write servers to save it. Every hint that says "set it
// under Where and how often" is a fix only for such a session; to anyone
// else it points at a section they cannot see or a Save they do not have.
function sessionMayConfigureServer() {
  return sessionMay("settings:read") && sessionMay("servers:write");
}

// The permission that starts a snapshot, a restore, a .sql build or a
// check. Its name is frozen (#1573 keeps every permission name), and it is
// written once so the vocabulary ratchet counts it once.
const PERM_SNAPSHOT_CREATE = "baseline:create";

function gatePermissions() {
  const perms = capsCache.permissions || {};
  $all("[data-perm]").forEach((node) => node.classList.toggle("perm-off", perms[node.dataset.perm] === false));
}

// syncExtNav rebuilds the extension-view nav group from extViews. Idempotent
// across server switches: it drops the previously-injected group first, so a
// server that exposes fewer (or no) extension views cannot leave stale items
// behind. The group is anchored right after the built-in Monitor group (the one
// holding Status). el()/textContent only — a view label is never markup.
function syncExtNav() {
  const prev = document.getElementById("ext-nav-group");
  if (prev) prev.remove();
  if (!extViews.length) return;
  const nav = document.getElementById("nav");
  if (!nav) return;
  const group = el("div", { class: "nav-group", id: "ext-nav-group", "data-ext-nav": "1" });
  group.append(el("div", { class: "nav-label", text: "Extensions" }));
  for (const v of extViews) {
    const route = "ext-" + v.id;
    const item = el("a", {
      class: "nav-item",
      "data-ext-nav": "1",
      "data-route": route,
      href: "/" + route,
      onclick: (e) => { e.preventDefault(); pendingRecover = null; navigate(route); },
    }, icon("ext", "ni-icon"), el("span", { text: v.label }));
    group.append(item);
  }
  const statusItem = $('.nav-item[data-route="status"]');
  const anchor = statusItem ? statusItem.closest(".nav-group") : null;
  if (anchor && anchor.parentNode) anchor.parentNode.insertBefore(group, anchor.nextSibling);
  else nav.append(group);
}

// renderExtensionView loads a provider's ES module and hands it a mount node and
// a small context: apiBase ("/api/ext/<id>/") and the console's own authed fetch
// primitive (api), so the module reads its data routes with the operator's
// bearer credential and selected-server header already applied. serverGen is
// captured before the dynamic import and re-checked after: a server switch
// mid-import abandons the render (no cross-server paint), matching every other
// async view here.
// syncExtSettingsNav rebuilds the extension settings-panel nav items inside the
// existing Settings group (not a group of their own: a panel administers the
// console, so it belongs where MCP Server and Retention already are). They go
// right after Access profiles, in registration order (#1863): the panels a
// commercial build registers are mostly about who may see what, so they sit
// with the built-in entry that answers the same question, and a build that
// wants one entry last (a license card) registers it last. Idempotent — the
// previously injected items are removed first, so a capabilities refresh
// after a server switch or a re-login never accumulates duplicates.
function syncExtSettingsNav() {
  $all("[data-extset-nav]").forEach((n) => n.remove());
  if (!extSettings.length) return;
  let after = $('.nav-item[data-route="access-profiles"]');
  if (!after) return;
  for (const p of extSettings) {
    const route = "extset-" + p.id;
    const item = el("a", {
      class: "nav-item",
      "data-extset-nav": "1",
      "data-route": route,
      // settings:read is the visibility floor server-side, so the item hides
      // under a session that lacks it — the same data-perm contract every
      // built-in nav item uses. Without this the link stays lit for a scoped
      // operator and the panel's first request answers 403. syncExtSettingsNav
      // runs immediately before gatePermissions(), so these nodes are in the
      // DOM when the sweep reads [data-perm].
      "data-perm": "settings:read",
      href: "/" + route,
      onclick: (e) => { e.preventDefault(); pendingRecover = null; navigate(route); },
    }, icon("ext", "ni-icon"), el("span", { text: p.label }));
    after.after(item);
    after = item;
  }
}

// extContract is what an extension module receives. Both extension surfaces —
// the settings panel and the full view — build it here, so the two cannot
// drift into different contracts for the same kind of consumer.
//
// `apiBase` and `api` are the data plane. `ui` is the small set of console
// widgets an extension should NOT be reimplementing: a second copy drifts, and
// the operator ends up looking at two different date pickers in one console.
//
// Handed over explicitly rather than left to be discovered. app.js is a classic
// script, so its function declarations are reachable as window globals and an
// extension COULD simply call one — which is exactly the coupling to avoid.
// The two sides are built in different repos on different release cadences and
// never compile together, so a rename here would break the extension with no
// error at all: the widget would just stop appearing. What is in `ui` is a
// promise the console keeps; what is not is an internal that may move.
//
// dateField is fieldDateInput itself rather than a wrapper, so a rename cannot
// leave the two spellings pointing at different builders.
function extContract(apiBase) {
  return { apiBase, api, ui: { dateField: fieldDateInput } };
}


// renderExtensionSettings mounts a settings panel's ES module. Same contract as
// renderExtensionView, with the panel's own data prefix — the two surfaces are
// authorized differently server-side (settings:read/write vs extview:read), so
// handing a panel the view's apiBase would send its requests through the wrong
// gate and 403.
async function renderExtensionSettings(panel) {
  const gen = serverGen;
  const v = VIEW();
  clear(v);
  v.append(pageHead(panel.label, null));
  const mount = el("div", { class: "ext-view-mount" });
  v.append(mount);
  try {
    const mod = await import(panel.script);
    if (gen !== serverGen) return;
    if (!mod || typeof mod.render !== "function") {
      renderError(mount, "This settings panel did not export a render() function.");
      return;
    }
    mod.render(mount, extContract("/api/ext-settings/" + panel.id + "/"));
  } catch (err) {
    if (gen !== serverGen) return;
    renderError(mount, err);
  }
}

async function renderExtensionView(view) {
  const gen = serverGen;
  const v = VIEW();
  clear(v);
  v.append(pageHead(view.label, null));
  const mount = el("div", { class: "ext-view-mount" });
  v.append(mount);
  try {
    const mod = await import(view.script);
    if (gen !== serverGen) return;
    if (!mod || typeof mod.render !== "function") {
      renderError(mount, "This extension view did not export a render() function.");
      return;
    }
    mod.render(mount, extContract("/api/ext/" + view.id + "/"));
  } catch (err) {
    if (gen !== serverGen) return;
    renderError(mount, err);
  }
}

// ── server registry: switcher + modal CRUD ──────────────────────────────────

// serverLabel: the ephemeral boot entry's name is the reserved id "default",
// which reads as meaningless in the switcher — label it by its database name
// (what the entry actually is: the daemon's own index DB from --index-dsn).
function serverLabel(s) {
  if (s.kind === "ephemeral") return (s.dbname || s.name) + " (cli)";
  return s.name;
}

// serverNames: id to name, as last listed, for a message that has only the id.
let serverNames = new Map();

async function loadServers() {
  const data = await api("/api/servers");
  defaultServerId = data.default_id || "";
  const servers = data.servers || [];
  serverNames = new Map(servers.map((s) => [s.id, s.name]));
  // Reconcile a stale selection (server deleted elsewhere).
  if (currentServer && !servers.some((s) => s.id === currentServer)) setCurrentServer("");
  serversEmpty = !servers.length;
  updateSrvNote();
  const sel = document.getElementById("server-select");
  if (sel) {
    clear(sel);
    if (!servers.length) {
      // No listed servers: a hidden-boot fresh install (source-less watch),
      // or a registry-only console whose last entry was deleted.
      const o = opt("", "no servers yet");
      o.disabled = true;
      sel.append(o);
      sel.value = "";
    } else {
      // Registry servers first; the ephemeral boot entry goes last (it shows
      // only where it carries data: serve, or watch with --source-dsn).
      const ordered = servers.filter((s) => s.kind !== "ephemeral")
        .concat(servers.filter((s) => s.kind === "ephemeral"));
      ordered.forEach((s) => {
        const o = opt(s.id, serverLabel(s) + (s.flavor && s.flavor !== "mysql" ? " · " + (s.flavor === "postgres" ? "PG" : "MariaDB") : ""));
        if (s.kind === "ephemeral") o.title = "DBTrail's own index database (set with --index-dsn on the command line)";
        sel.append(o);
      });
      sel.value = currentServer || defaultServerId;
    }
  }
  return servers;
}

async function switchServer(id) {
  setCurrentServer(id);
  serverGen++;
  schemaCache = null;
  tablesCache.clear();
  // A switch is a fresh context: drop any carried undo-context and SQL so they
  // can never be auto-applied against a different server's index.
  pendingRecover = null;
  lastSQL = "";
  try { await gateCapabilities(); } catch (err) {
    if (err && err.status === 401) return; // chokepoint already raised the sign-in gate
    throw err;
  }
  renderRoute(); // re-render the current screen for the new server
}

// modal -----------------------------------------------------------------------

function buildServersModal() {
  const scrim = el("div", { class: "modal-scrim show" });
  const modal = el("div", { class: "modal", role: "dialog", "aria-label": "Servers" });

  const head = el("div", { class: "modal-head" });
  head.append(el("h2", { class: "modal-title", text: "Servers" }));
  const desc = el("p", { class: "modal-desc" },
    "The servers you're monitoring with DBTrail, saved in a file on this machine. ");
  desc.append(el("span", { "data-capability": "monitor" },
    "This process can also ", el("b", { text: "monitor" }),
    " a new MySQL database for you: add one below and DBTrail checks it's ready, sets up its index, and starts capturing changes; no terminal needed."));
  head.append(desc);
  head.append(el("button", { class: "btn btn-icon btn-ghost modal-x", type: "button", text: "✕", onclick: closeServersModal }));
  modal.append(head);

  const body = el("div", { class: "modal-body" });
  body.append(el("div", { class: "srv-list", id: "servers-list" }));
  // A session that may not add servers is not offered the button (the server
  // refuses the add anyway). The wrap stays: the form code hides and shows it.
  body.append(el("div", { id: "server-add-wrap", style: "margin-top:18px" },
    sessionMay("servers:write") ? el("button", { class: "btn btn-primary", type: "button", id: "server-add", text: "+ Add server", onclick: () => showServerForm(null) }) : null));
  body.append(el("div", { id: "server-form-mount" }));
  modal.append(body);

  scrim.append(modal);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) closeServersModal(); });
  return scrim;
}

function openServersModal() {
  const mount = document.getElementById("modal");
  clear(mount);
  mount.append(buildServersModal());
  // re-apply capability gating to the freshly-mounted [data-capability] nodes
  $all("[data-capability]", mount).forEach((n) => n.classList.toggle("cap-on", !!capsCache[n.dataset.capability]));
  focusModal(mount);
  refreshServersList();
}
// Closing the dialog renders the Overview again, so a server just added or
// started shows its Getting started list when it is the selected server, as
// the first server on a fresh install is (#1606).
function closeServersModal() {
  document.getElementById("modal").replaceChildren();
  if (routeFromLocation() === "overview") renderRoute();
}

// ── rotation settings ────────────────────────────────────────────────────────

// showRotationDialog edits the daemon-global built-in rotation policy (retain /
// interval / future partitions). Changes apply live — the watch loop re-reads
// them on its next cycle. Only reachable when this process is a supervisor
// (capsCache.monitor gates the ⌘K entry); the read-only console has no loop to
// tune and the PUT 403s there anyway.
async function showRotationDialog() {
  if (loginGateRaised) return;
  let cur;
  try { cur = await api("/api/rotation"); }
  catch (err) { toastError("Could not load rotation settings: " + ((err && err.message) || err)); return; }

  const mount = document.getElementById("modal");
  const scrim = el("div", { class: "modal-scrim show" });
  const modal = el("div", { class: "modal", role: "dialog", "aria-label": "Rotation" });

  const head = el("div", { class: "modal-head" });
  head.append(el("h2", { class: "modal-title", text: "Rotation" }));
  head.append(el("button", { class: "btn btn-icon btn-ghost modal-x", type: "button", text: "✕", onclick: closeRotationDialog }));
  modal.append(head);

  const form = el("form", { class: "modal-body" });
  // What the fields set, drawn from their values as they change (#1950); the
  // sentence it replaced is its text alternative.
  const shape = el("div", { class: "rot-shape" });
  const redraw = () => { clear(shape); shape.append(rotationTiles(form.elements.retain.value.trim(), form.elements.interval.value.trim(), form.elements.add_future.value.trim())); };
  form.append(shape);
  const grid = el("div", { class: "form-grid" });
  grid.append(srvField("Retention", "retain", { placeholder: "e.g. 30d, 24h" }));
  grid.append(srvField("Interval", "interval", { placeholder: "e.g. 1h, 30m" }));
  grid.append(srvField("Future partitions", "add_future", { placeholder: "e.g. 3" }));
  form.append(grid);
  form.elements.retain.value = cur.retain || "";
  form.elements.interval.value = cur.interval || "";
  form.elements.add_future.value = (cur.add_future != null ? cur.add_future : "");
  redraw();

  const note = el("p", { class: "form-hint", style: "margin-top:10px" });
  if (!cur.enabled) note.textContent = "Rotation is turned off. Your changes will be saved but won't take effect until DBTrail restarts.";
  else if (cur.source === "default") note.textContent = "Currently using DBTrail's built-in defaults. Saving creates a custom setting that takes effect immediately.";
  else note.textContent = "A custom setting is active and takes effect immediately.";
  form.append(note);

  form.append(el("p", { class: "form-hint", text: "One schedule for every monitored server; changes take effect on the next run." }));
  const msg = el("div", { class: "form-msg" });
  const foot = el("div", { class: "modal-foot" });
  // Plain at rest, filled once a field changes (#1950): the one primary is
  // the control being changed.
  const saveBtn = el("button", { class: "btn", type: "submit", text: "Save" });
  foot.append(saveBtn);
  foot.append(el("button", { class: "btn btn-ghost", type: "button", text: "Cancel", onclick: closeRotationDialog }));
  form.append(foot);
  form.append(msg);
  form.addEventListener("input", () => { saveBtn.classList.add("btn-primary"); redraw(); });
  form.addEventListener("submit", (e) => { e.preventDefault(); submitRotation(form, msg, cur); });
  modal.append(form);

  scrim.append(modal);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) closeRotationDialog(); });
  mount.replaceChildren(scrim);
  focusModal(scrim);
}

function closeRotationDialog() { document.getElementById("modal").replaceChildren(); }

async function submitRotation(form, msg, cur) {
  // Future partitions (#969): blank keeps the current value — the field came
  // prefilled, so an accidental clear must not silently save 0. Non-integer
  // input is rejected inline (mirroring the server's retain/interval 400s);
  // an explicit "0" stays valid (external partition management).
  const rawFuture = form.elements.add_future.value.trim();
  let addFuture;
  if (rawFuture === "") {
    addFuture = cur.add_future != null ? cur.add_future : 0;
  } else if (/^\d+$/.test(rawFuture)) {
    addFuture = parseInt(rawFuture, 10);
  } else {
    msg.textContent = "Future partitions must be a whole number (e.g. 3).";
    msg.className = "form-msg err";
    return;
  }
  const body = {
    retain: form.elements.retain.value.trim(),
    interval: form.elements.interval.value.trim(),
    add_future: addFuture,
  };
  try {
    await api("/api/rotation", { method: "PUT", body });
  } catch (err) {
    msg.textContent = (err && err.message) || String(err);
    msg.className = "form-msg err";
    return;
  }
  closeRotationDialog();
  // When the daemon booted with rotation off the loop isn't running, so the
  // save is inert until a restart — say so rather than implying it took effect.
  toast(cur.enabled ? "Rotation settings saved" : "Saved. Rotation is off, so this takes effect when DBTrail restarts");
}

// Nothing else in this dialog polls: the list is fetched when it opens and
// after an operator action. That is fine for a state, and wrong for a PHASE
// (#1690), because a phase names a step that ends. A frozen CLEANING UP would
// assert the daemon is still stuck in that exact step half an hour after it
// finished, which is the misreading the chip exists to prevent — worse than
// the vague frozen PENDING it replaced, because it is specific.
//
// So the list re-fetches itself while some row is still settling: it reports
// a phase, or its state changes on its own (SERVERS_SETTLING, #1992). A job is pending from launch until its
// first checkpoint, and that window often has no named step (a plain connect
// with no snapshot step); a frozen PENDING with only a Stop button reads as
// stuck, and pressing Stop to unstick it stops a healthy capture. The timer
// exists exactly as long as such a row does, and the guard below stops it the
// moment the list leaves the DOM.
// SERVERS_SETTLING are the states that change on their own, so a row in one
// of them must not freeze: pending flips to running at the first checkpoint,
// failed is retried after a backoff (consoleapp/monitor.go fail/retrying,
// and its tooltip says "retrying automatically"), and a stalled stream
// recovers when it makes progress again. lost_position, running and stopped
// change only through an operator action, which refreshes the list itself.
const SERVERS_SETTLING = new Set(["pending", "failed", "stalled"]);
let serversPhaseTimer = null;
const serversPhaseInterval = 5000;

// SERVERS_EMPTY_ART: a database with a dashed link to DBTrail, not made yet
// (static, so svgEl is right here).
const SERVERS_EMPTY_ART = `<svg viewBox="0 0 160 72" aria-hidden="true"><ellipse cx="40" cy="18" rx="24" ry="8" fill="var(--surface)" stroke="var(--ink-4)" stroke-width="1.5"/><path d="M16 18v34c0 4.4 10.7 8 24 8s24-3.6 24-8V18" fill="var(--surface)" stroke="var(--ink-4)" stroke-width="1.5"/><path d="M16 35c0 4.4 10.7 8 24 8s24-3.6 24-8" fill="none" stroke="var(--line)" stroke-width="1.5"/><path d="M72 38h30" stroke="var(--ink-4)" stroke-width="2" stroke-dasharray="4 4"/><rect x="108" y="20" width="44" height="36" rx="9" fill="none" stroke="var(--ink-4)" stroke-width="1.5" stroke-dasharray="4 3"/><path d="M130 31v14M123 38h14" stroke="var(--ink-3)" stroke-width="2"/></svg>`;

async function refreshServersList() {
  const list = document.getElementById("servers-list");
  if (serversPhaseTimer) { clearTimeout(serversPhaseTimer); serversPhaseTimer = null; }
  if (!list) return;
  let servers;
  try { servers = await loadServers(); }
  catch (err) { renderError(list, err); return; }
  clear(list);
  if (!servers.length) {
    // The .empty component with a small drawing (#1950), the same words.
    list.append(el("div", { class: "empty ev-empty srv-empty" },
      el("div", { class: "empty-art", "aria-hidden": "true" }, svgEl(SERVERS_EMPTY_ART)),
      el("p", { text: "No servers yet. Add your first connection." })));
    return;
  }
  servers.forEach((s) => list.append(serverRow(s)));
  if (servers.some((s) => s.monitor_phase || SERVERS_SETTLING.has(s.monitor_state))) serversPhaseTimer = setTimeout(refreshServersList, serversPhaseInterval);
}

function isLiveMonitorState(st) { return st === "running" || st === "pending" || st === "stalled" || st === "lost_position"; }

function serverRow(s) {
  const item = el("div", { class: "srv-item" });
  const nm = el("span", { class: "nm" },
    el("span", { class: "health-dot" + (s.connected ? " ok" : ""), title: s.connected ? "connected" : "not connected yet" }),
    serverLabel(s));
  item.append(nm);
  if (s.kind === "ephemeral") item.append(el("span", { class: "chip chip-cli", text: "CLI", title: "Set from the command line with --index-dsn" }));
  if (s.reconstruct) item.append(el("span", { class: "chip chip-tt", text: "TT", title: "Snapshot configured: Time-travel available" }));
  if (s.monitor_state) item.append(monitorChip(s));
  if (s.flavor && s.flavor !== "mysql") item.append(el("span", { class: "chip", text: s.flavor === "postgres" ? "PG" : s.flavor.toUpperCase(), title: "Source type: " + s.flavor }));
  // A registry entry with no source connection under a capturing console
  // never streams; the mark says so where the Start button would be (#1607).
  const noSource = s.kind !== "ephemeral" && capsKnown && capsCache.monitor && !s.has_source;
  if (noSource) item.append(el("span", { class: "chip chip-nosrc", text: "NO SOURCE", title: "No source connection: nothing is captured from this server. Edit it and add one." }));

  let desc;
  if (s.has_source && s.source_host) desc = "watching " + s.source_user + "@" + s.source_host + ":" + (s.source_port || (s.flavor === "postgres" ? "5432" : "3306")) + (s.source_database ? "/" + s.source_database : "") + (s.schemas ? " [" + s.schemas + "]" : "");
  else if (s.host) desc = s.user + "@" + s.host + ":" + (s.port || "3306") + "/" + s.dbname;
  else desc = s.dbname || "";
  item.append(el("span", { class: "srv-desc conn", text: desc }));

  const note = noCaptureNotes[s.id] && noCaptureReason(s);
  // data-nosrc: the Test result on this row says the same thing beside its
  // "index ok", so the two facts read as two connections (#1856).
  item.append(el("span", { class: "srv-status" + (note ? " pending" : ""), id: "srv-status-" + s.id, text: note ? "○ " + note : "", "data-nosrc": noSource ? "1" : null }));

  const acts = el("span", { class: "acts row-acts" });
  const monitorable = capsCache.monitor && s.has_source && s.kind !== "ephemeral";
  if (monitorable) {
    const running = isLiveMonitorState(s.monitor_state);
    acts.append(el("button", { class: "btn btn-sm", type: "button", text: running ? "Stop" : "Start",
      onclick: () => running ? stopMonitorRow(s.id) : startMonitorRow(s.id) }));
  }
  acts.append(el("button", { class: "btn btn-sm", type: "button", text: "Test", onclick: () => testServerRow(s.id) }));
  acts.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Edit", disabled: !s.editable, onclick: () => editServer(s.id) }));
  acts.append(el("button", { class: "btn btn-sm btn-danger", type: "button", text: "Delete", disabled: !s.deletable, onclick: () => deleteServer(s) }));
  item.append(acts);
  return item;
}

// server form (add/edit) ------------------------------------------------------

// srvField builds a labeled input row: <label class="field"><span/><input/></label>.
// FORWARDING_HINT explains the two optional forwarding fields of the server
// form (#2079) in the form's own words.
const FORWARDING_HINT = "Forwarding user and password are optional. They only matter when DBTrail was started with read routing: what a SQL client sends to the MySQL port then runs on this server as the forwarding user, not as the source user, so the forwarding user's permissions are all that port can do here. Give it SELECT only for a port that cannot change anything. Blank forwards with the source user. Saving a change to the forwarding account closes the connections open on that port for this server, so that none stays on the previous account; clients reconnect.";

// routePrefill remembers, per form, the forwarding account its server had
// when the form opened: serverFormBody sends the forwarding fields only when
// they differ from it.
const routePrefill = new WeakMap();

// routeFormProblem stops a save or a test that would do something else than
// the reader meant with the forwarding account; serverFormBody sends nothing
// about the account while it answers. Emptying the user of a saved account
// is not a removal: the control below the fields is. Remove ticked beside a
// typed user or password would throw one of the two away. A password typed
// with the user emptied would be set on the saved user, which the form no
// longer shows.
function routeFormProblem(form) {
  const f = form.elements, was = routePrefill.get(form) || { user: "" };
  if (!f.route_user || f.flavor.value === "postgres") return "";
  const user = f.route_user.value.trim(), typedPassword = f.route_password.value !== "";
  if (f.route_remove && f.route_remove.checked) {
    return typedPassword || (user !== "" && user !== was.user)
      ? "Remove the forwarding account is ticked, and a forwarding user or password was typed too. Untick it to save what you typed, or clear what you typed to remove the account."
      : "";
  }
  if (user === "" && typedPassword) {
    return was.user
      ? "Forwarding user is empty and a forwarding password was typed. Put " + was.user + " back to change its password, or type the user the password belongs to."
      : "A forwarding password was typed with no Forwarding user. Type the user it belongs to.";
  }
  if (user === "" && was.user) {
    return "Forwarding user is empty. To remove the forwarding account, tick Remove the forwarding account; to keep it, put " + was.user + " back.";
  }
  return "";
}

function srvField(label, name, opts) {
  opts = opts || {};
  return el("label", { class: "field" },
    el("span", { class: "field-label", text: label }),
    el("input", { class: "input", name, type: opts.type || "text", placeholder: opts.placeholder || "", autocomplete: opts.autocomplete }));
}

// tagFlavor marks a form node visible only for the given space-separated source
// families (e.g. "postgres" or "mysql mariadb"); applyFlavor toggles .flavor-on.
function tagFlavor(node, families) { node.setAttribute("data-flavor", families); return node; }

// applyFlavor reveals the [data-flavor] nodes matching the selected source
// family, mirroring the capability cap-on gating EXACTLY — a class toggle over a
// CSS :not() default-hide, never a [hidden]/style="display:none" toggle (the
// documented display-bug class this codebase already hit).
function applyFlavor(form) {
  const f = (form.elements.flavor && form.elements.flavor.value) || "mysql";
  $all("[data-flavor]", form).forEach((n) =>
    n.classList.toggle("flavor-on", n.dataset.flavor.split(" ").includes(f)));
}

// sqlString quotes a value as a MySQL string literal. Backslashes first: under
// the default sql_mode a backslash escapes, so 'a\' would swallow its own
// closing quote.
function sqlString(s) {
  return "'" + String(s).replace(/\\/g, "\\\\").replace(/'/g, "''") + "'";
}

// genSourcePassword makes the password the grant block creates the capture
// user with. The block is copied and run as written, so a literal password in
// it becomes a real password anyone can read in this file. It has one of each
// class validate_password's MEDIUM policy asks for, then 20 more, shuffled,
// and nothing that needs quoting in SQL or a shell. Without a cryptographic
// random source it returns "" rather than a guessable password; the block
// then shows a placeholder MySQL refuses.
function genSourcePassword() {
  const c = globalThis.crypto;
  if (!c || typeof c.getRandomValues !== "function") return "";
  const lower = "abcdefghijkmnopqrstuvwxyz", upper = "ABCDEFGHJKLMNPQRSTUVWXYZ", digit = "23456789", sym = "-_.";
  const pick = (set, n) => {
    const r = new Uint32Array(n);
    c.getRandomValues(r);
    return Array.from(r, (x) => set[x % set.length]);
  };
  const chars = [...pick(lower, 1), ...pick(upper, 1), ...pick(digit, 1), ...pick(sym, 1), ...pick(lower + upper + digit + sym, 20)];
  const r = new Uint32Array(chars.length);
  c.getRandomValues(r);
  for (let i = chars.length - 1; i > 0; i--) {
    const j = r[i] % (i + 1);
    [chars[i], chars[j]] = [chars[j], chars[i]];
  }
  return chars.join("");
}

// grantBlocks builds the SQL the add-server form shows for MySQL and MariaDB,
// for the user and password in the form, so what is copied is what is saved.
// A blank user falls back to 'dbtrail' in the text (never ''@'%', the
// anonymous user), and a blank password to a placeholder MySQL refuses. Both
// cases also comment the whole block out: see the reasons further down.
//
// The backup line is here even though the stream does not need it, because
// omitting it is a DELAYED failure: capture starts clean and only Create
// backup refuses, hours or days later. The form is the last place anyone
// reads a grant list before pasting it.
//
// The backup line is what the DEFAULT lock mode (ftwrl) checks for
// (internal/mydumperlock/privileges.go): RELOAD, plus BACKUP_ADMIN on MySQL
// and Percona 8.0 or later. LOCK TABLES is only what lock-all needs, which is
// the RDS/Aurora path: the commented alternative (#1658), or the live line
// when managed is set (the Connect screen's RDS box, #1953).
function grantBlocks(user, password, hasSavedPassword, managed, connect) {
  const typedUser = String(user || "").trim();
  const acct = sqlString(typedUser || "dbtrail") + "@'%'";
  // A placeholder, not '': an empty quoted string is a real password MySQL
  // accepts, and the block is one uncomment away from creating an account
  // with it. Unquoted, the line is a syntax error however it is run.
  const secret = password ? sqlString(password) : "<choose a password>";
  const grantBase =
    "CREATE USER " + acct + " IDENTIFIED BY " + secret + ";\n" +
    "-- Created it already on an earlier try? Run this instead of CREATE USER:\n" +
    "-- ALTER USER " + acct + " IDENTIFIED BY " + secret + ";\n" +
    "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO " + acct + ";\n";
  // Managed services cannot use the default lock mode (no BACKUP_ADMIN on
  // managed MySQL; RDS MariaDB's RELOAD excludes FLUSH TABLES WITH READ LOCK),
  // so they switch to lock-all, which locks tables instead of the instance.
  // The managed alternative, commented. True on both screens that show it:
  // the Connect screen (where ticking the RDS box makes it the live line)
  // and the full form, which has no box. Snapshots pick the matching mode on
  // their own (#1986), so nothing here asks for a setting.
  // On the Connect screen the box under the SQL swaps in the RDS block, so
  // the line only points at it (#1986, checked: the box sits in the row right
  // below the SQL).
  // MariaDB names only RDS: Aurora does not run MariaDB.
  const grantLockAll = (who, where) => connect
    ? "-- On " + where + "? Tick the box below."
    : "-- " + who + ", run this line instead of the GRANT RELOAD line above.\n" +
      "-- GRANT LOCK TABLES, SHOW VIEW ON *.* TO " + acct + ";";
  // SHOW VIEW is on every backup line: mydumper stops at the first view it
  // cannot read ("SHOW VIEW command denied"), so a schema holding one view
  // fails the whole backup on RELOAD alone.
  const grantBackups = "-- Snapshots (point-consistent by default). SHOW VIEW lets the snapshot copy views.\n";
  // On Amazon RDS or Aurora (#1953) the lock-all line is the live one: the
  // default lock mode cannot work there at all, so offering it would only
  // fail on the first snapshot.
  const grantManaged = (who) =>
    "-- " + who + ": this permission lets each snapshot start every table at the same point-in-time.\n" +
    "GRANT LOCK TABLES, SHOW VIEW ON *.* TO " + acct + ";";
  const blocks = managed ? {
    mysql: grantBase + grantBackups + grantManaged("Amazon RDS and Aurora"),
    mariadb: grantBase + grantBackups + grantManaged("Amazon RDS for MariaDB"),
  } : {
    mysql: grantBase + grantBackups +
      "-- BACKUP_ADMIN is MySQL/Percona 8.0 or later. On MySQL 5.7 run this instead:\n" +
      "-- GRANT RELOAD, SHOW VIEW ON *.* TO " + acct + ";\n" +
      "GRANT RELOAD, BACKUP_ADMIN, SHOW VIEW ON *.* TO " + acct + ";\n" + grantLockAll("On Amazon RDS, Aurora or Cloud SQL", "Amazon RDS or Aurora"),
    mariadb: grantBase + grantBackups +
      "GRANT RELOAD, SHOW VIEW ON *.* TO " + acct + ";\n" + grantLockAll("On Amazon RDS for MariaDB", "Amazon RDS"),
  };
  // Three cases where no line may be runnable, because the block would not
  // create the account the form is about to save.
  //
  // No password: on MariaDB, or MySQL 5.7 whose sql_mode lacks
  // NO_AUTO_CREATE_USER, a pasted block carries on past a refused CREATE USER
  // and the GRANT lines create the account themselves, with NO password.
  // No user: the account the block names would not be the one saved, since a
  // blank field means "keep the stored user" on an edit.
  // A backslash: whether it escapes depends on the server's
  // NO_BACKSLASH_ESCAPES, so the password MySQL stores could differ from the
  // one the form saves. Every line is commented out, under the reason.
  let why = "";
  if (!typedUser) why = "-- Fill in the source user. The SQL to run appears here with it.";
  else if (!password) {
    why = hasSavedPassword
      ? "-- Leave the password above blank to keep the saved one. Type a new one and the SQL to set it appears here."
      : "-- Fill in the source password. The SQL to run appears here with it.";
  } else if (/\\/.test(typedUser + password)) why = "-- The user or password has a backslash, which your server's sql_mode may read as an escape. Choose one without it.";
  if (why) {
    for (const k of Object.keys(blocks)) {
      blocks[k] = why + "\n" + blocks[k].split("\n").map((l) => (l.startsWith("--") ? l : "-- " + l)).join("\n");
    }
  }
  return blocks;
}

// refreshGrants redraws the grant blocks from the form's user and password.
function refreshGrants(form) {
  const f = form.elements;
  const managed = !!(f.cx_managed && f.cx_managed.checked);
  const b = grantBlocks(f.source_user.value, f.source_password.value, !!savedSourcePasswords.get(form), managed, !!form.dataset.connect);
  // The Connect screen shows one block, for the flavor step 1 found or chose.
  if (form.dataset.connect) $all("pre[data-grant]", form).forEach((p) => p.setAttribute("data-grant", f.flavor.value === "mariadb" ? "mariadb" : "mysql"));
  $all("pre[data-grant]", form).forEach((p) => { p.textContent = b[p.dataset.grant]; });
  // Built once with the Connect screen and only shown or hidden here, so a
  // reader who opened it does not see it snap shut on the next keystroke.
  const pit = $("details.cx-pit", form);
  if (pit) pit.hidden = !managed;
}

// generatedPasswords remembers, per form, the password applyGrantDefaults
// filled in, so a value nobody changed can be told from one somebody typed.
// Kept here rather than in a data- attribute: it is a secret.
const generatedPasswords = new WeakMap();

// pendingSourcePassword is the generated password for the next new server,
// kept for this page until a new server is saved with it. Someone who runs
// the block, closes the form (Cancel, Escape) and opens it again must find
// the same password, or the user they created no longer matches. A reload
// loses it: the block's commented ALTER USER line covers that case.
let pendingSourcePassword = "";

// grantDefaultsApply reports whether the grant block is one this form can fill
// in: a new server, on a process that captures, for MySQL or MariaDB. The
// PostgreSQL block creates no role, an edit keeps its stored password, and a
// process that cannot capture hides the whole source section.
function grantDefaultsApply(form) {
  const f = form.elements;
  return !f.id.value && !!capsCache.monitor && (f.flavor.value === "mysql" || f.flavor.value === "mariadb");
}

// savedSourcePasswords marks the forms whose entry already has a password
// stored, where a blank field means "keep it" rather than "none".
const savedSourcePasswords = new WeakMap();

// The two halves of "nobody has changed this". They are asked SEPARATELY,
// because the fields are changed separately: someone who types their own user
// name and then switches to PostgreSQL must still have the generated password
// taken back, and clicking that field must still select it.
function untouchedGrantPassword(form) {
  const gen = generatedPasswords.get(form);
  return !!gen && form.elements.source_password.value === gen;
}
function untouchedGrantUser(form) {
  return !!generatedPasswords.get(form) && form.elements.source_user.value === "dbtrail";
}
// untouchedGrantDefaults is the pair: an account entirely filled in by this
// form, which is what a save may leave behind.
function untouchedGrantDefaults(form) {
  return untouchedGrantUser(form) && untouchedGrantPassword(form);
}

// applyGrantDefaults fills in, or takes back, the account the grant block
// creates. Where the block applies, a blank user becomes "dbtrail" and a blank
// password a generated one, so running the block and pressing Save line up.
// Where it does not, values it filled and nobody changed are cleared again:
// sent to the server, they would read as a source with no host and the save
// would be refused, on a form where they may not even be visible.
function applyGrantDefaults(form) {
  const f = form.elements;
  if (grantDefaultsApply(form)) {
    if (!f.source_user.value) f.source_user.value = "dbtrail";
    if (!f.source_password.value) {
      if (!pendingSourcePassword) pendingSourcePassword = genSourcePassword();
      f.source_password.value = pendingSourcePassword;
      if (pendingSourcePassword) generatedPasswords.set(form, pendingSourcePassword);
    }
  } else {
    if (untouchedGrantUser(form)) f.source_user.value = "";
    if (untouchedGrantPassword(form)) {
      f.source_password.value = "";
      generatedPasswords.delete(form);
    }
  }
  refreshGrants(form);
}

// missingSourceHost reports a new monitored server whose source host was not
// filled in. Without this the server answers about the field it does see: the
// INDEX host, inside a collapsed section, or a source user the form filled in
// itself. Both name the wrong box.
function missingSourceHost(form) {
  const f = form.elements;
  return grantDefaultsApply(form) && !f.source_host.value.trim() && !f.host.value.trim();
}

function buildServerForm() {
  const form = el("form", { class: "filters", id: "server-form", style: "display:block;margin-top:18px" });
  form.append(el("input", { type: "hidden", name: "id" }));

  // Name is the one field every entry needs, whatever the path — keep it out
  // of both sections so the index section reads as fully optional.
  const top = el("div", { class: "form-grid" });
  top.append(srvField("Name", "name", { placeholder: "prod-db-01" }));
  form.append(top);

  const mon = el("fieldset", { class: "form-section", "data-capability": "monitor" });
  mon.append(el("legend", { class: "form-legend", text: "Monitor a source database" }));
  mon.append(el("p", { class: "form-hint", text: "Name, source host, user and password are required. DBTrail checks that the server is ready, then starts capturing changes." }));
  const monGrid = el("div", { class: "form-grid" });
  // Source family selector — reveals the PostgreSQL-only fields below.
  monGrid.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "Source type" }),
    el("select", { class: "select", name: "flavor" },
      opt("mysql", "MySQL"), opt("postgres", "PostgreSQL"), opt("mariadb", "MariaDB"))));
  monGrid.append(srvField("Source host", "source_host", { placeholder: "db.example.com" }));
  monGrid.append(srvField("Source port", "source_port", { placeholder: "3306" }));
  monGrid.append(srvField("Source user", "source_user", { placeholder: "repl" }));
  monGrid.append(srvField("Source password", "source_password", { type: "password", autocomplete: "new-password" }));
  // The forwarding account (#2079): optional, MySQL and MariaDB only. Sent
  // by serverFormBody only when changed; the password is never prefilled
  // and blank keeps the saved one. Removing a saved account is the control
  // in #route-extra, filled in when the form opens on a server that has one.
  monGrid.append(tagFlavor(srvField("Forwarding user", "route_user", { placeholder: "(optional) blank forwards with the source user", autocomplete: "off" }), "mysql mariadb"));
  monGrid.append(tagFlavor(srvField("Forwarding password", "route_password", { type: "password", autocomplete: "new-password" }), "mysql mariadb"));
  // PostgreSQL-only: a logical-replication connection is per-database, and the
  // slot/publication are operator-created (validate-don't-create).
  monGrid.append(tagFlavor(srvField("Database", "source_database", { placeholder: "appdb" }), "postgres"));
  monGrid.append(tagFlavor(srvField("Replication slot", "source_slot", { placeholder: "bintrail_slot" }), "postgres"));
  monGrid.append(tagFlavor(srvField("Publication", "source_publication", { placeholder: "bintrail_pub" }), "postgres"));
  monGrid.append(srvField("Schemas", "schemas", { placeholder: "(optional) shop,billing" }));
  monGrid.append(srvField("Archive to S3", "archive_s3", { placeholder: "(optional) s3://bucket/prefix/" }));
  // S3 store (#1575): where this server's buckets live when that is not AWS,
  // and optionally the keys that sign for them (without keys, the daemon's
  // credential chain signs). They apply per BUCKET, to uploads and reads
  // alike, for the Archive bucket above and the Backups bucket on the
  // settings page. The secret is never prefilled: blank keeps the saved one.
  monGrid.append(srvField("S3 endpoint", "s3_endpoint", { placeholder: "(optional) http://minio:9000 for MinIO, Wasabi, LocalStack" }));
  monGrid.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "S3 addressing" }),
    el("select", { class: "select", name: "s3_path_style" },
      opt("", "Path style (default with an endpoint)"), opt("path", "Path style: host/bucket/key"), opt("vhost", "Virtual-hosted: bucket.host/key"))));
  monGrid.append(srvField("S3 region", "s3_region", { placeholder: "(optional) us-east-1; MinIO ignores it, Wasabi wants its endpoint's" }));
  monGrid.append(srvField("S3 access key", "s3_access_key_id", { placeholder: "(optional) blank uses DBTrail's own credentials", autocomplete: "off" }));
  monGrid.append(srvField("S3 secret key", "s3_secret_access_key", { type: "password", autocomplete: "new-password" }));
  mon.append(monGrid);
  mon.append(tagFlavor(el("div", { id: "route-extra" }), "mysql mariadb"));
  mon.append(tagFlavor(el("p", { class: "form-hint", text: FORWARDING_HINT }), "mysql mariadb"));
  mon.append(el("p", { class: "form-hint", text: "Leave the S3 fields blank for AWS. They apply to the Archive and Snapshots locations set on this server, for uploads and reads alike, not to the default Snapshots location DBTrail was started with. A bucket has one store and one pair of keys, so two servers sharing a bucket must agree. Clearing the access key removes both keys." }));
  // The source user is the #1 friction point — spell out the grant inline,
  // never behind a <details>. REPLICATION SLAVE/CLIENT drive the stream;
  // SELECT covers the information_schema snapshot of columns/PKs/FKs. The
  // text comes from grantBlocks and is redrawn from the user and password
  // fields (showServerForm), so the block creates the user the form saves.
  const grantHint = tagFlavor(el("p", { class: "form-hint", style: "margin-top:10px" }), "mysql mariadb");
  grantHint.append("Source user needs ");
  grantHint.append(el("code", { text: "REPLICATION SLAVE, REPLICATION CLIENT, SELECT" }));
  grantHint.append(" to capture, plus the snapshot line if you want snapshots. Create one on the source; copy and run:");
  mon.append(grantHint);
  const grants = grantBlocks("", "");
  mon.append(tagFlavor(el("pre", { class: "form-code", "data-grant": "mysql", text: grants.mysql }), "mysql"));
  mon.append(tagFlavor(el("pre", { class: "form-code", "data-grant": "mariadb", text: grants.mariadb }), "mariadb"));
  // PostgreSQL prerequisites — the console reads them, it never runs CREATE
  // PUBLICATION / ALTER SYSTEM (validate-don't-create; capture is pgoutput-only).
  const pgHint = tagFlavor(el("p", { class: "form-hint", style: "margin-top:10px" }), "postgres");
  pgHint.append("PostgreSQL source needs ");
  pgHint.append(el("code", { text: "wal_level=logical" }));
  pgHint.append(", a role with the ");
  pgHint.append(el("code", { text: "REPLICATION" }));
  pgHint.append(" attribute, a publication you create, and ");
  pgHint.append(el("code", { text: "REPLICA IDENTITY FULL" }));
  pgHint.append(" on replicated tables; copy and run on the source:");
  mon.append(pgHint);
  mon.append(tagFlavor(el("pre", { class: "form-code", text:
    "CREATE PUBLICATION bintrail_pub FOR ALL TABLES;\n" +
    "ALTER TABLE your_table REPLICA IDENTITY FULL;" }), "postgres"));
  mon.append(el("p", { class: "form-hint", style: "margin-top:10px", text: "Archive to S3: old data is uploaded to S3 before it's deleted locally, so your history is kept and can still be searched. Needs AWS credentials set up where DBTrail runs (environment variables or an IAM role)." }));
  form.append(mon);

  // BYO index is the advanced path — collapsed behind a <details> so the
  // monitor-first form stays one field + source. Open/close rules live in
  // showServerForm.
  const adv = el("details", { class: "form-advanced", id: "server-advanced" });
  adv.append(el("summary", { class: "form-adv-summary", text: "Advanced: bring your own index (optional)" }));
  const idx = el("fieldset", { class: "form-section" });
  idx.append(el("legend", { class: "form-legend", text: "Index connection" }));
  const idxGrid = el("div", { class: "form-grid" });
  idxGrid.append(srvField("Host", "host", { placeholder: "127.0.0.1" }));
  idxGrid.append(srvField("Port", "port", { placeholder: "3306" }));
  idxGrid.append(srvField("User", "user", { placeholder: "bintrail" }));
  idxGrid.append(srvField("Password", "password", { type: "password", autocomplete: "new-password" }));
  idxGrid.append(srvField("Index database", "dbname", { placeholder: "binlog_index" }));
  idx.append(idxGrid);
  // The snapshot location, the keep count and the archive toggle are edited
  // on the Snapshots page only (#1582, #1681). This form does not carry them
  // at all: PUT /api/servers/{id} keeps the stored values when the request
  // leaves them out. It used to post them back from hidden fields, which put
  // back an OLD folder whenever the form had been opened before a change on
  // the Snapshots page.
  adv.append(idx);
  form.append(adv);

  // What Save and Test did opens in a notice centered on the screen, above
  // this dialog (#1769). Answered inside the form, a result landed below the
  // eyeline of the button that caused it (#1605, #1608), and a first save's
  // 18 check cards, 14 of them green, hid the one that failed. The line
  // above the buttons keeps a one-line summary with a way back to the notice.
  form.append(el("div", { id: "server-form-msg", class: "form-msg" }));
  const foot = el("div", { class: "modal-foot filter-actions" });
  foot.append(el("button", { class: "btn btn-primary", type: "submit", text: "Save" }));
  foot.append(el("button", { class: "btn", type: "button", id: "server-test", text: "Test connection" }));
  foot.append(el("button", { class: "btn btn-ghost", type: "button", id: "server-cancel", text: "Cancel" }));
  form.append(foot);
  return form;
}

// showServerForm returns false when the servers modal is gone (closed while a
// save or its startup checks were in flight): a caller then has no form to
// speak through and must say what happened somewhere that lasts.
function showServerForm(prefill, opts) {
  // A new server on a process that captures opens the short Connect screen
  // (#1804). The long form stays for Edit, for a console that only reads an
  // index, and for what Connect leaves out (PostgreSQL, MariaDB, S3, an own
  // index), reached by the link at its foot.
  if (!prefill && capsCache.monitor && !(opts && opts.full)) return showConnectForm(opts && opts.draft);
  const addWrap = document.getElementById("server-add-wrap");
  const mountEl = document.getElementById("server-form-mount");
  if (!addWrap || !mountEl) return false;
  addWrap.hidden = true;
  const form = buildServerForm();
  mountEl.replaceChildren(form);
  $all("[data-capability]", form).forEach((n) => n.classList.toggle("cap-on", !!capsCache[n.dataset.capability]));
  $("#server-cancel", form).addEventListener("click", hideServerForm);
  $("#server-test", form).addEventListener("click", () => testServerForm(form));
  form.addEventListener("submit", (e) => { e.preventDefault(); saveServer(form); });
  form.elements.flavor.addEventListener("change", () => { applyFlavor(form); applyGrantDefaults(form); });
  // "change" as well as "input": some autofill fills a field and fires only
  // change, which would leave the block showing another password.
  ["source_user", "source_password"].forEach((k) => ["input", "change"].forEach((ev) =>
    form.elements[k].addEventListener(ev, () => refreshGrants(form))));
  // The generated password sits masked in its field. Someone reusing their
  // own account clicks in and types, and the caret lands after the hidden
  // value: select it first, so typing replaces it instead of appending.
  form.elements.source_password.addEventListener("focus", () => {
    if (untouchedGrantPassword(form)) form.elements.source_password.select();
  });

  // Where the index connection is the whole form (serve-only process: no
  // monitor capability), or the entry being edited carries index fields,
  // the "advanced" block must start expanded — and in serve-only mode
  // there is nothing to collapse it back to, so the toggle hides. Note a
  // monitor-first save also ends with index fields (the derived index DSN
  // round-trips as host/dbname in the DTO), so "collapsed by default"
  // means a fresh add; editing any saved entry shows its index.
  const adv = $("#server-advanced", form);
  // baseline_dir/baseline_s3/no_archive left this form for the Backups &
  // snapshots settings page (#1582); they no longer open the fold, which
  // now only shows connection fields.
  const hasIndexFields = !!(prefill && (prefill.host || prefill.dbname));
  // Keep "bring your own index (optional)" COLLAPSED for a monitored source:
  // its index DSN is auto-derived and round-trips as host/dbname, so expanding
  // it — e.g. when a failed-preflight error card opens the form — would show
  // the operator a per-source index they never typed. Open it only for a pure
  // BYO-index entry (no source) or a serve-only process where the index is the
  // whole form.
  const byoIndex = hasIndexFields && !(prefill && prefill.has_source);
  adv.open = !capsCache.monitor || byoIndex;
  $(".form-adv-summary", adv).hidden = !capsCache.monitor;

  if (prefill) {
    form.elements.id.value = prefill.id || "";
    ["name", "host", "port", "user", "dbname", "archive_s3", "s3_endpoint", "s3_path_style", "s3_region", "s3_access_key_id", "source_host", "source_port", "source_user", "route_user", "schemas", "source_database", "source_slot", "source_publication"].forEach((k) => {
      if (form.elements[k] && prefill[k] != null) form.elements[k].value = prefill[k];
    });
    form.elements.route_password.placeholder = prefill.has_route_password ? "(unchanged; leave blank to keep)" : "";
    fillRouteExtra(form, prefill);
    form.elements.password.placeholder = prefill.has_password ? "(unchanged; leave blank to keep)" : "(none)";
    form.elements.source_password.placeholder = prefill.has_source_password ? "(unchanged; leave blank to keep)" : "";
    if (prefill.has_source_password) savedSourcePasswords.set(form, true);
    form.elements.s3_secret_access_key.placeholder = prefill.has_s3_secret_access_key ? "(unchanged; leave blank to keep)" : "";
  }
  // Flavor init runs for both add and edit; it's immutable after create (the
  // backend rejects a change on PUT), so disable the selector when editing.
  form.elements.flavor.value = (prefill && prefill.flavor) || "mysql";
  if (prefill && prefill.id) form.elements.flavor.disabled = true;
  applyFlavor(form);
  // After the id and flavor are set: they decide whether the grant block gets
  // a generated account (a new MySQL/MariaDB server on a capturing process)
  // or only shows the saved user (an edit: blank still keeps the password).
  applyGrantDefaults(form);
  form.elements.name.focus();
  return true;
}
// ── Connect (#1804) ──────────────────────────────────────────────────────────
//
// The short screen for adding a MySQL server: host, port, user, password, an
// optional name, the permissions block, and one button that calls
// POST /api/servers/check, which checks the server and starts capture only
// when nothing failed. What was typed is saved as a draft on the server as the
// person types, so a reload (to run the permissions block, say) brings it
// back. The password is never in the draft: the page keeps it, and after a
// reload the screen asks for it again.

// The draft save in flight (and whether another waits behind it), awaited
// before a check or a discard so a late save can never bring back a draft the
// check or Cancel just removed.
let connectDraftSaving = Promise.resolve();
let connectDraftQueued = false;

// connectBody is what the check and the draft send. The name is only what was
// typed: the placeholder holds the automatic name, and sent as typed it would
// be refused as a duplicate once another server took it.
function connectBody(form, withPassword) {
  const f = form.elements;
  const body = {
    // No name: the screen asks for none, and the server names the server
    // after its address, unique against the others (DeriveServerName).
    name: "", flavor: f.flavor.value || "mysql",
    source_host: f.source_host.value.trim(), source_port: f.source_port.value.trim(),
    source_user: f.source_user.value.trim(),
  };
  if (withPassword) body.source_password = f.source_password.value;
  // What step 1 found goes into the draft only: a reload comes back to step 2
  // with it instead of probing again (#1953).
  else if (connectIdentity.get(form)) body.identified = connectIdentityBody(form);
  return body;
}

// saveConnectDraftSoon saves the form now, or right after the save already
// on its way. No delay: the reload this exists for often comes a moment after
// the last keystroke. At most one save waits, and it reads the fields when it
// is sent, so a burst of typing costs two requests, not one per key.
function saveConnectDraftSoon(form) {
  // done: capture started or the screen was left; nothing may be saved after
  // that, or the next page load reopens a finished form. busy: a check is
  // running and has saved the form itself; save again when it ends, so what
  // was typed meanwhile is not lost.
  if (!form.isConnected || form.dataset.done) return;
  if (form.dataset.busy) { form.dataset.saveAfter = "1"; return; }
  if (connectDraftQueued) return;
  connectDraftQueued = true;
  connectDraftSaving = connectDraftSaving.then(() => {
    connectDraftQueued = false;
    if (!form.isConnected || form.dataset.done) return;
    return api("/api/servers/draft", { method: "PUT", body: connectBody(form, false) }).then((res) => {
      if (!form.isConnected || form.dataset.done) return;
      if (form.dataset.saveFailed) { delete form.dataset.saveFailed; formMsg("", false); }
    }, (err) => {
      if (!form.isConnected || form.dataset.done) return;
      form.dataset.saveFailed = "1";
      formMsg("This form could not be saved, so a reload would lose it: " + ((err && err.message) || err), true);
    });
  });
}

// flushConnectDraft waits for the save on its way and the one queued after it.
async function flushConnectDraft() {
  await connectDraftSaving.catch(() => {});
}

// ── Connect in three steps (#1953) ──────────────────────────────────────────
//
// 1. Where is it? Host and port. "Find it" asks POST /api/servers/identify,
//    which reads the server's greeting without logging in, and the answer is
//    drawn: what answered ("MariaDB 10.11 on Amazon RDS"), or the path from
//    DBTrail to the address with the broken part marked. A probe may count
//    against DBTrail's address on the server, so one runs ONLY when somebody
//    presses the button: never while typing, never on a page load.
// 2. Let DBTrail in. The SQL block for that server (MySQL or MariaDB, managed
//    or not) with a generated password, and "I ran it".
// 3. Checking. The startup checks as a list of lights (doctor.Lights), each
//    failing one with its fix. POST /api/servers/check saves and starts
//    capture the moment nothing fails, and a failing round is checked again
//    every 10 seconds, a bounded number of times, with nothing to press.

const FLAVOR_LABEL = { mysql: "MySQL", mariadb: "MariaDB" };
const MANAGED_LABEL = { rds: "Amazon RDS", aurora: "Amazon Aurora" };
const PROXY_LABEL = { proxysql: "ProxySQL", maxscale: "MaxScale", rds_proxy: "RDS Proxy" };
const CONNECT_STEP_BUTTON = { 1: "Find it", 2: "I ran it", 3: "Check again", done: "Done" };
const CONNECT_RECHECK_MS = 10000;
// 30 rounds of 10 seconds plus the checks themselves: a few minutes to change
// a setting, then it stops and waits for a press.
const CONNECT_RECHECK_MAX = 30;

// What step 1 found, per form: the identification, kept out of the DOM.
const connectIdentity = new WeakMap();
// connectRestored marks a form filled from a draft, for its whole life: the
// account may have been created with a password the page no longer has, so
// no step may generate one (#1804), not even a later Find it.
const connectRestored = new WeakSet();
// connectManaged is the RDS/Aurora box as the person set it, per address,
// so a reload or a second Find it does not quietly undo it.
const connectManaged = new WeakMap();
// connectAddr is the address a request was made for: an answer that comes
// back after Host or Port changed describes another server and is dropped.
function connectAddr(form) {
  const f = form.elements;
  return f.source_host.value.trim() + ":" + (f.source_port.value.trim() || "3306");
}
// The pending automatic re-check, per form, and how many rounds ran; and the
// same for a step 1 that did not get there.
const connectRecheck = new WeakMap();
const connectFindRetry = new WeakMap();

// connectSchedule runs fn after ms. Its own name so the Go harness, where
// setTimeout runs at once, can hold the re-check loop instead of spinning it.
function connectSchedule(fn, ms) { return setTimeout(fn, ms); }

// shortVersion keeps major.minor: "10.11.6-MariaDB-log" is "10.11".
function shortVersion(v) {
  const m = /^(\d+)\.(\d+)/.exec(String(v || ""));
  return m ? m[1] + "." + m[2] : "";
}

// connectTitle names what answered, as the found tile says it.
function connectTitle(id, flavor) {
  const v = id.flavor === flavor ? shortVersion(id.version) : "";
  return (FLAVOR_LABEL[flavor] || "MySQL") + (v ? " " + v : "") + (MANAGED_LABEL[id.managed] ? " on " + MANAGED_LABEL[id.managed] : "");
}

// connectNote says why the flavor is a choice, or "" when it was read.
function connectNote(id) {
  if (id.proxy) return "This looks like " + PROXY_LABEL[id.proxy] + " in front of your database. Choose what is behind it:";
  if (id.server_error && !id.version) return "It answered, but does not know DBTrail's address yet. The SQL below fixes that. Which is it?";
  if (!id.flavor) return "It speaks the MySQL protocol, but DBTrail cannot tell which server it is. Choose one:";
  return "";
}

// hostUnblockSQL is what clears a block (1129). Verified per server: MySQL
// 8.4 dropped FLUSH HOSTS, the host cache table works on 8.0 and 8.4, and
// MariaDB keeps FLUSH HOSTS. The greeting that carried 1129 names no version,
// so both are shown and one is commented.
const HOST_UNBLOCK_SQL = "-- MySQL:\nTRUNCATE TABLE performance_schema.host_cache;\n-- MariaDB: run this instead.\n-- FLUSH HOSTS;";

// identifyFailureParts says why step 1 did not get there: text, then the code
// to run, then a note, the part of the path that broke (for the drawing), its
// label, and for loopback the address to use. Every kind doctor.IdentifyKinds
// can send is here (TestIdentifyKindsSayEveryKind); another returns a plain
// sentence rather than nothing.
function identifyFailureParts(id, port) {
  const p = port || "3306";
  switch (id.kind) {
    case "name_not_found":
      return { broken: "name", label: "no such name", text: "DBTrail can't find that name. Check the spelling in Host." };
    case "host_unreachable":
      return { broken: "route", label: "no route", text: "DBTrail found no way to that address. Check Host, and that this machine can reach that network." };
    case "timeout": {
      const where = id.in_container ? "the machine DBTrail runs on"
        : id.from ? "DBTrail's address, " + id.from : "the address of the machine DBTrail runs on";
      const note = id.from ? "If DBTrail reaches the internet through NAT, allow that public address instead." : "";
      return { broken: "route", label: "no answer", copy: id.from || "",
        text: (MANAGED_LABEL[id.managed] ? "Nothing answered. In AWS, allow port " + p + " in the database's security group from "
          : "Nothing answered. A firewall is probably dropping the connection: allow port " + p + " from ") + where + ".",
        note };
    }
    case "port_closed":
      return { broken: "port", label: "refused", text: "Nothing listens on port " + p + " there. Check Port, that the server is running, and that bind-address is not only 127.0.0.1." };
    case "not_mysql":
      return { broken: "answer", label: "not MySQL",
        text: id.answer === "closed" ? "Something took the connection and closed it at once, the way a port forward with nothing behind it does. Check Port."
          : "Something answered, but not MySQL or MariaDB. Check Port." + (p === "5432" ? " 5432 is PostgreSQL's port: use the full form below for PostgreSQL." : "") };
    case "host_blocked":
      return { broken: "answer", label: "blocked", code: HOST_UNBLOCK_SQL,
        text: "The server blocked DBTrail's address after too many failed connections. Run this on it as an admin, then press Find it:" };
    case "loopback_in_container":
      return { broken: "route", label: "nothing here", use: id.suggest || "",
        text: "Nothing answered on this machine's own address. If DBTrail runs in a container, localhost is the container. A server answered at " + (id.suggest || "the host") + "." };
  }
  return { broken: "route", label: "", text: "DBTrail could not get there." };
}

// connectPath draws DBTrail, the host and the port joined by two segments,
// the broken one marked, with a sentence for readers that do not see it.
function connectPath(host, port, broken, label) {
  const seg = (bad) => el("span", { class: "cx-seg" + (bad ? " bad" : "") }, bad && label ? el("em", { text: label }) : "");
  const node = (text, bad) => el("span", { class: "cx-node" + (bad ? " bad" : "") }, text);
  const hostBad = broken === "name";
  const endBad = broken === "port" || broken === "answer";
  const said = { name: "DBTrail could not find " + host, route: "DBTrail could not reach " + host,
    port: "Nothing listens on port " + port + " at " + host, answer: "The server at " + host + " did not answer as MySQL" }[broken] || "";
  return el("div", { class: "cx-path", role: "img", "aria-label": said },
    node("DBTrail"), seg(broken === "name"), node(host, hostBad), seg(broken === "route"), node(":" + port, endBad));
}

// setConnectStep shows the steps up to n and names the one button for it.
function setConnectStep(form, n) {
  form.dataset.step = String(n);
  const num = n === "done" ? 4 : n;
  $all("[data-cx-step]", form).forEach((s) => { s.hidden = Number(s.dataset.cxStep) > num; });
  $all("[data-cx-dot]", form).forEach((d) => d.classList.toggle("on", Number(d.dataset.cxDot) <= num));
  const btn = form.querySelector("button[type=submit]");
  if (btn) btn.textContent = CONNECT_STEP_BUTTON[n];
  // The way to the full form is for steps 1 and 2. On step 3 the checks are
  // running for a server already found, and leaving would stop them.
  const full = form.querySelector("button#connect-full-form");
  if (full) full.hidden = num > 2;
}

function buildConnectForm() {
  const form = el("form", { class: "filters cx", id: "server-form", "data-connect": "1", style: "display:block;margin-top:18px" });
  form.append(el("input", { type: "hidden", name: "id" }));
  form.append(el("input", { type: "hidden", name: "flavor", value: "mysql" }));
  form.append(el("h3", { class: "form-legend", text: "Connect a database" }));
  form.append(el("div", { class: "cx-dots", "aria-hidden": "true" },
    el("span", { "data-cx-dot": "1" }), el("span", { "data-cx-dot": "2" }), el("span", { "data-cx-dot": "3" })));

  const s1 = el("div", { class: "cx-step", "data-cx-step": "1" });
  s1.append(el("p", { class: "cx-title", text: "1. Where is it?" }));
  const grid = el("div", { class: "form-grid cx-where" });
  grid.append(srvField("Host", "source_host", { placeholder: "db.example.com" }));
  grid.append(srvField("Port", "source_port", { placeholder: "3306" }));
  s1.append(grid);
  s1.append(el("div", { id: "connect-found", class: "cx-found-slot" }));
  form.append(s1);

  const s2 = el("div", { class: "cx-step", "data-cx-step": "2", hidden: true });
  s2.append(el("p", { class: "cx-title", text: "2. Let DBTrail in" }));
  s2.append(el("p", { class: "form-hint", text: "Run this on that server, as an admin:" }));
  const pre = el("pre", { class: "form-code", "data-grant": "mysql", text: grantBlocks("", "", false, false, true).mysql });
  s2.append(el("div", {}, pre,
    el("div", { class: "cx-row" },
      el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(pre.textContent, "SQL") }),
      el("label", { class: "cx-managed" },
        el("input", { type: "checkbox", name: "cx_managed", onchange: () => {
          connectManaged.set(form, { addr: connectAddr(form), on: !!form.elements.cx_managed.checked });
          refreshGrants(form);
          saveConnectDraftSoon(form);
        } }), " On Amazon RDS or Aurora")),
    // For the DBA who wants to know what LOCK TABLES is for (#1986). Closed,
    // and only beside the RDS permission, which is the one it explains.
    el("details", { class: "form-advanced cx-pit", hidden: true },
      el("summary", { class: "form-adv-summary", text: "How snapshots stay point-in-time" }),
      el("p", { class: "form-hint", text: "Writes to the copied tables may wait while a snapshot starts, until every copy thread marks the same point-in-time. " +
        "If a long query or open transaction is still running on those tables, the wait lasts until it ends." }),
      docsMore("guides/backup-strategy", "how-a-snapshot-stays-point-in-time", "how a snapshot stays point-in-time"))));
  const acct = el("div", { class: "form-grid" });
  acct.append(srvField("User", "source_user", { placeholder: "dbtrail" }));
  acct.append(srvField("Password", "source_password", { type: "password", autocomplete: "new-password" }));
  s2.append(acct);
  // Shown only when the form came back from a draft: the password is not in it.
  s2.append(el("p", { class: "form-hint", id: "connect-pw-again", hidden: true,
    text: "Type the password again. It is not kept after a reload. Lost it? Type a new one and run the ALTER USER line above." }));
  form.append(s2);

  const s3 = el("div", { class: "cx-step", "data-cx-step": "3", hidden: true });
  s3.append(el("p", { class: "cx-title", text: "3. Checking" }));
  s3.append(el("ol", { class: "cx-lights", id: "connect-lights", "aria-live": "polite" }));
  s3.append(el("p", { class: "cx-auto", id: "connect-auto", "aria-live": "polite" }));
  s3.append(el("div", { id: "connect-result" }));
  form.append(s3);

  form.append(el("div", { id: "server-form-msg", class: "form-msg" }));
  const foot = el("div", { class: "modal-foot filter-actions" });
  foot.append(el("button", { class: "btn btn-primary", type: "submit", text: CONNECT_STEP_BUTTON[1] }));
  foot.append(el("button", { class: "btn btn-ghost", type: "button", id: "server-cancel", text: "Cancel" }));
  // In the foot, on every step, taking no row of its own: a row above the
  // button would push it below the fold on steps 2 and 3.
  foot.append(el("button", { class: "btn btn-sm btn-ghost cx-full", type: "button", id: "connect-full-form", text: "PostgreSQL, S3 or your own store? Open the full form",
    onclick: () => openFullForm(form) }));
  form.append(foot);
  return form;
}

// showConnectForm mounts the Connect screen, filled from a saved draft when
// one is given. A draft that carries what step 1 found comes back at step 2
// without probing again. It never holds the password, so a restored form
// leaves the field empty and says so, and does NOT generate a new one: the
// account was created with the old one.
function showConnectForm(draft) {
  const addWrap = document.getElementById("server-add-wrap");
  const mountEl = document.getElementById("server-form-mount");
  if (!addWrap || !mountEl) return false;
  addWrap.hidden = true;
  const form = buildConnectForm();
  mountEl.replaceChildren(form);
  const f = form.elements;
  setConnectStep(form, 1);
  if (draft) {
    if (draft.source_user) connectRestored.add(form);
    f.source_host.value = draft.source_host || "";
    f.source_port.value = draft.source_port || "";
    f.source_user.value = draft.source_user || "";
    if (draft.identified) {
      const id = draft.identified;
      if (typeof id.managed_choice === "boolean") connectManaged.set(form, { addr: connectAddr(form), on: id.managed_choice });
      showIdentified(form, id, draft.flavor);
    }
    $("#connect-pw-again", form).hidden = !connectRestored.has(form);
    refreshGrants(form);
  }
  $("#server-cancel", form).addEventListener("click", () => discardConnect(form));
  form.addEventListener("submit", (e) => { e.preventDefault(); connectSubmit(form); });
  ["source_user", "source_password"].forEach((k) => ["input", "change"].forEach((ev) =>
    f[k].addEventListener(ev, () => refreshGrants(form))));
  // A new address undoes what was found for the old one: its block and its
  // checks describe another server.
  ["source_host", "source_port"].forEach((k) => f[k].addEventListener("input", () => {
    if (form.dataset.done) return;
    if (form.dataset.step !== "1") backToWhere(form);
    else { stopConnectRecheck(form); clearFindLine(form); }
  }));
  ["source_host", "source_port", "source_user"].forEach((k) =>
    f[k].addEventListener("input", () => saveConnectDraftSoon(form)));
  f.source_password.addEventListener("focus", () => { if (untouchedGrantPassword(form)) f.source_password.select(); });
  (draft && draft.identified ? f.source_password : f.source_host).focus();
  return true;
}

// connectSubmit is the one button: its step says what it does.
function connectSubmit(form) {
  const step = form.dataset.step;
  if (step === "1") return identifyConnect(form);
  if (step === "2") { setConnectStep(form, 3); return runConnectCheck(form, true); }
  if (step === "3") return runConnectCheck(form, true);
  if (step === "done") hideServerForm();
}

// backToWhere returns to step 1: the answer, the block's server and any
// running re-check belong to the address that was there before.
function backToWhere(form) {
  stopConnectRecheck(form);
  connectIdentity.delete(form);
  const found = $("#connect-found", form);
  if (found) found.replaceChildren();
  setConnectStep(form, 1);
}

async function identifyConnect(form, pressed = true) {
  if (form.dataset.busy) return;
  const f = form.elements;
  const round = pressed ? 1 : ((connectFindRetry.get(form) || {}).round || 0) + 1;
  stopConnectRecheck(form);
  if (!f.source_host.value.trim()) { formMsg("Fill in Host, the address of your database.", true); f.source_host.focus(); return; }
  const btn = form.querySelector("button[type=submit]");
  const asked = connectAddr(form);
  form.dataset.busy = "1";
  if (btn) { btn.disabled = true; btn.textContent = "Looking…"; }
  formMsg("", false);
  let id;
  try {
    id = await api("/api/servers/identify", { method: "POST", body: { source_host: f.source_host.value.trim(), source_port: f.source_port.value.trim() } });
  } catch (err) {
    if (form.isConnected && connectAddr(form) === asked) {
      formMsg("DBTrail could not look: " + ((err && err.message) || err), true);
      // An automatic look that failed as a request keeps looking when the
      // trouble can pass (the network, a 5xx); otherwise the line that
      // promised it is taken back.
      const passing = !(err && err.status) || err.status >= 500;
      if (!pressed && passing) scheduleFindRetry(form, round);
      else clearFindLine(form);
    }
    return;
  } finally {
    delete form.dataset.busy;
    if (btn) { btn.disabled = false; btn.textContent = CONNECT_STEP_BUTTON[form.dataset.step]; }
    if (form.dataset.saveAfter) { delete form.dataset.saveAfter; saveConnectDraftSoon(form); }
    // Use <host> pressed while a look was running: look at it now.
    if (form.dataset.findAfter) { delete form.dataset.findAfter; if (form.isConnected) identifyConnect(form, true); }
  }
  // Host or Port changed while it looked: the answer is about another server.
  if (!form.isConnected || connectAddr(form) !== asked) return;
  if (id.kind) {
    showNotFound(form, id);
    // localhost inside a container is fixed by Use <host>, not by waiting,
    // and its probe reads the greeting of a real server at the retry address,
    // which that server may count against DBTrail's address: never repeated.
    if (id.kind !== "loopback_in_container") scheduleFindRetry(form, round);
  } else showIdentified(form, id, "");
  saveConnectDraftSoon(form);
}

// scheduleFindRetry looks again in 10 seconds, CONNECT_RECHECK_MAX times at
// most, then waits for a press. It runs only for causes where no MySQL server
// is read, which is what counts toward max_connect_errors: no name, no route,
// no answer, a closed port, something that is not MySQL. A blocked address is
// refused before a login either way, and a greeting that answers ends the
// retries by moving to step 2. The loopback case reads a real server's
// greeting at the retry address, so identifyConnect never schedules it.
function scheduleFindRetry(form, round) {
  const line = form.querySelector("p#connect-find-auto");
  if (round >= CONNECT_RECHECK_MAX) {
    if (line) line.textContent = "Stopped trying. Press Find it when it is fixed.";
    return;
  }
  const asked = connectAddr(form);
  const timer = connectSchedule(() => {
    const r = connectFindRetry.get(form);
    if (!r || r.timer !== timer || !form.isConnected || form.dataset.done || form.dataset.step !== "1" || connectAddr(form) !== asked) return;
    identifyConnect(form, false);
  }, CONNECT_RECHECK_MS);
  connectFindRetry.set(form, { timer, round });
  if (line) line.textContent = "Trying again in 10 seconds.";
}

// clearFindLine takes back "Trying again", once no retry is coming.
function clearFindLine(form) {
  const line = form.querySelector("p#connect-find-auto");
  if (line) line.textContent = "";
}

// showNotFound draws why step 1 did not get there and stays on it.
function showNotFound(form, id) {
  connectIdentity.delete(form);
  const f = form.elements;
  const host = f.source_host.value.trim(), port = f.source_port.value.trim() || "3306";
  const p = identifyFailureParts(id, port);
  const box = el("div", { class: "cx-miss" }, connectPath(host, port, p.broken, p.label), el("p", { text: p.text }));
  if (p.code) {
    box.append(el("pre", { class: "form-code", text: p.code }));
    box.append(el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(p.code, "SQL") }));
  }
  if (p.copy) box.append(el("button", { class: "btn btn-sm", type: "button", text: "Copy " + p.copy, onclick: () => copyText(p.copy, "Address") }));
  if (p.use) {
    const useHost = p.use.replace(/:\d+$/, "");
    box.append(el("button", { class: "btn btn-sm", type: "button", text: "Use " + useHost, onclick: () => {
      f.source_host.value = useHost; saveConnectDraftSoon(form);
      if (form.dataset.busy) form.dataset.findAfter = "1";
      else identifyConnect(form, true);
    } }));
  }
  if (p.note) box.append(el("p", { class: "form-hint", text: p.note }));
  box.append(el("p", { class: "cx-auto", id: "connect-find-auto", "aria-live": "polite" }));
  $("#connect-found", form).replaceChildren(box);
  setConnectStep(form, 1);
}

// showIdentified draws what answered and opens step 2 for it. flavor is a
// choice made before (a restored draft), used when the greeting named none.
// A form filled from a draft never gets a generated password (#1804).
function showIdentified(form, id, flavor) {
  connectIdentity.set(form, id);
  const f = form.elements;
  f.flavor.value = id.flavor || (FLAVOR_LABEL[flavor] ? flavor : "") || f.flavor.value || "mysql";
  const chosen = connectManaged.get(form);
  f.cx_managed.checked = chosen && chosen.addr === connectAddr(form) ? chosen.on : !!id.managed;
  const title = el("strong", { text: connectTitle(id, f.flavor.value) });
  const tile = el("div", { class: "cx-found" }, el("span", { class: "cx-mark", "aria-hidden": "true", text: "✓" }), title);
  const note = connectNote(id);
  if (note) {
    const pick = el("div", { class: "cx-pick" });
    const choose = (fl) => {
      f.flavor.value = fl;
      $all("button", pick).forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.pick === fl)));
      title.textContent = connectTitle(id, fl);
      refreshGrants(form);
      saveConnectDraftSoon(form);
    };
    for (const fl of ["mysql", "mariadb"]) {
      // data-pick, not data-flavor: [data-flavor] is the full form's gating,
      // and a global rule hides every such node the full form did not light.
      pick.append(el("button", { class: "btn btn-sm", type: "button", "data-pick": fl, "aria-pressed": String(f.flavor.value === fl),
        text: FLAVOR_LABEL[fl], onclick: () => choose(fl) }));
    }
    tile.append(el("p", { text: note }), pick);
  }
  $("#connect-found", form).replaceChildren(tile);
  setConnectStep(form, 2);
  if (connectRestored.has(form)) refreshGrants(form);
  else applyGrantDefaults(form);
}

// connectIdentityBody is what the draft keeps of step 1's answer.
function connectIdentityBody(form) {
  const id = connectIdentity.get(form);
  if (!id) return undefined;
  const out = {};
  for (const k of ["version", "flavor", "managed", "proxy", "server_error"]) if (id[k]) out[k] = id[k];
  // What the person set the RDS box to, apart from what was detected: the
  // tile's title says only what the server's name showed.
  const chosen = connectManaged.get(form);
  if (chosen && chosen.addr === connectAddr(form)) out.managed_choice = chosen.on;
  return out;
}

function stopConnectRecheck(form) {
  for (const m of [connectRecheck, connectFindRetry]) {
    const r = m.get(form);
    if (r && r.timer) clearTimeout(r.timer);
    m.delete(form);
  }
}

// LIGHT_STATE says each light's state in words, for a screen reader.
const LIGHT_STATE = { ok: "passed", bad: "failed", warn: "passed with a warning", wait: "not checked yet" };

// LIGHT_WORDS names each light (doctor.Lights), in the order they are drawn.
const LIGHT_WORDS = [
  ["reach", "DBTrail reaches it"],
  ["login", "User logs in"],
  ["rows", "Change log keeps full rows"],
  ["permissions", "Permissions"],
  ["keys", "Every table has a key"],
  ["other", "Other checks"],
];

// connectLights turns a report into one light per question: ok, bad, warn,
// or wait (not reached: the connection failed before it). The connection
// check is the reach light's; when it passed, the user logged in too.
function connectLights(report) {
  const checks = (report && report.checks) || [];
  const by = {};
  for (const c of checks) (by[c.light || "other"] ||= []).push(c);
  const worst = (cs) => cs.some((c) => c.status === "fail") ? "bad" : cs.some((c) => c.status === "warn" && !c.optional) ? "warn" : "ok";
  const connFailed = (by.reach || []).concat(by.login || []).some((c) => c.status === "fail");
  const out = [];
  let blocked = false;
  for (const [light, label] of LIGHT_WORDS) {
    const cs = by[light] || [];
    let status;
    if (blocked) status = "wait";
    else if (light === "login" && !cs.length) status = connFailed ? "wait" : "ok";
    else status = worst(cs);
    if (light === "other" && (status === "ok" || !cs.length)) continue;
    out.push({ light, label, status, checks: cs.filter((c) => c.status === "fail" || (c.status === "warn" && !c.optional)) });
    if ((light === "reach" || light === "login") && status === "bad") blocked = true;
  }
  return out;
}

// drawConnectLights draws the lights, a failing one with its fix. Once capture
// started, the warnings are said once, in the result card, not again here.
function drawConnectLights(form, report, started) {
  const list = $("#connect-lights", form);
  if (!list) return;
  list.replaceChildren(...connectLights(report).map((l) => {
    // The state is said, not only coloured: a screen reader reads the label
    // with it ("User logs in: failed").
    const li = el("li", { class: "cx-light " + l.status, "aria-label": l.label + ": " + LIGHT_STATE[l.status] },
      el("span", { class: "cx-dot", "aria-hidden": "true", text: l.status === "ok" ? "✓" : l.status === "wait" ? "" : "!" }),
      el("span", { class: "cx-label", text: l.label }));
    if (l.checks.length && l.status !== "ok" && !started) li.append(connectFindings(l.checks, true));
    return li;
  }));
  // The fix is what the person came for: bring step 3 into view from its
  // title, so the lights, the fix and what happens next are what is on
  // screen, not the fields of step 2 above them.
  const bad = list.querySelector("li.bad");
  const s3 = form.querySelector('div[data-cx-step="3"]');
  if (bad && s3 && typeof s3.scrollIntoView === "function") s3.scrollIntoView({ block: "start", behavior: prefersReducedMotion() ? "auto" : "smooth" });
}

// runConnectCheck runs the startup checks for what the form holds and draws
// the lights. Nothing failed: the server is saved and capture starts, and the
// screen says so in place. Something failed: it is checked again in 10
// seconds, CONNECT_RECHECK_MAX times at most, then it waits for a press.
async function runConnectCheck(form, pressed) {
  if (form.dataset.busy || form.dataset.done) return;
  const f = form.elements;
  // The round is read before the pending re-check is dropped: dropping it
  // first counted every round as the first, and the loop never stopped.
  const round = pressed ? 1 : ((connectRecheck.get(form) || {}).round || 0) + 1;
  stopConnectRecheck(form);
  refreshGrants(form);
  if (!f.source_user.value.trim()) { setConnectStep(form, 2); formMsg("Fill in User.", true); f.source_user.focus(); return; }
  if (!f.source_password.value) { setConnectStep(form, 2); formMsg("Fill in Password. It is not kept after a reload.", true); f.source_password.focus(); return; }
  const body = connectBody(form, true);
  const asked = connectAddr(form);
  const btn = form.querySelector("button[type=submit]");
  // Cancel is off while the check runs: it cannot stop a start already on
  // its way, and a Cancel that is then followed by "Capture started" lies.
  const cancel = form.querySelector("button#server-cancel");
  const auto = $("#connect-auto", form);
  form.dataset.busy = "1";
  if (btn) { btn.disabled = true; btn.textContent = "Checking…"; }
  if (cancel) cancel.disabled = true;
  if (auto) auto.textContent = "Checking…";
  formMsg("", false);
  let res, failure = "", lasting = false;
  try {
    await flushConnectDraft();
    res = await api("/api/servers/check", { method: "POST", body });
  } catch (err) {
    failure = (err && err.message) || String(err);
    // No answer from the server at all (the network): say what did not run.
    // An answer carries its own words ("the startup checks could not run").
    if (!(err && err.status)) failure = "the checks could not run: " + failure;
    // A 4xx is an answer about what was sent (a refused save, a bad value):
    // asking again unchanged cannot fix it. Network trouble and 5xx can pass.
    lasting = !!(err && err.status >= 400 && err.status < 500);
  } finally {
    // A started capture keeps the form marked done, so no save that was
    // waiting can put back the draft the server just removed.
    if (res && res.started) form.dataset.done = "1";
    delete form.dataset.busy;
    if (btn) btn.disabled = false;
    if (cancel) cancel.disabled = false;
    if (form.dataset.saveAfter) { delete form.dataset.saveAfter; saveConnectDraftSoon(form); }
  }
  if (!form.isConnected) {
    if (res && (res.started || res.error)) openNotice(connectNotice(res));
    // No answer at all: the server may have saved and started it before the
    // answer was lost, so this says what is not known and where to look.
    else if (failure && !res) openNotice({ tone: "warn", title: "No answer from the checks", lines: ["DBTrail could not tell whether capture started: " + failure + ". Look for the server in the list before adding it again."], button: "OK" });
    else openNotice({ tone: "err", title: "Capture did not start", lines: ["Nothing was saved. Some checks did not pass."], button: "OK" });
    return;
  }
  // Host or Port changed while the checks ran. A start still happened, for
  // the address it was asked about, and is said as such; anything else is
  // about a server no longer on screen and is dropped.
  if (connectAddr(form) !== asked && !(res && res.started)) return;
  const result = $("#connect-result", form);
  result.replaceChildren();
  if (failure) {
    // The last round's lights are not this round's answer.
    $("#connect-lights", form).replaceChildren();
    setConnectStep(form, 3);
    const said = failure.charAt(0).toUpperCase() + failure.slice(1) + (/[.!?]$/.test(failure) ? "" : ".");
    if (lasting) {
      if (auto) auto.textContent = said + " Press Check again once it is fixed.";
      return;
    }
    return scheduleConnectRecheck(form, round, said);
  }
  drawConnectLights(form, res.doctor, !!res.started);
  if (res.started) {
    if (body.source_password === pendingSourcePassword) pendingSourcePassword = "";
    if (auto) auto.textContent = "";
    result.append(connectResultCard(res, form));
    setConnectStep(form, "done");
    await refreshServersList();
    return;
  }
  if (res.kept) await refreshServersList();
  if (res.error) {
    if (auto) auto.textContent = "";
    result.append(connectResultCard(res, form));
    setConnectStep(form, 3);
    return;
  }
  setConnectStep(form, 3);
  scheduleConnectRecheck(form, round);
}

// scheduleConnectRecheck checks again in 10 seconds, unless round was the
// last one. why, when given, is what went wrong and stays on the line.
function scheduleConnectRecheck(form, round, why) {
  const auto = $("#connect-auto", form);
  const lead = why ? why + " " : "";
  if (round >= CONNECT_RECHECK_MAX) {
    if (auto) auto.textContent = lead + "Stopped checking. Press Check again when it is fixed.";
    return;
  }
  const timer = connectSchedule(() => {
    const r = connectRecheck.get(form);
    if (!r || r.timer !== timer || !form.isConnected || form.dataset.done || form.dataset.step !== "3") return;
    runConnectCheck(form, false);
  }, CONNECT_RECHECK_MS);
  connectRecheck.set(form, { timer, round });
  if (auto) auto.textContent = lead + "Checking again in 10 seconds.";
}

// connectResultCard says in place how a check that got past every light
// ended: capture started (with what to look at when there is time), or it
// did not start, and whether anything was left behind.
function connectResultCard(res, form) {
  const n = connectNotice(res);
  const card = el("div", { class: "notice-inline " + (n.tone || "ok") }, el("p", { class: "cx-done", text: n.title }));
  for (const l of n.lines || []) card.append(el("p", { text: l }));
  for (const c of [].concat(n.content || [])) if (c) card.append(c);
  return card;
}

// discardConnect is Cancel: close the screen and throw the draft away, so the
// next page load does not open it again.
async function discardConnect(form) {
  form.dataset.done = "1";
  stopConnectRecheck(form);
  hideServerForm();
  await flushConnectDraft();
  try { await api("/api/servers/draft", { method: "DELETE" }); }
  catch (err) { toastError("Could not discard the saved form: " + ((err && err.message) || err)); }
}

// openFullForm leaves Connect for the long add form, carrying what was typed.
// The draft is thrown away: it describes the Connect screen, and left behind
// it would open Connect again on the next page load over a server the long
// form may already have added.
async function openFullForm(form) {
  const f = form.elements;
  const carry = {};
  ["source_host", "source_port", "source_user", "source_password", "flavor"].forEach((k) => { carry[k] = f[k].value; });
  form.dataset.done = "1";
  stopConnectRecheck(form);
  await flushConnectDraft();
  api("/api/servers/draft", { method: "DELETE" })
    .catch((err) => toastError("Could not discard the saved form: " + ((err && err.message) || err)));
  if (!form.isConnected || !showServerForm(null, { full: true })) return;
  const full = document.getElementById("server-form");
  if (!full) return;
  for (const [k, v] of Object.entries(carry)) if (v) full.elements[k].value = v;
  applyFlavor(full);
  refreshGrants(full);
}

// restoreConnectDraft reopens Connect after a reload when a draft is saved.
// Only for a session that may add servers, on a process that captures: any
// other session would be answered 403 for a form it cannot use.
async function restoreConnectDraft() {
  if (!capsCache.monitor || !sessionMay("servers:write")) return;
  let res;
  try { res = await api("/api/servers/draft"); }
  catch (err) { toastError("Could not read the saved Connect form: " + ((err && err.message) || err)); return; }
  // What this tab typed last, stashed as the page went away, wins field by
  // field over the server's copy: a reload right after typing can cut off the
  // save that was still on its way (see stashConnectDraft).
  const local = readConnectStash();
  const saved = res && res.found ? res.draft : {};
  const d = Object.assign({}, saved, local || {});
  // What step 1 found describes the address it was found at: a later host or
  // port from this tab's stash makes it describe nothing.
  if (d.identified && (d.source_host !== saved.source_host || (d.source_port || "") !== (saved.source_port || ""))) delete d.identified;
  if (!(d.source_host || d.source_port || d.source_user)) return;
  if (document.getElementById("server-form")) return; // somebody already opened a form
  if (!document.getElementById("server-form-mount")) openServersModal();
  showConnectForm(d);
  // Bring the server's copy up to what is now on screen.
  const form = document.getElementById("server-form");
  if (local && form) saveConnectDraftSoon(form);
}

// The last-moment copy of the Connect form, for this tab only. The server's
// draft is saved as the person types, but a reload cancels a save still on its
// way, and the queued one behind it is never sent: the walk measured one field
// of three kept that way. On pagehide the fields are written here, which is
// synchronous and survives the reload. Never the password.
const CONNECT_STASH_KEY = "dbtrail.connectDraft";

function stashConnectDraft() {
  const form = document.getElementById("server-form");
  if (!form || !form.dataset.connect || form.dataset.done) return;
  const b = connectBody(form, false);
  try { sessionStorage.setItem(CONNECT_STASH_KEY, JSON.stringify({ source_host: b.source_host, source_port: b.source_port, source_user: b.source_user })); }
  catch (_) { /* storage off: the server's copy is all there is */ }
}

function readConnectStash() {
  try {
    const raw = sessionStorage.getItem(CONNECT_STASH_KEY);
    sessionStorage.removeItem(CONNECT_STASH_KEY);
    const v = raw ? JSON.parse(raw) : null;
    if (!v || typeof v !== "object") return null;
    const out = {};
    for (const k of ["source_host", "source_port", "source_user"]) if (typeof v[k] === "string" && v[k]) out[k] = v[k];
    return Object.keys(out).length ? out : null;
  } catch (_) { return null; }
}

// connectNotice is the answer to a check that got past every light: capture
// started (with what to look at when there is time), or it did not start.
// The findings on top in plain words, every check one click away.
function connectNotice(res) {
  const checks = (res.doctor && res.doctor.checks) || [];
  const count = (k, one, many) => k + " " + (k === 1 ? one : many);
  const all = el("details", { class: "notice-all" },
    el("summary", { text: "All " + count(checks.length, "check", "checks") }), doctorCards(checks));
  const opt = optionalSection(checks);
  if (res.started) {
    const warns = warningChecks(checks);
    if (!warns.length) {
      return { tone: "ok", title: "Capture started", summary: "Capture started",
        lines: ["Capture started for " + res.name + ". Changes appear within a minute."],
        content: [opt, all].filter(Boolean), button: "OK" };
    }
    return { tone: "warn", title: "Capture started", summary: "Capture started, with " + count(warns.length, "warning", "warnings"),
      lines: [warns.length === 1 ? "Check this when you can:" : "Check these when you can:"],
      content: [connectFindings(warns), opt, all].filter(Boolean), button: "OK" };
  }
  if (res.error) {
    // Every check passed and the start itself failed. Say so, and say whether
    // anything was left behind: "nothing happened" over a saved server that
    // is not capturing is the one answer this must never give.
    return { tone: "err", title: "Capture did not start", summary: "Capture did not start",
      lines: ["Every check passed, but capture did not start: " + res.error,
        res.kept ? "DBTrail could not undo everything it set up for this server. Look for it in the server list and remove it there before you try again."
          : "Nothing was saved. Press Check again to try again."],
      content: [opt, all].filter(Boolean), button: "Back to the form" };
  }
  // A check that failed is drawn as the Connect screen's lights (#1953) and
  // checked again by itself; it is never a notice.
  return null;
}

// codeOf returns the code blocks of a check's own fix, joined. The fix doctor
// wrote carries what the typed values cannot: the GRANT names the account as
// SHOW GRANTS reported it, which may not be '<user>'@'%'.
function codeOf(remediation, firstOnly) {
  const code = remediationBlocks(remediation).filter((b) => b.kind === "code").map((b) => b.text);
  return firstOnly ? (code[0] || "") : code.join("\n");
}

const BINLOG_SETTING_WORDS = {
  log_bin: "Binary logging is off. Turn it on in the server's configuration file, then restart MySQL:",
  binlog_format: "binlog_format must be ROW. Run this on the server:",
  binlog_row_image: "binlog_row_image must be FULL. Run this on the server:",
};

// connectFindingParts says one finding in plain words: text, then the code to
// run (copied exactly as shown), then the check's own fix as prose when that is
// the only place the answer is (the address found on this machine). kind ""
// or one this page does not know returns null: the caller shows the check's
// own card instead, so a finding is never dropped.
function connectFindingParts(c, inLight) {
  const subjects = c.subjects || [];
  const statements = c.statements || [];
  switch (c.kind) {
    case "host_unreachable":
      return { text: "DBTrail could not reach that host. Check the address in Host." };
    case "port_closed":
      return { text: "The host answered, but nothing is listening on that port. Check Port. MySQL usually uses 3306." };
    case "timeout":
      return { text: "Nothing answered in time. Check Host and Port, and that a firewall or security group lets this machine in." };
    case "access_denied":
      return { text: "MySQL refused the user or the password. Check both. If you have not run the permissions block yet, run it first.",
        note: c.detail ? "MySQL said: " + c.detail : "" };
    case "loopback_in_container":
      return { text: "Nothing answered on this machine's own address.", rem: c.remediation || "" };
    case "missing_privilege": {
      const code = codeOf(c.remediation);
      return code ? { text: "The user is missing " + (subjects.join(", ") || "a permission") + ". Run this on the server as an admin user:", code }
        : { text: "The user is missing " + (subjects.join(", ") || "a permission") + ".", rem: c.remediation || "" };
    }
    case "binlog_settings":
      return { text: subjects.map((v) => BINLOG_SETTING_WORDS[v] || (v + " is not set the way DBTrail needs. Run this on the server:")).join(" ") ||
        "A binary log setting is wrong. Run this on the server:",
      // Only the first block is the thing to run. The rest of the fix (the
      // MySQL 5.7 route, a configuration file) is not SQL, and pasted with it
      // would fail; it stays under "All checks".
      code: codeOf(c.remediation, true) };
    case "no_primary_key":
    case "not_innodb": {
      // Statements pair with Subjects, one per table, and one is empty where
      // no safe statement could be written (no free column name for a key):
      // that table is named instead, never dropped.
      const code = statements.filter(Boolean).join("\n");
      const byHand = subjects.filter((_, i) => !statements[i]);
      const pk = c.kind === "no_primary_key";
      return {
        // Under the keys light its title already says what is wrong, so the
        // card starts at what to run; the notice after a start has no title.
        text: pk ? (code && inLight ? "Run this on the server to add one:"
          : "DBTrail cannot capture a table without a primary key." + (code ? " Run this on the server to add one:" : ""))
          : "DBTrail captures InnoDB tables only." + (code ? " Run this on the server at a quiet moment, since it rewrites each table:" : ""),
        code,
        note: byHand.length ? (pk ? "Add a primary key by hand to: " : "Convert by hand: ") + byHand.join(", ") : "",
      };
    }
  }
  return null;
}

function connectFindings(checks, inLight) {
  const box = el("div", { class: "doctor-cards" });
  (checks || []).forEach((c) => {
    const p = connectFindingParts(c, inLight);
    if (!p) { box.append(doctorCards([c])); return; }
    const card = el("div", { class: "doctor-card " + (c.status === "warn" ? "warn" : "fail") });
    card.append(el("p", { text: p.text }));
    if (p.code) {
      card.append(el("pre", { class: "form-code", text: p.code }));
      card.append(el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(p.code, "Fix") }));
    }
    if (p.note) card.append(el("p", { text: p.note }));
    const rem = p.rem ? remediationEl(p.rem) : null;
    if (rem) card.append(rem);
    box.append(card);
  });
  return box;
}

function hideServerForm() {
  const mountEl = document.getElementById("server-form-mount");
  if (mountEl) mountEl.replaceChildren();
  const addWrap = document.getElementById("server-add-wrap");
  if (addWrap) addWrap.hidden = false;
}

// formMsg sets the one line above the form's buttons. reopen, when given,
// adds a Show button that opens again the notice this line summarizes.
function formMsg(text, isError, reopen) {
  const m = document.getElementById("server-form-msg");
  if (!m) return;
  m.className = "form-msg " + (isError ? "err" : "ok");
  m.textContent = text;
  if (reopen) m.append(" ", el("button", { class: "btn btn-sm btn-ghost", type: "button", id: "server-form-reopen", text: "Show", onclick: reopen }));
}

// fillRouteExtra says, under the forwarding fields, what the server has
// saved when that is more than the user name the field shows, and adds the
// control that removes a saved account.
function fillRouteExtra(form, prefill) {
  routePrefill.set(form, { user: prefill.route_user || "" });
  const box = form.querySelector("div#route-extra");
  if (!box) return;
  const stored = prefill.has_route || prefill.route_unreadable || prefill.route_is_capture;
  if (!stored) return;
  if (prefill.route_unreadable) {
    box.append(el("p", { class: "form-msg err", text: "The forwarding account saved for this server cannot be read (was the servers file edited by hand?), so the MySQL port cannot forward for this server. Type the forwarding user and password again, or remove it below. Saving this form without doing either leaves it as it is." }));
  } else if (prefill.route_is_capture) {
    box.append(el("p", { class: "form-hint", text: "The forwarding account saved for this server is the same account DBTrail captures with, so the port is not on a separate account. Set another forwarding user, or remove it below." }));
  } else if (prefill.route_host) {
    box.append(el("p", { class: "form-hint", text: "This forwarding account connects to " + prefill.route_host + (prefill.route_port ? ":" + prefill.route_port : "") + ", not to the source's address. It keeps that address when its user or password is changed here." }));
  }
  box.append(el("label", { class: "check" }, el("input", { type: "checkbox", name: "route_remove" }),
    el("span", { text: "Remove the forwarding account (the port then forwards with the source user)" })));
}

// keep-password semantics: omit password fields when blank (= keep stored).
function serverFormBody(form) {
  const f = form.elements;
  const body = {
    name: f.name.value.trim(),
    flavor: f.flavor.value,
    host: f.host.value.trim(), port: f.port.value.trim(), user: f.user.value.trim(), dbname: f.dbname.value.trim(),
    archive_s3: f.archive_s3.value.trim(),
    s3_endpoint: f.s3_endpoint.value.trim(), s3_path_style: f.s3_path_style.value, s3_region: f.s3_region.value.trim(),
    s3_access_key_id: f.s3_access_key_id.value.trim(),
    source_host: f.source_host.value.trim(), source_port: f.source_port.value.trim(),
    source_user: f.source_user.value.trim(), schemas: f.schemas.value.trim(),
    source_database: f.source_database.value.trim(),
    source_slot: f.source_slot.value.trim(), source_publication: f.source_publication.value.trim(),
  };
  if (f.password.value !== "") body.password = f.password.value;
  if (f.source_password.value !== "") body.source_password = f.source_password.value;
  if (f.s3_secret_access_key.value !== "") body.s3_secret_access_key = f.s3_secret_access_key.value;
  // The forwarding account is sent only when the reader changed it: left
  // out, the server keeps what it has, including an account this form never
  // showed (saved by somebody else since it opened, or one it could not
  // read). A typed password goes with the user the form shows. Removing is
  // the remove control alone. A form routeFormProblem would stop (an
  // emptied user, Remove beside something typed) sends nothing about the
  // account. A PostgreSQL source has none: its
  // fields are hidden, and what was typed in them before the source type
  // changed is not sent.
  if (f.route_user && f.flavor.value !== "postgres" && !routeFormProblem(form)) {
    const was = routePrefill.get(form) || { user: "" };
    const user = f.route_user.value.trim();
    if (f.route_remove && f.route_remove.checked) {
      body.route_user = "";
    } else if (f.route_password.value !== "") {
      if (user !== "") body.route_user = user;
      body.route_password = f.route_password.value;
    } else if (user !== "" && user !== was.user) {
      body.route_user = user;
    }
  }
  // No source host: the account the grant block filled in is not a source the
  // user asked for, so it stays behind and the entry saves as index-only.
  // Values somebody typed are sent, so a forgotten host still gets its error.
  if (!body.source_host && untouchedGrantDefaults(form)) {
    body.source_user = "";
    delete body.source_password;
  }
  return body;
}

async function editServer(id) {
  // Catch only the fetch: a render throw from showServerForm must surface
  // as an uncaught error, not masquerade as "failed to load server".
  let s;
  try { s = await api("/api/servers/" + encodeURIComponent(id)); }
  catch (err) { toastError("Could not load server: " + ((err && err.message) || err)); return false; }
  return showServerForm(s);
}

async function saveServer(form) {
  const id = form.elements.id.value;
  refreshGrants(form);
  if (missingSourceHost(form)) { formMsg("Fill in Source host, the database you want DBTrail to watch.", true); form.elements.source_host.focus(); return; }
  const routeProblem = routeFormProblem(form);
  if (routeProblem) { formMsg(routeProblem, true); form.elements.route_user.focus(); return; }
  const body = serverFormBody(form);
  let saved;
  try {
    saved = await api(id ? "/api/servers/" + encodeURIComponent(id) : "/api/servers", { method: id ? "PUT" : "POST", body });
  } catch (err) {
    const why = (err && err.message) || String(err);
    formMsg(why, true);
    openNotice({ tone: "err", title: "Could not save", lines: [why], button: "Back to the form" });
    return;
  }
  // The pending password now belongs to the server just saved; the next new
  // server gets its own.
  if (!id && body.source_password && body.source_password === pendingSourcePassword) pendingSourcePassword = "";

  // Zero-terminal auto-start: a monitor-capable process with a source DSN starts
  // streaming on save (after preflight). Doctor warnings keep the form open.
  if (capsCache.monitor && saved.has_source && !isLiveMonitorState(saved.monitor_state)) {
    formMsg("Running startup checks…", false);
    const res = await startMonitor(saved.id);
    await refreshServersList();
    if (res && res.started && !doctorWarnings(res.doctor) && !doctorOptional(res.doctor)) { hideServerForm(); toast("Monitoring started. Events will appear within a minute"); return; }
    // The entry now EXISTS: the form is re-shown from the saved entry so that
    // Save is a real retry (a PUT of this id) rather than a second POST of the
    // same name, which the registry refuses as a duplicate. Save stays
    // enabled on a failure: it IS the retry once the operator has fixed the
    // server (most checks are fixed on the database side, with the same form
    // values). showServerForm rebuilds the form, so it goes before the notice
    // and the summary line that points back to it.
    if (!showServerForm(saved)) {
      // The modal was closed while the checks ran: nothing on screen can
      // carry the outcome, so it goes to a toast that stays until dismissed.
      if (!res || res.requestError) toastError("Could not start capture for " + saved.name + ": " + ((res && res.requestError) || "no answer"));
      else if (res.started && !doctorWarnings(res.doctor)) openStartedNotice(res, saved.id, saved.name);
      else if (res.started) toastError("Monitoring started for " + saved.name + ", with warnings; open Servers and press Start to review them");
      else toastError("Startup checks failed for " + saved.name + "; open Servers and press Start to see what to fix");
      return;
    }
    // The re-shown form is an edit of the saved entry, which never carries
    // the password back, so its grant block would lose the password this
    // server was just saved with. The most common reason for a failed first
    // start is that the block has not been run yet: put it back.
    const again = document.getElementById("server-form");
    if (again && body.source_password) {
      again.elements.source_password.value = body.source_password;
      // This form filled it in too, so clicking it selects it like any other.
      generatedPasswords.set(again, body.source_password);
      refreshGrants(again);
    }
    showStartupOutcome(res);
    return;
  }
  hideServerForm();
  // A server that will never stream is saved, but the save must not read as
  // capture (#1607): the reason and the one action for it land on the row's
  // status slot, kept for this session across list rebuilds (noCaptureNotes),
  // next to the row's own mark. When the row cannot be found (the list
  // failed to load, the modal was closed mid-save) the reason goes to a
  // toast that persists until dismissed, never to a 2-second one.
  const why = noCaptureReason(saved);
  if (why) noCaptureNotes[saved.id] = why;
  await refreshServersList();
  if (why && !document.getElementById("srv-status-" + saved.id)) toastError(why);
  toast((id ? "Server updated" : "Server added") + (why ? "; it will not capture changes yet" : ""));
}

// noCaptureReason names why a saved server will not stream, or null when it
// will (or already does, or is the CLI entry). Two cases, each with its one
// remedy: this console runs as `serve`, which never captures; or the entry
// has no source connection.
function noCaptureReason(s) {
  if (!s || s.kind === "ephemeral" || isLiveMonitorState(s.monitor_state)) return null;
  // A capability check that FAILED also leaves capsCache.monitor false, and
  // that is not serve mode: telling a watch operator to restart as watch would
  // be a confident wrong remedy. The honest one there is a reload.
  if (!capsKnown) return "will not capture: the capability check failed when the web interface loaded, so capture cannot be started. Reload the page";
  if (!capsCache.monitor) return "will not capture: DBTrail was started as serve, which reads an index and never captures. Run bintrail-console watch to capture from this server";
  if (!s.has_source) return "will not capture: no source connection. Edit this server and add one";
  return null;
}

// showStartupOutcome shows what Save or Start did when capture did not start
// cleanly (#1769): a notice with the checks that failed, or with the warnings
// a started capture carries, and a summary line on the form that opens it
// again once it is closed.
function showStartupOutcome(res) {
  const n = startupNotice(res);
  // Closing lands on Save, the retry, not on the Name field the rebuild
  // focused.
  const save = () => document.querySelector("#server-form-mount button[type=submit]");
  formMsg(n.summary, n.tone === "err", () => openNotice(Object.assign(startupNotice(res), { returnFocus: save() })));
  openNotice(Object.assign(n, { returnFocus: save() }));
}

// startupNotice builds that notice. Only the failures, or only the warnings,
// are on top: every check, green ones included, sits one click away under
// "All N checks", since 14 green cards around one red one is how the red one
// got missed. Built fresh on every call, so Show reopens it whole.
function startupNotice(res, name) {
  if (!res || res.requestError) {
    // The server may well have answered, with an error (the checks could not
    // run, or the start failed after they passed): say what it said.
    return { tone: "err", title: "Capture did not start", summary: "Capture did not start: the start request failed",
      lines: ["The request to start capture failed: " + ((res && res.requestError) || "no answer") + ". Press Save to try again."],
      button: "Back to the form" };
  }
  const checks = (res.doctor && res.doctor.checks) || [];
  // Optional improvements are not warnings: they fold under their own
  // section and never pick the tone or the count.
  const picked = res.started ? warningChecks(checks) : checks.filter((c) => c.status === "fail");
  // A start refused with no failing check is not a shape the server sends;
  // show every check rather than nothing.
  const shown = picked.length ? picked : checks;
  const count = (k, one, many) => k + " " + (k === 1 ? one : many);
  const all = el("details", { class: "notice-all" },
    el("summary", { text: "All " + count(checks.length, "check", "checks") }), doctorCards(checks));
  const opt = optionalSection(checks);
  if (res.started && !picked.length) {
    // Where no form names the server, the notice does.
    const who = String(name || "").trim();
    return { tone: "ok", title: "Capture started", summary: "Capture started",
      lines: ["Capture started" + (who ? " for " + who : "") + ". Changes appear within a minute."],
      content: [opt, all].filter(Boolean), button: "OK" };
  }
  if (res.started) {
    return { tone: "warn", title: "Capture started", summary: "Capture started, with " + count(picked.length, "warning", "warnings"),
      lines: [picked.length === 1 ? "Check this when you can:" : "Check these when you can:"],
      content: [doctorCards(shown), opt, all].filter(Boolean), button: "OK" };
  }
  return { tone: "err", title: "Capture did not start", summary: "Capture did not start: " + count(picked.length, "check", "checks") + " failed",
    lines: [picked.length === 1 ? "Fix this on the database, then press Save again." : "Fix these on the database, then press Save again."],
    content: [doctorCards(shown), opt, all].filter(Boolean), button: "Back to the form" };
}

async function deleteServer(s) {
  if (!window.confirm('Remove server "' + s.name + '"? This only removes the saved connection; nothing happens to the server itself.')) return;
  try { await api("/api/servers/" + encodeURIComponent(s.id), { method: "DELETE" }); }
  catch (err) { toastError("Could not remove server: " + ((err && err.message) || err)); return; }
  if (currentServer === s.id) { await switchServer(""); const sel = document.getElementById("server-select"); if (sel) sel.value = defaultServerId; }
  await refreshServersList();
  toast("Server removed");
}

// test / doctor / monitor -----------------------------------------------------

// s3TestText renders the S3 half of a Test connection result, one entry per
// bucket of the server's S3 store. needs_secret and needs_keys were not
// probed: the probe signs with credentials nobody typed only where the saved
// server already sends them. not_applied: the saved store is not in use.
function s3TestText(res) {
  return (res.s3 || []).map((b) => {
    const name = b.bucket ? "S3 " + b.bucket : "S3 store";
    if (b.needs_secret) return "○ " + name + ": type the S3 secret key to test these keys";
    if (b.needs_keys) return "○ " + name + ": save the server, or type S3 keys, to test these settings";
    const probed = b.ok ? "✓ " + name + " · " + b.latency_ms + " ms" : "✗ " + name + ": " + (b.error || "unreachable");
    return b.not_applied ? probed + " · ! " + name + " is saved but DBTrail is not using it; its log says why" : probed;
  }).join(" · ");
}

// routeTestText renders Test connection's login with the forwarding account
// (#2079), named as such so a failure is not read as the index's or the
// source user's. needs_password: not tried, because the saved password would
// have gone to an address or a user it was not saved for.
function routeTestText(res) {
  const r = res.route;
  if (!r) return "";
  const name = "forwarding account" + (r.user ? " " + r.user : "");
  if (r.needs_password) return "○ " + name + ": type its password to test it with these settings";
  // Not a failure and not a pass: this process has no port, so no login.
  if (r.skipped) return "○ " + name + ": not tried here (" + r.skipped + ")";
  return r.ok ? "✓ " + name + " logs in · " + r.latency_ms + " ms" : "✗ " + name + ": " + (r.error || "could not log in");
}

function testResultText(res) {
  const s3 = [routeTestText(res), s3TestText(res)].filter(Boolean).join(" · ");
  const withS3 = (text) => (s3 ? text + " · " + s3 : text);
  // provision_pending: a monitored source whose per-source index isn't created
  // yet (Start creates it). Reachable server, normal pre-Start state — render
  // it as a neutral hint, not a red failure.
  if (res.provision_pending) return withS3("○ " + (res.error || "index not created yet; click Start"));
  if (!res.ok) return withS3("✗ " + (res.error || "unreachable"));
  // Named: the connection tested is the INDEX (where captured changes are
  // stored), the one the row prints. Beside a NO SOURCE mark, a bare "ok"
  // read as a contradiction (#1856); the source is not probed here.
  let s = "✓ index ok · " + res.latency_ms + " ms";
  if (res.server_version) s += " · MySQL " + res.server_version;
  // has_index/schema_current are tri-state: absent = the metadata lookup itself
  // failed (unknown) — never render that as the confident negative.
  if (res.has_index === false) s += " · doesn't look like a DBTrail index (missing the binlog_events table)";
  else if (res.has_index === undefined || res.schema_current === undefined) s += " · index metadata unavailable";
  else if (res.schema_current === false) s += " · index schema outdated (run bintrail index/stream once)";
  return withS3(s);
}

// testResultClass colors a Test connection result: red when the index, the
// forwarding account or any
// bucket failed, neutral while something waits on the operator (index not
// created yet, a secret to type), green otherwise.
function testResultClass(res) {
  const s3 = res.s3 || [];
  const route = res.route;
  if (s3.some((b) => b.not_applied || (!b.ok && !b.needs_secret && !b.needs_keys))) return "err";
  if (route && !route.ok && !route.needs_password && !route.skipped) return "err";
  if (!res.ok && !res.provision_pending) return "err";
  if (res.provision_pending || (route && route.needs_password) || s3.some((b) => b.needs_secret || b.needs_keys)) return "pending";
  return "ok";
}

// testServerForm answers in a notice (#1769). The button carries the busy
// state itself, so the click reads as taken before any answer arrives.
async function testServerForm(form) {
  const id = form.elements.id.value;
  refreshGrants(form);
  if (missingSourceHost(form)) { formMsg("Fill in Source host, the database you want DBTrail to watch.", true); form.elements.source_host.focus(); return; }
  const routeProblem = routeFormProblem(form);
  if (routeProblem) { formMsg(routeProblem, true); form.elements.route_user.focus(); return; }
  const body = serverFormBody(form);
  const btn = form.querySelector("#server-test");
  // Dropped when the form that asked is gone (Cancel, or another server's
  // form took its place): over that form it would describe the wrong server.
  const show = (notice) => { if (btn && btn.isConnected) openNotice(Object.assign({ title: "Test connection", button: "Back to the form", returnFocus: btn }, notice)); };
  formMsg("", false);
  if (btn) { btn.disabled = true; btn.textContent = "Testing…"; }
  try {
    const res = await api(id ? "/api/servers/" + encodeURIComponent(id) + "/test" : "/api/servers/test", { method: "POST", body });
    show(res.doctor ? unsavedTestNotice(res) : { tone: testResultClass(res), lines: [testResultText(res)] });
  } catch (err) { show({ tone: "err", lines: ["✗ " + ((err && err.message) || err)] }); }
  finally { if (btn) { btn.disabled = false; btn.textContent = "Test connection"; } }
}

// unsavedTestNotice is Test's answer for a server not saved yet (#1767): the
// source half of the startup checks Save runs, so Test cannot pass what Save
// would refuse. The failures on top, else the warnings, else one line; every
// check is one click away, and the S3 store's result follows when there is one.
function unsavedTestNotice(res) {
  const checks = res.doctor.checks || [];
  const fails = checks.filter((c) => c.status === "fail");
  const warns = warningChecks(checks);
  const count = (k, one, many) => k + " " + (k === 1 ? one : many);
  const all = el("details", { class: "notice-all" },
    el("summary", { text: "All " + count(checks.length, "check", "checks") }), doctorCards(checks));
  const opt = optionalSection(checks);
  const s3 = [routeTestText(res), s3TestText(res)].filter(Boolean).join(" · ");
  const s3Line = s3 ? [s3] : [];
  if (fails.length) {
    return { tone: "err", lines: [(fails.length === 1 ? "Capture cannot start from this database yet. Fix this first:" : "Capture cannot start from this database yet. Fix these first:")].concat(s3Line),
      content: [doctorCards(fails), opt, all].filter(Boolean) };
  }
  // A clean database with an S3 store that failed is still a red answer.
  const s3Bad = testResultClass({ ok: true, s3: res.s3, route: res.route }) === "err";
  if (warns.length) {
    return { tone: s3Bad ? "err" : "warn", lines: ["✓ The database is ready to capture. Check these when you can:"].concat(s3Line),
      content: [doctorCards(warns), opt, all].filter(Boolean) };
  }
  return { tone: s3Bad ? "err" : "ok", lines: ["✓ The database is ready to capture."].concat(s3Line), content: [opt, all].filter(Boolean) };
}

async function testServerRow(id) {
  const slot = document.getElementById("srv-status-" + id);
  if (slot) { slot.className = "srv-status"; slot.textContent = "testing…"; }
  try {
    const res = await api("/api/servers/" + encodeURIComponent(id) + "/test", { method: "POST", body: {} });
    const note = noCaptureNotes[id]; // the row rebuild re-derives it; here it only needs to survive the test result
    const noSource = slot && slot.getAttribute("data-nosrc") === "1";
    if (slot) { slot.className = "srv-status " + testResultClass(res); slot.textContent = testResultText(res) + (note ? " · ○ " + note : "") + (noSource ? " · ○ no source database set, nothing to capture" : ""); }
  } catch (err) { if (slot) { slot.className = "srv-status err"; slot.textContent = "✗ " + ((err && err.message) || err); } }
}

function doctorWarnings(report) { return !!(report && report.warnings > 0); }
function doctorOptional(report) { return !!(report && report.optional > 0); }

// warningChecks and optionalChecks split a report's warns. An optional one
// (doctor marks it) is something capture works fine without, such as the SQL
// statement behind each change: it never counts as a warning, never colors a
// notice, and sits folded under "Optional improvements".
function warningChecks(checks) { return (checks || []).filter((c) => c.status === "warn" && !c.optional); }
function optionalChecks(checks) { return (checks || []).filter((c) => c.status === "warn" && c.optional); }

// optionalSection is that fold, closed: each item says in one line what it
// adds, then the statement to copy, exactly as doctor wrote it. Null when
// there is nothing optional to offer.
function optionalSection(checks) {
  const opt = optionalChecks(checks);
  if (!opt.length) return null;
  const box = el("div", { class: "doctor-cards" });
  opt.forEach((c) => {
    const card = el("div", { class: "doctor-card note optional" });
    const blocks = remediationBlocks(c.remediation);
    // A read that failed has no fix to offer: say which one and why.
    if (!blocks.length) card.append(el("p", { text: c.name + (c.detail ? ": " + c.detail : "") }));
    const body = el("div", { class: "dc-body" });
    blocks.forEach((b) => {
      if (b.kind === "p") body.append(el("p", { text: b.text }));
      else if (b.kind === "list") body.append(el("ul", {}, ...b.items.map((it) => el("li", { text: it }))));
      else {
        body.append(el("pre", { class: "form-code", text: b.text }));
        body.append(el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(b.text, "Statement") }));
      }
    });
    if (blocks.length) card.append(body);
    box.append(card);
  });
  return el("details", { class: "notice-all notice-optional" },
    el("summary", { text: "Optional improvements (" + opt.length + ")" }), box);
}

// remediationBlocks splits a fix doctor wrote into paragraphs, lists and code
// (#1777). doctor wraps its fixes at about 80 columns for a terminal, and shown
// verbatim in a box narrower than that every prose line broke a second time.
// It follows the convention every doctor fix is written to, the checks
// registered through ext.RegisterDoctorCheck included: prose flush left,
// commands and config indented, lists as "  - ".
// Flush-left lines in a row are one paragraph and reflow. An indented run is a
// list when each line is a "- " item or a deeper continuation of one, and code
// otherwise, kept exactly as written minus its common indent, so a numbered
// step with a command under it stays readable. Blank lines separate blocks.
function remediationBlocks(text) {
  const lines = String(text || "").split("\n");
  const width = (l) => l.match(/^[ \t]*/)[0].replace(/\t/g, "    ").length;
  const blocks = [];
  let i = 0;
  while (i < lines.length) {
    if (!lines[i].trim()) { i++; continue; }
    const run = [];
    const flush = width(lines[i]) === 0;
    while (i < lines.length && lines[i].trim() && (width(lines[i]) === 0) === flush) run.push(lines[i++]);
    if (flush) { blocks.push({ kind: "p", text: run.map((l) => l.trim()).join(" ") }); continue; }
    const base = Math.min(...run.map(width));
    const items = [];
    let list = true;
    run.forEach((l) => {
      const body = l.trim();
      if (width(l) === base && body.startsWith("- ")) items.push(body.slice(2).trim());
      else if (width(l) > base && items.length) items[items.length - 1] += " " + body;
      else list = false;
    });
    blocks.push(list ? { kind: "list", items } :
      { kind: "code", text: run.map((l) => " ".repeat(width(l) - base) + l.trim()).join("\n") });
  }
  return blocks;
}

// remediationEl draws remediationBlocks: paragraphs and lists in the page's
// own type, code in a box whose exact text is what gets copied. Null when the
// fix has nothing to show.
function remediationEl(text) {
  const blocks = remediationBlocks(text);
  if (!blocks.length) return null;
  const box = el("div", { class: "dc-rem" });
  blocks.forEach((b) => {
    if (b.kind === "p") box.append(el("p", { text: b.text }));
    else if (b.kind === "list") box.append(el("ul", {}, ...b.items.map((it) => el("li", { text: it }))));
    else box.append(el("pre", { text: b.text }));
  });
  return box;
}

// doctorCards renders startup checks as cards: the check, its detail, and the
// paste-ready fix doctor wrote for it.
function doctorCards(checks) {
  const box = el("div", { class: "doctor-cards" });
  (checks || []).forEach((chk) => {
    const known = ["pass", "fail", "warn"].includes(chk.status) ? chk.status : "skip";
    // An optional improvement is not a warning: a quiet note, never amber.
    const status = known === "warn" && chk.optional ? "note" : known;
    const mark = { pass: "✓", fail: "✗", warn: "!", skip: "–", note: "○" }[status];
    const card = el("div", { class: "doctor-card " + status });
    card.append(el("span", { class: "dc-mark", text: mark }));
    const bodyEl = el("div", { class: "dc-body" }, el("div", { class: "dc-name", text: chk.name + (chk.detail ? ": " + chk.detail : "") }));
    const rem = remediationEl(chk.remediation);
    if (rem) bodyEl.append(rem);
    card.append(bodyEl);
    box.append(card);
  });
  return box;
}

// startMonitor returns the start result, or { requestError } when the request
// itself failed. The caller says so where the operator is looking: the Save
// path in its notice, the row's Start in a lasting toast.
async function startMonitor(id) {
  try { return await api("/api/servers/" + encodeURIComponent(id) + "/monitor/start", { method: "POST", body: {} }); }
  catch (err) { return { requestError: (err && err.message) || String(err) }; }
}

async function startMonitorRow(id) {
  const slot = document.getElementById("srv-status-" + id);
  if (slot) { slot.className = "srv-status"; slot.textContent = "running checks…"; }
  const res = await startMonitor(id);
  await refreshServersList();
  if (!res || res.requestError) { toastError("Could not start: " + ((res && res.requestError) || "no answer")); return; }
  if (res.started && !doctorWarnings(res.doctor) && !doctorOptional(res.doctor)) { toast("Monitoring started"); return; }
  // Started, with only optional improvements to offer: the notice alone. A
  // form under it would leave the operator inside the edit form of a server
  // that captures, with the focus on Save.
  if (res.started && !doctorWarnings(res.doctor)) { openStartedNotice(res, id, serverNames.get(id)); return; }
  // The same notice Save opens, over this server's form: Save there is the
  // retry, since it starts a server that is not capturing yet.
  const opened = await editServer(id);
  if (opened) showStartupOutcome(res);
  else if (res.started) { toast("Monitoring started, with warnings"); }
  else { toastError("Startup checks failed"); }
}

// openStartedNotice opens the notice of a start that worked where no form is
// under it: it names the server, and closing it lands on something still on
// the page (the button that asked is gone once its row or card is redrawn).
function openStartedNotice(res, id, name) {
  const slot = document.getElementById("srv-status-" + id);
  const row = slot && slot.parentNode;
  const back = (row && row.querySelector(".row-acts button")) || document.getElementById("server-select");
  openNotice(Object.assign(startupNotice(res, name), { returnFocus: back }));
}

// ── notice: an action's outcome, centered above the dialog that asked ──────
//
// Its own mount above #modal (#1769), the way ⌘K has its own: the servers
// dialog and the form under it stay exactly as they were, every typed value
// included, and closing the notice goes back to them. While it is up the rest
// of the page is inert, so clicks, Tab and screen readers stay inside it;
// Escape, the button and a click outside close only this layer.
let noticeInerted = [];
let noticeReturnFocus = null;

function noticeOpen() {
  const m = document.getElementById("notice-mount");
  return !!(m && m.firstChild);
}

// openNotice shows { tone: ok|err|warn|pending, title, lines, content,
// button, returnFocus }. A second call while one is up replaces its content
// and keeps the first caller's focus target.
function openNotice(opts) {
  const mount = document.getElementById("notice-mount");
  // The sign-in gate is the answer while it is up; nothing here can be saved.
  if (!mount || loginGateRaised) return;
  if (!noticeOpen()) noticeReturnFocus = opts.returnFocus || document.activeElement;
  const box = el("div", { class: "notice " + (opts.tone || "pending"),
    role: opts.tone === "err" ? "alertdialog" : "dialog", "aria-modal": "true", "aria-labelledby": "notice-title" });
  box.append(el("div", { class: "notice-head" }, el("h2", { class: "modal-title", id: "notice-title", text: opts.title })));
  const body = el("div", { class: "notice-body" });
  for (const line of opts.lines || []) body.append(el("p", { class: "notice-line", text: line }));
  for (const node of [].concat(opts.content || [])) body.append(node);
  box.append(body);
  // The button row sits outside the scrolling body, so a long list of checks
  // never pushes the way back out of reach.
  const close = el("button", { class: "btn btn-primary", type: "button", id: "notice-close", text: opts.button || "OK", onclick: closeNotice });
  box.append(el("div", { class: "notice-foot" }, close));
  const scrim = el("div", { class: "notice-scrim" }, box);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) closeNotice(); });
  mount.replaceChildren(scrim);
  if (!noticeInerted.length) {
    noticeInerted = Array.from(document.body.children).filter((n) => n !== mount && !n.inert);
    noticeInerted.forEach((n) => { n.inert = true; });
  }
  close.focus();
}

function closeNotice() {
  const mount = document.getElementById("notice-mount");
  if (!mount || !mount.firstChild) return;
  mount.replaceChildren();
  noticeInerted.forEach((n) => { n.inert = false; });
  noticeInerted = [];
  const back = noticeReturnFocus;
  noticeReturnFocus = null;
  // The Save button that asked was rebuilt with the form; land on the new one.
  const target = back && back.isConnected ? back : document.querySelector("#server-form-mount button[type=submit]");
  if (target && target.focus) target.focus();
}

async function stopMonitorRow(id) {
  try { await api("/api/servers/" + encodeURIComponent(id) + "/monitor/stop", { method: "POST", body: {} }); toast("Monitoring stopped"); }
  catch (err) { toastError("Could not stop: " + ((err && err.message) || err)); }
  await refreshServersList();
}

// ── command palette (⌘K) ──────────────────────────────────────────────────

let cmdkSel = 0, cmdkItems = [];

function cmdkCommands() {
  // Same order as the sidebar (#1863). Snapshots is one entry for the three
  // pages that merged into it (#1573), findable by the names they had:
  // somebody who has used this console types "backup" or "verif", and an
  // entry they cannot find reads as a feature that was removed. alt is
  // lowercased and matched like the label, and never shown. Not gated on the
  // daemon, because the page opens on a standalone serve too.
  const cmds = [
    { group: "Navigate", label: "Overview", run: () => navigate("overview") },
    { group: "Navigate", label: "Snapshots",
      alt: ["backups", "verification", "snapshot settings"], run: () => navigate("snapshots") },
    { group: "Navigate", label: "Status", run: () => navigate("status") },
    { group: "Navigate", label: "Events", run: () => navigate("events") },
    { group: "Navigate", label: "Schema changes", run: () => navigate("schema-changes") },
    { group: "Navigate", label: "Recover", run: () => navigate("recover") },
  ];
  if (capsCache.reconstruct) cmds.push({ group: "Navigate", label: "Time-travel", run: () => navigate("timetravel") });
  cmds.push({ group: "Navigate", label: "MCP Server", run: () => navigate("connect") });
  if (capsCache.monitor) cmds.push({ group: "Navigate", label: "Retention", run: () => navigate("retention") });
  cmds.push({ group: "Navigate", label: "Access profiles", run: () => navigate("access-profiles") });
  cmds.push({ group: "Actions", label: "Manage servers", run: () => { closeCmdk(); openServersModal(); } });
  if (capsCache.monitor) cmds.push({ group: "Actions", label: "Configure rotation…", run: () => { closeCmdk(); showRotationDialog(); } });
  if (capsCache.auth) {
    cmds.push({
      group: "Actions",
      label: capsCache.auth.password_set ? "Change password…" : "Set password…",
      run: () => { closeCmdk(); showPasswordDialog(); },
    });
    if (capsCache.auth.auth_kind === "session") {
      cmds.push({ group: "Actions", label: "Log out", run: () => { closeCmdk(); doLogout(); } });
    }
  }
  cmds.push({ group: "Actions", label: "Search events for…", hint: "type then ↵", search: true });
  return cmds;
}

function openCmdk() {
  if (loginGateRaised) return; // the sign-in gate owns the screen
  const mount = document.getElementById("cmdk-mount");
  clear(mount);
  const scrim = el("div", { class: "cmdk-scrim open" });
  const panel = el("div", { class: "cmdk", role: "dialog", "aria-label": "Commands" });
  const top = el("div", { class: "cmdk-top" });
  top.append(icon("search"));
  const q = el("input", { class: "cmdk-q", id: "cmdk-q", placeholder: "Search commands, or type to find events…", autocomplete: "off", spellcheck: "false" });
  top.append(q);
  top.append(el("span", { class: "cmdk-escpill", text: "esc" }));
  panel.append(top);
  panel.append(el("div", { class: "cmdk-list", id: "cmdk-list" }));
  scrim.append(panel);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) closeCmdk(); });
  mount.append(scrim);
  q.addEventListener("input", () => renderCmdk(q.value));
  q.addEventListener("keydown", cmdkKeydown);
  renderCmdk("");
  q.focus();
}
function closeCmdk() { document.getElementById("cmdk-mount").replaceChildren(); }

function renderCmdk(query) {
  const list = document.getElementById("cmdk-list");
  if (!list) return;
  const q = (query || "").toLowerCase().trim();
  let cmds = cmdkCommands();
  if (q) cmds = cmds.filter((c) => c.search || c.label.toLowerCase().includes(q)
    || (c.alt || []).some((a) => a.toLowerCase().includes(q)));
  cmdkItems = cmds; cmdkSel = 0;
  clear(list);
  if (!cmds.length) { list.append(el("div", { class: "cmdk-empty", text: "No commands match." })); return; }
  let group = null;
  cmds.forEach((c, i) => {
    if (c.group !== group) { group = c.group; list.append(el("div", { class: "cmdk-group", text: group })); }
    const item = el("button", { class: "cmdk-item" + (i === cmdkSel ? " sel" : ""), type: "button",
      "data-idx": i, onclick: () => runCmdk(c, query) });
    item.append(icon("search", "cmdk-ic"));
    item.append(el("span", { class: "cmdk-label", text: c.search && q ? 'Search events: "' + query.trim() + '"' : c.label }));
    if (c.hint) item.append(el("span", { class: "cmdk-hint", text: c.hint }));
    list.append(item);
  });
}
function runCmdk(c, query) {
  if (c.search) { closeCmdk(); navigate("events", query && query.trim() ? { q: query.trim() } : null); return; }
  closeCmdk(); c.run();
}
function cmdkKeydown(e) {
  const list = document.getElementById("cmdk-list");
  if (e.key === "Escape") { closeCmdk(); return; }
  if (e.key === "ArrowDown") { e.preventDefault(); cmdkSel = Math.min(cmdkItems.length - 1, cmdkSel + 1); }
  else if (e.key === "ArrowUp") { e.preventDefault(); cmdkSel = Math.max(0, cmdkSel - 1); }
  else if (e.key === "Enter") { e.preventDefault(); const c = cmdkItems[cmdkSel]; if (c) runCmdk(c, e.target.value); return; }
  else return;
  $all(".cmdk-item", list).forEach((n) => n.classList.toggle("sel", Number(n.dataset.idx) === cmdkSel));
  const sel = $(".cmdk-item.sel", list); if (sel) sel.scrollIntoView({ block: "nearest" });
}

// ── global keyboard ──────────────────────────────────────────────────────────

function globalKeydown(e) {
  // The sign-in gate is modal: no shortcuts reach the workspace behind it.
  if (loginGateRaised) return;
  // A notice sits above every dialog and owns the keyboard while it is up
  // (#1769): Escape closes it and only it, never the form under it.
  if (noticeOpen()) {
    if (e.key === "Escape") { e.preventDefault(); closeNotice(); }
    return;
  }
  // Escape closes whatever dialog occupies the shared #modal slot (#968). The
  // ⌘K palette lives in its own mount and closes itself; the sign-in gate is
  // unreachable here (guard above), so it stays un-dismissable by design.
  if (e.key === "Escape") {
    const cmdk = document.getElementById("cmdk-mount");
    if (cmdk && cmdk.firstChild) return;
    const modalMount = document.getElementById("modal");
    // The recover busy dialog owns its Escape (a capture-phase handler that
    // also aborts the in-flight fetch, #1363) — never generically empty the
    // mount over it. This branch is still reachable with that dialog open:
    // cmdkKeydown closes the palette without stopping propagation, so the
    // SAME Escape that closed the palette lands here with the cmdk check
    // above already passing.
    if (modalMount && modalMount.querySelector(".busy-modal")) return;
    if (modalMount && modalMount.firstChild) {
      e.preventDefault();
      // The servers dialog closes through its own close, which renders the
      // Overview again for a server just added (#1606).
      if (modalMount.querySelector("#servers-list")) closeServersModal();
      else modalMount.replaceChildren();
    }
    return;
  }
  // ⌘K / Ctrl+K opens the palette anywhere.
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") { e.preventDefault(); openCmdk(); return; }
  // j/k/↵/u row nav — only on Events, only when not typing in a field.
  const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement && document.activeElement.tagName);
  if (typing || routeFromLocation() !== "events") return;
  const rows = $all(".ev-row", VIEW());
  if (e.key === "j") { e.preventDefault(); moveCursor(1); }
  else if (e.key === "k") { e.preventDefault(); moveCursor(-1); }
  else if (e.key === "Enter") {
    // A focused interactive element handles its own Enter (buttons, links,
    // expandable rows) — only drive the j/k cursor row otherwise (#968).
    const a = document.activeElement;
    if (a && (a.classList.contains("ev-row") || /^(BUTTON|A|SUMMARY)$/.test(a.tagName))) return;
    if (cursorIdx >= 0 && rows[cursorIdx]) { e.preventDefault(); rows[cursorIdx].click(); }
  }
  else if (e.key === "u") { if (cursorIdx >= 0 && lastEvents[cursorIdx]) { e.preventDefault(); undoEvent(lastEvents[cursorIdx]); } }
}

// ── init ─────────────────────────────────────────────────────────────────────

// bootSequence is the load-bearing startup order: servers (reconcile
// selection) → caps → route. ONE definition, called from both the normal boot
// and the post-login path — two inline copies of an order-sensitive sequence
// is how wrong-shape-UI bugs come back. Returns the server list (null when
// the boot aborted on a dead credential — the sign-in gate is already up).
async function bootSequence() {
  let servers = [];
  // renderRoute() below clears #view, so an in-view error here would be wiped;
  // toast it instead. If the backend is down, the chosen view surfaces its own
  // error when its fetch fails.
  try { servers = await loadServers(); } catch (err) {
    if (err && err.status === 401) return null;
    toastError("Could not load servers: " + ((err && err.message) || err));
  }
  try { await gateCapabilities(); } catch (err) {
    if (err && err.status === 401) return null;
    throw err;
  }
  renderRoute();
  // A Connect form left half-filled before a reload opens again (#1804).
  restoreConnectDraft();
  return servers;
}

async function init() {
  document.getElementById("server-select").addEventListener("change", (e) => switchServer(e.target.value));
  document.getElementById("manage-servers").addEventListener("click", openServersModal);
  document.getElementById("open-cmdk").addEventListener("click", openCmdk);
  document.getElementById("logout-btn").addEventListener("click", doLogout);
  document.addEventListener("keydown", globalKeydown);
  // Capture phase on purpose — see toastEscape. An Escape that closes a dialog
  // must not also dismiss an error notice behind it, and only the capture phase
  // still sees that the dialog was open.
  document.addEventListener("keydown", toastEscape, true);
  // A tab shown again brings the Overview up to date at once (#1801).
  document.addEventListener("visibilitychange", ovVisibilityChanged);
  // A half-filled Connect form is stashed as the page goes away (#1804).
  window.addEventListener("pagehide", stashConnectDraft);

  // Sidebar nav (real hrefs upgraded to in-place swaps). A manual nav starts
  // fresh — clear any carried "Undo" context so the sidebar's Recover link
  // never shows a stale banner (the undoEvent bridge sets it and navigates
  // directly, bypassing this handler, so its context survives).
  $all(".nav-item").forEach((a) => a.addEventListener("click", (e) => {
    e.preventDefault();
    // Route-less nav items are modal triggers (e.g. #nav-rotation) with their
    // own bindings — navigate(undefined) would coerce to /overview and repaint
    // the view behind the modal.
    if (!a.dataset.route) return;
    pendingRecover = null;
    navigate(a.dataset.route);
  }));
  document.getElementById("nav-rotation").addEventListener("click", showRotationDialog);
  window.addEventListener("popstate", onPopState);

  // Pre-auth gate: with no stored token, first try the HttpOnly session
  // cookie a login in another tab may have left (#1370) — on success the tab
  // proceeds signed-in with TOKEN empty (api() omits the Authorization header
  // and the cookie authenticates every call). Otherwise ask the
  // (unauthenticated) probe how this console authenticates BEFORE firing data
  // fetches that are guaranteed 401s. First run with no credential →
  // create-password screen; password configured → sign-in form; token mode →
  // the printed-link hint.
  if (!TOKEN && !(await probeCookieSession())) {
    let auth = {};
    try { auth = await fetchAuthInfo(); } catch (_) { /* server down — fall through, the view will surface it */ }
    if (auth.setup) { showLoginOverlay({ setup: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
    if (auth.password_login) { showLoginOverlay({ passwordLogin: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
    // An external provider can be the sole credential (no token, no
    // password): raise the gate with the SSO entry instead of toasting about
    // a printed token link that never existed in that deployment.
    if (auth.sso_start) { showLoginOverlay({ passwordLogin: false, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
    toastError("No token in the URL. Open the link that bintrail-console printed.");
  }

  const servers = await bootSequence();
  if (servers === null) return; // dead credential: the sign-in gate is up

  // First-run onboarding: a monitor-capable process with no source yet opens the
  // servers modal once per tab so the operator can add one without a terminal.
  try {
    if (capsCache.monitor && servers.every((s) => !s.has_source) && !sessionStorage.getItem(ONBOARD_KEY)) {
      sessionStorage.setItem(ONBOARD_KEY, "1");
      openServersModal();
    }
  } catch (_) {}
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
else init();
