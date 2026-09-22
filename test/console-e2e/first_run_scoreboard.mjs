// The first-run walk's scoreboard (#1800): what each column means, how it is
// computed from the walk's event log, and how a measured scoreboard is
// compared with the committed baseline (the ratchet) or with the targets.
// Pure functions only; first_run_walk.mjs produces the log and
// first_run_walk.test.mjs pins every rule here.
import { readFileSync } from "node:fs";

export const NOT_MEASURABLE = "not measurable";

// ── words ──────────────────────────────────────────────────────────────────

// A word is a whitespace-separated token with at least one letter or digit,
// so marks such as ✓ or a lone dash are not words and "shop.orders" is one.
export function countWords(text) {
  return String(text || "").split(/\s+/).filter((t) => /[\p{L}\p{N}]/u.test(t)).length;
}

// The closed word list agreed for the first run: none of these may appear in
// a sentence a person reads there. Each is listed with the forms it takes in
// English, so "Backups" and "monitoring" are caught and "trackpad" is not.
// Something copied (SQL, a command) may contain them; the measurement leaves
// code and pre text out before it gets here.
const BANNED_FORMS = {
  index: ["index", "indexes", "indexed", "indexing", "indexer", "indexers", "indices"],
  source: ["source", "sources", "sourced", "sourcing"],
  preflight: ["preflight", "preflights"],
  console: ["console", "consoles"],
  baseline: ["baseline", "baselines"],
  backup: ["backup", "backups"],
  daemon: ["daemon", "daemons"],
  watch: ["watch", "watches", "watched", "watching", "watcher", "watchers"],
  monitor: ["monitor", "monitors", "monitored", "monitoring"],
  protect: ["protect", "protects", "protected", "protecting", "protection", "protections"],
  track: ["track", "tracks", "tracked", "tracking", "tracker", "trackers"],
  stream: ["stream", "streams", "streamed", "streaming"],
};
const FORM_TO_WORD = new Map();
for (const [word, forms] of Object.entries(BANNED_FORMS)) for (const f of forms) FORM_TO_WORD.set(f, word);
// \b around each form: "bintrail_idx" and "index_dsn" run on into an
// underscore, which is a word character, so identifiers never match.
const BANNED_RE = new RegExp("\\b(" + [...FORM_TO_WORD.keys()].join("|") + ")\\b|—", "gi");

export function bannedHits(text) {
  const out = [];
  for (const m of String(text || "").matchAll(BANNED_RE)) {
    out.push({ word: m[0] === "—" ? "em dash" : FORM_TO_WORD.get(m[0].toLowerCase()), at: m.index });
  }
  return out;
}

// Two screens showing the same sentence show one thing to fix, so sentences
// are compared after folding spacing and numbers ("capture lag 146s" and
// "capture lag 10s" are one sentence).
export function normalizeChunk(text) {
  return String(text || "").replace(/\s+/g, " ").trim().replace(/\d+/g, "#");
}

export function bannedFromChunks(chunks) {
  const seen = new Set();
  const perWord = {};
  const examples = [];
  let total = 0;
  for (const c of chunks || []) {
    const key = normalizeChunk(c);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    const hits = bannedHits(key);
    if (!hits.length) continue;
    total += hits.length;
    for (const h of hits) perWord[h.word] = (perWord[h.word] || 0) + 1;
    examples.push(key.length > 120 ? key.slice(0, 117) + "..." : key);
  }
  return { total, perWord: Object.fromEntries(Object.entries(perWord).sort()), examples };
}

// ── columns ────────────────────────────────────────────────────────────────

// better: "lower" | "higher" | "true". target: the goal the issue sets, or
// undefined where it sets none (then target mode leaves the column alone).
// ratchet: false for a timing number, which is shown but would flake if CI
// failed on it; its stable sibling carries the ratchet.
// layout: true for a number that depends on where text WRAPS, which depends
// on the fonts of the machine that measured it. Those are compared only
// against a baseline recorded on the same platform; everywhere else they are
// shown and not compared, because a number that moves with the font is not
// evidence about the product. Every other column counts things — clicks,
// fields, sentences, elements — and is the same on any machine.
export const COLUMNS = {
  clicks_to_first_snapshot: { label: "Clicks to the first snapshot (walked)", better: "lower", target: 5 },
  clicks_to_first_snapshot_minimum: { label: "Clicks to the first snapshot (minimum)", better: "lower", target: 5 },
  clicks_to_capture: { label: "Clicks until capture runs", better: "lower" },
  fields_typed: { label: "Fields typed", better: "lower", target: 4 },
  forced_choices: { label: "Forced choices", better: "lower", target: 0 },
  trips_out_of_browser: { label: "Trips out of the browser", better: "lower", target: 1 },
  words_per_step_max: { label: "Visible words per step (worst step)", better: "lower", target: 40, layout: true },
  words_per_error_max: { label: "Visible words per error screen (worst)", better: "lower", target: 60, layout: true },
  banned_words: { label: "Banned words and em dashes on screen", better: "lower", target: 0 },
  yellow_red: { label: "Yellow or red elements", better: "lower", target: 0 },
  changes_visible_without_reload: { label: "Changes visible without reloading (of 3)", better: "higher", target: 3 },
  changes_visible_within_5s: { label: "Changes visible without reloading within 5 s (of 3)", better: "higher", target: 3, ratchet: false },
  primary_below_fold_px: { label: "Primary button below the fold at 1280x720 (px, worst)", better: "lower", target: 0, layout: true },
  final_window_matches_retention: { label: "Final-screen window equals the server's real retention", better: "true", target: true },
  copied_differs_from_shown: { label: "Copied text different from what is shown", better: "lower", target: 0 },
  connect_fields_kept_after_reload: { label: "Connect fields still filled after a reload (of 4)", better: "higher", target: 4 },
  renewal_sentence_with_gate_open: { label: "\"Kept up to date\" sentence shown while full reads are allowed", better: "lower", target: 0 },
  refused_update_named: { label: "A refused snapshot update is named on screen", better: "true", target: true },
};

// The five runs of #1800 and the columns each one reports.
export const RUNS = {
  clean: {
    title: "Clean MySQL 8.4, sign in to first snapshot",
    columns: ["clicks_to_first_snapshot", "clicks_to_first_snapshot_minimum", "fields_typed", "forced_choices",
      "trips_out_of_browser", "words_per_step_max", "words_per_error_max", "banned_words", "yellow_red",
      "changes_visible_without_reload", "changes_visible_within_5s", "primary_below_fold_px",
      "final_window_matches_retention", "copied_differs_from_shown"],
  },
  "reload-mid-connect": {
    title: "A reload in the middle of Connect",
    columns: ["connect_fields_kept_after_reload"],
  },
  "second-server": {
    title: "A second server, same install",
    columns: ["clicks_to_capture", "fields_typed", "forced_choices", "trips_out_of_browser", "words_per_step_max",
      "banned_words", "yellow_red", "primary_below_fold_px"],
  },
  "no-primary-key": {
    title: "A server with one table without a primary key",
    columns: ["clicks_to_capture", "fields_typed", "forced_choices", "trips_out_of_browser", "words_per_step_max",
      "words_per_error_max", "banned_words", "primary_below_fold_px"],
  },
  "refused-update": {
    title: "A refused snapshot update",
    columns: ["renewal_sentence_with_gate_open", "refused_update_named"],
  },
};

const nm = (reason) => ({ value: NOT_MEASURABLE, reason });
// A measurement that is gone because the thing it measured STOPPED HAPPENING,
// which is the product getting better, not the test losing its grip. The
// error screen for a folder that does not exist is the live example: a build
// that creates the folder has no such screen, and "was 71, now not
// measurable" would turn that improvement into a red build — pressure to
// weaken the rule that catches a measurement quietly dying.
const nmImproved = (reason) => ({ value: NOT_MEASURABLE, reason, improved: true });
const measured = (value, evidence) => (evidence === undefined ? { value } : { value, evidence });

// deriveRun turns one run's event log into its columns. Every column that the
// log cannot support says so with a reason: an absent measurement is never a
// zero, and never a pass.
export function deriveRun(name, log) {
  const def = RUNS[name];
  if (!def) throw new Error("unknown run " + JSON.stringify(name));
  log = log || [];
  const of = (kind) => log.filter((e) => e.kind === kind);
  const steps = of("step");
  const clicksBefore = (milestone, onlyRequired) => {
    const at = log.findIndex((e) => e.kind === "milestone" && e.name === milestone);
    if (at < 0) return null;
    return log.slice(0, at).filter((e) => e.kind === "click" && (!onlyRequired || !e.optional)).length;
  };
  const stepEvidence = steps.map((s) => ({
    step: s.name, kind: s.stepKind, words_above_fold: s.measure.wordsAboveFold, words_whole: s.measure.wordsWhole,
    layer: s.measure.layer,
  }));
  const maxWords = (kind) => {
    const pick = steps.filter((s) => s.stepKind === kind);
    if (!pick.length) {
      if (kind !== "error") return nm("no screen was measured");
      const gone = of("condition_absent").find((e) => e.what === "error");
      return gone ? nmImproved("no error screen in this run: " + gone.why) : nm("no error screen in this run");
    }
    const worst = pick.reduce((a, b) => (b.measure.wordsAboveFold > a.measure.wordsAboveFold ? b : a));
    return measured(worst.measure.wordsAboveFold, { worst: worst.name });
  };
  const changes = of("change");
  // Anchors the in-page measurement could not find. #view holding the page is
  // what every word and alarm count is read from; without it the numbers are
  // whatever `body` happened to contain, which is a smaller number on every
  // column that ratchets downward.
  const lostAnchors = steps.filter((st) => st.measure.anchors && st.measure.anchors.view === false).map((st) => st.name);
  const anchorLoss = lostAnchors.length
    ? nm("the page element the measurement reads was not found on: " + lostAnchors.join(", "))
    : null;

  const compute = {
    clicks_to_first_snapshot: () => {
      const n = clicksBefore("first_snapshot_done", false);
      return n === null ? nm("the walk did not reach a finished snapshot") : measured(n, {
        clicks: log.slice(0, log.findIndex((e) => e.kind === "milestone" && e.name === "first_snapshot_done"))
          .filter((e) => e.kind === "click").map((e) => e.label + (e.optional ? " (optional)" : "")),
      });
    },
    clicks_to_first_snapshot_minimum: () => {
      const n = clicksBefore("first_snapshot_done", true);
      return n === null ? nm("the walk did not reach a finished snapshot") : measured(n);
    },
    clicks_to_capture: () => {
      const n = clicksBefore("capture_running", false);
      return n === null ? nm("capture never started in this run") : measured(n, {
        clicks: log.slice(0, log.findIndex((e) => e.kind === "milestone" && e.name === "capture_running"))
          .filter((e) => e.kind === "click").map((e) => e.label),
      });
    },
    fields_typed: () => measured(of("type").length, { fields: of("type").map((e) => e.step + ": " + e.field) }),
    forced_choices: () => {
      // A choice the walk looks for by reading the screen reports what it
      // saw, every time. Finding nothing where there was something is the
      // walk losing its grip on that screen, not a choice that went away.
      const blind = of("forced_choice_probe").filter((e) => e.count === 0);
      if (blind.length) return nm("a forced choice could not be read: " + blind.map((e) => e.what).join(", "));
      return measured(of("forced_choice").length, { choices: of("forced_choice").map((e) => e.label) });
    },
    trips_out_of_browser: () => measured(of("out_of_browser").length, { trips: of("out_of_browser").map((e) => e.label) }),
    words_per_step_max: () => {
      if (anchorLoss) return anchorLoss;
      const r = maxWords("step");
      if (r.value !== NOT_MEASURABLE) r.evidence.steps = stepEvidence;
      return r;
    },
    words_per_error_max: () => anchorLoss || maxWords("error"),
    banned_words: () => {
      if (anchorLoss) return anchorLoss;
      if (!steps.length) return nm("no screen was measured");
      const b = bannedFromChunks(steps.flatMap((s) => s.measure.chunks || []));
      return measured(b.total, { per_word: b.perWord, sentences: b.examples });
    },
    yellow_red: () => {
      if (anchorLoss) return anchorLoss;
      if (!steps.length) return nm("no screen was measured");
      const list = steps.flatMap((s) => (s.measure.alarms || []).map((a) => s.name + ": " + a.selector + (a.text ? " \"" + a.text + "\"" : "")));
      return measured(list.length, { elements: list });
    },
    changes_visible_without_reload: () => {
      if (!changes.length) return nm("no change was made on the database in this run");
      return measured(changes.filter((c) => c.visible).length, {
        changes: changes.map((c) => c.op + ": " + (c.visible ? "seen after " + c.seconds + " s" : "not seen")),
      });
    },
    changes_visible_within_5s: () => {
      if (!changes.length) return nm("no change was made on the database in this run");
      return measured(changes.filter((c) => c.visible && c.seconds !== null && c.seconds <= 5).length);
    },
    primary_below_fold_px: () => {
      const withBtn = steps.filter((s) => s.measure.primary);
      if (!withBtn.length) return nm("no step named its primary button");
      const missing = withBtn.filter((s) => s.measure.primary.missing);
      if (missing.length) return nm("primary button not found on: " + missing.map((s) => s.name).join(", "));
      // Off the top or off to the side is not "0 px below the fold": the
      // number would read as a button in plain view.
      const off = withBtn.filter((s) => s.measure.primary.belowFoldPx === null);
      if (off.length) return nm("primary button outside the visible area, and not below it, on: " + off.map((s) => s.name).join(", "));
      const worst = withBtn.reduce((a, b) => (b.measure.primary.belowFoldPx > a.measure.primary.belowFoldPx ? b : a));
      return measured(worst.measure.primary.belowFoldPx, {
        worst: worst.name,
        steps: withBtn.map((s) => s.name + ": " + s.measure.primary.belowFoldPx + " px (" + s.measure.primary.label + ")"),
      });
    },
    final_window_matches_retention: () => {
      const r = of("retention_check").pop();
      if (!r) return nm("the walk did not reach the final screen");
      if (r.matches === null) return nm(r.reason);
      return measured(r.matches, { detail: r.reason });
    },
    copied_differs_from_shown: () => {
      const c = of("copy_check");
      if (!c.length) return nm("no block to copy was shown");
      // mismatch === null is "this comparison could not be made" — the block
      // no longer names a password, or the clipboard could not be read. It is
      // NOT "they match": the one check that guards "you copy one password
      // and the form holds another" would otherwise disappear as a pass.
      const unreadable = c.filter((e) => e.mismatch === null);
      if (unreadable.length) return nm("a copy could not be compared: " + unreadable.map((e) => e.label + " (" + e.detail + ")").join("; "));
      return measured(c.filter((e) => e.mismatch).length, { blocks: c.map((e) => e.label + ": " + (e.mismatch ? e.detail : "matches")) });
    },
    connect_fields_kept_after_reload: () => {
      const r = of("draft_check").pop();
      if (!r) return nm("the reload was not made");
      return measured(r.kept, { detail: r.detail });
    },
    renewal_sentence_with_gate_open: () => {
      const r = of("renewal_check").pop();
      if (!r) return nm("the final screen was not read");
      // A zero from a run that never put the product in the state this
      // column names is not a pass: it is a number standing in for something
      // nobody measured.
      if (r.exercised === false) return nm(r.detail);
      return measured(r.count, { detail: r.detail });
    },
    refused_update_named: () => {
      const r = of("refusal_check").pop();
      if (!r || r.named === null) return nm((r && r.reason) || "no refused update was produced");
      return measured(r.named, { detail: r.reason });
    },
  };
  const columns = {};
  for (const id of def.columns) columns[id] = compute[id]();
  // A run that threw measured nothing, whatever its log had got as far as
  // recording. Left alone, the counters it CAN still derive read as an
  // improvement — a walk that never opened a form typed no fields, so
  // "fields typed" comes out 0, which is the target. The run already fails
  // the whole thing, so this is about what the output SAYS: every column of
  // an unfinished run is not measurable, and names what broke.
  const failure = log.find((e) => e.kind === "error");
  if (failure) {
    for (const id of def.columns) columns[id] = nm("this run did not finish: " + failure.message);
  }
  return { title: def.title, columns };
}

// ── comparisons ────────────────────────────────────────────────────────────

const isNM = (v) => v === NOT_MEASURABLE;
const fmt = (v) => (typeof v === "string" ? v : JSON.stringify(v));

// worse(a, b): is b worse than a for this column?
function worse(col, a, b) {
  if (col.better === "lower") return b > a;
  if (col.better === "higher") return b < a;
  return a === true && b !== true;
}

function eachColumn(scoreboard, runsFilter, only, fn) {
  for (const [run, def] of Object.entries(RUNS)) {
    if (runsFilter && !runsFilter.includes(run)) continue;
    for (const id of def.columns) {
      if (only && !only.includes(id)) continue;
      fn(run, id, COLUMNS[id]);
    }
  }
}

// compareRatchet: CI fails when a number gets worse than the committed
// baseline. Better passes, with a note asking the PR that made it better to
// lower the baseline, so the ratchet keeps its grip. A column the baseline
// does not list fails: the file must say what every column was.
export function compareRatchet(scoreboard, baseline, opts = {}) {
  const failures = [], loosened = [], notMeasurable = [], passed = [], notCompared = [];
  // Where this scoreboard was measured against where the baseline was. An
  // absent platform on either side means an older file or a caller that does
  // not care, and then everything is compared as before.
  const here = opts.platform || scoreboard.platform || "";
  const there = baseline.platform || "";
  const otherPlatform = here && there && here !== there;
  const runs = opts.only ? Object.keys(baseline.runs || {}) : undefined;
  // What the baseline recorded as the REASON a column was not measurable.
  // baselineFrom writes it where the evidence goes, so comparing it costs the
  // file nothing and closes the hole where a column stays "not measurable"
  // for a completely different reason and nobody notices.
  const wasReason = (run, id) => {
    const e = (baseline.evidence || {})[run];
    return e && typeof e[id] === "string" ? e[id] : "";
  };
  eachColumn(scoreboard, runs, opts.only, (run, id, col) => {
    const bRun = (baseline.runs || {})[run];
    const cur = scoreboard.runs[run] && scoreboard.runs[run].columns[id];
    if (!bRun) { failures.push(`${run}: the run is missing from the baseline`); return; }
    if (!(id in bRun)) { failures.push(`${run}.${id}: missing from the baseline file`); return; }
    const was = bRun[id];
    if (!cur) { failures.push(`${run}.${id}: the walk did not produce run ${run} (baseline ${fmt(was)})`); return; }
    const now = cur.value;
    if (col.layout && otherPlatform) {
      notCompared.push(`${run}.${id}: ${fmt(now)} here, ${fmt(was)} recorded on ${there}; text wraps differently, so this is shown and not compared`);
      return;
    }
    if (col.ratchet === false) { passed.push(`${run}.${id}: ${fmt(now)} (shown, not ratcheted)`); return; }
    if (isNM(now) && isNM(was)) {
      const before = wasReason(run, id);
      if (before && before !== cur.reason) {
        failures.push(`${run}.${id}: still not measurable, but for a different reason: was "${before}", now "${cur.reason}"`);
        return;
      }
      notMeasurable.push(`${run}.${id}: not measurable (${cur.reason})`);
      return;
    }
    if (isNM(now)) {
      // A measurement gone because what it measured stopped happening is the
      // product improving; anything else is the test losing its grip.
      if (cur.improved) { loosened.push(`${run}.${id}: was ${fmt(was)}, and no longer happens (${cur.reason}); record it in the baseline`); return; }
      failures.push(`${run}.${id}: was ${fmt(was)}, now not measurable (${cur.reason})`);
      return;
    }
    if (isNM(was)) { loosened.push(`${run}.${id}: newly measurable at ${fmt(now)}; record it in the baseline`); return; }
    if (worse(col, was, now)) { failures.push(`${run}.${id}: worse than the baseline, ${fmt(was)} -> ${fmt(now)}`); return; }
    if (worse(col, now, was)) { loosened.push(`${run}.${id}: better than the baseline, ${fmt(was)} -> ${fmt(now)}; lower the baseline in this PR`); return; }
    passed.push(`${run}.${id}: ${fmt(now)}`);
  });
  // A run the baseline lists that the walk did not produce at all.
  for (const run of Object.keys(baseline.runs || {})) {
    if (!scoreboard.runs[run] && !failures.some((f) => f.startsWith(run))) failures.push(`${run}: the walk did not produce this run`);
  }
  return { failures, loosened, notMeasurable, passed, notCompared };
}

// compareTarget: the issue's targets. Not measurable is never a pass here.
export function compareTarget(scoreboard, opts = {}) {
  const failures = [], passed = [];
  // With `only` (the unit tests) a board may carry a single run; otherwise
  // every run is expected, and one the walk did not produce is a failure.
  const runs = opts.only ? Object.keys(scoreboard.runs) : undefined;
  eachColumn(scoreboard, runs, opts.only, (run, id, col) => {
    if (col.target === undefined) return;
    const cur = scoreboard.runs[run] && scoreboard.runs[run].columns[id];
    if (!cur) { failures.push(`${run}.${id}: the walk did not produce run ${run}`); return; }
    const now = cur.value;
    if (isNM(now)) { failures.push(`${run}.${id}: not measurable (${cur.reason}); target ${fmt(col.target)}`); return; }
    const ok = col.better === "lower" ? now <= col.target : col.better === "higher" ? now >= col.target : now === true;
    (ok ? passed : failures).push(`${run}.${id}: ${fmt(now)}, target ${col.better === "lower" ? "<= " : col.better === "higher" ? ">= " : ""}${fmt(col.target)}`);
  });
  return { failures, passed };
}

export function loadBaseline(file) {
  let text;
  try { text = readFileSync(file, "utf8"); } catch (err) { throw new Error("cannot read the baseline file " + file + ": " + err.message); }
  let b;
  try { b = JSON.parse(text); } catch (err) { throw new Error("the baseline file " + file + " is not JSON: " + err.message); }
  if (!b || b.schema !== 1 || typeof b.runs !== "object") throw new Error("the baseline file " + file + " has no schema 1 runs object");
  return b;
}

// baselineFrom writes a scoreboard's values in the baseline's shape, with a
// short evidence block that explains each number to a reviewer.
//
// The verbatim screen text is left OUT of the committed file: the list of
// sentences that carried a banned word runs to thousands of characters and
// rewrites itself on any copy edit, which would bury the numbers a reviewer
// came to read. It stays in the run's own first-run-scoreboard.json, whole.
// The columns that only mean something on the machine that measured them.
export function layoutColumns() {
  return Object.entries(COLUMNS).filter(([, c]) => c.layout).map(([id]) => id);
}

export function baselineFrom(scoreboard, meta) {
  const runs = {}, evidence = {};
  for (const [run, r] of Object.entries(scoreboard.runs)) {
    runs[run] = {};
    evidence[run] = {};
    for (const [id, c] of Object.entries(r.columns)) {
      runs[run][id] = c.value;
      if (c.evidence === undefined) {
        if (c.reason !== undefined) evidence[run][id] = c.reason;
        continue;
      }
      const e = Object.assign({}, c.evidence);
      if (e.sentences) { delete e.sentences; e.sentences_in = "the run's first-run-scoreboard.json"; }
      evidence[run][id] = e;
    }
  }
  return Object.assign({ schema: 1 }, meta, { runs, evidence });
}

// renderScoreboard prints one table per run: column, value, baseline, target.
export function renderScoreboard(scoreboard, baseline) {
  const lines = [];
  for (const [run, r] of Object.entries(scoreboard.runs)) {
    lines.push("", `== ${run}: ${r.title}`);
    const rows = [["column", "now", "baseline", "target"]];
    for (const [id, c] of Object.entries(r.columns)) {
      const col = COLUMNS[id];
      const b = baseline && baseline.runs && baseline.runs[run] ? baseline.runs[run][id] : undefined;
      const t = col.target === undefined ? "-" : (col.better === "lower" ? "<= " : col.better === "higher" ? ">= " : "") + fmt(col.target);
      rows.push([col.label, fmt(c.value) + (isNM(c.value) ? " (" + c.reason + ")" : ""), b === undefined ? "-" : fmt(b), t]);
    }
    const w = [0, 1, 2].map((i) => Math.min(70, Math.max(...rows.map((row) => row[i].length))));
    for (const row of rows) lines.push("  " + row.map((cell, i) => (i < 3 ? cell.padEnd(w[i]) : cell)).join("  "));
  }
  return lines.join("\n");
}
