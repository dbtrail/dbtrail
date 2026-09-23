// Unit tests for the first-run walk (#1800): the scoreboard arithmetic, the
// ratchet and target comparisons, the in-browser measurement rules, and the
// loud skip. Run with:
//
//   node --test test/console-e2e/first_run_walk.test.mjs
//
// The walk itself (first-run-walk.sh) needs Docker and MySQL; these do not.
// The DOM half launches the same headless Chromium the walk uses and loads
// first_run_measure.js into small fixture pages, so every measurement rule is
// checked against a page where the right answer is known. Each "not counted"
// case has a twin where the same text IS counted: a counter that counts
// nothing must fail here, not pass.
import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import {
  countWords, bannedHits, normalizeChunk, bannedFromChunks,
  deriveRun, compareRatchet, compareTarget, loadBaseline, baselineFrom, layoutColumns, RUNS, NOT_MEASURABLE,
  parseQuickstartBlock,
} from "./first_run_scoreboard.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));

// ── words ──────────────────────────────────────────────────────────────────

describe("countWords", () => {
  test("empty and blank text has no words", () => {
    assert.equal(countWords(""), 0);
    assert.equal(countWords("   \n\t  "), 0);
    assert.equal(countWords(undefined), 0);
  });
  test("marks and dashes alone are not words", () => {
    assert.equal(countWords("✓ — … ○ · → ›"), 0);
  });
  test("extra spaces and several lines do not change the count", () => {
    assert.equal(countWords("Capture   started,\n\n  with 2 warnings"), 5);
  });
  test("a token with a letter or digit is one word, whatever else it carries", () => {
    assert.equal(countWords("shop.orders #6"), 2);
    assert.equal(countWords("(optional) 3306 dbtrail@127.0.0.1:23306"), 3);
  });
});

// ── the quickstart permissions block ───────────────────────────────────────

describe("parseQuickstartBlock", () => {
  // The shape docs/quickstart.md publishes: a list item whose ```sql block is
  // indented, a password left as an unquoted placeholder, and commented lines
  // that must survive as comments.
  const page = (blockLines, heading = "## Prerequisites") => [
    "# Quickstart", "", "## Before you start", "", "  ```sql", "  SELECT 1;", "  ```", "",
    heading, "", "- create it on the source:", "", "  ```sql",
    ...blockLines.map((l) => "  " + l), "  ```", "", "  Put a password of your own in quotes.", "",
  ].join("\n");
  const REAL = [
    "CREATE USER 'dbtrail'@'%' IDENTIFIED BY <choose a password>;",
    "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';",
    "-- MariaDB and MySQL 5.7 have no BACKUP_ADMIN; run this instead:",
    "-- GRANT RELOAD, SHOW VIEW ON *.* TO 'dbtrail'@'%';",
  ];
  const throwsWith = (fn, re) => assert.throws(fn, (err) => re.test(err.message), "expected a message matching " + re);

  test("fills the placeholder and leaves every other line exactly as published", () => {
    const b = parseQuickstartBlock(page(REAL), "Frw-walk-9pw");
    assert.equal(b.user, "dbtrail");
    assert.equal(b.password, "Frw-walk-9pw");
    assert.equal(b.sql, [
      "CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'Frw-walk-9pw';",
      "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';",
      "-- MariaDB and MySQL 5.7 have no BACKUP_ADMIN; run this instead:",
      "-- GRANT RELOAD, SHOW VIEW ON *.* TO 'dbtrail'@'%';",
      "",
    ].join("\n"));
    // The commented alternatives stay commented: the walk runs the block, and
    // uncommenting one would grant a permission the page did not ask for.
    assert.equal((b.sql.match(/^-- GRANT/gm) || []).length, 1);
  });

  test("the block under ## Prerequisites is the one read, not an earlier one", () => {
    const b = parseQuickstartBlock(page(REAL), "Frw-walk-9pw");
    assert.match(b.sql, /CREATE USER/);
    assert.doesNotMatch(b.sql, /SELECT 1;/);
  });

  test("a block with no indent is read the same way", () => {
    const flat = ["# Quickstart", "", "## Prerequisites", "", "```sql", ...REAL, "```", ""].join("\n");
    assert.equal(parseQuickstartBlock(flat, "Frw-walk-9pw").sql.split("\n")[0], "CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'Frw-walk-9pw';");
  });

  // The guard this function exists for. An evaluator pasted a published block
  // and created a MySQL account whose password was in our documentation.
  test("a password a reader could paste and run is refused, not measured", () => {
    const leaked = ["CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'strong-password';", ...REAL.slice(1)];
    throwsWith(() => parseQuickstartBlock(page(leaked), "Frw-walk-9pw"), /paste and run/);
  });
  test("a quoted password inside a comment is refused too", () => {
    const leaked = [...REAL, "-- On MariaDB: CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'strong-password';"];
    throwsWith(() => parseQuickstartBlock(page(leaked), "Frw-walk-9pw"), /paste and run/);
  });

  test("any wording inside the angle brackets is a placeholder", () => {
    const other = ["CREATE USER 'dbtrail'@'%' IDENTIFIED BY <your own password>;", ...REAL.slice(1)];
    assert.match(parseQuickstartBlock(page(other), "Frw-walk-9pw").sql, /IDENTIFIED BY 'Frw-walk-9pw';/);
  });
  test("every placeholder is filled, not only the first", () => {
    const two = [...REAL, "-- CREATE USER 'dbtrail'@'localhost' IDENTIFIED BY <choose a password>;"];
    const sql = parseQuickstartBlock(page(two), "Frw-walk-9pw").sql;
    assert.equal((sql.match(/IDENTIFIED BY 'Frw-walk-9pw'/g) || []).length, 2);
    assert.doesNotMatch(sql, /IDENTIFIED BY </);
  });
  test("a placeholder that names nothing is refused", () => {
    const empty = ["CREATE USER 'dbtrail'@'%' IDENTIFIED BY <>;", ...REAL.slice(1)];
    throwsWith(() => parseQuickstartBlock(page(empty), "Frw-walk-9pw"), /names nothing/);
    const blank = ["CREATE USER 'dbtrail'@'%' IDENTIFIED BY <   >;", ...REAL.slice(1)];
    throwsWith(() => parseQuickstartBlock(page(blank), "Frw-walk-9pw"), /names nothing/);
  });

  test("a dollar sign in the password reaches MySQL unchanged", () => {
    // A replacement STRING would read $& and $1 as references and rewrite it.
    assert.match(parseQuickstartBlock(page(REAL), "Frw$&walk$1").sql, /IDENTIFIED BY 'Frw\$&walk\$1';/);
  });
  test("a password the walk cannot put in a SQL literal is refused", () => {
    throwsWith(() => parseQuickstartBlock(page(REAL), "it's"), /quote or a backslash/);
    throwsWith(() => parseQuickstartBlock(page(REAL), "back\\slash"), /quote or a backslash/);
    throwsWith(() => parseQuickstartBlock(page(REAL), ""), /must choose a password/);
  });

  test("a page missing the heading, the block or the statements says which", () => {
    throwsWith(() => parseQuickstartBlock("# Quickstart\n\nnothing here\n", "Frw-walk-9pw"), /no ## Prerequisites/);
    throwsWith(() => parseQuickstartBlock("## Prerequisites\n\njust prose\n", "Frw-walk-9pw"), /no ```sql block/);
    throwsWith(() => parseQuickstartBlock("## Prerequisites\n\n```sql\nCREATE USER 'x'@'%' IDENTIFIED BY <pw>;\n", "Frw-walk-9pw"), /never closed/);
    throwsWith(() => parseQuickstartBlock("## Prerequisites\n\n```sql\n\n```\n", "Frw-walk-9pw"), /is empty/);
    throwsWith(() => parseQuickstartBlock(page(["GRANT SELECT ON *.* TO 'dbtrail'@'%';"]), "Frw-walk-9pw"), /no CREATE USER/);
    throwsWith(() => parseQuickstartBlock(page(["CREATE USER 'dbtrail'@'%';"]), "Frw-walk-9pw"), /no IDENTIFIED BY <placeholder>/);
    throwsWith(() => parseQuickstartBlock("", "Frw-walk-9pw"), /no ## Prerequisites/);
  });

  // The page the walk actually reads, so a copy edit that breaks the walk
  // fails here, in a second, instead of after Docker and a browser in CI.
  test("the real docs/quickstart.md parses and publishes no runnable password", () => {
    const md = readFileSync(path.join(HERE, "..", "..", "docs", "quickstart.md"), "utf8");
    const b = parseQuickstartBlock(md, "Frw-walk-9pw");
    assert.equal(b.user, "dbtrail");
    assert.match(b.sql, /^CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'Frw-walk-9pw';$/m);
    assert.match(b.sql, /GRANT REPLICATION SLAVE/);
    assert.doesNotMatch(b.published, /IDENTIFIED BY '/i);
  });
});

// ── banned words ───────────────────────────────────────────────────────────

describe("bannedHits", () => {
  test("every banned word is caught in any case and its usual forms", () => {
    const text = "BACKUP Backups indexed Monitoring streaming sources consoles daemon's watching tracked protection preflight baselines";
    const words = bannedHits(text).map((h) => h.word);
    assert.deepEqual(words, ["backup", "backup", "index", "monitor", "stream", "source", "console", "daemon", "watch", "track", "protect", "preflight", "baseline"]);
  });
  test("an em dash is a hit; an en dash and a hyphen are not", () => {
    assert.deepEqual(bannedHits("a — b – c - d").map((h) => h.word), ["em dash"]);
  });
  test("a word that only shares a prefix with a banned word is not a hit", () => {
    assert.deepEqual(bannedHits("trackpad streamline consolidate watchdog indexOf reindex sourcery"), []);
  });
  test("an identifier with an underscore is not a sentence word", () => {
    assert.deepEqual(bannedHits("bintrail_idx_42 index_dsn"), []);
  });
  test("two hits in one sentence count twice", () => {
    assert.equal(bannedHits("the backup of the backup").length, 2);
  });
  test("empty text has no hits", () => {
    assert.deepEqual(bannedHits(""), []);
  });
});

describe("banned words across a walk", () => {
  test("the same sentence on two screens counts once", () => {
    const r = bannedFromChunks(["Set a Backup dir", "Set a Backup dir"]);
    assert.equal(r.total, 1);
  });
  test("sentences that differ only in numbers or spacing are the same sentence", () => {
    assert.equal(normalizeChunk("  capture lag 146s\n"), normalizeChunk("capture lag 10s"));
    const r = bannedFromChunks(["stream position binlog.000002", "stream   position binlog.000003"]);
    assert.equal(r.total, 1);
  });
  test("different sentences with the same banned word count separately, per word", () => {
    const r = bannedFromChunks(["Backups", "Backup settings", "Monitor a source database"]);
    assert.equal(r.total, 4);
    assert.deepEqual(r.perWord, { backup: 2, monitor: 1, source: 1 });
  });
  test("no chunks, no hits", () => {
    assert.deepEqual(bannedFromChunks([]), { total: 0, perWord: {}, examples: [] });
  });
});

// ── deriving a run's columns from its event log ───────────────────────────

const stepEv = (name, stepKind, m) => ({ kind: "step", name, stepKind, measure: Object.assign({
  wordsAboveFold: 0, wordsWhole: 0, chunks: [], alarms: [], primary: null }, m) });

describe("deriveRun (clean)", () => {
  const log = [
    { kind: "out_of_browser", label: "run the permissions block" },
    stepEv("sign-in", "step", { wordsAboveFold: 17, chunks: ["DBTrail"], primary: { belowFoldPx: 0 } }),
    { kind: "type", field: "password", step: "sign-in" },
    { kind: "type", field: "confirm", step: "sign-in" },
    { kind: "click", label: "Create & sign in", optional: false },
    { kind: "click", label: "Test connection", optional: true },
    stepEv("connect", "step", { wordsAboveFold: 120, chunks: ["Monitor a source database"], alarms: [{ selector: ".notice.warn" }], primary: { belowFoldPx: 612 } }),
    { kind: "copy_check", label: "permissions block", mismatch: true },
    { kind: "forced_choice", label: "Backup dir or Backup S3" },
    stepEv("folder-missing", "error", { wordsAboveFold: 71, alarms: [{ selector: ".error-box" }, { selector: ".warn-item" }] }),
    { kind: "change", op: "INSERT", visible: true, seconds: 7.5 },
    { kind: "change", op: "UPDATE", visible: false, seconds: null },
    { kind: "change", op: "DELETE", visible: false, seconds: null },
    { kind: "click", label: "Create backup", optional: false },
    { kind: "milestone", name: "first_snapshot_done" },
    { kind: "click", label: "after the snapshot", optional: false },
    { kind: "retention_check", matches: null, reason: "no retention sentence on the final screen" },
  ];
  const r = deriveRun("clean", log);
  const v = (id) => r.columns[id].value;
  test("clicks count up to the finished snapshot, walked and minimum", () => {
    assert.equal(v("clicks_to_first_snapshot"), 3);
    assert.equal(v("clicks_to_first_snapshot_minimum"), 2);
  });
  test("fields, forced choices and trips come straight from the log", () => {
    assert.equal(v("fields_typed"), 2);
    assert.equal(v("forced_choices"), 1);
    assert.equal(v("trips_out_of_browser"), 1);
  });
  test("words are the worst step, with error screens held apart", () => {
    assert.equal(v("words_per_step_max"), 120);
    assert.equal(v("words_per_error_max"), 71);
  });
  test("warning and error elements add up over every screen", () => {
    assert.equal(v("yellow_red"), 3);
  });
  test("banned words come from the screens' sentences", () => {
    assert.equal(v("banned_words"), 2);
  });
  test("changes: seen without a reload inside the wait, and inside 5 seconds", () => {
    assert.equal(v("changes_visible_without_reload"), 1);
    assert.equal(v("changes_visible_within_5s"), 0);
  });
  test("the primary button's distance below the fold is the worst step", () => {
    assert.equal(v("primary_below_fold_px"), 612);
  });
  test("copies that differ from what is shown are counted", () => {
    assert.equal(v("copied_differs_from_shown"), 1);
  });
  test("a check whose feature is missing is not measurable, with its reason", () => {
    assert.equal(r.columns.final_window_matches_retention.value, NOT_MEASURABLE);
    assert.match(r.columns.final_window_matches_retention.reason, /no retention sentence/);
  });
});

describe("deriveRun edge cases", () => {
  test("a walk that never finished a snapshot cannot report clicks to one", () => {
    const r = deriveRun("clean", [{ kind: "click", label: "x", optional: false }]);
    assert.equal(r.columns.clicks_to_first_snapshot.value, NOT_MEASURABLE);
    assert.match(r.columns.clicks_to_first_snapshot.reason, /snapshot/);
  });
  test("a run with no measured screen cannot report words", () => {
    const r = deriveRun("clean", []);
    assert.equal(r.columns.words_per_step_max.value, NOT_MEASURABLE);
  });
  test("a run with no error screen reports no error words rather than zero", () => {
    const r = deriveRun("clean", [stepEv("a", "step", { wordsAboveFold: 5 })]);
    assert.equal(r.columns.words_per_error_max.value, NOT_MEASURABLE);
  });
  test("a screen whose primary button was not found is not a zero", () => {
    const r = deriveRun("clean", [stepEv("a", "step", { primary: { belowFoldPx: null, missing: true } })]);
    assert.equal(r.columns.primary_below_fold_px.value, NOT_MEASURABLE);
  });
  test("changes never tried are not measurable, never zero", () => {
    const r = deriveRun("clean", []);
    assert.equal(r.columns.changes_visible_without_reload.value, NOT_MEASURABLE);
  });
  test("every run named in RUNS derives every column it declares", () => {
    for (const [name, def] of Object.entries(RUNS)) {
      const r = deriveRun(name, []);
      assert.deepEqual(Object.keys(r.columns).sort(), [...def.columns].sort(), name);
    }
  });
  test("a run that threw measures nothing, so a counter cannot read as an improvement", () => {
    const r = deriveRun("clean", [
      { kind: "click", label: "+ Add server", optional: false },
      stepEv("connect", "step", { wordsAboveFold: 126 }),
      { kind: "error", message: "the Save button was never found" },
    ]);
    for (const id of RUNS.clean.columns) {
      assert.equal(r.columns[id].value, NOT_MEASURABLE, id);
      assert.match(r.columns[id].reason, /did not finish: the Save button was never found/);
    }
    // And the ratchet calls every one of them worse, never better.
    const base = { schema: 1, runs: { clean: Object.fromEntries(RUNS.clean.columns.map((c) => [c, 1])) } };
    const cmp = compareRatchet({ runs: { clean: r } }, base);
    assert.equal(cmp.loosened.length, 0);
    assert.ok(cmp.failures.length >= RUNS.clean.columns.length - 1, JSON.stringify(cmp));
  });
  test("an unknown run name is refused, not silently empty", () => {
    assert.throws(() => deriveRun("no-such-run", []), /unknown run/);
  });
});

// ── comparing with the committed baseline (ratchet) and with the targets ──

const board = (cols) => ({ runs: { clean: { columns: Object.fromEntries(
  Object.entries(cols).map(([k, v]) => [k, typeof v === "object" && v !== null ? v : { value: v }])) } } });
const base = (cols) => ({ schema: 1, runs: { clean: cols } });

describe("compareRatchet", () => {
  test("equal is a pass", () => {
    const r = compareRatchet(board({ fields_typed: 8 }), base({ fields_typed: 8 }), { only: ["fields_typed"] });
    assert.deepEqual(r.failures, []);
  });
  test("worse on a lower-is-better column fails and names run, column and both numbers", () => {
    const r = compareRatchet(board({ fields_typed: 9 }), base({ fields_typed: 8 }), { only: ["fields_typed"] });
    assert.equal(r.failures.length, 1);
    assert.match(r.failures[0], /clean.*fields_typed.*8.*9/);
  });
  test("better passes, and says the baseline can come down", () => {
    const r = compareRatchet(board({ fields_typed: 4 }), base({ fields_typed: 8 }), { only: ["fields_typed"] });
    assert.deepEqual(r.failures, []);
    assert.equal(r.loosened.length, 1);
    assert.match(r.loosened[0], /fields_typed.*8.*4/);
  });
  test("worse on a higher-is-better column fails", () => {
    const r = compareRatchet(board({ changes_visible_without_reload: 0 }), base({ changes_visible_without_reload: 1 }), { only: ["changes_visible_without_reload"] });
    assert.equal(r.failures.length, 1);
  });
  test("a boolean that turns false fails", () => {
    const r = compareRatchet(board({ final_window_matches_retention: false }), base({ final_window_matches_retention: true }), { only: ["final_window_matches_retention"] });
    assert.equal(r.failures.length, 1);
  });
  test("a measurement that disappears fails", () => {
    const r = compareRatchet(board({ fields_typed: { value: NOT_MEASURABLE, reason: "x" } }), base({ fields_typed: 8 }), { only: ["fields_typed"] });
    assert.equal(r.failures.length, 1);
    assert.match(r.failures[0], /not measurable/);
  });
  test("a newly measurable column passes and asks for the baseline to record it", () => {
    const r = compareRatchet(board({ final_window_matches_retention: true }), base({ final_window_matches_retention: NOT_MEASURABLE }), { only: ["final_window_matches_retention"] });
    assert.deepEqual(r.failures, []);
    assert.equal(r.loosened.length, 1);
  });
  test("not measurable on both sides passes and stays listed", () => {
    const r = compareRatchet(board({ final_window_matches_retention: { value: NOT_MEASURABLE, reason: "x" } }), base({ final_window_matches_retention: NOT_MEASURABLE }), { only: ["final_window_matches_retention"] });
    assert.deepEqual(r.failures, []);
    assert.equal(r.notMeasurable.length, 1);
  });
  test("a column missing from the baseline fails: the file must list every column", () => {
    const r = compareRatchet(board({ fields_typed: 8 }), base({}), { only: ["fields_typed"] });
    assert.equal(r.failures.length, 1);
    assert.match(r.failures[0], /missing from the baseline/);
  });
  test("a run in the baseline that the walk did not produce fails", () => {
    const r = compareRatchet({ runs: {} }, base({ fields_typed: 8 }), { only: ["fields_typed"] });
    assert.equal(r.failures.length, 1);
    assert.match(r.failures[0], /clean/);
  });
  test("a timing column is shown but never ratcheted", () => {
    const r = compareRatchet(board({ changes_visible_within_5s: 0 }), base({ changes_visible_within_5s: 1 }), { only: ["changes_visible_within_5s"] });
    assert.deepEqual(r.failures, []);
  });
});

describe("compareRatchet, not-measurable reasons", () => {
  const nmBoard = (reason, improved) => ({ runs: { clean: { columns: { fields_typed: { value: NOT_MEASURABLE, reason, improved } } } } });
  const nmBase = (reason) => ({ schema: 1, runs: { clean: { fields_typed: NOT_MEASURABLE } }, evidence: { clean: { fields_typed: reason } } });
  test("the same reason on both sides passes", () => {
    const r = compareRatchet(nmBoard("no block to copy was shown"), nmBase("no block to copy was shown"), { only: ["fields_typed"] });
    assert.deepEqual(r.failures, []);
    assert.equal(r.notMeasurable.length, 1);
  });
  test("a DIFFERENT reason fails, naming both", () => {
    const r = compareRatchet(nmBoard("the page element was not found"), nmBase("no block to copy was shown"), { only: ["fields_typed"] });
    assert.equal(r.failures.length, 1);
    assert.match(r.failures[0], /different reason.*no block to copy.*page element/s);
  });
  test("a baseline with no recorded reason still passes, so an older file is not a failure", () => {
    const base = { schema: 1, runs: { clean: { fields_typed: NOT_MEASURABLE } } };
    assert.deepEqual(compareRatchet(nmBoard("anything"), base, { only: ["fields_typed"] }).failures, []);
  });
  test("a measurement that stopped happening because the product improved passes; any other loss fails", () => {
    const base = { schema: 1, runs: { clean: { fields_typed: 71 } } };
    const good = compareRatchet(nmBoard("no error screen: the folder is created for you", true), base, { only: ["fields_typed"] });
    assert.deepEqual(good.failures, []);
    assert.match(good.loosened[0], /no longer happens/);
    const bad = compareRatchet(nmBoard("no screen was measured"), base, { only: ["fields_typed"] });
    assert.equal(bad.failures.length, 1);
  });
});

describe("deriveRun, a measurement that lost its grip", () => {
  test("a step whose page element was not found makes every screen column not measurable", () => {
    const r = deriveRun("clean", [stepEv("a", "step", { wordsAboveFold: 3, anchors: { view: false } })]);
    for (const id of ["words_per_step_max", "words_per_error_max", "banned_words", "yellow_red"]) {
      assert.equal(r.columns[id].value, NOT_MEASURABLE, id);
      assert.match(r.columns[id].reason, /page element/);
    }
  });
  test("a forced choice the walk looked for and could not read is not zero choices", () => {
    const r = deriveRun("no-primary-key", [{ kind: "forced_choice_probe", what: "which ALTER TABLE form to run", count: 0 }]);
    assert.equal(r.columns.forced_choices.value, NOT_MEASURABLE);
    assert.match(r.columns.forced_choices.reason, /could not be read/);
  });
  test("a probe that DID find its choices still counts them", () => {
    const r = deriveRun("no-primary-key", [{ kind: "forced_choice_probe", what: "x", count: 2 }, { kind: "forced_choice", label: "which of 2" }]);
    assert.equal(r.columns.forced_choices.value, 1);
  });
  test("a copy that could not be compared is not a match", () => {
    const r = deriveRun("clean", [{ kind: "copy_check", label: "the block", mismatch: null, detail: "it no longer names a password" }]);
    assert.equal(r.columns.copied_differs_from_shown.value, NOT_MEASURABLE);
    assert.match(r.columns.copied_differs_from_shown.reason, /no longer names a password/);
  });
  test("an error screen that no longer happens is an improvement, not a lost measurement", () => {
    const r = deriveRun("clean", [stepEv("a", "step", {}), { kind: "condition_absent", what: "error", why: "the folder was there" }]);
    assert.equal(r.columns.words_per_error_max.value, NOT_MEASURABLE);
    assert.equal(r.columns.words_per_error_max.improved, true);
  });
  test("a primary button off the top of the page is not 0 px below the fold", () => {
    const r = deriveRun("clean", [stepEv("a", "step", { primary: { belowFoldPx: null, missing: false, above: true, label: "Save" } })]);
    assert.equal(r.columns.primary_below_fold_px.value, NOT_MEASURABLE);
    assert.match(r.columns.primary_below_fold_px.reason, /outside the visible area/);
  });
});

describe("compareRatchet across platforms", () => {
  const board = (v) => ({ runs: { clean: { columns: { primary_below_fold_px: { value: v }, fields_typed: { value: 8 } } } } });
  const base = (v, platform) => ({ schema: 1, platform, runs: { clean: { primary_below_fold_px: v, fields_typed: 8 } } });
  test("a number that moves with the font is shown, not compared, on another platform", () => {
    const r = compareRatchet(board(900), base(638, "darwin/x"), { platform: "linux/x", only: ["primary_below_fold_px", "fields_typed"] });
    assert.deepEqual(r.failures, []);
    assert.equal(r.notCompared.length, 1);
    assert.match(r.notCompared[0], /900 here, 638 recorded on darwin/);
  });
  test("on the SAME platform it is compared like any other column", () => {
    const r = compareRatchet(board(900), base(638, "linux/x"), { platform: "linux/x", only: ["primary_below_fold_px"] });
    assert.equal(r.failures.length, 1);
  });
  test("the columns that count things are compared on any platform", () => {
    const r = compareRatchet({ runs: { clean: { columns: { fields_typed: { value: 9 } } } } }, base(638, "darwin/x"),
      { platform: "linux/x", only: ["fields_typed"] });
    assert.equal(r.failures.length, 1);
    assert.match(r.failures[0], /fields_typed/);
  });
  test("a baseline with no platform recorded is compared as before", () => {
    const r = compareRatchet(board(900), base(638, undefined), { platform: "linux/x", only: ["primary_below_fold_px"] });
    assert.equal(r.failures.length, 1);
    assert.equal(r.notCompared.length, 0);
  });
  test("the layout-dependent set is exactly the three that the fold decides", () => {
    assert.deepEqual(layoutColumns().sort(), ["primary_below_fold_px", "words_per_error_max", "words_per_step_max"]);
  });
});

describe("deriveRun, a rule nobody put to the test", () => {
  test("a sentence checked without setting the condition it depends on is not a green zero", () => {
    const r = deriveRun("refused-update", [{ kind: "renewal_check", count: 0, exercised: false, detail: "nothing closed the door" }]);
    assert.equal(r.columns.renewal_sentence_with_gate_open.value, NOT_MEASURABLE);
    assert.match(r.columns.renewal_sentence_with_gate_open.reason, /nothing closed the door/);
  });
  test("once the condition IS set, the count is reported", () => {
    const r = deriveRun("refused-update", [{ kind: "renewal_check", count: 2, exercised: true, detail: "the door was closed" }]);
    assert.equal(r.columns.renewal_sentence_with_gate_open.value, 2);
  });
});

describe("compareTarget", () => {
  test("not measurable is never a pass", () => {
    const r = compareTarget(board({ final_window_matches_retention: { value: NOT_MEASURABLE, reason: "x" } }), { only: ["final_window_matches_retention"] });
    assert.equal(r.failures.length, 1);
    assert.match(r.failures[0], /not measurable/);
  });
  test("over the target fails, at the target passes", () => {
    assert.equal(compareTarget(board({ fields_typed: 5 }), { only: ["fields_typed"] }).failures.length, 1);
    assert.equal(compareTarget(board({ fields_typed: 4 }), { only: ["fields_typed"] }).failures.length, 0);
  });
  test("a higher-is-better column under the target fails", () => {
    assert.equal(compareTarget(board({ changes_visible_within_5s: 2 }), { only: ["changes_visible_within_5s"] }).failures.length, 1);
    assert.equal(compareTarget(board({ changes_visible_within_5s: 3 }), { only: ["changes_visible_within_5s"] }).failures.length, 0);
  });
  test("a false boolean fails", () => {
    assert.equal(compareTarget(board({ final_window_matches_retention: false }), { only: ["final_window_matches_retention"] }).failures.length, 1);
  });
});

describe("baselineFrom", () => {
  test("values and evidence are carried, and the verbatim sentences are not", () => {
    const sb = { runs: { clean: { columns: {
      banned_words: { value: 3, evidence: { per_word: { backup: 3 }, sentences: ["Set a Backup dir", "Backups"] } },
      fields_typed: { value: 8, evidence: { fields: ["connect: host"] } },
      final_window_matches_retention: { value: NOT_MEASURABLE, reason: "no sentence on screen" },
    } } } };
    const b = baselineFrom(sb, { measured_at_commit: "abc1234" });
    assert.equal(b.schema, 1);
    assert.equal(b.measured_at_commit, "abc1234");
    assert.deepEqual(b.runs.clean, { banned_words: 3, fields_typed: 8, final_window_matches_retention: NOT_MEASURABLE });
    assert.deepEqual(b.evidence.clean.banned_words.per_word, { backup: 3 });
    assert.equal(b.evidence.clean.banned_words.sentences, undefined);
    assert.match(b.evidence.clean.banned_words.sentences_in, /first-run-scoreboard/);
    assert.deepEqual(b.evidence.clean.fields_typed.fields, ["connect: host"]);
    assert.equal(b.evidence.clean.final_window_matches_retention, "no sentence on screen");
  });
  test("what it writes is what loadBaseline accepts", () => {
    const sb = { runs: Object.fromEntries(Object.entries(RUNS).map(([n, d]) => [n, { columns: Object.fromEntries(d.columns.map((c) => [c, { value: 1 }])) }])) };
    const file = path.join(process.env.TMPDIR || "/tmp", "frw-baseline-roundtrip-" + process.pid + ".json");
    writeFileSync(file, JSON.stringify(baselineFrom(sb, {}), null, 2));
    try {
      const back = loadBaseline(file);
      for (const [name, def] of Object.entries(RUNS)) for (const col of def.columns) assert.equal(back.runs[name][col], 1);
    } finally { rmSync(file, { force: true }); }
  });
});

describe("loadBaseline", () => {
  test("a missing file is an error, never a skip", () => {
    assert.throws(() => loadBaseline("/nonexistent/first_run_baseline.json"), /baseline/);
  });
  test("a file that is not JSON is an error", () => {
    assert.throws(() => loadBaseline(fileURLToPath(import.meta.url)), /baseline/);
  });
  test("the committed baseline loads and lists every column of every run", () => {
    const b = loadBaseline(path.join(HERE, "first_run_baseline.json"));
    for (const [name, def] of Object.entries(RUNS)) {
      assert.ok(b.runs[name], "baseline lacks run " + name);
      for (const col of def.columns) assert.ok(col in b.runs[name], name + " lacks " + col);
    }
  });
});

// ── the in-browser measurement, on pages where the answer is known ─────────

let chromium;
try { ({ chromium } = await import("playwright")); } catch (_) { chromium = null; }

describe("first_run_measure.js in a real browser", () => {
  let browser, page;
  const MEASURE = readFileSync(path.join(HERE, "first_run_measure.js"), "utf8");
  before(async () => {
    // No quiet skip: the walk needs this browser too, so its absence is a
    // failure of the environment the walk runs in.
    assert.ok(chromium, "playwright is not installed: run npm install in test/console-e2e");
    browser = await chromium.launch({ headless: true, channel: process.env.PW_CHANNEL || undefined });
    page = await browser.newPage({ viewport: { width: 1280, height: 720 } });
  });
  after(async () => { if (browser) await browser.close(); });

  // A page shaped like the console: sidebar, main view, and the three layers
  // that sit above it. Only `inner` changes per case.
  const shell = (view, layers = {}) => `<!doctype html><html><head><style>
      body { margin: 0; font: 16px/20px sans-serif; }
      .side { position: fixed; left: 0; top: 0; width: 200px; }
      main { margin-left: 220px; }
      .scroll { height: 100px; overflow-y: auto; }
      .tall { height: 2000px; }
    </style></head><body>
    <aside class="side">${layers.side || ""}</aside>
    <main><div id="view">${view}</div></main>
    <div id="modal">${layers.modal || ""}</div>
    <div id="login-mount">${layers.login || ""}</div>
    <div id="notice-mount">${layers.notice || ""}</div>
    <div id="toast-error" class="toast toast-error" hidden></div>
    </body></html>`;
  const measure = async (html, opts = {}) => {
    await page.setContent(html);
    await page.addScriptTag({ content: MEASURE });
    return page.evaluate((o) => window.__firstRunMeasure(o), opts);
  };

  test("visible words are counted (the positive control)", async () => {
    const m = await measure(shell("<p>one two three</p>"));
    assert.equal(m.wordsAboveFold, 3);
    assert.equal(m.layer, "view");
  });
  test("display:none, visibility:hidden and opacity:0 hide their words", async () => {
    const m = await measure(shell(`<p>seen</p><p style="display:none">a b</p>
      <p style="visibility:hidden">c d</p><div style="opacity:0"><p>e f</p></div>`));
    assert.equal(m.wordsAboveFold, 1);
  });
  test("a folded Details shows only its summary; unfolded it shows all", async () => {
    const closed = await measure(shell("<details><summary>More here</summary>hidden words inside</details>"));
    assert.equal(closed.wordsAboveFold, 2);
    const open = await measure(shell("<details open><summary>More here</summary>hidden words inside</details>"));
    assert.equal(open.wordsAboveFold, 5);
  });
  test("a folded Details AFTER the primary button is skipped whole, summary and banned words included", async () => {
    const html = shell(`<p>Read it now</p><button class="go">Go</button>
      <details><summary>Details</summary>the daemon takes a backup</details>`);
    const m = await measure(html, { primary: ".go" });
    assert.equal(m.wordsAboveFold, 4);
    assert.deepEqual(m.chunks.filter((c) => /Details|daemon/.test(c)), []);
  });
  test("a folded Details BEFORE the primary button keeps its summary counted", async () => {
    const m = await measure(shell(`<details><summary>Details</summary>x y</details>
      <p>Read it now</p><button class="go">Go</button>`), { primary: ".go" });
    assert.equal(m.wordsAboveFold, 5);
  });
  test("an unfolded Details after the button is ordinary text", async () => {
    const m = await measure(shell(`<button class="go">Go</button>
      <details open><summary>Details</summary>the daemon</details>`), { primary: ".go" });
    assert.equal(m.wordsAboveFold, 4);
    assert.ok(m.chunks.some((c) => /daemon/.test(c)));
  });
  test("words below the fold are not counted above it, but are in the whole count", async () => {
    const m = await measure(shell(`<p>above the fold</p><div style="height:800px"></div><p>below it here</p>`));
    assert.equal(m.wordsAboveFold, 3);
    assert.equal(m.wordsWhole, 6);
  });
  test("a paragraph cut by the fold counts only its lines above it", async () => {
    // 20px lines: the fold at 720px leaves exactly the first line of the
    // last paragraph above it.
    const m = await measure(shell(`<div style="height:700px"></div>
      <p style="margin:0;width:120px">aaa bbb ccc ddd eee fff</p>`));
    assert.ok(m.wordsAboveFold >= 1 && m.wordsAboveFold < 6, "got " + m.wordsAboveFold);
  });
  test("words scrolled out of sight inside a box are not above the fold", async () => {
    const m = await measure(shell(`<div class="scroll"><p style="margin:0">top line</p><div style="height:300px"></div><p>hidden bottom</p></div>`));
    assert.equal(m.wordsAboveFold, 2);
  });
  test("copyable SQL in a pre is not counted as words; inline code is", async () => {
    const m = await measure(shell(`<p>Run <code>GRANT SELECT</code> now</p><pre>CREATE USER 'x'@'%';</pre>`));
    assert.equal(m.wordsAboveFold, 4);
  });
  test("banned words inside code or pre are exempt; in a sentence they count", async () => {
    const m = await measure(shell(`<p>Your backup is ready</p><code>--baseline-dir</code><pre>bintrail baseline</pre>`));
    assert.deepEqual(m.chunks.filter((c) => /baseline/.test(c)), []);
    assert.ok(m.chunks.some((c) => /backup/.test(c)));
  });
  test("an empty field's placeholder is read; a filled field's is not", async () => {
    const m = await measure(shell(`<input placeholder="the daemon credentials"><input value="x" placeholder="not read at all">`));
    assert.equal(m.wordsAboveFold, 3);
    assert.ok(m.chunks.some((c) => /daemon/.test(c)));
  });
  test("a select shows its chosen option", async () => {
    const m = await measure(shell(`<select><option>Path style default</option><option>other words here</option></select>`));
    assert.equal(m.wordsAboveFold, 3);
  });
  test("the reading layer is the top one: gate, then notice, then dialog, then page", async () => {
    const all = { login: "<p>gate words</p>", notice: "<p>notice</p>", modal: "<p>dialog text here</p>", side: "<p>Backups</p>" };
    assert.equal((await measure(shell("<p>page</p>", all))).layer, "login");
    assert.equal((await measure(shell("<p>page</p>", { notice: all.notice, modal: all.modal }))).layer, "notice");
    const dlg = await measure(shell("<p>page</p>", { modal: all.modal, side: all.side }));
    assert.equal(dlg.layer, "modal");
    assert.equal(dlg.wordsAboveFold, 3);
    const pg = await measure(shell("<p>page words</p>", { side: all.side }));
    assert.equal(pg.layer, "view");
    assert.equal(pg.wordsAboveFold, 2, "the sidebar is not part of a step's words");
    assert.ok(pg.chunks.includes("Backups"), "the sidebar is on screen for banned words");
  });
  test("the whole count follows the inventory: main area plus every open dialog, all of it", async () => {
    const m = await measure(shell("<p>page words</p>", { modal: "<p>dialog text here</p>" }));
    assert.equal(m.wordsWhole, 5);
  });
  test("warning and error classes are counted when shown, with their text", async () => {
    const m = await measure(shell(`<div class="notice warn"><div class="doctor-card warn">! a</div><div class="doctor-card fail">x b</div></div>
      <span class="chip chip-mon">PENDING</span><div class="error-box">boom</div>`));
    assert.deepEqual(m.alarms.map((a) => a.selector).sort(),
      [".chip-mon", ".doctor-card.fail", ".doctor-card.warn", ".error-box", ".notice.warn"]);
  });
  test("warning classes that the interface paints neutral are not yellow or red", async () => {
    const m = await measure(shell(`<p class="cov-line warn">No events yet</p><span class="cov-chip warn">capture idle</span>
      <div class="doctor-card note">– nothing</div><div class="note-item">n</div><div class="muted-box">m</div>`));
    assert.deepEqual(m.alarms, []);
  });
  test("a hidden or empty error line is not an alarm; a shown one is", async () => {
    const m = await measure(shell(`<p class="form-msg err" hidden>gone</p><p class="form-msg err"></p><p class="form-msg err">Could not save</p>`));
    assert.equal(m.alarms.length, 1);
  });
  test("alarms behind the top layer are not on the step", async () => {
    const m = await measure(shell(`<div class="error-box">behind</div>`, { modal: "<p>dialog</p>" }));
    assert.deepEqual(m.alarms, []);
  });
  test("a failure toast counts wherever it floats", async () => {
    await page.setContent(shell("<p>x</p>"));
    await page.addScriptTag({ content: MEASURE });
    const m = await page.evaluate(() => {
      const t = document.getElementById("toast-error");
      t.hidden = false; t.textContent = "Backup failed";
      return window.__firstRunMeasure({});
    });
    assert.equal(m.alarms.length, 1);
  });
  test("a primary button below the fold reports how far; one in view reports 0", async () => {
    const below = await measure(shell(`<div style="height:1000px"></div><button class="go" style="height:40px">Save</button>`), { primary: ".go" });
    assert.ok(below.primary.belowFoldPx >= 300, JSON.stringify(below.primary));
    const inview = await measure(shell(`<button class="go">Save</button>`), { primary: ".go" });
    assert.equal(inview.primary.belowFoldPx, 0);
  });
  test("a button hidden inside a scrolled box is below that box's edge", async () => {
    const m = await measure(shell(`<div class="scroll"><div style="height:300px"></div><button class="go">Save</button></div>`), { primary: ".go" });
    assert.ok(m.primary.belowFoldPx > 150, JSON.stringify(m.primary));
  });
  test("a primary button that is not on the page is reported missing, not 0", async () => {
    const m = await measure(shell("<p>x</p>"), { primary: ".nope" });
    assert.equal(m.primary.missing, true);
    assert.equal(m.primary.belowFoldPx, null);
  });
  test("a primary button scrolled off the TOP is not 0 px below the fold", async () => {
    await page.setContent(shell(`<div class="scroll" id="sc"><button class="go">Save</button><div style="height:600px"></div></div>`));
    await page.addScriptTag({ content: MEASURE });
    const m = await page.evaluate(() => {
      document.getElementById("sc").scrollTop = 400;
      return window.__firstRunMeasure({ primary: ".go" });
    });
    assert.equal(m.primary.above, true);
    assert.equal(m.primary.belowFoldPx, null);
    assert.equal(m.primary.missing, false);
  });
  test("an alarm whose only content is an icon still counts", async () => {
    const m = await measure(shell(`<div class="warn-item"><svg width="16" height="16"></svg></div>`));
    assert.equal(m.alarms.length, 1);
    assert.equal(m.alarms[0].text, "(no text)");
  });
  test("the anchors it reads are reported, so a renamed one cannot pass as a small number", async () => {
    const found = await measure(shell("<p>x</p>", { side: "<p>s</p>" }));
    assert.deepEqual(found.anchors, { view: true, side: true, modal: true, notice: true, login: true, toastError: true });
    // The page element renamed: every word and alarm would otherwise be read
    // off <body> without a word of complaint.
    await page.setContent(`<html><body><main><div id="renamed"><p>one two three</p></div></main></body></html>`);
    await page.addScriptTag({ content: MEASURE });
    const lost = await page.evaluate(() => window.__firstRunMeasure({}));
    assert.equal(lost.anchors.view, false);
    assert.equal(lost.anchors.side, false);
  });
});

// The yellow-and-red list names classes in the console's own stylesheet and
// frontend. A rename there would make every selector match nothing, and
// "nothing is yellow" is exactly the score the redesign is aiming at — the
// one wrong answer that looks like success. This reads the shipped assets.
describe("the warning and error classes exist in the console", () => {
  const assets = ["app.js", "style.css"]
    .map((f) => readFileSync(path.join(HERE, "..", "..", "internal", "console", "assets", f), "utf8")).join("\n");
  const list = readFileSync(path.join(HERE, "first_run_measure.js"), "utf8");
  const selectors = [...list.slice(list.indexOf("const ALARMS = ["), list.indexOf("];", list.indexOf("const ALARMS = [")))
    .toString().matchAll(/"\.([^"]+)"/g)].map((m) => m[1]);
  test("the list was read", () => { assert.ok(selectors.length >= 20, "found " + selectors.length + " selectors"); });
  for (const sel of selectors) {
    test(sel + " is a class the console still uses", () => {
      for (const cls of sel.split(".")) assert.ok(assets.includes(cls), cls + " appears nowhere in the console assets");
    });
  }
});

// ── the loud skip ──────────────────────────────────────────────────────────

describe("first-run-walk.sh without Docker", () => {
  const script = path.join(HERE, "first-run-walk.sh");
  const run = (env) => spawnSync("bash", [script], {
    env: Object.assign({}, process.env, { DOCKER: "/nonexistent/docker", FIRST_RUN_WALK_SKIP_UNIT: "1" }, env),
    encoding: "utf8",
  });
  test("skips loudly with a non-zero exit, never a silent pass", () => {
    const r = run({ FIRST_RUN_WALK_ALLOW_SKIP: "" });
    assert.equal(r.status, 77, r.stdout + r.stderr);
    assert.match(r.stdout + r.stderr, /SKIPPED/);
    assert.match(r.stdout + r.stderr, /not a pass/);
  });
  test("an explicit local opt-in turns the skip into exit 0, still announced", () => {
    const r = run({ FIRST_RUN_WALK_ALLOW_SKIP: "1" });
    assert.equal(r.status, 0, r.stdout + r.stderr);
    assert.match(r.stdout + r.stderr, /SKIPPED/);
  });
});

// ── the committed baseline agrees with its own evidence ────────────────────
// The evidence block exists so a reviewer can see WHERE a number came from,
// and compareRatchet reads it only when it is a string (the not-measurable
// reason), so a number and the evidence under it can drift apart and every
// run stays green. That is not theoretical: the first PR to lower a column
// left `banned_words: 101` over a per-word map still summing to 114, and
// `changes_visible_without_reload: 3` over a list reading "UPDATE: not seen".
// An evidence block that contradicts its number is worse than none, because
// it is read as the proof.
//
// The identities below are structural: each one keys off a shape the file
// actually carries, and is checked wherever that shape appears, in any run
// and any column. A new run inherits them; a column with no evidence is not
// invented.
describe("the committed baseline agrees with its own evidence", () => {
  const baseline = loadBaseline(path.join(HERE, "first_run_baseline.json"));
  // Each identity: the evidence field it reads, and the count it must equal.
  const identities = {
    // Every banned word counted, per word, has to add up to the total.
    per_word: (m) => Object.values(m).reduce((a, b) => a + b, 0),
    // One entry per click walked.
    clicks: (a) => a.length,
    // "INSERT: seen after 4.3 s" counts, "UPDATE: not seen" does not.
    changes: (a) => a.filter((c) => /:\s*seen\b/.test(c)).length,
  };
  const found = [];
  for (const [run, cols] of Object.entries(baseline.evidence || {})) {
    for (const [col, ev] of Object.entries(cols || {})) {
      if (!ev || typeof ev !== "object") continue;
      for (const [field, count] of Object.entries(identities)) {
        if (!(field in ev)) continue;
        found.push({ run, col, field, count: count(ev[field]), value: baseline.runs[run] && baseline.runs[run][col] });
      }
    }
  }
  test("every number with countable evidence is checked", () => {
    // A guard that found nothing to check would pass for ever in silence.
    assert.ok(found.length >= 3, "no countable evidence found at all: " + JSON.stringify(found));
  });
  for (const f of found) {
    test(`${f.run}.${f.col}: its ${f.field} evidence counts ${f.value}`, () => {
      assert.equal(f.count, f.value,
        `${f.run}.${f.col} is recorded as ${f.value} but its ${f.field} evidence counts ${f.count}; ` +
        "refresh the evidence from the same run that produced the number, or the evidence is read as a proof of something that did not happen");
    });
  }
});
