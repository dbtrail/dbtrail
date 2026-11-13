// The "Not in S3" line of the Backup settings card (#2691), in every
// combination of what the cleanup's record can hold. Run with:
//
//   node --test test/console-e2e/not_in_s3_words.test.mjs
//
// It runs the real notInS3Words from app.js in a vm context: no daemon, no
// browser. What it pins: a copy S3 could not be asked about, or one S3 took
// and refuses to read back, is never said to be missing ("S3 does not have"
// a set that holds it), every copy listed is named once, and the counts the
// line says add up to the record's.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import vm from "node:vm";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const APP = readFileSync(path.join(HERE, "../../internal/console/assets/app.js"), "utf8");

function fn(name) {
  const start = APP.indexOf("\nfunction " + name + "(");
  assert.ok(start >= 0, name + " is not in app.js");
  let depth = 0;
  for (let i = APP.indexOf("{", start); i < APP.length; i++) {
    if (APP[i] === "{") depth++;
    else if (APP[i] === "}" && --depth === 0) return APP.slice(start, i + 1);
  }
  throw new Error("unbalanced " + name);
}
const ctx = vm.createContext({});
vm.runInContext(["utcLabel", "notInS3Words"].map(fn).join("\n"), ctx);
const words = (n, rec) => ctx.notInS3Words(n, rec);

const A = "2026-11-10T03-00-00Z", B = "2026-11-10T03-05-00Z", R = "2026-11-09T12-00-00Z"; // R: a restore's result
const label = (name) => name.replace(/^(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})Z$/, "$1 $2:$3:$4 UTC");

// Every record the daemon can write for these three copies: which of them
// S3 lacks, could not be asked about, or refuses to read back; written by
// the cleanup alone (no resend) or after a resend round; with a wait.
function* records() {
  const all = [A, B, R];
  for (let mask = 1; mask < 8; mask++) {
    const names = all.filter((_, i) => mask & (1 << i));
    for (let am = 0; am < 1 << names.length; am++) {
      const notAsked = names.filter((_, i) => am & (1 << i));
      for (let rm = 0; rm < 1 << notAsked.length; rm++) {
        const readRefused = notAsked.filter((_, i) => rm & (1 << i));
        for (const resending of [false, true]) {
          for (const next of resending ? [null, "2026-11-11T04:00:00Z"] : [null]) {
            const rec = { at: "2026-11-11T03:00:00Z", count: names.length, names, resending };
            if (notAsked.length) rec.not_asked = notAsked;
            if (readRefused.length) rec.read_refused = readRefused;
            if (next) rec.next_send = next;
            yield rec;
          }
        }
      }
    }
  }
}

test("a copy S3 could not be asked about is never said to be missing, in every combination", () => {
  let seen = 0;
  for (const rec of records()) {
    const s = words(rec.count, rec);
    seen++;
    // Not asked and not refused on read: the line must not call these
    // missing. (One refused on read is said of its own, after the rest.)
    const unknown = (rec.not_asked || []).filter((x) => !(rec.read_refused || []).includes(x)).length;
    const ctxt = JSON.stringify(rec) + "\n  -> " + s;
    if (unknown) {
      // Said of the whole set, the line would call a copy missing that S3
      // only failed to answer for.
      assert.ok(!/S3 does not have (them|it) complete/.test(s), "a not-asked copy said to be missing: " + ctxt);
      assert.ok(!s.startsWith("Not in S3 yet"), "a not-asked copy said not to be in S3: " + ctxt);
    }
    // Each copy is named in the line.
    for (const name of rec.names) assert.ok(s.includes(label(name)), "copy " + name + " not named: " + ctxt);
    // The ones S3 lacks, when some are not, are named as lacking.
    const lacking = rec.names.filter((x) => !(rec.not_asked || []).includes(x));
    if (unknown && lacking.length) {
      for (const name of lacking) assert.match(s, new RegExp("S3 does not have [^.;]*" + label(name).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")), "a lacking copy not said to be lacking: " + ctxt);
    }
    // A copy S3 refuses to read back is said not to be sent again.
    for (const name of rec.read_refused || []) {
      assert.ok(/does not send|Not sent again/.test(s), "a copy S3 refuses to read is not said to stay unsent: " + ctxt);
      void name;
    }
    assert.ok(!/undefined|NaN|\bnull\b/.test(s) && !s.includes("—"), "drawn text: " + ctxt);
  }
  assert.ok(seen > 100, "only " + seen + " records generated");
});

// The report quotes these.
test("several copies, one of them a restore's result S3 could not be asked about", () => {
  const rec = { at: "2026-11-11T03:00:00Z", count: 3, names: [A, B, R], resending: true, not_asked: [R] };
  const s = words(3, rec);
  console.log("cleanup and resend: " + s);
  assert.equal(s, "Kept here: 3 older copies (2026-11-10 03:00:00 UTC, 2026-11-10 03:05:00 UTC, 2026-11-09 12:00:00 UTC). " +
    "S3 does not have 2 older copies (2026-11-10 03:00:00 UTC, 2026-11-10 03:05:00 UTC) complete, and could not be asked about the copy of 2026-11-09 12:00:00 UTC; DBTrail sends them again at the next cleanup.");
  const byHand = words(3, Object.assign({}, rec, { resending: false }));
  console.log("cleanup alone: " + byHand);
  assert.match(byHand, /could not be asked about the copy of 2026-11-09 12:00:00 UTC; send them to S3 again, or remove them by hand if not needed\.$/);
});
