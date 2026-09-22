// Renders the Snapshots page and reads the two download lanes back off the DOM.
// go test never renders the SPA, so "the panel is mounted" and "the panel is
// VISIBLE with two file tiles on one lane and one on the other" are different
// claims; the Go guards cover the first, this covers the second.
//
// Run it against the e2e fixture stack, which is what has a real baseline
// snapshot to list:
//
//   sed "s|node console_e2e.mjs|SHOT_OUT=/tmp node shot-lanes.local.mjs|" \
//     test/console-e2e/run.sh > /tmp/r.sh && chmod +x /tmp/r.sh && \
//     (cd test/console-e2e && /tmp/r.sh)
//
// It creates the "byo-idx" registry server itself (run.sh does not; scenario 5
// of console_e2e.mjs does), so it works on a stack run.sh brought up alone.
import { chromium } from "playwright";
const OUT = process.env.SHOT_OUT;
const URL = process.env.CONSOLE_URL, TOK = process.env.CONSOLE_TOKEN;
const browser = await chromium.launch();
const page = await (await browser.newContext()).newPage();
await page.setViewportSize({ width: 1440, height: 1000 });
await page.goto(URL + "/?token=" + encodeURIComponent(TOK));
await page.waitForFunction(() => document.querySelector(".side"), { timeout: 15000 });
await page.evaluate(() => { if (typeof closeServersModal === "function") closeServersModal(); });
// The fixture with a real baseline snapshot is the "byo-idx" registry server;
// the default selection is the source-less boot entry, which has none.
const byoId = await page.evaluate(async (baselineDir) => {
  const r = await api("/api/servers");
  const found = (r.servers || []).find((x) => x.name === "byo-idx");
  if (found) return found.id;
  const res = await api("/api/servers", { method: "POST", body: {
    name: "byo-idx", host: "127.0.0.1", port: "13306", user: "root", password: "testroot",
    dbname: "bintrail_e2e_idx", baseline_dir: baselineDir,
  } });
  return res.id;
}, process.env.E2E_BASELINE_DIR || "");
console.log("byo-idx id:", byoId);
if (byoId) await page.evaluate(async (id) => { await switchServer(id); }, byoId);
await page.waitForTimeout(600);
await page.evaluate(() => navigate("snapshots"));
await page.waitForFunction(() => location.pathname === "/snapshots", { timeout: 10000 });
await page.waitForTimeout(1200);
await page.evaluate(() => { if (typeof closeServersModal === "function") closeServersModal(); });

const seen = await page.evaluate(() => {
  const q = (s) => Array.from(document.querySelectorAll(s));
  const panel = document.querySelector(".bk-take");
  const lanes = q(".bk-lane");
  const listTop = document.querySelector(".stg-list");
  return {
    panelPresent: !!panel,
    panelTitle: panel ? panel.querySelector(".ov-panel-title").textContent : null,
    lanes: lanes.map((l) => ({
      title: l.querySelector(".bk-lane-t") ? l.querySelector(".bk-lane-t").textContent : null,
      tiles: l.querySelectorAll(".bk-file").length,
      tileNames: Array.from(l.querySelectorAll(".bk-file-n")).map((n) => n.textContent),
      buttons: Array.from(l.querySelectorAll("button")).map((b) => b.textContent),
      visible: l.getBoundingClientRect().height > 0,
    })),
    panelAboveList: !!(panel && listTop) &&
      panel.getBoundingClientRect().top < listTop.getBoundingClientRect().top,
    rowsShown: q(".stg-row").length,
    pagerText: document.querySelector(".bk-pager") ? document.querySelector(".bk-pager").innerText.replace(/\s+/g, " ") : null,
  };
});
console.log(JSON.stringify(seen, null, 2));
await page.screenshot({ path: OUT + "/backups-lanes.png", fullPage: true });
await browser.close();
