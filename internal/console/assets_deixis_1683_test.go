package console

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

// deixisJS draws the functions whose sentences said "here" or "this page"
// for the screen (#1683) and returns every line of text they show, a
// tooltip included, with <code> left out: a flag keeps its spelling.
const deixisJS = `
const prose = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : n.tag === "code" ? "" : (n._text || "") + (n.children || []).map(prose).join(" ");
const tidy = (s) => s.replace(/\s+/g, " ").trim();
// A tooltip is read wherever it sits, also inside a line read whole.
const tips = (n, out) => {
  if (!n || n.nodeType === 3) return;
  const tip = n.title || (n.attrs || {}).title;
  if (tip) out.push(tidy(String(tip)));
  (n.children || []).forEach((c) => tips(c, out));
};
const lines = (n, out = []) => {
  if (!n) return out;
  if (typeof n === "string") { out.push(tidy(n)); return out; }
  if (n.nodeType === 3) return out;
  if (["p", "summary", "div", "span", "button", "a", "h2"].includes(n.tag) && !(n.children || []).some((c) => c && c.nodeType === 1 && ["p", "div", "section", "details"].includes(c.tag))) {
    tips(n, out);
    const t = tidy(prose(n));
    if (t) out.push(t);
    return out;
  }
  const tip = n.title || (n.attrs || {}).title;
  if (tip) out.push(tidy(String(tip)));
  (n.children || []).forEach((c) => lines(c, out));
  return out;
};
const run = (s) => vm.runInContext(s, ctx);
const out = {};
run('currentServer = "s1"; defaultServerId = "s1"; capsKnown = true;');
const servers = [{ id: "s1", name: "shop db", kind: "registry" }];
run('location.pathname = "/events";');
out.docs = lines(run("pageHead")("Events", null));
out.cov = lines(run("covCard")({ continuity: "ok", freshness: "idle", lag_seconds: 600,
  delta_from: "2026-09-01 09:00:00", delta_to: "2026-09-01 10:00:00" }, null));
run("capsCache = { reconstruct: false };");
out.state = lines(run("stateSection")({}));
run("routeArrivedFrom = [...SNAPSHOT_MOVED.keys()][0];");
out.moved = lines(run("snapshotsMovedNotice")("unknown"));
out.rotation = lines(run("rotationCard")({ enabled: false, retain: "7d", interval: "1h", add_future: 2, source: "default" }));
run("capsCache = { views: true, monitor: true, permissions: null };");
out.duck = lines(run("duckdbPanel")());
out.iceberg = lines(run("icebergComposeNote")({ id: "s1", kind: "registry" }));
run("capsCache = { views: false, monitor: true, permissions: null };");
out.lane = lines(run("backupDuckLane")({ configured: true, source: "/data/snaps", kind: "dir",
  snapshots: [{ time: "2026-06-10 12:00:00", location: "dir", files: [{ name: "x.parquet", bytes: 10 }] }] }));
out.token = lines(run("mcpTokenCard")({ managed: true, read_only: true, static: true, created_at: "2026-06-10T12:00:00Z" }, ""));
run("capsCache = { monitor: true };");
const fb = JSON.parse(process.argv[4]);
out.sqlNamed = lines(run("sqlClientPanel")(servers, fb.wildcard));
out.sqlOff = lines(run("sqlClientPanel")(servers, fb.off));
run("capsCache = {};");
out.sqlServe = lines(run("sqlClientPanel")(servers, fb.off));
run("capsKnown = false;");
out.noCapture = lines(run("noCaptureReason")({ id: "s1" }));
console.log(JSON.stringify(out));
`

// TestScreenTextNamesThePlace draws the sentences that pointed at the screen
// with "here" or "this page" and checks that each one now names what it
// means: a page by its sidebar name, a button by its label, or the web
// interface. Written on the docs site those words point at the docs site,
// so the page and its documentation could not both be right (#1683).
//
// It reads only the functions drawn below. "here" meaning a folder, a
// bucket or the machine DBTrail runs on is a different word and is not
// looked at; neither is a sentence in a function nobody draws, which the
// source list at the end covers only as far as its exact old wording.
func TestScreenTextNamesThePlace(t *testing.T) {
	fb, err := json.Marshal(map[string]flashbackStatusDTO{
		"wildcard": (&Server{flashbackListen: ":3308"}).flashbackStatus(),
		"off":      (&Server{}).flashbackStatus(),
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := runNodeConnectArgs(t, renderHarnessJS+deixisJS, string(fb))
	var got map[string][]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	deixis := regexp.MustCompile(`(?i)\bhere\b|\bthis page\b|\bconsole\b`)
	for k, ls := range got {
		if len(ls) == 0 {
			t.Errorf("%s: nothing was drawn", k)
		}
		t.Logf("%s:\n  %s", k, strings.Join(ls, "\n  "))
		for _, l := range ls {
			if deixis.MatchString(l) {
				t.Errorf("%s: %q points at the screen without naming it", k, l)
			}
		}
	}
	has := func(k, want string) {
		t.Helper()
		for _, l := range got[k] {
			if strings.Contains(l, want) {
				return
			}
		}
		t.Errorf("%s: want a line containing %q in:\n  %s", k, want, strings.Join(got[k], "\n  "))
	}
	has("docs", "Open the Events docs in a new tab")
	has("cov", "Either nothing changed on this server, or capture fell behind, and DBTrail cannot tell which.")
	has("state", "Configure a snapshot for this server to see a row's earlier state.")
	has("moved", "the capability check failed when the Snapshots page loaded; reload the page to get it back.")
	has("rotation", "Changes saved with Edit rotation take effect only after the daemon restarts.")
	has("duck", "Runs in your own DuckDB, not in DBTrail, and no credentials are in the file.")
	has("iceberg", "not the server picked in the left sidebar.")
	has("lane", "The file that describes these tables is not offered, because DBTrail is set not to read archived data.")
	has("token", "Why are there no New token and Delete token buttons?")
	has("token", "This token was created by a newer version of DBTrail. It keeps working, but this version cannot replace or delete it")
	has("token", "It is managed wherever it was set up, not in the web interface.")
	has("sqlServe", "Not available: this DBTrail is read-only.")
	has("sqlOff", "The port is set when the daemon starts, not from the web interface.")
	has("sqlNamed", "never shown in the web interface")
	has("sqlNamed", "the command uses the name the web interface was opened with.")
	has("noCapture", "the capability check failed when the web interface loaded, so capture cannot be started. Reload the page")

	// Sentences in functions that need a live page to draw (a fetch loop, a
	// dialog, a confirm box). Their old wording must be gone.
	js := readAsset(t, "app.js")
	for _, old := range []string{
		"It carries the access token this page needs.",
		`"This page stopped updating: "`,
		`"This page has not updated since "`,
		"Parts of this page are missing",
		"no archived changes configured on this page",
		"is read-only here; values are shown",
		"that this page cannot show",
		"You can create a new token here whenever you want.",
		"Saving here creates a custom setting",
		"old data is uploaded here before",
		"leaving the row as shown here",
	} {
		if strings.Contains(js, old) {
			t.Errorf("app.js still says %q", old)
		}
	}
}
