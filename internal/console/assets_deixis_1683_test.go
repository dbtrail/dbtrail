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
// A string appended to an element is text too (the router's lines are).
const prose = (n) => !n ? "" : typeof n === "string" ? n : n.nodeType === 3 ? n.textContent : n.tag === "code" ? "" : (n._text || "") + (n.children || []).map(prose).join(" ");
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
out.token = lines(run("mcpTokenStep")({ managed: true, read_only: true, static: true, created_at: "2026-06-10T12:00:00Z" }, ""));
run("capsCache = { monitor: true };");
const fb = JSON.parse(process.argv[4]);
// The port, on, with what the router answered (#2341): on every address of
// its machine, on one address, and not at all.
const rt = JSON.parse(process.argv[5]);
out.sqlNamed = lines(run("sqlClientPanel")(servers, fb.wildcard, "", null, rt.wild));
out.sqlHost = lines(run("sqlClientPanel")(servers, fb.wildcard, "", null, rt.host));
out.sqlDown = lines(run("sqlClientPanel")(servers, fb.wildcard, "", null, rt.down));
out.sqlNoRouter = lines(run("sqlClientPanel")(servers, fb.wildcard, "", null, rt.none));
out.sqlRouted = lines(run("sqlClientPanel")(servers, Object.assign({}, fb.wildcard, { routing_on: true }), "", null, rt.wild));
out.sqlOff = lines(run("sqlClientPanel")(servers, fb.off));
out.sqlOffMine = lines(run("sqlClientPanel")(servers, Object.assign({}, fb.off, { can_manage: true })));
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
		"wildcard": (&Server{flashback: flashbackState{startup: true, listen: ":3308"}}).flashbackStatus(),
		"off":      (&Server{}).flashbackStatus(),
	})
	if err != nil {
		t.Fatal(err)
	}
	router := func(listen string) routerViewDTO {
		return routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-11T14:31:00Z",
			Status: &RouterStatusDoc{Version: 1, Listen: listen, ReadOnly: true, CostThreshold: 10000, CopySnapshotsReadAt: "2026-10-11T14:30:50Z",
				RouterNumbers: RouterNumbers{CountingSince: "2026-10-11T09:00:00Z",
					Pool: &RouterPoolNumbers{Workers: 4, Running: 1, Waiting: 1, ThreadsEach: 2, MemoryEach: "2048MiB"},
					Servers: map[string]RouterServerNumbers{
						"s1": {Copy: 4, MySQL: 9, Refused: 3, Reasons: map[string]uint64{"expensive_plan": 4, "cheap_plan": 9}, CopySnapshot: "2026-10-11T14:19:00Z",
							CannotRoute: true, AccountRefused: true, SessionUntracked: true},
					}}}}
	}
	rt, err := json.Marshal(map[string]routerViewDTO{
		"wild": router("0.0.0.0:3310"),
		"host": router("10.0.0.5:3310"),
		"down": {Configured: true, URL: "http://router:9091/status", Error: "it could not be reached: connection refused"},
		"none": {},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := runNodeConnectArgs(t, renderHarnessJS+deixisJS, string(fb), string(rt))
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
	has("rotation", "Changes saved with Edit rotation take effect only after DBTrail restarts.")
	has("duck", "Runs in your own DuckDB, not in DBTrail, and no credentials are in the file.")
	has("iceberg", "not the server picked in the left sidebar.")
	// With views off the lane is unchanged: the archive carries its own views.sql.
	has("lane", "Unpack it, go into its folder, and run:")
	has("token", "Why are there no New token and Delete token buttons?")
	has("token", "This token was created by a newer version of DBTrail. It keeps working, but this version cannot replace or delete it")
	has("token", "It is managed wherever it was set up, not in the web interface.")
	// The MySQL port's panel (#2341). The router serves the port, so the
	// sentences that named a startup flag or the access token are gone; the
	// ones that took their place name the web interface, DBTrail, the router
	// or the sidebar, never "here".
	has("sqlServe", "Not available: this DBTrail only reads. The MySQL port is turned on in the web interface of the DBTrail service")
	has("sqlOff", "The MySQL port cannot be turned on or off from the web interface.")
	has("sqlOff", "Its saved setting cannot be changed by this DBTrail.")
	has("sqlOffMine", "Off. Turn it on and your usual MySQL client can read any table as it was at a chosen moment.")
	has("sqlNamed", "Password: the one shown when it was created. It cannot be shown again.")
	has("sqlNamed", "The command uses the name the web interface was opened with.")
	has("sqlNamed", "the server picked in the left sidebar")
	has("sqlNamed", "The router is the service that answers this port, on port 3310.")
	// Who answered, drawn (#2330): the caption (the server's name is code,
	// left out here), a figure per side with the tooltip that says what the
	// side is, the copy's lag, and the foot.
	has("sqlNamed", "Who answered for")
	has("sqlNamed", "Your MySQL")
	has("sqlNamed", "9 statements")
	has("sqlNamed", "DBTrail's copy")
	has("sqlNamed", "4 statements")
	has("sqlNamed", "Lag: unknown (last answer)")
	has("sqlNamed", "3 refused, run by neither")
	has("sqlNamed", "Statements the router sent to your MySQL server: the light reads, and everything that is not a read.")
	has("sqlNamed", "Heavy reads the router answered from DBTrail's copy instead of your MySQL server.")
	// Drawn, so not said again; and with no router answer, not drawn.
	for _, k := range []string{"sqlNamed", "sqlHost", "sqlRouted"} {
		for _, l := range got[k] {
			if strings.Contains(l, "by MySQL,") {
				t.Errorf("%s: the counts are drawn and also said: %q", k, l)
			}
		}
	}
	for _, k := range []string{"sqlDown", "sqlNoRouter"} {
		for _, l := range got[k] {
			if strings.Contains(l, "Who answered") || strings.Contains(l, "Your MySQL") || strings.Contains(l, "run by neither") {
				t.Errorf("%s: who answered is drawn with no answer from a router: %q", k, l)
			}
		}
	}
	// Where a statement goes is drawn as lanes now (TestRoutingBlockSaysWhoAnswered
	// reads them); the one sentence left is the one a drawing cannot say.
	has("sqlNamed", "Turn it on and an application can use this port in place of its MySQL.")
	has("sqlHost", "The port answers on 10.0.0.5 only.")
	has("sqlHost", "To answer other machines, start the router with another address")
	has("sqlDown", "The router that serves the port did not answer, so 3309 above is the port's usual number, not a confirmed one.")
	has("sqlDown", "No numbers from the router.")
	has("sqlNoRouter", "This DBTrail was not told where the router that serves the port is, so 3309 above is the port's usual number, not a confirmed one.")
	for _, k := range []string{"sqlNamed", "sqlHost", "sqlDown", "sqlNoRouter", "sqlRouted", "sqlOff", "sqlOffMine", "sqlServe"} {
		for _, l := range got[k] {
			for _, old := range []string{"access token", "when DBTrail starts", "time-travel port", "never shown", "Paste the"} {
				if strings.Contains(l, old) {
					t.Errorf("%s: %q still says %q", k, l, old)
				}
			}
		}
	}
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
