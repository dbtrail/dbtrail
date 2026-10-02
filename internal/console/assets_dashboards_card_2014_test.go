package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// #2014: what the Overview's "Dashboards for the team" card says in each
// state, rendered by the real page code from documents the real route
// produced. The texts are logged so a reviewer reads them as the user will.
func TestDashboardsCard_textsFromRealDocuments_2014(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	const prefix, empty = "dash-2014-card/", "dash-2014-card-empty/"
	newDashS3(t, append(snapKeys(prefix, "2026-10-02T06-00-00Z", "demo/prices", "demo/orders"),
		empty+"2026-10-02T06-00-00Z/_INCOMPLETE")...)
	docs := map[string]string{}
	get := func(name string, srv *Server) {
		code, _, raw := getDashboards(t, srv)
		if code != 200 {
			t.Fatalf("%s: code = %d: %s", name, code, raw)
		}
		docs[name] = raw
	}

	local := t.TempDir()
	writeBaselineFixture(t, local, "2026-10-02T07-00-00Z", "demo", "prices.parquet")
	s3srv := newDashServer(t, local, "s3://b/"+prefix)
	s3srv.rememberBucketRegion("b", "eu-west-1", true)
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "history.json"))
	if err != nil {
		t.Fatal(err)
	}
	left, omitted := LeftOutTablesOf([]LeftOut{{Table: "demo.order/items",
		Reason: `the name holds a "/", which a snapshot cannot store as a file name`}})
	if err := h.Append(BaselineRunRecord{ServerID: bootServerID, Kind: BaselineRunDump, SnapshotTime: "2026-10-02T06:00:00Z",
		StartedAt: "2026-10-02T06:00:00Z", FinishedAt: "2026-10-02T06:01:00Z", LeftOutTables: left, LeftOutTablesOmitted: omitted}); err != nil {
		t.Fatal(err)
	}
	s3srv.baselineHistory = h
	get("s3", s3srv)
	get("s3_empty", newDashServer(t, "s3://b/"+empty, ""))
	get("local_only", newDashServer(t, local, ""))
	// A bucket this store does not have: the listing fails.
	get("s3_unreadable", newDashServer(t, "s3://c/dash-2014-card/", ""))
	get("s3_pattern_chars", newDashServer(t, "s3://b/team [x]/", ""))
	get("none", newDashServer(t, "", ""))
	noArch := newDashServer(t, local, "")
	noArch.cm.boot.noArchive = true
	get("no_archive", noArch)

	// The refusal under a data profile, from the real handler.
	req := httptest.NewRequest("GET", "/api/dashboards", nil)
	req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{},
		&ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}))
	w := httptest.NewRecorder()
	s3srv.handleDashboards(w, req)
	var refusal struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &refusal); err != nil || w.Code != http.StatusForbidden {
		t.Fatalf("profile refusal: code = %d body = %s", w.Code, w.Body.String())
	}
	profileMsg, _ := json.Marshal(refusal.Error)

	var js strings.Builder
	js.WriteString(renderHarnessJS + `
const walk = (n, out) => { if (!n) return; if (n.nodeType === 3) { out.push(n.textContent); return; } if (n.nodeType !== 1) return; if (n._text) out.push(n._text); for (const c of n.children) walk(c, out); };
const text = (n) => { const out = []; walk(n, out); return out.join("").replace(/\s+/g, " ").trim(); };
const docs = {`)
	for name, raw := range docs {
		js.WriteString(`"` + name + `": ` + raw + ",\n")
	}
	js.WriteString(`};
const out = {};
for (const [k, d] of Object.entries(docs)) out[k] = text(ctx.dashPanelBody(d));
out.profile = ctx.dashErrorText(403, ` + string(profileMsg) + `);
out.denied = ctx.dashErrorText(403, "");
console.log(JSON.stringify(out));
`)
	path := filepath.Join(t.TempDir(), "dash.js")
	if err := os.WriteFile(path, []byte(js.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	rawOut, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, rawOut)
	}
	var got map[string]string
	if err := json.Unmarshal(rawOut, &got); err != nil {
		t.Fatalf("%v\n%s", err, rawOut)
	}
	for _, k := range []string{"s3", "s3_empty", "s3_unreadable", "s3_pattern_chars", "local_only", "none", "no_archive", "profile", "denied"} {
		t.Logf("%s: %s", k, got[k])
	}

	want := map[string][]string{
		"s3": {
			"Your team reads the copy straight from S3, in their own DuckDB. Nothing is downloaded, and each new session reads the newest snapshot.",
			"Newest snapshot on S3: 2026-10-02 06:00:00 UTC, 2 tables.",
			"A newer snapshot, from 2026-10-02 07:00:00 UTC, is on this machine and not on S3 yet.",
			"Download views.sql",
			"duckdb -init views.sql team.duckdb",
			".read views.sql",
			"In Metabase, paste it into Init SQL.",
			"Read access to s3://b/" + prefix + ".",
			"The bucket's region, eu-west-1. The file already names it.",
			"The file holds none, and DBTrail never hands out its own.",
			"1 table is not in this snapshot, so not in the file:",
			`demo.order/items: the name holds a "/"`,
		},
		"s3_empty":      {"Snapshots for this server go to S3, at s3://b/" + empty + ", and none has finished uploading there yet.", "See Snapshots"},
		"s3_unreadable": {"DBTrail could not prepare the file for the S3 location s3://c/dash-2014-card/. The reason is below.", "See Snapshots"},
		"local_only":    {"This server keeps its snapshots only on this machine, so a teammate's DuckDB cannot reach them.", "Add an S3 location"},
		"none":          {"No snapshot yet.", "Set up snapshots"},
		"no_archive":    {"Reading the copy is turned off for this server"},
		"profile":       {"Not available while a data profile is active."},
		"denied":        {"Your session is not allowed to get this file."},
	}
	for k, phrases := range want {
		for _, p := range phrases {
			if !strings.Contains(got[k], p) {
				t.Errorf("%s card lacks %q:\n%s", k, p, got[k])
			}
		}
	}
	for k, v := range got {
		if strings.Contains(v, "—") {
			t.Errorf("%s card has an em dash: %s", k, v)
		}
		// No CLI flag on screen but the duckdb command line itself.
		if strings.Contains(strings.ReplaceAll(v, "duckdb -init views.sql", ""), " -") {
			t.Errorf("%s card shows a flag: %s", k, v)
		}
	}
	// A local-only server is never offered a file, and the profile refusal
	// names no table.
	if strings.Contains(got["local_only"], "Download views.sql") || strings.Contains(got["profile"], "demo.") {
		t.Errorf("local_only = %s\nprofile = %s", got["local_only"], got["profile"])
	}
}
