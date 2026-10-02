package console

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #2006: the schedule card when the loop stopped taking full reads for a
// refusal one did not fix. The state goes through the real DTO builder and
// the real page code; the words are read from what the page draws.
func TestBackupScheduleCard_fallbackStopped_2006(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	rep := &stubScheduleReporter{full: true, state: map[string]BackupScheduleState{}}
	srv, id := newScheduleServer(t, rep)
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"5m"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	e, _ = srv.cm.reg.Get(id)
	reason := "full-table reconstruct failed: resolve schema for demo.mydumper_0: table demo.mydumper_0 not found in snapshot 8; consider re-running `bintrail snapshot`"
	refused := []RefusedTable{{Name: "demo.mydumper_0", Verdict: "refused", Reason: reason}}
	base := BackupScheduleState{LastFallbackAt: "2026-10-02T10:00:05Z", LastFallbackReason: reason,
		LastFallbackTables: 4, LastFallbackRefused: 1, LastFallbackRefusedTables: refused}
	now := time.Date(2026, 10, 2, 10, 20, 0, 0, time.UTC)

	dtoJSON := func(st BackupScheduleState) string {
		rep.state[id] = st
		b, err := json.Marshal(srv.backupScheduleDTO(context.Background(), e, now))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	stopped := base
	stopped.LastFallbackStoppedAt = "2026-10-02T10:05:07Z"
	withheld := stopped
	withheld.LastFallbackRefusedTables = nil // a data profile hides the names
	stoppedDoc, fellDoc, withheldDoc := dtoJSON(stopped), dtoJSON(base), dtoJSON(withheld)
	if !strings.Contains(stoppedDoc, `"stopped_at":"2026-10-02T10:05:07Z"`) || strings.Contains(fellDoc, "stopped_at") {
		t.Fatalf("stopped_at on the wire: stopped %s\nfell back %s", stoppedDoc, fellDoc)
	}

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
vm.runInContext("capsCache.backup_schedule = true;", ctx);
const walk = (n, f) => { if (!n || n.nodeType !== 1) return; f(n); for (const c of n.children) walk(c, f); };
const text = (n) => { const out = []; walk(n, (x) => { if (x._text) out.push(x._text); }); return out; };
const reds = (n) => { const out = []; walk(n, (x) => { if (x.tag === "p" && /\berr\b/.test(x.className) && x._text) out.push(x._text); }); return out; };
const cur = { id: "s1", kind: "registry", baseline_s3: "" };
const out = {};
for (const [k, doc] of Object.entries({ stopped: ` + stoppedDoc + `, fell: ` + fellDoc + `, withheld: ` + withheldDoc + ` })) {
  const card = ctx.backupScheduleCard(cur, { schedule: doc });
  out[k] = { all: text(card), reds: reds(card) };
}
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "card.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]struct {
		All  []string `json:"all"`
		Reds []string `json:"reds"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("%v\n%s", err, raw)
	}
	st, fell := got["stopped"], got["fell"]
	if w := strings.Join(got["withheld"].All, " | "); !strings.Contains(w, "was refused for 1 table, and") || strings.Contains(w, "mydumper_0") || strings.Contains(w, "below") {
		t.Errorf("with the names withheld the card must count, not point at a list: %s", w)
	}
	all := strings.Join(st.All, " | ")
	t.Logf("stopped card: %s", all)
	t.Logf("fallback card: %s", strings.Join(fell.All, " | "))
	for _, want := range []string{"No new snapshot is being published", "another full read would not fix it",
		"takes no full read in its place", "demo.mydumper_0", "No full read is taken in its place."} {
		if !strings.Contains(all, want) {
			t.Errorf("the stopped card does not say %q:\n%s", want, all)
		}
	}
	for _, bad := range []string{"bintrail snapshot", "\u2014", " --", "so a full read was started instead"} {
		if strings.Contains(all, bad) {
			t.Errorf("the stopped card says %q:\n%s", bad, all)
		}
	}
	if !strings.Contains(strings.Join(fell.Reds, " | "), "so a full read was started instead") ||
		strings.Contains(strings.Join(fell.All, " | "), "takes no full read") {
		t.Errorf("a fallback that has not stopped changed its words: %q", fell.All)
	}
}
