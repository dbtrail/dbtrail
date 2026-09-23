package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestSnapshotRetentionLines_1681 drives the page's own snapshotRetentionLines
// over every shape GET /api/baselines can give the four retention fields.
// The server OMITS a field that does not apply, so each case below is a
// presence case, and "nothing drawn" is an expected answer, not a default.
func TestSnapshotRetentionLines_1681(t *testing.T) {
	js := readAsset(t, "app.js")
	if !strings.Contains(functionBody(t, js, "function baselinesPanel("), "snapshotRetentionLines(b).forEach") {
		t.Fatal("baselinesPanel no longer draws the retention lines above the list")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	script := strings.Join([]string{
		functionBody(t, js, "function snapshotRetentionLines("),
		functionBody(t, js, "function utcLabel("),
		functionBody(t, js, "function firstLine("),
		functionBody(t, js, "function baselineRefreshNote("),
	}, "\n") + `
const backupFoldError = (e) => e, budgetRefusedTail = () => "", reusedCopiedNote = () => "";
// A node is its class, its own text, and its children, in order.
function el(tag, o, ...kids) {
  const n = { cls: (o && o.class) || "", text: (o && o.text) || "", kids: [] };
  n.append = (...k) => { for (const c of k) if (c) n.kids.push(c); };
  n.append(...kids);
  return n;
}
const flat = (n) => [n.text, ...n.kids.map(flat)].filter(Boolean).join(" | ");
const cases = JSON.parse(require("fs").readFileSync(0, "utf8"));
const out = {};
for (const [name, b] of Object.entries(cases)) {
  out[name] = snapshotRetentionLines(b).map((n) => ({ red: /\berr\b/.test(n.cls), text: flat(n) }));
}
// A failed automatic refresh is red; a successful one stays grey.
out.refreshFailed = [baselineRefreshNote({ state: "failed", last_error: "boom" }).cls];
out.refreshOK = [baselineRefreshNote({ state: "succeeded", tables: 2 }).cls];
console.log(JSON.stringify(out));
`
	type line struct {
		Red  bool   `json:"red"`
		Text string `json:"text"`
	}
	cases := map[string]any{
		"none":         map[string]any{},
		"keepMany":     map[string]any{"local_retention": map[string]any{"keep_newest": 7}},
		"keepOne":      map[string]any{"local_retention": map[string]any{"keep_newest": 1}},
		"keepZero":     map[string]any{"local_retention": map[string]any{"keep_newest": 0}},
		"pruneOnly":    map[string]any{"last_prune": map[string]any{"at": "2026-09-23T03:10:00Z", "removed": 3}},
		"pruneOne":     map[string]any{"last_prune": map[string]any{"at": "2026-09-23T03:10:00Z", "removed": 1}},
		"pruneNothing": map[string]any{"last_prune": map[string]any{"at": "2026-09-23T03:10:00Z", "removed": 0}},
		"keepAndPrune": map[string]any{"local_retention": map[string]any{"keep_newest": 7},
			"last_prune": map[string]any{"at": "2026-09-23T03:10:00Z", "removed": 2}},
		"recordErr": map[string]any{"last_prune_error": "open /snap/.prune: permission denied\nsecond line"},
		"failOne": map[string]any{"local_retention": map[string]any{"keep_newest": 7},
			"last_prune_failure": map[string]any{"at": "2026-09-23T03:10:00Z", "reason": "remove /snap/2026-09-01: busy"}},
		// Several causes, one per line; "; " inside a cause is part of it.
		"failMany": map[string]any{"last_prune_failure": map[string]any{"at": "2026-09-23T03:10:00Z",
			"reason": "remove /snap/a; b: busy\n\n  leftover /snap/.old still there  \ncannot read /snap: check read permissions\n"}},
		"failNoReason": map[string]any{"last_prune_failure": map[string]any{"at": "2026-09-23T03:10:00Z", "reason": ""}},
		// A failure after an older success: both said, the success first.
		"failAfterSuccess": map[string]any{"last_prune": map[string]any{"at": "2026-09-20T03:10:00Z", "removed": 4},
			"last_prune_failure": map[string]any{"at": "2026-09-23T03:10:00Z", "reason": "remove /snap/x: busy"}},
	}
	in, _ := json.Marshal(cases)
	dir := t.TempDir()
	path := filepath.Join(dir, "retention.js")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(node, path)
	cmd.Stdin = strings.NewReader(string(in))
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var colours struct {
		Failed []string `json:"refreshFailed"`
		OK     []string `json:"refreshOK"`
	}
	if err := json.Unmarshal(raw, &colours); err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	if len(colours.Failed) != 1 || colours.Failed[0] != "form-msg err" || len(colours.OK) != 1 || colours.OK[0] != "form-hint" {
		t.Errorf("refresh note colours: failed %v (want form-msg err), succeeded %v (want form-hint)", colours.Failed, colours.OK)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	const at = "2026-09-23 03:10:00 UTC"
	want := map[string][]line{
		"none":         {},
		"keepMany":     {{false, "Keeps the newest 7 snapshots on this machine."}},
		"keepOne":      {{false, "Keeps only the newest snapshot on this machine."}},
		"keepZero":     {},
		"pruneOnly":    {{false, "Removed 3 older copies on " + at + "."}},
		"pruneOne":     {{false, "Removed 1 older copy on " + at + "."}},
		"pruneNothing": {},
		"keepAndPrune": {{false, "Keeps the newest 7 snapshots on this machine. Removed 2 older copies on " + at + "."}},
		"recordErr":    {{true, "The record of older copies removed here could not be read: open /snap/.prune: permission denied"}},
		"failOne": {
			{false, "Keeps the newest 7 snapshots on this machine."},
			{true, "Removing older copies failed on " + at + ": | remove /snap/2026-09-01: busy"},
		},
		"failMany":     {{true, "Removing older copies failed on " + at + ": | remove /snap/a; b: busy | leftover /snap/.old still there | cannot read /snap: check read permissions"}},
		"failNoReason": {{true, "Removing older copies failed on " + at + ", with no reason recorded."}},
		"failAfterSuccess": {
			{false, "Removed 4 older copies on 2026-09-20 03:10:00 UTC."},
			{true, "Removing older copies failed on " + at + ": | remove /snap/x: busy"},
		},
	}
	for name, w := range want {
		var g []line
		if err := json.Unmarshal(got[name], &g); err != nil || g == nil {
			g = []line{}
		}
		if !reflect.DeepEqual(g, w) {
			t.Errorf("%s:\n got  %+v\n want %+v", name, g, w)
		}
	}
}
