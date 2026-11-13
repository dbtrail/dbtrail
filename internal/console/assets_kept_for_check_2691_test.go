package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// The two snapshots a check compares are kept on top of the count in a
// folder without S3 (#2691): the list says so on their rows, and does not
// mark them as going at the next cleanup.
func TestSnapshotList_keptForTheCheck(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	var rows []map[string]any
	for i := 1; i <= 6; i++ {
		rows = append(rows, map[string]any{"time": now.Add(-time.Duration(5*i) * time.Minute).Format(consoleTSFormat), "age_hours": float64(5*i) / 60,
			"tables": []string{"shop.orders"}, "kinds": []string{"dir"}})
	}
	// Newest first: the 4th is the last full read, the 5th the one before it.
	rows[3]["kept_for_check"], rows[4]["kept_for_check"] = true, true
	// What the server answers past the newest two (#2675): all four are past
	// the count; the pair is not going, the other two are.
	for _, i := range []int{2, 3, 4, 5} {
		rows[i]["past_count"] = true
	}
	rows[2]["going"], rows[5]["going"] = true, true
	raw, _ := json.Marshal(rows)
	script := renderHarnessJS + `
vm.runInContext("capsCache = { backup_schedule: true };", ctx);
const cur = { id: "a", name: "a", kind: "registry", baseline_dir: "/var/lib/dbtrail/snapshots/a" };
const all = (n, out = []) => { if (!n || typeof n !== "object") return out; out.push(n); for (const c of n.children || []) all(c, out); return out; };
const nodes = all(vm.runInContext("baselinesPanel", ctx)({ configured: true, snapshots: ` + string(raw) + `, local_retention: { keep_newest: 2 } }, [cur], {}));
console.log(JSON.stringify({
  going: nodes.filter((n) => n.textContent === "goes at the next cleanup").length,
  kept: nodes.filter((n) => n.textContent === "kept for the check").map((n) => (n.attrs && n.attrs.title) || n.title || ""),
}));
`
	path := filepath.Join(t.TempDir(), "check.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got struct {
		Going int
		Kept  []string
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	t.Logf("kept for the check: %d rows, hover %q; going: %d", len(got.Kept), got.Kept, got.Going)
	const hover = "The read of the database most tables of the newest snapshot come from (usually the last full read), or the snapshot before it: the two a check of the snapshots compares for those tables. Kept on top of the newest kept, since S3 does not hold them, until another read takes its place. A table read on its own since then has no pair kept."
	if len(got.Kept) != 2 || got.Kept[0] != hover || got.Kept[1] != hover || got.Going != 2 {
		t.Fatalf("%d rows kept for the check (%q) and %d going, want 2 kept and the other 2 past the count going", len(got.Kept), got.Kept, got.Going)
	}
}
