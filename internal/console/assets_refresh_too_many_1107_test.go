package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestBaselineRefreshNote_tooManyChanges (#1107): an automatic refresh
// refused for too many changed rows starts again from the same backup over a
// longer window, so "the next run retries" would be false. The line says to
// take a full backup instead, and every other refusal keeps its wording.
func TestBaselineRefreshNote_tooManyChanges(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	js := readAsset(t, "app.js")
	script := functionBody(t, js, "function baselineRefreshNote(") + "\n" +
		functionBody(t, js, "function backupFoldError(") + "\n" + `
const el = (tag, o) => o;
const utcLabel = (s) => s;
const reusedCopiedNote = () => "";
const err = "shop.a: too many changed rows for an update from the recorded changes: more than 1,000,000 distinct rows changed between the backup this starts from and the target moment, more than one table may hold in memory while 2 tables are processed at once";
console.log(JSON.stringify({
  budget: baselineRefreshNote({ state: "failed", refused: 1, last_error: err, too_many_changes: true }).text,
  gap: baselineRefreshNote({ state: "failed", refused: 1, last_error: "shop.a: capture gap" }).text,
}));
`
	path := filepath.Join(t.TempDir(), "refresh.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got struct{ Budget, Gap string }
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	t.Logf("budget: %s", got.Budget)
	if strings.Contains(got.Budget, "retries") || !strings.HasSuffix(got.Budget, "would be refused again: take a full backup.") {
		t.Errorf("budget refusal: %q", got.Budget)
	}
	if !strings.HasSuffix(got.Gap, "Nothing was overwritten; the next run retries.") {
		t.Errorf("other refusal changed wording: %q", got.Gap)
	}
	for _, s := range []string{got.Budget, got.Gap} {
		if strings.Contains(s, "—") || strings.Contains(s, "..") {
			t.Errorf("not clean copy: %q", s)
		}
	}
}
