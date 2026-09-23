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

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A prune attempt with two causes, produced by the REAL prune over real
// files, read back through GET /api/baselines and drawn by the page's own
// snapshotRetentionLines: one line per cause, exactly two. The two ends of
// the "\n" contract (baseline joins, the page splits) are tested here
// together, so neither side can change the separator alone.
func TestPruneFailureReason_rendersOneLinePerCause(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads and deletes through permissions")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(state, "solo")
	e, err := reg.Add(ServerEntry{Name: "solo", DSN: "u:p@tcp(h:3306)/solo", BaselineDir: dir, LocalKeepNewest: 1})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	old, newer := now.Add(-20*24*time.Hour), now.Add(-10*24*time.Hour)
	writeSnapshot(t, dir, old)
	writeSnapshot(t, dir, newer)
	// Cause 1: the older snapshot's schema folder cannot be read.
	unreadable := filepath.Join(dir, strings.ReplaceAll(old.Format(time.RFC3339), ":", "-"), "shop")
	// Cause 2: a leftover of an earlier removal that cannot be deleted.
	stuck := filepath.Join(dir, ".2026-01-01T00-00-00Z.pruning", "shop")
	if err := os.MkdirAll(stuck, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stuck, "t.parquet"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{unreadable, stuck} {
		if err := os.Chmod(d, 0o100); err != nil { // search only: no listing, no deleting inside
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_ = os.Chmod(unreadable, 0o700)
		_ = os.Chmod(stuck, 0o700)
	})
	if _, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: dir, KeepNewest: 1}); err != nil {
		t.Fatal(err)
	}

	srv := retentionServer(t, path, true)
	raw, body := retentionFields(t, srv, e.ID)
	var fail pruneFailureDTO
	if err := json.Unmarshal(raw["last_prune_failure"], &fail); err != nil {
		t.Fatalf("no last_prune_failure in %s: %v", body, err)
	}
	if n := len(strings.Split(fail.Reason, "\n")); n != 2 {
		t.Fatalf("the real reason has %d lines, want 2: %q", n, fail.Reason)
	}

	js := readAsset(t, "app.js")
	script := strings.Join([]string{
		functionBody(t, js, "function snapshotRetentionLines("),
		functionBody(t, js, "function utcLabel("),
		functionBody(t, js, "function firstLine("),
	}, "\n") + `
function el(tag, o, ...kids) {
  const n = { cls: (o && o.class) || "", text: (o && o.text) || "", kids: [] };
  n.append = (...k) => { for (const c of k) if (c) n.kids.push(c); };
  n.append(...kids);
  return n;
}
const b = JSON.parse(require("fs").readFileSync(0, "utf8"));
const causes = [];
for (const n of snapshotRetentionLines(b)) for (const k of n.kids) if (k.cls === "bk-retention-cause") causes.push(k.text);
console.log(JSON.stringify(causes));
`
	sp := filepath.Join(t.TempDir(), "render.js")
	if err := os.WriteFile(sp, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(node, sp)
	cmd.Stdin = strings.NewReader(body)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var causes []string
	if err := json.Unmarshal(out, &causes); err != nil {
		t.Fatalf("parse %q: %v", out, err)
	}
	if len(causes) != 2 {
		t.Fatalf("drawn %d cause lines, want 2: %q", len(causes), causes)
	}
	if !strings.Contains(causes[0], "removed earlier could not be deleted") || !strings.Contains(causes[1], "could not be read") || !strings.Contains(causes[1], unreadable) {
		t.Errorf("cause lines = %q", causes)
	}
}
