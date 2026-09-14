//go:build integration

package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunStatus_unreadableNewestBaselineIsNotGraded (#1639) drives the real
// command: with the newest baseline folder unreadable, `status --format json`
// reports baseline_staleness "unknown" and grades nothing, instead of grading
// the older folder as if it were the newest. With only an OLDER folder
// unreadable the readable one is listed and graded as before.
func TestRunStatus_unreadableNewestBaselineIsNotGraded(t *testing.T) {
	if os.Geteuid() == 0 {
		if os.Getenv("CI") != "" {
			t.Fatal("running as root under CI: the mode-000 fixture is a no-op and this coverage would silently vanish")
		}
		t.Skip("root bypasses directory read permissions")
	}
	ctx := context.Background()
	db, name := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	saved := struct {
		dsn, format, baselineDir string
		failOnGap                bool
	}{stIndexDSN, stFormat, stBaselineDir, stFailOnGap}
	t.Cleanup(func() {
		stIndexDSN, stFormat, stBaselineDir, stFailOnGap = saved.dsn, saved.format, saved.baselineDir, saved.failOnGap
	})
	stIndexDSN = testutil.IntegrationDSN(name)
	stFormat = "json"
	stFailOnGap = false
	statusCmd.SetContext(ctx)

	mk := func(root, ts string) string {
		dir := filepath.Join(root, ts)
		if err := os.MkdirAll(filepath.Join(dir, "shop"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "shop", "a.parquet"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	lock := func(dir string) {
		if err := os.Chmod(dir, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	}
	type report struct {
		Baselines         []json.RawMessage `json:"baselines"`
		BaselineStaleness string            `json:"baseline_staleness"`
	}
	run := func(root string) report {
		t.Helper()
		stBaselineDir = root
		var err error
		out := captureStdout(t, func() { err = runStatus(statusCmd, nil) })
		if err != nil {
			t.Fatalf("runStatus: %v", err)
		}
		var r report
		if err := json.Unmarshal([]byte(out), &r); err != nil {
			t.Fatalf("decode: %v\n%s", err, out)
		}
		return r
	}

	newer := t.TempDir()
	mk(newer, "2026-09-01T06-00-00Z")
	lock(mk(newer, "2026-09-02T06-00-00Z"))
	if r := run(newer); r.BaselineStaleness != "unknown" || len(r.Baselines) != 0 {
		t.Errorf("newest unreadable: staleness=%q baselines=%d; want unknown and none graded", r.BaselineStaleness, len(r.Baselines))
	}

	older := t.TempDir()
	lock(mk(older, "2026-09-01T06-00-00Z"))
	mk(older, "2026-09-02T06-00-00Z")
	if r := run(older); r.BaselineStaleness == "" || r.BaselineStaleness == "unknown" && len(r.Baselines) == 0 || len(r.Baselines) != 1 {
		t.Errorf("older unreadable: staleness=%q baselines=%d; want the readable snapshot listed and graded", r.BaselineStaleness, len(r.Baselines))
	}
}
