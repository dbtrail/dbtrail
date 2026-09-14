package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestRunBaselineAnchored_unreadableFolderIsInconclusive (#1639): with a
// folder at or after the newest pair unreadable, the console's verify grades
// no pair. Every table in scope is inconclusive with the cause, and a table
// named in the filter that the schema snapshot does not know is an error, as
// on the normal path.
func TestRunBaselineAnchored_unreadableFolderIsInconclusive(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory read permissions")
	}
	root := t.TempDir()
	for _, ts := range []time.Time{
		time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 2, 6, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC),
	} {
		dir := filepath.Join(root, reconstruct.SnapshotDirName(ts), "shop")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "a.parquet"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	newest := filepath.Join(root, reconstruct.SnapshotDirName(time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)))
	if err := os.Chmod(newest, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(newest, 0o755) })

	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"shop.a": {Schema: "shop", Table: "a"},
		"shop.b": {Schema: "shop", Table: "b"},
	})
	for _, tc := range []struct {
		name   string
		tables []string
		want   map[string]string
	}{
		{"every table", nil, map[string]string{"shop.a": "inconclusive", "shop.b": "inconclusive"}},
		{"filter with an unknown table", []string{"shop.a", "shop.zz"}, map[string]string{"shop.a": "inconclusive", "shop.zz": "error"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newVerifySupervisor(context.Background(), nil, nil)
			s.jobs["s1"] = &verifyJob{status: console.VerifyStatus{State: "running"}, mode: console.VerifyModeBaselineAnchored}
			if err := s.runBaselineAnchored(console.VerifyRequest{ServerID: "s1", Tables: tc.tables}, root, nil, resolver, "idx", ""); err != nil {
				t.Fatalf("runBaselineAnchored: %v", err)
			}
			got := map[string]string{}
			for _, r := range s.jobs["s1"].status.Results {
				key := r.Schema + "." + r.Table
				got[key] = r.Status
				if r.Status == "inconclusive" && !strings.Contains(r.Reason, "could not be read") && !strings.Contains(r.Reason, reconstruct.ErrUnreadableSnapshot.Error()) {
					t.Errorf("%s: reason %q does not carry the cause", key, r.Reason)
				}
			}
			if len(got) != len(tc.want) {
				t.Fatalf("results = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("results = %v, want %v", got, tc.want)
				}
			}
		})
	}
}
