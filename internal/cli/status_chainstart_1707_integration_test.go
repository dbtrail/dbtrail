//go:build integration

package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunStatus_gradesOnTheChainStart (#1707) drives the real command over a
// real index. The snapshot folder sits on the oldest hour the index covers,
// so graded on the folder both tables are inside coverage. The chain of
// deltas beside orders started three hours before that: a restore of orders
// needs events the index does not have, and status has to say broken.
func TestRunStatus_gradesOnTheChainStart(t *testing.T) {
	ctx := context.Background()
	db, name := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	floor, err := status.OldestDeltaFromDB(ctx, db, name)
	if err != nil || floor.Hour.IsZero() || floor.BelowIsUnknown {
		t.Fatalf("floor = %+v, err = %v", floor, err)
	}
	saved := struct {
		dsn, format, baselineDir string
		failOnGap                bool
	}{stIndexDSN, stFormat, stBaselineDir, stFailOnGap}
	t.Cleanup(func() {
		stIndexDSN, stFormat, stBaselineDir, stFailOnGap = saved.dsn, saved.format, saved.baselineDir, saved.failOnGap
	})
	stIndexDSN = testutil.IntegrationDSN(name)
	stFailOnGap = false
	statusCmd.SetContext(ctx)

	root := t.TempDir()
	folder := floor.Hour.UTC()
	chainStart := folder.Add(-3 * time.Hour)
	snap := folder.Format("2006-01-02T15-04-05Z")
	chainFixtureTable(t, root, snap, "shop", "orders", 2, chainStart.Format(time.RFC3339))
	chainFixtureTable(t, root, snap, "shop", "users", -1, "")
	chainFixtureTable(t, root, snap, "shop", "nostart", 0, "")
	stBaselineDir = root

	stFormat = "json"
	var runErr error
	out := captureStdout(t, func() { runErr = runStatus(statusCmd, nil) })
	if runErr != nil {
		t.Fatalf("runStatus: %v", runErr)
	}
	var r struct {
		Baselines []struct {
			Table            string `json:"table"`
			Staleness        string `json:"staleness"`
			ReadsFrom        string `json:"reads_from"`
			ReadsFromUnknown bool   `json:"reads_from_unknown"`
		} `json:"baselines"`
		BaselineStaleness string `json:"baseline_staleness"`
	}
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		t.Fatalf("decode: %v\n%s", err, out)
	}
	if len(r.Baselines) != 3 {
		t.Fatalf("listed %d baselines, want 3:\n%s", len(r.Baselines), out)
	}
	for _, b := range r.Baselines {
		switch b.Table {
		case "orders":
			if b.Staleness != "broken" || b.ReadsFrom != chainStart.Format(status.TSFmt) || b.ReadsFromUnknown {
				t.Errorf("orders: %+v, want broken and read from %s", b, chainStart.Format(status.TSFmt))
			}
		case "users":
			if b.Staleness == "broken" || b.Staleness == "unknown" || b.Staleness == "" || b.ReadsFrom != "" || b.ReadsFromUnknown {
				t.Errorf("users has no chain and sits inside coverage: %+v", b)
			}
		case "nostart":
			if b.Staleness != "unknown" || !b.ReadsFromUnknown || b.ReadsFrom != "" {
				t.Errorf("nostart: %+v, want unknown with the start unread", b)
			}
		default:
			t.Errorf("unexpected table %q", b.Table)
		}
	}
	if r.BaselineStaleness != "broken" {
		t.Errorf("baseline_staleness = %q, want broken", r.BaselineStaleness)
	}

	stFormat = "text"
	text := captureStdout(t, func() { runErr = runStatus(statusCmd, nil) })
	if runErr != nil {
		t.Fatalf("runStatus (text): %v", runErr)
	}
	if !strings.Contains(text, chainStart.Format(status.TSFmt)) || !strings.Contains(text, "⚠ broken") ||
		!strings.Contains(text, "unreadable") || !strings.Contains(text, "BASELINE STALE") {
		t.Errorf("text report:\n%s", text)
	}
}
