package consoleapp

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// #2022: the baseline_stale webhook grades the same tables as the console
// headline and `bintrail status`. A table later snapshots no longer carry
// (here mydumper_0, an older build's name for order.items) keeps its last
// copy forever, so grading it would page about a table that is gone.
func TestStalenessWatcher_retiredTableDoesNotAlert2022(t *testing.T) {
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "wp", DSN: "d1", BaselineDir: "/b"})
	now := time.Now().UTC()
	oldest := now.Add(-100 * time.Hour)
	old, fresh := oldest.Add(-time.Hour), now.Add(-time.Hour)
	files := []reconstruct.BaselineFile{
		{Schema: "demo", Table: "customers", SnapshotTime: fresh},
		{Schema: "demo", Table: "order.items", SnapshotTime: fresh},
		{Schema: "demo", Table: "customers", SnapshotTime: old},
		{Schema: "demo", Table: "mydumper_0", SnapshotTime: old},
	}
	n, f := testNotifier()
	w := &stalenessWatcher{
		n: n, registry: reg, unknownEdge: notify.NewEdge(0),
		listBaselines: func(context.Context, string) ([]reconstruct.BaselineFile, int, error) { return files, 0, nil },
		oldestDelta:   func(context.Context, string) (status.DeltaFloor, error) { return status.DeltaFloor{Hour: oldest}, nil },
	}
	w.runCycle(context.Background())
	if len(f.events) != 0 {
		t.Fatalf("a table no longer backed up must not page: %+v", f.events)
	}

	// An alert in force about the old name resolves once the rename shows
	// up, and the resolve names it rather than claiming a repair.
	files[0].SnapshotTime, files[1].SnapshotTime = now.Add(-3*time.Hour), now.Add(-3*time.Hour)
	files = files[2:]
	w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Resolved {
		t.Fatalf("setup: want the alert about demo.mydumper_0 in force, got %+v", f.events)
	}
	files = append(files,
		reconstruct.BaselineFile{Schema: "demo", Table: "customers", SnapshotTime: fresh},
		reconstruct.BaselineFile{Schema: "demo", Table: "order.items", SnapshotTime: fresh})
	w.runCycle(context.Background())
	if len(f.events) != 2 || !f.events[1].Resolved || f.events[1].Details["no_longer_graded"] != "demo.mydumper_0" {
		t.Fatalf("want a resolve naming demo.mydumper_0, got %+v", f.events)
	}
	f.events = nil

	// The guard is the rule, not a blanket: the same old copy alone in its
	// snapshot (a per-table baseline) still alerts.
	files[1].SnapshotTime = old.Add(-time.Hour)
	w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Details["tables"] != "demo.mydumper_0" {
		t.Fatalf("want one alert naming demo.mydumper_0, got %+v", f.events)
	}
}
