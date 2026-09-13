package consoleapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// TestStalenessWatcher_unattributableFloorNeverResolves is the load-bearing
// half of #1219: on a multi-source index a snapshot older than the live
// window grades UNKNOWN, and an unknown verdict must never travel the
// broken=false path — that call RESOLVES. Without the skip, adding a second
// source to an index would silently clear a standing broken-baseline alert
// while the restore window is just as broken as before.
func TestStalenessWatcher_unattributableFloorNeverResolves(t *testing.T) {
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "wp", DSN: "d1", BaselineDir: "/b"})
	now := time.Now().UTC()
	oldest := now.Add(-100 * time.Hour)
	n, f := testNotifier()
	files := []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: oldest.Add(-time.Hour)}}
	floor := status.DeltaFloor{Hour: oldest}
	w := &stalenessWatcher{
		n: n, registry: reg, unknownEdge: notify.NewEdge(0),
		listBaselines: func(context.Context, string) ([]reconstruct.BaselineFile, int, error) { return files, 0, nil },
		oldestDelta:   func(context.Context, string) (status.DeltaFloor, error) { return floor, nil },
	}
	w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Resolved {
		t.Fatalf("setup: want the broken alert active, got %+v", f.events)
	}

	// A second source appears: the archive floor is no longer attributable.
	floor.BelowIsUnknown = true
	w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("an unattributable floor must neither resolve nor re-fire: %+v", f.events)
	}

	if !w.unknownEdge.Active("staleness-attribution:d1\x1f/b") {
		t.Fatal("the cannot-evaluate condition must be latched, or it re-warns every cycle")
	}

	// Attribution is restored and the baseline is still broken: the edge was
	// never resolved, so the standing alert must not re-fire either.
	floor.BelowIsUnknown = false
	w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("unchanged broken condition must not re-fire: %+v", f.events)
	}
	// ...and the cannot-evaluate latch must be RELEASED, or a later relapse
	// would be swallowed by the repeat window and never warned about again.
	if w.unknownEdge.Active("staleness-attribution:d1\x1f/b") {
		t.Fatal("attribution recovered: the unknown edge must be resolved")
	}
}

// TestStalenessWatcher_unattributableStillGradesInWindow: ambiguity only
// disarms grading BELOW the live floor. Snapshots inside the live window need
// no attribution (every source writes the same partitions), so a healthy
// multi-source index still gets a real verdict instead of a permanent
// cannot-evaluate.
func TestStalenessWatcher_unattributableStillGradesInWindow(t *testing.T) {
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "wp", DSN: "d1", BaselineDir: "/b"})
	now := time.Now().UTC()
	oldest := now.Add(-100 * time.Hour)
	n, f := testNotifier()
	files := []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour)}}
	w := &stalenessWatcher{
		n: n, registry: reg, unknownEdge: notify.NewEdge(0),
		listBaselines: func(context.Context, string) ([]reconstruct.BaselineFile, int, error) { return files, 0, nil },
		oldestDelta: func(context.Context, string) (status.DeltaFloor, error) {
			return status.DeltaFloor{Hour: oldest, BelowIsUnknown: true}, nil
		},
	}
	w.runCycle(context.Background())
	if len(f.events) != 0 {
		t.Fatalf("a fresh baseline on a multi-source index must grade clean: %+v", f.events)
	}
}

func TestWatchNotifier_BaselineStale(t *testing.T) {
	n, f := testNotifier()
	n.BaselineStale("wp", "dsn1", true, "shop.legacy", "2026-07-28T00:00:00Z")
	n.BaselineStale("wp", "dsn1", true, "shop.legacy", "2026-07-28T00:00:00Z")
	if len(f.events) != 1 || f.events[0].Event != "baseline_stale" || f.events[0].Severity != "critical" {
		t.Fatalf("want one critical baseline_stale, got %+v", f.events)
	}
	if f.events[0].Details["tables"] != "shop.legacy" || f.events[0].Details["coverage_floor"] != "2026-07-28T00:00:00Z" {
		t.Fatalf("details wrong: %+v", f.events[0].Details)
	}
	// The coverage floor advances every rotation cycle while the SAME tables
	// stay broken — the edge detail is the table list precisely so this does
	// not re-page hourly.
	n.BaselineStale("wp", "dsn1", true, "shop.legacy", "2026-07-28T01:00:00Z")
	if len(f.events) != 1 {
		t.Fatalf("advancing floor with unchanged broken tables must not re-fire: %+v", f.events)
	}
	// A NEW table joining the broken set IS a new condition: immediate re-fire.
	n.BaselineStale("wp", "dsn1", true, "shop.legacy, shop.orders", "2026-07-28T01:00:00Z")
	if len(f.events) != 2 {
		t.Fatalf("a new broken table must fire through the repeat window: %+v", f.events)
	}
	n.BaselineStale("wp", "dsn1", false, "", "")
	if len(f.events) != 3 || !f.events[2].Resolved {
		t.Fatalf("recovery must resolve once, got %+v", f.events)
	}
	n.BaselineStale("wp", "dsn1", false, "", "")
	if len(f.events) != 3 {
		t.Fatalf("healthy with no prior alert must stay silent, got %+v", f.events)
	}
}

// TestStalenessWatcher_targets pins the all-or-nothing baseline fallback and
// the skip of servers with no baseline anywhere.
func TestStalenessWatcher_targets(t *testing.T) {
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "own-dir", DSN: "d1", BaselineDir: "/own"},
		console.ServerEntry{Name: "own-s3", DSN: "d2", BaselineS3: "s3://own"},
		console.ServerEntry{Name: "inherits", DSN: "d3"},
	)
	w := &stalenessWatcher{registry: reg, bootDSN: "boot-dsn", globalDir: "/global"}
	got := w.targets()
	if len(got) != 4 {
		t.Fatalf("want boot + 3 entries, got %+v", got)
	}
	bySrc := map[string]string{}
	for _, tg := range got {
		bySrc[tg.name] = tg.source
	}
	if bySrc["cli index"] != "/global" || bySrc["own-dir"] != "/own" || bySrc["own-s3"] != "s3://own" || bySrc["inherits"] != "/global" {
		t.Fatalf("fallback wrong: %+v", bySrc)
	}

	// No global baseline: boot and the inheriting entry drop out.
	w = &stalenessWatcher{registry: reg, bootDSN: "boot-dsn"}
	if got := w.targets(); len(got) != 2 {
		t.Fatalf("without a global source only own-baseline entries remain, got %+v", got)
	}
}

func TestStalenessWatcher_runCycle(t *testing.T) {
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "wp", DSN: "d1", BaselineDir: "/b"})
	now := time.Now().UTC()
	oldest := now.Add(-100 * time.Hour)
	n, f := testNotifier()

	files := []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: oldest.Add(-time.Hour)}, // superseded, broken
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour)},    // newest, fine
	}
	w := &stalenessWatcher{
		n: n, registry: reg,
		unknownEdge:   notify.NewEdge(0),
		listBaselines: func(context.Context, string) ([]reconstruct.BaselineFile, int, error) { return files, 0, nil },
		oldestDelta:   func(context.Context, string) (status.DeltaFloor, error) { return status.DeltaFloor{Hour: oldest}, nil },
	}

	// Newest per table is fine: no event.
	w.runCycle(context.Background())
	if len(f.events) != 0 {
		t.Fatalf("healthy newest snapshot must not alert: %+v", f.events)
	}

	// The newest snapshots of two tables slide past coverage: ONE critical,
	// carrying the full sorted broken set.
	files = append(files,
		reconstruct.BaselineFile{Schema: "shop", Table: "legacy", SnapshotTime: oldest.Add(-2 * time.Hour)},
		reconstruct.BaselineFile{Schema: "shop", Table: "carts", SnapshotTime: oldest.Add(-3 * time.Hour)},
	)
	w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Event != "baseline_stale" || f.events[0].Severity != "critical" {
		t.Fatalf("broken newest snapshots must alert once: %+v", f.events)
	}
	if f.events[0].Details["tables"] != "shop.carts, shop.legacy" {
		t.Fatalf("detail must list ALL broken tables sorted: %+v", f.events[0].Details)
	}

	// Same broken set on the next cycle, floor advanced by rotation: silent.
	oldestAdvanced := oldest.Add(time.Hour)
	w.oldestDelta = func(context.Context, string) (status.DeltaFloor, error) {
		return status.DeltaFloor{Hour: oldestAdvanced}, nil
	}
	w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("unchanged broken set with an advanced floor must not re-fire: %+v", f.events)
	}

	// Unknown floor: the target is skipped whole — no fire, and crucially NO
	// resolve of the active alert.
	w.oldestDelta = func(context.Context, string) (status.DeltaFloor, error) {
		return status.DeltaFloor{}, errors.New("index down")
	}
	w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("unknown floor must neither fire nor resolve: %+v", f.events)
	}

	// Unreadable baseline source: same skip-whole rule.
	w.oldestDelta = func(context.Context, string) (status.DeltaFloor, error) {
		return status.DeltaFloor{Hour: oldestAdvanced}, nil
	}
	w.listBaselines = func(context.Context, string) ([]reconstruct.BaselineFile, int, error) {
		return nil, 0, errors.New("bucket gone")
	}
	w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("unreadable source must neither fire nor resolve: %+v", f.events)
	}

	// Fresh baselines land for both tables: the alert resolves once.
	files = append(files,
		reconstruct.BaselineFile{Schema: "shop", Table: "legacy", SnapshotTime: now.Add(-time.Minute)},
		reconstruct.BaselineFile{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-time.Minute)},
	)
	w.listBaselines = func(context.Context, string) ([]reconstruct.BaselineFile, int, error) { return files, 0, nil }
	w.runCycle(context.Background())
	if len(f.events) != 2 || !f.events[1].Resolved {
		t.Fatalf("fresh baseline must resolve the alert: %+v", f.events)
	}
}

// TestStalenessWatcher_agingNeverFires pins the PR's core alerting decision:
// aging is informational (a bootstrap artifact on young installs — see
// status.baselineAgingFraction) and must never reach the webhook channel.
func TestStalenessWatcher_agingNeverFires(t *testing.T) {
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "wp", DSN: "d1", BaselineDir: "/b"})
	now := time.Now().UTC()
	oldest := now.Add(-100 * time.Hour)
	n, f := testNotifier()
	// Inside coverage but past 80% of the span: verdict is aging, not broken.
	files := []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: oldest.Add(10 * time.Hour)}}
	w := &stalenessWatcher{
		n: n, registry: reg, unknownEdge: notify.NewEdge(0),
		listBaselines: func(context.Context, string) ([]reconstruct.BaselineFile, int, error) { return files, 0, nil },
		oldestDelta:   func(context.Context, string) (status.DeltaFloor, error) { return status.DeltaFloor{Hour: oldest}, nil },
	}
	w.runCycle(context.Background())
	if len(f.events) != 0 {
		t.Fatalf("aging must never fire the webhook: %+v", f.events)
	}
}

// TestStalenessWatcher_targetIsolation: a failing first target must not
// disarm checking for the rest — the skip is per-target, never per-cycle.
func TestStalenessWatcher_targetIsolation(t *testing.T) {
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "a", DSN: "d1", BaselineDir: "/a"},
		console.ServerEntry{Name: "b", DSN: "d2", BaselineDir: "/b"},
	)
	now := time.Now().UTC()
	oldest := now.Add(-100 * time.Hour)
	n, f := testNotifier()
	w := &stalenessWatcher{
		n: n, registry: reg, unknownEdge: notify.NewEdge(0),
		listBaselines: func(_ context.Context, source string) ([]reconstruct.BaselineFile, int, error) {
			if source == "/a" {
				return nil, 0, errors.New("bucket gone")
			}
			return []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: oldest.Add(-time.Hour)}}, 0, nil
		},
		oldestDelta: func(context.Context, string) (status.DeltaFloor, error) { return status.DeltaFloor{Hour: oldest}, nil },
	}
	w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Server != "b" || f.events[0].Severity != "critical" {
		t.Fatalf("second target must still be graded when the first errors: %+v", f.events)
	}
}

// TestStalenessWatcher_sameDSNDifferentSources: two targets on ONE index with
// different baseline locations are distinct conditions — the healthy one must
// never Resolve (falsely all-clear) the broken one's alert.
func TestStalenessWatcher_sameDSNDifferentSources(t *testing.T) {
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "a", DSN: "d1", BaselineDir: "/stale"},
		console.ServerEntry{Name: "b", DSN: "d1", BaselineDir: "/fresh"},
	)
	now := time.Now().UTC()
	oldest := now.Add(-100 * time.Hour)
	n, f := testNotifier()
	w := &stalenessWatcher{
		n: n, registry: reg, unknownEdge: notify.NewEdge(0),
		listBaselines: func(_ context.Context, source string) ([]reconstruct.BaselineFile, int, error) {
			ts := now.Add(-time.Hour)
			if source == "/stale" {
				ts = oldest.Add(-time.Hour)
			}
			return []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: ts}}, 0, nil
		},
		oldestDelta: func(context.Context, string) (status.DeltaFloor, error) { return status.DeltaFloor{Hour: oldest}, nil },
	}
	w.runCycle(context.Background())
	w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Resolved || f.events[0].Severity != "critical" {
		t.Fatalf("want exactly one standing critical (no cross-resolve, no flap): %+v", f.events)
	}
}

// TestStalenessWatcher_emptyListingKeepsAlert: baselines vanishing without
// replacement is NOT a recovery — the active alert must neither resolve nor
// re-fire while the source lists nothing.
func TestStalenessWatcher_emptyListingKeepsAlert(t *testing.T) {
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "wp", DSN: "d1", BaselineDir: "/b"})
	now := time.Now().UTC()
	oldest := now.Add(-100 * time.Hour)
	n, f := testNotifier()
	files := []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: oldest.Add(-time.Hour)}}
	w := &stalenessWatcher{
		n: n, registry: reg, unknownEdge: notify.NewEdge(0),
		listBaselines: func(context.Context, string) ([]reconstruct.BaselineFile, int, error) { return files, 0, nil },
		oldestDelta:   func(context.Context, string) (status.DeltaFloor, error) { return status.DeltaFloor{Hour: oldest}, nil },
	}
	w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("setup: want the broken alert active, got %+v", f.events)
	}
	files = nil
	w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("empty listing must neither fire nor resolve: %+v", f.events)
	}
	// Snapshots reappear, still broken: the edge stayed active, so no re-fire.
	files = []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: oldest.Add(-time.Hour)}}
	w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("unchanged broken condition must not re-fire after the gap: %+v", f.events)
	}
}

// TestStalenessWatcher_partialListingIsNotGraded: a location that answered in
// PART (#1601, a snapshot directory the walk could not open) is not evaluable,
// like an unreadable one: the newest snapshot may sit in the directory that
// would not open. So the readable subset neither FIRES baseline_stale (cry
// wolf on a healthy backup) nor RESOLVES an active alert (false all-clear).
func TestStalenessWatcher_partialListingIsNotGraded(t *testing.T) {
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "a", DSN: "d1", BaselineDir: "/part"})
	now := time.Now().UTC()
	oldest := now.Add(-100 * time.Hour)
	n, f := testNotifier()
	stale := []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: oldest.Add(-time.Hour)}}
	fresh := []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Minute)}}
	skipped := 1
	w := &stalenessWatcher{
		n: n, registry: reg, unknownEdge: notify.NewEdge(0),
		listBaselines: func(context.Context, string) ([]reconstruct.BaselineFile, int, error) { return stale, skipped, nil },
		oldestDelta:   func(context.Context, string) (status.DeltaFloor, error) { return status.DeltaFloor{Hour: oldest}, nil },
	}
	// Stale AND partial: no fire. Graded, this would be a critical alert.
	w.runCycle(context.Background())
	w.runCycle(context.Background())
	if len(f.events) != 0 {
		t.Fatalf("a partial listing was graded: %+v", f.events)
	}
	// Fully read and stale: the alert fires.
	skipped = 0
	w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Severity != "critical" {
		t.Fatalf("a full stale listing must fire: %+v", f.events)
	}
	// Fresh but partial: the alert must NOT resolve on a subset.
	stale, skipped = fresh, 1
	w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("a partial listing must not resolve an active alert: %+v", f.events)
	}
	// Fresh and fully read: resolves.
	skipped = 0
	w.runCycle(context.Background())
	if len(f.events) != 2 || !f.events[1].Resolved {
		t.Fatalf("a full fresh listing must resolve: %+v", f.events)
	}
}
