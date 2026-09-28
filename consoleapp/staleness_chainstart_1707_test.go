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

// chainWatcher is a watcher over one server whose listing, floor and chain
// starts the test holds. bounds is keyed by schema.table; asked records the
// files the watcher wanted a chain start for, in order.
type chainWatcher struct {
	w      *stalenessWatcher
	files  []reconstruct.BaselineFile
	floor  status.DeltaFloor
	bounds map[string]status.ReadBound
	asked  []reconstruct.BaselineFile
}

func newChainWatcher(t *testing.T, n *watchNotifier) *chainWatcher {
	t.Helper()
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "wp", DSN: "d1", BaselineDir: "/b"})
	c := &chainWatcher{bounds: map[string]status.ReadBound{}}
	c.w = &stalenessWatcher{
		n: n, registry: reg, unknownEdge: notify.NewEdge(0),
		listBaselines: func(context.Context, string) ([]reconstruct.BaselineFile, int, error) { return c.files, 0, nil },
		oldestDelta:   func(context.Context, string) (status.DeltaFloor, error) { return c.floor, nil },
		readBounds: func(_ context.Context, files []reconstruct.BaselineFile) []status.ReadBound {
			out := make([]status.ReadBound, len(files))
			for i, f := range files {
				c.asked = append(c.asked, f)
				out[i] = c.bounds[f.Schema+"."+f.Table]
			}
			return out
		},
	}
	return c
}

const chainEdge = "staleness-attribution:d1\x1f/b"

// TestStalenessWatcher_gradesOnTheChainStart (#1707): a table whose snapshot
// folder is inside coverage and whose chain of deltas started below it is
// BROKEN, because a restore fetches events from the start of the chain. The
// table beside it, in the same folder and with no chain, is not.
func TestStalenessWatcher_gradesOnTheChainStart(t *testing.T) {
	now := time.Now().UTC()
	oldest := now.Add(-20 * time.Hour)
	n, f := testNotifier()
	c := newChainWatcher(t, n)
	c.floor = status.DeltaFloor{Hour: oldest}
	snap := now.Add(-time.Hour)
	c.files = []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: snap, DeltaUpserts: "orders.upserts.parquet"},
		{Schema: "shop", Table: "users", SnapshotTime: snap},
	}
	c.bounds["shop.orders"] = status.ReadBound{ChainStart: oldest.Add(-2 * time.Hour)}

	c.w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Event != "baseline_stale" || f.events[0].Resolved {
		t.Fatalf("a chain that started below coverage must alert: %+v", f.events)
	}
	if got := f.events[0].Details["tables"]; got != "shop.orders" {
		t.Fatalf("tables = %q, want only the table whose chain is past coverage", got)
	}

	// The chain is replaced by one that started inside coverage: resolved.
	c.bounds["shop.orders"] = status.ReadBound{ChainStart: snap.Add(-2 * time.Hour)}
	c.w.runCycle(context.Background())
	if len(f.events) != 2 || !f.events[1].Resolved {
		t.Fatalf("a chain inside coverage must resolve the alert: %+v", f.events)
	}
}

// TestStalenessWatcher_unreadChainNeitherFiresNorResolves (#1707): with the
// start of a chain unread, where a restore starts is not known. The target
// is skipped whole, like an unknown floor: no alert on a guess, and no
// resolve of an alert that is standing.
func TestStalenessWatcher_unreadChainNeitherFiresNorResolves(t *testing.T) {
	now := time.Now().UTC()
	oldest := now.Add(-20 * time.Hour)
	n, f := testNotifier()
	c := newChainWatcher(t, n)
	c.floor = status.DeltaFloor{Hour: oldest}
	snap := now.Add(-time.Hour)
	c.files = []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: snap, DeltaUpserts: "orders.upserts.parquet"},
		{Schema: "shop", Table: "legacy", SnapshotTime: oldest.Add(-time.Hour)},
	}
	c.bounds["shop.orders"] = status.ReadBound{ChainStart: snap.Add(-time.Hour)}
	c.w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Details["tables"] != "shop.legacy" {
		t.Fatalf("setup: want the alert standing on shop.legacy, got %+v", f.events)
	}

	// legacy gets a fresh snapshot and the chain of orders stops answering.
	// Graded on the folder, orders reads ok and the alert resolves.
	c.files = append(c.files, reconstruct.BaselineFile{Schema: "shop", Table: "legacy", SnapshotTime: snap})
	c.bounds["shop.orders"] = status.ReadBound{Unread: true}
	c.w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("an unread chain must neither fire nor resolve: %+v", f.events)
	}
	if !c.w.unknownEdge.Active(chainEdge) {
		t.Fatal("the cannot-evaluate condition must be latched")
	}

	// The chain answers again: the alert resolves and the latch is released.
	c.bounds["shop.orders"] = status.ReadBound{ChainStart: snap.Add(-time.Hour)}
	c.w.runCycle(context.Background())
	if len(f.events) != 2 || !f.events[1].Resolved {
		t.Fatalf("a readable chain inside coverage must resolve: %+v", f.events)
	}
	if c.w.unknownEdge.Active(chainEdge) {
		t.Fatal("the chain was read: the unknown edge must be resolved")
	}
}

// TestStalenessWatcher_unreadChainPastCoverageStillAlerts: a chain starts at
// or before the folder that holds it, so a FOLDER already below coverage is
// broken whatever the chain says. An unread chain must not mute that alert.
func TestStalenessWatcher_unreadChainPastCoverageStillAlerts(t *testing.T) {
	now := time.Now().UTC()
	oldest := now.Add(-20 * time.Hour)
	n, f := testNotifier()
	c := newChainWatcher(t, n)
	c.floor = status.DeltaFloor{Hour: oldest}
	c.files = []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: oldest.Add(-time.Hour), DeltaErr: context.DeadlineExceeded},
	}
	c.bounds["shop.orders"] = status.ReadBound{Unread: true}
	c.w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Details["tables"] != "shop.orders" {
		t.Fatalf("a folder past coverage must alert with its chain unread: %+v", f.events)
	}
}

// TestStalenessWatcher_readsTheChainOfEachNewestSnapshotOnly: the footer read
// is one request per table over S3, so the watcher asks for each table's
// newest snapshot and for nothing older.
func TestStalenessWatcher_readsTheChainOfEachNewestSnapshotOnly(t *testing.T) {
	now := time.Now().UTC()
	n, f := testNotifier()
	c := newChainWatcher(t, n)
	c.floor = status.DeltaFloor{Hour: now.Add(-20 * time.Hour)}
	older, newer := now.Add(-10*time.Hour), now.Add(-time.Hour)
	c.files = []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: older, DeltaUpserts: "old.parquet"},
		{Schema: "shop", Table: "orders", SnapshotTime: newer, DeltaUpserts: "new.parquet"},
		{Schema: "shop", Table: "users", SnapshotTime: older, DeltaUpserts: "users.parquet"},
	}
	c.w.runCycle(context.Background())
	if len(c.asked) != 2 || c.asked[0].DeltaUpserts != "new.parquet" || c.asked[1].DeltaUpserts != "users.parquet" {
		t.Fatalf("asked for %+v, want the newest file of each table", c.asked)
	}
	if len(f.events) != 0 {
		t.Fatalf("nothing is past coverage: %+v", f.events)
	}
}

// TestStalenessWatcher_noBoundReaderDoesNotGradeAChain: a watcher built with
// no reader of chain starts must not fall back on the folder's time for a
// table that has a chain. A table with none is graded as before.
func TestStalenessWatcher_noBoundReaderDoesNotGradeAChain(t *testing.T) {
	now := time.Now().UTC()
	oldest := now.Add(-20 * time.Hour)
	n, f := testNotifier()
	c := newChainWatcher(t, n)
	c.w.readBounds = nil
	c.floor = status.DeltaFloor{Hour: oldest}
	c.files = []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour), DeltaUpserts: "orders.upserts.parquet"},
	}
	c.w.runCycle(context.Background())
	if len(f.events) != 0 || !c.w.unknownEdge.Active(chainEdge) {
		t.Fatalf("a chain with no reader must latch cannot-evaluate: events %+v", f.events)
	}

	c.files = []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: oldest.Add(-time.Hour)}}
	c.w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Resolved {
		t.Fatalf("a table with no chain is graded on its folder: %+v", f.events)
	}
}

// TestStartStalenessWatch_readsChainStarts pins the wiring: the watcher the
// daemon starts reads chain starts. Left nil, every table with a chain would
// latch cannot-evaluate and no alert would ever fire for it.
func TestStartStalenessWatch_readsChainStarts(t *testing.T) {
	w := newStalenessWatcher(nil, testRegistryWithEntries(t), "", "", "")
	if w.readBounds == nil || w.listBaselines == nil || w.oldestDelta == nil {
		t.Fatalf("the watcher is missing a reader: %+v", w)
	}
}

// TestStalenessWatcher_unreadChainDoesNotMuteABrokenTable: what cannot be
// read for one table is no evidence about another. A table past coverage
// alerts although the chain of the table beside it is unread; only the
// resolve is withheld, because that one needs every table graded.
func TestStalenessWatcher_unreadChainDoesNotMuteABrokenTable(t *testing.T) {
	now := time.Now().UTC()
	oldest := now.Add(-20 * time.Hour)
	n, f := testNotifier()
	c := newChainWatcher(t, n)
	c.floor = status.DeltaFloor{Hour: oldest}
	snap := now.Add(-time.Hour)
	c.files = []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: snap, DeltaUpserts: "orders.upserts.parquet"},
		{Schema: "shop", Table: "audit", SnapshotTime: snap, DeltaErr: context.DeadlineExceeded},
	}
	c.bounds["shop.orders"] = status.ReadBound{ChainStart: oldest.Add(-2 * time.Hour)}
	c.bounds["shop.audit"] = status.ReadBound{Unread: true}

	c.w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Resolved || f.events[0].Details["tables"] != "shop.orders" {
		t.Fatalf("a broken table must alert beside an unread chain: %+v", f.events)
	}
	if !c.w.unknownEdge.Active(chainEdge) {
		t.Fatal("the unread chain must still be latched as cannot-evaluate")
	}

	// orders is repaired, audit is still unread: nothing resolves.
	c.bounds["shop.orders"] = status.ReadBound{ChainStart: snap.Add(-time.Hour)}
	c.w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("with a table ungraded the alert must not resolve: %+v", f.events)
	}

	// audit is readable again: now it resolves.
	c.bounds["shop.audit"] = status.ReadBound{ChainStart: snap.Add(-time.Hour)}
	c.w.runCycle(context.Background())
	if len(f.events) != 2 || !f.events[1].Resolved {
		t.Fatalf("every table graded and none broken must resolve: %+v", f.events)
	}
}
