package views

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestNewestFollow_readsAV0830Pair (#2242): a pair written by v0.83.0 has no
// sequence number. A view that follows the newest snapshot looked for the
// table's changes by the numbered names only, found none, and returned the
// table's file alone: the table as it was before the pair, with no error.
//
// And what comes after: the next refresh rewrites such a table in full, with
// no pair beside it. The same views file, read again, must not show that
// snapshot through a body built for the pair without saying so: it stops
// with an error until the views are generated again.
func TestNewestFollow_readsAV0830Pair(t *testing.T) {
	root := t.TempDir()
	base := writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "a", "b", "c")
	writeLegacyDeltaPair(t, base, []int64{0}, [][2]string{{"1", "changed"}})
	tables := []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet", SchemaKnown: true}}
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatal(err)
	}
	if !tables[0].DeltaLegacy {
		t.Fatalf("the fixture's pair is not read as a v0.83.0 pair: %+v", tables[0])
	}
	sqlText := Generate(Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow: FollowNewest, Baselines: tables,
	})
	state := func() (string, error) {
		db, err := loadViews(t, sqlText)
		if err != nil {
			return "", err
		}
		var got string
		err = db.QueryRow(`SELECT string_agg(status, ',' ORDER BY id) FROM shop.orders`).Scan(&got)
		return got, err
	}
	if got, err := state(); err != nil || got != "changed,b,c" {
		t.Fatalf("state = %q (err=%v), want the pair applied: changed,b,c", got, err)
	}
	// The next refresh: the table written in full, no pair.
	writeSnapshot(t, root, "2026-05-01T03-00-00Z", true, "x", "y", "z")
	got, err := state()
	if err == nil {
		t.Fatalf("after a refresh that rewrote the table, the same file answered %q with no error", got)
	}
	if !strings.Contains(err.Error(), "orders.posdel") {
		t.Fatalf("the refusal does not name the pair it looked for: %v", err)
	}
}
