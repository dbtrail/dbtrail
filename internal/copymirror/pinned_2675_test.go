package copymirror

import (
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

var t4 = snapshotdir.Name(time.Date(2026, 10, 1, 13, 0, 0, 0, time.UTC))

// #2675 point 2: a mirrored snapshot a SQL statement reads (PinSnapshot) is
// not removed past Keep while the statement runs; it goes in the first round
// after.
func TestSync_aPinnedMirroredSnapshotStays(t *testing.T) {
	src := newSource()
	cfg := testConfig(t, src)
	for _, n := range []string{t1, t2} {
		src.publish(n, map[string]string{"shop/orders.parquet": "orders at " + n})
		mustSync(t, cfg)
	}
	release := baseline.PinSnapshot(cfg.Dir, t1, "a SQL statement", 0)
	src.publish(t3, map[string]string{"shop/orders.parquet": "orders at three"})
	res := mustSync(t, cfg)
	if len(res.Removed) != 0 || !slices.Contains(visibleSnapshots(t, cfg.Dir), t1) {
		t.Fatalf("a snapshot a statement reads was removed: %+v, visible %v", res, visibleSnapshots(t, cfg.Dir))
	}
	release()
	src.publish(t4, map[string]string{"shop/orders.parquet": "orders at four"})
	res = mustSync(t, cfg)
	if !slices.Contains(res.Removed, t1) {
		t.Fatalf("released, it still stays: %+v", res)
	}
}
