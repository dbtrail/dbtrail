//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// BaselineOverridesReached is what the daemon waits on (#2640). It has to say
// what the fold itself would say, on the same index.
func TestBaselineOverridesReached_answersWhatTheFoldAnswers(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)
	truncateOf(t, db, "orders", 600, base.Add(10*time.Second))
	change(t, db, "orders", 800, 900, base.Add(12*time.Second), "8", `{"id":8,"status":"stale"}`)
	staged := stageTable(t, t.TempDir(), "orders", base.Add(15*time.Second), 5000)
	override := map[string]string{"shop.orders": staged}
	ask := func(at time.Time) error {
		return reconstruct.BaselineOverridesReached(context.Background(), reconstruct.FullTableConfig{
			IndexDSN: dsn, BaselineSrc: root, OutputDir: root, At: at, BaselineOverride: override,
		})
	}

	if err := ask(base.Add(30 * time.Second)); !errors.Is(err, reconstruct.ErrBaselineOverrideAhead) {
		t.Fatalf("capture behind the read: %v, want ErrBaselineOverrideAhead", err)
	}
	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders"}, override: override}
	if _, err := run.at(base.Add(30 * time.Second)); !errors.Is(err, reconstruct.ErrBaselineOverrideAhead) {
		t.Fatalf("the fold disagrees with the check: %v", err)
	}
	if err := ask(base.Add(14 * time.Second)); !errors.Is(err, reconstruct.ErrBaselineOverrideAhead) {
		t.Fatalf("an instant before the read: %v, want ErrBaselineOverrideAhead", err)
	}

	change(t, db, "orders", 5000, 5100, base.Add(35*time.Second), "7", `{"id":7,"status":"late"}`)
	if err := ask(base.Add(45 * time.Second)); err != nil {
		t.Fatalf("capture past the read: %v", err)
	}
	if _, err := run.at(base.Add(45 * time.Second)); err != nil {
		t.Fatalf("the fold disagrees with the check: %v", err)
	}
}

// Not a usable file: the check says so, and it is not "wait".
func TestBaselineOverridesReached_aFileThatCannotBeUsedIsNotAWait(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	_, dsn, root, base := lateDDLIndex(t)
	err := reconstruct.BaselineOverridesReached(context.Background(), reconstruct.FullTableConfig{
		IndexDSN: dsn, BaselineSrc: root, OutputDir: root, At: base.Add(time.Minute),
		BaselineOverride: map[string]string{"shop.orders": filepath.Join(t.TempDir(), "x", "shop", "orders.parquet")},
	})
	if !errors.Is(err, reconstruct.ErrBaselineOverride) || errors.Is(err, reconstruct.ErrBaselineOverrideAhead) {
		t.Fatalf("err = %v, want ErrBaselineOverride", err)
	}
}

// The binary log started again (RESET MASTER, a failover) and the read was
// taken in the new numbering. The check cannot order the two numberings and
// may say "reached"; the fold of the table that still starts from the old
// numbering refuses, and nothing is published.
func TestBaselineOverride_aRenumberedLogStillRefuses(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)
	addItems(t, db, root, base)
	insert := func(start uint64, at time.Time, table, pk, after string) {
		testutil.InsertEvent(t, db, "binlog.000001", start, start+50, at.Format("2006-01-02 15:04:05"), nil,
			"shop", table, 2, pk, nil, nil, []byte(after))
	}
	insert(100, base.Add(10*time.Second), "orders", "9", `{"id":9,"status":"new"}`)

	readAt := base.Add(15 * time.Second)
	dir := filepath.Join(t.TempDir(), reconstruct.SnapshotDirName(readAt))
	staged := filepath.Join(dir, "shop", "orders.parquet")
	writeOrdersShapedIn(t, staged, readAt, "binlog.000001", 150, [][]string{{"9", "new"}})
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatal(err)
	}
	insert(200, base.Add(20*time.Second), "items", "2", `{"id":2,"status":"X"}`)

	at := base.Add(30 * time.Second)
	override := map[string]string{"shop.orders": staged}
	if err := reconstruct.BaselineOverridesReached(context.Background(), reconstruct.FullTableConfig{
		IndexDSN: dsn, BaselineSrc: root, OutputDir: root, At: at, BaselineOverride: override,
	}); err != nil {
		t.Fatalf("the read's own numbering has reached it: %v", err)
	}
	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders", "shop.items"}, override: override}
	failures, err := run.at(at)
	if !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		t.Fatalf("err = %v (failures %+v), want ErrBinlogRenumbered", err, failures)
	}
	if _, ts := published(t, root, "items", at.Add(time.Second)); !ts.Equal(base) {
		t.Fatalf("a snapshot was published at %s across a renumbered log", ts)
	}
}

// A second TRUNCATE after the read: the fold from the read refuses it.
func TestBaselineOverride_truncatedAgainAfterTheReadIsRefused(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)
	truncateOf(t, db, "orders", 600, base.Add(10*time.Second))
	staged := stageTable(t, t.TempDir(), "orders", base.Add(15*time.Second), 700)
	truncateOf(t, db, "orders", 800, base.Add(20*time.Second))
	change(t, db, "orders", 900, 950, base.Add(25*time.Second), "1", `{"id":1,"status":"after"}`)

	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders"}, override: map[string]string{"shop.orders": staged}}
	at := base.Add(30 * time.Second)
	if _, err := run.at(at); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL", err)
	}
	if _, ts := published(t, root, "orders", at.Add(time.Second)); !ts.Equal(base) {
		t.Fatalf("a snapshot was published at %s over the second TRUNCATE", ts)
	}
}
