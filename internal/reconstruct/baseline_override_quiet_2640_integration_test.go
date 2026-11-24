//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A source that writes nothing after the read of some tables (#2640).
//
// The cut is made of indexed changes, so with nothing written after the read
// it stays before the read's coordinate, and the read was refused as ahead
// until the wait ran out. Capture's own saved position says how far it read:
// when it is past the read, and nothing indexed is past the time the run asks
// for, a run with a read folds every table to that position. One coordinate
// for every table, as before; the window of each table now reaches it, so a
// TRUNCATE of another table there still refuses that table.

// captured saves capture's checkpoint the way the stream does.
func captured(t *testing.T, db *sql.DB, mode, file string, pos uint64) {
	t.Helper()
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id)
		VALUES (1, ?, ?, ?, NOW(), 1)
		ON DUPLICATE KEY UPDATE mode = VALUES(mode), binlog_file = VALUES(binlog_file), binlog_position = VALUES(binlog_position)`, mode, file, pos)
}

// quietIndex is orders with one change, then a TRUNCATE, then the read of
// orders at binlog.000003:650, and nothing after it.
func quietIndex(t *testing.T) (db *sql.DB, dsn, root string, base time.Time, override map[string]string) {
	t.Helper()
	db, dsn, root, base = lateDDLIndex(t)
	addItems(t, db, root, base)
	change(t, db, "orders", 520, 560, base.Add(5*time.Second), "1", `{"id":1,"status":"changed"}`)
	truncateOf(t, db, "orders", 600, base.Add(10*time.Second))
	staged := stageTable(t, t.TempDir(), "orders", base.Add(15*time.Second), 650)
	return db, dsn, root, base, map[string]string{"shop.orders": staged}
}

func askReached(dsn, root string, at time.Time, override map[string]string) error {
	return reconstruct.BaselineOverridesReached(context.Background(), reconstruct.OverrideProbe{
		IndexDSN: dsn, BaselineSrc: root, OutputDir: root, At: at, Override: override,
	})
}

// The case: capture saved a position at the read, nothing was written after
// it. The check says reached, the fold publishes, and every table of the
// snapshot ends at that one position.
func TestBaselineOverride_quietSourceIsReachedByCapturesPosition_2640(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base, override := quietIndex(t)
	captured(t, db, "position", "binlog.000003", 650)
	at := base.Add(30 * time.Second)
	if err := askReached(dsn, root, at, override); err != nil {
		t.Fatalf("capture's position is at the read: %v", err)
	}
	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders", "shop.items"}, override: override}
	if failures, err := run.at(at); err != nil {
		t.Fatalf("the fold: %v (%+v)", err, failures)
	}
	for _, table := range []string{"orders", "items"} {
		p, ts := published(t, root, table, at.Add(time.Second))
		if ts.Equal(base) {
			t.Fatalf("%s: nothing was published", table)
		}
		m, err := baseline.ReadParquetMetadata(p)
		if err != nil {
			t.Fatal(err)
		}
		if m.BinlogFile != "binlog.000003" || m.BinlogPos != 650 {
			t.Fatalf("%s ends at %s:%d, want every table at the one position binlog.000003:650", table, m.BinlogFile, m.BinlogPos)
		}
	}
}

// Every case in which capture's position does not show the read was reached
// leaves the refusal as it was: ahead, wait.
func TestBaselineOverride_captureThatDoesNotShowTheReadStaysAhead_2640(t *testing.T) {
	for _, c := range []struct {
		name string
		save func(t *testing.T, db *sql.DB)
	}{
		{"capture saved a position before the read", func(t *testing.T, db *sql.DB) { captured(t, db, "position", "binlog.000003", 600) }},
		{"no position saved", func(t *testing.T, db *sql.DB) {}},
		// A GTID-mode checkpoint's file and position can sit inside a
		// transaction, so they do not say what was indexed.
		{"GTID mode", func(t *testing.T, db *sql.DB) { captured(t, db, "gtid", "binlog.000003", 900) }},
		{"a position in an earlier file", func(t *testing.T, db *sql.DB) { captured(t, db, "position", "binlog.000002", 9000) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			testutil.SkipIfNoMySQL(t)
			db, dsn, root, base, override := quietIndex(t)
			c.save(t, db)
			at := base.Add(30 * time.Second)
			if err := askReached(dsn, root, at, override); !errors.Is(err, reconstruct.ErrBaselineOverrideAhead) {
				t.Fatalf("err = %v, want ErrBaselineOverrideAhead", err)
			}
			run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders", "shop.items"}, override: override}
			if _, err := run.at(at); !errors.Is(err, reconstruct.ErrBaselineOverrideAhead) {
				t.Fatalf("the fold disagrees with the check: %v", err)
			}
		})
	}
}

// The source's clock runs ahead: a change made before the read is stamped
// after the run's target time. The cut lands at that change, before the read,
// and capture's position must not carry the run past it: that change would be
// in the read and missing from every other table. Ahead until the target
// passes the change's stamp; then reached.
func TestBaselineOverride_aSourceClockAheadIsNotCarriedPast_2640(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base, override := quietIndex(t)
	change(t, db, "items", 610, 640, base.Add(40*time.Second), "2", `{"id":2,"status":"stamped-ahead"}`)
	captured(t, db, "position", "binlog.000003", 650)
	at := base.Add(30 * time.Second)
	if err := askReached(dsn, root, at, override); !errors.Is(err, reconstruct.ErrBaselineOverrideAhead) {
		t.Fatalf("a change stamped after the target sits before the read: err = %v, want ErrBaselineOverrideAhead", err)
	}
	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders", "shop.items"}, override: override}
	if _, err := run.at(at); !errors.Is(err, reconstruct.ErrBaselineOverrideAhead) {
		t.Fatalf("the fold disagrees with the check: %v", err)
	}
	later := base.Add(45 * time.Second)
	if err := askReached(dsn, root, later, override); err != nil {
		t.Fatalf("the target past the change's stamp: %v", err)
	}
	if failures, err := run.at(later); err != nil {
		t.Fatalf("the fold: %v (%+v)", err, failures)
	}
}

// Another table truncated between the newest change and the read, and not
// read again: its window now reaches the read, so it is refused, and nothing
// is published. Without the window reaching it, that table would be
// published without the TRUNCATE beside a table read after it.
func TestBaselineOverride_anotherTableTruncatedThereIsRefused_2640(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base, override := quietIndex(t)
	truncateOf(t, db, "items", 620, base.Add(12*time.Second))
	captured(t, db, "position", "binlog.000003", 650)
	at := base.Add(30 * time.Second)
	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders", "shop.items"}, override: override}
	if _, err := run.at(at); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("err = %v, want items refused for its TRUNCATE", err)
	}
	if _, ts := published(t, root, "items", at.Add(time.Second)); !ts.Equal(base) {
		t.Fatalf("a snapshot was published at %s", ts)
	}
}

// The read table truncated again after the read, and nothing after that:
// capture's position is past the second TRUNCATE, so the window reaches it
// and the table is refused.
func TestBaselineOverride_truncatedAgainOnAQuietSourceIsRefused_2640(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base, override := quietIndex(t)
	truncateOf(t, db, "orders", 700, base.Add(20*time.Second))
	captured(t, db, "position", "binlog.000003", 700)
	at := base.Add(30 * time.Second)
	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders", "shop.items"}, override: override}
	if _, err := run.at(at); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("err = %v, want orders refused for its second TRUNCATE", err)
	}
}

// A run with no read of some tables keeps the cut it always had: capture's
// position does not move it.
func TestBaselineOverride_anOrdinaryRunKeepsItsCut_2640(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)
	change(t, db, "orders", 520, 560, base.Add(5*time.Second), "1", `{"id":1,"status":"changed"}`)
	captured(t, db, "position", "binlog.000003", 650)
	at := base.Add(30 * time.Second)
	run := rereadRun{dsn: dsn, root: root, tables: []string{"shop.orders"}}
	if failures, err := run.at(at); err != nil {
		t.Fatalf("the fold: %v (%+v)", err, failures)
	}
	p, _ := published(t, root, "orders", at.Add(time.Second))
	m, err := baseline.ReadParquetMetadata(p)
	if err != nil {
		t.Fatal(err)
	}
	if m.BinlogPos != 560 {
		t.Fatalf("an ordinary run ends at %s:%d, want the newest change's end 560", m.BinlogFile, m.BinlogPos)
	}
}
