//go:build integration

package metadata_test

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationLoadSnapshotEpochs_anchoredOnTheDDL is #1667: a snapshot taken
// for a DDL is in effect from when the DDL ran on the source
// (schema_changes.detected_at), not from when capture got to it. Epochs are
// compared against event timestamps and restore targets, which are source
// instants; with capture behind, the processing time put the change minutes or
// hours late. Not when another recorded DDL ran in between: the snapshot read
// the live schema after it.
func TestIntegrationLoadSnapshotEpochs_anchoredOnTheDDL(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	t0 := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	stamp := func(d time.Duration) string { return t0.Add(d).Format("2006-01-02 15:04:05") }

	// 1: the first snapshot. 2: taken 100s after a DDL that ran at 20s.
	// 3: taken at 5m with no DDL (a restart or a manual snapshot).
	// 4: taken at 6m for a DDL that ran at 2m, during the catch-up after 3.
	// 5: taken at 30m for a DDL that ran at 10m, after another DDL ran at 15m:
	// it already holds that one, so it counts from when it was taken.
	for _, s := range []struct {
		id    int
		taken time.Duration
	}{{1, -2 * time.Hour}, {2, 100 * time.Second}, {3, 5 * time.Minute}, {4, 6 * time.Minute}, {5, 30 * time.Minute}} {
		testutil.InsertSnapshot(t, db, s.id, stamp(s.taken), "shop", "t", "id", 1, "PRI", "int", "NO")
	}
	for _, c := range []struct {
		detected   time.Duration
		kind       string
		snapshotID any
	}{
		{20 * time.Second, "ALTER TABLE", 2},
		// The same snapshot recorded for a second table of one statement.
		{50 * time.Second, "RENAME TABLE", 2},
		{2 * time.Minute, "ALTER TABLE", 4},
		{10 * time.Minute, "ALTER TABLE", 5},
		// Its own snapshot failed: still a DDL the later snapshot holds.
		{15 * time.Minute, "ALTER TABLE", nil},
		// No snapshot (a TRUNCATE, or a failed one): anchors nothing.
		{-time.Hour, "TRUNCATE TABLE", nil},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
			VALUES (?, 'binlog.000001', 4, 'shop', 't', ?, 'x', ?)`, stamp(c.detected), c.kind, c.snapshotID)
	}

	check := func(t *testing.T, want []metadata.SnapshotEpoch) []metadata.SnapshotEpoch {
		t.Helper()
		got, err := metadata.LoadSnapshotEpochs(db)
		if err != nil {
			t.Fatalf("LoadSnapshotEpochs: %v", err)
		}
		ok := len(got) == len(want)
		for i := 0; ok && i < len(got); i++ {
			ok = got[i].ID == want[i].ID && got[i].At.Equal(want[i].At) && got[i].Taken.Equal(want[i].Taken)
		}
		if !ok {
			t.Fatalf("LoadSnapshotEpochs = %v, want %v", got, want)
		}
		return got
	}

	taken := map[int]time.Time{1: t0.Add(-2 * time.Hour), 2: t0.Add(100 * time.Second),
		3: t0.Add(5 * time.Minute), 4: t0.Add(6 * time.Minute), 5: t0.Add(30 * time.Minute)}
	epoch := func(id int, at time.Time) metadata.SnapshotEpoch {
		return metadata.SnapshotEpoch{ID: id, At: at, Taken: taken[id]}
	}
	t.Run("a snapshot taken for a recorded DDL is in effect from the DDL, ordered by that instant", func(t *testing.T) {
		epochs := check(t, []metadata.SnapshotEpoch{
			epoch(1, taken[1]),
			epoch(2, t0.Add(20*time.Second)),
			epoch(4, t0.Add(2*time.Minute)),
			epoch(3, taken[3]),
			epoch(5, taken[5]),
		})
		for _, c := range []struct {
			at   time.Duration
			want int
		}{{time.Minute, 2}, {3 * time.Minute, 4}, {5 * time.Minute, 3}, {20 * time.Minute, 3}, {30 * time.Minute, 5}} {
			if id, _ := metadata.EpochAt(epochs, t0.Add(c.at)); id != c.want {
				t.Errorf("EpochAt(+%s) = %d, want %d", c.at, id, c.want)
			}
		}
	})

	t.Run("an index without schema_changes keeps when each snapshot was taken", func(t *testing.T) {
		testutil.MustExec(t, db, "DROP TABLE schema_changes")
		check(t, []metadata.SnapshotEpoch{
			epoch(1, taken[1]), epoch(2, taken[2]), epoch(3, taken[3]), epoch(4, taken[4]), epoch(5, taken[5]),
		})
	})
}
