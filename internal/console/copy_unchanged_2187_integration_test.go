//go:build integration

package console

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2187: the routed port's "unchanged since the snapshot" check reads the
// live index, and a change indexed late lands in an old hour, the next one
// rotation archives and drops. archive_state records the newest position each
// archive holds (#2152); the check reads it, by the rule a fetch with no cut
// reads it, and an archive that may hold a change after a table's position
// sends the statement to MySQL.

const archiveWord = "archive"

// rotateBelow archives and drops, with the real rotation, every partition of
// r's index whose hour is before keepFrom.
func (r *unchangedRig) rotateBelow(keepFrom time.Time) {
	r.t.Helper()
	cutoff := keepFrom.Add(-30 * time.Minute)
	if _, err := rotation.Perform(context.Background(), r.db, r.dbName(), rotation.Options{
		RetainDur: time.Since(cutoff), RetainRaw: "test", ArchiveDir: r.t.TempDir(),
		BintrailID: "21870000-0000-0000-0000-000000000000", ArchiveCompression: "zstd", Format: "json", NoReplace: true,
	}); err != nil {
		r.t.Fatalf("rotation.Perform: %v", err)
	}
	var oldest string
	if err := r.db.QueryRow(`SELECT PARTITION_NAME FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events' ORDER BY PARTITION_ORDINAL_POSITION LIMIT 1`).Scan(&oldest); err != nil {
		r.t.Fatal(err)
	}
	if oldest != indexer.PartitionName(keepFrom) {
		r.t.Fatalf("fixture: the oldest live partition is %s, want %s", oldest, indexer.PartitionName(keepFrom))
	}
}

func (r *unchangedRig) dbName() string { return r.b.dbName }

func TestIntegrationCopyUnchanged_lateChangeArchivedAndDropped_2187(t *testing.T) {
	r := newUnchangedRig(t)
	h0 := r.stamp.Truncate(time.Hour)
	floor := copyTimeFloor(r.stamp) // h0 - 1h: where the lookup by position starts
	late, quiet := r.table(nil), r.table(nil)

	// Routine history before the snapshot's position, in the hours rotation
	// will take.
	r.event("shop", "busy", "binlog.000006", 100, h0.Add(-8*time.Hour+5*time.Minute))
	r.event("shop", "busy", "binlog.000006", 200, h0.Add(-7*time.Hour+5*time.Minute))
	// The late change: positioned after the snapshot, dated hours before it.
	r.event("shop", late.Table, "binlog.000008", 50, h0.Add(-6*time.Hour+10*time.Minute))
	// While its hour is live, the check's own search below the floor finds it.
	r.wantNot("shop."+late.Table+" changed since its snapshot", late)
	r.wantUnchanged(quiet)
	r.s.copyChanged.seen = nil // a restarted daemon remembers nothing

	// Rotation archives and drops the three oldest hours. The window the
	// lookups need (from the floor on) stays live, so the "rotated out"
	// refusal does not fire: without the archives, the check says unchanged.
	r.rotateBelow(h0.Add(-5 * time.Hour))
	if !h0.Add(-5 * time.Hour).Before(floor) {
		t.Fatal("fixture: the oldest live hour must be below the floor")
	}
	var maxFile sql.NullString
	if err := r.db.QueryRow(`SELECT max_binlog_file FROM archive_state WHERE partition_name = ?`,
		indexer.PartitionName(h0.Add(-6*time.Hour))).Scan(&maxFile); err != nil || maxFile.String != "binlog.000008" {
		t.Fatalf("fixture: archive_state records %v (err %v), want the late change's binlog.000008", maxFile, err)
	}

	r.wantNot(archiveWord, late)
	// Archives are not split by table: another table's late change sends this
	// one to MySQL too.
	r.wantNot(archiveWord, quiet)
	// Remembered, like a change found in the index.
	if why := r.s.copyChanged.get(bootServerID, late, r.anchor); !strings.Contains(why, archiveWord) {
		t.Fatalf("remembered %q, want the archive verdict", why)
	}
}

func TestIntegrationCopyUnchanged_routineArchivesAndShapes_2187(t *testing.T) {
	r := newUnchangedRig(t)
	h0 := r.stamp.Truncate(time.Hour)
	tb := r.table(nil)

	// Steady state: rotation archives hours whose changes are all before the
	// snapshot's position. Those archives are written NOW, long after the
	// snapshot, and still do not count.
	for i := 8; i >= 6; i-- {
		r.event("shop", "busy", "binlog.000006", uint64(1000-i), h0.Add(-time.Duration(i)*time.Hour+5*time.Minute))
	}
	r.rotateBelow(h0.Add(-5 * time.Hour))
	r.wantUnchanged(tb)

	label := indexer.PartitionName(h0.Add(-20 * time.Hour)) // an hour below the floor
	add := func(cols, vals string, args ...any) {
		t.Helper()
		testutil.MustExec(t, r.db, "INSERT INTO archive_state (partition_name, bintrail_id, local_path, row_count"+cols+") VALUES (?, 'shape', '/a/x.parquet', 1"+vals+")",
			append([]any{label}, args...)...)
	}
	reset := func() {
		t.Helper()
		testutil.MustExec(t, r.db, "DELETE FROM archive_state WHERE bintrail_id = 'shape'")
		r.s.copyChanged.seen = nil
	}
	for _, tc := range []struct {
		name, cols, vals string
		args             []any
		changed          bool
	}{
		{"not recorded, written long before the snapshot", ", archived_at", ", NOW() - INTERVAL 80 HOUR", nil, false},
		{"not recorded, written after the snapshot", "", "", nil, true},
		{"recorded at the position, written after the snapshot", ", max_event_id, max_binlog_file, max_start_pos", ", 9, 'binlog.000007', 4200", nil, true},
		{"recorded before the position, written after the snapshot", ", max_event_id, max_binlog_file, max_start_pos", ", 9, 'binlog.000007', 4199", nil, false},
		{"recorded with no coordinate", ", max_event_id", ", 9", nil, false},
		{"another binlog base name, written before the snapshot", ", max_event_id, max_binlog_file, max_start_pos, archived_at", ", 9, 'mysql-bin.000001', 4, NOW() - INTERVAL 80 HOUR", nil, false},
		{"another binlog base name, written after the snapshot", ", max_event_id, max_binlog_file, max_start_pos", ", 9, 'a.000001', 4", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset()
			add(tc.cols, tc.vals, tc.args...)
			if tc.changed {
				r.wantNot(archiveWord, tb)
			} else {
				r.wantUnchanged(tb)
			}
		})
	}
	reset()

	t.Run("no archive_state: cannot say", func(t *testing.T) {
		testutil.MustExec(t, r.db, "RENAME TABLE archive_state TO archive_state_gone")
		defer testutil.MustExec(t, r.db, "RENAME TABLE archive_state_gone TO archive_state")
		r.wantNot(archiveWord, tb)
	})
	t.Run("an archive_state that cannot be read: cannot say", func(t *testing.T) {
		testutil.MustExec(t, r.db, "RENAME TABLE archive_state TO archive_state_gone")
		testutil.MustExec(t, r.db, "CREATE TABLE archive_state (id INT PRIMARY KEY)")
		defer testutil.MustExec(t, r.db, "RENAME TABLE archive_state_gone TO archive_state")
		defer testutil.MustExec(t, r.db, "DROP TABLE archive_state")
		r.wantNot("could not be read", tb)
	})
	r.wantUnchanged(tb)
}

// TestIntegrationCopyUnchanged_archiveCost_2187 measures the whole check on
// an index with a year of hourly archives (8,760 archive_state rows, written
// one an hour up to now, every change before the snapshot), against the same
// check with none. The answer must stay "unchanged".
func TestIntegrationCopyUnchanged_archiveCost_2187(t *testing.T) {
	r := newUnchangedRig(t)
	tb := r.table(nil)
	measure := func(what string) float64 {
		t.Helper()
		var ms []float64
		for range 21 {
			start := time.Now()
			r.wantUnchanged(tb)
			ms = append(ms, float64(time.Since(start).Microseconds())/1000)
		}
		sort.Float64s(ms)
		t.Logf("whole check, %s: median %.2f ms, worst %.2f ms", what, ms[len(ms)/2], ms[len(ms)-1])
		return ms[len(ms)/2]
	}
	none := measure("no archive_state rows")

	const year = 365 * 24
	first := r.stamp.Truncate(time.Hour).Add(-(year + 24) * time.Hour)
	for lo := 0; lo < year; lo += 1000 {
		q, args := `INSERT INTO archive_state (partition_name, bintrail_id, local_path, row_count, max_event_id, max_binlog_file, max_start_pos, archived_at) VALUES `, []any{}
		for i := lo; i < min(lo+1000, year); i++ {
			if i > lo {
				q += ","
			}
			q += "(?, 'year', '/a/x.parquet', 10, ?, 'binlog.000001', ?, NOW() - INTERVAL ? HOUR)"
			args = append(args, indexer.PartitionName(first.Add(time.Duration(i)*time.Hour)), i+1, 4+i, year-i)
		}
		testutil.MustExec(t, r.db, q, args...)
	}
	scanned := measure(fmt.Sprintf("%d archive_state rows, no idx_archived_at", year))
	readAlone := func(what string) {
		t.Helper()
		var ms []float64
		for range 21 {
			start := time.Now()
			heads, present, err := query.LoadArchivesWrittenSince(context.Background(), r.db, r.stamp)
			if err != nil || !present {
				t.Fatalf("LoadArchivesWrittenSince: present=%v err=%v", present, err)
			}
			if n := heads.MayHoldAfter(r.stamp, r.anchor); n != 0 {
				t.Fatalf("MayHoldAfter = %d in the steady state", n)
			}
			ms = append(ms, float64(time.Since(start).Microseconds())/1000)
		}
		sort.Float64s(ms)
		t.Logf("archive read alone, %d rows, %s: median %.2f ms, worst %.2f ms", year, what, ms[len(ms)/2], ms[len(ms)-1])
	}
	readAlone("no idx_archived_at")
	// The index the migration adds.
	if err := indexer.EnsureArchiveStateSchema(r.db); err != nil {
		t.Fatalf("EnsureArchiveStateSchema: %v", err)
	}
	indexed := measure(fmt.Sprintf("%d archive_state rows, idx_archived_at", year))
	readAlone("idx_archived_at")
	var plan string
	if err := r.db.QueryRow(`EXPLAIN FORMAT=TREE SELECT partition_name FROM archive_state
		WHERE archived_at >= NOW() - INTERVAL (UNIX_TIMESTAMP() - ?) SECOND`, r.stamp.Unix()).Scan(&plan); err == nil {
		if !strings.Contains(plan, "idx_archived_at") {
			t.Errorf("the archive read does not use idx_archived_at:\n%s", plan)
		}
	} else {
		t.Logf("EXPLAIN FORMAT=TREE not available here: %v", err)
	}
	t.Logf("whole check over no rows: %+.2f ms scanned, %+.2f ms indexed", scanned-none, indexed-none)
}
