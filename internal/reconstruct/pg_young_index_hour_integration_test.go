//go:build integration

package reconstruct_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A snapshot taken before the index's first hour: the hours before it never
// had a partition, so they are not hours that were rotated out. The fold
// reads through them when the index records that nothing was dropped and
// that its capture started at or before the snapshot's floor, and refuses as
// before when either is missing.
func TestReconstructTables_PGSnapshotBeforeTheIndexsFirstHour(t *testing.T) {
	const day = 24 * time.Hour
	cases := []struct {
		name string
		from any // stream_state.pg_capture_from_lsn
		// snap and at are the snapshot's time and the fold's point, from the
		// index's first hour.
		snap, at time.Duration
		spoil    string // a statement run on the index before the fold
		// drop is the partition dropped by hand, unrecorded: 1 the first
		// hour's, 2 the one after it.
		drop int
		want     map[string]string
	}{
		{name: "capture started before the floor", from: 0x400, snap: -30 * time.Second, at: 10 * time.Minute,
			want: map[string]string{"1": "after-the-snapshot", "3": "newbie", "5": "keep"}},
		{name: "capture started at the floor", from: 0x500, snap: -30 * time.Second, at: 10 * time.Minute,
			want: map[string]string{"1": "after-the-snapshot", "3": "newbie", "5": "keep"}},
		// The fetch's own start does not move: a change hours before the
		// first partition's hour is read.
		{name: "hours before", from: 0x400, snap: -3 * time.Hour, at: 10 * time.Minute,
			want: map[string]string{"1": "after-the-snapshot", "3": "newbie", "5": "keep"}},
		{name: "the point is before the first hour too", from: 0x400, snap: -30 * time.Second, at: -10 * time.Second,
			want: map[string]string{"1": "after-the-snapshot", "5": "keep"}},
		// An hour from the first one on is judged as it always was.
		{name: "an hour after the first is still judged", from: 0x400, snap: -30 * time.Second, at: 2*time.Hour + 10*time.Minute, drop: 2},
		{name: "capture started after the floor", from: 0x501, snap: -30 * time.Second, at: 10 * time.Minute},
		{name: "no recorded start", snap: -30 * time.Second, at: 10 * time.Minute},
		{name: "history unknown", from: 0x400, snap: -30 * time.Second, at: 10 * time.Minute,
			spoil: "UPDATE partition_history SET history_known = 0 WHERE id = 1"},
		{name: "a partition was dropped", from: 0x400, snap: -30 * time.Second, at: 10 * time.Minute,
			spoil: fmt.Sprintf("UPDATE partition_history SET dropped_below_unix = %d WHERE id = 1", time.Now().Add(-day).Unix())},
		// The changes went with the partition; the point's own hour has
		// none, so only the first hour's absence can say so.
		{name: "the first partition is gone", from: 0x400, snap: -30 * time.Second, at: -10 * time.Second, drop: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newPGFixture(t)
			// After the snapshot and before the index's first hour: filed in
			// the first partition, which has no lower bound.
			f.event(0x600, tc.snap+10*time.Second, 2, "1", s("after-the-snapshot"))
			f.event(0x700, 5*time.Minute, 1, "3", s("newbie"))
			f.checkpoint(0x700)
			if tc.from != nil {
				testutil.MustExec(t, f.db, fmt.Sprintf("UPDATE stream_state SET pg_capture_from_lsn = %d WHERE id = 1", tc.from))
			}
			if tc.spoil != "" {
				testutil.MustExec(t, f.db, tc.spoil)
			}
			if tc.drop > 0 {
				testutil.MustExec(t, f.db, "ALTER TABLE binlog_events DROP PARTITION p_"+f.at(time.Duration(tc.drop-1)*time.Hour).Format("2006010215"))
			}
			root := t.TempDir()
			f.baseline(root, f.at(tc.snap), 0x500, "", [2]string{"1", "paid"}, [2]string{"5", "keep"})

			got, _, _, err := f.run(root, pgRun{at: f.at(tc.at)})
			if tc.want == nil {
				if err == nil || !strings.Contains(err.Error(), "hours with no data") {
					t.Fatalf("err = %v (rows %v), want the refusal for hours with no data", err, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("fold: %v", err)
			}
			if !sameRows(got, tc.want) {
				t.Fatalf("rows = %v, want %v", got, tc.want)
			}
		})
	}
}
