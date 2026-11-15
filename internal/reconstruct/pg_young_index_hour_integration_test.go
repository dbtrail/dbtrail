//go:build integration

package reconstruct_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A snapshot taken in the hour before the index's first partition: that hour
// never had a partition, so it is not one that was rotated out. The fold
// reads it when the index records a capture that started at or before the
// snapshot's floor, and refuses as before when nothing says the changes
// between the snapshot and the index's creation are in it.
func TestReconstructTables_PGSnapshotBeforeTheIndexsFirstHour(t *testing.T) {
	cases := []struct {
		name    string
		from    any  // stream_state.pg_capture_from_lsn
		unknown bool // the index's partition history is not known
		folds   bool
	}{
		{name: "capture started before the floor", from: 0x400, folds: true},
		{name: "capture started at the floor", from: 0x500, folds: true},
		{name: "capture started after the floor", from: 0x501},
		{name: "no recorded start"},
		{name: "history unknown", from: 0x400, unknown: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newPGFixture(t)
			snap := f.at(-30 * time.Second)
			// After the snapshot and before the index's first hour: filed in
			// the first partition, which has no lower bound.
			f.event(0x600, -20*time.Second, 2, "1", s("before-the-hour"))
			f.event(0x700, 5*time.Minute, 1, "3", s("newbie"))
			f.checkpoint(0x700)
			if tc.from != nil {
				testutil.MustExec(t, f.db, "UPDATE stream_state SET pg_capture_from_lsn = "+fmt.Sprint(tc.from)+" WHERE id = 1")
			}
			if tc.unknown {
				testutil.MustExec(t, f.db, "UPDATE partition_history SET history_known = 0 WHERE id = 1")
			}
			root := t.TempDir()
			f.baseline(root, snap, 0x500, "", [2]string{"1", "paid"}, [2]string{"5", "keep"})

			got, _, _, err := f.run(root, pgRun{at: f.at(10 * time.Minute)})
			if !tc.folds {
				if err == nil || !strings.Contains(err.Error(), "hours with no data") {
					t.Fatalf("err = %v, want the refusal for hours with no data", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("fold: %v", err)
			}
			if want := map[string]string{"1": "before-the-hour", "3": "newbie", "5": "keep"}; !sameRows(got, want) {
				t.Fatalf("rows = %v, want %v", got, want)
			}
		})
	}
}
