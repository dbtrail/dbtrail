//go:build integration

package reconstruct_test

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2151 through the real refresh and a real index. event_timestamp is when a
// change's STATEMENT STARTED on the source; the binlog holds changes in commit
// order. A statement that waited on a row lock is in the binlog AFTER the
// change it waited for and carries an EARLIER time (the shapes, on a real
// server: internal/streamrun/statement_time_vs_binlog_order_2151_integration_test.go).
// The row's value is the one the binlog holds last.

// TestRefresh_lastChangeOfARowIsTheLastInTheBinlog: two changes of row 1 in
// one refresh window. `first` is at the lower position and the later time,
// `last` at the higher position and two seconds earlier.
func TestRefresh_lastChangeOfARowIsTheLastInTheBinlog(t *testing.T) {
	for _, tc := range []struct {
		name string
		// type and after-image of each change; type 2 is an update, 3 a delete.
		firstType  uint8
		firstAfter string
		lastType   uint8
		lastAfter  string
		deltas     bool
		stream     bool
		want       []string
		// backfilled: the change at the higher position has the LOWER
		// event_id, the order `bintrail index` leaves when it adds an older
		// file after a newer one. Neither time nor id says which is last.
		backfilled bool
	}{
		{"two updates, table rewritten, stream index", 2, `{"id":1,"status":"A"}`, 2, `{"id":1,"status":"B"}`, false, true,
			[]string{"1=B", "2=paid", "3=shipped"}, false},
		{"two updates, delta pair, stream index", 2, `{"id":1,"status":"A"}`, 2, `{"id":1,"status":"B"}`, true, true,
			[]string{"1=B", "2=paid", "3=shipped"}, false},
		{"two updates, index not written by a stream", 2, `{"id":1,"status":"A"}`, 2, `{"id":1,"status":"B"}`, false, false,
			[]string{"1=B", "2=paid", "3=shipped"}, false},
		{"update then delete: the row is gone", 2, `{"id":1,"status":"A"}`, 3, "", false, true,
			[]string{"2=paid", "3=shipped"}, false},
		{"delete then insert: the row is there", 3, "", 1, `{"id":1,"status":"B"}`, true, true,
			[]string{"1=B", "2=paid", "3=shipped"}, false},
		{"ids against positions, table rewritten", 2, `{"id":1,"status":"A"}`, 2, `{"id":1,"status":"B"}`, false, false,
			[]string{"1=B", "2=x", "3=shipped"}, true},
		{"ids against positions, delta pair", 2, `{"id":1,"status":"A"}`, 2, `{"id":1,"status":"B"}`, true, false,
			[]string{"1=B", "2=x", "3=shipped"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var T time.Time
			r, _ := newLagRig(t, func(first time.Time) time.Time {
				T = first.Add(30*time.Hour + 30*time.Minute)
				return T.Add(-8 * time.Hour)
			})
			if tc.stream {
				markStreamCaptured(t, r.db)
			}
			started := T.Add(-time.Hour)
			firstID, lastID := uint64(10), uint64(20)
			if tc.backfilled {
				firstID, lastID = 20, 10
				// The refresh cuts at the end of the event with the highest
				// id. A change of another row, further on and with a higher
				// id still, keeps both changes of row 1 inside the cut.
				insertTableEvent(t, r.db, r.schema, "orders", 30, 500, started.Add(time.Second), 2, "2", `{"id":2,"status":"x"}`)
			}
			insertTableEvent(t, r.db, r.schema, "orders", firstID, 100, started.Add(2*time.Second), tc.firstType, "1", tc.firstAfter)
			insertTableEvent(t, r.db, r.schema, "orders", lastID, 300, started, tc.lastType, "1", tc.lastAfter)
			base, rep := r.refresh(t, T, tc.deltas, false)
			got := lagState(t, base, tc.deltas)
			if !equalStrings(got, tc.want) {
				t.Fatalf("refresh applied %d events; state = %v, want %v: row 1 holds the change whose statement started last, not the one the binlog holds last",
					rep.EventsApplied, got, tc.want)
			}
		})
	}
}

// TestRefresh_lastChangeOfARow_whenTheFoldGoesToDisk: one event per page and
// room for two changed rows in memory. Row 1's last change (B) is folded and
// written to disk before its earlier one (A) arrives, so the two meet only
// when the group is read back.
func TestRefresh_lastChangeOfARow_whenTheFoldGoesToDisk(t *testing.T) {
	for _, deltas := range []bool{false, true} {
		name := map[bool]string{false: "table rewritten", true: "delta pair"}[deltas]
		t.Run(name, func(t *testing.T) {
			var T time.Time
			r, _ := newLagRig(t, func(first time.Time) time.Time {
				T = first.Add(30*time.Hour + 30*time.Minute)
				return T.Add(-8 * time.Hour)
			})
			markStreamCaptured(t, r.db)
			started := T.Add(-time.Hour)
			insertTableEvent(t, r.db, r.schema, "orders", 10, 100, started.Add(3*time.Second), 2, "1", `{"id":1,"status":"A"}`)
			insertTableEvent(t, r.db, r.schema, "orders", 20, 300, started, 2, "1", `{"id":1,"status":"B"}`)
			insertTableEvent(t, r.db, r.schema, "orders", 30, 500, started.Add(time.Second), 2, "2", `{"id":2,"status":"x"}`)
			insertTableEvent(t, r.db, r.schema, "orders", 40, 700, started.Add(2*time.Second), 2, "3", `{"id":3,"status":"y"}`)
			reps, err := reconstruct.ReconstructTables(r.ctx, reconstruct.FullTableConfig{
				IndexDSN: r.dsn, BaselineSrc: r.root, Tables: []string{r.schema + ".orders"},
				At: T, OutputDir: r.root, OutputFormat: reconstruct.OutputFormatParquet,
				TableDeltas: deltas, FetchBatchSize: 1, MaxTouchedRows: 2,
			})
			if err != nil || len(reps) != 1 {
				t.Fatalf("refresh: reports=%d err=%v", len(reps), err)
			}
			if deltas && reps[0].DeltaSpillPasses == 0 {
				t.Fatalf("the pair was not written from changes on disk (DeltaSpillPasses = 0): this case tests nothing")
			}
			base, _, _, err := reconstruct.FindBaseline(r.ctx, r.root, r.schema, "orders", T)
			if err != nil {
				t.Fatalf("FindBaseline: %v", err)
			}
			got, want := lagState(t, base, deltas), []string{"1=B", "2=x", "3=y"}
			if !equalStrings(got, want) {
				t.Fatalf("state = %v, want %v", got, want)
			}
		})
	}
}

// TestRefresh_lastChangeOfARow_acrossTwoRefreshes: A is in the first refresh's
// window. B is past its cut and carries an earlier time than A. The second
// refresh selects by position and must end on B.
func TestRefresh_lastChangeOfARow_acrossTwoRefreshes(t *testing.T) {
	for _, deltas := range []bool{false, true} {
		name := map[bool]string{false: "table rewritten", true: "delta pair"}[deltas]
		t.Run(name, func(t *testing.T) {
			var T time.Time
			r, _ := newLagRig(t, func(first time.Time) time.Time {
				T = first.Add(30*time.Hour + 30*time.Minute)
				return T.Add(-8 * time.Hour)
			})
			markStreamCaptured(t, r.db)
			started := T.Add(-time.Hour)
			insertTableEvent(t, r.db, r.schema, "orders", 10, 100, started.Add(2*time.Second), 2, "1", `{"id":1,"status":"A"}`)
			base, _ := r.refresh(t, T, deltas, false)
			if got, want := lagState(t, base, deltas), []string{"1=A", "2=paid", "3=shipped"}; !equalStrings(got, want) {
				t.Fatalf("first refresh: state = %v, want %v", got, want)
			}
			insertTableEvent(t, r.db, r.schema, "orders", 20, 300, started, 2, "1", `{"id":1,"status":"B"}`)
			base, _ = r.refresh(t, T.Add(time.Hour), deltas, false)
			if got, want := lagState(t, base, deltas), []string{"1=B", "2=paid", "3=shipped"}; !equalStrings(got, want) {
				t.Fatalf("second refresh: state = %v, want %v", got, want)
			}
		})
	}
}
