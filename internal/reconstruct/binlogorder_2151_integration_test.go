//go:build integration

package reconstruct_test

import (
	"testing"
	"time"
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
	}{
		{"two updates, table rewritten, stream index", 2, `{"id":1,"status":"A"}`, 2, `{"id":1,"status":"B"}`, false, true,
			[]string{"1=B", "2=paid", "3=shipped"}},
		{"two updates, delta pair, stream index", 2, `{"id":1,"status":"A"}`, 2, `{"id":1,"status":"B"}`, true, true,
			[]string{"1=B", "2=paid", "3=shipped"}},
		{"two updates, index not written by a stream", 2, `{"id":1,"status":"A"}`, 2, `{"id":1,"status":"B"}`, false, false,
			[]string{"1=B", "2=paid", "3=shipped"}},
		{"update then delete: the row is gone", 2, `{"id":1,"status":"A"}`, 3, "", false, true,
			[]string{"2=paid", "3=shipped"}},
		{"delete then insert: the row is there", 3, "", 1, `{"id":1,"status":"B"}`, true, true,
			[]string{"1=B", "2=paid", "3=shipped"}},
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
			insertTableEvent(t, r.db, r.schema, "orders", 10, 100, started.Add(2*time.Second), tc.firstType, "1", tc.firstAfter)
			insertTableEvent(t, r.db, r.schema, "orders", 20, 300, started, tc.lastType, "1", tc.lastAfter)
			base, rep := r.refresh(t, T, tc.deltas, false)
			got := lagState(t, base, tc.deltas)
			if !equalStrings(got, tc.want) {
				t.Fatalf("refresh applied %d events; state = %v, want %v: row 1 holds the change whose statement started last, not the one the binlog holds last",
					rep.EventsApplied, got, tc.want)
			}
		})
	}
}
