//go:build integration

package console

import (
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2186: when the binlog-renumbering check cannot tell from a snapshot's
// event mark (the index was backfilled or rebuilt, the mark's event was
// deleted while older ones remain), "cannot say" is not "unchanged": the
// copy is not vouched for and MySQL answers.
func TestIntegrationCopyUnchanged_numberingUnchecked_2186(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(r *unchangedRig, markID uint64)
		want  string
	}{
		{name: "checked: vouched for"},
		{name: "backfilled index", want: "`bintrail index` also wrote into this index",
			setup: func(r *unchangedRig, _ uint64) {
				testutil.MustExec(t, r.db, `INSERT INTO index_state (binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at)
					VALUES ('binlog.000005', 1, 150, 1, 'completed', UTC_TIMESTAMP(), UTC_TIMESTAMP())`)
			}},
		{name: "the mark's event deleted while older ones remain", want: "was deleted from the index",
			setup: func(r *unchangedRig, markID uint64) {
				testutil.MustExec(t, r.db, `DELETE FROM binlog_events WHERE event_id = ?`, markID)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newUnchangedRig(t)
			r.event("shop", "elsewhere", r.anchor.File, r.anchor.Pos-200, r.stamp.Add(-2*time.Minute))
			r.event("shop", "elsewhere", r.anchor.File, r.anchor.Pos-100, r.stamp.Add(-time.Minute))
			id, err := strconv.ParseUint(r.lastID("elsewhere"), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			tb := r.table(func(md map[string]string) {
				md[baseline.MetaKeyEventMark] = reconstruct.EventMark{ID: id, File: r.anchor.File, End: r.anchor.Pos - 50}.Encode()
			})
			if tc.setup != nil {
				tc.setup(r, id)
			}
			if tc.want == "" {
				r.wantUnchanged(tb)
				return
			}
			r.wantNot(tc.want, tb)
			r.wantNot("binlog numbering not checked", tb)
		})
	}
}
