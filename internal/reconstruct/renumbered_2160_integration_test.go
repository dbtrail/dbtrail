//go:build integration

package reconstruct_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2160: the source's binlog numbering starts over (RESET MASTER, a failover
// to a server whose files are also binlog.N, a shorter log_bin base name) while
// capture keeps writing the same index. The events written after that sort
// BELOW the snapshot's anchor, so a window bounded by position cannot see
// them. A refresh must then fold them or refuse; publishing the old state, or
// moving the anchor past them, loses them for good.

// insertEventAt is insertTableEvent with the binlog file chosen by the caller.
func insertEventAt(t *testing.T, db *sql.DB, schema, table, file string, id, start uint64, at time.Time, pk, after string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO binlog_events
		(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
		VALUES (?, ?, ?, ?, ?, ?, ?, 2, ?, ?, ?)`,
		id, file, start, start+100, at.Format("2006-01-02 15:04:05"), schema, table, pk,
		[]byte(`{"id":`+pk+`,"status":"before"}`), []byte(after)); err != nil {
		t.Fatalf("insert event %d: %v", id, err)
	}
}

// refreshErr is floorRig.refresh returning the run's error instead of failing.
func (r *floorRig) refreshErr(at time.Time, deltas, carry bool) (string, *reconstruct.TableReport, error) {
	reps, err := reconstruct.ReconstructTables(r.ctx, reconstruct.FullTableConfig{
		IndexDSN: r.dsn, BaselineSrc: r.root, Tables: []string{r.schema + ".orders"},
		At: at, OutputDir: r.root, OutputFormat: reconstruct.OutputFormatParquet,
		TableDeltas: deltas, CarryForwardUnchanged: carry,
	})
	if err != nil {
		return "", nil, err
	}
	p, _, _, ferr := reconstruct.FindBaseline(r.ctx, r.root, r.schema, "orders", at)
	if ferr != nil {
		return "", nil, ferr
	}
	return p, reps[0], nil
}

func TestRefresh_afterTheBinlogNumberingStartsOver_neverPublishesAStaleCopy(t *testing.T) {
	for _, tc := range []struct {
		name          string
		deltas, carry bool
	}{
		{"table rewritten", false, false},
		{"table carried forward when unchanged", false, true},
		{"delta pairs", true, false},
		{"delta pairs, carried forward when unchanged", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var T time.Time
			r, _ := newLagRig(t, func(first time.Time) time.Time {
				T = first.Add(6*time.Hour + 30*time.Minute)
				return T.Add(-4 * time.Hour)
			})
			markStreamCaptured(t, r.db)

			// The old numbering: one change, folded by the first refresh, which
			// anchors the snapshot at binlog.000007:200.
			insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 10, 100, T.Add(-time.Hour), "1", `{"id":1,"status":"A"}`)
			if _, _, err := r.refreshErr(T, tc.deltas, tc.carry); err != nil {
				t.Fatalf("first refresh: %v", err)
			}

			// The numbering starts over: the next changes are in binlog.000001.
			insertEventAt(t, r.db, r.schema, "orders", "binlog.000001", 20, 300, T.Add(20*time.Minute), "1", `{"id":1,"status":"B"}`)
			base, rep, err := r.refreshErr(T.Add(time.Hour), tc.deltas, tc.carry)
			if err != nil {
				t.Logf("second refresh refused: %v", err)
				return
			}
			got := lagState(t, base, tc.deltas)
			if want := []string{"1=B", "2=paid", "3=shipped"}; !equalStrings(got, want) {
				t.Errorf("second refresh published %v (events applied %d, carried forward %v), want %v or a refusal: "+
					"the change indexed after the numbering started over is not in the snapshot",
					got, rep.EventsApplied, rep.CarriedForward, want)
			}

			// One more change and one more refresh: a stale copy the refresh
			// re-anchored past the lost change never gets it back.
			insertEventAt(t, r.db, r.schema, "orders", "binlog.000001", 30, 500, T.Add(80*time.Minute), "3", `{"id":3,"status":"C"}`)
			base, rep, err = r.refreshErr(T.Add(2*time.Hour), tc.deltas, tc.carry)
			if err != nil {
				t.Logf("third refresh refused: %v", err)
				return
			}
			got = lagState(t, base, tc.deltas)
			if want := []string{"1=B", "2=paid", "3=C"}; !equalStrings(got, want) {
				t.Errorf("third refresh published %v (events applied %d), want %v or a refusal", got, rep.EventsApplied, want)
			}
		})
	}
}

// The shape a position-mode stream leaves when it restarts after the reset:
// its saved file is gone, it jumps to the oldest file the source has and
// stamps the jump as a permanent loss. That stamp already stops the refresh.
func TestRefresh_afterTheBinlogNumberingStartsOver_aStampedCaptureLossRefuses(t *testing.T) {
	var T time.Time
	r, _ := newLagRig(t, func(first time.Time) time.Time {
		T = first.Add(6*time.Hour + 30*time.Minute)
		return T.Add(-4 * time.Hour)
	})
	markStreamCaptured(t, r.db)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 10, 100, T.Add(-time.Hour), "1", `{"id":1,"status":"A"}`)
	if _, _, err := r.refreshErr(T, false, false); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	testutil.MustExec(t, r.db, `UPDATE stream_state SET gap_lost_at = ?, gap_lost_detail = 'binlog gap detected but CANNOT be filled: required file binlog.000007 has been purged'
		WHERE id = 1`, T.Add(10*time.Minute).Format("2006-01-02 15:04:05"))
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000001", 20, 300, T.Add(20*time.Minute), "1", `{"id":1,"status":"B"}`)
	_, _, err := r.refreshErr(T.Add(time.Hour), false, false)
	if !errors.Is(err, reconstruct.ErrCaptureGap) {
		t.Fatalf("refresh over a stamped capture loss: err = %v, want ErrCaptureGap", err)
	}
}
