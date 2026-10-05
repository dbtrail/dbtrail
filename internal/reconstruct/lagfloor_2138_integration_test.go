//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2138 through the real refresh and a real index: a refresh that runs while
// the index is hours behind the source, followed by one that runs after the
// index caught up. event_timestamp is when the statement ran ON THE SOURCE and
// a refresh stamps what it writes with ITS OWN clock, so the events indexed
// after a lagging refresh are older than that stamp while sitting after its
// position. The next refresh must still apply them.

// insertTableEvent writes one event for any table of the schema. Type 2 is an
// update to after, type 3 a delete.
func insertTableEvent(t *testing.T, db *sql.DB, schema, table string, id, start uint64, at time.Time, evType uint8, pk, after string) {
	t.Helper()
	var afterJSON, before []byte
	if after != "" {
		afterJSON = []byte(after)
	}
	if evType == 3 {
		before = []byte(`{"id":` + pk + `,"status":"gone"}`)
	}
	if _, err := db.Exec(`INSERT INTO binlog_events
		(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
		VALUES (?, 'binlog.000001', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, start, start+100, at.Format("2006-01-02 15:04:05"), schema, table, evType, pk, before, afterJSON); err != nil {
		t.Fatalf("insert event %d: %v", id, err)
	}
}

// newLagRig is newFloorRig with the dump's time chosen by the caller. first is
// the hour the index's first partition starts at.
func newLagRig(t *testing.T, dumpAt func(first time.Time) time.Time) (*floorRig, time.Time) {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	first := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
	r := &floorRig{ctx: ctx, db: db, dsn: testutil.BaseDSN() + "/" + dbName, root: t.TempDir(), schema: "shop", h0: dumpAt(first)}
	seedOrdersSnapshot(t, db, r.schema, first)
	seedSourceBaseline(t, r.root, r.h0, r.schema)
	return r, first
}

func (r *floorRig) refresh(t *testing.T, at time.Time, deltas, carry bool) (string, *reconstruct.TableReport) {
	t.Helper()
	reps, err := reconstruct.ReconstructTables(r.ctx, reconstruct.FullTableConfig{
		IndexDSN: r.dsn, BaselineSrc: r.root, Tables: []string{r.schema + ".orders"},
		At: at, OutputDir: r.root, OutputFormat: reconstruct.OutputFormatParquet,
		TableDeltas: deltas, CarryForwardUnchanged: carry,
	})
	if err != nil {
		t.Fatalf("refresh(at=%s, deltas=%v, carry=%v): %v", at.Format(time.RFC3339), deltas, carry, err)
	}
	if len(reps) != 1 {
		t.Fatalf("refresh returned %d reports, want 1", len(reps))
	}
	p, _, _, err := reconstruct.FindBaseline(r.ctx, r.root, r.schema, "orders", at)
	if err != nil {
		t.Fatalf("FindBaseline(%s): %v", at.Format(time.RFC3339), err)
	}
	return p, reps[0]
}

// TestRefresh_afterALaggingRefresh_appliesWhatCaptureIndexedLater: the dump is
// old, the first refresh runs at wall clock T with the index `lag` behind, and
// the event indexed afterwards ran on the source at T-lag+1h.
func TestRefresh_afterALaggingRefresh_appliesWhatCaptureIndexedLater(t *testing.T) {
	for _, tc := range []struct {
		name   string
		lag    time.Duration
		deltas bool
		// carried: the first refresh finds nothing for orders and carries its
		// file forward; the event that moves the cut belongs to another table.
		carried bool
		stream  bool
	}{
		{"pair, 5 h behind", 5 * time.Hour, true, false, true},
		{"pair, 3 h behind", 3 * time.Hour, true, false, true},
		{"pair, 26 h behind", 26 * time.Hour, true, false, true},
		{"pair, 5 h behind, index not written by a stream", 5 * time.Hour, true, false, false},
		{"table rewritten, 5 h behind", 5 * time.Hour, false, false, true},
		{"table rewritten, 26 h behind", 26 * time.Hour, false, false, true},
		{"table carried forward, deltas on, 5 h behind", 5 * time.Hour, true, true, true},
		{"table carried forward, deltas off, 5 h behind", 5 * time.Hour, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, first := newLagRig(t, func(first time.Time) time.Time { return first })
			if tc.stream {
				markStreamCaptured(t, r.db)
			}
			T := first.Add(30*time.Hour + 30*time.Minute)
			// e1: already indexed when the first refresh runs.
			e1Table := "orders"
			if tc.carried {
				e1Table = "items"
			}
			insertTableEvent(t, r.db, r.schema, e1Table, 10, 100, T.Add(-tc.lag), 2, "1", `{"id":1,"status":"A"}`)
			_, rep := r.refresh(t, T, tc.deltas, tc.carried)
			if tc.carried != rep.CarriedForward {
				t.Fatalf("first refresh: CarriedForward = %v, want %v", rep.CarriedForward, tc.carried)
			}
			if !tc.carried && rep.EventsApplied != 1 {
				t.Fatalf("first refresh applied %d events, want 1", rep.EventsApplied)
			}
			// e2: indexed AFTER the first refresh, later position, and it ran
			// on the source an hour after e1: still hours before T.
			insertTableEvent(t, r.db, r.schema, "orders", 20, 300, T.Add(-tc.lag+time.Hour), 3, "2", "")
			base, rep := r.refresh(t, T.Add(time.Hour), tc.deltas, tc.carried)
			want := []string{"1=A", "3=shipped"}
			if tc.carried {
				want = []string{"1=new", "3=shipped"}
			}
			got := readOrdersState(t, base)
			if !equalStrings(got, want) {
				t.Fatalf("second refresh applied %d events; state = %v, want %v: the delete of id 2 was indexed after the first refresh and is not in the snapshot",
					rep.EventsApplied, got, want)
			}
		})
	}
}

// TestRefresh_firstAfterADump_appliesEventsOlderThanTheDumpStamp: the same
// shape with no refresh in between. The dump's stamp is the dump host's clock
// at T; the event after the dump's position ran on the source hours earlier
// (a source that is itself a delayed replica writes its primary's timestamps
// into its own binary log).
func TestRefresh_firstAfterADump_appliesEventsOlderThanTheDumpStamp(t *testing.T) {
	for _, deltas := range []bool{true, false} {
		name := "table rewritten"
		if deltas {
			name = "pair"
		}
		t.Run(name, func(t *testing.T) {
			var T time.Time
			r, _ := newLagRig(t, func(first time.Time) time.Time {
				T = first.Add(30 * time.Hour)
				return T
			})
			markStreamCaptured(t, r.db)
			insertTableEvent(t, r.db, r.schema, "orders", 20, 300, T.Add(-4*time.Hour), 3, "2", "")
			base, rep := r.refresh(t, T.Add(time.Hour), deltas, false)
			if got, want := readOrdersState(t, base), []string{"1=new", "3=shipped"}; !equalStrings(got, want) {
				t.Fatalf("refresh applied %d events; state = %v, want %v", rep.EventsApplied, got, want)
			}
		})
	}
}
