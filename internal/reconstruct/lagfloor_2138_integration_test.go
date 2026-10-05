//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/verify"
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

// lagState reads the table the way its layout is read: through the chain's
// state query with deltas on, as one file with them off.
func lagState(t *testing.T, base string, deltas bool) []string {
	t.Helper()
	if deltas {
		return readOrdersState(t, base)
	}
	out := readOrders(t, base)
	sort.Strings(out)
	return out
}

// TestRefresh_afterALaggingRefresh_appliesWhatCaptureIndexedLater: the first
// refresh runs at wall clock T with the index `lag` behind, and the event
// indexed afterwards ran on the source at T-lag+1h.
func TestRefresh_afterALaggingRefresh_appliesWhatCaptureIndexedLater(t *testing.T) {
	const old = 30*time.Hour + 30*time.Minute
	for _, tc := range []struct {
		name string
		lag  time.Duration
		// dumpAge is how long before T the dump was taken. Past a day the
		// second refresh rewrites the table (the chain is too old); under it,
		// it continues the chain with a second pair.
		dumpAge time.Duration
		deltas  bool
		// carried: the first refresh finds nothing for orders and keeps its
		// file; the event that moves the cut belongs to another table.
		carried  bool
		stream   bool
		wantPair bool
	}{
		{"second pair of a chain, 5 h behind", 5 * time.Hour, 8 * time.Hour, true, false, true, true},
		{"second pair of a chain, 3 h behind", 3 * time.Hour, 8 * time.Hour, true, false, true, true},
		{"second pair of a chain, index not written by a stream", 5 * time.Hour, 8 * time.Hour, true, false, false, true},
		{"chain rewritten for its age, 5 h behind", 5 * time.Hour, old, true, false, true, false},
		{"chain rewritten for its age, 26 h behind", 26 * time.Hour, old, true, false, true, false},
		{"table rewritten, 5 h behind", 5 * time.Hour, old, false, false, true, false},
		{"table rewritten, 26 h behind", 26 * time.Hour, old, false, false, true, false},
		{"untouched table with deltas on, 5 h behind", 5 * time.Hour, 8 * time.Hour, true, true, true, true},
		{"table carried forward, 5 h behind", 5 * time.Hour, old, false, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var T time.Time
			r, _ := newLagRig(t, func(first time.Time) time.Time {
				T = first.Add(old)
				return T.Add(-tc.dumpAge)
			})
			if tc.stream {
				markStreamCaptured(t, r.db)
			}
			// e1: already indexed when the first refresh runs.
			e1Table := "orders"
			if tc.carried {
				e1Table = "items"
			}
			insertTableEvent(t, r.db, r.schema, e1Table, 10, 100, T.Add(-tc.lag), 2, "1", `{"id":1,"status":"A"}`)
			_, rep := r.refresh(t, T, tc.deltas, tc.carried)
			// With deltas on, an untouched table is published as its previous
			// file; with them off it is carried forward.
			if tc.carried && !tc.deltas && !rep.CarriedForward {
				t.Fatalf("first refresh: the untouched table was not carried forward")
			}
			if want := map[bool]int64{false: 1, true: 0}[tc.carried]; rep.EventsApplied != want {
				t.Fatalf("first refresh applied %d events, want %d", rep.EventsApplied, want)
			}
			// e2: indexed AFTER the first refresh, later position, and it ran
			// on the source an hour after e1: still hours before T.
			insertTableEvent(t, r.db, r.schema, "orders", 20, 300, T.Add(-tc.lag+time.Hour), 3, "2", "")
			base, rep := r.refresh(t, T.Add(time.Hour), tc.deltas, tc.carried)
			want := []string{"1=A", "3=shipped"}
			if tc.carried {
				want = []string{"1=new", "3=shipped"}
			}
			got := lagState(t, base, tc.deltas)
			if !equalStrings(got, want) {
				t.Fatalf("second refresh applied %d events; state = %v, want %v: the delete of id 2 was indexed after the first refresh and is not in the snapshot",
					rep.EventsApplied, got, want)
			}
			if rep.DeltaPairWritten != tc.wantPair {
				t.Fatalf("second refresh: DeltaPairWritten = %v (%q), want %v", rep.DeltaPairWritten, rep.DeltaCompacted, tc.wantPair)
			}
		})
	}
}

// TestRefresh_filesIndexedIntoAStreamIndex_doNotHideALateEvent: `bintrail
// index` adds an older binlog file to an index a stream writes. Its rows get
// the newest ids with old positions, so the newest row of the late event's
// partition is one of them, before the previous cut. The refresh must not
// read that as "nothing after the cut in here".
func TestRefresh_filesIndexedIntoAStreamIndex_doNotHideALateEvent(t *testing.T) {
	for _, tc := range []struct {
		name string
		// state is the file's record in index_state, given T.
		state func(T time.Time) string
	}{
		{"the run completed after the previous refresh", func(T time.Time) string {
			return "'completed', '" + T.Add(5*time.Minute).Format("2006-01-02 15:04:05") + "', '" + T.Add(10*time.Minute).Format("2006-01-02 15:04:05") + "'"
		}},
		// No completed_at: as far as the index knows the run is still going,
		// however long ago it started.
		{"the run is still in progress", func(T time.Time) string {
			return "'in_progress', '2020-01-01 00:00:00', NULL"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var T time.Time
			r, _ := newLagRig(t, func(first time.Time) time.Time {
				T = first.Add(30*time.Hour + 30*time.Minute)
				return T.Add(-8 * time.Hour)
			})
			markStreamCaptured(t, r.db)
			insertTableEvent(t, r.db, r.schema, "orders", 10, 1000, T.Add(-5*time.Hour), 2, "1", `{"id":1,"status":"A"}`)
			r.refresh(t, T, true, false)
			late := T.Add(-4 * time.Hour)
			insertTableEvent(t, r.db, r.schema, "orders", 20, 1300, late, 3, "2", "")
			// The file's row: a higher id, the same hour, a position before the cut.
			insertTableEvent(t, r.db, r.schema, "items", 30, 50, late.Add(time.Minute), 2, "9", `{"id":9,"status":"old"}`)
			testutil.MustExec(t, r.db, `INSERT INTO index_state (binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at)
				VALUES ('binlog.000001', 1, 150, 1, `+tc.state(T)+`)`)
			// The stream goes on: this fixes the cut after the late event.
			insertTableEvent(t, r.db, r.schema, "items", 40, 1500, T.Add(20*time.Minute), 2, "9", `{"id":9,"status":"new"}`)
			base, rep := r.refresh(t, T.Add(time.Hour), true, false)
			if got, want := lagState(t, base, true), []string{"1=A", "3=shipped"}; !equalStrings(got, want) {
				t.Fatalf("refresh applied %d events; state = %v, want %v", rep.EventsApplied, got, want)
			}
		})
	}
}

// TestRefresh_lateEventOlderThanTheFirstPartition: the index's first
// partition has no lower bound, so an event older than its hour is stored in
// it. When such an event arrives late, the fetch starts at its time, hours
// below any partition's name. Those hours are not missing: the refresh must
// apply the event, not refuse with a coverage gap.
func TestRefresh_lateEventOlderThanTheFirstPartition(t *testing.T) {
	for _, deltas := range []bool{true, false} {
		t.Run(map[bool]string{true: "pair", false: "table rewritten"}[deltas], func(t *testing.T) {
			var T time.Time
			r, first := newLagRig(t, func(first time.Time) time.Time {
				T = first.Add(12*time.Hour + 30*time.Minute)
				return T.Add(-8 * time.Hour)
			})
			markStreamCaptured(t, r.db)
			insertTableEvent(t, r.db, r.schema, "orders", 10, 100, T.Add(-5*time.Hour), 2, "1", `{"id":1,"status":"A"}`)
			r.refresh(t, T, deltas, false)
			// Indexed after the first refresh; it ran five hours before the
			// first partition's own hour.
			insertTableEvent(t, r.db, r.schema, "orders", 20, 300, first.Add(-5*time.Hour), 3, "2", "")
			base, rep := r.refresh(t, T.Add(time.Hour), deltas, false)
			if got, want := lagState(t, base, deltas), []string{"1=A", "3=shipped"}; !equalStrings(got, want) {
				t.Fatalf("refresh applied %d events; state = %v, want %v", rep.EventsApplied, got, want)
			}
		})
	}
}

// TestVerify_afterALaggingRefresh_agreesWithARealRead: the check an operator
// has. `verify` compares the newest real read of a table with the snapshot
// before it plus the index. Over the chain above, a read of the true state
// must match: before #2138 the refresh had left the delete out, the read did
// not hold the row, and this reported a mismatch (which is how a snapshot
// that already lost a change shows up).
func TestVerify_afterALaggingRefresh_agreesWithARealRead(t *testing.T) {
	var T time.Time
	r, _ := newLagRig(t, func(first time.Time) time.Time {
		T = first.Add(30*time.Hour + 30*time.Minute)
		return first
	})
	markStreamCaptured(t, r.db)
	insertTableEvent(t, r.db, r.schema, "orders", 10, 100, T.Add(-5*time.Hour), 2, "1", `{"id":1,"status":"A"}`)
	r.refresh(t, T, false, false)
	insertTableEvent(t, r.db, r.schema, "orders", 20, 300, T.Add(-4*time.Hour), 3, "2", "")
	r.refresh(t, T.Add(time.Hour), false, false)
	// A third change, late as well, between the last refresh and the read:
	// verify's own fetch has the same floor to get right.
	insertTableEvent(t, r.db, r.schema, "orders", 30, 500, T.Add(-3*time.Hour), 2, "3", `{"id":3,"status":"sent"}`)
	writeReadOfOrders(t, r.root, T.Add(2*time.Hour), r.schema, 600, [][]string{{"1", "A"}, {"3", "sent"}})

	pairs, _, err := verify.FindBaselinePair(r.ctx, r.root)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: pairs=%d err=%v", len(pairs), err)
	}
	resolver, err := metadata.NewResolver(r.db, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	dbName := r.dsn[strings.LastIndex(r.dsn, "/")+1:]
	res, err := verify.VerifyBaselinePair(r.ctx, verify.BaselineConfig{IndexDB: r.db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}, pairs[0])
	if err != nil || res.Status != verify.StatusMatch {
		t.Fatalf("verify over the chain: %s (%q), err=%v; want a match with the read of the true state", res.Status, res.Detail, err)
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
			if got, want := lagState(t, base, deltas), []string{"1=new", "3=shipped"}; !equalStrings(got, want) {
				t.Fatalf("refresh applied %d events; state = %v, want %v", rep.EventsApplied, got, want)
			}
		})
	}
}
