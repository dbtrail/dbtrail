package verify

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2156 item 5: `verify --check recover` walks each row's changes and checks
// that every before-image is what the change before it left. Its input comes
// in (event_timestamp, event_id) order, and event_timestamp is the time a
// change's STATEMENT STARTED. Session B waits on a row lock that A holds: A
// is in the binary log first and carries the later time. Walked by time, B
// comes first and its before-image (A's value) reads as a hole in a chain
// that is whole in the binary log.
//
// The shape below is #2151's first one as the capture stores it: row 1
// inserted, then A (statement at :02, position 500) and B (statement at :00,
// waited, position 900).

func riAt(ev query.ResultRow, ts time.Time, file string, pos uint64) query.ResultRow {
	ev.EventTimestamp, ev.BinlogFile, ev.StartPos = ts, file, pos
	return ev
}

// lockWaitChain is row 1: INSERT, then A and B in time order (B first).
func lockWaitChain(file string) []query.ResultRow {
	ins := riAt(riEvent(10, event.EventInsert, "1", nil, riRow(1, "seed", 1)), riBase, file, 100)
	a := riAt(riEvent(11, event.EventUpdate, "1", riRow(1, "seed", 1), riRow(1, "wait-A", 1)), riBase.Add(12*time.Second), file, 500)
	b := riAt(riEvent(12, event.EventUpdate, "1", riRow(1, "wait-A", 1), riRow(1, "wait-B", 1)), riBase.Add(10*time.Second), file, 900)
	return []query.ResultRow{ins, b, a}
}

func proofStream(calls *int) func([]query.ResultRow) query.IDProof {
	return func([]query.ResultRow) query.IDProof {
		*calls++
		return query.IDsFollowStream
	}
}

func TestCheckRecoverChains2156_lockWaitIsWholeInBinlogOrder(t *testing.T) {
	// Without the rule: the time-order walk reports the chain broken.
	plain := checkRecoverChains(riInput(lockWaitChain("binlog.000001")))
	if plain.Status != StatusMismatch {
		t.Fatalf("premise: the time-order walk says %s (%s), want a mismatch", plain.Status, plain.Detail)
	}

	calls := 0
	in := riInput(lockWaitChain("binlog.000001"))
	in.IDsFollowBinlog = proofStream(&calls)
	out := checkRecoverChains(in)
	if out.Status != StatusMatch || out.Assertions != 2 {
		t.Fatalf("status = %s, assertions = %d (%s); want a match with 2 comparisons", out.Status, out.Assertions, out.Detail)
	}
	if calls != 1 {
		t.Fatalf("the proof was asked %d times, want once (one row needed it)", calls)
	}
	if strings.Contains(out.Detail, "order of changes") {
		t.Fatalf("a proven order carries a note: %q", out.Detail)
	}
}

func TestCheckRecoverChains2156_unprovenOrderKeepsTimeAndSaysSo(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rows  []query.ResultRow
		proof func([]query.ResultRow) query.IDProof
	}{
		{"ids unproven", lockWaitChain("binlog.000001"), nil},
		{"two base names", func() []query.ResultRow {
			r := lockWaitChain("binlog.000001")
			r[1].BinlogFile = "other.000001"
			return r
		}(), func([]query.ResultRow) query.IDProof { return query.IDsFollowStream }},
		{"no positions, ids against times", func() []query.ResultRow {
			r := lockWaitChain("")
			for i := range r {
				r[i].StartPos = 0
			}
			return r
		}(), func([]query.ResultRow) query.IDProof { return query.IDsFollowStream }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := riInput(tc.rows)
			in.IDsFollowBinlog = tc.proof
			out := checkRecoverChains(in)
			if out.Status != StatusMismatch {
				t.Fatalf("status = %s (%s), want the time-order answer (mismatch)", out.Status, out.Detail)
			}
			if !strings.Contains(out.Detail, "order of changes unproven: for 1 row(s)") {
				t.Fatalf("the mismatch does not say the order is unproven: %q", out.Detail)
			}
		})
	}
}

// A real hole on a row whose order nothing questions gets no note: the note
// is for rows where the two orders could differ.
func TestCheckRecoverChains2156_noNoteWhereTheOrdersAgree(t *testing.T) {
	for _, file := range []string{"", "binlog.000001"} {
		rows := []query.ResultRow{
			riEvent(1, event.EventInsert, "1", nil, riRow(1, "a", 1)),
			riEvent(2, event.EventUpdate, "1", riRow(1, "zzz", 1), riRow(1, "b", 1)),
		}
		for i := range rows {
			if file != "" {
				rows[i].BinlogFile, rows[i].StartPos = file, uint64(100*(i+1))
			}
		}
		calls := 0
		in := riInput(rows)
		in.IDsFollowBinlog = proofStream(&calls)
		out := checkRecoverChains(in)
		if out.Status != StatusMismatch || strings.Contains(out.Detail, "order of changes") {
			t.Fatalf("file %q: status = %s detail = %q; want a plain mismatch", file, out.Status, out.Detail)
		}
		if calls != 0 {
			t.Fatalf("file %q: the proof was asked %d times for rows already in order", file, calls)
		}
	}
}

// Reordering one row never moves another row's changes, nor the changes with
// no primary key on record (each is its own row).
func TestCheckRecoverChains2156_otherRowsKeepTheirPlaces(t *testing.T) {
	chain := lockWaitChain("binlog.000001")
	two := []query.ResultRow{
		riAt(riEvent(20, event.EventInsert, "2", nil, riRow(2, "x", 1)), riBase.Add(time.Second), "binlog.000001", 200),
		riAt(riEvent(21, event.EventUpdate, "2", riRow(2, "x", 1), riRow(2, "y", 1)), riBase.Add(11*time.Second), "binlog.000001", 950),
	}
	noPK := riAt(riEvent(30, event.EventUpdate, "", riRow(3, "p", 1), riRow(3, "q", 1)), riBase.Add(11*time.Second), "binlog.000001", 50)
	rows := []query.ResultRow{chain[0], two[0], chain[1], noPK, two[1], chain[2]}
	calls := 0
	in := riInput(rows)
	in.IDsFollowBinlog = proofStream(&calls)
	out := checkRecoverChains(in)
	if out.Status != StatusMatch || out.Assertions != 3 || out.UnwalkableEvents != 1 {
		t.Fatalf("status = %s assertions = %d unwalkable = %d (%s)", out.Status, out.Assertions, out.UnwalkableEvents, out.Detail)
	}
	var ids []uint64
	for _, r := range in.Events {
		ids = append(ids, r.EventID)
	}
	if want := []uint64{10, 20, 11, 30, 21, 12}; !slicesEqual(ids, want) {
		t.Fatalf("walk order = %v, want %v (row 1 reordered in its own places)", ids, want)
	}
}

// PostgreSQL changes carry an LSN as file and are in commit order already.
func TestCheckRecoverChains2156_postgresKeepsArrivalOrder(t *testing.T) {
	rows := lockWaitChain("0/16B3748")
	in := riInput(rows)
	in.IDsFollowBinlog = func([]query.ResultRow) query.IDProof { return query.IDsFollowStream }
	out := checkRecoverChains(in)
	if strings.Contains(out.Detail, "order of changes") {
		t.Fatalf("a PostgreSQL row carries the order note: %q", out.Detail)
	}
	if in.Events[1].EventID != 12 {
		t.Fatalf("PostgreSQL rows were reordered: %+v", in.Events)
	}
}

func slicesEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
