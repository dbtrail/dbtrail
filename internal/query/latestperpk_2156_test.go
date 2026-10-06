package query

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func keyRow(pk string, id uint64, file string, pos uint64, at time.Time) ResultRow {
	r := orderRow(id, file, pos, at)
	r.PKValues = pk
	return r
}

// The decision table of LatestPerPKInBinlog (#2156), one key per case unless
// the case is about several.
func TestLatestPerPKInBinlog_decisionTable(t *testing.T) {
	const f = "binlog.000007"
	at := func(s int) time.Time { return orderT0.Add(time.Duration(s) * time.Second) }
	never := func([]ResultRow) IDProof { panic("the index must not be asked when the two latest sets agree") }
	for _, tc := range []struct {
		name        string
		rows        []ResultRow
		n           int
		proof       func([]ResultRow) IDProof
		wantIDs     []uint64
		wantOrder   LatestPerPKOrder // warning not compared
		noteHas     string           // "" = no note
		proofCalled int              // -1 = not counted
	}{
		{
			name: "the lock wait on an index a stream writes: the change the binary log holds last",
			// B (id 2) started 2 s before A (id 1) and committed after it.
			rows:    []ResultRow{keyRow("1", 2, f, 900, at(0)), keyRow("1", 1, f, 400, at(2))},
			n:       1, proof: proven,
			wantIDs: []uint64{2}, wantOrder: LatestPerPKOrder{Disagreed: 1, Sorted: 1},
		},
		{
			name:    "the lock wait where the ids cannot vouch: statement time, and a note",
			rows:    []ResultRow{keyRow("1", 2, f, 900, at(0)), keyRow("1", 1, f, 400, at(2))},
			n:       1, proof: unproven,
			wantIDs: []uint64{1}, wantOrder: LatestPerPKOrder{Disagreed: 1, Refused: 1},
			noteHas: "this index cannot show which one is right",
		},
		{
			name: "the two latest sets agree: the index is not asked",
			rows: []ResultRow{keyRow("1", 1, f, 400, at(0)), keyRow("1", 2, f, 900, at(1)),
				keyRow("1", 3, f, 950, at(1))},
			n: 1, proof: never, wantIDs: []uint64{3},
		},
		{
			name: "fewer changes than n: all of them, nothing asked",
			rows: []ResultRow{keyRow("1", 2, f, 900, at(0)), keyRow("1", 1, f, 400, at(2))},
			n:    2, proof: never, wantIDs: []uint64{2, 1},
		},
		{
			name:    "a candidate with no coordinate (indexed by an older build)",
			rows:    []ResultRow{keyRow("1", 2, "", 0, at(0)), keyRow("1", 1, f, 400, at(2))},
			n:       1, proof: proven,
			wantIDs: []uint64{1}, wantOrder: LatestPerPKOrder{Disagreed: 1, Refused: 1},
			noteHas: "carry no binary log position",
		},
		{
			name:    "two base names",
			rows:    []ResultRow{keyRow("1", 2, "binlog2.000001", 900, at(0)), keyRow("1", 1, f, 400, at(2))},
			n:       1, proof: proven,
			wantIDs: []uint64{1}, wantOrder: LatestPerPKOrder{Disagreed: 1, Refused: 1},
			noteHas: "binary logs with different names",
		},
		{
			name: "an index built from files only, the two changes in two files",
			rows: []ResultRow{keyRow("1", 2, "binlog.000008", 100, at(0)), keyRow("1", 1, f, 400, at(2))},
			n:    1, proof: filesOnly,
			wantIDs: []uint64{1}, wantOrder: LatestPerPKOrder{Disagreed: 1, Refused: 1},
			noteHas: "built with `bintrail index` only",
		},
		{
			name: "an index built from files only, both changes in one file",
			rows: []ResultRow{keyRow("1", 2, f, 900, at(0)), keyRow("1", 1, f, 400, at(2))},
			n:    1, proof: filesOnly,
			wantIDs: []uint64{2}, wantOrder: LatestPerPKOrder{Disagreed: 1, Sorted: 1},
		},
		{
			// Event 1 has the later time AND the later position; the index
			// received event 2 after it. Positions and times agree, ids do not.
			name:    "positions agree with the times, the ids do not: statement time, and a note",
			rows:    []ResultRow{keyRow("1", 2, f, 400, at(0)), keyRow("1", 1, f, 900, at(2))},
			n:       1, proof: proven,
			wantIDs: []uint64{1}, wantOrder: LatestPerPKOrder{Disagreed: 1, Refused: 1},
			noteHas: "The index received event 2 (at binlog.000007:400) after event 1 (at binlog.000007:900)",
		},
		{
			name: "PostgreSQL: arrival order is commit order, no note",
			rows: []ResultRow{keyRow("1", 2, "0/1A2B", 900, at(0)), keyRow("1", 1, "0/1A00", 400, at(2))},
			n:    1, proof: proven,
			wantIDs: []uint64{1}, wantOrder: LatestPerPKOrder{Disagreed: 1},
		},
		{
			name: "rows without pk_values are each their own key: all kept",
			rows: []ResultRow{keyRow("", 2, f, 900, at(0)), keyRow("", 1, f, 400, at(2))},
			n:    1, proof: never, wantIDs: []uint64{2, 1},
		},
		{
			// Row 1 changed by A (ids 1, 3) and B (id 2): B waited on A's
			// first change and committed after A's second. Latest two by time:
			// ids 3 and 1 (the later times). In the binary log: 1, 3, 2.
			name: "n = 2: the latest two in the binary log",
			rows: []ResultRow{
				keyRow("1", 4, f, 50, at(-10)), // an older change, id out of the way
				keyRow("1", 2, f, 900, at(0)),
				keyRow("1", 1, f, 400, at(1)),
				keyRow("1", 3, f, 600, at(2)),
			},
			n: 2, proof: func([]ResultRow) IDProof { return IDsFollowStream },
			// id 4 is the largest id, so the ids put it among the latest two;
			// its position and its time put it first: refused.
			wantIDs: []uint64{1, 3}, wantOrder: LatestPerPKOrder{Disagreed: 1, Refused: 1},
			noteHas: "The index received event 4 (at binlog.000007:50) after event 3",
		},
		{
			name: "n = 2 on a clean stream index",
			rows: []ResultRow{
				keyRow("1", 1, f, 50, at(-10)),
				keyRow("1", 3, f, 900, at(0)),
				keyRow("1", 2, f, 400, at(1)),
				keyRow("1", 4, f, 950, at(2)),
			},
			n: 2, proof: proven,
			wantIDs: []uint64{3, 4}, wantOrder: LatestPerPKOrder{Disagreed: 1, Sorted: 1},
		},
		{
			name: "several keys: only the one that disagrees is asked about",
			rows: []ResultRow{
				keyRow("1", 2, f, 900, at(0)), keyRow("1", 1, f, 400, at(2)),
				keyRow("5", 3, f, 1000, at(3)), keyRow("5", 4, f, 1100, at(4)),
				keyRow("6", 5, f, 1200, at(5)),
			},
			n: 1, proof: proven, proofCalled: 1,
			wantIDs: []uint64{2, 4, 5}, wantOrder: LatestPerPKOrder{Disagreed: 1, Sorted: 1},
		},
		{name: "no rows", n: 1, proof: never, wantIDs: []uint64{}},
		{
			// The rows are given reversed (below) and come back as given.
			name: "n = 0 is no cap: the rows untouched",
			rows: []ResultRow{keyRow("1", 2, f, 900, at(0)), keyRow("1", 1, f, 400, at(2))},
			n:    0, proof: never, wantIDs: []uint64{1, 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			proof := func(r []ResultRow) IDProof { calls++; return tc.proof(r) }
			rows := slices.Clone(tc.rows)
			// Whatever order the rows come in, the answer is the same.
			slices.Reverse(rows)
			got, order := LatestPerPKInBinlog(rows, tc.n, proof)
			if !slices.Equal(orderIDs(got), tc.wantIDs) {
				t.Fatalf("kept %v, want %v", orderIDs(got), tc.wantIDs)
			}
			gotOrder := order
			gotOrder.warning = ""
			if gotOrder != tc.wantOrder {
				t.Fatalf("order = %+v, want %+v", gotOrder, tc.wantOrder)
			}
			note := order.Note()
			switch {
			case tc.noteHas == "" && note != "":
				t.Fatalf("note %q, want none", note)
			case tc.noteHas != "" && (!strings.Contains(note, tc.noteHas) || !strings.HasPrefix(note, "order of changes unproven: ")):
				t.Fatalf("note %q, want it to start with the marker and hold %q", note, tc.noteHas)
			}
			if tc.proofCalled > 0 && calls != tc.proofCalled {
				t.Fatalf("the index was asked %d times, want %d", calls, tc.proofCalled)
			}
			for i := 1; i < len(got) && tc.n > 0; i++ {
				if compareStatementTime(got[i-1], got[i]) > 0 {
					t.Fatalf("result not in statement-time order: %v", orderIDs(got))
				}
			}
		})
	}
}

// The candidates are what the SQL windows return; the pick over the
// candidates must be the pick over all the rows. For every key shape of up to
// four changes over two files, with every proof.
func TestLatestPerPKInBinlog_candidatesAreEnough(t *testing.T) {
	files := []string{"binlog.000001", "binlog.000002"}
	proofs := []func([]ResultRow) IDProof{proven, unproven, filesOnly}
	count := 0
	for size := 1; size <= 4; size++ {
		perms := permutations(size)
		for _, timeOrder := range perms {
			for _, posOrder := range perms {
				for fileMask := 0; fileMask < 1<<size; fileMask += 3 {
					var rows []ResultRow
					for i := range size {
						// ids 1..size; time and position from the permutations.
						rows = append(rows, keyRow("k", uint64(i+1), files[(fileMask>>i)&1],
							uint64(100*(posOrder[i]+1)), orderT0.Add(time.Duration(timeOrder[i])*time.Second)))
					}
					for n := 1; n <= 2; n++ {
						for _, p := range proofs {
							count++
							all, allOrder := LatestPerPKInBinlog(slices.Clone(rows), n, p)
							cand := candidatesOf(rows, n)
							fromCand, candOrder := LatestPerPKInBinlog(cand, n, p)
							if !slices.Equal(orderIDs(all), orderIDs(fromCand)) || allOrder != candOrder {
								t.Fatalf("rows %+v n=%d: over all rows %v (%+v), over the candidates %v (%+v)",
									rows, n, orderIDs(all), allOrder, orderIDs(fromCand), candOrder)
							}
						}
					}
				}
			}
		}
	}
	t.Logf("%d shapes", count)
}

// candidatesOf is what the SQL windows keep: per key, the latest n by
// (event_timestamp, event_id) or by event_id.
func candidatesOf(rows []ResultRow, n int) []ResultRow {
	byTime := slices.Clone(rows)
	slices.SortFunc(byTime, compareStatementTime)
	keep := map[uint64]bool{}
	perKey := map[string]int{}
	for i := len(byTime) - 1; i >= 0; i-- {
		if perKey[byTime[i].PKValues] < n {
			perKey[byTime[i].PKValues]++
			keep[byTime[i].EventID] = true
		}
	}
	byID := slices.Clone(rows)
	slices.SortFunc(byID, func(a, b ResultRow) int { return int(b.EventID) - int(a.EventID) })
	perKey = map[string]int{}
	for _, r := range byID {
		if perKey[r.PKValues] < n {
			perKey[r.PKValues]++
			keep[r.EventID] = true
		}
	}
	var out []ResultRow
	for _, r := range rows {
		if keep[r.EventID] {
			out = append(out, r)
		}
	}
	return out
}

func permutations(n int) [][]int {
	if n == 0 {
		return [][]int{{}}
	}
	var out [][]int
	for _, p := range permutations(n - 1) {
		for i := 0; i <= len(p); i++ {
			q := append(append(slices.Clone(p[:i]), n-1), p[i:]...)
			out = append(out, q)
		}
	}
	return out
}

// binlogOrderProofOnce reads stream_state and index_state once, whatever
// the number of sets it is asked about, and answers per set.
func TestBinlogOrderProofOnce_readsOnce(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("stream_state").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	// A file indexing run that ended 10 minutes after orderT0.
	mock.ExpectQuery("index_state").WillReturnRows(sqlmock.NewRows([]string{"m", "u"}).AddRow(orderT0.Add(10*time.Minute), 0))
	proof := binlogOrderProofOnce(context.Background(), db)
	early := []ResultRow{orderRow(1, "f.000001", 4, orderT0)}
	late := []ResultRow{orderRow(1, "f.000001", 4, orderT0.Add(48*time.Hour))}
	if got := proof(early); got != IDsUnproven {
		t.Fatalf("rows from before the file indexing ended: %v, want IDsUnproven", got)
	}
	if got := proof(late); got != IDsFollowStream {
		t.Fatalf("rows from well after it: %v, want IDsFollowStream", got)
	}
	if got := proof(early); got != IDsUnproven {
		t.Fatalf("asked again: %v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBinlogOrderProofOnce_readFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("stream_state").WillReturnError(errors.New("connection refused"))
	proof := binlogOrderProofOnce(context.Background(), db)
	rows := []ResultRow{orderRow(1, "f.000001", 4, orderT0)}
	for range 2 {
		if got := proof(rows); got != IDsUnproven {
			t.Fatalf("after a failed read: %v, want IDsUnproven", got)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
