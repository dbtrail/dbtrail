package query

import (
	"context"
	"errors"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// orderRow is one change for the tests below: where it is in the binary log,
// which id the index gave it, and when its statement started.
func orderRow(id uint64, file string, pos uint64, at time.Time) ResultRow {
	return ResultRow{EventID: id, BinlogFile: file, StartPos: pos, EventTimestamp: at}
}

func orderIDs(rows []ResultRow) []uint64 {
	out := make([]uint64, len(rows))
	for i := range rows {
		out[i] = rows[i].EventID
	}
	return out
}

func proven([]ResultRow) IDProof    { return IDsFollowStream }
func unproven([]ResultRow) IDProof  { return IDsUnproven }
func filesOnly([]ResultRow) IDProof { return IDsFollowFileIndexing }

var orderT0 = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

// lockWait is #2151's first shape in the order a fetch returns it: B (id 2)
// started 2 s before A (id 1) and is after it in the binary log.
func lockWait(file string) []ResultRow {
	return []ResultRow{
		orderRow(2, file, 900, orderT0),
		orderRow(1, file, 400, orderT0.Add(2*time.Second)),
	}
}

// The decision table of OrderByBinlog, one row per case (#2156).
func TestOrderByBinlog_decisionTable(t *testing.T) {
	const f = "binlog.000007"
	for _, tc := range []struct {
		name    string
		rows    []ResultRow
		proof   func([]ResultRow) IDProof
		want    BinlogOrderReason
		wantIDs []uint64
		warns   string // a fragment the warning must hold; "" = no warning
	}{
		{"no rows", nil, proven, OrderAgrees, []uint64{}, ""},
		{"one row", []ResultRow{orderRow(1, f, 4, orderT0)}, proven, OrderAgrees, []uint64{1}, ""},
		{"one row without a coordinate", []ResultRow{orderRow(1, "", 0, orderT0)}, proven, OrderAgrees, []uint64{1}, ""},
		{
			"the lock wait on an index a stream writes: binlog order",
			lockWait(f), proven, OrderSorted, []uint64{1, 2}, "",
		},
		{
			"the two orders agree: nothing is asked, nothing changes",
			[]ResultRow{orderRow(1, f, 400, orderT0), orderRow(2, f, 900, orderT0.Add(time.Second))},
			func([]ResultRow) IDProof { panic("the index must not be asked when the orders agree") },
			OrderAgrees, []uint64{1, 2}, "",
		},
		{
			"a row with no file",
			[]ResultRow{orderRow(2, "", 0, orderT0), orderRow(1, f, 400, orderT0.Add(2*time.Second))},
			proven, OrderNoCoordinate, []uint64{2, 1}, "1 of 2 changes carry no binary log position",
		},
		{
			"a start position that underflowed (MariaDB 11.4 before #1180)",
			[]ResultRow{orderRow(2, f, 1<<64-31, orderT0), orderRow(1, f, 400, orderT0.Add(2*time.Second))},
			proven, OrderNoCoordinate, []uint64{2, 1}, "carry no binary log position",
		},
		{
			"PostgreSQL: every file is an LSN",
			[]ResultRow{orderRow(2, "1/5", 5, orderT0), orderRow(1, "0/FFFFFFFF", 9, orderT0.Add(2*time.Second))},
			func([]ResultRow) IDProof { panic("a PostgreSQL set must not ask the index") },
			OrderPostgres, []uint64{2, 1}, "",
		},
		{
			"an LSN among binlog files",
			[]ResultRow{orderRow(2, "1/5", 5, orderT0), orderRow(1, f, 400, orderT0.Add(2*time.Second))},
			proven, OrderNoCoordinate, []uint64{2, 1}, "1 of 2 changes carry no binary log position",
		},
		{
			"two base names (the source was replaced)",
			[]ResultRow{orderRow(1, "old-bin.000090", 400, orderT0), orderRow(2, "new-bin.000001", 900, orderT0.Add(time.Hour))},
			proven, OrderSeveralBaseNames, []uint64{1, 2}, "different names (new-bin, old-bin)",
		},
		{
			"an index whose ids do not follow the binary log",
			lockWait(f), unproven, OrderIDsUnproven, []uint64{2, 1}, "cannot show which one is right",
		},
		{
			"no way to ask the index",
			lockWait(f), nil, OrderIDsUnproven, []uint64{2, 1}, "cannot show which one is right",
		},
		{
			// RESET MASTER between id 1 and id 2: the newer change is in a
			// LOWER file. Time order (1 then 2) is right, position order is
			// not, and the ids show it.
			"the numbering restarted inside the window (RESET MASTER)",
			[]ResultRow{orderRow(1, "binlog.000050", 400, orderT0), orderRow(2, "binlog.000001", 300, orderT0.Add(time.Hour))},
			proven, OrderRenumbered, []uint64{1, 2}, "event 1 is at binlog.000050:400 and the next one indexed, event 2, is at binlog.000001:300",
		},
		{
			// The same restart landing on the SAME file name: only the
			// position goes down, so the file alone would not show it.
			"the numbering restarted onto the same file name",
			[]ResultRow{orderRow(1, "binlog.000001", 9000, orderT0), orderRow(2, "binlog.000001", 300, orderT0.Add(time.Hour))},
			proven, OrderRenumbered, []uint64{1, 2}, "binary log position goes down",
		},
		{
			// A restart AND a lock wait after it: one inversion is enough to
			// refuse the whole set.
			"a restart and a lock wait in one window",
			[]ResultRow{
				orderRow(1, "binlog.000050", 400, orderT0),
				orderRow(3, "binlog.000001", 900, orderT0.Add(time.Hour)),
				orderRow(2, "binlog.000001", 300, orderT0.Add(time.Hour+2*time.Second)),
			},
			proven, OrderRenumbered, []uint64{1, 3, 2}, "binary log position goes down",
		},
		{
			// An index built ONLY with `bintrail index`, from a directory
			// holding the old source's binlog.000045 and the new source's
			// binlog.000001 (a failover, RESET MASTER): `--all` takes files by
			// name, so the NEW file was indexed first. Ids and positions rise
			// together and both are backwards; the times, 20 minutes apart,
			// are right. Nothing the index holds tells this from one
			// numbering, so a file-only index never vouches across files.
			"file-only index: a newer numbering's file indexed before the older one's",
			[]ResultRow{orderRow(50, "binlog.000045", 400, orderT0), orderRow(1, "binlog.000001", 900, orderT0.Add(20*time.Minute))},
			filesOnly, OrderIDsUnproven, []uint64{50, 1}, "built with `bintrail index` only and these changes are in 2 binary log files",
		},
		{
			// The same index, one numbering, a lock wait across a rotation:
			// refused as well. It cannot be told from the case above.
			"file-only index: a lock wait across a file rotation",
			[]ResultRow{orderRow(2, "binlog.000008", 4, orderT0), orderRow(1, "binlog.000007", 900, orderT0.Add(2*time.Second))},
			filesOnly, OrderIDsUnproven, []uint64{2, 1}, "these changes are in 2 binary log files",
		},
		{
			// Inside ONE file a file-only index does vouch: `bintrail index`
			// writes a file's changes in the order the file holds them.
			"file-only index: the lock wait inside one file",
			lockWait(f), filesOnly, OrderSorted, []uint64{1, 2}, "",
		},
		{
			// And the id walk still guards that one file: the same name
			// indexed twice with the position going back.
			"file-only index: one file name, the position goes down by id",
			[]ResultRow{orderRow(1, f, 9000, orderT0), orderRow(2, f, 300, orderT0.Add(time.Hour))},
			filesOnly, OrderRenumbered, []uint64{1, 2}, "binary log position goes down",
		},
		{
			// A lock wait across a rotation on an index a stream writes: A is
			// the last change of file 7, B the first of file 8, B started 2 s
			// earlier. The stream read them in that order.
			"a lock wait across a file rotation",
			[]ResultRow{orderRow(2, "binlog.000008", 4, orderT0), orderRow(1, "binlog.000007", 900, orderT0.Add(2*time.Second))},
			proven, OrderSorted, []uint64{1, 2}, "",
		},
		{
			// The .999999 to .1000000 rollover (#840): the longer name is the
			// later file, where plain string order says the opposite.
			"across the six-digit rollover",
			[]ResultRow{orderRow(2, "binlog.1000000", 4, orderT0), orderRow(1, "binlog.999999", 400, orderT0.Add(2*time.Second))},
			proven, OrderSorted, []uint64{1, 2}, "",
		},
		{
			// One compressed transaction: every row event carries the payload
			// event's coordinate, and the higher id is the later statement.
			"the same coordinate: the id decides",
			[]ResultRow{orderRow(3, f, 400, orderT0), orderRow(2, f, 400, orderT0), orderRow(1, f, 100, orderT0.Add(time.Second))},
			proven, OrderSorted, []uint64{1, 2, 3}, "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := slices.Clone(tc.rows)
			got := OrderByBinlog(rows, tc.proof)
			if got.Reason != tc.want {
				t.Fatalf("reason = %d, want %d (warning: %q)", got.Reason, tc.want, got.Warning())
			}
			if ids := orderIDs(rows); !slices.Equal(ids, tc.wantIDs) {
				t.Fatalf("order = %v, want %v", ids, tc.wantIDs)
			}
			warn := got.Warning()
			if (tc.warns == "") != (warn == "") {
				t.Fatalf("warning = %q, want one holding %q", warn, tc.warns)
			}
			if !strings.Contains(warn, tc.warns) {
				t.Fatalf("warning = %q, want it to hold %q", warn, tc.warns)
			}
			if warn != "" && !strings.Contains(warn, "in the order their statements started, as before") {
				t.Fatalf("warning does not say which order was kept: %q", warn)
			}
			if got.Sorted() != (tc.want == OrderSorted) {
				t.Fatalf("Sorted() = %v for reason %d", got.Sorted(), got.Reason)
			}
		})
	}
}

// No path leaves a third order: over random sets, with and without usable
// coordinates and with either answer from the index, the rows either stay
// exactly as they came, or are in ascending event_id order with no pair that
// LaterInBinlog contradicts. And an unchanged set that is not in binlog order
// always says why.
func TestOrderByBinlog_neverAThirdOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(2156))
	files := []string{"binlog.000001", "binlog.000002", "binlog.999999", "binlog.1000000", "other.000001", "", "0/16B3748"}
	sortedSeen, refusedSeen := 0, 0
	for trial := range 4000 {
		n := 2 + rng.Intn(7)
		pool := files
		if trial%2 == 0 {
			pool = files[:2] // one base name, all with coordinates: the sets that can sort
		}
		rows := make([]ResultRow, n)
		for i := range rows {
			pos := uint64(rng.Intn(4)) * 100
			if rng.Intn(40) == 0 {
				pos = 1<<64 - 31
			}
			rows[i] = orderRow(uint64(i+1), pool[rng.Intn(len(pool))], pos, orderT0.Add(time.Duration(rng.Intn(5))*time.Second))
		}
		if trial%4 == 0 {
			// A set as a stream writes it: ids follow the position.
			slices.SortFunc(rows, func(a, b ResultRow) int { return compareBinlogCoordinate(&a, &b) })
			for i := range rows {
				rows[i].EventID = uint64(i + 1)
			}
		}
		// The order a fetch returns.
		slices.SortStableFunc(rows, func(a, b ResultRow) int {
			if c := a.EventTimestamp.Compare(b.EventTimestamp); c != 0 {
				return c
			}
			return int(a.EventID) - int(b.EventID)
		})
		before := orderIDs(rows)
		answer := []IDProof{IDsUnproven, IDsFollowStream, IDsFollowStream, IDsFollowFileIndexing}[rng.Intn(4)]
		got := OrderByBinlog(rows, func([]ResultRow) IDProof { return answer })
		after := orderIDs(rows)

		if !got.Sorted() {
			if !slices.Equal(before, after) {
				t.Fatalf("trial %d: reason %d moved rows: %v to %v", trial, got.Reason, before, after)
			}
			if got.Reason != OrderAgrees && got.Reason != OrderPostgres {
				refusedSeen++
				if got.Warning() == "" {
					t.Fatalf("trial %d: reason %d kept the fetched order and says nothing", trial, got.Reason)
				}
			}
			continue
		}
		sortedSeen++
		if answer == IDsUnproven {
			t.Fatalf("trial %d: sorted on an index that did not confirm its ids", trial)
		}
		if answer == IDsFollowFileIndexing && distinctBinlogFiles(rows) > 1 {
			t.Fatalf("trial %d: sorted across %d files on an index built with `bintrail index` only", trial, distinctBinlogFiles(rows))
		}
		if !slices.IsSorted(after) {
			t.Fatalf("trial %d: sorted, and the ids are not ascending: %v", trial, after)
		}
		for i := range rows {
			for j := i + 1; j < len(rows); j++ {
				if LaterInBinlog(&rows[i], &rows[j]) {
					t.Fatalf("trial %d: row %d is before row %d and LaterInBinlog says it is after", trial, rows[i].EventID, rows[j].EventID)
				}
			}
		}
	}
	if sortedSeen < 100 || refusedSeen < 100 {
		t.Fatalf("the generator exercised %d sorts and %d refusals; it tests too little", sortedSeen, refusedSeen)
	}
}

// Note is said only for a reordered set.
func TestBinlogOrderNote(t *testing.T) {
	const f = "binlog.000007"
	rows := lockWait(f)
	if got := OrderByBinlog(slices.Clone(rows), unproven).Note(); got != "" {
		t.Fatalf("a refusal carries the reordered note: %q", got)
	}
	if got := OrderByBinlog(slices.Clone(rows), proven).Note(); !strings.Contains(got, "2 of 2 changes were written to the binary log in a different order") {
		t.Fatalf("note = %q", got)
	}
}

// compareBinlogCoordinate is a total order over rows with coordinates:
// antisymmetric and transitive, and for any two rows it says what
// LaterInBinlog says. A comparison without those properties makes a sort
// misorder without failing.
func TestCompareBinlogCoordinate_isATotalOrderAndIsLaterInBinlog(t *testing.T) {
	var rows []ResultRow
	id := uint64(0)
	for _, f := range []string{"b.000001", "b.000002", "b.999999", "b.1000000", "bb.000001", "a.000009"} {
		for _, pos := range []uint64{4, 4, 500} {
			id++
			rows = append(rows, orderRow(id, f, pos, orderT0))
		}
	}
	for i := range rows {
		for j := range rows {
			a, b := &rows[i], &rows[j]
			c := compareBinlogCoordinate(a, b)
			if c != -compareBinlogCoordinate(b, a) {
				t.Fatalf("not antisymmetric for %d and %d", a.EventID, b.EventID)
			}
			if (c > 0) != LaterInBinlog(a, b) {
				t.Fatalf("compare(%d, %d) = %d and LaterInBinlog = %v", a.EventID, b.EventID, c, LaterInBinlog(a, b))
			}
			if (c == 0) != (i == j) {
				t.Fatalf("compare(%d, %d) = 0 for two different rows, or not 0 for one", a.EventID, b.EventID)
			}
			for k := range rows {
				if c < 0 && compareBinlogCoordinate(b, &rows[k]) < 0 && compareBinlogCoordinate(a, &rows[k]) >= 0 {
					t.Fatalf("not transitive over %d, %d, %d", a.EventID, b.EventID, rows[k].EventID)
				}
			}
		}
	}
}

func TestBinlogBaseName(t *testing.T) {
	for in, want := range map[string]string{
		"binlog.000042":          "binlog",
		"mysql-bin.1000000":      "mysql-bin",
		"mariadb-bin.000001":     "mariadb-bin",
		"host.example.com.00012": "host.example.com",
		"nosuffix":               "nosuffix",
		"trailingdot.":           "trailingdot.",
		"binlog.00a042":          "binlog.00a042",
		"":                       "",
	} {
		if got := binlogBaseName(in); got != want {
			t.Errorf("binlogBaseName(%q) = %q, want %q", in, got, want)
		}
	}
}

// writersKeepBinlogOrder: which indexes hand out ids in binlog order.
func TestWritersKeepBinlogOrder(t *testing.T) {
	since := orderT0
	for _, tc := range []struct {
		name            string
		stream, running bool
		lastFile        time.Time
		want            bool
	}{
		{"a stream alone", true, false, time.Time{}, true},
		{"files alone", false, false, since.Add(time.Minute), true},
		{"files alone, a run still open", false, true, time.Time{}, true},
		{"a stream, and a file run still open", true, true, time.Time{}, false},
		{"a stream, and files indexed after these rows", true, false, since.Add(time.Minute), false},
		{"a stream, and files indexed just before these rows", true, false, since.Add(-30 * time.Minute), false},
		{"a stream, and files indexed long before these rows", true, false, since.Add(-2 * time.Hour), true},
	} {
		if got := writersKeepBinlogOrder(tc.stream, tc.running, tc.lastFile, since); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

// IDsFollowBinlog against the two tables it reads, and BinlogOrderProof's
// answer when a read fails: false, which keeps the fetched order and warns.
func TestIDsFollowBinlog(t *testing.T) {
	ctx := context.Background()
	if got, err := IDsFollowBinlog(ctx, nil, orderT0); got != IDsUnproven || err != nil {
		t.Fatalf("no index: %v, %v; want IDsUnproven, nil", got, err)
	}
	const stream = "SELECT 1 FROM stream_state WHERE id = 1"
	const state = "SELECT MAX\\(completed_at\\), COUNT\\(\\*\\) - COUNT\\(completed_at\\) FROM index_state"
	for _, tc := range []struct {
		name   string
		expect func(sqlmock.Sqlmock)
		want   IDProof
		fails  bool
	}{
		{"a stream, no file ever indexed", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(stream).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			m.ExpectQuery(state).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow(nil, 0))
		}, IDsFollowStream, false},
		{"a stream, files indexed after the rows", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(stream).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			m.ExpectQuery(state).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow(orderT0.Add(time.Hour), 0))
		}, IDsUnproven, false},
		// Half an hour before the EARLIEST of the rows BinlogOrderProof is
		// given below (and an hour and a half before the latest): too close.
		{"a stream, files indexed just before the earliest row", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(stream).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			m.ExpectQuery(state).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow(orderT0.Add(-30*time.Minute), 0))
		}, IDsUnproven, false},
		{"a stream, files indexed long before the earliest row", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(stream).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			m.ExpectQuery(state).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow(orderT0.Add(-2*time.Hour), 0))
		}, IDsFollowStream, false},
		{"a stream, a file run still open", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(stream).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			m.ExpectQuery(state).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow(nil, 1))
		}, IDsUnproven, false},
		// No stream: index_state is not even read. Whatever it holds, it
		// cannot show the files were indexed in the order the source wrote them.
		{"files only", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(stream).WillReturnRows(sqlmock.NewRows([]string{"1"}))
		}, IDsFollowFileIndexing, false},
		{"a stream, and no index_state table", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(stream).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			m.ExpectQuery(state).WillReturnError(&mysql.MySQLError{Number: 1146})
		}, IDsFollowStream, false},
		{"stream_state cannot be read", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(stream).WillReturnError(errors.New("connection refused"))
		}, IDsUnproven, true},
		{"index_state cannot be read", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(stream).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			m.ExpectQuery(state).WillReturnError(errors.New("connection refused"))
		}, IDsUnproven, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tc.expect(mock)
			got, err := IDsFollowBinlog(ctx, db, orderT0)
			if got != tc.want || (err != nil) != tc.fails {
				t.Fatalf("IDsFollowBinlog = %v, %v; want %v, error %v", got, err, tc.want, tc.fails)
			}

			// The same through BinlogOrderProof, with the earliest row time.
			tc.expect(mock)
			rows := []ResultRow{orderRow(1, "b.000001", 4, orderT0.Add(time.Hour)), orderRow(2, "b.000001", 9, orderT0)}
			if got := BinlogOrderProof(ctx, db)(rows); got != tc.want {
				t.Fatalf("BinlogOrderProof = %v, want %v", got, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if BinlogOrderProof(ctx, nil)(nil) != IDsUnproven {
		t.Fatal("BinlogOrderProof confirmed the order of no rows")
	}
}
