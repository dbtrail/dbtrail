package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// repickLatestInBinlog (#2156): a reader that bounds its keys with a Limit
// (the shim's full-table `_flashback` under its row cap, recover-cascade's
// child candidates) reads each key's latest change by statement time, which
// fixes WHICH keys it reads, and then asks for those keys' latest change in
// binary log order.

func repickFake(t *testing.T, byKey map[string]ResultRow, order LatestPerPKOrder) (func(Options, *LatestPerPKOrder) ([]ResultRow, error), *[]Options) {
	t.Helper()
	var calls []Options
	return func(o Options, latest *LatestPerPKOrder) ([]ResultRow, error) {
		calls = append(calls, o)
		var out []ResultRow
		if len(o.PKValuesIn) == 0 {
			for _, r := range byKey {
				out = append(out, r)
			}
		}
		for _, pk := range o.PKValuesIn {
			if r, ok := byKey[pk]; ok {
				out = append(out, r)
			}
		}
		*latest = order
		return out, nil
	}, &calls
}

func TestRepickLatestInBinlog_eachKeyInItsPlace(t *testing.T) {
	const f = "binlog.000002"
	at := func(s int) time.Time { return orderT0.Add(time.Duration(s) * time.Second) }
	drift := keyRow("", 7, f, 50, at(1))
	rows := []ResultRow{
		keyRow("5", 3, f, 1000, at(3)),
		drift,
		keyRow("1", 1, f, 400, at(2)),  // A: latest by time
		keyRow("9", 4, f, 1100, at(4)), // gone from the second read
	}
	byKey := map[string]ResultRow{
		"1":  keyRow("1", 2, f, 900, at(0)), // B: latest in the binary log
		"5":  keyRow("5", 3, f, 1000, at(3)),
		"77": keyRow("77", 9, f, 1200, at(5)), // a key the first read did not return
	}
	filters := Options{Schema: "s", Table: "t", LimitPerPK: 1, Order: "DESC", PKValues: "x",
		ColumnEq: []ColumnEq{{Column: "parent_id", Value: "1"}}}
	for _, tc := range []struct {
		name  string
		limit int
		keyed bool // the second read names the keys
	}{
		{"no limit: one read of the window", 0, false},
		{"under the limit: one read of the window", 5, false},
		{"at the limit: the keys are named", 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fetch, calls := repickFake(t, byKey, LatestPerPKOrder{Disagreed: 1, Sorted: 1})
			opts := filters
			opts.Limit = tc.limit
			got, order, err := repickLatestInBinlog(opts, rows, fetch)
			if err != nil {
				t.Fatal(err)
			}
			if ids := orderIDs(got); !slices.Equal(ids, []uint64{3, 7, 2, 4}) {
				t.Fatalf("rows %v, want [3 7 2 4]: each key replaced in its own place, the drift row and a key the second read missed kept, no key added", ids)
			}
			if orderIDs(rows)[2] != 1 {
				t.Fatal("the caller's rows were modified")
			}
			if order.Sorted != 1 || order.Disagreed != 1 {
				t.Fatalf("order %+v", order)
			}
			if len(*calls) != 1 {
				t.Fatalf("%d reads, want 1", len(*calls))
			}
			c := (*calls)[0]
			if c.Limit != 0 || c.Order != "" || c.LimitPerPK != 1 || len(c.ColumnEq) != 1 || c.Schema != "s" || c.Table != "t" {
				t.Fatalf("second read options %+v: want no Limit, the other filters kept", c)
			}
			if tc.keyed {
				if !slices.Equal(c.PKValuesIn, []string{"5", "1", "9"}) || c.PKValues != "" {
					t.Fatalf("keyed read: PKValues %q PKValuesIn %v, want the keys read", c.PKValues, c.PKValuesIn)
				}
			} else if c.PKValuesIn != nil || c.PKValues != "x" {
				t.Fatalf("window read: PKValues %q PKValuesIn %v, want the first read's own filter", c.PKValues, c.PKValuesIn)
			}
		})
	}
}

func TestRepickLatestInBinlog_batchesAndSumsTheOrder(t *testing.T) {
	var rows []ResultRow
	for i := range 1201 {
		rows = append(rows, keyRow(fmt.Sprint(i), uint64(i+1), "binlog.000001", uint64(10*(i+1)), orderT0))
	}
	fetch, calls := repickFake(t, nil, LatestPerPKOrder{Disagreed: 2, Sorted: 1, Refused: 1, warning: "w"})
	got, order, err := repickLatestInBinlog(Options{LimitPerPK: 1, Limit: len(rows)}, rows, fetch)
	if err != nil {
		t.Fatal(err)
	}
	var sizes []int
	for _, c := range *calls {
		sizes = append(sizes, len(c.PKValuesIn))
	}
	if !slices.Equal(sizes, []int{500, 500, 201}) {
		t.Fatalf("batch sizes %v", sizes)
	}
	if order.Disagreed != 6 || order.Sorted != 3 || order.Refused != 3 || order.warning != "w" {
		t.Fatalf("order %+v, want the three reads summed", order)
	}
	if !slices.Equal(orderIDs(got), orderIDs(rows)) {
		t.Fatal("rows changed though the reads found nothing")
	}
}

func TestRepickLatestInBinlog_refusals(t *testing.T) {
	fetch, calls := repickFake(t, nil, LatestPerPKOrder{})
	if _, _, err := repickLatestInBinlog(Options{LimitPerPK: 2}, []ResultRow{keyRow("1", 1, "", 0, orderT0)}, fetch); err == nil {
		t.Fatal("LimitPerPK 2 was accepted")
	}
	if got, _, err := repickLatestInBinlog(Options{LimitPerPK: 1}, nil, fetch); err != nil || got != nil || len(*calls) != 0 {
		t.Fatalf("no rows: (%v, %v), %d reads", got, err, len(*calls))
	}
	boom := errors.New("boom")
	_, _, err := repickLatestInBinlog(Options{LimitPerPK: 1}, []ResultRow{keyRow("1", 1, "", 0, orderT0)},
		func(Options, *LatestPerPKOrder) ([]ResultRow, error) { return nil, boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

// The MergedFetcher's repick reads like its Fetch (archives included, an
// archive it cannot read is an error) and asks every source for the named
// keys' candidates.
func TestMergedFetcher_repickLatestInBinlog(t *testing.T) {
	const f = "binlog.000003"
	archived := []ResultRow{
		keyRow("1", 2, f, 900, orderT0),
		keyRow("1", 1, f, 400, orderT0.Add(2*time.Second)),
	}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(want, got string) error {
		if !strings.Contains(got, want) {
			return errMismatch(want, got)
		}
		return nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("OR bt_rn_id <= ?").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
	mock.ExpectQuery("pk_values IN").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
	mock.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery("FROM index_state").WillReturnRows(sqlmock.NewRows([]string{"m", "u"}).AddRow(nil, 0))
	var asked [][]string
	m := &MergedFetcher{DB: db, Engine: New(db),
		ArchiveFetcher: func(_ context.Context, o Options, _ string) ([]ResultRow, error) {
			asked = append(asked, o.PKValuesIn)
			return slices.Clone(archived), nil
		},
		SourceResolver: func(context.Context, *sql.DB) ([]string, error) { return []string{"/a/bintrail_id=x"}, nil },
	}
	in := []ResultRow{keyRow("1", 1, f, 400, orderT0.Add(2*time.Second))}
	got, order, err := m.RepickLatestInBinlog(context.Background(), Options{Schema: "s", Table: "t", LimitPerPK: 1, Limit: 1}, in)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(orderIDs(got), []uint64{2}) || order.Sorted != 1 {
		t.Fatalf("rows %v order %+v, want event 2 taken in binary log order", orderIDs(got), order)
	}
	if len(asked) == 0 || !slices.Equal(asked[0], []string{"1"}) {
		t.Fatalf("the archive was asked for %v", asked)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	unreadable := &MergedFetcher{DB: db, Engine: New(db),
		ArchiveFetcher: func(context.Context, Options, string) ([]ResultRow, error) { return nil, errors.New("gone") },
		SourceResolver: func(context.Context, *sql.DB) ([]string, error) { return []string{"/a/bintrail_id=x"}, nil },
	}
	mock.ExpectQuery("OR bt_rn_id <= ?").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
	var are *ArchiveReadError
	if _, _, err := unreadable.RepickLatestInBinlog(context.Background(), Options{Schema: "s", Table: "t", LimitPerPK: 1}, in); !errors.As(err, &are) {
		t.Fatalf("an unreadable archive: err = %v, want *ArchiveReadError", err)
	}
}
