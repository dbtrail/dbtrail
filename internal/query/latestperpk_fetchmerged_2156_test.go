package query

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// FetchMerged under LatestInBinlog (#2156): the fetch asks every source for
// candidates, the merge does not trim them by time, and the pick is made over
// the merged set. The control runs the same data without the flag: the latest
// change by statement time, as before.
func TestFetchMerged_latestInBinlog(t *testing.T) {
	const f = "binlog.000003"
	archived := []ResultRow{
		// Row 1: B (id 2) started 2 s before A (id 1) and committed after it.
		keyRow("1", 2, f, 900, orderT0),
		keyRow("1", 1, f, 400, orderT0.Add(2*time.Second)),
		keyRow("5", 3, f, 1000, orderT0.Add(3*time.Second)),
	}
	for _, tc := range []struct {
		name      string
		flag      bool
		order     string
		wantSQL   string // a fragment the live query must hold
		noSQL     string // and one it must not
		wantIDs   []uint64
		wantOrder LatestPerPKOrder
	}{
		{"binary log order", true, "", "OR bt_rn_id <= ?", "", []uint64{2, 3}, LatestPerPKOrder{Disagreed: 1, Sorted: 1}},
		{"binary log order, newest first", true, "DESC", "OR bt_rn_id <= ?", "", []uint64{3, 2}, LatestPerPKOrder{Disagreed: 1, Sorted: 1}},
		{"without the flag: statement time", false, "", "bt_rn <= ?)", "bt_rn_id", []uint64{1, 3}, LatestPerPKOrder{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(want, got string) error {
				if !strings.Contains(got, want) || (tc.noSQL != "" && strings.Contains(got, tc.noSQL)) {
					return errMismatch(want, got)
				}
				return nil
			})))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery(tc.wantSQL).WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
			if tc.flag {
				// Row 1's candidates disagree: its history is read, from
				// the index and the archive, before the index is asked.
				mock.ExpectQuery("pk_values IN").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
				mock.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
				mock.ExpectQuery("FROM index_state").WillReturnRows(sqlmock.NewRows([]string{"m", "u"}).AddRow(nil, 0))
			}
			var asked []bool
			fetcher := func(_ context.Context, o Options, _ string) ([]ResultRow, error) {
				asked = append(asked, o.LatestPerPKCandidates)
				return slices.Clone(archived), nil
			}
			var order LatestPerPKOrder
			rows, _, err := FetchMerged(context.Background(), db, New(db), FetchMergedOptions{
				Opts:           Options{Schema: "s", Table: "t", LimitPerPK: 1, Order: tc.order},
				AllowGaps:      true,
				ArchiveFetcher: fetcher,
				SourceResolver: func(context.Context, *sql.DB) ([]string, error) { return []string{"/a/bintrail_id=x"}, nil },
				LatestInBinlog: tc.flag,
				LatestOrder:    &order,
			})
			if err != nil {
				t.Fatalf("FetchMerged: %v", err)
			}
			if !slices.Equal(orderIDs(rows), tc.wantIDs) {
				t.Fatalf("rows %v, want %v", orderIDs(rows), tc.wantIDs)
			}
			order.warning = ""
			if order != tc.wantOrder {
				t.Fatalf("order %+v, want %+v", order, tc.wantOrder)
			}
			want := []bool{tc.flag}
			if tc.flag {
				want = append(want, false) // the history read
			}
			if !slices.Equal(asked, want) {
				t.Fatalf("the archive was asked for candidates: %v, want %v", asked, want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFetchMerged_latestInBinlogNeedsLimitPerPKAndNoLimit(t *testing.T) {
	for _, opts := range []Options{{}, {LimitPerPK: 1, Limit: 10}} {
		_, _, err := FetchMerged(context.Background(), nil, New(nil), FetchMergedOptions{
			Opts: opts, NoArchive: true, AllowGaps: true, LatestInBinlog: true,
		})
		if err == nil || !strings.Contains(err.Error(), "LatestInBinlog requires LimitPerPK > 0 and no Limit") {
			t.Fatalf("opts %+v: err = %v", opts, err)
		}
	}
}

type mismatchErr struct{ want, got string }

func (e mismatchErr) Error() string { return "query does not hold " + e.want + ": " + e.got }

func errMismatch(want, got string) error { return mismatchErr{want, got} }
