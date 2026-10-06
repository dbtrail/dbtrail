package query

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// The history of a row whose two latest sets disagree is read whole, from the
// start of the window. A read with no start (the shim's `_flashback`) reaches
// back over the whole retention, so FetchMergedOptions.HistoryRowCap bounds
// how many rows it may buffer: each batch asks for at most what is left plus
// one, and one more than the cap is a *HistoryCapError, not a slower answer.
func TestFetchMerged_historyRowCap(t *testing.T) {
	const f = "binlog.000003"
	cands := []ResultRow{
		keyRow("1", 2, f, 900, orderT0),
		keyRow("1", 1, f, 400, orderT0.Add(2*time.Second)),
	}
	history := []ResultRow{
		keyRow("1", 2, f, 900, orderT0),
		keyRow("1", 1, f, 400, orderT0.Add(2*time.Second)),
		keyRow("1", 0, f, 100, orderT0.Add(-time.Hour)),
	}
	for _, tc := range []struct {
		name    string
		cap     int
		refused bool
	}{
		{"under the cap", 3, false},
		{"over the cap", 2, true},
		{"no cap", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			mock.MatchExpectationsInOrder(false)
			mock.ExpectQuery("OR bt_rn_id <= ?").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
			mock.ExpectQuery("pk_values IN").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
			mock.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			mock.ExpectQuery("FROM index_state").WillReturnRows(sqlmock.NewRows([]string{"m", "u"}).AddRow(nil, 0))
			var historyLimits []int
			fetcher := func(_ context.Context, o Options, _ string) ([]ResultRow, error) {
				if o.LatestPerPKCandidates {
					return slices.Clone(cands), nil
				}
				historyLimits = append(historyLimits, o.Limit)
				return slices.Clone(history), nil
			}
			_, _, err = FetchMerged(context.Background(), db, New(db), FetchMergedOptions{
				Opts:           Options{Schema: "s", Table: "t", LimitPerPK: 1},
				AllowGaps:      true,
				ArchiveFetcher: fetcher,
				SourceResolver: func(context.Context, *sql.DB) ([]string, error) { return []string{"/a/bintrail_id=x"}, nil },
				LatestInBinlog: true,
				HistoryRowCap:  tc.cap,
			})
			var capErr *HistoryCapError
			if tc.refused != errors.As(err, &capErr) {
				t.Fatalf("err = %v, want a *HistoryCapError: %v", err, tc.refused)
			}
			if tc.refused && capErr.Cap != tc.cap {
				t.Fatalf("cap error names %d, want %d", capErr.Cap, tc.cap)
			}
			if !tc.refused && err != nil {
				t.Fatal(err)
			}
			want := 0
			if tc.cap > 0 {
				want = tc.cap + 1
			}
			if len(historyLimits) != 1 || historyLimits[0] != want {
				t.Fatalf("the history read asked for Limit %v, want %d", historyLimits, want)
			}
		})
	}
}

// A row the second read does not find keeps the first read's answer, and
// that is a refusal like any other: counted, and said in the note.
func TestRepickLatestInBinlog_missingRowIsRefused(t *testing.T) {
	const f = "binlog.000002"
	rows := []ResultRow{keyRow("1", 1, f, 400, orderT0), keyRow("9", 4, f, 1100, orderT0)}
	fetch, _ := repickFake(t, map[string]ResultRow{"1": keyRow("1", 1, f, 400, orderT0)}, LatestPerPKOrder{})
	got, order, err := repickLatestInBinlog(Options{LimitPerPK: 1}, rows, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(orderIDs(got), []uint64{1, 4}) {
		t.Fatalf("rows %v", orderIDs(got))
	}
	if order.Refused != 1 || !strings.Contains(order.ReadNote(), "not found by the second read") {
		t.Fatalf("order %+v note %q, want the missing row refused and said", order, order.ReadNote())
	}
}

// Add counts a row refused by two reads once.
func TestLatestPerPKOrder_addCountsDistinctRows(t *testing.T) {
	var o LatestPerPKOrder
	o.Add(RefusedRow("10", "first"))
	o.Add(RefusedRow("10", "second"))
	o.Add(RefusedRow("11", "third"))
	if o.Refused != 2 || !strings.Contains(o.ReadNote(), "First") {
		t.Fatalf("order %+v note %q, want 2 distinct rows and the first reason", o, o.ReadNote())
	}
	o.Add(LatestPerPKOrder{Refused: 1, Sorted: 2})
	if o.Refused != 3 || o.Sorted != 2 {
		t.Fatalf("order %+v: a count without rows adds as it is", o)
	}
}
