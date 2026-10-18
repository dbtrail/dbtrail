package status

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

const spanListSQL = "FROM information_schema.PARTITIONS"

// spanParts lists partitions of a RANGE(TO_SECONDS(event_timestamp)) table,
// as bintrail creates it: name, TABLE_ROWS pairs.
func spanParts(pairs ...any) *sqlmock.Rows {
	return spanPartsAs("RANGE", "to_seconds(`event_timestamp`)", pairs...)
}

func spanPartsAs(method, expr string, pairs ...any) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"PARTITION_NAME", "TABLE_ROWS", "METHOD", "EXPR", "SUB"})
	for i := 0; i < len(pairs); i += 2 {
		r.AddRow(pairs[i], pairs[i+1], method, expr, false)
	}
	return r
}

// expectList expects the partition listing twice, before and after the
// probes, unless after is nil (a test that stops before the second).
func expectList(mock sqlmock.Sqlmock, rows *sqlmock.Rows) *sqlmock.ExpectedQuery {
	return mock.ExpectQuery(regexp.QuoteMeta(spanListSQL)).WillReturnRows(rows)
}

const fullSpanSQL = "SELECT MIN(event_timestamp), MAX(event_timestamp), COUNT(*)"

func expectFullSpan(mock sqlmock.Sqlmock, n int64) {
	mock.ExpectQuery(regexp.QuoteMeta(fullSpanSQL)).
		WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow(spanOld, spanNew, n))
}

func spanProbe(agg, name string) string {
	return regexp.QuoteMeta("SELECT " + agg + "(event_timestamp) FROM binlog_events PARTITION (`" + name + "`)")
}

func spanTS(v any) *sqlmock.Rows { return sqlmock.NewRows([]string{"t"}).AddRow(v) }

var (
	spanOld = time.Date(2026, 10, 18, 19, 58, 2, 0, time.UTC)
	spanNew = time.Date(2026, 10, 18, 20, 59, 35, 0, time.UTC)
)

// The oldest and newest events come from the first and last partition that
// holds a row, asked in partition order; empty partitions on either side are
// skipped by their answer, never by their TABLE_ROWS; the count is the
// estimate and says so; nothing reads the whole table.
func TestLoadEventSpan_readsOnlyTheEdgePartitions(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(spanListSQL)).WillReturnRows(spanParts(
		"p_2026101818", 5, // stale estimate on an empty partition
		"p_2026101819", 0, // estimate 0 on a partition that has rows
		"p_2026101820", 900,
		"p_2026101821", 0,
		"p_future", 0))
	mock.ExpectQuery(spanProbe("MIN", "p_2026101818")).WillReturnRows(spanTS(nil))
	mock.ExpectQuery(spanProbe("MIN", "p_2026101819")).WillReturnRows(spanTS(spanOld))
	mock.ExpectQuery(spanProbe("MAX", "p_future")).WillReturnRows(spanTS(nil))
	mock.ExpectQuery(spanProbe("MAX", "p_2026101821")).WillReturnRows(spanTS(nil))
	mock.ExpectQuery(spanProbe("MAX", "p_2026101820")).WillReturnRows(spanTS(spanNew))
	expectList(mock, spanParts("p_2026101818", 5, "p_2026101819", 0, "p_2026101820", 900, "p_2026101821", 0, "p_future", 0))

	var c CoverageInfo
	if err := loadEventSpan(context.Background(), db, &c); err != nil {
		t.Fatal(err)
	}
	if !c.EarliestEvent.Time.Equal(spanOld) || !c.LatestEvent.Time.Equal(spanNew) {
		t.Errorf("span = %v .. %v, want %v .. %v", c.EarliestEvent, c.LatestEvent, spanOld, spanNew)
	}
	if c.TotalEvents != 905 || !c.TotalEventsEstimated {
		t.Errorf("count = %d estimated=%v, want 905 estimated", c.TotalEvents, c.TotalEventsEstimated)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// Rows only in p_future (no hourly partition added ahead of the data) are
// found from both sides.
func TestLoadEventSpan_rowsOnlyInTheFuturePartition(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(spanListSQL)).WillReturnRows(spanParts("p_2026101800", 0, "p_future", 40))
	mock.ExpectQuery(spanProbe("MIN", "p_2026101800")).WillReturnRows(spanTS(nil))
	mock.ExpectQuery(spanProbe("MIN", "p_future")).WillReturnRows(spanTS(spanOld))
	mock.ExpectQuery(spanProbe("MAX", "p_future")).WillReturnRows(spanTS(spanNew))
	expectList(mock, spanParts("p_2026101800", 0, "p_future", 40))

	var c CoverageInfo
	if err := loadEventSpan(context.Background(), db, &c); err != nil {
		t.Fatal(err)
	}
	if !c.EarliestEvent.Time.Equal(spanOld) || !c.LatestEvent.Time.Equal(spanNew) || c.TotalEvents != 40 {
		t.Errorf("got %+v", c)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// An empty index is an exact zero, whatever the statistics claim, and is
// read from the partitions' answers, not from the count.
func TestLoadEventSpan_emptyIndexIsAnExactZero(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(spanListSQL)).WillReturnRows(spanParts("p_2026101800", 12, "p_future", 0))
	mock.ExpectQuery(spanProbe("MIN", "p_2026101800")).WillReturnRows(spanTS(nil))
	mock.ExpectQuery(spanProbe("MIN", "p_future")).WillReturnRows(spanTS(nil))
	expectList(mock, spanParts("p_2026101800", 12, "p_future", 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM binlog_events LIMIT 1")).WillReturnRows(sqlmock.NewRows([]string{"1"}))

	var c CoverageInfo
	if err := loadEventSpan(context.Background(), db, &c); err != nil {
		t.Fatal(err)
	}
	if c.EarliestEvent.Valid || c.LatestEvent.Valid || c.TotalEvents != 0 || c.TotalEventsEstimated {
		t.Errorf("got %+v, want an empty, exact span", c)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// Statistics that still say 0 on an index with rows are not believed: the
// count is taken exactly instead of reporting 0 beside a real oldest event.
func TestLoadEventSpan_zeroEstimateWithRowsCountsExactly(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(spanListSQL)).WillReturnRows(spanParts("p_2026101819", 0, "p_future", 0))
	mock.ExpectQuery(spanProbe("MIN", "p_2026101819")).WillReturnRows(spanTS(spanOld))
	mock.ExpectQuery(spanProbe("MAX", "p_future")).WillReturnRows(spanTS(nil))
	mock.ExpectQuery(spanProbe("MAX", "p_2026101819")).WillReturnRows(spanTS(spanNew))
	expectList(mock, spanParts("p_2026101819", 0, "p_future", 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM binlog_events")).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(7))

	var c CoverageInfo
	if err := loadEventSpan(context.Background(), db, &c); err != nil {
		t.Fatal(err)
	}
	if c.TotalEvents != 7 || c.TotalEventsEstimated {
		t.Errorf("count = %d estimated=%v, want an exact 7", c.TotalEvents, c.TotalEventsEstimated)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A partition rotation dropped after it was listed means the list is stale:
// the exact full query answers instead of a span read from a table that
// changed under it.
func TestLoadEventSpan_partitionDroppedMeanwhileFallsBackToTheFullQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	gone := &mysql.MySQLError{Number: erUnknownPartition, Message: "Unknown partition 'p_2026101818' in table 'binlog_events'"}
	expectList(mock, spanParts("p_2026101818", 3, "p_2026101819", 9, "p_future", 0))
	mock.ExpectQuery(spanProbe("MIN", "p_2026101818")).WillReturnError(gone)
	mock.ExpectQuery(spanProbe("MIN", "p_2026101819")).WillReturnRows(spanTS(spanOld))
	mock.ExpectQuery(spanProbe("MAX", "p_future")).WillReturnRows(spanTS(nil))
	mock.ExpectQuery(spanProbe("MAX", "p_2026101819")).WillReturnRows(spanTS(spanNew))
	expectList(mock, spanParts("p_2026101819", 9, "p_future", 0))
	expectFullSpan(mock, 9)

	var c CoverageInfo
	if err := loadEventSpan(context.Background(), db, &c); err != nil {
		t.Fatal(err)
	}
	if c.TotalEvents != 9 || c.TotalEventsEstimated || !c.EarliestEvent.Time.Equal(spanOld) || !c.LatestEvent.Time.Equal(spanNew) {
		t.Errorf("got %+v, want the full query's exact span", c)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// Rotation reorganising p_future into new hourly partitions between the
// listing and the probes moves rows into names the list does not have: the
// probes would see an emptied p_future and report an old newest event. A
// list that changed sends the read to the full query.
func TestLoadEventSpan_partitionsReorganisedMeanwhileFallBackToTheFullQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectList(mock, spanParts("p_2026101819", 9, "p_future", 30))
	mock.ExpectQuery(spanProbe("MIN", "p_2026101819")).WillReturnRows(spanTS(spanOld))
	mock.ExpectQuery(spanProbe("MAX", "p_future")).WillReturnRows(spanTS(nil)) // its rows just moved
	mock.ExpectQuery(spanProbe("MAX", "p_2026101819")).WillReturnRows(spanTS(spanOld))
	expectList(mock, spanParts("p_2026101819", 9, "p_2026101820", 30, "p_future", 0))
	expectFullSpan(mock, 39)

	var c CoverageInfo
	if err := loadEventSpan(context.Background(), db, &c); err != nil {
		t.Fatal(err)
	}
	if !c.LatestEvent.Time.Equal(spanNew) || c.TotalEvents != 39 || c.TotalEventsEstimated {
		t.Errorf("got %+v, want the full query's span", c)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// Every partition answering NULL while the table has a row (its rows moved
// to partitions the list missed, and the list then changed back) is never
// reported as an exact empty index.
func TestLoadEventSpan_allProbesEmptyButARowExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectList(mock, spanParts("p_future", 0))
	mock.ExpectQuery(spanProbe("MIN", "p_future")).WillReturnRows(spanTS(nil))
	expectList(mock, spanParts("p_future", 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM binlog_events LIMIT 1")).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	expectFullSpan(mock, 2)

	var c CoverageInfo
	if err := loadEventSpan(context.Background(), db, &c); err != nil {
		t.Fatal(err)
	}
	if c.TotalEvents != 2 || !c.EarliestEvent.Valid {
		t.Errorf("got %+v, want the full query's span", c)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A table partitioned another way (HASH, or RANGE on another column) does not
// keep time in partition order, so the full query answers.
func TestLoadEventSpan_otherLayoutsUseTheFullQuery(t *testing.T) {
	for _, tc := range []struct{ method, expr string }{
		{"HASH", "`event_id`"},
		{"KEY", ""},
		{"RANGE", "`event_id`"},
		{"LIST", "to_seconds(`event_timestamp`)"},
	} {
		t.Run(tc.method+" "+tc.expr, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			expectList(mock, spanPartsAs(tc.method, tc.expr, "p0", 5, "p1", 5))
			expectFullSpan(mock, 10)

			var c CoverageInfo
			if err := loadEventSpan(context.Background(), db, &c); err != nil {
				t.Fatal(err)
			}
			if c.TotalEvents != 10 || c.TotalEventsEstimated {
				t.Errorf("got %+v, want the full query's exact span", c)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// Any other failure is the section's failure, named with the partition.
func TestLoadEventSpan_otherErrorsFail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(spanListSQL)).WillReturnRows(spanParts("p_2026101819", 9))
	mock.ExpectQuery(spanProbe("MIN", "p_2026101819")).WillReturnError(errors.New("connection reset"))

	var c CoverageInfo
	err = loadEventSpan(context.Background(), db, &c)
	if err == nil || !strings.Contains(err.Error(), "p_2026101819") || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("err = %v", err)
	}
}

// A binlog_events without partitions keeps the single exact query.
func TestLoadEventSpan_unpartitionedTableCountsExactly(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectList(mock, spanParts())
	expectFullSpan(mock, 3)

	var c CoverageInfo
	if err := loadEventSpan(context.Background(), db, &c); err != nil {
		t.Fatal(err)
	}
	if c.TotalEvents != 3 || c.TotalEventsEstimated || !c.LatestEvent.Time.Equal(spanNew) {
		t.Errorf("got %+v", c)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The estimate is labelled wherever the total is shown.
func TestCoverageTotalSaysWhenItIsAnEstimate(t *testing.T) {
	for _, est := range []bool{false, true} {
		cov := &CoverageInfo{
			EarliestEvent:        sql.NullTime{Time: spanOld, Valid: true},
			LatestEvent:          sql.NullTime{Time: spanNew, Valid: true},
			TotalEvents:          1200,
			TotalEventsEstimated: est,
		}
		var b strings.Builder
		WriteStatus(&b, nil, nil, nil, cov, nil, nil)
		wantText := "Total events:   1200\n"
		if est {
			wantText = "Total events:   1200 (estimate)\n"
		}
		if !strings.Contains(b.String(), wantText) {
			t.Errorf("estimated=%v: want %q in:\n%s", est, wantText, b.String())
		}
		var j strings.Builder
		if err := WriteStatusJSON(&j, nil, nil, nil, cov, nil, nil); err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(j.String(), `"total_events_estimated": true`) || strings.Contains(j.String(), `"total_events_estimated":true`); got != est {
			t.Errorf("estimated=%v: JSON says so = %v: %s", est, got, j.String())
		}
	}
}
