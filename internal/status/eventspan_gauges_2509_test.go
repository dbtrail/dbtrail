package status

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// The gauges' span (#2509) never reads a partition's rows: sqlmock refuses
// any statement not expected here, so a MIN/MAX probe or a COUNT(*) fails
// the test by construction.

func edgeByKeySQL(order string) string {
	return regexp.QuoteMeta("SELECT event_timestamp FROM binlog_events FORCE INDEX (PRIMARY) ORDER BY event_id " + order + " LIMIT 1")
}

func gaugesSpan(t *testing.T, mock sqlmock.Sqlmock, db *sql.DB, newest sql.NullTime) CoverageInfo {
	t.Helper()
	var c CoverageInfo
	if err := loadEventSpanForGauges(context.Background(), db, &c, newest); err != nil {
		t.Fatalf("loadEventSpanForGauges: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	return c
}

// The oldest is the lower hour bound of the oldest partition whose estimate
// is not zero (p_2026102210's zero estimate is skipped), the newest is the
// capture's checkpointed event, the count is the estimate; one listing, no
// probe.
func TestLoadEventSpanForGauges_boundsFromEstimatesAndCheckpoint(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectList(mock, spanParts("p_2026102210", 0, "p_2026102211", 5, "p_2026102212", 7, "p_future", 0))
	newest := sql.NullTime{Time: spanNew, Valid: true}
	c := gaugesSpan(t, mock, db, newest)
	if want := time.Date(2026, 10, 22, 11, 0, 0, 0, time.UTC); !c.EarliestEvent.Valid || !c.EarliestEvent.Time.Equal(want) {
		t.Fatalf("earliest = %v, want %v (the oldest partition with an estimate, to the hour)", c.EarliestEvent, want)
	}
	if !c.LatestEvent.Valid || !c.LatestEvent.Time.Equal(spanNew) {
		t.Fatalf("latest = %v, want the checkpointed event %v", c.LatestEvent, spanNew)
	}
	if c.TotalEvents != 12 || !c.TotalEventsEstimated {
		t.Fatalf("count = %d estimated=%v, want 12 estimated", c.TotalEvents, c.TotalEventsEstimated)
	}
}

// No checkpoint (a file-mode index): the newest row by primary key, one row
// per partition merged, never MAX over a partition.
func TestLoadEventSpanForGauges_noCheckpointReadsNewestByKey(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectList(mock, spanParts("p_2026102211", 5, "p_future", 0))
	mock.ExpectQuery(edgeByKeySQL("DESC")).WillReturnRows(spanTS(spanNew))
	c := gaugesSpan(t, mock, db, sql.NullTime{})
	if !c.LatestEvent.Valid || !c.LatestEvent.Time.Equal(spanNew) {
		t.Fatalf("latest = %v, want %v by key", c.LatestEvent, spanNew)
	}
}

// Every estimate zero (statistics not caught up on a young index): the
// oldest row by primary key; when there is none the index is empty and
// nothing else is asked.
func TestLoadEventSpanForGauges_zeroEstimatesReadOldestByKey(t *testing.T) {
	t.Run("a row", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		expectList(mock, spanParts("p_2026102211", 0, "p_future", 0))
		mock.ExpectQuery(edgeByKeySQL("ASC")).WillReturnRows(spanTS(spanOld))
		c := gaugesSpan(t, mock, db, sql.NullTime{Time: spanNew, Valid: true})
		if !c.EarliestEvent.Valid || !c.EarliestEvent.Time.Equal(spanOld) {
			t.Fatalf("earliest = %v, want %v by key", c.EarliestEvent, spanOld)
		}
		if c.TotalEvents != 0 || !c.TotalEventsEstimated {
			t.Fatalf("count = %d estimated=%v, want the zero estimate, never a COUNT(*)", c.TotalEvents, c.TotalEventsEstimated)
		}
	})
	t.Run("empty index", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		expectList(mock, spanParts("p_2026102211", 0, "p_future", 0))
		mock.ExpectQuery(edgeByKeySQL("ASC")).WillReturnError(sql.ErrNoRows)
		c := gaugesSpan(t, mock, db, sql.NullTime{Time: spanNew, Valid: true})
		if c.EarliestEvent.Valid || c.LatestEvent.Valid || c.TotalEvents != 0 {
			t.Fatalf("empty index: %+v", c)
		}
	})
}

// Only p_future holds rows (a stream that never rotated): p_future has no
// hour bound, so the oldest row by key.
func TestLoadEventSpanForGauges_onlyFutureReadsOldestByKey(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectList(mock, spanParts("p_future", 9))
	mock.ExpectQuery(edgeByKeySQL("ASC")).WillReturnRows(spanTS(spanOld))
	c := gaugesSpan(t, mock, db, sql.NullTime{Time: spanNew, Valid: true})
	if !c.EarliestEvent.Valid || !c.EarliestEvent.Time.Equal(spanOld) || c.TotalEvents != 9 {
		t.Fatalf("earliest = %v count = %d, want %v by key and 9", c.EarliestEvent, c.TotalEvents, spanOld)
	}
}

// A table not partitioned by time keeps the exact query: it is small.
func TestLoadEventSpanForGauges_unpartitionedStaysExact(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectList(mock, spanPartsAs("HASH", "event_id", "p0", 3))
	expectFullSpan(mock, 3)
	c := gaugesSpan(t, mock, db, sql.NullTime{})
	if !c.EarliestEvent.Time.Equal(spanOld) || !c.LatestEvent.Time.Equal(spanNew) || c.TotalEvents != 3 || c.TotalEventsEstimated {
		t.Fatalf("unpartitioned: %+v", c)
	}
}
