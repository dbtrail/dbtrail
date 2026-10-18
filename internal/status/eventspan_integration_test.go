//go:build integration

package status_test

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The coverage span on a real partitioned binlog_events: empty hourly
// partitions in front of and behind the data, rows in two hours and in
// p_future. The oldest and newest event are the exact MIN and MAX a full scan
// would give, and the total is the real count when it is a count and a
// positive number when it is the estimate.
func TestLoadCoverage_spanFromTheEdgePartitions_integration(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	base := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	hours := []time.Time{base, base.Add(time.Hour), base.Add(2 * time.Hour), base.Add(3 * time.Hour), base.Add(4 * time.Hour)}
	testutil.SetupPartitionedTable(t, db, dbName, hours)

	cov, err := status.LoadCoverage(context.Background(), db)
	if err != nil {
		t.Fatalf("LoadCoverage on an empty index: %v", err)
	}
	if cov.EarliestEvent.Valid || cov.LatestEvent.Valid || cov.TotalEvents != 0 || cov.TotalEventsEstimated {
		t.Fatalf("empty index: got %+v, want no span and an exact zero", cov)
	}

	// Hour 0 and hour 4 stay empty; rows go to hours 1 and 3 and to p_future.
	at := []string{"2026-03-01 11:20:00", "2026-03-01 11:05:00", "2026-03-01 13:59:59", "2026-03-02 08:00:00"}
	for i, ts := range at {
		testutil.InsertEvent(t, db, "mysql-bin.000001", uint64(100*i+4), uint64(100*i+90), ts, nil,
			"shop", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	}

	cov, err = status.LoadCoverage(context.Background(), db)
	if err != nil {
		t.Fatalf("LoadCoverage: %v", err)
	}
	var wantMin, wantMax time.Time
	var wantN int64
	if err := db.QueryRow("SELECT MIN(event_timestamp), MAX(event_timestamp), COUNT(*) FROM binlog_events").Scan(&wantMin, &wantMax, &wantN); err != nil {
		t.Fatal(err)
	}
	if !cov.EarliestEvent.Valid || !cov.EarliestEvent.Time.Equal(wantMin) {
		t.Errorf("earliest = %v, want %v", cov.EarliestEvent, wantMin)
	}
	if !cov.LatestEvent.Valid || !cov.LatestEvent.Time.Equal(wantMax) {
		t.Errorf("latest = %v, want %v", cov.LatestEvent, wantMax)
	}
	if cov.TotalEventsEstimated {
		if cov.TotalEvents <= 0 {
			t.Errorf("estimated total = %d on an index with %d rows", cov.TotalEvents, wantN)
		}
	} else if cov.TotalEvents != wantN {
		t.Errorf("total = %d, want %d", cov.TotalEvents, wantN)
	}

	// After ANALYZE the statistics hold rows, so the total is the estimate
	// and is marked as one.
	testutil.MustExec(t, db, "ANALYZE TABLE binlog_events")
	cov, err = status.LoadCoverage(context.Background(), db)
	if err != nil {
		t.Fatalf("LoadCoverage after ANALYZE: %v", err)
	}
	if !cov.TotalEventsEstimated || cov.TotalEvents <= 0 {
		t.Errorf("after ANALYZE: total = %d estimated=%v, want a positive estimate", cov.TotalEvents, cov.TotalEventsEstimated)
	}
	if !cov.EarliestEvent.Time.Equal(wantMin) || !cov.LatestEvent.Time.Equal(wantMax) {
		t.Errorf("after ANALYZE the span moved: %v .. %v", cov.EarliestEvent, cov.LatestEvent)
	}
}
