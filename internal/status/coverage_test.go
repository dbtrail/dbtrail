package status

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func TestContinuityStatus(t *testing.T) {
	if got := ContinuityStatus(nil, errors.New("boom")); got != "unavailable" {
		t.Fatalf("read failure = %q, want unavailable", got)
	}
	if got := ContinuityStatus(nil, nil); got != "none" {
		t.Fatalf("no stream row = %q, want none", got)
	}
	if got := ContinuityStatus(&StreamStateInfo{GapColumnsPresent: false}, nil); got != "unknown" {
		t.Fatalf("legacy index = %q, want unknown", got)
	}
	gapped := &StreamStateInfo{GapColumnsPresent: true, GapLostAt: sql.NullTime{Time: time.Now(), Valid: true}}
	if got := ContinuityStatus(gapped, nil); got != "gap_lost" {
		t.Fatalf("stamped gap = %q, want gap_lost", got)
	}
	if got := ContinuityStatus(&StreamStateInfo{GapColumnsPresent: true}, nil); got != "ok" {
		t.Fatalf("clean stream = %q, want ok", got)
	}
}

// Query shapes shared by the CollectCoverageSummary tests. The summary hits,
// in order: partition listing (floor) → archive_state MIN/MAX/sources → partition
// listing (walk) → per-partition MAX probes (newest first, p_future first) →
// stream_state.
var (
	covPartsQ  = regexp.QuoteMeta("SELECT PARTITION_NAME FROM information_schema.PARTITIONS")
	covArchQ   = regexp.QuoteMeta("SELECT MIN(partition_name), MAX(partition_name), COUNT(DISTINCT bintrail_id) FROM archive_state")
	covProbeQ  = `MAX\(event_timestamp\) FROM binlog_events PARTITION`
	covStreamQ = "FROM stream_state"
)

var covStreamCols = []string{"mode", "binlog_file", "binlog_position", "gtid_set",
	"events_indexed", "last_event_time", "last_checkpoint",
	"server_id", "bintrail_id", "gap_lost_at", "gap_lost_detail"}

func covPartRows(names ...string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"PARTITION_NAME"})
	for _, n := range names {
		r.AddRow(n)
	}
	return r
}

func covProbeRow(ts any) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"max"}).AddRow(ts)
}

func TestCollectCoverageSummary(t *testing.T) {
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db, mock
	}
	latest := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	now := latest.Add(42 * time.Second)
	// Standard prologue: floor = p_2026080110; walk probes p_future (empty)
	// then p_2026080110 (holds latest).
	expectWindow := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110", "p_future"))
		mock.ExpectQuery(covArchQ).WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(nil, nil, 0))
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110", "p_future"))
		mock.ExpectQuery(covProbeQ).WillReturnRows(covProbeRow(nil)) // p_future first, empty
		mock.ExpectQuery(covProbeQ).WillReturnRows(covProbeRow(latest))
	}

	t.Run("streaming index: window, lag, ok", func(t *testing.T) {
		db, mock := newDB(t)
		expectWindow(mock)
		mock.ExpectQuery(covStreamQ).WillReturnRows(sqlmock.NewRows(covStreamCols).
			AddRow("gtid", "", 0, "uuid:1-9", 100, latest, latest, 7, "id-1", nil, nil))
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil {
			t.Fatal(err)
		}
		if !sum.Floor.Hour.Equal(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)) || !sum.DeltaTo.Equal(latest) {
			t.Fatalf("window = [%v, %v]", sum.Floor.Hour, sum.DeltaTo)
		}
		if sum.Continuity != "ok" || sum.LagSeconds == nil || *sum.LagSeconds != 42 {
			t.Fatalf("continuity=%q lag=%v", sum.Continuity, sum.LagSeconds)
		}
	})

	t.Run("file-mode index: no stream row, no lag claim", func(t *testing.T) {
		db, mock := newDB(t)
		expectWindow(mock)
		mock.ExpectQuery(covStreamQ).WillReturnError(sql.ErrNoRows)
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil {
			t.Fatal(err)
		}
		if sum.Continuity != "none" || sum.LagSeconds != nil {
			t.Fatalf("continuity=%q lag=%v — file mode must claim neither", sum.Continuity, sum.LagSeconds)
		}
	})

	t.Run("stamped gap surfaces as gap_lost", func(t *testing.T) {
		db, mock := newDB(t)
		expectWindow(mock)
		mock.ExpectQuery(covStreamQ).WillReturnRows(sqlmock.NewRows(covStreamCols).
			AddRow("gtid", "", 0, "uuid:1-9", 100, latest, latest, 7, "id-1", latest.Add(-time.Hour), "auto-advance"))
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil || sum.Continuity != "gap_lost" {
			t.Fatalf("continuity=%q err=%v", sum.Continuity, err)
		}
	})

	t.Run("acknowledged gap is gap_acknowledged with both instants, never ok", func(t *testing.T) {
		db, mock := newDB(t)
		mock.MatchExpectationsInOrder(false)
		expectWindow(mock)
		lost := latest.Add(-time.Hour)
		mock.ExpectQuery("SELECT mode, binlog_file").WillReturnRows(sqlmock.NewRows(covStreamCols).
			AddRow("gtid", "", 0, "uuid:1-9", 100, latest, latest, 7, "id-1", lost, "auto-advance"))
		mock.ExpectQuery("SELECT gap_acked_at FROM stream_state").
			WillReturnRows(sqlmock.NewRows([]string{"gap_acked_at"}).AddRow(lost.Add(10 * time.Minute)))
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil || sum.Continuity != "gap_acknowledged" {
			t.Fatalf("continuity=%q err=%v, want gap_acknowledged", sum.Continuity, err)
		}
		if !sum.GapLostAt.Equal(lost) || !sum.GapAckedAt.Equal(lost.Add(10*time.Minute)) {
			t.Fatalf("lost=%v acked=%v", sum.GapLostAt, sum.GapAckedAt)
		}
	})

	t.Run("stream read failure degrades to unavailable, not an error", func(t *testing.T) {
		db, mock := newDB(t)
		expectWindow(mock)
		mock.ExpectQuery(covStreamQ).WillReturnError(errors.New("conn reset"))
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil {
			t.Fatal(err)
		}
		if sum.Continuity != "unavailable" || sum.LagSeconds != nil {
			t.Fatalf("continuity=%q lag=%v — a read failure must degrade, never fabricate", sum.Continuity, sum.LagSeconds)
		}
	})

	t.Run("empty index with a live stream: no window edge, no fabricated lag", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110", "p_future"))
		mock.ExpectQuery(covArchQ).WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(nil, nil, 0))
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110", "p_future"))
		mock.ExpectQuery(covProbeQ).WillReturnRows(covProbeRow(nil)) // p_future empty
		mock.ExpectQuery(covProbeQ).WillReturnRows(covProbeRow(nil)) // dated empty too
		mock.ExpectQuery(covStreamQ).WillReturnRows(sqlmock.NewRows(covStreamCols).
			AddRow("gtid", "", 0, "uuid:1-9", 0, nil, latest, 7, "id-1", nil, nil))
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil {
			t.Fatal(err)
		}
		if !sum.DeltaTo.IsZero() || sum.LagSeconds != nil {
			t.Fatalf("empty index must have zero edge and NO lag (now−zero would be astronomical): %+v", sum)
		}
	})

	t.Run("floor failure degrades from to unknown, not the endpoint", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110"))
		mock.ExpectQuery(covArchQ).WillReturnError(&mysql.MySQLError{Number: 1045, Message: "access denied"})
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110"))
		mock.ExpectQuery(covProbeQ).WillReturnRows(covProbeRow(latest))
		mock.ExpectQuery(covStreamQ).WillReturnError(sql.ErrNoRows)
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil {
			t.Fatal(err)
		}
		if !sum.Floor.Hour.IsZero() || !sum.DeltaTo.Equal(latest) {
			t.Fatalf("unknown floor must stay zero: %+v", sum)
		}
	})

	t.Run("newest-event probe failure is fatal", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110"))
		mock.ExpectQuery(covArchQ).WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(nil, nil, 0))
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110"))
		mock.ExpectQuery(covProbeQ).WillReturnError(errors.New("boom"))
		if _, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now); err == nil {
			t.Fatal("a window without an upper edge must be an error")
		}
	})
}
