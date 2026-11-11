package consoleapp

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

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
)

// Stopping a source acknowledges a recorded capture loss. It must never erase
// the loss: gap_lost_at is what snapshot updates, reconstruct and verify
// refuse on.

func TestAcknowledgeGapLoss_stampsAckAndKeepsTheLoss(t *testing.T) {
	t.Logf("Stop runs: %s", acknowledgeGapLossSQL)
	set := acknowledgeGapLossSQL[strings.Index(acknowledgeGapLossSQL, " SET ")+5 : strings.Index(acknowledgeGapLossSQL, " WHERE ")]
	if strings.TrimSpace(set) != "gap_acked_at = UTC_TIMESTAMP()" {
		t.Errorf("the acknowledgement must assign gap_acked_at and nothing else, got SET %q", set)
	}
	if strings.Contains(set, "gap_lost") {
		t.Errorf("the acknowledgement must not write gap_lost_at or gap_lost_detail: SET %q", set)
	}
	// No loss stamped: no acknowledgement recorded.
	if !strings.Contains(acknowledgeGapLossSQL, "WHERE id = 1 AND gap_lost_at IS NOT NULL") {
		t.Errorf("the acknowledgement must be a no-op when no loss is stamped: %s", acknowledgeGapLossSQL)
	}

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Twice: the second acknowledgement is the same statement and no error.
	mock.ExpectExec(acknowledgeGapLossSQL).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(acknowledgeGapLossSQL).WillReturnResult(sqlmock.NewResult(0, 0))
	for i := range 2 {
		if err := acknowledgeGapLoss(context.Background(), db); err != nil {
			t.Fatalf("acknowledge #%d: %v", i+1, err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}

	mock.ExpectExec(acknowledgeGapLossSQL).WillReturnError(errors.New("conn reset"))
	if err := acknowledgeGapLoss(context.Background(), db); err == nil {
		t.Error("a failed acknowledgement must be reported, so Stop can warn that the loss stays unacknowledged")
	}
}

const gapAckSelect = "SELECT gap_lost_at, gap_lost_detail, gap_acked_at FROM stream_state WHERE id = 1"

var gapAckLost = time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)

// gapAckCases is every (loss, acknowledgement) shape the alarm readers meet,
// with what the alarm must do.
var gapAckCases = []struct {
	name      string
	lost, ack any // time.Time or nil
	detail    any
	wantLoud  bool
}{
	{"no loss", nil, nil, nil, false},
	{"acknowledgement with no loss stamped", nil, gapAckLost, nil, false},
	{"loss, never acknowledged", gapAckLost, nil, "binlogs purged", true},
	{"loss, acknowledged later", gapAckLost, gapAckLost.Add(time.Hour), "binlogs purged", false},
	{"loss and acknowledgement in the same second", gapAckLost, gapAckLost, "binlogs purged", false},
	{"second loss after the acknowledgement (the stamp nulled it)", gapAckLost.Add(2 * time.Hour), nil, "second purge", true},
	{"acknowledgement older than the loss", gapAckLost, gapAckLost.Add(-time.Second), "binlogs purged", true},
}

func gapAckMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

// The supervisor's re-hydrate at Start: an acknowledged loss does not put the
// source back in lost_position; an unacknowledged one does.
func TestUnacknowledgedGapDetail(t *testing.T) {
	for _, c := range gapAckCases {
		t.Run(c.name, func(t *testing.T) {
			db, mock := gapAckMock(t)
			mock.ExpectQuery(regexp.QuoteMeta(gapAckSelect)).WillReturnRows(
				sqlmock.NewRows([]string{"gap_lost_at", "gap_lost_detail", "gap_acked_at"}).AddRow(c.lost, c.detail, c.ack))
			got, err := unacknowledgedGapDetail(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			job := &monitorJob{state: "running"}
			if got != "" {
				job.markLostPosition(got)
			}
			st := job.snapshot()
			t.Logf("console state after Start: %q, error text %q", st.State, st.LastError)
			if loud := st.State == "lost_position"; loud != c.wantLoud {
				t.Errorf("lost_position = %v, want %v (detail %q)", loud, c.wantLoud, got)
			}
			if c.wantLoud && st.LastError != c.detail {
				t.Errorf("LastError = %q, want the recorded detail %q", st.LastError, c.detail)
			}
		})
	}

	t.Run("empty stream_state is a fresh start, not an error", func(t *testing.T) {
		db, mock := gapAckMock(t)
		mock.ExpectQuery(regexp.QuoteMeta(gapAckSelect)).WillReturnError(sql.ErrNoRows)
		if got, err := unacknowledgedGapDetail(context.Background(), db); got != "" || err != nil {
			t.Errorf("= (%q, %v), want (\"\", nil)", got, err)
		}
	})
	t.Run("a read failure is reported, never read as acknowledged-and-quiet", func(t *testing.T) {
		db, mock := gapAckMock(t)
		mock.ExpectQuery(regexp.QuoteMeta(gapAckSelect)).WillReturnError(errors.New("conn reset"))
		if _, err := unacknowledgedGapDetail(context.Background(), db); err == nil {
			t.Error("want the error, so Start warns that a recorded loss may not be re-surfaced")
		}
	})
}

// The continuity watch (webhook + Prometheus gauge): acknowledged is quiet,
// unacknowledged is loud, and the real notification text is logged.
func TestReadGapLostFrom(t *testing.T) {
	for _, c := range gapAckCases {
		t.Run(c.name, func(t *testing.T) {
			db, mock := gapAckMock(t)
			mock.ExpectQuery(regexp.QuoteMeta(gapAckSelect)).WillReturnRows(
				sqlmock.NewRows([]string{"gap_lost_at", "gap_lost_detail", "gap_acked_at"}).AddRow(c.lost, c.detail, c.ack))
			lost, detail, err := readGapLostFrom(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			if lost != c.wantLoud {
				t.Fatalf("gapLost = %v, want %v", lost, c.wantLoud)
			}
			if !lost && detail != "" {
				t.Errorf("a quiet verdict must carry no detail, got %q", detail)
			}

			// Drive the real watcher cycle with this verdict.
			name := "ack-" + strings.ReplaceAll(c.name, " ", "-")
			reg := testRegistryWithEntries(t, console.ServerEntry{Name: name, DSN: "dsn-" + name})
			n, sent := testNotifier()
			w := &continuityWatcher{
				n: n, registry: reg, unknownEdge: notify.NewEdge(0), prevNames: make(map[string]bool),
				readGap: func(context.Context, string) (bool, string, error) { return lost, detail, nil },
			}
			w.runCycle(context.Background())
			got, ok := gatherGauge(t, "bintrail_continuity_gap_lost", map[string]string{"server": name})
			want := 0.0
			if c.wantLoud {
				want = 1
			}
			if !ok || got != want {
				t.Errorf("bintrail_continuity_gap_lost = %v (found=%v), want %v", got, ok, want)
			}
			if c.wantLoud != (len(sent.events) == 1) {
				t.Errorf("notifications sent = %d, want loud=%v", len(sent.events), c.wantLoud)
			}
			for _, ev := range sent.events {
				t.Logf("webhook: severity=%s summary=%q details=%v", ev.Severity, ev.Summary, ev.Details)
			}
		})
	}

	t.Run("acknowledging a loud loss sends the cleared notification", func(t *testing.T) {
		n, sent := testNotifier()
		n.Continuity("srv", "dsn", true, "binlogs purged")
		n.Continuity("srv", "dsn", false, "")
		if len(sent.events) != 2 || !sent.events[1].Resolved {
			t.Fatalf("events = %+v, want the loss then its resolution", sent.events)
		}
		t.Logf("webhook after the acknowledgement: %q", sent.events[1].Summary)
		for _, want := range []string{"acknowledged", "still refused until a full snapshot is taken after it"} {
			if !strings.Contains(sent.events[1].Summary, want) {
				t.Errorf("resolution summary %q missing %q", sent.events[1].Summary, want)
			}
		}
		if strings.Contains(sent.events[1].Summary, "re-baselined") {
			t.Errorf("an acknowledgement re-baselines nothing: %q", sent.events[1].Summary)
		}
	})

	unknownCol := &mysql.MySQLError{Number: 1054, Message: "Unknown column 'gap_acked_at' in 'field list'"}
	const oldSelect = "SELECT gap_lost_at, gap_lost_detail FROM stream_state WHERE id = 1"

	t.Run("index without gap_acked_at: the old meaning, loss is loud", func(t *testing.T) {
		db, mock := gapAckMock(t)
		mock.ExpectQuery(regexp.QuoteMeta(gapAckSelect)).WillReturnError(unknownCol)
		mock.ExpectQuery(regexp.QuoteMeta(oldSelect)).WillReturnRows(
			sqlmock.NewRows([]string{"gap_lost_at", "gap_lost_detail"}).AddRow(gapAckLost, "binlogs purged"))
		lost, detail, err := readGapLostFrom(context.Background(), db)
		if err != nil || !lost || detail != "binlogs purged" {
			t.Errorf("= (%v, %q, %v), want (true, \"binlogs purged\", nil)", lost, detail, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("index without gap_acked_at and no loss: quiet", func(t *testing.T) {
		db, mock := gapAckMock(t)
		mock.ExpectQuery(regexp.QuoteMeta(gapAckSelect)).WillReturnError(unknownCol)
		mock.ExpectQuery(regexp.QuoteMeta(oldSelect)).WillReturnRows(
			sqlmock.NewRows([]string{"gap_lost_at", "gap_lost_detail"}).AddRow(nil, nil))
		if lost, _, err := readGapLostFrom(context.Background(), db); err != nil || lost {
			t.Errorf("= (%v, %v), want (false, nil)", lost, err)
		}
	})
	t.Run("legacy index with neither column is unknown, never 'no gap'", func(t *testing.T) {
		db, mock := gapAckMock(t)
		mock.ExpectQuery(regexp.QuoteMeta(gapAckSelect)).WillReturnError(unknownCol)
		mock.ExpectQuery(regexp.QuoteMeta(oldSelect)).WillReturnError(
			&mysql.MySQLError{Number: 1054, Message: "Unknown column 'gap_lost_at' in 'field list'"})
		if _, _, err := readGapLostFrom(context.Background(), db); err == nil {
			t.Error("want an error: the caller must treat this index as unknown")
		}
	})
	t.Run("any other failure is unknown, and does not take the fallback", func(t *testing.T) {
		db, mock := gapAckMock(t)
		mock.ExpectQuery(regexp.QuoteMeta(gapAckSelect)).WillReturnError(errors.New("conn reset"))
		if _, _, err := readGapLostFrom(context.Background(), db); err == nil {
			t.Error("want an error")
		}
	})
	t.Run("empty stream_state: no capture ran, no gap", func(t *testing.T) {
		db, mock := gapAckMock(t)
		mock.ExpectQuery(regexp.QuoteMeta(gapAckSelect)).WillReturnError(sql.ErrNoRows)
		if lost, _, err := readGapLostFrom(context.Background(), db); err != nil || lost {
			t.Errorf("= (%v, %v), want (false, nil)", lost, err)
		}
	})
}
