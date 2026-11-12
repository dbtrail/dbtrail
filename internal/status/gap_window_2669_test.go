package status

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// #2669. The window of changes still held starts at the delta floor, and the
// floor misses two places where older changes are still served:
//
//   - archived hours separated from the live partitions by a stretch nobody
//     archived. The floor stops at the break, which is right for grading
//     snapshots, but those hours can still be queried;
//   - events older than the oldest live partition's name. The oldest partition
//     takes every event dated before it, so re-indexing an old binlog after
//     rotation lands older changes under a later hour's name.
//
// Either way the index serves changes from both sides of an acknowledged loss,
// and the report must not say "ok".

var (
	gwSrvQ      = regexp.QuoteMeta("SELECT COUNT(*) FROM bintrail_servers")
	gwArchHeldQ = regexp.QuoteMeta("FROM archive_state WHERE partition_name <= ? OR (min_event_ts <= ?")
	gwEvHeldQ   = regexp.QuoteMeta("FROM binlog_events WHERE event_timestamp <= ?")
)

// gwArchHeldLegacyQ is the form for an archive_state without min_event_ts (1054).
var gwArchHeldLegacyQ = regexp.QuoteMeta("SELECT MIN(partition_name) FROM archive_state WHERE partition_name <= ?") + `\s*$`

func gwArchRows(min, max any, sources int) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(min, max, sources)
}

func gwArchHeldRows(partition, minEvent any) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"partition", "min_event_ts"}).AddRow(partition, minEvent)
}

func gwEvHeldRows(ts ...time.Time) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"event_timestamp"})
	for _, t := range ts {
		r.AddRow(t)
	}
	return r
}

func gwNewDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

// ackedAt is an acknowledged loss at lost.
func ackedAt(lost time.Time) *StreamStateInfo {
	s := ackedLostStream()
	s.GapLostAt = nt(lost)
	s.GapAckedAt = nt(lost.Add(time.Hour))
	return s
}

func TestLoadGapWindow_looksBelowTheFloor(t *testing.T) {
	ctx := context.Background()
	lost := gapLostStream().GapLostAt.Time // 2026-06-22 12:00:00
	live := func() *sqlmock.Rows { return covPartRows("p_2026071100", "p_2026071101", "p_future") }
	june := func() *sqlmock.Rows { return gwArchRows("p_2026060100", "p_2026062223", 1) }

	type step func(mock sqlmock.Sqlmock)
	for _, c := range []struct {
		name  string
		lost  time.Time
		steps []step
		want  string
	}{
		{"archives separated from the index, older than the loss", lost, []step{
			func(m sqlmock.Sqlmock) { m.ExpectQuery(covArchQ).WillReturnRows(june()) },
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwArchHeldQ).WithArgs("p_2026062212", lost).WillReturnRows(gwArchHeldRows("p_2026060100", nil))
			},
		}, ContinuityGapAcknowledged},
		{"loss in the stretch nobody archived", time.Date(2026, 6, 25, 3, 0, 0, 0, time.UTC), []step{
			func(m sqlmock.Sqlmock) { m.ExpectQuery(covArchQ).WillReturnRows(june()) },
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(0))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwArchHeldQ).WithArgs("p_2026062503", sqlmock.AnyArg()).WillReturnRows(gwArchHeldRows("p_2026060100", nil))
			},
		}, ContinuityGapAcknowledged},
		{"loss in the very hour of the oldest archive", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), []step{
			func(m sqlmock.Sqlmock) { m.ExpectQuery(covArchQ).WillReturnRows(june()) },
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwArchHeldQ).WithArgs("p_2026060100", sqlmock.AnyArg()).WillReturnRows(gwArchHeldRows("p_2026060100", nil))
			},
		}, ContinuityGapAcknowledged},
		{"every archive after the loss, nothing older in the index", lost, []step{
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(covArchQ).WillReturnRows(gwArchRows("p_2026063000", "p_2026070500", 1))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			},
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows(nil, nil)) },
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwEvHeldQ).WithArgs(lost).WillReturnRows(gwEvHeldRows()) },
		}, ContinuityOK},
		{"an archived hour named after the loss holds re-indexed older changes", lost, []step{
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(covArchQ).WillReturnRows(gwArchRows("p_2026070100", "p_2026070500", 1))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows("p_2026070100", time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)))
			},
		}, ContinuityGapAcknowledged},
		{"re-indexed old binlog in the oldest partition", lost, []step{
			func(m sqlmock.Sqlmock) { m.ExpectQuery(covArchQ).WillReturnRows(gwArchRows(nil, nil, 0)) },
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows(nil, nil)) },
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwEvHeldQ).WithArgs(lost).WillReturnRows(gwEvHeldRows(time.Date(2026, 6, 20, 9, 14, 0, 0, time.UTC)))
			},
		}, ContinuityGapAcknowledged},
		{"no archives, nothing older than the loss", lost, []step{
			func(m sqlmock.Sqlmock) { m.ExpectQuery(covArchQ).WillReturnRows(gwArchRows(nil, nil, 0)) },
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows(nil, nil)) },
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwEvHeldQ).WillReturnRows(gwEvHeldRows()) },
		}, ContinuityOK},
		{"no archive table at all, nothing older than the loss", lost, []step{
			func(m sqlmock.Sqlmock) { m.ExpectQuery(covArchQ).WillReturnError(&mysql.MySQLError{Number: 1146}) },
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwArchHeldQ).WillReturnError(&mysql.MySQLError{Number: 1146}) },
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwEvHeldQ).WillReturnRows(gwEvHeldRows()) },
		}, ContinuityOK},
		{"archive check fails: stays on record", lost, []step{
			func(m sqlmock.Sqlmock) { m.ExpectQuery(covArchQ).WillReturnRows(gwArchRows(nil, nil, 0)) },
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwArchHeldQ).WillReturnError(errors.New("conn reset")) },
		}, ContinuityGapAcknowledged},
		{"index check fails: stays on record", lost, []step{
			func(m sqlmock.Sqlmock) { m.ExpectQuery(covArchQ).WillReturnRows(gwArchRows(nil, nil, 0)) },
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows(nil, nil)) },
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwEvHeldQ).WillReturnError(errors.New("lock wait timeout")) },
		}, ContinuityGapAcknowledged},
		{"archive_state without min_event_ts, an archived hour before the loss", lost, []step{
			func(m sqlmock.Sqlmock) { m.ExpectQuery(covArchQ).WillReturnRows(june()) },
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwArchHeldQ).WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'min_event_ts'"})
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwArchHeldLegacyQ).WithArgs("p_2026062212").
					WillReturnRows(sqlmock.NewRows([]string{"p"}).AddRow("p_2026060100"))
			},
		}, ContinuityGapAcknowledged},
		{"archive_state without min_event_ts, nothing before the loss", lost, []step{
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(covArchQ).WillReturnRows(gwArchRows("p_2026063000", "p_2026070500", 1))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwArchHeldQ).WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'min_event_ts'"})
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwArchHeldLegacyQ).WillReturnRows(sqlmock.NewRows([]string{"p"}).AddRow(nil))
			},
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwEvHeldQ).WillReturnRows(gwEvHeldRows()) },
		}, ContinuityOK},
		{"a zero-date min_event_ts does not erase the archived hour", lost, []step{
			func(m sqlmock.Sqlmock) { m.ExpectQuery(covArchQ).WillReturnRows(june()) },
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows("p_2026060100", time.Time{}))
			},
		}, ContinuityGapAcknowledged},
		{"an archived hour after the loss is not a change held before it", lost, []step{
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(covArchQ).WillReturnRows(gwArchRows("p_2026063000", "p_2026070500", 1))
			},
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			},
			func(m sqlmock.Sqlmock) { m.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows("p_2026063000", nil)) },
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(gwEvHeldQ).WillReturnRows(gwEvHeldRows(time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)))
			},
		}, ContinuityGapAcknowledged},
		{"archives of more than one source: on record, nothing more asked", lost, []step{
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(covArchQ).WillReturnRows(gwArchRows("p_2026063000", "p_2026070500", 2))
			},
		}, ContinuityGapAcknowledged},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, mock := gwNewDB(t)
			mock.ExpectQuery(covPartsQ).WillReturnRows(live())
			for _, s := range c.steps {
				s(mock)
			}
			s := ackedAt(c.lost)
			LoadGapWindow(ctx, db, "binlog_index", s)
			if got := ContinuityStatus(s, nil); got != c.want {
				t.Errorf("verdict = %q (window %+v), want %q", got, s.Floor, c.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}

	t.Run("loss inside the live partitions: nothing below the floor is asked", func(t *testing.T) {
		db, mock := gwNewDB(t)
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026062200", "p_future"))
		mock.ExpectQuery(covArchQ).WillReturnRows(gwArchRows(nil, nil, 0))
		mock.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows(nil, nil))
		s := ackedAt(lost)
		LoadGapWindow(ctx, db, "binlog_index", s)
		if got := ContinuityStatus(s, nil); got != ContinuityGapAcknowledged {
			t.Errorf("verdict = %q, want gap_acknowledged", got)
		}
		if err := mock.ExpectationsWereMet(); err == nil {
			t.Error("the archives were asked about a loss the live partitions already hold")
		}
	})

	t.Run("cancelled read: stays on record", func(t *testing.T) {
		db, mock := gwNewDB(t)
		mock.ExpectQuery(covPartsQ).WillReturnRows(live())
		mock.ExpectQuery(covArchQ).WillReturnRows(gwArchRows(nil, nil, 0))
		mock.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows(nil, nil))
		mock.ExpectQuery(gwEvHeldQ).WillReturnRows(gwEvHeldRows())
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		s := ackedAt(lost)
		LoadGapWindow(cctx, db, "binlog_index", s)
		if got := ContinuityStatus(s, nil); got != ContinuityGapAcknowledged {
			t.Errorf("verdict = %q after a cancelled read, want gap_acknowledged", got)
		}
	})
}

// The floor that grades snapshots is not moved by what LoadGapWindow finds:
// a snapshot older than the break in the archives is still graded broken.
func TestLoadGapWindow_leavesTheSnapshotFloorAlone(t *testing.T) {
	db, mock := gwNewDB(t)
	mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026071100", "p_future"))
	mock.ExpectQuery(covArchQ).WillReturnRows(gwArchRows("p_2026060100", "p_2026062223", 1))
	mock.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	floor, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC); !floor.Hour.Equal(want) || floor.BelowIsUnknown {
		t.Fatalf("snapshot floor = %+v, want %v: archives separated by a break do not extend it", floor, want)
	}
}

// #2669 point 2. CollectStatus is what `bintrail status`, GET /api/status and
// the MCP status tool read, and only it decides whether the window is loaded.
// An acknowledged loss older than everything held reads "ok" only when the
// window was read, so removing that read turns the first case red.
func TestCollectStatus_readsTheGapWindow(t *testing.T) {
	latest := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 1, 9, 30, 0, 0, time.UTC)
	noArch := func() *sqlmock.Rows { return gwArchRows(nil, nil, 0) }
	for _, c := range []struct {
		name   string
		lost   time.Time
		below  func(m sqlmock.Sqlmock) // the reads below the floor, when they run
		arch   func() *sqlmock.Rows
		want   string
		inText string
	}{
		{"acknowledged loss older than everything held", before, func(m sqlmock.Sqlmock) {
			m.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows(nil, nil))
			m.ExpectQuery(gwEvHeldQ).WillReturnRows(gwEvHeldRows())
		}, noArch, ContinuityOK, "no gaps in the captured range"},
		{"acknowledged loss inside the oldest partition", time.Date(2026, 8, 1, 10, 30, 0, 0, time.UTC), func(sqlmock.Sqlmock) {},
			noArch, ContinuityGapAcknowledged, "no gaps since the loss acknowledged below"},
		{"acknowledged loss after the newest indexed event", latest.Add(20 * time.Second), func(sqlmock.Sqlmock) {},
			noArch, ContinuityGapAcknowledged, "no gaps since the loss acknowledged below"},
		{"archives separated from the index reach past the loss", before, func(m sqlmock.Sqlmock) {
			m.ExpectQuery(gwSrvQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			m.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows("p_2026060100", nil))
		}, func() *sqlmock.Rows { return gwArchRows("p_2026060100", "p_2026062223", 1) }, ContinuityGapAcknowledged, "no gaps since the loss acknowledged below"},
		{"an event older than the loss sits in the oldest partition", before, func(m sqlmock.Sqlmock) {
			m.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows(nil, nil))
			m.ExpectQuery(gwEvHeldQ).WillReturnRows(gwEvHeldRows(time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)))
		}, noArch, ContinuityGapAcknowledged, "no gaps since the loss acknowledged below"},
	} {
		t.Run(c.name, func(t *testing.T) {
			stream := func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT mode, binlog_file").WillReturnRows(sqlmock.NewRows(covStreamCols).
					AddRow("gtid", "", 0, "uuid:1-9", 100, latest, latest, 7, "id-1", c.lost, "auto-advance"))
				m.ExpectQuery("SELECT gap_acked_at FROM stream_state").
					WillReturnRows(sqlmock.NewRows([]string{"gap_acked_at"}).AddRow(c.lost.Add(10 * time.Minute)))
			}
			floor := func(m sqlmock.Sqlmock) {
				m.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110", "p_future"))
				m.ExpectQuery(covArchQ).WillReturnRows(c.arch())
			}

			// The status report, through the collector.
			db, mock := gwNewDB(t)
			mock.MatchExpectationsInOrder(false)
			mock.ExpectQuery("FROM index_state").WillReturnRows(sqlmock.NewRows([]string{"binlog_file", "status",
				"events_indexed", "file_size", "last_position", "started_at", "completed_at", "error_message", "bintrail_id"}))
			mock.ExpectQuery(regexp.QuoteMeta("SELECT PARTITION_NAME, IFNULL")).WillReturnRows(
				sqlmock.NewRows([]string{"name", "desc", "ord", "rows"}).AddRow("p_2026080110", "1", 1, 10).AddRow("p_future", "MAXVALUE", 2, 0))
			stream(mock)
			floor(mock)
			c.below(mock)
			data, err := CollectStatus(context.Background(), db, "binlog_index")
			if err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("CollectStatus did not read the window: %v", err)
			}
			text, doc, raw := renderGapWindow(t, data.Stream)
			t.Logf("status text:\n%s", streamLines(text))
			if doc.Stream.Continuity.Status != c.want {
				t.Fatalf("status JSON continuity = %q, want %q\n%s", doc.Stream.Continuity.Status, c.want, raw)
			}
			if !strings.Contains(text, c.inText) {
				t.Errorf("status text lacks %q:\n%s", c.inText, streamLines(text))
			}

			// The coverage summary, from the same index, says the same word.
			db2, mock2 := gwNewDB(t)
			mock2.MatchExpectationsInOrder(false)
			floor(mock2)
			mock2.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110", "p_future"))
			mock2.ExpectQuery(covProbeQ).WillReturnRows(covProbeRow(nil))
			mock2.ExpectQuery(covProbeQ).WillReturnRows(covProbeRow(latest))
			stream(mock2)
			c.below(mock2)
			sum, err := CollectCoverageSummary(context.Background(), db2, "binlog_index", latest.Add(42*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if sum.Continuity != doc.Stream.Continuity.Status {
				t.Errorf("coverage summary = %q, status = %q", sum.Continuity, doc.Stream.Continuity.Status)
			}
		})
	}
}

// The second belt behind the query's own exclusion: a zero date that still
// reaches Go never replaces an archived hour already found (review of #2711).
func TestArchivedAtOrBefore_ignoresAZeroDate(t *testing.T) {
	db, mock := gwNewDB(t)
	at := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows("p_2026060100", time.Time{}))
	held, err := archivedAtOrBefore(context.Background(), db, at)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC); !held.Equal(want) {
		t.Errorf("held = %v, want the archived hour %v", held, want)
	}
}
