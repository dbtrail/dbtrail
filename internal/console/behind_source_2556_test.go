package console

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// #2556: how far behind the source the data of the copy's last answer was,
// read like Seconds_Behind_Source. Capture caught up and the answer held
// every indexed event: 0. Caught up, events missing: newest − through, or
// ran − through when their times cannot say. Not caught up: ran − through.

var t0 = time.Date(2026, 10, 25, 14, 0, 0, 0, time.UTC)

// caught is a sample of a capture caught up at at, read at t0 (after every
// answer in these tests), with maxID the index's newest event.
func caught(at time.Time, maxID uint64, newest time.Time) captureSample {
	return captureSample{caughtUp: at, maxID: maxID, newest: newest, readAt: t0.Add(time.Second)}
}

func TestBehindSource_noStampIsRanMinusThrough_2556(t *testing.T) {
	through := t0.Add(-time.Hour)
	if got := behindSourceOf(t0, through, 100, captureSample{maxID: 100, newest: through, readAt: t0}); got != time.Hour {
		t.Fatalf("no stamp: %v, want 1h (ran − through)", got)
	}
}

// The source sat idle for an hour and the answer held every indexed event:
// 0, not an hour.
func TestBehindSource_heldEverythingIsZero_2556(t *testing.T) {
	through := t0.Add(-time.Hour)
	if got := behindSourceOf(t0, through, 100, caught(t0.Add(-20*time.Second), 100, through)); got != 0 {
		t.Fatalf("idle source, every event held: %v, want 0", got)
	}
}

// Events the answer did not read, newer than its data: their distance.
func TestBehindSource_missingNewerEventsAreTheirDistance_2556(t *testing.T) {
	through := t0.Add(-time.Hour)
	if got := behindSourceOf(t0, through, 100, caught(t0, 140, through.Add(5*time.Minute))); got != 5*time.Minute {
		t.Fatalf("snapshot 5 minutes behind the index: %v, want 5m", got)
	}
}

// The case that started this: a transaction that began before the snapshot
// and committed after it carries an OLDER time, so newest − through says
// nothing is missing. The ids say otherwise: ran − through, never 0.
func TestBehindSource_aLateCommitWithAnEarlyTimeIsNeverZero_2556(t *testing.T) {
	through := t0.Add(-6 * time.Minute)
	if got := behindSourceOf(t0, through, 100, caught(t0, 101, through)); got != 6*time.Minute {
		t.Fatalf("event 101 missing, its time not newer: %v, want 6m (ran − through)", got)
	}
	if got := behindSourceOf(t0, through, 100, caught(t0, 101, through.Add(-time.Minute))); got != 6*time.Minute {
		t.Fatalf("newest older than the data: %v, want 6m", got)
	}
}

// Nothing proves the answer held everything without both ids, and a sample
// read before the answer ran cannot show what the index held when it did.
func TestBehindSource_zeroNeedsBothIdsAndALaterRead_2556(t *testing.T) {
	through := t0.Add(-time.Hour)
	if got := behindSourceOf(t0, through, 0, caught(t0, 100, through)); got != time.Hour {
		t.Fatalf("answer's id unknown: %v, want 1h", got)
	}
	if got := behindSourceOf(t0, through, 100, caught(t0, 0, through)); got != time.Hour {
		t.Fatalf("index's id unknown: %v, want 1h", got)
	}
	early := caught(t0, 100, through)
	early.readAt = t0.Add(-time.Second)
	if got := behindSourceOf(t0, through, 100, early); got != time.Hour {
		t.Fatalf("sample read before the answer ran: %v, want 1h", got)
	}
	// And at or past the index's newest id is held everything: an id the
	// answer holds that the sample has not seen yet.
	if got := behindSourceOf(t0, through, 101, caught(t0, 100, through)); got != 0 {
		t.Fatalf("answer past the sample's newest id: %v, want 0", got)
	}
}

// After --reset the index keeps its events but capture has indexed none:
// no newest is never 0.
func TestBehindSource_noNewestIsNeverZero_2556(t *testing.T) {
	through := t0.Add(-time.Hour)
	if got := behindSourceOf(t0, through, 100, caught(t0, 200, time.Time{})); got != time.Hour {
		t.Fatalf("events missing, no newest: %v, want 1h", got)
	}
}

// The loop reads every 30 seconds, so the stamp can be after the answer.
func TestBehindSource_aStampAfterTheAnswerCounts_2556(t *testing.T) {
	through := t0.Add(-time.Hour)
	if got := behindSourceOf(t0, through, 100, caught(t0.Add(40*time.Second), 100, through)); got != 0 {
		t.Fatalf("stamp 40s after the answer: %v, want 0", got)
	}
}

// Past the window, either side, capture is not known to be caught up when the
// answer ran: ran − through, which grows with each answer.
func TestBehindSource_outsideTheWindowGrowsWithEachQuery_2556(t *testing.T) {
	through := t0.Add(-time.Hour)
	old := caught(t0.Add(-caughtUpWindow-time.Second), 100, through)
	old.readAt = t0.Add(time.Minute)
	first := behindSourceOf(t0, through, 100, old)
	second := behindSourceOf(t0.Add(10*time.Second), through, 100, old)
	if first != time.Hour || second != time.Hour+10*time.Second {
		t.Fatalf("old stamp: %v then %v, want 1h then 1h10s", first, second)
	}
	if got := behindSourceOf(t0, through, 100, caught(t0.Add(caughtUpWindow+time.Second), 100, through)); got != time.Hour {
		t.Fatalf("stamp past the window after the answer: %v, want 1h", got)
	}
	if got := behindSourceOf(t0, through, 100, caught(t0.Add(-caughtUpWindow), 100, through)); got != 0 {
		t.Fatalf("stamp exactly one window old: %v, want 0", got)
	}
}

func TestBehindSource_windowIsTwoHeartbeatPeriods_2556(t *testing.T) {
	if caughtUpWindow != 2*captureHeartbeatPeriod || captureHeartbeatPeriod != 30*time.Second {
		t.Fatalf("window %v, period %v", caughtUpWindow, captureHeartbeatPeriod)
	}
}

// The window is built on capture's own heartbeat period: a change there must
// move it here.
func TestBehindSource_periodIsCaptures_2556(t *testing.T) {
	if captureHeartbeatPeriod != streamrun.SourceHeartbeatPeriod {
		t.Fatalf("console says %v, capture asks the source for %v", captureHeartbeatPeriod, streamrun.SourceHeartbeatPeriod)
	}
}

const stampQuery = `SELECT TIMESTAMPDIFF\(MICROSECOND, caught_up_at, UTC_TIMESTAMP\(6\)\) FROM stream_state WHERE id = 1`

// readCaptureSample reads the stamp's AGE on the index clock and places it
// on the router's, then the index's newest id and newest event time.
func TestReadCaptureSample_recentStamp_2556(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	newest := t0.Add(-time.Hour)
	mock.ExpectQuery(stampQuery).WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow(int64(7_000_000)))
	mock.ExpectQuery(`SELECT 1 FROM index_state`).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT event_id FROM binlog_events FORCE INDEX \(PRIMARY\) ORDER BY event_id DESC LIMIT 1`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(4242)))
	mock.ExpectQuery(`information_schema.PARTITIONS`).WithArgs("idx").
		WillReturnRows(sqlmock.NewRows([]string{"p"}).AddRow("p_future").AddRow("p_2026102513"))
	mock.ExpectQuery("PARTITION \\(`p_future`\\)").WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(nil))
	mock.ExpectQuery("PARTITION \\(`p_2026102513`\\)").WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(newest))
	s, err := readCaptureSample(context.Background(), db, "idx", func() time.Time { return t0 })
	if err != nil {
		t.Fatal(err)
	}
	if !s.caughtUp.Equal(t0.Add(-7*time.Second)) || s.maxID != 4242 || !s.newest.Equal(newest) || !s.readAt.Equal(t0) {
		t.Fatalf("sample %+v", s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// An index `bintrail index` also wrote has ids out of the binary log's
// order: no newest id, so never the 0 shortcut.
func TestReadCaptureSample_backfilledIndexHasNoId_2556(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(stampQuery).WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow(int64(1_000_000)))
	mock.ExpectQuery(`SELECT 1 FROM index_state`).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery(`information_schema.PARTITIONS`).WillReturnRows(sqlmock.NewRows([]string{"p"}))
	s, err := readCaptureSample(context.Background(), db, "idx", func() time.Time { return t0 })
	if err != nil || s.maxID != 0 || s.caughtUp.IsZero() {
		t.Fatalf("backfilled: %+v, %v; want a stamp and no id", s, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A stamp too old for any answer is all that is read: a busy source, whose
// stamp is always old, never pays for the newest probe.
func TestReadCaptureSample_oldStampReadsNothingMore_2556(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(stampQuery).WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow(int64(3600_000_000)))
	s, err := readCaptureSample(context.Background(), db, "idx", func() time.Time { return t0 })
	if err != nil || !s.caughtUp.Equal(t0.Add(-time.Hour)) || s.maxID != 0 || !s.newest.IsZero() {
		t.Fatalf("old stamp: %+v, %v", s, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestReadCaptureSample_nullStampAndNoRow_2556(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(stampQuery).WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow(nil))
	s, err := readCaptureSample(context.Background(), db, "idx", func() time.Time { return t0 })
	if err != nil || !s.caughtUp.IsZero() || !s.readAt.Equal(t0) {
		t.Fatalf("NULL: %+v, %v; want an empty sample read at t0", s, err)
	}
	mock.ExpectQuery(stampQuery).WillReturnError(sql.ErrNoRows)
	s, err = readCaptureSample(context.Background(), db, "idx", func() time.Time { return t0 })
	if err != nil || !s.caughtUp.IsZero() || !s.readAt.Equal(t0) {
		t.Fatalf("no row: %+v, %v; want an empty sample read at t0", s, err)
	}
}

// An index an older capture last wrote has no caught_up_at until capture
// restarts on a newer build, and the router never migrates the index: no
// stamp, and the error is not one.
func TestReadCaptureSample_missingColumnIsNoStamp_2556(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(stampQuery).WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'caught_up_at' in 'field list'"})
	s, err := readCaptureSample(context.Background(), db, "idx", func() time.Time { return t0 })
	if err != nil || !s.caughtUp.IsZero() || !s.readAt.Equal(t0) {
		t.Fatalf("missing column: %+v, %v; want no stamp", s, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestReadCaptureSample_otherErrorsAreErrors_2556(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	missing := &mysql.MySQLError{Number: 1146, Message: "Table 'idx.stream_state' doesn't exist"}
	mock.ExpectQuery(stampQuery).WillReturnError(missing)
	if _, err := readCaptureSample(context.Background(), db, "idx", time.Now); !errors.Is(err, missing) {
		t.Fatalf("err %v, want %v", err, missing)
	}
}

// The status numbers: the last answer's distance once its through and the
// server's capture sample are both known, pending while either waits, and
// unknown, with a fixed reason, when either cannot be told.
func TestCopyAges_behindSourceNow_2556(t *testing.T) {
	ran, through := t0, t0.Add(-time.Hour)
	tailed := copyAnswer{side: "copy", ranAt: ran, tailThrough: through, tailThroughID: 100, allTailed: true}

	var c copyAges
	c.note("s1", tailed)
	if n := c.now("s1"); n.behindKnown || !n.behindPending {
		t.Fatalf("no capture sample read yet: %+v, want pending", n)
	}
	c.captureRead("s1", caught(ran.Add(-10*time.Second), 100, through), nil, "")
	n := c.now("s1")
	if !n.behindKnown || n.behind != 0 || !n.behindRan.Equal(ran) || n.behindPending {
		t.Fatalf("idle source caught up, every event held: %+v, want 0 behind, ran %v", n, ran)
	}
	// The same answer with a stamp out of the window is ran − through.
	c.captureRead("s1", caught(ran.Add(-time.Hour), 100, through), nil, "")
	if n := c.now("s1"); !n.behindKnown || n.behind != time.Hour {
		t.Fatalf("old stamp: %+v, want 1h", n)
	}
	// A failed read keeps the last sample: its stamp is still a real one.
	c.captureRead("s1", captureSample{}, errors.New("dial tcp: refused"), copyAgeWhyNoIndex)
	if n := c.now("s1"); !n.behindKnown || n.behind != time.Hour {
		t.Fatalf("failed read after a good one: %+v, want the last sample's 1h", n)
	}

	// With no sample ever read and the read failing: unknown, with the
	// fixed reason the failure was given and never the error's text.
	var d copyAges
	d.note("s2", tailed)
	d.captureRead("s2", captureSample{}, errors.New("Error 1142: SELECT command denied to user 'ro'@'10.0.0.7'"), copyAgeWhyNoState)
	if n := d.now("s2"); n.behindKnown || n.behindPending || n.behindWhy != copyAgeWhyNoState {
		t.Fatalf("state unreadable, no sample: %+v, want unknown (%q)", n, copyAgeWhyNoState)
	}

	// An answer whose through cannot be told: the distance is unknown for
	// the same reason; one that waits on the index: pending.
	var e copyAges
	e.note("s3", copyAnswer{side: "copy", ranAt: ran})
	e.captureRead("s3", captureSample{readAt: ran}, nil, "")
	if n := e.now("s3"); n.behindKnown || n.behindWhy != copyAgeWhyNoFiles {
		t.Fatalf("through unknown: %+v, want unknown (%q)", n, copyAgeWhyNoFiles)
	}
	e.note("s3", copyAnswer{side: "copy", ranAt: ran, ok: true, files: map[string][]string{"/snap/2026": {"/snap/2026/t.parquet"}}})
	if n := e.now("s3"); n.behindKnown || !n.behindPending {
		t.Fatalf("through pending: %+v, want pending", n)
	}
	// Placed in the index: the snapshot's cut id is the answer's.
	for key := range e.pendingFor()["s3"] {
		e.settleMark("s3", key, through, 100, "")
	}
	e.captureRead("s3", caught(ran, 100, through), nil, "")
	if n := e.now("s3"); !n.behindKnown || n.behind != 0 {
		t.Fatalf("snapshot cut at the newest event: %+v, want 0", n)
	}
	e.captureRead("s3", caught(ran, 101, through), nil, "")
	if n := e.now("s3"); !n.behindKnown || n.behind != time.Hour {
		t.Fatalf("an event after the snapshot's cut, not newer in time: %+v, want 1h", n)
	}
}

// A server forgotten while its state was read is not brought back.
func TestCopyAges_captureReadNeverRevivesAForgottenServer_2556(t *testing.T) {
	var c copyAges
	c.note("gone", copyAnswer{side: "copy", ranAt: t0, tailThrough: t0, allTailed: true})
	c.forget("gone")
	c.captureRead("gone", caught(t0, 1, t0), nil, "")
	if _, ok := c.by["gone"]; ok {
		t.Fatal("a forgotten server came back with a sample")
	}
}

// The status document must not turn into one index read per request: a
// server is due when never read, when its sample is a period old, or when
// its last answer ran after it was read.
func TestCopyAges_captureDue_2556(t *testing.T) {
	var c copyAges
	c.note("s", copyAnswer{side: "copy", ranAt: t0, tailThrough: t0, allTailed: true})
	if due := c.captureDue(t0); len(due) != 1 {
		t.Fatalf("never read: due %v", due)
	}
	c.captureRead("s", captureSample{readAt: t0.Add(time.Second)}, nil, "")
	if due := c.captureDue(t0.Add(10 * time.Second)); len(due) != 0 {
		t.Fatalf("read 9s ago, after the answer: due %v", due)
	}
	if due := c.captureDue(t0.Add(time.Second + captureReadEvery)); len(due) != 1 {
		t.Fatalf("read a period ago: due %v", due)
	}
	c.note("s", copyAnswer{side: "copy", ranAt: t0.Add(5 * time.Second), tailThrough: t0, allTailed: true})
	if due := c.captureDue(t0.Add(6 * time.Second)); len(due) != 1 {
		t.Fatalf("answer after the read: due %v", due)
	}
}

// An answer over several tables or snapshot directories holds every event
// through the least of their ids; any unknown makes it unknown.
func TestLeastEventID_2556(t *testing.T) {
	for _, c := range []struct {
		ids  []uint64
		want uint64
	}{
		{nil, 0},
		{[]uint64{7}, 7},
		{[]uint64{9, 4, 6}, 4},
		{[]uint64{4, 9}, 4},
		{[]uint64{9, 0, 4}, 0},
	} {
		if got := leastEventID(c.ids...); got != c.want {
			t.Errorf("leastEventID(%v) = %d, want %d", c.ids, got, c.want)
		}
	}
	if got := leastKnownID(10, t0, 0); got != 0 {
		t.Errorf("snapshot 10 with tails of unknown id: %d, want 0", got)
	}
	if got := leastKnownID(10, time.Time{}, 0); got != 10 {
		t.Errorf("snapshot 10, no tails: %d, want 10", got)
	}
	if got := leastKnownID(10, t0, 8); got != 8 {
		t.Errorf("snapshot 10, tails 8: %d, want 8", got)
	}
}
