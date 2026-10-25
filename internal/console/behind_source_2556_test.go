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
// read like Seconds_Behind_Source. Capture caught up when the answer ran:
// newest indexed − through. Otherwise: ran − through.

var t0 = time.Date(2026, 10, 25, 14, 0, 0, 0, time.UTC)

func TestBehindSource_noStampIsRanMinusThrough_2556(t *testing.T) {
	ran, through := t0, t0.Add(-time.Hour)
	if got := behindSourceOf(ran, through, captureSample{newest: through}); got != time.Hour {
		t.Fatalf("no stamp: %v, want 1h (ran − through)", got)
	}
}

func TestBehindSource_recentStampIsNewestMinusThrough_2556(t *testing.T) {
	ran := t0
	// The source sat idle for an hour: the newest event is an hour old, and
	// the answer read through it. Caught up 20s before the answer: 0, not 1h.
	s := captureSample{caughtUp: ran.Add(-20 * time.Second), newest: t0.Add(-time.Hour)}
	if got := behindSourceOf(ran, t0.Add(-time.Hour), s); got != 0 {
		t.Fatalf("idle source, caught up: %v, want 0", got)
	}
	// The answer read a snapshot 5 minutes older than the newest event.
	if got := behindSourceOf(ran, s.newest.Add(-5*time.Minute), s); got != 5*time.Minute {
		t.Fatalf("snapshot behind the index, caught up: %v, want 5m", got)
	}
}

// last_event_time is whole seconds and a snapshot can be cut past it: never
// below zero.
func TestBehindSource_cutPastTheNewestEventIsZero_2556(t *testing.T) {
	s := captureSample{caughtUp: t0, newest: t0.Add(-time.Hour)}
	if got := behindSourceOf(t0, s.newest.Add(500*time.Millisecond), s); got != 0 {
		t.Fatalf("through past newest: %v, want 0", got)
	}
}

// The loop reads every 30 seconds, so the stamp can be after the answer.
func TestBehindSource_aStampAfterTheAnswerCounts_2556(t *testing.T) {
	s := captureSample{caughtUp: t0.Add(40 * time.Second), newest: t0.Add(-time.Hour)}
	if got := behindSourceOf(t0, s.newest, s); got != 0 {
		t.Fatalf("stamp 40s after the answer: %v, want 0", got)
	}
}

// Past the window, capture is not known to be caught up: ran − through, which
// grows with each query and never freezes at the last healthy value.
func TestBehindSource_anOldStampGrowsWithEachQuery_2556(t *testing.T) {
	through := t0.Add(-time.Hour)
	s := captureSample{caughtUp: t0.Add(-caughtUpWindow - time.Second), newest: through}
	first := behindSourceOf(t0, through, s)
	second := behindSourceOf(t0.Add(10*time.Second), through, s)
	if first != time.Hour || second != time.Hour+10*time.Second {
		t.Fatalf("old stamp: %v then %v, want 1h then 1h10s", first, second)
	}
	// A stamp long after the answer says nothing of when it ran either.
	s.caughtUp = t0.Add(caughtUpWindow + time.Second)
	if got := behindSourceOf(t0, through, s); got != time.Hour {
		t.Fatalf("stamp past the window after the answer: %v, want 1h", got)
	}
	// And the edge of the window still counts.
	s.caughtUp = t0.Add(-caughtUpWindow)
	if got := behindSourceOf(t0, through, s); got != 0 {
		t.Fatalf("stamp exactly one window old: %v, want 0", got)
	}
}

func TestBehindSource_caughtUpWithNothingIndexedIsZero_2556(t *testing.T) {
	if got := behindSourceOf(t0, t0.Add(-time.Minute), captureSample{caughtUp: t0}); got != 0 {
		t.Fatalf("caught up, no event indexed: %v, want 0", got)
	}
}

func TestBehindSource_windowIsTwoHeartbeatPeriods_2556(t *testing.T) {
	if caughtUpWindow != 2*captureHeartbeatPeriod || captureHeartbeatPeriod != 30*time.Second {
		t.Fatalf("window %v, period %v", caughtUpWindow, captureHeartbeatPeriod)
	}
}

// readCaptureSample reads the stamp's AGE on the index clock and places it
// on the router's: the two machines' clocks are never compared.
func TestReadCaptureSample_stampOnTheRouterClock_2556(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	newest := t0.Add(-time.Hour)
	mock.ExpectQuery(`SELECT TIMESTAMPDIFF\(MICROSECOND, caught_up_at, UTC_TIMESTAMP\(6\)\), last_event_time FROM stream_state WHERE id = 1`).
		WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow(int64(7_000_000), newest))
	s, err := readCaptureSample(context.Background(), db, func() time.Time { return t0 })
	if err != nil {
		t.Fatal(err)
	}
	if !s.caughtUp.Equal(t0.Add(-7*time.Second)) || !s.newest.Equal(newest) || !s.readAt.Equal(t0) {
		t.Fatalf("sample %+v: want caught up 7s before the read, newest %v", s, newest)
	}
}

func TestReadCaptureSample_nullStampAndNoRow_2556(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`FROM stream_state`).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow(nil, nil))
	s, err := readCaptureSample(context.Background(), db, func() time.Time { return t0 })
	if err != nil || !s.caughtUp.IsZero() || !s.newest.IsZero() {
		t.Fatalf("NULLs: %+v, %v; want an empty sample", s, err)
	}
	mock.ExpectQuery(`FROM stream_state`).WillReturnError(sql.ErrNoRows)
	s, err = readCaptureSample(context.Background(), db, func() time.Time { return t0 })
	if err != nil || !s.caughtUp.IsZero() || !s.readAt.Equal(t0) {
		t.Fatalf("no row: %+v, %v; want an empty sample read at t0", s, err)
	}
}

// An index an older capture last wrote has no caught_up_at until capture
// restarts on a newer build, and the router never migrates the index: the
// sample has no stamp, and the error is not one.
func TestReadCaptureSample_missingColumnIsNoStamp_2556(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	newest := t0.Add(-time.Hour)
	mock.ExpectQuery(`caught_up_at`).WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'caught_up_at' in 'field list'"})
	mock.ExpectQuery(`SELECT last_event_time FROM stream_state WHERE id = 1`).
		WillReturnRows(sqlmock.NewRows([]string{"b"}).AddRow(newest))
	s, err := readCaptureSample(context.Background(), db, func() time.Time { return t0 })
	if err != nil || !s.caughtUp.IsZero() || !s.newest.Equal(newest) {
		t.Fatalf("missing column: %+v, %v; want no stamp and the newest event", s, err)
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
	boom := errors.New("connection refused")
	mock.ExpectQuery(`FROM stream_state`).WillReturnError(boom)
	if _, err := readCaptureSample(context.Background(), db, time.Now); !errors.Is(err, boom) {
		t.Fatalf("err %v, want %v", err, boom)
	}
}

// The status numbers: the last answer's distance once its through and the
// server's capture sample are both known, pending while either waits, and
// unknown, with a fixed reason, when either cannot be told.
func TestCopyAges_behindSourceNow_2556(t *testing.T) {
	ran, through := t0, t0.Add(-time.Hour)
	tailed := copyAnswer{side: "copy", ranAt: ran, tailThrough: through, allTailed: true}

	var c copyAges
	c.note("s1", tailed)
	if n := c.now("s1"); n.behindKnown || !n.behindPending {
		t.Fatalf("no capture sample read yet: %+v, want pending", n)
	}
	c.captureRead("s1", captureSample{caughtUp: ran.Add(-10 * time.Second), newest: through, readAt: ran}, nil)
	n := c.now("s1")
	if !n.behindKnown || n.behind != 0 || !n.behindRan.Equal(ran) || n.behindPending {
		t.Fatalf("idle source caught up: %+v, want 0 behind, ran %v", n, ran)
	}
	// The same answer with a stamp out of the window is ran − through.
	c.captureRead("s1", captureSample{caughtUp: ran.Add(-time.Hour), newest: through, readAt: ran}, nil)
	if n := c.now("s1"); !n.behindKnown || n.behind != time.Hour {
		t.Fatalf("old stamp: %+v, want 1h", n)
	}
	// A failed read keeps the last sample: its stamp is still a real one.
	c.captureRead("s1", captureSample{}, errors.New("dial tcp: refused"))
	if n := c.now("s1"); !n.behindKnown || n.behind != time.Hour {
		t.Fatalf("failed read after a good one: %+v, want the last sample's 1h", n)
	}

	// With no sample ever read and the index unreachable: unknown, with the
	// fixed reason and never the error's text.
	var d copyAges
	d.note("s2", tailed)
	d.captureRead("s2", captureSample{}, errors.New("dial tcp 10.0.0.7:3306: refused"))
	if n := d.now("s2"); n.behindKnown || n.behindPending || n.behindWhy != copyAgeWhyNoIndex {
		t.Fatalf("index unreachable, no sample: %+v, want unknown (%q)", n, copyAgeWhyNoIndex)
	}

	// An answer whose through cannot be told: the distance is unknown for
	// the same reason; one that waits on the index: pending.
	var e copyAges
	e.note("s3", copyAnswer{side: "copy", ranAt: ran})
	e.captureRead("s3", captureSample{readAt: ran}, nil)
	if n := e.now("s3"); n.behindKnown || n.behindWhy != copyAgeWhyNoFiles {
		t.Fatalf("through unknown: %+v, want unknown (%q)", n, copyAgeWhyNoFiles)
	}
	e.note("s3", copyAnswer{side: "copy", ranAt: ran, ok: true, files: map[string][]string{"/snap/2026": {"/snap/2026/t.parquet"}}})
	if n := e.now("s3"); n.behindKnown || !n.behindPending {
		t.Fatalf("through pending: %+v, want pending", n)
	}
}

// The window is built on capture's own heartbeat period: a change there must
// move it here.
func TestBehindSource_periodIsCaptures_2556(t *testing.T) {
	if captureHeartbeatPeriod != streamrun.SourceHeartbeatPeriod {
		t.Fatalf("console says %v, capture asks the source for %v", captureHeartbeatPeriod, streamrun.SourceHeartbeatPeriod)
	}
}
