package consoleapp

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The Overview's "is capture caught up" line for a PostgreSQL source
// (#2758). The source's WAL moves for the whole cluster and the capture
// saves its position on a timer, so "the source is ahead" is what a healthy
// capture reads: the answer is decided against an EARLIER read of the source.

func TestPGCaptureStatusFrom(t *testing.T) {
	t0 := captureT0
	at := func(d time.Duration) time.Time { return t0.Add(d) }
	held := func(lsn uint64, d time.Duration) *pgCaptureSample { return &pgCaptureSample{lsn: lsn, at: at(d)} }
	const (
		upToDate = console.CaptureStateUpToDate
		behind   = console.CaptureStateBehind
		unknown  = console.CaptureStateUnknown
	)
	// saved is a read whose position was saved d before it was read.
	saved := func(sent, source uint64, d time.Duration) pgCaptureRead {
		return pgCaptureRead{sent: sent, source: source, savedAgo: d}
	}
	cases := []struct {
		name  string
		r     pgCaptureRead
		prev  *pgCaptureSample
		now   time.Duration
		state string
		// detail is the whole reason; retry whether the page is asked to
		// ask again; next the read the following answer is held to.
		detail string
		retry  bool
		next   *pgCaptureSample
	}{
		{name: "equal on a first read",
			r: saved(500, 500, 0), now: 0,
			state: upToDate, next: held(500, 0)},
		{name: "equal, whatever was held and however long ago it was saved",
			r: saved(900, 900, 6*time.Hour), prev: held(700, 0), now: time.Hour,
			state: upToDate, next: held(900, time.Hour)},
		{name: "the source ahead on a first read is asked again",
			r: saved(500, 600, 0), now: 0,
			state: unknown, detail: captureDetailPGFirstRead, retry: true, next: held(600, 0)},

		{name: "the capture reached where the source was at the earlier read",
			r: saved(650, 800, 5*time.Second), prev: held(600, 0), now: 25 * time.Second,
			state: upToDate, next: held(800, 25*time.Second)},
		{name: "exactly where the source was",
			r: saved(600, 800, 5*time.Second), prev: held(600, 0), now: 25 * time.Second,
			state: upToDate, next: held(800, 25*time.Second)},
		{name: "reached at the edge of the time it is given",
			r: saved(650, 800, 0), prev: held(600, 0), now: pgReachWithin,
			state: upToDate, next: held(800, pgReachWithin)},
		{name: "reached, but the earlier read is too old to speak for now",
			r: saved(650, 800, 0), prev: held(600, 0), now: pgReachWithin + time.Second,
			state: unknown, detail: captureDetailPGFirstRead, retry: true, next: held(800, pgReachWithin+time.Second)},
		{name: "reached with a save at the edge of recent",
			r: saved(650, 800, pgSaveFresh), prev: held(600, 0), now: 40 * time.Second,
			state: upToDate, next: held(800, 40*time.Second)},
		{name: "reached, and then the capture stopped saving",
			r: saved(650, 800, pgSaveFresh+time.Second), prev: held(600, 0), now: 40 * time.Second,
			state: unknown, detail: captureDetailPGNotSaving, retry: true, next: held(800, 40*time.Second)},

		{name: "not reached, and no position saved since the earlier read",
			r: saved(550, 800, 30*time.Second), prev: held(600, 0), now: 25 * time.Second,
			state: unknown, detail: captureDetailPGNotSaved, retry: true, next: held(600, 0)},
		{name: "not reached, a position saved too soon after the earlier read",
			r: saved(550, 800, 5*time.Minute-captureConfirmGap+time.Second), prev: held(600, 0), now: 5 * time.Minute,
			state: unknown, detail: captureDetailPGNotSaved, retry: true, next: held(600, 0)},
		{name: "not reached with a position saved since, still inside the time it is given",
			r: saved(550, 800, time.Second), prev: held(600, 0), now: pgReachWithin - time.Second,
			state: unknown, detail: captureDetailPGReading, retry: true, next: held(600, 0)},
		{name: "not reached after the time it is given: behind",
			r: saved(550, 800, time.Second), prev: held(600, 0), now: pgReachWithin,
			state: behind, detail: captureDetailPGBehind, next: held(600, 0)},
		{name: "behind with the save exactly the confirming gap after the earlier read",
			r: saved(550, 800, time.Hour-captureConfirmGap), prev: held(600, 0), now: time.Hour,
			state: behind, detail: captureDetailPGBehind, next: held(600, 0)},
		{name: "behind on a source that has not moved since the earlier read",
			r: saved(550, 600, time.Second), prev: held(600, 0), now: pgReachWithin,
			state: behind, detail: captureDetailPGBehind, next: held(600, 0)},
		{name: "a save exactly the confirming gap after the earlier read counts as since",
			r: saved(550, 800, 25*time.Second-captureConfirmGap), prev: held(600, 0), now: 25 * time.Second,
			state: unknown, detail: captureDetailPGReading, retry: true, next: held(600, 0)},
		{name: "one position short is still behind",
			r: saved(599, 800, 0), prev: held(600, 0), now: time.Hour,
			state: behind, detail: captureDetailPGBehind, next: held(600, 0)},

		{name: "the capture past the source: nothing held survives",
			r: saved(900, 800, 0), prev: held(600, 0), now: 25 * time.Second,
			state: unknown, detail: captureDetailPGPast, next: nil},
		{name: "the source before where it was: read as a first read",
			r: saved(100, 300, 0), prev: held(600, 0), now: 25 * time.Second,
			state: unknown, detail: captureDetailPGFirstRead, retry: true, next: held(300, 25*time.Second)},
		{name: "a read that failed keeps what was held",
			r: pgCaptureRead{detail: captureDetailSourceSilent, cause: "dial"}, prev: held(600, 0), now: 25 * time.Second,
			state: unknown, detail: captureDetailSourceSilent, next: held(600, 0)},
		{name: "a read that failed with nothing held",
			r: pgCaptureRead{detail: captureDetailPGNoPosition}, now: 0,
			state: unknown, detail: captureDetailPGNoPosition, next: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, next := pgCaptureStatusFrom(tc.r, tc.prev, at(tc.now))
			if got.State != tc.state || got.Detail != tc.detail || (got.RetryInSeconds > 0) != tc.retry {
				t.Errorf("answer = %+v, want state %q detail %q retry %v", got, tc.state, tc.detail, tc.retry)
			}
			switch {
			case (next == nil) != (tc.next == nil):
				t.Errorf("held next = %+v, want %+v", next, tc.next)
			case next != nil && (next.lsn != tc.next.lsn || !next.at.Equal(tc.next.at)):
				t.Errorf("held next = %+v, want %+v", *next, *tc.next)
			}
		})
	}
}

// Through the reporter: a healthy capture on a source that keeps writing is
// unknown once, then up to date, and a capture that stopped is called behind
// only after the time it is given.
func TestCaptureStatus_postgres(t *testing.T) {
	e := console.ServerEntry{ID: "p", Name: "p", DSN: "u:p@tcp(idx:3306)/p", SourceDSN: "postgres://u:pw@h/db", Flavor: console.FlavorPostgres}
	var next pgCaptureRead
	reads := 0
	c, clock := captureReporter("", func(context.Context, string, string) captureProbeResult {
		t.Error("a PostgreSQL server was read with the MySQL read")
		return captureProbeResult{}
	})
	c.pg.read = func(_ context.Context, idx, src string) pgCaptureRead {
		reads++
		if idx != e.DSN || src != e.SourceDSN {
			t.Errorf("read %q <- %q", idx, src)
		}
		return next
	}
	ask := func() console.CaptureStatus { return c.CaptureStatus(context.Background(), e) }

	next = pgCaptureRead{sent: 500, source: 600}
	first := ask()
	if first.State != console.CaptureStateUnknown || first.RetryInSeconds == 0 || first.ServerID != "p" {
		t.Fatalf("first read: %+v", first)
	}
	// Inside the answer's lifetime nothing is read again.
	if again := ask(); again != first || reads != 1 {
		t.Fatalf("a second ask read the source again (%d reads): %+v", reads, again)
	}
	// The page asks again when told to: the answer has expired by then.
	*clock = clock.Add(captureStatusRetry)
	next = pgCaptureRead{sent: 700, source: 900, savedAgo: 2 * time.Second}
	if got := ask(); got.State != console.CaptureStateUpToDate || reads != 2 {
		t.Fatalf("after the capture passed where the source was: %+v (%d reads)", got, reads)
	}

	// The capture stops saving right there; the source keeps writing. 700 is
	// short of 900, where the source was at the read now held, and nothing
	// was saved since that read.
	*clock = clock.Add(captureStatusTTL)
	next = pgCaptureRead{sent: 700, source: 1000, savedAgo: captureStatusTTL + 2*time.Second}
	if got := ask(); got.State != console.CaptureStateUnknown || got.Detail != captureDetailPGNotSaved {
		t.Fatalf("a capture that stopped saving: %+v", got)
	}
	// It saves again without having read further, and the time it is given runs out.
	*clock = clock.Add(pgReachWithin)
	next = pgCaptureRead{sent: 700, source: 1100, savedAgo: time.Second}
	if got := ask(); got.State != console.CaptureStateBehind || got.Detail != captureDetailPGBehind {
		t.Fatalf("a capture that saves without advancing: %+v", got)
	}
	// And catches up.
	*clock = clock.Add(captureStatusTTL)
	next = pgCaptureRead{sent: 1100, source: 1100}
	if got := ask(); got.State != console.CaptureStateUpToDate {
		t.Fatalf("a capture that caught up: %+v", got)
	}

	// An edit of the source is another server to compare: nothing is reused.
	edited := e
	edited.SourceDSN = "postgres://u:pw@other/db"
	before := reads
	c.pg.read = func(context.Context, string, string) pgCaptureRead {
		reads++
		return pgCaptureRead{detail: captureDetailSourceSilent, cause: "dial"}
	}
	if got := c.CaptureStatus(context.Background(), edited); got.State != console.CaptureStateUnknown || reads != before+1 {
		t.Fatalf("after an edit of the source: %+v (%d reads)", got, reads-before)
	}
}

// A read that panics is an answer, not the end of the daemon.
func TestCaptureStatus_postgresReadPanics(t *testing.T) {
	c := newCaptureStatusReporter("")
	c.pg.read = func(context.Context, string, string) pgCaptureRead { panic("boom") }
	got := c.CaptureStatus(context.Background(), console.ServerEntry{ID: "p", DSN: "u:p@tcp(idx:3306)/p", SourceDSN: "postgres://h/db", Flavor: console.FlavorPostgres})
	if got.State != console.CaptureStateUnknown || got.Detail != captureDetailPanicked {
		t.Fatalf("got %+v", got)
	}
}

func TestPGCapturePosition(t *testing.T) {
	const stamp = int64(7) // seconds since the save, by the index's clock
	cols := []string{"flavor", "binlog_position", "saved_ago", "sent"}
	cases := []struct {
		name    string
		rows    *sqlmock.Rows
		err     error
		detail  string
		sent    uint64
		ago     time.Duration
		wantErr bool
	}{
		{name: "a trusted position", rows: sqlmock.NewRows(cols).AddRow("postgres", int64(4000), stamp, int64(4096)), sent: 4096, ago: 7 * time.Second},
		{name: "no capture on record", rows: sqlmock.NewRows(cols), detail: captureDetailNoCapture},
		{name: "an index without the columns", err: &mysql.MySQLError{Number: 1054, Message: "Unknown column 'pg_sent_through_at'"}, detail: captureDetailPGOldCapture},
		{name: "a position another writer's save untied", rows: sqlmock.NewRows(cols).AddRow("postgres", int64(4000), stamp, nil), detail: captureDetailPGOldCapture},
		{name: "a capture that has received no position yet", rows: sqlmock.NewRows(cols).AddRow("postgres", int64(4000), stamp, int64(0)), detail: captureDetailPGNoPosition},
		{name: "a new capture before its first checkpoint: the row its health poll wrote", rows: sqlmock.NewRows(cols).AddRow("postgres", nil, stamp, nil), detail: captureDetailPGNoPosition},
		{name: "a checkpoint of zero", rows: sqlmock.NewRows(cols).AddRow("postgres", int64(0), stamp, nil), detail: captureDetailPGNoPosition},
		{name: "a save with no stamp", rows: sqlmock.NewRows(cols).AddRow("postgres", int64(4000), nil, int64(4096)), detail: captureDetailPGNoPosition},
		{name: "a save stamped after the index's now", rows: sqlmock.NewRows(cols).AddRow("postgres", int64(4000), int64(-3), int64(4096)), sent: 4096, ago: 0},
		{name: "a MySQL capture's index", rows: sqlmock.NewRows(cols).AddRow("mysql", int64(4000), stamp, nil), detail: captureDetailPGNotPostgres},
		{name: "an index with no flavor", rows: sqlmock.NewRows(cols).AddRow(nil, int64(4000), stamp, int64(4096)), detail: captureDetailPGNotPostgres},
		{name: "a read that failed", err: errors.New("connection reset"), detail: captureDetailCheckpointUnread, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			exp := mock.ExpectQuery(regexp.QuoteMeta(pgCapturePositionQuery))
			if tc.err != nil {
				exp.WillReturnError(tc.err)
			} else {
				exp.WillReturnRows(tc.rows)
			}
			got, err := pgCapturePosition(context.Background(), db)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want an error: %v", err, tc.wantErr)
			}
			if got.detail != tc.detail || got.sent != tc.sent {
				t.Fatalf("got %+v, want detail %q sent %d", got, tc.detail, tc.sent)
			}
			if got.savedAgo != tc.ago {
				t.Fatalf("saved %s ago, want %s", got.savedAgo, tc.ago)
			}
		})
	}
}
