package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// The capture status read (#1794) for a PostgreSQL source (#2758).
//
// What is compared is one number on each side:
//
//   - the capture's sent-through position (stream_state.pg_sent_through_lsn,
//     #2746): every commit of the source below it is in the index. Trusted
//     only while pg_sent_through_at is the checkpoint's own stamp, the rule
//     `verify` reads it by, so a save by a writer that does not set it (an
//     older capture, `bintrail-pg reset`) is not read as a position;
//   - how far the source's WAL is flushed (pgcapture.SourceFlushedLSN), which
//     is as far as its walsender reads.
//
// The WAL belongs to the whole PostgreSQL cluster: another database, a
// vacuum, a checkpoint all move it. And the capture saves its position on a
// timer. So the two are equal only on a cluster where nothing at all was
// written since the last save, and "the source is ahead" is the ordinary
// answer of a healthy capture. The rules, then:
//
//   - up to date is the two positions equal, OR the capture's position at
//     or past where the source was at an earlier read no older than
//     pgSampleFresh: the capture holds everything the source had then;
//   - behind is said only against a read at least pgBehindAfter old that the
//     capture, having saved its position since, still has not reached;
//   - everything else is unknown with a reason, and a first read that finds
//     the source ahead asks to be asked again.
//
// No same-server check: the index is a MySQL server.

const (
	// pgSampleFresh bounds how old an earlier read of the source may be for
	// "the capture reached it" to mean up to date. Reaching where the source
	// was an hour ago says nothing about now.
	pgSampleFresh = 2 * time.Minute
	// pgBehindAfter is how long the capture is given to reach a read of the
	// source before it is called behind. Longer than a checkpoint period
	// (10 s) plus the wait for the walsender's next position report.
	pgBehindAfter = 60 * time.Second

	pgCaptureDetailNotPostgres = "the index was not written by a PostgreSQL capture"
	pgCaptureDetailOldCapture  = "the index holds no position to compare: it was last written by a capture that does not record how far the source has sent it"
	pgCaptureDetailNoPosition  = "the capture has not saved how far the source has sent it yet"
	pgCaptureDetailPast        = "the capture's position is past the source's: the source may have been restored or replaced"
	pgCaptureDetailFirstRead   = "the source looked ahead on a first read, and is asked again"
	pgCaptureDetailNotSaved    = "capture has not saved its position since the source was first asked"
	pgCaptureDetailReading     = "the source is ahead and capture is given time to reach it"
	pgCaptureDetailBehind      = "capture has not reached the position the source had at an earlier read"
)

// pgCaptureRead is one read of the two positions. detail is set, with the
// positions zero, when they could not both be read; cause is for the log
// only (scrubbed of both DSNs).
type pgCaptureRead struct {
	detail, cause string
	// sent is the capture's trusted sent-through position; source is how
	// far the source's WAL is flushed, read after sent.
	sent, source uint64
	// checkpoint is when the capture saved sent, by the index's clock.
	checkpoint time.Time
}

// pgCaptureSample is where the source was at one read.
type pgCaptureSample struct {
	lsn uint64
	at  time.Time
}

// pgCaptureSlot is one server's last answer and the read it is held to.
// Its mutex is held across a read: one read per server at a time, and a
// request that waited takes the answer of the read it waited for.
type pgCaptureSlot struct {
	mu     sync.Mutex
	key    string
	answer console.CaptureStatus
	at     time.Time
	ttl    time.Duration
	has    bool
	prev   *pgCaptureSample
	logged string
}

// pgCaptureStatus is the PostgreSQL half of captureStatusReporter.
type pgCaptureStatus struct {
	mu    sync.Mutex
	slots map[string]*pgCaptureSlot
	// read is replaced by tests; nil means pgCaptureFromDBs.
	read func(ctx context.Context, indexDSN, sourceDSN string) pgCaptureRead
}

// captureStatusPG answers for a PostgreSQL server e whose source is source.
func (c *captureStatusReporter) captureStatusPG(ctx context.Context, e console.ServerEntry, source string) console.CaptureStatus {
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	read := pgCaptureFromDBs
	if c.pg.read != nil {
		read = c.pg.read
	}
	key := e.DSN + "\x00" + source

	c.pg.mu.Lock()
	if c.pg.slots == nil {
		c.pg.slots = map[string]*pgCaptureSlot{}
	}
	slot := c.pg.slots[e.ID]
	if slot == nil || slot.key != key {
		slot = &pgCaptureSlot{key: key}
		c.pg.slots[e.ID] = slot
	}
	c.pg.mu.Unlock()

	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.has && now().Sub(slot.at) < slot.ttl {
		return slot.answer
	}
	// Detached from the request: a tab that closes mid-read must not leave
	// "the request was cancelled" behind as the state of the capture.
	r := boundedPGCaptureRead(context.WithoutCancel(ctx), windowProbeTimeout, func(ctx context.Context) (r pgCaptureRead) {
		// This process is the capture too: a panic here would end the
		// daemon over a line on a page.
		defer func() {
			if p := recover(); p != nil {
				slog.Error("capture status: the read of the source panicked", "server", e.Name, "id", e.ID, "panic", p, "stack", string(debug.Stack()))
				r = pgCaptureRead{detail: captureDetailPanicked, cause: "panic"}
			}
		}()
		return read(ctx, e.DSN, source)
	})
	at := now()
	answer, next := pgCaptureStatusFrom(r, slot.prev, at)
	answer.ServerID = e.ID
	slot.answer, slot.at, slot.has, slot.prev = answer, at, true, next
	slot.ttl = captureStatusTTL
	if answer.RetryInSeconds > 0 {
		slot.ttl = captureStatusPendingTTL
	}
	// Once per change of answer, not once per read.
	if said := answer.State + "|" + answer.Detail + "|" + r.cause; slot.logged != said {
		slot.logged = said
		reportCaptureStatus(e, answer, r.cause)
	}
	return answer
}

// boundedPGCaptureRead is boundedCaptureProbe for a PostgreSQL read.
func boundedPGCaptureRead(parent context.Context, within time.Duration, read func(context.Context) pgCaptureRead) pgCaptureRead {
	ctx, cancel := context.WithTimeout(parent, within)
	defer cancel()
	done := make(chan pgCaptureRead, 1) // buffered: a late answer must not block its goroutine
	go func() { done <- read(ctx) }()
	select {
	case r := <-done:
		return r
	case <-ctx.Done():
		return pgCaptureRead{detail: captureDetailTimedOut, cause: "timed out after " + within.String()}
	}
}

// pgCaptureStatusFrom is the answer for one read r, given the read the
// server is held to (prev, nil when none) and the time, with the read the
// next answer is held to. Pure.
func pgCaptureStatusFrom(r pgCaptureRead, prev *pgCaptureSample, now time.Time) (console.CaptureStatus, *pgCaptureSample) {
	unknown := func(detail string) console.CaptureStatus {
		return console.CaptureStatus{State: console.CaptureStateUnknown, Detail: detail}
	}
	again := func(detail string) console.CaptureStatus {
		a := unknown(detail)
		a.RetryInSeconds = int(captureStatusRetry / time.Second)
		return a
	}
	if r.detail != "" {
		// Nothing was learned about the source: the earlier read stays.
		return unknown(r.detail), prev
	}
	here := &pgCaptureSample{lsn: r.source, at: now}
	switch {
	case r.sent > r.source:
		// The source was read AFTER the capture's position, so on one
		// cluster it cannot be before it. What was held is void.
		return unknown(pgCaptureDetailPast), nil
	case r.sent == r.source:
		return console.CaptureStatus{State: console.CaptureStateUpToDate}, here
	case prev == nil || prev.lsn > r.source:
		// A first read, or a source now before where it was: another
		// cluster's positions are not held against this one.
		return again(pgCaptureDetailFirstRead), here
	}
	age := now.Sub(prev.at)
	if r.sent >= prev.lsn {
		if age <= pgSampleFresh {
			return console.CaptureStatus{State: console.CaptureStateUpToDate}, here
		}
		// Reached, but too long ago to speak for now.
		return again(pgCaptureDetailFirstRead), here
	}
	// Not reached. The earlier read stays: it is what the capture is held to.
	switch {
	case r.checkpoint.Sub(prev.at) < captureConfirmGap:
		return again(pgCaptureDetailNotSaved), prev
	case age < pgBehindAfter:
		return again(pgCaptureDetailReading), prev
	}
	return console.CaptureStatus{State: console.CaptureStateBehind, Detail: pgCaptureDetailBehind}, prev
}

// pgCaptureFromDBs reads the capture's position from the index, then the
// source's. In that order: see pgCaptureStatusFrom on a capture past its
// source.
func pgCaptureFromDBs(ctx context.Context, indexDSN, sourceDSN string) pgCaptureRead {
	idx, err := config.Connect(probeDSN(indexDSN))
	if err != nil {
		return pgCaptureRead{detail: captureDetailIndexSilent, cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	defer idx.Close()
	r, err := pgCapturePosition(ctx, idx)
	if err != nil {
		r.cause = config.ScrubDSNError(err, indexDSN, sourceDSN)
		return r
	}
	if r.detail != "" {
		return r
	}
	r.source, err = pgcapture.SourceFlushedLSN(ctx, sourceDSN)
	if err != nil {
		return pgCaptureRead{detail: captureDetailSourceSilent, cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	return r
}

// pgCapturePosition reads the capture's trusted sent-through position and
// when it was saved. A position that cannot be compared is a detail, not an
// error; an error is a read that failed, with its detail.
func pgCapturePosition(ctx context.Context, idx *sql.DB) (pgCaptureRead, error) {
	var (
		flavor     sql.NullString
		checkpoint sql.NullTime
		sent       sql.NullInt64
	)
	// One statement, so the position and the stamp it is trusted by are of
	// the same save.
	err := idx.QueryRowContext(ctx,
		"SELECT flavor, last_checkpoint, IF(pg_sent_through_at <=> last_checkpoint, pg_sent_through_lsn, NULL) FROM stream_state WHERE id = 1").
		Scan(&flavor, &checkpoint, &sent)
	var me *mysql.MySQLError
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return pgCaptureRead{detail: captureDetailNoCapture}, nil
	case errors.As(err, &me) && me.Number == 1054:
		// An index without the columns: the console reads indexes it never
		// migrates, and only a capture that has them adds them.
		return pgCaptureRead{detail: pgCaptureDetailOldCapture}, nil
	case err != nil:
		return pgCaptureRead{detail: captureDetailCheckpointUnread}, err
	case flavor.String != console.FlavorPostgres:
		return pgCaptureRead{detail: pgCaptureDetailNotPostgres}, nil
	case !sent.Valid:
		return pgCaptureRead{detail: pgCaptureDetailOldCapture}, nil
	case sent.Int64 == 0 || !checkpoint.Valid:
		return pgCaptureRead{detail: pgCaptureDetailNoPosition}, nil
	}
	return pgCaptureRead{sent: uint64(sent.Int64), checkpoint: checkpoint.Time}, nil
}
