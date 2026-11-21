package console

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The PostgreSQL port's wait for the source (#2985): a connection the source
// answered at a position P of its WAL is answered by the copy only once the
// copy's tails hold every commit before P. PostgreSQL only; the MySQL port's
// twin is waitForSource (#2645), which this follows step for step with a
// PostgreSQL source's positions:
//
//   - What it waits on is capture's SAVED position reaching P, never the
//     newest indexed event: on a quiet source the newest event stays behind
//     P forever (P moves with WAL that holds no row). Here the saved position
//     is the greater of the commit LSN of capture's last whole transaction
//     (PGCheckpoint) and the walsender position below which every commit is
//     indexed (PGSentThrough, #2746), which moves on a source with no commit.
//   - Then an update of the tails that BEGAN after that position was first
//     read: such an update reads everything capture saved by then. One
//     already published since needs no wake (a quiet source); otherwise the
//     follower is woken, and the wait is for its next publish.
//   - Reads of the saved position are shared by the statements of a server
//     that wait at the same moment (one read every pgCopyWaitPoll at most),
//     and a statement stops waiting at its own context: the limit, its
//     statement_timeout, a cancel request, the client leaving.
//   - A statement does not wait when waiting cannot help, decided before the
//     copy runs: the server has no tails, capture saved no position, or
//     capture is not moving fast enough to reach P within the time left
//     (pgCopyWaitHopeless, from the saved positions read in the last seconds).

const (
	// pgCopyWaitPoll is how often a waiting statement reads capture's saved
	// position (freshWaitPoll's value).
	pgCopyWaitPoll = 10 * time.Millisecond
	// pgCopyWaitWindow is how far back the saved positions it estimates
	// capture's pace from go.
	pgCopyWaitWindow = 5 * time.Second
	// pgCopyWaitSettle is how long a saved position must have been watched
	// before a position that has not moved says capture will not move
	// either.
	pgCopyWaitSettle = time.Second
)

// pgCopySaved is one read of capture's saved position.
type pgCopySaved struct {
	lsn uint64
	ok  bool // capture saved one
	at  time.Time
}

// pgCopyWaitServer is one server's shared reads.
type pgCopyWaitServer struct {
	mu      sync.Mutex
	reading chan struct{} // closed when the read in flight ends
	last    pgCopySaved
	lastErr error
	// seen is the saved position last read and seenAt when it was first
	// read, on the keeper's clock: every commit below it was indexed by then.
	seen    uint64
	seenAt  time.Time
	samples []pgCopySaved
	// warnedAt is when a failing read was last said at warn level.
	warnedAt time.Time
}

var pgCopyWaits struct {
	mu sync.Mutex
	by map[string]*pgCopyWaitServer
}

func pgCopyWaitFor(id string) *pgCopyWaitServer {
	pgCopyWaits.mu.Lock()
	defer pgCopyWaits.mu.Unlock()
	if pgCopyWaits.by == nil {
		pgCopyWaits.by = map[string]*pgCopyWaitServer{}
	}
	w := pgCopyWaits.by[id]
	if w == nil {
		w = &pgCopyWaitServer{}
		pgCopyWaits.by[id] = w
	}
	return w
}

// saved is capture's saved position, read now or by a read that ended under
// pgCopyWaitPoll/2 ago, and seenAt when that position was first read.
func (w *pgCopyWaitServer) saved(ctx context.Context, read func(context.Context) (uint64, bool, error), now func() time.Time) (pgCopySaved, time.Time, error) {
	w.mu.Lock()
	if w.reading == nil && (w.last.at.IsZero() || now().Sub(w.last.at) >= pgCopyWaitPoll/2) {
		ch := make(chan struct{})
		w.reading = ch
		go func() {
			rctx, cancel := context.WithTimeout(context.Background(), freshWaitReadTimeout)
			lsn, ok, err := read(rctx)
			cancel()
			at := now()
			w.mu.Lock()
			w.last, w.lastErr = pgCopySaved{lsn: lsn, ok: ok, at: at}, err
			if err == nil && ok {
				if lsn != w.seen || w.seenAt.IsZero() {
					w.seen, w.seenAt = lsn, at
				}
				w.samples = append(w.samples, w.last)
				cut := at.Add(-pgCopyWaitWindow)
				for len(w.samples) > 1 && w.samples[0].at.Before(cut) {
					w.samples = w.samples[1:]
				}
			}
			w.reading = nil
			w.mu.Unlock()
			close(ch)
		}()
	}
	ch := w.reading
	w.mu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		case <-ctx.Done():
			return pgCopySaved{}, time.Time{}, ctx.Err()
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lastErr != nil {
		return pgCopySaved{}, time.Time{}, w.lastErr
	}
	seenAt := w.last.at
	if w.last.ok && w.seen == w.last.lsn && w.seenAt.Before(seenAt) {
		seenAt = w.seenAt
	}
	return w.last, seenAt, nil
}

func (w *pgCopyWaitServer) recent() []pgCopySaved {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]pgCopySaved(nil), w.samples...)
}

// pgCopyWaitHopeless says why capture cannot reach want within left, from
// the saved positions read lately (oldest first, the last one now's), or ""
// when it may. Capture's pace is the distance its saved position moved over
// those reads: at that pace the distance to want must be covered in left.
// A position that has not moved for pgCopyWaitSettle is not moving; one
// watched for less says nothing yet, and the statement waits.
func pgCopyWaitHopeless(want uint64, samples []pgCopySaved, left time.Duration) string {
	if len(samples) == 0 || left <= 0 {
		return ""
	}
	first, last := samples[0], samples[len(samples)-1]
	if last.lsn >= want {
		return ""
	}
	span := last.at.Sub(first.at)
	if span < pgCopyWaitSettle {
		return ""
	}
	moved := float64(last.lsn) - float64(first.lsn)
	if moved <= 0 {
		return fmt.Sprintf("capture's saved position has not moved for %s and is %d bytes of WAL short of the source's", span.Round(time.Millisecond), want-last.lsn)
	}
	need := time.Duration(float64(want-last.lsn) / moved * float64(span))
	if need > left {
		return fmt.Sprintf("capture is %d bytes of WAL short of the source's position, about %s at its pace, more than the %s left", want-last.lsn, need.Round(time.Millisecond), left.Round(time.Millisecond))
	}
	return ""
}

// pgSavedPosition is the greater of capture's two saved positions: every
// commit of the source below it is in the index.
func pgSavedPosition(b *bundle) func(context.Context) (uint64, bool, error) {
	return func(ctx context.Context) (uint64, bool, error) {
		sent, sentOK, err := reconstruct.PGSentThrough(ctx, b.db)
		if err != nil {
			return 0, false, err
		}
		lsn, _, ok, err := reconstruct.PGCheckpoint(ctx, b.db)
		if err != nil {
			return 0, false, err
		}
		if sentOK {
			lsn, ok = max(lsn, sent), true
		}
		return lsn, ok, nil
	}
}

// PGCopyWaitOutcome is what WaitPGTailsPast found: PGCopyNoWait, there is
// nothing to wait on and the copy runs for the answer's age to decide;
// PGCopyReached, the tails hold the position; PGCopyNotReached, they do not
// in time (or cannot), and the source answers.
type PGCopyWaitOutcome int

const (
	PGCopyNoWait PGCopyWaitOutcome = iota
	PGCopyReached
	PGCopyNotReached
)

// pgCopyWaitWarnEvery is how often a server's failing read of capture's
// saved position is said at warn level: every statement for its copy goes to
// the source while it fails.
const pgCopyWaitWarnEvery = time.Minute

// warnRead says, at most every pgCopyWaitWarnEvery, that the saved position
// could not be read.
func (w *pgCopyWaitServer) warnRead(id string, err error) {
	w.mu.Lock()
	now := time.Now()
	due := now.Sub(w.warnedAt) >= pgCopyWaitWarnEvery
	if due {
		w.warnedAt = now
	}
	w.mu.Unlock()
	if due {
		slog.Warn("pgshim: capture's saved position cannot be read, so the copy does not wait for the source's position: statements on connections the source answered go to the source", "server", id, "error", err)
	}
}

// WaitPGTailsPast waits until this connection's server's tails hold every
// commit of the source before want (a WAL position), within ctx: Binding's
// CopyWait for the PostgreSQL port. It is decided before anything runs on
// the copy: CopyNotReached sends the statement to the source, CopyNoWait
// lets the copy run (the server keeps no recent changes yet; that run starts
// them) for the answer's age to decide.
func (q *SQLOnCopy) WaitPGTailsPast(ctx context.Context, want uint64) (PGCopyWaitOutcome, string) {
	if q == nil || q.s == nil || q.b == nil || q.b.db == nil {
		return PGCopyNoWait, "the server's index is not open"
	}
	q.s.tails.mu.Lock()
	k := q.s.tails.by[q.id]
	q.s.tails.mu.Unlock()
	if k == nil || !q.s.pgTailsFor(q.id, q.b) {
		return PGCopyNoWait, "no recent changes are kept for this server yet (fast mode, or no statement read its copy yet)"
	}
	if _, ok := k.src.(*pgTailSource); !ok {
		return PGCopyNoWait, "the copy's recent changes are not a PostgreSQL server's"
	}
	k.pub.mu.Lock()
	none := k.pub.views == nil
	k.pub.mu.Unlock()
	if none {
		return PGCopyNoWait, "no version of the recent changes is published yet"
	}
	w := pgCopyWaitFor(q.id)
	read := pgSavedPosition(q.b)
	var since time.Time
	for {
		saved, seenAt, err := w.saved(ctx, read, k.now)
		switch {
		case ctx.Err() != nil:
			return PGCopyNotReached, "the wait ran out before capture reached the source's position"
		case err != nil:
			w.warnRead(q.id, err)
			return PGCopyNotReached, "capture's saved position could not be read: " + err.Error()
		case !saved.ok:
			return PGCopyNotReached, "capture saved no position yet"
		}
		if saved.lsn >= want {
			since = seenAt
			break
		}
		left := time.Duration(1<<63 - 1)
		if d, ok := ctx.Deadline(); ok {
			left = time.Until(d)
		}
		if why := pgCopyWaitHopeless(want, w.recent(), left); why != "" {
			return PGCopyNotReached, why
		}
		t := time.NewTimer(pgCopyWaitPoll)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return PGCopyNotReached, "the wait ran out before capture reached the source's position"
		}
	}
	if !k.publishedSince(since) {
		k.wakeFollower()
	}
	if !k.waitPublishedSince(ctx, since) {
		return PGCopyNotReached, "the wait ran out before the copy's recent changes were brought up to the source's position"
	}
	return PGCopyReached, ""
}
