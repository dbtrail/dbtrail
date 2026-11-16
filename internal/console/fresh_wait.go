package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// Fresh mode's wait for the source (#2645). In fresh mode a statement on the
// SQL port reads the newest version of the tails the follower published, so
// a row a client wrote a few milliseconds before is not there yet. With
// BINTRAIL_SQL_FRESH_WAIT set, a statement whose tables all have tails first
// reads where the source's binary log stands, waits until capture's saved
// position (stream_state, saved within about 150 ms of each write since
// #2904) reaches it, wakes the follower, and waits for an update that began
// after that. All of it within the limit; past it the statement answers with
// what is ready, as without the wait, and /status counts it.
//
// What it compares: the source's (file, position) against capture's saved
// one, in binary log order, never the newest indexed event. The source's
// position is the end of its last event, and the index holds rows only: a
// commit's own event, another table's transaction, a DDL or a rotation leave
// the newest indexed row short of it forever. In GTID mode capture saves the
// coordinates of the last event it read too, which is at or past the end of
// every transaction it committed.
//
// A statement does not wait when waiting cannot help: a table it reads has
// no tail (it reads the snapshot), capture is known to be seconds behind (a
// source position read over freshWaitBehind ago is not reached yet), the
// source refused the read (a missing REPLICATION CLIENT; tried again after
// freshWaitRefusedRetry, said once in the log), the two positions are of two
// binary logs (another base name: the source was replaced) or capture saved
// none. A statement in a transaction is MySQL's (the router forwards it), and
// a time-travel statement reads no tails, so neither reaches this.

const (
	// freshWaitDefault is the limit for BINTRAIL_SQL_FRESH_WAIT=on.
	freshWaitDefault = 200 * time.Millisecond
	// freshWaitMax bounds the limit: a statement holds its worker while it
	// waits.
	freshWaitMax = 10 * time.Second
	// freshWaitPoll is how often a waiting statement reads capture's saved
	// position.
	freshWaitPoll = 10 * time.Millisecond
	// freshWaitBehind: a source position read this long ago that capture has
	// not saved yet says capture is behind by more than a limit can cover.
	freshWaitBehind = 2 * time.Second
	// freshWaitRefusedRetry is how long a refused read of the source's
	// position stops the wait for that server.
	freshWaitRefusedRetry = 5 * time.Minute
	// freshWaitSamples bounds the waits kept for the median.
	freshWaitSamples = 1024
	// freshWaitReadTimeout bounds one read of the source's position, which
	// statements share and none cancels.
	freshWaitReadTimeout = 5 * time.Second
)

// FreshWaitSetting reads BINTRAIL_SQL_FRESH_WAIT: off (unset, empty, off,
// false, 0, no), on (on, true, yes, 1: freshWaitDefault), or a duration from
// 1ms to freshWaitMax. warn says why a value was not used (then off).
func FreshWaitSetting() (limit time.Duration, warn string) {
	return freshWaitFrom(os.Getenv("BINTRAIL_SQL_FRESH_WAIT"))
}

func freshWaitFrom(raw string) (time.Duration, string) {
	v := strings.TrimSpace(raw)
	switch strings.ToLower(v) {
	case "", "off", "false", "0", "no":
		return 0, ""
	case "on", "true", "yes", "1":
		return freshWaitDefault, ""
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < time.Millisecond || d > freshWaitMax {
		return 0, fmt.Sprintf("BINTRAIL_SQL_FRESH_WAIT=%q is not on, off or a duration from 1ms to %s (write it like 200ms); statements do not wait", v, freshWaitMax)
	}
	return d, ""
}

// SourcePosition is where a source's binary log stands: its file and the
// end of its last event.
type SourcePosition struct {
	File string
	Pos  uint64
	// Identity is the server's, as capture records it in
	// stream_state.source_identity (@@server_uuid; server_id:<n> on
	// MariaDB); "" when not known. Two different ones are two servers, whose
	// positions say nothing of each other.
	Identity string
}

// SourcePositionReader reads e's source position (SHOW BINARY LOG STATUS,
// SHOW MASTER STATUS on older servers and MariaDB). An error that wraps
// ErrSourcePositionRefused is the server turning the read away.
type SourcePositionReader interface {
	SourcePosition(ctx context.Context, e ServerEntry) (SourcePosition, error)
}

// ErrSourcePositionRefused wraps a refused read of the source's position.
var ErrSourcePositionRefused = errors.New("the source refused the read of its binary log position")

// The reasons a statement did not wait, as /status counts them.
const (
	freshSkipNoTail      = "no_tail"
	freshSkipBehind      = "capture_behind"
	freshSkipRefused     = "position_refused"
	freshSkipUnread      = "position_unread"
	freshSkipOtherLog    = "other_binary_log"
	freshSkipOtherSource = "other_source"
	// freshSkipNotCurrent: capture reached the position and the tails were
	// brought up to date after, but the statement's tables were not among
	// them (a tail refused or frozen in that update): it read no tails.
	freshSkipNotCurrent = "tails_not_current"
	freshSkipNoCapture   = "no_capture_position"
	freshSkipStateUnread = "capture_position_unread"
	freshSkipNoSource    = "no_source"
)

// freshWaits is every server's wait state.
type freshWaits struct {
	mu sync.Mutex
	by map[string]*freshWaitServer
}

func (f *freshWaits) of(id string) *freshWaitServer {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.by == nil {
		f.by = map[string]*freshWaitServer{}
	}
	w := f.by[id]
	if w == nil {
		w = &freshWaitServer{skipped: map[string]uint64{}}
		f.by[id] = w
	}
	return w
}

func (f *freshWaits) snapshot() map[string]*freshWaitServer {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]*freshWaitServer, len(f.by))
	for id, w := range f.by {
		out[id] = w
	}
	return out
}

// positionRead is one read of the source's position, shared by every
// statement that arrives while it runs.
type positionRead struct {
	done chan struct{}
	pos  SourcePosition
	err  error
	at   time.Time // when it ended
}

// savedRead is one read of capture's saved position.
type savedRead struct {
	done chan struct{}
	pos  SourcePosition
	none bool // no capture saved one
	err  error
	at   time.Time // when it ended, on the keeper's clock
	wall time.Time // the same, on the wall clock (how old it is)
}

type positionSample struct {
	pos SourcePosition
	at  time.Time
}

// freshWaitServer is one server's wait state and counts.
type freshWaitServer struct {
	mu       sync.Mutex
	source   *positionRead // in flight
	// queued is the read the statements arriving while source is in flight
	// share, sent when it ends, with queuedReader and queuedEntry.
	queued       *positionRead
	queuedReader SourcePositionReader
	queuedEntry  ServerEntry
	saved    *savedRead    // in flight, or the last one
	samples  []positionSample
	// seen is the saved position last read and seenAt the end of the first
	// read that returned it: every row it names was committed before seenAt.
	seen   SourcePosition
	seenAt time.Time

	refusedUntil time.Time
	refusedWhy   string
	lastWhy      string

	statements, reached, ranOut, cancelled uint64
	skipped                                map[string]uint64
	waits                                  []time.Duration
	next                                   int
}

// RouterFreshWait is one server's waits for the source (#2645), present in
// /status only while BINTRAIL_SQL_FRESH_WAIT is on. Statements counts the
// fresh-mode statements that asked; Reached those whose tails reached the
// source's position within LimitMS, RanOut those that answered with what was
// ready at the limit, Cancelled those whose client left while waiting, and
// Skipped, by reason, those that did not wait at all. MedianWaitMS is over
// the recent statements that waited (reached or ran out).
type RouterFreshWait struct {
	LimitMS      int64             `json:"limit_ms"`
	Statements   uint64            `json:"statements"`
	Reached      uint64            `json:"reached"`
	RanOut       uint64            `json:"ran_out"`
	Cancelled    uint64            `json:"cancelled,omitempty"`
	Skipped      map[string]uint64 `json:"skipped"`
	MedianWaitMS *float64          `json:"median_wait_ms,omitempty"`
	// PositionRefused is why the source turns the read away, while it does
	// (statements do not wait for this server until it is tried again).
	PositionRefused string `json:"position_refused,omitempty"`
	// LastSkipDetail is the last unexpected failure's text (a read that
	// failed for another reason than a refusal).
	LastSkipDetail string `json:"last_skip_detail,omitempty"`
}

func (w *freshWaitServer) numbers(limit time.Duration, now time.Time) RouterFreshWait {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := RouterFreshWait{LimitMS: limit.Milliseconds(), Statements: w.statements, Reached: w.reached,
		RanOut: w.ranOut, Cancelled: w.cancelled, Skipped: map[string]uint64{}, LastSkipDetail: w.lastWhy}
	for k, v := range w.skipped {
		out.Skipped[k] = v
	}
	if now.Before(w.refusedUntil) {
		out.PositionRefused = w.refusedWhy
	}
	if len(w.waits) > 0 {
		d := slices.Clone(w.waits)
		slices.Sort(d)
		ms := float64(d[len(d)/2].Microseconds()) / 1000
		out.MedianWaitMS = &ms
	}
	return out
}

func (w *freshWaitServer) count(outcome string, waited time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.statements++
	switch outcome {
	case "reached":
		w.reached++
	case "ran_out":
		w.ranOut++
	case "cancelled":
		w.cancelled++
		return
	default:
		w.skipped[outcome]++
		return
	}
	if len(w.waits) < freshWaitSamples {
		w.waits = append(w.waits, waited)
	} else {
		w.waits[w.next] = waited
		w.next = (w.next + 1) % freshWaitSamples
	}
}

// sourcePosition is the source's position for a statement, read after the
// statement arrived: a read already sent to the source may have been
// answered before the client's own write committed, so a statement never
// joins one. It joins the next read instead, sent when the one in flight
// ends, which every statement arriving meanwhile shares: a burst costs two
// reads, not one each. Reads are not cancelled by any statement (others may
// share them), only bounded by freshWaitReadTimeout; a statement stops
// waiting at its own ctx.
func (w *freshWaitServer) sourcePosition(ctx context.Context, r SourcePositionReader, e ServerEntry) (SourcePosition, error) {
	w.mu.Lock()
	var call *positionRead
	switch {
	case w.source == nil:
		call = &positionRead{done: make(chan struct{})}
		w.source = call
		go w.readSource(call, r, e)
	case w.queued != nil:
		call = w.queued
	default:
		call = &positionRead{done: make(chan struct{})}
		w.queued = call
		w.queuedReader, w.queuedEntry = r, e
	}
	w.mu.Unlock()
	select {
	case <-call.done:
		return call.pos, call.err
	case <-ctx.Done():
		return SourcePosition{}, ctx.Err()
	}
}

// readSource runs one read, then the next one queued while it ran.
func (w *freshWaitServer) readSource(call *positionRead, r SourcePositionReader, e ServerEntry) {
	for call != nil {
		rctx, cancel := context.WithTimeout(context.Background(), freshWaitReadTimeout)
		pos, err := r.SourcePosition(rctx, e)
		cancel()
		w.mu.Lock()
		call.pos, call.err, call.at = pos, err, time.Now()
		if err == nil {
			w.samples = append(w.samples, positionSample{pos: pos, at: call.at})
			cut := call.at.Add(-5 * freshWaitBehind)
			for len(w.samples) > 1 && w.samples[0].at.Before(cut) {
				w.samples = w.samples[1:]
			}
		}
		next := w.queued
		w.queued = nil
		if next != nil {
			r, e = w.queuedReader, w.queuedEntry
		}
		w.source = next
		w.mu.Unlock()
		close(call.done)
		call = next
	}
}

// oldSample is the newest source position read at least freshWaitBehind
// before now, ok false when none.
func (w *freshWaitServer) oldSample(now time.Time) (SourcePosition, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.samples) - 1; i >= 0; i-- {
		if !w.samples[i].at.After(now.Add(-freshWaitBehind)) {
			return w.samples[i].pos, true
		}
	}
	return SourcePosition{}, false
}

// savedPosition is capture's saved position (read): the read in flight, or
// the last one if it ended within freshWaitPoll/2, or a new one. seenAt is,
// on now's clock, when the position it returns was first read: every row it
// names was committed before then.
func (w *freshWaitServer) savedPosition(ctx context.Context, read savedPositionRead, now func() time.Time) (pos SourcePosition, none bool, seenAt time.Time, err error) {
	w.mu.Lock()
	call := w.saved
	if call == nil || (isClosed(call.done) && time.Since(call.wall) >= freshWaitPoll/2) {
		call = &savedRead{done: make(chan struct{})}
		w.saved = call
		go func() {
			rctx, cancel := context.WithTimeout(context.Background(), freshWaitReadTimeout)
			defer cancel()
			p, none, err := read(rctx)
			at := now()
			w.mu.Lock()
			call.pos, call.none, call.err, call.at, call.wall = p, none, err, at, time.Now()
			if err == nil && !none && (p != w.seen || w.seenAt.IsZero()) {
				w.seen, w.seenAt = p, at
			}
			w.mu.Unlock()
			close(call.done)
		}()
	}
	w.mu.Unlock()
	select {
	case <-call.done:
	case <-ctx.Done():
		return SourcePosition{}, false, time.Time{}, ctx.Err()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if call.err != nil || call.none {
		return SourcePosition{}, call.none, time.Time{}, call.err
	}
	seenAt = call.at
	if w.seen == call.pos && w.seenAt.Before(seenAt) {
		seenAt = w.seenAt
	}
	return call.pos, false, seenAt, nil
}

// savedPositionRead reads capture's saved position; none true when capture
// saved none.
type savedPositionRead func(ctx context.Context) (pos SourcePosition, none bool, err error)

// indexSavedPosition reads it from the index.
func indexSavedPosition(db *sql.DB) savedPositionRead {
	return func(ctx context.Context) (SourcePosition, bool, error) {
		var p SourcePosition
		err := db.QueryRowContext(ctx, `SELECT binlog_file, binlog_position, COALESCE(source_identity, '') FROM stream_state WHERE id = 1`).Scan(&p.File, &p.Pos, &p.Identity)
		if errors.Is(err, sql.ErrNoRows) {
			return SourcePosition{}, true, nil
		}
		return p, false, err
	}
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// binlogBase and binlogSeq split a binary log file name at its last dot:
// binlog.000012 is ("binlog", "000012").
func binlogSplit(file string) (base, seq string) {
	i := strings.LastIndexByte(file, '.')
	if i < 0 {
		return file, ""
	}
	return file[:i], file[i+1:]
}

// positionReached says whether saved is at or past want in binary log order,
// and comparable false when the two name different binary logs (another base
// name, or a name with no sequence number), whose order says nothing.
// Sequence numbers compare by length first, then text: binlog.1000000 is
// after binlog.999999.
func positionReached(want, saved SourcePosition) (reached, comparable bool) {
	wb, ws := binlogSplit(want.File)
	sb, ss := binlogSplit(saved.File)
	if wb != sb || ws == "" || ss == "" {
		return false, false
	}
	if len(ws) != len(ss) {
		return len(ss) > len(ws), true
	}
	if ws != ss {
		return ss > ws, true
	}
	return saved.Pos >= want.Pos, true
}

// waitForSource is the wait of a fresh-mode statement of server id whose
// tables are read, over keeper k and index connection b. It returns when the
// tails cover the source's position as it was when the statement arrived,
// when the limit ran out, when the statement ended, or at once when waiting
// cannot help. It never fails the statement: the statement goes on to read
// what is published, as without it.
func (s *Server) waitForSource(ctx context.Context, id string, b *bundle, k *tailKeeper, read []views.BaselineTable) (done func(tailed bool)) {
	noop := func(bool) {}
	limit := s.freshWait
	if limit <= 0 || s.sourcePos == nil || k == nil || len(read) == 0 {
		return noop
	}
	readSaved := s.freshSavedHook
	if readSaved == nil {
		if b == nil || b.db == nil {
			return noop
		}
		readSaved = indexSavedPosition(b.db)
	}
	w := s.freshWaits.of(id)
	start := time.Now()
	var entry ServerEntry
	found := false
	if id != bootServerID && s.cm != nil && s.cm.reg != nil {
		entry, found = s.cm.reg.Get(id)
	}
	// A PostgreSQL source has no binary log; its LSN is another slice's.
	if !found || entry.SourceDSN == "" || entry.IsPostgres() {
		w.count(freshSkipNoSource, 0)
		return noop
	}
	// A table with no tail reads the snapshot: no wait changes it.
	pub, _ := k.published(read)
	for _, t := range read {
		if _, ok := pub[tailKey(t.Schema, t.Table)]; !ok {
			w.count(freshSkipNoTail, 0)
			return noop
		}
	}
	w.mu.Lock()
	refused := start.Before(w.refusedUntil)
	w.mu.Unlock()
	if refused {
		w.count(freshSkipRefused, 0)
		return noop
	}
	wctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	ended := func() {
		if errors.Is(ctx.Err(), context.Canceled) {
			// The client left (a statement's own time running out is a
			// deadline, and counts as running out).
			w.count("cancelled", time.Since(start))
			return
		}
		w.count("ran_out", time.Since(start))
	}
	want, err := w.sourcePosition(wctx, s.sourcePos, entry)
	if err != nil {
		switch {
		case wctx.Err() != nil:
			ended()
		case errors.Is(err, ErrSourcePositionRefused):
			w.mu.Lock()
			first := w.refusedWhy == ""
			w.refusedUntil, w.refusedWhy = time.Now().Add(freshWaitRefusedRetry), err.Error()
			w.mu.Unlock()
			if first {
				slog.Warn("Engine Routing: fresh mode does not wait for the source on this server: the source refuses to say where its binary log stands; "+
					"grant REPLICATION CLIENT to the account capture uses, or unset BINTRAIL_SQL_FRESH_WAIT", "server", id, "error", err)
			}
			w.count(freshSkipRefused, 0)
		default:
			w.mu.Lock()
			w.lastWhy = err.Error()
			w.mu.Unlock()
			slog.Debug("Engine Routing: the source's binary log position could not be read; the statement does not wait", "server", id, "error", err)
			w.count(freshSkipUnread, 0)
		}
		return noop
	}
	w.mu.Lock()
	// Read again: a refusal that comes back later is said again.
	w.refusedWhy = ""
	w.mu.Unlock()
	var since time.Time
	for {
		saved, none, seenAt, err := w.savedPosition(wctx, readSaved, k.now)
		switch {
		case wctx.Err() != nil:
			ended()
			return noop
		case err != nil:
			w.mu.Lock()
			w.lastWhy = err.Error()
			w.mu.Unlock()
			w.count(freshSkipStateUnread, 0)
			return noop
		case none:
			w.count(freshSkipNoCapture, 0)
			return noop
		}
		if want.Identity != "" && saved.Identity != "" && want.Identity != saved.Identity {
			// Capture's position is of another server (a failover, a
			// restore, a replaced source with the same binary log name).
			w.count(freshSkipOtherSource, 0)
			return noop
		}
		reached, comparable := positionReached(want, saved)
		if !comparable {
			w.count(freshSkipOtherLog, 0)
			return noop
		}
		if reached {
			since = seenAt
			break
		}
		if old, ok := w.oldSample(time.Now()); ok {
			if r, _ := positionReached(old, saved); !r {
				w.count(freshSkipBehind, 0)
				return noop
			}
		}
		t := time.NewTimer(freshWaitPoll)
		select {
		case <-t.C:
		case <-wctx.Done():
			t.Stop()
			ended()
			return noop
		}
	}
	// Capture holds every row the source had: an update that began after
	// that was first seen has them. One published since needs no wake (a
	// quiet source: capture's position has not moved for a while).
	if !k.publishedSince(since) {
		k.wakeFollower()
	}
	if !k.waitPublishedSince(wctx, since) {
		ended()
		return noop
	}
	waited := time.Since(start)
	// Counted once the statement read its tails: reached only if it did.
	return func(tailed bool) {
		if tailed {
			w.count("reached", waited)
			return
		}
		w.count(freshSkipNotCurrent, 0)
	}
}
