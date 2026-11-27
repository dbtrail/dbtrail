package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// Whether a PostgreSQL capture's record of the changes it dropped is current,
// for the port (#3202, pgshim capture_stale_3202.go).
//
// Capture saves that record with its position. A capture that saved once and
// then cannot save again keeps later drops in memory only, and the record
// reads as current. The port stops trusting it when both hold:
//
//   - capture's last save is older than pgCaptureStaleBound, by the index's
//     own clock (stream_state.last_checkpoint against the index's
//     UTC_TIMESTAMP(), in one statement, pgCapturePositionQuery): neither the
//     source's clock nor this machine's enters it;
//   - the source's WAL is past the last position capture saved (its trusted
//     sent-through position, #2746), read from the source as the server's
//     forwarding account. Compared by position only.
//
// The question to the source has a connection of its own, outside the
// port's pool of connections to its sources (pgSourceSlots): once a server
// is stale its statements go to the source and can fill that pool, and a
// question that waited for a slot would find none, turn the verdict to
// unchecked, and send the statements back to the copy exactly while the
// record cannot be trusted. It is one short connection and one row, at most
// one per server at a time (the cache's flight) and one a second while the
// save is old. And a question that times out while the save is still old
// keeps a stale verdict the server already had (pgCaptureStateFrom).
//
// The second is what tells a stall from a quiet source. A running capture
// that is resumed saves every checkpoint period (10 s in watch) even with
// nothing to capture, and within checkpointSoonest of any change, so a save
// older than the bound means it is not saving; and on a quiet source the
// sent-through position it saved last is the source's own, so the source is
// not past it. The WAL belongs to the whole PostgreSQL cluster: another
// database writing past a stalled capture's position counts too. That
// distrusts more, never less.
//
// Where it does not apply, the port's other rules stand: no row (#3191's
// rule, the list of tables not captured), a row with no position yet (a first
// run that saw no commit: other databases can move the WAL past it with
// nothing to capture, so it cannot be told from a stall), a sent-through
// position not trusted (a save by an older capture or bintrail-pg reset).
//
// A capture the operator stopped makes its last save on the way out and
// drops nothing after, but the port cannot tell that stop from one that
// failed to save, nor see a capture run by another process: while the
// source writes, the port takes the record as not current, and the
// statements go to the source, until capture runs and saves again.

// pgCaptureStaleBound is how old capture's last save may be before the
// source is asked whether it wrote past it: #3191's bound for a capture that
// saved nothing, the same failure (a capture that cannot save). Six of
// watch's checkpoint periods (10 s; bintrail-pg's is 5 s), each of which a
// running capture saves at even on a quiet source. An index write that is
// slow but goes through can take longer (indexer.WriteTimeout is 3 minutes);
// statements meanwhile go to the source, the safe side. A variable so a test
// can shorten it.
var pgCaptureStaleBound = console.PGNoCaptureStateBound

// pgCaptureStateTTL is how long one answer is kept for a server's
// statements, pgCaptureStateFailTTL how long an index that could not be read
// is the answer, and pgCaptureStateWait bounds each read (the index's, the
// source's with the wait for a connection of the port).
const (
	pgCaptureStateTTL     = time.Second
	pgCaptureStateFailTTL = 2 * time.Second
	pgCaptureStateWait    = 2 * time.Second
)

// pgCaptureSeen is what one verdict was read from, for the log.
type pgCaptureSeen struct {
	savedAgo    time.Duration
	saved       uint64
	source      uint64
	sourceAsked bool
}

// pgCaptureAskError is a failure to ask the source where its WAL stands,
// with what a client may be told of its cause (no address, no account).
type pgCaptureAskError struct {
	cause string
	err   error
	// timedOut: the source did not answer within pgCaptureStateWait.
	timedOut bool
}

func (e *pgCaptureAskError) Error() string { return e.err.Error() }
func (e *pgCaptureAskError) Unwrap() error { return e.err }

// pgCaptureStateFrom is the verdict on row, stream_state as
// readPGPositionRow reads it, with the source asked (sourceAt) only when the
// last save is older than bound. was is the server's last verdict: a stale
// one is kept when the source does not answer in time while the save is
// still old, since nothing says the source stopped writing, and dropping to
// unchecked would send the statements back to the copy. Pure but for
// sourceAt.
func pgCaptureStateFrom(ctx context.Context, row pgPositionRow, bound time.Duration, was pgshim.CaptureState,
	sourceAt func(context.Context) (uint64, error)) (pgshim.CaptureState, pgCaptureSeen) {
	if ok, _ := pgPositionRefusal(row); !ok {
		return pgshim.CaptureState{}, pgCaptureSeen{}
	}
	seen := pgCaptureSeen{savedAgo: row.savedFor(), saved: row.sentLSN()}
	if seen.savedAgo <= bound {
		return pgshim.CaptureState{}, seen
	}
	src, err := sourceAt(ctx)
	if err != nil {
		cause := "your PostgreSQL could not be asked where its WAL stands"
		var ae *pgCaptureAskError
		if errors.As(err, &ae) && ae.cause != "" {
			cause = ae.cause
		}
		if was.Kind == pgshim.CaptureStale && (errors.As(err, &ae) && ae.timedOut || errors.Is(err, context.DeadlineExceeded)) {
			return pgshim.CaptureState{Kind: pgshim.CaptureStale, Why: fmt.Sprintf(
				"capture has not saved its state for %d seconds, and your PostgreSQL had written past the last position it saved when last asked, so its record of the changes it dropped is not current",
				int64(seen.savedAgo/time.Second))}, seen
		}
		return pgshim.CaptureState{Kind: pgshim.CaptureUnchecked, Why: fmt.Sprintf(
			"capture has not saved its state for %d seconds, and %s", int64(seen.savedAgo/time.Second), cause)}, seen
	}
	seen.source, seen.sourceAsked = src, true
	if src <= seen.saved {
		return pgshim.CaptureState{}, seen
	}
	return pgshim.CaptureState{Kind: pgshim.CaptureStale, Why: fmt.Sprintf(
		"capture has not saved its state for %d seconds while your PostgreSQL wrote past the last position it saved, so its record of the changes it dropped is not current",
		int64(seen.savedAgo/time.Second))}, seen
}

// pgCaptureStateCache is one server's verdict, kept pgCaptureStateTTL, for
// all its connections. One question at a time (flight): a statement that
// comes while one runs waits for it as long as its own context lasts.
type pgCaptureStateCache struct {
	server    string
	logger    *slog.Logger
	readIndex func(context.Context) (pgPositionRow, error)
	askSource func(context.Context) (uint64, error)
	now       func() time.Time

	flight chan struct{}

	mu   sync.Mutex
	st   pgshim.CaptureState
	err  error
	at   time.Time
	has  bool
	said string
}

func newPGCaptureStateCache(server string, logger *slog.Logger, readIndex func(context.Context) (pgPositionRow, error),
	askSource func(context.Context) (uint64, error)) *pgCaptureStateCache {
	return &pgCaptureStateCache{server: server, logger: logger, readIndex: readIndex, askSource: askSource, now: time.Now,
		flight: make(chan struct{}, 1)}
}

// cached is the answer kept, when it is still the answer.
func (c *pgCaptureStateCache) cached() (pgshim.CaptureState, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ttl := pgCaptureStateTTL
	if c.err != nil {
		ttl = pgCaptureStateFailTTL
	}
	if c.has && c.now().Sub(c.at) < ttl {
		return c.st, c.err, true
	}
	return pgshim.CaptureState{}, nil, false
}

// state answers pgshim.Binding.CaptureState.
func (c *pgCaptureStateCache) state(ctx context.Context) (pgshim.CaptureState, error) {
	if st, err, ok := c.cached(); ok {
		return st, err
	}
	select {
	case c.flight <- struct{}{}:
	case <-ctx.Done():
		return pgshim.CaptureState{}, ctx.Err()
	}
	defer func() { <-c.flight }()
	if st, err, ok := c.cached(); ok {
		return st, err
	}
	// Detached from the statement: the answer is every statement's, and a
	// cancel of the one that asked must not be it.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pgCaptureStateWait)
	row, err := c.readIndex(rctx)
	cancel()
	var st pgshim.CaptureState
	var seen pgCaptureSeen
	if err == nil {
		c.mu.Lock()
		was := c.st
		if c.err != nil {
			was = pgshim.CaptureState{}
		}
		c.mu.Unlock()
		st, seen = pgCaptureStateFrom(context.WithoutCancel(ctx), row, pgCaptureStaleBound, was, c.askSource)
	} else {
		err = fmt.Errorf("server %s: read capture's state from the index: %w", c.server, err)
	}
	c.store(st, err, seen)
	return st, err
}

// store keeps an answer, and says in the log when it changes kind.
func (c *pgCaptureStateCache) store(st pgshim.CaptureState, err error, seen pgCaptureSeen) {
	c.mu.Lock()
	c.st, c.err, c.at, c.has = st, err, c.now(), true
	kind := st.Kind
	if err != nil {
		kind = "index_unread"
	}
	changed, was := kind != c.said, c.said
	c.said = kind
	c.mu.Unlock()
	if !changed {
		return
	}
	switch kind {
	case pgshim.CaptureStale:
		c.logger.Warn("PostgreSQL router: capture has not saved its state for longer than its bound while the source wrote past the last position it saved, so its record of the changes it dropped is not current; statements that read this server's tables go to the source (capture_state_stale), or are answered by the copy with a notice where they cannot go there, until capture saves again. Check that capture is running and can write to its index",
			"server", c.server, "saved_ago", seen.savedAgo, "bound", pgCaptureStaleBound,
			"saved_lsn", pglogrepl.LSN(seen.saved).String(), "source_lsn", pglogrepl.LSN(seen.source).String())
	case pgshim.CaptureUnchecked:
		c.logger.Warn("PostgreSQL router: capture has not saved its state for longer than its bound, and the source could not be asked whether it wrote since; the copy answers this server's statements, with a notice",
			"server", c.server, "saved_ago", seen.savedAgo, "bound", pgCaptureStaleBound, "why", st.Why)
	case "index_unread":
		c.logger.Warn("PostgreSQL router: capture's state could not be read from the index; the copy answers this server's statements, with a notice",
			"server", c.server, "error", err)
	default:
		switch {
		case was == "":
		case was == "index_unread":
			c.logger.Info("PostgreSQL router: capture's state can be read from the index again; statements go where they did before",
				"server", c.server)
		case seen.savedAgo <= pgCaptureStaleBound:
			c.logger.Info("PostgreSQL router: capture saves its state again; statements go where they did before",
				"server", c.server, "was", was, "saved_ago", seen.savedAgo)
		default:
			c.logger.Info("PostgreSQL router: the source's WAL is not past the last position capture saved; statements go where they did before",
				"server", c.server, "was", was, "saved_ago", seen.savedAgo)
		}
	}
}

// pgCaptureStateDoc is a server's verdict in /status, while it is not
// current.
type pgCaptureStateDoc struct {
	State     string `json:"state"`
	Why       string `json:"why"`
	CheckedAt string `json:"checked_at"`
}

// doc is the verdict for /status, nil when it is current, none was read,
// or the one read is older than pgCaptureStateFailTTL: a verdict nobody
// asked for since is not said as the state now. /status asks for a fresh one
// first (pgCaptureStateNow).
func (c *pgCaptureStateCache) doc() *pgCaptureStateDoc {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.has || c.now().Sub(c.at) > pgCaptureStateFailTTL {
		return nil
	}
	d := &pgCaptureStateDoc{State: c.st.Kind, Why: c.st.Why, CheckedAt: c.at.UTC().Format(time.RFC3339)}
	if c.err != nil {
		d.State, d.Why = pgshim.CaptureUnchecked, "capture's state could not be read from the index (the router's log has the details)"
	}
	if d.State == "" {
		return nil
	}
	return d
}

// pgCaptureStateCaches is the port's caches, one per server.
var pgCaptureStateCaches struct {
	mu  sync.Mutex
	of  map[string]*pgCaptureStateCache
	key map[string]pgCaptureStateKey
}

// pgCaptureStateKey is what a server's cache reads with: a change of either
// is a new cache.
type pgCaptureStateKey struct {
	index *sql.DB
	dsn   string
}

// pgPortCaptureState is pgshim.Binding.CaptureState for server, whose index
// is index and whose port forwards as dsn, one cache per server shared by its
// connections. The source is asked on a connection of the question's own,
// never one of the port's pool (see the top of this file).
func pgPortCaptureState(server string, index *sql.DB, dsn string, logger *slog.Logger) func(context.Context) (pgshim.CaptureState, error) {
	if index == nil || dsn == "" {
		return nil
	}
	key := pgCaptureStateKey{index, dsn}
	pgCaptureStateCaches.mu.Lock()
	if pgCaptureStateCaches.of == nil {
		pgCaptureStateCaches.of = map[string]*pgCaptureStateCache{}
		pgCaptureStateCaches.key = map[string]pgCaptureStateKey{}
	}
	c := pgCaptureStateCaches.of[server]
	if c == nil || pgCaptureStateCaches.key[server] != key {
		c = newPGCaptureStateCache(server, logger,
			func(ctx context.Context) (pgPositionRow, error) { return readPGPositionRow(ctx, index) },
			func(ctx context.Context) (uint64, error) { return pgAskFlushed(ctx, dsn) })
		pgCaptureStateCaches.of[server], pgCaptureStateCaches.key[server] = c, key
	}
	pgCaptureStateCaches.mu.Unlock()
	return c.state
}

// pgCaptureStateNow is server's verdict for /status, nil when none is to be
// said. It asks for one first, as a statement would (kept pgCaptureStateTTL:
// one read of the index, and the source only while the save is old), so
// that /status says the state now, and a stall or its end is logged, though
// no statement comes. Without statements or /status requests nothing is
// asked, and nothing logged.
func pgCaptureStateNow(ctx context.Context, server string) *pgCaptureStateDoc {
	pgCaptureStateCaches.mu.Lock()
	c := pgCaptureStateCaches.of[server]
	pgCaptureStateCaches.mu.Unlock()
	if c == nil {
		return nil
	}
	_, _ = c.state(ctx)
	return c.doc()
}

// pgCaptureStateServers are the servers the port keeps a verdict for.
func pgCaptureStateServers() []string {
	pgCaptureStateCaches.mu.Lock()
	defer pgCaptureStateCaches.mu.Unlock()
	out := make([]string, 0, len(pgCaptureStateCaches.of))
	for id := range pgCaptureStateCaches.of {
		out = append(out, id)
	}
	return out
}

// pgAskFlushed reads how far the source's WAL is flushed
// (pgcapture.QueryFlushedLSN: on a standby, how far it replayed), within
// pgCaptureStateWait, on a connection of its own (not the port's pool, see
// the top of this file). Nothing is written, and no row of a table is read.
func pgAskFlushed(ctx context.Context, dsn string) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, pgCaptureStateWait)
	defer cancel()
	failed := func(cause string, err error) error {
		e := &pgCaptureAskError{cause: cause, err: err}
		if ctx.Err() != nil {
			e.cause, e.timedOut = fmt.Sprintf("your PostgreSQL did not answer within %s", pgCaptureStateWait), true
		}
		return e
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, failed("the port could not connect to your PostgreSQL", err)
	}
	defer conn.Close(context.Background())
	lsn, err := pgcapture.QueryFlushedLSN(ctx, conn)
	if err != nil {
		return 0, failed(pgRefusedCause(err), err)
	}
	return lsn, nil
}
