package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// A table's shape on the PostgreSQL source now, for the port (#3251).
//
// Before the copy answers a statement, the port compares the columns of the
// tables it reads with the ones their snapshot recorded (pgshim
// shape_3251.go): PostgreSQL does not announce an ALTER TABLE to capture.
// This reads them from the source's catalog (pg_class, pg_namespace,
// pg_attribute: never a row of a table), as the account the server forwards
// with, and keeps each table's answer for pgShapeTTL, so a burst of
// statements asks once and a change is seen within that time. Each question
// takes one of the port's connections to its sources (pgSourceSlots), as a
// forwarded statement does, so the port's bound on them holds for it too.

// pgShapesSQL reads, for each OID asked, the table's schema and name and
// the columns a snapshot would store (pgbaseline's: not dropped, not
// generated), in attnum order. A table with no such column has one row with
// NULL columns; an OID the source does not have, none.
const pgShapesSQL = `SELECT n.nspname, c.relname, c.oid, a.attname, a.atttypid, a.atttypmod, a.attnum
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
WHERE c.oid = ANY($1::oid[])
ORDER BY c.oid, a.attnum`

// pgShapeTTL is how long a table's answer is kept, counted from when the
// question that read it was asked: a statement is decided by a read of the
// catalog asked at most pgShapeTTL before it, or by the one it waited for,
// so an ALTER TABLE committed more than pgShapeTTL before a statement starts
// is always seen by it. pgShapeFailTTL is how long a failure is the answer,
// counted from when it came, so a source that is down is not asked again by
// every statement while it is (the copy answers each meanwhile, with a
// notice). pgShapeWait bounds one question, the wait for a connection of the
// port's included. pgShapeIdle is how long a source no connection asks
// about keeps its answers (a server removed, an account changed).
const (
	pgShapeTTL     = time.Second
	pgShapeFailTTL = 2 * time.Second
	pgShapeWait    = 2 * time.Second
	pgShapeIdle    = 10 * time.Minute
)

// pgShapeCache is one source's answers, by OID, and its last failure. Its
// lock guards those alone and is never held across a question: a question
// runs on its own, one at a time per set of tables (a flight), and every
// statement that needs that set waits for it as long as its own context
// lasts, no longer.
//
// An answer is dated by when its question was asked (launched), never when
// it came: of two questions that overlap, the one asked later read the
// catalog later, and an earlier one that comes back last never overwrites
// it.
type pgShapeCache struct {
	ttl, failTTL time.Duration
	now          func() time.Time
	ask          func(ctx context.Context, oids []uint32) (map[uint32]pgshim.SourceShape, error)

	mu       sync.Mutex
	known    map[uint32]pgShapeKnown
	failed   error
	failedAt time.Time
	// okAt is when the newest question that answered was asked: a failure of
	// one asked before it is not the answer.
	okAt    time.Time
	flights map[string]*pgShapeFlight
	used    time.Time
}

// pgShapeKnown is what the source said of one table, and when the question
// that said it was asked.
type pgShapeKnown struct {
	at    time.Time
	shape pgshim.SourceShape
	found bool
}

// pgShapeFlight is one question on its way, asked at launched: done closes
// when its answer (or err) is in the cache.
type pgShapeFlight struct {
	launched time.Time
	done     chan struct{}
	err      error
}

// shapes answers pgshim.Binding.SourceShapes from what is kept, asking the
// source about the tables not known within ttl, all in one question.
func (c *pgShapeCache) shapes(ctx context.Context, oids []uint32) (map[uint32]pgshim.SourceShape, error) {
	// since: the question this statement waited for was asked then. What it
	// answered, or anything asked after it, is the answer, however old the
	// clock says it is by now: a source slower than ttl answers the
	// statements that waited for it, which do not ask again.
	var since time.Time
	waited := false
	for {
		c.mu.Lock()
		now := c.now()
		c.used = now
		var stale []uint32
		seen := map[uint32]bool{}
		for _, o := range oids {
			k, ok := c.known[o]
			if ok && (now.Sub(k.at) < c.ttl || waited && !k.at.Before(since)) || seen[o] {
				continue
			}
			seen[o] = true
			stale = append(stale, o)
		}
		if len(stale) == 0 {
			out := map[uint32]pgshim.SourceShape{}
			for _, o := range oids {
				if k := c.known[o]; k.found {
					out[o] = k.shape
				}
			}
			c.mu.Unlock()
			return out, nil
		}
		if c.failed != nil && now.Sub(c.failedAt) < c.failTTL {
			err := c.failed
			c.mu.Unlock()
			return nil, err
		}
		slices.Sort(stale)
		var key strings.Builder
		for _, o := range stale {
			fmt.Fprintf(&key, "%d,", o)
		}
		f := c.flights[key.String()]
		if f == nil {
			f = &pgShapeFlight{launched: now, done: make(chan struct{})}
			if c.flights == nil {
				c.flights = map[string]*pgShapeFlight{}
			}
			c.flights[key.String()] = f
			// The question is the source's, not this statement's: one that
			// is cancelled must not fail it for the others waiting on it.
			// pgAskShapes bounds it.
			go c.fly(context.WithoutCancel(ctx), key.String(), f, stale)
		}
		c.mu.Unlock()
		select {
		case <-f.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if f.err != nil {
			return nil, f.err
		}
		// Answered: read it from the cache, with whatever else is kept.
		since, waited = f.launched, true
	}
}

// fly asks the source and keeps its answer, dated when it was asked, over
// entries no newer; or its failure, dated when it came, unless a question
// asked after it already answered.
func (c *pgShapeCache) fly(ctx context.Context, key string, f *pgShapeFlight, oids []uint32) {
	got, err := c.ask(ctx, oids)
	c.mu.Lock()
	if err != nil {
		if !f.launched.Before(c.okAt) {
			c.failed, c.failedAt = err, c.now()
		}
	} else {
		if c.okAt.Before(f.launched) {
			c.okAt = f.launched
			c.failed = nil
		}
		if c.known == nil {
			c.known = map[uint32]pgShapeKnown{}
		}
		for o, v := range c.known {
			if f.launched.Sub(v.at) >= c.ttl {
				delete(c.known, o)
			}
		}
		for _, o := range oids {
			if old, ok := c.known[o]; ok && old.at.After(f.launched) {
				// A question asked after this one read the catalog later.
				continue
			}
			sh, found := got[o]
			c.known[o] = pgShapeKnown{at: f.launched, shape: sh, found: found}
		}
	}
	delete(c.flights, key)
	f.err = err
	c.mu.Unlock()
	close(f.done)
}

var pgShapeCaches struct {
	mu    sync.Mutex
	byDSN map[pgShapeCacheKey]*pgShapeCache
	// of is each server's key now: a server whose account changes drops
	// the cache of the old one.
	of map[string]pgShapeCacheKey
}

// pgShapeCacheKey is one source as one port reaches it: the account, and the
// port's connections to its sources.
type pgShapeCacheKey struct {
	dsn     string
	sources *pgSourceSlots
}

// pgPortSourceShapes is pgshim.Binding.SourceShapes for a server whose port
// forwards as dsn (its forwarding account, or its capture account when it
// said so, #3054: a read of the catalog alone, which the capture gate has
// nothing to say about), one cache per dsn and port shared by its
// connections. Each question takes a connection of sources first. server
// names the server in the error. A server's cache goes when it is asked
// for with another account, and any cache no connection asked about for
// pgShapeIdle (a server removed) when the next connection is made.
func pgPortSourceShapes(dsn, server string, sources *pgSourceSlots) func(ctx context.Context, oids []uint32) (map[uint32]pgshim.SourceShape, error) {
	key := pgShapeCacheKey{dsn, sources}
	pgShapeCaches.mu.Lock()
	if pgShapeCaches.byDSN == nil {
		pgShapeCaches.byDSN = map[pgShapeCacheKey]*pgShapeCache{}
		pgShapeCaches.of = map[string]pgShapeCacheKey{}
	}
	if old, ok := pgShapeCaches.of[server]; ok && old != key {
		delete(pgShapeCaches.byDSN, old)
	}
	pgShapeCaches.of[server] = key
	for k, c := range pgShapeCaches.byDSN {
		c.mu.Lock()
		idle := k != key && !c.used.IsZero() && time.Since(c.used) > pgShapeIdle && len(c.flights) == 0
		c.mu.Unlock()
		if idle {
			delete(pgShapeCaches.byDSN, k)
		}
	}
	for s, k := range pgShapeCaches.of {
		if _, ok := pgShapeCaches.byDSN[k]; !ok && s != server {
			delete(pgShapeCaches.of, s)
		}
	}
	c := pgShapeCaches.byDSN[key]
	if c == nil {
		c = &pgShapeCache{ttl: pgShapeTTL, failTTL: pgShapeFailTTL, now: time.Now, used: time.Now(), ask: func(ctx context.Context, oids []uint32) (map[uint32]pgshim.SourceShape, error) {
			return pgAskShapes(ctx, dsn, sources.acquire, oids)
		}}
		pgShapeCaches.byDSN[key] = c
	}
	pgShapeCaches.mu.Unlock()
	return func(ctx context.Context, oids []uint32) (map[uint32]pgshim.SourceShape, error) {
		got, err := c.shapes(ctx, oids)
		if err != nil {
			return nil, fmt.Errorf("server %s: read the columns of its tables from the source: %w", server, err)
		}
		return got, nil
	}
}

// errNoSourceSlot is a question that found every connection of the port to
// its sources taken for as long as it could wait.
var errNoSourceSlot = errors.New("every connection of the port to its sources was taken")

// pgAskShapes reads the answer from the source, within pgShapeWait, on one
// of the port's connections to its sources (acquire), given back when it
// ends.
func pgAskShapes(ctx context.Context, dsn string, acquire func(context.Context) (func(), bool), oids []uint32) (map[uint32]pgshim.SourceShape, error) {
	ctx, cancel := context.WithTimeout(ctx, pgShapeWait)
	defer cancel()
	// What the client is told of a failure: its cause in words, never the
	// error's own text (an address, an account, a column).
	failed := func(cause string, err error) error {
		if ctx.Err() != nil {
			cause = fmt.Sprintf("your PostgreSQL did not answer within %s", pgShapeWait)
		}
		return &pgshim.ShapeCheckError{Cause: cause, Err: err}
	}
	release, ok := acquire(ctx)
	if !ok {
		return nil, &pgshim.ShapeCheckError{Cause: "every connection of the port to your PostgreSQL stayed taken", Err: errNoSourceSlot}
	}
	defer release()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, failed("the port could not connect to your PostgreSQL", err)
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx, pgShapesSQL, oids)
	if err != nil {
		return nil, failed(pgRefusedCause(err), err)
	}
	defer rows.Close()
	out := map[uint32]pgshim.SourceShape{}
	for rows.Next() {
		var schema, table string
		var oid uint32
		var name *string
		var typ *uint32
		var mod *int32
		var num *int16
		if err := rows.Scan(&schema, &table, &oid, &name, &typ, &mod, &num); err != nil {
			return nil, failed("the port could not read your PostgreSQL's answer", err)
		}
		sh := out[oid]
		sh.Schema, sh.Name, sh.OID = schema, table, oid
		if name != nil && typ != nil && mod != nil && num != nil {
			sh.Columns = append(sh.Columns, baseline.PGColumn{Name: *name, TypeOID: *typ, TypeMod: *mod, Ordinal: int(*num)})
		}
		out[oid] = sh
	}
	if err := rows.Err(); err != nil {
		return nil, failed(pgRefusedCause(err), err)
	}
	return out, nil
}

// pgRefusedCause is the cause of a question the source answered with an
// error: its SQLSTATE, which says what without the message's words.
func pgRefusedCause(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "your PostgreSQL refused the question (SQLSTATE " + pe.Code + ")"
	}
	return "the port could not read your PostgreSQL's answer"
}
