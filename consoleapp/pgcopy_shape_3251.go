package consoleapp

import (
	"context"
	"errors"
	"fmt"
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

// pgShapesSQL reads, for each table named, its OID and the columns a
// snapshot would store (pgbaseline's: not dropped, not generated), in attnum
// order. A table with no such column has one row with NULL columns; a table
// the source does not have has none.
const pgShapesSQL = `SELECT n.nspname, c.relname, c.oid, a.attname, a.atttypid, a.atttypmod, a.attnum
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN ROWS FROM (pg_catalog.unnest($1::text[]), pg_catalog.unnest($2::text[])) AS w(s, t) ON w.s = n.nspname AND w.t = c.relname
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
ORDER BY n.nspname, c.relname, a.attnum`

// pgShapeTTL is how long a table's answer is kept: the longest an ALTER
// TABLE can go unseen by a statement on the copy. pgShapeFailTTL is how long
// a failure is the answer, so a source that is down is not asked again by
// every statement while it is (the copy answers each meanwhile, with a
// notice). pgShapeWait bounds one question, the wait for a connection of the
// port's included.
const (
	pgShapeTTL     = time.Second
	pgShapeFailTTL = 2 * time.Second
	pgShapeWait    = 2 * time.Second
)

// pgShapeCache is one source's answers, by table, and its last failure. Its
// lock guards those alone and is never held across a question: a question
// runs on its own, one at a time per set of tables (a flight), and every
// statement that needs that set waits for it as long as its own context
// lasts, no longer. Answers and failures are dated when they arrive.
type pgShapeCache struct {
	ttl, failTTL time.Duration
	now          func() time.Time
	ask          func(ctx context.Context, schemas, names []string) (map[[2]string]pgshim.SourceShape, error)

	mu       sync.Mutex
	known    map[[2]string]pgShapeKnown
	failed   error
	failedAt time.Time
	flights  map[string]*pgShapeFlight
}

// pgShapeKnown is what the source said of one table, and when it said it.
type pgShapeKnown struct {
	at    time.Time
	shape pgshim.SourceShape
	found bool
}

// pgShapeFlight is one question on its way: done closes when its answer (or
// err) is in the cache.
type pgShapeFlight struct {
	done chan struct{}
	err  error
}

// shapes answers pgshim.Binding.SourceShapes from what is kept, asking the
// source about the tables not known within ttl, all in one question.
func (c *pgShapeCache) shapes(ctx context.Context, tables [][2]string) (map[[2]string]pgshim.SourceShape, error) {
	// waited: a question this statement waited for has answered. What it
	// answered is the answer, however old the clock says it is by now, so a
	// clock that jumps cannot ask again and again.
	waited := false
	for {
		c.mu.Lock()
		now := c.now()
		var schemas, names []string
		seen := map[[2]string]bool{}
		for _, t := range tables {
			if k, ok := c.known[t]; ok && (waited || now.Sub(k.at) < c.ttl) || seen[t] {
				continue
			}
			seen[t] = true
			schemas, names = append(schemas, t[0]), append(names, t[1])
		}
		if len(names) == 0 {
			out := map[[2]string]pgshim.SourceShape{}
			for _, t := range tables {
				if k := c.known[t]; k.found {
					out[t] = k.shape
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
		key := strings.Join(schemas, "\x00") + "\x01" + strings.Join(names, "\x00")
		f := c.flights[key]
		if f == nil {
			f = &pgShapeFlight{done: make(chan struct{})}
			if c.flights == nil {
				c.flights = map[string]*pgShapeFlight{}
			}
			c.flights[key] = f
			// The question is the source's, not this statement's: one that
			// is cancelled must not fail it for the others waiting on it.
			// pgAskShapes bounds it.
			go c.fly(context.WithoutCancel(ctx), key, f, schemas, names)
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
		waited = true
	}
}

// fly asks the source and keeps its answer, or its failure, dated when it
// came.
func (c *pgShapeCache) fly(ctx context.Context, key string, f *pgShapeFlight, schemas, names []string) {
	got, err := c.ask(ctx, schemas, names)
	c.mu.Lock()
	at := c.now()
	if err != nil {
		c.failed, c.failedAt = err, at
	} else {
		c.failed = nil
		if c.known == nil {
			c.known = map[[2]string]pgShapeKnown{}
		}
		for k, v := range c.known {
			if at.Sub(v.at) >= c.ttl {
				delete(c.known, k)
			}
		}
		for i := range names {
			k := [2]string{schemas[i], names[i]}
			sh, found := got[k]
			c.known[k] = pgShapeKnown{at: at, shape: sh, found: found}
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
// names the server in the error.
func pgPortSourceShapes(dsn, server string, sources *pgSourceSlots) func(ctx context.Context, tables [][2]string) (map[[2]string]pgshim.SourceShape, error) {
	key := pgShapeCacheKey{dsn, sources}
	pgShapeCaches.mu.Lock()
	if pgShapeCaches.byDSN == nil {
		pgShapeCaches.byDSN = map[pgShapeCacheKey]*pgShapeCache{}
	}
	c := pgShapeCaches.byDSN[key]
	if c == nil {
		c = &pgShapeCache{ttl: pgShapeTTL, failTTL: pgShapeFailTTL, now: time.Now, ask: func(ctx context.Context, schemas, names []string) (map[[2]string]pgshim.SourceShape, error) {
			return pgAskShapes(ctx, dsn, sources.acquire, schemas, names)
		}}
		pgShapeCaches.byDSN[key] = c
	}
	pgShapeCaches.mu.Unlock()
	return func(ctx context.Context, tables [][2]string) (map[[2]string]pgshim.SourceShape, error) {
		got, err := c.shapes(ctx, tables)
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
func pgAskShapes(ctx context.Context, dsn string, acquire func(context.Context) (func(), bool), schemas, names []string) (map[[2]string]pgshim.SourceShape, error) {
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
	rows, err := conn.Query(ctx, pgShapesSQL, schemas, names)
	if err != nil {
		return nil, failed(pgRefusedCause(err), err)
	}
	defer rows.Close()
	out := map[[2]string]pgshim.SourceShape{}
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
		key := [2]string{schema, table}
		sh := out[key]
		sh.OID = oid
		if name != nil && typ != nil && mod != nil && num != nil {
			sh.Columns = append(sh.Columns, baseline.PGColumn{Name: *name, TypeOID: *typ, TypeMod: *mod, Ordinal: int(*num)})
		}
		out[key] = sh
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
