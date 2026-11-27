package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

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
// lock is held across a question: questions that meet wait for the first
// one's answer instead of each opening a connection.
type pgShapeCache struct {
	ttl, failTTL time.Duration
	now          func() time.Time
	ask          func(ctx context.Context, schemas, names []string) (map[[2]string]pgshim.SourceShape, error)

	mu       sync.Mutex
	known    map[[2]string]pgShapeKnown
	failed   error
	failedAt time.Time
}

// pgShapeKnown is what the source said of one table, and when it was asked.
type pgShapeKnown struct {
	at    time.Time
	shape pgshim.SourceShape
	found bool
}

// shapes answers pgshim.Binding.SourceShapes from what is kept, asking the
// source about the tables not known within ttl, all in one question.
func (c *pgShapeCache) shapes(ctx context.Context, tables [][2]string) (map[[2]string]pgshim.SourceShape, error) {
	out := map[[2]string]pgshim.SourceShape{}
	if len(tables) == 0 {
		return out, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	var schemas, names []string
	seen := map[[2]string]bool{}
	for _, t := range tables {
		if k, ok := c.known[t]; ok && now.Sub(k.at) < c.ttl || seen[t] {
			continue
		}
		seen[t] = true
		schemas, names = append(schemas, t[0]), append(names, t[1])
	}
	if len(names) > 0 {
		if c.failed != nil && now.Sub(c.failedAt) < c.failTTL {
			return nil, c.failed
		}
		got, err := c.ask(ctx, schemas, names)
		if err != nil {
			if ctx.Err() == nil {
				// A statement stopped while it asked is not the source's
				// failure.
				c.failed, c.failedAt = err, now
			}
			return nil, err
		}
		c.failed = nil
		if c.known == nil {
			c.known = map[[2]string]pgShapeKnown{}
		}
		for k, v := range c.known {
			if now.Sub(v.at) >= c.ttl {
				delete(c.known, k)
			}
		}
		// Dated from before the question: the answer is at least that new.
		for i := range names {
			key := [2]string{schemas[i], names[i]}
			s, found := got[key]
			c.known[key] = pgShapeKnown{at: now, shape: s, found: found}
		}
	}
	for _, t := range tables {
		if k := c.known[t]; k.found {
			out[t] = k.shape
		}
	}
	return out, nil
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
	release, ok := acquire(ctx)
	if !ok {
		return nil, errNoSourceSlot
	}
	defer release()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx, pgShapesSQL, schemas, names)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		key := [2]string{schema, table}
		s := out[key]
		s.OID = oid
		if name != nil && typ != nil && mod != nil && num != nil {
			s.Columns = append(s.Columns, baseline.PGColumn{Name: *name, TypeOID: *typ, TypeMod: *mod, Ordinal: int(*num)})
		}
		out[key] = s
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
