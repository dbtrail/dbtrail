package consoleapp

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// A column's collation on the PostgreSQL port's catalog (#3111).

// pgColumnCollation is what a snapshot's shape says of one column's
// collation: recorded is the shape's Collations, nondeterministic the
// column's. A snapshot taken before collations were recorded says nothing;
// of two columns whose names differ only in case (the copy cannot tell them
// apart) a nondeterministic one is kept.
func pgColumnCollation(recorded, nondeterministic bool, prev pgshim.Collation) pgshim.Collation {
	switch {
	case !recorded || prev == pgshim.CollationNondeterministic:
		return prev
	case nondeterministic:
		return pgshim.CollationNondeterministic
	}
	return pgshim.CollationDeterministic
}

// pgCollationAsker says, of the tables named (schema and name), which
// columns have a nondeterministic collation on the source, by lowercased
// schema, table and column; every column of a table it lists is known.
type pgCollationAsker func(ctx context.Context, tables [][2]string) (map[[3]string]bool, map[[2]string]bool, error)

// pgFillCollations asks the source about every table of the copy: its answer
// is today's, the snapshot's is the day it was taken (a column changed with
// ALTER ... COLLATE keeps its type, so nothing else notices). What it cannot
// learn keeps the snapshot's answer, or stays unknown, and the port's screen
// takes an unknown text column for nondeterministic.
func pgFillCollations(ctx context.Context, tables []pgshim.CatalogTable, ask pgCollationAsker) {
	var want [][2]string
	for _, t := range tables {
		want = append(want, [2]string{t.Schema, t.Name})
	}
	if len(want) == 0 {
		return
	}
	nondet, listed, err := ask(ctx, want)
	if err != nil {
		return
	}
	for i := range tables {
		t := &tables[i]
		if !listed[[2]string{strings.ToLower(t.Schema), strings.ToLower(t.Name)}] {
			continue
		}
		for j := range t.Columns {
			c := &t.Columns[j]
			c.Collation = pgshim.CollationDeterministic
			if nondet[[3]string{strings.ToLower(t.Schema), strings.ToLower(t.Name), strings.ToLower(c.Name)}] {
				c.Collation = pgshim.CollationNondeterministic
			}
		}
	}
}

// pgCollationsSQL reads the source's catalog only (pg_attribute, pg_class,
// pg_namespace, pg_collation): no row of any table. listed has one row per
// table found; nondeterministic one per such column.
const pgCollationsSQL = `SELECT n.nspname, c.relname, a.attname, COALESCE(NOT co.collisdeterministic, false)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN ROWS FROM (pg_catalog.unnest($1::text[]), pg_catalog.unnest($2::text[])) AS w(s, t) ON w.s = n.nspname AND w.t = c.relname
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation`

// pgCollationTTL is how long one source's answer is kept, and
// pgCollationFailTTL how long a failure is: a source that is down is not
// asked again by every catalog read while it is. pgCollationWait bounds one
// question, so that a source that does not answer holds a catalog read up to
// that long, and no more.
const (
	pgCollationTTL     = time.Minute
	pgCollationFailTTL = 30 * time.Second
	pgCollationWait    = 2 * time.Second
)

var pgCollationCache struct {
	mu   sync.Mutex
	byID map[string]*pgCollationAnswer
}

// pgCollationAnswer is one source's answer, or its failure (err), and the
// lock one question for it is asked under: questions that meet wait for the
// first one's answer instead of each opening a connection.
type pgCollationAnswer struct {
	ask    sync.Mutex
	at     time.Time
	err    error
	nondet map[[3]string]bool
	listed map[[2]string]bool
}

// pgSourceCollations is a pgCollationAsker that reads the source's catalog as
// dsn (the account statements are forwarded with: the server's forwarding
// account, or its capture account when it said so, #3054; a read of the
// catalog alone, which the capture gate has nothing to say about), one answer
// kept a minute per source and set of tables. server names the server in the
// log.
func pgSourceCollations(dsn, server string) pgCollationAsker {
	return func(ctx context.Context, tables [][2]string) (map[[3]string]bool, map[[2]string]bool, error) {
		var schemas, names []string
		key := dsn
		for _, t := range tables {
			schemas, names = append(schemas, t[0]), append(names, t[1])
			key += "\x00" + t[0] + "\x00" + t[1]
		}
		pgCollationCache.mu.Lock()
		if pgCollationCache.byID == nil {
			pgCollationCache.byID = map[string]*pgCollationAnswer{}
		}
		a := pgCollationCache.byID[key]
		if a == nil {
			a = &pgCollationAnswer{}
			pgCollationCache.byID[key] = a
		}
		for k, old := range pgCollationCache.byID {
			if old != a && old.ask.TryLock() {
				if !old.at.IsZero() && time.Since(old.at) > pgCollationTTL {
					delete(pgCollationCache.byID, k)
				}
				old.ask.Unlock()
			}
		}
		pgCollationCache.mu.Unlock()
		a.ask.Lock()
		defer a.ask.Unlock()
		switch {
		case !a.at.IsZero() && a.err == nil && time.Since(a.at) < pgCollationTTL:
			return a.nondet, a.listed, nil
		case !a.at.IsZero() && a.err != nil && time.Since(a.at) < pgCollationFailTTL:
			return nil, nil, a.err
		}
		nondet, listed, err := pgAskCollations(ctx, dsn, schemas, names)
		a.at, a.err, a.nondet, a.listed = time.Now(), err, nondet, listed
		if err != nil {
			slog.Warn("PostgreSQL port: the source could not say which columns have a nondeterministic collation; the snapshot's answer stands, and text whose collation it does not record is not compared on the copy (asked again in 30 seconds)",
				"server", server, "error", err)
		}
		return nondet, listed, err
	}
}

// pgAskCollations reads the answer from the source.
func pgAskCollations(ctx context.Context, dsn string, schemas, names []string) (map[[3]string]bool, map[[2]string]bool, error) {
	ctx, cancel := context.WithTimeout(ctx, pgCollationWait)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx, pgCollationsSQL, schemas, names)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	nondet, listed := map[[3]string]bool{}, map[[2]string]bool{}
	for rows.Next() {
		var schema, table string
		var column *string
		var n bool
		if err := rows.Scan(&schema, &table, &column, &n); err != nil {
			return nil, nil, err
		}
		listed[[2]string{strings.ToLower(schema), strings.ToLower(table)}] = true
		if column != nil && n {
			nondet[[3]string{strings.ToLower(schema), strings.ToLower(table), strings.ToLower(*column)}] = true
		}
	}
	return nondet, listed, rows.Err()
}
