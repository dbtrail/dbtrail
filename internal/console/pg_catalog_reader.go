package console

import (
	"context"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// RunPGCatalog is Run for what the PostgreSQL port reads for itself and that
// reads no table's rows: the copy's list of tables and columns, and the
// checks of a SET (a schema, a time zone). It takes no tails. A statement
// that takes tails first needs the tail keeper's lock, which an update of the
// tails holds for most of its length, so a statement the port was only about
// to screen waited for that update, holding its place on the copy.
//
// It still runs on a worker of the copy, so a busy copy is still said as
// busy, and it is still held to the server's lag limit: the list is as old as
// the snapshot it is read from. What it runs is not taken for the
// connection's last answer.
func (q *SQLOnCopy) RunPGCatalog(ctx context.Context, statement, schema string, sess sqlsandbox.Session) (sqlsandbox.Result, error) {
	// No server id: Run then marks no tails to read (tailsFor), and adds no
	// limit of its own to the one set here.
	reader := &SQLOnCopy{s: q.s, b: q.b, user: q.user}
	return reader.Run(q.pgCatalogContext(ctx), statement, schema, sess)
}

// pgCatalogContext is ctx held to this server's lag limit, read per statement
// as Run reads it.
func (q *SQLOnCopy) pgCatalogContext(ctx context.Context) context.Context {
	return withBehindLimit(ctx, q.id, q.s.maxBehindSourceOf(q.id))
}
