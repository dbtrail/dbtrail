package console

import "context"

// The PostgreSQL port's refusal before running (#3086). On a connection
// whose source already answered about a table, the port gives the copy's
// answer that reads it only when the table was read with its recent changes
// over its snapshot (pgshim copyOlderThanSource). A statement whose changes
// were not ready in fresh mode's wait used to run anyway, from the snapshot
// alone, and its answer was thrown away after: the client paid the copy's
// run and then the source's. The port now says so before the copy runs
// (pgshim.RunOptions.NeedTails), and the copy refuses the statement where a
// routed MySQL statement is refused (sqlsandbox.Session.StrictStar), with the
// same error: the port sends it to the source.
//
// Only that refusal. The rest of what StrictStar asks of a statement (MySQL's
// columns, its text and its dates) is MySQL's, and a PostgreSQL statement is
// not held to it.

type pgNeedTailsKey struct{}

// WithPGNeedTails marks ctx for a PostgreSQL statement whose answer is given
// only with its tables' recent changes: when fresh mode's wait for them ends
// without them, SQLOnCopy.Run refuses the statement before it runs, with a
// *sqlsandbox.MayHaveChangedError.
func WithPGNeedTails(ctx context.Context) context.Context {
	return context.WithValue(ctx, pgNeedTailsKey{}, true)
}

// pgNeedTails reports whether ctx was marked by WithPGNeedTails.
func pgNeedTails(ctx context.Context) bool {
	v, _ := ctx.Value(pgNeedTailsKey{}).(bool)
	return v
}

// PGNeedsTails reports whether ctx was marked by WithPGNeedTails, for the
// host that marks it.
func PGNeedsTails(ctx context.Context) bool { return pgNeedTails(ctx) }
