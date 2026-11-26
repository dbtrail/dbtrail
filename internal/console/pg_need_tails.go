package console

import "context"

// The PostgreSQL port's refusals before running in fresh mode (#3086,
// #3116). A routed MySQL statement (sqlsandbox.Session.StrictStar) is refused
// in two cases, and MySQL answers it: a table it reads has no recent changes
// ready when fresh mode's wait ends, so the copy would answer from the
// snapshot alone; or the recent changes it would read are older than an
// earlier answer of the copy on the same connection (seenFloor), so a value
// the client was shown could go back. A PostgreSQL statement whose source can
// answer it is marked by the port (pgshim.RunOptions.NeedTails) and refused
// in the same two cases, with the same error: the port sends it to the
// source. Unmarked, the copy answers it with a note that says why, as it
// answers a MySQL connection that does not route.
//
// Only those two refusals. The rest of what StrictStar asks of a statement
// (MySQL's columns, its text and its dates) is MySQL's, and a PostgreSQL
// statement is not held to it.

type pgNeedTailsKey struct{}

// WithPGNeedTails marks ctx for a PostgreSQL statement whose source can
// answer it: in fresh mode SQLOnCopy.Run refuses it before it runs, with a
// *sqlsandbox.MayHaveChangedError, when the wait for its tables' recent
// changes ends without them, or when they are older than an earlier answer
// of the copy on the connection.
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
