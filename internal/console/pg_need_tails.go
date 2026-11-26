package console

import (
	"context"
	"strings"

	"github.com/dbtrail/dbtrail/internal/views"
)

// The PostgreSQL port's refusals before running in fresh mode (#3086,
// #3116). A routed MySQL statement (sqlsandbox.Session.StrictStar) is refused
// in two cases, and MySQL answers it: a table it reads has no recent changes
// ready when fresh mode's wait ends, so the copy would answer from the
// snapshot alone; or the recent changes it would read are older than an
// earlier answer of the copy on the same connection (seenFloor), so a value
// the client was shown could go back. The PostgreSQL port marks a statement
// for each case it is refused in, apart (pgshim.RunOptions.NeedTails and
// NoOlderTails): a light statement whose source can answer it gets both, and
// a statement the source's plan found expensive stays on the copy, marked at
// most for the first. A marked statement is refused with the same error a
// routed MySQL one is: the port sends it to the source. Unmarked, the copy
// answers it with a note that says why, as it answers a MySQL connection
// that does not route.
//
// Only those two refusals. The rest of what StrictStar asks of a statement
// (MySQL's columns, its text and its dates) is MySQL's, and a PostgreSQL
// statement is not held to it.

type pgNeedTailsKey struct{}

// WithPGNeedTails marks ctx for a PostgreSQL statement not answered from the
// snapshot alone: in fresh mode SQLOnCopy.Run refuses it before it runs,
// with a *sqlsandbox.MayHaveChangedError, when the wait for its tables'
// recent changes ends without them.
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

type pgNoOlderTailsKey struct{}

// WithPGNoOlderTails marks ctx for a PostgreSQL statement not answered with
// recent changes older than an earlier answer of the copy on the connection
// read (#3116): in fresh mode SQLOnCopy.Run refuses it before it runs, with
// a *sqlsandbox.MayHaveChangedError.
func WithPGNoOlderTails(ctx context.Context) context.Context {
	return context.WithValue(ctx, pgNoOlderTailsKey{}, true)
}

// pgNoOlderTails reports whether ctx was marked by WithPGNoOlderTails.
func pgNoOlderTails(ctx context.Context) bool {
	v, _ := ctx.Value(pgNoOlderTailsKey{}).(bool)
	return v
}

// PGNoOlderTails reports whether ctx was marked by WithPGNoOlderTails, for
// the host that marks it.
func PGNoOlderTails(ctx context.Context) bool { return pgNoOlderTails(ctx) }

type pgPortStatementKey struct{}

// WithPGPortStatement marks ctx for a statement the PostgreSQL port answers a
// client with (not its catalog reads, not its probes): in fresh mode such a
// statement is never read without its tables' recent changes with nothing
// said (pgTailsUnsaid).
func WithPGPortStatement(ctx context.Context) context.Context {
	return context.WithValue(ctx, pgPortStatementKey{}, true)
}

// pgPortStatement reports whether ctx was marked by WithPGPortStatement.
func pgPortStatement(ctx context.Context) bool {
	v, _ := ctx.Value(pgPortStatementKey{}).(bool)
	return v
}

// PGPortStatement reports whether ctx was marked by WithPGPortStatement, for
// the host that marks it.
func PGPortStatement(ctx context.Context) bool { return pgPortStatement(ctx) }

// pgTailsUnsaid is why a PostgreSQL statement in fresh mode reads its tables
// without their recent changes when statementTailsFresh gave no reason for
// it (#3116), "" when it does not. Three ways led there with nothing said:
// a table that cannot have a tail now, tails stopped over their budget (both
// statementTailsFresh's ok false with no missing), and a statement whose
// tables could not be told from its text (sqlTailTables, tailsKnown false:
// a name with a letter outside ASCII, a qualified name that matches no view,
// a parse that is unsure). The reason then goes where statementTailsFresh's
// own does: a statement marked WithPGNeedTails is refused and the source
// answers it, any other is answered from the snapshot with the reason as its
// note. A statement that reads no table of the copy, or reads the change log
// (read as it is, not at an instant), gets "".
//
// The MySQL port says nothing there, under routing or not; this is the
// PostgreSQL port's only.
func pgTailsUnsaid(tailsKnown bool, tailTables, readTables []views.BaselineTable, allTailed bool, statement string) string {
	if allTailed || sqlMentionsEvents(statement) {
		return ""
	}
	if !tailsKnown {
		if len(readTables) == 0 {
			return ""
		}
		return "the tables this statement reads could not be told from its text, so their newest captured changes were not laid over the snapshot"
	}
	if len(tailTables) == 0 {
		return ""
	}
	names := make([]string, 0, len(tailTables))
	for _, t := range tailTables {
		names = append(names, t.Schema+"."+t.Table)
	}
	return "the newest captured changes of the tables it reads (" + strings.Join(names, ", ") + ") cannot all be laid over the snapshot now"
}

// What a heavy PostgreSQL statement the copy answered with older data was
// told, by kind (PGOlderKind), for the port's status (#3116).
const (
	// PGOlderTailsNotReady: from the snapshot alone, without the recent
	// changes of a table it reads (freshSnapshotNote).
	PGOlderTailsNotReady = "tails_not_ready"
	// PGOlderThanEarlier: with recent changes older than an earlier answer
	// of the copy on the connection read.
	PGOlderThanEarlier = "older_than_earlier"
)

// PGOlderKind is the kind of older data a note of SQLOnCopy.Run says an
// answer holds, "" for a note that says neither.
func PGOlderKind(note string) string {
	switch {
	case strings.HasPrefix(note, pgOlderThanEarlierNote):
		return PGOlderThanEarlier
	case strings.HasPrefix(note, "Answered from its ") && strings.Contains(note, " without the newest captured changes: "):
		return PGOlderTailsNotReady
	}
	return ""
}

// pgOlderThanEarlierNote begins the note of an answer read with recent
// changes older than an earlier answer of the copy on the connection
// (runSQLAt); a test holds the two the same.
const pgOlderThanEarlierNote = "Answered from captured changes older than an earlier answer of the copy on this connection"
