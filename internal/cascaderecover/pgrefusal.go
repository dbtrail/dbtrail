package cascaderecover

import "fmt"

// Surface names the recover-cascade surface a refusal is worded for.
type Surface string

const (
	SurfaceCLI     Surface = "cli"
	SurfaceConsole Surface = "console"
	SurfaceMCP     Surface = "mcp"
)

// PostgresCascadeError is recover-cascade's refusal on an index written by
// PostgreSQL capture. That capture does not record the source's foreign keys,
// so cascade synthesis finds no children and the script would bring back the
// parent alone while calling itself complete. The children PostgreSQL deleted
// are not lost: logical replication captures them as deletes of their own, in
// the parent's transaction.
//
// The way out it names is the one verified end to end (PostgreSQL 16): plain
// recover filtered on that transaction (its commit position is the rows'
// gtid). The reversal re-inserts the children before the parent (it undoes the
// transaction's changes newest first, and PostgreSQL writes the cascade's
// deletes after the parent's), so PostgreSQL accepts it only with the
// generator's trigger suppression (session_replication_role = replica), which
// only the command line offers. A plain recover script over that window is
// refused by PostgreSQL on the foreign key and applies nothing.
type PostgresCascadeError struct {
	Surface Surface
	Schema  string
}

// PostgresCascadeRefusal returns the refusal worded for surface. schema is the
// one the request named; empty prints a placeholder.
func PostgresCascadeRefusal(surface Surface, schema string) error {
	return &PostgresCascadeError{Surface: surface, Schema: schema}
}

func (e *PostgresCascadeError) Error() string {
	schema := e.Schema
	if schema == "" {
		schema = "<schema>"
	}
	cmd := fmt.Sprintf("bintrail-pg recover --schema %s --gtid <commit position> --suppress-triggers --dry-run", schema)
	why := "recover-cascade cannot run on a PostgreSQL index yet: PostgreSQL capture does not record the source's foreign keys, " +
		"so the cascade cannot be rebuilt from them, and the script would bring back the parent without its children. " +
		"Nothing is lost: the children the cascade deleted are captured as deletes of their own, in the same transaction as the parent's change. "
	priv := "The trigger suppression is what lets PostgreSQL accept the children before the parent; it needs superuser, or " +
		"GRANT SET ON PARAMETER session_replication_role on PostgreSQL 15 and later, and it also skips the foreign-key checks for that transaction"
	switch e.Surface {
	case SurfaceConsole:
		return why + "The web interface cannot build this undo yet (its recover writes the children before the parent, and PostgreSQL " +
			"refuses that on the foreign key). Run it from the command line, over that transaction (the commit position is the gtid of the " +
			"parent's change, shown in its history): `" + cmd + "`. " + priv + "."
	case SurfaceMCP:
		return why + "The `recover` tool cannot build this undo yet (it writes the children before the parent, and PostgreSQL refuses " +
			"that on the foreign key). An operator can run it from the command line, over that transaction (the commit position is the gtid " +
			"the `query` tool shows for the parent's change): `" + cmd + "`. " + priv + "."
	default:
		return why + "Undo that whole transaction with plain recover, filtered on it (the commit position is the gtid of the parent's " +
			"change, shown by `bintrail-pg query`): `" + cmd + "`. " + priv + "."
	}
}
