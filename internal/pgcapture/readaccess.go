package pgcapture

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
)

// Read access to the published tables (#3022).
//
// Capturing changes needs no SELECT: logical replication delivers the rows
// over the WAL. The first copy does. It reads every table of the publication
// with COPY (SELECT <columns> FROM <table>), as the role DBTrail connects
// with, so a role without SELECT passed every check of `server add` and the
// first snapshot then stopped with "permission denied for table ..." (42501).
//
// What is asked is what that COPY needs, of the role the connection runs as:
// USAGE on the table's schema, and SELECT on the table or on every column
// the copy reads. PostgreSQL answers through its own privilege functions, so
// ownership, a superuser, a role inherited from another and pg_read_all_data
// all count the way they do for the COPY itself. Nothing is granted here:
// the statements to run are shown, and a person runs them.

// ErrNoReadAccess marks a role that cannot read a table the copy reads, so a
// caller can tell it from a failed catalog read.
var ErrNoReadAccess = errors.New("the role cannot read a published table")

// readAccessNamed is how many tables, and how many statements of one kind, a
// message names. A publication can hold hundreds of tables; past this the
// rest are counted.
const readAccessNamed = 10

// UnreadableTable is one table the role cannot read, and which of the two
// permissions it lacks. At least one of them is true.
type UnreadableTable struct {
	Schema, Table string
	// NoUsage: the role has no USAGE on the schema.
	NoUsage bool
	// NoSelect: the role has SELECT neither on the table nor on every column
	// the copy reads.
	NoSelect bool
}

// ReadAccess is what the role of one connection can read of a publication.
type ReadAccess struct {
	// Role is current_user of the connection asked: the role the copy runs as.
	Role string
	// Tables counts the published tables that were checked, after filters.
	Tables int
	// Unreadable are the ones the role cannot read, by schema and table name.
	Unreadable []UnreadableTable
}

// readAccessQuery lists the tables of publication $1 with what the session's
// role may do with each. The columns asked about are the ones the copy
// reads (pgbaseline loadColumns): live, not dropped, not generated. A table
// with a table-level SELECT needs no look at its columns; without it, the
// copy still works when every one of those columns is granted on its own.
//
// The privilege functions answer NULL for a relation or schema dropped while
// this runs. That table is no longer there to read, so NULL counts as
// readable and the row says nothing.
const readAccessQuery = `
	SELECT pt.schemaname, pt.tablename,
	       COALESCE(has_schema_privilege(n.oid, 'USAGE'), true),
	       COALESCE(has_table_privilege(c.oid, 'SELECT'), true)
	       OR (EXISTS (SELECT 1 FROM pg_attribute a
	                   WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = '')
	           AND NOT EXISTS (SELECT 1 FROM pg_attribute a
	                   WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
	                     AND NOT COALESCE(has_column_privilege(c.oid, a.attnum, 'SELECT'), true)))
	FROM pg_publication_tables pt
	JOIN pg_namespace n ON n.nspname = pt.schemaname
	JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
	WHERE pt.pubname = $1
	ORDER BY pt.schemaname, pt.tablename`

// QueryReadAccess reports which tables of publication the role of conn
// cannot read, among those filters keep: the same narrowing the copy applies,
// so a table it would not read is not asked about. A publication that does
// not exist, or holds no table, answers with Tables 0 and no error: whether
// the publication exists is another check's question.
func QueryReadAccess(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters) (ReadAccess, error) {
	var acc ReadAccess
	if err := conn.QueryRow(ctx, "SELECT current_user::text").Scan(&acc.Role); err != nil {
		return ReadAccess{}, fmt.Errorf("pgcapture: reading the connection's role: %w", err)
	}
	rows, err := conn.Query(ctx, readAccessQuery, publication)
	if err != nil {
		return ReadAccess{}, fmt.Errorf("pgcapture: checking read access to the tables of publication %q: %w", publication, err)
	}
	defer rows.Close()
	for rows.Next() {
		var t UnreadableTable
		var usage, sel bool
		if err := rows.Scan(&t.Schema, &t.Table, &usage, &sel); err != nil {
			return ReadAccess{}, fmt.Errorf("pgcapture: checking read access to the tables of publication %q: %w", publication, err)
		}
		if !filters.Matches(t.Schema, t.Table) {
			continue
		}
		acc.Tables++
		if usage && sel {
			continue
		}
		t.NoUsage, t.NoSelect = !usage, !sel
		acc.Unreadable = append(acc.Unreadable, t)
	}
	// A partial list must never read as the whole one.
	if err := rows.Err(); err != nil {
		return ReadAccess{}, fmt.Errorf("pgcapture: checking read access to the tables of publication %q: %w", publication, err)
	}
	return acc, nil
}

// CheckReadAccess refuses when the role of conn cannot read a table the copy
// reads. The error wraps ErrNoReadAccess and carries, on one line, what is
// missing and the statements to run.
func CheckReadAccess(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters) error {
	acc, err := QueryReadAccess(ctx, conn, publication, filters)
	if err != nil {
		return err
	}
	if len(acc.Unreadable) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s Run on the database, as the tables' owner or a superuser: %s",
		ErrNoReadAccess, acc.Problem(), strings.Join(acc.Grants(), " "))
}

// Problem says, in one line, which tables the role cannot read and what each
// lacks: the first readAccessNamed by name, the rest counted. Empty when
// every table is readable. Pure.
func (a ReadAccess) Problem() string {
	if len(a.Unreadable) == 0 {
		return ""
	}
	named := a.Unreadable[:min(len(a.Unreadable), readAccessNamed)]
	parts := make([]string, len(named))
	for i, t := range named {
		parts[i] = printable(quoteQualified(t.Schema, t.Table)) + " (" + t.lacks() + ")"
	}
	list := strings.Join(parts, ", ")
	if more := len(a.Unreadable) - len(named); more > 0 {
		list += fmt.Sprintf(", and %d more", more)
	}
	return fmt.Sprintf("the role %s cannot read %d of the %s the copy reads: %s.",
		printable(quoteIdent(a.Role)), len(a.Unreadable), countTables(a.Tables), list)
}

func (t UnreadableTable) lacks() string {
	switch {
	case t.NoUsage && t.NoSelect:
		return "no USAGE on its schema, no SELECT"
	case t.NoUsage:
		return "no USAGE on its schema"
	}
	return "no SELECT"
}

// Missing names what is missing in a few words, one entry per permission:
// "USAGE on 1 schema", "SELECT on 3 published tables". Pure.
func (a ReadAccess) Missing() []string {
	schemas, tables := a.lackingSchemas(), a.lackingSelect()
	var out []string
	if n := len(schemas); n == 1 {
		out = append(out, "USAGE on 1 schema")
	} else if n > 1 {
		out = append(out, fmt.Sprintf("USAGE on %d schemas", n))
	}
	if n := len(tables); n == 1 {
		out = append(out, "SELECT on 1 published table")
	} else if n > 1 {
		out = append(out, fmt.Sprintf("SELECT on %d published tables", n))
	}
	return out
}

// lackingSchemas are the schemas the role has no USAGE on, each once, in the
// order they first appear.
func (a ReadAccess) lackingSchemas() []string {
	return distinctSchemas(a.Unreadable, func(t UnreadableTable) bool { return t.NoUsage })
}

func distinctSchemas(tables []UnreadableTable, keep func(UnreadableTable) bool) []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range tables {
		if keep(t) && !seen[t.Schema] {
			seen[t.Schema] = true
			out = append(out, t.Schema)
		}
	}
	return out
}

func (a ReadAccess) lackingSelect() []UnreadableTable {
	var out []UnreadableTable
	for _, t := range a.Unreadable {
		if t.NoSelect {
			out = append(out, t)
		}
	}
	return out
}

// WholeSchemas reports whether Grants names whole schemas instead of each
// table: true past readAccessNamed tables without SELECT. Pure.
func (a ReadAccess) WholeSchemas() bool {
	return len(a.lackingSelect()) > readAccessNamed
}

// Grants are the statements that give the role what it lacks, every
// identifier quoted by PostgreSQL's rules. USAGE first: without it a SELECT
// on the table is not enough.
//
// Up to readAccessNamed tables each get their own GRANT, which gives exactly
// what the copy needs. Past that the statements name each schema once (GRANT
// SELECT ON ALL TABLES IN SCHEMA), which is the statement docs/postgres.md
// gives for the setup, and covers every table of the schema, published or
// not; WholeSchemas says so, for the caller to tell. A list of schemas is cut
// at readAccessNamed too, with a last line, a comment, that counts the rest.
// Pure.
func (a ReadAccess) Grants() []string {
	role := printable(quoteIdent(a.Role))
	var out []string
	add := func(kind string, schemas []string, stmt func(schema string) string) {
		for _, s := range schemas[:min(len(schemas), readAccessNamed)] {
			out = append(out, stmt(s))
		}
		if more := len(schemas) - readAccessNamed; more > 0 {
			out = append(out, fmt.Sprintf("-- and the same %s for %d more schema(s)", kind, more))
		}
	}
	add("GRANT USAGE", a.lackingSchemas(), func(s string) string {
		return "GRANT USAGE ON SCHEMA " + printable(quoteIdent(s)) + " TO " + role + ";"
	})
	tables := a.lackingSelect()
	if !a.WholeSchemas() {
		for _, t := range tables {
			out = append(out, "GRANT SELECT ON TABLE "+printable(quoteQualified(t.Schema, t.Table))+" TO "+role+";")
		}
		return out
	}
	schemas := distinctSchemas(tables, func(UnreadableTable) bool { return true })
	add("GRANT SELECT", schemas, func(s string) string {
		return "GRANT SELECT ON ALL TABLES IN SCHEMA " + printable(quoteIdent(s)) + " TO " + role + ";"
	})
	return out
}

func quoteIdent(name string) string { return pgx.Identifier{name}.Sanitize() }

// printable keeps a name on the line it is printed in: an identifier may hold
// a newline or another control character, and these messages are read as
// lines (a statement per line, an error cut at its first line). A name
// changed here no longer names its table, which only matters for a name that
// could not be pasted from a line anyway.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s)
}
