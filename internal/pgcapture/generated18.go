package pgcapture

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Generated columns on PostgreSQL 18.
//
// PostgreSQL 18 refuses an UPDATE or a DELETE on a table whose replica
// identity contains a generated column that a publication the table belongs
// to does not publish ("Replica identity must not contain unpublished
// generated columns", SQLSTATE 42P10). Capture needs REPLICA IDENTITY FULL,
// which contains every column, so on a table with a generated column the
// setup this product asks for stops the SOURCE's own application from
// changing or deleting rows, before capture ever connects. INSERT still works.
//
// Measured on PostgreSQL 18.7:
//   - a STORED generated column is published when the publication is created
//     or altered WITH (publish_generated_columns = stored); then writes work;
//   - a VIRTUAL one (new in 18, and the default kind there) cannot be
//     published at all, so no publication setting helps;
//   - one publication is enough to refuse the write: a table in two
//     publications, one that publishes the column and one that does not, is
//     refused;
//   - with REPLICA IDENTITY DEFAULT the write works, which is the way out for
//     an application that is already stuck.
//
// PostgreSQL 17 and older have no such rule: they never publish a generated
// column and never check.
//
// What a publication publishes of a table is pg_publication_tables.attnames,
// so a generated column that is not in it is the whole condition; no setting
// is read and none is guessed.

// ErrGeneratedBlocksWrites marks the condition, so a caller can tell it from
// a failed catalog read.
var ErrGeneratedBlocksWrites = errors.New("a generated column that is not published blocks UPDATE and DELETE on PostgreSQL 18")

// UnpublishedGenerated is one generated column of a table that records whole
// rows and that Publication, which the table belongs to, does not publish.
type UnpublishedGenerated struct {
	Schema, Table, Column string
	// Virtual: a VIRTUAL generated column, which no publication can publish.
	Virtual bool
	// Publication is the publication that leaves the column out. It can be
	// another one than the capture's own: any publication of the table counts.
	Publication string
}

// pgGeneratedRuleVersion is the first server_version_num with the rule.
const pgGeneratedRuleVersion = 180000

// unpublishedGeneratedQuery lists, for every table of publication $1 at
// REPLICA IDENTITY FULL, each generated column that some publication of the
// table leaves out of what it publishes.
const unpublishedGeneratedQuery = `
	SELECT ours.schemaname, ours.tablename, a.attname, a.attgenerated = 'v', pt.pubname
	FROM pg_publication_tables ours
	JOIN pg_namespace n ON n.nspname = ours.schemaname
	JOIN pg_class c ON c.relname = ours.tablename AND c.relnamespace = n.oid
	JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> ''
	JOIN pg_publication_tables pt ON pt.schemaname = ours.schemaname AND pt.tablename = ours.tablename
	WHERE ours.pubname = $1 AND c.relreplident = 'f' AND NOT (a.attname = ANY (pt.attnames))
	ORDER BY 1, 2, 3, 5`

// QueryUnpublishedGenerated returns the generated columns that make
// PostgreSQL 18 refuse UPDATE and DELETE on tables of publication. Empty on
// a server older than 18, which has no such rule, and when there is none.
func QueryUnpublishedGenerated(ctx context.Context, conn *pgx.Conn, publication string) ([]UnpublishedGenerated, error) {
	var version int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		return nil, fmt.Errorf("pgcapture: reading server_version_num to check generated columns: %w", err)
	}
	if version < pgGeneratedRuleVersion {
		return nil, nil
	}
	rows, err := conn.Query(ctx, unpublishedGeneratedQuery, publication)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: checking generated columns of publication %q: %w", publication, err)
	}
	defer rows.Close()
	var found []UnpublishedGenerated
	for rows.Next() {
		var g UnpublishedGenerated
		if err := rows.Scan(&g.Schema, &g.Table, &g.Column, &g.Virtual, &g.Publication); err != nil {
			return nil, fmt.Errorf("pgcapture: checking generated columns of publication %q: %w", publication, err)
		}
		found = append(found, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: checking generated columns of publication %q: %w", publication, err)
	}
	return found, nil
}

// CheckGeneratedColumns refuses a source on which the setup capture needs
// stops the source's own application from updating or deleting rows. The
// error wraps ErrGeneratedBlocksWrites and says how to let the application
// write again first, then how to capture the table, or that it cannot be.
func CheckGeneratedColumns(ctx context.Context, conn *pgx.Conn, publication string) error {
	found, err := QueryUnpublishedGenerated(ctx, conn, publication)
	if err != nil {
		return err
	}
	if len(found) == 0 {
		return nil
	}
	return fmt.Errorf("pgcapture: %s: %w", GeneratedColumnsProblem(found), ErrGeneratedBlocksWrites)
}

// GeneratedColumnsProblem says what found means and what to do, in the order
// an operator needs it: the application first. Pure.
func GeneratedColumnsProblem(found []UnpublishedGenerated) string {
	type key struct{ schema, table string }
	tables := map[key]bool{}
	var stored, virtual []string
	storedPubs := map[string]bool{}
	// A table in two publications is taken out of each: one statement for
	// every (table, publication), or the advice leaves the table blocked.
	type inPub struct {
		key
		pub string
	}
	virtualTables := map[inPub]bool{}
	for _, g := range found {
		k := key{g.Schema, g.Table}
		tables[k] = true
		col := fmt.Sprintf("%s.%s.%s (publication %q)", g.Schema, g.Table, g.Column, g.Publication)
		if g.Virtual {
			virtual = append(virtual, col)
			virtualTables[inPub{k, g.Publication}] = true
		} else {
			stored = append(stored, col)
			storedPubs[g.Publication] = true
		}
	}
	names := make([]string, 0, len(tables))
	for k := range tables {
		names = append(names, quoteQualified(k.schema, k.table))
	}
	sort.Strings(names)

	var b strings.Builder
	fmt.Fprintf(&b, "on PostgreSQL 18 the source's own application cannot UPDATE or DELETE rows of %s: ", countTables(len(names)))
	b.WriteString("each records whole rows (REPLICA IDENTITY FULL) and has a generated column that a publication it belongs to does not publish, " +
		"which PostgreSQL 18 refuses (\"Replica identity must not contain unpublished generated columns\"). ")
	b.WriteString("To let the application write again now, run on each: ")
	for i, n := range names {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "ALTER TABLE %s REPLICA IDENTITY DEFAULT;", n)
	}
	if len(stored) > 0 {
		pubs := make([]string, 0, len(storedPubs))
		for p := range storedPubs {
			pubs = append(pubs, p)
		}
		sort.Strings(pubs)
		b.WriteString(" Then, to capture a table whose generated column is STORED (")
		b.WriteString(strings.Join(stored, ", "))
		b.WriteString("), make the publication publish it and set REPLICA IDENTITY FULL again:")
		for _, p := range pubs {
			fmt.Fprintf(&b, " ALTER PUBLICATION %s SET (publish_generated_columns = stored);", pgx.Identifier{p}.Sanitize())
		}
	}
	if len(virtual) > 0 {
		b.WriteString(" A VIRTUAL generated column (")
		b.WriteString(strings.Join(virtual, ", "))
		b.WriteString(") cannot be published by any setting, so its table cannot be captured: take it out of the publication:")
		vt := make([]string, 0, len(virtualTables))
		for k := range virtualTables {
			vt = append(vt, fmt.Sprintf(" ALTER PUBLICATION %s DROP TABLE %s;", pgx.Identifier{k.pub}.Sanitize(), quoteQualified(k.schema, k.table)))
		}
		sort.Strings(vt)
		b.WriteString(strings.Join(vt, ""))
		b.WriteString(" (a FOR ALL TABLES publication has no way to leave one table out: use a publication that lists its tables)")
	}
	return b.String()
}

func quoteQualified(schema, table string) string {
	return pgx.Identifier{schema, table}.Sanitize()
}

func countTables(n int) string {
	if n == 1 {
		return "1 table"
	}
	return fmt.Sprintf("%d tables", n)
}
