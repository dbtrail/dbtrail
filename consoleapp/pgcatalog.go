package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// pgCatalogColumnsSQL lists every column a statement on the copy can read,
// with the copy engine's type: the columns of its views, the server's hidden
// ones already left out, and none of the copy's own schema (main, where the
// change log is).
const pgCatalogColumnsSQL = "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns " +
	"WHERE table_catalog = current_database() AND table_schema <> 'main' ORDER BY table_schema, table_name, ordinal_position"

// pgCopyCatalog is the PostgreSQL port's catalog of a server's copy (#2769):
// the tables and columns a statement there reads, as the copy's engine has
// them, and from the snapshot what the source recorded of each table (its
// OID, each column's type there, NOT NULL, defaults, the primary key). A
// column the snapshot does not describe is reported with nothing of the
// source's; a primary key one of whose columns the server hides is not
// reported at all.
func pgCopyCatalog(ctx context.Context, copySQL pgCatalogSource, run func(context.Context, string) (sqlsandbox.Result, error)) ([]pgshim.CatalogTable, error) {
	res, copyTables, err := pgCatalogOneSnapshot(ctx, copySQL, run)
	if err != nil {
		return nil, err
	}
	recorded := map[[2]string]console.CopyTable{}
	for _, t := range copyTables {
		recorded[[2]string{t.Schema, t.Table}] = t
	}
	var out []pgshim.CatalogTable
	for _, r := range res.Rows {
		if len(r) != 4 {
			return nil, fmt.Errorf("the copy's list of columns has a row of %d cells", len(r))
		}
		schema, table, col, typ := fmt.Sprint(r[0]), fmt.Sprint(r[1]), fmt.Sprint(r[2]), fmt.Sprint(r[3])
		if n := len(out); n == 0 || out[n-1].Schema != schema || out[n-1].Name != table {
			out = append(out, pgshim.CatalogTable{Schema: schema, Name: table})
		}
		t := &out[len(out)-1]
		c := pgshim.CatalogColumn{Name: col, Type: typ}
		if rec, ok := lookupRecorded(recorded, schema, table); ok && rec.Relation != nil {
			for _, rc := range rec.Relation.Columns {
				// The copy's engine matches names without regard to case.
				if strings.EqualFold(rc.Name, col) {
					c.NotNull, c.Default, c.Identity = rc.NotNull, rc.Default, rc.Identity
					// The type on the source, which the port's screen reads
					// (#3068). Of two columns whose names differ only in
					// case, a char(n) is the one kept: the copy cannot tell
					// them apart, and that one is the one to be careful of.
					if !pgshim.PaddedOnSource(c.SourceType) {
						c.SourceType, c.SourceTypeMod = rc.TypeOID, rc.TypeMod
					}
					c.TrimmedOnCopy = c.TrimmedOnCopy || rec.Trimmed[rc.Name]
				}
			}
		}
		t.Columns = append(t.Columns, c)
	}
	for i := range out {
		t := &out[i]
		rec, ok := lookupRecorded(recorded, t.Schema, t.Name)
		if !ok || rec.Relation == nil {
			// Said, not left to be noticed: the catalog lists this table
			// without NOT NULL, defaults or a primary key.
			slog.Warn("PostgreSQL port: a table's snapshot does not record its shape, so the catalog lists it without NOT NULL, defaults or a primary key; a new snapshot records it",
				"schema", t.Schema, "table", t.Name)
			continue
		}
		t.OID = rec.Relation.OID
		have := map[string]bool{}
		for _, c := range t.Columns {
			have[strings.ToLower(c.Name)] = true
		}
		whole := len(rec.Relation.PrimaryKey) > 0
		for _, k := range rec.Relation.PrimaryKey {
			whole = whole && have[strings.ToLower(k)]
		}
		if whole {
			t.PrimaryKey = append([]string(nil), rec.Relation.PrimaryKey...)
		}
	}
	return out, nil
}

// pgCatalogSource is what pgCopyCatalog reads of the copy besides its
// columns (console.SQLOnCopy).
type pgCatalogSource interface {
	PGCopyStampFresh(ctx context.Context) (string, error)
	CopyTablesStamped(ctx context.Context) ([]console.CopyTable, string, error)
}

// pgCatalogOneSnapshot reads the copy's columns and what the snapshot
// recorded of each table, both of one snapshot: each is a read of its own,
// and a new snapshot between them would list one snapshot's columns with
// the other's types, and say a column the view trims is not trimmed, or the
// other way round (TrimmedOnCopy). The stamp before the columns and the one
// with the tables must agree; a snapshot changes about every few minutes,
// so one more try settles it, and a second change is an error the client
// can try again (40001), never a mix of the two.
func pgCatalogOneSnapshot(ctx context.Context, copySQL pgCatalogSource, run func(context.Context, string) (sqlsandbox.Result, error)) (sqlsandbox.Result, []console.CopyTable, error) {
	for try := 0; ; try++ {
		before, err := copySQL.PGCopyStampFresh(ctx)
		if err != nil {
			return sqlsandbox.Result{}, nil, err
		}
		res, err := run(ctx, pgCatalogColumnsSQL)
		if res.Spool != nil {
			_ = res.Spool.Close()
		}
		if err != nil {
			return sqlsandbox.Result{}, nil, err
		}
		if res.Truncated {
			return sqlsandbox.Result{}, nil, &pgshim.Error{Code: "54000", Message: "the copy has more columns than one statement returns on this port, so its catalog cannot be listed whole"}
		}
		copyTables, after, err := copySQL.CopyTablesStamped(ctx)
		if err != nil {
			return sqlsandbox.Result{}, nil, err
		}
		if before == after {
			return res, copyTables, nil
		}
		if try == 1 {
			return sqlsandbox.Result{}, nil, &pgshim.Error{Code: "40001", Message: "the copy took a new snapshot twice while its catalog was read; try the statement again"}
		}
	}
}

// lookupRecorded finds a table's snapshot record, its schema and name
// matched without regard to case, as the copy's engine matches them.
func lookupRecorded(recorded map[[2]string]console.CopyTable, schema, table string) (console.CopyTable, bool) {
	if t, ok := recorded[[2]string{schema, table}]; ok {
		return t, true
	}
	for k, t := range recorded {
		if strings.EqualFold(k[0], schema) && strings.EqualFold(k[1], table) {
			return t, true
		}
	}
	return console.CopyTable{}, false
}
