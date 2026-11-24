package console

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

// The PostgreSQL port's check of the char(n) columns a statement relies on
// the view to trim (#3219). The port decides with its catalog, which is one
// snapshot's; the statement runs on the snapshot of its moment, and a column
// whose type changed between the two would be compared as the catalog said.
// So the port hands over the names it took as trimmed (pgshim.RunOptions.
// Trimmed), and the copy refuses the statement, which the port then sends to
// the source, when a table it reads holds one of them as a char(n) its view
// does not trim. A new snapshot with the same types refuses nothing.

type pgTrimmedKey struct{}

// PGTrimmedUnknown, among the names, says the port could not read which
// columns the statement takes as trimmed: any table it reads that holds a
// char(n) refuses it.
const PGTrimmedUnknown = "\x00"

// WithPGTrimmed marks ctx with the names (lowercased) of the char(n) columns
// a PostgreSQL statement relies on the view to trim.
func WithPGTrimmed(ctx context.Context, names []string) context.Context {
	if len(names) == 0 {
		return ctx
	}
	return context.WithValue(ctx, pgTrimmedKey{}, names)
}

func pgTrimmed(ctx context.Context) []string {
	v, _ := ctx.Value(pgTrimmedKey{}).([]string)
	return v
}

// pgTrimmedMoved says why a statement that relies on the view trimming the
// char(n) columns names is not answered by the copy over tables, the ones it
// reads: one holds a column of one of those names that is a char(n) or a
// char(n)[] its view does not trim, or does not record its columns. "" when
// none does.
func pgTrimmedMoved(names []string, tables []views.BaselineTable) string {
	unknown := slices.Contains(names, PGTrimmedUnknown)
	for _, t := range tables {
		if t.PGRelation == nil {
			return fmt.Sprintf("the snapshot of %s.%s does not record its columns, so whether a char(n) of it is read without its padding cannot be told", t.Schema, t.Table)
		}
		trimmed := baseline.PGPaddedToTrim(t.PGRelation)
		for _, c := range t.PGRelation.Columns {
			padded := c.TypeOID == pgOIDBpchar || c.TypeOID == pgOIDBpcharArray
			if !padded {
				continue
			}
			if unknown && trimmed[c.Name] {
				return fmt.Sprintf("which columns of %s.%s the statement takes as read without their padding could not be told", t.Schema, t.Table)
			}
			if slices.Contains(names, strings.ToLower(c.Name)) && !trimmed[c.Name] {
				return fmt.Sprintf("the column %q of %s.%s is a char(n) this snapshot does not read without its padding, unlike the one the statement was read with", c.Name, t.Schema, t.Table)
			}
		}
	}
	return ""
}

// The source's types pgTrimmedMoved asks of a column.
const (
	pgOIDBpchar      = 1042
	pgOIDBpcharArray = 1014
)

// PGTrimmedNames are the names WithPGTrimmed marked ctx with, for the host
// that marks it.
func PGTrimmedNames(ctx context.Context) []string { return pgTrimmed(ctx) }
