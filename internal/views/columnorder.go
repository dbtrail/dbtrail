package views

import (
	"strings"
)

// Column order (#2111).
//
// A snapshot file holds its table's columns sorted by name (the baseline
// writer builds the Parquet schema from a map), so a state view written as
// `SELECT * FROM read_parquet(...)` returns them in that order, where MySQL
// returns them in the order the table declares. A reader by name never
// notices; a reader by position (a driver's row tuple, `INSERT ... SELECT *`,
// a CSV export) gets other columns with no error.
//
// A pinned view therefore lists the columns itself, in the order the CREATE
// TABLE in the file's footer declares them (BaselineTable.Columns), and puts
// the casts and collations the REPLACE list used to carry on the columns
// where they stand. Everything else keeps the star: a table whose order is
// not known, and every view that follows later snapshots.

// selectList is the select list of a pinned state view: every column of the
// table in the order MySQL returns them, each one bare or under the
// expression replaceParts gives it. "" when the table's order is not known,
// and the caller then emits the star (with its REPLACE list) it always did.
//
// The invariant replaceClause rests on is wider here: EVERY name in this list
// must exist in the file, or the view does not bind and takes the generated
// script with it, and every column of the file must be in the list, or the
// view hides it. baseline.ReadTableFooters checks both against the file's own
// column list before it reports an order at all, so nothing is re-checked
// here beyond what this function can see: a cast or a collation for a column
// the order does not name means the two were not read from one CREATE TABLE,
// and the list is refused rather than built without it.
func selectList(t BaselineTable) string {
	if !t.SchemaKnown || len(t.Columns) == 0 {
		return ""
	}
	exprs := map[string]string{}
	for _, p := range replaceParts(t) {
		exprs[p.name] = p.expr
	}
	items := make([]string, 0, len(t.Columns))
	for _, name := range t.Columns {
		if expr, ok := exprs[name]; ok {
			items = append(items, expr+" AS "+quoteIdent(name))
			delete(exprs, name)
			continue
		}
		items = append(items, quoteIdent(name))
	}
	if len(exprs) > 0 {
		return ""
	}
	return strings.Join(items, ", ")
}

// ordered wraps a state body that is built around a star (the table-delta
// bodies of internal/baseline) so that it returns the table's columns in the
// table's order: body is called with the REPLACE list to apply, and when the
// order is known that list is empty and the casts ride in the outer select
// list instead. With the order unknown the body is returned as it was.
func ordered(t BaselineTable, body func(replace string) string) string {
	if list := selectList(t); list != "" {
		return "SELECT " + list + " FROM (" + body("") + ")"
	}
	return body(replaceClause(t))
}

// writeColumnOrderNote says, once and above the views, in what order
// `SELECT *` returns a table's columns, because it is not the same in every
// file this package writes.
func writeColumnOrderNote(b *strings.Builder, in Input) {
	if in.Follow.follows() {
		b.WriteString("--\n")
		b.WriteString("-- SELECT * on these views returns each table's columns sorted by name, the\n")
		b.WriteString("-- order the snapshot files hold them in, not the order the table declares\n")
		b.WriteString("-- them in. Name the columns wherever their position matters. A file that\n")
		b.WriteString("-- follows later snapshots carries no column list on purpose: a column dropped\n")
		b.WriteString("-- at the source would stop every query on its table until the file is\n")
		b.WriteString("-- generated again.\n")
		return
	}
	for _, t := range in.SelectedBaselines() {
		if selectList(t) != "" {
			b.WriteString("--\n")
			b.WriteString("-- Each view lists its table's columns in the order the table declares them,\n")
			b.WriteString("-- so SELECT * returns them as MySQL does. The snapshot files hold them sorted\n")
			b.WriteString("-- by name; a table whose order could not be read is named below.\n")
			return
		}
	}
}

// columnOrderComments are a pinned view's own notes about `SELECT *`: that it
// returns the file's order because the table's is not known, and that MySQL
// returns a different set of columns. A table with no schema at all is not
// named here: decimalComments already says what that costs, this included.
func columnOrderComments(t BaselineTable) []string {
	if !t.SchemaKnown {
		return nil
	}
	var out []string
	if selectList(t) == "" {
		out = append(out, "SELECT * returns the columns in alphabetical order, not the table's: "+
			"the columns this file holds could not be read, or are not the ones its CREATE TABLE lists")
	}
	if t.StarDiffers != "" {
		out = append(out, "SELECT * differs from MySQL's: "+commentSafe(t.StarDiffers))
	}
	return out
}

// StarUnlikeMySQL says why `SELECT *` on this table's pinned view does not
// return what MySQL's does, or "" when it does: the same columns in the same
// order. A caller that must answer as MySQL would (read routing) keeps a
// statement with a star over such a table away from the copy.
//
// It asks selectList, not the fields, so the answer cannot drift from what
// the view is generated with.
func (t BaselineTable) StarUnlikeMySQL() string {
	switch {
	case !t.SchemaKnown:
		return "its snapshot carries no table definition, or it could not be read just now, so the order of its columns is not known"
	case selectList(t) == "":
		return "the order of its columns could not be read from its snapshot"
	case t.StarDiffers != "":
		return t.StarDiffers
	}
	return ""
}
