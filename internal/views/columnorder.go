package views

import (
	"regexp"
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
// The answer is about the PINNED view, the one the console's SQL session
// always generates: it asks selectList, as that view's generation does. A
// file that follows later snapshots carries no column list at all
// (writeStateViews), so there SELECT * is the file's order for every table,
// whatever this says.
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

// NamesUnlikeMySQL says why a NAME in statement could resolve on this table's
// pinned view to something other than what it is on MySQL, or "" when no name
// can: the question read routing asks of every table a statement reads
// (#2123).
//
// A snapshot holds no generated column. A statement that names one does not
// always fail on the copy: the name binds to whatever else answers to it
// there. Measured on DuckDB 1.5: another table's column of that name, in the
// same FROM or in an outer query; a select-list alias, from WHERE, GROUP BY,
// HAVING and from a later item of the same list; a table alias (the whole row
// comes back as one value); and, for a name such as user or current_date,
// the function DuckDB calls without parentheses. One table is enough for the
// last three. So the rule does not look at how many tables the statement
// reads: a statement whose text holds the name of a column this table lacks
// on the copy is not the copy's to answer.
//
// The text is searched, not parsed. A name that stands in a string, in a
// comment or inside a longer word keeps the statement on the source too:
// that costs a statement the copy could have answered, and no reading of the
// statement can be wrong about it. The search folds ASCII case only, and a
// statement with any character outside ASCII over such a table is refused
// without being searched: which letters outside ASCII a server takes for an
// ASCII one when it compares names (a Kelvin sign for k, an accented letter)
// depends on the server and its version, and is not followed here.
//
// One name is looked for over every table: _rowid, which a server answers for
// a table with a key of one integer column and no definition lists.
//
// Three tables are refused whatever the statement says, because what they
// lack on the copy is not known by name: one with no table definition (a
// snapshot written before the definition was embedded may or may not hold
// what MySQL holds), one whose file was not confirmed to hold exactly the
// columns its definition lists, and one with a column definition that could
// not be read. Not known is never read as "nothing is missing". The same for
// a missing column whose name is not plain ASCII letters, digits, _ and $:
// a server folds letters outside ASCII in ways this search does not follow,
// and a name with a quote in it is written doubled in a statement.
func (t BaselineTable) NamesUnlikeMySQL(statement string) string {
	switch {
	case !t.SchemaKnown:
		return "its snapshot carries no table definition, or it could not be read just now, so the columns it lacks on the copy are not known"
	case len(t.Columns) == 0:
		return "the columns its snapshot holds could not be checked against its table definition"
	case t.NotHeldUnread:
		return "a column definition in its snapshot could not be read, so the columns it lacks on the copy are not all known"
	}
	// Whatever the table: _rowid is MySQL's and MariaDB's other name for a
	// key made of one integer column. No CREATE TABLE lists it and no
	// snapshot holds it, and whether this table has such a key is not read
	// here, so the name keeps a statement over any table on the source.
	// Searched after upper and lower, which also brings the dotless i down
	// to i, as a server that compares names without case may.
	if strings.Contains(strings.ToLower(strings.ToUpper(statement)), "_rowid") {
		return "the statement names _rowid, which MySQL answers for a table with a key of one integer column and a snapshot does not hold"
	}
	if len(t.NotHeld) == 0 {
		return ""
	}
	for _, name := range t.NotHeld {
		if !searchableName.MatchString(name) {
			return "MySQL computes its column " + name + ", which a snapshot does not hold, and that name cannot be looked for in a statement"
		}
	}
	if !isASCII(statement) {
		return "the statement holds characters outside ASCII, so it cannot be searched for " + strings.Join(t.NotHeld, ", ") +
			", which MySQL computes and a snapshot does not hold"
	}
	folded := strings.ToLower(statement)
	for _, name := range t.NotHeld {
		if strings.Contains(folded, strings.ToLower(name)) {
			return "the statement names " + name + ", a column MySQL computes and a snapshot does not hold"
		}
	}
	return ""
}

// isASCII reports whether every byte of s is an ASCII one.
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// searchableName is a column name a statement can only spell one way, up to
// case: no quote to double, no letter MySQL folds onto another.
var searchableName = regexp.MustCompile(`^[A-Za-z0-9_$]+$`)
