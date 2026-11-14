package views

import (
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// DecimalColumn names one decimal or numeric column of a baseline table, with
// the precision and scale it was declared with. It is the baseline package's
// own type: the command layer reads these out of the Parquet footer and hands
// them straight over, and a second definition here would be one more place for
// the two to drift.
type DecimalColumn = baseline.DecimalColumn

// The baseline writer stores every MySQL DECIMAL and NUMERIC as text, because
// MySQL allows 65 digits of precision and DuckDB stops at 38
// (internal/baseline.MysqlToParquetNode has the full reasoning). That
// choice is right for storage and wrong for the reader: a state view built with
// a bare SELECT * hands DuckDB a VARCHAR, and the first aggregate anyone writes
// against a money column fails with
//
//	Binder Error: No function matches the given name and argument types 'sum(VARCHAR)'
//
// which reads like the data is wrong rather than like a storage choice (#1486).
//
// So the generated view casts those columns back, using the precision and scale
// the column was declared with rather than anything inferred from the stored
// text. The file is where the reader is standing when they hit this, so the
// file is where it gets fixed.

// BaselinePaths lists the Parquet files of the tables this file will describe,
// for a caller about to resolve their schemas.
func (in Input) BaselinePaths() []string {
	paths := make([]string, 0, len(in.Baselines))
	for _, t := range in.Baselines {
		paths = append(paths, t.Path)
	}
	return paths
}

// ApplyFooters records each table's decimal columns, and whether a table
// delta reserves one of its column names, from a map keyed by Parquet path, as
// baseline.TableFootersFor returns it. Pure, so the command layers that
// resolve the map share one rule for reading it.
//
// A table missing from the map keeps SchemaKnown false and is reported in the
// generated file as a table whose types could not be read. That is the same
// answer a caller gets by skipping this entirely after a failed resolution, so
// the degraded path and the partly-degraded path say the same thing.
func (in *Input) ApplyFooters(footers map[string]baseline.TableFooter) {
	for i := range in.Baselines {
		f, ok := footers[in.Baselines[i].Path]
		if !ok {
			continue
		}
		if f.PG != nil {
			// A PostgreSQL source's file: its own casts and column order, and
			// none of what follows, which is read from a MySQL CREATE TABLE.
			// SchemaKnown stays false on purpose: every reader of it asks
			// about that CREATE TABLE.
			in.Baselines[i].PGCasts = f.PG
			in.Baselines[i].Columns = f.Columns
			continue
		}
		in.Baselines[i].Decimals = f.Decimals
		in.Baselines[i].Datetimes = f.Datetimes
		in.Baselines[i].BinaryText = f.BinaryText
		in.Baselines[i].DeltaReserved = f.DeltaReserved
		in.Baselines[i].Columns = f.Columns
		in.Baselines[i].StarDiffers = f.StarDiffers
		in.Baselines[i].NotHeld = f.NotHeld
		in.Baselines[i].NotHeldUnread = f.NotHeldUnread
		in.Baselines[i].Temporal = f.Temporal
		in.Baselines[i].DistinctSafe, in.Baselines[i].DistinctOther = f.DistinctSafe, f.DistinctOther
		in.Baselines[i].UnixTimeSafe, in.Baselines[i].UnixTimeOther = f.UnixTimeSafe, f.UnixTimeOther
		in.Baselines[i].WholeSafe, in.Baselines[i].WholeOther = f.WholeSafe, f.WholeOther
		in.Baselines[i].Text = f.Text
		in.Baselines[i].ZeroDate = f.ZeroDate
		in.Baselines[i].Float = f.Float
		in.Baselines[i].SchemaKnown = true
	}
}

// writeDecimalNote explains the casts once, above the views, rather than
// repeating it in every one of them. A reader who finds a CAST in their own
// state view should be able to learn here why it is there, instead of assuming
// bintrail decided their money column needed rounding.
func writeDecimalNote(b *strings.Builder, in Input) {
	var cast, uncastable, unknown bool
	for _, t := range in.Baselines {
		if len(t.PGCasts) > 0 && !in.Follow.follows() {
			continue
		}
		if !t.SchemaKnown {
			unknown = true
			continue
		}
		for _, d := range t.Decimals {
			if castableDecimal(d) {
				cast = true
			} else {
				uncastable = true
			}
		}
	}
	if !cast && !uncastable && !unknown {
		return
	}
	b.WriteString("--\n")
	if cast || uncastable {
		b.WriteString("-- DECIMAL and NUMERIC columns are stored as text, so that a value MySQL can\n")
		b.WriteString("-- hold is never rounded to fit a narrower type. The views below cast them\n")
		b.WriteString("-- back to DECIMAL with the precision and scale the column was declared with,\n")
		b.WriteString("-- so sum() and the rest work on them directly.\n")
	}
	if uncastable {
		fmt.Fprintf(b, "-- Columns wider than %d digits have no DuckDB DECIMAL to be cast to, so they\n",
			baseline.MaxDuckDBDecimalPrecision)
		b.WriteString("-- stay text. They are named below. Cast them yourself when you need\n")
		b.WriteString("-- arithmetic; DOUBLE works if an approximate result is acceptable.\n")
	}
	if unknown {
		b.WriteString("-- Some files carry no column types, so their views cast nothing and every\n")
		if in.Follow.follows() {
			b.WriteString("-- decimal column in them reads as text. Those tables are named below. A\n")
		} else {
			// Only a pinned file gives _bin columns their collation
			// (writeStateViews), so only there is its absence a loss to name.
			b.WriteString("-- decimal column in them reads as text; no collations, so a text column\n")
			b.WriteString("-- MySQL declares _bin compares like any other, by the session's default\n")
			b.WriteString("-- collation; and no column order, so SELECT * returns their columns sorted\n")
			b.WriteString("-- by name. Those tables are named below. A\n")
		}
		b.WriteString("-- baseline older than this feature gains the casts when it is next taken or\n")
		b.WriteString("-- refreshed, and so does a PostgreSQL-source baseline written before its\n")
		b.WriteString("-- table shape was recorded. If a footer could not be read at all, the\n")
		b.WriteString("-- bintrail log has the error.\n")
	}
}

// writePGCastNote says, once, how the views read a PostgreSQL source's
// tables: a reader who finds a CAST or a COLLATE in their own state view
// should learn here where it came from.
func writePGCastNote(b *strings.Builder, in Input) {
	if in.Follow.follows() {
		// A following file reads these tables as text (writeStateViews).
		return
	}
	for _, t := range in.Baselines {
		if len(t.PGCasts) == 0 {
			continue
		}
		b.WriteString("--\n")
		b.WriteString("-- A PostgreSQL source's snapshot files hold every value as the text PostgreSQL\n")
		b.WriteString("-- wrote. The views below read each column as the type the table declares:\n")
		b.WriteString("-- whole numbers, numeric, real and double precision, boolean, date, timestamp\n")
		b.WriteString("-- with and without time zone, time with and without time zone, interval, uuid\n")
		b.WriteString("-- and bytea. Text columns are given byte comparison (COLLATE C), so they\n")
		b.WriteString("-- compare with regard to case as PostgreSQL's do. A numeric declared without\n")
		b.WriteString("-- precision is read as an exact\n")
		fmt.Fprintf(b, "-- DECIMAL(%d,%d). A value its type here cannot hold (NaN in a numeric, more\n",
			baseline.MaxDuckDBDecimalPrecision, baseline.PGNumericScale)
		b.WriteString("-- digits than fit) fails the statement that reads it; it is never rounded or\n")
		b.WriteString("-- read as NULL. An array of one dimension is read as a list (labels[1] is its\n")
		b.WriteString("-- first element) unless it is named below; one of more dimensions fails the\n")
		b.WriteString("-- statement that reads it. Columns of any other type stay text and are named\n")
		b.WriteString("-- below.\n")
		return
	}
}

// writeBinaryCollationNote says, once, why some text columns carry COLLATE C:
// a reader who finds it in their own state view should learn here that it is
// the column's MySQL collation, not a choice made for them.
func writeBinaryCollationNote(b *strings.Builder, in Input) {
	for _, t := range in.Baselines {
		if t.SchemaKnown && len(t.BinaryText) > 0 {
			b.WriteString("--\n")
			b.WriteString("-- Text columns MySQL declares under a _bin collation are given byte\n")
			b.WriteString("-- comparison (COLLATE C) in the views below, so they compare, group and sort\n")
			b.WriteString("-- as on MySQL whatever the session's default collation is.\n")
			return
		}
	}
}

// decimalComments returns the per-view notes: the columns this table could not
// have cast, and why. pinned says the file gives _bin columns their collation
// (a following file does not), so a table without column types loses that too. Silence would be the same bug in miniature, a column that
// reads as text with nothing anywhere saying so.
func decimalComments(t BaselineTable, pinned bool) []string {
	if len(t.PGCasts) > 0 {
		var text []string
		for _, c := range t.PGCasts {
			if c.As == "" {
				text = append(text, commentSafe(c.Name))
			}
		}
		if len(text) == 0 {
			return nil
		}
		return []string{strings.Join(text, ", ") + " (left as text: a type the views do not read)"}
	}
	if !t.SchemaKnown && pinned {
		return []string{"this file carries no column types, so nothing is cast; " +
			"decimal columns read as text, _bin columns compare by the session's collation " +
			"and SELECT * returns the columns in alphabetical order"}
	}
	if !t.SchemaKnown {
		// Deliberately does NOT say the footer could not be read. Three
		// different things land here and only one of them is a fault: a
		// baseline older than the embedded CREATE TABLE, a PostgreSQL-source
		// baseline (which never carries that key, by design, and whose values
		// are all text anyway), and a footer that genuinely would not open.
		// Naming the last one for all three sends two of them hunting a corrupt
		// file that is fine.
		return []string{"this file carries no column types, so nothing is cast; " +
			"decimal columns read as text"}
	}
	var uncastable []string
	for _, d := range t.Decimals {
		if castableDecimal(d) {
			continue
		}
		// The reason, per column, rather than one blanket sentence: a column
		// refused for an unreadable precision is not a column wider than
		// DuckDB, and telling an operator their DECIMAL(10,2) is too wide
		// would send them to fix a declaration that is fine.
		switch {
		case d.Precision > baseline.MaxDuckDBDecimalPrecision:
			uncastable = append(uncastable, fmt.Sprintf(
				"%s is DECIMAL(%d,%d), wider than DuckDB's %d digits",
				d.Name, d.Precision, d.Scale, baseline.MaxDuckDBDecimalPrecision))
		case d.Precision <= 0:
			uncastable = append(uncastable, fmt.Sprintf(
				"%s has no readable precision in this file's schema", d.Name))
		default:
			uncastable = append(uncastable, fmt.Sprintf(
				"%s is DECIMAL(%d,%d), which DuckDB has no type for",
				d.Name, d.Precision, d.Scale))
		}
	}
	if len(uncastable) == 0 {
		return nil
	}
	return []string{strings.Join(uncastable, "; ") + " (left as text)"}
}

// replaceClause builds the `* REPLACE (...)` list that re-types this
// table's decimal columns. Empty when there is nothing to cast, in which case
// the caller emits the plain `SELECT *` it always did.
//
// CAST, not TRY_CAST: the values were written out of a column of exactly this
// precision, so a value that will not fit means the file and the schema
// disagree. TRY_CAST would turn that into NULLs nobody would notice.
//
// Two invariants this rests on, neither of them local, so a future producer of
// an embedded CREATE TABLE has to know it must not break them:
//
//   - A column NAME cannot contain a backtick or a newline, because colRe
//     matches the backtick-delimited name within a single scanned line.
//     quoteIdent handles the double-quote case, but the per-view
//     "-- name: ..." comment line does no escaping at all, so a name carrying
//     a newline would end the comment and inject a statement into the
//     generated file.
//   - Every name in this REPLACE list must EXIST in the Parquet. DuckDB binds a
//     REPLACE list eagerly, so a name that is not there fails at CREATE VIEW
//     time and takes the whole generated script (or the console's whole panel
//     session, which execs it as one statement) with it, rather than costing
//     one view. It holds today because both producers derive the written
//     columns and the embedded CREATE TABLE from a single parse, and the
//     columns parseSchemaFrom drops (generated, MariaDB period) are dropped
//     from both halves together. Nothing here re-checks it: doing so would cost
//     another footer read per table.
//
// The same list carries the DATETIME columns of a table read under a session
// time zone (WallClockDatetimes): each becomes a zone-less TIMESTAMP holding
// the wall clock MySQL stored. The same two invariants apply to those names.
//
// And the text columns MySQL declares under a _bin collation (BinaryText):
// each is given byte comparison. Same invariants; a third one is theirs alone:
// the column must read as a VARCHAR, which holds because
// baseline.BinaryCollationColumns lists only the types the writer stores as
// Parquet strings.
func replaceClause(t BaselineTable) string {
	var parts []string
	for _, p := range replaceParts(t) {
		parts = append(parts, p.expr+" AS "+quoteIdent(p.name))
	}
	return strings.Join(parts, ", ")
}

// retyped is one column a state view reads through an expression instead of
// bare: its name, and the expression (without the alias).
type retyped struct{ name, expr string }

// replaceParts lists the columns replaceClause re-types, in its order. The
// pinned views' explicit select list (selectList) places the same expressions
// on the columns where they stand.
func replaceParts(t BaselineTable) []retyped {
	if len(t.PGCasts) > 0 {
		return pgParts(t)
	}
	if !t.SchemaKnown {
		return nil
	}
	var parts []retyped
	if t.WallClockDatetimes {
		for _, name := range t.Datetimes {
			// The file holds the wall clock labelled UTC, so reading it back
			// AT TIME ZONE 'UTC' gives that wall clock whatever the session's
			// zone is.
			parts = append(parts, retyped{name, quoteIdent(name) + " AT TIME ZONE 'UTC'"})
		}
	}
	for _, d := range t.Decimals {
		if !castableDecimal(d) {
			continue
		}
		parts = append(parts, retyped{d.Name, fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d))", quoteIdent(d.Name), d.Precision, d.Scale)})
	}
	for _, name := range t.BinaryText {
		// COLLATE C is byte comparison whatever the session's default
		// collation is, and it travels with the column: through a subquery,
		// a CTE, a function over it, GROUP BY, ORDER BY and a join.
		parts = append(parts, retyped{name, quoteIdent(name) + " COLLATE C"})
	}
	return parts
}

// pgParts are the columns of a PostgreSQL source's table that its view reads
// through an expression (#2755). The file holds each value as PostgreSQL's
// text, so a cast is all a type needs; the two that need more:
//
//   - bytea is \x and hex digits, so the digits are decoded, and a value
//     that does not start with \x (another output format) is refused: its
//     characters would decode to other bytes or to none.
//   - a date or timestamp before the common era ends in " BC", which the
//     cast is not trusted to read, so it is refused.
//   - a column left as text (json, an enum) still compares byte by byte, as
//     its text does in PostgreSQL.
//   - an array of one dimension is read as a list of its elements' type
//     (pgListExpr).
//   - a time of 24:00:00 is refused, since every surface would show it as
//     midnight; an interval loses the plus PostgreSQL writes on a positive
//     part that follows a negative one, which the cast does not take.
//   - a numeric declared without precision has no scale to cast to. It is
//     read as DECIMAL(38, PGNumericScale), and since a cast to a scale ROUNDS
//     a value with more decimals, such a value is refused first.
//
// CAST, never TRY_CAST, for the reason replaceClause gives: a value that does
// not fit must fail the statement, not turn into a NULL nobody would notice.
//
// The names come from the file's own footer and are checked against its
// column list before they reach here (baseline.ReadTableFooters), which is
// the invariant replaceClause states.
func pgParts(t BaselineTable) []retyped {
	var parts []retyped
	for _, c := range t.PGCasts {
		q := quoteIdent(c.Name)
		refuse := func(what string) string {
			return "error(" + sqlString(fmt.Sprintf("%s.%s: column %s holds %s, which the copy does not read", t.Schema, t.Table, c.Name, what)) + ")"
		}
		switch c.As {
		case "", baseline.PGAsText:
			parts = append(parts, retyped{c.Name, q + " COLLATE C"})
		case baseline.PGAsBytes:
			parts = append(parts, retyped{c.Name, fmt.Sprintf(
				`CASE WHEN %[1]s IS NULL OR starts_with(%[1]s, '\x') THEN unhex(substr(%[1]s, 3)) ELSE %[2]s END`,
				q, refuse(`bytes that are not written as \x and hex digits`))})
		case "DATE", "TIMESTAMP", "TIMESTAMPTZ":
			parts = append(parts, retyped{c.Name, fmt.Sprintf(
				"CASE WHEN ends_with(%[1]s, ' BC') THEN %[2]s ELSE CAST(%[1]s AS %[3]s) END",
				q, refuse("a date before the common era"), c.As)})
		case "TIME":
			// 24:00:00 is a time of day in PostgreSQL. The engine holds
			// it, but its Go driver hands a TIME over as a clock reading,
			// where it is 00:00:00: every surface would show midnight, with
			// no error. A TIMETZ is handed over with its offset and shows
			// 24:00:00, so it takes the plain cast.
			parts = append(parts, retyped{c.Name, fmt.Sprintf(
				"CASE WHEN starts_with(%[1]s, '24:') THEN %[2]s ELSE CAST(%[1]s AS TIME) END",
				q, refuse("the time 24:00:00"))})
		case "INTERVAL":
			// Under IntervalStyle postgres a positive part after a negative
			// one is written with a plus (-1 years +3 days), which the
			// engine's cast refuses; a plus appears nowhere else in the text.
			parts = append(parts, retyped{c.Name, "CAST(replace(" + q + ", '+', '') AS INTERVAL)"})
		case baseline.PGAsNumeric:
			msg := fmt.Sprintf("%s.%s: column %s holds a numeric with more than %d decimals, which the copy does not read exactly",
				t.Schema, t.Table, c.Name, baseline.PGNumericScale)
			parts = append(parts, retyped{c.Name, fmt.Sprintf(
				"CASE WHEN strpos(%[1]s, '.') > 0 AND length(%[1]s) - strpos(%[1]s, '.') > %[2]d THEN error(%[3]s) ELSE CAST(%[1]s AS DECIMAL(%[4]d,%[2]d)) END",
				q, baseline.PGNumericScale, sqlString(msg), baseline.MaxDuckDBDecimalPrecision)})
		default:
			if elem, ok := baseline.PGListElement(c.As); ok {
				parts = append(parts, retyped{c.Name, pgListExpr(q, elem, refuse)})
				continue
			}
			parts = append(parts, retyped{c.Name, "CAST(" + q + " AS " + c.As + ")"})
		}
	}
	return parts
}

// pgListElements reads the text of a PostgreSQL array of one dimension,
// with its outer braces taken off, as a list of its elements' text (#2787).
// PostgreSQL writes an element bare, or between double quotes with \ before
// a quote or a backslash; it quotes every element that holds a comma, a
// brace, a quote, a backslash or a blank, is empty, or spells NULL. So a
// bare NULL is the missing value and nothing else is: the test is on the
// element's bytes (hex), which no collation of the session folds.
const pgListElements = `list_transform(regexp_extract_all(%[1]s[2:-2], '(?s)"(?:[^"\\]|\\.)*"|[^,]+'), ` +
	`lambda x: CASE WHEN hex(x) = '4E554C4C' THEN NULL WHEN x[1:1] = '"' THEN regexp_replace(x[2:-2], '(?s)\\(.)', '\1', 'g') ELSE x END)`

// pgListExpr is the expression that reads column q, the text of a
// PostgreSQL array, as a list of elem. What a list of one dimension cannot
// hold fails the statement that reads it: an array of more dimensions
// ({{1,2},{3,4}}), one written with bounds of its own ([0:1]={1,2}), text
// that does not end where an array ends (a value cut short), and,
// for dates, an element before the common era, whose " BC" the engine's
// cast drops without a word.
func pgListExpr(q, elem string, refuse func(string) string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CASE WHEN %[1]s IS NULL THEN NULL WHEN NOT starts_with(%[1]s, '{') OR NOT ends_with(%[1]s, '}') OR starts_with(%[1]s, '{{') THEN %[2]s",
		q, refuse("an array of more than one dimension, with bounds of its own, or text that is not an array's"))
	switch elem {
	case "DATE", "TIMESTAMP", "TIMESTAMPTZ":
		fmt.Fprintf(&b, " WHEN contains(%s, ' BC') THEN %s", q, refuse("a date before the common era"))
	}
	fmt.Fprintf(&b, " ELSE CAST("+pgListElements+" AS %[2]s[]) END", q, elem)
	return b.String()
}

// castableDecimal reports whether DuckDB has a DECIMAL that can hold this
// column. MySQL allows 65 digits and DuckDB stops at 38, and a scale wider than
// the precision is not a DuckDB type either. Both stay text rather than being
// cast to something that would drop digits.
func castableDecimal(d DecimalColumn) bool {
	return d.Precision > 0 &&
		d.Precision <= baseline.MaxDuckDBDecimalPrecision &&
		d.Scale >= 0 && d.Scale <= d.Precision
}

// TextColumn and its kinds are the baseline package's (#2533), named here for
// the callers that read BaselineTable.Text, as DecimalColumn is.
type TextColumn = baseline.TextColumn

// TextKind is baseline.TextKind.
type TextKind = baseline.TextKind

const (
	TextFolded     = baseline.TextFolded
	TextBytes      = baseline.TextBytes
	TextBytesASCII = baseline.TextBytesASCII
	TextUnlike     = baseline.TextUnlike
)
