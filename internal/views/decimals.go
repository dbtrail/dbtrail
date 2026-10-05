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
		in.Baselines[i].Decimals = f.Decimals
		in.Baselines[i].Datetimes = f.Datetimes
		in.Baselines[i].BinaryText = f.BinaryText
		in.Baselines[i].DeltaReserved = f.DeltaReserved
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
			b.WriteString("-- decimal column in them reads as text; and no collations, so a text column\n")
			b.WriteString("-- MySQL declares _bin compares like any other, by the session's default\n")
			b.WriteString("-- collation. Those tables are named below. A\n")
		}
		b.WriteString("-- baseline older than this feature gains the casts when it is next taken or\n")
		b.WriteString("-- refreshed; a PostgreSQL-source baseline stores all its values as text and\n")
		b.WriteString("-- will not gain them. If a footer could not be read at all, the bintrail log\n")
		b.WriteString("-- has the error.\n")
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
	if !t.SchemaKnown && pinned {
		return []string{"this file carries no column types, so nothing is cast; " +
			"decimal columns read as text and _bin columns compare by the session's collation"}
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
	if !t.SchemaKnown {
		return ""
	}
	var parts []string
	if t.WallClockDatetimes {
		for _, name := range t.Datetimes {
			// The file holds the wall clock labelled UTC, so reading it back
			// AT TIME ZONE 'UTC' gives that wall clock whatever the session's
			// zone is.
			parts = append(parts, fmt.Sprintf("%s AT TIME ZONE 'UTC' AS %s", quoteIdent(name), quoteIdent(name)))
		}
	}
	for _, d := range t.Decimals {
		if !castableDecimal(d) {
			continue
		}
		parts = append(parts, fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d)) AS %s",
			quoteIdent(d.Name), d.Precision, d.Scale, quoteIdent(d.Name)))
	}
	for _, name := range t.BinaryText {
		// COLLATE C is byte comparison whatever the session's default
		// collation is, and it travels with the column: through a subquery,
		// a CTE, a function over it, GROUP BY, ORDER BY and a join.
		parts = append(parts, fmt.Sprintf("%s COLLATE C AS %s", quoteIdent(name), quoteIdent(name)))
	}
	return strings.Join(parts, ", ")
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
