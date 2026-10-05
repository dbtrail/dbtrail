package baseline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
)

// Column order (#2111).
//
// The writer stores a table's columns sorted by name (sortColumnsForParquet:
// the Parquet schema is built from a map), so every snapshot file ever written
// answers `SELECT *` in alphabetical order, where MySQL answers in the order
// the table declares. The declared order survives in one place, the CREATE
// TABLE in the file's footer, and the state views read it from there to list
// the columns explicitly.
//
// Two things are read, and kept apart:
//
//   - The ORDER (TableFooter.Columns), which is only used when the columns the
//     CREATE TABLE lists are exactly the columns the file holds. A view that
//     names a column the file lacks does not bind, and one that leaves a
//     column out hides it; so when the two sets differ the order is unknown,
//     and it is never guessed.
//   - Whether MySQL's `SELECT *` returns a different SET of columns from the
//     one the file holds (TableFooter.StarDiffers), which no ordering fixes.

// starDifference reads an embedded CREATE TABLE for the columns MySQL's
// `SELECT *` treats differently from a snapshot file, and says so in words, or
// "" when there are none:
//
//   - A generated column (STORED or VIRTUAL) and a MariaDB system-versioning
//     period column are returned by MySQL and are not in the file, because a
//     dump does not carry their values (generatedColumnLine).
//   - An INVISIBLE column (MySQL 8.0.23+, MariaDB 10.3+) is in the file and is
//     not returned by MySQL's `SELECT *`.
//   - A column definition the parser cannot read (colRe: a name holding a
//     backtick) is in neither the parsed list nor, for a file written from
//     that list, the file.
//
// A generated column that is also INVISIBLE is none of these: MySQL leaves it
// out and so does the file.
//
// The scan stops where parseSchemaFrom stops, so both read the same lines.
func starDifference(createSQL string) string {
	sc := scanColumnLines(createSQL)
	var parts []string
	if len(sc.generated) > 0 {
		parts = append(parts, "MySQL also returns "+columnsPhrase("generated column", sc.generated)+", which a snapshot does not hold")
	}
	if len(sc.invisible) > 0 {
		parts = append(parts, "MySQL leaves out "+columnsPhrase("invisible column", sc.invisible))
	}
	if sc.unreadable > 0 {
		parts = append(parts, fmt.Sprintf("%d column definition(s) could not be read", sc.unreadable))
	}
	return strings.Join(parts, "; ")
}

// columnScan is what an embedded CREATE TABLE says about the columns a
// snapshot file and MySQL treat differently, each list in declared order.
type columnScan struct {
	// generated: a dump carries no value for them, so the file does not hold
	// them; MySQL's `SELECT *` returns them.
	generated []string
	// hiddenGenerated: generated and INVISIBLE. Neither the file nor MySQL's
	// `SELECT *` has them, and a statement can still name them.
	hiddenGenerated []string
	// invisible: the file holds them, MySQL's `SELECT *` leaves them out.
	invisible []string
	// notHeld is generated and hiddenGenerated together, in declared order.
	notHeld []string
	// unreadable counts the column definitions colRe cannot read.
	unreadable int
}

// scanColumnLines reads a CREATE TABLE's column lines. The scan stops where
// parseSchemaFrom stops, so both read the same lines.
func scanColumnLines(createSQL string) columnScan {
	var sc columnScan
	for _, line := range strings.Split(createSQL, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "PRIMARY") ||
			strings.HasPrefix(trimmed, "UNIQUE") ||
			strings.HasPrefix(trimmed, "KEY") ||
			strings.HasPrefix(trimmed, "CONSTRAINT") ||
			trimmed == ");" || trimmed == ")" {
			break
		}
		if !strings.HasPrefix(trimmed, "`") {
			continue
		}
		loc := colRe.FindStringSubmatchIndex(line)
		if loc == nil {
			sc.unreadable++
			continue
		}
		name := line[loc[2]:loc[3]]
		hidden := false
		for _, w := range topLevelWords(line[loc[4]:]) {
			if w == "INVISIBLE" {
				hidden = true
			}
		}
		isGenerated := generatedColumnLine(line)
		if isGenerated {
			sc.notHeld = append(sc.notHeld, name)
		}
		switch {
		case isGenerated && !hidden:
			sc.generated = append(sc.generated, name)
		case isGenerated:
			sc.hiddenGenerated = append(sc.hiddenGenerated, name)
		case hidden:
			sc.invisible = append(sc.invisible, name)
		}
	}
	return sc
}

// systemVersioningRe is the table option of a MariaDB system-versioned table,
// looked for outside quoted strings.
var systemVersioningRe = regexp.MustCompile(`(?i)\bWITH\s+SYSTEM\s+VERSIONING\b`)

// columnsNotHeld names the columns a statement on MySQL can NAME and a
// snapshot file does not hold (#2123): every generated column, the INVISIBLE
// ones included (MySQL's `SELECT *` leaves those out, a statement that names
// one still reads it), and the row_start and row_end a MariaDB table versioned
// without declaring its period columns answers to, which its CREATE TABLE
// does not list.
//
// unread says the list is not the whole of them: a column definition the
// parser could not read (a name holding a backtick) may be one, and its name
// is not known. A caller that needs the whole list treats the table as one
// whose columns are not known.
//
// A statement that names one of these on the copy does not always fail: the
// name can bind to another table's column, to an alias, or to a function
// DuckDB calls without parentheses, and the copy then answers with other
// rows. Read routing keeps such a statement on the source.
func columnsNotHeld(createSQL string) (names []string, unread bool) {
	sc := scanColumnLines(createSQL)
	names = sc.notHeld
	if systemVersioningRe.MatchString(emptyQuoted(createSQL)) {
		for _, implicit := range []string{"row_start", "row_end"} {
			if !slices.Contains(names, implicit) {
				names = append(names, implicit)
			}
		}
	}
	return names, sc.unreadable > 0
}

// columnsPhrase is "generated column total" or "generated columns a, b".
func columnsPhrase(what string, names []string) string {
	if len(names) > 1 {
		what += "s"
	}
	return what + " " + strings.Join(names, ", ")
}

// columnNames lists a parsed schema's columns, in the order declared.
func columnNames(cols []Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

// fileColumnsQuery lists the columns each file holds. parquet_schema reads
// footers only; the row with children is the schema's root, not a column.
func fileColumnsQuery(paths []string) string {
	return "SELECT file_name, name FROM parquet_schema(" + fileListLiteral(paths) + ") WHERE coalesce(num_children, 0) = 0"
}

// confirmColumnOrder settles TableFooter.Columns for every footer read: the
// declared order is kept only for a file that holds exactly the declared
// columns. It runs in the footer read's own session, as one more footer-only
// query over the files whose CREATE TABLE parsed, with the same per-file
// fallback (decimalColumnsPerFile says why one bad file fails a batch).
//
// A file whose column list could not be read keeps its casts and loses its
// order, and is reported in orderUnread so a caller that caches asks again.
func confirmColumnOrder(ctx context.Context, db *sql.DB, st *footerScan) {
	paths := make([]string, 0, len(st.footers))
	for p := range st.footers {
		paths = append(paths, p)
	}
	if len(paths) == 0 {
		return
	}
	held := map[string]map[string]bool{}
	read := map[string]bool{}
	collect := func(rows *sql.Rows, asked []string) bool {
		defer rows.Close()
		got := map[string]map[string]bool{}
		for rows.Next() {
			var file, name string
			if err := rows.Scan(&file, &name); err != nil {
				slog.Debug("baseline: could not scan a Parquet column name", "error", err)
				return false
			}
			if got[file] == nil {
				got[file] = map[string]bool{}
			}
			got[file][name] = true
		}
		if err := rows.Err(); err != nil {
			slog.Debug("baseline: reading Parquet column names ended early", "error", err)
			return false
		}
		// Only a read that ran to its end counts, for every file asked: a
		// partial list would compare unequal for a reason that is not the
		// file's.
		// A file the read returned nothing for was not read: DuckDB reports
		// each file under the path it was asked with, and a file with no
		// column is not one the writer produces.
		for _, p := range asked {
			if got[p] != nil {
				held[p] = got[p]
				read[p] = true
			}
		}
		return true
	}
	ok := false
	rows, err := db.QueryContext(ctx, fileColumnsQuery(paths))
	if err == nil {
		ok = collect(rows, paths)
	} else {
		// Kept for the caller's one warning: when every file fails the cause
		// is usually not any one file, and this is the error that says why.
		st.orderErr = err
	}
	if !ok {
		for _, p := range paths {
			rows, err := db.QueryContext(ctx, fileColumnsQuery([]string{p}))
			if err != nil {
				st.orderErr = err
				slog.Debug("baseline: could not read a Parquet file's column names", "path", p, "error", err)
				continue
			}
			collect(rows, []string{p})
		}
	}
	st.settleColumnOrder(held, read)
}

// settleColumnOrder is confirmColumnOrder's decision, apart from its reads:
// held is each file's own column names and read says whether that list was
// read to its end. A footer keeps its declared order only when its file was
// read and holds exactly those columns.
func (st *footerScan) settleColumnOrder(held map[string]map[string]bool, read map[string]bool) {
	for p, f := range st.footers {
		switch {
		case !read[p]:
			st.orderUnread = append(st.orderUnread, p)
			f.Columns = nil
		case !sameColumnSet(f.Columns, held[p]):
			slog.Warn("baseline: a snapshot file does not hold exactly the columns its CREATE TABLE lists, "+
				"so the table's column order is not known; its state view returns SELECT * in alphabetical order",
				"path", p, "declared_columns", len(f.Columns), "file_columns", len(held[p]))
			f.Columns = nil
		}
		st.footers[p] = f
	}
}

// sameColumnSet reports whether declared names exactly the columns in held,
// each once. Names compare byte for byte: a difference in case is a
// difference, and the order then stays unknown.
func sameColumnSet(declared []string, held map[string]bool) bool {
	if len(declared) != len(held) {
		return false
	}
	seen := make(map[string]bool, len(declared))
	for _, name := range declared {
		if !held[name] || seen[name] {
			return false
		}
		seen[name] = true
	}
	return true
}
