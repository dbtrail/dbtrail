package pgdelta

import (
	"path"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// StateSQL is a SELECT of a table's rows as its base file and the chain
// beside it leave them. Each path names one file, whatever characters it
// holds (file): give them as they are, not made into globs. It is the
// base's rows whose row number no .pgdead file names, plus the newest
// version of every key in the .pgnew files that is not a deletion. base, dead and rows are file paths (or s3:// URLs); the
// two lists are the chain's, one entry per pair. With no pair it is the
// base's rows.
//
// The columns are the base file's, in its order, and nothing else: the
// pair's own columns and the row number the read adds are left out.
//
// Newest is the pair's sequence, which every row of a .pgnew file carries
// (SeqColumn, six digits, so its text orders as the number does). Keys are
// compared as their bytes (encode), whatever collation the session has: two
// keys that differ only in case, or in an accent, are two rows.
//
// Every read turns Hive partitioning off: a folder named "env=prod" above
// the files would otherwise add a column "env" to the table. A file listed
// twice under one spelling of its path, or two that clean to the same one,
// is read once. The same file under two paths that differ otherwise (a
// linked folder) is two files to this function, and each of its keys would
// be there twice: a chain's files are named from one listing of one folder.
func StateSQL(base string, dead, rows []string) string {
	return StateSQLTyped(base, dead, rows, "")
}

// StateSQLTyped is StateSQL for a base file that stores some columns as
// their type (#3315; baseline.MetaKeyPGTypedColumns): newReplace is a
// REPLACE list, applied to the rows of the .pgnew files, that reads those
// columns' text as the types the base holds them in, so the two sides of the
// union have one type per column. Empty, it is StateSQL to the byte.
func StateSQLTyped(base string, dead, rows []string, newReplace string) string {
	dead, rows = unique(dead), unique(rows)
	if len(dead) == 0 && len(rows) == 0 {
		return "SELECT * FROM read_parquet(" + file(base) + noHive + ")"
	}
	var b strings.Builder
	b.WriteString("SELECT bintrail_b.* EXCLUDE (file_row_number) FROM read_parquet(" + file(base) + ", file_row_number=true" + noHive + ") AS bintrail_b")
	if len(dead) > 0 {
		b.WriteString(" WHERE NOT EXISTS (SELECT 1 FROM read_parquet(" + list(dead) + noHive + ") AS bintrail_d" +
			` WHERE bintrail_d."` + baseline.TableDeltaPosColumn + `" = bintrail_b.file_row_number)`)
	}
	if len(rows) > 0 {
		src := "read_parquet(" + list(rows) + ", union_by_name=true" + noHive + ")"
		newest := "SELECT bintrail_n.* EXCLUDE (" + KeyColumn + ", " + OpColumn + ", " + SeqColumn + ")" +
			" FROM " + src + " AS bintrail_n" +
			" JOIN (SELECT encode(" + KeyColumn + ") AS bintrail_k, max(" + SeqColumn + ") AS bintrail_s FROM " + src + " GROUP BY 1) AS bintrail_m" +
			" ON encode(bintrail_n." + KeyColumn + ") = bintrail_m.bintrail_k AND bintrail_n." + SeqColumn + " = bintrail_m.bintrail_s" +
			" WHERE bintrail_n." + OpColumn + " = '" + OpPut + "'"
		if newReplace != "" {
			newest = "SELECT * REPLACE (" + newReplace + ") FROM (" + newest + ")"
		}
		b.WriteString(" UNION ALL BY NAME " + newest)
	}
	return b.String()
}

// TextStateSQL is StateSQL with every column of the base read as
// PostgreSQL's text (baseline.PGAllTextSQL), as the .pgnew files hold them:
// for a fold that writes the table again as text, whether its base stores
// some columns typed (#3315) or not. Over a base of text it reads what
// StateSQL reads.
func TextStateSQL(base string, dead, rows []string) string {
	dead, rows = unique(dead), unique(rows)
	if len(dead) == 0 && len(rows) == 0 {
		return "SELECT " + baseline.PGAllTextSQL("") + " FROM read_parquet(" + file(base) + noHive + ")"
	}
	s := StateSQL(base, dead, rows)
	const from = "SELECT bintrail_b.* EXCLUDE (file_row_number) FROM "
	return "SELECT " + baseline.PGAllTextSQL("bintrail_b", "file_row_number") + " FROM " + strings.TrimPrefix(s, from)
}

// noHive is the read_parquet option that keeps folder names out of the
// columns.
const noHive = ", hive_partitioning=false"

// unique is paths with each one once, in its first place. A local path is
// compared cleaned ("dir/./f" is "dir/f"); a URL as it is written.
func unique(paths []string) []string {
	seen := make(map[string]bool, len(paths))
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		k := p
		if !strings.Contains(p, "://") {
			k = path.Clean(p)
		}
		if !seen[k] {
			seen[k] = true
			out = append(out, p)
		}
	}
	return out
}

// quote is s as a SQL string literal.
func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// file is path as the engine is handed a file to read: a SQL string literal
// of the path made to match itself and nothing else. The engine takes every
// path as a glob, so a table named "or*ders" would be read with the file of
// "orders" beside it, and the row numbers of one file taken for the other's.
// Each pattern character is wrapped in a class of its own, which is how the
// engine takes one literally (a backslash does not escape).
func file(path string) string { return quote(literal(path)) }

// literal is path as a glob that matches itself alone.
// GlobLiteral is path as a glob that matches it alone: for whoever builds a
// glob of its own around a path (a folder, then a pattern).
func GlobLiteral(path string) string { return literal(path) }

func literal(path string) string {
	var b strings.Builder
	for _, c := range path {
		switch c {
		case '[', '*', '?', '{':
			b.WriteByte('[')
			b.WriteRune(c)
			b.WriteByte(']')
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}

// list is paths as a SQL list of string literals.
func list(paths []string) string {
	q := make([]string, len(paths))
	for i, p := range paths {
		q[i] = file(p)
	}
	return "[" + strings.Join(q, ", ") + "]"
}
