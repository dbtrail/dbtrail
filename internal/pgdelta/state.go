package pgdelta

import (
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// StateSQL is a SELECT of a table's rows as its base file and the chain
// beside it leave them: the base's rows whose row number no .pgdead file
// names, plus the newest version of every key in the .pgnew files that is
// not a deletion. base, dead and rows are file paths (or s3:// URLs); the
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
func StateSQL(base string, dead, rows []string) string {
	if len(dead) == 0 && len(rows) == 0 {
		return "SELECT * FROM read_parquet(" + quote(base) + ")"
	}
	var b strings.Builder
	b.WriteString("SELECT bintrail_b.* EXCLUDE (file_row_number) FROM read_parquet(" + quote(base) + ", file_row_number=true) AS bintrail_b")
	if len(dead) > 0 {
		b.WriteString(" WHERE NOT EXISTS (SELECT 1 FROM read_parquet(" + list(dead) + ") AS bintrail_d" +
			` WHERE bintrail_d."` + baseline.TableDeltaPosColumn + `" = bintrail_b.file_row_number)`)
	}
	if len(rows) > 0 {
		src := "read_parquet(" + list(rows) + ", union_by_name=true)"
		b.WriteString(" UNION ALL BY NAME SELECT bintrail_n.* EXCLUDE (" + KeyColumn + ", " + OpColumn + ", " + SeqColumn + ")" +
			" FROM " + src + " AS bintrail_n" +
			" JOIN (SELECT encode(" + KeyColumn + ") AS bintrail_k, max(" + SeqColumn + ") AS bintrail_s FROM " + src + " GROUP BY 1) AS bintrail_m" +
			" ON encode(bintrail_n." + KeyColumn + ") = bintrail_m.bintrail_k AND bintrail_n." + SeqColumn + " = bintrail_m.bintrail_s" +
			" WHERE bintrail_n." + OpColumn + " = '" + OpPut + "'")
	}
	return b.String()
}

// quote is s as a SQL string literal.
func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// list is paths as a SQL list of string literals.
func list(paths []string) string {
	q := make([]string, len(paths))
	for i, p := range paths {
		q[i] = quote(p)
	}
	return "[" + strings.Join(q, ", ") + "]"
}
