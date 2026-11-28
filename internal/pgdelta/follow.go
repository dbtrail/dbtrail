package pgdelta

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// FollowGlobs returns the two patterns FollowStateSQL reads a table's chain
// through: one for its .pgdead files, one for its .pgnew files, each widened
// to ALSO match the table's own .parquet file. It is the PostgreSQL
// counterpart of baseline.TableDeltaFollowGlobs, for a view that follows
// later snapshots and so cannot list the table's pairs when it is written.
//
// A following view is read across many snapshots, and a table can have a
// chain in one and none in the next (a full snapshot rewrites it bare).
// DuckDB refuses a glob that matches nothing, so each pattern also matches a
// file that is always there, the table's own, and the name filter of
// FollowStateSQL drops that file's rows again.
//
// The table file's name is taken literally (a table named "or*ders" does not
// read the files of "orders"). After it and its dot, six classes spell
// "parque" or six digits (a link's sequence, written with seqWidth digits),
// and the last class is the base's final "t" or the suffix's last letter. So
// a pattern admits the base, every link of the chain (one or many, as Paths
// names them), and nothing else a snapshot holds beside them: not the other
// half of a pair, not a MySQL delta file (".posdel", ".upserts"), not a file
// still being written (IsTemp: its name ends in the random digits
// os.CreateTemp puts after ".tmp-", which the last class does not take).
func FollowGlobs(basePath string) (dead, rows string) {
	head := literal(strings.TrimSuffix(basePath, baseSuffix)) + "."
	for _, c := range "parque" {
		head += "[0-9" + string(c) + "]"
	}
	head += "*"
	last := func(suffix string) string { return "[t" + suffix[len(suffix)-1:] + "]" }
	return head + last(DeadSuffix), head + last(NewSuffix)
}

// followNameFilter is the predicate that keeps, out of what a FollowGlobs
// pattern matched, only the files of the table at basePath's chain that end
// in suffix: "<table>.<six digits><suffix>", anchored at the start of the
// path or after a "/" (a relative glob run inside the folder gives a bare
// name) and at its end. The table's name is quoted, so a table named
// "or.ders" does not take the files of "orsders".
//
// It is what keeps a sibling table out: "orders.000003" is a table name of
// its own, and its file and its pairs match the pattern of "orders" but not
// this filter, whose sequence must be followed by the suffix itself.
func followNameFilter(basePath, suffix string) string {
	stem := strings.TrimSuffix(basePath, baseSuffix)
	stem = stem[strings.LastIndexByte(stem, '/')+1:]
	re := `(^|/)` + regexp.QuoteMeta(stem) + `\.` + fmt.Sprintf("[0-9]{%d}", seqWidth) + regexp.QuoteMeta(suffix) + "$"
	return "regexp_matches(filename, " + quote(re) + ")"
}

// FollowStateSQL is StateSQL for a view that follows later snapshots: the
// table as its file and every link of the chain beside it leave it, read
// through the patterns of FollowGlobs instead of a list of files, so that it
// is right for a snapshot with no chain, one link or many. It is the
// PostgreSQL counterpart of baseline.TableDeltaFollowStateSQL, and does what
// that does the way that does it.
//
// base, deadGlob and rowsGlob are SQL expressions: for the table file's path
// (as a glob that matches it alone) and for the two patterns (or whatever the
// caller's following mode builds around them: a session variable in front, a
// list taken from one listing of the snapshot). basePath is the table file's
// path as a plain string, for the name filter, which is what narrows each
// wider pattern back to the chain.
//
// The rows are StateSQL's over the same files: the base's rows whose row
// number no .pgdead file names, plus the newest version (the highest
// SeqColumn) of every key in the .pgnew files that is not a deletion, keys
// compared as their bytes.
//
// Two things make it hold when the table has no chain. The base is then the
// only file a pattern matches, so the pair's columns do not exist in what the
// reads return: each read is padded with an empty relation that declares
// them, so the SQL binds and the chain adds no rows. And the base's own
// columns reach the .pgdead read: the dead row number is cast back to BIGINT,
// what the base's row numbers are compared as, and the cast only sees .pgdead
// rows, because the name filter runs first.
//
// The .pgdead read does not use union_by_name, as the MySQL one does not: it
// takes its columns from the first file in name order, and a link's digits
// sort before the base's "parquet", so that is a .pgdead whenever the chain
// has one. A sibling table named "<table>.<six digits>" or "<table>.parque..."
// is matched by the patterns too: its columns join the .pgnew read, and one
// named "<table>.000000" sorts before the chain's first .pgdead, so the dead
// rows would read as none, without an error. A caller must not use this SQL
// for a table with such a sibling, as the MySQL views keep a table with one
// off theirs (chainReady); the residual is the same as MySQL's, a sibling
// created after the views were written.
//
// Like the MySQL following views, it does not check at read time that the
// chain is whole: what the patterns find is what is read.
//
// The base's columns are read as PostgreSQL's text (baseline.PGAllTextSQL),
// as the .pgnew files hold them: a following view is read across snapshots
// whose files may store some columns as their type (#3315) and may not, and
// the text of a stored column is the text it was written from, so the view
// reads every snapshot alike. So are the .pgnew rows: the pattern they are
// read through matches the base too, and a read by name takes some types
// from it (a uuid's text is read as UUID).
func FollowStateSQL(base, deadGlob, rowsGlob, basePath string) string {
	pos := `"` + baseline.TableDeltaPosColumn + `"`
	dead := "SELECT CAST(" + pos + " AS BIGINT) AS " + pos + " FROM (SELECT * FROM read_parquet(" + deadGlob + ", filename=true" + noHive + ")" +
		" WHERE " + followNameFilter(basePath, DeadSuffix) +
		" UNION ALL BY NAME SELECT NULL::BIGINT AS " + pos + " WHERE false)"
	src := "(SELECT " + baseline.PGAllTextSQL("") + " FROM read_parquet(" + rowsGlob + ", filename=true, union_by_name=true" + noHive + ")" +
		" WHERE " + followNameFilter(basePath, NewSuffix) +
		" UNION ALL BY NAME SELECT NULL::VARCHAR AS " + KeyColumn + ", NULL::VARCHAR AS " + OpColumn + ", NULL::VARCHAR AS " + SeqColumn + " WHERE false)"
	return "SELECT " + baseline.PGAllTextSQL("bintrail_b", "file_row_number") + " FROM read_parquet(" + base + ", file_row_number=true" + noHive + ") AS bintrail_b" +
		" WHERE NOT EXISTS (SELECT 1 FROM (" + dead + ") AS bintrail_d WHERE bintrail_d." + pos + " = bintrail_b.file_row_number)" +
		" UNION ALL BY NAME SELECT bintrail_n.* EXCLUDE (" + KeyColumn + ", " + OpColumn + ", " + SeqColumn + ", filename)" +
		" FROM " + src + " AS bintrail_n" +
		" JOIN (SELECT encode(" + KeyColumn + ") AS bintrail_k, max(" + SeqColumn + ") AS bintrail_s FROM " + src + " GROUP BY 1) AS bintrail_m" +
		" ON encode(bintrail_n." + KeyColumn + ") = bintrail_m.bintrail_k AND bintrail_n." + SeqColumn + " = bintrail_m.bintrail_s" +
		" WHERE bintrail_n." + OpColumn + " = '" + OpPut + "'"
}
