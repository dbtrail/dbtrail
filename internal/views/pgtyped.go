package views

import (
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A PostgreSQL source's snapshot file may store some columns as their type
// (#3315, baseline.MetaKeyPGTypedColumns). A pinned view reads such a file's
// stored columns as they are (pgParts leaves them out), and the rows that
// come from beside the file, which hold every column as text, through the
// casts the file's text would have taken (pgTextParts): the rows of a table
// delta's .pgnew files and of a tail. So every side of a union has one type
// per column, the one the view has always read the column as.

// pgNewReplace is the REPLACE list that reads the text of t's stored columns
// as their types, or "" when t's file stores none.
func pgNewReplace(t BaselineTable) string {
	parts := pgTextParts(t)
	items := make([]string, len(parts))
	for i, p := range parts {
		items[i] = p.expr + " AS " + quoteIdent(p.name)
	}
	return strings.Join(items, ", ")
}

// pgTypedTailBody is tailStateBody for a PostgreSQL table whose file stores
// columns typed and has no pair beside it.
func pgTypedTailBody(t BaselineTable) string {
	published := "SELECT * FROM read_parquet(" + sqlString(fileGlob(t.Path)) + ")"
	return ordered(t, func(replace string) string {
		segs := make([]string, len(t.Tail.Paths))
		for i, p := range t.Tail.Paths {
			segs[i] = sqlString(fileGlob(p))
		}
		return pgTailChainStateSQL(published, segs, t.Tail.PK, replace, pgNewReplace(t))
	})
}

// pgTailChainStateSQL is baseline.TableTailChainStateSQL with segReplace
// applied to each segment's rows: the REPLACE list that reads a stored
// column's text as the type the published state holds it in (pgNewReplace).
// Empty, it is TableTailChainStateSQL to the byte. It is that function's SQL
// written again for the PostgreSQL views, so the MySQL views' is not touched;
// a key's row is still the one in the newest segment that carries it.
func pgTailChainStateSQL(state string, segs []string, pk []string, replace, segReplace string) string {
	if segReplace == "" || len(segs) == 0 {
		return baseline.TableTailChainStateSQL(state, segs, pk, replace)
	}
	star := "*"
	if replace != "" {
		star = "* REPLACE (" + replace + ")"
	}
	keys := make([]string, len(pk))
	for i, c := range pk {
		keys[i] = quoteIdent(c)
	}
	using := strings.Join(keys, ", ")
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s FROM (SELECT bintrail_state.* FROM (%s) bintrail_state ANTI JOIN (SELECT %s FROM read_parquet([%s])) bintrail_tail USING (%s)",
		star, state, using, strings.Join(segs, ", "), using)
	for i, seg := range segs {
		fmt.Fprintf(&b, " UNION ALL BY NAME SELECT * REPLACE (%s) FROM (SELECT bintrail_seg.* EXCLUDE (\"%s\", \"%s\") FROM read_parquet(%s) bintrail_seg",
			segReplace, baseline.TableDeltaPKColumn, baseline.TableDeltaOpColumn, seg)
		if i < len(segs)-1 {
			fmt.Fprintf(&b, " ANTI JOIN (SELECT %s FROM read_parquet([%s])) bintrail_newer USING (%s)",
				using, strings.Join(segs[i+1:], ", "), using)
		}
		fmt.Fprintf(&b, " WHERE bintrail_seg.\"%s\" = '%s')", baseline.TableDeltaOpColumn, baseline.TableDeltaOpUpsert)
	}
	b.WriteString(")")
	return b.String()
}

// pgAnyStored says whether a table of in has a file that stores columns
// typed.
func pgAnyStored(in Input) bool {
	for _, t := range in.Baselines {
		if len(t.PGCasts) > 0 && len(t.PGStored) > 0 {
			return true
		}
	}
	return false
}
