package pgshim

import "strings"

// A column the copy does not hold as PostgreSQL does (#3233).
//
// PostgreSQL compares a domain over char(n) without its padding, an inet or a
// jsonb by value, a range by its bounds; the copy holds them as text, or bytes
// or a plain number, and compares what is written: WHERE dc = 'a' finds no row
// where PostgreSQL finds nine. A statement that compares, groups, joins,
// sorts, aggregates or hands to a function such a column is the source's, and
// is refused where there is no source. Merely showing one, as a whole item of
// a plain select list or through its *, stays on the copy: both sides write
// the same value. A table that holds such a column, in a statement that does
// not read it, stays on the copy too.

// The keys under which the screen's types hold what this rule reads. No name
// a statement writes starts with them.
const (
	userTypeKey    = "\x01"
	userTypeColKey = "\x01c"
	userTypeRelKey = "\x01t"
)

// userTypeColumn is the set of columns this rule covers: a type made after
// initdb (a domain, an enum, a composite, a range of the user's, an
// extension's), one the copy holds as text and the source does not, and two it
// holds in another form that compares otherwise. A padded column has rules of
// its own, and so does one whose type the snapshot does not record.
func userTypeColumn(c CatalogColumn) bool {
	if sourceMark(c) != "" {
		return false
	}
	switch c.SourceType {
	case oidTextArray, oidVarcharArray, oidNameArray:
		// Arrays of text: the copy holds and compares their elements as
		// PostgreSQL's text.
		return false
	}
	return c.SourceType >= firstUserOID || onlyTextOnCopy(c) || c.SourceType == oidBytea || c.SourceType == oidOID
}

// markUserType notes in the screen's types a column of a type not built in,
// of a table the statement reads.
func markUserType(out map[string][]string, table, column string) {
	out[userTypeKey] = []string{relType}
	out[userTypeColKey+column] = []string{relType}
	out[userTypeRelKey+strings.ToLower(table)] = []string{relType}
}

// userTypeView is the screen's types with only the columns of a type not built
// in marked, as paddedRead and paddedExempt read a padded column, and only
// their tables as tables that hold one.
func userTypeView(types map[string][]string) map[string][]string {
	view := map[string][]string{}
	for k, v := range types {
		switch {
		case strings.HasPrefix(k, userTypeColKey):
			view[strings.TrimPrefix(k, userTypeColKey)] = []string{bpcharType}
		case strings.HasPrefix(k, userTypeRelKey):
			view[relKey+strings.TrimPrefix(k, userTypeRelKey)] = []string{relType}
		case k != starUnprovenKey && strings.HasPrefix(k, listedKey):
			view[k] = v
		}
	}
	return view
}

// userTypeUse says what in a statement uses such a column where the copy would
// answer otherwise, "" when nothing does. A padded column's rule (paddedRead)
// is this one's: a whole item of a plain select list, or a star there, is
// shown; anywhere else the column is used. A star that does not bring one is
// taken out first, so that it does not count as a use.
func userTypeUse(toks []rtToken, types map[string][]string) string {
	if _, any := types[userTypeKey]; !any {
		return ""
	}
	const why = ", of a type the copy does not compare as PostgreSQL does"
	view := userTypeView(types)
	if what := userTypeReachedUnnamed(toks, view); what != "" {
		return what + why
	}
	exempt := paddedExempt(toks, view)
	rest := append([]rtToken(nil), toks...)
	shown, stars := false, 0
	for i, t := range toks {
		switch {
		case t.kind == rtOp && t.text == "*" && listStar(toks, i):
			// Each star reads its own FROM: past a few, none is taken out,
			// so that what the rule costs stays bounded.
			stars++
			if exempt[i] || stars <= userTypeStarsRead && !starMayBringUserType(toks, i, view) {
				rest[i] = rtToken{kind: rtNumber, text: "0"}
				continue
			}
			shown = true
			// A star with an operator after it (* NOT LIKE 'x') is the copy's
			// own way to apply it to every column.
			if i+1 < len(toks) && !isOpTok(toks, i+1, ",") && !isOpTok(toks, i+1, ")") && !isOpTok(toks, i+1, ";") &&
				!(toks[i+1].kind == rtWord && toks[i+1].text == "from") {
				return "a * with something applied to it" + why
			}
		case t.kind == rtWord || t.kind == rtQuoted:
			if _, marked := view[strings.ToLower(t.text)]; marked {
				shown = true
			}
		}
	}
	if firstWord(toks) == "table" {
		// TABLE t is SELECT * FROM t.
		at := 1
		if isOpTok(toks, 2, ".") {
			at = 3
		}
		if at >= len(toks) {
			shown = true
		} else if _, held := view[relKey+strings.ToLower(toks[at].text)]; held {
			shown = true
		}
	}
	if veto := paddedRead(rest, view, true); veto != "" {
		return "a column" + why + " (" + veto + ")"
	}
	if shown && orderedByPosition(toks) {
		return "a position in ORDER BY that may be a column" + why
	}
	return ""
}

// The array types of text, varchar and name (pg_type).
const (
	oidNameArray    = 1003
	oidTextArray    = 1009
	oidVarcharArray = 1015
)

// userTypeStarsRead is how many stars of a statement have their FROM read.
const userTypeStarsRead = 64

// orderedByPosition reports whether an ORDER BY of the statement holds a
// number, or is the copy's ORDER BY ALL: a key that may stand for any column
// of the answer.
func orderedByPosition(toks []rtToken) bool {
	for i, t := range toks {
		if t.kind != rtWord || t.text != "order" || i+1 >= len(toks) || toks[i+1].kind != rtWord || toks[i+1].text != "by" {
			continue
		}
		if i+2 < len(toks) && toks[i+2].kind == rtWord && toks[i+2].text == "all" {
			return true
		}
		depth := 0
		for j := i + 2; j < len(toks); j++ {
			u := toks[j]
			if isOpTok(toks, j, "(") {
				depth++
			} else if isOpTok(toks, j, ")") {
				if depth == 0 {
					break
				}
				depth--
			} else if depth == 0 && u.kind == rtWord && (u.text == "limit" || u.text == "offset" || u.text == "fetch" || u.text == "for") {
				break
			} else if u.kind == rtNumber {
				return true
			}
		}
	}
	return false
}

// paddedArgument reports whether a statement has an argument of a declared
// type the copy would read as plain text and PostgreSQL does not: a bpchar,
// which compares without its trailing blanks, a type made after initdb, or a
// built-in one the port writes no cast for (inet, jsonb, a range).
func paddedArgument(oids []uint32) bool {
	plain := func(oid uint32) bool {
		switch oid {
		case 0, oidText, oidVarchar, oidName, oidNumeric:
			return true
		}
		return typedCast[oid] != ""
	}
	for _, oid := range oids {
		if elem, isArray := arrayElement[oid]; isArray {
			oid = elem
		}
		if !plain(oid) {
			return true
		}
	}
	return false
}

// userTypeReachedUnnamed names what reaches a column without writing its name
// and that paddedRead does not tell: a list of column names after a quoted
// alias or one named within, the alias of a parenthesized join (a row of all
// its tables), and a method call on a name
// (t.to_json()), which the copy reads as a call on the row.
func userTypeReachedUnnamed(toks []rtToken, view map[string][]string) string {
	// For each open parenthesis, whether it opens a subquery: a JOIN right
	// inside one that does not is a parenthesized join.
	var subquery []bool
	for i, t := range toks {
		switch {
		case isOpTok(toks, i, "("):
			next := i + 1
			subquery = append(subquery, next < len(toks) && toks[next].kind == rtWord && (toks[next].text == "select" || toks[next].text == "with"))
		case isOpTok(toks, i, ")"):
			if n := len(subquery); n > 0 {
				subquery = subquery[:n-1]
			}
		case t.kind == rtWord && t.text == "join":
			if n := len(subquery); n > 0 && !subquery[n-1] {
				return "a parenthesized join over a table with a column"
			}
		}
		if !isOpTok(toks, i+1, "(") {
			continue
		}
		if t.kind == rtQuoted || t.kind == rtWord && t.text == "within" {
			return "a list of names or a call after " + t.text + ", on a table with a column"
		}
		if t.kind == rtWord && isOpTok(toks, i-1, ".") && i >= 2 && toks[i-2].text != "pg_catalog" {
			return "a call on a name (" + toks[i-2].text + "." + t.text + "), on a table with a column"
		}
	}
	return ""
}

// starMayBringUserType reports whether the * at i may stand for a column of a
// type not built in. Its own FROM clause is read: a star over tables of the
// catalog that hold none does not, whatever else the statement reads.
// Anything else in that FROM (a subquery, a WITH's name, a function, a table
// the catalog does not list) may.
func starMayBringUserType(toks []rtToken, i int, view map[string][]string) bool {
	word := func(j int, w string) bool {
		return j >= 0 && j < len(toks) && toks[j].kind == rtWord && toks[j].text == w
	}
	from, depth := -1, 0
	for j := i + 1; j < len(toks) && from < 0; j++ {
		switch {
		case isOpTok(toks, j, "(") || isOpTok(toks, j, "["):
			depth++
		case isOpTok(toks, j, ")") || isOpTok(toks, j, "]"):
			if depth == 0 {
				return true
			}
			depth--
		case depth == 0 && word(j, "from") && !word(j-1, "distinct"):
			from = j
		}
	}
	if from < 0 {
		return true
	}
	// The items of that FROM: the name after FROM, JOIN or a comma, each with
	// its alias.
	type item struct{ table, schema, alias string }
	var items []item
	head := true
	depth = 0
scan:
	for j := from + 1; j < len(toks); j++ {
		t := toks[j]
		switch {
		case isOpTok(toks, j, "(") || isOpTok(toks, j, "["):
			if depth == 0 && head {
				return true
			}
			depth++
			continue
		case isOpTok(toks, j, ")") || isOpTok(toks, j, "]"):
			if depth == 0 {
				break scan
			}
			depth--
			continue
		}
		if depth != 0 {
			continue
		}
		if isOpTok(toks, j, ";") || t.kind == rtWord && (fromClauseEnd[t.text] || t.text == "union" || t.text == "intersect" || t.text == "except") {
			break
		}
		if isOpTok(toks, j, ",") || word(j, "join") {
			head = true
			continue
		}
		if !head || t.kind == rtWord && starJoinWords[t.text] {
			continue
		}
		head = false
		if t.kind != rtWord {
			return true
		}
		it := item{table: t.text, alias: t.text}
		if isOpTok(toks, j+1, ".") {
			if j+2 >= len(toks) || toks[j+2].kind != rtWord {
				return true
			}
			it.schema, it.table, it.alias = t.text, toks[j+2].text, toks[j+2].text
			j += 2
		}
		if isOpTok(toks, j+1, "(") {
			return true
		}
		k := j + 1
		if word(k, "as") {
			k++
		}
		if k < len(toks) && toks[k].kind == rtWord && !aliasStop[toks[k].text] && !fromClauseEnd[toks[k].text] {
			it.alias = toks[k].text
			j = k
		} else if k < len(toks) && toks[k].kind == rtQuoted {
			return true
		}
		items = append(items, it)
	}
	if len(items) == 0 {
		return true
	}
	holds := func(it item) bool {
		key := it.table
		if it.schema != "" {
			key = it.schema + "." + it.table
		}
		if _, listed := view[listedKey+key]; !listed {
			return true
		}
		_, held := view[relKey+it.table]
		return held
	}
	if isOpTok(toks, i-1, ".") {
		if i < 2 {
			return true
		}
		q := strings.ToLower(toks[i-2].text)
		for _, it := range items {
			if it.alias == q {
				return holds(it)
			}
		}
		return true
	}
	for _, it := range items {
		if holds(it) {
			return true
		}
	}
	return false
}

// listStar reports whether the * at i stands for columns: not a
// multiplication, and not count(*).
func listStar(toks []rtToken, i int) bool {
	if isOpTok(toks, i-1, "(") && isOpTok(toks, i+1, ")") && i >= 2 && toks[i-2].kind == rtWord && toks[i-2].text == "count" && !isOpTok(toks, i-3, ".") {
		return false
	}
	if i == 0 || i+1 >= len(toks) {
		return true
	}
	before, after := toks[i-1], toks[i+1]
	endsValue := false
	switch before.kind {
	case rtNumber, rtString, rtParam, rtQuoted, rtTyped:
		endsValue = true
	case rtWord:
		endsValue = !aliasStop[before.text]
	case rtOp:
		endsValue = before.text == ")" || before.text == "]"
	}
	startsValue := false
	switch after.kind {
	case rtNumber, rtString, rtParam, rtQuoted, rtTyped:
		startsValue = true
	case rtWord:
		switch after.text {
		case "from", "exclude", "replace", "rename", "like", "ilike", "similar":
		default:
			startsValue = !aliasStop[after.text] || after.text == "case"
		}
	case rtOp:
		startsValue = after.text == "(" || after.text == "-" || after.text == "+"
	}
	return !(endsValue && startsValue)
}

// userTypeInText is userTypeUse for a statement lexSQL does not read: what it
// names is not known, so it is the copy's only when its text holds no star,
// no name written with escapes, and neither the name of such a column nor of
// a table that holds one.
func userTypeInText(text string, types map[string][]string) string {
	if _, any := types[userTypeKey]; !any {
		return ""
	}
	const why = "a statement the port does not read whole, on a table with a column the copy does not compare as PostgreSQL does"
	low := strings.ToLower(text)
	if strings.Contains(low, "*") || strings.Contains(low, "u&") {
		return why
	}
	for k := range types {
		for _, prefix := range []string{userTypeColKey, userTypeRelKey} {
			if n, ok := strings.CutPrefix(k, prefix); ok && (strings.Contains(low, n) || strings.Contains(low, strings.ReplaceAll(n, `"`, `""`))) {
				return why
			}
		}
	}
	return ""
}
