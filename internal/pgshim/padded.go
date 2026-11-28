package pgshim

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// char(n) columns (#3068).
//
// The copy holds a char(n) column as plain text with the blanks PostgreSQL
// pads it with ('ab' in a char(5) is 'ab   '), and compares, measures and
// concatenates those blanks. PostgreSQL does not: its bpchar type ignores
// trailing blanks when it compares, and drops them when the value becomes
// text (length, ||, a comparison with a text column, a UNION with one). So
// WHERE c = 'ab' finds three rows on PostgreSQL and none on the copy, with
// no error anywhere.
//
// The copy's own type for the column (VARCHAR) does not show it, so the
// screen is told the source's: the catalog carries the type the snapshot
// recorded for each column (CatalogColumn.SourceType), and the screen's
// types name such a column with a mark of its own in place of VARCHAR
// (screenTypes). paddedUse then sends to the source every statement that
// uses a marked column anywhere but in a plain select list, the one place
// where both sides write the same value. Marking too much sends a statement
// to the source that the copy would have answered well; marking too little
// is a wrong answer. So a column whose type on the source is not known is
// marked too.
//
// Not told apart: a domain over char(n). The snapshot records the domain's
// own type, not what it is made of, so such a column is not marked; a
// statement that reads it goes by the rule of user_types.go instead.

// The marks, in place of the copy's type. Each holds CHAR, so typeIsText
// takes a marked column for text, as the copy holds it.
const (
	// charNType is a char(n): every value is padded to the same n
	// characters, so two values of one column compare on the copy as on
	// PostgreSQL, and a select list writes them alike.
	charNType = "BPCHAR(N)"
	// bpcharType is a bpchar without a length, which keeps the blanks each
	// value was written with ('ab' and 'ab ' are equal on PostgreSQL and two
	// values on the copy), or a text column whose type on the source the
	// snapshot does not record, which may be either.
	bpcharType = "BPCHAR"
)

// oidBpcharArray is char(n)[] (pg_type _bpchar).
const oidBpcharArray = 1014

// PaddedOnSource reports whether a type of the source (pg_attribute.atttypid)
// is one PostgreSQL pads: char(n), bpchar, or an array of them.
func PaddedOnSource(oid uint32) bool { return oid == oidBpchar || oid == oidBpcharArray }

// paddedType reports whether a type of the screen is one of the marks.
func paddedType(t string) bool { return strings.HasPrefix(t, bpcharType) }

// sizedPadded reports whether a mark is a char(n)'s or a char(n)[]'s.
func sizedPadded(t string) bool { return strings.HasPrefix(t, charNType) }

// sourceMark is the mark of a column the source pads, "" for a column it
// does not: a char(n), a bpchar, an array of either, or a column the copy
// holds as text whose type on the source is not recorded.
func sourceMark(c CatalogColumn) string {
	array := ""
	if strings.HasSuffix(strings.TrimSpace(c.Type), "[]") {
		array = "[]"
	}
	switch c.SourceType {
	case oidBpchar, oidBpcharArray:
		if c.SourceType == oidBpcharArray {
			array = "[]"
		}
		// atttypmod is the length plus four; -1 is no length.
		if c.SourceTypeMod >= 0 {
			return charNType + array
		}
		return bpcharType + array
	case 0:
		if typeIsText(strings.ToUpper(c.Type)) {
			return bpcharType + array
		}
	}
	return ""
}

// screenTypes are the types the screen reads (screenForCopy): the copy's
// column types by lowercased column name, across tables, with a mark in
// place of the type of each column the source pads (sourceMark). read, when
// not nil, says which tables the statement reads: a padded column of any
// other table keeps the copy's type, so a statement on a table without a
// char(n) column is not sent to the source for a column of that name
// elsewhere.
func screenTypes(tables []CatalogTable, read func(*CatalogTable) bool) map[string][]string {
	out := map[string][]string{}
	for i := range tables {
		t := &tables[i]
		// The table is in the catalog, by name and by schema.name (paddedExempt).
		out[listedKey+strings.ToLower(t.Name)] = []string{relType}
		out[listedKey+strings.ToLower(t.Schema+"."+t.Name)] = []string{relType}
		for _, c := range t.Columns {
			n := strings.ToLower(c.Name)
			typ := strings.ToUpper(c.Type)
			if m := sourceMark(c); m != "" && (read == nil || read(t)) {
				if m == charNType && c.TrimmedOnCopy {
					// Read by the view without its padding: text to the
					// screen, under trimmedUse's own rule (trimmed.go). The
					// table is still named, for what reaches the column
					// without its name (paddedRead).
					out[trimmedKey] = []string{relType}
					out[trimmedKey+n] = []string{relType}
					out[relKey+strings.ToLower(t.Name)] = []string{relType}
				} else {
					typ = m
				}
			}
			out[n] = append(out[n], typ)
			if onlyTextOnCopy(c) && (read == nil || read(t)) {
				out[onlyCopyTextKey] = []string{relType}
				out[onlyCopyTextKey+n] = []string{relType}
			}
			if !provenNotPadded(c) && (read == nil || read(t)) {
				out[starUnprovenKey] = []string{relType}
			}
			if userTypeColumn(c) && (read == nil || read(t)) {
				markUserType(out, t.Name, n)
			}
			if paddedType(typ) {
				// The table itself: its name, or its alias, written as a
				// value stands for every column of it (paddedUse).
				out[relKey+strings.ToLower(t.Name)] = []string{relType}
			}
		}
	}
	return out
}

// relKey is the prefix under which the screen's types name a table that
// holds a padded column: no name a statement writes starts with it. relType
// is what stands there; it is not a type, and neither text nor a mark.
const (
	relKey  = "\x00"
	relType = "RELATION"
)

// CopyScreenFor is CopyScreen as a connection whose copy's catalog is tables
// decides it: the statement's tables are read from its text, and a column
// the source pads counts only in a table it reads. For internal/pgcompare.
func CopyScreenFor(stmt string, tables []CatalogTable) string {
	return CopyScreen(stmt, screenTypes(tables, tablesRead(RewriteForCopy(stmt), tables)))
}

// tablesRead says which of the copy's tables stmt reads, as the copy is
// handed it; nil when that cannot be told (its text is not one readsOf
// reads whole, or it names a table the catalog does not list), and then
// every table counts as read. More tables than it reads, never fewer: a
// name is matched without regard to case and, written without a schema, in
// every schema, as apartFromSource matches it.
func tablesRead(stmt string, tables []CatalogTable) func(*CatalogTable) bool {
	reads, why := readsOf(stmt, copyFuncs)
	if why != "" {
		return nil
	}
	hit := map[*CatalogTable]bool{}
	for _, ref := range reads.refs {
		schema, name := "", ref.parts[len(ref.parts)-1].text
		switch len(ref.parts) {
		case 1:
		case 2:
			schema = ref.parts[0].text
		default:
			return nil
		}
		found := false
		for i := range tables {
			t := &tables[i]
			if strings.EqualFold(t.Name, name) && (schema == "" || strings.EqualFold(t.Schema, schema)) {
				found = true
				hit[t] = true
			}
		}
		if !found && !reads.isCTE(ref) {
			return nil
		}
	}
	return func(t *CatalogTable) bool { return hit[t] }
}

// paddedKind is what the screen knows of a name: whether a column of that
// name is padded on the source, and whether every such column is a char(n).
func paddedKind(types map[string][]string, name string) (padded, sized bool) {
	sized = true
	for _, t := range types[name] {
		if paddedType(t) {
			padded = true
			sized = sized && sizedPadded(t)
		}
	}
	return padded, padded && sized
}

// paddedArray reports whether a column of that name is an array of a padded
// type. Only a plain char(n) is known to group the same on both sides.
func paddedArray(types map[string][]string, name string) bool {
	for _, t := range types[name] {
		if paddedType(t) && strings.HasSuffix(t, "[]") {
			return true
		}
	}
	return false
}

// paddedAmong reports whether any column of the screen's types is padded
// on the source, and whether every such column is a char(n).
func paddedAmong(types map[string][]string) (padded, sized bool) {
	sized = true
	for _, ts := range types {
		for _, t := range ts {
			if paddedType(t) {
				padded = true
				sized = sized && sizedPadded(t)
			}
		}
	}
	return padded, padded && sized
}

// paddedWhat names a marked column in a veto.
func paddedWhat(name string, sized bool) string {
	if sized {
		return fmt.Sprintf("the char(n) column %q", name)
	}
	return fmt.Sprintf("the column %q, a bpchar on the source or of a type the snapshot does not record", name)
}

// paddedUse says what in a statement uses a column the source pads in a way
// the copy answers otherwise, "" when nothing does. toks are the statement's
// tokens as lexSQL gives them; types the screen's (screenTypes). With no
// types nothing is marked, and nothing is said: a connection whose catalog
// could not be read says so itself (catalogUnread).
//
// The rule is by position, not by meaning: a padded column may be named only
// as a whole item of the select list of a statement that is one SELECT and
// nothing else (no subquery, no WITH, no UNION, INTERSECT or EXCEPT), with
// or without a table before it and an alias after it. There PostgreSQL and
// the copy write the same padded value, and nothing can read it again under
// another name, or, for a char(n), as a whole key of that statement's GROUP
// BY (#3219): every value of a char(n) has the same length, so the groups
// are the same. Named anywhere else (a comparison, a join, ORDER BY, a
// function, ||, a cast, a CASE, an aggregate, USING, an expression in GROUP
// BY), the statement is the source's. The same holds for a star: it stands for the padded column
// too, so it is left to the copy only as a whole item of that one select
// list.
//
// A bpchar without a length, and a column whose type is not recorded, keep
// the blanks each value was written with, so two values PostgreSQL finds
// equal are two on the copy: beside one of those, DISTINCT and GROUP BY are
// the source's as well.
func paddedUse(toks []rtToken, types map[string][]string) string {
	return paddedUseOrdered(toks, types, true)
}

// paddedUseOrdered is paddedUse; mainOrder as in trimmedUse.
func paddedUseOrdered(toks []rtToken, types map[string][]string, mainOrder bool) string {
	if why := userTypeUse(toks, types); why != "" {
		return why
	}
	if why := paddedRead(toks, types, true); why != "" {
		return why
	}
	return trimmedUse(toks, types, mainOrder)
}

// paddedRead is paddedUse. exemptions false reads the statement without any
// of the exemptions of padded_star.go and padded_lists.go, to tell whether
// one of them is what let it pass (paddedForNoSource).
func paddedRead(toks []rtToken, types map[string][]string, exemptions bool) string {
	anyPadded, allSized := paddedAmong(types)
	if !anyPadded {
		// A char(n) the view trims is reached without its name as any other
		// (NATURAL JOIN, TABLE, a list of column names, the table written as
		// a value): those are refused below. Its own uses are trimmedUse's.
		if _, ok := types[trimmedKey]; !ok {
			return ""
		}
		allSized = true
	}

	if why := keywordColumnList(toks); why != "" {
		return why + " on a copy that holds a char(n) column"
	}

	// The stars whose padded columns the statement never reads (padded_star.go).
	var exempt map[int]bool
	// And the later items of a FROM list and the stars of EXISTS (SELECT *
	// ...), which read no column (padded_lists.go).
	lists := false
	if exemptions {
		exempt = paddedExempt(toks, types)
		lists = listsProven(toks, types)
	}
	exists := &existsStars{}

	// One SELECT and nothing else, and where its select list's items are.
	selects, sel := 0, -1
	plain, groups, setOps := true, false, false
	for i, t := range toks {
		if t.kind != rtWord {
			continue
		}
		switch t.text {
		case "select":
			selects++
			if sel < 0 {
				sel = i
			}
		case "union", "intersect", "except":
			plain, setOps = false, true
		case "with", "values", "table":
			plain = false
		case "distinct", "group":
			groups = true
		}
	}
	plain = plain && selects == 1 && firstWord(toks) == "select"
	// items are the select list's items, each [start, end).
	var items [][2]int
	if plain {
		start, depth := sel+1, 0
	list:
		for j := sel + 1; ; j++ {
			if j >= len(toks) {
				items = append(items, [2]int{start, j})
				break
			}
			t := toks[j]
			switch {
			case t.kind == rtOp && (t.text == "(" || t.text == "["):
				depth++
			case t.kind == rtOp && (t.text == ")" || t.text == "]"):
				if depth == 0 {
					items = append(items, [2]int{start, j})
					break list
				}
				depth--
			case depth == 0 && t.kind == rtOp && t.text == ",":
				items = append(items, [2]int{start, j})
				start = j + 1
			case depth == 0 && (t.kind == rtOp && t.text == ";" || t.kind == rtWord && t.text == "from"):
				items = append(items, [2]int{start, j})
				break list
			}
		}
	}
	name := func(t rtToken) bool { return t.kind == rtWord || t.kind == rtQuoted }
	// wholeItem reports whether the token at i ends a whole select item: the
	// item is [DISTINCT | ALL] [name .]... <the token> [[AS] alias].
	// alias is where the item's alias stands, -1 when it has none.
	wholeItem := func(i int) (whole bool, alias int) {
		for _, it := range items {
			if i < it[0] || i >= it[1] {
				continue
			}
			j := it[0]
			if j < i && toks[j].kind == rtWord && (toks[j].text == "distinct" || toks[j].text == "all") {
				j++
			}
			for ; j < i; j += 2 {
				if !name(toks[j]) || !isOpTok(toks, j+1, ".") {
					return false, -1
				}
			}
			if j != i {
				return false, -1
			}
			switch rest := toks[i+1 : it[1]]; len(rest) {
			case 0:
				return true, -1
			case 1:
				return toks[i].kind != rtOp && name(rest[0]) && !(rest[0].kind == rtWord && rest[0].text == "as"), i + 1
			case 2:
				return toks[i].kind != rtOp && rest[0].kind == rtWord && rest[0].text == "as" && name(rest[1]), i + 2
			}
			return false, -1
		}
		return false, -1
	}
	// keys are the keys of the statement's GROUP BY, each [start, end), when
	// it is one SELECT and nothing else.
	var keys [][2]int
	if plain {
		depth := 0
	clause:
		for g, t := range toks {
			switch {
			case t.kind == rtOp && (t.text == "(" || t.text == "["):
				depth++
			case t.kind == rtOp && (t.text == ")" || t.text == "]"):
				depth--
			case depth == 0 && t.kind == rtWord && t.text == "group" && g+1 < len(toks) && toks[g+1].kind == rtWord && toks[g+1].text == "by":
				start, in := g+2, 0
				for j := g + 2; ; j++ {
					if j >= len(toks) {
						keys = append(keys, [2]int{start, j})
						break clause
					}
					u := toks[j]
					switch {
					case u.kind == rtOp && (u.text == "(" || u.text == "["):
						in++
					case u.kind == rtOp && (u.text == ")" || u.text == "]"):
						in--
					case in == 0 && u.kind == rtOp && u.text == ",":
						keys = append(keys, [2]int{start, j})
						start = j + 1
					case in == 0 && (u.kind == rtOp && u.text == ";" || u.kind == rtWord && groupEnd[u.text]):
						keys = append(keys, [2]int{start, j})
						break clause
					}
				}
			}
		}
	}
	// wholeKey reports whether the token at i is a whole key of GROUP BY:
	// [name .]... <the token>, nothing else. Every value of a char(n) is
	// padded to the same length, so the padded values PostgreSQL finds equal
	// are the ones the copy does, and the groups are the same (#3219). Not
	// so for a bpchar without a length (sized).
	wholeKey := func(i int) bool {
		for _, k := range keys {
			if i < k[0] || i >= k[1] {
				continue
			}
			if i != k[1]-1 {
				return false
			}
			j := k[0]
			for ; j < i; j += 2 {
				if !name(toks[j]) || !isOpTok(toks, j+1, ".") {
					return false
				}
			}
			return j == i
		}
		return false
	}
	at := func(i int) rtToken {
		if i < 0 || i >= len(toks) {
			return rtToken{kind: rtOp}
		}
		return toks[i]
	}
	// value reports whether the token at i can end a value (so a * after it
	// multiplies) or start one (so a * before it does).
	endsValue := func(i int) bool {
		t := at(i)
		switch t.kind {
		case rtNumber, rtString, rtParam, rtQuoted, rtTyped:
			return true
		case rtWord:
			return t.text != "select" && t.text != "distinct" && t.text != "all"
		}
		return t.text == ")" || t.text == "]"
	}
	startsValue := func(i int) bool {
		t := at(i)
		switch t.kind {
		case rtNumber, rtString, rtParam, rtQuoted, rtTyped:
			return true
		case rtWord:
			// * EXCLUDE (...), * REPLACE (...), * LIKE '...': the copy's own
			// ways to write a row.
			switch t.text {
			case "from", "exclude", "replace", "rename", "like", "ilike", "similar":
				return false
			}
			return true
		}
		return t.text == "(" || t.text == "-" || t.text == "+"
	}
	// given are the names the statement gives a padded column or a table
	// that holds one (an alias), by where each is given: written anywhere
	// else, a column's stands for the column, and a table's, with no dot
	// after it, for the whole row.
	colAlias, relAlias := map[string]int{}, map[string]int{}
	const otherRows = " (PostgreSQL ignores a char(n) column's trailing blanks, the copy compares them)"

	for i, t := range toks {
		// What reaches a column without writing its name. Each is the
		// source's on a connection that has one (vetoWords, an operator or
		// a function the screen does not know); here they are said again,
		// for the connection whose statements the copy answers alone.
		switch {
		case t.kind == rtWord && t.text == "natural":
			return "NATURAL JOIN of a table with a char(n) column: it joins by the column without naming it" + otherRows
		case t.kind == rtWord && t.text == "table" && !isOpTok(toks, i-1, ".") && !(i == 0 && !setOps && selects == 0):
			return "TABLE of a table with a char(n) column, where its rows are compared" + otherRows
		case t.kind == rtWord && t.text == "from" && (isOpTok(toks, i-1, "(") || firstWord(toks) == "from"):
			return "FROM written first, the copy's own syntax, on a copy that holds a char(n) column" + otherRows
		case t.kind == rtWord && t.text == "columns" && isOpTok(toks, i+1, "("):
			return "COLUMNS() on a copy that holds a char(n) column" + otherRows
		case t.kind == rtOp && strings.Contains(t.text, "#"):
			return "a column named by its position (#n) on a copy that holds a char(n) column" + otherRows
		case t.kind == rtWord && isOpTok(toks, i+1, "(") && !parenWords[t.text] && t.text != "over" && t.text != "filter" && t.text != "within":
			// t AS a(i, x), ) s(p, q): a list that gives the columns other
			// names. A call is never written right after a name, AS or a
			// closing parenthesis; a type's modifier (numeric(10,2)) holds
			// no name.
			p := at(i - 1)
			after := p.kind == rtQuoted || isOpTok(toks, i-1, ")") ||
				p.kind == rtWord && (p.text == "as" || !aliasStop[p.text] && !parenWords[p.text])
			if end := closingParen(toks, i+1); after && end > 0 {
				for _, in := range toks[i+2 : end] {
					if name(in) {
						return "a list of column names after a table (" + t.text + "(...)) on a copy that holds a char(n) column: it gives the column another name" + otherRows
					}
				}
			}
		}
		switch {
		case name(t):
			n := strings.ToLower(t.text)
			if _, rel := types[relKey+n]; rel && !isOpTok(toks, i+1, ".") {
				// A table that holds a padded column, not before a dot: named
				// in FROM (its alias is noted), or written as a value.
				p := at(i - 1)
				if !(p.kind == rtWord && (p.text == "from" || p.text == "join" || p.text == "table") || isOpTok(toks, i-1, ".") || lists && fromListItem(toks, i)) {
					return fmt.Sprintf("the table %q, which holds a char(n) column, written where a value or a second table of a list stands", n) + otherRows
				}
				j := i + 1
				if at(j).kind == rtWord && at(j).text == "as" {
					j++
				}
				if a := at(j); a.kind == rtQuoted || a.kind == rtWord && !aliasStop[a.text] {
					relAlias[strings.ToLower(a.text)] = j
				} else if a.kind == rtWord && (j > i+1 || !fromFollower[a.text]) {
					// A name PostgreSQL takes as the table's alias and this
					// reads as a keyword (FROM codes last): its uses as the
					// whole row could not be told from the keyword's.
					return fmt.Sprintf("the table %q, which holds a char(n) column, given the name %q, which the port reads as a keyword", n, a.text) + otherRows
				}
			}
			padded, sized := paddedKind(types, n)
			if !padded {
				continue
			}
			// Read without exemptions (paddedForNoSource), a cut counts as
			// any other use, so a connection with no source runs the whole
			// screen on it.
			if exemptions && paddedCut(toks, i) {
				continue
			}
			whole, alias := wholeItem(i)
			if !whole && sized && !paddedArray(types, n) && wholeKey(i) {
				// A key of GROUP BY: the same groups on both sides.
				continue
			}
			if !whole {
				return paddedWhat(n, sized) + ", used where its trailing blanks count (PostgreSQL ignores them, the copy compares them)"
			}
			if !sized && groups {
				return paddedWhat(n, sized) + ", beside DISTINCT or GROUP BY (PostgreSQL takes values that differ in trailing blanks for one)"
			}
			if alias >= 0 {
				colAlias[strings.ToLower(toks[alias].text)] = alias
			}
		case t.kind == rtOp && t.text == "*":
			if endsValue(i-1) && startsValue(i+1) {
				// A multiplication.
				continue
			}
			if isOpTok(toks, i-1, "(") && isOpTok(toks, i+1, ")") && at(i-2).kind == rtWord && at(i-2).text == "count" && !isOpTok(toks, i-3, ".") {
				// count(*) counts rows, whatever they hold.
				continue
			}
			if exempt[i] || lists && exists.star(toks, i) {
				continue
			}
			if whole, _ := wholeItem(i); !whole {
				return "* of a table with a char(n) column, outside a plain select list (PostgreSQL ignores its trailing blanks, the copy compares them)"
			}
			if !allSized && groups {
				return "* of a table with a bpchar column, or one of a type the snapshot does not record, beside DISTINCT or GROUP BY"
			}
		}
	}
	// A name given above, written again: the column under its alias
	// (SELECT c AS x ... WHERE x = 'ab', which the copy's engine takes), or
	// the table's whole row (count(DISTINCT a), a = b).
	for i, t := range toks {
		if !name(t) {
			continue
		}
		n := strings.ToLower(t.text)
		if def, ok := colAlias[n]; ok && def != i {
			return fmt.Sprintf("the name %q, given to a char(n) column, used again", n) + otherRows
		}
		if def, ok := relAlias[n]; ok && def != i && !isOpTok(toks, i+1, ".") {
			return fmt.Sprintf("the name %q, given to a table that holds a char(n) column, written where a value stands", n) + otherRows
		}
	}
	return ""
}

// groupEnd are the words that end a GROUP BY's list of keys.
var groupEnd = map[string]bool{
	"having": true, "order": true, "limit": true, "offset": true, "window": true, "fetch": true, "for": true,
	"union": true, "intersect": true, "except": true,
}

// catalogUnread is the veto of a statement that reads a table while the
// copy's catalog could not be read: which of its columns the source pads is
// not known, so the copy does not answer it. "" for a statement that reads
// no table (SELECT 1).
func catalogUnread(toks []rtToken) string {
	for _, t := range toks {
		if t.kind == rtWord && (t.text == "from" || t.text == "table") {
			return "a table read while the copy's columns could not be listed: one may be a char(n) on the source"
		}
	}
	return ""
}

var catalogUnreadWarned struct {
	mu sync.Mutex
	at time.Time
}

// warnCatalogUnread says, at most once a minute for the process, that
// statements for the copy are going to the source because the copy's
// catalog could not be read: the copy answers none that reads a table until
// it can.
func (s *session) warnCatalogUnread() {
	catalogUnreadWarned.mu.Lock()
	due := time.Since(catalogUnreadWarned.at) >= time.Minute
	if due {
		catalogUnreadWarned.at = time.Now()
	}
	catalogUnreadWarned.mu.Unlock()
	if due {
		s.logger.Warn("pgshim: the copy's columns could not be listed, so which of them is a char(n) on the source is not known and no statement that reads a table is answered by the copy (said once a minute)")
	}
}

// paddedInText is paddedUse for a statement whose text lexSQL does not read
// (a dollar quote, a string with a prefix, a number in another form), which
// only a connection that sends nothing to its source hands the copy: what it
// names is not known, so it is the copy's only when its text holds no name
// of a padded column, no star and no name written with Unicode escapes.
// types are every table's (the statement's tables are not read either).
func paddedInText(text string, types map[string][]string) string {
	if why := userTypeInText(text, types); why != "" {
		return why
	}
	low := strings.ToLower(text)
	if _, ok := types[trimmedKey]; ok {
		// A char(n) the view trims is safe only in the forms trimmedUse
		// reads, and this text is not read.
		for name := range types {
			if n, ok := strings.CutPrefix(name, trimmedKey); ok && n != "" && strings.Contains(low, n) {
				return fmt.Sprintf("the char(n) column %q, named in a statement the port does not read whole", n)
			}
		}
	}
	if padded, _ := paddedAmong(types); !padded {
		return ""
	}
	if strings.Contains(low, "*") || strings.Contains(low, "u&") {
		return "a statement the port does not read whole, with a * or a name written with escapes, on a copy that holds a char(n) column"
	}
	for name, ts := range types {
		for _, t := range ts {
			if paddedType(t) && strings.Contains(low, name) {
				_, sized := paddedKind(types, name)
				return paddedWhat(name, sized) + ", named in a statement the port does not read whole"
			}
		}
	}
	return ""
}

// paddedVeto says why a statement for the copy, text as the copy is handed
// it, is not the copy's to answer for a column the source pads, "" when it
// is: paddedUse, with what a connection knows of its catalog and of the
// statement's tables.
//
// A command of the copy's own (SHOW TABLES, DESCRIBE, SUMMARIZE) and an
// EXPLAIN have no answer of PostgreSQL's to differ from. A host that lists
// no catalog (Binding.Catalog nil) has no column to mark.
//
// s.catalogBusy says, after it, whether the catalog was unread because the
// copy was busy: the veto is then the moment's, not the statement's
// (paddedRefusal, describeWith).
func (s *session) paddedVeto(text string) string {
	s.catalogBusy = ""
	if s.argsPadded {
		return "an argument of a declared type the copy would read as plain text (a bpchar, a domain, inet...)"
	}
	toks, unlexed := lexSQL(text)
	switch firstWord(toks) {
	case "show", "describe", "summarize", "explain":
		return ""
	}
	if !s.canFallBack() {
		// Nothing else keeps such a value from the copy on a connection that
		// cannot reach its source, whatever is known of its columns.
		if unlexed == "" && paddedTypeNamed(toks) || unlexed != "" && paddedTypeInText(text) {
			s.logger.Debug("pgshim: not answered by the copy", "construct", paddedCastWhy)
			return paddedCastWhy
		}
	}
	if s.bound == nil || s.bound.Catalog == nil {
		return ""
	}
	types, unread := s.columnTypes(text)
	veto := ""
	switch {
	case unread && unlexed != "":
		veto = "a statement the port does not read whole, while the copy's columns could not be listed"
	case unread:
		veto = catalogUnread(toks)
	case unlexed != "":
		veto = paddedInText(text, types)
	default:
		if s.canFallBack() {
			veto = paddedUse(toks, types)
		} else {
			veto = paddedForNoSource(toks, types)
		}
		if veto == "" && s.argsBound && s.screened != "" {
			// A portal's arguments are written into the text as strings, which
			// trimmedUse may let through ('ab'), but the statement compares the
			// column with a parameter: its Describe, written with NULLs, was
			// vetoed and announced no rows, and the source decides it whatever
			// the value. Read as the client wrote it, it is vetoed the same.
			if tmpl, unlexed := lexSQL(s.screened); unlexed == "" {
				veto = trimmedUse(tmpl, types, s.canFallBack())
			}
		}
	}
	if veto != "" {
		if unread {
			s.warnCatalogUnread()
		}
		s.logger.Debug("pgshim: not answered by the copy", "construct", veto)
	}
	return veto
}

// paddedForNoSource is paddedUse for a connection with no source to fall back
// on. A statement that passes only by an exemption (a star nothing reads, a
// later item of a FROM list, the star of an EXISTS) passes the whole screen
// too: it was refused before, and the copy must not now answer it with text
// sorted its own way.
func paddedForNoSource(toks []rtToken, types map[string][]string) string {
	if paddedTypeNamed(toks) {
		return paddedCastWhy
	}
	veto := paddedUseOrdered(toks, types, false)
	if padded, _ := paddedAmong(types); veto != "" || !padded {
		return veto
	}
	if len(paddedExempt(toks, types)) > 0 || paddedRead(toks, types, false) != "" {
		return screenWith(toks, types, false, true)
	}
	return ""
}

// paddedCastWhy is why a statement that names the padded type is refused.
const paddedCastWhy = "a value cast to char(n) or \"char\" (PostgreSQL pads or cuts it and compares it without its trailing blanks, the copy does not)"

// paddedTypeInText is paddedTypeNamed for a statement the port does not
// read whole (an exponent, E'...', $$...$$): any of the padded type's names
// as a word of the text, in or out of quotes, varchar's char varying
// included: refusing more there is the safe side.
func paddedTypeInText(text string) bool {
	return paddedWordInText.MatchString(text)
}

var paddedWordInText = regexp.MustCompile(`(?i)(^|[^a-z0-9_$])(char|character|nchar|bpchar)($|[^a-z0-9_$])`)

// paddedTypeNamed reports whether a statement names the padded type or the
// one-byte "char" (#3233): a cast to one, CAST(... AS char(n)) or a typed
// literal such as bpchar 'a '. PostgreSQL pads or cuts such a value and
// compares it without its trailing blanks; the copy holds it as the text
// written ('abc'::"char" is a there, abc here). On a connection that reaches
// its source the general screen already sends every cast there; one that
// cannot must refuse it. char varying and character varying are varchar.
func paddedTypeNamed(toks []rtToken) bool {
	for i, t := range toks {
		if !paddedTypeWritten(t) || t.kind == rtParam {
			continue
		}
		if t.kind == rtWord && i+1 < len(toks) && toks[i+1].kind == rtWord && toks[i+1].text == "varying" {
			continue
		}
		return true
	}
	return false
}

// paddedError is the error of a statement paddedVeto keeps from the copy.
// On a connection that sends nothing to its source that error is the
// client's: the statement is refused rather than answered with other rows.
func (s *session) paddedError(veto string) *Error {
	msg := "the copy does not answer this as PostgreSQL would: " + veto
	if !s.canFallBack() {
		msg += "; this connection sends nothing to the source, so the statement is refused rather than answered with other rows"
	}
	return &Error{Code: "0A000", Message: msg}
}

// paddedRefusal refuses a statement about to run on the copy, text as the
// copy is handed it, that uses a column the source pads where the copy
// answers otherwise (paddedVeto). On a connection that can reach its source
// the screen sent such a statement there already (routeFree), and this sends
// there one that got past it; on one that cannot, the statement is refused.
func (s *session) paddedRefusal(text string) *pgErr {
	veto := s.paddedVeto(text)
	if veto == "" {
		return nil
	}
	if why := s.catalogBusy; why != "" && s.canFallBack() {
		// The copy was too busy to list its columns (#3109): refused as
		// busy, not sent to the source (route.go).
		s.busyRefused = why
		return busyRefusal(why)
	}
	s.fallTo(ReasonVeto)
	e := s.paddedError(veto)
	return &pgErr{e.Code, e.Message}
}
