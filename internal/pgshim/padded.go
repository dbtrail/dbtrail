package pgshim

import (
	"fmt"
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
// own type, not what it is made of, so such a column is read as any other
// type of the user's (an enum, say) and is not marked.

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
		for _, c := range t.Columns {
			n := strings.ToLower(c.Name)
			typ := strings.ToUpper(c.Type)
			if m := sourceMark(c); m != "" && (read == nil || read(t)) {
				typ = m
			}
			out[n] = append(out[n], typ)
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
// another name. Named anywhere else (a comparison, a join, GROUP BY, ORDER
// BY, a function, ||, a cast, a CASE, an aggregate, USING), the statement is
// the source's. The same holds for a star: it stands for the padded column
// too, so it is left to the copy only as a whole item of that one select
// list.
//
// A bpchar without a length, and a column whose type is not recorded, keep
// the blanks each value was written with, so two values PostgreSQL finds
// equal are two on the copy: beside one of those, DISTINCT and GROUP BY are
// the source's as well.
func paddedUse(toks []rtToken, types map[string][]string) string {
	anyPadded, allSized := paddedAmong(types)
	if !anyPadded {
		return ""
	}

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
				if !(p.kind == rtWord && (p.text == "from" || p.text == "join" || p.text == "table") || isOpTok(toks, i-1, ".")) {
					return fmt.Sprintf("the table %q, which holds a char(n) column, written where a value or a second table of a list stands", n) + otherRows
				}
				j := i + 1
				if at(j).kind == rtWord && at(j).text == "as" {
					j++
				}
				if a := at(j); a.kind == rtQuoted || a.kind == rtWord && !aliasStop[a.text] {
					relAlias[strings.ToLower(a.text)] = j
				}
			}
			padded, sized := paddedKind(types, n)
			if !padded {
				continue
			}
			whole, alias := wholeItem(i)
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
	if padded, _ := paddedAmong(types); !padded {
		return ""
	}
	low := strings.ToLower(text)
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
func (s *session) paddedVeto(text string) string {
	if s.bound == nil || s.bound.Catalog == nil {
		return ""
	}
	toks, unlexed := lexSQL(text)
	switch firstWord(toks) {
	case "show", "describe", "summarize", "explain":
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
		veto = paddedUse(toks, types)
	}
	if veto != "" {
		if unread {
			s.warnCatalogUnread()
		}
		s.logger.Debug("pgshim: not answered by the copy", "construct", veto)
	}
	return veto
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
	s.fallTo(ReasonVeto)
	e := s.paddedError(veto)
	return &pgErr{e.Code, e.Message}
}
