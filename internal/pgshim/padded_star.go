package pgshim

import "strings"

// A star over a char(n) column the statement never reads.
//
// paddedUse vetoes every padded column written by name outside a plain select
// list, and every star there, because a star may stand for one. But a star
// inside a WITH or a subquery often stands for columns the statement goes on
// to ignore, or for no padded column at all. paddedExempt finds those, and
// only those: it returns the tokens paddedUse may pass over. It never adds a
// veto, and anything it cannot follow makes it return nil, which leaves
// paddedUse deciding exactly as it would without it.
//
// What it relies on: in a statement that is not one plain SELECT, a padded
// column written by name is vetoed wherever it stands, so the only ways left
// to reach one are another star over the same rows, the row written whole as
// a value, the block with the star comparing or ordering its own rows, and a
// subquery handed to an expression. Each of those keeps the veto here.
//
// A char(n) column named in the last SELECT of a WITH is not exempt, even
// returned whole: its bytes match, but the port announces it as text where
// PostgreSQL announces bpchar and its length.

// listedKey is the prefix under which the screen's types say a table is in
// the catalog, by name and by schema.name, whether or not it holds a padded
// column (screenTypes). No name a statement writes starts with it.
const listedKey = relKey + relKey

// starUnprovenKey is present in the screen's types when a table the statement
// reads has a column whose type is not known to be free of padding. A domain
// over char(n) is recorded under the domain's own type, so it carries no mark:
// paddedUse's veto of the star is then all that keeps such a column from the
// copy, and no star is exempted.
const starUnprovenKey = listedKey + relKey

// firstUserOID is the first OID PostgreSQL gives an object made after initdb.
const firstUserOID = 16384

// provenNotPadded reports whether a column's type on the source is a built-in
// one, so that sourceMark tells for certain whether it is a char(n). A domain,
// an enum, a composite, an extension's type, or a type the snapshot does not
// record is not proven.
func provenNotPadded(c CatalogColumn) bool {
	return c.SourceType != 0 && c.SourceType < firstUserOID
}

// starPaddedTypes are the names of the padded type: a literal cast to one
// compares without its trailing blanks on PostgreSQL, whatever the columns.
var starPaddedTypes = map[string]bool{"char": true, "character": true, "bpchar": true, "nchar": true}

// starUnfollowed are words that put a statement outside what paddedExempt
// follows.
var starUnfollowed = map[string]bool{
	"recursive": true, "lateral": true, "natural": true, "using": true,
	"union": true, "intersect": true, "except": true, "values": true, "table": true,
	"into": true, "tablesample": true, "rollup": true, "cube": true, "grouping": true,
}

// starJoinWords separate the items of a FROM clause.
var starJoinWords = map[string]bool{
	"join": true, "inner": true, "left": true, "right": true, "full": true, "outer": true, "cross": true,
}

// starFromEnd are the words that end a FROM clause.
var starFromEnd = map[string]bool{
	"where": true, "group": true, "having": true, "order": true, "limit": true,
	"offset": true, "window": true, "fetch": true, "for": true,
}

type starScan struct {
	toks  []rtToken
	types map[string][]string
	// ctes says, by name, whether a WITH's rows hold a padded column.
	ctes map[string]bool
	// carriers are the names that stand for rows holding a padded column;
	// defs the tokens where such a name is defined or read as a FROM item.
	carriers map[string]bool
	defs     map[int]bool
	// exempt are the stars paddedUse may pass over.
	exempt map[int]bool
}

// paddedExempt is the stars of toks that paddedUse need not veto: see the
// comment at the top of this file. nil when the statement is not followed.
func paddedExempt(toks []rtToken, types map[string][]string) map[int]bool {
	n := len(toks)
	for n > 0 && isOpTok(toks, n-1, ";") {
		n--
	}
	if n == 0 {
		return nil
	}
	if _, unproven := types[starUnprovenKey]; unproven {
		return nil
	}
	for i, t := range toks[:n] {
		if t.kind == rtOp && t.text == ";" {
			return nil
		}
		if t.kind == rtQuoted && (strings.EqualFold(t.text, "bpchar") || t.text == "char") {
			return nil
		}
		// An argument's type is not in the text: declared bpchar, it compares
		// without its trailing blanks.
		if t.kind == rtParam {
			return nil
		}
		if t.kind != rtWord {
			continue
		}
		// A keyword after a dot is a column (u.limit), and after AS a label
		// (1 AS from), not the clause the reading below would take it for.
		if i > 0 && (isOpTok(toks, i-1, ".") || toks[i-1].kind == rtWord && toks[i-1].text == "as") && (aliasStop[t.text] || starFromEnd[t.text] || starJoinWords[t.text]) {
			return nil
		}
		if starPaddedTypes[t.text] || starUnfollowed[t.text] || t.text == "with" && i != 0 {
			return nil
		}
		if t.text == "by" && i+1 < n && toks[i+1].kind == rtWord && toks[i+1].text == "all" {
			return nil
		}
	}
	toks = toks[:n]
	a := &starScan{
		toks: toks, types: types,
		ctes: map[string]bool{}, carriers: map[string]bool{}, defs: map[int]bool{}, exempt: map[int]bool{},
	}
	main := 0
	if toks[0].kind == rtWord && toks[0].text == "with" {
		var ok bool
		if main, ok = a.withList(); !ok {
			return nil
		}
	}
	if _, ok := a.block(main, n, true); !ok {
		return nil
	}
	// A name that stands for rows with a padded column, written as anything
	// but a FROM item or before a dot, is the whole row as a value.
	for i, t := range toks {
		if (t.kind == rtWord || t.kind == rtQuoted) && a.carriers[strings.ToLower(t.text)] && !a.defs[i] && !isOpTok(toks, i+1, ".") {
			return nil
		}
	}
	return a.exempt
}

// withList reads the opening WITH and returns where its last SELECT starts.
func (a *starScan) withList() (int, bool) {
	toks := a.toks
	word := func(i int, w string) bool {
		return i < len(toks) && toks[i].kind == rtWord && toks[i].text == w
	}
	i := 1
	for {
		if i >= len(toks) || toks[i].kind != rtWord {
			return 0, false
		}
		name, at := toks[i].text, i
		if _, dup := a.ctes[name]; dup {
			return 0, false
		}
		i++
		if !word(i, "as") {
			return 0, false
		}
		i++
		if word(i, "not") && word(i+1, "materialized") {
			i += 2
		} else if word(i, "materialized") {
			i++
		}
		if !isOpTok(toks, i, "(") {
			return 0, false
		}
		end := closingParen(toks, i)
		if end < 0 {
			return 0, false
		}
		carries, ok := a.block(i+1, end, false)
		if !ok {
			return 0, false
		}
		a.ctes[name] = carries
		if carries {
			a.carriers[name] = true
			a.defs[at] = true
		}
		i = end + 1
		if !isOpTok(toks, i, ",") {
			return i, true
		}
		i++
	}
}

// starSource is one item of a FROM clause.
type starSource struct {
	alias   string
	carries bool
}

// block reads the SELECT in toks[lo:hi] and notes what in it is exempt. It
// says whether its rows hold a padded column. last: it is the statement's
// last SELECT, whose rows reach the client.
func (a *starScan) block(lo, hi int, last bool) (carries, ok bool) {
	toks := a.toks
	if lo >= hi || toks[lo].kind != rtWord || toks[lo].text != "select" {
		return false, false
	}
	fromAt, fromEnd, orderAt := -1, hi, -1
	distinct, shaped, over := false, false, false
	depth := 0
	for i := lo + 1; i < hi; i++ {
		t := toks[i]
		if t.kind == rtOp {
			switch t.text {
			case "(", "[":
				depth++
			case ")", "]":
				depth--
			}
			continue
		}
		if t.kind != rtWord {
			continue
		}
		if t.text == "over" {
			over = true
		}
		if depth != 0 {
			continue
		}
		switch t.text {
		case "distinct":
			distinct = true
		case "from":
			if fromAt < 0 {
				// IS [NOT] DISTINCT FROM in the select list is no FROM clause.
				if toks[i-1].kind == rtWord && toks[i-1].text == "distinct" {
					return false, false
				}
				fromAt = i
			}
		case "group", "having", "window":
			shaped = true
		case "order":
			if orderAt < 0 {
				orderAt = i
			}
		}
		if fromAt >= 0 && fromEnd == hi && starFromEnd[t.text] {
			fromEnd = i
		}
	}
	listEnd := hi
	var sources []starSource
	if fromAt >= 0 {
		listEnd = fromAt
		if sources, ok = a.fromItems(fromAt+1, fromEnd); !ok {
			return false, false
		}
	}
	first := lo + 1
	if first < listEnd && toks[first].kind == rtWord && (toks[first].text == "distinct" || toks[first].text == "all") {
		first++
	}
	var items [][2]int
	start, d := first, 0
	for i := first; i < listEnd; i++ {
		switch {
		case isOpTok(toks, i, "(") || isOpTok(toks, i, "["):
			d++
		case isOpTok(toks, i, ")") || isOpTok(toks, i, "]"):
			d--
		case d == 0 && isOpTok(toks, i, ","):
			items = append(items, [2]int{start, i})
			start = i + 1
		}
	}
	items = append(items, [2]int{start, listEnd})

	for _, it := range items {
		switch size := it[1] - it[0]; {
		case size == 1 && isOpTok(toks, it[0], "*"):
			if len(sources) == 0 {
				return false, false
			}
			starCarries := false
			for _, s := range sources {
				starCarries = starCarries || s.carries
			}
			if starCarries && (last || len(items) != 1 || len(sources) != 1 || distinct || shaped || over || orderAt >= 0) {
				return false, false
			}
			carries = carries || starCarries
			a.exempt[it[0]] = true
		case size == 3 && toks[it[0]].kind == rtWord && isOpTok(toks, it[0]+1, ".") && isOpTok(toks, it[0]+2, "*"):
			found := false
			for _, s := range sources {
				if s.alias != toks[it[0]].text {
					continue
				}
				found = true
				if s.carries && (last || len(items) != 1 || distinct || shaped || over || orderAt >= 0) {
					return false, false
				}
				carries = carries || s.carries
			}
			if !found {
				return false, false
			}
			a.exempt[it[0]+2] = true
		}
	}
	return carries, true
}

// fromItems reads a FROM clause, toks[lo:hi], of table names and subqueries
// with an alias. Anything else in it (a function, a parenthesized join, a
// quoted name, a column list) is not followed.
func (a *starScan) fromItems(lo, hi int) ([]starSource, bool) {
	toks := a.toks
	var out []starSource
	seen := map[string]bool{}
	joinWord := func(i int) bool { return toks[i].kind == rtWord && starJoinWords[toks[i].text] }
	for i := lo; i < hi; {
		for i < hi && joinWord(i) {
			i++
		}
		if i >= hi {
			return nil, false
		}
		// The item ends at a comma, a join word or ON.
		end, d := i, 0
		for ; end < hi; end++ {
			if isOpTok(toks, end, "(") {
				d++
			} else if isOpTok(toks, end, ")") {
				d--
			} else if d == 0 && (isOpTok(toks, end, ",") || joinWord(end) || toks[end].kind == rtWord && toks[end].text == "on") {
				break
			}
		}
		s, ok := a.fromItem(i, end)
		if !ok || seen[s.alias] {
			return nil, false
		}
		seen[s.alias] = true
		out = append(out, s)
		i = end
		if i < hi && toks[i].kind == rtWord && toks[i].text == "on" {
			for d = 0; i < hi; i++ {
				if isOpTok(toks, i, "(") {
					d++
				} else if isOpTok(toks, i, ")") {
					d--
				} else if d == 0 && (isOpTok(toks, i, ",") || joinWord(i)) {
					break
				}
			}
		}
		if i < hi && isOpTok(toks, i, ",") {
			i++
			if i >= hi {
				return nil, false
			}
		}
	}
	return out, len(out) > 0
}

// fromItem reads one item: [schema .] name [[AS] alias], or (SELECT ...)
// [AS] alias.
func (a *starScan) fromItem(lo, hi int) (starSource, bool) {
	toks := a.toks
	var s starSource
	i := lo
	nameAt := -1
	if isOpTok(toks, i, "(") {
		end := closingParen(toks, i)
		if end < 0 || end >= hi {
			return s, false
		}
		carries, ok := a.block(i+1, end, false)
		if !ok {
			return s, false
		}
		s.carries = carries
		i = end + 1
	} else {
		if toks[i].kind != rtWord {
			return s, false
		}
		nameAt = i
		name, schema := toks[i].text, ""
		i++
		if i < hi && isOpTok(toks, i, ".") {
			if i+1 >= hi || toks[i+1].kind != rtWord {
				return s, false
			}
			schema, name = name, toks[i+1].text
			nameAt = i + 1
			i += 2
		}
		carries, isCTE := a.ctes[name]
		if schema != "" || !isCTE {
			key := name
			if schema != "" {
				key = schema + "." + name
			}
			if _, listed := a.types[listedKey+key]; !listed {
				return s, false
			}
			_, carries = a.types[relKey+name]
		}
		s.carries, s.alias = carries, name
	}
	aliasAt := -1
	if i < hi && toks[i].kind == rtWord && toks[i].text == "as" {
		i++
		if i >= hi {
			return s, false
		}
	}
	if i < hi {
		if toks[i].kind != rtWord || aliasStop[toks[i].text] {
			return s, false
		}
		s.alias, aliasAt = toks[i].text, i
		i++
	}
	if i != hi || s.alias == "" {
		return s, false
	}
	if s.carries {
		a.carriers[s.alias] = true
		if nameAt >= 0 {
			a.carriers[toks[nameAt].text] = true
			a.defs[nameAt] = true
		}
		if aliasAt >= 0 {
			a.defs[aliasAt] = true
		}
	}
	return s, true
}
