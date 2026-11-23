package pgshim

import "strings"

// A position behind a star.
//
// The screen sends a statement that sorts text to the source: PostgreSQL
// sorts text by the database's collation, the copy by bytes. It finds the
// text by what the statement names. SELECT * FROM t ORDER BY 2 names
// nothing: the star stands for the columns and the position for one of
// them, so the statement read as having no text, and the copy sorted a text
// column its own way. starMayBeText says when a position may be one.

// fromClauseEnd are the words that end a FROM clause.
var fromClauseEnd = map[string]bool{
	"where": true, "group": true, "having": true, "order": true, "limit": true,
	"offset": true, "window": true, "fetch": true, "for": true,
}

// withColumnOrder is types with each table's column types in their order,
// under colsKey and the table's name: what a position behind a star is
// resolved with. A name two schemas share gets none, and neither does a
// table when the types were not read (nil stays nil).
func withColumnOrder(types map[string][]string, tables []CatalogTable) map[string][]string {
	if types == nil {
		return nil
	}
	for i := range tables {
		t := &tables[i]
		// The copy's own types: a char(n) is text there, which is all a
		// position asks. No mark of a padded column is put here: paddedUse
		// reads the marks of the tables a statement reads, and this is every
		// table.
		cols := make([]string, 0, len(t.Columns))
		for _, c := range t.Columns {
			cols = append(cols, strings.ToUpper(c.Type))
		}
		SetColumnOrder(types, t.Name, cols)
	}
	return types
}

// SetColumnOrder tells types the column types of table, in the table's
// order, for a position behind a star (SELECT * FROM table ORDER BY 2). Told
// twice for one name (two schemas), the name's order is not known. For a
// caller that builds the screen's types itself (internal/pgcompare).
func SetColumnOrder(types map[string][]string, table string, cols []string) {
	key := colsKey + strings.ToLower(table)
	if _, twice := types[key]; twice {
		types[key] = nil
		return
	}
	types[key] = append([]string{}, cols...)
}

// colsKey is the prefix under which the screen's types hold a table's column
// types in their order (screenTypes).
const colsKey = relKey + relKey + relKey

// starMayBeText reports whether a position in an ORDER BY (ORDER BY 2) may
// stand for a text column that a * put there, so that the rule on sorted
// text takes every position of the statement for text (sortKeyText).
//
// What is recognized is what stays: nothing is asked of a statement with no
// * in a select list, and one statement is read exactly, SELECT * FROM one
// table of the catalog, with its positions looked up in the table's
// columns: it reports text only when a position is a text column, or is not
// a column at all. For any other statement with a * it reports text when
// the rows under any * may hold some (fromMayHoldText), wherever the ORDER
// BY stands and however it is written: a statement this does not follow is
// sent to the source when it sorts by position, never left to the copy.
func starMayBeText(toks []rtToken, types map[string][]string) bool {
	word := func(i int, w string) bool {
		return i >= 0 && i < len(toks) && toks[i].kind == rtWord && toks[i].text == w
	}
	var stars []int
	for i := 1; i < len(toks); i++ {
		if !isOpTok(toks, i, "*") || isOpTok(toks, i-1, "(") && isOpTok(toks, i+1, ")") {
			// Not a star, or count(*).
			continue
		}
		if isOpTok(toks, i-1, ".") && isOpTok(toks, i+1, ")") && isOpTok(toks, i-3, "(") {
			// count(t.*): the row handed to a function, no column of the
			// answer.
			continue
		}
		if word(i-1, "select") && isOpTok(toks, i-2, "(") && word(i-3, "exists") && !isOpTok(toks, i-4, ".") {
			// EXISTS (SELECT * ...): PostgreSQL does not read the list of an
			// EXISTS, and it puts no column in the answer. (IN (SELECT *
			// ...) is not this: its star decides what is compared.)
			continue
		}
		if i+1 >= len(toks) || isOpTok(toks, i+1, ",") || isOpTok(toks, i+1, ")") || isOpTok(toks, i+1, ";") || word(i+1, "from") {
			// The star of a list, not a multiplication (a * b).
			stars = append(stars, i)
		}
	}
	if len(stars) == 0 {
		return false
	}
	isText := func(ty string) bool { return typeIsText(ty) || paddedType(ty) }

	// The one statement read exactly: SELECT * FROM table [[AS] alias] ...
	// with no other SELECT and no set operation.
	if cols, ok := starOverOneTable(toks, types); ok {
		depth, inOrder, start := 0, false, 0
		position := func(lo, hi int) (n int, ok bool) {
			if hi-lo >= 3 && word(hi-2, "nulls") && (word(hi-1, "first") || word(hi-1, "last")) {
				hi -= 2
			}
			if hi-lo >= 2 && (word(hi-1, "asc") || word(hi-1, "desc")) {
				hi--
			}
			for hi-lo >= 3 && isOpTok(toks, lo, "(") && isOpTok(toks, hi-1, ")") {
				lo, hi = lo+1, hi-1
			}
			if hi-lo != 1 || toks[lo].kind != rtNumber {
				return 0, false
			}
			for _, c := range toks[lo].text {
				if c < '0' || c > '9' || n > 1<<20 {
					return -1, true
				}
				n = n*10 + int(c-'0')
			}
			return n, true
		}
		text := func(lo, hi int) bool {
			n, ok := position(lo, hi)
			return ok && (n < 1 || n > len(cols) || isText(cols[n-1]))
		}
		for i := 0; i <= len(toks); i++ {
			end := i == len(toks) || depth == 0 && (isOpTok(toks, i, ";") || toks[i].kind == rtWord &&
				(toks[i].text == "limit" || toks[i].text == "offset" || toks[i].text == "fetch" || toks[i].text == "for"))
			switch {
			case inOrder && (end || depth == 0 && isOpTok(toks, i, ",")):
				if text(start, i) {
					return true
				}
				start, inOrder = i+1, !end
			case end:
			case isOpTok(toks, i, "(") || isOpTok(toks, i, "["):
				depth++
			case isOpTok(toks, i, ")") || isOpTok(toks, i, "]"):
				depth--
			case depth == 0 && word(i, "order") && word(i+1, "by"):
				inOrder, start = true, i+2
				i++
			}
		}
		return false
	}

	// Any other statement: by the rows under each star. What reading them
	// may cost in all is bounded; past it the statement is not followed.
	budget := 4*len(toks) + 1<<16
	for _, at := range stars {
		from, depth := -1, 0
		for j := at + 1; j < len(toks) && from < 0; j++ {
			if budget--; budget < 0 {
				return true
			}
			switch {
			case isOpTok(toks, j, "(") || isOpTok(toks, j, "["):
				depth++
			case isOpTok(toks, j, ")") || isOpTok(toks, j, "]"):
				if depth == 0 {
					// The block ends with no FROM of its own.
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
		end := len(toks)
		for j, depth := from+1, 0; j < len(toks); j++ {
			if budget--; budget < 0 {
				return true
			}
			if isOpTok(toks, j, "(") || isOpTok(toks, j, "[") {
				depth++
			} else if isOpTok(toks, j, ")") || isOpTok(toks, j, "]") {
				if depth == 0 {
					end = j
					break
				}
				depth--
			}
		}
		if fromMayHoldText(toks, from+1, end, types) {
			return true
		}
	}
	return false
}

// starOverOneTable reports whether the statement is SELECT * FROM one table
// of the catalog, with an alias or without, and nothing else that selects:
// one SELECT, no set operation, no WITH. cols are the table's column types
// in their order.
func starOverOneTable(toks []rtToken, types map[string][]string) (cols []string, ok bool) {
	word := func(i int, w string) bool {
		return i >= 0 && i < len(toks) && toks[i].kind == rtWord && toks[i].text == w
	}
	if !(word(0, "select") && isOpTok(toks, 1, "*") && word(2, "from")) {
		return nil, false
	}
	for i, t := range toks {
		if t.kind == rtWord && (i > 0 && t.text == "select" || t.text == "union" || t.text == "intersect" || t.text == "except" || t.text == "with") {
			return nil, false
		}
	}
	at := 3
	if at+2 < len(toks) && toks[at].kind == rtWord && isOpTok(toks, at+1, ".") {
		at += 2
	}
	if at >= len(toks) || toks[at].kind != rtWord {
		return nil, false
	}
	after := at + 1
	if word(after, "as") {
		after++
	}
	if after < len(toks) && toks[after].kind == rtWord && !aliasStop[toks[after].text] {
		after++
	}
	if !(after >= len(toks) || isOpTok(toks, after, ";") || toks[after].kind == rtWord && fromClauseEnd[toks[after].text]) {
		return nil, false
	}
	cols = types[colsKey+toks[at].text]
	return cols, len(cols) > 0
}

// fromMayHoldText reports whether the rows of the FROM clause that starts
// at lo (and ends before hi, or at the first WHERE, GROUP BY... at its own
// depth) may hold a text column, for a * over them: a table of the catalog
// with a text column, or one the port has no columns for; a name that is no
// table of the catalog (a WITH's); and, in a subquery, a string, a text
// column, a function that makes text, or a table with a text column when
// the subquery has a star of its own. A join's condition counts as the
// subquery does: more is taken for text, never less.
func fromMayHoldText(toks []rtToken, lo, hi int, types map[string][]string) bool {
	isText := func(ty string) bool { return typeIsText(ty) || paddedType(ty) }
	end, depth := hi, 0
	nestedStar := false
	for m := lo; m < hi; m++ {
		switch {
		case isOpTok(toks, m, "(") || isOpTok(toks, m, "["):
			depth++
		case isOpTok(toks, m, ")") || isOpTok(toks, m, "]"):
			depth--
		case depth == 0 && toks[m].kind == rtWord && fromClauseEnd[toks[m].text]:
			end = m
		case depth > 0 && isOpTok(toks, m, "*") && m > 0 && (toks[m-1].kind == rtOp && (toks[m-1].text == "," || toks[m-1].text == ".") ||
			toks[m-1].kind == rtWord && (toks[m-1].text == "select" || toks[m-1].text == "distinct" || toks[m-1].text == "all")):
			nestedStar = true
		}
		if end != hi {
			break
		}
	}
	depth = 0
	// opened says, for each open parenthesis, whether it opens a subquery;
	// subqueries how many of those are open. A parenthesis around a join
	// (FROM (a JOIN b ON ...)) is no subquery: its tables' columns are the
	// rows'.
	var opened []bool
	subqueries := 0
	for m := lo; m < end; m++ {
		u := toks[m]
		switch u.kind {
		case rtString, rtParam:
			return true
		case rtOp:
			switch u.text {
			case "(", "[":
				depth++
				sub := m+1 < end && toks[m+1].kind == rtWord && (toks[m+1].text == "select" || toks[m+1].text == "with")
				opened = append(opened, sub)
				if sub {
					subqueries++
				}
			case ")", "]":
				depth--
				if n := len(opened); n > 0 {
					if opened[n-1] {
						subqueries--
					}
					opened = opened[:n-1]
				}
			case "||":
				return true
			}
		case rtWord, rtQuoted:
			name := tokenText(toks, m)
			if isOpTok(toks, m+1, ".") || isOpTok(toks, m+1, "(") && !textMakers[name] {
				// A schema or a table before its dot, a function's name.
				continue
			}
			if textMakers[name] {
				return true
			}
			if cols, table := types[colsKey+name]; table {
				if len(cols) == 0 {
					return true
				}
				if subqueries > 0 && !nestedStar {
					// Inside a subquery with no star of its own, the table
					// gives the rows only what the subquery's list names.
					continue
				}
				for _, ty := range cols {
					if isText(ty) {
						return true
					}
				}
				continue
			}
			for _, ty := range types[name] {
				if isText(ty) {
					return true
				}
			}
			// The head of an item of this FROM that is no table of the
			// catalog: a WITH's name, whose columns are not listed here.
			if _, column := types[name]; !column && u.kind == rtWord && !aliasStop[name] && m > 0 &&
				(m == lo || isOpTok(toks, m-1, ",") || isOpTok(toks, m-1, ".") || toks[m-1].kind == rtWord && (toks[m-1].text == "join" || toks[m-1].text == "from")) {
				return true
			}
		}
	}
	return false
}

// tokenText is the name a token writes, lowercased, "" past the end.
func tokenText(toks []rtToken, i int) string {
	if i < 0 || i >= len(toks) {
		return ""
	}
	if toks[i].kind == rtQuoted {
		return strings.ToLower(toks[i].text)
	}
	return toks[i].text
}
