package pgshim

import "strings"

// A position behind a star.
//
// The screen sends a statement that sorts text to the source: PostgreSQL
// sorts text by the database's collation, the copy by bytes. It finds the
// text by what the statement names. SELECT * FROM t ORDER BY 2 names
// nothing: the star stands for the columns and the position for one of
// them, so the statement read as having no text, and the copy sorted a text
// column its own way. starByPosition reads the position.

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
		cols := make([]string, 0, len(t.Columns))
		for _, c := range t.Columns {
			typ := strings.ToUpper(c.Type)
			if m := sourceMark(c); m != "" {
				typ = m
			}
			cols = append(cols, typ)
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

const (
	starTextPositionVeto    = "ORDER BY a position that a * puts a text column at (the source sorts it by its collation)"
	starUnknownPositionVeto = "ORDER BY a position behind a * whose columns the port does not list, over rows that may hold text (the source sorts text by its collation)"
)

// starByPosition names an ORDER BY whose key is a position (ORDER BY 2) in a
// SELECT whose list has a *, when the column at that position is text or may
// be: PostgreSQL sorts text by the database's collation, the copy by bytes,
// and the rule on sorted text (screenWith) reads only what the statement
// names, which a * and a position do not. "" when nothing.
//
// The position is worked out where it can be: the list is one * and FROM is
// one table of the catalog. Anywhere else the key is vetoed when the rows
// the SELECT reads may hold text (a table with a text column under a *, a
// text column or a string in a subquery, a WITH's name), and left alone when
// they hold none (SELECT * FROM (SELECT id, k FROM t) s ORDER BY 1).
func starByPosition(toks []rtToken, types map[string][]string) string {
	type frame struct {
		sel, from  int
		items      int
		star, bare bool
		setOp      bool
	}
	isText := func(ty string) bool { return typeIsText(ty) || paddedType(ty) }
	// What reading the keys and the FROM clauses may cost in all: past it
	// the statement is not one this follows (ORDER BY written a hundred
	// thousand times), and the key is vetoed.
	budget := 4*len(toks) + 1<<16
	stack := []frame{{sel: -1, from: -1}}
	for i := 0; i < len(toks); i++ {
		f := &stack[len(stack)-1]
		t := toks[i]
		switch {
		case isOpTok(toks, i, "(") || isOpTok(toks, i, "["):
			stack = append(stack, frame{sel: -1, from: -1})
		case isOpTok(toks, i, ")") || isOpTok(toks, i, "]"):
			if len(stack) > 1 {
				stack = stack[:len(stack)-1]
			}
		case isOpTok(toks, i, ","):
			if f.sel >= 0 && f.from < 0 {
				f.items++
			}
		case isOpTok(toks, i, "*"):
			if f.sel >= 0 && f.from < 0 {
				p := toks[i-1]
				starts := p.kind == rtOp && (p.text == "," || p.text == ".") || p.kind == rtWord && (p.text == "select" || p.text == "distinct" || p.text == "all")
				ends := i+1 >= len(toks) || isOpTok(toks, i+1, ",") || isOpTok(toks, i+1, ")") || isOpTok(toks, i+1, ";") || toks[i+1].kind == rtWord && toks[i+1].text == "from"
				if starts && ends {
					f.star = true
					f.bare = p.kind == rtWord
				}
			}
		case t.kind != rtWord:
		case t.text == "select":
			if f.sel >= 0 {
				f.setOp = true
			}
			f.sel, f.from, f.items, f.star, f.bare = i, -1, 1, false, false
		case t.text == "from" && f.sel >= 0 && f.from < 0 && !(toks[i-1].kind == rtWord && toks[i-1].text == "distinct"):
			f.from = i
		case t.text == "union" || t.text == "intersect" || t.text == "except":
			f.setOp = true
		case t.text == "order" && i+1 < len(toks) && toks[i+1].kind == rtWord && toks[i+1].text == "by" && f.sel >= 0 && f.star:
			// Its keys, to the end of the block.
			depth, start := 0, i+2
			var keys [][2]int
			j := i + 2
		keys:
			for ; j < len(toks); j++ {
				if budget--; budget < 0 {
					return starUnknownPositionVeto
				}
				u := toks[j]
				switch {
				case isOpTok(toks, j, "(") || isOpTok(toks, j, "["):
					depth++
				case isOpTok(toks, j, ")") || isOpTok(toks, j, "]"):
					if depth == 0 {
						break keys
					}
					depth--
				case depth == 0 && isOpTok(toks, j, ","):
					keys = append(keys, [2]int{start, j})
					start = j + 1
				case depth == 0 && (isOpTok(toks, j, ";") || u.kind == rtWord && (u.text == "limit" || u.text == "offset" || u.text == "fetch" || u.text == "for" || u.text == "union" || u.text == "intersect" || u.text == "except")):
					break keys
				}
			}
			keys = append(keys, [2]int{start, j})
			for _, k := range keys {
				if k[1] <= k[0] || toks[k[0]].kind != rtNumber {
					continue
				}
				position := true
				for _, u := range toks[k[0]+1 : k[1]] {
					if u.kind != rtWord || !(u.text == "asc" || u.text == "desc" || u.text == "nulls" || u.text == "first" || u.text == "last") {
						position = false
					}
				}
				if !position {
					continue
				}
				// The one shape whose column is known: SELECT * FROM table.
				if f.bare && f.items == 1 && !f.setOp && f.from >= 0 {
					at := f.from + 1
					if at+2 < len(toks) && toks[at].kind == rtWord && isOpTok(toks, at+1, ".") {
						at += 2
					}
					after := at + 1
					if after < len(toks) && toks[after].kind == rtWord && toks[after].text == "as" {
						after++
					}
					if after < len(toks) && toks[after].kind == rtWord && !aliasStop[toks[after].text] {
						after++
					}
					one := after >= len(toks) || isOpTok(toks, after, ")") || isOpTok(toks, after, ";") || toks[after].kind == rtWord && fromClauseEnd[toks[after].text]
					if cols := types[colsKey+tokenText(toks, at)]; one && at < len(toks) && len(cols) > 0 {
						n := 0
						for _, c := range toks[k[0]].text {
							if c < '0' || c > '9' || n > 1<<20 {
								n = -1
								break
							}
							n = n*10 + int(c-'0')
						}
						if n < 1 || n > len(cols) {
							return starUnknownPositionVeto
						}
						if isText(cols[n-1]) {
							return starTextPositionVeto
						}
						continue
					}
				}
				// Anything else: by what the block's FROM may hold.
				if budget -= i - f.from; budget < 0 || f.from < 0 || f.setOp || fromMayHoldText(toks, f.from+1, i, types) {
					return starUnknownPositionVeto
				}
			}
		}
	}
	return ""
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
	for m := lo; m < end; m++ {
		u := toks[m]
		switch u.kind {
		case rtString, rtParam:
			return true
		case rtOp:
			switch u.text {
			case "(", "[":
				depth++
			case ")", "]":
				depth--
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
				if depth > 0 && !nestedStar {
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
			if _, column := types[name]; !column && depth == 0 && u.kind == rtWord && !aliasStop[name] && m > 0 &&
				(m == lo || isOpTok(toks, m-1, ",") || isOpTok(toks, m-1, ".") || toks[m-1].kind == rtWord && toks[m-1].text == "join") {
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
