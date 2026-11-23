package pgshim

import "strings"

// Two places a table that holds a char(n) column may stand without its
// padding counting (#3219), which paddedUse took for uses of its rows:
//
//   - as a later item of a FROM list written with commas (FROM orders,
//     order_line WHERE ...). The veto there was for a table's name written
//     as a value (count(DISTINCT t), a = b), which a name after a comma may
//     be; fromListItem tells the two apart by where the name stands. A join
//     written with commas compares what its WHERE compares, as one written
//     with JOIN ... ON does: a padded column named there is still vetoed by
//     name, and NATURAL and USING by their own rules.
//   - as the star of EXISTS (SELECT * FROM ...). PostgreSQL does not
//     evaluate the select list of an EXISTS subquery, and neither does the
//     copy: only whether a row is there. What the subquery compares it
//     compares by name, which is vetoed by name.
//
// Nothing here follows a subquery handed to anything but EXISTS (IN, = ANY,
// a scalar subquery, a row comparison): those read the star's values.

// fromListReach is how many tokens back from a name its FROM is looked for.
const fromListReach = 4096

// fromListItem reports whether the name at i is an item of a FROM list
// after a comma: between the FROM that opens the list and the name, at the
// name's own depth, stand only items of the list (names, with a schema or
// an alias, AS, and subqueries in parentheses) and the commas between them.
// Anything else on the way (a keyword, an operator, a number, a string, ON)
// and the name is not known to be one; so a list that mixes JOIN ... ON with
// commas is not followed, and stays vetoed. A FROM after DISTINCT is IS
// [NOT] DISTINCT FROM, no list, and neither is the FROM inside a function's
// parentheses.
func fromListItem(toks []rtToken, i int) bool {
	if !isOpTok(toks, i-1, ",") {
		return false
	}
	depth := 0
	for j := i - 1; j >= 0; j-- {
		if i-j > fromListReach {
			// A list longer than any a person writes: not followed, so that
			// a statement of nothing but names after commas costs its
			// length times this, not its length squared.
			return false
		}
		t := toks[j]
		switch t.kind {
		case rtOp:
			switch t.text {
			case ")":
				depth++
			case "(":
				if depth == 0 {
					return false
				}
				depth--
			case ",", ".":
			default:
				if depth == 0 {
					return false
				}
			}
		case rtWord:
			if depth != 0 {
				continue
			}
			if t.text == "from" {
				if j > 0 && toks[j-1].kind == rtWord && toks[j-1].text == "distinct" {
					return false
				}
				// The FROM of a function's own syntax (extract(year FROM x),
				// substring(x FROM 2), trim(both FROM x)) opens no list: the
				// parenthesis it stands in follows a function's name.
				in := 0
				for k := j - 1; k >= 0; k-- {
					switch {
					case isOpTok(toks, k, ")"):
						in++
					case isOpTok(toks, k, "(") && in > 0:
						in--
					case isOpTok(toks, k, "("):
						return !(k > 0 && toks[k-1].kind == rtWord && !parenWords[toks[k-1].text])
					}
				}
				return true
			}
			if t.text != "as" && (aliasStop[t.text] || parenWords[t.text]) {
				return false
			}
		case rtQuoted:
		default:
			if depth == 0 {
				return false
			}
		}
	}
	return false
}

// existsStars answers, for a statement, which stars are the whole select
// list of the subquery of an EXISTS that reads none of its rows' values:
// EXISTS (SELECT * FROM ...) with, in the subquery, no set operation (UNION,
// INTERSECT and EXCEPT compare the star's rows), and no DISTINCT, GROUP BY,
// HAVING or window, which put rows of equal values together: for a bpchar
// without a length PostgreSQL finds 'ab' and 'ab ' equal and the copy does
// not, so EXISTS (SELECT * FROM t GROUP BY 1 HAVING count(*) > 1) is true
// there and false here.
//
// The statement's parentheses are matched, and the words that end the
// exemption counted, once, the first time a star asks: a statement of
// EXISTS nested by the thousand costs its length, not its length squared.
type existsStars struct {
	ready bool
	// match is where each parenthesis' other end is, -1 for none.
	match []int
	// barred[i] is how many of the words above stand before token i.
	barred []int
}

// existsBarred are the words that keep a star under EXISTS vetoed.
var existsBarred = map[string]bool{
	"union": true, "intersect": true, "except": true,
	"distinct": true, "group": true, "having": true, "over": true, "window": true,
}

func (e *existsStars) star(toks []rtToken, i int) bool {
	word := func(j int, w string) bool { return j >= 0 && j < len(toks) && toks[j].kind == rtWord && toks[j].text == w }
	if !(word(i-1, "select") && isOpTok(toks, i-2, "(") && word(i-3, "exists") && word(i+1, "from")) || isOpTok(toks, i-4, ".") {
		return false
	}
	if !e.ready {
		e.ready = true
		e.match = make([]int, len(toks))
		e.barred = make([]int, len(toks)+1)
		var open []int
		for j, t := range toks {
			e.match[j] = -1
			e.barred[j+1] = e.barred[j]
			switch {
			case t.kind == rtWord && existsBarred[t.text]:
				e.barred[j+1]++
			case isOpTok(toks, j, "("):
				open = append(open, j)
			case isOpTok(toks, j, ")") && len(open) > 0:
				e.match[j], e.match[open[len(open)-1]] = open[len(open)-1], j
				open = open[:len(open)-1]
			}
		}
	}
	end := e.match[i-2]
	return end > i && e.barred[end] == e.barred[i]
}

// fromFollower are the keywords that may stand right after a table in FROM
// without being its alias.
var fromFollower = map[string]bool{
	"where": true, "group": true, "having": true, "order": true, "limit": true, "offset": true,
	"join": true, "left": true, "right": true, "full": true, "inner": true, "outer": true, "cross": true,
	"on": true, "using": true, "union": true, "intersect": true, "except": true, "window": true,
	"natural": true, "fetch": true, "for": true, "as": true,
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
					one := after >= len(toks) || isOpTok(toks, after, ")") || isOpTok(toks, after, ";") || toks[after].kind == rtWord && starFromEnd[toks[after].text]
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
				if f.from < 0 || f.setOp || fromMayHoldText(toks, f.from+1, i, types) {
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
		case depth == 0 && toks[m].kind == rtWord && starFromEnd[toks[m].text]:
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
