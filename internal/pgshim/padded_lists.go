package pgshim

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

// listsProven reports whether the two exemptions here may be used at all, by
// the rule the stars of padded_star.go go by: every column of the tables read
// is of a built-in type (a domain over char(n) carries no mark, so naming it
// would not be vetoed), and nothing in the text gives a value the padded type
// (a cast to char(n), an argument).
func listsProven(toks []rtToken, types map[string][]string) bool {
	if _, unproven := types[starUnprovenKey]; unproven {
		return false
	}
	for _, t := range toks {
		if paddedTypeWritten(t) {
			return false
		}
	}
	return true
}

// keywordColumnList names a table alias spelled like a keyword that may stand
// before a parenthesis, with a list of column names after it: codes AS
// materialized(p). Those two words are not read as functions, so the rename
// would go unseen and p would not be known for the column it is. As keywords
// they open a subquery, which starts with SELECT, WITH or another parenthesis.
func keywordColumnList(toks []rtToken) string {
	for i, t := range toks {
		if t.kind != rtWord || t.text != "materialized" && t.text != "exists" || !isOpTok(toks, i+1, "(") {
			continue
		}
		if isOpTok(toks, i+2, "(") {
			continue
		}
		if i+2 < len(toks) && toks[i+2].kind == rtWord && (toks[i+2].text == "select" || toks[i+2].text == "with") {
			continue
		}
		return "a list of column names after a table named " + t.text + ": it gives the columns other names"
	}
	return ""
}

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
// INTERSECT and EXCEPT compare the star's rows), no ORDER BY, and no
// DISTINCT, GROUP BY, HAVING or window, which put rows of equal values together: for a bpchar
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
	// An ORDER BY there changes no answer, but it may sort by a position of
	// the star, which the rule on sorted text does not look at under EXISTS.
	"order": true,
}

func (e *existsStars) star(toks []rtToken, i int) bool {
	if !existsListStar(toks, i) || i+1 >= len(toks) || toks[i+1].kind != rtWord || toks[i+1].text != "from" {
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
