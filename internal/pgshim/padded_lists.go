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

// existsStar reports whether the star at i is the whole select list of the
// subquery of an EXISTS: EXISTS (SELECT * FROM ...), with no set operation
// in the subquery (UNION, INTERSECT and EXCEPT compare the star's rows).
func existsStar(toks []rtToken, i int) bool {
	word := func(j int, w string) bool { return j >= 0 && j < len(toks) && toks[j].kind == rtWord && toks[j].text == w }
	if !(word(i-1, "select") && isOpTok(toks, i-2, "(") && word(i-3, "exists") && word(i+1, "from")) || isOpTok(toks, i-4, ".") {
		return false
	}
	end := closingParen(toks, i-2)
	if end < 0 {
		return false
	}
	for _, t := range toks[i+1 : end] {
		if t.kind == rtWord && (t.text == "union" || t.text == "intersect" || t.text == "except") {
			return false
		}
	}
	return true
}
