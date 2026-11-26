package pgshim

import "strings"

// A product decision (#3219): the ORDER BY of the main statement may sort text
// on the copy, which sorts by bytes where PostgreSQL sorts by the database's
// collation. The rows are the same and may come in another order ('B, a, c'
// for 'a, B, c'), and with a LIMIT or an OFFSET other rows may be the ones
// kept.
//
// Nothing else that sorts text is relaxed, because there the order decides a
// value or which rows feed a count: a window, an aggregate's ORDER BY, a
// subquery or a WITH, DISTINCT ON, min and max of text, a comparison by order.

// mainTextOrder decides, once per statement, which sort keys of text the
// copy may sort. Everything it needs is read in one pass, so a statement with
// many ORDER BYs (one per window) costs no more than its length.
type mainTextOrder struct {
	outer    map[int]bool // the ORDER tokens of the main statement
	padded   bool         // a char(n) column is among the tables read
	onlyCopy bool         // a column that is text only on the copy is too
	plain    func(name string) bool
	types    map[string][]string
	inherit  map[string]bool // names an output may inherit (outputNames)
}

// newMainTextOrder reads toks. types are the screen's with the statement's
// aliases in them, raw the screen's as the catalog gave them.
func newMainTextOrder(toks []rtToken, types, raw map[string][]string) *mainTextOrder {
	r := &mainTextOrder{outer: map[int]bool{}, types: types}
	if types == nil {
		return r
	}
	depth := 0
	for j, t := range toks {
		switch {
		case isOpTok(toks, j, "(") || isOpTok(toks, j, "["):
			depth++
		case isOpTok(toks, j, ")") || isOpTok(toks, j, "]"):
			depth--
		case t.kind == rtWord && t.text == "distinct" && j+1 < len(toks) && toks[j+1].kind == rtWord && toks[j+1].text == "on":
			r.outer = map[int]bool{}
			return r
		case depth == 0 && t.kind == rtWord && t.text == "order":
			r.outer[j] = true
		}
	}
	var read bool
	if r.inherit, read, _ = outputNames(toks); !read {
		r.outer = map[int]bool{}
		return r
	}
	r.padded, _ = paddedAmong(types)
	_, r.onlyCopy = raw[onlyCopyTextKey]
	if r.onlyCopy {
		r.plain = plainTextNames(toks, raw)
	}
	return r
}

// plainTextNames says of a column's name whether it is text on the source too
// (no column of that name is only text on the copy), that no alias of the
// statement may stand for it and that the statement names no columns in a
// list. One pass over toks, however many names are asked about.
func plainTextNames(toks []rtToken, raw map[string][]string) func(string) bool {
	defined := map[string]bool{}
	for name := range aliasDefinitions(toks) {
		defined[strings.ToLower(name)] = true
	}
	listed := columnList(toks)
	return func(name string) bool {
		if _, opaque := raw[onlyCopyTextKey+name]; opaque || listed || defined[name] {
			return false
		}
		ts := raw[name]
		for _, t := range ts {
			if !anyTextType([]string{t}) {
				return false
			}
		}
		return len(ts) > 0
	}
}

// relaxes reports whether the key of the ORDER at toks[i] may sort on the copy.
func (r *mainTextOrder) relaxes(i int, key []rtToken) bool {
	if !r.outer[i] {
		return false
	}
	bare := bareOrderKey(key)
	// PostgreSQL refuses a constant as a key; the copy would answer it.
	if len(bare) == 1 && (bare[0].kind == rtString || bare[0].kind == rtParam) {
		return false
	}
	if n := len(bare); n > 0 && (bare[n-1].kind == rtWord || bare[n-1].kind == rtQuoted) && r.inherit[strings.ToLower(bare[n-1].text)] {
		return false
	}
	if r.padded && orderKeyHidesColumn(bare, r.types) {
		return false
	}
	return !r.onlyCopy || r.plainColumn(bare)
}

// plainColumn reports whether a key is a column written by its name, with its
// qualifiers or without, that is text on the source too and that no alias of
// the statement may stand for. With an enum, a money or a numeric(50,2) among
// the tables read nothing else is known not to be one, and those sort by
// their meaning on PostgreSQL ('high' after 'medium', 10.00 after 9.00), which
// is another answer and not another collation.
func (r *mainTextOrder) plainColumn(key []rtToken) bool {
	last := len(key) - 1
	if last < 0 || last%2 != 0 {
		return false
	}
	for j := 0; j < last; j += 2 {
		if (key[j].kind != rtWord && key[j].kind != rtQuoted) || !isOpTok(key, j+1, ".") {
			return false
		}
	}
	return key[last].kind == rtWord && r.plain(key[last].text)
}

// outputNames are the column names an output of the statement may inherit
// with no alias written. PostgreSQL names `x::date`, `CAST(x AS date)`, a CASE
// by its ELSE and a scalar subquery by what it selects after the column inside
// them, and an ORDER BY of that bare name then sorts the OUTPUT, while the
// copy names those outputs otherwise and sorts the text column x.
//
// The walks inside CAST, ELSE and a subquery share one budget proportional to
// the statement, because the port accepts megabytes of text and this runs
// before anything is asked of the source. Past it the answer is false: the
// names could not be read, and no key is relaxed.
func outputNames(toks []rtToken) (names map[string]bool, read bool, steps int) {
	out := map[string]bool{}
	limit, cut := outputNamesLimit(toks), false
	spend := func() bool {
		if steps >= limit {
			cut = true
			return false
		}
		steps++
		return true
	}
	add := func(from, to int) bool {
		for j := from; j < to && j < len(toks); j++ {
			if !spend() {
				return false
			}
			if toks[j].kind == rtWord || toks[j].kind == rtQuoted {
				out[strings.ToLower(toks[j].text)] = true
			}
		}
		return true
	}
	// closing finds the parenthesis that closes the one at open, -1 when
	// there is none; the budget says whether the walk was cut.
	closing := func(open int) int {
		depth := 0
		for k := open; k < len(toks); k++ {
			if !spend() {
				return -1
			}
			if isOpTok(toks, k, "(") {
				depth++
			} else if isOpTok(toks, k, ")") {
				if depth--; depth == 0 {
					return k
				}
			}
		}
		return -1
	}
	depth, inList := 0, false
	for j, t := range toks {
		switch {
		case isOpTok(toks, j, "::"):
			k := j - 1
			for k >= 0 && isOpTok(toks, k, ")") {
				if !spend() {
					return nil, false, steps
				}
				k--
			}
			if k >= 0 && !add(k, k+1) {
				return nil, false, steps
			}
		case t.kind == rtWord && t.text == "cast" && isOpTok(toks, j+1, "("):
			if end := closing(j + 1); end > 0 && !add(j+2, end) {
				return nil, false, steps
			}
		case t.kind == rtWord && t.text == "else":
			for k, nested := j+1, 0; k < len(toks); k++ {
				if !spend() {
					return nil, false, steps
				}
				if toks[k].kind == rtWord && toks[k].text == "case" {
					nested++
				} else if toks[k].kind == rtWord && toks[k].text == "end" {
					if nested == 0 {
						if !add(j+1, k) {
							return nil, false, steps
						}
						break
					}
					nested--
				}
			}
		case isOpTok(toks, j, "("):
			if inList && depth == 0 && j+1 < len(toks) && toks[j+1].kind == rtWord && toks[j+1].text == "select" {
				if end := closing(j); end > 0 && !add(j+1, end) {
					return nil, false, steps
				}
			}
			depth++
		case isOpTok(toks, j, ")"):
			depth--
		case depth == 0 && t.kind == rtWord && t.text == "select":
			inList = true
		case depth == 0 && t.kind == rtWord && t.text == "from":
			inList = false
		}
		if cut {
			return nil, false, steps
		}
	}
	return out, true, steps
}

// outputNamesLimit is the most steps outputNames takes over toks.
func outputNamesLimit(toks []rtToken) int { return 4*len(toks) + 1024 }

// bareOrderKey is a key without its direction, its NULLS and the parentheses
// around it.
func bareOrderKey(key []rtToken) []rtToken {
	word := func(j int, w string) bool { return j >= 0 && j < len(key) && key[j].kind == rtWord && key[j].text == w }
	if n := len(key); n >= 3 && word(n-2, "nulls") && (word(n-1, "first") || word(n-1, "last")) {
		key = key[:n-2]
	}
	if n := len(key); n >= 2 && (word(n-1, "asc") || word(n-1, "desc")) {
		key = key[:n-1]
	}
	for len(key) >= 3 && isOpTok(key, 0, "(") && closingParen(key, 0) == len(key)-1 {
		key = key[1 : len(key)-1]
	}
	return key
}

// onlyCopyTextKey is present in the screen's types when a table the statement
// reads has a column the copy holds as text and the source does not (an enum,
// a domain, a range, money, inet, json, a numeric too wide for the copy); the
// key followed by a column's name marks that column. No name a statement
// writes starts with it.
const onlyCopyTextKey = colsKey + relKey + relKey

// onlyTextOnCopy reports whether the copy holds c as text and the source as
// something else. A column whose source type the snapshot does not record
// counts as one.
func onlyTextOnCopy(c CatalogColumn) bool {
	if !anyTextType([]string{strings.ToUpper(c.Type)}) {
		return false
	}
	switch c.SourceType {
	case oidText, oidVarchar, oidName, oidBpchar:
		return false
	}
	return true
}

// orderKeyHidesColumn reports whether an ORDER BY key is a position or a name
// no column has (an alias): it may stand for any column of the answer, a
// char(n) one included, which stays with the source when it is sorted.
func orderKeyHidesColumn(key []rtToken, types map[string][]string) bool {
	if len(key) != 1 {
		return false
	}
	switch key[0].kind {
	case rtNumber:
		return true
	case rtWord, rtQuoted:
		_, known := types[key[0].text]
		return !known
	}
	return false
}
