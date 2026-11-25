package pgshim

import (
	"fmt"
	"strconv"
	"strings"
)

// A char(n) the port's view reads without its padding (#3219, part 3b).
//
// The view trims a char(n)'s trailing blanks (TrimmedOnCopy), so the copy
// holds 'ab' where PostgreSQL holds 'ab   '. Most of what PostgreSQL does
// with a char(n) then comes out the same, but not all of it: PostgreSQL
// compares a char(n) with a string ignoring the string's trailing blanks as
// well, LIKE sees the padding, a varchar on the other side is compared as a
// char(n), and the lengths and positions the copy computes are of another
// type. So such a column is not marked (screenTypes): it is text to the rest
// of the screen, under every rule of text, and trimmedUse lets through only
// these uses of it, each measured on PostgreSQL 16, sending every other to
// the source:
//
//   - alone, where its value is only written out, grouped or counted: an
//     item of a SELECT list that is not a value of something else (the main
//     statement's, a table in FROM's, a CTE's, an EXISTS'), a key of GROUP
//     BY or PARTITION BY, the main statement's ORDER BY (where the order of
//     text is the copy's by the product's decision), count(c), count(DISTINCT
//     c), DISTINCT;
//   - IS [NOT] NULL;
//   - = and <> (!=) with a plain string whose value has no trailing blank;
//   - [NOT] IN a list of such strings;
//   - [NOT] LIKE a plain pattern that ends in %, with no _, blank or \;
//   - the first argument of substr or substring.
//
// Not let through, among others: another column on the other side (a
// varchar there is compared as a char(n), and which is which is not told
// well through aliases and USING), ||, length, char_length and position
// (the copy types them int8, PostgreSQL int4), a sort that is not the main
// statement's (a window's, an aggregate's, a subquery's), a cast, and any
// function or operator not named above.
//
// What a statement relies on is the catalog's, which is a snapshot's: the
// statement itself runs on the snapshot of its moment. The port hands the
// copy the names it took as trimmed (RunOptions.Trimmed), and the copy
// refuses the statement, which then goes to the source, when a table it
// reads holds one of those names as a char(n) its view does not trim.

// trimmedKey is, alone, the key under which the screen's types say a table
// a statement reads holds a trimmed char(n), and, before a lowercased name,
// that a column of that name is one. No name a statement writes starts with
// it.
const trimmedKey = "\x02"

// trimmedName reports whether a column named n (lowercased) is a char(n)
// the view trims, in a table the statement reads.
func trimmedName(types map[string][]string, n string) bool {
	_, ok := types[trimmedKey+n]
	return ok
}

// TrimmedNames are the names (lowercased) of the trimmed char(n) columns a
// statement names, in the order it first names them: what it relies on the
// copy's view to trim (RunOptions.Trimmed).
func trimmedNames(toks []rtToken, types map[string][]string) []string {
	if _, ok := types[trimmedKey]; !ok {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	for _, t := range toks {
		if t.kind != rtWord && t.kind != rtQuoted {
			continue
		}
		n := strings.ToLower(t.text)
		if trimmedName(types, n) && !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

// trimmedTerminators end the value on the other side of a comparison, or the
// column itself where it stands alone: nothing after it binds tighter.
var trimmedTerminators = map[string]bool{
	"and": true, "or": true, "then": true, "else": true, "end": true, "when": true,
	"order": true, "group": true, "having": true, "limit": true, "offset": true, "fetch": true, "window": true,
	"union": true, "intersect": true, "except": true,
	"from": true, "where": true, "on": true, "join": true, "inner": true, "left": true, "right": true, "full": true, "cross": true,
	"as": true, "asc": true, "desc": true, "nulls": true,
}

// trimmedBefore are the tokens after which a comparison of the column starts:
// nothing before it binds tighter.
var trimmedBefore = map[string]bool{
	"where": true, "and": true, "or": true, "not": true, "when": true, "on": true, "having": true, "select": true,
}

// trimmedUse says what in a statement uses a trimmed char(n) column in a way
// the copy may answer otherwise than PostgreSQL, "" when nothing does.
// mainOrder says whether the main statement's ORDER BY may sort by it: not
// on a connection with no source, where the copy's order of text would be
// the only answer.
func trimmedUse(toks []rtToken, types map[string][]string, mainOrder bool) string {
	if _, ok := types[trimmedKey]; !ok {
		return ""
	}
	depth := make([]int, len(toks)+1)
	d := 0
	for i, t := range toks {
		if t.kind == rtOp && (t.text == ")" || t.text == "]") {
			d--
		}
		depth[i] = d
		if t.kind == rtOp && (t.text == "(" || t.text == "[") {
			d++
		}
	}
	depth[len(toks)] = d
	at := func(i int) rtToken {
		if i < 0 || i >= len(toks) {
			return rtToken{kind: rtOp, text: ";"}
		}
		return toks[i]
	}
	word := func(i int, w string) bool { t := at(i); return t.kind == rtWord && t.text == w }
	isName := func(t rtToken) bool { return t.kind == rtWord || t.kind == rtQuoted }
	// ends reports whether the token at i ends the value before it.
	ends := func(i int) bool {
		t := at(i)
		if t.kind == rtOp {
			return t.text == ")" || t.text == "," || t.text == ";"
		}
		return t.kind == rtWord && trimmedTerminators[t.text]
	}
	// starts reports whether the token at i starts a comparison: nothing
	// before it binds tighter.
	starts := func(i int) bool {
		t := at(i)
		if t.kind == rtOp {
			return t.text == "(" || t.text == "," || t.text == ";"
		}
		return t.kind == rtWord && trimmedBefore[t.text]
	}
	// plain reports whether the token at i is a string the column may be
	// compared with: written plain ('...' is the only string the lexer
	// keeps), its value with no trailing blank, and no \, which a session
	// with standard_conforming_strings off reads as an escape.
	plain := func(i int) bool {
		t := at(i)
		return t.kind == rtString && !strings.HasSuffix(t.text, " ") && !strings.Contains(t.text, `\`)
	}
	// opens finds the ( that encloses the token at i, -1 when none.
	opens := func(i int) int {
		for j := i - 1; j >= 0; j-- {
			if depth[j] < depth[i] && isOpTok(toks, j, "(") {
				return j
			}
		}
		return -1
	}
	unions := false
	for _, t := range toks {
		if t.kind == rtWord && (t.text == "union" || t.text == "intersect" || t.text == "except") {
			unions = true
		}
	}

	for i, t := range toks {
		if !isName(t) || isOpTok(toks, i+1, ".") {
			continue
		}
		n := strings.ToLower(t.text)
		if !trimmedName(types, n) {
			continue
		}
		// The column with its qualifiers: [name .]... name.
		start := i
		for start >= 2 && isOpTok(toks, start-1, ".") && isName(at(start-2)) {
			start -= 2
		}
		what := fmt.Sprintf("the char(n) column %q", n)
		if unions {
			return what + " in a statement with UNION, INTERSECT or EXCEPT, where PostgreSQL may compare it with a varchar"
		}
		before, after := at(start-1), at(i+1)

		// The first argument of a call.
		// A ( after AND, OR, NOT, WHERE and the like groups; it does not call.
		if isOpTok(toks, start-1, "(") && at(start-2).kind == rtWord && !isOpTok(toks, start-3, ".") &&
			!trimmedBefore[at(start-2).text] && !trimmedClauses[at(start-2).text] && !trimmedTerminators[at(start-2).text] {
			switch at(start - 2).text {
			case "substr", "substring":
				if isOpTok(toks, i+1, ",") || word(i+1, "from") || word(i+1, "for") {
					continue
				}
			case "count":
				if isOpTok(toks, i+1, ")") {
					continue
				}
			}
			return what + fmt.Sprintf(" passed to %s()", at(start-2).text) + trimmedOther
		}
		if word(start-1, "distinct") && isOpTok(toks, start-2, "(") && word(start-3, "count") && isOpTok(toks, i+1, ")") {
			continue
		}

		// IS [NOT] NULL.
		if word(i+1, "is") {
			j := i + 2
			if word(j, "not") {
				j++
			}
			if word(j, "null") && starts(start-1) && ends(j+1) {
				continue
			}
			return what + " compared with IS" + trimmedOther
		}

		// = and <> with a plain string, either way round.
		cmp := func(t rtToken) bool { return t.kind == rtOp && (t.text == "=" || t.text == "<>" || t.text == "!=") }
		if cmp(after) {
			if plain(i+2) && ends(i+3) && starts(start-1) {
				continue
			}
			return what + " compared with something other than a plain string with no trailing blank" + trimmedOther
		}
		if cmp(before) {
			if plain(start-2) && starts(start-3) && ends(i+1) {
				continue
			}
			return what + " compared with something other than a plain string with no trailing blank" + trimmedOther
		}

		// [NOT] IN (plain strings), [NOT] LIKE 'x%'.
		j := i + 1
		if word(j, "not") {
			j++
		}
		if word(j, "in") {
			if !isOpTok(toks, j+1, "(") || !starts(start-1) {
				return what + " compared with IN" + trimmedOther
			}
			end := closingParen(toks, j+1)
			ok := end > j+2
			for k := j + 2; ok && k < end; k++ {
				if (k-j)%2 == 0 {
					ok = plain(k)
				} else {
					ok = isOpTok(toks, k, ",")
				}
			}
			if ok && ends(end+1) {
				continue
			}
			return what + " compared with IN, with something other than plain strings with no trailing blank" + trimmedOther
		}
		if word(j, "like") {
			p := at(j + 1)
			if p.kind == rtString && strings.HasSuffix(p.text, "%") && !strings.ContainsAny(p.text, "_ \\") && ends(j+2) && starts(start-1) {
				continue
			}
			return what + " compared with LIKE, with a pattern that does not end in % or holds _, a blank or \\" + trimmedOther
		}

		// Alone: written out, grouped, counted, or the main statement's sort.
		// The item ends after the column, or after an alias written with no
		// AS (with AS, AS itself ends it).
		if ends(i+1) || isName(after) && ends(i+2) {
			alias := !ends(i+1) || word(i+1, "as")
			if k := trimmedClause(toks, depth, start); k >= 0 {
				switch {
				case toks[k].text == "select":
					// Renamed in a table of FROM or a WITH, the column is read
					// again under the new name, qualified (s.y) or not, where
					// no rule here sees it: the source's. In the main list a
					// new name is read only by the main ORDER BY.
					if alias && depth[k] > 0 {
						break
					}
					if trimmedSelectList(toks, depth, opens, k) {
						if n := trimmedRowName(toks, opens(k)); n != "" && trimmedRowRead(toks, n) {
							return what + ", carried by " + strconv.Quote(n) + ", whose row is read whole: PostgreSQL writes it as a record with the padding, the copy as a struct without it"
						}
						continue
					}
				case alias:
				case word(k-1, "group"), word(k-1, "partition"):
					continue
				case word(k-1, "order") && depth[k] == 0:
					if mainOrder {
						continue
					}
					return what + " in ORDER BY, on a connection whose only answer would be the copy's order of text"
				}
			}
		}
		return what + ", used where its trailing blanks count" + trimmedOther
	}
	return ""
}

// trimmedOther closes a veto of trimmedUse.
const trimmedOther = " (PostgreSQL pads a char(n) and ignores the padding in some places and not in others; the copy holds it without the padding)"

// trimmedClauses are the words that start a clause of a SELECT, or a list
// that is not a SELECT list's or a BY's.
var trimmedClauses = map[string]bool{
	"select": true, "by": true, "from": true, "where": true, "on": true, "join": true, "having": true,
	"limit": true, "offset": true, "fetch": true, "window": true, "union": true, "intersect": true, "except": true,
	"values": true, "returning": true, "set": true, "using": true, "into": true,
}

// trimmedClause is where the clause the column at start is an item of
// begins: the SELECT of a SELECT list or the BY of a GROUP, PARTITION or
// ORDER BY, read back at the column's depth past the items before it; -1
// when it is in no such list (a WHERE, a call's arguments, an IN list).
func trimmedClause(toks []rtToken, depth []int, start int) int {
	if start <= 0 {
		return -1
	}
	p := toks[start-1]
	if !(p.kind == rtOp && p.text == "," || p.kind == rtWord && (p.text == "select" || p.text == "distinct" || p.text == "by")) {
		return -1
	}
	for j := start - 1; j >= 0; j-- {
		if depth[j] < depth[start] {
			return -1
		}
		if depth[j] > depth[start] || toks[j].kind != rtWord || !trimmedClauses[toks[j].text] {
			continue
		}
		if toks[j].text == "select" || toks[j].text == "by" {
			return j
		}
		return -1
	}
	return -1
}

// trimmedSelectList reports whether the SELECT at sel is one whose list's
// output is the answer or is read again by name: the main statement's, a
// table's in FROM or JOIN, a CTE's body (AS (...)), or an EXISTS'. Not a
// subquery that gives a value (IN (SELECT c ...), = (SELECT c ...)), whose
// other side PostgreSQL may compare as a char(n).
func trimmedSelectList(toks []rtToken, depth []int, opens func(int) int, sel int) bool {
	if depth[sel] == 0 {
		return true
	}
	o := opens(sel)
	if o < 0 || o != sel-1 || o == 0 {
		return false
	}
	t := toks[o-1]
	switch {
	case t.kind == rtWord && (t.text == "from" || t.text == "join" || t.text == "exists" || t.text == "as"):
		return true
	case t.kind == rtOp && t.text == ",":
		// A later item of a FROM list.
		for j := o - 2; j >= 0; j-- {
			if depth[j] < depth[o] {
				return false
			}
			if depth[j] == depth[o] && toks[j].kind == rtWord && trimmedClauses[toks[j].text] {
				return toks[j].text == "from"
			}
		}
	}
	return false
}

// trimmedFor is what a statement for the copy, text as the copy is handed
// it, relies on the view to trim (RunOptions.Trimmed). Read again from the
// catalog the screen read it with; when that cannot be read now (the copy
// is too busy, right after a new snapshot), TrimmedUnknown. It leaves what
// the screen said of the catalog as it was.
func (s *session) trimmedFor(text string) []string {
	if s.bound == nil || s.bound.Catalog == nil {
		return nil
	}
	busy := s.catalogBusy
	defer func() { s.catalogBusy = busy }()
	types, unread := s.columnTypes(text)
	if unread {
		return []string{TrimmedUnknown}
	}
	toks, _ := lexSQL(text)
	return trimmedNames(toks, types)
}

// trimmedRowName is the name a table of FROM or a WITH gives the rows of the
// SELECT whose list opens after the ( at open: the alias after its closing
// parenthesis, or the WITH's name before AS. "" when it gives none (EXISTS,
// the main statement), or one this does not read, which the source answers
// through other rules.
func trimmedRowName(toks []rtToken, open int) string {
	if open <= 0 {
		return ""
	}
	low := func(t rtToken) string {
		if t.kind == rtWord || t.kind == rtQuoted {
			return strings.ToLower(t.text)
		}
		return ""
	}
	if p := toks[open-1]; p.kind == rtWord && p.text == "as" {
		// WITH w AS (...): the name before AS. WITH w(y) AS (...) gives a
		// list, ")" there, and the WITH's other rules answer it.
		if open >= 2 {
			return low(toks[open-2])
		}
		return ""
	}
	end := closingParen(toks, open)
	if end < 0 || end+1 >= len(toks) {
		return ""
	}
	j := end + 1
	if toks[j].kind == rtWord && toks[j].text == "as" {
		j++
	}
	if j >= len(toks) || toks[j].kind == rtWord && aliasStop[toks[j].text] {
		return ""
	}
	return low(toks[j])
}

// trimmedRowRead reports whether the statement writes n where a value
// stands: not before a dot (s.c), not where it is given (after ")" or
// AS), not as a WITH's name (before AS), and not as the table read (after
// FROM or JOIN).
func trimmedRowRead(toks []rtToken, n string) bool {
	for i, t := range toks {
		if !(t.kind == rtWord || t.kind == rtQuoted) || strings.ToLower(t.text) != n {
			continue
		}
		if isOpTok(toks, i+1, ".") {
			continue
		}
		var p, pp rtToken
		if i > 0 {
			p = toks[i-1]
		}
		if i > 1 {
			pp = toks[i-2]
		}
		// Where it is given: after the ) of its body or AS, or as a WITH's
		// name (after WITH, RECURSIVE or the comma between two WITHs, before
		// AS). Where it is the table read: after the FROM of a query or a
		// JOIN, not the FROM of IS [NOT] DISTINCT FROM.
		given := p.kind == rtOp && p.text == ")" || p.kind == rtWord && p.text == "as"
		withName := (p.kind == rtWord && (p.text == "with" || p.text == "recursive") || p.kind == rtOp && p.text == ",") &&
			i+2 < len(toks) && toks[i+1].kind == rtWord && toks[i+1].text == "as" &&
			(isOpTok(toks, i+2, "(") || toks[i+2].kind == rtWord && (toks[i+2].text == "materialized" || toks[i+2].text == "not"))
		read := p.kind == rtWord && (p.text == "join" || p.text == "from" && !(pp.kind == rtWord && pp.text == "distinct"))
		if given || withName || read {
			continue
		}
		return true
	}
	return false
}
