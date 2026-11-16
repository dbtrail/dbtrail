package readrouter

import (
	"regexp"
	"strconv"
	"strings"
)

// VetoAvg is why AvgVeto keeps a statement on MySQL.
const VetoAvg = "AVG of something other than a whole-number column, or an AVG used other than as a whole column of the select list " +
	"(a DECIMAL with div_precision_increment more digits than its argument on MySQL, computed and compared on more digits than it prints)"

// VetoAvgLeft is why ForCopyOpts refuses a statement under MySQLAvg: an AVG
// call it did not write as the copy's macro (a comment before its
// parenthesis, a placeholder inside it in a prepared statement's piece,
// db.avg(...)), which the copy's own avg would answer as a DOUBLE.
const VetoAvgLeft = "AVG not written as the copy's exact average (the copy's own AVG is a DOUBLE)"

var (
	// avgOpen opens an AVG call, as MySQL reads one: the white space it takes
	// before the parenthesis (callOpen). callsOf keeps out a longer name.
	avgOpen = regexp.MustCompile(`(?i)avg` + callOpen)
	// avgDistinct is DISTINCT at the start of an AVG's argument, as a word:
	// AVG(DISTINCT i) and AVG(DISTINCT(i)).
	avgDistinct = regexp.MustCompile(`(?i)^distinct(?:[^\pL\pN_$]|$)`)
	// avgAlias is what may follow an AVG's closing parenthesis in its select
	// item: nothing, or an alias with or without AS. A string literal (blanked
	// to '' in the shape) is not read as an alias: its name is not known.
	avgAlias = regexp.MustCompile(`(?i)^(?:as[\s\v]+)?("[^"]*"|[\pL\pN_$]+)$`)
	// setOperation is a UNION, INTERSECT or EXCEPT: the copy and MySQL type a
	// column of the result from every branch.
	setOperation = regexp.MustCompile(`(?i)\b(?:union|intersect|except)\b`)
	// nameToken is a quoted name or a word.
	nameToken = regexp.MustCompile(`"[^"]*"|[\pL\pN_$]+`)
	// listClause opens a GROUP BY or an ORDER BY, whose items may name a
	// select-list column by its position.
	listClause = regexp.MustCompile(`(?i)\b(?:group|order)[\s\v]+by\b`)
	// listClauseEnd is a word that ends such a clause.
	listClauseEnd = regexp.MustCompile(`(?i)\b(?:having|group[\s\v]+by|order[\s\v]+by|limit|window|for|lock|into|procedure)\b|;`)
	// sortDirection is ASC or DESC after an ORDER BY item.
	sortDirection = regexp.MustCompile(`(?i)[\s\v]+(?:asc|desc)$`)
	// selectWord is the SELECT a statement opens with.
	selectWord = regexp.MustCompile(`(?i)^[\s\v]*select(?:[^\pL\pN_$]|$)`)
)

// AvgVeto says why the copy must not answer a routed statement because of an
// AVG call, or "" when it can (#2920). Under routing the copy is sent
// AVG(col) as its macro (sqlsandbox's avgMacros), which computes MySQL's
// AVG of whole numbers exactly: a DECIMAL with four decimals
// (div_precision_increment, which the routing port only lets through at its
// default of 4), rounded half away from zero. Measured on MySQL 8.0.46 and
// MariaDB 10.11.19, the same text and the same type and decimals on both:
// AVG of 1, 2 and 4 is 2.3333, of one 1 among 32 rows 0.0313 and of one -1
// -0.0313, of BIGINT values near 2^63 and BIGINT UNSIGNED near 2^64 exact to
// the last digit, of no row and of NULLs only NULL.
//
// shape is Shape of the statement; whole are the whole-number columns of the
// tables it reads (TINYINT to BIGINT, signed or not, and BOOL), other all
// their other columns, the ones whose type is not known among them.
//
// The copy may answer only when every AVG is a whole column of the select
// list (AVG(x), with or without an alias), its argument a bare column in
// whole and in no table's other, with or without DISTINCT. MySQL computes an
// AVG on more digits than it prints (AVG of 1, 2 and 4 is 2.333333333
// inside): AVG(k) * 3 is 7.0000 there, AVG(k) > 2.3333 is true on MySQL 8.0
// and false on MariaDB, and two averages that print alike sort apart. So an
// AVG in arithmetic, in a comparison, under another call, in HAVING or
// ORDER BY, named there by its alias or its position, or as a window, stays
// on MySQL. So do an AVG over a DECIMAL (its scale plus four, which the
// macro does not have), a FLOAT, a DOUBLE, text, a date or an expression
// (AVG(LENGTH(c))), whose type the router does not read; a statement with a
// subquery, a WITH or a UNION, where a name may be the statement's own; and a
// SELECT DISTINCT, which compares the averages.
func AvgVeto(shape string, whole, other []string) string {
	locs := callsOf(avgOpen, shape)
	if len(locs) == 0 {
		return ""
	}
	if subquery.MatchString(shape) || setOperation.MatchString(shape) {
		return VetoAvg
	}
	m := selectWord.FindStringIndex(shape)
	if m == nil {
		return VetoAvg
	}
	// Right after the word SELECT.
	listStart := m[0] + strings.Index(strings.ToLower(shape[m[0]:m[1]]), "select") + len("select")
	items, listEnd := selectItems(shape, listStart)
	known := knownNames(whole, other)
	aliases := map[string]bool{}
	positions := map[int]bool{}
	next := 0
	for _, l := range locs {
		if l[0] < listStart || l[0] >= listEnd || (l[0] > 0 && (shape[l[0]-1] == '.' || shape[l[0]-1] == '"')) {
			// Outside the select list, or db.avg(...), a stored function.
			return VetoAvg
		}
		for next < len(items) && items[next][1] <= l[0] {
			next++
		}
		if next == len(items) {
			return VetoAvg
		}
		item := items[next]
		if strings.TrimLeft(shape[item[0]:l[0]], " \t\n\r\f\v") != "" {
			// Something before the AVG in its item: DISTINCT, a sign, an
			// operand, an enclosing call.
			return VetoAvg
		}
		body, ok := closingBody(shape, l[1])
		if !ok {
			return VetoAvg
		}
		rest := strings.TrimSpace(shape[l[1]+len(body)+1 : item[1]])
		if rest != "" {
			a := avgAlias.FindStringSubmatch(rest)
			if a == nil || strings.EqualFold(a[1], "over") {
				return VetoAvg
			}
			aliases[sameName(unquote(a[1]))] = true
		}
		positions[next+1] = true
		arg := strings.TrimSpace(body)
		if d := avgDistinct.FindStringIndex(arg); d != nil {
			arg = strings.TrimSpace(arg[len("distinct"):])
		}
		c := columnRef.FindStringSubmatch(unwrap(arg))
		if c == nil || !known[sameName(unquote(c[1]))] {
			return VetoAvg
		}
		next++
	}
	tail := shape[listEnd:]
	for _, tok := range nameToken.FindAllString(tail, -1) {
		if strings.HasPrefix(tok, `"`) && strings.Contains(tok, "(") {
			// ORDER BY `AVG(k)`: the column MySQL names after the call.
			return VetoAvg
		}
		if aliases[sameName(unquote(tok))] {
			return VetoAvg
		}
	}
	for _, c := range listClause.FindAllStringIndex(tail, -1) {
		clause := tail[c[1]:]
		if e := listClauseEnd.FindStringIndex(clause); e != nil {
			clause = clause[:e[0]]
		}
		parts, _ := topLevelArgs(clause)
		for _, p := range parts {
			p = unwrap(strings.TrimSpace(sortDirection.ReplaceAllString(p, "")))
			if n, err := strconv.Atoi(p); err == nil && positions[n] {
				return VetoAvg
			}
		}
	}
	return ""
}

// AvgVeto is AvgVeto over this shape.
func (s StatementShape) AvgVeto(whole, other []string) string {
	return AvgVeto(string(s), whole, other)
}

// selectItems splits the select list of shape, which starts at from (right
// after SELECT), at its top-level commas, up to the FROM at the top level or
// the end. Each item is its [start, end) in shape; end is where the list
// stops. A quoted name and a blanked string are read whole.
func selectItems(shape string, from int) (items [][2]int, end int) {
	depth, start := 0, from
	for i := from; i < len(shape); i++ {
		switch c := shape[i]; {
		case c == '"' || c == '\'':
			if k := strings.IndexByte(shape[i+1:], c); k >= 0 {
				i += k + 1
			}
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			items = append(items, [2]int{start, i})
			start = i + 1
		case depth == 0 && (c == 'f' || c == 'F') && (i == 0 || !wordByte(shape[i-1])) &&
			len(shape) >= i+4 && strings.EqualFold(shape[i:i+4], "from") && (len(shape) == i+4 || !wordByte(shape[i+4])):
			return append(items, [2]int{start, i}), i
		}
	}
	return append(items, [2]int{start, len(shape)}), len(shape)
}

// mysqlAvgCall is MySQL's AVG(x) or AVG(DISTINCT x), read from rest, the text
// right after the word AVG, as the copy's macro over the same argument:
// _bintrail_avg(x) or _bintrail_avg_distinct(x). n is how many bytes of rest
// the call takes. ok is false when rest does not hold one argument between
// parentheses (a comment before the parenthesis, a piece of a prepared
// statement cut at a placeholder): the call is then left as written, and
// ForCopyOpts refuses it (VetoAvgLeft).
func mysqlAvgCall(rest string) (call string, n int, ok bool) {
	args, n, ok := callArgs(rest)
	if !ok || len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return "", 0, false
	}
	arg := argText(args[0])
	if avgDistinct.MatchString(arg) {
		return "_bintrail_avg_distinct(" + argText(arg[len("distinct"):]) + ")", n, true
	}
	return "_bintrail_avg(" + arg + ")", n, true
}

// avgLeft reports whether text, written for the copy, still calls AVG
// (VetoAvgLeft). A name after a dot counts: db.avg(x) is a stored function
// on MySQL and the copy's avg on a column there.
func avgLeft(text string) bool {
	return len(callsOf(avgOpen, scan(text).blanked)) > 0
}
