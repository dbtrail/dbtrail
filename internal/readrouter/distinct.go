package readrouter

import (
	"regexp"
	"strings"
)

// VetoDistinct is why DistinctVeto keeps a statement on MySQL.
const VetoDistinct = "DISTINCT inside an aggregate over text, a date, an expression or a column not known to be a number (not folded by the copy's collation)"

var (
	// distinctAgg opens an aggregate whose argument starts with DISTINCT.
	distinctAgg = regexp.MustCompile(`(?i)\b(count|sum|avg|min|max|group_concat)\s*\(\s*distinct(?:row)?\b`)
	// columnRef is a bare column, qualified by a table and a database or
	// not, each part a word or a double-quoted name.
	columnRef = regexp.MustCompile(`^(?:(?:"[^"]*"|[\pL\pN_$]+)\s*\.\s*){0,2}("[^"]*"|[\pL\pN_$]+)$`)
)

// DistinctVeto says why the copy must not answer a statement because of an
// aggregate over DISTINCT values, or "" when it can (#2532). The copy's
// collation does not reach DISTINCT inside an aggregate, so over text
// COUNT(DISTINCT status) counts 'Paid' and 'paid' twice where MySQL counts
// them once. Over a whole or exact number both count the same values. shape
// is Shape of the statement (DISTINCTROW, MySQL's other spelling, is read
// too); safe are the columns of the tables it reads whose type is such a
// number (baseline.DistinctColumns), other
// all their other columns, the ones whose type is not known among them.
//
// The copy may answer when every DISTINCT argument is a bare column whose
// name is in safe and in no table's other. Everything else stays on MySQL:
// an expression, a call, a literal; a name the statement may define itself
// (any subquery, derived table or WITH, where an alias can hold text under
// a number's name); and an aggregate in GROUP BY, HAVING or ORDER BY, where
// MySQL reads a select-list alias before the column. GROUP_CONCAT is kept
// whatever its argument (MySQL cuts it at group_concat_max_len).
func DistinctVeto(shape string, safe, other []string) string {
	locs := distinctAgg.FindAllStringSubmatchIndex(shape, -1)
	if len(locs) == 0 {
		return ""
	}
	if subquery.MatchString(shape) {
		return VetoDistinct
	}
	if c := aliasClause.FindStringIndex(shape); c != nil && c[0] < locs[len(locs)-1][0] {
		return VetoDistinct
	}
	notSafe := make(map[string]bool, len(other))
	for _, n := range other {
		notSafe[sameName(n)] = true
	}
	known := make(map[string]bool, len(safe))
	for _, n := range safe {
		if f := sameName(n); !notSafe[f] {
			known[f] = true
		}
	}
	for _, l := range locs {
		if strings.EqualFold(shape[l[2]:l[3]], "group_concat") {
			return VetoDistinct
		}
		body, ok := closingBody(shape, l[1])
		if !ok {
			return VetoDistinct
		}
		for arg := range strings.SplitSeq(body, ",") {
			m := columnRef.FindStringSubmatch(unwrap(strings.TrimSpace(arg)))
			if m == nil {
				return VetoDistinct
			}
			name := m[1]
			if strings.HasPrefix(name, `"`) {
				name = name[1 : len(name)-1]
			}
			if !known[sameName(name)] {
				return VetoDistinct
			}
		}
	}
	return ""
}

// DistinctVeto is DistinctVeto over this shape.
func (s StatementShape) DistinctVeto(safe, other []string) string {
	return DistinctVeto(string(s), safe, other)
}

// closingBody is the text from i to the parenthesis that closes the one
// opened before i, and whether there is one.
func closingBody(s string, i int) (string, bool) {
	depth := 1
	for j := i; j < len(s); j++ {
		switch s[j] {
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return s[i:j], true
			}
		}
	}
	return "", false
}

// unwrap takes off parentheses around the whole of s: DISTINCT(id) is id.
func unwrap(s string) string {
	for len(s) >= 2 && s[0] == '(' {
		body, ok := closingBody(s, 1)
		if !ok || len(body) != len(s)-2 {
			return s
		}
		s = strings.TrimSpace(body)
	}
	return s
}
