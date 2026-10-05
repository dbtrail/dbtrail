package readrouter

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// A plan that reads only its result (#2115).
//
// Two questions the policy asks have the same answer in the plan. "Can the
// source return the first n rows by reading about n?" and "how many rows
// does the statement return?" are both settled when the plan is ONE table,
// read alone, where every row the access path reads is a row of the result:
//
//   - the table or an index read whole with no condition at all, or
//   - an index read by key or by range (ref, eq_ref, const, range) where the
//     whole condition is the key's own bounds: nothing is left to check row
//     by row.
//
// Then a LIMIT with no sort stops the read once it has its rows, whatever
// the size of the range behind it; and the plan's row estimate for the table
// is an estimate of the result, off by what an index dive is off (measured
// 0.6x to 3.2x) and not by a guessed selectivity.
//
// What tells it is measured, in testdata/limits (MySQL 8.4.9, MariaDB
// 11.4.13 and 10.11.19 over the same 2,000,000 rows):
//
//   - `filtered` alone does not. MySQL reports 100 for a condition it has no
//     estimate for: a function of a column, arithmetic, a column compared
//     with a column (a07, a08, a28), each of which read the whole range for
//     500 rows. MariaDB reports 100 for a filter no index serves at all (b05).
//     So the condition's text is read, and only `column <comparison>
//     constant` joined by AND is taken.
//   - Every column of the condition must be one of the key parts the plan
//     says it used: a condition on another column is checked row by row
//     (a06, a26, p10), and so is one on a later column of the same key
//     (p01, p02: `used_key_parts` lists the first column only).
//   - A range on a column that is not the last key part used leaves the
//     later parts checked entry by entry, were a server to list them: only
//     the last part may carry a range.
//   - A condition MySQL keeps in attached_condition under a secondary index
//     that does not cover the statement is one it could not push down to the
//     index, which is what an index on a PREFIX of the column looks like
//     (p04 to p06: 222,000 rows read for none). Under the primary key, where
//     nothing is ever pushed down, a text value could be that same prefix
//     case and is refused; a number or a date cannot.
//   - A full scan, or an index walked whole, with ANY condition is never
//     this: the rows that match may all sit at the end (a15: 1,668,158 rows
//     read for 500).
//
// The reader takes MySQL's spelling of a condition (names in backticks). A
// MariaDB range read is not recognised, and does not need to be: with no
// cost in its plans, a range read was never the copy's. On MariaDB the
// property is seen for a table or an index read whole.
//
// Anything the reader does not know (a node, an access type, an operator)
// leaves the plan unrecognised, and the statement where it was.

// planWrappers are the nodes that may sit between the top block and its one
// table without changing what is read: MySQL's ordering_operation (which
// says by using_filesort whether it sorts), MariaDB's nested_loop list and
// its read_sorted_file/filesort pair.
var planWrappers = map[string]bool{"ordering_operation": true, "nested_loop": true, "read_sorted_file": true, "filesort": true}

// planNotes are the object- and list-valued keys that describe a node and
// hold no part of the plan.
var planNotes = map[string]bool{"cost_info": true, "possible_keys": true, "used_key_parts": true, "used_columns": true, "ref": true}

// readIsResult fills Plan.ReadIsResult and Plan.ResultRows from the top
// query block.
func readIsResult(qb map[string]any, p *Plan) {
	var table map[string]any
	if !loneTable(qb, &table) || table == nil {
		return
	}
	// Both engines say 100 when the access path alone decides the rows.
	if _, ok := table["filtered"]; !ok || number(table["filtered"]) != 100 {
		return
	}
	index, _ := table["index_condition"].(string)
	attached, _ := table["attached_condition"].(string)
	switch at, _ := table["access_type"].(string); at {
	case "ALL", "index":
		if index != "" || attached != "" {
			return
		}
	case "range", "ref", "eq_ref", "const":
		if !keyServes(table, index, attached) {
			return
		}
	default:
		return
	}
	// MySQL cuts rows_examined_per_scan to the LIMIT under an ordered index
	// walk and keeps the whole count in rows_produced_per_join; MariaDB has
	// the one number.
	rows := max(number(table["rows_examined_per_scan"]), number(table["rows_produced_per_join"]), number(table["rows"]))
	p.ReadIsResult, p.ResultRows = true, int64(rows)
}

// loneTable walks v, which must hold exactly one table under nothing but
// planWrappers; the table is left in *table. False for a second table and
// for any object or list it does not know: a subquery, a UNION, a derived
// table, a grouping, a window, a temporary table, a partition list.
func loneTable(v any, table *map[string]any) bool {
	switch x := v.(type) {
	case map[string]any:
		for key, child := range x {
			switch c := child.(type) {
			case bool:
				if key == "using_temporary_table" && c {
					return false
				}
			case map[string]any, []any:
				switch {
				case planNotes[key]:
				case planWrappers[key]:
					if !loneTable(c, table) {
						return false
					}
				case key == "table":
					t, _ := c.(map[string]any)
					if t == nil || *table != nil {
						return false
					}
					for k, tc := range t {
						switch on := tc.(type) {
						case map[string]any, []any:
							if !planNotes[k] {
								return false
							}
						case bool:
							// MySQL's skip scan and loose index scan read an
							// index in jumps: not the read described above.
							if on && strings.HasPrefix(k, "using_index_for_") {
								return false
							}
						}
					}
					*table = t
				default:
					return false
				}
			}
		}
	case []any:
		for _, child := range x {
			if !loneTable(child, table) {
				return false
			}
		}
	}
	return true
}

// keyServes reports whether the conditions of a table read by key or by
// range are all bounds of the key it is read by.
func keyServes(table map[string]any, index, attached string) bool {
	var parts []string
	list, _ := table["used_key_parts"].([]any)
	for _, v := range list {
		s, ok := v.(string)
		if !ok {
			return false
		}
		parts = append(parts, s)
	}
	if len(parts) == 0 {
		return false
	}
	covering, _ := table["using_index"].(bool)
	key, _ := table["key"].(string)
	if attached != "" && !covering && key != "PRIMARY" {
		return false
	}
	for i, cond := range []string{index, attached} {
		if cond == "" {
			continue
		}
		preds, ok := readCondition(cond)
		if !ok {
			return false
		}
		for _, pr := range preds {
			at := slices.Index(parts, pr.col)
			switch {
			case at < 0:
				return false
			case at < len(parts)-1 && !pr.point:
				return false
			case pr.str && i == 1 && !covering:
				return false
			}
		}
	}
	return true
}

// condPred is one `column <comparison> constant` of a condition.
type condPred struct {
	col   string // the column's own name, without its table
	point bool   // =, <=> or IN: a value, not a range
	str   bool   // compared with a plain text value
}

// condToken is one token of a condition as MySQL prints it.
var condToken = regexp.MustCompile("^(?:" +
	"(?P<name>(?:`(?:[^`]|``)*`\\.)*`(?:[^`]|``)*`)" + // `db`.`table`.`column`
	"|(?P<typed>(?:TIMESTAMP|DATE|TIME)'(?:[^'\\\\]|''|\\\\.)*')" +
	"|(?P<text>'(?:[^'\\\\]|''|\\\\.)*')" +
	"|(?P<num>-?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][-+]?\\d+)?)" +
	"|(?P<op><=>|<=|>=|=|<|>)" +
	"|(?P<word>and|between|in)\\b" +
	"|(?P<punct>[(),])" +
	")")

// condNamePart is one backtick-quoted part of a name.
var condNamePart = regexp.MustCompile("`(?:[^`]|``)*`")

// readCondition reads a condition as MySQL prints it in a plan:
// comparisons of a column with a constant (=, <=>, <, <=, >, >=, BETWEEN,
// IN), joined by AND, in any parentheses. ok is false for anything else: a
// function, arithmetic, OR, NOT, <>, IS NULL, LIKE, a subquery, a column
// on both sides, the constant on the left, a cached expression.
func readCondition(cond string) (preds []condPred, ok bool) {
	type token struct{ kind, text string }
	var toks []token
	names := condToken.SubexpNames()
	for s := strings.TrimSpace(cond); s != ""; s = strings.TrimSpace(s) {
		m := condToken.FindStringSubmatch(s)
		if m == nil {
			return nil, false
		}
		for i := 1; i < len(m); i++ {
			if m[i] != "" {
				toks = append(toks, token{names[i], m[i]})
				break
			}
		}
		s = s[len(m[0]):]
	}
	pos := 0
	peek := func() token {
		if pos < len(toks) {
			return toks[pos]
		}
		return token{}
	}
	take := func(kind, text string) bool {
		if t := peek(); t.kind == kind && (text == "" || t.text == text) {
			pos++
			return true
		}
		return false
	}
	// constant takes one constant and says whether it is a plain text.
	constant := func() (str, ok bool) {
		switch t := peek(); t.kind {
		case "text":
			pos++
			return true, true
		case "typed", "num":
			pos++
			return false, true
		}
		return false, false
	}
	var and func() bool
	term := func() bool {
		if take("punct", "(") {
			return and() && take("punct", ")")
		}
		name := peek()
		if !take("name", "") {
			return false
		}
		parts := condNamePart.FindAllString(name.text, -1)
		part := parts[len(parts)-1]
		pr := condPred{col: strings.ReplaceAll(part[1:len(part)-1], "``", "`")}
		switch op := peek(); {
		case take("op", ""):
			str, ok := constant()
			if !ok {
				return false
			}
			pr.point, pr.str = op.text == "=" || op.text == "<=>", str
		case take("word", "between"):
			lo, ok1 := constant()
			if !ok1 || !take("word", "and") {
				return false
			}
			hi, ok2 := constant()
			if !ok2 {
				return false
			}
			pr.str = lo || hi
		case take("word", "in"):
			if !take("punct", "(") {
				return false
			}
			for {
				str, ok := constant()
				if !ok {
					return false
				}
				pr.str = pr.str || str
				if !take("punct", ",") {
					break
				}
			}
			if !take("punct", ")") {
				return false
			}
			pr.point = true
		default:
			return false
		}
		preds = append(preds, pr)
		return true
	}
	and = func() bool {
		for {
			if !term() {
				return false
			}
			if !take("word", "and") {
				return true
			}
		}
	}
	if !and() || pos != len(toks) || len(preds) == 0 {
		return nil, false
	}
	return preds, true
}

// resultLimit is a LIMIT of numbers that ends the statement: "LIMIT n",
// "LIMIT offset, n", "LIMIT n OFFSET offset".
var resultLimit = regexp.MustCompile(`(?i)\blimit\s+(\d+)(?:\s*,\s*(\d+)|\s+offset\s+(\d+))?\s*;?\s*$`)

// resultRows is the plan's estimate of the rows the statement returns, or 0
// when there is none to trust: the plan does not read only its result
// (Plan.ReadIsResult), or the statement returns something else than the rows
// it reads (an aggregate, GROUP BY, DISTINCT, a window: unboundedWork), or
// its LIMIT is not a number known here (a placeholder). A LIMIT cuts the
// estimate: it bounds the result whatever the plan says.
func resultRows(stmt string, p Plan) int64 {
	if !p.ReadIsResult || hintComment.MatchString(stmt) {
		return 0
	}
	blanked, _, _, _ := scrub(stmt)
	if unboundedWork.MatchString(blanked) {
		return 0
	}
	est := uint64(max(p.ResultRows, 0))
	switch len(anyLimit.FindAllStringIndex(blanked, 2)) {
	case 0:
		return int64(est)
	case 1:
	default:
		return 0
	}
	m := resultLimit.FindStringSubmatch(blanked)
	if m == nil {
		return 0
	}
	limit, offset := m[1], m[3]
	if m[2] != "" {
		limit, offset = m[2], m[1]
	}
	n, err := strconv.ParseUint(limit, 10, 64)
	if err != nil {
		return 0
	}
	if offset != "" {
		off, err := strconv.ParseUint(offset, 10, 64)
		if err != nil {
			return 0
		}
		est -= min(off, est)
	}
	return int64(min(n, est))
}
