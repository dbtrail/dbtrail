// Package sqlcompare runs the same read statements against a MySQL source and
// against the analytical copy (through the console's MySQL-protocol port) and
// says, per statement, whether the two answers are the same (issue #2038).
//
// It exists because the read router's one real risk is not a statement the
// copy refuses (MySQL runs it) but one the copy answers DIFFERENTLY without
// an error. The veto list in internal/readrouter is hand-made from the
// differences known so far; this tool is how the list grows: play a real
// workload through both sides and look at what came back different.
//
// Verdicts, per statement:
//
//	EQUAL         both sides returned the same rows
//	DIFFERENT     the rows differ (Kind says how: rows, order, case, null, precision, text, columns)
//	NOT_ON_COPY   the copy refused the statement (its error is the detail); the router would forward it
//	SOURCE_ERROR  MySQL itself refused the statement
//	INCONCLUSIVE  the copy cut its result at the port's cap, the source returned more rows than the
//	              tool reads, or the source's own answer changed between two runs (live writes)
//	SKIPPED       not a SELECT (this tool is read-only)
//
// Each result also carries what the router WOULD do with the statement
// (readrouter.Classify/Veto + EXPLAIN on the source), so the report can say
// which differences matter: a DIFFERENT statement the router would send to
// the copy is the finding; a DIFFERENT one it would forward anyway is a
// veto doing its job.
package sqlcompare

import (
	"fmt"
	"io"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// Verdict is the outcome of one statement's comparison.
type Verdict string

const (
	Equal        Verdict = "EQUAL"
	Different    Verdict = "DIFFERENT"
	NotOnCopy    Verdict = "NOT_ON_COPY"
	SourceError  Verdict = "SOURCE_ERROR"
	Inconclusive Verdict = "INCONCLUSIVE"
	Skipped      Verdict = "SKIPPED"
)

// Rows is one side's answer: column names and rows of cells, nil for NULL.
// Cells are the text the MySQL protocol carried, uninterpreted.
type Rows struct {
	Columns []string
	Rows    [][]*string
}

// ParseStatements reads statements from a file: one or more lines each,
// separated by `;`. The split follows quotes and comments, so a `;` inside
// a string literal or a comment does not end a statement. Ordinary
// comments are dropped; MySQL's executable and hint comments (`/*!`, `/*+`)
// are kept, since they change what the server does. Duplicates are dropped,
// keeping the first.
func ParseStatements(r io.Reader) ([]string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var out []string
	seen := map[string]bool{}
	for _, raw := range splitStatements(string(data)) {
		st := strings.TrimSpace(raw)
		if st == "" || seen[st] {
			continue
		}
		seen[st] = true
		out = append(out, st)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no statements found")
	}
	return out, nil
}

// splitStatements walks the text once, tracking what is open, and splits at
// every `;` that is outside a string, an identifier and a comment.
func splitStatements(text string) []string {
	var out []string
	var cur strings.Builder
	n := len(text)
	for i := 0; i < n; {
		c := text[i]
		switch {
		case c == '/' && i+1 < n && text[i+1] == '*':
			end := strings.Index(text[i+2:], "*/")
			if end < 0 {
				end = n - i - 2
			}
			if i+2 < n && (text[i+2] == '!' || text[i+2] == '+') {
				cur.WriteString(text[i : i+end+4])
			} else {
				cur.WriteByte(' ')
			}
			i += end + 4
		case c == '#', c == '-' && i+1 < n && text[i+1] == '-' && (i+2 >= n || text[i+2] == ' ' || text[i+2] == '\t' || text[i+2] == '\n' || text[i+2] == '\r'):
			nl := strings.IndexByte(text[i:], '\n')
			if nl < 0 {
				i = n
			} else {
				i += nl
			}
		case c == '\'' || c == '"' || c == '`':
			j := i + 1
			for j < n {
				if text[j] == '\\' && c != '`' {
					j += 2
					continue
				}
				if text[j] == c {
					if j+1 < n && text[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			if j >= n {
				j = n - 1
			}
			cur.WriteString(text[i : j+1])
			i = j + 1
		case c == ';':
			out = append(out, cur.String())
			cur.Reset()
			i++
		default:
			cur.WriteByte(c)
			i++
		}
	}
	out = append(out, cur.String())
	return out
}

// ReadOnlyVeto returns why a statement that Classify calls a SELECT must
// still not be run by this tool, or "": it writes (a WITH ... DELETE/UPDATE/
// INSERT, SELECT ... INTO OUTFILE/DUMPFILE/@var), takes a lock
// (FOR UPDATE, GET_LOCK), occupies the server (SLEEP, BENCHMARK, the
// replication waits) or holds a second statement after a `;`. A stored
// function with side effects cannot be screened by text; the docs say so.
func ReadOnlyVeto(stmt string) string {
	s := strings.ToLower(readrouter.Scrub(stmt))
	if strings.Contains(s, ";") {
		return "more than one statement"
	}
	if sideEffectFn.MatchString(s) {
		return "a function with side effects (lock, sleep, benchmark, replication wait)"
	}
	if intoRE.MatchString(s) {
		return "SELECT ... INTO (writes a file or a variable)"
	}
	if lockingRE.MatchString(s) {
		return "a locking read"
	}
	// A top-level write keyword after a WITH: MySQL 8 CTE-DML.
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && (i == 0 || !isWordByte(s[i-1])) && writeKeyword.MatchString(s[i:]) {
				return "a WITH that writes"
			}
		}
	}
	return ""
}

var (
	sideEffectFn = regexp.MustCompile(`\b(get_lock|release_lock|release_all_locks|is_free_lock|sleep|benchmark|master_pos_wait|source_pos_wait|wait_for_executed_gtid_set|wait_until_sql_thread_after_gtids)\s*\(`)
	intoRE       = regexp.MustCompile(`\binto\s+(outfile|dumpfile|@)`)
	lockingRE    = regexp.MustCompile(`\b(for\s+update|lock\s+in\s+share\s+mode|for\s+share)\b`)
	writeKeyword = regexp.MustCompile(`^(delete|update|insert|replace)\b`)
)

// TopLevelOrderBy reports whether the statement's outer query has an ORDER
// BY, which is when row order is part of the answer. An ORDER BY inside a
// subquery, a derived table or a window function (`OVER (... ORDER BY ...)`)
// orders nothing outside its parentheses.
func TopLevelOrderBy(stmt string) bool {
	s := strings.ToLower(readrouter.Scrub(stmt))
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case 'o':
			if depth == 0 && orderByAt.MatchString(s[i:]) && (i == 0 || !isWordByte(s[i-1])) {
				return true
			}
		}
	}
	return false
}

var orderByAt = regexp.MustCompile(`^order\s+by\b`)

func isWordByte(b byte) bool {
	return b == '_' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

// Compare says whether two answers to the same statement are the same, and
// if not, how. ordered says whether row order is part of the answer (the
// statement has a top-level ORDER BY); without it rows are compared as a
// multiset.
//
// Kind, when Different: "columns" (column counts differ, a column both sides
// name stands at another position, or a position holds two different plain
// column names: columnsDiffer), "rows" (row
// counts differ, or rows present on one side only), "order" (same rows,
// different order; with the NULL position named when that is the whole
// difference), and for a differing cell: "case" (equal ignoring case: a
// collation difference), "null" (NULL on one side only), "precision" (both
// numeric and equal once rounded to 4 decimals), "text" (anything else).
func Compare(src, cp Rows, ordered bool) (Verdict, string, string) {
	if len(src.Columns) != len(cp.Columns) {
		return Different, "columns", fmt.Sprintf("source returned %d columns, copy %d", len(src.Columns), len(cp.Columns))
	}
	if detail := columnsDiffer(src.Columns, cp.Columns); detail != "" {
		// Before the cells: a client that reads by position gets other
		// columns whatever they hold, and two columns with equal cells (or a
		// result with no rows) would otherwise read as equal (#2111).
		return Different, "columns", fmt.Sprintf("%s (source: %s; copy: %s)", detail,
			strings.Join(src.Columns, ", "), strings.Join(cp.Columns, ", "))
	}
	if len(src.Rows) != len(cp.Rows) {
		return Different, "rows", fmt.Sprintf("source returned %d rows, copy %d", len(src.Rows), len(cp.Rows))
	}
	if ordered {
		for i := range src.Rows {
			if kind, detail, ok := compareRow(src.Rows[i], cp.Rows[i], i); !ok {
				// Same multiset in a different order is an ordering
				// difference, not a value one.
				if sameMultiset(src.Rows, cp.Rows) {
					return Different, "order", "same rows in a different order" + nullOrderNote(src.Rows, cp.Rows)
				}
				return Different, kind, detail
			}
		}
		return Equal, "", ""
	}
	missing, extra := multisetDiff(src.Rows, cp.Rows)
	if len(missing) == 0 && len(extra) == 0 {
		return Equal, "", ""
	}
	// Name the difference by the closest pair: a source-only row and a
	// copy-only row that agree on every cell but one, which then says how
	// they differ (case, null, precision, text). No such pair: rows exist on
	// one side only.
	for _, m := range missing {
		for _, e := range extra {
			if cellsDiffering(m, e) != 1 {
				continue
			}
			// Only a RELATED pair names a kind (the same value spelled
			// differently); two unrelated values are a row missing and a
			// row extra, not a cell that changed.
			if kind, detail, _ := compareRow(m, e, -1); kind == "case" || kind == "precision" {
				return Different, kind, strings.Replace(detail, "row 0 ", "", 1)
			}
		}
	}
	return Different, "rows", fmt.Sprintf("%d row(s) only in the source (first: %s); %d only in the copy (first: %s)", len(missing), showRow(missing), len(extra), showRow(extra))
}

// columnsDiffer says how two answers with the same number of columns differ
// in their columns, or "" when they do not. Names compare without regard to
// case, MySQL's rule for column names. Two rules, and nothing else:
//
//   - A name both sides return must stand at the same positions on both. One
//     column named differently by each side does not switch this off for the
//     others.
//   - Where the two sides name a position differently and BOTH names are
//     plain identifiers, they are two different columns.
//
// A position where either name is not a plain identifier is left to the
// cells: each engine names an expression its own way (COUNT(*) and
// count_star(), n+1 and (n + 1)), and MySQL names a string literal by its
// value (abc) where DuckDB quotes it ('abc'), so one plain name against one
// that is not says nothing.
func columnsDiffer(src, cp []string) string {
	at := func(cols []string) map[string][]int {
		m := map[string][]int{}
		for i, c := range cols {
			k := strings.ToLower(c)
			m[k] = append(m[k], i)
		}
		return m
	}
	srcAt, cpAt := at(src), at(cp)
	for i, name := range src {
		there, ok := cpAt[strings.ToLower(name)]
		if !ok {
			continue
		}
		here := srcAt[strings.ToLower(name)]
		if slices.Equal(here, there) {
			continue
		}
		// Name the first position the two lists disagree on.
		j := there[0]
		for k := 0; k < len(here) && k < len(there); k++ {
			if here[k] != there[k] {
				i, j = here[k], there[k]
				break
			}
		}
		if len(here) != len(there) && i == j {
			return fmt.Sprintf("%s is %d column(s) on the source and %d on the copy", name, len(here), len(there))
		}
		return fmt.Sprintf("%s is column %d on the source and column %d on the copy", name, i+1, j+1)
	}
	for i := range src {
		if !strings.EqualFold(src[i], cp[i]) && plainName.MatchString(src[i]) && plainName.MatchString(cp[i]) {
			return fmt.Sprintf("column %d is %s on the source and %s on the copy", i+1, src[i], cp[i])
		}
	}
	return ""
}

// plainName is a column name that can only be a column or an alias: a bare
// identifier, not an expression's text.
var plainName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// multisetDiff returns the rows only in a (missing from b) and only in b.
func multisetDiff(a, b [][]*string) (onlyA, onlyB [][]*string) {
	count := map[string]int{}
	for _, r := range b {
		count[rowKey(r)]++
	}
	for _, r := range a {
		k := rowKey(r)
		if count[k] > 0 {
			count[k]--
			continue
		}
		onlyA = append(onlyA, r)
	}
	count = map[string]int{}
	for _, r := range a {
		count[rowKey(r)]++
	}
	for _, r := range b {
		k := rowKey(r)
		if count[k] > 0 {
			count[k]--
			continue
		}
		onlyB = append(onlyB, r)
	}
	return onlyA, onlyB
}

func cellsDiffering(a, b []*string) int {
	n := 0
	for c := range a {
		switch {
		case a[c] == nil && b[c] == nil:
		case a[c] == nil || b[c] == nil || *a[c] != *b[c]:
			n++
		}
	}
	return n
}

func showRow(rows [][]*string) string {
	if len(rows) == 0 {
		return "-"
	}
	parts := make([]string, len(rows[0]))
	for i, c := range rows[0] {
		parts[i] = show(c)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func compareRow(a, b []*string, row int) (kind, detail string, ok bool) {
	for c := range a {
		x, y := a[c], b[c]
		switch {
		case x == nil && y == nil:
			continue
		case x == nil || y == nil:
			return "null", fmt.Sprintf("row %d col %d: source %s, copy %s", row+1, c+1, show(x), show(y)), false
		case *x == *y:
			continue
		case strings.EqualFold(*x, *y):
			return "case", fmt.Sprintf("row %d col %d: source %s, copy %s (equal ignoring case: a collation difference)", row+1, c+1, show(x), show(y)), false
		case sameTo4Decimals(*x, *y):
			return "precision", fmt.Sprintf("row %d col %d: source %s, copy %s (equal once rounded to 4 decimals)", row+1, c+1, show(x), show(y)), false
		default:
			return "text", fmt.Sprintf("row %d col %d: source %s, copy %s", row+1, c+1, show(x), show(y)), false
		}
	}
	return "", "", true
}

// sameTo4Decimals: both numeric, equal once rounded to 4 decimals (what
// MySQL prints for AVG and division; the copy prints the full double) AND
// within 0.1% of each other, so two small numbers that both round to zero
// (0.00001 vs 0.00002, one twice the other) stay a value difference.
func sameTo4Decimals(a, b string) bool {
	x, err1 := strconv.ParseFloat(a, 64)
	y, err2 := strconv.ParseFloat(b, 64)
	if err1 != nil || err2 != nil {
		return false
	}
	if math.Round(x*10000) != math.Round(y*10000) {
		return false
	}
	return x == y || math.Abs(x-y) < 0.001*math.Max(math.Abs(x), math.Abs(y))
}

func show(v *string) string {
	if v == nil {
		return "NULL"
	}
	s := *v
	if len(s) > 60 {
		s = s[:57] + "..."
	}
	return strconv.Quote(s)
}

// rowKey is a row's canonical text, for multiset comparison. NULL and the
// empty string must not collide.
func rowKey(r []*string) string {
	var b strings.Builder
	for _, c := range r {
		if c == nil {
			b.WriteString("N;")
			continue
		}
		// Length-prefixed, so a cell containing the separator cannot make
		// two different rows share a key (binary columns carry anything).
		b.WriteString(strconv.Itoa(len(*c)))
		b.WriteByte(':')
		b.WriteString(*c)
	}
	return b.String()
}

func sortedKeys(rows [][]*string) []string {
	keys := make([]string, len(rows))
	for i, r := range rows {
		keys[i] = rowKey(r)
	}
	sort.Strings(keys)
	return keys
}

// sortIndex returns row indexes ordered by rowKey, so sorted position i maps
// back to a row.
func sortIndex(rows [][]*string) []int {
	idx := make([]int, len(rows))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return rowKey(rows[idx[a]]) < rowKey(rows[idx[b]]) })
	return idx
}

func sameMultiset(a, b [][]*string) bool {
	ka, kb := sortedKeys(a), sortedKeys(b)
	for i := range ka {
		if ka[i] != kb[i] {
			return false
		}
	}
	return true
}

// nullOrderNote names the NULL-position difference when the two orders
// differ only in where rows with a NULL sit: MySQL sorts NULLs first on
// ASC, DuckDB last.
func nullOrderNote(a, b [][]*string) string {
	strip := func(rows [][]*string) []string {
		var out []string
		for _, r := range rows {
			if !hasNull(r) {
				out = append(out, rowKey(r))
			}
		}
		return out
	}
	sa, sb := strip(a), strip(b)
	if len(sa) != len(sb) {
		return ""
	}
	for i := range sa {
		if sa[i] != sb[i] {
			return ""
		}
	}
	if len(sa) == len(a) {
		return "" // no NULLs involved
	}
	return "; only rows with NULL moved (MySQL sorts NULL first, the copy last)"
}

func hasNull(r []*string) bool {
	for _, c := range r {
		if c == nil {
			return true
		}
	}
	return false
}
