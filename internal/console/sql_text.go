package console

import (
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// The values a routed statement's text columns must hold for the copy to
// answer it (#2533), as DuckDB regular expressions that match a value that
// does NOT qualify. Measured against MySQL 8.0 on every pair of 389 texts
// under 20 collations (views.TextKind has the findings):
//
//   - sqlTextPrintable: printable ASCII with no trailing space. Under a
//     folding collation two such texts are equal on MySQL exactly when they
//     are equal on the copy; past it an accent, 'ß' or a full-width letter
//     is MySQL's plain letter and not the copy's, and a PAD SPACE collation
//     does not see a trailing space.
//   - sqlTextSortable: letters, digits and inner spaces. The only texts a
//     folding collation sorts as the copy does: a sign (_ [ { and the rest)
//     has its own weight on MySQL, and not the same one in every collation.
//   - sqlTextPadded: no control character, no trailing space. A UTF-8 _bin
//     column compares code points on MySQL as the copy compares its bytes,
//     but utf8mb4_bin pads with spaces, which a control character sorts
//     below.
//
// Each has a no-space twin for a PAD SPACE column the statement passes
// through a call that can cut text (sqlsandbox.TextUse.Wrapped): LEFT('Jo
// Ann', 3) is 'Jo ', which MySQL compares as 'Jo' and the copy does not, so
// there no space at all qualifies.
const (
	sqlTextPrintable = `[^ -~]| $`
	sqlTextSortable  = `[^A-Za-z0-9 ]| $`
	sqlTextPadded    = `[\x00-\x1f]| $`

	sqlTextPrintableNoSpace = `[^!-~]`
	sqlTextSortableNoSpace  = `[^A-Za-z0-9]`
	sqlTextPaddedNoSpace    = `[\x00-\x20]`
)

// sqlTextGuard is one column of one view whose values must not match
// pattern for the copy to answer.
type sqlTextGuard struct {
	view, label, column, pattern string
}

// sqlTextCheck says why the copy must not answer a routed statement because
// of the text it compares, or which of its text columns' values must
// qualify first (#2533). The copy's SQL session folds case and nothing else;
// MySQL's folding collations also equate accents and sort signs their own
// way. The rule:
//
//   - A string literal outside printable ASCII or ending with a space: MySQL's
//     (sqlsandbox.TextUse.OddLiteral).
//   - A string literal put in order (a comparison, MIN, MAX, an ORDER BY
//     expression, or anything at all when the ORDER BY is an ordinal or an
//     alias, or the statement holds a UNION, INTERSECT or EXCEPT and puts
//     anything in order, which then also puts every named column in order): MySQL's, unless it is letters, digits and inner spaces, or its
//     operand beside it is a bare date or number column.
//   - GREATEST, LEAST or a row comparison over text or a string: MySQL's.
//     The copy compares those by bytes, case included, whatever the values.
//   - A table whose column types are not known: MySQL's.
//   - A text column the statement names (or a star brings in, or a NATURAL
//     or USING join compares):
//     its collation decides (views.TextKind). One the copy never compares
//     as MySQL does (_cs, a language's rules, not known) is MySQL's. An ENUM
//     or a SET in an ORDER BY is MySQL's, which sorts it by its position in
//     the declared list. Every other one is guarded: printable ASCII, or
//     letters, digits and inner spaces where the statement puts it in order.
//     An ordinal or a star in the ORDER BY, or an ORDER BY name that is an
//     alias or no column here, puts every named column in order.
//
// Names are compared without case, as MySQL compares column names, and
// without their qualifier: a name in two tables is guarded in both. A column
// a hidden rule takes out of its view is not there to guard. What the guards
// cost is one scan of each guarded column per statement, over the views the
// statement reads, tails included.
func sqlTextCheck(narrowed views.Input, refs sqlsandbox.Refs) (string, []sqlTextGuard) {
	const refuse = "the copy does not answer this statement: "
	u := refs.Text
	if u.OddLiteral {
		return refuse + "a string in it holds a character beyond plain ASCII, or ends with a space, which MySQL compares another way than the copy", nil
	}
	tables := narrowed.SelectedStateViews()
	known, isText := map[string]bool{}, map[string]bool{}
	for _, sv := range tables {
		t := sv.Table
		if !t.SchemaKnown {
			// Its text columns are not known, which is not "it has none".
			return refuse + fmt.Sprintf("the column types of %s.%s are not known, so how it compares text is not either", t.Schema, t.Table), nil
		}
		for _, list := range [][]string{t.Columns, t.DistinctSafe, t.DistinctOther} {
			for _, n := range list {
				known[strings.ToLower(n)] = true
			}
		}
		for _, tc := range t.Text {
			known[strings.ToLower(tc.Name)] = true
			isText[strings.ToLower(tc.Name)] = true
		}
	}
	aliases := map[string]bool{}
	for _, a := range u.Aliases {
		aliases[strings.ToLower(a)] = true
	}
	// in reads a list of references into a set, and says whether one of them
	// may stand for another column: an alias, or no column of these tables.
	in := func(cols []sqlsandbox.ColumnRef) (map[string]bool, bool) {
		set, unsure := map[string]bool{}, false
		for _, c := range cols {
			f := strings.ToLower(c.Name)
			set[f] = true
			if !known[f] || aliases[f] {
				unsure = true
			}
		}
		return set, unsure
	}
	wrapped, _ := in(u.Wrapped)
	ordered, orderedUnsure := in(u.Ordered)
	sorted, sortedUnsure := in(u.Sorted)
	// Under a set operation a name stands for every part's column there,
	// and for a string a part selects: whatever is put in order, everything
	// may be.
	orderedAll := u.OrderedUnknown || orderedUnsure || u.SetOperation && len(u.Ordered) > 0
	sortedAll := u.OrderedUnknown || sortedUnsure
	for _, c := range u.ByBytes {
		if f := strings.ToLower(c.Name); !known[f] || isText[f] || aliases[f] {
			u.ByBytesLiteral = true
		}
	}
	if u.ByBytesLiteral {
		return refuse + "GREATEST, LEAST and a row comparison such as (a, b) > (x, y) compare text by its bytes on the copy, " +
			"case included, where MySQL folds case", nil
	}
	if orderedAll && u.LooseLiteral {
		// Sorted by an ordinal or an alias: a string with a sign in the
		// select list may be what is sorted.
		u.OrderedLiterals = append(u.OrderedLiterals, sqlsandbox.OrderedLiteral{Expression: true})
	}
	for _, lit := range u.OrderedLiterals {
		textual := lit.Expression || len(lit.Columns) == 0
		for _, c := range lit.Columns {
			if f := strings.ToLower(c.Name); !known[f] || isText[f] || aliases[f] {
				textual = true
			}
		}
		if textual {
			return refuse + "it puts in order a string that holds a sign (anything but letters, digits and inner spaces), " +
				"which MySQL sorts another way than the copy", nil
		}
	}

	named := map[string]bool{}
	for _, c := range refs.Columns {
		named[strings.ToLower(c.Name)] = true
	}
	// The views whose every text column counts as named: where whole rows
	// are compared, the ones a star brings in (all of them when the walk
	// could not place a star); a NATURAL or USING join compares columns it
	// does not name, in every table read.
	every := map[string]bool{}
	// A star's columns count whatever the statement does with them: a
	// subquery's star can feed an IN, a join or a comparison.
	switch {
	case refs.NamedJoin || refs.Natural || refs.Star || refs.Unsure:
		for _, sv := range tables {
			every[sv.Ref] = true
		}
	case len(refs.StarTables) > 0:
		starred := narrowed
		starred.OnlyViews = sqlWantedViews(narrowed, sqlsandbox.Refs{Tables: refs.StarTables})
		for _, sv := range starred.SelectedStateViews() {
			every[sv.Ref] = true
		}
	}

	var guards []sqlTextGuard
	for _, sv := range tables {
		t := sv.Table
		label := t.Schema + "." + t.Table
		for _, tc := range t.Text {
			f := strings.ToLower(tc.Name)
			if !named[f] && !every[sv.Ref] || narrowed.Hidden.ColumnHidden(t.Schema, t.Table, tc.Name) {
				continue
			}
			if tc.Kind == views.TextUnlike {
				return refuse + fmt.Sprintf("%s.%s is under a collation the copy does not compare as MySQL does "+
					"(one that tells case apart, follows a language's rules, or is not known)", label, tc.Name), nil
			}
			if tc.Positional && (sortedAll || sorted[f]) {
				return refuse + fmt.Sprintf("ORDER BY sorts the ENUM or SET column %s.%s by its position in the declared list on MySQL, "+
					"and by its text on the copy", label, tc.Name), nil
			}
			pattern := sqlTextPrintable
			switch {
			case tc.Kind == views.TextBytes:
				pattern = sqlTextPadded
			case tc.Kind == views.TextFolded && (orderedAll || ordered[f]):
				pattern = sqlTextSortable
			}
			if tc.Padded && wrapped[f] {
				pattern = map[string]string{sqlTextPrintable: sqlTextPrintableNoSpace,
					sqlTextSortable: sqlTextSortableNoSpace, sqlTextPadded: sqlTextPaddedNoSpace}[pattern]
			}
			guards = append(guards, sqlTextGuard{view: sv.Ref, label: label, column: tc.Name, pattern: pattern})
		}
	}
	return "", guards
}

// sqlTextDeclineSQL is the end of a routed statement's views script that
// declines the statement when a guarded column holds a value that does not
// qualify (sqlsandbox.DeclineSQL): one statement per view, which reads each
// guarded column once and stops at the first such value. "" with no guard.
func sqlTextDeclineSQL(guards []sqlTextGuard) string {
	var order []string
	byView := map[string][]sqlTextGuard{}
	for _, g := range guards {
		if _, ok := byView[g.view]; !ok {
			order = append(order, g.view)
		}
		byView[g.view] = append(byView[g.view], g)
	}
	var b strings.Builder
	for _, view := range order {
		var conds, cols []string
		for _, g := range byView[view] {
			conds = append(conds, "regexp_matches(CAST("+`"`+strings.ReplaceAll(g.column, `"`, `""`)+`"`+" AS VARCHAR), '"+g.pattern+"')")
			cols = append(cols, g.column)
		}
		what := fmt.Sprintf("a value in %s (%s) that the copy compares another way than MySQL (a letter with an accent, "+
			"a sign in a column the statement sorts, a trailing space)", byView[view][0].label, strings.Join(cols, ", "))
		b.WriteString(sqlsandbox.DeclineSQL("EXISTS (SELECT 1 FROM "+view+" WHERE "+strings.Join(conds, " OR ")+")", what))
	}
	return b.String()
}
