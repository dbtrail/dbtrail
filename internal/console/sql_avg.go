package console

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// sqlAvgRefusalFor says why the copy must not answer a routed statement
// because of an AVG call, or "" when it can (#2920). Under routing the copy is
// sent AVG(x) as its exact average of whole numbers (sqlsandbox's
// avgMacros), MySQL's DECIMAL with four decimals. The rule is the routing
// layer's (readrouter.AvgVeto, asked through types) over the tables the
// statement reads, their columns split in two: the whole numbers
// (views.BaselineTable.DistinctSafe without its DECIMAL columns: TINYINT to
// BIGINT and BOOL, none kept as text), and the rest.
//
// A table whose column types are not known can hold a DOUBLE under any name:
// refused when the statement calls AVG. With no reading of the statement
// (types is nil), a statement that may call AVG, or the copy's macros, is
// refused: not known is never "nothing to find".
func sqlAvgRefusalFor(narrowed views.Input, statement string, types sqlsandbox.ColumnTypes) string {
	const refuse = "the copy does not answer this statement: "
	if !mayCallAvg.MatchString(statement) {
		return ""
	}
	if types == nil {
		return refuse + "it could not be searched for AVG"
	}
	var whole, other []string
	for _, t := range narrowed.SelectedBaselines() {
		if !t.SchemaKnown {
			if why := types.AvgVeto(nil, nil); why != "" {
				return refuse + fmt.Sprintf("AVG over %s.%s, whose column types are not known", t.Schema, t.Table)
			}
			continue
		}
		decimal := map[string]bool{}
		for _, d := range t.Decimals {
			decimal[strings.ToLower(d.Name)] = true
		}
		for _, n := range t.DistinctSafe {
			if decimal[strings.ToLower(n)] {
				other = append(other, n)
			} else {
				whole = append(whole, n)
			}
		}
		other = append(other, t.DistinctOther...)
	}
	if why := types.AvgVeto(whole, other); why != "" {
		return refuse + why
	}
	return ""
}

// mayCallAvg is a word of AVG, or of the copy's macros the router writes it
// as, anywhere in a statement.
var mayCallAvg = regexp.MustCompile(`(?i)\b(avg|_bintrail_avg|_bintrail_avg_distinct)\b`)
