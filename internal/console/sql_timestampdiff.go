package console

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// sqlTimestampDiffRefusalFor says why the copy must not answer a routed
// statement because of a TIMESTAMPDIFF call, or "" when it can (#2611). Under
// routing the copy is sent TIMESTAMPDIFF(unit, a, b) as its macro, which
// gives MySQL's whole units over two dates. The rule is the routing layer's
// (readrouter.TimestampDiffVeto, asked through types) over the tables the
// statement reads, their columns split in two: DATE, whole-second DATETIME and
// TIMESTAMP (views.BaselineTable.UnixTimeSafe, which leaves out a fraction and
// a value kept as text), and the rest.
//
// zone is sqlsandbox.Session.TimeZone: "" for UTC, Etc/GMT... for a fixed
// offset, a name otherwise. Under a named zone a TIMESTAMP column counts as
// the rest, as for DATE_SUB: MySQL reads it on the wall clock, and the copy
// holds an instant, which has not been measured across a clock change. A table
// whose column types are not known can hold text under any name: refused when
// the statement calls one. With no reading of the statement (types is nil), a
// statement that may call one, or the copy's macros, is refused: not known is
// never "nothing to find".
func sqlTimestampDiffRefusalFor(narrowed views.Input, statement string, types sqlsandbox.ColumnTypes, zone string) string {
	const refuse = "the copy does not answer this statement: "
	if !mayCallTimestampDiff.MatchString(statement) {
		return ""
	}
	if types == nil {
		return refuse + "it could not be searched for TIMESTAMPDIFF"
	}
	named := zone != "" && !strings.HasPrefix(zone, "Etc/GMT")
	var dates, other []string
	for _, t := range narrowed.SelectedBaselines() {
		if !t.SchemaKnown {
			if why := types.TimestampDiffVeto(nil, nil); why != "" {
				return refuse + fmt.Sprintf("TIMESTAMPDIFF over %s.%s, whose column types are not known", t.Schema, t.Table)
			}
			continue
		}
		whole := map[string]bool{}
		for _, n := range t.UnixTimeSafe {
			whole[strings.ToLower(n)] = true
		}
		kind := map[string]string{}
		for _, c := range t.Temporal {
			kind[strings.ToLower(c.Name)] = strings.ToLower(c.Type)
		}
		for _, n := range t.Columns {
			f := strings.ToLower(n)
			switch k := kind[f]; {
			case whole[f] && (k == "date" || k == "datetime" || k == "timestamp" && !named):
				dates = append(dates, n)
			default:
				other = append(other, n)
			}
		}
	}
	if why := types.TimestampDiffVeto(dates, other); why != "" {
		return refuse + why
	}
	return ""
}

// mayCallTimestampDiff is a word of TIMESTAMPDIFF, or of the copy's macros
// the router writes it as, anywhere in a statement.
var mayCallTimestampDiff = regexp.MustCompile(`(?i)\b(timestampdiff|_bintrail_timestampdiff|_bintrail_timestampdiff_months)\b`)
