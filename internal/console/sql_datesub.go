package console

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// sqlDateSubRefusalFor says why the copy must not answer a routed statement
// because of a DATE_SUB or SUBDATE call, or "" when it can (#2611). Under
// routing the copy is sent DATE_SUB(d, INTERVAL n unit) as its macro, which
// gives MySQL's answer over a DATETIME or TIMESTAMP declared without
// fractional seconds, NOW() and UTC_TIMESTAMP(), and over a DATE stepped by
// less than a day. The rule is the routing layer's (readrouter.DateSubVeto,
// asked through types) over the tables the statement reads, their columns
// split by type: DATE, whole-second DATETIME and TIMESTAMP (both in
// views.BaselineTable.UnixTimeSafe, which leaves out a fraction and a value
// kept as text), and the rest.
//
// zone is sqlsandbox.Session.TimeZone: "" for UTC, Etc/GMT... for a fixed
// offset, a name otherwise. Under a named zone a TIMESTAMP column counts as
// the rest: MySQL steps its wall clock and the copy its instant, which part
// at a clock change. A table whose column types are not known can hold text
// under any name: refused when the statement calls one. With no reading of
// the statement (types is nil), a statement that may call one, or the copy's
// macro, is refused: not known is never "nothing to find".
func sqlDateSubRefusalFor(narrowed views.Input, statement string, types sqlsandbox.ColumnTypes, zone string) string {
	const refuse = "the copy does not answer this statement: "
	if !mayCallDateSub.MatchString(statement) {
		return ""
	}
	if types == nil {
		return refuse + "it could not be searched for DATE_SUB and SUBDATE"
	}
	named := zone != "" && !strings.HasPrefix(zone, "Etc/GMT")
	var dates, times, other []string
	for _, t := range narrowed.SelectedBaselines() {
		if !t.SchemaKnown {
			if why := types.DateSubVeto(nil, nil, nil); why != "" {
				return refuse + fmt.Sprintf("DATE_SUB or SUBDATE over %s.%s, whose column types are not known", t.Schema, t.Table)
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
			case !whole[f]:
				other = append(other, n)
			case k == "date":
				dates = append(dates, n)
			case k == "datetime", k == "timestamp" && !named:
				times = append(times, n)
			default:
				other = append(other, n)
			}
		}
	}
	if why := types.DateSubVeto(dates, times, other); why != "" {
		return refuse + why
	}
	return ""
}

// mayCallDateSub is a word of DATE_SUB or SUBDATE, or of the copy's macro
// the router writes them as, anywhere in a statement.
var mayCallDateSub = regexp.MustCompile(`(?i)\b(date_sub|subdate|_bintrail_date_sub)\b`)
