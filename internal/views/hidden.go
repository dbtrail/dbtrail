package views

import (
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/hiderules"
)

// ApplyHidden applies a server's hidden rules (#2529) to an input built for
// the analytical surfaces: a hidden table's state view is not defined at all
// (it leaves Baselines, so no listing, note or plan names it), and each
// remaining table carries the columns its view leaves out. The set is kept
// for the events view. An empty or nil set changes nothing.
//
// Only the surfaces that run statements for a reader call this. A views file
// handed to the operator's own DuckDB reads the files directly, where rules
// cannot hold, and is generated without it.
func ApplyHidden(in *Input, set *hiderules.Set) {
	if set.Empty() {
		return
	}
	kept := in.Baselines[:0:0]
	for _, t := range in.Baselines {
		if set.TableHidden(t.Schema, t.Table) {
			continue
		}
		t.HiddenColumns = set.HiddenColumns(t.Schema, t.Table)
		if everyColumnHidden(t, set) {
			// A view with no column fails at CREATE, which would break every
			// statement that installs every view: the table is hidden.
			continue
		}
		kept = append(kept, t)
	}
	in.HiddenEveryTable = len(in.Baselines) > 0 && len(kept) == 0
	in.Baselines = kept
	in.Hidden = set
}

// everyColumnHidden reports whether the table's known columns are all
// hidden. Unknown columns (no schema read) report false.
func everyColumnHidden(t BaselineTable, set *hiderules.Set) bool {
	if len(t.Columns) == 0 || len(t.HiddenColumns) == 0 {
		return false
	}
	for _, c := range t.Columns {
		if !set.ColumnHidden(t.Schema, t.Table, c) {
			return false
		}
	}
	return true
}

// hideColumns wraps one state view body (ending in ";\n") so the named
// columns leave it, matched without case. COLUMNS(lambda) keeps the body's
// column order and does not fail on a name the table does not have, so a
// rule for every table of a schema needs no per-table check.
func hideColumns(body string, cols []string) string {
	if len(cols) == 0 {
		return body
	}
	inner := strings.TrimSuffix(strings.TrimRight(body, "\n"), ";")
	return fmt.Sprintf("  SELECT COLUMNS(lambda c: lower(c) NOT IN (%s)) FROM (\n%s\n  ) AS bintrail_visible;\n",
		lowerList(cols), inner)
}

func lowerList(names []string) string {
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = "lower(" + sqlString(n) + ")"
	}
	return strings.Join(parts, ", ")
}

// tableMatch is the SQL condition "this event's table is ruled by r".
func tableMatch(schema, table string, all bool) string {
	cond := "lower(schema_name) = lower(" + sqlString(schema) + ")"
	if !all {
		cond += " AND lower(table_name) = lower(" + sqlString(table) + ")"
	}
	return "(" + cond + ")"
}

// hideEvents wraps the events view body (ending in ";\n"): rows of hidden
// tables are dropped, hidden columns leave row_before, row_after and
// changed_columns, and the statement text and its digest are blanked on any
// server with rules, because one statement can name several tables and
// carries their literals.
func hideEvents(body string, set *hiderules.Set, exclude []string) string {
	if set.Empty() {
		return body
	}
	excluded := map[string]bool{}
	for _, c := range exclude {
		excluded[strings.ToLower(c)] = true
	}
	inner := strings.TrimSuffix(strings.TrimRight(body, "\n"), ";")

	// Column rules grouped by the table condition they apply under.
	var anyTable []string
	var keyConds []string
	type group struct {
		cond string
		cols []string
	}
	var groups []*group
	byCond := map[string]*group{}
	for _, r := range set.ColumnRules() {
		cond := tableMatch(r.Schema, r.Table, r.AllTables)
		g := byCond[cond]
		if g == nil {
			g = &group{cond: cond}
			byCond[cond] = g
			groups = append(groups, g)
			anyTable = append(anyTable, cond)
		}
		g.cols = append(g.cols, r.Column)
	}
	keyHidden := func(key string) string {
		keyConds = keyConds[:0]
		for _, g := range groups {
			keyConds = append(keyConds, fmt.Sprintf("(%s AND lower(%s) IN (%s))", g.cond, key, lowerList(g.cols)))
		}
		return strings.Join(keyConds, " OR ")
	}

	var replace []string
	if len(groups) > 0 {
		ruled := strings.Join(anyTable, " OR ")
		for _, col := range []string{"row_before", "row_after"} {
			if excluded[col] {
				continue
			}
			replace = append(replace, fmt.Sprintf(
				"CASE WHEN %[1]s IS NOT NULL AND (%[2]s) THEN (SELECT coalesce(json_group_object(key, value), '{}'::JSON) FROM json_each(%[1]s) WHERE NOT (%[3]s)) ELSE %[1]s END AS %[1]s",
				col, ruled, keyHidden("key")))
		}
		// pk_values carries the key's values, and the key may be a hidden
		// column; which columns it holds is not in the row, so it is blanked
		// on every table a column rule applies to.
		if !excluded["pk_values"] {
			replace = append(replace, fmt.Sprintf("CASE WHEN %s THEN CAST(NULL AS VARCHAR) ELSE pk_values END AS pk_values", ruled))
		}
		if !excluded["changed_columns"] {
			replace = append(replace, fmt.Sprintf(
				"CASE WHEN changed_columns IS NOT NULL AND (%[1]s) THEN (SELECT coalesce(json_group_array(value), '[]'::JSON) FROM json_each(changed_columns) WHERE NOT (%[2]s)) ELSE changed_columns END AS changed_columns",
				ruled, keyHidden("value ->> '$'")))
		}
	}
	for _, col := range []string{"query_text", "query_hash"} {
		if !excluded[col] {
			replace = append(replace, "CAST(NULL AS VARCHAR) AS "+col)
		}
	}

	var b strings.Builder
	b.WriteString("  SELECT *")
	if len(replace) > 0 {
		b.WriteString(" REPLACE (\n    " + strings.Join(replace, ",\n    ") + "\n  )")
	}
	b.WriteString(" FROM (\n" + inner + "\n  ) AS bintrail_events")
	var hiddenTables []string
	for _, r := range set.TableRules() {
		hiddenTables = append(hiddenTables, tableMatch(r.Schema, r.Table, r.AllTables))
	}
	if len(hiddenTables) > 0 {
		b.WriteString("\n  WHERE NOT (" + strings.Join(hiddenTables, " OR ") + ")")
	}
	b.WriteString(";\n")
	return b.String()
}
