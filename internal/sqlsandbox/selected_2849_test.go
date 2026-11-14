package sqlsandbox

import (
	"reflect"
	"testing"
)

// #2849: the select list read item by item, for the fractional seconds MySQL
// declares a date and time result with. A column is resolved to the base
// tables of its select's FROM it can belong to; what the walk cannot tell
// the digits of is Unread, never a guess.
func TestCollectSelected(t *testing.T) {
	ev := TableRef{Name: "ev"}
	ev2 := TableRef{Name: "ev2"}
	shopEv := TableRef{Schema: "shop", Name: "ev"}
	col := func(name string, tables ...TableRef) SelectedColumn { return SelectedColumn{Name: name, Tables: tables} }
	cols := func(c ...SelectedColumn) []SelectedColumn { return c }
	unread := SelectedItem{Unread: true}
	cases := []struct {
		sql  string
		want []SelectedItem
	}{
		{"SELECT dt, e.dt3 AS x, MIN(dt), max(e.dt3), MAX(dt) OVER (), ANY_VALUE(dt) FROM ev e", []SelectedItem{
			{Columns: cols(col("dt", ev)), Itself: true}, {Columns: cols(col("dt3", ev)), Itself: true},
			{Columns: cols(col("dt", ev)), Itself: true}, {Columns: cols(col("dt3", ev)), Itself: true},
			{Columns: cols(col("dt", ev)), Itself: true}, {Columns: cols(col("dt", ev)), Itself: true}}},
		{"SELECT E.DT, shop.ev.dt, ev.dt3 FROM shop.ev e", []SelectedItem{
			{Columns: cols(col("DT", shopEv)), Itself: true}, unread, unread}},
		{"SELECT shop.ev.dt, ev.dt3 FROM shop.ev", []SelectedItem{
			{Columns: cols(col("dt", shopEv)), Itself: true}, {Columns: cols(col("dt3", shopEv)), Itself: true}}},
		{"SELECT a.dt, b.dt3, dt FROM ev a JOIN ev2 b ON a.id = b.id", []SelectedItem{
			{Columns: cols(col("dt", ev)), Itself: true}, {Columns: cols(col("dt3", ev2)), Itself: true},
			{Columns: cols(col("dt", ev, ev2)), Itself: true}}},
		// Date steps: the date's own digits, 6 for microseconds, and a day
		// or more said apart (a DATE stays a DATE on MySQL).
		{"SELECT _bintrail_date_add(dt3, INTERVAL 1 DAY), dt + INTERVAL 1 MICROSECOND, INTERVAL 2 HOUR + d, " +
			"_bintrail_date_sub(ts, INTERVAL '2' WEEK), dt - INTERVAL 90 MINUTE, date_add(dt, INTERVAL 1 SECOND) FROM ev", []SelectedItem{
			{Columns: cols(col("dt3", ev)), DayStep: true}, {Columns: cols(col("dt", ev)), Digits: 6},
			{Columns: cols(col("d", ev))}, {Columns: cols(col("ts", ev)), DayStep: true},
			{Columns: cols(col("dt", ev))}, {Columns: cols(col("dt", ev))}}},
		// A step by a column, a number added to a date, DuckDB's date_sub
		// (a count of units): not read.
		{"SELECT dt + INTERVAL (k) DAY, dt + k, date_sub('day', dt, dt3), k + 1 FROM ev", []SelectedItem{unread, unread, unread, unread}},
		{"SELECT now(), utc_timestamp(), now(3), from_unixtime(k), from_unixtime(floor(unix_timestamp(dt) / 3600) * 3600) FROM ev", []SelectedItem{
			{Clock: true}, {Clock: true}, unread, {FromUnixTime: true}, {FromUnixTime: true}}},
		{"SELECT now()", []SelectedItem{{Clock: true}}},
		{"SELECT COALESCE(dt, dt3), IFNULL(dt, NULL), GREATEST(dt, dt3), CASE WHEN k > 1 THEN dt ELSE dt3 END, IF(k > 1, dt, dt3), NULLIF(dt, dt3) FROM ev", []SelectedItem{
			{Columns: cols(col("dt", ev), col("dt3", ev))}, {Columns: cols(col("dt", ev))}, {Columns: cols(col("dt", ev), col("dt3", ev))},
			{Columns: cols(col("dt", ev), col("dt3", ev))}, {Columns: cols(col("dt", ev), col("dt3", ev))}, {Columns: cols(col("dt", ev))}}},
		// A cast, a literal, a subquery, a qualifier no FROM item has, a
		// constant beside a column.
		{"SELECT CAST(dt AS TIMESTAMP), TIMESTAMP '2026-01-01 00:00:00.5', (SELECT max(dt) FROM ev), z.dt, COALESCE(dt, '2026-01-01') FROM ev",
			[]SelectedItem{unread, unread, unread, unread, unread}},
		// A derived table or a WITH: the name is theirs, not a table's.
		{"SELECT x FROM (SELECT dt AS x FROM ev) s", []SelectedItem{unread}},
		{"SELECT s.x, ev.dt FROM (SELECT dt AS x FROM ev) s, ev", []SelectedItem{unread, {Columns: cols(col("dt", ev)), Itself: true}}},
		{"WITH w AS (SELECT dt FROM ev) SELECT dt FROM w", []SelectedItem{unread}},
		// Stars: one base table's columns, or not read.
		{"SELECT * FROM ev", []SelectedItem{{Star: &ev}}},
		{"SELECT e.*, dt FROM ev e", []SelectedItem{{Star: &ev}, {Columns: cols(col("dt", ev)), Itself: true}}},
		{"SELECT * FROM ev a JOIN ev2 b ON a.id = b.id", []SelectedItem{unread}},
		{"SELECT b.* FROM ev a JOIN ev2 b ON a.id = b.id", []SelectedItem{{Star: &ev2}}},
		{"SELECT * EXCLUDE (dt) FROM ev", []SelectedItem{unread}},
		{"SELECT COLUMNS('d.*') FROM ev", []SelectedItem{unread}},
		// A set operation: each side's column, as MySQL types the result
		// from both.
		{"SELECT dt FROM ev UNION ALL SELECT dt3 FROM ev2", []SelectedItem{{Columns: cols(col("dt", ev), col("dt3", ev2))}}},
		{"SELECT * FROM ev UNION ALL SELECT * FROM ev2", []SelectedItem{unread}},
		{"SELECT dt, k FROM ev UNION SELECT dt FROM ev", nil},
	}
	for _, c := range cases {
		got := parseForRefs(t, c.sql).Selected
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n  got  %+v\n  want %+v", c.sql, got, c.want)
		}
	}
}
