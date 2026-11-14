package console

import (
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2849: how MySQL declares each date and time column of an answer, from the
// select list and the tables' CREATE TABLE. Known only where it is certain:
// anything else is left for the port to decide.
func TestSQLDateScales(t *testing.T) {
	in := views.Input{Baselines: []views.BaselineTable{
		{Schema: "s", Table: "ev", Path: "/c/s/ev.parquet", SchemaKnown: true,
			Columns: []string{"id", "d", "dt", "dt3", "ts6", "ts", "tm", "note"},
			Temporal: []baseline.TemporalColumn{{Name: "d", Type: "date"}, {Name: "dt", Type: "datetime"}, {Name: "dt3", Type: "datetime", Digits: 3},
				{Name: "ts6", Type: "timestamp", Digits: 6}, {Name: "ts", Type: "timestamp"}, {Name: "tm", Type: "time"}}},
		// dt is DATETIME(6) here: a name in two tables.
		{Schema: "s", Table: "log", Path: "/c/s/log.parquet", SchemaKnown: true,
			Columns:  []string{"id", "dt", "odd"},
			Temporal: []baseline.TemporalColumn{{Name: "dt", Type: "datetime", Digits: 6}, {Name: "odd", Digits: -1}}},
		// A declaration not recorded.
		{Schema: "s", Table: "old", Path: "/c/s/old.parquet", SchemaKnown: true,
			Columns:  []string{"dt"},
			Temporal: []baseline.TemporalColumn{{Name: "dt", Type: "datetime", Digits: -1}}},
		// No CREATE TABLE at all.
		{Schema: "s", Table: "raw", Path: "/c/s/raw.parquet", Columns: []string{"dt"}},
		// MySQL's star returns other columns than the copy holds.
		{Schema: "s", Table: "gen", Path: "/c/s/gen.parquet", SchemaKnown: true, Columns: []string{"dt"}, StarDiffers: "a generated column",
			Temporal: []baseline.TemporalColumn{{Name: "dt", Type: "datetime"}}},
	}}
	ev := sqlsandbox.TableRef{Schema: "s", Name: "ev"}
	evBare := sqlsandbox.TableRef{Name: "EV"}
	logT := sqlsandbox.TableRef{Schema: "s", Name: "log"}
	col := func(name string, tables ...sqlsandbox.TableRef) []sqlsandbox.SelectedColumn {
		return []sqlsandbox.SelectedColumn{{Name: name, Tables: tables}}
	}
	known := func(d int) sqlsandbox.DateScale { return sqlsandbox.DateScale{Known: true, Digits: d} }
	var unknown sqlsandbox.DateScale
	routed, plain := true, false
	cases := []struct {
		why    string
		items  []sqlsandbox.SelectedItem
		routed bool
		want   []sqlsandbox.DateScale
	}{
		{"a column, its case and an unqualified table", []sqlsandbox.SelectedItem{
			{Columns: col("dt", ev), Itself: true}, {Columns: col("DT3", evBare), Itself: true}, {Columns: col("ts6", ev), Itself: true},
			{Columns: col("ts", ev), Itself: true}, {Columns: col("d", ev), Itself: true}},
			plain, []sqlsandbox.DateScale{known(0), known(3), {Known: true, Digits: 6, Timestamp: true}, {Known: true, Timestamp: true}, known(0)}},
		{"computed from a TIMESTAMP: a DATETIME", []sqlsandbox.SelectedItem{{Columns: col("ts6", ev)}},
			plain, []sqlsandbox.DateScale{known(6)}},
		{"the most digits of its columns, a step in microseconds", []sqlsandbox.SelectedItem{
			{Columns: append(col("dt", ev), col("dt3", ev)...)}, {Columns: col("dt", ev), Digits: 6}, {Columns: col("d", ev)}},
			plain, []sqlsandbox.DateScale{known(3), known(6), known(0)}},
		{"a DATE stepped by a day is a DATE on MySQL", []sqlsandbox.SelectedItem{{Columns: col("d", ev), DayStep: true}, {Columns: col("dt", ev), DayStep: true}},
			plain, []sqlsandbox.DateScale{unknown, known(0)}},
		{"NOW() and FROM_UNIXTIME", []sqlsandbox.SelectedItem{{Clock: true}, {FromUnixTime: true}, {Clock: true, Columns: col("dt3", ev)}},
			routed, []sqlsandbox.DateScale{known(0), known(0), known(3)}},
		{"FROM_UNIXTIME without routing: its argument is not known to be whole", []sqlsandbox.SelectedItem{{FromUnixTime: true}},
			plain, []sqlsandbox.DateScale{unknown}},
		{"not a date column, a TIME, a type not known, nothing to read", []sqlsandbox.SelectedItem{
			{Columns: col("note", ev)}, {Columns: col("tm", ev)}, {Columns: col("odd", logT)}, {}, {Unread: true}},
			plain, []sqlsandbox.DateScale{unknown, unknown, unknown, unknown, unknown}},
		{"a name two tables declare differently, or the same", []sqlsandbox.SelectedItem{
			{Columns: col("dt", ev, logT)}, {Columns: col("dt3", ev, logT)}},
			plain, []sqlsandbox.DateScale{unknown, known(3)}},
		{"a name in two schemas when written without one", []sqlsandbox.SelectedItem{{Columns: col("dt", sqlsandbox.TableRef{Name: "log"})}},
			plain, []sqlsandbox.DateScale{known(6)}},
		{"a declaration not recorded, no CREATE TABLE, another catalog", []sqlsandbox.SelectedItem{
			{Columns: col("dt", sqlsandbox.TableRef{Schema: "s", Name: "old"})}, {Columns: col("dt", sqlsandbox.TableRef{Schema: "s", Name: "raw"})},
			{Columns: col("dt", sqlsandbox.TableRef{Catalog: "other", Schema: "s", Name: "ev"})}},
			plain, []sqlsandbox.DateScale{unknown, unknown, unknown}},
		{"a star: the table's columns in its order, named", []sqlsandbox.SelectedItem{{Star: &ev}, {Columns: col("dt", ev)}},
			plain, []sqlsandbox.DateScale{{Name: "id"}, {Name: "d"}, {Known: true, Name: "dt"}, {Known: true, Digits: 3, Name: "dt3"},
				{Known: true, Digits: 6, Timestamp: true, Name: "ts6"}, {Known: true, Timestamp: true, Name: "ts"}, {Name: "tm"}, {Name: "note"}, known(0)}},
	}
	for _, c := range cases {
		got := sqlDateScales(in, sqlsandbox.Refs{Selected: c.items}, c.routed, true)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n  got  %+v\n  want %+v", c.why, got, c.want)
		}
	}
	// A clock that is DuckDB's own (not the MySQL port) has its microseconds.
	if got := sqlDateScales(in, sqlsandbox.Refs{Selected: []sqlsandbox.SelectedItem{{Clock: true}}}, routed, false); !reflect.DeepEqual(got, []sqlsandbox.DateScale{unknown}) {
		t.Errorf("DuckDB's now(): %+v, want unknown", got)
	}
	// A star that cannot be lined up leaves the whole list unread: the
	// columns after it would be counted from the wrong place.
	for _, ref := range []sqlsandbox.TableRef{{Schema: "s", Name: "raw"}, {Schema: "s", Name: "gen"}, {Schema: "s", Name: "nope"}, {Name: "dt"}} {
		r := ref
		if got := sqlDateScales(in, sqlsandbox.Refs{Selected: []sqlsandbox.SelectedItem{{Star: &r}, {Columns: col("dt", ev)}}}, plain, true); got != nil {
			t.Errorf("a star over %+v: %+v, want nil", ref, got)
		}
	}
	if got := sqlDateScales(in, sqlsandbox.Refs{}, plain, true); got != nil {
		t.Errorf("no select list: %+v, want nil", got)
	}
}
