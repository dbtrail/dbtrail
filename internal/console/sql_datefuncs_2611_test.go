package console

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2611: under routing a DATE_FORMAT or a DATEDIFF reaches the copy only
// when each date it is given is a DATE, DATETIME or TIMESTAMP column of the
// tables the statement reads, or a clock call. A text column, a column that
// is text in another table read, and a table whose types are not known keep
// it on MySQL.
func TestSQLDateFunctionRefusalFor(t *testing.T) {
	in := views.Input{Baselines: []views.BaselineTable{
		{Schema: "s", Table: "ev", Path: "/c/s/ev.parquet", SchemaKnown: true,
			Columns:  []string{"id", "d", "dt", "ts", "dt6", "note"},
			Temporal: []baseline.TemporalColumn{{Name: "d", Type: "date"}, {Name: "dt", Type: "datetime"}, {Name: "ts", Type: "timestamp"}, {Name: "dt6", Type: "datetime(6)"}}},
		{Schema: "s", Table: "log", Path: "/c/s/log.parquet", SchemaKnown: true,
			Columns:  []string{"id", "d", "tm"}, // d is text here
			Temporal: []baseline.TemporalColumn{{Name: "tm", Type: "time"}}},
		{Schema: "s", Table: "raw", Path: "/c/s/raw.parquet",
			Columns: []string{"d"}},
	}}
	only := func(names ...string) views.Input {
		var refs sqlsandbox.Refs
		for _, n := range names {
			refs.Tables = append(refs.Tables, sqlsandbox.TableRef{Schema: "s", Name: n})
		}
		narrowed := in
		narrowed.OnlyViews = sqlWantedViews(in, refs)
		return narrowed
	}
	for _, c := range []struct {
		stmt    string
		tables  []string
		refused bool
	}{
		{"SELECT id FROM ev", []string{"ev"}, false},
		{"SELECT DATE_FORMAT(d, '%Y'), DATE_FORMAT(dt, '%H'), DATE_FORMAT(ts, '%i'), DATE_FORMAT(dt6, '%f') FROM ev", []string{"ev"}, false},
		{"SELECT DATEDIFF(d, dt), DATEDIFF(ts, CURDATE()), DATEDIFF(NOW(), dt6) FROM ev", []string{"ev"}, false},
		{"SELECT DATE_FORMAT(NOW(), '%Y') FROM raw", []string{"raw"}, false},
		{"SELECT DATE_FORMAT(note, '%Y') FROM ev", []string{"ev"}, true},
		{"SELECT DATEDIFF(d, '2026-01-01') FROM ev", []string{"ev"}, true},
		{"SELECT DATE_FORMAT(d, '%Y') FROM log", []string{"log"}, true},
		{"SELECT DATE_FORMAT(tm, '%H') FROM log", []string{"log"}, true},
		{"SELECT DATE_FORMAT(ev.d, '%Y') FROM ev JOIN log USING (id)", []string{"ev", "log"}, true},
		{"SELECT DATE_FORMAT(d, '%Y') FROM raw", []string{"raw"}, true},
		{"SELECT DATE_FORMAT(dt, '%Y') FROM ev JOIN raw ON true", []string{"ev", "raw"}, true},
		{"SELECT DATEDIFF(dt, ts) FROM ev WHERE id IN (SELECT id FROM log)", []string{"ev", "log"}, true},
	} {
		if got := sqlDateFunctionRefusalFor(only(c.tables...), c.stmt, readrouter.ShapeOf(c.stmt)); (got != "") != c.refused {
			t.Errorf("sqlDateFunctionRefusalFor(%q) = %q, want refused=%v", c.stmt, got, c.refused)
		}
	}
	// With no reading of the statement, one that may call either, or the
	// copy's spelling of either, is refused.
	for _, stmt := range []string{"SELECT DATE_FORMAT(d, '%Y') FROM ev", "SELECT strftime(d, '%Y') FROM ev", "SELECT date_diff('day', d, d) FROM ev"} {
		if sqlDateFunctionRefusalFor(only("ev"), stmt, nil) == "" {
			t.Errorf("no reading, %q: not refused", stmt)
		}
	}
	if got := sqlDateFunctionRefusalFor(only("ev"), "SELECT d FROM ev", nil); got != "" {
		t.Errorf("no reading, no call: refused (%s)", got)
	}
}
