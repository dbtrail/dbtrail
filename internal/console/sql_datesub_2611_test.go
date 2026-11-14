package console

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2611: under routing a DATE_SUB or SUBDATE reaches the copy only over a
// DATETIME or TIMESTAMP column without fractional seconds (a TIMESTAMP only
// under UTC or a fixed offset), NOW() or UTC_TIMESTAMP(), or a DATE stepped
// by less than a day. A DATE stepped by a day, a fraction, text, a column
// that is text in another table read, and a table whose types are not known
// keep it on MySQL.
func TestSQLDateSubRefusalFor(t *testing.T) {
	in := views.Input{Baselines: []views.BaselineTable{
		{Schema: "s", Table: "ev", Path: "/c/s/ev.parquet", SchemaKnown: true,
			Columns:      []string{"id", "d", "dt", "ts", "dt6", "note"},
			Temporal:     []baseline.TemporalColumn{{Name: "d", Type: "date"}, {Name: "dt", Type: "datetime"}, {Name: "ts", Type: "timestamp"}, {Name: "dt6", Type: "datetime"}},
			UnixTimeSafe: []string{"d", "dt", "ts"}, UnixTimeOther: []string{"id", "dt6", "note"}},
		{Schema: "s", Table: "log", Path: "/c/s/log.parquet", SchemaKnown: true,
			Columns:  []string{"id", "dt", "tm"}, // dt is text here
			Temporal: []baseline.TemporalColumn{{Name: "tm", Type: "time"}}, UnixTimeOther: []string{"id", "dt", "tm"}},
		{Schema: "s", Table: "raw", Path: "/c/s/raw.parquet",
			Columns: []string{"dt"}},
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
		zone    string
		refused bool
	}{
		{"SELECT id FROM ev", []string{"ev"}, "", false},
		{"SELECT DATE_SUB(dt, INTERVAL 1 DAY), SUBDATE(ts, INTERVAL 1 MONTH), DATE_SUB(d, INTERVAL 1 HOUR) FROM ev", []string{"ev"}, "", false},
		{"SELECT DATE_SUB(ts, INTERVAL 1 MONTH) FROM ev", []string{"ev"}, "Etc/GMT-5", false},
		{"SELECT id FROM ev WHERE dt >= DATE_SUB(NOW(), INTERVAL 30 DAY)", []string{"ev"}, "", false},
		{"SELECT DATE_SUB(NOW(), INTERVAL 1 DAY) FROM raw", []string{"raw"}, "", false},
		{"SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM ev", []string{"ev"}, "America/New_York", false},
		{"SELECT DATE_SUB(ts, INTERVAL 1 DAY) FROM ev", []string{"ev"}, "America/New_York", true},
		{"SELECT DATE_SUB(d, INTERVAL 1 DAY) FROM ev", []string{"ev"}, "", true},
		{"SELECT DATE_SUB(dt6, INTERVAL 1 DAY) FROM ev", []string{"ev"}, "", true},
		{"SELECT DATE_SUB(note, INTERVAL 1 DAY) FROM ev", []string{"ev"}, "", true},
		{"SELECT DATE_SUB(dt, INTERVAL 1 MICROSECOND) FROM ev", []string{"ev"}, "", true},
		{"SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM log", []string{"log"}, "", true},
		{"SELECT DATE_SUB(ev.dt, INTERVAL 1 DAY) FROM ev JOIN log USING (id)", []string{"ev", "log"}, "", true},
		{"SELECT DATE_SUB(tm, INTERVAL 1 HOUR) FROM log", []string{"log"}, "", true},
		{"SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM raw", []string{"raw"}, "", true},
		{"SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM ev JOIN raw ON true", []string{"ev", "raw"}, "", true},
	} {
		got := sqlDateSubRefusalFor(only(c.tables...), c.stmt, readrouter.ShapeOf(c.stmt), c.zone)
		if (got != "") != c.refused {
			t.Errorf("[%s] sqlDateSubRefusalFor(%q) = %q, want refused=%v", c.zone, c.stmt, got, c.refused)
		}
		if got != "" && !strings.HasPrefix(got, "the copy does not answer this statement: ") {
			t.Errorf("%q: %q is not worded as the other refusals", c.stmt, got)
		}
	}
	// With no reading of the statement, one that may call either, or the
	// copy's macro, is refused.
	for _, stmt := range []string{"SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM ev", "SELECT subdate(dt, INTERVAL 1 DAY) FROM ev", "SELECT _bintrail_date_sub(dt, INTERVAL 1 DAY) FROM ev"} {
		if sqlDateSubRefusalFor(only("ev"), stmt, nil, "") == "" {
			t.Errorf("no reading, %q: not refused", stmt)
		}
	}
	if got := sqlDateSubRefusalFor(only("ev"), "SELECT dt FROM ev", nil, ""); got != "" {
		t.Errorf("no reading, no call: refused (%s)", got)
	}
}
