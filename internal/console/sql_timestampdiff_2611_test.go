package console

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2611: under routing a TIMESTAMPDIFF reaches the copy only over DATE
// columns, DATETIME and TIMESTAMP columns without fractional seconds (a
// TIMESTAMP only under UTC or a fixed offset), and the clock calls. A
// fraction, text, a column that is text in another table read, and a table
// whose types are not known keep it on MySQL.
func TestSQLTimestampDiffRefusalFor(t *testing.T) {
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
		{"SELECT TIMESTAMPDIFF(DAY, d, dt), TIMESTAMPDIFF(MONTH, dt, ts), TIMESTAMPDIFF(YEAR, d, CURDATE()) FROM ev", []string{"ev"}, "", false},
		{"SELECT TIMESTAMPDIFF(MONTH, ts, NOW()) FROM ev", []string{"ev"}, "Etc/GMT-5", false},
		{"SELECT id FROM ev WHERE TIMESTAMPDIFF(DAY, dt, NOW()) > 30", []string{"ev"}, "", false},
		{"SELECT TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) FROM raw", []string{"raw"}, "", false},
		{"SELECT TIMESTAMPDIFF(DAY, d, dt) FROM ev", []string{"ev"}, "America/New_York", false},
		{"SELECT TIMESTAMPDIFF(DAY, ts, dt) FROM ev", []string{"ev"}, "America/New_York", true},
		{"SELECT TIMESTAMPDIFF(DAY, dt6, dt) FROM ev", []string{"ev"}, "", true},
		{"SELECT TIMESTAMPDIFF(DAY, note, dt) FROM ev", []string{"ev"}, "", true},
		{"SELECT TIMESTAMPDIFF(DAY, id, dt) FROM ev", []string{"ev"}, "", true},
		{"SELECT TIMESTAMPDIFF(DAY, dt, NOW()) FROM log", []string{"log"}, "", true},
		{"SELECT TIMESTAMPDIFF(DAY, ev.dt, NOW()) FROM ev JOIN log USING (id)", []string{"ev", "log"}, "", true},
		{"SELECT TIMESTAMPDIFF(HOUR, tm, NOW()) FROM log", []string{"log"}, "", true},
		{"SELECT TIMESTAMPDIFF(DAY, dt, NOW()) FROM raw", []string{"raw"}, "", true},
		{"SELECT TIMESTAMPDIFF(DAY, dt, NOW()) FROM ev JOIN raw ON true", []string{"ev", "raw"}, "", true},
	} {
		got := sqlTimestampDiffRefusalFor(only(c.tables...), c.stmt, readrouter.ShapeOf(c.stmt), c.zone)
		if (got != "") != c.refused {
			t.Errorf("[%s] sqlTimestampDiffRefusalFor(%q) = %q, want refused=%v", c.zone, c.stmt, got, c.refused)
		}
		if got != "" && !strings.HasPrefix(got, "the copy does not answer this statement: ") {
			t.Errorf("%q: %q is not worded as the other refusals", c.stmt, got)
		}
	}
	// With no reading of the statement, one that may call it, or the copy's
	// macros, is refused.
	for _, stmt := range []string{"SELECT TIMESTAMPDIFF(DAY, dt, ts) FROM ev", "SELECT _bintrail_timestampdiff(dt, ts, 1) FROM ev", "SELECT _bintrail_timestampdiff_months(dt, ts, 1) FROM ev"} {
		if sqlTimestampDiffRefusalFor(only("ev"), stmt, nil, "") == "" {
			t.Errorf("no reading, %q: not refused", stmt)
		}
	}
	if got := sqlTimestampDiffRefusalFor(only("ev"), "SELECT dt FROM ev", nil, ""); got != "" {
		t.Errorf("no reading, no call: refused (%s)", got)
	}
}
