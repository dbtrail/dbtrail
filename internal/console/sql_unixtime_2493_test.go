package console

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2493: UNIX_TIMESTAMP over a DATE, or a DATETIME or TIMESTAMP without
// fractional seconds, is the copy's to answer under UTC or a fixed offset;
// anything else is MySQL's.
func TestSQLUnixTimeRefusalFor(t *testing.T) {
	table := func(name string, safe, whole, other []string) views.BaselineTable {
		cols := append(append(append([]string{}, safe...), whole...), other...)
		return views.BaselineTable{Schema: "tpcc", Table: name, Path: "/c/tpcc/" + name + ".parquet", SchemaKnown: true,
			Columns: cols, UnixTimeSafe: safe, UnixTimeOther: append(append([]string{}, whole...), other...),
			WholeSafe: whole, WholeOther: append(append([]string{}, safe...), other...)}
	}
	in := views.Input{Baselines: []views.BaselineTable{
		table("orders", []string{"o_entry_d"}, []string{"o_id"}, []string{"o_note"}),
		table("history", []string{"h_date"}, nil, []string{"h_amount", "h_at"}), // h_amount: DECIMAL(6,2), h_at: DATETIME(6)
	}}
	only := func(names ...string) views.Input {
		var refs sqlsandbox.Refs
		for _, n := range names {
			refs.Tables = append(refs.Tables, sqlsandbox.TableRef{Schema: "tpcc", Name: n})
		}
		narrowed := in
		narrowed.OnlyViews = sqlWantedViews(in, refs)
		return narrowed
	}
	issue := "SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(o_entry_d) / 3600) * 3600) AS period_start, COUNT(*) FROM orders GROUP BY period_start ORDER BY period_start"
	for _, c := range []struct {
		stmt    string
		zone    string
		refused bool
	}{
		{issue, "", false},
		{issue, "Etc/GMT-5", false},                            // a fixed offset (+05:00)
		{issue, "America/Bogota", true},                        // a named zone: a clock change not measured yet
		{"SELECT UNIX_TIMESTAMP(h_at) FROM history", "", true}, // fractional seconds
		{"SELECT UNIX_TIMESTAMP(o_note) FROM orders", "", true},
		{"SELECT FROM_UNIXTIME(o_id) FROM orders", "America/Bogota", false}, // an instant to a wall clock is unambiguous
		{"SELECT FROM_UNIXTIME(h_amount) FROM history", "", true},           // a decimal: MySQL prints its fraction digits
		{"SELECT FROM_UNIXTIME(o_id / 1000) FROM orders", "", true},
		{"SELECT o_id FROM orders", "America/Bogota", false},
	} {
		got := sqlUnixTimeRefusalFor(only("orders", "history"), c.stmt, readrouter.ShapeOf(c.stmt), c.zone)
		if (got != "") != c.refused {
			t.Errorf("%s [%q]: refusal %q, want refused %v", c.stmt, c.zone, got, c.refused)
		}
		if got != "" && !strings.Contains(got, "UNIX_TIMESTAMP") && !strings.Contains(got, "FROM_UNIXTIME") {
			t.Errorf("%s: refusal %q does not say why", c.stmt, got)
		}
	}
	// No reading of the statement: refused when it may hold a call.
	if got := sqlUnixTimeRefusalFor(in, "SELECT unix_timestamp(o_entry_d) FROM orders", nil, ""); got == "" {
		t.Error("no reading of a statement with UNIX_TIMESTAMP, yet the copy answers")
	}
	if got := sqlUnixTimeRefusalFor(in, "SELECT from_unixtime(o_id) FROM orders", nil, ""); got == "" {
		t.Error("no reading of a statement with FROM_UNIXTIME, yet the copy answers")
	}
	if got := sqlUnixTimeRefusalFor(in, "SELECT o_id FROM orders", nil, ""); got != "" {
		t.Errorf("no UNIX_TIMESTAMP in the statement: %q", got)
	}
}
