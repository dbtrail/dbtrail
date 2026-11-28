//go:build unix

package sqlsandbox

import (
	"context"
	"reflect"
	"testing"
)

// #2803: Session.PostgresNames changes the names of the answer's unnamed
// columns and nothing else, and only for the caller that sets it: the
// session of the MySQL port, the SQL card's, and the SQL card's over a
// PostgreSQL source (PostgresRules alone) get the engine's names as before.
func TestPostgresNames_onlyForTheSessionThatAsks(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	names := func(sql string, sess Session) ([]string, [][]any) {
		t.Helper()
		job := f.job(sql)
		job.Session = sess
		res, err := r.Run(context.Background(), job)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		out := make([]string, len(res.Columns))
		for i, c := range res.Columns {
			out[i] = c.Name
		}
		return out, res.Rows
	}
	const from = " FROM (VALUES (1, 'x'), (2, 'y')) t(a, b)"
	for _, c := range []struct {
		sql        string
		pg, engine []string
	}{
		{"SELECT count(*), sum(a), max(a) + 1, 1, count(*) AS n, count(*) AS \"count_star()\"" + from,
			[]string{"count", "sum", "?column?", "?column?", "n", "count_star()"},
			[]string{"count_star()", "sum(a)", "(max(a) + 1)", "1", "n", "count_star()"}},
		{"SELECT a, (a + 1)::int, a::bigint, lower(b)" + from + " ORDER BY a",
			[]string{"a", "int4", "a", "lower"},
			[]string{"a", "CAST((a + 1) AS INTEGER)", "CAST(a AS BIGINT)", "lower(b)"}},
		// A star: the engine's names, whole.
		{"SELECT *, count(*) OVER ()" + from + " ORDER BY a",
			[]string{"a", "b", "count() OVER ()"},
			[]string{"a", "b", "count() OVER ()"}},
	} {
		pg, pgRows := names(c.sql, Session{PostgresRules: true, PostgresNames: true})
		if !reflect.DeepEqual(pg, c.pg) {
			t.Errorf("PostgresNames, %s\n   got %q\n  want %q", c.sql, pg, c.pg)
		}
		for what, sess := range map[string]Session{
			"no session":         {},
			"PostgresRules only": {PostgresRules: true},
			"the MySQL port's":   {MySQLClock: true, MySQLText: true},
		} {
			got, rows := names(c.sql, sess)
			if !reflect.DeepEqual(got, c.engine) {
				t.Errorf("%s, %s\n   got %q\n  want the engine's %q", what, c.sql, got, c.engine)
			}
			if what == "PostgresRules only" && !reflect.DeepEqual(rows, pgRows) {
				t.Errorf("%s: the rows differ with the names\n  with: %v\n  without: %v", c.sql, pgRows, rows)
			}
		}
	}
}

// The port's macros for avg(x), sum(x) and a / b (#3219) are named as
// PostgreSQL names what the client wrote: avg, sum, and ?column?.
func TestPostgresNames_ownMacros(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	job := f.job("SELECT _bintrail_pg_sum32(a), _bintrail_pg_avgn(a), _bintrail_pg_div(sum(a), 2), _bintrail_pg_avg(a * 1.5::double), _bintrail_pg_sum32(a) AS s FROM (VALUES (1, 'x'), (2, 'y')) t(a, b)")
	job.Session = Session{PostgresRules: true, PostgresNames: true}
	res, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range res.Columns {
		got = append(got, c.Name)
	}
	if want := []string{"sum", "avg", "?column?", "avg", "s"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
