package sqlsandbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// #2493: the copy has UNIX_TIMESTAMP and FROM_UNIXTIME, as macros the worker
// defines on its connection. Each answers MySQL's value where it is exact,
// and errors where it is not, so a routed statement falls back to MySQL.
func TestRun_unixTimeFunctions(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	run := func(zone, q string) ([]any, error) {
		t.Helper()
		job := f.job(q)
		job.Session.TimeZone = zone
		res, err := r.Run(context.Background(), job)
		if err != nil {
			return nil, err
		}
		return res.Rows[0], nil
	}
	for _, tc := range []struct {
		zone, q string
		want    any // nil = SQL NULL; "error" = the statement errors
	}{
		// An instant is its seconds in any zone.
		{"", "SELECT unix_timestamp(TIMESTAMPTZ '2026-01-15 12:00:00+00')", int64(1768478400)},
		{"Etc/GMT-5", "SELECT unix_timestamp(TIMESTAMPTZ '2026-01-15 12:00:00+00')", int64(1768478400)},
		// A wall clock (a DATETIME under a non-UTC session) and a DATE are read
		// in the session's zone, as MySQL reads them.
		{"", "SELECT unix_timestamp(TIMESTAMP '2026-01-15 12:00:00')", int64(1768478400)},
		{"Etc/GMT-5", "SELECT unix_timestamp(TIMESTAMP '2026-01-15 12:00:00')", int64(1768478400 - 5*3600)},
		{"", "SELECT unix_timestamp(DATE '2026-01-15')", int64(1768435200)},
		{"Etc/GMT+3", "SELECT unix_timestamp(DATE '2026-01-15')", int64(1768435200 + 3*3600)},
		{"", "SELECT unix_timestamp(NULL::TIMESTAMP)", "error"}, // a NULL or a zero date, which MySQL reads as 0
		{"", "SELECT unix_timestamp(NULL::DATE)", "error"},
		{"", "SELECT unix_timestamp(TIMESTAMP '1970-01-01 00:00:00')", int64(0)},
		// Outside 1970-2038 MySQL and MariaDB answer their own way: MySQL's.
		{"", "SELECT unix_timestamp(TIMESTAMP '1969-12-31 23:59:59')", "error"},
		{"", "SELECT unix_timestamp(TIMESTAMP '2038-01-19 03:14:08')", "error"},
		{"", "SELECT unix_timestamp(TIMESTAMP '2038-01-19 03:14:07')", int64(2147483647)},
		{"", "SELECT unix_timestamp(TIMESTAMP '2026-01-15 12:00:00.75')", "error"}, // a decimal on MySQL
		{"", "SELECT unix_timestamp(TIMESTAMP '2026-01-15 12:00:00.000001')", "error"},
		{"", "SELECT unix_timestamp(TIMESTAMP '2026-01-15 12:00:00.000000')", int64(1768478400)},

		// FROM_UNIXTIME: the wall clock of an instant in the session's zone.
		{"", "SELECT CAST(from_unixtime(1768478400) AS VARCHAR)", "2026-01-15 12:00:00"},
		{"Etc/GMT-5", "SELECT CAST(from_unixtime(1768478400) AS VARCHAR)", "2026-01-15 17:00:00"},
		{"", "SELECT from_unixtime(NULL)", nil},
		{"", "SELECT CAST(from_unixtime(0) AS VARCHAR)", "1970-01-01 00:00:00"},
		{"", "SELECT from_unixtime(-1)", "error"},
		{"", "SELECT from_unixtime(1.5)", "error"},        // MySQL prints the fraction's digits; a whole 3600.0 is the router's to keep on MySQL (readrouter TestFromUnixTimeVeto)
		{"", "SELECT from_unixtime(2147483648)", "error"}, // MariaDB and MySQL part ways
		{"", "SELECT from_unixtime(1, '%Y')", "error"},    // the format is not here yet
		// The issue's bucket, whole.
		{"", "SELECT CAST(from_unixtime(floor(unix_timestamp(TIMESTAMP '2026-01-15 12:34:56') / 3600) * 3600) AS VARCHAR)", "2026-01-15 12:00:00"},
		{"Etc/GMT-5", "SELECT CAST(from_unixtime(floor(unix_timestamp(TIMESTAMP '2026-01-15 12:34:56') / 3600) * 3600) AS VARCHAR)", "2026-01-15 12:00:00"},
	} {
		got, err := run(tc.zone, tc.q)
		if tc.want == "error" {
			if err == nil {
				t.Errorf("[%s] %s = %v, want an error", tc.zone, tc.q, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("[%s] %s: %v", tc.zone, tc.q, err)
			continue
		}
		if fmt.Sprint(got[0]) != fmt.Sprint(tc.want) || (tc.want == nil) != (got[0] == nil) {
			t.Errorf("[%s] %s = %v (%T), want %v", tc.zone, tc.q, got[0], got[0], tc.want)
		}
	}
	// The result type MySQL returns for a whole-second column: an integer.
	job := f.job("SELECT unix_timestamp(TIMESTAMP '2026-01-15 12:00:00') AS u")
	res, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if typ := res.Columns[0].Type; !strings.Contains(typ, "BIGINT") {
		t.Errorf("unix_timestamp's type = %s, want BIGINT", typ)
	}
}

// A macro's own error is a decision about the value, not a fault of the copy:
// DeclinedValue tells it from every other query error, so read routing counts
// it as a statement the copy declined and not as one it failed.
func TestDeclinedValue2493(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"the FROM_UNIXTIME macro", &QueryError{Message: "Invalid Input Error: FROM_UNIXTIME of a negative, fractional or past-2038 number is not answered by the copy"}, true},
		{"the UNIX_TIMESTAMP macro", &QueryError{Message: "Invalid Input Error: UNIX_TIMESTAMP of a time before 1970 or after 2038-01-19 03:14:07 UTC is not answered by the copy"}, true},
		{"wrapped", fmt.Errorf("run: %w", &QueryError{Message: "Invalid Input Error: FROM_UNIXTIME of a negative, fractional or past-2038 number is not answered by the copy"}), true},
		{"another query error", &QueryError{Message: "Catalog Error: Table with name t does not exist"}, false},
		{"the marker outside a query error", errors.New("FROM_UNIXTIME of a negative, fractional or past-2038 number is not answered by the copy"), false},
		{"a refusal", &RefusedError{Reason: "is not answered by the copy"}, false},
	} {
		if got := DeclinedValue(c.err); got != c.want {
			t.Errorf("%s: DeclinedValue = %v, want %v", c.name, got, c.want)
		}
	}
}

// The macros' messages carry the mark DeclinedValue reads: a reworded
// message without it would turn every declined value back into a fault.
func TestMacroMessagesCarryTheMark2493(t *testing.T) {
	if n := strings.Count(unixTimeMacros, valueDeclinedMark+"')"); n != 4 {
		t.Fatalf("the macros' error messages end with %q %d times, want 4", valueDeclinedMark, n)
	}
}
