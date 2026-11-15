package sqlsandbox

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// PostgreSQL's 'infinity' and '-infinity', which a copy of a PostgreSQL
// table keeps, print as the engine prints them. They used to print as the
// date the engine stores them under: 5881580-07-11 for a DATE and
// 294247-01-10T04:00:54.775807Z for a timestamp, which read like real values.
func TestRun_infinityDatesPrintAsTheEngine(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	exprs := []string{
		"'infinity'::DATE", "'-infinity'::DATE",
		"'infinity'::TIMESTAMP", "'-infinity'::TIMESTAMP",
		"'infinity'::TIMESTAMPTZ", "'-infinity'::TIMESTAMPTZ",
		"'infinity'::TIMESTAMP_S", "'-infinity'::TIMESTAMP_S",
		"'infinity'::TIMESTAMP_MS", "'-infinity'::TIMESTAMP_MS",
		"'infinity'::TIMESTAMP_NS", "'-infinity'::TIMESTAMP_NS",
	}
	var sel []string
	for i, e := range exprs {
		sel = append(sel, e+" AS v"+strconv.Itoa(i), "("+e+")::VARCHAR AS t"+strconv.Itoa(i))
	}
	res, err := r.Run(context.Background(), f.job("SELECT "+strings.Join(sel, ", ")))
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range exprs {
		got, want := res.Rows[0][2*i], res.Rows[0][2*i+1]
		if got != want {
			t.Errorf("%s (%s) = %#v, want the engine's %#v", e, res.Columns[2*i].Type, got, want)
		}
		if s, _ := got.(string); !strings.HasSuffix(s, "infinity") {
			t.Errorf("%s = %#v, want infinity by name", e, got)
		}
	}

	// Inside a list too, and beside it an ordinary value.
	res, err = r.Run(context.Background(), f.job(`SELECT ['infinity'::DATE, DATE '2026-01-02'] AS ld, ['-infinity'::TIMESTAMP] AS lt`))
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Rows[0][0]; !strings.Contains(toJSON(t, got), `"infinity"`) || !strings.Contains(toJSON(t, got), `"2026-01-02"`) {
		t.Errorf("a list of dates = %s", toJSON(t, got))
	}
	if got := res.Rows[0][1]; !strings.Contains(toJSON(t, got), `"-infinity"`) {
		t.Errorf("a list of timestamps = %s", toJSON(t, got))
	}

	// The largest and smallest values that are not infinity stay what they
	// are.
	res, err = r.Run(context.Background(), f.job(`SELECT
		DATE '5881580-07-10' AS d, TIMESTAMP '294247-01-10 04:00:54.775806' AS ts,
		DATE '0001-01-01' AS d0, TIMESTAMP '1970-01-01 00:00:00' AS ts0`))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"5881580-07-10", "294247-01-10T04:00:54.775806Z", "0001-01-01", "1970-01-01T00:00:00Z"} {
		if got := res.Rows[0][i]; got != want {
			t.Errorf("%s = %#v, want %q", res.Columns[i].Name, got, want)
		}
	}
}

func toJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
