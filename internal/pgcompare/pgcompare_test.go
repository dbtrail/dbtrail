package pgcompare

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// TestMain doubles as the SQL sandbox's worker: the integration test
// re-executes this test binary, the way the console binary re-executes
// itself, so the copy is queried across the real process boundary.
func TestMain(m *testing.M) {
	if sqlsandbox.IsWorkerProcess() {
		os.Exit(sqlsandbox.WorkerMain(os.Stdin, os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

func TestIsQuery(t *testing.T) {
	for _, tc := range []struct {
		stmt string
		want bool
	}{
		{"SELECT 1", true},
		{"  select 1", true},
		{"(SELECT 1) UNION (SELECT 2)", true},
		{"WITH x AS (SELECT 1) SELECT * FROM x", true},
		{"VALUES (1), (2)", true},
		{"TABLE orders", true},
		{"-- note\nSELECT 1", true},
		{"/* a */ /* b */ select 1", true},
		{"selectx 1", false},
		{"DELETE FROM orders", false},
		{"INSERT INTO t SELECT 1", false},
		{"SET default_transaction_read_only = off", false},
		{"EXPLAIN SELECT 1", false},
		{"", false},
		{"   ", false},
		{"-- only a comment", false},
		{"/* never closed select 1", false},
		{"((", false},
	} {
		if got := IsQuery(tc.stmt); got != tc.want {
			t.Errorf("IsQuery(%q) = %v, want %v", tc.stmt, got, tc.want)
		}
	}
}

func TestCellText(t *testing.T) {
	str := func(s string) *string { return &s }
	for _, tc := range []struct {
		name string
		typ  string
		in   any
		want *string
	}{
		{"null", "INTEGER", nil, nil},
		{"true", "BOOLEAN", true, str("t")},
		{"false", "BOOLEAN", false, str("f")},
		{"whole number keeps every digit", "BIGINT", json.Number("9007199254740993"), str("9007199254740993")},
		{"decimal keeps its scale", "DECIMAL(10,2)", "10.00", str("10.00")},
		{"text is untouched", "VARCHAR", "2026-01-02T03:04:05Z", str("2026-01-02T03:04:05Z")},
		{"empty text is not null", "VARCHAR", "", str("")},
		{"timestamp", "TIMESTAMP", "2026-01-02T03:04:05Z", str("2026-01-02 03:04:05")},
		{"timestamp fraction drops trailing zeros", "TIMESTAMP", "2026-01-02T03:04:05.120000Z", str("2026-01-02 03:04:05.12")},
		{"timestamp microseconds", "TIMESTAMP", "2026-01-02T03:04:05.000001Z", str("2026-01-02 03:04:05.000001")},
		{"timestamp with zone", "TIMESTAMP WITH TIME ZONE", "2026-01-02T03:04:05Z", str("2026-01-02 03:04:05+00")},
		{"timestamp that does not parse is left", "TIMESTAMP", "infinity", str("infinity")},
		{"date", "DATE", "2026-01-02", str("2026-01-02")},
		{"list", "INTEGER[]", []any{json.Number("1"), nil}, str("[1,null]")},
	} {
		got := CellText(tc.typ, tc.in)
		switch {
		case got == nil && tc.want == nil:
		case got == nil || tc.want == nil || *got != *tc.want:
			t.Errorf("%s: CellText(%q, %#v) = %v, want %v", tc.name, tc.typ, tc.in, deref(got), deref(tc.want))
		}
	}
}

func deref(s *string) string {
	if s == nil {
		return "<NULL>"
	}
	return "\"" + *s + "\""
}

// fakeSide answers from a list, one entry per call.
type fakeSide struct {
	answers []fakeAnswer
	calls   []string
}

type fakeAnswer struct {
	rows sqlcompare.Rows
	over bool
	err  error
}

func (f *fakeSide) Query(_ context.Context, stmt string, _ int) (sqlcompare.Rows, bool, error) {
	f.calls = append(f.calls, stmt)
	if len(f.answers) == 0 {
		return sqlcompare.Rows{}, false, errors.New("fakeSide: no answer left")
	}
	a := f.answers[0]
	f.answers = f.answers[1:]
	return a.rows, a.over, a.err
}

func oneCol(vals ...string) sqlcompare.Rows {
	r := sqlcompare.Rows{Columns: []string{"v"}}
	for _, v := range vals {
		r.Rows = append(r.Rows, []*string{&v})
	}
	return r
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	run := func(t *testing.T, src, cp *fakeSide, opts Options, stmt string) Result {
		t.Helper()
		rep, err := Run(ctx, src, cp, opts, []string{stmt})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if len(rep.Results) != 1 || rep.Counts[rep.Results[0].Verdict] != 1 {
			t.Fatalf("report = %+v", rep)
		}
		return rep.Results[0]
	}

	t.Run("equal", func(t *testing.T) {
		r := run(t, &fakeSide{answers: []fakeAnswer{{rows: oneCol("a", "b")}}},
			&fakeSide{answers: []fakeAnswer{{rows: oneCol("b", "a")}}}, Options{}, "SELECT v FROM t")
		if r.Verdict != sqlcompare.Equal || r.SourceRows != 2 || r.CopyRows != 2 {
			t.Fatalf("got %+v", r)
		}
	})
	t.Run("order counts under ORDER BY", func(t *testing.T) {
		r := run(t, &fakeSide{answers: []fakeAnswer{{rows: oneCol("a", "b")}, {rows: oneCol("a", "b")}}},
			&fakeSide{answers: []fakeAnswer{{rows: oneCol("b", "a")}}}, Options{}, "SELECT v FROM t ORDER BY v")
		if r.Verdict != sqlcompare.Different || r.Kind != "order" {
			t.Fatalf("got %+v", r)
		}
	})
	t.Run("a write is not sent to either side", func(t *testing.T) {
		src, cp := &fakeSide{}, &fakeSide{}
		r := run(t, src, cp, Options{}, "DELETE FROM t")
		if r.Verdict != sqlcompare.Skipped || len(src.calls) != 0 || len(cp.calls) != 0 {
			t.Fatalf("got %+v, source calls %v, copy calls %v", r, src.calls, cp.calls)
		}
	})
	t.Run("source error stops before the copy", func(t *testing.T) {
		cp := &fakeSide{}
		r := run(t, &fakeSide{answers: []fakeAnswer{{err: errors.New("relation \"t\" does not exist")}}}, cp, Options{}, "SELECT 1 FROM t")
		if r.Verdict != sqlcompare.SourceError || !strings.Contains(r.Detail, "does not exist") || len(cp.calls) != 0 {
			t.Fatalf("got %+v, copy calls %v", r, cp.calls)
		}
	})
	t.Run("copy refusal", func(t *testing.T) {
		r := run(t, &fakeSide{answers: []fakeAnswer{{rows: oneCol("a")}}},
			&fakeSide{answers: []fakeAnswer{{err: &sqlsandbox.QueryError{Message: "Binder Error: no function"}}}}, Options{}, "SELECT v FROM t")
		if r.Verdict != sqlcompare.NotOnCopy || !strings.Contains(r.Detail, "Binder Error") {
			t.Fatalf("got %+v", r)
		}
	})
	t.Run("cut on either side is inconclusive", func(t *testing.T) {
		for _, side := range []string{"source", "copy"} {
			src := &fakeSide{answers: []fakeAnswer{{rows: oneCol("a"), over: side == "source"}}}
			cp := &fakeSide{answers: []fakeAnswer{{rows: oneCol("b"), over: side == "copy"}}}
			r := run(t, src, cp, Options{}, "SELECT v FROM t")
			if r.Verdict != sqlcompare.Inconclusive || r.Kind != "over_max_rows" {
				t.Fatalf("%s cut: got %+v", side, r)
			}
		}
	})
	t.Run("a live write is inconclusive", func(t *testing.T) {
		r := run(t, &fakeSide{answers: []fakeAnswer{{rows: oneCol("a")}, {rows: oneCol("b")}}},
			&fakeSide{answers: []fakeAnswer{{rows: oneCol("b")}}}, Options{}, "SELECT v FROM t")
		if r.Verdict != sqlcompare.Inconclusive || r.Kind != "source_changed" {
			t.Fatalf("got %+v", r)
		}
	})
	t.Run("a difference that holds on the second read stands", func(t *testing.T) {
		src := &fakeSide{answers: []fakeAnswer{{rows: oneCol("a")}, {rows: oneCol("a")}}}
		r := run(t, src, &fakeSide{answers: []fakeAnswer{{rows: oneCol("b")}}}, Options{}, "SELECT v FROM t")
		if r.Verdict != sqlcompare.Different || len(src.calls) != 2 {
			t.Fatalf("got %+v, source calls %d", r, len(src.calls))
		}
	})
	t.Run("NoRerun reads the source once", func(t *testing.T) {
		src := &fakeSide{answers: []fakeAnswer{{rows: oneCol("a")}}}
		r := run(t, src, &fakeSide{answers: []fakeAnswer{{rows: oneCol("b")}}}, Options{NoRerun: true}, "SELECT v FROM t")
		if r.Verdict != sqlcompare.Different || len(src.calls) != 1 {
			t.Fatalf("got %+v, source calls %d", r, len(src.calls))
		}
	})
	t.Run("a failed second read keeps the difference and says so", func(t *testing.T) {
		src := &fakeSide{answers: []fakeAnswer{{rows: oneCol("a")}, {err: errors.New("canceling statement")}}}
		r := run(t, src, &fakeSide{answers: []fakeAnswer{{rows: oneCol("b")}}}, Options{}, "SELECT v FROM t")
		if r.Verdict != sqlcompare.Different || !strings.Contains(r.Detail, "second read of the source failed") {
			t.Fatalf("got %+v", r)
		}
	})
	t.Run("a lost side stops the run instead of filing a verdict", func(t *testing.T) {
		for _, side := range []string{"source", "copy"} {
			src := &fakeSide{answers: []fakeAnswer{{rows: oneCol("a")}}}
			cp := &fakeSide{answers: []fakeAnswer{{rows: oneCol("a")}}}
			lost := &FatalError{Err: errors.New("gone")}
			if side == "source" {
				src.answers = []fakeAnswer{{err: lost}}
			} else {
				cp.answers = []fakeAnswer{{err: lost}}
			}
			rep, err := Run(ctx, src, cp, Options{}, []string{"SELECT v FROM t"})
			if err == nil || rep != nil || !strings.Contains(err.Error(), side+": gone") {
				t.Fatalf("%s lost: rep %+v, err %v", side, rep, err)
			}
		}
	})
}
