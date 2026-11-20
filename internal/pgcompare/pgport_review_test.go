package pgcompare

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/sqlcompare"
)

// Findings of a review of the PostgreSQL port, each pinned by the case that
// showed it.

// Block comments nest in PostgreSQL: what follows the outer one's end is the
// statement.
func TestIsQueryNestedComments(t *testing.T) {
	for _, tc := range []struct {
		stmt string
		want bool
	}{
		{"/* /* */ SELECT */ DELETE FROM t", false},
		{"/* /* */ */ DELETE FROM t", false},
		{"/* /* a */ b */ SELECT 1", true},
		{"/* /* a */ b */ (SELECT 1)", true},
		{"/* /* */ never closed SELECT 1", false},
		{"/*/ SELECT 1", false},
		{"/**/SELECT 1", true},
	} {
		if got := IsQuery(tc.stmt); got != tc.want {
			t.Errorf("IsQuery(%q) = %v, want %v", tc.stmt, got, tc.want)
		}
	}
}

// A function written as a quoted name is the same function.
func TestReadOnlyVetoQuotedNames(t *testing.T) {
	for _, tc := range []struct {
		stmt   string
		vetoed bool
	}{
		{`SELECT "pg_advisory_lock"(1)`, true},
		{`SELECT "pg_sleep"(50)`, true},
		{`SELECT "pg_sleep" (50)`, true},
		{`SELECT pg_catalog."pg_sleep"(1)`, true},
		{`SELECT "pg_catalog"."set_config"('a', 'b', false)`, true},
		{`SELECT "pg_sleep" FROM t`, false},
		{`SELECT 1 AS "for update" FROM t`, false},
		{`SELECT 'pg_sleep(1)'`, false},
		{`SELECT "a""b" FROM t`, false},
	} {
		if got := ReadOnlyVeto(tc.stmt) != ""; got != tc.vetoed {
			t.Errorf("ReadOnlyVeto(%q) vetoed = %v, want %v", tc.stmt, got, tc.vetoed)
		}
	}
}

// A second read of the source cut at the limit is not a whole answer: it
// matching the copy must not make a difference read as a live write.
func TestRunSecondReadCutKeepsTheDifference(t *testing.T) {
	src := &fakeSide{answers: []fakeAnswer{{rows: oneCol("a")}, {rows: oneCol("b"), over: true}}}
	cp := &fakeSide{answers: []fakeAnswer{{rows: oneCol("b")}}}
	rep, err := Run(context.Background(), src, cp, Options{MaxRows: 1}, []string{"SELECT v FROM t"})
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	if r.Verdict != sqlcompare.Different || !strings.Contains(r.Detail, "second read of the source had more than 1 rows") {
		t.Fatalf("got %+v", r)
	}
}
