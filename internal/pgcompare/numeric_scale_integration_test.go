//go:build integration

package pgcompare

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
)

// numericCase is one statement of the numeric's decimals, with what it
// gives through a connection that can reach its source (fwd, the copy as
// pgshim.RewriteForCopy writes it) and through one the copy answers alone
// (alone, pgshim.RewriteForCopyAlone). Alone, a statement is EQUAL,
// NOT_ON_COPY or SOURCE_ERROR, never DIFFERENT: nothing else would answer
// it there.
type numericCase struct {
	stmt       string
	fwd, alone expectation
}

// numericCases are measured against PostgreSQL 16, which writes, for
// instance: round(500.00, 2) and round(500, 2) as 500.00, round(12.50, 3)
// as 12.500, round(-0.004, 2) as 0.00, round(-5.0, -1) as -10, round(-0.5)
// as -1, round(2.5::float8) as 2 (half to even), round(5) as a double
// precision, avg(x) of a numeric with 16 to 20 decimals
// (500.0000000000000000, 0.50000000000000000000), and CASE WHEN c THEN
// round(avg(x), 2) ELSE 0 END as 500.00 or 0, each value with its own
// decimals. Before the copy's answers were rewritten on every connection, a
// connection without a forwarding account wrote round(avg(x), 2) of a
// numeric as 500 (#2756's bench report), round(x, 3) of a numeric(12,2) as
// 12.50 and round(qty, 2) as 5.
func numericCases() []numericCase {
	o, c, r := itOrders, itCustomers, itRound
	eq := expectation{verdict: sqlcompare.Equal}
	refused := func(says string) expectation {
		return expectation{verdict: sqlcompare.NotOnCopy, says: says, why: "by design: refused, never answered otherwise"}
	}
	srcErr := expectation{verdict: sqlcompare.SourceError}
	both := func(stmt string, e expectation) numericCase { return numericCase{stmt, e, e} }
	return []numericCase{
		// The bench's report: a numeric's avg rounded to 2, whole (10.00),
		// negative (-2.50), ending in a zero (11.70).
		both(`SELECT c.name, count(*) AS orders, round(sum(o.amount), 2) AS revenue, round(avg(o.amount), 2) AS avg_amount, count(DISTINCT o.customer_id) AS buyers
		    FROM `+o+` o JOIN `+c+` c ON c.id = o.customer_id WHERE o.amount IS NOT NULL GROUP BY c.name ORDER BY revenue DESC`, eq),
		both(`SELECT round(avg(amount), 2) FROM `+o+` WHERE id = 2`, eq),
		both(`SELECT customer_id, round(avg(amount), 2), round(avg(amount)), round(avg(qty)) FROM `+o+` GROUP BY customer_id ORDER BY customer_id`, eq),
		// round(x, k), round(x, -k) and round(x) of literals.
		both(`SELECT round(0, 2), round(0.0, 2), round(500, 2), round(500.00, 2), round(-12.345, 2), round(-0.004, 2), round(12.5, 0)`, eq),
		both(`SELECT round(15, -1), round(-15, -1), round(-25, -1), round(-5.0, -1), round(-4.0, -1), round(14.99, -1), round(1234.5, -5)`, eq),
		both(`SELECT round(12.5), round(-12.5), round(-0.5), round(-0.4), round(500.00)`, eq),
		// Of a numeric(12,2) column read as it is, and rounded.
		both(`SELECT id, amount FROM `+o+` ORDER BY id`, eq),
		both(`SELECT id, round(amount, 2), round(amount, 3), round(amount, 1), round(amount, 0), round(amount, -1), round(amount) FROM `+o+` ORDER BY id`, eq),
		// Of a whole number: exactly k decimals; round(qty) is a double
		// precision on PostgreSQL.
		both(`SELECT id, round(qty, 2), round(qty, 0), round(qty, -1) FROM `+o+` ORDER BY id`, eq),
		both(`SELECT id, round(qty) FROM `+o+` ORDER BY id`, refused("whole number")),
		// Of a double precision: half to even on PostgreSQL, and no
		// round(double precision, k) at all.
		both(`SELECT id, round(ratio) FROM `+o+` ORDER BY id`, refused("half to even")),
		both(`SELECT id, round(ratio, 2) FROM `+o+` ORDER BY id`, srcErr),
		// Sums.
		both(`SELECT sum(amount), round(sum(amount), 2), round(sum(amount), 3), round(sum(qty), 2), round(sum(id), 1) FROM `+o, eq),
		// avg of a numeric or a whole number: PostgreSQL's 16 to 20 decimals.
		// A connection that can reach its source answers the quotient to 16
		// decimals (#3219), PostgreSQL's own text for these; alone it is
		// refused as before.
		{`SELECT avg(amount) FROM ` + o, eq, refused("avg()")},
		{`SELECT customer_id, avg(qty) FROM ` + o + ` GROUP BY customer_id ORDER BY customer_id`, eq, refused("avg()")},
		// NULL.
		both(`SELECT round(avg(amount), 2), round(amount, 2), round(amount), round(amount, -1) FROM `+o+` WHERE id = 7 GROUP BY amount`, eq),
		// Inside a CASE or a COALESCE: each value keeps its decimals on
		// PostgreSQL, so a 0 beside a round(..., 2) is refused, and a value
		// with the round's decimals, or NULL, is answered.
		both(`SELECT customer_id, CASE WHEN customer_id = 1 THEN round(avg(amount), 2) ELSE 0 END FROM `+o+` GROUP BY customer_id ORDER BY customer_id`, refused("different numbers of decimals")),
		both(`SELECT customer_id, CASE WHEN customer_id = 1 THEN round(avg(amount), 2) ELSE 0.00 END FROM `+o+` GROUP BY customer_id ORDER BY customer_id`, eq),
		both(`SELECT customer_id, CASE WHEN customer_id < 4 THEN round(avg(amount), 2) END AS a FROM `+o+` GROUP BY customer_id ORDER BY customer_id`, eq),
		both(`SELECT customer_id, coalesce(round(avg(amount), 2), 0) FROM `+o+` GROUP BY customer_id ORDER BY customer_id`, eq),
		both(`SELECT coalesce(round(avg(amount), 2), 0) FROM `+o+` WHERE id = 7`, refused("different numbers of decimals")),
		both(`SELECT id FROM `+o+` WHERE CASE WHEN qty > 5 THEN round(amount, 1) ELSE 0 END > 9 ORDER BY id`, refused("different numbers of decimals")),
		both(`SELECT id, CASE WHEN round(amount, 1) > 9 THEN 'big' ELSE 'small' END FROM `+o+` ORDER BY id`, eq),
		// A name that stands for a round, read from a subquery, is checked as
		// the round is.
		both(`SELECT coalesce(r, 0) FROM (SELECT round(amount, 2) AS r FROM `+o+` WHERE id = 7) s`, refused("different numbers of decimals")),
		both(`SELECT coalesce(r, 0) FROM (SELECT round(amount, 2) AS r FROM `+o+` WHERE id = 2) s`, eq),
		{`SELECT id, lag(round(amount, 2), 1, 0) OVER (ORDER BY id) FROM ` + o + ` ORDER BY id`,
			expectation{verdict: sqlcompare.Different, kind: "precision", why: "the screen sends it to the source"}, refused("lag")},
		// Beside other values in one column: refused alone; with a
		// forwarding account the screen sends it to the source.
		{`SELECT round(amount, 2) FROM ` + o + ` WHERE id = 6 UNION ALL SELECT 0`,
			expectation{verdict: sqlcompare.Different, kind: "precision", why: "the screen sends it to the source"}, refused("UNION")},
		{`SELECT greatest(round(amount, 2), 0) FROM ` + o + ` WHERE id = 5`,
			expectation{verdict: sqlcompare.Different, kind: "precision", why: "the screen sends it to the source"}, refused("greatest")},
		// Quotients on a half at each k and 1,500 random values, of both
		// signs: the copy rounds half away from zero, as PostgreSQL.
		both(`SELECT id, round(n, 0), round(n, 1), round(n, 3), round(n, 5), round(n, 8), round(n, -1), round(n, -3), round(n) FROM `+r+` ORDER BY id`, eq),
		both(`SELECT id, round(d, 2), round(d, 4), round(d, -2), round(i, 2), round(i, -1), round(i, -3), round(d) FROM `+r+` ORDER BY id`, eq),
		both(`SELECT g, round(sum(n), 2), round(sum(i), 1), round(avg(d)), round(avg(i)), round(100 * avg(n)) FROM `+r+` GROUP BY g ORDER BY g`, eq),
		// What the macros refuse alone too.
		both(`SELECT round(9 / 2, 1)`, refused("both sides of the division are whole numbers")),
		{`SELECT id, round(amount / qty + 1, 2) FROM ` + o + ` WHERE qty <> 0 ORDER BY id`,
			expectation{verdict: sqlcompare.Different, kind: "precision", why: "a division inside a round: the screen sends it to the source"},
			refused("round() of this shape")},
	}
}

// numericForward is numericCases as the main list measures them: through a
// connection that can reach its source.
func numericForward() []expectation {
	var out []expectation
	for _, c := range numericCases() {
		e := c.fwd
		e.stmt = c.stmt
		out = append(out, e)
	}
	return out
}

// checkNumericAlone runs numericCases through the copy as a connection with
// no forwarding account hands it each statement, refusals included.
func checkNumericAlone(t *testing.T, ctx context.Context, src *Source, alone *SnapshotCopy) {
	t.Helper()
	alone.Rewrite = func(s string) string {
		out, _ := pgshim.RewriteForCopyAlone(s)
		return out
	}
	alone.Refuse = func(s string) error {
		_, err := pgshim.RewriteForCopyAlone(s)
		return err
	}
	cases := numericCases()
	stmts := make([]string, len(cases))
	for i, c := range cases {
		stmts[i] = c.stmt
	}
	rep, err := Run(ctx, src, alone, Options{NoRerun: true, MaxRows: 2000}, stmts)
	if err != nil {
		t.Fatalf("Run, alone: %v", err)
	}
	for i, r := range rep.Results {
		e := cases[i].alone
		line := fmt.Sprintf("%-13s %-10s %s\n    detail: %s", r.Verdict, r.Kind, r.Statement, r.Detail)
		if r.Verdict == sqlcompare.Different {
			t.Errorf("alone, answered otherwise than PostgreSQL:\n  %s", line)
			continue
		}
		if r.Verdict != e.verdict || r.Kind != e.kind || !strings.Contains(r.Detail, e.says) {
			t.Errorf("alone, want %s/%q (%s; says %q), got:\n  %s", e.verdict, e.kind, e.why, e.says, line)
			continue
		}
		t.Log("alone: " + line)
	}
}
