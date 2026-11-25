//go:build integration

package pgcompare

import (
	"context"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
)

// closeCase is one statement with a division or an avg() written without a
// round() (#3219), which the copy answers as PostgreSQL's quotient rounded
// to 16 decimals where PostgreSQL writes it with 16 significant digits or
// more: the same rows in the same order, every cell the same text, except
// that a number with decimals may be written with more or fewer of them and
// must then be the same number once both are rounded to the fewer
// (sameButDecimals). refused is a piece of the copy's refusal when the copy
// does not answer and the statement goes to the source; sourceFails is a
// statement PostgreSQL itself fails.
type closeCase struct {
	stmt        string
	refused     string
	sourceFails bool
}

func closeCases() []closeCase {
	o, r, odd := itOrders, itRound, itOdd
	return []closeCase{
		// avg of a numeric and of a whole number.
		{stmt: `SELECT avg(amount) FROM ` + o},
		{stmt: `SELECT sum(amount), avg(amount) FROM ` + o},
		{stmt: `SELECT avg(qty) FROM ` + o},
		{stmt: `SELECT customer_id, avg(qty) FROM ` + o + ` GROUP BY customer_id ORDER BY customer_id`},
		{stmt: `SELECT customer_id, avg(amount), avg(qty), avg(id), count(*) FROM ` + o + ` GROUP BY customer_id ORDER BY customer_id`},
		{stmt: `SELECT avg(amount), avg(qty) FROM ` + o + ` WHERE id = 7`},
		{stmt: `SELECT avg(amount) FROM ` + o + ` WHERE id > 100`},
		{stmt: `SELECT avg(fixed) FROM ` + odd},
		{stmt: `SELECT g, avg(d), avg(i), avg(n) FROM ` + r + ` GROUP BY g ORDER BY g`},
		{stmt: `SELECT avg(amount * 2 + qty), avg(coalesce(amount, 0)), avg(abs(qty)), 3 * avg(amount) FROM ` + o},
		// Used to decide which rows come out, and in which order.
		{stmt: `SELECT customer_id FROM ` + o + ` GROUP BY customer_id HAVING avg(amount) > 5 ORDER BY customer_id`},
		{stmt: `SELECT id FROM ` + o + ` WHERE amount > (SELECT avg(amount) FROM ` + o + `) ORDER BY id`},
		{stmt: `SELECT customer_id, avg(amount) AS a FROM ` + o + ` GROUP BY customer_id ORDER BY a DESC NULLS LAST, customer_id`},
		{stmt: `SELECT id FROM ` + o + ` WHERE amount / 3 = 3.3 OR amount / 4 = 2.5 ORDER BY id`},
		{stmt: `SELECT amount / nullif(qty, 0) AS unit, count(*) FROM ` + o + ` GROUP BY amount / nullif(qty, 0) ORDER BY unit NULLS LAST`},
		// A division on its own.
		{stmt: `SELECT sum(amount) / 2.0, sum(amount) / 100 FROM ` + o},
		{stmt: `SELECT 100.00 * sum(amount) / (1 + sum(amount)) AS share FROM ` + o},
		{stmt: `SELECT customer_id, sum(amount) / count(*), count(*) / sum(amount) FROM ` + o + ` WHERE amount > 0 GROUP BY customer_id ORDER BY customer_id`},
		{stmt: `SELECT id, amount / qty, qty / amount FROM ` + o + ` WHERE qty <> 0 AND amount <> 0 ORDER BY id`},
		{stmt: `SELECT id, amount / 7.0, qty / 4.0, 1 / amount, id / 2.5 FROM ` + o + ` WHERE amount <> 0 ORDER BY id`},
		{stmt: `SELECT id, qty / 2.0 FROM ` + o + ` ORDER BY id`},
		{stmt: `SELECT id, amount / 3, amount / 100, amount / -7, amount / 2 FROM ` + o + ` ORDER BY id`},
		{stmt: `SELECT id, fixed / 3 FROM ` + odd + ` ORDER BY id`},
		{stmt: `SELECT id, amount / qty FROM ` + o + ` WHERE id = 7`},
		{stmt: `SELECT id, amount / nullif(qty, 0) FROM ` + o + ` ORDER BY id`},
		{stmt: `SELECT id FROM ` + o + ` WHERE amount / nullif(qty, 0) > 1 ORDER BY id`},
		{stmt: `SELECT id, CASE WHEN qty <> 0 THEN amount / qty END FROM ` + o + ` ORDER BY id`},
		{stmt: `SELECT id FROM ` + o + ` WHERE qty <> 0 AND amount / qty > 1 ORDER BY id`},
		{stmt: `SELECT id, amount / qty / 3.0, amount * 2 / qty, -amount / qty, amount / -qty FROM ` + o + ` WHERE qty <> 0 ORDER BY id`},
		{stmt: `SELECT id, d / nullif(i, 0), i / nullif(d, 0), n / nullif(d, 0) FROM ` + r + ` ORDER BY id`},
		{stmt: `SELECT g, sum(d) / count(*), sum(d) / nullif(sum(i), 0) FROM ` + r + ` GROUP BY g ORDER BY g`},
		{stmt: `SELECT avg(amount) / 2.0, sum(amount) / avg(qty) FROM ` + o},
		{stmt: `SELECT avg(amount / nullif(qty, 0)), sum(amount / nullif(qty, 0)) FROM ` + o},
		{stmt: `SELECT DISTINCT ON (customer_id) customer_id, amount / nullif(qty, 0) FROM ` + o + ` ORDER BY customer_id, id`},
		// A round beside a division in one CASE: PostgreSQL keeps each value's
		// own decimals, and the check of a CASE around a round refuses it.
		{stmt: `SELECT id, CASE WHEN qty > 5 THEN round(amount, 2) ELSE amount / nullif(qty, 0) END FROM ` + o + ` ORDER BY id`, refused: "different numbers of decimals"},
		{stmt: `SELECT id, CASE WHEN qty > 5 THEN amount / 2 ELSE amount / nullif(qty, 0) END FROM ` + o + ` ORDER BY id`},
		{stmt: `SELECT id, round(amount / nullif(qty, 0), 2), amount / nullif(qty, 0) FROM ` + o + ` ORDER BY id`},
		// A quotient so small that 16 decimals would lose it: the copy
		// refuses, and the source answers.
		{stmt: `SELECT id FROM ` + o + ` WHERE id = 1 AND amount / 90000000 > 0`, refused: "under 0.000001"},
		// PostgreSQL's own error: a divisor of zero.
		{stmt: `SELECT id, amount / qty FROM ` + o + ` ORDER BY id`, sourceFails: true},
		{stmt: `SELECT sum(amount) / sum(qty - qty) FROM ` + o, sourceFails: true},

		// A CASE on a side, read by its values (caseClass): the shapes of
		// CH-benCHmark's Q8 and Q14, a share of a total.
		{stmt: `SELECT sum(CASE WHEN qty > 5 THEN amount ELSE 0 END) / sum(amount) FROM ` + o},
		{stmt: `SELECT 100.00 * sum(CASE WHEN status LIKE 'p%' THEN amount ELSE 0 END) / (1 + sum(amount)) AS promo FROM ` + o},
		{stmt: `SELECT (100.00 * sum(CASE WHEN status LIKE 'p%' THEN amount ELSE 0 END) / (1 + sum(amount))) AS promo FROM ` + o + ` WHERE id < 3`},
		// No row matching: a sum of NULLs is NULL, and a share of zero.
		{stmt: `SELECT customer_id, sum(CASE WHEN qty > 5 THEN amount END) / count(*), sum(CASE WHEN qty > 5 THEN amount ELSE 0 END) / nullif(sum(amount), 0) FROM ` + o + ` GROUP BY customer_id ORDER BY customer_id`},
		{stmt: `SELECT sum(CASE WHEN qty > 5 THEN amount END) / sum(amount) FROM ` + o + ` WHERE id = 7`},
		// A whole number and a numeric among the values, and a count of
		// matches over a count, made a numeric by its factor.
		{stmt: `SELECT sum(CASE WHEN qty > 5 THEN qty ELSE amount END) / sum(amount), sum(CASE WHEN qty > 5 THEN qty ELSE 0 END) / sum(amount) FROM ` + o},
		{stmt: `SELECT 100.0 * sum(CASE WHEN qty > 5 THEN 1 ELSE 0 END) / count(*) FROM ` + o},
		// Signs on either side, a simple CASE, one inside another.
		{stmt: `SELECT -sum(CASE WHEN qty > 5 THEN amount ELSE 0 END) / sum(amount), sum(amount) / -sum(CASE WHEN qty > 5 THEN amount ELSE 0 END) FROM ` + o},
		{stmt: `SELECT sum(CASE WHEN qty < 3 THEN amount ELSE 0 END) / sum(CASE WHEN qty < 3 THEN -amount END) FROM ` + o},
		{stmt: `SELECT sum(CASE customer_id WHEN 1 THEN amount WHEN 2 THEN qty ELSE 0 END) / sum(amount) FROM ` + o},
		{stmt: `SELECT sum(CASE WHEN qty > 5 THEN CASE WHEN status = 'paid' THEN amount ELSE 1 END ELSE 0 END) / sum(amount) FROM ` + o},
		// Quotients past 16 decimals, which round.
		{stmt: `SELECT sum(CASE WHEN id <= 2 THEN amount ELSE 0 END) / 7, sum(CASE WHEN id = 2 THEN amount ELSE 0 END) / 3, sum(CASE WHEN id = 9 THEN amount END) / 9 FROM ` + o},
		// Row by row, with a divisor that is NULL where it would be zero.
		{stmt: `SELECT id, amount / nullif(CASE WHEN qty > 5 THEN qty ELSE 0 END, 0) FROM ` + o + ` ORDER BY id`},
		// avg of a CASE.
		{stmt: `SELECT avg(CASE WHEN qty > 5 THEN amount ELSE 0 END), avg(CASE WHEN qty > 5 THEN qty END) FROM ` + o},
		// Used to choose and to sort the rows.
		{stmt: `SELECT customer_id FROM ` + o + ` GROUP BY customer_id HAVING sum(CASE WHEN qty > 5 THEN amount ELSE 0 END) / nullif(sum(amount), 0) > 0.5 ORDER BY customer_id`},
		{stmt: `SELECT customer_id, sum(CASE WHEN qty > 5 THEN amount ELSE 0 END) / nullif(sum(amount), 0) AS share FROM ` + o + ` GROUP BY customer_id ORDER BY share DESC NULLS LAST, customer_id`},
		// Too small a quotient: the copy refuses, the source answers.
		{stmt: `SELECT sum(CASE WHEN id = 1 THEN amount ELSE 0 END) / 90000000 FROM ` + o, refused: "under 0.000001"},
		// A divisor of zero, as a sum and in a row: PostgreSQL's error.
		{stmt: `SELECT sum(amount) / sum(CASE WHEN qty > 1000 THEN qty ELSE 0 END) FROM ` + o, sourceFails: true},
		{stmt: `SELECT id, amount / (CASE WHEN qty > 5 THEN qty ELSE 0 END) FROM ` + o + ` ORDER BY id`, sourceFails: true},
	}
}

var plainNumber = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)

// roundTo is the numerator of x rounded half away from zero to k decimals,
// over 10^k.
func roundTo(x *big.Rat, k int) *big.Int {
	scaled := new(big.Rat).Mul(x, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(k)), nil)))
	twice := new(big.Int).Mul(scaled.Num(), big.NewInt(2))
	den := scaled.Denom()
	// floor((2n + d) / 2d) for a positive x, mirrored for a negative one.
	neg := twice.Sign() < 0
	twice.Abs(twice)
	q := new(big.Int).Div(new(big.Int).Add(twice, den), new(big.Int).Mul(den, big.NewInt(2)))
	if neg {
		q.Neg(q)
	}
	return q
}

// sameButDecimals reports whether two cells are the same text, or the same
// number written with different numbers of decimals: both plain numbers, the
// source's with decimals (a whole number or a text of the source must come
// back as the same text), and equal once both are rounded to the fewer
// decimals of the two.
func sameButDecimals(src, cp *string) bool {
	if src == nil || cp == nil {
		return src == nil && cp == nil
	}
	if *src == *cp {
		return true
	}
	if !plainNumber.MatchString(*src) || !plainNumber.MatchString(*cp) || !strings.Contains(*src, ".") || !strings.Contains(*cp, ".") {
		return false
	}
	decimals := func(s string) int { return len(s) - strings.IndexByte(s, '.') - 1 }
	k := min(decimals(*src), decimals(*cp))
	a, okA := new(big.Rat).SetString(*src)
	b, okB := new(big.Rat).SetString(*cp)
	if !okA || !okB || a.Sign() != b.Sign() {
		// A zero is never forgiven against a number that is not.
		return false
	}
	return roundTo(a, k).Cmp(roundTo(b, k)) == 0
}

// closeRows is "" when the copy's rows are the source's, cell by cell and in
// the source's order, each cell sameButDecimals as the other.
func closeRows(src, cp sqlcompare.Rows) string {
	if len(src.Rows) != len(cp.Rows) {
		return fmt.Sprintf("%d rows on the source, %d on the copy", len(src.Rows), len(cp.Rows))
	}
	for i := range src.Rows {
		if len(src.Rows[i]) != len(cp.Rows[i]) {
			return fmt.Sprintf("row %d: %d cells on the source, %d on the copy", i+1, len(src.Rows[i]), len(cp.Rows[i]))
		}
		for j := range src.Rows[i] {
			if !sameButDecimals(src.Rows[i][j], cp.Rows[i][j]) {
				return fmt.Sprintf("row %d, column %d: %s on the source, %s on the copy", i+1, j+1, cellText(src.Rows[i][j]), cellText(cp.Rows[i][j]))
			}
		}
	}
	return ""
}

func cellText(v *string) string {
	if v == nil {
		return "NULL"
	}
	return *v
}

// copyColumnTypes is the copy's column types by lowercased name, as the
// port's screen reads them (checkRouting).
func copyColumnTypes(t *testing.T, ctx context.Context, cp *SnapshotCopy) map[string][]string {
	t.Helper()
	cols, _, err := cp.Query(ctx, "SELECT lower(column_name), data_type FROM information_schema.columns WHERE table_schema = 'public'", 10000)
	if err != nil {
		t.Fatalf("the copy's column types: %v", err)
	}
	types := map[string][]string{}
	for _, r := range cols.Rows {
		if r[0] != nil && r[1] != nil {
			types[*r[0]] = append(types[*r[0]], strings.ToUpper(*r[1]))
		}
	}
	return types
}

func TestSameButDecimals(t *testing.T) {
	s := func(v string) *string { return &v }
	for _, c := range []struct {
		src, cp string
		want    bool
	}{
		{"7.5000000000000000", "7.5000000000000000", true},
		{"0.33333333333333333333", "0.3333333333333333", true},
		{"0.66666666666666666667", "0.6666666666666667", true},
		{"41152.263374485597", "41152.2633744855967078", true},
		{"-0.66666666666666666667", "-0.6666666666666667", true},
		{"0.00000000000000000000", "0.0000000000000000", true},
		{"0.33333333333333333333", "0.3333333333333334", false},
		{"0.10000000000000000000", "0.0999999999999999", false},
		{"0.00000000000000003333", "0.0000000000000000", false},
		{"-0.00000000000000003333", "0.0000000000000000", false},
		{"41152.263374485597", "41152.2633744855977078", false},
		{"7.5", "7.6", false},
		// A whole number or a text of the source must come back as written.
		{"3", "3.0000000000000000", false},
		{"3.0000000000000000", "3", false},
		{"abc", "abc", true},
		{"1e5", "100000.0", false},
		{"NaN", "NaN", true},
	} {
		if got := sameButDecimals(s(c.src), s(c.cp)); got != c.want {
			t.Errorf("sameButDecimals(%q, %q) = %v, want %v", c.src, c.cp, got, c.want)
		}
	}
	if !sameButDecimals(nil, nil) || sameButDecimals(nil, s("1.0")) || sameButDecimals(s("1.0"), nil) {
		t.Errorf("NULL is only the same as NULL")
	}
}

// checkCloseNumbers runs closeCases on the source and on the copy, as a
// connection that can reach its source hands the copy each statement. The
// comparison is this file's own: the list of TestIntegrationPGCompare
// compares text, and a statement here would be DIFFERENT there for its
// decimals alone. A connection the copy answers alone divides and averages
// as it did before #3219 (numericCases).
func checkCloseNumbers(t *testing.T, ctx context.Context, src *Source, fwd *SnapshotCopy, types map[string][]string) {
	t.Helper()
	fwd.Rewrite = func(s string) string { return pgshim.RewriteForCopyTyped(s, types) }
	differ := 0
	for _, c := range closeCases() {
		// The port must leave the statement to the copy: a statement it sent
		// to the source would prove nothing here.
		if veto := pgshim.CopyScreen(c.stmt, types); veto != "" && !c.sourceFails {
			t.Errorf("the port sends it to the source (%s), so the copy is never asked: %s", veto, c.stmt)
			continue
		}
		srcRows, _, srcErr := src.Query(ctx, c.stmt, 5000)
		if c.sourceFails {
			if srcErr == nil {
				t.Errorf("PostgreSQL answered, want its error: %s", c.stmt)
			}
			// The copy must not answer what PostgreSQL fails.
			if rows, _, err := fwd.Query(ctx, c.stmt, 5000); err == nil {
				t.Errorf("the copy answered a statement PostgreSQL fails (%d rows): %s", len(rows.Rows), c.stmt)
			} else if !strings.Contains(err.Error(), "division by zero") {
				t.Errorf("the copy failed otherwise than by PostgreSQL's error: %v: %s", err, c.stmt)
			}
			continue
		}
		if srcErr != nil {
			t.Errorf("source: %v: %s", srcErr, c.stmt)
			continue
		}
		cpRows, _, err := fwd.Query(ctx, c.stmt, 5000)
		switch {
		case c.refused != "":
			if err == nil || !strings.Contains(err.Error(), c.refused) {
				t.Errorf("want the copy's refusal %q, got %d rows, err %v: %s", c.refused, len(cpRows.Rows), err, c.stmt)
			}
		case err != nil:
			t.Errorf("the copy refused: %v: %s", err, c.stmt)
		default:
			if why := closeRows(srcRows, cpRows); why != "" {
				t.Errorf("answered otherwise than PostgreSQL: %s\n    %s", why, c.stmt)
				continue
			}
			if v, _, _ := sqlcompare.Compare(srcRows, cpRows, true); v != sqlcompare.Equal {
				differ++
			}
			t.Logf("close (%d rows): %s", len(cpRows.Rows), c.stmt)
		}
	}
	// The cases are here because their text can differ: if none does, the
	// list measures nothing this file's comparison is needed for.
	if differ == 0 {
		t.Errorf("no close case differs from PostgreSQL in its text: sameButDecimals was never needed")
	}
}
