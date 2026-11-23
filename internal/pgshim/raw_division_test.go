package pgshim

import (
	"strings"
	"testing"
	"time"
)

// A division the engine divides as written must have only columns of whole
// numbers or of floats, and whole numbers, in its whole left side.
func TestCopyScreen_rawDivisionLeftSide(t *testing.T) {
	types := map[string][]string{
		"id": {"BIGINT"}, "qty": {"INTEGER"}, "pack": {"SMALLINT"}, "ratio": {"DOUBLE"}, "amount": {"DECIMAL(12,2)"}, "note": {"VARCHAR"},
	}
	for _, q := range []string{
		"SELECT qty / 2 FROM t", "SELECT -qty / 2 FROM t", "SELECT o.qty / 2 FROM t o", "SELECT s.o.qty / 2 FROM s.o",
		"SELECT qty * pack / 100 FROM t", "SELECT 2 * qty / 3 FROM t", "SELECT qty * id * pack / 7 FROM t",
		"SELECT ratio / 4, ratio * qty / 4 FROM t", "SELECT id + qty / 2 FROM t",
		"SELECT amount + qty / 2 FROM t", "SELECT id FROM t WHERE qty / 2 > 1",
		"SELECT DISTINCT qty / 2 FROM t", "SELECT CASE WHEN id > 1 THEN qty / 2 END FROM t",
	} {
		if veto := CopyScreen(q, types); veto != "" {
			t.Errorf("%s: sent to the source for %s", q, veto)
		}
	}
	for _, q := range []string{
		"SELECT sum(id) * qty / 2 FROM t GROUP BY qty",
		"SELECT amount * qty / 2 FROM t",
		"SELECT o.amount * o.qty / 2 FROM t o",
		"SELECT (amount + 1) * qty / 2 FROM t",
		"SELECT (qty + 1) * qty / 2 FROM t",
		"SELECT 1.5 * qty / 2 FROM t",
		"SELECT amount % qty / 2 FROM t",
		"SELECT amount / qty * pack / 2 FROM t",
		"SELECT amount ^ qty / 2 FROM t",
		"SELECT amount::int * qty / 2 FROM t",
		"SELECT note * qty / 2 FROM t",
		"SELECT nosuch * qty / 2 FROM t",
		"SELECT abs(amount) * qty / 2 FROM t",
		"SELECT $1 * qty / 2 FROM t",
		"SELECT 7 / 2",
		// As before: % is not the copy's, and the second slash of a chain
		// has a number before it.
		"SELECT qty % 7 / 2 FROM t",
		"SELECT qty / 2 / 3 FROM t",
		"SELECT amount / 2 FROM t",
		"WITH c(qty) AS (SELECT amount FROM t) SELECT qty / 2 FROM c",
		"SELECT qty / 2 FROM (SELECT amount FROM t) AS s(qty)",
	} {
		if veto := CopyScreen(q, types); veto == "" {
			t.Errorf("%s: left to the copy", q)
		}
	}
}

// A product is read back only so far: past maxRawFactors values the statement
// goes to the source, and a very long one costs no more than its length.
func TestCopyScreen_rawDivisionLongProduct(t *testing.T) {
	types := map[string][]string{"qty": {"INTEGER"}}
	product := func(n int) string { return "SELECT " + strings.Repeat("qty * ", n-1) + "qty / 2 FROM t" }
	if veto := CopyScreen(product(maxRawFactors), types); veto != "" {
		t.Errorf("a product of %d values: sent to the source for %s", maxRawFactors, veto)
	}
	if veto := CopyScreen(product(maxRawFactors+1), types); veto != "a division" {
		t.Errorf("a product of %d values: veto %q", maxRawFactors+1, veto)
	}
	toks, why := lexSQL("SELECT " + strings.Repeat("qty / 2 * ", 1<<20/10) + "qty FROM t")
	if why != "" {
		t.Fatal(why)
	}
	known := func(string) bool { return true }
	start := time.Now()
	for i := range toks {
		if isOpTok(toks, i, "/") {
			rawDivisionLeftOK(toks, i, known)
		}
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("reading the left side of every division of 1 MB took %v", took)
	}
}
