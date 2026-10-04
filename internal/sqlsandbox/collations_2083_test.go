package sqlsandbox

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// collationPairs is where text equality on the copy stands against MySQL's
// default collation (#2083). mysql is what MySQL 8.4.9 answers for
// `a = b` under utf8mb4_0900_ai_ci, measured on a live server; onCopy is what
// the copy's session default (nocase.noaccent) answers; icu is what DuckDB's
// nocase.icu_noaccent answers, the one collation in the pinned engine that
// equates 'ß' with 'ss' and a full-width letter with its ASCII form.
//
// The copy's default disagrees with MySQL on 26 of these 57 pairs, and the
// ICU one on 1. It is not the default because it costs about twice as much on
// every comparison of text (measured on 3 million rows, 2 threads: GROUP BY a
// text column 359 ms against 653 ms, a full sort 919 ms against 2598 ms), and
// which of the two the copy should pay for is a product decision. The
// documentation lists the difference; this table is what keeps that list true
// across an engine bump.
var collationPairs = []struct {
	a, b             string
	mysql, onCopy, icu bool
}{
	{"Paid", "paid", true, true, true},
	{"café", "cafe", true, true, true},
	{"ß", "ss", true, false, true},
	{"ẞ", "SS", true, false, true},
	{"Ａ", "A", true, false, true},
	{"ａ", "A", true, false, true},
	{"a ", "a", false, false, false},
	{" a", "a", false, false, false},
	{"æ", "ae", true, false, true},
	{"Æ", "ae", true, false, true},
	{"œ", "oe", true, false, true},
	{"ø", "o", true, false, true},
	{"đ", "d", true, false, true},
	{"ł", "l", true, false, true},
	{"ı", "i", false, false, false},
	{"İ", "i", true, true, true},
	{"あ", "ア", true, false, true},
	{"ｱ", "ア", true, false, true},
	{"が", "か", true, true, true},
	{"ñ", "n", true, true, true},
	{"ü", "u", true, true, true},
	{"ü", "ue", false, false, false},
	{"a\u200d", "a", true, false, true},
	{"a\u0301", "á", true, true, true},
	{"ǆ", "dž", true, false, true},
	{"ﬁ", "fi", true, false, true},
	{"²", "2", true, false, true},
	{"①", "1", true, false, true},
	{"Ⅳ", "IV", true, false, true},
	{"σ", "ς", true, false, true},
	{"Σ", "σ", true, true, true},
	{"я", "Я", true, true, true},
	{"ё", "е", true, true, true},
	{"й", "и", false, true, false},
	{"a-b", "ab", false, false, false},
	{"a_b", "ab", false, false, false},
	{"a.b", "ab", false, false, false},
	{"", " ", false, false, false},
	{"a\t", "a", false, false, false},
	{"e", "é", true, true, true},
	{"E", "é", true, true, true},
	{"ö", "o", true, true, true},
	{"å", "a", true, true, true},
	{"ç", "c", true, true, true},
	{"þ", "th", false, false, false},
	{"ð", "d", true, false, true},
	{"ĳ", "ij", true, false, true},
	{"ŉ", "n", false, false, false},
	{"µ", "μ", true, false, true},
	{"K", "K", true, true, true},
	{"Å", "Å", true, true, true},
	{"𝐀", "A", true, false, false},
	{"😀", "😁", false, false, false},
	{"１２", "12", true, false, true},
	{"ｶﾞ", "ガ", true, false, true},
	{"ー", "-", false, false, false},
	{"々", "〃", false, false, false},
}

func TestDefaultCollation_whereItDiffersFromMySQL(t *testing.T) {
	// The default under test is the one the lock-down sets.
	if script := strings.Join(lockdownStatements([]string{"/copy"}), "\n"); !strings.Contains(script, "SET default_collation = 'nocase.noaccent'") {
		t.Fatalf("the copy's default collation changed; re-measure collationPairs against it and update docs/time-travel-sql.md:\n%s", script)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, c := range []struct {
		collation string
		want      func(i int) bool
	}{
		{"nocase.noaccent", func(i int) bool { return collationPairs[i].onCopy }},
		{"nocase.icu_noaccent", func(i int) bool { return collationPairs[i].icu }},
	} {
		if _, err := db.Exec("SET default_collation = '" + c.collation + "'"); err != nil {
			t.Fatalf("SET default_collation = %s: %v", c.collation, err)
		}
		differs := 0
		for i, p := range collationPairs {
			var got bool
			q := "SELECT a = b FROM (SELECT '" + p.a + "'::VARCHAR AS a, '" + p.b + "'::VARCHAR AS b)"
			if err := db.QueryRow(q).Scan(&got); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			if got != c.want(i) {
				t.Errorf("%s: %q = %q is %v, recorded %v", c.collation, p.a, p.b, got, c.want(i))
			}
			if got != p.mysql {
				differs++
			}
		}
		t.Logf("%s differs from MySQL's utf8mb4_0900_ai_ci on %d of %d pairs", c.collation, differs, len(collationPairs))
	}
	// The two the issue names, said outright.
	for _, p := range collationPairs[2:5] {
		if !p.mysql || p.onCopy || !p.icu {
			t.Errorf("%q = %q: mysql %v, copy %v, icu %v; the documentation says MySQL equates them, the copy does not, and nocase.icu_noaccent would", p.a, p.b, p.mysql, p.onCopy, p.icu)
		}
	}
}
