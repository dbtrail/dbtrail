package sqlsandbox

import (
	"database/sql"
	"slices"
	"strconv"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// collationPairs is where text equality on the copy stands against MySQL's
// default collation (#2083). mysql is what MySQL 8.4.9 answers for
// `a = b` under utf8mb4_0900_ai_ci, measured on a live server; nocase is
// what the copy's session default (DuckDB's built-in nocase) answers; icu is
// what nocase.icu_noaccent answered, the copy's default from #2083 to #2532;
// plain is what the built-in nocase.noaccent answers, the default before
// #2083. The two former defaults stay as references: they are what a change
// of the default is weighed against.
//
// The copy's default folds case and nothing else: it disagrees with MySQL on
// 36 of these 57 pairs, every one of them a pair MySQL holds equal and the
// copy does not (accents, 'ß' and 'ss', full-width and ligature forms). An
// equality on them matches fewer rows on the copy; a <>, NOT IN, anti-join,
// DISTINCT or GROUP BY returns more. nocase.icu_noaccent
// disagreed on 1, nocase.noaccent on 26. The documentation says so; this
// table is what keeps it true across an engine bump.
var collationPairs = []struct {
	a, b                      string
	mysql, plain, icu, nocase bool
}{
	{"Paid", "paid", true, true, true, true},
	{"café", "cafe", true, true, true, false},
	{"ß", "ss", true, false, true, false},
	{"ẞ", "SS", true, false, true, false},
	{"Ａ", "A", true, false, true, false},
	{"ａ", "A", true, false, true, false},
	{"a ", "a", false, false, false, false},
	{" a", "a", false, false, false, false},
	{"æ", "ae", true, false, true, false},
	{"Æ", "ae", true, false, true, false},
	{"œ", "oe", true, false, true, false},
	{"ø", "o", true, false, true, false},
	{"đ", "d", true, false, true, false},
	{"ł", "l", true, false, true, false},
	{"ı", "i", false, false, false, false},
	{"İ", "i", true, true, true, true},
	{"あ", "ア", true, false, true, false},
	{"ｱ", "ア", true, false, true, false},
	{"が", "か", true, true, true, false},
	{"ñ", "n", true, true, true, false},
	{"ü", "u", true, true, true, false},
	{"ü", "ue", false, false, false, false},
	{"a\u200d", "a", true, false, true, false},
	{"a\u0301", "á", true, true, true, false},
	{"ǆ", "dž", true, false, true, false},
	{"ﬁ", "fi", true, false, true, false},
	{"²", "2", true, false, true, false},
	{"①", "1", true, false, true, false},
	{"Ⅳ", "IV", true, false, true, false},
	{"σ", "ς", true, false, true, false},
	{"Σ", "σ", true, true, true, true},
	{"я", "Я", true, true, true, true},
	{"ё", "е", true, true, true, false},
	{"й", "и", false, true, false, false},
	{"a-b", "ab", false, false, false, false},
	{"a_b", "ab", false, false, false, false},
	{"a.b", "ab", false, false, false, false},
	{"", " ", false, false, false, false},
	{"a\t", "a", false, false, false, false},
	{"e", "é", true, true, true, false},
	{"E", "é", true, true, true, false},
	{"ö", "o", true, true, true, false},
	{"å", "a", true, true, true, false},
	{"ç", "c", true, true, true, false},
	{"þ", "th", false, false, false, false},
	{"ð", "d", true, false, true, false},
	{"ĳ", "ij", true, false, true, false},
	{"ŉ", "n", false, false, false, false},
	{"µ", "μ", true, false, true, false},
	{"K", "K", true, true, true, true},
	{"Å", "Å", true, true, true, true},
	{"𝐀", "A", true, false, false, false},
	{"😀", "😁", false, false, false, false},
	{"１２", "12", true, false, true, false},
	{"ｶﾞ", "ガ", true, false, true, false},
	{"ー", "-", false, false, false, false},
	{"々", "〃", false, false, false, false},
}

// mariadbPadPairs are the pairs of collationPairs that MariaDB 11.4 answers
// differently from MySQL 8.4, each under its default collation
// (utf8mb4_uca1400_ai_ci against utf8mb4_0900_ai_ci), measured on live
// servers: the two that differ by a trailing space, which MariaDB's default
// ignores (PAD SPACE) and MySQL's does not. On the other 55 the two agree, so
// what this table says about MySQL holds for a MariaDB source too, except
// that no DuckDB collation pads: against MariaDB the copy's default (nocase)
// differs on 38 pairs (these two and the 36 it differs from MySQL on).
var mariadbPadPairs = []int{6, 37}

func TestCollationPairs_mariadbDiffersOnlyOnTrailingSpaces(t *testing.T) {
	for _, i := range mariadbPadPairs {
		p := collationPairs[i]
		if strings.TrimRight(p.a, " ") != strings.TrimRight(p.b, " ") || p.mysql {
			t.Errorf("pair %d (%q, %q) is recorded as a trailing-space pair MySQL keeps apart; it is not", i, p.a, p.b)
		}
	}
	differs := 0
	for i, p := range collationPairs {
		mariadb := p.mysql || slices.Contains(mariadbPadPairs, i)
		if p.nocase != mariadb {
			differs++
		}
	}
	if differs != 38 {
		t.Errorf("against MariaDB the copy's default differs on %d pairs, documented 38 in docs/time-travel-sql.md", differs)
	}
}

func TestDefaultCollation_whereItDiffersFromMySQL(t *testing.T) {
	// The default under test is the one the lock-down sets.
	if script := strings.Join(lockdownStatements([]string{"/copy"}, spillSpec{}), "\n"); !strings.Contains(script, "SET default_collation = 'nocase'") {
		t.Fatalf("the copy's default collation changed; re-measure collationPairs against it and update docs/time-travel-sql.md:\n%s", script)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, c := range []struct {
		collation   string
		want        func(i int) bool
		wantDiffers int
	}{
		{"nocase", func(i int) bool { return collationPairs[i].nocase }, 36},
		{"nocase.icu_noaccent", func(i int) bool { return collationPairs[i].icu }, 1},
		{"nocase.noaccent", func(i int) bool { return collationPairs[i].plain }, 26},
	} {
		if _, err := db.Exec("SET default_collation = '" + c.collation + "'"); err != nil {
			t.Fatalf("SET default_collation = %s: %v", c.collation, err)
		}
		var differs []string
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
				differs = append(differs, p.a+" = "+p.b)
			}
		}
		if len(differs) != c.wantDiffers {
			t.Errorf("%s differs from MySQL's utf8mb4_0900_ai_ci on %d of %d pairs, recorded %d: %q", c.collation, len(differs), len(collationPairs), c.wantDiffers, differs)
		}
	}
	// Every difference of the copy's default is of one kind: a pair MySQL
	// holds equal and the copy does not. A pair of the other kind (the copy
	// equal, MySQL apart) would make an EQUALITY return rows MySQL leaves
	// out, with no error: that would need its own line in the
	// documentation before this default could stay.
	for _, p := range collationPairs {
		if p.nocase && !p.mysql {
			t.Errorf("%q = %q is true on the copy and false on MySQL: the copy returns MORE rows for it", p.a, p.b)
		}
	}
	// Case folds, accents do not: the one-line summary the documentation gives.
	if p := collationPairs[0]; !p.nocase || !p.mysql {
		t.Errorf("%q = %q: the copy %v, MySQL %v; both must fold case", p.a, p.b, p.nocase, p.mysql)
	}
	if p := collationPairs[1]; p.nocase || !p.mysql {
		t.Errorf("%q = %q: the copy %v, MySQL %v; the copy keeps accents apart, MySQL does not", p.a, p.b, p.nocase, p.mysql)
	}
}

// collationSortList and mysqlSortOrder: 48 strings and the order MySQL 8.4.9
// returns them in (ORDER BY s, i over a utf8mb4_0900_ai_ci column, i being
// the position in this list), measured on a live server. Letters, case,
// accents, punctuation, digits, a space, the empty string, kana, Cyrillic,
// 'ß', 'æ', 'ø' and a full-width letter.
var collationSortList = []string{"a", "A", "b", "B", "Z", "z", "_", "-", " ", "1", "10", "2", "9", ":", "@", "[", "é", "e", "f", "ñ", "n", "o", "ö", "ø", "p", "ss", "ß", "st", "æ", "ad", "af", "あ", "ア", "か", "Ａ", "~", "!", "a b", "ab", "a-b", "Ab", "aB", "", "я", "Я", "ё", "е", "ж"}

var mysqlSortOrder = []int{42, 8, 6, 7, 13, 36, 15, 14, 35, 9, 10, 11, 12, 0, 1, 34, 37, 39, 38, 40, 41, 29, 28, 30, 2, 3, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 4, 5, 45, 46, 47, 43, 44, 31, 32, 33}

// nocaseSortOrder is the order the copy's default (nocase) returns those 48
// strings in: punctuation by code point, accented letters, 'ß', 'æ', 'ø'
// and the full-width letter after 'z'. A documented difference from
// MySQL's; nocase.icu_noaccent, the default before #2532, sorted them as
// MySQL does and stays here as the reference that the list and MySQL's
// order are still right.
var nocaseSortOrder = []int{42, 8, 36, 7, 9, 10, 11, 12, 13, 14, 15, 6, 0, 1, 37, 39, 38, 40, 41, 29, 30, 2, 3, 17, 18, 20, 21, 24, 25, 27, 4, 5, 35, 26, 28, 16, 19, 22, 23, 46, 47, 43, 44, 45, 31, 33, 32, 34}

func TestDefaultCollation_orderAgainstMySQL(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	values := make([]string, len(collationSortList))
	for i, s := range collationSortList {
		values[i] = "(" + strconv.Itoa(i) + ", '" + s + "')"
	}
	query := "SELECT i FROM (VALUES " + strings.Join(values, ", ") + ") t(i, s) ORDER BY s, i"
	order := func(collation string) []int {
		if _, err := db.Exec("SET default_collation = '" + collation + "'"); err != nil {
			t.Fatal(err)
		}
		rows, err := db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []int
		for rows.Next() {
			var i int
			if err := rows.Scan(&i); err != nil {
				t.Fatal(err)
			}
			out = append(out, i)
		}
		return out
	}
	if got := order("nocase.icu_noaccent"); !slices.Equal(got, mysqlSortOrder) {
		t.Errorf("nocase.icu_noaccent no longer sorts like MySQL, so the reference list is out of date:\n got  %v\n want %v", got, mysqlSortOrder)
	}
	if got := order("nocase"); !slices.Equal(got, nocaseSortOrder) {
		t.Errorf("the copy's default (nocase) sorts differently than recorded; update docs/time-travel-sql.md:\n got  %v\n want %v", got, nocaseSortOrder)
	}
	if slices.Equal(nocaseSortOrder, mysqlSortOrder) {
		t.Error("the copy's default now sorts like MySQL; the difference in docs/time-travel-sql.md is out of date")
	}
}

// copyEquatesMoreThanMySQL is the dangerous direction: texts the copy's
// collation holds equal and MySQL's utf8mb4_0900_ai_ci does not, so a filter,
// a join or a GROUP BY on them returns MORE rows on the copy (or fewer
// groups) with no error. One pair per family; the families and their sizes
// are in docs/time-travel-sql.md. Each pair was measured on MySQL 8.4.9
// (false on every one) and on MariaDB 11.4 under utf8mb4_uca1400_ai_ci (true
// on every one). It was measured for nocase.icu_noaccent, the copy's
// default before #2532, which held every pair equal; plain is what DuckDB's
// built-in nocase.noaccent answers, nocase what the copy's default answers
// now: of these families only the newer-than-Unicode-9 letter survives it.
var copyEquatesMoreThanMySQL = []struct {
	family, a, b  string
	plain, nocase bool
}{
	{"l followed by a middle dot", "l·l", "ll", false, false},
	{"Thai consonant and leading vowel in either order", "กเ", "เก", false, false},
	{"Lao consonant and leading vowel in either order", "ກເ", "ເກ", false, false},
	{"Cyrillic i with a combining breve against short i", "й", "й", true, false},
	{"Arabic alef with a combining madda against alef with madda", "آ", "آ", true, false},
	{"Javanese tarung against its long form", "ꦴ", "ꦵ", true, false},
	{"combining mark newer than Unicode 9 (U+1DF8)", "a᷸", "a", true, false},
	{"letter newer than Unicode 9 (Georgian Mtavruli against Mkhedruli)", "Რ", "რ", true, true},
}

// mysqlEquatesMoreThanCopy is the other kind, beyond the 57 pairs above (an
// equality on them matches fewer rows on the copy, a <> or NOT IN more):
// texts MySQL 8.4.9 holds equal (measured: true on every one, on MariaDB 11.4
// as well) and the copy does not, under either collation. The copy returns
// FEWER rows for them, never more.
var mysqlEquatesMoreThanCopy = [][2]string{
	{"ぁ", "あ"},  // small kana against the normal one
	{"ッ", "ツ"},  // the same in katakana
	{"🅰", "a"},  // an enclosed letter
	{"𝒜", "A"},  // a mathematical letter
	{"℃", "°C"}, // a symbol against its spelling
	{"№", "No"},
	{"・", "･"}, // the katakana middle dot, full width against half width
}

// The pairs in the two lists above stay as recorded. A change here means
// DuckDB's collation moved: re-measure against MySQL and update the list of
// differences in docs/time-travel-sql.md.
func TestDefaultCollation_pairsBeyondTheMeasuredList(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	equal := func(collation, a, b string) bool {
		t.Helper()
		if _, err := db.Exec("SET default_collation = '" + collation + "'"); err != nil {
			t.Fatal(err)
		}
		var got bool
		q := "SELECT a = b FROM (SELECT '" + a + "'::VARCHAR AS a, '" + b + "'::VARCHAR AS b)"
		if err := db.QueryRow(q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return got
	}
	for _, p := range copyEquatesMoreThanMySQL {
		if got := equal("nocase", p.a, p.b); got != p.nocase {
			t.Errorf("%s: %q = %q on the copy (nocase) is %v, recorded %v: update the list in docs/time-travel-sql.md", p.family, p.a, p.b, got, p.nocase)
		}
		if !equal("nocase.icu_noaccent", p.a, p.b) {
			t.Errorf("%s: %q = %q under nocase.icu_noaccent is false now; the reference changed", p.family, p.a, p.b)
		}
		if got := equal("nocase.noaccent", p.a, p.b); got != p.plain {
			t.Errorf("%s: %q = %q under nocase.noaccent is %v, recorded %v", p.family, p.a, p.b, got, p.plain)
		}
	}
	for _, p := range mysqlEquatesMoreThanCopy {
		for _, collation := range []string{"nocase", "nocase.icu_noaccent", "nocase.noaccent"} {
			if equal(collation, p[0], p[1]) {
				t.Errorf("%q = %q is true under %s now, as on MySQL: drop it from the documentation", p[0], p[1], collation)
			}
		}
	}
}

// Order outside the 48 strings above is not the same everywhere. A Han
// character beyond the main block (U+20000) sorts after one inside it
// (U+4E2D) on MySQL 8.4.9 and on MariaDB 11.4. The copy's default (nocase)
// agrees; nocase.icu_noaccent, the default before #2532, sorted it before,
// so max() over the three values below was U+4E2D there.
func TestDefaultCollation_hanOutsideTheMainBlockSortsDifferently(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	const mysqlMax = "\U00020000"
	for collation, want := range map[string]string{"nocase": mysqlMax, "nocase.icu_noaccent": "中", "nocase.noaccent": mysqlMax} {
		if _, err := db.Exec("SET default_collation = '" + collation + "'"); err != nil {
			t.Fatal(err)
		}
		var got string
		if err := db.QueryRow("SELECT max(x) FROM (VALUES ('" + mysqlMax + "'), ('中'), ('z')) t(x)").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s: max is %q (%+q), recorded %q; MySQL answers %+q", collation, got, got, want, mysqlMax)
		}
	}
}

// decomposedEqualOnMySQL: a letter written as a base letter and a combining
// mark against the same letter in one character. MySQL 8.4.9 holds each
// pair equal (measured, with MariaDB 11.4 agreeing except on the Hangul
// syllable). The copy's default (nocase) does not normalize, so text that
// arrives decomposed does not match its composed form: documented in
// docs/time-travel-sql.md.
var decomposedEqualOnMySQL = [][2]string{
	{"e\u0301", "\u00e9"},      // e + combining acute against é
	{"n\u0303", "\u00f1"},      // n + combining tilde against ñ
	{"A\u030a", "\u00c5"},      // A + combining ring against Å
	{"\u1100\u1161", "\uac00"}, // two Hangul jamo against the syllable
}

func TestDefaultCollation_decomposedTextStaysApart(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("SET default_collation = 'nocase'"); err != nil {
		t.Fatal(err)
	}
	for _, p := range decomposedEqualOnMySQL {
		var got bool
		if err := db.QueryRow("SELECT '" + p[0] + "'::VARCHAR = '" + p[1] + "'::VARCHAR").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got {
			t.Errorf("%+q = %+q is true under nocase now, as on MySQL: drop the line from docs/time-travel-sql.md", p[0], p[1])
		}
	}
}
