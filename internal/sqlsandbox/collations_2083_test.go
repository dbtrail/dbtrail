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
// `a = b` under utf8mb4_0900_ai_ci, measured on a live server; icu is what
// the copy's session default (nocase.icu_noaccent) answers; plain is what
// DuckDB's built-in nocase.noaccent answers, which was the copy's default
// before and is what a DuckDB without ICU can offer.
//
// The copy's default disagrees with MySQL on 1 of these 57 pairs (a
// mathematical bold letter against its plain one); nocase.noaccent on 26. The
// documentation says so; this table is what keeps it true across an engine
// bump, and what a change of the default has to be measured against.
var collationPairs = []struct {
	a, b              string
	mysql, plain, icu bool
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

// mariadbPadPairs are the pairs of collationPairs that MariaDB 11.4 answers
// differently from MySQL 8.4, each under its default collation
// (utf8mb4_uca1400_ai_ci against utf8mb4_0900_ai_ci), measured on live
// servers: the two that differ by a trailing space, which MariaDB's default
// ignores (PAD SPACE) and MySQL's does not. On the other 55 the two agree, so
// what this table says about MySQL holds for a MariaDB source too, except
// that neither DuckDB collation pads: against MariaDB the copy's default
// differs on 3 pairs (these two and the one it differs from MySQL on).
var mariadbPadPairs = []int{6, 37}

func TestCollationPairs_mariadbDiffersOnlyOnTrailingSpaces(t *testing.T) {
	for _, i := range mariadbPadPairs {
		p := collationPairs[i]
		if strings.TrimRight(p.a, " ") != strings.TrimRight(p.b, " ") || p.mysql {
			t.Errorf("pair %d (%q, %q) is recorded as a trailing-space pair MySQL keeps apart; it is not", i, p.a, p.b)
		}
	}
}

func TestDefaultCollation_whereItDiffersFromMySQL(t *testing.T) {
	// The default under test is the one the lock-down sets.
	if script := strings.Join(lockdownStatements([]string{"/copy"}, spillSpec{}), "\n"); !strings.Contains(script, "SET default_collation = 'nocase.icu_noaccent'") {
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
	// The two the issue names, said outright.
	for _, p := range collationPairs[2:5] {
		if !p.mysql || p.plain || !p.icu {
			t.Errorf("%q = %q: mysql %v, nocase.noaccent %v, the copy %v; MySQL equates them, and so must the copy", p.a, p.b, p.mysql, p.plain, p.icu)
		}
	}
}

// collationSortList and mysqlSortOrder: 48 strings and the order MySQL 8.4.9
// returns them in (ORDER BY s, i over a utf8mb4_0900_ai_ci column, i being
// the position in this list), measured on a live server. Letters, case,
// accents, punctuation, digits, a space, the empty string, kana, Cyrillic,
// 'ß', 'æ', 'ø' and a full-width letter.
var collationSortList = []string{"a", "A", "b", "B", "Z", "z", "_", "-", " ", "1", "10", "2", "9", ":", "@", "[", "é", "e", "f", "ñ", "n", "o", "ö", "ø", "p", "ss", "ß", "st", "æ", "ad", "af", "あ", "ア", "か", "Ａ", "~", "!", "a b", "ab", "a-b", "Ab", "aB", "", "я", "Я", "ё", "е", "ж"}

var mysqlSortOrder = []int{42, 8, 6, 7, 13, 36, 15, 14, 35, 9, 10, 11, 12, 0, 1, 34, 37, 39, 38, 40, 41, 29, 28, 30, 2, 3, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 4, 5, 45, 46, 47, 43, 44, 31, 32, 33}

// ORDER BY on the copy returns those 48 strings in MySQL's order, position
// for position. The built-in nocase.noaccent does not: it sorts punctuation
// by ASCII code and puts 'ß', 'æ', 'ø' and the full-width letter after 'z'.
func TestDefaultCollation_ordersLikeMySQL(t *testing.T) {
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
		t.Errorf("the copy's order differs from MySQL's:\n got  %v\n want %v", got, mysqlSortOrder)
	}
	if got := order("nocase.noaccent"); slices.Equal(got, mysqlSortOrder) {
		t.Error("nocase.noaccent now sorts like MySQL too; the comparison in the documentation is out of date")
	}
}

// copyEquatesMoreThanMySQL is the dangerous direction: texts the copy's
// collation holds equal and MySQL's utf8mb4_0900_ai_ci does not, so a filter,
// a join or a GROUP BY on them returns MORE rows on the copy (or fewer
// groups) with no error. One pair per family; the families and their sizes
// are in docs/time-travel-sql.md. Each pair was measured on MySQL 8.4.9
// (false on every one) and on MariaDB 11.4 under utf8mb4_uca1400_ai_ci (true
// on every one: a MariaDB source agrees with the copy here). plain is what
// DuckDB's built-in nocase.noaccent answers, the copy's default before
// nocase.icu_noaccent.
var copyEquatesMoreThanMySQL = []struct {
	family, a, b string
	plain        bool
}{
	{"l followed by a middle dot", "l·l", "ll", false},
	{"Thai consonant and leading vowel in either order", "กเ", "เก", false},
	{"Lao consonant and leading vowel in either order", "ກເ", "ເກ", false},
	{"Cyrillic i with a combining breve against short i", "й", "й", true},
	{"Arabic alef with a combining madda against alef with madda", "آ", "آ", true},
	{"Javanese tarung against its long form", "ꦴ", "ꦵ", true},
	{"combining mark newer than Unicode 9 (U+1DF8)", "a᷸", "a", true},
	{"letter newer than Unicode 9 (Georgian Mtavruli against Mkhedruli)", "Რ", "რ", true},
}

// mysqlEquatesMoreThanCopy is the safe direction, beyond the 57 pairs above:
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
		if !equal("nocase.icu_noaccent", p.a, p.b) {
			t.Errorf("%s: %q = %q is false on the copy now; MySQL says false too, so this difference is gone: drop it from the documentation", p.family, p.a, p.b)
		}
		if got := equal("nocase.noaccent", p.a, p.b); got != p.plain {
			t.Errorf("%s: %q = %q under nocase.noaccent is %v, recorded %v", p.family, p.a, p.b, got, p.plain)
		}
	}
	for _, p := range mysqlEquatesMoreThanCopy {
		for _, collation := range []string{"nocase.icu_noaccent", "nocase.noaccent"} {
			if equal(collation, p[0], p[1]) {
				t.Errorf("%q = %q is true under %s now, as on MySQL: drop it from the documentation", p[0], p[1], collation)
			}
		}
	}
}

// Order outside the 48 strings above is not the same everywhere. A Han
// character beyond the main block (U+20000) sorts after one inside it
// (U+4E2D) on MySQL 8.4.9 and on MariaDB 11.4, and before it on the copy, so
// max() over the three values below is U+20000 there and U+4E2D here.
func TestDefaultCollation_hanOutsideTheMainBlockSortsDifferently(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	const mysqlMax = "\U00020000"
	for collation, want := range map[string]string{"nocase.icu_noaccent": "中", "nocase.noaccent": mysqlMax} {
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
