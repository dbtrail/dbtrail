package readrouter

import (
	"strings"
	"testing"
)

// #2628: on the port without routing, MySQL's DATE_FORMAT is written
// strftime for the copy, its format in the copy's specifiers, and MySQL's
// DATEDIFF(a, b) date_diff('day', CAST(b AS DATE), CAST(a AS DATE)). The copy
// has neither function. What the rewrite cannot write with certainty is left
// as written, for UnroutedFunctionVeto to refuse.
func TestForCopyMySQLDateFunctions(t *testing.T) {
	on := CopyOptions{MySQLDateFunctions: true}
	rewritten := map[string]string{
		"SELECT DATE_FORMAT(d, '%Y-%m-%d') FROM t": "SELECT strftime(d, '%Y-%m-%d') FROM t",
		"select date_format(d,'%Y') from t":        "select strftime(d, '%Y') from t",
		// Every specifier the copy writes as MySQL does, in the copy's spelling.
		"SELECT DATE_FORMAT(d, '%a %b %c %d %e %f %H %h %I %i %j %k %l %M %m %p %r %S %s %T %U %v %W %w %x %Y %y %%')": "SELECT strftime(d, '%a %b %-m %d %-d %f %H %I %I %M %j %-H %-I %B %m %p %I:%M:%S %p %S %S %H:%M:%S %U %V %A %w %G %Y %y %%')",
		"SELECT DATE_FORMAT(d, 'it''s %Y')":                                                     "SELECT strftime(d, 'it''s %Y')",
		"SELECT DATE_FORMAT(d, '100%%')":                                                        "SELECT strftime(d, '100%%')",
		"SELECT DATE_FORMAT(d, 'week')":                                                         "SELECT strftime(d, 'week')",
		"SELECT DATE_FORMAT( d , '%Y' )":                                                        "SELECT strftime( d , '%Y')",
		"SELECT DATE_FORMAT\n(d, '%Y')":                                                         "SELECT strftime(d, '%Y')",
		"SELECT DATE_FORMAT(COALESCE(a, b), '%Y')":                                              "SELECT strftime(COALESCE(a, b), '%Y')",
		"SELECT DATE_FORMAT(IF(s = ',)', a, b), '%m')":                                          "SELECT strftime(IF(s = ',)', a, b), '%m')",
		"SELECT DATE_FORMAT(`d`, '%Y') FROM `t`":                                                `SELECT strftime("d", '%Y') FROM "t"`,
		"SELECT DATE_FORMAT(`a,b)`, '%Y')":                                                      `SELECT strftime("a,b)", '%Y')`,
		"SELECT DATE_FORMAT(d /* when, ) */, '%Y')":                                             "SELECT strftime(d /* when, ) */, '%Y')",
		"SELECT DATE_FORMAT((SELECT max(d) FROM t), '%Y')":                                      "SELECT strftime((SELECT max(d) FROM t), '%Y')",
		"SELECT DATE_FORMAT(NOW(), '%Y'), DATE_FORMAT(d, '%m')":                                 "SELECT strftime(NOW(), '%Y'), strftime(d, '%m')",
		"SELECT DATE_FORMAT(d, '%Y-%m') AS m, COUNT(*) FROM t GROUP BY DATE_FORMAT(d, '%Y-%m')": "SELECT strftime(d, '%Y-%m') AS m, COUNT(*) FROM t GROUP BY strftime(d, '%Y-%m')",
		"SELECT DATE_FORMAT(d, '%Y') FROM t WHERE s = 'DATE_FORMAT(d, ''%D'')'":                 "SELECT strftime(d, '%Y') FROM t WHERE s = 'DATE_FORMAT(d, ''%D'')'",
		// A prepared statement's argument, as interpolate writes it.
		"SELECT DATE_FORMAT(d,  '%Y-%m' ) FROM t": "SELECT strftime(d, '%Y-%m') FROM t",

		"SELECT DATEDIFF(a, b) FROM t":                                      "SELECT date_diff('day', CAST(b AS DATE), CAST(a AS DATE)) FROM t",
		"SELECT datediff( a ,b ) FROM t":                                    "SELECT date_diff('day', CAST(b AS DATE), CAST(a AS DATE)) FROM t",
		"SELECT DATEDIFF('2026-01-02 00:00:01', '2026-01-01 23:59:59')":     "SELECT date_diff('day', CAST('2026-01-01 23:59:59' AS DATE), CAST('2026-01-02 00:00:01' AS DATE))",
		"SELECT DATEDIFF(NOW(), d) FROM t":                                  "SELECT date_diff('day', CAST(d AS DATE), CAST(NOW() AS DATE)) FROM t",
		"SELECT DATEDIFF(d, NULL)":                                          "SELECT date_diff('day', CAST(NULL AS DATE), CAST(d AS DATE))",
		"SELECT DATEDIFF(COALESCE(a, b), IFNULL(c, d))":                     "SELECT date_diff('day', CAST(IFNULL(c, d) AS DATE), CAST(COALESCE(a, b) AS DATE))",
		"SELECT DATEDIFF((SELECT max(d) FROM t), d)":                        "SELECT date_diff('day', CAST(d AS DATE), CAST((SELECT max(d) FROM t) AS DATE))",
		"SELECT DATEDIFF(a -- first\n, b)":                                  "SELECT date_diff('day', CAST(b AS DATE), CAST(a -- first\n AS DATE))",
		"SELECT DATEDIFF(DATE_FORMAT(d, '%Y-%m-01'), d)":                    "SELECT date_diff('day', CAST(d AS DATE), CAST(strftime(d, '%Y-%m-01') AS DATE))",
		"SELECT * FROM t WHERE DATEDIFF(`d`, '2026-01-01') > 3 LIMIT 10, 5": `SELECT * FROM t WHERE date_diff('day', CAST('2026-01-01' AS DATE), CAST("d" AS DATE)) > 3 LIMIT 10, 5`,
	}
	for stmt, want := range rewritten {
		got, why := ForCopyOpts(stmt, on)
		if why != "" || got != want {
			t.Errorf("ForCopyOpts(%q) =\n  %q, %q\nwant\n  %q", stmt, got, why, want)
		}
	}
	// Left as written: not a call it writes with certainty, or not a call.
	kept := []string{
		"SELECT DATE_FORMAT(d, '%D')",         // 29th: the copy has no way to write it
		"SELECT DATE_FORMAT(d, '%u')",         // weeks counted otherwise
		"SELECT DATE_FORMAT(d, '%V')",         // likewise
		"SELECT DATE_FORMAT(d, '%X')",         // likewise
		"SELECT DATE_FORMAT(d, '%Q')",         // Q on MySQL
		"SELECT DATE_FORMAT(d, '%-d')",        // -d on MySQL, the day on the copy
		"SELECT DATE_FORMAT(d, '%Y %')",       // a % at the end
		"SELECT DATE_FORMAT(d, '')",           // the copy refuses an empty format
		"SELECT DATE_FORMAT(d, f) FROM t",     // a format from a column
		"SELECT DATE_FORMAT(d, NULL)",         // not a string
		"SELECT DATE_FORMAT(d, '%Y' '-%m')",   // two literals, one string on MySQL
		"SELECT DATE_FORMAT(d, _utf8mb4'%Y')", // a prefix
		"SELECT DATE_FORMAT(d, N'%Y')",        // likewise
		"SELECT DATE_FORMAT(d, /* c */ '%Y')", // a comment beside the format
		"SELECT DATE_FORMAT(d, CONCAT('%', 'Y'))",
		"SELECT DATE_FORMAT(d)",                 // one argument: an error on MySQL
		"SELECT DATE_FORMAT(d, '%Y', 'x')",      // three
		"SELECT DATE_FORMAT(, '%Y')",            // an empty argument
		"SELECT DATE_FORMAT(d, '%Y'",            // never closed
		"SELECT t.date_format(d, '%Y') FROM t",  // a name next to a dot
		"SELECT date_format FROM t",             // a column
		"SELECT 'DATE_FORMAT(d, ''%Y'')'",       // inside a string
		"SELECT 1 /* DATE_FORMAT(d, '%Y') */",   // inside a comment
		"SELECT datediff('day', a, b) FROM t",   // the copy's own, with a unit first
		"SELECT DATEDIFF(a) FROM t",             // one argument
		"SELECT DATEDIFF(a, b, c) FROM t",       // three
		"SELECT DATEDIFF(a, ) FROM t",           // an empty argument
		"SELECT DATEDIFF(, b) FROM t",           // likewise
		"SELECT t.datediff(a, b) FROM t",        // a name next to a dot
		"SELECT DATE_FORMATTED(d, '%Y') FROM t", // a longer word
		"",
	}
	for _, stmt := range kept {
		want, _ := ForCopy(stmt)
		if got, _ := ForCopyOpts(stmt, on); got != want {
			t.Errorf("ForCopyOpts(%q) = %q; want it as ForCopy writes it, %q", stmt, got, want)
		}
	}
	// Off, the default (the routed ladder and sql-compare): left alone.
	for _, stmt := range []string{"SELECT DATE_FORMAT(d, '%Y') FROM t", "SELECT DATEDIFF(a, b) FROM t"} {
		if got, _ := ForCopy(stmt); got != stmt {
			t.Errorf("ForCopy rewrote %q: %q", stmt, got)
		}
	}
	// With the other rewrites in the same statement.
	got, _ := ForCopyOpts("SELECT DATE_FORMAT(d, '%b %Y') FROM t STRAIGHT_JOIN u ON u.id = t.id WHERE b LIKE 'x%' LIMIT 4, 2",
		CopyOptions{MySQLDateFunctions: true, LimitOffsetComma: true, LikeIgnoresCase: true})
	if want := `SELECT strftime(d, '%b %Y') FROM t JOIN u ON u.id = t.id WHERE b ILIKE 'x%' LIMIT 2 OFFSET 4`; got != want {
		t.Errorf("with the other rewrites: %q, want %q", got, want)
	}
}

func TestStrftimeFormat(t *testing.T) {
	for f, want := range map[string]string{
		"%Y-%m-%d %H:%i:%s":   "%Y-%m-%d %H:%M:%S",
		"%M %e, %Y":           "%B %-d, %Y",
		"%l:%i %p":            "%-I:%M %p",
		"%%Y":                 "%%Y",
		"%%%Y":                "%%%Y",
		"no specifier at all": "no specifier at all",
		"it''s":               "it''s",
		"día %d":              "día %d",
	} {
		if got, ok := strftimeFormat(f); !ok || got != want {
			t.Errorf("strftimeFormat(%q) = %q, %v; want %q", f, got, ok, want)
		}
	}
	for _, f := range []string{"", "%", "%Y%", "%D", "%u", "%V", "%X", "%q", "%E", "% Y", "%-m", "%\u00e9"} {
		if got, ok := strftimeFormat(f); ok {
			t.Errorf("strftimeFormat(%q) = %q, want it refused", f, got)
		}
	}
}

// What is still MySQL's DATE_FORMAT, or MySQL's DATEDIFF with two
// arguments, in the text sent to the copy is refused with how to write it:
// the copy fails on both with its own error.
func TestUnroutedFunctionVeto(t *testing.T) {
	refused := map[string]string{
		"SELECT DATE_FORMAT(d, '%D') FROM t":                 "strftime(d, format)",
		"SELECT date_format (d, f) FROM t":                   "strftime(d, format)",
		"SELECT DATE_FORMAT(d, '%Y') FROM t # c":             "strftime(d, format)",
		"SELECT DATE_FORMAT(d)":                              "strftime(d, format)",
		"SELECT DATE_FORMAT(d, 'C:\\') FROM t":               "strftime(d, format)",
		`SELECT DATE_FORMAT("d", '%Y') FROM "t"`:             "strftime(d, format)",
		"SELECT datediff(a, b) FROM t":                       "date_diff('day', CAST(b AS DATE), CAST(a AS DATE))",
		"SELECT DATEDIFF('2026-01-02', '2026-01-01')":        "date_diff",
		"SELECT DATEDIFF(COALESCE(a, b), c)":                 "date_diff",
		"SELECT DATEDIFF(\"a,b\", c) FROM t":                 "date_diff",
		"SELECT DATEDIFF(a, 'x, y')":                         "date_diff",
		"SELECT x FROM t WHERE DATEDIFF(a, b) > 1 -- a note": "date_diff",
		"SELECT `a-- b`, DATE_FORMAT(d, '%Y') FROM t":        "strftime(d, format)",
	}
	for stmt, want := range refused {
		r, ok := UnroutedFunctionVeto(stmt)
		if !ok || !strings.Contains(r.Message(), want) {
			t.Errorf("%q: %+v, %v; want it refused naming %q", stmt, r, ok, want)
		}
	}
	answered := []string{
		"SELECT strftime(d, '%Y') FROM t",
		"SELECT datediff('day', a, b) FROM t",
		"SELECT date_diff('day', CAST(b AS DATE), CAST(a AS DATE)) FROM t",
		"SELECT DATEDIFF(a) FROM t", // the copy refuses it by itself, as MySQL does
		"SELECT 'DATE_FORMAT(d, ''%Y'')', 'DATEDIFF(a, b)'",
		"SELECT 1 -- DATE_FORMAT(d, '%Y')",
		"SELECT 1 /* DATEDIFF(a, b) */",
		"SELECT t.date_format, t.datediff FROM t",
		"SELECT date_format, datediff FROM t",
		`SELECT "date_format"."x" FROM t`,
		"SELECT DATE_FORMATTED(d, '%Y')",
		"",
	}
	for _, stmt := range answered {
		if r, ok := UnroutedFunctionVeto(stmt); ok {
			t.Errorf("%q refused: %+v", stmt, r)
		}
	}
}

func TestCallArgs(t *testing.T) {
	for s, want := range map[string][]string{
		"(a, b) FROM t":             {"a", " b"},
		" \n(a)":                    {"a"},
		"()":                        {""},
		"(f(a, b), 'x,)', \"y,)\")": {"f(a, b)", " 'x,)'", ` "y,)"`},
		"(a /* , ) */, b)":          {"a /* , ) */", " b"},
		"(a -- , )\n, b)":           {"a -- , )\n", " b"},
		"('it''s', b)":              {"'it''s'", " b"},
		"(a --x, b)":                {"a --x", " b"}, // no space after --: not a comment on MySQL
	} {
		got, _, ok := callArgs(s)
		if !ok || strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("callArgs(%q) = %q, %v; want %q", s, got, ok, want)
		}
	}
	for _, s := range []string{"", "a, b)", "(a, b", "('a, b)", "(\"a, b)", "(a /* b)", "(a -- b)"} {
		if got, _, ok := callArgs(s); ok {
			t.Errorf("callArgs(%q) = %q, want it refused", s, got)
		}
	}
}
