package pgshim

import (
	"strings"
	"testing"
)

func TestCopyScreen_modAndAscii(t *testing.T) {
	types := map[string][]string{
		"id": {"BIGINT"}, "qty": {"INTEGER"}, "small": {"SMALLINT"}, "amount": {"DECIMAL(12,2)"}, "ratio": {"DOUBLE"}, "name": {"VARCHAR"},
	}
	for _, q := range []string{
		"SELECT mod(qty, 7), mod(id, 10000), mod(-qty, 3), mod(qty, -3), mod(amount, 3) FROM t",
		"SELECT mod(qty * id, 10000), mod((qty * qty), 10000), mod (qty + 1, 2) FROM t",
		"SELECT o.qty FROM t o, u WHERE mod(o.id * o.qty, 10000) = u.qty",
		"SELECT mod(qty, 3), count(*) FROM t GROUP BY mod(qty, 3)",
		"SELECT ascii('A'), ascii(name), ascii(t.name), ascii(substr(name, 1, 1)) FROM t",
		"SELECT ascii(substring(name from 1 for 1)) FROM t",
	} {
		if veto := CopyScreen(q, types); veto != "" {
			t.Errorf("%s: sent to the source for %s", q, veto)
		}
	}
	for q, says := range map[string]string{
		"SELECT mod(qty, 0) FROM t":                                      "mod()",
		"SELECT mod(qty, 00) FROM t":                                     "mod()",
		"SELECT mod(qty, -0) FROM t":                                     "mod()",
		"SELECT mod(qty, -1) FROM t":                                     "mod()",
		"SELECT mod(qty, id) FROM t":                                     "mod()",
		"SELECT mod(amount, 2.5) FROM t":                                 "mod()",
		"SELECT mod(qty, $1) FROM t":                                     "mod()",
		"SELECT mod(qty, 1234567890) FROM t":                             "mod()",
		"SELECT mod(ratio, 2) FROM t":                                    "mod()",
		"SELECT mod(name, 2) FROM t":                                     "mod()",
		"SELECT mod(small, 2) FROM t":                                    "mod()",
		"SELECT mod(length(name), 2) FROM t":                             "mod()",
		"SELECT mod(sum(qty), 10) FROM t":                                "mod()",
		"SELECT mod(mod(qty, 3), 2) FROM t":                              "mod()",
		"SELECT mod(amount * amount, 7) FROM t":                          "mod()",
		"SELECT mod(0.1234567890123456 * 0.1234567890123456, 7)":         "mod()",
		"SELECT mod(1234567890, 7)":                                      "mod()",
		"SELECT mod(qty) FROM t":                                         "mod()",
		"SELECT mod(qty, 2, 3) FROM t":                                   "mod()",
		"SELECT mod(nosuch, 2) FROM t":                                   "mod()",
		"WITH c(qty) AS (SELECT ratio FROM t) SELECT mod(qty, 2) FROM c": "",
		"SELECT mod(qty, 2) FROM (SELECT ratio FROM t) AS s(qty)":        "mod()",
		"SELECT ascii(qty) FROM t":                                       "ascii()",
		"SELECT ascii(5)":                                                "ascii()",
		"SELECT ascii(nosuch) FROM t":                                    "ascii()",
		"SELECT ascii(name || 'x') FROM t":                               "ascii()",
		"SELECT ascii(substr(qty, 1, 1)) FROM t":                         "ascii()",
		"SELECT ascii() FROM t":                                          "ascii()",
		"WITH c(name) AS (SELECT qty FROM t) SELECT ascii(name) FROM c":  "",
		"SELECT mod(qty, 3) / mod(qty, 2) FROM t":                        "",
	} {
		if veto := CopyScreen(q, types); veto == "" || !strings.Contains(veto, says) {
			t.Errorf("%s: veto %q, want it sent to the source (%q)", q, veto, says)
		}
	}
}

// A column that is text only on the copy, and a cut of a char(n), keep ascii
// from the copy.
func TestCopyScreen_asciiOfWhatIsNotPlainText(t *testing.T) {
	tables := []CatalogTable{{Schema: "public", Name: "t", Columns: []CatalogColumn{
		{Name: "id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1},
		{Name: "word", Type: "VARCHAR", SourceType: oidText, SourceTypeMod: -1},
		{Name: "prio", Type: "VARCHAR", SourceType: 90001, SourceTypeMod: -1},
		{Name: "doc", Type: "VARCHAR", SourceType: 114, SourceTypeMod: -1},
		{Name: "code", Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: 9},
	}}}
	screen := func(q string) string {
		return CopyScreen(q, withColumnOrder(screenTypes(tables, tablesRead(RewriteForCopy(q), tables)), tables))
	}
	for _, q := range []string{
		"SELECT ascii(prio) FROM t",
		"SELECT ascii(doc) FROM t",
		"SELECT ascii(substr(prio, 1, 1)) FROM t",
		"SELECT ascii(substr(code, 2, 1)) FROM t",
		"SELECT ascii(substring(code from 1 for 1)) FROM t",
		"SELECT ascii(x) FROM (SELECT prio AS x FROM t) s",
	} {
		if veto := screen(q); veto == "" {
			t.Errorf("%s: left to the copy", q)
		}
	}
}

// Before the columns' types are known, a call inside mod is already refused.
func TestScreenUntyped_modOfACall(t *testing.T) {
	if screenUntyped(mustLex(t, "SELECT mod(length(name), 2) FROM t")) == "" {
		t.Error("a call inside mod was left to the copy")
	}
	if why := screenUntyped(mustLex(t, "SELECT mod(qty * id, 10000) FROM t")); why != "" {
		t.Errorf("columns inside mod were sent to the source: %s", why)
	}
}

func TestExactClass_modAndAscii(t *testing.T) {
	of := func(name string) []string { return map[string][]string{"qty": {"INTEGER"}}[name] }
	for expr, want := range map[string]exactCls{"mod(qty, 3)": clsUnknown, pgAscii + "('a')": clsWhole32} {
		toks, _ := lexSQL(expr)
		if got := exactClass(toks, of); got != want {
			t.Errorf("exactClass(%q) = %d, want %d", expr, got, want)
		}
	}
}
