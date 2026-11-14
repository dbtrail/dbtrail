package readrouter

import (
	"strings"
	"testing"
)

// #2827: a statement that names a FLOAT column of the tables it reads, or has
// a star over one, is kept on MySQL. MySQL compares and computes a FLOAT as a
// double (3.14 stored as a float is not 3.14 there), the copy as a float.
func TestFloatVeto_2827(t *testing.T) {
	floats := []string{"f", "fu"}
	cases := []struct {
		stmt   string
		floats []string
		star   bool
		veto   bool
		why    string
	}{
		{"SELECT id FROM fl WHERE f = 3.14", floats, false, true, "a comparison"},
		{"SELECT id FROM fl WHERE fu IN (3.14, 0.1)", floats, false, true, "the other FLOAT column"},
		{"SELECT id FROM fl WHERE `f` = 3.14", floats, false, true, "quoted"},
		{"SELECT fl.f FROM fl", floats, false, true, "qualified by the table"},
		{"SELECT db.fl.`f` FROM db.fl", floats, false, true, "qualified by schema and table, quoted"},
		{"SELECT F FROM fl", floats, false, true, "upper case in the statement"},
		{"SELECT f FROM fl", []string{"F"}, false, true, "upper case in the table"},
		{"SELECT id FROM fl ORDER BY f", floats, false, true, "only in ORDER BY"},
		{"SELECT id FROM fl WHERE f = ?", floats, false, true, "a placeholder"},
		{"SELECT MIN(f * 2) FROM fl", floats, false, true, "arithmetic"},
		{"SELECT a.id FROM fl a JOIN fl b ON a.f = b.d", floats, false, true, "a join on it"},
		{"SELECT d AS f FROM fl", floats, false, true, "an alias of the name: costs a refusal, never a wrong answer"},
		{"SELECT * FROM fl", floats, false, true, "a star"},
		{"SELECT fl.* FROM fl", floats, false, true, "a table's star"},
		{"SELECT id FROM fl", floats, true, true, "a star the parse saw"},
		{"SELECT COUNT(*) FROM fl", floats, false, false, "COUNT(*) is not a star over columns"},
		{"SELECT id, d FROM fl WHERE d = 3.14", floats, false, false, "the table's other columns"},
		{"SELECT ff, f_x, xf, fu2 FROM fl", floats, false, false, "longer names that hold the name"},
		{"SELECT id FROM fl WHERE name = 'f'", floats, false, false, "the name inside a string"},
		{"SELECT id FROM fl WHERE f = 3.14", nil, false, false, "no FLOAT column"},
		{"SELECT año FROM t", []string{"año"}, false, true, "a name outside ASCII"},
		{"SELECT ano FROM t", []string{"año"}, false, false, "an accent is not dropped"},
		{"SELECT x FROM t", []string{"my col"}, false, true, "a name that cannot be looked for"},
	}
	for _, c := range cases {
		got := ShapeOf(c.stmt).FloatVeto(c.floats, c.star)
		if (got != "") != c.veto {
			t.Errorf("%q over %q (%s): %q, want veto=%v", c.stmt, c.floats, c.why, got, c.veto)
		}
	}
	if got := FloatVeto("", floats, false); got == "" {
		t.Error("a statement with no shape: no veto, want one (not known is never nothing to find)")
	}
	if got := FloatVeto("SELECT \xff FROM fl", floats, false); got == "" {
		t.Error("a statement that is not UTF-8: no veto, want one")
	}
	if got := FloatVeto("", nil, false); got != "" {
		t.Errorf("no FLOAT column and no shape: %q, want none", got)
	}
	// The reason names the column and says what differs.
	if got := ShapeOf("SELECT id FROM fl WHERE `fu` = 1").FloatVeto(floats, false); !strings.Contains(got, "fu") || !strings.Contains(got, "FLOAT") {
		t.Errorf("reason %q, want it to name fu and FLOAT", got)
	}
	if got := ShapeOf("SELECT * FROM fl").FloatVeto(floats, false); !strings.Contains(got, "star") || !strings.Contains(got, "f") {
		t.Errorf("reason %q, want it to name the star and the column", got)
	}
}
