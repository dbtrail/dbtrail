package console

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2918: the length functions are answered by the copy only over a text
// column whose bytes are the copy's UTF-8, which each table's snapshot says
// (views.BaselineTable.LengthSafe). The rule is asked once about every
// table's columns; a table whose types are not known makes no column safe.
func TestSQLLengthRefusalFor_2918(t *testing.T) {
	table := func(name string, known bool, safe, other []string) views.BaselineTable {
		return views.BaselineTable{Schema: "shop", Table: name, Path: "/c/shop/" + name + ".parquet", SchemaKnown: known,
			Columns: append(append([]string{}, safe...), other...), LengthSafe: safe, LengthOther: other}
	}
	in := views.Input{Baselines: []views.BaselineTable{table("a", true, []string{"u"}, []string{"l"}), table("b", true, []string{"c"}, []string{"id"})}}
	asked := &lengthAsked{}
	if got := sqlLengthRefusalFor(in, "SELECT LENGTH(u) FROM a", asked); got != "" {
		t.Errorf("a reading that vetoes nothing: %q", got)
	}
	if asked.calls != 1 || strings.Join(asked.safe, ",") != "u,c" || strings.Join(asked.other, ",") != "l,id" {
		t.Errorf("asked %d times about %q / %q, want once about u,c / l,id", asked.calls, asked.safe, asked.other)
	}
	asked.why = "x"
	if got := sqlLengthRefusalFor(in, "SELECT LENGTH(u) FROM a", asked); got != "the copy does not answer this statement: x" {
		t.Errorf("refusal %q, want the reason", got)
	}

	// A table whose column types are not known: nothing is safe.
	unknown := views.Input{Baselines: []views.BaselineTable{table("a", true, []string{"u"}, nil), table("z", false, nil, nil)}}
	asked = &lengthAsked{}
	sqlLengthRefusalFor(unknown, "SELECT LENGTH(u) FROM a", asked)
	if len(asked.safe) != 0 {
		t.Errorf("a table of unknown types beside: asked with safe %q, want none", asked.safe)
	}

	// The real rule, end to end through the shape.
	for stmt, refused := range map[string]bool{
		"SELECT LENGTH(u), CHAR_LENGTH(c) FROM a JOIN b": false,
		"SELECT LENGTH(l) FROM a":                        true,
		"SELECT LENGTH(LOWER(u)) FROM a":                 true,
		"SELECT u FROM a":                                false,
	} {
		if got := sqlLengthRefusalFor(in, stmt, readrouter.ShapeOf(stmt)); (got != "") != refused {
			t.Errorf("%s: %q, want refused %v", stmt, got, refused)
		}
	}

	// No reading of the statement: a statement with a length call is not
	// answered, one without is.
	if got := sqlLengthRefusalFor(in, "SELECT CHAR_LENGTH(u) FROM a", nil); got == "" {
		t.Error("no reading of the statement with CHAR_LENGTH: answered, want a refusal")
	}
	if got := sqlLengthRefusalFor(in, "SELECT u FROM a", nil); got != "" {
		t.Errorf("no reading of the statement, no length call: %q", got)
	}

	// No reading of the statement, and a comment between the name and the
	// parenthesis: the rewriter takes each of these for a call, so the
	// fallback must too.
	for _, stmt := range []string{
		"SELECT LENGTH/**/(l) FROM a",
		"SELECT LENGTH -- x\n(l) FROM a",
		"SELECT LENGTH#x\n(l) FROM a",
		"SELECT char_length /* a */ /* b */ (l) FROM a",
		"SELECT `LENGTH`(l) FROM a",
	} {
		if got := sqlLengthRefusalFor(in, stmt, nil); got == "" {
			t.Errorf("no reading of %q: answered, want a refusal", stmt)
		}
	}
	// A name or a call only inside a string or a comment is no call.
	for _, stmt := range []string{"SELECT 'length(l)' FROM a", "SELECT u FROM a -- length(l)\n"} {
		if got := sqlLengthRefusalFor(in, stmt, nil); got != "" {
			t.Errorf("no reading of %q: %q, want none", stmt, got)
		}
	}

	// The refusal says which call kept the statement on MySQL.
	if got := sqlLengthRefusalFor(in, "SELECT LENGTH(u), LENGTH(l) FROM a", readrouter.ShapeOf("SELECT LENGTH(u), LENGTH(l) FROM a")); !strings.Contains(got, "LENGTH(l)") || strings.Contains(got, "LENGTH(u)") {
		t.Errorf("refusal %q: want it to name LENGTH(l), not LENGTH(u)", got)
	}
}

// lengthAsked records what LengthVeto is asked; every other question vetoes
// nothing.
type lengthAsked struct {
	askedTypes
	calls       int
	safe, other []string
	why         string
}

func (a *lengthAsked) LengthVeto(safe, other []string) string {
	a.calls++
	a.safe, a.other = safe, other
	return a.why
}
