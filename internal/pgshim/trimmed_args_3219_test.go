package pgshim

import (
	"context"
	"strings"
	"testing"
)

// A portal's arguments are written into its text as strings before the
// screen reads it, so 'ab' would pass where the statement compares a trimmed
// char(n) with a parameter. Its Describe, written with NULLs, was vetoed and
// announced no rows: the run must be vetoed too, whatever the value, or rows
// would follow a NoData.
func TestPaddedVeto_aParameterComparedWithATrimmedCharIsVetoedWhateverItsValue(t *testing.T) {
	s := &session{user: "app"}
	s.initSettings("public", "app")
	s.reported = map[string]string{}
	s.database = "orders"
	s.bound = &Binding{Catalog: func(context.Context) ([]CatalogTable, error) {
		return []CatalogTable{{Schema: "public", Name: "t", Columns: []CatalogColumn{
			{Name: "id", Type: "INTEGER", SourceType: oidInt4},
			{Name: "code", Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: 9, TrimmedOnCopy: true}}}}, nil
	}}
	run := func(template, filled string) string {
		t.Helper()
		s.screened, s.argsBound = template, true
		defer func() { s.screened, s.argsBound = "", false }()
		return s.paddedVeto(filled)
	}
	// The premise: the text with the value written in passes on its own.
	if why := s.paddedVeto("SELECT count(*) FROM t WHERE code =  'ab' "); why != "" {
		t.Fatalf("code = 'ab' vetoed on its own: %s", why)
	}
	for _, tc := range []struct{ template, filled string }{
		{"SELECT count(*) FROM t WHERE code = $1", "SELECT count(*) FROM t WHERE code =  'ab' "},
		{"SELECT count(*) FROM t WHERE code = $1", "SELECT count(*) FROM t WHERE code =  'ab ' "},
		{"SELECT id FROM t WHERE code IN ($1, $2)", "SELECT id FROM t WHERE code IN ( 'ab' ,  'x' )"},
		{"SELECT id FROM t WHERE code LIKE $1", "SELECT id FROM t WHERE code LIKE  'a%' "},
	} {
		if why := run(tc.template, tc.filled); !strings.Contains(why, `"code"`) {
			t.Errorf("%s with %s: veto %q, want the char(n) column's", tc.template, tc.filled, why)
		}
	}
	// A parameter that is not compared with the column leaves it as before.
	for _, tc := range []struct{ template, filled string }{
		{"SELECT code FROM t WHERE id = $1", "SELECT code FROM t WHERE id =  1 "},
		{"SELECT count(*) FROM t WHERE code = 'ab' AND id = $1", "SELECT count(*) FROM t WHERE code = 'ab' AND id =  1 "},
	} {
		if why := run(tc.template, tc.filled); why != "" {
			t.Errorf("%s: vetoed: %s", tc.template, why)
		}
	}
	// Without a template (a simple query) the text alone decides.
	if why := s.paddedVeto("SELECT count(*) FROM t WHERE code = 'ab'"); why != "" {
		t.Errorf("a simple query with code = 'ab': vetoed: %s", why)
	}
}
