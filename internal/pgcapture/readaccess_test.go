package pgcapture

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// The statements shown for a role that cannot read a published table
// (#3022): each identifier quoted by PostgreSQL's rules, USAGE before SELECT.
func TestReadAccessGrantsQuoteEveryName(t *testing.T) {
	acc := ReadAccess{Role: `Db Trail"x`, Tables: 4, Unreadable: []UnreadableTable{
		{Schema: "Sales Data", Table: `Order "Items"`, NoUsage: true, NoSelect: true},
		{Schema: "Sales Data", Table: "plain", NoUsage: true},
		{Schema: "public", Table: "customers", NoSelect: true},
	}}
	want := []string{
		`GRANT USAGE ON SCHEMA "Sales Data" TO "Db Trail""x";`,
		`GRANT SELECT ON TABLE "Sales Data"."Order ""Items""" TO "Db Trail""x";`,
		`GRANT SELECT ON TABLE "public"."customers" TO "Db Trail""x";`,
	}
	if got := acc.Grants(); !slices.Equal(got, want) {
		t.Errorf("Grants:\n got %q\nwant %q", got, want)
	}
	if acc.WholeSchemas() {
		t.Error("WholeSchemas for three tables")
	}
	wantProblem := `the role "Db Trail""x" cannot read 3 of the 4 tables the copy reads: ` +
		`"Sales Data"."Order ""Items""" (no USAGE on its schema, no SELECT), "Sales Data"."plain" (no USAGE on its schema), "public"."customers" (no SELECT).`
	if got := acc.Problem(); got != wantProblem {
		t.Errorf("Problem:\n got %s\nwant %s", got, wantProblem)
	}
}

func TestReadAccessNothingMissing(t *testing.T) {
	for _, acc := range []ReadAccess{{}, {Role: "dbtrail", Tables: 7}} {
		if p, g := acc.Problem(), acc.Grants(); p != "" || len(g) != 0 {
			t.Errorf("%+v: Problem %q, Grants %q, want nothing", acc, p, g)
		}
	}
}

// One table, one permission: the singular words.
func TestReadAccessOneTable(t *testing.T) {
	acc := ReadAccess{Role: "dbtrail", Tables: 1, Unreadable: []UnreadableTable{{Schema: "public", Table: "customers", NoSelect: true}}}
	if got, want := acc.Problem(), `the role "dbtrail" cannot read 1 of the 1 table the copy reads: "public"."customers" (no SELECT).`; got != want {
		t.Errorf("Problem:\n got %s\nwant %s", got, want)
	}
	if got, want := acc.Grants(), []string{`GRANT SELECT ON TABLE "public"."customers" TO "dbtrail";`}; !slices.Equal(got, want) {
		t.Errorf("Grants = %q, want %q", got, want)
	}
}

// Exactly the limit is still named table by table; one more names schemas.
func TestReadAccessAtTheLimit(t *testing.T) {
	mk := func(n int) ReadAccess {
		acc := ReadAccess{Role: "dbtrail", Tables: n}
		for i := range n {
			acc.Unreadable = append(acc.Unreadable, UnreadableTable{Schema: "public", Table: fmt.Sprintf("t%03d", i), NoSelect: true})
		}
		return acc
	}
	at := mk(readAccessNamed)
	if at.WholeSchemas() || len(at.Grants()) != readAccessNamed || strings.Contains(at.Problem(), "more") {
		t.Errorf("at the limit: WholeSchemas %v, %d statements, problem %s", at.WholeSchemas(), len(at.Grants()), at.Problem())
	}
	over := mk(readAccessNamed + 1)
	if !over.WholeSchemas() {
		t.Error("one past the limit: WholeSchemas is false")
	}
	if got, want := over.Grants(), []string{`GRANT SELECT ON ALL TABLES IN SCHEMA "public" TO "dbtrail";`}; !slices.Equal(got, want) {
		t.Errorf("one past the limit: Grants = %q, want %q", got, want)
	}
	if !strings.HasSuffix(over.Problem(), `"public"."t009" (no SELECT), and 1 more.`) {
		t.Errorf("one past the limit: problem %s", over.Problem())
	}
}

// Hundreds of tables: the first ones by name and the rest counted, and one
// statement per schema, not five hundred lines.
func TestReadAccessHundredsOfTables(t *testing.T) {
	acc := ReadAccess{Role: "dbtrail", Tables: 520}
	for i := range 500 {
		schema := "app"
		if i >= 300 {
			schema = "Billing 2"
		}
		acc.Unreadable = append(acc.Unreadable, UnreadableTable{Schema: schema, Table: fmt.Sprintf("t%03d", i), NoSelect: true, NoUsage: schema != "app"})
	}
	p := acc.Problem()
	if strings.Count(p, "(no ") != readAccessNamed || !strings.HasSuffix(p, ", and 490 more.") ||
		!strings.HasPrefix(p, `the role "dbtrail" cannot read 500 of the 520 tables the copy reads: "app"."t000" (no SELECT), `) {
		t.Errorf("Problem: %s", p)
	}
	if len(p) > 600 {
		t.Errorf("Problem is %d bytes long", len(p))
	}
	want := []string{
		`GRANT USAGE ON SCHEMA "Billing 2" TO "dbtrail";`,
		`GRANT SELECT ON ALL TABLES IN SCHEMA "app" TO "dbtrail";`,
		`GRANT SELECT ON ALL TABLES IN SCHEMA "Billing 2" TO "dbtrail";`,
	}
	if got := acc.Grants(); !slices.Equal(got, want) {
		t.Errorf("Grants:\n got %q\nwant %q", got, want)
	}
}

// Hundreds of schemas: the list of statements is cut too, and says so.
func TestReadAccessHundredsOfSchemas(t *testing.T) {
	acc := ReadAccess{Role: "dbtrail", Tables: 300}
	for i := range 300 {
		acc.Unreadable = append(acc.Unreadable, UnreadableTable{Schema: fmt.Sprintf("s%03d", i), Table: "t", NoUsage: true, NoSelect: true})
	}
	g := acc.Grants()
	if len(g) != 2*(readAccessNamed+1) {
		t.Fatalf("%d statements, want %d:\n%s", len(g), 2*(readAccessNamed+1), strings.Join(g, "\n"))
	}
	if g[readAccessNamed] != "-- and the same GRANT USAGE for 290 more schema(s)" || g[len(g)-1] != "-- and the same GRANT SELECT for 290 more schema(s)" {
		t.Errorf("the counted rest: %q and %q", g[readAccessNamed], g[len(g)-1])
	}
	if g[0] != `GRANT USAGE ON SCHEMA "s000" TO "dbtrail";` || g[readAccessNamed+1] != `GRANT SELECT ON ALL TABLES IN SCHEMA "s000" TO "dbtrail";` {
		t.Errorf("first statements: %q, %q", g[0], g[readAccessNamed+1])
	}
}

// A schema named in a list that is not sorted by schema is still named once.
func TestReadAccessSchemasNamedOnce(t *testing.T) {
	acc := ReadAccess{Role: "r", Tables: 3, Unreadable: []UnreadableTable{
		{Schema: "a", Table: "x", NoUsage: true}, {Schema: "b", Table: "y", NoUsage: true}, {Schema: "a", Table: "z", NoUsage: true},
	}}
	if got, want := acc.Grants(), []string{`GRANT USAGE ON SCHEMA "a" TO "r";`, `GRANT USAGE ON SCHEMA "b" TO "r";`}; !slices.Equal(got, want) {
		t.Errorf("Grants = %q, want %q", got, want)
	}
}

// A name with a line break stays on its line: the messages are read as lines.
func TestReadAccessNamesStayOnOneLine(t *testing.T) {
	acc := ReadAccess{Role: "ro\nle", Tables: 1, Unreadable: []UnreadableTable{{Schema: "s\r\n1", Table: "t\tab", NoUsage: true, NoSelect: true}}}
	for _, s := range append(acc.Grants(), acc.Problem()) {
		if strings.ContainsAny(s, "\r\n\t") {
			t.Errorf("a control character in %q", s)
		}
	}
	if got, want := acc.Grants()[1], `GRANT SELECT ON TABLE "s??1"."t?ab" TO "ro?le";`; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
