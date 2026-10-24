package hiderules

import (
	"slices"
	"strings"
	"testing"
)

// Each case is a value an operator can type into the panel or send to the
// API. Written before the parser (#2529).
func TestParseEdgeCases(t *testing.T) {
	cases := []struct {
		name        string
		tables      []string
		columns     []string
		wantTables  []string
		wantColumns []string
		wantErr     string // substring; "" = no error
	}{
		{name: "nil is empty", wantTables: nil, wantColumns: nil},
		{name: "blank entries dropped", tables: []string{"", "  ", "\t"}, columns: []string{" "}},
		{name: "trimmed", tables: []string{"  shop.audit_log  "}, wantTables: []string{"shop.audit_log"}},
		{name: "spaces around the dot", tables: []string{"shop . audit_log"}, wantTables: []string{"shop.audit_log"}},
		{name: "schema wildcard table", tables: []string{"hr.*"}, wantTables: []string{"hr.*"}},
		{name: "duplicate differing case keeps first spelling", tables: []string{"Shop.Audit", "shop.audit"}, wantTables: []string{"Shop.Audit"}},
		{name: "duplicate column", columns: []string{"shop.customers.email", "SHOP.customers.EMAIL"}, wantColumns: []string{"shop.customers.email"}},
		{name: "column in every table of a schema", columns: []string{"shop.*.email"}, wantColumns: []string{"shop.*.email"}},
		{name: "backticks unneeded are dropped", tables: []string{"`shop`.`audit`"}, wantTables: []string{"shop.audit"}},
		{name: "backticks keep a dot inside a name", tables: []string{"`my.db`.t"}, wantTables: []string{"`my.db`.t"}},
		{name: "escaped backtick", tables: []string{"shop.`we``ird`"}, wantTables: []string{"shop.`we``ird`"}},
		{name: "quoted star is a literal name", tables: []string{"shop.`*`"}, wantTables: []string{"shop.`*`"}},
		{name: "same prefix is another table", tables: []string{"shop.cust", "shop.customers"}, wantTables: []string{"shop.cust", "shop.customers"}},

		{name: "table without schema", tables: []string{"audit_log"}, wantErr: `"audit_log"`},
		{name: "table with three parts", tables: []string{"shop.customers.email"}, wantErr: "columns list"},
		{name: "empty schema", tables: []string{".audit"}, wantErr: "empty"},
		{name: "empty table", tables: []string{"shop."}, wantErr: "empty"},
		{name: "schema wildcard refused", tables: []string{"*.audit"}, wantErr: "schema"},
		{name: "space inside unquoted name", tables: []string{"shop.audit log"}, wantErr: "backticks"},
		{name: "two entries on one line", tables: []string{"shop.a, shop.b"}, wantErr: "one entry per line"},
		{name: "newline inside an entry", tables: []string{"shop.a\nshop.b"}, wantErr: "one entry per line"},
		{name: "unterminated backtick", tables: []string{"shop.`audit"}, wantErr: "backtick"},
		{name: "empty quoted name", tables: []string{"shop.``"}, wantErr: "empty"},
		{name: "column without table", columns: []string{"shop.email"}, wantErr: "schema.table.column"},
		{name: "column star refused", columns: []string{"shop.customers.*"}, wantErr: "tables list"},
		{name: "column schema wildcard refused", columns: []string{"*.customers.email"}, wantErr: "schema"},
		{name: "star inside a name is not a prefix wildcard", tables: []string{"shop.user_*"}, wantErr: "stands alone"},
		{name: "quoted star inside a name is a literal", tables: []string{"shop.`user_*`"}, wantTables: []string{"shop.`user_*`"}},
		{name: "non-breaking space inside unquoted name", tables: []string{"shop.a\u00a0b"}, wantErr: "backticks"},
		{name: "every bad entry is named", tables: []string{"a", "b"}, wantErr: `"b"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Parse(Rules{Tables: c.tables, Columns: c.columns})
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			r := got.Rules()
			if !slices.Equal(r.Tables, c.wantTables) || !slices.Equal(r.Columns, c.wantColumns) {
				t.Fatalf("got tables=%q columns=%q, want tables=%q columns=%q", r.Tables, r.Columns, c.wantTables, c.wantColumns)
			}
		})
	}
}

func TestParseRefusesTooMany(t *testing.T) {
	many := make([]string, MaxEntries+1)
	for i := range many {
		many[i] = "s.t" + strings.Repeat("x", i%7) + string(rune('a'+i%26)) + strings.Repeat("y", i/26)
	}
	if _, err := Parse(Rules{Tables: many}); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("err = %v, want a limit refusal", err)
	}
}

func TestMatching(t *testing.T) {
	s, err := Parse(Rules{
		Tables:  []string{"shop.audit_log", "hr.*", "`my.db`.t"},
		Columns: []string{"shop.customers.email", "shop.customers.Token", "shop.*.ssn", "shop.audit_log.x"},
	})
	if err != nil {
		t.Fatal(err)
	}
	tableCases := []struct {
		schema, table string
		hidden        bool
	}{
		{"shop", "audit_log", true},
		{"SHOP", "AUDIT_LOG", true}, // case-insensitive, like MySQL identifiers here
		{"shop", "audit_log2", false},
		{"shop", "audit", false},
		{"hr", "anything", true},
		{"hrx", "anything", false},
		{"my.db", "t", true},
		{"my", "db.t", false},
		{"shop", "customers", false},
	}
	for _, c := range tableCases {
		if got := s.TableHidden(c.schema, c.table); got != c.hidden {
			t.Errorf("TableHidden(%q,%q) = %v, want %v", c.schema, c.table, got, c.hidden)
		}
	}
	if got := s.HiddenColumns("Shop", "Customers"); !slices.Equal(got, []string{"email", "Token", "ssn"}) {
		t.Errorf("HiddenColumns(shop.customers) = %q", got)
	}
	if got := s.HiddenColumns("shop", "orders"); !slices.Equal(got, []string{"ssn"}) {
		t.Errorf("HiddenColumns(shop.orders) = %q, want the schema-wide rule", got)
	}
	if got := s.HiddenColumns("billing", "customers"); got != nil {
		t.Errorf("HiddenColumns(billing.customers) = %q, want nil", got)
	}
	if !s.ColumnHidden("shop", "customers", "EMAIL") || s.ColumnHidden("shop", "customers", "name") {
		t.Error("ColumnHidden disagrees with HiddenColumns")
	}
	if s.Empty() {
		t.Error("Empty() on a set with rules")
	}
}

func TestEmptyAndNil(t *testing.T) {
	var nilSet *Set
	if !nilSet.Empty() || nilSet.TableHidden("a", "b") || nilSet.HiddenColumns("a", "b") != nil || nilSet.ColumnHidden("a", "b", "c") {
		t.Fatal("a nil *Set must hide nothing")
	}
	s, err := Parse(Rules{Tables: []string{" "}})
	if err != nil || !s.Empty() {
		t.Fatalf("blank-only rules: set=%v err=%v, want empty", s, err)
	}
	if r := s.Rules(); r.Tables != nil || r.Columns != nil {
		t.Fatalf("empty Rules() = %+v, want nil slices so YAML omits them", r)
	}
}

// The canonical form must parse back to itself, or a saved rule would change
// meaning on the next load.
func TestCanonicalRoundTrips(t *testing.T) {
	in := Rules{
		Tables:  []string{"`my.db`.t", "shop.`we``ird`", "shop.`*`", "hr.*", "`shop`.plain", "s.`\u00a0users`", "s.`users\v`", "s.` users`", "s.`user_*`"},
		Columns: []string{"`a b`.t.c", "shop.*.ssn"},
	}
	s1, err := Parse(in)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := Parse(s1.Rules())
	if err != nil {
		t.Fatalf("canonical form does not parse: %v (%+v)", err, s1.Rules())
	}
	if !slices.Equal(s1.Rules().Tables, s2.Rules().Tables) || !slices.Equal(s1.Rules().Columns, s2.Rules().Columns) {
		t.Fatalf("round trip changed: %+v -> %+v", s1.Rules(), s2.Rules())
	}
	if s2.TableHidden("s", "users") {
		t.Fatal("a quoted name with leading or trailing whitespace read back as a different, real table")
	}
	if !s2.TableHidden("s", "\u00a0users") || !s2.TableHidden("s", "users\v") || !s2.TableHidden("s", " users") {
		t.Fatal("a quoted name with whitespace lost its whitespace on the round trip")
	}
	if s2.TableHidden("shop", "x") || !s2.TableHidden("shop", "*") {
		t.Fatal("a quoted star must stay a literal table name after the round trip")
	}
}

func TestLoadRefusesWhatItCannotEnforce(t *testing.T) {
	if s, err := Load(nil); err != nil || !s.Empty() {
		t.Fatalf("Load(nil) = %v, %v; want an empty set", s, err)
	}
	s, err := Load(&Rules{Tables: []string{"shop.audit_log"}})
	if err != nil || !s.TableHidden("shop", "audit_log") {
		t.Fatalf("Load(valid) = %v, %v", s, err)
	}
	// A hand edit that does not parse: never "hide nothing".
	if _, err := Load(&Rules{Tables: []string{"audit_log"}}); err == nil || !strings.Contains(err.Error(), "do not parse") {
		t.Fatalf("Load(unparseable) err = %v", err)
	}
	// Keys a newer build wrote: restrictions this build cannot enforce.
	_, err = Load(&Rules{Tables: []string{"shop.a"}, Extra: map[string]any{"row_filters": []any{"x"}, "allow": 1}})
	if err == nil || !strings.Contains(err.Error(), "allow, row_filters") || !strings.Contains(err.Error(), "upgrade") {
		t.Fatalf("Load(future keys) err = %v", err)
	}
}
