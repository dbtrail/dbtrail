package pgshim

import "testing"

// A connection with no source refuses a value cast to char(n) or "char"
// (#3233): the copy would answer it as the text written.
func TestPaddedForNoSource_castToPaddedType(t *testing.T) {
	types := map[string][]string{"id": {"BIGINT"}, "name": {"VARCHAR"}}
	for _, q := range []string{
		`SELECT 'abc'::"char" FROM t`,
		`SELECT name::"char" FROM t`,
		`SELECT 'abc'::char(2) FROM t`,
		`SELECT 'a '::CHAR(3) = 'a' FROM t`,
		`SELECT length(CAST('ab' AS char(4))) FROM t`,
		`SELECT CAST(name AS character(2)) FROM t`,
		`SELECT CAST(name AS nchar(2)) FROM t`,
		`SELECT name::bpchar FROM t`,
		`SELECT 'abc'::pg_catalog."char" FROM t`,
		`SELECT 'abc'::pg_catalog.bpchar FROM t`,
		`SELECT bpchar 'a ' = 'a' FROM t`,
		`SELECT count(*) FROM t WHERE name::char(4) = 'a'`,
		`SELECT 'abc'::"bpchar" FROM t`,
	} {
		toks, why := lexSQL(q)
		if why != "" {
			t.Fatalf("%s: not lexed: %s", q, why)
		}
		if veto := paddedForNoSource(toks, types); veto == "" {
			t.Errorf("%s: answered by the copy, want it refused", q)
		}
	}
	for _, q := range []string{
		`SELECT id, name FROM t`,
		`SELECT name::varchar(2) FROM t`,
		`SELECT 'abc'::character varying(2) FROM t`,
		`SELECT CAST(name AS char varying(3)) FROM t`,
		`SELECT count(*) FROM t WHERE name = $1`,
		`SELECT 'char' FROM t`,
		`SELECT "Char" FROM t`,
	} {
		toks, why := lexSQL(q)
		if why != "" {
			t.Fatalf("%s: not lexed: %s", q, why)
		}
		if veto := paddedForNoSource(toks, types); veto != "" {
			t.Errorf("%s: refused for %s", q, veto)
		}
	}
}

// A statement the port does not read whole is held to the same rule by its
// text.
func TestPaddedTypeInText(t *testing.T) {
	for q, want := range map[string]bool{
		`SELECT 'ab'::char(5) = 'ab   ', 1e0`:    true,
		`SELECT E'abc'::"char"`:                  true,
		`SELECT $$ab $$::BPCHAR = 'ab'`:          true,
		`SELECT CAST(x AS Character(2)), 1e0`:    true,
		`SELECT n.nchar_x, E'a', 1e0 FROM n`:     false,
		`SELECT varchar_col, E'char_' FROM t`:    false,
		`SELECT 1e0, $$text$$`:                   false,
		`SELECT "my$char" FROM t WHERE x = E'a'`: false,
	} {
		if got := paddedTypeInText(q); got != want {
			t.Errorf("%s: %v, want %v", q, got, want)
		}
	}
}
