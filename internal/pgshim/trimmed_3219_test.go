package pgshim

import "testing"

// trimmedCatalog: a has a char(n) the view trims (c), a varchar (v), a text
// (t); b another trimmed char(n) (c2) and a text (x); d a char(n) named c
// the view does not trim (a domain over it, or one of two names that differ
// only in case); e a varchar named c.
func trimmedCatalog() []CatalogTable {
	trimmed := func(n string) CatalogColumn {
		return CatalogColumn{Name: n, Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: 9, TrimmedOnCopy: true}
	}
	col := func(n, typ string, oid uint32) CatalogColumn {
		return CatalogColumn{Name: n, Type: typ, SourceType: oid, SourceTypeMod: -1}
	}
	return []CatalogTable{
		{Schema: "public", Name: "a", Columns: []CatalogColumn{col("id", "INTEGER", 23), trimmed("c"), col("v", "VARCHAR", oidVarchar), col("t", "VARCHAR", oidText), col("k", "INTEGER", 23), col("arr", "VARCHAR[]", 1009)}},
		{Schema: "public", Name: "b", Columns: []CatalogColumn{col("id", "INTEGER", 23), trimmed("c2"), col("v2", "VARCHAR", oidVarchar), col("t2", "VARCHAR", oidText), col("x", "VARCHAR", oidText)}},
		{Schema: "public", Name: "d", Columns: []CatalogColumn{col("id", "INTEGER", 23), {Name: "c", Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: 9}}},
		{Schema: "public", Name: "e", Columns: []CatalogColumn{col("id", "INTEGER", 23), col("c", "VARCHAR", oidVarchar)}},
	}
}

// Every use of a trimmed char(n) the screen lets through gives PostgreSQL's
// rows: the forms measured on PostgreSQL 16 against the copy without
// padding (#3219, part 3b). Everything else is the source's.
func TestTrimmedUse_allowed(t *testing.T) {
	for _, q := range []string{
		// Shown, grouped, counted, sorted.
		"SELECT c FROM a",
		"SELECT a.c, count(*) FROM a GROUP BY a.c",
		"SELECT DISTINCT c FROM a",
		"SELECT count(c), count(DISTINCT c) FROM a",
		"SELECT id FROM a ORDER BY c",
		"SELECT c FROM a WHERE c = 'ab' ORDER BY id LIMIT 1",
		"SELECT id FROM a WHERE c IS NULL",
		"SELECT id FROM a WHERE c IS NOT NULL",
		// Compared with a plain string with no trailing blank.
		"SELECT id FROM a WHERE c = 'ab'",
		"SELECT id FROM a WHERE 'ab' = c",
		"SELECT id FROM a WHERE c <> 'ab'",
		"SELECT id FROM a WHERE c != 'ab'",
		"SELECT id FROM a WHERE NOT c = 'ab'",
		"SELECT id FROM a WHERE c = ''",
		"SELECT id FROM a WHERE c = 'ab''c'",
		"SELECT id FROM a WHERE c = 'ab\t'",
		"SELECT id FROM a WHERE C = 'ab'",
		`SELECT id FROM a WHERE "c" = 'ab'`,
		"SELECT id FROM a WHERE a.c = 'ab'",
		"SELECT id FROM a WHERE A.C = 'ab'",
		"SELECT id FROM a WHERE c = 'ab' AND k = 1",
		"SELECT id FROM a WHERE k = 1 OR c = 'ab'",
		"SELECT CASE WHEN c = 'ab' THEN 1 ELSE 0 END FROM a",
		// IN of plain strings.
		"SELECT id FROM a WHERE c IN ('ab', 'x')",
		"SELECT id FROM a WHERE c NOT IN ('ab')",
		// LIKE with a pattern that ends in %.
		"SELECT id FROM a WHERE c LIKE 'ab%'",
		"SELECT id FROM a WHERE c LIKE 'ab%%'",
		"SELECT id FROM a WHERE c LIKE '%'",
		"SELECT id FROM a WHERE c LIKE 'a%b%'",
		"SELECT id FROM a WHERE c NOT LIKE 'ab%'",
		// Cut.
		"SELECT id, substring(c from 'a.') FROM a ORDER BY id",
		"SELECT id FROM a WHERE substring(c from 'a.') = 'ab'",
		"SELECT id, substr(c, 1, 2) FROM a ORDER BY id",
		"SELECT ascii(substring(c from 1 for 1)) FROM a",
		// Grouped by a window's partition, which does not depend on the order.
		"SELECT id, row_number() OVER (PARTITION BY c ORDER BY id) FROM a",
		// In a subquery whose output is read again by name, or EXISTS.
		"SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.c2 = 'ab')",
		"SELECT s.c FROM (SELECT c FROM a) s WHERE s.c = 'ab'",
		"WITH w AS (SELECT c FROM a) SELECT c FROM w WHERE c = 'ab'",
		// A name of a table the statement does not read counts for nothing.
		"SELECT id FROM e WHERE c = 'ab  '",
	} {
		if why := CopyScreenFor(q, trimmedCatalog()); why != "" {
			t.Errorf("%s\n    sent to the source: %s", q, why)
		}
	}
}

func TestTrimmedUse_source(t *testing.T) {
	for _, q := range []string{
		// A string PostgreSQL compares without its trailing blanks.
		"SELECT id FROM a WHERE c = 'ab '",
		"SELECT id FROM a WHERE 'ab  ' = c",
		"SELECT id FROM a WHERE c = '   '",
		"SELECT id FROM a WHERE c IN ('a ', 'b')",
		"SELECT id FROM a WHERE c IN ('a', $1)",
		"SELECT id FROM a WHERE c IN ('a', 1)",
		"SELECT id FROM a WHERE c IN (SELECT t2 FROM b)",
		// A string whose value is not read here, a parameter, an expression.
		"SELECT id FROM a WHERE c = E'ab'",
		"SELECT id FROM a WHERE c = U&'ab'",
		"SELECT id FROM a WHERE c = $x$ab$x$",
		"SELECT id FROM a WHERE c = 'ab'::bpchar",
		"SELECT id FROM a WHERE c = 'a' || 'b'",
		"SELECT id FROM a WHERE c = $1",
		"SELECT id FROM a WHERE $1 = c",
		"SELECT id FROM a WHERE c = upper('ab')",
		"SELECT id FROM a WHERE c = t || ''",
		"SELECT id FROM a WHERE c = t::varchar",
		"SELECT id FROM a WHERE c = arr[1]",
		"SELECT id FROM a WHERE c = 'ab' COLLATE \"C\"",
		"SELECT id FROM a WHERE c = (SELECT 'ab'::varchar)",
		// A varchar: PostgreSQL compares it as bpchar.
		"SELECT id FROM a WHERE c = v",
		"SELECT id FROM a WHERE v = c",
		"SELECT a.id FROM a JOIN b ON a.c = b.v2",
		// LIKE that sees the padding, or a pattern not read here.
		"SELECT id FROM a WHERE c LIKE '%b'",
		"SELECT id FROM a WHERE c LIKE 'ab'",
		"SELECT id FROM a WHERE c LIKE 'a_%'",
		"SELECT id FROM a WHERE c LIKE 'ab %'",
		`SELECT id FROM a WHERE c LIKE 'ab\%'`,
		"SELECT id FROM a WHERE c LIKE 'ab%' ESCAPE '#'",
		"SELECT id FROM a WHERE c ILIKE 'ab%'",
		"SELECT id FROM a WHERE t LIKE c",
		// Left out of this slice (no report of the 22 needs them): another
		// column on the other side, and what turns it into text, whose
		// lengths the copy types int8 where PostgreSQL types int4.
		"SELECT id FROM a WHERE c = t",
		"SELECT a.id FROM a JOIN b ON a.c = b.c2",
		"SELECT a.id FROM a JOIN b ON b.c2 = a.c",
		"SELECT a.id FROM a JOIN b ON a.c = b.t2",
		"SELECT id FROM a WHERE c = v::text",
		"SELECT id, c || 1 FROM a ORDER BY id",
		"SELECT id, 1 || c FROM a ORDER BY id",
		"SELECT id, c || c FROM a ORDER BY id",
		"SELECT id, length(c) FROM a ORDER BY id",
		"SELECT id, char_length(c) FROM a ORDER BY id",
		"SELECT id, character_length(c) FROM a ORDER BY id",
		"SELECT id, position('a' in c) FROM a ORDER BY id",
		"SELECT id FROM a WHERE position('a' in c) = 1",
		// Sorted where the order of bytes changes values or rows, not only
		// the order of the result.
		"SELECT id, row_number() OVER (ORDER BY c) FROM a",
		"SELECT id, rank() OVER (PARTITION BY k ORDER BY c) FROM a",
		"SELECT string_agg(c, ',' ORDER BY c) FROM a",
		"SELECT array_agg(c ORDER BY c) FROM a",
		"SELECT s.id FROM (SELECT id FROM a ORDER BY c LIMIT 2) s",
		"WITH w AS (SELECT id FROM a ORDER BY c LIMIT 2) SELECT id FROM w",
		"SELECT id FROM a WHERE id IN (SELECT id FROM a ORDER BY c LIMIT 2)",
		// Forms not measured.
		"SELECT id FROM a WHERE c IS DISTINCT FROM 'ab'",
		"SELECT id FROM a WHERE c = ANY('{ab}')",
		"SELECT id FROM a WHERE NULLIF(c, 'ab') IS NULL",
		"SELECT id FROM a WHERE COALESCE(c, 'x') = 'ab'",
		"SELECT CASE c WHEN 'ab' THEN 1 END FROM a",
		"SELECT concat(c, 'x') FROM a",
		"SELECT octet_length(c) FROM a",
		"SELECT upper(c) FROM a",
		"SELECT c::text FROM a",
		"SELECT CAST(c AS text) FROM a",
		"SELECT id FROM a WHERE (c, 1) = ('ab', 1)",
		"SELECT ascii(c) FROM a",
		"SELECT min(c) FROM a",
		"SELECT id FROM a WHERE c < 'b'",
		"SELECT id FROM a WHERE c BETWEEN 'a' AND 'b'",
		// Its output compared where PostgreSQL takes the other side as bpchar.
		"SELECT id FROM a WHERE v IN (SELECT c FROM a)",
		"SELECT id FROM a WHERE v = (SELECT c FROM a LIMIT 1)",
		"SELECT v FROM a UNION SELECT c FROM a",
		"SELECT c FROM a UNION ALL SELECT v FROM a",
		// Reached without its name.
		"SELECT id FROM a NATURAL JOIN e",
		"SELECT count(DISTINCT a) FROM a",
		"SELECT a.id FROM a JOIN b ON a = b",
		"SELECT * FROM a JOIN e USING (c)",
		// Under another name.
		"SELECT s.x FROM (SELECT v AS x FROM a) s, b WHERE b.c2 = s.x",
		"SELECT y FROM (SELECT c AS y FROM a) s WHERE y = 'ab'",
		"SELECT q FROM a AS s(q) WHERE q = 'ab'",
		// The same name, not trimmed, in a table it reads.
		"SELECT a.id FROM a JOIN d ON a.id = d.id WHERE c = 'ab'",
		"SELECT id FROM d WHERE c = 'ab'",
	} {
		if why := CopyScreenFor(q, trimmedCatalog()); why == "" {
			t.Errorf("%s\n    left to the copy", q)
		}
	}
}

// A statement the port does not read whole that names a trimmed char(n) is
// not the copy's on a connection with no source.
func TestTrimmedUse_notReadWhole(t *testing.T) {
	types := screenTypes(trimmedCatalog(), nil)
	if why := paddedInText("SELECT id FROM a WHERE c = E'ab  '", types); why == "" {
		t.Error("an E'' string compared with a trimmed char(n) was left to the copy")
	}
}

// With no source, the copy's order of text would be the only answer.
func TestTrimmedUse_noSourceOrder(t *testing.T) {
	types := screenTypes(trimmedCatalog(), nil)
	toks, _ := lexSQL("SELECT id FROM a ORDER BY c")
	if why := paddedForNoSource(toks, types); why == "" {
		t.Error("ORDER BY a trimmed char(n) left to the copy with no source")
	}
	toks, _ = lexSQL("SELECT id FROM a WHERE c = 'ab'")
	if why := paddedForNoSource(toks, types); why != "" {
		t.Errorf("c = 'ab' with no source: %s", why)
	}
}
