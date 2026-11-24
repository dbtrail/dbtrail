package pgshim

import (
	"strings"
	"testing"
)

// A column the copy does not compare as PostgreSQL does is the source's wherever
// a statement uses it (#3233), and stays on the copy where it is only shown.

func userTypeCatalog() []CatalogTable {
	const domain, enum, domainOverInt = 90101, 90102, 90103
	col := func(name, typ string, oid uint32) CatalogColumn {
		return CatalogColumn{Name: name, Type: typ, SourceType: oid, SourceTypeMod: -1}
	}
	return []CatalogTable{
		{Schema: "public", Name: "tk", Columns: []CatalogColumn{
			col("id", "INTEGER", oidInt4), col("title", "VARCHAR", oidText),
			col("dc", "VARCHAR", domain), col("prio", "VARCHAR", enum), col("di", "INTEGER", domainOverInt),
		}},
		{Schema: "public", Name: "plain", Columns: []CatalogColumn{
			col("id", "INTEGER", oidInt4), col("prio", "VARCHAR", oidText), col("label", "VARCHAR", oidVarchar),
		}},
	}
}

func userTypeScreen(q string) string { return CopyScreenFor(q, userTypeCatalog()) }

func TestUserType_toTheSource(t *testing.T) {
	for _, q := range []string{
		// Named, anywhere, a plain select list included.
		`SELECT count(*) FROM tk WHERE dc = 'a'`,
		`SELECT sum(length(dc)) FROM tk`,
		`SELECT id FROM tk WHERE prio = 'high'`,
		// A domain the copy does not hold as text is one too.
		`SELECT id FROM tk WHERE di = 1`,
		`SELECT prio, count(*) FROM tk GROUP BY prio`,
		`SELECT id FROM tk ORDER BY prio`,
		`SELECT a.id FROM tk a JOIN plain b ON a.dc = b.label`,
		`SELECT a.id FROM tk a JOIN plain b USING (prio)`,
		`SELECT s.x FROM (SELECT dc AS x FROM tk) s`,
		`WITH x AS (SELECT * FROM tk) SELECT id FROM x WHERE dc = 'a'`,
		`SELECT DISTINCT dc FROM tk`,
		`SELECT dc FROM tk ORDER BY 1`,
		`SELECT * FROM tk ORDER BY 3`,
		`SELECT dc, count(*) FROM tk GROUP BY 1`,
		`SELECT dc AS x FROM tk ORDER BY x`,
		`SELECT id, dc FROM tk WHERE id IN (SELECT id FROM plain)`,
		// A star past a plain select list.
		`SELECT DISTINCT * FROM tk`,
		`SELECT DISTINCT ON (id) * FROM tk`,
		`SELECT * FROM (SELECT * FROM tk) s`,
		`WITH x AS (SELECT * FROM tk) SELECT * FROM x`,
		`SELECT id FROM plain WHERE label IN (SELECT * FROM tk)`,
		// The row as a value, or the column by another way.
		`SELECT tk FROM tk`,
		`SELECT to_json(t) FROM tk t`,
		`SELECT count(DISTINCT t) FROM tk t`,
		`SELECT count(t.*) FROM tk t`,
		`WITH x AS (SELECT * FROM tk) SELECT to_json(x) FROM x`,
		`SELECT p.id FROM plain p NATURAL JOIN tk`,
		`SELECT x FROM tk AS a(i, t, x)`,
		`SELECT label FROM plain WHERE label IN (TABLE tk)`,
		`SELECT id FROM tk WHERE #3 = 'a'`,
		`SELECT COLUMNS('d') FROM tk`,
		// A column list after a quoted alias, or one named within.
		`SELECT count(*) FROM tk AS "a"(i, t, x) WHERE x = 'a'`,
		`SELECT count(*) FROM tk AS within(i, t, x) WHERE x = 'a'`,
		// A row of a parenthesized join, a method call on a row.
		`SELECT j FROM (plain CROSS JOIN tk) AS j`,
		`SELECT count(DISTINCT j) FROM (plain JOIN tk ON plain.id = tk.id) j`,
		`SELECT t.to_json() FROM tk t`,
		// The copy's own forms of a star.
		`SELECT DISTINCT ON (id) * NOT LIKE 'x%' FROM tk`,
	} {
		if veto := userTypeScreen(q); veto == "" {
			t.Errorf("%s: left to the copy", q)
		}
	}
}

func TestUserType_leftToTheCopy(t *testing.T) {
	for _, q := range []string{
		`SELECT id, title FROM tk WHERE id > 1`,
		// Shown, whole, in a plain select list: both sides write the value.
		`SELECT dc FROM tk`,
		`SELECT t.dc AS x, t.id FROM tk t`,
		`SELECT "dc" FROM tk`,
		`SELECT * FROM tk`,
		`SELECT * FROM tk WHERE id = 1`,
		`SELECT t.* FROM tk t`,
		`SELECT * FROM plain p JOIN tk t ON t.id = p.id`,
		`SELECT count(*) FROM tk`,
		`SELECT id * 2 FROM tk`,
		`SELECT count(*) FROM (SELECT * FROM tk) s`,
		`WITH x AS (SELECT * FROM tk) SELECT id FROM x`,
		`WITH x AS (SELECT * FROM tk), y AS (SELECT * FROM x) SELECT id, title FROM y`,
		`WITH s AS (SELECT id, title FROM tk) SELECT * FROM s`,
		`WITH s AS (SELECT id FROM tk), r AS (SELECT s.*, 1 AS n FROM s) SELECT r.id, t.title FROM r JOIN tk t ON t.id = r.id`,
		`SELECT p.id, p.label FROM plain p JOIN tk t ON t.id = p.id`,
		`SELECT p.* FROM plain p JOIN tk t ON t.id = p.id`,
		// A star over a table that holds none, beside one that does.
		`SELECT p.* FROM plain p JOIN tk t ON t.id = p.id WHERE p.id = $1`,
		`SELECT p.* FROM plain p JOIN tk t USING (id)`,
		`SELECT * FROM plain UNION ALL SELECT id, title, title FROM tk`,
		`SELECT id FROM plain p WHERE EXISTS (SELECT * FROM tk t WHERE t.id = p.id)`,
		// The same name in a table the statement reads alone is plain text.
		`SELECT prio FROM plain`,
		`SELECT * FROM plain`,
	} {
		if veto := userTypeScreen(q); veto != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, veto)
		}
	}
}

// On a connection with no source this rule is the only screen, so what the
// other rules of the screen would also catch is read here by itself.
func TestUserType_theRuleAlone(t *testing.T) {
	use := func(q string) string {
		toks, why := lexSQL(q)
		if why != "" {
			t.Fatalf("%s: %s", q, why)
		}
		tables := userTypeCatalog()
		return userTypeUse(toks, screenTypes(tables, tablesRead(q, tables)))
	}
	for _, q := range []string{
		`SELECT dc FROM tk ORDER BY 1`,
		`SELECT * FROM tk ORDER BY 3`,
		`SELECT * FROM tk ORDER BY (3) DESC NULLS LAST`,
		`SELECT count(*) FROM tk AS within(i, t, x) WHERE x = 'a'`,
		`SELECT count(*) FROM tk AS "a"(i, t, x) WHERE x = 'a'`,
		`SELECT t.to_json() FROM tk t`,
		`SELECT 1 WHERE EXISTS (SELECT * FROM tk INTERSECT SELECT * FROM plain)`,
		`SELECT 1 WHERE EXISTS (SELECT * FROM tk GROUP BY 1, 2 HAVING count(*) > 1)`,
		`TABLE tk ORDER BY 3`,
		`SELECT dc FROM tk ORDER BY ALL`,
		`SELECT * FROM tk ORDER BY ALL`,
	} {
		if use(q) == "" {
			t.Errorf("%s: left to the copy", q)
		}
	}
	for _, q := range []string{
		`SELECT id, count(*) FROM tk GROUP BY id ORDER BY 2`,
		`SELECT id FROM plain p WHERE EXISTS (SELECT * FROM tk t WHERE t.id = p.id)`,
		`TABLE plain ORDER BY 2`,
		`TABLE tk`,
	} {
		if veto := use(q); veto != "" {
			t.Errorf("%s: %s", q, veto)
		}
	}
}

func TestUserType_aDeclaredArgument(t *testing.T) {
	const oidInet, oidJSONB, oidInt4Array, oidTextArray = 869, 3802, 1007, 1009
	for _, oid := range []uint32{oidBpchar, oidBpcharArray, 90101, oidInet, oidJSONB} {
		if !paddedArgument([]uint32{oidInt4, oid}) {
			t.Errorf("an argument of type %d: left to the copy", oid)
		}
	}
	for _, oid := range []uint32{0, oidText, oidVarchar, oidInt4, oidInt8, oidNumeric, oidTimestamp, oidInt4Array, oidTextArray} {
		if paddedArgument([]uint32{oid}) {
			t.Errorf("an argument of type %d: sent to the source", oid)
		}
	}
}

// A built-in type the copy holds in another form is used and shown by the
// same rule; one it holds as PostgreSQL does is not caught.
func TestUserType_builtInTypes(t *testing.T) {
	const oidMoney, oidInet, oidJSONB = 790, 869, 3802
	col := func(name, typ string, oid uint32) CatalogColumn {
		return CatalogColumn{Name: name, Type: typ, SourceType: oid, SourceTypeMod: -1}
	}
	tables := []CatalogTable{{Schema: "public", Name: "b", Columns: []CatalogColumn{
		col("id", "INTEGER", oidInt4), col("cost", "VARCHAR", oidMoney), col("addr", "VARCHAR", oidInet),
		col("doc", "VARCHAR", oidJSONB), col("raw", "BLOB", oidBytea), col("num", "UINTEGER", oidOID),
		col("one", "VARCHAR", oidChar), col("n", "BIGINT", oidInt8), col("title", "VARCHAR", oidText),
		col("tags", "VARCHAR[]", oidTextArray), col("vtags", "VARCHAR[]", oidVarcharArray), col("ntags", "VARCHAR[]", oidNameArray),
		col("ips", "VARCHAR[]", 1041), // inet[]
	}}}
	for _, q := range []string{
		`SELECT id FROM b WHERE addr = '10.0.0.1'`, `SELECT id FROM b WHERE doc = '{}'`, `SELECT min(cost) FROM b`,
		`SELECT id FROM b WHERE raw = 'ab'`, `SELECT id FROM b WHERE num = '0010'`, `SELECT count(DISTINCT addr) FROM b`,
		`SELECT id FROM b WHERE one = 'a'`,
		`SELECT id FROM b WHERE '10.0.0.1' = ANY(ips)`,
	} {
		if veto := CopyScreenFor(q, tables); veto == "" {
			t.Errorf("%s: left to the copy", q)
		}
	}
	for _, q := range []string{`SELECT cost, addr, doc FROM b`, `SELECT * FROM b`, `SELECT raw, num FROM b`, `SELECT id FROM b WHERE n > 1 AND title = 'x'`,
		`SELECT id FROM b WHERE 'z' = ANY(tags)`, `SELECT id FROM b WHERE 'z' = ANY(vtags)`, `SELECT id FROM b WHERE 'z' = ANY(ntags)`} {
		if veto := CopyScreenFor(q, tables); veto != "" {
			t.Errorf("%s: sent to the source (%s)", q, veto)
		}
	}
}

// A star over a table with no such column decides as it would on a catalog
// that holds none at all.
func TestUserType_aStarOverOtherTablesIsUntouched(t *testing.T) {
	without := userTypeCatalog()[1:]
	for _, q := range []string{
		`SELECT * FROM plain`,
		`SELECT p.* FROM plain p WHERE p.id = 1`,
		`SELECT * FROM (SELECT * FROM plain) s`,
		`WITH x AS (SELECT * FROM plain) SELECT * FROM x`,
		`SELECT DISTINCT * FROM plain`,
		`SELECT * FROM plain ORDER BY 2`,
	} {
		if got, want := userTypeScreen(q), CopyScreenFor(q, without); got != want {
			t.Errorf("%s: %q, and %q with no such table in the catalog", q, got, want)
		}
	}
}

// A statement the port does not read whole cannot be told not to read it.
func TestUserType_aStatementNotReadWhole(t *testing.T) {
	types := screenTypes(userTypeCatalog(), nil)
	for _, q := range []string{`SELECT $$a$$, dc FROM tk`, `SELECT $$a$$, * FROM plain`, `SELECT $$a$$ FROM tk`} {
		if veto := paddedInText(q, types); !strings.Contains(veto, "does not compare as PostgreSQL") {
			t.Errorf("%s: %q", q, veto)
		}
	}
	if veto := paddedInText(`SELECT $$a$$, label FROM plain`, types); veto != "" {
		t.Errorf("a statement of another table: %q", veto)
	}
}
