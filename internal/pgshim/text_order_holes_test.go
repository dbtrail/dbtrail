package pgshim

import (
	"strings"
	"testing"
	"time"
)

// Statements that sort or compare text and that the screen read as having no
// text in them. What PostgreSQL and the copy answer was measured by
// internal/pgcompare (textOrderHoles).

// orderedScreen is the screen's decision with the copy's columns in their
// order, as a connection has them (columnTypes).
func orderedScreen(stmt string) string {
	tables := starCatalog()
	return CopyScreen(stmt, withColumnOrder(screenTypes(tables, tablesRead(RewriteForCopy(stmt), tables)), tables))
}

// starCatalog is a copy of two tables with no char(n) column (a star over
// one of those is another rule's, paddedUse): plain is (id, code text, name,
// status, qty), nums (id, k, at), all numbers and a timestamp.
func starCatalog() []CatalogTable {
	col := func(name, typ string, oid uint32) CatalogColumn {
		return CatalogColumn{Name: name, Type: typ, SourceType: oid, SourceTypeMod: -1}
	}
	return []CatalogTable{
		{Schema: "public", Name: "plain", Columns: []CatalogColumn{
			col("id", "INTEGER", oidInt4), col("code", "VARCHAR", oidText), col("name", "VARCHAR", oidVarchar),
			col("status", "VARCHAR", oidText), col("qty", "INTEGER", oidInt4)}},
		{Schema: "public", Name: "nums", Columns: []CatalogColumn{
			col("id", "INTEGER", oidInt4), col("k", "INTEGER", oidInt4), col("at", "TIMESTAMP", 1114)}},
	}
}

// peopleTypes are the copy's types for a table whose text columns are named
// like words of ORDER BY.
var peopleTypes = map[string][]string{
	"id": {"INTEGER"}, "age": {"INTEGER"}, "born": {"DATE"}, "amount": {"DECIMAL(12,2)"},
	"first": {"VARCHAR"}, "last": {"VARCHAR"}, "name": {"VARCHAR"}, "nulls": {"VARCHAR"},
	"rows": {"VARCHAR"}, "range": {"VARCHAR"}, "groups": {"VARCHAR"},
}

func TestMaybeTextType(t *testing.T) {
	if !typeIsText(maybeTextType) {
		t.Fatalf("%q is not read as text", maybeTextType)
	}
}

// A name the statement gives a value, that is also a column's name.
func TestCopyScreen_aliasNamedLikeItsColumn(t *testing.T) {
	toSource := map[string]string{
		`SELECT name AS name FROM people ORDER BY name`:                                "ORDER BY",
		`SELECT p.name AS name FROM people p ORDER BY p.name`:                          "ORDER BY",
		`SELECT name name FROM people ORDER BY name`:                                   "ORDER BY",
		`SELECT min(name) AS name FROM people`:                                         "min()",
		`SELECT max(name) AS name, id FROM people GROUP BY id`:                         "max()",
		`SELECT id AS name FROM people ORDER BY name`:                                  "ORDER BY",
		`SELECT name AS id FROM people ORDER BY id`:                                    "ORDER BY",
		`SELECT name AS age FROM people WHERE id > 1 ORDER BY age`:                     "ORDER BY",
		`WITH s AS (SELECT name AS name FROM people) SELECT name FROM s ORDER BY name`: "ORDER BY",
		`SELECT s.name FROM (SELECT name AS name, id FROM people) s ORDER BY s.name`:   "ORDER BY",
		`SELECT greatest(name, last) AS name FROM people`:                              "greatest()",
		`SELECT name AS name, count(*) FROM people GROUP BY name ORDER BY 1`:           "ORDER BY",
		`SELECT last AS first, first AS last FROM people ORDER BY first`:               "ORDER BY",
	}
	for q, want := range toSource {
		if why := CopyScreen(q, peopleTypes); !strings.Contains(why, want) {
			t.Errorf("%s:\n  got %q, want it sent to the source for %q", q, why, want)
		}
	}
	for _, q := range []string{
		// The same name over a column that is not text.
		`SELECT id AS id, age AS age FROM people ORDER BY age, id`,
		`SELECT p.amount AS amount, p.age AS age FROM people p ORDER BY amount DESC NULLS LAST, age`,
		`SELECT born AS born FROM people ORDER BY born`,
		`SELECT id AS id, sum(age) AS age FROM people GROUP BY id ORDER BY age DESC`,
		`SELECT s.age FROM (SELECT age AS age, id FROM people) s WHERE s.age > 3 ORDER BY s.age`,
		// A name of its own.
		`SELECT id, sum(amount) AS total FROM people GROUP BY id ORDER BY total DESC`,
		`SELECT id, count(*) AS n FROM people GROUP BY id HAVING count(*) > 1 ORDER BY n, id`,
		// The text column under its own name, not sorted or compared by order.
		`SELECT name AS name, id FROM people ORDER BY id`,
		`SELECT name AS name, count(*) FROM people GROUP BY name`,
	} {
		if why := CopyScreen(q, peopleTypes); why != "" {
			t.Errorf("%s:\n  sent to the source (%s), want the copy", q, why)
		}
		if toks, lexWhy := lexSQL(RewriteForCopy(q)); lexWhy == "" {
			if why := screenUntyped(toks); why != "" {
				t.Errorf("%s:\n  vetoed without its types (%s), so it never reaches the plan", q, why)
			}
		}
	}
}

// A text column named like a word of ORDER BY is the key, not a modifier.
func TestCopyScreen_columnNamedLikeAnOrderWord(t *testing.T) {
	for _, q := range []string{
		`SELECT first, last FROM people ORDER BY last`,
		`SELECT first, last FROM people ORDER BY first`,
		`SELECT id FROM people ORDER BY first DESC`,
		`SELECT id FROM people ORDER BY last NULLS LAST, id`,
		`SELECT id FROM people ORDER BY last ASC NULLS FIRST`,
		`SELECT id FROM people ORDER BY id, last`,
		`SELECT p.id FROM people p ORDER BY p.first`,
		`SELECT id FROM people ORDER BY nulls`,
		`SELECT id FROM people ORDER BY nulls NULLS LAST`,
		`SELECT id, row_number() OVER (ORDER BY last) FROM people`,
		`SELECT string_agg(first, ',' ORDER BY last) FROM people`,
		// A key cut short by a word that ends the list: nothing was read of it.
		`SELECT id FROM people ORDER BY rows`,
		`SELECT id FROM people ORDER BY id, range`,
		`SELECT id FROM people ORDER BY groups DESC`,
		// Compared and aggregated by order too.
		`SELECT min(first), max(last) FROM people`,
		`SELECT id FROM people WHERE last > 'B'`,
		`SELECT id FROM people WHERE first BETWEEN 'a' AND 'c'`,
		`SELECT greatest(first, last) FROM people`,
	} {
		if why := CopyScreen(q, peopleTypes); why == "" {
			t.Errorf("%s: left to the copy, want the source (text by order)", q)
		}
	}
	for _, q := range []string{
		`SELECT id, first, last FROM people ORDER BY id DESC NULLS LAST`,
		`SELECT id, first FROM people ORDER BY age DESC NULLS FIRST, id ASC`,
		`SELECT id FROM people ORDER BY born NULLS LAST`,
		`SELECT id FROM people ORDER BY id ASC`,
		`SELECT id FROM people WHERE first = 'Ana' OR last IN ('Ana', 'bruno') ORDER BY id`,
		`SELECT id, row_number() OVER (PARTITION BY last ORDER BY age DESC NULLS LAST) FROM people`,
		// A copy with a text column named last: NULLS LAST is still no name.
		`SELECT id, age FROM people ORDER BY 2 DESC NULLS LAST, 1`,
		`SELECT id, sum(age) AS years FROM people GROUP BY 1 ORDER BY 2 DESC NULLS LAST, 1 ASC NULLS FIRST`,
		`SELECT DISTINCT first, last FROM people`,
	} {
		if why := CopyScreen(q, peopleTypes); why != "" {
			t.Errorf("%s:\n  sent to the source (%s), want the copy", q, why)
		}
		if toks, lexWhy := lexSQL(RewriteForCopy(q)); lexWhy == "" {
			if why := screenUntyped(toks); why != "" {
				t.Errorf("%s:\n  vetoed without its types (%s)", q, why)
			}
		}
	}
}

// ORDER BY a position, in a statement with a * in a select list.
func TestStarByPosition(t *testing.T) {
	// plain is (id, code text, name, status, qty); nums (id, k, at).
	for _, q := range []string{
		`SELECT * FROM plain ORDER BY 2`,
		`SELECT * FROM plain ORDER BY 1, 3 DESC NULLS LAST`,
		`SELECT * FROM plain p WHERE qty > 1 ORDER BY 4 LIMIT 3`,
		`SELECT * FROM public.plain AS p ORDER BY 2`,
		`SELECT * FROM plain ORDER BY 9`,
		`SELECT * FROM plain ORDER BY 0`,
		`SELECT * FROM plain, nums WHERE nums.id = plain.id ORDER BY 3`,
		`SELECT * FROM nums n, plain p WHERE p.id = n.id ORDER BY 5`,
		`SELECT * FROM plain p JOIN nums c ON c.id = p.id ORDER BY 1`,
		`SELECT p.* FROM plain p ORDER BY 2`,
		`SELECT *, qty FROM plain ORDER BY 2`,
		`SELECT * FROM (SELECT * FROM plain) s ORDER BY 2`,
		`SELECT * FROM (SELECT id, name FROM plain) s ORDER BY 2`,
		`SELECT * FROM (SELECT id, 'x' AS tag FROM plain) s ORDER BY 2`,
		`WITH x AS (SELECT id, qty FROM plain) SELECT * FROM x ORDER BY 2`,
		`SELECT * FROM nowhere ORDER BY 1`,
		`SELECT id FROM plain WHERE id IN (SELECT * FROM plain ORDER BY 2)`,
		`SELECT * FROM plain UNION ALL SELECT * FROM plain ORDER BY 2`,
		// However the ORDER BY is written, and wherever it stands.
		`(SELECT * FROM plain) ORDER BY 2`,
		`(SELECT * FROM plain) UNION ALL (SELECT * FROM plain) ORDER BY 2`,
		`SELECT * FROM plain ORDER BY (2)`,
		`SELECT * FROM plain ORDER BY ((2)) DESC NULLS LAST`,
		`SELECT * FROM plain UNION ALL SELECT id, NULL, NULL, NULL, k FROM nums ORDER BY 2`,
		`SELECT id, NULL, NULL, NULL, k FROM nums UNION ALL SELECT * FROM plain ORDER BY 2`,
		`SELECT * FROM (plain p JOIN nums n ON n.id = p.id) ORDER BY 2`,
		`SELECT * FROM (nums n JOIN plain p ON n.id = p.id) ORDER BY 5`,
		`WITH c AS (SELECT * FROM plain) SELECT * FROM (SELECT * FROM c) s ORDER BY 2`,
		`WITH c AS (SELECT id, name FROM plain) SELECT * FROM (SELECT * FROM c) s ORDER BY 2`,
		`SELECT DISTINCT ON (2) * FROM plain ORDER BY 2`,
		`SELECT DISTINCT * FROM plain ORDER BY 2`,
		`SELECT ALL * FROM plain ORDER BY 2`,
		`SELECT * FROM plain ORDER BY 2;`,
		`SELECT * FROM plain ORDER BY 1, 2 LIMIT 5 OFFSET 1`,
		`SELECT n.*, p.* FROM nums n, plain p ORDER BY 5`,
		`SELECT * FROM plain WHERE id IN (SELECT id FROM nums) ORDER BY 2`,
	} {
		if got := orderedScreen(q); !strings.Contains(got, "ORDER BY") {
			t.Errorf("%s:\n  got %q, want it sent to the source for the text it sorts", q, got)
		}
	}
	for _, q := range []string{
		// A whole number at that position: sorted the same on both sides.
		`SELECT * FROM plain ORDER BY 1`,
		`SELECT * FROM plain ORDER BY 5 DESC, 1`,
		`SELECT * FROM plain ORDER BY (1), ((5)) DESC NULLS LAST`,
		`SELECT * FROM plain p WHERE qty > 1 ORDER BY 1 LIMIT 3`,
		`SELECT * FROM public.plain ORDER BY 1`,
		`SELECT * FROM nums ORDER BY 1`,
		`SELECT * FROM nums ORDER BY 3 DESC NULLS LAST, 2, 1`,
		`(SELECT * FROM nums) ORDER BY 2`,
		`SELECT * FROM nums UNION ALL SELECT * FROM nums ORDER BY 2`,
		`SELECT DISTINCT ON (2) * FROM nums ORDER BY 2`,
		// Rows with no text in them.
		`SELECT * FROM nums a, nums b WHERE b.id = a.k ORDER BY 1, 4`,
		`SELECT * FROM (nums a JOIN nums b ON b.id = a.k) ORDER BY 2`,
		`SELECT * FROM nums a JOIN (SELECT id, qty FROM plain) p ON p.id = a.id ORDER BY 5`,
		`SELECT * FROM (SELECT id, k FROM nums) s ORDER BY 1`,
		`SELECT s.* FROM (SELECT id, qty FROM plain) s ORDER BY 2`,
		// No position: a statement with a star is read as before.
		`SELECT * FROM plain ORDER BY id`,
		`SELECT * FROM plain`,
		`SELECT * FROM plain p JOIN nums n ON n.id = p.id WHERE (n.k) > 5`,
		`SELECT * FROM plain p, nums n WHERE n.id = p.id AND n.k BETWEEN 1 AND (5)`,
		`SELECT * FROM (SELECT * FROM plain) s WHERE qty > 1`,
		`SELECT count(*) FROM (SELECT * FROM plain) s GROUP BY (s.qty)`,
		// No star: the other rules'.
		`SELECT id, qty FROM plain ORDER BY 2`,
		`SELECT id, count(*) FROM plain GROUP BY 1 ORDER BY 2`,
		`SELECT id, sum(qty) AS units FROM plain GROUP BY 1 ORDER BY 2 DESC, 1`,
		`SELECT count(*) FROM plain ORDER BY 1`,
		`SELECT id * 2 FROM plain ORDER BY 1`,
		`SELECT id, qty * 2, count(*) FROM plain GROUP BY 1, 2 ORDER BY 1, 2`,
	} {
		if veto := orderedScreen(q); veto != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, veto)
		}
		// The screen that runs before the types are known does not have the
		// columns' order, and says nothing here.
		if toks, lexWhy := lexSQL(RewriteForCopy(q)); lexWhy == "" {
			if why := screenUntyped(toks); why != "" {
				t.Errorf("%s: vetoed without its types (%s)", q, why)
			}
		}
	}
	// A table two schemas share has no one order: not resolved.
	types := map[string][]string{"id": {"INTEGER"}}
	SetColumnOrder(types, "t", []string{"INTEGER"})
	if why := CopyScreen(`SELECT * FROM t ORDER BY 1`, types); why != "" {
		t.Errorf("one table: %q", why)
	}
	SetColumnOrder(types, "t", []string{"VARCHAR"})
	if why := CopyScreen(`SELECT * FROM t ORDER BY 1`, types); why == "" {
		t.Error("a name two schemas share: a position behind a star left to the copy")
	}
	// Without the copy's types nothing is listed: any column may be text.
	if why := CopyScreen(`SELECT * FROM t ORDER BY 1`, nil); why == "" {
		t.Error("no types: a position behind a star left to the copy")
	}
}

// A quoted alias is an alias: the name it gives is no longer the column of
// that name.
func TestCopyScreen_quotedAlias(t *testing.T) {
	for _, q := range []string{
		`SELECT name AS "amount" FROM people ORDER BY amount`,
		`SELECT name AS "amount" FROM people ORDER BY "amount"`,
		`SELECT name "amount" FROM people ORDER BY amount`,
		`SELECT amount AS amount FROM (SELECT name AS "amount" FROM people) s ORDER BY amount`,
		`SELECT min(name) AS "age" FROM people`,
		`SELECT name AS "age" FROM people WHERE id > 0 ORDER BY age DESC`,
	} {
		if why := CopyScreen(q, peopleTypes); why == "" {
			t.Errorf("%s: left to the copy, want the source (text by order under a quoted name)", q)
		}
	}
	for _, q := range []string{
		`SELECT age AS "years", id FROM people ORDER BY years`,
		`SELECT sum(amount) AS "total" FROM people ORDER BY total`,
		`SELECT id AS "key", amount AS "amount" FROM people ORDER BY amount DESC, "key"`,
	} {
		if why := CopyScreen(q, peopleTypes); why != "" {
			t.Errorf("%s:\n  sent to the source (%s), want the copy", q, why)
		}
	}
}

// A name that only may be text is not concatenated as text: it may be a
// float, which the copy writes otherwise.
func TestCopyScreen_maybeTextIsNotConcatenated(t *testing.T) {
	types := map[string][]string{"id": {"INTEGER"}, "name": {"VARCHAR"}, "price": {"DOUBLE"}, "qty": {"INTEGER"}}
	for _, q := range []string{
		`SELECT name || '!' FROM (SELECT "price" AS name FROM items) s`,
		`SELECT name || '!' FROM (SELECT min(DISTINCT price) AS name FROM items) s`,
		`SELECT name || '!' FROM (SELECT public.items.price AS name FROM items) s`,
		`SELECT concat(name, '!') FROM (SELECT "price" AS name FROM items) s`,
	} {
		if why := CopyScreen(q, types); why == "" {
			t.Errorf("%s: left to the copy, want the source", q)
		}
	}
	// The text column itself concatenates as before.
	if why := CopyScreen(`SELECT name || '!' FROM items`, types); why != "" {
		t.Errorf("a text column concatenated: sent to the source (%s)", why)
	}
}

// The mark of a name that may be text is put only where a text column has
// that name and the name stands for a value: a table's alias, and a name
// over columns that are not text, change nothing of the statement.
func TestCopyScreen_noTextFromANameThatIsNoValue(t *testing.T) {
	// A sysbench-like copy: c is a text column, k a whole number.
	types := map[string][]string{"id": {"INTEGER"}, "k": {"INTEGER"}, "c": {"VARCHAR"}, "pad": {"VARCHAR"}, "d": {"DATE"}, "born": {"TIMESTAMP"}}
	for _, q := range []string{
		`SELECT c.id, c.k FROM sbtest1 AS c ORDER BY 1`,
		`SELECT c.id, count(*) AS n FROM sbtest1 AS c GROUP BY 1 ORDER BY 2 DESC NULLS LAST`,
		`SELECT c.id, k.k FROM sbtest1 c, sbtest2 k WHERE k.id = c.id ORDER BY 1`,
		`SELECT born::date AS d, count(*) FROM sbtest1 GROUP BY 1 ORDER BY 1`,
		`SELECT id AS k, k AS id FROM sbtest1 ORDER BY 1, 2`,
		`SELECT s.k FROM (SELECT id AS k FROM sbtest1) s WHERE s.k > 5 ORDER BY s.k`,
	} {
		if why := CopyScreen(q, types); why != "" {
			t.Errorf("%s:\n  sent to the source (%s), want the copy", q, why)
		}
	}
}

// The work of the rule is bounded: a megabyte of ORDER BYs, of keys, of
// nested ORDER BYs and of parentheses that never close answers at once.
func TestStarByPosition_bounded(t *testing.T) {
	tables := starCatalog()
	types := withColumnOrder(screenTypes(tables, nil), tables)
	for name, q := range map[string]string{
		"ORDER BY written over and over": "SELECT * FROM plain " + strings.Repeat("ORDER BY 1 ", (1<<20)/11),
		"a list of keys":                 "SELECT * FROM plain ORDER BY 1" + strings.Repeat(", 1", (1<<20)/3),
		"keys with modifiers":            "SELECT * FROM plain ORDER BY 1" + strings.Repeat(", 5 desc nulls last", (1<<20)/19),
		"nested ORDER BYs":               "SELECT * FROM plain ORDER BY " + strings.Repeat("(SELECT * FROM plain ORDER BY ", (1<<20)/30) + "1",
		"subqueries in FROM":             "SELECT * FROM " + strings.Repeat("(SELECT * FROM ", (1<<20)/15) + "plain ORDER BY 1",
		"open parentheses":               "SELECT * FROM plain ORDER BY " + strings.Repeat("( ", 1<<19),
		"stars":                          "SELECT " + strings.Repeat("*, ", (1<<20)/3) + "1 FROM plain ORDER BY 1",
		"stars over subqueries":          "SELECT * FROM " + strings.Repeat("(SELECT * FROM nums) s, ", (1<<20)/24) + "nums ORDER BY 1",
	} {
		toks, why := lexSQL(q)
		if why != "" {
			t.Fatalf("%s: not read: %s", name, why)
		}
		done := make(chan struct{})
		start := time.Now()
		go func() { starMayBeText(toks, types); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("a megabyte of %s: no answer in 10 s", name)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Errorf("a megabyte of %s took %v", name, took)
		}
	}
}
