package pgshim

import (
	"strings"
	"testing"
)

// The ORDER BY of the main statement may sort text on the copy. What else
// sorts text decides a value or which rows are counted, and stays.
func TestCopyScreen_mainOrderOfText(t *testing.T) {
	types := map[string][]string{
		"id": {"INTEGER"}, "qty": {"INTEGER"}, "name": {"VARCHAR"}, "city": {"VARCHAR"},
	}
	for _, q := range []string{
		"SELECT name FROM t ORDER BY name",
		"SELECT name FROM t ORDER BY name DESC NULLS LAST",
		"SELECT id, name FROM t ORDER BY 2",
		"SELECT name AS n FROM t ORDER BY n",
		"SELECT t.name FROM t ORDER BY t.name, id",
		"SELECT name FROM t ORDER BY name LIMIT 20",
		"SELECT name FROM t ORDER BY name LIMIT 20 OFFSET 40",
		"SELECT name, count(*) FROM t GROUP BY name ORDER BY name",
		"SELECT substr(name, 1, 1), count(*) FROM t GROUP BY 1 ORDER BY 1",
		"SELECT name FROM t UNION ALL SELECT city FROM t ORDER BY 1",
		"WITH s AS (SELECT name FROM t) SELECT name FROM s ORDER BY name",
		"SELECT name FROM (SELECT name FROM t) s ORDER BY name",
	} {
		if veto := CopyScreen(q, types); veto != "" {
			t.Errorf("%s: sent to the source for %s", q, veto)
		}
		// The rule that lets it through is this one and no other.
		if veto := CopyScreenSortingLikeSource(q, types); !strings.Contains(veto, "ORDER BY") {
			t.Errorf("%s: without the main ORDER BY's rule the veto is %q", q, veto)
		}
	}
	for q, says := range map[string]string{
		// A value depends on the order.
		"SELECT id, rank() OVER (ORDER BY name) FROM t":                       "ORDER BY",
		"SELECT id, row_number() OVER (PARTITION BY id ORDER BY name) FROM t": "ORDER BY",
		"SELECT string_agg(city, ',' ORDER BY name) FROM t":                   "ORDER BY",
		"SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY name) FROM t":     "",
		"SELECT min(name) FROM t":                                             "min()",
		"SELECT max(name) FROM t":                                             "max()",
		"SELECT id FROM t WHERE name < 'b'":                                   "by order",
		"SELECT id FROM t ORDER BY name < 'b'":                                "by order",
		// Which rows are kept or counted.
		"SELECT count(*) FROM (SELECT name FROM t ORDER BY name LIMIT 5) s":           "ORDER BY",
		"WITH s AS (SELECT name FROM t ORDER BY name LIMIT 5) SELECT count(*) FROM s": "ORDER BY",
		"SELECT id FROM t WHERE id IN (SELECT id FROM t ORDER BY name LIMIT 5)":       "ORDER BY",
		"SELECT DISTINCT ON (id) id, name FROM t ORDER BY id, name":                   "ORDER BY",
		// COLLATE is how a client asks for PostgreSQL's order.
		`SELECT name FROM t ORDER BY name COLLATE "default"`: "COLLATE",
	} {
		if veto := CopyScreen(q, types); veto == "" || !strings.Contains(veto, says) {
			t.Errorf("%s: veto %q, want it sent to the source (%q)", q, veto, says)
		}
	}
	// With the columns not known any name may be a char(n).
	if veto := CopyScreen("SELECT name FROM t ORDER BY name", nil); veto == "" {
		t.Error("ORDER BY with the columns not known was left to the copy")
	}
}

// A char(n) column sorted stays with the source, by its name, by a position
// or by an alias that may stand for it.
func TestCopyScreen_mainOrderOfACharColumn(t *testing.T) {
	tables := paddedStarCatalog()
	screen := func(q string) string {
		return CopyScreen(q, withColumnOrder(screenTypes(tables, tablesRead(RewriteForCopy(q), tables)), tables))
	}
	for _, q := range []string{
		"SELECT code FROM codes ORDER BY code",
		"SELECT id, code FROM codes ORDER BY 2",
		"SELECT code AS c FROM codes ORDER BY c",
		"SELECT * FROM codes ORDER BY 2",
	} {
		if veto := screen(q); veto == "" {
			t.Errorf("%s: left to the copy", q)
		}
	}
	// Beside a char(n) column, a text column sorted by its own name is the
	// copy's.
	if q := "SELECT id, label FROM codes ORDER BY label"; screen(q) != "" {
		t.Errorf("%s: sent to the source for %s", q, screen(q))
	}
}

// A connection with no source keeps every ORDER BY of text that reaches the
// screen from the copy: the decision was about a connection that can reach
// PostgreSQL.
func TestPaddedForNoSource_keepsTextOrder(t *testing.T) {
	tables := paddedStarCatalog()
	q := "SELECT substr(code, 1, 1) AS k2 FROM codes ORDER BY k2"
	toks, _ := lexSQL(q)
	types := withColumnOrder(screenTypes(tables, tablesRead(q, tables)), tables)
	if veto := paddedForNoSource(toks, types); veto == "" {
		t.Errorf("%s: left to a copy with no source", q)
	}
}

// With nothing but real text and char(n) in the table, the only thing keeping
// a position or an alias over the char(n) column from the copy is the rule for
// a key that hides a column.
func TestCopyScreen_aKeyThatMayBeACharColumn(t *testing.T) {
	tables := []CatalogTable{{Schema: "public", Name: "fixed", Columns: []CatalogColumn{
		{Name: "id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1},
		{Name: "code", Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: 9},
		{Name: "label", Type: "VARCHAR", SourceType: oidText, SourceTypeMod: -1},
	}}}
	screen := func(q string) string {
		return CopyScreen(q, withColumnOrder(screenTypes(tables, tablesRead(RewriteForCopy(q), tables)), tables))
	}
	for _, q := range []string{
		"SELECT id, code FROM fixed ORDER BY 2",
		"SELECT * FROM fixed ORDER BY 2",
		"SELECT code, count(*) FROM fixed GROUP BY code ORDER BY 1",
	} {
		if veto := screen(q); veto == "" {
			t.Errorf("%s: left to the copy", q)
		}
	}
	if q := "SELECT id, label FROM fixed ORDER BY label"; screen(q) != "" {
		t.Errorf("%s: sent to the source for %s", q, screen(q))
	}
}

// What is text only on the copy sorts by its meaning on PostgreSQL: the
// answer would be another one, not another collation.
func TestCopyScreen_mainOrderOfWhatIsTextOnlyOnTheCopy(t *testing.T) {
	const (
		enumOID, domainOID   = 90001, 90002
		oidMoney, oidJSONB   = 790, 3802
		oidNumericT, oidInet = 1700, 869
		oidJSONT             = 114
	)
	tables := []CatalogTable{{Schema: "public", Name: "tickets", Columns: []CatalogColumn{
		{Name: "id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1},
		{Name: "title", Type: "VARCHAR", SourceType: oidText, SourceTypeMod: -1},
		{Name: "prio", Type: "VARCHAR", SourceType: enumOID, SourceTypeMod: -1},
		{Name: "dom", Type: "VARCHAR", SourceType: domainOID, SourceTypeMod: -1},
		{Name: "wide", Type: "VARCHAR", SourceType: oidNumericT, SourceTypeMod: 50<<16 | 2 + 4},
		{Name: "cost", Type: "VARCHAR", SourceType: oidMoney, SourceTypeMod: -1},
		{Name: "addr", Type: "VARCHAR", SourceType: oidInet, SourceTypeMod: -1},
		{Name: "doc", Type: "VARCHAR", SourceType: oidJSONT, SourceTypeMod: -1},
		{Name: "bin", Type: "VARCHAR", SourceType: oidJSONB, SourceTypeMod: -1},
	}}, {Schema: "public", Name: "plain", Columns: []CatalogColumn{
		{Name: "id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1},
		{Name: "prio", Type: "VARCHAR", SourceType: oidText, SourceTypeMod: -1},
		{Name: "label", Type: "VARCHAR", SourceType: oidVarchar, SourceTypeMod: 24},
	}}}
	screen := func(q string) string {
		return CopyScreen(q, withColumnOrder(screenTypes(tables, tablesRead(RewriteForCopy(q), tables)), tables))
	}
	for _, q := range []string{
		"SELECT id FROM tickets ORDER BY prio DESC LIMIT 1",
		"SELECT id FROM tickets t ORDER BY t.prio",
		"SELECT id FROM tickets ORDER BY dom",
		"SELECT id FROM tickets ORDER BY wide DESC LIMIT 1",
		"SELECT id FROM tickets ORDER BY cost",
		"SELECT id FROM tickets ORDER BY addr",
		"SELECT id FROM tickets ORDER BY doc",
		"SELECT id FROM tickets ORDER BY bin",
		// By a position, an alias, an expression.
		"SELECT id, prio FROM tickets ORDER BY 2",
		"SELECT * FROM tickets ORDER BY 3",
		"SELECT prio AS p FROM tickets ORDER BY p",
		"SELECT prio p FROM tickets ORDER BY p",
		"SELECT prio AS title FROM tickets ORDER BY title",
		"SELECT title FROM (SELECT prio AS title FROM tickets) s ORDER BY title",
		"WITH s(title) AS (SELECT prio FROM tickets) SELECT title FROM s ORDER BY title",
		"SELECT title FROM tickets x(a, title) ORDER BY title",
		"SELECT id FROM tickets ORDER BY lower(title)",
		"SELECT id FROM tickets ORDER BY title || prio",
		// The same name, text in a table the join also reads.
		"SELECT p.id FROM plain p JOIN tickets t ON p.id = t.id ORDER BY p.prio",
	} {
		if veto := screen(q); veto == "" {
			t.Errorf("%s: left to the copy", q)
		}
	}
	for _, q := range []string{
		"SELECT id, title FROM tickets ORDER BY title",
		"SELECT t.id FROM tickets t ORDER BY t.title DESC NULLS LAST, id",
		// The enum is in a table this statement does not read.
		"SELECT prio FROM plain ORDER BY prio",
		"SELECT id, label FROM plain ORDER BY 2",
		"SELECT label AS l FROM plain ORDER BY l",
		"SELECT id, label FROM plain ORDER BY substr(label, 1, 1), id",
	} {
		if veto := screen(q); veto != "" {
			t.Errorf("%s: sent to the source for %s", q, veto)
		}
	}
}

// An output PostgreSQL names after a column is what an ORDER BY of that bare
// name sorts there; the copy would sort the text column.
func TestCopyScreen_anOutputNamedAfterTheColumn(t *testing.T) {
	types := map[string][]string{"id": {"INTEGER"}, "dia": {"DATE"}, "fecha": {"VARCHAR"}, "name": {"VARCHAR"}}
	for _, q := range []string{
		"SELECT fecha::date FROM eventos ORDER BY fecha DESC LIMIT 1",
		"SELECT CAST(fecha AS date) FROM eventos ORDER BY fecha DESC LIMIT 1",
		"SELECT e.fecha::date FROM eventos e ORDER BY fecha DESC LIMIT 1",
		"SELECT CAST(e.fecha AS date) FROM eventos e ORDER BY fecha DESC LIMIT 1",
		"SELECT e.fecha::date FROM eventos e ORDER BY e.fecha DESC LIMIT 1",
		"SELECT (fecha)::date FROM eventos ORDER BY fecha",
		"SELECT CAST((fecha) AS date) FROM eventos ORDER BY fecha",
		"SELECT CASE WHEN id > 0 THEN 'z' ELSE fecha END FROM eventos ORDER BY fecha",
		"SELECT CASE WHEN id > 0 THEN 'z' ELSE CASE WHEN id < 0 THEN 'y' ELSE fecha END END FROM eventos ORDER BY fecha",
		"SELECT (SELECT max(id) AS fecha FROM u) FROM eventos ORDER BY fecha",
		"SELECT (SELECT fecha FROM u LIMIT 1) FROM eventos ORDER BY fecha",
	} {
		if veto := CopyScreen(q, types); veto == "" {
			t.Errorf("%s: left to the copy", q)
		}
	}
	for _, q := range []string{
		"SELECT fecha FROM eventos ORDER BY fecha",
		"SELECT dia::date, fecha FROM eventos ORDER BY fecha",
		"SELECT fecha FROM (SELECT fecha, id FROM eventos) s ORDER BY fecha",
	} {
		if veto := CopyScreen(q, types); veto != "" {
			t.Errorf("%s: sent to the source for %s", q, veto)
		}
	}
}

// A statement built to make the walk for inherited names long is cut by its
// budget, counted in steps and not in seconds, and goes to the source.
func TestCopyScreen_outputNamesHaveABudget(t *testing.T) {
	types := map[string][]string{"id": {"INTEGER"}, "name": {"VARCHAR"}}
	const size = 1 << 20
	times := func(_ string, length int) int { return size / length }
	for what, q := range map[string]string{
		"else": "SELECT " + strings.Repeat("else ", times("", 5)) + "name FROM t ORDER BY name",
		"cast": "SELECT " + strings.Repeat("cast(", times("", 14)) + "name" + strings.Repeat(" AS text)", times("", 14)) + " FROM t ORDER BY name",
		"case": "SELECT " + strings.Repeat("case when a then b else ", times("", 28)) + "c" + strings.Repeat(" end", times("", 28)) + " FROM t ORDER BY name",
	} {
		toks, why := lexSQL(q)
		if why != "" {
			t.Fatalf("%s: %s", what, why)
		}
		_, read, steps := outputNames(toks)
		if read {
			t.Errorf("%s: read whole", what)
		}
		if limit := outputNamesLimit(toks); steps > limit {
			t.Errorf("%s: %d steps, over the limit of %d", what, steps, limit)
		}
		r := newMainTextOrder(toks, types, types)
		for i := range toks {
			if r.outer[i] {
				t.Errorf("%s: a key may still be relaxed", what)
				break
			}
		}
		if veto := CopyScreen(q, types); veto == "" {
			t.Errorf("%s: left to the copy", what)
		}
	}
	// A CASE of a normal size is read.
	q := "SELECT CASE WHEN id > 0 THEN 'a' ELSE 'b' END, name FROM t ORDER BY name"
	if veto := CopyScreen(q, types); veto != "" {
		t.Errorf("%s: sent to the source for %s", q, veto)
	}
}

// With the real screen, what is not the main ORDER BY of a plain text key.
func TestCopyScreen_mainOrderStaysOnTheSource(t *testing.T) {
	types := map[string][]string{"id": {"INTEGER"}, "name": {"VARCHAR"}}
	for _, q := range []string{
		"SELECT id FROM t GROUP BY id ORDER BY min(name)",
		"SELECT max(name) AS name FROM t ORDER BY name",
		"SELECT count(*) FROM (SELECT id FROM t ORDER BY name) s",
		"SELECT name FROM t ORDER BY 'name'",
		"SELECT name FROM t ORDER BY ('name') DESC",
		"SELECT name FROM t ORDER BY $1",
	} {
		if veto := CopyScreen(q, types); veto == "" {
			t.Errorf("%s: left to the copy", q)
		}
	}
	tables := paddedStarCatalog()
	q := "SELECT id, code FROM codes ORDER BY 2"
	if CopyScreen(q, withColumnOrder(screenTypes(tables, tablesRead(RewriteForCopy(q), tables)), tables)) == "" {
		t.Errorf("%s: left to the copy", q)
	}
}

// One pass per statement, however many ORDER BYs it holds.
func TestCopyScreen_manyOrderBysCostOnePass(t *testing.T) {
	q := "SELECT " + strings.Repeat("rank() OVER (ORDER BY 1), ", 20000) + "1 FROM t"
	toks, why := lexSQL(q)
	if why != "" {
		t.Fatal(why)
	}
	got := testing.AllocsPerRun(1, func() { newMainTextOrder(toks, map[string][]string{"id": {"INTEGER"}}, nil) })
	if got > 64 {
		t.Errorf("%.0f allocations reading the statement", got)
	}
}
