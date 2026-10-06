package views

import (
	"database/sql"
	"fmt"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// duckWords lists what one query of the linked DuckDB returns, lower case.
func duckWords(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var w string
		if err := rows.Scan(&w); err != nil {
			t.Fatal(err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// bareWord reports whether w can be written into a statement as it is.
func bareWord(w string) bool {
	for i := 0; i < len(w); i++ {
		c := w[i]
		if !(c == '_' || '0' <= c && c <= '9' || 'a' <= c && c <= 'z') {
			return false
		}
	}
	return w != ""
}

// TestDuckDB_typeWordsBeforeAString ties the list read routing refuses
// before a string literal (readrouter.CopyTypeWords, #2131) to the linked
// engine. `text 'Label'` is a constant of type text there, and the column or
// alias called text on MySQL. The words that can be read that way are the
// engine's type names and whatever keyword it takes for one, so every one of
// them has to be either refused or listed as read alike on both sides
// (readrouter.SameTypedLiteral), and nothing else may be in either list. A
// new type in a later engine fails this until it is put in one of the two.
func TestDuckDB_typeWordsBeforeAString(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	types := duckWords(t, db, `SELECT DISTINCT lower(type_name) FROM duckdb_types() ORDER BY 1`)
	keywords := duckWords(t, db, `SELECT keyword_name FROM duckdb_keywords() ORDER BY 1`)
	if len(types) < 50 || len(keywords) < 100 {
		t.Fatalf("DuckDB listed %d types and %d keywords, so this would pass on almost nothing", len(types), len(keywords))
	}
	// A string each kind of type can read: text, a number, a date, a time, a
	// date and time, a truth value, JSON, a UUID, a list, an interval.
	strs := []string{"Label", "1", "2026-01-01", "10:00:00", "2026-01-01 10:00:00", "true", "{}",
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "[1]", "1 day"}
	engine := map[string]bool{}
	for _, w := range types {
		if bareWord(w) {
			engine[w] = true
		}
	}
	for _, w := range keywords {
		if !bareWord(w) || engine[w] {
			continue
		}
		for _, s := range strs {
			var v any
			if db.QueryRow(`SELECT `+w+` '`+s+`'`).Scan(&v) == nil {
				engine[w] = true
				break
			}
		}
	}
	refused, alike := readrouter.CopyTypeWords(), readrouter.SameTypedLiteral()
	for w := range engine {
		if !slices.Contains(refused, w) && !slices.Contains(alike, w) {
			t.Errorf("DuckDB reads %s '...' as a constant of that type, and read routing neither refuses it nor lists it as read alike on MySQL", w)
		}
	}
	for _, w := range slices.Concat(refused, alike) {
		if !engine[w] {
			t.Errorf("%s is listed as a word DuckDB reads as a type before a string, and this DuckDB does not", w)
		}
	}
	// The difference itself, with and without white space, over a column and
	// over an alias of that name: the constant, not the column's value.
	if _, err := db.Exec(`CREATE TABLE tw (id INT, "text" VARCHAR, "json" VARCHAR); INSERT INTO tw VALUES (1, 'body', 'body')`); err != nil {
		t.Fatal(err)
	}
	for stmt, want := range map[string]string{
		`SELECT text 'Label' FROM tw`:                         "Label",
		`SELECT text'Label' FROM tw`:                          "Label",
		`SELECT text 'Label' FROM (SELECT 'body' AS text) t`:  "Label",
		`SELECT CAST(json '1' AS VARCHAR) FROM tw`:            "1",
		`SELECT "text" FROM tw`:                               "body",
		`SELECT CAST(date '2026-01-01' AS VARCHAR) FROM tw`:   "2026-01-01",
		`SELECT CAST(t.text AS VARCHAR) FROM tw t WHERE id=1`: "body",
	} {
		var got string
		if err := db.QueryRow(stmt).Scan(&got); err != nil || got != want {
			t.Errorf("%s: %q, %v; want %q", stmt, got, err, want)
		}
	}
}

// TestDuckDB_copyOnlyReservedWords ties the words read routing keeps on
// MySQL when one is written as a name (readrouter.CopyOnlyReserved, #2158)
// to the linked engine: each is one of the keywords the engine cannot read
// bare (BareKeywords, itself pinned by
// TestDuckDB_keywordsThatCannotBeTypedBare), and the engine does with each
// what the rule assumes. Bare, a column of that name is a syntax error;
// quoted, after a dot, or as a column alias after AS it is read, which is
// why those three spellings are not kept back.
//
// Whether MySQL and MariaDB take a word bare is not something this engine
// can say: the routed fixture of #2131 asks a real server.
func TestDuckDB_copyOnlyReservedWords(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	words := readrouter.CopyOnlyReserved()
	if len(words) < 30 {
		t.Fatalf("%d words listed, so this would pass on almost nothing", len(words))
	}
	if _, err := db.Exec(`CREATE TABLE ja (id INT); INSERT INTO ja VALUES (1), (2); CREATE TABLE jb (id INT); INSERT INTO jb VALUES (1), (1), (3)`); err != nil {
		t.Fatal(err)
	}
	for _, w := range words {
		if !slices.Contains(bareKeywords, w) {
			t.Errorf("%s is listed as a word DuckDB cannot read bare, and it is not one of its keywords of that kind", w)
			continue
		}
		q := quoteIdent(w)
		if _, err := db.Exec(`CREATE OR REPLACE TABLE kw (id INT, ` + q + ` VARCHAR); INSERT INTO kw VALUES (1, 'body')`); err != nil {
			t.Fatalf("%s: %v", w, err)
		}
		var got string
		for _, stmt := range []string{`SELECT ` + w + ` FROM kw`, `SELECT id FROM kw WHERE ` + w + ` >= 'a'`} {
			if err := db.QueryRow(stmt).Scan(&got); err == nil {
				t.Errorf("%s: DuckDB answered %q, and read routing keeps it on MySQL as a statement DuckDB refuses", stmt, got)
			}
		}
		for _, stmt := range []string{
			`SELECT ` + q + ` FROM kw`,
			`SELECT x.` + w + ` FROM kw x WHERE x.` + w + ` >= 'a'`,
			`SELECT q.` + w + ` FROM (SELECT ` + q + ` AS ` + w + ` FROM kw) q`,
		} {
			if err := db.QueryRow(stmt).Scan(&got); err != nil || got != "body" {
				t.Errorf("%s: %q, %v; read routing sends it to DuckDB as a statement it reads", stmt, got, err)
			}
		}
		// A word right after AS is let through, and AS can also name a
		// table. There DuckDB has to refuse the word or read an alias, as
		// MySQL does: a plain join, two rows, and not the join the word
		// spells without AS (FULL: four rows).
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM ja AS ` + w + ` JOIN jb b USING (id)`).Scan(&n); err == nil && n != 2 {
			t.Errorf("FROM ja AS %s JOIN jb: DuckDB counted %d rows, and a plain join has 2; read routing lets a word after AS through", w, n)
		}
	}
	// The two shapes DuckDB answers instead of refusing, with another value
	// than MySQL's: MySQL reads a table alias and a column alias.
	if _, err := db.Exec(`CREATE OR REPLACE TABLE kw (id INT, v VARCHAR); INSERT INTO kw VALUES (1, 'body'), (2, NULL);
		CREATE TABLE k2 (id INT); INSERT INTO k2 VALUES (1), (3)`); err != nil {
		t.Fatal(err)
	}
	// Each with what MySQL 8.4.9 and MariaDB 11.4 answered for it (#2158).
	for _, c := range []struct{ stmt, want, mysql string }{
		{`SELECT count(*) FROM kw full JOIN k2 b USING (id)`, "3", "1"},
		{`SELECT count(*) FROM kw anti JOIN k2 b ON b.id = 1`, "0", "2"},
		{`SELECT count(*) FROM kw asof JOIN k2 b USING (id)`, "2", "1"},
		{`SELECT count(*) FROM kw positional JOIN k2 b`, "2", "4"},
		{`SELECT count(*) FROM kw semi JOIN k2 b ON b.id = 1`, "2", "2"},
		{`SELECT v isnull FROM kw WHERE id = 1`, "false", "body"},
		{`SELECT v notnull FROM kw WHERE id = 1`, "true", "body"},
	} {
		var v any
		err := db.QueryRow(c.stmt).Scan(&v)
		if got := fmt.Sprint(v); err != nil || got != c.want {
			t.Errorf("%s: %q, %v; want %q (MySQL: %s)", c.stmt, got, err, c.want, c.mysql)
		}
		if readrouter.Veto(c.stmt) == "" {
			t.Errorf("%s: DuckDB answers it its own way and read routing does not keep it on MySQL", c.stmt)
		}
	}
}
