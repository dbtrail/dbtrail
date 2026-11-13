package views

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2787: a PostgreSQL array column is read as a list. The expression is run
// on the engine against the text PostgreSQL writes for an array
// (array_out), each case with the list it must give.

// pgListSQL is the view's expression for one array column named c.
func pgListSQL(t *testing.T, as string) string {
	t.Helper()
	parts := pgParts(BaselineTable{Schema: "public", Table: "t", PGCasts: []baseline.PGCast{{Name: "c", As: as}}})
	if len(parts) != 1 {
		t.Fatalf("pgParts gave %d parts", len(parts))
	}
	return parts[0].expr
}

func openDuck(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	return db
}

func TestPGList_readsWhatPostgresWrites(t *testing.T) {
	db := openDuck(t)
	cases := []struct {
		as, text, want string
	}{
		{"VARCHAR[]", `{}`, `[]`},
		{"VARCHAR[]", `{red}`, `["red"]`},
		{"VARCHAR[]", `{red,blue}`, `["red","blue"]`},
		// Quoted: a blank, a comma, braces, a bracket, the empty string.
		{"VARCHAR[]", `{"two words","a,b","a{b}","x]y",""}`, `["two words","a,b","a{b}","x]y",""]`},
		// A quote and a backslash inside quotes are written with a backslash.
		{"VARCHAR[]", `{"a\"b","a\\b","\\\\","a\\","\""}`, `["a\"b","a\\b","\\\\","a\\","\""]`},
		// NULL bare is the missing value; quoted, or in another case, it is text.
		{"VARCHAR[]", `{NULL,"NULL",null,Null,nULL}`, `[null,"NULL","null","Null","nULL"]`},
		// Single quotes are not quoting in PostgreSQL's array text.
		{"VARCHAR[]", `{it's,'q',O'Brien}`, `["it's","'q'","O'Brien"]`},
		{"VARCHAR[]", "{ünï,日本,\"line\nbreak\",\"tab\there\"}", `["ünï","日本","line\nbreak","tab\there"]`},
		// A combining mark right after a quote or a brace is its own
		// character: the cuts count code points.
		{"VARCHAR[]", "{\"\u0301x y\",\u0301}", "[\"\u0301x y\",\"\u0301\"]"},
		{"VARCHAR[]", `{"{","}","{{","[0:1]={1}"}`, `["{","}","{{","[0:1]={1}"]`},
		{"INTEGER[]", `{1,2,NULL,-3}`, `[1,2,null,-3]`},
		{"INTEGER[]", `{}`, `[]`},
		{"BIGINT[]", `{9223372036854775807,-9223372036854775808}`, `[9223372036854775807,-9223372036854775808]`},
		{"SMALLINT[]", `{1,NULL}`, `[1,null]`},
		{"UINTEGER[]", `{4294967295}`, `[4294967295]`},
		{"BOOLEAN[]", `{t,f,NULL}`, `[true,false,null]`},
		{"DOUBLE[]", `{1.5,-2,NULL}`, `[1.5,-2.0,null]`},
		{"FLOAT[]", `{1.5}`, `[1.5]`},
		{"DECIMAL(10,2)[]", `{1.50,-2.25,NULL}`, `[1.5,-2.25,null]`},
		{"DATE[]", `{2026-01-31,NULL}`, `["2026-01-31",null]`},
		{"TIMESTAMP[]", `{"2026-01-01 10:00:00","2026-01-02 11:00:00.5",NULL}`, `["2026-01-01 10:00:00","2026-01-02 11:00:00.5",null]`},
		{"TIMESTAMPTZ[]", `{"2026-01-01 10:00:00+00",NULL}`, `["2026-01-01 10:00:00+00",null]`},
		// char(n) keeps its padding, which PostgreSQL writes between quotes.
		{"VARCHAR[]", `{"ab  ",cd}`, `["ab  ","cd"]`},
		{"UUID[]", `{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}`, `["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]`},
	}
	for _, c := range cases {
		q := "SELECT CAST(to_json(" + pgListSQL(t, c.as) + ") AS VARCHAR) FROM (SELECT ? AS c) t"
		var got sql.NullString
		if err := db.QueryRow(q, c.text).Scan(&got); err != nil {
			t.Errorf("%s %s: %v", c.as, c.text, err)
			continue
		}
		if !sameJSON(got.String, c.want) {
			t.Errorf("%s %s\n got %s\nwant %s", c.as, c.text, got.String, c.want)
		}
	}
}

// sameJSON compares two JSON texts as values, so the engine's spacing does
// not count. Numbers are compared as their text.
func sameJSON(a, b string) bool {
	norm := func(s string) (string, bool) {
		d := json.NewDecoder(strings.NewReader(s))
		d.UseNumber()
		var v any
		if err := d.Decode(&v); err != nil {
			return "", false
		}
		out, err := json.Marshal(v)
		return string(out), err == nil
	}
	x, ok1 := norm(a)
	y, ok2 := norm(b)
	return ok1 && ok2 && x == y
}

// NaN and the infinities of a float are elements like any other.
func TestPGList_floatsThatAreNotNumbers(t *testing.T) {
	db := openDuck(t)
	for _, as := range []string{"DOUBLE[]", "FLOAT[]"} {
		var got string
		q := "SELECT CAST(list_transform(" + pgListSQL(t, as) + ", lambda v: CAST(v AS VARCHAR)) AS VARCHAR) FROM (SELECT '{NaN,Infinity,-Infinity,1.5}' AS c) t"
		if err := db.QueryRow(q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", as, err)
		}
		if got != "[nan, inf, -inf, 1.5]" {
			t.Errorf("%s: %s", as, got)
		}
	}
}

// A NULL array is NULL, not an empty list, and the element type is the
// column's even then.
func TestPGList_nullArrayAndItsType(t *testing.T) {
	db := openDuck(t)
	var isNull bool
	var typ string
	q := "SELECT x IS NULL, typeof(x) FROM (SELECT " + pgListSQL(t, "DECIMAL(10,2)[]") + " AS x FROM (SELECT CAST(NULL AS VARCHAR) AS c) t) u"
	if err := db.QueryRow(q).Scan(&isNull, &typ); err != nil {
		t.Fatal(err)
	}
	if !isNull || typ != "DECIMAL(10,2)[]" {
		t.Errorf("NULL array: is null %v, type %s", isNull, typ)
	}
}

// The folding collation the MySQL session sets must not turn the text
// "null" into a missing element, nor change what the list holds.
func TestPGList_underAFoldingCollation(t *testing.T) {
	db := openDuck(t)
	if _, err := db.Exec("SET default_collation = 'nocase'"); err != nil {
		t.Fatal(err)
	}
	var got string
	q := "SELECT CAST(to_json(" + pgListSQL(t, "VARCHAR[]") + ") AS VARCHAR) FROM (SELECT ? AS c) t"
	if err := db.QueryRow(q, `{null,NULL,Red}`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !sameJSON(got, `["null",null,"Red"]`) {
		t.Errorf("got %s", got)
	}
	// What is NOT kept under such a session: an element compares as the
	// session compares text, since a list carries no collation of its own.
	// The SQL card of a PostgreSQL server does not run under one (#2786);
	// docs/postgres.md says so for a DuckDB of the reader's own.
	var folded bool
	if err := db.QueryRow("SELECT x[1] = 'RED' FROM (SELECT " + pgListSQL(t, "VARCHAR[]") + " AS x FROM (SELECT '{red}' AS c) t) u").Scan(&folded); err != nil {
		t.Fatal(err)
	}
	if !folded {
		t.Error("an element no longer folds under nocase: docs/postgres.md says it does")
	}
}

// What a list of one dimension cannot hold fails the statement, with the
// column named; it is never read as something else.
func TestPGList_refusesWhatItCannotHold(t *testing.T) {
	db := openDuck(t)
	cases := []struct {
		as, text, says string
	}{
		{"INTEGER[]", `{{1,2},{3,4}}`, "more than one dimension"},
		{"VARCHAR[]", `{{a,b},{c,d}}`, "more than one dimension"},
		{"INTEGER[]", `[0:1]={1,2}`, "bounds of its own"},
		{"VARCHAR[]", `[0:1]={a,b}`, "bounds of its own"},
		// Text cut short, or with something after the array.
		{"INTEGER[]", `{1,2`, "not an array's"},
		{"VARCHAR[]", `{a,b}xyz`, "not an array's"},
		{"VARCHAR[]", `{`, "not an array's"},
		{"VARCHAR[]", ``, "not an array's"},
		{"DATE[]", `{"0044-03-15 BC"}`, "before the common era"},
		{"TIMESTAMP[]", `{"0044-03-15 00:00:00 BC",NULL}`, "before the common era"},
		{"TIMESTAMPTZ[]", `{"0044-03-15 00:00:00+00 BC"}`, "before the common era"},
		// The engine's own refusal: an element that is not of the type.
		{"INTEGER[]", `{1,x}`, "Could not convert"},
		{"INTEGER[]", `{99999999999}`, ""},
		{"DECIMAL(4,2)[]", `{123.456}`, ""},
	}
	for _, c := range cases {
		var got sql.NullString
		err := db.QueryRow("SELECT CAST("+pgListSQL(t, c.as)+" AS VARCHAR) FROM (SELECT ? AS c) t", c.text).Scan(&got)
		if err == nil {
			t.Errorf("%s %s was read as %q", c.as, c.text, got.String)
			continue
		}
		if !strings.Contains(err.Error(), c.says) {
			t.Errorf("%s %s: %v, want it to say %q", c.as, c.text, err, c.says)
		}
		if c.says != "" && !strings.HasPrefix(c.says, "Could") && !strings.Contains(err.Error(), "public.t: column c") {
			t.Errorf("%s %s: the refusal does not name the column: %v", c.as, c.text, err)
		}
	}
}

// What a statement does with the list: an element by its position, counted
// from 1 as on PostgreSQL, ANY, the length, and one row per element.
func TestPGList_whatAStatementDoesWithIt(t *testing.T) {
	db := openDuck(t)
	expr := pgListSQL(t, "VARCHAR[]")
	if _, err := db.Exec("CREATE TABLE src AS SELECT * FROM (VALUES (1, '{red,blue}'), (2, '{}'), (3, NULL), (4, '{green,NULL,red}')) v(id, c)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE VIEW t AS SELECT id, " + expr + " AS labels FROM src"); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][2]string{
		{"SELECT string_agg(coalesce(labels[1], '-'), ',' ORDER BY id) FROM t", "red,-,-,green"},
		{"SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM t WHERE 'red' = ANY(labels)", "1,4"},
		{"SELECT string_agg(coalesce(len(labels)::VARCHAR, 'null'), ',' ORDER BY id) FROM t", "2,0,null,3"},
		{"SELECT count(*)::VARCHAR FROM (SELECT unnest(labels) FROM t) u", "5"},
		{"SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM t WHERE labels IS NULL", "3"},
	} {
		q, want := c[0], c[1]
		var got string
		if err := db.QueryRow(q).Scan(&got); err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		if got != want {
			t.Errorf("%s = %q, want %q", q, got, want)
		}
	}
}

// The view's select list carries the expression, and the column is no longer
// named as left as text.
func TestPGList_inTheSelectList(t *testing.T) {
	tbl := BaselineTable{Schema: "public", Table: "orders", Path: "/snap/public/orders.parquet", Rel: "public/orders.parquet"}
	in := Input{Baselines: []BaselineTable{tbl}}
	in.ApplyFooters(map[string]baseline.TableFooter{tbl.Path: {
		Columns: []string{"id", "labels", "blobs"},
		PG:      []baseline.PGCast{{Name: "id", As: "BIGINT"}, {Name: "labels", As: "VARCHAR[]"}, {Name: "blobs"}},
	}})
	tbl = in.Baselines[0]
	got := selectList(tbl)
	if !strings.Contains(got, `AS VARCHAR[]) END AS "labels"`) || !strings.Contains(got, `regexp_extract_all("labels"[2:-2]`) {
		t.Errorf("select list: %s", got)
	}
	notes := decimalComments(tbl, true)
	if len(notes) != 1 || !strings.HasPrefix(notes[0], "blobs (left as text") {
		t.Errorf("notes = %q, want only blobs left as text", notes)
	}
}
