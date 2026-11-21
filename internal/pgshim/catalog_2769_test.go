package pgshim

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The system catalog (#2769): the statements the tools were seen sending
// (testdata/catalog_2769.sql) are each recognized, answered from the
// catalog the binding gives, and never sent to the copy; any other catalog
// statement is refused by name.

func capturedCatalogStatements(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile("testdata/catalog_2769.sql")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for i, block := range strings.Split(string(b), "\n====\n") {
		if i == 0 {
			continue // the header
		}
		if s := strings.TrimSpace(block); s != "" {
			out = append(out, s)
		}
	}
	if len(out) < 40 {
		t.Fatalf("only %d captured statements", len(out))
	}
	return out
}

func TestCatalog_everyCapturedStatementIsRecognized(t *testing.T) {
	for _, q := range capturedCatalogStatements(t) {
		canon := catalogCanon(q)
		st, m := matchCatalog(canon)
		if st == nil {
			t.Errorf("not recognized:\n%s", canon)
			continue
		}
		// The parts it reads answer without a refusal.
		v := testCatalogView()
		if _, perr := st.rows(v, m); perr != nil {
			t.Errorf("%s refused %q: %s", st.name, canon, perr.msg)
		}
	}
}

func TestCatalogCanon(t *testing.T) {
	for in, want := range map[string]string{
		"SELECT  a\n\t FROM b ;":                  "select a from b",
		"SELECT 'Mixed Case' , \"Quoted\" FROM X": "select 'Mixed Case',\"Quoted\" from x",
		"select ( select 1 ) -- trailing\n":       "select (select 1)",
		"select /* a /* nested */ comment */ 1":   "select 1",
		"SELECT E'\\n' AS x, 'it''s'":             "select e'\\n' as x,'it''s'",
		"select f(a ,b )":                         "select f(a,b)",
		"select f(a, b)":                          "select f(a,b)",
	} {
		if got := catalogCanon(in); got != want {
			t.Errorf("catalogCanon(%q) = %q, want %q", in, got, want)
		}
	}
	// A statement broken across lines otherwise is the same statement.
	a := `SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE (pg_catalog.pg_table_is_visible(c.oid)) AND c.relname = 'orders' AND c.relkind in ('r', 'v', 'm', 'f', 'p')`
	b := "SELECT c.oid\n  FROM pg_catalog.pg_class c\n  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n WHERE ( pg_catalog.pg_table_is_visible(c.oid) )\n   AND c.relname = 'orders'\n   AND c.relkind in ('r', 'v', 'm', 'f', 'p');"
	if catalogCanon(a) != catalogCanon(b) {
		t.Errorf("\n%q\n%q", catalogCanon(a), catalogCanon(b))
	}
}

func TestReadsCatalog(t *testing.T) {
	for q, want := range map[string]bool{
		"select * from pg_catalog.pg_class":              true,
		"select count(*) from information_schema.tables": true,
		"select relname from pg_class":                   true,
		"select * from x join pg_namespace n on true":    true,
		"select 'pg_class' as x":                         false,
		"select * from my_pg_class":                      false,
		"select * from orders":                           false,
		"select pg_class_id from t":                      false,
		// A quoted name reads as the name: a column called "pg_class" is
		// refused with the catalog, the safe side.
		`select "pg_class" from t`:                     true,
		"select * from s.pg_class":                     false,
		`SELECT * FROM "pg_catalog"."pg_class"`:        true,
		`SELECT * FROM "information_schema"."columns"`: true,
		`SELECT e'\'', * FROM pg_catalog.pg_class`:     true,
		"SELECT $$'$$, * FROM pg_class":                true,
		"SELECT $t$pg_class$t$ AS x":                   false,
		"select * from pg_prepared_statements":         true,
		"select format_type(23, -1)":                   true,
		"select current_schemas(true)":                 true,
		"-- pg_class\nselect 1":                        false,
	} {
		if got := readsCatalog(q); got != want {
			t.Errorf("readsCatalog(%q) = %v, want %v", q, got, want)
		}
	}
}

func TestLikeRegexp(t *testing.T) {
	for _, c := range []struct {
		pattern, s string
		want       bool
	}{
		{"%", "", true}, {"%", "anything", true}, {"c2769%", "c2769_orders", true}, {"c2769%", "x_c2769", false},
		{"a_c", "abc", true}, {"a_c", "ac", false}, {`a\_c`, "a_c", true}, {`a\_c`, "abc", false},
		{"a.c", "abc", false}, {"public", "public", true}, {"public", "Public", false},
	} {
		re, perr := likeRegexp(c.pattern)
		if perr != nil {
			t.Fatal(perr.msg)
		}
		if got := re.MatchString(c.s); got != c.want {
			t.Errorf("%q LIKE %q = %v, want %v", c.s, c.pattern, got, c.want)
		}
	}
}

func TestRelFilterOf(t *testing.T) {
	v := testCatalogView()
	names := func(w string) string {
		f, perr := relFilterOf(catalogCanon(w))
		if perr != nil {
			return "refused"
		}
		var out []string
		for i := range v.rels {
			if r := &v.rels[i]; f.keep(v, r) {
				out = append(out, r.schema+"."+r.name)
			}
		}
		return strings.Join(out, " ")
	}
	for w, want := range map[string]string{
		"c.relkind IN ('r','p','') AND n.nspname <> 'pg_catalog' AND n.nspname !~ '^pg_toast' AND n.nspname <> 'information_schema' AND pg_catalog.pg_table_is_visible(c.oid)": "public.orders",
		"c.relkind IN ('r','p','t','s','') AND n.nspname OPERATOR(pg_catalog.~) '^(sales)$' COLLATE pg_catalog.default":                                                        "sales.items",
		"c.relname OPERATOR(pg_catalog.~) '^(ord.*)$' COLLATE pg_catalog.default AND pg_catalog.pg_table_is_visible(c.oid)":                                                    "public.orders public.orders_pkey",
		"c.relkind IN ('i','')":                         "public.orders_pkey sales.items_pkey",
		"n.nspname LIKE 'sales' AND c.relname LIKE '%'": "sales.items sales.items_pkey",
		"c.relname LIKE '%' AND (false OR ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ) OR ( c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' ) )": "public.orders sales.items",
		// A system table's type names no table of the copy.
		"c.relname LIKE '%' AND (false OR ( c.relkind = 'r' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ) )": "",
		"(n.nspname = 'sales') AND c.relname = 'items' AND c.relkind in ('r', 'v', 'm', 'f', 'p')":                                  "sales.items",
		"c.relname = 'orders' AND c.relhasrules":        "refused",
		"pg_catalog.pg_table_is_visible(c.oid) OR true": "refused",
	} {
		if got := names(w); got != want {
			t.Errorf("%s\n got %q\nwant %q", w, got, want)
		}
	}
}

// testCatalogView is a catalog of two tables in two schemas, the session's
// schema public.
func testCatalogView() *catalogView { return testCatalogViewOf(testCatalog()) }

// testCatalogViewOf is the catalog of these tables for the user app, whose
// schema is public, in the database orders.
func testCatalogViewOf(tables []CatalogTable) *catalogView {
	s := &session{user: "app"}
	s.initSettings("public", "app")
	s.reported = map[string]string{}
	s.catalogTables = tables
	s.catalogAt = time.Now()
	s.database = "orders"
	s.bound = &Binding{Catalog: func(context.Context) ([]CatalogTable, error) { return tables, nil }}
	v, _ := s.catalog()
	return v
}

func testCatalog() []CatalogTable {
	return []CatalogTable{
		{Schema: "public", Name: "orders", OID: 16385, PrimaryKey: []string{"id"}, Columns: []CatalogColumn{
			{Name: "id", Type: "INTEGER", NotNull: true}, {Name: "amount", Type: "DECIMAL(10,2)"}, {Name: "note", Type: "VARCHAR", Default: "'x'::text"},
			{Name: "at", Type: "TIMESTAMP WITH TIME ZONE"}, {Name: "flags", Type: "BOOLEAN[]"}, {Name: "iv", Type: "INTERVAL"}}},
		{Schema: "sales", Name: "items", OID: 16390, PrimaryKey: []string{"sku", "line"}, Columns: []CatalogColumn{
			{Name: "sku", Type: "VARCHAR", NotNull: true}, {Name: "line", Type: "INTEGER", NotNull: true}}},
	}
}

// catalogLogin binds every connection to the test catalog and an executor
// that records what reached it.
type catalogLogin struct {
	mu   sync.Mutex
	ran  []string
	read int
	// tables are the catalog's, testCatalog's when nil.
	tables []CatalogTable
}

func (l *catalogLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	return &Binding{Schema: "public", Actor: "server:t",
		Run: func(_ context.Context, st string, _ RunOptions) (sqlsandbox.Result, error) {
			l.mu.Lock()
			l.ran = append(l.ran, st)
			l.mu.Unlock()
			return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "x", Type: "INTEGER"}}, Rows: [][]any{{int32(1)}}}, nil
		},
		Catalog: func(context.Context) ([]CatalogTable, error) {
			l.mu.Lock()
			l.read++
			l.mu.Unlock()
			if l.tables != nil {
				return l.tables, nil
			}
			return testCatalog(), nil
		}}, nil
}

func TestCatalog_overTheWire(t *testing.T) {
	l := &catalogLogin{}
	addr := serveAddrCfg(t, Config{Login: l.login})
	r, err := dialStatus(t, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	// psql's \dt.
	want(t, `\dt`, r.query(`SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;`),
		"Columns Schema:19,Name:19,Type:25,Owner:19", "DataRow public|orders|table|app", "CommandComplete SELECT 1", "ReadyForQuery I")
	// SQLAlchemy's columns, with the types the port announces in a result.
	got := r.query(`SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), ( SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef ) AS DEFAULT, a.attnotnull, a.attrelid as table_oid, pgd.description as comment, a.attgenerated as generated, (SELECT json_build_object( 'always', a.attidentity = 'a', 'start', s.seqstart, 'increment', s.seqincrement, 'minvalue', s.seqmin, 'maxvalue', s.seqmax, 'cache', s.seqcache, 'cycle', s.seqcycle) FROM pg_catalog.pg_sequence s JOIN pg_catalog.pg_class c on s.seqrelid = c."oid" WHERE c.relkind = 'S' AND a.attidentity != '' AND s.seqrelid = pg_catalog.pg_get_serial_sequence( a.attrelid::regclass::text, a.attname )::regclass::oid ) as identity_options FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_description pgd ON ( pgd.objoid = a.attrelid AND pgd.objsubid = a.attnum) WHERE a.attrelid = 16385 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`)
	want(t, "SQLAlchemy columns", got,
		"Columns attname:19,format_type:25,default:25,attnotnull:16,table_oid:26,comment:25,generated:18,identity_options:114",
		"DataRow id|integer|NULL|t|16385|NULL||NULL",
		"DataRow amount|numeric(10,2)|NULL|f|16385|NULL||NULL",
		"DataRow note|text|'x'::text|f|16385|NULL||NULL",
		"DataRow at|timestamp with time zone|NULL|f|16385|NULL||NULL",
		"DataRow flags|boolean[]|NULL|f|16385|NULL||NULL",
		"DataRow iv|text|NULL|f|16385|NULL||NULL",
		"CommandComplete SELECT 6", "ReadyForQuery I")
	// Refused by name, never empty.
	got = r.query("SELECT * FROM pg_catalog.pg_class")
	if !strings.HasPrefix(got[0], "Error 0A000") || !strings.Contains(got[0], "system catalog") {
		t.Fatalf("an unrecognized catalog statement: %q", got)
	}
	l.mu.Lock()
	ran, read := l.ran, l.read
	l.mu.Unlock()
	if len(ran) != 0 {
		t.Fatalf("a catalog statement reached the copy: %q", ran)
	}
	if read != 1 {
		t.Fatalf("the catalog was read %d times for statements a second apart, want once", read)
	}
}

// pgx describes a catalog statement before it runs it, and reads its
// answer in binary where it asks.
func TestCatalog_extendedProtocol(t *testing.T) {
	for name, mode := range extendedModes {
		t.Run(name, func(t *testing.T) {
			l := &catalogLogin{}
			addr := serveAddrCfg(t, Config{Login: l.login})
			cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://app:%s@%s/orders?sslmode=disable", portPassword, addr))
			if err != nil {
				t.Fatal(err)
			}
			cfg.DefaultQueryExecMode = mode
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(context.Background())
			rows, err := conn.Query(ctx, "SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE (n.nspname = $1) AND c.relname = $2 AND c.relkind in ('r', 'v', 'm', 'f', 'p')", "sales", "items")
			if err != nil {
				t.Fatal(err)
			}
			var oids []uint32
			for rows.Next() {
				var o uint32
				if err := rows.Scan(&o); err != nil {
					t.Fatal(err)
				}
				oids = append(oids, o)
			}
			if rows.Err() != nil || len(oids) != 1 || oids[0] != 16390 {
				t.Fatalf("oids %v %v", oids, rows.Err())
			}
			var kind byte
			var has bool
			if err := conn.QueryRow(ctx, `SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid) WHERE c.oid = $1`, "16390").
				Scan(nil, &kind, &has, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil); err != nil || kind != 'r' || !has {
				t.Fatalf("relkind %q relhasindex %v: %v", kind, has, err)
			}
		})
	}
}

// What a raw client describes is what the statement answers.
func TestCatalog_describe(t *testing.T) {
	l := &catalogLogin{}
	r := dialRaw(t, l.login)
	want(t, "describe", r.send(&pgproto3.Parse{Query: "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' ORDER BY nspname"}, &pgproto3.Describe{ObjectType: 'S'}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"ParseComplete", "ParameterDescription []", "RowDescription nspname/0", "BindComplete", "DataRow public", "DataRow sales", "CommandComplete SELECT 2", "ReadyForQuery I")
}

func TestCatalog_refusesWhatItCannotRead(t *testing.T) {
	v := testCatalogView()
	for _, w := range []string{
		// A user type JDBC asks for with a condition this does not read.
		"c.relname LIKE '%' AND (false OR ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND c.relhasrules ) )",
		"c.relname LIKE '%' AND (false OR ( n.nspname !~ '^pg_' ) )",
		`c.relname LIKE 'a\'`,
	} {
		if _, perr := relFilterOf(catalogCanon(w)); perr == nil {
			t.Errorf("%s: read", w)
		}
	}
	// Two conditions on one name both hold.
	f, perr := relFilterOf(catalogCanon("c.relname OPERATOR(pg_catalog.~) '^(ord.*)$' COLLATE pg_catalog.default AND c.relname LIKE '%pkey'"))
	if perr != nil {
		t.Fatal(perr.msg)
	}
	var kept []string
	for i := range v.rels {
		if f.keep(v, &v.rels[i]) {
			kept = append(kept, v.rels[i].name)
		}
	}
	if strings.Join(kept, " ") != "orders_pkey" {
		t.Errorf("kept %v", kept)
	}
	// \d of an index is refused, not "not found".
	st, m := matchCatalog(catalogCanon(`SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname OPERATOR(pg_catalog.~) '^(orders_pkey)$' COLLATE pg_catalog.default AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3`))
	if st == nil {
		t.Fatal("not recognized")
	}
	if _, perr := st.rows(v, m); perr == nil {
		t.Error("\\d of an index answered")
	}
	for in, want := range map[string]string{"orders": "orders", "Sales.Items": "sales items", `"my.table"`: "my.table", `s."T"`: "s T"} {
		parts, ok := parseIdent(in)
		if !ok || strings.Join(parts, " ") != want {
			t.Errorf("parseIdent(%q) = %v %v", in, parts, ok)
		}
	}
}
