package pgshim

import (
	"strings"
	"testing"
)

// #3054: the capture gate's two halves that do not need a source, case by
// case. What the source itself confirms (proofSQL) is tested against a real
// PostgreSQL in the integration test.

func TestCaptureTextRefusal(t *testing.T) {
	path := []string{"$user", "public"}
	for _, c := range []struct {
		name, q string
		path    []string
		refs    []string // as written, when it passes
		why     string   // a fragment of the refusal, "" when it passes
	}{
		{name: "a plain read", q: "SELECT id, total FROM orders WHERE id = 1", refs: []string{"orders"}},
		{name: "with its schema", q: "SELECT * FROM public.orders", refs: []string{"public.orders"}},
		{name: "quoted, as written", q: `SELECT * FROM "Public"."Orders"`, refs: []string{`"Public"."Orders"`}},
		{name: "a join", q: "SELECT * FROM orders o JOIN customers c ON c.id = o.customer_id", refs: []string{"orders", "customers"}},
		{name: "a subquery reaching another table", q: "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM secret.vip)", refs: []string{"orders", "secret.vip"}},
		{name: "a derived table", q: "SELECT * FROM (SELECT * FROM pg_catalog.pg_authid) a", refs: []string{"pg_catalog.pg_authid"}},
		{name: "a WITH over another table", q: "WITH v AS (SELECT * FROM secret.vip) SELECT * FROM v", refs: []string{"secret.vip", "v"}},
		{name: "no table at all", q: "SELECT 1", refs: nil},
		{name: "the system catalog, unqualified (the source resolves it)", q: "SELECT rolpassword FROM pg_authid", refs: []string{"pg_authid"}},

		{name: "a function that runs a query", q: "SELECT query_to_xml('select * from secret.vip', true, true, '')", why: "the function query_to_xml()"},
		{name: "a function that reads a file", q: "SELECT pg_read_file('/etc/passwd')", why: "the function pg_read_file()"},
		{name: "a sequence", q: "SELECT nextval('s')", why: "the function nextval()"},
		{name: "a function in FROM", q: "SELECT * FROM generate_series(1, 3)", why: "a function in FROM"},
		{name: "a function written with its schema", q: "SELECT public.f(1)", why: "a function written with its schema"},
		{name: "a database in the name", q: "SELECT * FROM shop.public.orders", why: "names a table with its database"},
		{name: "two statements", q: "SELECT 1; SELECT * FROM secret.vip", why: "more than one statement"},
		{name: "SELECT INTO", q: "SELECT * INTO t2 FROM orders", why: "INTO"},
		{name: "a locking read", q: "SELECT * FROM orders FOR UPDATE", why: "(UPDATE)"},
		{name: "a locking read, FOR SHARE", q: "SELECT * FROM orders for share", why: "a locking read"},
		{name: "a locking read, FOR NO KEY UPDATE", q: "SELECT * FROM orders FOR NO KEY UPDATE", why: "(UPDATE)"},
		{name: "a locking read, FOR KEY SHARE", q: "SELECT * FROM orders FOR KEY SHARE", why: "a locking read"},
		{name: "TABLE", q: "TABLE orders", why: "not a SELECT"},
		{name: "a write", q: "DELETE FROM orders", why: "not a SELECT"},
		{name: "a data-modifying WITH", q: "WITH d AS (DELETE FROM orders RETURNING *) SELECT * FROM d", why: "a WITH whose body is not a SELECT"},
		{name: "a cursor", q: "DECLARE c CURSOR FOR SELECT * FROM orders", why: "not a SELECT"},
		{name: "COPY", q: "COPY orders TO STDOUT", why: "not a SELECT"},
		{name: "LATERAL", q: "SELECT * FROM orders, LATERAL (SELECT * FROM secret.vip) v", why: "LATERAL"},
		{name: "ONLY", q: "SELECT * FROM ONLY orders", why: "ONLY"},
		{name: "an empty statement", q: "  ;", why: "an empty statement"},

		{name: "a server setting", q: "SELECT current_setting('archive_command')", why: "calls current_setting()"},
		{name: "a sleep that holds the source", q: "SELECT pg_sleep(600)", why: "calls pg_sleep()"},
		{name: "a cast that looks a role up", q: "SELECT 'postgres'::regrole", why: "casts to regrole"},
		{name: "a cast that looks a table up", q: "SELECT 'secret.vip'::regclass", why: "casts to regclass"},
		{name: "search_path naming pg_catalog", q: "SELECT * FROM orders", path: []string{"public", "pg_catalog"}, why: "search_path names pg_catalog"},
		{name: "search_path naming pg_temp", q: "SELECT * FROM orders", path: []string{"pg_temp", "public"}, why: "search_path names pg_temp"},
		{name: "search_path naming another session's temporary schema", q: "SELECT * FROM orders", path: []string{"pg_temp_3"}, why: "search_path names pg_temp_3"},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := c.path
			if p == nil {
				p = path
			}
			reads, why := captureTextRefusal(c.q, p)
			if c.why != "" {
				if why == "" || !strings.Contains(why, c.why) {
					t.Fatalf("refusal %q, want one about %q", why, c.why)
				}
				return
			}
			if why != "" {
				t.Fatalf("refused: %s", why)
			}
			var got []string
			for _, r := range reads.refs {
				got = append(got, r.written())
			}
			if strings.Join(got, "|") != strings.Join(c.refs, "|") {
				t.Errorf("names %q, want %q", got, c.refs)
			}
		})
	}
}

func TestCaptureTablesRefusal(t *testing.T) {
	copyTables := []CatalogTable{{Schema: "public", Name: "orders"}, {Schema: "public", Name: "customers"}, {Schema: "Sales", Name: "Q1"}}
	const notHeld, system = "this statement reads a table the copy does not hold", "this statement reads a table of the system catalog"
	for _, c := range []struct {
		name   string
		proven [][2]string
		copy   []CatalogTable
		want   string
	}{
		{"a table of the copy", [][2]string{{"public", "orders"}}, copyTables, ""},
		{"two tables of the copy", [][2]string{{"public", "orders"}, {"public", "customers"}}, copyTables, ""},
		{"no table", nil, copyTables, ""},
		{"a name with capitals, exactly", [][2]string{{"Sales", "Q1"}}, copyTables, ""},
		{"a schema not captured", [][2]string{{"secret", "vip"}}, copyTables, notHeld},
		{"a table of the copy joined with one it does not hold", [][2]string{{"public", "orders"}, {"secret", "vip"}}, copyTables, notHeld},
		{"another case of a table of the copy", [][2]string{{"public", "Orders"}}, copyTables, notHeld},
		{"another case of a schema of the copy", [][2]string{{"PUBLIC", "orders"}}, copyTables, notHeld},
		{"a table dropped from the copy", [][2]string{{"public", "orders"}}, nil, notHeld},
		{"the password hashes", [][2]string{{"pg_catalog", "pg_authid"}}, copyTables, system},
		{"information_schema", [][2]string{{"information_schema", "sql_features"}}, copyTables, system},
		{"pg_toast", [][2]string{{"pg_toast", "pg_toast_1"}}, copyTables, system},
		{"another pg_ schema", [][2]string{{"pg_temp_3", "t"}}, copyTables, system},
		// A copy that somehow lists a system table does not make it one of
		// its own.
		{"the system catalog listed by the copy", [][2]string{{"pg_catalog", "pg_authid"}}, []CatalogTable{{Schema: "pg_catalog", Name: "pg_authid"}}, system},
	} {
		if got := captureTablesRefusal(c.proven, c.copy); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// The words the client gets, whole.
func TestCaptureRefusedText(t *testing.T) {
	got := captureRefused("this statement reads a table the copy does not hold")
	want := &pgErr{"0A000", "this server forwards to its source with the account DBTrail captures with, so the port sends there only a SELECT whose every table is a table of the copy, and this statement reads a table the copy does not hold; nothing was sent to the source"}
	if *got != *want {
		t.Errorf("got  %+v\nwant %+v", *got, *want)
	}
	t.Logf("client sees: ERROR %s %s", got.code, got.msg)
}
