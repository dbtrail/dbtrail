package pgshim

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A char(n) column is the source's wherever its padding counts (#3068).

// paddedCatalog is a copy with a table that holds char(n) columns (codes), a
// table of plain text one of whose columns shares a name with a char(n) one
// (plain), and a table whose snapshot records no types (legacy).
func paddedCatalog() []CatalogTable {
	col := func(name, typ string, oid uint32, mod int32) CatalogColumn {
		return CatalogColumn{Name: name, Type: typ, SourceType: oid, SourceTypeMod: mod}
	}
	return []CatalogTable{
		{Schema: "public", Name: "codes", Columns: []CatalogColumn{
			col("id", "INTEGER", oidInt4, -1),
			col("code", "VARCHAR", oidBpchar, 9),        // char(5)
			col("label", "VARCHAR", oidText, -1),        // text
			col("tags", "VARCHAR[]", oidBpcharArray, 7), // char(3)[]
			col("free", "VARCHAR", oidBpchar, -1),       // bpchar, no length
			col("one", "VARCHAR", oidChar, -1),          // "char", one byte
			col("kind", "VARCHAR", 16400, -1),           // a type of the user's (an enum)
			col("k", "INTEGER", oidInt4, -1),
		}},
		{Schema: "public", Name: "plain", Columns: []CatalogColumn{
			col("id", "INTEGER", oidInt4, -1),
			col("code", "VARCHAR", oidText, -1),
			col("name", "VARCHAR", oidVarchar, 24),
			col("status", "VARCHAR", oidText, -1),
			col("qty", "INTEGER", oidInt4, -1),
		}},
		{Schema: "public", Name: "legacy", Columns: []CatalogColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "word", Type: "VARCHAR"},
		}},
	}
}

// paddedScreen is the screen's decision for stmt on a connection whose copy
// is paddedCatalog, as routeFree makes it.
func paddedScreen(stmt string) string { return CopyScreenFor(stmt, paddedCatalog()) }

// isPaddedVeto reports whether a veto is paddedUse's.
func isPaddedVeto(veto string) bool {
	return strings.Contains(veto, "char(n) column") || strings.Contains(veto, "bpchar")
}

func TestPadded_sourceMark(t *testing.T) {
	for _, tc := range []struct {
		col  CatalogColumn
		want string
	}{
		{CatalogColumn{Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: 9}, charNType},
		{CatalogColumn{Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: -1}, bpcharType},
		{CatalogColumn{Type: "VARCHAR[]", SourceType: oidBpcharArray, SourceTypeMod: 7}, charNType + "[]"},
		{CatalogColumn{Type: "VARCHAR[]", SourceType: oidBpcharArray, SourceTypeMod: -1}, bpcharType + "[]"},
		// A table read as text throughout keeps an array as text.
		{CatalogColumn{Type: "VARCHAR", SourceType: oidBpcharArray, SourceTypeMod: 7}, charNType + "[]"},
		// Not padded: text, varchar(n), "char" (one byte), name, a number.
		{CatalogColumn{Type: "VARCHAR", SourceType: oidText, SourceTypeMod: -1}, ""},
		{CatalogColumn{Type: "VARCHAR", SourceType: oidVarchar, SourceTypeMod: 24}, ""},
		{CatalogColumn{Type: "VARCHAR", SourceType: oidChar, SourceTypeMod: -1}, ""},
		{CatalogColumn{Type: "VARCHAR", SourceType: oidName, SourceTypeMod: -1}, ""},
		{CatalogColumn{Type: "INTEGER", SourceType: oidInt4, SourceTypeMod: -1}, ""},
		// A type of the user's (an enum, a domain): not told apart.
		{CatalogColumn{Type: "VARCHAR", SourceType: 16400, SourceTypeMod: -1}, ""},
		// No type recorded: a text column may be a char(n), a number is not.
		{CatalogColumn{Type: "VARCHAR"}, bpcharType},
		{CatalogColumn{Type: "VARCHAR[]"}, bpcharType + "[]"},
		{CatalogColumn{Type: "INTEGER"}, ""},
		{CatalogColumn{Type: "DECIMAL(12,2)"}, ""},
	} {
		got := sourceMark(tc.col)
		if got != tc.want {
			t.Errorf("%+v: mark %q, want %q", tc.col, got, tc.want)
		}
		if got != "" && (!typeIsText(got) || !paddedType(got)) {
			t.Errorf("the mark %q is not text to the screen, or not a mark", got)
		}
	}
	// The copy's own types are never taken for a mark, and the type the
	// screen gives a defined name is not either.
	for _, typ := range []string{"VARCHAR", "VARCHAR[]", "INTEGER", "DECIMAL(38,18)", "TIMESTAMP WITH TIME ZONE", "BLOB", "JSON", notTextType} {
		if paddedType(typ) {
			t.Errorf("the copy's type %s is read as a mark", typ)
		}
	}
	if !sizedPadded(charNType) || !sizedPadded(charNType+"[]") || sizedPadded(bpcharType) || sizedPadded(bpcharType+"[]") {
		t.Error("sizedPadded does not tell a char(n) from a bpchar")
	}
	if !PaddedOnSource(oidBpchar) || !PaddedOnSource(oidBpcharArray) || PaddedOnSource(oidText) || PaddedOnSource(oidChar) || PaddedOnSource(0) {
		t.Error("PaddedOnSource")
	}
}

func TestPadded_toTheSource(t *testing.T) {
	for _, q := range []string{
		// Every comparison.
		`SELECT id FROM codes WHERE code = 'ab'`,
		`SELECT id FROM codes WHERE code <> 'ab'`,
		`SELECT id FROM codes WHERE code != 'ab'`,
		`SELECT id FROM codes WHERE code < 'b'`,
		`SELECT id FROM codes WHERE code >= 'ab'`,
		`SELECT id FROM codes WHERE code BETWEEN 'a' AND 'b'`,
		`SELECT id FROM codes WHERE code IN ('ab', 'x')`,
		`SELECT id FROM codes WHERE code NOT IN ('ab')`,
		`SELECT id FROM codes WHERE code LIKE 'ab'`,
		`SELECT id FROM codes WHERE code LIKE 'ab%'`,
		`SELECT id FROM codes WHERE code ILIKE 'AB'`,
		`SELECT id FROM codes WHERE code IS DISTINCT FROM 'ab'`,
		`SELECT id FROM codes WHERE code = label`,
		`SELECT id FROM codes WHERE 'ab' = code`,
		`SELECT id FROM codes WHERE code IS NULL`,
		`SELECT id FROM codes WHERE code = $1`,
		// The name written every way.
		`SELECT id FROM codes WHERE codes.code = 'ab'`,
		`SELECT id FROM codes c WHERE c.code = 'ab'`,
		`SELECT id FROM codes AS c WHERE c.code = 'ab'`,
		`SELECT id FROM public.codes WHERE public.codes.code = 'ab'`,
		`SELECT id FROM codes WHERE "code" = 'ab'`,
		`SELECT id FROM codes WHERE CODE = 'ab'`,
		`SELECT id FROM Codes WHERE Code = 'ab'`,
		// A join's condition, USING.
		`SELECT a.id FROM codes a JOIN plain b ON a.code = b.name`,
		`SELECT a.id FROM codes a JOIN plain b ON b.name = a.code`,
		`SELECT a.id FROM codes a JOIN plain b USING (code)`,
		`SELECT a.id FROM codes a, plain b WHERE a.code = b.code`,
		// GROUP BY, DISTINCT, ORDER BY.
		`SELECT code, count(*) FROM codes GROUP BY code`,
		`SELECT count(*) FROM codes GROUP BY code`,
		`SELECT DISTINCT length(code) FROM codes`,
		`SELECT DISTINCT ON (code) id FROM codes`,
		`SELECT id FROM codes ORDER BY code`,
		`SELECT id, row_number() OVER (PARTITION BY code) FROM codes`,
		`SELECT id FROM codes GROUP BY id HAVING max(code) = 'ab'`,
		// Set operations.
		`SELECT label FROM codes INTERSECT SELECT code FROM codes`,
		`SELECT code FROM codes EXCEPT SELECT label FROM codes`,
		`SELECT code FROM codes UNION SELECT name FROM plain`,
		`SELECT code FROM codes UNION ALL SELECT name FROM plain`,
		`SELECT * FROM codes INTERSECT SELECT * FROM codes`,
		// Lengths, concatenation, any function, a cast, CASE, aggregates.
		`SELECT id, length(code) FROM codes`,
		`SELECT id, char_length(code) FROM codes`,
		`SELECT id, octet_length(code) FROM codes`,
		`SELECT id, code || 'x' FROM codes`,
		`SELECT id, 'x' || code FROM codes`,
		`SELECT id, concat(code, '|') FROM codes`,
		`SELECT id, upper(code) FROM codes`,
		`SELECT id, trim(code) FROM codes`,
		`SELECT id, substring(code, 1, 2) FROM codes`,
		`SELECT id, coalesce(code, 'none') FROM codes`,
		`SELECT id, code::text FROM codes`,
		`SELECT id, CAST(code AS text) FROM codes`,
		`SELECT id, CASE WHEN code = 'ab' THEN 1 ELSE 0 END FROM codes`,
		`SELECT id, CASE code WHEN 'ab' THEN 1 ELSE 0 END FROM codes`,
		`SELECT id, CASE WHEN id = 1 THEN code ELSE label END FROM codes`,
		`SELECT min(code) FROM codes`,
		`SELECT count(code) FROM codes`,
		`SELECT count(DISTINCT code) FROM codes`,
		`SELECT string_agg(code, ',') FROM codes`,
		`SELECT id, (code) FROM codes`,
		// Through a name the statement gives it: a WITH, a subquery.
		`WITH x AS (SELECT id, code AS z FROM codes) SELECT id FROM x WHERE z = 'ab'`,
		`WITH x AS (SELECT id, code FROM codes) SELECT id FROM x WHERE id = 1`,
		`WITH x AS (SELECT * FROM codes) SELECT id FROM x WHERE code = 'ab'`,
		`WITH x(a, b) AS (SELECT id, code FROM codes) SELECT a FROM x WHERE b = 'ab'`,
		`SELECT id FROM (SELECT id, code AS z FROM codes) s WHERE z = 'ab'`,
		`SELECT count(*) FROM (SELECT * FROM codes) s WHERE s.code = 'ab'`,
		`SELECT s.q FROM (SELECT * FROM codes) AS s(p, q)`,
		`SELECT id FROM (SELECT c.* FROM codes c) s WHERE code = 'ab'`,
		`SELECT id FROM plain WHERE name IN (SELECT code FROM codes)`,
		`SELECT id, (SELECT code FROM codes WHERE codes.id = plain.id) FROM plain`,
		`SELECT code FROM codes WHERE id IN (SELECT id FROM plain)`,
		// A star anywhere but a plain select list.
		`SELECT id FROM codes WHERE (codes.*) IS NOT NULL`,
		`SELECT * EXCLUDE (id) FROM codes`,
		`SELECT COLUMNS(*) FROM codes`,
		`SELECT count(codes.*) FROM codes`,
		// An array of char(n).
		`SELECT id FROM codes WHERE tags[1] = 'ab'`,
		`SELECT id FROM codes WHERE 'ab' = ANY(tags)`,
		`SELECT id, tags[1] FROM codes`,
		// A bpchar without a length: its values keep their own blanks.
		`SELECT id FROM codes WHERE free = 'ab'`,
		`SELECT DISTINCT free FROM codes`,
		`SELECT free, count(*) FROM codes GROUP BY 1`,
		`SELECT DISTINCT * FROM codes`,
		// What reaches the column without writing its name, or under a name
		// the statement gives it: said by this rule too, for a connection
		// that sends nothing to its source and has no other screen.
		`SELECT label FROM plain WHERE name IN (TABLE codes)`,
		`SELECT name FROM plain EXCEPT TABLE codes`,
		`SELECT p.id FROM plain p NATURAL JOIN codes`,
		`SELECT x FROM codes AS a(i, x) WHERE x = 'ab'`,
		`SELECT x FROM codes a(i, x) WHERE x = 'ab'`,
		`SELECT code AS x FROM codes WHERE x = 'ab'`,
		`SELECT code AS x, length(x) FROM codes`,
		`SELECT code x, x || '!' FROM codes`,
		`SELECT id FROM codes WHERE #2 = 'ab'`,
		`SELECT id FROM codes WHERE COLUMNS('c') = 'ab'`,
		`SELECT name FROM plain WHERE name IN (FROM codes)`,
		`SELECT DISTINCT ON (id) * EXCLUDE (id) FROM codes`,
		// The table's whole row as a value.
		`SELECT count(DISTINCT codes) FROM codes`,
		`SELECT count(DISTINCT c) FROM codes c`,
		`SELECT a.id FROM codes a JOIN codes AS b ON a = b`,
		`SELECT k, codes FROM codes`,
		// A table whose snapshot records no types.
		`SELECT id FROM legacy WHERE word = 'x'`,
		`SELECT id, length(word) FROM legacy`,
		`SELECT DISTINCT word FROM legacy`,
		`SELECT DISTINCT * FROM legacy`,
		`SELECT word, count(*) FROM legacy GROUP BY 1`,
	} {
		if veto := paddedScreen(q); !isPaddedVeto(veto) {
			t.Errorf("%s: %q, want it sent to the source for its padded column", q, veto)
		}
	}
}

func TestPadded_leftToTheCopy(t *testing.T) {
	for _, q := range []string{
		// A plain select list: both sides write the padded value.
		`SELECT code FROM codes`,
		`SELECT id, code FROM codes WHERE id = 3`,
		`SELECT codes.code, id FROM codes`,
		`SELECT c.code AS x, c.id FROM codes c WHERE c.k > 1`,
		`SELECT c.code x FROM codes c`,
		`SELECT "code" FROM codes`,
		`SELECT public.codes.code FROM public.codes`,
		`SELECT id, code, label FROM codes WHERE label = 'x' AND k IN (1, 2)`,
		`SELECT DISTINCT code FROM codes`,
		`SELECT code, count(*) FROM codes GROUP BY 1`,
		`SELECT code, id FROM codes LIMIT 5`,
		`SELECT tags FROM codes WHERE id = 1`,
		`SELECT free FROM codes WHERE id = 1`,
		`SELECT a.code, b.name FROM codes a JOIN plain b ON a.id = b.id`,
		// A star in a plain select list.
		`SELECT * FROM codes`,
		`SELECT * FROM codes WHERE id = 1`,
		`SELECT codes.* FROM codes WHERE k > 1`,
		`SELECT c.*, c.id FROM codes c`,
		`SELECT * FROM legacy WHERE id = 1`,
		`SELECT word FROM legacy`,
		// Nothing of the padded columns.
		`SELECT count(*) FROM codes`,
		`SELECT id, k * 2 FROM codes WHERE k > 1`,
		`SELECT count(*) * 2 FROM codes`,
		`SELECT id FROM codes WHERE label = 'x'`,
		`SELECT k, count(*) FROM codes GROUP BY k`,
		`SELECT id FROM (SELECT id, k FROM codes) s WHERE k = 1`,
		`WITH x AS (SELECT id, label FROM codes) SELECT count(*) FROM x WHERE label = 'a'`,
		// "char" (one byte) and a type of the user's are not padded.
		`SELECT id FROM codes WHERE one = 'a'`,
		`SELECT id FROM codes WHERE kind = 'new'`,
	} {
		if veto := paddedScreen(q); veto != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, veto)
		}
	}
}

// A table without a char(n) column routes as it did: the padded columns of
// another table say nothing of it, a column of the same name included.
func TestPadded_aTableWithoutOneRoutesAsBefore(t *testing.T) {
	tables := paddedCatalog()
	// The same catalog with every column's type recorded as plain text: the
	// screen before #3068.
	before := map[string][]string{}
	for _, tb := range tables {
		for _, c := range tb.Columns {
			before[strings.ToLower(c.Name)] = append(before[strings.ToLower(c.Name)], strings.ToUpper(c.Type))
		}
	}
	for _, q := range []string{
		`SELECT id FROM plain WHERE code = 'ab'`,
		`SELECT id FROM plain WHERE code <> 'ab' AND status = 'new'`,
		`SELECT id, length(code), code || 'x' FROM plain`,
		`SELECT code, count(*) FROM plain GROUP BY code`,
		`SELECT DISTINCT code FROM plain`,
		`SELECT count(DISTINCT code) FROM plain`,
		`SELECT id FROM plain WHERE code IN ('ab', 'x') AND name LIKE 'a%'`,
		`SELECT a.id FROM plain a JOIN plain b ON a.code = b.name`,
		`SELECT count(*) FROM (SELECT * FROM plain) s`,
		`WITH x AS (SELECT id, code AS z FROM plain) SELECT id FROM x WHERE z = 'ab'`,
		`SELECT code FROM plain UNION ALL SELECT name FROM plain`,
		`SELECT * FROM plain WHERE qty > 1`,
		`SELECT status, sum(qty) FROM plain WHERE code = 'ab' GROUP BY status`,
		`SELECT id FROM plain WHERE id IN (SELECT id FROM plain WHERE code = 'ab')`,
		// What the screen sent to the source before still goes there, for
		// the reason it did.
		`SELECT id FROM plain ORDER BY code`,
		`SELECT id FROM plain WHERE code < 'b'`,
		`SELECT id, upper(code) FROM plain`,
		`SELECT now()`,
		`SELECT 1`,
		`SELECT a.id FROM plain a NATURAL JOIN plain b`,
		`SELECT count(DISTINCT p) FROM plain p`,
	} {
		want := CopyScreen(q, before)
		got := paddedScreen(q)
		if got != want {
			t.Errorf("%s: %q, want %q as before", q, got, want)
		}
		if isPaddedVeto(got) {
			t.Errorf("%s: sent to the source for a padded column it does not read: %s", q, got)
		}
	}
	// The statements of the screen's own tests, on a copy with no char(n)
	// column at all, are decided as before whatever the marks are given.
	if padded, _ := paddedAmong(routeTypes); padded {
		t.Fatal("routeTypes holds a mark")
	}
}

// When the statement's tables cannot be told, every table counts as read.
func TestPadded_tablesNotTold(t *testing.T) {
	tables := paddedCatalog()
	for _, q := range []string{
		// A nested WITH, a function in FROM, LATERAL: not read whole.
		`SELECT id FROM (WITH y AS (SELECT id, code FROM plain) SELECT id, code FROM y) s WHERE code = 'ab'`,
		`SELECT id FROM plain, LATERAL (SELECT 1) l WHERE code = 'ab'`,
		// A table the catalog does not list.
		`SELECT id FROM elsewhere WHERE code = 'ab'`,
		// Three names.
		`SELECT id FROM db.public.plain WHERE code = 'ab'`,
	} {
		if tablesRead(RewriteForCopy(q), tables) != nil {
			t.Errorf("%s: its tables were told", q)
			continue
		}
		toks, _ := lexSQL(RewriteForCopy(q))
		if veto := paddedUse(toks, screenTypes(tables, nil)); !isPaddedVeto(veto) {
			t.Errorf("%s: %q, want it sent to the source: code is a char(n) in a table it may read", q, veto)
		}
	}
	// A name is matched without regard to case and in every schema; a WITH's
	// name is not a table, and a table of that name still counts.
	read := tablesRead(`WITH codes AS (SELECT id FROM plain) SELECT id FROM codes`, tables)
	if read == nil || !read(&tables[0]) || !read(&tables[1]) || read(&tables[2]) {
		t.Error("a WITH named as a table: want codes and plain read, legacy not")
	}
	read = tablesRead(`SELECT id FROM PUBLIC.Plain`, tables)
	if read == nil || read(&tables[0]) || !read(&tables[1]) {
		t.Error("PUBLIC.Plain: want plain read, codes not")
	}
}

// The rule itself, token by token: what counts as a whole select item.
func TestPadded_use(t *testing.T) {
	types := screenTypes(paddedCatalog(), nil)
	use := func(q string) string {
		toks, why := lexSQL(q)
		if why != "" {
			t.Fatalf("%s: %s", q, why)
		}
		return paddedUse(toks, types)
	}
	for q, want := range map[string]bool{
		`SELECT code FROM codes`:                                               false,
		`(SELECT code FROM codes)`:                                             false,
		`SELECT code FROM codes;`:                                              false,
		`SELECT ALL code FROM codes`:                                           false,
		`SELECT k * 2, 2 * k, (k) * (k) FROM codes`:                            false,
		`SELECT count(*), count(*) * 2 FROM codes`:                             false,
		`SELECT extract(year FROM d), code FROM codes`:                         false,
		`SELECT id, CAST(k AS decimal(10,2)) FROM codes c WHERE c.k IN (1, 2)`: false,
		`SELECT k, count(*) FILTER (WHERE k > 1), row_number() OVER (PARTITION BY k) FROM codes AS c GROUP BY k`: false,
		`SELECT id FROM codes c JOIN plain p USING (id) WHERE NOT (c.k = 1) AND p.qty BETWEEN (1) AND (2)`:       false,
		`FROM codes SELECT id WHERE k = 1`:                       true,
		`SELECT 1`:                                               false,
		`SELECT code + 1 FROM codes`:                             true,
		`SELECT code, code FROM codes WHERE code = 'a'`:          true,
		`SELECT code AS x FROM codes ORDER BY code`:              true,
		`SELECT code FROM codes; SELECT code FROM codes`:         true,
		`FROM codes SELECT code`:                                 true,
		`TABLE codes`:                                            false,
		`VALUES (1)`:                                             false,
		`SELECT * FROM codes, plain`:                             false,
		`SELECT *, * FROM codes`:                                 false,
		`SELECT x.* FROM codes x`:                                false,
		`SELECT f(*) FROM codes`:                                 true,
		`SELECT s.count(*) FROM codes`:                           true,
		`SELECT DISTINCT ON (id) * FROM codes`:                   true,
		`SELECT * REPLACE (1 AS id) FROM codes`:                  true,
		`SELECT id FROM codes WHERE id IN (SELECT * FROM plain)`: true,
	} {
		if got := use(q); (got != "") != want {
			t.Errorf("%s: %q, want a veto: %v", q, got, want)
		}
	}
	// No mark among the types, or no types at all: nothing is said.
	toks, _ := lexSQL(`SELECT id FROM plain WHERE code = 'ab'`)
	if got := paddedUse(toks, routeTypes); got != "" {
		t.Errorf("no mark: %q", got)
	}
	if got := paddedUse(toks, nil); got != "" {
		t.Errorf("no types: %q", got)
	}
	if got := catalogUnread(toks); got == "" {
		t.Error("a table read with the catalog unread was left to the copy")
	}
	one, _ := lexSQL(`SELECT 1 + 1`)
	if got := catalogUnread(one); got != "" {
		t.Errorf("SELECT 1 + 1 with the catalog unread: %q", got)
	}
}

// paddedLogin is a port whose copy is paddedCatalog and answers one row to
// whatever it is handed, keeping what it ran.
type paddedLogin struct {
	forward, barred, catalogFails bool
	mu                            sync.Mutex
	ran                           []string
	notes                         []RouteNote
}

func (l *paddedLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	b := &Binding{Actor: "server:codes", Schema: "public",
		Run: func(_ context.Context, st string, _ RunOptions) (sqlsandbox.Result, error) {
			l.mu.Lock()
			l.ran = append(l.ran, st)
			l.mu.Unlock()
			return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{int32(7)}}}, nil
		},
		Catalog: func(context.Context) ([]CatalogTable, error) {
			if l.catalogFails {
				return nil, errors.New("the copy's catalog is not there")
			}
			return paddedCatalog(), nil
		},
		Routed: func(n RouteNote) {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.notes = append(l.notes, n)
		}}
	if l.forward || l.barred {
		// The source cannot be reached: 08001 says a statement was sent there.
		b.Forward = &Forward{DSN: "postgres://fwd:pw@127.0.0.1:1/db?sslmode=disable&connect_timeout=2", Cost: &CostPolicy{}}
	}
	if l.barred {
		b.Forward.Barred = func() string { return "it hides tables" }
	}
	return b, nil
}

func (l *paddedLogin) ranOnCopy(stmt string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, st := range l.ran {
		if strings.Contains(st, stmt) {
			return true
		}
	}
	return false
}

func (l *paddedLogin) last() RouteNote {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.notes) == 0 {
		return RouteNote{}
	}
	return l.notes[len(l.notes)-1]
}

// Through the port: with a forwarding account the statement goes to the
// source and the copy is never asked; without one, and on a server that
// hides tables, it is refused and the copy is never asked either.
func TestPadded_throughThePort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	code := func(err error) string {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			return pe.Code
		}
		if err != nil {
			return err.Error()
		}
		return ""
	}
	message := func(err error) string {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			return pe.Message
		}
		return ""
	}
	padded := []string{
		`SELECT id FROM codes WHERE code = 'ab'`,
		`SELECT id, length(code) FROM codes`,
		`SELECT count(*) FROM (SELECT * FROM codes) s WHERE s.code = 'ab'`,
		`SELECT id FROM legacy WHERE word = 'x'`,
		`SELECT p.id FROM plain p NATURAL JOIN codes`,
		`SELECT code AS x FROM codes WHERE x = 'ab'`,
		`SELECT count(DISTINCT c) FROM codes c`,
	}
	plain := []string{
		`SELECT code FROM codes`,
		`SELECT * FROM codes WHERE id = 1`,
		`SELECT id FROM plain WHERE code = 'ab'`,
		`SELECT count(*) FROM plain WHERE status = 'new'`,
	}
	for _, mode := range []string{"none", "barred", "forward"} {
		l := &paddedLogin{forward: mode == "forward", barred: mode == "barred"}
		addr := serveAddrCfg(t, Config{Login: l.login})
		c, err := pgconn.Connect(ctx, fmt.Sprintf("host=%s port=%s user=codes password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(context.Background())
		runs := map[string]func(string) error{
			"simple": func(stmt string) error { _, err := c.Exec(ctx, stmt).ReadAll(); return err },
			"extended": func(stmt string) error {
				return c.ExecParams(ctx, stmt, nil, nil, nil, nil).Read().Err
			},
		}
		for how, run := range runs {
			for _, stmt := range padded {
				err := run(stmt)
				if l.ranOnCopy(stmt) {
					t.Errorf("%s, %s: %s was run on the copy", mode, how, stmt)
				}
				n := l.last()
				switch mode {
				case "forward":
					if code(err) != "08001" || n.Side != RouteSource || n.Reason != ReasonVeto {
						t.Errorf("%s, %s: %s: %v, noted %s/%s; want it sent to the source (08001, the source is down) as a veto", mode, how, stmt, err, n.Side, n.Reason)
					}
				default:
					if code(err) != "0A000" || !strings.Contains(message(err), "refused rather than answered with other rows") {
						t.Errorf("%s, %s: %s: %v; want it refused, 0A000", mode, how, stmt, err)
					}
				}
			}
			for _, stmt := range plain {
				if err := run(stmt); err != nil {
					t.Errorf("%s, %s: %s: %v, want the copy's answer", mode, how, stmt, err)
				}
				if !l.ranOnCopy(strings.TrimPrefix(stmt, "SELECT ")) {
					t.Errorf("%s, %s: %s was not run on the copy", mode, how, stmt)
				}
			}
		}
		// An argument is a value like any other.
		err = c.ExecParams(ctx, `SELECT id FROM codes WHERE code = $1`, [][]byte{[]byte("ab")}, nil, nil, nil).Read().Err
		if want := map[string]string{"forward": "08001", "none": "0A000", "barred": "0A000"}[mode]; code(err) != want {
			t.Errorf("%s: an argument compared with the char(n) column: %v, want %s", mode, err, want)
		}
		if l.ranOnCopy(`FROM codes WHERE code =`) {
			t.Errorf("%s: a comparison of the char(n) column with an argument was run on the copy", mode)
		}
		if mode == "forward" {
			continue
		}
		// The copy's own commands, and an EXPLAIN, have no answer of
		// PostgreSQL's to differ from.
		for _, stmt := range []string{`SHOW TABLES`, `DESCRIBE codes`, `EXPLAIN SELECT id FROM codes WHERE code = 'ab'`} {
			if _, err := c.Exec(ctx, stmt).ReadAll(); err != nil {
				t.Errorf("%s: %s: %v, want the copy's answer", mode, stmt, err)
			}
		}
		// A statement the port does not read whole: the copy's when it can
		// name no padded column, refused when it may.
		if _, err := c.Exec(ctx, `SELECT id FROM plain WHERE status = E'new'`).ReadAll(); err != nil {
			t.Errorf("%s: a string with a prefix on a table without a char(n): %v, want the copy's answer", mode, err)
		}
		for _, stmt := range []string{
			`SELECT id FROM codes WHERE label = E'x' AND code = 'ab'`,
			`SELECT count(*) FROM codes WHERE label = E'x'`,
			`SELECT id FROM codes WHERE U&"c\006Fde" = 'ab'`,
		} {
			if _, err := c.Exec(ctx, stmt).ReadAll(); code(err) != "0A000" {
				t.Errorf("%s: %s: %v, want it refused", mode, stmt, err)
			}
			if l.ranOnCopy(stmt) {
				t.Errorf("%s: %s was run on the copy", mode, stmt)
			}
		}
		if st := c.TxStatus(); st != 'I' {
			t.Errorf("%s: the connection is left in status %c", mode, st)
		}
	}
}

// A catalog that cannot be read: which column is a char(n) is not known, so
// no statement that reads a table is the copy's.
func TestPadded_catalogUnread(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, forward := range []bool{false, true} {
		l := &paddedLogin{forward: forward, catalogFails: true}
		addr := serveAddrCfg(t, Config{Login: l.login})
		c, err := pgconn.Connect(ctx, fmt.Sprintf("host=%s port=%s user=codes password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(context.Background())
		_, err = c.Exec(ctx, `SELECT id FROM plain WHERE qty = 1`).ReadAll()
		var pe *pgconn.PgError
		want := "0A000"
		if forward {
			want = "08001"
		}
		if !errors.As(err, &pe) || pe.Code != want {
			t.Errorf("forward=%v: a table read with the catalog unread: %v, want %s", forward, err, want)
		}
		if l.ranOnCopy(`FROM plain`) {
			t.Errorf("forward=%v: it was run on the copy", forward)
		}
		if _, err := c.Exec(ctx, `SELECT 1 + 1`).ReadAll(); err != nil {
			t.Errorf("forward=%v: SELECT 1 + 1: %v, want the copy's answer", forward, err)
		}
	}
}

// The screen's decision as a session makes it, with the statement's tables
// read from its text.
func TestPadded_route(t *testing.T) {
	b := &Binding{Schema: "public", Catalog: func(context.Context) ([]CatalogTable, error) { return paddedCatalog(), nil },
		Forward: &Forward{DSN: "postgres://fwd:pw@127.0.0.1:1/db"}}
	s := &session{bound: b, logger: discardLogger(), currentDB: "public"}
	s.initSettings("public", "app")
	for stmt, want := range map[string]routeDecision{
		`SELECT id FROM codes WHERE code = 'ab'`:                 {RouteSource, ReasonVeto},
		`SELECT id FROM codes c JOIN plain p ON c.code = p.name`: {RouteSource, ReasonVeto},
		`SELECT id, length(code) FROM codes`:                     {RouteSource, ReasonVeto},
		`SELECT code FROM codes`:                                 {RouteCopy, ReasonAnswersTheSame},
		`SELECT * FROM codes WHERE id = 1`:                       {RouteCopy, ReasonAnswersTheSame},
		`SELECT id FROM plain WHERE code = 'ab'`:                 {RouteCopy, ReasonAnswersTheSame},
		`SELECT count(*) FROM (SELECT * FROM plain) s`:           {RouteCopy, ReasonAnswersTheSame},
		`SELECT id FROM legacy WHERE word = 'x'`:                 {RouteSource, ReasonVeto},
		`SELECT id FROM legacy WHERE id = 1`:                     {RouteCopy, ReasonAnswersTheSame},
	} {
		// What depends on the types is asked of a statement headed for the
		// copy, once the plan sent it there.
		if got := s.screenTyped(stmt, s.route(s.classify(stmt), stmt)); got != want {
			t.Errorf("%s: %+v, want %+v", stmt, got, want)
		}
	}
}
