package pgshim

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// psql's backslash commands, one psql major at a time (#3059). Each major
// writes \l, \dt and the statements of \d its own way; what each sends is in
// testdata/catalog_3059/psql<major>.sql, collected from that version's psql
// (capture.sh there). Every one of those statements is recognized, by a form
// that names that major, and answered as psql 16's is; a text none of them
// sends is still refused.

// psqlSent is one statement a psql sent, and the command it sent it for.
type psqlSent struct {
	command, text string
}

func psqlStatements(t *testing.T, major int) []psqlSent {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("testdata/catalog_3059/psql%d.sql", major))
	if err != nil {
		t.Fatal(err)
	}
	header, _, _ := strings.Cut(string(b), "\n")
	if !strings.Contains(header, fmt.Sprintf("psql (PostgreSQL) %d.", major)) {
		t.Fatalf("psql%d.sql says it is from %q", major, header)
	}
	var out []psqlSent
	for i, block := range strings.Split(string(b), "\n====\n") {
		if i == 0 {
			continue // the header
		}
		command, text, ok := strings.Cut(block, "\n")
		if !ok || !strings.HasPrefix(command, `-- \`) || strings.TrimSpace(text) == "" {
			t.Fatalf("psql%d.sql, block %d: not a command and its statement:\n%s", major, i, block)
		}
		out = append(out, psqlSent{strings.TrimPrefix(command, "-- "), strings.TrimSpace(text)})
	}
	if len(out) < 40 {
		t.Fatalf("psql%d.sql holds only %d statements", major, len(out))
	}
	return out
}

func psqlMajors() []int {
	var out []int
	for m := psqlOldest; m <= psqlNewest; m++ {
		out = append(out, m)
	}
	return out
}

// psqlCatalog is the tables capture.sh creates, with the OIDs the
// PostgreSQL it starts gives them: the statements in testdata name tables by
// those OIDs.
func psqlCatalog() []CatalogTable {
	return []CatalogTable{
		{Schema: "public", Name: "c2769_orders", OID: 16386, PrimaryKey: []string{"id"}, Columns: []CatalogColumn{
			{Name: "id", Type: "INTEGER", NotNull: true}, {Name: "note", Type: "VARCHAR"}, {Name: "amount", Type: "DECIMAL(10,2)"},
			{Name: "created", Type: "TIMESTAMP WITH TIME ZONE"}, {Name: "d", Type: "DATE"}, {Name: "code", Type: "VARCHAR", NotNull: true, Default: "'x'::text"},
			{Name: "big", Type: "BIGINT"}, {Name: "flags", Type: "BOOLEAN[]"}, {Name: "u", Type: "UUID"}, {Name: "ok", Type: "BOOLEAN"}, {Name: "r", Type: "FLOAT"}}},
		{Schema: "c2769s", Name: "c2769_items", OID: 16394, PrimaryKey: []string{"sku", "line"}, Columns: []CatalogColumn{
			{Name: "sku", Type: "VARCHAR", NotNull: true}, {Name: "line", Type: "INTEGER", NotNull: true}, {Name: "qty", Type: "INTEGER", NotNull: true}}},
		{Schema: "public", Name: "c2769_text", OID: 16401, PrimaryKey: []string{"id"}, Columns: []CatalogColumn{
			{Name: "id", Type: "INTEGER", NotNull: true}, {Name: "vc", Type: "VARCHAR"}, {Name: "doc", Type: "VARCHAR"}, {Name: "iv", Type: "VARCHAR"}}},
	}
}

// psqlAnswer is what one statement of a psql was answered: the form that
// recognized it and its rows, each cell under its column's name (a second
// column of one name is name#2).
type psqlAnswer struct {
	form string
	rows []map[string]string
}

func answerPsql(t *testing.T, v *catalogView, major int, sent psqlSent) (psqlAnswer, *catalogStmt) {
	t.Helper()
	// A statement that does not name the catalog would not be refused when
	// it is not recognized: it would run on the copy's engine.
	if !readsCatalog(sent.text) {
		t.Fatalf("psql %d, %s: the statement does not read as a catalog statement:\n%s", major, sent.command, sent.text)
	}
	st, m := matchCatalog(catalogCanon(sent.text))
	if st == nil {
		t.Fatalf("psql %d, %s: not recognized:\n%s", major, sent.command, sent.text)
	}
	if st.psql[0] == 0 || major < st.psql[0] || major > st.psql[1] {
		t.Fatalf("psql %d, %s: recognized as %q, a form of psql %d to %d", major, sent.command, st.name, st.psql[0], st.psql[1])
	}
	rows, perr := st.rows(v, m)
	if perr != nil {
		t.Fatalf("psql %d, %s: %q refused it: %s\n%s", major, sent.command, st.name, perr.msg, sent.text)
	}
	a := psqlAnswer{form: st.name}
	for _, r := range rows {
		if len(r) != len(st.cols) {
			t.Fatalf("psql %d, %s: %q answers %d cells under %d columns", major, sent.command, st.name, len(r), len(st.cols))
		}
		named := map[string]string{}
		for i, c := range st.cols {
			name := c.name
			for n := 2; ; n++ {
				if _, taken := named[name]; !taken {
					break
				}
				name = fmt.Sprintf("%s#%d", c.name, n)
			}
			cell := "NULL"
			if b := catalogCell(r[i]); b != nil {
				cell = string(b)
			}
			named[name] = cell
		}
		a.rows = append(a.rows, named)
	}
	return a, st
}

func TestCatalogPsql_everyVersionIsRecognized(t *testing.T) {
	v := testCatalogViewOf(psqlCatalog())
	sentBy := map[*catalogStmt]map[int]bool{}
	for _, major := range psqlMajors() {
		for _, sent := range psqlStatements(t, major) {
			_, st := answerPsql(t, v, major, sent)
			if sentBy[st] == nil {
				sentBy[st] = map[int]bool{}
			}
			sentBy[st][major] = true
		}
	}
	// And nothing more: a form says which majors send it, and each of them
	// does. A text no psql was seen sending is one nobody checked the
	// answer of.
	psqlForms := 0
	for _, st := range catalogStmts {
		if st.psql[0] == 0 {
			if strings.HasPrefix(st.name, "psql") {
				t.Errorf("%q does not say which psql sends it", st.name)
			}
			continue
		}
		psqlForms++
		if st.psql[0] < psqlOldest || st.psql[1] > psqlNewest || st.psql[0] > st.psql[1] {
			t.Errorf("%q is said to be sent by psql %d to %d, and psql %d to %d were collected", st.name, st.psql[0], st.psql[1], psqlOldest, psqlNewest)
			continue
		}
		for major := st.psql[0]; major <= st.psql[1]; major++ {
			if !sentBy[st][major] {
				t.Errorf("%q is said to be sent by psql %d, and no statement of testdata/catalog_3059/psql%d.sql is that one:\n%s", st.name, major, major, st.text)
			}
		}
	}
	if psqlForms < 20 {
		t.Errorf("only %d forms of psql", psqlForms)
	}
}

// Whatever the psql, the port says the same of the copy: the statements of
// one command, recognized as one kind of statement, have the same rows, cell
// by cell in the columns two versions both ask for.
func TestCatalogPsql_everyVersionIsAnsweredAlike(t *testing.T) {
	v := testCatalogViewOf(psqlCatalog())
	answers := map[int]map[string]psqlAnswer{}
	for _, major := range psqlMajors() {
		answers[major] = map[string]psqlAnswer{}
		for _, sent := range psqlStatements(t, major) {
			a, _ := answerPsql(t, v, major, sent)
			key := sent.command + " / " + a.form
			if _, twice := answers[major][key]; twice {
				t.Fatalf("psql %d sends two statements recognized as %s", major, key)
			}
			answers[major][key] = a
		}
	}
	// What psql 16 is told, which is what the port was first checked with
	// against a real PostgreSQL.
	cells := func(a psqlAnswer, cols ...string) string {
		var lines []string
		for _, r := range a.rows {
			var line []string
			for _, c := range cols {
				line = append(line, r[c])
			}
			lines = append(lines, strings.Join(line, "|"))
		}
		return strings.Join(lines, "\n")
	}
	for key, want := range map[string]string{
		`\l / psql \l`:                                     "orders|app|UTF8|C|C|NULL",
		`\dn / psql \dn`:                                   "c2769s|app\npublic|app",
		`\dn c2769* / psql \dn`:                            "c2769s|app",
		`\dt / psql \dt`:                                   "public|c2769_orders|table|app\npublic|c2769_text|table|app",
		`\dt c2769* / psql \dt`:                            "public|c2769_orders|table|app\npublic|c2769_text|table|app",
		`\dt c2769s.* / psql \dt`:                          "c2769s|c2769_items|table|app",
		`\dt *.* / psql \dt`:                               "c2769s|c2769_items|table|app\npublic|c2769_orders|table|app\npublic|c2769_text|table|app",
		`\d / psql \dt`:                                    "public|c2769_orders|table|app\npublic|c2769_text|table|app",
		`\d c2769_orders / psql \d lookup`:                 "16386|public|c2769_orders",
		`\d c2769s.c2769_items / psql \d lookup`:           "16394|c2769s|c2769_items",
		`\d c2769s.c2769_items / psql \d columns`:          "sku|text|NULL|t\nline|integer|NULL|t\nqty|integer|NULL|t",
		`\d c2769s.c2769_items / psql \d indexes`:          "c2769_items_pkey|t|PRIMARY KEY (sku, line)",
		`\d+ c2769s.c2769_items / psql \d+ columns`:        "sku|text|NULL|t|x\nline|integer|NULL|t|p\nqty|integer|NULL|t|p",
		`\d c2769_orders / psql \d table ?column?`:         "r|t",
		`\d+ c2769_orders / psql \d table array_to_string`: "r|t",
	} {
		cols := map[string][]string{
			`psql \l`: {"Name", "Owner", "Encoding", "Collate", "Ctype", "Access privileges"}, `psql \dn`: {"Name", "Owner"}, `psql \dt`: {"Schema", "Name", "Type", "Owner"},
			`psql \d lookup`: {"oid", "nspname", "relname"}, `psql \d columns`: {"attname", "format_type", "pg_get_expr", "attnotnull"},
			`psql \d indexes`: {"relname", "indisprimary", "pg_get_constraintdef"}, `psql \d+ columns`: {"attname", "format_type", "pg_get_expr", "attnotnull", "attstorage"},
			`psql \d table ?column?`: {"relkind", "relhasindex"}, `psql \d table array_to_string`: {"relkind", "relhasindex"},
		}[key[strings.Index(key, " / ")+3:]]
		for _, major := range psqlMajors() {
			a, there := answers[major][key]
			if !there {
				t.Errorf("psql %d sent nothing recognized as %s", major, key)
				continue
			}
			if got := cells(a, cols...); got != want {
				t.Errorf("psql %d, %s:\n got %q\nwant %q", major, key, got, want)
			}
		}
	}
	// And every other statement: the same rows as psql 16's of that kind,
	// in the columns both ask for.
	for _, major := range psqlMajors() {
		for key, a := range answers[major] {
			b, there := answers[16][key]
			if !there {
				// What 16 does not ask for (18's foreign keys and NOT NULL
				// constraints): the copy has none.
				if len(a.rows) != 0 {
					t.Errorf("psql %d, %s: psql 16 does not ask, and the answer has rows: %v", major, key, a.rows)
				}
				continue
			}
			if len(a.rows) != len(b.rows) {
				t.Errorf("psql %d, %s: %d rows, and psql 16 is answered %d", major, key, len(a.rows), len(b.rows))
				continue
			}
			for i := range a.rows {
				for col, cell := range a.rows[i] {
					if other, both := b.rows[i][col]; both && other != cell {
						t.Errorf("psql %d, %s, row %d, %s: %q, and psql 16 is answered %q", major, key, i, col, cell, other)
					}
				}
			}
		}
	}
}

// A catalog statement written a way no psql writes it is refused by name,
// as before, however close it is to one a psql sends: one more relation
// kind, one more column, one more condition, a join another version has.
func TestCatalogPsql_aTextNoVersionSendsIsRefused(t *testing.T) {
	first := func(major int, command string) string {
		for _, sent := range psqlStatements(t, major) {
			if sent.command == command {
				return sent.text
			}
		}
		t.Fatalf("psql %d sent nothing for %s", major, command)
		return ""
	}
	// change rewrites the one statement of a command that holds old, once.
	// It fails when there is not exactly one such place, so a change cannot
	// quietly test nothing.
	change := func(major int, command, old, new string) string {
		var found []string
		for _, sent := range psqlStatements(t, major) {
			if sent.command == command && strings.Contains(sent.text, old) {
				found = append(found, sent.text)
			}
		}
		if len(found) != 1 || strings.Count(found[0], old) != 1 {
			t.Fatalf("psql %d's %s: want %q once in one statement, found it in %d", major, command, old, len(found))
		}
		return strings.Replace(found[0], old, new, 1)
	}
	variants := map[string]string{
		"one more relation kind":                      change(14, `\dt`, `WHEN 's' THEN 'special'`, `WHEN 's' THEN 'special' WHEN 'c' THEN 'composite type'`),
		"13's kinds with 14's join":                   change(13, `\dt`, `LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace`, `LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam`),
		"one more column in the list":                 change(14, `\dt`, `pg_catalog.pg_get_userbyid(c.relowner) as "Owner"`, `pg_catalog.pg_get_userbyid(c.relowner) as "Owner", c.reltuples as "Rows"`),
		"a condition this does not read":              change(14, `\dt`, `AND pg_catalog.pg_table_is_visible(c.oid)`, `AND pg_catalog.pg_table_is_visible(c.oid) AND c.relhasrules`),
		"a condition that widens the list":            change(13, `\dt`, `AND pg_catalog.pg_table_is_visible(c.oid)`, `OR true`),
		"another table read beside the list":          change(14, `\dt *.*`, `ORDER BY 1,2`, `UNION ALL SELECT 'a', relname, 'b', 'c' FROM pg_catalog.pg_class WHERE true ORDER BY 1,2`),
		"something after the list":                    change(13, `\dt`, `ORDER BY 1,2;`, `ORDER BY 1,2 LIMIT 1;`),
		"one more column of \\l":                      change(14, `\l`, `d.datctype as "Ctype",`, `d.datctype as "Ctype", d.datconnlimit as "Limit",`),
		"\\l with a condition":                        change(17, `\l`, `ORDER BY 1`, `WHERE d.datallowconn ORDER BY 1`),
		"15's \\l in another order":                   change(15, `\l`, `NULL as "ICU Locale",`, `NULL as "ICU Rules",`),
		"\\d+'s columns of another table":             change(13, `\d+ c2769_orders`, `FROM pg_catalog.pg_attribute a`, `FROM pg_catalog.pg_attribute a, pg_catalog.pg_class c`),
		"18's indexes with another answer written in": change(18, `\d c2769_orders`, `false AS conperiod`, `true AS conperiod`),
		"the table, or any other":                     change(13, `\d c2769_orders`, `WHERE c.oid = '16386';`, `WHERE c.oid = '16386' OR true;`),
	}
	l := &catalogLogin{tables: psqlCatalog()}
	addr := serveAddrCfg(t, Config{Login: l.login})
	r, err := dialStatus(t, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	for what, q := range variants {
		got := r.query(q)
		if len(got) == 0 || !strings.HasPrefix(got[0], "Error 0A000") || !strings.Contains(got[0], catalogRefusal.msg) {
			t.Errorf("%s: answered %q\n%s", what, got, q)
		}
	}
	// The statements they were made from are answered on the same
	// connection, so it is the change that was refused.
	for _, c := range []struct {
		major   int
		command string
		rows    int
	}{{13, `\dt`, 2}, {14, `\dt`, 2}, {14, `\dt *.*`, 3}, {14, `\l`, 1}, {15, `\l`, 1}, {17, `\l`, 1}} {
		got := r.query(first(c.major, c.command))
		if len(got) != c.rows+3 || !strings.HasPrefix(got[0], "Columns ") || got[len(got)-2] != fmt.Sprintf("CommandComplete SELECT %d", c.rows) {
			t.Errorf("psql %d's %s: answered %q", c.major, c.command, got)
		}
	}
	l.mu.Lock()
	ran := l.ran
	l.mu.Unlock()
	if len(ran) != 0 {
		t.Errorf("a catalog statement reached the copy: %q", ran)
	}
}

// What each psql is told over the wire, with the columns it asked for.
func TestCatalogPsql_overTheWire(t *testing.T) {
	l := &catalogLogin{tables: psqlCatalog()}
	addr := serveAddrCfg(t, Config{Login: l.login})
	r, err := dialStatus(t, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	send := func(major int, command string) []string {
		for _, sent := range psqlStatements(t, major) {
			if sent.command == command {
				return r.query(sent.text)
			}
		}
		t.Fatalf("psql %d sent nothing for %s", major, command)
		return nil
	}
	tables := []string{"Columns Schema:19,Name:19,Type:25,Owner:19", "DataRow public|c2769_orders|table|app", "DataRow public|c2769_text|table|app", "CommandComplete SELECT 2", "ReadyForQuery I"}
	for _, major := range psqlMajors() {
		want(t, fmt.Sprintf(`psql %d's \dt`, major), send(major, `\dt`), tables...)
	}
	for major, w := range map[int][]string{
		13: {"Columns Name:19,Owner:19,Encoding:19,Collate:25,Ctype:25,Access privileges:25", "DataRow orders|app|UTF8|C|C|NULL"},
		14: {"Columns Name:19,Owner:19,Encoding:19,Collate:25,Ctype:25,Access privileges:25", "DataRow orders|app|UTF8|C|C|NULL"},
		15: {"Columns Name:19,Owner:19,Encoding:19,Collate:25,Ctype:25,ICU Locale:25,Locale Provider:25,Access privileges:25", "DataRow orders|app|UTF8|C|C|NULL|libc|NULL"},
		16: {"Columns Name:19,Owner:19,Encoding:19,Locale Provider:25,Collate:25,Ctype:25,ICU Locale:25,ICU Rules:25,Access privileges:25", "DataRow orders|app|UTF8|libc|C|C|NULL|NULL|NULL"},
		17: {"Columns Name:19,Owner:19,Encoding:19,Locale Provider:25,Collate:25,Ctype:25,Locale:25,ICU Rules:25,Access privileges:25", "DataRow orders|app|UTF8|libc|C|C|NULL|NULL|NULL"},
		18: {"Columns Name:19,Owner:19,Encoding:19,Locale Provider:25,Collate:25,Ctype:25,Locale:25,ICU Rules:25,Access privileges:25", "DataRow orders|app|UTF8|libc|C|C|NULL|NULL|NULL"},
	} {
		want(t, fmt.Sprintf(`psql %d's \l`, major), send(major, `\l`), append(w, "CommandComplete SELECT 1", "ReadyForQuery I")...)
	}
	l.mu.Lock()
	ran := l.ran
	l.mu.Unlock()
	if len(ran) != 0 {
		t.Errorf("a catalog statement reached the copy: %q", ran)
	}
}
