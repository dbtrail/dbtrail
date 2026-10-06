package readrouter

import (
	"strings"
	"testing"
)

// A word the copy takes for a type, right before a string: the column or the
// alias of that name on the source, a constant on the copy (#2131). Kept on
// the source from the text, whatever the tables are, so that an alias given
// in the statement itself is caught as a table's column is.
func TestVeto_typeWordBeforeAString(t *testing.T) {
	for _, stmt := range []string{
		"SELECT text 'Label' FROM (SELECT 'body' AS text) t",
		"SELECT text'Label' FROM (SELECT 'body' AS text) t",
		"SELECT json'1' FROM (SELECT 1 AS json) t",
		"SELECT TEXT 'Label' FROM t",
		"SELECT text /* c */ 'Label' FROM t",
		"SELECT text -- c\n 'Label' FROM t",
		"SELECT text\n\t'Label' FROM t",
		"SELECT text 'Label' FROM t",
		"SELECT datetime '2026-01-01' FROM t",
		"SELECT uuid 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', bool '1' FROM t",
		"SELECT id, string 'x' FROM t",
		"SELECT q.text 'Label' FROM t q",
		"SELECT nchar 'x', character 'y' FROM t",
	} {
		if got := Veto(stmt); got != vetoTypeString {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, vetoTypeString)
		}
		if _, refusal := ForCopy(stmt); refusal != vetoTypeString {
			t.Errorf("ForCopy(%q) refusal = %q, want %q", stmt, refusal, vetoTypeString)
		}
	}
	for _, stmt := range []string{
		// The typed literals both sides read the same way.
		"SELECT id FROM t WHERE d >= DATE '2026-01-01'",
		"SELECT id FROM t WHERE ts >= TIMESTAMP '2026-01-01 10:00:00'",
		"SELECT TIME '10:00:00'",
		"SELECT d + INTERVAL '1' DAY FROM t",
		"SELECT ALL 'x', id FROM t",
		"SELECT DISTINCT 'x', id FROM t",
		"SELECT id FROM t WHERE NOT 'x'",
		// A type word that no string follows directly.
		"SELECT text FROM t WHERE text = 'Label'",
		"SELECT text, 'Label' FROM t",
		"SELECT text AS 'Label' FROM t",
		"SELECT CAST(a AS char) FROM t WHERE b IN ('x')",
		"SELECT json, uuid FROM t WHERE note = 'text'",
		// A longer word, and a word that is not a type.
		"SELECT context 'Label' FROM t",
		"SELECT status 'Label' FROM t",
	} {
		if got := Veto(stmt); got == vetoTypeString {
			t.Errorf("Veto(%q) = %q, want it not kept back by this", stmt, got)
		}
	}
	// A quoted name before a string is the older refusal (#2081), and the
	// quote ends the word: `text` 'Label' is not this one.
	if got := Veto("SELECT `text` 'Label' FROM t"); got != vetoNameString {
		t.Errorf("quoted: %q, want %q", got, vetoNameString)
	}
	// A string or a quoted name between the word and the string is not
	// "right before".
	for _, stmt := range []string{"SELECT text = 'a' 'b' FROM t", "SELECT json, `a` FROM t WHERE c = 'x'"} {
		if got := Veto(stmt); got == vetoTypeString {
			t.Errorf("Veto(%q) = %q", stmt, got)
		}
	}
}

// Two lists must not overlap: a word is either refused before a string or
// read the same on both sides.
func TestCopyTypeWords_shape(t *testing.T) {
	for _, w := range SameTypedLiteral() {
		if copyTypeWords[w] {
			t.Errorf("%s is both refused before a string and listed as read alike", w)
		}
	}
	for _, list := range [][]string{CopyTypeWords(), SameTypedLiteral(), CopyOnlyReserved()} {
		for _, w := range list {
			if w != strings.ToLower(w) || !wordName(w) {
				t.Errorf("%q: the lists hold lower-case words", w)
			}
		}
	}
}

// A word the copy reserves and the source takes for a name (#2158). The copy
// refuses most such statements (a column called at); it answers two shapes
// with something else: the word as a table alias before JOIN, and isnull or
// notnull as a column alias written without AS.
func TestVeto_copyReservedWords(t *testing.T) {
	for _, stmt := range []string{
		"SELECT status FROM ev WHERE at >= '2026-01-02 00:00:00' AND at < '2026-01-03' AND note = 'nope' LIMIT 5",
		"SELECT AT FROM ev",
		"SELECT id FROM ev ORDER BY at",
		"SELECT id FROM ev WHERE id > 1 AND at IN ('a')",
		"SELECT max(at) FROM ev",
		"SELECT id, end FROM ev",
		"SELECT CASE WHEN id > 1 THEN end END FROM ev",
		"SELECT CASE end WHEN 1 THEN 2 END FROM ev",
		// A column called case, after a dot, closes nothing.
		"SELECT q.case, end FROM q",
		// WINDOW, a name, and a word that only starts with AS.
		"SELECT window w asc FROM ev",
		"SELECT id FROM ev ORDER BY offset",
		"SELECT id + offset FROM ev LIMIT 5 OFFSET 2",
		"SELECT id FROM ev WHERE id > 1 + offset",
		"SELECT only FROM ev",
		"SELECT id FROM ev WHERE only = 1",
		"SELECT cast FROM ev",
		"SELECT id FROM ev WHERE id = any",
		"SELECT id FROM ev GROUP BY window",
		"SELECT window asc FROM ev",
		"SELECT id FROM ev ORDER BY window",
		"SELECT `id` FROM `ev` WHERE at = 1",
		"SELECT id FROM ev at",
		"SELECT id end FROM ev",
		"WITH at AS (SELECT 1 AS id) SELECT id FROM at",
		// The copy answers these with something else.
		"SELECT COUNT(*) FROM a full JOIN b USING (id)",
		"SELECT COUNT(*) FROM a FULL\n JOIN b ON b.id = 1",
		"SELECT COUNT(*) FROM a semi JOIN b USING (id)",
		"SELECT COUNT(*) FROM a anti JOIN b USING (id)",
		"SELECT COUNT(*) FROM a asof JOIN b USING (id)",
		"SELECT COUNT(*) FROM a positional JOIN b",
		"SELECT COUNT(*) FROM `a`ANTI JOIN `b` USING (`id`)",
		"SELECT COUNT(*) FROM (SELECT 1) full JOIN b ON TRUE",
		"SELECT v isnull FROM t",
		"SELECT v notnull FROM t",
		"SELECT `v`NOTNULL FROM `t`",
		"SELECT (v)isnull FROM t",
	} {
		if got := Veto(stmt); got != vetoCopyReserved {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, vetoCopyReserved)
		}
	}
	for _, stmt := range []string{
		// Quoted, after a dot, a column alias after AS: the copy reads it.
		"SELECT `at` FROM ev WHERE `at` >= '2026-01-02'",
		"SELECT ev.at, e . at FROM ev e WHERE ev.at >= '2026-01-02'",
		"SELECT id AS at, status AS end FROM ev",
		"SELECT x.full FROM a x JOIN b USING (id)",
		"SELECT `isnull`, t.notnull, is_null, isnullable FROM t",
		"SELECT COUNT(*) FROM a `full` JOIN b USING (id)",
		// The keyword on the source too.
		"SELECT CASE WHEN id > 1 THEN 2 END, CASE id WHEN 1 THEN (CASE WHEN id THEN 1 END) END FROM ev",
		"SELECT id FROM ev ORDER BY id LIMIT 20 OFFSET 40",
		"SELECT id FROM ev ORDER BY id LIMIT ? OFFSET ?",
		"SELECT * FROM orders OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY",
		"SELECT * FROM orders ORDER BY amount FETCH FIRST 1 ROW ONLY",
		"SELECT CAST(id AS CHAR), id = ANY (SELECT id FROM ev), id > SOME(SELECT 1) FROM ev",
		"SELECT * FROM a, LATERAL (SELECT 1) q",
		"SELECT SUM(n) OVER w FROM t WINDOW w AS (ORDER BY id), w2 AS (w)",
		"SELECT SUM(n) OVER w FROM t window\n w\tas(ORDER BY id)",
		// In a string, in a comment, inside a longer word.
		"SELECT id FROM ev WHERE note = 'at' /* at */",
		"SELECT format, created_at, at_, _at, carefull, endpoint FROM ev",
		// Reserved on the source too: the keyword, or quoted.
		"SELECT id FROM ev a LEFT JOIN ev b USING (id) GROUP BY id ORDER BY id DESC",
		"SELECT id FROM t WHERE v IS NULL OR v IS NOT NULL",
	} {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
	// What the carve-outs let through is the copy's to refuse, as before: a
	// function it does not have.
	if got := Veto("SELECT id FROM t WHERE ISNULL(v)"); got != "" {
		t.Errorf("ISNULL(v): %q, want none (the copy has no such function and refuses it)", got)
	}
	// The statement this costs: the copy might have answered it.
	if stmt := "SELECT CAST(ts AT TIME ZONE 'UTC' AS DATE) FROM t"; Veto(stmt) != vetoCopyReserved {
		t.Errorf("Veto(%q) = %q; this test records it as kept on the source", stmt, Veto(stmt))
	}
}

func TestNameVeto(t *testing.T) {
	names := []string{"id", "at", "status", "note", "date", "año", "x y", "Text"}
	const str = "right before a string"
	for _, c := range []struct{ stmt, name string }{
		{"SELECT status 'Label' FROM ev", "status"},
		{"SELECT status'Label' FROM ev", "status"},
		{"SELECT STATUS /* c */ 'Label' FROM ev", "status"},
		{"SELECT e.status 'Label' FROM ev e", "status"},
		{"SELECT id, note\n'N' FROM ev", "note"},
		{"SELECT año 'Year' FROM ev", "año"},
		{"SELECT AÑO 'Year' FROM ev", "año"},
		{"SELECT at 'When' FROM ev", "at"},
		{"SELECT text 'Label' FROM ev", "text"},
		// Not right before it, or not a column, or a literal on both sides.
		{"SELECT status, 'Label' FROM ev WHERE status = 'x' AND note IN ('a') AND status LIKE 'b'", ""},
		{"SELECT status AS 'Label' FROM ev", ""},
		{"SELECT `status` 'Label' FROM ev", ""},
		{"SELECT other 'Label' FROM ev", ""},
		{"SELECT id FROM ev WHERE date >= date '2026-01-01'", ""},
		{"SELECT CASE status WHEN 'a' THEN 'b' ELSE 'c' END FROM ev", ""},
		{"SELECT status FROM ev WHERE at >= '2026-01-02'", ""},
	} {
		got := NameVeto(Shape(c.stmt), names)
		switch {
		case c.name == "" && got != "":
			t.Errorf("NameVeto(%q) = %q, want none", c.stmt, got)
		case c.name != "" && (!strings.Contains(got, str) || !strings.Contains(got, " "+c.name+" ")):
			t.Errorf("NameVeto(%q) = %q, want a reason holding %q and the name %s", c.stmt, got, str, c.name)
		}
		if via := ShapeOf(c.stmt).NameVeto(names); via != got {
			t.Errorf("StatementShape.NameVeto(%q) = %q, NameVeto %q", c.stmt, via, got)
		}
	}
	// No column, no question; and a table without such a column keeps no
	// statement back.
	for _, names := range [][]string{nil, {"x y", "a.b"}, {"id", "made"}, {"date", "TIME", "timestamp"}} {
		if got := NameVeto(Shape("SELECT id, status 'x', date '2026-01-01', time '10:00:00' FROM ev"), names); got != "" {
			t.Errorf("names %v: %q, want none", names, got)
		}
	}
}

// As TestColumnVeto_isLinear: the work grows with the statement and no
// faster, for both readings.
func TestCopyWords_areLinear(t *testing.T) {
	names := []string{"at", "end", "offset", "status"}
	shapes := map[string]func(n int) string{
		"qualified": func(n int) string { return "SELECT " + strings.Repeat("ev.at, ", n) + "1 FROM ev" },
		"quoted":    func(n int) string { return "SELECT " + strings.Repeat("`at`, `status`, ", n) + "1 FROM ev" },
		"cases": func(n int) string {
			return "SELECT " + strings.Repeat("case when a then ", n) + "1" + strings.Repeat(" end", n) + " FROM ev"
		},
		"spaces": func(n int) string {
			return "SELECT " + strings.Repeat("status"+strings.Repeat(" ", 20)+", cast"+strings.Repeat(" ", 20)+"(1 AS x), ", n) + "1 FROM ev LIMIT 1 OFFSET 2"
		},
		"aliases":    func(n int) string { return "SELECT " + strings.Repeat("1 AS at, ", n) + "1 FROM ev" },
		"long words": func(n int) string { return "SELECT " + strings.Repeat("a", 10*n) + ", ev.status FROM ev" },
		"strings":    func(n int) string { return "SELECT " + strings.Repeat("status = 'x', ", n) + "1 FROM ev" },
	}
	for name, build := range shapes {
		for kind, work := range map[string]func(string) (string, int){
			"reserved": copyReservedWork,
			"names":    func(shape string) (string, int) { return nameVetoWork(shape, names) },
		} {
			why, small := work(Shape(build(2000)))
			if why != "" {
				t.Errorf("%s, %s: kept back as %q, want none", name, kind, why)
			}
			_, large := work(Shape(build(8000)))
			ratio := float64(large) / float64(small)
			t.Logf("%-10s %-8s %9d steps at 2,000, %9d at 8,000: x%.2f", name, kind, small, large, ratio)
			if small == 0 || ratio > 4.5 {
				t.Errorf("%s, %s: %d steps at 2,000 and %d at 8,000 (x%.2f): the work grows faster than the text", name, kind, small, large, ratio)
			}
		}
	}
}

// The user and the role the statement runs as, written without parentheses
// (or, for the role, with them): MySQL's and MariaDB's on the source
// (root@localhost; NONE or NULL for the role), DuckDB's own on the copy
// (duckdb). Both answer. Measured on MySQL 8.4.9, MariaDB 11.4 and the copy.
// With parentheses current_user() was kept back already.
func TestVeto_currentUserWithoutParentheses(t *testing.T) {
	for _, stmt := range []string{
		"SELECT current_user FROM t",
		"SELECT id, CURRENT_USER FROM t",
		"SELECT current_role FROM t",
		"SELECT current_role() FROM t",
		"SELECT id FROM t WHERE owner = current_user",
	} {
		if got := Veto(stmt); got != vetoCurrentUser {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, vetoCurrentUser)
		}
	}
	for _, stmt := range []string{
		"SELECT `current_user`, t.current_role, current_users, my_current_user FROM t",
	} {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
}
