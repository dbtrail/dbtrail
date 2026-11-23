//go:build integration

package pgcompare

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// char(n) columns (#3068), measured on PostgreSQL 16 and on the copy.
//
// The copy holds a char(n) as text with its padding and compares the
// padding; PostgreSQL ignores it. Every statement below runs on both, and
// each verdict is pinned: the DIFFERENT ones are the wrong answers the port
// gave before, and the test holds the port's screen (pgshim.CopyScreenFor,
// with the type the source declares for each column) to sending every one of
// them to the source. The statements that stay on the copy are pinned EQUAL,
// so the screen is not held to that by sending everything away; and the same
// statements on a table of plain text are decided as they were before.

const (
	chCodes = "pgchr_codes"
	chPlain = "pgchr_plain"
	chPub   = "pgchr_pub"
	chSlot  = "pgchr_slot"
)

func charSeed() []string {
	rows := `(1, 'ab', 'apple', 1), (2, 'AB', 'Apple', 1), (3, 'abcde', 'APPLE', 2),
		(4, 'a', 'banana', 2), (5, '', 'a_b', 3), (6, NULL, 'a%b', 3),
		(7, 'ab', 'ab', 3), (8, 'x', '', 4), (9, NULL, NULL, 4),
		(10, 'é', 'éclair', 5), (11, ' l', ' lead', 5), (12, 't ', 'trail ', 5),
		(13, 'a b', 'a b', 6), (14, '10', '10', 6), (15, '9', '9', 6),
		(16, 'ab', 'apple', 7), (17, 'ß', 'ß', 7), (18, 'ss', 'ss', 7)`
	return []string{
		// c is the char(n); u a bpchar without a length, which keeps the
		// blanks each value was written with; arr an array of char(n).
		`CREATE TABLE ` + chCodes + ` (id int PRIMARY KEY, c char(5), t text, k int, u bpchar, arr char(3)[])`,
		`INSERT INTO ` + chCodes + ` (id, c, t, k) VALUES ` + rows,
		`UPDATE ` + chCodes + ` SET u = c, arr = ARRAY[c::char(3), 'x']`,
		`UPDATE ` + chCodes + ` SET u = 'ab  ' WHERE id = 7`,
		// The same rows in a table of plain text.
		`CREATE TABLE ` + chPlain + ` (id int PRIMARY KEY, c text, t text, k int)`,
		`INSERT INTO ` + chPlain + ` VALUES ` + rows,
		`CREATE PUBLICATION ` + chPub + ` FOR TABLE ` + chCodes + `, ` + chPlain,
	}
}

// charCase is one statement, what the two sides give for it, and whether the
// port lets the copy answer it.
type charCase struct {
	stmt    string
	verdict sqlcompare.Verdict
	// onCopy: the screen leaves it to the copy. Only an EQUAL statement may
	// be.
	onCopy bool
}

func charCases() []charCase {
	x, p := chCodes, chPlain
	// differs: the copy answers otherwise, so the source answers.
	differs := func(stmt string) charCase { return charCase{stmt, sqlcompare.Different, false} }
	// sent: the two sides happen to agree, and the screen still sends it to
	// the source: the rule is by where the column stands, not by the values
	// a table holds today.
	sent := func(stmt string) charCase { return charCase{stmt, sqlcompare.Equal, false} }
	// stays: the same answer, left to the copy.
	stays := func(stmt string) charCase { return charCase{stmt, sqlcompare.Equal, true} }
	return []charCase{
		// The statements of the issue.
		differs(`SELECT id FROM ` + x + ` WHERE c = 'ab' ORDER BY id`),
		differs(`SELECT id FROM ` + x + ` WHERE c <> 'ab' ORDER BY id`),
		differs(`SELECT id, length(c) FROM ` + x + ` ORDER BY id`),
		differs(`SELECT id, c || 'x' FROM ` + x + ` ORDER BY id`),
		differs(`SELECT id FROM ` + x + ` WHERE c = t ORDER BY id`),
		differs(`SELECT id FROM ` + x + ` WHERE c IN ('ab', 'x') ORDER BY id`),
		differs(`SELECT a.id, b.id FROM ` + x + ` a JOIN ` + x + ` b ON a.c = b.t ORDER BY a.id, b.id`),
		differs(`SELECT DISTINCT length(c) FROM ` + x),
		differs(`SELECT t FROM ` + x + ` INTERSECT SELECT c FROM ` + x),
		differs(`SELECT c FROM ` + x + ` EXCEPT SELECT t FROM ` + x),
		// Around them.
		differs(`SELECT id FROM ` + x + ` WHERE c = '' ORDER BY id`),
		differs(`SELECT id FROM ` + x + ` WHERE c IS DISTINCT FROM 'ab' ORDER BY id`),
		differs(`SELECT id FROM ` + x + ` WHERE c BETWEEN 'ab' AND 'ab' ORDER BY id`),
		differs(`SELECT id, char_length(c) FROM ` + x + ` ORDER BY id`),
		// The copy has no octet_length of text: refused there, and the
		// source's either way.
		{`SELECT id, octet_length(c) FROM ` + x + ` ORDER BY id`, sqlcompare.NotOnCopy, false},
		differs(`SELECT id, c::text || '|' FROM ` + x + ` ORDER BY id`),
		differs(`SELECT id, CASE WHEN c = 'ab' THEN 1 ELSE 0 END FROM ` + x + ` ORDER BY id`),
		differs(`SELECT a.id FROM ` + x + ` a JOIN ` + p + ` b ON a.c = b.c ORDER BY a.id`),
		differs(`SELECT a.id FROM ` + x + ` a JOIN ` + p + ` b USING (c) ORDER BY a.id`),
		differs(`SELECT c FROM ` + x + ` UNION SELECT t FROM ` + x),
		differs(`WITH y AS (SELECT id, c AS z FROM ` + x + `) SELECT id FROM y WHERE z = 'ab' ORDER BY id`),
		differs(`SELECT s.id FROM (SELECT * FROM ` + x + `) s WHERE s.c = 'ab' ORDER BY s.id`),
		differs(`SELECT id FROM ` + p + ` WHERE c IN (SELECT c FROM ` + x + `) ORDER BY id`),
		// A bpchar without a length: values PostgreSQL finds equal are two
		// on the copy.
		differs(`SELECT id FROM ` + x + ` WHERE u = 'ab' ORDER BY id`),
		differs(`SELECT DISTINCT u FROM ` + x),
		differs(`SELECT u, count(*) FROM ` + x + ` GROUP BY 1`),
		differs(`SELECT count(DISTINCT u) FROM ` + x),
		// An array of char(n).
		differs(`SELECT id FROM ` + x + ` WHERE arr[1] = 'ab' ORDER BY id`),
		// The same answer today, by the values these rows hold: sent anyway.
		sent(`SELECT id FROM ` + x + ` WHERE c = 'ab   ' ORDER BY id`),
		// concat() writes a char(n) with its blanks on PostgreSQL too.
		sent(`SELECT id, concat(c, '|') FROM ` + x + ` ORDER BY id`),
		sent(`SELECT id FROM ` + x + ` WHERE c LIKE 'ab%' ORDER BY id`),
		sent(`SELECT id FROM ` + x + ` WHERE c IS NULL ORDER BY id`),
		// Sorted, a char(n) is text: the source sorts it by its collation.
		sent(`SELECT c, count(*) FROM ` + x + ` GROUP BY c ORDER BY c`),
		// A whole key of GROUP BY (#3219): every value of a char(n) has the
		// same length, so both sides make the same groups.
		stays(`SELECT c, count(*) FROM ` + x + ` GROUP BY c`),
		stays(`SELECT count(*), sum(k) FROM ` + x + ` GROUP BY c`),
		stays(`SELECT a.c, count(*) AS n, sum(b.k) FROM ` + x + ` a JOIN ` + p + ` b ON b.id = a.id GROUP BY a.c`),
		stays(`SELECT c, k, count(*) FROM ` + x + ` WHERE k < 6 GROUP BY c, k HAVING count(*) >= 1`),
		// A bpchar without a length keeps each value's own blanks: two
		// groups on the copy where PostgreSQL makes one.
		differs(`SELECT u, count(*) FROM ` + x + ` GROUP BY u`),
		// The key used again, or inside an expression: the source's.
		sent(`SELECT c, count(*) FROM ` + x + ` GROUP BY c HAVING c IS NOT NULL`),
		sent(`SELECT count(DISTINCT c), count(c) FROM ` + x),
		// A plain select list: both sides write the padded value.
		stays(`SELECT id, c FROM ` + x + ` ORDER BY id`),
		stays(`SELECT a.id, a.c AS code FROM ` + x + ` a WHERE a.k > 2 ORDER BY a.id`),
		stays(`SELECT DISTINCT c FROM ` + x),
		stays(`SELECT c, count(*) FROM ` + x + ` GROUP BY 1`),
		stays(`SELECT * FROM ` + x + ` ORDER BY id`),
		stays(`SELECT id, u FROM ` + x + ` ORDER BY id`),
		stays(`SELECT id, arr FROM ` + x + ` ORDER BY id`),
		stays(`SELECT a.id, a.c, b.c FROM ` + x + ` a JOIN ` + p + ` b ON a.id = b.id ORDER BY a.id`),
		// Nothing of the padded columns.
		stays(`SELECT count(*) FROM ` + x),
		stays(`SELECT id FROM ` + x + ` WHERE t = 'ab' ORDER BY id`),
		stays(`SELECT id, length(t), t || 'x' FROM ` + x + ` ORDER BY id`),
	}
}

// charPlainCases are the statements of the issue on the table of plain
// text: all EQUAL, and decided by the screen as before #3068.
func charPlainCases() []string {
	p := chPlain
	return []string{
		`SELECT id FROM ` + p + ` WHERE c = 'ab' ORDER BY id`,
		`SELECT id FROM ` + p + ` WHERE c <> 'ab' ORDER BY id`,
		`SELECT id, length(c) FROM ` + p + ` ORDER BY id`,
		`SELECT id, c || 'x' FROM ` + p + ` ORDER BY id`,
		`SELECT t FROM ` + p + ` INTERSECT SELECT c FROM ` + p,
		`SELECT count(*) FROM (SELECT * FROM ` + p + `) s`,
		`WITH y AS (SELECT id, c AS z FROM ` + p + `) SELECT id FROM y WHERE z = 'ab' ORDER BY id`,
	}
}

func TestIntegrationPGCompareCharPadding3068(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+chPub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+chCodes+", "+chPlain)
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", chSlot)
	}
	dropAll()
	t.Cleanup(dropAll)
	for _, s := range charSeed() {
		if _, err := setup.Exec(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	outDir := t.TempDir()
	repl := baseDSN + "?replication=database"
	if strings.Contains(baseDSN, "?") {
		repl = baseDSN + "&replication=database"
	}
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: baseDSN, ReplDSN: repl, SlotName: chSlot, Publication: chPub,
		OutputDir: outDir, Compression: "none", Parallelism: 2,
	}); err != nil {
		t.Fatalf("pgbaseline.Run: %v", err)
	}
	snapshot := ""
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, ok := snapshotdir.ParseTime(e.Name()); ok && e.IsDir() {
			snapshot = filepath.Join(outDir, e.Name())
		}
	}
	if snapshot == "" {
		t.Fatalf("no snapshot directory in %s: %v", outDir, entries)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}})
	cp, err := OpenSnapshotCopy(ctx, runner, snapshot, "public")
	if err != nil {
		t.Fatalf("open the copy: %v", err)
	}
	cp.Rewrite = pgshim.RewriteForCopy
	src, err := OpenSource(ctx, baseDSN, "public")
	if err != nil {
		t.Fatalf("open the source: %v", err)
	}
	t.Cleanup(func() { src.Close(context.Background()) })

	// The port's catalog of the two tables: the copy's type of each column,
	// and the type the source declares for it. before are the copy's types
	// alone, which is all the screen knew before #3068.
	declared, _, err := src.Query(ctx, `SELECT c.relname, a.attname, a.atttypid::oid::int8, a.atttypmod FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relname IN ('`+chCodes+`', '`+chPlain+`') AND c.relnamespace = 'public'::regnamespace AND a.attnum > 0 AND NOT a.attisdropped`, 1000)
	if err != nil {
		t.Fatalf("the source's column types: %v", err)
	}
	type sourceType struct {
		oid uint32
		mod int32
	}
	onSource := map[[2]string]sourceType{}
	for _, r := range declared.Rows {
		oid, err1 := strconv.ParseUint(*r[2], 10, 32)
		mod, err2 := strconv.ParseInt(*r[3], 10, 32)
		if err1 != nil || err2 != nil {
			t.Fatalf("a column's type on the source: %v %v", err1, err2)
		}
		onSource[[2]string{*r[0], *r[1]}] = sourceType{uint32(oid), int32(mod)}
	}
	held, _, err := cp.Query(ctx, "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position", 1000)
	if err != nil {
		t.Fatalf("the copy's column types: %v", err)
	}
	var catalog []pgshim.CatalogTable
	before := map[string][]string{}
	for _, r := range held.Rows {
		table, col, typ := *r[0], *r[1], *r[2]
		st, ok := onSource[[2]string{table, col}]
		if !ok {
			t.Fatalf("the copy holds %s.%s, which the source does not declare", table, col)
		}
		if n := len(catalog); n == 0 || catalog[n-1].Name != table {
			catalog = append(catalog, pgshim.CatalogTable{Schema: "public", Name: table})
		}
		tb := &catalog[len(catalog)-1]
		tb.Columns = append(tb.Columns, pgshim.CatalogColumn{Name: col, Type: typ, SourceType: st.oid, SourceTypeMod: st.mod})
		before[strings.ToLower(col)] = append(before[strings.ToLower(col)], strings.ToUpper(typ))
		if col == "c" && table == chCodes && strings.ToUpper(typ) != "VARCHAR" {
			t.Fatalf("the copy holds the char(5) column as %s: the screen's marks assume plain text", typ)
		}
	}
	if len(catalog) != 2 {
		t.Fatalf("the copy's catalog has %d tables, want 2: %+v", len(catalog), catalog)
	}

	cases := charCases()
	stmts := make([]string, len(cases))
	for i, c := range cases {
		stmts[i] = c.stmt
	}
	rep, err := Run(ctx, src, cp, Options{NoRerun: true}, stmts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	onCopy := 0
	for i, r := range rep.Results {
		c := cases[i]
		veto := pgshim.CopyScreenFor(r.Statement, catalog)
		side := "copy"
		if veto != "" {
			side = "source (" + veto + ")"
		}
		line := fmt.Sprintf("%-10s %-6s -> %s\n    %s\n    detail: %s", r.Verdict, r.Kind, side, r.Statement, r.Detail)
		switch {
		case r.Verdict != c.verdict:
			t.Errorf("want %s, got:\n  %s", c.verdict, line)
		case r.Verdict != sqlcompare.Equal && veto == "":
			t.Errorf("the copy answers it otherwise than PostgreSQL, and the port lets the copy answer it:\n  %s", line)
		case c.onCopy && veto != "":
			t.Errorf("the same answer on both sides, and the port sends it to the source:\n  %s", line)
		case !c.onCopy && veto == "":
			t.Errorf("want it sent to the source, and the port lets the copy answer it:\n  %s", line)
		default:
			t.Log(line)
		}
		if veto == "" {
			onCopy++
		}
		// What the screen knew before: every DIFFERENT statement here that
		// it let through was a wrong answer of the port's.
		if r.Verdict == sqlcompare.Different && pgshim.CopyScreen(r.Statement, before) == "" {
			t.Logf("    before #3068 the port let the copy answer it")
		}
	}
	t.Logf("%d of %d statements are left to the copy", onCopy, len(cases))

	// The same statements on plain text: the same answer, and the screen
	// decides them as it did.
	plain := charPlainCases()
	rep, err = Run(ctx, src, cp, Options{NoRerun: true}, plain)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, r := range rep.Results {
		now, was := pgshim.CopyScreenFor(r.Statement, catalog), pgshim.CopyScreen(r.Statement, before)
		if now != was {
			t.Errorf("a table of plain text: the screen says %q, and said %q before: %s", now, was, r.Statement)
		}
		if r.Verdict != sqlcompare.Equal && now == "" {
			t.Errorf("a table of plain text: %s (%s) on the copy: %s\n    detail: %s", r.Verdict, r.Kind, r.Statement, r.Detail)
		}
		t.Logf("%-10s -> %q: %s", r.Verdict, now, r.Statement)
	}
}
