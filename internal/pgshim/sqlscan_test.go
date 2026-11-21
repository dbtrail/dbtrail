package pgshim

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// scanShape writes a scan as kinds and texts, to compare with.
func scanShape(q string) string {
	toks, why := scanSQL(q)
	names := map[scanKind]string{scWord: "w", scQuoted: "q", scString: "s", scEscape: "e", scPrefix: "p", scUnicode: "u", scDollar: "d", scNumber: "n", scParam: "$", scOp: "o"}
	var parts []string
	for _, t := range toks {
		parts = append(parts, names[t.kind]+":"+t.text)
	}
	if why != "" {
		parts = append(parts, "!"+why)
	}
	return strings.Join(parts, " ")
}

// TestScanSQLCutsAsPostgreSQL: where comments, strings, quoted names and
// dollar quotes end, as PostgreSQL's scanner ends them.
func TestScanSQLCutsAsPostgreSQL(t *testing.T) {
	for _, c := range []struct{ q, want string }{
		// A -- comment ends at a line feed and at a carriage return.
		{"a --x\nb", "w:a w:b"},
		{"a --x\rb", "w:a w:b"},
		{"a --x\r\nb", "w:a w:b"},
		{"a --x", "w:a"},
		// Block comments nest.
		{"a /* x /* y */ z */ b", "w:a w:b"},
		{"a /* x", "w:a !a comment that is not closed"},
		// An operator ends where a comment starts.
		{"a +-- x\nb", "w:a o:+ w:b"},
		{"a */* x */ b", "w:a o:* w:b"},
		// A doubled quote is a quote; a backslash is a character.
		{`'a''b' c`, `s:'a''b' w:c`},
		{`'a\' c`, `s:'a\' w:c`},
		// E starts an escape string only as a token of its own.
		{`e'a\'b' c`, `e:e'a\'b' w:c`},
		{`E'a\\' c`, `e:E'a\\' w:c`},
		{`xe'a\' c`, `w:xe s:'a\' w:c`},
		{`ae'a\' c`, `w:ae s:'a\' w:c`},
		{`a_e'a\' c`, `w:a_e s:'a\' w:c`},
		{`é'a\' c`, `w:é s:'a\' w:c`},
		{`1e'a' c`, `!a number run into a string`},
		{`$1e'a' c`, `!a parameter run into a string`},
		// A string goes on in a second piece a line below, read as the first.
		{"'a'\n'b' c", "s:'a'\n'b' w:c"},
		{"'a' --x\n 'b' c", "s:'a' --x\n 'b' w:c"},
		{"'a' 'b' c", "s:'a' s:'b' w:c"},
		{"e'a'\n'b\\'c' d", "e:e'a'\n'b\\'c' w:d"},
		// B, X and N strings; U& strings and names.
		{`b'01' x'ff' n'a' c`, `p:b'01' p:x'ff' p:n'a' w:c`},
		{`u&'a' U&"b" c`, `u:u&'a' u:U&"b" w:c`},
		{`menu&4`, `w:menu o:& n:4`},
		// A dollar quote starts only where a token can.
		{`$$a'b$$ c`, `d:$$a'b$$ w:c`},
		{`$t$a $u$ b$t$ c`, `d:$t$a $u$ b$t$ w:c`},
		{`a$t$ b $t$ c`, `w:a$t$ w:b !a dollar quote that is not closed`},
		{`a $ b`, `w:a !a dollar sign that starts neither a parameter nor a dollar quote`},
		{`1$$a$$ c`, `n:1 d:$$a$$ w:c`},
		{`$1$$a$$ c`, `$:$1 d:$$a$$ w:c`},
		{`$$a`, `!a dollar quote that is not closed`},
		{`$1 $2`, `$:$1 $:$2`},
		// Quoted names.
		{`"a""b" c`, `q:a"b w:c`},
		{`"" c`, `!an empty quoted name`},
		{`"a`, `!a quoted name that is not closed`},
		{`"a--b" c`, `q:a--b w:c`},
		// Names fold A to Z only.
		{`SeLeCt É`, `w:select w:É`},
		// Numbers.
		{`1.5e3 .5 1..2 0x1F 1_000`, `n:1.5e3 n:.5 n:1 o:. o:. n:2 n:0x1F n:1_000`},
		// What is not a character of SQL.
		{"a \v b", `w:a w:b`},
		{"e'a'\n\v'b' c", `!a string that is not closed`},
		{"a \x01 b", `w:a !the character '\x01' outside a string`},
		{`a \ b`, `w:a !the character '\\' outside a string`},
		{`'a`, `!a string that is not closed`},
	} {
		if got := scanShape(c.q); got != c.want {
			t.Errorf("%q\n got  %s\n want %s", c.q, got, c.want)
		}
	}
}

// TestReadersAgreeOnGeneratedTexts feeds the port's readers many texts made
// of the pieces they have to agree on (comments, strings of every kind,
// dollar quotes, quoted names, line ends) and holds them to one another: a
// text the routing reader or the session reader takes has, on the shared
// scanner, the same number of statements and the same first word.
func TestReadersAgreeOnGeneratedTexts(t *testing.T) {
	pieces := []string{
		"select", "begin", "x", "e", "E", "xe", "u", "1", "$1", ";", ";", " ", " ", "\n", "\r", "\r\n", "\t",
		"--", "-- c", "/*", "*/", "/* c */", "'", "'", "''", `\`, `\'`, "e'", "E'", "b'", "u&'", `u&"`, `"`, `""`,
		"$$", "$t$", "$", "a$", "(", ")", ",", ".", "+", "-", "*", "/", "=", "read", "write", "only", "commit", "é",
		"with", "with w as (select 1)", "delete", "update", "insert", "explain", "set", `"delete"`,
	}
	// A small generator of its own, so the texts are the same on every run.
	seed := uint64(0x9E3779B97F4A7C15)
	next := func(n int) int {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		return int(seed % uint64(n))
	}
	firstOf := func(toks []scanTok) (int, string) {
		sts := scanStatements(toks)
		if len(sts) == 0 {
			return 0, ""
		}
		st := sts[0]
		for len(st) > 0 && st[0].isOp("(") {
			st = st[1:]
		}
		if len(st) == 0 || st[0].kind != scWord {
			return len(sts), ""
		}
		return len(sts), st[0].text
	}
	for n := 0; n < 200000; n++ {
		var b strings.Builder
		for k := 2 + next(9); k > 0; k-- {
			b.WriteString(pieces[next(len(pieces))])
		}
		q := b.String()
		toks, why := scanSQL(q)
		count, first := firstOf(toks)

		// The routing reader: what it takes whole, the scanner reads the same.
		if rt, rwhy := lexSQL(q); rwhy == "" {
			if why == "an empty quoted name" {
				// PostgreSQL refuses it wherever it stands; nothing to compare.
				continue
			}
			if why != "" {
				t.Fatalf("%q: the routing reader takes it, the scanner does not (%s)", q, why)
			}
			rcount, started := 0, false
			for _, tok := range rt {
				if tok.kind == rtOp && tok.text == ";" {
					started = false
					continue
				}
				if !started {
					rcount, started = rcount+1, true
				}
			}
			if rcount != count {
				t.Fatalf("%q: the routing reader counts %d statements, the scanner %d", q, rcount, count)
			}
			if w := firstWord(rt); count > 0 && rt[0].kind != rtOp && w != first {
				t.Fatalf("%q: the routing reader's first word is %q, the scanner's %q", q, w, first)
			}
		}

		// The session reader: a statement it answers is one statement, whose
		// first word the scanner reads the same, and the text it sends the
		// source for it holds nothing of the client's.
		if cmd, ok, _ := parseSessionCmd(q); ok {
			if why != "" || count != 1 {
				t.Fatalf("%q: read as a session statement, and the scanner reads %d statements (%s)", q, count, why)
			}
			if cmd.kind == cmdBegin || cmd.kind == cmdCommit || cmd.kind == cmdRollback || cmd.kind == cmdSetTx {
				if first != "begin" && first != "start" && first != "commit" && first != "end" && first != "rollback" && first != "abort" && first != "set" {
					t.Fatalf("%q: read as transaction control, first word %q", q, first)
				}
				text, perr := cmd.sourceText()
				if perr == nil && strings.Contains(strings.ToUpper(text), "WRITE") {
					t.Fatalf("%q: the port would send %q", q, text)
				}
			}
		}

		// The screen: a text sent on was read whole, and every statement of it
		// begins with a word that reads.
		if forwardRefusal(q) == nil {
			if why != "" {
				t.Fatalf("%q: sent on, and not read whole (%s)", q, why)
			}
			for _, st := range scanStatements(toks) {
				for len(st) > 0 && st[0].isOp("(") {
					st = st[1:]
				}
				if len(st) == 0 {
					continue
				}
				if st[0].kind != scWord || !fwdReads[st[0].text] && st[0].text != "rollback" {
					t.Fatalf("%q: sent on with a statement that begins %q", q, st[0].text)
				}
				if v := st[0].text; v == "with" || v == "explain" || v == "declare" {
					depth := 0
					for k, tok := range st {
						if tok.isOp("(") {
							depth++
						} else if tok.isOp(")") {
							depth--
						}
						forUpdate := tok.isWord("update") && k > 0 && (st[k-1].isWord("for") || st[k-1].isWord("key"))
						if depth <= 0 && tok.kind == scWord && fwdWrites[tok.text] && !forUpdate {
							t.Fatalf("%q: sent on with %s outside the parentheses of a %s", q, tok.text, v)
						}
					}
				}
			}
		}
	}
}

// TestSourceTextIsThePortsOwn: what the port sends the source for a
// transaction statement is written from the modes it read, whatever else the
// client's text held.
func TestSourceTextIsThePortsOwn(t *testing.T) {
	for _, c := range []struct{ q, want string }{
		{"BEGIN", "BEGIN READ ONLY"},
		{"begin work", "BEGIN READ ONLY"},
		{"BEGIN -- c\n", "BEGIN READ ONLY"},
		{"BEGIN -- c\r", "BEGIN READ ONLY"},
		{"BEGIN /* c */ ;", "BEGIN READ ONLY"},
		{"START TRANSACTION READ ONLY", "BEGIN READ ONLY"},
		{"BEGIN ISOLATION LEVEL READ COMMITTED, READ ONLY DEFERRABLE", "BEGIN ISOLATION LEVEL READ COMMITTED, READ ONLY"},
		{"BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", "BEGIN ISOLATION LEVEL READ UNCOMMITTED, READ ONLY"},
		{"SET TRANSACTION READ ONLY", "SET TRANSACTION READ ONLY"},
		{"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"},
		{"COMMIT", "COMMIT"},
		{"END WORK", "COMMIT"},
		{"commit -- c\r", "COMMIT"},
		{"ROLLBACK", "ROLLBACK"},
		{"ABORT TRANSACTION", "ROLLBACK"},
	} {
		cmd, ok, _ := parseSessionCmd(c.q)
		if !ok {
			t.Errorf("%q: not read as a session statement", c.q)
			continue
		}
		got, perr := cmd.sourceText()
		if perr != nil || got != c.want {
			t.Errorf("%q: sends %q (%v), want %q", c.q, got, perr, c.want)
		}
	}
	// Modes the port does not keep are never written.
	for _, q := range []string{
		"BEGIN READ WRITE", "START TRANSACTION READ WRITE", "BEGIN ISOLATION LEVEL SERIALIZABLE", "SET TRANSACTION READ WRITE",
		"BEGIN ISOLATION LEVEL REPEATABLE READ",
	} {
		cmd, ok, _ := parseSessionCmd(q)
		if !ok {
			t.Errorf("%q: not read as a session statement", q)
			continue
		}
		if text, perr := cmd.sourceText(); perr == nil {
			t.Errorf("%q: the port would send %q", q, text)
		}
	}
	// What follows a comment's carriage return is part of the statement: the
	// session reader sees it, as PostgreSQL does.
	for _, q := range []string{"BEGIN --\rREAD WRITE", "BEGIN -- c\r READ WRITE", "BEGIN --\r\nREAD WRITE", "BEGIN --\nREAD WRITE", "START TRANSACTION --\rREAD WRITE",
		"SET TRANSACTION --\rREAD WRITE"} {
		cmd, ok, _ := parseSessionCmd(q)
		if !ok || !cmd.modes.readWrite {
			t.Errorf("%q: READ WRITE was not read (ok %v)", q, ok)
			continue
		}
		if checkModes(cmd.modes) == nil {
			t.Errorf("%q: not refused", q)
		}
	}
	for _, q := range []string{"BEGIN --\rREAD WRITE; SELECT 1", "COMMIT --\rAND CHAIN", "BEGIN --\r; SELECT 1", "ROLLBACK --\r AND CHAIN"} {
		if _, ok, _ := parseSessionCmd(q); ok {
			t.Errorf("%q: read as one session statement", q)
		}
		if forwardRefusal(q) == nil {
			t.Errorf("%q: not refused by the screen", q)
		}
	}
}

// TestForwardScreenReadsAsPostgreSQL: what a comment's carriage return, a
// name that ends like a dollar tag or a name that ends in e used to hide from
// the screen is read, and refused; and the reads beside them still go.
func TestForwardScreenReadsAsPostgreSQL(t *testing.T) {
	refused := []string{
		// After a comment closed by a carriage return.
		"SELECT 1 --\r; INSERT INTO t VALUES (1)",
		"SELECT 1 -- c\r; DELETE FROM t",
		"SELECT 1 --\r\n; DROP TABLE t",
		"SELECT --\r set_config('a', 'b', false)",
		"SELECT 1 --\r; COMMIT",
		// Between two names that end like a dollar tag.
		"SELECT a$t$ FROM u; INSERT INTO t VALUES (1); SELECT b$t$ FROM u",
		"SELECT a$$, set_config('a', 'b', false), b$$ FROM u",
		// After a quote that follows a name ending in e.
		`SELECT 1 AS mode'\'; INSERT INTO t VALUES (1) --'`,
		`SELECT typename'\', set_config('a', 'b', false) --'`,
		// A string in pieces is read as its first piece is.
		"SELECT e'a'\n'\\' , ' ; INSERT INTO t VALUES (1) --'",
		// Texts that cannot be read with certainty.
		"SELECT 1INTO x FROM t", "SELECT * FROM t LIMIT 1FOR UPDATE", "SELECT $1abc", "SELECT 10abc", "SELECT 0x1Fg",
		"SELECT 1e'a'", "SELECT 'a", "SELECT /* a", `SELECT "a`, "SELECT $t$ a", "SELECT a \x01 b", "SELECT 1 $ 2",
		// Writes inside what begins as a read.
		"WITH w AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM w",
		"WITH w AS (UPDATE t SET v = 1 RETURNING id) SELECT * FROM w",
		"WITH w AS (DELETE FROM t RETURNING id) SELECT * FROM w",
		"WITH w AS (MERGE INTO t USING u ON true WHEN MATCHED THEN DELETE) SELECT 1",
		"WITH w AS (SELECT 1) DELETE FROM t", "WITH w AS (SELECT 1) UPDATE t SET v = 1", "WITH w AS (SELECT 1) INSERT INTO t SELECT * FROM w",
		"WITH w AS (SELECT 1) MERGE INTO t USING w ON true WHEN MATCHED THEN DELETE", "EXPLAIN ANALYZE WITH w AS (SELECT 1) DELETE FROM t",
		"DECLARE c CURSOR FOR WITH w AS (SELECT 1) DELETE FROM t RETURNING id", "ROLLBACK TO a b", "ROLLBACK TO a, pg_sleep(1)", "ROLLBACK TO 'a'",
		"SELECT crosstab('select 1')", "SELECT * FROM connectby('t', 'a', 'b', '1', 0)",
		// A write after WITH's list, whatever stands between the two.
		"WITH RECURSIVE w(a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM w WHERE a < 2) SEARCH DEPTH FIRST BY a SET o DELETE FROM t",
		"WITH RECURSIVE w(a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM w WHERE a < 2) SEARCH BREADTH FIRST BY a SET o INSERT INTO t SELECT a FROM w",
		"WITH RECURSIVE w(a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM w WHERE a < 2) CYCLE a SET c USING p UPDATE t SET v = 1",
		"WITH RECURSIVE w(a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM w WHERE a < 2) CYCLE a SET c TO 1 DEFAULT 0 USING p DELETE FROM t",
		"WITH RECURSIVE w(a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM w WHERE a < 2) SEARCH DEPTH FIRST BY a SET o CYCLE a SET c USING p MERGE INTO t USING w ON true WHEN MATCHED THEN DELETE",
		"WITH w AS (SELECT 1) /* c */ DELETE FROM t", "WITH w AS (SELECT 1) --\r\nDELETE FROM t", "WITH w AS (SELECT 1), x AS (SELECT 2) UPDATE t SET v = 1",
		"EXPLAIN (ANALYZE) WITH RECURSIVE w(a) AS (SELECT 1) SEARCH DEPTH FIRST BY a SET o DELETE FROM t",
		"DECLARE c CURSOR FOR WITH RECURSIVE w(a) AS (SELECT 1) CYCLE a SET c USING p DELETE FROM t RETURNING id",
		// The keyword without quotes is the keyword, wherever it stands.
		"WITH w AS (SELECT 1) SELECT 1 AS delete FROM w", "WITH w AS (SELECT 1) SELECT update FROM w",
		"EXPLAIN ANALYZE INSERT INTO t VALUES (1)",
		"EXPLAIN (ANALYZE, BUFFERS) DELETE FROM t",
		"EXPLAIN ANALYZE VERBOSE UPDATE t SET v = 1",
		"EXPLAIN CREATE TABLE x AS SELECT 1",
		"EXPLAIN",
		"SELECT * INTO x FROM t",
		"SELECT * INTO TEMP x FROM t",
		"SELECT id INTO TEMPORARY TABLE x FROM t",
		"SELECT * FROM t FOR UPDATE",
		"SELECT * FROM t FOR NO KEY UPDATE",
		"SELECT * FROM t FOR SHARE SKIP LOCKED",
		"SELECT * FROM t FOR KEY SHARE",
		// Functions that act beyond a read, however they are written.
		"SELECT pg_terminate_backend(1)", "SELECT pg_cancel_backend(1)", "SELECT pg_catalog.pg_reload_conf()", `SELECT "pg_switch_wal"()`,
		"SELECT pg_create_logical_replication_slot('a', 'b')", "SELECT pg_drop_replication_slot('a')", "SELECT pg_replication_origin_create('a')",
		"SELECT pg_logical_emit_message(true, 'a', 'b')", "SELECT lo_create(0)", "SELECT lo_unlink(1)", "SELECT lowrite(1, 'a')", "SELECT lo_import('/a')",
		"SELECT pg_read_file('a')", "SELECT pg_read_binary_file('a')", "SELECT pg_ls_dir('.')", "SELECT pg_stat_file('a')", "SELECT pg_stat_reset()",
		"SELECT pg_stat_reset_shared('bgwriter')", "SELECT pg_stat_statements_reset()", "SELECT nextval('s')", "SELECT setval('s', 1)",
		"SELECT query_to_xml('insert into t values (1)', true, false, '')", "SELECT query_to_xml(v, true, false, '') FROM t",
		"SELECT query_to_xml('select pg_notify(''a'', ''b'')', true, false, '')", "SELECT query_to_xml(e'select 1', true, false, '')",
		"SELECT query_to_xml('select 1' || v, true, false, '') FROM t", "SELECT query_to_xml('select 1 --' || chr(13) || 'x', true, false, '')",
		"SELECT query_to_xml('select query_to_xml(''delete from t'', true, false, '''')', true, false, '')",
		"SELECT query_to_xml($$select 1$$, true, false, '')", "SELECT ts_stat('select set_config(''a'', ''b'', false)')", "SELECT ts_rewrite(q, 'select 1')",
		"SELECT query_to_xmlschema(v, true, false, '') FROM t", "SELECT query_to_xml_and_xmlschema(v, true, false, '') FROM t",
		"SELECT pg_terminate_backend /* c */ (1)",
		"SELECT pg_terminate_backend\n(1)", "SELECT PG_TERMINATE_BACKEND(1)", "SELECT set_config --\r\n('a', 'b', false)",
		"SELECT pg_advisory_lock(1)", "SELECT pg_notify('a', 'b')", "SELECT dblink('a', 'b')",
		"SELECT txid_current()", "SELECT pg_current_xact_id()",
	}
	for _, q := range refused {
		if forwardRefusal(q) == nil {
			t.Errorf("not refused: %q", q)
		}
	}
	sent := []string{
		"SELECT 1 --\r", "SELECT 1 -- c\r\n", "SELECT a$t$ FROM u", "SELECT a$t$, b$t$ FROM u", `SELECT 1 AS mode`, `SELECT typename, 'a' FROM u`,
		`SELECT 'it''s' , e'a\'b', $$a;b$$, $t$a$t$ FROM u`, "SELECT 'a'\n'b'", "SELECT b'01', x'ff'", "SELECT 0x1F, 1_000, 1.5e3",
		"SELECT * FROM t WHERE v = 'insert into'", "SELECT substring(v for 2) FROM t", "SELECT count(update), max(delete) FROM t",
		"SELECT (update + 1), (insert) FROM t", "WITH w AS (SELECT 1) SELECT * FROM w", "EXPLAIN SELECT 1", "EXPLAIN ANALYZE SELECT 1",
		"EXPLAIN (ANALYZE, FORMAT JSON) SELECT 1", "EXPLAIN (FORMAT JSON) WITH w AS (SELECT 1) SELECT * FROM w", "EXPLAIN VERBOSE (SELECT 1)",
		"DECLARE c CURSOR FOR SELECT 1", "FETCH 1 FROM c", "CLOSE c", "SAVEPOINT a", "RELEASE a", "ROLLBACK TO a", `ROLLBACK TO SAVEPOINT "A"`, "ROLLBACK WORK TO a", "rollback /* c */ transaction to savepoint a -- c\r", "ROLLBACK TO a;",
		"SELECT 1; SELECT 2", "(SELECT 1) UNION (SELECT 2)", "VALUES (1)", "TABLE t", "SHOW server_version",
		"SELECT pg_size_pretty(pg_relation_size('t')), lower(v), log(2), currval('s'), pg_sleep(0), txid_current_snapshot(), txid_current_if_assigned() FROM t",
		"SELECT pg_stat_get_numscans(1), pg_backend_pid(), pg_is_in_recovery(), pg_current_wal_lsn()",
		"SELECT lo_count, nextval FROM t", "SELECT é FROM t",
		"WITH RECURSIVE w(a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM w WHERE a < 2) SEARCH DEPTH FIRST BY a SET o SELECT * FROM w",
		"WITH RECURSIVE w(a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM w WHERE a < 2) CYCLE a SET c USING p SELECT * FROM w",
		`WITH w AS (SELECT 1 AS "delete") SELECT "delete", "update" FROM w`, "WITH w AS (SELECT count(update) FROM t) SELECT * FROM w",
		"WITH w AS (SELECT 1) TABLE w", "WITH w AS (SELECT 1) VALUES (1)", "EXPLAIN WITH w AS (SELECT 1) SELECT * FROM w",
		"SELECT query_to_xml('select counter from t', true, false, '')", "SELECT ts_stat('select v from t')",
		"SELECT query_to_xml('select query_to_xml(''select 1'', true, false, '''')', true, false, '')",
		"SELECT table_to_xml('t', true, false, '')",
	}
	for _, q := range sent {
		if perr := forwardRefusal(q); perr != nil {
			t.Errorf("refused: %q: %s", q, perr.msg)
		}
	}
}

// TestBeginIsThePortsOwnAndVerified: the block the client opens is opened on
// the source with the port's text, READ ONLY in so many words, whatever the
// client wrote around its BEGIN; and a source that answers that the block is
// not read-only gets nothing of the client's.
func TestBeginIsThePortsOwnAndVerified(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src}
	c := costPortConn(t, ctx, l)
	for _, begin := range []string{"BEGIN", "begin /* c */ work -- c\r", "START TRANSACTION ISOLATION LEVEL READ COMMITTED"} {
		if _, err := c.Exec(ctx, begin).ReadAll(); err != nil {
			t.Fatalf("%q: %v", begin, err)
		}
		if _, err := c.Exec(ctx, "COMMIT -- c\r").ReadAll(); err != nil {
			t.Fatalf("COMMIT after %q: %v", begin, err)
		}
	}
	src.mu.Lock()
	begins := append([]string(nil), src.begins...)
	src.mu.Unlock()
	want := []string{
		"BEGIN READ ONLY; SHOW transaction_read_only",
		"BEGIN READ ONLY; SHOW transaction_read_only",
		"BEGIN ISOLATION LEVEL READ COMMITTED, READ ONLY; SHOW transaction_read_only",
	}
	if strings.Join(begins, "\n") != strings.Join(want, "\n") {
		t.Errorf("the source was sent\n %q\nwant\n %q", begins, want)
	}

	rw := startFakeSource(t, costPlans)
	rw.blockReadWrite = true
	l2 := &costLogin{src: rw}
	d := costPortConn(t, ctx, l2)
	_, err := d.Exec(ctx, "BEGIN").ReadAll()
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "08006" || !strings.Contains(pe.Message, "not read-only") {
		t.Fatalf("BEGIN on a source whose block is not read-only: %v, want 08006", err)
	}
	if _, err := d.Exec(ctx, "SELECT sum(n) FROM big").ReadAll(); err == nil {
		// Outside a block now: the copy may answer; what matters is below.
		_ = err
	}
	if ran := rw.ran(); len(ran) != 0 {
		t.Errorf("the source ran %q in a block that was not read-only", ran)
	}
}

// TestReadsCatalogSeesANameWrittenAgainstAWord: a quoted catalog name with no
// blank around it is still the catalog's.
func TestReadsCatalogSeesANameWrittenAgainstAWord(t *testing.T) {
	for _, q := range []string{
		`SELECT * FROM"pg_class"`, `SELECT * FROM "pg_class"x`, `SELECT * FROM pg_catalog."pg_class"`, `SELECT * FROM "pg_catalog"."pg_class"`,
		`SELECT * FROM U&"pg_class"`, "SELECT * FROM pg_class --\rWHERE true", `SELECT * FROM/**/"pg_class"`,
	} {
		if !readsCatalog(q) {
			t.Errorf("not read as a catalog statement: %q", q)
		}
	}
	for _, q := range []string{`SELECT 'pg_class'`, `SELECT * FROM t -- pg_class`, `SELECT * FROM myschema.pg_class`, `SELECT $$pg_class$$`} {
		if readsCatalog(q) {
			t.Errorf("read as a catalog statement: %q", q)
		}
	}
}
