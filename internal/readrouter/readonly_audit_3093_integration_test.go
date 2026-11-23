//go:build integration

package readrouter

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #3093 against a real server: with the session marked read-only, the
// SOURCE refuses a write whatever its text is. Every statement here is sent
// straight down the forwarding path, with no screen in front, as a write
// would arrive if the screen had read it as something else.
func TestIntegrationForwarder_readOnlySession_3093(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	for _, q := range []string{
		"CREATE TABLE t (a INT NOT NULL PRIMARY KEY)",
		"INSERT INTO t VALUES (1),(2),(3)",
		"CREATE FUNCTION writes_a_row() RETURNS INT MODIFIES SQL DATA BEGIN INSERT INTO t SELECT max(a) + 100 FROM t; RETURN 1; END",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%.60s: %v", q, err)
		}
	}
	state := func() string {
		t.Helper()
		var rows, sum, tables int
		if err := db.QueryRow("SELECT count(*), coalesce(sum(a), 0) FROM t").Scan(&rows, &sum); err != nil {
			t.Fatalf("read the source: %v", err)
		}
		if err := db.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_schema = ?", name).Scan(&tables); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("rows %d sum %d tables %d", rows, sum, tables)
	}
	open := func(readOnly bool) *Forwarder {
		t.Helper()
		fw, err := NewForwarder(testutil.IntegrationDSN(name), config.SSL{Mode: "preferred"}, DefaultPolicy(), 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(fw.Close)
		fw.ReadOnlySession = readOnly
		return fw
	}
	ctx := context.Background()
	before := state()

	ro := open(true)
	refused := func(when, q string) {
		t.Helper()
		_, err := ro.Forward(ctx, q, &BufferSink{})
		var me *mysql.MyError
		if !errors.As(err, &me) || me.Code != mysql.ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION {
			t.Errorf("%s, %q: got %v, want the source's error 1792", when, q, err)
		}
		if after := state(); after != before {
			t.Fatalf("%s, %q changed the source on a read-only session: %s, was %s", when, q, after, before)
		}
	}
	run := func(q string) {
		t.Helper()
		if _, err := ro.Forward(ctx, q, &BufferSink{}); err != nil {
			t.Fatalf("%q: %v", q, err)
		}
	}
	writes := []string{
		"INSERT INTO t VALUES (9)",
		"UPDATE t SET a = a + 10",
		"DELETE FROM t",
		"REPLACE INTO t VALUES (1)",
		"INSERT INTO t SELECT 50",
		"WITH c AS (SELECT 1) DELETE FROM t",
		"/*!50000 DELETE FROM t */",
		"SELECT writes_a_row()",
		"DO writes_a_row()",
		"SET @a = writes_a_row()",
		"CREATE TABLE made_by_port (a INT)",
		"ALTER TABLE t ADD COLUMN b INT",
		"TRUNCATE TABLE t",
		"DROP TABLE t",
	}
	for _, q := range writes {
		refused("under autocommit", q)
	}
	run("BEGIN")
	refused("after BEGIN", "INSERT INTO t VALUES (9)")
	refused("after BEGIN", "SELECT writes_a_row()")
	run("ROLLBACK")
	run("START TRANSACTION WITH CONSISTENT SNAPSHOT")
	refused("after START TRANSACTION", "DELETE FROM t")
	run("COMMIT")
	run("SET autocommit = 0")
	refused("with autocommit off", "INSERT INTO t VALUES (9)")
	run("COMMIT")
	refused("with autocommit off, after COMMIT", "DELETE FROM t")
	run("SET autocommit = 1")
	refused("with autocommit back on", "INSERT INTO t VALUES (9)")

	// Reads are answered as before.
	var buf BufferSink
	if _, err := ro.Forward(ctx, "SELECT count(*) FROM t", &buf); err != nil || len(buf.Rows) != 1 {
		t.Fatalf("a read on the read-only session: %v, %d rows", err, len(buf.Rows))
	}

	// The control: the same statements land on a session that is not marked.
	rw := open(false)
	for _, q := range []string{"INSERT INTO t VALUES (9)", "SELECT writes_a_row()"} {
		was := state()
		if _, err := rw.Forward(ctx, q, &BufferSink{}); err != nil {
			t.Fatalf("%q on a session that is not read-only: %v", q, err)
		}
		if state() == was {
			t.Fatalf("%q did not change the source: the refusals above prove nothing", q)
		}
	}
}

// What the screen refuses so the mark stays: each of these, sent to a
// marked session, makes it writable again. If one stops doing so on some
// server, the screen's refusal of it is only caution; if a NEW way appears,
// it belongs here and in the screen.
func TestIntegrationReadOnlySession_whatTakesTheMarkBack_3093(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	if _, err := db.Exec("CREATE TABLE t (a INT NOT NULL PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i, undo := range []string{
		"START TRANSACTION READ WRITE",
		"SET SESSION TRANSACTION READ WRITE",
		"SET TRANSACTION READ WRITE",
		"SET SESSION transaction_read_only = 0",
		"SET @@transaction_read_only = 0",
	} {
		if ReadOnlyRefusal(undo) == "" {
			t.Errorf("the screen allows %q", undo)
		}
		fw, err := NewForwarder(testutil.IntegrationDSN(name), config.SSL{Mode: "preferred"}, DefaultPolicy(), 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		fw.ReadOnlySession = true
		if _, err := fw.Forward(ctx, undo, &BufferSink{}); err != nil {
			t.Fatalf("%q: %v", undo, err)
		}
		if _, err := fw.Forward(ctx, fmt.Sprintf("INSERT INTO t VALUES (%d)", i), &BufferSink{}); err != nil {
			t.Errorf("after %q the source still refused a write (%v): the list above is out of date", undo, err)
		}
		fw.Close()
	}
}

// The screen's reading against the server's, on generated text (#3093).
// Texts are put together from the pieces that change how a statement is
// read (comment openers, quotes, backslashes, separators, read keywords)
// around a write. Whatever the screen allows is run on a real server, with
// several statements per packet enabled (more than the port's own
// connection allows) and under each sql_mode and character set that changes
// the reading; nothing the screen allows may change the table. These
// sessions are NOT read-only: the screen is on its own here.
func TestIntegrationReadOnlyScreen_generatedTextAgainstTheServer_3093(t *testing.T) {
	admin, name := testutil.CreateTestDB(t)
	reset := func() {
		t.Helper()
		for _, q := range []string{"DROP TABLE IF EXISTS t", "DROP TABLE IF EXISTS u", "CREATE TABLE t (a INT NOT NULL PRIMARY KEY)", "INSERT INTO t VALUES (1),(2),(3)"} {
			if _, err := admin.Exec(q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	reset()
	const clean = "rows 3 sum 6 tables 1"

	ctx := context.Background()
	db, err := sql.Open("mysql", testutil.IntegrationDSN(name)+"&multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sessions := map[string][]string{
		"default":                           {"SET SESSION sql_mode = ''"},
		"ANSI_QUOTES":                       {"SET SESSION sql_mode = 'ANSI_QUOTES'"},
		"NO_BACKSLASH_ESCAPES":              {"SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'"},
		"ANSI_QUOTES,NO_BACKSLASH_ESCAPES":  {"SET SESSION sql_mode = 'ANSI_QUOTES,NO_BACKSLASH_ESCAPES'"},
		"latin1":                            {"SET SESSION sql_mode = ''", "SET NAMES latin1"},
		"latin1,ANSI_QUOTES,NO_BACKSLASH_E": {"SET SESSION sql_mode = 'ANSI_QUOTES,NO_BACKSLASH_ESCAPES'", "SET NAMES latin1"},
	}
	conns := map[string]*sql.Conn{}
	for mode, setup := range sessions {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		for _, q := range setup {
			if _, err := c.ExecContext(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		conns[mode] = c
	}
	state := func(c *sql.Conn) string {
		var rows, sum, tables int
		if err := c.QueryRowContext(ctx, "SELECT count(*), coalesce(sum(a), 0) FROM t").Scan(&rows, &sum); err != nil {
			return "t cannot be read: " + err.Error()
		}
		if err := c.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema = ?", name).Scan(&tables); err != nil {
			return err.Error()
		}
		return fmt.Sprintf("rows %d sum %d tables %d", rows, sum, tables)
	}

	pieces := []string{
		"DELETE FROM t", "INSERT INTO t VALUES (9)", "UPDATE t SET a = a + 1", "DROP TABLE t", "CREATE TABLE u (a INT)",
		"DELETE FROM t", "INSERT INTO t VALUES (9)",
		"SELECT 1", "SELECT 'a", "SELECT \"a", "SELECT 1 FROM t", "SHOW TABLES", "TABLE t", "VALUES ROW(1)",
		"WITH c AS (SELECT 1)", "EXPLAIN", "EXPLAIN ANALYZE", "ANALYZE", "DESC", "BEGIN", "SET @a = 1", "SET @a =", "USE " + name, "START TRANSACTION",
		"'", "'", "\"", "\"", "`", "\\", "\\'", "\\\"", "''", "\"\"",
		"/*", "*/", "/*!", "/*!50000", "/*!99999", "/*M!", "/*/", "/*+",
		"--", "-- ", "--\t", "--\r", "--\n", "#",
		"\n", "\r", ";", ";", " ", " ", "(", ")", ",", "x", "1", "\\N", "\xa0", "\x0b", "\x7f", "\xc3\xa9",
	}
	rng := rand.New(rand.NewSource(3093))
	const texts = 40000
	allowed, ran, holes := 0, 0, 0
	for i := 0; i < texts && holes < 10; i++ {
		var b strings.Builder
		for n := 1 + rng.Intn(7); n > 0; n-- {
			b.WriteString(pieces[rng.Intn(len(pieces))])
			if rng.Intn(3) == 0 {
				b.WriteByte(' ')
			}
		}
		stmt := b.String()
		if ReadOnlyRefusal(stmt) != "" {
			continue
		}
		allowed++
		for mode, c := range conns {
			_, _ = c.ExecContext(ctx, stmt) // most are syntax errors: what matters is the table
			ran++
			after := state(c)
			_, _ = c.ExecContext(ctx, "ROLLBACK")
			if after != clean {
				holes++
				t.Errorf("the screen allows %q and under %s the server changed the table with it: %s", stmt, mode, after)
				reset()
			}
		}
	}
	t.Logf("%d texts, %d allowed by the screen, %d runs on the server", texts, allowed, ran)
	if allowed < texts/100 {
		t.Errorf("only %d of %d generated texts passed the screen: the test compares almost nothing", allowed, texts)
	}
}
