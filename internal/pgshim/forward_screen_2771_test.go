package pgshim

import (
	"strings"
	"testing"
)

// TestForwardRefusal is the screen a statement passes before it is sent to
// the source (#2771): reads go, everything else is refused with a reason,
// and nothing in a string, a comment or a quoted name is read as a verb.
func TestForwardRefusal(t *testing.T) {
	for _, c := range []struct {
		stmt     string
		code     string // "" = sent
		contains string
	}{
		{stmt: "SELECT 1"},
		{stmt: "  select * from t where s = 'insert into x'"},
		{stmt: "(SELECT 1) UNION (SELECT 2)"},
		{stmt: "WITH a AS (SELECT 1) SELECT * FROM a"},
		{stmt: "VALUES (1), (2)"},
		{stmt: "TABLE orders"},
		{stmt: "SHOW work_mem"},
		{stmt: "EXPLAIN SELECT 1"},
		{stmt: "DECLARE c CURSOR FOR SELECT 1"},
		{stmt: "FETCH 10 FROM c"},
		{stmt: "MOVE 1 IN c"},
		{stmt: "CLOSE c"},
		{stmt: "SAVEPOINT a"},
		{stmt: "RELEASE SAVEPOINT a"},
		{stmt: "ROLLBACK TO SAVEPOINT a"},
		{stmt: "rollback work to a"},
		{stmt: "SELECT 1; SELECT 2"},
		{stmt: "SELECT $$; DELETE FROM t$$"},
		{stmt: "SELECT $tag$ ; drop table t $tag$"},
		{stmt: "SELECT 1 -- ; DELETE FROM t"},
		{stmt: "SELECT 1 /* ; DELETE /* nested */ FROM t */"},
		{stmt: "SELECT E'\\'; DELETE FROM t'"},
		{stmt: "SELECT 1;"},
		{stmt: "INSERT INTO t VALUES (1)", code: "25006", contains: "INSERT is not a read"},
		{stmt: "  update t set a = 1", code: "25006", contains: "UPDATE"},
		{stmt: "SELECT 1; DELETE FROM t", code: "25006", contains: "DELETE"},
		{stmt: "SELECT 'x';insert into t values(1)", code: "25006", contains: "INSERT"},
		{stmt: "CREATE TEMP TABLE x (a int)", code: "25006", contains: "CREATE"},
		{stmt: "TRUNCATE t", code: "25006", contains: "TRUNCATE"},
		{stmt: "MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE", code: "25006", contains: "MERGE"},
		{stmt: "VACUUM t", code: "25006", contains: "VACUUM"},
		{stmt: "LOCK TABLE t", code: "25006", contains: "LOCK"},
		{stmt: "PREPARE p AS DELETE FROM t", code: "25006", contains: "PREPARE"},
		{stmt: "EXECUTE p", code: "25006", contains: "EXECUTE"},
		{stmt: "frobnicate the table", code: "25006", contains: "sends only reads"},
		{stmt: "COPY t TO STDOUT", code: "0A000", contains: "COPY is not served"},
		{stmt: "copy (select 1) to stdout", code: "0A000", contains: "COPY is not served"},
		{stmt: "LISTEN chan", code: "0A000", contains: "LISTEN is not served"},
		{stmt: "UNLISTEN *", code: "0A000", contains: "LISTEN is not served"},
		{stmt: "NOTIFY chan, 'x'", code: "25006", contains: "NOTIFY"},
		{stmt: "DO $$ BEGIN END $$", code: "0A000", contains: "DO runs code"},
		{stmt: "SELECT 1; COMMIT", code: "0A000", contains: "COMMIT in a statement with others"},
		{stmt: "BEGIN; SELECT 1", code: "0A000", contains: "BEGIN in a statement with others"},
		{stmt: "COMMIT AND CHAIN", code: "0A000", contains: "COMMIT"},
		{stmt: "SET search_path = a; SELECT 1", code: "0A000", contains: "SET"},
		{stmt: "SELECT set_config('default_transaction_read_only', 'off', false)", code: "0A000", contains: "set_config()"},
		{stmt: `SELECT pg_catalog."set_config"('a', 'b', false)`, code: "0A000", contains: "set_config()"},
		{stmt: "SELECT SET_CONFIG ('a','b',false)", code: "0A000", contains: "set_config()"},
		{stmt: "SELECT pg_notify('c', 'x')", code: "0A000", contains: "pg_notify()"},
		{stmt: "SELECT pg_advisory_lock(1)", code: "0A000", contains: "pg_advisory_lock()"},
		{stmt: "SELECT pg_try_advisory_xact_lock(1)", code: "0A000", contains: "pg_try_advisory_xact_lock()"},
		{stmt: "SELECT * FROM dblink('x', 'select 1') AS t(a int)", code: "0A000", contains: "dblink()"},
		{stmt: `SELECT U&"s\0065t_config"('a','b',false)`, code: "0A000", contains: "Unicode escapes"},
		{stmt: "SELECT pg_advisory_lock\n(1)", code: "0A000", contains: "pg_advisory_lock()"},
		{stmt: "SELECT pg_notify\t('c', 'x')", code: "0A000", contains: "pg_notify()"},
		{stmt: "DECLARE c CURSOR WITH HOLD FOR SELECT 1", code: "0A000", contains: "WITH HOLD"},
		{stmt: "declare c cursor\nwith hold for select 1", code: "0A000", contains: "WITH HOLD"},
		{stmt: "DECLARE c CURSOR WITHOUT HOLD FOR SELECT 1"},
		{stmt: "SELECT 'set_config(' AS a"},
		{stmt: "SELECT my_set_config_thing(1)"},
		{stmt: "SELECT 1\x00", code: "22021", contains: "NUL"},
	} {
		perr := forwardRefusal(c.stmt)
		switch {
		case c.code == "" && perr != nil:
			t.Errorf("%q refused: %s %s", c.stmt, perr.code, perr.msg)
		case c.code != "" && perr == nil:
			t.Errorf("%q sent, want %s", c.stmt, c.code)
		case c.code != "" && (perr.code != c.code || !strings.Contains(perr.msg, c.contains)):
			t.Errorf("%q: %s %q, want %s with %q", c.stmt, perr.code, perr.msg, c.code, c.contains)
		}
		if perr != nil && strings.Contains(perr.msg, "frobnicate") {
			t.Errorf("%q: the refusal repeats an unknown word: %s", c.stmt, perr.msg)
		}
	}
}
