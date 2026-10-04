package readrouter

import (
	"strings"
	"testing"
)

// What read-only mode lets through: reads, and the session and transaction
// control a client needs around them. Every spelling here must come back
// with no refusal.
func TestReadOnlyRefusal_allows(t *testing.T) {
	for _, stmt := range []string{
		// Reads.
		"SELECT 1",
		"select * from orders where id = 1",
		"SELECT 1;",
		"  \n\t SELECT 1 ;  \n",
		"SeLeCt status, count(*) FROM orders GROUP BY status",
		"(SELECT 1) UNION (SELECT 2)",
		"((SELECT 1))",
		"WITH t AS (SELECT 1 AS a) SELECT a FROM t",
		"with recursive n (i) as (select 1 union all select i + 1 from n where i < 5) select * from n",
		"WITH a AS (SELECT 1), b (x, y) AS (SELECT 1, 2) SELECT * FROM a, b",
		"WITH a AS (SELECT 1) (SELECT * FROM a)",
		"TABLE orders",
		"TABLE orders ORDER BY id LIMIT 5",
		"VALUES ROW(1, 2), ROW(3, 4)",
		"/* a comment */ SELECT 1",
		"-- a comment\nSELECT 1",
		"# a comment\nSELECT 1",
		"SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM orders",
		"/* mysql-connector-j-8.4.0 */SELECT @@session.auto_increment_increment AS auto_increment_increment",
		"select @@version_comment limit 1",
		"SELECT DATABASE()",
		// A keyword inside a string, a quoted name or a comment is not a keyword.
		"SELECT 'insert into t values (1)'",
		"SELECT * FROM notes WHERE body = 'please DELETE this; DROP TABLE x'",
		"SELECT * FROM t WHERE note = 'into outfile'",
		"SELECT `into`, `for update` FROM t",
		"SELECT 1 /* into outfile 'x' for update */",
		"SELECT 1 -- for update",
		"SELECT 'it''s; here'",
		`SELECT "a;b"`,
		`SELECT 'O\'Brien'`,
		`SELECT * FROM t WHERE name LIKE '%\_%'`,
		`SELECT 'c:\\dir\\'`,
		"SELECT for_update, into_count, lock_in FROM t",
		// A quoted name that is not a lock function, and one that only
		// looks like it (both servers answer "incorrect routine name").
		"SELECT `get_lock` FROM t",
		"SELECT `is_free_lock`('x'), `my_get_lock`('x')",
		"SELECT 'get_lock'('x')",
		"SELECT 5 - -3, 'caf\xc3\xa9 -- x', `col--\xc3\xa9`",
		"SELECT 1 /* --\xa0 */ -- \xa0 tail",
		`SELECT \N`,
		`SELECT a, \N FROM t`,
		"SELECT updated_at FROM t WHERE format = 'x'",
		// Session and transaction control.
		"BEGIN",
		"begin work;",
		"START TRANSACTION",
		"START TRANSACTION READ ONLY",
		"START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
		"COMMIT",
		"COMMIT WORK",
		"COMMIT AND NO CHAIN",
		"ROLLBACK",
		"rollback;",
		"ROLLBACK TO SAVEPOINT sp1",
		"ROLLBACK TO sp1",
		"SAVEPOINT sp1",
		"SAVEPOINT `my sp`",
		"RELEASE SAVEPOINT sp1",
		"USE shop",
		"USE `shop`;",
		"SHOW TABLES",
		"SHOW DATABASES",
		"SHOW FULL COLUMNS FROM orders LIKE 'a%'",
		"SHOW CREATE TABLE orders",
		"SHOW VARIABLES LIKE 'sql_mode'",
		"SHOW WARNINGS",
		"DESCRIBE orders",
		"DESC orders",
		"EXPLAIN SELECT * FROM orders",
		"EXPLAIN FORMAT=JSON SELECT * FROM orders",
		"EXPLAIN DELETE FROM orders WHERE id = 1", // a plan, nothing runs
		"EXPLAIN ANALYZE SELECT * FROM orders",
		"EXPLAIN ANALYZE FORMAT=TREE SELECT * FROM orders",
		// The SETs a driver sends when it connects, and session settings.
		"SET NAMES utf8mb4",
		"SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'",
		"set names latin1",
		"/* driver */ SET NAMES utf8mb4",
		"SET NAMES 'utf8mb4' /* c */",
		"SET NAMES utf8mb4, @@SESSION.sql_mode = 'STRICT_TRANS_TABLES'",
		"SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci, autocommit = 1, time_zone = '+00:00'",
		"SET /* c */ character_set_client = 'utf8mb4'",
		// MariaDB: ANALYZE runs the statement and prints its plan.
		"ANALYZE SELECT * FROM orders",
		"ANALYZE FORMAT=JSON SELECT * FROM orders",
		"SET CHARACTER SET utf8",
		"SET autocommit=1",
		"SET autocommit = 0",
		"SET SESSION sql_mode = 'ANSI_QUOTES'",
		"SET @@session.time_zone = '+00:00'",
		"SET @@sql_select_limit = 100",
		"SET LOCAL max_execution_time = 1000",
		"SET time_zone = '+00:00', sql_mode = ''",
		"SET character_set_results = NULL",
		"SET character_set_client = utf8mb4",
		"SET @a = 1",
		"SET @a := (SELECT max(id) FROM orders), @b = 2",
		"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
		"SET SESSION TRANSACTION READ ONLY",
		"SET autocommit=1, sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')",
	} {
		if why := ReadOnlyRefusal(stmt); why != "" {
			t.Errorf("refused %q: %s", stmt, why)
		}
	}
}

// What read-only mode refuses: anything not positively recognised above.
// want is a fragment of the reason, so the test also pins that the reason
// names the right thing.
func TestReadOnlyRefusal_refuses(t *testing.T) {
	for _, tc := range []struct{ stmt, want string }{
		// Plain writes and DDL, in the spellings a client can give them.
		{"INSERT INTO t VALUES (1)", "INSERT"},
		{"insert into t values (1)", "INSERT"},
		{"  \n\tInSeRt INTO t VALUES (1)", "INSERT"},
		{"/* c */ INSERT INTO t VALUES (1)", "INSERT"},
		{"-- c\nINSERT INTO t VALUES (1)", "INSERT"},
		{"# c\nINSERT INTO t VALUES (1)", "INSERT"},
		{"(INSERT INTO t VALUES (1))", "INSERT"},
		{"UPDATE t SET a = 1", "UPDATE"},
		{"DELETE FROM t", "DELETE"},
		{"REPLACE INTO t VALUES (1)", "REPLACE"},
		{"CREATE TABLE x (a INT)", "CREATE"},
		{"CREATE TEMPORARY TABLE x (a INT)", "CREATE"},
		{"ALTER TABLE t ADD COLUMN b INT", "ALTER"},
		{"DROP TABLE t", "DROP"},
		{"TRUNCATE TABLE t", "TRUNCATE"},
		{"RENAME TABLE a TO b", "RENAME"},
		{"GRANT ALL ON *.* TO 'u'@'%'", "GRANT"},
		{"REVOKE ALL ON *.* FROM 'u'@'%'", "REVOKE"},
		{"KILL 42", "KILL"},
		{"KILL QUERY 42", "KILL"},
		{"CALL p()", "CALL"},
		{"DO SLEEP(1)", "DO"},
		{"HANDLER t OPEN", "HANDLER"},
		{"LOCK TABLES t WRITE", "LOCK"},
		{"UNLOCK TABLES", "UNLOCK"},
		{"LOAD DATA INFILE '/tmp/x' INTO TABLE t", "LOAD"},
		{"XA START 'x'", "XA"},
		{"XA COMMIT 'x'", "XA"},
		{"PREPARE s FROM 'DELETE FROM t'", "PREPARE"},
		{"PREPARE s FROM @q", "PREPARE"},
		{"EXECUTE s", "EXECUTE"},
		{"DEALLOCATE PREPARE s", "DEALLOCATE"},
		{"FLUSH TABLES", "FLUSH"},
		{"RESET MASTER", "RESET"},
		{"ANALYZE TABLE t", "ANALYZE"},
		{"ANALYZE FORMAT=JSON DELETE FROM t", "ANALYZE"}, // MariaDB: it executes
		{"OPTIMIZE TABLE t", "OPTIMIZE"},
		{"BINLOG 'abc'", "BINLOG"},
		{"SHUTDOWN", "SHUTDOWN"},
		{"INSTALL PLUGIN x SONAME 'x.so'", "INSTALL"},
		{"CHANGE MASTER TO MASTER_HOST = 'x'", "CHANGE"},
		{"STOP REPLICA", "STOP"},
		{"", "not recognised"},
		{"   ", "not recognised"},
		{";", "not recognised"},
		{"12345", "not recognised"},
		// SETs that reach past the session.
		{"SET PASSWORD = 'x'", "SET"},
		{"SET PASSWORD FOR 'u'@'%' = 'x'", "SET"},
		{"SET GLOBAL max_connections = 1", "SET"},
		{"set global max_connections = 1", "SET"},
		{"SET PERSIST max_connections = 1", "SET"},
		{"SET PERSIST_ONLY max_connections = 1", "SET"},
		{"SET @@global.max_connections = 1", "SET"},
		{"SET @@GLOBAL.max_connections = 1", "SET"},
		{"SET @@persist.max_connections = 1", "SET"},
		{"SET sql_mode = '', GLOBAL max_connections = 1", "SET"},
		{"SET sql_mode = '', @@global.max_connections = 1", "SET"},
		{"SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED", "SET"},
		{"SET ROLE admin", "SET"},
		{"SET DEFAULT ROLE admin TO 'u'@'%'", "SET"},
		{"SET RESOURCE GROUP g", "SET"},
		{"SET gtid_next = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1'", "SET"},
		{"SET SESSION gtid_next = 'AUTOMATIC'", "SET"},
		{"SET @@session.gtid_next = 'AUTOMATIC'", "SET"},
		{"SET NAMES gbk", "character set"},
		{"SET NAMES 'sjis'", "character set"},
		{"SET NAMES utf8mb4, GLOBAL max_connections = 1", "SET"},
		{"SET NAMES gbk, autocommit = 1", "character set"},
		{"SET NAMES utf8mb4, character_set_client = gbk", "character set"},
		{"SET NAMES 'ut''f8'", "SET"},
		{"SET NAMES", "SET"},
		{"SET NAMES utf8mb4 extra", "SET"},
		{"ANALYZE FORMAT=JSON UPDATE t SET a = 1", "ANALYZE"},
		{"ANALYZE SELECT * FROM t INTO OUTFILE '/tmp/x'", "INTO"},
		{"ANALYZE WITH c AS (SELECT 1) DELETE FROM t", "WITH"},
		{"SET character_set_client = gbk", "character set"},
		{"SET sql_mode = '', character_set_client = gbk", "character set"},
		{"SET CHARACTER SET big5", "character set"},
		{"SET", "SET"},
		{"SET @a = GET_LOCK('x', 1)", "GET_LOCK"},
		// A SELECT that writes or locks.
		{"SELECT * FROM t INTO OUTFILE '/tmp/x'", "INTO"},
		{"SELECT * INTO OUTFILE '/tmp/x' FROM t", "INTO"},
		{"SELECT a FROM t INTO DUMPFILE '/tmp/x'", "INTO"},
		{"SELECT a INTO @v FROM t", "INTO"},
		{"select a from t into\noutfile '/tmp/x'", "INTO"},
		{"SELECT a FROM t INTO/**/OUTFILE '/tmp/x'", "INTO"},
		{"SELECT 1.INTO OUTFILE '/tmp/x'", "INTO"},
		{"SELECT .1INTO OUTFILE '/tmp/x'", "INTO"},  // MySQL reads a number, then INTO
		{"SELECT 1e5INTO OUTFILE '/tmp/x'", "INTO"}, // the same
		{"SELECT 'a'INTO OUTFILE '/tmp/x'", "INTO"},
		{`SELECT \NINTO OUTFILE '/tmp/x'`, "INTO"}, // \N is NULL to MySQL, and INTO may follow it directly
		{`SELECT * FROM t WHERE a <=> \NFOR UPDATE`, "locking read"},
		{"TABLE t INTO OUTFILE '/tmp/x'", "INTO"},
		{"(SELECT 1) INTO OUTFILE '/tmp/x'", "INTO"},
		{"SELECT * FROM t FOR UPDATE", "locking read"},
		{"SELECT * FROM t WHERE id = 1 for   update", "locking read"},
		{"SELECT * FROM t FOR\nUPDATE", "locking read"},
		{"SELECT * FROM t FOR\vUPDATE", "locking read"}, // a vertical tab is a space to MySQL
		{"SELECT * FROM t FOR\xa0UPDATE", "locking read"},
		{"SELECT * FROM t FOR/* c */UPDATE", "locking read"},
		{"SELECT * FROM t FOR SHARE", "locking read"},
		{"SELECT * FROM t FOR UPDATE SKIP LOCKED", "locking read"},
		{"SELECT * FROM t LOCK IN SHARE MODE", "locking read"},
		{"SELECT * FROM (SELECT * FROM t FOR UPDATE) x", "locking read"},
		{"SELECT GET_LOCK('x', 10)", "GET_LOCK"},
		{"SELECT RELEASE_LOCK('x')", "GET_LOCK"},
		{"SELECT get_lock ('x', 1)", "GET_LOCK"},
		{"SELECT get_lock/**/('x', 1)", "GET_LOCK"},
		// A quoted name still calls the built-in (MySQL 8.4 and MariaDB 11.4
		// both return 1 for these), in backticks and, under ANSI_QUOTES, in
		// double quotes.
		{"SELECT `get_lock`('zz', 1)", "GET_LOCK"},
		{"SELECT `GET_LOCK` ('zz', 1)", "GET_LOCK"},
		{"SELECT `release_lock`('zz')", "GET_LOCK"},
		{"SELECT `release_all_locks`()", "GET_LOCK"},
		{"SELECT 1, `Release_All_Locks`/* c */()", "GET_LOCK"},
		{`SELECT "get_lock"('zz', 1)`, "GET_LOCK"},
		{`SELECT "release_all_locks"()`, "GET_LOCK"},
		{"SET @a = `get_lock`('zz', 1)", "GET_LOCK"},
		// `--` in front of a byte above 0x7f: under latin1 the servers read
		// --\xa0 as the start of a comment (0xA0 is a space there), under
		// utf8mb4 they do not. The screen cannot know which, so it refuses.
		{"SELECT 1 --\xa0 ' \n INTO OUTFILE '/tmp/x' -- '", "--"},
		{"SELECT * FROM t --\xa0 ' \n FOR UPDATE -- '", "--"},
		{"SELECT 1 --\xa0 ' \n , get_lock('a',1) -- '", "--"},
		{"SELECT 1 --\xc2\xa0 x", "--"},
		{"SELECT 1 --\xe9", "--"},
		{"SELECT NEXTVAL(seq)", "sequence"},
		{"SELECT NEXT VALUE FOR seq", "sequence"},
		{"SELECT SETVAL(seq, 10)", "sequence"},
		// A CTE in front of a write.
		{"WITH c AS (SELECT 1) DELETE FROM t", "WITH"},
		{"WITH c AS (SELECT id FROM t) UPDATE t SET a = 1 WHERE id IN (SELECT id FROM c)", "WITH"},
		{"WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c", "WITH"},
		{"with recursive c (i) as (select 1), d as (select 2) delete from t", "WITH"},
		{"WITH c AS (SELECT ')' ) DELETE FROM t", "WITH"},
		{"WITH c AS (SELECT 1", "WITH"},
		{"WITH", "WITH"},
		// More than one statement.
		{"SELECT 1; DELETE FROM t", "more than one statement"},
		{"SELECT 1;DELETE FROM t;", "more than one statement"},
		{"SELECT 1;;", "more than one statement"},
		{"BEGIN; DELETE FROM t; COMMIT", "more than one statement"},
		{"SELECT 1 /* c */; /* c */ DROP TABLE t", "more than one statement"},
		// Executable comments: MySQL runs what is inside.
		{"/*!50000 DELETE FROM t */", "executable comment"},
		{"/*! DELETE FROM t */", "executable comment"},
		{"SELECT 1 /*!50000 INTO OUTFILE '/tmp/x' */", "executable comment"},
		{"/*M!100100 DELETE FROM t */", "executable comment"},
		{"/*!40101 SET NAMES utf8 */", "executable comment"},
		// EXPLAIN ANALYZE runs the statement.
		{"EXPLAIN ANALYZE DELETE FROM t", "EXPLAIN ANALYZE"},
		{"explain analyze\nupdate t set a = 1", "EXPLAIN ANALYZE"},
		{"EXPLAIN ANALYZE FORMAT=TREE DELETE t1 FROM t1 JOIN t2 USING (id)", "EXPLAIN ANALYZE"},
		{"EXPLAIN FORMAT=TREE ANALYZE DELETE FROM t", "EXPLAIN ANALYZE"},
		{"DESCRIBE ANALYZE DELETE FROM t", "EXPLAIN ANALYZE"},
		{"DESC ANALYZE DELETE FROM t", "EXPLAIN ANALYZE"},
		{"EXPLAIN ANALYZE WITH c AS (SELECT 1) DELETE FROM t", "WITH"},
		{"EXPLAIN SELECT 1 INTO @v", "INTO"},
		// Transaction verbs with something else attached.
		{"BEGIN NOT ATOMIC DELETE FROM t; END", "more than one statement"},
		{"BEGIN NOT ATOMIC DELETE FROM t", "BEGIN"},
		{"COMMIT DELETE", "COMMIT"},
		{"START SLAVE", "START"},
		{"START REPLICA", "START"},
		{"ROLLBACK TO SAVEPOINT a b", "ROLLBACK"},
		{"USE a b", "USE"},
		// A write hidden behind a string that reads two ways (backslash
		// escapes on or off on the source): refused if either reading writes.
		{`SELECT 'a\' INTO OUTFILE '/tmp/x' -- '`, "INTO"},
		{`SELECT 'x\'' INTO OUTFILE 'f'`, "INTO"},
		{`SELECT "a\" FOR UPDATE -- "`, "locking read"},
		{"SELECT 1 --\r' \n INTO OUTFILE 'x'", "INTO"},
		// What cannot be read at all.
		{"SELECT 'unterminated", "unterminated"},
		{"SELECT 1 /* unterminated", "unterminated"},
		{"SELECT `unterminated", "unterminated"},
		{"SELECT 1\x00; DELETE FROM t", "NUL"},
	} {
		why := ReadOnlyRefusal(tc.stmt)
		if why == "" {
			t.Errorf("allowed %q", tc.stmt)
			continue
		}
		if !strings.Contains(why, tc.want) {
			t.Errorf("%q refused as %q, want a reason holding %q", tc.stmt, why, tc.want)
		}
		if strings.Contains(why, "—") {
			t.Errorf("%q: reason holds an em dash: %s", tc.stmt, why)
		}
	}
}

// A refusal never repeats the statement's literals: the reason goes to the
// log and the client, and a literal may be data.
func TestReadOnlyRefusal_reasonCarriesNoLiterals(t *testing.T) {
	for _, stmt := range []string{
		"INSERT INTO t VALUES ('s3cr3t')",
		"s3cr3t_keyword_that_is_very_long_and_not_sql_at_all_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 1",
		"s3cr3t",
		"hunter2secretpassword",
		"s3cr3t INSERT INTO t VALUES (1)",
		"(s3cr3t)",
		"SET PASSWORD = 's3cr3t'",
	} {
		why := ReadOnlyRefusal(stmt)
		low := strings.ToLower(why)
		if strings.Contains(low, "s3cr3t") || strings.Contains(low, "hunter2") || len(why) > 200 {
			t.Errorf("reason for %q is %q", stmt, why)
		}
	}
}
