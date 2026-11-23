package readrouter

import "testing"

// #3093: the read-only screen against the ways MySQL reads a statement.
// Each row is a text and what the screen must do with it. "refused" rows
// hold a write, a lock, or a way to make the session writable again, in a
// form a reader of the first keyword alone would miss; "allowed" rows are
// reads the same forms must not break. The test beside this one
// (readonly_audit_3093_integration_test.go) sends generated texts to a real
// server and checks that nothing the screen allows changes a table.
func TestReadOnlyScreen_audit_3093(t *testing.T) {
	type row struct {
		class, stmt string
		refused     bool
	}
	const no, yes = false, true
	rows := []row{
		// Comments the server runs.
		{"executable comment", "/*!50000 DELETE FROM t */", yes},
		{"executable comment", "/*! DELETE FROM t */", yes},
		{"executable comment", "/*M! DELETE FROM t */", yes},
		{"executable comment", "/*M!100100 DELETE FROM t */", yes},
		{"executable comment", "SELECT 1 /*! INTO OUTFILE '/tmp/x' */", yes},
		{"executable comment", "SELECT /*!99999 1, */ 1", yes},
		{"executable comment", "SELECT 1 /* /*! DELETE FROM t */ */", yes},
		// Comments the server skips, in front of or inside a write.
		{"comment", "/* c */ DELETE FROM t", yes},
		{"comment", "/**/DELETE FROM t", yes},
		{"comment", "-- c\nDELETE FROM t", yes},
		{"comment", "--\nDELETE FROM t", yes},
		{"comment", "--\tc\nDELETE FROM t", yes},
		{"comment", "# c\nDELETE FROM t", yes},
		{"comment", "#\nDELETE FROM t", yes},
		{"comment", "SELECT 1 /* c */ INTO @a", yes},
		{"comment", "SELECT 1 --\n INTO OUTFILE '/tmp/x'", yes},
		{"comment", "SELECT * FROM t # c\n FOR UPDATE", yes},
		{"comment", "SELECT * FROM t FOR/**/UPDATE", yes},
		{"comment", "SELECT * FROM t FOR-- c\nUPDATE", yes},
		{"comment", "SELECT 1 /* c", yes},
		{"comment", "SELECT 1 /*/ DELETE FROM t", yes},
		{"comment", "SELECT 1 /*/ */ ; DELETE FROM t", yes},
		{"comment", "/* c */ SELECT 1 -- c", no},
		{"comment", "SELECT 1 # c", no},
		{"comment", "SELECT /* DELETE FROM t */ 1", no},
		{"comment", "SELECT 1 /*/ DELETE FROM t */", no},
		// `--` opens a comment only before white space or a control byte.
		{"-- without a space", "SELECT 1 --c\n; DELETE FROM t", yes},
		{"-- without a space", "SELECT 1 -- c\n; DELETE FROM t", yes},
		{"-- without a space", "SELECT 2--1", no},
		{"-- without a space", "SELECT 2 --1", no},
		{"-- without a space", "SELECT 1 --\r; DELETE FROM t", no}, // no newline: the rest is the comment
		{"-- without a space", "SELECT 1 --\x01; DELETE FROM t", no},
		{"-- without a space", "SELECT 1 --\x01\n; DELETE FROM t", yes},
		{"-- without a space", "SELECT 1 --\xa0\n; DELETE FROM t", yes},
		{"-- without a space", "SELECT 1 --\xc3\xa9\n; DELETE FROM t", yes},
		{"-- without a space", "SELECT 1 --", no},
		// Where a string ends, under each sql_mode that changes it.
		{"quotes", "SELECT 'a' ; DELETE FROM t", yes},
		{"quotes", `SELECT 'a\' ; DELETE FROM t -- '`, yes},
		{"quotes", `SELECT "a\" ; DELETE FROM t -- "`, yes},
		{"quotes", `SELECT 'a\\' ; DELETE FROM t -- '`, yes},
		{"quotes", `SELECT 'a\'' ; DELETE FROM t -- '`, yes},
		{"quotes", `SELECT 'a''' ; DELETE FROM t -- '`, yes},
		{"quotes", `SELECT "a""" ; DELETE FROM t -- "`, yes},
		{"quotes", `SELECT 'a\' INTO OUTFILE '/tmp/x' -- '`, yes},
		{"quotes", `SELECT "a" INTO OUTFILE "/tmp/x"`, yes},
		{"quotes", `SELECT "GET_LOCK"('a', 1)`, yes},
		{"quotes", "SELECT `get_lock`('a', 1)", yes},
		{"quotes", "SELECT 'a", yes},
		{"quotes", "SELECT `a", yes},
		{"quotes", "SELECT ';'", no},
		{"quotes", "SELECT 'DELETE FROM t'", no},
		{"quotes", `SELECT "INTO OUTFILE"`, no},
		{"quotes", "SELECT `into` FROM t", no},
		{"quotes", "SELECT `a``; DELETE FROM t -- `", no}, // one quoted name
		{"quotes", `SELECT 1 "; DELETE FROM t; -- "`, no}, // one string or one name
		{"quotes", `SELECT 'a\'b'`, no},
		{"quotes", `SELECT 'it''s'`, no},
		{"quotes", `SELECT '\\'`, no},
		{"quotes", `SELECT \N`, no},
		// More than one statement in the packet.
		{"multi-statement", "SELECT 1; DELETE FROM t", yes},
		{"multi-statement", "SELECT 1;DELETE FROM t;", yes},
		{"multi-statement", "BEGIN; DELETE FROM t", yes},
		{"multi-statement", "SET @a = 1; DELETE FROM t", yes},
		{"multi-statement", "USE db; DELETE FROM t", yes},
		{"multi-statement", "; DELETE FROM t", yes},
		{"multi-statement", "SELECT 1;;", yes},
		{"multi-statement", "SELECT 1;", no},
		{"multi-statement", "SELECT 1 ; ", no},
		{"multi-statement", "SELECT 1; -- done", no},
		{"multi-statement", "SELECT 1; /* done */", no},
		// A read's keyword in front of something that is not a read.
		{"statement class", "(DELETE FROM t)", yes},
		{"statement class", "WITH c AS (SELECT 1) DELETE FROM t", yes},
		{"statement class", "WITH c AS (SELECT 1) UPDATE t SET a = 1", yes},
		{"statement class", "WITH c AS (SELECT ')') DELETE FROM t", yes},
		{"statement class", "EXPLAIN ANALYZE DELETE FROM t", yes},
		{"statement class", "DESC ANALYZE UPDATE t SET a = 1", yes},
		{"statement class", "EXPLAIN ANALYZE WITH c AS (SELECT 1) DELETE FROM t", yes},
		{"statement class", "ANALYZE TABLE t", yes},
		{"statement class", "ANALYZE FORMAT=JSON DELETE FROM t", yes},
		{"statement class", "TABLE t INTO OUTFILE '/tmp/x'", yes},
		{"statement class", "SELECT * FROM t INTO DUMPFILE '/tmp/x'", yes},
		{"statement class", "SELECT 1 INTO @a", yes},
		{"statement class", "SELECT 1e5INTO @a", yes},
		{"statement class", "SELECT .1INTO @a", yes},
		{"statement class", "SELECT * FROM t LOCK IN SHARE MODE", yes},
		{"statement class", "SELECT * FROM t FOR SHARE", yes},
		{"statement class", "SELECT GET_LOCK('a', 1)", yes},
		{"statement class", "SELECT NEXTVAL(s)", yes},
		{"statement class", "SELECT NEXT VALUE FOR s", yes},
		{"statement class", "DO SLEEP(1)", yes},
		{"statement class", "CALL p()", yes},
		{"statement class", "HANDLER t OPEN", yes},
		{"statement class", "LOAD DATA INFILE 'x' INTO TABLE t", yes},
		{"statement class", "LOCK TABLES t WRITE", yes},
		{"statement class", "XA START 'a'", yes},
		{"statement class", "BEGIN NOT ATOMIC DELETE FROM t; END", yes},
		{"statement class", "\x00DELETE FROM t", yes},
		{"statement class", "SELECT 1\x00; DELETE FROM t", yes},
		{"statement class", "\xef\xbb\xbfDELETE FROM t", yes},
		{"statement class", "(SELECT 1) UNION (SELECT 2)", no},
		{"statement class", "WITH c AS (SELECT 1) SELECT * FROM c", no},
		{"statement class", "EXPLAIN DELETE FROM t", no}, // a plan: nothing runs
		{"statement class", "EXPLAIN ANALYZE SELECT 1", no},
		{"statement class", "TABLE t", no},
		{"statement class", "VALUES ROW(1)", no},
		{"statement class", "SHOW TABLES", no},
		{"statement class", "DESC t", no},
		// Text-protocol prepared statements hide the text in a string. The
		// binary protocol's text is screened as it is sent (COM_STMT_PREPARE).
		{"prepared", "PREPARE s FROM 'DELETE FROM t'", yes},
		{"prepared", "PREPARE s FROM @a", yes},
		{"prepared", "EXECUTE s", yes},
		{"prepared", "DEALLOCATE PREPARE s", yes},
		{"prepared", "EXECUTE IMMEDIATE 'DELETE FROM t'", yes},
		{"prepared", "DELETE FROM t WHERE a = ?", yes},
		{"prepared", "SELECT * FROM t WHERE a = ? INTO @a", yes},
		{"prepared", "SELECT * FROM t WHERE a = ?", no},
		// SET beyond the session.
		{"SET", "SET GLOBAL read_only = 0", yes},
		{"SET", "SET @@GLOBAL.read_only = 0", yes},
		{"SET", "SET @@global . read_only = 0", yes},
		{"SET", "SET PERSIST max_connections = 1", yes},
		{"SET", "SET @a = 1, GLOBAL max_connections = 1", yes},
		{"SET", "SET @a = IF(1, 2, 3), GLOBAL max_connections = 1", yes},
		{"SET", "SET NAMES utf8mb4, GLOBAL max_connections = 1", yes},
		{"SET", "SET PASSWORD = 'x'", yes},
		{"SET", "SET ROLE ALL", yes},
		{"SET", "SET gtid_next = 'AUTOMATIC'", yes},
		{"SET", "SET NAMES gbk", yes},
		{"SET", "SET character_set_client = sjis", yes},
		{"SET", "SET STATEMENT max_statement_time = 1 FOR DELETE FROM t", yes},
		{"SET", "SET @a = (SELECT 1 INTO OUTFILE '/tmp/x')", yes},
		{"SET", "SET @a = 1", no},
		{"SET", "SET autocommit = 0", no},
		{"SET", "SET SESSION sql_mode = 'ANSI_QUOTES'", no},
		{"SET", "SET NAMES utf8mb4", no},
		// The session is read-only on the source: nothing takes that back.
		{"read-only session", "START TRANSACTION READ WRITE", yes},
		{"read-only session", "start transaction with consistent snapshot, read write", yes},
		{"read-only session", "START TRANSACTION READ/**/WRITE", yes},
		{"read-only session", "START TRANSACTION READ\nWRITE", yes},
		{"read-only session", "START TRANSACTION READ -- c\n WRITE", yes},
		{"read-only session", "SET TRANSACTION READ WRITE", yes},
		{"read-only session", "SET SESSION TRANSACTION READ WRITE", yes},
		{"read-only session", "SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE", yes},
		{"read-only session", "SET transaction_read_only = 0", yes},
		{"read-only session", "SET @@transaction_read_only = OFF", yes},
		{"read-only session", "SET @@session.transaction_read_only := 0", yes},
		{"read-only session", "SET @@SESSION.TRANSACTION_READ_ONLY = DEFAULT", yes},
		{"read-only session", "SET SESSION tx_read_only = 0", yes},
		{"read-only session", "SET LOCAL transaction_read_only = 0", yes},
		{"read-only session", "SET autocommit = 1, @@local.tx_read_only = 0", yes},
		{"read-only session", "SET `transaction_read_only` = 0", yes},
		{"read-only session", "BEGIN", no},
		{"read-only session", "START TRANSACTION", no},
		{"read-only session", "START TRANSACTION READ ONLY", no},
		{"read-only session", "START TRANSACTION WITH CONSISTENT SNAPSHOT", no},
		{"read-only session", "SET TRANSACTION READ ONLY", no},
		{"read-only session", "SET SESSION TRANSACTION READ ONLY", no},
		{"read-only session", "SET TRANSACTION ISOLATION LEVEL READ COMMITTED", no},
		{"read-only session", "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED, READ ONLY", no},
		{"read-only session", "SET transaction_isolation = 'READ-COMMITTED'", no},
		{"read-only session", "COMMIT", no},
		{"read-only session", "ROLLBACK", no},
	}
	for _, r := range rows {
		why := ReadOnlyRefusal(r.stmt)
		switch {
		case r.refused && why == "":
			t.Errorf("%s: %q is allowed, want it refused", r.class, r.stmt)
		case !r.refused && why != "":
			t.Errorf("%s: %q is refused (%s), want it allowed", r.class, r.stmt, why)
		}
	}
}
