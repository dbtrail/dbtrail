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
		// Transaction control. What would take back the read-only mark of
		// the session on the source passes the screen and is answered by
		// the port (TestReadOnlyMark_3093).
		{"transactions", "BEGIN", no},
		{"transactions", "START TRANSACTION", no},
		{"transactions", "START TRANSACTION READ ONLY", no},
		{"transactions", "START TRANSACTION WITH CONSISTENT SNAPSHOT", no},
		{"transactions", "SET TRANSACTION READ ONLY", no},
		{"transactions", "SET SESSION TRANSACTION READ ONLY", no},
		{"transactions", "SET SESSION TRANSACTION READ WRITE", no},
		{"transactions", "SET TRANSACTION ISOLATION LEVEL READ COMMITTED", no},
		{"transactions", "SET transaction_isolation = 'READ-COMMITTED'", no},
		{"transactions", "COMMIT", no},
		{"transactions", "ROLLBACK", no},
		{"transactions", "BEGIN NOT ATOMIC DELETE FROM t; END", yes},
		{"transactions", "XA START 'a'", yes},
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

// #3093: what a read-only port does with a statement that would take back
// the read-only mark of the session on the source. The SET forms are what
// drivers and pools send when a connection stops being read-only: answered
// by the port, never sent. Measured on MySQL 8.0.46 with read_only=ON and an
// account without SUPER: the SET forms answer OK and a later write is
// refused; START TRANSACTION READ WRITE answers error 1290.
func TestReadOnlyMark_3093(t *testing.T) {
	for _, tc := range []struct {
		stmt string
		want MarkAction
		send string
	}{
		{"SELECT 1", MarkNone, ""},
		{"SET autocommit = 1", MarkNone, ""},
		{"SET SESSION TRANSACTION READ ONLY", MarkNone, ""},
		{"START TRANSACTION READ ONLY", MarkNone, ""},
		{"START TRANSACTION", MarkNone, ""},
		{"BEGIN", MarkNone, ""},
		{"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", MarkNone, ""},
		{"SET @a = 'read write'", MarkNone, ""},
		{"SELECT 'SET transaction_read_only = 0'", MarkNone, ""},
		{"SET @transaction_read_only = 0", MarkNone, ""}, // a user variable
		{"DELETE FROM t", MarkNone, ""},                  // the screen refuses it
		{"SET GLOBAL transaction_read_only = 0", MarkNone, ""},

		{"SET SESSION TRANSACTION READ WRITE", MarkAnswer, ""},
		{"set session transaction read write", MarkAnswer, ""},
		{"SET TRANSACTION READ WRITE", MarkAnswer, ""},
		{"SET LOCAL TRANSACTION READ WRITE;", MarkAnswer, ""},
		{"/* pool */ SET SESSION TRANSACTION READ/**/WRITE", MarkAnswer, ""},
		{"SET SESSION TRANSACTION READ\nWRITE -- c", MarkAnswer, ""},
		{"SET transaction_read_only = 0", MarkAnswer, ""},
		{"SET SESSION transaction_read_only = OFF", MarkAnswer, ""},
		{"SET SESSION transaction_read_only = 'OFF'", MarkAnswer, ""},
		{"SET @@session.transaction_read_only := 0", MarkAnswer, ""},
		{"SET @@SESSION.TRANSACTION_READ_ONLY = DEFAULT", MarkAnswer, ""},
		{"SET @@transaction_read_only = 0", MarkAnswer, ""},
		{"SET @@local.tx_read_only = 0", MarkAnswer, ""},
		{"SET LOCAL tx_read_only = 0", MarkAnswer, ""},
		{"SET tx_read_only = 0, transaction_read_only = 0", MarkAnswer, ""},
		{"SET transaction_read_only = 1", MarkAnswer, ""},

		{"SET autocommit = 1, transaction_read_only = 0", MarkRewrite, "set autocommit = 1"},
		{"SET transaction_read_only=0, autocommit=1, @a = 2", MarkRewrite, "set autocommit=1, @a = 2"},
		{"SET NAMES utf8mb4, tx_read_only = 0", MarkRewrite, "set names utf8mb4"},
		{"SET @a = IF(1, 2, 3), @@session.transaction_read_only = 0", MarkRewrite, "set @a = if(1, 2, 3)"},
		{"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE", MarkRewrite, "set session transaction isolation level read committed, read only"},
		{"SET TRANSACTION READ WRITE, ISOLATION LEVEL REPEATABLE READ", MarkRewrite, "set transaction read only, isolation level repeatable read"},

		{"START TRANSACTION READ WRITE", MarkRefuse, ""},
		{"start transaction with consistent snapshot, read write", MarkRefuse, ""},
		{"START TRANSACTION READ/**/WRITE", MarkRefuse, ""},
		{"START TRANSACTION READ -- c\n WRITE", MarkRefuse, ""},
		// With quoted text the rest cannot be rebuilt from the screen's reading.
		{"SET @a = 'x', transaction_read_only = 0", MarkRefuse, ""},
		{"SET sql_mode = `ansi`, tx_read_only = 0", MarkRefuse, ""},
		// One reading of the backslash holds the mark and another does not.
		{`SET @a = '\', transaction_read_only = 0 -- '`, MarkRefuse, ""},
		{`SET @a = "\", @@session.tx_read_only = 0 -- "`, MarkRefuse, ""},
	} {
		got, send := ReadOnlyMark(tc.stmt)
		if got != tc.want || send != tc.send {
			t.Errorf("ReadOnlyMark(%q) = %d, %q; want %d, %q", tc.stmt, got, send, tc.want, tc.send)
		}
		if got == MarkRewrite {
			// What is sent in its place passes the screen and holds no mark.
			if why := ReadOnlyRefusal(send); why != "" {
				t.Errorf("%q is rewritten to %q, which the screen refuses: %s", tc.stmt, send, why)
			}
			if again, _ := ReadOnlyMark(send); again != MarkNone {
				t.Errorf("%q is rewritten to %q, which still changes the mark", tc.stmt, send)
			}
		}
	}
}
