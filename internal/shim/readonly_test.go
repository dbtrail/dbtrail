package shim

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// readOnlyHandler is routingHandler with read-only mode on, and an observer
// that records every decision.
func readOnlyHandler(t *testing.T, r *fakeRouter, f *fakeFreeSQL) (*Handler, *[]string) {
	t.Helper()
	var seen []string
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	h.BindRouter(r, RouterConfig{
		MaxCopyAge: time.Minute,
		ReadOnly:   true,
		Observe:    func(side RouteSide, reason RouteReason) { seen = append(seen, string(side)+"/"+string(reason)) },
	})
	return h, &seen
}

// untouched fails the test if the handler asked ANYTHING of the source: a
// forwarded statement, an EXPLAIN, a PREPARE or a USE.
func untouched(t *testing.T, stmt string, r *fakeRouter) {
	t.Helper()
	if len(r.forwarded) != 0 || len(r.explained) != 0 || len(r.prepared) != 0 || len(r.useDBs) != 0 {
		t.Errorf("%q reached the source: forwarded %v, explained %v, prepared %d, use %v", stmt, r.forwarded, r.explained, len(r.prepared), r.useDBs)
	}
}

// refusedReadOnly fails the test unless err is the read-only refusal: MySQL's
// code 1290, naming the flag.
func refusedReadOnly(t *testing.T, stmt string, err error) {
	t.Helper()
	var me *mysql.MyError
	if !errors.As(err, &me) {
		t.Errorf("%q: got %v, want the read-only refusal", stmt, err)
		return
	}
	if me.Code != mysql.ER_OPTION_PREVENTS_STATEMENT || !strings.Contains(me.Message, "--route-read-only") || !strings.Contains(me.Message, "Nothing was sent to the source") {
		t.Errorf("%q: got %d %q, want 1290 naming --route-read-only", stmt, me.Code, me.Message)
	}
	if strings.Contains(me.Message, "—") {
		t.Errorf("%q: the refusal holds an em dash: %s", stmt, me.Message)
	}
}

// One statement per class the mode must refuse (the acceptance list of #2079
// and the shapes that hide a write from a first-keyword reader). Each is sent
// as a text statement AND as a binary-protocol PREPARE, on a fresh
// connection, and must leave the source untouched both ways.
var readOnlyRefused = []string{
	"INSERT INTO t VALUES (1)",
	"UPDATE t SET a = 1",
	"DELETE FROM t",
	"REPLACE INTO t VALUES (1)",
	"CREATE TABLE x (a INT)",
	"ALTER TABLE t ADD COLUMN b INT",
	"DROP TABLE t",
	"TRUNCATE TABLE t",
	"CREATE TEMPORARY TABLE x (a INT)",
	"GRANT ALL ON *.* TO 'u'@'%'",
	"KILL 42",
	"SET PASSWORD = 'x'",
	"SET GLOBAL max_connections = 1",
	"SET PERSIST max_connections = 1",
	"SET @@global.max_connections = 1",
	"SELECT * FROM t INTO OUTFILE '/tmp/x'",
	"SELECT a FROM t INTO DUMPFILE '/tmp/x'",
	"TABLE t INTO OUTFILE '/tmp/x'",
	"SELECT * FROM t FOR UPDATE",
	"SELECT * FROM t FOR SHARE",
	"SELECT * FROM t LOCK IN SHARE MODE",
	"WITH c AS (SELECT 1) DELETE FROM t",
	"WITH c AS (SELECT 1) UPDATE t SET a = 1",
	"WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c",
	"SELECT 1; DELETE FROM t",
	"/*!50000 DELETE FROM t */",
	"EXPLAIN ANALYZE DELETE FROM t",
	"PREPARE s FROM 'DELETE FROM t'",
	"EXECUTE s",
	"CALL p()",
	"DO SLEEP(1)",
	"HANDLER t OPEN",
	"LOCK TABLES t WRITE",
	"LOAD DATA INFILE '/tmp/x' INTO TABLE t",
	"XA START 'x'",
	"  /* c */ (\n\tdElEtE FROM t)",
	`SELECT 'a\' INTO OUTFILE '/tmp/x' -- '`,
}

func TestReadOnly_refusedStatementsNeverReachTheSource(t *testing.T) {
	for _, stmt := range readOnlyRefused {
		// toCopy: a statement that slipped through as a SELECT would be
		// explained, and one that slipped through as anything else forwarded.
		r := &fakeRouter{toCopy: true, reason: "expensive"}
		f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
		h, seen := readOnlyHandler(t, r, f)
		_, err := h.HandleQuery(stmt)
		refusedReadOnly(t, stmt, err)
		untouched(t, stmt, r)
		if f.calls != 0 {
			t.Errorf("%q ran on the copy", stmt)
		}
		if strings.Join(*seen, ",") != "refused/read_only" {
			t.Errorf("%q observed as %v, want one refused/read_only", stmt, *seen)
		}

		// The same text through COM_STMT_PREPARE.
		r = &fakeRouter{toCopy: true, reason: "expensive"}
		h, seen = readOnlyHandler(t, r, f)
		_, _, _, err = h.HandleStmtPrepare(stmt)
		refusedReadOnly(t, "PREPARE "+stmt, err)
		untouched(t, "PREPARE "+stmt, r)
		if strings.Join(*seen, ",") != "refused/read_only" {
			t.Errorf("PREPARE %q observed as %v, want one refused/read_only", stmt, *seen)
		}
	}
}

// A prepared write with placeholders is refused at PREPARE, so there is no
// statement to execute later.
func TestReadOnly_preparedWriteWithPlaceholders(t *testing.T) {
	r := &fakeRouter{}
	h, _ := readOnlyHandler(t, r, &fakeFreeSQL{})
	const stmt = "UPDATE t SET a = ? WHERE id = ?"
	_, _, ctx, err := h.HandleStmtPrepare(stmt)
	refusedReadOnly(t, stmt, err)
	untouched(t, stmt, r)
	if ctx != nil {
		t.Errorf("a refused PREPARE handed back a statement: %v", ctx)
	}
}

// Reads and the control statements around them keep working, and reach the
// source exactly as with the mode off.
func TestReadOnly_readsAndSessionControlStillWork(t *testing.T) {
	r := &fakeRouter{toCopy: false, reason: "cheap"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h, seen := readOnlyHandler(t, r, f)
	stmts := []string{
		"SET NAMES utf8mb4",
		"SET autocommit=1",
		"SET SESSION sql_mode = ''",
		"select @@version_comment limit 1",
		"BEGIN",
		"SELECT * FROM t WHERE id = 1",
		"SAVEPOINT a",
		"ROLLBACK TO SAVEPOINT a",
		"COMMIT",
		"START TRANSACTION READ ONLY",
		"ROLLBACK",
		"SHOW TABLES",
		"DESCRIBE t",
		"EXPLAIN SELECT * FROM t",
		"WITH c AS (SELECT 1) SELECT * FROM c",
		"SELECT 'insert into t; delete from t'",
	}
	for _, stmt := range stmts {
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Errorf("%q: %v", stmt, err)
		}
	}
	if strings.Join(r.forwarded, "|") != strings.Join(stmts, "|") {
		t.Errorf("forwarded = %v, want every statement verbatim", r.forwarded)
	}
	// USE goes through UseDB, as a statement and as COM_INIT_DB.
	if _, err := h.HandleQuery("USE shop"); err != nil {
		t.Errorf("USE shop: %v", err)
	}
	if err := h.UseDB("shop2"); err != nil {
		t.Errorf("COM_INIT_DB: %v", err)
	}
	if strings.Join(r.useDBs, ",") != "shop,shop2" {
		t.Errorf("USE reached the router as %v", r.useDBs)
	}
	for _, s := range *seen {
		if strings.HasPrefix(s, "refused/") {
			t.Errorf("an allowed statement was observed as refused: %v", *seen)
			break
		}
	}
	// A prepared read is prepared on the source and executes there.
	_, _, ctx, err := h.HandleStmtPrepare("SELECT * FROM t WHERE id = ?")
	if err != nil {
		t.Fatalf("prepare a read: %v", err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1)}); err != nil {
		t.Fatalf("execute a read: %v", err)
	}
	if len(r.prepared) != 1 || len(r.prepared[0].executed) != 1 {
		t.Errorf("the prepared read did not run on the source")
	}
}

// An expensive read still goes to the copy under read-only mode.
func TestReadOnly_expensiveReadStillGoesToTheCopy(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "plan cost 20146 >= 10000"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h, _ := readOnlyHandler(t, r, f)
	res, err := h.HandleQuery("SELECT status, count(*) FROM t GROUP BY status")
	if err != nil {
		t.Fatal(err)
	}
	if firstCell(t, res) != "copy" {
		t.Error("the expensive read did not go to the copy")
	}
}

// With the mode off every statement of the refused list is forwarded as
// before: the gate is the flag, not the classifier.
func TestReadOnly_offForwardsWrites(t *testing.T) {
	for _, stmt := range readOnlyRefused {
		r := &fakeRouter{}
		f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
		h := routingHandler(t, r, f, time.Minute)
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Errorf("%q with the mode off: %v", stmt, err)
		}
		if len(r.forwarded) != 1 || r.forwarded[0] != stmt {
			t.Errorf("%q with the mode off: forwarded %v", stmt, r.forwarded)
		}
	}
}
