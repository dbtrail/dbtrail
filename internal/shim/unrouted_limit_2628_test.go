package shim

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// #2628: a port the copy answers alone writes MySQL's LIMIT offset, count as
// LIMIT count OFFSET offset before the copy runs it, as text and as a
// prepared statement (whose arguments are written into the text). What it
// cannot write with certainty is refused with error 1235 and the copy's
// spelling, and never run: the copy would only fail on it in its own words.
func TestUnrouted_limitOffsetComma(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("n", "INTEGER", json.Number("1"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)

	for stmt, want := range map[string]string{
		"SELECT a FROM t LIMIT 10, 5":                             "SELECT a FROM t LIMIT 5 OFFSET 10",
		"SELECT `a` FROM `t` LIMIT 0,2":                           `SELECT "a" FROM "t" LIMIT 2 OFFSET 0`,
		"SELECT a FROM t LIMIT 5 OFFSET 10":                       "SELECT a FROM t LIMIT 5 OFFSET 10",
		"SELECT * FROM (SELECT a FROM t LIMIT 1, 2) d LIMIT 0, 1": "SELECT * FROM (SELECT a FROM t LIMIT 2 OFFSET 1) d LIMIT 1 OFFSET 0",
	} {
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Errorf("%q: %v", stmt, err)
			continue
		}
		if f.gotStmt != want {
			t.Errorf("%q: the copy ran %q, want %q", stmt, f.gotStmt, want)
		}
	}

	refusedLimit := func(label string, err error) {
		t.Helper()
		var me *mysql.MyError
		if !errors.As(err, &me) || me.Code != mysql.ER_NOT_SUPPORTED_YET || !strings.Contains(me.Message, "LIMIT count OFFSET offset") {
			t.Errorf("%s: err %v, want the 1235 refusal with the copy's spelling", label, err)
		}
	}
	before := f.calls
	for _, stmt := range []string{
		"SELECT a FROM t LIMIT 1 /* c */ , 2",
		"SELECT a FROM t LIMIT 1, 18446744073709551615",
		"SELECT a FROM t LIMIT 1, 2 # a MySQL comment", // the names rewrite refuses it: sent as written
		"SELECT a FROM t WHERE b = '$x$' LIMIT 1, 2",   // copyReadsOtherwise: sent as written
	} {
		_, err := h.HandleQuery(stmt)
		refusedLimit(stmt, err)
	}

	// A prepared statement runs as text here, its arguments written in.
	_, _, ctx, err := h.HandleStmtPrepare("SELECT a FROM t WHERE b = ? LIMIT ?, ?")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{"x", int64(20), int64(10)}); err != nil {
		t.Fatalf("prepared LIMIT ?, ?: %v", err)
	}
	if want := "SELECT a FROM t WHERE b =  'x'  LIMIT 10 OFFSET 20 "; f.gotStmt != want {
		t.Errorf("prepared LIMIT ?, ?: the copy ran %q, want %q", f.gotStmt, want)
	}
	before++
	for _, args := range [][]any{{"x", nil, int64(10)}, {"x", int64(-1), int64(10)}, {"x", "20", int64(10)}} {
		_, err := h.HandleStmtExecute(ctx, "", args)
		refusedLimit("prepared with arguments", err)
	}
	// A DOUBLE or a DECIMAL argument, even a whole one, is refused as MySQL
	// refuses it (1210): written in, 2.0 would be the number 2.
	for _, args := range [][]any{
		{"x", 2.0, int64(10)}, {"x", int64(1), 2.5}, {"x", float32(1), int64(1)},
		{"x", int64(1), mysql.TypedBytes{Type: mysql.MYSQL_TYPE_NEWDECIMAL, Bytes: []byte("2")}},
	} {
		_, err := h.HandleStmtExecute(ctx, "", args)
		var me *mysql.MyError
		if !errors.As(err, &me) || me.Code != mysql.ER_WRONG_ARGUMENTS || !strings.Contains(me.Message, "Incorrect arguments to mysqld_stmt_execute") {
			t.Errorf("prepared with %v: err %v, want MySQL's 1210", args, err)
		}
	}
	// Not a LIMIT argument: a DOUBLE elsewhere is written in as before.
	_, _, ctx2, err := h.HandleStmtPrepare("SELECT a FROM t WHERE b = ? LIMIT 1, 2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx2, "", []any{2.0}); err != nil {
		t.Errorf("a DOUBLE outside LIMIT: %v", err)
	}
	before++
	if f.calls != before {
		t.Errorf("the copy ran %d refused statement(s)", f.calls-before)
	}

	// MySQL refuses a LIMIT in an IN subquery (error 1235); so does this port.
	_, err = h.HandleQuery("SELECT a FROM t WHERE a IN (SELECT a FROM u LIMIT 2)")
	var me *mysql.MyError
	if !errors.As(err, &me) || me.Code != mysql.ER_NOT_SUPPORTED_YET || !strings.Contains(me.Message, "LIMIT & IN/ALL/ANY/SOME subquery") {
		t.Errorf("LIMIT in IN: err %v, want MySQL's 1235", err)
	}
	if f.calls != before {
		t.Errorf("the copy ran the LIMIT in IN")
	}
}
