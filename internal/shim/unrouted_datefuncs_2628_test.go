package shim

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// #2628: a port the copy answers alone writes MySQL's DATE_FORMAT as the
// copy's strftime and MySQL's DATEDIFF(a, b) as date_diff before the copy
// runs them, as text and as a prepared statement (whose arguments are written
// into the text). What it cannot write with certainty is refused with error
// 1235 and the copy's spelling, and never run: the copy has neither function.
// DIV stays refused, now with the copy's spelling for a DECIMAL too.
func TestUnrouted_mysqlDateFunctions(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("n", "INTEGER", json.Number("1"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)

	for stmt, want := range map[string]string{
		"SELECT DATE_FORMAT(d, '%Y-%m-%d %H:%i:%s') FROM t": "SELECT strftime(d, '%Y-%m-%d %H:%M:%S') FROM t",
		"SELECT DATE_FORMAT(`d`, '%M %e') FROM `t`":         `SELECT strftime("d", '%B %-d') FROM "t"`,
		"SELECT DATEDIFF(a, b) FROM t":                      "SELECT date_diff('day', CAST(b AS DATE), CAST(a AS DATE)) FROM t",
		"SELECT DATEDIFF(a, b) FROM t LIMIT 1, 2":           "SELECT date_diff('day', CAST(b AS DATE), CAST(a AS DATE)) FROM t LIMIT 2 OFFSET 1",
		"SELECT datediff('day', a, b) FROM t":               "SELECT datediff('day', a, b) FROM t",
		"SELECT strftime(d, '%Y') FROM t":                   "SELECT strftime(d, '%Y') FROM t",
	} {
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Errorf("%q: %v", stmt, err)
			continue
		}
		if f.gotStmt != want {
			t.Errorf("%q: the copy ran %q, want %q", stmt, f.gotStmt, want)
		}
	}

	refused := func(label, want string, err error) {
		t.Helper()
		var me *mysql.MyError
		if !errors.As(err, &me) || me.Code != mysql.ER_NOT_SUPPORTED_YET || !strings.Contains(me.Message, want) {
			t.Errorf("%s: err %v, want the 1235 refusal naming %q", label, err, want)
		}
	}
	before := f.calls
	for stmt, want := range map[string]string{
		"SELECT DATE_FORMAT(d, '%D') FROM t":                   "strftime(d, format)",
		"SELECT DATE_FORMAT(d, f) FROM t":                      "strftime(d, format)",
		"SELECT DATE_FORMAT(d, '%Y') FROM t # a MySQL comment": "strftime(d, format)", // the names rewrite refuses it: sent as written
		"SELECT DATE_FORMAT(d, '%Y') FROM t WHERE b = '$x$'":   "strftime(d, format)", // copyReadsOtherwise: sent as written
		"SELECT DATEDIFF(a, b) FROM t WHERE b = '$x$'":         "date_diff('day', CAST(b AS DATE), CAST(a AS DATE))",
		"SELECT dc DIV 2 FROM t":                               "trunc(a / nullif(b, 0))",
	} {
		_, err := h.HandleQuery(stmt)
		refused(stmt, want, err)
	}
	if f.calls != before {
		t.Errorf("the copy ran %d refused statement(s)", f.calls-before)
	}

	// A prepared statement runs as text here, its arguments written in.
	_, _, ctx, err := h.HandleStmtPrepare("SELECT DATE_FORMAT(d, ?), DATEDIFF(d, ?) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{"%Y-%m", "2026-01-01"}); err != nil {
		t.Fatalf("prepared: %v", err)
	}
	if want := "SELECT strftime(d, '%Y-%m'), date_diff('day', CAST('2026-01-01' AS DATE), CAST(d AS DATE)) FROM t"; f.gotStmt != want {
		t.Errorf("prepared: the copy ran %q, want %q", f.gotStmt, want)
	}
	before = f.calls
	for _, args := range [][]any{{"%D", "2026-01-01"}, {nil, "2026-01-01"}} {
		_, err := h.HandleStmtExecute(ctx, "", args)
		refused("prepared", "strftime(d, format)", err)
	}
	if f.calls != before {
		t.Errorf("the copy ran %d refused prepared statement(s)", f.calls-before)
	}
}
