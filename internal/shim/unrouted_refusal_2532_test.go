package shim

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// #2532: a port the copy answers alone refuses a statement written for
// MySQL that the copy would answer differently, with what to write instead,
// and never runs it. What is written the copy's way is answered.
func TestUnrouted_refusesMySQLOnlyConstructs(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("n", "INTEGER", json.Number("1"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	for _, stmt := range []string{"SELECT WEEK(d) FROM t", "SELECT a DIV 2 FROM t", "SELECT GROUP_CONCAT(a) FROM t"} {
		_, err := h.HandleQuery(stmt)
		var me *mysql.MyError
		if !errors.As(err, &me) || me.Code != mysql.ER_NOT_SUPPORTED_YET || !strings.Contains(me.Message, "Write it for the copy instead") || !strings.Contains(me.Message, "Engine Routing") {
			t.Errorf("%q: err %v, want a 1235 refusal that says what to write instead", stmt, err)
		}
	}
	if f.calls != 0 {
		t.Errorf("the copy ran %d refused statement(s)", f.calls)
	}
	for _, stmt := range []string{"SELECT weekofyear(d) FROM t", "SELECT a // 2 FROM t", "SELECT name FROM t WHERE name LIKE '%x%'", "SELECT COUNT(DISTINCT name) FROM t"} {
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Errorf("%q: %v, want it answered", stmt, err)
		}
	}
	if f.calls != 4 {
		t.Errorf("the copy ran %d statement(s), want 4", f.calls)
	}
	// A prepared statement runs as text on this port: refused the same way.
	_, _, ctx, err := h.HandleStmtPrepare("SELECT a DIV 2 FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1)}); err == nil || !strings.Contains(err.Error(), "Write it for the copy instead") {
		t.Errorf("prepared DIV: err %v, want the refusal", err)
	}
	// Driver chatter still gets its answers.
	for _, stmt := range []string{"SELECT @@version_comment LIMIT 1", "SET NAMES utf8mb4"} {
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Errorf("%q: %v", stmt, err)
		}
	}
}
