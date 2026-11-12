package shim

import (
	"encoding/json"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// #2686: a connection answered from the copy alone because its server has no
// account to forward with gets that reason for a statement the copy cannot
// run, instead of the copy's own refusal; reads still reach the copy, and a
// connection with no reason set behaves as before.
func TestCopyOnlyRefusal_2686(t *testing.T) {
	const why = "the server has no forwarding account, and this port never forwards with the account DBTrail captures with. Set it, then reconnect"
	for _, tc := range []struct {
		stmt    string
		refused bool
	}{
		{"INSERT INTO orders VALUES (1)", true},
		{"update orders set x = 1", true},
		{"DELETE FROM orders", true},
		{"CREATE TABLE t (id int)", true},
		{"SELECT id FROM orders FOR UPDATE", true},
		{"SELECT GET_LOCK('a', 1)", true},
		{"SELECT id FROM orders", false},
		{"WITH x AS (SELECT 1) SELECT * FROM x", false},
		{"SHOW TABLES", false},
	} {
		t.Run(tc.stmt, func(t *testing.T) {
			f := &fakeFreeSQL{res: oneCell("id", "INTEGER", json.Number("7"))}
			h := NewHandler(nil, nil)
			h.BindFreeSQL(f)
			h.SetCopyOnlyReason(why)
			_, err := h.HandleQuery(tc.stmt)
			if tc.refused {
				wantMyError(t, err, mysql.ER_OPTION_PREVENTS_STATEMENT, "answered from its copy alone", why, "Nothing was sent to your MySQL")
				if f.gotStmt != "" {
					t.Fatalf("the copy was asked to run %q", f.gotStmt)
				}
				return
			}
			if err != nil && f.gotStmt == "" {
				t.Fatalf("a read did not reach the copy: %v", err)
			}
		})
	}
	// No reason set: the copy is asked, as before.
	f := &fakeFreeSQL{res: oneCell("id", "INTEGER", json.Number("7"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	_, _ = h.HandleQuery("INSERT INTO orders VALUES (1)")
	if f.gotStmt != "INSERT INTO orders VALUES (1)" {
		t.Fatalf("with no reason set the statement did not reach the copy: %q", f.gotStmt)
	}
}
