package shim

import (
	"strings"
	"testing"
	"time"
)

// A routed statement written with STRAIGHT_JOIN reaches the copy rewritten
// (the copy has no STRAIGHT_JOIN) and asking for its joins in the written
// order, as MySQL makes them; one without reaches it with the copy's own
// join order. The text and the prepared paths alike: what is asked is read
// from the client's statement, not from the rewritten one.
func TestRouter_straightJoinKeepsTheJoinOrderOnTheCopy(t *testing.T) {
	cases := []struct {
		name, stmt string
		keep       bool
	}{
		{"a join", "SELECT a.x, count(*) FROM a STRAIGHT_JOIN b WHERE a.id = b.id GROUP BY a.x", true},
		{"every join, with ON", "SELECT a.x, count(*) FROM a STRAIGHT_JOIN b ON a.id = b.id GROUP BY a.x", true},
		{"mixed with a comma", "SELECT a.x, count(*) FROM a STRAIGHT_JOIN b, c WHERE a.id = b.id AND c.y = a.y GROUP BY a.x", false},
		{"none", "SELECT a.x, count(*) FROM a JOIN b ON a.id = b.id GROUP BY a.x", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &fakeRouter{toCopy: true, reason: "expensive"}
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
			h := NewHandler(nil, nil)
			h.BindFreeSQL(f)
			h.BindRouter(r, RouterConfig{MaxCopyAge: time.Minute})
			if _, err := h.HandleQuery(c.stmt); err != nil {
				t.Fatalf("HandleQuery: %v", err)
			}
			if f.gotStmt == "" {
				t.Fatal("the statement never reached the copy")
			}
			if f.gotSess.KeepJoinOrder != c.keep {
				t.Errorf("text: KeepJoinOrder = %v, want %v", f.gotSess.KeepJoinOrder, c.keep)
			}
			if strings.Contains(strings.ToUpper(f.gotStmt), "STRAIGHT_JOIN") {
				t.Errorf("the copy was sent %q: STRAIGHT_JOIN is not the copy's", f.gotStmt)
			}

			f.gotStmt = ""
			_, _, ctx, err := h.HandleStmtPrepare(strings.Replace(c.stmt, "GROUP BY", "AND a.y = ? GROUP BY", 1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1)}); err != nil {
				t.Fatal(err)
			}
			if f.gotStmt == "" {
				t.Fatal("the prepared statement never reached the copy")
			}
			if f.gotSess.KeepJoinOrder != c.keep {
				t.Errorf("prepared: KeepJoinOrder = %v, want %v", f.gotSess.KeepJoinOrder, c.keep)
			}
		})
	}
}

// What the routing ladder sends to the copy has its join order picked by
// the shape of its FROM (sqlsandbox.Session.JoinOrderByShape), unless the
// client wrote STRAIGHT_JOIN, which keeps the order written as before. A
// port without routing runs every statement as it always did. The text and
// the prepared paths alike.
func TestJoinOrderByShape_onlyForRoutedStatements(t *testing.T) {
	cases := []struct {
		name, stmt  string
		routed      bool
		keep, shape bool
	}{
		{"routed, tables listed with commas", "SELECT a.x, count(*) FROM a, b WHERE a.id = b.id GROUP BY a.x", true, false, true},
		{"routed, a JOIN", "SELECT a.x, count(*) FROM a JOIN b ON a.id = b.id GROUP BY a.x", true, false, true},
		{"routed, STRAIGHT_JOIN", "SELECT a.x, count(*) FROM a STRAIGHT_JOIN b WHERE a.id = b.id GROUP BY a.x", true, true, false},
		{"not routed", "SELECT a.x, count(*) FROM a, b WHERE a.id = b.id GROUP BY a.x", false, false, false},
		{"not routed, STRAIGHT_JOIN", "SELECT a.x, count(*) FROM a STRAIGHT_JOIN b WHERE a.id = b.id GROUP BY a.x", false, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
			h := NewHandler(nil, nil)
			h.BindFreeSQL(f)
			if c.routed {
				h.BindRouter(&fakeRouter{toCopy: true, reason: "expensive"}, RouterConfig{MaxCopyAge: time.Minute})
			}
			if _, err := h.HandleQuery(c.stmt); err != nil {
				t.Fatalf("HandleQuery: %v", err)
			}
			if f.gotStmt == "" {
				t.Fatal("the statement never reached the copy")
			}
			if f.gotSess.JoinOrderByShape != c.shape || (c.routed && f.gotSess.KeepJoinOrder != c.keep) {
				t.Errorf("text: JoinOrderByShape %v KeepJoinOrder %v, want %v %v", f.gotSess.JoinOrderByShape, f.gotSess.KeepJoinOrder, c.shape, c.keep)
			}

			f.gotStmt = ""
			_, _, ctx, err := h.HandleStmtPrepare(strings.Replace(c.stmt, "GROUP BY", "AND a.y = ? GROUP BY", 1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1)}); err != nil {
				t.Fatal(err)
			}
			if f.gotStmt == "" {
				t.Fatal("the prepared statement never reached the copy")
			}
			if f.gotSess.JoinOrderByShape != c.shape || (c.routed && f.gotSess.KeepJoinOrder != c.keep) {
				t.Errorf("prepared: JoinOrderByShape %v KeepJoinOrder %v, want %v %v", f.gotSess.JoinOrderByShape, f.gotSess.KeepJoinOrder, c.shape, c.keep)
			}
		})
	}
}
