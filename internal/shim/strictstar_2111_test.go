package shim

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2111: under routing the client expects MySQL's answer, so the copy is
// asked to refuse a star it cannot answer as MySQL would
// (sqlsandbox.Session.StrictStar) and the refusal is forwarded like any
// other. With no router the copy answers as it always did.
func TestRouter_asksTheCopyForMySQLsStar(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	if _, err := h.HandleQuery("SELECT * FROM orders"); err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 || !f.gotSess.StrictStar {
		t.Errorf("routed: copy calls = %d, session = %+v; want one call with StrictStar set", f.calls, f.gotSess)
	}

	// The copy refusing for that reason is a fallback, like every refusal.
	f.err = &sqlsandbox.ColumnsDifferError{Reason: "SELECT * on shop.orders would not return the columns MySQL returns"}
	res, err := h.HandleQuery("SELECT * FROM orders")
	if err != nil {
		t.Fatalf("the copy's refusal reached the client: %v", err)
	}
	if firstCell(t, res) != "mysql" || len(r.forwarded) != 1 {
		t.Errorf("the refused statement was not forwarded: forwarded = %q", r.forwarded)
	}

	plain := &fakeFreeSQL{res: oneCell("x", "INTEGER", json.Number("1"))}
	h = NewHandler(nil, nil)
	h.BindFreeSQL(plain)
	if _, err := h.HandleQuery("SELECT * FROM orders"); err != nil {
		t.Fatal(err)
	}
	if plain.calls != 1 || plain.gotSess.StrictStar {
		t.Errorf("no router: copy calls = %d, session = %+v; want one call with StrictStar off", plain.calls, plain.gotSess)
	}
}

// The copy declining a star is a decision, not a fault: it is counted under
// its own reason and warned about under its own key, so that it neither
// reads as "the copy is broken" nor uses up the one warning a connection
// gets for a real fault (the warnings are logged once per connection per
// kind, and a pooled connection lives for hours).
func TestRouter_columnsDifferIsItsOwnReason(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{err: &sqlsandbox.ColumnsDifferError{Reason: "SELECT * on shop.gen would not return the columns MySQL returns"}, updatedAt: time.Now()}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	if _, err := h.HandleQuery("SELECT * FROM gen"); err != nil {
		t.Fatalf("the copy's refusal reached the client: %v", err)
	}
	if len(got) != 1 || got[0] != "mysql/copy_columns_differ" {
		t.Errorf("observed %v, want exactly [mysql/copy_columns_differ]", got)
	}
	if h.routeWarned["copy"] || !h.routeWarned["columns"] {
		t.Errorf("warned keys = %v, want the refusal under its own key and the fault's key unused", h.routeWarned)
	}
	// A real fault afterwards on the same connection still gets its warning
	// and its own reason.
	f.err = errors.New("Binder Error: no such function")
	if _, err := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a"); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1] != "mysql/copy_refused" || !h.routeWarned["copy"] {
		t.Errorf("after a real fault: observed %v, warned %v; want mysql/copy_refused and the fault's key used", got, h.routeWarned)
	}
}

// A prepared statement reaches the copy through the same call, so it asks
// for MySQL's star too, and its refusal is forwarded under the same reason.
func TestPreparedRouted_asksTheCopyForMySQLsStar(t *testing.T) {
	r := &fakeRouter{toCopy: true}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	_, _, ctx, err := h.HandleStmtPrepare("SELECT * FROM orders WHERE id > ?")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(5)}); err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 || !f.gotSess.StrictStar {
		t.Errorf("prepared: copy calls = %d, session = %+v; want one call with StrictStar set", f.calls, f.gotSess)
	}
	f.err = &sqlsandbox.ColumnsDifferError{Reason: "SELECT * on shop.orders would not return the columns MySQL returns"}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(5)}); err != nil {
		t.Fatalf("the copy's refusal reached the client: %v", err)
	}
	if got[len(got)-1] != "mysql/copy_columns_differ" {
		t.Errorf("observed %v, want the last one mysql/copy_columns_differ", got)
	}
}
