package shim

import (
	"encoding/json"
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
	f.err = &sqlsandbox.RefusedError{Reason: "SELECT * on shop.orders would not return the columns MySQL returns"}
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
