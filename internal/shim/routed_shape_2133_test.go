package shim

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// #2133: the copy is handed the statement written for it (names requoted, a
// prepared statement's arguments written in), which is not the text the
// routing layer read. So it is also handed that reading, the statement's
// shape, to look for arithmetic on the date columns of the tables it reads:
// of the client's own text, and of a prepared statement's template.
func TestRouter_handsTheCopyTheStatementsShape(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	const text = "SELECT `created_on` + 1, 'a + b' FROM orders /* c */"
	if _, err := h.HandleQuery(text); err != nil {
		t.Fatal(err)
	}
	if want := readrouter.ShapeOf(text); f.calls != 1 || f.gotSess.Types != want || want != `SELECT "created_on" + 1, '' FROM orders  ` {
		t.Errorf("text: copy calls = %d, shape = %q; want one call with %q", f.calls, f.gotSess.Types, want)
	}

	const template = "SELECT `created_on` + ? FROM orders WHERE id > ?"
	_, _, ctx, err := h.HandleStmtPrepare(template)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1), int64(5)}); err != nil {
		t.Fatal(err)
	}
	// The template's shape, with its placeholders: the arguments are not in
	// it, so a number bound next to a date column still reads as arithmetic.
	if want := readrouter.ShapeOf(template); f.calls != 2 || f.gotSess.Types != want || want != `SELECT "created_on" + ? FROM orders WHERE id > ?` {
		t.Errorf("prepared: copy calls = %d, shape = %q; want a second call with %q", f.calls, f.gotSess.Types, want)
	}

	// A port with no routing asks for nothing.
	plain := &fakeFreeSQL{res: oneCell("x", "INTEGER", "1")}
	h = NewHandler(nil, nil)
	h.BindFreeSQL(plain)
	if _, err := h.HandleQuery("SELECT created_on + 1 FROM orders"); err != nil {
		t.Fatal(err)
	}
	if plain.calls != 1 || plain.gotSess.Types != nil || plain.gotSess.StrictStar {
		t.Errorf("no router: copy calls = %d, session = %+v; want one call with no shape", plain.calls, plain.gotSess)
	}
}

// A string argument that starts with a two-digit year is the year 2026 on
// MySQL and the year 26 on the copy: the text veto reads the statement and
// never sees an argument, so the execution is kept on MySQL here.
func TestPreparedRouted_twoDigitYearArgument(t *testing.T) {
	r := &fakeRouter{toCopy: true}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	_, _, ctx, err := h.HandleStmtPrepare("SELECT count(*) FROM t WHERE d = ?")
	if err != nil {
		t.Fatal(err)
	}
	for arg, want := range map[string]string{"26-01-15": "mysql", " 26/1/5 10:00:00": "mysql", "2026-01-15": "copy", "1-2-3": "copy", "12-AB": "copy"} {
		res, err := h.HandleStmtExecute(ctx, "", []any{strArg(arg)})
		if err != nil {
			t.Fatal(err)
		}
		if got := binaryFirstCell(t, res); got != want {
			t.Errorf("string argument %q answered by %s, want %s", arg, got, want)
		}
	}
	// A DATE argument carries its whole year.
	res, err := h.HandleStmtExecute(ctx, "", []any{dateTimeArg(10, 2026, 1, 15)})
	if err != nil {
		t.Fatal(err)
	}
	if got := binaryFirstCell(t, res); got != "copy" {
		t.Errorf("a DATE argument was answered by %s, want copy", got)
	}
}
