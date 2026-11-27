package shim

import (
	"strings"
	"testing"
	"time"
)

// likeHandler is routingHandler with the server's LIKE switch set as on.
func likeHandler(t *testing.T, on bool) (*Handler, *fakeFreeSQL) {
	t.Helper()
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	h.BindRouter(r, RouterConfig{MaxCopyAge: time.Minute, LikeIgnoresCase: on})
	if err := h.UseDB("shop"); err != nil {
		t.Fatal(err)
	}
	return h, f
}

// The server's switch decides how the copy is sent a statement with LIKE:
// off, as written, and only because its plan is expensive (heavy.go: the
// copy compares with case, so its answer may differ; a cheap one is MySQL's,
// TestRouter_heavyOnCopy_vetoStands); on, as ILIKE.
func TestRouter_likeFollowsTheServersSwitch(t *testing.T) {
	const stmt = "SELECT count(*) FROM `items` WHERE `name` LIKE 'PR%'"
	h, f := likeHandler(t, false)
	res, err := h.HandleQuery(stmt)
	if err != nil {
		t.Fatal(err)
	}
	if got := firstCell(t, res); got != "copy" || f.calls != 1 {
		t.Errorf("switch off: answered by %s with %d copy calls, want the copy once", got, f.calls)
	}
	if want := `SELECT count(*) FROM "items" WHERE "name" LIKE 'PR%'`; f.gotStmt != want {
		t.Errorf("switch off: the copy got %q, want %q", f.gotStmt, want)
	}

	h, f = likeHandler(t, true)
	res, err = h.HandleQuery(stmt)
	if err != nil {
		t.Fatal(err)
	}
	if got := firstCell(t, res); got != "copy" {
		t.Fatalf("switch on: answered by %s, want the copy", got)
	}
	if want := `SELECT count(*) FROM "items" WHERE "name" ILIKE 'PR%'`; f.gotStmt != want {
		t.Errorf("the copy got %q, want %q", f.gotStmt, want)
	}
	// REGEXP is not LIKE: MySQL either way.
	calls := f.calls
	res, err = h.HandleQuery("SELECT count(*) FROM items WHERE name REGEXP '^pr'")
	if err != nil {
		t.Fatal(err)
	}
	if got := firstCell(t, res); got != "mysql" || f.calls != calls {
		t.Errorf("REGEXP with the switch on: answered by %s, want MySQL", got)
	}
}

// A prepared statement with LIKE: the template is sent ILIKE with the switch
// on, and an argument with a backslash keeps it on MySQL, where the backslash
// escapes the next character of the pattern and on the copy it would not.
func TestPreparedRouted_likeFollowsTheServersSwitch(t *testing.T) {
	run := func(h *Handler, ctx any, args ...any) string {
		t.Helper()
		res, err := h.HandleStmtExecute(ctx, "", args)
		if err != nil {
			t.Fatal(err)
		}
		return binaryFirstCell(t, res)
	}
	const q = "SELECT count(*) FROM items WHERE name LIKE ?"

	h, f := likeHandler(t, false)
	_, _, ctx, err := h.HandleStmtPrepare(q)
	if err != nil {
		t.Fatal(err)
	}
	if got := run(h, ctx, strArg("pr%")); got != "mysql" || f.calls != 0 {
		t.Errorf("switch off: answered by %s, want MySQL", got)
	}

	h, f = likeHandler(t, true)
	_, _, ctx, err = h.HandleStmtPrepare(q)
	if err != nil {
		t.Fatal(err)
	}
	if got := run(h, ctx, strArg("pr%")); got != "copy" {
		t.Fatalf("switch on: answered by %s, want the copy", got)
	}
	if !strings.Contains(squash(f.gotStmt), "ILIKE 'pr%'") {
		t.Errorf("the copy got %q, want ILIKE with the argument", f.gotStmt)
	}
	calls := f.calls
	for _, arg := range []string{`a\_c`, `100\%`, `\\`} {
		if got := run(h, ctx, strArg(arg)); got != "mysql" {
			t.Errorf("argument %q answered by %s, want MySQL", arg, got)
		}
	}
	if f.calls != calls {
		t.Errorf("the copy was asked %d times for a backslash pattern", f.calls-calls)
	}

	// A backslash in an argument of a statement without LIKE is not this
	// rule's: the copy answers it as before.
	_, _, ctx, err = h.HandleStmtPrepare("SELECT count(*) FROM items WHERE name = ?")
	if err != nil {
		t.Fatal(err)
	}
	if got := run(h, ctx, strArg(`a\b`)); got != "copy" {
		t.Errorf("backslash without LIKE answered by %s, want the copy", got)
	}
}
