package shim

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// #2790: a port the copy answers alone refuses MySQL's DATEDIFF of a string
// with a UTC offset after its time, as text and as a prepared statement whose
// argument holds it, and never runs it: the copy would answer another day
// than MySQL, without an error. A string MySQL reads no offset in (a Z, a
// space before the offset) is still written as date_diff and answered.
func TestUnrouted_dateDiffOffsetRefused(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("n", "INTEGER", json.Number("1"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)

	refused := func(label, want string, err error) {
		t.Helper()
		var me *mysql.MyError
		if !errors.As(err, &me) || me.Code != mysql.ER_NOT_SUPPORTED_YET || !strings.Contains(me.Message, want) {
			t.Errorf("%s: err %v, want the 1235 refusal naming %q", label, err, want)
		}
	}
	const offsetRefusal = "CAST(CAST(a AS TIMESTAMPTZ) AS DATE)"
	before := f.calls
	for stmt, want := range map[string]string{
		"SELECT DATEDIFF('2026-01-15 02:00:00+05:00', d) FROM t":             offsetRefusal,
		"SELECT datediff(d, ' 2026-01-15 02:00:00-05:00 ') FROM t":           offsetRefusal,
		"SELECT DATEDIFF(COALESCE('2026-01-15 02:00:00+0500', d), d) FROM t": offsetRefusal,
		"SELECT DATEDIFF(d, '2026-01-15 02:00:00+05:00') FROM t LIMIT 1, 2":  offsetRefusal,
		// Sent as written (a backslash): refused either way, never run.
		`SELECT DATEDIFF(d, '2026-01-15 02:00:00+05:00') FROM t WHERE s = 'C:\temp'`: "DATEDIFF",
	} {
		_, err := h.HandleQuery(stmt)
		refused(stmt, want, err)
	}
	if f.calls != before {
		t.Errorf("the copy ran %d refused statement(s)", f.calls-before)
	}

	// A prepared statement runs as text here, its arguments written in.
	_, _, ctx, err := h.HandleStmtPrepare("SELECT DATEDIFF(d, ?) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	before = f.calls
	for _, arg := range []string{"2026-01-15 02:00:00+05:00", "2026-01-15 02:00:00.5-00:00"} {
		_, err := h.HandleStmtExecute(ctx, "", []any{arg})
		refused("prepared "+arg, offsetRefusal, err)
	}
	if f.calls != before {
		t.Errorf("the copy ran %d refused prepared statement(s)", f.calls-before)
	}

	for stmt, want := range map[string]string{
		"SELECT DATEDIFF('2026-01-15 02:00:00Z', d) FROM t":          "SELECT date_diff('day', CAST(d AS DATE), CAST('2026-01-15 02:00:00Z' AS DATE)) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00 +05:00', d) FROM t":    "SELECT date_diff('day', CAST(d AS DATE), CAST('2026-01-15 02:00:00 +05:00' AS DATE)) FROM t",
		"SELECT DATEDIFF(d, dt), '2026-01-15 02:00:00+05:00' FROM t": "SELECT date_diff('day', CAST(dt AS DATE), CAST(d AS DATE)), '2026-01-15 02:00:00+05:00' FROM t",
	} {
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Errorf("%q: %v", stmt, err)
			continue
		}
		if f.gotStmt != want {
			t.Errorf("%q: the copy ran %q, want %q", stmt, f.gotStmt, want)
		}
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{"2026-01-15 02:00:00Z"}); err != nil {
		t.Errorf("prepared Z: %v", err)
	}
}
