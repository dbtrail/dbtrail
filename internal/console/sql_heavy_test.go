package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// Through the real worker: an expensive routed statement
// (sqlsandbox.Session.HeavyOnCopy) is answered past the refusals over how
// the copy computes, sorts or prints a value, and says the first one it was
// let past (Result.Waived). The refusals over which table, columns or names
// it reads still hold.
func TestSQL_realWorkerHeavyOnCopy(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "ev", "CREATE TABLE `ev` (\n  `id` int NOT NULL,\n  `created_on` date DEFAULT NULL,\n"+
		"  `name` varchar(20) DEFAULT NULL,\n  `n` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "2026-01-01", "a-b", "5"})
	ctx := context.Background()
	strict := func(stmt string, heavy bool) sqlsandbox.Session {
		return sqlsandbox.Session{StrictStar: true, HeavyOnCopy: heavy, Types: readrouter.ShapeOf(stmt)}
	}

	for _, c := range []struct {
		stmt, says string // what the refusal and the waiver both name
		rows       string // what the copy answers when let past
	}{
		// A date column's type (#2133): the copy's date, MySQL's number.
		{"SELECT created_on + 1 FROM shop.ev", "created_on", "2026-01-02"},
		// A string with a sign put in order (#2533).
		{"SELECT id FROM shop.ev WHERE name >= 'a-a'", "the copy does not answer this statement", "1"},
	} {
		_, err := f.s.runSQL(ctx, f.s.cm.boot, "u", c.stmt, "", 0, strict(c.stmt, false))
		var refusal *sqlStarRefusal
		if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, c.says) {
			t.Errorf("%s, not heavy: err = %v, want a refusal naming %q (the case tests nothing otherwise)", c.stmt, err, c.says)
			continue
		}
		out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", c.stmt, "", 0, strict(c.stmt, true))
		if err != nil {
			t.Errorf("%s, heavy: %v, want the copy's answer", c.stmt, err)
			continue
		}
		if got := fmt.Sprintf("%v", out.Result.Rows); !strings.Contains(got, c.rows) {
			t.Errorf("%s, heavy: rows %s, want %s", c.stmt, got, c.rows)
		}
		if out.Result.Waived != refusal.Message {
			t.Errorf("%s, heavy: waived %q, want the refusal it was let past, %q", c.stmt, out.Result.Waived, refusal.Message)
		}
	}

	// What changes what the statement reads is refused, heavy or not, with
	// the same refusal.
	for _, stmt := range []string{
		// A name the copy reads as a type (#2131).
		"SELECT n 'label' FROM shop.ev",
		// Both kinds: the one that holds is said.
		"SELECT n 'label', created_on + 1 FROM shop.ev",
	} {
		_, plain := f.s.runSQL(ctx, f.s.cm.boot, "u", stmt, "", 0, strict(stmt, false))
		_, heavy := f.s.runSQL(ctx, f.s.cm.boot, "u", stmt, "", 0, strict(stmt, true))
		var p, h *sqlStarRefusal
		if !errors.As(plain, &p) || !errors.As(heavy, &h) {
			t.Errorf("%s: not heavy %v, heavy %v; want a refusal from both", stmt, plain, heavy)
			continue
		}
		if strings.Contains(h.Message, "date, time or year column") {
			t.Errorf("%s, heavy: refused for %q, the refusal it is let past", stmt, h.Message)
		}
	}

	// Nothing to let past: nothing waived.
	out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT n FROM shop.ev", "", 0, strict("SELECT n FROM shop.ev", true))
	if err != nil || out.Result.Waived != "" {
		t.Errorf("a plain statement, heavy: err %v, waived %q; want an answer with nothing waived", err, out.Result.Waived)
	}

	// Through the port too: the answer reaches the routing ladder with what
	// it was let past.
	stmt := "SELECT created_on + 1 FROM shop.ev"
	res, err := (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, stmt, "", strict(stmt, true))
	if err != nil || !strings.Contains(res.Waived, "created_on") {
		t.Errorf("port: err %v, waived %q; want an answer that says the date column", err, res.Waived)
	}
}
