//go:build integration

package console

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2369 end to end: a statement on the port reads a table with its tail
// over it, so it sees a change capture indexed after the snapshot, and says
// how far that was indexed; with tails off, or for a table capture does not
// record, it reads the published copy.
func TestIntegrationPortReadsTheTail(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on") // tails are opt-in: the fresh mode
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := newTailRig(t)
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true
	captured := true
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) {
		return func(string, string) bool { return captured }, true
	}
	tbl := r.table(nil) // 1=new in the published copy
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	r.change(tbl.Table, event.EventUpdate, "1", "paid", "binlog.000008", 100, at)
	r.change(tbl.Table, event.EventInsert, "2", "two", "binlog.000008", 200, at)

	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	read := func(t *testing.T) []string {
		t.Helper()
		res, err := q.Run(context.Background(), "SELECT CAST(id AS VARCHAR) || '=' || status FROM shop."+tbl.Table+" ORDER BY id", "", sqlsandbox.Session{})
		if err != nil {
			t.Fatal(err)
		}
		q.NoteRoute("copy")
		var out []string
		for _, row := range res.Rows {
			out = append(out, row[0].(string))
		}
		return out
	}
	// settled reads once, so the keeper builds the table's tail in the
	// background (#2483: no statement waits for one) and its follower
	// publishes it (no statement brings it up to date either), and waits for
	// both: an update that starts the build, the build, an update that
	// publishes it.
	settled := func(t *testing.T) []string {
		t.Helper()
		read(t)
		r.s.tails.mu.Lock()
		k := r.s.tails.by[bootServerID]
		r.s.tails.mu.Unlock()
		if k != nil {
			k.followOnce(context.Background(), time.Minute)
			k.settle()
			k.followOnce(context.Background(), time.Minute)
		}
		return read(t)
	}
	status := func() map[string]string {
		out := map[string]string{}
		for _, p := range q.lastAnswerStatus(context.Background()) {
			out[p[0]] = p[1]
		}
		return out
	}

	if got := settled(t); len(got) != 2 || got[0] != "1=paid" || got[1] != "2=two" {
		t.Fatalf("with its tail: %v", got)
	}
	if st := status(); st["last_answer_indexed_through"] != at.Format(time.RFC3339) {
		t.Errorf("indexed through %q, want the newest event's %s", st["last_answer_indexed_through"], at.Format(time.RFC3339))
	}

	// A statement the parse cannot tell the tables of reads no tail: it
	// would build one for every table of the copy.
	res, err := q.Run(context.Background(), "WITH a AS (SELECT * FROM shop."+tbl.Table+"), b AS (SELECT * FROM a) SELECT CAST(id AS VARCHAR) || '=' || status FROM b ORDER BY id", "", sqlsandbox.Session{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].(string) != "1=new" {
		t.Errorf("a statement whose tables the parse could not tell: %v", res.Rows)
	}

	t.Setenv("BINTRAIL_SQL_TAILS", "off")
	if got := read(t); len(got) != 1 || got[0] != "1=new" {
		t.Fatalf("tails off: %v", got)
	}
	os.Setenv("BINTRAIL_SQL_TAILS", "on")

	captured = false
	r.s.tails = tailKeepers{} // a keeper made again, with the filters now
	if got := settled(t); len(got) != 1 || got[0] != "1=new" {
		t.Fatalf("a table capture does not record: %v", got)
	}
}
