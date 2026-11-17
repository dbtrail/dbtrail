//go:build integration

package console

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2756 end to end: in a process that asked for PostgreSQL tails, a
// statement on the copy of a PostgreSQL server, run by PostgreSQL's rules,
// reads a table with its tail over it: the changes captured after the
// snapshot, up to the capture's saved position. In a process that did not
// ask, the same statement reads the published copy, as before.
func TestIntegrationPGStatementReadsTheTail(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r := newPGTailRig(t)
	r.b.pgSource = true // the boot server's family is its bundle's
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) { return nil, true }

	// Two keys that differ only in case: under PostgreSQL's rules they are
	// two rows, and a change of one leaves the other.
	tbl := r.table(nil, [2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"k", "lower"}, [2]string{"K", "upper"})
	r.change(tbl.Table, event.EventUpdate, "1", "paid", 0x610, 20*time.Minute)
	r.change(tbl.Table, event.EventInsert, "3", "three", 0x620, 21*time.Minute)
	r.change(tbl.Table, event.EventDelete, "2", "two", 0x630, 21*time.Minute)
	r.change(tbl.Table, event.EventUpdate, "K", "UPPER", 0x640, 22*time.Minute)
	r.checkpoint(0x640)
	// Visible in the index, past the saved position.
	r.change(tbl.Table, event.EventUpdate, "1", "not saved yet", 0x700, 23*time.Minute)

	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	read := func(t *testing.T) []string {
		t.Helper()
		res, err := q.Run(context.Background(), "SELECT id || '=' || status FROM public."+tbl.Table+" ORDER BY id", "", sqlsandbox.Session{PostgresRules: true})
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
	// settled: a read starts the table's build, the follower publishes it.
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
	published := []string{"1=one", "2=two", "K=upper", "k=lower"}

	// Not asked for: the published copy, as before this change.
	if got := settled(t); !reflect.DeepEqual(got, published) {
		t.Fatalf("in a process that did not ask for PostgreSQL tails: %v, want %v", got, published)
	}

	EnablePostgresTails(true)
	t.Cleanup(func() { EnablePostgresTails(false) })
	r.s.tails = tailKeepers{} // the keeper made again, a PostgreSQL one now
	if got, want := settled(t), []string{"1=paid", "3=three", "K=UPPER", "k=lower"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("with its tail: %v, want %v", got, want)
	}
	r.s.tails.mu.Lock()
	k := r.s.tails.by[bootServerID]
	r.s.tails.mu.Unlock()
	if k == nil {
		t.Fatal("no keeper for the server")
	}
	if _, ok := k.src.(*pgTailSource); !ok {
		t.Fatalf("the server's keeper reads through %T", k.src)
	}

	// The position is saved past the last change: it shows.
	r.checkpoint(0x700)
	if got, want := settled(t), []string{"1=not saved yet", "3=three", "K=UPPER", "k=lower"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("once the position is saved: %v, want %v", got, want)
	}

	// Fast mode: the published copy.
	t.Setenv("BINTRAIL_SQL_TAILS", "off")
	if got := read(t); !reflect.DeepEqual(got, published) {
		t.Fatalf("tails off: %v, want %v", got, published)
	}
}

// A table published as its previous file and a table delta beside it
// (#2757) is read with both, and in fresh mode with its tail over them:
// what the snapshot update left, and what was captured after it.
func TestIntegrationPGStatementReadsTheTailOverATableDelta(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r := newPGTailRig(t)
	r.b.pgSource = true
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) { return nil, true }
	EnablePostgresTails(true)
	t.Cleanup(func() { EnablePostgresTails(false) })

	tbl := r.table(nil, [2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"3", "three"})
	// A snapshot update: row 1 changed, row 2 deleted, row 9 inserted. The
	// file stays, the delta holds the three.
	r.change(tbl.Table, event.EventUpdate, "1", "ONE", 0x610, 20*time.Minute)
	r.change(tbl.Table, event.EventDelete, "2", "two", 0x620, 20*time.Minute)
	r.change(tbl.Table, event.EventInsert, "9", "nine", 0x630, 20*time.Minute)
	r.pair(tbl, map[string]string{"1": "ONE", "2": "", "9": "nine"})
	// After it: row 9 changes again, row 3 is deleted, row 4 arrives.
	r.update(tbl.Table, "9", "nine", "NINE", 0x700, 22*time.Minute)
	r.change(tbl.Table, event.EventDelete, "3", "three", 0x710, 22*time.Minute)
	r.change(tbl.Table, event.EventInsert, "4", "four", 0x720, 22*time.Minute)
	r.checkpoint(0x720)

	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	read := func(t *testing.T) []string {
		t.Helper()
		res, err := q.Run(context.Background(), "SELECT id || '=' || status FROM public."+tbl.Table+" ORDER BY id", "", sqlsandbox.Session{PostgresRules: true})
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
	settled := func(t *testing.T) []string {
		t.Helper()
		read(t)
		r.s.tails.mu.Lock()
		k := r.s.tails.by[bootServerID]
		r.s.tails.mu.Unlock()
		if k == nil {
			t.Fatal("no keeper for the server")
		}
		k.followOnce(context.Background(), time.Minute)
		k.settle()
		k.followOnce(context.Background(), time.Minute)
		return read(t)
	}
	if got, want := settled(t), []string{"1=ONE", "4=four", "9=NINE"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fresh, over the file and its delta: %v, want %v", got, want)
	}
	// Fast mode: the file and its delta, as the snapshot update left them.
	t.Setenv("BINTRAIL_SQL_TAILS", "off")
	if got, want := read(t), []string{"1=ONE", "3=three", "9=nine"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fast, the file and its delta: %v, want %v", got, want)
	}
}
