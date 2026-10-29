package console

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The port's ask for the joins in the order written (a STRAIGHT_JOIN,
// sqlsandbox.Session.KeepJoinOrder) reaches the copy's engine through the
// console's run, and a statement without it plans as always.
func TestSQLOnCopy_keepJoinOrderReachesTheEngine(t *testing.T) {
	f := newSQLFixture(t, sandboxRunner{sqlsandbox.New(sqlsandbox.Config{})}, false)
	q := &SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}
	for _, keep := range []bool{true, false} {
		res, err := q.Run(context.Background(), "SELECT current_setting('disabled_optimizers')", "", sqlsandbox.Session{KeepJoinOrder: keep})
		if err != nil {
			t.Fatalf("keep %v: %v", keep, err)
		}
		off := strings.ToLower(fmt.Sprint(res.Rows[0][0]))
		if strings.Contains(off, "join_order") != keep {
			t.Errorf("keep %v: the engine ran with disabled_optimizers = %q", keep, off)
		}
	}
}

// The port's ask for a result in a file (sqlsandbox.Session.ResultToFile,
// #2575) reaches the engine as a file cap and no row cap: a result past the
// memory cap and past the port's row cap comes back whole, in a file. A
// statement that does not ask keeps both caps.
func TestSQLOnCopy_resultToFileReachesTheEngine(t *testing.T) {
	limits := sqlsandbox.DefaultLimits()
	limits.MaxResultBytes = 4 << 10
	f := newSQLFixture(t, sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Limits: limits, SpillDir: t.TempDir()})}, false)
	q := &SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}
	stmt := fmt.Sprintf("SELECT i, 'row ' || i AS s FROM range(%d) t(i)", DefaultSQLPortMaxRows+500)
	if !q.ResultFileRoom() {
		t.Fatal("the port is told there is no disk for a result file on a disk that has it")
	}

	res, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{ResultToFile: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Spool == nil {
		t.Fatalf("not in a file: %d rows in memory, truncated %v", len(res.Rows), res.Truncated)
	}
	defer res.Spool.Close()
	if res.Spool.Rows() != int64(DefaultSQLPortMaxRows+500) || res.Truncated {
		t.Errorf("%d rows, truncated %v; want every row", res.Spool.Rows(), res.Truncated)
	}

	if res, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{}); err == nil && res.Spool == nil && !res.Truncated {
		t.Errorf("without the ask: %d rows, not cut, no error; want the caps to hold", len(res.Rows))
	} else if err == nil && res.Spool != nil {
		res.Spool.Close()
		t.Error("a statement that did not ask came back in a file")
	}
}

// A prepared statement's result is held in memory up to 8 MB, then comes
// from a file (#2575): its binary encoding holds it twice. Nothing else is
// held to less than the runner's cap, and a runner cap below 8 MB stands.
func TestSQLResultMemoryBytes(t *testing.T) {
	s := &Server{sqlLimits: sqlsandbox.Limits{MaxResultBytes: 64 << 20}}
	for _, c := range []struct {
		sess sqlsandbox.Session
		want int64
	}{
		{sqlsandbox.Session{}, 0},
		{sqlsandbox.Session{ResultToFile: true}, 0},
		{sqlsandbox.Session{BinaryResult: true}, 0}, // no file to send the rest from
		{sqlsandbox.Session{ResultToFile: true, BinaryResult: true}, sqlPortBinaryMemoryBytes},
	} {
		if got := s.sqlResultMemoryBytes(c.sess); got != c.want {
			t.Errorf("%+v: %d, want %d", c.sess, got, c.want)
		}
	}
	s.sqlLimits.MaxResultBytes = 1 << 20
	if got := s.sqlResultMemoryBytes(sqlsandbox.Session{ResultToFile: true, BinaryResult: true}); got != 1<<20 {
		t.Errorf("under a 1 MB runner cap: %d", got)
	}
}

// The 8 MB reach the engine: a result of about 10 MB, held in memory for a
// text statement, comes from a file for a prepared one.
func TestSQLOnCopy_binaryResultIsHeldToLessMemory(t *testing.T) {
	f := newSQLFixture(t, sandboxRunner{sqlsandbox.New(sqlsandbox.Config{SpillDir: t.TempDir()})}, false)
	q := &SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}
	const stmt = "SELECT i, repeat('x', 1000) AS s FROM range(10000) t(i)"
	text, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{ResultToFile: true})
	if err != nil {
		t.Fatal(err)
	}
	if text.Spool != nil {
		text.Spool.Close()
		t.Fatal("a text statement's 10 MB went to a file")
	}
	bin, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{ResultToFile: true, BinaryResult: true})
	if err != nil {
		t.Fatal(err)
	}
	if bin.Spool == nil {
		t.Fatalf("a prepared statement's 10 MB stayed in memory (%d rows)", len(bin.Rows))
	}
	bin.Spool.Close()
}
