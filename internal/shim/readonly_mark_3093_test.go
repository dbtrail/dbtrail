package shim

import (
	"reflect"
	"testing"
	"time"
)

// #3093: the statements that would make the source session writable again
// are answered by the port and never reach the source. The sequence is the
// one a Java driver sends around a read-only transaction and when its pool
// takes a connection back: none of it is an error, the reads between are
// forwarded, and the rest of a SET that also holds something else still
// reaches the source.
func TestReadOnly_theMarkIsAnsweredByThePort_3093(t *testing.T) {
	r := &fakeRouter{toCopy: false, reason: "cheap"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h, seen := readOnlyHandler(t, r, f)
	for _, stmt := range []string{
		"SET SESSION TRANSACTION READ ONLY",
		"SELECT * FROM t WHERE id = 1",
		"SET SESSION TRANSACTION READ WRITE",
		"SELECT * FROM t WHERE id = 2",
		"set session transaction read write",
		"SET TRANSACTION READ WRITE",
		"SET SESSION transaction_read_only = 0",
		"SET @@session.tx_read_only = OFF",
		"SET autocommit = 1, transaction_read_only = 0",
		"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE",
	} {
		if res, err := h.HandleQuery(stmt); err != nil || res == nil {
			t.Errorf("%q: %v, %v; want an answer and no error", stmt, res, err)
		}
	}
	want := []string{
		"SET SESSION TRANSACTION READ ONLY",
		"SELECT * FROM t WHERE id = 1",
		"SELECT * FROM t WHERE id = 2",
		"set autocommit = 1",
		"set session transaction isolation level read committed, read only",
	}
	if !reflect.DeepEqual(r.forwarded, want) {
		t.Errorf("the source saw %q, want %q", r.forwarded, want)
	}
	for _, s := range *seen {
		if s == "refused/"+string(RouteReasonReadOnly) {
			t.Errorf("one of them was counted as refused: %v", *seen)
			break
		}
	}

	// A write after it is refused as before, and so are the forms the port
	// does not answer.
	before := len(r.forwarded)
	for _, stmt := range []string{
		"UPDATE t SET a = 1",
		"START TRANSACTION READ WRITE",
		"SET @a = 'x', transaction_read_only = 0",
	} {
		_, err := h.HandleQuery(stmt)
		refusedReadOnly(t, stmt, err)
	}
	// Prepared, a statement that changes the mark would run on the source
	// at each execution: refused.
	for _, stmt := range []string{"SET SESSION TRANSACTION READ WRITE", "SET SESSION transaction_read_only = ?"} {
		_, _, ctx, err := h.HandleStmtPrepare(stmt)
		refusedReadOnly(t, stmt, err)
		if ctx != nil {
			t.Errorf("%q was prepared", stmt)
		}
	}
	if len(r.forwarded) != before || len(r.prepared) != 0 {
		t.Errorf("a refused statement reached the source: %q, %d prepared", r.forwarded[before:], len(r.prepared))
	}
}

// With read-only mode off the port answers nothing itself: the client's
// statements are the source's to answer.
func TestReadOnlyOff_theMarkStatementsAreForwarded_3093(t *testing.T) {
	r := &fakeRouter{toCopy: false, reason: "cheap"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	stmts := []string{"SET SESSION TRANSACTION READ WRITE", "START TRANSACTION READ WRITE", "SET autocommit = 1, transaction_read_only = 0"}
	for _, stmt := range stmts {
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Errorf("%q: %v", stmt, err)
		}
	}
	if !reflect.DeepEqual(r.forwarded, stmts) {
		t.Errorf("the source saw %q, want every statement as sent", r.forwarded)
	}
}
