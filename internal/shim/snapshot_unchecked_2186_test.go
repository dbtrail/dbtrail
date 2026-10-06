package shim

import (
	"errors"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// #2186: a `_snapshot` read whose binlog-renumbering check could not tell
// carries the check's note as a warning beside the order note (#2156): one
// SHOW WARNINGS row each, only on a statement that succeeded.
func TestFinishWithNotes_2186(t *testing.T) {
	h := NewHandler(nil, nil)
	conn := &warningsConn{}
	h.BindConn(conn)
	q := TimeTravelQuery{Schema: "s", Table: "t"}
	notes := nonEmptyNotes("binlog numbering not checked: x", "", "order of changes unproven: y")
	if len(notes) != 2 {
		t.Fatalf("nonEmptyNotes = %q", notes)
	}
	if _, err := h.finishWithNotes(q, notes, nil, errors.New("merge failed")); err == nil {
		t.Fatal("the error was lost")
	}
	if conn.warnings != 0 || len(h.lastWarnings) != 0 {
		t.Fatalf("a failed statement left notes: %d, %q", conn.warnings, h.lastWarnings)
	}
	res := &mysql.Result{}
	if got, err := h.finishWithNotes(q, notes, res, nil); err != nil || got != res {
		t.Fatalf("(%v, %v)", got, err)
	}
	if conn.warnings != 2 || len(h.lastWarnings) != 2 || h.lastWarnings[0] != notes[0] {
		t.Fatalf("%d warning(s), %q; want both notes", conn.warnings, h.lastWarnings)
	}
}
