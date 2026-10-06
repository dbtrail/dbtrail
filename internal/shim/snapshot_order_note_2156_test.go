package shim

import (
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// A `_snapshot` order note (#2156) reaches the client without free SQL bound:
// the count on the connection, the text from SHOW WARNINGS with code 1105, and
// the next statement clears both. With no note pending, SHOW WARNINGS stays
// the empty OK it always was.
func TestSnapshotOrderNote_showWarningsWithoutFreeSQL(t *testing.T) {
	h := NewHandler(nil, nil)
	conn := &warningsConn{}
	h.BindConn(conn)
	if r, err := h.HandleQuery("SHOW WARNINGS"); err != nil || r.Resultset != nil {
		t.Fatalf("SHOW WARNINGS with nothing pending = (%+v, %v), want the empty OK", r, err)
	}

	// What runSnapshotFullTable does when the order is unproven.
	h.setWarningsCoded(mysql.ER_UNKNOWN_ERROR, []string{"order of changes unproven: for 1 row(s) ..."})
	if conn.warnings != 1 {
		t.Fatalf("connection warnings = %d, want 1", conn.warnings)
	}
	w, err := h.HandleQuery("show warnings;")
	if err != nil {
		t.Fatal(err)
	}
	rows := textRows(t, w.Resultset)
	if len(rows) != 1 || rows[0][0] != "Warning" || rows[0][1] != "1105" || !strings.HasPrefix(rows[0][2], "order of changes unproven") {
		t.Fatalf("SHOW WARNINGS rows = %v, want the note with code 1105", rows)
	}

	_, _ = h.HandleQuery("SELECT * FROM _flashback.orders AS OF '2026-01-01' WHERE id = 1")
	if conn.warnings != 0 {
		t.Fatalf("connection warnings after the next statement = %d, want 0", conn.warnings)
	}
	if r, err := h.HandleQuery("SHOW WARNINGS"); err != nil || r.Resultset != nil {
		t.Fatalf("SHOW WARNINGS after the next statement = (%+v, %v), want the empty OK", r, err)
	}
}
