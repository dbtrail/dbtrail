package shim

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// An answer from an earlier copy (#2210) raises the copy's note as a
// warning with the port's own code, 1105; beside a cut cell the code stays
// the cut's, 1262, and both messages are there.
func TestFreeSQL_olderCopyNoteIsAWarning(t *testing.T) {
	for _, c := range []struct {
		name  string
		cut   int
		code  int64
		count uint16
	}{
		{"alone", 0, mysql.ER_UNKNOWN_ERROR, 1},
		{"beside a cut cell", 2, mysql.ER_WARN_TOO_MANY_RECORDS, 2},
	} {
		res := oneCell("id", "INTEGER", json.Number("1"))
		res.Note = "Answered from the copy of 2026-04-30 02:00 UTC: ..."
		res.TruncatedCells = c.cut
		h := NewHandler(nil, nil)
		h.BindFreeSQL(&fakeFreeSQL{res: res})
		conn := &warningsConn{}
		h.BindConn(conn)
		if _, err := h.HandleQuery("SELECT id FROM orders"); err != nil {
			t.Fatal(err)
		}
		if conn.warnings != c.count {
			t.Errorf("%s: connection warnings = %d, want %d", c.name, conn.warnings, c.count)
		}
		w, err := h.HandleQuery("SHOW WARNINGS")
		if err != nil {
			t.Fatal(err)
		}
		rows := textRows(t, w.Resultset)
		last := rows[len(rows)-1]
		if len(rows) != int(c.count) || !strings.HasPrefix(last[2], "Answered from the copy of") {
			t.Errorf("%s: SHOW WARNINGS rows = %v", c.name, rows)
		}
		if got := rows[0][1]; got != strconv.FormatInt(c.code, 10) {
			t.Errorf("%s: code = %v, want %d", c.name, got, c.code)
		}
	}
}
