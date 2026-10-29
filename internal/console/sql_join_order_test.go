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
