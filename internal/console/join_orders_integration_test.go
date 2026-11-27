//go:build integration

package console

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// Through a real run on the copy: a statement of three tables listed with
// commas, not connected as written, runs in the connected order the copy
// picks by its shape, and join_orders counts it only once its answer is the
// client's (NoteRoute "copy"); a run the port then throws away and sends to
// MySQL (NoteRoute "mysql") is not counted.
func TestIntegrationJoinOrdersCountTheAnswersTheClientGot(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "off")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := newTailRig(t)
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.s.routing = newRoutingStats(time.Now(), nil)
	r.b.baselineConfigured = true
	a, b, c := r.table(nil), r.table(nil), r.table(nil)
	stmt := fmt.Sprintf("SELECT count(*) AS n FROM shop.%s x, shop.%s y, shop.%s z WHERE x.id = z.id AND y.id = z.id", a.Table, b.Table, c.Table)

	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	run := func(t *testing.T) {
		t.Helper()
		res, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{JoinOrderByShape: true})
		if err != nil {
			t.Fatal(err)
		}
		if res.JoinOrderByShape != sqlsandbox.JoinOrderByShapeConnected || fmt.Sprint(res.Rows) != "[[1]]" {
			t.Fatalf("order %q, rows %v", res.JoinOrderByShape, res.Rows)
		}
	}
	counted := func() uint64 { return r.s.routing.snapshot()[bootServerID].JoinOrders[sqlsandbox.JoinOrderByShapeConnected] }

	run(t)
	q.NoteRoute("mysql") // the port threw the copy's answer away
	if n := counted(); n != 0 {
		t.Fatalf("a thrown-away answer was counted: %d", n)
	}
	run(t)
	q.NoteRoute("copy")
	if n := counted(); n != 1 {
		t.Fatalf("the answer the client got: %d, want 1", n)
	}
	// Committed once: a later decision with nothing pending adds nothing.
	q.NoteRoute("copy")
	if n := counted(); n != 1 {
		t.Fatalf("counted twice: %d", n)
	}
}
