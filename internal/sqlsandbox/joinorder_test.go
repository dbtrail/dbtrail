package sqlsandbox

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// A statement that asks for its joins in the order written (the port's
// STRAIGHT_JOIN) runs with the copy's join reordering off, and only that
// statement: the next one on the same runner, which may reuse the worker
// process, plans its joins as always.
func TestRun_keepJoinOrder(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	const q = "SELECT current_setting('disabled_optimizers') AS off, current_setting('lock_configuration') AS locked"
	for i, keep := range []bool{true, false, true, false} {
		job := f.job(q)
		job.Session.KeepJoinOrder = keep
		res, err := r.Run(context.Background(), job)
		if err != nil {
			t.Fatalf("statement %d (keep %v): %v", i, keep, err)
		}
		off := strings.ToLower(fmt.Sprint(res.Rows[0][0]))
		if got := strings.Contains(off, "join_order"); got != keep {
			t.Errorf("statement %d (keep %v): disabled_optimizers = %q", i, keep, off)
		}
		if locked := res.Rows[0][1]; locked != "true" && locked != true {
			t.Errorf("statement %d: lock_configuration = %v, want it on", i, locked)
		}
	}
}
