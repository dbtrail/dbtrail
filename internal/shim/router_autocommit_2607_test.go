package shim

import (
	"testing"
	"time"
)

// #2607: the port opens its connection to the source on demand, so the
// session's first statement is asked "in a transaction?" before any answer
// from the source says so. A source whose sessions open with autocommit off
// (its configuration, an init_connect) reports it first in the answer to the
// EXPLAIN that connection runs. The statement is asked again after the plan,
// before the copy: it is MySQL's, as in_transaction.
func TestRouter_transactionLearnedFromThePlan_2607(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive", inTxnAfterDecide: true}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	var decided, answered []string
	h := answeredHandler(r, f, RouterConfig{MaxCopyAge: time.Minute}, &decided, &answered)
	if _, err := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a"); err != nil {
		t.Fatal(err)
	}
	if len(decided) != 1 || decided[0] != "mysql/in_transaction" {
		t.Errorf("decided %v, want [mysql/in_transaction]", decided)
	}
	if f.calls != 0 {
		t.Errorf("the copy ran the statement %d times, want none", f.calls)
	}
}
