package shim

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// A statement on its way to the copy that ends before the copy answers (its
// client left while it waited its turn, the port is stopping, the
// connection's cap ran out) is not handed to MySQL: nobody would read the
// answer, and with no time limit on the line a client giving up is the way a
// long wait usually ends. It is counted on its own, as refused, and never as
// a fault of the copy. The same on a router and on a port that hands a busy
// copy's reads to MySQL.
func TestRouter_aStatementThatEndedIsNotForwarded(t *testing.T) {
	for _, busyRefuses := range []bool{true, false} {
		for _, copyErr := range []error{
			context.Canceled,
			context.DeadlineExceeded,
			fmt.Errorf("reserve a slot: %w", context.Canceled),
		} {
			t.Run(fmt.Sprintf("busyRefuses=%v/%v", busyRefuses, copyErr), func(t *testing.T) {
				r := &fakeRouter{toCopy: true, reason: "expensive"}
				f := &fakeFreeSQL{err: copyErr, updatedAt: time.Now()}
				var got []string
				h := serviceHandler(t, r, f, RouterConfig{AnyCopyAge: true, BusyRefuses: busyRefuses}, &got)
				_, err := h.HandleQuery(expensive2084)
				var myErr *mysql.MyError
				if !errors.As(err, &myErr) || myErr.Code != mysql.ER_QUERY_INTERRUPTED {
					t.Fatalf("err = %v, want error %d (interrupted)", err, mysql.ER_QUERY_INTERRUPTED)
				}
				if len(r.forwarded) != 0 {
					t.Errorf("a statement nobody waits for was sent to MySQL: %v", r.forwarded)
				}
				if len(got) != 1 || got[0] != "refused/cancelled" {
					t.Errorf("observed %v, want [refused/cancelled]", got)
				}
				if len(h.routeWarned) != 0 {
					t.Errorf("warned %v: a client leaving is not a fault to warn about", h.routeWarned)
				}
			})
		}
	}
}
