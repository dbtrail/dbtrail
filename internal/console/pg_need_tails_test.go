package console

import (
	"context"
	"testing"
	"time"
)

func TestPGNeedTails(t *testing.T) {
	if pgNeedTails(context.Background()) {
		t.Error("an unmarked context needs its tails")
	}
	ctx := WithPGNeedTails(context.Background())
	if !pgNeedTails(ctx) {
		t.Error("a marked context does not")
	}
	child, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if !pgNeedTails(child) {
		t.Error("a context derived from a marked one does not")
	}
	if pgNoOlderTails(ctx) {
		t.Error("marked for the snapshot alone, it is marked for older changes too")
	}
	older := WithPGNoOlderTails(context.Background())
	if !pgNoOlderTails(older) || pgNeedTails(older) || !PGNoOlderTails(older) {
		t.Error("WithPGNoOlderTails does not mark older changes alone")
	}
}

// #3116: what an earlier answer of the copy read is kept per connection
// (SQLOnCopy.seen), and each connection of a port gets its own SQLOnCopy, so
// a new connection is held to nothing another one was shown.
func TestSQLOnCopyForStartsEachConnectionClean_3116(t *testing.T) {
	s := &Server{sqlRunner: &fakeSQLRunner{}}
	b := &bundle{}
	first, why := s.sqlOnCopyFor(b, "x")
	if first == nil {
		t.Fatalf("no SQL on the copy: %s", why)
	}
	first.seen.Store(42)
	second, _ := s.sqlOnCopyFor(b, "x")
	if second == first {
		t.Fatal("two connections share one SQLOnCopy")
	}
	if got := second.seen.Load(); got != 0 {
		t.Errorf("a new connection starts with the floor %d, want 0", got)
	}
}
