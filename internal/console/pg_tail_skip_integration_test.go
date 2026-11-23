//go:build integration

package console

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/views"
)

// A read that found nothing for some tables says nothing of another: a build
// catching up from the same event must still get that table's change.
func TestIntegrationPGTailSince_aSkipOverOtherTablesIsNotTheirs(t *testing.T) {
	ctx := context.Background()
	r := newPGTailRig(t)
	quiet := r.table(nil, [2]string{"1", "one"})
	built := r.table(nil, [2]string{"2", "two"})
	r.change(quiet.Table, event.EventUpdate, "1", "uno", 0x600, 20*time.Minute)
	r.checkpoint(0x600)
	from, err := r.k.src.Newest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r.change(built.Table, event.EventUpdate, "2", "dos", 0x610, 21*time.Minute)
	r.checkpoint(0x610)
	to, err := r.k.src.Newest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	read := func(tbl views.BaselineTable) int {
		t.Helper()
		n := 0
		if err := r.k.src.Since(ctx, []views.BaselineTable{tbl}, from.ID, to.ID, func(page []query.ResultRow) error {
			n += len(page)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := read(quiet); n != 0 {
		t.Fatalf("the quiet table was handed %d rows", n)
	}
	if n := read(built); n != 1 {
		t.Fatalf("the table that changed was handed %d rows after a read of another found none, want 1", n)
	}
}
