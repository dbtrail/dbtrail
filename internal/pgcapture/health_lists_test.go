package pgcapture

import (
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
)

// What the health panel is given: the tables captured without undo, and the
// ones not captured. A table is in one list only, and a table outside the
// capture's scope is in neither.
func TestHealthTableLists(t *testing.T) {
	ids := []TableIdentity{
		{Schema: "public", Table: "orders", Identity: "d"},
		{Schema: "public", Table: "accounts", Identity: "i", MissesKey: []string{"id"}},
		// Deferrable key AND an identity index: not captured, never "recorded".
		{Schema: "public", Table: "queue", Identity: "i"},
		{Schema: "public", Table: "events", Identity: "f", PartitionsBelowFull: []string{`"public"."events_1"`}},
		{Schema: "public", Table: "logs", Identity: "f", PartitionsBelowFull: []string{`"public"."logs_1"`}, PartitionsMissKey: []string{`"public"."logs_1"`}},
		{Schema: "other", Table: "out_of_scope", Identity: "d"},
	}
	deferrable := []DeferrableKey{
		{Schema: "public", Table: "queue", Constraint: "queue_pkey"},
		// At FULL, so not among the identities at all.
		{Schema: "public", Table: "ledger", Constraint: "ledger_pkey"},
		{Schema: "other", Table: "out_too", Constraint: "k"},
	}
	undo, not := HealthTableLists(ids, deferrable, event.Filters{Schemas: map[string]bool{"public": true}})
	wantUndo := []string{"public.events (relreplident=f, 1 partition(s) not FULL)", "public.orders (relreplident=d)"}
	wantNot := []string{
		"public.accounts (replica identity without the primary key)",
		"public.ledger (primary key DEFERRABLE)",
		"public.logs (1 partition(s) with a replica identity without the primary key)",
		"public.queue (primary key DEFERRABLE)",
	}
	if !slices.Equal(undo, wantUndo) || !slices.Equal(not, wantNot) {
		t.Errorf("without undo %q\nwant         %q\nnot captured %q\nwant         %q", undo, wantUndo, not, wantNot)
	}
	// Nothing to list is two empty lists, never nil: the page reads both.
	if undo, not := HealthTableLists(nil, nil, event.Filters{}); undo == nil || not == nil || len(undo)+len(not) != 0 {
		t.Errorf("empty: %#v, %#v", undo, not)
	}
}
