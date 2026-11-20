package console

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// A PostgreSQL capture keeps flowing while it drops the changes of a table
// below REPLICA IDENTITY FULL, and records each drop in capture_skips. The
// copy's tail of that table must not be trusted after such a drop: it would
// serve old rows as fresh.
func TestPGTailCaptureLoss_droppedChanges(t *testing.T) {
	read := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	ledger := func(at time.Time, truncated bool, tables ...string) sql.NullString {
		doc, err := json.Marshal(map[string]any{"replica_identity_not_full": map[string]any{
			"count": 2, "last_at": at, "tables": tables, "tables_truncated": truncated}})
		if err != nil {
			t.Fatal(err)
		}
		return sql.NullString{String: string(doc), Valid: true}
	}
	state := func(skips sql.NullString) *status.StreamStateInfo {
		return &status.StreamStateInfo{Mode: "gtid", GapColumnsPresent: true, CaptureSkips: skips}
	}
	for _, tc := range []struct {
		name    string
		skips   sql.NullString
		refused bool
	}{
		{name: "no ledger, as an older PostgreSQL capture leaves it", skips: sql.NullString{}},
		{name: "an empty ledger", skips: sql.NullString{String: "{}", Valid: true}},
		{name: "a drop of this table after the source read", skips: ledger(read.Add(time.Minute), false, "public.orders"), refused: true},
		{name: "a drop of this table at the source read's own instant", skips: ledger(read, false, "public.orders"), refused: true},
		{name: "a drop of this table before the source read, which the read covers", skips: ledger(read.Add(-time.Minute), false, "public.orders")},
		{name: "a drop of another table only", skips: ledger(read.Add(time.Minute), false, "public.items")},
		{name: "a drop of another table, the list cut short", skips: ledger(read.Add(time.Minute), true, "public.items"), refused: true},
		{name: "a drop that names no table", skips: ledger(read.Add(time.Minute), false), refused: true},
		{name: "a drop with no time", skips: ledger(time.Time{}, false, "public.orders"), refused: true},
		{name: "a ledger that does not parse", skips: sql.NullString{String: "{", Valid: true}, refused: true},
		{name: "the same name in another case is another table", skips: ledger(read.Add(time.Minute), false, "public.Orders")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			why := pgTailCaptureLoss(state(tc.skips), read, "public", "orders")
			if (why != "") != tc.refused {
				t.Fatalf("pgTailCaptureLoss = %q, want refused=%v", why, tc.refused)
			}
			if tc.refused && tc.skips.String != "{" && !strings.Contains(why, "public.orders") {
				t.Errorf("the refusal must name the table: %q", why)
			}
		})
	}

	// A slot loss still refuses whatever the ledger says.
	st := state(sql.NullString{})
	st.GapLostAt = sql.NullTime{Time: read.Add(time.Minute), Valid: true}
	if pgTailCaptureLoss(st, read, "public", "orders") == "" {
		t.Error("a lost slot after the source read must refuse")
	}
}
