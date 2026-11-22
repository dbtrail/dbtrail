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
		{name: "a drop of this table well before the source read, which the read covers", skips: ledger(read.Add(-10*time.Minute), false, "public.orders")},
		{name: "a drop of this table a minute before the source read, inside the clocks' margin", skips: ledger(read.Add(-time.Minute), false, "public.orders"), refused: true},
		{name: "a drop of another table only", skips: ledger(read.Add(time.Minute), false, "public.items")},
		{name: "a drop of another table, the list cut short", skips: ledger(read.Add(time.Minute), true, "public.items"), refused: true},
		{name: "a drop that names no table", skips: ledger(read.Add(time.Minute), false), refused: true},
		{name: "a drop with no time", skips: ledger(time.Time{}, false, "public.orders"), refused: true},
		{name: "a ledger that does not parse", skips: sql.NullString{String: "{", Valid: true}, refused: true},
		{name: "the same name in another case is another table", skips: ledger(read.Add(time.Minute), false, "public.Orders")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			why := pgTailCaptureLoss(state(tc.skips), 0, read, "public", "orders")
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
	if pgTailCaptureLoss(st, 0, read, "public", "orders") == "" {
		t.Error("a lost slot after the source read must refuse")
	}
}

// The snapshot's read time is the source's clock (pgbaseline reads it with
// the server's now()) and a drop's time is the daemon's (parser.SkipCounters).
// With the daemon's clock 2 minutes behind the source, a drop committed
// AFTER the snapshot's LSN is dated before its read time: by the clocks it
// would look covered by the snapshot. The ledger also records the drop's
// commit LSN, and that says it came after: refused.
func TestPGTailCaptureLoss_daemonClockBehindTheSource(t *testing.T) {
	read := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	const snapshotLSN = 0x16B374D848
	doc, err := json.Marshal(map[string]any{"replica_identity_not_full": map[string]any{
		"count": 1, "last_at": read.Add(-2 * time.Minute), "last_pos": snapshotLSN + 4096, "tables": []string{"public.orders"}}})
	if err != nil {
		t.Fatal(err)
	}
	st := &status.StreamStateInfo{Mode: "gtid", GapColumnsPresent: true, CaptureSkips: sql.NullString{String: string(doc), Valid: true}}
	if why := pgTailCaptureLoss(st, snapshotLSN, read, "public", "orders"); why == "" {
		t.Error("a drop after the snapshot's LSN, dated before its read by a clock 2 minutes behind, must refuse")
	}
	// The other way: a drop BEFORE the snapshot's LSN that a clock 2 minutes
	// ahead dates after the read is covered by the snapshot.
	doc, err = json.Marshal(map[string]any{"replica_identity_not_full": map[string]any{
		"count": 1, "last_at": read.Add(2 * time.Minute), "last_pos": snapshotLSN - 4096, "tables": []string{"public.orders"}}})
	if err != nil {
		t.Fatal(err)
	}
	st.CaptureSkips = sql.NullString{String: string(doc), Valid: true}
	if why := pgTailCaptureLoss(st, snapshotLSN, read, "public", "orders"); why != "" {
		t.Errorf("a drop before the snapshot's LSN, dated after its read by a clock ahead: %q, want covered", why)
	}
	// A snapshot with its LSN and no read time is the rule's to judge, by
	// position; one with neither refuses.
	if why := pgTailCaptureLoss(st, snapshotLSN, time.Time{}, "public", "orders"); why != "" {
		t.Errorf("no read time, the LSN known, a drop before it: %q, want covered", why)
	}
	if why := pgTailCaptureLoss(st, 0, time.Time{}, "public", "orders"); why == "" {
		t.Error("neither the LSN nor the read time: must refuse")
	}
}
