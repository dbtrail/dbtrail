//go:build integration

package reconstruct

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A read of one PostgreSQL table at a past moment is refused, with what to
// run, when the capture dropped changes of it since the snapshot the read
// starts from; covered, it goes on; a MySQL index with the very same ledger
// is not asked.
func TestCheckPGPastRead(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	const ledger = `{"replica_identity_not_full":{"count":2,"last_at":"2026-11-21T12:00:05Z","last_pos":2000,"tables":["public.t"],"last_detail":"replica identity 'd' (default: primary key only)"}}`
	for _, flavor := range []string{"postgres", "mysql"} {
		t.Run(flavor, func(t *testing.T) {
			db, _ := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatalf("EnsureSchema: %v", err)
			}
			testutil.MustExec(t, db, "INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, capture_skips) VALUES (1, 'gtid', ?, NOW(), 1, ?)", flavor, ledger)
			ctx := context.Background()
			// Full reads (fold generation 0): compared by position.
			old := baseline.DumpMetadata{LSN: 1000, LastDumpAt: time.Date(2026, 11, 21, 11, 0, 0, 0, time.UTC)}
			err := CheckPGPastRead(ctx, db, "public", "t", old)
			if flavor == "mysql" {
				if err != nil {
					t.Fatalf("a MySQL index is not asked: %v", err)
				}
				return
			}
			t.Logf("refusal: %v", err)
			if !errors.Is(err, ErrPGPastReadNotCovered) {
				t.Fatalf("want the refusal, got %v", err)
			}
			for _, want := range []string{"a read of public.t at a past moment is refused", `ALTER TABLE "public"."t" REPLICA IDENTITY FULL`, "take a full read of it"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal lacks %q", want)
				}
			}
			if strings.Contains(err.Error(), "--") {
				t.Errorf("the refusal must name no command-line flag (MCP prints it as it is): %v", err)
			}
			// A snapshot read after the drop, or another table: covered.
			if err := CheckPGPastRead(ctx, db, "public", "t", baseline.DumpMetadata{LSN: 3000, LastDumpAt: time.Date(2026, 11, 21, 13, 0, 0, 0, time.UTC)}); err != nil {
				t.Errorf("a snapshot after the drop: %v", err)
			}
			if err := CheckPGPastRead(ctx, db, "public", "u", old); err != nil {
				t.Errorf("another table: %v", err)
			}
			// A fold whose floor is past the drop, its last real read before
			// it: refused (the race with the ledger's save, or a copy folded
			// by 0.138.0).
			if err := CheckPGPastRead(ctx, db, "public", "t", baseline.DumpMetadata{LSN: 3000, LastDumpAt: time.Date(2026, 11, 21, 11, 0, 0, 0, time.UTC), FoldGeneration: 2}); !errors.Is(err, ErrPGPastReadNotCovered) {
				t.Errorf("a fold past the drop: %v", err)
			}
			// No snapshot (the recorded changes alone): refused.
			if err := CheckPGPastRead(ctx, db, "public", "t", baseline.DumpMetadata{}); !errors.Is(err, ErrPGPastReadNotCovered) {
				t.Errorf("a read with no snapshot: %v", err)
			}
			// A lost slot, stamped by a daemon clock 2 minutes behind the
			// server's: after the read, it reads as before it. Refused within
			// the margin, not well before it.
			testutil.MustExec(t, db, "UPDATE stream_state SET capture_skips = NULL, gap_lost_at = ?, gap_lost_detail = 'replication slot invalidated' WHERE id = 1",
				time.Date(2026, 11, 21, 11, 58, 0, 0, time.UTC))
			read := baseline.DumpMetadata{LSN: 1000, LastDumpAt: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
			if err := CheckPGPastRead(ctx, db, "public", "t", read); !errors.Is(err, ErrPGPastReadNotCovered) || !strings.Contains(err.Error(), "capture lost events") {
				t.Errorf("a slot lost within the margin of the read: %v", err)
			}
			read.LastDumpAt = time.Date(2026, 11, 21, 13, 0, 0, 0, time.UTC)
			if err := CheckPGPastRead(ctx, db, "public", "t", read); err != nil {
				t.Errorf("a slot lost well before the read: %v", err)
			}
			testutil.MustExec(t, db, "UPDATE stream_state SET capture_skips = ?, gap_lost_at = NULL WHERE id = 1", ledger)
			// The same through PGNotCovered, which verify reads.
			if why, err := PGNotCovered(ctx, db, "public", "t", old); err != nil || why == "" {
				t.Errorf("PGNotCovered: %q, %v", why, err)
			}
		})
	}
}
