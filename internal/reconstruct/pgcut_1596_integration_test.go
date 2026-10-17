//go:build integration

package reconstruct

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The PostgreSQL fold bounds against a real index (#1596), through the real
// first-event-past search: events written in commit order, LSNs across the
// 0/... to 1/... boundary, and the stream checkpoint deciding whether the
// newest transaction is complete.
func TestResolvePGFoldBounds_integration(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 12, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	first := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
	add := func(lsn uint64, at time.Time) {
		testutil.MustExec(t, db, fmt.Sprintf(`INSERT INTO binlog_events
			(binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_after)
			VALUES ('%s', %d, %d, '%s', 'public', 'orders', 1, '1', '{"id":"1"}')`,
			lsnText(lsn), lsn, lsn, at.Format("2006-01-02 15:04:05")))
	}
	t1, t2, t3 := first.Add(10*time.Minute), first.Add(2*time.Hour), first.Add(3*time.Hour)
	add(0xFFFFFF00, t1)
	add(0xFFFFFF00, t1) // a second row of the same transaction
	add(1<<32, t2)
	add(1<<32+0x40, t3)

	// A point between the first and second transaction: stop right before 1/0.
	b, err := resolvePGFoldBounds(ctx, db, t1.Add(time.Minute), 0x100, first)
	if err != nil {
		t.Fatal(err)
	}
	if b.Until == nil || b.Until.Pos != 1<<32-1 || b.Next != 1<<32 {
		t.Fatalf("bounds = until %+v next %X, want until FFFFFFFF next 1/0", b.Until, b.Next)
	}

	// Past everything, not checkpointed: the newest transaction is left whole.
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, flavor, events_indexed, last_checkpoint, server_id)
		VALUES (1, 'gtid', '1/0', 4294967296, 'postgres', 3, UTC_TIMESTAMP(), 1)`)
	b, err = resolvePGFoldBounds(ctx, db, t3.Add(time.Hour), 0x100, first)
	if err != nil {
		t.Fatal(err)
	}
	if b.Until == nil || b.Until.Pos != 1<<32+0x3F || b.Next != 1<<32+0x40 {
		t.Fatalf("not checkpointed: until %+v next %X", b.Until, b.Next)
	}
	// Checkpointed: taken.
	testutil.MustExec(t, db, `UPDATE stream_state SET binlog_position = 4294967360 WHERE id = 1`)
	b, err = resolvePGFoldBounds(ctx, db, t3.Add(time.Hour), 0x100, first)
	if err != nil {
		t.Fatal(err)
	}
	if b.Until == nil || b.Until.Pos != 1<<32+0x40 || b.Next != 1<<32+0x41 {
		t.Fatalf("checkpointed: until %+v next %X", b.Until, b.Next)
	}
}
