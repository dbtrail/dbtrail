//go:build integration

package pgcapture

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// On a primary, QueryFlushed answers the flushed position and the source's
// own time for it: the server's clock, not this host's, and no later than a
// commit made afterwards.
func TestQueryFlushed_realPrimary(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	var before time.Time
	if err := conn.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&before); err != nil {
		t.Fatal(err)
	}
	lsn, sourceAt, err := QueryFlushed(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	var after time.Time
	var flushedAfter string
	if err := conn.QueryRow(ctx, "SELECT clock_timestamp(), pg_current_wal_flush_lsn()::text").Scan(&after, &flushedAfter); err != nil {
		t.Fatal(err)
	}
	if lsn == 0 {
		t.Fatal("no flushed position")
	}
	if sourceAt.Before(before) || sourceAt.After(after) {
		t.Fatalf("the source's time for the reading is %s, outside the source's own %s .. %s", sourceAt, before, after)
	}
	if sourceAt.Location() != time.UTC {
		t.Fatalf("the source's time is in %s, want UTC", sourceAt.Location())
	}
	// The older entry point answers the same position.
	if again, err := QueryFlushedLSN(ctx, conn); err != nil || again < lsn {
		t.Fatalf("QueryFlushedLSN = %d, %v; want at least %d", again, err, lsn)
	}
}
