//go:build integration

package query

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

func insertLSNEvent(t *testing.T, db *sql.DB, lsn uint64, pk string, at time.Time) {
	t.Helper()
	text := fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn))
	testutil.MustExec(t, db, fmt.Sprintf(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_after)
		VALUES ('%s', %d, %d, '%s', 'public', 'orders', 1, '%s', '{"id":"%s"}')`,
		text, lsn, lsn, at.Format("2006-01-02 15:04:05"), pk, pk))
}

// A PostgreSQL-anchored fetch against a real index (#1596): bounded by the
// LSN number across the 0/... to 1/... boundary, where the text order is
// backwards, and reaching back for a change after the anchor whose time is
// before the fetch's own (the #2138 shape, through the same partition heads).
func TestFetch_LSNAnchoredWindow(t *testing.T) {
	ctx := context.Background()
	db, first := headsRig(t, 12)
	hour := func(n int) time.Time { return first.Add(time.Duration(n)*time.Hour + 10*time.Minute) }
	// Anchored at 0/FFFFFF00: by text length every 1/... row would sort before
	// it, so the old rule dropped them all.
	insertLSNEvent(t, db, 0xFFFFFE00, "before", hour(1)) // 0/FFFFFE00: before the anchor
	insertLSNEvent(t, db, 0xFFFFFF00, "at", hour(2))     // the anchor itself
	insertLSNEvent(t, db, 1<<32, "inside", hour(4))      // 1/0
	insertLSNEvent(t, db, 1<<32+0x200, "after", hour(5)) // 1/200: past the upper bound

	since := hour(4) // later than the anchor's own row: the read must reach back for it
	until := hour(6)
	rows, err := New(db).Fetch(ctx, Options{Schema: "public", Table: "orders", Since: &since, Until: &until,
		SincePos: &BinlogPos{File: "0/FFFFFF00", Pos: 0xFFFFFF00},
		UntilPos: &BinlogPos{File: "1/100", Pos: 1<<32 + 0x100},
		Order:    "ASC"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, r.PKValues)
	}
	if want := []string{"at", "inside"}; !slices.Equal(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
}
