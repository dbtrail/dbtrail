//go:build integration

package query

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Where an index's coverage is known to begin, for the baseline gap check
// (#2843). For a PostgreSQL source the capture records where it started
// reading from, and that is the answer when it is earlier than the oldest
// surviving event; for every other flavor nothing but the oldest event is
// read.
func TestCoverageStart_2843(t *testing.T) {
	setState := func(t *testing.T, db *sql.DB, flavor string, from any) {
		t.Helper()
		testutil.MustExec(t, db, "DELETE FROM stream_state")
		if _, err := db.Exec(`INSERT INTO stream_state (id, mode, flavor, binlog_file, binlog_position, server_id, last_checkpoint, pg_capture_from_lsn)
			VALUES (1, 'gtid', ?, '0/2000', 8192, 1, UTC_TIMESTAMP(), ?)`, flavor, from); err != nil {
			t.Fatalf("seed stream_state: %v", err)
		}
	}
	for _, tc := range []struct {
		name   string
		flavor string // of the index and of the caller, unless ask says otherwise
		ask    string
		from   any      // pg_capture_from_lsn
		events []uint64 // start positions, in insertion order
		drop   bool     // an index without the column
		want   uint64
		wantOK bool
	}{
		{name: "the capture started before the oldest event", flavor: "postgres", from: uint64(0x1000), events: []uint64{0x3000, 0x4000}, want: 0x1000, wantOK: true},
		{name: "no event survives: the start is the answer", flavor: "postgres", from: uint64(0x1000), want: 0x1000, wantOK: true},
		{name: "an oldest event before the recorded start is kept", flavor: "postgres", from: uint64(0x5000), events: []uint64{0x3000}, want: 0x3000, wantOK: true},
		{name: "a start past the 32-bit boundary", flavor: "postgres", from: uint64(1<<32 + 0x10), events: []uint64{1<<32 + 0x500}, want: 1<<32 + 0x10, wantOK: true},
		{name: "no recorded start: the oldest event", flavor: "postgres", from: nil, events: []uint64{0x3000}, want: 0x3000, wantOK: true},
		{name: "a zero start is not one", flavor: "postgres", from: uint64(0), events: []uint64{0x3000}, want: 0x3000, wantOK: true},
		{name: "no recorded start and no event", flavor: "postgres", from: nil},
		{name: "an index a capture older than the column wrote", flavor: "postgres", events: []uint64{0x3000}, drop: true, want: 0x3000, wantOK: true},
		{name: "a MySQL index is not asked for it", flavor: "mysql", from: uint64(0x1000), events: []uint64{0x3000}, want: 0x3000, wantOK: true},
		{name: "a MariaDB caller is not asked for it", flavor: "postgres", ask: "mariadb", from: uint64(0x1000), events: []uint64{0x3000}, want: 0x3000, wantOK: true},
		{name: "a PostgreSQL caller on an index of another flavor", flavor: "mysql", ask: "postgres", from: uint64(0x1000), events: []uint64{0x3000}, want: 0x3000, wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, first := headsRig(t, 4)
			setState(t, db, tc.flavor, tc.from)
			for i, pos := range tc.events {
				insertLSNEvent(t, db, pos, fmt.Sprintf("k%d", i), first.Add(time.Duration(i)*time.Minute))
			}
			if tc.drop {
				testutil.MustExec(t, db, "ALTER TABLE stream_state DROP COLUMN pg_capture_from_lsn")
			}
			ask := tc.ask
			if ask == "" {
				ask = tc.flavor
			}
			got, ok := CoverageStart(db, ask)
			if ok != tc.wantOK || got.StartPos != tc.want {
				t.Fatalf("CoverageStart = %+v, %v; want position %#x, %v", got, ok, tc.want, tc.wantOK)
			}
			if ok {
				if want := fmt.Sprintf("%X/%X", tc.want>>32, uint32(tc.want)); got.BinlogFile != want {
					t.Fatalf("the position's text is %q, want %q", got.BinlogFile, want)
				}
			}
			// OldestIndexedEvent itself is what it was: the oldest event.
			old, oldOK := OldestIndexedEvent(db)
			if wantOld := len(tc.events) > 0; oldOK != wantOld || (oldOK && old.StartPos != tc.events[0]) {
				t.Fatalf("OldestIndexedEvent = %+v, %v; want the first event inserted", old, oldOK)
			}
		})
	}
	if got, ok := CoverageStart(nil, "postgres"); ok || got.StartPos != 0 {
		t.Fatalf("CoverageStart(nil) = %+v, %v", got, ok)
	}
}
