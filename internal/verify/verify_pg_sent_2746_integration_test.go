//go:build integration

package verify

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2746: readPGStreamPoint reads pg_sent_through_lsn, from the save that
// wrote it (pg_sent_through_at = last_checkpoint). A NULL (MySQL, or a
// capture built before #2746) and a missing column (an index the console
// reads without migrating it) are both "no sent position", never an error;
// 0 is a capture of this build that has not saved one yet.
func TestIntegrationReadPGStreamPoint_sentThrough_2746(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id)
		VALUES (1, 'gtid', '0/3E8', 1000, '0/3E8', 'postgres', UTC_TIMESTAMP(), 7)`)

	for _, tc := range []struct {
		name      string
		set       string
		wantKnown bool
		wantSent  uint64
	}{
		{"NULL", "NULL", false, 0},
		{"zero", "0", true, 0},
		{"a position", "4294967313", true, 1<<32 | 0x11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testutil.MustExec(t, db, "UPDATE stream_state SET pg_sent_through_lsn = "+tc.set+", pg_sent_through_at = last_checkpoint WHERE id = 1")
			st, err := readPGStreamPoint(ctx, db)
			if err != nil {
				t.Fatalf("readPGStreamPoint: %v", err)
			}
			if st.sentKnown != tc.wantKnown || st.sentThrough != tc.wantSent || st.checkpoint != 1000 {
				t.Fatalf("known %v sent %d checkpoint %d; want known %v sent %d checkpoint 1000",
					st.sentKnown, st.sentThrough, st.checkpoint, tc.wantKnown, tc.wantSent)
			}
		})
	}

	// A save that did not write the position (an older capture, or
	// bintrail-pg reset) moves last_checkpoint and not pg_sent_through_at: the
	// position left behind is not that save's, and is not trusted.
	t.Run("a position another save left behind", func(t *testing.T) {
		testutil.MustExec(t, db, "UPDATE stream_state SET pg_sent_through_lsn = 5000, pg_sent_through_at = last_checkpoint - INTERVAL 5 SECOND WHERE id = 1")
		st, err := readPGStreamPoint(ctx, db)
		if err != nil {
			t.Fatalf("readPGStreamPoint: %v", err)
		}
		if st.sentKnown {
			t.Fatalf("got %+v; want the untied position not trusted", st)
		}
	})

	t.Run("an index without the columns", func(t *testing.T) {
		testutil.MustExec(t, db, "ALTER TABLE stream_state DROP COLUMN pg_sent_through_lsn, DROP COLUMN pg_sent_through_at")
		st, err := readPGStreamPoint(ctx, db)
		if err != nil {
			t.Fatalf("readPGStreamPoint: %v; a missing column must read as no sent position", err)
		}
		if st.sentKnown || st.checkpoint != 1000 || st.flavor != flavorPostgres {
			t.Fatalf("got %+v; want the row read, with no sent position", st)
		}
	})
}
