//go:build integration

package rotation

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// An online idx_type_lookup build holds binlog_events for its whole run, so
// every rotation statement meanwhile gives up as busy (measured: add-future
// and drop both failed every attempt of a 2-attempt window during a 17 s
// build on 3M events). The busy error names the build, not a mystery reader,
// and says how far the partitions ahead reach. Here the build is held in its
// metadata-lock wait behind a reader, which is how it shows in the process
// list for the whole test.
func TestBusyErrorNamesARunningTypeIndexBuild(t *testing.T) {
	shortDDLLock(t, 1)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, "ALTER TABLE binlog_events DROP INDEX idx_type_lookup")
	now := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{now, now.Add(time.Hour), now.Add(2 * time.Hour)})
	end := holdRead(t, db, dbName)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "SET SESSION lock_wait_timeout = 30"); err != nil {
		t.Fatal(err)
	}
	built := make(chan error, 1)
	go func() {
		_, err := conn.ExecContext(context.Background(), "ALTER TABLE binlog_events ADD INDEX idx_type_lookup (event_type, schema_name, table_name, event_timestamp), ALGORITHM=INPLACE, LOCK=NONE")
		built <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE 'ALTER TABLE binlog_events ADD INDEX idx_type_lookup%'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the build never showed in the process list")
		}
		time.Sleep(50 * time.Millisecond)
	}

	err = alterBinlogEvents(context.Background(), db,
		"ALTER TABLE `"+dbName+"`.binlog_events REORGANIZE PARTITION p_future INTO (PARTITION p_2099010100 VALUES LESS THAN (TO_SECONDS('2099-01-01 01:00:00')), PARTITION p_future VALUES LESS THAN MAXVALUE)", nil)
	end()
	if berr := <-built; berr != nil {
		t.Fatalf("build: %v", berr)
	}
	if !errors.Is(err, errTableBusy) {
		t.Fatalf("got %v, want errTableBusy", err)
	}
	for _, want := range []string{"idx_type_lookup", "rotation resumes after it", "partitions ahead"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("busy error %q does not say %q", err, want)
		}
	}
}
