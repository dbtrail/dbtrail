//go:build integration

package streamrun

import (
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// regrowBinlogTo flushes the source's binary log until its current file is
// named file, then writes filler into fillerDB (a schema capture does not
// read) until that file is longer than minSize: the new numbering has grown
// back to and past the old checkpoint's name and offset.
func regrowBinlogTo(t *testing.T, sourceDB, fillerDB *sql.DB, file string, minSize uint64) {
	t.Helper()
	for i := 0; ; i++ {
		cur, _, err := config.CurrentBinlogPosition(sourceDB)
		if err != nil {
			t.Fatalf("read the source's binlog position: %v", err)
		}
		if cur == file {
			break
		}
		if i > 500 {
			t.Fatalf("the new numbering did not reach %s (now at %s)", file, cur)
		}
		testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	}
	testutil.MustExec(t, fillerDB, `CREATE TABLE IF NOT EXISTS filler (id INT AUTO_INCREMENT PRIMARY KEY, pad VARCHAR(1000))`)
	for {
		_, pos, err := config.CurrentBinlogPosition(sourceDB)
		if err != nil {
			t.Fatalf("read the source's binlog position: %v", err)
		}
		if uint64(pos) > minSize {
			return
		}
		testutil.MustExec(t, fillerDB, "INSERT INTO filler (pad) VALUES (?)", strings.Repeat("x", 997))
	}
}

// regrowSource is one source server for the #2172 scenario.
type regrowSource struct {
	db, filler *sql.DB
	schema     string
	dsn        string
	flavor     string
}

// TestIntegrationPositionResetRegrownPastCheckpoint is #2172 on a real MySQL.
// A position-mode stream indexes rows, some after its last durable checkpoint
// (a crash). The source's binlog numbering then starts over and grows back
// past the checkpoint's file name AND offset before capture restarts, so the
// checkpoint's file exists again and is long enough: nothing in the names or
// sizes tells it from the file the checkpoint was read from.
//
//	run 1  clean: index 1-5, checkpoint durably at F:P (F is not the first
//	       file, so a restart at F:4 and one at the oldest file differ).
//	run 2  crash: index 6-10 while every checkpoint write fails.
//	       RESET the source's binlogs; write 11-12 in the new numbering's
//	       first file; grow the new numbering back to a file named F longer
//	       than P.
//	run 3  restart: must see that F is another file, keep 1-10, restart from
//	       the new numbering's first file (11-12 captured), stamp the loss.
//	run 4  crash in the new numbering: index 14 past the checkpoint.
//	run 5  ordinary restart: the stored identity verifies, the cleanup
//	       replays 14 and stamps nothing new; 1-15 exactly once.
//
// Before the fix run 3 deleted 6-10 (compared against F:P of another file)
// and read the new F from offset P, the middle of an unrelated event:
// "ERROR 1236 (HY000): log event entry exceeded max_allowed_packet".
func TestIntegrationPositionResetRegrownPastCheckpoint(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	fillerDB, _ := testutil.CreateTestDB(t)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}
	var gtidMode string
	if err := sourceDB.QueryRow("SELECT @@gtid_mode").Scan(&gtidMode); err != nil {
		t.Fatalf("read @@gtid_mode: %v", err)
	}
	if gtidMode == "ON" {
		t.Skip("skipping: the source runs gtid_mode=ON, so a fresh run captures in GTID mode, not position mode")
	}
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (id INT PRIMARY KEY, amount INT NOT NULL)`)
	runRegrowPastCheckpoint(t, regrowSource{db: sourceDB, filler: fillerDB, schema: sourceName,
		dsn: testutil.IntegrationDSN(sourceName), flavor: gomysql.MySQLFlavor}, 99960)
}

// TestIntegrationPositionResetRegrownPastCheckpoint_MariaDB: the same on a
// MariaDB source in position mode, whose file header (Gtid_list after the
// format description) and zero-position fill (#1117) differ from MySQL's.
func TestIntegrationPositionResetRegrownPastCheckpoint_MariaDB(t *testing.T) {
	sourceDB, sourceName := mariadbFlavorSource(t)
	fillerDB, _ := testutil.CreateTestMariaDB(t)
	runRegrowPastCheckpoint(t, regrowSource{db: sourceDB, filler: fillerDB, schema: sourceName,
		dsn: mariadbSourceDSN(sourceName), flavor: gomysql.MariaDBFlavor}, 99970)
}

func runRegrowPastCheckpoint(t *testing.T, src regrowSource, serverIDBase uint32) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	insert := func(lo, hi int) func() {
		return func() {
			for i := lo; i <= hi; i++ {
				testutil.MustExec(t, src.db, "INSERT INTO orders (id, amount) VALUES (?, ?)", i, i*10)
			}
		}
	}
	indexedThrough := func(hi int) func() bool {
		return func() bool {
			var n int
			if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events
				WHERE schema_name = ? AND table_name = 'orders' AND pk_values = ?`,
				src.schema, strconv.Itoa(hi)).Scan(&n); err != nil {
				t.Fatalf("poll indexed pk %d: %v", hi, err)
			}
			return n > 0
		}
	}
	cfg := func(n uint32) Config {
		return Config{
			IndexDSN:   testutil.IntegrationDSN(indexName),
			SourceDSN:  src.dsn,
			Flavor:     src.flavor,
			ServerID:   serverIDBase + n,
			BatchSize:  1,
			Schemas:    src.schema,
			Checkpoint: 1,
			GapTimeout: 30,
			Format:     "text",
			SSLMode:    "preferred",
			Deps:       testStreamDeps(),
		}
	}
	storedIdentity := func(when string) string {
		t.Helper()
		var file string
		var id sql.NullString
		if err := indexDB.QueryRow("SELECT binlog_file, binlog_file_identity FROM stream_state WHERE id = 1").Scan(&file, &id); err != nil {
			t.Fatalf("read binlog_file_identity: %v", err)
		}
		if !id.Valid || id.String == "" {
			t.Fatalf("%s: the checkpoint at %s stores no binlog_file_identity, so the restart would take the unverified path", when, file)
		}
		return id.String
	}
	lostAt := func() sql.NullTime {
		t.Helper()
		var at sql.NullTime
		if err := indexDB.QueryRow("SELECT gap_lost_at FROM stream_state WHERE id = 1").Scan(&at); err != nil {
			t.Fatalf("read gap_lost_at: %v", err)
		}
		return at
	}

	// The checkpoint must not sit in the source's first file, or "restart at
	// the checkpoint's file from :4" would pass for "restart at the oldest".
	for range 3 {
		testutil.MustExec(t, src.db, "FLUSH BINARY LOGS")
	}

	// run 1: position mode, from the source's current position.
	first := cfg(0)
	f, p, err := config.CurrentBinlogPosition(src.db)
	if err != nil {
		t.Fatal(err)
	}
	first.StartFile, first.StartPos = f, p
	if err := runOneUntil(t, first, true, insert(1, 5), indexedThrough(5)); err != nil {
		t.Fatalf("run 1 (clean): %v", err)
	}
	durable, err := loadStreamState(indexDB)
	if err != nil || durable == nil || durable.mode != "position" {
		t.Fatalf("run 1 checkpoint = %+v, err %v: want a position-mode checkpoint", durable, err)
	}
	storedIdentity("after run 1")

	// run 2
	lift := blockCheckpoints(t, indexDB)
	if err := runOneUntil(t, cfg(1), false, insert(6, 10), indexedThrough(10)); err != nil {
		t.Fatalf("run 2 (crash): %v", err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, 10))
	lift()

	// A file's identity has one-second resolution; a reset and regrow inside
	// the second the checkpoint's file was created is out of reach (documented).
	time.Sleep(1100 * time.Millisecond)
	resetSourceBinlogs(t, src.db)
	insert(11, 12)()
	regrowBinlogTo(t, src.db, src.filler, durable.binlogFile, durable.binlogPos+512)
	t.Logf("checkpoint %s:%d; the new numbering is back at %s, past that offset", durable.binlogFile, durable.binlogPos, durable.binlogFile)

	// run 3
	if err := runOneUntil(t, cfg(2), false, insert(13, 13), indexedThrough(13)); err != nil {
		t.Errorf("run 3 (restart after the reset and regrow): %v", err)
	}
	var detail sql.NullString
	if err := indexDB.QueryRow("SELECT gap_lost_detail FROM stream_state WHERE id = 1").Scan(&detail); err != nil {
		t.Fatal(err)
	}
	stamped := lostAt()
	if !stamped.Valid {
		t.Error("run 3 stamped no capture loss: the restart read the new file under the old checkpoint's name as if it were the same file")
	} else if !strings.Contains(detail.String, "is not the file the checkpoint") {
		t.Errorf("run 3 loss detail %q does not name the replaced file", detail.String)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, 13))
	if t.Failed() {
		t.FailNow()
	}
	storedIdentity("after run 3")

	// run 4: crash in the new numbering.
	lift = blockCheckpoints(t, indexDB)
	if err := runOneUntil(t, cfg(3), false, insert(14, 14), indexedThrough(14)); err != nil {
		t.Fatalf("run 4 (crash in the new numbering): %v", err)
	}
	lift()
	before := storedIdentity("before run 5")

	// run 5: the identity verifies; the ordinary cleanup replays 14.
	if err := runOneUntil(t, cfg(4), false, insert(15, 15), indexedThrough(15)); err != nil {
		t.Fatalf("run 5 (restart in the new numbering): %v", err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, 15))
	if again := lostAt(); !again.Valid || !again.Time.Equal(stamped.Time) {
		t.Errorf("run 5 re-stamped the capture loss (%v, was %v): an ordinary restart read its own file as another one", again, stamped)
	}
	if after := storedIdentity("after run 5"); after != before {
		t.Logf("checkpoint file identity before run 5 %s, after %s (the checkpoint moved to another file)", before, after)
	}
}
