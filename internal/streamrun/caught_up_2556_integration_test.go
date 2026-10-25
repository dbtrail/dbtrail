//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2556 against a real server: the source's own replication heartbeats, asked
// for every second on the test's connection, end in stream_state.caught_up_at
// when capture has everything, and never while the source keeps writing.

// caughtUpRun is one capture run for these tests.
type caughtUpRun struct {
	sourceBase string // base DSN of the source server (no database)
	flavor     string // gomysql.MySQLFlavor or gomysql.MariaDBFlavor
	indexDB    *sql.DB
	sourceName string
	start      func(*replication.BinlogSyncer) (*replication.BinlogStreamer, error)
	state      *streamState
	// checkpoint is the save interval. When the index lives on the source
	// server, each save is itself a binlog event, so it must be longer than
	// the heartbeat period or the source never sits idle for one.
	checkpoint time.Duration
	runFor     time.Duration
}

func (r caughtUpRun) run(t *testing.T) {
	t.Helper()
	mc, err := drivermysql.ParseDSN(r.sourceBase + "/")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	host, portStr, err := net.SplitHostPort(mc.Addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	cfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(90000 + time.Now().UnixNano()%9000),
		Flavor:          r.flavor,
		Host:            host,
		Port:            uint16(port),
		User:            mc.User,
		Password:        mc.Passwd,
		HeartbeatPeriod: time.Second,
	}
	if r.flavor == gomysql.MariaDBFlavor {
		// What production sets for MariaDB (streamrun.go, step 5): positions
		// filled in for events the source sends with LogPos 0, which is what
		// the heartbeat rule compares against.
		cfg.DumpCommandFlag |= replication.BINLOG_SEND_ANNOTATE_ROWS_EVENT
		cfg.FillZeroLogPos = true
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()
	streamer, err := r.start(syncer)
	if err != nil {
		t.Fatalf("StartSync: %v", err)
	}
	resolver, err := metadata.NewResolver(r.indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	sp := parser.NewStreamParser(resolver, parser.Filters{Schemas: map[string]bool{r.sourceName: true}}, nil)
	sp.SetHeartbeatPeriod(time.Second)
	if r.flavor == gomysql.MariaDBFlavor {
		sp.SetFlavor("mariadb")
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.runFor)
	defer cancel()
	events := make(chan parser.Event, 100)
	go func() {
		defer close(events)
		_ = sp.Run(ctx, streamer, events)
	}()
	if err := streamLoop(ctx, events, indexer.New(r.indexDB, 100), r.indexDB, r.checkpoint, r.state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}
}

func caughtUpAt(t *testing.T, indexDB *sql.DB) sql.NullTime {
	t.Helper()
	var at sql.NullTime
	if err := indexDB.QueryRow(`SELECT caught_up_at FROM stream_state WHERE id = 1`).Scan(&at); err != nil {
		t.Fatalf("read caught_up_at: %v", err)
	}
	return at
}

func indexedRows(t *testing.T, indexDB *sql.DB, schema string) int {
	t.Helper()
	var n int
	if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events WHERE schema_name = ?`, schema).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// positionSource prepares a table with five rows written after the start
// position, and the snapshot, on a server without GTIDs.
func positionSource(t *testing.T, sourceDB *sql.DB, sourceName string, indexDB *sql.DB) (string, uint32) {
	t.Helper()
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, amount INT NOT NULL)`)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled")
	}
	file, pos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Skipf("skipping: cannot read binlog position: %v", err)
	}
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	for i := range 5 {
		testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (?)", i)
	}
	return file, pos
}

// Idle after five writes: stamped, by a checkpoint that had already indexed
// all five, at a time no earlier than the last write.
func TestIntegrationCaughtUp_idleSourceStamps_2556(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestDB(t)
	file, pos := positionSource(t, sourceDB, sourceName, indexDB)
	var wroteAt time.Time
	if err := indexDB.QueryRow(`SELECT UTC_TIMESTAMP(6)`).Scan(&wroteAt); err != nil {
		t.Fatal(err)
	}
	caughtUpRun{
		sourceBase: testutil.BaseDSN(), flavor: gomysql.MySQLFlavor, indexDB: indexDB, sourceName: sourceName,
		start: func(s *replication.BinlogSyncer) (*replication.BinlogStreamer, error) {
			return s.StartSync(gomysql.Position{Name: file, Pos: pos})
		},
		state:      &streamState{mode: "position", serverID: 99997},
		checkpoint: 2500 * time.Millisecond, runFor: 8 * time.Second,
	}.run(t)
	at := caughtUpAt(t, indexDB)
	if !at.Valid {
		t.Fatal("caught_up_at is NULL after the source sat idle for several heartbeat periods")
	}
	if at.Time.Before(wroteAt) {
		t.Fatalf("caught_up_at %v is before the last write %v", at.Time, wroteAt)
	}
	if n := indexedRows(t, indexDB, sourceName); n != 5 {
		t.Fatalf("%d rows indexed, want 5", n)
	}
}

// A source that never stops writing never sends a heartbeat: never stamped.
func TestIntegrationCaughtUp_busySourceNeverStamps_2556(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestDB(t)
	file, pos := positionSource(t, sourceDB, sourceName, indexDB)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(150 * time.Millisecond):
				_, _ = sourceDB.Exec("INSERT INTO orders (amount) VALUES (1)")
			}
		}
	}()
	caughtUpRun{
		sourceBase: testutil.BaseDSN(), flavor: gomysql.MySQLFlavor, indexDB: indexDB, sourceName: sourceName,
		start: func(s *replication.BinlogSyncer) (*replication.BinlogStreamer, error) {
			return s.StartSync(gomysql.Position{Name: file, Pos: pos})
		},
		state:      &streamState{mode: "position", serverID: 99996},
		checkpoint: 2500 * time.Millisecond, runFor: 6 * time.Second,
	}.run(t)
	close(stop)
	wg.Wait()
	if n := indexedRows(t, indexDB, sourceName); n < 20 {
		t.Fatalf("only %d rows indexed: the writer was not running, so this proves nothing", n)
	}
	if at := caughtUpAt(t, indexDB); at.Valid {
		t.Fatalf("caught_up_at = %v while the source wrote every 150ms", at.Time)
	}
}

// A restart under GTID auto-position, against a source with nothing new:
// the source skips everything this stream already has and idles at a
// position the stream never received an event at. Two heartbeats in a row at
// that position are the proof: stamped, with no row indexed.
func TestIntegrationCaughtUp_gtidRestartAgainstAnIdleSource_2556(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	base := testutil.SkipIfNoGTIDSource(t)
	name := fmt.Sprintf("caughtup2556_%d", time.Now().UnixNano())
	root, err := sql.Open("mysql", base+"/")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	testutil.MustExec(t, root, "CREATE DATABASE `"+name+"`")
	t.Cleanup(func() { _, _ = root.Exec("DROP DATABASE IF EXISTS `" + name + "`") })
	sourceDB, err := sql.Open("mysql", base+"/"+name+"?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer sourceDB.Close()
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (id INT PRIMARY KEY, amount INT NOT NULL)`)
	testutil.MustExec(t, sourceDB, `INSERT INTO orders VALUES (1, 1), (2, 2)`)

	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	// The index must live on another server: its checkpoint writes on the
	// source would be new transactions the stream receives, and the stamp
	// would come from the "at the received position" rule instead of the
	// pair of idle heartbeats this test is about.
	var srcUUID, idxUUID string
	if err := sourceDB.QueryRow(`SELECT @@server_uuid`).Scan(&srcUUID); err != nil {
		t.Fatal(err)
	}
	if err := indexDB.QueryRow(`SELECT @@server_uuid`).Scan(&idxUUID); err != nil {
		t.Fatal(err)
	}
	if srcUUID == idxUUID {
		t.Fatalf("the GTID source and the index are the same server (%s): this test would prove the wrong rule", srcUUID)
	}
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{name}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	var executed string
	if err := sourceDB.QueryRow(`SELECT @@GLOBAL.gtid_executed`).Scan(&executed); err != nil {
		t.Fatal(err)
	}
	set, err := gomysql.ParseMysqlGTIDSet(executed)
	if err != nil {
		t.Fatalf("parse gtid_executed: %v", err)
	}
	caughtUpRun{
		sourceBase: base, flavor: gomysql.MySQLFlavor, indexDB: indexDB, sourceName: name,
		start: func(s *replication.BinlogSyncer) (*replication.BinlogStreamer, error) {
			return s.StartSyncGTID(set.Clone())
		},
		state:      &streamState{mode: "gtid", serverID: 99995, accGTID: set},
		checkpoint: 500 * time.Millisecond, runFor: 6 * time.Second,
	}.run(t)
	if at := caughtUpAt(t, indexDB); !at.Valid {
		t.Fatal("caught_up_at is NULL after a GTID restart against an idle source")
	}
	if n := indexedRows(t, indexDB, name); n != 0 {
		t.Fatalf("%d rows indexed on a restart that should have skipped everything", n)
	}
}

// MariaDB sends its own heartbeats: the same stamp from an idle source.
func TestIntegrationCaughtUp_mariadbIdleSourceStamps_2556(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	file, pos := positionSource(t, sourceDB, sourceName, indexDB)
	caughtUpRun{
		sourceBase: testutil.MariaDBBaseDSN(), flavor: gomysql.MariaDBFlavor, indexDB: indexDB, sourceName: sourceName,
		start: func(s *replication.BinlogSyncer) (*replication.BinlogStreamer, error) {
			return s.StartSync(gomysql.Position{Name: file, Pos: pos})
		},
		state:      &streamState{mode: "position", serverID: 99994, flavor: "mariadb"},
		checkpoint: 500 * time.Millisecond, runFor: 6 * time.Second,
	}.run(t)
	if at := caughtUpAt(t, indexDB); !at.Valid {
		t.Fatal("caught_up_at is NULL after an idle MariaDB source")
	}
	if n := indexedRows(t, indexDB, sourceName); n != 5 {
		t.Fatalf("%d rows indexed, want 5", n)
	}
}

// The loop itself, fed by hand: what it stamps is the marker's READ time,
// rows of a transaction still open keep it out, and the reset and gap
// advance wipe a stored stamp once.

func runLoopFor(t *testing.T, indexDB *sql.DB, state *streamState, evs ...parser.Event) {
	t.Helper()
	events := make(chan parser.Event, len(evs))
	for _, ev := range evs {
		events <- ev
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := streamLoop(ctx, events, indexer.New(indexDB, 100), indexDB, 100*time.Millisecond, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}
}

func secondsAgo(t *testing.T, indexDB *sql.DB) float64 {
	t.Helper()
	var ago sql.NullFloat64
	if err := indexDB.QueryRow(`SELECT TIMESTAMPDIFF(MICROSECOND, caught_up_at, UTC_TIMESTAMP(6)) / 1e6 FROM stream_state WHERE id = 1`).Scan(&ago); err != nil {
		t.Fatal(err)
	}
	if !ago.Valid {
		return -1
	}
	return ago.Float64
}

func insertEv(id int, end uint64) parser.Event {
	return parser.Event{BinlogFile: "binlog.000001", StartPos: end - 50, EndPos: end, Timestamp: time.Now().UTC(),
		Schema: "testdb", Table: "orders", EventType: parser.EventInsert, PKValues: strconv.Itoa(id),
		RowAfter: map[string]any{"id": int64(id)}}
}

func TestIntegrationCaughtUp_loopStampsTheReadTime_2556(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	// Read an hour ago, then a transaction that arrives after it: the stamp is
	// an hour old, not the save time, and the later rows do not cancel it.
	runLoopFor(t, indexDB, &streamState{mode: "position", serverID: 1},
		parser.Event{EventType: parser.EventCaughtUp, ReadAt: time.Now().Add(-time.Hour)},
		insertEv(1, 200),
		parser.Event{BinlogFile: "binlog.000001", EndPos: 250, EventType: parser.EventCommit})
	if ago := secondsAgo(t, indexDB); ago < 3590 || ago > 3660 {
		t.Fatalf("caught_up_at is %.1fs ago, want about 3600: it must be the marker's read time", ago)
	}
}

func TestIntegrationCaughtUp_loopIgnoresAMarkerInsideATransaction_2556(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runLoopFor(t, indexDB, &streamState{mode: "position", serverID: 1},
		insertEv(1, 200),
		parser.Event{EventType: parser.EventCaughtUp, ReadAt: time.Now()})
	if ago := secondsAgo(t, indexDB); ago != -1 {
		t.Fatalf("caught_up_at set (%.1fs ago) from a marker inside an open transaction", ago)
	}
}

func TestIntegrationCaughtUp_resetAndGapAdvanceClearTheStamp_2556(t *testing.T) {
	for _, c := range []struct {
		name  string
		clear func(*sql.DB, *streamState) error
	}{
		{"reset at the same position", func(db *sql.DB, s *streamState) error { return persistResetDiscard(db, s, true, "", nil) }},
		{"gap auto-advance", func(db *sql.DB, s *streamState) error { return persistGapAutoAdvance(db, s, "lost") }},
	} {
		t.Run(c.name, func(t *testing.T) {
			indexDB, _ := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, indexDB)
			running := &streamState{mode: "position", serverID: 1, binlogFile: "binlog.000001", safeFile: "binlog.000001", safePos: 4,
				caughtUpReadAt: time.Now()}
			if err := saveCheckpoint(indexDB, running); err != nil {
				t.Fatal(err)
			}
			if secondsAgo(t, indexDB) < 0 {
				t.Fatal("setup: no stamp written")
			}
			advanced := &streamState{mode: "position", serverID: 1, binlogFile: "binlog.000002", safeFile: "binlog.000002", safePos: 4}
			if err := c.clear(indexDB, advanced); err != nil {
				t.Fatal(err)
			}
			if ago := secondsAgo(t, indexDB); ago != -1 {
				t.Fatalf("caught_up_at survived (%.1fs ago): it describes coordinates the checkpoint left", ago)
			}
			// The advanced state runs on: its next marker stamps again.
			advanced.caughtUpReadAt = time.Now()
			if err := saveCheckpoint(indexDB, advanced); err != nil {
				t.Fatal(err)
			}
			if ago := secondsAgo(t, indexDB); ago < 0 || ago > 60 {
				t.Fatalf("after the clear, a new marker stamps %.1fs ago, want a fresh stamp", ago)
			}
		})
	}
}
