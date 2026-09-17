//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
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

// ─── stream_state persistence ────────────────────────────────────────────────────────

func TestStreamState_loadEmpty(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	state, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState failed: %v", err)
	}
	if state != nil {
		t.Errorf("expected nil for empty stream_state, got %+v", state)
	}
}

func TestStreamState_upsertAndLoad(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	state := &streamState{
		mode:          "position",
		binlogFile:    "binlog.000001",
		binlogPos:     1024,
		eventsIndexed: 50,
		serverID:      99999,
		lastEventTime: sql.NullTime{
			Time:  time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
			Valid: true,
		},
	}

	if err := saveCheckpoint(db, state); err != nil {
		t.Fatalf("saveCheckpoint failed: %v", err)
	}

	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil state after save")
	}

	if loaded.mode != "position" {
		t.Errorf("mode: expected position, got %q", loaded.mode)
	}
	if loaded.binlogFile != "binlog.000001" {
		t.Errorf("binlogFile: expected binlog.000001, got %q", loaded.binlogFile)
	}
	if loaded.binlogPos != 1024 {
		t.Errorf("binlogPos: expected 1024, got %d", loaded.binlogPos)
	}
	if loaded.eventsIndexed != 50 {
		t.Errorf("eventsIndexed: expected 50, got %d", loaded.eventsIndexed)
	}
	if loaded.serverID != 99999 {
		t.Errorf("serverID: expected 99999, got %d", loaded.serverID)
	}
}

// TestStreamState_mariadbRoundTrip verifies the source flavor persists through a
// checkpoint save/load cycle and then drives resume parsing: a MariaDB gtid_set
// ("0-1-100") survives and re-parses with the MariaDB parser. Without the
// persisted flavor, resume would call ParseMysqlGTIDSet and reject the set.
func TestStreamState_mariadbRoundTrip(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	if err := saveCheckpoint(db, &streamState{
		mode:     "gtid",
		gtidSet:  "0-1-100",
		flavor:   gomysql.MariaDBFlavor,
		serverID: 42,
	}); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}

	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil state after save")
	}
	if loaded.flavor != gomysql.MariaDBFlavor {
		t.Errorf("flavor: expected mariadb, got %q", loaded.flavor)
	}
	if loaded.gtidSet != "0-1-100" {
		t.Errorf("gtidSet: expected 0-1-100, got %q", loaded.gtidSet)
	}

	// The persisted flavor must drive resume parsing.
	mode, _, gtidStr, _, accGTID, err := resolveStartForFlavor("", "", 0, loaded, loaded.flavor)
	if err != nil {
		t.Fatalf("resume resolveStartForFlavor: %v", err)
	}
	if mode != "gtid" || gtidStr != "0-1-100" {
		t.Errorf("resume: mode=%q gtid=%q, want gtid/0-1-100", mode, gtidStr)
	}
	if _, ok := accGTID.(*gomysql.MariadbGTIDSet); !ok {
		t.Errorf("resume accGTID type = %T, want *MariadbGTIDSet", accGTID)
	}
}

// TestStreamState_mysqlFlavorDefault verifies a checkpoint written without an
// explicit flavor loads back as mysql (the NOT NULL DEFAULT) — pre-MariaDB
// callers and rows are unaffected.
func TestStreamState_mysqlFlavorDefault(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	if err := saveCheckpoint(db, &streamState{mode: "position", binlogFile: "binlog.000001", binlogPos: 4, serverID: 1}); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}
	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded.flavor != gomysql.MySQLFlavor {
		t.Errorf("flavor default: expected mysql, got %q", loaded.flavor)
	}
}

func TestStreamState_upsertUpdate(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// First checkpoint.
	s1 := &streamState{
		mode: "position", binlogFile: "binlog.000001", binlogPos: 100,
		eventsIndexed: 10, serverID: 1,
	}
	if err := saveCheckpoint(db, s1); err != nil {
		t.Fatalf("first saveCheckpoint: %v", err)
	}

	// Second checkpoint — advances position.
	s2 := &streamState{
		mode: "position", binlogFile: "binlog.000002", binlogPos: 500,
		eventsIndexed: 250, serverID: 1,
	}
	if err := saveCheckpoint(db, s2); err != nil {
		t.Fatalf("second saveCheckpoint: %v", err)
	}

	// Verify only one row exists and it reflects the latest state.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM stream_state").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row in stream_state, got %d", count)
	}

	loaded, _ := loadStreamState(db)
	if loaded.binlogFile != "binlog.000002" {
		t.Errorf("expected binlog.000002, got %q", loaded.binlogFile)
	}
	if loaded.eventsIndexed != 250 {
		t.Errorf("expected 250 events, got %d", loaded.eventsIndexed)
	}
}

func TestStreamState_gtidMode(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	gtidSet := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-42"
	state := &streamState{
		mode:          "gtid",
		gtidSet:       gtidSet,
		eventsIndexed: 42,
		serverID:      12345,
	}

	if err := saveCheckpoint(db, state); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}

	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded.mode != "gtid" {
		t.Errorf("mode: expected gtid, got %q", loaded.mode)
	}
	if loaded.gtidSet != gtidSet {
		t.Errorf("gtidSet: expected %q, got %q", gtidSet, loaded.gtidSet)
	}
}

// ─── stream_state: bintrail_id round-trip ────────────────────────────────────────────

func TestStreamState_bintrailID(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	const id = "aabbccdd-0000-0000-0000-000000000002"
	state := &streamState{mode: "position", binlogFile: "binlog.000001", binlogPos: 100, serverID: 1, bintrailID: id}
	if err := saveCheckpoint(db, state); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}
	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil state")
	}
	if loaded.bintrailID != id {
		t.Errorf("bintrailID: expected %q, got %q", id, loaded.bintrailID)
	}
}

func TestStreamState_emptyBintrailIDStoresNULL(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	state := &streamState{mode: "position", binlogFile: "binlog.000001", serverID: 1, bintrailID: ""}
	if err := saveCheckpoint(db, state); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}
	var got sql.NullString
	if err := db.QueryRow("SELECT bintrail_id FROM stream_state WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("query bintrail_id: %v", err)
	}
	if got.Valid {
		t.Errorf("expected bintrail_id to be NULL, got %q", got.String)
	}
}

// ─── deleteEventsSinceCheckpoint: resume-time dedup rollover safety (#840) ────────────

// TestDeleteEventsSinceCheckpoint_rolloverSafe pins the #840 fix on the
// resume-time dedup DELETE against real MySQL: after mysql-bin.999999 the
// server continues with mysql-bin.1000000, and plain lexicographic
// binlog_file comparison inverts ('999999' > '1000000'). Resuming at
// mysql-bin.1000000:300 must delete only the straggler row at-or-beyond that
// checkpoint and must NOT touch the legitimately-indexed pre-rollover row —
// pre-fix, the buggy `binlog_file > ?` matched the pre-rollover file and
// permanently destroyed it.
func TestDeleteEventsSinceCheckpoint_rolloverSafe(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "mysql-bin.999999", 100, 200, ts, nil, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))   // pre-rollover, pre-checkpoint (must survive)
	testutil.InsertEvent(t, db, "mysql-bin.1000000", 100, 200, ts, nil, "mydb", "orders", 1, "2", nil, nil, []byte(`{"id":2}`)) // checkpoint file, below pos (must survive)
	testutil.InsertEvent(t, db, "mysql-bin.1000000", 300, 400, ts, nil, "mydb", "orders", 1, "3", nil, nil, []byte(`{"id":3}`)) // checkpoint file, at-or-beyond pos (the straggler; must be deleted)

	n, err := deleteEventsSinceCheckpoint(db, "mysql-bin.1000000", 300, noDedupFloor)
	if err != nil {
		t.Fatalf("deleteEventsSinceCheckpoint: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 row deleted (the straggler), got %d", n)
	}

	rows, err := db.Query("SELECT pk_values FROM binlog_events ORDER BY pk_values")
	if err != nil {
		t.Fatalf("query surviving rows: %v", err)
	}
	defer rows.Close()
	var survivors []string
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			t.Fatalf("scan pk_values: %v", err)
		}
		survivors = append(survivors, pk)
	}
	want := []string{"1", "2"}
	if len(survivors) != len(want) || survivors[0] != want[0] || survivors[1] != want[1] {
		t.Fatalf("expected surviving pks %v (pre-rollover row preserved), got %v", want, survivors)
	}
}

// TestDeleteEventsSinceCheckpointGTID_rolloverSafe is the GTID-mode sibling
// of TestDeleteEventsSinceCheckpoint_rolloverSafe: deleteEventsSinceCheckpointGTID
// delegates its position-keyed delete to deleteEventsSinceCheckpoint, so the
// same pre-rollover row must survive when resuming through the GTID entry
// point too.
func TestDeleteEventsSinceCheckpointGTID_rolloverSafe(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	gtid1 := uuid + ":1"
	gtid2 := uuid + ":2"
	testutil.InsertEvent(t, db, "mysql-bin.999999", 100, 200, ts, &gtid1, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "mysql-bin.1000000", 100, 200, ts, &gtid2, "mydb", "orders", 1, "2", nil, nil, []byte(`{"id":2}`))
	testutil.InsertEvent(t, db, "mysql-bin.1000000", 300, 400, ts, nil, "mydb", "orders", 1, "3", nil, nil, []byte(`{"id":3}`))

	savedSet, err := gomysql.ParseMysqlGTIDSet(uuid + ":1-2")
	if err != nil {
		t.Fatalf("parse saved set: %v", err)
	}

	n, err := deleteEventsSinceCheckpointGTID(db, "mysql-bin.1000000", 300, savedSet, gomysql.MySQLFlavor, noDedupFloor)
	if err != nil {
		t.Fatalf("deleteEventsSinceCheckpointGTID: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 row deleted (the straggler, no GTID stragglers), got %d", n)
	}

	var survives int
	if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE binlog_file = 'mysql-bin.999999' AND pk_values = '1'").Scan(&survives); err != nil {
		t.Fatalf("query pre-rollover row: %v", err)
	}
	if survives != 1 {
		t.Fatalf("expected the pre-rollover row to survive the GTID-mode dedup delete, got count=%d", survives)
	}
}

// ─── streamLoop (in-memory, no live replication) ─────────────────────────────────────────

// TestStreamLoop_flushAndCheckpoint verifies that streamLoop correctly batches
// events, flushes them, and saves a checkpoint — using a live index database
// but no replication connection.
func TestStreamLoop_flushAndCheckpoint(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Insert a minimal schema snapshot so the indexer can run.
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "amount", 2, "", "decimal", "YES")

	idx := indexer.New(db, 10)

	events := make(chan parser.Event, 20)
	ts := time.Now().UTC()

	// Send 3 synthetic events (fewer than the batch size of 10).
	for i := range 3 {
		events <- parser.Event{
			BinlogFile: "binlog.000001",
			StartPos:   uint64(i * 100),
			EndPos:     uint64((i + 1) * 100),
			Timestamp:  ts,
			Schema:     "testdb",
			Table:      "orders",
			EventType:  parser.EventInsert,
			PKValues:   strconv.Itoa(i + 1),
			RowAfter:   map[string]any{"id": int64(i + 1), "amount": 9.99},
		}
	}
	close(events)

	state := &streamState{
		mode:     "position",
		serverID: 1,
	}

	if err := streamLoop(context.Background(), events, idx, db, time.Hour, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	if state.eventsIndexed != 3 {
		t.Errorf("expected 3 events indexed, got %d", state.eventsIndexed)
	}

	// Verify checkpoint was written to the DB.
	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved")
	}
	if loaded.eventsIndexed != 3 {
		t.Errorf("checkpoint: expected 3 events, got %d", loaded.eventsIndexed)
	}

	// Verify rows are in binlog_events.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&count); err != nil {
		t.Fatalf("count binlog_events: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows in binlog_events, got %d", count)
	}
}

// TestStreamLoop_flushFailurePropagates verifies that a flush failure at a
// checkpoint is RETURNED to the caller (the stream aborts loudly) instead of
// being swallowed with a warning — the silent-skip that let an un-indexable
// event vanish (#652). The batch-full and DDL flush paths already propagated;
// this covers the ticker / channel-closed checkpoint path that did not.
//
// The failure is forced by dropping binlog_events before the flush, so the
// INSERT errors. Any InsertBatch error flows through the same checkpoint() path
// the fix changed, so this is a deterministic stand-in for the oversized-event
// (max_allowed_packet) rejection that motivated the issue.
func TestStreamLoop_flushFailurePropagates(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "id", 1, "PRI", "int", "NO")

	// Batch size 10 so the single event is not flushed inline (len < BatchSize);
	// it flushes at channel close, exercising the checkpoint() path.
	idx := indexer.New(db, 10)

	events := make(chan parser.Event, 1)
	events <- parser.Event{
		BinlogFile: "binlog.000001",
		StartPos:   0,
		EndPos:     100,
		Timestamp:  time.Now().UTC(),
		Schema:     "testdb",
		Table:      "orders",
		EventType:  parser.EventInsert,
		PKValues:   "1",
		RowAfter:   map[string]any{"id": int64(1)},
	}
	close(events)

	// Force the flush to fail (the pool stays open so CreateTestDB's cleanup can
	// still drop the database).
	testutil.MustExec(t, db, "DROP TABLE binlog_events")

	state := &streamState{mode: "position", serverID: 1}
	err := streamLoop(context.Background(), events, idx, db, time.Hour, state, observe.ForSource("test"), nil)
	if err == nil {
		t.Fatal("expected streamLoop to propagate the flush failure, got nil (a silent skip — #652)")
	}
	if !strings.Contains(err.Error(), "INSERT") {
		t.Errorf("expected a batch-INSERT flush error, got: %v", err)
	}
}

// TestStreamLoop_saveCheckpointFailureDoesNotAbort verifies the deliberate split
// in the fail-loud fix: a FLUSH failure aborts the stream, but a saveCheckpoint
// failure stays a warning (it only re-streams from an older checkpoint on
// restart — re-processing, not data loss). A regression that made checkpoint
// errors abort would crash streams on transient stream_state write blips.
//
// binlog_events is kept (the flush succeeds) and stream_state is dropped (so
// saveCheckpoint fails); streamLoop must still return nil.
func TestStreamLoop_saveCheckpointFailureDoesNotAbort(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "id", 1, "PRI", "int", "NO")

	idx := indexer.New(db, 10)

	events := make(chan parser.Event, 1)
	events <- parser.Event{
		BinlogFile: "binlog.000001",
		StartPos:   0,
		EndPos:     100,
		Timestamp:  time.Now().UTC(),
		Schema:     "testdb",
		Table:      "orders",
		EventType:  parser.EventInsert,
		PKValues:   "1",
		RowAfter:   map[string]any{"id": int64(1)},
	}
	close(events)

	// The flush (into binlog_events) succeeds; only saveCheckpoint fails.
	testutil.MustExec(t, db, "DROP TABLE stream_state")

	state := &streamState{mode: "position", serverID: 1}
	if err := streamLoop(context.Background(), events, idx, db, time.Hour, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("a saveCheckpoint failure must NOT abort the stream, got: %v", err)
	}
	if state.eventsIndexed != 1 {
		t.Errorf("expected the event to be indexed despite the checkpoint failure, got %d", state.eventsIndexed)
	}
}

// ─── streamLoop live replication ───────────────────────────────────────────────────────

// TestStreamLoop_liveReplication is a full end-to-end test that connects as a
// replica to the Docker MySQL, streams events, and verifies they are indexed.
// It is skipped gracefully if binary logging or replication privileges are unavailable.
func TestStreamLoop_liveReplication(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestDB(t)

	// Create a table on the source.
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		amount DECIMAL(10,2) NOT NULL
	)`)

	// Skip if binary logging is not enabled.
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}

	// Capture current binlog position before any inserts.
	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Skipf("skipping: cannot read binlog position: %v", err)
	}

	// Take schema snapshot into the index DB. (The metadata.EnsureResolver
	// helper auto-snapshots-then-loads; here we only need the snapshot taken,
	// since NewResolver(indexDB, 0) below loads it. Inlined so this engine test
	// stays in package streamrun without importing the cmd layer.)
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Parse source DSN to get connection details for the syncer (inlined from
	// the helper now in config.ParseSourceDSN).
	mc, err := drivermysql.ParseDSN(testutil.IntegrationDSN(sourceName))
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	hostStr, portStr, err := net.SplitHostPort(mc.Addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	portN, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("ParseUint(port): %v", err)
	}
	host, port, user, password := hostStr, uint16(portN), mc.User, mc.Passwd

	// Insert rows so the streamer has something to receive.
	for i := range 5 {
		testutil.MustExec(t, sourceDB,
			"INSERT INTO orders (amount) VALUES (?)", float64(i+1)*10.0)
	}

	// Start BinlogSyncer from the captured position.
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99998,
		Flavor:   "mysql",
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
	})
	defer syncer.Close()

	streamer, syncErr := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if syncErr != nil {
		t.Skipf("skipping: StartSync failed (replication may not be granted): %v", syncErr)
	}

	// Build resolver and filters for the source schema.
	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	filters := parser.Filters{Schemas: map[string]bool{sourceName: true}}

	sp := parser.NewStreamParser(resolver, filters, nil)
	idx := indexer.New(indexDB, 100)

	// Run with a short deadline — just long enough to capture the 5 inserts.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	state := &streamState{mode: "position", serverID: 99998}
	if err := streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	parseErr := <-parseErrCh
	if parseErr != nil &&
		!errors.Is(parseErr, context.DeadlineExceeded) &&
		!errors.Is(parseErr, context.Canceled) {
		t.Fatalf("StreamParser error: %v", parseErr)
	}

	if state.eventsIndexed < 5 {
		t.Errorf("expected at least 5 events indexed, got %d", state.eventsIndexed)
	}

	// Verify the checkpoint reflects the streamed position.
	loaded, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved after streaming")
	}
}

// ─── streamLoop — additional behaviour ────────────────────────────────────────────────

// TestStreamLoop_contextCancel verifies that cancelling the context causes
// streamLoop to flush the in-flight batch and write a checkpoint before returning.
func TestStreamLoop_contextCancel(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := indexer.New(db, 100) // large batch — events stay in batch until cancel

	events := make(chan parser.Event, 10)
	ts := time.Now().UTC()

	// Enqueue 2 events but do NOT close the channel — simulates an active stream.
	for i := range 2 {
		events <- parser.Event{
			BinlogFile: "binlog.000001",
			StartPos:   uint64(i * 100),
			EndPos:     uint64((i + 1) * 100),
			Timestamp:  ts,
			Schema:     "testdb",
			Table:      "orders",
			EventType:  parser.EventInsert,
			PKValues:   strconv.Itoa(i + 1),
			RowAfter:   map[string]any{"id": int64(i + 1), "amount": 9.99},
		}
	}

	state := &streamState{mode: "position", serverID: 1}
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay — the 2 buffered events are consumed first,
	// then the batch sits idle until context cancellation triggers the flush.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if err := streamLoop(ctx, events, idx, db, time.Hour, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	if state.eventsIndexed != 2 {
		t.Errorf("expected 2 events indexed, got %d", state.eventsIndexed)
	}

	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved on context cancel")
	}
	if loaded.eventsIndexed != 2 {
		t.Errorf("checkpoint: expected 2 events, got %d", loaded.eventsIndexed)
	}
}

// TestStreamLoop_tickerCheckpoint verifies that the periodic ticker fires a
// checkpoint even when the events channel stays open and receives no further events.
// TestStreamLoop_gtidCheckpointAtCommit verifies #491: in GTID mode the durable
// gtid_set is advanced ONLY at a transaction commit boundary (EventCommit), never
// at the leading EventGTID. A checkpoint that fires mid-transaction must not claim
// the in-flight transaction — otherwise a restart (GTID auto-position) would skip
// it and lose data. The row itself must still be indexed (at-least-once).
func TestStreamLoop_gtidCheckpointAtCommit(t *testing.T) {
	const gtid = "11111111-1111-1111-1111-111111111111:1"
	const uuid = "11111111-1111-1111-1111-111111111111"

	newGTIDState := func(t *testing.T) *streamState {
		gs, err := gomysql.ParseMysqlGTIDSet("")
		if err != nil {
			t.Fatalf("ParseMysqlGTIDSet: %v", err)
		}
		return &streamState{mode: "gtid", serverID: 1, accGTID: gs.(*gomysql.MysqlGTIDSet)}
	}
	setupIdx := func(t *testing.T) (*indexer.Indexer, *sql.DB) {
		db, _ := testutil.CreateTestDB(t)
		testutil.InitIndexTables(t, db)
		testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00", "testdb", "orders", "id", 1, "PRI", "int", "NO")
		testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00", "testdb", "orders", "amount", 2, "", "decimal", "YES")
		return indexer.New(db, 10), db
	}
	insertEvent := parser.Event{
		BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
		Timestamp: time.Now().UTC(), GTID: gtid,
		Schema: "testdb", Table: "orders", EventType: parser.EventInsert,
		PKValues: "1", RowAfter: map[string]any{"id": int64(1), "amount": 9.99},
	}

	// Scenario A: GTID + row, then checkpoint (channel close) BEFORE the commit.
	// The GTID must NOT be persisted, but the row must be indexed.
	t.Run("mid-transaction checkpoint does not claim the GTID", func(t *testing.T) {
		idx, db := setupIdx(t)
		events := make(chan parser.Event, 10)
		events <- parser.Event{BinlogFile: "binlog.000001", EndPos: 100, GTID: gtid, EventType: parser.EventGTID}
		events <- insertEvent
		close(events) // → checkpoint() fires mid-transaction

		state := newGTIDState(t)
		if err := streamLoop(context.Background(), events, idx, db, time.Hour, state, observe.ForSource("test"), nil); err != nil {
			t.Fatalf("streamLoop: %v", err)
		}

		loaded, err := loadStreamState(db)
		if err != nil || loaded == nil {
			t.Fatalf("loadStreamState err=%v nil=%v", err, loaded == nil)
		}
		if strings.Contains(loaded.gtidSet, uuid) {
			t.Errorf("gtid_set must NOT contain the uncommitted GTID, got %q", loaded.gtidSet)
		}
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&count); err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != 1 {
			t.Errorf("the row must still be indexed (no loss): want 1, got %d", count)
		}
	})

	// Scenario B: GTID + row + COMMIT, then checkpoint. The GTID must be persisted.
	t.Run("committed transaction advances the GTID", func(t *testing.T) {
		idx, db := setupIdx(t)
		events := make(chan parser.Event, 10)
		events <- parser.Event{BinlogFile: "binlog.000001", EndPos: 100, GTID: gtid, EventType: parser.EventGTID}
		events <- insertEvent
		events <- parser.Event{BinlogFile: "binlog.000001", EndPos: 250, GTID: gtid, EventType: parser.EventCommit}
		close(events)

		state := newGTIDState(t)
		if err := streamLoop(context.Background(), events, idx, db, time.Hour, state, observe.ForSource("test"), nil); err != nil {
			t.Fatalf("streamLoop: %v", err)
		}

		loaded, err := loadStreamState(db)
		if err != nil || loaded == nil {
			t.Fatalf("loadStreamState err=%v nil=%v", err, loaded == nil)
		}
		if !strings.Contains(loaded.gtidSet, uuid) {
			t.Errorf("gtid_set must contain the committed GTID, got %q", loaded.gtidSet)
		}
	})

	// Scenario C: a DDL auto-commits its own GTID (no XID). EventDDL must flush
	// pending rows AND advance the GTID — the only path that claims a DDL's GTID.
	t.Run("DDL advances the GTID and flushes prior rows", func(t *testing.T) {
		idx, db := setupIdx(t)
		events := make(chan parser.Event, 10)
		events <- parser.Event{BinlogFile: "binlog.000001", EndPos: 100, GTID: gtid, EventType: parser.EventGTID}
		events <- insertEvent
		events <- parser.Event{BinlogFile: "binlog.000001", EndPos: 300, GTID: gtid, EventType: parser.EventDDL,
			Schema: "testdb", Table: "orders", DDLType: parser.DDLAlterTable, DDLQuery: "ALTER TABLE orders ADD c INT"}
		close(events)

		state := newGTIDState(t)
		if err := streamLoop(context.Background(), events, idx, db, time.Hour, state, observe.ForSource("test"), nil); err != nil {
			t.Fatalf("streamLoop: %v", err)
		}

		loaded, err := loadStreamState(db)
		if err != nil || loaded == nil {
			t.Fatalf("loadStreamState err=%v nil=%v", err, loaded == nil)
		}
		if !strings.Contains(loaded.gtidSet, uuid) {
			t.Errorf("DDL must advance the GTID, got %q", loaded.gtidSet)
		}
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&count); err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != 1 {
			t.Errorf("the pre-DDL row must be flushed before the GTID advances: want 1, got %d", count)
		}
	})
}

// TestStreamLoop_positionModeCommitBoundary verifies that for a GTID-enabled
// source running in POSITION mode (accGTID nil), EventCommit is a harmless no-op
// for GTID tracking yet still advances binlogPos to the commit boundary, and the
// durable gtid_set stays empty (#491 — locks the comment's position-mode claim).
func TestStreamLoop_positionModeCommitBoundary(t *testing.T) {
	const gtid = "11111111-1111-1111-1111-111111111111:1"
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00", "testdb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00", "testdb", "orders", "amount", 2, "", "decimal", "YES")
	idx := indexer.New(db, 10)

	events := make(chan parser.Event, 10)
	events <- parser.Event{BinlogFile: "binlog.000001", EndPos: 100, GTID: gtid, EventType: parser.EventGTID}
	events <- parser.Event{BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200, Timestamp: time.Now().UTC(),
		GTID: gtid, Schema: "testdb", Table: "orders", EventType: parser.EventInsert,
		PKValues: "1", RowAfter: map[string]any{"id": int64(1), "amount": 9.99}}
	events <- parser.Event{BinlogFile: "binlog.000001", EndPos: 250, GTID: gtid, EventType: parser.EventCommit}
	close(events)

	state := &streamState{mode: "position", serverID: 1} // accGTID nil
	if err := streamLoop(context.Background(), events, idx, db, time.Hour, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	loaded, err := loadStreamState(db)
	if err != nil || loaded == nil {
		t.Fatalf("loadStreamState err=%v nil=%v", err, loaded == nil)
	}
	if loaded.gtidSet != "" {
		t.Errorf("position mode must not persist a gtid_set, got %q", loaded.gtidSet)
	}
	if loaded.binlogPos != 250 {
		t.Errorf("binlogPos must land on the commit boundary (250), got %d", loaded.binlogPos)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("the row must be indexed, got %d", count)
	}
}

// setGTIDMode transitions @@GLOBAL.gtid_mode through the required permissive
// steps. Returns an error so callers can skip when privileges/state don't allow.
func setGTIDMode(db *sql.DB, on bool) error {
	stmts := []string{
		"SET @@GLOBAL.enforce_gtid_consistency = ON",
		"SET @@GLOBAL.gtid_mode = OFF_PERMISSIVE",
		"SET @@GLOBAL.gtid_mode = ON_PERMISSIVE",
		"SET @@GLOBAL.gtid_mode = ON",
	}
	if !on {
		stmts = []string{
			"SET @@GLOBAL.gtid_mode = ON_PERMISSIVE",
			"SET @@GLOBAL.gtid_mode = OFF_PERMISSIVE",
			"SET @@GLOBAL.gtid_mode = OFF",
			"SET @@GLOBAL.enforce_gtid_consistency = OFF",
		}
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return nil
}

// TestStreamLoop_gtidAdvancesOnCommit_live is the end-to-end proof of #491: with
// a GTID-enabled source, the StreamParser must emit EventCommit at each XID, and
// streamLoop must advance the durable gtid_set off those commits. It toggles the
// server's gtid_mode (restored in cleanup) and skips if that isn't possible.
func TestStreamLoop_gtidAdvancesOnCommit_live(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Register restore BEFORE attempting the transition, so a partial enable that
	// leaves gtid_mode in a permissive state is still undone (a leaked gtid_mode
	// would silently break every other test on the shared server). Skip the
	// down-sequence when already OFF (enable never progressed) — OFF→ON_PERMISSIVE
	// is itself an invalid jump.
	t.Cleanup(func() {
		var mode string
		if err := sourceDB.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&mode); err == nil && mode == "OFF" {
			_, _ = sourceDB.Exec("SET @@GLOBAL.enforce_gtid_consistency = OFF")
			return
		}
		if err := setGTIDMode(sourceDB, false); err != nil {
			t.Logf("warning: failed to restore gtid_mode=OFF: %v", err)
		}
	})
	if err := setGTIDMode(sourceDB, true); err != nil {
		t.Skipf("skipping: cannot enable gtid_mode on the test server: %v", err)
	}

	var serverUUID string
	if err := sourceDB.QueryRow("SELECT @@server_uuid").Scan(&serverUUID); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT, amount DECIMAL(10,2) NOT NULL)`)
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}

	mc, err := drivermysql.ParseDSN(testutil.IntegrationDSN(sourceName))
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	hostStr, portStr, _ := net.SplitHostPort(mc.Addr)
	portN, _ := strconv.ParseUint(portStr, 10, 16)

	// Capture the source's executed set at the stream-start position so we can
	// assert the stream accumulated exactly the transactions that follow.
	var gtidBefore string
	if err := sourceDB.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&gtidBefore); err != nil {
		t.Fatalf("gtid_executed (before): %v", err)
	}

	// Interleave an implicit-commit statement — ANALYZE TABLE logs a GTID + QUERY
	// with NO XID and is not table DDL — between two autocommit INSERTs (GTID +
	// BEGIN + row + XID). The ANALYZE's GTID can only land via the next-GTID
	// fallback, so this is the end-to-end proof that the fallback works against a
	// real binlog (#491).
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (10.0)")
	testutil.MustExec(t, sourceDB, "ANALYZE TABLE orders")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (20.0)")

	var gtidAfter string
	if err := sourceDB.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&gtidAfter); err != nil {
		t.Fatalf("gtid_executed (after): %v", err)
	}

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99997, Flavor: "mysql",
		Host: hostStr, Port: uint16(portN), User: mc.User, Password: mc.Passwd,
	})
	defer syncer.Close()

	streamer, syncErr := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if syncErr != nil {
		t.Skipf("skipping: StartSync failed: %v", syncErr)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	sp := parser.NewStreamParser(resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	idx := indexer.New(indexDB, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	gs, _ := gomysql.ParseMysqlGTIDSet("")
	state := &streamState{mode: "gtid", serverID: 99997, accGTID: gs.(*gomysql.MysqlGTIDSet)}
	if err := streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}
	if perr := <-parseErrCh; perr != nil &&
		!errors.Is(perr, context.DeadlineExceeded) && !errors.Is(perr, context.Canceled) {
		t.Fatalf("StreamParser error: %v", perr)
	}

	loaded, err := loadStreamState(indexDB)
	if err != nil || loaded == nil {
		t.Fatalf("loadStreamState err=%v nil=%v", err, loaded == nil)
	}
	// The streamed checkpoint must cover every transaction the source executed in
	// the window — INSERT (XID), ANALYZE (next-GTID fallback), INSERT (XID). If the
	// fallback regressed, the ANALYZE's GTID would be missing and the source's
	// executed set would NOT be fully covered. (Concurrent transactions on the
	// shared server only ADD GTIDs to the streamed set, so Contain stays robust.)
	if !strings.Contains(loaded.gtidSet, serverUUID) {
		t.Fatalf("gtid_set must contain the source UUID %q, got %q", serverUUID, loaded.gtidSet)
	}
	afterSet, err := gomysql.ParseMysqlGTIDSet(NormalizeGTIDSet(gtidAfter))
	if err != nil {
		t.Fatalf("parse gtid_executed (after) %q: %v", gtidAfter, err)
	}
	parts := make([]string, 0, 2)
	if gtidBefore != "" {
		parts = append(parts, gtidBefore)
	}
	if loaded.gtidSet != "" {
		parts = append(parts, loaded.gtidSet)
	}
	combined, err := gomysql.ParseMysqlGTIDSet(NormalizeGTIDSet(strings.Join(parts, ",")))
	if err != nil {
		t.Fatalf("parse combined set: %v", err)
	}
	if !combined.Contain(afterSet) {
		t.Errorf("fallback regression: the source's executed set is not fully covered by the streamed checkpoint — the ANALYZE's GTID is missing.\n  before=%q\n  after=%q\n  loaded=%q",
			gtidBefore, gtidAfter, loaded.gtidSet)
	}
}

func TestStreamLoop_tickerCheckpoint(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := indexer.New(db, 100)

	events := make(chan parser.Event, 10)
	ts := time.Now().UTC()

	// Enqueue 1 event but keep the channel open.
	events <- parser.Event{
		BinlogFile: "binlog.000001",
		StartPos:   0,
		EndPos:     100,
		Timestamp:  ts,
		Schema:     "testdb",
		Table:      "orders",
		EventType:  parser.EventInsert,
		PKValues:   "1",
		RowAfter:   map[string]any{"id": int64(1), "amount": 9.99},
	}

	state := &streamState{mode: "position", serverID: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		// 5 ms interval guarantees the ticker fires well before the 50 ms sleep.
		done <- streamLoop(ctx, events, idx, db, 5*time.Millisecond, state, observe.ForSource("test"), nil)
	}()

	// Wait for: (1) event consumed, (2) ticker to fire and save checkpoint.
	time.Sleep(50 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved by ticker")
	}
	if loaded.eventsIndexed != 1 {
		t.Errorf("expected 1 event in checkpoint, got %d", loaded.eventsIndexed)
	}
}

// TestStreamLoop_positionTracking verifies that state.binlogFile and
// state.binlogPos are updated from each event, reflecting the last event processed.
func TestStreamLoop_positionTracking(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := indexer.New(db, 100)

	events := make(chan parser.Event, 10)
	ts := time.Now().UTC()

	// Three events advancing across two binlog files.
	testEvents := []parser.Event{
		{
			BinlogFile: "binlog.000001", StartPos: 0, EndPos: 100, Timestamp: ts,
			Schema: "testdb", Table: "orders", EventType: parser.EventInsert,
			PKValues: "1", RowAfter: map[string]any{"id": int64(1)},
		},
		{
			BinlogFile: "binlog.000002", StartPos: 4, EndPos: 200, Timestamp: ts,
			Schema: "testdb", Table: "orders", EventType: parser.EventInsert,
			PKValues: "2", RowAfter: map[string]any{"id": int64(2)},
		},
		{
			BinlogFile: "binlog.000002", StartPos: 200, EndPos: 350, Timestamp: ts,
			Schema: "testdb", Table: "orders", EventType: parser.EventInsert,
			PKValues: "3", RowAfter: map[string]any{"id": int64(3)},
		},
	}
	for _, ev := range testEvents {
		events <- ev
	}
	close(events)

	state := &streamState{mode: "position", serverID: 1}

	if err := streamLoop(context.Background(), events, idx, db, time.Hour, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	// Final position should reflect the last event consumed.
	if state.binlogFile != "binlog.000002" {
		t.Errorf("binlogFile: expected binlog.000002, got %q", state.binlogFile)
	}
	if state.binlogPos != 350 {
		t.Errorf("binlogPos: expected 350, got %d", state.binlogPos)
	}
	if state.eventsIndexed != 3 {
		t.Errorf("eventsIndexed: expected 3, got %d", state.eventsIndexed)
	}
}

// TestStreamLoop_batchOverflow verifies that when the batch size is reached
// before the channel closes, events are flushed mid-stream and all rows are written.
func TestStreamLoop_batchOverflow(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	const batchSize = 3
	idx := indexer.New(db, batchSize)

	events := make(chan parser.Event, 20)
	ts := time.Now().UTC()

	// 7 events requires ceil(7/3) = 3 flushes (3 + 3 + 1 on close).
	const total = 7
	for i := range total {
		events <- parser.Event{
			BinlogFile: "binlog.000001",
			StartPos:   uint64(i * 100),
			EndPos:     uint64((i + 1) * 100),
			Timestamp:  ts,
			Schema:     "testdb",
			Table:      "orders",
			EventType:  parser.EventInsert,
			PKValues:   strconv.Itoa(i + 1),
			RowAfter:   map[string]any{"id": int64(i + 1)},
		}
	}
	close(events)

	state := &streamState{mode: "position", serverID: 1}

	if err := streamLoop(context.Background(), events, idx, db, time.Hour, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	if state.eventsIndexed != total {
		t.Errorf("eventsIndexed: expected %d, got %d", total, state.eventsIndexed)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&count); err != nil {
		t.Fatalf("count binlog_events: %v", err)
	}
	if count != total {
		t.Errorf("binlog_events rows: expected %d, got %d", total, count)
	}
}

// TestStreamLoop_gtidAccumulation verifies that event GTID values are accumulated
// into state.gtidSet and persisted in the checkpoint.
func TestStreamLoop_gtidAccumulation(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := indexer.New(db, 100)

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562" // go-mysql lowercases UUIDs

	// Seed the accumulated GTID set at uuid:1.
	gs, err := gomysql.ParseMysqlGTIDSet(uuid + ":1")
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet: %v", err)
	}
	acc := gs.(*gomysql.MysqlGTIDSet)

	state := &streamState{
		mode:     "gtid",
		serverID: 1,
		accGTID:  acc,
		gtidSet:  uuid + ":1",
	}

	ts := time.Now().UTC()
	events := make(chan parser.Event, 16)

	// Three transactions, each GTID start → row → COMMIT. GTIDs accumulate at the
	// commit boundary (#491), not on the row event — so each EventCommit advances
	// the set: uuid:1 (seed) + 2,3,4 → uuid:1-4.
	for i, gno := range []int64{2, 3, 4} {
		g := fmt.Sprintf("%s:%d", uuid, gno)
		events <- parser.Event{BinlogFile: "binlog.000001", EndPos: uint64(i*100 + 10), GTID: g, EventType: parser.EventGTID}
		events <- parser.Event{
			BinlogFile: "binlog.000001",
			StartPos:   uint64(i * 100),
			EndPos:     uint64((i + 1) * 100),
			Timestamp:  ts,
			GTID:       g,
			Schema:     "testdb",
			Table:      "orders",
			EventType:  parser.EventInsert,
			PKValues:   strconv.Itoa(i + 1),
			RowAfter:   map[string]any{"id": int64(i + 1)},
		}
		events <- parser.Event{BinlogFile: "binlog.000001", EndPos: uint64((i+1)*100 + 10), GTID: g, EventType: parser.EventCommit}
	}
	close(events)

	if err := streamLoop(context.Background(), events, idx, db, time.Hour, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	// state.gtidSet should now encode uuid:1-4 (1 from seed + 2,3,4 from events).
	if !strings.Contains(state.gtidSet, uuid) {
		t.Errorf("state.gtidSet: expected UUID, got %q", state.gtidSet)
	}
	if !strings.Contains(state.gtidSet, "1-4") {
		t.Errorf("state.gtidSet: expected range 1-4, got %q", state.gtidSet)
	}

	// Checkpoint should also persist the accumulated GTID.
	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved")
	}
	if !strings.Contains(loaded.gtidSet, "1-4") {
		t.Errorf("checkpoint gtidSet: expected 1-4 range, got %q", loaded.gtidSet)
	}
}
