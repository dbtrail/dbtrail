//go:build integration

package streamrun

import (
	"context"
	"errors"
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

// TestStreamLoop_liveReplication_mariadb is the MariaDB-source end-to-end guard
// (alpha). It pairs a MySQL INDEX (CreateTestDB, 13306) with a MariaDB SOURCE
// (CreateTestMariaDB, 13307) and streams real row events through the full
// engine. Beyond proving rows are captured, it is the discriminator for two
// MariaDB-specific fixes that no unit test exercises:
//
//   - config.CurrentBinlogPosition must succeed against MariaDB (SHOW MASTER
//     STATUS returns 4 columns there, not 5 — the column-tolerant scan).
//   - the BinlogSyncer Flavor="mariadb" handshake + the MariadbGTIDEvent parser
//     case must populate domain-server-seq GTIDs on indexed rows.
//
// It runs in POSITION mode (the mode this end-to-end guard exercises)
// and asserts via the indexed-event counter, not raw event counts — MariaDB
// interleaves ANNOTATE_ROWS / GTID_LIST / BINLOG_CHECKPOINT events the indexer
// never counts.
func TestStreamLoop_liveReplication_mariadb(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		amount DECIMAL(10,2) NOT NULL
	)`)

	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MariaDB")
	}

	// THE position-discovery discriminator: this call, not memory, proves the
	// SHOW BINARY LOG STATUS / SHOW MASTER STATUS fallback chain works on the
	// pinned MariaDB. A 4-vs-5 column scan mismatch would fail here.
	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition against MariaDB failed (position discovery regression): %v", err)
	}

	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	mc, err := drivermysql.ParseDSN(testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true")
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

	for i := range 5 {
		testutil.MustExec(t, sourceDB,
			"INSERT INTO orders (amount) VALUES (?)", float64(i+1)*10.0)
	}

	// The MariaDB flavor drives the replication handshake (mariadb_slave_capability
	// + the MariaDB GTID dump command).
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99997,
		Flavor:   gomysql.MariaDBFlavor,
		Host:     hostStr,
		Port:     uint16(portN),
		User:     mc.User,
		Password: mc.Passwd,
		// Mirrors the production syncer (#1117): MariaDB 11.4+ sends
		// cache-buffered events with LogPos=0; without the fill the belt in
		// handleRows rejects the rows rather than index underflowed positions.
		FillZeroLogPos: true,
	})
	defer syncer.Close()

	streamer, syncErr := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if syncErr != nil {
		testutil.SkipOrFailMariaDB(t, "StartSync against MariaDB failed (replication may not be granted): %v", syncErr)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	filters := parser.Filters{Schemas: map[string]bool{sourceName: true}}

	sp := parser.NewStreamParser(resolver, filters, nil)
	idx := indexer.New(indexDB, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	state := &streamState{mode: "position", flavor: gomysql.MariaDBFlavor, serverID: 99997}
	if err := streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test-mariadb"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	parseErr := <-parseErrCh
	if parseErr != nil &&
		!errors.Is(parseErr, context.DeadlineExceeded) &&
		!errors.Is(parseErr, context.Canceled) {
		t.Fatalf("StreamParser error: %v", parseErr)
	}

	if state.eventsIndexed < 5 {
		t.Errorf("expected at least 5 events indexed from MariaDB, got %d", state.eventsIndexed)
	}

	// The checkpoint must record the MariaDB flavor so a later resume re-parses
	// any saved gtid_set with the MariaDB parser.
	loaded, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved after streaming")
	}
	if loaded.flavor != gomysql.MariaDBFlavor {
		t.Errorf("checkpoint flavor = %q, want mariadb", loaded.flavor)
	}

	// At least one indexed row must carry a MariaDB domain-server-seq GTID,
	// proving the MariadbGTIDEvent parser case fired end-to-end.
	var gtid string
	if err := indexDB.QueryRow(
		`SELECT gtid FROM binlog_events WHERE gtid IS NOT NULL AND gtid <> '' ORDER BY event_id LIMIT 1`,
	).Scan(&gtid); err != nil {
		t.Fatalf("query indexed gtid: %v", err)
	}
	// MariaDB GTID is "domain-server-seq" (digits and dashes), never a MySQL UUID.
	if strings.Count(gtid, "-") != 2 || strings.Contains(gtid, ":") {
		t.Errorf("indexed gtid %q is not MariaDB domain-server-seq form", gtid)
	}
}

// TestStreamLoop_gtidAdvancesOnCommit_mariadb is the MariaDB sibling of
// TestStreamLoop_gtidAdvancesOnCommit_live. It runs streamLoop in GTID mode with
// a *MariadbGTIDSet accumulator so MariadbGTIDSet.Update() — the durable-checkpoint
// path that position mode never exercises — is covered in CI. It proves the GTID
// checkpoint advances (and is re-parseable) for a MariaDB source.
func TestStreamLoop_gtidAdvancesOnCommit_mariadb(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT, amount DECIMAL(10,2) NOT NULL)`)
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}

	// Seed the accumulator from the source's current GTID position so Update()
	// appends onto a realistic non-empty *MariadbGTIDSet.
	var seedGTID string
	if err := sourceDB.QueryRow("SELECT @@gtid_binlog_pos").Scan(&seedGTID); err != nil {
		t.Fatalf("read @@gtid_binlog_pos: %v", err)
	}
	accGTID, err := parseGTIDSetForFlavor(gomysql.MariaDBFlavor, seedGTID)
	if err != nil {
		t.Fatalf("parse seed GTID %q: %v", seedGTID, err)
	}

	mc, err := drivermysql.ParseDSN(testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	hostStr, portStr, _ := net.SplitHostPort(mc.Addr)
	portN, _ := strconv.ParseUint(portStr, 10, 16)

	testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (10.0)")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (20.0)")

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99996, Flavor: gomysql.MariaDBFlavor,
		Host: hostStr, Port: uint16(portN), User: mc.User, Password: mc.Passwd,
		FillZeroLogPos: true, // #1117 — mirrors the production syncer
	})
	defer syncer.Close()

	streamer, syncErr := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if syncErr != nil {
		testutil.SkipOrFailMariaDB(t, "StartSync against MariaDB failed: %v", syncErr)
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

	state := &streamState{mode: "gtid", flavor: gomysql.MariaDBFlavor, serverID: 99996, accGTID: accGTID}
	if err := streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test-mariadb-gtid"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	parseErr := <-parseErrCh
	if parseErr != nil &&
		!errors.Is(parseErr, context.DeadlineExceeded) &&
		!errors.Is(parseErr, context.Canceled) {
		t.Fatalf("StreamParser error: %v", parseErr)
	}

	// advanceGTID must have grown the durable set via MariadbGTIDSet.Update().
	if state.gtidSet == "" {
		t.Fatal("expected accumulated GTID set after committing transactions, got empty")
	}
	if strings.Contains(state.gtidSet, ":") {
		t.Errorf("accumulated set %q is not MariaDB domain-server-seq form", state.gtidSet)
	}
	// The persisted set must re-parse with the MariaDB parser (resume contract).
	if _, err := parseGTIDSetForFlavor(gomysql.MariaDBFlavor, state.gtidSet); err != nil {
		t.Errorf("accumulated MariaDB set %q does not re-parse: %v", state.gtidSet, err)
	}
}

// TestStreamLoop_mariadbRowPositionsSaneAndMonotonic is the #1117 position
// baseline: MariaDB 11.4+ writes cache-buffered events (TABLE_MAP, rows,
// ANNOTATE) with end_log_pos=0 in the binlog, so without FillZeroLogPos every
// captured row stored start_pos = 2^64-EventSize (underflow) and end_pos = 0.
// This streams real traffic and asserts every indexed row carries sane,
// monotonic positions. Starting mid-file (CurrentBinlogPosition of a server
// with prior traffic) also exercises the connect-time FDE guard exemption
// end-to-end: before the guard fix, FillZeroLogPos's filled FDE false-tripped
// the wraparound detector on exactly this resume shape and Run failed loud.
func TestStreamLoop_mariadbRowPositionsSaneAndMonotonic(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT, amount DECIMAL(10,2) NOT NULL)`)
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}

	mc, err := drivermysql.ParseDSN(testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	hostStr, portStr, _ := net.SplitHostPort(mc.Addr)
	portN, _ := strconv.ParseUint(portStr, 10, 16)

	for i := range 5 {
		testutil.MustExec(t, sourceDB,
			"INSERT INTO orders (amount) VALUES (?)", float64(i+1)*10.0)
	}

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99995, Flavor: gomysql.MariaDBFlavor,
		Host: hostStr, Port: uint16(portN), User: mc.User, Password: mc.Passwd,
		FillZeroLogPos: true, // #1117 — mirrors the production syncer
	})
	defer syncer.Close()

	streamer, syncErr := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if syncErr != nil {
		testutil.SkipOrFailMariaDB(t, "StartSync against MariaDB failed: %v", syncErr)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	sp := parser.NewStreamParser(resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	sp.SetFlavor(gomysql.MariaDBFlavor)
	idx := indexer.New(indexDB, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	state := &streamState{mode: "position", flavor: gomysql.MariaDBFlavor, serverID: 99995}
	if err := streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test-mariadb-pos"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	parseErr := <-parseErrCh
	if parseErr != nil &&
		!errors.Is(parseErr, context.DeadlineExceeded) &&
		!errors.Is(parseErr, context.Canceled) {
		// A wraparound false-trip on the connect-time FDE lands here.
		t.Fatalf("StreamParser error: %v", parseErr)
	}

	rows, err := indexDB.Query(
		`SELECT binlog_file, start_pos, end_pos FROM binlog_events ORDER BY event_id`)
	if err != nil {
		t.Fatalf("query indexed positions: %v", err)
	}
	defer rows.Close()

	var count int
	lastByFile := map[string]uint64{}
	for rows.Next() {
		var file string
		var start, end uint64
		if err := rows.Scan(&file, &start, &end); err != nil {
			t.Fatalf("scan positions: %v", err)
		}
		count++
		// The corrupt shape this test exists to prevent: start ≈ 2^64, end = 0.
		if end == 0 {
			t.Errorf("row %d: end_pos = 0 (position was never established)", count)
		}
		if start >= end {
			t.Errorf("row %d: start_pos %d >= end_pos %d", count, start, end)
		}
		// LogPos is a uint32 wire field; anything above 4GiB here is underflow.
		if start > uint64(^uint32(0)) {
			t.Errorf("row %d: start_pos %d exceeds the uint32 wire-format range (underflow)", count, start)
		}
		if prev, ok := lastByFile[file]; ok && start < prev {
			t.Errorf("row %d: start_pos %d went backward from %d within %s", count, start, prev, file)
		}
		lastByFile[file] = start
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate positions: %v", err)
	}
	if count < 5 {
		t.Fatalf("expected at least 5 indexed rows with positions to check, got %d", count)
	}
}

// TestStreamLoop_mariadbResumeDedupDoesNotDeleteIndexedRows is the #1117
// resume-safety acceptance (the GTID-mode half of #500's acceptance, which was
// unrunnable while this bug corrupted start_pos): after streaming and
// checkpointing MariaDB traffic, running the resume-time dedup against the
// saved checkpoint — exactly what One does on restart — must delete NOTHING.
// Before the fix, every row's start_pos (~2^64) satisfied `start_pos >= pos`
// and the whole file's worth of indexed rows vanished on every restart.
func TestStreamLoop_mariadbResumeDedupDoesNotDeleteIndexedRows(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT, amount DECIMAL(10,2) NOT NULL)`)
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}

	var seedGTID string
	if err := sourceDB.QueryRow("SELECT @@gtid_binlog_pos").Scan(&seedGTID); err != nil {
		t.Fatalf("read @@gtid_binlog_pos: %v", err)
	}
	accGTID, err := parseGTIDSetForFlavor(gomysql.MariaDBFlavor, seedGTID)
	if err != nil {
		t.Fatalf("parse seed GTID %q: %v", seedGTID, err)
	}

	mc, err := drivermysql.ParseDSN(testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	hostStr, portStr, _ := net.SplitHostPort(mc.Addr)
	portN, _ := strconv.ParseUint(portStr, 10, 16)

	testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (10.0)")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (20.0)")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (30.0)")

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99994, Flavor: gomysql.MariaDBFlavor,
		Host: hostStr, Port: uint16(portN), User: mc.User, Password: mc.Passwd,
		FillZeroLogPos: true, // #1117 — mirrors the production syncer
	})
	defer syncer.Close()

	streamer, syncErr := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if syncErr != nil {
		testutil.SkipOrFailMariaDB(t, "StartSync against MariaDB failed: %v", syncErr)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	sp := parser.NewStreamParser(resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	sp.SetFlavor(gomysql.MariaDBFlavor)
	idx := indexer.New(indexDB, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	state := &streamState{mode: "gtid", flavor: gomysql.MariaDBFlavor, serverID: 99994, accGTID: accGTID}
	if err := streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test-mariadb-resume"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	parseErr := <-parseErrCh
	if parseErr != nil &&
		!errors.Is(parseErr, context.DeadlineExceeded) &&
		!errors.Is(parseErr, context.Canceled) {
		t.Fatalf("StreamParser error: %v", parseErr)
	}

	var indexed int
	if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&indexed); err != nil {
		t.Fatalf("count indexed rows: %v", err)
	}
	if indexed < 3 {
		t.Fatalf("expected at least 3 indexed rows before the resume dedup, got %d", indexed)
	}

	saved, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if saved == nil {
		t.Fatal("expected a saved checkpoint after streaming")
	}
	savedSet, err := parseGTIDSetForFlavor(gomysql.MariaDBFlavor, saved.gtidSet)
	if err != nil {
		t.Fatalf("re-parse saved GTID set %q: %v", saved.gtidSet, err)
	}

	// The exact resume-time cut One performs on restart, both modes. With sane
	// start_pos values, nothing sits at-or-beyond the checkpoint.
	n, err := deleteEventsSinceCheckpointGTID(indexDB, saved.binlogFile, saved.binlogPos, savedSet, gomysql.MariaDBFlavor, saved.dedupFloorID)
	if err != nil {
		t.Fatalf("deleteEventsSinceCheckpointGTID: %v", err)
	}
	if n != 0 {
		t.Fatalf("resume dedup deleted %d already-indexed rows; a restart must not destroy the index", n)
	}

	var after int
	if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&after); err != nil {
		t.Fatalf("count rows after dedup: %v", err)
	}
	if after != indexed {
		t.Fatalf("row count changed across the resume dedup: %d -> %d", indexed, after)
	}
}

// TestStreamParser_mariadbMidTransactionResumeExactPositions is the #1117
// review acceptance: a position-mode resume landing MID-TRANSACTION (a legal
// #775 statement-boundary checkpoint) on MariaDB 11.4+. The server honors the
// offset and re-sends the file's FDE with LogPos zeroed; FillZeroLogPos fills
// that ghost to resumePos+len(FDE), and the transaction tail's cache-buffered
// events inherit the overshoot until the genuine XID snaps back — so before
// the resumeFillCorrector, the tail rows were stored inflated by exactly
// len(FDE) (positions that are not event boundaries — a checkpoint persisting
// one is a fatal 1236 on the next restart) and the snap-back tripped the
// wraparound guard, killing the stream on every such resume.
//
// The test streams a 3-statement transaction twice: pass 1 from a
// transaction boundary (known-good positions), pass 2 resuming from the
// SECOND statement's end (mid-transaction). The tail row's positions in pass
// 2 must EXACTLY equal pass 1's — the exactness anchor that catches any
// constant-offset inflation a sane/monotonic check would miss.
func TestStreamParser_mariadbMidTransactionResumeExactPositions(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT, amount DECIMAL(10,2) NOT NULL)`)
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}

	// One transaction, three statements — each statement is its own
	// ANNOTATE/TABLE_MAP/rows(STMT_END_F) group in the binlog.
	tx, err := sourceDB.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for i := range 3 {
		if _, err := tx.Exec("INSERT INTO orders (amount) VALUES (?)", float64(i+1)*10.0); err != nil {
			t.Fatalf("tx insert %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	mc, err := drivermysql.ParseDSN(testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	hostStr, portStr, _ := net.SplitHostPort(mc.Addr)
	portN, _ := strconv.ParseUint(portStr, 10, 16)

	// capture streams from (file, pos) with the production syncer config and
	// returns the parsed events for this test's schema.
	capture := func(serverID uint32, file string, pos uint32) []parser.Event {
		t.Helper()
		syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
			ServerID: serverID, Flavor: gomysql.MariaDBFlavor,
			Host: hostStr, Port: uint16(portN), User: mc.User, Password: mc.Passwd,
			FillZeroLogPos: true, // #1117 — mirrors the production syncer
		})
		defer syncer.Close()

		streamer, syncErr := syncer.StartSync(gomysql.Position{Name: file, Pos: pos})
		if syncErr != nil {
			testutil.SkipOrFailMariaDB(t, "StartSync against MariaDB failed: %v", syncErr)
		}

		sp := parser.NewStreamParser(resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
		sp.SetFlavor(gomysql.MariaDBFlavor)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		events := make(chan parser.Event, 100)
		parseErrCh := make(chan error, 1)
		go func() {
			defer close(events)
			parseErrCh <- sp.Run(ctx, streamer, events)
		}()
		var got []parser.Event
		for ev := range events {
			got = append(got, ev)
		}
		if parseErr := <-parseErrCh; parseErr != nil &&
			!errors.Is(parseErr, context.DeadlineExceeded) &&
			!errors.Is(parseErr, context.Canceled) {
			// Before the corrector, the mid-transaction pass died here with
			// the wraparound false-trip.
			t.Fatalf("StreamParser error: %v", parseErr)
		}
		return got
	}

	inserts := func(evs []parser.Event) []parser.Event {
		var rows []parser.Event
		for _, ev := range evs {
			if ev.EventType == parser.EventInsert {
				rows = append(rows, ev)
			}
		}
		return rows
	}

	// Pass 1: from the pre-transaction boundary — known-good positions.
	pass1 := inserts(capture(99993, binlogFile, binlogPos))
	if len(pass1) != 3 {
		t.Fatalf("pass 1: expected the 3 transaction rows, got %d", len(pass1))
	}
	for i := 1; i < 3; i++ {
		if pass1[i].StartPos < pass1[i-1].EndPos {
			t.Fatalf("pass 1 rows not contiguous/increasing: %+v", pass1)
		}
	}

	// Pass 2: resume from the SECOND statement's end — mid-transaction.
	midPos := uint32(pass1[1].EndPos)
	pass2 := inserts(capture(99992, pass1[1].BinlogFile, midPos))
	if len(pass2) != 1 {
		t.Fatalf("pass 2 (mid-transaction resume): expected exactly the tail row, got %d rows", len(pass2))
	}
	// THE exactness anchor: the tail row's positions after a mid-transaction
	// resume must byte-for-byte equal the positions a boundary start produced.
	if pass2[0].StartPos != pass1[2].StartPos || pass2[0].EndPos != pass1[2].EndPos {
		t.Errorf("mid-transaction resume stored tail row at [%d, %d], want exactly [%d, %d] (constant-offset inflation)",
			pass2[0].StartPos, pass2[0].EndPos, pass1[2].StartPos, pass1[2].EndPos)
	}
	if pass2[0].PKValues != pass1[2].PKValues {
		t.Errorf("mid-transaction resume replayed pk %q, want %q", pass2[0].PKValues, pass1[2].PKValues)
	}
}

// TestDetectMariaDBGTIDGap_livePurge is the discriminator for real MariaDB GTID
// gap detection. The sqlmock unit tests pin the decision tree; this proves the
// three live queries (SHOW BINARY LOGS, BINLOG_GTID_POS, @@gtid_binlog_pos)
// behave as the gate-#1 design verified against MariaDB 11.4 — including the part
// no unit test can: that BINLOG_GTID_POS over the oldest surviving binlog yields
// a real purge floor after PURGE BINARY LOGS.
//
// It manufactures a genuine unfillable gap, then closes the auto-advance loop:
// advancing the checkpoint to the floor must (a) re-parse with the MariaDB parser
// and (b) clear the unfillable gap on the next detection — i.e. the advanced
// position re-syncs cleanly, which is the one thing the design flagged as needing
// a live check.
func TestDetectMariaDBGTIDGap_livePurge(t *testing.T) {
	sourceDB, _ := testutil.CreateTestMariaDB(t)

	// PURGE BINARY LOGS mutates server-wide binlog state, so this test must be
	// the only writer — the dedicated CI job serializes MariaDB tests with -p 1.
	testutil.MustExec(t, sourceDB, `CREATE TABLE gap_probe (
		id INT PRIMARY KEY AUTO_INCREMENT, v INT NOT NULL)`)

	for i := range 3 {
		testutil.MustExec(t, sourceDB, "INSERT INTO gap_probe (v) VALUES (?)", i)
	}

	// Checkpoint T1: the GTID position after the first few transactions.
	var checkpoint string
	if err := sourceDB.QueryRow("SELECT @@gtid_binlog_pos").Scan(&checkpoint); err != nil {
		t.Fatalf("read checkpoint @@gtid_binlog_pos: %v", err)
	}
	if checkpoint == "" {
		testutil.SkipOrFailMariaDB(t, "MariaDB reported an empty GTID position; GTID binlogging not active")
	}

	// Roll several binlog files (with a transaction in each) so PURGE has earlier
	// files to drop and the floor lands strictly past T1.
	for i := range 5 {
		testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
		testutil.MustExec(t, sourceDB, "INSERT INTO gap_probe (v) VALUES (?)", 100+i)
	}

	// Before purging: the checkpoint is behind but every binlog is still present,
	// so detection must NOT report an unfillable gap.
	pre, err := detectMariaDBGTIDGap(sourceDB, checkpoint, 30*time.Second)
	if err != nil {
		t.Fatalf("pre-purge detect: %v", err)
	}
	if pre.HasGap && !pre.Fillable {
		t.Fatalf("pre-purge: expected fillable/no-gap before any purge, got %+v", pre)
	}

	// Purge up to the current active binlog so T1's binlogs are gone.
	latestFile, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	testutil.MustExec(t, sourceDB, "PURGE BINARY LOGS TO '"+latestFile+"'")

	// Now the gap MUST be unfillable, and PurgedGTIDSet must carry the floor.
	gap, err := detectMariaDBGTIDGap(sourceDB, checkpoint, 30*time.Second)
	if err != nil {
		t.Fatalf("post-purge detect: %v", err)
	}
	if !gap.HasGap || gap.Fillable {
		t.Fatalf("post-purge: expected an UNFILLABLE gap for a checkpoint below the purge floor, got %+v", gap)
	}
	if gap.PurgedGTIDSet == "" {
		t.Fatal("post-purge: expected the purge floor in PurgedGTIDSet")
	}
	if strings.Contains(gap.PurgedGTIDSet, ":") {
		t.Errorf("purge floor %q is not MariaDB domain-server-seq form", gap.PurgedGTIDSet)
	}

	// Auto-advance contract (a): the floor re-parses with the MariaDB parser —
	// exactly what runStream feeds parseGTIDSetForFlavor + StartSyncGTID.
	if _, err := parseGTIDSetForFlavor(gomysql.MariaDBFlavor, gap.PurgedGTIDSet); err != nil {
		t.Fatalf("advanced checkpoint (floor) %q does not re-parse with the MariaDB parser: %v", gap.PurgedGTIDSet, err)
	}

	// Auto-advance contract (b): resuming from the floor clears the unfillable
	// gap — the advanced position re-syncs cleanly instead of re-tripping.
	after, err := detectMariaDBGTIDGap(sourceDB, gap.PurgedGTIDSet, 30*time.Second)
	if err != nil {
		t.Fatalf("post-advance detect: %v", err)
	}
	if after.HasGap && !after.Fillable {
		t.Fatalf("post-advance: advancing to the floor must clear the unfillable gap, got %+v", after)
	}
}
