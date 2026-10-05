//go:build integration

package streamrun

import (
	"bytes"
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
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A row the parser cannot map, produced by a real server, streamed over the
// replication protocol, and read back from the persisted ledger (#2139).
//
// The source column is VARCHAR in latin2. Its non-ASCII letters are single
// bytes above 0x7F there, which is not valid UTF-8, and latin2 is not a
// character set the row mapper converts. So in ONE statement of two rows the
// ASCII row is captured and the other is dropped. Before #2139 that drop left
// one WARN in the log and a clean ledger: `status` said "no events skipped"
// over an index that was missing a change.
func TestStreamLoop_unmappableRowReachesThePersistedLedger(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestDB(t)

	testutil.MustExec(t, sourceDB, `CREATE TABLE notes (
		id   INT PRIMARY KEY,
		body VARCHAR(32) CHARACTER SET latin2 NOT NULL
	)`)

	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}
	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// One statement, one rows event, two rows: id=1 maps, id=2 does not.
	testutil.MustExec(t, sourceDB,
		"INSERT INTO notes (id, body) VALUES (1, 'plain'), (2, 'za\u017c\u00f3\u0142\u0107 g\u0119\u015bl\u0105')")
	// A second statement whose only row maps: capture must still be running
	// after the drop.
	testutil.MustExec(t, sourceDB, "INSERT INTO notes (id, body) VALUES (3, 'after')")

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
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99213, Flavor: "mysql",
		Host: hostStr, Port: uint16(portN), User: mc.User, Password: mc.Passwd,
	})
	defer syncer.Close()
	streamer, err := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if err != nil {
		t.Fatalf("StartSync: %v", err)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	// Wired as One wires it: the parser and the checkpoint share one tally.
	skips := parser.NewSkipCounters(nil)
	sp := parser.NewStreamParser(resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	sp.SetSkipCounters(skips)
	idx := indexer.New(indexDB, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()
	state := &streamState{mode: "position", serverID: 99213, skips: skips}
	if err := streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}
	if parseErr := <-parseErrCh; parseErr != nil &&
		!errors.Is(parseErr, context.DeadlineExceeded) && !errors.Is(parseErr, context.Canceled) {
		t.Fatalf("the stream stopped on a row it could not map; a skip must never stop capture: %v", parseErr)
	}

	// The index holds the rows that map, and not the one that does not.
	rows, err := indexDB.Query(
		`SELECT pk_values FROM binlog_events WHERE schema_name = ? AND table_name = 'notes' ORDER BY event_id`, sourceName)
	if err != nil {
		t.Fatalf("query binlog_events: %v", err)
	}
	defer rows.Close()
	var pks []string
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		pks = append(pks, pk)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if strings.Join(pks, ",") != "1,3" {
		t.Fatalf("indexed pks = %v, want [1 3]: the row before and the statement after the unmappable row", pks)
	}

	// The ledger, read back the way every reader reads it.
	stream, err := status.LoadStreamState(context.Background(), indexDB)
	if err != nil {
		t.Fatalf("LoadStreamState: %v", err)
	}
	if stream == nil {
		t.Fatal("no checkpoint was saved")
	}
	ledger, ok := stream.ParseCaptureSkips()
	if !ok {
		t.Fatalf("persisted ledger not readable: %+v", stream.CaptureSkips)
	}
	entry := ledger[status.CaptureSkipReasonRowMapFailed]
	if entry.Count != 1 {
		t.Fatalf("persisted ledger = %s, want 1 %s", stream.CaptureSkips.String, status.CaptureSkipReasonRowMapFailed)
	}
	if len(entry.Tables) != 1 || entry.Tables[0] != sourceName+".notes" {
		t.Errorf("ledger tables = %v, want [%s.notes]", entry.Tables, sourceName)
	}
	if entry.LastFile != binlogFile || entry.LastPos == 0 {
		t.Errorf("attribution = %s:%d, want a position in %s", entry.LastFile, entry.LastPos, binlogFile)
	}
	if len(ledger) != 1 {
		t.Errorf("unexpected other reasons in the ledger: %s", stream.CaptureSkips.String)
	}

	var text bytes.Buffer
	status.WriteStatus(&text, nil, nil, nil, nil, nil, stream)
	for _, want := range []string{"DEGRADED", status.CaptureSkipReasonRowMapFailed, sourceName + ".notes"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("status text missing %q:\n%s", want, text.String())
		}
	}
}
