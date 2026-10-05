//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A row the parser cannot map, produced by a real server and read by a real
// consumer of the skip ledger (#2139).
//
// The source column is VARCHAR in latin2. 'ą' is the single byte 0xB1 there,
// which is not valid UTF-8, and latin2 is not a character set the row mapper
// converts, so that row is dropped while the plain ASCII row of the SAME
// statement is captured. Before #2139 the drop left one WARN and a clean
// ledger, so captureComparable (the check a snapshot update runs before it
// trusts the index instead of re-reading the source) accepted an index that
// was missing a change.
func TestRowMapFailure_realBinlogRefusesComparison(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE notes (
		id   INT PRIMARY KEY,
		body VARCHAR(32) CHARACTER SET latin2 NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	// One statement, one rows event, two rows: the first maps, the second
	// does not.
	testutil.MustExec(t, sourceDB, "INSERT INTO notes (id, body) VALUES (1, 'plain'), (2, 'zażółć gęślą')")
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog),
	)
	if out, err := cpCmd.CombinedOutput(); err != nil {
		t.Fatalf("docker cp %s: %v\n%s", currentBinlog, err, out)
	}

	skips := parser.NewSkipCounters(nil)
	p := parser.New(tmpDir, res, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	p.SetSkipCounters(skips)
	events := make(chan parser.Event, 50)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		errCh <- p.ParseFile(ctx, currentBinlog, events)
	}()
	var ins []parser.Event
	for ev := range events {
		if ev.Table == "notes" && ev.EventType == parser.EventInsert {
			ins = append(ins, ev)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile stopped on a row it could not map; a skip must never stop capture: %v", err)
	}
	if len(ins) != 1 || ins[0].PKValues != "1" {
		t.Fatalf("captured %d INSERT rows (%+v), want only id=1", len(ins), ins)
	}

	// The ledger as the stream would persist it with its next checkpoint.
	raw, err := skips.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	st := streamStateFor(uuidA + ":1-10")
	st.CaptureSkips = sql.NullString{String: raw, Valid: true}
	ledger, ok := st.ParseCaptureSkips()
	if !ok {
		t.Fatalf("ledger not readable: %s", raw)
	}
	entry := ledger[status.CaptureSkipReasonRowMapFailed]
	if entry.Count != 1 || len(entry.Tables) != 1 || entry.Tables[0] != sourceName+".notes" {
		t.Fatalf("ledger entry = %+v (raw %s), want 1 skip naming %s.notes", entry, raw, sourceName)
	}

	// No full read of the source has happened since the drop: the index is
	// not comparable.
	anchor := time.Now().UTC().Add(time.Hour)
	if ok, detail := captureComparable(st, anchor, time.Time{}); ok {
		t.Fatal("captureComparable accepted an index that dropped a row")
	} else if detail == "" {
		t.Fatal("the refusal must say why")
	}
	if ok, _ := captureComparable(st, anchor, time.Now().UTC().Add(-time.Hour)); ok {
		t.Fatal("captureComparable accepted a drop newer than the last full read")
	}
	// Control: the same state with a clean ledger IS comparable, so the
	// refusals above come from the ledger entry and nothing else.
	clean := streamStateFor(uuidA + ":1-10")
	if ok, detail := captureComparable(clean, anchor, time.Time{}); !ok {
		t.Fatalf("control: a clean ledger must be comparable, got %q", detail)
	}
}
