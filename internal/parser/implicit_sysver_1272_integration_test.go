//go:build integration

package parser_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestParseFile_implicitSystemVersioning_mariadb pins the #1272 fix end to
// end against a real MariaDB binlog. An IMPLICITLY system-versioned table
// (`CREATE TABLE ... WITH SYSTEM VERSIONING`, no declared period columns)
// hides row_start/row_end from information_schema.COLUMNS while its binlog
// row images carry them — before the snapshot synthesized the hidden columns,
// the parser skipped EVERY event of the table as a column-count mismatch and
// a full-table reconstruct silently returned baseline-only state.
//
// Three proofs in one run:
//  1. TakeSnapshot records the synthetic columns (4 total) with row_end a
//     generated PK member, so reconstruct.GeneratedPKColumn fires (#1266).
//  2. The parser CAPTURES the table's events instead of skipping them, and
//     every pk_values carries the extended composite key.
//  3. The captured mix documents the versioned-binlog reality (same as the
//     explicit form, verified on MariaDB 11.4): an UPDATE logs the
//     current-row update PLUS a history-row INSERT, and a DELETE logs a
//     row_end tombstone UPDATE, never a Delete_rows — so INSERT+UPDATE+DELETE
//     on one row yields 2 inserts / 2 updates / 0 deletes.
func TestParseFile_implicitSystemVersioning_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE imp (id INT PRIMARY KEY, val VARCHAR(20)) WITH SYSTEM VERSIONING`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	tm, err := res.Resolve(sourceName, "imp")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(tm.Columns) != 4 {
		t.Fatalf("snapshot has %d columns, want 4 (id, val + synthetic row_start,row_end): %+v", len(tm.Columns), tm.Columns)
	}
	if c, ok := reconstruct.GeneratedPKColumn(tm.PKColumnMetas()); !ok || c.Name != "row_end" {
		t.Fatalf("GeneratedPKColumn = (%q, %v), want (row_end, true) — the synthetic PK extension is missing", c.Name, ok)
	}

	// Real binlog window: one row through INSERT → UPDATE → DELETE.
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition (MariaDB): %v", err)
	}
	testutil.MustExec(t, sourceDB, "INSERT INTO imp VALUES (1,'a')")
	testutil.MustExec(t, sourceDB, "UPDATE imp SET val='b' WHERE id=1")
	testutil.MustExec(t, sourceDB, "DELETE FROM imp WHERE id=1")
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mariadb:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog),
	)
	if out, err := cpCmd.CombinedOutput(); err != nil {
		testutil.SkipOrFailMariaDB(t, "docker cp %s from bintrail-test-mariadb failed: %v\n%s", currentBinlog, err, out)
	}

	p := parser.New(tmpDir, res, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	events := make(chan parser.Event, 100)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		errCh <- p.ParseFile(context.Background(), currentBinlog, events)
	}()
	all := drainEvents(events)
	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile: %v", err)
	}

	var ins, upd, del int
	for _, ev := range dmlEvents(all) {
		if ev.Schema != sourceName || ev.Table != "imp" {
			continue
		}
		switch ev.EventType {
		case parser.EventInsert:
			ins++
		case parser.EventUpdate:
			upd++
		case parser.EventDelete:
			del++
		}
		// The extended key must reach pk_values: "id|row_end" — a single
		// component means the synthetic PK member was not applied.
		if !strings.Contains(ev.PKValues, "|") {
			t.Errorf("pk_values %q lacks the row_end component", ev.PKValues)
		}
	}
	if ins == 0 && upd == 0 && del == 0 {
		t.Fatal("no events captured for the implicitly-versioned table — the column-count-mismatch skip is back (#1272)")
	}
	if ins != 2 || upd != 2 || del != 0 {
		t.Fatalf("event mix = %d inserts / %d updates / %d deletes, want 2/2/0 (history INSERT + tombstone UPDATE, no Delete_rows)", ins, upd, del)
	}
}

// TestTakeSnapshot_pkLessVersionedTableRefused_mariadb pins the validation-bypass fix
// against a real MariaDB: `CREATE TABLE t (x INT) WITH SYSTEM VERSIONING` is
// legal and PK-less, and MariaDB reports it as TABLE_TYPE 'SYSTEM VERSIONED'
// — before the widened scan it bypassed the no-PK validation entirely, and
// the synthesis would have fabricated a one-column generated PK whose
// sentinel row_end collapses every live row onto one pk_values. Strict
// TakeSnapshot must refuse it like any other PK-less table. The name ends in
// _mariadb because the MariaDB job picks its tests by name (#2137).
func TestTakeSnapshot_pkLessVersionedTableRefused_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE nopk (x INT) WITH SYSTEM VERSIONING`)

	_, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("strict TakeSnapshot must refuse a PK-less system-versioned table, got nil")
	}
	if !strings.Contains(err.Error(), "without a primary key") || !strings.Contains(err.Error(), "nopk") {
		t.Fatalf("want the no-PK validation refusal naming nopk, got: %v", err)
	}
}
