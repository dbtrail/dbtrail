//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A PostgreSQL table file carried forward with a table delta beside it holds
// the rows as they were read, before the folder it sits in. A TRUNCATE
// between the two writes no row change, so the only thing that sees it is
// the TRUNCATE check, which every reader of one row at a past moment (the
// command line, the shim, the console, the MCP tool, verify) starts at the
// time FindBaseline returns. That time is the file's own, so the TRUNCATE is
// caught; from the folder's time it is not.
func TestFindBaseline_PGChainTruncateIsCaught(t *testing.T) {
	ctx := context.Background()
	f := newPGFixture(t)
	root := t.TempDir()
	taken := f.at(10 * time.Minute)
	f.baseline(root, taken, 0x500, taken.Add(-time.Minute).Format(time.RFC3339), pairRows()...)
	f.other(0x600, 11*time.Minute)
	f.event(0x700, 12*time.Minute, 2, "1", s("changed"))
	f.checkpoint(0x700)
	at1 := f.at(14 * time.Minute)
	reconstruct.SetPGTableDeltas(t, true)
	if _, _, _, err := f.run(root, pgRun{at: at1, carry: true, chainFloor: f.base}); err != nil {
		t.Fatal(err)
	}
	if len(f.pairFiles(root, at1)) != 2 || !f.sameFile(f.tableFile(root, taken), f.tableFile(root, at1)) {
		t.Fatalf("the update did not publish the file and a pair: %v", f.pairFiles(root, at1))
	}
	// Indexed after that update ran (capture behind), at a time between the
	// file's and the folder's.
	testutil.MustExec(t, f.db, fmt.Sprintf(`INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES ('%s', '0/690', 1680, 'public', 'orders', 'TRUNCATE TABLE', 'TRUNCATE public.orders')`, f.at(13*time.Minute).Format("2006-01-02 15:04:05")))

	until := f.at(16 * time.Minute)
	path, since, _, err := reconstruct.FindBaseline(ctx, root, "public", "orders", until)
	if err != nil {
		t.Fatal(err)
	}
	// The file's bound, a minute before its read (f.baseline's deltaSince).
	if want := taken.Add(-time.Minute); path != f.tableFile(root, at1) || !since.Equal(want) {
		t.Fatalf("FindBaseline = %s at %s; want %s at the file's own bound %s", path, since, f.tableFile(root, at1), want)
	}
	if err := reconstruct.CheckDestructiveDDL(ctx, f.db, "public", "orders", reconstruct.DDLWindow{Since: since, Until: until}); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("the TRUNCATE check from the time FindBaseline returns: %v; want the refusal", err)
	}
	// What the folder's time missed.
	if err := reconstruct.CheckDestructiveDDL(ctx, f.db, "public", "orders", reconstruct.DDLWindow{Since: at1, Until: until}); err != nil {
		t.Fatalf("from the folder's time the check should see nothing (that is the defect): %v", err)
	}
}

// The same over a file a snapshot update FOLDED (wrote in full from the file
// before and the changes since): its footer's time is that update's clock,
// and where its rows stand is its PGDeltaSince, earlier. A full read at 10
// minutes, an update at 14 that folds the table, a pair beside that file at
// 18, and a TRUNCATE at 13 indexed late: from the folded file's time (14) it
// is missed, from its bound it is caught.
func TestFindBaseline_PGChainOverAFoldedFileTruncateIsCaught(t *testing.T) {
	ctx := context.Background()
	f := newPGFixture(t)
	root := t.TempDir()
	// A full read's file records no bound: the first update folds it.
	f.baseline(root, f.at(10*time.Minute), 0x500, "", pairRows()...)
	f.other(0x600, 11*time.Minute)
	f.event(0x700, 11*time.Minute, 2, "1", s("one"))
	f.checkpoint(0x700)
	reconstruct.SetPGTableDeltas(t, true)
	at1 := f.at(14 * time.Minute)
	_, md1, _, err := f.run(root, pgRun{at: at1, carry: true, chainFloor: f.base})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.pairFiles(root, at1)) != 0 || md1.PGDeltaSince == "" {
		t.Fatalf("the first update did not fold the table: pairs %v, bound %q", f.pairFiles(root, at1), md1.PGDeltaSince)
	}
	bound, err := time.Parse(time.RFC3339, md1.PGDeltaSince)
	if err != nil {
		t.Fatal(err)
	}
	truncatedAt := f.at(13 * time.Minute)
	if !bound.Before(truncatedAt) || !md1.SnapshotTimestamp.After(truncatedAt) {
		t.Fatalf("the folded file is at %s with bound %s: the test needs a bound before %s and a time after it", md1.SnapshotTimestamp, bound, truncatedAt)
	}
	f.event(0x800, 16*time.Minute, 2, "3", s("three"))
	f.checkpoint(0x800)
	at2 := f.at(18 * time.Minute)
	if _, _, _, err := f.run(root, pgRun{at: at2, carry: true, chainFloor: f.base}); err != nil {
		t.Fatal(err)
	}
	if len(f.pairFiles(root, at2)) != 2 || !f.sameFile(f.tableFile(root, at1), f.tableFile(root, at2)) {
		t.Fatalf("the second update did not publish the folded file and a pair: %v", f.pairFiles(root, at2))
	}
	testutil.MustExec(t, f.db, fmt.Sprintf(`INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES ('%s', '0/790', 1880, 'public', 'orders', 'TRUNCATE TABLE', 'TRUNCATE public.orders')`, truncatedAt.Format("2006-01-02 15:04:05")))

	until := f.at(20 * time.Minute)
	_, since, _, err := reconstruct.FindBaseline(ctx, root, "public", "orders", until)
	if err != nil {
		t.Fatal(err)
	}
	if !since.Equal(bound) {
		t.Fatalf("FindBaseline = %s; want the folded file's bound %s (its time is %s)", since, bound, md1.SnapshotTimestamp)
	}
	if err := reconstruct.CheckDestructiveDDL(ctx, f.db, "public", "orders", reconstruct.DDLWindow{Since: since, Until: until}); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("the TRUNCATE check from the time FindBaseline returns: %v; want the refusal", err)
	}
	if err := reconstruct.CheckDestructiveDDL(ctx, f.db, "public", "orders", reconstruct.DDLWindow{Since: md1.SnapshotTimestamp, Until: until}); err != nil {
		t.Fatalf("from the folded file's own time the check should see nothing (that is the defect): %v", err)
	}
}
