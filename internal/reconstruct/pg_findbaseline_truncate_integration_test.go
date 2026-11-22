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
	if path != f.tableFile(root, at1) || !since.Equal(taken) {
		t.Fatalf("FindBaseline = %s at %s; want %s at the file's own time %s", path, since, f.tableFile(root, at1), taken)
	}
	if err := reconstruct.CheckDestructiveDDL(ctx, f.db, "public", "orders", reconstruct.DDLWindow{Since: since, Until: until}); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("the TRUNCATE check from the time FindBaseline returns: %v; want the refusal", err)
	}
	// What the folder's time missed.
	if err := reconstruct.CheckDestructiveDDL(ctx, f.db, "public", "orders", reconstruct.DDLWindow{Since: at1, Until: until}); err != nil {
		t.Fatalf("from the folder's time the check should see nothing (that is the defect): %v", err)
	}
}
