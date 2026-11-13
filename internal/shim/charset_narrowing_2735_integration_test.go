//go:build integration

package shim

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2735 in the shim's full-table _snapshot: an ALTER TABLE between the
// snapshot and AS OF narrowed users.name from utf8mb4 to latin1, with no row
// events. A baseline value latin1 cannot hold is '?' in the source now, so the
// read refuses; one latin1 holds came through the ALTER unchanged, so the read
// answers; and a row rewritten after the ALTER carries the new value, so it
// answers too.
func TestSnapshotFullTable_charsetNarrowing(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(5 * time.Minute)
	alterAt := hourTop.Add(7 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)

	// Snapshot 1 is the table before the ALTER, snapshot 2 the one capture
	// took for it.
	for _, s := range []struct {
		id      int
		at      time.Time
		charset string
	}{{1, snapTime.Add(-time.Minute), "utf8mb4"}, {2, alterAt, "latin1"}} {
		ts := s.at.Format("2006-01-02 15:04:05")
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, character_set_name, is_nullable)
			VALUES (?, ?, 'myapp', 'users', 'id', 1, 'PRI', 'int', 'int', NULL, 'NO'),
			       (?, ?, 'myapp', 'users', 'name', 2, '', 'varchar', 'varchar(20)', ?, 'YES')`,
			s.id, ts, s.id, ts, s.charset)
	}
	testutil.MustExec(t, db, `INSERT INTO schema_changes
		(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
		VALUES (?, 'mysql-bin.000009', 800, 'myapp', 'users', 'ALTER TABLE', 'ALTER TABLE users MODIFY name VARCHAR(20) CHARACTER SET latin1', 2)`,
		alterAt)

	baselineWith := func(name string) string {
		t.Helper()
		root := t.TempDir()
		dir := filepath.Join(root, reconstruct.SnapshotDirName(snapTime), "myapp")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		w, err := baseline.NewWriter(filepath.Join(dir, "users.parquet"), usersBaselineCols(), baseline.WriterConfig{
			Compression:  "none",
			RowGroupSize: 100,
			Metadata: map[string]string{
				baseline.MetaKeyBinlogFile: "mysql-bin.000009",
				baseline.MetaKeyBinlogPos:  "500",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range [][]string{{"1", name}, {"2", "plain"}} {
			if err := w.WriteRow(r, []bool{false, false}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return root
	}
	handler := func(root string) *Handler {
		return NewHandlerWithConfig(db, Config{AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: root}, slog.Default())
	}
	table := TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf}
	names := func(t *testing.T, h *Handler) []string {
		t.Helper()
		res, err := h.runSnapshot(table)
		if err != nil {
			t.Fatalf("_snapshot of the table: %v", err)
		}
		var out []string
		for _, c := range rowCells(t, res.Resultset) {
			out = append(out, c[1])
		}
		return out
	}

	lost := baselineWith("Ñandú ✓")
	_, err := handler(lost).runSnapshot(table)
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) || !strings.Contains(err.Error(), "from utf8mb4 to latin1") {
		t.Fatalf("a baseline value latin1 cannot hold, across the ALTER: %v, want ErrDestructiveDDL naming the change", err)
	}
	if strings.Contains(err.Error(), "✓") {
		t.Errorf("the refusal carries the row's value: %v", err)
	}

	kept := baselineWith("Ñandú €")
	if got := names(t, handler(kept)); len(got) != 2 || got[0] != "Ñandú €" {
		t.Errorf("a baseline value latin1 holds: %q, want it unchanged", got)
	}

	// The row is rewritten after the ALTER: its value is the change's, which
	// the latin1 column already held.
	testutil.InsertEvent(t, db, "mysql-bin.000009", 900, 950, alterAt.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		"myapp", "users", uint8(event.EventUpdate), "1", []byte(`["name"]`),
		[]byte(`{"id":1,"name":"?"}`), []byte(`{"id":1,"name":"Ñandú"}`))
	if got := names(t, handler(lost)); len(got) != 2 || got[0] != "Ñandú" {
		t.Errorf("a row rewritten after the ALTER: %q, want the rewritten value", got)
	}
}
