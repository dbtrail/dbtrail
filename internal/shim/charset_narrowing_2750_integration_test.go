//go:build integration

package shim

import (
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedNarrowingIndex records two schema snapshots of myapp.users (name
// utf8mb4, then latin1) and the ALTER TABLE between them, at alterAt and
// mysql-bin.000009:800, with no row events (#2750).
func seedNarrowingIndex(t *testing.T, alterAt time.Time) (*sql.DB, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	addHourlyPartition(t, db, alterAt.Truncate(time.Hour))
	for _, s := range []struct {
		id      int
		at      time.Time
		charset string
	}{{1, alterAt.Add(-10 * time.Minute), "utf8mb4"}, {2, alterAt, "latin1"}} {
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
	return db, dbName
}

func narrowingAlterAt() time.Time {
	return time.Now().UTC().Truncate(time.Hour).Add(7 * time.Minute)
}

// The single-row _snapshot: a baseline value latin1 cannot hold refuses, one
// it holds answers, and a row rewritten after the ALTER answers.
func TestSnapshotRow_charsetNarrowing(t *testing.T) {
	alterAt := narrowingAlterAt()
	db, dbName := seedNarrowingIndex(t, alterAt)
	snapTime := alterAt.Add(-2 * time.Minute)
	asOf := alterAt.Add(3 * time.Minute)
	root := writeBaselineSnapshot(t, snapTime, "myapp", "users", usersBaselineCols(), [][]string{{"1", "Ñandú ✓"}, {"2", "Ñandú €"}})
	h := NewHandlerWithConfig(db, Config{AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: root}, slog.Default())
	row := func(pk string) TimeTravelQuery {
		return TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", PKColumn: "id", PKValue: pk, AsOf: asOf}
	}

	_, err := h.runSnapshot(row("1"))
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) || !strings.Contains(err.Error(), "from utf8mb4 to latin1") {
		t.Fatalf("a baseline value latin1 cannot hold: %v, want ErrDestructiveDDL naming the change", err)
	}
	if strings.Contains(err.Error(), "✓") {
		t.Errorf("the refusal carries the row's value: %v", err)
	}
	res, err := h.runSnapshot(row("2"))
	if err != nil {
		t.Fatalf("a baseline value latin1 holds: %v", err)
	}
	if c := rowCells(t, res.Resultset); len(c) != 1 || c[0][1] != "Ñandú €" {
		t.Errorf("a baseline value latin1 holds: %q", c)
	}

	testutil.InsertEvent(t, db, "mysql-bin.000009", 900, 950, alterAt.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		"myapp", "users", uint8(event.EventUpdate), "1", []byte(`["name"]`),
		[]byte(`{"id":1,"name":"?"}`), []byte(`{"id":1,"name":"Ñandú"}`))
	res, err = h.runSnapshot(row("1"))
	if err != nil {
		t.Fatalf("a row rewritten after the ALTER: %v", err)
	}
	if c := rowCells(t, res.Resultset); len(c) != 1 || c[0][1] != "Ñandú" {
		t.Errorf("a row rewritten after the ALTER: %q", c)
	}
}

// The reads from recorded changes alone: single-row and full-table
// _flashback, and the full-table _snapshot with no snapshot location. A row
// whose last change, before the ALTER, wrote a value latin1 cannot hold
// refuses, without naming a snapshot it does not have; once the row is
// rewritten after the ALTER, each read answers.
func TestChangesOnlyReads_charsetNarrowing(t *testing.T) {
	alterAt := narrowingAlterAt()
	db, dbName := seedNarrowingIndex(t, alterAt)
	asOf := alterAt.Add(3 * time.Minute)
	before := alterAt.Add(-time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "mysql-bin.000009", 600, 650, before, nil,
		"myapp", "users", uint8(event.EventInsert), "3", nil, nil, []byte(`{"id":3,"name":"Ñandú ✓"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000009", 650, 700, before, nil,
		"myapp", "users", uint8(event.EventInsert), "4", nil, nil, []byte(`{"id":4,"name":"Ñandú €"}`))
	h := NewHandlerWithConfig(db, Config{AllowGaps: true, NoArchive: true, IndexDBName: dbName}, slog.Default())

	reads := map[string]func() (int, error){
		"_flashback row": func() (int, error) {
			res, err := h.runPointInTime(TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "users", PKColumn: "id", PKValue: "3", AsOf: asOf})
			if err != nil {
				return 0, err
			}
			return len(res.Resultset.RowDatas), nil
		},
		"_flashback table": func() (int, error) {
			res, err := h.runPointInTime(TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "users", AsOf: asOf})
			if err != nil {
				return 0, err
			}
			return len(res.Resultset.RowDatas), nil
		},
		"_snapshot table, no location": func() (int, error) {
			res, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf})
			if err != nil {
				return 0, err
			}
			return len(res.Resultset.RowDatas), nil
		},
	}
	for name, read := range reads {
		_, err := read()
		if !errors.Is(err, reconstruct.ErrDestructiveDDL) || !strings.Contains(err.Error(), "recorded changes alone") ||
			!strings.Contains(err.Error(), "from utf8mb4 to latin1") {
			t.Errorf("%s: %v, want the changes-only refusal naming the change", name, err)
			continue
		}
		if strings.Contains(err.Error(), "✓") || strings.Contains(err.Error(), "snapshot this starts from") {
			t.Errorf("%s: the refusal carries the value or names a snapshot it does not have: %v", name, err)
		}
	}
	// A value latin1 holds, read on its own, answers.
	if res, err := h.runPointInTime(TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "users", PKColumn: "id", PKValue: "4", AsOf: asOf}); err != nil || len(res.Resultset.RowDatas) != 1 {
		t.Errorf("_flashback row latin1 holds: %v", err)
	}

	testutil.InsertEvent(t, db, "mysql-bin.000009", 900, 950, alterAt.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		"myapp", "users", uint8(event.EventUpdate), "3", []byte(`["name"]`),
		[]byte(`{"id":3,"name":"Ñandú ?"}`), []byte(`{"id":3,"name":"Ñandú"}`))
	want := map[string]int{"_flashback row": 1, "_flashback table": 2, "_snapshot table, no location": 2}
	for name, read := range reads {
		if n, err := read(); err != nil || n != want[name] {
			t.Errorf("%s after the row was rewritten: %d rows, %v; want %d rows", name, n, err, want[name])
		}
	}
}
