//go:build integration

package reconstruct

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// SpellIndexPKFilter is the seam every --pk/--pks lookup goes through (CLI
// query and recover, MCP, console): for a system-versioned table it must
// turn the declared-key value into the stored spellings (#2007), and leave
// any other table's lookup as it was.
func TestSpellIndexPKFilter_sysVersionedKey_2007(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	for _, row := range []string{
		"(1, NOW(), 'shop', 'prices', 'id', 1, 'PRI', 'int', 'NO', 0)",
		"(1, NOW(), 'shop', 'prices', 'row_start', 2, '', 'timestamp', 'NO', 1)",
		"(1, NOW(), 'shop', 'prices', 'row_end', 3, 'PRI', 'timestamp', 'NO', 1)",
		"(1, NOW(), 'shop', 'plain', 'id', 1, 'PRI', 'int', 'NO', 0)",
	} {
		testutil.MustExec(t, db, "INSERT INTO schema_snapshots (snapshot_id, snapshot_time, schema_name, table_name, "+
			"column_name, ordinal_position, column_key, data_type, is_nullable, is_generated) VALUES "+row)
	}

	opts := query.Options{Schema: "shop", Table: "prices", PKValues: "2"}
	if _, err := SpellIndexPKFilter(context.Background(), db, &opts); err != nil {
		t.Fatal(err)
	}
	want := "2,2|2038-01-19 03:14:07.999999,2|2106-02-07 06:28:15.999999"
	if opts.PKValues != "" || opts.PKValuesAlt != "" || strings.Join(opts.PKValuesIn, ",") != want {
		t.Fatalf("versioned --pk: PKValues=%q Alt=%q In=%q, want only In=%q", opts.PKValues, opts.PKValuesAlt, opts.PKValuesIn, want)
	}

	opts = query.Options{Schema: "shop", Table: "prices", PKValuesIn: []string{"2", "3"}}
	if _, err := SpellIndexPKFilter(context.Background(), db, &opts); err != nil {
		t.Fatal(err)
	}
	if len(opts.PKValuesIn) != 6 {
		t.Fatalf("versioned --pks: In=%q, want both values with both markers", opts.PKValuesIn)
	}

	opts = query.Options{Schema: "shop", Table: "plain", PKValues: "2"}
	if _, err := SpellIndexPKFilter(context.Background(), db, &opts); err != nil {
		t.Fatal(err)
	}
	if opts.PKValues != "2" || len(opts.PKValuesIn) != 0 {
		t.Fatalf("plain table: PKValues=%q In=%q, want the lookup unchanged", opts.PKValues, opts.PKValuesIn)
	}
}

// A table versioned once and no longer (DROP SYSTEM VERSIONING) still holds
// events stored under the versioned key: the lookup reads the newest
// snapshot in which the table WAS versioned, and keeps the typed value for
// the events from after the drop.
func TestSpellIndexPKFilter_versioningDroppedLater_2007(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	for _, row := range []string{
		"(1, NOW(), 'shop', 'prices', 'id', 1, 'PRI', 'int', 'NO', 0)",
		"(1, NOW(), 'shop', 'prices', 'row_start', 2, '', 'timestamp', 'NO', 1)",
		"(1, NOW(), 'shop', 'prices', 'row_end', 3, 'PRI', 'timestamp', 'NO', 1)",
		"(2, NOW(), 'shop', 'prices', 'id', 1, 'PRI', 'int', 'NO', 0)",
	} {
		testutil.MustExec(t, db, "INSERT INTO schema_snapshots (snapshot_id, snapshot_time, schema_name, table_name, "+
			"column_name, ordinal_position, column_key, data_type, is_nullable, is_generated) VALUES "+row)
	}
	opts := query.Options{Schema: "shop", Table: "prices", PKValues: "2"}
	if _, err := SpellIndexPKFilter(context.Background(), db, &opts); err != nil {
		t.Fatal(err)
	}
	if len(opts.PKValuesIn) != 3 || opts.PKValuesIn[0] != "2" {
		t.Fatalf("In=%q, want the typed value plus both versioned spellings", opts.PKValuesIn)
	}
	if opts.PKAliases["2|2106-02-07 06:28:15.999999"] != "2" {
		t.Fatalf("aliases %v must map each added spelling back to the typed value", opts.PKAliases)
	}
}

// A source recorded as MySQL or PostgreSQL has no system versioning: the
// lookup does not even read the snapshot.
func TestSpellIndexPKFilter_mysqlSourceSkips_2007(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, "INSERT INTO schema_snapshots (snapshot_id, snapshot_time, schema_name, table_name, "+
		"column_name, ordinal_position, column_key, data_type, is_nullable, is_generated) VALUES "+
		"(1, NOW(), 'shop', 'prices', 'id', 1, 'PRI', 'int', 'NO', 0), "+
		"(1, NOW(), 'shop', 'prices', 'row_end', 3, 'PRI', 'timestamp', 'NO', 1)")
	testutil.MustExec(t, db, "INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id) VALUES (1, 'position', 'mysql', NOW(), 1)")
	opts := query.Options{Schema: "shop", Table: "prices", PKValues: "2"}
	if _, err := SpellIndexPKFilter(context.Background(), db, &opts); err != nil {
		t.Fatal(err)
	}
	if opts.PKValues != "2" || len(opts.PKValuesIn) != 0 {
		t.Fatalf("mysql source: PKValues=%q In=%q, want the lookup unchanged", opts.PKValues, opts.PKValuesIn)
	}
}

// Versioning added or dropped between the snapshot and the target moment
// splits the row's changes across two key spellings; a single-row lookup by
// one of them would miss the other period silently. Refuse (#2007).
func TestCheckSysVersioningChange_2007(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	since := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)
	ctx := context.Background()

	if err := CheckSysVersioningChange(ctx, db, "shop", "prices", since, until); err != nil {
		t.Fatalf("no DDL, no snapshots: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES (?, 'b.1', 4, 'shop', 'prices', 'ALTER TABLE', 'ALTER TABLE prices ADD SYSTEM VERSIONING')`, since.Add(-time.Hour))
	if err := CheckSysVersioningChange(ctx, db, "shop", "prices", since, until); err != nil {
		t.Fatalf("versioning added BEFORE the window: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES (?, 'b.1', 9, 'shop', 'prices', 'ALTER TABLE', 'alter table prices drop  system versioning')`, since.Add(10*time.Minute))
	err := CheckSysVersioningChange(ctx, db, "shop", "prices", since, until)
	if err == nil || !errors.Is(err, ErrSchemaChanged) || !strings.Contains(err.Error(), "system versioning") {
		t.Fatalf("versioning dropped inside the window: err=%v, want a schema-change refusal", err)
	}

	// No DDL recorded (file mode), but the snapshots on either side differ.
	db2, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db2)
	for _, row := range []string{
		"(1, ?, 'shop', 'prices', 'id', 1, 'PRI', 'int', 'NO', 0)",
		"(2, ?, 'shop', 'prices', 'id', 1, 'PRI', 'int', 'NO', 0)",
		"(2, ?, 'shop', 'prices', 'row_end', 3, 'PRI', 'timestamp', 'NO', 1)",
	} {
		at := since.Add(-time.Hour)
		if strings.HasPrefix(row, "(2") {
			at = since.Add(20 * time.Minute)
		}
		testutil.MustExec(t, db2, "INSERT INTO schema_snapshots (snapshot_id, snapshot_time, schema_name, table_name, "+
			"column_name, ordinal_position, column_key, data_type, is_nullable, is_generated) VALUES "+row, at)
	}
	if err := CheckSysVersioningChange(ctx, db2, "shop", "prices", since, until); err == nil || !errors.Is(err, ErrSchemaChanged) {
		t.Fatalf("snapshot shapes differ across the window: err=%v, want a schema-change refusal", err)
	}
}

// The setup console and MCP share with the CLI (#2007): on a MariaDB index a
// versioned table gets a handle and the typed value (ExpandKey spells it),
// an ordinary one the stored spelling and no handle.
func TestPrepareSingleRowLookup_2007(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, "INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id) VALUES (1, 'gtid', 'mariadb', NOW(), 1)")
	since := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	create := "CREATE TABLE `prices` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB WITH SYSTEM VERSIONING;"
	pk := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"},
		{Name: "row_end", IsPK: true, DataType: "timestamp", IsGenerated: true}}
	h, search, err := PrepareSingleRowLookup(context.Background(), db, "shop", "prices", "2", create, pk,
		SingleRowKeyColumns(&metadata.TableMeta{PKColumns: []string{"id", "row_end"}, Columns: pk}, "mariadb"), since, since.Add(time.Hour))
	if err != nil || h == nil || search != "2" {
		t.Fatalf("versioned: handle=%v search=%q err=%v, want a handle and the typed value", h, search, err)
	}
	h, search, err = PrepareSingleRowLookup(context.Background(), db, "shop", "plain", "2",
		"CREATE TABLE `plain` (\n  `id` int NOT NULL\n) ENGINE=InnoDB;", pk[:1], []string{"id"}, since, since.Add(time.Hour))
	if err != nil || h != nil || search != "2" {
		t.Fatalf("plain: handle=%v search=%q err=%v, want no handle", h, search, err)
	}
}
