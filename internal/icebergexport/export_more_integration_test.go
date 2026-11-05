//go:build integration

package icebergexport

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// insertSnapshotTyped is testutil.InsertSnapshot plus COLUMN_TYPE, which the
// ENUM label mapper and the type-drift check read.
func insertSnapshotTyped(t *testing.T, db *sql.DB, snapshotID int, at time.Time, schema, table, column string,
	ordinal int, key, dataType, columnType, nullable string) {
	t.Helper()
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, column_type)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshotID, at.UTC().Format("2006-01-02 15:04:05"), schema, table, column, ordinal, key, dataType, nullable, columnType)
}

func writeBaseline(t *testing.T, root string, at time.Time, schema, table, createSQL string, rows [][]string, meta map[string]string) {
	t.Helper()
	snapDir := filepath.Join(root, strings.ReplaceAll(at.UTC().Format(time.RFC3339), ":", "-"))
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	md := map[string]string{
		baseline.MetaKeyCreateTableSQL: createSQL,
		"bintrail.snapshot_timestamp":  at.UTC().Format(time.RFC3339),
	}
	for k, v := range meta {
		md[k] = v
	}
	w, err := baseline.NewWriter(filepath.Join(snapDir, schema, table+".parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100, Metadata: md,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		nulls := make([]bool, len(r))
		if err := w.WriteRow(r, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
}

// TestIntegrationExport_enumAndTextDecodedPerEpoch: ENUM ordinals become the
// labels of the snapshot in effect at each event, and base64 TEXT becomes
// text. Delete the DecodePage call and this reads "2" and "Ynll".
func TestIntegrationExport_enumAndTextDecodedPerEpoch(t *testing.T) {
	f := seedFixture(t)
	const createSQL = "CREATE TABLE `notes` (\n  `id` int NOT NULL,\n  `status` enum('new','paid','shipped') DEFAULT NULL,\n  `note` text,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "notes", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "notes", "status", 2, "", "enum", "enum('new','paid','shipped')", "YES")
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "notes", "note", 3, "", "text", "text", "YES")
	writeBaseline(t, f.baseDir, f.base, f.schema, "notes", createSQL,
		[][]string{{"1", "new", "hello"}, {"2", "new", "hello"}},
		map[string]string{baseline.MetaKeyBinlogFile: "binlog.000001", baseline.MetaKeyBinlogPos: "100"})
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	// Under snapshot 1, ordinal 2 is "paid".
	testutil.InsertEvent(t, f.db, "binlog.000001", 100, 200, f.base.Add(5*time.Minute).Format("2006-01-02 15:04:05"), nil,
		f.schema, "notes", 2, "1", nil,
		[]byte(fmt.Sprintf(`{"id":1,"status":1,"note":"%s"}`, b64("hello"))),
		[]byte(fmt.Sprintf(`{"id":1,"status":2,"note":"%s"}`, b64("bye"))))
	// Snapshot 2 reorders the labels: ordinal 1 is now "paid".
	at2 := f.base.Add(10 * time.Minute)
	insertSnapshotTyped(t, f.db, 2, at2, f.schema, "notes", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 2, at2, f.schema, "notes", "status", 2, "", "enum", "enum('paid','new','shipped')", "YES")
	insertSnapshotTyped(t, f.db, 2, at2, f.schema, "notes", "note", 3, "", "text", "text", "YES")
	testutil.InsertEvent(t, f.db, "binlog.000001", 200, 300, f.base.Add(15*time.Minute).Format("2006-01-02 15:04:05"), nil,
		f.schema, "notes", 2, "2", nil,
		[]byte(fmt.Sprintf(`{"id":2,"status":2,"note":"%s"}`, b64("hello"))),
		[]byte(fmt.Sprintf(`{"id":2,"status":1,"note":"%s"}`, b64("later"))))

	cfg := f.config(t.TempDir(), f.base.Add(20*time.Minute))
	cfg.Tables = []string{f.schema + ".notes"}
	o := runOne(t, cfg)
	if o.Verdict != VerdictLoaded || o.Events != 2 {
		t.Fatalf("run = %+v", o)
	}
	ddb := openDuckDBIceberg(t)
	rows, err := ddb.Query(fmt.Sprintf("SELECT id, status, note FROM iceberg_scan('%s') ORDER BY id", o.Location))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id int
		var status, note string
		if err := rows.Scan(&id, &status, &note); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%d=%s/%s", id, status, note))
	}
	want := "1=paid/bye,2=paid/later"
	if strings.Join(got, ",") != want {
		t.Fatalf("rows = %v, want %s (labels per epoch, text decoded)", got, want)
	}
}

// TestIntegrationExport_untypedEpochRefuses: an event whose epoch (the
// snapshot in effect at its timestamp) does not describe the table cannot be
// decoded; with a TEXT column in play the table refuses instead of guessing.
func TestIntegrationExport_untypedEpochRefuses(t *testing.T) {
	f := seedFixture(t)
	const createSQL = "CREATE TABLE `notes` (\n  `id` int NOT NULL,\n  `note` text,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	// Snapshot 1 (base) describes notes; snapshot 2 (base+10m) does NOT (it
	// only carries orders, seeded by the fixture under id 1, so re-seed it);
	// snapshot 3 (base+18m) describes notes again, so the LATEST resolver
	// finds the table and the run reaches the fold.
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "notes", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "notes", "note", 2, "", "text", "text", "YES")
	insertSnapshotTyped(t, f.db, 2, f.base.Add(10*time.Minute), f.schema, "orders", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 3, f.base.Add(18*time.Minute), f.schema, "notes", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 3, f.base.Add(18*time.Minute), f.schema, "notes", "note", 2, "", "text", "text", "YES")
	writeBaseline(t, f.baseDir, f.base, f.schema, "notes", createSQL, [][]string{{"1", "hello"}},
		map[string]string{baseline.MetaKeyBinlogFile: "binlog.000001", baseline.MetaKeyBinlogPos: "100"})
	// The event falls under snapshot 2, which has no notes table.
	testutil.InsertEvent(t, f.db, "binlog.000001", 100, 200, f.base.Add(15*time.Minute).Format("2006-01-02 15:04:05"), nil,
		f.schema, "notes", 2, "1", nil, []byte(`{"id":1,"note":"aGVsbG8="}`), []byte(`{"id":1,"note":"Ynll"}`))
	cfg := f.config(t.TempDir(), f.base.Add(20*time.Minute))
	cfg.Tables = []string{f.schema + ".notes"}
	o := runOne(t, cfg)
	if o.Verdict != VerdictRefused || !strings.Contains(o.Detail, "could not be decoded") {
		t.Fatalf("verdict = %s (%s), want a refusal about the undecodable epoch", o.Verdict, o.Detail)
	}
}

// TestIntegrationExport_pageBoundarySameSecond: two events on one key in the
// same second, one per page. Rebuild the fold per page and the DELETE wins
// only if it happens to come last; the key must be gone either way.
func TestIntegrationExport_pageBoundarySameSecond(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	ts := f.base.Add(25 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, f.db, "binlog.000001", 400, 500, ts, nil, f.schema, "orders", 2, "4", nil,
		[]byte(`{"id":4,"status":"new","amount":40,"updated_at":"2026-08-28 13:00:00"}`),
		[]byte(`{"id":4,"status":"shipped","amount":40,"updated_at":"2026-08-28 13:00:00"}`))
	testutil.InsertEvent(t, f.db, "binlog.000001", 500, 600, ts, nil, f.schema, "orders", 3, "4", nil,
		[]byte(`{"id":4,"status":"shipped","amount":40,"updated_at":"2026-08-28 13:00:00"}`), nil)
	cfg := f.config(t.TempDir(), f.base.Add(40*time.Minute))
	cfg.FetchBatchSize = 1
	o := runOne(t, cfg)
	if o.Verdict != VerdictLoaded || o.Events != 5 || o.Deletes != 2 {
		t.Fatalf("run = %+v (%s), want 5 events with 2 deletes (3 and 4)", o, o.Detail)
	}
	ddb := openDuckDBIceberg(t)
	equalRows(t, "page boundary", duckRows(t, ddb, o.Location), []string{"1=new", "2=paid"})
}

func TestIntegrationExport_baselineWithoutAnchorRefuses(t *testing.T) {
	f := seedFixture(t)
	const createSQL = "CREATE TABLE `plain` (\n  `id` int NOT NULL,\n  `v` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "plain", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "plain", "v", 2, "", "varchar", "varchar(10)", "YES")
	writeBaseline(t, f.baseDir, f.base, f.schema, "plain", createSQL, [][]string{{"1", "a"}}, nil)
	cfg := f.config(t.TempDir(), f.base.Add(20*time.Minute))
	cfg.Tables = []string{f.schema + ".plain"}
	o := runOne(t, cfg)
	if o.Verdict != VerdictRefused || !strings.Contains(o.Detail, "carries no binlog position") {
		t.Fatalf("verdict = %s (%s)", o.Verdict, o.Detail)
	}
}

func TestIntegrationExport_atNotAfterCursorRefuses(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	warehouse := t.TempDir()
	at := f.base.Add(20 * time.Minute)
	if o := runOne(t, f.config(warehouse, at)); o.Verdict != VerdictLoaded {
		t.Fatalf("run 1 = %s (%s)", o.Verdict, o.Detail)
	}
	f.seedSecondWindow(t)
	o := runOne(t, f.config(warehouse, at))
	if o.Verdict != VerdictRefused || !strings.Contains(o.Detail, "not after the table's cursor") {
		t.Fatalf("verdict = %s (%s), want the forward-only refusal", o.Verdict, o.Detail)
	}
}

// TestIntegrationExport_cutBeforeCursorRefuses: the index no longer holds
// events past the cursor and its newest one is BEFORE it (a reset source, a
// restored index). The window would be empty; the cursor must not move.
func TestIntegrationExport_cutBeforeCursorRefuses(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	warehouse := t.TempDir()
	o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute)))
	if o.Verdict != VerdictLoaded {
		t.Fatalf("run 1 = %s (%s)", o.Verdict, o.Detail)
	}
	testutil.MustExec(t, f.db, `DELETE FROM binlog_events WHERE start_pos >= 300`)
	// Capture has read far past the cursor (#2621): that excuses a cut behind
	// a BASELINE anchor, never one behind a cursor a run committed, which
	// names an event the index held and no longer does.
	testutil.MustExec(t, f.db, `INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, gtid_set, events_indexed, last_checkpoint, server_id)
		VALUES (1, 'position', 'binlog.000009', 4, '', 3, NOW(), 1)`)
	o2 := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
	if o2.Verdict != VerdictRefusedGap || !strings.Contains(o2.Detail, "is before") {
		t.Errorf("verdict = %s (%s), want refused-gap naming the cut before the cursor", o2.Verdict, o2.Detail)
	}
	// A refusal carries no Cursor, so the table itself is the witness. Errorf
	// above so that a run that did not refuse also shows WHERE the cursor went.
	if got := storedCursor(t, warehouse, f.schema, "orders"); got != o.Cursor {
		t.Fatalf("cursor moved: %s -> %s", o.Cursor, got)
	}
}

// storedCursor reads the cursor the Iceberg table carries in its properties,
// the way the next run will.
func storedCursor(t *testing.T, warehouse, schema, tbl string) string {
	t.Helper()
	cat, release, err := openWarehouse(context.Background(), warehouse)
	if err != nil {
		t.Fatalf("open warehouse: %v", err)
	}
	defer release()
	icetbl, found, err := loadTable(context.Background(), cat, catalog.ToIdentifier(schema, tbl))
	if err != nil || !found {
		t.Fatalf("load %s.%s: found=%v err=%v", schema, tbl, found, err)
	}
	cur, err := readCursor(icetbl.Properties())
	if err != nil || cur == nil {
		t.Fatalf("read cursor of %s.%s: %+v (%v)", schema, tbl, cur, err)
	}
	return cur.String()
}

func TestIntegrationExport_emptyIndexWithCursorRefuses(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	warehouse := t.TempDir()
	if o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute))); o.Verdict != VerdictLoaded {
		t.Fatalf("run 1 = %s (%s)", o.Verdict, o.Detail)
	}
	testutil.MustExec(t, f.db, `DELETE FROM binlog_events`)
	o := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
	// The table folded deltas once, so events existed and are gone: refused,
	// without asserting whether they rotated out or were reset.
	if o.Verdict != VerdictRefused || !strings.Contains(o.Detail, "no live events") {
		t.Fatalf("verdict = %s (%s), want refused for an index with no history", o.Verdict, o.Detail)
	}
}

// TestIntegrationExport_freshInstallLoadsThenWaits: baseline taken, stream
// not yet indexing anything. The load is the answer; an empty index is not
// lost history, and the run must not exit 1 telling the operator it is.
func TestIntegrationExport_freshInstallLoadsThenWaits(t *testing.T) {
	f := seedFixture(t)
	const createSQL = "CREATE TABLE `plain` (\n  `id` int NOT NULL,\n  `v` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "plain", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "plain", "v", 2, "", "varchar", "varchar(10)", "YES")
	writeBaseline(t, f.baseDir, f.base, f.schema, "plain", createSQL, [][]string{{"1", "a"}},
		map[string]string{baseline.MetaKeyBinlogFile: "binlog.000001", baseline.MetaKeyBinlogPos: "100"})
	cfg := f.config(t.TempDir(), f.base.Add(20*time.Minute))
	cfg.Tables = []string{f.schema + ".plain"}
	o := runOne(t, cfg)
	if o.Verdict != VerdictLoaded || o.RowsLoaded != 1 || !strings.Contains(o.Detail, "no live events yet") {
		t.Fatalf("verdict = %s rows=%d (%s), want loaded with the nothing-to-fold note", o.Verdict, o.RowsLoaded, o.Detail)
	}
	if got := storedCursor(t, cfg.Warehouse, f.schema, "plain"); got != o.Cursor || !strings.HasPrefix(got, "binlog.000001:100 ") {
		t.Fatalf("stored cursor = %s, outcome cursor = %s, want the snapshot anchor", got, o.Cursor)
	}
}

// TestIntegrationExport_atAtSnapshotInstantIsALoad: --at equal to the
// snapshot's own timestamp asks for the table as of the dump. That is the
// load; the forward-only refusal is for a RE-RUN.
func TestIntegrationExport_atAtSnapshotInstantIsALoad(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	o := runOne(t, f.config(t.TempDir(), f.base))
	if o.Verdict != VerdictLoaded || o.Events != 0 || !strings.Contains(o.Detail, "snapshot's instant") {
		t.Fatalf("verdict = %s events=%d (%s), want a plain load", o.Verdict, o.Events, o.Detail)
	}
}

func TestIntegrationExport_typeOnlyAlterRefuses(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	warehouse := t.TempDir()
	if o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute))); o.Verdict != VerdictLoaded {
		t.Fatalf("run 1 = %s (%s)", o.Verdict, o.Detail)
	}
	// Same names, one type widened: decimal(10,2) -> decimal(12,4).
	at2 := f.base.Add(22 * time.Minute)
	insertSnapshotTyped(t, f.db, 2, at2, f.schema, "orders", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 2, at2, f.schema, "orders", "status", 2, "", "varchar", "varchar(20)", "YES")
	insertSnapshotTyped(t, f.db, 2, at2, f.schema, "orders", "amount", 3, "", "decimal", "decimal(12,4)", "YES")
	insertSnapshotTyped(t, f.db, 2, at2, f.schema, "orders", "updated_at", 4, "", "datetime", "datetime", "YES")
	f.seedSecondWindow(t)
	o := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
	if o.Verdict != VerdictRefusedDDL || !strings.Contains(o.Detail, "decimal(12,4)") {
		t.Fatalf("verdict = %s (%s), want refused-ddl naming the widened type", o.Verdict, o.Detail)
	}
}

func TestIntegrationExport_captureSkipsRefuse(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	warehouse := t.TempDir()
	if o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute))); o.Verdict != VerdictLoaded {
		t.Fatalf("run 1 = %s (%s)", o.Verdict, o.Detail)
	}
	f.seedSecondWindow(t)
	skips := fmt.Sprintf(`{"column_count_mismatch":{"count":3,"last_at":%q,"tables":["%s.orders"]}}`,
		f.base.Add(26*time.Minute).UTC().Format(time.RFC3339), f.schema)
	testutil.MustExec(t, f.db, `INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, gtid_set, events_indexed, last_checkpoint, server_id, capture_skips)
		VALUES (1, 'position', 'binlog.000001', 700, '', 6, NOW(), 1, ?)`, skips)
	o := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
	if o.Verdict != VerdictRefusedGap || !strings.Contains(o.Detail, "skipped 3 event(s)") {
		t.Fatalf("verdict = %s (%s), want refused-gap naming the skipped events", o.Verdict, o.Detail)
	}
	// The tally keeps ONE timestamp per reason. A skip recorded AFTER --at
	// cannot be told apart from earlier skips of the same reason inside the
	// window, so it refuses too; treating it as clean would commit and move
	// the cursor past a window the index provably does not hold in full.
	skips = fmt.Sprintf(`{"column_count_mismatch":{"count":8,"last_at":%q,"tables":["%s.orders"]}}`,
		f.base.Add(50*time.Minute).UTC().Format(time.RFC3339), f.schema)
	testutil.MustExec(t, f.db, `UPDATE stream_state SET capture_skips = ? WHERE id = 1`, skips)
	o = runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
	if o.Verdict != VerdictRefusedGap || !strings.Contains(o.Detail, "skipped 8 event(s)") {
		t.Fatalf("skip after --at: verdict = %s (%s), want refused-gap", o.Verdict, o.Detail)
	}
	if got := storedCursor(t, warehouse, f.schema, "orders"); !strings.HasPrefix(got, "binlog.000001:400 ") {
		t.Fatalf("cursor = %s, want it left at the run-1 cut", got)
	}
}

// TestIntegrationExport_twoTablesOneCut: the cut is index-wide. A table with
// no events still moves its cursor to the cut (a properties-only commit, no
// snapshot) and reports unchanged, so the next run starts where this one
// looked.
func TestIntegrationExport_twoTablesOneCut(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	const createSQL = "CREATE TABLE `customers` (\n  `id` int NOT NULL,\n  `name` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "customers", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "customers", "name", 2, "", "varchar", "varchar(20)", "YES")
	writeBaseline(t, f.baseDir, f.base, f.schema, "customers", createSQL, [][]string{{"1", "ann"}},
		map[string]string{baseline.MetaKeyBinlogFile: "binlog.000001", baseline.MetaKeyBinlogPos: "100"})
	cfg := f.config(t.TempDir(), f.base.Add(20*time.Minute))
	cfg.Tables = []string{f.schema + ".orders", f.schema + ".customers"}
	outs, err := Run(context.Background(), cfg)
	if err != nil || len(outs) != 2 {
		t.Fatalf("run: %v %+v", err, outs)
	}
	if outs[0].Verdict != VerdictLoaded || outs[1].Verdict != VerdictLoaded {
		t.Fatalf("first run = %+v", outs)
	}
	f.seedSecondWindow(t)
	cfg.At = f.base.Add(40 * time.Minute)
	outs, err = Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if outs[0].Verdict != VerdictExported || outs[1].Verdict != VerdictUnchanged {
		t.Fatalf("second run = %+v", outs)
	}
	if outs[0].Cursor != outs[1].Cursor {
		t.Fatalf("cursors differ after one run: orders %s, customers %s (the cut is index-wide)", outs[0].Cursor, outs[1].Cursor)
	}
	ddb := openDuckDBIceberg(t)
	var n int
	if err := ddb.QueryRow(fmt.Sprintf("SELECT count(*) FROM iceberg_snapshots('%s')", outs[1].Location)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("customers snapshots = %d, want 1 (the load only; an empty window adds none)", n)
	}
}

// TestIntegrationExport_cutBetweenEvents: with --at between two events, the
// cut is the START of the first event past --at, so that event is excluded
// now and included next run. Nothing folded twice, nothing missed.
func TestIntegrationExport_cutBetweenEvents(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	f.seedSecondWindow(t) // 25m, 26m, 27m
	warehouse := t.TempDir()
	o := runOne(t, f.config(warehouse, f.base.Add(26*time.Minute+30*time.Second)))
	if o.Verdict != VerdictLoaded || o.Events != 5 {
		t.Fatalf("run 1 = %+v (%s), want 5 events (first window + 25m + 26m)", o, o.Detail)
	}
	if !strings.HasPrefix(o.Cursor, "binlog.000001:600 ") {
		t.Fatalf("cursor = %s, want the START of the 27m event (600)", o.Cursor)
	}
	o2 := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
	if o2.Verdict != VerdictExported || o2.Events != 1 || o2.Deletes != 1 {
		t.Fatalf("run 2 = %+v (%s), want exactly the 27m DELETE", o2, o2.Detail)
	}
	ddb := openDuckDBIceberg(t)
	equalRows(t, "after both runs", duckRows(t, ddb, o2.Location), []string{"2=paid", "4=shipped", "5=new"})
}

func TestIntegrationExport_noServersTableRefusesRun(t *testing.T) {
	f := seedFixture(t)
	testutil.MustExec(t, f.db, `DROP TABLE bintrail_servers`)
	_, err := Run(context.Background(), f.config(t.TempDir(), f.base.Add(20*time.Minute)))
	if err == nil || !strings.Contains(err.Error(), "no bintrail_servers table") {
		t.Fatalf("err = %v, want the missing-registry refusal", err)
	}
}

// TestIntegrationExport_anchorGapIsReported: a baseline anchored BEFORE the
// oldest event the index holds cannot prove coverage of the span between
// (#781). Like reconstruct, the export proceeds and says so in the outcome.
func TestIntegrationExport_anchorGapIsReported(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	const createSQL = "CREATE TABLE `early` (\n  `id` int NOT NULL,\n  `v` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "early", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "early", "v", 2, "", "varchar", "varchar(10)", "YES")
	writeBaseline(t, f.baseDir, f.base, f.schema, "early", createSQL, [][]string{{"1", "a"}},
		map[string]string{baseline.MetaKeyBinlogFile: "binlog.000001", baseline.MetaKeyBinlogPos: "4"})
	testutil.InsertEvent(t, f.db, "binlog.000001", 700, 800, f.base.Add(12*time.Minute).Format("2006-01-02 15:04:05"), nil,
		f.schema, "early", 2, "1", nil, []byte(`{"id":1,"v":"a"}`), []byte(`{"id":1,"v":"b"}`))
	cfg := f.config(t.TempDir(), f.base.Add(20*time.Minute))
	cfg.Tables = []string{f.schema + ".early"}
	o := runOne(t, cfg)
	if o.Verdict != VerdictLoaded || !strings.Contains(o.Detail, "unproven") {
		t.Fatalf("outcome = %s (%s), want loaded with the coverage-unproven note", o.Verdict, o.Detail)
	}
}
