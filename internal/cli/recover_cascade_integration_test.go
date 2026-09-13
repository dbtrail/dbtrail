//go:build integration

package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedCascadeIndex builds a minimal indexed cascade in a fresh index DB: a
// parent DELETE plus two child INSERTs that referenced it, and the fk_constraints
// row marking child.pid -> parent.id ON DELETE CASCADE. Returns (db, dbName, dsn).
func seedCascadeIndex(t *testing.T) (*sql.DB, string, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")

	// child INSERTs (the cascade victims' last indexed state) and the parent
	// DELETE that cascaded them. The cascade child deletes are intentionally
	// NOT inserted — that is the blind spot the command reconstructs.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, childTs, nil,
		dbName, "child", 1 /* INSERT */, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, childTs, nil,
		dbName, "child", 1 /* INSERT */, "11", nil, nil, []byte(`{"id":11,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, parentTs, nil,
		dbName, "parent", 3 /* DELETE */, "1", nil, []byte(`{"id":1}`), nil)

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_child', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`,
		dbName, dbName)
	// Production writes fk_constraints and schema_snapshots in ONE transaction
	// with the same snapshot_id; the FK-snapshot-at-delete selection (#834)
	// reads the snapshot time from schema_snapshots, so the fixture must
	// preserve that invariant (snapshot taken before the delete).
	testutil.InsertSnapshot(t, db, 1, h.Format("2006-01-02 15:04:05"), dbName, "parent", "id", 1, "PRI", "int", "NO")

	return db, dbName, testutil.IntegrationDSN(dbName)
}

func runCascadeCmd(t *testing.T) error {
	t.Helper()
	c := &cobra.Command{}
	c.SetContext(context.Background())
	return runRecoverCascade(c, nil)
}

// TestRecoverCascade_endToEnd drives the command over a seeded index and checks
// the emitted SQL re-inserts the parent and its cascade-deleted children inside
// the FK-checks-off wrapper, with the Phase-1 scope header, and reports complete.
func TestRecoverCascade_endToEnd(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	_, dbName, dsn := seedCascadeIndex(t)
	out := t.TempDir() + "/cascade.sql"

	// Reset command globals to a clean state for this run.
	rcIndexDSN, rcSchema, rcTable = dsn, dbName, "parent"
	rcPK, rcPKs, rcSince, rcUntil = "", nil, "", ""
	rcOutput, rcDryRun, rcFormat = out, false, "text"
	rcLookback, rcMaxDepth, rcLimit, rcAllowIncomplete = "30d", 5, 1000, false

	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	for _, want := range []string{
		"SET FOREIGN_KEY_CHECKS=0;",
		"SET FOREIGN_KEY_CHECKS=1;",
		"Phase-1",
		"`" + dbName + "`.`parent`", // parent re-insert
		"`" + dbName + "`.`child`",  // child re-inserts
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("output missing %q\n---\n%s", want, sql)
		}
	}
	// Both children must be re-inserted.
	if c := strings.Count(sql, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want 2 child INSERTs, got %d\n---\n%s", c, sql)
	}
	// A clean cascade with no archives must NOT be flagged incomplete.
	if strings.Contains(sql, "INCOMPLETE RECOVERY") {
		t.Errorf("clean cascade should not be flagged incomplete\n---\n%s", sql)
	}

	// 0 parent deletes (a --pk that matches nothing), no archives: a legitimately
	// empty result is complete and exits 0 (the operator gets a stderr warning).
	rcPK = "999"
	if err := runCascadeCmd(t); err != nil {
		t.Errorf("0 matched parents with no archives must exit 0, got: %v", err)
	}
}

// TestRecoverCascade_generatedPKChildGatedViaCLIWiring pins the CLI surface's
// PKMetas wiring (#1273): the snapshot marks the child's PK as containing a
// STORED GENERATED member (the MariaDB system-versioning shape), so the
// CLI-built cascade.Options must carry the probe. With it, the edge is
// skipped with the permanent generatedpk caveat and NO child INSERTs are
// synthesized from the indexed events; delete the `PKMetas:` line in the
// CLI's SynthesizeVictims call and both assertions fail (Phase-1 would scan
// and synthesize the two children).
func TestRecoverCascade_generatedPKChildGatedViaCLIWiring(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)
	// seedCascadeIndex snapshots only the parent; give the child the
	// versioned PK shape (id, row_end) with row_end STORED GENERATED.
	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour).Format("2006-01-02 15:04:05")
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, ?, ?, 'child', 'id', 1, 'PRI', 'int', 'NO', 0),
		       (1, ?, ?, 'child', 'pid', 2, '', 'int', 'YES', 0),
		       (1, ?, ?, 'child', 'row_end', 3, 'PRI', 'timestamp', 'NO', 1)`,
		h, dbName, h, dbName, h, dbName)

	out := t.TempDir() + "/cascade.sql"
	cleanCascadeFlags(dsn, dbName, out)
	rcAllowIncomplete = true // caveat still lands in the output banner; exit stays 0

	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade with --allow-incomplete: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	if !strings.Contains(sql, "generated column") || !strings.Contains(sql, "PERMANENT") {
		t.Errorf("output must carry the permanent generated-PK caveat\n---\n%s", sql)
	}
	if c := strings.Count(sql, "INSERT INTO `"+dbName+"`.`child`"); c != 0 {
		t.Errorf("gated child edge must synthesize no INSERTs, got %d\n---\n%s", c, sql)
	}
}

// addArchiveRow makes ResolveArchiveSources return non-empty (no disk
// dependency) by registering an S3-located archived partition.
func addArchiveRow(t *testing.T, db *sql.DB) {
	t.Helper()
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(bintrail_id, partition_name, s3_bucket, s3_key)
		VALUES ('bt', 'p_2026010100', 'bucket', 'bintrail_id=bt/p_2026010100/data.parquet')`)
}

// cleanCascadeFlags sets every recover-cascade global to a valid baseline for an
// integration run, so no stale value leaks in from another test.
func cleanCascadeFlags(dsn, dbName, out string) {
	rcIndexDSN, rcSchema, rcTable = dsn, dbName, "parent"
	rcPK, rcPKs, rcSince, rcUntil = "", nil, "", ""
	rcOutput, rcDryRun, rcFormat = out, false, "text"
	rcLookback, rcMaxDepth, rcLimit, rcAllowIncomplete = "30d", 5, 1000, false
	rcBaselineDir, rcBaselineS3 = "", ""
	rcNoArchive = false
}

// writeArchivedEvents writes rows as ONE Parquet archive file per hour under
// base (a bintrail_id=… directory) and registers each hour in archive_state
// with its local path. Rows of the same hour share a file.
func writeArchivedEvents(t *testing.T, db *sql.DB, base string, rows ...query.ResultRow) {
	t.Helper()
	byHour := map[time.Time][]query.ResultRow{}
	for _, r := range rows {
		h := r.EventTimestamp.UTC().Truncate(time.Hour)
		byHour[h] = append(byHour[h], r)
	}
	for h, rs := range byHour {
		hourDir := filepath.Join(base, "event_date="+h.Format("2006-01-02"), "event_hour="+h.Format("15"))
		if err := os.MkdirAll(hourDir, 0o755); err != nil {
			t.Fatal(err)
		}
		pq := filepath.Join(hourDir, "events.parquet")
		if _, err := buffer.WriteParquet(rs, pq, "none"); err != nil {
			t.Fatalf("WriteParquet: %v", err)
		}
		testutil.MustExec(t, db, `INSERT INTO archive_state
			(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
			VALUES (?, 'bt', ?, ?, NULL, NULL, NULL)`, "p_"+h.Format("2006010215"), pq, len(rs))
	}
}

// archivedChildInsert / archivedParentDelete are the two row shapes the
// archived-cascade tests need (child pid=1; parent id=1).
func archivedChildInsert(dbName, pk string, at time.Time) query.ResultRow {
	id, _ := strconv.ParseInt(pk, 10, 64)
	return query.ResultRow{
		EventID: uint64(900000 + id), BinlogFile: "b.000001", StartPos: 4, EndPos: 40,
		EventTimestamp: at.UTC(), SchemaName: dbName, TableName: "child",
		EventType: 1 /*INSERT*/, PKValues: pk,
		RowAfter: map[string]any{"id": id, "pid": int64(1), "payload": "archived-" + pk},
	}
}

func archivedParentDelete(dbName string, at time.Time) query.ResultRow {
	return query.ResultRow{
		EventID: 800001, BinlogFile: "b.000001", StartPos: 41, EndPos: 80,
		EventTimestamp: at.UTC(), SchemaName: dbName, TableName: "parent",
		EventType: 3 /*DELETE*/, PKValues: "1",
		RowBefore: map[string]any{"id": int64(1)},
	}
}

// writeArchivedChildInsert writes ONE child INSERT (pid=1) as a Parquet
// archive for an hour the live index does NOT hold (#1615: evidence that
// rotated out of the live index).
func writeArchivedChildInsert(t *testing.T, db *sql.DB, dbName, pk string, at time.Time) {
	t.Helper()
	writeArchivedEvents(t, db, filepath.Join(t.TempDir(), "bintrail_id=bt"), archivedChildInsert(dbName, pk, at))
}

// TestRecoverCascade_incompleteExit proves the dangerous "nothing found" case:
// 0 live parents matched BUT the index has archived partitions --no-archive excluded →
// flagged incomplete, exit non-zero unless --allow-incomplete; SQL still written.
func TestRecoverCascade_incompleteExit(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)
	addArchiveRow(t, db)

	out := t.TempDir() + "/cascade.sql"
	cleanCascadeFlags(dsn, dbName, out)
	rcPK = "999"       // matches no live parent → the "nothing found, but archives exist" trap
	rcNoArchive = true // the caveat under test is the one --no-archive keeps (#1615 reads archives otherwise)

	rcAllowIncomplete = false
	err := runCascadeCmd(t)
	if err == nil {
		t.Fatal("expected a non-nil error when incomplete and --allow-incomplete is false")
	}
	if !strings.Contains(err.Error(), "INCOMPLETE") {
		t.Errorf("error should explain incompleteness, got: %v", err)
	}
	b, rerr := os.ReadFile(out)
	if rerr != nil {
		t.Fatalf("output should still be written on incomplete: %v", rerr)
	}
	if !strings.Contains(string(b), "INCOMPLETE RECOVERY") {
		t.Errorf("output should carry the incomplete header")
	}

	rcAllowIncomplete = true
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("--allow-incomplete should exit 0, got: %v", err)
	}
}

// TestRecoverCascade_noArchiveWithArchivesIsFlagged: since #1615 archives are
// READ by default, so --no-archive on an index that has them is a coverage
// decision — the run is flagged incomplete (children whose events were
// archived are not reconstructed) and --allow-incomplete accepts it. Before,
// with archives never read, their mere presence was a warning to avoid
// cry-wolf; that rationale is gone. Also exercises --pk filtering (pk=1).
func TestRecoverCascade_noArchiveWithArchivesIsFlagged(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)
	addArchiveRow(t, db)

	out := t.TempDir() + "/cascade.sql"
	cleanCascadeFlags(dsn, dbName, out)
	rcPK = "1"         // matches the seeded parent delete → parents found
	rcNoArchive = true // the caveat under test is the one --no-archive keeps (#1615 reads archives otherwise)

	if err := runCascadeCmd(t); err == nil {
		t.Fatal("--no-archive over an index with archives is a coverage decision and must exit non-zero without --allow-incomplete")
	}
	b, _ := os.ReadFile(out)
	if !strings.Contains(string(b), "INCOMPLETE RECOVERY") || !strings.Contains(string(b), "--no-archive excluded them") {
		t.Errorf("the output must flag the exclusion\n---\n%s", string(b))
	}
	rcAllowIncomplete = true
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("--allow-incomplete accepts the exclusion, got: %v", err)
	}
}

// TestRecoverCascade_jsonExitParity pins the #568 fix: --format json must honor
// the same exit contract as text — a partial result exits non-zero (the body's
// `complete:false` is on stdout, but consumers gating on exit code must see it).
func TestRecoverCascade_jsonExitParity(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)
	addArchiveRow(t, db)

	out := t.TempDir() + "/cascade.sql"
	cleanCascadeFlags(dsn, dbName, out)
	rcFormat = "json"
	rcPK = "999"       // 0 parents + archives → incomplete
	rcNoArchive = true // the caveat under test is the one --no-archive keeps (#1615 reads archives otherwise)

	rcAllowIncomplete = false
	if err := runCascadeCmd(t); err == nil {
		t.Fatal("JSON mode must exit non-zero when incomplete, like text mode (#568)")
	}
	// --allow-incomplete suppresses the coverage-gap exit in JSON mode too.
	rcAllowIncomplete = true
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("JSON + --allow-incomplete should exit 0, got: %v", err)
	}
}

// writeChildBaseline writes a real Parquet snapshot of the `child` table at
// parquetPath using DuckDB (the same engine ReadBaselineRows queries with).
func writeChildBaseline(t *testing.T, parquetPath string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(parquetPath), 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer d.Close()
	// child 10,11 reference parent 1 (cascade victims); 12 references parent 2.
	// created_at is a TIMESTAMP so the test also verifies a DuckDB time.Time
	// non-PK value renders as a MySQL DATETIME literal in the recovery SQL.
	q := fmt.Sprintf(
		`COPY (SELECT * FROM (VALUES `+
			`(10,1,'keep10',TIMESTAMP '2026-05-01 12:00:00'),`+
			`(11,1,'keep11',TIMESTAMP '2026-05-02 13:00:00'),`+
			`(12,2,'other',TIMESTAMP '2026-05-03 14:00:00')`+
			`) AS t(id,pid,payload,created_at)) TO '%s' (FORMAT PARQUET)`,
		strings.ReplaceAll(parquetPath, "'", "''"))
	if _, err := d.Exec(q); err != nil {
		t.Fatalf("write baseline parquet: %v", err)
	}
}

// TestRecoverCascade_phase2BaselineRecoversUntouchedChild proves Phase-2
// end-to-end through the real provider: children that exist ONLY in a baseline
// snapshot (no binlog event — the gap Phase-1 misses) are recovered, scoped to
// the deleted parent, with the run reported complete.
func TestRecoverCascade_phase2BaselineRecoversUntouchedChild(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Schema snapshot so the resolver knows child PK (id) + columns.
	snapTs := "2026-06-01 00:00:00"
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "payload", 3, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "created_at", 4, "", "datetime", "YES")

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`, dbName, dbName)

	// Parent DELETE in the binlog; the children have NO binlog events — they
	// live only in the baseline (the Phase-1 blind spot).
	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	parentTs := h.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, parentTs, nil, dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	// Baseline snapshot dated before the parent delete.
	baselineDir := t.TempDir()
	// Dated inside the live hour: a snapshot older than the oldest live partition
	// is a gapped window since #1615 (hours the index never held are not trusted).
	snapDir := filepath.Join(baselineDir, h.Add(1*time.Minute).Format("2006-01-02T15-04-05Z"))
	writeChildBaseline(t, filepath.Join(snapDir, dbName, "child.parquet"))
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("write success marker: %v", err)
	}

	out := filepath.Join(t.TempDir(), "cascade.sql")
	cleanCascadeFlags(testutil.IntegrationDSN(dbName), dbName, out)
	rcPK = "1"
	rcBaselineDir = baselineDir
	defer func() { rcBaselineDir = "" }()

	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	for _, want := range []string{
		"keep10", "keep11", // children recovered from the baseline
		"'2026-05-01 12:00:00", // DuckDB time.Time non-PK value → MySQL DATETIME literal
		"Phase-2 baseline fallback ACTIVE",
		"`" + dbName + "`.`parent`", // parent restored
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("output missing %q\n---\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "other") {
		t.Errorf("child 12 (pid=2, different parent) must NOT be recovered\n---\n%s", sql)
	}
	if strings.Contains(sql, "INCOMPLETE RECOVERY") {
		t.Errorf("a baseline-covered cascade must be complete\n---\n%s", sql)
	}
}

// TestRecoverCascade_phase2StaleBaselineWarnsExitZero pins the #618 CORRECTION
// end to end: when Phase-2 falls back to an older baseline snapshot because
// the child table is absent from the newest one, the command must (a) still
// print the SQL, (b) still recover the untouched baseline children, (c) print
// the advisory under the distinct "NOTE — advisory" banner rather than
// "!!! INCOMPLETE RECOVERY", and — the concrete, load-bearing assertion — (d)
// EXIT ZERO without --allow-incomplete. A review of the original #618 PR found
// it had wired this through Result.Incomplete, which made recover-cascade
// exit non-zero on every run against a deployment whose baseline had gone
// stale, even though the issue's own analysis says no data is missing.
func TestRecoverCascade_phase2StaleBaselineWarnsExitZero(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	snapTs := "2026-06-01 00:00:00"
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "payload", 3, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "created_at", 4, "", "datetime", "YES")

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`, dbName, dbName)

	// Parent DELETE in the binlog; the children have NO binlog events — they
	// live only in the baseline (Phase-1 blind spot, forces real Phase-2
	// augmentation so the gate at cascade.go's `baseCovered && len(baseRows) >
	// 0` is satisfied and the stale warning has a chance to fire).
	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	parentTs := h.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, parentTs, nil, dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	// TWO baseline generations, both inside the live hour and before the parent
	// delete above: the older one has shop.child, the newer one does NOT —
	// this is exactly the #466/#618 stale-fallback trigger.
	baselineDir := t.TempDir()
	olderDir := filepath.Join(baselineDir, h.Add(1*time.Minute).Format("2006-01-02T15-04-05Z")) // inside the live hour (#1615)
	writeChildBaseline(t, filepath.Join(olderDir, dbName, "child.parquet"))
	if err := baseline.WriteSuccessMarker(olderDir); err != nil {
		t.Fatalf("write success marker (older): %v", err)
	}
	newerDir := filepath.Join(baselineDir, h.Add(2*time.Minute).Format("2006-01-02T15-04-05Z"))
	if err := os.MkdirAll(newerDir, 0o755); err != nil {
		t.Fatalf("mkdir newer snapshot dir: %v", err)
	}
	if err := baseline.WriteSuccessMarker(newerDir); err != nil {
		t.Fatalf("write success marker (newer): %v", err)
	}

	out := filepath.Join(t.TempDir(), "cascade.sql")
	cleanCascadeFlags(testutil.IntegrationDSN(dbName), dbName, out)
	rcPK = "1"
	rcBaselineDir = baselineDir
	defer func() { rcBaselineDir = "" }()

	// The concrete assertion: exit code 0, WITHOUT --allow-incomplete.
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("a stale-but-complete baseline recovery must exit 0 without --allow-incomplete, got: %v", err)
	}

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	for _, want := range []string{
		"keep10", "keep11", // baseline children still recovered
		"NOTE — advisory, recovery below is COMPLETE",
		"is stale", // reconstruct.staleFallback's message text
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("output missing %q\n---\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "INCOMPLETE RECOVERY") {
		t.Errorf("a stale-but-complete baseline fallback must NOT render the INCOMPLETE banner\n---\n%s", sql)
	}
}

// writeCompositeChildBaseline writes a `child` snapshot with a composite PK (a,b).
func writeCompositeChildBaseline(t *testing.T, parquetPath string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(parquetPath), 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer d.Close()
	q := fmt.Sprintf(
		`COPY (SELECT * FROM (VALUES (1,2,1,'c12')) AS t(a,b,pid,payload)) TO '%s' (FORMAT PARQUET)`,
		strings.ReplaceAll(parquetPath, "'", "''"))
	if _, err := d.Exec(q); err != nil {
		t.Fatalf("write composite baseline parquet: %v", err)
	}
}

// TestRecoverCascade_phase2DedupCompositePK pins the load-bearing dedup contract:
// a child present in BOTH the binlog (touched, still referencing the parent) AND
// the baseline must be emitted EXACTLY ONCE. If the provider's composite-PK
// encoding (CanonicalizePKMap + BuildPKValues) diverged by one byte from the
// indexer's pk_values, the dedup would miss and the recovery SQL would
// double-INSERT the PK (which FOREIGN_KEY_CHECKS=0 does not suppress).
func TestRecoverCascade_phase2DedupCompositePK(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	snapTs := "2026-06-01 00:00:00"
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "a", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "b", 2, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 3, "", "int", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "payload", 4, "", "varchar", "YES")

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`, dbName, dbName)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")
	// child (a=1,b=2) is TOUCHED in the binlog (still referencing parent 1)...
	testutil.InsertEvent(t, db, "b.000001", 10, 20, childTs, nil, dbName, "child", 1 /*INSERT*/, "1|2", nil, nil, []byte(`{"a":1,"b":2,"pid":1,"payload":"c12"}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, parentTs, nil, dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	// ...and ALSO present in the baseline (same composite PK).
	baselineDir := t.TempDir()
	snapDir := filepath.Join(baselineDir, h.Add(1*time.Minute).Format("2006-01-02T15-04-05Z")) // inside the live hour (#1615)
	writeCompositeChildBaseline(t, filepath.Join(snapDir, dbName, "child.parquet"))
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("success marker: %v", err)
	}

	out := filepath.Join(t.TempDir(), "cascade.sql")
	cleanCascadeFlags(testutil.IntegrationDSN(dbName), dbName, out)
	rcPK = "1"
	rcBaselineDir = baselineDir
	defer func() { rcBaselineDir = "" }()

	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	if c := strings.Count(sql, "`"+dbName+"`.`child`"); c != 1 {
		t.Errorf("composite child present in binlog AND baseline must dedup to ONE INSERT, got %d\n---\n%s", c, sql)
	}
}

// TestRecoverCascade_setNullEmitsGuardedUpdate proves ON DELETE SET NULL recovery
// end-to-end: a child that referenced the deleted parent (and survives with its
// FK nulled) is restored by an idempotent UPDATE carrying the `... AND fk IS NULL`
// guard — so a re-run or a later re-point of the child is never clobbered.
func TestRecoverCascade_setNullEmitsGuardedUpdate(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	snapTs := "2026-06-01 00:00:00"
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'SET NULL', 'RESTRICT')`, dbName, dbName)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, childTs, nil, dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, parentTs, nil, dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	out := filepath.Join(t.TempDir(), "cascade.sql")
	cleanCascadeFlags(testutil.IntegrationDSN(dbName), dbName, out)
	rcPK = "1"

	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	// The restore is a guarded UPDATE, not an INSERT (the child row survives).
	wantUpdate := "UPDATE `" + dbName + "`.`child` SET `pid` = 1 WHERE `id` = 10 AND `pid` IS NULL"
	if !strings.Contains(sql, wantUpdate) {
		t.Errorf("missing guarded SET NULL restore %q\n---\n%s", wantUpdate, sql)
	}
	if strings.Contains(sql, "INSERT INTO `"+dbName+"`.`child`") {
		t.Errorf("SET NULL child must be UPDATEd, not re-INSERTed\n---\n%s", sql)
	}
}

// TestRecoverCascade_archivesOutsideWindowKeepBaseline pins the CLI wiring of
// the #1615 gate end to end: an archived partition exists (months away), the
// baseline snapshot sits INSIDE the hour the live index still holds, and the
// parent was deleted later in that same hour. The live window [snapshot, T]
// is contiguous, so augmentation must run and the untouched children come
// back from the baseline with the run reported complete. Before the fix the
// bare existence of the archive skipped augmentation: parent only, INCOMPLETE.
func TestRecoverCascade_archivesOutsideWindowKeepBaseline(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	snapTs := "2026-06-01 00:00:00"
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "payload", 3, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "created_at", 4, "", "datetime", "YES")
	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`, dbName, dbName)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	parentTs := h.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, parentTs, nil, dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)
	writeArchivedChildInsert(t, db, dbName, "77", time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)) // readable, nowhere near the window

	// Snapshot dated inside the live hour, before the parent delete.
	baselineDir := t.TempDir()
	snapDir := filepath.Join(baselineDir, h.Add(5*time.Minute).Format("2006-01-02T15-04-05Z"))
	writeChildBaseline(t, filepath.Join(snapDir, dbName, "child.parquet"))
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("write success marker: %v", err)
	}

	out := filepath.Join(t.TempDir(), "cascade.sql")
	cleanCascadeFlags(testutil.IntegrationDSN(dbName), dbName, out)
	rcPK = "1"
	rcBaselineDir = baselineDir
	defer func() { rcBaselineDir = "" }()
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	for _, want := range []string{"keep10", "keep11", "Phase-2 baseline fallback ACTIVE"} {
		if !strings.Contains(sql, want) {
			t.Errorf("output missing %q (the archive gate must not fire on a live-contiguous window)\n---\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "INCOMPLETE RECOVERY") || strings.Contains(sql, "skipped baseline augmentation") {
		t.Errorf("a live-contiguous window must be complete\n---\n%s", sql)
	}
	if strings.Contains(sql, "archived-77") {
		t.Errorf("child 77 was archived months before this parent's window and must not be recovered\n---\n%s", sql)
	}
}

// TestRecoverCascade_baselineOlderThanIndexSkipsAugmentation pins the behavior
// change through the real probe on the CLI surface: a snapshot dated before the
// oldest live partition is a window the index never held, so augmentation is
// skipped and flagged — whether or not an archive is registered.
func TestRecoverCascade_baselineOlderThanIndexSkipsAugmentation(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	snapTs := "2026-06-01 00:00:00"
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "payload", 3, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "created_at", 4, "", "datetime", "YES")
	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`, dbName, dbName)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	parentTs := h.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, parentTs, nil, dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)
	// No archive_state row at all: the probe, not the existence flag, decides.

	baselineDir := t.TempDir()
	snapDir := filepath.Join(baselineDir, "2026-06-01T00-00-00Z") // months before the only live hour
	writeChildBaseline(t, filepath.Join(snapDir, dbName, "child.parquet"))
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("write success marker: %v", err)
	}

	out := filepath.Join(t.TempDir(), "cascade.sql")
	cleanCascadeFlags(testutil.IntegrationDSN(dbName), dbName, out)
	rcPK = "1"
	rcBaselineDir = baselineDir
	rcAllowIncomplete = true
	defer func() { rcBaselineDir = ""; rcAllowIncomplete = false }()
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	if strings.Contains(sql, "keep10") || strings.Contains(sql, "keep11") {
		t.Errorf("hours the index never held must not be trusted: baseline children must NOT be emitted\n---\n%s", sql)
	}
	for _, want := range []string{"INCOMPLETE RECOVERY", "cannot serve every hour"} {
		if !strings.Contains(sql, want) {
			t.Errorf("output missing %q\n---\n%s", want, sql)
		}
	}
}

// TestRecoverCascade_childOnlyInArchiveRecovered is the second half of #1615
// end to end on the CLI: the child's INSERT rotated out of the live index into
// a Parquet archive (no live partition holds its hour), the parent DELETE is
// live, no baseline. The cascade scan reads the archive and the child is
// recovered, complete. Before this the scan was live-only: parent only.
func TestRecoverCascade_childOnlyInArchiveRecovered(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	snapTs := "2026-06-01 00:00:00"
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "payload", 3, "", "varchar", "YES")
	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`, dbName, dbName)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h}) // only the parent's hour is live
	parentTs := h.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, parentTs, nil, dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)
	writeArchivedChildInsert(t, db, dbName, "10", h.Add(-3*time.Hour+10*time.Minute)) // rotated out, archived

	out := filepath.Join(t.TempDir(), "cascade.sql")
	cleanCascadeFlags(testutil.IntegrationDSN(dbName), dbName, out)
	rcPK = "1"
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	if !strings.Contains(sql, "archived-10") {
		t.Errorf("the archived child INSERT must be found by the merged scan\n---\n%s", sql)
	}
	if strings.Contains(sql, "INCOMPLETE RECOVERY") {
		t.Errorf("an archive-covered scan is complete\n---\n%s", sql)
	}

	// --no-archive: same index, live scan only → the child is invisible and
	// the run is flagged: archives exist and were excluded.
	rcNoArchive = true
	rcAllowIncomplete = true
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade --no-archive --allow-incomplete: %v", err)
	}
	b, _ = os.ReadFile(out)
	if strings.Contains(string(b), "archived-10") || !strings.Contains(string(b), "INCOMPLETE RECOVERY") {
		t.Errorf("--no-archive must not read the archive and must flag the exclusion\n---\n%s", string(b))
	}
}

// TestRecoverCascade_unreadableArchiveRefuses: an archive_state row this host
// cannot read (an S3 key with no credentials) makes the merged scan partial;
// like `recover`, the command refuses rather than emit a script missing part
// of the evidence, and names the escape hatch.
func TestRecoverCascade_unreadableArchiveRefuses(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)
	addArchiveRow(t, db) // s3://bucket/bintrail_id=bt — unreadable here
	out := t.TempDir() + "/cascade.sql"
	cleanCascadeFlags(dsn, dbName, out)
	rcPK = "1"
	err := runCascadeCmd(t)
	if err == nil {
		t.Fatal("an unreadable archive source must refuse, not silently narrow the scan")
	}
	for _, want := range []string{"could not be read", "pass --no-archive"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should contain %q, got: %v", want, err)
		}
	}
	// The hint is for THAT error only: a plain connection failure gets none.
	if hint := archiveReadHint(errors.New("dial tcp: connection refused")); hint != "" {
		t.Errorf("an unrelated error must not carry the --no-archive hint: %q", hint)
	}
}

// TestRecoverCascade_parentAndChildOnlyInArchiveRecovered is the booth case
// that motivated #1615: a parent deleted days ago, its DELETE and its
// children's INSERTs all rotated out to Parquet, nothing of it left in the
// live index. Both scans read the archives: parent found, child recovered.
func TestRecoverCascade_parentAndChildOnlyInArchiveRecovered(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	snapTs := "2026-06-01 00:00:00"
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "payload", 3, "", "varchar", "YES")
	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`, dbName, dbName)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h}) // live hour holds nothing relevant
	base := filepath.Join(t.TempDir(), "bintrail_id=bt")
	writeArchivedEvents(t, db, base,
		archivedChildInsert(dbName, "10", h.Add(-5*time.Hour+10*time.Minute)),
		archivedParentDelete(dbName, h.Add(-4*time.Hour+30*time.Minute)))

	out := filepath.Join(t.TempDir(), "cascade.sql")
	cleanCascadeFlags(testutil.IntegrationDSN(dbName), dbName, out)
	rcPK = "1"
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	for _, want := range []string{"`" + dbName + "`.`parent`", "archived-10"} {
		if !strings.Contains(sql, want) {
			t.Errorf("output missing %q: the archived parent and its archived child must both be found\n---\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "INCOMPLETE RECOVERY") {
		t.Errorf("an archive-covered scan is complete\n---\n%s", sql)
	}
}
