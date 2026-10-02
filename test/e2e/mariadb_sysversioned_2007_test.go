//go:build integration

package e2e_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2007: a MariaDB system-versioned table through the real chain. Capture,
// a mydumper snapshot, then refreshes, must hold the table's CURRENT rows,
// equal to a plain SELECT on the source; a full-table reconstruct at a past
// moment must equal SELECT ... FOR SYSTEM_TIME AS OF that moment; and the
// reversal SQL must, applied to the source, bring the current rows back.
//
// Covered shapes: hidden period columns (prices), declared INVISIBLE period
// columns (exp), PARTITION BY SYSTEM_TIME (parted), a plain table beside them
// (plain), and versioning added by ALTER to a table with data (altv). Inside
// the windows: several UPDATEs of one row, DELETE then re-INSERT of one id,
// INSERT then DELETE, and DELETE HISTORY.
func TestEndToEnd_MariaDBSystemVersionedRefresh(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	testutil.SkipIfNoMariaDB(t)
	mydumper := requireMydumper(t)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	// No column is named v: textRows reads a column of that name as a VECTOR.
	tables := []chainTable{
		{name: "prices", cols: []string{"id", "sku", "price"}, pk: []string{"id"}},
		{name: "exp", cols: []string{"id", "n"}, pk: []string{"id"}},
		{name: "parted", cols: []string{"id", "n"}, pk: []string{"id"}},
		{name: "plain", cols: []string{"id", "n"}, pk: []string{"id"}},
		{name: "altv", cols: []string{"id", "n"}, pk: []string{"id"}},
	}
	byName := map[string]chainTable{}
	for _, ct := range tables {
		byName[ct.name] = ct
	}
	exec := func(stmts ...string) {
		t.Helper()
		for _, s := range stmts {
			testutil.MustExec(t, sourceDB, s)
		}
	}
	exec(
		"CREATE TABLE prices (id INT PRIMARY KEY, sku VARCHAR(20), price DECIMAL(10,2)) WITH SYSTEM VERSIONING",
		"CREATE TABLE exp (id INT PRIMARY KEY, n INT, rs TIMESTAMP(6) GENERATED ALWAYS AS ROW START INVISIBLE, "+
			"re TIMESTAMP(6) GENERATED ALWAYS AS ROW END INVISIBLE, PERIOD FOR SYSTEM_TIME(rs, re)) WITH SYSTEM VERSIONING",
		"CREATE TABLE parted (id INT PRIMARY KEY, n INT) WITH SYSTEM VERSIONING "+
			"PARTITION BY SYSTEM_TIME (PARTITION p_hist HISTORY, PARTITION p_cur CURRENT)",
		"CREATE TABLE plain (id INT PRIMARY KEY, n INT)",
		"CREATE TABLE altv (id INT PRIMARY KEY, n INT)",
		"INSERT INTO prices VALUES (1,'a',1.00),(2,'b',2.00),(3,'c',3.00)",
		"INSERT INTO exp (id, n) VALUES (1,1),(2,2),(3,3)",
		"INSERT INTO parted VALUES (1,1),(2,2),(3,3)",
		"INSERT INTO plain VALUES (1,1),(2,2)",
		"INSERT INTO altv VALUES (1,1),(2,2)",
		// History from before the snapshot: mydumper must leave it out.
		"UPDATE prices SET price = 1.10 WHERE id = 1",
	)

	binPath, coverDir := chainBinary(t)
	indexDB, indexName := testutil.CreateTestDB(t)
	indexDSN := testutil.SnapshotDSN(indexName)
	sourceDSN := testutil.MariaDBBaseDSN() + "/" + sourceName
	tmp := t.TempDir()
	baseDir := filepath.Join(tmp, "baselines")
	run(t, binPath, coverDir, "init", "--index-dsn", indexDSN, "--partitions", "4")
	run(t, binPath, coverDir, "snapshot", "--source-dsn", sourceDSN, "--index-dsn", indexDSN, "--schemas", sourceName)

	var gtidPos string
	if err := sourceDB.QueryRow("SELECT @@gtid_binlog_pos").Scan(&gtidPos); err != nil || gtidPos == "" {
		t.Fatalf("read @@gtid_binlog_pos: %q, %v", gtidPos, err)
	}
	stream := startStream(t, binPath, coverDir,
		"--index-dsn", indexDSN, "--source-dsn", sourceDSN, "--start-gtid", gtidPos,
		"--server-id", strconv.Itoa(980000+int(time.Now().UnixNano()%9999)),
		"--schemas", sourceName, "--checkpoint", "1")

	dump := func(dir string) {
		t.Helper()
		run(t, binPath, coverDir, "dump", "--source-dsn", sourceDSN, "--output-dir", dir,
			"--schemas", sourceName, "--mydumper-path", mydumper)
		run(t, binPath, coverDir, "baseline", "--input", dir, "--output", baseDir)
	}
	dump(filepath.Join(tmp, "dump1"))
	secondBoundary()

	// Event count of a statement, for waitIndexed: on a versioned table an
	// UPDATE logs the update plus the kept old version (2), a DELETE logs
	// one UPDATE that ends the row (1), an INSERT one row.
	want := 0
	dml := func(n int, stmts ...string) {
		t.Helper()
		exec(stmts...)
		want += n
	}
	nowAt := func() string {
		secondBoundary()
		return time.Now().UTC().Truncate(time.Second).Add(-time.Second).Format("2006-01-02 15:04:05")
	}
	state := func() map[string][]string {
		out := map[string][]string{}
		for _, ct := range tables {
			out[ct.name] = ct.textRows(t, sourceDB, sourceName)
		}
		return out
	}

	// ── Window 1a ────────────────────────────────────────────────────────────
	dml(2, "UPDATE prices SET price = 1.20 WHERE id = 1")
	dml(2, "UPDATE prices SET price = 1.30 WHERE id = 1")
	dml(2, "UPDATE prices SET sku = 'bb' WHERE id = 2")
	dml(1, "DELETE FROM prices WHERE id = 3")
	dml(1, "INSERT INTO prices VALUES (3, 'c2', 3.30)")
	dml(2, "UPDATE exp SET n = 10 WHERE id = 1")
	dml(1, "DELETE FROM parted WHERE id = 3")
	dml(1, "UPDATE plain SET n = 9 WHERE id = 1")
	waitIndexed(t, indexDB, sourceName, want, stream)
	atMid := nowAt()

	// ── Window 1b ────────────────────────────────────────────────────────────
	dml(1, "INSERT INTO prices VALUES (4, 'd', 4.00)")
	dml(1, "DELETE FROM prices WHERE id = 4")
	dml(1, "DELETE FROM exp WHERE id = 2")
	dml(1, "INSERT INTO exp (id, n) VALUES (5, 5)")
	dml(2, "UPDATE parted SET n = 20 WHERE id = 2")
	dml(1, "INSERT INTO parted VALUES (6, 6)")
	waitIndexed(t, indexDB, sourceName, want, stream)
	at1 := nowAt()
	state1 := state()

	// A past moment, rebuilt from the mydumper snapshot plus window 1a, must
	// equal what the source itself keeps for that moment.
	pitDir := filepath.Join(tmp, "pit")
	var versioned []string
	for _, n := range []string{"prices", "exp", "parted"} {
		versioned = append(versioned, sourceName+"."+n)
	}
	run(t, binPath, coverDir, "reconstruct", "--index-dsn", indexDSN, "--baseline-dir", baseDir,
		"--tables", strings.Join(versioned, ","), "--at", atMid, "--output-format", "parquet", "--output", pitDir)
	for _, n := range []string{"prices", "exp", "parted"} {
		ct := byName[n]
		got := parquetTableRows(t, pitDir, sourceName, ct)
		wantRows := ct.asOfRows(t, sourceDB, sourceName, atMid)
		if len(wantRows) == 0 || strings.Join(got, "\n") != strings.Join(wantRows, "\n") {
			t.Errorf("%s reconstructed at %s differs from FOR SYSTEM_TIME AS OF\n got:\n  %s\nwant:\n  %s",
				n, atMid, strings.Join(got, "\n  "), strings.Join(wantRows, "\n  "))
		}
	}

	// Single-row reconstruct looks the row up under the stored spellings
	// and reads its events for the current row (#2007): several UPDATEs
	// (id 1), and DELETE then re-INSERT (id 3).
	for _, id := range []string{"1", "3"} {
		out := run(t, binPath, coverDir, "reconstruct", "--index-dsn", indexDSN, "--baseline-dir", baseDir,
			"--schema", sourceName, "--table", "prices", "--pk", id, "--pk-columns", "id", "--at", at1)
		assertReconstructedRow(t, "prices id="+id+" at T1", out, byName["prices"].sourceRow(t, sourceDB, sourceName, "id = "+id))
	}
	// Naming the period column in the key is refused with the reason.
	if out, errOut, err := runResult(binPath, coverDir, "reconstruct", "--index-dsn", indexDSN, "--baseline-dir", baseDir,
		"--schema", sourceName, "--table", "prices", "--pk", "1|x", "--pk-columns", "id,row_end", "--at", at1); err == nil ||
		!strings.Contains(out+errOut, "declared primary key") {
		t.Errorf("single-row reconstruct keyed with row_end: err=%v, want the refusal\n%s\n%s", err, out, errOut)
	}

	// ── Refresh 1: every table rewritten in full ─────────────────────────────
	run(t, binPath, coverDir, "baseline", "refresh", "--index-dsn", indexDSN, "--baseline-dir", baseDir,
		"--at", at1, "--table-deltas=false")
	assertSnapshotState(t, binPath, coverDir, baseDir, tmp, "refresh 1", sourceName, tables, state1)

	// ── Window 2, then refresh 2 as a delta on top of refresh 1 ──────────────
	dml(2, "UPDATE prices SET price = 9.99 WHERE id = 2")
	dml(1, "DELETE FROM prices WHERE id = 1")
	dml(1, "INSERT INTO prices VALUES (7, 'g', 7.00)")
	dml(2, "UPDATE exp SET n = 11 WHERE id = 1")
	dml(1, "DELETE FROM exp WHERE id = 3")
	dml(2, "UPDATE parted SET n = 21 WHERE id = 1")
	var history int
	if err := sourceDB.QueryRow("SELECT COUNT(*) FROM prices FOR SYSTEM_TIME ALL WHERE row_end < NOW(6)").Scan(&history); err != nil || history == 0 {
		t.Fatalf("count prices history rows: %d, %v", history, err)
	}
	dml(history, "DELETE HISTORY FROM prices")
	waitIndexed(t, indexDB, sourceName, want, stream)
	at2 := nowAt()
	state2 := state()
	run(t, binPath, coverDir, "baseline", "refresh", "--index-dsn", indexDSN, "--baseline-dir", baseDir, "--at", at2)
	if pairs, _ := filepath.Glob(filepath.Join(baseDir, "*", sourceName, "prices*.upserts")); len(pairs) == 0 {
		t.Fatal("refresh 2 wrote no delta pair for prices, so the delta chain was not exercised")
	}
	assertSnapshotState(t, binPath, coverDir, baseDir, tmp, "refresh 2", sourceName, tables, state2)

	// ── Versioning added by ALTER after the snapshot: one refusal a full
	// snapshot cures, then refreshes work. ─────────────────────────────────────
	exec("ALTER TABLE altv ADD SYSTEM VERSIONING")
	dml(2, "UPDATE altv SET n = 22 WHERE id = 2")
	waitIndexed(t, indexDB, sourceName, want, stream)
	at3 := nowAt()
	out, errOut, err := runResult(binPath, coverDir, "baseline", "refresh", "--index-dsn", indexDSN,
		"--baseline-dir", baseDir, "--at", at3, "--tables", sourceName+".altv")
	if err == nil || !strings.Contains(out+errOut, "new full snapshot") {
		t.Fatalf("refresh of a table versioned after its snapshot: err=%v, want the curable refusal\n%s\n%s", err, out, errOut)
	}
	// Single-row reconstruct of altv across the ALTER: its changes sit
	// under two key spellings, so it refuses rather than miss half.
	if out, errOut, err := runResult(binPath, coverDir, "reconstruct", "--index-dsn", indexDSN, "--baseline-dir", baseDir,
		"--schema", sourceName, "--table", "altv", "--pk", "2", "--pk-columns", "id", "--at", at3); err == nil ||
		!strings.Contains(out+errOut, "system versioning") {
		t.Errorf("single-row reconstruct across ADD SYSTEM VERSIONING: err=%v, want a refusal\n%s\n%s", err, out, errOut)
	}
	dump(filepath.Join(tmp, "dump2"))
	secondBoundary()
	dml(2, "UPDATE altv SET n = 33 WHERE id = 1")
	dml(1, "DELETE FROM altv WHERE id = 2")
	dml(2, "UPDATE prices SET sku = 'gg' WHERE id = 7")
	waitIndexed(t, indexDB, sourceName, want, stream)
	at4 := nowAt()
	state4 := state()

	stream.stop(t)
	run(t, binPath, coverDir, "baseline", "refresh", "--index-dsn", indexDSN, "--baseline-dir", baseDir, "--at", at4)
	assertSnapshotState(t, binPath, coverDir, baseDir, tmp, "refresh after the ALTER and a full snapshot", sourceName, tables, state4)

	// ── Reversal SQL for window 2, applied to the source, brings back the
	// current rows window 2 changed. ─────────────────────────────────────────
	since, err := time.Parse("2006-01-02 15:04:05", at1)
	if err != nil {
		t.Fatal(err)
	}
	sinceW2 := since.Add(time.Second).Format("2006-01-02 15:04:05")
	applyDB, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/"+sourceName+"?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer applyDB.Close()

	// By key: id 1 was deleted in window 2. `query --pk 1` and `recover
	// --pk 1` type the declared key and must find it (#2007), and the
	// script must bring the row back.
	if out := run(t, binPath, coverDir, "query", "--index-dsn", indexDSN, "--schema", sourceName, "--table", "prices",
		"--pk", "1", "--format", "json"); !strings.Contains(out, "event_id") {
		t.Errorf("query --pk 1 on a versioned table found no events:\n%s", out)
	}
	pkSQL := filepath.Join(tmp, "recover-pk.sql")
	run(t, binPath, coverDir, "recover", "--index-dsn", indexDSN, "--schema", sourceName, "--table", "prices",
		"--pk", "1", "--since", sinceW2, "--until", at2, "--output", pkSQL)
	pkScript, err := os.ReadFile(pkSQL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyDB.Exec(string(pkScript)); err != nil {
		t.Fatalf("apply recover --pk 1: %v\n%s", err, pkScript)
	}
	want1 := state1["prices"][0]
	if got := byName["prices"].textRows(t, sourceDB, sourceName); len(got) == 0 || got[0] != want1 {
		t.Fatalf("recover --pk 1 did not bring the deleted row back: got %q, want first row %q\n%s", got, want1, pkScript)
	}
	exec("DELETE FROM prices WHERE id = 1") // back to the state the window-wide reversal below starts from
	recSQL := filepath.Join(tmp, "recover.sql")
	run(t, binPath, coverDir, "recover", "--index-dsn", indexDSN, "--schema", sourceName,
		"--since", sinceW2, "--until", at2, "--output", recSQL)
	script, err := os.ReadFile(recSQL)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(script), "skipped, system-versioned table") {
		t.Errorf("the reversal script skipped no history event, so the versioned path was not exercised:\n%s", script)
	}
	if _, err := applyDB.Exec(string(script)); err != nil {
		t.Fatalf("apply the reversal script: %v\n%s", err, script)
	}
	// Window 3 touched only altv and prices id 7, which window 2 inserted:
	// its reversal removes the row whatever window 3 did to it.
	for _, n := range []string{"prices", "exp", "parted", "plain"} {
		got := byName[n].textRows(t, sourceDB, sourceName)
		if strings.Join(got, "\n") != strings.Join(state1[n], "\n") {
			t.Errorf("%s after applying the reversal of window 2 differs from its state before it\n got:\n  %s\nwant:\n  %s\nscript:\n%s",
				n, strings.Join(got, "\n  "), strings.Join(state1[n], "\n  "), script)
		}
	}
}

// asOfRows is textRows over FOR SYSTEM_TIME AS OF the END of the second at
// (UTC): captured events carry whole-second times, so a reconstruct at a
// second includes every change made during it.
func (ct chainTable) asOfRows(t *testing.T, db *sql.DB, schema, at string) []string {
	t.Helper()
	parts := make([]string, len(ct.cols))
	for i, c := range ct.cols {
		parts[i] = fmt.Sprintf("IFNULL(CAST(`%s` AS CHAR),'NULL')", c)
	}
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), "SET time_zone = '+00:00'"); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf("SELECT CONCAT_WS(',', %s) FROM `%s`.`%s` FOR SYSTEM_TIME AS OF CAST(? AS DATETIME(6)) + INTERVAL 999999 MICROSECOND ORDER BY %s",
		strings.Join(parts, ", "), schema, ct.name, "`"+strings.Join(ct.pk, "`,`")+"`")
	rows, err := conn.QueryContext(t.Context(), q, at)
	if err != nil {
		t.Fatalf("AS OF capture of %s: %v", ct.name, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

// parquetTableRows reads <dir>/<snapshot>/<schema>/<table>.parquet with every
// column cast to text, in the same shape as textRows.
func parquetTableRows(t *testing.T, dir, schema string, ct chainTable) []string {
	t.Helper()
	// <dir>/current/ mirrors the newest snapshot; read the timestamped one.
	all, _ := filepath.Glob(filepath.Join(dir, "*", schema, ct.name+".parquet"))
	var files []string
	for _, f := range all {
		if !strings.Contains(f, string(filepath.Separator)+"current"+string(filepath.Separator)) {
			files = append(files, f)
		}
	}
	if len(files) != 1 {
		t.Fatalf("want one %s.parquet under %s, found %v", ct.name, dir, files)
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	parts := make([]string, len(ct.cols))
	for i, c := range ct.cols {
		parts[i] = fmt.Sprintf(`coalesce(CAST("%s" AS VARCHAR), 'NULL')`, c)
	}
	q := fmt.Sprintf(`SELECT concat_ws(',', %s) FROM read_parquet('%s') ORDER BY "%s"`,
		strings.Join(parts, ", "), files[0], strings.Join(ct.pk, `", "`))
	rows, err := ddb.Query(q)
	if err != nil {
		t.Fatalf("read %s: %v", files[0], err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

// assertSnapshotState compares the newest snapshot (through the DuckDB state
// views, which follow its delta chain) with the source's current rows.
func assertSnapshotState(t *testing.T, binPath, coverDir, baseDir, tmp, label, schema string, tables []chainTable, want map[string][]string) {
	t.Helper()
	viewsFile := filepath.Join(tmp, "views-"+strings.ReplaceAll(label, " ", "-")+".sql")
	run(t, binPath, coverDir, "views", "--baseline-dir", baseDir, "--output", viewsFile)
	for _, ct := range tables {
		got := viewTextRows(t, viewsFile, schema, ct)
		if len(want[ct.name]) == 0 || strings.Join(got, "\n") != strings.Join(want[ct.name], "\n") {
			t.Errorf("%s: snapshot of %s differs from the source's current rows\n got:\n  %s\nwant:\n  %s",
				label, ct.name, strings.Join(got, "\n  "), strings.Join(want[ct.name], "\n  "))
		}
	}
}

// viewTextRows reads a table's state view with every column cast to text.
func viewTextRows(t *testing.T, viewsFile, schema string, ct chainTable) []string {
	t.Helper()
	sqlText, err := os.ReadFile(viewsFile)
	if err != nil {
		t.Fatal(err)
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	if _, err := ddb.Exec(string(sqlText)); err != nil {
		t.Fatalf("run views.sql: %v", err)
	}
	parts := make([]string, len(ct.cols))
	for i, c := range ct.cols {
		parts[i] = fmt.Sprintf(`coalesce(CAST("%s" AS VARCHAR), 'NULL')`, c)
	}
	q := fmt.Sprintf(`SELECT concat_ws(',', %s) FROM "state_%s_%s" ORDER BY "%s"`,
		strings.Join(parts, ", "), schema, ct.name, strings.Join(ct.pk, `", "`))
	rows, err := ddb.Query(q)
	if err != nil {
		t.Fatalf("read the state view of %s: %v", ct.name, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}
