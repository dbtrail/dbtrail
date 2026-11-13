//go:build integration

package e2e_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2735 through the real chain (capture, a mydumper snapshot, a full-table
// reconstruct to a mydumper dump), on MariaDB and on MySQL. Each ALTER below
// writes no row event, so the reconstruct carries the snapshot's values
// through it:
//
//   - lossy: utf8mb4 to latin1 under an empty sql_mode, over 'Ñandú ✓'. The
//     source keeps 'Ñandú ?'; the reconstruct must refuse, naming the change.
//   - rewritten: the same ALTER, then the row is updated after it. The value
//     the dump carries is the update's, so it must answer.
//   - lossless: utf8mb4 to latin1 under the strict default, over 'Ñandú €',
//     which latin1 holds; widened: latin1 to utf8mb4. Neither lost a letter,
//     so both must answer, which is what keeps a widening from forcing a full
//     read for nothing.
func TestEndToEnd_MariaDBCharsetNarrowingReconstruct(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	testutil.SkipIfNoMariaDB(t)
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	charsetNarrowingChain(t, sourceDB, sourceName, testutil.MariaDBBaseDSN()+"/"+sourceName, func() []string {
		var gtidPos string
		if err := sourceDB.QueryRow("SELECT @@gtid_binlog_pos").Scan(&gtidPos); err != nil || gtidPos == "" {
			t.Fatalf("read @@gtid_binlog_pos: %q, %v", gtidPos, err)
		}
		return []string{"--start-gtid", gtidPos}
	})
}

func TestEndToEnd_MySQLCharsetNarrowingReconstruct(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	sourceDB, sourceName := testutil.CreateTestDB(t)
	var logBin int
	var format string
	if err := sourceDB.QueryRow("SELECT @@log_bin, @@binlog_format").Scan(&logBin, &format); err != nil || logBin != 1 || format != "ROW" {
		t.Skipf("the test MySQL does not write a ROW binary log (log_bin=%d, binlog_format=%q, %v)", logBin, format, err)
	}
	// Capture starts at the server's current position on its first run.
	charsetNarrowingChain(t, sourceDB, sourceName, testutil.SnapshotDSN(sourceName), func() []string { return nil })
}

func charsetNarrowingChain(t *testing.T, sourceDB *sql.DB, sourceName, sourceDSN string, startArgs func() []string) {
	t.Helper()
	mydumper := requireMydumper(t)
	exec := func(stmts ...string) {
		t.Helper()
		for _, s := range stmts {
			testutil.MustExec(t, sourceDB, s)
		}
	}
	exec(
		"CREATE TABLE lossy (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=utf8mb4",
		"CREATE TABLE rewritten (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=utf8mb4",
		"CREATE TABLE lossless (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=utf8mb4",
		"CREATE TABLE widened (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=latin1",
		"INSERT INTO lossy VALUES (1, 'Ñandú ✓'), (2, 'plain')",
		"INSERT INTO rewritten VALUES (1, 'Ñandú ✓')",
		"INSERT INTO lossless VALUES (1, 'Ñandú €')",
		"INSERT INTO widened VALUES (1, 'Ñandú')",
	)

	binPath, coverDir := chainBinary(t)
	indexDB, indexName := testutil.CreateTestDB(t)
	indexDSN := testutil.SnapshotDSN(indexName)
	tmp := t.TempDir()
	baseDir := filepath.Join(tmp, "baselines")
	run(t, binPath, coverDir, "init", "--index-dsn", indexDSN, "--partitions", "4")
	run(t, binPath, coverDir, "snapshot", "--source-dsn", sourceDSN, "--index-dsn", indexDSN, "--schemas", sourceName)
	stream := startStream(t, binPath, coverDir, append([]string{
		"--index-dsn", indexDSN, "--source-dsn", sourceDSN,
		"--server-id", strconv.Itoa(970000 + int(time.Now().UnixNano()%9999)),
		"--schemas", sourceName, "--checkpoint", "1"}, startArgs()...)...)

	dumpDir := filepath.Join(tmp, "dump1")
	run(t, binPath, coverDir, "dump", "--source-dsn", sourceDSN, "--output-dir", dumpDir,
		"--schemas", sourceName, "--mydumper-path", mydumper)
	run(t, binPath, coverDir, "baseline", "--input", dumpDir, "--output", baseDir)
	secondBoundary()

	// A row change before the ALTERs, so the window has events.
	exec("UPDATE lossy SET name = 'plain2' WHERE id = 2")
	waitIndexed(t, indexDB, sourceName, 1, stream)

	ctx := context.Background()
	conn, err := sourceDB.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	for _, s := range []string{
		"SET SESSION sql_mode = ''",
		"ALTER TABLE lossy MODIFY name VARCHAR(20) CHARACTER SET latin1",
		"ALTER TABLE rewritten MODIFY name VARCHAR(20) CHARACTER SET latin1",
		"SET SESSION sql_mode = DEFAULT",
		"ALTER TABLE lossless CONVERT TO CHARACTER SET latin1",
		"ALTER TABLE widened CONVERT TO CHARACTER SET utf8mb4",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	conn.Close()
	for tbl, want := range map[string]string{"lossy": "Ñandú ?", "rewritten": "Ñandú ?", "lossless": "Ñandú €", "widened": "Ñandú"} {
		var got string
		if err := sourceDB.QueryRow("SELECT CONVERT(name USING utf8mb4) FROM " + tbl + " WHERE id = 1").Scan(&got); err != nil || got != want {
			t.Fatalf("%s after its ALTER: %q, %v; want %q", tbl, got, err, want)
		}
	}
	waitSnapshotCharset(t, indexDB, sourceName, "lossy", "name", "latin1", stream)
	waitSnapshotCharset(t, indexDB, sourceName, "rewritten", "name", "latin1", stream)
	waitSnapshotCharset(t, indexDB, sourceName, "lossless", "name", "latin1", stream)
	waitSnapshotCharset(t, indexDB, sourceName, "widened", "name", "utf8mb4", stream)
	exec("UPDATE rewritten SET name = 'fixed' WHERE id = 1")
	// The ALTERs wrote no row event: two UPDATEs are all the index holds.
	waitIndexed(t, indexDB, sourceName, 2, stream)
	secondBoundary()
	at := time.Now().UTC().Truncate(time.Second).Add(-time.Second).Format(time.RFC3339)
	stream.stop(t)

	reconstruct := func(tbl string) (string, string, error) {
		out := filepath.Join(tmp, "out-"+tbl)
		stdout, stderr, err := runResult(binPath, coverDir, "reconstruct", "--index-dsn", indexDSN,
			"--baseline-dir", baseDir, "--tables", sourceName+"."+tbl, "--at", at,
			"--output-format", "mydumper", "--output", out, "--parallelism", "1")
		return out, stdout + stderr, err
	}

	_, output, err := reconstruct("lossy")
	if err == nil || !strings.Contains(output, "changed column name from utf8mb4 to latin1") || !strings.Contains(output, "replaced with '?'") {
		t.Fatalf("reconstruct of lossy across the narrowing: err=%v, want the refusal naming the change\n%s", err, output)
	}
	if strings.Contains(output, "✓") {
		t.Errorf("the refusal carries the row's value:\n%s", output)
	}
	t.Logf("lossy refused as wanted:\n%s", output)

	for tbl, want := range map[string]string{
		"rewritten": "fixed",
		"lossless":  "Ñandú €",
		// The dump loads under SET NAMES binary into the CREATE TABLE it
		// carries, the snapshot's, which still says latin1 here.
		"widened": "\xd1and\xfa",
	} {
		out, output, err := reconstruct(tbl)
		if err != nil {
			t.Fatalf("reconstruct of %s: %v, want it to answer: no letter was lost\n%s", tbl, err, output)
		}
		data, err := os.ReadFile(filepath.Join(out, sourceName+"."+tbl+".00000.sql"))
		if err != nil {
			t.Fatalf("%s: read the dump: %v\n%s", tbl, err, output)
		}
		if !strings.Contains(string(data), want) {
			t.Errorf("%s: the dump does not hold %q:\n%s", tbl, want, data)
		}
	}
}
