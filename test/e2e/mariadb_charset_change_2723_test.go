//go:build integration

package e2e_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2723 through the real chain on MariaDB (capture, a mydumper snapshot,
// refresh): a column's character set changed by an ALTER that is not
// ALTER IGNORE, so the refusal must come from comparing the carried CREATE
// TABLE with the schema snapshot, not from reading the statement (#2640).
//
//   - narrowed: utf8mb4 to latin1 with an empty sql_mode. MariaDB turns the ✓ into
//     '?' and writes no row event for it.
//   - widened: CONVERT TO CHARACTER SET utf8mb4 on a latin1 table. Values are
//     kept; the carried CREATE TABLE would still say latin1.
//
// A table with mixed character sets and no ALTER still refreshes and matches
// the source, which pins that the character sets MariaDB prints and the ones
// its information_schema reports are read as equal.
func TestEndToEnd_MariaDBCharsetChangeRefusesRefresh(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	testutil.SkipIfNoMariaDB(t)
	mydumper := requireMydumper(t)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	exec := func(stmts ...string) {
		t.Helper()
		for _, s := range stmts {
			testutil.MustExec(t, sourceDB, s)
		}
	}
	exec(
		"CREATE TABLE narrowed (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=utf8mb4",
		"CREATE TABLE widened (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=latin1",
		"CREATE TABLE mixed (id INT PRIMARY KEY, a VARCHAR(10), b VARCHAR(10) CHARACTER SET latin1, "+
			"c VARCHAR(10) COLLATE utf8mb4_bin, d VARCHAR(10) CHARACTER SET utf8mb3, "+
			"e ENUM('x','y') CHARACTER SET latin1, n INT) DEFAULT CHARSET=utf8mb4",
		"INSERT INTO narrowed VALUES (1, 'Ñandú ✓'), (2, 'plain')",
		"INSERT INTO widened VALUES (1, 'Ñandú'), (2, 'plain')",
		"INSERT INTO mixed VALUES (1, 'a', 'b', 'c', 'd', 'x', 1), (2, 'a2', 'b2', 'c2', 'd2', 'y', 2)",
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

	dumpDir := filepath.Join(tmp, "dump1")
	run(t, binPath, coverDir, "dump", "--source-dsn", sourceDSN, "--output-dir", dumpDir,
		"--schemas", sourceName, "--mydumper-path", mydumper)
	run(t, binPath, coverDir, "baseline", "--input", dumpDir, "--output", baseDir)
	secondBoundary()

	// One row change on each table, so each has events after the snapshot.
	exec("UPDATE narrowed SET name = 'plain2' WHERE id = 2",
		"UPDATE widened SET name = 'plain2' WHERE id = 2",
		"UPDATE mixed SET n = 9 WHERE id = 1")
	waitIndexed(t, indexDB, sourceName, 3, stream)

	// sql_mode is per session: run both ALTERs on one connection.
	ctx := context.Background()
	conn, err := sourceDB.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	for _, s := range []string{
		"SET SESSION sql_mode = ''",
		"ALTER TABLE narrowed MODIFY name VARCHAR(20) CHARACTER SET latin1",
		"ALTER TABLE widened CONVERT TO CHARACTER SET utf8mb4",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	conn.Close()
	var after string
	if err := sourceDB.QueryRow("SELECT CONVERT(name USING utf8mb4) FROM narrowed WHERE id = 1").Scan(&after); err != nil || after != "Ñandú ?" {
		t.Fatalf("narrowed after the ALTER: %q, %v; want the ✓ replaced by '?'", after, err)
	}
	waitSnapshotCharset(t, indexDB, sourceName, "widened", "name", "utf8mb4", stream)
	waitSnapshotCharset(t, indexDB, sourceName, "narrowed", "name", "latin1", stream)
	// The conversion wrote no row event: the index still holds the three
	// UPDATEs alone.
	var events int
	if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ?", sourceName).Scan(&events); err != nil || events != 3 {
		t.Fatalf("events indexed for %s after the ALTERs: %d, %v; want 3", sourceName, events, err)
	}
	secondBoundary()
	at := time.Now().UTC().Truncate(time.Second).Add(-time.Second).Format("2006-01-02 15:04:05")
	stream.stop(t)

	for tbl, want := range map[string]string{
		"narrowed": "name (varchar(20) CHARACTER SET utf8mb4 -> varchar(20) CHARACTER SET latin1)",
		"widened":  "name (varchar(20) CHARACTER SET latin1 -> varchar(20) CHARACTER SET utf8mb4)",
	} {
		out, errOut, err := runResult(binPath, coverDir, "baseline", "refresh", "--index-dsn", indexDSN,
			"--baseline-dir", baseDir, "--at", at, "--tables", sourceName+"."+tbl)
		if err == nil || !strings.Contains(out+errOut, want) || !strings.Contains(out+errOut, "changed shape since its baseline") {
			t.Fatalf("refresh of %s after its character set changed: err=%v, want the schema-change refusal naming %q\n%s\n%s",
				tbl, err, want, out, errOut)
		}
		t.Logf("refresh of %s refused as wanted:\n%s%s", tbl, out, errOut)
	}

	run(t, binPath, coverDir, "baseline", "refresh", "--index-dsn", indexDSN,
		"--baseline-dir", baseDir, "--at", at, "--tables", sourceName+".mixed")
	mixed := chainTable{name: "mixed", cols: []string{"id", "a", "b", "c", "d", "e", "n"}, pk: []string{"id"}}
	assertSnapshotState(t, binPath, coverDir, baseDir, tmp, "refresh of mixed character sets", sourceName,
		[]chainTable{mixed}, map[string][]string{"mixed": mixed.textRows(t, sourceDB, sourceName)})
}

// waitSnapshotCharset waits until a schema snapshot records column's character
// set as charset: capture snapshots the schema when it reaches a DDL, and the
// refresh compares with that snapshot.
func waitSnapshotCharset(t *testing.T, indexDB *sql.DB, schema, table, column, charset string, p *streamProc) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if p.cmd.ProcessState != nil {
			t.Fatalf("bintrail stream exited early\n%s", p.log.String())
		}
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM schema_snapshots WHERE schema_name = ? AND table_name = ? AND column_name = ? AND character_set_name = ?",
			schema, table, column, charset).Scan(&n); err == nil && n > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no schema snapshot of %s.%s.%s with character set %s after 45s\n%s", schema, table, column, charset, p.log.String())
}
