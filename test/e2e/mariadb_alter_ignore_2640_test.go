//go:build integration

package e2e_test

import (
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2640: MariaDB's ALTER IGNORE TABLE ... ADD UNIQUE keeps the first row of
// each duplicate and deletes the others, and writes the statement to the
// binlog with no row event for the deletes. Through the real chain (capture,
// a mydumper snapshot, refresh): the deletes must really be missing from the
// index, the statement must reach schema_changes, and the refresh of that
// table must refuse instead of publishing a copy that keeps the duplicates.
// An ALTER IGNORE that adds a plain index, on another table, still refreshes.
func TestEndToEnd_MariaDBAlterIgnoreUniqueRefusesRefresh(t *testing.T) {
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
		"CREATE TABLE dups (id INT PRIMARY KEY, email VARCHAR(40))",
		"CREATE TABLE plain (id INT PRIMARY KEY, n INT)",
		"INSERT INTO dups VALUES (1,'a@x'),(2,'a@x'),(3,'b@x'),(4,'b@x'),(5,'c@x')",
		"INSERT INTO plain VALUES (1,1),(2,2)",
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
		"--server-id", strconv.Itoa(970000+int(time.Now().UnixNano()%9999)),
		"--schemas", sourceName, "--checkpoint", "1")

	dumpDir := filepath.Join(tmp, "dump1")
	run(t, binPath, coverDir, "dump", "--source-dsn", sourceDSN, "--output-dir", dumpDir,
		"--schemas", sourceName, "--mydumper-path", mydumper)
	run(t, binPath, coverDir, "baseline", "--input", dumpDir, "--output", baseDir)
	secondBoundary()

	// One row change on each table, so both have events after the snapshot.
	exec("UPDATE dups SET email = 'd@x' WHERE id = 5", "UPDATE plain SET n = 9 WHERE id = 1")
	waitIndexed(t, indexDB, sourceName, 2, stream)

	exec("ALTER IGNORE TABLE dups ADD UNIQUE KEY uk_email (email)",
		"ALTER IGNORE TABLE plain ADD INDEX i_n (n)")
	var left int
	if err := sourceDB.QueryRow("SELECT COUNT(*) FROM dups").Scan(&left); err != nil || left != 3 {
		t.Fatalf("rows left in dups after ALTER IGNORE ... ADD UNIQUE: %d, %v; want 3 (the duplicates of a@x and b@x deleted)", left, err)
	}
	waitSchemaChange(t, indexDB, sourceName, "plain", stream)
	// The two deletes wrote no row event: the index still holds the two
	// UPDATEs alone. Read after the second ALTER is indexed, so the first
	// one's binlog events have all been read.
	var events int
	if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ?", sourceName).Scan(&events); err != nil || events != 2 {
		t.Fatalf("events indexed for %s after the ALTER IGNORE: %d, %v; want 2, the deletes having no row events", sourceName, events, err)
	}
	var recorded string
	if err := indexDB.QueryRow("SELECT ddl_query FROM schema_changes WHERE schema_name = ? AND table_name = 'dups' AND ddl_type = 'ALTER TABLE'",
		sourceName).Scan(&recorded); err != nil || !strings.Contains(strings.ToUpper(recorded), "ALTER IGNORE TABLE") {
		t.Fatalf("schema_changes row for the ALTER IGNORE on dups: %q, %v", recorded, err)
	}
	secondBoundary()
	at := time.Now().UTC().Truncate(time.Second).Add(-time.Second).Format("2006-01-02 15:04:05")
	stream.stop(t)

	out, errOut, err := runResult(binPath, coverDir, "baseline", "refresh", "--index-dsn", indexDSN,
		"--baseline-dir", baseDir, "--at", at, "--tables", sourceName+".dups")
	if err == nil || !strings.Contains(out+errOut, "ALTER TABLE ... IGNORE ... UNIQUE") ||
		!strings.Contains(out+errOut, "changed the table's rows without writing row changes") {
		t.Fatalf("refresh of a table after ALTER IGNORE ... ADD UNIQUE: err=%v, want the refusal naming it\n%s\n%s", err, out, errOut)
	}
	t.Logf("refresh of dups refused as wanted:\n%s%s", out, errOut)

	// The ALTER IGNORE that deleted nothing does not stop its table.
	run(t, binPath, coverDir, "baseline", "refresh", "--index-dsn", indexDSN,
		"--baseline-dir", baseDir, "--at", at, "--tables", sourceName+".plain")
	plain := chainTable{name: "plain", cols: []string{"id", "n"}, pk: []string{"id"}}
	assertSnapshotState(t, binPath, coverDir, baseDir, tmp, "refresh of plain after ALTER IGNORE ... ADD INDEX", sourceName,
		[]chainTable{plain}, map[string][]string{"plain": plain.textRows(t, sourceDB, sourceName)})
}

// waitSchemaChange waits until capture has recorded a statement on table.
func waitSchemaChange(t *testing.T, indexDB *sql.DB, schema, table string, p *streamProc) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if p.cmd.ProcessState != nil {
			t.Fatalf("bintrail stream exited early\n%s", p.log.String())
		}
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM schema_changes WHERE schema_name = ? AND table_name = ?",
			schema, table).Scan(&n); err == nil && n > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no schema_changes row for %s.%s after 45s\n%s", schema, table, p.log.String())
}
