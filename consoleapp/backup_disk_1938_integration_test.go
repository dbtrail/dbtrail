//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"github.com/dbtrail/dbtrail/internal/config"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1938 against a real server: the estimate's query as the server answers
// it, including a size read right after a table grew (MySQL 8 caches
// information_schema sizes for a day by default) and a schema with no
// tables, which answers with no rows.
func TestIntegrationEstimateDumpSize(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	ctx := context.Background()
	sourceDSN := testutil.BaseDSN() + "/"

	empty, err := estimateDumpSize(ctx, sourceDSN, config.SSL{Mode: "preferred"}, []string{name})
	if err != nil || empty.Tables != 0 || empty.Bytes != 0 || empty.Unsized != 0 {
		t.Fatalf("empty schema: %+v, %v", empty, err)
	}

	for _, stmt := range []string{
		"CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT, pad CHAR(200) NOT NULL, KEY (pad))",
		"CREATE VIEW v1 AS SELECT id FROM t1",
		"INSERT INTO t1 (pad) VALUES (REPEAT('x', 200))",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	// Read once on a session with the server's default expiry, so the cache
	// holds the size of a one-row table.
	var cached int64
	if err := db.QueryRow("SELECT DATA_LENGTH + INDEX_LENGTH FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 't1'", name).Scan(&cached); err != nil {
		t.Fatal(err)
	}
	for range 7 { // 2^7 rows, each about 200 bytes plus the index
		if _, err := db.Exec("INSERT INTO t1 (pad) SELECT CONCAT(REPEAT('y', 150), id, RAND()) FROM t1"); err != nil {
			t.Fatal(err)
		}
	}
	for range 7 {
		if _, err := db.Exec("INSERT INTO t1 (pad) SELECT CONCAT(REPEAT('z', 150), id, RAND()) FROM t1 LIMIT 5000"); err != nil {
			t.Fatal(err)
		}
	}
	// InnoDB's own statistics catch up in the background within seconds of a
	// change this size; the cache a default session reads does not, for up to
	// a day. So: poll the estimate until the engine has caught up, then show
	// the default session still reads the one-row size. Without the SET in
	// estimateDumpSize the poll never gets past the cached value.
	var est dumpEstimate
	deadline := time.Now().Add(90 * time.Second)
	for {
		est, err = estimateDumpSize(ctx, sourceDSN, config.SSL{Mode: "preferred"}, []string{name})
		if err != nil {
			t.Fatal(err)
		}
		if est.Bytes > cached || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	if est.Tables != 1 {
		t.Fatalf("tables = %d, want 1: the view is not a table the dump copies", est.Tables)
	}
	if est.Bytes <= cached {
		t.Fatalf("estimate %d never rose above the cached one-row size %d: the stale cache was read", est.Bytes, cached)
	}
	var stale int64
	if err := db.QueryRow("SELECT DATA_LENGTH + INDEX_LENGTH FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 't1'", name).Scan(&stale); err != nil {
		t.Fatal(err)
	}
	t.Logf("default session still reads %d bytes", stale)
	t.Logf("cached one-row size %d, fresh estimate %d bytes", cached, est.Bytes)

	// Every non-system schema: this one's table is counted, the system
	// schemas are not.
	all, err := estimateDumpSize(ctx, sourceDSN, config.SSL{Mode: "preferred"}, nil)
	if err != nil || all.Tables < 1 || all.Bytes < est.Bytes {
		t.Fatalf("all schemas: %+v, %v", all, err)
	}
	// The table counts, read the way the estimate reads them: every base
	// table on the server, and the system schemas alone. The estimate must
	// be the first minus the second, so a filter that let the system schemas
	// in (or dropped user ones) shows up as a different count.
	var everything, sys int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE'").Scan(&everything); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ('mysql','sys','performance_schema','information_schema')").Scan(&sys); err != nil {
		t.Fatal(err)
	}
	if sys == 0 {
		t.Fatal("the server has no system tables, so this case proves nothing")
	}
	if all.Tables != everything-sys {
		t.Fatalf("all schemas counted %d tables; the server has %d, %d of them in system schemas, so want %d",
			all.Tables, everything, sys, everything-sys)
	}

	// A table stored compressed (#1938): the server reports its compressed
	// size, so it is counted and named. And the data is summed apart from the
	// indexes: t1 has a secondary index, so the two totals differ.
	if _, err := db.Exec("CREATE TABLE zipped (id INT PRIMARY KEY AUTO_INCREMENT, pad VARCHAR(200)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO zipped (pad) VALUES (REPEAT('x', 200))"); err != nil {
		t.Fatal(err)
	}
	withZipped, err := estimateDumpSize(ctx, sourceDSN, config.SSL{Mode: "preferred"}, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	if withZipped.Tables != 2 || withZipped.Compressed != 1 || len(withZipped.CompressedTop) != 1 || withZipped.CompressedTop[0] != name+".zipped" {
		t.Fatalf("with a compressed table: %+v", withZipped)
	}
	if withZipped.DataBytes <= 0 || withZipped.DataBytes >= withZipped.Bytes {
		t.Fatalf("data %d, data plus indexes %d: want the data alone to be the smaller, positive sum", withZipped.DataBytes, withZipped.Bytes)
	}

	// The session's own time limit, as the server took it: MySQL knows one
	// variable and MariaDB the other, and the one it knows holds the limit.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if stale := doctor.PrepareDumpEstimateSession(ctx, conn); stale {
		t.Fatal("this server refused fresh sizes for a reason other than not having the cache")
	}
	var mysqlMS, mariaS float64
	errMy := conn.QueryRowContext(ctx, "SELECT @@SESSION.max_execution_time").Scan(&mysqlMS)
	errMa := conn.QueryRowContext(ctx, "SELECT @@SESSION.max_statement_time").Scan(&mariaS)
	t.Logf("max_execution_time = %v (%v), max_statement_time = %v (%v)", mysqlMS, errMy, mariaS, errMa)
	switch {
	case errMy == nil && errMa != nil:
		if mysqlMS != float64(doctor.DumpEstimateServerLimit.Milliseconds()) {
			t.Fatalf("max_execution_time = %v ms, want %d", mysqlMS, doctor.DumpEstimateServerLimit.Milliseconds())
		}
	case errMa == nil && errMy != nil:
		if mariaS != doctor.DumpEstimateServerLimit.Seconds() {
			t.Fatalf("max_statement_time = %v s, want %v", mariaS, doctor.DumpEstimateServerLimit.Seconds())
		}
	default:
		t.Fatalf("expected exactly one of the two variables to exist on this server: %v / %v", errMy, errMa)
	}

	// A source that refuses gives an error, which the verdict turns into
	// "the check did not run".
	if _, err := estimateDumpSize(ctx, strings.Replace(sourceDSN, ":testroot@", ":wrong@", 1), config.SSL{Mode: "preferred"}, nil); err == nil {
		t.Fatal("a wrong password returned an estimate")
	} else {
		t.Log(fmt.Sprint("refused as expected: ", err))
	}
}
