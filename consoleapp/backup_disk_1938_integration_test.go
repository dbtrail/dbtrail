//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"github.com/dbtrail/dbtrail/internal/config"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1938 against a real server: the estimate's query as the server answers
// it, including a size read right after a table grew (MySQL 8 caches
// information_schema sizes for a day by default) and a schema with no
// tables, whose SUM is NULL.
func TestIntegrationEstimateDumpSize(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	ctx := context.Background()
	sourceDSN := testutil.BaseDSN() + "/"

	empty, err := estimateDumpSize(ctx, sourceDSN, config.SSL{Mode: "preferred"}, []string{name})
	if err != nil || empty.tables != 0 || empty.bytes != 0 || empty.unsized != 0 {
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
		if est.bytes > cached || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	if est.tables != 1 {
		t.Fatalf("tables = %d, want 1: the view is not a table the dump copies", est.tables)
	}
	if est.bytes <= cached {
		t.Fatalf("estimate %d never rose above the cached one-row size %d: the stale cache was read", est.bytes, cached)
	}
	var stale int64
	if err := db.QueryRow("SELECT DATA_LENGTH + INDEX_LENGTH FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 't1'", name).Scan(&stale); err != nil {
		t.Fatal(err)
	}
	t.Logf("default session still reads %d bytes", stale)
	t.Logf("cached one-row size %d, fresh estimate %d bytes", cached, est.bytes)

	// Every non-system schema: this one's table is counted, the system
	// schemas are not.
	all, err := estimateDumpSize(ctx, sourceDSN, config.SSL{Mode: "preferred"}, nil)
	if err != nil || all.tables < 1 || all.bytes < est.bytes {
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
	if all.tables != everything-sys {
		t.Fatalf("all schemas counted %d tables; the server has %d, %d of them in system schemas, so want %d",
			all.tables, everything, sys, everything-sys)
	}

	// A source that refuses gives an error, which the verdict turns into
	// "the check did not run".
	if _, err := estimateDumpSize(ctx, strings.Replace(sourceDSN, ":testroot@", ":wrong@", 1), config.SSL{Mode: "preferred"}, nil); err == nil {
		t.Fatal("a wrong password returned an estimate")
	} else {
		t.Log(fmt.Sprint("refused as expected: ", err))
	}
}
