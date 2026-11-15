//go:build integration

package consoleapp

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// requireMydumperEnv makes TestIntegrationFirstCopyWithTheRealMydumper fail
// instead of skip when mydumper is not on PATH. CI sets it in the shard that
// installs mydumper (the one that runs the rest of consoleapp).
const requireMydumperEnv = "BINTRAIL_REQUIRE_MYDUMPER"

// The first copy of a server, the console's own pipeline with nothing faked:
// the mydumper on PATH dumps the test MySQL, and the dump becomes a published
// Parquet snapshot. It runs on every MySQL cell, so a server whose defaults
// break the dump fails here. MySQL 9.7 is one: explain_format defaults to
// TREE there, and the pinned mydumper crashes with a segmentation fault on it
// unless the console hands it explain_format = TRADITIONAL for its sessions
// (mydumperlock.ExplainFormatSection, #2211).
func TestIntegrationFirstCopyWithTheRealMydumper(t *testing.T) {
	if _, err := exec.LookPath("mydumper"); err != nil {
		if os.Getenv(requireMydumperEnv) == "1" {
			t.Fatalf("%s=1 and mydumper is not on PATH", requireMydumperEnv)
		}
		t.Skip("mydumper is not on PATH")
	}
	db, name := testutil.CreateTestDB(t)
	for _, q := range []string{
		"CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(40), born DATETIME(3), price DECIMAL(10,2), doc JSON)",
		"INSERT INTO items VALUES (1,'one','2026-01-02 03:04:05.678',1.50,'{\"a\":1}'),(2,'two',NULL,NULL,NULL),(3,'three','2026-03-04 00:00:00',3.00,'[1,2]')",
		"CREATE TABLE notes (k VARCHAR(16) PRIMARY KEY, body TEXT)",
		"INSERT INTO notes VALUES ('a','first'),('b','second')",
		"CREATE VIEW big_items AS SELECT * FROM items WHERE id > 1",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	var version, explainFormat string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	_ = db.QueryRow("SELECT @@explain_format").Scan(&explainFormat) // absent on MariaDB and old 8.0
	t.Logf("source %s, explain_format=%q", version, explainFormat)

	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.LockModeFTWRL)
	local := t.TempDir()
	out, err := s.execute(console.BaselineRequest{
		ServerID: "s1", ServerName: "shop-db",
		SourceDSN: testutil.BaseDSN() + "/", Schemas: []string{name}, LocalDir: local,
	})
	if err != nil {
		t.Fatalf("first copy: %v", err)
	}
	if out.stats.TablesProcessed != 2 || out.stats.RowsWritten != 5 {
		t.Errorf("first copy wrote %d tables and %d rows, want 2 and 5", out.stats.TablesProcessed, out.stats.RowsWritten)
	}
	if out.snapDir == "" {
		t.Fatal("first copy published no snapshot directory")
	}
	if _, err := os.Stat(filepath.Join(out.snapDir, baseline.SuccessMarker)); err != nil {
		t.Errorf("the snapshot is not marked complete: %v", err)
	}
}
