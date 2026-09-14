//go:build integration

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunVerifyBaselinePair_newestFolderUnreadable (#1639): with the newest
// baseline folder unreadable, verify used to pair the two before it and could
// print "match". It now reports every table inconclusive with the cause, as a
// parseable --format json document, and exits non-zero.
func TestRunVerifyBaselinePair_newestFolderUnreadable(t *testing.T) {
	if os.Geteuid() == 0 {
		if os.Getenv("CI") != "" {
			t.Fatal("running as root under CI: the mode-000 fixture is a no-op and this coverage would silently vanish")
		}
		t.Skip("root bypasses directory read permissions")
	}
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt string
		ord           int
	}{{"id", "PRI", "int", 1}, {"status", "", "varchar", 2}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.dt)
	}
	baseDir := t.TempDir()
	now := time.Now().UTC()
	t1 := now.Truncate(time.Hour).Add(-3 * time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	// Two identical readable baselines with no events between them would
	// verify as a match; the third, newest one is the folder nobody can read.
	writeCLIBaseline(t, baseDir, t1, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}}, 200)
	writeCLIBaseline(t, baseDir, t1.Add(time.Hour), dbName, "orders", createSQL, cols, [][]string{{"1", "a"}}, 200)
	writeCLIBaseline(t, baseDir, t1.Add(2*time.Hour), dbName, "orders", createSQL, cols, [][]string{{"1", "b"}}, 300)
	locked := filepath.Join(baseDir, strings.ReplaceAll(t1.Add(2*time.Hour).Format(time.RFC3339), ":", "-"))
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{t1, t1.Add(time.Hour), t1.Add(2 * time.Hour), now.Truncate(time.Hour)})

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	saved := struct {
		noArchive bool
		format    string
	}{vfyNoArchive, vfyFormat}
	vfyNoArchive, vfyFormat = true, "json"
	t.Cleanup(func() { vfyNoArchive, vfyFormat = saved.noArchive, saved.format })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	var runErr error
	out.WriteString(captureStdout(t, func() {
		runErr = runVerifyBaselinePair(cmd, db, resolver, dbName, baseDir, duckdbutil.Tuning{}, "")
	}))
	if runErr == nil {
		t.Fatalf("an all-inconclusive run must exit non-zero; output:\n%s", out.String())
	}
	var rep struct {
		Tables []struct {
			Table  string `json:"table"`
			Status string `json:"status"`
			Detail string `json:"reason"`
		} `json:"tables"`
		Summary struct {
			Match        int `json:"match"`
			Inconclusive int `json:"inconclusive"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("--format json output is not a report (%v):\n%s", err, out.String())
	}
	if rep.Summary.Match != 0 || rep.Summary.Inconclusive != 1 || len(rep.Tables) != 1 ||
		rep.Tables[0].Status != "inconclusive" || !strings.Contains(rep.Tables[0].Detail, filepath.Base(locked)) {
		t.Fatalf("report = %+v; want orders inconclusive naming %s", rep, filepath.Base(locked))
	}
}
