//go:build integration

package consoleapp

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// sql-compare against a real MySQL source and the port as the copy. The copy
// holds the SAME rows as the source, so every difference the run reports is a
// semantic one: MySQL's case-insensitive collation groups 'live' and 'LIVE'
// together, the copy does not.
func TestIntegrationSQLCompare(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	srcDB, srcName := testutil.CreateTestDB(t)
	for _, q := range []string{
		"CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, status VARCHAR(32))",
		"INSERT INTO orders VALUES (1,'live'),(2,'live'),(3,'LIVE')",
		"ANALYZE TABLE orders",
	} {
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	sourceDSN := testutil.IntegrationDSN(srcName)

	baseDir := t.TempDir()
	writeCompareBaseline(t, baseDir, srcName, [][]string{{"1", "live"}, {"2", "live"}, {"3", "LIVE"}})

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = serveFlashback(ctx, srv, ln, flashbackConfig{}); close(served) }()
	defer func() { cancel(); <-served }()
	copyDSN := fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr(), srcName)

	stmts := filepath.Join(t.TempDir(), "stmts.sql")
	if err := os.WriteFile(stmts, []byte(`
-- identical data, deterministic order
SELECT id, status FROM orders ORDER BY id;
-- MySQL groups live/LIVE together (collation), the copy does not
SELECT status, count(*) FROM orders GROUP BY status;
SELECT id FROM orders WHERE status = 'live';
-- same rows; MySQL orders live/LIVE as equal (then by id), the copy puts 'LIVE' first
SELECT status FROM orders ORDER BY status, id;
-- MySQL syntax the copy lacks
SELECT CONVERT(status USING utf8mb4) s, count(*) FROM orders GROUP BY s;
-- the router vetoes NOW(): a difference here does not count
SELECT id, NOW() FROM orders ORDER BY id;
UPDATE orders SET status = 'x';
WITH c AS (SELECT id FROM orders) DELETE FROM orders WHERE id IN (SELECT id FROM c);
SELECT * FROM nope;
`+"-- what an ORM sends: backtick names, which the copy refuses (nothing is translated)\nSELECT `status`, count(*) FROM `orders` GROUP BY `status`;\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fh, err := os.Open(stmts)
	if err != nil {
		t.Fatal(err)
	}
	statements, err := sqlcompare.ParseStatements(fh)
	fh.Close()
	if err != nil {
		t.Fatal(err)
	}
	// Scan rule at 2 rows: the 3-row full scans are "copy", the PK order
	// scan is not ALL and stays "mysql"; cost rule off (3 rows cost ~1).
	rep, err := sqlcompare.Run(context.Background(), sqlcompare.Options{
		SourceDSN: sourceDSN, CopyDSN: copyDSN, Policy: readrouter.Policy{ScanRows: 2},
	}, statements)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	by := map[string]sqlcompare.Result{}
	for _, r := range rep.Results {
		by[r.Statement] = r
	}
	want := map[string]struct {
		verdict sqlcompare.Verdict
		kind    string
		route   string
	}{
		"SELECT id, status FROM orders ORDER BY id":                                           {sqlcompare.Equal, "", ""},
		"SELECT status, count(*) FROM orders GROUP BY status":                                 {sqlcompare.Different, "rows", "copy"},
		"SELECT id FROM orders WHERE status = 'live'":                                         {sqlcompare.Different, "rows", "copy"},
		"SELECT status FROM orders ORDER BY status, id":                                       {sqlcompare.Different, "order", "copy"},
		"SELECT `status`, count(*) FROM `orders` GROUP BY `status`":                           {sqlcompare.NotOnCopy, "", "copy"},
		"SELECT CONVERT(status USING utf8mb4) s, count(*) FROM orders GROUP BY s":             {sqlcompare.NotOnCopy, "", "copy"},
		"WITH c AS (SELECT id FROM orders) DELETE FROM orders WHERE id IN (SELECT id FROM c)": {sqlcompare.Skipped, "not_read_only", "mysql"},
		"UPDATE orders SET status = 'x'":                                                      {sqlcompare.Skipped, "not_a_select", "mysql"},
		"SELECT * FROM nope":                                                                  {sqlcompare.SourceError, "", "mysql"},
	}
	for stmt, w := range want {
		r, ok := by[stmt]
		if !ok {
			t.Errorf("no result for %q", stmt)
			continue
		}
		if r.Verdict != w.verdict || (w.kind != "" && r.Kind != w.kind) || (w.route != "" && r.Route != w.route) {
			t.Errorf("%q: got %s/%s route=%s (%s) %q, want %s/%s route=%s", stmt, r.Verdict, r.Kind, r.Route, r.RouteReason, r.Detail, w.verdict, w.kind, w.route)
		}
	}
	// NOW() moves between the two source reads (INCONCLUSIVE: source changed)
	// or not (DIFFERENT) depending on the second boundary; either way the
	// router vetoes it, so it never counts.
	if r := by["SELECT id, NOW() FROM orders ORDER BY id"]; r.Route != "mysql" || !strings.Contains(r.RouteReason, "veto: NOW") || (r.Verdict != sqlcompare.Different && r.Verdict != sqlcompare.Inconclusive) {
		t.Errorf("NOW(): got %s route=%s (%s), want DIFFERENT or INCONCLUSIVE, vetoed", r.Verdict, r.Route, r.RouteReason)
	}
	if rep.CopyDiffers != 3 || rep.CopyOrderDiffers != 1 || !rep.Failed() {
		t.Errorf("CopyDiffers = %d, CopyOrderDiffers = %d, want 3 and 1 (the collation differences the router would route to the copy; NOW() is vetoed; the ORDER BY one is counted apart)", rep.CopyDiffers, rep.CopyOrderDiffers)
	}
	var status string
	var n int
	if err := srcDB.QueryRow("SELECT status, (SELECT count(*) FROM orders) FROM orders WHERE id = 1").Scan(&status, &n); err != nil || status != "live" || n != 3 {
		t.Errorf("the source was written to (status=%q, rows=%d, err=%v): the UPDATE and the WITH ... DELETE must have been skipped", status, n, err)
	}
	// A copy port with routing ON is refused before anything runs.
	routed := serveRouting(t, srv, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}})
	if _, err := sqlcompare.Run(context.Background(), sqlcompare.Options{SourceDSN: sourceDSN, CopyDSN: fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, routed, srcName)}, statements); err == nil || !strings.Contains(err.Error(), "read routing on") {
		t.Errorf("a routing port was accepted as the copy: err = %v", err)
	}
	var out bytes.Buffer
	sqlcompare.WriteText(&out, rep)
	for _, s := range []string{"DIFFERENT    router=copy", "same rows in a different order", "NOT_ON_COPY", "SKIPPED", "3 statement(s) the router would send to the copy answer DIFFERENTLY", "1 copy-routed statement(s) return the same rows in a different order"} {
		if !strings.Contains(out.String(), s) {
			t.Errorf("text report lacks %q:\n%s", s, out.String())
		}
	}

	// Sampling from performance_schema: the statements this test just ran on
	// the source are there when the consumer is on (the test container
	// enables it); otherwise the error names the consumer.
	sampled, err := sqlcompare.SampleSource(context.Background(), sourceDSN, 50, srcName)
	if err != nil {
		if !strings.Contains(err.Error(), "events_statements_history_long") {
			t.Errorf("SampleSource error does not name the consumer: %v", err)
		}
		t.Logf("sampling unavailable here: %v", err)
		return
	}
	if !contains(sampled, "SELECT status, count(*) FROM orders GROUP BY status") {
		t.Errorf("sampled statements lack the GROUP BY this test ran: %q", sampled)
	}
	for _, s := range sampled {
		if readrouter.Classify(s) != readrouter.KindSelect {
			t.Errorf("sampled a non-SELECT: %q", s)
		}
	}
}

func writeCompareBaseline(t *testing.T, dir, schema string, rows [][]string) {
	t.Helper()
	snap := time.Now().UTC().Add(-time.Minute).Format("2006-01-02T15-04-05Z")
	path := filepath.Join(dir, snap, schema, "orders.parquet")
	schemaFile := filepath.Join(t.TempDir(), schema+".orders-schema.sql")
	ddl := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	if err := os.WriteFile(schemaFile, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// serveRouting opens a second port on the same console with the given config.
func serveRouting(t *testing.T, srv *console.Server, cfg flashbackConfig) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = serveFlashback(ctx, srv, ln, cfg); close(served) }()
	t.Cleanup(func() { cancel(); <-served })
	return ln.Addr().String()
}
