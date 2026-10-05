//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Two shapes that used to be sent to the copy and are the source's (#2115),
// and the ones next to them that must stay the copy's, each pinned by who
// answered and under which reason.
//
// The source's ev has 40,000 rows saying "live", one per minute; the copy's
// has 40 saying "copy", inside the range the statements read. The range is
// 1,500 rows: over the 1,000 the older LIMIT rule accepted, and over the
// copy's row cap, set to 50 here. The thresholds are low enough for every
// read of the range to be an expensive plan (cost 20, a full scan of 1,000
// rows). FORCE INDEX keeps the plan a range read on every server version:
// the rule is under test, not the optimizer's choice between a range and a
// full scan.
const (
	limitsRange = "at >= '2026-01-02 00:00:00' AND at < '2026-01-03 01:00:00'"
	limitsDDL   = "CREATE TABLE `ev` (\n  `id` int NOT NULL,\n  `at` datetime NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  `note` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `k_at` (`at`)\n);\n"
)

func TestIntegrationFlashbackRoutingLimits_2115(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestDB(t)
	routingLimits(t, srcDB, srcName, testutil.IntegrationDSN(srcName), []limitsCase{
		// The statement of the issue: a few rows of a wide range.
		{"a LIMIT over a wide range", "SELECT status FROM ev FORCE INDEX (k_at) WHERE " + limitsRange + " LIMIT 5", nil, "live", "bounded_limit"},
		{"the same, prepared", "SELECT status FROM ev FORCE INDEX (k_at) WHERE at >= ? AND at < ? LIMIT 5", []any{"2026-01-02 00:00:00", "2026-01-03 01:00:00"}, "live", "bounded_limit"},
		// 1,500 rows, the copy returns 50 at most: not tried there.
		{"a result over the copy's row cap", "SELECT status FROM ev FORCE INDEX (k_at) WHERE " + limitsRange, nil, "live", "result_over_row_cap"},
		// The copy's, as before.
		{"a LIMIT with a filter no index serves", "SELECT status FROM ev FORCE INDEX (k_at) WHERE " + limitsRange + " AND note = 'nope' LIMIT 5", nil, "copy", "expensive_plan"},
		{"a LIMIT under a sort no index serves", "SELECT status FROM ev ORDER BY note DESC LIMIT 5", nil, "copy", "expensive_plan"},
		{"an aggregate: one row out of the whole table", "SELECT min(status), count(*) FROM ev", nil, "copy", "expensive_plan"},
	})
}

// On MariaDB a range read was never the copy's (its plans carry no cost the
// threshold can be compared with), so the LIMIT over the range stays where
// it was; a result over the row cap is seen for a table read whole.
func TestIntegrationFlashbackRoutingLimitsMariaDBSource_2115(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	testutil.SkipIfNoMariaDB(t)
	srcName := fmt.Sprintf("route_limits_maria_%d", time.Now().UnixNano())
	root, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := root.Exec("CREATE DATABASE `" + srcName + "`"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = root.Exec("DROP DATABASE `" + srcName + "`") })
	srcDB, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/"+srcName)
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	routingLimits(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true", []limitsCase{
		{"a LIMIT over a wide range", "SELECT status FROM ev FORCE INDEX (k_at) WHERE " + limitsRange + " LIMIT 5", nil, "live", "cheap_plan"},
		{"the same, prepared", "SELECT status FROM ev FORCE INDEX (k_at) WHERE at >= ? AND at < ? LIMIT 5", []any{"2026-01-02 00:00:00", "2026-01-03 01:00:00"}, "live", "cheap_plan"},
		// 40,000 rows, the copy returns 50 at most: not tried there.
		{"a result over the copy's row cap", "SELECT status FROM ev", nil, "live", "result_over_row_cap"},
		// The copy's, as before.
		{"a LIMIT under a sort no index serves", "SELECT status FROM ev ORDER BY note DESC LIMIT 5", nil, "copy", "expensive_plan"},
		{"an aggregate: one row out of the whole table", "SELECT min(status), count(*) FROM ev", nil, "copy", "expensive_plan"},
	})
}

type limitsCase struct {
	name, stmt string
	args       []any
	side       string // "live" (the source) or "copy"
	reason     string
}

func routingLimits(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string, cases []limitsCase) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	var rows strings.Builder
	rows.WriteString("INSERT INTO ev VALUES ")
	for i := 1; i <= 40000; i++ {
		if i > 1 {
			rows.WriteByte(',')
		}
		fmt.Fprintf(&rows, "(%d,TIMESTAMPADD(MINUTE,%d,'2026-01-01 00:00:00'),'live','n%d')", i, i, i%100)
	}
	for _, q := range []string{strings.TrimSuffix(strings.TrimSpace(limitsDDL), ";"), rows.String(), "ANALYZE TABLE ev"} {
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%.80s: %v", q, err)
		}
	}
	var inRange int
	if err := srcDB.QueryRow("SELECT count(*) FROM ev WHERE " + limitsRange).Scan(&inRange); err != nil || inRange != 1500 {
		t.Fatalf("the range holds %d rows on the source, %v; the fixture is written for 1,500", inRange, err)
	}

	// The copy: 40 rows inside the range, each matching the filter no index
	// serves, so a statement the copy answers returns rows and says "copy".
	baseDir := t.TempDir()
	snap := time.Now().UTC().Add(-time.Minute).Format("2006-01-02T15-04-05Z")
	schemaFile := filepath.Join(t.TempDir(), srcName+".ev-schema.sql")
	if err := os.WriteFile(schemaFile, []byte(limitsDDL), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(baseDir, snap, srcName, "ev.parquet"), cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: limitsDDL}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 40; i++ {
		if err := w.WriteRow([]string{strconv.Itoa(i), "2026-01-02 01:00:00", "copy", "nope"}, []bool{false, false, false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	const rowCap = 50
	// FlashbackListen and ReadRouting are display config (the tally is
	// reported only for a console that says it routes); the port below is
	// served by hand on its own listener.
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg, SQLLimits: sqlsandbox.Limits{MaxRows: rowCap},
		FlashbackListen: "127.0.0.1:3308", ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, CostThreshold: 20, ScanRows: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		_ = serveFlashback(ctx, srv, ln, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{CostThreshold: 20, ScanRows: 1000}})
		close(served)
	}()
	t.Cleanup(func() { cancel(); <-served })
	conn := openFlashback(t, ln.Addr().String(), ent.ID, "tok", srcName)
	defer conn.Close()

	reasons := func() map[string]uint64 {
		t.Helper()
		req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
		req.Header.Set("Authorization", "Bearer tok")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		var fb struct {
			Routing struct {
				Servers map[string]struct {
					Reasons map[string]uint64 `json:"reasons"`
				} `json:"servers"`
			} `json:"routing"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &fb); err != nil {
			t.Fatalf("decode /api/flashback: %v (%s)", err, rec.Body.String())
		}
		out := map[string]uint64{}
		for k, v := range fb.Routing.Servers[ent.ID].Reasons {
			out[k] = v
		}
		return out
	}
	for _, tc := range cases {
		before := reasons()
		res, err := conn.Query(tc.stmt, tc.args...)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		var first sql.NullString
		n := 0
		for res.Next() {
			cols, _ := res.Columns()
			dest := make([]any, len(cols))
			dest[0] = &first
			for i := 1; i < len(dest); i++ {
				dest[i] = new(sql.RawBytes)
			}
			if n == 0 {
				if err := res.Scan(dest...); err != nil {
					t.Fatal(err)
				}
			}
			n++
		}
		if err := res.Err(); err != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
		res.Close()
		// Exactly one decision, under the reason named: a statement tried on
		// the copy and then run on the source would be copy_refused.
		var got []string
		for reason, count := range reasons() {
			for range count - before[reason] {
				got = append(got, reason)
			}
		}
		if len(got) != 1 || got[0] != tc.reason || first.String != tc.side {
			t.Errorf("%s: answered %q (%d rows) under %v, want %q under [%s]\n  %s", tc.name, first.String, n, got, tc.side, tc.reason, tc.stmt)
		}
		if tc.reason == "result_over_row_cap" && n <= rowCap {
			t.Errorf("%s: %d rows, want more than the copy's cap of %d: the source's whole answer", tc.name, n, rowCap)
		}
	}
}
