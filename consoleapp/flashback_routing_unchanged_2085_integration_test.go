//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2085 end to end: a real MySQL source, a real index, a snapshot three days
// old and a routing port whose freshness limit is fifteen minutes. Before,
// every heavy read on that port went to MySQL. Now the copy answers the ones
// over tables the index holds no change of since the snapshot.
//
// Who answered is read from the rows: the source's rows say "live" and the
// copy's say "copy". The index is written by hand, so the router's question
// ("did this table change since its snapshot") is answered by what the test
// put in it, and a table the index is silent about goes to the copy even
// though its rows differ here. The one table that must compare EQUAL holds
// the same rows on both sides.

// routedWatermark is the capture status reporter of the rig: its watermark
// is whatever the test sets, and every ask is counted.
type routedWatermark struct {
	mu    sync.Mutex
	ago   time.Duration // capture complete as of this long ago; 0 = not known
	asked int
}

func (w *routedWatermark) CaptureStatus(context.Context, console.ServerEntry) console.CaptureStatus {
	return console.CaptureStatus{State: console.CaptureStateUnknown}
}

func (w *routedWatermark) CaptureWatermark(context.Context, console.ServerEntry) console.CaptureWatermark {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.asked++
	if w.ago == 0 {
		return console.CaptureWatermark{Detail: "the source did not answer"}
	}
	return console.CaptureWatermark{Through: time.Now().Add(-w.ago)}
}

func (w *routedWatermark) set(ago time.Duration) {
	w.mu.Lock()
	w.ago = ago
	w.mu.Unlock()
}

func (w *routedWatermark) asks() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.asked
}

const unchangedTableDDL = "CREATE TABLE `%s` (\n  `id` int NOT NULL,\n  `side` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"

func TestIntegrationFlashbackRoutedUnchangedTables_2085(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	now := time.Now().UTC()
	snapAt := now.Add(-72 * time.Hour).Truncate(time.Second) // far past the 15-minute limit
	anchorFile, anchorPos := "binlog.000007", uint64(4200)
	srcName := fmt.Sprintf("routed_unch_%d", now.UnixNano())

	// The index: hourly partitions from before the snapshot to now, a
	// capture on record with nothing lost, and no event yet.
	index, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, index)
	if err := indexer.EnsureSchema(index); err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, index, serverid.DDLBintrailServerChanges)
	var hours []time.Time
	for h := snapAt.Truncate(time.Hour).Add(-5 * time.Hour); !h.After(now); h = h.Add(time.Hour) {
		hours = append(hours, h)
	}
	testutil.SetupPartitionedTable(t, index, indexName, hours)
	testutil.MustExec(t, index, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, events_indexed, last_checkpoint, server_id, capture_skips)
		VALUES (1, 'gtid', 'binlog.000009', 100, '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50', 0, UTC_TIMESTAMP(), 1, '{}')`)
	indexed := func(table string, pos uint64, at time.Time) {
		testutil.InsertEvent(t, index, "binlog.000008", pos, pos+50, at.Format("2006-01-02 15:04:05"), nil,
			srcName, table, 2, "1", []byte(`["side"]`), []byte(`{"id":1,"side":"copy"}`), []byte(`{"id":1,"side":"live"}`))
	}

	// The source: every table says "live", but `same`.
	root, err := sql.Open("mysql", testutil.BaseDSN()+"/")
	if err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, root, "CREATE DATABASE `"+srcName+"`")
	t.Cleanup(func() { _, _ = root.Exec("DROP DATABASE `" + srcName + "`"); root.Close() })
	sourceDSN := testutil.BaseDSN() + "/" + srcName
	src, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	// The snapshot: the same tables, saying "copy", each with the footer a
	// locked dump writes. `carried` and `ancient` were written by earlier
	// runs and carried into this snapshot with their own footers.
	baseDir := t.TempDir()
	snapDir := filepath.Join(baseDir, snapAt.Format("2006-01-02T15-04-05Z"))
	tables := map[string]time.Time{
		"quiet": snapAt, "busy": snapAt, "same": snapAt, "late": snapAt,
		"carried": snapAt.Add(-3 * time.Hour),       // older than the snapshot, inside what the index holds
		"ancient": snapAt.Add(-10 * 24 * time.Hour), // older than the index's oldest partition
	}
	for table, stamp := range tables {
		ddl := fmt.Sprintf(unchangedTableDDL, table)
		testutil.MustExec(t, src, strings.TrimSuffix(strings.TrimSpace(ddl), ";"))
		side := "live"
		if table == "same" {
			side = "copy"
		}
		for id := 1; id <= 3; id++ {
			testutil.MustExec(t, src, "INSERT INTO `"+table+"` VALUES (?, ?)", id, side)
		}
		testutil.MustExec(t, src, "ANALYZE TABLE `"+table+"`")
		cols, err := baseline.ParseSchemaText(ddl)
		if err != nil {
			t.Fatal(err)
		}
		at := stamp.Format(time.RFC3339)
		w, err := baseline.NewWriter(filepath.Join(snapDir, srcName, table+".parquet"), cols, baseline.WriterConfig{
			Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
				baseline.MetaKeyCreateTableSQL: ddl, baseline.MetaKeyBinlogFile: anchorFile, baseline.MetaKeyBinlogPos: strconv.FormatUint(anchorPos, 10),
				baseline.MetaKeySnapshotTimestamp: at, baseline.MetaKeyLastDumpAt: at, baseline.MetaKeyFoldGeneration: "0",
				baseline.MetaKeySnapshotProducer: baseline.ProducerDump, baseline.MetaKeyLockMode: string(baseline.LockModeFTWRL),
			}})
		if err != nil {
			t.Fatal(err)
		}
		for id := 1; id <= 3; id++ {
			if err := w.WriteRow([]string{strconv.Itoa(id), "copy"}, []bool{false, false}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// One table changed since the snapshot, as far as the index knows.
	indexed("busy", 500, snapAt.Add(time.Hour))

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: testutil.IntegrationDSN(indexName), SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	wm := &routedWatermark{ago: 10 * time.Second}
	maxAge := 15 * time.Minute
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg, CaptureStatus: wm,
		FlashbackListen: "127.0.0.1:3308", ReadRouting: console.ReadRoutingConfig{MaxCopyAge: maxAge, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	addr := serveRouting(t, srv, flashbackConfig{RouteMaxCopyAge: maxAge, RoutePolicy: readrouter.Policy{ScanRows: 2}})
	c := routedConn(t, addr, ent.ID, srcName, "")

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
	// heavy runs a full scan of the named tables (the copy's, by the scan
	// rule at 2 rows) and returns who answered and the reason counted for it.
	heavy := func(stmt string) (side, reason string) {
		t.Helper()
		before := reasons()
		rows := connStrings(t, c, stmt)
		after := reasons()
		for k, v := range after {
			if v != before[k] {
				if reason != "" {
					t.Fatalf("%s: two reasons moved (%s and %s)", stmt, reason, k)
				}
				reason = k
			}
		}
		return sidesOf(rows), reason
	}
	scan := func(table string) string { return "SELECT side, count(*) FROM " + table + " GROUP BY side" }
	expect := func(stmt, wantSide, wantReason string) {
		t.Helper()
		side, reason := heavy(stmt)
		if side != wantSide || reason != wantReason {
			t.Errorf("%s: answered by %q under %q, want %q under %q", stmt, side, reason, wantSide, wantReason)
		}
	}

	// A heavy read over a table nothing was written to since the snapshot:
	// the copy, three days past a fifteen-minute limit.
	expect(scan("quiet"), "copy", "tables_unchanged")
	// One changed table: today's rule.
	expect(scan("busy"), "live", "copy_too_old")
	// One changed table among several: today's rule.
	expect("SELECT q.side, count(*) FROM quiet q JOIN busy b ON b.id = q.id GROUP BY q.side", "live", "copy_too_old")
	// The same through a prepared statement.
	func() {
		t.Helper()
		before := reasons()
		rows, err := c.QueryContext(ctx, "SELECT side, count(*) FROM quiet WHERE id > ? GROUP BY side", 0)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var side string
		var n int
		for rows.Next() {
			if err := rows.Scan(&side, &n); err != nil {
				t.Fatal(err)
			}
		}
		if after := reasons(); side != "copy" || n != 3 || after["tables_unchanged"] != before["tables_unchanged"]+1 {
			t.Errorf("prepared scan of quiet: %s/%d, tables_unchanged %d -> %d; want copy/3 and one more", side, n, before["tables_unchanged"], after["tables_unchanged"])
		}
	}()

	// A table whose file was written by an earlier run and carried into this
	// snapshot is judged from its own footer: unchanged while the index
	// still reaches back to it, and not vouched for once it does not.
	expect(scan("carried"), "copy", "tables_unchanged")
	expect(scan("ancient"), "live", "copy_too_old")

	// A cheap read pays for none of it: the watermark is not even asked.
	asks := wm.asks()
	expect("SELECT side FROM quiet WHERE id = 1", "live", "cheap_plan")
	if wm.asks() != asks {
		t.Errorf("a cheap read asked for the capture watermark %d time(s)", wm.asks()-asks)
	}

	// Capture not known to be up to date, or known only as of longer ago
	// than the limit: today's rule, for the table the copy just answered.
	wm.set(0)
	expect(scan("quiet"), "live", "copy_too_old")
	wm.set(time.Hour)
	expect(scan("quiet"), "live", "copy_too_old")
	wm.set(10 * time.Second)
	expect(scan("quiet"), "copy", "tables_unchanged")

	// The answer is as of the check: a change the index receives afterwards
	// sends the NEXT statement over that table to MySQL.
	expect(scan("late"), "copy", "tables_unchanged")
	indexed("late", 900, now.Add(-time.Minute))
	expect(scan("late"), "live", "copy_too_old")

	// EQUAL to MySQL, statement for statement: the table that holds the same
	// rows on both sides, read through the routed port and straight from the
	// source.
	direct := directConn(t, sourceDSN+"?timeout=5s")
	viaPort := connStrings(t, c, "SELECT id, side FROM same WHERE side <> 'x' ORDER BY id")
	if got := reasons()["tables_unchanged"]; got < 5 {
		t.Errorf("tables_unchanged = %d after the scan of `same`, want it counted", got)
	}
	if fromSource := connStrings(t, direct, "SELECT id, side FROM same WHERE side <> 'x' ORDER BY id"); !reflect.DeepEqual(viaPort, fromSource) {
		t.Errorf("the copy's answer differs from MySQL's:\n copy:  %v\n mysql: %v", viaPort, fromSource)
	}
	// And sql-compare's verdict for it, over a port that only serves the copy.
	copyOnly := serveRouting(t, srv, flashbackConfig{})
	rep, err := sqlcompare.Run(ctx, sqlcompare.Options{
		SourceDSN: sourceDSN, CopyDSN: fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, copyOnly, srcName), Policy: readrouter.Policy{ScanRows: 2},
	}, []string{"SELECT side, count(*) FROM same GROUP BY side", "SELECT id, side FROM same WHERE side <> 'x' ORDER BY id"})
	if err != nil {
		t.Fatalf("sql-compare: %v", err)
	}
	for _, r := range rep.Results {
		if r.Verdict != sqlcompare.Equal || r.Route != "copy" {
			t.Errorf("sql-compare %q: %s route=%s (%s), want EQUAL and the copy", r.Statement, r.Verdict, r.Route, r.Detail)
		}
	}
}
