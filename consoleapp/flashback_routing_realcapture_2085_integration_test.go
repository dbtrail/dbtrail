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
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// #2085 with nothing replaced between the source and the routed port: a
// MySQL source in GTID mode, the supervisor's own capture stream indexing it
// into an index on another server, the real capture status reporter reading
// both, and the port deciding from what they say.
//
// What is still written by hand is the snapshot's files (no mydumper in this
// job): the rows are read from the source under a global read lock together
// with its binlog position, which is what a locked dump records, and the
// files carry that position in their footers. Their stamp is set ten minutes
// back instead of waiting out the limit: the reporter keeps an answer for 30
// seconds, so a limit a test can wait out would be shorter than the age the
// watermark is allowed to reach, and no read would ever be vouched for.
//
// The source needs GTIDs and the index must be on another server (an index
// on the source is never compared with it), so this runs where a second
// MySQL with gtid_mode=ON is given (testutil.SkipIfNoGTIDSource): the CI
// shard that runs the TestIntegrationFlashback tests starts one.
func TestIntegrationFlashbackRoutedUnchangedRealCapture_2085(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	sourceBase := testutil.SkipIfNoGTIDSource(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now().UTC()
	srcName := fmt.Sprintf("routed_real_%d", now.UnixNano())

	// The source: two tables with the same rows, and one to see the stream
	// alive with.
	src, err := sql.Open("mysql", sourceBase+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = src.Exec("DROP DATABASE IF EXISTS `" + srcName + "`"); src.Close() })
	testutil.MustExec(t, src, "CREATE DATABASE `"+srcName+"`")
	const tableDDL = "CREATE TABLE `%s` (\n  `id` int NOT NULL,\n  `note` varchar(16) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	for _, table := range []string{"steady", "written"} {
		testutil.MustExec(t, src, strings.TrimSuffix(strings.TrimSpace(strings.Replace(fmt.Sprintf(tableDDL, table), "TABLE `", "TABLE `"+srcName+"`.`", 1)), ";"))
		for id := 1; id <= 3; id++ {
			testutil.MustExec(t, src, "INSERT INTO `"+srcName+"`.`"+table+"` VALUES (?, 'first')", id)
		}
		testutil.MustExec(t, src, "ANALYZE TABLE `"+srcName+"`.`"+table+"`")
	}
	testutil.MustExec(t, src, "CREATE TABLE `"+srcName+"`.beat (id INT PRIMARY KEY)")

	// The index, on the main test server: hourly partitions around now, as
	// an index that has been running for a few hours has.
	index, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, index)
	if err := indexer.EnsureSchema(index); err != nil {
		t.Fatal(err)
	}
	var hours []time.Time
	for h := now.Truncate(time.Hour).Add(-3 * time.Hour); !h.After(now.Add(2 * time.Hour)); h = h.Add(time.Hour) {
		hours = append(hours, h)
	}
	testutil.SetupPartitionedTable(t, index, indexName, hours)

	baseDir := t.TempDir()
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "real", DSN: testutil.IntegrationDSN(indexName), SourceDSN: sourceBase + "/",
		Schemas: srcName, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}

	// The capture: the supervisor's stream, as `watch` runs it for a server
	// saved in the console.
	sup := newMonitorSupervisor(ctx, testutil.IntegrationDSN(indexName), nil, 0)
	if err := sup.Start(ctx, ent); err != nil {
		t.Fatalf("start the capture: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), ent.ID) })
	waitStreamLive(t, src, index, srcName, "beat", "id")

	// The snapshot, as a locked dump takes it: every write waits, the rows
	// and the binlog position are read, the lock is released.
	snapAt := now.Add(-10 * time.Minute).Truncate(time.Second)
	func() {
		lock, err := src.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		if _, err := lock.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
			t.Fatal(err)
		}
		defer func() { _, _ = lock.ExecContext(ctx, "UNLOCK TABLES") }()
		file, pos := sourceBinlogPosition(t, lock)
		at := snapAt.Format(time.RFC3339)
		for _, table := range []string{"steady", "written"} {
			ddl := fmt.Sprintf(tableDDL, table)
			cols, err := baseline.ParseSchemaText(ddl)
			if err != nil {
				t.Fatal(err)
			}
			w, err := baseline.NewWriter(filepath.Join(baseDir, snapAt.Format("2006-01-02T15-04-05Z"), srcName, table+".parquet"), cols, baseline.WriterConfig{
				Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
					baseline.MetaKeyCreateTableSQL: ddl, baseline.MetaKeyBinlogFile: file, baseline.MetaKeyBinlogPos: strconv.FormatUint(pos, 10),
					baseline.MetaKeySnapshotTimestamp: at, baseline.MetaKeyLastDumpAt: at, baseline.MetaKeyFoldGeneration: "0",
					baseline.MetaKeySnapshotProducer: baseline.ProducerDump, baseline.MetaKeyLockMode: string(baseline.LockModeFTWRL),
				}})
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range connStrings(t, lock, "SELECT id, note FROM `"+srcName+"`.`"+table+"` ORDER BY id") {
				if err := w.WriteRow(row, []bool{false, false}); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}()

	// The port: a two-minute limit, eight minutes behind the snapshot's
	// stamp, and the reporter `watch` installs.
	maxAge := 2 * time.Minute
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg, CaptureStatus: newCaptureStatusReporter(""),
		FlashbackListen: "127.0.0.1:3308", ReadRouting: console.ReadRoutingConfig{MaxCopyAge: maxAge, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	addr := serveRouting(t, srv, flashbackConfig{RouteMaxCopyAge: maxAge, RoutePolicy: readrouter.Policy{ScanRows: 2}})
	c := routedConn(t, addr, ent.ID, srcName, "")
	direct := directConn(t, sourceBase+"/"+srcName+"?timeout=5s")

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
	scan := func(table string) string {
		return "SELECT note, count(*) FROM " + table + " GROUP BY note ORDER BY note"
	}
	// heavy runs a full scan of table through the port and requires: the
	// reason counted for it, and an answer equal to what MySQL says of the
	// same statement right after.
	heavy := func(table, wantReason string) {
		t.Helper()
		before := reasons()
		viaPort := connStrings(t, c, scan(table))
		after := reasons()
		var moved []string
		for k, v := range after {
			if v != before[k] {
				moved = append(moved, k)
			}
		}
		if len(moved) != 1 || moved[0] != wantReason {
			t.Errorf("scan of %s: counted under %v, want %s", table, moved, wantReason)
		}
		if fromSource := connStrings(t, direct, scan(table)); !reflect.DeepEqual(viaPort, fromSource) {
			t.Errorf("scan of %s under %v differs from MySQL's answer:\n port:  %v\n mysql: %v", table, moved, viaPort, fromSource)
		}
	}

	// Capture saves its position every ten seconds. Until that position
	// holds everything the source has executed (the inserts above), the
	// first read of the source finds it ahead and proves nothing yet.
	waitCaptureHolds(t, src, index)

	// Past the limit, nothing written to either table since the snapshot:
	// the copy answers both, and says what MySQL says.
	heavy("steady", "tables_unchanged")
	heavy("written", "tables_unchanged")

	// One row changes on the source. Once capture has indexed it, a read of
	// that table goes to MySQL; a read of the other still goes to the copy,
	// which shows it was the table and not the watermark that changed.
	testutil.MustExec(t, src, "UPDATE `"+srcName+"`.written SET note = 'second' WHERE id = 2")
	deadline := time.Now().Add(20 * time.Second)
	for n := 0; n == 0; {
		if err := index.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ? AND table_name = 'written'", srcName).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			if time.Now().After(deadline) {
				t.Fatal("capture never indexed the change of `written`")
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	heavy("written", "copy_too_old")
	heavy("steady", "tables_unchanged")
	if got := connStrings(t, c, "SELECT note FROM written WHERE note = 'second'"); len(got) != 1 {
		t.Errorf("the changed row through the port: %v, want the one row MySQL has", got)
	}
}

// sourceBinlogPosition is the source's current binlog file and position, by
// whichever statement its version answers.
func sourceBinlogPosition(t *testing.T, c *sql.Conn) (file string, pos uint64) {
	t.Helper()
	var lastErr error
	for _, stmt := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		rs, err := c.QueryContext(context.Background(), stmt)
		if err != nil {
			lastErr = err
			continue
		}
		cols, _ := rs.Columns()
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if len(cols) >= 2 && rs.Next() {
			if lastErr = rs.Scan(ptrs...); lastErr == nil {
				file = string(vals[0])
				pos, lastErr = strconv.ParseUint(string(vals[1]), 10, 64)
			}
		} else {
			lastErr = fmt.Errorf("%s returned no row", stmt)
		}
		rs.Close()
		if lastErr == nil {
			return file, pos
		}
	}
	t.Fatalf("the source's binlog position could not be read: %v", lastErr)
	return "", 0
}

// waitCaptureHolds waits until the GTID set capture has saved in index
// contains everything src has executed.
func waitCaptureHolds(t *testing.T, src, index *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var executed string
		var saved sql.NullString
		if err := src.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
			t.Fatal(err)
		}
		err := index.QueryRow("SELECT gtid_set FROM stream_state WHERE id = 1").Scan(&saved)
		if err == nil && saved.Valid {
			have, errHave := gomysql.ParseMysqlGTIDSet(strings.Join(strings.Fields(saved.String), ""))
			want, errWant := gomysql.ParseMysqlGTIDSet(strings.Join(strings.Fields(executed), ""))
			if errHave == nil && errWant == nil && have.Contain(want) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture's saved position never reached the source: saved %q (%v), executed %q", saved.String, err, executed)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
