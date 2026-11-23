//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The measurement of #2640 on a source that runs with GTIDs, captured the way
// `watch` captures it: the sibling of
// reconstruct's TestRereadCoordinate_tablesListDumpUnderWrites, which runs on
// a source without GTIDs and indexes its binary logs from the files.
//
// Two questions, under writes that never pause:
//
//  1. Is the coordinate a `--tables-list` dump records the instant its rows
//     were read at, when the index is written by a stream that follows the
//     source by GTID? The fold starts a table at the binlog file and position
//     its base recorded (query.Options.SincePos) in every capture mode: the
//     GTID set in the footer is never what a fold starts from. So what a GTID
//     source could change is the file and position the stream stamps on each
//     event, which it takes from the rotate event and the event headers the
//     source sends, not from a file it opened.
//  2. Is the GTID set the dump records the set of that same instant? Every
//     transaction indexed before the coordinate must be in it and none at or
//     after it. Nothing folds from the set today; it is recorded in every
//     table file, so a set from another instant would be a wrong record.
//
// Run by hand: it needs mydumper (BINTRAIL_TEST_MYDUMPER) and a second MySQL
// with gtid_mode=ON (BINTRAIL_TEST_GTID_SOURCE_DSN), the index on the main
// test server.
func TestIntegrationRereadCoordinateGTIDSource_2640(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	mydumper := os.Getenv("BINTRAIL_TEST_MYDUMPER")
	if mydumper == "" {
		t.Skip("BINTRAIL_TEST_MYDUMPER is not set: this measurement runs the real mydumper")
	}
	sourceBase := testutil.SkipIfNoGTIDSource(t)
	// The two modes a read of some tables is taken under (no-lock says of
	// itself that it may tear, and safe-no-lock stops under writes).
	for _, mode := range []baseline.LockMode{baseline.LockModeFTWRL, baseline.LockModeLockAll} {
		for round := 1; round <= 3; round++ {
			t.Run(fmt.Sprintf("%s/round%d", mode, round), func(t *testing.T) {
				measureRereadCoordinateGTID(t, mydumper, sourceBase, mode)
			})
		}
	}
}

func measureRereadCoordinateGTID(t *testing.T, mydumper, sourceBase string, mode baseline.LockMode) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now().UTC()
	srcName := fmt.Sprintf("reread_gtid_%d", now.UnixNano())

	root, err := sql.Open("mysql", sourceBase+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = root.Exec("DROP DATABASE IF EXISTS `" + srcName + "`"); root.Close() })
	testutil.MustExec(t, root, "CREATE DATABASE `"+srcName+"`")
	src, err := sql.Open("mysql", sourceBase+"/"+srcName+"?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	src.SetMaxOpenConns(8)
	for _, table := range []string{"t", "sibling"} {
		testutil.MustExec(t, src, "CREATE TABLE "+table+" (id BIGINT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(40) NOT NULL, n INT NOT NULL) ENGINE=InnoDB")
		testutil.MustExec(t, src, "INSERT INTO "+table+" (val, n) VALUES ('seed', 0)")
		// 2^14 rows: enough for the dump to take longer than one write.
		for range 14 {
			testutil.MustExec(t, src, "INSERT INTO "+table+" (val, n) SELECT 'seed', 0 FROM "+table)
		}
	}
	testutil.MustExec(t, src, "CREATE TABLE beat (id INT PRIMARY KEY)")

	// The index, on the main test server: hourly partitions around now.
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

	// The capture: the supervisor's stream, as `watch` runs it for a server
	// saved in the console.
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "reread-gtid", DSN: testutil.IntegrationDSN(indexName), SourceDSN: sourceBase + "/",
		Schemas: srcName, BaselineDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	sup := newMonitorSupervisor(ctx, testutil.IntegrationDSN(indexName), nil, nil)
	if err := sup.Start(ctx, ent); err != nil {
		t.Fatalf("start the capture: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), ent.ID) })
	waitStreamLive(t, src, index, srcName, "beat", "id")
	var streamMode string
	if err := index.QueryRow("SELECT mode FROM stream_state WHERE id = 1").Scan(&streamMode); err != nil {
		t.Fatalf("read the stream's mode: %v", err)
	}
	if streamMode != "gtid" {
		t.Fatalf("the stream follows the source by %q, want gtid: this run would measure the other path", streamMode)
	}

	// Writers: transactions of one to three statements over both tables, so
	// the dump's instant can fall inside a transaction and between two.
	var (
		stop    atomic.Bool
		commits atomic.Int64
		wg      sync.WaitGroup
	)
	for w := range 4 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + time.Now().UnixNano()))
			for !stop.Load() {
				tx, err := src.Begin()
				if err != nil {
					continue
				}
				ok := true
				for range 1 + rng.Intn(3) {
					table := "t"
					if rng.Intn(3) == 0 {
						table = "sibling"
					}
					id := 1 + rng.Intn(40000)
					switch rng.Intn(4) {
					case 0:
						_, err = tx.Exec("INSERT INTO "+table+" (val, n) VALUES (?, 1)", fmt.Sprintf("w%d-%d", w, commits.Load()))
					case 1:
						_, err = tx.Exec("DELETE FROM "+table+" WHERE id = ?", id)
					default:
						_, err = tx.Exec("UPDATE "+table+" SET n = n + 1, val = ? WHERE id = ?", fmt.Sprintf("w%d-%d", w, commits.Load()), id)
					}
					if err != nil {
						ok = false
						break
					}
				}
				// A deadlock or a lock wait under the dump's lock is rolled
				// back and tried again: only committed work counts.
				if !ok {
					_ = tx.Rollback()
					continue
				}
				if tx.Commit() == nil {
					commits.Add(1)
				}
			}
		}(w)
	}
	stopWriters := func() { stop.Store(true); wg.Wait() }
	defer stopWriters()

	time.Sleep(400 * time.Millisecond)
	before := commits.Load()
	host, port, user, password, err := config.ParseSourceDSN(sourceBase + "/" + srcName)
	if err != nil {
		t.Fatalf("ParseSourceDSN: %v", err)
	}
	dumpDir := filepath.Join(t.TempDir(), "dump")
	if err := os.MkdirAll(dumpDir, 0o777); err != nil {
		t.Fatal(err)
	}
	// The daemon's own flags (buildConsoleMydumperArgs), with the table list
	// in place of the schema selection.
	out, dumpErr := exec.Command(mydumper,
		"--host", host, "--port", strconv.Itoa(int(port)), "--user", user, "--password", password,
		"--threads", "4", "--compress-protocol", "--complete-insert",
		"--sync-thread-lock-mode", mode.MydumperValue(), "--trx-tables",
		"--tables-list", srcName+".t",
		"--outputdir", dumpDir).CombinedOutput()
	during := commits.Load() - before
	time.Sleep(400 * time.Millisecond)
	stopWriters()
	if dumpErr != nil {
		t.Fatalf("mydumper: %v\n%s", dumpErr, out)
	}
	if rawMeta, err := os.ReadFile(filepath.Join(dumpDir, "metadata")); err == nil {
		t.Logf("mydumper metadata:\n%s", rawMeta)
	}
	want := rereadGTIDRows(t, src, "SELECT id, val, n FROM t")

	// The dump, converted as a full read's is, into a staging folder.
	staging := t.TempDir()
	if _, err := baseline.Run(ctx, baseline.Config{InputDir: dumpDir, OutputDir: staging, Compression: "zstd", RowGroupSize: 10000}); err != nil {
		t.Fatalf("baseline.Run: %v", err)
	}
	entries, err := os.ReadDir(staging)
	if err != nil {
		t.Fatal(err)
	}
	var reads []string
	for _, e := range entries {
		if _, ok := snapshotdir.ParseTime(e.Name()); ok && e.IsDir() {
			reads = append(reads, e.Name())
		}
	}
	if len(reads) != 1 {
		t.Fatalf("the staging folder holds %v, want one read", entries)
	}
	staged := filepath.Join(staging, reads[0], srcName, "t.parquet")
	meta, err := baseline.ReadParquetMetadata(staged)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if meta.BinlogFile == "" || meta.BinlogPos <= 0 {
		t.Fatalf("the dump recorded no binlog coordinate: %+v", meta)
	}
	if meta.GTIDSet == "" {
		t.Fatalf("the dump of a source with GTIDs recorded no GTID set: %+v", meta)
	}
	coordinate := query.BinlogPos{File: meta.BinlogFile, Pos: uint64(meta.BinlogPos)}
	dumpSet, err := gomysql.ParseMysqlGTIDSet(meta.GTIDSet)
	if err != nil {
		t.Fatalf("the GTID set the dump recorded does not parse: %q: %v", meta.GTIDSet, err)
	}

	// Capture has to pass every write: one more, after the writers stopped,
	// and it is indexed after all of them.
	testutil.MustExec(t, src, "INSERT INTO beat (id) VALUES (1)")
	for deadline := time.Now().Add(90 * time.Second); ; time.Sleep(300 * time.Millisecond) {
		var n int
		_ = index.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ? AND table_name = 'beat' AND pk_values = '1'", srcName).Scan(&n)
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("capture did not reach the write made after the writers stopped")
		}
	}

	// Question 2: the recorded GTID set against the recorded position, over
	// every transaction of the schema the stream indexed.
	rows, err := index.Query("SELECT binlog_file, start_pos, gtid FROM binlog_events WHERE schema_name = ?", srcName)
	if err != nil {
		t.Fatalf("read the indexed events: %v", err)
	}
	var beforeC, afterC, beforeNotInSet, afterInSet, tableBefore, tableAfter int
	func() {
		defer rows.Close()
		for rows.Next() {
			var (
				p    query.BinlogPos
				gtid sql.NullString
			)
			if err := rows.Scan(&p.File, &p.Pos, &gtid); err != nil {
				t.Fatalf("scan: %v", err)
			}
			if !gtid.Valid || gtid.String == "" {
				t.Fatalf("an event at %s:%d carries no GTID on a source with GTIDs", p.File, p.Pos)
			}
			one, err := gomysql.ParseMysqlGTIDSet(gtid.String)
			if err != nil {
				t.Fatalf("event GTID %q: %v", gtid.String, err)
			}
			in := dumpSet.Contain(one)
			if p.Pos < coordinate.Pos && p.File == coordinate.File || (p.File != coordinate.File && p.AtOrBefore(coordinate)) {
				beforeC++
				if !in {
					beforeNotInSet++
				}
			} else {
				afterC++
				if in {
					afterInSet++
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate: %v", err)
		}
	}()
	if err := index.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ? AND table_name = 't' AND binlog_file = ? AND start_pos < ?",
		srcName, coordinate.File, coordinate.Pos).Scan(&tableBefore); err != nil {
		t.Fatal(err)
	}
	if err := index.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ? AND table_name = 't' AND binlog_file = ? AND start_pos >= ?",
		srcName, coordinate.File, coordinate.Pos).Scan(&tableAfter); err != nil {
		t.Fatal(err)
	}
	t.Logf("mode %s, stream by gtid: coordinate %s:%d; %d commits while mydumper ran; indexed events before the coordinate %d (of the table %d), at or after it %d (of the table %d); before it and NOT in the recorded GTID set %d, at or after it and IN the set %d",
		mode, coordinate.File, coordinate.Pos, during, beforeC, tableBefore, afterC, tableAfter, beforeNotInSet, afterInSet)
	if tableBefore == 0 || tableAfter == 0 {
		t.Fatalf("the table was not written on both sides of the dump's coordinate (before %d, after %d): this run measures nothing", tableBefore, tableAfter)
	}
	if beforeNotInSet != 0 || afterInSet != 0 {
		t.Fatalf("mode %s: the GTID set the dump recorded is NOT the set at its recorded position: %d transactions before the position are missing from the set, %d at or after it are in the set",
			mode, beforeNotInSet, afterInSet)
	}

	// Question 1: the fold from the staged read, over the index the GTID
	// stream wrote.
	out2 := t.TempDir()
	at := time.Now().UTC().Truncate(time.Second).Add(2 * time.Second)
	_, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
		IndexDSN: testutil.IntegrationDSN(indexName), BaselineSrc: out2, OutputDir: out2,
		Tables: []string{srcName + ".t"}, At: at, OutputFormat: reconstruct.OutputFormatParquet,
		BaselineOverride: map[string]string{srcName + ".t": staged},
	})
	if err != nil {
		t.Fatalf("the fold from the staged read: %v (failures %+v)", err, failures)
	}
	published, _, _, err := reconstruct.FindBaseline(ctx, out2, srcName, "t", at.Add(time.Second))
	if err != nil {
		t.Fatalf("FindBaseline: %v", err)
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer ddb.Close()
	got := rereadGTIDRows(t, ddb, fmt.Sprintf("SELECT id, val, n FROM parquet_scan('%s')", published))

	var missing, extra, differ int
	for id, w := range want {
		g, ok := got[id]
		switch {
		case !ok:
			missing++
		case g != w:
			differ++
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			extra++
		}
	}
	if missing+extra+differ != 0 {
		t.Fatalf("mode %s, stream by gtid: the folded table is NOT the source: %d rows of the source missing, %d rows the source does not hold, %d rows with other values (source %d rows, folded %d)",
			mode, missing, extra, differ, len(want), len(got))
	}
	t.Logf("mode %s, stream by gtid: the folded table is the source, %d rows", mode, len(want))
}

func rereadGTIDRows(t *testing.T, db *sql.DB, q string) map[int64]string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id, n int64
		var val string
		if err := rows.Scan(&id, &val, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if _, dup := out[id]; dup {
			t.Fatalf("id %d appears twice", id)
		}
		out[id] = fmt.Sprintf("%s/%d", val, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return out
}
