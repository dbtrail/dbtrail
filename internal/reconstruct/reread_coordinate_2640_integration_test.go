//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The measurement #2640's design asks for before a table is read again alone:
// is the binlog coordinate a `--tables-list` dump records the instant its rows
// were read at? If it is, the dump folded forward from that coordinate is the
// source table. If the coordinate were LATER than the read, the changes
// between the two would be in neither the dump nor the fold, and nothing
// downstream could notice.
//
// Nothing here is faked: the real mydumper dumps ONE table of a real server
// while writers change that table and a sibling without pause, the dump is
// converted by baseline.Run as a full read's is, the server's own binary logs
// are parsed and indexed by the writers capture uses, and the engine folds the
// table from the staged read (FullTableConfig.BaselineOverride) up to the end
// of the binlog. The result is compared, row by row, with the source after
// the writers stopped.
//
// A coordinate EARLIER than the read cannot be seen this way and does not need
// to be: the fold applies whole row images, last one wins, so replaying a
// change the dump already holds gives the same row.
//
// Run by hand, like the #1938 disk-full test: it needs mydumper, which no CI
// runner has. BINTRAIL_TEST_MYDUMPER is the mydumper to run (a wrapper around
// the pinned image works: the output directory is the last argument).
const mydumperEnv = "BINTRAIL_TEST_MYDUMPER"

// mysqlContainer is the Docker container of the test server, whose binary
// logs are copied out of it: the name the other tests use, unless
// BINTRAIL_TEST_MYSQL_CONTAINER names another.
func mysqlContainer() string {
	if c := os.Getenv("BINTRAIL_TEST_MYSQL_CONTAINER"); c != "" {
		return c
	}
	return "bintrail-test-mysql"
}

func TestRereadCoordinate_tablesListDumpUnderWrites(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	mydumper := os.Getenv(mydumperEnv)
	if mydumper == "" {
		t.Skipf("%s is not set: this measurement runs the real mydumper", mydumperEnv)
	}
	// The three modes a full read of the daemon can be point-consistent
	// under, and no-lock, which says of itself that it is not.
	for _, mode := range []baseline.LockMode{baseline.LockModeFTWRL, baseline.LockModeLockAll, baseline.LockModeSafeNoLock, baseline.LockModeNoLock} {
		for round := 1; round <= 3; round++ {
			t.Run(fmt.Sprintf("%s/round%d", mode, round), func(t *testing.T) {
				measureRereadCoordinate(t, mydumper, mode)
			})
		}
	}
}

func measureRereadCoordinate(t *testing.T, mydumper string, mode baseline.LockMode) {
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	sourceDB.SetMaxOpenConns(8)

	for _, table := range []string{"t", "sibling"} {
		testutil.MustExec(t, sourceDB, "CREATE TABLE "+table+" (id BIGINT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(40) NOT NULL, n INT NOT NULL) ENGINE=InnoDB")
		testutil.MustExec(t, sourceDB, "INSERT INTO "+table+" (val, n) VALUES ('seed', 0)")
		// 2^14 rows: enough for the dump to take longer than one write.
		for range 14 {
			testutil.MustExec(t, sourceDB, "INSERT INTO "+table+" (val, n) SELECT 'seed', 0 FROM "+table)
		}
	}
	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	firstBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
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
				tx, err := sourceDB.Begin()
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
	host, port, user, password, err := config.ParseSourceDSN(testutil.BaseDSN() + "/" + sourceName)
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
		"--tables-list", sourceName+".t",
		"--outputdir", dumpDir).CombinedOutput()
	during := commits.Load() - before
	time.Sleep(400 * time.Millisecond)
	stopWriters()
	if dumpErr != nil {
		if mode == baseline.LockModeSafeNoLock && strings.Contains(string(out), "SAFE_NO_LOCK") {
			// What this mode is for: it stops instead of writing a dump it
			// cannot place.
			t.Logf("safe-no-lock refused the dump under writes, as it says it does:\n%s", out)
			return
		}
		t.Fatalf("mydumper: %v\n%s", dumpErr, out)
	}
	if rawMeta, err := os.ReadFile(filepath.Join(dumpDir, "metadata")); err == nil {
		t.Logf("mydumper metadata:\n%s", rawMeta)
	}

	want := sourceRows(t, sourceDB, "t")

	// The dump, converted as a full read's is, into a staging folder.
	staging := t.TempDir()
	if _, err := baseline.Run(ctx, baseline.Config{InputDir: dumpDir, OutputDir: staging, Compression: "zstd", RowGroupSize: 10000}); err != nil {
		t.Fatalf("baseline.Run: %v", err)
	}
	// baseline.Run also leaves its `current` link and lock beside the read.
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
	staged := filepath.Join(staging, reads[0], sourceName, "t.parquet")
	meta, err := baseline.ReadParquetMetadata(staged)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if meta.BinlogFile == "" || meta.BinlogPos <= 0 {
		t.Fatalf("the dump recorded no binlog coordinate: %+v", meta)
	}
	coordinate := query.BinlogPos{File: meta.BinlogFile, Pos: uint64(meta.BinlogPos)}

	// Capture: every binary log written since the writers started.
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	indexBinlogsFrom(t, sourceDB, indexDB, res, sourceName, firstBinlog)
	beforeC, afterC := eventsAround(t, indexDB, sourceName, "t", coordinate)
	t.Logf("mode %s: coordinate %s:%d (gtid %q); %d commits while mydumper ran; events of the table before the coordinate %d, after it %d",
		mode, coordinate.File, coordinate.Pos, meta.GTIDSet, during, beforeC, afterC)
	if beforeC == 0 || afterC == 0 {
		t.Fatalf("the table was not written on both sides of the dump's coordinate (before %d, after %d): this run measures nothing", beforeC, afterC)
	}

	root := t.TempDir()
	at := time.Now().UTC().Truncate(time.Second).Add(2 * time.Second)
	_, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
		IndexDSN: testutil.BaseDSN() + "/" + indexName, BaselineSrc: root, OutputDir: root,
		Tables: []string{sourceName + ".t"}, At: at, OutputFormat: reconstruct.OutputFormatParquet,
		BaselineOverride: map[string]string{sourceName + ".t": staged},
	})
	if err != nil {
		t.Fatalf("the fold from the staged read: %v (failures %+v)", err, failures)
	}
	published, _, _, err := reconstruct.FindBaseline(ctx, root, sourceName, "t", at.Add(time.Second))
	if err != nil {
		t.Fatalf("FindBaseline: %v", err)
	}
	got := parquetRows(t, published)

	missing, extra, differ := diffRows(want, got)
	if missing+extra+differ == 0 {
		t.Logf("mode %s: the folded table is the source, %d rows", mode, len(want))
		return
	}
	msg := fmt.Sprintf("mode %s: the folded table is NOT the source: %d rows of the source missing, %d rows the source does not hold, %d rows with other values (source %d rows, folded %d)",
		mode, missing, extra, differ, len(want), len(got))
	if mode == baseline.LockModeNoLock {
		// The mode that says it may tear. Recorded, not failed.
		t.Log(msg)
		return
	}
	t.Fatal(msg)
}

func sourceRows(t *testing.T, db *sql.DB, table string) map[int64]string {
	t.Helper()
	rows, err := db.Query("SELECT id, val, n FROM " + table)
	if err != nil {
		t.Fatalf("read the source: %v", err)
	}
	defer rows.Close()
	return scanRows(t, rows)
}

func parquetRows(t *testing.T, path string) map[int64]string {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer ddb.Close()
	rows, err := ddb.Query(fmt.Sprintf("SELECT id, val, n FROM parquet_scan('%s')", path))
	if err != nil {
		t.Fatalf("parquet_scan %s: %v", path, err)
	}
	defer rows.Close()
	return scanRows(t, rows)
}

func scanRows(t *testing.T, rows *sql.Rows) map[int64]string {
	t.Helper()
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

func diffRows(want, got map[int64]string) (missing, extra, differ int) {
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
	return missing, extra, differ
}

// indexBinlogsFrom parses every binary log of the test server from first up
// to the one before the current, and indexes the events of schema.
func indexBinlogsFrom(t *testing.T, sourceDB, indexDB *sql.DB, res *metadata.Resolver, schema, first string) {
	t.Helper()
	current, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	rows, err := sourceDB.Query("SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	cols, _ := rows.Columns()
	var files []string
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan SHOW BINARY LOGS: %v", err)
		}
		name := string(vals[0])
		older := len(name) < len(first) || (len(name) == len(first) && name < first)
		if !older && name != current {
			files = append(files, name)
		}
	}
	rows.Close()
	if len(files) == 0 {
		t.Fatalf("no binary log between %s and %s", first, current)
	}
	// Where the server keeps its binary logs, by its own account.
	var basename string
	if err := sourceDB.QueryRow("SELECT @@log_bin_basename").Scan(&basename); err != nil {
		t.Fatalf("read @@log_bin_basename: %v", err)
	}
	binlogDir := filepath.Dir(basename)
	tmpDir := t.TempDir()
	idx := indexer.New(indexDB, 1000)
	p := parser.New(tmpDir, res, parser.Filters{Schemas: map[string]bool{schema: true}}, nil)
	for _, f := range files {
		// `docker exec cat`, not `docker cp`: cp cannot read a data directory
		// kept on a tmpfs mount.
		dst, err := os.Create(filepath.Join(tmpDir, f))
		if err != nil {
			t.Fatal(err)
		}
		cat := exec.Command("docker", "exec", mysqlContainer(), "cat", binlogDir+"/"+f)
		cat.Stdout = dst
		var stderr strings.Builder
		cat.Stderr = &stderr
		err = cat.Run()
		if cerr := dst.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			t.Fatalf("copy %s out of the server: %v\n%s", f, err, stderr.String())
		}
		evCh := make(chan event.Event, 1000)
		parseErr := make(chan error, 1)
		go func() {
			defer close(evCh)
			parseErr <- p.ParseFile(context.Background(), f, evCh)
		}()
		var batch []event.Event
		flush := func() {
			if len(batch) == 0 {
				return
			}
			if _, err := idx.InsertBatch(batch); err != nil {
				t.Fatalf("InsertBatch: %v", err)
			}
			batch = batch[:0]
		}
		for ev := range evCh {
			if batch = append(batch, ev); len(batch) == 1000 {
				flush()
			}
		}
		flush()
		if err := <-parseErr; err != nil {
			t.Fatalf("ParseFile %s: %v", f, err)
		}
		_ = os.Remove(filepath.Join(tmpDir, f))
	}
}

// eventsAround counts the indexed events of schema.table that start before c
// and at or after it.
func eventsAround(t *testing.T, indexDB *sql.DB, schema, table string, c query.BinlogPos) (before, after int) {
	t.Helper()
	rows, err := indexDB.Query("SELECT binlog_file, start_pos FROM binlog_events WHERE schema_name = ? AND table_name = ?", schema, table)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var p query.BinlogPos
		if err := rows.Scan(&p.File, &p.Pos); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if p.Pos < c.Pos && p.File == c.File || (p.File != c.File && p.AtOrBefore(c)) {
			before++
		} else {
			after++
		}
	}
	return before, after
}
