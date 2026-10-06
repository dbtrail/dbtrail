//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// #2150: live-source verify on a table that is written to for the whole
// check. The source side is a consistent snapshot; the reconstruction is cut
// at that snapshot's own position, so the writes committed while the table is
// read (and after) are out of both sides and the check is a MATCH, never the
// false MISMATCH a cut at wall-clock time gave.
//
// The capture is the real one (One, GTID mode, checkpoint every second), run
// for the whole test as `watch` runs it, so verify also has to wait for it to
// reach each snapshot. MySQL runs on the GTID source (stock MySQL: the
// position is pinned with FLUSH TABLES ... WITH READ LOCK); MariaDB on the MariaDB
// source (its own snapshot position).

const (
	underWritesSeed    = 2000
	underWritesWriters = 6
	underWritesChecks  = 3
)

type underWrites struct {
	flavor     string // gomysql flavor
	sourceDSN  string // with the database
	sourceDB   *sql.DB
	sourceName string
	indexDB    *sql.DB
	indexName  string
}

func (u underWrites) gtidVar() string {
	if u.flavor == gomysql.MariaDBFlavor {
		return "@@global.gtid_binlog_pos"
	}
	return "@@global.gtid_executed"
}

// setup creates and seeds the table, starts the capture, makes
// preBaselineWrites changes and waits for them to be indexed, then writes the
// baseline at that state with its binlog coordinates. It returns the verify
// config and a stop func for the capture.
func (u underWrites) setup(t *testing.T, preBaselineWrites int) (verify.Config, func()) {
	t.Helper()
	testutil.MustExec(t, u.sourceDB, "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL, note VARCHAR(32) NOT NULL) ENGINE=InnoDB")
	var vals []string
	for id := 1; id <= underWritesSeed; id++ {
		vals = append(vals, fmt.Sprintf("(%d,0,'seed')", id))
	}
	testutil.MustExec(t, u.sourceDB, "INSERT INTO t VALUES "+strings.Join(vals, ","))

	hour := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, u.indexDB, u.indexName, []time.Time{hour.Add(-time.Hour), hour, hour.Add(time.Hour), hour.Add(2 * time.Hour)})
	if _, err := metadata.TakeSnapshot(u.sourceDB, u.indexDB, []string{u.sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// The capture, from the source's current position, checkpointing every
	// second.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- One(ctx, Config{
			IndexDSN:   testutil.IntegrationDSN(u.indexName),
			SourceDSN:  u.sourceDSN,
			Flavor:     u.flavor,
			ServerID:   92150,
			BatchSize:  200,
			Schemas:    u.sourceName,
			Checkpoint: 1,
			GapTimeout: 30,
			Format:     "text",
			SSLMode:    "preferred",
			Deps:       testStreamDeps(),
		})
	}()
	stop := func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(30 * time.Second):
			t.Error("the capture did not stop")
		}
	}
	// Running once its first checkpoint is saved.
	deadline := time.Now().Add(60 * time.Second)
	for {
		var n int
		if err := u.indexDB.QueryRow("SELECT COUNT(*) FROM stream_state WHERE id = 1").Scan(&n); err == nil && n == 1 {
			break
		}
		select {
		case err := <-errCh:
			t.Fatalf("the capture stopped before its first checkpoint: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatal("the capture saved no checkpoint in 60s")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Changes indexed before the baseline: the snapshot then starts after
	// the capture's last change, the shape of a quiet source.
	for i := range preBaselineWrites {
		testutil.MustExec(t, u.sourceDB, "UPDATE t SET v = v + 1, note = 'pre' WHERE id = ?", 1+i%underWritesSeed)
	}
	if preBaselineWrites > 0 {
		waitIndexedCount(t, u.indexDB, u.sourceName, preBaselineWrites, 60*time.Second)
	}

	// The baseline: the table as it is now, at the binlog position nothing
	// has been written past yet (no writer runs here).
	file, pos, err := config.CurrentBinlogPosition(u.sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	snapTime := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	root := t.TempDir()
	dir := filepath.Join(root, snapTime.Format("2006-01-02T15-04-05")+"Z", u.sourceName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	ddl := "CREATE TABLE `t` (\n  `id` INT NOT NULL,\n  `v` INT NOT NULL,\n  `note` VARCHAR(32) NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols, err := baseline.ParseSchemaText(ddl)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(dir, "t.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 1000,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: ddl,
			baseline.MetaKeyBinlogFile:     file,
			baseline.MetaKeyBinlogPos:      strconv.FormatUint(uint64(pos), 10),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := u.sourceDB.Query("SELECT id, v, note FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, v, note string
		if err := rows.Scan(&id, &v, &note); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow([]string{id, v, note}, []bool{false, false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	resolver, err := metadata.NewResolver(u.indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	return verify.Config{
		SourceDB: u.sourceDB, IndexDB: u.indexDB, Resolver: resolver,
		BaselineSource: root, IndexDBName: u.indexName, NoArchive: true,
	}, stop
}

// writers runs the steady traffic: single-row updates, inserts and deletes,
// and two-statement transactions, on as many connections. commits counts
// what committed.
func (u underWrites) writers(t *testing.T, commits *atomic.Int64) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var next atomic.Int64
	next.Store(underWritesSeed)
	for w := range underWritesWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 2150))
			for ctx.Err() == nil {
				var err error
				switch op := r.IntN(10); {
				case op < 5:
					_, err = u.sourceDB.ExecContext(ctx, "UPDATE t SET v = v + 1, note = 'upd' WHERE id = ?", 1+r.IntN(underWritesSeed))
				case op < 7:
					_, err = u.sourceDB.ExecContext(ctx, "INSERT INTO t VALUES (?, 1, 'ins')", next.Add(1))
				case op < 8:
					_, err = u.sourceDB.ExecContext(ctx, "DELETE FROM t WHERE id = ?", underWritesSeed+1+r.Int64N(max(1, next.Load()-underWritesSeed)))
				default:
					var tx *sql.Tx
					if tx, err = u.sourceDB.BeginTx(ctx, nil); err == nil {
						_, err = tx.ExecContext(ctx, "UPDATE t SET v = v + 10 WHERE id = ?", 1+r.IntN(underWritesSeed))
						if err == nil {
							_, err = tx.ExecContext(ctx, "UPDATE t SET note = 'tx' WHERE id = ?", 1+r.IntN(underWritesSeed))
						}
						if err == nil {
							err = tx.Commit()
						} else {
							_ = tx.Rollback()
						}
					}
				}
				if err == nil {
					commits.Add(1)
				} else if ctx.Err() == nil && !strings.Contains(err.Error(), "Deadlock") {
					t.Errorf("writer: %v", err)
					return
				}
			}
		}()
	}
	return func() { cancel(); wg.Wait() }
}

func runVerifyUnderWrites(t *testing.T, u underWrites) {
	cfg, stopCapture := u.setup(t, 0)
	defer stopCapture()
	// Stock MySQL pins the position only when asked to pause the table's
	// writes; MariaDB ignores it (its own snapshot position needs no lock).
	cfg.PauseWrites = u.flavor == gomysql.MySQLFlavor

	var commits atomic.Int64
	stopWriters := u.writers(t, &commits)
	defer stopWriters()

	ctx := context.Background()
	for i := range underWritesChecks {
		time.Sleep(300 * time.Millisecond) // writes before the read, too
		before := commits.Load()
		res, err := verify.VerifyTable(ctx, cfg, u.sourceName, "t")
		during := commits.Load() - before
		if err != nil {
			t.Fatalf("check %d: VerifyTable: %v", i+1, err)
		}
		t.Logf("check %d: %s, %d rows on both sides, %d writes committed during it, anchor %.80s, detail %q",
			i+1, res.Status, res.SourceRows, during, res.Anchor, res.Detail)
		if during == 0 {
			t.Fatalf("check %d: no write committed while it ran, so it proves nothing about writes", i+1)
		}
		if res.Status != verify.StatusMatch {
			t.Fatalf("check %d: %s (%s): source rows %d, reconstructed %d; a table written to during the check must compare equal",
				i+1, res.Status, res.Detail, res.SourceRows, res.ReconstructRows)
		}
		if strings.Contains(res.Detail, "not cut at the snapshot") {
			t.Fatalf("check %d: matched without the cut: %q", i+1, res.Detail)
		}
	}
}

// TestIntegrationVerifyLiveSourceUnderWrites2150_mysql: stock MySQL with
// GTIDs on and --pause-writes, the position pinned by the table lock.
func TestIntegrationVerifyLiveSourceUnderWrites2150_mysql(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	base := testutil.SkipIfNoGTIDSource(t)
	u := gtidSourceDB(t, base)
	u.indexDB, u.indexName = testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, u.indexDB)
	runVerifyUnderWrites(t, u)
}

// TestIntegrationVerifyLiveSourceUnderWrites2150_mariadb: MariaDB, the
// position from the server's own snapshot coordinate.
func TestIntegrationVerifyLiveSourceUnderWrites2150_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	u := underWrites{flavor: gomysql.MariaDBFlavor, sourceDB: sourceDB, sourceName: sourceName,
		sourceDSN: testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true"}
	u.indexDB, u.indexName = testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, u.indexDB)
	runVerifyUnderWrites(t, u)
}

// gtidSourceDB creates a database on the GTID source server.
func gtidSourceDB(t *testing.T, base string) underWrites {
	t.Helper()
	name := fmt.Sprintf("verify2150_%d", time.Now().UnixNano())
	root, err := sql.Open("mysql", base+"/")
	if err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, root, "CREATE DATABASE `"+name+"`")
	t.Cleanup(func() { _, _ = root.Exec("DROP DATABASE IF EXISTS `" + name + "`"); root.Close() })
	db, err := sql.Open("mysql", base+"/"+name+"?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(32)
	t.Cleanup(func() { db.Close() })
	return underWrites{flavor: gomysql.MySQLFlavor, sourceDB: db, sourceName: name, sourceDSN: base + "/" + name + "?parseTime=true"}
}

// TestIntegrationVerifyLiveSourceAnchorFallbacks2150: on the GTID source,
// the two ways the table lock is not had. An account without LOCK TABLES
// gets today's unanchored read, and the result says so; a table with a write
// transaction held open for longer than the lock waits is inconclusive, never
// compared against a guessed position.
func TestIntegrationVerifyLiveSourceAnchorFallbacks2150(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	base := testutil.SkipIfNoGTIDSource(t)
	u := gtidSourceDB(t, base)
	u.indexDB, u.indexName = testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, u.indexDB)
	cfg, stopCapture := u.setup(t, 0)
	defer stopCapture()
	cfg.PauseWrites = true // both cases are about the lock --pause-writes asks for
	ctx := context.Background()

	t.Run("no LOCK TABLES grant", func(t *testing.T) {
		user := fmt.Sprintf("v2150_%d", time.Now().UnixNano()%1e9)
		root, err := sql.Open("mysql", base+"/")
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		testutil.MustExec(t, root, "CREATE USER '"+user+"'@'%' IDENTIFIED BY 'pw2150'")
		defer func() { _, _ = root.Exec("DROP USER '" + user + "'@'%'") }()
		testutil.MustExec(t, root, "GRANT SELECT ON `"+u.sourceName+"`.* TO '"+user+"'@'%'")
		// A statement with no row change (the GRANT) reaches the capture's
		// saved position only with the next transaction: one, elsewhere.
		testutil.MustExec(t, u.sourceDB, "CREATE TABLE IF NOT EXISTS beat (id INT PRIMARY KEY)")
		testutil.MustExec(t, u.sourceDB, "INSERT INTO beat VALUES (1)")
		cfgNoLock := cfg
		addr := strings.TrimPrefix(base[strings.Index(base, "@"):], "@")
		cfgNoLock.SourceDB, err = sql.Open("mysql", user+":pw2150@"+addr+"/"+u.sourceName+"?parseTime=true")
		if err != nil {
			t.Fatal(err)
		}
		defer cfgNoLock.SourceDB.Close()
		res, err := verify.VerifyTable(ctx, cfgNoLock, u.sourceName, "t")
		if err != nil {
			t.Fatalf("VerifyTable: %v", err)
		}
		t.Logf("%s: %s", res.Status, res.Detail)
		if res.Status != verify.StatusMatch || !strings.Contains(res.Detail, "not cut at the snapshot") || !strings.Contains(res.Detail, "LOCK TABLES") {
			t.Fatalf("status %s, detail %q; want a match (no writes) that says the read was not cut, and that LOCK TABLES is why", res.Status, res.Detail)
		}
	})

	t.Run("a write transaction held open", func(t *testing.T) {
		hold, err := u.sourceDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = hold.Rollback() }()
		if _, err := hold.ExecContext(ctx, "UPDATE t SET v = v + 1 WHERE id = 1"); err != nil {
			t.Fatal(err)
		}
		res, err := verify.VerifyTable(ctx, cfg, u.sourceName, "t")
		if err != nil {
			t.Fatalf("VerifyTable: %v", err)
		}
		t.Logf("%s: %s", res.Status, res.Detail)
		if res.Status != verify.StatusInconclusive || !strings.Contains(res.Detail, "could not be pinned") {
			t.Fatalf("status %s, detail %q; want inconclusive because the position could not be pinned", res.Status, res.Detail)
		}
	})
}

// TestIntegrationVerifyLiveSourceQuietAfterBaseline2150: changes captured,
// then a baseline, then nothing. The snapshot's last change ends before the
// baseline's position (which is past that transaction's commit), and the
// check must still compare, and match, with the read cut.
func TestIntegrationVerifyLiveSourceQuietAfterBaseline2150(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	base := testutil.SkipIfNoGTIDSource(t)
	u := gtidSourceDB(t, base)
	u.indexDB, u.indexName = testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, u.indexDB)
	cfg, stopCapture := u.setup(t, 25)
	defer stopCapture()
	cfg.PauseWrites = true
	res, err := verify.VerifyTable(context.Background(), cfg, u.sourceName, "t")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	t.Logf("%s: anchor %.80s, detail %q", res.Status, res.Anchor, res.Detail)
	if res.Status != verify.StatusMatch || res.Detail != "" {
		t.Fatalf("status %s, detail %q; want a plain match on a quiet source", res.Status, res.Detail)
	}
}

// TestIntegrationVerifyLiveSourceUnpausedByDefault2150: stock MySQL with
// GTIDs on, writes during the whole check, and no PauseWrites (the default
// everywhere, and the only mode of the web console). verify must not pause
// the source's writes, so it cannot pin the position: every verdict carries
// the note that the read was not cut and that the table must take no writes
// during it. Under these writes today's time cut usually reports a MISMATCH
// (writes after the snapshot are in the reconstruction); the note is what
// tells that apart from a real divergence.
func TestIntegrationVerifyLiveSourceUnpausedByDefault2150(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	base := testutil.SkipIfNoGTIDSource(t)
	u := gtidSourceDB(t, base)
	u.indexDB, u.indexName = testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, u.indexDB)
	cfg, stopCapture := u.setup(t, 0)
	defer stopCapture()
	if cfg.PauseWrites {
		t.Fatal("setup turned PauseWrites on; the default must be off")
	}
	var commits atomic.Int64
	stopWriters := u.writers(t, &commits)
	defer stopWriters()
	for i := range underWritesChecks {
		time.Sleep(300 * time.Millisecond)
		before := commits.Load()
		res, err := verify.VerifyTable(context.Background(), cfg, u.sourceName, "t")
		if err != nil {
			t.Fatalf("check %d: VerifyTable: %v", i+1, err)
		}
		t.Logf("check %d: %s with %d writes during it, detail %q", i+1, res.Status, commits.Load()-before, res.Detail)
		if res.Status == verify.StatusInconclusive || res.Status == verify.StatusError {
			t.Fatalf("check %d: %s (%s); want the comparison made, as before this change", i+1, res.Status, res.Detail)
		}
		if !strings.Contains(res.Detail, "not cut at the snapshot") || !strings.Contains(res.Detail, "takes no writes during the read") ||
			!strings.Contains(res.Detail, "pause of writes") {
			t.Fatalf("check %d: detail %q; want the note that the read was not cut because pausing writes was not asked for, and that the table must take no writes", i+1, res.Detail)
		}
	}
}
