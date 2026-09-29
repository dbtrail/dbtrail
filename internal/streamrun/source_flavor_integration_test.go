//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Source-flavor detection through the real One entry point. The MySQL cases
// are named *Flavor* and the MariaDB ones *MariaDB*, so the MySQL job runs the
// first group, and the MariaDB-source job (whose -run filter matches both
// words) runs all of them against a real MariaDB. The pure decision table is
// in internal/metadata (TestResolveSourceFlavor); these pin the wiring: that
// One asks, refuses, and records what it found in stream_state.

// flavorTestConfig is a first-run Config for a source that exists on the
// server behind sourceDSN, with a fresh index.
func flavorTestConfig(indexName, sourceDSN, schema, flavor string, serverID uint32) Config {
	return Config{
		IndexDSN:   testutil.IntegrationDSN(indexName),
		SourceDSN:  sourceDSN,
		Flavor:     flavor,
		ServerID:   serverID,
		BatchSize:  100,
		Schemas:    schema,
		Checkpoint: 1,
		GapTimeout: 30,
		Format:     "text",
		SSLMode:    "preferred",
		Deps:       testStreamDeps(),
	}
}

// storedFlavor reads stream_state.flavor, "" when there is no checkpoint.
func storedFlavor(t *testing.T, indexDB *sql.DB) string {
	t.Helper()
	var f string
	err := indexDB.QueryRow("SELECT flavor FROM stream_state WHERE id = 1").Scan(&f)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatalf("read stream_state.flavor: %v", err)
	}
	return f
}

// runAttachedWithFlavorHook runs cfg until its first checkpoint (then until
// done) and returns the flavor the real OnFlavorResolved hook reported.
func runAttachedWithFlavorHook(t *testing.T, cfg Config, writes func(), done func() bool) (string, error) {
	t.Helper()
	var mu sync.Mutex
	var got string
	cfg.Hooks = &Hooks{OnFlavorResolved: func(f string) { mu.Lock(); got = f; mu.Unlock() }}
	err := runOneUntil(t, cfg, true, writes, done)
	mu.Lock()
	defer mu.Unlock()
	return got, err
}

// mysqlFlavorSource makes a MySQL source schema with one table: the stream's
// first schema snapshot refuses an empty schema ("no columns found").
func mysqlFlavorSource(t *testing.T) string {
	t.Helper()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		amount DECIMAL(10,2) NOT NULL
	)`)
	return sourceName
}

// TestOne_SourceFlavor_mysqlDetectedWhenUndeclared: a MySQL source with no
// declared flavor captures as mysql and records it.
func TestOne_SourceFlavor_mysqlDetectedWhenUndeclared(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceName := mysqlFlavorSource(t)

	cfg := flavorTestConfig(indexName, testutil.IntegrationDSN(sourceName), sourceName, "", 99981)
	got, err := runAttachedWithFlavorHook(t, cfg, nil, func() bool { return true })
	if err != nil {
		t.Fatalf("One: %v", err)
	}
	if got != metadata.FlavorMySQL {
		t.Errorf("detected flavor = %q, want mysql", got)
	}
	if f := storedFlavor(t, indexDB); f != metadata.FlavorMySQL {
		t.Errorf("stream_state.flavor = %q, want mysql", f)
	}
}

// TestOne_SourceFlavor_mysqlDeclaredMariaDBRefuses: declaring mariadb for a
// MySQL server refuses before anything is written.
func TestOne_SourceFlavor_mysqlDeclaredMariaDBRefuses(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceName := mysqlFlavorSource(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := One(ctx, flavorTestConfig(indexName, testutil.IntegrationDSN(sourceName), sourceName, "mariadb", 99982))
	var mm *metadata.FlavorMismatchError
	if !errors.As(err, &mm) {
		t.Fatalf("want *metadata.FlavorMismatchError, got %v", err)
	}
	if mm.Declared != "mariadb" || mm.Detected != "mysql" {
		t.Errorf("mismatch = declared %q detected %q, want mariadb/mysql", mm.Declared, mm.Detected)
	}
	if f := storedFlavor(t, indexDB); f != "" {
		t.Errorf("a refusal wrote a checkpoint with flavor %q", f)
	}
}

// TestOne_SourceFlavor_detectionFailure: when the server cannot be asked, an
// undeclared flavor refuses and a declared one runs.
func TestOne_SourceFlavor_detectionFailure(t *testing.T) {
	failing := func(*sql.DB) (string, string, error) {
		return "", "", errors.New("SELECT VERSION() failed: Error 1142: command denied")
	}

	t.Run("undeclared refuses", func(t *testing.T) {
		indexDB, indexName := testutil.CreateTestDB(t)
		testutil.InitIndexTables(t, indexDB)
		sourceName := mysqlFlavorSource(t)
		cfg := flavorTestConfig(indexName, testutil.IntegrationDSN(sourceName), sourceName, "", 99983)
		cfg.Deps.DetectSourceFlavor = failing
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		err := One(ctx, cfg)
		var ue *metadata.FlavorUndetectedError
		if !errors.As(err, &ue) {
			t.Fatalf("want *metadata.FlavorUndetectedError, got %v", err)
		}
		if f := storedFlavor(t, indexDB); f != "" {
			t.Errorf("a refusal wrote a checkpoint with flavor %q", f)
		}
	})

	t.Run("declared mysql runs", func(t *testing.T) {
		indexDB, indexName := testutil.CreateTestDB(t)
		testutil.InitIndexTables(t, indexDB)
		sourceName := mysqlFlavorSource(t)
		cfg := flavorTestConfig(indexName, testutil.IntegrationDSN(sourceName), sourceName, "mysql", 99984)
		cfg.Deps.DetectSourceFlavor = failing
		if err := runOneUntil(t, cfg, true, nil, func() bool { return true }); err != nil {
			t.Fatalf("a declared flavor must run when detection fails, got %v", err)
		}
		if f := storedFlavor(t, indexDB); f != metadata.FlavorMySQL {
			t.Errorf("stream_state.flavor = %q, want mysql", f)
		}
	})
}

// mariadbFlavorSource makes a MariaDB source schema with one table, or skips
// (fails, in the MariaDB job) when there is no MariaDB with a binlog.
func mariadbFlavorSource(t *testing.T) (*sql.DB, string) {
	t.Helper()
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		testutil.SkipOrFailMariaDB(t, "binary logging not enabled on test MariaDB")
	}
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		amount DECIMAL(10,2) NOT NULL
	)`)
	return sourceDB, sourceName
}

func mariadbSourceDSN(schema string) string {
	return testutil.MariaDBBaseDSN() + "/" + schema + "?parseTime=true"
}

// indexedOrders counts captured rows for the orders table.
func indexedOrders(t *testing.T, indexDB *sql.DB, schema string) int {
	t.Helper()
	var n int
	if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events
		WHERE schema_name = ? AND table_name = 'orders'`, schema).Scan(&n); err != nil {
		t.Fatalf("count indexed rows: %v", err)
	}
	return n
}

// TestOne_MariaDB_flavorDetectedWhenUndeclared: the case the old default got
// wrong. A MariaDB with nothing declared captures as mariadb: the checkpoint
// says so, the rows land with sane positions (on 11.4+ this needs the #1117
// fill, which only the mariadb flavor turns on), and, when the server writes
// ANNOTATE_ROWS, the statement text is captured (only requested as mariadb).
func TestOne_MariaDB_flavorDetectedWhenUndeclared(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := mariadbFlavorSource(t)

	cfg := flavorTestConfig(indexName, mariadbSourceDSN(sourceName), sourceName, "", 99985)
	got, err := runAttachedWithFlavorHook(t, cfg,
		func() {
			for i := range 3 {
				testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (?)", float64(i+1))
			}
		},
		func() bool { return indexedOrders(t, indexDB, sourceName) >= 3 })
	if err != nil {
		t.Fatalf("One: %v", err)
	}
	if got != metadata.FlavorMariaDB {
		t.Errorf("detected flavor = %q, want mariadb", got)
	}
	if f := storedFlavor(t, indexDB); f != metadata.FlavorMariaDB {
		t.Errorf("stream_state.flavor = %q, want mariadb", f)
	}
	var bad int
	if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events
		WHERE schema_name = ? AND (end_pos = 0 OR start_pos >= end_pos)`, sourceName).Scan(&bad); err != nil {
		t.Fatalf("position probe: %v", err)
	}
	if bad != 0 {
		t.Errorf("%d rows carry impossible positions: the stream did not run as mariadb", bad)
	}
	var annotate string
	if err := sourceDB.QueryRow("SELECT @@binlog_annotate_row_events").Scan(&annotate); err == nil && annotate == "1" {
		var withText int
		if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events
			WHERE schema_name = ? AND query_text IS NOT NULL`, sourceName).Scan(&withText); err != nil {
			t.Fatalf("query_text probe: %v", err)
		}
		if withText == 0 {
			t.Error("binlog_annotate_row_events is ON but no row carries its statement text: ANNOTATE_ROWS was not requested")
		}
	}
}

// TestOne_MariaDB_declaredMySQLRefuses: declaring mysql for a MariaDB server
// refuses, naming both flavors.
func TestOne_MariaDB_declaredMySQLRefuses(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	_, sourceName := mariadbFlavorSource(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := One(ctx, flavorTestConfig(indexName, mariadbSourceDSN(sourceName), sourceName, "mysql", 99986))
	var mm *metadata.FlavorMismatchError
	if !errors.As(err, &mm) {
		t.Fatalf("want *metadata.FlavorMismatchError, got %v", err)
	}
	if mm.Declared != "mysql" || mm.Detected != "mariadb" {
		t.Errorf("mismatch = declared %q detected %q, want mysql/mariadb", mm.Declared, mm.Detected)
	}
	t.Logf("refusal: %v", err)
	if f := storedFlavor(t, indexDB); f != "" {
		t.Errorf("a refusal wrote a checkpoint with flavor %q", f)
	}
}

// TestOne_MariaDB_resumeOldMySQLDefaultCheckpoint: a MariaDB that an older
// build captured under the old "mysql" default left a POSITION checkpoint
// saying mysql. Detection says mariadb: the stream resumes at the saved
// position as mariadb and the next checkpoint records mariadb.
func TestOne_MariaDB_resumeOldMySQLDefaultCheckpoint(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := mariadbFlavorSource(t)

	file, pos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	testutil.MustExec(t, indexDB, `INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, flavor, last_checkpoint, server_id)
		VALUES (1, 'position', ?, ?, 'mysql', UTC_TIMESTAMP(), 99987)`, file, pos)

	// Written before the resume: the saved position is behind them, so they
	// are replayed.
	for i := range 2 {
		testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (?)", float64(i+1))
	}

	cfg := flavorTestConfig(indexName, mariadbSourceDSN(sourceName), sourceName, "", 99987)
	if err := runOneUntil(t, cfg, false, nil, func() bool {
		return indexedOrders(t, indexDB, sourceName) >= 2 && storedFlavor(t, indexDB) == metadata.FlavorMariaDB
	}); err != nil {
		t.Fatalf("One: %v", err)
	}
	if f := storedFlavor(t, indexDB); f != metadata.FlavorMariaDB {
		t.Errorf("stream_state.flavor = %q after resume, want mariadb", f)
	}
}
