//go:build integration

package oracle_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/byos"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/oracle"
	"github.com/dbtrail/dbtrail/internal/streamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Defaults sized for the CI shard: the whole test well under 150 s on the
// integration box. BINTRAIL_ORACLE_SEEDS / BINTRAIL_ORACLE_STEPS raise them
// for a long run; BINTRAIL_ORACLE_SEED replays one seed.
const (
	defaultSeeds = 3
	defaultSteps = 12
)

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("%s=%q: want a positive integer", name, v)
	}
	return n
}

// TestOracle is the #2499 oracle over the real pipeline: a schema per seed,
// real capture (streamrun.One), the generator's DML, and at every cut a
// snapshot or a refresh with table deltas, then the port's read of the copy
// compared with the source.
//
// It skips without MySQL unless BINTRAIL_ORACLE_REQUIRE=1.
func TestOracle(t *testing.T) {
	if os.Getenv("BINTRAIL_ORACLE_REQUIRE") == "1" {
		t.Setenv("BINTRAIL_REQUIRE_MYSQL", "1")
	}
	testutil.SkipIfNoMySQL(t)

	steps := envInt(t, "BINTRAIL_ORACLE_STEPS", defaultSteps)
	var seeds []uint64
	if one := os.Getenv("BINTRAIL_ORACLE_SEED"); one != "" {
		n, err := strconv.ParseUint(one, 10, 64)
		if err != nil {
			t.Fatalf("BINTRAIL_ORACLE_SEED=%q: %v", one, err)
		}
		seeds = []uint64{n}
	} else {
		for i := 1; i <= envInt(t, "BINTRAIL_ORACLE_SEEDS", defaultSeeds); i++ {
			seeds = append(seeds, uint64(i))
		}
	}
	tables := oracle.Tables()
	if names := os.Getenv("BINTRAIL_ORACLE_TABLES"); names != "" {
		tables = nil
		for _, n := range strings.Split(names, ",") {
			tables = append(tables, oracle.TableByName(strings.TrimSpace(n)))
		}
	}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			start := time.Now()
			res := runSeed(t, seed, steps, tables)
			t.Logf("seed %d: %d steps, %d ops, steps applied %v (%d fallbacks), %.1fs wall",
				seed, res.Steps, res.Ops, res.Applied, res.Fallbacks, time.Since(start).Seconds())
		})
	}
}

func runSeed(t *testing.T, seed uint64, steps int, tables []oracle.Table) oracle.Result {
	t.Helper()
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	hour := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, indexDB, indexName, []time.Time{
		hour.Add(-2 * time.Hour), hour.Add(-time.Hour), hour, hour.Add(time.Hour), hour.Add(2 * time.Hour),
	})
	for _, tb := range tables {
		testutil.MustExec(t, sourceDB, tb.DDL)
	}
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	indexDSN := testutil.IntegrationDSN(indexName)
	stop := startCapture(t, sourceDB, indexDB, indexDSN, sourceName, 92499+uint32(seed%1000))
	defer stop()

	// Not t.TempDir(): its name carries the subtest's "seed=N", and DuckDB
	// reads a "key=value" path segment as a hive partition column, which the
	// fold then reports as a column the CREATE TABLE lacks.
	root, err := os.MkdirTemp("", "oracle-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	snapshot := &oracle.SnapshotStep{Root: root, Source: sourceDB, W: 1}
	refresh := &oracle.RefreshStep{IndexDSN: indexDSN, Root: root, W: 3}
	run := &oracle.Run{
		Seed: seed, Steps: steps, Schema: sourceName, Tables: tables,
		Source: sourceDB, Index: indexDB, Root: root,
		Between: []oracle.Step{snapshot, refresh}, Fallback: snapshot,
		Logf: t.Logf,
	}
	res, err := run.Do(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// startCapture runs streamrun.One on the source into the index and waits
// for its first checkpoint: with none saved, One attaches at the source's
// CURRENT position, so writes before that checkpoint would never be
// streamed.
func startCapture(t *testing.T, sourceDB, indexDB *sql.DB, indexDSN, schema string, serverID uint32) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- streamrun.One(ctx, streamrun.Config{
			IndexDSN:   indexDSN,
			SourceDSN:  testutil.IntegrationDSN(schema),
			ServerID:   serverID,
			BatchSize:  200,
			Schemas:    schema,
			Checkpoint: 1,
			GapTimeout: 30,
			Format:     "text",
			SSLMode:    "preferred",
			Deps: streamrun.Deps{
				ValidateBinlogFormat:   metadata.ValidateBinlogFormat,
				ValidateBinlogRowImage: metadata.ValidateBinlogRowImage,
				ValidateNoFKCascades:   metadata.ValidateNoFKCascades,
				ParseSchemaList:        cliutil.ParseSchemaList,
				ResolveServerIdentity:  byos.ResolveServerIdentity,
				EnsureResolver:         metadata.EnsureResolver,
				BuildIndexFilters:      cliutil.BuildIndexFilters,
				InsertSchemaChange:     indexer.InsertSchemaChange,
				ParseSourceDSN:         config.ParseSourceDSN,
				OutputJSON:             cliutil.OutputJSON,
				DetectSourceFlavor:     metadata.DetectSourceFlavor,
			},
		})
	}()
	stop = func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(30 * time.Second):
			t.Error("the capture did not stop")
		}
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		if _, ok, err := oracle.CheckpointPosition(context.Background(), indexDB); err == nil && ok {
			return stop
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
}
