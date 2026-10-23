//go:build integration

package console

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/oracle"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// The correctness oracle (#2499), slice 2: the port's tails. The REAL
// keeper (tailKeeper over indexTailSource) follows the real index that the
// real capture fills from a real source, and the copy is read the way a
// statement on the MySQL port reads it (statementTails over the port's
// views input). It lives in this package because the keeper and the read
// are package-private on purpose: the oracle drives them as the port does,
// not a re-implementation.
//
// Two scenarios. Mixed: snapshot, refresh and tail steps drawn by weight,
// so a tail is laid over a fresh snapshot (a chain of one file) and over a
// delta chain, rebuilt on a new cut after a refresh, and holds a key
// changed in two tail steps and a key deleted then re-inserted across them.
// Chain: tails almost only, so the chain of files grows to its merge and
// merges, and the read sees the merged chain.
//
// A table the port answers as of the published cut (no tail) is compared
// with the rows read at that cut, AND the step asserts why there is no
// tail: a key the tail does not support (only integer keys have one, the
// product's own rule: t_text's VARCHAR key), or no change to the table since
// the cut. Any other reason fails the run naming it: that is the detector
// for tails silently off.

func TestOracleTails(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	if os.Getenv("BINTRAIL_ORACLE_REQUIRE") == "" && os.Getenv("BINTRAIL_ORACLE_SEEDS") == "" {
		t.Skip("the tails oracle runs on request: BINTRAIL_ORACLE_SEEDS=N or BINTRAIL_ORACLE_REQUIRE=1 (it takes about a minute per seed)")
	}
	seeds := oracleEnvInt(t, "BINTRAIL_ORACLE_SEEDS", 2)
	steps := oracleEnvInt(t, "BINTRAIL_ORACLE_STEPS", 12)
	first := uint64(oracleEnvInt(t, "BINTRAIL_ORACLE_SEED", 1))
	tables := oracle.Tables()
	if names := os.Getenv("BINTRAIL_ORACLE_TABLES"); names != "" {
		tables = nil
		for _, n := range strings.Split(names, ",") {
			tables = append(tables, oracle.TableByName(strings.TrimSpace(n)))
		}
	}
	t.Run("mixed", func(t *testing.T) {
		for seed := first; seed < first+uint64(seeds); seed++ {
			t.Run("seed="+strconv.FormatUint(seed, 10), func(t *testing.T) {
				res, chains := runTailSeed(t, seed, steps, tables, 1, 3, 4)
				if res.Applied["tail"] == 0 {
					t.Fatalf("no tail step applied in %d steps: %v", steps, res.Applied)
				}
				t.Logf("seed %d: %d steps, %d ops, applied %v, chains up to %d files, in %s", seed, res.Steps, res.Ops, res.Applied, chains.longest, res.Wall.Round(time.Millisecond))
			})
		}
	})
	t.Run("chain", func(t *testing.T) {
		// Tails almost only: ~15 consecutive tail steps over one snapshot.
		res, chains := runTailSeed(t, first, 16, tables, 1, 0, 40)
		if chains.longest < 2 {
			t.Fatalf("the chain never grew past one file in %d tail steps: %v", res.Applied["tail"], res.Applied)
		}
		if !chains.merged {
			t.Fatalf("the chain grew to %d files and never merged in %d tail steps; the merge (at 8 files or twice the rows) was not exercised", chains.longest, res.Applied["tail"])
		}
		if chains.longest > 8 {
			t.Fatalf("a chain of %d files was read; the keeper merges at 8", chains.longest)
		}
		t.Logf("chain: %d steps, applied %v, longest chain %d files, merged", res.Steps, res.Applied, chains.longest)
	})
}

func oracleEnvInt(t *testing.T, name string, def int) int {
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

// chainWatch is what the reads saw of the tails' chains: the longest, and
// whether one shrank after growing OVER THE SAME CUT (a merge). A new
// snapshot or refresh starts the chain again from one file, which is not
// a merge, so the memory is per table and per published cut.
type chainWatch struct {
	longest int
	merged  bool
	last    map[string]int
	cuts    map[string]string
}

func (w *chainWatch) see(table, cut string, files int) {
	if w.last == nil {
		w.last, w.cuts = map[string]int{}, map[string]string{}
	}
	if files > w.longest {
		w.longest = files
	}
	if w.cuts[table] == cut {
		if prev := w.last[table]; prev >= 2 && files < prev {
			w.merged = true
		}
	}
	w.last[table], w.cuts[table] = files, cut
}

// tailStep brings the keeper's tails up to date at the cut, as the port's
// follower does between statements. It publishes nothing: the copy's
// published cut stays the last snapshot's or refresh's.
type tailStep struct {
	t      *testing.T
	s      *Server
	k      *tailKeeper
	root   string
	w      int
	tables map[string]oracle.Table
}

func (s *tailStep) Name() string    { return "tail" }
func (s *tailStep) Weight() int     { return s.w }
func (s *tailStep) Publishes() bool { return false }

func (s *tailStep) Apply(ctx context.Context, cut *oracle.Cut) error {
	in, _, err := oracle.PortInput(ctx, s.root, cut.At, false)
	if err != nil {
		return err
	}
	// As the follower: the tables a statement asked for, then one update
	// (its builds run off the lock) and the one that installs them.
	s.k.published(in.Baselines)
	for range 2 {
		if !s.k.followOnce(ctx, tailFollowTimeout) {
			return fmt.Errorf("the follower had no table to bring up to date")
		}
		s.k.settle()
	}
	// followOnce reports an update that failed (the index, its deadline)
	// only to the keeper's note and the log, and a build that failed only
	// to its retry clock: read both, or a failure here would surface steps
	// later as "no tail" with its reason lost.
	s.k.noteMu.Lock()
	failing := s.k.failing
	s.k.noteMu.Unlock()
	if failing != "" {
		return fmt.Errorf("the follower could not bring the tails up to date: %s", failing)
	}
	s.k.mu.Lock()
	defer s.k.mu.Unlock()
	for _, tb := range in.Baselines {
		if b := s.k.builds[tailKey(tb.Schema, tb.Table)]; b != nil && b.failures > 0 && !b.running {
			return fmt.Errorf("the tail of %s.%s could not be built (%d failures in a row; DBTrail's log has the error)", tb.Schema, tb.Table, b.failures)
		}
	}
	return nil
}

// read is the port's read of the copy at cut: the views input with each
// table's tail laid over it where the port would lay one, and the tables
// it answers as of the published cut, each with its reason checked.
func (s *tailStep) read(ctx context.Context, cut *oracle.Cut, wallClock bool, watch *chainWatch) (oracle.Read, error) {
	in, dir, err := oracle.PortInput(ctx, s.root, cut.At, wallClock)
	if err != nil {
		return oracle.Read{}, err
	}
	rd := oracle.Read{SnapshotDir: dir, AsPublished: map[string]bool{}}
	for i, tb := range in.Baselines {
		// One statement reads one table: per table is the port's
		// all-or-nothing.
		out, _, ok := s.s.statementTails(ctx, s.k, in.Baselines, []views.BaselineTable{tb})
		if ok && out[i].Tail != nil {
			in.Baselines[i].Tail = out[i].Tail
			watch.see(tb.Table, tailCutOf(tb), len(out[i].Tail.Paths))
			continue
		}
		rd.AsPublished[tb.Table] = true
		if why := s.whyNoTail(tb, cut); why != "" {
			return oracle.Read{}, fmt.Errorf("%s.%s answered as of the published cut: %s", tb.Schema, tb.Table, why)
		}
	}
	rd.SQL = views.Generate(in)
	return rd, nil
}

// whyNoTail is "" when the port legitimately has no tail over tb at cut: the
// table's key is not one a tail supports (the product's own rule, asked of
// the source's CREATE TABLE: t_text's VARCHAR key), or nothing changed the
// table since that cut (an empty tail is retired). Anything else is the
// reason, as a failure — a tail refused for a PK-changing UPDATE included:
// since #2501 the tail folds such an UPDATE as a DELETE of the old key and an
// INSERT of the new one, so that refusal is a finding, not the generator's
// doing.
func (s *tailStep) whyNoTail(tb views.BaselineTable, cut *oracle.Cut) string {
	s.k.mu.Lock()
	e := s.k.tables[tailKey(tb.Schema, tb.Table)]
	var refused string
	keys := -1
	if e != nil {
		refused = e.refused
		if e.tail != nil {
			keys = e.tail.Keys()
		}
	}
	s.k.mu.Unlock()
	switch {
	case cut.Step == cut.PublishedStep:
		// The read right after a snapshot or a refresh: the copy as of the
		// published cut IS the cut, and the keeper's entry may still be the
		// old window's (its rebuild over the new cut runs off the lock and
		// installs on the next update), so there is nothing to verify.
		return ""
	case refused != "" && refused == s.keyRefusal(tb):
		return ""
	case refused != "":
		// Before the untouched case on purpose: a refusal for a reason the
		// generator did not cause is a finding even over a table no op
		// touched since the cut.
		return "the tail is refused: " + refused
	case !cut.TouchedSincePublish(tb.Table):
		return ""
	case e == nil:
		return "the keeper has no entry for the table, and ops since the published cut changed it"
	default:
		return fmt.Sprintf("no tail laid over the table (the keeper holds %d keys), and ops since the published cut changed it", keys)
	}
}

// keyRefusal is what the product refuses tb's tail with for its key, or ""
// when its key supports a tail.
func (s *tailStep) keyRefusal(tb views.BaselineTable) string {
	ot, ok := s.tables[tb.Table]
	if !ok {
		s.t.Fatalf("%s is not a table of the run", tb.Table)
	}
	// The definition the keeper itself reads: the one in the copy's footer.
	md, err := baseline.ReadParquetMetadata(tb.Path)
	if err != nil {
		s.t.Fatalf("read the footer of %s: %v", tb.Path, err)
	}
	cols, err := baseline.ParseSchemaText(md.CreateTableSQL)
	if err != nil {
		s.t.Fatalf("parse the CREATE TABLE of %s from its footer: %v", tb.Table, err)
	}
	_, why := reconstruct.TailKeyRefusal(tb.Schema+"."+tb.Table, cols, ot.PK)
	return why
}

// runTailSeed runs one seed with snapshot, refresh and tail steps at the
// given weights (a weight of 0 leaves the step out).
func runTailSeed(t *testing.T, seed uint64, steps int, tables []oracle.Table, wSnapshot, wRefresh, wTail int) (oracle.Result, *chainWatch) {
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
	captureErr, stopCapture, err := oracle.StartCapture(ctx, indexDB, indexDSN, testutil.IntegrationDSN(sourceName), sourceName, 93499+uint32(seed%1000))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stopCapture(); err != nil {
			t.Error(err)
		}
	}()

	// Not t.TempDir(): its name carries "seed=N", which DuckDB reads as a
	// hive partition column.
	root, err := os.MkdirTemp("", "oracle-tails-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })

	// The port's keeper over the real index, as tail_source_2369's rig
	// builds it, with the resolver the capture's schema snapshot gives it
	// and a budget no step reaches.
	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	s := &Server{cm: newConnManager(nil, false), captureStatus: &fakeWatermark{ago: 5 * time.Second}}
	b := &bundle{db: indexDB, dbName: indexName, engine: query.New(indexDB), baselineSrc: root, resolver: resolver}
	s.cm.boot = b
	// The keeper's parent is not t.TempDir() either: its tail files are read
	// with read_parquet, and a "seed=N" segment above them would be a hive
	// partition column.
	keeperDir, err := os.MkdirTemp("", "oracle-tails-keeper-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(keeperDir) })
	k := newTailKeeper(newIndexTailSource(s, b).capturing(nil), keeperDir, tailBudget{Keys: 1_000_000}, time.Minute, time.Hour)
	defer k.close()

	snapshot := &oracle.SnapshotStep{Root: root, Source: sourceDB, W: wSnapshot}
	tail := &tailStep{t: t, s: s, k: k, root: root, w: wTail, tables: map[string]oracle.Table{}}
	for _, tb := range tables {
		tail.tables[tb.Name] = tb
	}
	between := []oracle.Step{snapshot}
	if wRefresh > 0 {
		between = append(between, &oracle.RefreshStep{IndexDSN: indexDSN, Root: root, W: wRefresh})
	}
	between = append(between, tail)
	watch := &chainWatch{}
	run := &oracle.Run{
		Seed: seed, Steps: steps, Schema: sourceName, Tables: tables,
		Source: sourceDB, Index: indexDB, Capture: captureErr, Root: root,
		Between: between,
		Read: func(ctx context.Context, cut *oracle.Cut, wallClock bool) (oracle.Read, error) {
			return tail.read(ctx, cut, wallClock, watch)
		},
		Logf: t.Logf,
	}
	res, err := run.Do(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return res, watch
}
