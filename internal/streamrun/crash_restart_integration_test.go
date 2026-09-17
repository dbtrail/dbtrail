//go:build integration

// Crash→restart acceptance coverage for #500 (exactly-once stream indexing).
//
// The dedup CODE landed in #874 (deleteEventsSinceCheckpoint /
// deleteEventsSinceCheckpointGTID, called from One before StartSync). What was
// missing — and what this file adds — is the issue's own acceptance criteria:
//
//  1. A real kill-mid-stream test: stream into the partitioned binlog_events,
//     stop the process WITHOUT letting the checkpoint become durable, restart
//     through One(), and assert no duplicate AND no lost rows. POSITION mode
//     gets the full cycle; GTID mode gets mode selection + dedup against a real
//     database, but not the re-stream (and not the dedup/StartSync ordering) —
//     see TestIntegrationResumeGTIDDedupsBeforeStartSync for why no
//     GTID-capable source is reachable from this suite.
//  2. The `start_pos >= pos` boundary, constructed against a real DB instead of
//     reasoned by inspection.
//
// Either way this reaches what the sqlmock tests in streamrun_test.go cannot:
// they assert the emitted DELETE string against a mocked RowsAffected and never
// touch the partitioned table or One()'s mode selection. Those tests are left
// untouched — this is additional coverage, not a replacement.
package streamrun

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/byos"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// ─── Simulating an ungraceful stop ───────────────────────────────────────────
//
// A crash is only observable to the next process through the DATABASE: rows are
// already flushed to binlog_events (batches flush independently of the
// checkpoint ticker) while stream_state still names an older, durable position.
// Reproducing that in-process needs the checkpoint write to not stick — killing
// the goroutine is not enough, because streamLoop's ctx.Done branch flushes AND
// checkpoints on the way out, which is a *graceful* shutdown.
//
// So the crash is staged with a BEFORE UPDATE trigger on stream_state that
// SIGNALs: every saveCheckpoint during that run fails (warn-only by design, see
// TestStreamLoop_saveCheckpointFailureDoesNotAbort) while the previously saved
// row survives untouched. Rows keep flowing into binlog_events. When the run
// ends, the durable checkpoint is exactly where it was before it started — the
// same state a `kill -9` between a flush and the next checkpoint leaves behind.
// It uses only production code paths and, unlike a timing-based kill, it is
// deterministic.
//
// One() writes stream_state ONLY through streamLoop's saveCheckpoint on a plain
// resume (persistResetDiscard needs --reset; persistGapAutoAdvance needs an
// unfillable gap), so the trigger cannot fire anywhere else in these tests.

const blockCheckpointTriggerName = "bintrail_test_block_checkpoint"

// blockCheckpoints installs the crash simulation and returns a func that lifts
// it again (the "process restarts" step).
func blockCheckpoints(t *testing.T, indexDB *sql.DB) func() {
	t.Helper()
	testutil.MustExec(t, indexDB, "DROP TRIGGER IF EXISTS "+blockCheckpointTriggerName)
	testutil.MustExec(t, indexDB, `
		CREATE TRIGGER `+blockCheckpointTriggerName+` BEFORE UPDATE ON stream_state
		FOR EACH ROW SIGNAL SQLSTATE '45000'
		    SET MESSAGE_TEXT = 'simulated crash: checkpoint never became durable'`)
	lifted := false
	t.Cleanup(func() {
		if !lifted {
			indexDB.Exec("DROP TRIGGER IF EXISTS " + blockCheckpointTriggerName)
		}
	})
	return func() {
		lifted = true
		testutil.MustExec(t, indexDB, "DROP TRIGGER IF EXISTS "+blockCheckpointTriggerName)
	}
}

// ─── Stream harness ──────────────────────────────────────────────────────────

// testStreamDeps mirrors streamdeps.Default(). It is inlined rather than
// imported because streamdeps imports streamrun (the DI seam runs that way on
// purpose), so importing it from a streamrun test would be an import cycle.
func testStreamDeps() Deps {
	return Deps{
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
	}
}

// runOneUntil runs a single streamrun.One in a goroutine, performs the source
// writes, then polls done until it reports true and stops the stream.
//
// waitAttached matters only on a FIRST run: with no checkpoint, One
// auto-discovers the source's CURRENT binlog position, so rows written before
// it attaches are never streamed — the caller must wait for the first
// successful checkpoint (the ticker fires it even with zero events). On a
// RESUME the start position comes from the saved checkpoint, so writes may be
// issued immediately; they are replayed whenever the syncer catches up. That
// distinction is load-bearing here, not cosmetic: during the crash run every
// checkpoint save fails by design, so an attach signal would never arrive and
// waiting for one would deadlock.
func runOneUntil(t *testing.T, cfg Config, waitAttached bool, writes func(), done func() bool) error {
	t.Helper()

	var once sync.Once
	var connected atomic.Bool
	attached := make(chan struct{})
	cfg.Hooks = &Hooks{
		OnSourceConnected: func() { connected.Store(true) },
		OnCheckpoint: func() {
			if !connected.Load() {
				t.Error("OnCheckpoint fired before OnSourceConnected: the first-run list would show capture started before the connection")
			}
			once.Do(func() { close(attached) })
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- One(ctx, cfg) }()

	deadline := time.After(120 * time.Second)
	if waitAttached {
		select {
		case <-attached:
		case err := <-errCh:
			return fmt.Errorf("stream exited before it attached: %w", err)
		case <-deadline:
			cancel()
			<-errCh
			t.Fatal("timed out waiting for the stream to attach")
		}
	}

	if writes != nil {
		writes()
	}

	for !done() {
		select {
		case err := <-errCh:
			return fmt.Errorf("stream exited before the expected rows were indexed: %w", err)
		case <-deadline:
			cancel()
			<-errCh
			t.Fatal("timed out waiting for the expected rows to be indexed")
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()
	return <-errCh
}

// ─── Assertions ──────────────────────────────────────────────────────────────

// indexedPKs returns pk_values → row count for one source table. Counting per
// PK (not a bare COUNT(*)) is what makes this an exactly-once assertion: a
// total alone cannot tell one duplicate plus one lost row from a clean run.
func indexedPKs(t *testing.T, db *sql.DB, schema, table string) map[string]int {
	t.Helper()
	rows, err := db.Query(`
		SELECT pk_values, COUNT(*) FROM binlog_events
		WHERE schema_name = ? AND table_name = ? GROUP BY pk_values`, schema, table)
	if err != nil {
		t.Fatalf("query indexed pks: %v", err)
	}
	defer rows.Close()
	counts := make(map[string]int)
	for rows.Next() {
		var pk string
		var n int
		if err := rows.Scan(&pk, &n); err != nil {
			t.Fatalf("scan indexed pk: %v", err)
		}
		counts[pk] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate indexed pks: %v", err)
	}
	return counts
}

// assertExactlyOnce fails when any expected PK is missing (data LOST) or
// present more than once (data DUPLICATED), or when an unexpected PK appears.
//
// PRECONDITION: it groups by pk_values alone, so "exactly one indexed row per
// PK" is the right invariant only while every source row is INSERT-only and
// touched exactly once — which is what these tests' fixtures do. A fixture that
// UPDATEs or DELETEs a row would legitimately produce a second event for that
// PK, and this helper would report it as DUPLICATED. Any such fixture needs the
// counts keyed by (pk_values, event_id) or filtered by event_type instead.
func assertExactlyOnce(t *testing.T, counts map[string]int, wantPKs []string) {
	t.Helper()
	for _, pk := range wantPKs {
		switch n := counts[pk]; {
		case n == 0:
			t.Errorf("pk %s: LOST — expected exactly 1 indexed row, got 0", pk)
		case n > 1:
			t.Errorf("pk %s: DUPLICATED — expected exactly 1 indexed row, got %d", pk, n)
		}
	}
	want := make(map[string]bool, len(wantPKs))
	for _, pk := range wantPKs {
		want[pk] = true
	}
	var extra []string
	for pk := range counts {
		if !want[pk] {
			extra = append(extra, pk)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		t.Errorf("unexpected indexed pks %v (want exactly %v)", extra, wantPKs)
	}
}

func pkRange(lo, hi int) []string {
	out := make([]string, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		out = append(out, strconv.Itoa(i))
	}
	return out
}

// ─── Acceptance criterion #1: kill mid-stream, restart, no dups, no loss ─────

// TestIntegrationCrashRestartExactlyOnce_position is acceptance criterion #1
// of #500: kill a live stream without letting the checkpoint flush, restart it
// through One(), and prove the re-received events are neither duplicated nor
// lost. Three phases, all through the SAME One() entry point the CLI uses:
//
//	run 1  clean: index pks 1-5, checkpoint durably.
//	run 2  crash: index pks 6-10 while every checkpoint write fails, so the
//	       durable checkpoint stays at run 1's — rows now sit BEYOND it.
//	run 3  restart: One() must delete those stragglers before StartSync and
//	       let replication re-deliver them, ending with 1-12 exactly once.
//
// Without the dedup, run 3 re-receives 6-10 from the stale checkpoint and
// re-inserts them under fresh event_ids: 5 duplicates, and this test fails.
func TestIntegrationCrashRestartExactlyOnce_position(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(sourceName)
	const serverIDBase = 99870

	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY,
		amount INT NOT NULL
	)`)

	insert := func(lo, hi int) func() {
		return func() {
			for i := lo; i <= hi; i++ {
				testutil.MustExec(t, sourceDB, "INSERT INTO orders (id, amount) VALUES (?, ?)", i, i*10)
			}
		}
	}
	indexedThrough := func(hi int) func() bool {
		return func() bool {
			var n int
			if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events
				WHERE schema_name = ? AND table_name = 'orders' AND pk_values = ?`,
				sourceName, strconv.Itoa(hi)).Scan(&n); err != nil {
				t.Fatalf("poll indexed pk %d: %v", hi, err)
			}
			return n > 0
		}
	}

	baseCfg := func(serverID uint32) Config {
		return Config{
			IndexDSN:   testutil.IntegrationDSN(indexName),
			SourceDSN:  sourceDSN,
			Flavor:     gomysql.MySQLFlavor,
			ServerID:   serverID,
			BatchSize:  1, // flush every event: rows land in binlog_events immediately
			Schemas:    sourceName,
			Checkpoint: 1,
			GapTimeout: 30,
			Format:     "text",
			SSLMode:    "preferred",
			Deps:       testStreamDeps(),
		}
	}

	// ── run 1: clean, establishes the durable checkpoint ──────────────────
	// No --start-file/--start-gtid and no checkpoint: One auto-discovers the
	// start point. GTID discovery is tried first (#1131), but this suite's
	// MySQL runs gtid_mode=OFF, so CurrentGTIDExecuted returns "" and One
	// falls back to position discovery and runs in POSITION mode. (On a
	// gtid_mode=ON source a fresh run would checkpoint in GTID mode instead —
	// covered by unit tests with stubbed discovery callbacks.)
	cfg1 := baseCfg(serverIDBase)
	if err := runOneUntil(t, cfg1, true, insert(1, 5), indexedThrough(5)); err != nil {
		t.Fatalf("run 1 (clean): %v", err)
	}

	durable, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState after run 1: %v", err)
	}
	if durable == nil {
		t.Fatal("run 1 saved no checkpoint — the crash simulation would be meaningless")
	}
	if durable.mode != "position" {
		t.Fatalf("run 1 checkpoint mode = %q, want \"position\" — the test is not exercising the intended One() branch",
			durable.mode)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 5))

	// ── run 2: the crash ──────────────────────────────────────────────────
	restart := blockCheckpoints(t, indexDB)
	cfg2 := baseCfg(serverIDBase + 1)
	if err := runOneUntil(t, cfg2, false, insert(6, 10), indexedThrough(10)); err != nil {
		t.Fatalf("run 2 (crash): %v", err)
	}

	crashed, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState after run 2: %v", err)
	}
	if crashed.binlogFile != durable.binlogFile || crashed.binlogPos != durable.binlogPos ||
		crashed.gtidSet != durable.gtidSet {
		t.Fatalf("run 2 advanced the durable checkpoint (%s:%d gtid=%q → %s:%d gtid=%q); "+
			"the crash was not simulated and run 3 would have nothing to dedup",
			durable.binlogFile, durable.binlogPos, durable.gtidSet,
			crashed.binlogFile, crashed.binlogPos, crashed.gtidSet)
	}
	// Pre-condition for the restart: pks 6-10 are indexed but sit beyond the
	// durable checkpoint. This is the exact state a kill -9 leaves behind.
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 10))

	// ── run 3: restart — dedup, replay, and keep streaming ────────────────
	restart()
	cfg3 := baseCfg(serverIDBase + 2)
	// Waiting on pk 12 — written only during run 3, so it sits AFTER 6-10 in
	// the binlog — is what makes the final assertion race-free. Sampling on
	// "10 rows present" alone would pass trivially at run 3's first instant,
	// before the dedup ran; once 12 is indexed, the stale 6-10 have provably
	// been deleted and re-delivered, because the binlog is sequential.
	if err := runOneUntil(t, cfg3, false, insert(11, 12), indexedThrough(12)); err != nil {
		t.Fatalf("run 3 (restart): %v", err)
	}

	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 12))

	// No-loss also means the payload survived the delete/re-insert round trip.
	var amount sql.NullString
	if err := indexDB.QueryRow(`SELECT JSON_EXTRACT(row_after, '$.amount') FROM binlog_events
		WHERE schema_name = ? AND table_name = 'orders' AND pk_values = '7'`,
		sourceName).Scan(&amount); err != nil {
		t.Fatalf("read re-inserted row 7: %v", err)
	}
	if amount.String != "70" {
		t.Errorf("re-inserted row 7 amount = %q, want \"70\" (row image lost across the replay)", amount.String)
	}
}

// TestIntegrationResumeGTIDDedupsBeforeStartSync is the GTID-mode half of
// acceptance criterion #1. It stops short of a full crash→restart→re-stream
// cycle, and deliberately so: GTID mode needs a GTID-enabled source, and none
// is reachable here. The MySQL container this suite uses — locally and in the
// CI integration matrix — runs gtid_mode=OFF, and flipping that global
// mid-suite would perturb every other test sharing the server. MariaDB (always
// GTID-capable) is disqualified for a different reason: on 11.4+ every
// intra-transaction row event arrives with LogPos=0, so start_pos/end_pos land
// in the index as garbage (start_pos=2^64-EventSize) and no position-keyed
// assertion can mean anything there. See the separate issue filed for that.
//
// What it DOES pin against a real database — and what the sqlmock tests in
// streamrun_test.go never reach — is the part of One() that chooses what to do:
//
//   - mode selection from a real stream_state row (saved.mode == "gtid" with no
//     --start-gtid flag ⇒ the GTID branch, not the position branch),
//   - the real deleteEventsSinceCheckpointGTID against the real partitioned
//     binlog_events: the position cut, the open-transaction GTID sweep, and the
//     rows that must SURVIVE both.
//
// The SURVIVING-ROW SET is what proves the GTID branch was taken. Had One gone
// down the position branch, deleteEventsSinceCheckpoint(binlog.000042, 5000)
// would have removed "beyond-checkpoint" (start_pos 5000 >= 5000) and left
// "open-transaction" (start_pos 3000) alive, because only the GTID sibling's
// second pass sweeps a below-offset row whose GTID the saved set lacks. The
// assertion at the bottom would then fail with three survivors instead of two.
//
// What this test does NOT prove, despite its name, is the ORDER of the dedup
// relative to StartSyncGTID. "BeforeStartSync" names where the dedup sits in
// One(), not something observed here: the assertion runs after One has fully
// returned, so either order would leave the same rows behind. In particular
// StartSyncGTID SUCCEEDS against this gtid_mode=OFF source — go-mysql writes
// COM_BINLOG_DUMP_GTID and hands back a streamer without reading the reply, so
// the 1236 arrives asynchronously on the first GetEvent and One surfaces it
// from the streaming loop, well past syncer setup. A run log shows exactly
// that: "deleted events at or beyond the saved checkpoint ... rows_deleted=2",
// then "begin to sync binlog from GTID set", "Connected to server
// flavor=mysql", "Streaming started", then streamLoop's own ticker logging
// `checkpoint saved file="" pos=0`. The ordering itself is pinned by One's
// source and by the sqlmock tests in streamrun_test.go.
func TestIntegrationResumeGTIDDedupsBeforeStartSync(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestDB(t)
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY,
		amount INT NOT NULL
	)`)

	const (
		uuid           = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
		checkpointFile = "binlog.000042"
		earlierFile    = "binlog.000041"
		checkpointPos  = 5000
	)
	inSet := uuid + ":2"   // committed before the checkpoint
	openTx := uuid + ":9"  // flushed, but never durably checkpointed
	beyond := uuid + ":10" // at/after the checkpoint offset

	// The saved checkpoint: GTID mode, a durable set of :1-3, positioned at
	// checkpointFile:5000. saveCheckpoint is the production writer, so this is
	// byte-for-byte the row a real crashed stream would leave behind.
	if err := saveCheckpoint(indexDB, &streamState{
		mode:       "gtid",
		binlogFile: checkpointFile,
		binlogPos:  checkpointPos,
		gtidSet:    uuid + ":1-3",
		flavor:     gomysql.MySQLFlavor,
		serverID:   99890,
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	const ts = "2026-02-19 14:00:00"
	testutil.InsertEvent(t, indexDB, earlierFile, 9000, 9100, ts, &inSet, sourceName, "orders", 1, "earlier-file", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, indexDB, checkpointFile, 1000, 2000, ts, &inSet, sourceName, "orders", 1, "committed", nil, nil, []byte(`{"id":2}`))
	testutil.InsertEvent(t, indexDB, checkpointFile, 3000, 4000, ts, &openTx, sourceName, "orders", 1, "open-transaction", nil, nil, []byte(`{"id":3}`))
	testutil.InsertEvent(t, indexDB, checkpointFile, 5000, 6000, ts, &beyond, sourceName, "orders", 1, "beyond-checkpoint", nil, nil, []byte(`{"id":4}`))

	// A bounded context: if a future source DOES speak GTID, One would attach
	// and tail forever instead of returning. The assertions below are on the
	// database, so either exit is fine.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := One(ctx, Config{
		IndexDSN:   testutil.IntegrationDSN(indexName),
		SourceDSN:  testutil.IntegrationDSN(sourceName),
		Flavor:     gomysql.MySQLFlavor,
		ServerID:   99891,
		BatchSize:  1,
		Schemas:    sourceName,
		Checkpoint: 1,
		GapTimeout: 30,
		Format:     "text",
		SSLMode:    "preferred",
		Deps:       testStreamDeps(),
	})

	// gtid_mode=OFF ⇒ the server refuses the GTID (AUTO_POSITION) dump:
	//   ERROR 1236 ... cannot start in AUTO_POSITION mode: this server has
	//   GTID_MODE = OFF instead of ON
	// This is corroboration, not the proof: it establishes only that the run
	// went down a GTID-exclusive path and then stopped, NOT how far it got.
	// detectGTIDGap runs BEFORE the dedup and has GTID-flavoured error returns
	// of its own ("parse checkpoint GTID set", "query @@gtid_purged",
	// "checkpoint GTID set is empty"), any of which would satisfy this check
	// without a single row having been deleted. The surviving-row assertion
	// below is what actually pins the branch and the dedup. The message is
	// matched loosely (any GTID-flavoured error) so a server-side wording
	// change doesn't turn this into a false red.
	var gtidMode string
	_ = sourceDB.QueryRow("SELECT @@gtid_mode").Scan(&gtidMode)
	if gtidMode != "ON" {
		if err == nil {
			t.Fatal("expected One to fail on the GTID dump against a gtid_mode=OFF source, got nil")
		}
		if !strings.Contains(strings.ToUpper(err.Error()), "GTID") {
			t.Fatalf("expected a GTID-mode failure (proving the GTID branch was selected), got: %v", err)
		}
	}

	// The position cut removes "beyond-checkpoint"; the open-transaction sweep
	// removes "open-transaction" (its GTID is not in the saved :1-3). The row
	// below the offset whose GTID the set DOES contain, and the row on an
	// EARLIER binlog file, must both survive — deleting either would destroy
	// data the source will never re-send.
	assertPKs(t, survivingPKs(t, indexDB, sourceName), []string{"committed", "earlier-file"},
		"GTID resume must delete only what StartSyncGTID will re-send")
}

// ─── Acceptance criterion #2: the start_pos >= pos boundary ──────────────────

// seedBoundaryEvents wipes binlog_events and lays down two ADJACENT events on
// one binlog file, straddling checkpoint offset 200:
//
//	pk 1 — [100, 200)  its END is the checkpoint
//	pk 2 — [200, 300)  its START is the checkpoint
func seedBoundaryEvents(t *testing.T, db *sql.DB, gtid1, gtid2 *string) {
	t.Helper()
	testutil.MustExec(t, db, "DELETE FROM binlog_events")
	const ts = "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000007", 100, 200, ts, gtid1, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000007", 200, 300, ts, gtid2, "mydb", "orders", 1, "2", nil, nil, []byte(`{"id":2}`))
}

// survivingPKs lists the pk_values still in binlog_events for one schema.
//
// The schema scope is load-bearing, not cosmetic. Its GTID caller runs a real
// One() against this same index DB: StartSyncGTID SUCCEEDS even on a
// gtid_mode=OFF source (see TestIntegrationResumeGTIDDedupsBeforeStartSync), so
// a live stream is attached and indexing for as long as that call runs. Nothing
// lands today only because the source is freshly created and no DML runs
// against it; an unscoped SELECT would turn any future fixture that writes to
// the source into a flaky "unexpected surviving pk".
func survivingPKs(t *testing.T, db *sql.DB, schema string) []string {
	t.Helper()
	rows, err := db.Query("SELECT pk_values FROM binlog_events WHERE schema_name = ? ORDER BY pk_values", schema)
	if err != nil {
		t.Fatalf("query survivors: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			t.Fatalf("scan survivor: %v", err)
		}
		out = append(out, pk)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate survivors: %v", err)
	}
	return out
}

func assertPKs(t *testing.T, got, want []string, msg string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: surviving pks = %v, want %v", msg, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: surviving pks = %v, want %v", msg, got, want)
		}
	}
}

// TestIntegrationDedupBoundaryIsEventStart pins the `start_pos >= pos` boundary
// against a real database — acceptance criterion #2 of #500, which the
// reasoned-by-inspection argument left unguarded.
//
// The invariant is an EQUALITY, and both of its edges must be pinned:
//
//	delete-set  ≡  { rows with start_pos >= P }        (deleteEventsSinceCheckpoint)
//	resend-set  ≡  { events with start_pos >= P }      (StartSync from offset P)
//
// It holds because the checkpoint persists the last durable event's END offset
// (streamLoop: state.binlogPos = ev.EndPos, safePos likewise) — so P is always
// an event boundary, and the two sets coincide.
//
// Each edge breaks a different way, so the test asserts both:
//   - the row ENDING at P (start_pos < P) is NOT re-sent, so it must SURVIVE.
//     Widening the delete to `start_pos > pos - 1` / `end_pos > pos` destroys
//     already-indexed data that will never be re-delivered — silent LOSS.
//   - the row STARTING at P is re-sent, so it must be DELETED. Narrowing the
//     delete to `start_pos > pos` leaves it in place and the replay inserts a
//     second copy — silent DUPLICATION.
//
// Should the checkpoint ever be changed to persist an event's START offset, the
// resend-set gains that event while the delete-set (`>=` on the same value)
// still matches it, but the surviving-row edge below no longer describes the
// last durable event — this test is where that inversion has to be re-argued.
func TestIntegrationDedupBoundaryIsEventStart(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	t.Run("checkpoint at the boundary between the two events", func(t *testing.T) {
		seedBoundaryEvents(t, db, nil, nil)
		n, err := deleteEventsSinceCheckpoint(db, "binlog.000007", 200, noDedupFloor)
		if err != nil {
			t.Fatalf("deleteEventsSinceCheckpoint: %v", err)
		}
		if n != 1 {
			t.Fatalf("rows deleted = %d, want exactly 1 (only the event STARTING at the checkpoint)", n)
		}
		assertPKs(t, survivingPKs(t, db, "mydb"), []string{"1"},
			"the event ENDING at the checkpoint is not re-sent by StartSync and must survive")
	})

	t.Run("checkpoint past both events deletes nothing", func(t *testing.T) {
		seedBoundaryEvents(t, db, nil, nil)
		n, err := deleteEventsSinceCheckpoint(db, "binlog.000007", 300, noDedupFloor)
		if err != nil {
			t.Fatalf("deleteEventsSinceCheckpoint: %v", err)
		}
		if n != 0 {
			t.Fatalf("rows deleted = %d, want 0 (nothing is re-sent from offset 300)", n)
		}
		assertPKs(t, survivingPKs(t, db, "mydb"), []string{"1", "2"}, "checkpoint past both events")
	})

	t.Run("checkpoint at the first event's start deletes both", func(t *testing.T) {
		seedBoundaryEvents(t, db, nil, nil)
		n, err := deleteEventsSinceCheckpoint(db, "binlog.000007", 100, noDedupFloor)
		if err != nil {
			t.Fatalf("deleteEventsSinceCheckpoint: %v", err)
		}
		if n != 2 {
			t.Fatalf("rows deleted = %d, want 2 (StartSync from 100 re-sends both)", n)
		}
		assertPKs(t, survivingPKs(t, db, "mydb"), nil, "checkpoint at the first event's start")
	})
}

// TestIntegrationDedupBoundaryIsEventStartGTID is the GTID-mode half of the
// boundary pin. GTID resume is transaction-aligned rather than byte-aligned, so
// deleteEventsSinceCheckpointGTID keeps the same `start_pos >= P` cut and adds
// one sweep: rows BELOW P whose GTID the checkpoint's set does not yet contain
// belong to a transaction that was still open when the checkpoint was written.
// StartSyncGTID replays such a transaction from its beginning, so those rows
// are in the resend-set too and must be deleted — while a row below P whose
// GTID the set DOES contain is not replayed and must survive.
func TestIntegrationDedupBoundaryIsEventStartGTID(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	committed := uuid + ":1" // durably in the saved set
	stillOpen := uuid + ":2" // flushed, but the checkpoint never recorded it
	savedSet, err := gomysql.ParseMysqlGTIDSet(uuid + ":1")
	if err != nil {
		t.Fatalf("parse saved set: %v", err)
	}

	t.Run("committed transaction below the checkpoint survives", func(t *testing.T) {
		seedBoundaryEvents(t, db, &committed, &committed)
		n, err := deleteEventsSinceCheckpointGTID(db, "binlog.000007", 200, savedSet, gomysql.MySQLFlavor, noDedupFloor)
		if err != nil {
			t.Fatalf("deleteEventsSinceCheckpointGTID: %v", err)
		}
		if n != 1 {
			t.Fatalf("rows deleted = %d, want exactly 1 (only the event starting at the checkpoint)", n)
		}
		assertPKs(t, survivingPKs(t, db, "mydb"), []string{"1"},
			"a transaction the saved GTID set already contains is not replayed and must survive")
	})

	t.Run("open transaction below the checkpoint is swept", func(t *testing.T) {
		// pk 1 is flushed below the checkpoint offset but its GTID is NOT in the
		// saved set — the mid-transaction checkpoint case (#491) that the
		// position-keyed delete alone cannot reach.
		seedBoundaryEvents(t, db, &stillOpen, &stillOpen)
		n, err := deleteEventsSinceCheckpointGTID(db, "binlog.000007", 200, savedSet, gomysql.MySQLFlavor, noDedupFloor)
		if err != nil {
			t.Fatalf("deleteEventsSinceCheckpointGTID: %v", err)
		}
		if n != 2 {
			t.Fatalf("rows deleted = %d, want 2 (the at-checkpoint row plus the open-transaction straggler)", n)
		}
		assertPKs(t, survivingPKs(t, db, "mydb"), nil, "open-transaction rows must not survive the resume")
	})
}
