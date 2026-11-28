// Package pgbaseline produces Parquet baseline snapshots directly from a live
// PostgreSQL source (#593 slice C) — the PG sibling of the mydumper→Parquet
// converter in internal/baseline, with no dump step: it COPYs each published
// table inside one REPEATABLE READ snapshot and streams the rows into
// internal/baseline.Writer.
//
// CAPTURE SIDE ONLY. This package links pgx/pglogrepl (via internal/pgcapture)
// and therefore must never be imported by the read layer (internal/baseline,
// internal/reconstruct, query/recover/shim/console) — the #528 guard
// (internal/event.TestReadLayerDoesNotLinkGoMySQL) enforces that boundary.
// The dependency points the other way: pgbaseline imports internal/baseline
// for the Writer, markers, and metadata keys.
//
// Anchoring: the output embeds baseline.MetaKeyLSN — a delta-replay FLOOR
// (pgcapture.AnchorOnSlot: the replication slot's confirmed_flush_lsn, read
// BEFORE the snapshot transaction opens) — so deltas AT OR
// AFTER that LSN, applied by reconstruct, yield the table state at any later
// time (#771: the corrected contract is "from the floor forward", NOT
// "strictly after the live pg_current_wal_lsn() anchor" — see Stats.AnchorLSN
// and the snapshot-anchor comment below for why the live LSN alone cannot be
// the cutoff). The replication slot is ensured to exist AND to have its
// position BEFORE the snapshot opens (ordering invariant: the floor LSN, read
// at that point, is ≤ every LSN read afterwards on the same connection, in
// particular the anchor LSN); any resulting overlap is redelivered and is
// harmless because reconstruct's merge is last-write-wins idempotent over
// full-row images. A slot that is still being created has no position and
// anchors nothing: the run waits for it, up to Config.SlotWait when another
// session creates it, and is refused past that, before any table is read
// (#3078); a slot this run creates itself is waited for as long as its
// creation takes. The slot is looked at
// once more when the tables are read, and a snapshot whose slot is gone or is
// not where it was is not published.
//
// Values are stored as raw PostgreSQL text (Column.RawText — COPY text output,
// unescaped): identical to the pgoutput text rendering the delta path indexes,
// so the PK join between baseline and deltas is an identity string match. No
// type conversion, ever. That identity only holds because BOTH sessions render
// under the SAME session GUCs: every COPY connection here and the walsender
// session are pinned to the canonical rendering GUCs (#593 slice D,
// pgcapture/gucpin.go — TimeZone=UTC, DateStyle=ISO, extra_float_digits=3,
// bytea_output=hex, IntervalStyle=postgres), and the pinned set is stamped
// into the Parquet metadata (baseline.MetaKeyRenderGUCs) so readers can tell a
// pre-pin baseline from a pinned one.
package pgbaseline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// Config holds all parameters for a PostgreSQL baseline run.
type Config struct {
	// QueryDSN is an ordinary connection string: catalog reads, the snapshot
	// transaction, and the COPY transfers all run on it. Required.
	QueryDSN string
	// ReplDSN is a REPLICATION connection string (replication=database), used
	// ONLY to create the slot when it does not exist yet. Empty is fine when
	// the slot already exists (e.g. `bintrail-pg stream` created it); a
	// missing slot with an empty ReplDSN is an actionable fatal error, never
	// a silent skip — a baseline without a slot has no delta stream to anchor.
	ReplDSN string
	// SlotWait is how long the run waits for the slot to have its position
	// when another session (capture, another snapshot) is creating it
	// (pgcapture.AnchorOnSlot); past it the run is refused before any table
	// is read. Zero or negative is pgcapture.DefaultSlotWait. A slot that
	// already has its position is never waited for, and a creation by this
	// run itself is not bounded by it: only the run's context ends it.
	SlotWait time.Duration
	// SlotName and Publication mirror `bintrail-pg stream`'s. The publication
	// defines the table set (narrowed by Filters); the slot is the ordering
	// anchor.
	SlotName    string
	Publication string
	// Filters narrows the published set client-side (same semantics as
	// stream's --schemas/--tables via cliutil.BuildIndexFilters).
	Filters event.Filters

	OutputDir    string
	Compression  string // "zstd" (default), "snappy", "gzip", "none"
	RowGroupSize int    // rows per row group; <=0 → 500_000
	Parallelism  int    // concurrent table COPYs; <=0 → runtime.NumCPU()
	// WriterID is the identity this snapshot is signed with (#1762): the
	// source's bintrail_id. Empty leaves it unsigned. See baseline/writersig.go.
	WriterID string

	// DirStarting, when set, is told the snapshot directory's parent and
	// name right before Run creates it: the name comes from the source's own
	// clock, so the caller cannot know it sooner. The console journals the
	// directory there and removes it when the run then fails (#1938, #2180),
	// as it does for a MySQL read. Nil for the command line.
	DirStarting func(root, name string)

	// BeforePublish, when set, is asked once every table is read and the
	// slot was found where it should be, right before the snapshot is
	// published: an error leaves the folder _INCOMPLETE and fails the run.
	// The console asks its index there whether capture started after the
	// snapshot's floor, which only the index knows (#3078).
	BeforePublish func(Stats) error

	Logger *slog.Logger // nil → slog.Default()
}

// testHookAfterSnapshot, when non-nil, runs right after the snapshot
// transaction is established and the anchor LSN is read — the seam the
// concurrent-writer boundary test uses to commit rows that must land OUTSIDE
// the snapshot (and therefore inside the delta stream, at LSN > anchor).
// Test-only; never set in production code.
var testHookAfterSnapshot func()

// Stats describes the outcome of a baseline run.
type Stats struct {
	TablesProcessed int
	RowsWritten     int64
	FilesWritten    int
	// AnchorLSN is pg_current_wal_lsn() at snapshot establishment — informational
	// only (roughly "where the snapshot's visible data sits in the WAL timeline").
	// It is NOT the delta-replay cutoff (#771): a transaction committing
	// concurrently with the snapshot can flush its commit record at or before
	// this LSN while still being invisible to the snapshot's MVCC view, so using
	// AnchorLSN as "deltas start strictly after here" can silently drop that
	// transaction from both the baseline and the delta window. Use DeltaStartLSN
	// (also embedded in the Parquet metadata as baseline.MetaKeyLSN) instead.
	AnchorLSN uint64
	// DeltaStartLSN is the safe floor for delta replay: the replication slot's
	// confirmed_flush_lsn (pgcapture.AnchorOnSlot), read BEFORE the
	// snapshot transaction opened. It is always <= AnchorLSN. A consumer should
	// replay deltas at or after DeltaStartLSN, not strictly after AnchorLSN;
	// any overlap with data already in the baseline is harmless because the
	// baseline+delta merge is last-write-wins over full-row images.
	DeltaStartLSN uint64
	SnapshotTime  time.Time // DB now() at snapshot establishment (UTC)
	SlotCreated   bool      // true when this run created the replication slot
	// SlotWaited is how long the run waited for a slot that was still being
	// created to have its position; zero when it had one at once.
	SlotWaited time.Duration
	// TablesLeftOut are the published tables this snapshot does not hold
	// because a schema or table name cannot be a file path in it (#3113).
	TablesLeftOut []baseline.LeftOutTable
}

// Run takes a consistent baseline snapshot of every table the publication
// streams and writes one Parquet file per table under
// <OutputDir>/<timestamp>/<schema>/<table>.parquet, mirroring
// internal/baseline.Run's marker/manifest discipline exactly (_INCOMPLETE
// before work, manifest + _SUCCESS only on full success).
func Run(ctx context.Context, cfg Config) (Stats, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if err := baseline.ValidateCodec(cfg.Compression); err != nil {
		return Stats{}, err
	}
	rowGroupSize := cfg.RowGroupSize
	if rowGroupSize <= 0 {
		rowGroupSize = 500_000
	}
	compression := cfg.Compression
	if compression == "" {
		compression = "zstd"
	}

	// Pinned rendering GUCs (#593 slice D, pgcapture/gucpin.go): this anchor
	// connection runs the COPY itself when Parallelism==1, so its session GUCs
	// determine the baseline text — pinned identically to the walsender.
	// Startup-packet placement keeps the REPEATABLE READ anchor below intact
	// (the pin is in effect before BEGIN; no SET inside the transaction).
	conn, err := pgcapture.ConnectQueryPinned(ctx, cfg.QueryDSN)
	if err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: connect (query DSN): %w", err)
	}
	defer conn.Close(context.Background())

	// Before the slot: a role that cannot read a table to copy is refused
	// with nothing created (#3022).
	if err := refuseUnreadable(ctx, conn, cfg.Publication, cfg.Filters, logger); err != nil {
		return Stats{}, err
	}
	// Nor is a slot made for a run with no table it can store (#3113).
	if left, err := refuseAllUnstorable(ctx, conn, cfg.Publication, cfg.Filters, logger); err != nil {
		return Stats{TablesLeftOut: left}, err
	}

	// The slot exists AND has its position BEFORE the snapshot transaction
	// opens — the ordering invariant that makes the baseline anchorable: the
	// slot delivers every commit at or after that position, which is ≤ the
	// anchor LSN read below, so no delta between them can be missed (overlap
	// is redelivered; the merge is idempotent). A slot that is still being
	// created is waited for, and the run is refused past the wait with
	// nothing read (#3078): read now, the rows would be older than where the
	// slot starts delivering, and what commits in between is in neither.
	var replConnect func(context.Context) (*pgconn.PgConn, error)
	if cfg.ReplDSN != "" {
		replConnect = func(ctx context.Context) (*pgconn.PgConn, error) {
			// Renders no rows (slot creation only); pinned for uniformity.
			return pgcapture.ConnectReplPinned(ctx, cfg.ReplDSN)
		}
	}
	// The position is the delta-replay floor (#771), read BEFORE the snapshot
	// transaction opens: the slot's own confirmed_flush_lsn only ever advances
	// past transactions already fully resolved in WAL order. Read here, on
	// this same connection before BEGIN, it is <= every LSN read afterwards
	// (including the anchor below) — the invariant that makes "replay deltas
	// from this floor" safe regardless of the snapshot-vs-concurrent-commit
	// race described on SlotFloorLSN.
	slotAnchor, err := pgcapture.AnchorOnSlot(ctx, conn, cfg.SlotName, replConnect, cfg.SlotWait, logger)
	if err != nil {
		return Stats{}, err
	}
	created, floorLSN := slotAnchor.Created, slotAnchor.Floor
	if created {
		logger.Info("pgbaseline: created replication slot", "slot", cfg.SlotName)
	}

	// Open the snapshot transaction. The FIRST statement both establishes the
	// REPEATABLE READ MVCC snapshot and reads the WAL anchor + snapshot time,
	// so (anchorLSN, snapshotTime, visible data) are fixed together — but
	// anchorLSN is NOT the delta-replay cutoff (#771, see Stats.AnchorLSN):
	// a transaction can flush its commit record at or before this LSN while
	// still being invisible to this snapshot (WAL-flush happens before the
	// transaction is removed from the procarray), so treating "strictly after
	// anchorLSN" as the delta window can silently drop it from both the
	// baseline AND the deltas. floorLSN (above), not anchorLSN, is what gets
	// embedded as the delta-replay cutoff.
	if _, err := conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY"); err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: begin snapshot transaction: %w", err)
	}
	defer func() {
		// Best-effort: the transaction is READ ONLY, rollback cannot lose data.
		_, _ = conn.Exec(context.Background(), "ROLLBACK")
	}()

	var anchorText string
	var snapshotTime time.Time
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text, now()").Scan(&anchorText, &snapshotTime); err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: read snapshot anchor (pg_current_wal_lsn): %w", err)
	}
	anchorLSN, err := pglogrepl.ParseLSN(anchorText)
	if err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: parse anchor LSN %q: %w", anchorText, err)
	}
	snapshotTime = snapshotTime.UTC()
	if testHookAfterSnapshot != nil {
		testHookAfterSnapshot()
	}

	// deltaStartLSN is what actually gets embedded as the replay cutoff.
	// floorLSN <= anchorLSN by construction (read earlier on the same
	// connection); this clamp is defense in depth only — it must never
	// trigger — so a violated assumption can never make the embedded
	// metadata claim a LATER delta start than the snapshot boundary itself.
	deltaStartLSN := floorLSN
	if deltaStartLSN > anchorLSN {
		logger.Warn("pgbaseline: slot floor LSN exceeded the snapshot anchor LSN (unexpected — clamping)",
			"floor_lsn", uint64(floorLSN), "anchor_lsn", uint64(anchorLSN))
		deltaStartLSN = anchorLSN
	}

	// Discovery + column resolution run INSIDE the snapshot transaction, so
	// the schema the Parquet files carry is consistent with the copied data.
	tables, err := discoverTables(ctx, conn, cfg.Publication, cfg.Filters, logger)
	if err != nil {
		return Stats{}, err
	}
	if len(tables) == 0 {
		// Never publish an empty baseline (mirrors baseline.Run's #461 guard):
		// success here would surface weeks later as ErrNoBaseline — or as
		// Time-travel silently reconstructing from an older snapshot.
		return Stats{}, fmt.Errorf("pgbaseline: publication %q streams no tables matching the filters — the baseline would be empty; check the publication's table list and the --schemas/--tables filters", cfg.Publication)
	}
	// Before anything is read or created: a name that is not one path
	// component never reaches a file path (#3113).
	tables, leftOut := splitStorable(tables, logger)
	if len(tables) == 0 {
		return Stats{TablesLeftOut: leftOut}, allLeftOutError(leftOut)
	}
	// The catalog's expressions and type names are read with an empty
	// search_path, so every name in them comes back schema-qualified (as
	// pg_dump reads them): a CREATE TABLE made from them binds to the same
	// objects whatever the loading session's search_path. Restored before any
	// row is read, so values render as they always have.
	var searchPath string
	if err := conn.QueryRow(ctx, "SELECT current_setting('search_path')").Scan(&searchPath); err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: read search_path: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT set_config('search_path', '', true)"); err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: clear search_path for the catalog reads: %w", err)
	}
	for i := range tables {
		shape, gens, err := loadColumns(ctx, conn, tables[i].OID, tables[i].Schema, tables[i].Table, logger)
		if err != nil {
			return Stats{}, err
		}
		key, err := loadPrimaryKey(ctx, conn, tables[i].OID, tables[i].Schema, tables[i].Table)
		if err != nil {
			return Stats{}, err
		}
		tables[i].Shape, tables[i].Generated, tables[i].PrimaryKey = shape, gens, key
		tables[i].Columns = make([]string, len(shape))
		for j, c := range shape {
			tables[i].Columns[j] = c.Name
		}
	}
	if _, err := conn.Exec(ctx, "SELECT set_config('search_path', $1, true)", searchPath); err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: restore search_path after the catalog reads: %w", err)
	}

	concurrency := cfg.Parallelism
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}
	if concurrency > len(tables) {
		concurrency = len(tables)
	}
	if concurrency < 1 {
		concurrency = 1
	}

	// Parallel workers each need their own connection sharing THIS snapshot:
	// export it while the anchor transaction is open (pg_export_snapshot is
	// only valid until the exporting transaction ends, so the anchor
	// transaction stays open until all workers finish).
	var snapshotID string
	if concurrency > 1 {
		if err := conn.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
			return Stats{}, fmt.Errorf("pgbaseline: export snapshot for parallel workers: %w", err)
		}
	}

	tsStr := snapshotTime.Format(time.RFC3339)
	tsDir := strings.ReplaceAll(tsStr, ":", "-")

	// Crash-safety (#467), mirroring baseline.Run: create the snapshot
	// directory and flag it _INCOMPLETE BEFORE any table conversion; only
	// full success replaces the marker with _SUCCESS.
	snapDir := filepath.Join(cfg.OutputDir, tsDir)
	if cfg.DirStarting != nil {
		cfg.DirStarting(cfg.OutputDir, tsDir)
	}
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: create snapshot directory %s: %w", snapDir, err)
	}
	// The marker write is FATAL, not best-effort: marker-absent directories
	// are complete-by-default (legacy compat), so continuing without the
	// _INCOMPLETE flag and then dying uncatchably (SIGKILL/OOM/power loss)
	// mid-COPY would leave a markerless partial snapshot that discovery
	// serves as the newest complete baseline — the exact #467 silent loss
	// the marker exists to close. A run whose crash-safety net failed to
	// deploy has nothing to salvage; abort before any table is copied.
	if err := baseline.WriteIncompleteMarker(snapDir); err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: could not write incomplete-snapshot marker in %s (refusing to copy without the crash-safety marker): %w", snapDir, err)
	}

	stats := Stats{AnchorLSN: uint64(anchorLSN), DeltaStartLSN: uint64(deltaStartLSN), SnapshotTime: snapshotTime, SlotCreated: created, SlotWaited: slotAnchor.Waited, TablesLeftOut: leftOut}

	var (
		mu   sync.Mutex
		errs []error
	)
	// The work channel is BUFFERED to len(tables) and fully fed + closed
	// BEFORE any worker starts: a worker that exits early (context cancelled,
	// openWorkerConn failure — e.g. SQLSTATE 53300 near max_connections) then
	// simply stops draining, and the remaining sends can never block. An
	// unbuffered channel fed after worker startup deadlocked Run whenever the
	// surviving workers were fewer than the remaining tables (review blocker).
	work := make(chan tableInfo, len(tables))
	for _, t := range tables {
		work <- t
	}
	close(work)

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Worker connection: the anchor conn itself when serial, else a
			// fresh conn adopting the exported snapshot.
			wconn := conn
			if concurrency > 1 {
				var err error
				wconn, err = openWorkerConn(ctx, cfg.QueryDSN, snapshotID)
				if err != nil {
					// Logged HERE, not only surfaced via the return: with
					// multiple failures only the first reaches the caller,
					// and this one is actionable (connection limits).
					logger.Error("pgbaseline: parallel worker could not open its connection", "error", err)
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				defer func() {
					_, _ = wconn.Exec(context.Background(), "ROLLBACK")
					wconn.Close(context.Background())
				}()
			}
			for t := range work {
				if ctx.Err() != nil {
					return
				}
				n, err := processTable(ctx, wconn, t, cfg.OutputDir, tsDir, tsStr, uint64(deltaStartLSN), uint64(anchorLSN), compression, rowGroupSize, logger)
				mu.Lock()
				if err != nil {
					logger.Error("pgbaseline: failed to process table",
						"schema", t.Schema, "table", t.Table, "error", err)
					errs = append(errs, fmt.Errorf("%s.%s: %w", t.Schema, t.Table, err))
				} else {
					stats.TablesProcessed++
					stats.FilesWritten++
					stats.RowsWritten += n
					logger.Info("pgbaseline: table complete",
						"schema", t.Schema, "table", t.Table, "rows", n)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Every early return below leaves the _INCOMPLETE marker in place; only
	// full success publishes the snapshot (manifest, then _SUCCESS) — exactly
	// baseline.Run's close-out discipline.
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if len(errs) > 0 {
		if len(errs) > 1 {
			// Every failure (table AND worker-connection) was logged at append
			// time above; the wrapped error carries the first for errors.Is/As.
			return stats, fmt.Errorf("pgbaseline: %d failures across %d tables (all logged); first: %w", len(errs), len(tables), errs[0])
		}
		return stats, errs[0]
	}
	// The slot once more, now that the tables are read (#3078): one that is
	// gone, lost, being created again or behind where it stood is not the
	// slot this snapshot started under, and a gap known now is an error of
	// the run, not a line for a later reader to find.
	if err := checkSlotAtEnd(ctx, cfg, conn, floorLSN); err != nil {
		return stats, fmt.Errorf("pgbaseline: %w", err)
	}
	if cfg.BeforePublish != nil {
		if err := cfg.BeforePublish(stats); err != nil {
			return stats, err
		}
	}
	baseline.SignSnapshot(snapDir, cfg.WriterID)
	if err := baselineintegrity.WriteManifest(snapDir); err != nil {
		return stats, fmt.Errorf("pgbaseline: snapshot complete but could not write integrity manifest: %w", err)
	}
	if err := baseline.CompleteSnapshot(snapDir); err != nil {
		return stats, publishError(err)
	}
	// The key index beside each table's file (#3085), for the snapshot
	// updates that find changed rows in it. After the snapshot is published
	// and the source's transaction is closed: a build is a sort of every key,
	// and the source must not hold its snapshot open for it. Nothing here
	// fails the run; a table without an index is read as before.
	// Ended here, not at return: the deferred ROLLBACK and Close then find
	// the connection closed and do nothing, which is what they are for.
	_, _ = conn.Exec(context.Background(), "ROLLBACK")
	conn.Close(context.Background())
	for _, t := range tables {
		key := keyInTableOrder(t)
		if len(key) == 0 || ctx.Err() != nil {
			continue
		}
		pgdelta.WriteKeyIndexBeside(ctx, t.Schema, t.Table, filepath.Join(snapDir, t.Schema, t.Table+".parquet"), key, duckdbutil.DefaultTuningFor(ctx))
	}
	return stats, nil
}

// keyInTableOrder is t's primary key in the order of its columns, the order
// a key is joined in (pgdelta.BasePositions); nil when it has none, or names
// a column the file does not hold.
func keyInTableOrder(t tableInfo) []string {
	in := make(map[string]bool, len(t.PrimaryKey))
	for _, c := range t.PrimaryKey {
		in[c] = true
	}
	var key []string
	for _, c := range t.Columns {
		if in[c] {
			key = append(key, c)
		}
	}
	if len(key) != len(in) || len(in) != len(t.PrimaryKey) {
		return nil
	}
	return key
}

// checkSlotAtEnd asks pgcapture.CheckSlotStillAnchors on a connection of its
// own. The anchor connection has sat idle in its transaction for the whole of
// a parallel read, and a server that closed it meanwhile
// (idle_in_transaction_session_timeout) would fail a snapshot that is whole
// over a question that is not about its rows: pg_replication_slots is not
// under the snapshot, so any connection shows the slot as it is. Only when no
// new connection can be had is the anchor connection asked, and an error then
// carries both causes.
//
// A read that failed (a connection cut for a second, a query error) is asked
// again, slotEndAttempts times in all, slotEndPause apart, before the run is
// failed: discarding hours of reading over a blip is the wrong trade. A
// verdict about the slot (gone, lost, being created again, behind where it
// stood, not a pgoutput slot) is returned at once: asking again would not
// change it.
func checkSlotAtEnd(ctx context.Context, cfg Config, anchor *pgx.Conn, floor pglogrepl.LSN) error {
	var err error
	for attempt := 1; ; attempt++ {
		err = checkSlotOnce(ctx, cfg, anchor, floor)
		if err == nil || !pgcapture.IsSlotUnreadable(err) || ctx.Err() != nil {
			return err
		}
		if attempt == slotEndAttempts {
			return fmt.Errorf("the replication slot could not be read after the tables were (%d attempts), so whether the snapshot can be anchored on it is not known: %w", attempt, err)
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(slotEndPause):
		}
	}
}

// slotEndAttempts and slotEndPause: how often, and how far apart,
// checkSlotAtEnd asks a slot whose read failed. Variables for the tests.
var (
	slotEndAttempts = 3
	slotEndPause    = time.Second
)

// connectSlotCheck and checkSlotStillAnchors are the two calls of
// checkSlotOnce; variables so a test makes them fail. connectSlotCheck
// returns how to close what it opened.
var (
	connectSlotCheck = func(ctx context.Context, dsn string) (*pgx.Conn, func(), error) {
		conn, err := pgcapture.ConnectQueryPinned(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return conn, func() { conn.Close(context.Background()) }, nil
	}
	checkSlotStillAnchors = pgcapture.CheckSlotStillAnchors
)

// checkSlotOnce is one ask of checkSlotAtEnd. A connection that cannot be
// opened is a failed read (pgcapture.IsSlotUnreadable) only when the anchor
// connection could not read the slot either; the error then names both.
func checkSlotOnce(ctx context.Context, cfg Config, anchor *pgx.Conn, floor pglogrepl.LSN) error {
	fresh, closeFresh, cerr := connectSlotCheck(ctx, cfg.QueryDSN)
	if cerr == nil {
		defer closeFresh()
		return checkSlotStillAnchors(ctx, fresh, cfg.SlotName, floor)
	}
	err := checkSlotStillAnchors(ctx, anchor, cfg.SlotName, floor)
	if err != nil && pgcapture.IsSlotUnreadable(err) {
		return slotReadBothFailed{connect: cerr, anchor: err}
	}
	return err
}

// slotReadBothFailed: no new connection could be opened, and the anchor
// connection could not read the slot. Unwraps to both, so it is still a
// failed read.
type slotReadBothFailed struct{ connect, anchor error }

func (e slotReadBothFailed) Error() string {
	return fmt.Sprintf("a new connection to read the replication slot could not be opened (%v), and the snapshot's own connection could not read it either (%v)", e.connect, e.anchor)
}

func (e slotReadBothFailed) Unwrap() []error { return []error{e.connect, e.anchor} }

// publishError words a failed publish. A vanished marker is returned as it
// is (#2180): the folder is NOT complete, so the "snapshot complete" wording
// of a marker-write failure would be false about it.
func publishError(err error) error {
	if errors.Is(err, baseline.ErrIncompleteMarkerVanished) {
		return err
	}
	return fmt.Errorf("pgbaseline: snapshot complete but could not write %s marker: %w", baseline.SuccessMarker, err)
}

// openWorkerConn opens a connection whose transaction adopts the exported
// snapshot, so a parallel worker sees exactly the anchor transaction's data.
func openWorkerConn(ctx context.Context, dsn, snapshotID string) (*pgx.Conn, error) {
	// Every parallel worker runs COPYs — its session renders baseline text, so
	// it gets the same rendering-GUC pin as the anchor (#593 slice D).
	c, err := pgcapture.ConnectQueryPinned(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgbaseline: connect parallel worker: %w", err)
	}
	if _, err := c.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY"); err != nil {
		c.Close(context.Background())
		return nil, fmt.Errorf("pgbaseline: begin worker transaction: %w", err)
	}
	// The snapshot ID comes from pg_export_snapshot(), not user input, but is
	// still quoted through a literal escape for defense in depth.
	if _, err := c.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+strings.ReplaceAll(snapshotID, "'", "''")+"'"); err != nil {
		c.Close(context.Background())
		return nil, fmt.Errorf("pgbaseline: adopt exported snapshot %q: %w", snapshotID, err)
	}
	return c, nil
}

// tableFooter is the footer of one table of a PostgreSQL snapshot.
func tableFooter(t tableInfo, tsStr string, deltaStartLSN, anchorLSN uint64) (map[string]string, error) {
	// The table's OID and typed column list (#1596 slice 1). Written only if
	// it reads back: a shape the readers would reject is a bug here, and a
	// baseline without its shape must not be published as if it had one.
	shape, err := baseline.PGRelation{OID: t.OID, Columns: t.Shape, Generated: t.Generated, PrimaryKey: t.PrimaryKey, Collations: true}.Encode()
	if err != nil {
		return nil, err
	}
	if _, _, err := baseline.ParsePGRelation(shape); err != nil {
		return nil, fmt.Errorf("pgbaseline: %s.%s: %w", t.Schema, t.Table, err)
	}
	taken, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		return nil, fmt.Errorf("pgbaseline: %s.%s: the snapshot's time %q cannot be read: %w", t.Schema, t.Table, tsStr, err)
	}
	return map[string]string{
		// How far back the changes this file may be missing reach: the one
		// it can miss is a commit that raced the snapshot's start
		// (baseline.PGDeltaMargin). It is the instant a reader already
		// assumed of a file that recorded none, measured from the snapshot's
		// time. Recorded, it stays this file's own when the file is published
		// again in a later snapshot, unchanged or with a table delta beside
		// it: without it the file had to be written again in full by the
		// first update after every full read.
		baseline.MetaKeyPGDeltaSince: taken.Add(-baseline.PGDeltaMargin).UTC().Format(time.RFC3339),

		baseline.MetaKeySnapshotTimestamp: tsStr,
		"bintrail.source_database":        t.Schema,
		"bintrail.source_table":           t.Table,
		"bintrail.bintrail_version":       baseline.Version,
		// #1545: a PostgreSQL baseline is a real read of the source, same as
		// mydumper's. Without this it carries no producer key and only its LSN
		// dates it.
		baseline.MetaKeySnapshotProducer: baseline.ProducerDump,
		// #1570: the read of the source every descendant inherits.
		baseline.MetaKeyLastDumpAt:     tsStr,
		baseline.MetaKeyFoldGeneration: "0",
		// #1380: one REPEATABLE READ snapshot for every table, so the read
		// is of one instant by construction.
		baseline.MetaKeyLockMode: baseline.LockStampPGRepeatableRead,
		// The LSN delta-replay floor (#593 slice A, corrected by #771): deltas
		// for this table replay from AT OR AFTER this point — the slot's own
		// confirmed_flush_lsn (pgcapture.AnchorOnSlot), NOT the
		// live pg_current_wal_lsn() read when the snapshot was taken (that
		// live LSN can be at-or-after a concurrently-committing transaction's
		// commit record while the transaction is still invisible to the
		// snapshot — see Stats.AnchorLSN). Any overlap this floor introduces
		// with rows already in the baseline is harmless: the merge is
		// last-write-wins over full-row images. MetaKeyCreateTableSQL is
		// deliberately absent: it is MySQL text the reconstruct engine parses
		// as MySQL; MetaKeyPGRelation below is the PostgreSQL shape (#1596).
		baseline.MetaKeyLSN: strconv.FormatUint(deltaStartLSN, 10),
		// Where the source's WAL stood when the snapshot opened (#2963): the
		// rows are as of here, past the floor above. A reader that lays the
		// changes from the floor over this file waits until capture has
		// indexed every commit below this position.
		baseline.MetaKeyPGAnchorLSN: strconv.FormatUint(anchorLSN, 10),
		// The rendering-GUC stamp (#593 slice D): records that this baseline's
		// text was rendered under the pinned canonical GUCs. Readers use its
		// ABSENCE to detect a pre-pin baseline whose GUC-sensitive text may
		// not join post-pin deltas (warn + re-baseline guidance).
		baseline.MetaKeyRenderGUCs: pgcapture.RenderGUCsStamp(),
		baseline.MetaKeyPGRelation: shape,
	}, nil
}

// processTable COPYs one table into its Parquet file. Returns rows written.
//
// There is deliberately NO local skip-if-exists here (review medium): every
// run gets a fresh now()-named snapshot directory, so a prior run's file can
// never legitimately be at this path, and blindly trusting any size>0 file
// would CRC-certify a stale or partial Parquet (possibly carrying another
// anchor's MetaKeyLSN) into a _SUCCESS baseline. The CLI's --retry applies
// only to baseline.Upload's S3 object skip, which keys on real object state.
func processTable(ctx context.Context, conn *pgx.Conn, t tableInfo, outputDir, tsDir, tsStr string, deltaStartLSN, anchorLSN uint64, compression string, rowGroupSize int, logger *slog.Logger) (int64, error) {
	outPath := filepath.Join(outputDir, tsDir, t.Schema, t.Table+".parquet")

	md, err := tableFooter(t, tsStr, deltaStartLSN, anchorLSN)
	if err != nil {
		return 0, err
	}
	typed := typedColumnsOf(t)
	n, err := copyTable(ctx, conn, t, outPath, md, typed, compression, rowGroupSize, logger)
	if notTyped := (copyNotTyped{}); errors.As(err, &notTyped) {
		// Read again, in the same snapshot: the first COPY was read to its
		// end, so the connection and its transaction are still open.
		logger.Info("pgbaseline: a value of a column the snapshot stores as its type has no single spelling there; the table is written with every column as text",
			"table", t.Schema+"."+t.Table, "column", notTyped.column)
		n, err = copyTable(ctx, conn, t, outPath, md, nil, compression, rowGroupSize, logger)
	}
	return n, err
}

// rowWriter is the file a table's COPY is written to: every column as text
// (baseline.Writer), or some typed (baseline.PGWriter).
type rowWriter interface {
	WriteRow(values []string, nulls []bool) error
	Close() error
}

// copyNotTyped is a COPY read through to its end without being written,
// because a value of column, which typed stores as its type, is not one the
// type holds with the same text (baseline.ErrPGNotTyped).
type copyNotTyped struct{ column string }

func (e copyNotTyped) Error() string {
	return fmt.Sprintf("column %s holds a value that is not stored typed", e.column)
}

// copyTable COPYs one table into its Parquet file, the columns of typed
// stored as their type and every other one as text. Returns rows written.
func copyTable(ctx context.Context, conn *pgx.Conn, t tableInfo, outPath string, md map[string]string, typed map[string]string, compression string, rowGroupSize int, logger *slog.Logger) (int64, error) {
	cfg := baseline.WriterConfig{
		Compression:  compression,
		RowGroupSize: rowGroupSize,
		Metadata:     md,
	}
	var w rowWriter
	if len(typed) > 0 {
		enc, err := baseline.EncodePGTypedColumns(typed)
		if err != nil {
			return 0, err
		}
		withTyped := make(map[string]string, len(md)+1)
		for k, v := range md {
			withTyped[k] = v
		}
		withTyped[baseline.MetaKeyPGTypedColumns] = enc
		cfg.Metadata = withTyped
		pw, err := baseline.NewPGWriter(outPath, t.Columns, typed, cfg)
		if err != nil {
			return 0, fmt.Errorf("create writer: %w", err)
		}
		w = pw
	} else {
		cols := make([]baseline.Column, len(t.Columns))
		for i, name := range t.Columns {
			cols[i] = baseline.Column{Name: name, RawText: true}
		}
		bw, err := baseline.NewWriter(outPath, cols, cfg)
		if err != nil {
			return 0, fmt.Errorf("create writer: %w", err)
		}
		w = bw
	}
	var closed bool
	defer func() {
		if !closed {
			_ = w.Close()
			if err := os.Remove(outPath); err != nil && !os.IsNotExist(err) {
				logger.Warn("pgbaseline: failed to remove partial file", "path", outPath, "error", err)
			}
		}
	}()

	// A value a typed column cannot hold stops the writing, not the COPY:
	// an error handed back to the COPY closes the connection, which holds
	// the snapshot every other table is read in. The rest is read and
	// dropped, and the caller writes the table again as text.
	var notTyped *baseline.PGNotTypedError
	sink := newCopyTextSink(len(t.Columns), func(values []string, nulls []bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if notTyped != nil {
			return nil
		}
		err := w.WriteRow(values, nulls)
		if errors.As(err, &notTyped) {
			return nil
		}
		return err
	})

	// COPY (SELECT <explicit columns> ...) rather than COPY <table>:
	//   - a partitioned parent (relkind='p') rejects direct COPY but the
	//     SELECT form reads all partitions under the parent name;
	//   - the explicit column list pins catalog order and excludes generated
	//     columns (loadColumns), keeping the column set identical to deltas.
	// Identifiers are quoted with PostgreSQL rules via pgx.Identifier.
	colList := make([]string, len(t.Columns))
	for i, name := range t.Columns {
		colList[i] = pgx.Identifier{name}.Sanitize()
	}
	copySQL := fmt.Sprintf("COPY (SELECT %s FROM %s) TO STDOUT (FORMAT text)",
		strings.Join(colList, ", "), pgx.Identifier{t.Schema, t.Table}.Sanitize())
	tag, err := conn.PgConn().CopyTo(ctx, sink, copySQL)
	if err != nil {
		return sink.rows, fmt.Errorf("copy %s.%s: %w", t.Schema, t.Table, err)
	}
	if err := sink.Flush(); err != nil {
		return sink.rows, fmt.Errorf("copy %s.%s: %w", t.Schema, t.Table, err)
	}
	// Belt-and-suspenders integrity check (review blocker): the server's COPY
	// command tag carries ITS row count. Any parser desync — a line silently
	// mis-split, a skipped `\.`, a lost chunk — surfaces here as a count
	// mismatch instead of a truncated baseline that looks complete.
	if want := tag.RowsAffected(); want != sink.rows {
		return sink.rows, fmt.Errorf("copy %s.%s: parsed %d rows but the server reported %d — COPY parser desync, refusing to publish a truncated baseline", t.Schema, t.Table, sink.rows, want)
	}
	if notTyped != nil {
		return 0, copyNotTyped{column: notTyped.Column}
	}

	closed = true
	if err := w.Close(); err != nil {
		os.Remove(outPath)
		return sink.rows, fmt.Errorf("close writer: %w", err)
	}
	return sink.rows, nil
}
