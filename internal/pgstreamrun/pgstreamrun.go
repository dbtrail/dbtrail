// Package pgstreamrun is the PostgreSQL streaming CONSUMER: it drives the
// internal/pgcapture capturer (the producer) into the shared indexer, persisting a
// durable LSN checkpoint to stream_state. It is the PostgreSQL analog of
// internal/streamrun, deliberately a SEPARATE package — the go-mysql GTID/position
// types and binlog gap detection in streamrun do not parameterize. It reuses the
// source-agnostic core: event.Event, indexer.Indexer, and the batch→flush→checkpoint
// loop shape.
//
// #534 (the consumer); closes #530's end-to-end acceptance (a live PG change stream
// indexed into binlog_events via the existing indexer).
package pgstreamrun

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// pgFlavor is the stream_state.flavor value for a PostgreSQL source. The checkpoint
// rides the existing gtid-mode columns (the cursor is a single monotonic LSN, like a
// GTID set, advanced only at commit boundaries) — mode='gtid' needs no shared-schema
// change; flavor='postgres' is what actually distinguishes a PG checkpoint.
const pgFlavor = "postgres"

// defaultEventBuffer is the size of the channel between the capturer and the loop.
const defaultEventBuffer = 1000

// healthPollInterval is how often the loop polls the source for a health snapshot
// (slot WAL-retention + REPLICA IDENTITY coverage) to persist for the console (#599).
// Slot health changes slowly, so 30s keeps the catalog read cheap; the console's
// staleness indicator degrades a snapshot older than a few intervals (a dead daemon
// must not read as healthy). healthPollTimeout bounds one probe so a hung source can
// only briefly pause event draining (the poll runs inline in the loop's select).
const (
	healthPollInterval = 30 * time.Second
	healthPollTimeout  = 5 * time.Second
)

// Config binds everything One needs.
type Config struct {
	IndexDSN    string // the index MySQL database (same store as a MySQL source)
	ReplDSN     string // PostgreSQL replication connection (must carry replication=database)
	QueryDSN    string // PostgreSQL normal connection (catalog PK lookup + slot/publication checks)
	SlotName    string
	Publication string
	ServerID    uint32 // identifier recorded in stream_state (server_id is NOT NULL)
	StartLSN    uint64 // explicit start LSN; ignored once a checkpoint exists; 0 = let the slot's ConsistentPoint decide on first run
	Schemas     string // comma-separated schema filter (empty = all)
	Tables      string // comma-separated table filter (empty = all)
	BatchSize   int
	Partitions  int           // binlog_events partitions for the one-time bootstrap (0 → 48)
	Checkpoint  time.Duration // checkpoint interval (0 → 5s)
	Logger      *slog.Logger
	// Hooks, when non-nil, lets a supervisor observe this stream's liveness —
	// a console-started PG stream flips pending→running once rows index or the
	// checkpoint ticker fires. Mirrors streamrun.Hooks MINUS OnGapAutoAdvance: a
	// lost PG slot is fatal (One returns it; the supervisor reconnects), not a
	// continue-after-loss.
	Hooks *Hooks
}

// Hooks are liveness callbacks a supervisor attaches to one PG stream. All are
// optional and invoked synchronously from the stream loop — keep them fast.
type Hooks struct {
	// OnCheckpoint fires after each successful checkpoint tick, INCLUDING an
	// idle source with nothing committed (the ticker fires regardless) — the
	// "attached and healthy" signal that flips a supervised job pending→running
	// even before the first row.
	OnCheckpoint func()
	// OnIndexed fires after a batch flush that wrote rows.
	OnIndexed func(n int64)
	// OnSourceConnected fires once per run, when replication has started on
	// the slot (#1606). Unlike OnCheckpoint, which the ticker fires even
	// before the capturer connects, it means the source was reached.
	OnSourceConnected func()
}

// sourceConnectedHook adapts Hooks.OnSourceConnected to the capturer's
// OnStarted callback; nil when unset.
func sourceConnectedHook(h *Hooks) func() {
	if h == nil {
		return nil
	}
	return h.OnSourceConnected
}

// pgStreamState mirrors the streamrun checkpoint row, scoped to what a PG source
// needs. The live cursor (last committed LSN) is tracked in the loop, NOT here — this
// holds the loaded resume point and the running counters.
type pgStreamState struct {
	lsn           uint64 // resume LSN (loaded from the checkpoint; 0 on first run)
	eventsIndexed int64
	lastEventTime sql.NullTime
	serverID      uint32
}

// One runs a complete PostgreSQL replication stream until ctx is cancelled. It
// returns nil on graceful shutdown and a non-nil error on a fatal failure (so a
// supervisor reconnects).
func One(ctx context.Context, cfg Config) error {
	if cfg.IndexDSN == "" || cfg.ReplDSN == "" || cfg.QueryDSN == "" || cfg.SlotName == "" || cfg.Publication == "" {
		return fmt.Errorf("pgstreamrun: One requires IndexDSN, ReplDSN, QueryDSN, SlotName, and Publication")
	}
	// A non-positive --write-timeout would make every index write's
	// context.WithTimeout fire immediately (#959).
	if indexer.WriteTimeout <= 0 {
		return fmt.Errorf("pgstreamrun: invalid --write-timeout %s: must be a positive duration", indexer.WriteTimeout)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	partitions := cfg.Partitions
	if partitions <= 0 {
		partitions = 48
	}
	checkpointInterval := cfg.Checkpoint
	if checkpointInterval <= 0 {
		checkpointInterval = 5 * time.Second
	}

	indexDB, err := config.Connect(cfg.IndexDSN)
	if err != nil {
		return fmt.Errorf("pgstreamrun: connect index database: %w", err)
	}
	defer indexDB.Close()

	// Bootstrap (idempotent) then migrate: CreateIndexTables handles a fresh index DB
	// (EnsureSchema alone fails on one), EnsureSchema adds any columns on an existing
	// one.
	if err := indexer.CreateIndexTables(ctx, indexDB, partitions, false, nil); err != nil {
		return fmt.Errorf("pgstreamrun: bootstrap index schema: %w", err)
	}
	if err := indexer.EnsureSchema(indexDB); err != nil {
		return fmt.Errorf("pgstreamrun: migrate index schema: %w", err)
	}

	saved, err := loadStreamStatePG(indexDB)
	if err != nil {
		return err
	}
	startLSN := resolveStartLSN(saved, cfg.StartLSN, logger)

	state := &pgStreamState{lsn: startLSN, serverID: cfg.ServerID}
	if saved != nil {
		state.eventsIndexed = saved.eventsIndexed
	}

	cap := pgcapture.New(pgcapture.Config{
		ReplDSN:            cfg.ReplDSN,
		QueryDSN:           cfg.QueryDSN,
		SlotName:           cfg.SlotName,
		Publication:        cfg.Publication,
		Filters:            cliutil.BuildIndexFilters(cfg.Schemas, cfg.Tables),
		StartLSN:           pglogrepl.LSN(startLSN),
		ExpectExistingSlot: saved != nil, // resuming → the slot must still exist + be valid
		OnStarted:          sourceConnectedHook(cfg.Hooks),
		Logger:             logger,
		// The shapes the index holds (#2441), loaded by the capturer once it
		// knows where the slot resends from. If they cannot be read, the
		// capturer captures rows without detecting schema changes for that
		// run and says so at ERROR level; it never stops capture.
		LoadKnownShapes: func(from uint64) (pgcapture.KnownState, error) { return loadKnownState(indexDB, from) },
		LastChange:      lastIndexedChange(indexDB),
	})
	idx := indexer.New(indexDB, cfg.BatchSize)

	// Bridge loop-exit and capturer-exit (mirrors streamrun.One). The capturer's
	// Run does NOT close the events channel (its contract), so the wrapper goroutine
	// closes it when Run returns: a capturer that dies on its own (slot lost
	// mid-stream, decode desync, network drop) then makes streamLoopPG drain the
	// remaining events and exit on the close, where One reads the real error and
	// returns it for the supervisor to reconnect — instead of hanging forever on a
	// never-closed channel under a never-cancelled parent ctx. The explicit cancel()
	// after the loop covers the other direction: if streamLoopPG returns first, it
	// unblocks the capturer's emit/receive so it can return.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	events := make(chan event.Event, defaultEventBuffer)
	captureErr := make(chan error, 1)
	go func() {
		defer close(events)
		captureErr <- cap.Run(ctx, events)
	}()

	// The health probe reads slot/RI state on its own short-lived catalog connection
	// (connect-per-poll), independent of the capturer's replication and query conns.
	probe := func(c context.Context) (pgcapture.HealthSnapshot, error) {
		return pgcapture.ProbeHealth(c, cfg.QueryDSN, cfg.SlotName, cfg.Publication)
	}
	loopErr := streamLoopPG(ctx, events, idx, indexDB, cap, checkpointInterval, probe, healthPollInterval, state, logger, cfg.Hooks)
	cancel() // loop exited first → unblock the capturer's emit/receive so it returns

	// Run returns nil on ctx-cancel; only a real capture error matters.
	capErr := <-captureErr
	if loopErr != nil {
		return loopErr
	}
	if capErr != nil && !errors.Is(capErr, context.Canceled) {
		// A lost/missing slot on resume means WAL behind the checkpoint is gone for
		// good. Durably record the permanent loss (best-effort, layered on an already
		// fatal error) so index-only `status` shows the loud badge even after we exit —
		// otherwise the only trace is this one log line. Reuses the gap_lost_* columns.
		if errors.Is(capErr, pgcapture.ErrSlotLost) || errors.Is(capErr, pgcapture.ErrSlotMissingOnResume) {
			persistSlotLostPG(indexDB, cfg.ServerID, capErr.Error(), logger)
		}
		return capErr
	}
	logger.Info("pgstreamrun: stopped", "events_indexed", state.eventsIndexed,
		"last_lsn", pglogrepl.LSN(state.lsn))
	return nil
}

// persistSlotLostPG durably records a lost/missing replication slot in the shared
// stream_state.gap_lost_at/gap_lost_detail columns — the same machinery the MySQL path
// uses for an unfillable binlog gap (streamrun.persistGapAutoAdvance), so index-only
// `status` shows the loud permanent-loss banner after the process exits.
//
// It is an UPSERT, not a bare UPDATE: a slot can be detected lost on the very FIRST
// ensureSlot — before any commit has written the stream_state row — so an
// `UPDATE ... WHERE id=1` would match zero rows and silently record nothing (the exact
// false-negative this slice prevents). The INSERT seeds a minimal but complete row
// (mode/flavor/server_id/last_checkpoint are NOT NULL); ON DUPLICATE KEY preserves an
// existing checkpoint and only stamps the loss (it does not touch last_checkpoint or
// the position columns — saveCheckpointPG owns those, so a real checkpoint survives).
//
// Best-effort, layered on an ALREADY-fatal lost-slot error (no checkpoint is advanced —
// we are aborting): if the stamp fails we log and let the original fatal error
// propagate, never masking it. The detail is the lost-slot error text, which names the
// slot but carries no DSN/secret.
//
// Mid-stream invalidation is NOT routed here: once streaming, a lost slot surfaces as a
// raw replication-protocol error without the sentinel, so One returns it unstamped; the
// supervisor re-invokes One, and ensureSlot re-detects the lost slot at startup, which
// reaches this stamp on that next run.
func persistSlotLostPG(db *sql.DB, serverID uint32, detail string, logger *slog.Logger) {
	_, err := db.Exec(`
		INSERT INTO stream_state (id, mode, flavor, server_id, last_checkpoint, gap_lost_at, gap_lost_detail)
		VALUES (1, 'gtid', ?, ?, UTC_TIMESTAMP(), UTC_TIMESTAMP(), ?)
		ON DUPLICATE KEY UPDATE
			gap_lost_at     = UTC_TIMESTAMP(),
			gap_lost_detail = VALUES(gap_lost_detail)`,
		pgFlavor, serverID, detail)
	if err != nil {
		logger.Error("pgstreamrun: could not persist lost-slot record; `status` will not show the permanent-loss banner",
			"error", err, "detail", detail)
	}
}

// sourceHealthJSON is the wire/persisted shape written to stream_state.source_health
// (#599) — a clean, frontend-friendly projection of pgcapture.HealthSnapshot.
// SlotHealth's sql.NullInt64 and pglogrepl.LSN do not marshal to a usable JSON shape,
// so this flattens them: SafeWalSize is a *int64 (null = unlimited retention or an
// already-lost slot), LSNs are their "X/Y" strings. CheckedAt is RFC3339 UTC so the
// index-only console can compute staleness unambiguously — a frozen snapshot over a
// dead daemon must read as stale, not healthy.
type sourceHealthJSON struct {
	Exists                 bool     `json:"exists"`
	Active                 bool     `json:"active"`
	WalStatus              string   `json:"wal_status"`
	RetainedBytes          int64    `json:"retained_bytes"`
	SafeWalSize            *int64   `json:"safe_wal_size"`
	RestartLSN             string   `json:"restart_lsn"`
	ConfirmedFlushLSN      string   `json:"confirmed_flush_lsn"`
	ReplicaIdentityNotFull []string `json:"replica_identity_not_full"`
	CheckedAt              string   `json:"checked_at"`
	// ProbeError, when set, means this snapshot records a FAILED probe rather than a
	// reading — the slot fields are absent/zero. Persisted so the console shows an
	// explicit "probe failing" state instead of a blank panel (see buildProbeErrorJSON).
	ProbeError string `json:"probe_error,omitempty"`
}

// buildSourceHealthJSON marshals a probe result + the wall-clock it was taken into the
// wire shape. checkedAt is stamped by the caller (not inside, so a test is deterministic).
func buildSourceHealthJSON(snap pgcapture.HealthSnapshot, checkedAt time.Time) ([]byte, error) {
	h := sourceHealthJSON{
		Exists:                 snap.Slot.Exists,
		Active:                 snap.Slot.Active,
		WalStatus:              snap.Slot.WalStatus,
		RetainedBytes:          snap.Slot.RetainedBytes,
		RestartLSN:             snap.Slot.RestartLSN.String(),
		ConfirmedFlushLSN:      snap.Slot.ConfirmedFlushLSN.String(),
		ReplicaIdentityNotFull: snap.ReplicaIdentityNotFull,
		CheckedAt:              checkedAt.UTC().Format(time.RFC3339),
	}
	if snap.Slot.SafeWalSize.Valid {
		v := snap.Slot.SafeWalSize.Int64
		h.SafeWalSize = &v
	}
	if h.ReplicaIdentityNotFull == nil {
		h.ReplicaIdentityNotFull = []string{} // marshal as [] not null — a cleaner contract for the frontend
	}
	return json.Marshal(h)
}

// buildProbeErrorJSON records a FAILED probe as a source_health snapshot so the console
// renders an explicit "probe failing" state instead of nothing. Without this, a probe
// that never once succeeds — a standby source (the slot-health query runs the
// primary-only pg_current_wal_lsn()), or a query-DSN auth/network failure that begins
// after startup — would leave the panel ABSENT, indistinguishable from "no daemon
// configured", and the staleness indicator cannot age a snapshot that was never written.
// That is a silent failure in the exact visibility path #599 exists to provide. The slot
// fields stay zero; the frontend keys off probe_error.
func buildProbeErrorJSON(probeErr string, checkedAt time.Time) ([]byte, error) {
	return json.Marshal(sourceHealthJSON{
		ReplicaIdentityNotFull: []string{},
		CheckedAt:              checkedAt.UTC().Format(time.RFC3339),
		ProbeError:             probeErr,
	})
}

// saveSourceHealth persists the latest source-health snapshot to
// stream_state.source_health (#599). Like persistSlotLostPG it is an UPSERT, not a bare
// UPDATE: the daemon polls health before the first commit writes the row (the slot
// exists from startup), so an `UPDATE ... WHERE id=1` would match zero rows and silently
// drop every snapshot until the first checkpoint — the console would show no health on
// an idle stream. The INSERT seeds the NOT NULL columns; ON DUPLICATE KEY touches ONLY
// source_health, never the checkpoint/position columns (saveCheckpointPG owns those, so
// a real checkpoint survives this write). Best-effort: a failed write is logged, never
// fatal to the stream.
func saveSourceHealth(db *sql.DB, serverID uint32, healthJSON []byte, logger *slog.Logger) {
	// #959: bound the health write so a stalled index link cannot wedge the poll loop.
	ctx, cancel := context.WithTimeout(context.Background(), indexer.WriteTimeout)
	defer cancel()
	_, err := db.ExecContext(ctx, `
		INSERT INTO stream_state (id, mode, flavor, server_id, last_checkpoint, source_health)
		VALUES (1, 'gtid', ?, ?, UTC_TIMESTAMP(), ?)
		ON DUPLICATE KEY UPDATE
			source_health = VALUES(source_health)`,
		pgFlavor, serverID, healthJSON)
	if err != nil {
		logger.Warn("pgstreamrun: could not persist source-health snapshot; the console health panel will stay stale",
			"error", err)
	}
}

// The caps on one source transaction held in an open index transaction
// (#2391): see indexer.DefaultMaxTxnRows. Variables so a test can shrink them.
var (
	maxTxnRows = indexer.DefaultMaxTxnRows
	maxTxnOpen = indexer.DefaultMaxTxnOpen
)

// streamLoopPG batches row events into the indexer and checkpoints the durable LSN.
//
// The checkpoint cursor is the last COMPLETE transaction's commit LSN (lastCommitLSN)
// and nothing else — never a per-row LSN. Rows of an in-flight transaction are held
// uncommitted in an index transaction until its commit (#2391), and the checkpoint
// stays at the last commit, so a crash resumes at a transaction boundary and
// PostgreSQL re-delivers the in-flight tail; only a transaction split past the
// caps can leave part of itself in the index to be delivered again. This is the #491 invariant. The order at every checkpoint is
// flush → saveCheckpointPG → AckCommitted: PostgreSQL WAL is released only after the
// rows are durably indexed AND the checkpoint is durably persisted.
func streamLoopPG(
	ctx context.Context,
	events <-chan event.Event,
	idx *indexer.Indexer,
	indexDB *sql.DB,
	cap *pgcapture.Capturer,
	checkpointInterval time.Duration,
	probe func(context.Context) (pgcapture.HealthSnapshot, error),
	healthInterval time.Duration,
	state *pgStreamState,
	logger *slog.Logger,
	hooks *Hooks,
) error {
	ticker := time.NewTicker(checkpointInterval)
	defer ticker.Stop()

	// Source-health polling (#599) shares the loop's single goroutine — no concurrent
	// writer of the stream_state row, so saveSourceHealth can never race saveCheckpointPG.
	// healthC stays nil when no probe is configured (the case then never fires).
	var healthC <-chan time.Time
	if probe != nil {
		healthTicker := time.NewTicker(healthInterval)
		defer healthTicker.Stop()
		healthC = healthTicker.C
	}
	pollHealth := func() {
		pctx, cancel := context.WithTimeout(ctx, healthPollTimeout)
		defer cancel()
		snap, err := probe(pctx)
		if err != nil {
			// A cancellation is a graceful shutdown (the loop's ctx is done), not a
			// source-side failure — don't persist a spurious "probe failing" snapshot that
			// would make a clean stop look broken. A poll TIMEOUT (pctx deadline, ctx still
			// live) is a real hung-source failure and DOES fall through to be recorded.
			if ctx.Err() != nil {
				return
			}
			logger.Warn("pgstreamrun: source-health probe failed; recording it so the console shows the failure, not a blank panel", "error", err)
			// Persist the failure (not just log it): a never-written snapshot leaves the
			// panel absent and the staleness net has nothing to age — the silent-failure
			// the visibility feature must not have. buildProbeErrorJSON cannot fail.
			if errJSON, mErr := buildProbeErrorJSON(err.Error(), time.Now()); mErr == nil {
				saveSourceHealth(indexDB, state.serverID, errJSON, logger)
			}
			return
		}
		healthJSON, err := buildSourceHealthJSON(snap, time.Now())
		if err != nil {
			logger.Warn("pgstreamrun: marshal source-health snapshot", "error", err)
			return
		}
		saveSourceHealth(indexDB, state.serverID, healthJSON, logger)
	}

	var lastCommitLSN uint64

	// snapshotByTable maps "schema.table" → the snapshot_id of that table's most
	// recent EventRelation (#533). Per-TABLE, not a single scalar: pgoutput sends a
	// RelationMessage once per relation per session, so a second table's snapshot
	// would otherwise clobber the first, and every later row of the first table would
	// be stamped the wrong snapshot_id (recovery would silently fall back to an
	// all-columns WHERE — #531 left unclosed for all but the last table). A map miss
	// yields 0 → the safe SchemaVersion-0 all-columns fallback, which an in-scope row
	// never hits (its RelationMessage always precedes it).
	snapshotByTable := make(map[string]uint32)

	// inferredSeen counts the inferred schema changes written in the current
	// transaction, so a resent one is recognised without dropping a second
	// real change of the same kind in the same transaction (#2441).
	inferredSeen := &inferredCounter{}

	// The writer (#2391): rows of complete transactions are batched together,
	// and the transaction being read is held in an index transaction of its
	// own until its commit, so a reader never sees part of it (past the
	// indexer's row/time caps it is made visible in parts, and logged).
	w := indexer.NewTxnBatcher(idx, maxTxnRows, maxTxnOpen, indexer.TxnBatcherHooks{
		Published: func(n int64, _ []indexer.CommitStamp) {
			state.eventsIndexed += n
			// Liveness: any rows indexed flip a supervised job pending→running.
			if n > 0 && hooks != nil && hooks.OnIndexed != nil {
				hooks.OnIndexed(n)
			}
		},
		Split: func(why string, rows int64, open time.Duration, first bool) {
			if first {
				logger.Warn("pgstreamrun: a source transaction is too large to make visible all at once; readers can see part of it until its commit is captured",
					"reason", why, "rows_so_far", rows, "open_for", open.Round(time.Millisecond))
			}
		},
	})
	defer w.Close()

	// checkpoint commits the rows of complete transactions, then persists the
	// last commit LSN. The order is commit → saveCheckpointPG → AckCommitted:
	// PostgreSQL releases WAL only after the rows are durably indexed AND the
	// checkpoint is durably persisted. Every row at or before lastCommitLSN
	// belongs to a complete transaction, so it is in the index by then.
	checkpoint := func() error {
		if err := w.CommitComplete(); err != nil {
			return err
		}
		if lastCommitLSN == 0 {
			return nil // nothing committed yet — nothing durable to checkpoint
		}
		if err := saveCheckpointPG(indexDB, state, lastCommitLSN); err != nil {
			return err
		}
		// Only now is it safe to let PostgreSQL release WAL up to lastCommitLSN.
		if cap != nil {
			cap.AckCommitted(lastCommitLSN)
		}
		state.lsn = lastCommitLSN
		return nil
	}

	// stop drops the rows of a transaction still being read before the last
	// checkpoint: committing them would leave half a transaction visible while
	// capture is down. PostgreSQL re-delivers it from the checkpoint.
	stop := func() error {
		if w.InTxn() {
			if n := w.Discard(); n > 0 {
				logger.Info("pgstreamrun: capture stopped inside a source transaction; its rows are not kept, and the next start receives it again",
					"rows_discarded", n)
			}
		}
		return checkpoint()
	}

	for {
		select {
		case <-ctx.Done():
			if err := stop(); err != nil {
				logger.Warn("pgstreamrun: final checkpoint failed on shutdown", "error", err)
			}
			return nil

		case <-w.CapC():
			if err := w.OnCap(); err != nil {
				return err
			}

		case <-ticker.C:
			if err := checkpoint(); err != nil {
				return err
			}
			// Fire on the ticker, NOT inside checkpoint(): checkpoint early-returns
			// at lastCommitLSN==0 for a connected-but-idle source, and liveness must
			// still signal so the supervised job leaves "pending".
			if hooks != nil && hooks.OnCheckpoint != nil {
				hooks.OnCheckpoint()
			}

		case <-healthC:
			// Inline in the loop (no second goroutine): a ~tens-of-ms catalog read every
			// healthInterval, bounded by healthPollTimeout, so a hung source pauses event
			// draining only briefly. Errors are logged, never fatal — a failed health
			// poll must not abort the stream.
			pollHealth()

		case ev, ok := <-events:
			if !ok {
				if err := stop(); err != nil {
					logger.Warn("pgstreamrun: final checkpoint failed", "error", err)
				}
				return nil
			}
			// An inferred schema change is dated at the LOW end of its bracket
			// (#2441), possibly long before the stream's position, so it is not
			// a reading of how far the stream is. (The change and commit of the
			// same transaction follow it and would set the time anyway; this
			// keeps a checkpoint between them from saving the older time.)
			if !ev.Timestamp.IsZero() && !strings.HasPrefix(ev.DDLQuery, event.InferredDDLPrefix) {
				state.lastEventTime = sql.NullTime{Time: ev.Timestamp, Valid: true}
			}
			switch ev.EventType {
			case event.EventCommit:
				// The boundary: record the commit LSN; its rows are already in the
				// writer (pgoutput delivers them before the commit).
				lastCommitLSN = ev.EndPos
				inferredSeen.reset()
				if err := w.End(); err != nil {
					return err
				}
			case event.EventDDL:
				// A DDL marker: a TRUNCATE audit record (#827), or a schema change
				// inferred from a Relation shape (#2441; the decoder emits it
				// BEFORE that shape's EventRelation, so a crash between the two
				// re-delivers and re-records it rather than losing it). Persist it to schema_changes immediately, the
				// source-neutral analog of the MySQL DDL path, so the truncate is a
				// durable, queryable record (bintrail-mcp list_schema_changes / the
				// schema_changes table) rather than a transient warn. Like the
				// EventRelation snapshot it commits in its own statement, OUTSIDE the
				// batch→checkpoint→ack sequence: it does NOT advance lastCommitLSN (the
				// TRUNCATE is mid-transaction; the following EventCommit advances the
				// cursor) and does NOT force a flush of the in-flight batch. A crash
				// before the next checkpoint re-delivers the TRUNCATE and re-records a
				// benign duplicate marker.
				if dup, err := inferredSeen.recorded(indexDB, ev); err != nil {
					return fmt.Errorf("pgstreamrun: check for an already recorded %s on %s.%s: %w", ev.DDLType, ev.Schema, ev.Table, err)
				} else if dup {
					continue
				}
				if err := indexer.InsertSchemaChange(indexDB, ev, nil); err != nil {
					return fmt.Errorf("pgstreamrun: record %s for %s.%s: %w", ev.DDLType, ev.Schema, ev.Table, err)
				}
			case event.EventRelation:
				// A relation's shape (#533): persist it as a schema snapshot and record
				// its snapshot_id for stamping this table's subsequent rows. The snapshot
				// commits immediately (its own txn), BEFORE the rows referencing it are
				// flushed — so "snapshot durable before its rows durable" holds; it sits
				// outside the flush→checkpoint→ack sequence, so it does not advance the
				// cursor and does NOT force a flush (already-batched rows carry their own,
				// already-persisted snapshot_ids — per-row schema_version makes a mixed
				// batch correct). A crash before the next checkpoint just re-delivers and
				// re-snapshots (a benign orphan id).
				snapCtx, snapCancel := context.WithTimeout(context.Background(), indexer.WriteTimeout)
				id, werr := metadata.WritePGSnapshot(snapCtx, indexDB, ev.Relation)
				snapCancel()
				if werr != nil {
					return fmt.Errorf("pgstreamrun: persist schema snapshot for %s.%s: %w", ev.Schema, ev.Table, werr)
				}
				snapshotByTable[ev.Schema+"."+ev.Table] = uint32(id)
			case event.EventGTID:
				// PostgreSQL does not emit a leading GTID marker; ignore defensively.
			default:
				ev.SchemaVersion = snapshotByTable[ev.Schema+"."+ev.Table]
				if err := w.Add(ev); err != nil {
					return err
				}
			}
		}
	}
}

// loadStreamStatePG loads the single-row checkpoint (id=1) for a PostgreSQL source.
// It returns nil on first run (no row). If a row exists but is NOT a PostgreSQL
// checkpoint, it fails loud rather than misread/clobber a MySQL/MariaDB checkpoint
// (stream_state is single-row; an index DB serves one source).
func loadStreamStatePG(db *sql.DB) (*pgStreamState, error) {
	var (
		flavor        string
		lsn           uint64
		eventsIndexed int64
		serverID      uint32
		lastEventTime sql.NullTime
	)
	err := db.QueryRow(`SELECT flavor, binlog_position, events_indexed, server_id, last_event_time
		FROM stream_state WHERE id = 1`).Scan(&flavor, &lsn, &eventsIndexed, &serverID, &lastEventTime)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pgstreamrun: load checkpoint: %w", err)
	}
	if flavor != pgFlavor {
		return nil, fmt.Errorf("pgstreamrun: index database holds a %q checkpoint, not %q — refusing to stream a PostgreSQL source into a non-PostgreSQL index", flavor, pgFlavor)
	}
	if lsn == 0 {
		// A row without a durable LSN cursor is NOT a resumable checkpoint.
		// saveCheckpointPG never writes position 0 (the loop skips lastCommitLSN==0,
		// and 0/0 is not a valid PostgreSQL LSN), so a zero-position row was seeded
		// by persistSlotLostPG/saveSourceHealth before any commit, or is the cleared
		// row `bintrail-pg reset` leaves behind (#1082: reset preserves the row so a
		// gap_lost_* continuity-loss record survives it). Treat it as first-run —
		// the capturer creates a fresh slot instead of demanding the one reset just
		// dropped — and leave the loss record untouched for `status` to surface.
		return nil, nil
	}
	return &pgStreamState{lsn: lsn, eventsIndexed: eventsIndexed, serverID: serverID, lastEventTime: lastEventTime}, nil
}

// saveCheckpointPG persists the durable cursor (commitLSN — the last complete
// transaction) and the running counters. mode='gtid'/flavor='postgres': the LSN
// rides binlog_position (uint64) and binlog_file/gtid_set (the "X/Y" string).
func saveCheckpointPG(db *sql.DB, state *pgStreamState, commitLSN uint64) error {
	lsnStr := pglogrepl.LSN(commitLSN).String()
	var lastEventTime any
	if state.lastEventTime.Valid {
		lastEventTime = state.lastEventTime.Time
	}
	// #959: bound the checkpoint write so a mid-statement stall surfaces as an
	// error within WriteTimeout instead of freezing the stream on TCP retransmit.
	ctx, cancel := context.WithTimeout(context.Background(), indexer.WriteTimeout)
	defer cancel()
	_, err := db.ExecContext(ctx, `
		INSERT INTO stream_state
			(id, mode, binlog_file, binlog_position, gtid_set, flavor,
			 events_indexed, last_event_time, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(), ?)
		ON DUPLICATE KEY UPDATE
			binlog_file     = VALUES(binlog_file),
			binlog_position = VALUES(binlog_position),
			gtid_set        = VALUES(gtid_set),
			flavor          = VALUES(flavor),
			events_indexed  = VALUES(events_indexed),
			last_event_time = VALUES(last_event_time),
			last_checkpoint = UTC_TIMESTAMP(),
			server_id       = VALUES(server_id)`,
		lsnStr, commitLSN, lsnStr, pgFlavor, state.eventsIndexed, lastEventTime, state.serverID)
	if err != nil {
		return fmt.Errorf("pgstreamrun: save checkpoint at %s: %w", lsnStr, err)
	}
	return nil
}

// resolveStartLSN picks the start LSN: a saved checkpoint wins (idempotent resume);
// otherwise the explicit Config.StartLSN; otherwise 0 — on first run 0 is correct,
// the capturer creates the slot and starts from its ConsistentPoint.
func resolveStartLSN(saved *pgStreamState, flagLSN uint64, logger *slog.Logger) uint64 {
	if saved != nil {
		logger.Info("pgstreamrun: resuming from checkpoint", "lsn", pglogrepl.LSN(saved.lsn))
		return saved.lsn
	}
	if flagLSN != 0 {
		logger.Info("pgstreamrun: starting from configured LSN", "lsn", pglogrepl.LSN(flagLSN))
		return flagLSN
	}
	logger.Info("pgstreamrun: first run — starting from the slot's consistent point")
	return 0
}

// loadKnownState reads what DDL inference needs at start (#2441): the shapes
// a resend from the slot's position is compared against, and the inferred
// DROP/RENAME rows already recorded, which say where a table gave up a name.
func loadKnownState(indexDB *sql.DB, from uint64) (pgcapture.KnownState, error) {
	shapes, err := loadKnownShapes(indexDB, from)
	if err != nil {
		return pgcapture.KnownState{}, err
	}
	releases, err := loadKnownReleases(indexDB)
	if err != nil {
		return pgcapture.KnownState{}, err
	}
	return pgcapture.KnownState{Shapes: shapes, Releases: releases}, nil
}

// loadKnownReleases reads the inferred DROP TABLE and RENAME TABLE rows that
// carry the text (not a rename's pointer row under its new name): each says
// table OID gave up schema_name.table_name at binlog_pos.
func loadKnownReleases(indexDB *sql.DB) ([]pgcapture.KnownRelease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), indexer.WriteTimeout)
	defer cancel()
	rows, err := indexDB.QueryContext(ctx, `
		SELECT schema_name, table_name, binlog_pos, LEFT(ddl_query, 600) FROM schema_changes
		WHERE ddl_type IN ('DROP TABLE', 'RENAME TABLE') AND ddl_query LIKE ?`,
		event.InferredDDLPrefix+"%")
	if err != nil {
		return nil, fmt.Errorf("pgstreamrun: load recorded table drops and renames: %w", err)
	}
	defer rows.Close()
	var out []pgcapture.KnownRelease
	for rows.Next() {
		var r pgcapture.KnownRelease
		var head string
		if err := rows.Scan(&r.Schema, &r.Table, &r.LSN, &head); err != nil {
			return nil, fmt.Errorf("pgstreamrun: scan recorded table drops and renames: %w", err)
		}
		oid, ok := inferredOID(head)
		if !ok {
			continue
		}
		if end := strings.Index(head, "*/"); end < 0 || strings.HasPrefix(strings.TrimSpace(head[end+2:]), "(same ") {
			continue // a pointer row: its table is the name taken, not given up
		}
		r.OID = oid
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgstreamrun: load recorded table drops and renames: %w", err)
	}
	return out, nil
}

// loadKnownShapes reads the stored shapes per table for DDL inference.
func loadKnownShapes(indexDB *sql.DB, from uint64) ([]pgcapture.KnownShape, error) {
	shapes, err := metadata.LoadPGShapes(context.Background(), indexDB, from)
	if err != nil {
		return nil, fmt.Errorf("pgstreamrun: load the last known table shapes: %w", err)
	}
	out := make([]pgcapture.KnownShape, len(shapes))
	for i, s := range shapes {
		k := pgcapture.KnownShape{OID: s.Relation.OID, Schema: s.Relation.Schema, Table: s.Relation.Table,
			SeenLSN: s.Relation.SeenLSN, SeenAt: s.Relation.SeenAt}
		for _, c := range s.Relation.Columns {
			k.Columns = append(k.Columns, pgcapture.ShapeColumn{Name: c.Name, TypeOID: c.TypeOID, TypeMod: c.TypeMod})
		}
		out[i] = k
	}
	return out, nil
}

// lastIndexedChange returns the newest indexed change to a table that
// committed before an LSN: the lower bound of a schema change when the stream
// has not decoded one since it started (#2441). The plain comparison uses
// idx_row_lookup; the BINARY one keeps the index's case-insensitive collation
// from answering for "A" when asked about a.
func lastIndexedChange(indexDB *sql.DB) func(schema, table string, beforeLSN uint64) (uint64, time.Time, bool, error) {
	return func(schema, table string, beforeLSN uint64) (uint64, time.Time, bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), indexer.WriteTimeout)
		defer cancel()
		var lsn uint64
		var at time.Time
		err := indexDB.QueryRowContext(ctx, `
			SELECT end_pos, event_timestamp FROM binlog_events
			WHERE schema_name = ? AND table_name = ?
			  AND BINARY schema_name = ? AND BINARY table_name = ?
			  AND end_pos < ?
			ORDER BY event_timestamp DESC, event_id DESC
			LIMIT 1`, schema, table, schema, table, beforeLSN).Scan(&lsn, &at)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, time.Time{}, false, nil
		}
		if err != nil {
			return 0, time.Time{}, false, err
		}
		return lsn, at.UTC(), true, nil
	}
}

// inferredCounter recognises an inferred schema change (#2441) that is
// already in schema_changes, written before a restart made PostgreSQL resend
// the transaction that produced it.
//
// The same change is the same table OID at the same stream position with the
// same kind; a DROP and a RENAME of one OID at one position count as one kind
// (which of the two depends on the live catalog, read again on the resend).
// The text is not compared: a resend can word the same change differently.
// One transaction can change one table twice the same way (two ALTERs), so
// existence is not enough: the n-th such change of a transaction is a resend
// only when the index already holds more than n-1 of them. A resend emits
// them in the same order. Other DDL markers are not checked (a TRUNCATE
// recorded twice stays a benign duplicate, as before).
type inferredCounter struct{ emitted map[string]int }

func (c *inferredCounter) reset() { c.emitted = nil }

func inferredFamily(k event.DDLKind) string {
	if k == event.DDLDropTable || k == event.DDLRenameTable {
		return "gone"
	}
	return string(k)
}

func (c *inferredCounter) recorded(indexDB *sql.DB, ev event.Event) (bool, error) {
	if !strings.HasPrefix(ev.DDLQuery, event.InferredDDLPrefix) {
		return false, nil
	}
	oid, ok := inferredOID(ev.DDLQuery)
	if !ok {
		return false, nil
	}
	// The row that carries the text: the first table's (a rename writes a
	// second row, under the new name, that points at it).
	sch, tbl := ev.Schema, ev.Table
	if len(ev.DDLTables) > 0 {
		sch, tbl = ev.DDLTables[0].Schema, ev.DDLTables[0].Table
	}
	fam := inferredFamily(ev.DDLType)
	key := fmt.Sprintf("%d/%d/%s", ev.EndPos, oid, fam)
	if c.emitted == nil {
		c.emitted = map[string]int{}
	}
	before := c.emitted[key]
	c.emitted[key] = before + 1

	ctx, cancel := context.WithTimeout(context.Background(), indexer.WriteTimeout)
	defer cancel()
	rows, err := indexDB.QueryContext(ctx, `
		SELECT ddl_type FROM schema_changes
		WHERE binlog_pos = ? AND binlog_file = ?
		  AND BINARY schema_name = ? AND BINARY table_name = ?
		  AND ddl_query LIKE ?`,
		ev.EndPos, ev.BinlogFile, sch, tbl, fmt.Sprintf("%s oid=%d;%%", event.InferredDDLPrefix, oid))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	held := 0
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			return false, err
		}
		if inferredFamily(event.DDLKind(kind)) == fam {
			held++
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return held > before, nil
}

// inferredOID reads the table OID from an inferred row's marker.
func inferredOID(q string) (uint32, bool) {
	rest := strings.TrimPrefix(q, event.InferredDDLPrefix)
	f, _, _ := strings.Cut(strings.TrimSpace(rest), ";")
	v, found := strings.CutPrefix(f, "oid=")
	if !found {
		return 0, false
	}
	n, err := strconv.ParseUint(v, 10, 32)
	return uint32(n), err == nil && n != 0
}
