package pgcapture

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// defaultStandbyInterval is used when StandbyInterval is unset and wal_sender_timeout
// cannot be read (or is disabled). It is conservatively below the PostgreSQL default
// wal_sender_timeout (60s) so a quiet stream still refreshes the server's liveness
// timer well within the deadline.
const defaultStandbyInterval = 10 * time.Second

// standbyWriteTimeout bounds each standby status write so a stalled socket (TCP
// backpressure while we are parked emitting, not draining) cannot block the loop —
// and ctx-cancel — forever. See sendStandby.
const standbyWriteTimeout = 10 * time.Second

// startupTimeout bounds the one-shot catalog/slot work in Run (publication + slot
// checks, consistent-point creation, wal_sender_timeout read) so a hung catalog
// can't wedge Run before streaming begins. StartReplication and the receive loop
// deliberately use the full run ctx — they are long-lived.
const startupTimeout = 30 * time.Second

// beforeStartReplication, when set, runs right before Run starts
// replication, after it read the slot's position: a test makes the slot's
// creation end there (SetBeforeStartReplicationForTest).
var beforeStartReplication func()

// SetBeforeStartReplicationForTest makes fn run in every Run right before
// replication starts, for tests of what a capture records when the slot's
// position could not be read before that moment (#3078).
func SetBeforeStartReplicationForTest(fn func()) (restore func()) {
	prev := beforeStartReplication
	beforeStartReplication = fn
	return func() { beforeStartReplication = prev }
}

// Config binds everything Run needs. ReplDSN MUST carry replication=database (it is
// a CopyBoth replication connection and cannot run queries); QueryDSN is a normal
// connection used for the catalog PK lookup and the slot/publication checks.
type Config struct {
	ReplDSN     string
	QueryDSN    string
	SlotName    string
	Publication string
	Filters     event.Filters
	StartLSN    pglogrepl.LSN // the resume checkpoint; 0 on first run (see ensureSlot — the gate is slot existence, not this)
	// ExpectExistingSlot, set by the consumer when resuming from a saved checkpoint,
	// makes ensureSlot fail loud if the slot is missing or invalidated (wal_status=
	// 'lost') instead of silently creating a fresh slot that would skip the WAL since
	// the checkpoint. Leave false on first run. See ensureSlot / #532.
	ExpectExistingSlot bool
	// StandbyInterval is how often a standby status update is sent (server liveness +
	// confirmed_flush_lsn feedback). 0 derives it from the server's wal_sender_timeout
	// (timeout/3, floor 1s), falling back to defaultStandbyInterval. Tests set it low.
	StandbyInterval time.Duration
	Logger          *slog.Logger
	// OnStarted, when set, is called once replication has started on the
	// slot: both source connections are open and the stream is attached.
	OnStarted func()
	// LoadKnownShapes and LastChange turn on DDL inference (#2441): what the
	// index holds of each table's shapes for a resend starting at fromLSN
	// (the slot's confirmed position, read at start), and the newest indexed
	// change to a table before an LSN. Both nil = inference off.
	LoadKnownShapes func(fromLSN uint64) (KnownState, error)
	LastChange      func(schema, table string, beforeLSN uint64) (lsn uint64, at time.Time, ok bool, err error)
	// Skips is the capture-skip ledger: each change of a table capture does not
	// take (a primary key declared DEFERRABLE, a replica identity that does not
	// hold the primary key) is dropped and recorded there under its reason, and
	// each captured row breaks its run of consecutive skips. The consumer
	// persists it with its checkpoints. nil records nothing (the changes are
	// still dropped, with the warning).
	Skips *parser.SkipCounters

	// captureBelowFull turns off the decoder's gate on tables below REPLICA
	// IDENTITY FULL (Decoder.captureBelowFull). Unexported: only this
	// package's tests set it (export_test.go).
	captureBelowFull bool
}

// Capturer decodes a PostgreSQL logical-replication stream into event.Event. It
// mirrors parser.StreamParser: a producer whose Run emits on an out channel,
// returning nil on graceful ctx-cancel and a non-nil error on a decode/network
// failure (so a supervisor reconnects). The one divergence from StreamParser is the
// consumer→server feedback PostgreSQL requires: see AckCommitted.
type Capturer struct {
	cfg    Config
	logger *slog.Logger
	// lastAcked is the confirmed_flush_lsn the consumer has DURABLY persisted; every
	// standby status update reports it and never anything ahead, so PostgreSQL can
	// never release WAL for rows the index has not durably recorded.
	lastAcked atomic.Uint64
	// startedFrom is the slot position this run started reading from; 0
	// until replication has started, and when it could not be read.
	startedFrom atomic.Uint64
}

// StartedFrom is where this run started reading the source from: the
// consistent point of the slot it created, or, for a slot that was already
// there, the slot's own position (SlotFloorLSN) read before replication
// started (or, when it could not be read then, right after replication
// started, before anything was acknowledged: the same position). Every
// transaction that commits at or after it is delivered. 0 before replication
// starts, and on a resume whose read failed: no position, never a guess. A
// first run whose position cannot be read at all does not start (#3078).
//
// It is what a first run records as the start of the capture (#2843). On a
// resume it is the saved checkpoint, which is not a start.
func (c *Capturer) StartedFrom() uint64 { return c.startedFrom.Load() }

// New constructs a Capturer. It does not open connections (Run does), mirroring
// NewStreamParser's no-I/O constructor. logger may be nil (slog.Default()).
func New(cfg Config) *Capturer {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Capturer{cfg: cfg, logger: logger}
}

// AckCommitted records the LSN the consumer has DURABLY persisted (its checkpoint
// saved after the batch was flushed to the index). The Run loop reports this in
// every standby status update and never a position ahead of it — the #491 invariant
// in PostgreSQL clothing, here additionally guarding the source's WAL (acking before
// persisting would let PostgreSQL drop WAL the index never recorded, unrecoverable).
// The consumer (#534) must call this with the EventCommit commit LSN, only after its
// saveCheckpoint succeeds. Safe to call concurrently with Run.
func (c *Capturer) AckCommitted(lsn uint64) {
	// Advance-only: lastAcked is durable progress, which is monotonic, so a stale or
	// out-of-order ack (a consumer retry/ordering race) must not regress it — that
	// would make the next standby update report a lower confirmed_flush_lsn. (Not a
	// data-loss guard — PostgreSQL clamps a lower client LSN forward and re-delivers;
	// this keeps the reported cursor monotonic by construction.)
	for {
		old := c.lastAcked.Load()
		if lsn <= old {
			return
		}
		if c.lastAcked.CompareAndSwap(old, lsn) {
			return
		}
	}
}

// Run opens the replication + query connections, validates the publication, ensures
// the slot, starts replication, and emits event.Event on out until ctx is cancelled.
// It returns nil on graceful ctx-cancel and does NOT close out (the caller owns the
// channel, mirroring streamrun).
func (c *Capturer) Run(ctx context.Context, out chan<- event.Event) error {
	if c.cfg.ReplDSN == "" || c.cfg.QueryDSN == "" || c.cfg.SlotName == "" || c.cfg.Publication == "" {
		return fmt.Errorf("pgcapture: Run requires ReplDSN, QueryDSN, SlotName, and Publication")
	}

	// Pinned rendering GUCs (#593 slice D, gucpin.go): the walsender session's
	// GUCs determine pgoutput's tuple text, which must byte-match the baseline
	// COPY text (pinned identically in internal/pgbaseline).
	replConn, err := ConnectReplPinned(ctx, c.cfg.ReplDSN)
	if err != nil {
		return fmt.Errorf("pgcapture: connect replication: %w", err)
	}
	defer closeReplConn(replConn)

	queryConn, err := pgx.Connect(ctx, c.cfg.QueryDSN)
	if err != nil {
		return fmt.Errorf("pgcapture: connect query: %w", err)
	}
	defer closeQueryConn(queryConn)

	// Bound the one-shot startup catalog/slot work so a hung catalog can't wedge Run
	// before streaming begins (StartReplication and the receive loop below use the
	// full run ctx — they are long-lived, these are not).
	startupCtx, cancelStartup := context.WithTimeout(ctx, startupTimeout)
	defer cancelStartup()

	if err := validatePublication(startupCtx, queryConn, c.cfg.Publication, c.cfg.Filters); err != nil {
		return err
	}
	// Before the replica identity warning, whose message tells an operator how
	// to set REPLICA IDENTITY FULL: on PostgreSQL 18 that is what stops the
	// source's application from writing to a table with a generated column.
	if err := CheckGeneratedColumns(startupCtx, queryConn, c.cfg.Publication); err != nil {
		return err
	}
	// A table whose primary key is DEFERRABLE does not stop capture: the
	// decoder skips it, each time PostgreSQL describes it
	// (WithDeferrableResolver), and the other tables go on. Refusing to start
	// over one such table kept EVERY table uncaptured, and for good: each
	// automatic restart (a network cut, a write that timed out) was refused
	// again, while the slot went on retaining WAL on the source. Said here once,
	// for the tables already in the publication; a failed read only loses the
	// warning.
	if found, err := QueryDeferrableKeys(startupCtx, queryConn, c.cfg.Publication); err != nil {
		c.logger.Warn("pgcapture: could not list the published tables whose primary key is DEFERRABLE; each table is still checked when PostgreSQL describes it", "error", err)
	} else {
		var kept []DeferrableKey
		for _, k := range found {
			if c.cfg.Filters.Matches(k.Schema, k.Table) {
				kept = append(kept, k)
			}
		}
		if len(kept) > 0 {
			c.logger.Warn("pgcapture: " + DeferrableKeysProblem(kept) + " Until then the changes of " + thisOrThese(len(kept)) + " are NOT captured (each one is counted as skipped in capture health); the other tables are captured normally")
		}
	}
	// A table below REPLICA IDENTITY FULL is not captured (the decoder's gate,
	// requireFull): said here once, at start, for the tables already in the
	// publication; the decoder says it again for each description of such a
	// table it receives, and is where its changes are dropped and counted.
	belowFull, err := validateReplicaIdentity(startupCtx, queryConn, c.cfg.Publication, c.cfg.Filters)
	if err != nil {
		return err
	}
	if len(belowFull) > 0 {
		names := make([]string, len(belowFull))
		for i, t := range belowFull {
			names[i] = t.Name()
		}
		c.logger.Warn("pgcapture: "+countTables(len(belowFull))+" not at REPLICA IDENTITY FULL (a partitioned table counts when any of its partitions is not); their changes are NOT captured (each one is counted as skipped in capture health) until each is, the other tables are captured normally. Run ALTER TABLE <t> REPLICA IDENTITY FULL on each table, and on each partition of a partitioned one; `bintrail-pg doctor` prints the statements. On PostgreSQL 18 run the doctor first: a generated column the publication does not publish makes PostgreSQL refuse UPDATE and DELETE on a table at FULL",
			"tables", strings.Join(names, ", "))
	}

	// Non-fatal capture-coverage warnings (#555 UNLOGGED, #556 FK cascade child): tables
	// whose changes will be silently ABSENT from the index. Unlike the validate* gates
	// above, these do NOT abort capture (an operator may keep UNLOGGED data on purpose,
	// and a missing cascade child may be intentionally out of recovery scope) — but they
	// must be visible now, not discovered during a failed recovery.
	c.warnCaptureCoverage(startupCtx, queryConn)

	startLSN, err := ensureSlot(startupCtx, replConn, queryConn, c.cfg.SlotName, c.cfg.StartLSN, c.cfg.ExpectExistingSlot)
	if err != nil {
		return err
	}

	// Where the slot stands is where replication starts, whatever LSN is
	// asked for: PostgreSQL moves a lower one forward to it and never
	// rewinds a slot. So it is read from the slot itself, for the slot this
	// run just created (its consistent point) and for one that was already
	// there (a snapshot taken before the first capture creates it, or a slot
	// an earlier capture left further on), before replication starts, while
	// it is still where the slot was left. The later of that and the LSN
	// asked for is where this run reads from. A failed read is no position:
	// the LSN asked for alone could be before the slot.
	var from pglogrepl.LSN
	floor, fErr := SlotFloorLSN(startupCtx, queryConn, c.cfg.SlotName)
	if fErr == nil {
		from = max(floor, startLSN)
	}
	if hook := beforeStartReplication; hook != nil {
		hook()
	}

	if err := pglogrepl.StartReplication(ctx, replConn, c.cfg.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		Mode:       pglogrepl.LogicalReplication,
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", c.cfg.Publication)},
	}); err != nil {
		return fmt.Errorf("pgcapture: start replication slot %q from %s: %w", c.cfg.SlotName, startLSN, err)
	}
	// The read before failed: the slot was still being created (by a
	// snapshot, which then finished it before replication started), or the
	// startup budget ran out right there (#3078). A first run reads it again
	// now. Replication has started, so the slot is this run's alone (no other
	// session can acquire it) and has its position; and no standby update has
	// been sent yet (the receive loop below sends them), so the slot's
	// confirmed_flush_lsn is still exactly where PostgreSQL started this
	// stream: it moves a lower requested LSN forward to it. Left unread, the
	// first checkpoint would record no start, every resume would keep it so,
	// and the gap checks that compare a snapshot with this start would be off
	// for this server for good, with nothing to show it. So a read that
	// fails again fails the run, and the supervisor starts it again.
	if fErr != nil && !c.cfg.ExpectExistingSlot {
		rctx, cancel := context.WithTimeout(ctx, startupTimeout)
		floor, err := SlotFloorLSN(rctx, queryConn, c.cfg.SlotName)
		cancel()
		if err != nil {
			return fmt.Errorf("pgcapture: where this capture starts could not be read from replication slot %q, before replication started (%v) nor after (%w); without it the snapshot gap checks could not be made for this server, so capture is started again", c.cfg.SlotName, fErr, err)
		}
		from = max(floor, startLSN)
		c.logger.Info("pgcapture: read the slot's position once replication started, since it could not be read before", "slot", c.cfg.SlotName, "from", from, "first_error", fErr)
	}
	// Seed the durable cursor to the resume point: everything up to startLSN is
	// already durably indexed (first run = ConsistentPoint, nothing precedes it).
	c.lastAcked.Store(uint64(startLSN))
	c.startedFrom.Store(uint64(from))
	c.logger.Info("pgcapture: started", "slot", c.cfg.SlotName, "publication", c.cfg.Publication, "start_lsn", startLSN)
	if c.cfg.OnStarted != nil {
		c.cfg.OnStarted()
	}

	// Catalog-backed PKResolver over the query conn, bounded by the Run ctx (+ a
	// timeout) so a hung catalog lookup cannot wedge or outlive the stream.
	resolvePK := func(relationID uint32, schema, table string) ([]metadata.ColumnMeta, error) {
		qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return queryPK(qctx, queryConn, relationID)
	}
	// Catalog-backed AttrResolver (identity/generated, #557), same conn + bounding.
	attrResolver := func(relationID uint32, schema, table string) (map[string]ColumnAttrs, error) {
		qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return queryColumnAttrs(qctx, queryConn, relationID)
	}
	// Catalog-backed check of a deferrable primary key, asked at every
	// description of a relation; same conn + bounding.
	deferrable := func(relationID uint32, schema, table string) (string, error) {
		qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return queryDeferrableKey(qctx, queryConn, relationID)
	}
	// What the catalog says of a relation's partitions, asked at every
	// description of a relation. A failed or timed-out read does not stop the
	// stream: the decoder reads that relation's old rows one by one, and asks
	// no relation's partitions again for a minute. On a connection of its
	// own, opened again when it is found closed: a read that times out closes
	// the connection it ran on, which would end the stream at the next
	// primary-key lookup. Bounded well under the other lookups' 10 s: this
	// runs in the loop that also tells the server the stream is alive, and it
	// is the one lookup that is tried again.
	partsReader := &partitionsReader{dsn: c.cfg.QueryDSN}
	defer partsReader.close()
	partitions := func(relationID uint32, schema, table string) (Partitions, error) {
		qctx, cancel := context.WithTimeout(ctx, partitionsReadTimeout)
		defer cancel()
		return partsReader.read(qctx, relationID)
	}
	opts := []DecoderOption{WithAttrResolver(attrResolver), WithSkipCounters(c.cfg.Skips),
		WithDeferrableResolver(deferrable), WithPartitionsResolver(partitions)}
	if c.cfg.captureBelowFull {
		opts = append(opts, withCaptureBelowFull())
	}
	stampsClose := func() {}
	defer func() { stampsClose() }()
	if c.cfg.LastChange != nil && c.cfg.LoadKnownShapes != nil {
		opts = append(opts, inferenceOrWarn(func() (DDLInference, error) {
			start, err := publicationOIDs(startupCtx, queryConn, c.cfg.Publication)
			if err != nil {
				return DDLInference{}, err
			}
			// PostgreSQL resends from the slot's own confirmed position, which
			// can be before what the index already holds; the shapes loaded
			// must cover a comparison from there.
			var fromText string
			if err := queryConn.QueryRow(startupCtx, `SELECT COALESCE(confirmed_flush_lsn, '0/0'::pg_lsn)::text FROM pg_replication_slots WHERE slot_name = $1`, c.cfg.SlotName).Scan(&fromText); err != nil {
				return DDLInference{}, fmt.Errorf("pgcapture: read confirmed position of slot %q: %w", c.cfg.SlotName, err)
			}
			from, err := pglogrepl.ParseLSN(fromText)
			if err != nil {
				return DDLInference{}, fmt.Errorf("pgcapture: parse confirmed position %q of slot %q: %w", fromText, c.cfg.SlotName, err)
			}
			known, err := c.cfg.LoadKnownShapes(uint64(from))
			if err != nil {
				return DDLInference{}, err
			}
			stamps, closeStamps := stampsReader(startupCtx, ctx, c.cfg.QueryDSN, c.cfg.Publication, c.logger)
			stampsClose = closeStamps
			// When the publication's tables were read, by the server's clock.
			var startAt time.Time
			if err := queryConn.QueryRow(startupCtx, "SELECT now()").Scan(&startAt); err != nil {
				return DDLInference{}, fmt.Errorf("pgcapture: read the server's time: %w", err)
			}
			return DDLInference{
				Known:     known.Shapes,
				Releases:  known.Releases,
				StartOIDs: start,
				StartAt:   startAt.UTC(),
				Lookup: func(oid uint32) (RelationFacts, error) {
					qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
					defer cancel()
					return queryRelationFacts(qctx, queryConn, oid)
				},
				LastChange: c.cfg.LastChange,
				Stamps:     stamps,
			}, nil
		}, c.logger)...)
	}
	decoder := NewDecoder(resolvePK, c.cfg.Filters, c.logger, opts...)

	interval := c.cfg.StandbyInterval
	if interval <= 0 {
		interval = c.deriveStandbyInterval(startupCtx, queryConn)
	}
	standbyTicker := time.NewTicker(interval)
	defer standbyTicker.Stop()

	err = c.receiveLoop(ctx, replConn, decoder, out, interval, standbyTicker)
	if ctx.Err() != nil {
		return nil // graceful shutdown (ctx-cancel surfaced as a receive/emit error)
	}
	return err
}

// warnCaptureCoverage logs the non-fatal coverage gaps (#555 UNLOGGED tables, #556 FK
// cascade children whose parent is published but the child is not). A probe FAILURE is
// logged at debug and never blocks the stream — these are advisory, and the same
// catalog state is also reported by `bintrail-pg doctor`.
func (c *Capturer) warnCaptureCoverage(ctx context.Context, conn *pgx.Conn) {
	// A probe FAILURE is itself surfaced as a WARN, not swallowed at Debug: at the
	// default --log-level=info a Debug line is invisible, so downgrading the failure
	// would silently disable the very guard this function exists to provide — the exact
	// silent-failure class #555/#556 close. Visibility matches the doctor side, which
	// also maps a probe failure to a WARN.
	// #3108: a publication without TRUNCATE. Said once per start; the doctor
	// and each health probe say it again, so a publication changed while
	// capture runs is seen there.
	if exists, publishes, err := QueryPublishesTruncate(ctx, conn, c.cfg.Publication); err != nil {
		c.logger.Warn("pgcapture: could not read whether the publication publishes TRUNCATE; run `bintrail-pg doctor` to re-check", "error", err)
	} else if exists && !publishes {
		c.logger.Warn("pgcapture: "+TruncateNotPublishedProblem(c.cfg.Publication)+". Inserts, updates and deletes are captured normally. To record TRUNCATE, run the statement in fix on the source as the publication's owner",
			"fix", TruncateFixStatement(c.cfg.Publication))
	}

	if unlogged, err := listUnloggedCaptureTables(ctx, conn, c.cfg.Publication, c.cfg.Filters); err != nil {
		c.logger.Warn("pgcapture: UNLOGGED-table coverage check failed to run — could NOT determine whether a captured table writes no WAL (a silent recovery hole); run `bintrail-pg doctor` to re-check", "error", err)
	} else if len(unlogged) > 0 {
		c.logger.Warn("pgcapture: UNLOGGED table(s) in capture scope produce no WAL — their changes are NOT captured and cannot be recovered (ALTER TABLE <t> SET LOGGED if you need them)",
			"tables", strings.Join(unlogged, ", "))
	}

	if children, err := listUncoveredCascadeChildren(ctx, conn, c.cfg.Publication); err != nil {
		c.logger.Warn("pgcapture: FK cascade-child coverage check failed to run — could NOT determine whether a cascade child is unpublished (cascade deletes would be silently lost); run `bintrail-pg doctor` to re-check", "error", err)
	} else {
		for _, ch := range children {
			c.logger.Warn("pgcapture: FK cascade child is NOT in the publication — cascade deletes from its parent are not captured and cannot be recovered (add it to the publication)",
				"child", ch.Child, "parent", ch.Parent, "action", ch.Action)
		}
	}
}

// receiveLoop is the single-goroutine replication loop. pgconn is NOT concurrency-
// safe, so everything — receiving, decoding, emitting, and sending standby updates —
// happens here in sequence; a second goroutine touching the connection is barred.
func (c *Capturer) receiveLoop(ctx context.Context, replConn *pgconn.PgConn, decoder *Decoder, out chan<- event.Event, interval time.Duration, standbyTicker *time.Ticker) error {
	var sent sentMarker
	for {
		// Deadline so a quiet server still triggers a periodic standby update.
		recvCtx, cancel := context.WithDeadline(ctx, time.Now().Add(interval))
		msg, err := replConn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err() // shutdown — Run translates to nil
			}
			if pgconn.Timeout(err) {
				if err := c.sendStandby(replConn); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("pgcapture: receive message: %w", err)
		}
		// When this message was read off the connection: event.Event.ReadAt
		// of the changes it carries, from which the consumer measures how
		// long a change waited to be in the index (#2762).
		readAt := time.Now()

		cd, ok := msg.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		// A message with nothing in it has no kind to read: an error the
		// supervisor reconnects on, where reading its first byte stopped
		// the whole process.
		kind, err := copyDataKind(cd.Data)
		if err != nil {
			return err
		}
		switch kind {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("pgcapture: parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				if err := c.sendStandby(replConn); err != nil {
					return err
				}
			}
			if ev, ok := sent.mark(uint64(pkm.ServerWALEnd)); ok {
				if err := c.emit(ctx, replConn, out, ev, standbyTicker); err != nil {
					return err
				}
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("pgcapture: parse XLogData: %w", err)
			}
			logmsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				// A malformed logical message is stream desync, not noise — fail loud.
				return fmt.Errorf("pgcapture: parse logical message at %s: %w", xld.WALStart, err)
			}
			ev, emit, err := decoder.Decode(logmsg)
			if err != nil {
				return err
			}
			if emit {
				ev.ReadAt = readAt
				if err := c.emit(ctx, replConn, out, ev, standbyTicker); err != nil {
					return err
				}
			}
			// Drain any events the message produced beyond its single return value.
			// Only a multi-relation TRUNCATE (`TRUNCATE a, b`) fills this — one marker
			// per in-scope relation past the first — so none is silently dropped (#827).
			for _, pev := range decoder.TakePending() {
				pev.ReadAt = readAt
				if err := c.emit(ctx, replConn, out, pev, standbyTicker); err != nil {
					return err
				}
			}
		}
	}
}

// sentMarker turns keepalives into EventSentThrough markers (#2746).
//
// A logical walsender's keepalive carries sentPtr: the end of the last WAL
// record it decoded (XLogSendLogical sets it after LogicalDecodingProcessRecord;
// PostgreSQL 15+ may send instead the end of the commit record of an empty
// transaction it is skipping, which can be past sentPtr; that is safe too,
// since the skipped transaction sends nothing and everything before it was
// sent already). pgoutput writes a transaction's messages while
// decoding its commit record, so the messages of every transaction that
// committed below that position were put on the connection before the
// keepalive, and this loop has emitted them before it reads the keepalive. A
// keepalive can arrive between two messages of one transaction (the walsender
// sends one while it waits on a full socket); sentPtr then still ends before
// that transaction's commit record, so its commit LSN is at or past the
// position and the marker does not claim it. That holds because the stream
// runs pgoutput proto_version 1, which never streams a transaction before its
// commit.
//
// Only a position past the last one marked is emitted: on a quiet database the
// walsender sends a keepalive at every wakeup, mostly with the same position.
type sentMarker struct{ last uint64 }

func (s *sentMarker) mark(walEnd uint64) (event.Event, bool) {
	if walEnd <= s.last {
		return event.Event{}, false
	}
	s.last = walEnd
	return event.Event{EventType: event.EventSentThrough, EndPos: walEnd}, true
}

// emit delivers one event on out. It must not let a slow consumer starve the
// keepalive: while blocked it keeps sending standby updates on the ticker (which
// satisfies wal_sender_timeout — the server resets its liveness timer on ANY client
// message, no WAL consumption required) and stays responsive to ctx-cancel. It is
// the keepalive-in-the-same-select pattern; a second goroutine is barred (pgconn is
// not concurrency-safe). While parked here we are intentionally NOT draining the
// server→client stream; sendStandby's bounded write keeps that backpressure from
// wedging the loop.
func (c *Capturer) emit(ctx context.Context, replConn *pgconn.PgConn, out chan<- event.Event, ev event.Event, standbyTicker *time.Ticker) error {
	for {
		select {
		case out <- ev:
			return nil
		case <-standbyTicker.C:
			if err := c.sendStandby(replConn); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// sendStandby reports the consumer's DURABLE position (lastAcked) as the WAL
// write/flush position — never the received position — so confirmed_flush_lsn can
// never advance past durably-indexed WAL. The write is bounded by a deadline:
// SendStandbyStatusUpdate ignores its context and does a deadline-less net write,
// which under TCP backpressure (we stop draining while parked in emit) could
// otherwise block forever and deadlock the loop and ctx-cancel.
func (c *Capturer) sendStandby(replConn *pgconn.PgConn) error {
	lsn := pglogrepl.LSN(c.lastAcked.Load())
	if nc := replConn.Conn(); nc != nil {
		_ = nc.SetWriteDeadline(time.Now().Add(standbyWriteTimeout))
		defer func() { _ = nc.SetWriteDeadline(time.Time{}) }()
	}
	if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn}); err != nil {
		return fmt.Errorf("pgcapture: send standby status (flushed=%s): %w", lsn, err)
	}
	return nil
}

// deriveStandbyInterval reads the server's wal_sender_timeout (milliseconds; 0 =
// disabled) and returns a third of it — so a quiet stream refreshes the server's
// liveness timer ~3x per timeout window, comfortably under the drop threshold —
// floored at 1s. (The walsender's own keepalive cadence is timeout/2; a third is
// the deliberately more conservative client side.) It falls back to
// defaultStandbyInterval on any error or when the timeout is disabled. This avoids
// baking in the 60s default — an operator who set a shorter wal_sender_timeout would
// otherwise see the connection dropped.
func (c *Capturer) deriveStandbyInterval(ctx context.Context, queryConn *pgx.Conn) time.Duration {
	var ms int
	err := queryConn.QueryRow(ctx, `SELECT setting::int FROM pg_settings WHERE name = 'wal_sender_timeout'`).Scan(&ms)
	if err != nil || ms <= 0 {
		return defaultStandbyInterval
	}
	interval := time.Duration(ms) * time.Millisecond / 3
	if interval < time.Second {
		interval = time.Second
	}
	return interval
}

// closeReplConn / closeQueryConn close on a fresh background context: Run's ctx is
// already cancelled exactly when a graceful shutdown runs these defers, and
// pgconn/pgx Close take a context for the clean terminate message — passing the
// cancelled ctx would skip it (an abrupt drop).
func closeReplConn(conn *pgconn.PgConn) {
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Close(cctx)
}

func closeQueryConn(conn *pgx.Conn) {
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Close(cctx)
}

// inferenceOrWarn prepares DDL inference (#2441), or, when what it needs
// cannot be read at start, leaves it off for this run with a loud warning:
// the schema-changes timeline must never take row capture down with it.
// The next start tries again.
func inferenceOrWarn(prepare func() (DDLInference, error), logger *slog.Logger) []DecoderOption {
	cfg, err := prepare()
	if err != nil {
		logger.Error("pgcapture: schema changes will NOT be detected until the next restart (rows are captured normally): could not read what DDL detection needs at start",
			"error", err)
		return nil
	}
	return []DecoderOption{WithDDLInference(cfg)}
}

// copyDataKind is the kind of a CopyData message, its first byte. A message
// with nothing in it has none.
func copyDataKind(data []byte) (byte, error) {
	if len(data) == 0 {
		return 0, errors.New("pgcapture: the source sent an empty CopyData message")
	}
	return data[0], nil
}
