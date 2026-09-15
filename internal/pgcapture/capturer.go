package pgcapture

import (
	"context"
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
}

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
	if err := validateReplicaIdentity(startupCtx, queryConn, c.cfg.Publication); err != nil {
		return err
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

	if err := pglogrepl.StartReplication(ctx, replConn, c.cfg.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		Mode:       pglogrepl.LogicalReplication,
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", c.cfg.Publication)},
	}); err != nil {
		return fmt.Errorf("pgcapture: start replication slot %q from %s: %w", c.cfg.SlotName, startLSN, err)
	}
	// Seed the durable cursor to the resume point: everything up to startLSN is
	// already durably indexed (first run = ConsistentPoint, nothing precedes it).
	c.lastAcked.Store(uint64(startLSN))
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
	decoder := NewDecoder(resolvePK, c.cfg.Filters, c.logger, WithAttrResolver(attrResolver))

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

		cd, ok := msg.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		switch cd.Data[0] {
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
				if err := c.emit(ctx, replConn, out, ev, standbyTicker); err != nil {
					return err
				}
			}
			// Drain any events the message produced beyond its single return value.
			// Only a multi-relation TRUNCATE (`TRUNCATE a, b`) fills this — one marker
			// per in-scope relation past the first — so none is silently dropped (#827).
			for _, pev := range decoder.TakePending() {
				if err := c.emit(ctx, replConn, out, pev, standbyTicker); err != nil {
					return err
				}
			}
		}
	}
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
