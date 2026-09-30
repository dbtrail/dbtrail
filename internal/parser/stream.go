// Package parser — this file provides StreamParser, which reads events from a
// BinlogStreamer (network replication) and emits them on the same Event channel
// as Parser (file-based). Both use the shared handleRows function internally.
package parser

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/observe"
)

// StreamParser reads events from a live BinlogStreamer and sends parsed row
// events to an output channel, mirroring the interface of Parser but without
// requiring binlog files on disk.
type StreamParser struct {
	resolver      atomic.Pointer[metadata.Resolver]
	filters       Filters
	logger        *slog.Logger
	schemaVersion atomic.Uint32 // actual snapshot_id from schema_snapshots; updated by SwapResolver
	// onDDL, when set, runs SYNCHRONOUSLY inside Run for a DDL event, BEFORE
	// that event is emitted and before ANY subsequent binlog event is
	// decoded. See SetSyncDDLHook for why this must not move off the parse
	// path, and why the ordering (hook, then emit) is load-bearing for #760.
	onDDL atomic.Pointer[func(Event) error]
	// skips, when set, tallies capture-time discards (#1034): every event Run
	// reads and drops (column-count mismatch, table not in snapshot,
	// statement-format DML, ...) is counted per reason so the stream consumer
	// can persist the counters with its checkpoint and `status` can surface a
	// Capture health verdict. Set via SetSkipCounters BEFORE Run starts (same
	// happens-before contract as SetFlavor); nil = no counting (file parser,
	// tests).
	skips *SkipCounters
	// flavor is the source flavor ("mysql" or "mariadb"; see SetFlavor), used
	// only to word remediation text (GTIDExecutedHint) in the position-
	// wraparound error inside Run. Never consulted for parsing decisions —
	// MySQL vs MariaDB event types (GTIDEvent vs MariadbGTIDEvent) already
	// disambiguate that.
	flavor string
}

// NewStreamParser creates a StreamParser that resolves column names via
// resolver and applies the given filters.
// logger may be nil, in which case slog.Default() is used.
func NewStreamParser(resolver *metadata.Resolver, filters Filters, logger *slog.Logger) *StreamParser {
	if logger == nil {
		logger = slog.Default()
	}
	sp := &StreamParser{filters: filters, logger: logger}
	if resolver != nil {
		sp.schemaVersion.Store(uint32(resolver.SnapshotID()))
		sp.resolver.Store(resolver)
	}
	return sp
}

// SwapResolver atomically replaces the resolver used for column resolution
// and updates schemaVersion to the new resolver's SnapshotID.
// Safe to call concurrently while Run is executing in another goroutine.
func (sp *StreamParser) SwapResolver(r *metadata.Resolver) {
	sp.schemaVersion.Store(uint32(r.SnapshotID()))
	sp.resolver.Store(r)
}

// SetFlavor sets the source flavor ("mysql" or "mariadb", matching
// gomysql.MySQLFlavor/MariaDBFlavor) used to word the position-wraparound
// error (see Run) flavor-appropriately. Must be called before the goroutine
// that runs Run is spawned — Run reads it without synchronization, relying
// on the happens-before edge from the spawning statement (mirrors how
// resolver/filters are fixed before Run begins). Empty (never called)
// defaults to MySQL wording.
func (sp *StreamParser) SetFlavor(flavor string) {
	sp.flavor = flavor
}

// SetSkipCounters wires the capture-skip tally (#1034) Run records discards
// into. Must be called before the goroutine that runs Run is spawned — Run
// reads it without synchronization, relying on the happens-before edge from
// the spawning statement (the SetFlavor contract). nil disables counting.
func (sp *StreamParser) SetSkipCounters(c *SkipCounters) {
	sp.skips = c
}

// GTIDExecutedHint returns the flavor-appropriate system variable an
// operator should read to obtain the source's current executed GTID set, for
// remediation messages that direct the operator to --start-gtid. MySQL has
// gtid_executed; MariaDB has no such variable — its analog is
// gtid_binlog_pos (see detectMariaDBGTIDGap in internal/streamrun, which
// queries the same variable for gap detection).
func GTIDExecutedHint(flavor string) string {
	if flavor == "mariadb" {
		return "SELECT @@gtid_binlog_pos"
	}
	return "SELECT @@GLOBAL.gtid_executed"
}

// SetSyncDDLHook registers fn to run synchronously inside Run, for each DDL
// event, BEFORE that event is emitted on out and BEFORE any subsequent
// binlog event is decoded.
//
// This is the ONLY correct place for the auto-snapshot-on-DDL work (#396):
// the binlog is sequential — `CREATE TABLE t; INSERT INTO t;` puts the row
// events immediately after the DDL — but the parser goroutine runs ahead of
// the consumer through the events channel. A consumer-side handler swaps the
// resolver only after the parser has already decoded (and, for an unknown
// table, silently skipped) the rows that followed the DDL. Blocking the parse
// loop until fn returns closes that window: fn typically takes a fresh schema
// snapshot and calls SwapResolver, so the very next row event decodes with
// the post-DDL schema.
//
// fn returning a non-nil error aborts Run immediately, WITHOUT ever emitting
// the DDL event (#760): if the post-DDL snapshot/resolver refresh failed, the
// resolver is stale and every row event that follows this DDL in the binlog
// would otherwise be silently skipped as "column count mismatch" / "table
// not in snapshot" while the stream checkpoint keeps advancing past them — a
// permanent, unmarked loss. The checkpoint (in this package's consumers)
// advances off events it actually receives, including the DDL event itself;
// withholding that event on hook failure means the checkpoint stays exactly
// where it was BEFORE this DDL. A supervisor restart then resumes from
// there, re-reads the same DDL off the binlog, re-runs this hook (retrying
// the snapshot), and only then decodes the rows that follow — with a fresh
// resolver. Emitting the DDL event first and running fn after would let the
// checkpoint advance past the DDL before a failure is even known, defeating
// the retry.
//
// While fn runs, no new events are produced (the replication connection
// buffers server-side); consumers keep draining already-emitted events.
// Safe to call concurrently with Run.
func (sp *StreamParser) SetSyncDDLHook(fn func(Event) error) {
	if fn == nil {
		sp.onDDL.Store(nil)
		return
	}
	sp.onDDL.Store(&fn)
}

// ErrResentCutTransaction marks a ResentCutTransactionError, for errors.Is.
var ErrResentCutTransaction = errors.New("the source re-sent a transaction that was cut by a disconnect after some of its rows were captured")

// ResentCutTransactionError: the connection to the source dropped in the
// middle of a transaction, after Rows of its row events were already emitted,
// and on reconnect the source sent the whole transaction again. Indexing the
// re-send would put those rows in twice. The stream stops instead; capture
// restarts from its last checkpoint, and that checkpoint's resume cleanup
// deletes the partial copy (its GTID is not in the saved set) before the
// transaction is read again in full.
type ResentCutTransactionError struct {
	GTID string
	Rows int
}

func (e *ResentCutTransactionError) Error() string {
	return fmt.Sprintf("the connection to the source dropped in the middle of transaction %s after %d of its row events were captured, "+
		"and the source sent it again from the start; stopping so capture restarts from its last checkpoint and indexes it once", e.GTID, e.Rows)
}

func (e *ResentCutTransactionError) Is(target error) bool { return target == ErrResentCutTransaction }

// Run reads events from the streamer and sends matching row events to out.
// It tracks the current binlog filename (from RotateEvent) and GTID (from
// GTIDEvent), and uses them to populate each emitted Event.
//
// Returns nil when the context is cancelled (graceful shutdown) or when the
// streamer is closed. Returns a non-nil error on network or decode failure.
func (sp *StreamParser) Run(ctx context.Context, streamer *replication.BinlogStreamer, out chan<- Event) error {
	var currentFile string
	var currentGTID string
	var currentConnectionID uint32 // pseudo_thread_id from most recent QueryEvent
	// currentCommitTsUS is the microsecond commit timestamp of the transaction
	// in flight, from its GTID event. Transaction-scoped like currentGTID; zero
	// when the source writes none (MariaDB, or MySQL < 8.0.1). gtid_mode=OFF
	// still yields a value: 8.0 stamps the ANONYMOUS_GTID_EVENT too.
	var currentCommitTsUS uint64
	// lastLogPos tracks the highest EventHeader.LogPos seen since the last
	// RotateEvent, to detect a same-file position wraparound (#845) live,
	// during streaming — see the check at the top of handleEvent below.
	var lastLogPos uint32
	// fillCorr undoes the post-reconnect position overshoot FillZeroLogPos
	// introduces when a resume lands mid-transaction on a MariaDB 11.4+
	// source (#1117) — see resumeFillCorrector. It runs BEFORE the
	// wraparound guard below so the guard (and everything downstream) only
	// ever sees true file offsets.
	var fillCorr resumeFillCorrector
	// currentQueryText holds the original SQL statement from the most recent
	// ROWS_QUERY_EVENT (MySQL, binlog_rows_query_log_events=ON) or
	// ANNOTATE_ROWS event (MariaDB, binlog_annotate_row_events=ON; the syncer
	// must request it via BINLOG_SEND_ANNOTATE_ROWS_EVENT). Statement-scoped —
	// see the file parser's currentQueryText for the full contract.
	var currentQueryText string

	// Re-send guard. After a transparent reconnect in GTID mode, go-mysql
	// resumes from the set it held BEFORE the last GTID event it read
	// (BinlogSyncer.retrySync hands prevGset to the source), so the source
	// sends that last transaction again. If this parser already saw it commit,
	// indexing it again would duplicate its rows forever.
	//
	//   - lastCommittedGTID: the last transaction known COMPLETE: its XID (or
	//     next-GTID fallback) commit was emitted, or, for a DDL that carries
	//     rows (CREATE TABLE ... SELECT), its XID arrived. A plain DDL never
	//     lands here: from the re-send alone it cannot be told apart from a
	//     CREATE ... SELECT cut before its rows, so a re-sent DDL is processed
	//     again (a second DDL event is cheap; lost rows are not).
	//   - ddlTxnGTID: the GTID of a DDL transaction still open, promoted at its
	//     XID.
	//   - afterRotate: set by an artificial rotate and consumed by the next
	//     transaction start. Servers send one at every (re)connect AND at every
	//     binlog file switch, so it never means "reconnect" by itself: it only
	//     arms two exact-GTID comparisons at the next transaction start.
	//   - skipTxn: drop every event of a re-sent committed transaction until the
	//     next transaction starts.
	var lastCommittedGTID, ddlTxnGTID string
	var afterRotate, skipTxn bool
	// txnRows counts the row events emitted for the transaction in flight.
	var txnRows int

	// em is the stamped way out (#1223 T1). It is REBUILT on every delivered
	// binlog event, just below, so each emitted row carries the time go-mysql
	// handed us the event it came from. The closures below close over the
	// variable, not over a value, so they always send through the current stamp.
	var em emitter

	// emitCommit signals a transaction commit boundary so the stream consumer can
	// advance the durable GTID checkpoint only after the transaction's rows have
	// been received (#491). It commits the in-flight transaction (currentGTID) and
	// clears it, so the next-GTID fallback below won't re-commit the same GTID.
	// No-op for non-GTID sources (currentGTID empty); a GTID-enabled source running
	// in position mode still emits these, harmlessly — the consumer ignores commit
	// events when accGTID is nil, and binlogPos lands on the XID boundary.
	emitCommit := func(hdr *replication.EventHeader) error {
		if currentGTID == "" {
			return nil
		}
		commitEv := Event{
			BinlogFile: currentFile,
			EndPos:     uint64(hdr.LogPos),
			Timestamp:  time.Unix(int64(hdr.Timestamp), 0).UTC(),
			GTID:       currentGTID,
			EventType:  EventCommit,
		}
		if err := em.send(ctx, commitEv); err != nil {
			return err
		}
		lastCommittedGTID = currentGTID
		currentGTID = "" // committed; the next-GTID fallback must not re-commit it
		return nil
	}

	// beginTxn is shared by both flavors' GTID events. After a rotate, the
	// same GTID as the transaction still waiting for its next-GTID fallback
	// commit is that transaction sent again (cut by a disconnect, or with no
	// XID): it was never committed, so it is not committed now either. Any
	// other GTID means the pending one ended, and the fallback commits it as
	// always (this is the file-switch case). Then it decides whether the new
	// transaction is a re-send of one already complete (see the re-send guard
	// above), which sets skipTxn.
	beginTxn := func(hdr *replication.EventHeader, gtid string) error {
		if afterRotate && currentGTID != "" && currentGTID == gtid {
			if txnRows > 0 {
				// Cut after some of its rows were already emitted (and may
				// already be written). The re-send would index them again,
				// and they cannot be taken back from here: stop, so capture
				// restarts from its last checkpoint, whose cleanup deletes
				// the partial copy before the transaction is read again.
				return &ResentCutTransactionError{GTID: gtid, Rows: txnRows}
			}
			currentGTID = ""
		}
		if err := emitCommit(hdr); err != nil {
			return err
		}
		currentGTID = gtid
		skipTxn = afterRotate && gtid != "" && gtid == lastCommittedGTID
		afterRotate = false
		ddlTxnGTID = ""
		txnRows = 0
		if skipTxn {
			sp.logger.Info("dropping a transaction the source re-sent after a reconnect; it was already captured",
				"gtid", gtid, "file", currentFile)
		}
		return nil
	}

	// emitGTIDTracking emits an EventGTID for the in-flight currentGTID so the
	// stream consumer accumulates it even when no rows follow (#124). No-op when
	// currentGTID is empty (non-GTID source). Shared by the MySQL GTIDEvent and
	// MariaDB MariadbGTIDEvent cases so both flavors track identically.
	emitGTIDTracking := func(hdr *replication.EventHeader) error {
		if currentGTID == "" {
			return nil
		}
		gtidEv := Event{
			BinlogFile: currentFile,
			EndPos:     uint64(hdr.LogPos),
			Timestamp:  time.Unix(int64(hdr.Timestamp), 0).UTC(),
			GTID:       currentGTID,
			EventType:  EventGTID,
		}
		return em.send(ctx, gtidEv)
	}

	// handleEvent processes one binlog event. It is recursive: with
	// binlog_transaction_compression=ON the source wraps each transaction's
	// events (BEGIN + TABLE_MAP + rows + XID) in a single zstd-compressed
	// Transaction_payload event — delivered as-is over the replication
	// protocol — which go-mysql hands over pre-decoded in ev.Events.
	// Returns ctx.Err() on cancellation; the Run loop translates that into
	// a graceful nil.
	var handleEvent func(binlogEv *replication.BinlogEvent) error
	handleEvent = func(binlogEv *replication.BinlogEvent) error {
		// Post-reconnect fill correction (#1117) — must run before the
		// wraparound guard so both the guard and the emitted events see true
		// file offsets, not FillZeroLogPos's ghost-FDE overshoot.
		if fillCorr.Observe(binlogEv) {
			sp.logger.Debug("corrected a dynamically-filled binlog position inflated by the post-reconnect FDE overshoot",
				"file", currentFile,
				"event_type", binlogEv.Header.EventType.String(),
				"pos", binlogEv.Header.LogPos)
		}
		// EventHeader.LogPos is a uint32 wire field (4 bytes, the SAME
		// COM_BINLOG_DUMP format limit resolveStartForFlavor guards on resume,
		// internal/streamrun/streamrun.go) — the position immediately after
		// this event within the CURRENT file. A single oversized transaction
		// can delay rotation past 4GiB (max_binlog_size is only checked
		// between transactions); once the true file offset crosses 2^32,
		// MySQL/MariaDB itself wraps end_log_pos on the wire before bintrail
		// ever sees it — there is no uncorrupted 64-bit value anywhere on the
		// wire to recover (RotateEvent.Position is 8 bytes but only
		// meaningful at rotation, and COM_BINLOG_DUMP resume is itself capped
		// at 4 bytes, so even a true running offset tracked internally could
		// never be used to resume position-mode past this point). The
		// unmistakable signature of this happening is the position going
		// BACKWARD within the same file with no RotateEvent in between —
		// legitimate binlog position is strictly increasing within a file.
		// Catching it here, before this event's rows are captured or any
		// checkpoint advances past it, prevents both a corrupt checkpoint and
		// silent re-indexing/duplication under the wrapped offset — the
		// resume-time guard in resolveStartForFlavor is provably unreachable
		// for this bug (every writer of the saved position already derives
		// from this same uint32 field, so it can never itself exceed
		// math.MaxUint32; the wrap happens here, upstream, at the source).
		if _, isRotate := binlogEv.Event.(*replication.RotateEvent); isRotate {
			lastLogPos = 0 // new file (real or fake-resume rotate): positions restart
		} else if _, isFDE := binlogEv.Event.(*replication.FormatDescriptionEvent); isFDE {
			// A FORMAT_DESCRIPTION event neither trips the guard nor advances
			// lastLogPos. At a mid-file (re)connect the server re-sends the
			// file's FDE with LogPos zeroed on the wire (its physical offset is
			// near the file START, not the resume point, so a real value would
			// be wrong either way). Under FillZeroLogPos (#1117, MariaDB 11.4+)
			// go-mysql fills that zero to resumePos+EventSize — a synthetic
			// value that OVERSHOOTS the next transaction's real positions
			// (verified live: resume at 938 → FDE filled to 1190 → next GTID
			// event real 980), which read here as a backward jump and
			// false-tripped the wraparound guard. Skipping the FDE costs no
			// detection power: a genuine 4GiB wrap can never first manifest on
			// an FDE — in-file FDEs sit at offset 4, immediately after a
			// RotateEvent already reset lastLogPos to 0.
		} else if hdr := binlogEv.Header; lastLogPos != 0 && hdr.LogPos != 0 && hdr.LogPos < lastLogPos {
			return &PositionWraparoundError{msg: fmt.Sprintf(
				"binlog position wraparound detected in %q: position went from %d back to %d with no intervening "+
					"file rotation — this file has grown past the 4GiB wire-format limit for a single binlog position "+
					"(typically one oversized transaction delaying rotation), and the source is truncating end_log_pos "+
					"on the wire; position-mode streaming cannot safely continue past this point. Switch to GTID mode, "+
					"which has no positional limit: restart with --start-gtid using the source's current executed "+
					"GTID set (%s)",
				currentFile, lastLogPos, hdr.LogPos, GTIDExecutedHint(sp.flavor))}
		} else if hdr.LogPos > lastLogPos {
			lastLogPos = hdr.LogPos
		}

		// Events of a re-sent committed transaction are dropped until the next
		// transaction starts. Rotations and GTID events still go through: the
		// file name must stay current, and a GTID event ends the skip. A tagged
		// GTID event (MySQL 8.4) starts a transaction too, so it ends the skip
		// even though nothing below handles it.
		if skipTxn {
			switch binlogEv.Event.(type) {
			case *replication.RotateEvent, *replication.GTIDEvent, *replication.MariadbGTIDEvent:
			case *replication.GtidTaggedLogEvent:
				skipTxn = false
			default:
				return nil
			}
		}

		switch ev := binlogEv.Event.(type) {
		case *replication.RotateEvent:
			currentFile = string(ev.NextLogName)
			if binlogEv.Header.Flags&replication.LOG_EVENT_ARTIFICIAL_F != 0 {
				// (Re)connect or file switch. currentGTID is kept: whether the
				// pending transaction ended is decided at the next transaction
				// start (beginTxn). A skip in progress ends: after a reconnect
				// the source starts over.
				skipTxn = false
				afterRotate = true
			}

		case *replication.GTIDEvent:
			// A new transaction is starting, so the previous one has terminated. If
			// it wasn't already committed by its XID or a table DDL, commit it now.
			// This is the catch-all for transactions that carry a GTID but emit no
			// XID and aren't table DDL — two families:
			//   * implicitly-committed DDL/DCL: GRANT/REVOKE, CREATE/DROP DATABASE,
			//     CREATE/DROP VIEW/TRIGGER/PROCEDURE/FUNCTION, CREATE/DROP INDEX,
			//     ANALYZE/OPTIMIZE TABLE;
			//   * explicit terminators logged as a QUERY with no XID: XA COMMIT, or
			//     a COMMIT of a non-transactional/mixed-engine transaction.
			// (A normal InnoDB COMMIT does NOT reach here — it ends in an XID_EVENT.)
			// Without this their GTID would never advance the checkpoint, causing
			// endless re-streaming and eventually a false data-loss gap alarm (#491).
			//
			// Limitation: the fallback fires on the NEXT GTID, so the LAST such
			// statement before an idle period or shutdown stays uncommitted until
			// traffic resumes — it is re-streamed on restart (harmless: these carry
			// no rows). DML is unaffected (it commits immediately at its XID).
			if err := beginTxn(binlogEv.Header, formatGTID(binlogEv.Header.EventType, ev.SID, ev.GNO)); err != nil {
				return err
			}
			if skipTxn {
				currentGTID = ""
				return nil
			}
			currentQueryText = "" // transaction boundary — statement text never crosses it
			// immediate_commit_timestamp (µs since epoch), MySQL 8.0.1+; zero
			// on older servers — the "unknown" value Event.CommitTsUS documents.
			currentCommitTsUS = ev.ImmediateCommitTimestamp
			if err := emitGTIDTracking(binlogEv.Header); err != nil {
				return err
			}

		case *replication.MariadbGTIDEvent:
			// MariaDB analogue of GTIDEvent: a MariaDB source emits a
			// MariadbGTIDEvent (domain-server-seq) instead of a GTIDEvent. The
			// commit-boundary and tracking-emit logic is identical — see the
			// GTIDEvent case above for the #491 next-GTID-fallback rationale.
			// ev.GTID.String() yields "0-1-100" and "" for the zero GTID, so the
			// emit guard inside emitGTIDTracking is built in.
			if err := beginTxn(binlogEv.Header, ev.GTID.String()); err != nil {
				return err
			}
			if skipTxn {
				currentGTID = ""
				return nil
			}
			currentQueryText = ""
			// MariaDB's GTID event has no commit timestamp: reset rather than
			// carry the previous transaction's value forward — a stale
			// microsecond stamp reads as precise and would be worse than none.
			currentCommitTsUS = 0
			if err := emitGTIDTracking(binlogEv.Header); err != nil {
				return err
			}

		case *replication.QueryEvent:
			currentConnectionID = ev.SlaveProxyID
			// New statement scope (BEGIN of the next transaction, or a DDL) —
			// the previous statement's ROWS_QUERY text is stale.
			currentQueryText = ""
			ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
			if ddlEv, ok := parseDDL(sp.logger, currentFile, binlogEv.Header.LogPos, ts, currentGTID, string(ev.Query), string(ev.Schema), sp.schemaVersion.Load()); ok {
				// Synchronous DDL hook runs BEFORE ddlEv is emitted (#760,
				// reordered from emit-then-hook). The consumer (streamLoop)
				// advances the durable checkpoint's binlogPos/GTID off events
				// it receives, including EventDDL itself — so if ddlEv were
				// emitted first and the hook then failed, the checkpoint
				// would already have moved past this DDL by the time Run
				// aborts. A restart would resume AFTER the DDL, never
				// re-read the QUERY_EVENT that carries it, and never re-fire
				// this hook — leaving the resolver stale forever while rows
				// keep getting silently skipped as "column count mismatch".
				// Running the hook first and returning its error WITHOUT
				// ever sending ddlEv means a failure leaves the checkpoint
				// exactly where it was before this DDL; a restart re-reads
				// the DDL from the binlog and retries the snapshot.
				if hook := sp.onDDL.Load(); hook != nil {
					if err := (*hook)(ddlEv); err != nil {
						return fmt.Errorf("sync DDL hook: %w", err)
					}
				}
				if err := em.send(ctx, ddlEv); err != nil {
					return err
				}
				ddlTxnGTID = currentGTID
				// Table DDL auto-commits its own GTID; EventDDL is the commit
				// boundary the consumer acts on, so clear the in-flight GTID to keep
				// the next-GTID fallback from re-committing it. Other QueryEvents
				// (BEGIN, SAVEPOINT, ...) deliberately do NOT commit here — DML
				// commits at its XID below, and other implicitly-committed statements
				// commit via the next-GTID fallback (#491).
				currentGTID = ""
			} else if kw, isDML := statementDML(string(ev.Query)); isDML {
				// STATEMENT/MIXED-format DML (or a session flip off ROW): the row
				// image is not in the binlog, so this change cannot be captured.
				// Fail LOUD + metric + skip ledger, symmetric to the partial-image
				// guard (#493) — but only when the statement's schema is in capture
				// scope; an out-of-scope drop (system schema, or excluded by the
				// configured filters) is not a coverage gap and must not raise the
				// alarm, mark Capture health DEGRADED, or feed the consecutive-skip
				// escalation (#1000: RDS's rdsadmin writes mysql.* heartbeats in
				// STATEMENT format — a constant false alarm otherwise).
				// NOTE: never log the statement text — a DML statement embeds row
				// VALUES; keyword + file/pos + connection_id locate it without
				// leaking data into operator logs.
				if schema := string(ev.Schema); !statementDMLInScope(schema, &sp.filters) {
					sp.logger.Debug("statement-format DML for out-of-scope schema — not captured, not a coverage gap",
						"file", currentFile,
						"pos", binlogEv.Header.LogPos,
						"schema", schema,
						"statement_type", kw,
						"connection_id", ev.SlaveProxyID)
				} else {
					sp.logger.Warn("statement-format DML in binlog — event NOT captured (bintrail requires binlog_format=ROW; a STATEMENT/MIXED format or a session-level override produced this)",
						"file", currentFile,
						"pos", binlogEv.Header.LogPos,
						"statement_type", kw,
						"connection_id", ev.SlaveProxyID)
					observe.StatementDMLDropped()
					sp.skips.RecordSkipAttributed(SkipStatementFormatDML, SkipAttribution{
						File:          currentFile,
						Pos:           uint64(binlogEv.Header.LogPos),
						StatementType: kw,
						ConnectionID:  ev.SlaveProxyID,
					})
				}
			}

		case *replication.XIDEvent:
			// InnoDB transaction commit — the boundary at which it's safe to
			// advance the durable GTID checkpoint (#491).
			currentQueryText = ""
			if currentGTID == "" && ddlTxnGTID != "" {
				// The XID of a DDL transaction that carries rows (CREATE
				// TABLE ... SELECT): now it is complete.
				lastCommittedGTID = ddlTxnGTID
				ddlTxnGTID = ""
			}
			if err := emitCommit(binlogEv.Header); err != nil {
				return err
			}

		case *replication.RowsQueryEvent:
			// The original SQL statement (binlog_rows_query_log_events=ON),
			// emitted right before the statement's TABLE_MAP + rows events.
			// Sanitized ONCE here at the capture boundary — every downstream
			// path (index INSERT, BYOS buffer/payload) sees bounded, valid
			// UTF-8 text.
			currentQueryText = event.SanitizeQueryText(string(ev.Query))

		case *replication.MariadbAnnotateRowsEvent:
			// MariaDB's positional sibling of ROWS_QUERY_EVENT
			// (binlog_annotate_row_events=ON on the source; only sent when the
			// syncer requested BINLOG_SEND_ANNOTATE_ROWS_EVENT).
			currentQueryText = event.SanitizeQueryText(string(ev.Query))

		case *replication.RowsEvent:
			// nil gapTracker: the stream keeps its warn-and-continue skip
			// behavior (the synchronous DDL hook, SetSyncDDLHook, is the
			// stream's post-DDL correctness mechanism, not file-level failure).
			err := handleRows(ctx, sp.logger, sp.resolver.Load(), &sp.filters, binlogEv, ev, currentFile, currentGTID, currentConnectionID, currentCommitTsUS, currentQueryText, sp.schemaVersion.Load(), em, nil, sp.skips)
			// Statement boundary — see the file parser's RowsEvent case: the
			// STMT_END_F clear prevents a ROWS_QUERY-less later statement in
			// the same transaction from inheriting this statement's text.
			if ev.Flags&replication.RowsEventStmtEndFlag != 0 {
				currentQueryText = ""
			}
			return err

		case *replication.TransactionPayloadEvent:
			for _, inner := range ev.Events {
				rewriteInnerHeader(inner.Header, binlogEv.Header)
				if err := handleEvent(inner); err != nil {
					return err
				}
			}

		default:
			// Unnamed event types are dropped. Say so at debug level: this
			// switch's silence is how ROWS_QUERY_EVENT stayed invisible for
			// years — nothing reported that the stream was seeing an event
			// kind with no case. Debug, not info: the common unnamed types
			// (FORMAT_DESCRIPTION, PREVIOUS_GTIDS, STOP, HEARTBEAT…) are
			// uninteresting and frequent, so a louder level would be noise;
			// the point is that the answer EXISTS when someone asks why some
			// source metadata never appears in the index.
			sp.logger.Debug("binlog event type not handled by the stream parser",
				"file", currentFile,
				"pos", binlogEv.Header.LogPos,
				"event_type", binlogEv.Header.EventType.String())
		}
		return nil
	}

	for {
		binlogEv, err := streamer.GetEvent(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled — graceful shutdown
			}
			return WrapReplicationError(err)
		}

		// T1 (#1223): the moment the replication client delivered this event.
		// Everything emitted while handling it carries this stamp — taken HERE
		// and not where the consumer receives off `out`, because `out` is
		// buffered and a receive-side stamp would swallow the queue wait, which
		// is the largest part of the lag precisely when the indexer is behind.
		em = emitter{ch: out, readAt: time.Now(), rows: &txnRows}

		if err := handleEvent(binlogEv); err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled during event processing
			}
			return err
		}
	}
}
