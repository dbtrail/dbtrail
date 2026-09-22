// Package parser reads MySQL ROW-format binlog files and emits typed events.
// It uses go-mysql-org/go-mysql for low-level binlog decoding and the
// metadata.Resolver to map column ordinals to column names.
package parser

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/observe"
)

// ─── Event types ─────────────────────────────────────────────────────────────

// EventType is the source-agnostic event-type, moved to internal/event (#528).
// Aliased here so the capture layer and existing callers keep using parser.EventType.
type EventType = event.EventType

const (
	EventInsert   = event.EventInsert
	EventUpdate   = event.EventUpdate
	EventDelete   = event.EventDelete
	EventDDL      = event.EventDDL
	EventGTID     = event.EventGTID
	EventSnapshot = event.EventSnapshot
	EventCommit   = event.EventCommit
)

// Event is the source-agnostic change event, moved to internal/event (#528).
// Aliased here so the binlog parser and existing callers keep using parser.Event.
type Event = event.Event

// emitter is the parser's only way out to a consumer. It exists so the read
// timestamp (#1223 T1) is stamped STRUCTURALLY: every event leaves through
// send, so a new emit path cannot forget to stamp the way it could if ReadAt
// were just another field each Event literal had to remember to set.
//
// readAt is the zero Time on the file path — see event.Event.ReadAt for why
// consumers must skip, not observe, a zero.
type emitter struct {
	ch     chan<- Event
	readAt time.Time
}

// emitTo builds an unstamped emitter, for the file path and for tests.
func emitTo(ch chan<- Event) emitter { return emitter{ch: ch} }

// send stamps and delivers one event, honouring cancellation exactly as the
// bare channel sends it replaced.
func (e emitter) send(ctx context.Context, ev Event) error {
	ev.ReadAt = e.readAt
	select {
	case e.ch <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ─── Filters ─────────────────────────────────────────────────────────────────

// Filters is the source-agnostic schema/table filter, moved to internal/event
// (#528). Aliased here so the parser and existing callers keep using parser.Filters.
type Filters = event.Filters

// ─── Parser ──────────────────────────────────────────────────────────────────

// Parser reads binlog files and emits Events onto a channel.
type Parser struct {
	binlogDir     string
	resolver      atomic.Pointer[metadata.Resolver]
	filters       Filters
	logger        *slog.Logger
	schemaVersion atomic.Uint32 // actual snapshot_id from schema_snapshots; updated by SwapResolver
	skips         *SkipCounters // optional (#1199): file-mode run tally, see SetSkipCounters
}

// SetSkipCounters wires an optional skip tally into the file-index path
// (#1199). Unlike the stream (where the counters persist to
// stream_state.capture_skips), file-mode counters are run-scoped: the caller
// summarizes them at end of run. They exist because a VALIDATION-EXCLUDED
// table's skips no longer fail the file via the #778 gap tracker, and per-event
// WARNs alone scroll away — without a tally the run would end with no
// aggregate signal. Nil (the default) keeps every RecordSkip a no-op.
func (p *Parser) SetSkipCounters(s *SkipCounters) { p.skips = s }

// New creates a Parser that reads from binlogDir, resolves column names via
// resolver, and applies the given filters.
// logger may be nil, in which case slog.Default() is used.
func New(binlogDir string, resolver *metadata.Resolver, filters Filters, logger *slog.Logger) *Parser {
	if logger == nil {
		logger = slog.Default()
	}
	p := &Parser{
		binlogDir: binlogDir,
		filters:   filters,
		logger:    logger,
	}
	if resolver != nil {
		p.schemaVersion.Store(uint32(resolver.SnapshotID()))
		p.resolver.Store(resolver)
	}
	return p
}

// SwapResolver atomically replaces the resolver used for column resolution
// and updates schemaVersion to the new resolver's SnapshotID.
// Safe to call concurrently while ParseFile is running.
func (p *Parser) SwapResolver(r *metadata.Resolver) {
	p.schemaVersion.Store(uint32(r.SnapshotID()))
	p.resolver.Store(r)
}

// ParseFiles parses multiple binlog files in order, sending events to the channel.
func (p *Parser) ParseFiles(ctx context.Context, filenames []string, events chan<- Event) error {
	for _, name := range filenames {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := p.ParseFile(ctx, name, events); err != nil {
			return fmt.Errorf("error in %s: %w", name, err)
		}
	}
	return nil
}

// ParseFile parses a single binlog file and sends matching events to the channel.
// It stops early if ctx is cancelled.
func (p *Parser) ParseFile(ctx context.Context, filename string, events chan<- Event) error {
	fullPath := filepath.Join(p.binlogDir, filename)

	// currentGTID holds the GTID of the transaction currently being processed.
	// It is updated on every GTID_LOG_EVENT and carried into every subsequent
	// rows event until the next GTID_LOG_EVENT resets it.
	var currentGTID string
	// currentConnectionID holds the MySQL pseudo_thread_id from the most
	// recent QueryEvent. For DML transactions, this is the QUERY(BEGIN)
	// event that precedes the row events.
	var currentConnectionID uint32
	// currentCommitTsUS holds the microsecond commit timestamp of the current
	// transaction, from its GTID event. Transaction-scoped like currentGTID:
	// set by the GTID event, zero when the source writes none (MariaDB, or
	// MySQL < 8.0.1). With gtid_mode=OFF, MySQL 8.0 still emits an
	// ANONYMOUS_GTID_EVENT carrying the stamp — go-mysql decodes it into the
	// same GTIDEvent struct, so the value survives there too.
	var currentCommitTsUS uint64
	// currentQueryText holds the original SQL statement from the most recent
	// ROWS_QUERY_EVENT (MySQL, binlog_rows_query_log_events=ON) or
	// ANNOTATE_ROWS event (MariaDB, binlog_annotate_row_events=ON). It is
	// statement-scoped: each statement's event overwrites the previous one,
	// and one statement's text covers ALL of its (possibly chained) rows
	// events. Cleared in three places, each load-bearing: QUERY and GTID
	// boundaries (across transactions), and the STMT_END_F rows event (the
	// statement boundary WITHIN a transaction — the only clear that stops a
	// later ROWS_QUERY-less statement in the same transaction from inheriting
	// stale text when the variable is toggled off mid-transaction).
	var currentQueryText string

	// gaps records rows skipped because the snapshot is stale (a table absent
	// from it, or a diverging column count) for events at-or-after the snapshot
	// time. The file-mode DDL resolver swap runs consumer-side — one buffered
	// channel behind the parser — so rows following an in-file CREATE/ALTER can
	// decode against a stale resolver and be skipped before the swap lands. A
	// non-empty tracker turns into a hard error below so the file is marked
	// 'failed' (and re-indexed after a fresh snapshot) instead of silently
	// 'completed' with an undetected gap (#778).
	var gaps schemaGapTracker

	// The file path never stamps a read time: re-indexing a binlog from disk
	// has no availability lag to measure (#1224).
	em := emitTo(events)

	bp := replication.NewBinlogParser()
	// Pin TIMESTAMP-column string rendering to UTC. go-mysql's default (nil
	// location) formats fracTime.String() using the raw time.Unix(...) value,
	// whose Location is the process's time.Local — leaking host-local wall
	// clock into stored data even though DATETIME (decoded separately, always
	// time.UTC) and the rest of the system (verify, shim, reconstruct) assume
	// UTC at rest (#757).
	bp.SetTimestampStringLocation(time.UTC)

	// lastEnd tracks the end offset of the previous event, to reconstruct the
	// positions MariaDB 11.4+ leaves out of the file itself (#1117): events
	// written through the transaction or statement cache (TABLE_MAP, row
	// events, ANNOTATE) are copied into the binlog with end_log_pos=0 — only
	// directly-written events (GTID, XID) carry a real value. Binlog files are
	// contiguous, so a zero-LogPos event's true end is exactly the previous
	// event's end plus its own EventSize — the same computation go-mysql's
	// FillZeroLogPos performs for the replication stream. Without this, every
	// such row is stored with start_pos = 2^64-EventSize (underflow) and
	// end_pos = 0. Parsing always begins at the file start (offset 4, after
	// the magic), so the running offset is exact from the first event.
	lastEnd := uint32(4)

	// handleEvent processes one binlog event. It is recursive: with
	// binlog_transaction_compression=ON the source wraps each transaction's
	// events (BEGIN + TABLE_MAP + rows + XID) in a single zstd-compressed
	// Transaction_payload event, which go-mysql delivers pre-decoded in
	// ev.Events — recursing dispatches them through the same cases.
	var handleEvent func(binlogEv *replication.BinlogEvent) error
	handleEvent = func(binlogEv *replication.BinlogEvent) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Zero-LogPos fill (see lastEnd above). Transaction_payload inner
		// events never take this branch: rewriteInnerHeader has already
		// stamped them with the outer event's (non-zero) coordinates by the
		// time they recurse through here.
		if hdr := binlogEv.Header; hdr.LogPos == 0 && hdr.Flags&replication.LOG_EVENT_ARTIFICIAL_F == 0 {
			hdr.LogPos = lastEnd + hdr.EventSize
		}
		if binlogEv.Header.LogPos > 0 {
			lastEnd = binlogEv.Header.LogPos
		}

		switch ev := binlogEv.Event.(type) {
		case *replication.GTIDEvent:
			currentGTID = formatGTID(binlogEv.Header.EventType, ev.SID, ev.GNO)
			currentQueryText = "" // transaction boundary — statement text never crosses it
			// immediate_commit_timestamp: microseconds since epoch, written by
			// MySQL 8.0.1+. Zero on older servers, which is exactly the
			// "unknown" value Event.CommitTsUS documents.
			currentCommitTsUS = ev.ImmediateCommitTimestamp

		case *replication.MariadbGTIDEvent:
			// MariaDB source: the GTID arrives as domain-server-seq (e.g.
			// "0-1-100"). ev.GTID.String() returns "" for the zero GTID,
			// mirroring formatGTID's not-enabled behavior.
			currentGTID = ev.GTID.String()
			currentQueryText = ""
			// MariaDB's GTID event carries no commit timestamp. Reset rather
			// than leave the previous transaction's value in place: a stale
			// microsecond stamp on the wrong transaction is worse than none,
			// because it reads as precise.
			currentCommitTsUS = 0

		case *replication.QueryEvent:
			currentConnectionID = ev.SlaveProxyID
			// A new QUERY (BEGIN of the next transaction, or a DDL) opens a new
			// statement scope; any ROWS_QUERY text from the previous statement
			// is stale. The statement's own ROWS_QUERY_EVENT arrives AFTER this.
			currentQueryText = ""
			ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
			if ddlEv, ok := parseDDL(p.logger, filename, binlogEv.Header.LogPos, ts, currentGTID, string(ev.Query), string(ev.Schema), p.schemaVersion.Load()); ok {
				if err := em.send(ctx, ddlEv); err != nil {
					return err
				}
			} else if kw, isDML := statementDML(string(ev.Query)); isDML {
				// STATEMENT/MIXED-format DML (or a session flip off ROW): the row
				// image is not in the binlog, so this change cannot be captured.
				// Fail LOUD + metric, symmetric to the partial-image guard (#493) —
				// but only when the statement's schema is in capture scope; an
				// out-of-scope drop (system schema, or excluded by the configured
				// filters) is not a coverage gap and must not raise the alarm
				// (#1000: RDS's rdsadmin writes mysql.* in STATEMENT format).
				// NOTE: never log the statement text — a DML statement embeds row
				// VALUES; keyword + file/pos + connection_id locate it without
				// leaking data into operator logs.
				if schema := string(ev.Schema); !statementDMLInScope(schema, &p.filters) {
					p.logger.Debug("statement-format DML for out-of-scope schema — not captured, not a coverage gap",
						"file", filename,
						"pos", binlogEv.Header.LogPos,
						"schema", schema,
						"statement_type", kw,
						"connection_id", ev.SlaveProxyID)
				} else {
					p.logger.Warn("statement-format DML in binlog — event NOT captured (bintrail requires binlog_format=ROW; a STATEMENT/MIXED format or a session-level override produced this)",
						"file", filename,
						"pos", binlogEv.Header.LogPos,
						"statement_type", kw,
						"connection_id", ev.SlaveProxyID)
					observe.StatementDMLDropped()
				}
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
			// (binlog_annotate_row_events=ON).
			currentQueryText = event.SanitizeQueryText(string(ev.Query))

		case *replication.RowsEvent:
			err := handleRows(ctx, p.logger, p.resolver.Load(), &p.filters, binlogEv, ev, filename, currentGTID, currentConnectionID, currentCommitTsUS, currentQueryText, p.schemaVersion.Load(), em, &gaps, p.skips)
			// The LAST rows event of a statement carries STMT_END_F — the
			// actual statement boundary. Clearing here keeps one statement's
			// text alive across its chained/split rows events, while a later
			// statement in the SAME transaction that logged no ROWS_QUERY
			// (variable toggled off mid-transaction — MySQL allows it; there
			// is no GTID/QUERY boundary in between) can never inherit it.
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
			// Every event type this switch does not name is dropped — silently,
			// until now. ROWS_QUERY_EVENT carried the originating SQL for years
			// and was invisible for exactly this reason: nothing said the
			// parser was seeing an event kind it had no case for. Debug level,
			// because the common case is genuinely uninteresting (FORMAT_DESC,
			// STOP, PREVIOUS_GTIDS…) and a busy binlog would flood any louder
			// level; the point is that the information EXISTS when someone
			// asks why a source's metadata is not showing up.
			p.logger.Debug("binlog event type not handled by the parser",
				"file", filename,
				"pos", binlogEv.Header.LogPos,
				"event_type", binlogEv.Header.EventType.String())
		}
		return nil
	}

	if err := bp.ParseFile(fullPath, 0, handleEvent); err != nil {
		return err
	}
	return gaps.err(filename)
}

// rewriteInnerHeader stamps a Transaction_payload inner event's header with
// the outer payload event's file coordinates. Inner events were never written
// to the binlog individually: MySQL zeroes their end_log_pos (LogPos=0 —
// verified on 8.0.46 and in go-mysql's 8.0.27 test fixture) while EventSize
// is the genuine event length, so deriving start_pos as LogPos-EventSize
// would underflow uint64. The physical location of every inner event IS the
// payload event that carries it.
func rewriteInnerHeader(inner, outer *replication.EventHeader) {
	inner.LogPos = outer.LogPos
	inner.EventSize = outer.EventSize
}

// ─── Row event handler ────────────────────────────────────────────────────────

// firstPartialImage reports whether any row image in a RowsEvent omitted
// columns — the signature of a non-FULL binlog_row_image (MINIMAL/NOBLOB). It
// returns the absent column indices of the first partial image found, or nil
// when every image is complete. The returned indices are go-mysql's 0-based
// column ordinals (NOT bintrail's 1-based metadata.ColumnMeta.OrdinalPosition);
// the guard surfaces them verbatim in its error for diagnosis.
//
// go-mysql populates RowsEvent.SkippedColumns in lock-step with RowsEvent.Rows:
// one entry per decoded image (for UPDATE there are two images per logical row —
// before and after), each listing the column ordinals whose present-bit was
// clear in the event's column bitmap. Under binlog_row_image=FULL every bit is
// set, so each entry is an empty (non-nil) slice; under MINIMAL/NOBLOB the
// omitted columns appear here while go-mysql pads them to nil in Rows.
// Confirmed empirically against go-mysql v1.13.0, including that a FULL image of
// a table with a VIRTUAL generated column yields no skips (no false positive).
func firstPartialImage(skipped [][]int) []int {
	for _, image := range skipped {
		if len(image) > 0 {
			return image
		}
	}
	return nil
}

// err is the file's fail-loud verdict once parsing ends: nil when nothing was
// skipped, else a *SchemaGapError so the file is marked failed and usage
// telemetry sees the same schema_mismatch class as the hard drift error —
// both mean "the snapshot is stale".
func (g *schemaGapTracker) err(filename string) error {
	if g.count == 0 {
		return nil
	}
	return &SchemaGapError{msg: fmt.Sprintf(
		"schema gap: %d row event(s) in %s were skipped because the schema snapshot is stale "+
			"(first: %s) — these rows were NOT indexed. Run `bintrail snapshot` against the current "+
			"schema and re-index this file (a failed file re-indexes from the start). This commonly "+
			"follows a CREATE/ALTER TABLE within the file whose auto-snapshot landed too late for the "+
			"buffered rows",
		g.count, filename, g.first)}
}

// SchemaGapError is the file-mode sibling of SchemaDriftError (#778): rows
// were skipped because their table was absent from a stale snapshot, so the
// file is failed rather than indexed with holes. Same root cause, same
// usage-telemetry class (schema_mismatch); the message is for the operator
// and never leaves the process.
type SchemaGapError struct{ msg string }

func (e *SchemaGapError) Error() string { return e.msg }

// TelemetryClass implements telemetry.Classed.
func (e *SchemaGapError) TelemetryClass() string { return "schema_mismatch" }

// schemaGapTracker records recoverable schema gaps: rows skipped because their
// table is absent from the snapshot or its column count diverged, for an event
// AT-OR-AFTER the snapshot time (a STALE snapshot, not a historical pre-snapshot
// state). It is populated ONLY on the file-index path (Parser.ParseFile); the
// stream path passes a nil tracker and keeps its warn-and-continue behavior,
// which is backed by the synchronous DDL hook (SetSyncDDLHook). ParseFile turns
// a non-empty tracker into a hard file-level error so the file is marked
// 'failed' and re-indexed (after a fresh snapshot) rather than silently marked
// 'completed' with an undetected gap — the file-mode DDL resolver swap runs
// consumer-side, one buffered channel behind the parser, so post-DDL rows can
// decode with a stale resolver and be skipped before the swap lands (#778).
type schemaGapTracker struct {
	count int
	first string // human detail of the first gap, for the file-level error
}

// record notes one skipped-row schema gap and returns true if it was the first
// (so callers escalate to a log line exactly once, not per skipped row). Nil-safe.
func (t *schemaGapTracker) record(detail string) bool {
	if t == nil {
		return false
	}
	t.count++
	if t.count == 1 {
		t.first = detail
		return true
	}
	return false
}

// eventPredatesSnapshot reports whether binlogEv safely predates the resolver's
// snapshot — the lenient/historical case (mirrors the #700 name-drift
// asymmetry). When true, a schema mismatch is a routine historical state
// (re-indexing old binlogs, or a stream backlog written before a re-snapshot)
// with no converging remediation, so it must NOT escalate to a file-level
// failure. Unknown times (a zero event timestamp or a zero snapshot time) are
// treated as NOT predating (strict), so an unknown age never takes the lenient
// path.
func eventPredatesSnapshot(binlogEv *replication.BinlogEvent, resolver *metadata.Resolver) bool {
	snapTime := resolver.SnapshotTime()
	if binlogEv.Header.Timestamp == 0 || snapTime.IsZero() {
		return false
	}
	eventTime := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
	return eventTime.Before(snapTime)
}

// snapshotExcludedSchemas are the system schemas metadata.TakeSnapshot always
// omits (WHERE TABLE_SCHEMA NOT IN (...)). No resolver ever contains their
// tables, so a row event on one of them (e.g. RDS binlogging periodic
// mysql.rds_heartbeat2 UPDATEs with ~now timestamps) is a routine, permanent
// skip with NO converging remediation — re-snapshotting still excludes them.
// It must NOT escalate a file to 'failed' via the gap tracker (#778 regression),
// only ever warn-and-skip. statementDMLInScope leans on the same fact: a
// statement-format DML whose default DB is one of these schemas cannot be a
// capture gap (#1000). Keep this list byte-identical to the TakeSnapshot
// NOT IN clause.
func isSnapshotExcludedSchema(schema string) bool {
	switch strings.ToLower(schema) {
	case "information_schema", "performance_schema", "mysql", "sys":
		return true
	default:
		return false
	}
}

// handleRows processes a RowsEvent, resolving column names and dispatching to
// the appropriate emit function. It is shared by Parser.ParseFile and StreamParser.Run.
//
// gapTracker is non-nil only on the file-index path: when a row event is skipped
// because its table is absent from the snapshot or the column count diverged AND
// the event is at-or-after the snapshot time (a stale snapshot), the skip is
// recorded so ParseFile can fail the whole file rather than complete it with an
// undetected gap (#778). The stream path passes nil.
//
// skips (#1034): on the STREAM path the counters persist to
// stream_state.capture_skips so `status` can render the discards — every
// warn-and-skip return below records a per-reason counter, and every event
// that clears the guards records a capture (breaking the consecutive-skip run
// that escalates to one ERROR). The FILE path passes the Parser's optional
// run-scoped counters (#1199, see SetSkipCounters): stale-snapshot skips still
// fail the whole file via gapTracker, but validation-excluded tables are
// carved out of that failure and the tally is their aggregate signal.
func handleRows(
	ctx context.Context,
	logger *slog.Logger,
	resolver *metadata.Resolver,
	filters *Filters,
	binlogEv *replication.BinlogEvent,
	rowsEv *replication.RowsEvent,
	filename, currentGTID string,
	connectionID uint32,
	commitTsUS uint64,
	queryText string,
	schemaVersion uint32,
	out emitter,
	gapTracker *schemaGapTracker,
	skips *SkipCounters,
) error {
	schema := string(rowsEv.Table.Schema)
	table := string(rowsEv.Table.Table)

	if !filters.Matches(schema, table) {
		return nil
	}

	if resolver == nil {
		logger.Warn("no resolver available — skipping event",
			"file", filename, "pos", binlogEv.Header.LogPos,
			"schema", schema, "table", table)
		skips.RecordSkip(SkipNoResolver)
		return nil
	}

	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		// Table not in snapshot — warn and skip all rows for this event.
		// Resolve's error text carries the diagnosis: for a table the
		// degraded snapshot EXCLUDED by validation (#1051) it names the
		// exclusion reason and the real fix (give the table a PK / InnoDB),
		// not the non-converging "re-run `bintrail snapshot`" (#1199).
		logger.Warn("table not in snapshot — skipping",
			"file", filename,
			"pos", binlogEv.Header.LogPos,
			"error", err)
		// File-index path only: an at-or-after-snapshot skip is a stale
		// snapshot (e.g. a table created by a DDL earlier in this same file
		// whose consumer-side resolver swap landed too late for these buffered
		// rows). Record it so ParseFile fails the file instead of completing it
		// with an undetected gap (#778). Pre-snapshot events stay a warn-only
		// historical skip. A VALIDATION-EXCLUDED table (#1051) is carved out
		// like the system schemas: it is absent from the snapshot on purpose
		// and re-snapshotting excludes it again, so failing the file would pin
		// it behind a remediation that cannot converge (#1199) — its skips
		// stay visible via the per-event WARN above and the skip tally below
		// (persisted on the stream; run-scoped + end-of-run summary in file
		// mode, which has no capture ledger by design — an empty stream_state
		// is file mode's "no capture ran" marker).
		exclusionReason, excludedByValidation := resolver.ExclusionReason(schema, table)
		if gapTracker != nil && !excludedByValidation && !isSnapshotExcludedSchema(schema) && !eventPredatesSnapshot(binlogEv, resolver) {
			if gapTracker.record(fmt.Sprintf("%s.%s not in snapshot %d at %s:%d", schema, table, schemaVersion, filename, binlogEv.Header.LogPos)) {
				logger.Error("schema gap: skipping rows for a table absent from the snapshot at-or-after snapshot time — the snapshot is stale; this file will be marked failed (run `bintrail snapshot`, then re-index)",
					"file", filename, "pos", binlogEv.Header.LogPos, "schema", schema, "table", table)
			}
		}
		// Stream path (#1034): count the discard so `status` can show it —
		// except for snapshot-excluded system schemas (e.g. mysql.rds_heartbeat2),
		// a routine permanent skip that must never mark capture degraded.
		//
		// The two causes go under DIFFERENT reasons (#1296): a table missing
		// because the snapshot never saw it is fixed by a fresh snapshot, while
		// a VALIDATION-EXCLUDED table is re-excluded by every future snapshot,
		// so one shared reason would make `status` hand half of these operators
		// a remediation that can never converge. The table name rides along so
		// the verdict can say WHICH table stopped being captured instead of
		// leaving that answer only in this log line.
		if !isSnapshotExcludedSchema(schema) {
			reason := SkipTableNotInSnapshot
			if excludedByValidation {
				reason = SkipTableExcludedFromSnapshot
			}
			skips.RecordSkipAttributed(reason, SkipAttribution{
				File:   filename,
				Pos:    uint64(binlogEv.Header.LogPos),
				Schema: schema,
				Table:  table,
				Detail: exclusionReason,
			})
		}
		return nil
	}

	// Column count validation: the binlog TABLE_MAP_EVENT reports how many
	// columns exist in the table at write time. If it differs from the snapshot,
	// the column-to-name mapping would be wrong — skip and warn.
	if int(rowsEv.Table.ColumnCount) != len(tm.Columns) {
		logger.Warn("column count mismatch — skipping (consider re-running `bintrail snapshot`)",
			"file", filename,
			"pos", binlogEv.Header.LogPos,
			"schema", schema,
			"table", table,
			"binlog_columns", rowsEv.Table.ColumnCount,
			"snapshot_columns", len(tm.Columns))
		// File-index path only: an at-or-after-snapshot count divergence is a
		// stale snapshot (a DDL earlier in this same file added/removed a column
		// and its consumer-side resolver swap landed too late for these buffered
		// rows). Record it so ParseFile fails the file (#778). Pre-snapshot
		// events stay a warn-only historical skip.
		if gapTracker != nil && !isSnapshotExcludedSchema(schema) && !eventPredatesSnapshot(binlogEv, resolver) {
			if gapTracker.record(fmt.Sprintf("%s.%s column count %d vs snapshot %d at %s:%d", schema, table, rowsEv.Table.ColumnCount, len(tm.Columns), filename, binlogEv.Header.LogPos)) {
				logger.Error("schema gap: skipping rows whose column count diverges from the snapshot at-or-after snapshot time — the snapshot is stale; this file will be marked failed (run `bintrail snapshot`, then re-index)",
					"file", filename, "pos", binlogEv.Header.LogPos, "schema", schema, "table", table)
			}
		}
		// Same reasoning as the not-in-snapshot site above (#1296): name the
		// table in the ledger so the status verdict can say which table stopped
		// being captured. This reason's fix IS a fresh snapshot.
		skips.RecordSkipAttributed(SkipColumnCountMismatch, SkipAttribution{
			File:   filename,
			Pos:    uint64(binlogEv.Header.LogPos),
			Schema: schema,
			Table:  table,
		})
		return nil
	}

	// Column NAME cross-check (#700): with binlog_row_metadata=FULL the
	// TABLE_MAP event embeds the table's real column names at write time,
	// giving us per-event ground truth to hold the snapshot against. This
	// catches the drift class the count guard above CANNOT see: a same-count
	// schema change (a column rename, or a DROP+ADD in one ALTER) leaves the
	// count equal while every value after the changed ordinal would be
	// attributed to the WRONG column name — silent corruption, worse than the
	// count-mismatch skip. The compare is case-insensitive per MySQL's
	// identifier rules (a case-only rename does not change the mapping) and
	// includes generated columns (present in both the snapshot and the FULL
	// TABLE_MAP metadata — a different knob from the #493 guard's
	// binlog_row_image below). Under the default binlog_row_metadata=MINIMAL the event
	// carries no names and the check degrades to a no-op.
	//
	// A divergence splits on the event's age relative to the snapshot:
	//
	//   * Event AT-OR-AFTER the snapshot → the snapshot is STALE (the schema
	//     changed after it was taken). Fail loud like the partial-image guard
	//     below — proceeding would write wrong data that `recover` later
	//     trusts, and the remediation (re-run `bintrail snapshot`) genuinely
	//     converges: the fresh snapshot matches these events.
	//
	//   * Event BEFORE the snapshot → a routine historical state, not a
	//     stale snapshot: re-indexing old files after a rename, or a stream
	//     catching up through a backlog written before the operator
	//     re-snapshotted. Re-snapshotting is a NO-OP here (the old names are
	//     baked into the binlog), so a hard error would be a permanent dead
	//     end with no converging remediation. Proceed exactly as before
	//     #700 — values index under the snapshot's CURRENT names, which is
	//     positionally correct for a pure rename (and is what makes the
	//     generated recovery SQL executable against the live table) — and
	//     warn loudly per rows event, the count guard's verbosity class.
	//
	// The snapshot time comes from the bintrail host clock (TakeSnapshot)
	// while event timestamps come from the source server; a large clock skew
	// can misclassify a rename taken moments around the snapshot — the
	// failure mode is a loud warning instead of a hard stop, never silence.
	// A zero snapshot time (unknown) stays strict,
	// and so does a zero EVENT timestamp (tool-generated/rewritten binlogs
	// occasionally carry zeroed headers — an unknown age must not take the
	// lenient path).
	if names := rowsEv.Table.ColumnNameString(); len(names) > 0 && len(names) == len(tm.Columns) {
		for i := range names {
			if mysqlIdentEqualFold(names[i], tm.Columns[i].Name) {
				continue
			}
			eventTime := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
			if snapTime := resolver.SnapshotTime(); binlogEv.Header.Timestamp != 0 && !snapTime.IsZero() && eventTime.Before(snapTime) {
				logger.Warn("column names differ from snapshot for a pre-snapshot event — indexing under the snapshot's names",
					"file", filename,
					"pos", binlogEv.Header.LogPos,
					"schema", schema,
					"table", table,
					"ordinal", i+1,
					"binlog_column", names[i],
					"snapshot_column", tm.Columns[i].Name,
					"event_time", eventTime,
					"snapshot_time", snapTime)
				break
			}
			return &SchemaDriftError{msg: fmt.Sprintf(
				"schema drift detected at %s:%d for %s.%s: binlog TABLE_MAP column %d is %q but schema snapshot %d has %q; "+
					"the snapshot is stale (a column was renamed, or dropped and re-added, since it was taken) and indexing "+
					"these events would attribute row values to the wrong columns — run `bintrail snapshot` against the "+
					"current schema, then re-run indexing (a failed file re-indexes from the start; a stream resumes from "+
					"its checkpoint); if this event actually PREDATES the snapshot, check for clock skew between the "+
					"bintrail host and the source server",
				filename, binlogEv.Header.LogPos, schema, table,
				i+1, names[i], schemaVersion, tm.Columns[i].Name)}
		}
	}

	// Partial row image guard (#493): bintrail requires binlog_row_image=FULL.
	// Under MINIMAL/NOBLOB, MySQL omits columns from the before/after image and
	// go-mysql pads the absent positions to nil — which we would otherwise store
	// as a genuine NULL, silently corrupting the before/after images that
	// `recover` later trusts. The server-global SHOW VARIABLES check in
	// `metadata.ValidateBinlogRowImage` is one-shot and session-settable, so it
	// can be bypassed; this per-row check is the authoritative chokepoint that
	// covers BOTH the file-index and stream paths. Fail loud rather than index
	// NULL-filled rows.
	if firstSkipped := firstPartialImage(rowsEv.SkippedColumns); firstSkipped != nil {
		return &PartialRowImageError{msg: fmt.Sprintf(
			"partial binlog row image detected at %s:%d for %s.%s (%d column(s) absent: %v); "+
				"bintrail requires binlog_row_image=FULL — set it server-wide (a session-level "+
				"override produced this event) and re-generate the binlog, or these events would "+
				"be indexed with absent columns stored as NULL",
			filename, binlogEv.Header.LogPos, schema, table,
			len(firstSkipped), firstSkipped)}
	}

	// LogPos points to the byte AFTER the event. Subtract EventSize to get
	// start. A LogPos below EventSize would underflow into a ~2^64 start_pos —
	// the corruption that made the resume-time dedup (start_pos >= checkpoint)
	// delete every already-indexed row (#1117). Both producers now guarantee a
	// real position (FillZeroLogPos on the MariaDB stream syncers; the running-
	// offset fill in ParseFile), so this is a fail-loud belt: refuse to index a
	// row whose position could not be established rather than store a value
	// that later reads as "beyond every checkpoint".
	if uint64(binlogEv.Header.LogPos) < uint64(binlogEv.Header.EventSize) {
		return fmt.Errorf(
			"row event at %s has end position %d smaller than its size %d (%s.%s) — the binlog position for "+
				"this event could not be established (MariaDB 11.4+ writes cache-buffered events with end_log_pos=0; "+
				"the zero-LogPos fill should have replaced it before this point); refusing to index the row with an "+
				"underflowed start_pos, which the resume-time dedup would treat as beyond every checkpoint",
			filename, binlogEv.Header.LogPos, binlogEv.Header.EventSize, schema, table)
	}
	startPos := uint64(binlogEv.Header.LogPos) - uint64(binlogEv.Header.EventSize)
	endPos := uint64(binlogEv.Header.LogPos)
	ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
	pkCols := tm.PKColumnMetas()
	// stmtEnd marks the last ROWS_EVENT of a statement (STMT_END_F). endPos is a
	// valid POSITION-mode resume point only at a statement boundary — the stream
	// consumer stamps this onto every emitted row event so its safe checkpoint
	// never lands between the chunks of a split statement (#775).
	stmtEnd := rowsEv.Flags&replication.RowsEventStmtEndFlag != 0

	// MariaDB's MARIADB_*_ROWS_COMPRESSED_EVENT_V1 (log_bin_compress=ON, #520)
	// dispatch alongside their uncompressed siblings: go-mysql v1.13.0 already
	// routes them into RowsEvent decoding and decompresses the payload
	// (mysql.DecompressMariadbData in RowsEvent.DecodeData), so by the time the
	// event reaches handleRows its Rows/SkippedColumns are fully decoded — only
	// the header EventType still says "compressed".
	switch binlogEv.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		skips.RecordCaptured()
		return emitInserts(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, connectionID, commitTsUS, queryText, startPos, endPos, ts, pkCols, schemaVersion, stmtEnd, out)

	case replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2,
		replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		skips.RecordCaptured()
		return emitDeletes(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, connectionID, commitTsUS, queryText, startPos, endPos, ts, pkCols, schemaVersion, stmtEnd, out)

	case replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2,
		replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
		skips.RecordCaptured()
		return emitUpdates(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, connectionID, commitTsUS, queryText, startPos, endPos, ts, pkCols, schemaVersion, stmtEnd, out)

	default:
		// A RowsEvent whose type matches none of the above — e.g.
		// PARTIAL_UPDATE_ROWS_EVENT, which a MySQL source emits under
		// binlog_row_value_options=PARTIAL_JSON (out of support; binlog_row_image=FULL
		// is required). Decoding these is deferred; warn loudly — including how many
		// rows were skipped — rather than dropping them silently (a data-loss class).
		// Standard MySQL and MariaDB ROW DML always matches a specific case above.
		logger.Warn("unhandled row event type — rows skipped",
			"file", filename,
			"pos", binlogEv.Header.LogPos,
			"schema", schema,
			"table", table,
			"event_type", binlogEv.Header.EventType,
			"rows_skipped", len(rowsEv.Rows))
		observe.UnhandledRowsDropped(len(rowsEv.Rows))
		skips.RecordSkip(SkipUnhandledRowEvent)
	}

	return nil
}

func emitInserts(
	ctx context.Context,
	logger *slog.Logger,
	resolver *metadata.Resolver,
	rows [][]any,
	schema, table, filename, gtid string,
	connectionID uint32,
	commitTsUS uint64,
	queryText string,
	startPos, endPos uint64,
	ts time.Time,
	pkCols []metadata.ColumnMeta,
	schemaVersion uint32,
	stmtEnd bool,
	out emitter,
) error {
	for _, row := range rows {
		named, err := resolver.MapRow(schema, table, row)
		if err != nil {
			logger.Warn("failed to map INSERT row — skipping",
				"schema", schema, "table", table, "error", err)
			continue
		}
		ev := Event{
			BinlogFile: filename, StartPos: startPos, EndPos: endPos,
			Timestamp: ts, GTID: gtid, ConnectionID: connectionID, CommitTsUS: commitTsUS, QueryText: queryText,
			Schema: schema, Table: table, EventType: EventInsert,
			PKValues:      BuildPKValues(pkCols, named),
			RowAfter:      named,
			SchemaVersion: schemaVersion,
			StmtEnd:       stmtEnd,
		}
		if err := out.send(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

func emitDeletes(
	ctx context.Context,
	logger *slog.Logger,
	resolver *metadata.Resolver,
	rows [][]any,
	schema, table, filename, gtid string,
	connectionID uint32,
	commitTsUS uint64,
	queryText string,
	startPos, endPos uint64,
	ts time.Time,
	pkCols []metadata.ColumnMeta,
	schemaVersion uint32,
	stmtEnd bool,
	out emitter,
) error {
	for _, row := range rows {
		named, err := resolver.MapRow(schema, table, row)
		if err != nil {
			logger.Warn("failed to map DELETE row — skipping",
				"schema", schema, "table", table, "error", err)
			continue
		}
		ev := Event{
			BinlogFile: filename, StartPos: startPos, EndPos: endPos,
			Timestamp: ts, GTID: gtid, ConnectionID: connectionID, CommitTsUS: commitTsUS, QueryText: queryText,
			Schema: schema, Table: table, EventType: EventDelete,
			PKValues:      BuildPKValues(pkCols, named),
			RowBefore:     named,
			SchemaVersion: schemaVersion,
			StmtEnd:       stmtEnd,
		}
		if err := out.send(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

func emitUpdates(
	ctx context.Context,
	logger *slog.Logger,
	resolver *metadata.Resolver,
	rows [][]any,
	schema, table, filename, gtid string,
	connectionID uint32,
	commitTsUS uint64,
	queryText string,
	startPos, endPos uint64,
	ts time.Time,
	pkCols []metadata.ColumnMeta,
	schemaVersion uint32,
	stmtEnd bool,
	out emitter,
) error {
	// go-mysql delivers UPDATE rows as interleaved before/after pairs:
	//   rows[0]=before0, rows[1]=after0, rows[2]=before1, rows[3]=after1, ...
	for i := 0; i+1 < len(rows); i += 2 {
		before, err := resolver.MapRow(schema, table, rows[i])
		if err != nil {
			logger.Warn("failed to map UPDATE before-row — skipping",
				"schema", schema, "table", table, "error", err)
			continue
		}
		after, err := resolver.MapRow(schema, table, rows[i+1])
		if err != nil {
			logger.Warn("failed to map UPDATE after-row — skipping",
				"schema", schema, "table", table, "error", err)
			continue
		}
		ev := Event{
			BinlogFile: filename, StartPos: startPos, EndPos: endPos,
			Timestamp: ts, GTID: gtid, ConnectionID: connectionID, CommitTsUS: commitTsUS, QueryText: queryText,
			Schema: schema, Table: table, EventType: EventUpdate,
			PKValues:      BuildPKValues(pkCols, before), // PK from before-image
			RowBefore:     before,
			RowAfter:      after,
			SchemaVersion: schemaVersion,
			StmtEnd:       stmtEnd,
		}
		if err := out.send(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// mysqlIdentEqualFold reports whether two column identifiers are equal under
// MySQL's case-insensitive identifier comparison. strings.EqualFold covers
// every case MySQL folds except the Turkish dotted/dotless I pair: MySQL's
// identifier collation treats İ (U+0130) and ı (U+0131) as equal to I/i
// (verified on 8.4: CREATE TABLE t (i INT, İ INT) fails with a duplicate-
// column error, and RENAME COLUMN İstanbul TO istanbul succeeds as a
// case-style rename), while Unicode simple folding maps neither to 'i' — so
// a plain EqualFold would flag that legal case-style rename as drift (#700).
// Accents are NOT folded by MySQL identifiers (verified on 8.4: CREATE TABLE
// t (e INT, é INT) succeeds with two distinct columns), so no wider
// normalization is needed.
func mysqlIdentEqualFold(a, b string) bool {
	if strings.EqualFold(a, b) {
		return true
	}
	dotless := func(r rune) rune {
		if r == 'İ' || r == 'ı' {
			return 'i'
		}
		return r
	}
	return strings.EqualFold(strings.Map(dotless, a), strings.Map(dotless, b))
}

// BuildPKValues forwards to event.BuildPKValues (kept for back-compat; the
// canonical doc lives on event.BuildPKValues).
func BuildPKValues(pkColumns []metadata.ColumnMeta, row map[string]any) string {
	return event.BuildPKValues(pkColumns, row)
}

// ChangedColumns forwards to event.ChangedColumns (kept for back-compat; the
// canonical doc lives on event.ChangedColumns).
func ChangedColumns(before, after map[string]any) []string {
	return event.ChangedColumns(before, after)
}

// formatGTID formats a MySQL GTID from the raw 16-byte server UUID (SID) and
// the group number (GNO). Returns an empty string when there is no real GTID
// to report: eventType is ANONYMOUS_GTID_EVENT (gtid_mode=OFF still wraps
// every transaction in this event; go-mysql decodes it into the same
// GTIDEvent struct as a real GTID_EVENT, with a 16-zero-byte SID that would
// otherwise pass the length check below and format into a fake-but-valid-
// looking GTID, e.g. "00000000-0000-0000-0000-000000000000:0" — #678), or SID
// is not 16 bytes.
func formatGTID(eventType replication.EventType, sid []byte, gno int64) string {
	if eventType == replication.ANONYMOUS_GTID_EVENT {
		return ""
	}
	if len(sid) != 16 {
		return ""
	}
	// SID bytes map directly to the UUID string groups without byte-swapping.
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x:%d",
		sid[0:4], sid[4:6], sid[6:8], sid[8:10], sid[10:16], gno)
}

// DDLKind is the source-agnostic DDL-kind, moved to internal/event (#528).
// Aliased here so the parser and existing callers keep using parser.DDLKind.
type DDLKind = event.DDLKind

const (
	DDLAlterTable    = event.DDLAlterTable
	DDLCreateTable   = event.DDLCreateTable
	DDLDropTable     = event.DDLDropTable
	DDLRenameTable   = event.DDLRenameTable
	DDLTruncateTable = event.DDLTruncateTable
	DDLReplaceTable  = event.DDLReplaceTable
)

// ddlVerbRe recognizes a table DDL statement by its verb, matched against
// normalizeDDL's output (comments gone, whitespace collapsed to one space),
// anchored at the start. Group 1 is the verb, group 2 the text after it.
//
// Covered (#1664), MySQL and MariaDB:
//
//	ALTER [ONLINE] [IGNORE] TABLE
//	CREATE [OR REPLACE] TABLE
//	DROP TABLE[S]
//	RENAME TABLE[S]
//	TRUNCATE [TABLE]
//
// The verb must end at a character that cannot continue a name, which keeps
// TABLESPACE out. A TEMPORARY table is deliberately not matched: it is in no
// schema snapshot, and a DROP TABLE event refuses every reconstruct of a table
// with that name over its window.
// The s flag in ddlVerbRe and ddlNameRe is load-bearing: normalizeDDL copies
// quoted strings verbatim, line breaks included, and the trailing .* must run
// past them.
var ddlVerbRe = regexp.MustCompile(
	"(?is)^(ALTER(?: ONLINE)?(?: IGNORE)? TABLE|CREATE(?: OR REPLACE)? TABLE|DROP TABLES?|RENAME TABLES?|TRUNCATE(?: TABLE)?)" +
		"((?:[^\\w$\\x{80}-\\x{10FFFF}].*)?)$")

// ddlNameRe reads the first table after the verb: an optional IF [NOT] EXISTS,
// then [schema.]table, each name backticked, double-quoted (ANSI_QUOTES) or
// unquoted (MySQL allows any character from U+0080 up in an unquoted name).
// Group 1 is the schema, group 2 the table, group 3 what follows.
var ddlNameRe = regexp.MustCompile(
	"(?is)^ ?(?:IF(?: NOT)? EXISTS ?)?" +
		"(?:(`[^`]+`|\"[^\"]+\"|[\\w$\\x{80}-\\x{10FFFF}]+) ?\\. ?)?" +
		"(`[^`]+`|\"[^\"]+\"|[\\w$\\x{80}-\\x{10FFFF}]+)(.*)$")

// ddlKeysOnlyRe is an ALTER TABLE that only turns index maintenance off or
// on, which mysqldump wraps around every table's rows as
// /*!40000 ALTER TABLE `t` DISABLE KEYS */. It changes no definition, so it
// takes no schema snapshot.
var ddlKeysOnlyRe = regexp.MustCompile(`(?i)^ ?(?:DISABLE|ENABLE) KEYS ?;? ?$`)

// ddlHeadLimit bounds how much of a statement normalizeDDL builds: only the
// head can name a DDL verb and its table (two 64-character names, quoted, plus
// the modifiers), and parseDDL sees every QUERY_EVENT, including
// statement-format DML many megabytes long.
const ddlHeadLimit = 512

// unquoteDDLName strips the backticks or double quotes around a name.
func unquoteDDLName(s string) string {
	if len(s) >= 2 && (s[0] == '`' || s[0] == '"') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return s
}

// normalizeDDL returns queryStr as the server reads it for recognizing a DDL
// verb: plain comments (/* */, -- and #) removed, the body of an executable
// comment (/*!NNNNN ... */, MariaDB's /*M!NNNNN ... */) kept without its
// markers, and every run of whitespace or removed comment turned into one
// space. Quoted strings and backticked names are copied verbatim, so text
// inside them is never taken for a comment or a keyword.
//
// MySQL writes DDL to the binlog as the client sent it, comments included
// (a migration tool's /* app */, gh-ost's rename /* gh-ost */ table), and
// logs the implicit drop of a temporary table as
// DROP /*!40005 TEMPORARY */ TABLE, which must keep its TEMPORARY. It stops
// once ddlHeadLimit bytes are built.
func normalizeDDL(queryStr string) string {
	return normalizeDDLUpTo(queryStr, ddlHeadLimit)
}

// normalizeDDLUpTo is normalizeDDL building at most limit bytes. The whole
// statement is only worth building once it is known to be a DROP or RENAME,
// whose tail is nothing but the names it acts on.
func normalizeDDLUpTo(queryStr string, limit int) string {
	var b strings.Builder
	pending := false // a space is owed before the next copied byte
	open := 0        // executable comments whose closing */ is still ahead
	gap := func() { pending = b.Len() > 0 }
	for i := 0; i < len(queryStr) && b.Len() < limit; {
		rest := queryStr[i:]
		switch c := queryStr[i]; {
		case isSQLSpace(c):
			gap()
			i++
		case strings.HasPrefix(rest, "/*!") || strings.HasPrefix(rest, "/*M!"):
			i += strings.Index(rest, "!") + 1
			for i < len(queryStr) && queryStr[i] >= '0' && queryStr[i] <= '9' {
				i++
			}
			open++
			gap()
		case strings.HasPrefix(rest, "*/") && open > 0:
			open--
			i += 2
			gap()
		case strings.HasPrefix(rest, "/*"):
			end := strings.Index(rest[2:], "*/")
			if end < 0 {
				return b.String()
			}
			i += 2 + end + 2
			gap()
		case c == '#' || strings.HasPrefix(rest, "--") && (len(rest) == 2 || isSQLSpace(rest[2])):
			nl := strings.IndexByte(rest, '\n')
			if nl < 0 {
				return b.String()
			}
			i += nl + 1
			gap()
		case c == '\'' || c == '"' || c == '`':
			if pending {
				b.WriteByte(' ')
				pending = false
			}
			j := i + 1
			for j < len(queryStr) {
				if queryStr[j] == '\\' && c != '`' {
					j += 2
					continue
				}
				if queryStr[j] == c {
					if j+1 < len(queryStr) && queryStr[j+1] == c {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			j = min(j, len(queryStr))
			// Copied up to the limit only: a quoted DML value can be megabytes.
			b.WriteString(queryStr[i:min(j, i+limit-b.Len())])
			i = j
		default:
			if pending {
				b.WriteByte(' ')
				pending = false
			}
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// dmlKeywords are the statement prefixes that, under binlog_format=ROW, would
// have been logged as ROW events (and captured). Seeing one as a QUERY_EVENT
// means the source logged this DML in STATEMENT form (binlog_format=STATEMENT
// or MIXED, or a session-level override) — the row image is NOT in the binlog
// and the change cannot be captured. TRUNCATE is deliberately NOT here: it is
// always statement-logged and never produces row events under any binlog_format,
// so it is not a "row-DML that ROW format would have captured" and must never
// trip this loss detector (a false increment of statement_dml_dropped reads as
// data loss when nothing was lost). parseDDL claims TRUNCATE as DDL, comments
// and all (#1664); a TRUNCATE it still does not recognize must fall through
// here silently — again, nothing was lost.
var dmlKeywords = []string{"INSERT", "UPDATE", "DELETE", "REPLACE", "LOAD DATA"}

// statementDML reports whether queryStr is a data-modifying statement logged in
// STATEMENT form — the DML that binlog_format=ROW would have captured as row
// events. It trims leading whitespace and SQL comments, then matches a DML
// keyword prefix on a word boundary. Transaction-control (BEGIN/COMMIT/ROLLBACK/
// SAVEPOINT/XA/SET) and every DDL/DCL verb are excluded by construction: only
// the dmlKeywords allowlist matches, so a keyword prefix on a non-DML statement
// cannot false-positive. Returns the matched keyword (for the warning) and true,
// or "" and false. Callers MUST give DDL (parseDDL) the chance to claim the
// statement first. TRUNCATE is excluded (see dmlKeywords) — it never produces
// row events, so it must return false here even when parseDDL misses it.
func statementDML(queryStr string) (string, bool) {
	upper := strings.ToUpper(stripLeadingSQLComments(queryStr))
	for _, kw := range dmlKeywords {
		if !strings.HasPrefix(upper, kw) {
			continue
		}
		// Word boundary: the keyword must be the whole statement or be followed
		// by whitespace, so INSERT matches "INSERT INTO ..." but not an
		// identifier that merely starts with those letters.
		if rest := upper[len(kw):]; rest == "" || isSQLSpace(rest[0]) {
			return kw, true
		}
	}
	return "", false
}

// statementDMLInScope reports whether a statement-format DML with the given
// session default database warrants the operator-facing coverage-gap signal
// (WARN + statement_dml_dropped metric + capture-skip ledger). The gate keys
// on the QUERY_EVENT's session default DB — the schema a bare
// `INSERT INTO t ...` resolves against — which is a HEURISTIC, not proof: a
// statement can qualify a different schema explicitly, so
// `USE legacy; UPDATE shop.orders ...` is silenced under `--schemas shop`
// even though its target is captured. That is issue #1000's accepted
// tradeoff (the default DB is the only scope key available in the
// QUERY_EVENT); genuine ambiguity — an empty default DB — errs loud.
//
//   - Empty schema (no session default DB): ambiguous → in scope, warn.
//   - System schema (isSnapshotExcludedSchema): snapshots always exclude
//     these, so nothing capturable was lost. This is the RDS/Aurora false
//     alarm: `rdsadmin@localhost` writes mysql.* heartbeat/bookkeeping rows
//     in STATEMENT format as routine housekeeping on every deployment;
//     counting those marked Capture health DEGRADED — and, on an idle
//     source, tripped the consecutive-skip escalation ERROR — with no real
//     gap anywhere.
//   - Schema not in the configured --schemas filter: the operator asked not
//     to capture it.
//   - --tables filter configured and no filtered table lives in the schema:
//     same reasoning per-table. When at least one filtered table IS in the
//     schema, the statement might target it → in scope, warn.
func statementDMLInScope(schema string, filters *Filters) bool {
	if schema == "" {
		return true
	}
	if isSnapshotExcludedSchema(schema) {
		return false
	}
	if filters == nil {
		return true
	}
	if filters.Schemas != nil && !filters.Schemas[schema] {
		return false
	}
	if filters.Tables != nil {
		prefix := schema + "."
		for key := range filters.Tables {
			if strings.HasPrefix(key, prefix) {
				return true
			}
		}
		return false
	}
	return true
}

// stripLeadingSQLComments removes leading whitespace and SQL comments so the
// DML-prefix check sees the first real keyword. Handles the three MySQL comment
// forms (/* */ block, -- to EOL, # to EOL); loops because a statement can carry
// several. The common case (no leading comment) returns after one TrimLeft.
func stripLeadingSQLComments(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n\f\v")
		switch {
		case strings.HasPrefix(s, "/*"):
			end := strings.Index(s[2:], "*/")
			if end < 0 {
				return "" // unterminated — no real statement follows
			}
			s = s[2+end+2:]
		case strings.HasPrefix(s, "--"):
			// MySQL treats -- as a comment only when followed by whitespace/EOL.
			if len(s) > 2 && !isSQLSpace(s[2]) {
				return s
			}
			nl := strings.IndexByte(s, '\n')
			if nl < 0 {
				return ""
			}
			s = s[nl+1:]
		case strings.HasPrefix(s, "#"):
			nl := strings.IndexByte(s, '\n')
			if nl < 0 {
				return ""
			}
			s = s[nl+1:]
		default:
			return s
		}
	}
}

func isSQLSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n' || b == '\f' || b == '\v'
}

// parseDDL parses a QUERY_EVENT for DDL statements and returns a DDL Event.
// Returns zero Event and false if the query is not a DDL statement.
// TRUNCATE is included for audit purposes but does not invalidate the snapshot
// (callers should skip auto-snapshot for DDLTruncateTable).
//
// defaultSchema is the QUERY_EVENT's session default database (#1435): the
// unqualified form (`USE wordpress; TRUNCATE TABLE t`) is what people and
// ORMs actually type, and recording it with an empty schema_name made most
// DDL rows unfindable by the schema filter. A schema qualified in the
// statement always wins — the fallback is exactly the database MySQL itself
// resolved the unqualified name against. It can be empty (a client with no
// default database running qualified DDL); then schema_name stays empty, as
// before. Historical rows are NOT backfilled: the original session's default
// is unrecoverable, and matching empty rows against any filter would lie.
func parseDDL(logger *slog.Logger, filename string, logPos uint32, timestamp time.Time, gtid, queryStr, defaultSchema string, schemaVersion uint32) (Event, bool) {
	m := ddlVerbRe.FindStringSubmatch(normalizeDDL(queryStr))
	if m == nil {
		return Event{}, false
	}
	// A verb whose table cannot be read is still DDL, with an empty table, as
	// the prefix check always returned it: the snapshot hooks refresh every
	// schema in scope on any table DDL, so dropping the event would leave the
	// rows after it decoded with the old columns.
	var schema, table string
	if n := ddlNameRe.FindStringSubmatch(m[2]); n != nil {
		schema, table = unquoteDDLName(n[1]), unquoteDDLName(n[2])
		if strings.HasPrefix(strings.ToUpper(m[1]), "ALTER") && ddlKeysOnlyRe.MatchString(n[3]) {
			return Event{}, false
		}
	}
	var ddlType DDLKind
	switch verb := strings.ToUpper(m[1]); {
	case strings.HasPrefix(verb, "ALTER"):
		ddlType = DDLAlterTable
	case strings.HasPrefix(verb, "CREATE OR REPLACE"):
		ddlType = DDLReplaceTable
	case strings.HasPrefix(verb, "CREATE"):
		ddlType = DDLCreateTable
	case strings.HasPrefix(verb, "DROP"):
		ddlType = DDLDropTable
	case strings.HasPrefix(verb, "RENAME"):
		ddlType = DDLRenameTable
	default:
		ddlType = DDLTruncateTable
	}
	if schema == "" {
		schema = defaultSchema
	}
	// A DROP or RENAME can name several tables, and each one's rows stop being
	// what they were: every name gets its schema_changes row, so the
	// destructive-DDL guards see it. Read from the whole statement, past the
	// head, and the first name read this way is the event's own, so the two
	// never disagree.
	var tables []event.DDLTable
	if ddlType == DDLDropTable || ddlType == DDLRenameTable {
		if full := ddlVerbRe.FindStringSubmatch(normalizeDDLUpTo(queryStr, len(queryStr)+1)); full != nil {
			tables = ddlNameList(full[2], ddlType == DDLRenameTable, defaultSchema)
		}
		if len(tables) > 0 {
			schema, table = tables[0].Schema, tables[0].Table
		}
	}

	startPos := uint64(0) // DDL events have no row-level start position
	endPos := uint64(logPos)

	logger.Warn("DDL detected",
		"file", filename,
		"pos", logPos,
		"ddl_type", ddlType,
		"schema", schema,
		"table", table,
		"tables_named", len(tables),
		"query", queryStr)

	return Event{
		BinlogFile:    filename,
		StartPos:      startPos,
		EndPos:        endPos,
		Timestamp:     timestamp,
		GTID:          gtid,
		Schema:        schema,
		Table:         table,
		EventType:     EventDDL,
		DDLQuery:      queryStr,
		DDLType:       ddlType,
		DDLTables:     tables,
		SchemaVersion: schemaVersion,
	}, true
}

// ddlNameList reads the tables a DROP or RENAME names from rest, the
// normalized statement after its verb: every [schema.]name in order, repeats
// kept, until something that is not the list (DROP's RESTRICT, CASCADE, WAIT,
// NOWAIT or ";", or anything unexpected), keeping what it read. A RENAME
// gives both sides of every "a TO b" pair. An unqualified name takes
// defaultSchema, as MySQL resolves it.
func ddlNameList(rest string, rename bool, defaultSchema string) []event.DDLTable {
	sc := ddlScanner{s: rest}
	sc.keyword("IF EXISTS")
	var out []event.DDLTable
	for {
		n, ok := sc.qualifiedName(defaultSchema)
		if !ok {
			return out
		}
		out = append(out, n)
		if rename {
			// MariaDB: RENAME TABLE a [WAIT n | NOWAIT] TO b.
			if sc.keyword("WAIT") {
				sc.number()
			} else {
				sc.keyword("NOWAIT")
			}
			if !sc.keyword("TO") {
				return out
			}
			if n, ok = sc.qualifiedName(defaultSchema); !ok {
				return out
			}
			out = append(out, n)
		}
		sc.space()
		if !sc.byte(',') {
			return out
		}
	}
}

// ddlScanner walks a normalizeDDL result: runs of whitespace are single
// spaces, comments are gone, quoted names are verbatim.
type ddlScanner struct {
	s string
	i int
}

func (sc *ddlScanner) space() {
	for sc.i < len(sc.s) && sc.s[sc.i] == ' ' {
		sc.i++
	}
}

func (sc *ddlScanner) byte(c byte) bool {
	if sc.i < len(sc.s) && sc.s[sc.i] == c {
		sc.i++
		return true
	}
	return false
}

// keyword consumes kw (words separated by single spaces, any case) when it is
// next and not the start of a longer identifier.
func (sc *ddlScanner) keyword(kw string) bool {
	sc.space()
	end := sc.i + len(kw)
	if end > len(sc.s) || !strings.EqualFold(sc.s[sc.i:end], kw) {
		return false
	}
	if end < len(sc.s) {
		if r, _ := utf8.DecodeRuneInString(sc.s[end:]); isDDLNameRune(r) {
			return false
		}
	}
	sc.i = end
	return true
}

func (sc *ddlScanner) number() {
	sc.space()
	for sc.i < len(sc.s) && (sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' || sc.s[sc.i] == '.') {
		sc.i++
	}
}

// qualifiedName reads [schema.]name.
func (sc *ddlScanner) qualifiedName(defaultSchema string) (event.DDLTable, bool) {
	first, ok := sc.ident()
	if !ok {
		return event.DDLTable{}, false
	}
	sc.space()
	if sc.byte('.') {
		if table, ok := sc.ident(); ok {
			return event.DDLTable{Schema: first, Table: table}, true
		}
	}
	return event.DDLTable{Schema: defaultSchema, Table: first}, true
}

// ident reads one name: backticked or double-quoted (ANSI_QUOTES), where a
// doubled quote stands for itself, or unquoted.
func (sc *ddlScanner) ident() (string, bool) {
	sc.space()
	if sc.i >= len(sc.s) {
		return "", false
	}
	if q := sc.s[sc.i]; q == '`' || q == '"' {
		var b strings.Builder
		for j := sc.i + 1; j < len(sc.s); j++ {
			if sc.s[j] != q {
				b.WriteByte(sc.s[j])
				continue
			}
			if j+1 < len(sc.s) && sc.s[j+1] == q {
				b.WriteByte(q)
				j++
				continue
			}
			if b.Len() == 0 {
				return "", false
			}
			sc.i = j + 1
			return b.String(), true
		}
		return "", false // no closing quote
	}
	start := sc.i
	for sc.i < len(sc.s) {
		r, size := utf8.DecodeRuneInString(sc.s[sc.i:])
		if !isDDLNameRune(r) {
			break
		}
		sc.i += size
	}
	return sc.s[start:sc.i], sc.i > start
}

// isDDLNameRune is a rune an unquoted name may hold: ddlNameRe's class.
func isDDLNameRune(r rune) bool {
	return r == '_' || r == '$' || r >= 0x80 ||
		r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
}

// SchemaDriftError is the #700 hard error: a TABLE_MAP whose column names
// disagree with the schema snapshot at or after the snapshot's own time, so
// indexing would attribute row values to the wrong columns. The message names
// the file, position, table and both column spellings; that text is for the
// operator and never leaves the process — the only thing the type exposes to
// usage telemetry is its class.
type SchemaDriftError struct{ msg string }

func (e *SchemaDriftError) Error() string { return e.msg }

// TelemetryClass implements telemetry.Classed.
func (e *SchemaDriftError) TelemetryClass() string { return "schema_mismatch" }

// PartialRowImageError is the #493 guard: a row event whose image omits
// columns (binlog_row_image=MINIMAL/NOBLOB, possibly a session override that
// the one-shot startup check could not see). A server setting, so its
// usage-telemetry class is config_invalid; the message names the file,
// position and table for the operator and never leaves the process.
type PartialRowImageError struct{ msg string }

func (e *PartialRowImageError) Error() string { return e.msg }

// TelemetryClass implements telemetry.Classed.
func (e *PartialRowImageError) TelemetryClass() string { return "config_invalid" }
