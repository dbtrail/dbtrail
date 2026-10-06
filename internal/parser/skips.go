// Package parser — this file provides SkipCounters, the capture-time tally of
// events the daemon READ from the stream and chose to DROP (#1034): per-reason
// monotonic counters, persisted alongside the stream checkpoint so `bintrail
// status` can render a Capture health verdict that survives daemon restarts.
//
// This is the sibling of the #649 continuity verdict: continuity answers "did
// the stream LOSE events it never received?" (a binlog gap); these counters
// answer "did the stream DISCARD events it did receive?" (e.g. the column-count
// guard rejecting every row against a stale/corrupt snapshot). Both end the
// same way for the user — a restore window they believe exists and doesn't —
// but without these counters the second failure was invisible: the checkpoint
// stayed fresh and continuity honestly reported "no gaps" while 100% of rows
// were skipped.
package parser

import (
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"time"
)

// Capture-skip reason identifiers. Stable strings: they are persisted verbatim
// in stream_state.capture_skips and surfaced by `bintrail status` (text and
// JSON), so renaming one silently splits its history into two counters.
const (
	// SkipColumnCountMismatch — the binlog TABLE_MAP column count diverged
	// from the schema snapshot; indexing would map values to wrong names.
	SkipColumnCountMismatch = "column_count_mismatch"
	// SkipTableNotInSnapshot — the row's table is absent from the schema
	// snapshot (system schemas the snapshot deliberately excludes are NOT
	// counted — e.g. mysql.rds_heartbeat2 is a routine, permanent skip that
	// must never mark capture degraded).
	SkipTableNotInSnapshot = "table_not_in_snapshot"
	// SkipTableExcludedFromSnapshot — the row's table is absent from the
	// snapshot because the snapshot VALIDATOR excluded it (#1051: no explicit
	// primary key, or a non-InnoDB engine). Split from SkipTableNotInSnapshot
	// (#1296) because the two have opposite remedies and merging them hands the
	// operator a remediation that cannot converge: re-snapshotting excludes
	// this table again, forever. The fix is on the source's DDL, not here.
	SkipTableExcludedFromSnapshot = "table_excluded_from_snapshot"
	// SkipNoResolver — no schema snapshot was available at all.
	SkipNoResolver = "no_resolver"
	// SkipUnhandledRowEvent — a RowsEvent type bintrail does not decode
	// (e.g. PARTIAL_UPDATE_ROWS_EVENT under binlog_row_value_options).
	SkipUnhandledRowEvent = "unhandled_row_event"
	// SkipRowMapFailed — a rows event whose table and column count matched the
	// snapshot, but at least one of its rows could not be mapped to the
	// columns (#2139). In practice a CHAR/VARCHAR value that is not valid
	// UTF-8 in a column whose character set bintrail does not transcode, or
	// whose character set a pre-#756 snapshot never captured
	// (metadata.coerceTextEncoding). Counted once per rows event, like every
	// other reason, however many of its rows failed; the rows that map are
	// still indexed.
	SkipRowMapFailed = "row_map_failed"
	// SkipStatementFormatDML — a STATEMENT/MIXED-format DML whose row image
	// is not in the binlog (#999); the change cannot be captured.
	SkipStatementFormatDML = "statement_format_dml"
	// SkipUnpairedUpdateImage — an UPDATE rows event carried an odd number of
	// row images (#2144). The binlog format always writes before/after pairs,
	// so this means a decoding fault; the complete pairs are indexed and the
	// trailing image is dropped, recorded once per event like row_map_failed.
	SkipUnpairedUpdateImage = "unpaired_update_image"
	// SkipUnreadablePreviousLedger — meta-reason (#1206): the previous run's
	// persisted ledger existed but could not be parsed at restart. Not an
	// event skip — it preserves the FACT that a loss tally may have been
	// destroyed, so `status` stays non-clean (and --fail-on-gap keeps failing
	// closed) instead of the next checkpoint silently laundering the evidence
	// into "{}" = OK. Cleared only by the operator acknowledgement runbook
	// (clear the ledger with the daemon stopped).
	SkipUnreadablePreviousLedger = "unreadable_previous_ledger"
)

// SkipEscalationThreshold is the number of CONSECUTIVE skipped events (no
// captured event in between) after which SkipCounters emits a single ERROR
// with remediation guidance — the per-event WARNs blend into noise exactly
// when they matter most (#1034 observed 100% skips for ~2 days). One ERROR
// per degraded episode: the flag re-arms only after an event is captured.
//
// The run is all-or-nothing per EVENT, and that is a decision (#2144), not an
// accident: an event that gets at least one row through counts as captured and
// resets the run, so a table where most rows of every event are dropped never
// escalates. That condition is reported by the per-reason ledger entry (which
// marks capture DEGRADED in `status`), not by this line, whose job is to say
// the whole stream is discarding everything.
const SkipEscalationThreshold = 100

// SkipStat is one reason's monotonic tally. The JSON field names are the
// persistence format of stream_state.capture_skips — internal/status decodes
// the same shape independently (it deliberately does not import this package).
type SkipStat struct {
	Count  int64     `json:"count"`
	LastAt time.Time `json:"last_at"`
	// Last-seen attribution (#999): where the most recent skip happened, enough
	// to hunt the source without ever storing statement text (a DML statement
	// embeds row VALUES — same reason the per-event WARN omits it). Optional —
	// only the STREAMING statement-DML site stamps these today (file-mode
	// `bintrail index` detects the same drops but persists no ledger at all);
	// omitempty keeps documents persisted before this change (and reasons
	// without attribution) unchanged.
	LastFile          string `json:"last_file,omitempty"`
	LastPos           uint64 `json:"last_pos,omitempty"`
	LastStatementType string `json:"last_statement_type,omitempty"`
	LastConnectionID  uint32 `json:"last_connection_id,omitempty"`

	// Tables lists the distinct "schema.table" names this reason dropped rows
	// for, capped at MaxLedgerTables (#1296). Without it an operator is told
	// changes are missing but not WHICH table's — the first question they ask,
	// and one only the daemon log could answer before. Absent on a ledger
	// written by an older daemon: consumers must then name no table at all
	// rather than present an empty list as "none".
	Tables []string `json:"tables,omitempty"`
	// TablesTruncated records that more distinct tables were skipped than
	// Tables holds, or that the list is incomplete because part of Count was
	// recorded before this reason carried table names. The list is capped because this document is persisted in a
	// single stream_state column and grows monotonically — an unfiltered source
	// with thousands of unsnapshotted tables would grow it without bound. With
	// no flag, a capped list would read as the complete set.
	TablesTruncated bool `json:"tables_truncated,omitempty"`
	// LastDetail is the newest per-skip explanation the detection site could
	// state (today: the snapshot validator's exclusion reason). Display-only
	// free text, never parsed.
	LastDetail string `json:"last_detail,omitempty"`
}

// MaxLedgerTables caps SkipStat.Tables — enough names to act on in one sitting
// without letting a persisted, monotonic document grow unbounded.
const MaxLedgerTables = 8

// SkipAttribution locates one skipped event: binlog file/pos, the statement
// keyword (derived from the statement text, which is then discarded), and the
// connection id from the QUERY event post-header. Never carries the statement
// text itself.
type SkipAttribution struct {
	File          string
	Pos           uint64
	StatementType string
	ConnectionID  uint32
	// Schema and Table name the table whose rows were dropped. Kept as two
	// plain strings, not a slice: RecordSkipAttributed compares this struct
	// against its zero value with ==, which a slice field would break at
	// compile time.
	Schema string
	Table  string
	// Detail is a short human explanation of THIS skip (today: the snapshot
	// validator's exclusion reason). Optional.
	Detail string
}

// SkipCounters aggregates capture-time skips by reason. Safe for concurrent
// use; every method is nil-receiver-safe so the file-index path (which has its
// own failure mechanism, the #778 gap tracker) can simply pass nil.
type SkipCounters struct {
	mu          sync.Mutex
	logger      *slog.Logger
	byReason    map[string]SkipStat
	consecutive int64
	escalated   bool
}

// NewSkipCounters creates an empty counter set. logger (nil = slog.Default())
// receives the single escalation ERROR when SkipEscalationThreshold
// consecutive events have been skipped.
func NewSkipCounters(logger *slog.Logger) *SkipCounters {
	if logger == nil {
		logger = slog.Default()
	}
	return &SkipCounters{logger: logger, byReason: map[string]SkipStat{}}
}

// Seed replaces the counters with a previously persisted Snapshot document, so
// a daemon restart resumes the monotonic tallies instead of silently zeroing
// the DEGRADED verdict. Empty raw is a no-op (nothing persisted yet); invalid
// JSON is returned as an error and leaves the counters unchanged.
func (c *SkipCounters) Seed(raw string) error {
	if c == nil || raw == "" {
		return nil
	}
	m := map[string]SkipStat{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byReason = m
	return nil
}

// SeedPreserving is Seed for the daemon restart path (#1206). A document that
// fails to parse is NOT discarded into fresh counters: the failure itself is
// recorded under SkipUnreadablePreviousLedger, so the next checkpoint persists
// a non-clean ledger instead of overwriting possible loss evidence with the
// affirmative "{}". The parse error is returned for the caller to log.
func (c *SkipCounters) SeedPreserving(raw string) error {
	err := c.Seed(raw)
	if err != nil {
		c.RecordSkip(SkipUnreadablePreviousLedger)
	}
	return err
}

// RecordSkip notes one skipped event under reason, stamping the current time.
// When SkipEscalationThreshold consecutive events have been skipped it emits
// ONE ERROR with remediation (re-armed by the next RecordCaptured).
func (c *SkipCounters) RecordSkip(reason string) {
	c.RecordSkipAttributed(reason, SkipAttribution{})
}

// RecordSkipAttributed is RecordSkip carrying the skipped event's location
// (#999). A zero attribution leaves any previously stamped attribution for the
// reason in place — an unattributed count must not erase the last useful lead
// (consequence: the attribution can lag last_at if a caller mixes attributed
// and unattributed skips of one reason).
func (c *SkipCounters) RecordSkipAttributed(reason string, attr SkipAttribution) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.byReason[reason]
	st.Count++
	st.LastAt = time.Now().UTC()
	if attr != (SkipAttribution{}) {
		st.LastFile, st.LastPos = attr.File, attr.Pos
		st.LastStatementType, st.LastConnectionID = attr.StatementType, attr.ConnectionID
		if attr.Detail != "" {
			st.LastDetail = attr.Detail
		}
		if attr.Table != "" {
			// A count with no table list was written before this reason
			// carried table names (a ledger persisted by an older daemon, or
			// an unattributed RecordSkip). Readers take "no tables" to mean
			// "any table"; a list that starts now would narrow that to the
			// tables seen from here on and clear the ones dropped earlier.
			// Mark the list incomplete so it keeps reading as "these and
			// others" (#2139).
			if st.Count > 1 && len(st.Tables) == 0 {
				st.TablesTruncated = true
			}
			addLedgerTable(&st, attr.Schema, attr.Table)
		}
	}
	c.byReason[reason] = st
	c.consecutive++
	if c.consecutive >= SkipEscalationThreshold && !c.escalated {
		c.escalated = true
		remediation := "the schema snapshot is likely stale or corrupt — run `bintrail snapshot` against the source, then restart the stream"
		switch reason {
		case SkipStatementFormatDML:
			remediation = "set binlog_format=ROW server-wide on the source (a STATEMENT/MIXED format or a session-level override is producing row-less events)"
		case SkipTableExcludedFromSnapshot:
			// Never point this reason at `bintrail snapshot`: the validator
			// excludes the same table on every re-run (#1051/#1199), so that
			// remediation loops forever while the operator believes they fixed it.
			remediation = "these tables are excluded from every snapshot by validation — give each an explicit PRIMARY KEY on an InnoDB engine at the source; re-snapshotting alone will not capture them"
		}
		c.logger.Error("sustained event skipping — capture is effectively stopped: every recent event was read from the stream and discarded, while the checkpoint keeps advancing past them; the skipped changes are NOT in the index",
			"consecutive_skips", c.consecutive,
			"reason", reason,
			"remediation", remediation)
	}
}

// addLedgerTable adds "schema.table" to st.Tables if it is new and the cap
// allows, flagging TablesTruncated otherwise. Insertion order is preserved (the
// first tables to break are the ones that broke first); dedup is linear because
// the slice never exceeds MaxLedgerTables entries.
func addLedgerTable(st *SkipStat, schema, table string) {
	name := schema + "." + table
	if slices.Contains(st.Tables, name) {
		return
	}
	if len(st.Tables) >= MaxLedgerTables {
		st.TablesTruncated = true
		return
	}
	st.Tables = append(st.Tables, name)
}

// RecordCaptured notes that an event cleared every guard and was emitted for
// indexing: the consecutive-skip run is broken and the escalation re-arms.
// The per-reason tallies are monotonic and deliberately NOT reset.
func (c *SkipCounters) RecordCaptured() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consecutive = 0
	c.escalated = false
}

// Count returns how many events were skipped under reason so far.
func (c *SkipCounters) Count(reason string) int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byReason[reason].Count
}

// Snapshot returns the JSON document persisted to stream_state.capture_skips:
// {"<reason>":{"count":N,"last_at":"RFC3339"}}. Always valid JSON — "{}" when
// nothing was skipped, which is the affirmative evaluated-and-clean marker
// that lets `status` print "Capture health: OK" (a NULL column means no
// skip-aware daemon ever wrote, so OK must not be asserted).
func (c *SkipCounters) Snapshot() (string, error) {
	if c == nil {
		return "{}", nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := json.Marshal(c.byReason)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Total returns the sum of all per-reason counts (test/display helper).
func (c *SkipCounters) Total() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int64
	for _, st := range c.byReason {
		n += st.Count
	}
	return n
}
