// Package indexer consumes parsed binlog events and batch-inserts them into
// the binlog_events table in the index MySQL database.
package indexer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// WriteTimeout bounds a single index write (e.g. a batch INSERT, the checkpoint
// UPSERT, or a statement-digest lookup) so a mid-statement network stall surfaces as an error in minutes
// instead of the kernel's ~13-16 min TCP give-up. config.Connect sets only a
// connect timeout, never a read/write deadline, so without this a frozen VM or
// an idle-dropped NLB leaves the daemon blocked — healthy to `watch`, capturing
// nothing and advancing no checkpoint (#959). The window sits well above any
// healthy batch INSERT and below the kernel give-up, and is tunable per
// deployment via --write-timeout for the cases where a healthy write legitimately
// runs long: a very large batch over a slow link, or index-side MDL contention
// from a concurrent partition rotation (#959).
const DefaultWriteTimeout = 3 * time.Minute

// WriteTimeout is the effective deadline, set from --write-timeout at startup
// (default DefaultWriteTimeout). A var (not const) so the flag can set it and
// tests can shrink it — a test that mutates it must NOT call t.Parallel() (it is
// a shared global).
var WriteTimeout = DefaultWriteTimeout

// ErrWriteDeadline tags an index write that ran out of WriteTimeout, as opposed
// to one the server refused. It exists because nothing else distinguished the
// two: every flush failure reached the caller as an opaque error, so an index
// that was merely SLOW (a heavy analytical read starving it) ended capture just
// as surely as an un-indexable event did — the daemon exited and nothing brought
// it back (#1482).
//
// A deadline is the only failure class where "the same write, later" is a
// reasonable thing to attempt: the write never got a verdict, so the server
// neither accepted nor rejected it. Match with errors.Is; never on message text.
//
// Deliberately NOT a licence to re-execute the INSERT in place. The deadline
// fires client-side, and the driver reacts by closing the socket without a KILL
// QUERY, so a slow batch usually goes on to COMMIT on the server after the
// client has given up. Re-running that statement would duplicate every row it
// wrote, permanently. The only duplicate-safe way to act on this tag is to
// restart the stream, which replays from the checkpoint through
// streamrun's dedup-on-resume — the step that exists to drop exactly those rows.
var ErrWriteDeadline = errors.New("index write deadline exceeded")

// writeDeadlineError carries ErrWriteDeadline alongside the original error
// without changing the message operators read: Error() delegates verbatim, and
// the multi-error Unwrap keeps BOTH the wrapped cause (context.DeadlineExceeded,
// still matched by errors.Is, as write_timeout_test.go pins) and the sentinel
// reachable in the chain.
type writeDeadlineError struct{ err error }

func (e *writeDeadlineError) Error() string   { return e.err.Error() }
func (e *writeDeadlineError) Unwrap() []error { return []error{e.err, ErrWriteDeadline} }

// insertColumnsSQL is the column list of insertBatch's multi-row INSERT.
// insertColumnCount must equal its column count — pinned by a unit test.
const insertColumnsSQL = `binlog_file, start_pos, end_pos, event_timestamp, gtid, connection_id, ` +
	`schema_name, table_name, event_type, pk_values, ` +
	`changed_columns, row_before, row_after, schema_version, query_text, query_hash, commit_ts_us`

const (
	// insertColumnCount is the number of columns (= placeholders per row) in
	// insertBatch's multi-row INSERT.
	insertColumnCount = 17
	// maxPreparedStmtParams is MySQL's hard cap on placeholders in one
	// prepared statement (ER_PS_MANY_PARAM, error 1390).
	maxPreparedStmtParams = 65535
	// MaxBatchSize is the largest batch whose INSERT stays under the
	// placeholder cap. Derived from insertColumnCount so adding a column to
	// the INSERT lowers the limit instead of silently re-breaking large
	// batches (#956).
	MaxBatchSize = maxPreparedStmtParams / insertColumnCount
)

// rowPlaceholders is one row's "(?,...,?)" tuple, derived from
// insertColumnCount so the placeholder count and MaxBatchSize stay in
// lockstep.
var rowPlaceholders = "(" + strings.TrimSuffix(strings.Repeat("?,", insertColumnCount), ",") + ")"

// Indexer consumes event.Events from a channel and batch-inserts them into
// the binlog_events table.
type Indexer struct {
	db        *sql.DB
	batchSize int
	onDDL     func(ev event.Event) error
	// digestWarnOnce rate-limits the STATEMENT_DIGEST-unavailable warning to
	// one line per Indexer — without it a non-8.0 index would warn every batch.
	digestWarnOnce sync.Once
	// digestPartialWarnOnce rate-limits the some-statements-failed warning:
	// a systematic-but-partial condition (one application's statements always
	// tripping the digest) must be discoverable without waiting for the
	// all-failed case, but must not warn on every batch either.
	digestPartialWarnOnce sync.Once
	// digestUnavailable short-circuits digesting for the Indexer's lifetime
	// once the index has proven it lacks STATEMENT_DIGEST entirely (MySQL
	// error 1305, unknown function — e.g. a MariaDB index outside the 8.0+
	// contract). Without it a long-lived stream daemon would pay one failed
	// combined SELECT plus N failed per-text SELECTs on every batch, forever,
	// after its single warning line.
	digestUnavailable bool
}

// New creates an Indexer writing to db with the given batch size.
func New(db *sql.DB, batchSize int) *Indexer {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if batchSize > MaxBatchSize {
		slog.Warn("batch size exceeds MySQL's prepared-statement placeholder cap, clamping",
			"requested", batchSize, "max", MaxBatchSize)
		batchSize = MaxBatchSize
	}
	return &Indexer{db: db, batchSize: batchSize}
}

// SetOnDDL registers a callback invoked when a DDL event is received.
// The current batch is flushed before the callback is called.
// DDL events are NOT inserted into binlog_events.
func (idx *Indexer) SetOnDDL(fn func(event.Event) error) {
	idx.onDDL = fn
}

// Run reads events from the channel until it is closed or ctx is cancelled,
// flushing to MySQL in batches. Returns the total number of rows inserted.
func (idx *Indexer) Run(ctx context.Context, events <-chan event.Event) (int64, error) {
	batch := make([]event.Event, 0, idx.batchSize)
	var total int64

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := idx.insertBatch(batch)
		if err != nil {
			return err
		}
		total += n
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		case ev, ok := <-events:
			if !ok {
				// Channel closed — flush the final partial batch.
				return total, flush()
			}
			// DDL events: flush current batch, invoke callback, skip insertion.
			if ev.EventType == event.EventDDL {
				if err := flush(); err != nil {
					return total, err
				}
				if idx.onDDL != nil {
					if err := idx.onDDL(ev); err != nil {
						return total, fmt.Errorf("onDDL callback: %w", err)
					}
				}
				continue
			}
			batch = append(batch, ev)
			if len(batch) >= idx.batchSize {
				if err := flush(); err != nil {
					return total, err
				}
			}
		}
	}
}

// InsertBatch writes a batch of events and returns the count of rows inserted.
// This exported method allows callers (e.g. the stream command) that need
// manual checkpoint control between batches.
func (idx *Indexer) InsertBatch(batch []event.Event) (int64, error) {
	return idx.insertBatch(batch)
}

// BatchSize returns the configured batch size.
func (idx *Indexer) BatchSize() int {
	return idx.batchSize
}

// insertBatch writes a batch of events in a single multi-row INSERT.
// event_id and pk_hash are omitted — they are AUTO_INCREMENT and STORED
// generated respectively, so MySQL computes them on write.
func (idx *Indexer) insertBatch(batch []event.Event) (int64, error) {
	valClause := strings.TrimRight(strings.Repeat(rowPlaceholders+",", len(batch)), ",")
	insertSQL := `INSERT INTO binlog_events (` + insertColumnsSQL + `) VALUES ` + valClause

	// Sanitize each event's captured statement text, then resolve the batch's
	// DISTINCT texts to STATEMENT_DIGEST hashes in one round trip (#699).
	// Sanitizing first keeps the stored hash consistent with the stored text.
	sanitized := make([]string, len(batch))
	var distinct []string
	seen := make(map[string]struct{})
	for i := range batch {
		if batch[i].QueryText == "" {
			continue
		}
		sanitized[i] = event.SanitizeQueryText(batch[i].QueryText)
		if _, ok := seen[sanitized[i]]; !ok {
			seen[sanitized[i]] = struct{}{}
			distinct = append(distinct, sanitized[i])
		}
	}
	digests := idx.digestStatements(distinct)

	args := make([]any, 0, len(batch)*insertColumnCount)
	for i := range batch {
		ev := &batch[i]

		if err := checkPKValuesLength(ev.Schema, ev.Table, ev.PKValues); err != nil {
			return 0, err
		}

		changed, err := marshalJSON(event.ChangedColumns(ev.RowBefore, ev.RowAfter))
		if err != nil {
			return 0, fmt.Errorf("marshal changed_columns for %s.%s: %w", ev.Schema, ev.Table, err)
		}
		rowBefore, err := marshalRow(ev.RowBefore)
		if err != nil {
			return 0, fmt.Errorf("marshal row_before for %s.%s: %w", ev.Schema, ev.Table, err)
		}
		rowAfter, err := marshalRow(ev.RowAfter)
		if err != nil {
			return 0, fmt.Errorf("marshal row_after for %s.%s: %w", ev.Schema, ev.Table, err)
		}

		args = append(args,
			ev.BinlogFile,
			ev.StartPos,
			ev.EndPos,
			// FLOOR into event_timestamp's DATETIME(0) — never let MySQL round
			// (#1025). The default sql_mode rounds a bound sub-second fraction to
			// the NEAREST second, so an event committed at …06.885 was stored as
			// …07 (up to 0.5s AHEAD of its true time, approached as the fraction
			// nears .500) and `AS OF 'now'` (fractional) then evaluated
			// `07 <= …06.885` as false, omitting a row that already existed.
			// Flooring gives stored <= true event time < stored + 1s. The cost is
			// the mirror image, bounded by that same second: the event now also
			// answers `event_timestamp <= …06.500`, and a `Since` of …06.885 no
			// longer sees it. Rounding already did exactly that to every fraction
			// below .500 — flooring makes the skew uniform instead of half-and-half,
			// and the Since/Until bounds the query path actually receives (baseline
			// anchors, the keyset cursor, shim literals) are second-granular.
			// Correct ONLY while the column is DATETIME(0) (see schema.go):
			// widening it makes this line silent data loss. No-op for MySQL and
			// MariaDB, whose parsers build Timestamp from time.Unix(sec, 0); it is
			// the Postgres capturer's microsecond commit time that this bites.
			ev.Timestamp.Truncate(time.Second),
			nullOrString(ev.GTID),
			nullOrUint32(ev.ConnectionID),
			ev.Schema,
			ev.Table,
			uint8(ev.EventType),
			ev.PKValues,
			changed,
			rowBefore,
			rowAfter,
			ev.SchemaVersion,
			nullOrString(sanitized[i]),
			nullOrString(digests[sanitized[i]]),
			nullOrUint64(ev.CommitTsUS),
		)
	}

	// #959: bound the write with a deadline so a mid-statement network stall
	// surfaces as an error (ExecContext closes the connection on timeout) instead
	// of blocking on kernel TCP retransmission for minutes while `watch` sees a
	// healthy daemon capturing nothing.
	ctx, cancel := context.WithTimeout(context.Background(), WriteTimeout)
	defer cancel()
	result, err := idx.db.ExecContext(ctx, insertSQL, args...)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			// Distinguish a slow-but-working link (a batch too large to transmit
			// within WriteTimeout) from a genuine stall, so the operator knows the
			// knob rather than chasing a phantom network fault (#959).
			//
			// Tagged with ErrWriteDeadline (#1482) so a supervisor can tell this
			// apart from every other flush failure WITHOUT matching on message
			// text. The message itself is unchanged.
			return 0, &writeDeadlineError{fmt.Errorf("batch INSERT of %d events exceeded the %s write deadline "+
				"(a network stall, or a batch too large for the link — raise --write-timeout, "+
				"or lower --batch-size / the server max_allowed_packet): %w", len(batch), WriteTimeout, err)}
		}
		return 0, fmt.Errorf("batch INSERT of %d events failed: %w", len(batch), err)
	}
	n, _ := result.RowsAffected()
	return n, nil
}

// checkPKValuesLength guards against a PK value that would overflow
// binlog_events.pk_values (VARCHAR(512), event.MaxPKValuesLen). Without this
// guard: under strict sql_mode (MySQL 8.0 default) the batch INSERT
// hard-fails with an unhelpful Error 1406; under non-strict mode MySQL
// silently truncates pk_values while pk_hash (a STORED generated column) is
// computed over the truncated string and every read path binds the FULL
// value — a permanent silent mismatch that makes the row invisible to
// query/recover forever. This is not a new failure mode, just a clearer,
// sql_mode-independent version of the failure strict mode already produces.
//
// Measured in runes (event.MaxPKValuesLen's contract), matching how VARCHAR
// capacity is counted in characters, not bytes — a multibyte PK value must
// not false-trip this on byte length alone.
func checkPKValuesLength(schema, table, pkValues string) error {
	if n := utf8.RuneCountInString(pkValues); n > event.MaxPKValuesLen {
		return fmt.Errorf(
			"event for %s.%s has a primary key %d characters long, exceeding the %d-character limit of binlog_events.pk_values (VARCHAR(%d)); "+
				"this row's primary key (composite width or a wide single column) cannot be safely indexed — truncating it would silently corrupt "+
				"the generated pk_hash and make the row permanently unrecoverable via query/recover; narrow the primary key (fewer or shorter "+
				"columns) or contact support about a wider index schema",
			schema, table, n, event.MaxPKValuesLen, event.MaxPKValuesLen)
	}
	return nil
}

// ─── Query-text enrichment (#699) ─────────────────────────────────────────────

// digestStatements resolves distinct statement texts to their
// STATEMENT_DIGEST() hashes on the index connection (STATEMENT_DIGEST exists
// on MySQL 8.0+, the index contract floor). Texts ending in the truncation
// marker are skipped up front: a truncated fragment misrepresents the
// statement's true shape — NULL is the honest value — and it usually ends
// mid-token, failing to parse anyway (MySQL error 3676).
//
// The happy path is ONE combined SELECT over all texts. That SELECT fails as
// a unit when ANY single text is unparseable, so on error it falls back to
// per-text digests — one bad statement can then never null the whole batch's
// hashes. The digest is an enrichment: every failure degrades to a missing
// map entry (NULL query_hash) while query_text is still stored. Failures are
// surfaced without flooding: per-statement details at debug, plus one
// warning per Indexer for the first partial failure and one for the first
// all-failed batch; a missing STATEMENT_DIGEST function (1305) additionally
// disables digesting for the Indexer's lifetime.
func (idx *Indexer) digestStatements(texts []string) map[string]string {
	if idx.digestUnavailable {
		return nil
	}
	candidates := make([]string, 0, len(texts))
	for _, t := range texts {
		if !strings.HasSuffix(t, event.QueryTextTruncationMarker) {
			candidates = append(candidates, t)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	// #959: one deadline bounds the WHOLE digest phase (the combined query plus
	// any per-text fallback), not each round-trip independently — otherwise a
	// stall costs up to (K+1)×WriteTimeout before the batch INSERT even starts.
	// Digests are best-effort query_hash enrichment (#699): this phase returns
	// whatever it resolved and lets the bounded INSERT be the loud terminator.
	// Kept deliberately SEPARATE from the INSERT's own deadline so a slow-but-
	// healthy digest phase can never eat the durable write's budget.
	ctx, cancel := context.WithTimeout(context.Background(), WriteTimeout)
	defer cancel()

	out, combinedErr := idx.digestCombined(ctx, candidates)
	if combinedErr == nil {
		return out
	}
	// ER_SP_DOES_NOT_EXIST: the index simply has no STATEMENT_DIGEST function
	// — no per-text retry can succeed now or later. Warn once and stop trying.
	var myErr *mysql.MySQLError
	if errors.As(combinedErr, &myErr) && myErr.Number == 1305 {
		idx.digestUnavailable = true
		idx.digestWarnOnce.Do(func() {
			slog.Warn("STATEMENT_DIGEST is not available on the index connection — query_hash will be NULL for all events (query_text is unaffected)",
				"error", combinedErr)
		})
		return nil
	}
	slog.Debug("combined STATEMENT_DIGEST failed — falling back to per-text digests", "error", combinedErr)

	out = make(map[string]string, len(candidates))
	failures := 0
	var lastErr error
	for _, t := range candidates {
		var v sql.NullString
		// The shared phase ctx bounds every probe together (#959). On a genuine
		// stall the first probe exhausts the deadline and the rest would return
		// DeadlineExceeded instantly — stop probing and let the INSERT surface the
		// stall fatally rather than spinning through K no-op failures.
		err := idx.db.QueryRowContext(ctx, "SELECT STATEMENT_DIGEST(?)", t).Scan(&v)
		if err != nil {
			failures++
			lastErr = err
			slog.Debug("STATEMENT_DIGEST failed for one statement — query_hash stays NULL for it",
				"error", err, "statement_prefix", truncateForLog(t))
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			continue
		}
		if v.Valid {
			out[t] = v.String
		}
	}
	switch {
	case failures == len(candidates):
		idx.digestWarnOnce.Do(func() {
			slog.Warn("STATEMENT_DIGEST failed for every statement in a batch — query_hash will be NULL (query_text is unaffected)",
				"error", lastErr)
		})
	case failures > 0:
		// Partial failure: a systematic condition affecting one application's
		// statements would otherwise stay invisible until someone notices
		// NULL hashes in query results months later.
		idx.digestPartialWarnOnce.Do(func() {
			slog.Warn("STATEMENT_DIGEST failed for some statements — their query_hash stays NULL (per-statement details at debug level; this warning prints once)",
				"failed", failures, "of", len(candidates), "error", lastErr)
		})
	}
	return out
}

// truncateForLog bounds a statement text for a debug log line, cutting at a
// rune boundary (the input is sanitized UTF-8; keep it valid).
func truncateForLog(s string) string {
	const max = 120
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// digestCombined runs the single-round-trip form: one SELECT with one
// STATEMENT_DIGEST expression per text.
func (idx *Indexer) digestCombined(ctx context.Context, texts []string) (map[string]string, error) {
	var sb strings.Builder
	sb.WriteString("SELECT STATEMENT_DIGEST(?)")
	for range len(texts) - 1 {
		sb.WriteString(", STATEMENT_DIGEST(?)")
	}
	args := make([]any, len(texts))
	for i, t := range texts {
		args[i] = t
	}
	vals := make([]sql.NullString, len(texts))
	ptrs := make([]any, len(texts))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	// The caller's phase ctx bounds this round-trip together with any per-text
	// fallback (#959) — see digestStatements.
	if err := idx.db.QueryRowContext(ctx, sb.String(), args...).Scan(ptrs...); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(texts))
	for i, t := range texts {
		if vals[i].Valid {
			out[t] = vals[i].String
		}
	}
	return out, nil
}

// ─── Serialisation helpers ────────────────────────────────────────────────────

// marshalRow encodes a named row map to JSON, returning nil for a nil map.
// []byte values that contain a JSON object/array (e.g. from MySQL JSON
// columns) are embedded as raw JSON rather than base64-encoded.
func marshalRow(row map[string]any) ([]byte, error) {
	if row == nil {
		return nil, nil
	}
	// Promote valid-JSON []byte values to json.RawMessage so they are embedded
	// rather than base64-encoded in the output JSON. Gated on
	// looksLikeJSONContainer, not bare json.Valid: go-mysql delivers
	// TEXT/BLOB columns as []byte too (MySQL's binlog row format has no
	// separate wire type for JSON vs. TEXT/BLOB), so a plain TEXT value that
	// happens to be the literal string "false"/"true"/"null" or a bare
	// numeric string ("0", "123") is also valid JSON — json.Valid alone would
	// silently turn that string into a JSON bool/null/number, corrupting it
	// (#736). Restricting to object/array payloads mirrors
	// query.looksLikeJSONContainer, which guards the same ambiguity on the
	// baseline/Parquet read side.
	//
	// go-mysql itself delivers only BLOB/TEXT, JSON, and GEOMETRY columns as
	// []byte (VARCHAR/CHAR/VARBINARY/BINARY arrive as Go string from go-mysql;
	// ENUM/SET as int64) — but metadata.MapRow now reinterprets BINARY/
	// VARBINARY as []byte too (#756, before this function ever sees the row),
	// so by the time a row reaches marshalRow, []byte covers those five kinds.
	// They all go through the same looksLikeJSONContainer/json.Valid gate
	// above: a BINARY/VARBINARY value whose first non-whitespace byte happens
	// to be '{'/'[' and whose full content happens to be valid JSON would be
	// promoted too, same residual ambiguity as a TEXT/BLOB value that
	// genuinely looks like JSON (documented below) — astronomically unlikely
	// for real binary content, and not fixable from bytes alone.
	//
	// Residual, accepted ambiguity (not fixed here, same limit as the
	// baseline-side guard above): a plain TEXT/BLOB value whose content
	// genuinely LOOKS like a JSON object/array (e.g. literal text
	// `{"a":1}`) is still promoted, same as before. Resolving this fully
	// would require tagging each captured value with its real column type
	// at the point it's read (available via metadata.Resolver.MapRow, but
	// not carried through to here) rather than guessing from content —
	// a deeper fix, out of scope for #736's reported corruption class.
	normalized := make(map[string]any, len(row))
	for k, v := range row {
		if b, ok := v.([]byte); ok && looksLikeJSONContainer(b) && json.Valid(b) {
			normalized[k] = json.RawMessage(b)
		} else {
			normalized[k] = v
		}
	}
	return json.Marshal(normalized)
}

// looksLikeJSONContainer reports whether b's first non-whitespace byte is '{'
// or '[' — a cheap prefix test that distinguishes JSON object/array payloads
// from bare string/numeric/bool/null literals that json.Valid would also
// accept. Deliberate duplicate of query.looksLikeJSONContainer (same
// rationale, different package — not worth a shared dependency for one
// pure function).
func looksLikeJSONContainer(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}

// marshalJSON encodes v to JSON, returning nil if v is nil.
func marshalJSON(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}

// nullOrString returns nil when s is empty (stored as SQL NULL), else s.
func nullOrString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullOrUint32 returns nil when v is 0 (stored as SQL NULL), else v.
func nullOrUint32(v uint32) any {
	if v == 0 {
		return nil
	}
	return v
}

// nullOrUint64 mirrors nullOrUint32 for the microsecond commit timestamp: zero
// is the parser's "the source wrote none" value (MariaDB, pre-8.0.1 MySQL), and
// it must reach the column as NULL rather than as the epoch.
func nullOrUint64(v uint64) any {
	if v == 0 {
		return nil
	}
	return v
}

// EnsureSchema adds any columns introduced after the initial schema to
// binlog_events, schema_snapshots, and stream_state. It is idempotent — safe
// to call on every startup.
func EnsureSchema(db *sql.DB) error {
	if err := ensureColumn(db, "binlog_events", "connection_id",
		`ALTER TABLE binlog_events ADD COLUMN connection_id INT UNSIGNED DEFAULT NULL COMMENT 'MySQL connection ID (pseudo_thread_id) that produced this event' AFTER gtid`,
	); err != nil {
		return err
	}
	// column_type carries the full type declaration (e.g. "datetime(6)") so
	// full-table reconstruct (#187, #212) can tell the declared fractional
	// precision of DATETIME/TIMESTAMP PK columns, and so the shim can map
	// ENUM/SET ordinals back to labels (#472). Without this the PK
	// canonicalizer cannot distinguish DATETIME(0) from DATETIME(6) with
	// whole-second values.
	if err := ensureColumn(db, "schema_snapshots", "column_type",
		`ALTER TABLE schema_snapshots ADD COLUMN column_type TEXT DEFAULT NULL COMMENT 'full type from information_schema.COLUMNS.COLUMN_TYPE' AFTER data_type`,
	); err != nil {
		return err
	}
	// #212 created column_type as VARCHAR(128), which a realistic ENUM
	// declaration exceeds — and under strict mode the resulting 1406
	// ("Data too long") aborts the ENTIRE snapshot transaction, not just
	// one column. Widen pre-existing installs to TEXT (#472). Existing
	// values are preserved; the resolver already COALESCEs NULL to ''.
	if err := ensureColumnWidened(db, "schema_snapshots", "column_type", "text",
		`ALTER TABLE schema_snapshots MODIFY COLUMN column_type TEXT DEFAULT NULL COMMENT 'full type from information_schema.COLUMNS.COLUMN_TYPE'`,
	); err != nil {
		return err
	}
	// character_set_name carries information_schema.COLUMNS.CHARACTER_SET_NAME
	// (#756): NULL for BINARY/VARBINARY/numeric columns, populated for CHAR/
	// VARCHAR. go-mysql delivers those four types as a raw Go string with no
	// charset applied, and an invalid-UTF-8 value (a legacy latin1 table, a
	// binary UUID/digest) previously reached json.Marshal, which silently
	// replaces bad bytes with U+FFFD — at-rest data loss. Capturing the charset
	// lets MapRow transcode a latin1 CHAR/VARCHAR value to UTF-8 (MySQL's
	// "latin1" is actually cp1252) and fail loud instead of guess for any other
	// non-UTF-8 charset; a pre-#756 snapshot has this column NULL, so its
	// CHAR/VARCHAR columns keep failing loud on invalid UTF-8 until re-snapshotted.
	if err := ensureColumn(db, "schema_snapshots", "character_set_name",
		`ALTER TABLE schema_snapshots ADD COLUMN character_set_name VARCHAR(32) DEFAULT NULL COMMENT 'information_schema.COLUMNS.CHARACTER_SET_NAME; NULL for BINARY/VARBINARY/numeric columns; enables safe latin1-to-UTF8 transcoding of CHAR/VARCHAR values at index time instead of silent U+FFFD corruption (#756)' AFTER column_type`,
	); err != nil {
		return err
	}
	// pg_type_oid/pg_type_mod carry the PostgreSQL per-column type identity (pg_type
	// OID + atttypmod) from a pgoutput RelationMessage (#533). They are captured at
	// stream time because the offline recover path has no live PostgreSQL catalog to
	// rebuild them from. Nullable: MySQL snapshots leave them NULL (MySQL uses
	// data_type/column_type). WritePGSnapshot writes them; the type-faithful renderer
	// that reads them is a later #533 slice.
	if err := ensureColumn(db, "schema_snapshots", "pg_type_oid",
		`ALTER TABLE schema_snapshots ADD COLUMN pg_type_oid INT UNSIGNED DEFAULT NULL COMMENT 'PostgreSQL pg_type OID (pgoutput RelationMessage); NULL for MySQL snapshots (#533)' AFTER is_generated`,
	); err != nil {
		return err
	}
	if err := ensureColumn(db, "schema_snapshots", "pg_type_mod",
		`ALTER TABLE schema_snapshots ADD COLUMN pg_type_mod INT DEFAULT NULL COMMENT 'PostgreSQL atttypmod (pgoutput RelationMessage); NULL for MySQL snapshots (#533)' AFTER pg_type_oid`,
	); err != nil {
		return err
	}
	// is_identity_always marks a PostgreSQL GENERATED ALWAYS AS IDENTITY column (#557).
	// Recovery emits OVERRIDING SYSTEM VALUE on a reverse-INSERT and omits the column
	// from a reverse-UPDATE SET (PostgreSQL rejects SET on it). NOT NULL DEFAULT 0 so
	// existing rows + MySQL snapshots read back as "not identity" with no migration.
	if err := ensureColumn(db, "schema_snapshots", "is_identity_always",
		`ALTER TABLE schema_snapshots ADD COLUMN is_identity_always TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1 if PostgreSQL GENERATED ALWAYS AS IDENTITY; 0 for MySQL (#557)' AFTER pg_type_mod`,
	); err != nil {
		return err
	}
	// query_text/query_hash carry the original SQL statement that produced each
	// row event (#699): the text from the binlog's ROWS_QUERY/ANNOTATE_ROWS
	// event (opt-in on the source via binlog_rows_query_log_events /
	// binlog_annotate_row_events), and its STATEMENT_DIGEST computed on the
	// index connection at insert time. Nullable: rows indexed before this
	// column existed, or while capture is off at the source, read back NULL.
	// event.SanitizeQueryText caps every text at event.MaxQueryTextBytes
	// (16 KiB) before it reaches this column; MEDIUMTEXT (not TEXT) is
	// headroom on top of that cap, so raising the cap — or any future path
	// that bypasses sanitization — cannot turn into a strict-mode 1406 that
	// aborts a whole batch INSERT.
	if err := ensureColumn(db, "binlog_events", "query_text",
		`ALTER TABLE binlog_events ADD COLUMN query_text MEDIUMTEXT DEFAULT NULL COMMENT 'original SQL statement from ROWS_QUERY/ANNOTATE_ROWS; NULL unless binlog_rows_query_log_events (MySQL) / binlog_annotate_row_events (MariaDB) is ON at the source (#699)' AFTER schema_version`,
	); err != nil {
		return err
	}
	if err := ensureColumn(db, "binlog_events", "query_hash",
		`ALTER TABLE binlog_events ADD COLUMN query_hash CHAR(64) DEFAULT NULL COMMENT 'STATEMENT_DIGEST(query_text) computed on the index connection at index time; groups statements by normalized shape (#699)' AFTER query_text`,
	); err != nil {
		return err
	}
	// commit_ts_us carries the transaction's microsecond commit timestamp from
	// the GTID event (#18). Nullable and additive: rows indexed before this
	// column existed keep NULL, and so do rows from sources that never write
	// the value (MariaDB, MySQL < 8.0.1).
	if err := ensureColumn(db, "binlog_events", "commit_ts_us",
		`ALTER TABLE binlog_events ADD COLUMN commit_ts_us BIGINT UNSIGNED DEFAULT NULL COMMENT 'transaction commit time in microseconds since epoch, from the GTID event immediate_commit_timestamp (MySQL 8.0.1+, anonymous GTID events included); NULL on MariaDB and pre-8.0.1 MySQL' AFTER query_hash`,
	); err != nil {
		return err
	}
	if err := EnsureArchiveStateSchema(db); err != nil {
		return err
	}
	// gap_lost_at/_detail record an unfillable-gap auto-advance durably
	// (#402): the advanced checkpoint is persisted, so without these columns
	// the only trace of the permanently lost events would be an in-memory
	// flag that a daemon restart silently discards.
	if err := ensureColumn(db, "stream_state", "gap_lost_at",
		`ALTER TABLE stream_state ADD COLUMN gap_lost_at DATETIME DEFAULT NULL COMMENT 'when an unfillable binlog gap forced an auto-advance (events permanently lost); cleared only by an explicit monitor Stop; --reset re-stamps or preserves it' AFTER bintrail_id`,
	); err != nil {
		return err
	}
	if err := ensureColumn(db, "stream_state", "gap_lost_detail",
		`ALTER TABLE stream_state ADD COLUMN gap_lost_detail TEXT DEFAULT NULL COMMENT 'human-readable description of the lost gap' AFTER gap_lost_at`,
	); err != nil {
		return err
	}
	// source_health holds the latest source-side health snapshot a streaming
	// daemon polls (#599): for PostgreSQL, replication-slot wal_status/lag and
	// REPLICA IDENTITY coverage, with an embedded checked_at so the index-only
	// console can show staleness. Source-agnostic JSON payload (one index schema
	// for all source families); NULL on every index no daemon has polled.
	if err := ensureColumn(db, "stream_state", "source_health",
		`ALTER TABLE stream_state ADD COLUMN source_health JSON DEFAULT NULL COMMENT 'latest source-side health snapshot (PostgreSQL: replication-slot wal_status/lag + REPLICA IDENTITY coverage) with an embedded checked_at; serialized payload, source-agnostic column' AFTER gap_lost_detail`,
	); err != nil {
		return err
	}
	// capture_skips counts events the streaming daemon READ and chose to DROP
	// (#1034): per-reason monotonic counters ({"<reason>":{"count":N,
	// "last_at":"RFC3339"}}), persisted with every checkpoint so the DEGRADED
	// verdict survives daemon restarts and `status` can render Capture health.
	// "{}" is the affirmative evaluated-and-clean marker; NULL (legacy index,
	// or one no skip-aware daemon has written) means the verdict is unknown
	// and status omits the line rather than asserting OK from absent data —
	// the same never-a-false-ok philosophy as the gap_lost_* columns above.
	if err := ensureColumn(db, "stream_state", "capture_skips",
		`ALTER TABLE stream_state ADD COLUMN capture_skips JSON DEFAULT NULL COMMENT 'per-reason monotonic counters of events the daemon read and chose to drop (#1034): {"<reason>":{"count":N,"last_at":"RFC3339"}}; {} = evaluated and clean; NULL = no skip-aware daemon has written yet' AFTER source_health`,
	); err != nil {
		return err
	}
	// capture_skips_ack (#1314) is the operator's acknowledgement of the tally
	// above: {"<reason>":{"count":N,"at":"RFC3339"}}. It is a SEPARATE column
	// and not a key inside capture_skips because saveCheckpoint rewrites that
	// column from the daemon's in-memory tally on every checkpoint — anything
	// stored there would be gone within seconds. Nothing in the capture path
	// writes this one; only `status --ack-capture-skips` and the console's
	// acknowledge endpoint do. NULL = never acknowledged.
	if err := ensureColumn(db, "stream_state", "capture_skips_ack",
		`ALTER TABLE stream_state ADD COLUMN capture_skips_ack JSON DEFAULT NULL COMMENT 'operator acknowledgement of capture_skips (#1314): {"<reason>":{"count":N,"at":"RFC3339"}}; a reason is acknowledged while ack.count >= skips.count' AFTER capture_skips`,
	); err != nil {
		return err
	}
	// flavor records the source database flavor (mysql/mariadb) so a resume
	// parses the saved gtid_set with the correct GTID parser. NOT NULL DEFAULT
	// 'mysql' means existing rows read back as mysql with no data migration,
	// keeping every pre-MariaDB install unchanged.
	if err := ensureColumn(db, "stream_state", "flavor",
		`ALTER TABLE stream_state ADD COLUMN flavor VARCHAR(16) NOT NULL DEFAULT 'mysql' COMMENT 'source flavor: mysql or mariadb; selects the GTID parser on resume' AFTER gtid_set`,
	); err != nil {
		return err
	}
	// delete_rule/update_rule carry each FK's referential action so cascade
	// recovery can tell which edges are ON DELETE CASCADE at recovery time.
	// `recover` is source-less, so the rule must live in the index rather than
	// be re-queried from the source. NOT NULL DEFAULT '' means pre-existing
	// rows read back as "unknown" (treated as non-cascade) with no data
	// migration; a fresh snapshot of the schema populates the real rule.
	//
	// fk_constraints post-dates the original schema and may be absent on very
	// old indexes (TakeSnapshot tolerates its absence). Only migrate it when
	// present; `bintrail init` (the DDL) creates the table with these columns,
	// and a snapshot then populates the rule once the table exists. The block is
	// gated by `if hasFK` (rather than an early return) so any migration added
	// after it still runs on an index that predates fk_constraints.
	hasFK, err := tableExists(db, "fk_constraints")
	if err != nil {
		return err
	}
	if hasFK {
		if err := ensureColumn(db, "fk_constraints", "delete_rule",
			`ALTER TABLE fk_constraints ADD COLUMN delete_rule VARCHAR(16) NOT NULL DEFAULT '' COMMENT 'ON DELETE rule (CASCADE/RESTRICT/SET NULL/NO ACTION); empty for pre-cascade-recovery snapshots' AFTER referenced_column_name`,
		); err != nil {
			return err
		}
		if err := ensureColumn(db, "fk_constraints", "update_rule",
			`ALTER TABLE fk_constraints ADD COLUMN update_rule VARCHAR(16) NOT NULL DEFAULT '' COMMENT 'ON UPDATE rule; empty for pre-cascade-recovery snapshots' AFTER delete_rule`,
		); err != nil {
			return err
		}
	}
	// snapshot_id_seq post-dates the original schema (#844): a dedicated
	// AUTO_INCREMENT counter table that lets metadata.TakeSnapshot/
	// WritePGSnapshot allocate snapshot_id values without the deadlock-prone
	// `MAX(snapshot_id)+1 FOR UPDATE` pattern it replaces (metadata.
	// DDLSnapshotIDSeq has the full rationale). metadata.go's own callers
	// also self-heal this table lazily on first use — the standalone
	// `bintrail snapshot` command does not go through EnsureSchema — but
	// creating it eagerly here means the daemon-driven writers named in
	// #844 (the DDL hook, the console baseline trigger) never hit that lazy
	// path under load.
	hasSnapshotIDSeq, err := tableExists(db, "snapshot_id_seq")
	if err != nil {
		return err
	}
	if !hasSnapshotIDSeq {
		if _, err := db.Exec(metadata.DDLSnapshotIDSeq); err != nil {
			return fmt.Errorf("failed to create snapshot_id_seq: %w", err)
		}
	}
	// snapshot_exclusions post-dates the original schema (#1051): the explicit
	// record of tables a degraded DDL-hook snapshot excluded (no PK /
	// non-InnoDB), which the cascade FK loaders flag from. The writer
	// self-heals it lazily (metadata.ensureSnapshotExclusionsTable) and the
	// readers tolerate its absence; creating it eagerly here keeps daemon
	// startup the migration point, like snapshot_id_seq above.
	hasSnapshotExclusions, err := tableExists(db, "snapshot_exclusions")
	if err != nil {
		return err
	}
	if !hasSnapshotExclusions {
		if _, err := db.Exec(metadata.DDLSnapshotExclusions); err != nil {
			return fmt.Errorf("failed to create snapshot_exclusions: %w", err)
		}
	}
	return nil
}

// WrapSchemaMigrationErr rewrites an EnsureSchema failure for the READ plane
// (query, recover, verify, shim, reconstruct, recover-cascade,
// the MCP tools) into an actionable error.
//
// EnsureSchema issues ALTER TABLE / CREATE TABLE statements, which a
// SELECT-only DSN — the natural pairing with the RBAC --profile feature —
// lacks the privilege for. MySQL then returns errno 1142
// (ER_TABLEACCESS_DENIED_ERROR) or 1044 (ER_DBACCESS_DENIED_ERROR), whose raw
// message names neither the privilege cause nor the fix, so the read plane
// refuses to read data it could otherwise serve with an unhelpful error.
// Detect those two errors specifically and rewrite them to name both cause
// and fix; every other EnsureSchema failure (connection drop, context
// deadline, etc.) is not a privilege issue and passes through with a plain
// "schema migration" wrap so it isn't misdiagnosed.
//
// The CAPTURE plane (index, stream, agent, rotate) legitimately requires a
// privileged DSN and must keep failing hard on any EnsureSchema error — those
// call sites must NOT use this helper.
func WrapSchemaMigrationErr(err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1142 || mysqlErr.Number == 1044) {
		return fmt.Errorf("index schema is out of date and this user's DSN cannot migrate it (missing ALTER/CREATE privilege) — run any capture-plane command (index, stream, agent, or rotate) once with a privileged DSN to bring the schema up to date: %w", err)
	}
	return fmt.Errorf("schema migration: %w", err)
}

// tableExists reports whether a base table named `table` exists in the current
// database.
func tableExists(db *sql.DB, table string) (bool, error) {
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table).Scan(&n); err != nil {
		return false, fmt.Errorf("check table %s: %w", table, err)
	}
	return n > 0, nil
}

// ensureColumnWidened runs an idempotent ALTER TABLE MODIFY COLUMN: it
// checks the column's current information_schema DATA_TYPE and bails out
// when it already matches wantDataType (lowercase, e.g. "text"). A column
// that does not exist at all is also a no-op — ensureColumn owns creation;
// this helper only ever widens an existing one.
func ensureColumnWidened(db *sql.DB, table, column, wantDataType, alterSQL string) error {
	var dataType string
	err := db.QueryRow(`SELECT DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME   = ?
		  AND COLUMN_NAME  = ?`, table, column).Scan(&dataType)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check %s.%s type: %w", table, column, err)
	}
	if strings.EqualFold(dataType, wantDataType) {
		return nil
	}
	if _, err := db.Exec(alterSQL); err != nil {
		return fmt.Errorf("widen %s.%s to %s: %w", table, column, wantDataType, err)
	}
	return nil
}

// ensureColumn runs an idempotent ALTER TABLE ADD COLUMN: checks
// information_schema, bails out if the column already exists, and swallows
// the "duplicate column" error if a concurrent process added it between our
// check and the ALTER.
func ensureColumn(db *sql.DB, table, column, alterSQL string) error {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME   = ?
		  AND COLUMN_NAME  = ?`, table, column).Scan(&count)
	if err != nil {
		return fmt.Errorf("check %s.%s column: %w", table, column, err)
	}
	if count > 0 {
		return nil
	}
	if _, err := db.Exec(alterSQL); err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1060 {
			return nil
		}
		return fmt.Errorf("add %s.%s column: %w", table, column, err)
	}
	return nil
}

// InsertSchemaChange records a DDL detection in the schema_changes table.
// snapshotID may be nil when no auto-snapshot was taken (file mode).
func InsertSchemaChange(db *sql.DB, ev event.Event, snapshotID *int) error {
	var snapArg any
	if snapshotID != nil {
		snapArg = *snapshotID
	}
	// #959: bound the DDL write like the other hot-loop index writes.
	ctx, cancel := context.WithTimeout(context.Background(), WriteTimeout)
	defer cancel()
	_, err := db.ExecContext(ctx, `
		INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, gtid, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.Timestamp, ev.BinlogFile, ev.EndPos,
		nullOrString(ev.GTID), ev.Schema, ev.Table, ev.DDLType, ev.DDLQuery, snapArg)
	return err
}


// EnsureArchiveStateSchema adds the archive_state columns introduced after the
// initial schema. Idempotent, like EnsureSchema, which calls it.
//
// Split out so a caller that needs ONLY this table does not migrate
// binlog_events as a side effect (#1535). `archive reconcile` is that caller:
// its dry run is documented as a read-only cron drift monitor, and adding a
// column to the largest table in the deployment is not something such a run
// should ever start.
func EnsureArchiveStateSchema(db *sql.DB) error {
	// min_event_ts/max_event_ts record the content-derived event_timestamp
	// range of each archived partition (#1037). A backfill after a capture
	// stall lands old events in the OLDEST live RANGE partition, so the
	// Parquet file archived under that partition's hour label can hold rows
	// from much earlier hours; any pruning that trusts the label alone
	// (planner hour mapping, date-scoped S3 listings) then silently skips
	// them. The planner reads these columns to expand archive coverage and to
	// tell the archive fetcher which mislabeled files a time-scoped read must
	// still open. NULL on rows written before this column existed (or by
	// upload/reconcile, which do not scan row contents): the planner falls
	// back to label-only pruning for those rows, exactly the pre-#1037
	// behavior.
	if err := ensureColumn(db, "archive_state", "min_event_ts",
		`ALTER TABLE archive_state ADD COLUMN min_event_ts DATETIME DEFAULT NULL COMMENT 'MIN(event_timestamp) of the archived rows; NULL for archives written before #1037 or registered by upload/reconcile. Content-derived pruning: may precede the partition hour label when backfilled events landed in the partition' AFTER s3_uploaded_at`,
	); err != nil {
		return err
	}
	if err := ensureColumn(db, "archive_state", "max_event_ts",
		`ALTER TABLE archive_state ADD COLUMN max_event_ts DATETIME DEFAULT NULL COMMENT 'MAX(event_timestamp) of the archived rows; see min_event_ts (#1037)' AFTER min_event_ts`,
	); err != nil {
		return err
	}
	// column_set is the archived file's own column set (#1535): lowercase names,
	// sorted, comma-joined. It is a GROUPING KEY, not a description — the
	// DuckDB views generator emits one read_parquet per distinct set so the
	// bind opens one footer per SCHEMA instead of one per file, and
	// union_by_name (which is what makes the bind O(files)) is no longer
	// needed within a group.
	//
	// NULL on every row written before this column existed. Absent means
	// UNKNOWN, never "the same as the others": a partition with no recorded
	// set cannot join a group, and the generator falls back to the globbed
	// union_by_name leg rather than silently leaving it out of the view.
	// `archive reconcile --repair` records it from the footer, offline.
	if err := ensureColumn(db, "archive_state", "column_set",
		`ALTER TABLE archive_state ADD COLUMN column_set VARCHAR(4096) DEFAULT NULL COMMENT 'the archived Parquet file own column set: lowercase, sorted, comma-joined (#1535). NULL = unknown (written before this column, or registered without a footer read); archive reconcile --repair records it, needing --deep on an S3-only archive' AFTER max_event_ts`,
	); err != nil {
		return err
	}
	return nil
}
