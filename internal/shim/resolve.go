package shim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// This file holds the WIRE-NEUTRAL time-travel seam (#1008). The single-row
// _flashback / _snapshot resolve — fetch → ENUM/SET epoch map → ApplyAt fold —
// produces a plain `map[string]any` row image and a `[]string` column order.
// Neither depends on the MySQL wire protocol, so a second front-end (the
// PostgreSQL wire server in internal/pgshim) reuses these methods verbatim and
// inherits, for free, the ENUM/SET mapping (#472/#475), the residual-TOAST
// refusal (#592), the transaction-atomic cut (#988), and the #1007 PostgreSQL
// _snapshot baseline fold. Only the FINAL render (image → *mysql.Result) stays
// MySQL-typed, in handler.go; the pgwire front-end renders the same image to
// RowDescription/DataRow.
//
// Errors are returned in a wire-neutral shape too: a fetch / coverage failure
// comes back as a *ResolveError carrying a protocol-independent Class, so each
// front-end maps it to its own error code (MySQL 1526/1317/1105 here; PG
// SQLSTATE in pgshim) without re-classifying. A data-fault from ApplyAt or the
// baseline read (e.g. a residual TOAST marker) comes back RAW so it lands on the
// "internal fault" branch of every front-end.

// ResolveErrClass classifies a single-row resolve failure independent of the
// wire protocol. It mirrors the branches wrapFetchError has always drawn.
type ResolveErrClass int

const (
	// ResolveFault is an internal / server-side fault (index-DB outage,
	// archive S3 error, a resultset-build bug). MySQL → ER_UNKNOWN_ERROR
	// (1105); PG → SQLSTATE XX000 internal_error.
	ResolveFault ResolveErrClass = iota
	// ResolveGap is a coverage gap: the AS OF instant falls in a
	// rotated-but-unarchived hole the index no longer retains. MySQL →
	// ER_NO_PARTITION_FOR_GIVEN_VALUE (1526); PG → SQLSTATE 22023.
	ResolveGap
	// ResolveTimeout is a per-query deadline (QueryTimeout) expiry. MySQL →
	// ER_QUERY_INTERRUPTED (1317); PG → SQLSTATE 57014 query_canceled.
	ResolveTimeout
	// ResolveCanceled is a client-disconnect / shutdown cancel. MySQL →
	// ER_QUERY_INTERRUPTED (1317); PG → SQLSTATE 57014.
	ResolveCanceled
)

// ResolveError wraps a classified single-row resolve failure so both wire
// front-ends map it to their own protocol code without re-running the
// gap/deadline/cancel classification. Unwrap exposes the underlying error for
// errors.Is/As.
type ResolveError struct {
	Class ResolveErrClass
	QType QueryType
	Err   error
}

func (e *ResolveError) Error() string { return fmt.Sprintf("resolve %s: %s", e.QType, e.Err) }
func (e *ResolveError) Unwrap() error { return e.Err }

// classifyFetchError reproduces wrapFetchError's classification but returns a
// wire-neutral *ResolveError instead of a *mysql.MyError, so the pgwire
// front-end reuses it. A coverage gap wins first (it is a client-input concern
// distinct from an internal failure); then, if the query context is already
// dead, its error takes over regardless of what the driver surfaced (go-mysql
// wraps ctx.Err() but sqlmock and the DuckDB archive path return their own
// cancellation sentinels, and the wire code must reflect WHY the query died, not
// which driver noticed first). The discarded underlying error is logged (Warn)
// before it is overwritten so an operator can still find the real cause; nil
// logger is a no-op.
func classifyFetchError(ctx context.Context, qType QueryType, err error, logger *slog.Logger) *ResolveError {
	var gapErr *query.GapError
	if errors.As(err, &gapErr) {
		return &ResolveError{Class: ResolveGap, QType: qType, Err: gapErr}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if logger != nil && !errors.Is(err, ctxErr) {
			logger.Warn("shim: query context ended before fetch returned; discarding underlying error from the wire response",
				"query_type", qType, "ctx_err", ctxErr, "underlying_err", err)
		}
		err = ctxErr
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &ResolveError{Class: ResolveTimeout, QType: qType, Err: err}
	}
	if errors.Is(err, context.Canceled) {
		return &ResolveError{Class: ResolveCanceled, QType: qType, Err: err}
	}
	return &ResolveError{Class: ResolveFault, QType: qType, Err: err}
}

// mysqlResolveError maps a classified *ResolveError to the MySQL wire error the
// client sees. The messages are byte-identical to the pre-#1008 wrapFetchError
// so no MySQL behaviour changes.
func mysqlResolveError(rerr *ResolveError) error {
	switch rerr.Class {
	case ResolveGap:
		// %s (not .Error()) so a would-be &ResolveError{Class: ResolveGap} with a
		// nil Err formats safely instead of panicking — the PG renderer's Gap
		// branch is already nil-robust this way. Byte-identical for the reachable
		// case (classifyFetchError always sets a non-nil Err).
		return mysql.NewError(mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE,
			fmt.Sprintf("resolve %s: %s", rerr.QType, rerr.Err))
	case ResolveTimeout:
		return mysql.NewError(mysql.ER_QUERY_INTERRUPTED, fmt.Sprintf(
			"resolve %s: query exceeded the shim's --query-timeout and was aborted; narrow the AS OF range, filter by PK, or raise --query-timeout", rerr.QType))
	case ResolveCanceled:
		return mysql.NewError(mysql.ER_QUERY_INTERRUPTED, fmt.Sprintf(
			"resolve %s: query canceled (client disconnected or shim shutting down)", rerr.QType))
	default:
		return fmt.Errorf("resolve %s: %w", rerr.QType, rerr.Err)
	}
}

// mysqlRenderErr maps a resolve error to the MySQL wire: a *ResolveError goes
// through mysqlResolveError; a raw data-fault (ApplyAt / baseline read) passes
// through unchanged so go-mysql/server emits ER_UNKNOWN_ERROR (1105) with the
// original message — exactly what the pre-#1008 single-row paths did. The one
// exception is a binlog-renumbering refusal (#2174), which gets the code of
// the other unreadable-history refusals (renumberedRefusal).
func mysqlRenderErr(err error) error {
	var rerr *ResolveError
	if errors.As(err, &rerr) {
		return mysqlResolveError(rerr)
	}
	return renumberedRefusal(err)
}

// ResolveFlashbackRow resolves a single-row _flashback query (binlog-only) to
// its row image at q.AsOf, independent of the wire protocol. A nil map means the
// row did not exist at AsOf (never created, or its latest surviving event was a
// DELETE) — the caller renders a zero-row resultset. q MUST be a single-row
// query (q.PKColumn != ""); routing the full-table shape (PKColumn == "") is the
// caller's responsibility. Errors: a fetch/coverage failure is a *ResolveError;
// an ApplyAt data-fault (residual TOAST marker, #592) is raw.
//
// The MySQL renderer (runPointInTime) and the pgwire renderer share this method,
// so the transaction-atomic cut (#988), the raw-vs-encoded PK routing (#826),
// and the ENUM/SET epoch mapping (#472/#475) live in exactly one place.
func (h *Handler) ResolveFlashbackRow(ctx context.Context, q TimeTravelQuery) (map[string]any, error) {
	opts := query.Options{
		Schema: q.Schema,
		Table:  q.Table,
		Until:  &q.AsOf,
	}
	// q.PKValue may legitimately be "" (`WHERE name = ''` against a NOT-NULL
	// string PK). buildQuery DISABLES the PK filter when Options.PKValues == "",
	// which would fold every event for the WHOLE TABLE onto one state — a silent
	// wrong answer. Route "" through PKValuesIn (`pk_values IN (?)`), which keeps
	// the fetch scoped to the one row; keep the pk_hash fast path otherwise.
	// q.PKValue is raw/unescaped (#826); pk_values is event.BuildPKValues-
	// encoded, so re-encode it (eventPKValue).
	encoded := h.eventPKValue(q)
	if q.PKValue != "" {
		opts.PKValues = encoded
	} else {
		opts.PKValuesIn = []string{encoded}
	}
	engine := query.New(h.indexDB)
	rows, _, err := reconstruct.FetchEventsAtomic(ctx, h.indexDB, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         h.cfg.IndexDBName,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	}, q.AsOf)
	if err != nil {
		return nil, classifyFetchError(ctx, q.Type, err, h.logger)
	}

	// ENUM/SET ordinals → labels (#472/#475) BEFORE the fold: ApplyAt replaces
	// the image wholesale per event, so pre-mapped events make the final state
	// carry labels the way the live table did when the event happened.
	h.mapEventImages(q.Schema, q.Table, rows)
	image, err := reconstruct.ApplyAt(nil, rows, q.AsOf)
	if err != nil {
		// Residual unchanged-TOAST marker (#592) — a capture-invariant
		// violation, i.e. a server-side data fault. Raw error → the caller's
		// "internal fault" branch. Refusing beats serving the marker's JSON.
		return nil, err
	}
	if len(image) == 0 {
		// Row never existed at AsOf (no event in the window), or its latest
		// surviving event was a DELETE. A non-DELETE tail that folds to empty is
		// a corrupt/partial row image — surfaced as a Warn, not silently.
		h.warnCorruptImageDrop(q.Schema, q.Table, rows)
		return nil, nil
	}
	return image, nil
}

// ResolveSnapshotRow resolves a single-row _snapshot query (baseline-aware) to
// its row image at q.AsOf, independent of the wire protocol. With no usable
// baseline source it degrades to the binlog-only ResolveFlashbackRow — the same
// fallback the MySQL path drew. A nil map means the row did not exist at AsOf.
// Errors follow the same neutral shape as ResolveFlashbackRow.
//
// This carries the #1007 PostgreSQL fix (the flavor bypass at
// baselinePKStringMatchable): a PG source's empty DATA_TYPE token would
// otherwise silently degrade every PG _snapshot to binlog-only. Because the fix
// lives here, the pgwire front-end inherits it for free — a second renderer must
// NOT re-derive it.
func (h *Handler) ResolveSnapshotRow(ctx context.Context, q TimeTravelQuery) (map[string]any, error) {
	src := h.baselineSource()
	if src == "" {
		h.logger.Debug("shim: _snapshot has no baseline source configured; using binlog-only path",
			"schema", q.Schema, "table", q.Table)
		return h.ResolveFlashbackRow(ctx, q)
	}

	// Guard the PK type before attempting a baseline match. ReadBaselineRow
	// binds q.PKValue as a string against the typed Parquet column and relies on
	// DuckDB's implicit coercion; only types whose Parquet representation coerces
	// cleanly from a string literal are safe here (see baselinePKStringMatchable),
	// so for the rest we fall back to binlog-only with a signal.
	dataType, ok := h.pkDataType(q.Schema, q.Table, q.PKColumn)
	if !ok {
		h.logger.Debug("shim: _snapshot cannot resolve PK type; using binlog-only path",
			"schema", q.Schema, "table", q.Table, "pk_column", q.PKColumn)
		return h.ResolveFlashbackRow(ctx, q)
	}
	// PostgreSQL baselines store every column as raw pgoutput text (#593), so
	// ReadBaselineRow's string-bound match is a string-identity join that can
	// only recover the right row or find nothing. The MySQL DATA_TYPE token is
	// empty for a PG source (PG records pg_type_oid), so without this flavor
	// bypass every PG _snapshot would silently degrade to binlog-only and the
	// baseline fold the docs promise would never run (#1006/#1007). The flavor
	// read is short-circuited so the MySQL hot path never pays for it.
	if !baselinePKStringMatchable(dataType) {
		if flavor := query.SourceFlavor(h.indexDB); flavor != "postgres" {
			if dataType == "" {
				h.logger.Warn("shim: _snapshot could not confirm a postgres source flavor (stream_state read empty or failed); "+
					"baseline fold skipped, using binlog-only path — a row untouched within the binlog window will read as absent",
					"schema", q.Schema, "table", q.Table, "pk_column", q.PKColumn, "flavor", flavor)
			} else {
				h.logger.Warn("shim: _snapshot PK type not safe for baseline lookup; using binlog-only path",
					"schema", q.Schema, "table", q.Table, "pk_column", q.PKColumn, "pk_type", dataType)
			}
			return h.ResolveFlashbackRow(ctx, q)
		}
	}

	// Stale-fallback warning (#466) discarded here — see the full-table path.
	baselinePath, snapshotTime, _, err := reconstruct.FindBaseline(ctx, src, q.Schema, q.Table, q.AsOf)
	if err != nil {
		if errors.Is(err, reconstruct.ErrNoBaseline) {
			h.logger.Debug("shim: _snapshot found no baseline at-or-before AsOf; using binlog-only path",
				"schema", q.Schema, "table", q.Table)
			return h.ResolveFlashbackRow(ctx, q)
		}
		// A real baseline-source failure (unreadable dir, S3 outage) is a
		// server-side fault: raw error → the caller's "internal fault" branch.
		return nil, err
	}

	// Refuse if a TRUNCATE/DROP/RENAME hit this table in the window: same blind
	// spot as the full-table path — no row events to invalidate the baseline
	// image, so the row would silently resolve as if it still existed (#764).
	// The baseline's position is read here, ahead of the fetch that also
	// takes it: the check places a statement indexed late by it (#1912).
	sincePos, ddlMark, eventMark := snapshotAnchor(ctx, baselinePath, h.logger, q.Schema, q.Table)
	if err := reconstruct.CheckDestructiveDDL(ctx, h.indexDB, q.Schema, q.Table,
		reconstruct.DDLWindow{Since: snapshotTime, Until: q.AsOf, Anchor: sincePos, Mark: ddlMark}); err != nil {
		return nil, err
	}
	if err := h.checkSnapshotNumbering(ctx, q, snapshotTime, sincePos, eventMark); err != nil {
		return nil, err
	}

	// q.PKValue is passed RAW here — Parquet baseline rows store actual column
	// values, not event.BuildPKValues-encoded pk_values, so this seam must NOT
	// apply event.EscapePKValue (unlike the delta fetch below). The one PK meta
	// passed is for a MariaDB UUID/INET key: ReadBaselineRow re-spells the
	// value as the text the baseline holds (the client may write another
	// spelling MariaDB accepts). Binary-family PKs never reach this call
	// (baselinePKStringMatchable routed them to the binlog-only path above),
	// so the fixed BINARY(n) pad-and-retry (#1157) has nothing to reconcile.
	pkMetas := []metadata.ColumnMeta{{Name: q.PKColumn, DataType: dataType, IsPK: true}}
	baselineRow, err := reconstruct.ReadBaselineRow(ctx, baselinePath, map[string]string{q.PKColumn: q.PKValue}, pkMetas)
	if err != nil {
		return nil, err
	}

	opts := query.Options{
		Schema:   q.Schema,
		Table:    q.Table,
		Since:    &snapshotTime,
		SincePos: sincePos,
		Until:    &q.AsOf,
	}
	// Delta fetch matches binlog_events.pk_values (BuildPKValues-encoded), so
	// re-encode with EscapePKValue — the mirror of the raw ReadBaselineRow match
	// above (#826). The empty-string PK routes through PKValuesIn for the same
	// reason as ResolveFlashbackRow.
	encoded := eventPKValueTyped(q, dataType, true)
	if q.PKValue != "" {
		opts.PKValues = encoded
	} else {
		opts.PKValuesIn = []string{encoded}
	}
	engine := query.New(h.indexDB)
	rows, _, err := reconstruct.FetchEventsAtomic(ctx, h.indexDB, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         h.cfg.IndexDBName,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	}, q.AsOf)
	if err != nil {
		return nil, classifyFetchError(ctx, q.Type, err, h.logger)
	}

	h.mapEventImages(q.Schema, q.Table, rows)
	state, err := reconstruct.ApplyAt(baselineRow, rows, q.AsOf)
	if err != nil {
		return nil, err
	}
	if len(state) == 0 {
		h.warnCorruptImageDrop(q.Schema, q.Table, rows)
		return nil, nil
	}
	return state, nil
}

// ColumnsFor returns the wire column list for a single-row time-travel result,
// independent of the wire protocol. An explicit projection (q.Columns) is used
// verbatim; otherwise the table's newest-snapshot DDL order, with any image-only
// keys (e.g. a column dropped since AsOf, #600) appended. A nil/empty image (row
// absent at AsOf) yields the snapshot order alone, so a zero-row resultset still
// carries the real column names.
func (h *Handler) ColumnsFor(image map[string]any, q TimeTravelQuery) []string {
	if q.Columns != nil {
		return q.Columns
	}
	ddl := h.columnOrderFor(q.Schema, q.Table)
	if len(image) == 0 {
		return ddl
	}
	return orderColumns(image, ddl)
}

// PKColumnCheck reports whether q's WHERE column is a safe single-column PK
// match, independent of the wire protocol (#296/#821). reject == true means the
// query must be refused and msg carries the reason; each front-end wraps msg in
// its own syntax-error code (MySQL ER_PARSE_ERROR 1064; PG SQLSTATE 42601). The
// full-table shape (PKColumn == "") and a nil resolver are permissive here, as
// they were in the original validatePKColumn.
func (h *Handler) PKColumnCheck(q TimeTravelQuery) (msg string, reject bool) {
	if q.PKColumn == "" {
		return "", false
	}
	if h.resolverFn == nil {
		return "", false
	}
	r, err := h.resolverCache.get(time.Now, resolverCacheTTL, h.resolverFn, h.logger)
	if err != nil {
		// Cannot confirm q.PKColumn is the table's PK. A column-qualified WHERE
		// is guaranteed here (the full-table shape returned above), so fail loud
		// rather than join the literal against pk_values and risk the wrong row
		// (#821). ErrNoSnapshots (benign first-install state) still can't verify
		// the PK, so it rejects too — only the no-WHERE full-table path is exempt.
		reason := "schema_snapshots lookup failed"
		if errors.Is(err, metadata.ErrNoSnapshots) {
			reason = "no schema snapshots available yet"
		}
		h.logger.Warn("shim: cannot verify PK column; rejecting column-qualified WHERE",
			"err", err, "reason", reason, "schema", q.Schema, "table", q.Table, "pk_column", q.PKColumn)
		return fmt.Sprintf(
			"cannot verify WHERE column %s is the primary key of %s.%s (%s); refusing to run so a non-PK WHERE cannot silently return the wrong row",
			q.PKColumn, q.Schema, q.Table, reason,
		), true
	}
	tm, err := r.Resolve(q.Schema, q.Table)
	if err != nil {
		h.logger.Warn("shim: table not in any snapshot; rejecting column-qualified WHERE",
			"err", err, "schema", q.Schema, "table", q.Table, "pk_column", q.PKColumn)
		return fmt.Sprintf(
			"cannot verify WHERE column %s is the primary key of %s.%s (table not present in any indexed snapshot); refusing to run so a non-PK WHERE cannot silently return the wrong row",
			q.PKColumn, q.Schema, q.Table,
		), true
	}
	if len(tm.PKColumns) == 0 {
		return fmt.Sprintf(
			"%s.%s has no primary key declared in the indexed snapshot; cannot resolve %s by PK",
			q.Schema, q.Table, q.Type,
		), true
	}
	if len(tm.PKColumns) > 1 {
		return fmt.Sprintf(
			"%s.%s has a composite primary key (%s); WHERE %s = <value> shape supports only single-column PKs",
			q.Schema, q.Table, strings.Join(tm.PKColumns, ", "), q.PKColumn,
		), true
	}
	if q.PKColumn != tm.PKColumns[0] {
		return fmt.Sprintf(
			"WHERE column must be the primary key of %s.%s (expected %s, got %s)",
			q.Schema, q.Table, tm.PKColumns[0], q.PKColumn,
		), true
	}
	return "", false
}

// eventPKValue spells q.PKValue the way binlog_events.pk_values stores it.
// For most keys that is event.EscapePKValue of the raw value (#826). A MariaDB
// UUID/INET4/INET6 key is stored as its captured bytes (metadata.MapRow pads
// them to full width, #1944), while the client writes text, in any spelling
// MariaDB accepts; that text is parsed to the bytes and spelled by
// event.BuildPKValues. Text that is not a value of the type is left as typed,
// so it matches nothing, as it would on the server.
func (h *Handler) eventPKValue(q TimeTravelQuery) string {
	dt, ok := h.pkDataType(q.Schema, q.Table, q.PKColumn)
	return eventPKValueTyped(q, dt, ok)
}

// eventPKValueTyped is eventPKValue with the PK type already resolved, so a
// caller that gated on the type spells the key by that same answer.
func eventPKValueTyped(q TimeTravelQuery, dt string, ok bool) string {
	if ok && metadata.MariaDBFixedWidth(dt) > 0 {
		if b, err := metadata.ParseMariaDBFixed(dt, q.PKValue); err == nil {
			col := metadata.ColumnMeta{Name: q.PKColumn, DataType: dt, IsPK: true}
			return event.BuildPKValues([]metadata.ColumnMeta{col}, map[string]any{q.PKColumn: b})
		}
	}
	return event.EscapePKValue(q.PKValue)
}
