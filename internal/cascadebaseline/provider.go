// Package cascadebaseline implements cascade.BaselineProvider over
// internal/reconstruct — the Phase-2 fallback that lets cascade recovery
// recover a child row that has no binlog event in the lookback window by
// reading it out of a `bintrail baseline` Parquet snapshot.
//
// It is a LEAF package on purpose: the CLI (internal/cli) and the console
// (internal/console) each used to carry a private near-identical copy of this
// provider, and the copies drifted (#1101, #1102). Hosting the single
// implementation here — rather than having the console import internal/cli, or
// folding it into internal/cascade — keeps the console binary free of the whole
// CLI command layer and keeps the pure cascade engine free of the DuckDB/S3
// dependencies that baseline reads pull in.
package cascadebaseline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// FindBaselineFunc locates the baseline snapshot covering schema.table at-or-before
// at, returning its path, the snapshot's timestamp and any stale-fallback warning
// — the shape of reconstruct.FindBaseline with the source already bound.
//
// It is injected rather than called directly so each surface composes with its
// own baseline-resolution policy: the CLI binds a single --baseline-dir/--baseline-s3
// source (see Source), while the console passes its bundle's findBaseline, which
// retries the durable S3 copy when the local dir has no baseline for the table
// (#766). Before this was injectable the console's copy called
// reconstruct.FindBaseline directly and silently lost that fallback (#1102).
type FindBaselineFunc func(ctx context.Context, schema, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error)

// Source binds a single baseline source (a local directory or an s3:// prefix)
// to reconstruct.FindBaseline — the no-fallback lookup the CLI uses.
func Source(src string) FindBaselineFunc {
	return func(ctx context.Context, schema, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
		return reconstruct.FindBaseline(ctx, src, schema, table, at)
	}
}

// Provider implements cascade.BaselineProvider: it finds the child table's
// baseline snapshot, scans it for rows referencing the deleted parent, and
// encodes each row's PK to match binlog_events.pk_values so the cascade engine
// can dedup against Phase-1.
type Provider struct {
	find     FindBaselineFunc
	resolver *metadata.Resolver // for child PK columns
	// db is the index the cascade engine reads changes from: the baseline's
	// event mark is checked against it (#2177).
	db *sql.DB
}

// renumberNotices logs each lasting condition of the numbering check once
// per process: the console and the MCP server run cascade recoveries for as
// long as they are up.
var renumberNotices reconstruct.NoticeOnce

// New builds a Provider from a baseline lookup, the schema resolver used to
// encode each baseline row's PK, and the index the cascade engine reads
// changes from (db; a baseline with an event mark is refused without it). It never returns nil: callers assign the result
// to a cascade.BaselineProvider interface variable and test that variable for
// nil to decide whether Phase-2 ran, so a typed-nil would report an active
// baseline that does not exist.
func New(find FindBaselineFunc, resolver *metadata.Resolver, db *sql.DB) *Provider {
	return &Provider{find: find, resolver: resolver, db: db}
}

// BaselineChildren implements cascade.BaselineProvider.
func (p *Provider) BaselineChildren(ctx context.Context, schema, table, fkCol, parentPK string, at time.Time, limit int) (cascade.BaselineLookup, bool, error) {
	path, snap, stale, err := p.find(ctx, schema, table, at)
	if err != nil {
		if errors.Is(err, reconstruct.ErrNoBaseline) {
			return cascade.BaselineLookup{}, false, nil // table not covered → Phase-1 only
		}
		return cascade.BaselineLookup{}, false, err
	}
	tm, err := p.resolver.Resolve(schema, table)
	if err != nil {
		return cascade.BaselineLookup{}, false, fmt.Errorf("resolve %s.%s for baseline: %w", schema, table, err)
	}
	// A generated PK member — the MariaDB system-versioning shape (#1266) —
	// cannot canonicalize against a baseline that omits the column; without
	// this gate every row below dies with MissingPKColumnError and its
	// misleading "run `bintrail snapshot` to refresh" remediation, which no
	// re-snapshot can ever satisfy. Fail loud with the real cause instead,
	// same stance as the fkFilterSafe refusal below.
	if c, found := reconstruct.GeneratedPKColumn(tm.PKColumnMetas()); found {
		// Classified as reconstruct.ErrGeneratedPK (#1273) so the cascade
		// engine's caveat classifier files this under the permanent
		// `generatedpk:` caveat — and skips Phase-1 for the edge too — instead
		// of the transient-sounding `baselinefail:` bucket. Built via
		// GeneratedPKRefusalError, not %w: the sentinel's own text would
		// stutter against the gate reason's opening clause.
		return cascade.BaselineLookup{}, false, reconstruct.GeneratedPKRefusalError(fmt.Sprintf(
			"baseline scan of %s.%s: %s",
			schema, table, reconstruct.GeneratedPKGateReason(c, "the cascade baseline fallback")))
	}
	// A PK type the baseline canonicalizer cannot handle (FLOAT/DOUBLE, TIME,
	// BIT, JSON, the spatial family) is the OTHER permanent refusal, and #1460 is
	// what happened without this gate: CanonicalizePKMap below refused each
	// row with a plain error, which the cascade engine files under its
	// transient `baselinefail:` bucket as "baseline lookup failed ...
	// (recovery may be partial)". That reads as worth retrying when it never
	// will succeed, and the engine re-ran this whole lookup — FindBaseline,
	// the Parquet metadata read, ReadBaselineRows — once per parent key on
	// the edge, all to fail identically.
	//
	// Classified as reconstruct.ErrUnsupportedPKType so the engine files it as
	// permanent and memoizes the child table. Unlike the generated-PK refusal
	// above, this one does NOT make the edge's binlog candidates unsafe, so
	// the engine keeps running Phase-1 for it; see cascade.BaselineProvider's
	// contract for that asymmetry.
	//
	// FirstUnsupportedPKType skips an EMPTY DataType on purpose (the
	// PostgreSQL snapshot signature, #533). Nothing on the cascade path reads
	// the recorded source flavor, so the wrong-path verdict PKTypeGateReason
	// renders for an empty type would name a cause this code cannot know. A
	// PG-shaped snapshot does not reach the per-row canonicalizer from here
	// anyway: fkFilterSafe below rejects the same empty type token on the FK
	// column first.
	if c, found := reconstruct.FirstUnsupportedPKType(tm.PKColumnMetas()); found {
		return cascade.BaselineLookup{}, false, reconstruct.PKTypeRefusalError(
			fmt.Sprintf("baseline scan of %s.%s", schema, table),
			reconstruct.PKTypeGateReason(c, "the cascade baseline fallback", "read"))
	}
	// The baseline's exact recorded binlog position, when it has one (#797) —
	// see BaselineLookup.SincePos. Best-effort: a read failure just leaves the
	// candidate-victim fetch anchored on SnapshotTime alone, same as before
	// #797 — it must not block the (already-succeeded) baseline row scan below.
	//
	// Below BOTH permanent-refusal gates on purpose. This read only warns, and
	// a table about to be refused for a permanent reason should not first
	// emit a warning about an anchor no lookup will use: the operator would
	// chase the wrong problem. Nothing depends on it running earlier.
	var sincePos *query.BinlogPos
	var eventMark string
	if bmeta, berr := baseline.ReadParquetMetadataAny(ctx, path); berr != nil {
		slog.Warn("cascade: could not read baseline metadata for position-anchored victim fetch; falling back to timestamp-only Since",
			"schema", schema, "table", table, "path", path, "error", berr)
	} else {
		eventMark = bmeta.EventMark
		if bmeta.BinlogFile != "" && bmeta.BinlogPos > 0 {
			sincePos = &query.BinlogPos{File: bmeta.BinlogFile, Pos: uint64(bmeta.BinlogPos)}
		}
	}

	// The FK filter binds parentPK as a STRING against the baseline column.
	// DuckDB coerces it exactly for integer/string FK columns, but for
	// DATETIME/DECIMAL/DATE the string form may not match the stored value and
	// would silently zero-match. Refuse those (flagged as a coverage gap) rather
	// than under-recover silently.
	if !fkFilterSafe(columnDataType(tm, fkCol)) {
		return cascade.BaselineLookup{}, false, fmt.Errorf(
			"baseline scan of %s.%s by FK column %q (type %q) is unsupported (string match may not coerce); baseline augmentation skipped",
			schema, table, fkCol, columnDataType(tm, fkCol))
	}

	// The engine reads this table's changes in [snapshot, at] from the
	// snapshot's position, and treats a baseline row with no later change as
	// untouched. When the source's binary log started again after the
	// snapshot, every later change sorts before that position and is not
	// read: the SQL would restore children as the snapshot left them (#2177).
	// The same check verify and _snapshot run (#2174), over this read's
	// window. Before the row scan, and so also when no row matches: a covered
	// lookup with zero rows still widens the engine's scan to the snapshot.
	// A snapshot without an event mark: no check, today's behavior.
	unchecked, err := p.checkNumbering(ctx, schema, table, sincePos, eventMark, snap, at)
	if err != nil {
		return cascade.BaselineLookup{}, false, err
	}

	// Fetch one more than the cap so truncation is observable.
	fetch := 0
	if limit > 0 {
		fetch = limit + 1
	}
	rows, err := reconstruct.ReadBaselineRows(ctx, path, map[string]string{fkCol: parentPK}, fetch)
	if err != nil {
		return cascade.BaselineLookup{}, false, err
	}
	trunc := false
	if limit > 0 && len(rows) > limit {
		trunc = true
		rows = rows[:limit]
	}

	pkCols := tm.PKColumnMetas()
	out := make([]cascade.BaselineRow, 0, len(rows))
	for _, r := range rows {
		// Canonicalize PK values the same way the indexer encoded pk_values, so
		// the dedup key matches a Phase-1 victim's PKValues exactly.
		canon, cerr := reconstruct.CanonicalizePKMap(r, pkCols)
		if cerr != nil {
			return cascade.BaselineLookup{}, false, fmt.Errorf("canonicalize baseline PK for %s.%s: %w", schema, table, cerr)
		}
		out = append(out, cascade.BaselineRow{
			PKValues: event.BuildPKValues(pkCols, canon),
			Row:      r,
		})
	}
	return cascade.BaselineLookup{SnapshotTime: snap, Rows: out, Truncated: trunc, SincePos: sincePos, StaleMessage: stale.Message,
		UncheckedMessage: unchecked}, true, nil
}

func columnDataType(tm *metadata.TableMeta, name string) string {
	for _, c := range tm.Columns {
		if c.Name == name {
			return c.DataType
		}
	}
	return ""
}

// fkFilterSafe reports whether a string-bound equality filter on a column of
// this DATA_TYPE coerces exactly in DuckDB (integer + string families). Types
// where the string form may diverge from the stored value (datetime, decimal,
// date, …) are excluded so the baseline FK scan never silently zero-matches.
func fkFilterSafe(dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "int", "integer", "smallint", "tinyint", "mediumint", "bigint",
		"char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set":
		return true
	default:
		return false
	}
}

// checkNumbering runs reconstruct.CheckNumberingFrom for one lookup, over the
// window the engine reads by position: schema.table from the snapshot to at.
// A renumbering is returned as is (it wraps reconstruct.ErrBinlogRenumbered,
// which the engine names under its own caveat); a failure of the check is
// wrapped with what was being checked. unchecked is the check's note when it
// could not tell (#2186), which the lookup carries to the engine.
func (p *Provider) checkNumbering(ctx context.Context, schema, table string, anchor *query.BinlogPos, mark string, snap, at time.Time) (unchecked string, err error) {
	if mark == "" {
		return "", nil
	}
	if p.db == nil {
		return "", fmt.Errorf("baseline of %s.%s has an event mark but no index connection was given to check it; "+
			"whether the source's binary log started again after the snapshot cannot be told", schema, table)
	}
	unchecked, err = reconstruct.CheckNumberingFromRead(ctx, p.db, anchor, mark, reconstruct.ReadWindow{
		Schema: schema, Table: table, Since: snap, Until: at, Notice: renumberNotices.To(nil),
	})
	if err == nil || errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		return unchecked, err
	}
	return "", fmt.Errorf("check the binlog numbering since the snapshot of %s.%s: %w", schema, table, err)
}
