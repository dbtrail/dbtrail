package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// PGSourceChecksum fingerprints one live PostgreSQL table at a consistent
// snapshot, applying normalize to every non-NULL scanned value — the seam
// through which VerifyTablePG reaches the live source WITHOUT this package
// linking the PostgreSQL driver stack. This package (and internal/cli, which
// links it into the core binary) must stay pgx-free: cliapp's
// TestCoreBinaryIsPostgresFree and internal/event's dep guard ban the PG
// capture stack from the core binary and the read layer. The one
// implementation is internal/pgverifysource (LiveSource), linked only by
// pgx-carrying binaries: cmd/bintrail-pg via cli.SetPGLiveVerifyConnect, and
// the console daemon directly.
//
// VerifyTablePG passes its own render normalizer as normalize, so an
// implementation never chooses normalization policy — symmetry with the
// reconstruct side stays by construction.
type PGSourceChecksum func(ctx context.Context, schema, table string, normalize func(raw []byte) []byte) (consistency.TableChecksum, error)

// PGLiveConfig wires the data sources live-source verify needs for a
// PostgreSQL source (#1024) — the PG sibling of Config. SourceChecksum
// replaces Config.SourceDB: the live fingerprint must run under the capture
// plane's pinned render GUCs on a PG driver connection, both of which live
// behind the PGSourceChecksum seam (internal/pgverifysource).
type PGLiveConfig struct {
	// SourceChecksum is the live-source fingerprint provider — wire it with
	// internal/pgverifysource.LiveSource. VerifyTablePG calls it serially,
	// one table at a time.
	SourceChecksum PGSourceChecksum
	IndexDB        *sql.DB
	Resolver       *metadata.Resolver
	BaselineSource string // local dir or s3:// prefix, passed to FindBaseline
	IndexDBName    string
	NoArchive      bool
	ArchiveFetcher query.ArchiveFetcher
	// DuckDBTuning is the resource budget for the baseline-merge DuckDB
	// session the reconstruct step opens — see Config.DuckDBTuning.
	DuckDBTuning duckdbutil.Tuning
}

// PGTargetTables returns the tables a PostgreSQL live-source (or
// recover-inputs) run should cover: the explicit "schema.table" list when one
// was given, else every table the resolver knows. It exists because the
// MySQL-path fallback — SELECT ... WHERE snapshot_id = MAX(snapshot_id) —
// silently returns ONE table on a PG index: WritePGSnapshot stores one
// relation per snapshot_id, so the newest snapshot_id names the most recently
// (re-)published relation, not the schema. The per-table resolver
// (metadata.NewLatestPerTableResolver, chosen by ResolverFor) already folds
// every relation's newest snapshot into one view; enumerate that.
func PGTargetTables(resolver *metadata.Resolver, explicit []string) ([]query.SchemaTable, error) {
	var out []query.SchemaTable
	if len(explicit) > 0 {
		for _, entry := range explicit {
			schema, table, ok := strings.Cut(entry, ".")
			if !ok || schema == "" || table == "" {
				return nil, fmt.Errorf("invalid table filter %q (want schema.table)", entry)
			}
			out = append(out, query.SchemaTable{Schema: schema, Table: table})
		}
	} else {
		for _, tm := range resolver.AllTables() {
			out = append(out, query.SchemaTable{Schema: tm.Schema, Table: tm.Table})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Table < out[j].Table
	})
	return out, nil
}

// pgNormalizeRenderedBytes is the live-scan normalization hook for a
// PostgreSQL source — normalizeRenderedBytes under an EMPTY data type,
// because that is exactly what the reconstruct side does:
// metadata.WritePGSnapshot stores data_type="" for every PG column, so
// renderCellNormalized on the recon side resolves to
// normalizeRenderedBytes(b, ""). Both sides therefore apply the identical
// policy (in practice: only the type-independent JSON-container
// canonicalization; the MySQL zero-date/TIME/FLOAT rewrites are keyed on
// MySQL DATA_TYPE tokens a PG snapshot never carries). Symmetry by
// construction — see ConsistentTableChecksumNormalized's doc for why an
// asymmetric normalization trades one false mismatch for another.
func pgNormalizeRenderedBytes(raw []byte) []byte {
	return normalizeRenderedBytes(raw, "")
}

// VerifyTablePG verifies one table of a PostgreSQL source in live-source mode
// (#1024): fingerprint the live source at a REPEATABLE READ snapshot
// (consistency.ConsistentTableChecksumPG), reconstruct the table to that same
// point from baseline + indexed deltas, render both sides to PostgreSQL's own
// text form, and compare — the PG sibling of VerifyTable. Differences from
// the MySQL path, each anchored to an existing PG-path decision:
//
//   - No PK-type gate: PG stores every PK column as raw text on BOTH the
//     baseline (COPY text) and delta (pgoutput text) sides, so the match is
//     string-identity — same bypass as VerifyBaselinePair's pg branch.
//   - The delta window is time-bounded ONLY (no SincePos/UntilPos): PG events
//     carry a non-monotonic "X/Y" LSN in binlog_file that the
//     length-lexicographic position filter cannot bound correctly — the same
//     "PG never sets a position bound" invariant baselineFetchOptions keeps
//     (#1022 is the deferred numeric-LSN refinement).
//   - Coverage is checked against the stream's LSN checkpoint
//     (indexCoversPG), which can PROVE coverage but not always disprove it —
//     see pgCoverageVerdict for the honest degradation.
//
// The T0→T1 alignment caveat of VerifyTable applies identically: writes
// committed between the snapshot opening and the reconstruct's Until surface
// as a (safe, conclusive) mismatch — run against a quiescent source,
// off-peak.
func VerifyTablePG(ctx context.Context, cfg PGLiveConfig, schema, table string) (TableResult, error) {
	res := TableResult{Schema: schema, Table: table}

	tm, err := cfg.Resolver.Resolve(schema, table)
	if err != nil {
		return res, fmt.Errorf("resolve %s.%s: %w", schema, table, err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return inconclusive(res, "table has no primary key"), nil
	}

	// 1. Live source fingerprint at a consistent snapshot, normalized with the
	// same policy the reconstruct render applies (see pgNormalizeRenderedBytes).
	if cfg.SourceChecksum == nil {
		return res, fmt.Errorf("source checksum %s.%s: no PostgreSQL source checksum wired (see PGSourceChecksum; internal/pgverifysource.LiveSource provides it)", schema, table)
	}
	src, err := cfg.SourceChecksum(ctx, schema, table, pgNormalizeRenderedBytes)
	if err != nil {
		return res, fmt.Errorf("source checksum %s.%s: %w", schema, table, err)
	}
	// A zero anchor would make the coverage verdict below vacuously "proven"
	// (any nonzero checkpoint >= 0). The one shipped implementation errors
	// before it can return LSN 0, so this guard enforces the seam's contract
	// rather than papering over a live bug — turning a convention into an
	// invariant instead of a silent pass.
	if src.LSN == 0 {
		return inconclusive(res, "source checksum carried no WAL anchor; cannot check index coverage"), nil
	}
	res.SourceDigest = src.Digest
	res.SourceRows = src.RowCount
	res.Anchor = fmt.Sprintf("LSN:%d", src.LSN) // same label form as anchorLabel's pg branch
	asOf := time.Now().UTC()

	// 2. Find the baseline at-or-before asOf — identical to the MySQL path.
	// Found BEFORE the coverage check (the MySQL path's opposite order)
	// because the PG gap_lost verdict is scoped to the comparison window,
	// whose start is this baseline's snapshot time.
	baselinePath, snapshotTime, _, err := reconstruct.FindBaseline(ctx, cfg.BaselineSource, schema, table, asOf)
	if err != nil {
		if isNoBaseline(err) {
			return inconclusive(res, "no baseline at-or-before the snapshot; reconstruct would omit never-touched rows"), nil
		}
		return res, fmt.Errorf("find baseline %s.%s: %w", schema, table, err)
	}

	// 3. Coverage: has the index durably absorbed everything the snapshot
	// reflects? Inconclusive when provably not (or when events were
	// permanently lost inside the window); a note when it cannot be proven
	// either way.
	covered, coverageNote := indexCoversPG(ctx, cfg.IndexDB, src.LSN, snapshotTime)
	if !covered {
		return inconclusive(res, coverageNote), nil
	}

	// 4. Latest event per PK in (baseline, asOf]. Time bounds only — no
	// SincePos/UntilPos on a PG window (see the function doc).
	engine := query.New(cfg.IndexDB)
	rows, _, err := query.FetchMerged(ctx, cfg.IndexDB, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:     schema,
			Table:      table,
			Since:      &snapshotTime,
			Until:      &asOf,
			LimitPerPK: 1,
		},
		DBName:         cfg.IndexDBName,
		NoArchive:      cfg.NoArchive,
		ArchiveFetcher: cfg.ArchiveFetcher,
	})
	if err != nil {
		var gap *query.GapError
		if errors.As(err, &gap) {
			return inconclusive(res, "coverage gap in the reconstruction window: "+gap.Error()), nil
		}
		return res, fmt.Errorf("fetch changes %s.%s: %w", schema, table, err)
	}
	// Same event-normalization passes as VerifyBaselinePair's pg branch runs —
	// both are structurally no-ops while WritePGSnapshot stores empty
	// data_type, and keeping the calls identical means the two PG verify
	// modes cannot drift if that ever changes.
	reconstruct.MapEventEnumLabels(cfg.IndexDB, cfg.Resolver, schema, table, rows)
	binariesTyped := reconstruct.DecodeEventBinaries(cfg.IndexDB, schema, table, rows)
	changes := make(map[string]*query.ResultRow, len(rows))
	for i := range rows {
		changes[rows[i].PKValues] = &rows[i]
	}

	// 5. Hash exactly the column set the live scan hashed, in its order —
	// same rule as VerifyTable step 5. The PG live scan and pgbaseline share
	// one column contract (live, non-dropped, non-generated, attnum order),
	// so a name missing from the index snapshot means the capture daemon has
	// not (re-)published this relation since the column appeared.
	colByName := make(map[string]metadata.ColumnMeta, len(tm.Columns))
	for _, c := range tm.Columns {
		colByName[c.Name] = c
	}
	orderedCols := make([]metadata.ColumnMeta, 0, len(src.Columns))
	for _, name := range src.Columns {
		cm, ok := colByName[name]
		if !ok {
			return inconclusive(res, fmt.Sprintf("source column %q is absent from the index schema snapshot (the capture daemon snapshots a relation when it first streams it); stream an event for this table so the relation is re-published", name)), nil
		}
		orderedCols = append(orderedCols, cm)
	}
	// Deferred-representation gate — mirrored from VerifyBaselinePair for the
	// same no-drift reason as the normalization passes above: with empty PG
	// data types isDeferredType never fires, so this is constant-off today.
	deferredCol, deferredRepr := deferredReprUnresolved(orderedCols, changes, binariesTyped)
	var deferredDetail string
	if deferredRepr {
		deferredDetail = deferredReprDetail(deferredCol)
	}

	// 6. Reconstruct to asOf and hash in the source's text form. pgTextPK=true:
	// the PK join is text-identity, the MySQL canonicalizer must not run.
	reconDigest, reconCount, emitErr := reconstructDigest(ctx, baselinePath, schema, table, pkCols, changes, rows, orderedCols, true, renderCellNormalized, cfg.DuckDBTuning)
	if emitErr != nil {
		return res, fmt.Errorf("reconstruct %s.%s: %w", schema, table, emitErr)
	}
	res.ReconstructDigest = reconDigest
	res.ReconstructRows = reconCount
	if coverageNote != "" {
		res.Detail = coverageNote
	}

	// 7. Compare — the same pure core as every other verify mode. Unlike the
	// MySQL sibling (where the GTID-off coverage note is a rare corner), the
	// coverage-unverified note is the ROUTINE state on PG (checkpoint <
	// anchor on any non-idle cluster), and index lag is a common CAUSE of a
	// reported divergence — so the note is APPENDED to a non-empty verdict
	// detail, never dropped: a "row count differs" whose real cause is a
	// lagging daemon must carry the one clue that explains it.
	status, detail := classify(res.SourceDigest, res.SourceRows, res.ReconstructDigest, res.ReconstructRows, deferredDetail)
	// How the snapshot was read (#1380), same rule as every other mode. A
	// PostgreSQL snapshot is read inside one transaction and records it; one
	// written before the record existed has none, and neither has one whose
	// footer cannot be read.
	var snapshotLock baseline.ReadConsistency
	if bmeta, berr := baseline.ReadParquetMetadataAny(ctx, baselinePath); berr != nil {
		slog.Warn("could not read the snapshot's footer, so how it was locked is not known",
			"schema", schema, "table", table, "path", baselinePath, "error", berr)
	} else {
		snapshotLock = baseline.ReadConsistencyOf(bmeta)
	}
	v := withSnapshotLock(status, detail,
		lockSide{what: "the snapshot of " + snapshotTime.UTC().Format(time.RFC3339), lock: snapshotLock})
	status, detail = v.status, v.detail
	res.SnapshotLock = v.lock.String()
	if v.kind != "" {
		res.InconclusiveKind = v.kind
	}
	res.Status = status
	switch {
	case detail == "":
		// StatusMatch: res.Detail already carries the coverage note (or "").
	case coverageNote != "":
		res.Detail = detail + " (note: " + coverageNote + ")"
	default:
		res.Detail = detail
	}
	return res, nil
}

// indexCoversPG reads the stream checkpoint and applies pgCoverageVerdict —
// the PG sibling of indexCovers. Read errors degrade to not-covered with the
// error as the reason (inconclusive, never a false mismatch), matching the
// MySQL path's handling.
//
// windowStart scopes the gap_lost stamp to the comparison window: a loss
// stamped BEFORE the baseline's snapshot time is outside the window — the
// baseline is a fresh dump of the source, so it re-covers whatever the gap
// lost — and must not degrade the verdict, or one historical gap would make
// this index permanently unverifiable no matter how many clean baselines
// follow. Same window-scoping convention as `verify --check recover` ("a
// stamped gap_lost_at inside the window degrades to inconclusive").
func indexCoversPG(ctx context.Context, indexDB *sql.DB, anchorLSN uint64, windowStart time.Time) (bool, string) {
	var (
		flavor    sql.NullString
		pos       sql.NullInt64
		gapLostAt sql.NullTime
		gapDetail sql.NullString
	)
	err := indexDB.QueryRowContext(ctx,
		"SELECT flavor, binlog_position, gap_lost_at, gap_lost_detail FROM stream_state WHERE id = 1").
		Scan(&flavor, &pos, &gapLostAt, &gapDetail)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "index has no stream state yet (daemon not running or never checkpointed)"
	}
	if err != nil {
		return false, "could not read index coverage: " + err.Error()
	}
	var checkpoint uint64
	if pos.Valid && pos.Int64 > 0 {
		checkpoint = uint64(pos.Int64)
	}
	return pgCoverageVerdict(flavor.String, checkpoint, anchorLSN, gapLostInWindow(gapLostAt, windowStart), gapDetail.String)
}

// gapLostInWindow reports whether a stamped permanent loss falls inside the
// comparison window (at-or-after its start). Pure — see indexCoversPG for why
// a pre-window loss must not degrade the verdict.
func gapLostInWindow(gapLostAt sql.NullTime, windowStart time.Time) bool {
	return gapLostAt.Valid && !gapLostAt.Time.Before(windowStart)
}

// pgCoverageVerdict is the pure coverage decision for a PostgreSQL source —
// separated from the stream_state read (indexCoversPG) so the verdict grid is
// unit-testable, the same fetch/verdict split as internal/doctor's object-lock
// check.
//
// PostgreSQL has no GTID-set containment: the only durable index cursor is
// the stream's checkpoint — the last commit LSN it has durably indexed
// (saveCheckpointPG) — while the snapshot anchor (pg_current_wal_lsn) also
// advances with WAL the capture can never see as events: commits in OTHER
// databases of the cluster, and non-transactional activity (autovacuum,
// checkpoints). So the grid is deliberately asymmetric:
//
//   - checkpoint >= anchor PROVES coverage (everything the snapshot reflects
//     committed at-or-before the anchor, and the index has durably absorbed
//     commits past it) → covered, no note.
//   - checkpoint < anchor proves NOTHING: the distance may be entirely
//     unindexable WAL. Refusing here would make verify permanently
//     inconclusive on any shared or merely non-idle cluster (cry-wolf), so it
//     proceeds with a "coverage unverified" note — the same documented
//     trade-off as the MySQL path on a GTID-off source, where a genuinely
//     behind index surfaces as a (conclusive, investigable) mismatch rather
//     than being silently masked.
//
// Hard stops stay hard: a stamped permanent loss (gap_lost_at) means events
// in the window may be gone — inconclusive, never a false mismatch — and a
// missing/foreign-flavor/zero checkpoint means no PG stream has ever
// checkpointed here, so there is nothing to assume "caught up".
func pgCoverageVerdict(flavor string, checkpointLSN, anchorLSN uint64, gapLost bool, gapDetail string) (bool, string) {
	if flavor != flavorPostgres {
		return false, fmt.Sprintf("index stream state has flavor %q, not a PostgreSQL capture; check that this index belongs to the PostgreSQL source being verified", flavor)
	}
	if gapLost {
		detail := strings.TrimSpace(gapDetail)
		if detail == "" {
			detail = "no detail recorded"
		}
		return false, "the capture stream recorded a permanent event loss inside the comparison window (" + detail + "); events the reconstruction needs may be missing, so a content difference would not be conclusive"
	}
	if checkpointLSN == 0 {
		return false, "index has no LSN checkpoint yet (the PostgreSQL stream has not committed a checkpoint against this index)"
	}
	if checkpointLSN >= anchorLSN {
		return true, ""
	}
	return true, fmt.Sprintf(
		"coverage unverified (index checkpoint %s is behind the snapshot anchor %s, but WAL from other databases and non-transactional activity advances the anchor without producing indexable events, so containment cannot be proven): assuming the capture daemon is caught up",
		formatLSN(checkpointLSN), formatLSN(anchorLSN))
}

// formatLSN renders a WAL position in PostgreSQL's canonical X/Y form —
// byte-identical to pglogrepl.LSN.String(), reimplemented locally because
// this package must not link the PG driver stack (see PGSourceChecksum).
func formatLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}
