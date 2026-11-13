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
	"github.com/go-sql-driver/mysql"
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
	// CoverageWait bounds how long a check whose snapshot is pinned waits,
	// per table, for a running capture to reach the snapshot's consistent
	// point (#2198; see waitIndexCoversPG). Zero means DefaultCoverageWait;
	// negative means do not wait.
	CoverageWait time.Duration
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
//   - The delta window starts at the baseline's LSN floor and recorded bound
//     (reconstruct.PGDeltaWindow, #1596; positions compare as numbers since
//     #2446): from the snapshot's time alone, a commit that raced the
//     snapshot's start was left out and read as a mismatch. It ends at the
//     snapshot (pgLiveDeltaOptions). baselineFetchOptions still reads by time.
//   - Coverage is checked against the stream's LSN checkpoint
//     (indexCoversPG), which can PROVE coverage but not always disprove it —
//     see pgCoverageVerdict for the honest degradation.
//
// Writes during the check (#2198): the source is read at the snapshot a
// temporary logical slot exported (consistency.AnchorPGSlot), which holds
// exactly the transactions committed before the slot's consistent point, and
// the reconstruction stops at the same point: every PostgreSQL row event
// carries its commit LSN in end_pos, so the cut is `end_pos < consistent
// point` by number, on the index and on the archives alike. A table with
// steady writes then compares equal. When the slot cannot be had (the role
// lacks REPLICATION, no free slot, ...) the read is cut at the time the scan
// ended, as before #2198, and every verdict says so: a write to the table
// during the read shows as a mismatch.
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
	// openedBy is taken before the snapshot opens: the baseline the
	// reconstruction starts from must be older than the read (step 2).
	openedBy := time.Now().UTC()
	src, err := cfg.SourceChecksum(ctx, schema, table, pgNormalizeRenderedBytes)
	if errors.Is(err, consistency.ErrPGSnapshotWait) {
		return inconclusive(res, err.Error()), nil
	}
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
	// cutLSN is where the reconstruction stops (0 = not cut), and
	// unpinnedNote is carried on every verdict when it is not.
	var (
		cutLSN       uint64
		unpinnedNote string
	)
	if src.Anchor == consistency.AnchorPGSlot {
		cutLSN = src.LSN
	} else {
		why := src.AnchorUnpinned
		if why == "" {
			why = "the source reader did not pin the snapshot"
		}
		unpinnedNote = unanchoredNote(why)
	}

	// 2. Find the baseline at-or-before the read opened: one taken while the
	// table was being read is newer than the snapshot it is compared with
	// (the MySQL path's rule since #2150). Found BEFORE the coverage check
	// (the MySQL path's opposite order) because the PG gap_lost verdict is
	// scoped to the comparison window, whose start is this baseline's
	// snapshot time. Held until this table is checked (#2675), as on the
	// MySQL path.
	baselinePath, snapshotTime, _, releaseBaseline, err := reconstruct.FindBaselineHeld(ctx, cfg.BaselineSource, schema, table, openedBy)
	if err != nil {
		if isNoBaseline(err) {
			return inconclusive(res, withNote("no baseline at-or-before the snapshot; reconstruct would omit never-touched rows", unpinnedNote)), nil
		}
		return res, fmt.Errorf("find baseline %s.%s: %w", schema, table, err)
	}
	defer releaseBaseline()

	// 3. Coverage: has the index durably absorbed everything the snapshot
	// reflects? Inconclusive when provably not (or when events were
	// permanently lost inside the window); a note when it cannot be proven
	// either way. With the read cut at the snapshot, a running capture is a
	// few seconds behind it on a table with writes, so the check waits for it.
	var (
		covered      bool
		coverageNote string
		lastEventAt  time.Time
	)
	if cutLSN != 0 {
		wait := cfg.CoverageWait
		if wait == 0 {
			wait = DefaultCoverageWait
		}
		covered, coverageNote, lastEventAt, err = waitIndexCoversPG(ctx, cfg.IndexDB, cutLSN, snapshotTime, wait)
		if err != nil {
			return res, fmt.Errorf("wait for the index to reach the snapshot of %s.%s: %w", schema, table, err)
		}
	} else {
		covered, coverageNote = indexCoversPG(ctx, cfg.IndexDB, src.LSN, snapshotTime)
	}
	if !covered {
		return inconclusive(res, withNote(coverageNote, unpinnedNote)), nil
	}

	// 4. Latest event per PK from the baseline's floor to asOf (see the
	// function doc). A change from before the snapshot's time is either in
	// the snapshot already (its full row image changes nothing) or the one
	// the snapshot missed.
	bmeta, err := baseline.ReadParquetMetadataAny(ctx, baselinePath)
	if err != nil {
		return res, fmt.Errorf("read baseline metadata %s.%s: %w", schema, table, err)
	}
	deltaSince, deltaFrom, deltaProven, err := reconstruct.PGDeltaWindow(bmeta, snapshotTime)
	if err != nil {
		return res, fmt.Errorf("%s.%s: %w", schema, table, err)
	}
	// With the cut, the time bound is only the coarse one, and it must not
	// drop a change the snapshot holds that the source's clock stamped past
	// this host's asOf: the capture's last indexed change, stamped by the
	// source, is at or past every change below the cut (liveReadUntil, the
	// MySQL path's rule).
	until := asOf
	if cutLSN != 0 {
		until = liveReadUntil(asOf, lastEventAt)
	}
	deltaOpts := pgLiveDeltaOptions(schema, table, deltaSince, deltaFrom, deltaProven, until, cutLSN)
	engine := query.New(cfg.IndexDB)
	rows, _, err := query.FetchMerged(ctx, cfg.IndexDB, engine, query.FetchMergedOptions{
		Opts:           deltaOpts,
		DBName:         cfg.IndexDBName,
		NoArchive:      cfg.NoArchive,
		ArchiveFetcher: cfg.ArchiveFetcher,
	})
	if err != nil {
		var gap *query.GapError
		if errors.As(err, &gap) {
			return inconclusive(res, withNote("coverage gap in the reconstruction window: "+gap.Error(), unpinnedNote)), nil
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
			return inconclusive(res, withNote(fmt.Sprintf("source column %q is absent from the index schema snapshot (DBTrail snapshots a relation when it first streams it); stream an event for this table so the relation is re-published", name), unpinnedNote)), nil
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
	res.Detail = withNote(res.Detail, unpinnedNote)
	return res, nil
}

// pgLiveDeltaOptions is the delta fetch of a PostgreSQL live-source check:
// the window from the baseline (since, from, proven: reconstruct.PGDeltaWindow)
// to asOf, the latest change per row. With cutLSN != 0 (the consistent point
// of the slot whose snapshot was read) it also stops strictly before cutLSN:
// UntilPos is at-or-before, so it is cutLSN-1, spelled as an LSN (a File with
// a slash) so that both tiers compare end_pos by number (query.BinlogPos.IsLSN);
// a File without one would take the binlog-file rule and match nothing.
// A commit record can start exactly at the consistent point, and that
// transaction is not in the snapshot, hence strict.
func pgLiveDeltaOptions(schema, table string, since time.Time, from *query.BinlogPos, proven bool, asOf time.Time, cutLSN uint64) query.Options {
	opts := query.Options{
		Schema:     schema,
		Table:      table,
		Since:      &since,
		SincePos:   from,
		Until:      &asOf,
		LimitPerPK: 1,
	}
	if cutLSN != 0 {
		opts.UntilPos = &query.BinlogPos{File: formatLSN(cutLSN - 1), Pos: cutLSN - 1}
	}
	if proven {
		opts = opts.SinceProven()
	}
	return opts
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
	st, err := readPGStreamPoint(ctx, indexDB)
	if err != nil {
		return false, err.Error()
	}
	return st.verdict(anchorLSN, windowStart)
}

// pgStreamPoint is the PostgreSQL capture's saved position as stream_state
// holds it.
type pgStreamPoint struct {
	flavor     string
	checkpoint uint64 // 0 = none saved
	gapLostAt  sql.NullTime
	gapDetail  string
	// age is how long ago the checkpoint was saved, by the index server's
	// clock (the one that stamped it); savedAt is the stamp itself, to tell
	// two saves apart.
	age     sql.NullInt64
	savedAt string
	// lastEvent is the source's stamp of the newest change the capture
	// indexed (zero when none).
	lastEvent time.Time
	// sentThrough is stream_state.pg_sent_through_lsn (#2746): every commit
	// of the source database below it is indexed. sentKnown is false when the
	// column is NULL or missing (a capture that predates #2746) or when the
	// last save was not one that wrote it (see readPGStreamPoint); 0 with
	// sentKnown is a capture that has not saved a position yet.
	sentThrough uint64
	sentKnown   bool
}

// readPGStreamPoint reads pgStreamPoint. Its errors are already worded as
// the verdict's reason (inconclusive, never a false mismatch).
func readPGStreamPoint(ctx context.Context, indexDB *sql.DB) (pgStreamPoint, error) {
	var (
		st        pgStreamPoint
		flavor    sql.NullString
		pos       sql.NullInt64
		gapDetail sql.NullString
		savedAt   sql.NullString
		lastEvent sql.NullTime
	)
	// The sent position (#2746) is read in the same statement, so it is from
	// the same save as the checkpoint, and only while pg_sent_through_at is
	// that save's last_checkpoint: another writer's save (an older capture,
	// bintrail-pg reset) unties it. An index without the columns (error 1054:
	// the console reads indexes it never migrates) is read without them, as
	// one with no sent position.
	const pgPointCols = "flavor, binlog_position, gap_lost_at, gap_lost_detail, TIMESTAMPDIFF(SECOND, last_checkpoint, UTC_TIMESTAMP()), CAST(last_checkpoint AS CHAR), last_event_time"
	var sent sql.NullInt64
	err := indexDB.QueryRowContext(ctx,
		"SELECT "+pgPointCols+", IF(pg_sent_through_at <=> last_checkpoint, pg_sent_through_lsn, NULL) FROM stream_state WHERE id = 1").
		Scan(&flavor, &pos, &st.gapLostAt, &gapDetail, &st.age, &savedAt, &lastEvent, &sent)
	if me := (*mysql.MySQLError)(nil); errors.As(err, &me) && me.Number == 1054 {
		sent = sql.NullInt64{}
		err = indexDB.QueryRowContext(ctx, "SELECT "+pgPointCols+" FROM stream_state WHERE id = 1").
			Scan(&flavor, &pos, &st.gapLostAt, &gapDetail, &st.age, &savedAt, &lastEvent)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return st, errors.New("index has no stream state yet (DBTrail not running or never checkpointed)")
	}
	if err != nil {
		return st, errors.New("could not read index coverage: " + err.Error())
	}
	st.flavor, st.gapDetail, st.savedAt = flavor.String, gapDetail.String, savedAt.String
	if lastEvent.Valid {
		st.lastEvent = lastEvent.Time.UTC()
	}
	if pos.Valid && pos.Int64 > 0 {
		st.checkpoint = uint64(pos.Int64)
	}
	if sent.Valid && sent.Int64 >= 0 {
		st.sentThrough, st.sentKnown = uint64(sent.Int64), true
	}
	return st, nil
}

func (st pgStreamPoint) verdict(anchorLSN uint64, windowStart time.Time) (bool, string) {
	return pgCoverageVerdict(st.flavor, st.checkpoint, anchorLSN, gapLostInWindow(st.gapLostAt, windowStart), st.gapDetail)
}

// waitIndexCoversPG is indexCoversPG for a read cut at the snapshot's
// consistent point (#2198), waiting up to wait while the only thing the
// verdict lacks is the capture reaching that point: checkpoint < anchor, the
// "coverage unverified" branch of pgCoverageVerdict, which on a table with
// writes is a capture a few seconds behind, and reconstructing then would
// leave out the changes it has not indexed yet (a false mismatch). It also
// returns the source's stamp of the newest change the capture indexed, for
// the read's time bound.
//
// That same branch is also the steady state of a quiet database: the anchor
// is past the last commit there (creating the slot writes WAL) and other
// databases move it on, so no checkpoint ever reaches it. There the proof is
// the walsender's sent position the capture saves (#2746,
// pg_sent_through_lsn): every commit below it is indexed, so once it reaches
// the anchor the snapshot's changes are all in the index, with no commit
// needed near the anchor. With that position saved, the wait ends only on the
// proof; at the deadline without it the check is inconclusive, because a
// capture held up before delivering a commit below the anchor (a large
// transaction still being decoded, heavy WAL elsewhere, network lag) looks the
// same as a quiet one from the checkpoint alone. A sent position that a whole
// wait did not move is remembered, and later tables of the same run that
// find it unmoved are inconclusive without waiting again.
//
// A capture built before #2746 saves no sent position. For it the wait ends,
// with the verdict as it is (covered, with the note), once the capture has
// saved its position twice during the wait without it moving, which is not a
// proof, and the note says so. A wait that ended that way is remembered
// (stuckCheckpoints), and later tables of the same run whose wait would start
// from it do not wait again.
//
// Either way, a capture that is still moving its position when the wait runs
// out is behind, provably, and the check is inconclusive, and a capture that
// is not running (no save within coverageStale) is not waited for.
func waitIndexCoversPG(ctx context.Context, indexDB *sql.DB, anchorLSN uint64, windowStart time.Time, wait time.Duration) (bool, string, time.Time, error) {
	return waitPGCoverage(ctx, func() (pgStreamPoint, error) { return readPGStreamPoint(ctx, indexDB) },
		indexDB, anchorLSN, windowStart, wait)
}

// waitPGCoverage is waitIndexCoversPG with the read as a seam, for tests.
// stuck keys stuckCheckpoints.
func waitPGCoverage(ctx context.Context, read func() (pgStreamPoint, error), stuck *sql.DB, anchorLSN uint64, windowStart time.Time, wait time.Duration) (bool, string, time.Time, error) {
	deadline := time.Now().Add(wait)
	var (
		started   bool
		from      uint64 // the checkpoint the wait started from
		fromSent  uint64 // the sent position the wait started from (#2746)
		sentMoved bool   // the sent position, or whether it is known, changed during the wait
		lastSave  string
		lastLSN   uint64
		unmoved   int // saves seen during the wait that left the position where it was
	)
	for {
		st, err := read()
		if cerr := ctx.Err(); cerr != nil {
			return false, "", time.Time{}, cerr
		}
		if err != nil {
			return false, err.Error(), time.Time{}, nil
		}
		covered, note := st.verdict(anchorLSN, windowStart)
		if covered && note != "" && st.sentKnown && st.sentThrough >= anchorLSN {
			// Proven without a commit near the anchor (#2746).
			return true, "", st.lastEvent, nil
		}
		if !covered || note == "" || wait < 0 {
			return covered, note, st.lastEvent, nil
		}
		switch {
		case !st.age.Valid:
			return covered, note + ". The capture has no checkpoint time, so it was not waited for", st.lastEvent, nil
		case time.Duration(st.age.Int64)*time.Second > coverageStale:
			return covered, note + fmt.Sprintf(". The capture's last checkpoint is %ds old: is the capture running?", st.age.Int64), st.lastEvent, nil
		}
		if !started {
			started, from, lastSave, lastLSN = true, st.checkpoint, st.savedAt, st.checkpoint
			fromSent = st.sentThrough
			if st.sentKnown && stuckCheckpoints.is(stuck, stuckSentKey(st.sentThrough)) {
				return false, pgSentNotProven(st.sentThrough, anchorLSN, 0), st.lastEvent, nil
			}
			if !st.sentKnown && stuckCheckpoints.is(stuck, formatLSN(from)) {
				return covered, note + ". The capture's saved position did not move during an earlier wait of this run, so it was not waited for again", st.lastEvent, nil
			}
		} else if st.savedAt != lastSave {
			if st.checkpoint == lastLSN {
				unmoved++
			} else {
				unmoved = 0
			}
			lastSave, lastLSN = st.savedAt, st.checkpoint
		}
		if !st.sentKnown || st.sentThrough != fromSent {
			sentMoved = true
		}
		if unmoved >= 2 && !st.sentKnown {
			stuckCheckpoints.set(stuck, formatLSN(st.checkpoint))
			return covered, note + ". The capture saved its position twice during the wait without it moving: no change of this database was committed between it and the snapshot, or the capture is held up before delivering one", st.lastEvent, nil
		}
		if !time.Now().Before(deadline) {
			if st.checkpoint != from {
				return false, fmt.Sprintf("%s: the capture's saved position moved from %s to %s during a %s wait and is still before the snapshot's %s, so it has not indexed every change the snapshot holds; run the check again",
					indexBehind, formatLSN(from), formatLSN(st.checkpoint), wait, formatLSN(anchorLSN)), st.lastEvent, nil
			}
			if st.sentKnown {
				// Remembered only when the whole wait left it where it was: a
				// position that moved is a capture making progress, and the
				// next table may well find it past its snapshot.
				if !sentMoved {
					stuckCheckpoints.set(stuck, stuckSentKey(st.sentThrough))
				}
				return false, pgSentNotProven(st.sentThrough, anchorLSN, wait), st.lastEvent, nil
			}
			stuckCheckpoints.set(stuck, formatLSN(from))
			return covered, note, st.lastEvent, nil
		}
		select {
		case <-ctx.Done():
			return false, "", time.Time{}, ctx.Err()
		case <-time.After(coveragePoll):
		}
	}
}

// pgSentNotProven is the inconclusive reason when the capture saves the
// walsender's sent position (#2746) but it did not reach the snapshot within
// the wait. wait 0 means the table was not waited for: an earlier table of
// the run waited its full time on the same position.
func pgSentNotProven(sent, anchorLSN uint64, wait time.Duration) string {
	got := "it has not saved one yet"
	if sent != 0 {
		got = "it saved " + formatLSN(sent)
	}
	within := fmt.Sprintf("within a %s wait", wait)
	if wait == 0 {
		within = "during an earlier wait of this run, which this table did not repeat"
	}
	return fmt.Sprintf("coverage not proven: %s, the capture did not confirm that PostgreSQL had sent it every change committed before the snapshot's %s (%s), so a change the snapshot holds may not be indexed yet and a difference would not be conclusive; run the check again, and if this repeats, check the capture's replication lag",
		within, formatLSN(anchorLSN), got)
}

// stuckSentKey is the stuckCheckpoints entry for a sent position a whole
// wait did not move; it cannot equal an entry of a checkpoint.
func stuckSentKey(sent uint64) string { return "sent " + formatLSN(sent) }

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
// Hard stops stay hard: a stamped permanent loss (gap_lost_at, which an
// operator's acknowledgement leaves in place) means events
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
		"coverage unverified (index checkpoint %s is behind the snapshot anchor %s, but WAL from other databases and non-transactional activity advances the anchor without producing indexable events, so containment cannot be proven): assuming DBTrail is caught up",
		formatLSN(checkpointLSN), formatLSN(anchorLSN))
}

// formatLSN renders a WAL position in PostgreSQL's canonical X/Y form —
// byte-identical to pglogrepl.LSN.String(), reimplemented locally because
// this package must not link the PG driver stack (see PGSourceChecksum).
func formatLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}
