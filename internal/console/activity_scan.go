package console

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The Overview aggregate's scan: how ONE materialization reads the index, and
// the two things that keep it from hurting the index it reads.
//
// 1. At most one scan per index database, enforced by the INDEX SERVER, not by
// this process. Every scan holds a MySQL named lock (GET_LOCK) on the pinned
// connection its statements run on. A Go-side single-flight cannot see a
// second console process, a console restarted while its old statement still
// runs, or a client that gave up on a statement: go-sql-driver cancels by
// closing the socket WITHOUT KILL QUERY, so the server keeps scanning after
// the client returned. The named lock belongs to the server session, so it
// stays held until that statement really ends, and nothing can start another
// scan meanwhile. Each statement also carries a server-side
// MAX_EXECUTION_TIME, so an orphaned statement (its client gone) ends by
// itself instead of running for as long as the scan takes.
//
// 2. The cost follows what CHANGED, not the retention. The aggregate is
// counted per live partition, and the counts of a partition whose whole range
// is already behind "now" are kept with a fingerprint: the partition's
// MAX(event_id), read BEFORE its counts. A refresh re-reads every fingerprint
// (an index lookup per partition, "Select tables optimized away") and recounts
// only the partitions whose fingerprint moved, plus the partition(s) still
// open at "now". Why the fingerprint is enough:
//   - every row reaches binlog_events through an INSERT that lets
//     AUTO_INCREMENT assign event_id (indexer, stream); an assigned id is
//     larger than every id the table ever held (MySQL 8.0 persists the
//     counter), so an insert into ANY partition, including a backlog
//     catch-up writing an old hour, raises that partition's MAX;
//   - the only DELETE capture issues is the resume-time dedup, which removes
//     the rows written after the last checkpoint: with one writer per index
//     database those are the newest ids, so every partition that loses rows
//     loses its MAX and the fingerprint moves;
//   - rotation drops whole partitions (they leave the listing and their
//     counts are dropped), REORGANIZE only splits p_future (which is never
//     cached), and a name recreated after a drop receives new, larger ids.
//
// Reading the fingerprint BEFORE counting is load-bearing: a row committed in
// between is then counted but not fingerprinted, which only causes one
// needless recount; the other order would leave it uncounted for good.
// What the argument does not cover is any change that leaves a partition's
// MAX in place: two writers on one index database committing out of order;
// `restore-index` re-inserting archived rows with their original ids into a
// partition that already holds rows; and the dedup's legacy shapes with no
// event_id floor, which can reach older rows (the GTID straggler pass after a
// MariaDB RESET MASTER renumbers GTIDs, or the position pass when file names
// after a failover sort "greater"). Those are bounded by a rolling recount:
// every refresh also recounts the one kept partition whose counts are oldest,
// once they are older than activityReverifyAge. That bound is slow on a long
// retention (one hour of it per refresh), which is the price of not
// re-scanning the retention; a COUNT(*) fingerprint would close the holes but
// IS a scan of the partition.
const (
	// activityStmtTimeout is the server-side deadline on each aggregate
	// statement (MAX_EXECUTION_TIME). One partition is an hour of capture.
	activityStmtTimeout = 5 * time.Minute

	// activityStmtGrace is how much longer than the server's own deadline the
	// client waits before giving up on a statement. The client should never be
	// the one that gives up: a client-side cancel is exactly what leaves a
	// statement running on the server with nobody waiting for it.
	activityStmtGrace = 30 * time.Second

	// activityRunTimeout is the last-resort leash on one whole scan. Kept
	// partitions survive it, so a first scan longer than this resumes where
	// it stopped on the next attempt.
	activityRunTimeout = 30 * time.Minute

	// activityReverifyAge is how old kept counts may get before a refresh
	// recounts them anyway (one partition per refresh). See the file comment.
	activityReverifyAge = time.Hour

	// activityMaxStoredGroups caps the (schema, table, event_type) rows kept
	// across all partitions and scopes of one server. Past it, further
	// partitions are counted on every refresh instead of kept.
	activityMaxStoredGroups = 500000

	// activityMetaTimeout bounds the partition listing and a fingerprint
	// statement, both index-only reads; one stuck behind a metadata lock must
	// not hold the scan for activityRunTimeout.
	activityMetaTimeout = time.Minute

	// activityFingerprintChunk is how many partitions one fingerprint
	// statement reads.
	activityFingerprintChunk = 100
)

// errActivityBusy reports that another session holds the index's activity
// lock: a scan started by another console process, or by this console before
// a restart, is still running on the index server.
var errActivityBusy = errors.New("another activity count is still running on this index (started by another console, or by this console before it restarted); a new one starts once it finishes")

// activityLockName is the named lock for one index database. MySQL caps lock
// names at 64 characters (error 3057), and a database name can be 64 on its
// own, so the name is hashed. Instance-wide on purpose: every console reading
// this database, OSS or not, takes the same lock.
func activityLockName(dbName string) string {
	sum := sha256.Sum256([]byte(dbName))
	return "bintrail.activity." + hex.EncodeToString(sum[:16])
}

// activityQuerier is what the scan reads through: the pinned *sql.Conn that
// holds the lock.
type activityQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// acquireActivityLock takes the index's activity lock on conn without
// waiting. The returned release gives it back; if that fails, the connection
// is discarded rather than returned to the pool still holding the lock.
func acquireActivityLock(ctx context.Context, conn *sql.Conn, dbName string) (func(), error) {
	name := activityLockName(dbName)
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil {
		return nil, fmt.Errorf("take the activity lock: %w", err)
	}
	if !got.Valid {
		return nil, fmt.Errorf("take the activity lock: GET_LOCK returned NULL")
	}
	if got.Int64 != 1 {
		// Name the session that holds it: a console host that vanished
		// without closing its connection leaves an IDLE session holding the
		// lock until wait_timeout, and the operator needs its id to KILL it.
		var holder sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", name).Scan(&holder); err == nil && holder.Valid {
			return nil, fmt.Errorf("%w; the index connection holding it is %d (lock %s): if no console is counting, that connection is dead and `KILL %d` frees it",
				errActivityBusy, holder.Int64, name, holder.Int64)
		}
		return nil, errActivityBusy
	}
	return func() {
		rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var released sql.NullInt64
		if err := conn.QueryRowContext(rctx, "SELECT RELEASE_LOCK(?)", name).Scan(&released); err != nil || released.Int64 != 1 {
			// A pooled connection that still holds the lock would block every
			// later scan for as long as it lives. Closing it frees the lock.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}, nil
}

// activityPartition is one live partition of binlog_events and its range
// [lo, hi). loOpen marks the first partition (no lower bound); hiOpen marks
// MAXVALUE (p_future).
type activityPartition struct {
	name           string
	lo, hi         time.Time
	loOpen, hiOpen bool
}

// activityGroup is one grouped row of the aggregate.
type activityGroup struct {
	schema, table string
	etype         uint8
	n             int64
}

// activityPartCount is a kept count of one closed partition.
type activityPartCount struct {
	lo, hi    time.Time // the range counted
	maxID     sql.NullInt64
	rows      []activityGroup
	truncated bool
	countedAt time.Time
}

// activityPartStore is one scope's kept partition counts. It is only touched
// while the cache's scan lock is held.
type activityPartStore struct {
	parts map[string]*activityPartCount
}

func newActivityPartStore() *activityPartStore {
	return &activityPartStore{parts: map[string]*activityPartCount{}}
}

func (s *activityPartStore) groups() int {
	n := 0
	for _, p := range s.parts {
		n += len(p.rows)
	}
	return n
}

// activityScan carries what one scan needs besides the database.
type activityScan struct {
	dbName      string
	deny, allow []query.SchemaTable
	// store holds this scope's kept counts; nil keeps nothing.
	store *activityPartStore
	// storeBudget is how many more groups store may take; <0 = unlimited.
	storeBudget int
	stmtTimeout time.Duration
	now         time.Time
}

// computeActivity is one materialization: take the index's activity lock,
// derive the live-retention window, count it, fold, and stamp the result.
// This is the ONLY data path behind /api/activity — there is no archive read
// to fall back to (see the #1352 header comment).
func computeActivity(ctx context.Context, db *sql.DB, sc activityScan) (activityResponse, error) {
	if sc.stmtTimeout <= 0 {
		sc.stmtTimeout = activityStmtTimeout
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return activityResponse{}, fmt.Errorf("open an index connection: %w", err)
	}
	defer conn.Close()
	release, err := acquireActivityLock(ctx, conn, sc.dbName)
	if err != nil {
		return activityResponse{}, err
	}
	defer release()

	until := sc.now
	if until.IsZero() {
		until = time.Now()
	}
	until = until.UTC().Truncate(time.Second)
	sc.now = until

	var parts []activityPartition
	if sc.dbName != "" {
		if parts, err = listActivityPartitions(ctx, conn, sc.dbName); err != nil {
			return activityResponse{}, err
		}
	}
	since, label := activityWindow(parts, until)

	var agg *activityAgg
	if len(parts) == 0 {
		// Not partitioned, or no database name to list partitions for: one
		// statement over the window, as before.
		agg = newActivityAgg()
		rows, truncated, err := countActivity(ctx, conn, "", since, until, sc)
		if err != nil {
			return activityResponse{}, err
		}
		foldActivityGroups(agg, rows)
		agg.truncated = agg.truncated || truncated
	} else if agg, err = collectPartitioned(ctx, conn, parts, since, until, sc); err != nil {
		return activityResponse{}, err
	}

	resp := agg.response(label, since, until)
	resp.RefreshedAt = until.Format(consoleTSFormat)
	resp.Complete = !resp.Truncated
	if resp.Truncated {
		resp.Notes = append(resp.Notes, fmt.Sprintf(
			"This index has more than %d table/event-type groups in the window; the counts below are a floor, not the total.", activityMaxGroups))
	}
	return resp, nil
}

// listActivityPartitions reads binlog_events' partitions in range order.
func listActivityPartitions(ctx context.Context, q activityQuerier, dbName string) ([]activityPartition, error) {
	ctx, cancel := context.WithTimeout(ctx, activityMetaTimeout)
	defer cancel()
	rows, err := q.QueryContext(ctx, `
		SELECT PARTITION_NAME, PARTITION_DESCRIPTION FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND PARTITION_NAME IS NOT NULL
		ORDER BY PARTITION_ORDINAL_POSITION`, dbName)
	if err != nil {
		return nil, fmt.Errorf("list live partitions: %w", err)
	}
	defer rows.Close()
	var out []activityPartition
	var prev *activityPartition
	for rows.Next() {
		var name string
		var desc sql.NullString
		if err := rows.Scan(&name, &desc); err != nil {
			return nil, err
		}
		p := activityPartition{name: name, loOpen: true}
		if prev != nil && !prev.hiOpen {
			p.lo, p.loOpen = prev.hi, false
		}
		secs, perr := strconv.ParseInt(strings.TrimSpace(desc.String), 10, 64)
		if perr != nil {
			// MAXVALUE, or anything this build cannot read: treat as open at
			// the top, which is never kept and always bounded by "now".
			p.hiOpen = true
		} else {
			p.hi = time.Unix(secs-62167219200, 0).UTC()
		}
		out = append(out, p)
		prev = &out[len(out)-1]
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// activityWindow derives the Overview's window from what the live table
// actually holds: since = the oldest dated live partition's hour, until = now.
// The window IS the live retention (#1352) — if rotation keeps 12 hours live,
// the window is 12 hours — which is what makes the single-tier read honest by
// construction: a window equal to live coverage cannot contain archived-only
// hours, so no completeness caveat is needed and none is computed.
//
// When the floor cannot be determined — no dated partitions yet (a fresh index
// whose rows all sit in p_future) or a bundle with no database name — the
// window falls back to a bounded, stated activityFallbackWindow rather than an
// unbounded scan or a fabricated floor.
func activityWindow(parts []activityPartition, until time.Time) (time.Time, string) {
	var oldest time.Time
	for _, p := range parts {
		t, ok := indexer.PartitionDate(p.name)
		if !ok {
			continue // p_future / malformed
		}
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	if oldest.IsZero() || !oldest.Before(until) {
		return until.Add(-activityFallbackWindow), activityFallbackLabel
	}
	return oldest, "live retention · " + windowLabel(until.Sub(oldest))
}

// activityWork is one partition's share of the window.
type activityWork struct {
	p      activityPartition
	lo, hi time.Time
	// closed: the partition's whole range is behind "now", so its counts
	// depend only on its rows and can be kept under a fingerprint.
	closed bool
}

// planActivity lists what each partition contributes to [since, until). A
// partition starting at or after until, or ending at or before since, holds no
// row of the window and is skipped; the rest are counted over their share,
// which for a closed partition is its own range clipped at since.
func planActivity(parts []activityPartition, since, until time.Time) []activityWork {
	var out []activityWork
	for _, p := range parts {
		if !p.loOpen && !p.lo.Before(until) {
			continue
		}
		if !p.hiOpen && !p.hi.After(since) {
			continue
		}
		lo := since
		if !p.loOpen && p.lo.After(since) {
			lo = p.lo
		}
		if !p.hiOpen && !p.hi.After(until) {
			out = append(out, activityWork{p: p, lo: lo, hi: p.hi, closed: true})
		} else {
			out = append(out, activityWork{p: p, lo: lo, hi: until})
		}
	}
	return out
}

// collectPartitioned counts the window partition by partition, reusing kept
// counts whose fingerprint has not moved.
func collectPartitioned(ctx context.Context, q activityQuerier, parts []activityPartition, since, until time.Time, sc activityScan) (*activityAgg, error) {
	works := planActivity(parts, since, until)
	store := sc.store
	if store == nil {
		store = newActivityPartStore()
		sc.storeBudget = 0
	}

	// Kept counts of partitions that are gone or no longer closed are dropped
	// first: rotation dropped them, or their range changed.
	closed := map[string]activityWork{}
	var closedNames []string
	for _, w := range works {
		if w.closed {
			closed[w.p.name] = w
			closedNames = append(closedNames, w.p.name)
		}
	}
	for name, pc := range store.parts {
		if w, ok := closed[name]; !ok || !pc.lo.Equal(w.lo) || !pc.hi.Equal(w.hi) {
			if sc.storeBudget >= 0 {
				sc.storeBudget += len(pc.rows)
			}
			delete(store.parts, name)
		}
	}

	// Fingerprints BEFORE counts (see the file comment for why the order
	// matters).
	fps, err := activityFingerprints(ctx, q, closedNames)
	if err != nil {
		return nil, err
	}

	reuse := map[string]bool{}
	var reverify string
	var reverifyAt time.Time
	for _, name := range closedNames {
		pc := store.parts[name]
		if pc == nil || pc.maxID != fps[name] {
			continue
		}
		reuse[name] = true
		if sc.now.Sub(pc.countedAt) >= activityReverifyAge && (reverify == "" || pc.countedAt.Before(reverifyAt)) {
			reverify, reverifyAt = name, pc.countedAt
		}
	}
	if reverify != "" {
		delete(reuse, reverify)
	}

	// Newest first: if a statement fails partway, the hours an operator looks
	// at first are the ones already kept for the next attempt.
	fresh := map[string][]activityGroup{}
	truncated := false
	for i := len(works) - 1; i >= 0; i-- {
		w := works[i]
		if w.closed && reuse[w.p.name] {
			continue
		}
		rows, trunc, err := countActivity(ctx, q, w.p.name, w.lo, w.hi, sc)
		if err != nil {
			return nil, err
		}
		truncated = truncated || trunc
		if old := store.parts[w.p.name]; old != nil {
			if sc.storeBudget >= 0 {
				sc.storeBudget += len(old.rows)
			}
			delete(store.parts, w.p.name)
		}
		if w.closed && (sc.storeBudget < 0 || len(rows) <= sc.storeBudget) {
			store.parts[w.p.name] = &activityPartCount{
				lo: w.lo, hi: w.hi, maxID: fps[w.p.name], rows: rows,
				truncated: trunc, countedAt: sc.now,
			}
			if sc.storeBudget >= 0 {
				sc.storeBudget -= len(rows)
			}
			continue
		}
		fresh[w.p.name] = rows
	}

	agg := newActivityAgg()
	for _, w := range works {
		rows := fresh[w.p.name]
		if pc := store.parts[w.p.name]; pc != nil && w.closed {
			rows = pc.rows
			truncated = truncated || pc.truncated
		}
		if !foldActivityGroups(agg, rows) {
			break
		}
	}
	agg.truncated = agg.truncated || truncated
	return agg, nil
}

// activityFingerprints reads MAX(event_id) of each named partition.
func activityFingerprints(ctx context.Context, q activityQuerier, names []string) (map[string]sql.NullInt64, error) {
	out := make(map[string]sql.NullInt64, len(names))
	for start := 0; start < len(names); start += activityFingerprintChunk {
		chunk := names[start:min(start+activityFingerprintChunk, len(names))]
		parts := make([]string, len(chunk))
		for i, name := range chunk {
			parts[i] = fmt.Sprintf("SELECT %d AS i, MAX(event_id) AS m FROM binlog_events PARTITION (%s)", i, quotePartition(name))
		}
		fctx, cancel := context.WithTimeout(ctx, activityMetaTimeout)
		rows, err := q.QueryContext(fctx, strings.Join(parts, " UNION ALL "))
		if err != nil {
			cancel()
			return nil, fmt.Errorf("read partition fingerprints: %w", err)
		}
		err = scanFingerprints(rows, chunk, out)
		cancel()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func scanFingerprints(rows *sql.Rows, chunk []string, out map[string]sql.NullInt64) error {
	defer rows.Close()
	for rows.Next() {
		var i int
		var m sql.NullInt64
		if err := rows.Scan(&i, &m); err != nil {
			return err
		}
		if i >= 0 && i < len(chunk) {
			out[chunk[i]] = m
		}
	}
	return rows.Err()
}

func quotePartition(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// countActivity runs the grouped COUNT over [lo, hi), restricted to one
// partition when partition is set. It returns at most activityMaxGroups rows;
// truncated reports that more existed.
func countActivity(ctx context.Context, q activityQuerier, partition string, lo, hi time.Time, sc activityScan) ([]activityGroup, bool, error) {
	stmt, args := buildActivitySQLFor(partition, sc.stmtTimeout, lo, hi, sc.deny, sc.allow)
	sctx, cancel := context.WithTimeout(ctx, sc.stmtTimeout+activityStmtGrace)
	defer cancel()
	rows, err := q.QueryContext(sctx, stmt, args...)
	if err != nil {
		return nil, false, activityStmtError(partition, err)
	}
	defer rows.Close()
	var out []activityGroup
	for rows.Next() {
		if len(out) >= activityMaxGroups {
			return out, true, nil
		}
		var g activityGroup
		if err := rows.Scan(&g.schema, &g.table, &g.etype, &g.n); err != nil {
			return nil, false, err
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, false, activityStmtError(partition, err)
	}
	return out, false, nil
}

func activityStmtError(partition string, err error) error {
	if partition == "" {
		return fmt.Errorf("count the window: %w", err)
	}
	return fmt.Errorf("count partition %s: %w", partition, err)
}

// foldActivityGroups adds rows to agg; false once agg's group cap tripped.
func foldActivityGroups(agg *activityAgg, rows []activityGroup) bool {
	for _, g := range rows {
		if !agg.add(g.schema, g.table, g.etype, g.n) {
			return false
		}
	}
	return true
}
