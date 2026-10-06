package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

// CoarseSinceFloor is the time bound a fetch anchored on a binlog position
// (Options.SincePos) actually applies for Options.Since: the start of that
// hour, less one more hour (#797). buildQuery uses it for the index and
// PartitionHeads uses it to know which partitions that bound leaves out; the
// two must be the same instant, which is why it has one definition.
func CoarseSinceFloor(since time.Time) time.Time {
	return since.Truncate(time.Hour).Add(-time.Hour)
}

// partitionHead is one partition of binlog_events and its newest row.
type partitionHead struct {
	// name is the partition's name; "" for a table that is not partitioned.
	name string
	// lower is the lowest event_timestamp the partition can hold: the upper
	// bound of the partition before it. open is set for the first partition,
	// which has no lower bound.
	lower time.Time
	open  bool
	// empty: the partition holds no row.
	empty bool
	// pos is where the row with the highest event_id starts. unknown is set
	// when that row carries no usable coordinate.
	pos     BinlogPos
	unknown bool
}

// PartitionHeads answers one question for a fetch that starts at a binlog
// position: how far back in TIME can an event after that position be (#2138).
//
// # Why the question exists
//
// Such a fetch is exact by position (Options.SincePos) and also carries a time
// (Options.Since), which buildQuery turns into a floor on event_timestamp so
// the index and the archives can be pruned. Every caller derives that time
// from a clock that is not the source's: a refresh stamps what it writes with
// its own clock, a dump with the dump host's. event_timestamp is when the
// statement ran ON THE SOURCE. Whenever the index receives an event later
// than it ran (capture catching up after an outage, a source that is itself
// a delayed replica, a transaction open for hours) the event sits after the
// position and BEFORE the floor: the fetch drops it, with no error, and a
// refresh then publishes a snapshot without that change.
//
// # How it is answered
//
// By looking, not by assuming a margin. The row with the highest event_id of
// a partition is the last one written into it, so under the premise that
// ascending event_id is binlog order its position is the highest the
// partition holds. A partition whose newest row starts before the anchor
// holds nothing after the anchor and can be left out; one whose newest row is
// at or after it may, and the fetch must reach it. That costs one primary-key
// seek per partition, once (measured on MySQL 8.4.9: see the #2138 entry in
// docs/CHANGELOG.md).
//
// The premise is the one ResolveSnapshotCut already stakes the cut on. It
// holds for an index a stream writes and for binlog files indexed in order.
// It does not hold while `bintrail index` adds files to an index a stream
// writes: those rows get the newest ids with old positions and can sit on top
// of a partition, hiding later positions below them. index_state says when
// that last happened, and a fetch anchored before it treats every partition
// as one that may hold a later event.
//
// The answer is only ever an EARLIER time than the caller's: the floor moves
// back or stays.
//
// # Archives
//
// A late event lands in an OLD partition, and old partitions are the next to
// be rotated out. Once its partition is archived and dropped, no partition
// shows it (#2152). The archive's own record in archive_state does: rotation
// records the newest binlog coordinate each file holds (folded over every
// row, so it does not depend on the event_id premise above), and an archive
// whose newest coordinate is at or after what the snapshot already saw makes
// the fetch reach its hour. archivesBelow says what "already saw" is, and why
// it is the previous refresh's cut when the caller knows it rather than the
// table's own anchor. An archive with no record (written before the record
// existed, or registered by a path that did not read the file) is reached
// when it was written after the fetch's own time, less a clock margin: the
// only archives that can hold an event indexed after the snapshot. Older ones
// cannot, and counting them would move every update back by the whole
// retention.
//
// What it still cannot see: an hour rotated out WITHOUT an archive, and an
// archive whose archive_state row is gone (`archive reconcile --repair`
// re-registers it).
//
// A value is a snapshot of the index. Load it AFTER the fetch's upper bound is
// fixed (a refresh: after its cut), or a late event indexed in between is
// under the bound and not in the picture.
type PartitionHeads struct {
	parts []partitionHead
	// streamCaptured and lastFileIndexed are the two facts the premise is
	// checked against: whether a stream writes this index, and when a binlog
	// file was last being indexed into it (zero: never).
	streamCaptured  bool
	lastFileIndexed time.Time
	// fileIndexingUnfinished: index_state holds a run with no completed_at.
	fileIndexingUnfinished bool
	// archives are the rows of archive_state (#2152).
	archives []archiveHead
}

// archiveHead is one archive_state row as the floor reads it.
type archiveHead struct {
	name string
	// lower is the oldest event_timestamp the file can hold: its partition's
	// hour, or min_event_ts when that is earlier (a first-partition archive
	// holds every older row).
	lower time.Time
	// recorded: max_event_id is set, so hasPos/pos say exactly whether the
	// file holds an event an anchored fetch returns. hasPos is false for a
	// recorded file in which no row carries such a coordinate.
	recorded bool
	hasPos   bool
	pos      BinlogPos
	// archivedAt is when the row was written, in UTC by the index server's
	// clock.
	archivedAt time.Time
}

// archiveWrittenMargin is how much earlier than a fetch's own time an archive
// with no record must have been written to be left out. The two times come
// from different clocks (the index server's and the host that stamped the
// snapshot), and erring long only widens a fetch.
const archiveWrittenMargin = time.Hour

// fileIndexingMargin is how much earlier than a fetch's own time a file
// indexing run must have ended to be counted as over before it. The two times
// come from different clocks (the index server's and the host that stamped
// the snapshot), and erring long only widens a fetch.
const fileIndexingMargin = time.Hour

// isUnknownPartitionErr reports MySQL's ER_UNKNOWN_PARTITION (1735): a
// statement named a partition that is no longer there. Rotation drops old
// partitions while a refresh runs, and the old ones are the ones read here.
func isUnknownPartitionErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1735
}

// partitionReadAttempts bounds the re-reads after a partition went away
// between being listed and being read. One rotation pass drops its
// partitions in a single statement, so a second read sees the new list.
const partitionReadAttempts = 3

// LoadPartitionHeads reads the picture PartitionHeads answers from.
func LoadPartitionHeads(ctx context.Context, db *sql.DB) (*PartitionHeads, error) {
	pictureLoads.Add(1)
	var h *PartitionHeads
	var err error
	for range partitionReadAttempts {
		if h, err = loadPartitionHeadsOnce(ctx, db); !isUnknownPartitionErr(err) {
			break
		}
	}
	return h, err
}

func loadPartitionHeadsOnce(ctx context.Context, db *sql.DB) (*PartitionHeads, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, PARTITION_DESCRIPTION
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'
		ORDER BY PARTITION_ORDINAL_POSITION`)
	if err != nil {
		return nil, fmt.Errorf("list the partitions of binlog_events: %w", err)
	}
	h := &PartitionHeads{}
	prev, first := time.Time{}, true
	for rows.Next() {
		var name, desc sql.NullString
		if err := rows.Scan(&name, &desc); err != nil {
			rows.Close()
			return nil, fmt.Errorf("list the partitions of binlog_events: %w", err)
		}
		p := partitionHead{name: name.String, lower: prev, open: first}
		first = false
		if name.Valid && desc.String != "MAXVALUE" {
			secs, perr := strconv.ParseInt(desc.String, 10, 64)
			if perr != nil {
				rows.Close()
				return nil, fmt.Errorf("partition %s of binlog_events has a bound this build cannot read (%q)", name.String, desc.String)
			}
			prev = time.Unix(secs-mysqlToSecondsConst, 0).UTC()
		}
		h.parts = append(h.parts, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("list the partitions of binlog_events: %w", err)
	}
	rows.Close()
	if len(h.parts) == 0 {
		return nil, errors.New("binlog_events is not in this index database")
	}

	// The newest row of every partition, a few statements for all of them:
	// one statement per partition is one round trip per partition, about 720
	// on an index that keeps a month, and that is paid by every reader that
	// loads its own picture (measured: see the #2138 entry in the changelog).
	for i := range h.parts {
		h.parts[i].empty = true
	}
	for lo := 0; lo < len(h.parts); lo += headsPerStatement {
		hi := min(lo+headsPerStatement, len(h.parts))
		var q strings.Builder
		for i := lo; i < hi; i++ {
			if i > lo {
				q.WriteString(" UNION ALL ")
			}
			// The primary key leads with event_id, so each branch is one
			// seek from the end of its partition's own index.
			fmt.Fprintf(&q, "(SELECT %d AS part, binlog_file, start_pos FROM binlog_events%s ORDER BY event_id DESC LIMIT 1)",
				i, partitionClause([]string{h.parts[i].name}))
		}
		rows, err := db.QueryContext(ctx, q.String())
		if err != nil {
			return nil, fmt.Errorf("read the newest event of each partition: %w", err)
		}
		for rows.Next() {
			var i int
			var file string
			var pos uint64
			if err := rows.Scan(&i, &file, &pos); err != nil {
				rows.Close()
				return nil, fmt.Errorf("read the newest event of each partition: %w", err)
			}
			if i < lo || i >= hi {
				rows.Close()
				return nil, fmt.Errorf("read the newest event of each partition: the server answered for partition %d, outside %d to %d", i, lo, hi-1)
			}
			h.parts[i].empty = false
			if file == "" {
				h.parts[i].unknown = true
			} else {
				h.parts[i].pos = BinlogPos{File: file, Pos: pos}
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("read the newest event of each partition: %w", err)
		}
	}

	// After the partitions, on purpose (#2152): rotation registers an archive
	// BEFORE it drops the partition, so a partition dropped between the two
	// reads is in this one. Read the other way round, it would be in neither.
	if h.archives, err = loadArchiveHeads(ctx, db); err != nil {
		return nil, err
	}

	// After the heads, on purpose: a file indexing run that put a row on top
	// of a partition before the read above had already recorded itself here.
	if h.streamCaptured, err = StreamCaptured(ctx, db); err != nil {
		return nil, err
	}
	// A run with no completed_at is still going as far as the index knows.
	// It is kept apart from the time of the last finished one, not folded
	// into it as "now": while files are being indexed is exactly when the
	// newest row of a partition says nothing about the others, for a fetch
	// of any age.
	var last sql.NullTime
	var unfinished int64
	err = db.QueryRowContext(ctx, "SELECT MAX(completed_at), COUNT(*) - COUNT(completed_at) FROM index_state").Scan(&last, &unfinished)
	if err != nil && !isMissingTableErr(err) {
		return nil, fmt.Errorf("read index_state: %w", err)
	}
	if err == nil {
		h.fileIndexingUnfinished = unfinished > 0
		if last.Valid {
			h.lastFileIndexed = last.Time.UTC()
		}
		if h.streamCaptured && h.fileIndexingUnfinished {
			warnFileIndexingInProgress(ctx, db)
		}
	}
	return h, nil
}

// loadArchiveHeads reads archive_state for the floor: every row, whatever
// backend holds the file (a local path, an S3 object, both). The time of each
// row is computed by the server as an age, so the session time zone the
// DATETIME was written in cancels out. No archive_state table is an index
// that never archived; an archive_state an older build created and nothing
// migrated yet (1054) is read without the record, every row "not recorded".
func loadArchiveHeads(ctx context.Context, db *sql.DB) ([]archiveHead, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT partition_name, min_event_ts, max_event_id, max_binlog_file, max_start_pos,
		       TIMESTAMPDIFF(SECOND, archived_at, NOW()), UTC_TIMESTAMP()
		FROM archive_state`)
	legacy := false
	if err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1054 {
			legacy = true
			rows, err = db.QueryContext(ctx, `
				SELECT partition_name, TIMESTAMPDIFF(SECOND, archived_at, NOW()), UTC_TIMESTAMP()
				FROM archive_state`)
		}
	}
	if err != nil {
		if isMissingTableErr(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read archive_state: %w", err)
	}
	defer rows.Close()
	var out []archiveHead
	for rows.Next() {
		var name string
		var minTS sql.NullTime
		var maxID, maxPos sql.Null[uint64]
		var maxFile sql.NullString
		var age int64
		var now time.Time
		if legacy {
			err = rows.Scan(&name, &age, &now)
		} else {
			err = rows.Scan(&name, &minTS, &maxID, &maxFile, &maxPos, &age, &now)
		}
		if err != nil {
			return nil, fmt.Errorf("read archive_state: %w", err)
		}
		// A name that is not an hour is skipped, as the planner skips it: no
		// fetch routes such a file by hour either.
		label, ok := ParsePartitionName(name)
		if !ok {
			continue
		}
		a := archiveHead{name: name, lower: label, recorded: maxID.Valid,
			archivedAt: now.UTC().Add(-time.Duration(age) * time.Second)}
		if minTS.Valid && minTS.Time.Before(label) {
			a.lower = minTS.Time.UTC()
		}
		if maxID.Valid && maxFile.Valid && maxFile.String != "" {
			a.hasPos, a.pos = true, BinlogPos{File: maxFile.String, Pos: maxPos.V}
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read archive_state: %w", err)
	}
	return out, nil
}

// headsPerStatement is how many partitions one statement of
// loadPartitionHeadsOnce reads the newest row of.
const headsPerStatement = 250

// fileIndexingWarnEvery spaces the warning below: every fetch that loads a
// picture would otherwise repeat it.
const fileIndexingWarnEvery = 10 * time.Minute

var fileIndexingWarned atomic.Int64 // unix seconds of the last warning

// warnFileIndexingInProgress tells the operator that a `bintrail index` run
// is recorded as still going on an index a stream writes. It is not an error
// while the run really is going. A run that crashed leaves the same record
// for good, and from then on every fetch looks through every older partition:
// correct, slower, and invisible without this line.
func warnFileIndexingInProgress(ctx context.Context, db *sql.DB) {
	var file string
	var started, now time.Time
	err := db.QueryRowContext(ctx, `SELECT binlog_file, started_at, UTC_TIMESTAMP() FROM index_state
		WHERE completed_at IS NULL ORDER BY started_at LIMIT 1`).Scan(&file, &started, &now)
	if err != nil {
		// No such run (the usual answer), or the read failed: the warning is
		// advice, and the widening it explains happens either way.
		return
	}
	last := fileIndexingWarned.Load()
	if now.Unix()-last < int64(fileIndexingWarnEvery/time.Second) || !fileIndexingWarned.CompareAndSwap(last, now.Unix()) {
		return
	}
	slog.Warn(fileIndexingInProgressWarning(file, started, now))
}

// fileIndexingInProgressWarning is the text of that warning.
func fileIndexingInProgressWarning(file string, started, now time.Time) string {
	return fmt.Sprintf("the index records a `bintrail index` run that has not finished: file %s, started %s (%s ago). "+
		"While it is recorded as running, every snapshot update and every read that continues from a snapshot looks through all the older hours of the index instead of the few it needs, which is slower and loses nothing. "+
		"If that run is still going, this stops by itself when it ends. If it crashed or was stopped, mark it as ended: "+
		"UPDATE index_state SET status = 'failed', completed_at = UTC_TIMESTAMP() WHERE binlog_file = '%s' AND completed_at IS NULL; "+
		"That keeps every change the run had already indexed and only records that it stopped; the rest of that file stays unindexed. "+
		"Do not run `bintrail index` on that file again: it starts the file from its beginning and would index the same changes a second time.",
		file, started.UTC().Format(time.RFC3339), now.Sub(started).Round(time.Minute), strings.ReplaceAll(file, "'", "''"))
}

// firstPartitionEnd is the upper bound of the first partition: every row
// older than it is stored in that partition, whatever hour it belongs to,
// because the first partition has no lower bound. ok is false when the table
// has one partition or none.
func (h *PartitionHeads) firstPartitionEnd() (time.Time, bool) {
	if len(h.parts) < 2 {
		return time.Time{}, false
	}
	return h.parts[1].lower, true
}

// partitionClause is the ` PARTITION (...)` selector for names, or "" for a
// table that is not partitioned (its one entry is named "").
func partitionClause(names []string) string {
	if len(names) == 0 || names[0] == "" {
		return ""
	}
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = "`" + strings.ReplaceAll(n, "`", "``") + "`"
	}
	return " PARTITION (" + strings.Join(quoted, ", ") + ")"
}

// orderProven reports whether, for a fetch whose own time is since, the
// newest row of a partition is known to carry its highest position.
func (h *PartitionHeads) orderProven(since time.Time) bool {
	return writersKeepBinlogOrder(h.streamCaptured, h.fileIndexingUnfinished, h.lastFileIndexed, since)
}

// below names the partitions that the floor of a fetch at since leaves out,
// in whole or in part, and that may hold an event at or after anchor.
func (h *PartitionHeads) below(since time.Time, anchor BinlogPos) []string {
	floor := CoarseSinceFloor(since)
	proven := h.orderProven(since)
	var names []string
	for _, p := range h.parts {
		if p.empty || !(p.open || p.lower.Before(floor)) {
			continue
		}
		if !proven || p.unknown || anchor.AtOrBefore(p.pos) {
			names = append(names, p.name)
		}
	}
	return names
}

// archivesBelow is below for the archives (#2152): the oldest time an
// archive the floor of a fetch at since leaves out can hold, among those that
// may hold an event at or after anchor that the snapshot has not seen, and how
// many there are. Zero time: no such archive.
//
// checked is Options.ArchivesCheckedThrough, the cut of the refresh that
// published the snapshot. Capture indexes in commit order, so every row
// indexed after that refresh ran sits after its cut, and every row between the
// table's own anchor and the cut was already in the index when that refresh
// looked. A recorded archive is then read only when its newest change is at
// or after the cut, whenever it was written: that is what keeps a table with
// no changes, whose anchor stays old, from reading the archives rotation
// writes after every refresh.
//
// With no cut (an index `bintrail index` also wrote, where rows with old
// positions are indexed late; a snapshot that recorded none), a recorded
// archive is read when its newest change is at or after the anchor AND it was
// written after since, less the clock margin: any row the snapshot has not
// seen was indexed after it, into a file written after it. An archive with no
// record is read when it was written after since, in both cases.
func (h *PartitionHeads) archivesBelow(since time.Time, anchor BinlogPos, checked *BinlogPos) (from time.Time, n int) {
	floor := CoarseSinceFloor(since)
	writtenAfter := since.Add(-archiveWrittenMargin)
	seen := anchor
	if checked != nil && anchor.AtOrBefore(*checked) {
		seen = *checked
	}
	for _, a := range h.archives {
		if !a.lower.Before(floor) {
			continue
		}
		written := !a.archivedAt.Before(writtenAfter)
		var may bool
		switch {
		case !a.recorded:
			may = written
		case checked != nil:
			may = a.hasPos && seen.AtOrBefore(a.pos)
		default:
			may = written && a.hasPos && anchor.AtOrBefore(a.pos)
		}
		if !may {
			continue
		}
		n++
		if from.IsZero() || a.lower.Before(from) {
			from = a.lower
		}
	}
	return from, n
}

// SinceFor returns the time a fetch with these options must start from: its
// own Since, or the oldest event of its table in the partitions that may hold
// an event after its position and that its floor would leave out. Options
// without both Since and SincePos are returned as they are.
//
// An error means the question could not be answered. The caller must not go
// on with its own time: that is the silent loss this exists to stop.
//
// h may be a picture several fetches share, taken earlier in a run. When a
// partition it names was dropped since (rotation), the answer is read again
// from a fresh picture: what the dropped partition held is in an archive now
// and out of any picture's sight, the limit PartitionHeads states.
func (h *PartitionHeads) SinceFor(ctx context.Context, db *sql.DB, opts Options) (*time.Time, error) {
	since, _, err := h.sinceForWith(ctx, db, opts)
	return since, err
}

// sinceForWith is SinceFor plus the picture the answer was read from: h, or
// the fresh one a dropped partition made it load.
func (h *PartitionHeads) sinceForWith(ctx context.Context, db *sql.DB, opts Options) (*time.Time, *PartitionHeads, error) {
	used := h
	since, err := h.sinceFor(ctx, db, opts)
	for i := 1; isUnknownPartitionErr(err) && i < partitionReadAttempts; i++ {
		fresh, lerr := LoadPartitionHeads(ctx, db)
		if lerr != nil {
			return nil, nil, lerr
		}
		used = fresh
		since, err = fresh.sinceFor(ctx, db, opts)
	}
	return since, used, err
}

// runPicture is one PartitionHeads for the length of ONE short run: a
// recover-cascade asks for the children of every (foreign key, parent row),
// and loading the picture for each of those fetches cost more than the
// fetches (1,000 parents and three keys: about two minutes). It is loaded on
// first need and never refreshed, which is why it must not outlive a run: a
// picture older than a fetch's upper bound can miss a late event. A daemon
// that serves reads for hours loads its own per fetch instead.
type runPicture struct {
	once  sync.Once
	heads *PartitionHeads
	err   error
}

// SearchesBelowItsOwnFloor returns o marked as a fetch whose caller covers
// the hours below its time floor itself, so the engine must not look at the
// partitions for it (#2138).
//
// Legitimate for exactly one shape: the caller runs a SECOND fetch for the
// same table and position with no Since and Until at CoarseSinceFloor(Since),
// which searches every older hour by position alone, or proves from the index
// that no such search can find anything, and treats any failure of either as
// "changed" or as a refusal. The routed port's "did this table change since
// its snapshot" check is that caller: it asks per statement inside the
// capture process, under a two-second budget, and has its own one-row proof
// that makes the look at every partition a cost with nothing to add. A caller
// that only wants to skip the cost must not use this: without the second
// search it is the silent loss #2138 was.
func (o Options) SearchesBelowItsOwnFloor() Options {
	o.sinceSettled = true
	return o
}

// pictureLoads counts the pictures loaded by this process.
var pictureLoads atomic.Int64

// PictureLoads is how many times this process has loaded a PartitionHeads:
// a diagnostic, and what a test reads to show that a path loads none, or one.
func PictureLoads() int64 { return pictureLoads.Load() }

func (r *runPicture) get(ctx context.Context, db *sql.DB) (*PartitionHeads, error) {
	r.once.Do(func() { r.heads, r.err = LoadPartitionHeads(ctx, db) })
	return r.heads, r.err
}

// needsPicture reports whether a fetch with these options will read a
// PartitionHeads.
func needsPicture(opts Options) bool {
	return !opts.sinceSettled && opts.Since != nil && opts.SincePos != nil
}

// ForRun returns f for the use of one short run (one recover-cascade): when
// f is an *Engine, a copy that loads its PartitionHeads once and shares it
// between that run's fetches. A *MergedFetcher already does. Anything else
// is returned as it is.
func ForRun(f Fetcher) Fetcher {
	if e, ok := f.(*Engine); ok && e.run == nil {
		return &Engine{db: e.db, run: &runPicture{}}
	}
	return f
}

// settleSince is what every fetch runs before it reads: it replaces
// opts.Since with SinceFor's answer and marks the options so the same fetch
// does not ask again further down. heads may be nil (a picture is loaded).
// moved reports that the time changed, and firstEnd is the picture's
// firstPartitionEnd (zero when it has none).
func settleSince(ctx context.Context, db *sql.DB, opts *Options, heads *PartitionHeads) (moved bool, firstEnd time.Time, err error) {
	if opts.sinceSettled || opts.Since == nil || opts.SincePos == nil {
		return false, time.Time{}, nil
	}
	if heads == nil {
		if heads, err = LoadPartitionHeads(ctx, db); err != nil {
			return false, time.Time{}, fmt.Errorf("cannot tell how far back the changes after %s:%d reach: %w",
				opts.SincePos.File, opts.SincePos.Pos, err)
		}
	}
	since, used, err := heads.sinceForWith(ctx, db, *opts)
	if err != nil {
		return false, time.Time{}, err
	}
	moved = since != opts.Since
	opts.Since, opts.sinceSettled = since, true
	firstEnd, _ = used.firstPartitionEnd()
	return moved, firstEnd, nil
}

func (h *PartitionHeads) sinceFor(ctx context.Context, db *sql.DB, opts Options) (*time.Time, error) {
	if opts.Since == nil || opts.SincePos == nil {
		return opts.Since, nil
	}
	since := *opts.Since
	names := h.below(since, *opts.SincePos)
	archFrom, archN := h.archivesBelow(since, *opts.SincePos, opts.ArchivesCheckedThrough)
	if len(names) == 0 {
		if archN == 0 {
			return opts.Since, nil
		}
		h.logMoved(opts, since, archFrom, 0, archN)
		return &archFrom, nil
	}
	// The first row in time order, read off idx_row_lookup (schema_name,
	// table_name, event_timestamp): one seek per named partition.
	q := "SELECT event_timestamp FROM binlog_events" + partitionClause(names) + " WHERE event_timestamp < ?"
	args := []any{CoarseSinceFloor(since)}
	if opts.Schema != "" {
		q += " AND schema_name = ?"
		args = append(args, opts.Schema)
	}
	if opts.Table != "" {
		q += " AND table_name = ?"
		args = append(args, opts.Table)
	}
	q += " ORDER BY event_timestamp LIMIT 1"
	var oldest time.Time
	err := db.QueryRowContext(ctx, q, args...).Scan(&oldest)
	if errors.Is(err, sql.ErrNoRows) {
		if archN == 0 {
			return opts.Since, nil
		}
		h.logMoved(opts, since, archFrom, len(names), archN)
		return &archFrom, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find how far back the changes after %s:%d reach for %s.%s: %w",
			opts.SincePos.File, opts.SincePos.Pos, opts.Schema, opts.Table, err)
	}
	// An archive is not narrowed to the table (that would mean opening the
	// file here): its whole time range is reached.
	if archN > 0 && archFrom.Before(oldest) {
		oldest = archFrom
	}
	h.logMoved(opts, since, oldest, len(names), archN)
	return &oldest, nil
}

// logMoved says that a fetch starts earlier than its own time, and why.
func (h *PartitionHeads) logMoved(opts Options, since, from time.Time, partitions, archives int) {
	slog.Info("the fetch starts earlier than the time of the snapshot it continues: "+
		"the index or its archives hold events that may come after the snapshot's position and that ran on the source before that time",
		"schema", opts.Schema, "table", opts.Table,
		"snapshot_time", since.UTC().Format(time.RFC3339), "fetch_from", from.UTC().Format(time.RFC3339),
		"anchor", opts.SincePos.File+":"+strconv.FormatUint(opts.SincePos.Pos, 10),
		"partitions", partitions, "archives", archives, "order_proven", h.orderProven(since))
}
