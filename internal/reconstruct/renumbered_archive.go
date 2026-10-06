package reconstruct

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The window's archived hours (#2186).
//
// The bounded check (ReadWindow.check) reads the live binlog_events table.
// The read it guards also takes hours rotation already moved into Parquet
// archives, with the same position filter, so a numbering that started over
// inside an archived hour would be dropped there unseen. checkArchives asks
// the same question of those archives.
//
// archive_state alone cannot answer it. Its newest position is per FILE (all
// tables) and is a maximum: a file whose rows after the mark run past the
// mark and then start again from binlog.000001 records the same newest
// position as one that never started again. What it does prove is the other
// direction: a file whose newest event_id is at or below the mark's holds
// nothing indexed after the snapshot, so nothing to look at. Every other file
// in the window (and every file with no record) is read: only its small
// columns, for this one table, which the read also scans, so the check never
// costs more than the read. A file found clean as a whole is remembered.

// archiveProbe asks the question of one archive file; archiveEventByID reads
// one event of one. Variables so unit tests can count and stub the reads.
var (
	archiveProbe     = parquetquery.FirstBelowMark
	archiveEventByID = parquetquery.EventByID
)

// archiveSpans remembers, per mark, table and archive file (a file archived
// again, with archived_at restamped, is another key), what a whole-file read
// found: nothing, or the earliest and the latest change below the mark by
// recorded time. A later read whose window ends before the earliest, or
// starts after the latest, needs no file read; one whose window holds either
// refuses with it. One per process; the least recently used entry goes first.
var archiveSpans = newLRU[archiveSpanKey, parquetquery.BelowMarkSpan](archiveCacheCap)

// archiveCacheCap bounds archiveSpans and archiveMarks.
const archiveCacheCap = 4096

type archiveSpanKey struct {
	db            *sql.DB
	mark          string
	schema, table string
	file          string
	archivedAt    int64
}

// windowArchive is one archive_state row the window reaches.
type windowArchive struct {
	partition  string
	file       string // local path or s3:// URL; "" when neither is reachable
	maxEventID sql.Null[uint64]
	archivedAt int64
}

// unreadableNote is the note for archived hours of the window this process
// cannot open.
func unreadableNote(hours []string) string {
	more := ""
	if len(hours) > 1 {
		more = fmt.Sprintf(" and %d more", len(hours)-1)
	}
	return uncheckedNote(fmt.Sprintf("the archived hour %s%s that this read reaches has no file this process can open (no local copy, and no S3 copy, or one that is not there)",
		hours[0], more))
}

// checkArchives runs the check over the archives of the window from floor
// to w.Until. A change found refuses (startedOverErr); an archive in the
// window that cannot be read gives the unchecked note, after every other
// archive was asked (a refusal elsewhere still wins).
func (w *ReadWindow) checkArchives(ctx context.Context, db *sql.DB, m *EventMark, floor time.Time) (string, error) {
	archives, err := loadWindowArchives(ctx, db, floor, w.Until)
	if err != nil {
		return "", err
	}
	var unreadable []string
	for _, a := range archives {
		if a.maxEventID.Valid && a.maxEventID.V <= m.ID {
			continue // nothing in it was indexed after the snapshot
		}
		if a.file == "" {
			unreadable = append(unreadable, a.partition)
			continue
		}
		e, found, err := w.probeArchive(ctx, db, m, a, floor)
		if errors.Is(err, parquetquery.ErrArchiveObjectMissing) {
			unreadable = append(unreadable, a.partition)
			continue
		}
		if err != nil {
			return "", fmt.Errorf("read the archived hour %s for %s.%s's changes after the snapshot's event mark: %w", a.partition, w.Schema, w.Table, err)
		}
		if found {
			return "", startedOverErr(e, m)
		}
	}
	if len(unreadable) > 0 {
		return unreadableNote(unreadable), nil
	}
	return "", nil
}

// probeArchive asks one archive file for a change below the mark inside this
// read's window [floor, w.Until]. The whole file is read once per mark,
// table and file (archiveSpans); this window is then judged by the earliest
// and the latest such change, and only a window strictly between the two is
// read again, bounded to it.
func (w *ReadWindow) probeArchive(ctx context.Context, db *sql.DB, m *EventMark, a windowArchive, floor time.Time) (indexedEvent, bool, error) {
	key := archiveSpanKey{db: db, mark: m.Encode(), schema: w.Schema, table: w.Table, file: a.file, archivedAt: a.archivedAt}
	q := parquetquery.BelowMark{Schema: w.Schema, Table: w.Table, AfterID: m.ID, MarkFile: m.File, MarkEnd: m.End}
	if base := BinlogBaseName(m.File); base != m.File {
		q.Base = base
	}
	span, ok := archiveSpans.get(key)
	if !ok {
		var err error
		if span, err = archiveProbe(ctx, a.file, q); err != nil {
			return indexedEvent{}, false, err
		}
		archiveSpans.put(key, span)
	}
	if !span.Found {
		return indexedEvent{}, false, nil
	}
	in := func(t time.Time) bool { return !t.After(w.Until) && (floor.IsZero() || !t.Before(floor)) }
	asEvent := func(r parquetquery.BelowMarkRow) indexedEvent {
		return indexedEvent{id: r.EventID, file: r.File, end: r.End}
	}
	switch {
	case span.Earliest.At.After(w.Until), !floor.IsZero() && span.Latest.At.Before(floor):
		return indexedEvent{}, false, nil
	case in(span.Earliest.At):
		return asEvent(span.Earliest), true, nil
	case in(span.Latest.At):
		return asEvent(span.Latest), true, nil
	}
	// The earliest is before the window and the latest after it: one in
	// between may or may not be inside.
	q.From, q.Until = floor, w.Until
	inner, err := archiveProbe(ctx, a.file, q)
	return asEvent(inner.Earliest), inner.Found, err
}

// markStatus is what the archives say about an event mark that is no longer
// in the live index (#2186).
type markStatus int

const (
	markNoArchive  markStatus = iota // no archive may hold it: rotated without one, or never archived
	markArchived                     // an archive holds it, the same event
	markOtherEvent                   // an archive holds its id as another event (a rebuilt index)
	markAbsent                       // the archives that could hold it do not: deleted before rotation
	markUnreadable                   // an archive that could hold it cannot be opened
)

// markProbeLimit bounds how many archive files markInArchives opens.
const markProbeLimit = 48

// archiveMarks remembers markInArchives' answers that are about the files
// themselves (archived, another event, absent), per index and mark.
var archiveMarks = newLRU[archiveMarkKey, markStatus](archiveCacheCap)

type archiveMarkKey struct {
	db   *sql.DB
	mark string
}

// markInArchives looks for the mark's own event in the archives, so the
// archived window is checked only from a mark the index can still vouch for:
// rotation archives an hour before dropping it, and the resume cleanup of a
// restarted capture deletes events (the mark among them) that it captures
// again under new ids at their old positions, which would read as a
// numbering that started over. The files that may hold it are those whose
// newest event id is at or above the mark's, or not recorded, smallest first.
func markInArchives(ctx context.Context, db *sql.DB, m *EventMark) (markStatus, string, error) {
	key := archiveMarkKey{db: db, mark: m.Encode()}
	if st, ok := archiveMarks.get(key); ok {
		return st, "", nil
	}
	rows, err := db.QueryContext(ctx, `SELECT partition_name, local_path, s3_bucket, s3_key FROM archive_state
		WHERE max_event_id >= ? OR max_event_id IS NULL
		ORDER BY max_event_id IS NULL, max_event_id, partition_name LIMIT ?`, m.ID, markProbeLimit+1)
	if err != nil {
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && me.Number == 1054 {
			rows, err = db.QueryContext(ctx, `SELECT partition_name, local_path, s3_bucket, s3_key FROM archive_state
				ORDER BY partition_name LIMIT ?`, markProbeLimit+1)
		}
		if errors.As(err, &me) && me.Number == 1146 {
			return markNoArchive, "", nil
		}
		if err != nil {
			return 0, "", fmt.Errorf("read archive_state: %w", err)
		}
	}
	type candidate struct{ partition, file string }
	var cands []candidate
	for rows.Next() {
		var c candidate
		var local, bucket, k sql.NullString
		if err := rows.Scan(&c.partition, &local, &bucket, &k); err != nil {
			rows.Close()
			return 0, "", fmt.Errorf("read archive_state: %w", err)
		}
		c.file = archiveFileOf(local.String, bucket.String, k.String)
		cands = append(cands, c)
	}
	rerr := rows.Err()
	rows.Close()
	if rerr != nil {
		return 0, "", fmt.Errorf("read archive_state: %w", rerr)
	}
	if len(cands) == 0 {
		return markNoArchive, "", nil
	}
	var unreadable []string
	for i, c := range cands {
		if i == markProbeLimit {
			return markUnreadable, uncheckedNote(fmt.Sprintf("the event the snapshot's event mark names is not in the live index, and more than %d archives could hold it", markProbeLimit)), nil
		}
		if c.file == "" {
			unreadable = append(unreadable, c.partition)
			continue
		}
		row, found, err := archiveEventByID(ctx, c.file, m.ID)
		if errors.Is(err, parquetquery.ErrArchiveObjectMissing) {
			unreadable = append(unreadable, c.partition)
			continue
		}
		if err != nil {
			return 0, "", fmt.Errorf("look for the event the snapshot's event mark names in the archived hour %s: %w", c.partition, err)
		}
		if !found {
			continue
		}
		st := markArchived
		if row.File != m.File || row.End != m.End {
			st = markOtherEvent
		}
		archiveMarks.put(key, st)
		return st, "", nil
	}
	if len(unreadable) > 0 {
		return markUnreadable, unreadableNote(unreadable), nil
	}
	archiveMarks.put(key, markAbsent)
	return markAbsent, "", nil
}

// archiveFileOf is the file of an archive_state row this process can open:
// its local path when that exists here, else its S3 copy; "" when neither.
func archiveFileOf(local, bucket, key string) string {
	switch {
	case local != "" && fileExists(local):
		return local
	case bucket != "" && key != "":
		return "s3://" + bucket + "/" + strings.TrimPrefix(key, "/")
	}
	return ""
}

// loadWindowArchives reads the archive_state rows whose file can hold a
// change recorded between floor (zero: no lower bound) and until: an hour
// label from floor's hour on, whose content starts at or before until (its
// label, or its recorded oldest change when that is earlier, #1037). A row's
// file is its local path when that exists here, else its S3 copy. No
// archive_state table: none. A table an older build created and nothing
// migrated yet is read without the newer columns, every file then read.
func loadWindowArchives(ctx context.Context, db *sql.DB, floor, until time.Time) ([]windowArchive, error) {
	lower := "p_"
	if !floor.IsZero() {
		lower = floor.UTC().Truncate(time.Hour).Format("p_2006010215")
	}
	upper := until.UTC().Truncate(time.Hour).Format("p_2006010215")
	const cols = `partition_name, local_path, s3_bucket, s3_key, UNIX_TIMESTAMP(archived_at)`
	// Hours after the window's end are left out on the server, except one
	// whose content starts earlier than its label (the rule below).
	rows, err := db.QueryContext(ctx, `SELECT `+cols+`, max_event_id, min_event_ts FROM archive_state
		WHERE partition_name >= ? AND (partition_name <= ? OR min_event_ts <= ?)`, lower, upper, until.UTC())
	legacy := false
	if err != nil {
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && me.Number == 1054 {
			legacy = true
			rows, err = db.QueryContext(ctx, `SELECT `+cols+` FROM archive_state WHERE partition_name >= ?`, lower)
		}
	}
	if err != nil {
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && me.Number == 1146 {
			return nil, nil
		}
		return nil, fmt.Errorf("read archive_state: %w", err)
	}
	defer rows.Close()
	var out []windowArchive
	for rows.Next() {
		var a windowArchive
		var local, bucket, key sql.NullString
		var archivedAt sql.NullString
		var minTS sql.NullTime
		if legacy {
			err = rows.Scan(&a.partition, &local, &bucket, &key, &archivedAt)
		} else {
			err = rows.Scan(&a.partition, &local, &bucket, &key, &archivedAt, &a.maxEventID, &minTS)
		}
		if err != nil {
			return nil, fmt.Errorf("read archive_state: %w", err)
		}
		label, ok := query.ParsePartitionName(a.partition)
		if !ok {
			continue // not an hour: no read routes such a file by hour either
		}
		start := label
		if minTS.Valid && minTS.Time.Before(start) {
			start = minTS.Time
		}
		if start.After(until) {
			continue
		}
		if t, err := parseUnixSeconds(archivedAt.String); err == nil {
			a.archivedAt = t.Unix()
		}
		a.file = archiveFileOf(local.String, bucket.String, key.String)
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read archive_state: %w", err)
	}
	return out, nil
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// lru is a small least-recently-used map, safe for concurrent use.
type lru[K comparable, V any] struct {
	mu  sync.Mutex
	cap int
	ll  *list.List
	m   map[K]*list.Element
}

type lruEntry[K comparable, V any] struct {
	k K
	v V
}

func newLRU[K comparable, V any](capacity int) *lru[K, V] {
	return &lru[K, V]{cap: capacity, ll: list.New(), m: map[K]*list.Element{}}
}

func (c *lru[K, V]) get(k K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[k]; ok {
		c.ll.MoveToFront(e)
		return e.Value.(lruEntry[K, V]).v, true
	}
	var zero V
	return zero, false
}

func (c *lru[K, V]) put(k K, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[k]; ok {
		e.Value = lruEntry[K, V]{k, v}
		c.ll.MoveToFront(e)
		return
	}
	c.m[k] = c.ll.PushFront(lruEntry[K, V]{k, v})
	if c.ll.Len() > c.cap {
		last := c.ll.Back()
		c.ll.Remove(last)
		delete(c.m, last.Value.(lruEntry[K, V]).k)
	}
}

// reset empties the cache (tests).
func (c *lru[K, V]) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	c.m = map[K]*list.Element{}
}
