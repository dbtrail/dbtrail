package reconstruct

import (
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

// archiveProbe asks the question of one archive file; a variable so unit
// tests can count and stub the reads.
var archiveProbe = parquetquery.FirstBelowMark

// archiveClean remembers the archive files a check read whole and found
// clean for a mark and a table. A file archived again (archived_at
// restamped) is another key. One per process, bounded like renumberChecked.
var archiveClean = struct {
	sync.Mutex
	m map[archiveCleanKey]bool
}{m: map[archiveCleanKey]bool{}}

type archiveCleanKey struct {
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
		if err != nil {
			return "", fmt.Errorf("read the archived hour %s for %s.%s's changes after the snapshot's event mark: %w", a.partition, w.Schema, w.Table, err)
		}
		if found {
			return "", startedOverErr(e, m)
		}
	}
	if len(unreadable) > 0 {
		more := ""
		if len(unreadable) > 1 {
			more = fmt.Sprintf(" and %d more", len(unreadable)-1)
		}
		return uncheckedNote(fmt.Sprintf("the archived hour %s%s that this read reaches has no file this process can open (no local copy, no S3 copy recorded)",
			unreadable[0], more)), nil
	}
	return "", nil
}

// probeArchive asks one archive file: first over the whole file, which is
// remembered when clean; a change found there is asked again of this read's
// own window, as the live check re-asks a stretch past its end.
func (w *ReadWindow) probeArchive(ctx context.Context, db *sql.DB, m *EventMark, a windowArchive, floor time.Time) (indexedEvent, bool, error) {
	key := archiveCleanKey{db: db, mark: m.Encode(), schema: w.Schema, table: w.Table, file: a.file, archivedAt: a.archivedAt}
	archiveClean.Lock()
	clean := archiveClean.m[key]
	archiveClean.Unlock()
	if clean {
		return indexedEvent{}, false, nil
	}
	q := parquetquery.BelowMark{Schema: w.Schema, Table: w.Table, AfterID: m.ID, MarkFile: m.File, MarkEnd: m.End}
	if base := BinlogBaseName(m.File); base != m.File {
		q.Base = base
	}
	_, found, err := archiveProbe(ctx, a.file, q)
	if err != nil || !found {
		if err == nil {
			archiveClean.Lock()
			if len(archiveClean.m) >= renumberCheckedCap {
				archiveClean.m = map[archiveCleanKey]bool{}
			}
			archiveClean.m[key] = true
			archiveClean.Unlock()
		}
		return indexedEvent{}, false, err
	}
	q.From, q.Until = floor, w.Until
	r, found, err := archiveProbe(ctx, a.file, q)
	return indexedEvent{id: r.EventID, file: r.File, end: r.End}, found, err
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
		switch {
		case local.String != "" && fileExists(local.String):
			a.file = local.String
		case bucket.String != "" && key.String != "":
			a.file = "s3://" + bucket.String + "/" + strings.TrimPrefix(key.String, "/")
		}
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
