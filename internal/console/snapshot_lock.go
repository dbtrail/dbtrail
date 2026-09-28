package console

import (
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// How a snapshot was locked when the database was read (#1380).
//
// Every table file records it in its footer (baseline.MetaKeyLockMode) and a
// snapshot updated from the recorded changes inherits it, so the answer for a
// snapshot is the worst of its tables: one table read with no locks makes the
// snapshot torn. Three rules, the same on the row and in the detail:
//
//   - No record is "unknown", never "consistent": every snapshot written
//     before the record existed has none.
//   - A table that was not looked at is not spoken for. Over S3 a footer is
//     one request per table, which the listing does not spend, so a snapshot
//     with a table only there has no answer, unless a table that WAS looked
//     at is torn: that is true of the snapshot whatever the others say.
//   - A footer that cannot be read counts as unknown.

// snapshotLocks folds the per-table answers of one snapshot.
type snapshotLocks struct {
	looked    int // tables whose footer was asked for
	torn      int
	unknown   int // no record, or a footer that could not be read
	notLooked int // tables nobody asked about (S3)
}

func (l *snapshotLocks) add(c baseline.ReadConsistency, read bool) {
	l.looked++
	switch {
	case !read:
		l.unknown++
	case c == baseline.ReadTorn:
		l.torn++
	case c != baseline.ReadConsistent:
		l.unknown++
	}
}

// verdict is the snapshot's word: torn | unknown | consistent, or "" when it
// cannot be said (no table looked at, or some not looked at and none torn).
func (l snapshotLocks) verdict() string {
	switch {
	case l.torn > 0:
		return baseline.ReadTorn.String()
	case l.looked == 0 || l.notLooked > 0:
		return ""
	case l.unknown > 0:
		return baseline.ReadUnknown.String()
	}
	return baseline.ReadConsistent.String()
}

// lockMemo remembers the lock record of local table files, so the listing
// does not open every footer of every snapshot on every load. A published
// table file is never modified, and the entry is keyed on the file's size and
// modification time as well as its path, so a different file at the same path
// is read again.
type lockMemo struct {
	mu sync.Mutex
	m  map[string]lockMemoEntry
}

type lockMemoEntry struct {
	size int64
	mod  time.Time
	lock baseline.ReadConsistency
}

// lockMemoCap bounds the memo. Past it the memo starts over: the entries of
// pruned snapshots are never asked for again and would otherwise stay.
const lockMemoCap = 20000

var snapshotLockMemo = &lockMemo{m: map[string]lockMemoEntry{}}

// of answers for one local table file. read is false when the file or its
// footer could not be read, which is logged and never remembered.
func (m *lockMemo) of(path string) (lock baseline.ReadConsistency, read bool) {
	fi, err := os.Stat(path)
	if err != nil {
		slog.Warn("console: a snapshot table file cannot be read, so how it was locked is not known",
			"path", path, "error", err)
		return baseline.ReadUnknown, false
	}
	m.mu.Lock()
	e, ok := m.m[path]
	m.mu.Unlock()
	if ok && e.size == fi.Size() && e.mod.Equal(fi.ModTime()) {
		return e.lock, true
	}
	md, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		slog.Warn("console: a snapshot table's footer cannot be read, so how it was locked is not known",
			"path", path, "error", err)
		return baseline.ReadUnknown, false
	}
	lock = baseline.ReadConsistencyOf(md)
	m.mu.Lock()
	if len(m.m) >= lockMemoCap {
		m.m = map[string]lockMemoEntry{}
	}
	m.m[path] = lockMemoEntry{size: fi.Size(), mod: fi.ModTime(), lock: lock}
	m.mu.Unlock()
	return lock, true
}
