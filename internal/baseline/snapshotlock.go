package baseline

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// How a snapshot was locked when the source was read (#1380).
//
// A snapshot taken with no locks can be torn: its rows were copied at
// different moments, so they may not agree with each other. Once written it
// looked exactly like a consistent one, and a fold carried it forward looking
// the same. The footer now records the lock mode of the read, and every file
// folded from it inherits the record.
//
// # Where the record lives, and why
//
// In the footer of every table file, not in a file beside _SUCCESS like the
// writer's signature (writersig.go) or the record of skipped views
// (viewsskipped.go). The lock mode is a property of the whole read, so one
// file per snapshot looks like the natural place. It is not, for one reason:
// a snapshot is not always the product of one read. A fold takes each table
// from the newest snapshot that holds it, and a table that did not change is
// carried forward as a hard link to the older snapshot's file (#1545). A
// record beside _SUCCESS has to be rebuilt by every producer from every
// snapshot it took a table from, and one producer that forgets publishes a
// torn table under a snapshot that says nothing. A record in the footer
// travels with the rows: a carried file IS the older snapshot's file, so it
// says what that snapshot said, with nothing to copy and nothing to forget.
// It also reaches the readers that matter most (verify, a restore) with no
// second read, because they open that footer already.
//
// # Three answers, never two
//
// The record is written for EVERY mode, not only the torn one. Writing it
// only for no-lock would make its absence mean two things that cannot be told
// apart: a snapshot written before the record existed, and one written after
// that was consistent. The first group is the one to doubt, since no-lock was
// the default before #1377. So absence is "unknown", and only a record clears
// a snapshot. ReadConsistencyOf is the one reader.
const MetaKeyLockMode = "bintrail.lock_mode"

// LockStampPGRepeatableRead is the MetaKeyLockMode value of a PostgreSQL
// snapshot. pgbaseline reads every table inside one REPEATABLE READ
// transaction, its workers adopting that transaction's exported snapshot, so
// the read is of one instant by construction and there is no mode to choose.
// It has a value of its own because no LockMode names what PostgreSQL did.
const LockStampPGRepeatableRead = "pg-repeatable-read"

// ReadConsistency says whether the rows of a snapshot's table are known to
// be of one instant. The zero value is ReadUnknown, on purpose: a value
// nobody set must never read as consistent.
type ReadConsistency int

const (
	// ReadUnknown: nothing on record says how the source was locked. Every
	// snapshot written before the record existed, a dump this program did
	// not run, and a record this program cannot read.
	ReadUnknown ReadConsistency = iota
	// ReadConsistent: the read was locked (or, for safe-no-lock, would have
	// stopped instead of writing a torn snapshot).
	ReadConsistent
	// ReadTorn: the read took no locks, by the operator's choice.
	ReadTorn
)

// String is the word used on every surface: unknown | consistent | torn.
func (c ReadConsistency) String() string {
	switch c {
	case ReadConsistent:
		return "consistent"
	case ReadTorn:
		return "torn"
	}
	return "unknown"
}

// isLockMode reports whether s is exactly one of the four lock modes.
func isLockMode(s string) bool {
	switch LockMode(s) {
	case LockModeFTWRL, LockModeLockAll, LockModeSafeNoLock, LockModeNoLock:
		return true
	}
	return false
}

// ReadConsistencyOfStamp reads one MetaKeyLockMode value. Only an exact value
// this program writes is an answer. Anything else is unknown: empty, another
// spelling, a value from a later version. It does not go through
// ParseLockMode, which reads "" as the default mode and would turn a snapshot
// with no record into a consistent one.
func ReadConsistencyOfStamp(stamp string) ReadConsistency {
	switch {
	case isLockMode(stamp) && LockMode(stamp).PointConsistent():
		return ReadConsistent
	case isLockMode(stamp):
		return ReadTorn
	case stamp == LockStampPGRepeatableRead:
		return ReadConsistent
	}
	return ReadUnknown
}

// ReadConsistencyOf answers for the rows of one file, from its footer.
func ReadConsistencyOf(md DumpMetadata) ReadConsistency {
	return ReadConsistencyOfStamp(md.LockMode)
}

// readConsistencyRank orders the answers from best to worst. Torn is worse
// than unknown: it is known to be what unknown may be.
var readConsistencyRank = map[ReadConsistency]int{ReadConsistent: 1, ReadUnknown: 2, ReadTorn: 3}

// WorstReadConsistency is the answer for several tables taken together: the
// worst of them. No table at all is unknown, never consistent.
func WorstReadConsistency(of ...ReadConsistency) ReadConsistency {
	if len(of) == 0 {
		return ReadUnknown
	}
	worst := ReadConsistent
	for _, c := range of {
		rank, ok := readConsistencyRank[c]
		if !ok {
			c, rank = ReadUnknown, readConsistencyRank[ReadUnknown]
		}
		if rank > readConsistencyRank[worst] {
			worst = c
		}
	}
	return worst
}

// LockModeMarkerFile is written into a mydumper output directory by the run
// that started mydumper, and names the lock mode it asked for. mydumper's own
// metadata file does not record it, and `bintrail dump` and `bintrail
// baseline` are two commands, so without this file the conversion cannot know.
//
// It is written only when the mode was really sent to mydumper
// (--sync-thread-lock-mode). A build too old for that flag, or one whose
// version could not be read, chooses its own mode, and what it chose is not
// on record: no file, and the snapshot reads as unknown.
const LockModeMarkerFile = "bintrail_dump_lock_mode"

// WriteLockModeMarker records in dir the lock mode mydumper was run with.
func WriteLockModeMarker(dir string, mode LockMode) error {
	if !isLockMode(string(mode)) {
		return fmt.Errorf("write %s: %q is not a lock mode", LockModeMarkerFile, string(mode))
	}
	path := filepath.Join(dir, LockModeMarkerFile)
	if err := os.WriteFile(path, []byte(string(mode)+"\n"), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", LockModeMarkerFile, err)
	}
	return nil
}

// RecordDumpLockMode is WriteLockModeMarker for the two callers that run
// mydumper (`bintrail dump` and the web interface's full snapshot), after a
// dump that succeeded. sent says whether mode was really given to mydumper;
// when it was not, no record is written, and one left by an earlier dump into
// the same directory is removed. Nothing here fails the dump: without the
// record the snapshot says nothing about its locks, which reads as unknown.
func RecordDumpLockMode(dir string, mode LockMode, sent bool) {
	if !sent {
		if err := os.Remove(filepath.Join(dir, LockModeMarkerFile)); err != nil && !os.IsNotExist(err) {
			slog.Warn("could not remove an earlier lock mode record from the dump; remove it by hand before converting this dump",
				"dir", dir, "file", LockModeMarkerFile, "error", err)
		}
		slog.Warn("mydumper was not given a lock mode, so it chose its own and the dump does not record one; "+
			"the snapshot made from it will not say how it was locked",
			"dir", dir)
		return
	}
	if err := WriteLockModeMarker(dir, mode); err != nil {
		slog.Warn("could not record the dump's lock mode; the snapshot made from it will not say how it was locked",
			"dir", dir, "lock_mode", string(mode), "error", err)
	}
}

// readLockModeMarker reads LockModeMarkerFile from a dump directory. It
// returns "" when the file is absent, cannot be read, or does not hold exactly
// one of the four lock modes: the snapshot is then written with no record and
// reads as unknown. A file that is there and not understood is logged, since
// it means a record was meant and lost.
func readLockModeMarker(inputDir string) string {
	path := filepath.Join(inputDir, LockModeMarkerFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("the dump's lock mode record could not be read; the snapshot will not say how it was locked",
				"path", path, "error", err)
		}
		return ""
	}
	mode := strings.TrimSpace(string(data))
	if isLockMode(mode) {
		return mode
	}
	slog.Warn("the dump's lock mode record does not name a lock mode; the snapshot will not say how it was locked",
		"path", path, "value", clipLockStamp(mode))
	return ""
}

// clipLockStamp bounds a value that is about to be logged or shown and is not
// one this program wrote.
func clipLockStamp(s string) string {
	s = strings.Join(strings.Fields(strings.ToValidUTF8(s, "?")), " ")
	if r := []rune(s); len(r) > 40 {
		return string(r[:40]) + "..."
	}
	return s
}
