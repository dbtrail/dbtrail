package console

import (
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// Until #2658 a server's keep-newest count did nothing while the server had
// an S3 destination, and it was saved all the same: every server added from
// the web interface since #1681 got 3, with or without a bucket. From #2658
// the count governs the local folder of every server, so a count saved under
// the old rule would start removing local snapshots from servers that never
// removed any. This step, once per registry, sets those counts to 0: the
// servers go on keeping every local snapshot, as they did, until someone
// sets a count on them.
//
// Unlike the location migration it reads no process setting, so it runs
// where the file is read (LoadRegistry and Reload), in memory, in every
// process alike: the watch daemon that prunes and a console that only shows
// the count must agree before either has saved anything. The record that it
// ran is written with the next save (the daemon saves it at startup,
// SaveLocalKeepMigration); until then every read of the file repeats the
// same step and lands on the same answer.
//
// The record alone did not survive a downgrade (#2675): an older version
// keeps it, and saves the 3 it does not use on a server it adds with a
// destination, which this rule then applied on the way back up. Every count
// beside a destination this build saves is marked with that count and that
// destination (ServerEntry.LocalKeepSavedWith), and a load clears one that
// does not carry its mark, like the counts the first run cleared.

// localKeepWithS3 is the registry envelope's record that counts saved under
// the old rule were cleared. After it, a count on a server with an S3
// destination is one this rule applies, when the server's entry says so
// (Marked).
type localKeepWithS3 struct {
	// At is when it ran, RFC3339 UTC.
	At string `yaml:"at"`
	// Cleared are the ids whose count was set to 0.
	Cleared []string `yaml:"cleared,omitempty"`
	// Marked says every count beside an S3 destination in this file carries
	// the mark of the build that saved it (ServerEntry.LocalKeepSavedWith,
	// #2675). Without it the record did not stop a downgrade: a version from
	// before #2658 saves a count of 3 it does not use on a server it adds
	// with a destination, the record survives that version (it keeps keys
	// it does not know), and on the way back up the count applied and the
	// folder was pruned to 3. A count with no mark, or with the mark of
	// another count or destination, is now one such a version saved, and is
	// cleared instead (clearInertKeepCounts).
	//
	// Absent on a file last saved by the first version with the record and
	// no mark (it drops keys of the record it does not know): that version
	// applied the counts it saved, so they are marked as they are, once.
	Marked bool `yaml:"entries_marked,omitempty"`
}

// keepMark is what ServerEntry.LocalKeepSavedWith holds for e: its count and
// its S3 destination as written, or "" when the count does not reach a
// destination (none, or keep everything), where every version applies it
// alike.
func keepMark(e ServerEntry) string {
	if e.BaselineS3 == "" || e.LocalKeepNewest <= 0 {
		return ""
	}
	return strconv.Itoa(e.LocalKeepNewest) + " " + e.BaselineS3
}

// clearInertKeepCounts is the step itself, on a file just read (or a new
// one). It returns the names of the servers whose count it cleared, and
// whether the file needs saving for it: the first time (the record), once
// to mark the counts of a file that has the record and no marks, and
// whenever a count beside a destination does not carry its mark.
func clearInertKeepCounts(f *registryFile) (cleared []string, ran bool) {
	switch {
	case f.LocalKeepWithS3 == nil:
	case !f.LocalKeepWithS3.Marked:
		for i := range f.Servers {
			f.Servers[i].LocalKeepSavedWith = keepMark(f.Servers[i])
		}
		f.LocalKeepWithS3.Marked = true
		return nil, true
	default:
		for i, e := range f.Servers {
			if keepMark(e) == "" || e.LocalKeepSavedWith == keepMark(e) {
				continue
			}
			f.Servers[i].LocalKeepNewest = 0
			f.Servers[i].LocalKeepSavedWith = ""
			cleared = append(cleared, e.Name)
		}
		return cleared, len(cleared) > 0
	}
	rec := &localKeepWithS3{At: time.Now().UTC().Format(time.RFC3339), Marked: true}
	for i, e := range f.Servers {
		// The value as written, blanks included: before #2658 any non-empty
		// destination made the count do nothing.
		if e.BaselineS3 == "" || e.LocalKeepNewest <= 0 {
			continue
		}
		f.Servers[i].LocalKeepNewest = 0
		rec.Cleared = append(rec.Cleared, e.ID)
		cleared = append(cleared, e.Name)
	}
	f.LocalKeepWithS3 = rec
	return cleared, true
}

// SaveLocalKeepMigration writes the record of the step above into the file,
// for the process that owns it, and says once which servers it touched. It
// does nothing when the file already held the record and every count beside
// a destination carried its mark, when it does not exist
// yet (nothing to migrate: the record rides along with the first save), or
// when this process may not write it. A failed save is logged and nothing
// more: the step is already in force in memory, and the next start repeats
// it.
func (r *Registry) SaveLocalKeepMigration() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.keepMigrationPending {
		return
	}
	r.keepMigrationPending = false
	if len(r.keepMigrationCleared) > 0 {
		slog.Warn("servers with an S3 destination had a number of local snapshots to keep that never applied to them; it was cleared, so they keep every local snapshot as before. "+
			"Set Keep by count on the "+PageSnapshots+" page to keep only the newest on this machine",
			"servers", strings.Join(r.keepMigrationCleared, ", "))
	}
	if r.path == "" || r.readOnly || len(r.file.Servers) == 0 {
		return
	}
	if err := r.save(); err != nil {
		slog.Warn("the server registry could not be saved after clearing counts that never applied; they stay cleared in this process, and the next start repeats the step",
			"file", r.path, "error", err)
	}
}
