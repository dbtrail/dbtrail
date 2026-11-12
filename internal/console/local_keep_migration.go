package console

import (
	"log/slog"
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

// localKeepWithS3 is the registry envelope's record that counts saved under
// the old rule were cleared. After it, a count on a server with an S3
// destination is one this rule applies.
type localKeepWithS3 struct {
	// At is when it ran, RFC3339 UTC.
	At string `yaml:"at"`
	// Cleared are the ids whose count was set to 0.
	Cleared []string `yaml:"cleared,omitempty"`
}

// clearInertKeepCounts is the step itself, on a file just read (or a new
// one). It returns the names of the servers whose count it cleared, and
// whether it ran at all (false once the file records it).
func clearInertKeepCounts(f *registryFile) (cleared []string, ran bool) {
	if f.LocalKeepWithS3 != nil {
		return nil, false
	}
	rec := &localKeepWithS3{At: time.Now().UTC().Format(time.RFC3339)}
	for i, e := range f.Servers {
		if strings.TrimSpace(e.BaselineS3) == "" || e.LocalKeepNewest <= 0 {
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
// does nothing when the file already held the record, when it does not exist
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
