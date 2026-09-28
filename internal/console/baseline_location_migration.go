package console

import (
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"
)

// Before #1684 a registry server with no snapshot location of its own read
// the one the process was started with (--baseline-dir / --baseline-s3), and
// only for reads: every write path refused that shared store. The fallback is
// gone, so the servers that relied on it get the value written into their own
// entry, once, on the first start of a build without it. Without this step
// time travel would stop working for them with no message.

// baselineLocationMigrated is the registry envelope's record that the
// migration ran. It is what makes the migration run once: after it, a server
// with no location is one the operator left that way, and a later start must
// not fill it back in.
type baselineLocationMigrated struct {
	// At is when it ran, RFC3339 UTC.
	At string `yaml:"at"`
	// Dir and S3 are the process values it applied, verbatim.
	Dir string `yaml:"dir,omitempty"`
	S3  string `yaml:"s3,omitempty"`
	// Servers are the ids it wrote into.
	Servers []string `yaml:"servers,omitempty"`
}

// LocationMigration is what MigrateProcessBaselineLocation did at this start.
type LocationMigration struct {
	// Done: the registry records the migration, from this start or an
	// earlier one. False only before it runs.
	Done bool `json:"-"`
	// Migrated names the servers that got the process location at this start.
	Migrated []string `json:"servers,omitempty"`
	// MissingDir names the migrated servers whose folder does not exist right
	// now. It is written all the same: it is what they read before, and a
	// mount that is not there yet can come back.
	MissingDir []string `json:"missing_dir,omitempty"`
	// NotSaved is why the registry FILE does not hold the result, empty when
	// it does or when nothing changed. This process reads the migrated values
	// anyway; a later start migrates again until a save succeeds.
	NotSaved string `json:"not_saved,omitempty"`
}

// MigrateProcessBaselineLocation writes the process-wide snapshot location
// into every registry server that has none of its own, exactly as the old
// read-path fallback resolved it: all or nothing (a server naming its own
// folder OR bucket keeps just that), both values copied verbatim, untrimmed,
// empty ones included. It runs once per registry: the envelope records it,
// and a registry that records it is left alone. A start with no process
// location records nothing: there is nothing to migrate, and recording it
// would let a start that merely lacked the flag (a serve run without
// --baseline-dir, a missing variable) cancel the migration of a later start
// that has it, for good.
//
// The entries change in memory first and the file is saved once. When the
// file cannot be saved (written by a newer version, or the write failed),
// the in-memory entries keep the migrated values, so this process reads
// what it read yesterday, and the report says the file was not updated.
//
// The command-line server is not in the registry and is never touched.
func (r *Registry) MigrateProcessBaselineLocation(dir, s3 string) LocationMigration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file.BaselineLocationMigrated != nil {
		r.migration = LocationMigration{Done: true}
		return r.migration
	}
	if dir == "" && s3 == "" {
		r.migration = LocationMigration{}
		return r.migration
	}
	rec := &baselineLocationMigrated{At: time.Now().UTC().Format(time.RFC3339), Dir: dir, S3: s3}
	var rep LocationMigration
	// The folder is looked at once: anything but a directory that answers
	// (missing, not a directory, no permission, a dead mount) is named.
	dirProblem := ""
	if dir != "" && !strings.HasPrefix(dir, "s3://") {
		if fi, err := os.Stat(dir); err != nil {
			dirProblem = err.Error()
		} else if !fi.IsDir() {
			dirProblem = "not a directory"
		}
	}
	for i, e := range r.file.Servers {
		if e.BaselineDir != "" || e.BaselineS3 != "" {
			continue
		}
		r.file.Servers[i].BaselineDir = dir
		r.file.Servers[i].BaselineS3 = s3
		if dir != "" {
			// A count saved for a server with no folder (new servers got 3)
			// would, on this folder, prune snapshots the command-line server
			// wrote the moment DBTrail runs without --baseline-dir (which
			// is what stops the folder being excluded from pruning). The
			// folder holds snapshots that are not this server's: keep them all.
			r.file.Servers[i].LocalKeepNewest = 0
		}
		rec.Servers = append(rec.Servers, e.ID)
		rep.Migrated = append(rep.Migrated, e.Name)
		if dirProblem != "" {
			rep.MissingDir = append(rep.MissingDir, e.Name)
		}
	}
	// Recorded in memory whatever happens next: with no server to migrate
	// there is no reason to create or rewrite a file, and the record rides
	// along with the next save the operator makes.
	r.file.BaselineLocationMigrated = rec
	rep.Done = true
	if len(rep.Migrated) > 0 {
		switch {
		case r.readOnly:
			rep.NotSaved = "it was written by a newer version of DBTrail, which this one does not change"
		default:
			if err := r.save(); err != nil {
				rep.NotSaved = err.Error()
				// save publishes the bucket table only on success; the
				// entries in memory changed either way.
				r.syncBucketStores()
			}
		}
	}
	r.migration = rep
	logLocationMigration(r.path, dir, s3, dirProblem, rep)
	return rep
}

// LocationMigration returns what the migration did at this start, for the
// Snapshots page.
func (r *Registry) LocationMigration() LocationMigration {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.migration
	m.Migrated = slices.Clone(m.Migrated)
	m.MissingDir = slices.Clone(m.MissingDir)
	return m
}

// startupLocationFlags names the frozen flags the log lines below talk
// about, once, so an operator can grep for them.
const startupLocationFlags = "--baseline-dir / --baseline-s3"

func logLocationMigration(path, dir, s3, dirProblem string, rep LocationMigration) {
	if len(rep.Migrated) == 0 {
		return
	}
	if rep.NotSaved != "" {
		slog.Warn("snapshot locations: servers that used the startup location were given it as their own, but the server registry could not be updated; this process reads them as before, and the next start tries again",
			"servers", rep.Migrated, "flags", startupLocationFlags, "dir", dir, "s3", s3, "file", path, "reason", rep.NotSaved)
	} else {
		slog.Info("snapshot locations: servers that used the startup location now have it as their own",
			"servers", rep.Migrated, "flags", startupLocationFlags, "dir", dir, "s3", s3, "file", path)
	}
	if len(rep.MissingDir) > 0 {
		slog.Warn("snapshot locations: the startup folder cannot be read right now; it was written as it is and not created, so these servers read nothing from it until it is back",
			"servers", rep.MissingDir, "dir", dir, "problem", dirProblem)
	}
	// The startup location is also the command-line server's, so even one
	// migrated server shares it, and now takes snapshots and restores there.
	slog.Warn("snapshot locations: these servers now share the startup snapshot location with the command-line server and with each other, and their snapshots can mix there; give each its own folder or S3 prefix"+onPage(PageSnapshots),
		"servers", rep.Migrated)
}
