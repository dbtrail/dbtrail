package console

import (
	"errors"
	"fmt"
	"strings"
)

// A snapshot location has no per-server segment: every writer's snapshots
// land side by side as <location>/<timestamp>/. Two writers on one location
// fold each other's snapshots (the newest one there may be the other's),
// share a <timestamp>/ directory when they publish in the same second, and
// overwrite each other's signature. Reads are safe; writes are not. So every
// WRITE path (the periodic refresh, "Read database now", the schedule, a
// restore, the compaction of delta chains) refuses a location that more than
// one writer resolves to (#1684). Reads stay exactly as they are.
//
// The writers of a location are every registry entry naming it, plus the
// command-line server when this process refreshes it. The command-line
// server is never refused by this rule: the location is its own startup
// flag, and the registry servers are the ones that came to it (the upgrade
// migration gives servers the startup location as their own).

// ErrSharedLocation is the refusal of a write into a shared location.
var ErrSharedLocation = errors.New("this server's snapshot location is shared")

// commandLineWriterName names the command-line server in refusals.
const commandLineWriterName = "the command-line server"

// CommandLineWriter is the command-line server's location when this process
// writes snapshots for it; zero when it writes none.
type CommandLineWriter struct {
	Dir, S3 string
	Writes  bool
}

// LocationWriters returns the other writers of e's snapshot location, by
// name, in registry order, the command-line server last. Empty means e is
// the only one. Folders are compared as canonicalDir resolves them; S3
// locations overlap when one prefix contains the other.
func LocationWriters(entries []ServerEntry, e ServerEntry, cli CommandLineWriter) []string {
	var out []string
	for _, o := range entries {
		if o.ID == e.ID {
			continue
		}
		if sameDir(e.BaselineDir, o.BaselineDir) || s3Overlap(e.BaselineS3, o.BaselineS3) {
			out = append(out, o.Name)
		}
	}
	if cli.Writes && (sameDir(e.BaselineDir, cli.Dir) || s3Overlap(e.BaselineS3, cli.S3)) {
		out = append(out, commandLineWriterName)
	}
	return out
}

// SharedLocationRefusal is the refusal for a write by e into a location
// others also write, or nil. It names them and the fix.
func SharedLocationRefusal(entries []ServerEntry, e ServerEntry, cli CommandLineWriter) error {
	others := LocationWriters(entries, e, cli)
	if len(others) == 0 {
		return nil
	}
	return sharedLocationError(others)
}

func sharedLocationError(others []string) error {
	return fmt.Errorf("%w with %s, and snapshots there have no per-server folder, so writing would mix them; give this server its own folder or prefix%s",
		ErrSharedLocation, strings.Join(others, ", "), onPage(PageSnapshots))
}

// sameDir: both set and naming the same folder.
func sameDir(a, b string) bool {
	return a != "" && b != "" && canonicalDir(a) == canonicalDir(b)
}

// s3Overlap: both set, and one prefix contains the other. Compared on
// path segments, so s3://b/p and s3://b/p2 do not overlap.
func s3Overlap(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	a, b = strings.TrimRight(a, "/"), strings.TrimRight(b, "/")
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// SetCommandLineWriter records the command-line server's location for a
// process that writes snapshots for it, so WriteRefusal can count it.
// Called once where the registry is loaded.
func (r *Registry) SetCommandLineWriter(dir, s3 string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cliWriter = CommandLineWriter{Dir: dir, S3: s3, Writes: dir != "" || s3 != ""}
}

// WriteRefusal is SharedLocationRefusal over this registry: the refusal for
// a write by the server with e's id, or nil. Nil-safe, and nil for an id
// the registry does not hold (the command-line server).
func (r *Registry) WriteRefusal(e ServerEntry) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	entries := append([]ServerEntry(nil), r.file.Servers...)
	cli := r.cliWriter
	r.mu.Unlock()
	known := false
	for _, o := range entries {
		if o.ID == e.ID {
			known = true
			break
		}
	}
	if !known {
		return nil
	}
	return SharedLocationRefusal(entries, e, cli)
}
