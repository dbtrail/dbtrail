package console

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// The state of a server's SQL mirror, for the Snapshots page (#2302).
//
// A server whose snapshots are only in S3 answers SQL from a local mirror of
// them. Whether SQL works there, from which snapshot, and why not when it
// does not, was only in the daemon's log; the page that shows the bucket is
// where a person looks.
//
// Two sources, kept apart on purpose. What SQL can read is asked of the disk,
// here, on every request: it is true in any process that sees the folder (the
// read-only console and the router read mirrors they do not fill), and it
// cannot disagree with what a statement finds. What the filler is doing or
// failed at is reported by the filler (CopyMirrorReporter), and only a
// process that fills has one.

// CopyMirrorReport is what the process that fills a server's mirror last did.
type CopyMirrorReport struct {
	// Source is the snapshot root the report is about. A server pointed at
	// another bucket has a new, empty mirror; what the last round found in
	// the old one says nothing about it, and is not shown for it.
	Source string
	// CheckedAt is when the last round for this server ended.
	CheckedAt time.Time
	// SourceNewest is the newest complete snapshot the bucket listed in that
	// round, by directory name; "" when it established none.
	SourceNewest string
	// Err is why that round failed, "" when it did not.
	Err string
	// Bringing is the snapshot a round is downloading right now, by
	// directory name, and how many bytes it set out to download; "" when no
	// download is under way.
	Bringing      string
	BringingBytes int64
}

// CopyMirrorReporter reports on the mirrors this process fills. ok is false
// for a server it has not run a round for yet.
type CopyMirrorReporter interface {
	CopyMirrorReport(serverID string) (report CopyMirrorReport, ok bool)
}

// sqlMirrorDTO is the Snapshots page's view of the mirror. Present only for a
// server that has one, or would with its switch on (its snapshots are in S3
// and it keeps no local folder).
type sqlMirrorDTO struct {
	// State is one word for the page to branch on:
	//
	//	ready      SQL reads the mirror, which is up to date as far as known
	//	bringing   a snapshot is being downloaded (Ready says whether SQL
	//	           answers meanwhile from an earlier one)
	//	failed     the last round failed (Ready as above)
	//	empty      the bucket holds no complete snapshot to bring
	//	waiting    nothing brought yet and no round reported
	//	unfilled   nothing brought, and this process brings nothing
	//	off        the mirror is switched off for this server (no_sql_mirror)
	State string `json:"state"`
	// Ready: SQL answers from the mirror. It is the statement path's own
	// test (bundle.usesSQLMirror), asked here again, never a second opinion.
	Ready bool `json:"ready"`
	// Newest is the time of the snapshot SQL reads, omitted when there is none.
	Newest *time.Time `json:"newest,omitempty"`
	// Behind is the time of a newer complete snapshot the bucket listed in
	// the last round and the mirror does not hold: SQL is answering from an
	// older one than the list below shows.
	Behind *time.Time `json:"behind,omitempty"`
	// Bringing is the time of the snapshot being downloaded, and
	// BringingBytes what it set out to download.
	Bringing      *time.Time `json:"bringing,omitempty"`
	BringingBytes int64      `json:"bringing_bytes,omitempty"`
	// Error is the last round's failure, one line.
	Error string `json:"error,omitempty"`
	// CheckedAt is when the filler last looked at the bucket.
	CheckedAt *time.Time `json:"checked_at,omitempty"`
}

// sqlMirrorOf builds the mirror's state for bundle b, nil when the server has
// no mirror.
func (s *Server) sqlMirrorOf(b *bundle, serverID string) *sqlMirrorDTO {
	if b == nil || !strings.HasPrefix(b.baselineSrc, "s3://") {
		return nil
	}
	if b.sqlMirrorOff {
		// Nothing else is true of it: no round runs for this server, and
		// what an earlier one left is about a folder that is being removed.
		return &sqlMirrorDTO{State: "off"}
	}
	if b.sqlMirrorDir == "" {
		return nil
	}
	out := &sqlMirrorDTO{Ready: b.usesSQLMirror()}
	if out.Ready {
		if at, ok := newestMirroredSnapshot(b.sqlMirrorDir); ok {
			out.Newest = &at
		}
	}
	var rep CopyMirrorReport
	reported := false
	if s.copyMirror != nil {
		rep, reported = s.copyMirror.CopyMirrorReport(serverID)
		if strings.TrimSuffix(rep.Source, "/") != strings.TrimSuffix(b.baselineSrc, "/") {
			// About another bucket: this mirror has not been looked at.
			rep, reported = CopyMirrorReport{}, false
		}
	}
	if at, ok := snapshotdir.ParseTime(rep.SourceNewest); ok && out.Newest != nil && at.After(*out.Newest) {
		out.Behind = &at
	}
	if !rep.CheckedAt.IsZero() {
		at := rep.CheckedAt.UTC()
		out.CheckedAt = &at
	}
	bringing, isBringing := snapshotdir.ParseTime(rep.Bringing)
	switch {
	case isBringing:
		out.State, out.Bringing, out.BringingBytes = "bringing", &bringing, rep.BringingBytes
	case rep.Err != "":
		line, _, _ := strings.Cut(strings.TrimSpace(rep.Err), "\n")
		if line = strings.TrimSpace(line); len(line) > sqlMirrorErrorMax {
			line = line[:sqlMirrorErrorMax] + "…"
		}
		out.State, out.Error = "failed", line
	case out.Ready:
		out.State = "ready"
	case !s.fillsCopyMirrors:
		out.State = "unfilled"
	case reported:
		// A round ran, did not fail, and there is still nothing to read.
		out.State = "empty"
	default:
		out.State = "waiting"
	}
	return out
}

// sqlMirrorErrorMax bounds the failure line sent to the page. Long enough
// for the longest sentence the filler writes (the no-room one, with a path
// and three sizes), which cut short loses the half that says what to do.
const sqlMirrorErrorMax = 600

// sqlPending reports whether SQL on b is not available now and will become
// so without anyone doing anything: its mirror is being filled by this
// process and holds nothing yet. The page asks again instead of concluding
// that this server has no SQL. The caller has already established that
// nothing else keeps SQL from this session (sqlAllowed).
func (s *Server) sqlPending(b *bundle) bool {
	return s.fillsCopyMirrors && b != nil && b.sqlMirrorDir != "" &&
		strings.HasPrefix(b.baselineSrc, "s3://") && !b.usesSQLMirror()
}

// newestMirroredSnapshot is the time of the newest snapshot in a mirror
// folder that a statement can read: a complete snapshot directory
// (baseline.SnapshotComplete) holding a table file under a schema directory,
// which is what reconstruct.HasLocalSnapshot looks for. A directory that is
// not there, cannot be read, or holds none is "no".
func newestMirroredSnapshot(dir string) (time.Time, bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return time.Time{}, false
	}
	var times []time.Time
	for _, e := range entries {
		at, ok := snapshotdir.ParseTime(e.Name())
		snap := filepath.Join(dir, e.Name())
		if !ok || !e.IsDir() || !baseline.SnapshotComplete(snap) || !holdsATable(snap) {
			continue
		}
		times = append(times, at)
	}
	if len(times) == 0 {
		return time.Time{}, false
	}
	sort.Slice(times, func(i, j int) bool { return times[i].After(times[j]) })
	return times[0], true
}

// holdsATable reports whether a snapshot directory has a table file in one
// of its schema directories.
func holdsATable(snap string) bool {
	schemas, err := os.ReadDir(snap)
	if err != nil {
		return false
	}
	for _, sch := range schemas {
		if !sch.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(snap, sch.Name()))
		if err != nil {
			continue
		}
		for _, f := range files {
			if !f.IsDir() && strings.HasSuffix(f.Name(), ".parquet") {
				return true
			}
		}
	}
	return false
}
