package console

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
)

// S3CleanupRecord is what the last S3 cleanup of one destination did
// (#2596), as the daemon's cleanup loop records it for the settings page.
// Held in this process's memory: a restart starts without one, and a
// standalone `serve`, which runs no cleanup, never has one.
type S3CleanupRecord struct {
	At         string `json:"at"`
	Removed    int    `json:"removed"`
	KeptNewest int    `json:"kept_newest,omitempty"`
	// Deferred snapshots past the age go in a later cleanup; Stopped says
	// why this one ended early ("bound", "busy", "cancelled").
	Deferred int    `json:"deferred,omitempty"`
	Stopped  string `json:"stopped,omitempty"`
	// Error is why the cleanup stopped on a failure, and Snapshot the one it
	// stopped on.
	Error    string `json:"error,omitempty"`
	Snapshot string `json:"snapshot,omitempty"`
	// AccessDenied: the error was a refused permission.
	AccessDenied bool `json:"access_denied,omitempty"`
	// Skipped: a restore or a SQL file build was reading the destination,
	// so this cleanup did not run. The error of the last cleanup that did
	// run is kept on it, so a failure does not vanish behind skips.
	Skipped bool `json:"skipped,omitempty"`
}

var (
	s3CleanupsMu sync.Mutex
	s3Cleanups   = map[string]S3CleanupRecord{}
)

// RecordS3Cleanup stores the last cleanup of the destination dest.
func RecordS3Cleanup(dest string, rec S3CleanupRecord) {
	s3CleanupsMu.Lock()
	defer s3CleanupsMu.Unlock()
	if prev, ok := s3Cleanups[normS3(dest)]; ok && rec.Skipped && rec.Error == "" {
		rec.Error, rec.Snapshot, rec.AccessDenied = prev.Error, prev.Snapshot, prev.AccessDenied
	}
	s3Cleanups[normS3(dest)] = rec
}

func lastS3Cleanup(dest string) *S3CleanupRecord {
	s3CleanupsMu.Lock()
	defer s3CleanupsMu.Unlock()
	rec, ok := s3Cleanups[normS3(dest)]
	if !ok {
		return nil
	}
	return &rec
}

// LastS3CleanupForTest is lastS3Cleanup for the daemon package's tests.
func LastS3CleanupForTest(dest string) *S3CleanupRecord { return lastS3Cleanup(dest) }

func resetS3Cleanups() {
	s3CleanupsMu.Lock()
	defer s3CleanupsMu.Unlock()
	s3Cleanups = map[string]S3CleanupRecord{}
}

// s3RetainState is what the settings row says about e's S3 age: the age the
// cleanup runs at when it does, or why it does not although e has one set.
// The reasons: "no_loop" (this process runs no cleanup, as a standalone
// serve, which cannot see the service's settings either), "not_s3" (the
// destination is not s3://), "too_short" or "unreadable" (e's own value),
// "startup_destination" (the destination DBTrail was started with, never
// cleaned) and "shared" (sharers with no age, or one that cannot be read).
func (s *Server) s3RetainState(e ServerEntry) (minutes int, off string, unset, unreadable []string) {
	if e.BaselineS3 == "" || e.BaselineS3Retain == "" {
		return 0, "", nil, nil
	}
	if !s.localPruneLoop {
		return 0, "no_loop", nil, nil
	}
	if !strings.HasPrefix(strings.TrimSpace(e.BaselineS3), "s3://") {
		return 0, "not_s3", nil, nil
	}
	if _, err := ParseS3Retain(e.BaselineS3Retain); err != nil {
		if d, perr := cliutil.ParseRetain(strings.TrimSpace(e.BaselineS3Retain)); perr == nil && d > 0 {
			return 0, "too_short", nil, nil
		}
		return 0, "unreadable", nil, nil
	}
	entries := s.cm.reg.List()
	local := s.pruneRetainMinutes()
	age, _, on := S3RetentionFor(entries, s.cm.defaultBaselineS3, e.BaselineS3, time.Duration(local)*time.Minute)
	if on {
		return int(age.Minutes()), "", nil, nil
	}
	if normS3(s.cm.defaultBaselineS3) == normS3(e.BaselineS3) {
		return 0, "startup_destination", nil, nil
	}
	for _, x := range entries {
		if x.ID == e.ID || normS3(x.BaselineS3) != normS3(e.BaselineS3) {
			continue
		}
		if strings.TrimSpace(x.BaselineS3Retain) == "" {
			unset = append(unset, x.Name)
		} else if _, err := ParseS3Retain(x.BaselineS3Retain); err != nil {
			unreadable = append(unreadable, x.Name)
		}
	}
	slices.Sort(unset)
	slices.Sort(unreadable)
	return 0, "shared", unset, unreadable
}
