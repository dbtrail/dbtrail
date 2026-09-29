package console

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
)

// PreviousLocation is a snapshot location a server used before its current
// one (#1684). A location change used to overwrite the only value, and every
// snapshot written at the old place became invisible to time travel, restore
// and the snapshot list, though nothing had deleted it. Now the old place is
// remembered, read-only: reads consult it after the current one, and nothing
// ever writes to it, prunes it or counts it as a writer of that location.
//
// LeftAt is when this server stopped writing there, RFC3339 in UTC. Every
// snapshot this server wrote at that place is older than it, so a read never
// takes a newer one from there: whatever is newer was written by someone
// else, a server that took the place over, signed or not.
type PreviousLocation struct {
	Location string `yaml:"location" json:"location"`
	LeftAt   string `yaml:"left_at" json:"left_at"`
}

// ErrUnknownPreviousLocation is the refusal to forget a location this server
// does not remember.
var ErrUnknownPreviousLocation = errors.New("this server has no such previous snapshot location")

// locationNow is the clock a location change is stamped with; a test sets it.
var locationNow = time.Now

// Until is the newest instant a snapshot of this server can have at the
// location. ok is false when LeftAt does not parse: the location is then
// never read, and the reader says why.
func (p PreviousLocation) Until() (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, p.LeftAt)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// SameSnapshotLocation reports whether a and b name one snapshot location.
// It is the one rule every comparison of locations uses: the list of previous
// locations, and the write guard that refuses a shared one. Two rules would
// let a place the guard calls shared be a place the list calls new.
//
//   - Empty is no location, and equal to nothing.
//   - A folder and a bucket are never one location.
//   - S3: the scheme and the bucket without case (a bucket name has no upper
//     case), the prefix WITH case, trailing slashes aside. S3 keys are case
//     sensitive, so s3://b/Snap and s3://b/snap hold different objects, and
//     merging them would hide one of the two histories. A nested prefix is
//     another location: a listing reads only the snapshot folders right
//     under its prefix.
//   - A folder: the same folder on disk when both exist (a symlink, and upper
//     and lower case where the filesystem ignores case), else the same path
//     once cleaned.
func SameSnapshotLocation(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	as, bs := isS3Location(a), isS3Location(b)
	if as != bs {
		return false
	}
	if as {
		return s3LocationKey(a) == s3LocationKey(b)
	}
	if canonicalDir(a) == canonicalDir(b) {
		return true
	}
	ai, aerr := os.Stat(a)
	bi, berr := os.Stat(b)
	return aerr == nil && berr == nil && ai.IsDir() && bi.IsDir() && os.SameFile(ai, bi)
}

// isS3Location reports an s3:// location, whatever the scheme's case.
func isS3Location(loc string) bool {
	return len(loc) >= 5 && strings.EqualFold(loc[:5], "s3://")
}

// s3LocationKey is the comparable form of an s3:// location: the bucket
// lower-cased, the prefix as typed, trailing slashes removed.
func s3LocationKey(loc string) string {
	rest := strings.TrimRight(loc[5:], "/")
	bucket, prefix, _ := strings.Cut(rest, "/")
	return strings.ToLower(bucket) + "/" + prefix
}

// nextPreviousLocations is the list after an edit from old to e. Each place
// old named that e no longer names goes first, stamped now; a place e names
// again leaves the list (it is current); nothing appears twice. Always a
// fresh slice: the registry's rollback copy shares entries with the list,
// and an append into old's array would change the copy too.
func nextPreviousLocations(old, e ServerEntry, now time.Time) []PreviousLocation {
	current := func(loc string) bool {
		return SameSnapshotLocation(loc, e.BaselineDir) || SameSnapshotLocation(loc, e.BaselineS3)
	}
	var out []PreviousLocation
	listed := func(loc string) bool {
		for _, p := range out {
			if SameSnapshotLocation(p.Location, loc) {
				return true
			}
		}
		return false
	}
	stamp := now.UTC().Format(time.RFC3339)
	for _, loc := range []string{old.BaselineDir, old.BaselineS3} {
		if loc == "" || current(loc) || listed(loc) {
			continue
		}
		out = append(out, PreviousLocation{Location: loc, LeftAt: stamp})
	}
	for _, p := range old.PreviousBaselineLocations {
		if p.Location == "" || current(p.Location) || listed(p.Location) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// ForgetPreviousLocation removes loc from the server's previous locations,
// and returns the entry as saved. Nothing at the location is deleted: it is
// only no longer read. Refused for a location the list does not hold, the
// current one included.
func (r *Registry) ForgetPreviousLocation(id, loc string) (ServerEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ServerEntry{}, ErrRegistryReadOnly
	}
	loc = strings.TrimSpace(loc)
	for i, e := range r.file.Servers {
		if e.ID != id {
			continue
		}
		var kept []PreviousLocation
		for _, p := range e.PreviousBaselineLocations {
			if !SameSnapshotLocation(p.Location, loc) {
				kept = append(kept, p)
			}
		}
		if len(kept) == len(e.PreviousBaselineLocations) {
			return ServerEntry{}, fmt.Errorf("%w: %q", ErrUnknownPreviousLocation, loc)
		}
		prev := slices.Clone(r.file.Servers)
		r.file.Servers[i].PreviousBaselineLocations = kept
		if err := r.save(); err != nil {
			r.file.Servers = prev // roll back
			return ServerEntry{}, err
		}
		return r.file.Servers[i], nil
	}
	return ServerEntry{}, ErrUnknownServer
}
