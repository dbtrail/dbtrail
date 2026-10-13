package telemetry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Install age buckets. Deliberately few and wide: the value rides on a beacon
// next to version, os and arch, and a finer age would help single an install
// out of a small population.
const (
	AgeUnder7d = "<7d"
	Age7to30d  = "7d-30d"
	Age30to90d = "30d-90d"
	AgeOver90d = ">90d"
)

// installRecord is what a daemon remembers between runs so a beacon can say
// two things about the install it runs on: roughly how long it has been
// reporting, and, once, which version it ran before this one.
//
// It is NOT an identifier and never goes on the wire as stored. FirstSeen is a
// calendar day, shared by every install that started that day, and reaches the
// wire only as one of four buckets. Versions holds a major.minor per daemon
// name, shared by every install on that release.
//
// It is a separate file from the consent record on purpose. A beacon updates
// it, and a read-modify-write of the consent file from a months-lived daemon
// could write a stale copy over an opt-out recorded a moment earlier by
// `telemetry off` in another process. Keeping the two apart means the beacon
// path never opens the consent file for writing at all.
type installRecord struct {
	// FirstSeen is the UTC day of this directory's first spooled beacon.
	FirstSeen string `json:"first_seen,omitempty"`
	// Versions is the major.minor that last beaconed here, per daemon name.
	// Per daemon because two daemons can share this directory (a package
	// install running `watch` and `shim` as one user) at different versions;
	// one shared value would have each see the other's version as "previous"
	// and report a version change every single day.
	Versions map[string]string `json:"last_versions,omitempty"`
}

func (r installRecord) equal(o installRecord) bool {
	return r.FirstSeen == o.FirstSeen && maps.Equal(r.Versions, o.Versions)
}

const firstSeenLayout = "2006-01-02"

// maxInstallDaemons bounds the Versions map. There are fewer than ten
// long-running commands; a file with more names than this was not written by
// this program, and its versions are dropped as a whole.
const maxInstallDaemons = 16

// minorVersionRE is the only shape a recorded version may have: what
// minorVersion produces for a release build, with a length bound. The value
// comes back off a file on the operator's disk and goes on the wire as
// previous_version, so an unbounded run of digits there would be room for a
// hand-edited (or tampered) file to carry an identifier. Ingestion applies the
// same bound.
var minorVersionRE = regexp.MustCompile(`^[0-9]{1,3}\.[0-9]{1,4}$`)

// InstallPath returns the install record's path inside dir.
func InstallPath(dir string) string { return filepath.Join(dir, "telemetry-install.json") }

// loadInstall reads the install record.
//
// readable is false in exactly one case: the file is there and cannot be read
// (wrong owner, mode 000). That is not "no record": treating it as one would
// restart the install's clock and overwrite a file this process was not
// allowed to see, so the caller sends no install fields and leaves it alone.
//
// Everything else degrades FIELD BY FIELD to "nothing recorded". The file sits
// on an operator's disk where it can be edited and its contents end up on the
// wire, so a first_seen that is not exactly a calendar day, or a version that
// is not a bounded major.minor under a real daemon name, is dropped; a wrong
// type in one field does not take the other with it.
//
// dir "" has no record and is never read: a relative path here would resolve
// against whatever directory the process was started in.
func loadInstall(dir string) (rec installRecord, readable bool) {
	if dir == "" {
		return rec, true
	}
	data, err := os.ReadFile(InstallPath(dir))
	if err != nil {
		return rec, errors.Is(err, fs.ErrNotExist)
	}
	var raw struct {
		FirstSeen json.RawMessage `json:"first_seen"`
		Versions  json.RawMessage `json:"last_versions"`
	}
	if json.Unmarshal(data, &raw) != nil {
		return rec, true
	}
	var first string
	if json.Unmarshal(raw.FirstSeen, &first) == nil {
		if _, err := time.Parse(firstSeenLayout, first); err == nil {
			rec.FirstSeen = first
		}
	}
	var versions map[string]string
	if json.Unmarshal(raw.Versions, &versions) == nil && len(versions) <= maxInstallDaemons {
		for name, v := range versions {
			if name != "other" && sanitizeCommand(name) == name && minorVersionRE.MatchString(v) {
				if rec.Versions == nil {
					rec.Versions = map[string]string{}
				}
				rec.Versions[name] = v
			}
		}
	}
	return rec, true
}

// saveInstall writes the install record atomically (temp file, rename), mode
// 0600 like the consent file.
func saveInstall(dir string, r installRecord) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config directory %s: %w", dir, err)
	}
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshal install record: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".telemetry-install-*.json")
	if err != nil {
		return fmt.Errorf("create temp install record: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp install record: %w", err)
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("write install record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp install record: %w", err)
	}
	if err := os.Rename(tmp.Name(), InstallPath(dir)); err != nil {
		return fmt.Errorf("replace install record: %w", err)
	}
	return nil
}

// removeInstall deletes the install record. Opting out leaves nothing of
// telemetry's on disk except the choice itself.
func removeInstall(dir string) error {
	if dir == "" {
		return nil
	}
	if err := os.Remove(InstallPath(dir)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove install record: %w", err)
	}
	return nil
}

// undoVersion takes back the version a beacon recorded for daemon when that
// beacon turned out not to exist, so the change is reported by the next one.
//
// It re-reads the file and touches ONE entry, and only if that entry is still
// what this beacon wrote. Writing back the snapshot taken before the beacon
// would erase whatever another daemon sharing the folder recorded in between,
// and that daemon would then report its own change a second time. first_seen
// is left as it is: a clock started by a beacon that failed to spool is at
// most a day early.
func undoVersion(dir, daemon, wrote, before string) error {
	cur, readable := loadInstall(dir)
	if !readable || cur.Versions[daemon] != wrote {
		return nil
	}
	if before == "" {
		delete(cur.Versions, daemon)
	} else {
		cur.Versions[daemon] = before
	}
	return saveInstall(dir, cur)
}

// installAgeBucket coarsens the days between first and now. A first day in
// the future is a clock that moved backwards, not an age, and reads as the
// youngest bucket.
func installAgeBucket(first, now time.Time) string {
	days := int(now.UTC().Truncate(24*time.Hour).Sub(first.UTC().Truncate(24*time.Hour)) / (24 * time.Hour))
	switch {
	case days < 7:
		return AgeUnder7d
	case days < 30:
		return Age7to30d
	case days < 90:
		return Age30to90d
	default:
		return AgeOver90d
	}
}

func validInstallAge(s string) bool {
	switch s {
	case AgeUnder7d, Age7to30d, Age30to90d, AgeOver90d:
		return true
	}
	return false
}

// stampInstall fills a beacon's install fields from rec and returns the record
// that beacon leaves behind. It touches no file.
//
// current is this build's major.minor, or "unknown" for a source build. A
// source build never reports or records a version: it has none worth naming,
// and recording "unknown" would erase the release this install really came
// from.
func stampInstall(e *Event, rec installRecord, daemon, current string, now time.Time) installRecord {
	next := installRecord{FirstSeen: rec.FirstSeen, Versions: maps.Clone(rec.Versions)}

	first, err := time.Parse(firstSeenLayout, rec.FirstSeen)
	if err != nil {
		first = now.UTC()
		next.FirstSeen = first.Format(firstSeenLayout)
	}
	e.InstallAge = installAgeBucket(first, now)

	if minorVersionRE.MatchString(current) && daemon != "other" {
		prev := rec.Versions[daemon]
		// Only an UPGRADE is a change, and the record only ever moves up.
		// Two processes can share a daemon name and this folder at different
		// versions (`bintrail stream` and `bintrail-pg stream`, or two copies
		// mid rolling upgrade). If any difference counted, each would see the
		// other's version as "previous" and report a change every day. With
		// the record at the highest version seen, the older process finds a
		// newer one recorded, reports nothing and leaves it alone. The price
		// is that a downgrade is not reported.
		if prev == "" || minorNewer(current, prev) {
			if prev != "" {
				e.PreviousVersion = prev
			}
			if next.Versions == nil {
				next.Versions = map[string]string{}
			}
			next.Versions[daemon] = current
		}
	}
	return next
}

// minorNewer reports whether major.minor a is a later release than b. Both
// have already matched minorVersionRE, so the fields are short digit runs.
func minorNewer(a, b string) bool {
	amaj, amin, _ := strings.Cut(a, ".")
	bmaj, bmin, _ := strings.Cut(b, ".")
	am, _ := strconv.Atoi(amaj)
	bm, _ := strconv.Atoi(bmaj)
	if am != bm {
		return am > bm
	}
	an, _ := strconv.Atoi(amin)
	bn, _ := strconv.Atoi(bmin)
	return an > bn
}
