package telemetry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	spoolDirName = "telemetry-spool"
	spoolSuffix  = ".ndjson"
	claimedMark  = ".claimed-"

	// maxSpoolFileBytes caps a single day's spool. A box that never reaches the
	// network must not grow unbounded; past the cap events are dropped, which is
	// the correct tradeoff for aggregate usage data.
	maxSpoolFileBytes = 5 << 20 // 5 MiB

	// maxSpoolAge bounds how long an undelivered event lingers on disk.
	maxSpoolAge = 7 * 24 * time.Hour

	// claimReclaimAfter is how long a claim may sit before another run adopts
	// it. It exists for a drainer that died mid-flight — which is the COMMON
	// case, not an exotic one: a sub-100ms command's process exits while its
	// detached drain goroutine is still in its HTTP call.
	//
	// It must exceed drainDeadline so it can never steal from a drainer that is
	// genuinely still working; 30x that leaves ample margin while keeping
	// recovery quick enough that an abandoned batch is not stranded for an
	// afternoon.
	claimReclaimAfter = time.Minute
)

// SpoolDir returns the spool directory inside dir.
func SpoolDir(dir string) string { return filepath.Join(dir, spoolDirName) }

// PurgeSpool removes every locally spooled event.
//
// Needed because drain runs ONLY while telemetry is enabled: without this, an
// operator who runs `telemetry off` would leave whatever was spooled before
// their decision sitting on disk indefinitely — never sent, never aged out —
// belonging to precisely the person who asked for none.
func PurgeSpool(dir string) error {
	if dir == "" {
		return nil
	}
	if err := os.RemoveAll(SpoolDir(dir)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove telemetry spool: %w", err)
	}
	return nil
}

// errSpoolFull is returned for an event dropped at the day's size cap. It is
// an error, not a quiet nil, because a caller may keep state that depends on
// the event existing (Beacon's install record does).
var errSpoolFull = errors.New("spool file at its size cap, event dropped")

// appendEvent writes one NDJSON line. Plain O_APPEND with no fsync: this runs
// at the end of every command, and an fsync per invocation is exactly the kind
// of hot-path tax telemetry must never impose. A single write of a short line
// is atomic enough that concurrent appenders interleave whole lines rather
// than corrupting each other.
//
// There is deliberately NO network call on this path, ever.
func appendEvent(spoolDir string, e Event, now time.Time) error {
	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal telemetry event: %w", err)
	}
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		return fmt.Errorf("create spool directory: %w", err)
	}
	path := filepath.Join(spoolDir, now.UTC().Format("2006-01-02")+spoolSuffix)

	if fi, err := os.Stat(path); err == nil && fi.Size() >= maxSpoolFileBytes {
		return errSpoolFull
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open spool file: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append telemetry event: %w", err)
	}
	return nil
}

// claimName builds a claim path for base, stamped with this process and
// instant so the name itself records when the claim was taken.
func claimName(spoolDir, base string, now time.Time) string {
	return filepath.Join(spoolDir, base+claimedMark+strconv.Itoa(os.Getpid())+"-"+strconv.FormatInt(now.UnixNano(), 10))
}

// unclaimedBase strips any claim suffix, so re-claiming a file does not append
// suffix after suffix.
func unclaimedBase(name string) string {
	if i := strings.Index(name, claimedMark); i >= 0 {
		return name[:i]
	}
	return name
}

// claimStamp recovers when a claim was taken, from the claim's own name.
//
// The claim time canNOT be read from the file's mtime: os.Rename preserves it,
// so a freshly claimed file still carries the time of its last APPEND. For any
// prior-day spool file — the normal drain target — that is hours or days ago,
// which would mark a just-created claim as abandoned instantly and let a second
// drainer adopt and re-send a batch that is still in flight.
func claimStamp(name string) (time.Time, bool) {
	i := strings.LastIndex(name, claimedMark)
	if i < 0 {
		return time.Time{}, false
	}
	suffix := name[i+len(claimedMark):]
	j := strings.LastIndex(suffix, "-")
	if j < 0 {
		return time.Time{}, false
	}
	nanos, err := strconv.ParseInt(suffix[j+1:], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, nanos), true
}

// drain delivers every spooled batch and removes it.
//
// Exclusion is by claim-by-rename: a drainer atomically renames a file to a
// unique claim and works on that immutable snapshot. rename(2) fails once the
// source is gone, so of two racing drainers exactly one wins — including when
// the file being claimed is itself an abandoned claim being adopted, which is
// why adoption re-renames rather than working in place.
//
// Delivery failures drop the batch rather than retrying: an offline box must
// not accumulate a backlog it will one day flush all at once, and losing
// aggregate usage counts costs nothing worth a retry queue.
func drain(spoolDir string, now time.Time, send func([]byte) error) {
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		return // no spool yet, or unreadable — nothing to do, never an error
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		path := filepath.Join(spoolDir, name)
		isClaim := strings.Contains(name, claimedMark)
		if !isClaim && !strings.HasSuffix(name, spoolSuffix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Age a claim from when it was claimed and a spool file from its last
		// append. A claim whose name predates this scheme falls back to mtime,
		// which only delays its expiry.
		stamp, haveStamp := claimStamp(name)
		age := now.Sub(info.ModTime())
		if isClaim && haveStamp {
			age = now.Sub(stamp)
		}
		if age > maxSpoolAge {
			debugf("dropping spool file older than %s: %s", maxSpoolAge, name)
			os.Remove(path)
			continue
		}
		if isClaim && (!haveStamp || now.Sub(stamp) < claimReclaimAfter) {
			continue // a live drainer may still hold this
		}

		claimed := claimName(spoolDir, unclaimedBase(name), now)
		if err := os.Rename(path, claimed); err != nil {
			continue // another drainer claimed it first
		}
		data, readErr := os.ReadFile(claimed)
		if readErr != nil || len(data) == 0 {
			os.Remove(claimed)
			continue
		}
		sendErr := send(data)
		// Removed only AFTER the send attempt: if this process exits mid-send
		// the file survives as a claim and a later run adopts it, instead of the
		// events vanishing into a delete that already happened.
		os.Remove(claimed)
		if sendErr != nil {
			debugf("send failed, dropping batch and stopping: %v", sendErr)
			return
		}
	}
}
