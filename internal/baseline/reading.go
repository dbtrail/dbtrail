package baseline

import (
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// A long reader of OLDER snapshots, in this process, stops the local prune of
// its folder while it runs (#2678). The count has no time floor any more and
// is applied after each snapshot, so a job that stays on one older snapshot
// for minutes or hours (a restore to a moment, a .sql build, a check, a
// download) would otherwise have it removed after N more snapshots: 15
// minutes at the defaults. The S3 cleanup already stands still for a restore
// and a .sql build (consoleapp s3PruneBusyFn); this is the same rule for the
// local folder, asked by the prune itself so that no caller of it can forget.
//
// In memory, on purpose: a reader that dies with the process holds nothing,
// and one in ANOTHER process is not seen (the command-line tools reading a
// folder the daemon prunes get an error when their snapshot goes, and run
// again).

// readHoldLimit is how long one registration stops the prune. Past it the
// reader is taken for stuck and the prune goes on: a job that never ends
// must not let a folder that gets a snapshot every few minutes fill the
// disk. None of these jobs declares a run time of its own (a restore, a
// .sql build, a check and a download are bounded only by the daemon's
// life), so this is not sized against one: it is a day, the longest
// interval a schedule can have, so a reader still registered after it has
// outlived every slot of every schedule. A job that really runs longer
// loses the snapshot it is on, fails, and is started again.
const readHoldLimit = 24 * time.Hour

type readHold struct {
	what  string
	since time.Time
}

var (
	readsMu sync.Mutex
	reads   = map[string]map[*readHold]struct{}{}
)

// readKey is root with symlinks resolved, so a reader and the prune that
// spell one folder two ways meet. A root that does not resolve (not created
// yet) is compared by its cleaned absolute path.
func readKey(root string) string {
	if p, err := filepath.EvalSymlinks(root); err == nil {
		root = p
	}
	if abs, err := filepath.Abs(root); err == nil {
		root = abs
	}
	return filepath.Clean(root)
}

// BeginRead registers a job that reads snapshots of root which may be older
// than the newest, and returns what ends it. what names the job in the log
// ("a restore to a moment"). While any is registered, PruneLocal removes
// nothing from root and reports PruneResult.Reading. end is safe to call
// more than once; call it on every exit (defer), a panic included.
func BeginRead(root, what string) (end func()) {
	if root == "" || strings.HasPrefix(root, "s3://") {
		// Nothing local to protect: the S3 cleanup has its own rule.
		return func() {}
	}
	key := readKey(root)
	h := &readHold{what: what, since: time.Now()}
	readsMu.Lock()
	if reads[key] == nil {
		reads[key] = map[*readHold]struct{}{}
	}
	reads[key][h] = struct{}{}
	readsMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			readsMu.Lock()
			delete(reads[key], h)
			if len(reads[key]) == 0 {
				delete(reads, key)
			}
			readsMu.Unlock()
		})
	}
}

// readingNow reports the reader that stops a prune of root now: the one
// registered longest ago among those inside readHoldLimit, and when it
// registered. stuck counts the registrations past the limit, which no longer
// stop it.
func readingNow(root string, now time.Time) (what string, since time.Time, stuck int) {
	key := readKey(root)
	readsMu.Lock()
	defer readsMu.Unlock()
	for h := range reads[key] {
		if now.Sub(h.since) > readHoldLimit {
			stuck++
			continue
		}
		if what == "" || h.since.Before(since) {
			what, since = h.what, h.since
		}
	}
	return what, since, stuck
}

// Reading names the job that stops a prune of root now, "" when none does:
// what PruneLocal would report as PruneResult.Reading.
func Reading(root string) string {
	what, _, _ := readingNow(root, time.Now())
	return what
}

// A reader that is on ONE known snapshot pins that snapshot instead of
// stopping the folder's prune (#2678): a snapshot download, a SQL statement,
// the Explain of a check. The prune goes on, keeps the pinned snapshot
// (PruneResult.KeptPinned) and removes the others past the count, so however
// many such readers there are, and however long one of them takes, the
// folder holds at most the snapshots they are reading on top of its count.
//
// Every pin has a deadline, given by who takes it from what it reads (a
// download: the size of the snapshot). Past it the pin no longer keeps the
// snapshot: a reader whose length someone else decides (an HTTP client)
// must not decide how long a snapshot stays on the disk.

type snapshotPin struct {
	what  string
	until time.Time
}

var pins = map[string]map[string]map[*snapshotPin]struct{}{} // root -> snapshot name -> pins; under readsMu

// PinSnapshot keeps the snapshot root/name from the prune until release is
// called or limit has passed, whichever is first. limit <= 0 is
// readHoldLimit. release is safe to call more than once.
func PinSnapshot(root, name, what string, limit time.Duration) (release func()) {
	if root == "" || name == "" || strings.HasPrefix(root, "s3://") {
		return func() {}
	}
	if limit <= 0 {
		limit = readHoldLimit
	}
	key := readKey(root)
	p := &snapshotPin{what: what, until: time.Now().Add(limit)}
	readsMu.Lock()
	if pins[key] == nil {
		pins[key] = map[string]map[*snapshotPin]struct{}{}
	}
	if pins[key][name] == nil {
		pins[key][name] = map[*snapshotPin]struct{}{}
	}
	pins[key][name][p] = struct{}{}
	readsMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			readsMu.Lock()
			delete(pins[key][name], p)
			if len(pins[key][name]) == 0 {
				delete(pins[key], name)
			}
			if len(pins[key]) == 0 {
				delete(pins, key)
			}
			readsMu.Unlock()
		})
	}
}

// pinnedNow returns the snapshots of root a pin still keeps at now, each
// with the reader that pins it.
func pinnedNow(root string, now time.Time) map[string]string {
	key := readKey(root)
	readsMu.Lock()
	defer readsMu.Unlock()
	var out map[string]string
	for name, set := range pins[key] {
		for p := range set {
			if now.Before(p.until) {
				if out == nil {
					out = map[string]string{}
				}
				out[name] = p.what
				break
			}
		}
	}
	return out
}

// Pinned reports the snapshots of root a reader pins now, for tests and
// listings.
func Pinned(root string) []string {
	var out []string
	for name := range pinnedNow(root, time.Now()) {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// PinSnapshotCapped is PinSnapshot for readers anyone allowed can start as
// many of as they like (a snapshot download): it refuses, with ok false, a
// pin that would make more than max DIFFERENT snapshots of root pinned by
// readers of the same kind (what). Another reader of a snapshot already
// pinned is always let in: it keeps nothing more on the disk. So downloads
// repeated at will keep at most max snapshots past the count, not every
// snapshot there is.
func PinSnapshotCapped(root, name, what string, limit time.Duration, max int) (release func(), ok bool) {
	if root == "" || name == "" || strings.HasPrefix(root, "s3://") {
		return func() {}, true
	}
	key := readKey(root)
	now := time.Now()
	readsMu.Lock()
	distinct, already := 0, false
	for n, set := range pins[key] {
		for p := range set {
			if p.what == what && now.Before(p.until) {
				distinct++
				already = already || n == name
				break
			}
		}
	}
	readsMu.Unlock()
	if !already && distinct >= max {
		return func() {}, false
	}
	return PinSnapshot(root, name, what, limit), true
}
