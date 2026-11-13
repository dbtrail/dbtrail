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
// In memory, on purpose: a reader that dies with the process holds nothing.
// A reader in ANOTHER process is not seen here; it holds the one snapshot it
// reads with HoldSnapshot (readhold_unix.go, #2675), which the prune of any
// process honours.

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
	key := readKey(root)
	readsMu.Lock()
	defer readsMu.Unlock()
	return insertPinLocked(key, name, what, limit)
}

// insertPinLocked adds one pin and returns what releases it. The caller
// holds readsMu, so a check made under the same lock (PinDownload)
// and this insert are one step.
func insertPinLocked(key, name, what string, limit time.Duration) (release func()) {
	if limit <= 0 {
		limit = readHoldLimit
	}
	p := &snapshotPin{what: what, until: time.Now().Add(limit)}
	if pins[key] == nil {
		pins[key] = map[string]map[*snapshotPin]struct{}{}
	}
	if pins[key][name] == nil {
		pins[key][name] = map[*snapshotPin]struct{}{}
	}
	pins[key][name][p] = struct{}{}
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

// downloadReader names a snapshot download's pin: the kind of reader the cap
// below counts.
const downloadReader = "a snapshot download"

// PinDownload pins the snapshot a download streams, under a cap on what the
// downloads of one folder hold back from the cleanup together (#2678).
// Anyone allowed to download can start as many as they like, and a download
// lasts as long as its client takes, so without a cap downloads repeated at
// will keep any number of old snapshots on the disk.
//
// kept are the snapshots the cleanup keeps NOW whatever readers there are
// (KeptNow), read just before this call. The cap is worked out on that
// current state, not on what a snapshot was when its download started: a
// download of the newest snapshot holds nothing back at first, and five
// minutes later it is the only thing keeping a snapshot past the count.
//
//   - Another download of a snapshot a download already pins: let in. It
//     keeps nothing more on the disk.
//   - Any other snapshot: refused once the downloads pin ceiling different
//     snapshots in all. A snapshot the cleanup keeps today is past the
//     count a few minutes on, so every pin counts toward this.
//   - A snapshot past what the cleanup keeps: also refused once max such
//     snapshots are pinned by downloads.
//
// So the downloads of a folder never pin more than ceiling snapshots, and
// that is the most they can hold back from the cleanup, however many
// requests are made and for however long.
// The count and the insert are ONE step under the lock: requests arriving
// together must not all pass the check before any of them is counted.
func PinDownload(root, name string, limit time.Duration, kept map[string]bool, max, ceiling int) (release func(), ok bool) {
	if root == "" || name == "" || strings.HasPrefix(root, "s3://") {
		return func() {}, true
	}
	key := readKey(root)
	now := time.Now()
	readsMu.Lock()
	defer readsMu.Unlock()
	total, past, already := 0, 0, false
	for n, set := range pins[key] {
		for p := range set {
			if p.what == downloadReader && now.Before(p.until) {
				total++
				if !kept[n] {
					past++
				}
				already = already || n == name
				break
			}
		}
	}
	switch {
	case already:
		// Holds nothing more.
	case total >= ceiling:
		// A rule of its own, asked first and of every snapshot not already
		// pinned: downloads never pin more than ceiling snapshots.
		return func() {}, false
	case !kept[name] && past >= max:
		return func() {}, false
	}
	return insertPinLocked(key, name, downloadReader, limit), true
}

// HoldSnapshotOfTable is HoldSnapshot for the snapshot a table file of it
// lives in: <root>/<snapshot>/<schema>/<table>.parquet, the path
// reconstruct.FindBaseline returns.
func HoldSnapshotOfTable(tablePath string) (release func(), gone bool) {
	// Any URL (s3://, file://, http://) is read unheld: only a plain local
	// path names a folder whose marker can be held.
	if tablePath == "" || strings.Contains(tablePath, "://") {
		return func() {}, false
	}
	return HoldSnapshot(filepath.Dir(filepath.Dir(tablePath)))
}
