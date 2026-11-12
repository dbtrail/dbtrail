package baseline

import (
	"path/filepath"
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
// registered longest ago among those inside readHoldLimit. stuck counts the
// registrations past the limit, which no longer stop it.
func readingNow(root string, now time.Time) (what string, stuck int) {
	key := readKey(root)
	readsMu.Lock()
	defer readsMu.Unlock()
	var oldest time.Time
	for h := range reads[key] {
		if now.Sub(h.since) > readHoldLimit {
			stuck++
			continue
		}
		if what == "" || h.since.Before(oldest) {
			what, oldest = h.what, h.since
		}
	}
	return what, stuck
}

// Reading names the job that stops a prune of root now, "" when none does:
// what PruneLocal would report as PruneResult.Reading.
func Reading(root string) string {
	what, _ := readingNow(root, time.Now())
	return what
}
