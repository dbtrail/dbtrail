package baseline

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// CurrentLinkName is the name of the pointer a baseline root carries alongside
// its timestamped snapshot directories:
//
//	<root>/2026-08-30T03-00-00Z/   <- a snapshot
//	<root>/2026-08-31T03-00-00Z/   <- a newer snapshot
//	<root>/current -> 2026-08-31T03-00-00Z
//
// It exists so a generated artifact can name a path that stays correct after
// the next snapshot lands. `bintrail views` points its table views (shop.orders)
// at <root>/current/... by default, so a periodically refreshed baseline
// reaches an already-generated views file without regenerating it (#1484).
//
// The pointer is a SYMLINK, not a copy or a rewritten directory, for one
// reason: replacing it is a single rename(2), so no path ever resolves into a
// half-published snapshot, and a reader mid-query holds already-open files and
// finishes against the snapshot it started on.
//
// State that precisely rather than as an absolute. rename(2) does not serialize
// a query against the swap: a statement resolving two state views either side
// of one can still land on different snapshots. What the pointer buys is that
// the window is a single rename rather than the hours a file left behind would
// span, and that no reader ever sees a partially updated pointer.
//
// It is deliberately NOT part of discovery. FindBaseline, ListBaselines,
// DiscoverBaselines and PruneLocal all enumerate with os.ReadDir + IsDir(),
// which reports false for a symlink, and every one of them additionally
// requires the name to parse as a timestamp. So "current" is invisible to
// recovery: it can never be selected as a baseline, and it can never be
// pruned. Recovery reads snapshots by their own names, as it always has.
const CurrentLinkName = "current"

// currentLinkTmp prefixes the staging name PublishCurrentPointer renames from.
// It is dot-prefixed and does not parse as a timestamp, so a leftover from a
// crash between the symlink and the rename is invisible to discovery too (and
// is a dangling symlink of a few bytes, not a tree).
const currentLinkTmp = "." + CurrentLinkName + ".tmp"

// stagingName returns a per-call staging name under a baselines root. It must
// stay UNIQUE per call: a shared one lets two producers writing into the same
// root interleave, so that one removes the other's staged link and the winner
// publishes a target it never chose.
//
// Uniqueness alone does NOT make concurrent publishing correct -- both
// publishers would still race to rename, and the last one would win whether or
// not its snapshot is newer. lockPointer is what settles that; the unique name
// only stops two publishers from destroying each other's staged link, which is
// a separate failure it would be wrong to leave open.
func stagingName() string {
	// The counter, not the clock, is what makes this unique. time.Now() is
	// coarse enough on macOS that two calls in one loop return the SAME
	// UnixNano, which the uniqueness test caught; the timestamp is kept only
	// because it makes a leftover legible. The pid separates processes.
	return fmt.Sprintf("%s.%d.%d.%d", currentLinkTmp,
		os.Getpid(), time.Now().UnixNano(), stagingSeq.Add(1))
}

var stagingSeq atomic.Uint64

// PublishCurrentPointer repoints <root>/current at snapshotDir, where root is
// snapshotDir's parent. It is called from WriteSuccessMarker, so the pointer
// moves exactly when a snapshot becomes complete — never before, so the
// pointer can never name a snapshot that is still being written.
//
// Three rules, each of which only ever declines to move the pointer:
//
//   - Snapshot directories only. snapshotDir's base name must parse as a
//     baseline timestamp, the same predicate discovery uses. `reconstruct
//     --output-format mydumper` writes its _SUCCESS into an operator-chosen
//     dump directory that is not a snapshot under a baselines root; that call
//     lands here and does nothing.
//   - Forward only. A snapshot can be produced for a past instant (`reconstruct
//     --output-format parquet --at <yesterday>` does exactly that), and
//     completing it must not walk the pointer backwards to it.
//   - Never clobber real data. If <root>/current exists and is not a symlink,
//     an operator has a directory or file by that name; refuse rather than
//     remove it.
//
// What it deliberately does NOT check is that the new snapshot carries every
// table the current one does. The pointer names the newest COMPLETE snapshot,
// whatever tables that snapshot holds, and a followed views file can therefore
// lose a view when the table set shrinks: the file keeps naming
// <root>/current/<schema>/<table>.parquet, which stops resolving. The generated
// file checks for exactly that up front and refuses by TABLE NAME before it
// creates anything (#1558), rather than letting the read break at whichever
// view happens to be the one that moved.
//
// That is the chosen failure, not an oversight. A superset rule would freeze
// the pointer FOREVER the first time a table is legitimately dropped, since no
// later snapshot could ever satisfy it again, and every already-generated file
// would go on returning older and older rows with nothing to indicate it. This
// product's whole reason for existing argues the other way: a loud failure that
// names the missing table beats silent staleness. The generated file's header
// says to regenerate after a table is added or dropped, which is exactly this
// case.
//
// The unattended producers do not shrink the table set anyway: the periodic
// refresh folds exactly the tables of the snapshot it read (newestSnapshotTables),
// and the console's dump filters by configured SCHEMA, not by table. Reaching
// this needs an operator action -- `--tables` on a hand-run baseline, or
// narrowing a server's schemas -- which is the same act the header already
// tells them to regenerate after.
//
// The link target is the bare directory name, not an absolute path, so a
// baselines root stays valid after being moved, bind-mounted at a different
// path in a container, or copied to another host.
func PublishCurrentPointer(snapshotDir string) error {
	name := filepath.Base(snapshotDir)
	ts, ok := snapshotdir.ParseTime(name)
	if !ok {
		return nil // not a snapshot directory under a baselines root
	}
	// A snapshot dated in the FUTURE never takes the pointer. `bintrail baseline
	// --timestamp` accepts any ISO 8601 with no upper bound, and a host clock
	// that jumps forward produces the same shape. Letting one take the pointer
	// froze it forever: every later real snapshot then found the future one as
	// the newest and declined, so every followed file served that snapshot's
	// rows indefinitely with no error at query time. computeKeepers already
	// skips future-dated snapshots for the same reason.
	if ts.After(time.Now().UTC()) {
		return nil
	}
	root := filepath.Dir(snapshotDir)
	link := filepath.Join(root, CurrentLinkName)

	fi, err := os.Lstat(link)
	switch {
	case err == nil && fi.Mode()&os.ModeSymlink == 0:
		return fmt.Errorf("%s exists and is not a symlink; refusing to replace it "+
			"(move it aside to let baseline snapshots publish the %s pointer)", link, CurrentLinkName)
	case err == nil:
		// An unreadable or unparseable target is treated as "no usable pointer"
		// and replaced: a dangling or hand-edited link is worse than a correct
		// one, and anything this function wrote parses. The forward-only rule
		// itself is below, against the snapshot directories rather than against
		// this link.
	case !os.IsNotExist(err):
		return fmt.Errorf("stat %s: %w", link, err)
	}

	// Forward only, decided against the newest COMPLETE snapshot on disk. See
	// newestCompleteSnapshot for why the pointer itself is the wrong thing to
	// compare with. A snapshot produced for a past instant (`reconstruct
	// --output-format parquet --at`) completes normally and simply does not
	// take the pointer.
	// Serialize the read-then-write. Each half is correct alone; the PAIR is
	// what a peer can interleave with, and narrowing the gap never closed it.
	unlock, err := lockPointer(root)
	if err != nil {
		return err
	}
	defer unlock()

	best, err := newestCompleteSnapshot(root)
	if err != nil {
		// Refuse, do not fall through. Reaching the swap with the rule skipped
		// is the guard disarming itself: a snapshot produced for a past instant
		// would take the pointer, every followed file would start serving that
		// day's rows, and forward-only would pin the mistake in place.
		return fmt.Errorf("cannot decide whether %s may take the pointer: %w", name, err)
	}
	if best != "" && best != name {
		if bestTS, ok := snapshotdir.ParseTime(best); ok && bestTS.After(ts) {
			return nil
		}
	}

	// A per-process staging name, NOT a shared one. Two producers writing into
	// the same root would otherwise interleave: A stages its link, B removes it
	// and stages its own, and A's rename then publishes B's target while A
	// reports success. With a snapshot legitimately produced for a past instant
	// (`reconstruct --output-format parquet --at`), that lands the pointer on
	// the OLDER snapshot and forward-only keeps it there.
	if err := swapPointer(root, link, name); err != nil {
		return err
	}
	sweepStagingLeftovers(root)
	return nil
}

// newestCompleteSnapshot returns the name of the newest COMPLETE snapshot
// directory under root, and whether there is one.
//
// This is the source of truth the forward-only rule compares against, and it is
// chosen precisely because nothing overwrites it. Comparing against the POINTER
// cannot be made correct under concurrency: two publishers read the same old
// pointer, both conclude they may advance, and the last rename wins whichever
// snapshot is newer. A re-read afterwards does not save it either -- the loser
// sees its OWN name landed and concludes it won legitimately, because the
// evidence that it clobbered a newer one is exactly what it overwrote.
// Measured with a re-read loop: 11 of 120 concurrent pairs still ended on the
// older snapshot.
//
// Snapshot directories are not overwritten, and _SUCCESS is written before
// PublishCurrentPointer is called, so by the time a publisher runs, every
// snapshot that could outrank it is already visible and already marked
// complete.
//
// This listing is what the decision is MADE from; it is not what makes the
// decision safe. Read and rename are two syscalls, and a peer can complete a
// newer snapshot between them however fresh the listing is -- measured at 12 in
// 400 concurrent pairs under load. lockPointer serializes the pair. Comparing
// against the directories rather than the pointer still matters: it is the only
// source neither publisher overwrites, so the rule stays correct for a peer that
// crashed while holding nothing.
func newestCompleteSnapshot(root string) (string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", fmt.Errorf("read the baselines root: %w", err)
	}
	now := time.Now().UTC()
	var bestName string
	var bestTS time.Time
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		ts, ok := snapshotdir.ParseTime(e.Name())
		if !ok || ts.After(now) || !SnapshotComplete(filepath.Join(root, e.Name())) {
			continue
		}
		if bestName == "" || ts.After(bestTS) {
			bestName, bestTS = e.Name(), ts
		}
	}
	return bestName, nil
}

// swapPointer stages a link under a unique name and renames it over the
// pointer. rename(2) replaces an existing symlink in one step, so a reader
// resolves either the old target or the new one, never a half-written pointer.
func swapPointer(root, link, name string) error {
	tmp := filepath.Join(root, stagingName())
	if err := os.Symlink(name, tmp); err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, link); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("publish %s: %w", link, err)
	}
	return nil
}

// sweepStagingLeftovers removes dangling staging links from publishes that died
// between the symlink and the rename. Best-effort and unlogged: each is a
// dangling symlink of a few bytes, invisible to discovery, and the upload skips
// them by prefix. Without this they would accumulate, one per crashed publish,
// since the staging name is unique per call and the prune and discard sweeps
// both require IsDir().
func sweepStagingLeftovers(root string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	mine := fmt.Sprintf("%s.%d.", currentLinkTmp, os.Getpid())
	for _, e := range entries {
		// OUR leftovers only. A bare prefix match would remove a peer's staged
		// link in the window between its Symlink and its Rename, turning a
		// benign race into a publish failure. A peer sweeps its own on its next
		// publish, and a dead process's are a dangling symlink of a few bytes.
		if e.Type()&os.ModeSymlink != 0 && strings.HasPrefix(e.Name(), mine) {
			_ = os.Remove(filepath.Join(root, e.Name()))
		}
	}
}

// ResolveCurrentPointer returns the snapshot directory name <root>/current
// points at, and whether the root carries a usable pointer at all. A missing
// pointer, a non-symlink, or a target that does not parse as a baseline
// timestamp all report false: callers fall back to naming a snapshot directly.
//
// It deliberately does NOT check that the target exists. The pointer's value is
// that it is read at QUERY time by whatever opens the generated file, which may
// be long after and on another host; reporting "usable" from the state of this
// filesystem right now would be the wrong question.
func ResolveCurrentPointer(root string) (string, bool) {
	link := filepath.Join(root, CurrentLinkName)
	fi, err := os.Lstat(link)
	if err != nil || fi.Mode()&os.ModeSymlink == 0 {
		return "", false
	}
	target, err := os.Readlink(link)
	if err != nil {
		return "", false
	}
	// A BARE name, not filepath.Base of an arbitrary target. PublishCurrentPointer
	// only ever writes a bare directory name, so anything with a separator in it
	// was made by hand -- and taking its Base would let `current ->
	// ../../other-root/<same timestamp>` pass RewriteToPointer's equality check
	// and redirect every state view outside this root.
	if target != filepath.Base(target) {
		return "", false
	}
	if _, ok := snapshotdir.ParseTime(target); !ok {
		return "", false
	}
	return target, true
}

// RewriteToPointer rewrites baseline table paths under root so they read
// through <root>/current instead of naming a snapshot directory, and reports
// whether it did. It is the shared half of "follow the newest snapshot": both
// producers of a views file (the `bintrail views` command and the console
// download) apply it, so neither can drift into a different rule about when
// following is safe.
//
// It rewrites nothing and reports false unless EVERY path is a table of the one
// snapshot the pointer currently names. A pointer naming some other snapshot is
// not a degraded version of following — it would serve rows from a snapshot the
// generated file does not describe, which is worse than being one refresh
// behind. Same for a root with no usable pointer (every root written before
// this feature, until its next snapshot completes) and for a root that is not a
// local directory at all: an s3:// baseline destination reaches this function
// verbatim from the console, and filepath.Rel succeeds on one, so the URL check
// below is what refuses it rather than any caller filtering first.
// It also returns each path's tail below its snapshot directory
// ("<schema>/<table>.parquet", forward-slashed), because splitSnapshotPath
// already computes it to decide the rewrite at all. Handing it back is what
// keeps a caller from deriving the same tail a SECOND way: this function
// resolves the root with filepath.Rel, which CLEANS both sides, and a caller
// cutting a raw byte prefix instead disagrees the moment the root is spelled
// "./baselines", "/data/bl//" or "/data/./bl" (#1558). The two answers being
// derived once is the point; returning them separately is the mechanism.
func RewriteToPointer(root string, paths []string) ([]string, []string, bool) {
	// An empty root is the --baseline-s3 shape on the CLI side. Refused rather
	// than passed through: ResolveCurrentPointer would probe a bare "current"
	// against the PROCESS working directory, which is the same class of mistake
	// as reading a server-side filename against the wrong base.
	// A URL root, not a directory. The console hands its configured baseline
	// destination through verbatim, and that can be an s3:// URL: filepath.Rel
	// succeeds on one, so the only thing that would refuse the rewrite is an
	// os.Lstat of "s3:/bucket/.../current" — a RELATIVE path, probed against
	// the process working directory. Refusing structurally is one line and
	// removes the probe; leaving it to Lstat makes the outcome depend on what
	// happens to sit next to the daemon's CWD.
	if root == "" || strings.Contains(root, "://") || len(paths) == 0 {
		return nil, nil, false
	}
	pointer, ok := ResolveCurrentPointer(root)
	if !ok {
		return nil, nil, false
	}
	current := filepath.Join(root, CurrentLinkName)
	out := make([]string, len(paths))
	rels := make([]string, len(paths))
	for i, path := range paths {
		snapshot, rest := splitSnapshotPath(root, path)
		if snapshot != pointer {
			return nil, nil, false
		}
		out[i] = filepath.Join(current, rest)
		rels[i] = filepath.ToSlash(rest)
	}
	return out, rels, true
}

// splitSnapshotPath splits a baseline table path into its snapshot directory
// name and the part below it (<schema>/<table>.parquet). Both are "" when path
// is not a table under root.
func splitSnapshotPath(root, path string) (snapshot, rest string) {
	rel, err := filepath.Rel(root, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", ""
	}
	// filepath.Rel cleans its result, so a path naming the snapshot directory
	// itself has no separator left to cut, with or without a trailing one. That
	// is why "there is something below the snapshot directory" needs no check of
	// its own: !found already covers it.
	snapshot, rest, found := strings.Cut(rel, string(filepath.Separator))
	if !found {
		return "", ""
	}
	return snapshot, rest
}
