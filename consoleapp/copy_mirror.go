package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/copymirror"
)

// The local mirror of servers whose snapshots are only in S3 (#2302).
//
// SQL on the copy runs in a worker with no network, so it cannot read a
// snapshot that is only in a bucket. This loop keeps, for each such server,
// the newest snapshots of its bucket in a folder under the state directory
// (console.Registry.CopyMirrorDir), which the SQL surfaces read once it holds
// one. A server that keeps a local folder has no mirror: the folder is the
// copy.
//
// One goroutine, one server at a time: a mirror is a read cache, and it must
// never be what takes the daemon's disk, network or memory from capture. For
// the same reason nothing here can stop the daemon: a round that fails is
// logged and tried again, and a panic in one is recovered and reported as
// that server's failure.

// copyMirrorInterval is how often the loop looks for a snapshot it was not
// told about (one published by another process). One listing request per
// server when there is nothing new; the daemon's own uploads kick a round at
// once.
const copyMirrorInterval = 5 * time.Minute

// copyMirrorKick asks the loop for a round now. Buffered by one: a second
// request while one is waiting is the same request.
var copyMirrorKick = make(chan struct{}, 1)

// kickCopyMirror asks for a round without ever waiting for it. Safe to call
// with no loop running.
func kickCopyMirror() {
	select {
	case copyMirrorKick <- struct{}{}:
	default:
	}
}

// copyMirrorTarget is one server to mirror.
type copyMirrorTarget struct {
	id, name string
	dir      string // the mirror folder
	s3       string // the snapshot root it follows
	region   string
}

// bootMirror is the snapshot location the daemon was started with for the
// server on its command line, which is not in the registry.
type bootMirror struct{ dir, s3 string }

// copyMirrorTargets lists the servers that have a mirror: those the registry
// gives a mirror folder to, the command-line server among them.
func copyMirrorTargets(reg *console.Registry, boot bootMirror) []copyMirrorTarget {
	if reg == nil {
		return nil
	}
	var out []copyMirrorTarget
	if dir := reg.BootCopyMirrorDir(boot.dir, boot.s3); dir != "" {
		out = append(out, copyMirrorTarget{id: console.BootServerID, name: console.BootServerID, dir: dir, s3: boot.s3})
	}
	for _, e := range reg.List() {
		if dir := reg.CopyMirrorDir(e); dir != "" {
			out = append(out, copyMirrorTarget{id: e.ID, name: e.Name, dir: dir, s3: e.BaselineS3, region: e.S3Region})
		}
	}
	return out
}

// copyMirrorStatus is what the last round left for one server.
type copyMirrorStatus struct {
	At time.Time
	// Newest is the newest snapshot in the mirror, what SQL reads; ""
	// when it holds none. SourceNewest is the newest the bucket listed.
	Newest, SourceNewest string
	// Err is why the last round failed, "" when it did not.
	Err string
}

type copyMirrorState struct {
	mu   sync.Mutex
	m    map[string]copyMirrorStatus
	said map[string]time.Time
	now  func() time.Time // nil is time.Now
}

func newCopyMirrorState() *copyMirrorState {
	return &copyMirrorState{m: map[string]copyMirrorStatus{}, said: map[string]time.Time{}}
}

// copyMirrorRepeatEvery is how often a line that keeps being true is logged
// again. Not once per round, which is every few minutes for as long as a
// disk stays full; and not once per daemon, which leaves a mirror that has
// been failing for weeks with a single line far up the log.
const copyMirrorRepeatEvery = time.Hour

// changingWords matches every run of letters, digits and the punctuation of
// an identifier that holds a digit: a count, a size, a duration, and the
// request and host ids S3 puts in each error, which are new on every request.
var changingWords = regexp.MustCompile(`[A-Za-z0-9+/=_.:-]*[0-9][A-Za-z0-9+/=_.:-]*`)

// sayNow reports whether a line about server id should be logged now: the
// first time, and again once copyMirrorRepeatEvery has passed. Two texts
// that differ only in their numbers are one line (free bytes, bytes read
// before a connection dropped, a request id), or a failure that carries one
// would be new every round.
func (s *copyMirrorState) sayNow(id, text string) bool {
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	key := id + "\x00" + changingWords.ReplaceAllString(text, "#")
	s.mu.Lock()
	defer s.mu.Unlock()
	if last, ok := s.said[key]; ok && now.Sub(last) < copyMirrorRepeatEvery {
		return false
	}
	// What was said too long ago to hold anything back is forgotten, so the
	// map holds an hour of distinct lines and not a daemon's lifetime of them.
	for k, last := range s.said {
		if now.Sub(last) >= copyMirrorRepeatEvery {
			delete(s.said, k)
		}
	}
	s.said[key] = now
	return true
}

func (s *copyMirrorState) of(id string) copyMirrorStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[id]
}

// set records a round and returns what the round before left.
func (s *copyMirrorState) set(id string, st copyMirrorStatus) (prev copyMirrorStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev = s.m[id]
	s.m[id] = st
	return prev
}

// syncCopyMirror runs one round for one server against its bucket.
func syncCopyMirror(ctx context.Context, t copyMirrorTarget) (copymirror.Result, error) {
	// Opened per round, not kept: see copymirror.OpenS3.
	src, err := copymirror.OpenS3(ctx, t.s3, t.region)
	if err != nil {
		return copymirror.Result{}, err
	}
	return copymirror.Sync(ctx, copymirror.Config{Dir: t.dir, Source: src, SourceName: t.s3,
		// A first download can take long, and the round says nothing else
		// until it ends: without this line the log shows a server whose SQL
		// answers "not ready yet" and no sign that anything is happening.
		OnDownload: func(snapshot string, files int, bytes int64) {
			slog.Info("copy mirror: bringing a snapshot from S3 for SQL on the copy",
				"server", t.name, "snapshot", snapshot, "files", files, "bytes_to_download", bytes)
		}})
}

// runCopyMirrorRound runs one round for every server that has a mirror, one
// after another. A server that fails, or whose round panics, does not keep
// the next one from its round.
func runCopyMirrorRound(ctx context.Context, reg *console.Registry, boot bootMirror, state *copyMirrorState,
	sync func(context.Context, copyMirrorTarget) (copymirror.Result, error)) {
	for _, t := range copyMirrorTargets(reg, boot) {
		if ctx.Err() != nil {
			return
		}
		res, err := guardedCopyMirrorSync(ctx, t, sync)
		st := copyMirrorStatus{At: time.Now().UTC(), Newest: res.Newest, SourceNewest: res.SourceNewest}
		if err != nil {
			st.Err = err.Error()
		}
		prev := state.set(t.id, st)
		// Whether the daemon is stopping is the daemon's context to say,
		// never the error's: a request the round itself gave up on is a
		// cancellation too.
		reportCopyMirror(slog.Default(), state, t, res, err, prev, ctx.Err() != nil)
	}
}

// guardedCopyMirrorSync turns a panic in one server's round into that
// server's error. Unrecovered it would end the daemon, which is the capture
// plane; and it is reported like any failure, never swallowed, so the round
// is not mistaken for one that had nothing to do.
func guardedCopyMirrorSync(ctx context.Context, t copyMirrorTarget,
	sync func(context.Context, copyMirrorTarget) (copymirror.Result, error)) (res copymirror.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("copy mirror: a round panicked; the daemon and the other servers' mirrors are unaffected",
				"server", t.name, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("the round panicked: %v", r)
		}
	}()
	return sync(ctx, t)
}

// reportCopyMirror logs what a round did. What happened once (a snapshot
// brought, a mirror followed again) is said once; what goes on being true (a
// failure, a bucket with nothing to mirror) is said when it starts and again
// every copyMirrorRepeatEvery, never every round.
//
// stopping is whether the daemon is shutting down: a round cut short by that
// has failed at nothing.
func reportCopyMirror(log *slog.Logger, state *copyMirrorState, t copyMirrorTarget, res copymirror.Result, err error, prev copyMirrorStatus, stopping bool) {
	for _, n := range res.Notes {
		if state.sayNow(t.id, "note: "+n) {
			log.Warn("copy mirror: "+n, "server", t.name)
		}
	}
	if res.Downloaded != "" {
		// Said whatever came after: statements already read this snapshot,
		// even if the round then failed at removing an older one.
		log.Info("copy mirror: brought a snapshot from S3 for SQL on the copy",
			"server", t.name, "snapshot", res.Downloaded, "downloaded_bytes", res.BytesDownloaded,
			"linked_bytes", res.BytesLinked, "resumed_bytes", res.BytesResumed, "removed", res.Removed)
	}
	switch {
	case err != nil && stopping:
		// The daemon is stopping.
	case err != nil:
		if state.sayNow(t.id, "error: "+err.Error()) {
			log.Warn("copy mirror: the local copy SQL reads for this server could not be brought up to date; "+
				"it keeps answering from the snapshot it has, if any, and the next round tries again",
				"server", t.name, "source", t.s3, "mirror_newest", res.Newest, "error", err)
		}
	case res.Busy:
		// Another process is bringing this mirror up to date.
	case res.Downloaded != "":
		// Said above.
	case res.Newest == "":
		// No error and nothing to read: SQL on this server answers "not
		// ready yet", and this is the line that says why it will stay so.
		if state.sayNow(t.id, "empty") {
			log.Warn("copy mirror: there is no snapshot to bring for this server, so SQL on the copy has nothing to read: "+
				"the bucket lists no complete snapshot under this location (none taken yet, one still uploading, or another prefix)",
				"server", t.name, "source", t.s3)
		}
	case prev.Err != "":
		log.Info("copy mirror: this server's mirror is being followed again", "server", t.name, "mirror_newest", res.Newest)
	}
}

// sweepCopyMirrors removes the mirror folders no server uses any more: a
// server's folder for a bucket it no longer points at, a removed server's,
// one of a server that now keeps a local folder.
//
// It deletes a whole tree, so it proves the tree is a mirror first, at every
// level, and leaves alone anything it cannot prove:
//
//   - the mirror root and the server's folder are real folders (not links)
//     of this process's user that no other user can write into (ownFolder):
//     where another user can write, an entry can be swapped for a link
//     between the check and the removal;
//   - the folder to remove is one too, and carries the file a mirror writes
//     at its root (copymirror.SourceFile). A folder without it is not a
//     mirror: removed only if it is empty, which is what a mirror made and
//     never started looks like.
func sweepCopyMirrors(reg *console.Registry, boot bootMirror) {
	root := reg.CopyMirrorRoot()
	if root == "" {
		return
	}
	if _, err := os.Lstat(root); errors.Is(err, os.ErrNotExist) {
		return
	}
	if why := ownFolder(root); why != "" {
		warnCopyMirrorSweepOnce(root, "copy mirror: mirror folders that are no longer used are not removed, because the folder that holds them cannot be trusted", why)
		return
	}
	wanted := map[string]bool{}
	for _, t := range copyMirrorTargets(reg, boot) {
		wanted[t.dir] = true
	}
	servers, err := os.ReadDir(root)
	if err != nil {
		warnCopyMirrorSweepOnce(root, "copy mirror: cannot look for mirror folders that are no longer used", err.Error())
		return
	}
	for _, s := range servers {
		serverDir := filepath.Join(root, s.Name())
		if !s.IsDir() || ownFolder(serverDir) != "" {
			continue
		}
		mirrors, err := os.ReadDir(serverDir)
		if err != nil {
			warnCopyMirrorSweepOnce(serverDir, "copy mirror: cannot look for mirror folders that are no longer used", err.Error())
			continue
		}
		for _, m := range mirrors {
			dir := filepath.Join(serverDir, m.Name())
			if !m.IsDir() || wanted[dir] || ownFolder(dir) != "" {
				continue
			}
			if fi, err := os.Lstat(filepath.Join(dir, copymirror.SourceFile)); err != nil || !fi.Mode().IsRegular() {
				// Not shown to be a mirror. Gone if it is empty; otherwise
				// it is somebody's, and stays.
				if os.Remove(dir) != nil {
					warnCopyMirrorSweepOnce(dir, "copy mirror: a folder among the mirrors is not a mirror and was left as it is", "it has no "+copymirror.SourceFile)
				}
				continue
			}
			if err := os.RemoveAll(dir); err != nil {
				warnCopyMirrorSweepOnce(dir, "copy mirror: could not remove a mirror folder no server uses any more", err.Error())
				continue
			}
			slog.Info("copy mirror: removed a mirror folder no server uses any more", "dir", dir)
		}
		// Gone if that left it empty; a folder with anything in it stays.
		_ = os.Remove(serverDir)
	}
}

// copyMirrorSweepWarned keeps a sweep that runs every few minutes from saying
// the same thing about the same folder every time.
var copyMirrorSweepWarned sync.Map

func warnCopyMirrorSweepOnce(dir, msg, why string) {
	if _, said := copyMirrorSweepWarned.LoadOrStore(dir+"\x00"+why, true); !said {
		slog.Warn(msg, "dir", dir, "why", why)
	}
}

// startCopyMirrorLoop keeps every S3-only server's mirror up to date for the
// life of the daemon. No flag and no error: with no such server a round does
// nothing, and one can be added at any time.
//
// bootDir and bootS3 are the snapshot locations the daemon was started with,
// for the server on its command line.
func startCopyMirrorLoop(ctx context.Context, reg *console.Registry, bootDir, bootS3 string) {
	if reg == nil {
		return
	}
	boot := bootMirror{dir: bootDir, s3: bootS3}
	state := newCopyMirrorState()
	runOnce := func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("copy mirror: the round panicked outside any server's own; the daemon is unaffected and the next round runs as usual",
					"panic", r, "stack", string(debug.Stack()))
			}
		}()
		sweepCopyMirrors(reg, boot)
		runCopyMirrorRound(ctx, reg, boot, state, syncCopyMirror)
	}
	go func() {
		if ctx.Err() == nil {
			runOnce()
		}
		t := time.NewTicker(copyMirrorInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			case <-copyMirrorKick:
			}
			runOnce()
		}
	}()
}
