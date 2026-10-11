package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

// copyMirrorTargets lists the servers that have a mirror: those the registry
// gives a mirror folder to.
func copyMirrorTargets(reg *console.Registry) []copyMirrorTarget {
	if reg == nil {
		return nil
	}
	var out []copyMirrorTarget
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
	mu sync.Mutex
	m  map[string]copyMirrorStatus
}

func newCopyMirrorState() *copyMirrorState {
	return &copyMirrorState{m: map[string]copyMirrorStatus{}}
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
	return copymirror.Sync(ctx, copymirror.Config{Dir: t.dir, Source: src, SourceName: t.s3})
}

// runCopyMirrorRound runs one round for every server that has a mirror, one
// after another. A server that fails, or whose round panics, does not keep
// the next one from its round.
func runCopyMirrorRound(ctx context.Context, reg *console.Registry, state *copyMirrorState,
	sync func(context.Context, copyMirrorTarget) (copymirror.Result, error)) {
	for _, t := range copyMirrorTargets(reg) {
		if ctx.Err() != nil {
			return
		}
		res, err := guardedCopyMirrorSync(ctx, t, sync)
		st := copyMirrorStatus{At: time.Now().UTC(), Newest: res.Newest, SourceNewest: res.SourceNewest}
		if err != nil {
			st.Err = err.Error()
		}
		reportCopyMirror(t, res, err, state.set(t.id, st))
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

// reportCopyMirror logs what a round did. A failure that repeats is logged
// once, not every five minutes; so is a round with nothing to do.
func reportCopyMirror(t copyMirrorTarget, res copymirror.Result, err error, prev copyMirrorStatus) {
	for _, n := range res.Notes {
		slog.Warn("copy mirror: "+n, "server", t.name)
	}
	switch {
	case err != nil && errors.Is(err, context.Canceled):
		// The daemon is stopping.
	case err != nil && err.Error() != prev.Err:
		slog.Warn("copy mirror: the local copy SQL reads for this server could not be brought up to date; "+
			"it keeps answering from the snapshot it has, if any, and the next round tries again",
			"server", t.name, "source", t.s3, "mirror_newest", res.Newest, "error", err)
	case err == nil && res.Downloaded != "":
		slog.Info("copy mirror: brought a snapshot from S3 for SQL on the copy",
			"server", t.name, "snapshot", res.Downloaded, "downloaded_bytes", res.BytesDownloaded,
			"linked_bytes", res.BytesLinked, "resumed_bytes", res.BytesResumed, "removed", res.Removed)
	case err == nil && prev.Err != "":
		slog.Info("copy mirror: this server's mirror is being followed again", "server", t.name, "mirror_newest", res.Newest)
	}
}

// sweepCopyMirrors removes the mirror folders no server uses any more: a
// server's folder for a bucket it no longer points at, a removed server's,
// one of a server that now keeps a local folder. Only folders two levels
// below the mirror root, which is this code's own; a file or a link there is
// left alone.
func sweepCopyMirrors(reg *console.Registry) {
	root := reg.CopyMirrorRoot()
	if root == "" {
		return
	}
	wanted := map[string]bool{}
	for _, t := range copyMirrorTargets(reg) {
		wanted[t.dir] = true
	}
	servers, err := os.ReadDir(root)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("copy mirror: cannot look for mirror folders that are no longer used", "dir", root, "error", err)
		}
		return
	}
	for _, s := range servers {
		if !s.IsDir() {
			continue
		}
		serverDir := filepath.Join(root, s.Name())
		mirrors, err := os.ReadDir(serverDir)
		if err != nil {
			slog.Warn("copy mirror: cannot look for mirror folders that are no longer used", "dir", serverDir, "error", err)
			continue
		}
		for _, m := range mirrors {
			dir := filepath.Join(serverDir, m.Name())
			if !m.IsDir() || wanted[dir] {
				continue
			}
			if err := os.RemoveAll(dir); err != nil {
				slog.Warn("copy mirror: could not remove a mirror folder no server uses any more", "dir", dir, "error", err)
				continue
			}
			slog.Info("copy mirror: removed a mirror folder no server uses any more", "dir", dir)
		}
		// Gone if that left it empty; a folder with anything in it stays.
		_ = os.Remove(serverDir)
	}
}

// startCopyMirrorLoop keeps every S3-only server's mirror up to date for the
// life of the daemon. No flag and no error: with no such server a round does
// nothing, and one can be added at any time.
func startCopyMirrorLoop(ctx context.Context, reg *console.Registry) {
	if reg == nil {
		return
	}
	state := newCopyMirrorState()
	runOnce := func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("copy mirror: the round panicked outside any server's own; the daemon is unaffected and the next round runs as usual",
					"panic", r, "stack", string(debug.Stack()))
			}
		}()
		sweepCopyMirrors(reg)
		runCopyMirrorRound(ctx, reg, state, syncCopyMirror)
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
