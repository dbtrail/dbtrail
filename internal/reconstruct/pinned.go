package reconstruct

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"strings"
	"time"
)

// PinFunc keeps the snapshot that holds the table file at path from the
// local cleanup while one reader reads it, until release is called (#2691).
// what names the reader in the cleanup's log. It is given by the process
// that prunes the folder (the console), which knows the folder's count and
// the cap on what readers may hold back; a nil PinFunc pins nothing, which is
// right for a process that prunes nothing (a standalone shim or MCP server).
type PinFunc func(path, what string) (release func())

// FindFunc is a snapshot lookup with its location bound: FindBaseline's
// shape.
type FindFunc func(ctx context.Context, schema, table string, at time.Time) (string, time.Time, StaleWarning, error)

// FindPinned is find, with the snapshot it found pinned by pin until release
// is called: the way a reader of one table at a past moment (/api/reconstruct,
// the MCP reconstruct tool, `_snapshot` through the port) keeps the cleanup
// from removing the snapshot under it (#2691). Call release on every exit,
// once the last read of the snapshot is done (defer).
//
// The lookup and the pin are two steps, so the cleanup can remove the
// snapshot between them. When the table file is gone once the pin is held,
// the snapshot is looked up again, once: the next lookup finds what is there
// now, or says there is nothing. A file still missing after that is left to
// the read, which reports it.
func FindPinned(ctx context.Context, find FindFunc, pin PinFunc, schema, table string, at time.Time, what string) (path string, snapshotTime time.Time, stale StaleWarning, release func(), err error) {
	for attempt := 0; ; attempt++ {
		path, snapshotTime, stale, err = find(ctx, schema, table, at)
		if err != nil || pin == nil {
			return path, snapshotTime, stale, func() {}, err
		}
		release = pin(path, what)
		if attempt > 0 || !tableFileGone(path) {
			return path, snapshotTime, stale, release, nil
		}
		release()
	}
}

// tableFileGone reports whether the local table file at path is no longer
// there. Any other answer (it is there, it cannot be looked at, it is in
// S3) is not gone: the read says what is wrong with it.
func tableFileGone(path string) bool {
	if path == "" || strings.HasPrefix(path, "s3://") {
		return false
	}
	_, err := os.Stat(path)
	return errors.Is(err, fs.ErrNotExist)
}
