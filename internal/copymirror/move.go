package copymirror

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// ErrDestHoldsSnapshots: MoveSnapshots found a snapshot in the folder it was
// to fill. That folder's own snapshots serve, and nothing is moved.
var ErrDestHoldsSnapshots = errors.New("the folder already holds snapshots")

// ErrBusy: another round holds the mirror.
var ErrBusy = errors.New("another process is using the mirror")

// MoveSnapshots moves the complete snapshots of the mirror at dir into dest,
// a server's local snapshot folder that holds none (#2431: a server that kept
// its snapshots only in S3 and is given a folder keeps what its mirror already
// brought). Oldest first, each marked complete again where it now lives, which
// writes its own views file there and moves dest's `current` pointer to the
// newest. A snapshot that is not complete, an interrupted download and the
// mirror's own files stay; the sweep removes them with the mirror.
//
// An absent mirror moves nothing and is no error. A rename that fails (dest
// on another disk) stops the move: what was moved stays, complete, and the
// error says where it stopped.
func MoveSnapshots(dir, dest string) (moved []string, err error) {
	if dir == "" {
		return nil, nil
	}
	if _, err := os.Lstat(dir); errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	m := &mirror{cfg: Config{Dir: dir}}
	unlock, busy, err := lockMirror(dir)
	if err != nil {
		return nil, err
	}
	if busy {
		return nil, ErrBusy
	}
	defer unlock()
	held, err := m.held()
	if err != nil {
		return nil, err
	}
	if len(held) == 0 {
		return nil, nil
	}
	if fi, err := os.Lstat(filepath.Join(dir, SourceFile)); err != nil || !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s: %w", dir, ErrNotAMirror)
	}
	entries, err := os.ReadDir(dest)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dest, err)
	}
	for _, e := range entries {
		if _, ok := snapshotdir.ParseTime(e.Name()); ok && e.IsDir() {
			return nil, fmt.Errorf("%s: %w", dest, ErrDestHoldsSnapshots)
		}
	}
	for i := len(held) - 1; i >= 0; i-- { // held is newest first
		name := held[i].name
		to := filepath.Join(dest, name)
		if _, err := os.Lstat(to); !errors.Is(err, fs.ErrNotExist) {
			return moved, fmt.Errorf("move snapshot %s into %s: something is already there under its name", name, dest)
		}
		if err := os.Rename(filepath.Join(dir, name), to); err != nil {
			return moved, fmt.Errorf("move snapshot %s into %s: %w", name, dest, err)
		}
		moved = append(moved, name)
		if err := baseline.WriteSuccessMarker(to); err != nil {
			return moved, fmt.Errorf("mark snapshot %s complete in %s: %w", name, dest, err)
		}
	}
	return moved, nil
}
