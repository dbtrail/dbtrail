package consoleapp

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A fold, a restore and a compaction all start from a snapshot that is
// already there, and publish their result as this server's. A snapshot
// location has no per-server folder, so the snapshot they start from may be
// another writer's: another server this registry knows (refused earlier, by
// Registry.WriteRefusal), but also writers it cannot see, like the
// command-line server once --baseline-refresh-interval is turned on, a
// cron-driven `bintrail baseline refresh`, or the compose baseline profile.
// Building on another writer's snapshot would publish a snapshot that
// belongs to neither, and take over the tables only it has. Every snapshot
// is signed by its writer (#1762), so the job reads the signature of the
// snapshot it starts from and refuses another writer's (#1684).
//
// An unsigned snapshot, which is every snapshot written before signatures
// existed, keeps today's behaviour: it names no writer, and refusing it
// would stop every existing installation.

// onSnapshotsPage names where the fix is made, from the page's one name.
var onSnapshotsPage = " (" + console.PageSnapshots + " page)"

// errForeignSource marks a refusal of this kind, so the schedule does not
// answer it with a full read into the same location (ForeignSource).
var errForeignSource = errors.New("the snapshot to build on is not this server's")

// sayNotChecked says, once per snapshot, that a writer was not checked.
var (
	notCheckedMu   sync.Mutex
	notCheckedSaid = map[string]bool{}
)

// snapshotSignersFunc is reconstruct.SnapshotSigners behind a seam.
var snapshotSignersFunc = reconstruct.SnapshotSigners

// foldSourceRefusal is the refusal for a job of the server whose index is
// indexDSN that starts from the snapshot of source at at, or nil.
func foldSourceRefusal(indexDSN, source string, at time.Time) error {
	snap := strings.TrimRight(source, "/") + "/" + reconstruct.SnapshotDirName(at)
	signers, known, err := snapshotSignersFunc(source, at)
	if err != nil {
		// Fail closed: the directory was just listed, so a signature that
		// cannot be read is a problem worth stopping for.
		return fmt.Errorf("%w: could not read who wrote the snapshot to build on (%s): %w", errForeignSource, snap, err)
	}
	if !known {
		// An S3 directory no listing here has read: the same as an unsigned
		// one, which is today's behaviour. Said, so it is never silent.
		notCheckedMu.Lock()
		first := !notCheckedSaid[snap]
		notCheckedSaid[snap] = true
		notCheckedMu.Unlock()
		if first {
			slog.Info("snapshot writer: the snapshot to build on has not been read here, so who wrote it is not checked", "snapshot", snap)
		}
		return nil
	}
	if len(signers) == 0 {
		return nil
	}
	own, err := snapshotWriterIDFunc(indexDSN)
	if err != nil {
		return fmt.Errorf("%w: the snapshot to build on (%s) was written by %s, and this server's own identity could not be read to compare (%v); nothing was built. Fix the index connection, or, if that writer is not this server, give this server its own folder or prefix%s",
			errForeignSource, snap, strings.Join(signers, ", "), err, onSnapshotsPage)
	}
	ownID := reconstruct.NormalizeSnapshotWriter(own)
	ok := ownID != ""
	foreign := slices.DeleteFunc(slices.Clone(signers), func(w string) bool { return w == ownID })
	if len(foreign) == 0 {
		return nil
	}
	if !ok {
		// The index names no writer now. The snapshot may still be this
		// server's own, signed under an identity its index no longer gives
		// (a second source registered beside it, #1762's serverid rules):
		// said, because the fix is then on the index, not the folder.
		return fmt.Errorf("%w: the snapshot to build on (%s) was signed by %s, and this server's index names no writer to compare with; it may be this server's own under an identity the index no longer gives, or another writer's. Nothing was built. Check the index's bintrail_id, or give this server its own folder or prefix%s",
			errForeignSource, snap, strings.Join(foreign, ", "), onSnapshotsPage)
	}
	return fmt.Errorf("%w: the snapshot to build on (%s) was written by another writer (%s), not by this server (%s); building on it would publish a snapshot that belongs to neither. Give this server its own folder or prefix%s, or stop the other writer",
		errForeignSource, snap, strings.Join(foreign, ", "), ownID, onSnapshotsPage)
}
