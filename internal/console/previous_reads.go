package console

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Reads across a server's previous snapshot locations (#1684).
//
// Every read that answers "what did this server's data look like" consults
// the current locations first and then each previous one, and the newest
// snapshot wins. A previous location is read under two guards, because the
// place may since belong to someone else:
//
//  1. Only up to the moment this server left it. Everything newer was written
//     by whoever took the place over, and an index that names no writer
//     publishes unsigned, so the signature alone cannot tell it apart.
//  2. Not a snapshot another writer signed (#1762), nor, fail closed, one
//     whose signature cannot be told here. An unsigned snapshot keeps
//     today's behaviour: every snapshot written before signatures existed
//     is unsigned.
//
// A previous location that cannot be read never shortens the answer
// silently: the answer names it.

// findBaselineAt is reconstruct.FindBaseline behind a seam, so the lookup
// across locations can be driven without real snapshots in a bucket.
var findBaselineAt = reconstruct.FindBaseline

// previousSnapshotSigners is reconstruct.SnapshotSigners behind a seam.
var previousSnapshotSigners = reconstruct.SnapshotSigners

// bundleOwnWriter is the bundle's own writer identity, normalized; "" when
// its index names none or cannot be read. A seam for tests.
var bundleOwnWriter = func(ctx context.Context, b *bundle) string {
	return ownWriterID(ctx, b.db, "")
}

// PreviousSnapshotRefusal says why the snapshot of the previous location loc
// taken at snap must not be served as this server's, nil when it may. own is
// this server's writer identity, normalized, "" when unknown; asked only for
// a signed snapshot.
func PreviousSnapshotRefusal(loc string, snap time.Time, own func() string) error {
	name := strings.TrimRight(loc, "/") + "/" + reconstruct.SnapshotDirName(snap)
	signers, known, err := previousSnapshotSigners(loc, snap)
	if err != nil {
		return fmt.Errorf("the snapshot %s in the previous location %s was not used: who wrote it could not be read (%v)", name, loc, err)
	}
	if !known {
		return fmt.Errorf("the snapshot %s in the previous location %s was not used: who wrote it is not known here", name, loc)
	}
	if len(signers) == 0 {
		return nil
	}
	ownID := own()
	if ownID == "" {
		return fmt.Errorf("the snapshot %s in the previous location %s was not used: it was signed by %s, and this server's index names no writer to compare with",
			name, loc, strings.Join(signers, ", "))
	}
	foreign := slices.DeleteFunc(slices.Clone(signers), func(w string) bool { return w == ownID })
	if len(foreign) > 0 {
		return fmt.Errorf("the snapshot %s in the previous location %s was not used: it was written by another writer (%s), not by this server",
			name, loc, strings.Join(foreign, ", "))
	}
	return nil
}

// previousUntilNote is why a previous location whose move time does not
// parse is not read.
func previousUntilNote(p PreviousLocation) string {
	return fmt.Sprintf("the previous snapshot location %s was not read: the time this server left it (%q) cannot be read", p.Location, p.LeftAt)
}

// snapshotDirOf is the time of the snapshot folder a table path sits in,
// fallback when the path does not name one. The time FindBaseline returns is
// a read bound, which under a delta chain (#1638) is the chain's start, not
// the snapshot's folder.
func snapshotDirOf(path string, fallback time.Time) time.Time {
	if t, ok := reconstruct.SnapshotDirTime(path); ok {
		return t
	}
	return fallback
}

// withPreviousLocations completes the current locations' answer with the
// previous ones. A current answer that is a refusal other than "none there"
// stands: the current location may hold a newer snapshot it could not read.
func (b *bundle) withPreviousLocations(ctx context.Context, schema, table string, at time.Time, path string, snapshotTime time.Time, stale reconstruct.StaleWarning, err error) (string, time.Time, reconstruct.StaleWarning, error) {
	if err != nil && !errors.Is(err, reconstruct.ErrNoBaseline) {
		return path, snapshotTime, stale, err
	}
	found := err == nil
	var best time.Time
	if found {
		best = snapshotDirOf(path, snapshotTime)
	}
	ownID, asked := "", false
	own := func() string {
		if !asked {
			ownID, asked = bundleOwnWriter(ctx, b), true
		}
		return ownID
	}
	var notes []string
	for _, p := range b.previous {
		until, ok := p.Until()
		if !ok {
			notes = append(notes, previousUntilNote(p))
			continue
		}
		bound := at
		if until.Before(bound) {
			bound = until
		}
		pp, pt, ps, perr := findBaselineAt(ctx, p.Location, schema, table, bound)
		if errors.Is(perr, reconstruct.ErrNoBaseline) {
			continue
		}
		if perr != nil {
			slog.Warn("snapshot lookup: a previous snapshot location could not be read; the answer comes from the others",
				"table", schema+"."+table, "location", p.Location, "err", perr)
			notes = append(notes, fmt.Sprintf("the previous snapshot location %s could not be read: %v", p.Location, perr))
			continue
		}
		dir := snapshotDirOf(pp, pt)
		if found && !dir.After(best) {
			continue // the same time or older: the location asked first wins
		}
		if refusal := PreviousSnapshotRefusal(p.Location, dir, own); refusal != nil {
			notes = append(notes, refusal.Error())
			continue
		}
		path, snapshotTime, stale, found, best = pp, pt, ps, true, dir
	}
	if !found {
		if len(notes) == 0 {
			return "", time.Time{}, reconstruct.StaleWarning{}, err
		}
		// Not ErrNoBaseline: a plain "no snapshot" would read as a history
		// that is shorter than it is.
		return "", time.Time{}, reconstruct.StaleWarning{}, fmt.Errorf("no snapshot of %s.%s at or before %s could be used: %s",
			schema, table, at.UTC().Format(time.RFC3339), strings.Join(notes, "; "))
	}
	if len(notes) > 0 {
		said := strings.Join(notes, "; ")
		if stale.Stale() {
			stale.Message += "; " + said
		} else {
			stale = reconstruct.StaleWarning{Message: said, UsingSnapshot: snapshotTime, NewestSnapshot: snapshotTime}
		}
	}
	return path, snapshotTime, stale, nil
}

// listSource is one location a listing reads: current, or previous with the
// record of when the server left it.
type listSource struct {
	Source   string
	Previous *PreviousLocation
}

// previousFiles keeps the files of a previous location that are this
// server's to show: taken at or before until, in a snapshot refuse allows.
// hidden counts the snapshots left out, why says why in words.
func previousFiles(loc string, files []reconstruct.BaselineFile, until time.Time, refuse func(loc string, snap time.Time) error) (kept []reconstruct.BaselineFile, hidden int, why string) {
	after, refused := map[int64]bool{}, map[int64]bool{}
	verdict := map[int64]error{}
	var firstRefusal error
	for _, f := range files {
		ts := f.SnapshotTime.UnixNano()
		if f.SnapshotTime.After(until) {
			after[ts] = true
			continue
		}
		if refuse != nil {
			err, asked := verdict[ts]
			if !asked {
				err = refuse(loc, f.SnapshotTime)
				verdict[ts] = err
			}
			if err != nil {
				if firstRefusal == nil {
					firstRefusal = err
				}
				refused[ts] = true
				continue
			}
		}
		kept = append(kept, f)
	}
	var parts []string
	if n := len(after); n > 0 {
		parts = append(parts, fmt.Sprintf("%d written after this server left, by whoever uses the location now", n))
	}
	if n := len(refused); n > 0 {
		parts = append(parts, fmt.Sprintf("%d not this server's: %v", n, firstRefusal))
	}
	if len(parts) > 0 {
		slog.Info("console: snapshots of a previous location are not listed as this server's", "location", loc, "why", strings.Join(parts, "; "))
	}
	return kept, len(after) + len(refused), strings.Join(parts, "; ")
}

// readSourcesOf is every location a read of the bundle consults, in order:
// the current ones (baselineSourcesOf), then the previous ones, most recently
// left first. baselineSourcesOf stays current-only: the Restore card, the
// prune record and the shared-location warning are about where this server
// writes.
func readSourcesOf(b *bundle) []listSource {
	var out []listSource
	for _, src := range baselineSourcesOf(b) {
		out = append(out, listSource{Source: src})
	}
	if b == nil {
		return out
	}
	for i := range b.previous {
		p := b.previous[i]
		out = append(out, listSource{Source: p.Location, Previous: &p})
	}
	return out
}

// previousRefusal is PreviousSnapshotRefusal bound to the bundle's own
// identity, read at most once.
func previousRefusal(ctx context.Context, b *bundle) func(loc string, snap time.Time) error {
	ownID, asked := "", false
	own := func() string {
		if !asked {
			ownID, asked = bundleOwnWriter(ctx, b), true
		}
		return ownID
	}
	return func(loc string, snap time.Time) error {
		return PreviousSnapshotRefusal(loc, snap, own)
	}
}
