package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// foldBaseAcross picks where a fold toward at starts (#1684): the newest
// snapshot at or before at in the standing source (where the fold reads
// today) or in one of the server's previous locations. The fold still writes
// only where the server writes now; only its READ source moves.
//
// A previous location is taken under the guards every read of one applies:
//   - its newest snapshot at or before at must be from before the server
//     left it. The fold reads each table's newest snapshot at or before at
//     in that one source, so a newer snapshot there, written by whoever uses
//     the place now, would be folded in; such a location is not used at all;
//   - that snapshot must not be another writer's, nor one whose signature
//     cannot be told here (console.PreviousSnapshotRefusal).
//
// A previous location that cannot be read, or that is not used, is logged
// when the fold can start elsewhere, and named in the refusal when it
// cannot: never a quiet "no snapshot". The standing source's own refusal
// (an unreadable folder there, #1639) stands as before.
func foldBaseAcross(ctx context.Context, indexDSN, standing string, previous []console.PreviousLocation, at time.Time) (source string, tables []string, anchor time.Time, err error) {
	tables, anchor, err = snapshotAt(ctx, standing, at)
	if len(previous) == 0 {
		return standing, tables, anchor, err
	}
	if err != nil {
		if !console.CurrentLocationEmpty(err) {
			return standing, nil, time.Time{}, console.NotConsulted(err, len(previous))
		}
		tables, anchor = nil, time.Time{} // a new place nothing was written to yet
	}
	source = standing
	own := ownIdentityOnce(indexDSN)
	var notes []string
	for _, p := range previous {
		until, ok := p.Until()
		if !ok {
			notes = append(notes, fmt.Sprintf("the previous snapshot location %s was not read: the time this server left it (%q) cannot be read", p.Location, p.LeftAt))
			continue
		}
		ptables, panchor, perr := snapshotAt(ctx, p.Location, at)
		if perr != nil {
			notes = append(notes, fmt.Sprintf("the previous snapshot location %s could not be read: %v", p.Location, perr))
			continue
		}
		if len(ptables) == 0 {
			continue
		}
		if panchor.After(until) {
			notes = append(notes, fmt.Sprintf("the previous snapshot location %s was not used: its newest snapshot at or before that moment (%s) was written after this server left it (%s)",
				p.Location, panchor.UTC().Format(time.RFC3339), until.Format(time.RFC3339)))
			continue
		}
		if len(tables) > 0 && !panchor.After(anchor) {
			continue // the same time or older: the location asked first wins
		}
		if refusal := console.PreviousSnapshotRefusal(p.Location, panchor, own); refusal != nil {
			notes = append(notes, refusal.Error())
			continue
		}
		source, tables, anchor = p.Location, ptables, panchor
	}
	if len(notes) > 0 {
		if len(tables) == 0 {
			return source, nil, time.Time{}, fmt.Errorf("no snapshot of this server at or before %s could be used: %s",
				at.UTC().Format("2006-01-02 15:04:05"), strings.Join(notes, "; "))
		}
		slog.Warn("snapshot base: previous snapshot locations were left out; the fold starts from what could be used",
			"source", source, "anchor", anchor.UTC().Format(time.RFC3339), "why", strings.Join(notes, "; "))
	}
	if source != standing {
		slog.Info("snapshot base: starting from a previous snapshot location; the result is written to the current one",
			"source", source, "anchor", anchor.UTC().Format(time.RFC3339))
	}
	return source, tables, anchor, nil
}

// listBaselinesUnreadable is reconstruct.ListBaselinesUnreadable behind a seam.
var listBaselinesUnreadable = reconstruct.ListBaselinesUnreadable

// listForVerify is the listing the baseline-anchored check pairs from: the
// current source, then the server's previous locations (#1684), merged into
// one listing newest snapshot first, as verify.FindBaselinePairIn wants it.
// A previous location contributes only snapshots from before the server left
// it and not refused by console.PreviousSnapshotRefusal; on a time two
// locations share, the current copy is kept.
//
// A previous location that cannot be read is an unreadable folder
// (reconstruct.ErrUnreadableSnapshot): the check then marks every table
// inconclusive and names it. It may hold the snapshot a table's pair needs,
// and "only one snapshot, nothing to compare" would be a false all-clear.
func listForVerify(ctx context.Context, indexDSN, src string, previous []console.PreviousLocation) ([]reconstruct.BaselineFile, []reconstruct.UnreadableSnapshot, error) {
	files, unreadable, err := listBaselinesUnreadable(ctx, src)
	if len(previous) == 0 {
		return files, unreadable, err
	}
	if err != nil {
		if !console.CurrentLocationEmpty(err) {
			return nil, nil, console.NotConsulted(err, len(previous))
		}
		files, unreadable = nil, nil // a new place nothing was written to yet
	}
	own := ownIdentityOnce(indexDSN)
	type key struct {
		at            int64
		schema, table string
	}
	seen := map[key]bool{}
	for _, f := range files {
		seen[key{f.SnapshotTime.UnixNano(), f.Schema, f.Table}] = true
	}
	for _, p := range previous {
		until, ok := p.Until()
		if !ok {
			return nil, nil, fmt.Errorf("%w: the previous snapshot location %s was not read, because the time this server left it (%q) cannot be read; forget it%s, or fix the saved settings",
				reconstruct.ErrUnreadableSnapshot, p.Location, p.LeftAt, onSnapshotsPage)
		}
		pf, pu, err := listBaselinesUnreadable(ctx, p.Location)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: the previous snapshot location %s could not be read (%v); make it readable, or forget it%s",
				reconstruct.ErrUnreadableSnapshot, p.Location, err, onSnapshotsPage)
		}
		verdict := map[int64]error{}
		for _, f := range pf {
			if f.SnapshotTime.After(until) {
				continue
			}
			ts := f.SnapshotTime.UnixNano()
			v, done := verdict[ts]
			if !done {
				v = console.PreviousSnapshotRefusal(p.Location, f.SnapshotTime, own)
				verdict[ts] = v
				if v != nil && !console.ForeignPreviousSnapshot(v) {
					// Who wrote it could not be told: it may be this
					// server's own, and leaving it out could turn into
					// "nothing to compare". Every table is inconclusive.
					return nil, nil, fmt.Errorf("%w: %v", reconstruct.ErrUnreadableSnapshot, v)
				}
				if v != nil {
					slog.Warn("verify: a snapshot of a previous location is another writer's and is not compared", "why", v)
				}
			}
			k := key{ts, f.Schema, f.Table}
			if v != nil || seen[k] {
				continue
			}
			seen[k] = true
			files = append(files, f)
		}
		for _, u := range pu {
			if !u.SnapshotTime.After(until) {
				unreadable = append(unreadable, u)
			}
		}
	}
	sort.SliceStable(files, func(i, j int) bool {
		a, b := files[i], files[j]
		if !a.SnapshotTime.Equal(b.SnapshotTime) {
			return a.SnapshotTime.After(b.SnapshotTime)
		}
		if a.Schema != b.Schema {
			return a.Schema < b.Schema
		}
		return a.Table < b.Table
	})
	return files, unreadable, nil
}

// ownIdentityOnce reads the identity of the server whose index is indexDSN
// at most once, normalized, and logs a failure: a signed snapshot in a
// previous location is then not used, and the refusal says why.
func ownIdentityOnce(indexDSN string) func() (string, error) {
	var id string
	var err error
	asked := false
	return func() (string, error) {
		if !asked {
			asked = true
			var raw string
			raw, err = snapshotWriterIDFunc(indexDSN)
			if err != nil {
				slog.Warn("snapshot base: this server's own identity could not be read; a signed snapshot in a previous location is not used",
					"error", err)
				return "", err
			}
			id = reconstruct.NormalizeSnapshotWriter(raw)
		}
		return id, err
	}
}
