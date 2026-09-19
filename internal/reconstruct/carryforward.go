package reconstruct

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
)

// carryForward publishes a table into a new Parquet snapshot by taking the
// previous snapshot's file as-is, for the case where the delta window held no
// events for that table at all.
//
// # Why this is correct rather than a shortcut
//
// The fold that would otherwise run has an empty change map, so its output is
// the baseline's rows in the baseline's own encoding: the same bytes, minus
// whatever a re-encode would perturb. What makes carrying the file forward
// safe, though, is not that the rows match but that the ANCHOR travels with
// them. Each table's Parquet footer holds its own
// bintrail.baseline_binlog_file / _position, so a table with no deltas has an
// anchor that still points exactly at where its deltas resume. The next fold
// picks up from there and finds the same nothing.
//
// Snapshot DISCOVERY does not read that footer: findBaselineLocal derives a
// snapshot's time from its directory name. So a file carried into a newer
// directory is discovered at the newer time, which is what makes this work
// without rewriting the footer — and rewriting it is not cheap, since a
// Parquet footer is not practically editable in place and re-emitting the file
// costs about what the fold costs.
//
// # What must stay ahead of this
//
// "No row events" is NOT the same as "nothing happened": a TRUNCATE, DROP or
// RENAME emits no row events at all. CheckDestructiveDDL covers the window and
// REFUSES, and it runs at step 3b, before the change map exists. This must
// stay behind it. The same ordering gives the stale-schema and capture-gap
// refusals for free.
//
// The chain stays continuous across cycles: this run checked from the source
// snapshot's time and publishes at the new one, so the next run checking from
// the new time skips no window.
//
// The stale-schema check (3a-bis) is likewise ahead of this and likewise
// refuses. The capture-gap check is NOT, in the sense that matters: under
// --allow-gaps it reports and proceeds, which is why carryForwardEligible
// takes the finding rather than relying on the ordering.
//
// # Integrity
//
// The source is validated against its own snapshot's _MANIFEST before it is
// copied. Skipping that would let a corrupt file propagate into a fresh
// snapshot and be re-certified under the new manifest, which is worse than
// reading it: the merge path validates on read (materializeBaselineLocal), so
// carrying forward without validating would be the one route into a snapshot
// that bypasses the check.
//
// What that check does NOT do is worth stating: ValidateLocalFile passes when
// the snapshot has no manifest at all, or lists no entry for this file. It
// catches a CRC mismatch, not an absent guarantee. A legacy snapshot with no
// manifest is carried forward unverified, exactly as the merge path would read
// it unverified.
//
// # Hard link, then copy
//
// A link is tried first because the whole point is to stop paying for bytes
// that did not change. Under the daemon loop the old and new snapshot
// directories live under one baseline root and therefore one filesystem, so it
// normally succeeds. That is a property of the loop, not of the function: the
// CLI's `baseline refresh --baseline-dir /a --output /b` is a documented shape
// and puts the two on different devices, which is what the copy path is for.
// Snapshot files are written once and never modified in place, so sharing an
// inode between two snapshots is safe. The copy fallback covers a filesystem
// that has no links and the cross-device case.
//
// One consequence to know: a prune that removes the older snapshot will not
// reclaim a linked file's bytes while the newer one still references it. That
// is correct (the data is still in use) but it means the prune pass behind
// `bintrail baseline --baseline-retain` reports reclaimed bytes it did not
// reclaim: it sums file sizes, and a shared inode's blocks are counted whole.
//
// `du` is the narrower hazard it is tempting to state too broadly: one `du`
// over the baseline root tracks inodes within its own traversal and reports the
// truth. It is a `du` run per snapshot directory, as separate invocations, that
// counts the shared file twice.
// linkFile is os.Link, indirected so a test can drive the copy fallback.
//
// Every test machine has one filesystem, so os.Link always succeeds and the
// copy branch never ran anywhere: replacing copyFile's call with `return nil`
// published a snapshot MISSING the table's Parquet file and passed both tiers.
// A missing file is not caught until a later reconstruct, verify or drill trips
// over it, which is a long way from here.
var linkFile = os.Link

// carryForward reports HOW it published as well as whether it did: linked is
// true only when the destination shares the source's inode, which is the only
// arm that saves disk. The copy arm is still a reuse (no fold ran, no source
// was read), but every byte was written again — collapsing the two is how the
// console came to confirm a disk saving the daemon log denied (#1578).
func carryForward(ctx context.Context, srcPath, snapshotDir, schema, table string) (linked bool, err error) {
	return carryForwardFile(ctx, srcPath, filepath.Join(snapshotDir, schema, table+".parquet"), true)
}

// carryForwardFile is carryForward for one file at an explicit destination:
// the base, or one file of a table delta's chain (#1718), which travel the
// same way. validate runs the manifest check on the source first; a caller
// that just validated the file (readTableDelta, over every pair of a chain)
// passes false rather than hash the same bytes twice per refresh.
// A caller passing validate=false asserts that srcPath was already validated
// against its snapshot's manifest in this run (readTableDelta does, for every
// pair of a chain): the manifest writer reuses that manifest's digest for the
// linked file (#1717), which is only right for a file that was checked
// against it.
func carryForwardFile(ctx context.Context, srcPath, dst string, validate bool) (linked bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if validate {
		if err := baselineintegrity.ValidateLocalFile(srcPath); err != nil {
			return false, fmt.Errorf("validate the snapshot being carried forward: %w", err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return false, err
	}
	// Remove before writing, and the reason is the COPY path rather than the
	// link path. os.Create truncates, and after a carry-forward the destination
	// may share an inode with a file an OLDER snapshot still references, so
	// truncating in place would empty that one too. Unlinking first breaks the
	// share before anything is written. (os.Link would also fail with EEXIST,
	// though reconstruct's leftovers refusal already rules that out.)
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		return false, err
	}
	linkErr := linkFile(srcPath, dst)
	if linkErr == nil {
		return true, nil
	}
	// The copy is the designed fallback, not a failure, so the error is never
	// returned. The LEVEL splits by cause, because the two causes are worlds
	// apart. A cross-device destination is the documented shape (`--baseline-dir
	// /a --output /b`): it copies by design, every time, and saying so at Warn
	// would be a line per table per cycle forever. Anything else, a permission
	// or a filesystem with no links, silently costs the operator the exact
	// rewrite the opt-in was taken to avoid, and Debug is below the console
	// binary's default level, so it would never be said at all.
	if errors.Is(linkErr, syscall.EXDEV) {
		slog.Debug("carry forward: source and destination are on different filesystems, copying instead",
			"src", srcPath, "dst", dst)
	} else {
		slog.Warn("carry forward: could not hard link, so the file was COPIED and no disk space was saved. "+
			"Reusing an unchanged table is meant to avoid rewriting it; a copy still writes every byte.",
			"src", srcPath, "dst", dst, "error", linkErr)
	}
	return false, copyFile(srcPath, dst)
}

// carryForwardEligible reports whether a table can be published by carrying its
// previous file forward instead of folding.
//
// # Off unless asked for
//
// enabled is the operator's explicit opt-in, and the default is off. The output
// is the same rows either way, but the REPRESENTATION on disk is not: carrying
// a file forward can leave two snapshots sharing one inode (a hard link where
// the filesystem allows one, a copy otherwise), so a prune reports space it
// will not reclaim while the newer snapshot references it, and one snapshot
// ends up holding tables anchored at different binlog coordinates. Those are defensible trade-offs for a loop that would otherwise
// rewrite terabytes to apply a handful of rows, and they are not something to
// hand an operator without being asked.
//
// # A known capture gap disqualifies the table
//
// This is the same trap as TRUNCATE, one step further along, and it is worth
// stating separately because the refusal that closes it does not refuse.
// CheckDestructiveDDL returns an error and stops the run. CheckCaptureGapStatus
// under --allow-gaps returns the FINDING and lets the run continue, so a gap
// reaches this point as a value rather than as an abort.
//
// Two things go wrong if it is ignored. The events in the gap are permanently
// lost, so an empty change map no longer means the table was untouched: the
// missing events may be exactly this table's, and carrying the previous file
// forward would republish a state the source has moved on from while the
// summary calls it "unchanged".
//
// And the marker would vanish. A gap the run proceeded over is stamped into
// the new snapshot as bintrail.capture_gap by the merge path, which this
// branch skips, so a carried file would arrive in the new snapshot carrying no
// record that its window was knowingly incomplete. That inheritance exists so
// nobody has to reconstruct the provenance chain by hand (#1170).
//
// Falling through to the fold costs a rewrite and gets both right: the rows are
// re-emitted and the stamp is written.
//
// S3 sources are excluded deliberately rather than incidentally. A refresh
// writes Parquet to a filesystem, and the daemon-wide interval loop is always
// local-to-local; since #1539 a per-server scheduled fold on an S3-backed
// server reads its previous snapshot from the BUCKET, where carrying a file
// forward would mean downloading it — an S3 source would have to be downloaded, which buys the
// re-encode back and reintroduces the cost this avoids. Those runs take the
// ordinary merge path, which is correct, just not free.
func carryForwardEligible(enabled bool, format, srcPath string, changes int, capGap *CaptureGap) bool {
	// Mydumper output is a SQL dump for a human to load, not a snapshot to be
	// discovered, so there is no previous file to carry: the rows still have
	// to be emitted.
	return enabled && format == OutputFormatParquet && changes == 0 && capGap == nil &&
		!strings.HasPrefix(srcPath, "s3://")
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	// Close before returning success: a deferred close would drop a write
	// error on a file this snapshot is about to certify in its manifest.
	return out.Close()
}
