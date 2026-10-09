package reconstruct

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
)

// resolveMerge and resolveValidate are the merge ResolveTableDelta runs and
// the check it runs on each input first, variables so a test can count the
// times they are reached and fail one.
var (
	resolveMerge    = CompactTableDeltaMinor
	resolveValidate = baselineintegrity.ValidateLocalFile
)

// ResolveTableDelta writes the resolved pair of the chain beside basePath
// (#2231, baseline.ResolvedTableDeltaPaths): the WHOLE chain merged into one
// range pair, the last pair included, under the directory beside the
// snapshots. The merge is CompactTableDeltaMinor's, so the pair holds each
// key once and the state read through it is the state read through the
// chain; unlike a compaction's, the result is never linked into a snapshot.
//
// prevBase is the same table's file in the previous snapshot, or "". A
// refresh with no change to a table carries its chain forward, and then the
// pair that resolved the previous snapshot's chain resolves this one: it is
// linked instead of merged again. The test is that pair's footers naming
// THIS chain (resolvedIsFor: its start, its base, its range and its last
// pair's position), which only the pair of the same chain does; not that
// the chain's files are the same inodes, which a carry-forward that had to
// copy would fail at every refresh.
//
// Before a merge reads a file of the chain, the file is checked against the
// snapshot's manifest when the manifest lists it
// (baselineintegrity.ValidateLocalFile; a snapshot with no manifest is merged
// unchecked, as every other reader reads it): the pair is read in place of
// those files, so it must not be the way damaged bytes reach a statement. A
// linked pair was checked when it was merged.
//
// A pair already there is kept only if its footers name this chain and this
// base (resolvedIsFor); one that does not is replaced. A reader trusts the
// pair by its name, the snapshot's and the range, so this is where a pair
// left under a snapshot directory someone replaced by hand is caught.
//
// The two files are made in a directory of this call's own and renamed into
// place, the upserts last: a reader finds both or takes neither, two calls
// for one chain cannot remove each other's files, and a failure leaves
// nothing a reader would take for a pair.
//
// done is false, with no error, where there is nothing to resolve: a chain of
// one pair, a legacy pair, a path with no place for one.
func ResolveTableDelta(ctx context.Context, basePath string, chain *baseline.TableDeltaChain, prevBase string) (done, linked bool, err error) {
	if chain == nil || chain.Legacy {
		return false, false, nil
	}
	want, ok := baseline.ResolvedTableDeltaPaths(basePath, chain.Files)
	if !ok {
		return false, false, nil
	}
	if ResolvedTableDeltaCurrent(basePath, chain) {
		return true, false, nil
	}
	dir := filepath.Dir(want.Upserts)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, false, fmt.Errorf("resolve table delta: %w", err)
	}
	removeOldWorkDirs(dir)
	work, err := os.MkdirTemp(dir, resolveWorkPrefix)
	if err != nil {
		return false, false, fmt.Errorf("resolve table delta: %w", err)
	}
	defer os.RemoveAll(work)
	made := baseline.TableDeltaFile{Posdel: filepath.Join(work, filepath.Base(want.Posdel)), Upserts: filepath.Join(work, filepath.Base(want.Upserts))}
	if prev, ok := carriedResolved(ctx, basePath, chain, prevBase); ok {
		// A link that fails (another filesystem, a pair removed meanwhile)
		// is not a failure of the table: merge it.
		linked = os.Link(prev.Posdel, made.Posdel) == nil && os.Link(prev.Upserts, made.Upserts) == nil
	}
	if !linked {
		for _, p := range []string{made.Posdel, made.Upserts} {
			os.Remove(p) // the half a failed link left
		}
		for _, p := range chain.Paths() {
			if err := resolveValidate(p); err != nil {
				return false, false, fmt.Errorf("resolve table delta: %w", err)
			}
		}
		if _, err := resolveMerge(ctx, basePath, chain, want.SeqLo, want.Seq, work); err != nil {
			return false, false, fmt.Errorf("resolve table delta: %w", err)
		}
	}
	// A pair that is there and is not this chain's is being replaced: its
	// upserts go first, so that between the two renames below a reader finds
	// no pair and reads the chain, never this chain's dead rows beside the
	// other's upserts.
	if _, found := baseline.FindResolvedTableDelta(basePath, chain.Files); found {
		if err := os.Remove(want.Upserts); err != nil && !os.IsNotExist(err) {
			return false, false, fmt.Errorf("resolve table delta: %w", err)
		}
	}
	// The upserts last: both files present is the reader's test for a
	// finished pair. A posdel a killed run left alone is replaced here.
	for _, mv := range [][2]string{{made.Posdel, want.Posdel}, {made.Upserts, want.Upserts}} {
		if err := os.Rename(mv[0], mv[1]); err != nil {
			return false, false, fmt.Errorf("resolve table delta: %w", err)
		}
	}
	return true, linked, nil
}

// ResolvedTableDeltaCurrent says whether the chain beside basePath has its
// resolved pair, finished and merged from this chain (resolvedIsFor).
func ResolvedTableDeltaCurrent(basePath string, chain *baseline.TableDeltaChain) bool {
	if chain == nil || chain.Legacy {
		return false
	}
	have, found := baseline.FindResolvedTableDelta(basePath, chain.Files)
	return found && resolvedIsFor(have, chain)
}

// resolveWorkPrefix starts the name of the directory one ResolveTableDelta
// call works in, beside the pairs. A dot name no pair has.
const resolveWorkPrefix = ".work-"

// resolveWorkMaxAge is past any merge still running: a work directory older
// than this was left by a run that was killed.
const resolveWorkMaxAge = time.Hour

// removeOldWorkDirs removes the work directories killed runs left in dir. A
// daemon killed in the middle of a merge (out of memory, say) leaves one at
// each try, with the files it had written, and the snapshot they sit under
// may stay one of the newest for as long as nothing refreshes.
func removeOldWorkDirs(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), resolveWorkPrefix) {
			continue
		}
		if fi, err := e.Info(); err == nil && time.Since(fi.ModTime()) > resolveWorkMaxAge {
			os.RemoveAll(filepath.Join(dir, e.Name()))
		}
	}
}

// resolvedIsFor says whether the pair's footers name chain: its start, its
// base and its range, on both files. The merge stamps them from the chain's
// last pair, so a pair merged from another chain of the same range under the
// same snapshot name does not pass.
func resolvedIsFor(pair baseline.TableDeltaFile, chain *baseline.TableDeltaChain) bool {
	last, err := baseline.ReadParquetMetadata(chain.Last().Upserts)
	if err != nil {
		return false
	}
	for _, p := range []string{pair.Upserts, pair.Posdel} {
		m, err := baseline.ReadParquetMetadata(p)
		if err != nil || !m.DeltaChainStart.Equal(last.DeltaChainStart) || m.DeltaBaseAnchor != last.DeltaBaseAnchor ||
			m.DeltaBaseSize != last.DeltaBaseSize || m.DeltaSeqLo != pair.SeqLo || m.DeltaSeq != pair.Seq ||
			m.BinlogFile != last.BinlogFile || m.BinlogPos != last.BinlogPos {
			return false
		}
	}
	return true
}

// carriedResolved finds the previous snapshot's resolved pair when it is the
// pair of this snapshot's chain too: the same range, and footers that name
// this chain.
func carriedResolved(ctx context.Context, basePath string, chain *baseline.TableDeltaChain, prevBase string) (baseline.TableDeltaFile, bool) {
	var none baseline.TableDeltaFile
	if prevBase == "" || prevBase == basePath {
		return none, false
	}
	prevChain, err := baseline.ListTableDelta(ctx, prevBase)
	if err != nil || prevChain == nil || prevChain.Legacy || len(prevChain.Files) == 0 {
		return none, false
	}
	if prevChain.Files[0].SeqLo != chain.Files[0].SeqLo || prevChain.Last().Seq != chain.Last().Seq {
		return none, false
	}
	pair, ok := baseline.FindResolvedTableDelta(prevBase, prevChain.Files)
	if !ok || !resolvedIsFor(pair, chain) {
		return none, false
	}
	return pair, true
}
