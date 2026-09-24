package consoleapp

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The table-delta compaction job (#1723). A refresh with table deltas on
// writes one small pair per window and links the earlier pairs forward, so
// a chain grows by one pair per refresh; every reader opens every pair, and
// every upload sends every pair again. This job merges the chain's first
// pairs into ONE range pair (reconstruct.CompactTableDeltaMinor, DuckDB
// only) and stages it under "<snapshot dir>/.compact/"; the NEXT refresh links
// the range forward in place of the pairs it merged. The base file is never
// touched here: folding the chain INTO the table (the major compaction) is
// the refresh's rewrite path still, and its own job is the second half of
// #1723.
//
// It shares the per-server single-flight with every other backup job, and
// runs right after a refresh has released its slot, never inside it.

// compactMinPairs is how many pairs a chain must have before its prefix is
// merged. A variable so a test can hit the rule with a short chain.
var compactMinPairs = 16

// compactDirFor is the staging root beside a server's snapshots: the SAME
// filesystem, so the refresh links the result forward instead of copying it,
// and a dot name that no listing reads as a snapshot.
func compactDirFor(baselineDir string) string {
	if baselineDir == "" {
		return ""
	}
	return filepath.Join(baselineDir, ".compact")
}

// listSnapshotChains and compactMinor are the two seams a test drives the job
// through without a real chain.
var (
	listSnapshotChains = baseline.SnapshotTableDeltaChains
	compactMinor       = reconstruct.CompactTableDeltaMinor
	readChainFooter    = baseline.ReadParquetMetadata
)

// compactCandidate is one chain the job will merge: the base path, its
// schema and table, and the chain as listed in the newest snapshot.
type compactCandidate struct {
	base          string
	schema, table string
	chain         *baseline.TableDeltaChain
}

// maybeCompact starts the job for req's server when the newest local
// snapshot holds a chain of compactMinPairs pairs or more with no result
// staged for it yet. Quiet otherwise: most refreshes find nothing to do.
func (s *baselineSupervisor) maybeCompact(req refreshRequest) {
	if !req.TableDeltas || req.BaselineDir == "" || s.ctx.Err() != nil {
		return
	}
	at, _, err := reconstruct.NewestSnapshot(s.ctx, req.BaselineDir)
	if err != nil {
		slog.Warn("baseline compact: could not read the snapshot directory, so no chain is merged", "server", req.ServerName, "dir", req.BaselineDir, "error", err)
		return
	}
	if at.IsZero() {
		return
	}
	snapDir := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(at))
	chains, err := listSnapshotChains(s.ctx, snapDir)
	if err != nil {
		slog.Warn("baseline compact: could not list the newest snapshot's chains", "server", req.ServerName, "snapshot", snapDir, "error", err)
		return
	}
	var due []compactCandidate
	for base, c := range chains {
		if c.Legacy || len(c.Files) < compactMinPairs {
			continue
		}
		schema, table := filepath.Base(filepath.Dir(base)), strings.TrimSuffix(filepath.Base(base), ".parquet")
		um, err := readChainFooter(c.Last().Upserts)
		if err != nil || um.DeltaChainStart.IsZero() {
			slog.Debug("baseline compact: chain skipped, its last pair carries no readable chain footer (the refresh sets such a chain aside)",
				"server", req.ServerName, "base", base, "error", err)
			continue
		}
		dir := reconstruct.CompactionDir(compactDirFor(req.BaselineDir), schema, table, um.DeltaChainStart)
		if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err == nil {
			continue // staged already; the next refresh adopts it
		}
		due = append(due, compactCandidate{base: base, schema: schema, table: table, chain: c})
	}
	if len(due) == 0 {
		return
	}
	sort.Slice(due, func(i, j int) bool { return due[i].base < due[j].base })
	if err := s.TriggerCompact(req, due); err != nil {
		slog.Info("baseline compact: not started; the server is busy, the next refresh tries again",
			"server", req.ServerName, "id", req.ServerID, "chains", len(due))
	}
}

// TriggerCompact claims the server's slot and runs the job for the chains.
func (s *baselineSupervisor) TriggerCompact(req refreshRequest, due []compactCandidate) error {
	s.mu.Lock()
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	s.compacts[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp()}
	s.mu.Unlock()
	slog.Info("baseline compact: starting", "server", req.ServerName, "id", req.ServerID, "chains", len(due))
	go s.runCompact(req, due)
	return nil
}

func (s *baselineSupervisor) runCompact(req refreshRequest, due []compactCandidate) {
	defer s.recoverBaselineJob(baselineJobCompact, req.ServerID, req.ServerName)
	started := time.Now().UTC()
	elapsed := time.Now()
	root := compactDirFor(req.BaselineDir)
	merged, failed, skipped := 0, 0, 0
	var firstErr error
	for i, c := range due {
		if s.ctx.Err() != nil {
			// The daemon is shutting down: the chains not yet looked at
			// are counted, not folded into "not merged" as if tried.
			skipped = len(due) - i
			break
		}
		if err := s.compactOne(req, root, c); err != nil {
			if s.ctx.Err() != nil {
				// Stopped inside the merge: this chain was not tried to
				// completion either, so it counts with the ones after it.
				skipped = len(due) - i
				break
			}
			failed++
			if firstErr == nil {
				firstErr = err
			}
			slog.Warn("baseline compact: chain not merged", "server", req.ServerName, "schema", c.schema, "table", c.table, "error", err)
			continue
		}
		merged++
	}
	// A chain that could not be merged is a failed run, whatever else was
	// merged: it is retried at every refresh, paying the manifest check
	// each time, and only the recorded error says why. A daemon shutdown
	// is not a failure: nothing is wrong with the chain, nothing is left
	// staged by the interrupted merge, and the next refresh looks again.
	// It is logged, and the status says so, but no run is recorded: a red
	// entry in the history for a clean restart would be a false alarm.
	var runErr error
	if failed > 0 {
		runErr = fmt.Errorf("%d of %d chain(s) not merged; first: %w", failed, len(due), firstErr)
	}
	if skipped > 0 {
		slog.Info("baseline compact: stopped by daemon shutdown; the next refresh looks at the chains again",
			"server", req.ServerName, "id", req.ServerID, "chains_merged", merged, "chains_left", skipped)
	} else {
		s.recordCompactRun(req, started, merged, failed, runErr)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.compacts[req.ServerID]
	if st == nil {
		st = &console.BaselineStatus{}
		s.compacts[req.ServerID] = st
	}
	st.FinishedAt = nowStamp()
	st.Tables, st.Refused = merged, failed+skipped
	if skipped > 0 {
		st.State, st.LastError = "failed", fmt.Sprintf("stopped by daemon shutdown after %d of %d chain(s); the next refresh looks again", merged, len(due))
		return
	}
	if runErr != nil {
		st.State, st.LastError = "failed", runErr.Error()
		return
	}
	st.State, st.LastError = "succeeded", ""
	slog.Info("baseline compact: done; the next refresh links each range in place of the pairs it merged",
		"server", req.ServerName, "id", req.ServerID, "chains_merged", merged, "chains_failed", failed,
		"took_ms", time.Since(elapsed).Milliseconds())
}

// recordCompactRun writes the job's run to the history. No trigger: this is
// the daemon's housekeeping, not the schedule's slot (with the refresh's
// trigger the Snapshots page would show it as the last scheduled run, a full
// backup that produced nothing). A failure that repeats is ONE record whose
// end moves (console.AppendCompact): the job is retried at every refresh,
// and at a 5-minute interval a persistent one would otherwise take the
// server's capped history over from the refreshes the page shows.
func (s *baselineSupervisor) recordCompactRun(req refreshRequest, started time.Time, merged, failed int, runErr error) {
	if s.history == nil {
		return
	}
	rec := console.BaselineRunRecord{
		Kind: console.BaselineRunCompact, ServerID: req.ServerID, ServerName: req.ServerName,
		StartedAt: started.Format(time.RFC3339), FinishedAt: nowStamp(), Tables: merged, Refused: failed,
	}
	if runErr != nil {
		rec.Error = runErr.Error()
	}
	if _, err := s.history.AppendCompact(rec); err != nil {
		slog.Warn("baseline history: could not record the compaction run", "server", req.ServerName, "error", err)
	}
}

// compactOne merges all but the last pair of one chain into a range pair
// under the staging directory, after checking every input pair against the
// snapshot's manifest: a compaction is the one path by which corrupt bytes
// could reach a file the next snapshot certifies as its own.
func (s *baselineSupervisor) compactOne(req refreshRequest, root string, c compactCandidate) error {
	files := c.chain.Files
	lo, hi := files[0].SeqLo, files[len(files)-2].Seq
	for _, f := range files[:len(files)-1] {
		for _, p := range []string{f.Posdel, f.Upserts} {
			if err := baselineintegrity.ValidateLocalFile(p); err != nil {
				return fmt.Errorf("pair %d: %w", f.Seq, err)
			}
		}
	}
	um, err := readChainFooter(c.chain.Last().Upserts)
	if err != nil {
		return err
	}
	dir := reconstruct.CompactionDir(root, c.schema, c.table, um.DeltaChainStart)
	// A previous attempt's leftovers (no _SUCCESS, or one the refresh did
	// not adopt) are replaced whole.
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	mc, err := compactMinor(s.ctx, c.base, c.chain, lo, hi, dir)
	if err != nil {
		os.RemoveAll(dir)
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, baseline.SuccessMarker), nil, 0o644); err != nil {
		os.RemoveAll(dir)
		return err
	}
	slog.Info("baseline compact: chain prefix merged into one range pair", "server", req.ServerName,
		"schema", c.schema, "table", c.table, "range", fmt.Sprintf("%d-%d", mc.Lo, mc.Hi), "pairs_merged", mc.Merged)
	return nil
}

// sweepCompactStaging removes every staged compaction without its _SUCCESS:
// a job that died mid-write. Runs at the top of every refresh cycle, like
// the discarded-snapshot sweep, because nothing else ever lists ".compact".
// Complete results are the refresh's to adopt or reject, except with table
// deltas OFF: then no refresh will ever look at them (adoption runs only on
// the delta path, and the next refresh rewrites every table, which ends
// every chain), so a complete result would sit there for good, a full copy
// of the pairs it merged. With deltas off everything under ".compact" goes.
func sweepCompactStaging(req refreshRequest) {
	root := compactDirFor(req.BaselineDir)
	if root == "" {
		return
	}
	if !req.TableDeltas {
		if err := os.RemoveAll(root); err != nil {
			slog.Warn("baseline compact: could not remove the staging directory with table deltas off", "dir", root, "error", err)
		}
		return
	}
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			slog.Warn("baseline compact: could not walk the staging directory; unfinished compactions there are not reclaimed", "dir", path, "error", err)
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if strings.Count(rel, string(filepath.Separator)) != 2 { // <schema>/<table>/<chain start>
			return nil
		}
		if _, err := os.Stat(filepath.Join(path, baseline.SuccessMarker)); err == nil {
			return filepath.SkipDir
		}
		if err := os.RemoveAll(path); err != nil {
			slog.Warn("baseline compact: could not remove an unfinished compaction", "dir", path, "error", err)
		} else {
			slog.Info("baseline compact: removed an unfinished compaction left by an earlier daemon", "dir", path)
		}
		return filepath.SkipDir
	})
}

// compactStatusFor is the job's last status for a server, idle if none.
func (s *baselineSupervisor) compactStatusFor(serverID string) console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.compacts[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}
