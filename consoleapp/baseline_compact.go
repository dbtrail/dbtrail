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
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The table-delta compaction job (#1723). A refresh with table deltas on
// writes one small pair per window and links the earlier pairs forward, so
// a chain grows by one pair per refresh; every reader opens every pair, and
// every upload sends every pair again. This job merges the chain's first
// pairs into ONE range pair (reconstruct.CompactTableDeltaMinor, DuckDB
// only) and stages it under "<snapshot dir>/.compact/"; the NEXT refresh links
// the range forward in place of the pairs it merged. The same job folds an
// old or large chain INTO its table (the major compaction, #1735), one table
// per run, and stages the new table file the same way; see compactMajorOne.
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

// listSnapshotChains, compactMinor and compactMajor are the seams a test
// drives the job through without a real chain.
var (
	listSnapshotChains = baseline.SnapshotTableDeltaChains
	compactMinor       = reconstruct.CompactTableDeltaMinor
	compactMajor       = reconstruct.CompactTableDeltaMajor
	readChainFooter    = baseline.ReadParquetMetadata
)

// compactCandidate is one chain the job will merge: the base path, its
// schema and table, and the chain as listed in the newest snapshot. major
// says it is folded into its table (#1735) rather than merged into a range,
// and why.
type compactCandidate struct {
	base          string
	schema, table string
	chain         *baseline.TableDeltaChain
	chainStart    time.Time
	major         string
	// skipErr, when set, is this chain's failure in the run without anything
	// being tried: its staging folder could not be looked at.
	skipErr error
}

// The major compaction (#1735) folds a chain INTO its table: a full pass over
// the table, run by this job so the refresh does not pay it in its own slot.
// ONE per run, the chain that started first: the job holds the refresh's
// slot, and folding every due table in one go would keep the refresh out for
// the sum of their passes, which is the missed-slot spiral the job exists to
// end. The others wait for the next run, one refresh later.
//
// majorLead is how long before the refresh's line (#1904) a chain is folded:
// the job has to run and its result be adopted by the next refresh before the
// refresh would end the chain itself by writing the table in full.
func majorLead(interval time.Duration) time.Duration { return time.Hour + 2*interval }

// majorRetryAfter is how long the job leaves a chain whose fold failed
// before folding it again. Without it the chain that started first, failing
// every time (a disk that cannot take the table), would win every run and no
// other table would ever be folded.
var majorRetryAfter = 6 * time.Hour

// compactBusyTries is how many looks in a row may find the server busy before
// the job counts as not running: the refresh then keeps its own rules.
const compactBusyTries = 3

// stagedResult says what waits in a chain's staging folder: "" (nothing
// complete), "minor" or "major", and whether a refresh refused a fold of this
// chain (reconstruct.CompactionRefusedMarker). err is set when the folder
// cannot be looked at: the job then leaves the chain alone, and says so as a
// failed run.
func stagedResult(dir, table string) (kind string, refused bool, err error) {
	exists := func(name string) (bool, error) {
		_, err := os.Stat(filepath.Join(dir, name))
		if err == nil {
			return true, nil
		}
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	fail := func(err error) (string, bool, error) {
		return "", false, fmt.Errorf("the staging folder %s cannot be looked at, so the chain beside it is not merged or folded: %w", dir, err)
	}
	if refused, err = exists(reconstruct.CompactionRefusedMarker); err != nil {
		return fail(err)
	}
	done, err := exists(baseline.SuccessMarker)
	if err != nil {
		return fail(err)
	}
	if !done {
		return "", refused, nil
	}
	major, err := exists(table + ".parquet")
	if err != nil {
		return fail(err)
	}
	if major {
		return "major", refused, nil
	}
	return "minor", refused, nil
}

// foldJob is what the job remembers per server between runs.
type foldJob struct {
	// blocked: the job's last look stopped before it could run (a snapshot
	// another writer signed, a shared location, a folder it could not
	// list). The refresh then keeps its own day and quarter rules, since no
	// job is folding for it.
	blocked bool
	// failed: when each chain's fold last failed, by staging folder.
	failed map[string]time.Time
	// refusedSaid: refused chains already said, by staging folder.
	refusedSaid map[string]bool
	// busy: looks in a row that found the server's slot taken.
	busy int
}

// foldJobLocked returns the server's state, created on first use. Callers hold s.mu.
func (s *baselineSupervisor) foldJobLocked(serverID string) *foldJob {
	if s.foldJobs == nil {
		s.foldJobs = map[string]*foldJob{}
	}
	j := s.foldJobs[serverID]
	if j == nil {
		j = &foldJob{failed: map[string]time.Time{}, refusedSaid: map[string]bool{}}
		s.foldJobs[serverID] = j
	}
	return j
}

func (s *baselineSupervisor) setFoldJobBlocked(serverID string, blocked bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.foldJobLocked(serverID).blocked = blocked
}

// foldJobBlocked says whether the job could not run at its last look.
func (s *baselineSupervisor) foldJobBlocked(serverID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.foldJobLocked(serverID).blocked
}

// chainSizes sizes a chain and its table for the major compaction's size
// rule. An error leaves the rule out for this chain (the age and the line
// still apply): a file listed a moment ago that cannot be sized now will be
// looked at again after the next refresh.
func chainSizes(base string, c *baseline.TableDeltaChain) (chain, table int64, err error) {
	fi, err := os.Stat(base)
	if err != nil {
		return 0, 0, err
	}
	for _, p := range c.Paths() {
		pi, err := os.Stat(p)
		if err != nil {
			return 0, 0, err
		}
		chain += pi.Size()
	}
	return chain, fi.Size(), nil
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
		s.setFoldJobBlocked(req.ServerID, true)
		return
	}
	if at.IsZero() {
		return
	}
	snapDir := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(at))
	chains, err := listSnapshotChains(s.ctx, snapDir)
	if err != nil {
		slog.Warn("baseline compact: could not list the newest snapshot's chains", "server", req.ServerName, "snapshot", snapDir, "error", err)
		s.setFoldJobBlocked(req.ServerID, true)
		return
	}
	now := time.Now().UTC()
	line, interval := s.chainFloorFor(req.ServerID)
	type looked struct {
		cand   compactCandidate
		staged string
	}
	var all []looked
	var unreadable []compactCandidate
	major := -1
	for base, c := range chains {
		if c.Legacy {
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
		staged, refused, serr := stagedResult(dir, table)
		cand := compactCandidate{base: base, schema: schema, table: table, chain: c, chainStart: um.DeltaChainStart}
		if serr != nil {
			// A failed chain in the run, so the page and the history say it.
			cand.skipErr = serr
			unreadable = append(unreadable, cand)
			continue
		}
		if staged == "major" {
			continue // the next refresh adopts it
		}
		// A staged minor does not stop a fold: the fold takes every pair the
		// minor merged, and replaces it.
		chainBytes, tableBytes, err := chainSizes(base, c)
		s.mu.Lock()
		failedAt, failed := s.foldJobLocked(req.ServerID).failed[dir]
		s.mu.Unlock()
		switch {
		case refused:
			// A refresh refused a fold of this chain and said why; folding it
			// again would be refused again. Its first pairs are still merged,
			// and the refresh's own rules end it.
			s.mu.Lock()
			j := s.foldJobLocked(req.ServerID)
			first := !j.refusedSaid[dir]
			j.refusedSaid[dir] = true
			s.mu.Unlock()
			if first {
				slog.Warn("snapshot compaction: a refresh refused this chain's fold; it is not folded again, the refresh ends the chain itself",
					"server", req.ServerName, "schema", schema, "table", table, "dir", dir)
			}
		case err != nil:
			// Sizes of zero would read as "not large": no fold this run.
			slog.Warn("snapshot compaction: could not size a chain; it is not folded at this run",
				"server", req.ServerName, "base", base, "error", err)
		case failed && now.Sub(failedAt) < majorRetryAfter:
			// Its fold failed recently: the others get their turn.
		default:
			cand.major = reconstruct.MajorCompactionReason(um, now, chainBytes, tableBytes, line, majorLead(interval))
		}
		all = append(all, looked{cand, staged})
		if cand.major == "" {
			continue
		}
		if m := all[max(major, 0)].cand; major < 0 || cand.chainStart.Before(m.chainStart) || (cand.chainStart.Equal(m.chainStart) && base < m.base) {
			major = len(all) - 1
		}
	}
	var due []compactCandidate
	for i, l := range all {
		if i == major {
			continue
		}
		// Not folded at this run: merged if long enough, folded at a later one.
		l.cand.major = ""
		if l.staged == "" && len(l.cand.chain.Files) >= compactMinPairs {
			due = append(due, l.cand)
		}
	}
	due = append(due, unreadable...)
	if major >= 0 {
		// Last: the minors are quick, and are staged even if the fold is
		// stopped by a shutdown.
		due = append(due, all[major].cand)
	}
	if len(due) == 0 {
		s.mu.Lock()
		j := s.foldJobLocked(req.ServerID)
		j.blocked, j.busy = false, 0
		s.mu.Unlock()
		return
	}
	// The merged chain is staged for the next refresh to adopt as this
	// server's: another writer's snapshot is left alone (#1684), and the
	// refusal is the job's result, where the page reads it. Checked only
	// when there is something to merge, so a folder with nothing due never
	// reports a failure, and before anything is written.
	if err := foldSourceRefusal(req.IndexDSN, req.BaselineDir, at); err != nil {
		slog.Warn("snapshot compaction: not started", "server", req.ServerName, "id", req.ServerID, "error", err)
		s.setFoldJobBlocked(req.ServerID, true)
		s.mu.Lock()
		if !s.busyLocked(req.ServerID) {
			s.compacts[req.ServerID] = &console.BaselineStatus{State: "failed", Since: nowStamp(), FinishedAt: nowStamp(), LastError: err.Error()}
		}
		s.mu.Unlock()
		return
	}
	// Minors by table, the fold (if any) after them.
	sort.SliceStable(due, func(i, j int) bool {
		if (due[i].major == "") != (due[j].major == "") {
			return due[i].major == ""
		}
		return due[i].base < due[j].base
	})
	err = s.TriggerCompact(req, due)
	busy := errors.Is(err, console.ErrBaselineRunning)
	s.mu.Lock()
	j := s.foldJobLocked(req.ServerID)
	if busy {
		j.busy++
	} else {
		j.busy = 0
	}
	busyLooks := j.busy
	j.blocked = (err != nil && !busy) || busyLooks >= compactBusyTries
	s.mu.Unlock()
	if err != nil {
		if busy {
			if busyLooks >= compactBusyTries {
				slog.Warn("snapshot compaction: the job has not had the slot for several looks in a row; until it runs, the refresh writes tables in full itself when their chains are a day old or past a quarter of the table",
					"server", req.ServerName, "id", req.ServerID, "looks", busyLooks)
				return
			}
			slog.Info("baseline compact: not started; the server is busy, the next refresh tries again",
				"server", req.ServerName, "id", req.ServerID, "chains", len(due))
			return
		}
		slog.Warn("snapshot compaction: not started", "server", req.ServerName, "id", req.ServerID, "chains", len(due), "error", err)
	}
}

// TriggerCompact claims the server's slot and runs the job for the chains.
func (s *baselineSupervisor) TriggerCompact(req refreshRequest, due []compactCandidate) error {
	// The merge writes into the folder, and a folder another server writes
	// too may hold its chains (#1684). The command-line server is not in the
	// registry and is never refused here.
	if s.reg != nil {
		if e, ok := s.reg.Get(req.ServerID); ok {
			if err := s.reg.WriteRefusal(e); err != nil {
				return err
			}
		}
	}
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
		one := s.compactOne
		switch {
		case c.skipErr != nil:
			one = func(refreshRequest, string, compactCandidate) error { return c.skipErr }
		case c.major != "":
			one = s.compactMajorOne
		}
		if err := one(req, root, c); err != nil {
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
		st.State, st.LastError = "failed", fmt.Sprintf("stopped because DBTrail shut down after %d of %d chain(s); the next refresh looks again", merged, len(due))
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
	// A refusal record (#1735) stays: it is what keeps the chain from being
	// folded again.
	if err := reconstruct.ClearCompactionDir(dir); err != nil {
		return err
	}
	mc, err := compactMinor(s.ctx, c.base, c.chain, lo, hi, dir)
	if err != nil {
		reconstruct.ClearCompactionDir(dir)
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, baseline.SuccessMarker), nil, 0o644); err != nil {
		reconstruct.ClearCompactionDir(dir)
		return err
	}
	slog.Info("baseline compact: chain prefix merged into one range pair", "server", req.ServerName,
		"schema", c.schema, "table", c.table, "range", fmt.Sprintf("%d-%d", mc.Lo, mc.Hi), "pairs_merged", mc.Merged)
	return nil
}

// compactMajorOne folds one chain into its table under the staging folder
// (#1735), replacing whatever waited there for the chain (a minor result, or
// an attempt that did not finish). The fold validates the table and every
// pair against the snapshot's manifest before reading them. The _SUCCESS
// marker is written last, so a fold stopped halfway is swept, never adopted.
func (s *baselineSupervisor) compactMajorOne(req refreshRequest, root string, c compactCandidate) error {
	dir := reconstruct.CompactionDir(root, c.schema, c.table, c.chainStart)
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	started := time.Now()
	mc, err := compactMajor(s.ctx, c.base, dir, newDiskSpaceCheck(), duckdbutil.Tuning{})
	if err != nil {
		os.RemoveAll(dir)
		if s.ctx.Err() == nil {
			s.mu.Lock()
			s.foldJobLocked(req.ServerID).failed[dir] = time.Now().UTC()
			s.mu.Unlock()
		}
		return fmt.Errorf("%s.%s: %w", c.schema, c.table, err)
	}
	if err := os.WriteFile(filepath.Join(dir, baseline.SuccessMarker), nil, 0o644); err != nil {
		os.RemoveAll(dir)
		return err
	}
	slog.Info("snapshot compaction: chain folded into its table; the next refresh puts the table in place and starts the chain again from it",
		"server", req.ServerName, "schema", c.schema, "table", c.table, "reason", c.major,
		"folded_through", mc.Seq, "rows", mc.Rows, "took_ms", time.Since(started).Milliseconds())
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
		if _, err := os.Stat(filepath.Join(path, reconstruct.CompactionRefusedMarker)); err == nil {
			return filepath.SkipDir // the record that stops the fold running again
		}
		if err := os.RemoveAll(path); err != nil {
			slog.Warn("baseline compact: could not remove an unfinished compaction", "dir", path, "error", err)
		} else {
			slog.Info("baseline compact: removed an unfinished compaction left by an earlier daemon", "dir", path)
		}
		return filepath.SkipDir
	})
}

// chainFloor is what the last refresh of a server told the job about its
// line: where it ends chains, and how often it runs.
type chainFloor struct {
	line     time.Time
	interval time.Duration
}

// recordChainFloor keeps the line a refresh drew for the job that runs after
// it. A zero line (the index could not be read) is kept as zero: an old line
// would be a guess.
func (s *baselineSupervisor) recordChainFloor(serverID string, line time.Time, interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chainFloors == nil {
		s.chainFloors = map[string]chainFloor{}
	}
	s.chainFloors[serverID] = chainFloor{line: line, interval: interval}
}

func (s *baselineSupervisor) chainFloorFor(serverID string) (time.Time, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.chainFloors[serverID]
	return f.line, f.interval
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
