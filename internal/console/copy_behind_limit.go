package console

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// The copy's lag limit per server (#2777). Off by default, the copy is an
// asynchronous replica whose lag is a number to watch (#2084). With a limit
// saved (ServerEntry.SQLMaxBehindSource), a statement on the SQL port that
// reads the copy's tables is answered there only if the data it would read
// is at most that far behind the source, measured like the number the router
// reports after an answer (behind_source_seconds, behindSourceOf) but BEFORE
// the statement runs: routed, it goes to MySQL past the limit; on a
// connection that does not route, it is refused with the lag in the error.
//
// What it is measured from, and the slack that leaves:
//   - the data's instant: the tails' when every table the statement reads
//     has one (fresh mode), else the snapshot's cut read from the index,
//     the older of the two when it reads both;
//   - capture's state, read from the index at most every captureReadEvery
//     (30 s): a quiet source whose capture is caught up and whose data holds
//     every indexed event is 0 behind. So a capture that stops can still
//     read as caught up for up to caughtUpWindow plus captureReadEvery
//     (about a minute and a half), the same window the reported number has;
//   - capture's sample itself can be up to captureReadEvery old with capture
//     healthy: events indexed after it was read are not in the copy's data
//     and the lag can still read 0. A limit under about a minute and a half
//     is not kept to the second, and a source clock ahead of the router's
//     reads as less lag (copyAgeOf clamps at zero), as the reported number
//     does;
//   - an instant that cannot be told (no index, a snapshot whose cut is not
//     recorded or cannot be read) is not within any limit: unknown, and the
//     statement is MySQL's. A copy read through the moving "current" link is
//     placed for each statement and never remembered.
//
// A limit that cannot be applied because the index cannot be read is warned
// about once in a while per server (copyFaultLog): otherwise the copy would
// stop answering with nothing said above debug.
//
// The change log (events) is history, not the tables' state: a statement
// that reads none of the copy's tables is not held to the limit.

// maxBehindSourceCap is the largest limit that can be saved: a year. Past it
// the number is a typo, and a Duration of it would overflow long before.
const maxBehindSourceCap = 365 * 24 * 60 * 60

// errMaxBehindSource is why a limit cannot be saved.
var errMaxBehindSource = errors.New("max_behind_source_seconds must be a whole number of seconds from 0 (no limit) to 31536000 (a year)")

// checkMaxBehindSource refuses a limit that cannot be saved.
func checkMaxBehindSource(v int64) error {
	if v < 0 || v > maxBehindSourceCap {
		return errMaxBehindSource
	}
	return nil
}

// behindLimitKey carries a statement's lag limit from the port's Run to the
// views callback, where the tables it reads are known.
type behindLimitKey struct{}

type behindLimit struct {
	id    string
	limit time.Duration
}

// withBehindLimit is ctx for a statement of server id held to limit; a zero
// limit is no limit and leaves ctx as it is.
func withBehindLimit(ctx context.Context, id string, limit time.Duration) context.Context {
	if limit <= 0 {
		return ctx
	}
	return context.WithValue(ctx, behindLimitKey{}, behindLimit{id: id, limit: limit})
}

// behindLimitFrom is the limit withBehindLimit set, ok false when none.
func behindLimitFrom(ctx context.Context) (behindLimit, bool) {
	l, ok := ctx.Value(behindLimitKey{}).(behindLimit)
	return l, ok && l.limit > 0
}

// maxBehindSourceOf is server id's saved limit, read now so a change saved
// while a connection is open governs its next statement; zero for an unknown
// id and for the boot entry, which has no choices of its own.
func (s *Server) maxBehindSourceOf(id string) time.Duration {
	if s == nil || s.cm == nil || s.cm.reg == nil || id == bootServerID {
		return 0
	}
	e, ok := s.cm.reg.Get(id)
	if !ok || e.SQLMaxBehindSource <= 0 {
		return 0
	}
	return time.Duration(min(e.SQLMaxBehindSource, maxBehindSourceCap)) * time.Second
}

// sqlBehindRefusal is the views callback's refusal of a statement held to a
// limit: behind is how far behind the source its data was, unknown says why
// that could not be told instead.
type sqlBehindRefusal struct {
	behind  time.Duration
	limit   time.Duration
	unknown string
}

func (e *sqlBehindRefusal) Error() string {
	limit := int64(e.limit.Seconds())
	if e.unknown != "" {
		return fmt.Sprintf("how far behind the source the copy is cannot be told (%s), and this server's limit is %d s "+
			"(max_behind_source_seconds), so the copy does not answer this statement", e.unknown, limit)
	}
	// Rounded up, like the reported number: a lag is never said smaller.
	behind := int64((e.behind + time.Second - 1) / time.Second)
	return fmt.Sprintf("the copy is %d s behind the source, more than this server's limit of %d s "+
		"(max_behind_source_seconds), so it does not answer this statement", behind, limit)
}

// behindBeforeRun is how far behind the source a statement about to run at
// now would be, its data holding every indexed event through through (a
// source event time) and through the event throughID (0 = unknown). It is
// behindSourceOf for an answer that has not run yet: the sample was read
// before now, so the branch that needs a sample read after the run (the
// answer held every event the index held) takes one read within
// captureReadEvery instead. haveSample false is no sample: the conservative
// now − through.
func behindBeforeRun(now, through time.Time, throughID uint64, s captureSample, haveSample bool) time.Duration {
	if !haveSample {
		return copyAgeOf(now, through)
	}
	if !s.caughtUp.IsZero() && s.maxID != 0 && throughID >= s.maxID {
		d := now.Sub(s.caughtUp)
		if d <= caughtUpWindow && d >= -caughtUpWindow && now.Sub(s.readAt) <= captureReadEvery && !s.readAt.After(now) {
			return 0
		}
	}
	return behindSourceOf(now, through, throughID, s)
}

// statementBehind is how far behind the source the data a statement is about
// to read on server id's copy is, or why that cannot be told (one of the
// copyAgeWhy reasons, safe to show a client; the detail goes to the log).
// read are the copy's tables it reads; tailThrough, tailThroughID and
// allTailed what its tails hold (statementTailsMark), zero when it reads
// none.
func (s *Server) statementBehind(ctx context.Context, b *bundle, id string, in views.Input, read []views.BaselineTable,
	tailThrough time.Time, tailThroughID uint64, allTailed bool) (time.Duration, string) {
	if b == nil || b.db == nil {
		// Capture's state and the snapshot's cut are both read from it.
		return 0, copyAgeWhyNoIndex
	}
	through, throughID := tailThrough, tailThroughID
	if !allTailed {
		files, ok := copyReadFrom(in, read)
		if !ok || len(files) == 0 {
			return 0, copyAgeWhyNoFiles
		}
		cut, cutID, why := s.snapshotCutFor(ctx, b, id, files)
		if why != "" {
			return 0, why
		}
		through, throughID = olderOf(cut, tailThrough), leastKnownID(cutID, tailThrough, tailThroughID)
	}
	if through.IsZero() {
		return 0, copyAgeWhyNoFiles
	}
	sample, have := s.captureSampleFor(ctx, b, id)
	return behindBeforeRun(time.Now(), through, throughID, sample, have), ""
}

// snapshotCutFor is the instant and least event id the snapshot directories
// files names hold every event through, from the server's remembered cuts
// (copyAges, one index read per set of snapshots) or the index. A set read
// through the moving "current" link is asked every time and never kept.
func (s *Server) snapshotCutFor(ctx context.Context, b *bundle, id string, files map[string][]string) (time.Time, uint64, string) {
	key := copyAgesKey(files)
	cacheable := copyAgesCacheable(key)
	if cacheable {
		if cut, cutID, ok := s.copyAges.knownCut(id, key); ok {
			return cut, cutID, ""
		}
	}
	rctx, cancel := context.WithTimeout(ctx, copyUnchangedTimeout)
	cut, cutID, why := s.copyIndexedThroughMark(rctx, b.db, files)
	cancel()
	if why != "" {
		if ctx.Err() == nil {
			s.copyAges.faults.warn(id, "Engine Routing: how far the copy's snapshot was indexed is not known, so statements held to the server's lag limit are answered by MySQL (or refused on a connection that does not route)",
				"server", id, "reason", why)
		}
		return time.Time{}, 0, copyAgeWhyNotPlaced
	}
	if cacheable {
		s.copyAges.keepCut(id, key, cut, cutID)
	}
	return cut, cutID, ""
}

// captureSampleFor is server id's capture state no older than
// captureReadEvery, read from the index when the one kept is older or there
// is none; have false when it cannot be read (then the lag is the
// conservative one, never a guess that capture is caught up).
func (s *Server) captureSampleFor(ctx context.Context, b *bundle, id string) (captureSample, bool) {
	now := time.Now()
	if sample, ok := s.copyAges.sampleSince(id, now.Add(-captureReadEvery)); ok {
		return sample, true
	}
	rctx, cancel := context.WithTimeout(ctx, copyUnchangedTimeout)
	sample, err := readCaptureSample(rctx, b.db, b.dbName, time.Now)
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			s.copyAges.faults.warn(id, "Engine Routing: capture's state could not be read for the server's lag limit; the copy's lag is taken as its data's age, never as caught up",
				"server", id, "detail", err.Error())
		}
		return captureSample{}, false
	}
	s.copyAges.keepSample(id, sample)
	return sample, true
}
