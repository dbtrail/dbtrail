package console

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

// negativeDecimalTTL bounds how long a FAILED footer resolution is remembered,
// for the same reason negativeRegionTTL exists: a failure is not a property of
// the snapshot, so a blip must not cost every file this daemon hands out its
// casts until someone restarts it.
const negativeDecimalTTL = 5 * time.Minute

type baselineDecimalEntry struct {
	decimals map[string]baseline.TableFooter
	at       time.Time
	failed   bool
}

// resolveBaselineDecimals fills in the state views' decimal casts, memoized per
// snapshot.
//
// The memoization is not an optimization, it is the rule this path is under.
// Server.bucketRegions carries it in so many words: buildViewsInput runs on
// EVERY SQL panel query, so a network round trip does not belong there in the
// steady state. Reading a footer per table would be N of them, unbounded in the
// table count, inside the panel's setup deadline and behind its single-flight
// latch, which turns one slow bucket into 429s for every other panel user.
//
// A snapshot directory is immutable once complete, so a SUCCESSFUL answer never
// expires: the cache key carries the snapshot timestamp, and taking a new
// baseline produces a new key rather than a stale hit. A resolution that FAILED
// expires after negativeDecimalTTL.
//
// A table that is merely ABSENT from a successful answer is cached with it, and
// deliberately so: that means the file carries no embedded schema, which for an
// old baseline or a PostgreSQL-source one is a permanent fact about an
// immutable file, not a fault to retry on every query.
func (s *Server) resolveBaselineDecimals(ctx context.Context, in *views.Input) {
	if len(in.Baselines) == 0 {
		return
	}
	key := fmt.Sprintf("%s@%s", in.BaselineSource, in.BaselineSnapshot.UTC().Format(time.RFC3339))

	s.baselineDecimalMu.Lock()
	e, ok := s.baselineDecimals[key]
	s.baselineDecimalMu.Unlock()
	if ok && (!e.failed || time.Since(e.at) < negativeDecimalTTL) {
		in.ApplyFooters(e.decimals)
		return
	}

	// The lock is held only around the map, never across the read: sync.Mutex
	// is not context-aware, so a goroutine blocked on it could not be released
	// by the panel's setup deadline. Two callers racing the same snapshot just
	// do the same read twice, which is harmless.
	decimals, err := baseline.TableFootersFor(ctx, in.BaselinePaths())
	if err == nil {
		in.ApplyFooters(decimals)
	}
	// A canceled or expired context is the CALLER's state, never a fact about
	// the snapshot, and this cache never forgets a "successful" answer. One
	// browser hitting Stop mid-download (#1583 put a cancelable caller in
	// front of this) would otherwise memoize either a negative entry or a
	// PARTIAL positive one — five minutes, or forever, of silently uncast
	// decimals for every later file of this snapshot — under a warning that
	// sends the operator chasing a storage fault that never happened.
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		slog.Warn("console: could not read baseline column types from the Parquet footers; "+
			"the state views will not cast decimal columns", "error", err)
	}
	s.rememberBaselineDecimals(key, decimals, err != nil)
}

func (s *Server) rememberBaselineDecimals(key string, decimals map[string]baseline.TableFooter, failed bool) {
	s.baselineDecimalMu.Lock()
	defer s.baselineDecimalMu.Unlock()
	if s.baselineDecimals == nil {
		s.baselineDecimals = map[string]baselineDecimalEntry{}
	}
	s.baselineDecimals[key] = baselineDecimalEntry{decimals: decimals, at: time.Now(), failed: failed}
}
