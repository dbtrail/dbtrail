package consoleapp

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// pgSavedRow is stream_state as a PostgreSQL capture that has saved a
// checkpoint leaves it: saved savedAgo seconds ago by the index's clock,
// with a trusted sent-through position sent.
func pgSavedRow(savedAgo, sent int64) pgPositionRow {
	return pgPositionRow{
		flavor:   sql.NullString{String: console.FlavorPostgres, Valid: true},
		position: sql.NullInt64{Int64: sent - 16, Valid: true},
		savedAgo: sql.NullInt64{Int64: savedAgo, Valid: true},
		sent:     sql.NullInt64{Int64: sent, Valid: true},
	}
}

// The rule (#3202), case by case, written before the code. The source is
// asked only once the index says capture's last save is older than the
// bound; the verdict reads the save's age from the index's own clock and
// compares the source by WAL position only, so no clock of the source or of
// this machine enters it.
func TestPGCaptureState_3202_theRule(t *testing.T) {
	const bound = time.Minute
	const sent = 1 << 30
	type answer struct {
		lsn uint64
		err error
	}
	for _, tc := range []struct {
		name   string
		row    pgPositionRow
		source *answer // nil: the source must not be asked
		want   string  // CaptureState.Kind
		why    []string
	}{
		// #3191's path, unchanged: no row is that rule's (the port's list of
		// tables not captured), not this one.
		{name: "capture never wrote its row", row: pgPositionRow{none: true}},
		{name: "an index from before the columns", row: pgPositionRow{oldIndex: true}},
		{name: "not a PostgreSQL capture", row: func() pgPositionRow {
			r := pgSavedRow(600, sent)
			r.flavor.String = "mysql"
			return r
		}()},
		// A first run on a quiet database saves no position until a
		// transaction ends, while other databases of the cluster move its
		// WAL: nothing here can tell that from a stall, so the rule does
		// not apply.
		{name: "a row with no position yet", row: func() pgPositionRow {
			r := pgSavedRow(600, sent)
			r.position.Int64 = 0
			return r
		}()},
		// A save by a writer that does not set the sent-through position
		// (an older capture, bintrail-pg reset): no position to compare.
		{name: "a sent-through position not trusted", row: func() pgPositionRow {
			r := pgSavedRow(600, sent)
			r.sent = sql.NullInt64{}
			return r
		}()},
		{name: "saved lately", row: pgSavedRow(10, sent)},
		{name: "saved exactly at the bound", row: pgSavedRow(60, sent)},
		// The lost-lock case of #3212: the run that lost the lock stops
		// saving by design, and the new owner saves, which moves the stamp.
		{name: "a new owner of the capture saves", row: pgSavedRow(2, sent)},
		// The index's clock stepped back past the save: a save just made.
		{name: "a save stamped after the index's now", row: pgSavedRow(-120, sent)},
		// Saves stop while the source is idle: nothing written, nothing to
		// drop. A capture the operator stopped, on a quiet source, is the
		// same row.
		{name: "saves stopped, the source idle", row: pgSavedRow(600, sent), source: &answer{lsn: sent}},
		{name: "saves stopped, the source before the capture", row: pgSavedRow(600, sent), source: &answer{lsn: sent - 100}},
		// The defect: saves stopped while the source writes. A capture the
		// operator stopped while the source writes, and a gap between two
		// owners of the capture longer than the bound, are the same row:
		// nobody records what is dropped.
		{name: "saves stopped, the source wrote", row: pgSavedRow(61, sent), source: &answer{lsn: sent + 1}, want: pgshim.CaptureStale,
			why: []string{"capture has not saved its state for 61 seconds", "your PostgreSQL wrote past the last position it saved"}},
		{name: "saves stopped long ago, the source wrote", row: pgSavedRow(3600, sent), source: &answer{lsn: sent + 1<<20}, want: pgshim.CaptureStale,
			why: []string{"3600 seconds"}},
		// The source cannot be asked: not known.
		{name: "saves stopped, the source cannot be asked", row: pgSavedRow(600, sent),
			source: &answer{err: &pgCaptureAskError{cause: "the port could not connect to your PostgreSQL", err: errors.New("dial tcp 10.0.3.7:5432: refused")}},
			want:   pgshim.CaptureUnchecked, why: []string{"the port could not connect to your PostgreSQL"}},
		{name: "saves stopped, the source failed otherwise", row: pgSavedRow(600, sent), source: &answer{err: errors.New("dial tcp 10.0.3.7:5432: refused")},
			want: pgshim.CaptureUnchecked, why: []string{"your PostgreSQL could not be asked"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			asked := 0
			st, seen := pgCaptureStateFrom(context.Background(), tc.row, bound, func(context.Context) (uint64, error) {
				asked++
				if tc.source == nil {
					return 0, errors.New("not to be asked")
				}
				return tc.source.lsn, tc.source.err
			})
			if (tc.source != nil) != (asked == 1) || asked > 1 {
				t.Errorf("the source was asked %d times, want it asked: %v", asked, tc.source != nil)
			}
			if st.Kind != tc.want {
				t.Errorf("%+v, want kind %q", st, tc.want)
			}
			for _, w := range tc.why {
				if !strings.Contains(st.Why, w) {
					t.Errorf("why %q does not say %q", st.Why, w)
				}
			}
			if strings.Contains(st.Why, "10.0.3.7") {
				t.Errorf("why %q names an address", st.Why)
			}
			if st.Kind != "" && seen.savedAgo == 0 {
				t.Errorf("what was seen is not kept for the log: %+v", seen)
			}
		})
	}
}

// pgCaptureFixture is a cache over a fake index and source, with a clock
// the test moves and the log it writes.
type pgCaptureFixture struct {
	c      *pgCaptureStateCache
	mu     sync.Mutex
	row    pgPositionRow
	rowErr error
	lsn    uint64
	srcErr error
	reads  atomic.Int32
	asks   atomic.Int32
	now    time.Time
	log    *bytes.Buffer
	block  chan struct{}
}

func newPGCaptureFixture() *pgCaptureFixture {
	f := &pgCaptureFixture{now: time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC), log: &bytes.Buffer{}}
	logger := slog.New(slog.NewTextHandler(f.log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	f.c = newPGCaptureStateCache("srv1", logger,
		func(ctx context.Context) (pgPositionRow, error) {
			f.reads.Add(1)
			f.mu.Lock()
			b := f.block
			f.mu.Unlock()
			if b != nil {
				select {
				case <-b:
				case <-ctx.Done():
					return pgPositionRow{}, ctx.Err()
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.row, f.rowErr
		},
		func(context.Context) (uint64, error) {
			f.asks.Add(1)
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.lsn, f.srcErr
		})
	f.c.now = func() time.Time {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.now
	}
	return f
}

func (f *pgCaptureFixture) set(row pgPositionRow, lsn uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.row, f.lsn = row, lsn
}

func (f *pgCaptureFixture) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// A burst of statements asks once; the answer is kept pgCaptureStateTTL,
// a failure pgCaptureStateFailTTL. It goes stale, warns once, says so for
// /status, and clears by itself when capture saves again.
func TestPGCaptureState_3202_cacheAndLog(t *testing.T) {
	ctx := context.Background()
	f := newPGCaptureFixture()
	f.set(pgSavedRow(5, 1000), 1000)
	for range 3 {
		if st, err := f.c.state(ctx); err != nil || st.Kind != "" {
			t.Fatalf("healthy: %+v %v", st, err)
		}
	}
	if f.reads.Load() != 1 || f.asks.Load() != 0 {
		t.Errorf("index read %d times, source asked %d times: want once and never", f.reads.Load(), f.asks.Load())
	}
	f.advance(pgCaptureStateTTL)
	f.set(pgSavedRow(90, 1000), 2000)
	st, err := f.c.state(ctx)
	if err != nil || st.Kind != pgshim.CaptureStale {
		t.Fatalf("stale: %+v %v", st, err)
	}
	if f.reads.Load() != 2 || f.asks.Load() != 1 {
		t.Errorf("index read %d times, source asked %d times after the answer expired", f.reads.Load(), f.asks.Load())
	}
	if doc := f.c.doc(); doc == nil || doc.State != pgshim.CaptureStale || !strings.Contains(doc.Why, "90 seconds") || doc.CheckedAt == "" {
		t.Errorf("status document: %+v", doc)
	}
	logged := f.log.String()
	if strings.Count(logged, "level=WARN") != 1 || !strings.Contains(logged, "capture_state_stale") || !strings.Contains(logged, "saved_ago=1m30s") ||
		!strings.Contains(logged, "saved_lsn=0/3E8") || !strings.Contains(logged, "source_lsn=0/7D0") {
		t.Errorf("the warning: %s", logged)
	}
	// Still stale at the next read: not said again.
	f.advance(pgCaptureStateTTL)
	if st, _ := f.c.state(ctx); st.Kind != pgshim.CaptureStale {
		t.Fatalf("still stale: %+v", st)
	}
	if n := strings.Count(f.log.String(), "level=WARN"); n != 1 {
		t.Errorf("warned %d times for one stall", n)
	}
	// Capture saves again: clears by itself, said at info.
	f.advance(pgCaptureStateTTL)
	f.set(pgSavedRow(1, 2000), 2000)
	if st, _ := f.c.state(ctx); st.Kind != "" {
		t.Fatalf("after capture saved again: %+v", st)
	}
	if !strings.Contains(f.log.String(), "level=INFO") || !strings.Contains(f.log.String(), "saves its state again") {
		t.Errorf("the clearing is not said: %s", f.log.String())
	}
	if doc := f.c.doc(); doc != nil {
		t.Errorf("status document after it cleared: %+v", doc)
	}
	// The index cannot be read: the error, kept pgCaptureStateFailTTL.
	f.advance(pgCaptureStateTTL)
	f.mu.Lock()
	f.rowErr = errors.New("dial tcp 10.0.3.9:3306: refused")
	f.mu.Unlock()
	if _, err := f.c.state(ctx); err == nil {
		t.Fatal("an index that cannot be read must be an error")
	}
	reads := f.reads.Load()
	f.advance(pgCaptureStateTTL)
	if _, err := f.c.state(ctx); err == nil || f.reads.Load() != reads {
		t.Errorf("a failure is the answer for %s: %v, read %d more times", pgCaptureStateFailTTL, err, f.reads.Load()-reads)
	}
	f.advance(pgCaptureStateFailTTL)
	f.mu.Lock()
	f.rowErr = nil
	f.mu.Unlock()
	if st, err := f.c.state(ctx); err != nil || st.Kind != "" {
		t.Errorf("after the failure's time: %+v %v", st, err)
	}
}

// The verdict does not depend on this machine's clock: a clock hours ahead
// or behind changes how long an answer is kept, never what it is.
func TestPGCaptureState_3202_thisMachinesClockDoesNotDecide(t *testing.T) {
	for _, skew := range []time.Duration{-10 * time.Hour, 10 * time.Hour} {
		f := newPGCaptureFixture()
		f.advance(skew)
		f.set(pgSavedRow(5, 1000), 5000)
		if st, _ := f.c.state(context.Background()); st.Kind != "" {
			t.Errorf("skew %s, saved lately: %+v", skew, st)
		}
		f.advance(pgCaptureStateTTL)
		f.set(pgSavedRow(300, 1000), 1000)
		if st, _ := f.c.state(context.Background()); st.Kind != "" {
			t.Errorf("skew %s, an idle source: %+v", skew, st)
		}
	}
}

// One question at a time: statements that arrive while it runs wait for
// it, each no longer than its own context, and take its answer.
func TestPGCaptureState_3202_oneQuestionAtATime(t *testing.T) {
	f := newPGCaptureFixture()
	f.set(pgSavedRow(5, 1000), 1000)
	f.block = make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if st, err := f.c.state(context.Background()); err != nil || st.Kind != "" {
				t.Errorf("%+v %v", st, err)
			}
		}()
	}
	// A statement whose time runs out while it waits leaves with its error.
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	if _, err := f.c.state(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a waiter whose time ran out: %v", err)
	}
	f.mu.Lock()
	close(f.block)
	f.block = nil
	f.mu.Unlock()
	wg.Wait()
	if n := f.reads.Load(); n != 1 {
		t.Errorf("the index was read %d times for statements that met, want once", n)
	}
}

// The bound is #3191's: one number for "capture is not saving".
func TestPGCaptureState_3202_bound(t *testing.T) {
	if pgCaptureStaleBound != console.PGNoCaptureStateBound {
		t.Errorf("bound %s, want #3191's %s", pgCaptureStaleBound, console.PGNoCaptureStateBound)
	}
}

// /status counts what the copy answered while capture's record was not
// current, by kind, apart from the reasons.
func TestPGCaptureState_3202_statusCounts(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	r.note("srv1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHiddenRules, CaptureState: pgshim.CaptureStale})
	r.note("srv1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, CaptureState: pgshim.CaptureUnchecked})
	r.note("srv1", pgshim.RouteNote{Side: pgshim.RouteSource, Reason: pgshim.ReasonCaptureStateStale})
	n := r.doc("").Servers["srv1"]
	if n.CaptureStateOnCopy[pgshim.CaptureStale] != 1 || n.CaptureStateOnCopy[pgshim.CaptureUnchecked] != 1 || n.Reasons[pgshim.ReasonCaptureStateStale] != 1 {
		t.Errorf("%+v", n)
	}
}
