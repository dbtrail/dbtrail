package cascade_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The #2177 engine halves. The Phase-2 provider refuses a baseline whose
// changes cannot be read by binlog position (the source's numbering started
// again after the snapshot): the engine must say so under its own caveat,
// never as a transient lookup failure, and fall back to Phase-1, which reads
// by time and is not affected by the numbering. The REAL provider's
// classification is pinned in internal/cascadebaseline.

const renumberedReason = "the changes since the snapshot cannot be found by binlog position: " +
	"the source's binary log started again from another numbering. A new full snapshot is needed."

// renumberedProvider refuses every lookup with err.
type renumberedProvider struct {
	err   error
	calls int
}

func (p *renumberedProvider) BaselineChildren(context.Context, string, string, string, string, time.Time, int) (cascade.BaselineLookup, bool, error) {
	p.calls++
	return cascade.BaselineLookup{}, false, p.err
}

func renumberedErr() error {
	return fmt.Errorf("baseline of app.child: %w", fmt.Errorf("%w: the source's binary log started again from another numbering. A new full snapshot is needed.", reconstruct.ErrBinlogRenumbered))
}

func TestSynthesizeVictims_renumberedBaselineIsNamedAndPhase1Runs(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	fks, parents := twoParentDeletes()
	parents = parents[:1]
	prov := &renumberedProvider{err: renumberedErr()}
	res, serr := cascade.SynthesizeVictims(context.Background(), query.New(db), fks, parents, cascade.Options{Baseline: prov})
	// The sqlmock index has no expectations: the Phase-1 fetch fails, which
	// proves it was attempted on the same pass that refused Phase-2.
	if serr == nil {
		t.Fatal("expected the unexpected-query error from the Phase-1 fetch; the engine must have attempted it")
	}
	var named, phase1 bool
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "victim query for app.child failed") {
			phase1 = true
		}
		if strings.Contains(msg, "baseline lookup failed") {
			t.Errorf("a renumbering must not land in the transient baselinefail bucket: %q", msg)
		}
		if strings.Contains(msg, "no baseline covers app.child") {
			t.Errorf("a renumbering is not a missing baseline: %q", msg)
		}
		if !strings.HasPrefix(msg, "app.child's baseline snapshot is not used") {
			continue
		}
		named = true
		if !strings.Contains(msg, "started again from another numbering") || !strings.Contains(msg, "A new full snapshot is needed") {
			t.Errorf("the caveat must carry the renumbering reason verbatim: %q", msg)
		}
		if !strings.Contains(msg, "untouched within the lookback window") {
			t.Errorf("the caveat must name the lookback window as the boundary (same words as nobaseline:/pktype:): %q", msg)
		}
		// recover_cascade (MCP) returns this caveat to the client.
		if strings.Contains(msg, "--") {
			t.Errorf("the caveat must not name a CLI flag: %q", msg)
		}
	}
	if !named {
		t.Errorf("Incomplete must name the renumbering, got: %v", res.Incomplete)
	}
	if !phase1 {
		t.Errorf("Phase-1 must still run when the baseline is not used, got: %v", res.Incomplete)
	}
}

// Not memoized per table, unlike the PK-type refusal: a later parent can
// resolve a newer baseline taken after the restart, which is usable.
func TestSynthesizeVictims_renumberedBaselineAskedPerParent(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	fks, parents := twoParentDeletes()
	prov := &renumberedProvider{err: renumberedErr()}
	_, _ = cascade.SynthesizeVictims(context.Background(), query.New(db), fks, parents, cascade.Options{Baseline: prov})
	if prov.calls != len(parents) {
		t.Errorf("provider calls = %d, want %d (one per parent: the baseline found can differ per parent)", prov.calls, len(parents))
	}
}

// A failed check (the index could not be read) is an error, not a verdict:
// it stays in the baselinefail bucket and never reads as a renumbering.
func TestSynthesizeVictims_failedNumberingCheckIsBaselineFail(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	fks, parents := twoParentDeletes()
	parents = parents[:1]
	prov := &renumberedProvider{err: errors.New("check the binlog numbering since the snapshot of app.child: read whether `bintrail index` wrote into this index: Error 1146")}
	res, _ := cascade.SynthesizeVictims(context.Background(), query.New(db), fks, parents, cascade.Options{Baseline: prov})
	var fail bool
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "baseline lookup failed for app.child") && strings.Contains(msg, "Error 1146") {
			fail = true
		}
		if strings.HasPrefix(msg, "app.child's baseline snapshot is not used") {
			t.Errorf("a failed check must not be reported as a renumbering: %q", msg)
		}
	}
	if !fail {
		t.Errorf("a failed check must surface as a baseline lookup failure, got: %v", res.Incomplete)
	}
}

// The window stays the lookback one: `since` is widened to the snapshot only
// when the baseline is used, and this one is not.
func TestSynthesizeVictims_renumberedBaselineLeavesTheLookbackWindowNarrow(t *testing.T) {
	const lookback = 73 * time.Hour
	var sinceArgs []time.Time
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	for i := 0; i < 8; i++ {
		mock.ExpectQuery(".*").WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(),
			captureTimes{got: &sinceArgs}, captureTimes{got: &sinceArgs},
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
	}
	fks, parents := twoParentDeletes()
	parents = parents[:1]
	rootTS := parents[0].EventTimestamp
	if _, serr := cascade.SynthesizeVictims(context.Background(), query.New(db), fks, parents, cascade.Options{
		Baseline: &renumberedProvider{err: renumberedErr()},
		Lookback: lookback,
	}); serr != nil {
		t.Fatalf("SynthesizeVictims: %v", serr)
	}
	if len(sinceArgs) < 2 {
		t.Fatalf("expected the candidate fetch's time bounds, captured %d", len(sinceArgs))
	}
	if want := rootTS.Add(-lookback); !sinceArgs[0].Equal(want) {
		t.Errorf("since = %v, want rootTS-lookback %v", sinceArgs[0], want)
	}
}
