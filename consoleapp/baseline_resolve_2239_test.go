package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2239, what #2231 left open in the resolved pairs job.

// A table whose merge failed waits an hour before it is tried again. That
// wait was earned by one chain: a table written again in full starts a new
// one, and a table that is gone has none, so neither keeps the entry.
func TestMaybeResolve_aWaitDoesNotOutliveTheChainThatEarnedIt(t *testing.T) {
	sup, req, rs, _ := resolveRig(t, []int{1}, map[string]int{"a": 2, "b": 2})
	// Another server's table waiting, in the same map: not this run's to drop.
	foreign := resolveRetryKey("other", filepath.Join(req.BaselineDir, "x", "shop", "b.parquet"))
	sup.resolveRetry[foreign] = time.Now().Add(time.Hour)

	rigList := listSnapshotChains
	pairsOfB := 2 // 0 leaves the table out of the listing
	listSnapshotChains = func(ctx context.Context, snapDir string) (map[string]*baseline.TableDeltaChain, error) {
		all, err := rigList(ctx, snapDir)
		out := map[string]*baseline.TableDeltaChain{}
		for base, c := range all {
			if filepath.Base(base) != "b.parquet" {
				out[base] = c
				continue
			}
			if pairsOfB > 0 {
				out[base] = &baseline.TableDeltaChain{Files: c.Files[:pairsOfB]}
			}
		}
		return out, err
	}
	waiting := func() int {
		sup.mu.Lock()
		defer sup.mu.Unlock()
		n := 0
		for k := range sup.resolveRetry {
			if k != foreign {
				n++
			}
		}
		return n
	}
	fail := func(table string) {
		rs.mu.Lock()
		rs.failTable = table
		rs.mu.Unlock()
	}

	for _, c := range []struct {
		name  string
		pairs int
	}{{"written again in full: a chain of one pair", 1}, {"gone from the snapshot", 0}} {
		fail("b")
		pairsOfB = 2
		sup.maybeResolve(req)
		if waiting() != 1 {
			t.Fatalf("%s: tables waiting = %d after b failed, want 1", c.name, waiting())
		}
		fail("")
		tried := len(rs.Calls())
		pairsOfB = c.pairs
		sup.maybeResolve(req)
		if waiting() != 0 {
			t.Fatalf("%s: b still waits out the hour of a chain it no longer has", c.name)
		}
		if len(rs.Calls()) != tried {
			t.Fatalf("%s: a merge ran with nothing to merge: %+v", c.name, rs.Calls()[tried:])
		}
		// Its chain grows back to two pairs within the hour: tried at once.
		pairsOfB = 2
		sup.maybeResolve(req)
		if calls := rs.Calls(); len(calls) != tried+1 || calls[tried].table != "b" {
			t.Fatalf("%s: with a new chain of two pairs b was not tried: %+v", c.name, calls[tried:])
		}
		// Back to no pair of b for the next case.
		if err := os.RemoveAll(filepath.Join(req.BaselineDir, baseline.ResolvedDirName)); err != nil {
			t.Fatal(err)
		}
	}
	sup.mu.Lock()
	_, kept := sup.resolveRetry[foreign]
	sup.mu.Unlock()
	if !kept {
		t.Error("another server's waiting table was dropped by this server's run")
	}
}
