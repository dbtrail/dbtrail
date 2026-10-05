package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// The compaction job (#1723) never ran in a daemon until its wiring was fixed
// (TriggerRefresh did not stamp TableDeltas on the request). These are the
// things that only matter once it does run after every refresh.

// maybeCompact runs on the refresh's goroutine, after the refresh's own guard
// has returned. A panic in the scan for chains would take the daemon down,
// and capture with it.
func TestMaybeCompact_aPanicInTheScanStaysInTheJob(t *testing.T) {
	sup, req, _, _ := compactRig(t, compactMinPairs)
	listSnapshotChains = func(context.Context, string) (map[string]*baseline.TableDeltaChain, error) {
		panic("a footer the reader cannot take")
	}
	sup.maybeCompact(req) // must return
}

// A merge that fails is not tried again after every refresh: each try
// checksums every pair, runs DuckDB and holds the server's slot.
func TestMaybeCompact_aFailedMergeWaitsBeforeItIsTriedAgain(t *testing.T) {
	sup, req, cs, _ := compactRig(t, compactMinPairs)
	cs.err = errors.New("no room")
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, "s"); st.State != "failed" || len(cs.Calls()) != 1 {
		t.Fatalf("first try: status = %+v calls = %v", st, cs.Calls())
	}
	sup.maybeCompact(req)
	sup.maybeCompact(req)
	time.Sleep(50 * time.Millisecond) // a second try would have started a goroutine
	if calls := cs.Calls(); len(calls) != 1 {
		t.Fatalf("merges = %v: the failed merge was tried again at once", calls)
	}
	// Once the wait is over, and working again, it runs and the wait is gone.
	sup.mu.Lock()
	if until := sup.compactRetry["s"]; time.Until(until) < 30*time.Minute {
		t.Errorf("the wait ends at %s, want about an hour from now", until)
	}
	sup.compactRetry["s"] = time.Now().Add(-time.Second)
	sup.mu.Unlock()
	cs.mu.Lock()
	cs.err = nil
	cs.mu.Unlock()
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, "s"); st.State != "succeeded" || len(cs.Calls()) != 2 {
		t.Fatalf("after the wait: status = %+v calls = %v", st, cs.Calls())
	}
	sup.mu.Lock()
	_, waiting := sup.compactRetry["s"]
	sup.mu.Unlock()
	if waiting {
		t.Error("a run that succeeded left the wait in place")
	}
}

// The merged pair is a second copy of the pairs it merges. Without room for
// it the merge does not start.
func TestCompactJob_checksTheDiskBeforeItMerges(t *testing.T) {
	sup, req, cs, stage := compactRig(t, compactMinPairs)
	prev := diskSpaceFn
	t.Cleanup(func() { diskSpaceFn = prev })
	diskSpaceFn = func(string) (uint64, uint64, error) { return 1 << 20, 1 << 40, nil } // 1 MiB free
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "failed" || len(cs.Calls()) != 0 {
		t.Fatalf("status = %+v calls = %v, want the run refused before any merge", st, cs.Calls())
	}
	if want := errFoldDiskFull.Error(); !strings.Contains(st.LastError, want) {
		t.Errorf("LastError = %q, want it to say %q", st.LastError, want)
	}
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Errorf("a refused merge left a staging directory: %v", err)
	}
}

// A failure that repeats is one record, also with the refreshes that run
// between two tries: in a daemon a refresh record always sits in between.
func TestAppendCompact_aRepeatedFailureIsOneRecordAcrossRefreshes(t *testing.T) {
	h, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	fail := func(at string) console.BaselineRunRecord {
		return console.BaselineRunRecord{ServerID: "s", StartedAt: at, FinishedAt: at, Error: "no room", Refused: 1}
	}
	refresh := func(at string) {
		if err := h.Append(console.BaselineRunRecord{ServerID: "s", Kind: console.BaselineRunRefresh, StartedAt: at, FinishedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	added, _ := h.AppendCompact(fail("2026-10-05T10:00:00Z"))
	refresh("2026-10-05T10:05:00Z")
	added2, _ := h.AppendCompact(fail("2026-10-05T10:05:30Z"))
	refresh("2026-10-05T10:10:00Z")
	added3, _ := h.AppendCompact(fail("2026-10-05T10:10:30Z"))
	if !added || added2 || added3 {
		t.Errorf("new records = %v, %v, %v; want only the first", added, added2, added3)
	}
	var compacts []console.BaselineRunRecord
	for _, r := range h.List("s") {
		if r.Kind == console.BaselineRunCompact {
			compacts = append(compacts, r)
		}
	}
	if len(compacts) != 1 || compacts[0].FinishedAt != "2026-10-05T10:10:30Z" {
		t.Fatalf("compaction records = %+v, want one whose end moved to the last try", compacts)
	}
	// A different error, or a success in between, is a new record.
	other := fail("2026-10-05T10:15:30Z")
	other.Error = "pair 3: integrity"
	if added, _ := h.AppendCompact(other); !added {
		t.Error("a different failure was folded into the earlier one")
	}
}
