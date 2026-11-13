package consoleapp

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The snapshots S3 could not be asked about (#2689) are sent again like the
// ones it lacks, and the record the page reads after the round keeps them
// named as not asked, minus the ones just sent.
func TestPruneCycleKeepsNotAskedOnTheRecord(t *testing.T) {
	d := t.TempDir()
	for _, n := range []string{"a", "b", "c"} {
		snapDir(t, d, n, true)
	}
	prev := resendSnapshotFn
	t.Cleanup(func() { resendSnapshotFn = prev })
	var tried []string
	resendSnapshotFn = func(_ context.Context, _, _, name string) error {
		tried = append(tried, name)
		if name == "a" {
			return nil
		}
		return errors.New("put: 403 Forbidden")
	}
	prune := func(_ context.Context, o baseline.PruneOptions) (baseline.PruneResult, error) {
		return baseline.PruneResult{KeptNotDurable: 3, Denied: 2, NotAtDestination: []string{"a", "b", "c"}, NotAsked: []string{"a", "c"}}, nil
	}
	runBaselinePruneCycle(context.Background(), []baselinePruneTarget{{dir: d, s3: "s3://bkt/p/"}}, 4*60*60*1e9, prune)
	if !slices.Equal(tried, []string{"a", "b", "c"}) {
		t.Fatalf("tried %v, want a, b and c: a snapshot not asked about is sent again too", tried)
	}
	rec, ok, err := baseline.ReadNotInDestination(d)
	if err != nil || !ok || rec.Count != 2 || !slices.Equal(rec.Names, []string{"b", "c"}) || !slices.Equal(rec.NotAsked, []string{"c"}) || rec.NextSend == nil {
		t.Fatalf("record %+v ok=%v err=%v: want b and c left, c as not asked, and the hour's wait", rec, ok, err)
	}
}
