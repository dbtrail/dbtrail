package consoleapp

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// The Explain of a check pins the two snapshots the check compared while it
// runs, and lets them go when it ends (#2691: it had no test of its own). A
// pair in S3, or with a side missing, pins nothing and still runs.
func TestExplain_pinsTheComparedPairWhileItRuns(t *testing.T) {
	root := t.TempDir()
	prev, next := "2026-06-01T00-00-00Z", "2026-06-02T00-00-00Z"
	for _, c := range []struct {
		name string
		pair verify.BaselinePair
		want []string
	}{
		{"local pair", verify.BaselinePair{
			PrevPath: filepath.Join(root, prev, "wp", "posts.parquet"),
			NewPath:  filepath.Join(root, next, "wp", "posts.parquet"),
		}, []string{prev, next}},
		{"in S3, one side missing", verify.BaselinePair{
			PrevPath: "s3://bkt/b/" + prev + "/wp/posts.parquet",
		}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := explainableSupervisor(t)
			s.jobs["s1"].pairs["wp.posts"] = c.pair
			during := make(chan []string, 1)
			s.explainFn = func(context.Context, string, bool, string, string, verify.BaselinePair) (*console.VerifyExplanation, error) {
				during <- baseline.Pinned(root)
				return &console.VerifyExplanation{}, nil
			}
			if _, err := s.Explain("s1", "wp", "posts"); !errors.Is(err, console.ErrExplainRunning) {
				t.Fatalf("Explain: %v, want ErrExplainRunning", err)
			}
			var got []string
			select {
			case got = <-during:
			case <-time.After(5 * time.Second):
				t.Fatal("the Explain never ran")
			}
			if !slices.Equal(got, c.want) {
				t.Fatalf("pinned while the Explain ran: %v, want %v", got, c.want)
			}
			if _, err := waitExplain(t, s, "wp", "posts"); err != nil {
				t.Fatalf("the Explain failed: %v", err)
			}
			// The result is published just before the goroutine's deferred
			// releases run: give them a moment.
			left := baseline.Pinned(root)
			for i := 0; len(left) != 0 && i < 250; i++ {
				time.Sleep(20 * time.Millisecond)
				left = baseline.Pinned(root)
			}
			if len(left) != 0 {
				t.Fatalf("the Explain is over and %v is still pinned", left)
			}
		})
	}
}

// The port hands the shim the target's pin (#2691): without it, `_snapshot`
// reads unpinned whatever the console sets on the target.
func TestFlashbackShimConfig_carriesThePin(t *testing.T) {
	var pinned []string
	tgt := console.FlashbackTarget{PinBaseline: func(path, what string) func() {
		pinned = append(pinned, path+" "+what)
		return func() {}
	}}
	cfg := flashbackShimConfig(tgt, flashbackConfig{}, nil)
	if cfg.PinBaseline == nil {
		t.Fatal("the shim configuration carries no pin")
	}
	cfg.PinBaseline("/b/s/shop/orders.parquet", "a _snapshot query")()
	if !slices.Equal(pinned, []string{"/b/s/shop/orders.parquet a _snapshot query"}) {
		t.Fatalf("the shim's pin reached %v, want the target's", pinned)
	}
}
