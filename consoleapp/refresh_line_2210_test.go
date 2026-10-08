package consoleapp

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

// The fold is handed half of the console's line, read live at each cycle
// (#2210): a memory change saved in the web interface moves it on the next
// refresh. With no line wired there is no rule.
func TestRunRefresh_foldEndsChainsAtHalfTheLine(t *testing.T) {
	folds := probeFolds(t)
	stubCoverage(t, false, true) // never skipped as quiet: every cycle folds
	stubLiveFloor(t, refreshAt.Add(-40*time.Minute), true)
	stubReadsFrom(t, refreshAt.Add(-10*time.Minute), nil)
	stubIndexRetention(t, rotation.Effective{Retain: 48 * time.Hour, Raw: "48h", Source: rotation.RetainRecorded})
	sup, _, run := deltasRig(t, true)

	run(refreshAt)
	line := int64(48 << 20)
	get := func() int64 { return line }
	sup.sqlChainLine.Store(&get)
	run(refreshAt.Add(5 * time.Minute))
	line = 96 << 20
	run(refreshAt.Add(10 * time.Minute))

	cfgs := folds.all()
	if len(cfgs) != 3 {
		t.Fatalf("folded %d time(s), want 3", len(cfgs))
	}
	for i, want := range []int64{0, 24 << 20, 48 << 20} {
		if got := cfgs[i].MaxChainUpserts; got != want {
			t.Errorf("cycle %d: MaxChainUpserts = %d, want %d", i, got, want)
		}
	}
}

// What watch wires is the console's FOLD line, not its refusal line: at the
// default 2 GB a table's chain ends at 24 MB, not at half of the 384 MB past
// which SQL on the copy answers from an earlier copy (#2210).
func TestWireSQLChainLine_handsTheFoldLine(t *testing.T) {
	folds := probeFolds(t)
	stubCoverage(t, false, true)
	stubLiveFloor(t, refreshAt.Add(-40*time.Minute), true)
	stubReadsFrom(t, refreshAt.Add(-10*time.Minute), nil)
	stubIndexRetention(t, rotation.Effective{Retain: 48 * time.Hour, Raw: "48h", Source: rotation.RetainRecorded})
	sup, _, run := deltasRig(t, true)
	srv := &console.Server{}
	wireSQLChainLine(sup, srv)
	run(refreshAt)
	cfgs := folds.all()
	if len(cfgs) != 1 {
		t.Fatalf("folded %d time(s), want 1", len(cfgs))
	}
	if got := cfgs[0].MaxChainUpserts; got != 24<<20 {
		t.Errorf("MaxChainUpserts = %d MiB, want 24 (half the fold line; the refusal line is %d MiB)", got>>20, srv.SQLChainLimit()>>20)
	}
}
