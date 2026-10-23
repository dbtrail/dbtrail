package oracle

import (
	"context"
	"math/rand/v2"
	"testing"
)

type fakeStep struct {
	name string
	w    int
}

func (f fakeStep) Name() string                      { return f.name }
func (f fakeStep) Weight() int                       { return f.w }
func (f fakeStep) Apply(context.Context, *Cut) error { return nil }
func (f fakeStep) Publishes() bool                   { return true }

// TestCut_sincePublish: the window since the published cut is what a
// refresh folds and a tail holds, so a touch is asked over it, not over
// this step's ops; a no-op and an op that changed nothing do not touch.
func TestCut_sincePublish(t *testing.T) {
	c := &Cut{
		Ops: []Op{{Table: "t_int", Changes: []Change{{Kind: KindUpdate, Key: 1}}}},
		SincePublish: []Op{
			{Table: "t_uniq", Changes: []Change{{Kind: KindPKUpdate, Key: 3, NewKey: 4}}},
			{Table: "t_comp", Noop: true, Changes: []Change{{Kind: KindUpdate, Key: 5}}},
			{Table: "t_text"},
			{Table: "t_int", Changes: []Change{{Kind: KindUpdate, Key: 1}}},
		},
	}
	for table, want := range map[string]bool{"t_int": true, "t_uniq": true, "t_comp": false, "t_text": false, "t_none": false} {
		if got := c.TouchedSincePublish(table); got != want {
			t.Errorf("TouchedSincePublish(%s) = %v, want %v", table, got, want)
		}
	}
}

// TestSteps_publish: a snapshot and a refresh publish the cut; the run
// compares a table the port answers as of the published cut with the rows
// of the last such step, so a step that said it publishes when it does not
// would compare against rows the copy cannot hold.
func TestSteps_publish(t *testing.T) {
	if !(&SnapshotStep{}).Publishes() || !(&RefreshStep{}).Publishes() {
		t.Fatal("a snapshot and a refresh publish the cut")
	}
}

// TestRun_drawStepFollowsWeights: steps are drawn by weight from a stream of
// the seed's own, and the draw is reproducible.
func TestRun_drawStepFollowsWeights(t *testing.T) {
	r := &Run{Between: []Step{fakeStep{"snapshot", 1}, fakeStep{"refresh", 3}}}
	count := map[string]int{}
	draw := rand.New(rand.NewPCG(5, 1))
	for range 4000 {
		count[r.drawStep(draw).Name()]++
	}
	if count["refresh"] < 2800 || count["refresh"] > 3200 {
		t.Fatalf("refresh drawn %d of 4000 with weight 3 of 4", count["refresh"])
	}
	a, b := rand.New(rand.NewPCG(5, 1)), rand.New(rand.NewPCG(5, 1))
	for range 100 {
		if r.drawStep(a).Name() != r.drawStep(b).Name() {
			t.Fatal("the step draw is not reproducible for one seed")
		}
	}
}
