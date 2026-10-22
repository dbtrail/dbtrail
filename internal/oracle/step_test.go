package oracle

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// pkChangeErr is the fold's own refusal text, so a reword of it fails here
// instead of silently turning every refused refresh into a run failure.
func pkChangeErr(table string) error {
	return reconstruct.PKChangingUpdateError("s", table, "7", "28")
}

// TestRefreshStep_refusedOnlyForTheGeneratorsPKChange: a refresh counts as
// refused only when every failed table failed on the fold's PK-change guard
// and this step's ops moved a row on that table. Anything else is a failure
// of the run, so a product defect cannot hide behind the fallback.
func TestRefreshStep_refusedOnlyForTheGeneratorsPKChange(t *testing.T) {
	s := &RefreshStep{}
	moved := &Cut{Ops: []Op{{Table: "t_int", Changes: []Change{{Kind: KindPKUpdate, Key: 7, NewKey: 28}}}}}
	quiet := &Cut{Ops: []Op{{Table: "t_int", Changes: []Change{{Kind: KindUpdate, Key: 7}}}}}
	cases := []struct {
		name string
		err  error
		cut  *Cut
		want bool
	}{
		{"pk change on the moved table", &RefreshError{Failures: []reconstruct.TableFailure{{Table: "t_int", Err: pkChangeErr("t_int")}}, Err: pkChangeErr("t_int")}, moved, true},
		{"pk change on a table whose ops moved nothing", &RefreshError{Failures: []reconstruct.TableFailure{{Table: "t_text", Err: pkChangeErr("t_text")}}, Err: pkChangeErr("t_text")}, moved, false},
		{"pk change with no PK-changing op", &RefreshError{Failures: []reconstruct.TableFailure{{Table: "t_int", Err: pkChangeErr("t_int")}}, Err: pkChangeErr("t_int")}, quiet, false},
		{"one table refused, another failed otherwise", &RefreshError{Failures: []reconstruct.TableFailure{
			{Table: "t_int", Err: pkChangeErr("t_int")}, {Table: "t_comp", Err: errors.New("capture gap")}}, Err: errors.New("joined")}, moved, false},
		{"another error on the moved table", &RefreshError{Failures: []reconstruct.TableFailure{{Table: "t_int", Err: errors.New("capture gap")}}, Err: errors.New("capture gap")}, moved, false},
		{"an error that is not a RefreshError", pkChangeErr("t_int"), moved, false},
		{"a RefreshError without failures", &RefreshError{Err: errors.New("index unreachable")}, moved, false},
	}
	for _, c := range cases {
		if got := s.Refused(c.err, c.cut); got != c.want {
			t.Errorf("%s: refused = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestCut_hadPKChangeOn(t *testing.T) {
	c := &Cut{Ops: []Op{
		{Table: "t_int", Changes: []Change{{Kind: KindInsert, Key: 1}}},
		{Table: "t_uniq", Changes: []Change{{Kind: KindDelete, Key: 2}, {Kind: KindPKUpdate, Key: 3, NewKey: 4}}},
	}}
	if !c.HadPKChange() || !c.HadPKChangeOn("t_uniq") || c.HadPKChangeOn("t_int") {
		t.Fatalf("any=%v t_uniq=%v t_int=%v; want true true false", c.HadPKChange(), c.HadPKChangeOn("t_uniq"), c.HadPKChangeOn("t_int"))
	}
}

type fakeStep struct {
	name string
	w    int
}

func (f fakeStep) Name() string                      { return f.name }
func (f fakeStep) Weight() int                       { return f.w }
func (f fakeStep) Apply(context.Context, *Cut) error { return nil }

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
