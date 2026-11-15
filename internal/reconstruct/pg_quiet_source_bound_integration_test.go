//go:build integration

package reconstruct_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// caughtUp records that capture was last caught up with its source at d into
// the hour (stream_state.caught_up_at).
func (f *pgFixture) caughtUp(d time.Duration) {
	f.t.Helper()
	testutil.MustExec(f.t, f.db, fmt.Sprintf("UPDATE stream_state SET caught_up_at = '%s' WHERE id = 1", f.at(d).Format("2006-01-02 15:04:05.000000")))
}

// A source with no change since the snapshot: nothing is folded, and the
// time bound the folded snapshot records still moves, to when capture was
// last caught up. Without that moment (no stamp, an index that predates it)
// the bound stays where the snapshot had it, as before.
func TestReconstructTables_PGQuietSourceMovesTheBound(t *testing.T) {
	cases := []struct {
		name  string
		setup func(f *pgFixture)
		want  time.Duration // the bound the folded snapshot records, into the hour
	}{
		{name: "caught up", setup: func(f *pgFixture) { f.caughtUp(25 * time.Minute) }, want: 24 * time.Minute},
		{name: "never caught up", setup: func(*pgFixture) {}, want: 9 * time.Minute},
		{name: "an index that predates the stamp", setup: func(f *pgFixture) {
			testutil.MustExec(f.t, f.db, "ALTER TABLE stream_state DROP COLUMN caught_up_at")
		}, want: 9 * time.Minute},
		// The newest change is not checkpointed: it is left to the next
		// fold, and it committed before the stamp.
		{name: "caught up, the newest change not checkpointed", setup: func(f *pgFixture) {
			f.event(0x600, 12*time.Minute, 2, "1", s("in-flight"))
			f.caughtUp(25 * time.Minute)
		}, want: 11 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newPGFixture(t)
			// A change before the snapshot's floor, checkpointed: the index
			// is not empty and holds nothing to fold.
			f.event(0x480, 5*time.Minute, 2, "1", s("before"))
			f.checkpoint(0x480)
			tc.setup(f)
			root := t.TempDir()
			f.baseline(root, f.at(10*time.Minute), 0x500, "", [2]string{"1", "paid"})

			got, md, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			if got["1"] != "paid" {
				t.Fatalf("rows = %v", got)
			}
			if want := f.at(tc.want).Format(time.RFC3339); md.PGDeltaSince != want {
				t.Fatalf("the folded snapshot's bound = %q, want %q", md.PGDeltaSince, want)
			}
		})
	}
}
