package cascade_test

import (
	"context"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2186: a baseline used although the binlog-renumbering check could not
// tell (the index was rebuilt or backfilled, the mark's event is gone, an
// archived hour has no readable file) makes the result incomplete, with the
// check's note: the recovery must not be reported complete over a window
// nobody could check.
type uncheckedProvider struct{ note string }

func (p uncheckedProvider) BaselineChildren(_ context.Context, _, _, _, _ string, at time.Time, _ int) (cascade.BaselineLookup, bool, error) {
	return cascade.BaselineLookup{SnapshotTime: at.Add(-time.Hour), UncheckedMessage: p.note}, true, nil
}

func TestSynthesizeVictims_uncheckedNumberingMarksIncomplete_2186(t *testing.T) {
	const note = "binlog numbering not checked: `bintrail index` also wrote into this index, so the order of its event ids is not the order of the binary log, so whether the source's binary log started again after the snapshot cannot be told"
	for _, tc := range []struct {
		name, note string
		want       bool
	}{
		{"checked", "", false},
		{"not checked", note, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.MatchExpectationsInOrder(false)
			for range 4 {
				mock.ExpectQuery(`.`).WillReturnRows(sqlmock.NewRows([]string{"x"}))
			}
			fks, parents := twoParentDeletes()
			res, _ := cascade.SynthesizeVictims(context.Background(), query.New(db), fks, parents[:1], cascade.Options{Baseline: uncheckedProvider{tc.note}})
			var got string
			for _, msg := range res.Incomplete {
				if strings.Contains(msg, "binlog numbering not checked") {
					got = msg
				}
			}
			if (got != "") != tc.want {
				t.Fatalf("Incomplete = %v; want the note: %v", res.Incomplete, tc.want)
			}
			if !tc.want {
				return
			}
			for _, w := range []string{"app.child's baseline snapshot is used", "`bintrail index`", "may be partial"} {
				if !strings.Contains(got, w) {
					t.Errorf("caveat does not say %q: %s", w, got)
				}
			}
			// recover_cascade (MCP) returns this caveat to the client.
			if strings.Contains(got, "--") {
				t.Errorf("the caveat names a CLI flag: %s", got)
			}
		})
	}
}
