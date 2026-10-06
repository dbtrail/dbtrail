//go:build integration

package cascade_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2186 review: the binlog-numbering note marks a cascade incomplete only
// where the baseline is used: its rows reach the output, or (no row matched)
// the candidate scan read the window from the snapshot's position. Never on
// a lookup the engine did not use.
func TestPhase2_uncheckedNumberingOnlyWhereTheBaselineIsUsed_2186(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	eng := query.New(db)
	T := time.Now().UTC()
	const note = "binlog numbering not checked: `bintrail index` also wrote into this index"
	pos := &query.BinlogPos{File: "binlog.000007", Pos: 200}
	row := []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}}
	for _, tc := range []struct {
		name string
		rows []cascade.BaselineRow
		pos  *query.BinlogPos
		want bool
	}{
		{"a baseline row reaches the output", row, nil, true},
		{"no row, the scan read from the snapshot's position", nil, pos, true},
		{"no row, no position: the baseline shaped nothing", nil, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prov := &fakeUnchecked{snap: T.Add(-2 * time.Hour), rows: tc.rows, pos: tc.pos, note: note}
			res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T), cascade.Options{Baseline: prov})
			if err != nil {
				t.Fatalf("SynthesizeVictims: %v", err)
			}
			got := false
			for _, msg := range res.Incomplete {
				if strings.Contains(msg, note) && strings.Contains(msg, "baseline snapshot is used") {
					got = true
				}
			}
			if got != tc.want {
				t.Fatalf("Incomplete = %v; want the note: %v", res.Incomplete, tc.want)
			}
		})
	}
}

type fakeUnchecked struct {
	snap time.Time
	rows []cascade.BaselineRow
	pos  *query.BinlogPos
	note string
}

func (f *fakeUnchecked) BaselineChildren(context.Context, string, string, string, string, time.Time, int) (cascade.BaselineLookup, bool, error) {
	return cascade.BaselineLookup{SnapshotTime: f.snap, Rows: f.rows, SincePos: f.pos, UncheckedMessage: f.note}, true, nil
}
