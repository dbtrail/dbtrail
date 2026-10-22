package reconstruct

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// #2496 slice 1: the base-position lookup reports what it cost (time and
// base rows examined) beside fetch_ms / fold_ms, and the join path reads
// the base's key columns ONCE per lookup: the NULL-key check rides the same
// DuckDB pass as the position lookup instead of a second full pass.

// nullKeyBase writes an integer-keyed base whose second row has a NULL key.
func nullKeyBase(t *testing.T) string {
	t.Helper()
	cols, err := baseline.ParseSchemaText(intKeyDDL)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(t.TempDir(), "t.parquet")
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 7})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{{"1", "a"}, {"", "lost"}, {"3", "c"}} {
		if err := w.WriteRow(r, []bool{r[0] == "", false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return base
}

// TestLookupBasePositions_oneBasePassPerLookup_2496: every lookup, joined
// or scanned, reads the base's rows in exactly one DuckDB query — including
// the window whose keys are all out of range, where the only thing left to
// do is the NULL check, and the base with no rows at all.
func TestLookupBasePositions_oneBasePassPerLookup_2496(t *testing.T) {
	ctx := context.Background()
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	empty, _ := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, nil)
	for _, tc := range []struct {
		name  string
		base  string
		keys  []string
		allow bool
		want  []int64
	}{
		{"join, keys in the base", base, []string{"1", "3"}, true, []int64{0, 2}},
		{"scan, keys in the base", base, []string{"1", "3"}, false, []int64{0, 2}},
		{"join, every key out of range", base, []string{"3000000000"}, true, nil},
		{"join, empty base", empty, []string{"1"}, true, nil},
		{"scan, empty base", empty, []string{"1"}, false, nil},
	} {
		got, st, err := lookupBasePositionsWith(ctx, tc.base, "s", "t", pkCols, touched(tc.keys...), duckdbutil.DefaultTuning(), tc.allow)
		if err != nil || !reflect.DeepEqual(got, tc.want) || st.Joined != tc.allow {
			t.Fatalf("%s: err=%v got=%v st=%+v", tc.name, err, got, st)
		}
		if st.BaseReads != 1 {
			t.Fatalf("%s: the base's rows were read in %d queries, want 1", tc.name, st.BaseReads)
		}
	}
}

// TestLookupBasePositions_nullKeyRefusedFromTheJoinPass_2496: a NULL key
// is refused with the same text as before, whether the window's other keys
// match rows before and after the NULL row, match nothing, or are all out
// of range — and from the one pass, not a second one.
func TestLookupBasePositions_nullKeyRefusedFromTheJoinPass_2496(t *testing.T) {
	base := nullKeyBase(t)
	pkCols := pkColsIntID()
	for _, keys := range [][]string{{"1", "3"}, {"1"}, {"3"}, {"7"}, {"3000000000"}, {"1", "3000000000"}} {
		_, st, err := lookupBasePositionsWith(context.Background(), base, "s", "t", pkCols, touched(keys...), duckdbutil.DefaultTuning(), true)
		if err == nil || !st.Joined || st.BaseReads != 1 {
			t.Fatalf("keys %v: err=%v st=%+v; want the refusal from one joined pass", keys, err, st)
		}
		want := `canonicalize baseline PK for s.t: key column "id" holds NULL in the base file (MySQL forbids NULL in PK columns; a baseline written before #522 lost unsigned values past the signed range this way — take a fresh baseline)`
		if err.Error() != want {
			t.Fatalf("keys %v: refusal text changed:\n got: %s\nwant: %s", keys, err, want)
		}
	}
	// The scan path still refuses when it reaches the row.
	_, st, err := lookupBasePositionsWith(context.Background(), base, "s", "t", pkCols, touched("1", "3"), duckdbutil.DefaultTuning(), false)
	if err == nil || st.Joined || !strings.Contains(err.Error(), "NULL") || st.BaseReads != 1 {
		t.Fatalf("scan: err=%v st=%+v", err, st)
	}
}

// TestLookupBasePositions_compositeNullNamesItsColumn_2496: NULL in the
// second column of a composite key names that column, with the first
// column's NULL check ahead of it in the same row.
func TestLookupBasePositions_compositeNullNamesItsColumn_2496(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `w` smallint NOT NULL,\n  `d` bigint unsigned NOT NULL,\n  `v` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`w`,`d`)\n) ENGINE=InnoDB;\n"
	cols, err := baseline.ParseSchemaText(ddl)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(t.TempDir(), "c.parquet")
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 7})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{{"1", "1", "a"}, {"1", "", "lost"}, {"2", "2", "c"}} {
		if err := w.WriteRow(r, []bool{false, r[1] == "", false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	pk := []metadata.ColumnMeta{{Name: "w", OrdinalPosition: 1, IsPK: true, DataType: "smallint"}, {Name: "d", OrdinalPosition: 2, IsPK: true, DataType: "bigint"}}
	for _, keys := range [][]string{{"1|1"}, {"2|2"}, {"9|9"}} {
		_, st, err := lookupBasePositionsWith(context.Background(), base, "s", "t", pk, touched(keys...), duckdbutil.DefaultTuning(), true)
		if err == nil || !st.Joined || st.BaseReads != 1 || !strings.Contains(err.Error(), `key column "d" holds NULL in the base file`) {
			t.Fatalf("keys %v: err=%v st=%+v", keys, err, st)
		}
	}
}

// TestTableDelta_reportsLookupCost_2496: the report carries the lookup's
// time and the base rows it examined, summed over the passes of a spilled
// window: the join examines only the matches, the scan every base row once
// per pass.
func TestTableDelta_reportsLookupCost_2496(t *testing.T) {
	for _, tc := range []struct {
		name string
		join bool
	}{
		{"integer keys joined in DuckDB", true},
		{"key columns scanned through Go", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			noCompaction(t)
			prev := tableDeltaJoinKeys
			tableDeltaJoinKeys = tc.join
			t.Cleanup(func() { tableDeltaJoinKeys = prev })

			memBase, _, memRep := spilledChain(t, bigWindow("final"), nil)
			baseRows := int64(baseRowCount(t, memBase))
			if memRep.DeltaSpillPasses != 0 || memRep.DeltaDeadRows == 0 || baseRows == 0 {
				t.Fatalf("fixture: passes=%d dead=%d base rows=%d", memRep.DeltaSpillPasses, memRep.DeltaDeadRows, baseRows)
			}
			want := baseRows
			if tc.join {
				want = memRep.DeltaDeadRows
			}
			if memRep.LookupRows != want || memRep.LookupDuration <= 0 {
				t.Fatalf("in memory: lookup_rows=%d lookup=%s; want %d rows and a positive time", memRep.LookupRows, memRep.LookupDuration, want)
			}

			w := bigWindow("final")
			spill := func(t *testing.T) *changeSpill { return spillOf(t, 4, cloneChanges(w)) }
			_, _, rep := spilledChain(t, w, spill)
			if rep.DeltaSpillPasses < 3 {
				t.Fatalf("the spill was read in %d pass(es): the fixture has to exercise more than one", rep.DeltaSpillPasses)
			}
			want = baseRows * int64(rep.DeltaSpillPasses)
			if tc.join {
				want = rep.DeltaDeadRows
			}
			if rep.LookupRows != want || rep.LookupDuration <= 0 {
				t.Fatalf("spilled in %d passes: lookup_rows=%d lookup=%s; want %d rows and a positive time",
					rep.DeltaSpillPasses, rep.LookupRows, rep.LookupDuration, want)
			}
			if rep.DeltaDeadRows != memRep.DeltaDeadRows {
				t.Fatalf("dead rows %d from the spill, %d from memory", rep.DeltaDeadRows, memRep.DeltaDeadRows)
			}
		})
	}
}

// TestTableDelta_logLineCarriesLookupCost_2496: the per-table line carries
// lookup_ms and lookup_rows with the report's values, beside fetch_ms and
// fold_ms. Process-global (slog.SetDefault): not parallel.
func TestTableDelta_logLineCarriesLookupCost_2496(t *testing.T) {
	noCompaction(t)
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	_, _, rep := spilledChain(t, bigWindow("final"), nil)
	var line string
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.Contains(l, `msg="table published as a delta over its previous file"`) {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no delta line in the log:\n%s", buf.String())
	}
	for _, want := range []string{
		fmt.Sprintf(" lookup_ms=%d ", rep.LookupDuration.Milliseconds()),
		fmt.Sprintf(" lookup_rows=%d", rep.LookupRows),
		fmt.Sprintf(" fetch_ms=%d ", rep.FetchDuration.Milliseconds()),
		fmt.Sprintf(" fold_ms=%d ", rep.FoldDuration.Milliseconds()),
	} {
		if !strings.Contains(line+" ", want) {
			t.Errorf("the delta line lacks %q:\n%s", want, line)
		}
	}
	if rep.LookupRows == 0 {
		t.Errorf("the fixture's lookup examined no rows, so the line pins nothing")
	}
}
