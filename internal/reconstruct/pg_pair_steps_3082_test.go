package reconstruct

import (
	"testing"
	"time"
)

// pgStepFields is every step field a pair line carries, the end of a pair's
// with fold_pair_ms.
var pgStepFields = []string{"fetch_ms", "fold_ms", "positions_ms", "pair_write_ms", "carry_ms"}

// checkPGStepFields fails t unless fields (a log line's, as numbers) holds
// every step and total_ms, and the steps add up to no more than the total.
func checkPGStepFields(t *testing.T, line string, fields map[string]int64, ending bool) {
	t.Helper()
	want := pgStepFields
	if ending {
		want = append(append([]string{}, want...), "fold_pair_ms")
	}
	var sum int64
	for _, k := range want {
		v, ok := fields[k]
		if !ok || v < 0 {
			t.Fatalf("%s: field %s is %d (present %v)", line, k, v, ok)
		}
		sum += v
	}
	total, ok := fields["total_ms"]
	if !ok || sum > total {
		t.Fatalf("%s: the steps add up to %d ms, the whole is %d ms (present %v)", line, sum, total, ok)
	}
}

// The step times of a pair line: every field, as milliseconds, adding up to
// no more than the whole since the table started; fold_pair_ms only on the
// line of a pair that ends.
func TestPGPairStepsAttrs(t *testing.T) {
	s := pgPairSteps{
		start: time.Now().Add(-10 * time.Second),
		fetch: 1500 * time.Millisecond, fold: 700 * time.Millisecond,
		positions: 2 * time.Second, carry: 30 * time.Millisecond,
		pairWrite: 3 * time.Second, foldPair: 900 * time.Millisecond,
	}
	for _, ending := range []bool{false, true} {
		a := s.attrs(ending)
		fields := map[string]int64{}
		for i := 0; i+1 < len(a); i += 2 {
			fields[a[i].(string)] = a[i+1].(int64)
		}
		checkPGStepFields(t, "attrs", fields, ending)
		if _, has := fields["fold_pair_ms"]; has != ending {
			t.Fatalf("ending %v: fold_pair_ms present %v", ending, has)
		}
		if fields["positions_ms"] != 2000 || fields["pair_write_ms"] != 3000 || fields["total_ms"] < 10000 {
			t.Fatalf("ending %v: %v", ending, fields)
		}
	}
}

// The key index's build is on the table's line only on the update that
// paid for it (#3085), as on the MySQL line.
func TestPGPairStepsSayTheKeyIndexBuildOnce(t *testing.T) {
	has := func(a []any) bool {
		for i := 0; i < len(a); i += 2 {
			if a[i] == "lookup_index_built_ms" {
				return true
			}
		}
		return false
	}
	if has(pgPairSteps{start: time.Now()}.attrs(false)) {
		t.Error("lookup_index_built_ms on an update that built no index")
	}
	if !has(pgPairSteps{start: time.Now(), indexBuilt: 7 * time.Millisecond}.attrs(true)) {
		t.Error("no lookup_index_built_ms on the update that built the index")
	}
}
