package consoleapp

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// The pg-router's thresholds for routing by the source's plan: their
// defaults, where each value comes from, and what is refused.
func TestPGRouterCostSettings(t *testing.T) {
	clearPGRouterEnv(t)
	st, err := pgRouterSettingsFrom(pgRouterTestCmd(t))
	if err != nil {
		t.Fatal(err)
	}
	if st.Cost != (pgshim.CostPolicy{CostThreshold: 50000, ScanRows: 1000000}) {
		t.Fatalf("defaults: %+v", st.Cost)
	}

	t.Setenv("BINTRAIL_PG_ROUTER_COST_THRESHOLD", " 1234.5 ")
	t.Setenv("BINTRAIL_PG_ROUTER_SCAN_ROWS", "77")
	if st, err = pgRouterSettingsFrom(pgRouterTestCmd(t)); err != nil || st.Cost != (pgshim.CostPolicy{CostThreshold: 1234.5, ScanRows: 77}) {
		t.Fatalf("from the environment: %+v %v", st.Cost, err)
	}
	// A flag wins over its variable, and one rule may be off.
	if st, err = pgRouterSettingsFrom(pgRouterTestCmd(t, "--cost-threshold", "0", "--scan-rows", "5")); err != nil || st.Cost != (pgshim.CostPolicy{CostThreshold: 0, ScanRows: 5}) {
		t.Fatalf("flags over the environment: %+v %v", st.Cost, err)
	}
	clearPGRouterEnv(t)

	for _, tc := range []struct {
		args []string
		env  [2]string
		want string
	}{
		{[]string{"--cost-threshold", "0", "--scan-rows", "0"}, [2]string{}, "are both 0"},
		{[]string{"--cost-threshold", "-1"}, [2]string{}, "--cost-threshold must be 0 or more"},
		{[]string{"--scan-rows", "-1"}, [2]string{}, "--scan-rows must be 0 or more"},
		{[]string{"--cost-threshold", "NaN"}, [2]string{}, "--cost-threshold must be a number"},
		{[]string{"--cost-threshold", "+Inf"}, [2]string{}, "--cost-threshold must be a number"},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_COST_THRESHOLD", "cheap"}, `BINTRAIL_PG_ROUTER_COST_THRESHOLD="cheap" is not a number`},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_SCAN_ROWS", "1e6"}, `BINTRAIL_PG_ROUTER_SCAN_ROWS="1e6" is not a whole number`},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_SCAN_ROWS", "-5"}, "BINTRAIL_PG_ROUTER_SCAN_ROWS must be 0 or more"},
		{[]string{"--cost-threshold", "0"}, [2]string{"BINTRAIL_PG_ROUTER_SCAN_ROWS", "0"}, "--cost-threshold and BINTRAIL_PG_ROUTER_SCAN_ROWS are both 0"},
	} {
		clearPGRouterEnv(t)
		if tc.env[0] != "" {
			t.Setenv(tc.env[0], tc.env[1])
		}
		_, err := pgRouterSettingsFrom(pgRouterTestCmd(t, tc.args...))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v %v: %v, want an error saying %q", tc.args, tc.env, err, tc.want)
		}
	}
}

func TestPGRouterCostStartupLine(t *testing.T) {
	for _, tc := range []struct {
		pol  pgshim.CostPolicy
		want []string
	}{
		{pgshim.DefaultCostPolicy(), []string{"costs 50,000 or more (--cost-threshold)", "sequential scan of 1,000,000 rows or more (--scan-rows)", "has no index"}},
		{pgshim.CostPolicy{CostThreshold: 2500}, []string{"costs 2,500 or more", "the rule on sequential scans is off"}},
		{pgshim.CostPolicy{ScanRows: 999}, []string{"sequential scan of 999 rows or more", "the rule on cost is off"}},
	} {
		st := pgRouterSettings{Listen: "127.0.0.1:5433", MaxSourceConns: 16, Cost: tc.pol}
		out := pgRouterStartupLines(st, "2GB", "the default", nil, 1, true)
		for _, w := range tc.want {
			if !strings.Contains(out, w) {
				t.Errorf("%+v: the start-up lines do not say %q:\n%s", tc.pol, w, out)
			}
		}
	}
	for n, want := range map[int64]string{0: "0", 7: "7", 999: "999", 1000: "1,000", 50000: "50,000", 1000000: "1,000,000", 13351258: "13,351,258", -1500: "-1,500"} {
		if got := groupThousands(n); got != want {
			t.Errorf("groupThousands(%d) = %q, want %q", n, got, want)
		}
	}
}
