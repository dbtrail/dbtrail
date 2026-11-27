package consoleapp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// The PostgreSQL port's row cap is its own (#3286): 200,000 rows, where the
// MySQL port keeps 10,000. CH-benCHmark's Q16 returns about 100,000.
func TestPGRouterRowCapIsItsOwn_3286(t *testing.T) {
	if defaultPGRouterMaxRows != 200_000 || console.DefaultSQLPortMaxRows != 10_000 {
		t.Fatalf("PostgreSQL port %d, MySQL port %d; want 200,000 and 10,000", defaultPGRouterMaxRows, console.DefaultSQLPortMaxRows)
	}
	st, err := pgRouterSettingsFrom(pgRouterTestCmd(t))
	if err != nil {
		t.Fatal(err)
	}
	// The plan rule that sends an estimated big result to the source moves
	// with it: a result estimated under 200,000 rows is tried on the copy.
	if st.SQLPortMaxRows != 200_000 || st.Cost.RowCap != 200_000 {
		t.Errorf("row cap %d, cost rule's %d; want 200,000 for both", st.SQLPortMaxRows, st.Cost.RowCap)
	}
	t.Setenv("BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS", "5000")
	if st, err = pgRouterSettingsFrom(pgRouterTestCmd(t)); err != nil || st.SQLPortMaxRows != 5000 || st.Cost.RowCap != 5000 {
		t.Errorf("the variable still sets it: %d, %d, %v", st.SQLPortMaxRows, st.Cost.RowCap, err)
	}
}

// /status counts the results refused for their size, by which cap.
func TestPGRouteStatsCountsResultTooLarge_3286(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy, Failed: true, TooLarge: pgshim.TooLargeRows})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy, Failed: true, TooLarge: pgshim.TooLargeRows})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy, Failed: true, TooLarge: pgshim.TooLargeBytes})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy})
	n := r.doc("").Servers["s1"]
	if n.ResultTooLarge["rows"] != 2 || n.ResultTooLarge["bytes"] != 1 || len(n.ResultTooLarge) != 2 {
		t.Errorf("result_too_large %v", n.ResultTooLarge)
	}
	b, err := json.Marshal(n)
	if err != nil || !strings.Contains(string(b), `"result_too_large":{"bytes":1,"rows":2}`) {
		t.Errorf("%s %v", b, err)
	}
	// A server with none has no field.
	r.note("s2", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy})
	if b, _ := json.Marshal(r.doc("").Servers["s2"]); strings.Contains(string(b), "result_too_large") {
		t.Errorf("%s", b)
	}
}
