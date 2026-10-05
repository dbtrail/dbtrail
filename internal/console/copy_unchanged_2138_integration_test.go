//go:build integration

package console

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2138 and the routed port's "did these tables change" check (#2085). The
// check asks the engine with a time and a position per table, per statement,
// inside the capture process, and covers the hours below its time floor with
// its own second lookup. It must not also pay for the engine's look at every
// partition.

func TestIntegrationCopyUnchanged_loadsNoPictureOfTheIndex_2138(t *testing.T) {
	r := newUnchangedRig(t)
	tables := []views.BaselineTable{r.table(nil), r.table(nil), r.table(nil)}
	before := query.PictureLoads()
	r.wantUnchanged(tables...)
	if got := query.PictureLoads() - before; got != 0 {
		t.Fatalf("a statement over %d tables loaded the picture of the index %d time(s), want 0", len(tables), got)
	}
	// And the answer is still "changed" for a change that began before the
	// floor and committed after the cut, found by the check's own search.
	late := r.table(nil)
	r.event("shop", late.Table, "binlog.000008", 200, r.stamp.Add(-5*time.Hour))
	r.wantNot("changed since its snapshot", late)
	if got := query.PictureLoads() - before; got != 0 {
		t.Fatalf("the search below the floor loaded the picture %d time(s), want 0", got)
	}
}

// TestMeasureCopyUnchanged prints what the whole check costs for one and for
// three tables never written, on an index of BINTRAIL_MEASURE_2138 events
// over 720 hourly partitions. Skipped unless the variable is set.
func TestMeasureCopyUnchanged(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("BINTRAIL_MEASURE_2138"))
	if n <= 0 {
		t.Skip("set BINTRAIL_MEASURE_2138=<events> to measure")
	}
	r := newUnchangedRig(t)
	ctx := context.Background()
	first := time.Now().UTC().Truncate(time.Hour).Add(-720 * time.Hour)
	r.partitionFrom(first)
	conn, err := r.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_log_bin = 0"); err != nil {
		t.Fatal(err)
	}
	// Every event is positioned before the snapshot's cut (binlog.000001),
	// in binlog order, in fifty other tables.
	const chunk = 100000
	span := int64(719 * 3600)
	for off := 0; off < n; off += chunk {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`INSERT /*+ SET_VAR(cte_max_recursion_depth = 200000) */ INTO binlog_events
			(binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
			WITH RECURSIVE s (i) AS (SELECT %d UNION ALL SELECT i + 1 FROM s WHERE i + 1 < %d)
			SELECT 'binlog.000001', i * 100, i * 100 + 99, TIMESTAMPADD(SECOND, FLOOR(i * %d / %d), '%s'),
			       'shop', CONCAT('busy', i %% 50), 2, i %% 100000,
			       JSON_OBJECT('id', i %% 100000, 'status', 'before'), JSON_OBJECT('id', i %% 100000, 'status', 'after')
			FROM s`, off, min(off+chunk, n), span, n, first.Format("2006-01-02 15:04:05"))); err != nil {
			t.Fatalf("load at %d: %v", off, err)
		}
	}
	if _, err := r.db.Exec("ANALYZE TABLE binlog_events"); err != nil {
		t.Fatal(err)
	}
	tables := []views.BaselineTable{r.table(nil), r.table(nil), r.table(nil)}
	for _, k := range []int{1, 3} {
		var ms []float64
		loads := query.PictureLoads()
		for range 11 {
			start := time.Now()
			if why := r.ask(tables[:k]...); why != "" {
				t.Fatalf("not vouched for: %s", why)
			}
			ms = append(ms, float64(time.Since(start).Microseconds())/1000)
		}
		first := ms[0]
		sort.Float64s(ms)
		t.Logf("whole check, %d table(s): first %.1f ms, median %.1f ms, worst %.1f ms, pictures loaded per check %.1f",
			k, first, ms[len(ms)/2], ms[len(ms)-1], float64(query.PictureLoads()-loads)/11)
	}
}
