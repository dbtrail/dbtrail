//go:build integration

package console

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// A refresh ten minutes ago ended every delta chain for age and rewrote
// every table in full at its cut, binlog.000009:500. Two of them had no
// change since their last full read, three days ago: t1 nobody writes (no
// last event on record) and t2 was written once, an hour after that read
// (its last event carried forward). Rotation has since dropped every hour
// but the last two, that read's and that event's with them. Both tables'
// lookups start at the run's mark, as a table carried below the run's cut
// does (#2592), and both have a tail and are vouched for.
func TestIntegrationTailSource_tableRewrittenAtTheRunsCutWithoutChanges(t *testing.T) {
	ctx := context.Background()
	r, _ := newTailRig(t)
	x := newIndexTailSource(r.s, r.b).capturing(nil)
	now := time.Now().UTC()

	// t2's one change, long before the run.
	r.change("t2", event.EventUpdate, "1", "once", "binlog.000008", 100, r.stamp.Add(time.Hour))
	var t2Last uint64
	if err := r.db.QueryRow("SELECT MAX(event_id) FROM binlog_events WHERE table_name = 't2'").Scan(&t2Last); err != nil {
		t.Fatal(err)
	}
	// The run's mark: the last event the index held when it started.
	r.change("busy", event.EventUpdate, "1", "paid", "binlog.000009", 450, now.Add(-15*time.Minute))
	var markID uint64
	if err := r.db.QueryRow("SELECT MAX(event_id) FROM binlog_events WHERE table_name = 'busy'").Scan(&markID); err != nil {
		t.Fatal(err)
	}
	mark := reconstruct.EventMark{ID: markID, File: "binlog.000009", End: 500}
	runAt := now.Add(-10 * time.Minute).Truncate(time.Second)
	dir := filepath.Join(r.root, reconstruct.SnapshotDirName(runAt))
	ddlOf := func(name string) string {
		return "CREATE TABLE `" + name + "` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	}
	// What the run wrote for each table: its cut, its mark, its stamp, and
	// the table's last full read as the source read.
	runFile := func(name string, lastEventID uint64) views.BaselineTable {
		tb := views.BaselineTable{Schema: "shop", Table: name, Path: filepath.Join(dir, "shop", name+".parquet")}
		md := map[string]string{
			baseline.MetaKeyBinlogFile: "binlog.000009", baseline.MetaKeyBinlogPos: "500",
			baseline.MetaKeyFoldCut:           "binlog.000009:500",
			baseline.MetaKeySnapshotTimestamp: runAt.Format(time.RFC3339),
			baseline.MetaKeyLastDumpAt:        r.stamp.Format(time.RFC3339), baseline.MetaKeyFoldGeneration: "3",
			baseline.MetaKeySnapshotProducer: baseline.ProducerReconstruct,
			baseline.MetaKeyEventMark:        mark.Encode(),
			baseline.MetaKeyLockMode:         string(baseline.LockModeFTWRL),
			baseline.MetaKeyCreateTableSQL:   ddlOf(name),
		}
		if lastEventID > 0 {
			md[baseline.MetaKeyLastEventID] = strconv.FormatUint(lastEventID, 10)
		}
		r.write(tb.Path, ddlOf(name), md)
		return tb
	}
	static, rare := runFile("t1", 0), runFile("t2", t2Last)
	r.write(filepath.Join(dir, "shop", "busy.parquet"), ddlOf("busy"), map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000009", baseline.MetaKeyBinlogPos: "500",
		baseline.MetaKeyFoldCut:           "binlog.000009:500",
		baseline.MetaKeySnapshotTimestamp: runAt.Format(time.RFC3339),
		baseline.MetaKeyLastDumpAt:        r.stamp.Format(time.RFC3339), baseline.MetaKeyFoldGeneration: "3",
		baseline.MetaKeySnapshotProducer: baseline.ProducerReconstruct,
		baseline.MetaKeyEventMark:        mark.Encode(),
		baseline.MetaKeyLastEventID:      strconv.FormatUint(markID, 10),
		baseline.MetaKeyLockMode:         string(baseline.LockModeFTWRL),
		baseline.MetaKeyCreateTableSQL:   ddlOf("busy"),
	})
	if err := os.WriteFile(filepath.Join(dir, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r.rotateBelow(now.Add(-2 * time.Hour).Truncate(time.Hour))

	// Nothing changed since the run: both are vouched for.
	r.wantUnchanged(static)
	r.wantUnchanged(rare)
	r.wantUnchanged(static, rare)

	// After the run, each changes once; the tails read exactly that.
	r.change("t1", event.EventUpdate, "1", "late", "binlog.000009", 900, now.Add(-2*time.Minute))
	r.change("t2", event.EventUpdate, "1", "late", "binlog.000009", 1000, now.Add(-time.Minute))
	var upTo uint64
	if err := r.db.QueryRow("SELECT MAX(event_id) FROM binlog_events").Scan(&upTo); err != nil {
		t.Fatal(err)
	}
	fill := func(tb views.BaselineTable) ([]string, string) {
		t.Helper()
		var got []string
		why, err := x.Fill(ctx, tb, upTo, func(rows []query.ResultRow) error {
			for _, row := range rows {
				got = append(got, row.BinlogFile+":"+strconv.FormatUint(uint64(row.StartPos), 10))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return got, why
	}
	for _, c := range []struct {
		tb   views.BaselineTable
		want string
	}{{static, "binlog.000009:900"}, {rare, "binlog.000009:1000"}} {
		got, why := fill(c.tb)
		if why != "" {
			t.Fatalf("%s: refused: %s", c.tb.Table, why)
		}
		if strings.Join(got, ",") != c.want {
			t.Fatalf("%s: the tail read %v, want %s", c.tb.Table, got, c.want)
		}
		// The guards about the table's own read keep their span.
		if cut := r.s.tailLookupCut(c.tb); !cut.sourceRead.Equal(r.stamp) || !cut.sinceMark {
			t.Fatalf("%s: lookup cut %+v, want the run's mark and the table's own read %v", c.tb.Table, cut, r.stamp)
		}
	}
	r.wantNot("changed since its snapshot", static)
	r.wantNot("changed since its snapshot", rare)

	// The run's mark rotated away or naming another event still refuses:
	// TestIntegrationTailSource_staticTableStartsAtItsDirectorysRefreshCut_2592
	// pins both on the same lookup.
}
