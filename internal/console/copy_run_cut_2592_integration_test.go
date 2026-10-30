//go:build integration

package console

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2592 against a real index: a table nobody writes, last read by a full
// read three days ago, sits in a directory a refresh published ten minutes
// ago. Rotation has dropped every hour but the last two. Its tail starts at
// the refresh's cut, where its own cut would need hours the index no longer
// holds.
func TestIntegrationTailSource_staticTableStartsAtItsDirectorysRefreshCut_2592(t *testing.T) {
	ctx := context.Background()
	r, _ := newTailRig(t)
	x := newIndexTailSource(r.s, r.b).capturing(nil)
	now := time.Now().UTC()

	quiet := r.table(nil) // a full read's footer: binlog.000007:4200, read 72 h ago, no event since

	// What a refresh ten minutes ago wrote beside it: a busy table's file at
	// the run's cut, whose mark names the last event the index held then.
	r.change("busy", event.EventUpdate, "1", "paid", "binlog.000009", 450, now.Add(-15*time.Minute))
	var markID uint64
	if err := r.db.QueryRow("SELECT MAX(event_id) FROM binlog_events WHERE table_name = 'busy'").Scan(&markID); err != nil {
		t.Fatal(err)
	}
	mark := reconstruct.EventMark{ID: markID, File: "binlog.000009", End: 500}
	runAt := now.Add(-10 * time.Minute).Truncate(time.Second)
	ddl := "CREATE TABLE `busy` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	busyPath := filepath.Join(r.root, unchangedSnapshotDir, "shop", "busy.parquet")

	// After the run, the quiet table changes once.
	r.change(quiet.Table, event.EventUpdate, "1", "late", "binlog.000009", 900, now.Add(-2*time.Minute))
	var upTo uint64
	if err := r.db.QueryRow("SELECT MAX(event_id) FROM binlog_events").Scan(&upTo); err != nil {
		t.Fatal(err)
	}
	r.rotateBelow(now.Add(-2 * time.Hour).Truncate(time.Hour))

	fill := func() ([]string, string) {
		var got []string
		why, err := x.Fill(ctx, quiet, upTo, func(rows []query.ResultRow) error {
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

	// Before the refresh's file is there, the directory is a full read's:
	// the table's own cut, and the hours it needs are gone.
	if _, why := fill(); !strings.Contains(why, "rotated out") {
		t.Fatalf("without a refresh cut: %q, want the rotation refusal", why)
	}

	r.write(busyPath, ddl, map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000009", baseline.MetaKeyBinlogPos: "500",
		baseline.MetaKeySnapshotTimestamp: runAt.Format(time.RFC3339),
		baseline.MetaKeySnapshotProducer:  baseline.ProducerReconstruct,
		baseline.MetaKeyEventMark:         mark.Encode(),
		baseline.MetaKeyLockMode:          string(baseline.LockModeFTWRL),
		baseline.MetaKeyCreateTableSQL:    ddl,
	})
	r.s.copyRunCuts = copyRunCutMemo{} // the test rewrote a directory, which a published one never is

	got, why := fill()
	if why != "" {
		t.Fatalf("refused: %s", why)
	}
	if strings.Join(got, ",") != "binlog.000009:900" {
		t.Fatalf("the tail read %v, want the one change after the refresh's cut", got)
	}

	// The guards about the table's own read keep their span: the cut the
	// keeper compares is the run's, the read the capture-loss check asks
	// over is still the full read's.
	floor, why := x.Cut(quiet)
	if why != "" || floor.anchor != (query.BinlogPos{File: "binlog.000009", Pos: 500}) {
		t.Fatalf("Cut = %+v %q, want the refresh's cut", floor, why)
	}
	if c := r.s.tailCutOf(quiet); !c.sourceRead.Equal(r.stamp) {
		t.Fatalf("the table's own read moved: %v, want %v", c.sourceRead, r.stamp)
	}

	// The mark's event rotated away: no time for the cut, refused, never
	// read from the table's own older floor.
	deleteIndexedEvent(t, r, markID)
	r.s.copyRunCuts = copyRunCutMemo{}
	if _, why := fill(); !strings.Contains(why, "was cut at") {
		t.Fatalf("with the mark's event gone: %q", why)
	}
}

func deleteIndexedEvent(t *testing.T, r *unchangedRig, id uint64) {
	t.Helper()
	if _, err := r.db.Exec("DELETE FROM binlog_events WHERE event_id = ?", id); err != nil {
		t.Fatal(err)
	}
}
