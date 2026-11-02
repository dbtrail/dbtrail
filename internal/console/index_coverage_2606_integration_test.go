//go:build integration

package console

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// newYoungRig is an index created by the real path (indexer.CreateIndexTables,
// which records its partition history) minutes before a snapshot taken right
// after it: a server added a moment ago (#2606). Its partitions start at the
// creation hour, after the floor of any snapshot taken in that hour.
func newYoungRig(t *testing.T) *unchangedRig {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 3, false, nil); err != nil {
		t.Fatal(err)
	}
	r := &unchangedRig{t: t, db: db, root: t.TempDir(),
		stamp:  time.Now().UTC().Truncate(time.Second),
		anchor: query.BinlogPos{File: "binlog.000007", Pos: 4200}}
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, events_indexed, last_checkpoint, server_id, capture_skips)
		VALUES (1, 'gtid', 'binlog.000009', 100, '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50', 0, UTC_TIMESTAMP(), 1, '{}')`)
	r.wm = &fakeWatermark{ago: 5 * time.Second}
	r.s = &Server{cm: newConnManager(nil, false), captureStatus: r.wm}
	r.b = &bundle{db: db, dbName: dbName, engine: query.New(db), baselineSrc: r.root}
	r.s.cm.boot = r.b
	tables := map[string]*metadata.TableMeta{}
	for i := 1; i <= 5; i++ {
		name := "t" + strconv.Itoa(i)
		tables["shop."+name] = &metadata.TableMeta{Schema: "shop", Table: name, PKColumns: []string{"id"}, Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int", ColumnType: "int"},
			{Name: "status", OrdinalPosition: 2, DataType: "varchar", ColumnType: "varchar(32)"},
		}}
	}
	r.b.resolver = metadata.NewResolverFromTables(1, tables)
	return r
}

// The box repro: a server added minutes ago, its first snapshot, a change
// after it. The tail holds the change and the unchanged lookup answers for a
// quiet table; neither says the index was rotated, since nothing was.
func TestIntegrationIndexCoverage_youngIndex_2606(t *testing.T) {
	ctx := context.Background()
	r := newYoungRig(t)
	x := newIndexTailSource(r.s, r.b).capturing(nil)
	busy, quiet := r.table(nil), r.table(nil)
	r.change(busy.Table, event.EventUpdate, "1", "late", "binlog.000009", 900, r.stamp.Add(time.Second))
	var upTo uint64
	if err := r.db.QueryRow("SELECT MAX(event_id) FROM binlog_events").Scan(&upTo); err != nil {
		t.Fatal(err)
	}
	fill := func() ([]string, string) {
		t.Helper()
		var got []string
		why, err := x.Fill(ctx, busy, upTo, func(rows []query.ResultRow) error {
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

	got, why := fill()
	if why != "" || len(got) != 1 || got[0] != "binlog.000009:900" {
		t.Fatalf("tail of a young index: rows %v, refusal %q; want the one change, no refusal", got, why)
	}
	r.wantUnchanged(quiet)

	// A drop recorded above the floor: the index may lack what the margin
	// needs, and both refuse.
	testutil.MustExec(t, r.db, "UPDATE partition_history SET dropped_below_unix = ? WHERE id = 1", time.Now().UTC().Truncate(time.Hour).Unix())
	if _, why := fill(); !strings.Contains(why, "rotated out") {
		t.Errorf("a recorded drop above the floor: refusal %q, want the index not to hold the window", why)
	}
	r.wantNot("rotated out", quiet)
	// One at or below the floor is not in the way.
	floor := copyTimeFloor(r.stamp)
	testutil.MustExec(t, r.db, "UPDATE partition_history SET dropped_below_unix = ? WHERE id = 1", floor.Unix())
	if _, why := fill(); why != "" {
		t.Errorf("a recorded drop at the floor: refusal %q, want none", why)
	}
	r.wantUnchanged(quiet)

	// An index created after the snapshot (emptied and created again): the
	// events between are not in it.
	testutil.MustExec(t, r.db, "UPDATE partition_history SET dropped_below_unix = NULL, created_unix = ? WHERE id = 1", r.stamp.Add(time.Minute).Unix())
	if _, why := fill(); !strings.Contains(why, "began after the snapshot") {
		t.Errorf("an index created after the snapshot: refusal %q, want one", why)
	}
	r.wantNot("began after the snapshot", quiet)

	// History unknown (a restored index), and no record at all (an older
	// build's index): measured by the oldest partition, as before.
	testutil.MustExec(t, r.db, "UPDATE partition_history SET created_unix = ?, history_known = 0 WHERE id = 1", r.stamp.Unix())
	if _, why := fill(); !strings.Contains(why, "rotated out") {
		t.Errorf("history unknown: refusal %q, want the old rule's", why)
	}
	testutil.MustExec(t, r.db, "DROP TABLE partition_history")
	if _, why := fill(); !strings.Contains(why, "rotated out") {
		t.Errorf("no record: refusal %q, want the old rule's", why)
	}
	r.wantNot("rotated out", quiet)
}
