//go:build integration

package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2756 against a real index: a PostgreSQL table's tail, driven by the same
// keeper a MySQL table's is, starts at its snapshot's LSN floor, follows the
// index up to the capture's saved position and no further, and is refused by
// what would make it wrong. Each case has its own table.

// pgTailRig is an index a PostgreSQL stream wrote (real partitions, one
// schema snapshot per table, a saved position), a snapshot folder taken ten
// minutes into the current hour, and a keeper over pgTailSource.
type pgTailRig struct {
	t      *testing.T
	db     *sql.DB
	s      *Server
	b      *bundle
	k      *tailKeeper
	root   string
	base   time.Time // the current hour
	at     time.Time // the snapshot's time
	tables atomic.Int32
}

const pgTailFloor = 0x500

func newPGTailRig(t *testing.T) *pgTailRig {
	t.Helper()
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Hour)
	r := &pgTailRig{t: t, db: db, root: t.TempDir(), base: base, at: base.Add(10 * time.Minute)}
	r.s = &Server{cm: newConnManager(nil, false), captureStatus: &fakeWatermark{ago: 5 * time.Second}}
	r.b = &bundle{db: db, dbName: dbName, engine: query.New(db), baselineSrc: r.root}
	r.s.cm.boot = r.b
	r.k = newTailKeeper(newPGTailSource(r.s, r.b).capturing(nil), t.TempDir(), tailBudget{Keys: 1000}, time.Minute, time.Hour)
	return r
}

func pgLSN(lsn uint64) string { return fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn)) }

// position is the capture's saved position alone: the commit LSN of the
// last whole transaction, as a save writes it when no transaction ended
// since the one before.
func (r *pgTailRig) position(lsn uint64) {
	r.t.Helper()
	testutil.MustExec(r.t, r.db, fmt.Sprintf(`INSERT INTO stream_state (id, mode, binlog_file, binlog_position, flavor, events_indexed, last_checkpoint, server_id)
		VALUES (1, 'gtid', '%s', %d, 'postgres', 1, UTC_TIMESTAMP(), 1)
		ON DUPLICATE KEY UPDATE binlog_file = VALUES(binlog_file), binlog_position = VALUES(binlog_position)`, pgLSN(lsn), lsn))
}

// checkpoint is a save after a transaction ended: the position, and the
// newest event at or before it, as the capture records them.
func (r *pgTailRig) checkpoint(lsn uint64) {
	r.t.Helper()
	r.position(lsn)
	testutil.MustExec(r.t, r.db, fmt.Sprintf(`UPDATE stream_state SET pg_checkpoint_event_id =
		(SELECT MAX(event_id) FROM binlog_events WHERE start_pos <= %d) WHERE id = 1`, lsn))
}

// shape records table's shape as the stream saw it at lsn: id (the key) and
// status, and whatever extra names.
func (r *pgTailRig) shape(table string, lsn uint64, extra ...string) {
	r.t.Helper()
	oid, _ := strconv.Atoi(table[1:])
	rel := &metadata.PGRelationSchema{OID: uint32(17000 + oid), Schema: "public", Table: table, SeenLSN: lsn, SeenAt: r.base,
		Columns: []metadata.PGRelationColumn{
			{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 25, TypeMod: -1},
			{Name: "status", Ordinal: 2, TypeOID: 25, TypeMod: -1},
		}}
	for i, c := range extra {
		rel.Columns = append(rel.Columns, metadata.PGRelationColumn{Name: c, Ordinal: 3 + i, TypeOID: 25, TypeMod: -1})
	}
	if _, err := metadata.WritePGSnapshot(context.Background(), r.db, rel); err != nil {
		r.t.Fatalf("WritePGSnapshot: %v", err)
	}
}

// table writes a table file of rows (id, status) in the snapshot's folder,
// its footer changed by edit, records the table's shape, and returns it as
// the views name it.
func (r *pgTailRig) table(edit func(md map[string]string), rows ...[2]string) views.BaselineTable {
	r.t.Helper()
	n := int(r.tables.Add(1))
	name := "t" + strconv.Itoa(n)
	rel, err := baseline.PGRelation{OID: uint32(17000 + n), Columns: []baseline.PGColumn{
		{Name: "id", TypeOID: 25, TypeMod: -1, Type: "text", NotNull: true, Ordinal: 1},
		{Name: "status", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 2},
	}, PrimaryKey: []string{"id"}}.Encode()
	if err != nil {
		r.t.Fatal(err)
	}
	stamp := r.at.Format(time.RFC3339)
	md := map[string]string{
		baseline.MetaKeySnapshotTimestamp: stamp,
		baseline.MetaKeyLastDumpAt:        stamp,
		baseline.MetaKeyLSN:               strconv.FormatUint(pgTailFloor, 10),
		baseline.MetaKeyPGDeltaSince:      r.at.Add(-time.Minute).Format(time.RFC3339),
		baseline.MetaKeyPGRelation:        rel,
		baseline.MetaKeyRenderGUCs:        baseline.RenderGUCsPinned,
		baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
	}
	if edit != nil {
		edit(md)
	}
	path := filepath.Join(r.root, reconstruct.SnapshotDirName(r.at), "public", name+".parquet")
	w, err := baseline.NewWriter(path, []baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		r.t.Fatal(err)
	}
	for _, row := range rows {
		if err := w.WriteRow([]string{row[0], row[1]}, []bool{false, false}); err != nil {
			r.t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		r.t.Fatal(err)
	}
	r.shape(name, 0x300)
	return views.BaselineTable{Schema: "public", Table: name, Path: path}
}

// change indexes one row of the transaction committed at lsn, d into the
// hour: id's status becomes status. Its values are text, as PostgreSQL
// capture writes them.
func (r *pgTailRig) change(table string, typ event.EventType, id, status string, lsn uint64, d time.Duration) {
	r.t.Helper()
	img, _ := json.Marshal(map[string]string{"id": id, "status": status})
	old, _ := json.Marshal(map[string]string{"id": id, "status": "before"})
	var before, after []byte
	switch typ {
	case event.EventInsert:
		after = img
	case event.EventUpdate:
		before, after = old, img
	case event.EventDelete:
		before = img
	}
	testutil.InsertEvent(r.t, r.db, pgLSN(lsn), lsn, lsn, r.base.Add(d).Format("2006-01-02 15:04:05"), nil,
		"public", table, uint8(typ), id, nil, before, after)
}

// update indexes an UPDATE of id from one status to another, committed at
// lsn: its before-image is the row the change before it left.
func (r *pgTailRig) update(table, id, from, to string, lsn uint64, d time.Duration) {
	r.t.Helper()
	before, _ := json.Marshal(map[string]string{"id": id, "status": from})
	after, _ := json.Marshal(map[string]string{"id": id, "status": to})
	testutil.InsertEvent(r.t, r.db, pgLSN(lsn), lsn, lsn, r.base.Add(d).Format("2006-01-02 15:04:05"), nil,
		"public", table, uint8(event.EventUpdate), id, nil, before, after)
}

// pair writes a table delta of PostgreSQL's own beside tbl's file, as a
// snapshot update does: each change is a key and its new status, "" for a
// deleted key. cut is the LSN the delta's window ends at, recorded in its
// footer as an update records it; edit changes that footer.
func (r *pgTailRig) pair(tbl views.BaselineTable, changes map[string]string, cut uint64, edit func(md map[string]string)) {
	r.t.Helper()
	stamp := r.at.Format(time.RFC3339)
	md := map[string]string{
		baseline.MetaKeySnapshotTimestamp: stamp,
		baseline.MetaKeyLSN:               strconv.FormatUint(cut, 10),
		baseline.MetaKeyPGDeltaSince:      r.at.Add(-time.Minute).Format(time.RFC3339),
		baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
	}
	if edit != nil {
		edit(md)
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		r.t.Fatal(err)
	}
	defer duck.Close()
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	at, err := pgdelta.BasePositions(context.Background(), duck, tbl.Path, []string{"id"}, func(k string) bool { _, ok := changes[k]; return ok })
	if err != nil {
		r.t.Fatal(err)
	}
	_, _, _, err = pgdelta.WritePair(pgdelta.PairInput{
		BasePath: tbl.Path, Seq: 0, Base: "base-of-" + tbl.Table, Run: "run-0",
		Columns: []string{"id", "status"}, KeyColumns: []string{"id"}, Positions: at, Metadata: md,
		Changes: func(yield func(pgdelta.Change) error) error {
			for _, k := range keys {
				c := pgdelta.Change{Key: k, Delete: changes[k] == ""}
				if !c.Delete {
					c.Values, c.Nulls = []string{k, changes[k]}, []bool{false, false}
				}
				if err := yield(c); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		r.t.Fatalf("WritePair: %v", err)
	}
}

// state is tbl as a statement reads it: its tail over its file, or "no tail"
// when it has none.
func (r *pgTailRig) state(ctx context.Context, tbl views.BaselineTable) []string {
	r.t.Helper()
	got, err := settledRefresh(r.t, ctx, r.k, []views.BaselineTable{tbl})
	if err != nil {
		r.t.Fatal(err)
	}
	v, ok := got[tailKey(tbl.Schema, tbl.Table)]
	if !ok {
		return []string{"no tail"}
	}
	return merged(r.t, tbl, v)
}

func TestIntegrationPGTailSource(t *testing.T) {
	ctx := context.Background()
	r := newPGTailRig(t)
	noTail := []string{"no tail"}

	t.Run("no saved position, no tail", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"})
		r.change(tbl.Table, event.EventUpdate, "1", "paid", 0x600, 20*time.Minute)
		if m, err := r.k.src.Newest(ctx); err != nil || m.ID != 0 {
			t.Fatalf("Newest with no saved position: %+v, %v", m, err)
		}
		if got := r.state(ctx, tbl); !reflect.DeepEqual(got, noTail) {
			t.Fatalf("state %v, want no tail", got)
		}
	})

	t.Run("the window from the floor, then the index as it grows, never past the saved position", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"}, [2]string{"2", "two"})
		r.change(tbl.Table, event.EventUpdate, "1", "old", 0x400, 5*time.Minute) // before the floor
		r.change(tbl.Table, event.EventUpdate, "1", "paid", 0x610, 20*time.Minute)
		r.change(tbl.Table, event.EventInsert, "3", "three", 0x620, 21*time.Minute)
		r.change("t999", event.EventInsert, "9", "x", 0x630, 21*time.Minute)
		r.checkpoint(0x630)
		if got, want := r.state(ctx, tbl), []string{"1=paid", "2=two", "3=three"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
		m, err := r.k.src.Newest(ctx)
		if err != nil || m.Pos != reconstruct.PGTailPos(0x630) {
			t.Fatalf("Newest: %+v, %v; want the event at 630, spelled for the keeper", m, err)
		}
		if ok, err := r.k.src.Holds(ctx, m); err != nil || !ok {
			t.Fatalf("Holds its own mark: %v, %v", ok, err)
		}

		// Rows of a transaction the capture has not saved its position
		// after: visible in the index, not in the tail.
		r.change(tbl.Table, event.EventDelete, "2", "two", 0x700, 22*time.Minute)
		if got, want := r.state(ctx, tbl), []string{"1=paid", "2=two", "3=three"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("before the position is saved: state %v, want %v", got, want)
		}
		r.checkpoint(0x700)
		if got, want := r.state(ctx, tbl), []string{"1=paid", "3=three"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("once it is saved: state %v, want %v", got, want)
		}
	})

	t.Run("what a crash left behind is not read until it comes again", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"})
		// Two transactions were visible and not saved when capture stopped:
		// T1 at 810 and the first row of T2 at 820.
		r.change(tbl.Table, event.EventUpdate, "1", "T1", 0x810, 23*time.Minute)
		r.change(tbl.Table, event.EventInsert, "5", "T2 first row", 0x820, 23*time.Minute)
		// It starts again: T1 comes a second time, and its position is saved.
		r.change(tbl.Table, event.EventUpdate, "1", "T1", 0x810, 23*time.Minute)
		r.checkpoint(0x810)
		if got, want := r.state(ctx, tbl), []string{"1=T1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("with T2 left behind below T1's second delivery: state %v, want %v", got, want)
		}
		// T2 comes again, whole.
		r.change(tbl.Table, event.EventInsert, "5", "T2 first row", 0x820, 23*time.Minute)
		r.change(tbl.Table, event.EventInsert, "6", "T2 second row", 0x820, 23*time.Minute)
		r.checkpoint(0x820)
		if got, want := r.state(ctx, tbl), []string{"1=T1", "5=T2 first row", "6=T2 second row"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("once T2 is delivered whole: state %v, want %v", got, want)
		}
	})

	t.Run("the saved transaction arriving a second time is not read until it is whole", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"})
		// One transaction inserts a row and then changes it.
		r.change(tbl.Table, event.EventInsert, "7", "first", 0x840, 23*time.Minute)
		r.update(tbl.Table, "7", "first", "last", 0x840, 23*time.Minute)
		r.checkpoint(0x840)
		if got, want := r.state(ctx, tbl), []string{"1=one", "7=last"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
		// The capture starts again and PostgreSQL sends that transaction a
		// second time: its first row is in, under the saved position's own
		// LSN, and the position is saved again with no transaction ended.
		r.change(tbl.Table, event.EventInsert, "7", "first", 0x840, 23*time.Minute)
		r.position(0x840)
		if got, want := r.state(ctx, tbl), []string{"1=one", "7=last"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("with the second delivery half in: state %v, want %v (the row was never committed as it first was)", got, want)
		}
		r.update(tbl.Table, "7", "first", "last", 0x840, 23*time.Minute)
		r.checkpoint(0x840)
		if got, want := r.state(ctx, tbl), []string{"1=one", "7=last"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("with the second delivery whole: state %v, want %v", got, want)
		}
		// And the tail goes on after it.
		r.update(tbl.Table, "7", "last", "later", 0x848, 23*time.Minute)
		r.checkpoint(0x848)
		if got, want := r.state(ctx, tbl), []string{"1=one", "7=later"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("after the second delivery: state %v, want %v", got, want)
		}
	})

	t.Run("two transactions arriving a second time do not end the server's other tails", func(t *testing.T) {
		a := r.table(nil, [2]string{"1", "one"})
		b := r.table(nil, [2]string{"1", "one"})
		both := []views.BaselineTable{a, b}
		r.change(b.Table, event.EventUpdate, "1", "B", 0x850, 23*time.Minute)
		r.checkpoint(0x850)
		got, err := settledRefresh(t, ctx, r.k, both)
		if err != nil {
			t.Fatal(err)
		}
		before, ok := got[tailKey(b.Schema, b.Table)]
		if !ok || len(before.Paths) == 0 {
			t.Fatalf("the other table has no tail: %v", got)
		}
		// sameTail: the other table's tail is the one it had. Its first file
		// is the same file, where tails built again write new ones.
		sameTail := func(when string, want []string) {
			t.Helper()
			got, err := settledRefresh(t, ctx, r.k, both)
			if err != nil {
				t.Fatal(err)
			}
			va, ok := got[tailKey(a.Schema, a.Table)]
			if !ok {
				t.Fatalf("%s: the table has no tail", when)
			}
			if state := merged(t, a, va); !reflect.DeepEqual(state, want) {
				t.Fatalf("%s: state %v, want %v", when, state, want)
			}
			after, ok := got[tailKey(b.Schema, b.Table)]
			if !ok || len(after.Paths) == 0 || after.Paths[0] != before.Paths[0] {
				t.Fatalf("%s: the other table's tail was built again: %v, was %v", when, after.Paths, before.Paths)
			}
		}
		// Ta and Tb were visible and not saved; both come again, and one
		// read meets all four.
		for range 2 {
			r.change(a.Table, event.EventUpdate, "1", "Ta", 0x860, 23*time.Minute)
			r.change(a.Table, event.EventInsert, "8", "Tb", 0x870, 23*time.Minute)
		}
		r.checkpoint(0x870)
		sameTail("both deliveries in one read", []string{"1=Ta", "8=Tb"})

		// Tc and Td are read, whole. Then they come again, in a later read:
		// Tc's second delivery is below what was already handed.
		r.change(a.Table, event.EventInsert, "9", "Tc", 0x880, 23*time.Minute)
		r.update(a.Table, "8", "Tb", "Td", 0x890, 23*time.Minute)
		r.checkpoint(0x890)
		sameTail("the first delivery", []string{"1=Ta", "8=Td", "9=Tc"})
		r.change(a.Table, event.EventInsert, "9", "Tc", 0x880, 23*time.Minute)
		r.update(a.Table, "8", "Tb", "Td", 0x890, 23*time.Minute)
		r.checkpoint(0x890)
		sameTail("the second delivery, in a later read", []string{"1=Ta", "8=Td", "9=Tc"})
	})

	t.Run("a long run of rows left behind is passed once, not read at every refresh", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"})
		r.k.src.(*pgTailSource).pageSize = 3
		defer func() { r.k.src.(*pgTailSource).pageSize = tailPageSize }()
		r.change(tbl.Table, event.EventUpdate, "1", "seen", 0x8a0, 23*time.Minute)
		r.checkpoint(0x8a0)
		if got, want := r.state(ctx, tbl), []string{"1=seen"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
		// A large transaction at 8c0 was visible in part when the capture
		// stopped: ten rows, more than three pages. Then the transaction at
		// the saved position comes again and its position is saved.
		for i := range 10 {
			r.change(tbl.Table, event.EventInsert, strconv.Itoa(100+i), "partial", 0x8c0, 23*time.Minute)
		}
		r.change(tbl.Table, event.EventUpdate, "1", "seen", 0x8a0, 23*time.Minute)
		r.checkpoint(0x8a0)
		if got, want := r.state(ctx, tbl), []string{"1=seen"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("with a transaction left half visible: state %v, want %v", got, want)
		}
		// The source goes on past it, and the large transaction arrives
		// whole.
		for i := range 10 {
			r.change(tbl.Table, event.EventInsert, strconv.Itoa(100+i), "whole", 0x8c0, 23*time.Minute)
		}
		r.change(tbl.Table, event.EventInsert, "200", "after", 0x8d0, 23*time.Minute)
		r.checkpoint(0x8d0)
		got := r.state(ctx, tbl)
		if len(got) != 12 || got[0] != "1=seen" || got[1] != "100=whole" || got[10] != "109=whole" || got[11] != "200=after" {
			t.Fatalf("once it arrived whole: state %v", got)
		}
	})

	// Where the read of the index starts: at the snapshot's own bound of
	// time, not before it. Read with the floor as well, the engine starts up
	// to two hours earlier and drops by its position every change of the
	// table in between: 2,000,000 of them took 8 s to drop where the 500 of
	// the window take 10 ms, and a statement waits 3 s for its tail.
	//
	// The marker is a row no capture writes: before the bound by its time,
	// past the floor by its position. The bound says there is none such, so
	// a read that returns it started before the bound.
	t.Run("the index is read from the snapshot's bound of time, not before", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"})
		// The bound is a minute before the snapshot (10 minutes into the
		// hour): 5 minutes in is before it, and inside the two hours.
		r.change(tbl.Table, event.EventUpdate, "1", "marker", 0x910, 5*time.Minute)
		r.change(tbl.Table, event.EventInsert, "2", "two", 0x920, 24*time.Minute)
		r.checkpoint(0x920)
		// The keeper's cursor goes past both before this table is first
		// asked for, so its tail is built by the read of the window (Fill)
		// and not handed the rows as they arrive, by their ids.
		other := r.table(nil, [2]string{"7", "seven"})
		r.state(ctx, other)
		if got, want := r.state(ctx, tbl), []string{"1=one", "2=two"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v: the read started before the snapshot's bound", got, want)
		}
	})

	// And the floor still holds within the bound: a change after the bound
	// by its time and below the floor by its position is in the snapshot.
	t.Run("a change after the bound and below the floor is not in the tail", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"})
		r.change(tbl.Table, event.EventUpdate, "1", "in the snapshot", pgTailFloor-1, 9*time.Minute+30*time.Second)
		r.change(tbl.Table, event.EventInsert, "2", "two", 0x930, 24*time.Minute)
		r.checkpoint(0x930)
		other := r.table(nil, [2]string{"7", "seven"})
		r.state(ctx, other)
		if got, want := r.state(ctx, tbl), []string{"1=one", "2=two"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	// A table nothing has written to since capture began has no shape in
	// the index. It has a tail all the same, an empty one, from the shape
	// its file records: without it every statement that joins it would be
	// answered from the snapshot alone.
	t.Run("a table with no shape in the index has an empty tail, not none", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"}, [2]string{"2", "two"})
		testutil.MustExec(t, r.db, "DELETE FROM schema_snapshots WHERE table_name = '"+tbl.Table+"'")
		other := r.table(nil, [2]string{"7", "seven"})
		r.change(other.Table, event.EventUpdate, "7", "SEVEN", 0x940, 24*time.Minute)
		r.checkpoint(0x940)
		if got, want := r.state(ctx, tbl), []string{"1=one", "2=two"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v (the file's rows under an empty tail)", got, want)
		}
		// And when a change of it arrives, with the shape the stream gives
		// it then, the tail takes it.
		r.shape(tbl.Table, 0x950)
		r.change(tbl.Table, event.EventUpdate, "1", "ONE", 0x960, 25*time.Minute)
		r.checkpoint(0x960)
		if got, want := r.state(ctx, tbl), []string{"1=ONE", "2=two"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("after its first change: state %v, want %v", got, want)
		}
	})

	t.Run("a table delta beside the file: the tail starts at the delta's cut", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"}, [2]string{"2", "two"})
		// A snapshot update published the file with what changed since it
		// was written: row 1 changed, row 2 gone.
		r.change(tbl.Table, event.EventUpdate, "1", "ONE", 0x8e0, 24*time.Minute)
		r.change(tbl.Table, event.EventDelete, "2", "two", 0x8e8, 24*time.Minute)
		// The delta's footer records where the NEXT change starts, as a
		// table file's does: the floor is inclusive, so one past its last.
		r.pair(tbl, map[string]string{"1": "ONE", "2": ""}, 0x8e9, nil)
		// And one more change after that update.
		r.change(tbl.Table, event.EventInsert, "3", "three", 0x900, 24*time.Minute)
		r.checkpoint(0x900)
		// The tail holds what came after the delta's cut and nothing the
		// delta holds: state lays it over the FILE alone, so rows 1 and 2
		// read as the file has them. (The view reads the delta between.)
		if got, want := r.state(ctx, tbl), []string{"1=one", "2=two", "3=three"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v: the tail holds changes the delta already holds, or misses the one after it", got, want)
		}
	})

	t.Run("a table delta whose cut cannot be read: no tail, never the file's floor instead", func(t *testing.T) {
		for name, write := range map[string]func(tbl views.BaselineTable){
			"its footer records no LSN": func(tbl views.BaselineTable) {
				r.pair(tbl, map[string]string{"1": "ONE"}, 0x8e9, func(md map[string]string) { delete(md, baseline.MetaKeyLSN) })
			},
			"it ends before its file was written": func(tbl views.BaselineTable) {
				r.pair(tbl, map[string]string{"1": "ONE"}, pgTailFloor-1, nil)
			},
			"it was published over a loss of captured changes": func(tbl views.BaselineTable) {
				r.pair(tbl, map[string]string{"1": "ONE"}, 0x8e9, func(md map[string]string) { md[baseline.MetaKeyCaptureGap] = "a gap" })
			},
			"one file of its two": func(tbl views.BaselineTable) {
				_, rows, err := pgdelta.Paths(tbl.Path, 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(rows, []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			"a file that is not a delta's, under its name": func(tbl views.BaselineTable) {
				dead, rows, err := pgdelta.Paths(tbl.Path, 0)
				if err != nil {
					t.Fatal(err)
				}
				for _, f := range []string{dead, rows} {
					if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
						t.Fatal(err)
					}
				}
			},
		} {
			tbl := r.table(nil, [2]string{"1", "one"})
			r.change(tbl.Table, event.EventUpdate, "1", "ONE", 0x8e0, 24*time.Minute)
			write(tbl)
			r.change(tbl.Table, event.EventUpdate, "1", "paid", 0x900, 24*time.Minute)
			r.checkpoint(0x900)
			if got := r.state(ctx, tbl); !reflect.DeepEqual(got, noTail) {
				t.Errorf("%s: state %v, want no tail", name, got)
			}
		}
	})

	t.Run("a snapshot with no LSN, or no bound of its own time: no tail or a tail from the margin", func(t *testing.T) {
		none := r.table(func(md map[string]string) { delete(md, baseline.MetaKeyLSN) }, [2]string{"1", "one"})
		r.change(none.Table, event.EventUpdate, "1", "paid", 0x910, 24*time.Minute)
		r.checkpoint(0x910)
		if got := r.state(ctx, none); !reflect.DeepEqual(got, noTail) {
			t.Fatalf("no LSN: state %v, want no tail", got)
		}
		// A full read's file records no bound: its reader starts a minute
		// before the snapshot.
		margin := r.table(func(md map[string]string) { delete(md, baseline.MetaKeyPGDeltaSince) }, [2]string{"1", "one"})
		r.change(margin.Table, event.EventUpdate, "1", "paid", 0x920, 24*time.Minute)
		r.checkpoint(0x920)
		if got, want := r.state(ctx, margin), []string{"1=paid"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("no recorded bound: state %v, want %v", got, want)
		}
	})

	t.Run("the table's shape changes after its floor: its tail ends", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"})
		r.change(tbl.Table, event.EventUpdate, "1", "paid", 0xa00, 25*time.Minute)
		r.checkpoint(0xa00)
		if got, want := r.state(ctx, tbl), []string{"1=paid"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
		r.shape(tbl.Table, 0xa10, "note")
		if got := r.state(ctx, tbl); !reflect.DeepEqual(got, noTail) {
			t.Fatalf("after a column was added at the source: state %v, want no tail", got)
		}
	})

	// Last: a loss on record is the whole server's.
	t.Run("capture lost events since the snapshot: no table has a tail", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"})
		r.change(tbl.Table, event.EventUpdate, "1", "paid", 0xb00, 26*time.Minute)
		r.checkpoint(0xb00)
		if got, want := r.state(ctx, tbl), []string{"1=paid"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
		testutil.MustExec(t, r.db, "UPDATE stream_state SET gap_lost_at = UTC_TIMESTAMP() + INTERVAL 1 HOUR WHERE id = 1")
		if got := r.state(ctx, tbl); !reflect.DeepEqual(got, noTail) {
			t.Fatalf("after a loss on record: state %v, want no tail", got)
		}
	})
}
