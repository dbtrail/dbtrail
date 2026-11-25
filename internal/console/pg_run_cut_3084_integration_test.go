//go:build integration

package console

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// carried3084 publishes tbl in the snapshot folder of at as an update that
// found no change of it does: the same file, linked, footer and all.
func (r *pgTailRig) carried3084(tbl views.BaselineTable, at time.Time) views.BaselineTable {
	r.t.Helper()
	path := filepath.Join(r.root, reconstruct.SnapshotDirName(at), "public", tbl.Table+".parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.Link(tbl.Path, path); err != nil {
		r.t.Fatal(err)
	}
	return views.BaselineTable{Schema: tbl.Schema, Table: tbl.Table, Path: path}
}

// folded3084 is the footer of a file an update wrote from the rig's first
// read: the same read, so a tail may keep its chain across it.
func (r *pgTailRig) folded3084(md map[string]string) {
	md[baseline.MetaKeySnapshotProducer] = baseline.ProducerReconstruct
	md[baseline.MetaKeyLastDumpAt] = r.at.Format(time.RFC3339)
	md[baseline.MetaKeyFoldGeneration] = "1"
}

func (r *pgTailRig) entry3084(k *tailKeeper, tbl views.BaselineTable) tailTable {
	k.mu.Lock()
	defer k.mu.Unlock()
	if e := k.tables[tailKey(tbl.Schema, tbl.Table)]; e != nil {
		return e.tail
	}
	return nil
}

// #3084 against a real index: a table nobody writes, carried by three
// snapshot updates in a row, keeps its tail, and so does the table beside it
// that the updates wrote, on a source that changed and on one that did not.
// Each first look at a new snapshot reads through both tails, with no build.
// A statement that joins them reads both tails.
func TestIntegrationPGCarriedTableKeepsItsTail_3084(t *testing.T) {
	ctx := context.Background()
	r := newPGTailRig(t)
	// quiet is a lookup table: never changed. busy points into it.
	quiet := r.table(nil, [2]string{"1", "gold"}, [2]string{"2", "silver"})
	busy := r.table(nil, [2]string{"a", "1"}, [2]string{"b", "1"})
	r.update(busy.Table, "a", "1", "2", 0x610, 20*time.Minute)
	r.checkpoint(0x610)
	if got, want := r.state(ctx, busy), []string{"a=2", "b=1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("before the updates: %v, want %v", got, want)
	}
	if got, want := r.state(ctx, quiet), []string{"1=gold", "2=silver"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("before the updates: %v, want %v", got, want)
	}
	quietTail, busyTail := r.entry3084(r.k, quiet), r.entry3084(r.k, busy)
	if quietTail == nil || busyTail == nil {
		t.Fatal("no tails before the updates")
	}
	newestID := func() uint64 {
		var id uint64
		if err := r.db.QueryRow("SELECT MAX(event_id) FROM binlog_events").Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	// update publishes a snapshot at at as an update ending at lsn-1 does:
	// quiet carried, busy written with rows, and the folder's run cut.
	update := func(at time.Time, lsn uint64, rows ...[2]string) (views.BaselineTable, views.BaselineTable) {
		t.Helper()
		q := r.carried3084(quiet, at)
		b := r.rewritten(busy, at, lsn, r.folded3084, rows...)
		writePGRunCut3084(t, filepath.Join(r.root, reconstruct.SnapshotDirName(at)), at, lsn, newestID(), "public."+quiet.Table, "public."+busy.Table)
		return q, b
	}
	look := func(when string, q, b views.BaselineTable, wantBusy []string) {
		t.Helper()
		got, err := r.k.refresh(ctx, []views.BaselineTable{q, b})
		if err != nil {
			t.Fatal(err)
		}
		for _, tbl := range []views.BaselineTable{q, b} {
			if _, ok := got[tailKey(tbl.Schema, tbl.Table)]; !ok {
				t.Fatalf("%s: the first look at %s has no tail: it is built again from nothing", when, tbl.Table)
			}
		}
		if rows := merged(t, b, got[tailKey(b.Schema, b.Table)]); !reflect.DeepEqual(rows, wantBusy) {
			t.Fatalf("%s: busy reads %v, want %v", when, rows, wantBusy)
		}
		if rows := merged(t, q, got[tailKey(q.Schema, q.Table)]); !reflect.DeepEqual(rows, []string{"1=gold", "2=silver"}) {
			t.Fatalf("%s: quiet reads %v", when, rows)
		}
		if r.entry3084(r.k, quiet) != quietTail || r.entry3084(r.k, busy) != busyTail {
			t.Fatalf("%s: a tail was replaced", when)
		}
	}

	// 1: the source quiet since the update. Nothing past the run's end, so
	// only the run's event id says the cursor reached it.
	q1, b1 := update(r.at.Add(11*time.Minute), 0x611, [2]string{"a", "2"}, [2]string{"b", "1"})
	look("the first update, a quiet source", q1, b1, []string{"a=2", "b=1"})
	// 2: still quiet. The same end as the update before.
	q2, b2 := update(r.at.Add(12*time.Minute), 0x611, [2]string{"a", "2"}, [2]string{"b", "1"})
	look("the second update, nothing changed", q2, b2, []string{"a=2", "b=1"})
	// 3: busy changed, and changes again after the update.
	// The keeper follows capture all along, as its follower does.
	r.update(busy.Table, "b", "1", "2", 0x620, 21*time.Minute)
	r.checkpoint(0x620)
	r.k.followOnce(ctx, time.Minute)
	q3, b3 := update(r.at.Add(13*time.Minute), 0x621, [2]string{"a", "2"}, [2]string{"b", "2"})
	r.update(busy.Table, "a", "2", "1", 0x630, 22*time.Minute)
	r.checkpoint(0x630)
	r.k.followOnce(ctx, time.Minute)
	look("the third update, another table changed", q3, b3, []string{"a=1", "b=2"})
}

// The same three updates, read by a statement that joins the two tables:
// fresh, through both tails, over the newest snapshot.
func TestIntegrationPGStatementJoiningACarriedTableStaysFresh_3084(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r := newPGTailRig(t)
	r.b.pgSource = true
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) { return nil, true }
	EnablePostgresTails(true)
	t.Cleanup(func() { EnablePostgresTails(false) })

	quiet := r.table(nil, [2]string{"1", "gold"}, [2]string{"2", "silver"})
	busy := r.table(nil, [2]string{"a", "1"})
	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	stmt := "SELECT b.id || '=' || g.status FROM public." + busy.Table + " b JOIN public." + quiet.Table + " g ON g.id = b.status ORDER BY b.id"
	read := func(t *testing.T) []string {
		t.Helper()
		res, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{PostgresRules: true})
		if err != nil {
			t.Fatal(err)
		}
		q.NoteRoute("copy")
		var out []string
		for _, row := range res.Rows {
			out = append(out, row[0].(string))
		}
		return out
	}
	keeper := func(t *testing.T) *tailKeeper {
		t.Helper()
		r.s.tails.mu.Lock()
		k := r.s.tails.by[bootServerID]
		r.s.tails.mu.Unlock()
		if k == nil {
			t.Fatal("no keeper for the server")
		}
		return k
	}
	settle := func(t *testing.T) {
		t.Helper()
		k := keeper(t)
		k.followOnce(context.Background(), time.Minute)
		k.settle()
		k.followOnce(context.Background(), time.Minute)
	}
	r.update(busy.Table, "a", "1", "2", 0x610, 20*time.Minute)
	r.checkpoint(0x610)
	read(t)
	settle(t)
	if got, want := read(t), []string{"a=silver"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("before the updates: %v, want %v", got, want)
	}
	k := keeper(t)
	quietTail := r.entry3084(k, quiet)
	if quietTail == nil {
		t.Fatal("no tail of the joined table")
	}
	lsn := uint64(0x611)
	for i := 1; i <= 3; i++ {
		at := r.at.Add(time.Duration(10+i) * time.Minute)
		r.carried3084(quiet, at)
		r.rewritten(busy, at, lsn, r.folded3084, [2]string{"a", "2"})
		var id uint64
		if err := r.db.QueryRow("SELECT MAX(event_id) FROM binlog_events").Scan(&id); err != nil {
			t.Fatal(err)
		}
		writePGRunCut3084(t, filepath.Join(r.root, reconstruct.SnapshotDirName(at)), at, lsn, id, "public."+quiet.Table, "public."+busy.Table)
		if got, want := read(t), []string{"a=silver"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("update %d: %v, want %v", i, got, want)
		}
		if r.entry3084(k, quiet) != quietTail {
			t.Fatalf("update %d: the joined table's tail was built again", i)
		}
	}
	// A change after the last update reaches the statement through busy's
	// tail, joined with quiet's.
	r.update(busy.Table, "a", "2", "1", 0x700, 22*time.Minute)
	r.checkpoint(0x700)
	settle(t)
	if got, want := read(t), []string{"a=gold"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after a change past the updates: %v, want %v", got, want)
	}
	if r.entry3084(k, quiet) != quietTail {
		t.Fatal("the joined table's tail was built again")
	}
}
