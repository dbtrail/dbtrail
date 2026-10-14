//go:build integration

package console

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2369 against a real index: a table's tail starts from the window the
// refresh would fold next, follows the index by id, and is refused by what
// would make it incomplete. Each case has its own table on unchangedRig.

// tailRig is unchangedRig with a schema snapshot (a tail's key comes from
// it) and a keeper over the real index.
func newTailRig(t *testing.T) (*unchangedRig, *tailKeeper) {
	t.Helper()
	r := newUnchangedRig(t)
	tables := map[string]*metadata.TableMeta{}
	for i := 1; i <= 20; i++ {
		name := "t" + strconv.Itoa(i)
		tables["shop."+name] = &metadata.TableMeta{Schema: "shop", Table: name, PKColumns: []string{"id"}, Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int", ColumnType: "int"},
			{Name: "status", OrdinalPosition: 2, DataType: "varchar", ColumnType: "varchar(32)"},
		}}
	}
	r.b.resolver = metadata.NewResolverFromTables(1, tables)
	return r, newTailKeeper(newIndexTailSource(r.s, r.b), t.TempDir(), 1000, time.Minute, time.Hour)
}

// change indexes a row event of shop.table: id's status becomes status.
func (r *unchangedRig) change(table string, typ event.EventType, id, status, file string, pos uint64, at time.Time) {
	r.t.Helper()
	img := []byte(`{"id":` + id + `,"status":"` + status + `"}`)
	var before, after []byte
	switch typ {
	case event.EventInsert:
		after = img
	case event.EventUpdate:
		before, after = []byte(`{"id":`+id+`,"status":"before"}`), img
	case event.EventDelete:
		before = img
	}
	testutil.InsertEvent(r.t, r.db, file, pos, pos+50, at.UTC().Format("2006-01-02 15:04:05"), nil,
		"shop", table, uint8(typ), id, nil, before, after)
}

// merged reads t's published state with its tail over it.
func merged(t *testing.T, tbl views.BaselineTable, v tailView) []string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := "SELECT * FROM read_parquet('" + tbl.Path + "')"
	if v.Path != "" {
		q = baseline.TableTailStateSQL(q, "'"+v.Path+"'", v.PK, "")
	}
	rs, err := db.Query("SELECT CAST(id AS VARCHAR) || '=' || status FROM (" + q + ") ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var s string
		if err := rs.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func TestIntegrationTailSource(t *testing.T) {
	ctx := context.Background()
	r, k := newTailRig(t)
	hourAgo := time.Now().UTC().Add(-time.Hour)

	t.Run("the window after the cut, then the index as it grows", func(t *testing.T) {
		tbl := r.table(nil)                                                                               // one row: 1=new, cut at binlog.000007:4200
		r.change(tbl.Table, event.EventUpdate, "1", "old", "binlog.000007", 100, r.stamp.Add(-time.Hour)) // before the cut
		r.change(tbl.Table, event.EventUpdate, "1", "paid", "binlog.000008", 100, hourAgo)
		r.change(tbl.Table, event.EventInsert, "2", "two", "binlog.000008", 200, hourAgo)
		r.change("other", event.EventInsert, "9", "x", "binlog.000008", 300, hourAgo)
		got, err := k.refresh(ctx, []views.BaselineTable{tbl})
		if err != nil {
			t.Fatal(err)
		}
		v, ok := got[tailKey("shop", tbl.Table)]
		if !ok {
			t.Fatal("no tail")
		}
		if s := strings.Join(merged(t, tbl, v), ","); s != "1=paid,2=two" {
			t.Fatalf("merged %s", s)
		}
		first := v.Through
		// Later: a delete, and a late commit indexed after with an early time.
		r.change(tbl.Table, event.EventDelete, "2", "two", "binlog.000009", 100, time.Now().UTC())
		r.change(tbl.Table, event.EventInsert, "3", "late", "binlog.000009", 200, r.stamp.Add(time.Hour))
		got, err = k.refresh(ctx, []views.BaselineTable{tbl})
		if err != nil {
			t.Fatal(err)
		}
		v = got[tailKey("shop", tbl.Table)]
		if s := strings.Join(merged(t, tbl, v), ","); s != "1=paid,3=late" {
			t.Fatalf("merged %s", s)
		}
		if !v.Through.After(first) && !v.Through.Equal(first) {
			t.Errorf("the instant went back: %v -> %v", first, v.Through)
		}
	})

	t.Run("a capture in position mode has tails too", func(t *testing.T) {
		testutil.MustExec(t, r.db, "UPDATE stream_state SET mode = 'position', gtid_set = NULL WHERE id = 1")
		defer testutil.MustExec(t, r.db, "UPDATE stream_state SET mode = 'gtid', gtid_set = '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50' WHERE id = 1")
		tbl := r.table(nil)
		r.change(tbl.Table, event.EventUpdate, "1", "pos", "binlog.000010", 100, hourAgo)
		got, err := k.refresh(ctx, []views.BaselineTable{tbl})
		if err != nil {
			t.Fatal(err)
		}
		if s := strings.Join(merged(t, tbl, got[tailKey("shop", tbl.Table)]), ","); s != "1=pos" {
			t.Fatalf("merged %s", s)
		}
	})

	t.Run("refused: a loss in capture since the snapshot", func(t *testing.T) {
		testutil.MustExec(t, r.db, "UPDATE stream_state SET gap_lost_at = UTC_TIMESTAMP() WHERE id = 1")
		defer testutil.MustExec(t, r.db, "UPDATE stream_state SET gap_lost_at = NULL WHERE id = 1")
		tbl := r.table(nil)
		got, err := k.refresh(ctx, []views.BaselineTable{tbl})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := got[tailKey("shop", tbl.Table)]; ok {
			t.Fatal("a tail across a capture gap")
		}
	})

	t.Run("refused: a schema change after the cut", func(t *testing.T) {
		tbl := r.table(nil)
		testutil.MustExec(t, r.db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query) VALUES (UTC_TIMESTAMP(), 'binlog.000011', 100, 'shop', ?, 'ALTER TABLE', 'ALTER TABLE x ADD c int')`, tbl.Table)
		got, err := k.refresh(ctx, []views.BaselineTable{tbl})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := got[tailKey("shop", tbl.Table)]; ok {
			t.Fatal("a tail across a schema change")
		}
	})

	t.Run("refused: a table not in the schema snapshot", func(t *testing.T) {
		tbl := r.table(nil)
		tbl.Table = "nowhere"
		got, err := k.refresh(ctx, []views.BaselineTable{tbl})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := got[tailKey("shop", "nowhere")]; ok {
			t.Fatal("a tail with no key")
		}
	})
}
