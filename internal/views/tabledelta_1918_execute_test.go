package views

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1918: a following view generated while a table has a chain of deltas must
// keep answering, with the right rows, when a later snapshot holds the table
// rewritten in full with no chain (a refresh or a full backup with table
// deltas off), and again when a chain comes back. Every test here creates the
// views ONCE and moves the snapshot under them, the way a periodic refresh
// does, so a view that only works when freshly created cannot pass.

// fx1918 publishes snapshots of one table under a baselines root.
type fx1918 struct {
	t       *testing.T
	root    string
	table   string
	posType string // "" or the MySQL type of an extra column named "pos"
	cols    []baseline.Column
}

func newFx1918(t *testing.T, table, posType string) *fx1918 {
	t.Helper()
	extra := ""
	if posType != "" {
		extra = "  `pos` " + posType + " DEFAULT NULL,\n"
	}
	ddl := "CREATE TABLE `" + table + "` (\n  `id` int unsigned NOT NULL,\n  `big` bigint DEFAULT NULL,\n" +
		"  `at` datetime DEFAULT NULL,\n  `amount` decimal(10,2) DEFAULT NULL,\n  `status` varchar(32) DEFAULT NULL,\n" +
		extra + "  PRIMARY KEY (`id`)\n);\n"
	cols, err := baseline.ParseSchemaText(ddl)
	if err != nil {
		t.Fatal(err)
	}
	return &fx1918{t: t, root: t.TempDir(), table: table, posType: posType, cols: cols}
}

// row is one row of the table: every column is derived from the id and the
// status, so an expectation lists "id=status" and still checks every column.
type row1918 struct {
	id     int
	status string
}

func (f *fx1918) values(r row1918) []string {
	v := []string{
		fmt.Sprint(r.id), fmt.Sprint(int64(r.id) * 10_000_000_000),
		fmt.Sprintf("2026-01-02 03:04:%02d", r.id%60), fmt.Sprintf("%d.50", r.id), r.status,
	}
	switch {
	case f.posType == "":
	case strings.HasPrefix(f.posType, "varchar"):
		v = append(v, "not a number "+r.status)
	case f.posType == "datetime":
		v = append(v, fmt.Sprintf("2025-05-06 07:08:%02d", r.id%60))
	default:
		v = append(v, fmt.Sprint(r.id*100))
	}
	return v
}

// want renders a row the way stateOf reads it back.
func (f *fx1918) want(rows ...row1918) []string {
	out := []string{}
	for _, r := range rows {
		v := f.values(r)
		s := fmt.Sprintf("%s|%s|%s|%s|%s", v[0], v[1], v[2], v[3], v[4])
		if f.posType != "" {
			s += "|" + v[5]
		}
		out = append(out, s)
	}
	return out
}

func (f *fx1918) basePath(stamp string) string {
	return filepath.Join(f.root, stamp, "shop", f.table+".parquet")
}

// publish writes a complete snapshot holding the table with these rows, in
// this order (row number i is rows[i]).
func (f *fx1918) publish(stamp string, rows ...row1918) string {
	f.t.Helper()
	base := f.basePath(stamp)
	w, err := baseline.NewWriter(base, f.cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		f.t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow(f.values(r), make([]bool, len(f.cols))); err != nil {
			f.t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.root, stamp, baseline.SuccessMarker), nil, 0o644); err != nil {
		f.t.Fatal(err)
	}
	return base
}

// pair writes pair seq of the chain beside base: dead row numbers, current
// images, and tombstones (ids whose row an earlier pair added).
func (f *fx1918) pair(base string, seq int, dead []int64, ups []row1918, tombstones ...int) {
	f.t.Helper()
	n := len(f.cols)
	err := baseline.WriteTableDeltaPair(base, seq, f.cols, nil, dead, func(emit func([]string, []bool) error) error {
		for _, r := range ups {
			if err := emit(append(f.values(r), fmt.Sprint(r.id), baseline.TableDeltaOpUpsert), make([]bool, n+2)); err != nil {
				return err
			}
		}
		for _, id := range tombstones {
			vals, nulls := make([]string, n+2), make([]bool, n+2)
			for i := range n {
				nulls[i] = true
			}
			vals[n], vals[n+1] = fmt.Sprint(id), baseline.TableDeltaOpDelete
			if err := emit(vals, nulls); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		f.t.Fatal(err)
	}
}

// views generates the file against snapshot stamp and loads it into a fresh
// session, which the caller keeps for every later snapshot.
func (f *fx1918) views(follow FollowMode, stamp string) (*sql.DB, string, BaselineTable) {
	f.t.Helper()
	tables := []BaselineTable{{
		Schema: "shop", Table: f.table, Path: f.basePath(stamp), Rel: "shop/" + f.table + ".parquet",
		SchemaKnown: true, Decimals: []DecimalColumn{{Name: "amount", Precision: 10, Scale: 2}},
	}}
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		f.t.Fatalf("MarkTableDeltas: %v", err)
	}
	if follow == FollowPointer {
		if err := os.Symlink(stamp, filepath.Join(f.root, baseline.CurrentLinkName)); err != nil {
			f.t.Fatal(err)
		}
		tables[0].Path = filepath.Join(f.root, baseline.CurrentLinkName, "shop", f.table+".parquet")
	}
	sqlText := Generate(Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: f.root, BaselineSnapshot: time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC),
		Follow: follow, Baselines: tables,
	})
	return execViews(f.t, sqlText), sqlText, tables[0]
}

// moveTo points the views at snapshot stamp without recreating them: the
// `current` link for FollowPointer, the file's own SET VARIABLE statement
// for FollowNewest (stamp is always the newest marked snapshot here).
func (f *fx1918) moveTo(db *sql.DB, follow FollowMode, sqlText, stamp string) {
	f.t.Helper()
	if follow == FollowPointer {
		link := filepath.Join(f.root, baseline.CurrentLinkName)
		if err := os.Remove(link); err != nil {
			f.t.Fatal(err)
		}
		if err := os.Symlink(stamp, link); err != nil {
			f.t.Fatal(err)
		}
		return
	}
	i := strings.Index(sqlText, "SET VARIABLE "+newestVar)
	j := strings.Index(sqlText[i:], ";")
	if i < 0 || j < 0 {
		f.t.Fatalf("generated file carries no SET VARIABLE %s statement", newestVar)
	}
	if _, err := db.Exec(sqlText[i : i+j+1]); err != nil {
		f.t.Fatalf("re-run SET VARIABLE: %v", err)
	}
}

// stateOf reads every row of the view, every column, and the column types
// the view binds to right now.
func (f *fx1918) stateOf(db *sql.DB, view string) (rows []string, types []string, err error) {
	f.t.Helper()
	sel := `concat_ws('|', id, big, strftime("at", '%Y-%m-%d %H:%M:%S'), amount, status`
	if f.posType == "datetime" {
		sel += ", strftime(pos, '%Y-%m-%d %H:%M:%S')"
	} else if f.posType != "" {
		sel += ", pos"
	}
	r, err := db.Query("SELECT " + sel + ") FROM " + view + " ORDER BY id")
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()
	rows = []string{}
	for r.Next() {
		var s string
		if err := r.Scan(&s); err != nil {
			return nil, nil, err
		}
		rows = append(rows, s)
	}
	if err := r.Err(); err != nil {
		return nil, nil, err
	}
	d, err := db.Query("SELECT column_name || ' ' || column_type FROM (DESCRIBE SELECT * FROM " + view + ")")
	if err != nil {
		return nil, nil, err
	}
	defer d.Close()
	for d.Next() {
		var s string
		if err := d.Scan(&s); err != nil {
			return nil, nil, err
		}
		types = append(types, s)
	}
	return rows, types, d.Err()
}

func viewName1918(table string) string {
	return stateViewName("shop", table, map[string]bool{})
}

// TestFollowingDeltaView_everyShapeARefreshLeaves_1918 walks one session's
// views across seven snapshots, in both following modes: the chain the views
// were generated against, then a full rewrite with no chain (the #1918
// failure: "No files found"), a chain again, a chain of three pairs with a
// tombstone, a chain that only deletes, an empty table, and an empty table
// with a chain. Each snapshot holds different rows, so a view that kept
// reading an earlier one cannot pass, and the column types must not move.
func TestFollowingDeltaView_everyShapeARefreshLeaves_1918(t *testing.T) {
	for _, mode := range []struct {
		name   string
		follow FollowMode
	}{{"pointer", FollowPointer}, {"newest", FollowNewest}} {
		t.Run(mode.name, func(t *testing.T) {
			f := newFx1918(t, "orders", "")
			view := viewName1918("orders")
			steps := []struct {
				name  string
				write func(stamp string)
				want  []string
			}{
				{"chain, as generated", func(s string) {
					b := f.publish(s, row1918{1, "a"}, row1918{2, "b"}, row1918{3, "c"})
					f.pair(b, 0, nil, nil)
					f.pair(b, 1, []int64{0}, []row1918{{1, "changed"}, {9, "new"}})
				}, f.want(row1918{1, "changed"}, row1918{2, "b"}, row1918{3, "c"}, row1918{9, "new"})},
				{"rewritten in full, no chain", func(s string) {
					f.publish(s, row1918{1, "r1"}, row1918{2, "r2"}, row1918{4, "r4"})
				}, f.want(row1918{1, "r1"}, row1918{2, "r2"}, row1918{4, "r4"})},
				{"a chain again", func(s string) {
					b := f.publish(s, row1918{1, "x1"}, row1918{5, "x5"})
					f.pair(b, 0, nil, nil)
					f.pair(b, 1, []int64{1}, []row1918{{6, "x6"}})
				}, f.want(row1918{1, "x1"}, row1918{6, "x6"})},
				{"a chain of three pairs", func(s string) {
					b := f.publish(s, row1918{1, "a"}, row1918{2, "b"}, row1918{3, "c"})
					f.pair(b, 0, nil, nil)
					f.pair(b, 1, []int64{0}, []row1918{{1, "v2"}, {7, "seven"}})
					f.pair(b, 2, nil, []row1918{{1, "v3"}}, 7)
				}, f.want(row1918{1, "v3"}, row1918{2, "b"}, row1918{3, "c"})},
				{"a chain that only deletes", func(s string) {
					b := f.publish(s, row1918{1, "a"}, row1918{2, "b"}, row1918{3, "c"})
					f.pair(b, 0, nil, nil)
					f.pair(b, 1, []int64{0, 2}, nil)
				}, f.want(row1918{2, "b"})},
				{"an empty table, no chain", func(s string) {
					f.publish(s)
				}, f.want()},
				{"an empty table with a chain", func(s string) {
					b := f.publish(s)
					f.pair(b, 0, nil, nil)
					f.pair(b, 1, nil, []row1918{{8, "eight"}})
				}, f.want(row1918{8, "eight"})},
			}
			var db *sql.DB
			var sqlText string
			var firstTypes []string
			for i, st := range steps {
				stamp := fmt.Sprintf("2026-04-30T%02d-00-00Z", i+1)
				st.write(stamp)
				if i == 0 {
					var tbl BaselineTable
					db, sqlText, tbl = f.views(mode.follow, stamp)
					if !tbl.Delta || tbl.DeltaLegacy {
						t.Fatalf("the views were not generated against a chain: Delta=%v Legacy=%v", tbl.Delta, tbl.DeltaLegacy)
					}
				} else {
					f.moveTo(db, mode.follow, sqlText, stamp)
				}
				got, types, err := f.stateOf(db, view)
				if err != nil {
					t.Fatalf("%s: the view failed: %v", st.name, err)
				}
				if !reflect.DeepEqual(got, st.want) {
					t.Fatalf("%s: state = %v, want %v", st.name, got, st.want)
				}
				// The column set and types the view binds to must not move
				// with the shape on disk, and the decimal cast must survive.
				if i == 0 {
					firstTypes = types
					if !slices.Contains(types, "amount DECIMAL(10,2)") || len(types) != 5 {
						t.Fatalf("%s: the view's columns = %v, want the table's five with amount cast", st.name, types)
					}
				} else if !reflect.DeepEqual(types, firstTypes) {
					t.Fatalf("%s: the view's columns = %v, want %v as when the views were generated", st.name, types, firstTypes)
				}
			}
		})
	}
}

// TestFollowingPlainView_noChainEver_1918: a table that never has a chain
// keeps the plain body, and follows snapshots with the exact rows. (That it
// refuses once a chain does appear is TestFollowingStateView_refusesOnceADeltaAppears.)
func TestFollowingPlainView_noChainEver_1918(t *testing.T) {
	for _, mode := range []struct {
		name   string
		follow FollowMode
	}{{"pointer", FollowPointer}, {"newest", FollowNewest}} {
		t.Run(mode.name, func(t *testing.T) {
			f := newFx1918(t, "orders", "")
			view := viewName1918("orders")
			f.publish("2026-04-30T01-00-00Z", row1918{1, "a"}, row1918{2, "b"})
			db, sqlText, tbl := f.views(mode.follow, "2026-04-30T01-00-00Z")
			if tbl.Delta {
				t.Fatal("a table with no chain was marked")
			}
			if strings.Contains(sqlText, "file_row_number") {
				t.Fatalf("a table with no chain got the chain body:\n%s", sqlText)
			}
			if got, _, err := f.stateOf(db, view); err != nil || !reflect.DeepEqual(got, f.want(row1918{1, "a"}, row1918{2, "b"})) {
				t.Fatalf("first snapshot: %v err=%v", got, err)
			}
			f.publish("2026-04-30T02-00-00Z", row1918{3, "c"})
			f.moveTo(db, mode.follow, sqlText, "2026-04-30T02-00-00Z")
			if got, _, err := f.stateOf(db, view); err != nil || !reflect.DeepEqual(got, f.want(row1918{3, "c"})) {
				t.Fatalf("second snapshot: %v err=%v", got, err)
			}
		})
	}
}

// TestFollowingDeltaView_tableWithItsOwnPosColumn_1918: the base's columns
// now reach the read of the dead row numbers, so a table with a column of its
// own named "pos" (which a chain allows) changes that column's type there.
// The chain must still kill exactly the right rows, and the table's own pos
// values must come through untouched, before and after a full rewrite.
func TestFollowingDeltaView_tableWithItsOwnPosColumn_1918(t *testing.T) {
	for _, posType := range []string{"int", "varchar(64)", "datetime"} {
		t.Run(posType, func(t *testing.T) {
			f := newFx1918(t, "orders", posType)
			view := viewName1918("orders")
			b := f.publish("2026-04-30T01-00-00Z", row1918{1, "a"}, row1918{2, "b"}, row1918{3, "c"})
			f.pair(b, 0, nil, nil)
			f.pair(b, 1, []int64{0, 2}, []row1918{{1, "changed"}, {9, "new"}})
			db, sqlText, _ := f.views(FollowPointer, "2026-04-30T01-00-00Z")
			got, _, err := f.stateOf(db, view)
			if want := f.want(row1918{1, "changed"}, row1918{2, "b"}, row1918{9, "new"}); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("with a chain: %v err=%v, want %v", got, err, want)
			}
			f.publish("2026-04-30T02-00-00Z", row1918{4, "d"}, row1918{5, "e"})
			f.moveTo(db, FollowPointer, sqlText, "2026-04-30T02-00-00Z")
			got, _, err = f.stateOf(db, view)
			if want := f.want(row1918{4, "d"}, row1918{5, "e"}); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("rewritten with no chain: %v err=%v, want %v", got, err, want)
			}
		})
	}
}

// TestFollowingDeltaView_globMetacharacterInTheName_1918: the base's name is
// part of the pattern now, so a "[" in it must be escaped there too, or the
// pattern would not match the base and the no-chain case fails again.
func TestFollowingDeltaView_globMetacharacterInTheName_1918(t *testing.T) {
	f := newFx1918(t, "or[d]ers", "")
	view := viewName1918("or[d]ers")
	b := f.publish("2026-04-30T01-00-00Z", row1918{1, "a"}, row1918{2, "b"})
	f.pair(b, 0, nil, nil)
	f.pair(b, 1, []int64{1}, []row1918{{3, "c"}})
	db, sqlText, tbl := f.views(FollowPointer, "2026-04-30T01-00-00Z")
	if !tbl.Delta {
		t.Fatal("the chain beside or[d]ers was not found")
	}
	if got, _, err := f.stateOf(db, view); err != nil || !reflect.DeepEqual(got, f.want(row1918{1, "a"}, row1918{3, "c"})) {
		t.Fatalf("with a chain: %v err=%v", got, err)
	}
	f.publish("2026-04-30T02-00-00Z", row1918{7, "g"})
	f.moveTo(db, FollowPointer, sqlText, "2026-04-30T02-00-00Z")
	if got, _, err := f.stateOf(db, view); err != nil || !reflect.DeepEqual(got, f.want(row1918{7, "g"})) {
		t.Fatalf("rewritten with no chain: %v err=%v", got, err)
	}
}
