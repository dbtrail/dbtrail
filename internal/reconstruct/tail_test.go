package reconstruct

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2369: a table's tail laid over its published state. Every case reads the
// view through DuckDB, as the SQL port does, because the seam that can go
// wrong is between the tail file and the SQL that merges it: a silent
// duplicate, or a deleted row coming back.

const tailOrdersDDL = "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"

var tailOrdersPK = []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "bigint", ColumnType: "bigint unsigned"}}

// writeTailBase writes a table file with ddl's columns and rows (each a
// column -> text map), and returns its path.
func writeTailBase(t *testing.T, dir, name, ddl string, rows ...map[string]string) string {
	t.Helper()
	cols, err := baseline.ParseSchemaText(ddl)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: ddl}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		vals, nulls := make([]string, len(cols)), make([]bool, len(cols))
		for i, c := range cols {
			v, ok := r[c.Name]
			vals[i], nulls[i] = v, !ok
		}
		if err := w.WriteRow(vals, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// tailEvent is a change of orders as the index returns it, decoded: numbers
// as json.Number, in binary log order by pos.
func tailEvent(id uint64, typ event.EventType, key string, status string, pos uint64) query.ResultRow {
	ev := query.ResultRow{EventID: id, BinlogFile: "binlog.000003", StartPos: pos, EndPos: pos + 10,
		SchemaName: "shop", TableName: "orders", EventType: typ, PKValues: key}
	img := map[string]any{"id": json.Number(key), "status": status}
	switch typ {
	case event.EventInsert:
		ev.RowAfter = img
	case event.EventUpdate:
		ev.RowBefore, ev.RowAfter = map[string]any{"id": json.Number(key), "status": "before"}, img
	case event.EventDelete:
		ev.RowBefore = img
	}
	return ev
}

func lit(p string) string { return "'" + strings.ReplaceAll(p, "'", "''") + "'" }

// readState runs sql in DuckDB, under the sandbox's collation, and prints its
// rows as "id=status", ordered by id as text.
func readState(t *testing.T, stateSQL string) []string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "SET default_collation = 'nocase.icu_noaccent'"); err != nil {
		t.Fatal(err)
	}
	rs, err := db.QueryContext(ctx, "SELECT CAST(id AS VARCHAR), coalesce(status, 'NULL') FROM ("+stateSQL+") ORDER BY 1 COLLATE \"binary\"")
	if err != nil {
		t.Fatalf("%v\n%s", err, stateSQL)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var id, st string
		if err := rs.Scan(&id, &st); err != nil {
			t.Fatal(err)
		}
		out = append(out, id+"="+st)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// tailOver folds events into a fresh tail of orders, writes it beside the
// base, and returns the merged state of the published state SQL with it.
func tailOver(t *testing.T, dir, base, published string, events ...[]query.ResultRow) string {
	t.Helper()
	tail, err := NewTail("shop", "orders", tailOrdersDDL, base, tailOrdersPK)
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range events {
		if err := tail.Apply(page); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, fmt.Sprintf("orders.tail.%d.parquet", tail.LastEventID()))
	if _, err := tail.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	// The tail outlives the write: its folded images are left as they were.
	for k, ev := range tail.fold.Changes {
		if _, ok := ev.RowAfter[baseline.TableDeltaOpColumn]; ok {
			t.Fatalf("writing the tail changed the image of key %s", k)
		}
	}
	return baseline.TableTailStateSQL(published, lit(path), tail.PKColumns(), "")
}

func TestTail_overThePublishedState(t *testing.T) {
	const big = "18446744073709551615"
	dir := t.TempDir()
	base := writeTailBase(t, dir, "orders.parquet", tailOrdersDDL,
		map[string]string{"id": "1", "status": "one"},
		map[string]string{"id": "2", "status": "two"},
		map[string]string{"id": "3", "status": "three"},
		map[string]string{"id": big, "status": "big"},
		map[string]string{"id": "18446744073709551614", "status": "big minus one"},
	)
	published := "SELECT * FROM read_parquet(" + lit(base) + ")"
	want := func(t *testing.T, got []string, want ...string) {
		t.Helper()
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("got  %v\nwant %v", got, want)
		}
	}
	all := []string{"1=one", "18446744073709551614=big minus one", big + "=big", "2=two", "3=three"}

	t.Run("an empty tail reads as the published state", func(t *testing.T) {
		want(t, readState(t, tailOver(t, t.TempDir(), base, published)), all...)
	})
	t.Run("insert, update and delete", func(t *testing.T) {
		got := readState(t, tailOver(t, t.TempDir(), base, published, []query.ResultRow{
			tailEvent(10, event.EventInsert, "9", "nine", 100),
			tailEvent(11, event.EventUpdate, "2", "two edited", 110),
			tailEvent(12, event.EventDelete, "1", "", 120),
		}))
		want(t, got, "18446744073709551614=big minus one", big+"=big", "2=two edited", "3=three", "9=nine")
	})
	t.Run("a key touched twice keeps its last change in the binary log, across pages", func(t *testing.T) {
		got := readState(t, tailOver(t, t.TempDir(), base, published,
			[]query.ResultRow{tailEvent(21, event.EventUpdate, "3", "later", 300)},
			[]query.ResultRow{tailEvent(20, event.EventUpdate, "3", "earlier", 200)},
		))
		want(t, got, "1=one", "18446744073709551614=big minus one", big+"=big", "2=two", "3=later")
	})
	t.Run("inserted then deleted inside the tail: nowhere", func(t *testing.T) {
		got := readState(t, tailOver(t, t.TempDir(), base, published, []query.ResultRow{
			tailEvent(30, event.EventInsert, "11", "eleven", 100),
			tailEvent(31, event.EventDelete, "11", "", 110),
		}))
		want(t, got, all...)
	})
	t.Run("the largest unsigned key, deleted and updated, exactly", func(t *testing.T) {
		got := readState(t, tailOver(t, t.TempDir(), base, published, []query.ResultRow{
			tailEvent(40, event.EventDelete, big, "", 100),
			tailEvent(41, event.EventUpdate, "18446744073709551614", "edited", 110),
		}))
		want(t, got, "1=one", "18446744073709551614=edited", "2=two", "3=three")
	})
	t.Run("a capture replay folds to the same state", func(t *testing.T) {
		evs := []query.ResultRow{tailEvent(50, event.EventUpdate, "2", "x", 100), tailEvent(51, event.EventDelete, "3", "", 110)}
		replay := []query.ResultRow{tailEvent(60, event.EventUpdate, "2", "x", 100), tailEvent(61, event.EventDelete, "3", "", 110)}
		want(t, readState(t, tailOver(t, t.TempDir(), base, published, evs, replay)),
			readState(t, tailOver(t, t.TempDir(), base, published, evs))...)
	})
	t.Run("over a published pair that already replaced the key, the tail wins", func(t *testing.T) {
		d := t.TempDir()
		posdel := filepath.Join(d, "orders.000000.posdel")
		if _, err := baseline.WritePosdel(posdel, map[string]string{}, []int64{1}); err != nil { // base row 2
			t.Fatal(err)
		}
		cols, _ := baseline.ParseSchemaText(tailOrdersDDL)
		upsCols, _ := baseline.TableDeltaColumns(cols)
		upserts := filepath.Join(d, "orders.000000.upserts")
		w, err := newParquetTableWriter(upserts, upsCols, map[string]string{})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow(map[string]any{"id": json.Number("2"), "status": "pair", baseline.TableDeltaPKColumn: "2", baseline.TableDeltaOpColumn: "u"}, "shop", "orders"); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		withPair := baseline.TableDeltaOnePairStateSQL(lit(base), lit(posdel), lit(upserts), "")
		if got := readState(t, withPair); !strings.Contains(strings.Join(got, ","), "2=pair") {
			t.Fatalf("the published pair does not read: %v", got)
		}
		got := readState(t, tailOver(t, d, base, withPair, []query.ResultRow{tailEvent(70, event.EventUpdate, "2", "tail", 100)}))
		want(t, got, "1=one", "18446744073709551614=big minus one", big+"=big", "2=tail", "3=three")
	})
	t.Run("the published casts apply to the tail's rows too", func(t *testing.T) {
		tail, err := NewTail("shop", "orders", tailOrdersDDL, base, tailOrdersPK)
		if err != nil {
			t.Fatal(err)
		}
		if err := tail.Apply([]query.ResultRow{tailEvent(90, event.EventInsert, "9", "nine", 100)}); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "orders.tail.parquet")
		if _, err := tail.WriteFile(path); err != nil {
			t.Fatal(err)
		}
		got := readState(t, baseline.TableTailStateSQL(published, lit(path), tail.PKColumns(), "upper(status) AS status"))
		want(t, got, "1=ONE", "18446744073709551614=BIG MINUS ONE", big+"=BIG", "2=TWO", "3=THREE", "9=NINE")
	})
	t.Run("the view has the published columns and types, column for column", func(t *testing.T) {
		merged := tailOver(t, t.TempDir(), base, published, []query.ResultRow{tailEvent(80, event.EventInsert, "9", "nine", 100)})
		if a, b := describe(t, published), describe(t, merged); a != b {
			t.Errorf("published %s\nwith tail %s", a, b)
		}
	})
}

func describe(t *testing.T, q string) string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rs, err := db.Query("DESCRIBE " + q)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	cols, _ := rs.Columns()
	var out []string
	for rs.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprint(vals[0], " ", vals[1]))
	}
	return strings.Join(out, "; ")
}

// A composite integer key, negative values included: the tombstone carries
// every part.
func TestTail_compositeIntegerKey(t *testing.T) {
	const ddl = "CREATE TABLE `lines` (\n  `o` int NOT NULL,\n  `n` smallint NOT NULL,\n  `status` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`o`,`n`)\n);\n"
	pk := []metadata.ColumnMeta{{Name: "o", IsPK: true, DataType: "int"}, {Name: "n", IsPK: true, DataType: "smallint"}}
	dir := t.TempDir()
	base := writeTailBase(t, dir, "lines.parquet", ddl,
		map[string]string{"o": "-1", "n": "1", "status": "a"},
		map[string]string{"o": "-1", "n": "2", "status": "b"},
		map[string]string{"o": "1", "n": "-1", "status": "c"},
	)
	tail, err := NewTail("shop", "lines", ddl, base, pk)
	if err != nil {
		t.Fatal(err)
	}
	ev := func(id uint64, typ event.EventType, o, n, st string) query.ResultRow {
		img := map[string]any{"o": json.Number(o), "n": json.Number(n), "status": st}
		r := query.ResultRow{EventID: id, BinlogFile: "binlog.000003", StartPos: id * 10, SchemaName: "shop", TableName: "lines",
			EventType: typ, PKValues: o + "|" + n, RowBefore: img}
		if typ != event.EventDelete {
			r.RowAfter = img
		}
		return r
	}
	if err := tail.Apply([]query.ResultRow{ev(1, event.EventDelete, "-1", "2", ""), ev(2, event.EventUpdate, "1", "-1", "c2")}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "lines.tail.parquet")
	if _, err := tail.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	db, _ := sql.Open("duckdb", "")
	defer db.Close()
	rs, err := db.Query("SELECT o || '/' || n || '=' || status FROM (" + baseline.TableTailStateSQL("SELECT * FROM read_parquet("+lit(base)+")", lit(path), tail.PKColumns(), "") + ") ORDER BY o, n")
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var got []string
	for rs.Next() {
		var s string
		_ = rs.Scan(&s)
		got = append(got, s)
	}
	if strings.Join(got, ",") != "-1/1=a,1/-1=c2" {
		t.Errorf("got %v", got)
	}
}

// Which tables have a tail: only integer keys, a definition on record, no
// system versioning.
func TestNewTail_refusals(t *testing.T) {
	ddl := func(idType string) string {
		return "CREATE TABLE `t` (\n  `id` " + idType + " NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	}
	pk := []metadata.ColumnMeta{{Name: "id", IsPK: true}}
	dir := t.TempDir()
	for _, ok := range []string{"tinyint", "tinyint(1)", "smallint unsigned", "mediumint", "int", "int unsigned", "bigint", "bigint unsigned"} {
		base := writeTailBase(t, dir, strings.ReplaceAll(ok, " ", "_")+".parquet", ddl(ok))
		if _, err := NewTail("s", "t", ddl(ok), base, pk); err != nil {
			t.Errorf("%s: refused: %v", ok, err)
		}
	}
	for _, c := range []struct{ name, ddl, why string }{
		{"text key", ddl("varchar(10)"), "only integer keys"},
		{"decimal key", ddl("decimal(10,0)"), "only integer keys"},
		{"year key", ddl("year"), "only integer keys"},
		{"bit key", ddl("bit(8)"), "only integer keys"},
		{"binary key", ddl("binary(16)"), "only integer keys"},
		{"no definition", "", "does not record"},
		{"system versioned", strings.TrimSuffix(ddl("int"), ";\n") + " WITH SYSTEM VERSIONING;\n", "system-versioned"},
	} {
		_, err := NewTail("s", "t", c.ddl, "", pk)
		var r *TailRefusal
		if !errors.As(err, &r) || !strings.Contains(r.Reason, c.why) {
			t.Errorf("%s: %v, want a refusal saying %q", c.name, err, c.why)
		}
	}
	var r *TailRefusal
	intBase := writeTailBase(t, dir, "int_base.parquet", ddl("int"))
	if _, err := NewTail("s", "t", ddl("int"), intBase, nil); !errors.As(err, &r) || !strings.Contains(r.Reason, "no primary key") {
		t.Errorf("no key: %v", err)
	}
	if _, err := NewTail("s", "t", ddl("int"), intBase, []metadata.ColumnMeta{{Name: "other"}}); !errors.As(err, &r) || !strings.Contains(r.Reason, "not in its copy") {
		t.Errorf("a key the copy does not have: %v", err)
	}
}

// The tail takes its own table's row events only: another table's (one whose
// name starts the same, or differs only in case, included) is a caller's
// bug, and so is a DDL.
func TestTail_applyOnlyItsTable(t *testing.T) {
	tail, err := NewTail("shop", "orders", tailOrdersDDL, writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL), tailOrdersPK)
	if err != nil {
		t.Fatal(err)
	}
	other := tailEvent(1, event.EventInsert, "1", "x", 10)
	other.TableName = "orders_archive"
	if err := tail.Apply([]query.ResultRow{other}); err == nil {
		t.Error("orders_archive's event was folded into orders")
	}
	cased := tailEvent(2, event.EventInsert, "1", "x", 10)
	cased.TableName = "Orders"
	if err := tail.Apply([]query.ResultRow{cased}); err == nil {
		t.Error("Orders' event was folded into orders: two tables where names keep their case")
	}
	ddl := tailEvent(3, event.EventDDL, "", "", 20)
	if err := tail.Apply([]query.ResultRow{ddl}); err == nil {
		t.Error("a DDL event was folded")
	}
	// A key-changing UPDATE refuses, as the refresh refuses it (#782).
	moved := tailEvent(4, event.EventUpdate, "5", "x", 30)
	moved.RowBefore = map[string]any{"id": json.Number("4"), "status": "x"}
	moved.PKValues = "4"
	if err := tail.Apply([]query.ResultRow{moved}); err == nil {
		t.Error("a key-changing UPDATE was folded")
	}
}

func TestTailKeyValues(t *testing.T) {
	for _, c := range []struct {
		key string
		n   int
		ok  bool
	}{
		{"5", 1, true}, {"-5", 1, true}, {"0", 1, true}, {"18446744073709551615", 1, true}, {"1|-2", 2, true},
		{"", 1, false}, {"-", 1, false}, {"-0", 1, false}, {"05", 1, false}, {"-0x1", 1, false}, {"1.0", 1, false}, {"1|2", 1, false}, {"1", 2, false}, {`1\|2`, 2, false},
	} {
		_, err := tailKeyValues(c.key, c.n)
		if (err == nil) != c.ok {
			t.Errorf("%q/%d: err %v, want ok=%v", c.key, c.n, err, c.ok)
		}
	}
}

// The key as the copy spells it: metadata naming it ID and the copy id must
// still file a tombstone's key under the copy's name, or the key is NULL and
// the deleted row comes back.
func TestTail_keyNameCaseFromTheCopy(t *testing.T) {
	dir := t.TempDir()
	base := writeTailBase(t, dir, "orders.parquet", tailOrdersDDL,
		map[string]string{"id": "1", "status": "one"}, map[string]string{"id": "2", "status": "two"})
	tail, err := NewTail("shop", "orders", tailOrdersDDL, base, []metadata.ColumnMeta{{Name: "ID", IsPK: true, DataType: "bigint"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := tail.PKColumns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("key columns %v, want the copy's spelling", got)
	}
	if err := tail.Apply([]query.ResultRow{tailEvent(1, event.EventDelete, "1", "", 10)}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "orders.tail.parquet")
	if _, err := tail.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	got := readState(t, baseline.TableTailStateSQL("SELECT * FROM read_parquet("+lit(base)+")", lit(path), tail.PKColumns(), ""))
	if strings.Join(got, ",") != "2=two" {
		t.Errorf("got %v, want the deleted row gone", got)
	}
}

// #843 for the tail: a change after the cut whose image lacks a column the
// copy has (dropped at the source) refuses the tail instead of writing NULL
// beside rows that keep the old value.
func TestTail_aDroppedColumnRefuses(t *testing.T) {
	tail, err := NewTail("shop", "orders", tailOrdersDDL, writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL), tailOrdersPK)
	if err != nil {
		t.Fatal(err)
	}
	ev := tailEvent(1, event.EventInsert, "9", "nine", 10)
	delete(ev.RowAfter, "status")
	if err := tail.Apply([]query.ResultRow{ev}); err != nil {
		t.Fatal(err)
	}
	_, err = tail.WriteFile(filepath.Join(t.TempDir(), "orders.tail.parquet"))
	var r *TailRefusal
	if !errors.As(err, &r) || !strings.Contains(r.Reason, "status") {
		t.Fatalf("got %v, want a refusal naming status", err)
	}
}

// A copy that stores a now-binary column as text (written before VECTOR
// joined the binary list) has no tail: the merge would turn its text into
// bytes on every published row.
func TestNewTail_binaryStoredAsTextRefuses(t *testing.T) {
	old := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(64) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	now := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` vector(2) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	base := writeTailBase(t, t.TempDir(), "t.parquet", old, map[string]string{"id": "1", "v": "0x00"})
	_, err := NewTail("s", "t", now, base, []metadata.ColumnMeta{{Name: "id", IsPK: true}})
	var r *TailRefusal
	if !errors.As(err, &r) || !strings.Contains(r.Reason, "stores v as text") {
		t.Fatalf("got %v, want a refusal naming v", err)
	}
	if _, err := NewTail("s", "t", now, filepath.Join(t.TempDir(), "gone.parquet"), []metadata.ColumnMeta{{Name: "id", IsPK: true}}); !errors.As(err, &r) {
		t.Errorf("an unreadable copy: %v, want a refusal", err)
	}
}

// An event with no place in a binary log cannot be ordered against the
// others, and the last change of a key is chosen by that order.
func TestTail_anEventWithNoPositionRefuses(t *testing.T) {
	tail, err := NewTail("shop", "orders", tailOrdersDDL, writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL), tailOrdersPK)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []func(*query.ResultRow){
		func(r *query.ResultRow) { r.BinlogFile = "" },
		func(r *query.ResultRow) { r.BinlogFile = "0/16B3748" }, // a PostgreSQL LSN
		func(r *query.ResultRow) { r.StartPos = 1 << 63 },
	} {
		ev := tailEvent(1, event.EventInsert, "9", "nine", 10)
		f(&ev)
		if err := tail.Apply([]query.ResultRow{ev}); err == nil {
			t.Errorf("%s:%d was folded", ev.BinlogFile, ev.StartPos)
		}
	}
}

// A version of the tail is never written over another: a statement reads
// the file twice.
func TestTail_writeFileRefusesAnExistingName(t *testing.T) {
	dir := t.TempDir()
	tail, err := NewTail("shop", "orders", tailOrdersDDL, writeTailBase(t, dir, "orders.parquet", tailOrdersDDL), tailOrdersPK)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "orders.tail.1.parquet")
	if _, err := tail.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.WriteFile(path); err == nil {
		t.Error("a version was written over another")
	}
}

// #2483: a newer published cut prunes the tail instead of rebuilding it. A key
// stays exactly when the read of the newer cut's window would return its last
// event (indexTailSource.Fill): start position at or after the anchor (the
// anchor is where the next write begins, so an event starting there is after
// the cut) and, when the cut records one, an id above its last event id.
func TestTail_pruneThrough(t *testing.T) {
	base := writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL)
	newTail := func(t *testing.T, evs ...query.ResultRow) *Tail {
		t.Helper()
		tail, err := NewTail("shop", "orders", tailOrdersDDL, base, tailOrdersPK)
		if err != nil {
			t.Fatal(err)
		}
		if err := tail.Apply(evs); err != nil {
			t.Fatal(err)
		}
		return tail
	}
	keys := func(tail *Tail) string {
		var out []string
		for k := range tail.fold.Changes {
			out = append(out, k)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	at := func(file string, pos uint64) query.BinlogPos { return query.BinlogPos{File: file, Pos: pos} }

	t.Run("before the anchor dropped, at it and after it kept", func(t *testing.T) {
		tail := newTail(t,
			tailEvent(1, event.EventUpdate, "1", "before", 100),
			tailEvent(2, event.EventUpdate, "2", "at", 200),
			tailEvent(3, event.EventDelete, "3", "", 300),
		)
		n, err := tail.PruneThrough(at("binlog.000003", 200), 0)
		if err != nil || n != 1 || keys(tail) != "2,3" {
			t.Fatalf("dropped %d (%v), kept %s", n, err, keys(tail))
		}
	})
	t.Run("a key keeps its last change: an earlier one before the anchor does not drop it", func(t *testing.T) {
		tail := newTail(t,
			tailEvent(1, event.EventUpdate, "1", "old", 100),
			tailEvent(5, event.EventUpdate, "1", "new", 500),
		)
		if n, err := tail.PruneThrough(at("binlog.000003", 200), 0); err != nil || n != 0 || keys(tail) != "1" {
			t.Fatalf("dropped %d (%v), kept %s", n, err, keys(tail))
		}
	})
	t.Run("an id at or below the cut's last event id is dropped, one above kept", func(t *testing.T) {
		tail := newTail(t,
			tailEvent(7, event.EventUpdate, "1", "at id", 300),
			tailEvent(8, event.EventUpdate, "2", "above", 400),
		)
		if n, err := tail.PruneThrough(at("binlog.000003", 200), 7); err != nil || n != 1 || keys(tail) != "2" {
			t.Fatalf("dropped %d (%v), kept %s", n, err, keys(tail))
		}
	})
	t.Run("later files are after the anchor, by length first", func(t *testing.T) {
		e1 := tailEvent(1, event.EventUpdate, "1", "next file", 4)
		e1.BinlogFile = "binlog.000004"
		e2 := tailEvent(2, event.EventUpdate, "2", "longer name", 4)
		e2.BinlogFile = "binlog.1000000"
		e3 := tailEvent(3, event.EventUpdate, "3", "earlier file", 900)
		e3.BinlogFile = "binlog.000002"
		tail := newTail(t, e3, e1, e2)
		if n, err := tail.PruneThrough(at("binlog.000003", 500), 0); err != nil || n != 1 || keys(tail) != "1,2" {
			t.Fatalf("dropped %d (%v), kept %s", n, err, keys(tail))
		}
	})
	t.Run("everything at or before the anchor: an empty tail", func(t *testing.T) {
		tail := newTail(t, tailEvent(1, event.EventInsert, "1", "x", 100))
		if n, err := tail.PruneThrough(at("binlog.000003", 101), 0); err != nil || n != 1 || tail.Keys() != 0 {
			t.Fatalf("dropped %d (%v), kept %s", n, err, keys(tail))
		}
	})
	t.Run("an empty tail prunes to nothing", func(t *testing.T) {
		tail := newTail(t)
		if n, err := tail.PruneThrough(at("binlog.000003", 101), 0); err != nil || n != 0 {
			t.Fatalf("dropped %d (%v)", n, err)
		}
	})
	t.Run("refused, and nothing dropped, when no order relates a key's change to the anchor", func(t *testing.T) {
		for name, c := range map[string]struct {
			anchor query.BinlogPos
			edit   func(*Tail)
		}{
			"an anchor with no file":                 {anchor: at("", 200)},
			"an anchor that is an LSN":               {anchor: at("0/16B3748", 200)},
			"an anchor under another base name":      {anchor: at("mysql-bin.000003", 200)},
			"a key's change with no position":        {anchor: at("binlog.000003", 200), edit: func(tl *Tail) { tl.fold.Changes["3"].BinlogFile = "" }},
			"a key's change under another numbering": {anchor: at("binlog.000003", 200), edit: func(tl *Tail) { tl.fold.Changes["3"].BinlogFile = "mysql-bin.000009" }},
		} {
			tail := newTail(t,
				tailEvent(1, event.EventUpdate, "1", "before", 100),
				tailEvent(3, event.EventUpdate, "3", "after", 300),
			)
			if c.edit != nil {
				c.edit(tail)
			}
			if n, err := tail.PruneThrough(c.anchor, 0); err == nil || n != 0 || keys(tail) != "1,3" {
				t.Errorf("%s: dropped %d (%v), kept %s", name, n, err, keys(tail))
			}
		}
	})
	t.Run("the pruned tail writes only what it kept, over the newer copy", func(t *testing.T) {
		dir := t.TempDir()
		newer := writeTailBase(t, dir, "orders.parquet", tailOrdersDDL,
			map[string]string{"id": "1", "status": "in the cut"},
			map[string]string{"id": "2", "status": "in the cut"},
		)
		tail := newTail(t,
			tailEvent(1, event.EventUpdate, "1", "in the cut", 100),
			tailEvent(2, event.EventUpdate, "2", "after the cut", 300),
		)
		if _, err := tail.PruneThrough(at("binlog.000003", 200), 1); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "orders.tail.parquet")
		if _, err := tail.WriteFile(path); err != nil {
			t.Fatal(err)
		}
		got := readState(t, baseline.TableTailStateSQL("SELECT * FROM read_parquet("+lit(newer)+")", lit(path), tail.PKColumns(), ""))
		if strings.Join(got, ",") != "1=in the cut,2=after the cut" {
			t.Errorf("%v", got)
		}
		var n int
		db, _ := sql.Open("duckdb", "")
		defer db.Close()
		if err := db.QueryRow("SELECT count(*) FROM read_parquet(" + lit(path) + ")").Scan(&n); err != nil || n != 1 {
			t.Errorf("the file holds %d rows (%v), want the 1 key kept", n, err)
		}
	})
}
