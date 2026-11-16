package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

var pgTailRel = baseline.PGRelation{OID: 16500, Columns: []baseline.PGColumn{
	{Name: "id", TypeOID: 25, TypeMod: -1, Type: "text", NotNull: true, Ordinal: 1},
	{Name: "status", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 2},
}, PrimaryKey: []string{"id"}}

var pgTailCols = []string{"id", "status"}

// writePGTailBase writes a PostgreSQL table file of (id, status) rows and
// returns its path and footer. mutate, when set, changes the footer first.
func writePGTailBase(t *testing.T, dir string, mutate func(map[string]string), rows ...[2]string) (string, baseline.DumpMetadata) {
	t.Helper()
	rel, err := pgTailRel.Encode()
	if err != nil {
		t.Fatal(err)
	}
	md := map[string]string{
		baseline.MetaKeySnapshotTimestamp: "2026-11-16T00:00:00Z",
		baseline.MetaKeyLSN:               fmt.Sprint(0x500),
		baseline.MetaKeyPGRelation:        rel,
		baseline.MetaKeyRenderGUCs:        baseline.RenderGUCsPinned,
		baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
	}
	if mutate != nil {
		mutate(md)
	}
	path := filepath.Join(dir, "orders.parquet")
	w, err := baseline.NewWriter(path, rawTextColumns(pgTailCols), baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow([]string{r[0], r[1]}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	footer, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, footer
}

// readPGState runs sql in DuckDB as the PostgreSQL session compares text
// (by its bytes) and prints its rows as "id=status", ordered by id.
func readPGState(t *testing.T, stateSQL string) []string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rs, err := db.QueryContext(context.Background(), "SELECT id, coalesce(status, 'NULL') FROM ("+stateSQL+") ORDER BY 1 COLLATE \"binary\"")
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

func newTestPGTail(t *testing.T, md baseline.DumpMetadata) *PGTail {
	t.Helper()
	tail, err := NewPGTail("mydb", "orders", md, pgTailCols, pkColsIntID())
	if err != nil {
		t.Fatal(err)
	}
	return tail
}

func pgPublished(base string) string { return "SELECT * FROM read_parquet(" + lit(base) + ")" }

// The newest change of each key lies over the published rows: an update, a
// delete, an insert, a row inserted and deleted, a key with a pipe and a
// backslash, and two keys that differ only in case.
func TestPGTail_OverTheCopy(t *testing.T) {
	dir := t.TempDir()
	base, md := writePGTailBase(t, dir, nil,
		[2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"3", "three"},
		[2]string{`a|b\c`, "piped"}, [2]string{"k", "lower"}, [2]string{"K", "upper"})
	tail := newTestPGTail(t, md)
	// Out of order on purpose: the later LSN wins, whatever the page order.
	pages := [][]query.ResultRow{
		{pgUpd(11, 0x620, "1", "1", "ONE again"), pgUpd(10, 0x610, "1", "1", "ONE")},
		{pgDel(12, 0x630, "2"), pgIns(13, 0x640, "9", "nine")},
		{pgIns(14, 0x650, "8", "brief"), pgDel(15, 0x660, "8")},
		{pgUpd(16, 0x670, `a|b\c`, `a|b\c`, "piped twice"), pgUpd(17, 0x680, "K", "K", "UPPER")},
	}
	for _, p := range pages {
		if err := tail.Apply(p); err != nil {
			t.Fatal(err)
		}
	}
	if tail.Keys() != 6 || tail.Dirty() != 6 || tail.LastEventID() != 17 || !reflect.DeepEqual(tail.PKColumns(), []string{"id"}) {
		t.Fatalf("keys %d dirty %d last %d pk %v", tail.Keys(), tail.Dirty(), tail.LastEventID(), tail.PKColumns())
	}
	path := filepath.Join(dir, "1-orders.parquet")
	rows, err := tail.WriteFile(path)
	if err != nil || rows != 6 || tail.Dirty() != 0 {
		t.Fatalf("WriteFile: %d rows, %v, dirty %d", rows, err, tail.Dirty())
	}
	got := readPGState(t, baseline.TableTailStateSQL(pgPublished(base), lit(path), tail.PKColumns(), ""))
	want := []string{"1=ONE again", "3=three", "9=nine", "K=UPPER", `a|b\c=piped twice`, "k=lower"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
	// A file is never written over.
	if _, err := tail.WriteFile(path); err == nil {
		t.Fatal("WriteFile wrote over a file a statement may be reading")
	}
}

// Events as the keeper is handed them (PGTailEvent) fold the same, and the
// page is left as it was handed over.
func TestPGTail_KeeperSpelling(t *testing.T) {
	dir := t.TempDir()
	base, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"})
	tail := newTestPGTail(t, md)
	page := []query.ResultRow{pgUpd(10, 0x610, "1", "1", "ONE"), pgIns(11, 0x620, "2", "two")}
	for i := range page {
		if !PGTailEvent(&page[i]) {
			t.Fatalf("event %d has no LSN", page[i].EventID)
		}
	}
	if pos := (query.BinlogPos{File: page[0].BinlogFile, Pos: page[0].StartPos}); pos != PGTailPos(0x610) || pos.IsLSN() || !PGTailPos(0x610).AtOrBefore(PGTailPos(0x620)) || PGTailPos(0x620).AtOrBefore(PGTailPos(0x610)) {
		t.Fatalf("a keeper's position %+v does not order as a binary log coordinate", pos)
	}
	if err := tail.Apply(page); err != nil {
		t.Fatal(err)
	}
	if page[0].BinlogFile != PGTailLog || page[1].BinlogFile != PGTailLog {
		t.Fatalf("Apply changed the page it was handed: %q, %q", page[0].BinlogFile, page[1].BinlogFile)
	}
	path := filepath.Join(dir, "1-orders.parquet")
	if _, err := tail.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	if got, want := readPGState(t, baseline.TableTailStateSQL(pgPublished(base), lit(path), tail.PKColumns(), "")), []string{"1=ONE", "2=two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
	// An event with a binary log position has no place in a PostgreSQL tail.
	bad := pgIns(12, 0x630, "3", "three")
	bad.BinlogFile = "binlog.000003"
	if err := tail.Apply([]query.ResultRow{bad}); err == nil || !strings.Contains(err.Error(), "no LSN") {
		t.Fatalf("an event with a binary log position: %v", err)
	}
	if PGTailEvent(&bad) {
		t.Fatal("PGTailEvent took an event with no LSN")
	}
}

// An UPDATE that changes the key deletes the old key and files the new one.
func TestPGTail_KeyChange(t *testing.T) {
	dir := t.TempDir()
	base, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"}, [2]string{"2", "two"})
	tail := newTestPGTail(t, md)
	if err := tail.Apply([]query.ResultRow{pgEv(10, 0x610, event.EventUpdate, pgImg("1", "one"), pgImg("7", "moved"))}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "1-orders.parquet")
	if _, err := tail.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	if got, want := readPGState(t, baseline.TableTailStateSQL(pgPublished(base), lit(path), tail.PKColumns(), "")), []string{"2=two", "7=moved"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
}

// A chain: a whole file, then the keys changed since, newest first.
func TestPGTail_Chain(t *testing.T) {
	dir := t.TempDir()
	base, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"3", "three"})
	tail := newTestPGTail(t, md)
	if err := tail.Apply([]query.ResultRow{pgUpd(10, 0x610, "1", "1", "ONE"), pgDel(11, 0x620, "2")}); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(dir, "1-orders.parquet")
	if _, err := tail.WriteFile(first); err != nil {
		t.Fatal(err)
	}
	if err := tail.Apply([]query.ResultRow{pgIns(12, 0x630, "2", "two again"), pgUpd(13, 0x640, "3", "3", "THREE")}); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(dir, "2-orders.parquet")
	if rows, err := tail.WriteDelta(second); err != nil || rows != 2 {
		t.Fatalf("WriteDelta: %d rows, %v", rows, err)
	}
	state := baseline.TableTailChainStateSQL(pgPublished(base), []string{lit(first), lit(second)}, tail.PKColumns(), "")
	if got, want := readPGState(t, state), []string{"1=ONE", "2=two again", "3=THREE"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
}

// A newer cut drops the keys it already holds, by LSN and by event id, with
// their transaction records, and the next file must be whole.
func TestPGTail_PruneThrough(t *testing.T) {
	dir := t.TempDir()
	_, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"})
	tail := newTestPGTail(t, md)
	if err := tail.Apply([]query.ResultRow{pgUpd(10, 0x610, "1", "1", "ONE"), pgIns(11, 0x620, "2", "two"), pgIns(12, 0x630, "3", "three")}); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.PruneThrough(query.BinlogPos{File: lsnText(0x620), Pos: 0x620}, 0); err == nil {
		t.Fatal("a cut that is not spelled as a keeper's position was taken")
	}
	if _, err := tail.PruneThrough(PGTailPos(0), 0); err == nil {
		t.Fatal("a cut with no LSN was taken")
	}
	if tail.Keys() != 3 {
		t.Fatalf("a refused prune dropped keys: %d left", tail.Keys())
	}
	// At or after 0x620 stays; an id at or below 11 goes.
	n, err := tail.PruneThrough(PGTailPos(0x620), 11)
	if err != nil || n != 2 || tail.Keys() != 1 {
		t.Fatalf("PruneThrough: dropped %d, %v, %d left", n, err, tail.Keys())
	}
	if _, kept := tail.fold.Changes["3"]; !kept {
		t.Fatalf("the key after the cut was dropped: %v", tail.fold.Changes)
	}
	for _, k := range []string{"1", "2"} {
		if _, has := tail.fold.pgTxn[k]; has {
			t.Fatalf("the transaction record of dropped key %s was kept", k)
		}
	}
	if _, err := tail.WriteDelta(filepath.Join(dir, "2-orders.parquet")); err == nil {
		t.Fatal("a delta was written after a prune")
	}
	if rows, err := tail.WriteFile(filepath.Join(dir, "3-orders.parquet")); err != nil || rows != 1 {
		t.Fatalf("WriteFile after a prune: %d rows, %v", rows, err)
	}
	// A key dropped and changed again starts over.
	if err := tail.Apply([]query.ResultRow{pgUpd(13, 0x640, "1", "1", "ONE more")}); err != nil {
		t.Fatal(err)
	}
	if rows, err := tail.WriteDelta(filepath.Join(dir, "4-orders.parquet")); err != nil || rows != 1 {
		t.Fatalf("WriteDelta after the whole file: %d rows, %v", rows, err)
	}
}

// A column the source publishes and the copy does not hold (a generated
// one) is in the image and not in the tail's file.
func TestPGTail_OnlyTheCopysColumns(t *testing.T) {
	dir := t.TempDir()
	base, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"})
	tail := newTestPGTail(t, md)
	after := map[string]any{"id": "1", "status": "ONE", "status_upper": "ONE"}
	before := map[string]any{"id": "1", "status": "one", "status_upper": "ONE"}
	if err := tail.Apply([]query.ResultRow{pgEv(10, 0x610, event.EventUpdate, before, after)}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "1-orders.parquet")
	if _, err := tail.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rs, err := db.Query("SELECT * FROM (" + baseline.TableTailStateSQL(pgPublished(base), lit(path), tail.PKColumns(), "") + ")")
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if cols, _ := rs.Columns(); !reflect.DeepEqual(cols, pgTailCols) {
		t.Fatalf("the table reads with columns %v, want %v", cols, pgTailCols)
	}
}

func TestNewPGTail_Refusals(t *testing.T) {
	pk := pkColsIntID()
	cases := []struct {
		name   string
		mutate func(map[string]string)
		cols   []string
		pk     []metadata.ColumnMeta
		want   string
	}{
		{name: "no LSN", mutate: func(m map[string]string) { delete(m, baseline.MetaKeyLSN) }, cols: pgTailCols, pk: pk, want: "records no LSN"},
		{name: "no recorded shape", mutate: func(m map[string]string) { delete(m, baseline.MetaKeyPGRelation) }, cols: pgTailCols, pk: pk, want: "records no table shape"},
		{name: "rendered under other settings", mutate: func(m map[string]string) { m[baseline.MetaKeyRenderGUCs] = "other" }, cols: pgTailCols, pk: pk, want: "rendered under other settings"},
		{name: "the file has another column", cols: []string{"id", "status", "extra"}, pk: pk, want: "disagree"},
		{name: "no primary key", cols: pgTailCols, want: "no primary key"},
		{name: "the key is not in the copy", cols: pgTailCols, pk: []metadata.ColumnMeta{{Name: "code", IsPK: true}}, want: "is not in its copy"},
		{name: "the key changed since the copy", cols: pgTailCols, pk: []metadata.ColumnMeta{{Name: "status", IsPK: true}}, want: "was taken under (id)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, md := writePGTailBase(t, t.TempDir(), c.mutate, [2]string{"1", "one"})
			_, err := NewPGTail("mydb", "orders", md, c.cols, c.pk)
			var refusal *TailRefusal
			if !asTailRefusal(err, &refusal) || !strings.Contains(refusal.Reason, c.want) {
				t.Fatalf("NewPGTail: %v, want a refusal with %q", err, c.want)
			}
		})
	}
}

func asTailRefusal(err error, into **TailRefusal) bool {
	r, ok := err.(*TailRefusal)
	if ok {
		*into = r
	}
	return ok
}

// A row is never written under another row's key, and a key that cannot be
// read back refuses the write.
func TestPGTail_KeyGuards(t *testing.T) {
	dir := t.TempDir()
	_, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"})
	tail := newTestPGTail(t, md)
	if err := tail.Apply([]query.ResultRow{pgIns(10, 0x610, "2", "two")}); err != nil {
		t.Fatal(err)
	}
	// The folded image's key column no longer says what its key says.
	tail.fold.Changes["2"].RowAfter["id"] = "3"
	if _, err := tail.WriteFile(filepath.Join(dir, "1-orders.parquet")); err == nil || !strings.Contains(err.Error(), "is filed under key") {
		t.Fatalf("a row under another row's key: %v", err)
	}
	long := newTestPGTail(t, md)
	key := strings.Repeat("x", event.MaxPKValuesLen)
	if err := long.Apply([]query.ResultRow{pgIns(11, 0x620, key, "long")}); err != nil {
		t.Fatal(err)
	}
	_, err := long.WriteFile(filepath.Join(dir, "2-orders.parquet"))
	var refusal *TailRefusal
	if !asTailRefusal(err, &refusal) || !strings.Contains(refusal.Reason, "as long as the index stores one") {
		t.Fatalf("a key as long as the index stores: %v", err)
	}
}

func TestPGKeyParts(t *testing.T) {
	for _, parts := range [][]string{{"1"}, {""}, {"a", "b"}, {`a|b`, `c\d`}, {`\`, `|`}, {"", ""}, {"é", "日本"}} {
		key := pgKeyOf(parts)
		got, err := pgKeyParts(key, len(parts))
		if err != nil || !reflect.DeepEqual(got, parts) {
			t.Errorf("%q: %q, %v; want %q", key, got, err, parts)
		}
	}
	for key, n := range map[string]int{"a|b": 1, "a": 2, `a\`: 1, `a\b`: 1, "": 2} {
		if got, err := pgKeyParts(key, n); err == nil {
			t.Errorf("%q as %d column(s): %q, want a refusal", key, n, got)
		}
	}
}
