package reconstruct

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2501: an UPDATE that changes a row's primary key is folded as the two
// events it is, a DELETE of the old key and an INSERT of the new one, both at
// the UPDATE's own place in the binary log, so the window's "last change per
// key" rule resolves every shape #782 listed instead of refusing the table.

const pkFile = "binlog.000009"

// pkEvent is a change of orders at pos of pkFile with event id. An UPDATE's
// PKValues is the BEFORE image's key, as the parser stores it; images carry
// json.Number, as query.UnmarshalRowImage decodes them.
func pkEvent(id uint64, pos uint64, typ event.EventType, before, after map[string]any) query.ResultRow {
	ev := query.ResultRow{EventID: id, BinlogFile: pkFile, StartPos: pos, EndPos: pos + 40,
		EventTimestamp: foldBase, SchemaName: "mydb", TableName: "orders", EventType: typ,
		RowBefore: before, RowAfter: after}
	switch typ {
	case event.EventInsert:
		ev.PKValues = event.BuildPKValues(pkColsIntID(), after)
	default:
		ev.PKValues = event.BuildPKValues(pkColsIntID(), before)
	}
	return ev
}

func pkImg(id int, status string) map[string]any {
	return map[string]any{"id": json.Number(fmt.Sprint(id)), "status": status}
}

func pkUpd(id, pos uint64, from, to int, status string) query.ResultRow {
	return pkEvent(id, pos, event.EventUpdate, pkImg(from, "before"), pkImg(to, status))
}

func pkIns(id, pos uint64, key int, status string) query.ResultRow {
	return pkEvent(id, pos, event.EventInsert, nil, pkImg(key, status))
}

func pkDel(id, pos uint64, key int) query.ResultRow {
	return pkEvent(id, pos, event.EventDelete, pkImg(key, "gone"), nil)
}

// foldPK folds pages into a fresh result with the MySQL policy.
func foldPK(t *testing.T, pages ...[]query.ResultRow) *foldResult {
	t.Helper()
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	for i, p := range pages {
		if err := foldPage(p, "mydb", "orders", pkColsIntID(), nil, foldOptions{}, res); err != nil {
			t.Fatalf("foldPage(page %d): %v", i, err)
		}
	}
	return res
}

// wantState checks the change map: a key mapped to "" must be DEAD (its last
// change a DELETE), any other value is the status its surviving image holds;
// a key absent from want must be absent from the map (untouched).
func wantState(t *testing.T, res *foldResult, want map[int]string) {
	t.Helper()
	for key, status := range want {
		ev := res.Changes[pkStrForInt(key)]
		if ev == nil {
			t.Errorf("key %d is not in the change map, want %q", key, status)
			continue
		}
		if ev.RowBefore != nil {
			t.Errorf("key %d kept a before-image; the map must hold retainEvent's trimmed copy", key)
		}
		if status == "" {
			if ev.EventType != event.EventDelete || ev.RowAfter != nil {
				t.Errorf("key %d = %v %v, want dead (a DELETE with no image)", key, ev.EventType, ev.RowAfter)
			}
			continue
		}
		if ev.EventType == event.EventDelete {
			t.Errorf("key %d is dead, want alive with %q", key, status)
			continue
		}
		if got := ev.RowAfter["status"]; got != status {
			t.Errorf("key %d = %q, want %q", key, got, status)
		}
		// The surviving image must carry ITS key: an image filed under another
		// key is the #782 duplicate/resurrection in a new coat.
		if got := event.BuildPKValues(pkColsIntID(), ev.RowAfter); got != pkStrForInt(key) {
			t.Errorf("key %d holds an image whose key is %q", key, got)
		}
	}
	if len(res.Changes) != len(want) {
		t.Errorf("change map holds %d keys, want %d: %v", len(res.Changes), len(want), keysOf(res.Changes))
	}
}

func keysOf(m map[string]*query.ResultRow) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The six #782 shapes, each over one page. The state each must leave is the
// one the source holds after the same statements.
func TestFoldPage_pkChange_shapes(t *testing.T) {
	cases := []struct {
		name string
		page []query.ResultRow
		want map[int]string
	}{
		{"A: UPDATE 1→2; DELETE 2 (resurrection)", []query.ResultRow{pkUpd(1, 100, 1, 2, "moved"), pkDel(2, 200, 2)}, map[int]string{1: "", 2: ""}},
		{"B: UPDATE 1→2; UPDATE 2 (duplicate)", []query.ResultRow{pkUpd(1, 100, 1, 2, "moved"), pkUpd(2, 200, 2, 2, "final")}, map[int]string{1: "", 2: "final"}},
		{"C: UPDATE 1→2; INSERT 1 (silent drop)", []query.ResultRow{pkUpd(1, 100, 1, 2, "moved"), pkIns(2, 200, 1, "reinserted")}, map[int]string{1: "reinserted", 2: "moved"}},
		{"D: UPDATE 1→2; UPDATE 2→3 (chain)", []query.ResultRow{pkUpd(1, 100, 1, 2, "moved"), pkUpd(2, 200, 2, 3, "moved again")}, map[int]string{1: "", 2: "", 3: "moved again"}},
		{"E: DELETE 2; UPDATE 1→2 (reuse of a deleted key)", []query.ResultRow{pkDel(1, 100, 2), pkUpd(2, 200, 1, 2, "took its place")}, map[int]string{1: "", 2: "took its place"}},
		{"F: UPDATE 1→2; UPDATE 2→1 (swap back)", []query.ResultRow{pkUpd(1, 100, 1, 2, "away"), pkUpd(2, 200, 2, 1, "back")}, map[int]string{1: "back", 2: ""}},
		{"a lone PK change", []query.ResultRow{pkUpd(1, 100, 1, 2, "moved")}, map[int]string{1: "", 2: "moved"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := foldPK(t, c.page)
			wantState(t, res, c.want)
			if res.Updates+res.Inserts+res.Deletes != int64(len(c.page)) {
				t.Errorf("counted %d inserts, %d updates, %d deletes over %d events: a PK change is ONE update, not an insert plus a delete",
					res.Inserts, res.Updates, res.Deletes, len(c.page))
			}
		})
	}
}

// The counters: a PK change counts once, as the UPDATE it is. Inserts and
// Deletes are the refresh's arithmetic (#2505); counting the synthetic rows
// would make the window claim two rows it never changed.
func TestFoldPage_pkChange_countsOnceAsAnUpdate(t *testing.T) {
	res := foldPK(t, []query.ResultRow{pkIns(1, 50, 9, "nine"), pkUpd(2, 100, 1, 2, "moved"), pkDel(3, 200, 9)})
	if res.Inserts != 1 || res.Updates != 1 || res.Deletes != 1 {
		t.Fatalf("inserts %d updates %d deletes %d, want 1 1 1", res.Inserts, res.Updates, res.Deletes)
	}
}

// Both synthetic entries sit at the UPDATE's coordinates, and the two do not
// share an image: the DELETE has none, so the writer's `row = ev.RowAfter`
// (which it mutates) touches one map only.
func TestFoldPage_pkChange_syntheticRowsCarryTheUpdatesPlace(t *testing.T) {
	u := pkUpd(7, 300, 1, 2, "moved")
	res := foldPK(t, []query.ResultRow{u})
	d, i := res.Changes[pkStrForInt(1)], res.Changes[pkStrForInt(2)]
	for _, ev := range []*query.ResultRow{d, i} {
		if ev == nil {
			t.Fatal("a synthetic row is missing")
		}
		if ev.BinlogFile != pkFile || ev.StartPos != 300 || ev.EventID != 7 || !ev.EventTimestamp.Equal(foldBase) {
			t.Errorf("synthetic row at %s:%d id %d %s, want the UPDATE's %s:300 id 7 %s", ev.BinlogFile, ev.StartPos, ev.EventID, ev.EventTimestamp, pkFile, foldBase)
		}
		if ev.SchemaName != "mydb" || ev.TableName != "orders" {
			t.Errorf("synthetic row of %s.%s", ev.SchemaName, ev.TableName)
		}
	}
	if d.EventType != event.EventDelete || d.RowAfter != nil || d.RowBefore != nil {
		t.Errorf("DELETE side = %v before %v after %v", d.EventType, d.RowBefore, d.RowAfter)
	}
	if i.EventType != event.EventInsert || i.RowBefore != nil || i.RowAfter["status"] != "moved" {
		t.Errorf("INSERT side = %v before %v after %v", i.EventType, i.RowBefore, i.RowAfter)
	}
	// The #843 guard saw the event once: both images, intersected.
	if !res.SawImage || len(res.ImageColumns) != 2 {
		t.Errorf("image columns %v (saw %v), want the two columns of both images", res.ImageColumns, res.SawImage)
	}
}

// G: one rows-event moves several rows (`UPDATE ... SET id = id + 1 ORDER BY
// id DESC`): every row shares the statement's (file, pos) and the ids rise in
// the order the server applied them, which is the fold's tie rule.
func TestFoldPage_pkChange_multiRowStatement(t *testing.T) {
	page := []query.ResultRow{pkUpd(10, 100, 3, 4, "3→4"), pkUpd(11, 100, 2, 3, "2→3"), pkUpd(12, 100, 1, 2, "1→2")}
	wantState(t, foldPK(t, page), map[int]string{1: "", 2: "1→2", 3: "2→3", 4: "3→4"})
}

// H: the chains cut at a page boundary resolve as on one page (mould:
// TestFoldPage_lastWriteWinsAcrossPages).
func TestFoldPage_pkChange_acrossPages(t *testing.T) {
	cases := []struct {
		name  string
		pages [][]query.ResultRow
		want  map[int]string
	}{
		{"C split", [][]query.ResultRow{{pkUpd(1, 100, 1, 2, "moved")}, {pkIns(2, 200, 1, "reinserted")}}, map[int]string{1: "reinserted", 2: "moved"}},
		{"D split", [][]query.ResultRow{{pkUpd(1, 100, 1, 2, "moved")}, {pkUpd(2, 200, 2, 3, "moved again")}}, map[int]string{1: "", 2: "", 3: "moved again"}},
		{"E split", [][]query.ResultRow{{pkDel(1, 100, 2)}, {pkUpd(2, 200, 1, 2, "took its place")}}, map[int]string{1: "", 2: "took its place"}},
		{"F split", [][]query.ResultRow{{pkUpd(1, 100, 1, 2, "away")}, {pkUpd(2, 200, 2, 1, "back")}}, map[int]string{1: "back", 2: ""}},
		{"A split, three pages", [][]query.ResultRow{{pkIns(1, 50, 5, "five")}, {pkUpd(2, 100, 1, 2, "moved")}, {pkDel(3, 200, 2)}}, map[int]string{1: "", 2: "", 5: "five"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wantState(t, foldPK(t, c.pages...), c.want)
		})
	}
}

// Rows with no binary log coordinate (archives from before positions were
// stored, pre-#1180 MariaDB positions): LaterInBinlog cannot order them and
// arrival order rules, for the halves as for any row. C and E arrive in
// their real order here.
func TestFoldPage_pkChange_noCoordinatesArrivalOrder(t *testing.T) {
	strip := func(evs ...query.ResultRow) []query.ResultRow {
		for i := range evs {
			evs[i].BinlogFile, evs[i].StartPos, evs[i].EndPos = "", 0, 0
		}
		return evs
	}
	wantState(t, foldPK(t, strip(pkUpd(1, 0, 1, 2, "moved"), pkIns(2, 0, 1, "reinserted"))), map[int]string{1: "reinserted", 2: "moved"})
	wantState(t, foldPK(t, strip(pkDel(1, 0, 2), pkUpd(2, 0, 1, 2, "took its place"))), map[int]string{1: "", 2: "took its place"})
	wantState(t, foldPK(t, strip(pkUpd(1, 0, 1, 2, "moved")), strip(pkDel(2, 0, 2))), map[int]string{1: "", 2: ""})
}

// I: the #2151 shape with a PK change. A DELETE of key 2 that waited on the
// row lock the UPDATE 1→2 held commits AFTER it (pos 300) but started earlier,
// so it arrives first. Key 2 must keep the DELETE: the synthetic INSERT sits at
// the UPDATE's position (100), which is earlier in the binary log.
func TestFoldPage_pkChange_outOfOrderByLock(t *testing.T) {
	d := pkDel(2, 300, 2)
	d.EventTimestamp = foldBase
	u := pkUpd(1, 100, 1, 2, "moved")
	u.EventTimestamp = foldBase.Add(2e9)
	for _, pages := range [][][]query.ResultRow{{{d, u}}, {{d}, {u}}} {
		wantState(t, foldPK(t, pages...), map[int]string{1: "", 2: ""})
	}
	// The mirror: the INSERT of key 1 that waited on the lock commits after
	// the UPDATE 1→2 and arrives first; key 1 must keep the INSERT.
	i := pkIns(2, 300, 1, "reinserted")
	i.EventTimestamp = foldBase
	for _, pages := range [][][]query.ResultRow{{{i, u}}, {{i}, {u}}} {
		wantState(t, foldPK(t, pages...), map[int]string{1: "reinserted", 2: "moved"})
	}
}

// spellingRefusal asserts the #2501 refusal of a split the index's spelling
// does not prove: it names both keys, the stored spelling and the remedy.
func spellingRefusal(t *testing.T, err error, before, after string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected the spelling refusal, got nil")
	}
	for _, want := range []string{"PK-changing UPDATE", "mydb.orders", fmt.Sprintf("%q", before), fmt.Sprintf("%q", after), "cannot follow", "bintrail baseline", "UUID"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
	if strings.Contains(err.Error(), "before-image primary key") {
		t.Errorf("the spelling refusal must not blame the before-image fold, which is not the cause any more:\n%s", err)
	}
}

// pkRefusal asserts today's #782 refusal text, unchanged: what the
// PostgreSQL gate and the three untouched paths say.
func pkRefusal(t *testing.T, err error, before, after string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected the PK-changing UPDATE refusal, got nil")
	}
	for _, want := range []string{"PK-changing UPDATE", "mydb.orders", fmt.Sprintf("%q", before), fmt.Sprintf("%q", after), "bintrail baseline"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
}

// J and the per-type table: the split happens only when the decoded
// before-image spells the key exactly as the parser stored it, which proves
// the synthetic INSERT's key (spelled from the decoded after-image) is what a
// later real event on that key carries, AND when every decoded value of the
// new key is one formatPKValue spells the same way from a string as from the
// parser's bytes: a scalar (a JSON container the indexer promoted prints as
// `[1 2]`, not `[1,2]`) and, for a string, valid UTF-8 (a TEXT key decodes
// to a Go string, on which the hex rule for invalid bytes never applies).
// Each row builds the stored key the way the parser does (event.BuildPKValues
// over the Go values go-mysql and metadata.MapRow deliver), stores the images
// the way the indexer does (encoding/json) and decodes them the way the fold
// does (query.UnmarshalRowImage, then decodeImageBinaries / the MariaDB
// fixed-width renderer). The rows are asserted from those literal shapes,
// not from captured rows: the scripted oracle (TestOracle_pkChangeShapes_2501)
// is where INT, composite, VARCHAR and DATETIME keys go through real capture.
func TestFoldPage_pkChange_spellingProofPerType(t *testing.T) {
	type row struct {
		name   string
		pkCols []metadata.ColumnMeta
		// parser-side images (go-mysql + MapRow types)
		before, after map[string]any
		// decode: how the fold's page decoder sees the images; nil = as stored
		decode func(map[string]any)
		split  bool
	}
	colsOf := func(cols ...metadata.ColumnMeta) []metadata.ColumnMeta { return cols }
	bin := []byte{0xB2, 0x81, 0xC3, 0x28} // not UTF-8: spelled 0xB281C328
	bin2 := []byte{0xB2, 0x82, 0xC3, 0x28}
	decodeBinary := func(m map[string]any) { decodeImageBinaries(m, map[string]bool{"id": true}) }
	decodeText := func(m map[string]any) { decodeImageBinaries(m, map[string]bool{"id": false}) }
	// The indexer promotes TEXT bytes that look like a JSON container to a
	// JSON value (marshalRow), which decodes as []any.
	decodeTextJSON := func(m map[string]any) {
		decodeText(m)
		if m["id"] == "[1,2]" {
			m["id"] = []any{json.Number("1"), json.Number("2")}
		}
	}
	// A MariaDB UUID key: the parser pads it to 16 bytes and stores them raw
	// or hex; the fold's decoder renders the stored base64 as the server's
	// text form (renderImageMariaDBFixed), which never equals the stored key.
	uuid1 := []byte{0xB2, 0x81, 0xC3, 0x28, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	uuid2 := []byte{0xB2, 0x82, 0xC3, 0x28, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	decodeUUID := func(m map[string]any) { renderImageMariaDBFixed(m, map[string]string{"id": "uuid"}) }
	float := func(m map[string]any) {
		for k, v := range m {
			if n, ok := v.(json.Number); ok {
				f, _ := n.Float64()
				m[k] = f
			}
		}
	}
	rows := []row{
		{"int", colsOf(metadata.ColumnMeta{Name: "id", DataType: "int"}),
			map[string]any{"id": int32(1), "v": "a"}, map[string]any{"id": int32(2), "v": "a"}, nil, true},
		{"composite int", colsOf(metadata.ColumnMeta{Name: "a", DataType: "int"}, metadata.ColumnMeta{Name: "b", DataType: "int"}),
			map[string]any{"a": int32(1), "b": int32(5)}, map[string]any{"a": int32(1), "b": int32(6)}, nil, true},
		{"varchar with | and \\", colsOf(metadata.ColumnMeta{Name: "id", DataType: "varchar"}),
			map[string]any{"id": `x|y\z`}, map[string]any{"id": `p|q\r`}, nil, true},
		{"binary(4) decoded by the epoch", colsOf(metadata.ColumnMeta{Name: "id", DataType: "binary"}),
			map[string]any{"id": bin}, map[string]any{"id": bin2}, decodeBinary, true},
		{"binary(4) left as stored base64 (decode degraded)", colsOf(metadata.ColumnMeta{Name: "id", DataType: "binary"}),
			map[string]any{"id": bin}, map[string]any{"id": bin2}, nil, false},
		{"text key, both keys ASCII", colsOf(metadata.ColumnMeta{Name: "id", DataType: "text"}),
			map[string]any{"id": []byte("cafe")}, map[string]any{"id": []byte("cafes")}, decodeText, true},
		// A latin1 byte in the NEW key: the parser stores the later DELETE of
		// that key as 0x636166E9, but the decoded string spells it raw.
		{"text key, new key holds a byte that is not UTF-8", colsOf(metadata.ColumnMeta{Name: "id", DataType: "text"}),
			map[string]any{"id": []byte("cafe")}, map[string]any{"id": []byte{'c', 'a', 'f', 0xE9}}, decodeText, false},
		{"text key, new key looks like a JSON container", colsOf(metadata.ColumnMeta{Name: "id", DataType: "text"}),
			map[string]any{"id": []byte("k1")}, map[string]any{"id": []byte("[1,2]")}, decodeTextJSON, false},
		{"mariadb uuid key rendered as text", colsOf(metadata.ColumnMeta{Name: "id", DataType: "uuid"}),
			map[string]any{"id": uuid1}, map[string]any{"id": uuid2}, decodeUUID, false},
		{"datetime", colsOf(metadata.ColumnMeta{Name: "id", DataType: "datetime"}),
			map[string]any{"id": "2026-04-01 08:00:00"}, map[string]any{"id": "2026-04-01 08:00:01"}, nil, true},
		{"decimal", colsOf(metadata.ColumnMeta{Name: "id", DataType: "decimal"}),
			map[string]any{"id": "12.50"}, map[string]any{"id": "12.51"}, nil, true},
		{"bigint unsigned above 2^53", colsOf(metadata.ColumnMeta{Name: "id", DataType: "bigint", ColumnType: "bigint unsigned"}),
			map[string]any{"id": uint64(18446744073709551615)}, map[string]any{"id": uint64(18446744073709551614)}, nil, true},
		// Past 2^53 a float64 image cannot spell the key: "%v" prints
		// 9.007199254740992e+15, not 9007199254740993 (and 2^64-1 and 2^64-2
		// would even be ONE float, so no change would be seen at all).
		{"bigint above 2^53 decoded as float64", colsOf(metadata.ColumnMeta{Name: "id", DataType: "bigint", ColumnType: "bigint unsigned"}),
			map[string]any{"id": uint64(9007199254740993)}, map[string]any{"id": uint64(9007199254740995)}, float, false},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			stored := event.BuildPKValues(r.pkCols, r.before)
			newKey := event.BuildPKValues(r.pkCols, r.after)
			if stored == newKey {
				t.Fatal("fixture: the key did not change")
			}
			decode := func(m map[string]any) map[string]any {
				b, err := json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				out := query.UnmarshalRowImage(b)
				if r.decode != nil {
					r.decode(out)
				}
				return out
			}
			ev := query.ResultRow{EventID: 1, BinlogFile: pkFile, StartPos: 100, EventTimestamp: foldBase,
				SchemaName: "mydb", TableName: "orders", EventType: event.EventUpdate, PKValues: stored,
				RowBefore: decode(r.before), RowAfter: decode(r.after)}
			res := &foldResult{Changes: map[string]*query.ResultRow{}}
			err := foldPage([]query.ResultRow{ev}, "mydb", "orders", r.pkCols, nil, foldOptions{}, res)
			if !r.split {
				spellingRefusal(t, err, stored, event.BuildPKValues(r.pkCols, ev.RowAfter))
				if len(res.Changes) != 0 {
					t.Errorf("a refused page left %d entries", len(res.Changes))
				}
				return
			}
			if err != nil {
				t.Fatalf("foldPage: %v", err)
			}
			// A later real event on the new key, spelled by the parser, must
			// land on the synthetic INSERT's entry: one key, not two spellings.
			later := query.ResultRow{EventID: 2, BinlogFile: pkFile, StartPos: 200, EventTimestamp: foldBase,
				SchemaName: "mydb", TableName: "orders", EventType: event.EventDelete, PKValues: newKey, RowBefore: decode(r.after)}
			if err := foldPage([]query.ResultRow{later}, "mydb", "orders", r.pkCols, nil, foldOptions{}, res); err != nil {
				t.Fatalf("foldPage(later): %v", err)
			}
			if len(res.Changes) != 2 {
				t.Fatalf("change map holds %v, want exactly the old key %q dead and the new key %q dead", keysOf(res.Changes), stored, newKey)
			}
			for _, k := range []string{stored, newKey} {
				if ev := res.Changes[k]; ev == nil || ev.EventType != event.EventDelete {
					t.Errorf("key %q = %+v, want a DELETE", k, ev)
				}
			}
		})
	}
}

// K: a PostgreSQL window keeps refusing. Its rows carry no binary log
// coordinate (an LSN), so the ordering rule the split relies on does not
// apply, and its before-image may hold only the key.
func TestFoldPage_pkChange_postgresRefuses(t *testing.T) {
	u := pkUpd(1, 100, 1, 2, "moved")
	u.BinlogFile, u.StartPos, u.EndPos = "0/16B3748", 0, 0
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	err := foldPage([]query.ResultRow{u}, "mydb", "orders", pkColsIntID(), nil, foldOptions{pg: true}, res)
	pkRefusal(t, err, "1", "2")
	if len(res.Changes) != 0 {
		t.Errorf("a refused page left %d entries", len(res.Changes))
	}
}

// The touched hook reports every key a page wrote or declined to write,
// both keys of a PK change included: what a tail marks dirty.
func TestFoldPage_pkChange_touchedReportsBothKeys(t *testing.T) {
	var touched []string
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	page := []query.ResultRow{pkUpd(1, 100, 1, 2, "moved"), pkUpd(2, 50, 3, 3, "older")}
	if err := foldPage(page, "mydb", "orders", pkColsIntID(), nil, foldOptions{touched: func(k string) { touched = append(touched, k) }}, res); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(touched, ","); got != "1,2,3" {
		t.Errorf("touched %q, want 1,2,3", got)
	}
}

// L: the refresh's arithmetic (#2505) closes over the memo's three windows
// when the PK change counts once as an UPDATE, with the pair's numbers taken
// from the fold's change map the way the writer takes them (keys, alive rows,
// base positions found), before and after a trip through the spill.
func TestRefreshArithmetic_pkChange_2501(t *testing.T) {
	// base holds keys 1 and 2 (or only 1): the lookup finds a touched key
	// there, which is a Dead row of the pair.
	numbers := func(changes map[string]*query.ResultRow, base map[int]bool) (keys, upserts, dead int64) {
		for k, ev := range changes {
			keys++
			if ev.EventType != event.EventDelete {
				upserts++
			}
			var id int
			fmt.Sscan(k, &id)
			if base[id] {
				dead++
			}
		}
		return
	}
	cases := []struct {
		name string
		page []query.ResultRow
		base map[int]bool
	}{
		{"1→2 alone", []query.ResultRow{pkUpd(1, 100, 1, 2, "moved")}, map[int]bool{1: true}},
		{"1→2; DELETE 2", []query.ResultRow{pkUpd(1, 100, 1, 2, "moved"), pkDel(2, 200, 2)}, map[int]bool{1: true}},
		{"DELETE 2; 1→2", []query.ResultRow{pkDel(1, 100, 2), pkUpd(2, 200, 1, 2, "moved")}, map[int]bool{1: true, 2: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := foldPK(t, c.page)
			check := func(stage string, changes map[string]*query.ResultRow) {
				t.Helper()
				keys, upserts, dead := numbers(changes, c.base)
				a := refreshArithmetic{Pair: true, Seq: 0, RowsBefore: int64(len(c.base)), Inserts: res.Inserts, Deletes: res.Deletes,
					Keys: keys, Upserts: upserts, Dead: dead}
				if p := a.problems(); len(p) != 0 {
					t.Errorf("%s: %v (keys %d upserts %d dead %d, inserts %d deletes %d)", stage, p, keys, upserts, dead, res.Inserts, res.Deletes)
				}
				// The rewrite's view of the same window: a base row the window
				// deletes is skipped, a key the base lacks is appended, a key it
				// holds is replaced.
				var skipped, replaced, appended int64
				for k, ev := range changes {
					var id int
					fmt.Sscan(k, &id)
					switch {
					case ev.EventType == event.EventDelete && c.base[id]:
						skipped++
					case ev.EventType == event.EventDelete:
						// A dead key the base never held: the merge never
						// meets it.
					case c.base[id]:
						replaced++
					default:
						appended++
					}
				}
				passed := int64(len(c.base)) - skipped - replaced
				r := refreshArithmetic{Rewrite: true, RowsBefore: int64(len(c.base)), Inserts: res.Inserts, Deletes: res.Deletes,
					BaselineRows: passed, UpdatesApplied: replaced, DeletesSkipped: skipped, InsertsEmitted: appended, RowsWritten: passed + replaced + appended}
				if p := r.problems(); len(p) != 0 {
					t.Errorf("%s (rewrite): %v", stage, p)
				}
			}
			check("in memory", res.Changes)
			spilled := loadAll(t, spillOf(t, 10, res.Changes))
			if len(spilled) != len(res.Changes) {
				t.Fatalf("the spill holds %d entries, the map %d", len(spilled), len(res.Changes))
			}
			for k, ev := range res.Changes {
				got := spilled[k]
				if got == nil || got.EventType != ev.EventType || got.EventID != ev.EventID || (got.RowAfter == nil) != (ev.RowAfter == nil) {
					t.Errorf("key %q came back as %+v, went in as %+v", k, got, ev)
				}
			}
			check("after the spill", spilled)
		})
	}
}

// The spill keeps both synthetic rows apart, each under its own key, and
// a later drain on either key wins as for any row.
func TestChangeSpill_pkChange_2501(t *testing.T) {
	first := foldPK(t, []query.ResultRow{pkUpd(1, 100, 1, 2, "moved")})
	second := foldPK(t, []query.ResultRow{pkDel(2, 200, 2), pkIns(3, 300, 1, "reinserted")})
	got := loadAll(t, spillOf(t, 10, first.Changes, second.Changes))
	if ev := got[pkStrForInt(1)]; ev == nil || ev.EventType != event.EventInsert || ev.RowAfter["status"] != "reinserted" {
		t.Errorf("key 1 = %+v, want the INSERT of the second drain", ev)
	}
	if ev := got[pkStrForInt(2)]; ev == nil || ev.EventType != event.EventDelete || ev.RowAfter != nil {
		t.Errorf("key 2 = %+v, want the DELETE of the second drain", ev)
	}
	if len(got) != 2 {
		t.Errorf("read back %d entries, want 2", len(got))
	}
}

// M: the tail (#2369) folds a PK change and marks BOTH keys changed, so a
// delta segment carries the tombstone of the old key and the row under the
// new one, and the view reads the moved row once.
func TestTail_pkChange_2501(t *testing.T) {
	dir := t.TempDir()
	base := writeTailBase(t, dir, "orders.parquet", tailOrdersDDL,
		map[string]string{"id": "1", "status": "one"},
		map[string]string{"id": "2", "status": "two"},
		map[string]string{"id": "3", "status": "three"})
	published := "SELECT * FROM read_parquet(" + lit(base) + ")"
	tail, err := NewTail("shop", "orders", tailOrdersDDL, base, tailOrdersPK)
	if err != nil {
		t.Fatal(err)
	}
	moved := tailEvent(4, event.EventUpdate, "5", "moved", 30)
	moved.RowBefore = map[string]any{"id": json.Number("1"), "status": "one"}
	moved.PKValues = "1"
	if err := tail.Apply([]query.ResultRow{moved}); err != nil {
		t.Fatalf("a key-changing UPDATE was refused: %v", err)
	}
	if tail.Keys() != 2 || tail.Dirty() != 2 {
		t.Fatalf("keys %d dirty %d, want 2 and 2: the old key's tombstone and the new key's row", tail.Keys(), tail.Dirty())
	}
	seg := filepath.Join(dir, "orders.tail.4.parquet")
	if _, err := tail.WriteDelta(seg); err != nil {
		t.Fatal(err)
	}
	if tail.Dirty() != 0 {
		t.Errorf("dirty %d after the write", tail.Dirty())
	}
	rows := readTailRows(t, seg)
	if want := "1=d,5=u"; rows != want {
		t.Errorf("segment rows %q, want %q (tombstone of 1, row under 5)", rows, want)
	}
	got := readState(t, baseline.TableTailStateSQL(published, lit(seg), tail.PKColumns(), ""))
	if want := "2=two,3=three,5=moved"; strings.Join(got, ",") != want {
		t.Errorf("state %v, want %s", got, want)
	}
	// Then the moved row moves back on a later page: 5→1. Both keys dirty again.
	back := tailEvent(5, event.EventUpdate, "1", "back", 40)
	back.RowBefore = map[string]any{"id": json.Number("5"), "status": "moved"}
	back.PKValues = "5"
	if err := tail.Apply([]query.ResultRow{back}); err != nil {
		t.Fatal(err)
	}
	if tail.Dirty() != 2 {
		t.Fatalf("dirty %d, want 2", tail.Dirty())
	}
	seg2 := filepath.Join(dir, "orders.tail.5.parquet")
	if _, err := tail.WriteDelta(seg2); err != nil {
		t.Fatal(err)
	}
	if rows := readTailRows(t, seg2); rows != "1=u,5=d" {
		t.Errorf("second segment rows %q, want 1=u,5=d", rows)
	}
	got = readState(t, baseline.TableTailChainStateSQL(published, []string{lit(seg), lit(seg2)}, tail.PKColumns(), ""))
	if want := "1=back,2=two,3=three"; strings.Join(got, ",") != want {
		t.Errorf("state over the chain %v, want %s", got, want)
	}
}

// readTailRows prints a tail segment's rows as "key=op", in key order.
func readTailRows(t *testing.T, path string) string {
	t.Helper()
	got := readState(t, fmt.Sprintf("SELECT CAST(%q AS VARCHAR) AS id, %q AS status FROM read_parquet(%s)", baseline.TableDeltaPKColumn, baseline.TableDeltaOpColumn, lit(path)))
	return strings.Join(got, ",")
}

// The refusal the PostgreSQL gate and the three untouched paths keep is the
// #782 error, text unchanged; the spelling refusal is its own text, naming
// the stored and the decoded spelling and what to do.
func TestPKChangingUpdateErr_isUnchanged(t *testing.T) {
	err := pkChangingUpdateErr("mydb", "orders", "1", "2")
	pkRefusal(t, err, "1", "2")
	if errors.Unwrap(err) != nil {
		t.Error("the refusal must stay a plain error")
	}
	err = pkSpellingErr("mydb", "orders", "0xB281", "0xB282", `the index stores the old key as "0xB281" but its row image decodes as "sYE="`)
	spellingRefusal(t, err, "0xB281", "0xB282")
	if !strings.Contains(err.Error(), `"sYE="`) {
		t.Errorf("the spelling refusal must show the decoded spelling:\n%s", err)
	}
}
