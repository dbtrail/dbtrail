package reconstruct

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

var pgCols = []string{"id", "created_at", "amount", "doc", "note"}

// pgMergeRel is the recorded shape of the merge tests' five-column table.
var pgMergeRel = baseline.PGRelation{OID: 16411, Columns: []baseline.PGColumn{
	{Name: "id", TypeOID: 23, TypeMod: -1, Type: "integer"},
	{Name: "created_at", TypeOID: 1184, TypeMod: -1, Type: "timestamp with time zone"},
	{Name: "amount", TypeOID: 1700, TypeMod: -1, Type: "numeric"},
	{Name: "doc", TypeOID: 3802, TypeMod: -1, Type: "jsonb"},
	{Name: "note", TypeOID: 25, TypeMod: -1, Type: "text"},
}}

// writePGBaseline writes a baseline the way pgbaseline does: every column
// RawText, the LSN floor, the shape and the rendering stamp.
func writePGBaseline(t *testing.T, dir string, rows [][]string, nulls [][]bool) string {
	t.Helper()
	rel, err := pgMergeRel.Encode()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "public", "orders.parquet")
	w, err := baseline.NewWriter(path, rawTextColumns(pgCols), baseline.WriterConfig{Compression: "none", RowGroupSize: 2,
		Metadata: map[string]string{
			baseline.MetaKeySnapshotTimestamp: "2026-10-17T00:00:00Z",
			baseline.MetaKeyLSN:               strconv.FormatUint(0x500, 10),
			baseline.MetaKeyPGRelation:        rel,
			baseline.MetaKeyRenderGUCs:        baseline.RenderGUCsPinned,
			baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
			baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
		}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if err := w.WriteRow(rows[i], nulls[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func pgPK() []metadata.ColumnMeta {
	return []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true}}
}

// A PostgreSQL fold end to end through mergeBaselineIntoParquet (#1596): the
// values are PostgreSQL's own text and come out byte for byte, NULL stays
// distinct from an empty string, the text PK joins the deltas, and the footer
// reads back as an LSN-anchored PostgreSQL baseline with no binlog anchor.
func TestMergeBaselineIntoParquet_PG(t *testing.T) {
	src := writePGBaseline(t, t.TempDir(), [][]string{
		{"1", "2026-10-17 01:02:03.123456+00", "12.3400", `{"a": [1, 2]}`, "ünïcode"},
		{"2", "2026-10-17 01:02:03+00", "0.00", `{}`, ""},
		{"3", "", "", "", ""},
		{"5", "", "", "", ""},
	}, [][]bool{{false, false, false, false, false}, {false, false, false, false, false}, {false, true, true, true, true},
		// Row 5 passes through untouched: an empty string, not NULL.
		{false, true, true, true, false}})
	srcMeta, err := baseline.ReadParquetMetadata(src)
	if err != nil {
		t.Fatal(err)
	}
	str := func(s string) any { return s }
	changes := map[string]*query.ResultRow{
		"2": {EventType: event.EventUpdate, PKValues: "2", RowAfter: map[string]any{
			"id": str("2"), "created_at": str("2026-10-17 05:00:00+00"), "amount": str("1e-7"), "doc": str(`{"b": null}`), "note": str("")}},
		"3": {EventType: event.EventDelete, PKValues: "3"},
		"4": {EventType: event.EventInsert, PKValues: "4", RowAfter: map[string]any{
			"id": str("4"), "created_at": str("infinity"), "amount": str("NaN"), "doc": str(`[]`), "note": str("x")}},
	}
	snapDir := t.TempDir()
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	rep := &TableReport{Schema: "public", Table: "orders"}
	in := mergeInput{LocalBaselinePath: src, Schema: "public", Table: "orders", PKCols: pgPK(), Changes: changes,
		SnapshotDir: snapDir, SnapshotAt: at, PG: true, PGCutLSN: 0x1000,
		SourceBaseline: baselineMeta{Path: src, Time: at.Add(-12 * time.Hour), Metadata: srcMeta}}
	if err := mergeBaselineIntoParquet(context.Background(), in, rep); err != nil {
		t.Fatalf("mergeBaselineIntoParquet: %v", err)
	}
	out := filepath.Join(snapDir, "public", "orders.parquet")

	// Read the written file directly, not through the merge's own reader.
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	rs, err := duck.Query(`SELECT id, created_at, amount, doc, note FROM parquet_scan('` + out + `') ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	type cell = sql.NullString
	got := map[string][4]cell{}
	for rs.Next() {
		var id string
		var c [4]cell
		if err := rs.Scan(&id, &c[0], &c[1], &c[2], &c[3]); err != nil {
			t.Fatal(err)
		}
		got[id] = c
	}
	rs.Close()
	v := func(s string) cell { return cell{String: s, Valid: true} }
	null := cell{}
	want := map[string][4]cell{
		"1": {v("2026-10-17 01:02:03.123456+00"), v("12.3400"), v(`{"a": [1, 2]}`), v("ünïcode")},
		"2": {v("2026-10-17 05:00:00+00"), v("1e-7"), v(`{"b": null}`), v("")},
		"4": {v("infinity"), v("NaN"), v(`[]`), v("x")},
		"5": {null, null, null, v("")},
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want 1, 2, 4 and 5 (3 deleted)", got)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("row %s = %+v, want %+v", id, got[id], w)
		}
	}

	meta, err := baseline.ReadParquetMetadata(out)
	if err != nil {
		t.Fatal(err)
	}
	if meta.LSN != 0x1000 || meta.PGRelation != srcMeta.PGRelation || meta.RenderGUCs != baseline.RenderGUCsPinned ||
		meta.BinlogFile != "" || meta.BinlogPos != 0 || meta.CreateTableSQL != "" || meta.EventMark != "" {
		t.Fatalf("footer = LSN %d rel %v gucs %q binlog %q:%d create %q mark %q",
			meta.LSN, meta.PGRelation == srcMeta.PGRelation, meta.RenderGUCs, meta.BinlogFile, meta.BinlogPos, meta.CreateTableSQL, meta.EventMark)
	}
	if AnchorOf(meta) != nil {
		t.Error("a PostgreSQL fold has a binlog anchor: the next run would order its LSN as a file name")
	}
}
