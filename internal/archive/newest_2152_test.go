package archive

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// What an archive records about the newest change it holds (#2152): the
// highest event_id, and the highest binlog coordinate under the order the
// anchored fetch's own predicate uses (file by length, then by name, then the
// start position). Each case is one way a partition's rows can look.
func TestNewestAdd_2152(t *testing.T) {
	file := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	pos := func(p uint64) sql.Null[uint64] { return sql.Null[uint64]{V: p, Valid: true} }
	type row struct {
		id   uint64
		file sql.NullString
		pos  sql.Null[uint64]
	}
	cases := []struct {
		name     string
		rows     []row
		wantID   uint64
		wantPos  bool
		wantFile string
		wantAt   uint64
	}{
		{name: "no rows: recorded, nothing an anchored fetch can return", wantID: 0},
		{name: "same file, the higher position wins",
			rows:   []row{{1, file("binlog.000007"), pos(500)}, {2, file("binlog.000007"), pos(900)}},
			wantID: 2, wantPos: true, wantFile: "binlog.000007", wantAt: 900},
		{name: "a later file beats a higher position in an earlier one",
			rows:   []row{{1, file("binlog.000007"), pos(900)}, {2, file("binlog.000008"), pos(4)}},
			wantID: 2, wantPos: true, wantFile: "binlog.000008", wantAt: 4},
		{name: "the suffix rollover: the longer name is the later file",
			rows:   []row{{1, file("binlog.1000000"), pos(4)}, {2, file("binlog.999999"), pos(900)}},
			wantID: 2, wantPos: true, wantFile: "binlog.1000000", wantAt: 4},
		{name: "the newest id is not the newest position (file indexing added old rows last)",
			rows:   []row{{1, file("binlog.000009"), pos(300)}, {9, file("binlog.000002"), pos(800)}},
			wantID: 9, wantPos: true, wantFile: "binlog.000009", wantAt: 300},
		{name: "a row with an empty file name cannot be returned by an anchored fetch",
			rows:   []row{{1, file(""), pos(99999)}, {2, file("binlog.000003"), pos(10)}},
			wantID: 2, wantPos: true, wantFile: "binlog.000003", wantAt: 10},
		{name: "a NULL file is skipped too",
			rows:   []row{{5, sql.NullString{}, pos(99999)}},
			wantID: 5},
		{name: "only empty names: recorded, no coordinate",
			rows:   []row{{3, file(""), pos(1)}},
			wantID: 3},
		{name: "a NULL start position counts as 0 within its file",
			rows:   []row{{1, file("binlog.000004"), pos(70)}, {2, file("binlog.000005"), sql.Null[uint64]{}}},
			wantID: 2, wantPos: true, wantFile: "binlog.000005", wantAt: 0},
		{name: "an underflowed start position is a number the predicate compares as such",
			rows:   []row{{1, file("binlog.000004"), pos(70)}, {2, file("binlog.000004"), pos(1<<64 - 50)}},
			wantID: 2, wantPos: true, wantFile: "binlog.000004", wantAt: 1<<64 - 50},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var n Newest
			for _, r := range tc.rows {
				n.Add(r.id, r.file, r.pos)
			}
			if n.EventID != tc.wantID || n.HasPos != tc.wantPos || n.File != tc.wantFile || n.Pos != tc.wantAt {
				t.Fatalf("got id=%d hasPos=%v %s:%d; want id=%d hasPos=%v %s:%d",
					n.EventID, n.HasPos, n.File, n.Pos, tc.wantID, tc.wantPos, tc.wantFile, tc.wantAt)
			}
		})
	}
}

// ReadContent reads the same facts back from a Parquet file, for the paths
// that register a file they did not write (reconcile --repair, restore-index).
// It must agree with the fold ArchivePartition runs while writing: the cases
// that can disagree are the order (rollover, newest id with an old position)
// and the rows the anchored fetch never returns.
func TestReadContent_matchesFold_2152(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p_2026021900.parquet")
	w, err := baseline.NewWriter(path, BinlogEventColumns, baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	type r struct {
		id       uint64
		file     string
		fileNull bool
		pos      uint64
		posNull  bool
		ts       string
	}
	in := []r{
		{id: 10, file: "binlog.999999", pos: 900, ts: "2026-02-19 00:10:00"},
		{id: 11, file: "binlog.1000000", pos: 4, ts: "2026-02-18 21:00:00"},
		{id: 12, file: "", pos: 777777, ts: "2026-02-19 00:20:00"},
		{id: 13, fileNull: true, pos: 888888, ts: "2026-02-19 00:30:00"},
		{id: 40, file: "binlog.000002", pos: 123, ts: "2026-02-19 00:59:59"},
		{id: 14, file: "binlog.1000000", posNull: true, ts: "2026-02-19 00:40:00"},
	}
	var fold Newest
	for _, x := range in {
		vals := make([]string, len(BinlogEventColumns))
		nulls := make([]bool, len(BinlogEventColumns))
		for i := range nulls {
			nulls[i] = true
		}
		set := func(i int, v string) { vals[i], nulls[i] = v, false }
		set(0, strconv.FormatUint(x.id, 10))
		if !x.fileNull {
			set(1, x.file)
		}
		if !x.posNull {
			set(2, strconv.FormatUint(x.pos, 10))
		}
		set(4, x.ts)
		if err := w.WriteRow(vals, nulls); err != nil {
			t.Fatal(err)
		}
		fold.Add(x.id, sql.NullString{String: x.file, Valid: !x.fileNull}, sql.Null[uint64]{V: x.pos, Valid: !x.posNull})
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := ReadContent(context.Background(), path)
	if err != nil {
		t.Fatalf("ReadContent: %v", err)
	}
	if got.Newest != fold {
		t.Fatalf("ReadContent newest = %+v; the fold while writing = %+v", got.Newest, fold)
	}
	if want := (Newest{EventID: 40, File: "binlog.1000000", Pos: 4, HasPos: true}); fold != want {
		t.Fatalf("fold = %+v, want %+v", fold, want)
	}
	if want := time.Date(2026, 2, 18, 21, 0, 0, 0, time.UTC); !got.MinEventTS.Equal(want) {
		t.Errorf("MinEventTS = %v, want %v", got.MinEventTS, want)
	}
	if want := time.Date(2026, 2, 19, 0, 59, 59, 0, time.UTC); !got.MaxEventTS.Equal(want) {
		t.Errorf("MaxEventTS = %v, want %v", got.MaxEventTS, want)
	}
}

// An archive with no rows is recorded (event id 0), never "unknown": the
// floor reads an unrecorded archive as one that may hold anything.
func TestReadContent_emptyFile_2152(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p_2026021900.parquet")
	w, err := baseline.NewWriter(path, BinlogEventColumns, baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := ReadContent(context.Background(), path)
	if err != nil {
		t.Fatalf("ReadContent: %v", err)
	}
	if got.Newest != (Newest{}) || !got.MinEventTS.IsZero() || !got.MaxEventTS.IsZero() {
		t.Fatalf("ReadContent on an empty archive = %+v, want the zero value", got)
	}
}
