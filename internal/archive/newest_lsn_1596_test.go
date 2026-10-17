package archive

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A PostgreSQL archive's newest change is its highest LSN by number (#1596):
// by text length "0/FFFFFF00" would beat "1/100", and within one length
// "9/12345678" would beat "10/1234567".
func TestNewestAndReadContent_LSN(t *testing.T) {
	rows := []struct {
		id  uint64
		lsn uint64
	}{{1, 0xFFFFFF00}, {2, 1<<32 + 0x100}, {3, 9<<32 + 0x12345678}, {4, 0x10<<32 + 0x1234567}}
	text := func(l uint64) string { return strconv.FormatUint(l>>32, 16) + "/" + strconv.FormatUint(l&0xFFFFFFFF, 16) }

	path := filepath.Join(t.TempDir(), "p_2026101700.parquet")
	w, err := baseline.NewWriter(path, BinlogEventColumns, baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	var fold Newest
	for _, r := range rows {
		vals := make([]string, len(BinlogEventColumns))
		nulls := make([]bool, len(BinlogEventColumns))
		for i := range nulls {
			nulls[i] = true
		}
		set := func(i int, v string) { vals[i], nulls[i] = v, false }
		set(0, strconv.FormatUint(r.id, 10))
		set(1, text(r.lsn))
		set(2, strconv.FormatUint(r.lsn, 10))
		set(4, "2026-10-17 00:10:00")
		if err := w.WriteRow(vals, nulls); err != nil {
			t.Fatal(err)
		}
		fold.Add(r.id, sql.NullString{String: text(r.lsn), Valid: true}, sql.Null[uint64]{V: r.lsn, Valid: true})
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	want := Newest{EventID: 4, File: text(0x10<<32 + 0x1234567), Pos: 0x10<<32 + 0x1234567, HasPos: true}
	if fold != want {
		t.Fatalf("fold = %+v, want %+v", fold, want)
	}
	got, err := ReadContent(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Newest != want {
		t.Fatalf("ReadContent newest = %+v, want %+v", got.Newest, want)
	}
}
