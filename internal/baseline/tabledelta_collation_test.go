package baseline

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/parquet-go/parquet-go"
)

// The state view partitions upserts by bintrail_pk, a text column. The
// console's SQL sandbox runs with default_collation = 'nocase.icu_noaccent'
// (#2038); without COLLATE C on the partition, two keys that differ only in
// case ('abc' and 'ABC', a _bin or VARBINARY primary key) fold into one
// partition and the older upsert vanishes from the table, with no error.
func TestTableDeltaState_keysDifferingInCaseSurviveNocaseSession(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "2026-10-04T00-00-00Z", "shop")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "tokens.parquet")
	cols := []Column{
		{Name: "tok", MySQLType: "varchar(16)", ParquetType: parquet.String()},
		{Name: "n", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	w, err := NewWriter(base, cols, WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{MetaKeyBinlogFile: "b.1", MetaKeyBinlogPos: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"zzz", "0"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	upsCols, err := TableDeltaColumns(cols)
	if err != nil {
		t.Fatal(err)
	}
	idx := map[string]int{}
	for i, c := range upsCols {
		idx[c.Name] = i
	}
	row := func(tok, n string) []string {
		r := make([]string, len(upsCols))
		r[idx["tok"]], r[idx["n"]], r[idx[TableDeltaPKColumn]], r[idx[TableDeltaOpColumn]] = tok, n, tok, TableDeltaOpUpsert
		return r
	}
	nulls := make([]bool, len(upsCols))
	md := map[string]string{MetaKeyBinlogFile: "b.1", MetaKeyBinlogPos: "2", MetaKeyDeltaBaseAnchor: "b.1:1", MetaKeyDeltaBaseSize: "1"}
	err = WriteTableDeltaPair(base, 0, cols, md, nil, func(emit func([]string, []bool) error) error {
		if err := emit(row("abc", "1"), nulls); err != nil {
			return err
		}
		return emit(row("ABC", "2"), nulls)
	})
	if err != nil {
		t.Fatal(err)
	}
	posdel, upserts := TableDeltaGlobs(base)
	q := "SELECT count(*) FROM (" + TableDeltaStateSQL("'"+base+"'", "'"+posdel+"'", "'"+upserts+"'", base, "") + ")"
	for _, session := range []string{"", "SET default_collation = 'nocase.noaccent'", "SET default_collation = 'nocase.icu_noaccent'"} {
		ddb, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		if session != "" {
			if _, err := ddb.Exec(session); err != nil {
				t.Fatal(err)
			}
		}
		var n int
		if err := ddb.QueryRow(q).Scan(&n); err != nil {
			t.Fatalf("%s: %v", session, err)
		}
		ddb.Close()
		if n != 3 {
			t.Errorf("session %q: state has %d rows, want 3 (zzz, abc and ABC): a case-folding collation reached the key partition", session, n)
		}
	}
}
