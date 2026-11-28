package query

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// A snapshot read of a file that stores columns typed (#3315, the footer
// key) reads every column as text; any other file is read as it is.
func TestPGScanSourceReadsTypedFilesAsText(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write := func(md map[string]string) string {
		path := filepath.Join(t.TempDir(), "t.parquet")
		type row struct {
			ID string `parquet:"id"`
			N  int32  `parquet:"n"`
		}
		opts := []parquet.WriterOption{}
		for k, v := range md {
			opts = append(opts, parquet.KeyValueMetadata(k, v))
		}
		if err := parquet.WriteFile(path, []row{{"1", 42}}, opts...); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for _, tc := range []struct {
		md   map[string]string
		want string
	}{
		{nil, "INTEGER"},
		{map[string]string{pgTypedColumnsKey: `{"v":1,"columns":{"n":"INTEGER"}}`}, "VARCHAR"},
	} {
		src, err := pgScanSource(context.Background(), db, "'"+write(tc.md)+"'")
		if err != nil {
			t.Fatal(err)
		}
		var id, typ string
		if err := db.QueryRow("SELECT id, typeof(n) FROM "+src).Scan(&id, &typ); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual([]string{id, typ}, []string{"1", tc.want}) {
			t.Fatalf("%v: %s %s", tc.md, id, typ)
		}
	}
	// A list of another version is refused, never read through the cast.
	if _, err := pgScanSource(context.Background(), db, "'"+write(map[string]string{pgTypedColumnsKey: `{"v":2,"columns":{"n":"INTEGER"}}`})+"'"); err == nil {
		t.Fatal("a list of another version was read")
	}
}
