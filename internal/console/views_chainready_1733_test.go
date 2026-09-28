package console

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestViewsAPI_realFootersReachTheChainRule_1733 is the download's half of
// the CLI test of the same name: over real baseline files, the footer read
// (through this server's memo, asked twice so the cached answer is used too)
// decides each table's body. An ordinary table gets the chain-aware body, a
// table with a column a delta reserves keeps reading its file.
func TestViewsAPI_realFootersReachTheChainRule_1733(t *testing.T) {
	dir := t.TempDir()
	snap := filepath.Join(dir, "2026-06-10T12-00-00Z")
	for name, create := range map[string]string{
		"orders": "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(8) DEFAULT NULL\n);\n",
		"odd":    "CREATE TABLE `odd` (\n  `id` int NOT NULL,\n  `filename` varchar(8) DEFAULT NULL\n);\n",
	} {
		cols, err := baseline.ParseSchemaText(create)
		if err != nil {
			t.Fatal(err)
		}
		w, err := baseline.NewWriter(filepath.Join(snap, "shop", name+".parquet"), cols, baseline.WriterConfig{
			Compression: "none", RowGroupSize: 100, Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: create}})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow([]string{"1", "a"}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(snap, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baseline.PublishCurrentPointer(snap); err != nil {
		t.Fatal(err)
	}
	srv := newViewsServer(t, dir, false)
	for _, round := range []string{"first", "cached"} {
		rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
		if rec.Code != 200 {
			t.Fatalf("%s: code = %d, body = %s", round, rec.Code, body)
		}
		sqlText := string(body)
		view := func(name string) string {
			i := strings.Index(sqlText, "CREATE OR REPLACE VIEW \""+name+"\"")
			if i < 0 {
				t.Fatalf("%s: no view %s in:\n%s", round, name, sqlText)
			}
			return sqlText[i : i+strings.Index(sqlText[i:], ";")]
		}
		if v := view("state_shop_orders"); !strings.Contains(v, "bintrail_latest") {
			t.Errorf("%s: an ordinary table did not get the chain-aware body:\n%s", round, v)
		}
		if v := view("state_shop_odd"); strings.Contains(v, "file_row_number") {
			t.Errorf("%s: a table with a filename column got the chain-aware body, which does not bind:\n%s", round, v)
		}
		if strings.Contains(sqlText, "reads the table file alone") {
			t.Errorf("%s: a table the footer read cleared is announced as reading its file alone", round)
		}
	}
}
