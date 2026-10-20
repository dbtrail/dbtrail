package console

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2479, the wiring: the job the console hands the worker carries the
// PRIMARY KEY each view's table declares in its file, under the view's name.
func TestSQLAPI_jobCarriesTheTablesKeys(t *testing.T) {
	f := newSQLFixture(t, &fakeSQLRunner{}, false)
	// The fixture's file carries no CREATE TABLE; a snapshot's files do.
	const ddl = "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"
	cols, err := baseline.ParseSchemaText(ddl)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(f.schemaDir+"/orders.parquet", cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: ddl}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "new"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w := f.post(t, `{"sql":"SELECT id, status FROM shop.orders GROUP BY id, status"}`); w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	got := f.runner.jobs[len(f.runner.jobs)-1].Keys
	want := []sqlsandbox.TableKey{{Schema: "shop", Table: "orders", Columns: []string{"id"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Keys = %+v, want %+v", got, want)
	}
}
