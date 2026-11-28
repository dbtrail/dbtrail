package console

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// writeSQLTableRows writes one table of schema ch of the copy with the rows
// given, as the baseline writer writes it.
func writeSQLTableRows(t *testing.T, root, table, createSQL string, rows ...[]string) {
	t.Helper()
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(root, sqlSnapshotDirName, "ch", table+".parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100, Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := w.WriteRow(row, make([]bool, len(row))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// CH-benCHmark Q8 through runSQL and the real worker. n_name is a PAD SPACE
// char(25) compared inside a division: SUM(CASE WHEN n2.n_name = 'Germany'
// ...) / SUM(ol_amount). The division sees a number, so n_name is held to
// the plain rule (printable ASCII, no trailing space) and a name with an
// inner space ('United States') is answered. A value outside that rule is
// still declined, and a call that cuts n_name itself still holds it to no
// space at all ('United States' cut to 'United ' ends with one).
func TestSQL_realWorkerTextComparedInsideACut(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	const q8 = "SELECT extract(YEAR FROM o_entry_d) AS l_year, sum(CASE WHEN n2.n_name = 'Germany' THEN ol_amount ELSE 0 END) / sum(ol_amount) AS mkt_share " +
		"FROM item, supplier, stock, order_line, orders, customer, nation n1, nation n2, region " +
		"WHERE i_id = s_i_id AND ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey " +
		"AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id " +
		"AND n1.n_nationkey = ascii(substring(c_state from 1 for 1)) AND n1.n_regionkey = r_regionkey AND ol_i_id < 1000 " +
		"AND r_name = 'Europe' AND su_nationkey = n2.n_nationkey AND i_data LIKE '%b' AND i_id = ol_i_id GROUP BY l_year ORDER BY l_year"
	const cutName = "SELECT count(*) FROM nation WHERE LEFT(n_name, 6) = 'United'"
	const declined = "a value in ch.nation (n_name) that the copy compares another way than MySQL"
	tables := func(t *testing.T, root, other string) {
		latin1 := "\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n"
		writeSQLTableRows(t, root, "item", "CREATE TABLE `item` (\n  `i_id` int NOT NULL,\n  `i_data` varchar(50) NOT NULL,\n  PRIMARY KEY (`i_id`)"+latin1,
			[]string{"1", "xb"})
		writeSQLTableRows(t, root, "supplier", "CREATE TABLE `supplier` (\n  `su_suppkey` int NOT NULL,\n  `su_nationkey` int NOT NULL,\n  PRIMARY KEY (`su_suppkey`)"+latin1,
			[]string{"1", "54"})
		writeSQLTableRows(t, root, "stock", "CREATE TABLE `stock` (\n  `s_w_id` int NOT NULL,\n  `s_i_id` int NOT NULL,\n  PRIMARY KEY (`s_w_id`,`s_i_id`)"+latin1,
			[]string{"1", "1"})
		writeSQLTableRows(t, root, "order_line", "CREATE TABLE `order_line` (\n  `ol_w_id` int NOT NULL,\n  `ol_d_id` int NOT NULL,\n  `ol_o_id` int NOT NULL,\n"+
			"  `ol_i_id` int NOT NULL,\n  `ol_supply_w_id` int NOT NULL,\n  `ol_amount` decimal(6,2) NOT NULL,\n  PRIMARY KEY (`ol_w_id`,`ol_d_id`,`ol_o_id`)"+latin1,
			[]string{"1", "1", "1", "1", "1", "10.00"})
		writeSQLTableRows(t, root, "orders", "CREATE TABLE `orders` (\n  `o_w_id` int NOT NULL,\n  `o_d_id` int NOT NULL,\n  `o_id` int NOT NULL,\n"+
			"  `o_c_id` int NOT NULL,\n  `o_entry_d` datetime NOT NULL,\n  PRIMARY KEY (`o_w_id`,`o_d_id`,`o_id`)"+latin1,
			[]string{"1", "1", "1", "1", "2026-01-01 10:00:00"})
		writeSQLTableRows(t, root, "customer", "CREATE TABLE `customer` (\n  `c_w_id` int NOT NULL,\n  `c_d_id` int NOT NULL,\n  `c_id` int NOT NULL,\n"+
			"  `c_state` char(2) NOT NULL,\n  PRIMARY KEY (`c_w_id`,`c_d_id`,`c_id`)"+latin1,
			[]string{"1", "1", "1", "AB"})
		writeSQLTableRows(t, root, "nation", "CREATE TABLE `nation` (\n  `n_nationkey` int NOT NULL,\n  `n_name` char(25) NOT NULL,\n  `n_regionkey` int NOT NULL,\n"+
			"  PRIMARY KEY (`n_nationkey`)"+latin1,
			[]string{"54", "Germany", "5"}, []string{"65", other, "5"})
		writeSQLTableRows(t, root, "region", "CREATE TABLE `region` (\n  `r_regionkey` int NOT NULL,\n  `r_name` char(55) NOT NULL,\n  PRIMARY KEY (`r_regionkey`)"+latin1,
			[]string{"5", "Europe"})
	}
	for _, c := range []struct {
		name, stmt string
		declined   bool
	}{
		{"United States", q8, false},
		{"Perú", q8, true},
		{"United States", cutName, true},
		{"Germany", cutName, false},
	} {
		t.Run(c.name+"/"+c.stmt[:20], func(t *testing.T) {
			runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
			f := newSQLFixture(t, runner, false)
			tables(t, f.root, c.name)
			sess := sqlsandbox.Session{StrictStar: true, Types: readrouter.ShapeOf(c.stmt)}
			out, err := f.s.runSQL(context.Background(), f.s.cm.boot, "u", c.stmt, "ch", 0, sess)
			switch {
			case c.declined && (err == nil || !strings.Contains(err.Error(), declined)):
				t.Errorf("err = %v, want the copy to decline it for n_name", err)
			case !c.declined && err != nil:
				t.Errorf("%v, want the copy's answer", err)
			case !c.declined && len(out.Result.Rows) != 1:
				t.Errorf("rows = %v, want one", out.Result.Rows)
			}
		})
	}
}
