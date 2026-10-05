package baseline

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// writeOrderFile writes one snapshot file with the columns of fileDDL and the
// CREATE TABLE footerDDL in its footer ("" writes no footer key), so a test
// can make the two disagree.
func writeOrderFile(t *testing.T, name, fileDDL, footerDDL string) string {
	t.Helper()
	cols, err := ParseSchemaText(fileDDL)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "2026-04-30T03-00-00Z", "shop", name+".parquet")
	cfg := WriterConfig{Compression: "none", RowGroupSize: 100}
	if footerDDL != "" {
		cfg.Metadata = map[string]string{MetaKeyCreateTableSQL: footerDDL}
	}
	w, err := NewWriter(path, cols, cfg)
	if err != nil {
		t.Fatal(err)
	}
	row := make([]string, len(cols))
	nulls := make([]bool, len(cols))
	for i := range nulls {
		nulls[i] = true
	}
	if err := w.WriteRow(row, nulls); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

const orderDDL = "CREATE TABLE `orders` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `customer_id` int NOT NULL,\n" +
	"  `status` varchar(16) DEFAULT NULL,\n" +
	"  `amount` decimal(12,2) DEFAULT NULL,\n" +
	"  `created_at` datetime DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB;\n"

// The footer read reports the table's columns in the order the CREATE TABLE
// declares them, which is not the order the file holds them in (#2111).
func TestReadTableFooters_columnsInDeclaredOrder(t *testing.T) {
	path := writeOrderFile(t, "orders", orderDDL, orderDDL)
	footers, err := TableFootersFor(context.Background(), []string{path})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "customer_id", "status", "amount", "created_at"}
	if got := footers[path].Columns; !reflect.DeepEqual(got, want) {
		t.Errorf("Columns = %q, want the declared order %q", got, want)
	}
	if got := footers[path].StarDiffers; got != "" {
		t.Errorf("StarDiffers = %q for a table MySQL reads the same", got)
	}
}

// The order is only known when the file holds exactly the columns the CREATE
// TABLE lists. Anything else leaves it unknown, with the casts kept: a view
// naming a column the file lacks would not bind, and one leaving a column out
// would hide it.
func TestReadTableFooters_orderUnknownWhenTheFileAndTheFooterDisagree(t *testing.T) {
	fewer := strings.Replace(orderDDL, "  `status` varchar(16) DEFAULT NULL,\n", "", 1)
	renamed := strings.Replace(orderDDL, "`status`", "`Status`", 1)
	for _, c := range []struct{ name, fileDDL, footerDDL string }{
		{"the footer lists a column the file lacks", fewer, orderDDL},
		{"the file holds a column the footer does not list", orderDDL, fewer},
		{"a name differs in case", orderDDL, renamed},
	} {
		t.Run(c.name, func(t *testing.T) {
			logs := captureLogs(t)
			path := writeOrderFile(t, "orders", c.fileDDL, c.footerDDL)
			read, err := ReadTableFooters(context.Background(), []string{path})
			if err != nil {
				t.Fatal(err)
			}
			f, ok := read.Footers[path]
			if !ok {
				t.Fatal("the footer was not read at all: the casts would be lost with the order")
			}
			if f.Columns != nil {
				t.Errorf("Columns = %q, want nil (unknown)", f.Columns)
			}
			if len(f.Decimals) != 1 || f.Decimals[0].Name != "amount" {
				t.Errorf("Decimals = %+v, want amount kept", f.Decimals)
			}
			if len(read.Unread) != 0 {
				t.Errorf("Unread = %q: the file was read, a retry would learn nothing", read.Unread)
			}
			if !strings.Contains(logs.String(), "column order is not known") {
				t.Errorf("nothing was logged about the unknown order:\n%s", logs.String())
			}
		})
	}
}

// A file with no CREATE TABLE has no order to report: it is absent, as before.
func TestReadTableFooters_noSchemaHasNoOrder(t *testing.T) {
	path := writeOrderFile(t, "orders", orderDDL, "")
	read, err := ReadTableFooters(context.Background(), []string{path})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := read.Footers[path]; ok || len(read.NoSchema) != 1 {
		t.Errorf("Footers = %+v, NoSchema = %q; want the file under NoSchema only", read.Footers, read.NoSchema)
	}
}

// Several files in one read, so the batched column read is the one under
// test, and each keeps its own order.
func TestReadTableFooters_columnOrderPerFile(t *testing.T) {
	other := "CREATE TABLE `items` (\n  `sku` varchar(8) NOT NULL,\n  `id` int NOT NULL,\n  `at` date DEFAULT NULL\n);\n"
	a := writeOrderFile(t, "orders", orderDDL, orderDDL)
	b := writeOrderFile(t, "items", other, other)
	c := writeOrderFile(t, "legacy", other, "")
	footers, err := TableFootersFor(context.Background(), []string{a, b, c})
	if err != nil {
		t.Fatal(err)
	}
	if got := footers[a].Columns; len(got) != 5 || got[0] != "id" || got[4] != "created_at" {
		t.Errorf("orders: %q", got)
	}
	if got, want := footers[b].Columns, []string{"sku", "id", "at"}; !reflect.DeepEqual(got, want) {
		t.Errorf("items: %q, want %q", got, want)
	}
	if _, ok := footers[c]; ok {
		t.Error("the file with no CREATE TABLE got a footer")
	}
}

// What MySQL's SELECT * returns that a snapshot file does not, and the other
// way round.
func TestStarDifference(t *testing.T) {
	cases := []struct {
		name, body string
		want       []string // every one must appear; none means ""
	}{
		{"plain columns", "  `id` int NOT NULL,\n  `name` varchar(8) DEFAULT NULL,\n", nil},
		{"a stored generated column", "  `id` int NOT NULL,\n  `total` int GENERATED ALWAYS AS (`id` * 2) STORED,\n  `z` int,\n",
			[]string{"MySQL also returns generated column total"}},
		{"a virtual generated column", "  `id` int NOT NULL,\n  `v` int GENERATED ALWAYS AS (`id` + 1) VIRTUAL,\n",
			[]string{"generated column v"}},
		{"two generated columns", "  `a` int GENERATED ALWAYS AS (1) STORED,\n  `id` int,\n  `b` int AS (2) PERSISTENT,\n",
			[]string{"generated columns a, b"}},
		{"a MariaDB period column", "  `id` int NOT NULL,\n  `row_start` timestamp(6) GENERATED ALWAYS AS ROW START,\n",
			[]string{"generated column row_start"}},
		{"a MySQL invisible column", "  `id` int NOT NULL,\n  `secret` varchar(8) DEFAULT NULL /*!80023 INVISIBLE */,\n",
			[]string{"MySQL leaves out invisible column secret"}},
		{"a MariaDB invisible column", "  `id` int NOT NULL,\n  `secret` varchar(8) INVISIBLE DEFAULT NULL,\n",
			[]string{"invisible column secret"}},
		{"generated and invisible: in neither answer", "  `id` int NOT NULL,\n  `g` int GENERATED ALWAYS AS (`id`) VIRTUAL /*!80023 INVISIBLE */,\n", nil},
		{"the word in a comment, a default and a name", "  `invisible` int NOT NULL COMMENT 'INVISIBLE',\n  `s` varchar(16) DEFAULT 'invisible',\n" +
			"  `e` enum('INVISIBLE','x') DEFAULT NULL,\n", nil},
		{"a name holding a backtick", "  `id` int NOT NULL,\n  `we``ird` int DEFAULT NULL,\n",
			[]string{"1 column definition(s) could not be read"}},
		{"everything at once", "  `g` int AS (1) STORED,\n  `h` int INVISIBLE,\n  `x``y` int,\n  `id` int,\n",
			[]string{"generated column g", "invisible column h", "1 column definition(s) could not be read"}},
		{"a generated column after the keys is not a column line", "  `id` int NOT NULL,\n  PRIMARY KEY (`id`),\n  `late` int AS (1) STORED,\n", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := starDifference("CREATE TABLE `t` (\n" + c.body + "  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n")
			if len(c.want) == 0 && got != "" {
				t.Errorf("starDifference = %q, want none", got)
			}
			for _, w := range c.want {
				if !strings.Contains(got, w) {
					t.Errorf("starDifference = %q, want it to say %q", got, w)
				}
			}
		})
	}
}

// The footer read carries the difference next to the order: a generated
// column is not in the file, so the order of the rest is still known.
func TestReadTableFooters_starDiffersWithAKnownOrder(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `total` int GENERATED ALWAYS AS (`id` * 2) STORED,\n" +
		"  `b` int DEFAULT NULL,\n  `a` int DEFAULT NULL /*!80023 INVISIBLE */,\n  PRIMARY KEY (`id`)\n);\n"
	path := writeOrderFile(t, "t", ddl, ddl)
	footers, err := TableFootersFor(context.Background(), []string{path})
	if err != nil {
		t.Fatal(err)
	}
	f := footers[path]
	if want := []string{"id", "b", "a"}; !reflect.DeepEqual(f.Columns, want) {
		t.Errorf("Columns = %q, want %q", f.Columns, want)
	}
	if !strings.Contains(f.StarDiffers, "generated column total") || !strings.Contains(f.StarDiffers, "invisible column a") {
		t.Errorf("StarDiffers = %q", f.StarDiffers)
	}
}

func TestSameColumnSet(t *testing.T) {
	held := map[string]bool{"a": true, "b": true}
	for _, c := range []struct {
		declared []string
		want     bool
	}{
		{[]string{"b", "a"}, true},
		{[]string{"a"}, false},
		{[]string{"a", "b", "c"}, false},
		{[]string{"a", "a"}, false},
		{[]string{"a", "B"}, false},
		{nil, false},
	} {
		if got := sameColumnSet(c.declared, held); got != c.want {
			t.Errorf("sameColumnSet(%q) = %v, want %v", c.declared, got, c.want)
		}
	}
	if !sameColumnSet(nil, map[string]bool{}) {
		t.Error("two empty sets are the same set")
	}
}

// A file whose CREATE TABLE was read and whose own column list was not keeps
// its casts, loses its order and is reported unread, so a caller that caches
// the answer asks again instead of keeping "order unknown" for good.
func TestFooterScan_orderUnreadIsRetried(t *testing.T) {
	st := footerScan{
		footers:     map[string]TableFooter{"a.parquet": {Decimals: []DecimalColumn{}}, "b.parquet": {Columns: []string{"id"}}},
		unparsable:  map[string]bool{},
		failed:      map[string]bool{},
		orderUnread: []string{"a.parquet"},
	}
	r := st.result([]string{"a.parquet", "b.parquet", "c.parquet"}, nil)
	if !reflect.DeepEqual(r.Unread, []string{"a.parquet"}) {
		t.Errorf("Unread = %q, want the file whose column list was not read", r.Unread)
	}
	if _, ok := r.Footers["a.parquet"]; !ok {
		t.Error("the file lost its footer with its order")
	}
	if !reflect.DeepEqual(r.NoSchema, []string{"c.parquet"}) {
		t.Errorf("NoSchema = %q, want only the file with no footer", r.NoSchema)
	}
}

// The decision confirmColumnOrder makes from what it read: the declared order
// is kept for a file that was read and holds exactly those columns, and for
// no other.
func TestFooterScan_settleColumnOrder(t *testing.T) {
	order := []string{"id", "b", "a"}
	st := footerScan{footers: map[string]TableFooter{
		"same.parquet":   {Columns: order},
		"unread.parquet": {Columns: order},
		"fewer.parquet":  {Columns: order},
		"absent.parquet": {Columns: order},
	}}
	cols := func(names ...string) map[string]bool {
		m := map[string]bool{}
		for _, n := range names {
			m[n] = true
		}
		return m
	}
	st.settleColumnOrder(
		map[string]map[string]bool{"same.parquet": cols("a", "b", "id"), "unread.parquet": cols("a", "b", "id"), "fewer.parquet": cols("a", "id")},
		map[string]bool{"same.parquet": true, "fewer.parquet": true, "absent.parquet": true})
	if got := st.footers["same.parquet"].Columns; !reflect.DeepEqual(got, order) {
		t.Errorf("a file holding the declared columns: Columns = %q, want %q", got, order)
	}
	for _, p := range []string{"unread.parquet", "fewer.parquet", "absent.parquet"} {
		if got := st.footers[p].Columns; got != nil {
			t.Errorf("%s: Columns = %q, want nil (unknown)", p, got)
		}
	}
	if !reflect.DeepEqual(st.orderUnread, []string{"unread.parquet"}) {
		t.Errorf("orderUnread = %q, want only the file whose list was not read", st.orderUnread)
	}
}
