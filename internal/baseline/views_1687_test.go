package baseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// The fixtures under testdata/views_* are real dumps, not hand-written files:
// MySQL 8.0.46, dumped with mydumper v1.0.3-1 (the build shipped in the
// bintrail-console 0.90.0 image) and --complete-insert. See the README in each
// directory for the schema they were taken from.
const (
	fixtureViews     = "testdata/views_mydumper_1.0.3_mysql_8.0.46"
	fixtureViewsEdge = "testdata/views_edge_names_mydumper_1.0.3_mysql_8.0.46"
	fixtureViewsCSV  = "testdata/views_csv_mydumper_1.0.3_mysql_8.0.46"
)

// A source with two tables and one view used to fail the whole run with
// "no columns found in schema SQL" and leave the snapshot _INCOMPLETE (#1687).
func TestRun_RealDumpWithView_1687(t *testing.T) {
	out := t.TempDir()
	_, err := Run(context.Background(), Config{InputDir: fixtureViews, OutputDir: out})
	if err != nil {
		t.Fatalf("Run on a dump that holds a view: %v", err)
	}
	snaps, err := filepath.Glob(filepath.Join(out, "20*"))
	if err != nil || len(snaps) != 1 {
		t.Fatalf("snapshot directories = %v (err %v), want exactly one", snaps, err)
	}
	snap := snaps[0]
	for _, want := range []string{SuccessMarker, "shop/orders.parquet", "shop/customers.parquet"} {
		if _, err := os.Stat(filepath.Join(snap, want)); err != nil {
			t.Errorf("%s missing from the snapshot: %v", want, err)
		}
	}
	for _, absent := range []string{IncompleteMarker, "shop/big_orders.parquet"} {
		if _, err := os.Stat(filepath.Join(snap, absent)); !os.IsNotExist(err) {
			t.Errorf("%s must not be in the snapshot (stat err = %v)", absent, err)
		}
	}
}

func tableNames(tables []TableFiles) []string {
	names := make([]string, len(tables))
	for i, tf := range tables {
		names[i] = tf.Database + "." + tf.Table
	}
	return names
}

func viewNames(views []SkippedView) []string {
	names := make([]string, len(views))
	for i, v := range views {
		names[i] = v.Database + "." + v.Name
	}
	return names
}

func wantNames(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s = %q, want %q", what, got, want)
	}
}

func TestRun_RealDumpWithView_ReportsTheView_1879(t *testing.T) {
	stats, err := Run(context.Background(), Config{InputDir: fixtureViews, OutputDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.TablesProcessed != 2 || stats.RowsWritten != 6 {
		t.Errorf("tables = %d, rows = %d, want 2 tables and 6 rows", stats.TablesProcessed, stats.RowsWritten)
	}
	wantNames(t, "ViewsSkipped", stats.ViewsSkipped, []string{"shop.big_orders"})
}

// The real dump of a schema built to confuse the detection: tables whose
// names end in -view, -schema and -schema-view, views whose names are a
// prefix of a table's and the reverse, an empty table, an empty MEMORY table
// (the engine of mydumper's view placeholder), and names mydumper renames to
// mydumper_N because they hold a dot.
func TestDiscoverDump_RealDumpEdgeNames_1687(t *testing.T) {
	tables, views, err := DiscoverDump(fixtureViewsEdge)
	if err != nil {
		t.Fatalf("DiscoverDump: %v", err)
	}
	wantNames(t, "tables", tableNames(tables), []string{
		"edge.empty_real",
		"edge.items",
		"edge.items-schema-view",
		"edge.log-schema",
		"edge.mem_real",
		"edge.mydumper_0", // table `Mixed.Case`
		"edge.orders-view",
		"edge.sales",
	})
	wantNames(t, "views", viewNames(views), []string{
		"edge.mydumper_1", // view `Totals.By-schema`
		"edge.sale",
		"edge.sales_recent",
		"onlyviews.v1",
	})
	for _, tf := range tables {
		wantData := 1
		if tf.Table == "empty_real" || tf.Table == "mem_real" {
			wantData = 0
		}
		if len(tf.DataFiles) != wantData {
			t.Errorf("%s.%s: %d data files %q, want %d", tf.Database, tf.Table, len(tf.DataFiles), tf.DataFiles, wantData)
		}
	}

	stats, err := Run(context.Background(), Config{InputDir: fixtureViewsEdge, OutputDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.TablesProcessed != 8 || stats.RowsWritten != 7 {
		t.Errorf("tables = %d, rows = %d, want 8 tables and 7 rows", stats.TablesProcessed, stats.RowsWritten)
	}
	if len(stats.ViewsSkipped) != 4 {
		t.Errorf("ViewsSkipped = %q, want 4 views", stats.ViewsSkipped)
	}
}

// mydumper --format CSV writes the same two files for a view.
func TestDiscoverDump_RealCSVDump_1687(t *testing.T) {
	tables, views, err := DiscoverDump(fixtureViewsCSV)
	if err != nil {
		t.Fatalf("DiscoverDump: %v", err)
	}
	wantNames(t, "tables", tableNames(tables), []string{"shop.customers", "shop.orders"})
	wantNames(t, "views", viewNames(views), []string{"shop.big_orders"})
}

// Text of the real files, for the cases no single dump can hold.
const (
	realPlaceholder = "/*!40101 SET NAMES utf8mb4*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"CREATE TABLE IF NOT EXISTS `v`(\n`id` int,\n`total` int\n) ENGINE=MEMORY ENCRYPTION='N';\n"
	realViewFile = "/*!40101 SET NAMES utf8mb4*/;\nDROP TABLE IF EXISTS `v`;\nDROP VIEW IF EXISTS `v`;\n" +
		"SET character_set_client = latin1;\n" +
		"CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` AS select `t`.`id` AS `id` from `t`;\n"
	realTableSchema = "/*!40101 SET NAMES utf8mb4*/;\nCREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	realData        = "/*!40101 SET NAMES utf8mb4*/;\nINSERT INTO `t` (`id`) VALUES(1)\n,(2)\n;\n"
)

func TestDiscoverDump_NamesAndSiblings_1687(t *testing.T) {
	tests := []struct {
		name       string
		files      map[string]string
		wantTables []string
		wantViews  []string
	}{
		{
			name: "placeholder and view file",
			files: map[string]string{
				"d.v-schema.sql":      realPlaceholder,
				"d.v-schema-view.sql": realViewFile,
			},
			wantViews: []string{"d.v"},
		},
		{
			// Without the sibling this is a table like any other: an empty
			// MEMORY table looks like this. It must reach the converter.
			name:       "placeholder with no view file is a table",
			files:      map[string]string{"d.v-schema.sql": realPlaceholder},
			wantTables: []string{"d.v"},
		},
		{
			name:      "view file with no placeholder",
			files:     map[string]string{"d.v-schema-view.sql": realViewFile},
			wantViews: []string{"d.v"},
		},
		{
			name: "upper case and a dot in the name",
			files: map[string]string{
				"Shop.Big.Orders-schema.sql":      realPlaceholder,
				"Shop.Big.Orders-schema-view.sql": realViewFile,
				"Shop.Orders-schema.sql":          realTableSchema,
				"Shop.Orders.00000.sql":           realData,
			},
			wantTables: []string{"Shop.Orders"},
			wantViews:  []string{"Shop.Big.Orders"},
		},
		{
			name: "lower case create or replace view",
			files: map[string]string{
				"d.v-schema.sql":      realPlaceholder,
				"d.v-schema-view.sql": "create or replace view `v` as select 1;\n",
			},
			wantViews: []string{"d.v"},
		},
		{
			name: "create view inside a version comment",
			files: map[string]string{
				"d.v-schema.sql":      realPlaceholder,
				"d.v-schema-view.sql": "/*!50001 CREATE ALGORITHM=UNDEFINED VIEW `v` AS select 1 */;\n",
			},
			wantViews: []string{"d.v"},
		},
		{
			// mydumper 0.10 names a table's one data file <db>.<table>.sql,
			// so table `x-schema-view` owns d.x-schema-view.sql. Table `x`
			// beside it is empty and must not be taken for a view.
			name: "mydumper 0.10: rows of a table named x-schema-view",
			files: map[string]string{
				"d.x-schema.sql":             realTableSchema,
				"d.x-schema-view-schema.sql": realTableSchema,
				"d.x-schema-view.sql":        realData,
			},
			wantTables: []string{"d.x", "d.x-schema-view"},
		},
		{
			name: "mydumper 0.10: tables x and x-schema-view, both with rows",
			files: map[string]string{
				"d.x-schema.sql":             realTableSchema,
				"d.x.sql":                    realData,
				"d.x-schema-view-schema.sql": realTableSchema,
				"d.x-schema-view.sql":        realData,
			},
			wantTables: []string{"d.x", "d.x-schema-view"},
		},
		{
			name: "rows that start past the scanner limit are rows",
			files: map[string]string{
				"d.x-schema.sql":             realTableSchema,
				"d.x-schema-view-schema.sql": realTableSchema,
				"d.x-schema-view.sql":        "INSERT INTO `t` VALUES(\"" + strings.Repeat("a", 200<<10) + "\")\n;\n",
			},
			wantTables: []string{"d.x", "d.x-schema-view"},
		},
		{
			// Only the start of a line is a statement. The comment line is
			// cut so that its second buffer-sized piece starts with CREATE.
			name: "create view in the middle of a long line is not a statement",
			files: map[string]string{
				"d.x-schema.sql":             realTableSchema,
				"d.x-schema-view-schema.sql": realTableSchema,
				"d.x-schema-view.sql": "-- " + strings.Repeat("a", 64<<10-3) +
					"CREATE ALGORITHM=UNDEFINED VIEW `v` AS select 1\n" + realData,
			},
			wantTables: []string{"d.x", "d.x-schema-view"},
		},
		{
			// The layout that writes CREATE VIEW into -schema.sql itself.
			name: "create view in the schema file, no view file",
			files: map[string]string{
				"d.v-schema.sql": "CREATE ALGORITHM=UNDEFINED VIEW `v` AS select 1;\n",
				"d.t-schema.sql": realTableSchema,
			},
			wantTables: []string{"d.t"},
			wantViews:  []string{"d.v"},
		},
		{
			name: "create table in the view file is not a view",
			files: map[string]string{
				"d.x-schema.sql":      realTableSchema,
				"d.x-schema-view.sql": "CREATE TABLE `my VIEW of x` (\n  `id` int\n);\n",
			},
			wantTables: []string{"d.x"},
		},
		{
			// A view has no rows. Rows win over a stray view file.
			name: "table with rows beside a view file of its name",
			files: map[string]string{
				"d.t-schema.sql":      realTableSchema,
				"d.t.00000.sql":       realData,
				"d.t-schema-view.sql": realViewFile,
			},
			wantTables: []string{"d.t"},
		},
		{
			name: "table names that end like the view files",
			files: map[string]string{
				"d.orders-view-schema.sql":        realTableSchema,
				"d.orders-view.00000.sql":         realData,
				"d.orders-schema-schema.sql":      realTableSchema,
				"d.orders-schema.00000.sql":       realData,
				"d.orders-schema-view.00000.sql":  realData,
				"d.orders-schema-view-schema.sql": realTableSchema,
			},
			wantTables: []string{"d.orders-schema", "d.orders-schema-view", "d.orders-view"},
		},
		{
			name: "view whose name is the start of a table's, and the reverse",
			files: map[string]string{
				"d.sale-schema.sql":           realPlaceholder,
				"d.sale-schema-view.sql":      realViewFile,
				"d.sales-schema.sql":          realTableSchema,
				"d.sales.00000.sql":           realData,
				"d.sal-schema.sql":            realTableSchema,
				"d.sales_old-schema.sql":      realPlaceholder,
				"d.sales_old-schema-view.sql": realViewFile,
			},
			wantTables: []string{"d.sal", "d.sales"},
			wantViews:  []string{"d.sale", "d.sales_old"},
		},
		{
			name: "same view name in two schemas, a table in one of them",
			files: map[string]string{
				"a.v-schema.sql":      realPlaceholder,
				"a.v-schema-view.sql": realViewFile,
				"b.v-schema.sql":      realTableSchema,
				"b.v.00000.sql":       realData,
			},
			wantTables: []string{"b.v"},
			wantViews:  []string{"a.v"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, content := range tt.files {
				writeFile(t, dir, name, content)
			}
			tables, views, err := DiscoverDump(dir)
			if err != nil {
				t.Fatalf("DiscoverDump: %v", err)
			}
			wantNames(t, "tables", tableNames(tables), tt.wantTables)
			wantNames(t, "views", viewNames(views), tt.wantViews)
			for _, tf := range tables {
				for _, f := range tf.DataFiles {
					if strings.HasSuffix(f, viewSchemaSuffix) && tf.Table != "x-schema-view" {
						t.Errorf("%s.%s reads the view file %s as rows", tf.Database, tf.Table, f)
					}
				}
			}
		})
	}
}

// The rows of a 0.10 table named x-schema-view survive: the file is still
// handed to the converter as that table's data.
func TestDiscoverDump_UnchunkedDataKeepsItsFile_1687(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "d.x-schema-view-schema.sql", realTableSchema)
	writeFile(t, dir, "d.x-schema-view.sql", realData)
	tables, _, err := DiscoverDump(dir)
	if err != nil {
		t.Fatalf("DiscoverDump: %v", err)
	}
	if len(tables) != 1 || len(tables[0].DataFiles) != 1 || filepath.Base(tables[0].DataFiles[0]) != "d.x-schema-view.sql" {
		t.Fatalf("tables = %+v, want d.x-schema-view with its one data file", tables)
	}
}

// A view file is never read as the rows of a table named <view>-schema-view.
func TestDiscoverDump_ViewFileIsNotData_1687(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "d.v-schema.sql", realPlaceholder)
	writeFile(t, dir, "d.v-schema-view.sql", realViewFile)
	writeFile(t, dir, "d.v-schema-view-schema.sql", realTableSchema)
	writeFile(t, dir, "d.v-schema-view.00000.sql", realData)
	tables, views, err := DiscoverDump(dir)
	if err != nil {
		t.Fatalf("DiscoverDump: %v", err)
	}
	wantNames(t, "views", viewNames(views), []string{"d.v"})
	if len(tables) != 1 || tables[0].Table != "v-schema-view" {
		t.Fatalf("tables = %+v, want d.v-schema-view only", tables)
	}
	if got := tables[0].DataFiles; len(got) != 1 || filepath.Base(got[0]) != "d.v-schema-view.00000.sql" {
		t.Errorf("data files = %q, want the chunk file only", got)
	}
}

func TestDiscoverDump_ViewFileThatCannotBeJudged_1687(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "d.v-schema.sql", realPlaceholder)
		writeFile(t, dir, "d.v-schema-view.sql", "")
		_, _, err := DiscoverDump(dir)
		if err == nil || !strings.Contains(err.Error(), "d.v-schema-view.sql is empty") || !strings.Contains(err.Error(), "whether d.v is a view") {
			t.Fatalf("err = %v, want one that names the empty file and the object", err)
		}
	})
	t.Run("blank lines only", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "d.v-schema.sql", realPlaceholder)
		writeFile(t, dir, "d.v-schema-view.sql", "\n  \n\n")
		if _, _, err := DiscoverDump(dir); err == nil || !strings.Contains(err.Error(), "is empty") {
			t.Fatalf("err = %v, want the empty-file error", err)
		}
	})
	t.Run("unreadable", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root reads a mode-000 file")
		}
		dir := t.TempDir()
		writeFile(t, dir, "d.v-schema.sql", realPlaceholder)
		writeFile(t, dir, "d.v-schema-view.sql", realViewFile)
		if err := os.Chmod(filepath.Join(dir, "d.v-schema-view.sql"), 0); err != nil {
			t.Fatal(err)
		}
		_, _, err := DiscoverDump(dir)
		if err == nil || !strings.Contains(err.Error(), "d.v-schema-view.sql") || !errors.Is(err, os.ErrPermission) {
			t.Fatalf("err = %v, want a permission error that names the file", err)
		}
	})
	t.Run("compressed", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "d.v-schema.sql", realPlaceholder)
		writeFile(t, dir, "d.v-schema-view.sql.zst", "x")
		if _, _, err := DiscoverDump(dir); err == nil || !strings.Contains(err.Error(), "compressed") {
			t.Fatalf("err = %v, want the compressed-dump refusal", err)
		}
	})
}

// A MEMORY table dumped with no rows has the placeholder's shape and no view
// file. It is not skipped: it reaches the converter, which refuses it as it
// did before this change.
func TestRun_PlaceholderWithoutViewFileIsNotSkipped_1687(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "d.v-schema.sql", realPlaceholder)
	writeFile(t, dir, "d.t-schema.sql", realTableSchema)
	writeFile(t, dir, "d.t.00000.sql", realData)
	stats, err := Run(context.Background(), Config{InputDir: dir, OutputDir: t.TempDir(), Timestamp: time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)})
	if err == nil || !strings.Contains(err.Error(), "d.v: parse schema: no columns found") {
		t.Fatalf("err = %v, want the converter's refusal of d.v", err)
	}
	if len(stats.ViewsSkipped) != 0 {
		t.Errorf("ViewsSkipped = %q, want none", stats.ViewsSkipped)
	}
}

func TestRun_NoViews_ReportsNone_1879(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "d.t-schema.sql", realTableSchema)
	writeFile(t, dir, "d.t.00000.sql", realData)
	stats, err := Run(context.Background(), Config{InputDir: dir, OutputDir: t.TempDir(), Timestamp: time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.ViewsSkipped != nil {
		t.Errorf("ViewsSkipped = %q, want nil", stats.ViewsSkipped)
	}
}

func TestRun_DumpWithViewsOnly_1687(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "d.v-schema.sql", realPlaceholder)
	writeFile(t, dir, "d.v-schema-view.sql", realViewFile)
	writeFile(t, dir, "d.w-schema.sql", realPlaceholder)
	writeFile(t, dir, "d.w-schema-view.sql", realViewFile)
	out := t.TempDir()
	_, err := Run(context.Background(), Config{InputDir: dir, OutputDir: out, Timestamp: time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)})
	if err == nil || !strings.Contains(err.Error(), "the dump holds 2 views and no table") {
		t.Fatalf("err = %v, want one that says the dump holds views only", err)
	}
	if entries, _ := os.ReadDir(out); len(entries) != 0 {
		t.Errorf("output holds %d entries, want none: nothing was converted", len(entries))
	}
}

func TestRun_TablesFilterAndViews_1687(t *testing.T) {
	at := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	t.Run("naming a view is refused", func(t *testing.T) {
		for _, filter := range [][]string{{"shop.big_orders"}, {"shop.orders", "SHOP.Big_Orders"}} {
			_, err := Run(context.Background(), Config{InputDir: fixtureViews, OutputDir: t.TempDir(), Timestamp: at, Tables: filter})
			if err == nil || !strings.Contains(err.Error(), "--tables names 1 view (shop.big_orders)") {
				t.Errorf("filter %q: err = %v, want the refusal that names the view", filter, err)
			}
		}
	})
	t.Run("naming tables copies them and reports no view", func(t *testing.T) {
		stats, err := Run(context.Background(), Config{InputDir: fixtureViews, OutputDir: t.TempDir(), Timestamp: at, Tables: []string{"shop.orders"}})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if stats.TablesProcessed != 1 || len(stats.ViewsSkipped) != 0 {
			t.Errorf("tables = %d, views = %q, want 1 table and no view reported", stats.TablesProcessed, stats.ViewsSkipped)
		}
	})
}
