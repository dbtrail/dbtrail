package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Real dumps made by mydumper 1.0.3-1 (the console image's build, arm64)
// against MariaDB 11.8 (#2006). mydumper writes a table whose name it will not
// put in a file name (a dot, a slash, "@", non-ASCII, a long name, and even a
// real table already called mydumper_N) under a made-up mydumper_N name, and
// renumbers them on every dump. The real name is in the -schema.sql CREATE
// TABLE and in metadata's real_table_name. The demo dump also holds a table
// named `order/items`, which no snapshot can store as a file name.
const (
	fixtureNames2006          = "testdata/mydumper-1.0.3-names/demo"
	fixtureDottedSchema2006   = "testdata/mydumper-1.0.3-names/dotted-schema"
	fixtureSchemaNoCreate2006 = "testdata/mydumper-1.0.3-names/dotted-schema-regex"
	slashTableFile2006        = "demo.mydumper_5" // real name order/items
)

// copyDump copies a fixture dump into a temp dir, leaving out every file
// whose name starts with one of skip.
func copyDump(t *testing.T, src string, skip ...string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		left := false
		for _, s := range skip {
			if strings.HasPrefix(e.Name(), s+"-") || strings.HasPrefix(e.Name(), s+".") {
				left = true
			}
		}
		if left {
			continue
		}
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

func TestDiscoverDump_RealNamesFromTheDump_2006(t *testing.T) {
	dir := copyDump(t, fixtureNames2006, slashTableFile2006)
	tables, views, err := DiscoverDump(dir)
	if err != nil {
		t.Fatalf("DiscoverDump: %v", err)
	}
	wantNames(t, "tables", tableNames(tables), []string{
		"demo.a b",
		"demo.a-b",
		"demo.a.b",
		"demo.a@b",
		"demo.mydumper_1", // a real table of that name, written as mydumper_2
		"demo.mydumper_2", // a real table of that name, written as mydumper_3
		"demo.order items",
		"demo.order.items",
		"demo.pedidos_año",
		"demo.plain",
		"demo.x234567890123456789012345678901234567890123456789012345678901.y",
		"demo.注文",
	})
	wantNames(t, "views", viewNames(views), []string{"demo.v plain", "demo.v.dotted"})

	// Each real name keeps the files mydumper wrote for it.
	byName := map[string]TableFiles{}
	for _, tf := range tables {
		byName[tf.Database+"."+tf.Table] = tf
	}
	for name, file := range map[string]string{
		"demo.order.items": "demo.mydumper_4",
		"demo.mydumper_2":  "demo.mydumper_3",
		"demo.a.b":         "demo.mydumper_0",
	} {
		tf := byName[name]
		if filepath.Base(tf.SchemaFile) != file+"-schema.sql" {
			t.Errorf("%s: schema file %s, want %s-schema.sql", name, tf.SchemaFile, file)
		}
		if len(tf.DataFiles) != 1 || filepath.Base(tf.DataFiles[0]) != file+".00000.sql" {
			t.Errorf("%s: data files %q, want [%s.00000.sql]", name, tf.DataFiles, file)
		}
	}
}

func TestRun_RealNamesReachTheSnapshot_2006(t *testing.T) {
	dir := copyDump(t, fixtureNames2006, slashTableFile2006)
	out := t.TempDir()
	stats, err := Run(context.Background(), Config{InputDir: dir, OutputDir: out})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Ten tables of 3 rows, the real mydumper_2 with 1, the real mydumper_1 empty.
	if stats.TablesProcessed != 12 || stats.RowsWritten != 31 {
		t.Errorf("tables = %d, rows = %d, want 12 tables and 31 rows", stats.TablesProcessed, stats.RowsWritten)
	}
	snaps, _ := filepath.Glob(filepath.Join(out, "20*", "demo"))
	if len(snaps) != 1 {
		t.Fatalf("want one snapshot dir, got %q", snaps)
	}
	demo := snaps[0]
	files, _ := os.ReadDir(demo)
	var got []string
	for _, f := range files {
		got = append(got, f.Name())
	}
	for _, want := range []string{"order.items.parquet", "a.b.parquet", "注文.parquet", "mydumper_2.parquet"} {
		if _, err := os.Stat(filepath.Join(demo, want)); err != nil {
			t.Errorf("%s missing from the snapshot; it holds %q", want, got)
		}
	}
	for _, f := range got {
		if f == "mydumper_0.parquet" || f == "mydumper_4.parquet" || f == "mydumper_9.parquet" {
			t.Errorf("snapshot holds a made-up table file %s", f)
		}
	}
	md, err := ReadParquetMetadata(filepath.Join(demo, "order.items.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if md.CreateTableSQL == "" || !strings.Contains(md.CreateTableSQL, "`order.items`") {
		t.Errorf("CREATE TABLE in the footer does not name order.items: %q", md.CreateTableSQL)
	}
}

// A name that cannot be stored as a file name fails the read, naming the
// real table and the file it came from, and nothing else is converted.
// leftOutOf runs DiscoverDumpNames and returns the left-out list as
// "table: reason" lines, failing on a discovery error.
func leftOutOf(t *testing.T, dir string) ([]TableFiles, []LeftOutTable) {
	t.Helper()
	tables, _, left, err := DiscoverDumpNames(dir)
	if err != nil {
		t.Fatalf("DiscoverDumpNames: %v", err)
	}
	return tables, left
}

// A name that cannot be stored as a file name leaves out THAT table, with
// its real name and why; every other table is converted (decision on #2006:
// the dump already read all of production).
func TestRun_SlashInNameLeavesOutOnlyThatTable_2006(t *testing.T) {
	tables, left := leftOutOf(t, fixtureNames2006)
	if len(tables) != 12 || len(left) != 1 || left[0].Table != "demo.order/items" ||
		!strings.Contains(left[0].Reason, `"/"`) || !strings.Contains(left[0].Reason, "demo.mydumper_5-schema.sql") {
		t.Fatalf("tables %d, left out %+v; want 12 kept and demo.order/items left out naming the slash and the file", len(tables), left)
	}
	out := t.TempDir()
	stats, err := Run(context.Background(), Config{InputDir: fixtureNames2006, OutputDir: out})
	if err != nil {
		t.Fatalf("Run refused the whole dump for one table: %v", err)
	}
	if stats.TablesProcessed != 12 || len(stats.TablesLeftOut) != 1 || stats.TablesLeftOut[0].Table != "demo.order/items" {
		t.Fatalf("stats = %+v", stats)
	}
	if m, _ := filepath.Glob(filepath.Join(out, "20*", "demo", "order*")); len(m) != 2 {
		t.Errorf("want order.items and order items in the snapshot and nothing for order/items, got %q", m)
	}
	// A caller that cannot report left-out tables still gets them as an error.
	if _, _, err := DiscoverDump(fixtureNames2006); err == nil || !strings.Contains(err.Error(), "demo.order/items") {
		t.Errorf("DiscoverDump = %v, want the left-out table as an error", err)
	}
}

// A schema whose name holds a dot: mydumper writes it as mydumper_0 and keeps
// the real name only in its CREATE DATABASE. Its tables are left out, named
// with the real schema.
func TestDiscoverDump_DottedSchemaLeftOut_2006(t *testing.T) {
	tables, left := leftOutOf(t, fixtureDottedSchema2006)
	if len(tables) != 0 || len(left) != 2 {
		t.Fatalf("tables %d, left %+v", len(tables), left)
	}
	for _, l := range left {
		if !strings.HasPrefix(l.Table, "my.db.") || !strings.Contains(l.Reason, "holds a dot") {
			t.Errorf("left out %+v: want it named under my.db, saying the schema holds a dot", l)
		}
	}
	// Only that schema: with every table left out the run fails, saying why.
	_, err := Run(context.Background(), Config{InputDir: fixtureDottedSchema2006, OutputDir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "my.db.t") {
		t.Fatalf("Run = %v, want a failure naming the left-out tables", err)
	}
}

// The console's multi-schema dump (--regex) writes no CREATE DATABASE file:
// a renamed schema cannot be read back, so its tables are left out, named by
// table only, never as mydumper_0.
func TestDiscoverDump_MadeUpSchemaWithoutCreateDatabase_2006(t *testing.T) {
	dir := copyDump(t, fixtureSchemaNoCreate2006)
	writeFile(t, dir, "shop.orders-schema.sql", "CREATE TABLE `orders` (\n  `id` int NOT NULL\n);\n")
	tables, left := leftOutOf(t, dir)
	if len(tables) != 1 || tables[0].Database != "shop" {
		t.Fatalf("tables = %+v, want shop.orders kept", tables)
	}
	if len(left) != 1 || left[0].Table != "t" || left[0].Name != "t" || left[0].Schema != "" || strings.Contains(left[0].Table, "mydumper_0") ||
		!strings.Contains(left[0].Reason, "made-up name") {
		t.Fatalf("left out %+v, want table t with the reason", left)
	}
}

func TestCreateTableName_2006(t *testing.T) {
	for _, c := range []struct {
		name, sql, want string
		ok              bool
	}{
		{"plain", "CREATE TABLE `t` (\n`id` int\n);\n", "t", true},
		{"after set lines", "/*!40101 SET NAMES utf8mb4*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\nCREATE TABLE `order.items` (\n", "order.items", true},
		{"if not exists", "CREATE TABLE IF NOT EXISTS `v.dotted`(\n", "v.dotted", true},
		{"doubled backtick", "CREATE TABLE `order``items` (\n", "order`items", true},
		{"double quotes", "CREATE TABLE \"a.b\" (\n", "a.b", true},
		{"lower case", "create table `x` (\n", "x", true},
		{"space and unicode", "CREATE TABLE `注文 x` (\n", "注文 x", true},
		{"schema qualified", "CREATE TABLE `demo`.`t.u` (\n", "t.u", true},
		{"unterminated", "CREATE TABLE `abc (\n", "", false},
		{"empty name", "CREATE TABLE `` (\n", "", false},
		{"no create", "/*!40101 SET NAMES utf8mb4*/;\n", "", false},
		{"view", "CREATE ALGORITHM=UNDEFINED VIEW `v` AS select 1;\n", "", false},
		{"empty file", "", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "x-schema.sql")
			if err := os.WriteFile(p, []byte(c.sql), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := createdName(p, "TABLE")
			if (err == nil) != c.ok || got != c.want {
				t.Errorf("createdName = %q, %v; want %q, ok=%v", got, err, c.want, c.ok)
			}
		})
	}
}

func TestStorableName_2006(t *testing.T) {
	long := strings.Repeat("注", 60)    // 180 bytes: fits
	tooLong := strings.Repeat("注", 64) // 192 bytes + the longest delta suffix
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"order.items", true}, {"order items", true}, {"a@b", true}, {long, true},
		{"order/items", false}, {`order\items`, false}, {".", false}, {"..", false},
		{"", false}, {"a\x00b", false}, {strings.Repeat("x", 250), false},
	} {
		err := storableName(c.name)
		if (err == nil) != c.ok {
			t.Errorf("storableName(%q) = %v, want ok=%v", c.name, err, c.ok)
		}
	}
	if err := storableName(tooLong); err != nil && !strings.Contains(err.Error(), "bytes") {
		t.Errorf("a long name's refusal does not say it is too long: %v", err)
	}
}

// Two files that claim the same real name (a dump folder holding two dumps,
// or a hand-edited one) are refused, never one silently kept.
// Two files that claim the same real name: neither is known to be it, so
// both are left out; never one silently kept.
func TestDiscoverDump_TwoFilesOneRealName_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "demo.mydumper_0-schema.sql", "CREATE TABLE `order.items` (\n  `id` int NOT NULL\n);\n")
	writeFile(t, dir, "demo.mydumper_1-schema.sql", "CREATE TABLE `order.items` (\n  `id` int NOT NULL\n);\n")
	writeFile(t, dir, "demo.plain-schema.sql", "CREATE TABLE `plain` (\n  `id` int NOT NULL\n);\n")
	tables, left := leftOutOf(t, dir)
	if len(tables) != 1 || tables[0].Table != "plain" || len(left) != 2 || left[0].Table != "demo.order.items" {
		t.Fatalf("tables %+v, left %+v", tables, left)
	}
}

// metadata's real_table_name and the CREATE TABLE must agree.
func TestDiscoverDump_MetadataDisagrees_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "demo.mydumper_0-schema.sql", "CREATE TABLE `order.items` (\n  `id` int NOT NULL\n);\n")
	writeFile(t, dir, "metadata", "[`demo`.`mydumper_0`]\nreal_table_name=order.lines\nrows = 0\n")
	_, left := leftOutOf(t, dir)
	if len(left) != 1 || !strings.Contains(left[0].Reason, "order.lines") || !strings.Contains(left[0].Reason, "order.items") {
		t.Fatalf("left %+v, want both names said", left)
	}
}

// A made-up file name whose CREATE TABLE cannot be read is left out even
// when metadata names it: a name with a line break cuts the metadata line.
func TestDiscoverDump_MetadataAloneIsNotEnough_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "demo.mydumper_0-schema.sql", "CREATE TABLE `a\nb` (\n  `id` int NOT NULL\n);\n")
	writeFile(t, dir, "metadata", "[`demo`.`mydumper_0`]\nreal_table_name=a\nb\nrows = 0\n")
	tables, left := leftOutOf(t, dir)
	if len(tables) != 0 || len(left) != 1 || left[0].Table == "demo.a" {
		t.Fatalf("tables %+v, left %+v: must not publish the name cut at its line break", tables, left)
	}
}

// A dotted schema written raw into the file name splits wrongly at the
// first dot; the CREATE TABLE shows it.
func TestDiscoverDump_RawDottedSchemaLeftOut_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "my.db.t-schema.sql", "CREATE TABLE `t` (\n  `id` int NOT NULL\n);\n")
	tables, left := leftOutOf(t, dir)
	if len(tables) != 0 || len(left) != 1 || !strings.Contains(left[0].Reason, "holds a dot") {
		t.Fatalf("tables %+v, left %+v", tables, left)
	}
}

// A dotted file name whose CREATE TABLE cannot be read, with no metadata,
// is left out: never the file name split at its first dot.
func TestDiscoverDump_DottedNameUnreadable_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "demo.order.items-schema.sql", "/*!40101 SET NAMES utf8mb4*/;\n")
	tables, left := leftOutOf(t, dir)
	if len(tables) != 0 || len(left) != 1 || left[0].Table == "demo.order.items" {
		t.Fatalf("tables %+v, left %+v", tables, left)
	}
}

// Metadata values as mydumper 1.0.3-1 wrote them (measured): raw, a leading
// space and backslashes kept, no key-file escapes; they match the CREATE TABLE.
func TestDiscoverDump_MetadataValuesAreRaw_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "demo. lead-schema.sql", "CREATE TABLE ` lead` (\n  `id` int NOT NULL\n);\n")
	writeFile(t, dir, "demo.mydumper_5-schema.sql", "CREATE TABLE `a\\\\b` (\n  `id` int NOT NULL\n);\n")
	writeFile(t, dir, "demo.mydumper_6-schema.sql", "CREATE TABLE `a=b#c;d` (\n  `id` int NOT NULL\n);\n")
	writeFile(t, dir, "metadata", "[`demo`.` lead`]\nreal_table_name= lead\n[`demo`.`mydumper_5`]\nreal_table_name=a\\\\b\n[`demo`.`mydumper_6`]\nreal_table_name=a=b#c;d\n")
	tables, left := leftOutOf(t, dir)
	wantNames(t, "tables", tableNames(tables), []string{"demo. lead", "demo.a=b#c;d"})
	// A backslash is a legal name on the source, but the snapshot's delta
	// files treat it as a path separator: left out, saying so.
	if len(left) != 1 || left[0].Table != `demo.a\\b` || !strings.Contains(left[0].Reason, `"\"`) {
		t.Fatalf("left out %+v, want demo.a\\b for its backslash", left)
	}
}

// A view written under a made-up name with no metadata is reported by the
// name its CREATE VIEW gives.
func TestDiscoverDump_ViewNameFromCreateView_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "demo.plain-schema.sql", "CREATE TABLE `plain` (\n  `id` int NOT NULL\n);\n")
	writeFile(t, dir, "demo.mydumper_7-schema-view.sql", "/*!40101 SET NAMES binary*/;\nCREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v.dotted` AS select 1 AS `x`;\n")
	_, views, _, err := DiscoverDumpNames(dir)
	if err != nil {
		t.Fatal(err)
	}
	wantNames(t, "views", viewNames(views), []string{"demo.v.dotted"})
}
