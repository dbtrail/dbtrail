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
func TestDiscoverDump_SlashInNameFailsLoudly_2006(t *testing.T) {
	_, _, err := DiscoverDump(fixtureNames2006)
	if err == nil {
		t.Fatal("DiscoverDump accepted a table named order/items")
	}
	for _, want := range []string{"`demo`.`order/items`", "demo.mydumper_5-schema.sql", `"/"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say %s", err, want)
		}
	}
	if strings.Contains(err.Error(), "order.items") || strings.Contains(err.Error(), "注文") {
		t.Errorf("error names tables that are fine: %q", err)
	}
	out := t.TempDir()
	if _, err := Run(context.Background(), Config{InputDir: fixtureNames2006, OutputDir: out}); err == nil {
		t.Fatal("Run converted a dump holding order/items")
	}
}

// A schema whose name holds a dot: mydumper writes it as mydumper_0 and
// keeps the real name only in its CREATE DATABASE. Every consumer splits
// "schema.table" at the first dot, so it is refused rather than stored.
func TestDiscoverDump_DottedSchemaRefused_2006(t *testing.T) {
	_, _, err := DiscoverDump(fixtureDottedSchema2006)
	if err == nil {
		t.Fatal("DiscoverDump accepted schema my.db")
	}
	if !strings.Contains(err.Error(), "`my.db`") {
		t.Errorf("error %q does not name the real schema my.db", err)
	}
	if strings.Contains(err.Error(), "`mydumper_0`.") {
		t.Errorf("error names the made-up schema as if it were real: %q", err)
	}
}

// The console's multi-schema dump (--regex) writes no CREATE DATABASE file,
// so a renamed schema cannot be read back: refused, never published as
// mydumper_0.
func TestDiscoverDump_MadeUpSchemaWithoutCreateDatabase_2006(t *testing.T) {
	_, _, err := DiscoverDump(fixtureSchemaNoCreate2006)
	if err == nil {
		t.Fatal("DiscoverDump published schema mydumper_0")
	}
	if !strings.Contains(err.Error(), "mydumper_0") || !strings.Contains(err.Error(), "real name") {
		t.Errorf("error %q does not say the schema's real name is unknown", err)
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
func TestDiscoverDump_TwoFilesOneRealName_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "demo.mydumper_0-schema.sql", "CREATE TABLE `order.items` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n")
	writeFile(t, dir, "demo.mydumper_1-schema.sql", "CREATE TABLE `order.items` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n")
	_, _, err := DiscoverDump(dir)
	if err == nil || !strings.Contains(err.Error(), "`demo`.`order.items`") {
		t.Fatalf("err = %v, want a refusal naming demo.order.items", err)
	}
}

// metadata's real_table_name and the CREATE TABLE must agree; when they do
// not, the dump is not what it says and nothing is converted.
func TestDiscoverDump_MetadataDisagrees_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "demo.mydumper_0-schema.sql", "CREATE TABLE `order.items` (\n  `id` int NOT NULL\n);\n")
	writeFile(t, dir, "metadata", "[`demo`.`mydumper_0`]\nreal_table_name=order.lines\nrows = 0\n")
	_, _, err := DiscoverDump(dir)
	if err == nil || !strings.Contains(err.Error(), "order.lines") {
		t.Fatalf("err = %v, want a refusal naming both names", err)
	}
}

// A made-up file name whose CREATE TABLE cannot be read is refused even
// when metadata names it: a name with a line break cuts the metadata line.
func TestDiscoverDump_MetadataAloneIsNotEnough_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "demo.mydumper_0-schema.sql", "CREATE TABLE `a\nb` (\n  `id` int NOT NULL\n);\n")
	writeFile(t, dir, "metadata", "[`demo`.`mydumper_0`]\nreal_table_name=a\nb\nrows = 0\n")
	if _, _, err := DiscoverDump(dir); err == nil {
		t.Fatal("published a table under a name cut at its line break")
	}
}

// A dotted schema written raw into the file name splits wrongly at the
// first dot; the CREATE TABLE shows it, and the read is refused.
func TestDiscoverDump_RawDottedSchemaRefused_2006(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "my.db.t-schema.sql", "CREATE TABLE `t` (\n  `id` int NOT NULL\n);\n")
	_, _, err := DiscoverDump(dir)
	if err == nil || !strings.Contains(err.Error(), "`t`") {
		t.Fatalf("err = %v, want a refusal naming the real table", err)
	}
}
