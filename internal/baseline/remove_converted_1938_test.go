package baseline

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// #1938: a full read keeps mydumper's whole dump on disk until its last table
// is converted, so the peak is the dump plus the whole snapshot. With
// RemoveConvertedData a table's dump files go as soon as its Parquet file is
// closed, so the dump shrinks while the snapshot grows.

const hdr1938 = "/*!40101 SET NAMES utf8mb4*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n"

func schema1938(table string) string {
	return hdr1938 + "CREATE TABLE `" + table + "` (\n  `id` int(11) NOT NULL,\n  `label` varchar(30) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
}

func rows1938(table string, ids ...string) string {
	var vals []string
	for _, id := range ids {
		vals = append(vals, "("+id+",\"r"+id+"\")")
	}
	return hdr1938 + "INSERT INTO `" + table + "` (`id`,`label`) VALUES" + strings.Join(vals, "\n,") + "\n;\n"
}

// dump1938 is what mydumper 1.0.3 leaves for: orders in two data files, items
// in one, empty with a schema and no data, and a view. extra is added on top
// (and may replace a file).
func dump1938(t *testing.T, extra map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"metadata": "# Started dump at: 2026-10-02 01:57:48\n[config]\nquote-character = BACKTICK\n\n[source]\n" +
			"# SOURCE_LOG_FILE = \"binlog.000002\"\n# SOURCE_LOG_POS = 839\n\n# Finished dump at: 2026-10-02 01:57:49\n",
		"shop-schema-create.sql": hdr1938 + "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `shop`;\n",
		"shop.orders-schema.sql": schema1938("orders"),
		"shop.orders.00000.sql":  rows1938("orders", "1", "2"),
		"shop.orders.00001.sql":  rows1938("orders", "3"),
		"shop.items-schema.sql":  schema1938("items"),
		"shop.items.00000.sql":   rows1938("items", "1"),
		"shop.empty-schema.sql":  schema1938("empty"),
		// A view, the way mydumper 1.0.3 writes one (see the testdata folder
		// views_mydumper_1.0.3_mysql_8.0.46): a stand-in table and the view.
		"shop.v_orders-schema.sql": hdr1938 + "CREATE TABLE IF NOT EXISTS `v_orders`(\n`id` int\n) ENGINE=MEMORY ENCRYPTION='N';\n",
		"shop.v_orders-schema-view.sql": hdr1938 + "DROP TABLE IF EXISTS `v_orders`;\nDROP VIEW IF EXISTS `v_orders`;\n" +
			"CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v_orders` AS select `orders`.`id` AS `id` from `orders`;\n",
	}
	for name, body := range extra {
		files[name] = body
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func listing(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	slices.Sort(names)
	return names
}

var at1938 = time.Date(2026, 10, 10, 13, 51, 42, 0, time.UTC)

func digests(t *testing.T, out string) map[string]string {
	t.Helper()
	got := map[string]string{}
	files, _ := filepath.Glob(filepath.Join(out, snapshotdir.Name(at1938), "shop", "*.parquet"))
	for _, f := range files {
		md, err := ReadParquetMetadata(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if md.ContentDigest == "" {
			t.Fatalf("%s carries no content digest", f)
		}
		got[filepath.Base(f)] = md.ContentDigest
	}
	return got
}

func TestRun_removeConvertedData_1938(t *testing.T) {
	cfg := func(in, out string, remove bool) Config {
		return Config{InputDir: in, OutputDir: out, Compression: "zstd", Timestamp: at1938, RemoveConvertedData: remove}
	}

	t.Run("off, the default: the dump is left exactly as it was found", func(t *testing.T) {
		in, out := dump1938(t, nil), t.TempDir()
		before := listing(t, in)
		if _, err := Run(context.Background(), cfg(in, out, false)); err != nil {
			t.Fatal(err)
		}
		if after := listing(t, in); !slices.Equal(before, after) {
			t.Fatalf("the dump changed:\nbefore %v\n after %v", before, after)
		}
	})

	t.Run("on: the data files of converted tables go, and the snapshot is the same", func(t *testing.T) {
		in, out := dump1938(t, nil), t.TempDir()
		stats, err := Run(context.Background(), cfg(in, out, true))
		if err != nil {
			t.Fatal(err)
		}
		// Everything that is not a table's rows stays: the metadata, the
		// schemas (a schema is read after its table for nothing, but it is
		// small, and the view's is not a table's at all).
		want := []string{"metadata", "shop-schema-create.sql", "shop.empty-schema.sql", "shop.items-schema.sql",
			"shop.orders-schema.sql", "shop.v_orders-schema-view.sql", "shop.v_orders-schema.sql"}
		if got := listing(t, in); !slices.Equal(got, want) {
			t.Fatalf("left in the dump:\n got %v\nwant %v", got, want)
		}
		// The same rows as a run that kept the dump.
		in2, out2 := dump1938(t, nil), t.TempDir()
		stats2, err := Run(context.Background(), cfg(in2, out2, false))
		if err != nil {
			t.Fatal(err)
		}
		a, b := digests(t, out), digests(t, out2)
		if len(a) != 3 || len(b) != 3 {
			t.Fatalf("snapshots hold %d and %d tables, want orders, items and empty in both", len(a), len(b))
		}
		for name, d := range b {
			if a[name] != d {
				t.Errorf("%s: digest %q with the dump removed, %q with it kept", name, a[name], d)
			}
		}
		if stats.RowsWritten != 4 || stats.RowsWritten != stats2.RowsWritten || stats.TablesProcessed != stats2.TablesProcessed {
			t.Fatalf("stats %+v with the dump removed, %+v with it kept", stats, stats2)
		}
		if _, err := os.Stat(filepath.Join(out, snapshotdir.Name(at1938), SuccessMarker)); err != nil {
			t.Fatalf("the snapshot was not completed: %v", err)
		}
	})

	t.Run("on: a table that does not convert keeps ALL its data files", func(t *testing.T) {
		// orders' second file is cut in the middle of a statement, after its
		// first file converted cleanly.
		in := dump1938(t, map[string]string{"shop.orders.00001.sql": hdr1938 + "INSERT INTO `orders` (`id`,`label`) VALUES(3,\"r"})
		if _, err := Run(context.Background(), cfg(in, t.TempDir(), true)); err == nil {
			t.Fatal("a cut table converted")
		}
		got := listing(t, in)
		for _, keep := range []string{"shop.orders.00000.sql", "shop.orders.00001.sql"} {
			if !slices.Contains(got, keep) {
				t.Errorf("%s of the table that failed was removed: %v", keep, got)
			}
		}
		// The table beside it converted, and its data went: the failure is
		// the cut table's and not the whole dump's.
		if slices.Contains(got, "shop.items.00000.sql") {
			t.Errorf("no table converted, so this case proves nothing about the one that failed: %v", got)
		}
	})

	t.Run("on: a table the filter leaves out keeps its data", func(t *testing.T) {
		in, out := dump1938(t, nil), t.TempDir()
		c := cfg(in, out, true)
		c.Tables = []string{"shop.items"}
		if _, err := Run(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		got := listing(t, in)
		if slices.Contains(got, "shop.items.00000.sql") || !slices.Contains(got, "shop.orders.00000.sql") || !slices.Contains(got, "shop.orders.00001.sql") {
			t.Fatalf("left in the dump: %v", got)
		}
	})

	t.Run("on with retry is refused before anything is read or removed", func(t *testing.T) {
		in, out := dump1938(t, nil), t.TempDir()
		before := listing(t, in)
		c := cfg(in, out, true)
		c.Retry = true
		_, err := Run(context.Background(), c)
		if err == nil || !strings.Contains(err.Error(), "retry") {
			t.Fatalf("err = %v, want a refusal that names retry", err)
		}
		if after := listing(t, in); !slices.Equal(before, after) {
			t.Fatalf("the dump changed under a refused run: %v", after)
		}
		if made := listing(t, out); len(made) != 0 {
			t.Fatalf("a refused run wrote %v", made)
		}
	})

	t.Run("on: a data file that cannot be removed does not fail the run", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root removes anything")
		}
		in, out := dump1938(t, nil), t.TempDir()
		if err := os.Chmod(in, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(in, 0o755) })
		if _, err := Run(context.Background(), cfg(in, out, true)); err != nil {
			t.Fatalf("the run failed over a file it could not remove: %v", err)
		}
		if got := listing(t, in); !slices.Contains(got, "shop.orders.00000.sql") {
			t.Fatalf("the test did not keep the files: %v", got)
		}
		if len(digests(t, out)) != 3 {
			t.Fatal("the snapshot is not whole")
		}
	})
}

// The same over the REAL dumps kept as test data (mydumper 1.0.3: renamed
// mydumper_N tables, names with dots and spaces, views, a view's stand-in
// table, CSV/tab output with its LOAD DATA file): what is left is exactly
// every file that is not a converted table's data, and each table's rows are
// the ones a run that kept the dump wrote.
func TestRun_removeConvertedData_realDumps_1938(t *testing.T) {
	var dumps []string
	for _, pattern := range []string{"testdata/views_*", "testdata/mydumper-1.0.3-names/*"} {
		found, _ := filepath.Glob(pattern)
		for _, d := range found {
			if _, err := os.Stat(filepath.Join(d, "metadata")); err == nil {
				dumps = append(dumps, d)
			}
		}
	}
	if len(dumps) < 5 {
		t.Fatalf("only %d real dumps found under testdata: %v", len(dumps), dumps)
	}
	copyDump := func(t *testing.T, src string) string {
		dst := t.TempDir()
		if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
			t.Fatal(err)
		}
		return dst
	}
	parquetDigests := func(t *testing.T, out string) map[string]string {
		got := map[string]string{}
		files, _ := filepath.Glob(filepath.Join(out, snapshotdir.Name(at1938), "*", "*.parquet"))
		for _, f := range files {
			md, err := ReadParquetMetadata(f)
			if err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			got[filepath.Base(filepath.Dir(f))+"/"+filepath.Base(f)] = md.ContentDigest
		}
		return got
	}
	removedAny, keptWhole := false, false
	for _, src := range dumps {
		t.Run(src, func(t *testing.T) {
			kept, in := copyDump(t, src), copyDump(t, src)
			outKept, out := t.TempDir(), t.TempDir()
			_, errKept := Run(context.Background(), Config{InputDir: kept, OutputDir: outKept, Compression: "zstd", Timestamp: at1938})
			tables, _, _, derr := DiscoverDumpNames(in)
			if derr != nil {
				t.Fatalf("discover: %v", derr)
			}
			before := listing(t, in)
			stats, err := Run(context.Background(), Config{InputDir: in, OutputDir: out, Compression: "zstd", Timestamp: at1938, RemoveConvertedData: true})
			if (err == nil) != (errKept == nil) {
				t.Fatalf("with the dump kept: %v; with it removed as it goes: %v", errKept, err)
			}
			if err != nil {
				// This dump converts neither way. No table was written, so
				// no file may be gone.
				if stats.TablesProcessed != 0 {
					t.Fatalf("a real dump that fails with %d tables written is not covered here: %v", stats.TablesProcessed, err)
				}
				if got := listing(t, in); !slices.Equal(got, before) {
					t.Fatalf("no table converted (%v), and the dump changed:\n got %v\nwant %v", err, got, before)
				}
				keptWhole = true
				return
			}
			// Every data file of a table Run was given is gone, and nothing else.
			gone := map[string]bool{}
			for _, tf := range tables {
				for _, f := range tf.DataFiles {
					gone[filepath.Base(f)] = true
				}
			}
			var want []string
			for _, name := range before {
				if !gone[name] {
					want = append(want, name)
				}
			}
			if got := listing(t, in); !slices.Equal(got, want) {
				t.Fatalf("left in the dump:\n got %v\nwant %v", got, want)
			}
			if len(gone) > 0 {
				removedAny = true
			}
			a, b := parquetDigests(t, out), parquetDigests(t, outKept)
			if len(a) == 0 || len(a) != len(b) {
				t.Fatalf("%d tables with the dump removed, %d with it kept", len(a), len(b))
			}
			for name, d := range b {
				if a[name] != d {
					t.Errorf("%s: digest %q with the dump removed, %q with it kept", name, a[name], d)
				}
			}
		})
	}
	if !removedAny || !keptWhole {
		t.Fatalf("removed from a real dump: %v; a real dump that fails left whole: %v. The loop needs both", removedAny, keptWhole)
	}
}
