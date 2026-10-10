package baseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

const dumpHeader1938 = "/*!40101 SET NAMES utf8mb4*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n"

// writeDump1938 writes what mydumper 1.0.3 leaves for two small tables. With
// cut, the second table's data stops in the middle of a statement.
func writeDump1938(t *testing.T, dir string, cut bool) {
	t.Helper()
	table := func(name string) string {
		return dumpHeader1938 + "CREATE TABLE `" + name + "` (\n  `id` int(11) NOT NULL,\n  `label` varchar(30) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
	}
	second := dumpHeader1938 + "INSERT INTO `b` (`id`,`label`) VALUES(1,\"uno\")\n,(2,\"dos\")\n;\n"
	if cut {
		second = dumpHeader1938 + "INSERT INTO `b` (`id`,`label`) VALUES(1,\"uno\")\n,(2,\"d"
	}
	files := map[string]string{
		"metadata": "# Started dump at: 2026-10-02 01:57:48\n[config]\nquote-character = BACKTICK\n\n[source]\n" +
			"# SOURCE_LOG_FILE = \"binlog.000002\"\n# SOURCE_LOG_POS = 839\n\n# Finished dump at: 2026-10-02 01:57:49\n",
		"shop-schema-create.sql": dumpHeader1938 + "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `shop`;\n",
		"shop.a-schema.sql":      table("a"),
		"shop.a.00000.sql":       dumpHeader1938 + "INSERT INTO `a` (`id`,`label`) VALUES(1,\"uno\")\n,(2,\"dos\")\n;\n",
		"shop.b-schema.sql":      table("b"),
		"shop.b.00000.sql":       second,
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// #1938: a caller that removes the folder of a failed run has to tell a run
// that left a PARTIAL snapshot from one that wrote every table and could not
// finish it. Only Run knows which, so it says so with a type; the words of the
// failure are what they were.
func TestRun_aFailureAfterEveryTableSaysSo_1938(t *testing.T) {
	at := time.Date(2026, 10, 10, 13, 51, 42, 0, time.UTC)
	cfg := func(in, out string) Config {
		return Config{InputDir: in, OutputDir: out, Compression: "zstd", Timestamp: at}
	}

	t.Run("a table that does not convert: a partial snapshot", func(t *testing.T) {
		in, out := t.TempDir(), t.TempDir()
		writeDump1938(t, in, true)
		_, err := Run(context.Background(), cfg(in, out))
		if err == nil || errors.Is(err, ErrAllTablesConverted) {
			t.Fatalf("err = %v, want a table failure that is not ErrAllTablesConverted", err)
		}
	})

	t.Run("the manifest cannot be written: every table is there", func(t *testing.T) {
		in, out := t.TempDir(), t.TempDir()
		writeDump1938(t, in, false)
		// A folder where the manifest file goes: the one write that fails
		// comes after the last table.
		snap := filepath.Join(out, snapshotdir.Name(at))
		if err := os.MkdirAll(filepath.Join(snap, baselineintegrity.ManifestName, "x"), 0o755); err != nil {
			t.Fatal(err)
		}
		_, err := Run(context.Background(), cfg(in, out))
		t.Logf("error: %v", err)
		if !errors.Is(err, ErrAllTablesConverted) {
			t.Fatalf("err = %v, want ErrAllTablesConverted", err)
		}
		if !strings.HasPrefix(err.Error(), "snapshot complete but could not write integrity manifest: ") {
			t.Fatalf("the message changed: %v", err)
		}
		// What the type promises: both tables are on disk, and the folder is
		// still marked incomplete.
		parquet, _ := filepath.Glob(filepath.Join(snap, "*", "*.parquet"))
		if flat, _ := filepath.Glob(filepath.Join(snap, "*.parquet")); len(flat) > 0 {
			parquet = append(parquet, flat...)
		}
		if len(parquet) != 2 {
			t.Fatalf("parquet files = %v, want one per table", parquet)
		}
		if _, err := os.Stat(filepath.Join(snap, IncompleteMarker)); err != nil {
			t.Fatalf("the folder is not marked incomplete: %v", err)
		}
		if _, err := os.Stat(filepath.Join(snap, SuccessMarker)); err == nil {
			t.Fatal("a snapshot without its manifest was published")
		}
	})

	t.Run("a complete run is no error at all", func(t *testing.T) {
		in, out := t.TempDir(), t.TempDir()
		writeDump1938(t, in, false)
		if _, err := Run(context.Background(), cfg(in, out)); err != nil {
			t.Fatalf("a whole dump did not convert: %v", err)
		}
	})
}
