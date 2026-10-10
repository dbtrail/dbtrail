package cliapp

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// #1938: the console's full read removes a table's dump files once the table
// is converted, because that dump is its own. `bintrail baseline` converts a
// dump that is the operator's, with --retry to convert it again, so through
// this command the dump must come out exactly as it went in.
func TestRunBaseline_leavesTheDumpUntouched_1938(t *testing.T) {
	origInput, origOutput, origTS, origRetry := bslInput, bslOutput, bslTimestamp, bslRetry
	t.Cleanup(func() { bslInput, bslOutput, bslTimestamp, bslRetry = origInput, origOutput, origTS, origRetry })
	// The command is run directly, not through Execute, which is what gives
	// it a context.
	origCtx := baselineCmd.Context()
	baselineCmd.SetContext(context.Background())
	t.Cleanup(func() { baselineCmd.SetContext(origCtx) })

	const hdr = "/*!40101 SET NAMES utf8mb4*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n"
	in := t.TempDir()
	for name, body := range map[string]string{
		"metadata": "# Started dump at: 2026-10-02 01:57:48\n[config]\nquote-character = BACKTICK\n\n[source]\n" +
			"# SOURCE_LOG_FILE = \"binlog.000002\"\n# SOURCE_LOG_POS = 839\n\n# Finished dump at: 2026-10-02 01:57:49\n",
		"shop-schema-create.sql": hdr + "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `shop`;\n",
		"shop.orders-schema.sql": hdr + "CREATE TABLE `orders` (\n  `id` int(11) NOT NULL,\n  `label` varchar(30) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n",
		"shop.orders.00000.sql":  hdr + "INSERT INTO `orders` (`id`,`label`) VALUES(1,\"uno\")\n,(2,\"dos\")\n;\n",
		"shop.orders.00001.sql":  hdr + "INSERT INTO `orders` (`id`,`label`) VALUES(3,\"tres\")\n;\n",
	} {
		if err := os.WriteFile(filepath.Join(in, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	list := func() []string {
		entries, err := os.ReadDir(in)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range entries {
			fi, _ := e.Info()
			names = append(names, e.Name()+":"+fi.ModTime().String())
		}
		slices.Sort(names)
		return names
	}
	before := list()

	for _, retry := range []bool{false, true} {
		bslInput, bslOutput, bslTimestamp, bslRetry = in, t.TempDir(), "2026-10-10T13:51:42Z", retry
		if err := runBaseline(baselineCmd, nil); err != nil {
			t.Fatalf("retry=%v: %v", retry, err)
		}
		written, _ := filepath.Glob(filepath.Join(bslOutput, "*", "shop", "orders.parquet"))
		if len(written) == 0 {
			t.Fatalf("retry=%v: the table was not converted, so the dump was never at risk: %v", retry, written)
		}
		if after := list(); !slices.Equal(before, after) {
			t.Fatalf("retry=%v: the dump changed:\nbefore %v\n after %v", retry, before, after)
		}
	}
}
