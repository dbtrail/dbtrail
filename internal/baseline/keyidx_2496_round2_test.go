package baseline

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/keyidx"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// Review round on #2496 slice 2 at the conversion site: --retry builds the
// index a killed run left out, the index stays local on upload, builds run
// one at a time however many workers convert, and the header names the key
// in the file's order whatever order the PRIMARY KEY line declares.

// writeDumpTables writes a mydumper 1.0.3-shaped dump of tables (name →
// CREATE TABLE body) under schema shop, each with two rows (1, 2) in its
// first integer column.
func writeDumpTables(t *testing.T, dir string, tables map[string]string, rows map[string]string) {
	t.Helper()
	files := map[string]string{
		"metadata": "# Started dump at: 2026-10-02 01:57:48\n[config]\nquote-character = BACKTICK\n\n[source]\n" +
			"# SOURCE_LOG_FILE = \"binlog.000002\"\n# SOURCE_LOG_POS = 839\n\n# Finished dump at: 2026-10-02 01:57:49\n",
		"shop-schema-create.sql": dumpHeader1938 + "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `shop`;\n",
	}
	for name, body := range tables {
		files["shop."+name+"-schema.sql"] = dumpHeader1938 + "CREATE TABLE `" + name + "` (\n" + body + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
		r, ok := rows[name]
		if !ok {
			r = "INSERT INTO `" + name + "` (`id`,`label`) VALUES(1,\"uno\")\n,(2,\"dos\")\n;\n"
		}
		files["shop."+name+".00000.sql"] = dumpHeader1938 + r
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// headerNames reads the key column names off the index's documented header:
// the type codes at 40, then a 2-byte length and the bytes per name.
func headerNames(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n := int(b[10])
	off := int(keyidx.HeaderSize(nil)) + n // the fixed header, then the type codes
	var names []string
	for range n {
		l := int(binary.BigEndian.Uint16(b[off:]))
		names = append(names, string(b[off+2:off+2+l]))
		off += 2 + l
	}
	return names
}

func TestRun_retryBuildsTheMissingIndex_2496(t *testing.T) {
	in, out := t.TempDir(), t.TempDir()
	writeDump1938(t, in, false)
	at := time.Date(2026, 10, 10, 13, 51, 42, 0, time.UTC)
	cfg := Config{InputDir: in, OutputDir: out, Compression: "zstd", Timestamp: at}
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(out, snapshotdir.Name(at))
	a, b := filepath.Join(snap, "shop", "a.parquet"), filepath.Join(snap, "shop", "b.parquet")
	// As a run killed between the conversion and the build leaves things:
	// a has no index and a .tmp; b has an index that is not its own.
	if err := os.Remove(keyidx.PathFor(a)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyidx.PathFor(a)+".tmp", []byte("half"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(keyidx.PathFor(b)); err != nil {
		t.Fatal(err)
	}
	if ok, _ := keyidx.Bound(b); ok {
		t.Fatal("fixture: b still has its index")
	}
	var builds atomic.Int32
	keyidx.BuildHook = func() { builds.Add(1) }
	t.Cleanup(func() { keyidx.BuildHook = nil })
	cfg.Retry = true
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	for _, base := range []string{a, b} {
		if ok, err := keyidx.Bound(base); !ok || err != nil {
			t.Fatalf("after --retry, %s: bound %v err %v", base, ok, err)
		}
	}
	if _, err := os.Stat(keyidx.PathFor(a) + ".tmp"); err == nil {
		t.Fatal("the leftover .tmp survived the retry")
	}
	if builds.Load() != 2 {
		t.Fatalf("%d builds on retry, want 2", builds.Load())
	}
	// Everything bound: a further retry builds nothing and says so.
	logs := captureLogs2496(t)
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if builds.Load() != 2 || strings.Count(logs.String(), `msg="key index present"`) != 2 {
		t.Fatalf("a retry over bound indexes: %d builds, log:\n%s", builds.Load(), logs.String())
	}
}

func TestUpload_leavesTheKeyIndexLocal_2496(t *testing.T) {
	outputDir := t.TempDir()
	snap := filepath.Join(outputDir, "2025-01-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"shop/orders.parquet", "shop/orders.keyidx", "shop/orders.keyidx.tmp", "shop/orders.000001.posdel", SuccessMarker} {
		if err := os.WriteFile(filepath.Join(snap, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var uploaded []string
	ops := s3UploadOps{
		putEmpty:     func(context.Context, string) error { return nil },
		uploadFile:   func(_ context.Context, _, k string) error { uploaded = append(uploaded, filepath.Base(k)); return nil },
		objectExists: func(context.Context, string) (bool, error) { return false, nil },
		deleteObject: func(context.Context, string) error { return nil },
	}
	n, err := uploadWithOps(context.Background(), outputDir, "p", false, lockedOps(ops))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(uploaded)
	if want := []string{SuccessMarker, "orders.000001.posdel", "orders.parquet"}; !slices.Equal(uploaded, want) || n != 3 {
		t.Fatalf("uploaded %v (%d), want %v", uploaded, n, want)
	}
	for name, want := range map[string]bool{"t.keyidx": true, "t.keyidx.tmp": true, "t.parquet": false, "t.posdel": false, "_MANIFEST": false} {
		if isKeyIndexArtifact(name) != want {
			t.Errorf("isKeyIndexArtifact(%q) = %v", name, !want)
		}
	}
}

func TestRun_buildsOneIndexAtATime_2496(t *testing.T) {
	in, out := t.TempDir(), t.TempDir()
	tables := map[string]string{}
	for i := range 6 {
		tables[fmt.Sprintf("t%d", i)] = "  `id` int(11) NOT NULL,\n  `label` varchar(30) DEFAULT NULL,\n  PRIMARY KEY (`id`)"
	}
	writeDumpTables(t, in, tables, nil)
	var cur, peak atomic.Int32
	keyidx.BuildHook = func() {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
	}
	t.Cleanup(func() { keyidx.BuildHook = nil })
	at := time.Date(2026, 10, 10, 13, 51, 42, 0, time.UTC)
	if _, err := Run(context.Background(), Config{InputDir: in, OutputDir: out, Compression: "zstd", Timestamp: at}); err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 1 {
		t.Fatalf("%d index builds ran at once, want 1", peak.Load())
	}
	snap := filepath.Join(out, snapshotdir.Name(at))
	for name := range tables {
		if ok, err := keyidx.Bound(filepath.Join(snap, "shop", name+".parquet")); !ok || err != nil {
			t.Fatalf("%s: bound %v err %v", name, ok, err)
		}
	}
}

// An order_line-shaped table: the columns in table order (o, d, w, number),
// the PRIMARY KEY declared (w, d, o, number). The index names the key in
// the FILE's order, which the converter makes alphabetical.
func TestRun_indexNamesFollowTheFileOrder_2496(t *testing.T) {
	in, out := t.TempDir(), t.TempDir()
	writeDumpTables(t, in, map[string]string{
		"order_line": "  `ol_o_id` int NOT NULL,\n  `ol_d_id` int NOT NULL,\n  `ol_w_id` int NOT NULL,\n  `ol_number` int NOT NULL,\n  `ol_qty` int DEFAULT NULL,\n  PRIMARY KEY (`ol_w_id`,`ol_d_id`,`ol_o_id`,`ol_number`)",
	}, map[string]string{
		"order_line": "INSERT INTO `order_line` (`ol_o_id`,`ol_d_id`,`ol_w_id`,`ol_number`,`ol_qty`) VALUES(3,2,1,1,5)\n,(3,2,1,2,6)\n,(1,1,1,1,7)\n;\n",
	})
	at := time.Date(2026, 10, 10, 13, 51, 42, 0, time.UTC)
	if _, err := Run(context.Background(), Config{InputDir: in, OutputDir: out, Compression: "zstd", Timestamp: at}); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(out, snapshotdir.Name(at), "shop", "order_line.parquet")
	if got := keyidx.PrimaryKeyColumns(mustRead(t, filepath.Join(in, "shop.order_line-schema.sql"))); !slices.Equal(got, []string{"ol_w_id", "ol_d_id", "ol_o_id", "ol_number"}) {
		t.Fatalf("the dump declares the key %q", got)
	}
	if got, want := headerNames(t, keyidx.PathFor(base)), []string{"ol_d_id", "ol_number", "ol_o_id", "ol_w_id"}; !slices.Equal(got, want) {
		t.Fatalf("header names %q, want the file's order %q", got, want)
	}
}

// captureLogs2496 routes the default logger into a buffer at Debug level
// (process-global: not parallel).
func captureLogs2496(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func mustRead(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
