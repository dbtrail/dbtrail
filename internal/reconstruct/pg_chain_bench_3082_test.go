package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestPGChainUpdateCostBench measures what one PostgreSQL snapshot update
// costs as updates accumulate beside one table file (#3082): a table of
// BINTRAIL_PG_CHAIN_BENCH_ROWS rows, then BINTRAIL_PG_CHAIN_BENCH_UPDATES
// updates of BINTRAIL_PG_CHAIN_BENCH_CHANGES changed keys each (three in
// four update a row of the file, one in four inserts a new one), each
// published as the file and its delta the way a snapshot update publishes
// it (pgPrevPairOf, then writePGPair). After each update it reads the table
// through the delta as a statement on the copy does (a count over
// pgdelta.StateSQL) and reads 200 changed keys' values from it as fresh mode
// does (pgdelta.CutValuesOf), and prints one line per update.
//
// It runs only when BINTRAIL_PG_CHAIN_BENCH is set, and calls only what a
// build before #3082 has too, so the same file measures both.
func TestPGChainUpdateCostBench(t *testing.T) {
	if os.Getenv("BINTRAIL_PG_CHAIN_BENCH") == "" {
		t.Skip("set BINTRAIL_PG_CHAIN_BENCH=1 to measure")
	}
	num := func(name string, def int) int {
		if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
			return v
		}
		return def
	}
	rowsN := num("BINTRAIL_PG_CHAIN_BENCH_ROWS", 4_000_000)
	updates := num("BINTRAIL_PG_CHAIN_BENCH_UPDATES", 40)
	changes := num("BINTRAIL_PG_CHAIN_BENCH_CHANGES", 20_000)
	ctx := context.Background()
	prevSwitch := pgTableDeltas
	pgTableDeltas = func() bool { return true }
	t.Cleanup(func() { pgTableDeltas = prevSwitch })

	root := os.Getenv("BINTRAIL_PG_CHAIN_BENCH_DIR")
	if root == "" {
		root = t.TempDir()
	} else {
		root = filepath.Join(root, fmt.Sprintf("bench-%d", time.Now().UnixNano()))
		t.Cleanup(func() { os.RemoveAll(root) })
	}
	rel := baseline.PGRelation{OID: 16400, Columns: []baseline.PGColumn{
		{Name: "id", TypeOID: 25, TypeMod: -1, Type: "text", NotNull: true, Ordinal: 1},
		{Name: "status", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 2},
		{Name: "payload", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 3},
	}, PrimaryKey: []string{"id"}}
	relText, err := rel.Encode()
	if err != nil {
		t.Fatal(err)
	}
	cols := []string{"id", "status", "payload"}
	key := func(i int) string { return fmt.Sprintf("%010d", i) }
	payload := func(i, u int) string {
		return fmt.Sprintf("payload of row %d at update %d, some text to give a row its width", i, u)
	}

	t0 := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	base := filepath.Join(root, SnapshotDirName(t0), "public", "orders.parquet")
	md := map[string]string{
		baseline.MetaKeySnapshotTimestamp: t0.Format(time.RFC3339),
		baseline.MetaKeyLSN:               "4096",
		baseline.MetaKeyPGRelation:        relText,
		baseline.MetaKeyRenderGUCs:        baseline.RenderGUCsPinned,
		baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
		baseline.MetaKeyPGDeltaSince:      t0.Add(-time.Minute).Format(time.RFC3339),
	}
	writeAt := time.Now()
	w, err := baseline.NewWriter(base, []baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}, {Name: "payload", RawText: true}},
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100_000, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	for i := range rowsN {
		if err := w.WriteRow([]string{key(i), "new", payload(i, 0)}, []bool{false, false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("bench: base of %d rows written in %s\n", rowsN, time.Since(writeAt).Round(time.Millisecond))

	pkCols := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "text"}}
	cfg := FullTableConfig{OutputFormat: OutputFormatParquet, CarryForwardUnchanged: true}
	rng := rand.New(rand.NewPCG(1, 3082))
	src := base
	next, lsn := rowsN, uint64(8192)
	fmt.Println("bench: update,ms,written,files_beside,delta_rows,read_ms,values_ms")
	for u := 1; u <= updates; u++ {
		at := t0.Add(time.Duration(u) * 5 * time.Minute)
		bmeta, err := baseline.ReadParquetMetadata(src)
		if err != nil {
			t.Fatal(err)
		}
		fold := &foldResult{Changes: map[string]*query.ResultRow{}}
		var sample []string
		for len(fold.Changes) < changes {
			var i int
			typ := event.EventUpdate
			if rng.IntN(4) == 0 {
				i, next, typ = next, next+1, event.EventInsert
				fold.Inserts++
			} else {
				i = rng.IntN(rowsN)
				fold.Updates++
			}
			k := key(i)
			if _, twice := fold.Changes[k]; twice {
				continue
			}
			fold.Changes[k] = &query.ResultRow{EventType: typ, PKValues: k,
				RowAfter: map[string]any{"id": k, "status": fmt.Sprintf("u%d", u), "payload": payload(i, u)}}
			if len(sample) < 200 {
				sample = append(sample, k)
			}
		}
		fold.Total = int64(len(fold.Changes))
		lsn += 4096
		in := mergeInput{
			LocalBaselinePath: src, Schema: "public", Table: "orders", PKCols: pkCols,
			Changes: fold.Changes, SnapshotDir: filepath.Join(root, SnapshotDirName(at)), SnapshotAt: at,
			SourceBaseline: baselineMeta{Path: src, Time: t0, Metadata: bmeta},
			PG:             true, PGCutLSN: lsn, PGDeltaSince: at.Add(-time.Minute),
		}
		start := time.Now()
		prev, why := pgPrevPairOf(ctx, cfg, src, bmeta, cols)
		if why != "" {
			t.Fatalf("update %d: the delta before is not continued: %s", u, why)
		}
		if prev != nil && prev.over != "" {
			fmt.Printf("bench: update %d: the delta before is over (%s)\n", u, prev.over)
		}
		end := ""
		if prev != nil {
			end = prev.over
		}
		written, err := writePGPair(ctx, in, fold, &TableReport{}, prev, end, start)
		took := time.Since(start)
		if err != nil || !written {
			t.Fatalf("update %d: written %v, %v", u, written, err)
		}
		src = filepath.Join(in.SnapshotDir, "public", "orders.parquet")
		dead, rows, err := pgCutChain(src)
		if err != nil {
			t.Fatal(err)
		}
		duck, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		var deltaRows int64
		if len(rows) > 0 {
			if err := duck.QueryRowContext(ctx, "SELECT coalesce(sum(num_rows), 0) FROM parquet_file_metadata(["+quotedList(rows)+"])").Scan(&deltaRows); err != nil {
				t.Fatal(err)
			}
		}
		readAt := time.Now()
		var n int64
		if err := duck.QueryRowContext(ctx, "SELECT count(*) FROM ("+pgdelta.StateSQL(src, dead, rows)+")").Scan(&n); err != nil {
			t.Fatal(err)
		}
		read := time.Since(readAt)
		if want := int64(next); n != want {
			t.Fatalf("update %d: the table reads %d rows, want %d", u, n, want)
		}
		valuesAt := time.Now()
		got, err := pgdelta.CutValuesOf(ctx, duck, src, dead, rows, []string{"id"}, sample, []string{"status"}, 0)
		values := time.Since(valuesAt)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range sample {
			if v := got[k]["status"]; v == nil || *v != fmt.Sprintf("u%d", u) {
				t.Fatalf("update %d: key %s reads %v", u, k, v)
			}
		}
		duck.Close()
		fmt.Printf("bench: %d,%d,%v,%d,%d,%d,%d\n", u, took.Milliseconds(), written, 2*len(rows), deltaRows, read.Milliseconds(), values.Milliseconds())
	}
}

func quotedList(paths []string) string {
	out := ""
	for i, p := range paths {
		if i > 0 {
			out += ", "
		}
		out += "'" + p + "'"
	}
	return out
}
