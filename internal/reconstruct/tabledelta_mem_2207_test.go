package reconstruct

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2207: where the memory of an in-place rewrite goes. Not a regression test:
// it runs only when BINTRAIL_2207_PROBE is set, one probe per process so the
// numbers of one do not carry another's leftovers.
//
//	BINTRAIL_2207_PROBE    build | scan | paged | rewrite
//	BINTRAIL_2207_DIR      work directory, shared by the probes (required)
//	BINTRAIL_2207_ROWS     rows in each base (build)
//	BINTRAIL_2207_WINDOWS  windows in the chain (build, default 10)
//	BINTRAIL_2207_CHANGES  changed rows per window (build, default ROWS/40)
//	BINTRAIL_2207_PAR      tables (build) / tables rewritten at once (rewrite)
//	BINTRAIL_2207_THREADS, BINTRAIL_2207_MEMLIMIT  DuckDB tuning (unset = the
//	                       daemon's: duckdbutil.DefaultTuning)
//
// The table is shaped like sysbench-tpcc's stock: an int key, ten char(24),
// a varchar(50) and four small numbers. Linux only (/proc/self/statm).
func TestRewriteMemoryProfile2207(t *testing.T) {
	probe := os.Getenv("BINTRAIL_2207_PROBE")
	if probe == "" {
		t.Skip("set BINTRAIL_2207_PROBE to profile the rewrite (#2207)")
	}
	if runtime.GOOS != "linux" {
		t.Skip("samples /proc/self/statm")
	}
	work := os.Getenv("BINTRAIL_2207_DIR")
	if work == "" {
		t.Fatal("BINTRAIL_2207_DIR is required")
	}
	tuning := duckdbutil.Tuning{Threads: envInt(t, "BINTRAIL_2207_THREADS", 0), MemoryLimit: os.Getenv("BINTRAIL_2207_MEMLIMIT")}
	prevFrac, prevFloor := tableDeltaMaxFraction, tableDeltaMinCompactBytes
	tableDeltaMaxFraction = 1e9
	t.Cleanup(func() { tableDeltaMaxFraction, tableDeltaMinCompactBytes = prevFrac, prevFloor })
	statePath := filepath.Join(work, "state.json")

	if probe == "build" {
		build2207(t, work, statePath)
		return
	}
	var st state2207
	b, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("run the build probe first: %v", err)
	}
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	switch probe {
	case "scan":
		peak := sampleMemory(func() {
			_, err := mergeBaselineImages(ctx, mergeCore{
				LocalBaselinePath: st.Tables[0].Base, Schema: "mydb", Table: "orders",
				PKCols: pkColsIntID(), Changes: map[string]*query.ResultRow{}, DuckDBTuning: tuning,
			}, func(map[string]any) error { return nil })
			if err != nil {
				t.Error(err)
			}
		})
		t.Logf("merge read of one base (today's scan): %s", peak)
	case "paged":
		paged2207(t, st, tuning)
	case "rewrite":
		par := envInt(t, "BINTRAIL_2207_PAR", len(st.Tables))
		tableDeltaMaxFraction, tableDeltaMinCompactBytes = 0, 0
		peak := sampleMemory(func() {
			var wg sync.WaitGroup
			for i, tb := range st.Tables[:par] {
				wg.Go(func() {
					rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
					m := wideWindow(rng, st.Rows, st.Changes, st.Windows+1)
					at := tb.PrevTime.Add(5*time.Minute + time.Duration(rng.Intn(1e6))*time.Second)
					cut := &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * (st.Windows + 1))}
					out, rep, err := deltaWindow(t, tb.Root, tb.Base, tb.PrevTime, m, at, cut, func(p *tableDeltaPublish) {
						p.cfg.DuckDBTuning = tuning
					})
					defer os.RemoveAll(filepath.Dir(filepath.Dir(out)))
					if err != nil {
						t.Error(err)
						return
					}
					if rep.DeltaCompacted == "" {
						t.Errorf("table %d: the window was not a rewrite", i)
					}
				})
			}
			wg.Wait()
		})
		t.Logf("whole rewrite, %d table(s) at once: %s", par, peak)
	default:
		t.Fatalf("unknown probe %q", probe)
	}
}

type table2207 struct {
	Base, Root string
	PrevTime   time.Time
}

type state2207 struct {
	Rows, Changes, Windows int
	Tables                 []table2207
}

func build2207(t *testing.T, work, statePath string) {
	st := state2207{Rows: envInt(t, "BINTRAIL_2207_ROWS", 0), Windows: envInt(t, "BINTRAIL_2207_WINDOWS", 10)}
	if st.Rows == 0 {
		t.Fatal("BINTRAIL_2207_ROWS is required to build")
	}
	st.Changes = envInt(t, "BINTRAIL_2207_CHANGES", st.Rows/40)
	par := envInt(t, "BINTRAIL_2207_PAR", 1)
	t0 := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	for i := range par {
		start := time.Now()
		dir := filepath.Join(work, fmt.Sprintf("t%d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		base := writeStockBaseline(t, filepath.Join(dir, "orders.parquet"), st.Rows)
		root := filepath.Join(dir, "snaps")
		rng := rand.New(rand.NewSource(int64(i + 1)))
		prevTime := t0
		for w := range st.Windows {
			at := t0.Add(time.Duration(w+1) * 5 * time.Minute)
			cut := &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * (w + 1))}
			nb, _, err := deltaWindow(t, root, base, prevTime, wideWindow(rng, st.Rows, st.Changes, w+1), at, cut, nil)
			if err != nil {
				t.Fatalf("table %d window %d: %v", i, w, err)
			}
			base, prevTime = nb, at
		}
		st.Tables = append(st.Tables, table2207{base, root, prevTime})
		t.Logf("table %d: base %s, chain %s, built in %s", i, fileSize(t, base), chainSize(t, base), time.Since(start).Round(time.Second))
	}
	b, _ := json.Marshal(st)
	if err := os.WriteFile(statePath, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// paged2207 reads one base a page at a time by file_row_number and reports
// memory, the time of the first and last page against a count(*), and the
// plan of the last page: whether DuckDB skips the row groups outside it.
func paged2207(t *testing.T, st state2207, tuning duckdbutil.Tuning) {
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	applyDuckDBTuning(context.Background(), ddb, tuning)
	lit := "'" + strings.ReplaceAll(st.Tables[0].Base, "'", "''") + "'"
	const page = ParquetWriterRowGroupSize
	pageQ := func(lo int) string {
		return fmt.Sprintf("SELECT * EXCLUDE (file_row_number) FROM read_parquet(%s, file_row_number = true) WHERE file_row_number >= %d AND file_row_number < %d", lit, lo, lo+page)
	}
	drain := func(q string) (int, time.Duration) {
		start := time.Now()
		r, err := ddb.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		cols, _ := r.Columns()
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		n := 0
		for r.Next() {
			if err := r.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			n++
		}
		return n, time.Since(start)
	}
	_, tc := drain("SELECT count(*) FROM read_parquet(" + lit + ")")
	n0, t0 := drain(pageQ(0))
	last := (st.Rows - 1) / page * page
	nl, tl := drain(pageQ(last))
	t.Logf("count(*) %s; first page %d rows in %s; last page (from row %d) %d rows in %s",
		tc.Round(time.Millisecond), n0, t0.Round(time.Millisecond), last, nl, tl.Round(time.Millisecond))
	var plan strings.Builder
	r, err := ddb.Query("EXPLAIN ANALYZE " + pageQ(last))
	if err != nil {
		t.Fatal(err)
	}
	for r.Next() {
		var k, v string
		if err := r.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(v)
	}
	r.Close()
	for _, line := range strings.Split(plan.String(), "\n") {
		l := strings.Trim(line, " │┃|")
		if strings.Contains(l, "Rows") || strings.Contains(l, "file_row_number") || strings.Contains(l, "SCAN") || strings.Contains(l, "Total Time") {
			t.Log(l)
		}
	}
	runtime.GC()
	peak := sampleMemory(func() {
		for lo := 0; lo < st.Rows; lo += page {
			drain(pageQ(lo))
		}
	})
	t.Logf("paged read of one base (%d rows a page): %s", page, peak)
}

const stockCreateTableSQL = "CREATE TABLE `orders` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `s_w_id` smallint NOT NULL,\n" +
	"  `s_quantity` smallint DEFAULT NULL,\n" +
	"  `s_dist_01` char(24) DEFAULT NULL,\n  `s_dist_02` char(24) DEFAULT NULL,\n  `s_dist_03` char(24) DEFAULT NULL,\n" +
	"  `s_dist_04` char(24) DEFAULT NULL,\n  `s_dist_05` char(24) DEFAULT NULL,\n  `s_dist_06` char(24) DEFAULT NULL,\n" +
	"  `s_dist_07` char(24) DEFAULT NULL,\n  `s_dist_08` char(24) DEFAULT NULL,\n  `s_dist_09` char(24) DEFAULT NULL,\n" +
	"  `s_dist_10` char(24) DEFAULT NULL,\n" +
	"  `s_ytd` decimal(8,0) DEFAULT NULL,\n" +
	"  `s_order_cnt` smallint DEFAULT NULL,\n" +
	"  `s_remote_cnt` smallint DEFAULT NULL,\n" +
	"  `s_data` varchar(50) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB;\n"

var stockDist = func() []string {
	var out []string
	for i := 1; i <= 10; i++ {
		out = append(out, fmt.Sprintf("s_dist_%02d", i))
	}
	return out
}()

// stockText is a value of n characters that differs per row and per version,
// so zstd does not collapse it the way it would a constant.
func stockText(id, version, col, n int) string {
	s := strconv.FormatUint(uint64(id)*2654435761+uint64(version*97+col*13), 36)
	for len(s) < n {
		s += s
	}
	return s[:n]
}

func stockRow(id, version int) map[string]any {
	row := map[string]any{
		"id": float64(id), "s_w_id": float64(id % 200), "s_quantity": float64((id + version) % 100),
		"s_ytd": float64(version), "s_order_cnt": float64(version), "s_remote_cnt": float64(0),
		"s_data": stockText(id, version, 99, 26+id%25),
	}
	for c, name := range stockDist {
		row[name] = stockText(id, 0, c, 24)
	}
	return row
}

func wideWindow(rng *rand.Rand, rows, changes, version int) map[string]*query.ResultRow {
	m := make(map[string]*query.ResultRow, changes)
	for range changes {
		id := 1 + rng.Intn(rows)
		m[pkStrForInt(id)] = &query.ResultRow{PKValues: pkStrForInt(id), EventType: event.EventUpdate, RowAfter: stockRow(id, version)}
	}
	return m
}

func writeStockBaseline(t *testing.T, path string, rows int) string {
	t.Helper()
	cols, err := baseline.ParseSchemaText(stockCreateTableSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  ParquetWriterCompression,
		RowGroupSize: ParquetWriterRowGroupSize,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: stockCreateTableSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000007",
			baseline.MetaKeyBinlogPos:      "4",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	vals := make([]string, len(cols))
	nulls := make([]bool, len(cols))
	for id := 1; id <= rows; id++ {
		row := stockRow(id, 0)
		for i, c := range cols {
			switch v := row[c.Name].(type) {
			case float64:
				vals[i] = strconv.FormatFloat(v, 'f', -1, 64)
			case string:
				vals[i] = v
			}
		}
		if err := w.WriteRow(vals, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

type memPeak struct {
	rss, heapInuse, goSys uint64
	took                  time.Duration
}

func (p memPeak) String() string {
	return fmt.Sprintf("peak RSS %s, peak Go heap in use %s, Go runtime total %s, took %s",
		mib(p.rss), mib(p.heapInuse), mib(p.goSys), p.took.Round(100*time.Millisecond))
}

func mib(b uint64) string { return fmt.Sprintf("%.0f MiB", float64(b)/(1<<20)) }

// sampleMemory runs f and reports the highest resident memory and Go heap seen
// while it ran. The RSS minus the Go runtime's total is memory the Go runtime
// does not own: DuckDB's, allocated through cgo.
func sampleMemory(f func()) memPeak {
	var p memPeak
	var stop atomic.Bool
	done := make(chan struct{})
	page := uint64(os.Getpagesize())
	go func() {
		defer close(done)
		var ms runtime.MemStats
		for !stop.Load() {
			if b, err := os.ReadFile("/proc/self/statm"); err == nil {
				if f := strings.Fields(string(b)); len(f) > 1 {
					if n, err := strconv.ParseUint(f[1], 10, 64); err == nil {
						p.rss = max(p.rss, n*page)
					}
				}
			}
			runtime.ReadMemStats(&ms)
			p.heapInuse = max(p.heapInuse, ms.HeapInuse)
			p.goSys = max(p.goSys, ms.Sys)
			time.Sleep(100 * time.Millisecond)
		}
	}()
	start := time.Now()
	f()
	p.took = time.Since(start)
	stop.Store(true)
	<-done
	return p
}

func envInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", key, v, err)
	}
	return n
}

func fileSize(t *testing.T, path string) string {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return mib(uint64(fi.Size()))
}

func chainSize(t *testing.T, base string) string {
	t.Helper()
	chain, err := baseline.ListTableDelta(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	for _, f := range chain.Files {
		for _, p := range []string{f.Posdel, f.Upserts} {
			if fi, err := os.Stat(p); err == nil {
				n += uint64(fi.Size())
			}
		}
	}
	return fmt.Sprintf("%s in %d pairs", mib(n), len(chain.Files))
}
