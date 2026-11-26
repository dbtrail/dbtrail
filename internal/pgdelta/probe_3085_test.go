package pgdelta

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// A measurement, not a test of behavior (#3085): run only with
// BINTRAIL_PG_KEY_PROBE=1, and it fails nothing on its numbers. On a table
// file of BINTRAIL_PG_KEY_PROBE_ROWS rows (10 million by default), written
// the way a snapshot writes one (zstd, 500,000 rows a row group, every column
// text), in a session with the daemon's budget on a small host (one thread,
// 512 MiB), it reports:
//
//   - how long each of BasePositionsOf's two passes over the key columns
//     takes, for 1,000 scattered keys: the pass for a NULL, and the join;
//   - whether the engine prunes row groups for the join on file_row_number
//     baseValuesAt reads values by: EXPLAIN ANALYZE and the time of that join
//     for 1,000 scattered row numbers, and for 1,000 consecutive ones (the
//     control: if those read one row group and the scattered ones the whole
//     file, it prunes and scattered keys cannot benefit; if both read the
//     whole file, it does not prune).
//
// And the key index of the same file: its build, its size, and the same
// 1,000 keys found through it.
func TestProbeKeyPasses3085(t *testing.T) {
	if os.Getenv("BINTRAIL_PG_KEY_PROBE") != "1" {
		t.Skip("a measurement: set BINTRAIL_PG_KEY_PROBE=1 to run it")
	}
	freshNoNullMemo(t)
	n := 10_000_000
	if v := os.Getenv("BINTRAIL_PG_KEY_PROBE_ROWS"); v != "" {
		fmt.Sscan(v, &n)
	}
	ctx := context.Background()
	dir := t.TempDir()
	base := filepath.Join(dir, "orders.parquet")
	cols := []baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}, {Name: "note", RawText: true}, {Name: "amount", RawText: true}}
	began := time.Now()
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "zstd", RowGroupSize: 500_000})
	if err != nil {
		t.Fatal(err)
	}
	nulls := []bool{false, false, false, false}
	for i := 0; i < n; i++ {
		if err := w.WriteRow([]string{fmt.Sprintf("ord-%010d", i), "shipped", fmt.Sprintf("a note of row %d for the probe", i), fmt.Sprintf("%d.%02d", i%9973, i%100)}, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(base)
	t.Logf("file: %d rows, %d MB, written in %s", n, fi.Size()>>20, time.Since(began).Round(time.Millisecond))
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(base, old, old); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, q := range []string{"SET threads=1", "SET memory_limit='512MiB'"} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	var version string
	_ = db.QueryRow("SELECT version()").Scan(&version)
	t.Logf("engine %s, threads=1, memory_limit=512MiB", version)

	rng := rand.New(rand.NewPCG(3085, 1))
	scattered := make([]int64, 0, 1000)
	seen := map[int64]bool{}
	for len(scattered) < 1000 {
		p := rng.Int64N(int64(n))
		if !seen[p] {
			seen[p] = true
			scattered = append(scattered, p)
		}
	}
	slices.Sort(scattered)
	consecutive := make([]int64, 1000)
	for i := range consecutive {
		consecutive[i] = int64(n/2) + int64(i)
	}
	keys := make([]string, len(scattered))
	for i, p := range scattered {
		keys[i] = fmt.Sprintf("ord-%010d", p)
	}

	report := func(what string, times []time.Duration) {
		sorted := slices.Clone(times)
		slices.Sort(sorted)
		t.Logf("%s: first %s, median of %d %s", what, times[0].Round(time.Millisecond), len(times), sorted[len(sorted)/2].Round(time.Millisecond))
	}
	timed := func(runs int, f func()) []time.Duration {
		var out []time.Duration
		for i := 0; i < runs; i++ {
			at := time.Now()
			f()
			out = append(out, time.Since(at))
		}
		return out
	}

	src := "read_parquet(" + file(base) + ", file_row_number=true" + noHive + ")"
	report("the pass for a NULL key alone", timed(3, func() {
		var bad sql.NullInt64
		if err := db.QueryRowContext(ctx, `SELECT min(file_row_number) FROM `+src+` WHERE "id" IS NULL`).Scan(&bad); err != nil || bad.Valid {
			t.Fatal(err, bad)
		}
	}))
	var both, joinOnly []time.Duration
	for i := 0; i < 3; i++ {
		freshNoNullMemo(t)
		both = append(both, timed(1, func() {
			if got, err := BasePositionsOf(ctx, db, base, []string{"id"}, keys); err != nil || len(got) != len(keys) {
				t.Fatal(err, len(got))
			}
		})...)
		joinOnly = append(joinOnly, timed(1, func() {
			before := KeyNullPasses()
			if got, err := BasePositionsOf(ctx, db, base, []string{"id"}, keys); err != nil || len(got) != len(keys) || KeyNullPasses() != before {
				t.Fatal(err, len(got), "the second call ran the pass for a NULL")
			}
		})...)
	}
	report("BasePositionsOf, 1,000 scattered keys, both passes (before this change, every call)", both)
	report("BasePositionsOf, 1,000 scattered keys, the join alone (a file remembered)", joinOnly)

	// The key index beside the file: its build, its size, and the lookup
	// through it, whole and in its two parts (the blocks read, and the rows
	// found read back from the file to check them).
	freshNoNullMemo(t)
	res, err := BuildKeyIndex(ctx, base, []string{"id"}, duckdbutilTuning1x512())
	if err != nil || res.Skipped != "" {
		t.Fatal(err, res.Skipped)
	}
	t.Logf("key index: %d records, %d MB, built in %s", res.Records, res.Bytes>>20, res.Build.Round(time.Millisecond))
	indexed := timed(3, func() {
		before := KeyIndexLookups()
		if got, err := BasePositionsOf(ctx, db, base, []string{"id"}, keys); err != nil || len(got) != len(keys) || KeyIndexLookups() != before+1 {
			t.Fatal(err, len(got), "not answered from the key index")
		}
	})
	report("BasePositionsOf, 1,000 scattered keys, through the key index", indexed)
	var found map[string]int64
	sortedKeys := slices.Clone(keys)
	slices.Sort(sortedKeys)
	report("the key index alone: open and read the blocks", timed(3, func() {
		ix, err := openKeyIndex(base)
		if err != nil {
			t.Fatal(err)
		}
		found, err = ix.find(sortedKeys)
		t.Logf("blocks read: %d of %d", ix.blocksRead, len(ix.blocks))
		ix.Close()
		if err != nil || len(found) != len(keys) {
			t.Fatal(err, len(found))
		}
	}))
	// BaseValuesOf of one column for the same keys: through the index, one
	// read of the rows (the check and the values); without it, the join and
	// then the read of the values.
	report("BaseValuesOf, 1,000 scattered keys, one column, through the key index (one read of the rows)", timed(3, func() {
		before := rowReads.Load()
		if got, err := BaseValuesOf(ctx, db, base, []string{"id"}, keys, []string{"note"}, 0); err != nil || len(got) != len(keys) || rowReads.Load() != before+1 {
			t.Fatal(err, len(got), "not one read of the rows")
		}
	}))
	off := keyIndexOn
	keyIndexOn = func() bool { return false }
	report("BaseValuesOf, 1,000 scattered keys, one column, without the key index (the join, then the values)", timed(3, func() {
		if got, err := BaseValuesOf(ctx, db, base, []string{"id"}, keys, []string{"note"}, 0); err != nil || len(got) != len(keys) {
			t.Fatal(err, len(got))
		}
	}))
	keyIndexOn = off
	report("the check of the rows found against the file", timed(3, func() {
		if got, err := baseValuesAt(ctx, db, base, found, []string{"id"}, -1, nil); err != nil || len(got) != len(keys) {
			t.Fatal(err, len(got))
		}
	}))

	for _, c := range []struct {
		name string
		rows []int64
	}{{"1,000 scattered row numbers", scattered}, {"1,000 consecutive row numbers (control)", consecutive}} {
		nums := make([]string, len(c.rows))
		for i, p := range c.rows {
			nums[i] = fmt.Sprint(p)
		}
		wanted, cleanup, err := writeWantedKeys(nums)
		if err != nil {
			t.Fatal(err)
		}
		// baseValuesAt's own query, for one value column.
		q := "SELECT bintrail_b.file_row_number, encode(bintrail_b.\"note\")" +
			" FROM " + src + " AS bintrail_b" +
			" JOIN (SELECT CAST(" + KeyColumn + " AS BIGINT) AS bintrail_p FROM read_parquet(" + file(wanted) + noHive + ")) AS bintrail_w" +
			" ON bintrail_b.file_row_number = bintrail_w.bintrail_p"
		report("the join on file_row_number, "+c.name, timed(3, func() {
			rs, err := db.QueryContext(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			got := 0
			for rs.Next() {
				got++
			}
			rs.Close()
			if got != len(c.rows) {
				t.Fatalf("%d rows read back, want %d", got, len(c.rows))
			}
		}))
		positions := make(map[string]int64, len(c.rows))
		for i, p := range c.rows {
			positions[fmt.Sprint(i)] = p
		}
		report("baseValuesAt itself, "+c.name, timed(3, func() {
			if got, err := baseValuesAt(ctx, db, base, positions, []string{"note"}, -1, nil); err != nil || len(got) != len(c.rows) {
				t.Fatal(err, len(got))
			}
		}))
		rs, err := db.QueryContext(ctx, "EXPLAIN ANALYZE "+q)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rs.Next() {
			var k, v string
			if err := rs.Scan(&k, &v); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(v)
		}
		rs.Close()
		t.Logf("EXPLAIN ANALYZE, %s:\n%s", c.name, plan.String())
		cleanup()
	}
}

// duckdbutilTuning1x512 is the probe's budget for the key index's build: the
// same one thread and 512 MiB as its session.
func duckdbutilTuning1x512() duckdbutil.Tuning {
	return duckdbutil.Tuning{Threads: 1, MemoryLimit: "512MiB"}
}
