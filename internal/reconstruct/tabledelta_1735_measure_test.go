package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestMeasure1735_refreshWindows is the #1735 measurement, not a guard: a
// chain of refresh windows three hours apart over one large table, run once
// as the command line runs it (the refresh rewrites the table when the chain
// is a day old) and once as the console's job runs it (the job folds the
// chain after the refresh, and the next refresh puts the file in place). It
// prints how long each refresh took, and the job's own time.
//
//	BINTRAIL_MEASURE_1735=1000000 go test -run TestMeasure1735 -v ./internal/reconstruct/
//
// The value is the table's row count; unset, the test is skipped.
func TestMeasure1735_refreshWindows(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("BINTRAIL_MEASURE_1735"))
	if n <= 0 {
		t.Skip("set BINTRAIL_MEASURE_1735=<rows> to run the measurement")
	}
	const windows = 14
	const perWindow = 5000
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	for _, job := range []bool{false, true} {
		root := t.TempDir()
		base := writeMeasureBaseline(t, root, t0, n)
		fi, _ := os.Stat(base)
		t.Logf("job=%v: table of %d rows, %d MB", job, n, fi.Size()>>20)
		compactDir := filepath.Join(root, ".compact")
		prevTime, next := t0, n+1
		for w := 1; w <= windows; w++ {
			at := t0.Add(time.Duration(w) * 3 * time.Hour)
			changes := map[string]*query.ResultRow{}
			stride := n / perWindow
			for i := range perWindow - 200 {
				id := 1 + ((i*stride + w*7) % n)
				changes[pkStrForInt(id)] = upd(id, "w"+strconv.Itoa(w))
			}
			for range 100 {
				changes[pkStrForInt(next)] = ins(next, "new")
				next++
			}
			for i := range 100 {
				id := 3 + ((i*stride*3 + w*11) % n)
				changes[pkStrForInt(id)] = del(id)
			}
			cut := &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * w)}
			started := time.Now()
			nb, rep, err := deltaWindow(t, root, base, prevTime, changes, at, cut, func(p *tableDeltaPublish) {
				p.cfg.CompactionJob = job
				if job {
					p.cfg.CompactDir = compactDir
				}
			})
			if err != nil {
				t.Fatalf("window %d: %v", w, err)
			}
			took := time.Since(started)
			what := "pair " + strconv.Itoa(rep.DeltaSeq)
			switch {
			case rep.DeltaCompacted != "":
				what = "REWRITTEN IN FULL: " + rep.DeltaCompacted
			case rep.DeltaChainFolded != "":
				what += ", folded file adopted (pairs " + rep.DeltaChainFolded + ")"
			}
			t.Logf("  window %2d (chain %2dh): refresh %6d ms  %s", w, int(at.Sub(chainStartOf(t, nb)).Hours()), took.Milliseconds(), what)
			prevTime, base = at, nb
			if !job {
				continue
			}
			// The job, after the refresh, as maybeCompact runs it.
			bm, _ := baseline.ReadParquetMetadata(base)
			d, _ := readTableDelta(context.Background(), base, bm)
			if d == nil {
				continue
			}
			bi, _ := os.Stat(base)
			if MajorCompactionReason(d.Meta, at, d.PairSize, bi.Size(), time.Time{}, time.Hour) == "" {
				continue
			}
			dir := CompactionDir(compactDir, "mydb", "orders", d.Meta.DeltaChainStart)
			started = time.Now()
			if _, err := CompactTableDeltaMajor(context.Background(), base, dir, nil, duckdbutil.Tuning{}); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, baseline.SuccessMarker), nil, 0o644); err != nil {
				t.Fatal(err)
			}
			t.Logf("             job: fold %6d ms (outside the refresh, same slot)", time.Since(started).Milliseconds())
		}
	}
}

func chainStartOf(t *testing.T, base string) time.Time {
	t.Helper()
	bm, _ := baseline.ReadParquetMetadata(base)
	d, _ := readTableDelta(context.Background(), base, bm)
	if d == nil {
		return bm.SnapshotTimestamp
	}
	return d.Meta.DeltaChainStart
}

func writeMeasureBaseline(t *testing.T, root string, at time.Time, n int) string {
	t.Helper()
	path := filepath.Join(root, SnapshotDirName(at), "mydb", "orders.parquet")
	w, err := baseline.NewWriter(path, zooColumns(t), baseline.WriterConfig{
		Compression: ParquetWriterCompression, RowGroupSize: ParquetWriterRowGroupSize,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL:    zooCreateTableSQL,
			baseline.MetaKeyBinlogFile:        "binlog.000007",
			baseline.MetaKeyBinlogPos:         "4",
			baseline.MetaKeySnapshotTimestamp: at.UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	nulls := []bool{false, false, false, false, false, false, true, false}
	for i := 1; i <= n; i++ {
		s := strconv.Itoa(i)
		row := []string{s, s + "0", "name-" + s, "1.50", "2026-03-03 03:03:03", "2026-03-03", "", "0.5"}
		if err := w.WriteRow(row, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
