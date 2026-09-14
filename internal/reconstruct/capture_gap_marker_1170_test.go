package reconstruct

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestCaptureGapLines covers the marker a knowingly-gapped snapshot carries.
//
// The inheritance case is the one that matters: a refresh chain folds forward,
// so the events a gapped ancestor never captured are absent from every
// descendant. Dropping the ancestor's line at the first refresh would launder a
// knowingly-incomplete baseline into a clean-looking one — silently, and
// permanently.
func TestCaptureGapLines(t *testing.T) {
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	own := &CaptureGap{
		At:     time.Date(2026, 4, 20, 6, 0, 0, 0, time.UTC),
		Detail: "binlogs purged before the stream caught up",
		Since:  time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		Until:  at,
	}

	t.Run("clean window stamps nothing", func(t *testing.T) {
		if got := captureGapLines(mergeInput{SnapshotAt: at}); got != "" {
			t.Fatalf("got %q, want empty", got)
		}
	})

	t.Run("own gap", func(t *testing.T) {
		got := captureGapLines(mergeInput{SnapshotAt: at, CaptureGap: own})
		if !strings.HasPrefix(got, "2026-05-01T12:00:00Z: ") {
			t.Errorf("line is not stamped with the snapshot instant: %q", got)
		}
		if !strings.Contains(got, "2026-04-20T06:00:00Z") {
			t.Errorf("line does not name when capture was lost: %q", got)
		}
	})

	t.Run("inherited only", func(t *testing.T) {
		in := mergeInput{SnapshotAt: at}
		in.SourceBaseline.Metadata = baseline.DumpMetadata{CaptureGap: "2026-04-01T00:00:00Z: older loss"}
		got := captureGapLines(in)
		if got != "2026-04-01T00:00:00Z: older loss" {
			t.Fatalf("an inherited gap was dropped: %q", got)
		}
	})

	t.Run("inherited plus own, oldest first", func(t *testing.T) {
		in := mergeInput{SnapshotAt: at, CaptureGap: own}
		in.SourceBaseline.Metadata = baseline.DumpMetadata{CaptureGap: "2026-04-01T00:00:00Z: older loss"}
		lines := strings.Split(captureGapLines(in), "\n")
		if len(lines) != 2 {
			t.Fatalf("got %d line(s), want 2: %q", len(lines), lines)
		}
		if !strings.HasPrefix(lines[0], "2026-04-01T00:00:00Z") || !strings.HasPrefix(lines[1], "2026-05-01T12:00:00Z") {
			t.Fatalf("lines are not oldest-first: %q", lines)
		}
	})
}

// TestParquetSnapshot_stampsCaptureGap drives the marker through the real writer
// and reads it back the way a consumer would.
func TestParquetSnapshot_stampsCaptureGap(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	snapDir := t.TempDir()
	rep := &TableReport{Schema: "mydb", Table: "orders"}
	srcMeta, err := baseline.ReadParquetMetadata(src)
	if err != nil {
		t.Fatalf("read source metadata: %v", err)
	}
	in := mergeInput{
		LocalBaselinePath: src,
		CreateTableSQL:    zooCreateTableSQL,
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           map[string]*query.ResultRow{},
		SnapshotDir:       snapDir,
		SnapshotAt:        at,
		CaptureGap: &CaptureGap{
			At: time.Date(2026, 4, 20, 6, 0, 0, 0, time.UTC), Detail: "purged",
			Since: at.Add(-24 * time.Hour), Until: at,
		},
		SourceBaseline: baselineMeta{Path: src, Time: at.Add(-time.Hour), Metadata: srcMeta},
	}
	if err := mergeBaselineIntoParquet(t.Context(), in, rep); err != nil {
		t.Fatalf("mergeBaselineIntoParquet: %v", err)
	}

	meta, err := baseline.ReadParquetMetadata(filepath.Join(snapDir, "mydb", "orders.parquet"))
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if meta.CaptureGap == "" {
		t.Fatal("a snapshot published over a known capture gap carries no marker — the only durable record " +
			"that it is knowingly incomplete would be a log line the operator has since closed")
	}
	if !strings.Contains(meta.CaptureGap, "2026-04-20T06:00:00Z") {
		t.Errorf("marker does not name the loss: %q", meta.CaptureGap)
	}
}

// TestCheckBaselineSchemaCurrent covers the ALTER that no write followed — the
// drift the #602/#843 event-image guards structurally cannot see, because they
// need a delta event to carry (or stop carrying) the column.
func TestCheckBaselineSchemaCurrent(t *testing.T) {
	const createSQL = "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `status` varchar(32) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	col := func(name string, generated bool) metadata.ColumnMeta {
		return metadata.ColumnMeta{Name: name, IsGenerated: generated}
	}

	for _, tc := range []struct {
		name    string
		current []metadata.ColumnMeta
		wantErr string
	}{
		{"unchanged", []metadata.ColumnMeta{col("id", false), col("status", false)}, ""},
		{
			"case differences are not drift",
			[]metadata.ColumnMeta{col("ID", false), col("Status", false)},
			"",
		},
		{
			// mydumper never dumps a generated column's value, so ParseSchema
			// drops it; the snapshot must not report it as an addition.
			"generated column added",
			[]metadata.ColumnMeta{col("id", false), col("status", false), col("total", true)},
			"",
		},
		{
			"column added since the baseline",
			[]metadata.ColumnMeta{col("id", false), col("status", false), col("note", false)},
			"added since: note",
		},
		{
			"column dropped since the baseline",
			[]metadata.ColumnMeta{col("id", false)},
			"gone since: status",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm := &metadata.TableMeta{Schema: "mydb", Table: "orders", Columns: tc.current}
			err := checkBaselineSchemaCurrent(createSQL, tm, tm, "mydb", "orders")
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected refusal: %v", err)
			case tc.wantErr == "":
			case err == nil:
				t.Fatalf("expected a refusal containing %q", tc.wantErr)
			case !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
			// Classification is what `baseline refresh` reports on; a refusal
			// that is not tagged reads as a generic failure in the summary.
			if err != nil && !isSchemaChanged(err) {
				t.Errorf("refusal is not tagged ErrSchemaChanged: %v", err)
			}
		})
	}
}

func isSchemaChanged(err error) bool { return errors.Is(err, ErrSchemaChanged) }
