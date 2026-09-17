//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedAnchoredBaseline is seedSourceBaseline with the binlog anchor spelled
// out: #1689 is about a cut that has not reached that anchor.
func seedAnchoredBaseline(t *testing.T, root string, at time.Time, schema, file string, pos int) {
	t.Helper()
	snapDir := filepath.Join(root, strings.ReplaceAll(at.UTC().Format(time.RFC3339), ":", "-"))
	path := filepath.Join(snapDir, schema, "orders.parquet")
	cols, err := baseline.ParseSchemaText(ordersCreateSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: ordersCreateSQL,
			baseline.MetaKeyBinlogFile:     file,
			baseline.MetaKeyBinlogPos:      strconv.Itoa(pos),
			"bintrail.snapshot_timestamp":  at.UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, r := range [][]string{{"1", "new"}, {"2", "paid"}, {"3", "shipped"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
}

// TestRefresh_cutBehindTheAnchorSkipsTheFetch is #1689 against a real index:
// when capture has not yet reached the coordinate the previous backup is
// anchored on, the positional window is empty by construction and the fold
// must not query the index for that table at all.
//
// The fetch call is COUNTED rather than inferred, because an empty window and
// a skipped window produce the same report: zero events, the same rows, the
// same anchor. That indistinguishability is what let the cost hide.
func TestRefresh_cutBehindTheAnchorSkipsTheFetch(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	const schema = "shop"
	base := time.Now().UTC().Truncate(time.Hour)
	seedOrdersSnapshot(t, db, schema, base)

	root := t.TempDir()
	// The baseline is anchored well ahead of anything the index holds: the
	// source moved on and capture is still catching up.
	seedAnchoredBaseline(t, root, base, schema, "binlog.000001", 900000)

	// Indexed events, all BELOW the anchor: the index is not empty, it has
	// simply not reached the coordinate the baseline is anchored on. At least
	// one event is REQUIRED — with an empty index ResolveSnapshotCut returns no
	// cut, no UntilPos is set, nothing is provably empty and the fold would run,
	// so the assertion below would fail for a reason unrelated to the skip. The
	// customers row is a second table the run never touches; it says the index
	// carries unrelated traffic, and nothing more. (Exercising the per-table
	// half would take two tables in Tables with different anchors: the cut
	// itself is resolved once per run.)
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		base.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		schema, "customers", 1, "1", nil, nil, []byte(`{"id":1,"name":"n"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400,
		base.Add(2*time.Minute).Format("2006-01-02 15:04:05"), nil,
		schema, "orders", 2, "1", nil, []byte(`{"id":1,"status":"new"}`),
		[]byte(`{"id":1,"status":"paid"}`))

	var folds atomic.Int32
	restore := reconstruct.CountFoldWindowsForTest(&folds)
	defer restore()

	reports, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:  root,
		Tables:       []string{schema + ".orders"},
		At:           base.Add(30 * time.Minute),
		OutputDir:    root,
		OutputFormat: reconstruct.OutputFormatParquet,
	})
	if err != nil {
		t.Fatalf("ReconstructTables: %v", err)
	}
	if len(reports) != 1 {
		t.Fatalf("got %d reports, want 1", len(reports))
	}
	if got := folds.Load(); got != 0 {
		t.Errorf("the fold queried the index %d time(s) for a window that cannot hold an event; "+
			"walking the index to apply nothing is the cost #1689 is about", got)
	}
	if reports[0].EventsApplied != 0 {
		t.Errorf("EventsApplied = %d, want 0", reports[0].EventsApplied)
	}
	// The report must still count the baseline's rows: the skip is an
	// optimisation, not a shortcut past the merge. This is the report field, not
	// the emitted file — the file's contents and footer are pinned by the #1169
	// Parquet-output tests.
	if reports[0].BaselineRows != 3 {
		t.Errorf("BaselineRows = %d, want the baseline's 3 rows passed through", reports[0].BaselineRows)
	}
}

// The mirror: a cut that HAS passed the anchor must still fetch. Without this,
// a skip that fired for every table would pass the test above.
func TestRefresh_cutPastTheAnchorStillFetches(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	const schema = "shop"
	base := time.Now().UTC().Truncate(time.Hour)
	seedOrdersSnapshot(t, db, schema, base)

	root := t.TempDir()
	seedAnchoredBaseline(t, root, base, schema, "binlog.000001", 4)

	testutil.InsertEvent(t, db, "binlog.000001", 300, 400,
		base.Add(2*time.Minute).Format("2006-01-02 15:04:05"), nil,
		schema, "orders", 2, "1", []byte(`["status"]`), []byte(`{"id":1,"status":"new"}`),
		[]byte(`{"id":1,"status":"paid"}`))

	var folds atomic.Int32
	restore := reconstruct.CountFoldWindowsForTest(&folds)
	defer restore()

	reports, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:  root,
		Tables:       []string{schema + ".orders"},
		At:           base.Add(30 * time.Minute),
		OutputDir:    root,
		OutputFormat: reconstruct.OutputFormatParquet,
	})
	if err != nil {
		t.Fatalf("ReconstructTables: %v", err)
	}
	if got := folds.Load(); got != 1 {
		t.Fatalf("the fold queried the index %d time(s), want exactly 1: this window can hold events", got)
	}
	if len(reports) != 1 || reports[0].EventsApplied != 1 {
		t.Errorf("EventsApplied = %d, want the one indexed UPDATE applied", reports[0].EventsApplied)
	}
}

// TestRefresh_emptyWindowStillRefusesACoverageGap is the other half of #1689,
// and the one the optimisation nearly broke: the fetch it skips is also the
// ONLY thing on the full-table path that checks archive coverage. A table whose
// positional window is provably empty must still refuse when an hour inside its
// wall-clock window is in neither a live partition nor an archive.
//
// Why that is not cry-wolf on a window proven empty: the proof is about the
// PREDICATE, and the cut it is handed is the newest event the INDEX can see.
// That equals "where the source got to" only while coverage is complete. With
// an unreadable hour in the window, "no events between the anchor and the cut"
// stops meaning "nothing happened" — and this shape runs unattended, where
// AllowGaps is false precisely because nobody is watching it publish.
//
// BOTH assertions are load-bearing and neither works alone. Drop the fold count
// and the test still passes when the skip never fires, because the fetch raises
// the same *GapError — the refusal would prove nothing about the skip. Drop the
// refusal and it passes on a silent publish.
func TestRefresh_emptyWindowStillRefusesACoverageGap(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	const schema = "shop"
	base := time.Now().UTC().Truncate(time.Hour)
	seedOrdersSnapshot(t, db, schema, base)

	root := t.TempDir()
	// The baseline is three hours old. A fresh index is partitioned from the
	// CURRENT hour forward (indexer.buildPartitionDefs), and nothing has been
	// archived, so the three hours this window opens on are covered by neither
	// tier: real gaps, the shape rotation-without-archiving leaves behind.
	seedAnchoredBaseline(t, root, base.Add(-3*time.Hour), schema, "binlog.000001", 900000)

	// One indexed event, far below the anchor. It exists so the run resolves a
	// cut at all: with no cut there is no upper bound, nothing is provably
	// empty, and the skip under test would never be reached.
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400,
		base.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		schema, "orders", 2, "1", nil, []byte(`{"id":1,"status":"new"}`),
		[]byte(`{"id":1,"status":"paid"}`))

	var folds atomic.Int32
	restore := reconstruct.CountFoldWindowsForTest(&folds)
	defer restore()

	_, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:  root,
		Tables:       []string{schema + ".orders"},
		At:           base.Add(30 * time.Minute),
		OutputDir:    root,
		OutputFormat: reconstruct.OutputFormatParquet,
	})
	var gapErr *query.GapError
	if !errors.As(err, &gapErr) {
		t.Fatalf("got err %v, want a *query.GapError: skipping the fetch must not "+
			"skip the coverage refusal that only the fetch used to run", err)
	}
	if got := folds.Load(); got != 0 {
		t.Errorf("the fold queried the index %d time(s), want 0: the refusal has to come "+
			"from the skip path, or this test proves nothing about the skip", got)
	}
	// errors.As above unwraps, so it passes on the bare error too. The operator
	// reads the STRING — baseline refresh puts it in its per-table summary — and
	// the fetch path adds the anchor and the two fixes (#1193). Without this the
	// guidance would silently depend on whether capture was behind that minute.
	if !strings.Contains(err.Error(), "archive reconcile --repair") {
		t.Errorf("the refusal is missing the remediation the fetch path adds: %v", err)
	}
}
