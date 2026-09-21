package reconstruct

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #1570: every file a fold writes inherits when the source was last really
// read, and counts one more fold than the state it folded. These go through
// the real writers (publishWithTableDelta, the compaction, the plain rewrite).

// writeDumpLikeBaseline is writeStampedBaseline with the footer keys a dump
// writes since #1570 (keys=true), or only the producer a dump wrote before
// them (keys=false).
func writeDumpLikeBaseline(t *testing.T, root string, at time.Time, keys bool) string {
	t.Helper()
	rows, nulls := zooRows()
	path := filepath.Join(root, SnapshotDirName(at), "mydb", "orders.parquet")
	md := map[string]string{
		baseline.MetaKeyCreateTableSQL:    zooCreateTableSQL,
		baseline.MetaKeyBinlogFile:        "binlog.000007",
		baseline.MetaKeyBinlogPos:         "4",
		baseline.MetaKeySnapshotTimestamp: at.UTC().Format(time.RFC3339),
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
	}
	if keys {
		baseline.DumpSourceRead(at).Stamp(md)
	}
	w, err := baseline.NewWriter(path, zooColumns(t), baseline.WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rows {
		if err := w.WriteRow(r, nulls[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// tableSourceRead reads a table the way the Backups page does: its file's
// footer and its chain's newest pair.
func tableSourceRead(t *testing.T, base string) baseline.SourceRead {
	t.Helper()
	bm, err := baseline.ReadParquetMetadata(base)
	if err != nil {
		t.Fatal(err)
	}
	chain, err := baseline.ListTableDelta(t.Context(), base)
	if err != nil {
		t.Fatal(err)
	}
	if chain == nil {
		return baseline.ChainSourceRead(bm, nil)
	}
	lm, err := baseline.ReadParquetMetadata(chain.Last().Upserts)
	if err != nil {
		t.Fatal(err)
	}
	return baseline.ChainSourceRead(bm, &lm)
}

func wantSourceRead(t *testing.T, step string, got baseline.SourceRead, at time.Time, folds int) {
	t.Helper()
	if !got.At.Equal(at) || got.Folds != folds {
		t.Fatalf("%s: source read = {%s, %d folds}, want {%s, %d folds}", step, got.At.Format(time.RFC3339), got.Folds, at.Format(time.RFC3339), folds)
	}
}

func TestSourceRead_throughATableDeltaChain(t *testing.T) {
	noCompaction(t)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 3, 0, 0, 0, time.UTC)
	dump := writeDumpLikeBaseline(t, root, t0, true)
	// A full backup taken with deltas on starts the chain with an empty pair,
	// which carries no keys of its own.
	if err := baseline.WriteEmptyTableDeltas(filepath.Join(root, SnapshotDirName(t0))); err != nil {
		t.Fatal(err)
	}
	wantSourceRead(t, "the dump", tableSourceRead(t, dump), t0, 0)

	pos := func(p uint64) *query.BinlogPos { return &query.BinlogPos{File: "binlog.000007", Pos: p} }
	at1 := t0.Add(time.Hour)
	b1, rep, err := deltaWindow(t, root, dump, t0, changeMap(upd(1, "v1")), at1, pos(100), nil)
	if err != nil || !rep.DeltaPairWritten {
		t.Fatalf("window 1: %v (pair written %v)", err, rep != nil && rep.DeltaPairWritten)
	}
	wantSourceRead(t, "one pair over the dump", tableSourceRead(t, b1), t0, 1)

	// An empty window writes no pair: nothing was folded, nothing counted.
	at2 := t0.Add(2 * time.Hour)
	b2, rep, err := deltaWindow(t, root, b1, at1, changeMap(), at2, pos(200), nil)
	if err != nil || rep.DeltaPairWritten {
		t.Fatalf("window 2: %v (pair written %v, want none)", err, rep != nil && rep.DeltaPairWritten)
	}
	wantSourceRead(t, "an empty window", tableSourceRead(t, b2), t0, 1)

	at3 := t0.Add(3 * time.Hour)
	b3, _, err := deltaWindow(t, root, b2, at2, changeMap(upd(2, "v2")), at3, pos(300), nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSourceRead(t, "a second pair", tableSourceRead(t, b3), t0, 2)

	// A compaction folds the chain into a new base: one fold past the chain,
	// and the empty pair it starts the next chain with adds none.
	at4 := t0.Add(4 * time.Hour)
	b4, rep, err := deltaWindow(t, root, b3, at3, changeMap(upd(3, "v3")), at4, pos(400), func(p *tableDeltaPublish) {
		p.fold.Spill = spillOf(t, 1, changeMap(upd(3, "v3")))
		p.fold.Changes = nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.TableDelta || rep.DeltaCompacted == "" {
		t.Fatalf("window 4 was not compacted (TableDelta %v, reason %q)", rep.TableDelta, rep.DeltaCompacted)
	}
	wantSourceRead(t, "the compaction", tableSourceRead(t, b4), t0, 3)
	bm, err := baseline.ReadParquetMetadata(b4)
	if err != nil {
		t.Fatal(err)
	}
	if got := baseline.SourceReadOf(bm); !got.At.Equal(t0) || got.Folds != 3 {
		t.Fatalf("the rewritten base's own footer = %+v, want the dump's instant and 3 folds", got)
	}

	at5 := t0.Add(5 * time.Hour)
	b5, _, err := deltaWindow(t, root, b4, at4, changeMap(upd(4, "v4")), at5, pos(500), nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSourceRead(t, "a pair over the compacted base", tableSourceRead(t, b5), t0, 4)
}

// TestSourceRead_overLegacyFiles: a dump written before #1570 has no keys,
// and the first fold over it still dates the chain from the dump's own
// instant. A fold written before #1570 cannot be dated, and nothing after it
// pretends it can.
func TestSourceRead_overLegacyFiles(t *testing.T) {
	noCompaction(t)
	t0 := time.Date(2026, 5, 1, 3, 0, 0, 0, time.UTC)
	at1 := t0.Add(time.Hour)
	pos := &query.BinlogPos{File: "binlog.000007", Pos: 100}

	root := t.TempDir()
	legacyDump := writeDumpLikeBaseline(t, root, t0, false)
	b, _, err := deltaWindow(t, root, legacyDump, t0, changeMap(upd(1, "v1")), at1, pos, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSourceRead(t, "a fold over a pre-#1570 dump", tableSourceRead(t, b), t0, 1)
	_, ups := baseline.TableDeltaPaths(b, 0)
	um, err := baseline.ReadParquetMetadata(ups)
	if err != nil {
		t.Fatal(err)
	}
	if !um.LastDumpAt.Equal(t0) || um.FoldGeneration != 1 {
		t.Fatalf("the pair's footer = {%s, %d}, want the dump's instant stamped with 1 fold", um.LastDumpAt, um.FoldGeneration)
	}

	// writeStampedBaseline carries no producer: an old fold's footer shape.
	root2 := t.TempDir()
	legacyFold := writeStampedBaseline(t, root2, t0)
	b2, _, err := deltaWindow(t, root2, legacyFold, t0, changeMap(upd(1, "v1")), at1, pos, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := tableSourceRead(t, b2); got.Known() {
		t.Fatalf("a fold over an undatable file = %+v, want not on record", got)
	}
	_, ups2 := baseline.TableDeltaPaths(b2, 0)
	um2, err := baseline.ReadParquetMetadata(ups2)
	if err != nil {
		t.Fatal(err)
	}
	if !um2.LastDumpAt.IsZero() || um2.FoldGeneration != -1 {
		t.Fatalf("a fold over an undatable file stamped {%s, %d}; it must stamp nothing", um2.LastDumpAt, um2.FoldGeneration)
	}
}

// TestSnapshotFileMetadata_stampsTheSourceRead: the one footer builder every
// Parquet-mode file goes through stamps FoldedFrom one fold further, and a
// caller that leaves the field unset stamps nothing rather than a false read.
func TestSnapshotFileMetadata_stampsTheSourceRead(t *testing.T) {
	at := time.Date(2026, 5, 1, 3, 0, 0, 0, time.UTC)
	in := mergeInput{Schema: "s", Table: "t", SnapshotAt: at.Add(time.Hour)}
	md := snapshotFileMetadata(in)
	if _, ok := md[baseline.MetaKeyLastDumpAt]; ok {
		t.Fatal("an unset FoldedFrom stamped a source read")
	}
	if _, ok := md[baseline.MetaKeyFoldGeneration]; ok {
		t.Fatal("an unset FoldedFrom stamped a fold count")
	}
	in.FoldedFrom = baseline.SourceRead{At: at, Folds: 6}
	md = snapshotFileMetadata(in)
	if md[baseline.MetaKeyLastDumpAt] != "2026-05-01T03:00:00Z" || md[baseline.MetaKeyFoldGeneration] != "7" {
		t.Fatalf("stamped %q / %q, want the read's instant and 7 folds", md[baseline.MetaKeyLastDumpAt], md[baseline.MetaKeyFoldGeneration])
	}
}

// TestSourceRead_overAPairWrittenBeforeTheKeys is the shape an upgrade
// leaves: a dump that records its read, and a chain whose newest pair was
// written before #1570 (no count). The next pair keeps the dump's instant and
// must NOT invent a count: stamping "1 fold" there would have the page say
// "updated once" over a chain of any length.
func TestSourceRead_overAPairWrittenBeforeTheKeys(t *testing.T) {
	noCompaction(t)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 3, 0, 0, 0, time.UTC)
	base := writeDumpLikeBaseline(t, root, t0, true)
	fi, err := os.Stat(base)
	if err != nil {
		t.Fatal(err)
	}
	// A pre-#1570 pair: every key readTableDelta checks, none of the new ones.
	legacyAt := t0.Add(30 * time.Minute)
	if err := baseline.WriteTableDeltaPair(base, 0, zooColumns(t), map[string]string{
		baseline.MetaKeySnapshotTimestamp: legacyAt.Format(time.RFC3339),
		baseline.MetaKeySnapshotProducer:  baseline.ProducerReconstruct,
		baseline.MetaKeyCreateTableSQL:    zooCreateTableSQL,
		baseline.MetaKeyBinlogFile:        "binlog.000007",
		baseline.MetaKeyBinlogPos:         "50",
		baseline.MetaKeyDeltaChainStart:   t0.Format(time.RFC3339),
		baseline.MetaKeyDeltaBaseAnchor:   "binlog.000007:4",
		baseline.MetaKeyDeltaBaseSize:     strconv.FormatInt(fi.Size(), 10),
	}, nil, nil); err != nil {
		t.Fatal(err)
	}
	b, rep, err := deltaWindow(t, root, base, t0, changeMap(upd(1, "v1")), t0.Add(time.Hour), &query.BinlogPos{File: "binlog.000007", Pos: 100}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.DeltaPairWritten || rep.DeltaSeq != 1 {
		t.Fatalf("the legacy chain was not extended (pair written %v, seq %d): the fixture does not reach the case", rep.DeltaPairWritten, rep.DeltaSeq)
	}
	_, ups := baseline.TableDeltaPaths(b, 1)
	um, err := baseline.ReadParquetMetadata(ups)
	if err != nil {
		t.Fatal(err)
	}
	if !um.LastDumpAt.Equal(t0) || um.FoldGeneration != -1 {
		t.Fatalf("the pair over a pre-#1570 pair stamped {%s, %d}; want the dump's instant and NO count", um.LastDumpAt, um.FoldGeneration)
	}
	wantSourceRead(t, "the table", tableSourceRead(t, b), t0, -1)
}
