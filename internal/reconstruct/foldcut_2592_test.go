package reconstruct

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2592: a refresh that cut the index says so, with the cut; one that found
// the index empty carries its source file's position and must not claim it
// as a cut every table was folded through.
func TestSnapshotFileMetadata_stampsTheFoldCutOnlyWhenTheIndexWasCut(t *testing.T) {
	in := mergeInput{Schema: "s", Table: "t", SnapshotAt: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC),
		SourceBaseline: baselineMeta{Metadata: baseline.DumpMetadata{BinlogFile: "binlog.000047", BinlogPos: 500,
			EventMark: `{"id":9,"binlog_file":"binlog.000047","end_pos":400}`}}}
	md := snapshotFileMetadata(in)
	if _, ok := md[baseline.MetaKeyFoldCut]; ok {
		t.Fatal("a run with no cut claimed one")
	}
	if md[baseline.MetaKeyBinlogPos] != "500" {
		t.Fatalf("the source's anchor was not carried: %v", md)
	}
	in.Cut = &query.BinlogPos{File: "binlog.000058", Pos: 1200}
	if got := snapshotFileMetadata(in)[baseline.MetaKeyFoldCut]; got != "binlog.000058:1200" {
		t.Fatalf("fold cut = %q", got)
	}
	// A run told to go on past gaps may have skipped hours of the window:
	// its "nothing found" says nothing through the cut.
	in.GapsAllowed = true
	if _, ok := snapshotFileMetadata(in)[baseline.MetaKeyFoldCut]; ok {
		t.Fatal("a run that allowed gaps claimed a fold cut")
	}
	if snapshotFileMetadata(in)[baseline.MetaKeyBinlogPos] != "1200" {
		t.Fatal("a run that allowed gaps lost its position")
	}
	in.GapsAllowed = false
	in.PG = true
	if _, ok := snapshotFileMetadata(in)[baseline.MetaKeyFoldCut]; ok {
		t.Fatal("a PostgreSQL fold claimed a binlog cut")
	}
}
