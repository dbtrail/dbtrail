package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2212: on an S3-only server's update, a table with no events in the window
// is published by copying its previous object(s) inside S3 instead of being
// downloaded, rewritten and uploaded. The decision is the local carry-forward
// one at the same two places (step 5b, and the table-delta publish), behind
// every refusal, with S3 in place of the hard link. Edge cases, written
// before the code:
//   - the copy is off unless the run names where it will be uploaded
//     (S3CopyUnchangedTo), so the CLI, a restore and a local fold never copy;
//   - events in the window, a known capture gap, a mydumper output, a local
//     source: not copied (the same rules as the local carry);
//   - a previous snapshot in ANOTHER bucket: not copied, and said;
//     another prefix of the same bucket: copied;
//   - the source manifest has no digest for a file, or cannot be read: not
//     copied (the table is rewritten, and so gets a digest);
//   - with table deltas: the base AND its pair are copied, the chain start
//     recorded; a chain past the day cap or at the index floor, a chain of
//     more than the one pair an S3 update writes, a legacy pair, no chain at
//     all, a reserved column, no anchor: not copied;
//   - a copy writes NOTHING in the local snapshot directory;
//   - a table new in this window has no previous object: refused before this
//     point ("has no baseline snapshot"), never copied;
//   - a dropped or truncated table is refused by CheckDestructiveDDL before
//     the change map exists, so an empty window cannot stand for it.

const (
	prevS3 = "s3://bkt/srv/2026-10-07T11-00-00Z/"
	destS3 = "s3://bkt/srv/"
)

func stubS3CopySeams(t *testing.T, digests map[string]string, digestErr error) {
	t.Helper()
	prev := s3FileDigest
	t.Cleanup(func() { s3FileDigest = prev })
	s3FileDigest = func(_ context.Context, p string) (string, bool, error) {
		if digestErr != nil {
			return "", false, digestErr
		}
		d, ok := digests[p]
		return d, ok, nil
	}
}

func allDigests() map[string]string {
	return map[string]string{
		prevS3 + "shop/orders.parquet":        "00000001",
		prevS3 + "shop/orders.000000.posdel":  "00000002",
		prevS3 + "shop/orders.000000.upserts": "00000003",
	}
}

func copyCfg(t *testing.T, dest string) FullTableConfig {
	t.Helper()
	cfg := FullTableConfig{
		OutputFormat: OutputFormatParquet, S3CopyUnchangedTo: dest, CarryForwardUnchanged: true,
		At: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC),
	}
	cfg.snapshotDir = filepath.Join(t.TempDir(), SnapshotDirName(cfg.At))
	return cfg
}

func TestS3CopyEligible_2212(t *testing.T) {
	src := prevS3 + "shop/orders.parquet"
	cases := []struct {
		name       string
		dest, src  string
		format     string
		changes    int
		gap        *CaptureGap
		ok, saysIt bool
	}{
		{"same bucket and prefix", destS3, src, OutputFormatParquet, 0, nil, true, false},
		{"same bucket, another prefix", "s3://bkt/elsewhere/", src, OutputFormatParquet, 0, nil, true, false},
		{"off: no destination named", "", src, OutputFormatParquet, 0, nil, false, false},
		{"events in the window", destS3, src, OutputFormatParquet, 3, nil, false, false},
		{"a known capture gap", destS3, src, OutputFormatParquet, 0, &CaptureGap{}, false, false},
		{"mydumper output", destS3, src, OutputFormatMydumper, 0, nil, false, false},
		{"a local source", destS3, "/b/2026-10-07T11-00-00Z/shop/orders.parquet", OutputFormatParquet, 0, nil, false, false},
		{"another bucket", "s3://other/srv/", src, OutputFormatParquet, 0, nil, false, true},
		{"a destination that does not parse", "s3://", src, OutputFormatParquet, 0, nil, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, why := s3CopyEligible(tc.dest, tc.format, tc.src, tc.changes, tc.gap)
			if ok != tc.ok || (why != "") != tc.saysIt {
				t.Fatalf("(%v, %q), want ok=%v saysWhy=%v", ok, why, tc.ok, tc.saysIt)
			}
		})
	}
}

func TestS3CopyUnchanged_copiesTheBaseAndWritesNothingLocally(t *testing.T) {
	stubS3CopySeams(t, allDigests(), nil)
	cfg := copyCfg(t, destS3)
	rep := &TableReport{Schema: "shop", Table: "orders"}
	if !s3CopyUnchanged(context.Background(), cfg, "shop", "orders", prevS3+"shop/orders.parquet", 0, nil, rep) {
		t.Fatal("an unchanged table was not copied")
	}
	want := []baseline.RemoteCopy{{Rel: "shop/orders.parquet", Src: prevS3 + "shop/orders.parquet", CRC32C: "00000001"}}
	if len(rep.S3Copies) != 1 || rep.S3Copies[0] != want[0] {
		t.Fatalf("copies = %+v, want %+v", rep.S3Copies, want)
	}
	if !rep.CarriedForward || rep.CarriedByLink {
		t.Fatalf("carried=%v linked=%v: a copy in S3 is a reuse that saves no disk", rep.CarriedForward, rep.CarriedByLink)
	}
	if len(rep.Files) != 1 || rep.Files[0] != filepath.Join("shop", "orders.parquet") {
		t.Fatalf("Files = %v", rep.Files)
	}
	if _, err := os.Stat(cfg.snapshotDir); !os.IsNotExist(err) {
		t.Fatalf("the copy wrote into the local snapshot directory (stat: %v)", err)
	}
}

func TestS3CopyUnchanged_refusals(t *testing.T) {
	src := prevS3 + "shop/orders.parquet"
	t.Run("no digest at the source: rewritten instead", func(t *testing.T) {
		stubS3CopySeams(t, map[string]string{}, nil)
		rep := &TableReport{}
		if s3CopyUnchanged(context.Background(), copyCfg(t, destS3), "shop", "orders", src, 0, nil, rep) || rep.S3Copies != nil {
			t.Fatal("copied a file its source manifest does not vouch for")
		}
	})
	t.Run("an empty digest is no digest", func(t *testing.T) {
		stubS3CopySeams(t, map[string]string{src: ""}, nil)
		if s3CopyUnchanged(context.Background(), copyCfg(t, destS3), "shop", "orders", src, 0, nil, &TableReport{}) {
			t.Fatal("copied a file whose carried digest is empty")
		}
	})
	t.Run("the source manifest cannot be read: rewritten instead", func(t *testing.T) {
		stubS3CopySeams(t, nil, errors.New("SlowDown"))
		if s3CopyUnchanged(context.Background(), copyCfg(t, destS3), "shop", "orders", src, 0, nil, &TableReport{}) {
			t.Fatal("copied on an unreadable manifest")
		}
	})
	t.Run("reuse turned off", func(t *testing.T) {
		stubS3CopySeams(t, allDigests(), nil)
		cfg := copyCfg(t, destS3)
		cfg.CarryForwardUnchanged = false
		if s3CopyUnchanged(context.Background(), cfg, "shop", "orders", src, 0, nil, &TableReport{}) {
			t.Fatal("copied with reuse of unchanged tables turned off")
		}
	})
	t.Run("off without a destination", func(t *testing.T) {
		stubS3CopySeams(t, allDigests(), nil)
		if s3CopyUnchanged(context.Background(), copyCfg(t, ""), "shop", "orders", src, 0, nil, &TableReport{}) {
			t.Fatal("copied with no destination named")
		}
	})
}

// chainFixture is the shape an S3 update writes: the base and ONE empty pair
// numbered 0, whose chain started at the previous run.
func stubS3Chain(t *testing.T, files []baseline.TableDeltaFile, legacy bool, start time.Time, listErr error) {
	t.Helper()
	prevList, prevStart := listS3TableDelta, chainStartAt
	t.Cleanup(func() { listS3TableDelta, chainStartAt = prevList, prevStart })
	listS3TableDelta = func(context.Context, string) (*baseline.TableDeltaChain, error) {
		if listErr != nil {
			return nil, listErr
		}
		if files == nil && !legacy {
			return nil, nil
		}
		return &baseline.TableDeltaChain{Files: files, Legacy: legacy,
			LegacyPosdel: prevS3 + "shop/orders.posdel", LegacyUpserts: prevS3 + "shop/orders.upserts"}, nil
	}
	chainStartAt = func(context.Context, string) (time.Time, error) { return start, nil }
}

func onePair() []baseline.TableDeltaFile {
	return []baseline.TableDeltaFile{{Seq: 0, SeqLo: 0,
		Posdel: prevS3 + "shop/orders.000000.posdel", Upserts: prevS3 + "shop/orders.000000.upserts"}}
}

func deltaPublish(t *testing.T) tableDeltaPublish {
	cfg := copyCfg(t, destS3)
	cfg.TableDeltas = true
	return tableDeltaPublish{
		cfg: cfg, schema: "shop", table: "orders", basePath: prevS3 + "shop/orders.parquet",
		fold:       &foldResult{Changes: map[string]*query.ResultRow{}},
		anchorMeta: baseline.DumpMetadata{BinlogFile: "binlog.000001", BinlogPos: 4},
	}
}

func TestS3CopyUnchangedChain_copiesTheBaseAndItsPair(t *testing.T) {
	stubS3CopySeams(t, allDigests(), nil)
	start := time.Date(2026, 10, 7, 11, 0, 0, 0, time.UTC)
	stubS3Chain(t, onePair(), false, start, nil)
	p := deltaPublish(t)
	rep := &TableReport{}
	if !s3CopyUnchangedChain(context.Background(), p, true, "", rep) {
		t.Fatal("an unchanged table with deltas on was not copied")
	}
	got := map[string]string{}
	for _, c := range rep.S3Copies {
		got[c.Rel] = c.Src + "#" + c.CRC32C
	}
	want := map[string]string{
		"shop/orders.parquet":        prevS3 + "shop/orders.parquet#00000001",
		"shop/orders.000000.posdel":  prevS3 + "shop/orders.000000.posdel#00000002",
		"shop/orders.000000.upserts": prevS3 + "shop/orders.000000.upserts#00000003",
	}
	if len(got) != len(want) {
		t.Fatalf("copies = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %q, want %q", k, got[k], v)
		}
	}
	if !rep.S3CopyChainStart.Equal(start) || !rep.TableDelta || rep.DeltaPairWritten || rep.DeltaChainFiles != 1 {
		t.Fatalf("report = %+v", rep)
	}
	if _, err := os.Stat(p.cfg.snapshotDir); !os.IsNotExist(err) {
		t.Fatalf("the copy wrote into the local snapshot directory (stat: %v)", err)
	}
}

func TestS3CopyUnchangedChain_refusals(t *testing.T) {
	fresh := time.Date(2026, 10, 7, 11, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		files    []baseline.TableDeltaFile
		legacy   bool
		start    time.Time
		listErr  error
		mutate   func(*tableDeltaPublish)
		anchor   bool
		reserved string
		digests  map[string]string
	}{
		{name: "no chain beside the base (this run starts one)", files: nil, start: fresh, anchor: true},
		{name: "a legacy pair", legacy: true, start: fresh, anchor: true},
		{name: "a chain of two pairs", files: append(onePair(), baseline.TableDeltaFile{Seq: 1, SeqLo: 1,
			Posdel: prevS3 + "shop/orders.000001.posdel", Upserts: prevS3 + "shop/orders.000001.upserts"}), start: fresh, anchor: true},
		{name: "the chain is over a day old", files: onePair(), start: fresh.Add(-25 * time.Hour), anchor: true},
		{name: "the chain starts at the index floor", files: onePair(), start: fresh, anchor: true,
			mutate: func(p *tableDeltaPublish) { p.cfg.ChainStartFloor = fresh }},
		{name: "the listing fails", files: onePair(), start: fresh, listErr: errors.New("403"), anchor: true},
		{name: "events in the window", files: onePair(), start: fresh, anchor: true,
			mutate: func(p *tableDeltaPublish) { p.fold.Changes["1"] = &query.ResultRow{} }},
		{name: "a known capture gap", files: onePair(), start: fresh, anchor: true,
			mutate: func(p *tableDeltaPublish) { p.capGap = &CaptureGap{} }},
		{name: "no anchor", files: onePair(), start: fresh, anchor: false},
		{name: "a reserved column", files: onePair(), start: fresh, anchor: true, reserved: "file_row_number"},
		{name: "a pair file without a digest", files: onePair(), start: fresh, anchor: true,
			digests: map[string]string{prevS3 + "shop/orders.parquet": "00000001"}},
		{name: "another bucket", files: onePair(), start: fresh, anchor: true,
			mutate: func(p *tableDeltaPublish) { p.cfg.S3CopyUnchangedTo = "s3://other/srv/" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.digests
			if d == nil {
				d = allDigests()
			}
			stubS3CopySeams(t, d, nil)
			stubS3Chain(t, tc.files, tc.legacy, tc.start, tc.listErr)
			p := deltaPublish(t)
			if tc.mutate != nil {
				tc.mutate(&p)
			}
			rep := &TableReport{}
			if s3CopyUnchangedChain(context.Background(), p, tc.anchor, tc.reserved, rep) || rep.S3Copies != nil {
				t.Fatalf("copied: %+v", rep.S3Copies)
			}
		})
	}
}

// The manifest the fold writes lists the copied files with their carried
// digests, and only from reports that copied.
func TestCarriedDigests_2212(t *testing.T) {
	reps := []*TableReport{
		nil,
		{S3Copies: []baseline.RemoteCopy{{Rel: "shop/orders.parquet", CRC32C: "00000001"}, {Rel: "shop/orders.000000.posdel", CRC32C: "00000002"}}},
		{Files: []string{"crm/leads.parquet"}},
	}
	got := carriedDigests(reps)
	if len(got) != 2 || got["shop/orders.parquet"] != "00000001" || got["shop/orders.000000.posdel"] != "00000002" {
		t.Fatalf("carried = %v", got)
	}
}

// The read bound of a snapshot whose tables were copied: the copied chains
// count (they are usually the oldest), and a snapshot whose EVERY table was
// copied has a bound rather than an error.
func TestSnapshotReadsFromWith_countsCopiedTables_2212(t *testing.T) {
	at := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, SnapshotDirName(at)), 0o755); err != nil {
		t.Fatal(err)
	}
	old := at.Add(-3 * time.Hour)
	got, err := SnapshotReadsFromWith(context.Background(), root, at, []time.Time{at.Add(-time.Hour), old})
	if err != nil || !got.Equal(old) {
		t.Fatalf("all-copied snapshot: (%v, %v), want %v", got, err, old)
	}
	if _, err := SnapshotReadsFromWith(context.Background(), root, at, nil); err == nil ||
		!strings.Contains(err.Error(), "no table file") {
		t.Fatalf("an empty snapshot with no copies must still be an error, got %v", err)
	}
}

// The arm is wired where the local carry is: publishWithTableDelta over an
// s3:// base takes it before the compaction that would download the table.
func TestPublishWithTableDelta_copiesAnUnchangedS3Table(t *testing.T) {
	stubS3CopySeams(t, allDigests(), nil)
	stubS3Chain(t, onePair(), false, time.Date(2026, 10, 7, 11, 0, 0, 0, time.UTC), nil)
	rep := &TableReport{}
	if err := publishWithTableDelta(context.Background(), deltaPublish(t), rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.S3Copies) != 3 || rep.DeltaCompacted != "" {
		t.Fatalf("copies=%v compacted=%q, want the base and its pair copied", rep.S3Copies, rep.DeltaCompacted)
	}
}
