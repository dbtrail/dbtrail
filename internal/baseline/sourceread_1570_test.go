package baseline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// #1570: when did these rows last come from a real read of the source?

func TestSourceReadOf(t *testing.T) {
	dumpAt := ts("2026-06-03T03:00:00Z")
	foldAt := ts("2026-06-10T03:00:00Z")
	cases := []struct {
		name string
		md   DumpMetadata
		want SourceRead
	}{
		{
			name: "a file's own keys win",
			md: DumpMetadata{Producer: ProducerReconstruct, SnapshotTimestamp: foldAt,
				LastDumpAt: dumpAt, FoldGeneration: 4},
			want: SourceRead{At: dumpAt, Folds: 4},
		},
		{
			// A writer that knew the instant but not the count stamps the
			// instant alone; the count is then NOT zero, which would claim
			// this file was the read.
			name: "an instant with no count keeps the count unknown",
			md: DumpMetadata{Producer: ProducerReconstruct, SnapshotTimestamp: foldAt,
				LastDumpAt: dumpAt, FoldGeneration: -1},
			want: SourceRead{At: dumpAt, Folds: -1},
		},
		{
			name: "a dump written before the keys is its own read",
			md:   DumpMetadata{Producer: ProducerDump, SnapshotTimestamp: dumpAt, FoldGeneration: -1},
			want: SourceRead{At: dumpAt, Folds: 0},
		},
		{
			name: "a mysql dump older than the producer key is dated by its mydumper format",
			md:   DumpMetadata{SnapshotTimestamp: dumpAt, MydumperFormat: "csv", FoldGeneration: -1},
			want: SourceRead{At: dumpAt, Folds: 0},
		},
		{
			name: "a postgres dump older than the producer key is dated by its WAL floor",
			md:   DumpMetadata{SnapshotTimestamp: dumpAt, LSN: 42, FoldGeneration: -1},
			want: SourceRead{At: dumpAt, Folds: 0},
		},
		{
			// Never the directory's time: a dump with no writer instant could
			// be a carried file under a much newer directory, and dating it by
			// that directory would make it look fresher than it is.
			name: "a dump with no writer instant is not dated at all",
			md:   DumpMetadata{Producer: ProducerDump, FoldGeneration: -1},
			want: unknownSourceRead,
		},
		{
			name: "a fold written before the keys cannot be dated without its ancestors",
			md: DumpMetadata{Producer: ProducerReconstruct, SnapshotTimestamp: foldAt,
				DerivedFrom: dumpAt, FoldGeneration: -1},
			want: unknownSourceRead,
		},
		{
			name: "a producer a newer build wrote is not a dump, whatever else it carries",
			md: DumpMetadata{Producer: "snapshotter-v9", SnapshotTimestamp: foldAt,
				MydumperFormat: "csv", FoldGeneration: -1},
			want: unknownSourceRead,
		},
		{
			name: "no footer at all",
			md:   DumpMetadata{FoldGeneration: -1},
			want: unknownSourceRead,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := SourceReadOf(c.md); !got.At.Equal(c.want.At) || got.Folds != c.want.Folds {
				t.Fatalf("SourceReadOf = %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestChainSourceRead(t *testing.T) {
	dumpAt := ts("2026-06-03T03:00:00Z")
	base := DumpMetadata{Producer: ProducerDump, SnapshotTimestamp: dumpAt,
		LastDumpAt: dumpAt, FoldGeneration: 0}
	pair := func(at string, keys bool, folds int) *DumpMetadata {
		m := DumpMetadata{Producer: ProducerReconstruct, SnapshotTimestamp: ts(at), FoldGeneration: -1}
		if keys {
			m.LastDumpAt, m.FoldGeneration = dumpAt, folds
		}
		return &m
	}
	cases := []struct {
		name string
		base DumpMetadata
		last *DumpMetadata
		want SourceRead
	}{
		{name: "no chain: the table file answers", base: base, want: SourceRead{At: dumpAt, Folds: 0}},
		{
			// The empty pair a full backup starts the chain with carries no
			// keys and no producer (writeEmptyTableDelta): it is the dump's.
			name: "the empty pair written with the base adds no fold",
			base: base,
			last: &DumpMetadata{SnapshotTimestamp: dumpAt, FoldGeneration: -1},
			want: SourceRead{At: dumpAt, Folds: 0},
		},
		{
			name: "the newest pair counts the folds",
			base: base, last: pair("2026-06-03T09:00:00Z", true, 3),
			want: SourceRead{At: dumpAt, Folds: 3},
		},
		{
			// A pair from before the keys: the instant is still the base's
			// (a pair never reads the source), the count is not on record.
			name: "a pair written before the keys keeps the base's instant",
			base: base, last: pair("2026-06-03T09:00:00Z", false, 0),
			want: SourceRead{At: dumpAt, Folds: -1},
		},
		{
			// The base rules the instant: a pair cannot know more than the
			// state it folded, and trusting it here would let a damaged or
			// hand-copied pair make the table look fresher.
			name: "a base that cannot be dated is not dated by its pairs",
			base: DumpMetadata{Producer: ProducerReconstruct, SnapshotTimestamp: dumpAt, FoldGeneration: -1},
			last: pair("2026-06-03T09:00:00Z", true, 3),
			want: unknownSourceRead,
		},
		{
			name: "a pair that claims a different read than its base does not move the instant",
			base: base,
			last: &DumpMetadata{Producer: ProducerReconstruct, SnapshotTimestamp: ts("2026-06-03T09:00:00Z"),
				LastDumpAt: ts("2026-06-03T08:00:00Z"), FoldGeneration: 1},
			want: SourceRead{At: dumpAt, Folds: 1},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ChainSourceRead(c.base, c.last); !got.At.Equal(c.want.At) || got.Folds != c.want.Folds {
				t.Fatalf("ChainSourceRead = %+v, want %+v", got, c.want)
			}
		})
	}
}

// TestSourceReadNext_neverFresher: a fold copies the read it folded from and
// adds one; it never moves the instant, and unknown stays unknown.
func TestSourceReadNext_neverFresher(t *testing.T) {
	at := ts("2026-06-03T03:00:00Z")
	if got := (SourceRead{At: at, Folds: 0}).Next(); !got.At.Equal(at) || got.Folds != 1 {
		t.Fatalf("Next of a dump = %+v, want the dump's instant and one fold", got)
	}
	if got := (SourceRead{At: at, Folds: -1}).Next(); !got.At.Equal(at) || got.Folds != -1 {
		t.Fatalf("Next of an uncounted read = %+v, want the instant kept and the count still unknown", got)
	}
	if got := unknownSourceRead.Next(); got.Known() || got.Folds != -1 {
		t.Fatalf("Next of unknown = %+v, want unknown", got)
	}
	// The zero value is what a caller that forgot to set the field passes:
	// it must stamp nothing, not "read at the epoch".
	if got := (SourceRead{}).Next(); got.Known() {
		t.Fatalf("Next of the zero value = %+v, want unknown", got)
	}
}

func TestSourceReadStamp(t *testing.T) {
	at := ts("2026-06-03T03:00:00Z")
	cases := []struct {
		name string
		r    SourceRead
		want map[string]string
	}{
		{"known", SourceRead{At: at, Folds: 2}, map[string]string{
			MetaKeyLastDumpAt: "2026-06-03T03:00:00Z", MetaKeyFoldGeneration: "2"}},
		{"a dump", DumpSourceRead(at), map[string]string{
			MetaKeyLastDumpAt: "2026-06-03T03:00:00Z", MetaKeyFoldGeneration: "0"}},
		{"instant only", SourceRead{At: at, Folds: -1}, map[string]string{
			MetaKeyLastDumpAt: "2026-06-03T03:00:00Z"}},
		{"unknown", unknownSourceRead, map[string]string{}},
		{"zero value", SourceRead{}, map[string]string{}},
	}
	for _, c := range cases {
		md := map[string]string{}
		c.r.Stamp(md)
		if !reflect.DeepEqual(md, c.want) {
			t.Errorf("%s: Stamp wrote %v, want %v", c.name, md, c.want)
		}
	}
	// Written in UTC whatever zone the instant arrives in: the reader and
	// every other footer instant are UTC.
	md := map[string]string{}
	SourceRead{At: at.In(time.FixedZone("ART", -3*3600)), Folds: 0}.Stamp(md)
	if md[MetaKeyLastDumpAt] != "2026-06-03T03:00:00Z" {
		t.Errorf("a zoned instant was stamped as %q", md[MetaKeyLastDumpAt])
	}
}

func TestParseFoldGeneration(t *testing.T) {
	for raw, want := range map[string]int{
		"0": 0, "12": 12, "-1": -1, "-3": -1, "x": -1, "": -1, " 3": -1, "1.5": -1,
	} {
		if got := parseFoldGeneration("x.parquet", raw); got != want {
			t.Errorf("parseFoldGeneration(%q) = %d, want %d", raw, got, want)
		}
	}
}

// TestFooterReaders_agreeOnEveryKey: the local reader looks keys up by name
// and the S3 reader walks them as rows through a separate switch, so a key
// taught to one and not the other makes an S3-hosted backup quietly lose it
// (#1570 found MetaKeyCreateTableAsOf missing from the S3 side since #1651).
// Every MetaKey constant of this package is written into a real Parquet file,
// read back locally, applied through the S3 switch, and the two results must
// be identical. The key list is read from the package source, so a NEW key
// fails here until it has a value below, and then until both readers know it.
func TestFooterReaders_agreeOnEveryKey(t *testing.T) {
	values := map[string]string{
		"MetaKeyBinlogFile":        "binlog.000009",
		"MetaKeyBinlogPos":         "3000",
		"MetaKeyGTIDSet":           "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		"MetaKeyCreateTableSQL":    "CREATE TABLE `t` (`id` int NOT NULL, PRIMARY KEY (`id`))",
		"MetaKeyLSN":               "42",
		"MetaKeyRowCount":          "7",
		"MetaKeyContentDigest":     "v1:abc",
		"MetaKeyRenderGUCs":        RenderGUCsPinned,
		"MetaKeyCaptureGap":        "2026-06-10T03:00:00Z: a gap",
		"MetaKeyLastEventID":       "4500",
		"MetaKeySnapshotProducer":  ProducerReconstruct,
		"MetaKeyDerivedFrom":       "2026-06-03T03:00:00Z",
		"MetaKeyDerivedFromPath":   "/b/2026-06-03T03-00-00Z/shop/t.parquet",
		"MetaKeySnapshotTimestamp": "2026-06-10T03:00:00Z",
		"MetaKeyCreateTableAsOf":   "2026-06-01T03:00:00Z",
		"MetaKeyMydumperFormat":    "csv",
		"MetaKeyDeltaChainStart":   "2026-06-09T03:00:00Z",
		"MetaKeyDeltaBaseAnchor":   "binlog.000009:3000",
		"MetaKeyDeltaBaseSize":     "1024",
		"MetaKeyDeltaSeq":          "3",
		"MetaKeyDeltaSeqLo":        "1",
		"MetaKeyLastDumpAt":        "2026-06-01T03:00:00Z",
		"MetaKeyFoldGeneration":    "4",
	}
	keys := footerKeyConstants(t)
	if len(keys) < len(values) {
		t.Fatalf("found %d MetaKey constants in the package source, fewer than the %d this test has values for: the source scan is broken", len(keys), len(values))
	}
	md := map[string]string{}
	for name, key := range keys {
		v, ok := values[name]
		if !ok {
			t.Errorf("footer key %s (%q) has no value in this test: add one, and teach BOTH ReadParquetMetadata and applyS3FooterKV to read it", name, key)
			continue
		}
		md[key] = v
	}
	if t.Failed() {
		return
	}
	path := filepath.Join(t.TempDir(), "t.parquet")
	w, err := NewWriter(path, []Column{{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)}},
		WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	local, err := ReadParquetMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	s3 := DumpMetadata{DeltaSeq: -1, DeltaSeqLo: -1, FoldGeneration: -1}
	for k, v := range md {
		if applyS3FooterKV(&s3, "s3://b/t.parquet", k, v) {
			t.Fatalf("%s=%q reported a corrupt row count", k, v)
		}
	}
	if !reflect.DeepEqual(local, s3) {
		lv, sv := reflect.ValueOf(local), reflect.ValueOf(s3)
		for i := 0; i < lv.NumField(); i++ {
			if !reflect.DeepEqual(lv.Field(i).Interface(), sv.Field(i).Interface()) {
				t.Errorf("field %s: local reader %v, S3 reader %v", lv.Type().Field(i).Name, lv.Field(i).Interface(), sv.Field(i).Interface())
			}
		}
	}
	// And the new keys really were read, not merely equally dropped.
	if got := SourceReadOf(local); !got.At.Equal(ts("2026-06-01T03:00:00Z")) || got.Folds != 4 {
		t.Fatalf("SourceReadOf(read back) = %+v", got)
	}
}

// footerKeyConstants returns every package-level constant named MetaKey* whose
// value is a "bintrail." string literal, by name, from the non-test sources.
func footerKeyConstants(t *testing.T) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	out := map[string]string{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, n := range vs.Names {
					if !strings.HasPrefix(n.Name, "MetaKey") || i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					v, err := strconv.Unquote(lit.Value)
					if err == nil && strings.HasPrefix(v, "bintrail.") {
						out[n.Name] = v
					}
				}
			}
		}
	}
	return out
}

// TestRun_stampsItselfAsTheRead: a dump written by `bintrail baseline` carries
// both keys explicitly, not only the producer the reader could derive them
// from, so a reader of the file alone (a DuckDB user reading its footer)
// needs no rule to know the answer.
func TestRun_stampsItselfAsTheRead(t *testing.T) {
	inputDir, outputDir := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	const sqlData = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2025, 3, 1, 4, 5, 6, 0, time.UTC)
	if _, err := Run(t.Context(), Config{InputDir: inputDir, OutputDir: outputDir, Timestamp: at, Compression: "none", RowGroupSize: 100}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	path := filepath.Join(outputDir, "2025-03-01T04-05-06Z", "shop", "orders.parquet")
	m, err := ReadParquetMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	if !m.LastDumpAt.Equal(at) || m.FoldGeneration != 0 {
		t.Fatalf("a dump's own footer = {%s, %d}, want {%s, 0} stamped explicitly", m.LastDumpAt, m.FoldGeneration, at)
	}
}

// TestFooterReaders_aReadWithNoCountIsNotZero: a file that records its read
// and no count (a pair over a chain from before #1570) reads as "count not on
// record" through both readers, from the one starting value they share. A
// reader starting at the zero value would say this file IS the read.
func TestFooterReaders_aReadWithNoCountIsNotZero(t *testing.T) {
	md := map[string]string{MetaKeyLastDumpAt: "2026-06-01T03:00:00Z", MetaKeySnapshotProducer: ProducerReconstruct}
	path := filepath.Join(t.TempDir(), "t.parquet")
	w, err := NewWriter(path, []Column{{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)}},
		WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	local, err := ReadParquetMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	s3 := emptyFooterMetadata()
	for k, v := range md {
		applyS3FooterKV(&s3, "s3://b/t.parquet", k, v)
	}
	for name, m := range map[string]DumpMetadata{"local": local, "s3": s3} {
		if got := SourceReadOf(m); !got.Known() || got.Folds != -1 {
			t.Errorf("%s reader: %+v, want the read with the count not on record", name, got)
		}
	}
}

// TestLastFileUpserts_legacyLayout: the v0.83.0 layout has no numbered files,
// and asking it for its newest pair must not index an empty slice.
func TestLastFileUpserts_legacyLayout(t *testing.T) {
	c := &TableDeltaChain{Legacy: true, LegacyPosdel: "/s/t.posdel", LegacyUpserts: "/s/t.upserts"}
	if got := c.LastFileUpserts(); got != "/s/t.upserts" {
		t.Fatalf("legacy chain's newest upserts = %q", got)
	}
	n := &TableDeltaChain{Files: []TableDeltaFile{{Seq: 0, Upserts: "/s/t.000000.upserts"}, {Seq: 1, Upserts: "/s/t.000001.upserts"}}}
	if got := n.LastFileUpserts(); got != "/s/t.000001.upserts" {
		t.Fatalf("numbered chain's newest upserts = %q", got)
	}
}
