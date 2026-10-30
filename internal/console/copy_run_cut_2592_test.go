package console

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2592: a table nobody writes is carried unchanged by every refresh with
// its old footer, so its own cut stays at its last full read; once the index
// rotates past that read, its tail cannot be built and every statement that
// joins it reads the published copy. A directory a refresh published holds
// every table through the refresh's cut, which is where that table's lookup
// can start instead.

// runFooter is a footer a refresh writes when it cut the index at file:pos.
func runFooter(file string, pos int64, mark *reconstruct.EventMark, stamp time.Time) baseline.DumpMetadata {
	md := dirFooter(baseline.ProducerReconstruct, file, pos, mark)
	md.SnapshotTimestamp = stamp
	md.FoldCut = file + ":" + strconv.FormatInt(pos, 10)
	return md
}

func TestRunCutIn(t *testing.T) {
	dirAt := time.Date(2026, 10, 30, 19, 10, 36, 0, time.UTC)
	good := runFooter("binlog.000058", 1200, mk(99, "binlog.000058", 1200), dirAt)
	got, ok := runCutIn(good, dirAt)
	if !ok || got.at != (query.BinlogPos{File: "binlog.000058", Pos: 1200}) || got.mark.ID != 99 || !got.stamp.Equal(dirAt) {
		t.Fatalf("the directory's own run's footer: %+v ok=%v", got, ok)
	}
	for _, c := range []struct {
		name string
		edit func(*baseline.DumpMetadata)
		at   time.Time
	}{
		{"carried from an older run, whose cut a replay can leave above this run's",
			func(md *baseline.DumpMetadata) { md.SnapshotTimestamp = dirAt.Add(-5 * time.Minute) }, dirAt},
		{"a run that found the index empty carries its source's position, not a cut",
			func(md *baseline.DumpMetadata) { md.FoldCut = "" }, dirAt},
		{"a fold cut that is not the footer's position",
			func(md *baseline.DumpMetadata) { md.FoldCut = "binlog.000058:1100" }, dirAt},
		{"a full read's file", func(md *baseline.DumpMetadata) { md.Producer = baseline.ProducerDump }, dirAt},
		{"no producer", func(md *baseline.DumpMetadata) { md.Producer = "" }, dirAt},
		{"no mark", func(md *baseline.DumpMetadata) { md.EventMark = "" }, dirAt},
		{"a mark past its position", func(md *baseline.DumpMetadata) { md.EventMark = mk(99, "binlog.000058", 1201).Encode() }, dirAt},
		{"a mark on another numbering", func(md *baseline.DumpMetadata) { md.EventMark = mk(99, "mysql-bin.000001", 4).Encode() }, dirAt},
		{"no position", func(md *baseline.DumpMetadata) { md.BinlogFile, md.BinlogPos, md.FoldCut = "", 0, ":0" }, dirAt},
		{"no directory instant", func(*baseline.DumpMetadata) {}, time.Time{}},
	} {
		md := good
		c.edit(&md)
		if got, ok := runCutIn(md, c.at); ok {
			t.Errorf("%s: cut %+v, want none", c.name, got)
		}
	}
}

func TestPromoteToRunCut(t *testing.T) {
	read := time.Date(2026, 10, 30, 13, 25, 6, 0, time.UTC)
	own := copyCut{anchor: query.BinlogPos{File: "binlog.000047", Pos: 364556972}, stamp: read.Add(4 * time.Hour),
		lastEventID: 0, sourceRead: read, mark: *mk(79656530, "binlog.000047", 364556972), folded: true}
	run := runCut{at: query.BinlogPos{File: "binlog.000058", Pos: 1200}, mark: *mk(99000000, "binlog.000058", 1200),
		stamp: read.Add(6 * time.Hour)}

	got := promoteToRunCut(own, run)
	if got.anchor != run.at || got.mark != run.mark || !got.stamp.Equal(run.stamp) || !got.sinceMark || !got.folded {
		t.Fatalf("a table older than its directory's refresh cut was not moved to it: %+v", got)
	}
	// What the guards that are not about the lookup read stays the table's
	// own: a capture loss since its rows were read still refuses it.
	if !got.sourceRead.Equal(read) || got.lastEventID != own.lastEventID {
		t.Fatalf("the table's own read was replaced: %+v", got)
	}

	for _, c := range []struct {
		name string
		own  copyCut
		run  runCut
	}{
		{"refused stays refused", copyCut{refusal: "no"}, run},
		{"no run cut", own, runCut{}},
		{"the table's cut is the run's", func() copyCut { c := own; c.anchor = run.at; return c }(), run},
		{"the table's cut is past the run's (a newer read)", func() copyCut {
			c := own
			c.anchor = query.BinlogPos{File: "binlog.000059", Pos: 4}
			return c
		}(), run},
		{"another numbering", func() copyCut { c := own; c.anchor = query.BinlogPos{File: "mysql-bin.000001", Pos: 4}; return c }(), run},
	} {
		if got := promoteToRunCut(c.own, c.run); got != c.own {
			t.Errorf("%s: %+v, want the table's own cut", c.name, got)
		}
	}
}

// runCutDir lays out a published directory: quiet carried from an older
// run, busy with a pair this run wrote, and returns it with the footers.
func runCutDir(t *testing.T, success bool) (string, map[string]baseline.DumpMetadata) {
	t.Helper()
	at := time.Date(2026, 10, 30, 19, 10, 36, 0, time.UTC)
	snap := filepath.Join(t.TempDir(), reconstruct.SnapshotDirName(at))
	older := runFooter("binlog.000047", 900, mk(50, "binlog.000047", 900), at.Add(-70*time.Minute))
	files := map[string]baseline.DumpMetadata{
		"shop/quiet.parquet":       older,
		"shop/busy.parquet":        older,
		"shop/busy.000000.upserts": older,
		"shop/busy.000000.posdel":  {},
		"shop/busy.000001.upserts": runFooter("binlog.000058", 1200, mk(99, "binlog.000058", 1200), at),
		"shop/busy.000001.posdel":  {},
	}
	if success {
		files[baseline.SuccessMarker] = baseline.DumpMetadata{}
	}
	for rel := range files {
		p := filepath.Join(snap, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return snap + string(filepath.Separator), files
}

func footersOf(snap string, files map[string]baseline.DumpMetadata, reads map[string]int, fail map[string]error) func(string) (baseline.DumpMetadata, error) {
	return func(p string) (baseline.DumpMetadata, error) {
		rel, _ := filepath.Rel(snap, p)
		reads[rel]++
		if err := fail[rel]; err != nil {
			return baseline.DumpMetadata{}, err
		}
		md, ok := files[rel]
		if !ok {
			return baseline.DumpMetadata{}, os.ErrNotExist
		}
		return md, nil
	}
}

func TestCopyRunCutMemo(t *testing.T) {
	noWarn := func(path string, err error) { t.Errorf("warned about %s: %v", path, err) }

	snap, files := runCutDir(t, true)
	reads := map[string]int{}
	var m copyRunCutMemo
	got, ok := m.of(snap, footersOf(snap, files, reads, nil), noWarn)
	if !ok || got.at != (query.BinlogPos{File: "binlog.000058", Pos: 1200}) {
		t.Fatalf("directory cut = %+v ok=%v, want the pair this run wrote", got, ok)
	}
	if reads["shop/busy.000000.upserts"] != 0 {
		t.Fatal("an older pair was read: only the newest pair beside each table can be this run's")
	}
	// A published directory never changes: the second ask reads nothing.
	clear(reads)
	if _, ok := m.of(snap, footersOf(snap, files, reads, nil), noWarn); !ok || len(reads) != 0 {
		t.Fatalf("the second ask read %v", reads)
	}

	// A directory where no file is this run's (a full read's, or every table
	// carried): none, remembered once the directory is complete.
	carried := runFooter("binlog.000047", 900, mk(50, "binlog.000047", 900), time.Date(2026, 10, 30, 18, 0, 0, 0, time.UTC))
	snap2, files2 := runCutDir(t, true)
	files2["shop/busy.000001.upserts"] = carried
	var m2 copyRunCutMemo
	clear(reads)
	if got, ok := m2.of(snap2, footersOf(snap2, files2, reads, nil), noWarn); ok {
		t.Fatalf("no file of this run, yet a cut: %+v", got)
	}
	clear(reads)
	m2.of(snap2, footersOf(snap2, files2, reads, nil), noWarn)
	if len(reads) != 0 {
		t.Fatal("a complete directory's \"none\" was not remembered")
	}

	// With no marker at all (older than them): complete, remembered.
	snap3, files3 := runCutDir(t, false)
	files3["shop/busy.000001.upserts"] = carried
	var m3 copyRunCutMemo
	m3.of(snap3, footersOf(snap3, files3, reads, nil), noWarn)
	clear(reads)
	m3.of(snap3, footersOf(snap3, files3, reads, nil), noWarn)
	if len(reads) != 0 {
		t.Fatal("a directory older than the markers was read again for its \"none\"")
	}
	// Marked incomplete: "none" is asked again.
	if err := os.WriteFile(filepath.Join(snap3, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	var m3b copyRunCutMemo
	m3b.of(snap3, footersOf(snap3, files3, reads, nil), noWarn)
	clear(reads)
	m3b.of(snap3, footersOf(snap3, files3, reads, nil), noWarn)
	if len(reads) == 0 {
		t.Fatal("an incomplete directory's \"none\" was remembered")
	}

	// The run's own file cannot be read: no cut, said, and asked again.
	snap4, files4 := runCutDir(t, true)
	var m4 copyRunCutMemo
	var warned []string
	warn := func(path string, err error) { warned = append(warned, path) }
	fail := map[string]error{"shop/busy.000001.upserts": os.ErrPermission}
	if got, ok := m4.of(snap4, footersOf(snap4, files4, reads, fail), warn); ok {
		t.Fatalf("the run's file is unreadable, yet a cut: %+v", got)
	}
	if len(warned) == 0 {
		t.Fatal("an unreadable file was skipped without a word")
	}
	clear(reads)
	if _, ok := m4.of(snap4, footersOf(snap4, files4, reads, nil), noWarn); !ok {
		t.Fatal("a skipped file's \"none\" was remembered")
	}

	// Another table's unreadable file does not cost the directory its cut.
	snap5, files5 := runCutDir(t, true)
	var m5 copyRunCutMemo
	warned = nil
	if _, ok := m5.of(snap5, footersOf(snap5, files5, reads, map[string]error{"shop/quiet.parquet": errors.New("damaged")}), warn); !ok {
		t.Fatal("one damaged footer cost every table its cut")
	}

	// Not a snapshot directory's name: none, nothing read.
	var m6 copyRunCutMemo
	clear(reads)
	if _, ok := m6.of(filepath.Join(t.TempDir(), "notasnapshot")+string(filepath.Separator), footersOf(snap, files, reads, nil), noWarn); ok || len(reads) != 0 {
		t.Fatal("a directory with no snapshot name was read for a cut")
	}
}

// The pointer to the newest snapshot moves: a cut remembered for it would
// be applied to whatever it points at next. A directory reached through a
// link is never asked.
func TestTailLookupCut_neverThroughTheCurrentPointer(t *testing.T) {
	snap, files := runCutDir(t, true)
	// Named like a snapshot directory, so only the link check refuses it:
	// the pointer's own name already gives no snapshot instant.
	link := filepath.Join(filepath.Dir(filepath.Clean(snap)), reconstruct.SnapshotDirName(time.Date(2026, 10, 30, 19, 15, 36, 0, time.UTC)))
	if err := os.Symlink(filepath.Clean(snap), link); err != nil {
		t.Fatal(err)
	}
	s := &Server{}
	// Every file reads as its footer through the link too, so nothing but
	// the link check stands between the table and the run's cut.
	s.copyCuts.cuts = map[copyCutKey]baseline.DumpMetadata{}
	for rel, md := range files {
		p := filepath.Join(link, rel)
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		s.copyCuts.cuts[copyCutKey{p, fi.Size(), fi.ModTime().UnixNano()}] = md
	}
	md := cutFooter()
	md.BinlogFile, md.BinlogPos = "binlog.000047", 900
	md.LastDumpAt, md.FoldGeneration = cutStamp, 0
	quiet := views.BaselineTable{Schema: "shop", Table: "quiet", Path: filepath.Join(link, "shop", "quiet.parquet")}
	got := promoteViaPointer(s, quiet, md)
	if got.refusal != "" {
		t.Fatalf("fixture: the table's own cut is refused: %s", got.refusal)
	}
	if got.sinceMark {
		t.Fatalf("a table read through the pointer was moved to a cut: %+v", got)
	}
	if len(s.copyRunCuts.dirs) != 0 {
		t.Fatal("the pointer's directory was remembered")
	}
	// The same table through the directory itself is moved: the link is
	// what refused it above.
	direct := quiet
	direct.Path = filepath.Join(filepath.Clean(snap), "shop", "quiet.parquet")
	for rel, md := range files {
		p := filepath.Join(filepath.Clean(snap), rel)
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		s.copyCuts.cuts[copyCutKey{p, fi.Size(), fi.ModTime().UnixNano()}] = md
	}
	if got := promoteViaPointer(s, direct, md); !got.sinceMark {
		t.Fatalf("control: the table through its own directory was not moved: %+v", got)
	}
}

// promoteViaPointer runs tailLookupCut with t's footer stubbed as md.
func promoteViaPointer(s *Server, t views.BaselineTable, md baseline.DumpMetadata) copyCut {
	cut := copyCutOf(t, func(string) (baseline.DumpMetadata, error) { return md, nil })
	if cut.refusal != "" {
		return cut
	}
	return s.tailLookupCutFrom(t, cut)
}
