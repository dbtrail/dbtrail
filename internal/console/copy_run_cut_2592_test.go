package console

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2592: a table nobody writes is carried unchanged by every refresh with
// its old footer, so its own cut stays at its last full read; once the index
// rotates past that read, its tail cannot be built and every statement that
// joins it reads the published copy. A directory a refresh published holds
// every table through the refresh's cut, which is where that table's lookup
// can start instead.

func runFooter(producer, file string, pos int64, mark *reconstruct.EventMark, stamp time.Time) baseline.DumpMetadata {
	md := dirFooter(producer, file, pos, mark)
	md.SnapshotTimestamp = stamp
	return md
}

func TestRunCutOf(t *testing.T) {
	const rec, dump = baseline.ProducerReconstruct, baseline.ProducerDump
	t1 := time.Date(2026, 10, 30, 18, 0, 36, 0, time.UTC)
	t2 := t1.Add(5 * time.Minute)
	cases := []struct {
		name    string
		footers []baseline.DumpMetadata
		want    query.BinlogPos // zero = none
		mark    uint64
		stamp   time.Time
	}{
		{name: "nothing read"},
		{name: "a full read's directory has no refresh cut",
			footers: []baseline.DumpMetadata{
				runFooter(dump, "binlog.000047", 500, mk(9, "binlog.000047", 400), t1),
				runFooter(dump, "binlog.000047", 500, mk(9, "binlog.000047", 400), t1),
			}},
		{name: "the refresh's newest cut, over a carried older one",
			footers: []baseline.DumpMetadata{
				runFooter(rec, "binlog.000047", 900, mk(50, "binlog.000047", 900), t1),
				runFooter(rec, "binlog.000048", 300, mk(70, "binlog.000048", 300), t2),
				runFooter(dump, "binlog.000047", 500, mk(9, "binlog.000047", 400), t1),
			},
			want: query.BinlogPos{File: "binlog.000048", Pos: 300}, mark: 70, stamp: t2},
		{name: "a refresh footer with no mark is skipped, which only lowers the cut",
			footers: []baseline.DumpMetadata{
				runFooter(rec, "binlog.000048", 300, nil, t2),
				runFooter(rec, "binlog.000047", 900, mk(50, "binlog.000047", 900), t1),
			},
			want: query.BinlogPos{File: "binlog.000047", Pos: 900}, mark: 50, stamp: t1},
		{name: "a mark past its own position is skipped",
			footers: []baseline.DumpMetadata{
				runFooter(rec, "binlog.000048", 300, mk(70, "binlog.000048", 301), t2),
			}},
		{name: "a refresh footer with no position is skipped",
			footers: []baseline.DumpMetadata{
				runFooter(rec, "", 0, mk(70, "binlog.000048", 300), t2),
			}},
		{name: "two numberings: no cut",
			footers: []baseline.DumpMetadata{
				runFooter(rec, "binlog.000047", 900, mk(50, "binlog.000047", 900), t1),
				runFooter(rec, "mysql-bin.000002", 300, mk(70, "mysql-bin.000002", 300), t2),
			}},
	}
	for _, c := range cases {
		got, ok := runCutOf(c.footers)
		if (c.want == query.BinlogPos{}) {
			if ok {
				t.Errorf("%s: cut %+v, want none", c.name, got)
			}
			continue
		}
		if !ok || got.at != c.want || got.mark.ID != c.mark || !got.stamp.Equal(c.stamp) {
			t.Errorf("%s: cut %+v ok=%v, want %v mark %d stamp %v", c.name, got, ok, c.want, c.mark, c.stamp)
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
		{"the table's cut is past the run's (a newer read)", func() copyCut { c := own; c.anchor = query.BinlogPos{File: "binlog.000059", Pos: 4}; return c }(), run},
		{"another numbering", func() copyCut { c := own; c.anchor = query.BinlogPos{File: "mysql-bin.000001", Pos: 4}; return c }(), run},
	} {
		if got := promoteToRunCut(c.own, c.run); got != c.own {
			t.Errorf("%s: %+v, want the table's own cut", c.name, got)
		}
	}
}

// The directory's cut is read from every table's file and the newest pair
// beside it, as copyDirMark is; a file that cannot be read gives no cut.
func TestCopyRunCutMemo(t *testing.T) {
	dir := t.TempDir()
	stamp := time.Date(2026, 10, 30, 19, 10, 36, 0, time.UTC)
	snap := filepath.Join(dir, reconstruct.SnapshotDirName(stamp))
	files := map[string]baseline.DumpMetadata{
		"shop/static.parquet":      runFooter(baseline.ProducerReconstruct, "binlog.000047", 900, mk(50, "binlog.000047", 900), stamp.Add(-70*time.Minute)),
		"shop/busy.parquet":        runFooter(baseline.ProducerReconstruct, "binlog.000047", 900, mk(50, "binlog.000047", 900), stamp.Add(-70*time.Minute)),
		"shop/busy.000000.upserts": runFooter(baseline.ProducerReconstruct, "binlog.000047", 900, mk(50, "binlog.000047", 900), stamp.Add(-70*time.Minute)),
		"shop/busy.000000.posdel":  {},
		"shop/busy.000001.upserts": runFooter(baseline.ProducerReconstruct, "binlog.000058", 1200, mk(99, "binlog.000058", 1200), stamp),
		"shop/busy.000001.posdel":  {},
		"other/thing.parquet":      runFooter(baseline.ProducerReconstruct, "binlog.000047", 900, mk(50, "binlog.000047", 900), stamp.Add(-70*time.Minute)),
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
	reads := map[string]int{}
	footer := func(p string) (baseline.DumpMetadata, error) {
		rel, _ := filepath.Rel(snap, p)
		reads[rel]++
		md, ok := files[rel]
		if !ok {
			return baseline.DumpMetadata{}, os.ErrNotExist
		}
		return md, nil
	}
	var m copyRunCutMemo
	got, ok := m.of(snap+string(filepath.Separator), footer)
	if !ok || got.at != (query.BinlogPos{File: "binlog.000058", Pos: 1200}) {
		t.Fatalf("directory cut = %+v ok=%v, want the newest pair's", got, ok)
	}
	if reads["shop/busy.000000.upserts"] != 0 {
		t.Fatal("an older pair was read: only the newest pair beside each table bounds it")
	}
	// A published directory never changes: the second ask reads nothing.
	before := len(reads)
	for k := range reads {
		reads[k] = 0
	}
	if _, ok := m.of(snap+string(filepath.Separator), footer); !ok {
		t.Fatal("the remembered cut was lost")
	}
	for k, n := range reads {
		if n != 0 {
			t.Fatalf("the second ask read %s again (%d files read before)", k, before)
		}
	}

	// One table whose footer cannot be read: no cut, nothing guessed.
	var m2 copyRunCutMemo
	broken := func(p string) (baseline.DumpMetadata, error) {
		if filepath.Base(p) == "thing.parquet" {
			return baseline.DumpMetadata{}, os.ErrPermission
		}
		return footer(p)
	}
	if got, ok := m2.of(snap+string(filepath.Separator), broken); ok {
		t.Fatalf("a directory with an unreadable footer gave a cut: %+v", got)
	}
}
