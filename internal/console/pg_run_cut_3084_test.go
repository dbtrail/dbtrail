package console

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// writePGRunCut3084 writes dir's run cut as an update writes it
// (reconstruct.PGRunCutFile).
func writePGRunCut3084(t *testing.T, dir string, at time.Time, lsn, reach uint64, tables ...string) {
	t.Helper()
	held := [][2]string{}
	for _, n := range tables {
		s, tb, _ := strings.Cut(n, ".")
		held = append(held, [2]string{s, tb})
	}
	b, err := json.Marshal(map[string]any{"version": 1, "lsn": lsn, "reach_id": reach, "at": at.UTC().Format(time.RFC3339), "tables": held})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, reconstruct.PGRunCutFile), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// What Cut hands the keeper for a table, from its own cut and its folder's
// run cut. Every case is a table whose own floor is 0x500.
func TestPGTailSource_runCutFloor_3084(t *testing.T) {
	at := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	own := tailFloor{anchor: reconstruct.PGTailPos(0x500)}
	type folder struct {
		name    string
		at      time.Time
		noCut   bool   // no record
		lsn     uint64 // the run's end plus one
		reach   uint64
		tables  []string
		corrupt bool
		incomplete bool
	}
	cases := []struct {
		name   string
		f      folder
		table  string // default orders
		md     baseline.DumpMetadata
		link   bool // the table read through a symlink to the folder
		want   tailFloor
	}{
		// The first update after this change is installed: its folder has
		// no record, and the table's floor is its own, as before.
		{name: "no record", f: folder{noCut: true}, want: own},
		{name: "carried: the run's end and its id", f: folder{lsn: 0x701, reach: 9, tables: []string{"public.orders"}},
			want: tailFloor{anchor: reconstruct.PGTailPos(0x701), reachID: 9}},
		{name: "carried, the run's end with no id", f: folder{lsn: 0x701, tables: []string{"public.orders"}},
			want: tailFloor{anchor: reconstruct.PGTailPos(0x701)}},
		{name: "written at the run's end: its own floor with the run's id", f: folder{lsn: 0x500, reach: 9, tables: []string{"public.orders"}},
			want: tailFloor{anchor: reconstruct.PGTailPos(0x500), reachID: 9}},
		// A newer full read the run held at its own floor.
		{name: "own floor past the run's end", f: folder{lsn: 0x400, reach: 9, tables: []string{"public.orders"}}, want: own},
		{name: "the table is not in the record", f: folder{lsn: 0x701, reach: 9, tables: []string{"public.items"}}, want: own},
		{name: "another name's case", f: folder{lsn: 0x701, reach: 9, tables: []string{"public.Orders", "Public.orders", "public.orders "}}, want: own},
		{name: "a record written for another folder", f: folder{lsn: 0x701, reach: 9, at: at.Add(time.Second), tables: []string{"public.orders"}}, want: own},
		{name: "a record that does not read", f: folder{corrupt: true}, want: own},
		{name: "a folder still marked incomplete", f: folder{lsn: 0x701, reach: 9, tables: []string{"public.orders"}, incomplete: true}, want: own},
		// A full read's file whose rows are past its floor: anchorReached
		// decides when a tail goes over it, from its own floor.
		{name: "a full read with rows past its floor", f: folder{lsn: 0x701, reach: 9, tables: []string{"public.orders"}},
			md: baseline.DumpMetadata{PGAnchorLSN: 0x900}, want: own},
		{name: "through the pointer to the newest snapshot", f: folder{name: baseline.CurrentLinkName, lsn: 0x701, reach: 9, tables: []string{"public.orders"}}, want: own},
		{name: "through a link to the folder", f: folder{lsn: 0x701, reach: 9, tables: []string{"public.orders"}}, link: true, want: own},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			name := c.f.name
			if name == "" {
				name = reconstruct.SnapshotDirName(at)
			}
			dir := filepath.Join(root, name)
			if err := os.MkdirAll(filepath.Join(dir, "public"), 0o755); err != nil {
				t.Fatal(err)
			}
			recAt := at
			if !c.f.at.IsZero() {
				recAt = c.f.at
			}
			switch {
			case c.f.corrupt:
				if err := os.WriteFile(filepath.Join(dir, reconstruct.PGRunCutFile), []byte("{"), 0o644); err != nil {
					t.Fatal(err)
				}
			case !c.f.noCut:
				writePGRunCut3084(t, dir, recAt, c.f.lsn, c.f.reach, c.f.tables...)
			}
			if c.f.incomplete {
				if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			read := dir
			if c.link {
				// A link named as a snapshot folder of another time.
				read = filepath.Join(t.TempDir(), reconstruct.SnapshotDirName(at))
				if err := os.Symlink(dir, read); err != nil {
					t.Fatal(err)
				}
			}
			table := c.table
			if table == "" {
				table = "orders"
			}
			x := newPGTailSource(&Server{}, &bundle{})
			tbl := views.BaselineTable{Schema: "public", Table: table, Path: filepath.Join(read, "public", table+".parquet")}
			got := x.runCutFloor(tbl, pgCut{md: c.md, footer: true, floor: 0x500})
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("floor %+v, want %+v", got, c.want)
			}
			if got.lastEventID != 0 {
				t.Fatal("a PostgreSQL cut names a last event id: the chain-keeping prune (pg_tail_chain.go) would refuse it")
			}
		})
	}
}

// The memo: a found record and a folder with none stand; a record that does
// not read and an incomplete folder are asked again.
func TestPGRunCutMemo_3084(t *testing.T) {
	at := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	dir := filepath.Join(t.TempDir(), reconstruct.SnapshotDirName(at))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var m pgRunCutMemo
	warned := 0
	warn := func(error) { warned++ }
	if err := os.WriteFile(filepath.Join(dir, reconstruct.PGRunCutFile), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.of(dir, warn); ok || warned != 1 {
		t.Fatalf("a record that does not read: ok %v, %d warnings", ok, warned)
	}
	if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	writePGRunCut3084(t, dir, at, 0x701, 9, "public.orders")
	if _, ok := m.of(dir, warn); ok {
		t.Fatal("an incomplete folder's record was used")
	}
	if err := os.Remove(filepath.Join(dir, baseline.IncompleteMarker)); err != nil {
		t.Fatal(err)
	}
	cut, ok := m.of(dir, warn)
	if !ok || cut.LSN != 0x701 {
		t.Fatalf("once complete: ok %v, %+v", ok, cut)
	}
	// A published folder is not written again: the record stands.
	if err := os.Remove(filepath.Join(dir, reconstruct.PGRunCutFile)); err != nil {
		t.Fatal(err)
	}
	if cut, ok := m.of(dir, warn); !ok || cut.LSN != 0x701 {
		t.Fatalf("remembered: ok %v, %+v", ok, cut)
	}
}

// pgRunCutIndex is pgChainIndex whose Cut a test sets per table file: where
// the update moved it, as runCutFloor would. Fill still reads from the file's
// own floor, as the real source does.
type pgRunCutIndex struct {
	*pgChainIndex
	cuts map[string]tailFloor
}

func (f *pgRunCutIndex) Cut(t views.BaselineTable) (tailFloor, string) {
	f.mu.Lock()
	c, ok := f.cuts[t.Path]
	f.mu.Unlock()
	if ok {
		return c, ""
	}
	return f.pgChainIndex.Cut(t)
}

// A table nobody writes, carried by every update, keeps its tail across
// three updates and is read through it on the first look at each, while
// another table changes and while nothing does. It used to be built again
// from nothing at each update, and the first statement read the snapshot
// alone.
func TestPGRunCut_aCarriedTableKeepsItsTail_3084(t *testing.T) {
	ctx := context.Background()
	newRig := func(t *testing.T) (*pgRunCutIndex, *tailKeeper, func(dir string, cut tailFloor) views.BaselineTable) {
		f := &pgRunCutIndex{pgChainIndex: &pgChainIndex{pgCopyIndex: &pgCopyIndex{floors: map[string]uint64{}}, refuse: map[string]bool{}}, cuts: map[string]tailFloor{}}
		root := t.TempDir()
		// Every folder carries the same file: own floor 0x500.
		cutIn := func(dir string, cut tailFloor) views.BaselineTable {
			c := pgCopyCut(t, filepath.Join(root, dir), "orders", 0x500, [3]string{"1", "one", "n1"}, [3]string{"2", "two", "n2"})
			f.floors[c.Path] = 0x500
			if cut.anchor.File != "" {
				f.cuts[c.Path] = cut
			}
			return c
		}
		f.update("items", 0x600, "9", "x", "x") // another table: id 1
		k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 1000}, time.Minute, time.Hour)
		k.retryMin, k.retryMax = 0, 0
		t.Cleanup(k.close)
		return f, k, cutIn
	}
	at := func(lsn, reach uint64) tailFloor {
		return tailFloor{anchor: reconstruct.PGTailPos(lsn), reachID: reach}
	}
	key := tailKey("public", "orders")
	look := func(t *testing.T, k *tailKeeper, c views.BaselineTable) bool {
		t.Helper()
		got, err := k.refresh(ctx, []views.BaselineTable{c})
		if err != nil {
			t.Fatal(err)
		}
		_, ok := got[key]
		return ok
	}

	t.Run("three updates", func(t *testing.T) {
		f, k, cutIn := newRig(t)
		first := cutIn("snap0", tailFloor{})
		settled(t, ctx, k, first)
		if !look(t, k, first) {
			t.Fatal("no tail over the first snapshot")
		}
		fills := func() int {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.fills
		}
		built := fills()
		// Another table changed: the update ended after it.
		if !look(t, k, cutIn("snap1", at(0x601, 1))) {
			t.Fatal("first update: the carried table has no tail on the first look")
		}
		// Nothing changed since: the same end and id.
		if !look(t, k, cutIn("snap2", at(0x601, 1))) {
			t.Fatal("second update, a quiet source: the carried table has no tail on the first look")
		}
		f.update("items", 0x700, "9", "y", "y")
		if !look(t, k, cutIn("snap3", at(0x701, 2))) {
			t.Fatal("third update: the carried table has no tail on the first look")
		}
		k.settle()
		if n := fills() - built; n != 0 {
			t.Fatalf("the tail was built again %d times", n)
		}
		// And it takes the table's next change.
		f.update("orders", 0x800, "1", "uno", "u")
		last := cutIn("snap4", at(0x701, 2))
		want := []string{"1=uno/u", "2=two/n2"}
		if s := pgCopyState(t, last, settled(t, ctx, k, last)[key]); !reflect.DeepEqual(s, want) {
			t.Fatalf("after a change: %v, want %v", s, want)
		}
	})

	// What keeps it from standing, each on its own.
	for _, c := range []struct {
		name   string
		cut    tailFloor
		refuse bool // the two files are not from the same read
		second bool // checked at the second update, over the same floor
	}{
		{name: "no record: the floor did not move", cut: tailFloor{}},
		{name: "the run's end with no id, past the cursor", cut: at(0x601, 0)},
		{name: "an id the cursor has not reached", cut: at(0x601, 5)},
		{name: "the same floor with no id", cut: at(0x601, 0), second: true},
		{name: "the same floor from another read", cut: at(0x601, 1), refuse: true, second: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, k, cutIn := newRig(t)
			prev := cutIn("snap0", tailFloor{})
			if c.second {
				prev = cutIn("snap1", at(0x601, 1))
			}
			settled(t, ctx, k, prev)
			next := cutIn("snap9", c.cut)
			if c.refuse {
				f.refuse[next.Path] = true
			}
			if look(t, k, next) {
				t.Fatal("the tail stood for the newer cut")
			}
		})
	}
}

// MySQL never sees the field: only PostgreSQL's files of this package name
// it, and the keeper only reads it.
func TestTailFloorReachID_onlyPostgres_3084(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range files {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), "reachID") {
			continue
		}
		switch p {
		case "pg_run_cut.go":
		case "tail_keeper.go":
			for _, line := range strings.Split(string(b), "\n") {
				if strings.Contains(line, "reachID =") || strings.Contains(line, "reachID:") {
					t.Errorf("tail_keeper.go sets reachID: %s", strings.TrimSpace(line))
				}
			}
		default:
			t.Errorf("%s names tailFloor.reachID: only PostgreSQL's files may", p)
		}
	}
	// With no id, reachedFloor is what it was.
	k := &tailKeeper{mark: tailMark{ID: 10, Pos: reconstruct.PGTailPos(0x600)}}
	for _, c := range []struct {
		to   tailFloor
		want bool
	}{
		{tailFloor{anchor: reconstruct.PGTailPos(0x600)}, true},
		{tailFloor{anchor: reconstruct.PGTailPos(0x601)}, false},
		{tailFloor{anchor: reconstruct.PGTailPos(0x601), lastEventID: 10}, true},
		{tailFloor{anchor: reconstruct.PGTailPos(0x601), reachID: 10}, true},
		{tailFloor{anchor: reconstruct.PGTailPos(0x601), reachID: 11}, false},
	} {
		if got := k.reachedFloor(c.to); got != c.want {
			t.Errorf("reachedFloor(%+v) = %v, want %v", c.to, got, c.want)
		}
	}
}
