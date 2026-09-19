package reconstruct

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #1720: the fold stamps the highest event_id it applied into the files it
// writes, and the next fold floors its fetch on that id INSIDE the index —
// but only when a stream wrote the index, because only then is event_id order
// binlog order. The exact position gate (#797) is untouched either way; the
// floor is a hint that lets the range scan skip the rows a previous fold
// already read instead of re-reading and re-filtering them.

func TestEventIDFloor_onlyOnAStreamWrittenIndex(t *testing.T) {
	anchor := baseline.DumpMetadata{LastEventID: 4500}
	if got := eventIDFloor(anchor, true); got != 4500 {
		t.Fatalf("stream index: floor = %d, want the anchor's 4500", got)
	}
	if got := eventIDFloor(anchor, false); got != 0 {
		t.Fatalf("file-mode index: floor = %d, want none (ids there follow the file order given, not binlog order)", got)
	}
	if got := eventIDFloor(baseline.DumpMetadata{}, true); got != 0 {
		t.Fatalf("unstamped anchor: floor = %d, want none", got)
	}
}

func TestLastEventIDFor_neverMovesBackwards(t *testing.T) {
	anchor := baseline.DumpMetadata{LastEventID: 700}
	// No stream wrote the index: no stamp, whatever was applied or carried.
	// A file-era stamp trusted once a stream starts would floor on ids that
	// were never in binlog order (the hunter's case), so it is never made.
	if got := lastEventIDFor(&foldResult{LastEventID: 900}, anchor, false); got != 0 {
		t.Fatalf("file-mode index: stamp = %d, want none", got)
	}
	cases := []struct {
		name string
		fold *foldResult
		want uint64
	}{
		{"window applied newer events", &foldResult{LastEventID: 900}, 900},
		{"empty window carries the anchor's", &foldResult{}, 700},
		{"no fold at all carries the anchor's", nil, 700},
		// Impossible on a stream index (the fetch floors above 700), but a
		// file-mode index can hand back lower ids: the stamp still never
		// drops, or the next floor would re-read a window already folded.
		{"older ids than the anchor keep the anchor's", &foldResult{LastEventID: 650}, 700},
	}
	for _, c := range cases {
		if got := lastEventIDFor(c.fold, anchor, true); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
	if got := lastEventIDFor(&foldResult{LastEventID: 3}, baseline.DumpMetadata{}, true); got != 3 {
		t.Fatalf("first stamped fold over an unstamped anchor: %d, want 3", got)
	}
}

// TestFetchFloor_carriesLastEventID: the chain's last pair, not the base,
// is where the floor comes from with deltas on, position and id from the
// SAME footer; a legacy (v0.83.0) pair that never stamped one gives no
// floor, even over a stamped base, because the base's id was applied up to
// the base's position, not the pair's.
func TestFetchFloor_carriesLastEventID(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	base := baseline.DumpMetadata{BinlogFile: "binlog.000007", BinlogPos: 4, SnapshotTimestamp: t0, LastEventID: 100}

	_, anchor := fetchFloor(t0, base, nil)
	if anchor.LastEventID != 100 {
		t.Fatalf("no chain: LastEventID = %d, want the base's 100", anchor.LastEventID)
	}
	prev := &tableDelta{Meta: baseline.DumpMetadata{BinlogFile: "binlog.000009", BinlogPos: 3000, SnapshotTimestamp: t0.Add(time.Hour), LastEventID: 250}}
	_, anchor = fetchFloor(t0, base, prev)
	if anchor.LastEventID != 250 {
		t.Fatalf("with chain: LastEventID = %d, want the last pair's 250", anchor.LastEventID)
	}
	legacy := &tableDelta{Meta: baseline.DumpMetadata{BinlogFile: "binlog.000009", BinlogPos: 3000, SnapshotTimestamp: t0.Add(time.Hour)}}
	_, anchor = fetchFloor(t0, base, legacy)
	if anchor.LastEventID != 0 {
		t.Fatalf("unstamped pair: LastEventID = %d, want none (the base's 100 belongs to the base's position)", anchor.LastEventID)
	}
}

// TestSnapshotFileMetadata_stampsLastEventID: present as a decimal only when
// there is one; an older reader ignores the key, a newer one over an
// unstamped file sees 0 and keeps its time floor.
func TestSnapshotFileMetadata_stampsLastEventID(t *testing.T) {
	in := mergeInput{Schema: "s", Table: "t", SnapshotAt: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)}
	if _, ok := snapshotFileMetadata(in)[baseline.MetaKeyLastEventID]; ok {
		t.Fatal("a zero LastEventID was stamped")
	}
	in.LastEventID = 18446744073709551615 // the widest event_id the column holds
	if got := snapshotFileMetadata(in)[baseline.MetaKeyLastEventID]; got != "18446744073709551615" {
		t.Fatalf("stamp = %q", got)
	}
}

// TestTableDelta_pairFootersCarryLastEventID goes through the real writer: a
// window that applied events stamps their highest id on BOTH files of the
// pair, an empty window carries the previous pair's stamp forward (the fetch
// floor must not drop back to the base's), and a compaction keeps it on the
// rewritten base.
func TestTableDelta_pairFootersCarryLastEventID(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	stamp := func(id uint64) func(*tableDeltaPublish) {
		return func(p *tableDeltaPublish) { p.fold.LastEventID = id }
	}
	footer := func(path string) uint64 {
		t.Helper()
		m, err := baseline.ReadParquetMetadata(path)
		if err != nil {
			t.Fatalf("footer of %s: %v", path, err)
		}
		return m.LastEventID
	}

	at1 := t0.Add(5 * time.Minute)
	base1, _, err := deltaWindow(t, root, src, t0, changeMap(upd(1, "v1")), at1, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, stamp(41))
	if err != nil {
		t.Fatal(err)
	}
	// A base with no chain starts one at 0, carrying this window's changes.
	posdel, upserts := baseline.TableDeltaPaths(base1, 0)
	if footer(posdel) != 41 || footer(upserts) != 41 {
		t.Fatalf("first pair: posdel %d, upserts %d, want 41 on both", footer(posdel), footer(upserts))
	}
	// The same window over an index no stream wrote leaves no stamp.
	fileEra, _, err := deltaWindow(t, t.TempDir(), src, t0, changeMap(upd(1, "v1")), at1, &query.BinlogPos{File: "binlog.000009", Pos: 1000},
		func(p *tableDeltaPublish) { p.fold.LastEventID = 41; p.streamCaptured = false })
	if err != nil {
		t.Fatal(err)
	}
	if _, u := baseline.TableDeltaPaths(fileEra, 0); footer(u) != 0 {
		t.Fatalf("file-mode index: the pair was stamped %d", footer(u))
	}

	// An empty window: the pair is not written, and the stamp the chain
	// carries is still the first pair's.
	at2 := t0.Add(10 * time.Minute)
	base2, rep, err := deltaWindow(t, root, base1, at1, map[string]*query.ResultRow{}, at2, &query.BinlogPos{File: "binlog.000009", Pos: 2000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.TableDelta || rep.DeltaChainFiles != 1 {
		t.Fatalf("empty window: TableDelta %v, chain files %d; want the chain carried unchanged", rep.TableDelta, rep.DeltaChainFiles)
	}
	bmeta, err := baseline.ReadParquetMetadata(base2)
	if err != nil {
		t.Fatal(err)
	}
	prev, err := readTableDelta(t.Context(), base2, bmeta)
	if err != nil || prev == nil {
		t.Fatalf("readTableDelta after the empty window: %+v, %v", prev, err)
	}
	if _, anchor := fetchFloor(t0, bmeta, prev); anchor.LastEventID != 41 {
		t.Fatalf("floor after an empty window = %d, want the first pair's 41", anchor.LastEventID)
	}

	// A window that applied more, then a compaction whose fold reports no id
	// of its own (stamp 0): the rewritten base carries the CHAIN's newest
	// stamp, read from the last pair's footer, and the fresh empty pair 0
	// does too. This is the carry through the real writer.
	at3 := t0.Add(15 * time.Minute)
	base3, _, err := deltaWindow(t, root, base2, at2, changeMap(upd(2, "v2")), at3, &query.BinlogPos{File: "binlog.000009", Pos: 3000}, stamp(77))
	if err != nil {
		t.Fatal(err)
	}
	posdel, upserts = baseline.TableDeltaPaths(base3, 1)
	if footer(upserts) != 77 {
		t.Fatalf("second pair: %d, want 77", footer(upserts))
	}
	at4 := t0.Add(20 * time.Minute)
	base4, rep, err := deltaWindow(t, root, base3, at3, changeMap(upd(3, "v3")), at4, &query.BinlogPos{File: "binlog.000009", Pos: 4000},
		func(p *tableDeltaPublish) {
			p.fold.LastEventID = 0
			p.fold.Spill = spillOf(t, 1, changeMap(upd(3, "v3")))
			p.fold.Changes = nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if rep.TableDelta {
		t.Fatal("the spilled window was not compacted")
	}
	posdel, upserts = baseline.TableDeltaPaths(base4, 0)
	if footer(base4) != 77 || footer(posdel) != 77 || footer(upserts) != 77 {
		t.Fatalf("after compaction: base %d, pair 0 %d/%d, want the chain's 77 everywhere", footer(base4), footer(posdel), footer(upserts))
	}
}
