package reconstruct

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2495: a tail written as a chain, one WriteFile and then a WriteDelta per
// cycle, reads through baseline.TableTailChainStateSQL as the same state as
// the whole tail written as one file. Read through DuckDB: the seam that can
// go wrong is an older segment's row winning over a newer one's, or a row
// deleted in a newer segment coming back.

// tailChain is a tail and the segments written of it, oldest first.
type tailChain struct {
	tail *Tail
	dir  string
	segs []string
	n    int // names are never reused, as the keeper's are not
}

func newTailChain(t *testing.T, base string) *tailChain {
	t.Helper()
	tail, err := NewTail("shop", "orders", tailOrdersDDL, base, tailOrdersPK)
	if err != nil {
		t.Fatal(err)
	}
	return &tailChain{tail: tail, dir: t.TempDir()}
}

func (c *tailChain) next() string {
	c.n++
	return filepath.Join(c.dir, fmt.Sprintf("%d-shop.orders.parquet", c.n))
}

// apply folds pages into the tail.
func (c *tailChain) apply(t *testing.T, pages ...[]query.ResultRow) {
	t.Helper()
	for _, p := range pages {
		if err := c.tail.Apply(p); err != nil {
			t.Fatal(err)
		}
	}
}

// full starts a new chain with the whole tail.
func (c *tailChain) full(t *testing.T) int64 {
	t.Helper()
	p := c.next()
	n, err := c.tail.WriteFile(p)
	if err != nil {
		t.Fatal(err)
	}
	c.segs = []string{p}
	return n
}

// delta adds a segment with the keys changed since the last one.
func (c *tailChain) delta(t *testing.T) int64 {
	t.Helper()
	p := c.next()
	n, err := c.tail.WriteDelta(p)
	if err != nil {
		t.Fatal(err)
	}
	c.segs = append(c.segs, p)
	return n
}

func (c *tailChain) sql(published string) string {
	segs := make([]string, len(c.segs))
	for i, p := range c.segs {
		segs[i] = lit(p)
	}
	return baseline.TableTailChainStateSQL(published, segs, c.tail.PKColumns(), "")
}

// single is the same tail written as one file, over published.
func (c *tailChain) single(t *testing.T, published string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "whole.parquet")
	dirty := c.tail.Dirty()
	if _, err := c.tail.WriteFile(p); err != nil {
		t.Fatal(err)
	}
	if dirty > 0 {
		t.Fatal("the reference write would clear changed keys the chain has not written")
	}
	return baseline.TableTailStateSQL(published, lit(p), c.tail.PKColumns(), "")
}

func TestTailChain_newestSegmentWins(t *testing.T) {
	dir := t.TempDir()
	base := writeTailBase(t, dir, "orders.parquet", tailOrdersDDL,
		map[string]string{"id": "1", "status": "one"},
		map[string]string{"id": "2", "status": "two"},
		map[string]string{"id": "3", "status": "three"},
		map[string]string{"id": "4", "status": "four"},
	)
	published := "SELECT * FROM read_parquet(" + lit(base) + ")"
	check := func(t *testing.T, c *tailChain, want ...string) {
		t.Helper()
		got := readState(t, c.sql(published))
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("chain of %d\ngot  %v\nwant %v", len(c.segs), got, want)
		}
		if ref := readState(t, c.single(t, published)); strings.Join(got, ",") != strings.Join(ref, ",") {
			t.Errorf("the chain reads %v, the same tail as one file %v", got, ref)
		}
	}

	t.Run("a key changed again in a newer segment reads its newest change", func(t *testing.T) {
		c := newTailChain(t, base)
		c.apply(t, []query.ResultRow{tailEvent(10, event.EventUpdate, "2", "first", 100)})
		c.full(t)
		c.apply(t, []query.ResultRow{tailEvent(11, event.EventUpdate, "2", "second", 110)})
		c.delta(t)
		c.apply(t, []query.ResultRow{tailEvent(12, event.EventUpdate, "2", "third", 120)})
		c.delta(t)
		check(t, c, "1=one", "2=third", "3=three", "4=four")
	})
	t.Run("a key deleted in a newer segment does not come back from an older one", func(t *testing.T) {
		c := newTailChain(t, base)
		c.apply(t, []query.ResultRow{tailEvent(10, event.EventUpdate, "3", "edited", 100), tailEvent(11, event.EventInsert, "9", "nine", 101)})
		c.full(t)
		c.apply(t, []query.ResultRow{tailEvent(12, event.EventDelete, "3", "", 110), tailEvent(13, event.EventDelete, "9", "", 111)})
		c.delta(t)
		check(t, c, "1=one", "2=two", "4=four")
	})
	t.Run("deleted, then inserted again: its last value", func(t *testing.T) {
		c := newTailChain(t, base)
		c.apply(t, []query.ResultRow{tailEvent(10, event.EventDelete, "1", "", 100)})
		c.full(t)
		c.apply(t, []query.ResultRow{tailEvent(11, event.EventInsert, "1", "back", 110)})
		c.delta(t)
		check(t, c, "1=back", "2=two", "3=three", "4=four")
	})
	t.Run("a key only an older segment carries keeps its change there", func(t *testing.T) {
		c := newTailChain(t, base)
		c.apply(t, []query.ResultRow{tailEvent(10, event.EventDelete, "4", "", 100), tailEvent(11, event.EventUpdate, "1", "kept", 101)})
		c.full(t)
		c.apply(t, []query.ResultRow{tailEvent(12, event.EventUpdate, "2", "newer", 110)})
		c.delta(t)
		c.delta(t) // nothing changed: an empty segment
		check(t, c, "1=kept", "2=newer", "3=three")
	})
	t.Run("an empty first file and deltas over it", func(t *testing.T) {
		c := newTailChain(t, base)
		c.full(t)
		c.apply(t, []query.ResultRow{tailEvent(10, event.EventInsert, "7", "seven", 100)})
		c.delta(t)
		check(t, c, "1=one", "2=two", "3=three", "4=four", "7=seven")
	})
	t.Run("the chain has the published columns and types", func(t *testing.T) {
		c := newTailChain(t, base)
		c.apply(t, []query.ResultRow{tailEvent(10, event.EventInsert, "9", "nine", 100)})
		c.full(t)
		c.apply(t, []query.ResultRow{tailEvent(11, event.EventUpdate, "9", "ten", 110)})
		c.delta(t)
		if a, b := describe(t, published), describe(t, c.sql(published)); a != b {
			t.Errorf("published %s\nchain %s", a, b)
		}
	})
	t.Run("the published casts apply to every segment's rows", func(t *testing.T) {
		c := newTailChain(t, base)
		c.apply(t, []query.ResultRow{tailEvent(10, event.EventInsert, "8", "eight", 100)})
		c.full(t)
		c.apply(t, []query.ResultRow{tailEvent(11, event.EventInsert, "9", "nine", 110)})
		c.delta(t)
		segs := []string{lit(c.segs[0]), lit(c.segs[1])}
		got := readState(t, baseline.TableTailChainStateSQL(published, segs, c.tail.PKColumns(), "upper(status) AS status"))
		want := "1=ONE,2=TWO,3=THREE,4=FOUR,8=EIGHT,9=NINE"
		if strings.Join(got, ",") != want {
			t.Errorf("got %v, want %s", got, want)
		}
	})
}

func TestTailChain_compositeIntegerKey(t *testing.T) {
	const ddl = "CREATE TABLE `lines` (\n  `o` int NOT NULL,\n  `n` smallint NOT NULL,\n  `status` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`o`,`n`)\n);\n"
	pk := []metadata.ColumnMeta{{Name: "o", IsPK: true, DataType: "int"}, {Name: "n", IsPK: true, DataType: "smallint"}}
	dir := t.TempDir()
	base := writeTailBase(t, dir, "lines.parquet", ddl,
		map[string]string{"o": "1", "n": "1", "status": "a"},
		map[string]string{"o": "1", "n": "2", "status": "b"},
		map[string]string{"o": "-1", "n": "1", "status": "c"},
	)
	tail, err := NewTail("shop", "lines", ddl, base, pk)
	if err != nil {
		t.Fatal(err)
	}
	ev := func(id uint64, typ event.EventType, o, n, status string, pos uint64) query.ResultRow {
		r := query.ResultRow{EventID: id, BinlogFile: "binlog.000003", StartPos: pos, EndPos: pos + 10,
			SchemaName: "shop", TableName: "lines", EventType: typ, PKValues: o + "|" + n}
		img := map[string]any{"o": json.Number(o), "n": json.Number(n), "status": status}
		if typ == event.EventDelete {
			r.RowBefore = img
		} else {
			r.RowAfter = img
		}
		return r
	}
	d := t.TempDir()
	if err := tail.Apply([]query.ResultRow{ev(1, event.EventUpdate, "1", "2", "b1", 100)}); err != nil {
		t.Fatal(err)
	}
	s0 := filepath.Join(d, "0.parquet")
	if _, err := tail.WriteFile(s0); err != nil {
		t.Fatal(err)
	}
	if err := tail.Apply([]query.ResultRow{ev(2, event.EventDelete, "1", "2", "", 110), ev(3, event.EventUpdate, "-1", "1", "c1", 111)}); err != nil {
		t.Fatal(err)
	}
	s1 := filepath.Join(d, "1.parquet")
	if _, err := tail.WriteDelta(s1); err != nil {
		t.Fatal(err)
	}
	published := "SELECT * FROM read_parquet(" + lit(base) + ")"
	q := baseline.TableTailChainStateSQL(published, []string{lit(s0), lit(s1)}, tail.PKColumns(), "")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rs, err := db.Query("SELECT o || '/' || n || '=' || status FROM (" + q + ") ORDER BY o, n")
	if err != nil {
		t.Fatalf("%v\n%s", err, q)
	}
	defer rs.Close()
	var got []string
	for rs.Next() {
		var s string
		if err := rs.Scan(&s); err != nil {
			t.Fatal(err)
		}
		got = append(got, s)
	}
	if want := "-1/1=c1,1/1=a"; strings.Join(got, ",") != want {
		t.Errorf("got %v, want %s", got, want)
	}
}

// A cycle writes only the keys that changed since the last segment: the
// point of the chain.
func TestTail_writeDeltaWritesOnlyChangedKeys(t *testing.T) {
	base := writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL)
	c := newTailChain(t, base)
	var page []query.ResultRow
	for i := range 100 {
		page = append(page, tailEvent(uint64(i+1), event.EventInsert, fmt.Sprint(i+1), "x", uint64(100+i*20)))
	}
	c.apply(t, page)
	if got := c.tail.Dirty(); got != 100 {
		t.Fatalf("Dirty after 100 inserts = %d", got)
	}
	if n := c.full(t); n != 100 {
		t.Fatalf("the first file has %d rows, want 100", n)
	}
	if got := c.tail.Dirty(); got != 0 {
		t.Fatalf("Dirty after WriteFile = %d, want 0", got)
	}
	c.apply(t, []query.ResultRow{
		tailEvent(200, event.EventUpdate, "5", "y", 5000),
		tailEvent(201, event.EventUpdate, "5", "z", 5010),
		tailEvent(202, event.EventDelete, "6", "", 5020),
	})
	if n := c.delta(t); n != 2 {
		t.Errorf("the delta has %d rows, want 2 (keys 5 and 6)", n)
	}
	if n := c.delta(t); n != 0 {
		t.Errorf("a delta with nothing changed has %d rows", n)
	}
}

// A delta that fails keeps its keys as changed: the next one carries them.
func TestTail_aFailedDeltaKeepsItsKeys(t *testing.T) {
	dir := t.TempDir()
	base := writeTailBase(t, dir, "orders.parquet", tailOrdersDDL, map[string]string{"id": "1", "status": "one"})
	c := newTailChain(t, base)
	c.full(t)
	c.apply(t, []query.ResultRow{tailEvent(10, event.EventUpdate, "1", "lost?", 100)})
	taken := c.next()
	if err := os.WriteFile(taken, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.tail.WriteDelta(taken); err == nil {
		t.Fatal("WriteDelta over an existing name succeeded")
	}
	if c.tail.Dirty() != 1 {
		t.Fatalf("Dirty after a failed delta = %d, want 1", c.tail.Dirty())
	}
	c.dir = t.TempDir() // the next segment's name is free there
	c.apply(t, []query.ResultRow{tailEvent(11, event.EventInsert, "2", "two", 110)})
	if n := c.delta(t); n != 2 {
		t.Errorf("the next delta has %d rows, want 2", n)
	}
	published := "SELECT * FROM read_parquet(" + lit(base) + ")"
	if got := strings.Join(readState(t, c.sql(published)), ","); got != "1=lost?,2=two" {
		t.Errorf("got %s", got)
	}
}

// The #843 guard refuses a delta as it refuses a whole file.
func TestTail_aDeltaRefusesADroppedColumn(t *testing.T) {
	base := writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL)
	c := newTailChain(t, base)
	c.full(t)
	ev := tailEvent(10, event.EventInsert, "1", "", 100)
	delete(ev.RowAfter, "status")
	c.apply(t, []query.ResultRow{ev})
	var refusal *TailRefusal
	if _, err := c.tail.WriteDelta(c.next()); !errors.As(err, &refusal) || !strings.Contains(err.Error(), "status") {
		t.Fatalf("WriteDelta = %v, want a refusal naming status", err)
	}
}

// After a prune the chain starts again with a whole file: an older segment
// may carry a dropped key's change from before the newer cut, which the chain
// would lay over that cut's row. A delta is refused until then, and the
// dropped keys are no longer changed keys.
func TestTail_afterAPruneOnlyAWholeFile(t *testing.T) {
	newer := writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL,
		map[string]string{"id": "1", "status": "at the newer cut"})
	c := newTailChain(t, newer)
	c.apply(t, []query.ResultRow{tailEvent(10, event.EventUpdate, "1", "older", 100)})
	c.full(t) // seg 1 carries key 1 = older
	c.apply(t, []query.ResultRow{tailEvent(11, event.EventUpdate, "1", "at the newer cut", 200), tailEvent(12, event.EventUpdate, "2", "after", 500)})
	if c.tail.Dirty() != 2 {
		t.Fatalf("Dirty = %d", c.tail.Dirty())
	}
	n, err := c.tail.PruneThrough(query.BinlogPos{File: "binlog.000003", Pos: 300}, 0)
	if err != nil || n != 1 {
		t.Fatalf("PruneThrough = %d, %v", n, err)
	}
	if c.tail.Dirty() != 1 {
		t.Fatalf("Dirty after the prune = %d, want 1", c.tail.Dirty())
	}
	if _, err := c.tail.WriteDelta(c.next()); err == nil {
		t.Fatal("a delta after a prune was written: seg 1's key 1 would hide the newer cut's row")
	}
	if n := c.full(t); n != 1 {
		t.Errorf("the whole file has %d rows, want 1", n)
	}
	c.delta(t) // the new chain takes deltas again
	published := "SELECT * FROM read_parquet(" + lit(newer) + ")"
	if got := strings.Join(readState(t, c.sql(published)), ","); got != "1=at the newer cut,2=after" {
		t.Errorf("got %s", got)
	}
}

// A prune that drops nothing still restarts the chain: which cut the
// segments stand for is the caller's to keep, and one rule is one less to get
// wrong.
func TestTail_aPruneOfNothingStillNeedsAWholeFile(t *testing.T) {
	base := writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL)
	c := newTailChain(t, base)
	c.apply(t, []query.ResultRow{tailEvent(10, event.EventUpdate, "1", "x", 500)})
	c.full(t)
	if n, err := c.tail.PruneThrough(query.BinlogPos{File: "binlog.000003", Pos: 300}, 0); err != nil || n != 0 {
		t.Fatalf("PruneThrough = %d, %v", n, err)
	}
	if _, err := c.tail.WriteDelta(c.next()); err == nil {
		t.Fatal("a delta after a prune was written")
	}
}

func TestTableTailChainStateSQL_shortChains(t *testing.T) {
	pk := []string{"id"}
	if got, want := baseline.TableTailChainStateSQL("S", []string{"'a'"}, pk, "x AS x"), baseline.TableTailStateSQL("S", "'a'", pk, "x AS x"); got != want {
		t.Errorf("one segment:\n%s\nwant\n%s", got, want)
	}
	base := writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL, map[string]string{"id": "1", "status": "one"})
	published := "SELECT * FROM read_parquet(" + lit(base) + ")"
	if got := strings.Join(readState(t, baseline.TableTailChainStateSQL(published, nil, pk, "upper(status) AS status")), ","); got != "1=ONE" {
		t.Errorf("no segment: %s", got)
	}
}

// Random histories: inserts, updates and deletes over a few keys, pages
// sometimes out of binary log order, a delta after each cycle and now and
// then a whole file that starts the chain again. After every cycle the chain
// reads as the whole tail written as one file.
func TestTailChain_randomHistories(t *testing.T) {
	var baseRows []map[string]string
	for i := 1; i <= 6; i++ {
		baseRows = append(baseRows, map[string]string{"id": fmt.Sprint(i), "status": fmt.Sprintf("base%d", i)})
	}
	base := writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL, baseRows...)
	published := "SELECT * FROM read_parquet(" + lit(base) + ")"
	for seed := int64(1); seed <= 12; seed++ {
		r := rand.New(rand.NewSource(seed))
		c := newTailChain(t, base)
		c.full(t)
		id, pos := uint64(0), uint64(1000)
		for cycle := range 8 {
			var page []query.ResultRow
			for range r.Intn(6) {
				id++
				pos += 20
				key := fmt.Sprint(1 + r.Intn(10))
				at := pos
				if r.Intn(5) == 0 && pos > 1100 {
					at = pos - 90 // arrives after a later change of its row
				}
				typ := []event.EventType{event.EventInsert, event.EventUpdate, event.EventDelete}[r.Intn(3)]
				page = append(page, tailEvent(id, typ, key, fmt.Sprintf("s%d-c%d-e%d", seed, cycle, id), at))
			}
			c.apply(t, page)
			if r.Intn(6) == 0 {
				c.full(t)
			} else {
				c.delta(t)
			}
			got := readState(t, c.sql(published))
			want := readState(t, c.single(t, published))
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("seed %d cycle %d, %d segments:\nchain %v\nwhole %v", seed, cycle, len(c.segs), got, want)
			}
		}
	}
}
