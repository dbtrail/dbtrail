package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// #3082: when a chain is over, by the MySQL chain's rules. Each rule alone,
// at its edge.
func TestPGChainOver(t *testing.T) {
	day := 24 * time.Hour
	cases := []struct {
		name                  string
		lastSeq               int
		chainBytes, baseBytes int64
		age                   time.Duration
		over                  bool
	}{
		{"young and small", 3, 2 << 20, 100 << 20, time.Hour, false},
		{"a day old", 3, 2 << 20, 100 << 20, day, false},
		{"older than a day", 3, 2 << 20, 100 << 20, day + time.Minute, true},
		{"the last sequence a chain can reach", pgdelta.MaxSeq - 1, 2 << 20, 100 << 20, time.Hour, true},
		{"half of the file", 3, 50 << 20, 100 << 20, time.Hour, false},
		{"past half of the file", 3, 50<<20 + 1, 100 << 20, time.Hour, true},
		{"past half of a small file, under 1 MiB", 3, 1<<20 - 1, 1 << 10, time.Hour, false},
		{"past half of a small file, at 1 MiB", 3, 1 << 20, 1 << 10, time.Hour, true},
		// No rule on the number of pairs, as for MySQL: 288 is a day at
		// five minutes.
		{"288 pairs, small", 287, 2 << 20, 100 << 20, 23 * time.Hour, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			why := pgChainOver(c.lastSeq, c.chainBytes, c.baseBytes, c.age)
			if (why != "") != c.over {
				t.Fatalf("pgChainOver = %q, want over %v", why, c.over)
			}
		})
	}
}

// The PostgreSQL chain ends by the MySQL chain's rules, with its own
// variables: this holds the two equal until someone decides otherwise, and
// says so here.
func TestPGChainRulesAreTheMySQLChains(t *testing.T) {
	if pgChainMaxAge != tableDeltaMaxAge || pgChainMaxFraction != tableDeltaMaxFraction || pgChainMinCompactBytes != tableDeltaMinCompactBytes {
		t.Fatalf("PostgreSQL chain: %s, %v, %d bytes; MySQL chain: %s, %v, %d bytes",
			pgChainMaxAge, pgChainMaxFraction, pgChainMinCompactBytes, tableDeltaMaxAge, tableDeltaMaxFraction, tableDeltaMinCompactBytes)
	}
	if pgdelta.MaxSeq != baseline.TableDeltaMaxSeq {
		t.Fatalf("the last sequence: PostgreSQL %d, MySQL %d", pgdelta.MaxSeq, baseline.TableDeltaMaxSeq)
	}
}

// writeChainBase writes a PostgreSQL table file of (id, status).
func writeChainBase(t *testing.T, path string, rows ...[2]string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, []baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow([]string{r[0], r[1]}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// writeChainPair writes pair seq beside base: each change is a key and its
// new status, "" for a deleted key.
func writeChainPair(t *testing.T, duck *sql.DB, base string, seq int, changes map[string]string) (dead, rows string) {
	t.Helper()
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	at, err := pgdelta.BasePositionsOf(context.Background(), duck, base, []string{"id"}, keys)
	if err != nil {
		t.Fatal(err)
	}
	prevRun := ""
	if seq > 0 {
		prevRun = fmt.Sprintf("run-%d", seq-1)
	}
	dead, rows, _, err = pgdelta.WritePair(pgdelta.PairInput{
		BasePath: base, Seq: seq, Base: "base-1", Run: fmt.Sprintf("run-%d", seq), PrevRun: prevRun,
		Columns: []string{"id", "status"}, KeyColumns: []string{"id"}, Positions: at,
		Changes: func(yield func(pgdelta.Change) error) error {
			for _, k := range keys {
				c := pgdelta.Change{Key: k, Delete: changes[k] == ""}
				if !c.Delete {
					c.Values, c.Nulls = []string{k, changes[k]}, []bool{false, false}
				}
				if err := yield(c); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("WritePair %d: %v", seq, err)
	}
	return dead, rows
}

// A chain is folded into one file by the engine and held to its count: a
// key in several pairs is the newest of them once, a row two pairs take
// away is taken away once, a key deleted and put back is there. A pair that
// holds one key twice leaves no file.
func TestPGFoldChainIntoFile(t *testing.T) {
	ctx := context.Background()
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	dir := t.TempDir()
	base := filepath.Join(dir, "public", "orders.parquet")
	writeChainBase(t, base, [2]string{"1", "a"}, [2]string{"2", "b"}, [2]string{"3", "c"}, [2]string{"4", "d"})
	var dead, rows []string
	for seq, w := range []map[string]string{
		{"1": "a1", "2": "", "9": "nine"},
		{"1": "a2", "9": ""},
		{"2": "back", "1": "a3", "3": "c1"},
	} {
		d, r := writeChainPair(t, duck, base, seq, w)
		dead, rows = append(dead, d), append(rows, r)
	}
	dst := filepath.Join(dir, "next", "public", "orders.parquet")
	folded, total, err := pgFoldPairIntoFile(ctx, duck, base, dead, rows, dst, map[string]string{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	rs, err := duck.Query("SELECT id, status FROM read_parquet('" + folded + "')")
	if err != nil {
		t.Fatal(err)
	}
	for rs.Next() {
		var id, st string
		if err := rs.Scan(&id, &st); err != nil {
			t.Fatal(err)
		}
		got[id] = st
	}
	rs.Close()
	want := map[string]string{"1": "a3", "2": "back", "3": "c1", "4": "d"}
	if total != 4 || len(got) != 4 {
		t.Fatalf("folded %d rows: %v; want %v", total, got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("folded %v; want %v", got, want)
		}
	}
	os.Remove(folded)

	// A .pgnew that holds a key twice: the count would add up with the row
	// there twice, and it is refused.
	twice := filepath.Join(dir, "public", "orders.000003.pgnew")
	cols := []baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true},
		{Name: pgdelta.KeyColumn, RawText: true}, {Name: pgdelta.OpColumn, RawText: true}, {Name: pgdelta.SeqColumn, RawText: true}}
	w, err := baseline.NewWriter(twice, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range []string{"x", "y"} {
		if err := w.WriteRow([]string{"7", st, "7", pgdelta.OpPut, "000003"}, make([]bool, 5)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := pgFoldPairIntoFile(ctx, duck, base, dead, append(rows, twice), dst, nil); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("a pair holding a key twice: %v; want a refusal", err)
	}
}

// The files of a chain are checked against the snapshot's record of them: a
// pair the record names that is not there (the last pair lost, which the
// names alone cannot tell) and a pair there that the record does not name
// are both refused; a snapshot with no record says nothing.
func TestPGChainAgainstManifest(t *testing.T) {
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	snap := filepath.Join(t.TempDir(), "2026-05-01T12-00-00Z")
	base := filepath.Join(snap, "public", "orders.parquet")
	writeChainBase(t, base, [2]string{"1", "a"}, [2]string{"2", "b"})
	// Another table's, whose name starts with this one's.
	writeChainBase(t, filepath.Join(snap, "public", "orders_old.parquet"), [2]string{"1", "a"})
	writeChainPair(t, duck, filepath.Join(snap, "public", "orders_old.parquet"), 0, map[string]string{"1": "z"})
	// chainIn is the chain the folder's names give, the record left aside.
	chainIn := func(dir string) pgdelta.Chain {
		t.Helper()
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		c, err := pgdelta.ChainOf("orders", names)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	chainOf := func() pgdelta.Chain { return chainIn(filepath.Dir(base)) }
	for seq := range 3 {
		writeChainPair(t, duck, base, seq, map[string]string{"1": fmt.Sprintf("v%d", seq)})
	}
	if why := pgChainAgainstManifest(base, chainOf()); why != "" {
		t.Fatalf("no record: %q, want nothing said", why)
	}
	if err := baselineintegrity.WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	if why := pgChainAgainstManifest(base, chainOf()); why != "" {
		t.Fatalf("the chain the record names: %q", why)
	}
	// The last pair lost: the names are a whole chain of two.
	for _, suffix := range []string{pgdelta.DeadSuffix, pgdelta.NewSuffix} {
		if err := os.Remove(filepath.Join(snap, "public", "orders.000002"+suffix)); err != nil {
			t.Fatal(err)
		}
	}
	if c := chainOf(); len(c.Pairs) != 2 {
		t.Fatalf("the names give %d pairs", len(c.Pairs))
	}
	if why := pgChainAgainstManifest(base, chainOf()); !strings.Contains(why, "000002") {
		t.Fatalf("the last pair lost: %q; want it named", why)
	}
	// And whatever carries or continues the table refuses it.
	if _, err := pgPairBeside(base); !errors.Is(err, pgdelta.ErrBrokenChain) {
		t.Fatalf("pgPairBeside over a chain short of its record: %v; want ErrBrokenChain", err)
	}
	// Every pair lost, pair 0 too: no file says there was a chain, the
	// record does.
	for seq := range 2 {
		for _, suffix := range []string{pgdelta.DeadSuffix, pgdelta.NewSuffix} {
			if err := os.Remove(filepath.Join(snap, "public", fmt.Sprintf("orders.%06d%s", seq, suffix))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := pgPairBeside(base); !errors.Is(err, pgdelta.ErrBrokenChain) {
		t.Fatalf("pgPairBeside with every pair lost: %v; want ErrBrokenChain", err)
	}
	// A pair the record does not name: written after the record.
	snap2 := filepath.Join(filepath.Dir(snap), "2026-05-01T13-00-00Z")
	base2 := filepath.Join(snap2, "public", "orders.parquet")
	writeChainBase(t, base2, [2]string{"1", "a"})
	writeChainPair(t, duck, base2, 0, map[string]string{"1": "x"})
	if err := baselineintegrity.WriteManifest(snap2); err != nil {
		t.Fatal(err)
	}
	writeChainPair(t, duck, base2, 1, map[string]string{"1": "y"})
	if why := pgChainAgainstManifest(base2, chainIn(filepath.Dir(base2))); !strings.Contains(why, "000001") {
		t.Fatalf("a pair the record does not name: %q; want it named", why)
	}
}
