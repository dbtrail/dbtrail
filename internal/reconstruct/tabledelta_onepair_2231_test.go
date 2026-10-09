package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2231: a chain that is one pair is read without the aggregate and join that
// choose the newest version of each key (baseline.TableDeltaOnePairStateSQL).
// That is only the same state if a pair holds each key once, and if a chain
// merged into one range pair keeps the state the chain had. Both are held
// here: random chains read three ways against a model, and every writer of a
// pair checked for a repeated key.

// onePairSessions are the collations a state is read under: DuckDB's own, and
// the two the console's SQL sandbox has used, which fold case and accents.
var onePairSessions = []string{"", "SET default_collation = 'nocase.noaccent'", "SET default_collation = 'nocase.icu_noaccent'"}

// oddKeys are keys as pk_values spells them, the ones a comparison could get
// wrong: two that differ only in case, two only in an accent, an escaped pipe,
// an escaped backslash, text that reads as a hex escape and the byte it would
// decode to, and the empty key's nearest neighbours.
var oddKeys = []string{"abc", "ABC", "nandu", "ñandú", `p\|q`, `p\|Q`, `C:\\tmp`, `\x41`, "A", " ", "a b", "a  b"}

type randomChain struct {
	base  string
	pairs int
	// want is the state the chain describes, as stateLines prints it.
	want []string
}

// writeRandomChain writes a base and a chain of pairs beside it from seed, and
// computes the state they describe with a map: the last write of a key wins,
// a delete removes it. Every pair holds a key at most once, and kills the base
// row of every base key it touches, as the real writer does for a window
// (lookupBasePositions), so a position can repeat from one pair to the next.
func writeRandomChain(t *testing.T, seed uint64) randomChain {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 2231))
	// Outside ASCII and with a quote: the paths are SQL literals.
	dir := filepath.Join(t.TempDir(), "josé's", "2026-10-09T00-00-00Z", "tiënda")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "tokens.parquet")
	cols := []baseline.Column{
		{Name: "tok", MySQLType: "varchar(16)", ParquetType: parquet.String()},
		{Name: "n", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyBinlogFile: "b.1", baseline.MetaKeyBinlogPos: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	state, position := map[string]int{}, map[string]int64{}
	pool := slices.Clone(oddKeys)
	// Some odd keys start in the base, so a key that needs care is also one
	// whose base row has to die.
	baseRows := 1 + rng.IntN(12)
	var baseKeys []string
	for i := range baseRows {
		k := fmt.Sprintf("b%02d", i)
		if i < len(oddKeys) && rng.IntN(3) == 0 {
			k = oddKeys[i]
		} else {
			pool = append(pool, k)
		}
		baseKeys = append(baseKeys, k)
	}
	for i, k := range baseKeys {
		if err := w.WriteRow([]string{k, fmt.Sprint(i)}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
		state[k], position[k] = i, int64(i)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	upsCols, err := baseline.TableDeltaColumns(cols)
	if err != nil {
		t.Fatal(err)
	}
	idx := map[string]int{}
	for i, c := range upsCols {
		idx[c.Name] = i
	}
	pairs := 1 + rng.IntN(6)
	n := 100
	for seq := range pairs {
		type change struct {
			key string
			del bool
			n   int
		}
		var changes []change
		var dead []int64
		// The pair that starts a chain after a rewrite is empty; half the
		// chains start that way.
		if !(seq == 0 && rng.IntN(2) == 0) {
			for _, k := range pool {
				if rng.IntN(100) >= 35 {
					continue
				}
				n++
				c := change{key: k, del: rng.IntN(100) < 30, n: n}
				changes = append(changes, c)
				if pos, ok := position[k]; ok {
					dead = append(dead, pos)
				}
				if c.del {
					delete(state, k)
				} else {
					state[k] = c.n
				}
			}
		}
		sort.Slice(dead, func(i, j int) bool { return dead[i] < dead[j] })
		md := map[string]string{
			baseline.MetaKeyBinlogFile: "b.1", baseline.MetaKeyBinlogPos: fmt.Sprint(seq + 2),
			baseline.MetaKeyDeltaChainStart: "2026-10-09T00:00:00Z",
			baseline.MetaKeyDeltaBaseAnchor: "b.1:1", baseline.MetaKeyDeltaBaseSize: "1",
		}
		err := baseline.WriteTableDeltaPair(base, seq, cols, md, dead, func(emit func([]string, []bool) error) error {
			for _, c := range changes {
				r, nulls := make([]string, len(upsCols)), make([]bool, len(upsCols))
				r[idx[baseline.TableDeltaPKColumn]] = c.key
				if c.del {
					r[idx[baseline.TableDeltaOpColumn]] = baseline.TableDeltaOpDelete
					nulls[idx["tok"]], nulls[idx["n"]] = true, true
				} else {
					r[idx[baseline.TableDeltaOpColumn]] = baseline.TableDeltaOpUpsert
					r[idx["tok"]], r[idx["n"]] = c.key, fmt.Sprint(c.n)
				}
				if err := emit(r, nulls); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("seed %d: write pair %d: %v", seed, seq, err)
		}
	}
	var want []string
	for k, v := range state {
		want = append(want, fmt.Sprintf("n=%d tok=%s", v, k))
	}
	sort.Strings(want)
	return randomChain{base: base, pairs: pairs, want: want}
}

func sqlLit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// stateLines runs a state SQL in a new DuckDB session and prints every row as
// "n=<n> tok=<tok>", sorted as bytes.
func stateLines(t *testing.T, session, stateSQL string) []string {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	if session != "" {
		if _, err := ddb.Exec(session); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := ddb.Query("SELECT tok, n FROM (" + stateSQL + ")")
	if err != nil {
		t.Fatalf("session %q: %v\n%s", session, err, stateSQL)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var tok sql.NullString
		var n sql.NullInt64
		if err := rows.Scan(&tok, &n); err != nil {
			t.Fatal(err)
		}
		if !tok.Valid || !n.Valid {
			out = append(out, fmt.Sprintf("NULL ROW tok=%v n=%v", tok, n))
			continue
		}
		out = append(out, fmt.Sprintf("n=%d tok=%s", n.Int64, tok.String))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

// chainStateSQL is the state of base's chain through the join, every file
// named, as a pinned view names them.
func chainStateSQL(base string, chain *baseline.TableDeltaChain) string {
	var posdels, upserts []string
	for _, f := range chain.Files {
		posdels, upserts = append(posdels, sqlLit(f.Posdel)), append(upserts, sqlLit(f.Upserts))
	}
	return baseline.TableDeltaStateSQL(sqlLit(base), "["+strings.Join(posdels, ", ")+"]", "["+strings.Join(upserts, ", ")+"]", base, "")
}

// TestOnePairState_randomChains: the state of a chain is the same read three
// ways, and each is the state the model computes: through the join over the
// chain as written; through the join over the chain merged into one range
// pair (a view generated before the merge, or one that follows); and through
// the one-pair SQL over that pair. A chain of one pair is read as it is.
func TestOnePairState_randomChains(t *testing.T) {
	ctx := context.Background()
	sawOne, sawMerged := false, false
	for seed := uint64(1); seed <= 60; seed++ {
		c := writeRandomChain(t, seed)
		chain, err := baseline.ListTableDelta(ctx, c.base)
		if err != nil || chain == nil || len(chain.Files) != c.pairs {
			t.Fatalf("seed %d: chain = %v (err=%v), want %d pairs", seed, chain, err, c.pairs)
		}
		one := chain.Files[0]
		if c.pairs > 1 {
			// Beside a copy of the base, as a refresh installs a range: the
			// name filter of the join reads the stem from the base's path.
			outDir := filepath.Join(t.TempDir(), "merged")
			mc, err := CompactTableDeltaMinor(ctx, c.base, chain, 0, chain.Last().Seq, outDir)
			if err != nil {
				t.Fatalf("seed %d: merge the chain into one pair: %v", seed, err)
			}
			one = baseline.TableDeltaFile{SeqLo: mc.Lo, Seq: mc.Hi, Posdel: mc.Posdel, Upserts: mc.Upserts}
			sawMerged = true
		} else {
			sawOne = true
		}
		merged := &baseline.TableDeltaChain{Files: []baseline.TableDeltaFile{one}}
		reads := []struct{ name, sql string }{
			{"the join over the chain", chainStateSQL(c.base, chain)},
			{"the join over the one pair", chainStateSQL(c.base, merged)},
			{"the one-pair SQL", baseline.TableDeltaOnePairStateSQL(sqlLit(c.base), sqlLit(one.Posdel), sqlLit(one.Upserts), "")},
		}
		for _, session := range onePairSessions {
			for _, r := range reads {
				if got := stateLines(t, session, r.sql); !reflect.DeepEqual(got, c.want) {
					t.Fatalf("seed %d (%d pairs), session %q, %s:\n got  %v\n want %v", seed, c.pairs, session, r.name, got, c.want)
				}
			}
		}
	}
	// A generator that stopped producing one of the shapes would leave the
	// loop green and that shape unread.
	if !sawOne || !sawMerged {
		t.Fatalf("the seeds produced no chain of one pair (%v) or none of several (%v)", sawOne, sawMerged)
	}
}

// TestOnePairState_overTwoPairsIsNotTheState is why the one-pair SQL is for
// one pair: pointed at the newest pair of a chain of two, it loses what the
// first pair did, so a caller that picked it for a longer chain would be
// caught by the test above only if this difference exists.
func TestOnePairState_overTwoPairsIsNotTheState(t *testing.T) {
	for seed := uint64(1); seed <= 60; seed++ {
		c := writeRandomChain(t, seed)
		if c.pairs < 2 {
			continue
		}
		chain, err := baseline.ListTableDelta(context.Background(), c.base)
		if err != nil {
			t.Fatal(err)
		}
		last := chain.Last()
		got := stateLines(t, "", baseline.TableDeltaOnePairStateSQL(sqlLit(c.base), sqlLit(last.Posdel), sqlLit(last.Upserts), ""))
		if !reflect.DeepEqual(got, c.want) {
			return
		}
	}
	t.Fatal("the newest pair alone read as the chain's state for every seed: the random chains do not tell one pair from a chain")
}

// repeatedKeys counts the keys an upserts file holds more than once, compared
// as bytes.
func repeatedKeys(t *testing.T, upserts string) int {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	var rows, keys int
	q := fmt.Sprintf(`SELECT count(*), count(DISTINCT encode("%s")) FROM read_parquet(%s)`, baseline.TableDeltaPKColumn, sqlLit(upserts))
	if err := ddb.QueryRow(q).Scan(&rows, &keys); err != nil {
		t.Fatalf("count the keys of %s: %v", upserts, err)
	}
	if rows == 0 {
		t.Fatalf("%s holds no row: nothing was checked", upserts)
	}
	return rows - keys
}

// TestTableDeltaWriters_aPairHoldsAKeyOnce: every writer of a pair writes a
// key at most once, which is what lets a chain of one pair be read without
// choosing between versions. A window written from the change map, a window
// read back from a spill in several passes, and the range pair a merge
// writes from pairs that share keys.
func TestTableDeltaWriters_aPairHoldsAKeyOnce(t *testing.T) {
	noCompaction(t)
	t.Run("a window from memory", func(t *testing.T) {
		base2, _, _ := spilledChain(t, bigWindow("m"), nil)
		_, ups := baseline.TableDeltaPaths(base2, 1)
		if n := repeatedKeys(t, ups); n != 0 {
			t.Fatalf("%d keys are written more than once in a pair written from the change map", n)
		}
	})
	t.Run("a window from a spill", func(t *testing.T) {
		window := bigWindow("s")
		base2, _, rep := spilledChain(t, window, func(t *testing.T) *changeSpill {
			// Two rows reach the spill twice: what reads back is the last write.
			return spillOf(t, 4, changeMap(upd(3, "three-early"), ins(10, "new-early")), cloneChanges(window))
		})
		if rep.DeltaSpillPasses < 2 {
			t.Fatalf("the window was read back in %d passes; the case needs several", rep.DeltaSpillPasses)
		}
		_, ups := baseline.TableDeltaPaths(base2, 1)
		if n := repeatedKeys(t, ups); n != 0 {
			t.Fatalf("%d keys are written more than once in a pair written from a spill", n)
		}
	})
	t.Run("a merge of pairs that share keys", func(t *testing.T) {
		rows, nulls := zooRows()
		src := writeZooBaseline(t, rows, nulls)
		root := t.TempDir()
		t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
		base, at := src, t0
		for i := 1; i <= 3; i++ {
			next := t0.Add(time.Duration(i) * 5 * time.Minute)
			// Every window touches id 1 and id 5, so the three pairs share
			// two keys; the last deletes one of them.
			w := changeMap(upd(1, fmt.Sprint("v", i)), ins(5, fmt.Sprint("five", i)), ins(10+i, "own"))
			if i == 3 {
				w = changeMap(upd(1, "v3"), del(5), ins(13, "own"))
			}
			var err error
			base, _, err = deltaWindow(t, root, base, at, w, next, &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * i)}, nil)
			if err != nil {
				t.Fatal(err)
			}
			at = next
		}
		chain, err := baseline.ListTableDelta(context.Background(), base)
		if err != nil || chain == nil || len(chain.Files) != 3 {
			t.Fatalf("chain = %v (err=%v), want 3 pairs", chain, err)
		}
		shared := 0
		for _, f := range chain.Files {
			shared += len(upsertRows(t, f.Upserts))
		}
		mc, err := CompactTableDeltaMinor(context.Background(), base, chain, 0, 2, filepath.Join(t.TempDir(), "merged"))
		if err != nil {
			t.Fatal(err)
		}
		merged := len(upsertRows(t, mc.Upserts))
		if merged >= shared {
			t.Fatalf("the merge wrote %d rows from pairs holding %d: the pairs shared no key, so the case checked nothing", merged, shared)
		}
		if n := repeatedKeys(t, mc.Upserts); n != 0 {
			t.Fatalf("%d keys are written more than once in a merged pair", n)
		}
	})
}
