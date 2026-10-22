package oracle

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func opsOf(seed uint64, tables []Table, n int) []Op {
	g := NewGenerator(seed, tables)
	out := make([]Op, 0, n)
	for range n {
		out = append(out, g.Next())
	}
	return out
}

func flatten(ops []Op) string {
	var b strings.Builder
	for _, op := range ops {
		fmt.Fprintf(&b, "%s %s %v %v\n%s\n", op.Kind, op.Table, op.Keys, op.Noop, strings.Join(op.SQL, "\n"))
	}
	return b.String()
}

// TestGenerator_sameSeedSameStatements: a seed names one sequence exactly,
// so a failure's reproduction command replays the same statements.
func TestGenerator_sameSeedSameStatements(t *testing.T) {
	a := flatten(opsOf(7, Tables(), 500))
	b := flatten(opsOf(7, Tables(), 500))
	if a != b {
		t.Fatal("two generators with seed 7 produced different statements")
	}
	if c := flatten(opsOf(8, Tables(), 500)); c == a {
		t.Fatal("seeds 7 and 8 produced the same statements")
	}
}

// TestGenerator_keysStayInTheSet: every key an op touches is one of the
// seed's K keys, K in [MinKeys, MaxKeys], and the hot keys are drawn more
// often than an average key.
func TestGenerator_keysStayInTheSet(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3, 11, 29} {
		g := NewGenerator(seed, Tables())
		k := g.Keys()
		if k < MinKeys || k > MaxKeys {
			t.Fatalf("seed %d: %d keys, want %d..%d", seed, k, MinKeys, MaxKeys)
		}
		hits := make([]int, k)
		for range 3000 {
			op := g.Next()
			for _, key := range op.Keys {
				if key < 0 || key >= k {
					t.Fatalf("seed %d: %s on %s touched key %d outside 0..%d", seed, op.Kind, op.Table, key, k-1)
				}
				hits[key]++
			}
		}
		total := 0
		for _, h := range hits {
			total += h
		}
		avg := float64(total) / float64(k)
		for _, hot := range g.HotKeys() {
			if float64(hits[hot]) < 1.5*avg {
				t.Fatalf("seed %d: hot key %d drawn %d times, average %.0f; want it clearly hotter", seed, hot, hits[hot], avg)
			}
		}
	}
}

// TestGenerator_modelRules replays every op over an independent model of the
// tables and checks the rules each kind promises: an insert only of a key
// that is not live, a delete/update only of a live one, a reinsert only of a
// key that was deleted (or vacated by a PK change) and is not live, a PK
// change from a live key to a free one, and a unique code never shared by
// two live rows.
func TestGenerator_modelRules(t *testing.T) {
	for _, seed := range []uint64{1, 5, 9} {
		g := NewGenerator(seed, Tables())
		live := map[string]map[int]bool{}
		gone := map[string]map[int]bool{}
		codes := map[int]string{} // t_uniq: key → code literal
		for _, tb := range Tables() {
			live[tb.Name] = map[int]bool{}
			gone[tb.Name] = map[int]bool{}
		}
		seen := map[Kind]int{}
		for i := range 6000 {
			op := g.Next()
			seen[op.Kind]++
			l, d := live[op.Table], gone[op.Table]
			for _, s := range op.Changes {
				switch s.Kind {
				case KindInsert:
					if l[s.Key] {
						t.Fatalf("seed %d op %d: insert of live key %d on %s", seed, i, s.Key, op.Table)
					}
					l[s.Key] = true
					delete(d, s.Key)
				case KindReinsert:
					if l[s.Key] || !d[s.Key] {
						t.Fatalf("seed %d op %d: reinsert of key %d on %s (live=%v deleted=%v)", seed, i, s.Key, op.Table, l[s.Key], d[s.Key])
					}
					l[s.Key] = true
					delete(d, s.Key)
				case KindDelete:
					if !l[s.Key] {
						t.Fatalf("seed %d op %d: delete of key %d on %s that is not live", seed, i, s.Key, op.Table)
					}
					delete(l, s.Key)
					d[s.Key] = true
				case KindUpdate:
					if !l[s.Key] {
						t.Fatalf("seed %d op %d: update of key %d on %s that is not live", seed, i, s.Key, op.Table)
					}
				case KindPKUpdate:
					if !l[s.Key] || l[s.NewKey] {
						t.Fatalf("seed %d op %d: pk change %d→%d on %s (live %v → %v)", seed, i, s.Key, s.NewKey, op.Table, l[s.Key], l[s.NewKey])
					}
					delete(l, s.Key)
					d[s.Key] = true
					l[s.NewKey] = true
					delete(d, s.NewKey)
					if op.Table == "t_uniq" {
						codes[s.NewKey] = codes[s.Key]
						delete(codes, s.Key)
					}
				default:
					t.Fatalf("seed %d op %d: unexpected step kind %q", seed, i, s.Kind)
				}
				if op.Table == "t_uniq" && (s.Kind == KindInsert || s.Kind == KindReinsert) {
					codes[s.Key] = s.Code
				}
				if op.Table == "t_uniq" && (s.Kind == KindDelete) {
					delete(codes, s.Key)
				}
			}
			if op.Table == "t_uniq" {
				byCode := map[string]int{}
				for key := range l {
					c := codes[key]
					if c == "" {
						t.Fatalf("seed %d op %d: live t_uniq key %d has no code in the replay", seed, i, key)
					}
					if other, dup := byCode[c]; dup {
						t.Fatalf("seed %d op %d: code %s held by live keys %d and %d", seed, i, c, other, key)
					}
					byCode[c] = key
				}
			}
		}
		for _, k := range []Kind{KindUpdate, KindInsert, KindDelete, KindReinsert, KindPKUpdate, KindMulti, KindTxn, KindBurst} {
			if seen[k] == 0 {
				t.Fatalf("seed %d: kind %s never drawn in 6000 ops", seed, k)
			}
		}
	}
}

// TestGenerator_weights: over many draws the share of each kind follows the
// issue's weights, within the slack the fallbacks (an op that cannot apply to
// an empty table becomes an insert) leave.
func TestGenerator_weights(t *testing.T) {
	const n = 20000
	g := NewGenerator(3, Tables())
	count := map[Kind]int{}
	for range n {
		count[g.Next().Kind]++
	}
	total := 0
	for _, w := range Weights {
		total += w.Weight
	}
	for _, w := range Weights {
		want := float64(w.Weight) / float64(total)
		got := float64(count[w.Kind]) / n
		if got < want-0.05 || got > want+0.05 {
			t.Errorf("%s: %.3f of ops, weight says %.3f", w.Kind, got, want)
		}
	}
}

// TestGenerator_shapes: multi-row ops are ONE statement over 2..5 keys, a
// transaction holds 2..4 statements, a burst MinBurst..MaxBurst, and a
// no-op update sets the row to what it already holds.
func TestGenerator_shapes(t *testing.T) {
	g := NewGenerator(4, Tables())
	var noops, updates int
	for range 5000 {
		op := g.Next()
		switch op.Kind {
		case KindMulti:
			if len(op.SQL) != 1 || len(op.Keys) < 2 || len(op.Keys) > 5 {
				t.Fatalf("multi: %d statements over keys %v; want one statement over 2..5 keys\n%s", len(op.SQL), op.Keys, op.SQL)
			}
		case KindTxn:
			if len(op.SQL) < 2 || len(op.SQL) > 4 {
				t.Fatalf("txn: %d statements, want 2..4", len(op.SQL))
			}
		case KindBurst:
			if len(op.SQL) < MinBurst || len(op.SQL) > MaxBurst {
				t.Fatalf("burst: %d statements, want %d..%d", len(op.SQL), MinBurst, MaxBurst)
			}
		case KindUpdate:
			updates++
			if op.Noop {
				noops++
				if len(op.SQL) != 1 || !strings.HasPrefix(op.SQL[0], "UPDATE ") {
					t.Fatalf("no-op update is not one UPDATE: %v", op.SQL)
				}
			}
		}
	}
	if noops == 0 || noops*3 > updates {
		t.Fatalf("%d no-op updates of %d; want some, and a minority", noops, updates)
	}
}

// TestGenerator_noopUpdateKeepsTheRow: the SET of a no-op update repeats the
// value the generator's model holds, so MySQL changes nothing and writes no
// row event — the case that makes the barrier a position, not a count.
func TestGenerator_noopUpdateKeepsTheRow(t *testing.T) {
	g := NewGenerator(6, []Table{TableByName("t_int")})
	for range 3000 {
		op := g.Next()
		if op.Kind != KindUpdate || !op.Noop {
			continue
		}
		row := g.Row("t_int", op.Keys[0])
		if row == nil {
			t.Fatalf("no-op update of key %d but the model has no row", op.Keys[0])
		}
		want := fmt.Sprintf("UPDATE `t_int` SET `v` = %s, `note` = %s WHERE `id` = %s", row[1], row[2], row[0])
		if op.SQL[0] != want {
			t.Fatalf("no-op update\n got %s\nwant %s", op.SQL[0], want)
		}
		return
	}
	t.Fatal("no no-op update in 3000 ops")
}

// TestGenerator_literals: the SQL the five tables produce has the shapes
// MySQL accepts, generated and checked here with real draws rather than
// trusted from the renderer.
func TestGenerator_literals(t *testing.T) {
	g := NewGenerator(12, Tables())
	dec := regexp.MustCompile(`^-?[0-9]{1,9}\.[0-9]{2}$`)
	dt := regexp.MustCompile(`^'20[2-3][0-9]-[01][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]'$`)
	seenComp, seenNullDec, seenDec, seenDT := false, false, false, false
	for range 4000 {
		op := g.Next()
		for _, s := range op.SQL {
			if strings.Contains(s, "''") && !strings.Contains(s, "= ''") {
				t.Fatalf("empty quoted literal in %s", s)
			}
		}
		switch op.Table {
		case "t_comp":
			if op.Kind == KindMulti && strings.Contains(op.SQL[0], " IN (") {
				if !strings.Contains(op.SQL[0], "WHERE (`a`, `b`) IN ((") {
					t.Fatalf("composite multi-row WHERE: %s", op.SQL[0])
				}
				seenComp = true
			}
		case "t_types":
			for _, key := range op.Keys {
				row := g.Row("t_types", key)
				if row == nil {
					continue
				}
				switch {
				case row[2] == "NULL":
					seenNullDec = true
				case dec.MatchString(row[2]):
					seenDec = true
				default:
					t.Fatalf("t_types amount literal %q", row[2])
				}
				if row[3] != "NULL" && !dt.MatchString(row[3]) {
					t.Fatalf("t_types seen literal %q", row[3])
				}
				if row[3] != "NULL" {
					seenDT = true
				}
			}
		}
	}
	if !seenComp || !seenNullDec || !seenDec || !seenDT {
		t.Fatalf("shapes not all seen: comp=%v nulldec=%v dec=%v dt=%v", seenComp, seenNullDec, seenDec, seenDT)
	}
}

// TestGenerator_fallbacksWithoutLiveRows: on an empty table every op that
// needs a live row becomes an insert, and once every key is live an insert
// becomes a delete, without looping.
func TestGenerator_fallbacksWithoutLiveRows(t *testing.T) {
	g := NewGenerator(2, []Table{TableByName("t_int")})
	// Compound ops start with an insert too: the check is on the first CHANGE.
	first := g.Next()
	if first.Changes[0].Kind != KindInsert {
		t.Fatalf("first op on an empty table starts with %s, want an insert", first.Changes[0].Kind)
	}
	g = NewGenerator(2, []Table{TableByName("t_int")})
	g.fillAll("t_int")
	for range 50 {
		op := g.Next()
		for _, s := range op.Changes {
			if s.Kind == KindInsert {
				t.Fatalf("insert with every key live: %v", op.SQL)
			}
			if s.Kind == KindDelete || s.Kind == KindPKUpdate {
				return
			}
		}
	}
	t.Fatal("50 ops over a full table and none freed a key")
}

// TestGenerator_subsetOfTables: a generator over one table never names
// another.
func TestGenerator_subsetOfTables(t *testing.T) {
	for _, op := range opsOf(1, []Table{TableByName("t_text")}, 200) {
		if op.Table != "t_text" {
			t.Fatalf("op on %s with only t_text configured", op.Table)
		}
	}
}
