package views

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// writePGChainPair writes pair seq beside base, after pair seq-1: each
// change is a key and its new status, "" for a deleted key.
func writePGChainPair(t *testing.T, base string, seq int, changes map[string]string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	at, err := pgdelta.BasePositionsOf(context.Background(), db, base, []string{"id"}, keys)
	if err != nil {
		t.Fatal(err)
	}
	prevRun := ""
	if seq > 0 {
		prevRun = fmt.Sprintf("run-%d", seq-1)
	}
	_, _, _, err = pgdelta.WritePair(pgdelta.PairInput{
		BasePath: base, Seq: seq, Base: "base-1", Run: fmt.Sprintf("run-%d", seq), PrevRun: prevRun,
		Columns: pgPairCols, KeyColumns: []string{"id"}, Positions: at,
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
}

// #3082: a snapshot update writes one pair per window, each after the one
// before. The view reads the file and every pair: a key changed in several
// pairs reads as the newest, a key deleted and put back later is back, a key
// one pair inserts and a later one deletes is gone. A pair missing from the
// middle refuses the table when it is read.
func TestPGPairStateView_chain(t *testing.T) {
	root := t.TempDir()
	base := writePGPairBase(t, root, "orders", [2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"3", "three"}, [2]string{"10", "ten"})
	tbl := pgPairTable(base, "orders")
	writePGChainPair(t, base, 0, map[string]string{"1": "ONE", "2": "", "7": "seven"})
	writePGChainPair(t, base, 1, map[string]string{"1": "uno", "2": "back", "7": ""})
	writePGChainPair(t, base, 2, map[string]string{"1": "eins", "9": "nine"})

	text := pgPairGenerate(root, FollowNone, tbl)
	for seq := range 3 {
		if !strings.Contains(text, fmt.Sprintf("orders.%06d.pgnew", seq)) {
			t.Fatalf("the view does not name pair %d:\n%s", seq, text)
		}
	}
	got, err := pgPairRows(t, execViews(t, text), "orders")
	if want := []string{"1=eins", "2=back", "3=three", "9=nine", "10=ten"}; err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("through the chain: %v, %v; want %v\n%s", got, err, want, text)
	}

	dead, rows, err := pgdelta.Paths(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{dead, rows} {
		if err := os.Remove(f); err != nil {
			t.Fatal(err)
		}
	}
	broken := pgPairGenerate(root, FollowNone, tbl)
	if _, err := pgPairRows(t, execViews(t, broken), "orders"); err == nil || !strings.Contains(err.Error(), "cannot be read whole") {
		t.Fatalf("a chain with a pair missing in the middle: %v; want the refusal", err)
	}
}
