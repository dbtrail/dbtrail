package baseline

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/parquet-go/parquet-go"
)

// The daemon's own merges (a table written again in full, a chain merged into
// a range pair) read the newest version of every key through
// TableDeltaLatestSQL, not through the window the views use: over the files
// of a 100 M row table with 15 M upserts in 24 pairs, the window failed under
// the daemon's 4 GB memory limit with a temp directory to spill to (and
// passed under 1 GB), where the join passes from 500 MB to 8 GB. Two forms of
// one rule, so this file holds them to the same answer.

type mergeFixture struct {
	base, posdels, upserts string
}

// writeMergeFixture writes a base of five rows and three pairs in which keys
// repeat across pairs, a tombstone is the newest version of one key, a key is
// deleted and inserted again, and two keys differ only in case.
func writeMergeFixture(t *testing.T) mergeFixture {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "2026-10-04T00-00-00Z", "shop")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "tokens.parquet")
	cols := []Column{
		{Name: "tok", MySQLType: "varchar(16)", ParquetType: parquet.String()},
		{Name: "n", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	w, err := NewWriter(base, cols, WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{MetaKeyBinlogFile: "b.1", MetaKeyBinlogPos: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	for i, tok := range []string{"a", "b", "c", "d", "e"} {
		if err := w.WriteRow([]string{tok, fmt.Sprint(i)}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	upsCols, err := TableDeltaColumns(cols)
	if err != nil {
		t.Fatal(err)
	}
	idx := map[string]int{}
	for i, c := range upsCols {
		idx[c.Name] = i
	}
	type ev struct {
		tok, n string
		del    bool
	}
	pair := func(seq int, dead []int64, evs ...ev) {
		t.Helper()
		md := map[string]string{MetaKeyBinlogFile: "b.1", MetaKeyBinlogPos: fmt.Sprint(seq + 2), MetaKeyDeltaBaseAnchor: "b.1:1", MetaKeyDeltaBaseSize: "1"}
		err := WriteTableDeltaPair(base, seq, cols, md, dead, func(emit func([]string, []bool) error) error {
			for _, e := range evs {
				r, nulls := make([]string, len(upsCols)), make([]bool, len(upsCols))
				r[idx[TableDeltaPKColumn]] = e.tok
				if e.del {
					r[idx[TableDeltaOpColumn]] = TableDeltaOpDelete
					nulls[idx["tok"]], nulls[idx["n"]] = true, true
				} else {
					r[idx[TableDeltaOpColumn]] = TableDeltaOpUpsert
					r[idx["tok"]], r[idx["n"]] = e.tok, e.n
				}
				if err := emit(r, nulls); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// a: updated twice. b: deleted. c: deleted, then inserted again. d: updated
	// once. e: untouched. x: inserted, then deleted. abc / ABC: two keys.
	pair(0, []int64{0, 1}, ev{tok: "a", n: "10"}, ev{tok: "b", del: true}, ev{tok: "x", n: "70"}, ev{tok: "abc", n: "1"})
	pair(1, []int64{2}, ev{tok: "a", n: "11"}, ev{tok: "c", del: true}, ev{tok: "ABC", n: "2"})
	pair(2, []int64{3}, ev{tok: "c", n: "22"}, ev{tok: "d", n: "33"}, ev{tok: "x", del: true})

	var posdels, upserts []string
	for seq := range 3 {
		p, u := TableDeltaPaths(base, seq)
		posdels, upserts = append(posdels, "'"+p+"'"), append(upserts, "'"+u+"'")
	}
	return mergeFixture{base: base, posdels: "[" + strings.Join(posdels, ", ") + "]", upserts: "[" + strings.Join(upserts, ", ") + "]"}
}

func queryRows(t *testing.T, session, q string) []string {
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
	rows, err := ddb.Query(q)
	if err != nil {
		t.Fatalf("session %q: %v\n%s", session, err, q)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals, ptrs := make([]any, len(cols)), make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			parts[i] = fmt.Sprintf("%s=%v", cols[i], v)
		}
		sort.Strings(parts)
		out = append(out, strings.Join(parts, " "))
	}
	sort.Strings(out)
	return out
}

var mergeSessions = []string{"", "SET default_collation = 'nocase.noaccent'", "SET default_collation = 'nocase.icu_noaccent'"}

func TestTableDeltaMergeState_isTheStateTheViewsRead(t *testing.T) {
	f := writeMergeFixture(t)
	want := []string{"n=11 tok=a", "n=1 tok=abc", "n=2 tok=ABC", "n=22 tok=c", "n=33 tok=d", "n=4 tok=e"}
	sort.Strings(want)
	for _, session := range mergeSessions {
		views := queryRows(t, session, TableDeltaStateSQL("'"+f.base+"'", f.posdels, f.upserts, f.base, ""))
		merge := queryRows(t, session, TableDeltaMergeStateSQL("'"+f.base+"'", f.posdels, f.upserts, f.base))
		if !reflect.DeepEqual(merge, views) {
			t.Errorf("session %q: the merge reads a state the views do not\nmerge: %v\nviews: %v", session, merge, views)
		}
		if !reflect.DeepEqual(merge, want) {
			t.Errorf("session %q: merge state = %v, want %v", session, merge, want)
		}
	}
}

// The newest version of every key, the tombstones included and the technical
// columns kept: what a range pair is written from.
func TestTableDeltaLatestSQL(t *testing.T) {
	f := writeMergeFixture(t)
	delta := "SELECT * FROM read_parquet(" + f.upserts + ", filename=true, union_by_name=true)"
	// Key and operation of the row kept for each key; the payload is compared
	// against the window below.
	want := []string{"ABC:u", "a:u", "abc:u", "b:d", "c:u", "d:u", "x:d"}
	window := "SELECT * EXCLUDE (filename) FROM (" + delta + ") QUALIFY row_number() OVER (PARTITION BY \"" + TableDeltaPKColumn +
		"\" COLLATE C ORDER BY filename COLLATE C DESC) = 1"
	for _, session := range mergeSessions {
		got := queryRows(t, session, TableDeltaLatestSQL(delta))
		keys := queryRows(t, session, "SELECT \""+TableDeltaPKColumn+"\" || ':' || \""+TableDeltaOpColumn+"\" AS k FROM ("+TableDeltaLatestSQL(delta)+")")
		for i := range keys {
			keys[i] = strings.TrimPrefix(keys[i], "k=")
		}
		sort.Strings(keys)
		if !reflect.DeepEqual(keys, want) {
			t.Errorf("session %q: latest keeps %v, want %v", session, keys, want)
		}
		if old := queryRows(t, session, window); !reflect.DeepEqual(got, old) {
			t.Errorf("session %q: the join and the window it replaces disagree\n join: %v\nwindow: %v", session, got, old)
		}
	}
}
