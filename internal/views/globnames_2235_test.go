package views

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2235: read_parquet takes a path as a glob, so the files of a table whose
// name holds a glob character also match those of a neighbour the pattern
// covers. Every way a view reads a table must read that table's files only.

// globNames are table names beside a table named "orders". pattern says the
// name, read as a glob, matches the neighbour's file: the three that are
// patterns do, and the test checks that it is so. The other two are names a
// view must read as before: one with nothing special, and one in braces,
// which a local glob does not expand.
var globNames = []struct {
	table, neighbour string
	pattern          bool
}{
	{"plain", "orders", false},
	{"{orders,x}", "orders", false},
	{"or?ers", "orders", true},
	{"or*s", "orders", true},
	{"order[st]", "orders", true},
}

// writeTableBeside writes the table file of name beside the neighbour's,
// with rows of its own (ids 1..3, statuses p, q, r): a view that reads the
// neighbour's file instead of the table's shows a, b, c.
func writeTableBeside(t *testing.T, neighbour, name string) string {
	t.Helper()
	own := writeSnapshot(t, t.TempDir(), "2026-04-30T03-00-00Z", true, "p", "q", "r")
	raw, err := os.ReadFile(own)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(filepath.Dir(neighbour), name+".parquet")
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestStateView_aTableNamedLikeAGlobReadsItsOwnFiles: for each such name,
// each shape of table (its file alone, a v0.83.0 pair, one pair, two pairs)
// and each following mode, the view returns the table's own state. The
// neighbour's file holds the same three rows, so reading it too doubles
// them; the neighbour's pair kills id 3 and adds id 77, so reading that too
// loses a row or gains one.
func TestStateView_aTableNamedLikeAGlobReadsItsOwnFiles(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	alone := []string{"1=p", "2=q", "3=r"}
	shapes := []struct {
		name  string
		write func(t *testing.T, base string)
		want  []string
		// table, when set, changes what the generator knows of the table:
		// each of these sends the file-alone view down another body.
		table func(*BaselineTable)
	}{
		{name: "the file alone", write: func(*testing.T, string) {}, want: alone},
		{name: "the file alone, its columns listed", write: func(*testing.T, string) {}, want: alone,
			table: func(bt *BaselineTable) { bt.Columns = []string{"id", "status"} }},
		{name: "the file alone, a column read through an expression", write: func(*testing.T, string) {}, want: alone,
			table: func(bt *BaselineTable) { bt.BinaryText = []string{"status"} }},
		{name: "the file alone, its columns unknown", write: func(*testing.T, string) {}, want: alone,
			table: func(bt *BaselineTable) { bt.SchemaKnown = false }},
		{name: "a v0.83.0 pair", write: func(t *testing.T, base string) {
			writeLegacyDeltaPair(t, base, []int64{0}, [][2]string{{"1", "changed"}})
		}, want: []string{"1=changed", "2=q", "3=r"}},
		{name: "one pair", write: func(t *testing.T, base string) {
			writeDeltaPair(t, base, 0, []int64{0}, [][3]string{{"1", "changed", "u"}})
		}, want: []string{"1=changed", "2=q", "3=r"}},
		{name: "two pairs", write: func(t *testing.T, base string) {
			writeDeltaPair(t, base, 0, []int64{0}, [][3]string{{"1", "first", "u"}})
			writeDeltaPair(t, base, 1, []int64{0}, [][3]string{{"1", "changed", "u"}})
		}, want: []string{"1=changed", "2=q", "3=r"}},
		// The chain named by its two patterns, as the views file inside a
		// snapshot names it: a pinned view with no list of the pairs.
		{name: "two pairs by pattern", write: func(t *testing.T, base string) {
			writeDeltaPair(t, base, 0, []int64{0}, [][3]string{{"1", "first", "u"}})
			writeDeltaPair(t, base, 1, []int64{0}, [][3]string{{"1", "changed", "u"}})
		}, want: []string{"1=changed", "2=q", "3=r"}},
	}
	for _, n := range globNames {
		for _, shape := range shapes {
			for _, mode := range followModes {
				// A view that follows the newest snapshot does not read a
				// v0.83.0 pair, whatever the table is named ("plain" fails
				// the same way): not this issue's, and left out here.
				if shape.name == "a v0.83.0 pair" && mode.follow == FollowNewest {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", n.table, shape.name, mode.name), func(t *testing.T) {
					root := t.TempDir()
					neighbour := writeSnapshot(t, root, stamp, true, "a", "b", "c")
					if filepath.Base(neighbour) != n.neighbour+".parquet" {
						t.Fatalf("the fixture's table is %s, the case needs %s", filepath.Base(neighbour), n.neighbour)
					}
					// The neighbour has a chain of the same shape, so every
					// kind of file the table has, the neighbour has too.
					if shape.name == "a v0.83.0 pair" {
						writeLegacyDeltaPair(t, neighbour, []int64{2}, [][2]string{{"77", "neighbour"}})
					} else if !strings.HasPrefix(shape.name, "the file alone") {
						writeDeltaPair(t, neighbour, 0, []int64{2}, [][3]string{{"77", "neighbour", "u"}})
						if strings.HasPrefix(shape.name, "two pairs") {
							writeDeltaPair(t, neighbour, 1, nil, [][3]string{{"78", "neighbour", "u"}})
						}
					}
					base := writeTableBeside(t, neighbour, n.table)
					// The premise: unescaped, the table's path is a pattern
					// that takes the neighbour's file. A name for which it
					// is not would pass with no fix at all.
					matches, err := filepath.Glob(base)
					if err != nil {
						t.Fatal(err)
					}
					if takesNeighbour := slices.Contains(matches, neighbour); takesNeighbour != n.pattern {
						t.Fatalf("%s as a pattern matches %v: takes the neighbour = %v, the case says %v", n.table, matches, takesNeighbour, n.pattern)
					}
					shape.write(t, base)
					tables := []BaselineTable{{Schema: "shop", Table: n.table, Path: base, Rel: "shop/" + n.table + ".parquet", SchemaKnown: true}}
					if err := MarkTableDeltas(context.Background(), tables); err != nil {
						t.Fatalf("MarkTableDeltas: %v", err)
					}
					if shape.table != nil {
						shape.table(&tables[0])
					}
					if shape.name == "two pairs by pattern" {
						if len(tables[0].DeltaFiles) != 2 {
							t.Fatalf("chain = %+v, want two pairs", tables[0].DeltaFiles)
						}
						tables[0].DeltaFiles = nil
					}
					if mode.follow == FollowPointer {
						if err := os.Symlink(stamp, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
							t.Fatal(err)
						}
						tables[0].Path = filepath.Join(root, baseline.CurrentLinkName, "shop", n.table+".parquet")
					}
					sqlText := Generate(Input{
						GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
						BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
						Follow: mode.follow, Baselines: tables,
					})
					db := execViews(t, sqlText)
					rows, err := db.Query(`SELECT id::VARCHAR || '=' || status FROM shop."` + strings.ReplaceAll(n.table, `"`, `""`) + `" ORDER BY id, status`)
					if err != nil {
						t.Fatalf("%v\n%s", err, sqlText)
					}
					defer rows.Close()
					var got []string
					for rows.Next() {
						var s string
						if err := rows.Scan(&s); err != nil {
							t.Fatal(err)
						}
						got = append(got, s)
					}
					if err := rows.Err(); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, shape.want) {
						t.Fatalf("state view = %v, want %v\n%s", got, shape.want, sqlText)
					}
				})
			}
		}
	}
}

// TestStateView_aBackslashNameNeverReadsAnotherSchema: DuckDB's glob splits
// a pattern on a backslash as on a slash, so a table named `\..\hr\*` in
// "shop" is, read as a pattern, every table file of "hr". No class stands for
// a backslash, so no pattern names that one file: the table gets no view, the
// file says why, and every other view still loads. A name with a backslash
// and no pattern character is read as the file it names by a pinned view,
// and has no view when the view follows the snapshots, which finds a
// table's chain by a pattern built on its name. A pinned view whose chain is
// not named file by file does the same, and has none either.
func TestStateView_aBackslashNameNeverReadsAnotherSchema(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	at := time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name string
		// pinned says a pinned view exists (and reads the table's own
		// rows); a following one never does.
		pinned bool
		// byPattern gives the table a chain the view is not told the
		// files of, and the schema "hr" a chain a pattern would reach.
		byPattern bool
	}{
		{`a\*b`, false, false},
		{`\..\hr\*`, false, false},
		{`\..\hr\salar?es`, false, false},
		{`\..\hr\[s]alaries`, false, false},
		{`\..\hr\{salaries,x}`, false, false},
		{`\..\hr\salaries`, true, false},
		{`a\b`, true, false},
		{`\..\hr\salaries`, false, true},
	} {
		for _, mode := range []FollowMode{FollowNone, FollowPointer, FollowNewest} {
			t.Run(fmt.Sprintf("%s/pattern=%v/%v", c.name, c.byPattern, mode), func(t *testing.T) {
				root := t.TempDir()
				orders := writeSnapshot(t, root, stamp, true, "a", "b", "c")
				own := writeTableBeside(t, orders, c.name)
				// The other schema's table: the same file as shop.orders,
				// so a read that reaches it returns a, b, c.
				raw, err := os.ReadFile(orders)
				if err != nil {
					t.Fatal(err)
				}
				hr := filepath.Join(root, stamp, "hr", "salaries.parquet")
				if err := os.MkdirAll(filepath.Dir(hr), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(hr, raw, 0o644); err != nil {
					t.Fatal(err)
				}
				in := Input{
					GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
					BaselineSource: root, BaselineSnapshot: at, Follow: mode,
					Baselines: []BaselineTable{
						{Schema: "shop", Table: "orders", Path: orders, Rel: "shop/orders.parquet", SchemaKnown: true},
						{Schema: "shop", Table: c.name, Path: own, Rel: "shop/" + c.name + ".parquet", SchemaKnown: true},
					},
				}
				if c.byPattern {
					writeDeltaPair(t, hr, 0, []int64{2}, [][3]string{{"77", "hr", "u"}})
					writeDeltaPair(t, own, 0, nil, nil)
					// Said to have a chain, with no file of it named: what a
					// caller that does not list the chain hands over.
					in.Baselines[1].Delta = true
				}
				if mode == FollowPointer {
					if err := os.Symlink(stamp, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
						t.Fatal(err)
					}
					for i := range in.Baselines {
						in.Baselines[i].Path = filepath.Join(root, baseline.CurrentLinkName, in.Baselines[i].Rel)
					}
				}
				sqlText := Generate(in)
				db, err := loadViews(t, sqlText)
				if err != nil {
					t.Fatalf("the views do not load: %v\n%s", err, sqlText)
				}
				var n int
				if err := db.QueryRow(`SELECT count(*) FROM shop.orders WHERE status IN ('a', 'b', 'c')`).Scan(&n); err != nil || n != 3 {
					t.Fatalf("shop.orders, the table beside it: %d rows (err=%v), want 3\n%s", n, err, sqlText)
				}
				quoted := `shop."` + strings.ReplaceAll(c.name, `"`, `""`) + `"`
				rows, err := db.Query(`SELECT status FROM ` + quoted + ` ORDER BY 1`)
				if !c.pinned || mode != FollowNone {
					if err == nil {
						rows.Close()
						t.Fatalf("%s has a view; no path names its file, so it must have none\n%s", quoted, sqlText)
					}
					if !strings.Contains(sqlText, "holds a backslash, and DuckDB reads a path as a pattern") {
						t.Fatalf("the file does not say why %s has no view\n%s", quoted, sqlText)
					}
					return
				}
				if err != nil {
					t.Fatalf("%s: %v\n%s", quoted, err, sqlText)
				}
				defer rows.Close()
				var got []string
				for rows.Next() {
					var st string
					if err := rows.Scan(&st); err != nil {
						t.Fatal(err)
					}
					got = append(got, st)
				}
				if strings.Join(got, ",") != "p,q,r" {
					t.Fatalf("%s returned %v, want its own p, q, r (a, b, c are the other schema's)\n%s", quoted, got, sqlText)
				}
			})
		}
	}
}
