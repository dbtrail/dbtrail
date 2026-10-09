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

// TestStateView_aTableNamedWithABackslashStillLoads: DuckDB's glob splits a
// pattern on a backslash as on a slash, so no class can stand for one and an
// escaped path would name no file. Such a path is given as it is, which
// matches nothing as a pattern and is then read as the file it names: the
// view loads and reads its table, as it did before the paths were escaped.
func TestStateView_aTableNamedWithABackslashStillLoads(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	neighbour := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	const name = `a\*b`
	base := writeTableBeside(t, neighbour, name)
	tables := []BaselineTable{{Schema: "shop", Table: name, Path: base, Rel: "shop/" + name + ".parquet", SchemaKnown: true}}
	sqlText := Generate(Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow: FollowNone, Baselines: tables,
	})
	db, err := loadViews(t, sqlText)
	if err != nil {
		t.Fatalf("the views do not load: %v\n%s", err, sqlText)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM shop."a\*b" WHERE status IN ('p', 'q', 'r')`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("rows of the table's own = %d (err=%v), want 3\n%s", n, err, sqlText)
	}
}
