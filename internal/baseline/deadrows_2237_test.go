package baseline

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestBaseMinusDead (#2237): the base's rows less the dead row numbers, read
// through one 64-bit mask per block of 64 rows. Each case is a list of dead
// positions over a base of 200 rows whose id is its row number, and the
// answer is worked out here: every id not in the list.
//
// The base carries columns named like everything the statement names itself
// (the block, the mask, the position, the aliases), and must come back with
// exactly its own columns.
func TestBaseMinusDead(t *testing.T) {
	const n = 200
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := t.TempDir()
	base := filepath.Join(dir, "t.parquet")
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT i AS id, i AS bintrail_block, i AS bintrail_mask, i AS bintrail_p, i AS pos, i AS k, i AS m,
		i AS bintrail_b, i AS bintrail_dead FROM range(%d) t(i)) TO '%s' (FORMAT PARQUET, ROW_GROUP_SIZE 50)`, n, base)); err != nil {
		t.Fatal(err)
	}
	wantCols := "id,bintrail_block,bintrail_mask,bintrail_p,pos,k,m,bintrail_b,bintrail_dead"
	all := func() []string {
		var s []string
		for i := range n {
			s = append(s, fmt.Sprint(i))
		}
		return s
	}
	block := func(b int) []string {
		var s []string
		for i := b * 64; i < (b+1)*64; i++ {
			s = append(s, fmt.Sprint(i))
		}
		return s
	}
	cases := []struct {
		name string
		dead []string // SQL values, so NULL and out-of-range ones can be written
	}{
		{"none", nil},
		{"the first row", []string{"0"}},
		{"the last bit of a block: 1 << 63", []string{"63"}},
		{"the first row of the second block", []string{"64"}},
		{"both sides of a block's edge", []string{"63", "64"}},
		{"the last row", []string{fmt.Sprint(n - 1)}},
		{"a whole block", block(1)},
		{"a whole block and its neighbours' edges", append(block(1), "63", "128")},
		{"every row", all()},
		{"the same position twice", []string{"5", "5", "70", "70", "70"}},
		{"a NULL among them", []string{"5", "NULL", "70"}},
		{"only a NULL", []string{"NULL"}},
		{"positions the file does not have", []string{"5", fmt.Sprint(n), "100000", "9223372036854775807"}},
		{"a negative position", []string{"-1", "-64", "5"}},
		{"scattered", []string{"1", "2", "3", "62", "65", "127", "129", "190", "199"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dead := "SELECT NULL::BIGINT WHERE false"
			if len(c.dead) > 0 {
				dead = "SELECT x::BIGINT FROM (VALUES (" + strings.Join(c.dead, "), (") + ")) v(x)"
			}
			q := baseMinusDeadSQL("'"+base+"'", dead)
			var want []string
			for i := range n {
				if !slices.Contains(c.dead, fmt.Sprint(i)) {
					want = append(want, fmt.Sprint(i))
				}
			}
			rows, err := db.Query("SELECT * FROM (" + q + ") ORDER BY id")
			if err != nil {
				t.Fatalf("%v\n%s", err, q)
			}
			defer rows.Close()
			cols, _ := rows.Columns()
			if got := strings.Join(cols, ","); got != wantCols {
				t.Fatalf("columns = %s, want the base's own: %s", got, wantCols)
			}
			var got []string
			for rows.Next() {
				vals := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					t.Fatal(err)
				}
				// Every column of a row holds its row number: a value that
				// moved between columns would show here.
				for i, v := range vals {
					if fmt.Sprint(v) != fmt.Sprint(vals[0]) {
						t.Fatalf("row %v: column %s = %v", vals[0], cols[i], v)
					}
				}
				got = append(got, fmt.Sprint(vals[0]))
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("live rows = %v\nwant %v", got, want)
			}
		})
	}
}
