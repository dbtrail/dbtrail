package pgdelta

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// #3082 writes one pair per update, numbered on from the pair before: a
// following view reads such a chain, long, with a key in many pairs, a key
// deleted and put back, the same rows StateSQL reads through the exact list
// of its files, after any number of its links.
func TestFollowStateSQL_aLongNumberedChain(t *testing.T) {
	db := duck(t)
	base := filepath.Join(t.TempDir(), "public", "orders.parquet")
	cols, key := []string{"id", "val"}, []string{"id"}
	writeBase(t, base, cols, []cell{v("1"), v("base")}, []cell{v("2"), v("base")}, []cell{v("3"), v("base")})
	deadGlob, rowsGlob := FollowGlobs(base)
	follow := func() []string {
		t.Helper()
		q := FollowStateSQL(quote(literal(base)), quote(deadGlob), quote(rowsGlob), base)
		return stateOf(t, db, q, cols...)
	}
	var dead, rows []string
	if got, want := follow(), state(t, db, base, nil, nil, cols...); !reflect.DeepEqual(got, want) {
		t.Fatalf("no chain: following %q, listed %q", got, want)
	}
	for seq := range 14 {
		changes := []change{{key: "1", row: []cell{v("1"), v(fmt.Sprintf("v%d", seq))}}}
		switch seq {
		case 2:
			changes = append(changes, change{key: "2", del: true})
		case 5:
			changes = append(changes, change{key: "2", row: []cell{v("2"), v("back")}}, change{key: "9", row: []cell{v("9"), v("new")}})
		case 9:
			changes = append(changes, change{key: "9", del: true})
		}
		d, r := pair(t, db, base, seq, cols, key, changes...)
		dead, rows = append(dead, d), append(rows, r)
		if got, want := follow(), state(t, db, base, dead, rows, cols...); !reflect.DeepEqual(got, want) {
			t.Fatalf("after pair %d: following %q, listed %q", seq, got, want)
		}
	}
	want(t, "fourteen pairs", follow(), "[1] [v13]", "[2] [back]", "[3] [base]")
}

// stateOf runs q, a state's SELECT, and returns its rows as state does.
func stateOf(t *testing.T, db *sql.DB, q string, cols ...string) []string {
	t.Helper()
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = `"` + c + `"`
	}
	rs, err := db.Query("SELECT " + strings.Join(quoted, ", ") + " FROM (" + q + ") ORDER BY ALL")
	if err != nil {
		t.Fatalf("the state: %v\n%s", err, q)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		vals := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range vals {
			dest[i] = &vals[i]
		}
		if err := rs.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(vals))
		for i, x := range vals {
			parts[i] = "∅"
			if x.Valid {
				parts[i] = "[" + x.String + "]"
			}
		}
		out = append(out, strings.Join(parts, " "))
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}
