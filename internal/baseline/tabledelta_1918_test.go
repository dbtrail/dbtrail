package baseline

import (
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestTableDeltaFollowGlobs (#1918): the patterns a following view reads the
// chain through match the table's own file, so they never match nothing, plus
// the chain's plain and range pairs of their own suffix. Not the other suffix,
// not the v0.83.0 pair, not a sibling table, not a .tmp a compaction stages.
// Run through DuckDB's own glob, over a name with metacharacters.
func TestTableDeltaFollowGlobs(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{
		"or[d]ers.parquet",
		"or[d]ers.000000.posdel", "or[d]ers.000000.upserts",
		"or[d]ers.000001-000003.posdel", "or[d]ers.000001-000003.upserts",
		"or[d]ers.000004.posdel.tmp", "or[d]ers.000004.upserts.tmp",
		"or[d]ers.posdel", "or[d]ers.upserts", "or[d]ers.abcdef.upserts",
		"or[d]ers_archive.parquet", "or[d]ers_archive.000001.upserts", "or[d]ers_archive.000001.posdel",
		"or[d]ers.pending.parquet",
	} {
		if err := os.WriteFile(filepath.Join(dir, n), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	match := func(pattern string) []string {
		t.Helper()
		if !strings.Contains(pattern, "*") {
			t.Fatalf("glob %q holds no wildcard: over S3 it would report every key as present", pattern)
		}
		rows, err := db.Query("SELECT file FROM glob('" + strings.ReplaceAll(pattern, "'", "''") + "') ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var f string
			if err := rows.Scan(&f); err != nil {
				t.Fatal(err)
			}
			got = append(got, filepath.Base(f))
		}
		return got
	}
	posdel, upserts := TableDeltaFollowGlobs(filepath.Join(dir, "or[d]ers.parquet"))
	if got, want := match(posdel), []string{"or[d]ers.000000.posdel", "or[d]ers.000001-000003.posdel", "or[d]ers.parquet"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("posdel glob matched %v, want exactly %v", got, want)
	}
	if got, want := match(upserts), []string{"or[d]ers.000000.upserts", "or[d]ers.000001-000003.upserts", "or[d]ers.parquet"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("upserts glob matched %v, want exactly %v", got, want)
	}
	// The base must come LAST in DuckDB's order: the posdel read takes its
	// columns from the first file it opens, and that has to be a pair when
	// there is one. A digit sorts before "p", always.
	if got := match(posdel); got[len(got)-1] != "or[d]ers.parquet" {
		t.Fatalf("the base does not sort last: %v", got)
	}
}
