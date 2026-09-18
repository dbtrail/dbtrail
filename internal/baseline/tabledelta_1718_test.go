package baseline

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// #1718: a table delta is a CHAIN of numbered pairs beside the base, one pair
// per refresh, never rewritten. These tests pin the name grammar, the listing
// (half a pair at ANY sequence is a damaged snapshot) and the empty pair a full
// backup writes, which is sequence 0 of every chain.

func TestParseTableDeltaName(t *testing.T) {
	cases := []struct {
		name       string
		wantStem   string
		wantSeq    int
		wantSuffix string
		ok         bool
	}{
		{"orders.000001.posdel", "orders", 1, TableDeltaPosdelSuffix, true},
		{"orders.000001.upserts", "orders", 1, TableDeltaUpsertsSuffix, true},
		{"orders.000000.upserts", "orders", 0, TableDeltaUpsertsSuffix, true},
		{"orders.999999.posdel", "orders", 999999, TableDeltaPosdelSuffix, true},
		// The v0.83.0 layout: no sequence. Recognised (so an old snapshot is
		// still described) and told apart (so a refresh compacts it).
		{"orders.posdel", "orders", TableDeltaLegacySeq, TableDeltaPosdelSuffix, true},
		{"orders.upserts", "orders", TableDeltaLegacySeq, TableDeltaUpsertsSuffix, true},
		// A table whose own name ends in six digits keeps them: only the
		// segment BETWEEN the stem and the suffix is a sequence.
		{"t_202601.000002.posdel", "t_202601", 2, TableDeltaPosdelSuffix, true},
		{"t_202601.posdel", "t_202601", TableDeltaLegacySeq, TableDeltaPosdelSuffix, true},
		// Not deltas.
		{"orders.parquet", "", 0, "", false},
		{"orders.00001.posdel", "orders.00001", TableDeltaLegacySeq, TableDeltaPosdelSuffix, true}, // five digits: a table named orders.00001
		{"orders.0000010.upserts", "orders.0000010", TableDeltaLegacySeq, TableDeltaUpsertsSuffix, true},
		{"orders.notanumber.upserts", "orders.notanumber", TableDeltaLegacySeq, TableDeltaUpsertsSuffix, true},
		{".000001.posdel", "", 0, "", false},
		{"orders.000001.parquet", "", 0, "", false},
		{"_SUCCESS", "", 0, "", false},
	}
	for _, c := range cases {
		stem, seq, suffix, ok := ParseTableDeltaName(c.name)
		if ok != c.ok || stem != c.wantStem || seq != c.wantSeq || suffix != c.wantSuffix {
			t.Errorf("%q: (%q, %d, %q, %v), want (%q, %d, %q, %v)", c.name, stem, seq, suffix, ok, c.wantStem, c.wantSeq, c.wantSuffix, c.ok)
		}
	}
}

func TestTableDeltaPaths_seqIsZeroPadded(t *testing.T) {
	p, u := TableDeltaPaths("/snap/shop/orders.parquet", 7)
	if p != "/snap/shop/orders.000007.posdel" || u != "/snap/shop/orders.000007.upserts" {
		t.Fatalf("paths = %s, %s", p, u)
	}
	p, u = TableDeltaPaths("s3://b/snap/shop/orders.parquet", 0)
	if p != "s3://b/snap/shop/orders.000000.posdel" || u != "s3://b/snap/shop/orders.000000.upserts" {
		t.Fatalf("s3 paths = %s, %s", p, u)
	}
	// Round trip through the parser, so the writer and the lister agree.
	stem, seq, _, ok := ParseTableDeltaName(filepath.Base(u))
	if !ok || stem != "orders" || seq != 0 {
		t.Fatalf("parse(%s) = %q, %d, %v", u, stem, seq, ok)
	}
}

// TestTableDeltaGlobs: the pattern DuckDB reads the chain with matches every
// numbered file of THIS table and nothing else: not a sibling that shares the
// prefix, not the v0.83.0 pair, not a stray with letters where the sequence
// goes, and not the base. Run through DuckDB's own glob, over a name with
// metacharacters so the escaping is covered too.
func TestTableDeltaGlobs(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{
		"or[d]ers.parquet", "or[d]ers.000000.posdel", "or[d]ers.000000.upserts", "or[d]ers.000012.upserts",
		"or[d]ers.posdel", "or[d]ers.upserts", "or[d]ers.abcdef.upserts", "or[d]ers.0000001.upserts",
		"or[d]ers_archive.000001.upserts", "or[d]ers.000001.parquet",
	} {
		if err := os.WriteFile(filepath.Join(dir, n), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	_, ups := TableDeltaGlobs(filepath.Join(dir, "or[d]ers.parquet"))
	if !strings.Contains(ups, "*") && !strings.Contains(ups, "[0-9]") {
		t.Fatalf("glob %q holds no wildcard: over S3 it would report every key as present", ups)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT file FROM glob('" + strings.ReplaceAll(ups, "'", "''") + "') ORDER BY 1")
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
	want := []string{"or[d]ers.000000.upserts", "or[d]ers.000012.upserts"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("glob matched %v, want exactly %v", got, want)
	}
}

func TestMarkTableDeltaFiles(t *testing.T) {
	chains, err := MarkTableDeltaFiles("/snap/shop", []string{
		"orders.parquet", "orders.000000.posdel", "orders.000000.upserts",
		"orders.000002.posdel", "orders.000002.upserts", // a gap at 1 is fine: an empty window writes nothing
		"orders.000001.posdel", "orders.000001.upserts", // listed out of order
		"plain.parquet",
		"old.parquet", "old.posdel", "old.upserts",
	})
	if err != nil {
		t.Fatal(err)
	}
	c := chains["/snap/shop/orders.parquet"]
	if c == nil || c.Legacy || len(c.Files) != 3 {
		t.Fatalf("orders chain = %+v", c)
	}
	for i, want := range []int{0, 1, 2} {
		if c.Files[i].Seq != want || c.Files[i].Posdel != "/snap/shop/orders.00000"+string(rune('0'+want))+".posdel" {
			t.Fatalf("file %d = %+v", i, c.Files[i])
		}
	}
	if c.Last().Seq != 2 {
		t.Fatalf("Last = %+v", c.Last())
	}
	if chains["/snap/shop/plain.parquet"] != nil {
		t.Fatal("a table with no delta got a chain")
	}
	old := chains["/snap/shop/old.parquet"]
	if old == nil || !old.Legacy || len(old.Files) != 0 || old.LegacyPosdel != "/snap/shop/old.posdel" {
		t.Fatalf("legacy chain = %+v", old)
	}

	// Half a pair at any sequence names the table, whether it is the last
	// sequence or one in the middle. Applying dead positions with no upserts
	// deletes every row that sequence updated, and the reverse duplicates them.
	for _, half := range [][]string{
		{"orders.000000.posdel", "orders.000000.upserts", "orders.000001.posdel"},
		{"orders.000000.posdel", "orders.000000.upserts", "orders.000001.upserts", "orders.000002.posdel", "orders.000002.upserts"},
		{"orders.posdel"},
	} {
		_, err := MarkTableDeltaFiles("/snap/shop", append([]string{"orders.parquet"}, half...))
		if !errors.Is(err, ErrHalfTableDelta) || !strings.Contains(err.Error(), "orders.parquet") {
			t.Errorf("%v: err = %v, want ErrHalfTableDelta naming the table", half, err)
		}
	}
	// A chain with numbered files AND the legacy pair is two layouts at once:
	// refused, not merged.
	if _, err := MarkTableDeltaFiles("/snap/shop", []string{"orders.parquet", "orders.posdel", "orders.upserts",
		"orders.000000.posdel", "orders.000000.upserts"}); err == nil {
		t.Error("a table with both layouts was accepted")
	}
	// A chain with no sequence 0 has no start marker: the file a full backup
	// or a compaction writes. Refused for the same reason as half a pair.
	if _, err := MarkTableDeltaFiles("/snap/shop", []string{"orders.parquet", "orders.000001.posdel", "orders.000001.upserts"}); err == nil {
		t.Error("a chain without its sequence-0 pair was accepted")
	}
}

func TestSnapshotTableDeltas_numbered(t *testing.T) {
	snap := t.TempDir()
	touch := func(rel string) {
		p := filepath.Join(snap, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	touch("shop/orders.parquet")
	touch("shop/orders.000000.posdel")
	touch("shop/orders.000000.upserts")
	touch("shop/orders.000003.posdel")
	touch("shop/orders.000003.upserts")
	touch("shop/plain.parquet")
	got, err := SnapshotTableDeltas(t.Context(), snap)
	if err != nil {
		t.Fatal(err)
	}
	if !got[filepath.Join(snap, "shop/orders.parquet")] || got[filepath.Join(snap, "shop/plain.parquet")] || len(got) != 1 {
		t.Fatalf("SnapshotTableDeltas = %v, want only shop/orders", got)
	}
	chain, err := ListTableDelta(t.Context(), filepath.Join(snap, "shop/orders.parquet"))
	if err != nil || chain == nil || len(chain.Files) != 2 || chain.Last().Seq != 3 {
		t.Fatalf("ListTableDelta = %+v, %v", chain, err)
	}
	if c, err := ListTableDelta(t.Context(), filepath.Join(snap, "shop/plain.parquet")); err != nil || c != nil {
		t.Fatalf("ListTableDelta(plain) = %+v, %v", c, err)
	}
	touch("shop/half.000000.posdel")
	if _, err := SnapshotTableDeltas(t.Context(), snap); err == nil || !strings.Contains(err.Error(), "half.parquet") {
		t.Fatalf("half a pair: err = %v, want ErrHalfTableDelta naming the table", err)
	}
}

// TestTableDeltaColumns: the technical columns go LAST, after the table's own,
// and a table that already has one of the two names is refused rather than
// shadowed (the reader partitions on bintrail_pk; a table column of that name
// would make the state wrong without an error).
func TestTableDeltaColumns(t *testing.T) {
	cols, err := ParseSchemaText("CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `name` varchar(20)\n);")
	if err != nil {
		t.Fatal(err)
	}
	got, err := TableDeltaColumns(cols)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(got))
	for i, c := range got {
		names[i] = c.Name
	}
	if !reflect.DeepEqual(names, []string{"id", "name", TableDeltaPKColumn, TableDeltaOpColumn}) {
		t.Fatalf("columns = %v", names)
	}
	for _, reserved := range []string{"bintrail_pk", "BINTRAIL_OP", "file_row_number", "filename"} {
		bad, _ := ParseSchemaText("CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `" + reserved + "` int\n);")
		if _, err := TableDeltaColumns(bad); err == nil || !strings.Contains(err.Error(), reserved) {
			t.Errorf("a table with a column named %s: err = %v, want a refusal naming it", reserved, err)
		}
	}
}
