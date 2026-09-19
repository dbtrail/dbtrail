package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #1716: locating the base rows a window touched used to pull EVERY base
// key through Go. For integer keys the touched keys are handed to DuckDB as
// a table and the base is semi-joined against it, so only the matching rows
// cross into Go — where the same canonical check as before still decides.
// Every other key type keeps the full scan, and so does any integer key the
// canonical string cannot spell exactly: the join must never miss a row the
// scan would find.

// writeKeyedBaseline writes a base through the real writer from a CREATE
// TABLE and mydumper-text rows; pkNames says which columns form the key (the
// DDL parser does not read the PRIMARY KEY line).
func writeKeyedBaseline(t *testing.T, createSQL string, pkNames []string, rows [][]string) (path string, pkCols []metadata.ColumnMeta) {
	t.Helper()
	return writeKeyedBaselineGroups(t, createSQL, pkNames, rows, 7)
}

func writeKeyedBaselineGroups(t *testing.T, createSQL string, pkNames []string, rows [][]string, rowGroup int) (path string, pkCols []metadata.ColumnMeta) {
	t.Helper()
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	path = filepath.Join(t.TempDir(), "t.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: rowGroup,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range pkNames {
		for i, c := range cols {
			if c.Name == name {
				pkCols = append(pkCols, metadata.ColumnMeta{Name: c.Name, OrdinalPosition: i + 1, IsPK: true, DataType: c.MySQLType})
			}
		}
	}
	if len(pkCols) != len(pkNames) {
		t.Fatalf("pk columns %v not all found in %v", pkNames, cols)
	}
	return path, pkCols
}

func touched(keys ...string) map[string]*query.ResultRow {
	m := make(map[string]*query.ResultRow, len(keys))
	for _, k := range keys {
		m[k] = &query.ResultRow{PKValues: k}
	}
	return m
}

// bothWays runs the join path and the scan path and returns them; the caller
// asserts the join was taken where it should be and that both agree. When
// the join was taken, the rows that crossed into Go must be exactly the
// matches: a join that stopped narrowing would still return the right
// positions, and this is the assertion that catches it. The scan's count is
// pinned to the base's row count, so Examined is known to count every row
// DuckDB returned and not only the matches. Temp files must not accumulate.
func bothWays(t *testing.T, base string, pkCols []metadata.ColumnMeta, changes map[string]*query.ResultRow) (joined bool, got, want []int64) {
	t.Helper()
	ctx := context.Background()
	tempBefore := touchedKeyFiles(t)
	defer func() {
		if after := touchedKeyFiles(t); after != tempBefore {
			t.Fatalf("touched-keys files in the temp dir: %d before, %d after", tempBefore, after)
		}
	}()
	got, st, err := lookupBasePositionsWith(ctx, base, "s", "t", pkCols, changes, duckdbutil.DefaultTuning(), true)
	if err != nil {
		t.Fatalf("join path: %v", err)
	}
	want, scanSt, err := lookupBasePositionsWith(ctx, base, "s", "t", pkCols, changes, duckdbutil.DefaultTuning(), false)
	if err != nil {
		t.Fatalf("scan path: %v", err)
	}
	if scanSt.Joined {
		t.Fatal("the scan path reported a join")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("join = %v, scan = %v", got, want)
	}
	if st.Joined && st.Examined != len(got) {
		t.Fatalf("the join let %d rows into Go for %d matches", st.Examined, len(got))
	}
	if !st.Joined && st.Examined != scanSt.Examined {
		t.Fatalf("fallback examined %d rows, the scan %d", st.Examined, scanSt.Examined)
	}
	if n := baseRowCount(t, base); scanSt.Examined != n {
		t.Fatalf("the scan examined %d rows of a %d-row base", scanSt.Examined, n)
	}
	return st.Joined, got, want
}

func touchedKeyFiles(t *testing.T) int {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(os.TempDir(), "bintrail-touched-keys-*.csv"))
	if err != nil {
		t.Fatal(err)
	}
	return len(m)
}

func baseRowCount(t *testing.T, base string) int {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	var n int
	if err := ddb.QueryRow("SELECT count(*) FROM parquet_scan('" + strings.ReplaceAll(base, "'", "''") + "')").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

const intKeyDDL = "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"

func TestLookupBasePositions_integerKeysJoinInDuckDB(t *testing.T) {
	var rows [][]string
	for i := range 100 {
		rows = append(rows, []string{fmt.Sprint(i*3 - 50), "x"}) // negatives too
	}
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, rows)
	// Touched: some present, one absent (an INSERT), duplicates impossible (map).
	joined, got, _ := bothWays(t, base, pkCols, touched("-50", "1", "247", "999999"))
	if !joined {
		t.Fatal("integer key did not take the join")
	}
	if want := []int64{0, 17, 99}; !reflect.DeepEqual(got, want) {
		t.Fatalf("positions = %v, want %v", got, want)
	}
	if _, st, err := lookupBasePositionsWith(context.Background(), base, "s", "t", pkCols, nil, duckdbutil.DefaultTuning(), true); err != nil || st.Joined || st.Examined != 0 {
		t.Fatalf("no changes: %+v err=%v, want a no-op", st, err)
	}
}

func TestLookupBasePositions_compositeIntegerKey(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `w` smallint NOT NULL,\n  `d` bigint unsigned NOT NULL,\n  `v` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`w`,`d`)\n) ENGINE=InnoDB;\n"
	base, pkCols := writeKeyedBaseline(t, ddl, []string{"w", "d"}, [][]string{
		{"1", "1", "a"}, {"1", "2", "b"}, {"2", "1", "c"}, {"2", "18446744073709551615", "d"}, {"-3", "0", "e"},
	})
	if len(pkCols) != 2 || pkCols[0].Name != "w" {
		t.Fatalf("pk cols = %+v", pkCols)
	}
	joined, got, _ := bothWays(t, base, pkCols, touched("1|2", "2|18446744073709551615", "-3|0", "1|99"))
	if !joined {
		t.Fatal("composite integer key did not take the join")
	}
	if want := []int64{1, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("positions = %v, want %v", got, want)
	}
}

// TestLookupBasePositions_fallsBackWhenAKeyCannotBeSpelledAsInteger: a key
// the join could not hand to DuckDB exactly sends the WHOLE lookup back to
// the scan, never a partial join. Values the column cannot hold are dropped
// from the join instead (no base row can carry them).
func TestLookupBasePositions_fallsBackWhenAKeyCannotBeSpelledAsInteger(t *testing.T) {
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	for _, odd := range []string{"abc", "1|2", "", " 2", "+2", "2.0", `2\|`, "007", "-0", "02"} {
		joined, got, _ := bothWays(t, base, pkCols, touched("1", odd))
		if joined {
			t.Errorf("key %q: took the join", odd)
		}
		if !reflect.DeepEqual(got, []int64{0}) {
			t.Errorf("key %q: positions = %v", odd, got)
		}
	}
	// Out of the column's range: not a spelling problem, just no such row.
	joined, got, _ := bothWays(t, base, pkCols, touched("3", "5000000000", "-5000000000"))
	if !joined || !reflect.DeepEqual(got, []int64{2}) {
		t.Fatalf("out-of-range keys: joined=%v positions=%v, want the join and [2]", joined, got)
	}
	// Every key out of range: nothing to join, nothing found, no error.
	joined, got, _ = bothWays(t, base, pkCols, touched("5000000000"))
	if !joined || len(got) != 0 {
		t.Fatalf("all out of range: joined=%v positions=%v", joined, got)
	}
}

func TestLookupBasePositions_nonIntegerKeysKeepTheScan(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `code` varchar(8) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`code`)\n) ENGINE=InnoDB;\n"
	base, pkCols := writeKeyedBaseline(t, ddl, []string{"code"}, [][]string{{"a", "1"}, {"7", "2"}, {"c", "3"}})
	joined, got, _ := bothWays(t, base, pkCols, touched("7", "c"))
	if joined {
		t.Fatal("a varchar key took the join (a digit-only string is still text)")
	}
	if !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("positions = %v", got)
	}
	// The zoo base (varchar PK beside a BIGINT UNSIGNED column, #1155's
	// shapes): the scan, as before.
	rows, nulls := zooRows()
	zoo := writeZooBaseline(t, rows, nulls)
	zooPK := []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"}}
	joined, got, _ = bothWays(t, zoo, zooPK, touched("2", "3"))
	if !joined || !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("zoo int id: joined=%v positions=%v", joined, got)
	}
}

// TestLookupBasePositions_quotedColumnNames: an identifier that needs quoting
// on both sides of the join and in the CSV column map.
func TestLookupBasePositions_quotedColumnNames(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `order` int NOT NULL,\n  `it's` int NOT NULL,\n  PRIMARY KEY (`order`,`it's`)\n) ENGINE=InnoDB;\n"
	base, pkCols := writeKeyedBaseline(t, ddl, []string{"order", "it's"}, [][]string{{"1", "1"}, {"1", "2"}, {"2", "2"}})
	joined, got, _ := bothWays(t, base, pkCols, touched("1|2", "2|2"))
	if !joined || !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("quoted names: joined=%v positions=%v", joined, got)
	}
}

func TestLookupBasePositions_manyKeys(t *testing.T) {
	var rows [][]string
	for i := range 20000 {
		rows = append(rows, []string{fmt.Sprint(i), "x"})
	}
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, rows)
	var keys []string
	for i := 0; i < 20000; i += 3 {
		keys = append(keys, fmt.Sprint(i))
	}
	keys = append(keys, "20001", "-1")
	joined, got, _ := bothWays(t, base, pkCols, touched(keys...))
	if !joined || len(got) != 6667 || got[0] != 0 || got[6666] != 19998 {
		t.Fatalf("many keys: joined=%v n=%d", joined, len(got))
	}
}

// TestLookupBasePositions_everyIntegerWidth: the widths the writer produces
// (tinyint/year → INTEGER, int unsigned → BIGINT, bigint unsigned → UBIGINT)
// all take the join, with values at their edges.
func TestLookupBasePositions_everyIntegerWidth(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `a` tinyint NOT NULL,\n  `b` int unsigned NOT NULL,\n  `c` year NOT NULL,\n  `d` mediumint NOT NULL,\n  PRIMARY KEY (`a`,`b`,`c`,`d`)\n) ENGINE=InnoDB;\n"
	base, pkCols := writeKeyedBaseline(t, ddl, []string{"a", "b", "c", "d"}, [][]string{
		{"-128", "4294967295", "2026", "-8388608"}, {"127", "0", "1901", "8388607"}, {"0", "7", "2000", "0"},
	})
	joined, got, _ := bothWays(t, base, pkCols, touched("-128|4294967295|2026|-8388608", "0|7|2000|0", "127|1|1901|8388607"))
	if !joined || !reflect.DeepEqual(got, []int64{0, 2}) {
		t.Fatalf("widths: joined=%v positions=%v", joined, got)
	}
}

// TestLookupBasePositions_nullKeyIsRefusedOnBothPaths: a base whose key
// column holds NULL (a pre-#522 baseline that lost an unsigned value) is
// refused by the scan when it reaches the row; the join, which would never
// reach it, must refuse the same way rather than publish the row twice.
func TestLookupBasePositions_nullKeyIsRefusedOnBothPaths(t *testing.T) {
	cols, err := baseline.ParseSchemaText(intKeyDDL)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(t.TempDir(), "t.parquet")
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 7})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{{"1", "a"}, {"", "lost"}, {"3", "c"}} {
		if err := w.WriteRow(r, []bool{r[0] == "", false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	pkCols := pkColsIntID()
	// The scan refuses when it reaches the row; the join refuses from its
	// own probe, BEFORE any early return — including the window whose keys
	// are all out of range (the realistic shape: every new id of an
	// auto-increment table past 2^31 is), where there is nothing to join.
	for _, keys := range [][]string{{"3", "3000000000"}, {"3000000000"}} {
		for _, allow := range []bool{false, true} {
			_, st, err := lookupBasePositionsWith(context.Background(), base, "s", "t", pkCols, touched(keys...), duckdbutil.DefaultTuning(), allow)
			if err == nil || !strings.Contains(err.Error(), "NULL") || st.Joined != allow {
				t.Fatalf("keys %v allowJoin=%v: err=%v joined=%v, want the NULL-key refusal from that path", keys, allow, err, st.Joined)
			}
			if allow && !strings.Contains(err.Error(), "holds NULL in the base file") {
				t.Fatalf("keys %v: the join did not refuse from its own probe: %v", keys, err)
			}
		}
	}
	// NULL in the second column of a composite key is refused the same way.
	ddl := "CREATE TABLE `t` (\n  `w` smallint NOT NULL,\n  `d` bigint unsigned NOT NULL,\n  `v` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`w`,`d`)\n) ENGINE=InnoDB;\n"
	cols2, err := baseline.ParseSchemaText(ddl)
	if err != nil {
		t.Fatal(err)
	}
	base2 := filepath.Join(t.TempDir(), "c.parquet")
	w2, err := baseline.NewWriter(base2, cols2, baseline.WriterConfig{Compression: "none", RowGroupSize: 7})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{{"1", "1", "a"}, {"1", "", "lost"}, {"2", "2", "c"}} {
		if err := w2.WriteRow(r, []bool{false, r[1] == "", false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	pk2 := []metadata.ColumnMeta{{Name: "w", OrdinalPosition: 1, IsPK: true, DataType: "smallint"}, {Name: "d", OrdinalPosition: 2, IsPK: true, DataType: "bigint"}}
	_, st, err := lookupBasePositionsWith(context.Background(), base2, "s", "t", pk2, touched("1|1"), duckdbutil.DefaultTuning(), true)
	if err == nil || !st.Joined || !strings.Contains(err.Error(), `key column "d" holds NULL`) {
		t.Fatalf("composite key with NULL in the second column must name that column: err=%v joined=%v", err, st.Joined)
	}
}

// TestLookupBasePositions_emptyBase: nothing to match, nothing refused.
func TestLookupBasePositions_emptyBase(t *testing.T) {
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, nil)
	joined, got, _ := bothWays(t, base, pkCols, touched("1", "2"))
	if !joined || len(got) != 0 {
		t.Fatalf("empty base: joined=%v positions=%v", joined, got)
	}
}

// TestLookupBasePositions_unwritableTempDirCostsTheJoinNotTheRefresh: the
// touched-keys file is what the join needs and the scan does not; when it
// cannot be written the lookup scans, with the right answer and no error.
func TestLookupBasePositions_unwritableTempDirCostsTheJoinNotTheRefresh(t *testing.T) {
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "does", "not", "exist"))
	got, st, err := lookupBasePositionsWith(context.Background(), base, "s", "t", pkCols, touched("2"), duckdbutil.DefaultTuning(), true)
	if err != nil || st.Joined || !reflect.DeepEqual(got, []int64{1}) || st.Examined != 3 {
		t.Fatalf("unwritable temp dir: err=%v st=%+v got=%v, want the scan's answer with no error", err, st, got)
	}
}

// TestLookupBasePositions_timing is the measurement behind the change, not a
// pass/fail: BINTRAIL_MEASURE_1716=1 writes a 5 M-row integer-keyed base and
// times both paths for a 100 k-key window.
func TestLookupBasePositions_timing(t *testing.T) {
	if os.Getenv("BINTRAIL_MEASURE_1716") == "" {
		t.Skip("set BINTRAIL_MEASURE_1716=1 to run")
	}
	const n = 5_000_000
	rows := make([][]string, n)
	for i := range rows {
		rows[i] = []string{fmt.Sprint(i), "x"}
	}
	base, pkCols := writeKeyedBaselineGroups(t, intKeyDDL, []string{"id"}, rows, 100_000)
	keys := make([]string, 0, 100_000)
	for i := 0; i < n; i += n / 100_000 {
		keys = append(keys, fmt.Sprint(i))
	}
	ch := touched(keys...)
	for _, allow := range []bool{false, true} {
		start := time.Now()
		got, st, err := lookupBasePositionsWith(context.Background(), base, "s", "t", pkCols, ch, duckdbutil.DefaultTuning(), allow)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("join=%v: %d positions, %d rows into Go, %s", st.Joined, len(got), st.Examined, time.Since(start))
	}
}
