package reconstruct

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// A MariaDB UUID/INET4/INET6 primary key has two spellings: the index keys
// events by the captured bytes (metadata.MapRow pads them to full width, and
// event.BuildPKValues spells them), while the baseline row and the person
// typing --pk carry the text form. Before, the canonicalizer refused these
// types (full-table reconstruct failed for the whole table), and single-row
// reconstruct matched the baseline row by text but fetched NO events for the
// key, so it returned the snapshot-era row as the state at --at: silent and
// wrong.

// capturedPK is what the index stores for a row whose PK columns hold these
// hex byte values: the same padding MapRow applies, spelled by BuildPKValues.
func capturedPK(t *testing.T, cols []metadata.ColumnMeta, hexes ...string) string {
	t.Helper()
	row := map[string]any{}
	for i, c := range cols {
		b, err := hex.DecodeString(hexes[i])
		if err != nil {
			t.Fatal(err)
		}
		row[c.Name] = b
	}
	return event.BuildPKValues(cols, row)
}

var mariaDBFixedPKCases = []struct {
	name  string
	cols  []metadata.ColumnMeta
	text  []string // the baseline's text, one per column
	typed string   // what a person may type for --pk
	hexes []string
}{
	{"nil UUID (all NUL bytes, valid UTF-8 → stored verbatim)",
		[]metadata.ColumnMeta{colMeta("u", "uuid", "uuid")},
		[]string{"00000000-0000-0000-0000-000000000000"}, "00000000000000000000000000000000",
		[]string{"00000000000000000000000000000000"}},
	{"all-ones UUID (not UTF-8 → stored as 0x hex)",
		[]metadata.ColumnMeta{colMeta("u", "uuid", "uuid")},
		[]string{"ffffffff-ffff-ffff-ffff-ffffffffffff"}, "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
		[]string{"ffffffffffffffffffffffffffffffff"}},
	{"UUID whose bytes hold '|' and '\\' (escaped in pk_values)",
		[]metadata.ColumnMeta{colMeta("u", "uuid", "uuid")},
		[]string{"7c5c7c5c-5c7c-0000-0000-000000000000"}, "7C5C7C5C-5C7C-0000-0000-000000000000",
		[]string{"7c5c7c5c5c7c00000000000000000000"}},
	{"composite INET4 + INET6, trailing zero bytes",
		[]metadata.ColumnMeta{colMeta("a", "inet4", "inet4"), colMeta("b", "inet6", "inet6")},
		[]string{"124.92.0.0", "::"}, "124.92.0.0|0:0:0:0:0:0:0:0",
		[]string{"7c5c0000", "00000000000000000000000000000000"}},
	{"INET6 IPv4-mapped",
		[]metadata.ColumnMeta{colMeta("b", "inet6", "inet6")},
		[]string{"::ffff:1.2.3.4"}, "::FFFF:1.2.3.4",
		[]string{"00000000000000000000ffff01020304"}},
}

func TestCanonicalizePKMap_mariaDBFixedMatchesCapturedPK(t *testing.T) {
	for _, c := range mariaDBFixedPKCases {
		row := map[string]any{}
		for i, col := range c.cols {
			row[col.Name] = c.text[i] // DuckDB scans the baseline's text column as a Go string
		}
		canon, err := canonicalizePKMap(row, c.cols)
		if err != nil {
			t.Errorf("%s: canonicalizePKMap: %v", c.name, err)
			continue
		}
		got := event.BuildPKValues(c.cols, canon)
		if want := capturedPK(t, c.cols, c.hexes...); got != want {
			t.Errorf("%s: baseline key spelled %q, the index stores %q", c.name, got, want)
		}
	}
	// A baseline value that is not the type's text is not guessed at.
	if _, err := canonicalizePKValue("not-a-uuid", colMeta("u", "uuid", "uuid")); err == nil {
		t.Error("canonicalizePKValue accepted a value that is not a UUID")
	}
	if _, err := canonicalizePKValue(42, colMeta("u", "uuid", "uuid")); err == nil {
		t.Error("canonicalizePKValue accepted a non-string UUID")
	}
}

func TestIndexPKSpelling_mariaDBFixed(t *testing.T) {
	for _, c := range mariaDBFixedPKCases {
		want := capturedPK(t, c.cols, c.hexes...)
		if got := IndexPKSpelling(c.typed, c.cols); got != want {
			t.Errorf("%s: IndexPKSpelling(%q) = %q, the index stores %q", c.name, c.typed, got, want)
		}
	}
	// Not parseable: left as typed, so the lookup misses rather than hits
	// another row. The baseline read refuses such a value first.
	u := []metadata.ColumnMeta{colMeta("u", "uuid", "uuid")}
	if got := IndexPKSpelling("nope", u); got != "nope" {
		t.Errorf("unparseable UUID re-spelled to %q", got)
	}
}

func TestMariaDBFixedBaselineFilter(t *testing.T) {
	for _, c := range mariaDBFixedPKCases {
		filter := map[string]string{}
		typed := strings.Split(c.typed, "|")
		for i, col := range c.cols {
			filter[col.Name] = typed[i]
		}
		got, err := mariaDBFixedBaselineFilter(filter, c.cols)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		for i, col := range c.cols {
			if got[col.Name] != c.text[i] {
				t.Errorf("%s: filter %s = %q, want the baseline's text %q", c.name, col.Name, got[col.Name], c.text[i])
			}
		}
	}
	u := []metadata.ColumnMeta{colMeta("u", "uuid", "uuid"), colMeta("id", "int", "int")}
	if _, err := mariaDBFixedBaselineFilter(map[string]string{"u": "{nope}", "id": "1"}, u); err == nil {
		t.Error("an unparseable UUID must be refused, not looked up")
	}
	got, err := mariaDBFixedBaselineFilter(map[string]string{"u": "ffffffffffffffffffffffffffffffff", "id": "7"}, u)
	if err != nil || got["id"] != "7" || got["u"] != "ffffffff-ffff-ffff-ffff-ffffffffffff" {
		t.Errorf("mixed key: %v, %v", got, err)
	}
	if got, err := mariaDBFixedBaselineFilter(map[string]string{"u": "X"}, nil); err != nil || got["u"] != "X" {
		t.Errorf("no metas: the filter must pass through, got %v, %v", got, err)
	}
}

// --pk-columns is typed by a person: `U` names the column `u`, as it does on
// the server and in padFixedBinaryFilter.
func TestMariaDBFixedBaselineFilter_columnNameCase(t *testing.T) {
	got, err := mariaDBFixedBaselineFilter(map[string]string{"U": "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"},
		[]metadata.ColumnMeta{colMeta("u", "uuid", "uuid")})
	if err != nil || got["U"] != "ffffffff-ffff-ffff-ffff-ffffffffffff" {
		t.Errorf("filter under key U = %v, %v; want the canonical text under the same key", got, err)
	}
}

// CheckUntypedMariaDBFixedPK must not let a lookup through because it could
// not decide: a CREATE the schema parser cannot read (one line, say) still
// names the UUID key, and a footer that cannot be read is an error.
func TestCheckUntypedMariaDBFixedPK_undecidableCases(t *testing.T) {
	write := func(create string) string {
		p := filepath.Join(t.TempDir(), "t.parquet")
		cols := []baseline.Column{
			{Name: "u", MySQLType: "uuid", ParquetType: baseline.MysqlToParquetNode("uuid")},
		}
		w, err := baseline.NewWriter(p, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: create}})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow([]string{"00000000-0000-0000-0000-000000000001"}, []bool{false}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return p
	}
	filter := map[string]string{"u": "00000000-0000-0000-0000-000000000001"}
	ctx := context.Background()
	if err := CheckUntypedMariaDBFixedPK(ctx, write("CREATE TABLE t (u UUID PRIMARY KEY)"), filter, nil); err == nil {
		t.Error("a one-line CREATE naming a UUID key must still refuse")
	}
	if err := CheckUntypedMariaDBFixedPK(ctx, write("CREATE TABLE `t` (\n  `u` uuid NOT NULL,\n  PRIMARY KEY (`u`)\n);\n"), filter, nil); err == nil {
		t.Error("a UUID key without index types must refuse")
	}
	if err := CheckUntypedMariaDBFixedPK(ctx, filepath.Join(t.TempDir(), "missing.parquet"), filter, nil); err == nil {
		t.Error("an unreadable footer must be an error, not a pass")
	}
	if err := CheckUntypedMariaDBFixedPK(ctx, write("CREATE TABLE t (id INT PRIMARY KEY)"), map[string]string{"id": "1"}, nil); err != nil {
		t.Errorf("a plain key must pass: %v", err)
	}
	if err := CheckUntypedMariaDBFixedPK(ctx, "unused", filter, []metadata.ColumnMeta{colMeta("u", "uuid", "uuid")}); err != nil {
		t.Errorf("with index types there is nothing to check: %v", err)
	}
}
