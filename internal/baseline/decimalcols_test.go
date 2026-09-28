package baseline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestParseSchema_decimalPrecisionScale pins the (p,s) a decimal column was
// declared with, including MySQL's own defaults for the shorter spellings.
// Nothing about how the value is STORED changes — see MysqlToParquetNode — but
// a consumer that wants to hand the number back to an engine has to know which
// number it is.
func TestParseSchema_decimalPrecisionScale(t *testing.T) {
	createSQL := "CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `amount` decimal(6,2) DEFAULT NULL,\n" +
		"  `rate` numeric(12, 4) DEFAULT NULL,\n" +
		"  `qty` decimal(8) DEFAULT NULL,\n" +
		"  `bare` decimal DEFAULT NULL,\n" +
		"  `huge` decimal(65,30) DEFAULT NULL,\n" +
		"  `name` varchar(64) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"

	cols, err := ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	byName := map[string]Column{}
	for _, c := range cols {
		byName[c.Name] = c
	}

	want := map[string]struct{ precision, scale int }{
		"amount": {6, 2},
		"rate":   {12, 4},
		// MySQL's own defaults: DECIMAL(p) is (p,0) and a bare DECIMAL is (10,0).
		"qty":  {8, 0},
		"bare": {10, 0},
		"huge": {65, 30},
	}
	for name, w := range want {
		got, ok := byName[name]
		if !ok {
			t.Fatalf("column %q missing from the parsed schema", name)
		}
		if got.DecimalPrecision != w.precision || got.DecimalScale != w.scale {
			t.Errorf("column %q: got DECIMAL(%d,%d), want DECIMAL(%d,%d)",
				name, got.DecimalPrecision, got.DecimalScale, w.precision, w.scale)
		}
	}

	// A non-decimal column carries no precision. varchar(64) would otherwise
	// pick up a 64 from the same parenthesized args, and DecimalColumns would
	// then be the only thing standing between that and a nonsense cast.
	if c := byName["name"]; c.DecimalPrecision != 0 || c.DecimalScale != 0 {
		t.Errorf("varchar column carries DECIMAL(%d,%d), want no precision at all",
			c.DecimalPrecision, c.DecimalScale)
	}
	if c := byName["id"]; c.DecimalPrecision != 0 || c.DecimalScale != 0 {
		t.Errorf("int column carries DECIMAL(%d,%d), want no precision at all",
			c.DecimalPrecision, c.DecimalScale)
	}

	// DecimalColumns reports exactly the decimal family, in schema order.
	decs := DecimalColumns(cols)
	var names []string
	for _, d := range decs {
		names = append(names, d.Name)
	}
	wantNames := []string{"amount", "rate", "qty", "bare", "huge"}
	if len(names) != len(wantNames) {
		t.Fatalf("DecimalColumns returned %v, want %v", names, wantNames)
	}
	for i := range names {
		if names[i] != wantNames[i] {
			t.Fatalf("DecimalColumns returned %v, want %v", names, wantNames)
		}
	}
}

// TestTableFootersFor_readsTheEmbeddedSchema drives the real writer and the
// real footer read: the precision has to survive being written into the Parquet
// key-value metadata and read back out of it, which is the round trip
// `bintrail views` depends on.
func TestTableFootersFor_readsTheEmbeddedSchema(t *testing.T) {
	dir := t.TempDir()
	createSQL := "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `total` decimal(10,2) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	withDecimals := filepath.Join(dir, "orders.parquet")
	writeFixtureTable(t, withDecimals, createSQL, [][]string{{"1", "10.50"}})

	plainSQL := "CREATE TABLE `tags` (\n  `id` int NOT NULL,\n  `label` varchar(32) DEFAULT NULL\n);\n"
	noDecimals := filepath.Join(dir, "tags.parquet")
	writeFixtureTable(t, noDecimals, plainSQL, [][]string{{"1", "hi"}})

	// A baseline written before the CREATE TABLE was embedded in the footer.
	// Its schema cannot be read at all, which is a different fact from having
	// no decimal columns, and the caller has to be able to tell them apart.
	noSchema := filepath.Join(dir, "legacy.parquet")
	writeFixtureTableNoSchemaMeta(t, noSchema, plainSQL, [][]string{{"1", "hi"}})

	got, err := TableFootersFor(context.Background(), []string{withDecimals, noDecimals, noSchema})
	if err != nil {
		t.Fatalf("TableFootersFor: %v", err)
	}
	decs := got[withDecimals].Decimals
	if len(decs) != 1 {
		t.Fatalf("got %d decimal columns for %s, want 1 (%v)", len(decs), withDecimals, got)
	}
	if decs[0].Name != "total" || decs[0].Precision != 10 || decs[0].Scale != 2 {
		t.Errorf("got %+v, want {total 10 2}", decs[0])
	}

	// Schema read, nothing to cast: PRESENT and empty.
	plainFooter, ok := got[noDecimals]
	plain := plainFooter.Decimals
	if !ok {
		t.Errorf("a table whose schema WAS read must be present even with no decimal columns; "+
			"absence is how the caller reports that it could not look: %v", got)
	}
	if len(plain) != 0 {
		t.Errorf("got %v decimal columns for a table that has none", plain)
	}

	// Schema not readable: ABSENT.
	if _, ok := got[noSchema]; ok {
		t.Errorf("a baseline with no embedded CREATE TABLE must be absent, not reported as "+
			"having no decimal columns: %v", got)
	}
}

// writeFixtureTableNoSchemaMeta writes a baseline Parquet WITHOUT the embedded
// CREATE TABLE, the shape a baseline taken before that footer key existed has.
func writeFixtureTableNoSchemaMeta(t *testing.T, path, createSQL string, rows [][]string) {
	t.Helper()
	cols, err := ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	w, err := NewWriter(path, cols, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// writeFixtureTable writes a one-table baseline Parquet through the real writer,
// with the CREATE TABLE embedded in the footer exactly as the dump path does.
func writeFixtureTable(t *testing.T, path, createSQL string, rows [][]string) {
	t.Helper()
	cols, err := ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	w, err := NewWriter(path, cols, WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata:     map[string]string{MetaKeyCreateTableSQL: createSQL},
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestTableFootersFor_corruptSiblingKeepsOthersCast pins the blast radius of
// one unreadable file.
//
// DuckDB resolves a parquet_kv_metadata() file list up front, so a single
// truncated or zero-byte file fails the whole batched read. Letting that stand
// would mean one bad file in a snapshot costs EVERY table in it its casts,
// which turns a local fault into a layout-wide one for no reason: the other
// files' footers are perfectly readable.
func TestTableFootersFor_corruptSiblingKeepsOthersCast(t *testing.T) {
	dir := t.TempDir()
	createSQL := "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `total` decimal(10,2) DEFAULT NULL\n" +
		");\n"
	good := filepath.Join(dir, "good.parquet")
	writeFixtureTable(t, good, createSQL, [][]string{{"1", "1.00"}})

	// Zero bytes: what a truncated upload or an interrupted write leaves.
	corrupt := filepath.Join(dir, "corrupt.parquet")
	if err := os.WriteFile(corrupt, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := TableFootersFor(context.Background(), []string{good, corrupt})
	if err != nil {
		t.Fatalf("one unreadable file must not fail the whole read: %v", err)
	}
	if len(got[good].Decimals) != 1 || got[good].Decimals[0].Name != "total" {
		t.Errorf("the readable file lost its casts to a corrupt sibling: got %v", got)
	}
	// The corrupt one is absent, which is how the caller reports "could not look".
	if _, ok := got[corrupt]; ok {
		t.Errorf("an unreadable file must be absent, not reported as having no decimals: %v", got)
	}
}

// TestDecimalPrecisionScale_refusesMalformedArgs is about a silent wrong
// answer, not a missing one.
//
// MySQL's defaults are right for the spellings that OMIT the arguments: a bare
// `decimal` really is (10,0). Applying them to arguments that are PRESENT and
// unparseable is a guess, and it is not an inert one. DuckDB truncates a cast
// to a narrower scale by ROUNDING, silently: CAST('1.239' AS DECIMAL(10,1))
// returns 1.2 with no error. So a footer reading `decimal(10,2x)` that fell
// back to (10,0) would emit CAST(total AS DECIMAL(10,0)) and report every
// money value rounded to whole units, in a view whose entire purpose is that
// money arithmetic can be trusted.
//
// Refusing the column instead leaves it as text, which is what it reads as
// today and what the caller already knows how to report.
func TestDecimalPrecisionScale_refusesMalformedArgs(t *testing.T) {
	cases := []struct {
		name, createSQL string
		wantRefused     bool
	}{
		{"garbage scale", "  `c` decimal(10,2x) DEFAULT NULL,", true},
		{"garbage precision", "  `c` decimal(x,2) DEFAULT NULL,", true},
		{"empty precision", "  `c` decimal(,2) DEFAULT NULL,", true},
		{"three parts", "  `c` decimal(10,2,3) DEFAULT NULL,", true},
		// The spellings that legitimately omit arguments keep MySQL's defaults.
		{"bare", "  `c` decimal DEFAULT NULL,", false},
		{"precision only", "  `c` decimal(8) DEFAULT NULL,", false},
		{"both", "  `c` decimal(10,2) DEFAULT NULL,", false},
		{"spaced", "  `c` decimal( 12 , 4 ) DEFAULT NULL,", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols, err := ParseSchemaText("CREATE TABLE `t` (\n" + tc.createSQL + "\n);\n")
			if err != nil {
				t.Fatalf("ParseSchemaText: %v", err)
			}
			c := cols[0]
			refused := c.DecimalPrecision == 0 && c.DecimalScale == 0
			if refused != tc.wantRefused {
				t.Errorf("%q gave DECIMAL(%d,%d); refused=%v, want refused=%v",
					tc.createSQL, c.DecimalPrecision, c.DecimalScale, refused, tc.wantRefused)
			}
		})
	}
}

// TestTableFootersFor_marksDeltaReservedColumns: a table with a column a table delta
// reserves is marked, by the same case-insensitive rule the writer refuses
// its delta with, so the state views keep reading it without the chain-aware
// SQL that does not bind over such a table (#1733). Every reserved name is
// covered, and an ordinary table is not marked.
func TestTableFootersFor_marksDeltaReservedColumns(t *testing.T) {
	dir := t.TempDir()
	var paths []string
	for i, name := range append(append([]string{}, TableDeltaReservedColumns...), "FileName") {
		p := filepath.Join(dir, fmt.Sprintf("t%d.parquet", i))
		writeFixtureTable(t, p, "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `"+name+"` varchar(8) DEFAULT NULL\n);\n", [][]string{{"1", "x"}})
		paths = append(paths, p)
	}
	ordinary := filepath.Join(dir, "ordinary.parquet")
	writeFixtureTable(t, ordinary, "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `file_name` varchar(8) DEFAULT NULL\n);\n", [][]string{{"1", "x"}})
	got, err := TableFootersFor(context.Background(), append(paths, ordinary))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		if f, ok := got[p]; !ok || !f.DeltaReserved {
			t.Errorf("%s: footer %+v (present=%v), want DeltaReserved", p, f, ok)
		}
	}
	if f, ok := got[ordinary]; !ok || f.DeltaReserved {
		t.Errorf("an ordinary table was marked: %+v (present=%v)", f, ok)
	}
}
