package reconstruct

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// TestCheckBaselineSchemaCurrent_typeChange is #1651 at the guard: a column
// whose TYPE moved since the baseline refuses the refresh, because the emitted
// snapshot would carry the baseline's CREATE TABLE forward with the old type.
// The cases that must NOT refuse matter as much: a display width that only a
// server upgrade removed, and a snapshot too old (or too foreign) to know the
// type, which compares the base type word only, or nothing.
func TestCheckBaselineSchemaCurrent_typeChange(t *testing.T) {
	createSQL := "CREATE TABLE `orders` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `c` int DEFAULT NULL,\n" +
		"  `amount` decimal(10,2) DEFAULT NULL,\n" +
		"  `note` varchar(32) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	full := func(name, columnType string) metadata.ColumnMeta {
		dataType := columnType
		if n := strings.IndexAny(dataType, "( "); n >= 0 {
			dataType = dataType[:n]
		}
		return metadata.ColumnMeta{Name: name, DataType: dataType, ColumnType: columnType}
	}
	unchanged := func() []metadata.ColumnMeta {
		return []metadata.ColumnMeta{full("id", "int"), full("c", "int"), full("amount", "decimal(10,2)"), full("note", "varchar(32)")}
	}
	with := func(i int, c metadata.ColumnMeta) []metadata.ColumnMeta {
		cols := unchanged()
		cols[i] = c
		return cols
	}

	for _, tc := range []struct {
		name    string
		current []metadata.ColumnMeta
		wantErr string // "" = must not refuse
	}{
		// Control from the issue: int(11) in the baseline against int in the
		// current schema is the same column.
		{"display width only", unchanged(), ""},
		{"int widened to bigint", with(1, full("c", "bigint")), "type changed since: c (int -> bigint)"},
		{"signedness changed", with(1, full("c", "int unsigned")), "c (int -> int unsigned)"},
		{"decimal scale changed", with(2, full("amount", "decimal(12,4)")), "amount (decimal(10,2) -> decimal(12,4))"},
		{"in-place conversion varchar to int", with(3, full("note", "int")), "note (varchar(32) -> int)"},
		// Widenings keep the Parquet value but not the carried CREATE TABLE a
		// restore loads: a 40-character value refuses to load into VARCHAR(32).
		{"varchar length changed", with(3, full("note", "varchar(64)")), "note (varchar(32) -> varchar(64))"},
		{"varchar widened to text", with(3, full("note", "text")), "note (varchar(32) -> text)"},
		{"text family to binary", with(3, full("note", "varbinary(32)")), "note (varchar(32) -> varbinary(32))"},
		{"column name case differs, type same", with(1, full("C", "int")), ""},
		{
			"two columns changed are both named, in column order",
			[]metadata.ColumnMeta{full("id", "bigint"), full("c", "bigint"), full("amount", "decimal(10,2)"), full("note", "varchar(32)")},
			"type changed since: id (int(11) -> bigint), c (int -> bigint)",
		},
		{
			// Pre-#212 snapshot: DATA_TYPE only. The token still moved.
			"no column_type, data_type moved",
			with(1, metadata.ColumnMeta{Name: "c", DataType: "bigint"}),
			"c (int -> bigint)",
		},
		{
			// DATA_TYPE carries no length or scale: not knowable, not a change.
			"no column_type, data_type same token",
			with(2, metadata.ColumnMeta{Name: "amount", DataType: "decimal"}),
			"",
		},
		{
			// PostgreSQL snapshots carry neither.
			"no type at all",
			with(1, metadata.ColumnMeta{Name: "c"}),
			"",
		},
		{
			"added column and changed type are reported together",
			append(with(1, full("c", "bigint")), full("extra", "int")),
			"added since: extra; gone since: none; type changed since: c (int -> bigint)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm := &metadata.TableMeta{Schema: "mydb", Table: "orders", Columns: tc.current}
			err := checkBaselineSchemaCurrent(createSQL, tm, tm, "mydb", "orders")
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected refusal: %v", err)
			case tc.wantErr == "":
				return
			case err == nil:
				t.Fatalf("expected a refusal containing %q", tc.wantErr)
			case !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
			if !isSchemaChanged(err) {
				t.Errorf("refusal is not tagged ErrSchemaChanged, so baseline refresh would not report refused-ddl: %v", err)
			}
		})
	}
}

// TestCheckBaselineSchemaCurrent_exportComparesNamesOnly: the Iceberg export
// shares the column-name check but not the type check. It does not publish
// the carried CREATE TABLE and checks the types it exports itself, so an ENUM
// relabel (the shape its per-epoch decoding test uses) must not refuse there,
// while a dropped column still does.
func TestCheckBaselineSchemaCurrent_exportComparesNamesOnly(t *testing.T) {
	createSQL := "CREATE TABLE `n` (\n  `id` int NOT NULL,\n  `status` enum('new','paid') DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	relabeled := &metadata.TableMeta{Schema: "s", Table: "n", Columns: []metadata.ColumnMeta{
		{Name: "id", DataType: "int", ColumnType: "int"},
		{Name: "status", DataType: "enum", ColumnType: "enum('paid','new')"},
	}}
	if err := CheckBaselineSchemaCurrent(createSQL, relabeled, "s", "n"); err != nil {
		t.Errorf("the export refused an ENUM relabel it decodes per epoch: %v", err)
	}
	if err := checkBaselineSchemaCurrent(createSQL, relabeled, relabeled, "s", "n"); err == nil || !isSchemaChanged(err) {
		t.Errorf("the fold did not refuse the same relabel, which it would carry forward under the old CREATE TABLE: %v", err)
	}
	dropped := &metadata.TableMeta{Schema: "s", Table: "n", Columns: []metadata.ColumnMeta{{Name: "id", DataType: "int", ColumnType: "int"}}}
	if err := CheckBaselineSchemaCurrent(createSQL, dropped, "s", "n"); err == nil || !isSchemaChanged(err) {
		t.Errorf("the export no longer refuses a dropped column: %v", err)
	}
}

// TestCheckBaselineSchemaCurrent_mixedCaseColumn: MySQL column names are case
// insensitive, and the baseline and the snapshot can spell one differently.
// The lookup must fold both sides, or a same-type column reads as added and
// gone, and a real type change on it is missed.
func TestCheckBaselineSchemaCurrent_mixedCaseColumn(t *testing.T) {
	createSQL := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `Amount` decimal(10,2) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	for _, tc := range []struct {
		name, col, columnType, wantErr string
	}{
		{"same spelling, same type", "Amount", "decimal(10,2)", ""},
		{"other spelling, same type", "amount", "decimal(10,2)", ""},
		{"same spelling, type changed", "Amount", "decimal(12,4)", "Amount (decimal(10,2) -> decimal(12,4))"},
		{"other spelling, type changed", "AMOUNT", "decimal(12,4)", "AMOUNT (decimal(10,2) -> decimal(12,4))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm := &metadata.TableMeta{Schema: "mydb", Table: "orders", Columns: []metadata.ColumnMeta{
				{Name: "id", DataType: "int", ColumnType: "int"},
				{Name: tc.col, DataType: "decimal", ColumnType: tc.columnType},
			}}
			err := checkBaselineSchemaCurrent(createSQL, tm, tm, "mydb", "orders")
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected refusal: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || strings.Contains(err.Error(), "added since: amount") {
				t.Fatalf("err = %v, want only the type change %q", err, tc.wantErr)
			}
		})
	}
}

// TestCheckBaselineSchemaCurrent_typesAgainstTheSnapshotInEffect: names come
// from the latest snapshot, types only from the one the caller passes (the
// snapshot in effect at the target, taken after the baseline), and nil
// compares no types. A later type change must not refuse a fold that stops
// before it.
func TestCheckBaselineSchemaCurrent_typesAgainstTheSnapshotInEffect(t *testing.T) {
	createSQL := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `c` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	meta := func(ctype string) *metadata.TableMeta {
		return &metadata.TableMeta{Schema: "s", Table: "t", Columns: []metadata.ColumnMeta{
			{Name: "id", DataType: "int", ColumnType: "int"}, {Name: "c", DataType: ctype, ColumnType: ctype}}}
	}
	latest := meta("bigint")
	if err := checkBaselineSchemaCurrent(createSQL, latest, nil, "s", "t"); err != nil {
		t.Errorf("no snapshot in effect after the baseline: refused on the latest snapshot's type: %v", err)
	}
	if err := checkBaselineSchemaCurrent(createSQL, latest, meta("int"), "s", "t"); err != nil {
		t.Errorf("the snapshot in effect still says int: refused on a later change: %v", err)
	}
	if err := checkBaselineSchemaCurrent(createSQL, meta("int"), meta("bigint"), "s", "t"); err == nil || !strings.Contains(err.Error(), "c (int -> bigint)") {
		t.Errorf("the snapshot in effect says bigint: err = %v", err)
	}
}
