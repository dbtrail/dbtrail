package reconstruct

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// TestCheckBaselineSchemaCurrent_nullabilityChange is #1665 at the guard: the
// emitted snapshot carries the baseline's CREATE TABLE forward, and a restore
// loads it, so a column whose NULL-ness moved since the baseline refuses like a
// type change does. A column made nullable and then set to NULL otherwise
// publishes a backup that fails at load with "Column cannot be null".
func TestCheckBaselineSchemaCurrent_nullabilityChange(t *testing.T) {
	createSQL := "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `c` int NOT NULL,\n" +
		"  `note` varchar(32) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	col := func(name, columnType, nullable string) metadata.ColumnMeta {
		dataType := columnType
		if n := strings.IndexAny(dataType, "( "); n >= 0 {
			dataType = dataType[:n]
		}
		return metadata.ColumnMeta{Name: name, DataType: dataType, ColumnType: columnType, IsNullable: nullable}
	}
	unchanged := func() []metadata.ColumnMeta {
		return []metadata.ColumnMeta{col("id", "int", "NO"), col("c", "int", "NO"), col("note", "varchar(32)", "YES")}
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
		{"same NULL-ness", unchanged(), ""},
		{"a NOT NULL column now allows NULL", with(1, col("c", "int", "YES")), "type changed since: c (int NOT NULL -> int NULL)"},
		{"a nullable column is now NOT NULL", with(2, col("note", "varchar(32)", "NO")), "type changed since: note (varchar(32) NULL -> varchar(32) NOT NULL)"},
		{"type and NULL-ness changed are one entry", with(1, col("c", "bigint", "YES")), "type changed since: c (int NOT NULL -> bigint NULL)"},
		{"the snapshot does not know the NULL-ness", with(1, col("c", "int", "")), ""},
		{"lowercase from the snapshot", with(1, col("c", "int", "yes")), "c (int NOT NULL -> int NULL)"},
		{"no type in the snapshot, NULL-ness changed", with(1, metadata.ColumnMeta{Name: "c", IsNullable: "YES"}), "c (NOT NULL -> NULL)"},
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
				t.Errorf("refusal is not tagged ErrSchemaChanged: %v", err)
			}
			if n := strings.Count(err.Error(), " -> "); n != 1 {
				t.Errorf("the refusal lists %d changes, want 1: %v", n, err)
			}
		})
	}

	t.Run("compared only against the snapshot in effect after the baseline", func(t *testing.T) {
		tm := &metadata.TableMeta{Schema: "mydb", Table: "orders", Columns: with(1, col("c", "int", "YES"))}
		if err := checkBaselineSchemaCurrent(createSQL, tm, nil, "mydb", "orders"); err != nil {
			t.Errorf("no snapshot in effect: refused on the latest snapshot's NULL-ness: %v", err)
		}
	})
	t.Run("the Iceberg export does not compare it", func(t *testing.T) {
		tm := &metadata.TableMeta{Schema: "mydb", Table: "orders", Columns: with(1, col("c", "int", "YES"))}
		if err := CheckBaselineSchemaCurrent(createSQL, tm, "mydb", "orders"); err != nil {
			t.Errorf("the export refused a NULL-ness change it never publishes: %v", err)
		}
	})
}
