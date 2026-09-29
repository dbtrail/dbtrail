package baseline

import (
	"bytes"
	"testing"
)

// A VECTOR column holds packed 32-bit floats, not text. mydumper dumps it as
// `_binary "…"` (MariaDB 11.7+, measured with mydumper 1.0.3 and 1.0.5) or as
// 0x… under --hex-blob, and those bytes are almost never valid UTF-8. Stored
// in the STRING default, one such value made DuckDB refuse the whole Parquet
// file ("Invalid string encoding"), so every read of the table failed:
// reconstruct, drill, verify and the fold.
func TestVectorIsBinary(t *testing.T) {
	cols, err := ParseSchemaText("CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `v` vector(3) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n")
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	if len(cols) != 2 || cols[1].MySQLType != "vector" {
		t.Fatalf("columns = %+v, want id and a vector column", cols)
	}
	blob := MysqlToParquetNode2("blob", false)
	node := cols[1].ParquetType
	if node.Type().Kind() != blob.Type().Kind() ||
		(node.Type().LogicalType() == nil) != (blob.Type().LogicalType() == nil) {
		t.Errorf("vector column node is not a binary leaf like blob (kind %v, logical %v)",
			node.Type().Kind(), node.Type().LogicalType())
	}

	packed := []byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x20, 0x40, 0x00, 0x00, 0x40, 0xc0} // [1,2.5,-3]
	for _, raw := range []string{string(packed), "0x0000803F00002040000040C0"} {
		v, err := convertValue(cols[1], raw)
		if err != nil {
			t.Fatalf("convertValue(%q): %v", raw, err)
		}
		if got := v.ByteArray(); !bytes.Equal(got, packed) {
			t.Errorf("convertValue(%q) stored %x, want the packed floats %x", raw, got, packed)
		}
	}
}
