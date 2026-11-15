package baseline

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/consistency"
)

// The schema file and data file headers mydumper 1.0.3 writes against MariaDB
// 11/12 and MySQL 8 with its stock /etc/mydumper.cnf: SET NAMES utf8mb4 on
// every file, whatever bytes the rows below it hold.
const (
	utf8Header   = "/*!40101 SET NAMES utf8mb4*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n"
	binaryHeader = "/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n"
	latin1Schema = utf8Header + "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `v` varchar(20) DEFAULT NULL,\n" +
		"  `u` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;\n"
)

type latin1Row struct {
	id   int
	v, u sql.NullString
}

// convertLatin1Table writes the schema and data files, converts them, and
// returns the rows DuckDB reads back, in id order, and the content digest.
func convertLatin1Table(t *testing.T, schema string, data ...string) ([]latin1Row, string) {
	t.Helper()
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "l1.t-schema.sql")
	if err := os.WriteFile(schemaPath, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	tf := TableFiles{Database: "l1", Table: "t", SchemaFile: schemaPath, Format: "sql"}
	for i, d := range data {
		p := filepath.Join(dir, "l1.t.0000"+string(rune('0'+i))+".sql")
		if err := os.WriteFile(p, []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
		tf.DataFiles = append(tf.DataFiles, p)
	}
	outPath := filepath.Join(dir, "t.parquet")
	if _, err := processTable(context.Background(), tf, outPath, WriterConfig{Compression: "none", RowGroupSize: 100}); err != nil {
		t.Fatalf("processTable: %v", err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(context.Background(),
		"SELECT id, v, u FROM parquet_scan('"+strings.ReplaceAll(outPath, "'", "''")+"') ORDER BY id")
	if err != nil {
		t.Fatalf("DuckDB must read the table's file: %v", err)
	}
	defer rows.Close()
	var got []latin1Row
	for rows.Next() {
		var r latin1Row
		if err := rows.Scan(&r.id, &r.v, &r.u); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("DuckDB must read the table's file: %v", err)
	}
	md, err := ReadParquetMetadata(outPath)
	if err != nil {
		t.Fatal(err)
	}
	return got, md.ContentDigest
}

func digestOf(rows ...[]string) string {
	h := consistency.NewHasher()
	for _, r := range rows {
		nulls := make([]bool, len(r))
		for i, v := range r {
			nulls[i] = v == "\x00NULL"
		}
		vals := append([]string(nil), r...)
		for i := range vals {
			if nulls[i] {
				vals[i] = ""
			}
		}
		h.AddStrings(vals, nulls)
	}
	return h.Digest()
}

const null = "\x00NULL"

// TestProcessTable_latin1StoredBytesUnderUTF8Header is the dump the CI cells
// on MariaDB 11.4, 11.8 and 12.3 sometimes got: a utf8mb4 header over the
// stored latin1 bytes ("\xd1and\xfa" is 'Ñandú'), because the mydumper thread
// that dumped the rows had not run a schema job. Trusting the header kept the
// bytes as they came and DuckDB refused the whole file.
func TestProcessTable_latin1StoredBytesUnderUTF8Header(t *testing.T) {
	got, digest := convertLatin1Table(t, latin1Schema,
		utf8Header+"INSERT INTO `t` (`id`,`v`,`u`) VALUES(1,\"\xd1and\xfa\",\"caf\xc3\xa9\"),\n"+
			// Stored 'Ã©' is the bytes of 'é': valid UTF-8 by itself. The
			// other row of the same file settles it as stored bytes.
			"(2,\"\xc3\xa9\",NULL),\n(3,NULL,\"x\");\n")
	want := []latin1Row{
		{1, sql.NullString{String: "Ñandú", Valid: true}, sql.NullString{String: "café", Valid: true}},
		{2, sql.NullString{String: "Ã©", Valid: true}, sql.NullString{}},
		{3, sql.NullString{}, sql.NullString{String: "x", Valid: true}},
	}
	if !equalLatin1Rows(got, want) {
		t.Fatalf("rows = %+v, want %+v", got, want)
	}
	if w := digestOf([]string{"1", "\xd1and\xfa", "caf\xc3\xa9"}, []string{"2", "\xc3\xa9", null}, []string{"3", null, "x"}); digest != w {
		t.Fatalf("the content digest must be over the stored bytes: got %s, want %s", digest, w)
	}
}

// TestProcessTable_latin1ConvertedUnderUTF8Header is the other way the same
// dump comes out: the rows already converted to UTF-8. They must not be
// decoded a second time ('Ñ' would become 'Ã\u0091'), and the digest is still
// over the stored bytes, where the live checksum reads them.
func TestProcessTable_latin1ConvertedUnderUTF8Header(t *testing.T) {
	got, digest := convertLatin1Table(t, latin1Schema,
		utf8Header+"INSERT INTO `t` (`id`,`v`,`u`) VALUES(1,\"Ñandú\",\"café\"),\n(2,\"Ã©\",NULL),\n(3,\"plain\",\"x\");\n")
	want := []latin1Row{
		{1, sql.NullString{String: "Ñandú", Valid: true}, sql.NullString{String: "café", Valid: true}},
		{2, sql.NullString{String: "Ã©", Valid: true}, sql.NullString{}},
		{3, sql.NullString{String: "plain", Valid: true}, sql.NullString{String: "x", Valid: true}},
	}
	if !equalLatin1Rows(got, want) {
		t.Fatalf("rows = %+v, want %+v", got, want)
	}
	if w := digestOf([]string{"1", "\xd1and\xfa", "caf\xc3\xa9"}, []string{"2", "\xc3\xa9", null}, []string{"3", "plain", "x"}); digest != w {
		t.Fatalf("the content digest must be over the stored bytes: got %s, want %s", digest, w)
	}
}

// TestProcessTable_latin1MixedFiles: one table, two data files, one from a
// thread that converted and one from a thread that did not. Each file is
// decided on its own rows, and both runs of the same table give one digest.
func TestProcessTable_latin1MixedFiles(t *testing.T) {
	converted := utf8Header + "INSERT INTO `t` (`id`,`v`,`u`) VALUES(1,\"Ñandú\",NULL);\n"
	stored := utf8Header + "INSERT INTO `t` (`id`,`v`,`u`) VALUES(2,\"caf\xe9\",NULL);\n"
	got, digest := convertLatin1Table(t, latin1Schema, converted, stored)
	want := []latin1Row{
		{1, sql.NullString{String: "Ñandú", Valid: true}, sql.NullString{}},
		{2, sql.NullString{String: "café", Valid: true}, sql.NullString{}},
	}
	if !equalLatin1Rows(got, want) {
		t.Fatalf("rows = %+v, want %+v", got, want)
	}
	_, binDigest := convertLatin1Table(t, latin1Schema,
		binaryHeader+"INSERT INTO `t` (`id`,`v`,`u`) VALUES(1,\"\xd1and\xfa\",NULL);\n",
		binaryHeader+"INSERT INTO `t` (`id`,`v`,`u`) VALUES(2,\"caf\xe9\",NULL);\n")
	if digest != binDigest {
		t.Fatalf("the same rows dumped two ways must give one digest: %s and %s", digest, binDigest)
	}
}

// TestProcessTable_warnsOnTextThatIsNotUTF8: a latin2 column dumped as stored
// bytes is not decoded, and its file will not read back. The conversion still
// writes it (as before) but says so, once per column, without the value.
func TestProcessTable_warnsOnTextThatIsNotUTF8(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	dir := t.TempDir()
	schema := binaryHeader + "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `v` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin2 COLLATE=latin2_general_ci;\n"
	schemaPath := filepath.Join(dir, "l2.t-schema.sql")
	dataPath := filepath.Join(dir, "l2.t.00000.sql")
	if err := os.WriteFile(schemaPath, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, []byte(binaryHeader+"INSERT INTO `t` (`id`,`v`) VALUES(1,\"secret\xb1\"),\n(2,\"other\xb1\"),\n(3,\"plain\");\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tf := TableFiles{Database: "l2", Table: "t", SchemaFile: schemaPath, DataFiles: []string{dataPath}, Format: "sql"}
	if _, err := processTable(context.Background(), tf, filepath.Join(dir, "t.parquet"), WriterConfig{Compression: "none", RowGroupSize: 100}); err != nil {
		t.Fatalf("processTable: %v", err)
	}
	out := buf.String()
	if n := strings.Count(out, "not valid UTF-8"); n != 1 {
		t.Fatalf("want one warning for the column, got %d:\n%s", n, out)
	}
	for _, want := range []string{"db=l2", "table=t", "column=v", "charset=latin2", "file=" + dataPath} {
		if !strings.Contains(out, want) {
			t.Errorf("the warning does not name %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, "secret") || strings.Contains(out, "other") {
		t.Errorf("the warning carries a value:\n%s", out)
	}
}

// TestProcessTable_noWarningForValidText: latin1 decoded, utf8mb4 as it came,
// nothing to say.
func TestProcessTable_noWarningForValidText(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	convertLatin1Table(t, latin1Schema, utf8Header+"INSERT INTO `t` (`id`,`v`,`u`) VALUES(1,\"\xd1and\xfa\",\"✓\");\n")
	if strings.Contains(buf.String(), "not valid UTF-8") {
		t.Fatalf("unexpected warning:\n%s", buf.String())
	}
}

func equalLatin1Rows(a, b []latin1Row) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
