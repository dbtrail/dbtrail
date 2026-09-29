package reconstruct

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A MariaDB UUID/INET value reaches the writer as the server's text (from the
// baseline, and from the event decode). Written as a quoted string it does
// not load the way a mydumper dump is loaded: under the dump's
// `SET NAMES binary` preamble (which drill replays) MariaDB reads the quoted
// text as the BINARY form of the type and refuses it (ER 1292 in strict mode;
// NULL or a wrong value otherwise; drill hit ER 1062 on two distinct keys).
// The writer emits them as X'..' of the full-width bytes, which MariaDB reads
// as the value itself whatever the session charset.
func TestMydumperWriter_MariaDBFixedTypesAsHex(t *testing.T) {
	dir := t.TempDir()
	w, err := NewMydumperWriter(dir, "db", "t", []string{"id", "u", "i4", "i6", "note"}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	create := "/*!40101 SET NAMES binary*/;\nCREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `u` uuid DEFAULT NULL,\n  `i4` inet4 DEFAULT NULL,\n  `i6` INET6 DEFAULT NULL,\n  `note` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	if err := w.WriteSchema(create); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]any{int64(1), "123e4567-e89b-12d3-a456-426614174000", "10.0.0.0", "::ffff:1.2.3.4", "123e4567-e89b-12d3-a456-426614174000"}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]any{int64(2), nil, nil, nil, nil}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "db.t.00000.sql"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	for _, want := range []string{
		"(1, X'123e4567e89b12d3a456426614174000', X'0a000000', X'00000000000000000000ffff01020304', '123e4567-e89b-12d3-a456-426614174000')",
		"(2, NULL, NULL, NULL, NULL)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("chunk does not contain %s:\n%s", want, got)
		}
	}
}

// A value that is not the type's text (an event captured before capture
// padded these types, #1944) has no correct bytes to write. Writing it anyway
// would restore a wrong value that loads cleanly; the writer refuses instead.
func TestMydumperWriter_MariaDBFixedTypeUnrestorableValueRefused(t *testing.T) {
	w, err := NewMydumperWriter(t.TempDir(), "db", "t", []string{"id", "u"}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteSchema("CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `u` uuid,\n  PRIMARY KEY (`id`)\n);\n"); err != nil {
		t.Fatal(err)
	}
	err = w.WriteRow([]any{int64(1), "\x12>\ufffdg"})
	if err == nil || !strings.Contains(err.Error(), `"u"`) {
		t.Fatalf("WriteRow of a damaged UUID = %v, want a refusal naming the column", err)
	}
}

// The Parquet side (baseline refresh, reconstruct --output-format parquet)
// must refuse the same unrestorable value the mydumper writer refuses:
// published into the copy, it would be inherited by every later refresh.
func TestRenderBaselineValue_MariaDBFixedTypes(t *testing.T) {
	col := func(dt string) baseline.Column { return baseline.Column{Name: "c", MySQLType: dt} }
	if text, isNull, err := renderBaselineValue(col("uuid"), "123e4567-e89b-12d3-a456-426614174000"); err != nil || isNull || text != "123e4567-e89b-12d3-a456-426614174000" {
		t.Errorf("rendered UUID: %q, %v, %v", text, isNull, err)
	}
	if _, isNull, err := renderBaselineValue(col("inet4"), nil); err != nil || !isNull {
		t.Errorf("NULL INET4: %v, %v", isNull, err)
	}
	for _, c := range []struct{ dt, v string }{
		{"uuid", "\x12>�g"},          // captured before #1944
		{"inet4", "CgAAAA=="},        // base64 an untyped epoch left
		{"inet6", "0:0:0:0:0:0:0:1"}, // not the text the server prints
		{"uuid", "123E4567-E89B-12D3-A456-426614174000"},
	} {
		if text, _, err := renderBaselineValue(col(c.dt), c.v); err == nil || !strings.Contains(err.Error(), `"c"`) {
			t.Errorf("%s %q rendered as %q (%v), want a refusal naming the column", c.dt, c.v, text, err)
		}
	}
}

// Column names match case-insensitively, as they do on the server.
func TestMydumperWriter_MariaDBFixedColumnNameCase(t *testing.T) {
	dir := t.TempDir()
	w, err := NewMydumperWriter(dir, "db", "t", []string{"ID", "U"}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteSchema("CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `u` uuid,\n  PRIMARY KEY (`id`)\n);\n"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]any{int64(1), "00000000-0000-0000-0000-000000000001"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "db.t.00000.sql"))
	if !strings.Contains(string(b), "X'00000000000000000000000000000001'") {
		t.Errorf("UUID column named in another case was not written as X'..':\n%s", b)
	}
}
