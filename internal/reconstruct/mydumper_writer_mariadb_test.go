package reconstruct

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
