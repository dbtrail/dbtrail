package reconstruct

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// A reconstructed dump loads under SET NAMES binary, so a latin1 column must
// carry latin1 bytes. The copy holds its text as UTF-8 since #2634 (the
// baseline decodes it, capture decodes TEXT and VARCHAR), so the writer
// encodes it back. A value that is not valid UTF-8 is a change captured before
// this version, already latin1, and is written as it is.
func TestMydumperWriter_latin1ColumnsWrittenAsLatin1Bytes(t *testing.T) {
	dir := t.TempDir()
	w, err := NewMydumperWriter(dir, "db", "t", []string{"id", "v", "x", "u", "e"}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	w.SetColumnTypes([]metadata.ColumnMeta{
		{Name: "id", DataType: "int"},
		{Name: "V", DataType: "varchar", CharacterSet: "latin1"},
		{Name: "x", DataType: "mediumtext", CharacterSet: "LATIN1"},
		{Name: "u", DataType: "varchar", CharacterSet: "utf8mb4"},
		{Name: "e", DataType: "enum", CharacterSet: "latin1"},
	})
	if err := w.WriteRow([]any{int64(1), "café", "naïve € \u0081", "café", "sí"}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]any{int64(2), "caf\xe9", nil, "plain", "no"}); err != nil {
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
		"(1, 'caf\xe9', 'na\xefve \x80 \x81', 'café', 's\xed')",
		"(2, 'caf\xe9', NULL, 'plain', 'no')",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("chunk does not contain %q:\n%q", want, got)
		}
	}
}

func TestMydumperWriter_latin1RefusesWhatLatin1CannotHold(t *testing.T) {
	w, err := NewMydumperWriter(t.TempDir(), "db", "t", []string{"v"}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	w.SetColumnTypes([]metadata.ColumnMeta{{Name: "v", DataType: "varchar", CharacterSet: "latin1"}})
	err = w.WriteRow([]any{"日本"})
	if err == nil || !strings.Contains(err.Error(), "latin1") || !strings.Contains(err.Error(), `"v"`) {
		t.Fatalf("want a refusal naming the column and latin1, got %v", err)
	}
	w.Discard()
}

// The dump loads into the table its own CREATE TABLE makes, so that text
// decides which columns get latin1 bytes. The index's schema snapshot can
// disagree with it (a later ALTER ... CONVERT TO CHARACTER SET, or a snapshot
// from before #756 with no character set): it decides only when the CREATE
// TABLE cannot be read.
func TestMydumperWriter_latin1FromTheDumpsCreateTable(t *testing.T) {
	dir := t.TempDir()
	w, err := NewMydumperWriter(dir, "db", "t", []string{"id", "v", "u"}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	w.SetColumnTypes([]metadata.ColumnMeta{
		{Name: "id", DataType: "int"},
		{Name: "v", DataType: "varchar", CharacterSet: "utf8mb4"}, // converted after the snapshot
		{Name: "u", DataType: "varchar", CharacterSet: "latin1"},  // the reverse
	})
	create := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(20) DEFAULT NULL,\n  `u` varchar(20) CHARACTER SET utf8mb4 DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n"
	if err := w.WriteSchema(create); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]any{int64(1), "café", "café"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "db.t.00000.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "(1, 'caf\xe9', 'café')"; !strings.Contains(string(b), want) {
		t.Fatalf("chunk does not contain %q:\n%q", want, b)
	}

	// An unreadable CREATE TABLE leaves the snapshot's answer.
	dir2 := t.TempDir()
	w2, _ := NewMydumperWriter(dir2, "db", "t", []string{"v"}, 1<<20)
	w2.SetColumnTypes([]metadata.ColumnMeta{{Name: "v", DataType: "varchar", CharacterSet: "latin1"}})
	if err := w2.WriteSchema("-- placeholder\n"); err != nil {
		t.Fatal(err)
	}
	if err := w2.WriteRow([]any{"café"}); err != nil {
		t.Fatal(err)
	}
	w2.Close()
	b2, _ := os.ReadFile(filepath.Join(dir2, "db.t.00000.sql"))
	if !strings.Contains(string(b2), "('caf\xe9')") {
		t.Fatalf("unreadable CREATE: want the snapshot's latin1, got %q", b2)
	}
}
