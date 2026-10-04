package baseline

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// captureLogs swaps the default logger for one that writes text to a buffer,
// at Debug, for the length of the test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

const footerTestSQL = "CREATE TABLE `codes` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `code` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"

// ReadTableFooters says, per file, which of three things happened: the schema
// was read, the file was looked at and carries no usable CREATE TABLE (a fact
// about an immutable file), or the file could not be looked at (a fault that
// may be gone on the next try). A caller that caches needs the last two apart:
// the first is forever, the second must be retried (#2083 review).
func TestReadTableFooters_tellsUnreadFromNoSchema(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "snap", "shop", "codes.parquet")
	writeFixtureTable(t, good, footerTestSQL, [][]string{{"1", "AB"}})
	legacy := filepath.Join(dir, "snap", "shop", "legacy.parquet")
	writeFixtureTableNoSchemaMeta(t, legacy, footerTestSQL, [][]string{{"1", "AB"}})
	corrupt := filepath.Join(dir, "snap", "shop", "corrupt.parquet")
	if err := os.WriteFile(corrupt, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	unparsable := filepath.Join(dir, "snap", "shop", "unparsable.parquet")
	cols, err := ParseSchemaText(footerTestSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(unparsable, cols, WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{MetaKeyCreateTableSQL: "not a create table"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "AB"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	logs := captureLogs(t)
	// With an unreadable file in the list (the per-file path) and without
	// (the batched path): the same classification either way.
	for _, c := range []struct {
		name               string
		paths              []string
		unread, noSchema   []string
		wantWarnUnreadable bool
	}{
		{"batched", []string{good, legacy, unparsable}, nil, []string{legacy, unparsable}, false},
		{"per file", []string{good, legacy, corrupt, unparsable}, []string{corrupt}, []string{legacy, unparsable}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			logs.Reset()
			got, err := ReadTableFooters(context.Background(), c.paths)
			if err != nil {
				t.Fatal(err)
			}
			if b := got.Footers[good].BinaryText; !slices.Equal(b, []string{"code"}) {
				t.Errorf("the readable file lost its columns: %+v", got.Footers)
			}
			if len(got.Footers) != 1 {
				t.Errorf("Footers = %v, want only the readable file", got.Footers)
			}
			if !slices.Equal(got.Unread, c.unread) {
				t.Errorf("Unread = %v, want %v", got.Unread, c.unread)
			}
			if !slices.Equal(got.NoSchema, c.noSchema) {
				t.Errorf("NoSchema = %v, want %v", got.NoSchema, c.noSchema)
			}
			out := logs.String()
			if c.wantWarnUnreadable != strings.Contains(out, "could not be read") {
				t.Errorf("unreadable-files warning present = %v, want %v:\n%s", !c.wantWarnUnreadable, c.wantWarnUnreadable, out)
			}
			// Every warning about a lost schema names BOTH consequences.
			for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
				if line != "" && !(strings.Contains(line, "decimal") && strings.Contains(line, "_bin")) {
					t.Errorf("a warning names only part of what is lost: %s", line)
				}
			}
		})
	}
}

// A file with no CREATE TABLE in its footer is warned about once per table
// for the life of the process, not once per statement and not never: before,
// nothing was logged at any level, and a _bin column of such a table folded
// case with no trace anywhere.
func TestReadTableFooters_warnsOncePerTableWithNoSchema(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "2026-01-01T00-00-00Z", "warnonce", "legacy.parquet")
	writeFixtureTableNoSchemaMeta(t, first, footerTestSQL, [][]string{{"1", "AB"}})
	// The same table in a later snapshot: a new path, the same table.
	second := filepath.Join(dir, "2026-01-02T00-00-00Z", "warnonce", "legacy.parquet")
	writeFixtureTableNoSchemaMeta(t, second, footerTestSQL, [][]string{{"1", "AB"}})
	other := filepath.Join(dir, "2026-01-02T00-00-00Z", "warnonce", "other.parquet")
	writeFixtureTableNoSchemaMeta(t, other, footerTestSQL, [][]string{{"1", "AB"}})

	logs := captureLogs(t)
	for _, paths := range [][]string{{first}, {first}, {second}, {second, other}} {
		if _, err := ReadTableFooters(context.Background(), paths); err != nil {
			t.Fatal(err)
		}
	}
	out := logs.String()
	if n := strings.Count(out, "table=warnonce.legacy"); n != 1 {
		t.Errorf("warnonce.legacy was warned about %d times, want once:\n%s", n, out)
	}
	if n := strings.Count(out, "table=warnonce.other"); n != 1 {
		t.Errorf("warnonce.other was warned about %d times, want once:\n%s", n, out)
	}
	if !strings.Contains(out, "full snapshot") || !strings.Contains(out, "_bin") || !strings.Contains(out, "decimal") {
		t.Errorf("the warning must name the consequence and the cure:\n%s", out)
	}
}
