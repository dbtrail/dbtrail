package baseline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// A full backup is stamped with the marks its caller read from the index
// before the dump (#1912, #2160), on every file it writes. No mark given: no
// key, which readers take as "no mark".
func TestRun_stampsTheMarksItIsGiven(t *testing.T) {
	for _, c := range []struct {
		name            string
		ddlMark, events string
	}{
		{"both marks", `{"id":7}`, `{"id":42,"binlog_file":"binlog.000003","end_pos":900}`},
		{"no marks", "", ""},
	} {
		inputDir, outputDir := t.TempDir(), t.TempDir()
		copyFixture(t, "mydumper_v1_binary_json-schema.sql", filepath.Join(inputDir, "ptest.bins-schema.sql"))
		copyFixture(t, "mydumper_v1_binary_json.sql", filepath.Join(inputDir, "ptest.bins.00000.sql"))
		if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Run(context.Background(), Config{InputDir: inputDir, OutputDir: outputDir, Compression: "none",
			DDLMark: c.ddlMark, EventMark: c.events}); err != nil {
			t.Fatalf("%s: Run: %v", c.name, err)
		}
		matches, err := filepath.Glob(filepath.Join(outputDir, "20*", "ptest", "bins.parquet"))
		if err != nil || len(matches) != 1 {
			t.Fatalf("%s: table files written: %v (%v)", c.name, matches, err)
		}
		md, err := ReadParquetMetadata(matches[0])
		if err != nil {
			t.Fatal(err)
		}
		if md.DDLMark != c.ddlMark || md.EventMark != c.events {
			t.Errorf("%s: the footer carries DDL mark %q and event mark %q, want %q and %q", c.name, md.DDLMark, md.EventMark, c.ddlMark, c.events)
		}
	}
}
