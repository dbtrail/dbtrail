package baseline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// #1380: a snapshot records how the source was locked when it was read, and
// says one of three things. No record is unknown, never consistent.

func TestReadConsistencyOfStamp(t *testing.T) {
	cases := []struct {
		stamp string
		want  ReadConsistency
	}{
		{"ftwrl", ReadConsistent},
		{"lock-all", ReadConsistent},
		{"safe-no-lock", ReadConsistent},
		{"no-lock", ReadTorn},
		{LockStampPGRepeatableRead, ReadConsistent},
		// No record: every snapshot written before the record existed.
		{"", ReadUnknown},
		// Not a value this program writes. None of them may read as
		// consistent, and none as torn either: nothing is known.
		{"FTWRL", ReadUnknown},
		{"NO_LOCK", ReadUnknown},
		{"No-Lock", ReadUnknown},
		{" ftwrl", ReadUnknown},
		{"ftwrl ", ReadUnknown},
		{"ftwrl\n", ReadUnknown},
		{"ftwrl\nno-lock", ReadUnknown},
		{"ftwrl/", ReadUnknown},
		{"ftwrl-v2", ReadUnknown},
		{"consistent", ReadUnknown},
		{"torn", ReadUnknown},
		{"unknown", ReadUnknown},
		{"true", ReadUnknown},
		{"lock", ReadUnknown},
		{"pg", ReadUnknown},
	}
	for _, c := range cases {
		if got := ReadConsistencyOfStamp(c.stamp); got != c.want {
			t.Errorf("ReadConsistencyOfStamp(%q) = %s, want %s", c.stamp, got, c.want)
		}
		if got := ReadConsistencyOf(DumpMetadata{LockMode: c.stamp}); got != c.want {
			t.Errorf("ReadConsistencyOf(LockMode %q) = %s, want %s", c.stamp, got, c.want)
		}
	}
}

// The zero value is what a caller that set nothing passes.
func TestReadConsistency_zeroValueIsUnknown(t *testing.T) {
	var c ReadConsistency
	if c != ReadUnknown || c.String() != "unknown" {
		t.Fatalf("the zero value is %s (%d), want unknown", c, c)
	}
	if got := ReadConsistencyOf(DumpMetadata{}); got != ReadUnknown {
		t.Fatalf("a footer with no record reads %s, want unknown", got)
	}
	if got := ReadConsistencyOf(emptyFooterMetadata()); got != ReadUnknown {
		t.Fatalf("what every footer reader starts from reads %s, want unknown", got)
	}
	if got := ReadConsistency(42).String(); got != "unknown" {
		t.Fatalf("a value out of range is said as %q, want unknown", got)
	}
}

// Every lock mode has an answer, and it is PointConsistent's: a mode added
// to LockModeValues and not to the reader would read as unknown for ever.
func TestReadConsistency_everyLockModeHasAnAnswer(t *testing.T) {
	for _, m := range LockModeValues {
		want := ReadTorn
		if m.PointConsistent() {
			want = ReadConsistent
		}
		if got := ReadConsistencyOfStamp(string(m)); got != want {
			t.Errorf("mode %s reads %s, want %s", m, got, want)
		}
	}
}

func TestWorstReadConsistency(t *testing.T) {
	c, u, x := ReadConsistent, ReadUnknown, ReadTorn
	cases := []struct {
		name string
		of   []ReadConsistency
		want ReadConsistency
	}{
		{"no table is not consistent", nil, u},
		{"one consistent", []ReadConsistency{c}, c},
		{"all consistent", []ReadConsistency{c, c, c}, c},
		{"one unknown among consistent", []ReadConsistency{c, u, c}, u},
		{"one torn among consistent", []ReadConsistency{c, c, x}, x},
		{"torn first", []ReadConsistency{x, c, c}, x},
		{"torn beats unknown", []ReadConsistency{u, x}, x},
		{"torn beats unknown, other order", []ReadConsistency{x, u}, x},
		{"a value out of range is unknown", []ReadConsistency{c, ReadConsistency(42)}, u},
		{"a value out of range does not hide torn", []ReadConsistency{ReadConsistency(42), x}, x},
	}
	for _, tc := range cases {
		if got := WorstReadConsistency(tc.of...); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestLockModeMarker(t *testing.T) {
	for _, m := range LockModeValues {
		dir := t.TempDir()
		if err := WriteLockModeMarker(dir, m); err != nil {
			t.Fatalf("write %s: %v", m, err)
		}
		if got := readLockModeMarker(dir); got != string(m) {
			t.Errorf("mode %s read back as %q", m, got)
		}
	}
	// The zero mode is not the default mode: ParseLockMode reads "" as ftwrl,
	// and a record written that way would call a dump locked that nobody
	// said was.
	for _, m := range []LockMode{"", "FTWRL", "pg-repeatable-read", "x"} {
		dir := t.TempDir()
		if err := WriteLockModeMarker(dir, m); err == nil {
			t.Errorf("mode %q was written", m)
		}
		if _, err := os.Stat(filepath.Join(dir, LockModeMarkerFile)); err == nil {
			t.Errorf("mode %q left a record", m)
		}
	}
}

func TestReadLockModeMarker_whatIsNotARecord(t *testing.T) {
	for name, content := range map[string]string{
		"empty":             "",
		"only a line end":   "\n",
		"upper case":        "FTWRL\n",
		"mydumper spelling": "NO_LOCK\n",
		"two lines":         "ftwrl\nno-lock\n",
		"two words":         "ftwrl no-lock\n",
		"a path":            "ftwrl/\n",
		"the pg value":      LockStampPGRepeatableRead + "\n",
		"a verdict":         "consistent\n",
	} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, LockModeMarkerFile), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := readLockModeMarker(dir); got != "" {
			t.Errorf("%s: read as %q, want no record", name, got)
		}
	}
	// What the compose pipeline and an editor leave: a line end, or spaces
	// around the one word.
	for _, content := range []string{"no-lock", "no-lock\n", "no-lock\r\n", "  no-lock  \n\n"} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, LockModeMarkerFile), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := readLockModeMarker(dir); got != "no-lock" {
			t.Errorf("%q read as %q, want no-lock", content, got)
		}
	}
	if got := readLockModeMarker(t.TempDir()); got != "" {
		t.Errorf("a dump with no record read as %q", got)
	}
	// A directory where the file should be: unreadable, so no record.
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, LockModeMarkerFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := readLockModeMarker(dir); got != "" {
		t.Errorf("an unreadable record read as %q", got)
	}
}

// A mydumper that was not given the mode chose its own: no record, and a
// record left in the directory by an earlier dump goes.
func TestRecordDumpLockMode_notSent(t *testing.T) {
	dir := t.TempDir()
	if err := WriteLockModeMarker(dir, LockModeFTWRL); err != nil {
		t.Fatal(err)
	}
	RecordDumpLockMode(dir, LockModeFTWRL, false)
	if got := readLockModeMarker(dir); got != "" {
		t.Fatalf("a dump whose mode was not sent records %q", got)
	}
	RecordDumpLockMode(dir, LockModeNoLock, true)
	if got := readLockModeMarker(dir); got != "no-lock" {
		t.Fatalf("a dump taken with no-lock records %q", got)
	}
	// A directory that does not exist: nothing is written and nothing panics.
	RecordDumpLockMode(filepath.Join(dir, "missing"), LockModeFTWRL, true)
}

// lockOfRun converts a dump and returns the lock record of the file written.
func lockOfRun(t *testing.T, marker string) (string, bool) {
	t.Helper()
	inputDir, outputDir := t.TempDir(), t.TempDir()
	copyFixture(t, "mydumper_v1_binary_json-schema.sql", filepath.Join(inputDir, "ptest.bins-schema.sql"))
	copyFixture(t, "mydumper_v1_binary_json.sql", filepath.Join(inputDir, "ptest.bins.00000.sql"))
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if marker != "" {
		if err := os.WriteFile(filepath.Join(inputDir, LockModeMarkerFile), []byte(marker), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Run(context.Background(), Config{InputDir: inputDir, OutputDir: outputDir, Compression: "none"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(outputDir, "20*", "ptest", "bins.parquet"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("table files written: %v (%v)", matches, err)
	}
	md, err := ReadParquetMetadata(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	return md.LockMode, md.LockMode != ""
}

// From the dump to the footer, the three cases.
func TestRun_recordsTheLockModeOfTheDump(t *testing.T) {
	for _, c := range []struct {
		name, marker, wantStamp string
		want                    ReadConsistency
	}{
		{"a dump taken with locks", "ftwrl\n", "ftwrl", ReadConsistent},
		{"a dump taken with no locks", "no-lock\n", "no-lock", ReadTorn},
		// mydumper run by hand, or by a build that was not given the mode.
		{"a dump with no record", "", "", ReadUnknown},
		{"a record that names no mode", "FTWRL\n", "", ReadUnknown},
	} {
		stamp, has := lockOfRun(t, c.marker)
		if stamp != c.wantStamp || has != (c.wantStamp != "") {
			t.Errorf("%s: the footer records %q, want %q", c.name, stamp, c.wantStamp)
		}
		if got := ReadConsistencyOfStamp(stamp); got != c.want {
			t.Errorf("%s: the snapshot reads %s, want %s", c.name, got, c.want)
		}
	}
}
