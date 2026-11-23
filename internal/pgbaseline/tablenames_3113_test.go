package pgbaseline

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/views"
)

// The literal is here because the capture side does not import the views.
func TestViewsTempPrefixIsTheViewsOwn_3113(t *testing.T) {
	if viewsTempPrefix != views.SnapshotFileTempPrefix {
		t.Fatalf("viewsTempPrefix = %q, views uses %q", viewsTempPrefix, views.SnapshotFileTempPrefix)
	}
}

func TestUnstorableTable_3113(t *testing.T) {
	longest := strings.Repeat("a", maxFileNameBytes-longestFileSuffix)
	for _, c := range []struct {
		name string
		bad  bool
	}{
		{"", true},
		{viewsTempPrefix, true},
		{viewsTempPrefix + "123", true},
		{"/", true},
		{`\`, true},
		{`a\b`, true},
		{`..\x`, true},
		{"a/b", true},
		{"../x", true},
		{`..\..//x`, true},
		{"/etc/x", true},
		{"a\x00b", true},
		{longest + "a", true},
		// One path component each: <name>.parquet stays in the schema's
		// folder, and the readers open it.
		{longest, false},
		{".", false},
		{"..", false},
		{"...", false},
		{"orders", false},
		{"Orders", false},
		{"ORDERS", false},
		{" spaced name ", false},
		{"..x", false},
		{"x..", false},
		{".hidden", false},
		{"a.b", false},
		{"Ñandú", false},
		{"日本語", false},
		{"a\nb", false},
		{"t.000000.pgnew", false},
		{"_SUCCESS", false},
		// Look like a slash and are not one.
		{"a∕b", false}, // U+2215
		{"a／b", false}, // U+FF0F
	} {
		if err := unstorableTable(c.name); (err != nil) != c.bad {
			t.Errorf("unstorableTable(%q) = %v, want refused=%v", c.name, err, c.bad)
		}
	}
}

func TestUnstorableSchema_3113(t *testing.T) {
	for _, name := range []string{
		"", ".", "..", "/", `\`, `a\b`, "a/b", "../x", "a\x00b", strings.Repeat("s", maxFileNameBytes+1),
		baseline.SuccessMarker, baseline.IncompleteMarker, baseline.ChangesOnlyMarkerFile,
		baseline.ViewsSkippedName, baseline.NewestPointerName, baselineintegrity.ManifestName,
		"_ARCHIVE_CHECKED", "_ARCHIVE_CHECKED.tmp", "_PRUNING", baseline.WriterMarkerPrefix + "abc",
		views.SnapshotFileName, views.SnapshotFileTempPrefix + "1",
	} {
		if unstorableSchema(name) == nil {
			t.Errorf("schema %q was not refused", name)
		}
	}
	for _, name := range []string{
		"public", "Public", "_success", "_timescaledb_internal", "_SUCCESS2", "_WRITER",
		"...", ".hidden", " s ", "Ñandú", strings.Repeat("s", maxFileNameBytes),
	} {
		if err := unstorableSchema(name); err != nil {
			t.Errorf("schema %q: %v", name, err)
		}
	}
}

// Nothing is encoded, so two names cannot end up as one file: the name with a
// separator is out and the ones that look like it keep their own files.
func TestSplitStorable_3113(t *testing.T) {
	in := []tableInfo{
		{Schema: "public", Table: "a_b"},
		{Schema: "public", Table: "a/b"},
		{Schema: "public", Table: `a\b`},
		{Schema: "..", Table: "t"},
		{Schema: "public", Table: "Ñandú"},
	}
	keep, left := splitStorable(in, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if len(keep) != 2 || keep[0].Table != "a_b" || keep[1].Table != "Ñandú" {
		t.Fatalf("kept %+v", keep)
	}
	if len(left) != 3 {
		t.Fatalf("left out %+v", left)
	}
	if l := left[2]; l.Schema != ".." || l.Name != "t" || l.Table != `".."."t"` || !strings.Contains(l.Reason, "schema") {
		t.Fatalf("left out entry %+v", l)
	}
}

// A line break in a name must not forge a line of a log or of the output, and
// the run's error, which sessions with a data profile can read, names no table.
func TestLeftOutMessage_3113(t *testing.T) {
	_, left := splitStorable([]tableInfo{{Schema: "public", Table: "a/\nforged\x1b[2J"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if len(left) != 1 || left[0].Name != "a/\nforged\x1b[2J" {
		t.Fatalf("left out %+v: Name must stay the source's spelling", left)
	}
	if left[0].Table != `"public"."a/?forged?[2J"` {
		t.Fatalf("display name: %q", left[0].Table)
	}
	msg := allLeftOutError(left).Error()
	if strings.Contains(msg, "forged") || !strings.Contains(msg, "1 table(s)") {
		t.Fatalf("message: %q", msg)
	}
}
