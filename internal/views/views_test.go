package views

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
)

// goldenInput is the fixture layout: one local archive source and one S3 one
// (so the S3 preamble and both glob shapes are exercised), plus a baseline
// snapshot whose table names include a sanitization case and a deliberate view-
// name collision.
func goldenInput() Input {
	return Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:     "v0.50.0",
		ArchiveSources: []string{
			"/data/archives/bintrail_id=11111111-2222-3333-4444-555555555555",
			"s3://my-bucket/archives/bintrail_id=66666666-7777-8888-9999-000000000000",
		},
		PortableRouting:  true,
		ArchiveRegion:    "us-east-1",
		BaselineSource:   "s3://my-bucket/baselines/",
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Baselines: []BaselineTable{
			// Money columns of the ordinary shape: both get cast.
			{Schema: "shop", Table: "orders", Path: "s3://my-bucket/baselines/2026-04-30T03-00-00Z/shop/orders.parquet",
				SchemaKnown: true,
				Decimals: []DecimalColumn{
					{Name: "total", Precision: 10, Scale: 2},
					{Name: "tax_rate", Precision: 6, Scale: 4},
				}},
			// No decimal columns at all: nothing to cast, and nothing to say.
			{Schema: "shop", Table: "order_items", Path: "s3://my-bucket/baselines/2026-04-30T03-00-00Z/shop/order_items.parquet",
				SchemaKnown: true},
			// Sanitizes to the same view name as shop.order_items above. Its
			// DECIMAL(65,30) is past DuckDB's ceiling, so it stays text and the
			// file has to say which column and why.
			{Schema: "shop_order", Table: "items", Path: "s3://my-bucket/baselines/2026-04-30T03-00-00Z/shop_order/items.parquet",
				SchemaKnown: true,
				Decimals:    []DecimalColumn{{Name: "weight", Precision: 65, Scale: 30}}},
			// A hyphen and mixed case, neither legal bare in an identifier. Its
			// footer could not be read, which is a different fact from "no
			// decimal columns" and is stated as one.
			{Schema: "Legacy-DB", Table: "Audit Log", Path: "s3://my-bucket/baselines/2026-04-30T03-00-00Z/Legacy-DB/Audit Log.parquet"},
		},
	}
}

// TestGenerate_golden pins the whole generated file.
//
// This is the test the issue asks for by name: the events projection is built
// from archive.BinlogEventColumns, so adding, removing or renaming a column
// there changes this output and fails here — which is the point. A generated
// schema that silently falls behind the files it describes is worse than none,
// because the operator would trust it.
//
// Regenerate with `go test ./internal/views -update` after an INTENTIONAL
// change, and read the diff before committing it.
func TestGenerate_golden(t *testing.T) {
	got := Generate(goldenInput())
	golden := filepath.Join("testdata", "views.golden.sql")

	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Log("golden updated")
		return
	}

	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update to create it): %v", err)
	}
	if got != string(want) {
		t.Errorf("generated SQL differs from %s.\n--- got ---\n%s\n--- want ---\n%s", golden, got, want)
	}
}

// TestGenerate_projectsEveryArchivedColumn is the same guarantee stated as an
// invariant rather than a byte comparison: a reader who regenerates the golden
// without looking at the diff still gets caught if a column stops being
// projected.
func TestGenerate_projectsEveryArchivedColumn(t *testing.T) {
	got := Generate(goldenInput())
	for _, col := range archive.BinlogEventColumns {
		if !strings.Contains(got, col.Name) {
			t.Errorf("archived column %q is not projected by the events view — a query over the "+
				"generated schema would silently not see it", col.Name)
		}
	}
}

// TestGenerate_noArchives: an index whose partitions have never been archived
// is a normal state, not an error. The file must say so instead of emitting a
// view over an empty file list, which DuckDB rejects at CREATE time.
func TestGenerate_noArchives(t *testing.T) {
	in := goldenInput()
	in.ArchiveSources = nil
	got := Generate(in)
	if strings.Contains(got, "CREATE OR REPLACE VIEW \"events\"") {
		t.Error("an events view was emitted with no archive sources")
	}
	if !strings.Contains(got, "no archive sources are registered") {
		t.Errorf("missing the explanatory comment:\n%s", got)
	}
	// The S3 preamble must still appear — the baselines are on S3.
	if !strings.Contains(got, "LOAD httpfs") {
		t.Error("S3 preamble missing even though the baseline paths are s3://")
	}
}

// TestGenerate_noBaselines covers the other empty half.
func TestGenerate_noBaselines(t *testing.T) {
	in := goldenInput()
	in.Baselines = nil
	got := Generate(in)
	if strings.Contains(got, "state_") && !strings.Contains(got, "state_<schema>_<table>") {
		t.Errorf("a state view was emitted with no baselines:\n%s", got)
	}
	if !strings.Contains(got, "no baseline snapshot was discovered") {
		t.Errorf("missing the explanatory comment:\n%s", got)
	}
}

// TestGenerate_localOnlyOmitsS3Preamble: a purely local layout must not carry
// INSTALL httpfs / CREATE SECRET lines. They are harmless but they turn a
// self-contained local file into one that looks like it needs AWS.
func TestGenerate_localOnlyOmitsS3Preamble(t *testing.T) {
	in := Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		ArchiveSources: []string{"/data/archives/bintrail_id=abc"},
		BaselineSource: "/data/baselines",
		Baselines: []BaselineTable{
			{Schema: "shop", Table: "orders", Path: "/data/baselines/2026-04-30T03-00-00Z/shop/orders.parquet"},
		},
	}
	got := Generate(in)
	for _, marker := range []string{"httpfs", "CREATE OR REPLACE SECRET", "credential_chain"} {
		if strings.Contains(got, marker) {
			t.Errorf("local-only layout emitted the S3 marker %q:\n%s", marker, got)
		}
	}
}

// TestGenerate_noCredentialsInOutput is the property that makes the file safe to
// paste into a notebook or hand to a colleague. The credential-chain form is
// chosen precisely so nothing secret is renderable here — this pins that no
// future field starts leaking one.
func TestGenerate_noCredentialsInOutput(t *testing.T) {
	got := Generate(goldenInput())
	for _, forbidden := range []string{"KEY_ID '", "SECRET '", "SESSION_TOKEN", "password", "AKIA"} {
		// The commented-out alternative deliberately shows the KEY_ID form with
		// an ellipsis placeholder; only a line that is not a comment counts.
		for _, line := range strings.Split(got, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "--") {
				continue
			}
			if strings.Contains(line, forbidden) {
				t.Errorf("executable line contains %q: %s", forbidden, line)
			}
		}
	}
}

// TestStateViewName_collision: two different tables must never share a view
// name. Every statement is CREATE OR REPLACE, so a collision would leave one
// table's view silently pointing at the other's Parquet file.
func TestStateViewName_collision(t *testing.T) {
	used := map[string]bool{}
	first := stateViewName("shop", "order_items", used)
	second := stateViewName("shop_order", "items", used)
	if first == second {
		t.Fatalf("both tables got the view name %q", first)
	}
	if first != "state_shop_order_items" || second != "state_shop_order_items_2" {
		t.Fatalf("names = %q, %q", first, second)
	}
}

// TestGenerate_escapesPathLiterals: a path containing an apostrophe is unusual
// but legal, and the generated file is meant to be executed — an unescaped one
// would produce a syntax error at best and a broken glob at worst.
func TestGenerate_escapesPathLiterals(t *testing.T) {
	in := Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		ArchiveSources: []string{"/data/dani's archives/bintrail_id=abc"},
	}
	got := Generate(in)
	if !strings.Contains(got, "dani''s archives") {
		t.Errorf("apostrophe in a path was not doubled:\n%s", got)
	}
}

// TestArchiveGlob pins the glob against the layout rotation actually writes
// (archive.ParseArchivePath reads back the same three levels).
func TestArchiveGlob(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"/data/archives/bintrail_id=abc", "/data/archives/bintrail_id=abc/event_date=*/event_hour=*/*.parquet"},
		{"/data/archives/bintrail_id=abc/", "/data/archives/bintrail_id=abc/event_date=*/event_hour=*/*.parquet"},
		{"s3://b/p/bintrail_id=abc", "s3://b/p/bintrail_id=abc/event_date=*/event_hour=*/*.parquet"},
	} {
		if got := archiveGlob(tc.in); got != tc.want {
			t.Errorf("archiveGlob(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// A registry entry's baseline path and archive prefix are operator-supplied
// and unvalidated for newlines. Printed raw into a `--` comment, one newline
// ends the comment and leaves the rest of the value on its own line inside the
// header, where DuckDB will try to EXECUTE it.
//
// Scoped to the header block on purpose. Later in the file the same values
// appear inside single-quoted path literals, where a newline stays inside the
// quotes and produces an unresolvable path rather than a statement -- flagging
// those would make this fail for a reason it is not about.
func TestGenerate_aNewlineInAPathCannotEscapeTheHeaderComment(t *testing.T) {
	poison := "\nDROP TABLE shop.orders;\n"
	in := Input{
		GeneratedAt:          time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		ArchiveSources:       []string{"/data/archives/bintrail_id=abc" + poison},
		BaselineSource:       "/backups" + poison,
		BaselineSnapshot:     time.Date(2026, 4, 30, 9, 0, 0, 0, time.UTC),
		Baselines:            []BaselineTable{{Schema: "shop", Table: "orders", Path: "/backups/shop/orders.parquet"}},
		NewerElsewhere:       time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC),
		NewerElsewhereSource: "s3://bucket/prefix" + poison,
		// FollowPointer prints the root a SECOND time, in its own arm two lines
		// below. Left at the zero value this test is blind to that print, which
		// is how it survived the first pass.
		Follow: FollowPointer,
	}
	// The unchecked-location arm is the SIBLING of NewerElsewhere in the same
	// switch, so it never renders in the case above and needs its own input.
	unchecked := in
	unchecked.NewerElsewhere = time.Time{}
	unchecked.NewerElsewhereSource = ""
	unchecked.NewerElsewhereUnchecked = "s3://bucket/prefix" + poison

	// The header is the leading run of comment lines, closed by a blank line.
	for name, probe := range map[string]Input{"newer-elsewhere": in, "unchecked": unchecked} {
		for _, line := range strings.Split(Generate(probe), "\n") {
			if strings.TrimSpace(line) == "" {
				break
			}
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				t.Errorf("[%s] a newline in an operator-supplied path broke out of the header "+
					"comment, leaving this line for DuckDB to execute: %q", name, line)
			}
		}
	}
}
