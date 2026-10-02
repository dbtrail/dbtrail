package views

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// pointerRoot builds a local baselines root with one published snapshot and the
// `current` pointer aimed at it, and returns the root plus the two tables'
// absolute paths as a producer would hand them over.
func pointerRoot(t *testing.T) (string, []string) {
	t.Helper()
	root := t.TempDir()
	const stamp = "2026-05-30T03-00-00Z"
	writeSnapshot(t, root, stamp, true, "here")
	// A second table, copied from the first, so a test that asserts nothing was
	// created has something that would otherwise have been created. "audit"
	// sorts ahead of "shop", which is the half that matters: a view emitted
	// AFTER the failing one would be absent for the ordinary reason.
	src := filepath.Join(root, stamp, "shop", "orders.parquet")
	dstDir := filepath.Join(root, stamp, "audit")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read snapshot table: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dstDir, "events.parquet"), body, 0o644); err != nil {
		t.Fatalf("write second table: %v", err)
	}
	if err := baseline.PublishCurrentPointer(filepath.Join(root, stamp)); err != nil {
		t.Fatalf("publish pointer: %v", err)
	}
	return root, []string{
		filepath.Join(dstDir, "events.parquet"),
		src,
	}
}

// pointerFixture goes through ApplyFollow rather than setting Follow by hand.
// The pointer mode is the one that can only be reached with a real symlink on
// disk, so the golden for it has to build its Input directly; this is where the
// producer's own decision gets executed.
func pointerFixture(t *testing.T, root string, paths []string) string {
	t.Helper()
	in := Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   root,
		BaselineSnapshot: time.Date(2026, 5, 30, 3, 0, 0, 0, time.UTC),
		Baselines: []BaselineTable{
			{Schema: "audit", Table: "events", Path: paths[0]},
			{Schema: "shop", Table: "orders", Path: paths[1]},
		},
	}
	ApplyFollow(&in, root, false)
	if in.Follow != FollowPointer {
		t.Fatalf("ApplyFollow chose %v over a root with a published pointer; want FollowPointer", in.Follow)
	}
	for _, b := range in.Baselines {
		// The preflight cannot describe a table whose tail the producer did not
		// fill, and it fails OPEN on one, so this mode losing Rel would take the
		// whole check out with no test failing anywhere near it.
		if b.Rel == "" {
			t.Fatalf("ApplyFollow left %s.%s without a Rel; the preflight silently "+
				"skips every table it cannot name", b.Schema, b.Table)
		}
	}
	return Generate(in)
}

// TestPointerStateView_readsThroughTheSymlink is the assumption the pointer
// preflight rests on: DuckDB's glob() reports paths AS ASKED rather than
// resolved, so a listing taken through `current/` can be compared against the
// `current/` paths the views use. If glob ever started returning the snapshot
// directory it points at, the comparison would match nothing and the check
// would refuse every read of a perfectly healthy file.
func TestPointerStateView_readsThroughTheSymlink(t *testing.T) {
	root, paths := pointerRoot(t)
	db := execViews(t, pointerFixture(t, root, paths))

	var status string
	if err := db.QueryRow(`SELECT "status" FROM shop.orders`).Scan(&status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if status != "here" {
		t.Errorf("state view read %q; want \"here\"", status)
	}
}

func TestPointerStateView_raisesWhenTheTableLeftTheSnapshot(t *testing.T) {
	root, paths := pointerRoot(t)
	sqlText := pointerFixture(t, root, paths)

	// Generated while the table was there, read after it went: a DROP at the
	// source between the download and the query.
	if err := os.RemoveAll(filepath.Dir(paths[1])); err != nil {
		t.Fatalf("remove table: %v", err)
	}

	db, err := loadViews(t, sqlText)
	if err == nil {
		t.Fatal("the generated file loaded cleanly with shop.orders gone from the " +
			"pointed-to snapshot; a table that left must fail loudly, not read as empty")
	}
	if !strings.Contains(err.Error(), "shop/orders.parquet") {
		t.Errorf("failed with %v; want the refusal naming the table that is not there", err)
	}
	if !strings.Contains(err.Error(), baseline.CurrentLinkName) {
		t.Errorf("failed with %v; the message must name where it looked", err)
	}

	var n int
	if err := db.QueryRow(
		`SELECT count(*) FROM duckdb_views() WHERE NOT internal`).Scan(&n); err != nil {
		t.Fatalf("count views: %v", err)
	}
	if n != 0 {
		t.Errorf("the refused file left %d view(s) defined; the check has to run before "+
			"the first CREATE, not at the view that happens to break", n)
	}
}

// TestPointerStateView_acceptsATableNestedDeeper pins the preflight's glob as a
// SUPERSET of whatever tails the producer hands over.
//
// snapshotRel returns everything after the snapshot directory, so the depth of a
// Rel is the layout's business, not this check's. A glob narrower than that does
// not weaken the check, it inverts it: every table looks absent and a healthy
// file is refused before it creates anything. No layout produces this tail
// today, which is exactly why the coupling would go unnoticed.
func TestPointerStateView_acceptsATableNestedDeeper(t *testing.T) {
	root, paths := pointerRoot(t)

	// Same bytes, one directory further down than any current layout puts them.
	nested := filepath.Join(root, baseline.CurrentLinkName, "shop", "archived", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(nested), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	if err := os.WriteFile(nested, body, 0o644); err != nil {
		t.Fatalf("write nested table: %v", err)
	}

	in := Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   root,
		BaselineSnapshot: time.Date(2026, 5, 30, 3, 0, 0, 0, time.UTC),
		Follow:           FollowPointer,
		Baselines: []BaselineTable{{
			Schema: "shop", Table: "orders",
			Path: nested,
			Rel:  "shop/archived/orders.parquet",
		}},
	}

	db := execViews(t, Generate(in))
	var status string
	if err := db.QueryRow(`SELECT "status" FROM shop.orders`).Scan(&status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if status != "here" {
		t.Errorf("state view read %q; want \"here\"", status)
	}
}

// TestPointerFollow_carriesRelForARootThatIsNotAlreadyClean pins the fix for the
// one shape that made a following file promise a check it did not carry (#1558).
//
// The pointer branch sets Follow, and the preflight needs Rel. Those two used to
// come from DIFFERENT derivations of the same path: baseline.RewriteToPointer
// resolves with filepath.Rel, which cleans both sides, while views cut a raw
// byte prefix. They agree only for a root that is already clean, so
// `--baseline-dir ./baselines`, a trailing `//` from an expanded variable, or
// `/data/./baselines` produced a file that followed, printed "caught by the
// check below" in its own header, and contained no check.
//
// The roots here are the flag spellings, not exotica: the repo's own docs write
// this flag as `./baselines`.
func TestPointerFollow_carriesRelForARootThatIsNotAlreadyClean(t *testing.T) {
	clean, paths := pointerRoot(t)

	for _, root := range []string{clean, clean + "/", clean + "//", clean + "/.", filepath.Join(clean, "x", "..")} {
		t.Run(root, func(t *testing.T) {
			in := Input{
				GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
				Version:          "test",
				BaselineSource:   root,
				BaselineSnapshot: time.Date(2026, 5, 30, 3, 0, 0, 0, time.UTC),
				Baselines: []BaselineTable{
					{Schema: "audit", Table: "events", Path: paths[0]},
					{Schema: "shop", Table: "orders", Path: paths[1]},
				},
			}
			ApplyFollow(&in, root, false)
			if in.Follow != FollowPointer {
				t.Fatalf("root %q did not follow; want FollowPointer", root)
			}
			for _, b := range in.Baselines {
				if b.Rel == "" {
					t.Fatalf("root %q follows but left %s.%s without a Rel, so the preflight is "+
						"skipped while the header still promises it", root, b.Schema, b.Table)
				}
			}
			sqlText := Generate(in)
			if !strings.Contains(sqlText, missingVar) {
				t.Fatalf("root %q produced a following file with NO preflight:\n%s", root, sqlText)
			}
			// And the file still loads: a Rel derived from a cleaned root must
			// still match what glob reports for the path the views read.
			execViews(t, sqlText)
		})
	}
}

// TestPointerFollow_acceptsARootWithGlobMetacharacters pins the OTHER direction
// of the preflight's one dangerous failure: refusing a healthy file.
//
// The check compares each table against `glob(<dir> || '**/*.parquet')`, and
// under FollowPointer that dir is an operator-supplied path concatenated into a
// PATTERN. Measured against a real directory: a `[` makes the listing return
// nothing, so every table grades missing and a file whose every table is present
// is refused before it creates anything; `*` and `?` make it match siblings, so
// a table that really is gone can grade present. Both are wrong, and only one of
// them is loud.
//
// The paths in the view bodies are unaffected — read_parquet takes a literal —
// which is exactly why nothing else in this package would notice.
func TestPointerFollow_acceptsARootWithGlobMetacharacters(t *testing.T) {
	// Only `[` DISCRIMINATES: measured with the escaping removed, `*`, `?` and
	// `{` all still match here, because each root sits alone in a fresh
	// TempDir with no sibling to over-match and a lone `{` is inert. They are
	// kept as cases because they are the characters the escaping handles and a
	// future DuckDB could change its mind about any of them; the one that fails
	// today without the fix is meta[1].
	for _, name := range []string{"meta[1]", "meta*x", "meta?x", "meta{a"} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), name)
			if err := os.MkdirAll(root, 0o755); err != nil {
				t.Fatal(err)
			}
			const stamp = "2026-05-30T03-00-00Z"
			src := writeSnapshot(t, root, stamp, true, "here")
			if err := baseline.PublishCurrentPointer(filepath.Join(root, stamp)); err != nil {
				t.Fatalf("publish pointer: %v", err)
			}
			in := Input{
				GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
				Version:          "test",
				BaselineSource:   root,
				BaselineSnapshot: time.Date(2026, 5, 30, 3, 0, 0, 0, time.UTC),
				Baselines:        []BaselineTable{{Schema: "shop", Table: "orders", Path: src}},
			}
			ApplyFollow(&in, root, false)
			if in.Follow != FollowPointer {
				t.Fatalf("root %q did not follow", root)
			}
			sqlText := Generate(in)
			if !strings.Contains(sqlText, missingVar) {
				t.Fatalf("no preflight emitted for %q", root)
			}
			db := execViews(t, sqlText)
			var status string
			if err := db.QueryRow(`SELECT "status" FROM shop.orders`).Scan(&status); err != nil {
				t.Fatalf("query state view: %v", err)
			}
			if status != "here" {
				t.Errorf("state view read %q; want \"here\"", status)
			}
		})
	}
}

// TestPointerFollow_refusesToFollowABackslashRoot pins a refusal, not a feature.
//
// DuckDB's glob treats `\` as a path SEPARATOR: measured, a pattern under a root
// containing one matches NOTHING, while read_parquet reads the same literal path
// fine. So a followed file would build working view bodies and a dropped-table
// check that refused every table of a healthy snapshot. No escape reaches it —
// as-is, doubled, and a single-character class were all tried.
//
// The answer is to stop following. The file is pinned, its header says pinned,
// and it promises no check it does not carry. This asserts all three, because a
// refusal that left the header still promising would be the worse bug.
func TestPointerFollow_refusesToFollowABackslashRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), `back\up`)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	const stamp = "2026-05-30T03-00-00Z"
	src := writeSnapshot(t, root, stamp, true, "here")
	if err := baseline.PublishCurrentPointer(filepath.Join(root, stamp)); err != nil {
		t.Fatalf("publish pointer: %v", err)
	}
	in := Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   root,
		BaselineSnapshot: time.Date(2026, 5, 30, 3, 0, 0, 0, time.UTC),
		Baselines:        []BaselineTable{{Schema: "shop", Table: "orders", Path: src}},
	}
	ApplyFollow(&in, root, false)
	if in.Follow != FollowNone {
		t.Fatalf("Follow = %v for a root DuckDB's glob cannot express; the check would refuse "+
			"every table of a healthy snapshot", in.Follow)
	}
	sqlText := Generate(in)
	if strings.Contains(sqlText, "caught by the check below") {
		t.Error("the header promises the dropped-table check on a file that does not follow")
	}
	// And the pinned file still WORKS: the bodies read a literal path, which is
	// the half glob disagrees with.
	db := execViews(t, sqlText)
	var status string
	if err := db.QueryRow(`SELECT "status" FROM shop.orders`).Scan(&status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if status != "here" {
		t.Errorf("state view read %q; want \"here\"", status)
	}
}

// TestHeaderPromisesTheCheckOnlyWhenItIsThere closes the one path where the two
// could still come apart: a filtered render that selects no state view returns
// before the preflight, while the header has already been written.
func TestHeaderPromisesTheCheckOnlyWhenItIsThere(t *testing.T) {
	root, paths := pointerRoot(t)
	sqlText := pointerFixture(t, root, paths)
	if !strings.Contains(sqlText, "caught by the check below") || !strings.Contains(sqlText, missingVar) {
		t.Fatal("the ordinary following render lost either the promise or the check; " +
			"this guard reads both")
	}

	in := Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   root,
		BaselineSnapshot: time.Date(2026, 5, 30, 3, 0, 0, 0, time.UTC),
		Baselines: []BaselineTable{
			{Schema: "audit", Table: "events", Path: paths[0]},
			{Schema: "shop", Table: "orders", Path: paths[1]},
		},
		OnlyViews: map[string]bool{"events": true},
	}
	ApplyFollow(&in, root, false)
	filtered := Generate(in)
	if strings.Contains(filtered, missingVar) {
		t.Fatal("the filtered render emitted a preflight; this guard reads the case where it does not")
	}
	if strings.Contains(filtered, "caught by the check below") {
		t.Error("a render with no state views still promises the dropped-table check. A file that " +
			"advertises a guarantee it does not carry is worse than one that carries neither")
	}
}
