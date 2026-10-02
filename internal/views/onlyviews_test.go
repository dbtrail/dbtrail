package views

import (
	"strings"
	"testing"
)

// createdViews lists the view names a rendered script defines, in order.
func createdViews(t *testing.T, sql string) []string {
	t.Helper()
	var names []string
	for _, line := range strings.Split(sql, "\n") {
		const marker = "CREATE OR REPLACE VIEW "
		i := strings.Index(line, marker)
		if i < 0 {
			continue
		}
		name := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line[i+len(marker):]), " AS"))
		names = append(names, strings.ReplaceAll(name, `"`, ""))
	}
	return names
}

// TestGenerateViews_onlyWhatIsAsked pins the #1526 filter: a caller that names
// the views it needs gets those and nothing else, and a caller that names none
// gets no script at all. Defining a view over Parquet BINDS its columns, which
// is a file read and a network round trip per file on an S3 layout, so a view
// nobody asked for is latency nobody asked for.
func TestGenerateViews_onlyWhatIsAsked(t *testing.T) {
	in := goldenInput()
	all := createdViews(t, GenerateViews(in))
	if len(all) < 4 || all[0] != "events" {
		t.Fatalf("unfiltered render defines %v, want the events view and every state view", all)
	}

	for _, tc := range []struct {
		name string
		only ViewSet
		want []string
	}{
		{"nil selects everything", nil, all},
		{"one state view", ViewSet{"shop.orders": true}, []string{"shop.orders"}},
		{"the events view alone", ViewSet{"events": true}, []string{"events"}},
		{"two views", ViewSet{"events": true, "shop.orders": true}, []string{"events", "shop.orders"}},
		{"a name this layout does not define selects nothing", ViewSet{"nope": true}, nil},
		{"an empty set selects nothing", ViewSet{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := goldenInput()
			in.OnlyViews = tc.only
			got := createdViews(t, GenerateViews(in))
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("defined %v, want %v", got, tc.want)
			}
		})
	}
}

// TestGenerateViews_selectsNothingRendersNothing: the caller EXECUTES this
// text, and DuckDB answers a script of only comments with "empty query". So a
// render that defines no view must be empty, not a page of explanation.
func TestGenerateViews_selectsNothingRendersNothing(t *testing.T) {
	in := goldenInput()
	in.OnlyViews = ViewSet{}
	if got := GenerateViews(in); got != "" {
		t.Fatalf("a render that defines no view returned %d bytes, want none:\n%s", len(got), got)
	}
	// The same must hold for a layout that has nothing to define in the first
	// place, which is the shape the unfiltered path can also reach.
	if got := GenerateViews(Input{}); got != "" {
		t.Fatalf("an empty layout rendered %d bytes, want none:\n%s", len(got), got)
	}
}

// TestGenerateViews_stateNamesDoNotMoveWhenFiltered guards the rule that names
// are assigned over EVERY table, not over the selected ones. The golden fixture
// has a deliberate collision (shop.ORDER_ITEMS and shop.order_items are one name to
// DuckDB), and shop.ORDER_ITEMS only gets a suffix because its lowercase twin
// is in the snapshot. Name them after
// filtering and that suffix disappears the moment the sibling is left out —
// which would mean the name a reader queries by depends on the statement they
// wrote, and their own query would stop resolving.
func TestGenerateViews_stateNamesDoNotMoveWhenFiltered(t *testing.T) {
	in := goldenInput()
	full := createdViews(t, GenerateViews(in))
	var collided string
	for _, n := range full {
		if strings.HasPrefix(n, "shop.ORDER_ITEMS_") {
			collided = n
		}
	}
	if collided == "" {
		t.Fatalf("the fixture no longer collides on a state view name (%v); this guard covers nothing", full)
	}

	in.OnlyViews = ViewSet{asciiLower(collided): true}
	got := createdViews(t, GenerateViews(in))
	if len(got) != 1 || got[0] != collided {
		t.Fatalf("selecting %q alone defined %v, want just it", collided, got)
	}
	if !strings.Contains(GenerateViews(in), "/shop/ORDER_ITEMS.parquet") {
		t.Errorf("%q was rendered over the wrong file: the suffix moved to another table", collided)
	}
}

// TestDefinedViews reports exactly what a render defines, which is what a
// caller filtering by name needs in order to tell a name it can build from one
// it cannot.
func TestDefinedViews(t *testing.T) {
	in := goldenInput()
	if got, want := strings.ReplaceAll(strings.Join(in.DefinedViews(), ","), `"`, ""), strings.Join(createdViews(t, GenerateViews(in)), ","); got != want {
		t.Fatalf("DefinedViews = %s, but the render defines %s", got, want)
	}
	// No archive source and no live index: there is no events view to name.
	noEvents := goldenInput()
	noEvents.ArchiveSources = nil
	for _, n := range noEvents.DefinedViews() {
		if n == "events" {
			t.Fatal("DefinedViews claims an events view for a layout with no archive source and no index")
		}
	}
	// A registry that could not be READ defines no events view either, whatever
	// the source list happens to hold.
	failed := goldenInput()
	failed.ArchiveDiscoveryFailed = true
	for _, n := range failed.DefinedViews() {
		if n == "events" {
			t.Fatal("DefinedViews claims an events view although archive discovery failed")
		}
	}
	if got := createdViews(t, GenerateViews(failed)); len(got) == 0 || got[0] == "events" {
		t.Fatalf("the render disagrees with DefinedViews about the events view: %v", got)
	}
}

// TestGeneratedViewsAreIndependent is the premise the per-statement selection
// rests on: no generated view reads another, so building the ones a statement
// names needs no dependency closure. If a future view is ever defined in terms
// of `events` or of a `state_*` sibling, this fails, and the panel's selection
// has to grow that closure before the view lands.
func TestGeneratedViewsAreIndependent(t *testing.T) {
	in := goldenInput()
	sql := GenerateViews(in)
	names := in.DefinedViews()
	if len(names) < 2 {
		t.Fatalf("fixture defines %v; this guard needs at least two views", names)
	}
	// Split the script into one chunk per view definition, then look for any
	// other view's name inside a chunk's own body.
	chunks := strings.Split(sql, "CREATE OR REPLACE VIEW ")
	for _, chunk := range chunks[1:] {
		self := strings.Trim(strings.SplitN(chunk, " AS", 2)[0], `" `)
		body := strings.SplitN(chunk, " AS", 2)[1]
		// Comments carry prose about the other views; only the statement matters.
		var stmt []string
		for _, line := range strings.Split(body, "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				stmt = append(stmt, line)
			}
		}
		text := strings.Join(stmt, "\n")
		for _, other := range names {
			if other == self {
				continue
			}
			if strings.Contains(text, `"`+other+`"`) {
				t.Errorf("view %q reads %q: the SQL panel builds only the views a statement names, "+
					"so a view that depends on another needs that dependency built too", self, other)
			}
		}
	}
}

// TestNeedsS3_honorsOnlyViews: NeedsS3 answers "does this render reach S3", and
// a filtered render reaches only what it defines. Over-reporting is latency
// rather than a wrong answer (the caller resolves a credential chain nothing
// uses), which is exactly why it needs a guard: nothing else would notice.
func TestNeedsS3_honorsOnlyViews(t *testing.T) {
	s3Both := Input{
		ArchiveSources: []string{"s3://bucket/bintrail_id=x"},
		Baselines:      []BaselineTable{{Schema: "shop", Table: "orders", Path: "s3://bucket/state/orders.parquet"}},
	}
	s3ArchiveOnly := Input{
		ArchiveSources: []string{"s3://bucket/bintrail_id=x"},
		Baselines:      []BaselineTable{{Schema: "shop", Table: "orders", Path: "/local/state/orders.parquet"}},
	}
	s3BaselineOnly := Input{
		ArchiveSources: []string{"/local/bintrail_id=x"},
		Baselines:      []BaselineTable{{Schema: "shop", Table: "orders", Path: "s3://bucket/state/orders.parquet"}},
	}
	for _, tc := range []struct {
		name string
		in   Input
		only ViewSet
		want bool
	}{
		{"unfiltered, S3 everywhere", s3Both, nil, true},
		{"no view at all", s3Both, ViewSet{}, false},
		{"only the events view, archives on S3", s3ArchiveOnly, ViewSet{"events": true}, true},
		{"only a state view, archives on S3", s3ArchiveOnly, ViewSet{"shop.orders": true}, false},
		{"only the events view, baseline on S3", s3BaselineOnly, ViewSet{"events": true}, false},
		{"only a state view, baseline on S3", s3BaselineOnly, ViewSet{"shop.orders": true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in
			in.OnlyViews = tc.only
			if got := in.NeedsS3(); got != tc.want {
				t.Fatalf("NeedsS3 = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSelectedBaselines reports the baseline tables a filtered render actually
// reads, which is what a caller warning about those files has to count over.
func TestSelectedBaselines(t *testing.T) {
	in := goldenInput()
	if got, want := len(in.SelectedBaselines()), len(in.Baselines); got != want {
		t.Fatalf("unfiltered render reads %d baseline tables, want all %d", got, want)
	}
	in.OnlyViews = ViewSet{}
	if got := in.SelectedBaselines(); len(got) != 0 {
		t.Fatalf("a render that defines no view still reads %d baseline tables", len(got))
	}
	in.OnlyViews = ViewSet{"shop.orders": true}
	got := in.SelectedBaselines()
	if len(got) != 1 || got[0].Schema != "shop" || got[0].Table != "orders" {
		t.Fatalf("SelectedBaselines = %+v, want just shop.orders", got)
	}
	// It reports the tables of the views that are RENDERED, so it has to agree
	// with the render itself.
	if names := createdViews(t, GenerateViews(in)); len(names) != len(got) {
		t.Fatalf("the render defines %v but SelectedBaselines reports %d tables", names, len(got))
	}
}
