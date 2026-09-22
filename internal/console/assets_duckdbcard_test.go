package console

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// The Download a DuckDB schema card is the only caller of GET /api/views.sql
// that can ask for more than the default file (the Backups lane asks for the
// default only, #1573), so what it can ask for IS the console's whole surface
// over that endpoint.
//
// It carried three checkboxes and three multi-line caveats until #1549's
// follow-up. The caveats are what these guards used to pin, one string at a
// time. They pin the SHAPE now, because the card explains itself by drawing
// what the file will hold instead of describing it, and a drawing fails
// differently: wrong prose reads wrong, a wrong picture looks fine.

// TestDuckDBCardOffersOneDecision. --pin-snapshot and --include-live remain
// route parameters and CLI flags; they are not decisions to put in front of a
// first-time reader, and the live leg in particular can compete with capture on
// the source server. So exactly one box, and it is the change log.
func TestDuckDBCardOffersOneDecision(t *testing.T) {
	body := functionBody(t, readAsset(t, "app.js"), "function duckdbPanel(")

	// Two, and the second one is CONDITIONAL. The card is a first visit for
	// most readers, so the bar for a control is that skipping it would produce
	// the wrong file: the change log is left out by default and has to be
	// asked for, and the backup location only has an answer to give when this
	// server has two of them (#1551). Everything else stays a CLI flag.
	boxes := strings.Count(body, `type: "checkbox"`)
	if boxes != 2 {
		t.Errorf("duckdbPanel renders %d checkboxes, want exactly 2 (the change log, and the "+
			"backup location when the server has two); pin-snapshot and include-live are CLI "+
			"flags and route parameters, not first-visit decisions", boxes)
	}
	// The point of the pair above: unconditional, it is a control that changes
	// nothing for every server with one backup location, which is most of them.
	if !strings.Contains(body, "if (capsCache.views_portable_baseline) {") {
		t.Error("the backup-location box is not gated on the capability, so it is offered for " +
			"servers with only one location, where it changes nothing")
	}
	// The parameters are built in downloadViewsSQL, the one download path the
	// card and the Backups take-away lane share (#1573); the card must hand
	// its two boxes to it.
	dl := functionBody(t, readAsset(t, "app.js"), "async function downloadViewsSQL(")
	if !strings.Contains(body, "downloadViewsSQL({ events: events.checked, portable: !!(portable && portable.checked) })") {
		t.Error("the card does not hand its two boxes to downloadViewsSQL, so ticking them changes nothing")
	}
	if !strings.Contains(dl, "include_events=1") {
		t.Error("the download never sends include_events=1, so the change log cannot be asked for at all")
	}
	if !strings.Contains(dl, "portable_baseline=1") {
		t.Error("the download never sends portable_baseline=1, so its box changes nothing")
	}
	// Conditional, not always: the change log binds every archived file, so a
	// download nobody asked for it must not carry it.
	if !strings.Contains(dl, `if (opts.events) q.push("include_events=1")`) {
		t.Error("the change log is not conditional on the box, so the default download is not the cheap one")
	}
	// Its own filename. Saved as views.sql, the second download silently
	// replaces the first in the reader's downloads folder.
	if !strings.Contains(dl, `"views-portable.sql"`) {
		t.Error("both downloads are saved under one filename, so one silently overwrites the other")
	}
	// The two retired controls must not come back as silent always-on
	// parameters, which would be worse than the checkboxes were.
	for _, gone := range []string{"include_live", "pin_snapshot"} {
		if strings.Contains(body+dl, gone) {
			t.Errorf("duckdbPanel names %s again; it was removed from this surface, not moved into a default", gone)
		}
	}
}

// TestDuckDBCardDrawsTheCostInsteadOfStatingIt is the guard for the drawing.
//
// The card used to carry "It takes longer to open the further back your archive
// goes, because it reads a piece of every archived file". That sentence is the
// shape of the data, and the shape is drawable: your tables are one file each,
// the change log is one file per archived hour. Ticking the box lights the
// strip, so the cost is seen.
//
// A picture can lie in a way prose cannot: nobody reports a diagram that merely
// looks plausible. So the two halves are pinned to the one parameter the card
// can send. If the strip stops being tied to the change-log box, the drawing
// starts claiming something the download does not do.
func TestDuckDBCardDrawsTheCostInsteadOfStatingIt(t *testing.T) {
	js := readAsset(t, "app.js")
	shape := functionBody(t, js, "function duckdbShape(")
	card := functionBody(t, js, "function duckdbPanel(")

	// Both halves exist, and the change log is drawn as MANY units against the
	// tables' few. That contrast is the whole explanation; equal counts would
	// render a picture that says the two cost the same.
	tiles := regexp.MustCompile(`tiles\((\d+), "dk-tile"`).FindStringSubmatch(shape)
	bars := regexp.MustCompile(`tiles\((\d+), "dk-bar"`).FindStringSubmatch(shape)
	if tiles == nil || bars == nil {
		t.Fatal("duckdbShape no longer draws both a tile row (your tables) and a bar row (the change log)")
	}
	// Compared as NUMBERS. This read len() of the digit strings, which is not
	// quantity: it passed 24-vs-3 for the wrong reason and would have failed a
	// perfectly good 9-vs-3. A picture guard that cannot compare the two
	// quantities it exists to compare is decoration.
	nTiles, err1 := strconv.Atoi(tiles[1])
	nBars, err2 := strconv.Atoi(bars[1])
	if err1 != nil || err2 != nil {
		t.Fatalf("could not read the drawn counts (tiles=%q bars=%q)", tiles[1], bars[1])
	}
	if nBars < nTiles*4 {
		t.Errorf("the change log is drawn with %d units against the tables' %d; the picture has to read as a "+
			"different order of quantity, not as slightly more", nBars, nTiles)
	}

	// The strip is dimmed until the box is ticked, and the card is what wires
	// them together. Either half alone leaves a drawing that never changes.
	if !strings.Contains(shape, "dk-off") {
		t.Error("duckdbShape never applies dk-off, so the change-log half is drawn as if it were always included")
	}
	if !strings.Contains(card, `shape.events.classList.toggle("dk-off"`) {
		t.Error("the change-log box does not light the strip, so ticking it explains nothing")
	}
	if !strings.Contains(card, "events.onchange();") {
		t.Error("the initial state is never synced, so the strip renders lit before the box is ticked")
	}

	// Built with el(), not svgEl: that helper DOMParses STATIC icon constants
	// only, and routing a drawing through it is how a string-to-DOM path grows
	// an interpolated argument later.
	if strings.Contains(shape, "svgEl(") {
		t.Error("duckdbShape builds through svgEl, which is for static icon constants; draw with el()")
	}
}

// TestDuckDBCardStaysNearlyTextless is the Go half of the 300-character budget
// the e2e enforces on the rendered card. It cannot count rendered text, so it
// counts what the source can produce, and exists to fail on the desk rather
// than in CI when someone starts explaining again.
func TestDuckDBCardStaysNearlyTextless(t *testing.T) {
	js := readAsset(t, "app.js")
	body := functionBody(t, js, "function duckdbPanel(")
	// Only text OUTSIDE the fold: cnFine's contents are one click away and are
	// not what a first-time reader meets. Everything up to the fold is visible.
	visible := body
	if i := strings.Index(body, "cnFine("); i >= 0 {
		visible = body[:i]
	}
	total := 0
	for _, m := range regexp.MustCompile(`text:\s*((?:"(?:[^"\\]|\\.)*"\s*\+?\s*)+)`).FindAllStringSubmatch(visible, -1) {
		for _, lit := range regexp.MustCompile(`"((?:[^"\\]|\\.)*)"`).FindAllStringSubmatch(m[1], -1) {
			total += len(lit[1])
		}
	}
	if total == 0 {
		t.Fatal("no visible text found in duckdbPanel; this guard covers nothing")
	}
	if total > 200 {
		t.Errorf("duckdbPanel's visible text is %d characters. The card explains itself by drawing; "+
			"if something needs saying, either draw it or put it behind cnFine", total)
	}
}

// TestDuckDBCardMountsOnConnect: the DuckDB schema card lives on Connect AI
// (#1573), with or without the watch daemon, beside the other ways to take the
// data somewhere else; Backups no longer mounts it (its take-away lane
// downloads the default file itself). Between the SQL client panel and the
// Iceberg export, the order of the page from least to most setup.
func TestDuckDBCardMountsOnConnect(t *testing.T) {
	js := readAsset(t, "app.js")
	if strings.Contains(stripJSLineComments(functionBody(t, js, "async function renderBaselines(")), "duckdbPanel(") {
		t.Error("renderBaselines mounts duckdbPanel again; the card moved to Connect AI (#1573), and two " +
			"copies of one download put two different option sets in front of the reader")
	}
	connect := stripJSLineComments(functionBody(t, js, "function buildConnect("))
	// Gated on views and on settings:read, the permission GET /api/views.sql
	// checks: on Backups the listing's own permission hid it, and without the
	// second half a session denied it sees a button that can only be refused.
	mount := strings.Index(connect, `if (capsCache.views && (capsCache.permissions || {})["settings:read"] !== false) v.append(duckdbPanel())`)
	sql := strings.Index(connect, "sqlClientPanel(")
	ice := strings.Index(connect, "icebergExportPanel(")
	switch {
	case mount < 0:
		t.Fatal("buildConnect does not mount duckdbPanel on capsCache.views plus settings:read; with " +
			"the daemon running (monitor on) the views download would have no page at all, or a " +
			"session denied settings:read would get a button that only 403s")
	case sql < 0 || ice < 0:
		t.Fatal("buildConnect no longer mounts the SQL client panel or the Iceberg panel; re-anchor this order check")
	case !(sql < mount && mount < ice):
		t.Error("the DuckDB card is out of order on Connect AI: it belongs after the SQL client panel " +
			"and before the Iceberg export")
	}
}

// functionBody returns the text of one top-level function in app.js, from its
// declaration to the next top-level declaration.
//
// It lived in assets_sqlpanel_warnings_test.go until #1549 deleted that file
// with the SQL panel. Three other guards depend on it (capacity notes, the
// Iceberg panel, and the card above), so it moves here rather than going with
// the panel — this file is its heaviest remaining user.
func functionBody(t *testing.T, js, decl string) string {
	t.Helper()
	i := strings.Index(js, decl)
	if i < 0 {
		t.Fatalf("%s is gone from assets/app.js; this guard covers nothing", strings.TrimSuffix(decl, "("))
	}
	rest := js[i:]
	for _, stop := range []string{"\nfunction ", "\nasync function "} {
		if j := strings.Index(rest[1:], stop); j > 0 {
			rest = rest[:j+1]
		}
	}
	return rest
}

// TestDuckDBCardNamesMatchTheFileItParses is the anti-lying guard for the view
// list the card shows after a download (#1551 follow-up).
//
// The card reads the names out of the file's own bytes rather than asking for
// them, so a second source cannot drift from the first. What CAN drift is the
// shape it greps for. This drives views.Generate and extracts with a port of the
// JavaScript regex, so a change to how the generator writes a CREATE statement
// fails here instead of silently rendering an empty list.
func TestDuckDBCardNamesMatchTheFileItParses(t *testing.T) {
	in := views.Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		BaselineSource:   "/data/baselines",
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Baselines: []views.BaselineTable{
			{Schema: "shop", Table: "orders", Path: "/data/baselines/s/shop/orders.parquet"},
			// Sanitizes to the same name as shop.order_items, so the generator
			// suffixes it. A hand-derived list would get this pair wrong, which
			// is half the reason the card does not derive one.
			{Schema: "shop", Table: "order_items", Path: "/data/baselines/s/shop/order_items.parquet"},
			{Schema: "shop_order", Table: "items", Path: "/data/baselines/s/shop_order/items.parquet"},
			// Neither a hyphen nor a space is legal bare in an identifier.
			{Schema: "Legacy-DB", Table: "Audit Log", Path: "/data/baselines/s/Legacy-DB/Audit Log.parquet"},
		},
	}
	sqlText := views.Generate(in)
	want := in.DefinedViews()
	if len(want) == 0 {
		t.Fatal("the fixture defines no view, so this asserts nothing")
	}

	// The same expression app.js uses. Kept as a literal rather than read out of
	// the asset: the point is that these two agree, and reading one to test
	// itself would agree with anything.
	re := regexp.MustCompile(`(?m)^CREATE OR REPLACE VIEW "([^"]+)"`)
	var got []string
	for _, m := range re.FindAllStringSubmatch(sqlText, -1) {
		got = append(got, m[1])
	}
	if !slices.Equal(got, want) {
		t.Errorf("the card would list %v; the file defines %v", got, want)
	}

	// The character set the simple pattern above relies on. sanitizeIdent folds
	// everything outside [a-z0-9_], so a name can carry no quote of its own; if
	// that ever changes, the pattern needs the quote-doubling case back and this
	// says so before the list starts truncating names.
	safe := regexp.MustCompile(`^[a-z0-9_]+$`)
	for _, n := range want {
		if !safe.MatchString(n) {
			t.Errorf("view name %q is outside [a-z0-9_]; the card's pattern assumes it is not", n)
		}
	}

	// And the asset really does carry that expression.
	body := functionBody(t, readAsset(t, "app.js"), "function duckdbViewNames(")
	if !strings.Contains(body, `/^CREATE OR REPLACE VIEW "([^"]+)"/gm`) {
		t.Error("app.js no longer greps for the statement shape this test pinned")
	}

	// Placement, which fails OPEN if it breaks: insertBefore appends when its
	// reference node is null, so moving the foot off `body` would put the list
	// below the button with nothing raised and nothing to see in a diff.
	panel := functionBody(t, readAsset(t, "app.js"), "function duckdbPanel(")
	if !strings.Contains(panel, "body.insertBefore(duckdbNameList") {
		t.Error("the view list is no longer placed relative to the card foot")
	}
	if !strings.Contains(panel, `body.append(el("div", { class: "stg-cardfoot" }`) {
		t.Error("the card foot is not appended to `body`, so insertBefore's reference is null " +
			"and the list silently lands below the button instead of above it")
	}
}
