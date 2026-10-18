package console

import (
	"regexp"
	"strings"
	"testing"
)

// The Backups page answers one question: what do I download to open this in
// DuckDB, what do I download to load it into MySQL. The drawing carries the
// difference before any sentence does -- two file tiles on the DuckDB lane,
// one on the MySQL lane -- and a drawing can lie in a way prose cannot: it
// keeps rendering the old answer after the code stops producing it, and no
// screenshot notices because a picture of two files looks correct on its own.
//
// The first cut of this guard compared the tiles a lane PASSES against a
// hardcoded sentence, and review killed it twice over. It counted the
// argument list rather than the drawing, so slicing backupFilesShape to one
// tile passed; and once the views tile became conditional, the fixed sentence
// was wrong by construction on the ungated arm.
//
// So the sentence is no longer written by hand at all. backupLane derives the
// count word from the tiles it is handed, and these three checks pin that
// arrangement: nothing downstream may re-hardcode a count, and nothing may
// draw fewer tiles than it was given.
func TestTakeAwayLaneCountIsDerivedNotWritten(t *testing.T) {
	js := readAsset(t, "app.js")
	lane := stripJSLineComments(functionBody(t, js, "function backupLane("))
	if !strings.Contains(lane, "files.length") || !strings.Contains(lane, "LANE_COUNT_WORD") {
		t.Error("backupLane no longer derives its count word from the tiles it is handed. A lane " +
			"that states its own count can disagree with its own drawing, which is the whole " +
			"defect this panel's guard exists for")
	}
	// A lane that spelled its own count would silently win over the derived
	// one, so no caller may contain a count word.
	for _, fn := range []string{"function backupDuckLane(", "function backupSQLLane("} {
		body := stripJSLineComments(functionBody(t, js, fn))
		for _, word := range []string{"One download", "Two downloads", "One file", "Two files"} {
			if strings.Contains(body, word) {
				t.Errorf("%s writes %q by hand. The count comes from backupLane, which reads the "+
					"tiles actually drawn; a hand-written one drifts the first time a tile is gated",
					fn, word)
			}
		}
	}
	// And the drawing must render every tile it is given. Slicing here was a
	// surviving mutation: the DuckDB lane drew one tile under the words for
	// two, and every assertion stayed green.
	shape := stripJSLineComments(functionBody(t, js, "function backupFilesShape("))
	if !strings.Contains(shape, "files.forEach(") || strings.Contains(shape, ".slice(") {
		t.Error("backupFilesShape no longer draws one tile per file it is handed. The tile count " +
			"is the message on this panel, so a shape that drops one lies about what you need")
	}
}

// The DuckDB lane is ONE download: the archive carries its own views.sql,
// respelled to "./" paths so it opens wherever it is unpacked (#1583), and
// that file is made whatever the views capability says. The lane used to
// offer a second button, GET /api/views.sql, whose file names this server's
// folders and so pointed at nothing on the reader's machine. The lane must
// not grow it back, nor gate the tile on a capability the archive does not
// depend on (a tile hidden for a file the download still carries).
func TestDuckLaneIsOneDownload(t *testing.T) {
	js := readAsset(t, "app.js")
	lane := stripJSLineComments(functionBody(t, js, "function backupDuckLane("))
	for _, not := range []string{"downloadViewsSQL(", "/api/views.sql", "capsCache.views"} {
		if strings.Contains(lane, not) {
			t.Errorf("backupDuckLane mentions %s: the lane is the archive alone, which carries its own views.sql", not)
		}
	}
	if !strings.Contains(lane, `"duck", 1)`) {
		t.Error("backupDuckLane no longer tells backupLane it is one download; the lead would count its two tiles as two downloads")
	}
}

// The views file is named on two surfaces: the DuckDB schema card that builds
// it, and the take-away lane that draws it as the archive's second tile. They
// share a constant so they cannot drift; this pins that neither re-hardcodes it.
func TestViewsFileIsNamedFromOneConstant(t *testing.T) {
	js := stripJSLineComments(readAsset(t, "app.js"))
	decl := regexp.MustCompile(`const DUCKDB_VIEWS_FILE = "([^"]+)"`).FindStringSubmatch(js)
	if decl == nil {
		t.Fatal("DUCKDB_VIEWS_FILE is gone: the take-away lane and the card both name that file, " +
			"and two literals in two panels drift the moment one is renamed")
	}
	// Counted as a bare substring, not as a standalone "views.sql" literal: a
	// real re-hardcode embeds the name in a longer string (text: "Get
	// views.sql"), which a quoted-literal check walks straight past. The route
	// /api/views.sql legitimately contains it and is removed first.
	body := strings.ReplaceAll(js, "/api/"+decl[1], "")
	if n := strings.Count(body, decl[1]); n != 1 {
		t.Errorf("%q appears %d times outside a comment line; only its own declaration may spell "+
			"it out. Everywhere else it must come from DUCKDB_VIEWS_FILE, or the Snapshots lane can "+
			"promise a file the schema card no longer produces", decl[1], n)
	}

	// The card's CLASS: the panel wears it and the browser test resolves it.
	// The take-away lane's jump that also resolved it is gone (#1573: the lane
	// downloads the file itself), but the rule stays: one declaration,
	// everything else through the constant.
	cardDecl := regexp.MustCompile(`const DUCKDB_CARD_CLASS = "([^"]+)"`).FindStringSubmatch(js)
	if cardDecl == nil {
		t.Fatal("DUCKDB_CARD_CLASS is gone: the card wears it and the browser test finds the card by it, " +
			"and two literals drift the first time the card is restyled")
	}
	if n := strings.Count(js, cardDecl[1]); n != 1 {
		t.Errorf("%q appears %d times outside a comment line; only the DUCKDB_CARD_CLASS "+
			"declaration may spell it out, or something can look for a class the card "+
			"no longer wears", cardDecl[1], n)
	}
	if !strings.Contains(stripJSLineComments(functionBody(t, js, "function duckdbPanel(")), "DUCKDB_CARD_CLASS") {
		t.Error("duckdbPanel no longer goes through DUCKDB_CARD_CLASS; the card and what finds it " +
			"must share one spelling")
	}
}

// A panel nobody mounts answers nothing. This is the wiring half: the guards
// above prove the lanes are right, not that a reader ever sees them.
func TestTakeAwayPanelIsMountedOnItsOwnTab(t *testing.T) {
	js := readAsset(t, "app.js")
	body := stripJSLineComments(functionBody(t, js, "async function renderSnapshots("))
	// the append, not backupTakeAway(: dropping only the append leaves the
	// call sitting there, and a guard that reads the call reports a panel
	// that no reader can see. The lanes have the Download tab to themselves:
	// above the list of snapshots they pushed it off the screen.
	if !strings.Contains(body, "if (takeAway) tabs.panels.download.append(takeAway)") {
		t.Fatal("Snapshots no longer mounts backupTakeAway: the Parquet download goes back " +
			"inside a row expand, and the .sql builder leaves the page entirely — backupTakeAway " +
			"is its only caller")
	}
	// The panel the lanes go into must itself reach the page, and the bar
	// must offer its tab, or the mount above is into a node nobody sees.
	if !strings.Contains(body, "v.append(tabs.panels.download)") {
		t.Error("the Download panel is never appended to the view")
	}
	if !strings.Contains(body, "download: !!(takeAway || inPlace || inPlaceErr)") {
		t.Error("the tab bar is not told whether there are lanes or in-place panels to show, so the Download tab is never offered")
	}
	if !strings.Contains(body, "if (inPlace) tabs.panels.download.append(inPlace)") {
		t.Error("the in-place fold (DuckDB schema, Iceberg) is not mounted on the Download tab")
	}
	if !strings.Contains(functionBody(t, js, "function snapshotTabs("), `["download", "Download"]`) {
		t.Error("the tab bar has no Download tab")
	}
	if strings.Contains(body, "tabs.panels.versions.append(takeAway)") {
		t.Error("the lanes are back above the list of snapshots")
	}
	if !strings.Contains(body, "v.append(tabs.panels.versions)") || !strings.Contains(body, "baselinesPanel(") {
		t.Error("Snapshots no longer mounts the list of snapshots on Versions")
	}
}

// The lanes sit on a tab that is closed on arrival, so an outcome waiting
// there (a .sql build that failed, one ready and not yet downloaded) has to
// reach the tab itself: without the dot a reader who comes back to the page
// lands on Versions and nothing says a build is waiting.
func TestTakeAwayOutcomeMarksTheDownloadTab(t *testing.T) {
	js := readAsset(t, "app.js")
	take := stripJSLineComments(functionBody(t, js, "function backupTakeAway("))
	if !strings.Contains(take, `if (owed) panel.dataset.owed = "1";`) {
		t.Error("backupTakeAway no longer says on the panel that an outcome is waiting")
	}
	body := stripJSLineComments(functionBody(t, js, "async function renderSnapshots("))
	read := strings.Index(body, "takeAway.dataset.owed ? tabs.bar.querySelector('.snap-tab[data-tab=\"download\"]')")
	if read < 0 {
		t.Fatal("renderSnapshots no longer looks up the Download tab when the take-away panel has an outcome waiting")
	}
	rest := body[read:]
	if end := strings.Index(rest, "});"); end < 0 || !strings.Contains(rest[:end], `t.classList.add("has-alarm")`) {
		t.Error("the Download tab is found and never marked: an outcome waits behind a tab with no dot")
	}
}
