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

// The views half is a promise this lane keeps only when the server can make
// the file: its button downloads it through downloadViewsSQL (#1573; it used
// to jump to the card), and GET /api/views.sql answers only under
// capsCache.views. Ungated here, the lane drew a views.sql tile and a button
// that only fails -- exactly the trade views_api.go says this codebase
// refuses ("a button that only 404s is a lie").
func TestDuckLaneGatesTheFileItDoesNotProduce(t *testing.T) {
	js := readAsset(t, "app.js")
	lane := stripJSLineComments(functionBody(t, js, "function backupDuckLane("))
	if !strings.Contains(lane, "capsCache.views") {
		t.Fatal("the DuckDB lane no longer checks capsCache.views, so it can promise a views file " +
			"that no card on the page will produce")
	}
	// Both halves must sit behind it, and "behind" means INSIDE the branch,
	// not merely later in the file. Comparing byte offsets was the first cut
	// and it passed a mutation that gated the tile and left the button loose:
	// the button still came after `const hasViews = ...`.
	for _, half := range []string{"DUCKDB_VIEWS_FILE, cap:", "downloadViewsSQL("} {
		if !guardedByHasViews(lane, half) {
			t.Errorf("%q is not inside an `if (hasViews)` branch. A gated tile beside an ungated "+
				"button still offers a file the server will not make", half)
		}
	}
}

// guardedByHasViews reports whether every line mentioning needle sits inside
// an `if (hasViews)` branch -- either the single-statement form on the same
// line, or a braced block. Line-based on purpose: the lane uses both forms,
// so a brace matcher alone would miss the one-liner that pushes the tile.
func guardedByHasViews(body, needle string) bool {
	depth, seen := 0, false
	for _, line := range strings.Split(body, "\n") {
		guardOpensHere := strings.Contains(line, "if (hasViews)")
		if strings.Contains(line, needle) {
			seen = true
			if depth == 0 && !guardOpensHere {
				return false
			}
		}
		if guardOpensHere && strings.HasSuffix(strings.TrimSpace(line), "{") {
			depth++
			continue
		}
		if depth > 0 && strings.TrimSpace(line) == "}" {
			depth--
		}
	}
	return seen
}

// The views file is named on two surfaces: the card on Connect AI that builds
// it, and the Backups take-away lane that downloads the default one. They
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
func TestTakeAwayPanelIsMountedAboveTheList(t *testing.T) {
	body := stripJSLineComments(functionBody(t, readAsset(t, "app.js"), "async function renderSnapshots("))
	// the append, not backupTakeAway(: dropping only the append leaves the
	// call sitting there, and a guard that reads the call reports a panel
	// that no reader can see. Since round 3 the lanes live on the Versions
	// tab, above the list.
	mount := strings.Index(body, "tabs.panels.versions.append(takeAway)")
	list := strings.Index(body, "baselinesPanel(")
	// The panel the lanes go into must itself reach the page, or the mount
	// above is into a node nobody sees.
	if !strings.Contains(body, "v.append(tabs.panels.versions)") {
		t.Error("the Versions panel is never appended to the view")
	}
	switch {
	case mount < 0:
		t.Fatal("Snapshots no longer mounts backupTakeAway: the Parquet download goes back " +
			"inside a row expand, and the .sql builder leaves the page entirely — backupTakeAway " +
			"is its only caller")
	case list < 0:
		t.Fatal("Snapshots no longer mounts baselinesPanel")
	case mount > list:
		t.Error("the two lanes render BELOW the snapshots list. They are the answer to why the page " +
			"was opened; the list is how you pick a different one")
	}
}
