package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// baselineDirWithATable is a real discoverable snapshot, not an empty
// directory. The distinction is load-bearing: an EMPTY baseline dir defines no
// state view either, so a "default file has no events view" assertion driven
// from one passes without ever showing that the state views survived the flip
// -- and now it does not even get that far, since a render with no view at all
// is refused.
func baselineDirWithATable(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// saveViewsFlags restores every package-level flag var this file touches. They
// are globals cobra binds to, so a test that sets one and does not put it back
// changes what the NEXT test observes.
func saveViewsFlags(t *testing.T) {
	t.Helper()
	strs := []*string{&vIndexDSN, &vArchiveDir, &vArchiveS3, &vBintrailID, &vBaselineDir, &vBaselineS3, &vOut, &vDatabase, &vOldSchema}
	strVals := make([]string, len(strs))
	for i, p := range strs {
		strVals[i] = *p
	}
	bools := []*bool{&vNoBaselines, &vIncludeLive, &vIncludeEvents, &vPinSnapshot}
	boolVals := make([]bool, len(bools))
	for i, p := range bools {
		boolVals[i] = *p
	}
	t.Cleanup(func() {
		for i, p := range strs {
			*p = strVals[i]
		}
		for i, p := range bools {
			*p = boolVals[i]
		}
	})
}

func runViewsToString(t *testing.T) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetContext(context.Background())
	err := runViews(cmd, nil)
	return out.String(), err
}

// TestRunViews_eventsViewIsOptIn is THE assertion for #1535: the default file
// is the cheap one. Defining the events view opens one Parquet footer per
// archived file before it returns a row, so a default that includes it charges
// every reader for the whole change log to get their tables.
//
// Driven through runViews rather than views.Generate: the package-level test
// covers the renderer, and what can silently regress is the CLI forgetting to
// set the field at all.
func TestRunViews_eventsViewIsOptIn(t *testing.T) {
	saveViewsFlags(t)
	vIndexDSN, vBaselineDir, vBaselineS3 = "", "", ""
	vArchiveDir, vArchiveS3, vBintrailID = t.TempDir(), "", "aaaa"
	vNoBaselines, vIncludeLive, vIncludeEvents, vOut = true, false, true, "-"

	withEvents, err := runViewsToString(t)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(withEvents, `CREATE OR REPLACE VIEW "events"`) {
		t.Fatalf("--include-events did not produce the events view:\n%s", withEvents)
	}

	// Same layout, without the flag. --no-baselines has to go with it, or the
	// file would define nothing and the command refuses.
	vNoBaselines, vIncludeEvents = false, false
	vBaselineDir = baselineDirWithATable(t)
	byDefault, err := runViewsToString(t)
	if err != nil {
		t.Fatal(err)
	}
	// The state views are what the default file is FOR. Without this the
	// assertion below would pass on a file with nothing in it.
	if !strings.Contains(byDefault, `CREATE OR REPLACE VIEW "shop"."orders"`) {
		t.Fatalf("the default file defines no state view, so it proves nothing about "+
			"the events view being the only thing left out:\n%s", byDefault)
	}
	if strings.Contains(byDefault, `CREATE OR REPLACE VIEW "events"`) {
		t.Errorf("the DEFAULT file still defines the events view, so every reader pays "+
			"one Parquet footer read per archived file to get their tables:\n%s", byDefault)
	}
	if !strings.Contains(byDefault, "--include-events") {
		t.Errorf("the default file never says how to ask for the events view:\n%s", byDefault)
	}
}

// TestRunViews_refusesTheCombinationsThatDefineNothing: both refusals exist
// because the alternative is a command that succeeds and hands back something
// useless — an empty file, or a live leg with no view to hang on. Refused
// rather than quietly adjusted: turning the expensive view on because the
// operator asked for its leg is a cost they pay on every query afterwards.
func TestRunViews_refusesTheCombinationsThatDefineNothing(t *testing.T) {
	for _, tc := range []struct {
		name        string
		noBaselines bool
		includeLive bool
		wantSays    string
	}{
		{"no baselines and no events defines nothing", true, false, "leaves nothing to define"},
		{"a live leg with no view to hang on", false, true, "adds a leg to the events view"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saveViewsFlags(t)
			vIndexDSN, vBaselineDir, vBaselineS3 = "", baselineDirWithATable(t), ""
			vArchiveDir, vArchiveS3, vBintrailID = t.TempDir(), "", "aaaa"
			vNoBaselines, vIncludeLive, vIncludeEvents, vOut = tc.noBaselines, tc.includeLive, false, "-"

			out, err := runViewsToString(t)
			if err == nil {
				t.Fatalf("the command succeeded and produced:\n%s", out)
			}
			if !strings.Contains(err.Error(), tc.wantSays) {
				t.Errorf("the refusal does not say why: %v", err)
			}
			// A refusal that does not name the way out is a dead end.
			if !strings.Contains(err.Error(), "--include-events") {
				t.Errorf("the refusal never names the flag that fixes it: %v", err)
			}
		})
	}

	// The positive control: with --include-events both combinations are legal,
	// so the refusals above are about the missing view and not about the flags
	// themselves.
	t.Run("both are fine with the events view", func(t *testing.T) {
		saveViewsFlags(t)
		vIndexDSN, vBaselineDir, vBaselineS3 = "", "", ""
		vArchiveDir, vArchiveS3, vBintrailID = t.TempDir(), "", "aaaa"
		vNoBaselines, vIncludeLive, vIncludeEvents, vOut = true, false, true, "-"
		if _, err := runViewsToString(t); err != nil {
			t.Errorf("--no-baselines --include-events was refused: %v", err)
		}
	})
}

// TestRunViews_refusesAFileWithNoViewInIt is the guard for the regression the
// flip introduced and the flag refusals could not catch.
//
// `--no-baselines` is refused by name, but the IDENTICAL empty file is reached
// by simply not naming a baseline location — and before the flip that shape
// rendered the events view, so it was a useful command. After it, and before
// this refusal, it exited 0, printed "wrote views.sql", and left a file with
// nothing in it. The refusal is keyed on the OUTCOME for that reason: a
// flag-shaped one is only ever right about the paths someone thought of.
func TestRunViews_refusesAFileWithNoViewInIt(t *testing.T) {
	saveViewsFlags(t)
	vIndexDSN, vBaselineDir, vBaselineS3 = "", "", ""
	vArchiveDir, vArchiveS3, vBintrailID = t.TempDir(), "", "aaaa"
	vNoBaselines, vIncludeLive, vIncludeEvents, vOut = false, false, false, "-"

	out, err := runViewsToString(t)
	if err == nil {
		t.Fatalf("a file with no view in it was written successfully:\n%s", out)
	}
	for _, want := range []string{"no view at all", "--baseline-dir", "--include-events"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal never mentions %q, so it names neither the problem "+
				"nor either way out: %v", want, err)
		}
	}

	// The positive control: the SAME invocation with a real baseline succeeds.
	// Without it this test would pass on a command that refuses everything.
	vBaselineDir = baselineDirWithATable(t)
	if _, err := runViewsToString(t); err != nil {
		t.Errorf("a layout that DOES define a state view was refused too: %v", err)
	}
}

// TestRunViews_summaryNamesTheOmittedView: an unchanged scripted invocation
// keeps working after the flip and quietly loses the events view. The one line
// the operator sees has to say so, or the flip is invisible until DuckDB
// reports a table that does not exist. Reporting the archive-source COUNT for a
// file that reads no archive path was the shape that hid it.
func TestRunViews_summaryNamesTheOmittedView(t *testing.T) {
	saveViewsFlags(t)
	dir := t.TempDir()
	vIndexDSN, vBaselineDir, vBaselineS3 = "", baselineDirWithATable(t), ""
	vArchiveDir, vArchiveS3, vBintrailID = dir, "", "aaaa"
	vNoBaselines, vIncludeLive, vIncludeEvents = false, false, false
	vOut = filepath.Join(t.TempDir(), "views.sql")

	out, err := runViewsToString(t)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "events view: omitted") {
		t.Errorf("the summary does not say the events view was left out:\n%s", out)
	}
	if !strings.Contains(out, "--include-events") {
		t.Errorf("the summary does not name how to get it back:\n%s", out)
	}

	// With the view IN the file the same line must say so, or "omitted" would
	// be a constant rather than a report.
	vIncludeEvents = true
	out, err = runViewsToString(t)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "omitted") {
		t.Errorf("the summary says the events view was omitted from a file that has it:\n%s", out)
	}
	if !strings.Contains(out, "archive source(s)") {
		t.Errorf("the summary does not report what the events view reads:\n%s", out)
	}
}
