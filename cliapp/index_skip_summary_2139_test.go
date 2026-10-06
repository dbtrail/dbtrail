package cliapp

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// The end-of-run warning of `bintrail index`, built from a real run tally. Its
// hint is about validation-excluded tables; a row that could not be mapped
// (#2139) must not be read under that hint.
func TestIndexSkipSummary(t *testing.T) {
	skips := parser.NewSkipCounters(nil)
	skips.RecordSkipAttributed(parser.SkipTableExcludedFromSnapshot, parser.SkipAttribution{Schema: "shop", Table: "scratch"})
	without := indexSkipSummary(skips.Count(parser.SkipRowMapFailed) > 0)
	if strings.Contains(without, "row_map_failed") {
		t.Errorf("row_map_failed mentioned in a run that had none:\n%s", without)
	}

	skips.RecordSkipAttributed(parser.SkipRowMapFailed, parser.SkipAttribution{Schema: "shop", Table: "notes"})
	with := indexSkipSummary(skips.Count(parser.SkipRowMapFailed) > 0)
	if !strings.HasPrefix(with, without) {
		t.Errorf("the existing warning must stay as the start of the line:\n%s", with)
	}
	added := strings.TrimPrefix(with, without)
	for _, want := range []string{
		"row_map_failed is a different cause",
		"not valid UTF-8 and could not be converted",
		"the rest of their event was indexed",
		"`failed to map` warnings above",
		"the file holding them is marked failed",
	} {
		if !strings.Contains(added, want) {
			t.Errorf("missing %q:\n%s", want, added)
		}
	}
	if strings.Contains(added, "—") {
		t.Errorf("em dash in the added sentence:\n%s", added)
	}
	var none *parser.SkipCounters
	if none.Count(parser.SkipRowMapFailed) != 0 {
		t.Error("a nil tally must count zero")
	}
	t.Logf("\n%s", with)
}
