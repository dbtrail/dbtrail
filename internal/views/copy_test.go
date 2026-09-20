package views

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// TestHeader_saysAPeriodicRefreshNeverReachesThisFile is #1484.
//
// The header already said the state views point at ONE snapshot and to re-run
// after taking or refreshing a baseline. That covers an operator who takes
// baselines by hand. It does not cover the case the flag creates: a daemon
// publishes a new snapshot on a timer, nobody re-runs anything, and the file
// goes on reading the snapshot it was generated against. Seven snapshots in
// forty minutes were ignored that way.
func TestHeader_saysAPeriodicRefreshNeverReachesThisFile(t *testing.T) {
	head := header(Generate(goldenInput()))
	for _, want := range []string{
		// The flag by name is the discriminator: without it the reader who
		// takes baselines by hand and the reader whose daemon does it on a
		// timer read the same sentence and only one of them is covered.
		// With its binary: bintrail views is not where this flag lives.
		"`bintrail-console watch --baseline-refresh-interval`",
		// That the file does not follow it, and that nothing says so.
		"no error",
		"no warning",
		// What to actually do about it.
		// The remedy has to name the CADENCE. "Regenerate when convenient" is
		// the same words and none of the point: the whole finding is that the
		// refresh has a period and the file has to match it.
		"Regenerate this file on the same schedule",
	} {
		if !strings.Contains(head, want) {
			t.Errorf("the header does not say %q:\n%s", want, head)
		}
	}
}

// TestHeader_refreshNoteSurvivesAnEmptyBaseline: the note is part of the
// header's standing description of what this file is, not a per-baseline
// decoration, and it is worded so it stays TRUE with no state views ("any state
// view below"). Gating it would add a branch whose only effect is to withhold
// the warning.
//
// Both empty shapes, because they are different branches of the header:
// --no-baselines skips resolveBaselineViews entirely and leaves BaselineSource
// empty too, while a configured root with nothing under it renders "(none
// discoverable under ...)". Zeroing only Baselines tests the second and reads
// like it tests the first.
func TestHeader_refreshNoteSurvivesAnEmptyBaseline(t *testing.T) {
	for name, in := range map[string]Input{
		"--no-baselines":    func() Input { in := goldenInput(); in.Baselines, in.BaselineSource = nil, ""; return in }(),
		"root with nothing": func() Input { in := goldenInput(); in.Baselines = nil; return in }(),
	} {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(header(Generate(in)), "--baseline-refresh-interval") {
				t.Error("the refresh note disappeared when no baseline was discovered")
			}
		})
	}
}

// TestGenerate_noEmDashesInTheGeneratedFile.
//
// Fixed copy rule for everything this product emits at a human. The guard is
// over RENDERED output rather than the source, and over several shapes rather
// than the golden alone, because a dash in a branch no fixture renders is
// exactly how one ships.
func TestGenerate_noEmDashesInTheGeneratedFile(t *testing.T) {
	live := liveIdx()
	attributed := liveIdx()
	attributed.BintrailID = archiveID
	disagreeing := liveIdx()
	disagreeing.BintrailID = "from-the-registry"
	multi := liveIdx()
	multi.Attribution = AttributionMultiSource
	unreg := liveIdx()
	unreg.Attribution = AttributionUnregistered
	loopback := liveIdx()
	loopback.Host = "127.0.0.1"

	shapes := map[string]Input{
		"golden":           goldenInput(),
		"no archives":      func() Input { in := goldenInput(); in.ArchiveSources = nil; return in }(),
		"discovery failed": func() Input { in := goldenInput(); in.ArchiveSources, in.ArchiveDiscoveryFailed = nil, true; return in }(),
		"no baselines":     func() Input { in := goldenInput(); in.Baselines = nil; return in }(),
		"region ambiguous": func() Input { in := goldenInput(); in.RegionAmbiguous = true; return in }(),
		"console download": func() Input { in := goldenInput(); in.LiveLegUnavailable = true; return in }(),
		// The copy written INTO a snapshot, which no golden file covers.
		// Built through the REAL constructor, not goldenInput with the flag
		// flipped: that kept ArchiveSources, so rendersEvents() won at
		// writeHeader and the SnapshotScoped header branch stayed unrendered
		// (while the file it produced contradicted itself, promising globs
		// that follow rotation above a body saying events are not in it).
		// A branch nothing renders is where a dash survives unseen.
		"snapshot scoped": func() Input {
			in := SnapshotScopedInput(goldenInput().BaselineSnapshot, goldenInput().Baselines)
			in.RespellBaselines("/data/baselines/2026-04-30T03-00-00Z")
			return in
		}(),
		// The S3-compatible-store prose in writeS3Preamble is the one branch no
		// other shape reaches, and an unrendered branch is exactly where a dash
		// survives.
		"s3 endpoint": func() Input {
			in := goldenInput()
			in.S3Endpoint = storage.S3Endpoint{URL: "https://minio.internal:9000", PathStyle: true}
			return in
		}(),
		"live":              liveInput(live),
		"live attributed":   liveInput(attributed),
		"live disagreeing":  liveInput(disagreeing),
		"live multi source": liveInput(multi),
		"live unregistered": liveInput(unreg),
		"live loopback":     liveInput(loopback),
		"live only":         func() Input { in := liveInput(live); in.ArchiveSources = nil; return in }(),
	}
	for name, in := range shapes {
		t.Run(name, func(t *testing.T) {
			for _, dash := range []string{"\u2014", "\u2013"} {
				for _, line := range strings.Split(Generate(in), "\n") {
					if strings.Contains(line, dash) {
						t.Errorf("emitted line contains %q: %s", dash, line)
					}
				}
			}
		})
	}
}

// header returns everything before the first blank line, which is where the
// header block ends, so an assertion about the header cannot pass on a sentence
// in a preamble or a view comment further down.
//
// As PROSE, for the same reason livePreamble is: an assertion should be about
// the sentence, not about where it wraps.
func header(out string) string {
	block := out
	if i := strings.Index(out, "\n\n"); i >= 0 {
		block = out[:i]
	}
	var words []string
	for _, line := range strings.Split(block, "\n") {
		words = append(words, strings.Fields(strings.TrimPrefix(strings.TrimSpace(line), "--"))...)
	}
	return strings.Join(words, " ")
}
