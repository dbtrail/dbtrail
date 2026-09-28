package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// resetViewsFlags clears the package-level flag vars between subtests. The
// command's flags are package globals (the convention throughout this package),
// so a test that sets one leaks into the next without this.
// The bool half must list EVERY bool flag, not the ones that existed when this
// was written: vIncludeEvents (#1546), vIncludeLive (#1480) and vPinSnapshot
// (#1547) were each added later and each leaked into every subtest that ran
// after the one setting it, silently, because a leaked bool changes which
// refusal fires rather than producing an obviously wrong value.
func resetViewsFlags() {
	vIndexDSN, vArchiveDir, vArchiveS3, vBintrailID = "", "", "", ""
	vRegion, vBaselineDir, vBaselineS3, vOut = "", "", "", "views.sql"
	vNoBaselines, vIncludeEvents, vIncludeLive, vPinSnapshot = false, false, false, false
	vSchema = ""
}

// TestRunViews_flagValidation covers the refusals that must happen BEFORE any
// connection is opened — each one names a combination that would otherwise
// produce a silently wrong file rather than an error.
func TestRunViews_flagValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		set     func()
		wantErr string
	}{
		{
			// Without the id the glob would span every server that ever archived
			// into this root, and the events view would mix their binlog
			// coordinates under one name.
			name:    "archive dir without bintrail-id",
			set:     func() { vArchiveDir = "/data/archives" },
			wantErr: "--bintrail-id is required",
		},
		{
			name:    "archive s3 without bintrail-id",
			set:     func() { vArchiveS3 = "s3://bucket/archives/" },
			wantErr: "--bintrail-id is required",
		},
		{
			name:    "both baseline sources",
			set:     func() { vIndexDSN = "u:p@tcp(h:3306)/i"; vBaselineDir = "/b"; vBaselineS3 = "s3://b/" },
			wantErr: "mutually exclusive",
		},
		{
			// Only the events view reads an archive source, so asking for it
			// with nowhere to read from is the refusal. A baselines-only file
			// is NOT refused here: see
			// TestRunViews_baselinesOnlyNeedsNoIndex.
			name:    "include-events with no index and no explicit archives",
			set:     func() { vBaselineDir = "/b"; vIncludeEvents = true },
			wantErr: "--include-events needs an archive source",
		},
		{
			// The empty file stays refused, and by RendersAnyView rather than
			// by a flag check, so the reason names what the file would hold.
			name:    "nothing named at all",
			set:     func() {},
			wantErr: "no view at all",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetViewsFlags()
			tc.set()
			err := runViews(&cobra.Command{}, nil)
			if err == nil {
				t.Fatalf("expected an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestExplicitArchiveSources pins the bintrail_id scoping for operator-named
// roots, matching what `query` does with the same pair of flags.
func TestExplicitArchiveSources(t *testing.T) {
	resetViewsFlags()
	vArchiveDir = "/data/archives"
	vArchiveS3 = "s3://bucket/archives/"
	vBintrailID = "abc-123"

	got := explicitArchiveSources()
	want := []string{
		"/data/archives/bintrail_id=abc-123",
		"s3://bucket/archives/bintrail_id=abc-123",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("source %d = %q, want %q", i, got[i], want[i])
		}
	}
	resetViewsFlags()
}

// TestViewsCmd_registered guards the wiring: a command defined but never added
// to the root is invisible, and nothing else in the build would notice.
func TestViewsCmd_registered(t *testing.T) {
	root := &cobra.Command{Use: "bintrail"}
	AddReadCommands(root)
	for _, c := range root.Commands() {
		if c.Name() == "views" {
			return
		}
	}
	t.Fatal("`views` is not registered by AddReadCommands")
}

// TestRunViews_baselinesOnlyNeedsNoIndex is THE assertion for #1552: a
// snapshot downloaded from the console (GET /api/baselines/download) is
// extracted on a machine that may not reach the index at all, and the file
// that reads it defines state views and nothing else. Requiring --index-dsn
// there asked for a host to discover paths the file never names, and the
// documented way around it was to invent an --archive-dir, which produced the
// very file that had just been refused.
//
// Driven through runViews, not the generator: the refusal lives in the command
// layer, so a generator-level test could not see it.
func TestRunViews_baselinesOnlyNeedsNoIndex(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "2026-08-31T06-00-00Z", "wp", "wp_posts.parquet")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	saveViewsFlags(t)
	// Every archive-side flag empty: this is exactly what an operator has
	// after extracting the tarball, and nothing else.
	vIndexDSN, vArchiveDir, vArchiveS3, vBintrailID, vBaselineS3 = "", "", "", "", ""
	vBaselineDir, vOut = root, "-"
	vNoBaselines, vIncludeLive, vIncludeEvents, vPinSnapshot = false, false, false, false

	out, err := runViewsToString(t)
	if err != nil {
		t.Fatalf("runViews over a baselines-only root: %v", err)
	}
	if !strings.Contains(out, `CREATE OR REPLACE VIEW "state_wp_wp_posts"`) {
		t.Errorf("no state view for the snapshot's table in:\n%s", out)
	}
	// The events view reads an archive source and there is none, so it must
	// not be defined. Asserting its ABSENCE alone would pass on a file that
	// defines nothing at all, which is why the state view is checked above.
	if strings.Contains(out, `CREATE OR REPLACE VIEW "events"`) {
		t.Errorf("defined an events view with no archive source:\n%s", out)
	}
}
