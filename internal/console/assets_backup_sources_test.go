package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestBackupsPanelRendersEveryLocation guards the UI half of #1542.
//
// The endpoint can report two locations and an incomplete listing perfectly and
// the page can still show what it always showed: one path, and a list of
// snapshots with nothing saying it is a subset. That is the same bug one layer
// up, and it is invisible to every Go test in this package.
func TestBackupsPanelRendersEveryLocation(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	panel := functionBody(t, js, "function baselinesPanel(")

	if !strings.Contains(panel, "backupIncompleteNotice(b)") {
		t.Error("the panel never renders the incomplete notice, so a listing missing a whole " +
			"unreadable location renders as if it were the complete set")
	}
	// Both branches, checked by SPLITTING the function rather than counting.
	// A count of two passes on two calls sitting anywhere, including both on the
	// same branch — which is the thing it claims to rule out.
	emptyHalf, populatedHalf, split := strings.Cut(panel, `} else if (!(b.snapshots || []).length) {`)
	if !split {
		t.Fatal("baselinesPanel no longer has the empty-list branch this guard reads")
	}
	_ = emptyHalf
	populatedBranch, _, ok := strings.Cut(populatedHalf, "\n  } else {")
	if !ok {
		t.Fatal("baselinesPanel no longer has the populated branch this guard reads")
	}
	if !strings.Contains(populatedBranch, "backupIncompleteNotice(b)") {
		t.Error("the empty-list branch does not render the incomplete notice; a failed location " +
			"that leaves the list EMPTY renders as a flat \"no backups found\"")
	}
	if !strings.Contains(strings.TrimPrefix(populatedHalf, populatedBranch), "backupIncompleteNotice(b)") {
		t.Error("the populated branch does not render the incomplete notice; a shorter list " +
			"renders as if it were the whole set")
	}
	if !strings.Contains(panel, "backupSourceList(b)") {
		t.Error("the empty state prints a single source path; with two locations configured it names " +
			"only one of the places that came back empty")
	}

	strip := functionBody(t, js, "function baselineContextStrip(")
	if !strings.Contains(strip, "b.sources") {
		t.Error("the context strip prints only the primary source; on a server with a local " +
			"directory and a bucket, the bucket holding the snapshots is never named")
	}

	notice := functionBody(t, js, "function backupIncompleteNotice(")
	if !strings.Contains(notice, "s.error") {
		t.Error("the notice does not print the per-location error, so the operator is told " +
			"something failed but not what or where")
	}
	if !strings.Contains(notice, "error-box") {
		t.Error("the notice is not rendered as an error; the list under it is a SUBSET and a " +
			"quiet hint gets read past")
	}

	where := functionBody(t, js, "function backupWhereChip(")
	if !strings.Contains(where, "srcs.length < 2") {
		t.Error("the per-row location chip is not gated on there being more than one location; " +
			"repeating the single configured source on every row is noise")
	}
}

// TestBackupJobCardsOfferOnlyWhatTheirJobCanRead pins the gate that decides a
// card's default (#1542).
//
// The listing merges every location; the restore and .sql-export jobs each read
// ONE. A card defaulting to the merged newest can therefore name a snapshot its
// own job will refuse, while the page above says it is right there — the very
// failure the merge was supposed to end, one layer down.
//
// The field matters and it is easy to get wrong: `cur` is the RAW registry
// entry, while both jobs resolve through withBaselineDefaults, so a server that
// inherits the daemon-wide backup location has an empty cur.baseline_dir and
// still builds from a directory. b.kind comes off the bundle, which applies the
// same defaulting and the same dir-over-S3 preference the export does.
//
// The restore card is the exception and stays on cur.baseline_dir on purpose:
// its endpoint REFUSES the shared daemon store, because that fold would mix
// servers. Same field, opposite meanings. Where it READS is the other half
// (#1541): this server's S3 backups when it has them, else that directory,
// which is the scheduled update's rule and what the coverage card reports as
// restore_reads, so the card narrows on cur.baseline_s3.
func TestBackupJobCardsOfferOnlyWhatTheirJobCanRead(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)

	// Comments stripped first. This function's own comment explains why
	// cur.baseline_dir is the wrong field here, and a raw substring check reads
	// that explanation as the mistake it warns about.
	exportCard := stripJSLineComments(functionBody(t, js, "function backupSQLLane("))
	if strings.Contains(exportCard, "cur.baseline_dir") {
		t.Error("the .sql lane gates on cur.baseline_dir, which is the RAW registry entry. " +
			"A server inheriting the daemon-wide backup location has none, so the lane either " +
			"vanishes or defaults to a snapshot the build cannot read")
	}
	if !strings.Contains(exportCard, `const reads = b.kind === "dir" ? "dir" : "s3";`) || !strings.Contains(exportCard, `backupSnapshotsFor(b, reads)`) {
		t.Error("the .sql lane does not choose its default from the location the build " +
			"will actually read")
	}

	restoreCard := stripJSLineComments(functionBody(t, js, "function backupRestoreCard("))
	if !strings.Contains(restoreCard, "!cur.baseline_dir") {
		t.Error("the restore card no longer requires the server's OWN directory; its endpoint " +
			"refuses the shared daemon store, so the card must not offer it")
	}
	if !strings.Contains(restoreCard, `const reads = cur.baseline_s3 ? "s3" : "dir";`) || !strings.Contains(restoreCard, `backupSnapshotsFor(b, reads)`) {
		t.Error("the restore card does not narrow to the snapshots its fold can read: this server's " +
			"S3 backups when it has them, else its directory (#1541)")
	}

	for _, fn := range []string{"function backupSQLLane(", "function backupRestoreCard("} {
		if strings.Contains(stripJSLineComments(functionBody(t, js, fn)), "b.snapshots[0]") {
			t.Errorf("%s still defaults to the merged newest snapshot, which its job may not be "+
				"able to read", fn)
		}
	}
}

// backupSnapshotsFor is pure, so it is EXECUTED rather than matched: the three
// server shapes are where the gate went wrong, and a source assertion cannot
// tell which snapshot each one lands on.
func TestBackupSnapshotsFor_perServerShape(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := functionBody(t, string(raw), "function backupSnapshotsFor(") + `
const snaps = [
  {time: "2026-06-10", kinds: ["s3"]},
  {time: "2026-06-03", kinds: ["dir", "s3"]},
  {time: "2026-05-27", kinds: ["dir"]},
];
const pick = (kind) => { const u = backupSnapshotsFor({snapshots: snaps}, kind); return u.length ? u[0].time : null; };
console.log(JSON.stringify({dir: pick("dir"), s3: pick("s3"),
  none: backupSnapshotsFor({snapshots: [{time: "x"}]}, "dir").length}));
`
	dir := t.TempDir()
	path := filepath.Join(dir, "gate.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).Output()
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	var got struct {
		Dir  string `json:"dir"`
		S3   string `json:"s3"`
		None int    `json:"none"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	// The newest LOCAL one, not the newest overall: the newest is S3-only and a
	// dir-reading job would refuse it.
	if got.Dir != "2026-06-03" {
		t.Errorf("a dir-reading job defaults to %q, want 2026-06-03 (the newest one on disk)", got.Dir)
	}
	if got.S3 != "2026-06-10" {
		t.Errorf("an S3-reading job defaults to %q, want 2026-06-10", got.S3)
	}
	// A snapshot with no kinds is usable for anything: that is the pre-merge
	// behaviour, and the conservative default for a response shape that predates
	// the field.
	if got.None != 1 {
		t.Errorf("a snapshot carrying no kinds was filtered out (%d left); it must stay usable", got.None)
	}
}

// stripJSLineComments drops // lines so a guard reads CODE, not the comment
// explaining the trap it checks for.
func stripJSLineComments(body string) string {
	var b strings.Builder
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

// downloadBackup is shared by the per-row button and the DuckDB lane's, and
// it re-enables whichever one it was given when the transfer ends. It used to
// re-assert the PER-ROW label there, so one click renamed the lane's
// "Download the data" to "Download (.tar.gz) · 210 MB" for good -- and the
// next click captured the clobbered text as its own label.
//
// Source-level on purpose: no test renders a button, clicks it through a real
// transfer and reads the label back, so this is what stands between that bug
// and a second appearance.
func TestDownloadBackupRestoresTheLabelItWasGiven(t *testing.T) {
	body := stripJSLineComments(functionBody(t, readAsset(t, "app.js"), "async function downloadBackup("))
	if !strings.Contains(body, "const btnLabel = btn ? btn.textContent") {
		t.Error("downloadBackup no longer captures the caller's own button label")
	}
	if strings.Contains(body, `btn.textContent = "Download (.tar.gz) · "`) {
		t.Error("downloadBackup re-asserts the per-row button's label. That is right for that one " +
			"button and renames every other caller's: the DuckDB lane's button loses its name " +
			"after a single click, and the click after that adopts the wrong one")
	}
	if !strings.Contains(body, "btn.isConnected") {
		t.Error("downloadBackup revives a button that may have been detached by a repaint. The " +
			"node belongs to a lane nobody will see again, and the visible button is a different one")
	}
	if !strings.Contains(body, "backupDownloadBusy") {
		t.Error("nothing serialises whole-archive downloads. A repaint mid-transfer leaves the new " +
			"button enabled, and a second click buffers a second full archive in browser memory")
	}
}
