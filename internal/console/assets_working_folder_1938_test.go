package console

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// #1938: the folder full reads, S3-only updates and .sql builds write their
// temporary files to had four names on screen (".sql build folder", "temp
// folder", "staging folder", "staging directory"), and the settings row, the
// one place to move it, carried the name that hid full reads. It is "Working
// folder" everywhere a reader sees it. The row is drawn by the REAL
// backupDaemonEditRow from app.js in node.
func TestWorkingFolderSettingsRow1938(t *testing.T) {
	raw := runSnapshotFailureJS(t, `
const row = fn("backupDaemonEditRow");
const draw = (r, locked) => {
  const o = { label: "", hints: [], placeholder: "", disabled: false };
  walk(row(r, locked), (x) => {
    if (x.tag === "label") o.label = x.textContent;
    if (String(x.className || "").split(" ").includes("form-hint")) o.hints.push(x.textContent);
    if (x.tag === "input") { o.placeholder = x.attrs.placeholder || x.placeholder || ""; o.disabled = !!(x.disabled || x.attrs.disabled); }
  });
  return o;
};
console.log(JSON.stringify({
  empty: draw({ key: "staging_dir", value: "", source: "startup", needs_restart: true }, false),
  set: draw({ key: "staging_dir", value: "/data/work", source: "saved", needs_restart: true }, false),
  locked: draw({ key: "staging_dir", value: "", source: "startup", needs_restart: true }, true),
  verify: draw({ key: "verify_interval", value: "", source: "startup" }, false),
  retain: draw({ key: "baseline_retain", value: "", source: "startup" }, false),
}));
`)
	type drawnRow struct {
		Label       string
		Hints       []string
		Placeholder string
		Disabled    bool
	}
	var got map[string]drawnRow
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, raw)
	}
	const help = "Where DBTrail writes temporary files during a full read, an S3-only update, or a .sql build. " +
		"Needs room for the largest database it reads."
	for _, name := range []string{"empty", "set", "locked"} {
		r := got[name]
		if r.Label != "Working folder"+"restart to apply" {
			t.Errorf("%s: label = %q, want Working folder with the restart pill", name, r.Label)
		}
		// What the folder is for comes first; where the value comes from
		// (the line every row carries) stays after it.
		if len(r.Hints) != 2 || r.Hints[0] != help || !strings.Contains(r.Hints[1], "command line") {
			t.Errorf("%s: hints = %q, want what the folder is for, then where its value comes from", name, r.Hints)
		}
		// A daemon that names no default (the read-only serve, which writes
		// nothing here) gets a word in the input, not a folder it would have
		// to guess (#2255).
		if r.Placeholder != "default folder" {
			t.Errorf("%s: placeholder = %q, want default folder", name, r.Placeholder)
		}
	}
	// A session that cannot save still reads what the folder is for.
	if !got["locked"].Disabled || got["empty"].Disabled {
		t.Errorf("disabled: locked=%v editable=%v", got["locked"].Disabled, got["empty"].Disabled)
	}
	// The line belongs to this row alone.
	if h := got["verify"].Hints; len(h) != 1 || strings.Contains(h[0], "temporary files") {
		t.Errorf("another row carries a hint it never had: %q", h)
	}
	if h := got["retain"].Hints; len(h) != 2 || !strings.HasPrefix(h[0], "Days or hours") || strings.Contains(strings.Join(h, " "), "temporary files") {
		t.Errorf("the retention row's own hints changed: %q", h)
	}
}

// No sentence the page can show keeps one of the old names. Comment lines are
// skipped: they describe the code, where "staging" is still the word. The
// environment variable keeps its name and is not a match.
func TestNoOldWorkingFolderNameOnScreen1938(t *testing.T) {
	src, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	old := []string{"staging folder", "staging dir", "Staging problem", ".sql build folder", `"temp folder"`}
	for i, line := range strings.Split(string(src), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		for _, o := range old {
			if strings.Contains(strings.ToLower(line), strings.ToLower(o)) {
				t.Errorf("app.js:%d still shows %q: %s", i+1, o, strings.TrimSpace(line))
			}
		}
	}
}

// The refusal a relative path gets is shown under the row it was typed in,
// so it names the row.
func TestWorkingFolderRelativePathRefusal1938(t *testing.T) {
	err := ValidateBackupSetting(BackupSettingStagingDir, "work")
	if err == nil || !strings.HasPrefix(err.Error(), `working folder: "work" is relative`) {
		t.Fatalf("err = %v, want a refusal that starts with the row's name", err)
	}
	if err := ValidateBackupSetting(BackupSettingStagingDir, "/data/work"); err != nil {
		t.Fatalf("an absolute path was refused: %v", err)
	}
}

// The reason a full read replaces an S3-only update names the folder too, and
// it is also the key the page's remedy is chosen by. A job a daemon from
// before the rename left in its journal is classified again after the
// upgrade, from the old wording, and must not lose its remedy.
func TestNoStagingReasonKeepsItsCodeAcrossTheRename1938(t *testing.T) {
	if !strings.Contains(BackupWhyNoStagingPrefix, "the working folder") || strings.Contains(BackupWhyNoStagingPrefix, "staging") {
		t.Fatalf("the reason still uses the old name: %q", BackupWhyNoStagingPrefix)
	}
	const old = "an update for a server whose snapshots go only to S3 is built in the staging folder, which cannot be used"
	for _, why := range []string{
		BackupWhyNoStagingPrefix + "; the working folder /w cannot be written: permission denied",
		old + "; the staging folder /w cannot be written: permission denied",
		old,
	} {
		if got := BackupWhyCode(why); got != "no_staging" {
			t.Errorf("BackupWhyCode(%q) = %q, want no_staging", why, got)
		}
	}
	// Close to the old wording is not the old wording.
	if got := BackupWhyCode("an update is built in the staging folder"); got != "" {
		t.Errorf("an unknown reason was classified as %q", got)
	}
}
