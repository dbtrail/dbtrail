package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// The schedule DTO's wire names and the keys the Backups page reads are pinned
// against each other: a renamed field on either side compiles, passes every
// Go test, and leaves the card rendering "undefined" for that fact.
func TestBackupScheduleWireNamesMatchTheFrontend(t *testing.T) {
	js := readAsset(t, "app.js")
	body := jsFunctionBody(t, js, "backupScheduleCard")

	raw, err := json.Marshal(backupScheduleDTO{
		Every: "1d", At: "03:00", NextRun: "x", NextMethod: BackupMethodRefresh, NextMethodWhy: "w", NextMethodWhyCode: "c", NextMethodError: "e",
		Runnable: false, Reason: "r", Running: true, HistoryUnavailable: true,
		LastRun:      &backupScheduleRunDTO{Method: BackupMethodRefresh, Why: "w", WhyCode: "c", FinishedAt: "f", Error: "e", Tables: 1, Carried: 1, CarriedCopied: 1, Uploaded: 1},
		LastSkipped:  &backupScheduleSkipDTO{At: "a", Reason: "r"},
		LastFallback: &backupScheduleSkipDTO{At: "a", Reason: "r"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"every", "at", "next_run", "next_method", "next_method_why", "next_method_why_code", "next_method_error", "runnable", "reason", "running", "history_unavailable", "last_run", "last_skipped", "last_fallback"} {
		if _, ok := wire[key]; !ok {
			t.Errorf("backupScheduleDTO does not serialise %q (got %s)", key, raw)
		}
		if !strings.Contains(body, "sch."+key) {
			t.Errorf("backupScheduleCard never reads sch.%s", key)
		}
	}
	run := wire["last_run"].(map[string]any)
	for _, key := range []string{"method", "why", "why_code", "finished_at", "ok", "error", "tables", "carried", "carried_copied", "uploaded"} {
		if _, ok := run[key]; !ok {
			t.Errorf("last_run does not serialise %q (got %s)", key, raw)
		}
		if !strings.Contains(body, "run."+key) {
			t.Errorf("backupScheduleCard never reads run.%s", key)
		}
	}
	skip := wire["last_skipped"].(map[string]any)
	for _, key := range []string{"at", "reason"} {
		if _, ok := skip[key]; !ok {
			t.Errorf("last_skipped does not serialise %q (got %s)", key, raw)
		}
		if !strings.Contains(body, "skip."+key) {
			t.Errorf("backupScheduleCard never reads skip.%s", key)
		}
		if !strings.Contains(body, "fb."+key) {
			t.Errorf("backupScheduleCard never reads fb.%s (last_fallback)", key)
		}
	}
	// A dead rebuild half and a slot that cannot start are alarms, in red and
	// opening the card, not hints.
	for _, want := range []string{`sch.next_method_error) {`, `if (fb) {`, `"The next run cannot start: "`} {
		if !strings.Contains(body, want) {
			t.Errorf("backupScheduleCard lost the branch around %q", want)
		}
	}
	if !strings.Contains(body, `hit an internal error`) {
		t.Error("a crashed rebuild is rendered as a refusal")
	}
	// The fallback block itself is red and opens the card: the needles above
	// only proved the branch exists.
	if i := strings.Index(body, `if (fb) {`); i >= 0 {
		block := body[i:]
		if j := strings.Index(block, `if (skip`); j >= 0 {
			block = block[:j]
		}
		if !strings.Contains(block, `alarm = true`) || !strings.Contains(block, `class: "form-msg err"`) {
			t.Errorf("the fallback block is not a red alarm: %s", block)
		}
	}
	// Reasons assembled by the daemon never appear in this file, so the em
	// dash guard below cannot see one riding in on a fold error; they go
	// through plainWords.
	for _, want := range []string{`backupFoldError(skip.reason)`, `plainWords(sch.next_method_error)`, `plainWords(sch.reason`, `skip.at >= `, `backupsPer30Days(every.value)`, `never removed automatically`, `body.append(rate)`} {
		if !strings.Contains(body, want) {
			t.Errorf("backupScheduleCard lost %q", want)
		}
	}
	if save := jsFunctionBody(t, readAsset(t, "app.js"), "saveBackupSchedule"); !strings.Contains(save, `next_method_error`) {
		t.Error("saveBackupSchedule toasts a run the response says cannot start")
	}
	// The method is not an input any more: the form sends only when, and the
	// card never offers a producer to pick.
	if strings.Contains(body, "method: how.value") || strings.Contains(body, `el("select"`) {
		t.Error("the card still offers the producer as a choice; the daemon decides per run (ChooseBackupMethod)")
	}
	// The body the form sends is what the handler decodes. Scoped to the
	// card: `at:` and `method:` occur dozens of times elsewhere in app.js,
	// so a whole-file search passed without the feature.
	for _, send := range []string{"every: every.value", "at: at.value"} {
		if !strings.Contains(body, send) {
			t.Errorf("the schedule form never sends %q", send)
		}
	}
	// The capability gates the FORM only: a saved schedule is rendered from
	// the listing whether or not this process can run it, because the
	// not-runnable reason is the message the feature exists to show.
	if !strings.Contains(body, "capsCache.backup_schedule") {
		t.Error("the form is not gated on the backup_schedule capability, so the read-only console would offer a form that 403s")
	}
	if !strings.Contains(body, "if (!sch && !canEdit) return null") || strings.Contains(body, "if (!capsCache.backup_schedule ||") {
		t.Error("the whole card is gated on the capability, so a saved schedule on a daemon that cannot run it is hidden instead of reported")
	}
	caps, _ := json.Marshal(capabilitiesResponse{BackupSchedule: true})
	if !strings.Contains(string(caps), `"backup_schedule":true`) {
		t.Errorf("capabilities do not serialise backup_schedule: %s", caps)
	}
	for _, ep := range []string{`"/backup-schedule", { method: "PUT"`, `"/backup-schedule", { method: "DELETE"`} {
		if !strings.Contains(js, ep) {
			t.Errorf("app.js never calls %s", ep)
		}
	}
}

// The card can be unmounted with the whole suite green; this pins that the
// Backups page still calls it.
func TestBackupsPageStillMountsTheScheduleCard(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "renderBaselines")
	if !strings.Contains(body, "backupScheduleCard(") {
		t.Error("renderBaselines no longer mounts backupScheduleCard, so the schedule has no UI at all")
	}
}

// Copy rule: nothing the card shows uses an em dash, and a not-runnable
// schedule is never summarised as if it will run.
func TestBackupScheduleCard_copy(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "backupScheduleCard")
	if strings.Contains(body, "—") {
		t.Error("backupScheduleCard copy contains an em dash")
	}
	if !strings.Contains(body, "Cannot run: ") {
		t.Error("the state line does not say when the schedule cannot run")
	}
	if strings.Contains(body, "rebuild from change history") || strings.Contains(body, "Full backup (reads") {
		t.Error("the card still names the two producers as options; the operator picks when, the daemon picks how")
	}
}
