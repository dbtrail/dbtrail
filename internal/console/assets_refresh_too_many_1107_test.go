package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestBudgetRefusalLines (#1107): an update refused for too many changed rows
// starts again from the same backup over a longer window, so "the next run
// retries" would be false on every line that shows the refusal. The inputs
// are what Go actually produces: the daemon's error text and a marshaled
// BaselineStatus, so renaming the wire field or rewording the sentinel breaks
// this test instead of silently bringing the retry promise back.
func TestBudgetRefusalLines(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	marshal := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	two := "shop.a: " + reconstruct.TouchedRowBudgetError(1_000_000, 2).Error()
	one := "shop.a: " + reconstruct.TouchedRowBudgetError(2_000_000, 1).Error()
	budget := BaselineStatus{State: "failed", Refused: 1, LastError: two, TooManyChanges: true}
	gap := BaselineStatus{State: "failed", Refused: 1, LastError: "shop.a: capture gap"}

	js := readAsset(t, "app.js")
	var script strings.Builder
	for _, fn := range []string{"function baselineRefreshNote(", "function backupFoldError(", "function budgetRefusedTail(",
		"function touchedRowBudgetText(", "function restoreRefusedLine("} {
		script.WriteString(functionBody(t, js, fn) + "\n")
	}
	script.WriteString(`
const el = (tag, o) => o;
const utcLabel = (s) => s;
const reusedCopiedNote = () => "";
let capsCache = { baseline_trigger: true };
const budget = ` + marshal(budget) + `, gap = ` + marshal(gap) + `, one = ` + marshal(one) + `, two = ` + marshal(two) + `;
const out = {
  refresh: baselineRefreshNote(budget).text,
  refreshGap: baselineRefreshNote(gap).text,
  restore: restoreRefusedLine(budget),
  restoreGap: restoreRefusedLine(gap),
  matchOne: touchedRowBudgetText(one),
  matchTwo: touchedRowBudgetText(two),
  matchGap: touchedRowBudgetText(gap.last_error),
  scheduled: budgetRefusedTail("scheduled update"),
};
capsCache = {};
out.refreshNoTrigger = baselineRefreshNote(budget).text;
console.log(JSON.stringify(out));
`)
	path := filepath.Join(t.TempDir(), "budget.js")
	if err := os.WriteFile(path, []byte(script.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	outB, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, outB)
	}
	var got struct {
		Refresh, RefreshGap, Restore, RestoreGap, Scheduled, RefreshNoTrigger string
		MatchOne, MatchTwo, MatchGap                                          bool
	}
	if err := json.Unmarshal(outB, &got); err != nil {
		t.Fatalf("decode %q: %v", outB, err)
	}
	t.Logf("refresh: %s", got.Refresh)
	t.Logf("single-table error: %s", one)

	if !strings.HasSuffix(got.Refresh, "every automatic refresh starts from the same backup and is refused again.") {
		t.Errorf("refresh budget refusal: %q", got.Refresh)
	}
	if !strings.HasSuffix(got.RefreshNoTrigger, "is refused again; creating backups from the console is turned off here.") {
		t.Errorf("refresh budget refusal without the create button: %q", got.RefreshNoTrigger)
	}
	if !strings.HasSuffix(got.RefreshGap, "Nothing was overwritten; the next run retries.") {
		t.Errorf("other refresh refusal changed wording: %q", got.RefreshGap)
	}
	if !strings.HasSuffix(got.Restore, "Pick a moment closer to an existing backup.") {
		t.Errorf("restore budget refusal: %q", got.Restore)
	}
	if !strings.HasSuffix(got.RestoreGap, "published nothing: shop.a: capture gap. Nothing was overwritten.") {
		t.Errorf("other restore refusal changed wording: %q", got.RestoreGap)
	}
	if !got.MatchOne || !got.MatchTwo || got.MatchGap {
		t.Errorf("schedule lines recognise the Go refusal text: one=%v two=%v gap=%v", got.MatchOne, got.MatchTwo, got.MatchGap)
	}
	if strings.Contains(one, "tables are processed") || strings.Contains(one, "1 tables") {
		t.Errorf("single-table refusal talks about tables processed at once: %q", one)
	}
	for _, s := range []string{got.Refresh, got.RefreshNoTrigger, got.RefreshGap, got.Restore, got.Scheduled, one, two} {
		if strings.Contains(s, "retries") && s != got.RefreshGap || strings.Contains(s, "\u2014") || strings.Contains(s, "..") {
			t.Errorf("not clean copy: %q", s)
		}
	}
}
