package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// Review of #1991: the failure card's words for each case the first cut got
// wrong. Rendered from the real app.js in node.

const rawFold = "dump: something; output: ** (mydumper:7): CRITICAL **: Access denied"

type reviewCase struct {
	Failure map[string]any
	Where   string
	Name    string // server name passed to the card
}

func renderReviewCards(t *testing.T, cases map[string]reviewCase, caps string) map[string]drawn {
	t.Helper()
	in, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(rawFold)
	out := runSnapshotFailureJS(t, `
vm.runInContext("capsCache = `+caps+`;", ctx);
const cs = `+string(in)+`;
const res = {};
for (const [k, c] of Object.entries(cs)) res[k] = read(fn("snapshotFailureCard")(c.Failure, `+string(raw)+`, c.Where, "", c.Name));
console.log(JSON.stringify(res));
`)
	var got map[string]drawn
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	return got
}

func TestSnapshotFailureReview_words(t *testing.T) {
	ba := map[string]any{"kind": "missing_permission", "grant": "GRANT BACKUP_ADMIN ON *.* TO `u`@`%`;", "privileges": 1}
	baChosen := map[string]any{"kind": "missing_permission", "grant": "GRANT BACKUP_ADMIN ON *.* TO `u`@`%`;", "privileges": 1, "mode_chosen": true}
	cases := map[string]reviewCase{
		"backup_admin, automatic":     {ba, "now", ""},
		"backup_admin, chosen":        {baChosen, "now", ""},
		"postgres":                    {map[string]any{"postgres": true, "summary": "pg baseline: connection refused"}, "now", ""},
		"no kind, summary":            {map[string]any{"summary": "dump: there is not enough free disk to start this read"}, "now", ""},
		"too old, chosen":             {map[string]any{"kind": "mydumper_too_old", "min_version": "0.18.1", "mode_chosen": true}, "now", ""},
		"too old, automatic":          {map[string]any{"kind": "mydumper_too_old", "min_version": "0.18.1"}, "now", ""},
		"named":                       {map[string]any{"summary": "x"}, "scheduled", "shop-db"},
		"named, long":                 {map[string]any{"summary": "x"}, "scheduled", "orders-replica-eu-west-1-production-reporting-01"},
		"named postgres":              {map[string]any{"summary": "x", "postgres": true}, "scheduled", "pg-main"},
	}
	got := renderReviewCards(t, cases, "{ baseline_trigger: true }")
	want := map[string][]string{
		"backup_admin, automatic": {headline, "The database user needs one more permission. Run this on your database, then press Read database now again:",
			"On Amazon RDS or Aurora this permission cannot be granted."},
		"backup_admin, chosen": {headline, "The database user needs one more permission. Run this on your database, then press Read database now again:",
			"On Amazon RDS or Aurora this permission cannot be granted. DBTrail was started with a setting that forces this way of reading; the technical details below say where it is."},
		"postgres":           {"Snapshot did not finish.", "pg baseline: connection refused"},
		"no kind, summary":   {headline, "dump: there is not enough free disk to start this read"},
		"too old, chosen":    {headline, "Install mydumper 0.18.1 or newer where DBTrail runs, then press Read database now again. The technical details below show another way."},
		"too old, automatic": {headline, "Install mydumper 0.18.1 or newer where DBTrail runs, then press Read database now again."},
		"named":              {"Snapshot of shop-db did not finish. Your database was not changed.", "x", "The next scheduled snapshot tries again."},
		"named, long":        {"Snapshot of orders-replica-eu-west-1-production-reporting-01 did not finish. Your database was not changed.", "x", "The next scheduled snapshot tries again."},
		"named postgres":     {"Snapshot of pg-main did not finish.", "x", "The next scheduled snapshot tries again."},
	}
	for k, w := range want {
		d := got[k]
		t.Logf("%s: %q", k, d.Shown)
		if strings.Join(d.Shown, "\n") != strings.Join(w, "\n") {
			t.Errorf("%s:\n got  %q\n want %q", k, d.Shown, w)
		}
		if strings.Contains(strings.Join(d.Shown, " "), "automatic") || strings.Contains(strings.Join(d.Shown, " "), "CRITICAL") {
			t.Errorf("%s: names a control that does not exist, or raw output: %q", k, d.Shown)
		}
		if strings.Join(d.Fold, "") != rawFold || d.FoldOpen {
			t.Errorf("%s: fold %q (open %v)", k, d.Fold, d.FoldOpen)
		}
	}
}

// The Overview names the button only when this session could press it, for
// a run a person started.
func TestSnapshotFailureReview_overviewRetry(t *testing.T) {
	perm := map[string]any{"kind": "missing_permission", "grant": "GRANT LOCK TABLES ON *.* TO 'u'@'%';", "privileges": 1}
	permSched := map[string]any{"kind": "missing_permission", "grant": "GRANT LOCK TABLES ON *.* TO 'u'@'%';", "privileges": 1, "scheduled": true}
	press := "The database user needs one more permission. Run this on your database, then press Read database now again on the Snapshots page:"
	next := "The database user needs one more permission. Run this on your database, then the next scheduled snapshot will use it:"
	for _, c := range []struct {
		name, caps string
		failure    map[string]any
		want       string
	}{
		{"button available, a person's run", "{ baseline_trigger: true }", perm, press},
		{"button turned off", "{ baseline_trigger: false }", perm, next},
		{"a scheduled run", "{ baseline_trigger: true }", permSched, next},
	} {
		t.Run(c.name, func(t *testing.T) {
			in, _ := json.Marshal(c.failure)
			out := runSnapshotFailureJS(t, `
vm.runInContext("capsCache = `+c.caps+`;", ctx);
const rep = { complete: false, steps: [{ name: "Take the first full DB snapshot", state: "failed", detail: "dump: refused",
  fix: "Try again on the Snapshots page.", snapshot_failed: true, failure: `+string(in)+` }] };
let r = null; walk(fn("firstRunCard")(rep), (n) => { if (!r && n.tag === "li") r = read(n); });
console.log(JSON.stringify(r));
`)
			var d drawn
			if err := json.Unmarshal(out, &d); err != nil {
				t.Fatalf("decode %q: %v", out, err)
			}
			shown := strings.Join(d.Shown, " | ")
			if !strings.Contains(shown, c.want) {
				t.Errorf("shown %q, want %q", shown, c.want)
			}
		})
	}
}
