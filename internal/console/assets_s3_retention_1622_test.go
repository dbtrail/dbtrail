package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The S3 retention block (#1622) is a promise as much as a feature: dbtrail
// never deletes from S3 and never sets a bucket rule, the generated rule is
// scoped to the backup prefix and refused at the bucket root, and the page
// says what an age rule cannot do. The sentences are pinned as text; the
// generator and the refusal are EXECUTED below, since a text guard cannot
// see a regexp that starts matching the root.
func TestS3RetentionBlockKeepsThePromises(t *testing.T) {
	js := readAsset(t, "app.js")
	box := jsFunctionBody(t, js, "s3RetentionBox")
	row := jsFunctionBody(t, js, "backupServerRow")

	for _, want := range []string{
		"dbtrail never removes one",
		"dbtrail never deletes from S3 and never changes a bucket's rules",
		"replaces every lifecycle rule on the bucket",
		"cannot spare the only complete copy",
		"if the schedule stops it keeps expiring until none is left",
		"never to the archived changes",
		"sit at the bucket root",
		"the newest complete backup expires before the next one exists",
	} {
		if !strings.Contains(box, want) {
			t.Errorf("s3RetentionBox lost the sentence %q", want)
		}
	}
	// The mount condition IS the behaviour: the growth line under a
	// local-only server would be a false statement on screen.
	if !strings.Contains(row, "if (!srv.schedule_refusal && srv.resolved_s3) more.push(s3RetentionBox(srv));") {
		t.Error("backupServerRow does not mount the retention block exactly for a runnable schedule with an S3 destination")
	}
	// The root refusal comes before the form, not instead of it.
	if i, j := strings.Index(box, "if (!s.prefix) {"), strings.Index(box, "sit at the bucket root"); i < 0 || j < 0 || i > j {
		t.Error("the bucket-root refusal is not the branch guarded by !s.prefix")
	}
	// Negative assertions read the SPAN: jsFunctionBody cuts lines at "//",
	// and "s3://" sits on the root-refusal line.
	for _, name := range []string{"s3RetentionBox", "lifecycleRuleFor"} {
		if strings.Contains(jsFunctionSpan(t, js, name), "\u2014") {
			t.Errorf("em dash in %s", name)
		}
	}
}

// The pure helpers, EXECUTED: a rule at the bucket root would expire the
// archived changes too, a prefix without its trailing slash would also
// match a sibling prefix, and the refusal's boundary is one character.
func TestS3RetentionHelpersExecuted(t *testing.T) {
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
	js := string(raw)
	// Joined with newlines: functionBody ends at the next declaration, so
	// a body's trailing comment would otherwise swallow the next "function".
	script := strings.Join([]string{
		functionBody(t, js, "function everyMinutes("),
		functionBody(t, js, "function s3Parts("),
		functionBody(t, js, "function retentionTooShort("),
		functionBody(t, js, "function lifecycleRuleFor("),
	}, "\n") + `
const prefixOf = (u) => { const r = lifecycleRuleFor(u, 30); return r === null ? null : JSON.parse(r).Rules[0].Filter.Prefix; };
const out = {
  root: prefixOf("s3://b"), rootSlash: prefixOf("s3://b/"), nonS3: prefixOf("/var/backups"),
  prefix: prefixOf("s3://b/backups/"), nested: prefixOf("s3://b/a/b//"), bare: prefixOf("s3://b/backups"),
  zero: lifecycleRuleFor("s3://b/x", 0), nan: lifecycleRuleFor("s3://b/x", NaN),
  days: JSON.parse(lifecycleRuleFor("s3://b/x", 7)).Rules[0].Expiration.Days,
  minutes: [everyMinutes("5m"), everyMinutes("6h"), everyMinutes("1d"), everyMinutes("2w"), everyMinutes("")],
  short: [retentionTooShort("5m", 1), retentionTooShort("6h", 1), retentionTooShort("1d", 1), retentionTooShort("1d", 2), retentionTooShort("7d", 7), retentionTooShort("7d", 8), retentionTooShort("", 1)],
};
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "retention.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got struct {
		Root, RootSlash, NonS3, Zero, Nan *string
		Prefix, Nested, Bare            string
		Days                            int
		Minutes                         []int
		Short                           []bool
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if got.Root != nil || got.RootSlash != nil || got.NonS3 != nil || got.Zero != nil || got.Nan != nil {
		t.Errorf("a rule was generated where none must be: %s", out)
	}
	if got.Prefix != "backups/" || got.Nested != "a/b/" || got.Bare != "backups/" {
		t.Errorf("prefix scoping: %s (want backups/, a/b/, backups/: a trailing slash so backups-old/ is not matched)", out)
	}
	if got.Days != 7 {
		t.Errorf("Expiration.Days = %d", got.Days)
	}
	if want := []int{5, 360, 1440, 0, 0}; !equalInts(got.Minutes, want) {
		t.Errorf("everyMinutes = %v, want %v", got.Minutes, want)
	}
	if want := []bool{false, false, true, false, true, false, false}; !equalBools(got.Short, want) {
		t.Errorf("retentionTooShort = %v, want %v (equal is refused; an unreadable schedule never warns)", got.Short, want)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalBools(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
