package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// The settings row says whether the bucket rule it generates is actually in
// the bucket (#1680). Everything below is EXECUTED on the page's own
// functions: the sentences are produced from answers shaped like the ones
// the endpoint sends, and the covering test and the too-short test are run
// beside their Go twins, which the verdict and doctor use.

func runNodeExpiry(t *testing.T, script string) []byte {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	path := filepath.Join(t.TempDir(), "expiry.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	return out
}

func expiryFunctions(t *testing.T) string {
	t.Helper()
	js := readAsset(t, "app.js")
	return strings.Join([]string{
		functionBody(t, js, "function s3Parts("),
		functionBody(t, js, "function retentionTooShort("),
		functionBody(t, js, "function s3PrefixCovers("),
		functionBody(t, js, "function s3ExpiryWords("),
		functionBody(t, js, "function s3ExpiryLine("),
	}, "\n")
}

// TestS3ExpiryGoTwinsAgreeWithThePage: the verdict is computed in Go, and
// the issue requires the covering test to be the one the generator uses.
// One table, both implementations, every answer equal.
func TestS3ExpiryGoTwinsAgreeWithThePage(t *testing.T) {
	covers := [][2]string{
		{"s3://b/dbtrail", "s3://b/dbtrail"},
		{"s3://b/dbtrail", "s3://b/dbtrail/archives"},
		{"s3://b/dbtrail/", "s3://b/dbtrail"},
		{"s3://b/dbtrail", "s3://b/dbtrail/"},
		{"s3://b/dbtrail", "s3://b/dbtrail-archives"},
		{"s3://b/dbtrail", "s3://other/dbtrail"},
		{"s3://b/dbtrail/backups", "s3://b/dbtrail"},
		{"s3://b", "s3://b/dbtrail"},
		{"s3://b/", "s3://b/dbtrail"},
		{"s3://b", "s3://b"},
		{"s3://b/", "s3://b"},
		{"s3://b/dbtrail", "s3://b"},
		{"s3://b/Dbtrail", "s3://b/dbtrail"},
		{"s3://B/dbtrail", "s3://b/dbtrail"},
		{"s3://b/back", "s3://b/backups"},
		{"s3://b/x ", "s3://b/x"},
		{"s3://b/x//", "s3://b/x"},
		{"s3://b/x//", "s3://b/x//"},
		{"s3://b//x", "s3://b//x/y"},
		{"s3://b/x\ny", "s3://b/x\ny"},
		{"s3://b/x\n", "s3://b/x\n"},
		{"s3://b/x", "not a url"},
		{"not a url", "s3://b/x"},
		{"s3://", "s3://"},
		{"", ""},
	}
	short := [][2]int{{5, 1}, {360, 1}, {1440, 1}, {1440, 2}, {10080, 7}, {10080, 8}, {0, 1}, {-5, 1}, {1441, 1}, {1439, 1}}
	in, err := json.Marshal(map[string]any{"covers": covers, "short": short})
	if err != nil {
		t.Fatal(err)
	}
	out := runNodeExpiry(t, expiryFunctions(t)+`
const input = `+string(in)+`;
console.log(JSON.stringify({
  covers: input.covers.map(([o, i]) => s3PrefixCovers(o, i)),
  short: input.short.map(([m, d]) => retentionTooShort(m, d)),
}));
`)
	var got struct{ Covers, Short []bool }
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if len(got.Covers) != len(covers) || len(got.Short) != len(short) {
		t.Fatalf("the page answered %d and %d cases, want %d and %d", len(got.Covers), len(got.Short), len(covers), len(short))
	}
	trues := 0
	for i, c := range covers {
		if g := doctor.S3PrefixCovers(c[0], c[1]); g != got.Covers[i] {
			t.Errorf("covers(%q, %q): Go says %v, the page says %v", c[0], c[1], g, got.Covers[i])
		}
		if got.Covers[i] {
			trues++
		}
	}
	// A table where nothing covers would agree with a function that
	// always answers no.
	if trues < 5 || trues > len(covers)-5 {
		t.Errorf("%d of %d covering cases are true: the table no longer tells the two answers apart", trues, len(covers))
	}
	for i, c := range short {
		if g := doctor.RetentionTooShort(c[0], c[1]); g != got.Short[i] {
			t.Errorf("tooShort(%d, %d): Go says %v, the page says %v", c[0], c[1], g, got.Short[i])
		}
	}
}

type expiryWords struct {
	Text string `json:"text"`
	Warn bool   `json:"warn"`
}

// TestS3ExpirySentences produces every sentence from an answer and pins what
// each must and must not say. The three states never borrow each other's
// words: "No rule" appears for a bucket that was READ and had none, only.
func TestS3ExpirySentences(t *testing.T) {
	out := runNodeExpiry(t, expiryFunctions(t)+`
const daily = { schedule_every_minutes: 1440, schedule_every: "1d" };
const twoDays = { schedule_every_minutes: 2880, schedule_every: "2d" };
const stored = { schedule_every_minutes: 2880, schedule_every: "2d", schedule_refusal: "no folder" };
const none = {};
const denied = "operation error S3: GetBucketLifecycleConfiguration, https response error StatusCode: 403, api error AccessDenied: Access Denied";
const cases = {
  generated: [{ state: "in_force", bucket: "b", prefix: "backups", days: 30, rule_id: "dbtrail-backups-expire-30d" }, daily, 30],
  oneDay: [{ state: "in_force", bucket: "b", prefix: "backups", days: 1, rule_id: "r" }, none, 0],
  wholeBucket: [{ state: "in_force", bucket: "b", prefix: "backups", days: 365, rule_id: "bintrail-1yr-expiry", whole_bucket: true }, daily, 30],
  noID: [{ state: "in_force", bucket: "b", prefix: "backups", days: 14 }, daily, 30],
  tooShort: [{ state: "in_force", bucket: "b", prefix: "backups", days: 1, rule_id: "r" }, twoDays, 15],
  equalIsTooShort: [{ state: "in_force", bucket: "b", prefix: "backups", days: 2, rule_id: "r" }, twoDays, 15],
  justEnough: [{ state: "in_force", bucket: "b", prefix: "backups", days: 3, rule_id: "r" }, twoDays, 15],
  tooShortStored: [{ state: "in_force", bucket: "b", prefix: "backups", days: 1, rule_id: "r" }, stored, 0],
  dateAhead: [{ state: "in_force", bucket: "b", prefix: "backups", expires_on: "2027-01-01", date_rule_id: "dated" }, daily, 30],
  datePassed: [{ state: "in_force", bucket: "b", prefix: "backups", expires_on: "2026-01-01", date_rule_id: "dated", date_passed: true }, daily, 30],
  ageAndDate: [{ state: "in_force", bucket: "b", prefix: "backups", days: 30, rule_id: "r", expires_on: "2027-01-01" }, daily, 30],
  none: [{ state: "none", bucket: "b", prefix: "backups" }, daily, 30],
  noneOne: [{ state: "none", bucket: "b", prefix: "backups" }, daily, 1],
  noneNoSchedule: [{ state: "none", bucket: "b", prefix: "backups" }, none, 0],
  noneConditional: [{ state: "none", bucket: "b", prefix: "backups", conditional: 1 }, none, 0],
  noneConditionalTwo: [{ state: "none", bucket: "b", prefix: "backups", conditional: 2 }, daily, 30],
  denied: [{ state: "unreadable", bucket: "b", prefix: "backups", reason: "denied", error: denied }, daily, 30],
  unsupported: [{ state: "unreadable", bucket: "b", prefix: "backups", reason: "unsupported", error: "api error NotImplemented" }, daily, 30],
  network: [{ state: "unreadable", bucket: "b", prefix: "backups", reason: "error", error: "dial tcp: i/o timeout" }, daily, 30],
  networkDotted: [{ state: "unreadable", bucket: "b", prefix: "backups", reason: "error", error: "the store answered with nothing." }, daily, 30],
  unreadableBare: [{ state: "unreadable" }, daily, 30],
  notApplicable: [{ state: "not_applicable" }, daily, 30],
  unknownState: [{ state: "something-new", days: 30 }, daily, 30],
  emptyObject: [{}, daily, 30],
  nothing: [null, daily, 30],
  notAnObject: ["none", daily, 30],
};
const out = {};
for (const [k, [v, srv, per30]] of Object.entries(cases)) out[k] = s3ExpiryWords(v, srv, per30);
console.log(JSON.stringify(out, null, 1));
`)
	t.Logf("the sentences, as the page writes them:\n%s", out)
	var got map[string]expiryWords
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	const (
		noRule    = "No rule in the bucket expires these snapshots"
		unknown   = "it is not known whether old snapshots expire"
		perm      = "s3:GetBucketLifecycleConfiguration"
		limitless = "the bucket grows without limit"
		gap       = "moments with no snapshot in S3"
	)
	checks := []struct {
		name string
		warn bool
		has  []string
		not  []string
	}{
		{"generated", false, []string{"A rule in the bucket (dbtrail-backups-expire-30d) expires these snapshots after 30 days."}, []string{noRule, unknown, gap, limitless}},
		{"oneDay", false, []string{"after 1 day."}, []string{"1 days", gap}},
		{"wholeBucket", false, []string{"A rule on the whole bucket (bintrail-1yr-expiry) expires these snapshots after 365 days"}, []string{noRule, unknown, limitless}},
		{"noID", false, []string{"A rule in the bucket expires these snapshots after 14 days."}, []string{"()", "undefined"}},
		{"tooShort", true, []string{"after 1 day.", "every 2d", gap, "at least 3 days"}, []string{noRule, unknown}},
		{"equalIsTooShort", true, []string{"after 2 days.", gap, "at least 3 days"}, nil},
		{"justEnough", false, []string{"after 3 days."}, []string{gap}},
		{"tooShortStored", true, []string{gap}, nil},
		{"dateAhead", false, []string{"A rule in the bucket (dated) expires every snapshot here on 2027-01-01, whatever its age."}, []string{noRule, "after", "undefined"}},
		{"datePassed", true, []string{"has been expiring every snapshot here since 2026-01-01", "removed soon after it arrives"}, []string{noRule}},
		{"ageAndDate", false, []string{"after 30 days.", "Another rule expires every snapshot here on 2027-01-01"}, []string{"()", "undefined"}},
		{"none", true, []string{noRule, "About 30 arrive every 30 days and none leaves", limitless}, []string{unknown, perm}},
		{"noneOne", true, []string{noRule, "About 1 arrives every 30 days"}, []string{"1 arrive every"}},
		{"noneNoSchedule", true, []string{noRule, "Each one stays", limitless}, []string{"About", "every 30 days"}},
		{"noneConditional", true, []string{noRule, "1 rule on this prefix applies only to objects with a tag or a size limit, and is not counted."}, nil},
		{"noneConditionalTwo", true, []string{noRule, "2 rules on this prefix apply only to objects with a tag or a size limit, and are not counted."}, nil},
		{"denied", false, []string{"Could not read this bucket's rules", unknown, "The likely reason is a missing permission: " + perm}, []string{noRule, limitless, "expires these"}},
		{"unsupported", false, []string{"This S3 store does not answer when asked for its rules", unknown}, []string{noRule, limitless, perm}},
		{"network", false, []string{"Could not read this bucket's rules", unknown, "dial tcp: i/o timeout", perm}, []string{noRule, limitless}},
		{"networkDotted", false, []string{"answered with nothing. If"}, []string{".."}},
		{"unreadableBare", false, []string{"Could not read this bucket's rules", unknown}, []string{noRule, "undefined", ": ."}},
		{"unknownState", false, []string{"Could not tell whether old snapshots expire"}, []string{noRule, "30 days", limitless}},
		{"emptyObject", false, []string{"Could not tell whether old snapshots expire"}, []string{noRule}},
		{"nothing", false, []string{"Could not tell whether old snapshots expire"}, []string{noRule}},
		{"notAnObject", false, []string{"Could not tell whether old snapshots expire"}, []string{noRule}},
	}
	for _, c := range checks {
		w, ok := got[c.name]
		if !ok {
			t.Errorf("%s: no sentence", c.name)
			continue
		}
		if w.Warn != c.warn {
			t.Errorf("%s: warn = %v, want %v (%q)", c.name, w.Warn, c.warn, w.Text)
		}
		for _, s := range c.has {
			if !strings.Contains(w.Text, s) {
				t.Errorf("%s: %q does not say %q", c.name, w.Text, s)
			}
		}
		for _, s := range c.not {
			if strings.Contains(w.Text, s) {
				t.Errorf("%s: %q says %q", c.name, w.Text, s)
			}
		}
		if strings.ContainsAny(w.Text, "\u2014\u2013") || strings.Contains(w.Text, "undefined") || strings.Contains(w.Text, "NaN") {
			t.Errorf("%s: %q carries a dash or an unfilled value", c.name, w.Text)
		}
		if !strings.HasSuffix(w.Text, ".") {
			t.Errorf("%s: %q does not end as a sentence", c.name, w.Text)
		}
	}
	if len(checks) != len(got)-1 {
		t.Errorf("%d sentences checked, %d produced besides notApplicable", len(checks), len(got)-1)
	}
	if w := got["notApplicable"]; w.Text != "" || w.Warn {
		t.Errorf("a server with no bucket of its own got a sentence: %+v", w)
	}
}

// TestS3ExpiryLineAsksAfterTheRowIsDrawn runs the line itself with the
// page's el and apiWithin replaced: it is returned at once with its waiting
// words, asks the server's own route with a deadline, and a request that
// FAILS ends as "could not", never as the no-rule alarm and never as the
// waiting words left on screen.
func TestS3ExpiryLineAsksAfterTheRowIsDrawn(t *testing.T) {
	out := runNodeExpiry(t, expiryFunctions(t)+`
const el = (tag, attrs) => ({ tag, className: attrs.class, textContent: attrs.text, hidden: false });
let answer, asked = [];
const apiWithin = (path, ms) => { asked.push([path, ms]); return answer(); };
const srv = { id: "a b/c", schedule_every_minutes: 1440, schedule_every: "1d" };
(async () => {
  const out = {};
  const run = async (name, fn) => {
    answer = fn;
    const line = s3ExpiryLine(srv, 30);
    const first = { text: line.textContent, cls: line.className };
    await new Promise((r) => setTimeout(r, 5));
    out[name] = { first, text: line.textContent, cls: line.className, hidden: line.hidden };
  };
  await run("ruled", () => Promise.resolve({ state: "in_force", days: 30, rule_id: "r" }));
  await run("bare", () => Promise.resolve({ state: "none" }));
  await run("denied", () => Promise.resolve({ state: "unreadable", reason: "denied", error: "AccessDenied" }));
  await run("failed", () => Promise.reject(new Error("the server did not answer within 8s")));
  await run("forbidden", () => Promise.reject(new Error("forbidden")));
  await run("empty", () => Promise.resolve(null));
  await run("na", () => Promise.resolve({ state: "not_applicable" }));
  out.asked = asked;
  console.log(JSON.stringify(out));
})();
`)
	var got struct {
		Asked [][]any
		lineResults
	}
	if err := json.Unmarshal(out, &got.lineResults); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	r := got.lineResults
	for name, l := range map[string]lineResult{"ruled": r.Ruled, "bare": r.Bare, "denied": r.Denied, "failed": r.Failed, "forbidden": r.Forbidden, "empty": r.Empty, "na": r.Na} {
		if !strings.Contains(l.First.Text, "Checking whether the bucket expires old snapshots") || l.First.Cls != "form-hint s3-expiry" {
			t.Errorf("%s: the line starts as %+v, want the waiting words in grey", name, l.First)
		}
		if strings.Contains(l.Text, "Checking") {
			t.Errorf("%s: the waiting words stayed on screen: %q", name, l.Text)
		}
	}
	if !strings.Contains(r.Ruled.Text, "after 30 days") || r.Ruled.Cls != "form-hint s3-expiry" {
		t.Errorf("ruled: %+v", r.Ruled)
	}
	if !strings.Contains(r.Bare.Text, "No rule in the bucket") || r.Bare.Cls != "form-msg err s3-expiry" {
		t.Errorf("bare: %+v, want the no-rule sentence in red", r.Bare)
	}
	for name, l := range map[string]lineResult{"denied": r.Denied, "failed": r.Failed, "forbidden": r.Forbidden, "empty": r.Empty} {
		if strings.Contains(l.Text, "No rule") || strings.Contains(l.Text, "without limit") || l.Cls != "form-hint s3-expiry" || l.Hidden || l.Text == "" {
			t.Errorf("%s: %+v, want a grey sentence that claims nothing about the rules", name, l)
		}
	}
	if !strings.Contains(r.Failed.Text, "Could not ask whether the bucket expires old snapshots: the server did not answer within 8s.") {
		t.Errorf("failed: %q does not carry the failure", r.Failed.Text)
	}
	if !r.Na.Hidden {
		t.Errorf("na: the line is shown for a server with no bucket of its own: %+v", r.Na)
	}
	if len(got.Asked) != 7 {
		t.Fatalf("asked %d times, want 7", len(got.Asked))
	}
	for _, a := range got.Asked {
		if a[0] != "/api/servers/a%20b%2Fc/snapshot-expiry" {
			t.Errorf("asked %v, want the server's own route with its id escaped", a[0])
		}
		if ms, _ := a[1].(float64); ms <= 0 || ms > 15000 {
			t.Errorf("asked with a deadline of %v ms, want one, and a short one", a[1])
		}
	}
}

type lineResult struct {
	First struct {
		Text string `json:"text"`
		Cls  string `json:"cls"`
	} `json:"first"`
	Text   string `json:"text"`
	Cls    string `json:"cls"`
	Hidden bool   `json:"hidden"`
}

type lineResults struct {
	Ruled, Bare, Denied, Failed, Forbidden, Empty, Na lineResult
}

// TestS3ExpiryLineIsMounted: the line sits in the retention block, after the
// destination is known to be S3 and BEFORE the refusals that return early,
// so a bucket-root destination (no rule is generated there) still says
// whether the whole-bucket rule covers it.
func TestS3ExpiryLineIsMounted(t *testing.T) {
	box := jsFunctionBody(t, readAsset(t, "app.js"), "s3RetentionBox")
	mount := strings.Index(box, "wrap.append(s3ExpiryLine(srv, n));")
	notS3 := strings.Index(box, "This is not an s3")
	root := strings.Index(box, "if (!s.prefix) {")
	if mount < 0 || notS3 < 0 || root < 0 || !(notS3 < mount && mount < root) {
		t.Errorf("the expiry line is not mounted between the not-S3 return and the bucket-root refusal (not-S3 %d, mount %d, root %d)", notS3, mount, root)
	}
}
