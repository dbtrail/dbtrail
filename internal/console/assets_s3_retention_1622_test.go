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
		"the second command replaces every rule on the bucket",
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
	// The mount condition IS the behaviour: the block under a server whose
	// destination is the daemon default (shared by every server) would hand
	// out a rule on a prefix that is not this row's.
	if !strings.Contains(row, `if (srv.source === "server" && srv.baseline_s3) more.push(s3RetentionBox(srv, servers, daemonS3));`) {
		t.Error("backupServerRow does not mount the retention block exactly for a server with its own S3 destination")
	}
	// The root refusal comes before the form, not instead of it.
	if i, j := strings.Index(box, "if (!s.prefix) {"), strings.Index(box, "sit at the bucket root"); i < 0 || j < 0 || i > j {
		t.Error("the bucket-root refusal is not the branch guarded by !s.prefix")
	}
	// The em-dash rule is checked by the page-wide copy guard in
	// assets_backupsettings_test.go over the SPAN (a body check would fail
	// open past the "s3://" on the root-refusal line).
	for _, want := range []string{
		"would expire the archived changes too",
		"not an s3://bucket/prefix destination",
		"the schedule is stored; it cannot run right now",
		"would expire under this rule",
		"On a bucket with versioning",
		"Enter a whole number of days, 1 or more",
	} {
		if !strings.Contains(box, want) {
			t.Errorf("s3RetentionBox lost the sentence %q", want)
		}
	}
}

// The pure helpers, EXECUTED: a rule at the bucket root would expire the
// archived changes too, a prefix the page tidied up would name objects the
// daemon never wrote, a prefix without its slash would match a sibling, and
// the refusal's boundary is one character.
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
		functionBody(t, js, "function s3Parts("),
		functionBody(t, js, "function retentionTooShort("),
		functionBody(t, js, "function s3PrefixCovers("),
		functionBody(t, js, "function s3RetentionConflicts("),
		functionBody(t, js, "function lifecycleRuleFor("),
	}, "\n") + `
const me = { id: "a", name: "A", baseline_s3: "s3://b/dbtrail", archive_s3: "s3://b/arch" };
const others = [
  me,
  { id: "b", name: "B", baseline_s3: "", resolved_s3: "s3://b/dbtrail/shared", archive_s3: "s3://b/dbtrail/archives" },
  { id: "c", name: "C", baseline_s3: "s3://b/dbtrail-c", resolved_s3: "s3://b/dbtrail-c", archive_s3: "s3://b/dbtrail-c/arch" },
  { id: "d", name: "D", baseline_s3: "s3://b/dbtrail/d", resolved_s3: "s3://b/dbtrail/d", archive_s3: "" },
];
const conflicts = s3RetentionConflicts(me, others, "s3://b/dbtrail/shared");
const ownArchive = s3RetentionConflicts({ id: "a", name: "A", baseline_s3: "s3://b/x", archive_s3: "s3://b/x/arch" }, [], "");
const prefixOf = (u) => { const r = lifecycleRuleFor(u, 30); return r === null ? null : JSON.parse(r).Rules[0].Filter.Prefix; };
const rule7 = JSON.parse(lifecycleRuleFor("s3://b/x", 7)).Rules[0];
const out = {
  root: prefixOf("s3://b"), rootSlash: prefixOf("s3://b/"), nonS3: prefixOf("/var/backups"),
  prefix: prefixOf("s3://b/backups/"), bare: prefixOf("s3://b/backups"),
  doubleSlash: prefixOf("s3://b/x//"), trailingSpace: prefixOf("s3://b/backups "),
  zero: lifecycleRuleFor("s3://b/x", 0), nan: lifecycleRuleFor("s3://b/x", NaN), half: lifecycleRuleFor("s3://b/x", 2.5),
  days: rule7.Expiration.Days, noncurrent: rule7.NoncurrentVersionExpiration.NoncurrentDays,
  covers: [s3PrefixCovers("s3://b/dbtrail", "s3://b/dbtrail"), s3PrefixCovers("s3://b/dbtrail", "s3://b/dbtrail/archives"),
    s3PrefixCovers("s3://b/dbtrail", "s3://b/dbtrail-archives"), s3PrefixCovers("s3://b/dbtrail", "s3://other/dbtrail"),
    s3PrefixCovers("s3://b/dbtrail/backups", "s3://b/dbtrail"), s3PrefixCovers("s3://b/x", "not a url")],
  conflicts, ownArchive: ownArchive.archives,
  short: [retentionTooShort(5, 1), retentionTooShort(360, 1), retentionTooShort(1440, 1), retentionTooShort(1440, 2), retentionTooShort(10080, 7), retentionTooShort(10080, 8), retentionTooShort(0, 1)],
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
		Root, RootSlash, NonS3, Zero, Nan, Half  *string
		Prefix, Bare, DoubleSlash, TrailingSpace string
		Days, Noncurrent                         int
		Covers                                   []bool
		Short                                    []bool
		Conflicts                                struct{ Archives, Backups []string }
		OwnArchive                               []string
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if got.Root != nil || got.RootSlash != nil || got.NonS3 != nil || got.Zero != nil || got.Nan != nil || got.Half != nil {
		t.Errorf("a rule was generated where none must be: %s", out)
	}
	if got.Prefix != "backups/" || got.Bare != "backups/" {
		t.Errorf("prefix scoping: %s (want backups/ with the slash, so backups-old/ is not matched)", out)
	}
	// Byte-faithful to the daemon's keys: it trims ONE trailing slash and
	// nothing else, so these are the prefixes the objects really carry.
	if got.DoubleSlash != "x//" || got.TrailingSpace != "backups /" {
		t.Errorf("the page tidied the prefix the daemon did not: %s", out)
	}
	if got.Days != 7 || got.Noncurrent != 7 {
		t.Errorf("Expiration.Days = %d, NoncurrentDays = %d, want 7 and 7", got.Days, got.Noncurrent)
	}
	if want := []bool{true, true, false, false, false, false}; !equalBools(got.Covers, want) {
		t.Errorf("s3PrefixCovers = %v, want %v (equal and nested cover; a sibling sharing characters, another bucket, the reverse nesting and a non-URL do not)", got.Covers, want)
	}
	if strings.Join(got.Conflicts.Archives, ",") != "B" || strings.Join(got.Conflicts.Backups, ",") != "B,D,the daemon default (s3://b/dbtrail/shared)" {
		t.Errorf("conflicts = %+v: want B's archives refused, B (by its resolved default), D and the daemon default named, C (sibling prefix) untouched", got.Conflicts)
	}
	if strings.Join(got.OwnArchive, ",") != "this server" {
		t.Errorf("own archives under the backup prefix = %v, want [this server]", got.OwnArchive)
	}
	if want := []bool{false, false, true, false, true, false, false}; !equalBools(got.Short, want) {
		t.Errorf("retentionTooShort = %v, want %v (equal is refused; an unknown interval never warns)", got.Short, want)
	}
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

// The DTO carries what the block refuses on: the RAW archive destination
// and the interval as Go parsed it.
func TestBackupSettingsServerDTO_carriesArchiveAndInterval(t *testing.T) {
	srv := newTestServer(t)
	e := ServerEntry{ID: "s1", Name: "one", DSN: "d", BaselineDir: "/b", BaselineS3: "s3://b/backups", ArchiveS3: "s3://b/backups",
		BackupSchedule: &BackupSchedule{Every: "6h", At: "03:00"}}
	dto := srv.backupSettingsServerDTO(e)
	if dto.ArchiveS3 != "s3://b/backups" || dto.ScheduleEveryMinutes != 360 {
		t.Fatalf("dto = %+v, want archive_s3 and 360 minutes", dto)
	}
}
