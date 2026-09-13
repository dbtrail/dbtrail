package console

import (
	"strings"
	"testing"
)

// The S3 retention block (#1622) is a promise as much as a feature: dbtrail
// never deletes from S3 and never sets a bucket rule, the generated rule is
// scoped to the backup prefix and refused at the bucket root, and the page
// says what an age rule cannot do. This pins those sentences and the two
// facts of the generated JSON, since no JS harness runs the function.
func TestS3RetentionBlockKeepsThePromises(t *testing.T) {
	js := readAsset(t, "app.js")
	rule := jsFunctionBody(t, js, "lifecycleRuleFor")
	box := jsFunctionBody(t, js, "s3RetentionBox")
	row := jsFunctionBody(t, js, "backupServerRow")

	for _, want := range []string{`Filter: { Prefix: s.prefix + "/" }`, `Expiration: { Days: days }`, `if (!s || !s.prefix || !(days > 0)) return null;`} {
		if !strings.Contains(rule, want) {
			t.Errorf("lifecycleRuleFor lost %q", want)
		}
	}
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
	if !strings.Contains(row, "s3RetentionBox(srv)") || !strings.Contains(row, "srv.resolved_s3") {
		t.Error("backupServerRow does not mount the retention block for an S3 destination")
	}
	if strings.Contains(box, "—") || strings.Contains(rule, "—") {
		t.Error("em dash in UI copy")
	}
}
