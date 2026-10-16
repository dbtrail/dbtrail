package console

import (
	"strings"
	"testing"
)

// #2431: every server keeps a local copy, so a snapshot is written only for
// a server with a local folder. An S3 location alone is not a place to write.
func TestBaselinePrecheck_needsALocalFolder(t *testing.T) {
	base := ServerEntry{SourceDSN: "u:p@tcp(h:3306)/", DSN: "u:p@tcp(h:3306)/i"}
	for _, c := range []struct {
		name    string
		dir, s3 string
		ok      bool
	}{
		{"folder", "/srv/snaps", "", true},
		{"folder and bucket", "/srv/snaps", "s3://b/p/", true},
		{"bucket only", "", "s3://b/p/", false},
		{"blank folder and bucket", "  ", "s3://b/p/", false},
		{"nothing", "", "", false},
	} {
		e := base
		e.BaselineDir, e.BaselineS3 = c.dir, c.s3
		err := baselineTriggerPrecheck(e)
		if c.ok {
			if err != nil {
				t.Errorf("%s: %v, want nil", c.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), "no local snapshot folder") || !strings.Contains(err.Error(), PageSnapshots) {
			t.Errorf("%s: %v, want the local-folder refusal naming the page", c.name, err)
		}
		if hasOwnBackupLocation(e) {
			t.Errorf("%s: counts as a place a snapshot is written", c.name)
		}
	}
}
