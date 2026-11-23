package consoleapp

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #2475: the page says "this folder keeps every snapshot" from
// console.FolderKeepsEverySnapshot, and the cleanup acts on prunePlan. A folder
// the plan prunes must never be reported as growing, nor the reverse.
func TestFolderKeepsEverySnapshot_agreesWithPrunePlan(t *testing.T) {
	const global, globalS3 = "/daemon", "s3://b/daemon/"
	sets := map[string][]console.ServerEntry{
		"adopted, no count": {{ID: "1", Name: "a", BaselineDir: "/a"}},
		"a count":           {{ID: "1", Name: "a", BaselineDir: "/a", LocalKeepNewest: 3}},
		"a count with S3":   {{ID: "1", Name: "a", BaselineDir: "/a", BaselineS3: "s3://b/a/", LocalKeepNewest: 3, LocalKeepSavedWith: "3 s3://b/a/"}},
		"S3 and no count":   {{ID: "1", Name: "a", BaselineDir: "/a", BaselineS3: "s3://b/a/"}},
		"two share a folder": {
			{ID: "1", Name: "a", BaselineDir: "/s", LocalKeepNewest: 3},
			{ID: "2", Name: "b", BaselineDir: "/s", LocalKeepNewest: 3},
		},
		"two share a prefix": {
			{ID: "1", Name: "a", BaselineDir: "/a", BaselineS3: "s3://b/same/"},
			{ID: "2", Name: "b", BaselineDir: "/b", BaselineS3: "s3://b/same/"},
		},
		"a server on the daemon's folder": {{ID: "1", Name: "a", BaselineDir: global, LocalKeepNewest: 3}},
		"held after sharing":              {{ID: "1", Name: "a", BaselineDir: "/h", LocalKeepNewest: 3, LocalKeepHeldDir: "/h"}},
	}
	for name, entries := range sets {
		for _, gs3 := range []string{"", globalS3} {
			for _, retain := range []time.Duration{0, 24 * time.Hour} {
				pruned := map[string]bool{}
				for _, tgt := range prunePlan(entries, global, gs3, retain) {
					pruned[console.CanonicalDir(tgt.dir)] = true
				}
				dirs := []string{global}
				for _, e := range entries {
					dirs = append(dirs, e.BaselineDir)
				}
				for _, d := range dirs {
					got := console.FolderKeepsEverySnapshot(entries, d, global, gs3, retain > 0)
					if want := !pruned[console.CanonicalDir(d)]; got != want {
						t.Errorf("%s, daemon S3 %q, age %v, folder %s: keeps every = %v, the cleanup plan says %v", name, gs3, retain, d, got, want)
					}
				}
			}
		}
	}
}
