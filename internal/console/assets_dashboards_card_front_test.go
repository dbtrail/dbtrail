package console

import (
	"os"
	"regexp"
	"testing"
)

// The front of the Overview's "Dashboards for the team" card must say it reads
// from S3: it is the only card that does, and before this a reader looking for
// "DuckDB on my bucket" had nothing on the front pointing here.
func TestDashboardsCardFront_namesS3(t *testing.T) {
	src, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?s)\{ id: "dash",.*?\] \},`).Find(src)
	if m == nil {
		t.Fatal(`USE_CARDS entry id "dash" not found in app.js`)
	}
	for _, want := range []string{`["reads from S3", false]`, `straight from your S3`} {
		if !regexp.MustCompile(regexp.QuoteMeta(want)).Match(m) {
			t.Errorf("dash card front lacks %q:\n%s", want, m)
		}
	}
}
