package pgstreamrun

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// #3108: the doctor's verdict on a publication without TRUNCATE is a
// warning with the statement, never a failure (Start reads this report).
func TestTruncateResult_3108(t *testing.T) {
	if got := truncateResult("p", true, true); got.Name != truncateCheck || got.Status != doctor.StatusPass || got.Detail != "" || got.Remediation != "" {
		t.Errorf("publishes TRUNCATE: %+v", got)
	}
	if got := truncateResult("p", false, false); got.Status != doctor.StatusSkip {
		t.Errorf("missing publication: %+v, want a skip (Publication coverage fails for it)", got)
	}
	got := truncateResult("MyPub", true, false)
	if got.Status != doctor.StatusWarn {
		t.Fatalf("status = %s, want warn", got.Status)
	}
	const detail = `Publication "MyPub" does not publish TRUNCATE, so a TRUNCATE on the source is not recorded: the rows it removed stay in the copy and come back in every rebuild of the table, with nothing to say they are gone`
	if got.Detail != detail {
		t.Errorf("detail =\n%s\nwant\n%s", got.Detail, detail)
	}
	for _, want := range []string{
		"does not stop the server from being added or started",
		"as the publication's owner or a superuser",
		"\n\n  ALTER PUBLICATION \"MyPub\" SET (publish = 'insert, update, delete, truncate');\n",
	} {
		if !strings.Contains(got.Remediation, want) {
			t.Errorf("remediation lacks %q:\n%s", want, got.Remediation)
		}
	}
	if strings.Contains(got.Detail+got.Remediation, "—") {
		t.Errorf("an em dash in the text: %+v", got)
	}
}

// The wire field is present only when there is something to say: a snapshot
// with the zero value, from before this field or from a failed probe, must
// not read as a warning.
func TestSourceHealthJSONTruncateFix_3108(t *testing.T) {
	at := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ok, err := buildSourceHealthJSON(pgcapture.HealthSnapshot{}, at)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(ok), "truncate_fix") {
		t.Errorf("a publication that publishes TRUNCATE carries the field: %s", ok)
	}
	if perr, _ := buildProbeErrorJSON("boom", at); strings.Contains(string(perr), "truncate_fix") {
		t.Errorf("a failed probe carries the field: %s", perr)
	}
	fix := pgcapture.TruncateFixStatement("p")
	b, err := buildSourceHealthJSON(pgcapture.HealthSnapshot{TruncateFix: fix}, at)
	if err != nil {
		t.Fatal(err)
	}
	var h sourceHealthJSON
	if err := json.Unmarshal(b, &h); err != nil {
		t.Fatal(err)
	}
	if h.TruncateFix != fix || h.TruncateCheckError != "" {
		t.Errorf("truncate_fix = %q (error %q), want %q", h.TruncateFix, h.TruncateCheckError, fix)
	}
	if strings.Contains(string(ok), "truncate_check_error") {
		t.Errorf("a healthy snapshot carries a check error: %s", ok)
	}
	b, _ = buildSourceHealthJSON(pgcapture.HealthSnapshot{TruncateErr: errors.New("boom")}, at)
	if !strings.Contains(string(b), `"truncate_check_error":"boom"`) || strings.Contains(string(b), "truncate_fix") {
		t.Errorf("a failed read is not carried: %s", b)
	}
}
