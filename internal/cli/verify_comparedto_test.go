package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/verify"
)

// The text report says which read of the database the compared tables were
// checked against: the newest snapshot may be newer than the read.
func TestWriteVerifyText_namesTheRead(t *testing.T) {
	row := func(table, at string) verify.TableReport {
		return verify.TableReport{Schema: "shop", Table: table, Status: verify.StatusMatch, ComparedTo: at}
	}
	cases := []struct {
		name   string
		tables []verify.TableReport
		want   string
	}{
		{"one read", []verify.TableReport{row("a", "2026-09-02T03:00:00Z"), row("b", "2026-09-02T03:00:00Z")},
			"Compared against the last read of the database, at 2026-09-02T03:00:00Z."},
		{"two reads", []verify.TableReport{row("a", "2026-09-02T03:00:00Z"), row("b", "2026-09-05T03:00:00Z")},
			"Compared against each table's last read of the database, at 2 different times (--format json lists each)."},
		{"none compared", []verify.TableReport{row("a", "")}, ""},
	}
	for _, c := range cases {
		var out bytes.Buffer
		writeVerifyText(&out, &verify.Report{Tables: c.tables})
		got := out.String()
		if c.want == "" {
			if strings.Contains(got, "Compared against") {
				t.Errorf("%s: names a read no table was compared against:\n%s", c.name, got)
			}
			continue
		}
		if !strings.Contains(got, c.want) {
			t.Errorf("%s: text report does not say %q:\n%s", c.name, c.want, got)
		}
	}
}

// `bintrail verify --help` describes the check the command runs: each table's
// last read of the database against the baseline before it, not the two
// newest baselines, with the reasons that check can come back inconclusive.
func TestVerifyHelp_describesTheLastRead(t *testing.T) {
	help := strings.Join(strings.Fields(verifyCmd.Long), " ")
	for _, want := range []string{
		"takes the last baseline that read it from the database",
		"the table's last read no longer kept or not on record",
		"a TRUNCATE, DROP or RENAME between the two baselines",
		"The report names the read each table was compared against.",
	} {
		if !strings.Contains(help, want) {
			t.Errorf("verify --help does not say %q", want)
		}
	}
	for _, stale := range []string{"two most recent baselines", "new baseline's"} {
		if strings.Contains(help, stale) {
			t.Errorf("verify --help still says %q, the check it no longer runs", stale)
		}
	}
}
