package pgstreamrun

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// The warning about a capture role with more power than a forwarding user
// needs (0.140.0): the port forwards as that role by default.
func TestCaptureRoleResult(t *testing.T) {
	for _, c := range []struct {
		name          string
		super, bypass bool
		status        doctor.CheckStatus
		detail        string
	}{
		{"an ordinary role", false, false, doctor.StatusPass, ""},
		{"a superuser", true, false, doctor.StatusWarn, `The role "dbtrail" is a superuser`},
		{"BYPASSRLS", false, true, doctor.StatusWarn, `The role "dbtrail" can bypass row-level security (BYPASSRLS)`},
		{"both", true, true, doctor.StatusWarn, `The role "dbtrail" is a superuser, and can bypass row-level security (BYPASSRLS)`},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := captureRoleResult("dbtrail", c.super, c.bypass)
			if got.Name != captureRoleCheck || got.Status != c.status || got.Detail != c.detail {
				t.Fatalf("got %+v", got)
			}
			if c.status == doctor.StatusPass {
				if got.Remediation != "" {
					t.Fatalf("a pass with a remediation: %q", got.Remediation)
				}
				return
			}
			for _, w := range []string{"CREATE ROLE dbtrail_ro LOGIN", "GRANT SELECT ON ALL TABLES IN SCHEMA", "server forward-user <server> -u dbtrail_ro -p", "does not stop the server from being added"} {
				if !strings.Contains(got.Remediation, w) {
					t.Errorf("remediation lacks %q:\n%s", w, got.Remediation)
				}
			}
		})
	}
}
