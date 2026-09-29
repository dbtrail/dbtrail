//go:build integration

package doctor

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestBuild_MariaDBVersionRow runs doctor against the real MariaDB source of
// the MariaDB CI job: the version row must be wired into Build and must PASS
// on every version that job tests (10.11 and up). Its name is in that job's
// tripwire, so each leg proves it.
func TestBuild_MariaDBVersionRow(t *testing.T) {
	testutil.SkipIfNoMariaDB(t)

	r := Build(t.Context(), testutil.MariaDBBaseDSN()+"/?parseTime=true", "", "", 0)
	var found []CheckResult
	for _, c := range r.Checks {
		if c.Name == MariaDBVersionCheckName {
			found = append(found, c)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one %q row, got %d. Checks: %+v", MariaDBVersionCheckName, len(found), r.Checks)
	}
	if found[0].Status != StatusPass {
		t.Errorf("%q = %q (%s), want pass on a CI-tested MariaDB", MariaDBVersionCheckName, found[0].Status, found[0].Detail)
	}
	t.Logf("%s: %s", found[0].Name, found[0].Detail)
}
