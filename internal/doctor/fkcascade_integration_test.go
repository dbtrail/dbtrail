//go:build integration

package doctor

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// `bintrail doctor` against a real ON DELETE CASCADE prints the note naming
// the foreign key; a console build leaves it out. Run with -v to read the text.
func TestIntegrationFKCascadeCLIText(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, `CREATE TABLE customers (id INT PRIMARY KEY)`)
	testutil.MustExec(t, db, `CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL,
		CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE)`)

	r := Build(t.Context(), testutil.IntegrationDSN(name), "", name, 0)
	var c *CheckResult
	for i := range r.Checks {
		if r.Checks[i].Name == FKCascadeCheckName {
			c = &r.Checks[i]
		}
	}
	if c == nil || c.Status != StatusWarn || c.Detail != name+".orders → "+name+".customers, ON DELETE CASCADE" {
		t.Fatalf("cascade check = %+v", c)
	}
	var out strings.Builder
	if err := (&Report{Checks: []CheckResult{*c}}).Write(&out, "text"); err != nil {
		t.Fatal(err)
	}
	t.Logf("bintrail doctor:\n%s", out.String())

	for _, ch := range Build(t.Context(), testutil.IntegrationDSN(name), "", name, 0, ForConsole()).Checks {
		if ch.Name == FKCascadeCheckName {
			t.Errorf("ForConsole kept the cascade check: %+v", ch)
		}
	}
}
