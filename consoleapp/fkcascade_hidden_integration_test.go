//go:build integration

package consoleapp

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The web console says nothing about foreign keys that cascade: they are the
// user's design, and the console's Restore already brings the child rows
// back when the parent table is picked, so there is nothing for a console
// user to act on. The check must be absent from every report the supervisor
// hands the console (the Test of an unsaved server and the checks of a saved
// one), and absent from its counts, while `bintrail doctor` keeps it.
func TestIntegrationConsoleDoctor_omitsFKCascades(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, `CREATE TABLE customers (id INT PRIMARY KEY)`)
	testutil.MustExec(t, db, `CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL,
		CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE)`)

	// The same server through the CLI's Build: the finding is there, so the
	// absence below is the console's doing and not a clean fixture.
	cli := doctor.Build(context.Background(), testutil.IntegrationDSN(name), "", name, 0)
	found := false
	for _, c := range cli.Checks {
		found = found || (c.Name == doctor.FKCascadeCheckName && c.Status == doctor.StatusWarn)
	}
	if !found {
		t.Fatal("bintrail doctor no longer reports the cascade on this server; the test proves nothing")
	}

	sup := newMonitorSupervisor(context.Background(), testutil.IntegrationDSN(name), nil, 0)
	entry := console.ServerEntry{SourceDSN: testutil.BaseDSN() + "/", Schemas: name, DSN: testutil.IntegrationDSN(name)}
	for label, run := range map[string]func(context.Context, console.ServerEntry) (*console.DoctorReport, error){
		"unsaved": sup.DoctorUnsaved,
		"saved":   sup.Doctor,
	} {
		r, err := run(context.Background(), entry)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		warns := 0
		for _, c := range r.Checks {
			if c.Name == doctor.FKCascadeCheckName || strings.Contains(strings.ToLower(c.Detail+c.Remediation), "foreign key") {
				t.Errorf("%s: the console report carries the cascade check: %+v", label, c)
			}
			if c.Status == "warn" && !c.Optional {
				warns++
			}
		}
		if r.Warnings != warns {
			t.Errorf("%s: warnings counted %d, checks shown carry %d", label, r.Warnings, warns)
		}
	}
}
