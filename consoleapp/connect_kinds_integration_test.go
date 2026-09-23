//go:build integration

package consoleapp

import (
	"context"
	"net"
	"slices"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The typed findings (#1803) have to survive the supervisor's mapping into the
// web interface's shape: a field the doctor sets and the mapping forgets is a
// screen with nothing to draw.

func checkNamed(t *testing.T, r *console.DoctorReport, name string) console.DoctorCheck {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no %q check in %+v", name, r.Checks)
	return console.DoctorCheck{}
}

func TestIntegrationDoctorUnsaved_namesTablesWithoutAKey(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	for _, q := range []string{
		"CREATE TABLE keyed (id INT PRIMARY KEY)",
		"CREATE TABLE loose (v INT)",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	sup := newMonitorSupervisor(context.Background(), testutil.IntegrationDSN(name), nil, 0)
	r, err := sup.DoctorUnsaved(context.Background(), console.ServerEntry{
		SourceDSN: testutil.BaseDSN() + "/", Schemas: name,
	})
	if err != nil {
		t.Fatal(err)
	}
	c := checkNamed(t, r, doctor.PrimaryKeyCheckName)
	if c.Status != "fail" || c.Kind != doctor.KindNoPrimaryKey {
		t.Fatalf("status/kind = %s/%q, want fail/%q: a new server refuses at the first schema read", c.Status, c.Kind, doctor.KindNoPrimaryKey)
	}
	if want := []string{name + ".loose"}; !slices.Equal(c.Subjects, want) {
		t.Errorf("subjects = %v, want %v", c.Subjects, want)
	}
	if want := []string{"ALTER TABLE `" + name + "`.`loose` ADD COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;"}; !slices.Equal(c.Statements, want) {
		t.Errorf("statements = %v, want %v", c.Statements, want)
	}
}

func TestIntegrationDoctorUnsaved_provesTheLoopbackCase(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	cfg, err := mysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, closed, _ := net.SplitHostPort(l.Addr().String())
	l.Close()

	sup := newMonitorSupervisor(context.Background(), testutil.IntegrationDSN("unused"), nil, 0)
	sup.loopbackRetry = func(string, string) string { return cfg.Addr }
	typed := *cfg
	typed.Addr = net.JoinHostPort("localhost", closed)
	typed.Timeout = 2e9
	r, err := sup.DoctorUnsaved(context.Background(), console.ServerEntry{SourceDSN: typed.FormatDSN()})
	if err != nil {
		t.Fatal(err)
	}
	if c := checkNamed(t, r, doctor.SourceConnectionCheckName); c.Kind != doctor.KindLoopbackInContainer {
		t.Errorf("kind = %q, want %q: the supervisor must pass its retry to the doctor", c.Kind, doctor.KindLoopbackInContainer)
	}
}
