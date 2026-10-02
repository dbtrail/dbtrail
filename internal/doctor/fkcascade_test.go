package doctor

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

var fkCascadeCols = []string{"CONSTRAINT_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME",
	"UNIQUE_CONSTRAINT_SCHEMA", "REFERENCED_TABLE_NAME", "DELETE_RULE", "UPDATE_RULE"}

func fkCascadeCheck(t *testing.T, rows *sqlmock.Rows, queryErr error) CheckResult {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := mock.ExpectQuery("REFERENTIAL_CONSTRAINTS")
	if queryErr != nil {
		q.WillReturnError(queryErr)
	} else {
		q.WillReturnRows(rows)
	}
	c := checkFKCascades(db, []string{"demo"})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	return c
}

// What the user must not be handed: issue or bug numbers, project history,
// storage-engine internals, schema-change statements, em dashes, or a
// command-line flag the console has no field for.
var fkCascadeJargon = regexp.MustCompile(`#\d|Phase|phase-|InnoDB|Bug|ALTER|no longer|\x{2014}|--[a-z]|` +
	"`stream`|`watch`|`up`|`index`")

func TestFKCascadesPass(t *testing.T) {
	c := fkCascadeCheck(t, sqlmock.NewRows(fkCascadeCols), nil)
	if c.Name != FKCascadeCheckName || c.Status != StatusPass {
		t.Fatalf("got %q %s, want %q pass", c.Name, c.Status, FKCascadeCheckName)
	}
	if c.Remediation != "" {
		t.Errorf("a pass carries no fix, got %q", c.Remediation)
	}
}

func TestFKCascadesWarnNamesConstraints(t *testing.T) {
	rows := sqlmock.NewRows(fkCascadeCols).
		AddRow("demo", "orders", "fk_o", "demo", "customers", "CASCADE", "RESTRICT")
	c := fkCascadeCheck(t, rows, nil)
	if c.Name != FKCascadeCheckName || c.Status != StatusWarn {
		t.Fatalf("got %q %s, want %q warn", c.Name, c.Status, FKCascadeCheckName)
	}
	if want := "demo.orders → demo.customers, ON DELETE CASCADE"; c.Detail != want {
		t.Errorf("detail = %q, want %q", c.Detail, want)
	}
	if len(c.Subjects) != 1 || c.Subjects[0] != "demo.orders → demo.customers, ON DELETE CASCADE" {
		t.Errorf("subjects = %q", c.Subjects)
	}
	for _, s := range []string{c.Name, c.Detail, c.Remediation} {
		if m := fkCascadeJargon.FindAllString(s, -1); len(m) > 0 {
			t.Errorf("user-facing text carries %q:\n%s", m, s)
		}
	}
	assertConsoleWording(t, "fk cascades", c)
	if !strings.HasPrefix(c.Remediation, "Capture works normally.") {
		t.Errorf("the fix must open by saying nothing is broken:\n%s", c.Remediation)
	}
	// Both surfaces get their own way to restore, and the cascade-aware
	// restore comes before the optional schema change.
	for _, want := range []string{"Restore page", "`bintrail recover-cascade`"} {
		if !strings.Contains(c.Remediation, want) {
			t.Errorf("fix does not name %q:\n%s", want, c.Remediation)
		}
	}
	if r, s := strings.Index(c.Remediation, "Restore page"), strings.Index(c.Remediation, "RESTRICT"); s >= 0 && s < r {
		t.Errorf("the schema change comes before the restore:\n%s", c.Remediation)
	}
	// Every line flush left: the console turns an indented line into a code
	// box with a Copy button, and nothing here is meant to be run.
	for _, l := range strings.Split(c.Remediation, "\n") {
		if strings.HasPrefix(l, " ") || strings.HasPrefix(l, "\t") {
			t.Errorf("indented line would draw as code: %q", l)
		}
	}
}

func TestFKCascadesWarnCapsTheList(t *testing.T) {
	rows := sqlmock.NewRows(fkCascadeCols)
	for _, tbl := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		rows.AddRow("demo", tbl, "fk_"+tbl, "demo", "p", "CASCADE", "RESTRICT")
	}
	c := fkCascadeCheck(t, rows, nil)
	if !strings.HasSuffix(c.Detail, "; and 2 more") {
		t.Errorf("detail = %q, want the list capped with \"and 2 more\"", c.Detail)
	}
	if len(c.Subjects) != 7 {
		t.Errorf("subjects keep the whole list, got %d", len(c.Subjects))
	}
}

// A failed read is not a finding: it must not hand out the cascade advice as
// if cascades had been found.
func TestFKCascadesReadFailure(t *testing.T) {
	c := fkCascadeCheck(t, nil, errors.New("Error 1142: SELECT command denied"))
	if c.Status != StatusWarn || !strings.Contains(c.Detail, "1142") {
		t.Fatalf("got %s %q, want a warn naming the error", c.Status, c.Detail)
	}
	if strings.Contains(c.Remediation, "recover-cascade") {
		t.Errorf("a read failure carries the cascade advice:\n%s", c.Remediation)
	}
}
