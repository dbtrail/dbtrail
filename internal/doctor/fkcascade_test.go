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
// storage-engine internals, schema-change statements, any advice to change the
// user's foreign keys (RESTRICT), em dashes, flags or a list of commands.
var fkCascadeJargon = regexp.MustCompile(`#\d|Phase|phase-|InnoDB|Bug|ALTER|RESTRICT|no longer|\x{2014}|--[a-z]|` +
	"`stream`|`watch`|`up`|`index`")

func TestFKCascadesPass(t *testing.T) {
	c := fkCascadeCheck(t, sqlmock.NewRows(fkCascadeCols), nil)
	if c.Name != FKCascadeCheckName || c.Status != StatusPass {
		t.Fatalf("got %q %s, want %q pass", c.Name, c.Status, FKCascadeCheckName)
	}
	if c.Remediation != "" || c.Detail != "none found" {
		t.Errorf("a pass says none were found and carries no fix, got %q / %q", c.Detail, c.Remediation)
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
	if !strings.HasPrefix(c.Remediation, "Capture works normally.") {
		t.Errorf("the note must open by saying nothing is broken:\n%s", c.Remediation)
	}
	// Command-line wording only: the console never shows this check, so the
	// note names the CLI's commands and nothing of the console's.
	for _, want := range []string{"`bintrail recover-cascade`", "`bintrail recover`"} {
		if !strings.Contains(c.Remediation, want) {
			t.Errorf("note does not name %q:\n%s", want, c.Remediation)
		}
	}
	for _, bad := range []string{"console", "Restore page", "snapshot"} {
		if strings.Contains(c.Remediation, bad) {
			t.Errorf("note carries %q, which is not command-line wording:\n%s", bad, c.Remediation)
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
	if c.Status != StatusWarn || c.Detail != "could not read the foreign keys: Error 1142: SELECT command denied" {
		t.Fatalf("got %s %q, want a warn naming the error", c.Status, c.Detail)
	}
	if strings.Contains(c.Remediation, "recover-cascade") {
		t.Errorf("a read failure carries the cascade advice:\n%s", c.Remediation)
	}
}
