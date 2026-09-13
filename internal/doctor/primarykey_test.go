package doctor

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// colRows feeds metadata.TablesWithoutPrimaryKey's first query
// (information_schema.COLUMNS). key is COLUMN_KEY: "PRI" is what bintrail
// treats as row identity, everywhere.
func colRows(rows ...[3]string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{
		"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_KEY",
		"DATA_TYPE", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT", "GENERATION_EXPRESSION", "CHARACTER_SET_NAME",
	})
	for _, c := range rows {
		r.AddRow(c[0], c[1], "col", 1, c[2], "int", "int", "NO", nil, nil, nil)
	}
	return r
}

// tabRows feeds the second query (information_schema.TABLES).
func tabRows(rows ...[3]string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME", "ENGINE", "TABLE_TYPE"})
	for _, t := range rows {
		r.AddRow(t[0], t[1], "InnoDB", t[2])
	}
	return r
}

func pkDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	return db, mock, func() { db.Close() }
}

var errPKProbe = errors.New("Error 1142: SELECT command denied to user")

// A table whose only key is a UNIQUE NOT NULL column is NOT a finding, because
// MySQL reports COLUMN_KEY = 'PRI' for it and that is exactly what bintrail
// uses as row identity. Measured on live MySQL 8.4 and MariaDB 11.4: the
// snapshot accepts such a table and the resolver hands back its column as the
// PK, so warning about it would send the operator to fix something that works
// and would assert three failures that do not happen.
//
// This is the case a TABLE_CONSTRAINTS-shaped question gets wrong, which is
// why the check calls the snapshot's own classifier instead.
func TestCheckPrimaryKeys_aUniqueNotNullColumnIsAKey(t *testing.T) {
	db, mock, done := pkDB(t)
	defer done()
	mock.ExpectQuery("information_schema.COLUMNS").
		WillReturnRows(colRows([3]string{"shop", "uniq_notnull", "PRI"}))
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(tabRows([3]string{"shop", "uniq_notnull", "BASE TABLE"}))

	got := checkPrimaryKeys(db, nil)
	if got.Status != StatusPass {
		t.Errorf("status = %v (%q), want PASS. COLUMN_KEY='PRI' IS bintrail's row identity, so "+
			"this table is captured and recoverable; warning about it is a false alarm with "+
			"remediation that asserts failures which will not happen", got.Status, got.Detail)
	}
}

// A MariaDB system-versioned table without a key IS a finding. The pipeline
// classifies TABLE_TYPE IN ('BASE TABLE','SYSTEM VERSIONED') because the
// narrower filter was itself the bug (#1272), and this is the shape where the
// data loss is total: the snapshot excludes the table and capture skips its
// row events while a BASE TABLE-only check prints a clean tick.
func TestCheckPrimaryKeys_aSystemVersionedTableIsNotInvisible(t *testing.T) {
	db, mock, done := pkDB(t)
	defer done()
	mock.ExpectQuery("information_schema.COLUMNS").
		WillReturnRows(colRows([3]string{"shop", "sv_nopk", ""}))
	// The QUERY TEXT: the classifier must ASK for SYSTEM VERSIONED. sqlmock
	// returns canned rows whatever the SQL says, so an expectation that only
	// named the table would stay satisfied with the filter narrowed back to
	// BASE TABLE, which is the #1272 bypass. Measured: metadata's own tests
	// pin this only on the scoped branch, so the unscoped one rides on this.
	mock.ExpectQuery(`TABLE_TYPE IN \('BASE TABLE', 'SYSTEM VERSIONED'\)`).
		WillReturnRows(tabRows([3]string{"shop", "sv_nopk", "SYSTEM VERSIONED"}))

	got := checkPrimaryKeys(db, nil)
	if got.Status != StatusWarn || !strings.Contains(got.Detail, "shop.sv_nopk") {
		t.Errorf("status = %v detail = %q, want a WARN naming shop.sv_nopk. On MariaDB this is the "+
			"one shape with total data loss, and it is the one a BASE TABLE filter cannot see", got.Status, got.Detail)
	}
}

// The reassuring answer must stay reachable, or the check warns on every
// install and gets dismissed everywhere.
func TestCheckPrimaryKeys_passesWhenEveryTableHasOne(t *testing.T) {
	db, mock, done := pkDB(t)
	defer done()
	mock.ExpectQuery("information_schema.COLUMNS").
		WillReturnRows(colRows([3]string{"shop", "orders", "PRI"}))
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(tabRows([3]string{"shop", "orders", "BASE TABLE"}))

	if got := checkPrimaryKeys(db, nil); got.Status != StatusPass {
		t.Errorf("status = %v (%q), want PASS", got.Status, got.Detail)
	}
}

// Nothing visible is not "every table has a primary key". It is the same
// reassuring-answer-from-no-evidence shape this check exists to break, one
// level up, so it reports SKIP.
func TestCheckPrimaryKeys_nothingVisibleIsNotAPass(t *testing.T) {
	db, mock, done := pkDB(t)
	defer done()
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(colRows())

	got := checkPrimaryKeys(db, nil)
	if got.Status == StatusPass {
		t.Fatal("reported PASS with no tables visible, which tells the operator every table has a " +
			"primary key on the evidence of having seen none")
	}
	if got.Status != StatusSkip {
		t.Errorf("status = %v, want SKIP", got.Status)
	}
}

// A failed query is advisory here and must NOT be a FAIL: watch and up refuse
// to boot on any non-advisory FAIL, so a transient information_schema error
// would stop capture on a source that is capturing fine. The snapshot applies
// the same rule itself, so nothing is lost by warning.
func TestCheckPrimaryKeys_aFailedQueryWarnsAndDoesNotBlockBoot(t *testing.T) {
	db, mock, done := pkDB(t)
	defer done()
	mock.ExpectQuery("information_schema.COLUMNS").
		WillReturnError(errPKProbe)

	got := checkPrimaryKeys(db, nil)
	if got.Status == StatusFail {
		t.Fatal("returned FAIL, which makes watch and up refuse to start over an advisory check " +
			"whose answer gates nothing")
	}
	if got.Status != StatusWarn {
		t.Errorf("status = %v, want WARN", got.Status)
	}
	if got.Remediation == "" {
		t.Error("a warning with no next step")
	}
}

// The names stop at the cap and the count stays exact. Both boundaries are
// covered: at exactly the cap there is no "and N more" to print, and one past
// it there is exactly one.
func TestCheckPrimaryKeys_capBoundaries(t *testing.T) {
	for _, tc := range []struct {
		n        int
		wantMore string
		notWant  string
	}{
		{pkNameLimit, "", "more"},
		{pkNameLimit + 1, "and 1 more", ""},
		{pkNameLimit + 7, "and 7 more", ""},
	} {
		db, mock, done := pkDB(t)
		var cols, tabs [][3]string
		for i := 0; i < tc.n; i++ {
			name := "t" + string(rune('a'+i))
			cols = append(cols, [3]string{"shop", name, ""})
			tabs = append(tabs, [3]string{"shop", name, "BASE TABLE"})
		}
		mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(colRows(cols...))
		mock.ExpectQuery("information_schema.TABLES").WillReturnRows(tabRows(tabs...))

		got := checkPrimaryKeys(db, nil)
		if tc.wantMore != "" && !strings.Contains(got.Detail, tc.wantMore) {
			t.Errorf("n=%d: detail %q does not carry %q", tc.n, got.Detail, tc.wantMore)
		}
		if tc.notWant != "" && strings.Contains(got.Detail, tc.notWant) {
			t.Errorf("n=%d: detail %q says %q at exactly the cap, where nothing was cut", tc.n, got.Detail, tc.notWant)
		}
		if n := strings.Count(got.Detail, "shop."); n > pkNameLimit {
			t.Errorf("n=%d: printed %d names, over the cap of %d", tc.n, n, pkNameLimit)
		}
		// The COUNT is the real total, not the number of names printed. An
		// operator sizing the work needs it, and a count that shrank with the
		// list would understate the problem by exactly the amount that was cut.
		if want := fmt.Sprintf("%d table(s)", tc.n); !strings.Contains(got.Detail, want) {
			t.Errorf("n=%d: detail %q does not carry the real total %q", tc.n, got.Detail, want)
		}
		done()
	}
}
